'use strict';

const fs = require('node:fs');
const { parseUnits } = require('ethers');
const { V2_IDENTITY } = require('./profile');

const SNAPSHOT_SCHEMA_VERSION = 2;

function validateSnapshot(snapshot) {
  if (!snapshot || !['v2', 'v3-mainnet'].includes(snapshot.venue)) throw new Error('Invalid snapshot venue');
  if (!/^0x[0-9a-fA-F]{40}$/.test(snapshot.owner || '')) throw new Error('Snapshot owner is missing or invalid');
  const account = snapshot.account;
  if (!account || account.failed_to_fetch !== false) throw new Error('Snapshot account is unavailable or incomplete');
  if (!Number.isSafeInteger(account.subaccount_id) || account.subaccount_id <= 0) throw new Error('Snapshot subaccount is missing or invalid');
  if (snapshot.venue === 'v2' && (snapshot.owner.toLowerCase() !== V2_IDENTITY.owner.toLowerCase()
    || account.subaccount_id !== V2_IDENTITY.subaccount_id)) throw new Error('Snapshot does not belong to the production V2 account');
  if (typeof account.is_under_liquidation !== 'boolean') throw new Error('Snapshot liquidation state is missing');
  for (const key of ['initial_margin', 'maintenance_margin', 'subaccount_value']) {
    if (typeof account[key] !== 'string' || !/^-?\d+(?:\.\d+)?$/.test(account[key]) || !Number.isFinite(Number(account[key]))) {
      throw new Error(`Snapshot ${key} is missing or invalid`);
    }
  }
  if (snapshot.venue === 'v3-mainnet' && (!Number.isSafeInteger(account.manager_id) || account.manager_id <= 0
    || !Number.isSafeInteger(account.risk_universe_id) || account.risk_universe_id <= 0)) {
    throw new Error('Snapshot V3 trading manager or risk universe is missing');
  }
  for (const key of ['open_orders', 'trigger_orders', 'algo_orders']) {
    if (!Array.isArray(snapshot[key])) throw new Error(`Snapshot ${key} is missing`);
  }
  if (!Array.isArray(account.positions) || !Array.isArray(account.collaterals)) throw new Error('Snapshot balances are missing');
  return snapshot;
}

function balances(rows, key) {
  const totals = new Map();
  for (const row of rows) {
    const name = key(row);
    if (typeof name !== 'string' || !name || typeof row.amount !== 'string'
      || !/^-?\d+(?:\.\d{1,18})?$/.test(row.amount)) throw new Error('Snapshot is missing a balance identity or exact decimal amount');
    totals.set(name, (totals.get(name) || 0n) + parseUnits(String(row.amount), 18));
  }
  return totals;
}

// Conservative cutover comparison. Price/margin values need not match between engines;
// asset quantities and open positions must be explicitly reconciled.
function compareSnapshots(v2, v3, now = Date.now()) {
  const blockers = [];
  if (v2?.venue !== 'v2' || v3?.venue !== 'v3-mainnet') blockers.push('Expected V2 and V3 MAINNET snapshots');
  for (const snapshot of [v2, v3]) {
    try { validateSnapshot(snapshot); } catch (error) { blockers.push(`${snapshot?.venue || 'unknown'}: ${error.message}`); }
    const age = now - Date.parse(snapshot?.timestamp);
    // The frozen V2 snapshot may predate a venue-wide migration window. V3's
    // observation must be fresh; the operator must attest V2 stayed stopped.
    if (!Number.isFinite(age) || age < -30000 || (snapshot?.venue === 'v3-mainnet' && age > 300000)) {
      blockers.push(`${snapshot?.venue}: invalid timestamp or stale V3 snapshot (5-minute limit)`);
    }
    for (const key of ['open_orders', 'trigger_orders', 'algo_orders']) {
      if (!Array.isArray(snapshot?.[key]) || snapshot[key].length) blockers.push(`${snapshot?.venue}: ${key} must be drained and reconciled`);
    }
    if (snapshot?.account?.is_under_liquidation) blockers.push(`${snapshot.venue}: account is under liquidation`);
  }
  for (const [field, key] of [
    ['positions', row => row.instrument_name],
    ['collaterals', row => row.asset_name || row.currency],
  ]) {
    try {
      const left = balances(v2.account[field], key); const right = balances(v3.account[field], key);
      for (const name of new Set([...left.keys(), ...right.keys()])) {
        if ((left.get(name) || 0n) !== (right.get(name) || 0n)) blockers.push(`${field}: reconcile ${name} quantity difference`);
      }
    } catch { blockers.push(`${field}: missing or invalid snapshot data`); }
  }
  return {
    schema_version: SNAPSHOT_SCHEMA_VERSION,
    timestamp: new Date(now).toISOString(), account_comparison_passed: blockers.length === 0, blockers,
    owner: v3?.owner, subaccount_id: v3?.account?.subaccount_id,
    v2_identity: { owner: v2?.owner, subaccount_id: v2?.account?.subaccount_id },
    // This comparison cannot establish deployment exclusivity or migrate budgets.
    remaining_operator_checks: ['V2 writer stopped', 'Pending actions and order intents reconciled',
      'Production SQLite budgets, rules and history backed up and migrated', 'V3 manager/margin policy validated',
      'Dashboard switched to the same V3 account and ledger'],
  };
}

if (require.main === module) {
  try {
    const [v2Path, v3Path] = process.argv.slice(2);
    if (!v2Path || !v3Path) throw new Error('Usage: node integrations/derive-v3/handoff.js v2-snapshot.json v3-mainnet-snapshot.json');
    const report = compareSnapshots(JSON.parse(fs.readFileSync(v2Path, 'utf8')), JSON.parse(fs.readFileSync(v3Path, 'utf8')));
    console.log(JSON.stringify(report, null, 2));
    if (!report.account_comparison_passed) process.exitCode = 1;
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}

module.exports = { compareSnapshots, validateSnapshot, SNAPSHOT_SCHEMA_VERSION };
