#!/usr/bin/env node
'use strict';

// Offline preparation only. No venue client, signer, order sender or writable
// database is imported. An explicit replay time never represents a live quote.
const fs = require('node:fs');
const path = require('node:path');
const Database = require('better-sqlite3');
const { projectCredits } = require('../bot/spot-accumulation-store');
const { buildSpotPlan } = require('../bot/spot-accumulation');
const { coversRange } = require('../bot/economic-events');
const { checkedSqlitePath } = require('../bot/sqlite-paths');

function readEvidence(dbPath, accountId, from, to) {
  if (!Number.isFinite(Date.parse(from)) || !Number.isFinite(Date.parse(to))) throw new Error('Invalid evidence interval');
  from = new Date(from).toISOString(); to = new Date(to).toISOString();
  const db = new Database(path.resolve(dbPath), { readonly: true, fileMustExist: true });
  try {
    db.pragma('query_only = ON');
    return db.transaction(() => {
      // This command projects the initial rollout. Never re-label an allocation
      // already reserved/spent by a future runtime as fresh purchasing power.
      const tables = new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(row => row.name));
      for (const table of ['spot_accumulation_config', 'spot_batches', 'spot_reservations']) {
        if (tables.has(table) && db.prepare(`SELECT 1 FROM ${table} LIMIT 1`).get()) {
          throw new Error('Existing spot execution state requires reconciliation; initial-rollout projection refused');
        }
      }
      const events = db.prepare(`SELECT * FROM economic_events WHERE account_id=?
        AND event_type='trade' AND timestamp>=? AND timestamp<=? ORDER BY timestamp,event_id`).all(String(accountId), from, to);
      const orders = db.prepare("SELECT * FROM orders WHERE action='sell_put' AND success=1").all();
      const coverage = tables.has('economic_coverage')
        ? db.prepare("SELECT * FROM economic_coverage WHERE account_id=? AND dataset='trades'").all(String(accountId)) : [];
      const state = tables.has('bot_state') ? db.prepare('SELECT put_budget_for_cycle FROM bot_state WHERE id=1').get() : null;
      return { events, orders, history_complete: coversRange(coverage, from, to),
        cycle_budget_usdc: state?.put_budget_for_cycle == null ? null : String(state.put_budget_for_cycle) };
    })();
  } finally { db.close(); }
}

function buildReport(input, { nowMs = Date.now(), replay = false, evidence = null } = {}) {
  if (input?.schema_version !== 1 || !['mainnet', 'testnet'].includes(input.network)) {
    throw new Error('Expected schema_version=1 and explicit mainnet/testnet network');
  }
  const accountId = String(input.account_id);
  if (!/^[1-9]\d*$/.test(accountId) || !Number.isSafeInteger(Number(accountId))) throw new Error('Invalid source account');
  const activation = Date.parse(input.activation_timestamp);
  if (!Number.isFinite(activation) || activation > nowMs) throw new Error('Invalid/future activation timestamp');
  if (String(input.planner_input?.source?.subaccount_id) !== accountId) throw new Error('Planner source account mismatch');
  const source = evidence || input;
  if (!Array.isArray(source.events) || !Array.isArray(source.orders)) throw new Error('Source events and owned sell_put orders are required');
  if (source.events.some(row => !Number.isFinite(Date.parse(row.timestamp)) || Date.parse(row.timestamp) > nowMs)) {
    throw new Error('Future/invalid source evidence cannot fund a projection');
  }
  const credits = projectCredits({ events: source.events, orders: source.orders, accountId,
    activationTimestamp: new Date(activation).toISOString(), network: input.network, apiVersion: 'v3' });
  const planner = structuredClone(input.planner_input);
  planner.nowMs = nowMs;
  // Allocation authority comes only from reconciled put-sale evidence, never
  // from a caller-provided balance or allocation in the market snapshot.
  planner.allocations = { DRV: { source_usdc: credits.allocated_usdc.DRV, destination_usdc: '0' } };
  if (evidence) {
    if (evidence.cycle_budget_usdc == null) throw new Error('Database cycle budget unavailable');
    planner.source.cycle_budget_usdc = evidence.cycle_budget_usdc;
  }
  const historyComplete = source.history_complete === true;
  return {
    schema_version: 1, mode: 'offline_dry_run', replay, synthetic: input.synthetic === true, as_of: new Date(nowMs).toISOString(),
    network: input.network, account_id: accountId, activation_timestamp: new Date(activation).toISOString(),
    ready_to_trade: false, history_complete: historyComplete, credits,
    plan: buildSpotPlan(planner),
    blockers: [
      ...(!historyComplete ? ['Complete settled-history coverage is not established'] : []),
      ...(!credits.complete ? ['Some source evidence could not be credited'] : []),
      'Offline projection only: private order/margin previews and current session authority are required',
      'ALT setup, transfer recovery and venue balance settlement timing remain live-readiness gates',
      'This command does not sign or submit orders/transfers, reserve funds, or initialize execution state',
    ],
  };
}

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === '--help') { args.help = true; continue; }
    if (!['--input', '--db', '--as-of', '--out'].includes(key) || !argv[i + 1] || argv[i + 1].startsWith('--') || args[key]) {
      throw new Error(`Unknown, repeated or incomplete argument: ${key}`);
    }
    args[key] = argv[++i];
  }
  if (!args.help && !args['--input']) throw new Error('--input evidence.json is required');
  return args;
}

function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (args.help) {
    console.log('Usage: node scripts/spot-accumulation.js --input evidence.json [--db existing-noop.db] [--as-of ISO] [--out NEW-report.json]\nRead-only, offline initial-rollout projection. --as-of is historical replay, never live authority.');
    return;
  }
  const input = JSON.parse(fs.readFileSync(path.resolve(args['--input']), 'utf8'));
  const output = args['--out'] ? checkedSqlitePath(args['--out'], {
    protectedPaths: [args['--input'], ...(args['--db'] ? [args['--db']] : [])],
    mustBeNew: true, label: 'Report',
  }) : null;
  const nowMs = args['--as-of'] ? Date.parse(args['--as-of']) : Date.now();
  if (!Number.isFinite(nowMs)) throw new Error('Invalid --as-of timestamp');
  const evidence = args['--db'] ? readEvidence(args['--db'], input.account_id, input.activation_timestamp, new Date(nowMs).toISOString()) : null;
  const report = buildReport(input, { nowMs, replay: Boolean(args['--as-of']), evidence });
  const serialized = JSON.stringify(report, null, 2) + '\n';
  if (output) fs.writeFileSync(output, serialized, { flag: 'wx', mode: 0o600 });
  else process.stdout.write(serialized);
  return report;
}

if (require.main === module) {
  try { main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
module.exports = { readEvidence, buildReport, parseArgs, main };
