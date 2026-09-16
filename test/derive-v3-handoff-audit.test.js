'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { Wallet } = require('ethers');
const { readProfile, V2_IDENTITY } = require('../integrations/derive-v3/profile');
const { compareSnapshots } = require('../integrations/derive-v3/handoff');
const { validateHandoffReport, main } = require('../integrations/derive-v3/run-bot');
const { prepareState } = require('../integrations/derive-v3/prepare-state');
const { acquireWriter, openJournal, stateDirectory } = require('../integrations/derive-v3/state');
const { orderList } = require('../integrations/derive-v3/capture-snapshot');

const wallet = Wallet.createRandom();
const profile = readProfile({ NOOP_VENUE: 'v3-mainnet', DERIVE_V3_MAINNET_RELEASE: 'verified',
  DERIVE_V3_MAINNET_OWNER_ADDRESS: wallet.address, DERIVE_V3_MAINNET_SUBACCOUNT_ID: String(930000000 + process.pid) });
function snapshots() {
  const account = { failed_to_fetch: false, is_under_liquidation: false, subaccount_id: V2_IDENTITY.subaccount_id,
    initial_margin: '100', maintenance_margin: '100', subaccount_value: '100', positions: [],
    collaterals: [{ asset_name: 'USDC', amount: '100' }], manager_id: 1, risk_universe_id: 1 };
  const v2 = { venue: 'v2', owner: V2_IDENTITY.owner, timestamp: new Date().toISOString(), account,
    open_orders: [], trigger_orders: [], algo_orders: [] };
  const v3 = { ...v2, venue: 'v3-mainnet', owner: wallet.address, account: { ...account, subaccount_id: profile.subaccountId } };
  return [v2, v3];
}

test('handoff requires proof that trigger and algo orders are drained', () => {
  for (const venueIndex of [0, 1]) {
    for (const field of ['trigger_orders', 'algo_orders']) {
      const pair = snapshots(); pair[venueIndex][field] = [{ order_id: 'still-executable' }];
      assert.equal(compareSnapshots(...pair).account_comparison_passed, false);
      delete pair[venueIndex][field];
      assert.equal(compareSnapshots(...pair).account_comparison_passed, false);
    }
  }
  assert.equal(compareSnapshots(...snapshots()).account_comparison_passed, true);
});

test('handoff rejects wrong source identity and malformed account metadata', () => {
  const cases = [
    pair => { pair[0].owner = wallet.address; },
    pair => { pair[0].account.subaccount_id = 42; },
    pair => { delete pair[0].owner; },
    pair => { delete pair[1].account.initial_margin; },
    pair => { delete pair[1].account.failed_to_fetch; },
    pair => { pair[1].account.initial_margin = ' '; },
    pair => { pair[1].account.manager_id = 0; },
    pair => { pair[1].account.risk_universe_id = 0; },
    pair => { pair[1].account.collaterals = [{ asset_name: 'USDC', amount: 100 }]; },
  ];
  for (const change of cases) {
    const pair = snapshots(); change(pair);
    assert.equal(compareSnapshots(...pair).account_comparison_passed, false, change.toString());
  }
  assert.equal(compareSnapshots(null, null).account_comparison_passed, false);
});

test('mainnet activation requires the new complete report and correct source identity', () => {
  const report = compareSnapshots(...snapshots());
  assert.doesNotThrow(() => validateHandoffReport(report, profile));
  assert.throws(() => validateHandoffReport({ ...report, schema_version: undefined }, profile), /incomplete/);
  assert.throws(() => validateHandoffReport({ ...report, v2_identity: { owner: wallet.address, subaccount_id: 42 } }, profile), /identity/);
  assert.throws(() => validateHandoffReport({ ...report, blockers: ['Unresolved action'] }, profile), /failed/);
  assert.throws(() => validateHandoffReport(report, profile, { now: Date.parse(report.timestamp) + 300001 }), /stale/);
  assert.doesNotThrow(() => validateHandoffReport(report, profile, { alreadyActivated: true, now: Date.parse(report.timestamp) + 300001 }));
});

test('capture accepts only explicit complete order arrays', () => {
  assert.deepEqual(orderList([], 'get_algo_orders'), []);
  assert.deepEqual(orderList({ orders: [] }, 'get_open_orders'), []);
  assert.throws(() => orderList({}, 'get_trigger_orders'), /missing/);
});

test('runner holds its lock throughout preflight and always closes a failed client', async () => {
  const savedEnv = { ...process.env }; const savedArgv = process.argv;
  const p = { ...profile, name: 'v3-testnet', network: 'testnet', prefix: 'DERIVE_V3_TESTNET_', subaccountId: profile.subaccountId + 1 };
  const dir = stateDirectory(p);
  let closed = false; let started = false;
  try {
    process.argv = ['node', 'run-bot.js'];
    process.env.NOOP_VENUE = p.name; process.env.DERIVE_V3_TESTNET_OWNER_ADDRESS = wallet.address;
    process.env.DERIVE_V3_TESTNET_SUBACCOUNT_ID = String(p.subaccountId);
    process.env.DERIVE_V3_TESTNET_PRIVATE_KEY = wallet.privateKey;
    await assert.rejects(main({ adapterFactory: () => ({
      account: async () => { assert.throws(() => acquireWriter(p), /writer lock/); throw new Error('preflight failed'); },
      close: async () => { closed = true; },
    }), initializeDatabase: () => { started = true; }, startBot: () => { started = true; } }), /preflight failed/);
    assert.equal(closed, true); assert.equal(started, false);
    assert.equal(fs.existsSync(path.join(dir, 'writer.lock')), false);
  } finally {
    process.argv = savedArgv;
    for (const key of Object.keys(process.env)) if (!(key in savedEnv)) delete process.env[key];
    Object.assign(process.env, savedEnv); fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('runner refuses unresolved journal before opening its database', async () => {
  const savedEnv = { ...process.env }; const savedArgv = process.argv;
  const p = { ...profile, name: 'v3-testnet', network: 'testnet', prefix: 'DERIVE_V3_TESTNET_', subaccountId: profile.subaccountId + 2 };
  const dir = stateDirectory(p); let initialized = false;
  try {
    openJournal(p).append({ event: 'order_intent', nonce: '123456789' });
    process.argv = ['node', 'run-bot.js'];
    process.env.NOOP_VENUE = p.name; process.env.DERIVE_V3_TESTNET_OWNER_ADDRESS = wallet.address;
    process.env.DERIVE_V3_TESTNET_SUBACCOUNT_ID = String(p.subaccountId);
    process.env.DERIVE_V3_TESTNET_PRIVATE_KEY = wallet.privateKey;
    await assert.rejects(main({ adapterFactory: () => ({ account: async () => ({}), close: async () => {} }),
      initializeDatabase: () => { initialized = true; }, startBot: () => { initialized = true; } }), /Reconcile/);
    assert.equal(initialized, false);
  } finally {
    process.argv = savedArgv;
    for (const key of Object.keys(process.env)) if (!(key in savedEnv)) delete process.env[key];
    Object.assign(process.env, savedEnv); fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('state preparation rejects linked source wiki pages and releases its lock on failure', async () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-handoff-links-'));
  const dir = stateDirectory(profile);
  try {
    const wiki = path.join(temp, 'knowledge'); fs.mkdirSync(wiki);
    const shared = path.join(temp, 'production.md'); fs.writeFileSync(shared, 'preserve original');
    fs.symlinkSync(shared, path.join(wiki, 'linked.md'));
    await assert.rejects(prepareState(profile, path.join(temp, 'missing.db'), wiki), /symlink/);
    assert.equal(fs.existsSync(path.join(dir, 'writer.lock')), false);
    assert.equal(fs.readFileSync(shared, 'utf8'), 'preserve original');
  } finally { fs.rmSync(temp, { recursive: true, force: true }); fs.rmSync(dir, { recursive: true, force: true }); }
});
