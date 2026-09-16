'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');

const root = path.join(__dirname, '..');
const source = fs.readFileSync(path.join(root, 'script.js'), 'utf8');
const start = source.indexOf('const manageOpenOrders =');
const end = source.indexOf('\nconst FAILED_ENTRY_ACTION_COOLDOWN_MS', start);
assert.ok(start >= 0 && end > start);
const manageSource = source.slice(start, end);
const dbPath = path.join(root, 'bot', 'db.js');
const dbSource = fs.readFileSync(dbPath, 'utf8');

function fixture(t, { initialAmount = 0, initialValue = 0, v3 = true } = {}) {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-v3-resting-'));
  const module = { exports: {} };
  // Load the real database module against a temporary DB without importing the bot.
  vm.runInNewContext(dbSource, {
    module, exports: module.exports, require: createRequire(dbPath), __dirname: path.dirname(dbPath),
    process: { env: { DATA_DIR: dataDir } }, console,
  }, { filename: dbPath });
  const db = module.exports;
  t.after(() => { db.close(); fs.rmSync(dataDir, { recursive: true, force: true }); });
  const pending = db.insertPendingAction({ action: 'buy_put', instrument_name: 'ETH-20270924-6000-P', amount: 5, price: 20 });
  const pendingId = Number(pending.lastInsertRowid);
  db.updatePendingAction(pendingId, { status: 'resting' });
  db.insertRestingOrder({
    order_id: 'order-a', pending_action_id: pendingId,
    instrument_name: 'ETH-20270924-6000-P', action: 'buy_put', direction: 'buy',
    amount: 5, limit_price: 20, filled_amount: initialAmount, filled_value: initialValue,
  });
  const botData = { putNetBought: initialValue, putBudgetForCycle: 1000, putUnspentBuyLimit: 0 };
  db.saveBotState(botData);
  const state = { open: [], final: null, cancelResult: { order_status: 'cancelled' }, cancelCalls: 0, stale: false, fetchError: null };
  const context = vm.createContext({
    process: { env: {} }, console: { log() {} }, V3_VENUE: v3 ? {} : null, db, botData,
    fetchOpenOrders: async () => { if (state.fetchError) throw state.fetchError; return state.open; },
    fetchOrderStatus: async () => state.final,
    fetchSubaccount: async () => ({}),
    ordersRoughlyMatch: () => true,
    inferActionFromOpenOrder: (_order, tracked) => tracked?.action || 'buy_put',
    buildRollingOptionValueContext: () => ({}),
    validateRestingBuyPutEntryOrder: () => ({ valid: true }),
    validateRestingSellCallEntryOrder: () => ({ valid: true }),
    cancelOrder: async () => { state.cancelCalls++; return state.cancelResult; },
    persistCycleState: () => db.saveBotState(botData),
    notifyOrderLifecycle() {},
  });
  // Keep open orders backed by an active rule unless a test deliberately expires them.
  db.getActiveRules = () => [{ rule_type: 'entry', action: 'buy_put' }];
  vm.runInContext(`${manageSource}\nthis.manage = manageOpenOrders;`, context);
  const order = (filledAmount, averagePrice, status = 'open', extra = {}) => ({
    order_id: 'order-a', instrument_name: 'ETH-20270924-6000-P', direction: 'buy', amount: '5',
    filled_amount: String(filledAmount), average_price: String(averagePrice), order_status: status,
    creation_timestamp: Date.now(), ...extra,
  });
  return {
    db, botData, state, order, manage: context.manage,
    tracked: () => db.db.prepare('SELECT * FROM resting_orders WHERE order_id = ?').get('order-a'),
    action: () => db.db.prepare('SELECT * FROM pending_actions WHERE id = ?').get(pendingId),
    trades: () => db.db.prepare('SELECT * FROM orders ORDER BY id').all(),
  };
}

test('V3 books only incremental partial fills, including cumulative average-price changes', async (t) => {
  const f = fixture(t, { initialAmount: 1, initialValue: 8 });
  assert.equal(f.tracked().filled_amount, 1);
  assert.equal(f.tracked().filled_value, 8);
  f.state.open = [f.order(2, 10)];
  await f.manage({});
  assert.equal(f.botData.putNetBought, 20);
  assert.equal(f.db.loadBotState().put_net_bought, 20);
  assert.equal(f.tracked().filled_amount, 2);
  assert.equal(f.tracked().filled_value, 20);
  assert.equal(f.action().status, 'resting');
  assert.deepEqual(f.trades().map(row => [row.filled_amount, row.fill_price, row.total_value]), [[1, 12, 12]]);
  await f.manage({});
  assert.equal(f.botData.putNetBought, 20);
  assert.equal(f.trades().length, 1);

  f.state.open = [];
  f.state.final = f.order(5, 12, 'filled');
  await f.manage({});
  assert.equal(f.botData.putNetBought, 60);
  assert.equal(f.db.loadBotState().put_net_bought, 60);
  assert.equal(f.tracked().status, 'filled');
  assert.equal(f.action().status, 'executed');
  assert.deepEqual(f.trades().map(row => [row.filled_amount, row.total_value]), [[1, 12], [3, 40]]);
  await f.manage({});
  assert.equal(f.botData.putNetBought, 60);
  assert.equal(f.trades().length, 2);
});

for (const cancelResult of [null, { order_status: 'cancelled' }]) {
  test(`V3 ${cancelResult ? 'acknowledged' : 'failed'} cancellation stays tracked until terminal fill reconciliation`, async (t) => {
    const f = fixture(t);
    f.state.open = [f.order(1, 10, 'open', { creation_timestamp: Date.now() - 9 * 3600000 })];
    f.state.cancelResult = cancelResult;
    await f.manage({});
    assert.equal(f.state.cancelCalls, 1);
    assert.equal(f.tracked().status, 'open');
    assert.equal(f.action().status, 'resting');
    assert.equal(f.botData.putNetBought, 10);

    // Another fill races cancellation and appears only in the final status.
    f.state.open = [];
    f.state.final = f.order(2, 15, 'cancelled');
    await f.manage({});
    assert.equal(f.tracked().status, 'cancelled');
    assert.equal(f.action().status, 'executed');
    assert.equal(f.botData.putNetBought, 30);
    assert.deepEqual(f.trades().map(row => [row.filled_amount, row.total_value]), [[1, 10], [1, 20]]);
  });
}

test('V3 database failure rolls back budget, fill watermark, and trade records before retry', async (t) => {
  const f = fixture(t);
  f.state.open = [f.order(2, 10)];
  const update = f.db.updatePendingAction;
  f.db.updatePendingAction = () => { throw new Error('injected action write failure'); };
  await assert.rejects(f.manage({}), /injected action write failure/);
  assert.equal(f.botData.putNetBought, 0);
  assert.equal(f.db.loadBotState().put_net_bought, 0);
  assert.equal(f.tracked().filled_amount, 0);
  assert.equal(f.tracked().filled_value, 0);
  assert.equal(f.trades().length, 0);
  f.db.updatePendingAction = update;
  await f.manage({});
  assert.equal(f.botData.putNetBought, 20);
  assert.equal(f.trades().length, 1);
});

test('V3 never fuzzy-relinks an untracked exchange order', async (t) => {
  const f = fixture(t);
  f.state.open = [f.order(1, 10, 'open', { order_id: 'lookalike-order' })];
  await assert.rejects(f.manage({}), /no local accounting record/);
  assert.equal(f.tracked().order_id, 'order-a');
  assert.equal(f.tracked().filled_amount, 0);
  assert.equal(f.state.cancelCalls, 0);
});

test('V3 uncertain terminal status and malformed fill data stop further trading', async (t) => {
  const f = fixture(t, { initialAmount: 1, initialValue: 10 });
  await assert.rejects(f.manage({}), /status is uncertain/);
  f.state.open = [f.order(0, 0)];
  await assert.rejects(f.manage({}), /regressing cumulative fills/);
  f.state.open = [f.order(2, 10, 'open', { average_price: null })];
  await assert.rejects(f.manage({}), /invalid or regressing/);
  assert.equal(f.botData.putNetBought, 10);
  assert.equal(f.tracked().filled_amount, 1);
  assert.equal(f.trades().length, 0);
});

test('V3 missing account data propagates; V2 retains its existing read fallback', async (t) => {
  const v3 = fixture(t);
  v3.state.fetchError = new Error('exchange unavailable');
  await assert.rejects(v3.manage({}), /exchange unavailable/);
  const v2 = fixture(t, { v3: false });
  v2.state.fetchError = new Error('exchange unavailable');
  await v2.manage({});
  assert.equal(v2.tracked().status, 'open');
});

test('V2 terminal fill reconciliation retains the existing accounting behavior', async (t) => {
  const f = fixture(t, { v3: false });
  f.state.final = f.order(5, 12, 'filled');
  await f.manage({});
  assert.equal(f.botData.putNetBought, 60);
  assert.equal(f.tracked().status, 'filled');
  assert.deepEqual(f.trades().map(row => [row.filled_amount, row.total_value]), [[5, 60]]);
});

test('the actual V3 status helper preserves order fields through history fallback and keeps absence unknown', async () => {
  const { DeriveV3 } = require('../integrations/derive-v3');
  const { readProfile } = require('../integrations/derive-v3/profile');
  const profile = readProfile({ NOOP_VENUE: 'v3-testnet',
    DERIVE_V3_TESTNET_OWNER_ADDRESS: '0x1111111111111111111111111111111111111111',
    DERIVE_V3_TESTNET_SUBACCOUNT_ID: '987654321' });
  const record = {
    order_id: 'terminal-order', order_status: 'cancelled', instrument_name: 'ETH-20270924-6000-P',
    direction: 'buy', amount: '5', filled_amount: '1.234567890123456789', average_price: '10.123456789012345678',
  };
  let rows = [record];
  const adapter = new DeriveV3({ profile, client: { send: async (method) => {
    if (method === 'private/get_order') throw Object.assign(new Error('Does not exist'), { code: 11006 });
    assert.equal(method, 'private/get_order_history');
    return { orders: rows, pagination: { num_pages: 1 } };
  } } });
  const statusStart = source.indexOf('const fetchOrderStatus =');
  const statusEnd = source.indexOf('\n};', statusStart) + 3;
  const context = vm.createContext({ V3_VENUE: { adapter } });
  vm.runInContext(`${source.slice(statusStart, statusEnd)}\nthis.getStatus = fetchOrderStatus;`, context);
  assert.equal(await context.getStatus(record.order_id), record);
  assert.equal((await context.getStatus(record.order_id)).filled_amount, '1.234567890123456789');
  rows = [];
  assert.equal(await context.getStatus(record.order_id), null);
  rows = [{ ...record, average_price: undefined }];
  assert.equal((await context.getStatus(record.order_id)).average_price, undefined);
});

test('the actual trading pipeline skips rule evaluation and execution after failed V3 reconciliation', async () => {
  const call = source.indexOf('await manageOpenOrders(tickerMap, positions, instruments, spotPrice)');
  const blockStart = source.lastIndexOf('    try {', call);
  const blockEnd = source.indexOf('\n    // SQLite:', call);
  assert.ok(call >= 0 && blockStart >= 0 && blockEnd > call);
  let notifications = 0;
  const context = vm.createContext({
    tickerMap: {}, positions: [], instruments: [], spotPrice: 1,
    manageOpenOrders: async () => { throw new Error('unknown cumulative fills'); },
    evaluateTradingRules: () => assert.fail('must not evaluate new entries with unknown fills'),
    confirmAndExecutePending: () => assert.fail('must not execute with unknown fills'),
    console: { error() {} }, sendTelegram: () => { notifications++; },
  });
  await vm.runInContext(`(async () => { ${source.slice(blockStart, blockEnd)} })()`, context);
  assert.equal(notifications, 1);
});
