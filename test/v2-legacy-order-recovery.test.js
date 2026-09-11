const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const { test } = require('node:test');
const Database = require('better-sqlite3');
const { reconcileLegacyOrder, collectLegacyEvidence } = require('../bot/reconcile-legacy-order');

const root = path.join(__dirname, '..');
const accountId = '7';
const orderId = 'legacy-order-1';
const instrumentName = 'ETH-20261127-1600-P';
const quiet = { log() {}, warn() {}, error() {} };

function venueOrder(fields = {}) {
  return {
    order_id: orderId, nonce: '12345', subaccount_id: accountId,
    instrument_name: instrumentName, direction: 'buy', amount: '5',
    limit_price: '20', filled_amount: '2', average_price: '12', order_status: 'open',
    ...fields,
  };
}

function trade(tradeId, quantity, price, fields = {}) {
  return {
    trade_id: tradeId, order_id: orderId, subaccount_id: accountId,
    instrument_name: instrumentName, direction: 'buy',
    trade_amount: String(quantity), trade_price: String(price), ...fields,
  };
}

// Seed the actual pre-migration table before running production db.js. Only the
// SQLite constructor is replaced; the schema migrations and accounting are real.
function memoryDatabase(t, trackedAmount) {
  const sqlite = new Database(':memory:');
  t.after(() => { if (sqlite.open) sqlite.close(); });
  // The production loader creates the referenced pending_actions table below.
  sqlite.pragma('foreign_keys = OFF');
  sqlite.exec(`CREATE TABLE resting_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL UNIQUE,
    pending_action_id INTEGER REFERENCES pending_actions(id),
    instrument_name TEXT NOT NULL,
    action TEXT NOT NULL,
    direction TEXT NOT NULL,
    amount REAL NOT NULL,
    limit_price REAL NOT NULL,
    placed_at TEXT DEFAULT (datetime('now')),
    filled_amount REAL DEFAULT 0,
    status TEXT DEFAULT 'open'
  )`);
  if (trackedAmount !== null) {
    sqlite.prepare(`INSERT INTO resting_orders
      (order_id,instrument_name,action,direction,amount,limit_price,filled_amount)
      VALUES (?,?,?,?,?,?,?)`).run(orderId, instrumentName, 'buy_put', 'buy', 5, 20, trackedAmount);
  }
  assert.equal(sqlite.prepare('PRAGMA table_info(resting_orders)').all().some((column) => column.name === 'filled_value'), false);
  const filename = path.join(root, 'bot/db.js');
  const localRequire = createRequire(filename);
  const sandbox = {
    __dirname: path.dirname(filename), process: { env: { DATA_DIR: os.tmpdir() } },
    console: quiet, module: { exports: {} },
    require(name) {
      if (name === 'better-sqlite3') return function InMemoryDatabase() { return sqlite; };
      return localRequire(name);
    },
  };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), sandbox, { filename });
  sqlite.pragma('foreign_keys = ON');
  return sandbox.module.exports;
}

function fixture(t, { trackedAmount = 1, booked = true, spent = 110, logOverrides = {}, receipt } = {}) {
  const db = memoryDatabase(t, trackedAmount);
  db.saveBotState({ putNetBought: spent, putBudgetForCycle: 500, putUnspentBuyLimit: 37,
    lastAdvisoryRun: 123456, lastTradeReviewRun: 987654 });
  const initial = venueOrder({ filled_amount: '1', average_price: '10' });
  const firstTrade = trade('legacy-trade-1', 1, 10);
  const logged = {
    action: 'buy_put', success: true, instrument_name: instrumentName,
    price: 20, intended_amount: 5, filled_amount: 1, fill_price: 10, total_value: 10,
    raw_response: receipt || { result: { order: initial, trades: [firstTrade] } },
    ...logOverrides,
  };
  if (booked) db.insertOrder(logged);
  return {
    db, logged,
    payload: { db, accountId, orderId, order: venueOrder(), trades: [firstTrade, trade('legacy-trade-2', 1, 14)] },
  };
}

function savedOrder(db) {
  return db.db.prepare('SELECT * FROM resting_orders WHERE order_id=?').get(orderId);
}

function ledger(db) {
  return db.db.prepare('SELECT * FROM orders ORDER BY id').all();
}

function snapshot(db) {
  const hasRecoveries = db.db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='legacy_order_recoveries'").get();
  return {
    order: savedOrder(db), orders: ledger(db), state: db.loadBotState(),
    recoveries: hasRecoveries ? db.db.prepare('SELECT * FROM legacy_order_recoveries ORDER BY order_id').all() : null,
  };
}

function assertUnchangedAfterRejection(db, run) {
  const before = snapshot(db);
  assert.throws(run);
  assert.deepEqual(snapshot(db), before);
}

test('a migrated zero-fill order books the proved cumulative fill once', (t) => {
  const { db, payload } = fixture(t, { trackedAmount: 0, booked: false, spent: 100 });
  assert.equal(savedOrder(db).filled_value, 0);
  const stateBefore = db.loadBotState();
  reconcileLegacyOrder(payload);
  assert.equal(savedOrder(db).filled_amount, 2);
  assert.equal(savedOrder(db).filled_value, 24);
  assert.deepEqual(ledger(db).map((row) => [row.filled_amount, row.total_value]), [[2, 24]]);
  assert.deepEqual(db.loadBotState(), { ...stateBefore, put_net_bought: 124 });
  const after = snapshot(db);
  reconcileLegacyOrder(payload);
  assert.deepEqual(snapshot(db), after);
});

test('a migrated partial NULL watermark uses booked receipt evidence and accounts only the new delta', (t) => {
  const { db, payload } = fixture(t);
  assert.equal(savedOrder(db).filled_amount, 1);
  assert.equal(savedOrder(db).filled_value, null);
  const stateBefore = db.loadBotState();
  reconcileLegacyOrder(payload);
  assert.equal(savedOrder(db).filled_amount, 2);
  assert.equal(savedOrder(db).filled_value, 24);
  assert.deepEqual(ledger(db).map((row) => [row.filled_amount, row.fill_price, row.total_value]), [[1, 10, 10], [1, 14, 14]]);
  assert.deepEqual(db.loadBotState(), { ...stateBefore, put_net_bought: 124 });
  const after = snapshot(db);
  reconcileLegacyOrder(payload);
  assert.deepEqual(snapshot(db), after);
});

test('the old manager direct cumulative raw_response can prove its single booked partial fill', (t) => {
  const { db, payload } = fixture(t, { receipt: venueOrder({ filled_amount: '1', average_price: '10' }) });
  assert.equal(savedOrder(db).filled_value, null);
  reconcileLegacyOrder(payload);
  assert.equal(savedOrder(db).filled_amount, 2);
  assert.equal(savedOrder(db).filled_value, 24);
  assert.deepEqual(ledger(db).map((row) => [row.filled_amount, row.total_value]), [[1, 10], [1, 14]]);
  assert.equal(db.loadBotState().put_net_bought, 124);
});

test('a later genuine fill after legacy recovery books only its new quantity and value', (t) => {
  const { db, payload } = fixture(t);
  reconcileLegacyOrder(payload);
  const later = {
    ...payload,
    order: venueOrder({ filled_amount: '3', average_price: '13' }),
    trades: [...payload.trades, trade('legacy-trade-3', 1, 15)],
  };
  reconcileLegacyOrder(later);
  assert.equal(savedOrder(db).filled_amount, 3);
  assert.equal(savedOrder(db).filled_value, 39);
  assert.deepEqual(ledger(db).map((row) => [row.filled_amount, row.total_value]), [[1, 10], [1, 14], [1, 15]]);
  assert.equal(db.loadBotState().put_net_bought, 139);
  const after = snapshot(db);
  reconcileLegacyOrder(later);
  assert.deepEqual(snapshot(db), after);
});

test('an untracked initial partial receipt restores tracking and retains the original action', (t) => {
  const { db, payload } = fixture(t, { trackedAmount: null });
  assert.equal(savedOrder(db), undefined);
  reconcileLegacyOrder(payload);
  assert.equal(savedOrder(db).action, 'buy_put');
  assert.equal(savedOrder(db).amount, 5);
  assert.equal(savedOrder(db).limit_price, 20);
  assert.equal(savedOrder(db).filled_amount, 2);
  assert.equal(savedOrder(db).filled_value, 24);
  assert.equal(savedOrder(db).status, 'open');
  assert.equal(db.loadBotState().put_net_bought, 124);
  assert.deepEqual(ledger(db).map((row) => row.total_value), [10, 14]);
  const after = snapshot(db);
  reconcileLegacyOrder(payload);
  assert.deepEqual(snapshot(db), after);
});

test('an untracked receipt with no later fills restores its remainder without rebooking the initial fill', (t) => {
  const { db, payload } = fixture(t, { trackedAmount: null });
  reconcileLegacyOrder({ ...payload, order: venueOrder({ filled_amount: '1', average_price: '10' }), trades: [payload.trades[0]] });
  assert.equal(savedOrder(db).filled_amount, 1);
  assert.equal(savedOrder(db).filled_value, 10);
  assert.equal(db.loadBotState().put_net_bought, 110);
  assert.deepEqual(ledger(db).map((row) => row.total_value), [10]);
});

test('recovering an original short-call buyback preserves its action and leaves put spending unchanged', (t) => {
  const callName = 'ETH-20261127-4000-C';
  const initial = venueOrder({ instrument_name: callName, filled_amount: '1', average_price: '10' });
  const trades = [
    trade('legacy-trade-1', 1, 10, { instrument_name: callName }),
    trade('legacy-trade-2', 1, 14, { instrument_name: callName }),
  ];
  const { db, payload } = fixture(t, {
    trackedAmount: null,
    logOverrides: { action: 'buyback_call', instrument_name: callName },
    receipt: { result: { order: initial, trades: [trades[0]] } },
  });
  const stateBefore = db.loadBotState();
  reconcileLegacyOrder({ ...payload, order: venueOrder({ instrument_name: callName }), trades });
  assert.equal(savedOrder(db).action, 'buyback_call');
  assert.equal(savedOrder(db).instrument_name, callName);
  assert.equal(savedOrder(db).filled_value, 24);
  assert.deepEqual(ledger(db).map((row) => [row.action, row.total_value]), [['buyback_call', 10], ['buyback_call', 14]]);
  assert.deepEqual(db.loadBotState(), stateBefore);
});

for (const [name, change] of [
  ['order ID', (p) => ({ ...p, order: { ...p.order, order_id: 'different-order' } })],
  ['account', (p) => ({ ...p, accountId: '8' })],
  ['missing subaccount', (p) => ({ ...p, order: { ...p.order, subaccount_id: undefined } })],
  ['instrument', (p) => ({ ...p, order: { ...p.order, instrument_name: 'ETH-20261127-4000-C' } })],
  ['nonce', (p) => ({ ...p, order: { ...p.order, nonce: 'different-nonce' } })],
  ['direction', (p) => ({ ...p, order: { ...p.order, direction: 'sell' } })],
  ['trade account', (p) => ({ ...p, trades: [{ ...p.trades[0], subaccount_id: '8' }, p.trades[1]] })],
  ['trade order ID', (p) => ({ ...p, trades: [{ ...p.trades[0], order_id: 'different-order' }, p.trades[1]] })],
  ['duplicate trade IDs', (p) => ({ ...p, trades: [p.trades[0], { ...p.trades[1], trade_id: p.trades[0].trade_id }] })],
  ['missing trade ID', (p) => ({ ...p, trades: [{ ...p.trades[0], trade_id: undefined }, p.trades[1]] })],
  ['partial trade evidence', (p) => ({ ...p, trades: [p.trades[0]] })],
  ['cumulative value mismatch', (p) => ({ ...p, order: { ...p.order, average_price: '13' } })],
  ['unknown order lifecycle', (p) => ({ ...p, order: { ...p.order, order_status: 'unknown' } })],
]) {
  test(`legacy recovery rejects ${name} without changing the ledger, baseline, or budget`, (t) => {
    const { db, payload } = fixture(t);
    assertUnchangedAfterRejection(db, () => reconcileLegacyOrder(change(payload)));
  });
}

test('a NULL legacy watermark without booked fill evidence cannot be inferred from the current average', (t) => {
  const { db, payload } = fixture(t, { booked: false });
  assert.equal(savedOrder(db).filled_value, null);
  assertUnchangedAfterRejection(db, () => reconcileLegacyOrder(payload));
});

test('an untracked venue order without an original local receipt cannot be adopted', (t) => {
  const { db, payload } = fixture(t, { trackedAmount: null, booked: false, spent: 100 });
  assertUnchangedAfterRejection(db, () => reconcileLegacyOrder(payload));
});

test('an untracked receipt with no original strategy action cannot infer one from option direction', (t) => {
  const { db, payload } = fixture(t, { trackedAmount: null, logOverrides: { action: 'unknown' } });
  assertUnchangedAfterRejection(db, () => reconcileLegacyOrder(payload));
});

test('historical ledger values must agree with their exact receipt and trade evidence', (t) => {
  const { db, payload } = fixture(t, { logOverrides: { total_value: 11 } });
  assertUnchangedAfterRejection(db, () => reconcileLegacyOrder(payload));
});

test('a historical receipt with incomplete trade evidence cannot establish a booked baseline', (t) => {
  const { db, payload } = fixture(t, {
    receipt: { result: { order: venueOrder({ filled_amount: '1', average_price: '10' }), trades: [] } },
  });
  assertUnchangedAfterRejection(db, () => reconcileLegacyOrder(payload));
});

test('historical booked fills cannot exceed the legacy tracked watermark', (t) => {
  const { db, payload } = fixture(t, { trackedAmount: 0 });
  assertUnchangedAfterRejection(db, () => reconcileLegacyOrder(payload));
});

test('duplicated booked receipts cannot be used to establish a legacy accounting baseline', (t) => {
  const { db, payload, logged } = fixture(t);
  db.insertOrder(logged);
  assertUnchangedAfterRejection(db, () => reconcileLegacyOrder(payload));
});

test('regressing cumulative venue fills reject recovery without overwriting the legacy baseline', (t) => {
  const { db, payload } = fixture(t);
  assertUnchangedAfterRejection(db, () => reconcileLegacyOrder({
    ...payload, order: venueOrder({ filled_amount: '0', average_price: '0' }), trades: [],
  }));
});

test('a ledger write failure rolls back recovered baseline, incremental fill, and budget together', (t) => {
  const { db, payload } = fixture(t);
  const before = snapshot(db);
  db.db.exec("CREATE TRIGGER reject_recovery_fill BEFORE INSERT ON orders BEGIN SELECT RAISE(ABORT, 'fixture ledger failure'); END");
  assert.throws(() => reconcileLegacyOrder(payload), /fixture ledger failure/);
  assert.deepEqual(snapshot(db), before);
  assert.equal(savedOrder(db).filled_value, null);
});

test('an unresolved journaled submission blocks legacy recovery without releasing either order', (t) => {
  const { db, payload } = fixture(t);
  const { beginSubmission } = require('../bot/order-accounting');
  const submissionId = beginSubmission(db, null, {
    instrument_name: instrumentName, nonce: 'different-submission', subaccount_id: accountId,
    action: 'buy_put', direction: 'buy', amount: '5', limit_price: '20', time_in_force: 'gtc',
  });
  assertUnchangedAfterRejection(db, () => reconcileLegacyOrder(payload));
  assert.equal(db.db.prepare('SELECT status FROM execution_submissions WHERE id=?').get(submissionId).status, 'unknown');
});

test('legacy evidence collection reads every trade page and selects only the exact order', async () => {
  const calls = [];
  const current = venueOrder();
  const expectedTrades = [trade('legacy-trade-1', 1, 10), trade('legacy-trade-2', 1, 14)];
  const evidence = await collectLegacyEvidence({ orderId, accountId, read: async (method, params) => {
    calls.push([method, params.page]);
    assert.equal(String(params.subaccount_id), accountId);
    if (method === 'get_order') {
      assert.equal(params.order_id, orderId);
      return { order: current };
    }
    if (method === 'get_order_history') return { orders: [current] };
    assert.equal(method, 'get_trade_history');
    assert.equal(params.from_timestamp, 0);
    return { trades: params.page === 1
      ? Array.from({ length: 100 }, (_, i) => trade(`other-${i}`, 1, 1, { order_id: 'other-order' }))
      : expectedTrades };
  } });
  assert.deepEqual(evidence.order, current);
  assert.deepEqual(evidence.trades, expectedTrades);
  assert.ok(calls.some(([method, page]) => method === 'get_trade_history' && page === 2));
});

test('a full final trade page is complete when venue pagination explicitly identifies the final page', async () => {
  const current = venueOrder({ amount: '100', filled_amount: '100', average_price: '1', order_status: 'filled' });
  const trades = Array.from({ length: 100 }, (_, i) => trade(`full-page-${i}`, 1, 1));
  let tradeReads = 0;
  const evidence = await collectLegacyEvidence({ orderId, accountId, read: async (method, params) => {
    if (method === 'get_order') return { order: current };
    assert.equal(method, 'get_trade_history');
    assert.equal(params.order_id, orderId);
    assert.equal(params.page, 1, 'must not request an overflow page that repeats the final full page');
    tradeReads++;
    return { trades, pagination: { num_pages: 1 } };
  } });
  assert.equal(tradeReads, 1);
  assert.deepEqual(evidence.trades, trades);
});

test('a pruned direct order can use an exact terminal receipt from a later history page', async () => {
  const terminal = venueOrder({ order_status: 'cancelled' });
  const trades = [trade('legacy-trade-1', 1, 10), trade('legacy-trade-2', 1, 14)];
  const historyPages = [];
  const evidence = await collectLegacyEvidence({ orderId, accountId, read: async (method, params) => {
    if (method === 'get_order') throw new Error('order pruned');
    if (method === 'get_order_history') {
      historyPages.push(params.page);
      assert.equal(params.from_timestamp, 0);
      return { orders: params.page === 1
        ? Array.from({ length: 100 }, (_, i) => venueOrder({ order_id: `other-order-${i}` }))
        : [terminal], pagination: { num_pages: 2 } };
    }
    assert.equal(method, 'get_trade_history');
    return { trades, pagination: { num_pages: 1 } };
  } });
  assert.deepEqual(historyPages, [1, 2]);
  assert.deepEqual(evidence, { order: terminal, trades });
});

test('unknown venue evidence remains a failed recovery rather than a zero-fill conclusion', async () => {
  await assert.rejects(collectLegacyEvidence({ orderId, accountId, read: async (method) => {
    if (method === 'get_order') throw new Error('order pruned');
    assert.equal(method, 'get_order_history');
    return { orders: [] };
  } }));
});

test('a repeated trade-history page cannot be accepted as complete legacy evidence', async () => {
  const page = Array.from({ length: 100 }, (_, i) => trade(`other-${i}`, 1, 1, { order_id: 'other-order' }));
  await assert.rejects(collectLegacyEvidence({ orderId, accountId, read: async (method) => {
    if (method === 'get_order') return { order: venueOrder() };
    if (method === 'get_order_history') return { orders: [venueOrder()] };
    assert.equal(method, 'get_trade_history');
    return { trades: page };
  } }), /pagination|page|incomplete/i);
});
