'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const Database = require('better-sqlite3');
const { accountInitialReceipt } = require('../integrations/derive-v3/accounting');
const { reconcileOrders } = require('../integrations/derive-v3/cli');

test('ordinary bot entry rejects V3 before loading any database', () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const vm = require('node:vm');
  const { createRequire } = require('node:module');
  const entry = path.resolve(__dirname, '../bot/index.js');
  const actualRequire = createRequire(entry);
  let databaseLoaded = false;
  const context = {
    __dirname: path.dirname(entry), process: { env: { NOOP_VENUE: 'v3-testnet' } },
    require: name => {
      if (name === './db') { databaseLoaded = true; throw new Error('Must not load the production database'); }
      if (name.endsWith('/profile')) return { readProfile: () => ({ version: 3 }) };
      return actualRequire(name);
    },
  };
  assert.throws(() => vm.runInNewContext(fs.readFileSync(entry, 'utf8'), context), /dedicated isolated runner/);
  assert.equal(databaseLoaded, false);
  context.process.env.NOOP_V3_ISOLATED_RUNNER = '1';
  assert.throws(() => vm.runInNewContext(fs.readFileSync(entry, 'utf8'), { ...context }), /dedicated owner and subaccount/);
  assert.equal(databaseLoaded, false);
});

function fixture() {
  const sqlite = new Database(':memory:');
  sqlite.exec('CREATE TABLE events (kind TEXT, payload TEXT);');
  const write = kind => payload => sqlite.prepare('INSERT INTO events VALUES (?, ?)').run(kind, JSON.stringify(payload));
  const db = { db: sqlite, saveBotState: write('budget'), insertOrder: write('fill'),
    insertRestingOrder: write('resting'), updatePendingAction: (id, fields) => write('pending')({ id, ...fields }) };
  const params = { db, botData: { putNetBought: 7 }, action: 'buy_put', instrumentName: 'ETH-20260925-2000-P',
    amount: 1, price: 20, orderType: 'gtc', pendingActionId: 9, instrument: {}, spotPrice: 3000,
    order: {}, record: { order_id: 'abc', order_status: 'open', direction: 'buy', filled_amount: '0.25' },
    trades: [{ trade_amount: '0.25', trade_price: '20' }] };
  return { sqlite, params };
}

test('partial initial GTC fill saves budget, fill, remaining order and pending action together', () => {
  const { sqlite, params } = fixture();
  try {
    const result = accountInitialReceipt(params);
    assert.equal(result.resting, true);
    assert.equal(result.filledAmt, 0.25);
    assert.equal(params.botData.putNetBought, 12);
    const rows = sqlite.prepare('SELECT * FROM events').all();
    assert.deepEqual(rows.map(row => row.kind), ['budget', 'resting', 'fill', 'pending']);
    assert.equal(JSON.parse(rows[1].payload).filled_value, 5);
    assert.equal(JSON.parse(rows[1].payload).filled_amount, 0.25);
    assert.equal(JSON.parse(rows[3].payload).status, 'resting');
  } finally { sqlite.close(); }
});

test('accounting failure rolls back durable rows and in-memory budget', () => {
  const { sqlite, params } = fixture();
  try {
    params.db.updatePendingAction = () => { throw new Error('disk failure'); };
    assert.throws(() => accountInitialReceipt(params), /disk failure/);
    assert.equal(params.botData.putNetBought, 7);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM events').get().n, 0);
  } finally { sqlite.close(); }
});

test('IOC fill with missing trades cannot be mistaken for zero fill', () => {
  const { sqlite, params } = fixture();
  try {
    params.orderType = 'ioc';
    params.record.order_status = 'cancelled';
    params.trades = [];
    assert.throws(() => accountInitialReceipt(params), /does not match trades/);
    assert.equal(sqlite.prepare('SELECT COUNT(*) AS n FROM events').get().n, 0);
  } finally { sqlite.close(); }
});

test('reconcile clears only verified terminal zero fills, retaining open, filled and absent nonces', async () => {
  const rows = [
    { nonce: '1', order_id: 'open', order_status: 'open', filled_amount: '0' },
    { nonce: '2', order_id: 'filled', order_status: 'filled', filled_amount: '1' },
    { nonce: '3', order_id: 'cancel', order_status: 'cancelled', filled_amount: '0' },
    { nonce: '4', order_id: 'unknown-fill', order_status: 'cancelled' },
  ].map(row => ({ ...row, subaccount_id: 9 }));
  const events = [];
  const report = await reconcileOrders({ profile: { subaccountId: 9 }, read: async () => ({ orders: rows.slice(0, 1) }),
    history: async () => rows.slice(1) }, { unresolved: ['1', '2', '3', '4', '5'], append: e => events.push(e) });
  assert.deepEqual(report.resolved, ['cancel']);
  assert.deepEqual(report.unresolved, ['1', '2', '4', '5']);
  assert.deepEqual(events.filter(e => e.event === 'order_accounted').map(e => e.nonce), ['3']);
});
