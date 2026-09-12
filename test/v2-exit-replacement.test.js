'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const { test } = require('node:test');
const Database = require('better-sqlite3');
const { declaration } = require('./helpers/load-production');
const accounting = require('../bot/order-accounting');
const { compareRestingExitOrder } = require('../bot/resting-exit-plan');

const root = path.resolve(__dirname, '..');
const source = fs.readFileSync(path.join(root, 'script.js'), 'utf8');
const quiet = { log() {}, warn() {}, error() {} };
const instrumentName = 'ETH-20301210-3200-C';
const action = { action: 'buyback_call', instrument_name: instrumentName };

function memoryDatabase(t) {
  const filename = path.join(root, 'bot/db.js');
  const localRequire = createRequire(filename);
  const sqlite = new Database(':memory:');
  t.after(() => { if (sqlite.open) sqlite.close(); });
  const sandbox = {
    __dirname: path.dirname(filename), process: { env: { DATA_DIR: os.tmpdir() } },
    console: quiet, module: { exports: {} },
    require(name) {
      if (name === 'better-sqlite3') return function InMemoryDatabase() { return sqlite; };
      return localRequire(name);
    },
  };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), sandbox, { filename });
  return sandbox.module.exports;
}

function venueOrder(fields = {}) {
  return { order_id: 'old-exit', instrument_name: instrumentName, direction: 'buy',
    amount: '5', filled_amount: '0', average_price: '0', limit_price: '4',
    order_status: 'open', time_in_force: 'post_only', ...fields };
}

function seedOrder(state, fields = {}) {
  const live = venueOrder(fields);
  const pendingId = Number(state.db.insertPendingAction({ action: action.action,
    instrument_name: live.instrument_name, amount: Number(live.amount), price: Number(live.limit_price) }).lastInsertRowid);
  state.db.updatePendingAction(pendingId, { status: 'resting' });
  state.db.insertRestingOrder({ order_id: live.order_id, pending_action_id: pendingId,
    instrument_name: live.instrument_name, action: action.action, direction: live.direction,
    amount: Number(live.amount), limit_price: Number(live.limit_price), filled_amount: 0, filled_value: 0,
    exit_intent: Object.hasOwn(fields, 'exit_intent') ? fields.exit_intent : 'profit_capture' });
  state.venue.push(live);
  state.status.set(live.order_id, live);
  return live;
}

function runtime(t, { seed = true } = {}) {
  const db = memoryDatabase(t);
  const botData = { putNetBought: 123, putBudgetForCycle: 500, putUnspentBuyLimit: 17 };
  db.saveBotState(botData);
  const state = { db, botData, venue: [], status: new Map(), events: [], alerts: [],
    positions: [{ instrument_name: instrumentName, direction: 'short', amount: 5, avg_entry_price: 20 }],
    onOpen: null, onStatus: null, onCancel: null, onPositions: null };
  const bindings = {
    db, botData, compareRestingExitOrder, VENUE_AMOUNT_DECIMALS: 2,
    require(name) { assert.equal(name, './bot/order-accounting'); return accounting; },
    notifyOrderLifecycle: payload => { state.alerts.push(payload); },
    fetchOpenOrders: async options => {
      assert.equal(options.throwOnError, true);
      state.events.push('open');
      if (state.onOpen) await state.onOpen();
      return state.venue.map(order => ({ ...order }));
    },
    fetchOrderStatus: async id => {
      state.events.push(`status:${id}`);
      const record = state.onStatus ? await state.onStatus(id) : state.status.get(id);
      return record ? { ...record } : null;
    },
    cancelOrder: async (id, name) => {
      assert.equal(name, instrumentName);
      state.events.push(`cancel:${id}`);
      if (state.onCancel) return state.onCancel(id);
      state.venue = state.venue.filter(order => order.order_id !== id);
      state.status.set(id, { ...state.status.get(id), order_status: 'cancelled' });
      return { cancelled: true };
    },
    fetchPositions: async options => {
      assert.equal(options.throwOnError, true);
      state.events.push('positions');
      return state.onPositions ? state.onPositions() : state.positions.map(position => ({ ...position }));
    },
  };
  // Compile actual selected declarations only. No bot startup, credentials,
  // timers or network clients run; SQLite and order accounting remain real.
  const names = ['ACTION_POLICY', 'getActionPolicy', 'getCloseablePositionForExit', 'parseMaybeJsonObject',
    'floorOrderAmountToVenuePrecision', 'accountExitOrderObservation', 'readFreshExitOrderSnapshot', 'prepareExitOrderReplacement'];
  const execute = vm.compileFunction(`${names.map(name => declaration(source, name)).join('\n')}
    return { readFreshExitOrderSnapshot, prepareExitOrderReplacement };`, Object.keys(bindings));
  Object.assign(state, execute(...Object.values(bindings)));
  if (seed) seedOrder(state);
  return state;
}

function desired(fields = {}) {
  return { ...action, direction: 'buy', amount: 5, limit_price: 3,
    order_type: 'post_only', exit_intent: 'profit_capture', ...fields };
}
const stored = state => state.db.db.prepare('SELECT * FROM resting_orders ORDER BY order_id').all();
const fills = state => state.db.db.prepare('SELECT filled_amount,total_value,fill_price FROM orders ORDER BY id').all();
const cancels = state => state.events.filter(event => event.startsWith('cancel:'));
async function review(state) {
  const snapshot = await state.readFreshExitOrderSnapshot(action);
  return Object.freeze(snapshot.orders.map(order => Object.freeze({ ...order })));
}
function prepare(state, reviewedOrders, order = desired()) {
  return state.prepareExitOrderReplacement({ action, reviewedOrders, desiredOrder: order });
}

test('actual exit snapshot reconciles a reviewed partial fill once and preserves an immutable review baseline', async t => {
  const state = runtime(t);
  Object.assign(state.venue[0], { filled_amount: '1', average_price: '4' });
  const snapshot = await state.readFreshExitOrderSnapshot(action);
  assert.equal(snapshot.orders[0].filled_amount, '1');
  assert.equal(snapshot.orders[0].exit_intent, 'profit_capture');
  assert.ok(Number.isFinite(Date.parse(snapshot.observedAt)));
  await state.readFreshExitOrderSnapshot(action);
  assert.deepEqual(fills(state), [{ filled_amount: 1, total_value: 4, fill_price: 4 }]);
  assert.equal(stored(state)[0].filled_value, 4);
  assert.equal(state.db.loadBotState().put_net_bought, 123);
  assert.equal(state.alerts.length, 1);
  assert.equal(state.alerts[0].stage, 'partial_fill');
  assert.equal(state.alerts[0].filledAmount, 1);
  assert.equal(state.alerts[0].totalValue, 4);
});

for (const [name, corrupt] of [
  ['untracked order', state => { state.venue.push(venueOrder({ order_id: 'foreign-exit' })); }],
  ['duplicate venue identity', state => { state.venue.push({ ...state.venue[0] }); }],
  ['missing venue identity', state => { delete state.venue[0].order_id; }],
  ['terminal row in open snapshot', state => { state.venue[0].order_status = 'cancelled'; }],
  ['absent order with unknown status', state => { state.venue = []; state.status.clear(); }],
  ['absent order that status reports open', state => { state.venue = []; }],
]) {
  test(`actual exit snapshot defers ${name} and retains its reservation`, async t => {
    const state = runtime(t);
    corrupt(state);
    await assert.rejects(state.readFreshExitOrderSnapshot(action));
    assert.equal(stored(state)[0].status, 'open');
    assert.equal(stored(state)[0].amount, 5);
    assert.equal(cancels(state).length, 0);
  });
}

for (const [name, acknowledgement] of [['failed cancellation', null], ['cancellation ACK without terminal status', { cancelled: true }]]) {
  test(`${name} cannot authorize replacement while the venue order remains open`, async t => {
    const state = runtime(t);
    const reviewed = await review(state);
    state.onCancel = async () => acknowledgement;
    const result = await prepare(state, reviewed);
    assert.equal(result.allowed, false);
    assert.match(result.reason, /still open|deferred/);
    assert.deepEqual(cancels(state), ['cancel:old-exit']);
    assert.equal(stored(state)[0].status, 'open');
    assert.equal(state.events.includes('positions'), false);
    assert.deepEqual(fills(state), []);
  });
}

test('a cancellation transport exception retains accounting and never reads a replacement position', async t => {
  const state = runtime(t);
  const reviewed = await review(state);
  state.onCancel = async () => { throw new Error('fixture cancel timeout'); };
  await assert.rejects(prepare(state, reviewed), /fixture cancel timeout/);
  assert.equal(stored(state)[0].status, 'open');
  assert.equal(state.events.includes('positions'), false);
});

test('an ACK followed by missing terminal evidence retains the reservation', async t => {
  const state = runtime(t);
  const reviewed = await review(state);
  state.onCancel = async () => { state.venue = []; state.status.clear(); return { cancelled: true }; };
  const result = await prepare(state, reviewed);
  assert.equal(result.allowed, false);
  assert.match(result.reason, /unknown/);
  assert.equal(stored(state)[0].status, 'open');
  assert.equal(state.events.includes('positions'), false);
});

for (const [positionAmount, expectedAmount] of [[4, 2], [1.239, 1.23]]) {
  test(`review-time and cancellation-time fills are booked once, subtracted from the approved size and capped to ${expectedAmount}`, async t => {
    const state = runtime(t);
    Object.assign(state.venue[0], { filled_amount: '1', average_price: '4' });
    const reviewed = await review(state);
    Object.assign(state.venue[0], { filled_amount: '2', average_price: '3.75' });
    state.onCancel = async id => {
      state.venue = [];
      state.status.set(id, venueOrder({ filled_amount: '3', average_price: '3.5', order_status: 'cancelled' }));
      state.positions[0].amount = positionAmount;
      return { cancelled: true };
    };
    const result = await prepare(state, reviewed, desired({ amount: 4 }));
    assert.equal(result.allowed, true);
    assert.equal(result.amount, expectedAmount);
    assert.equal(result.positions[0].amount, positionAmount);
    assert.deepEqual(fills(state), [
      { filled_amount: 1, total_value: 4, fill_price: 4 },
      { filled_amount: 1, total_value: 3.5, fill_price: 3.5 },
      { filled_amount: 1, total_value: 3, fill_price: 3 },
    ]);
    assert.equal(reviewed[0].filled_amount, '1');
    assert.equal(stored(state)[0].filled_amount, 3);
    assert.equal(stored(state)[0].filled_value, 10.5);
    assert.equal(stored(state)[0].status, 'cancelled');
    assert.equal(state.db.loadBotState().put_net_bought, 123);
    assert.deepEqual(state.alerts.map(alert => [alert.stage, alert.filledAmount, alert.totalValue]),
      [['partial_fill', 1, 4], ['partial_fill', 1, 3.5], ['executed', 1, 3]]);
    const before = fills(state);
    const repeated = await prepare(state, reviewed, desired({ amount: 4 }));
    assert.equal(repeated.amount, expectedAmount);
    assert.deepEqual(fills(state), before);
    assert.equal(state.alerts.length, 3);
    assert.deepEqual(cancels(state), ['cancel:old-exit']);
  });
}

test('an exit that finished during review is accounted once without another cancellation', async t => {
  const state = runtime(t);
  const reviewed = await review(state);
  state.venue = [];
  state.status.set('old-exit', venueOrder({ filled_amount: '2', average_price: '3.5', order_status: 'cancelled' }));
  state.positions[0].amount = 3;
  const result = await prepare(state, reviewed);
  assert.equal(result.allowed, true);
  assert.equal(result.amount, 3);
  assert.deepEqual(cancels(state), []);
  assert.deepEqual(fills(state), [{ filled_amount: 2, total_value: 7, fill_price: 3.5 }]);
});

test('post-review fills reduce the approved cap before an apparently equivalent resting remainder can be kept', async t => {
  const state = runtime(t);
  const reviewed = await review(state);
  Object.assign(state.venue[0], { filled_amount: '2', average_price: '4' });
  const result = await prepare(state, reviewed, desired({ amount: 3, limit_price: 4 }));
  assert.equal(result.allowed, true);
  assert.equal(result.amount, 1, 'two fills after review leave only one of the three approved contracts');
  assert.deepEqual(cancels(state), ['cancel:old-exit']);
  assert.equal(stored(state)[0].status, 'cancelled');
  assert.deepEqual(fills(state), [{ filled_amount: 2, total_value: 8, fill_price: 4 }]);
});

test('fills consuming the reviewed approval leave no replacement quantity even if another position remains', async t => {
  const state = runtime(t);
  const reviewed = await review(state);
  state.venue = [];
  state.status.set('old-exit', venueOrder({ filled_amount: '5', average_price: '3', order_status: 'filled' }));
  state.positions[0].amount = 4;
  const result = await prepare(state, reviewed, desired({ amount: 2 }));
  assert.equal(result.allowed, false);
  assert.match(result.reason, /No approved closeable/);
  assert.deepEqual(fills(state), [{ filled_amount: 5, total_value: 15, fill_price: 3 }]);
});

test('a newly tracked exit appearing during review blocks replacement before cancellation', async t => {
  const state = runtime(t);
  const reviewed = await review(state);
  seedOrder(state, { order_id: 'new-exit' });
  const result = await prepare(state, reviewed);
  assert.equal(result.allowed, false);
  assert.match(result.reason, /changed during review/);
  assert.deepEqual(cancels(state), []);
});

test('a foreign untracked exit appearing during review blocks replacement before cancellation', async t => {
  const state = runtime(t);
  const reviewed = await review(state);
  state.venue.push(venueOrder({ order_id: 'foreign-exit' }));
  await assert.rejects(prepare(state, reviewed), /Untracked exit order/);
  assert.deepEqual(cancels(state), []);
});

for (const tracked of [false, true]) {
  test(`a new ${tracked ? 'tracked' : 'foreign'} exit after cancellation prevents overlapping replacement`, async t => {
    const state = runtime(t);
    const reviewed = await review(state);
    state.onCancel = async id => {
      state.venue = [];
      state.status.set(id, venueOrder({ order_status: 'cancelled' }));
      if (tracked) seedOrder(state, { order_id: 'new-exit' });
      else state.venue.push(venueOrder({ order_id: 'foreign-exit' }));
      return { cancelled: true };
    };
    if (tracked) {
      const result = await prepare(state, reviewed);
      assert.equal(result.allowed, false);
      assert.match(result.reason, /remains on the venue/);
    } else await assert.rejects(prepare(state, reviewed), /Untracked exit order/);
    assert.equal(state.events.includes('positions'), false);
    assert.equal(stored(state).find(order => order.order_id === 'old-exit').status, 'cancelled');
  });
}

test('an equivalent exit is kept without cancellation', async t => {
  const state = runtime(t);
  const reviewed = await review(state);
  const result = await prepare(state, reviewed, desired({ limit_price: 4 }));
  assert.equal(result.allowed, false);
  assert.match(result.reason, /Equivalent exit/);
  assert.deepEqual(cancels(state), []);
});

test('unknown original exit intent cannot authorize cancellation and replacement', async t => {
  const state = runtime(t, { seed: false });
  seedOrder(state, { exit_intent: null });
  const reviewed = await review(state);
  const result = await prepare(state, reviewed);
  assert.equal(result.allowed, false);
  assert.match(result.reason, /unresolved/);
  assert.deepEqual(cancels(state), []);
});

test('an empty reviewed book permits only a freshly verified closeable quantity', async t => {
  const state = runtime(t, { seed: false });
  state.positions[0].amount = 2.129;
  const result = await prepare(state, []);
  assert.equal(result.allowed, true);
  assert.equal(result.amount, 2.12);
  assert.deepEqual(state.events, ['open', 'open', 'positions']);
  assert.deepEqual(fills(state), []);
});

test('missing live positions do not fall back to a reviewed quantity', async t => {
  const state = runtime(t);
  const reviewed = await review(state);
  state.positions = [];
  const result = await prepare(state, reviewed);
  assert.equal(result.allowed, false);
  assert.match(result.reason, /No approved closeable/);
});

test('failure while accounting cancellation fills rolls back the ledger and reservation', async t => {
  const state = runtime(t);
  const reviewed = await review(state);
  state.onCancel = async id => {
    state.venue = [];
    state.status.set(id, venueOrder({ filled_amount: '1', average_price: '3', order_status: 'cancelled' }));
    return { cancelled: true };
  };
  state.db.db.exec("CREATE TRIGGER reject_exit_fill BEFORE INSERT ON orders BEGIN SELECT RAISE(ABORT, 'fixture accounting failure'); END");
  await assert.rejects(prepare(state, reviewed), /fixture accounting failure/);
  assert.equal(stored(state)[0].status, 'open');
  assert.equal(stored(state)[0].filled_amount, 0);
  assert.deepEqual(fills(state), []);
  assert.equal(state.db.loadBotState().put_net_bought, 123);
  assert.equal(state.events.includes('positions'), false);
  assert.deepEqual(state.alerts, []);
});
