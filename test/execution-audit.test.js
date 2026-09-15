const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const { test } = require('node:test');
const Database = require('better-sqlite3');

const root = path.join(__dirname, '..');
const source = fs.readFileSync(path.join(root, 'script.js'), 'utf8');
const quiet = { log() {}, warn() {}, error() {} };
const putName = 'ETH-20261127-1600-P';
const callName = 'ETH-20261127-4000-C';
const declarations = new Map();

// Load the actual declarations without evaluating script.js startup, credentials,
// timers or network clients. Compile each complete declaration, never a copied
// implementation of the behavior under test.
function declaration(name) {
  if (declarations.has(name)) return declarations.get(name);
  const marker = new RegExp(`^const ${name} =`, 'm').exec(source);
  assert.ok(marker, `Production declaration ${name} exists`);
  const lines = source.slice(marker.index).split('\n');
  let candidate = '';
  for (const line of lines) {
    candidate += `${line}\n`;
    try {
      new vm.Script(candidate);
      declarations.set(name, candidate);
      return candidate;
    } catch (error) {
      if (!(error instanceof SyntaxError)) throw error;
    }
  }
  throw new Error(`Cannot extract ${name}`);
}

function production(names, dependencies = {}) {
  const imported = {};
  for (const filename of ['order-pricing.js', 'trade-policy.js']) {
    const modulePath = path.join(root, 'bot', filename);
    if (fs.existsSync(modulePath)) Object.assign(imported, require(modulePath));
  }
  const sandbox = {
    ...imported,
    console: quiet,
    process: { env: {} },
    Date,
    ...dependencies,
  };
  const context = vm.createContext(sandbox);
  const declarationsSource = names.map((name) => {
    if (new RegExp(`^const ${name} =`, 'm').test(source)) return declaration(name);
    assert.ok(Object.hasOwn(imported, name), `Production declaration or module export ${name} exists`);
    return '';
  }).join('\n');
  vm.runInContext(`${declarationsSource}\nObject.assign(globalThis, { ${names.join(', ')} });`, context);
  return sandbox;
}

// Execute the real schema, migrations, prepared statements and transactions in
// native SQLite. The constructor override guarantees no production DB is opened.
function memoryDatabase(t) {
  const filename = path.join(root, 'bot/db.js');
  const localRequire = createRequire(filename);
  const sandbox = {
    __dirname: path.dirname(filename),
    process: { env: { DATA_DIR: os.tmpdir() } },
    console: quiet,
    module: { exports: {} },
    require(name) {
      if (name === 'better-sqlite3') {
        return function InMemoryDatabase() { return new Database(':memory:'); };
      }
      return localRequire(name);
    },
  };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), sandbox, { filename });
  const db = sandbox.module.exports;
  t.after(() => db.close());
  return db;
}

const pricing = [
  'getInstrumentPriceStep', 'roundToStep', 'getStepDecimals',
  'normalizePriceToStep', 'normalizeOrderPriceForVenue',
  'avoidRoundNumberRestingPrice', 'computePostOnlyRetryPrice', 'formatPostOnlyContext',
];
const actionPolicy = ['ACTION_POLICY', 'getActionPolicy', 'isEntryAction', 'isReduceOnlyExitAction', 'isRestingOrderType'];
const exitPolicy = [
  'getRuleIntent', 'getBuybackIntent', 'getPutExitIntent',
  'getCloseablePositionForExit', 'getSyntheticExitIntent',
  'isSyntheticRestingExitIntentAllowed', 'getExistingRestingExitOrder',
  'getSyntheticReduceOnlyPreflight',
];

function runtime(t, overrides = {}) {
  const db = memoryDatabase(t);
  const botData = { putNetBought: 0, putBudgetForCycle: 500, putUnspentBuyLimit: 0 };
  db.saveBotState(botData);
  const accounting = require('../bot/order-accounting');
  const names = [
    ...pricing, ...actionPolicy, ...exitPolicy,
    'parseExpiryFromInstrument', 'computeDteFromInstrumentName', 'computeCurrentValues',
    'getRuleEvaluationValues', 'evaluateConditions', 'parseMaybeJsonObject',
    'isThreatManagementBuybackCriteria', 'isBuybackProfitCaptureCondition',
    'getBuybackProfitCaptureCondition', 'conditionPasses', 'getBuybackCaptureGate', 'getSellPutProtectionGate',
    'floorOrderAmountToVenuePrecision', 'formatVenueOrderAmount', 'isVenueOrderAmountTradable',
    'extractOrderRecord', 'getOrderTrades', 'ordersRoughlyMatch', 'inferActionFromOpenOrder',
    'executeOrder', 'manageOpenOrders', 'summarizeReservedEntryCapacity',
  ];
  for (const optional of ['reconcileTrackedRestingOrder', 'cancelTrackedRestingOrder', 'getRestingExitInvalidReason']) {
    if (new RegExp(`^const ${optional} =`, 'm').test(source)) names.push(optional);
  }
  const sandbox = production(names, {
    db,
    botData,
    ...accounting,
    require: createRequire(path.join(root, 'script.js')),
    DERIVE_ACCOUNT_ADDRESS: 'fixture-account',
    CALL_BUYBACK_PROFIT_THRESHOLD: 80,
    PUT_ROLL_DTE_THRESHOLD: 25,
    PUT_MONETIZATION_PROFIT_THRESHOLD: 1000,
    VENUE_AMOUNT_DECIMALS: 2,
    VENUE_MIN_ORDER_AMOUNT: 0.1,
    persistCycleState: () => db.saveBotState(botData),
    sendTelegram() {},
    notifyOrderLifecycle() {},
    fetchFreshTickerForInstrument: async () => null,
    fetchPositions: async () => [],
    fetchOpenOrders: async () => [],
    fetchOrderStatus: async () => null,
    cancelOrder: async () => null,
    fetchSubaccount: async () => ({}),
    evaluateSellCallRetryMargin: async () => ({ allowed: true }),
    buildRollingOptionValueContext: () => ({}),
    validateRestingBuyPutEntryOrder: () => ({ valid: true }),
    validateRestingSellCallEntryOrder: () => ({ valid: true }),
    ...overrides,
  });
  return sandbox;
}

function instrument(name = putName) {
  return {
    instrument_name: name,
    price_step: 0.1,
    base_asset_address: 'fixture-address',
    base_asset_sub_id: 'fixture-sub-id',
    option_details: { option_type: name.endsWith('-P') ? 'put' : 'call', strike: 1600 },
  };
}

function venueOrder(fields = {}) {
  return {
    order_id: 'order-1',
    instrument_name: putName,
    direction: 'buy',
    amount: '5',
    filled_amount: '0',
    average_price: '0',
    limit_price: '10',
    order_status: 'open',
    creation_timestamp: Date.now(),
    ...fields,
  };
}

function seedResting(state, fields = {}) {
  const order = venueOrder(fields);
  const action = fields.action || (order.instrument_name.endsWith('-P') ? 'buy_put' : 'buyback_call');
  const pendingId = Number(state.db.insertPendingAction({
    action, instrument_name: order.instrument_name, amount: Number(order.amount), price: Number(order.limit_price),
  }).lastInsertRowid);
  state.db.updatePendingAction(pendingId, { status: 'confirmed' });
  state.db.insertRestingOrder({
    ...order,
    action,
    pending_action_id: pendingId,
    ...fields,
    amount: Number(order.amount),
    limit_price: Number(order.limit_price),
    filled_amount: Number(order.filled_amount),
  });
  return { order, pendingId };
}

function orders(state) {
  return state.db.db.prepare('SELECT * FROM orders ORDER BY id').all();
}

function pending(state, id) {
  return state.db.db.prepare('SELECT * FROM pending_actions WHERE id = ?').get(id);
}

function statusRuntime(post) {
  return production([
    'extractOrderRecord', 'extractOrderRecords', 'stringifyApiError',
    'fetchOrderHistoryRecord', 'fetchOrderStatus',
  ], {
    axios: { post },
    createWallet: () => ({}),
    signMessage: async () => 'fixture-signature',
    API_URL: { GET_ORDER_HISTORY: 'fixture:history' },
    SUBACCOUNT_ID: 'fixture-account',
    DERIVE_ACCOUNT_ADDRESS: 'fixture-wallet',
  });
}

test('resting exit position-read failure returns a terminal pre-send failure', async (t) => {
  let submitted = 0;
  const state = runtime(t, {
    fetchPositions: async (options) => {
      assert.equal(options.throwOnError, true);
      throw new Error('positions unavailable');
    },
    placeOrder: async () => { submitted++; throw new Error('must not submit'); },
  });
  const result = await state.executeOrder('buyback_call', callName, 1, 2, [instrument(callName)], 2500, 'post_only');
  assert.equal(result.failed, true);
  assert.match(result.reason, /positions unavailable/);
  assert.equal(submitted, 0);
  assert.equal(orders(state).length, 1);
  assert.equal(orders(state)[0].success, 0);
  assert.match(orders(state)[0].reason, /positions unavailable/);
});

for (const missingAsException of [false, true]) {
  test(`missing direct order and unavailable history remain unknown (${missingAsException ? 'HTTP exception' : 'API response'})`, async () => {
    const state = statusRuntime(async (url) => {
      if (url === 'fixture:history') throw new Error('history unavailable');
      const body = { error: { code: 11006, message: 'Does not exist' } };
      if (missingAsException) throw Object.assign(new Error('missing'), { response: { data: body } });
      return { data: body };
    });
    if (missingAsException) await assert.rejects(state.fetchOrderStatus('previously-tracked'), /history unavailable/);
    else assert.equal(await state.fetchOrderStatus('previously-tracked'), null);
  });
}

test('history recovery searches later pages and preserves recovered cumulative fills', async () => {
  const pages = [];
  const state = statusRuntime(async (url, body) => {
    if (url !== 'fixture:history') return { data: { error: { code: 11006, message: 'Does not exist' } } };
    pages.push(body.page);
    assert.ok(body.from_timestamp == null || body.from_timestamp === 0, 'recovery must not exclude older tracked orders with a seven-day cutoff');
    const records = body.page === 1
      ? Array.from({ length: 100 }, (_, index) => venueOrder({ order_id: `unrelated-${index}` }))
      : [venueOrder({ order_id: 'previously-tracked', order_status: 'filled', filled_amount: '5', average_price: '9' })];
    return { data: { result: { orders: records, has_more: body.page === 1 } } };
  });
  const status = await state.fetchOrderStatus('previously-tracked');
  assert.deepEqual(pages, [1, 2]);
  assert.equal(status.order_status, 'filled');
  assert.equal(Number(status.filled_amount), 5);
  assert.equal(Number(status.average_price), 9);
});

test('completed history search without the tracked order remains unknown', async () => {
  const state = statusRuntime(async (url) => url === 'fixture:history'
    ? { data: { result: { orders: [] } } }
    : { data: { error: { code: 11006, message: 'Does not exist' } } });
  assert.equal(await state.fetchOrderStatus('previously-tracked'), null);
});

test('partial GTC receipt retains its remainder and subsequent cumulative fills book only their deltas', async (t) => {
  const lifecycle = [];
  const state = runtime(t, { notifyOrderLifecycle: (event) => lifecycle.push(event) });
  const initial = venueOrder({ filled_amount: '1', average_price: '10' });
  state.placeOrder = async () => ({ result: { order: initial, trades: [{ trade_amount: '1', trade_price: '10' }] } });
  state.db.replaceActiveRules('fixture', [{ rule_type: 'entry', action: 'buy_put', criteria: {} }]);
  const result = await state.executeOrder('buy_put', putName, 5, 10, [instrument()], 2000, 'gtc');
  assert.equal(result.resting, true);
  assert.equal(state.db.getOpenRestingOrders().length, 1);
  assert.equal(state.db.getOpenRestingOrders()[0].filled_amount, 1);
  assert.equal(state.botData.putNetBought, 10);
  assert.equal(state.summarizeReservedEntryCapacity(state.db.getOpenRestingOrders()).putBudget, 40);
  assert.equal(lifecycle[0].stage, 'posted');
  assert.equal(lifecycle[0].orderId, 'order-1');

  state.fetchOpenOrders = async () => [venueOrder({ filled_amount: '2', average_price: '12' })];
  await state.manageOpenOrders({}, [], [instrument()], 2000);
  await state.manageOpenOrders({}, [], [instrument()], 2000);
  assert.equal(state.botData.putNetBought, 24);
  assert.equal(state.db.getOpenRestingOrders()[0].filled_amount, 2);
  assert.deepEqual(orders(state).map((row) => row.filled_amount), [1, 1]);
  assert.deepEqual(orders(state).map((row) => row.total_value), [10, 14]);
  assert.equal(lifecycle.length, 2, 'repeated cumulative status does not duplicate fill notifications');
  assert.equal(lifecycle[1].stage, 'partial_fill');
  assert.equal(lifecycle[1].filledAmount, 1);
  assert.equal(lifecycle[1].totalValue, 14);

  state.fetchOpenOrders = async () => [];
  state.fetchOrderStatus = async () => venueOrder({ order_status: 'cancelled', filled_amount: '3', average_price: '13' });
  await state.manageOpenOrders({}, [], [instrument()], 2000);
  await state.manageOpenOrders({}, [], [instrument()], 2000);
  assert.equal(state.botData.putNetBought, 39);
  assert.equal(state.db.loadBotState().put_net_bought, 39);
  assert.equal(state.db.getOpenRestingOrders().length, 0);
  assert.deepEqual(orders(state).map((row) => row.total_value), [10, 14, 15]);
  assert.equal(lifecycle.length, 3);
  assert.equal(lifecycle[2].status, 'cancelled');
  assert.equal(lifecycle[2].filledAmount, 1);
});

test('a receipt whose fills cannot be matched to trade receipts fails closed', async (t) => {
  const state = runtime(t, {
    placeOrder: async () => ({ result: { order: venueOrder({ order_status: 'filled', filled_amount: '5', average_price: '9' }) } }),
  });
  await assert.rejects(state.executeOrder('buy_put', putName, 5, 10, [instrument()], 2000, 'ioc'), /fills do not match/);
  assert.equal(state.botData.putNetBought, 0);
  assert.equal(state.db.getOpenRestingOrders().length, 0);
  assert.equal(orders(state).length, 0);
});

test('a zero-fill cancelled GTC receipt does not create a phantom open order', async (t) => {
  const state = runtime(t, {
    placeOrder: async () => ({ result: { order: venueOrder({ order_status: 'cancelled', cancel_reason: 'venue_cancelled' }) } }),
  });
  const result = await state.executeOrder('buy_put', putName, 5, 10, [instrument()], 2000, 'gtc');
  assert.notEqual(result.resting, true);
  assert.equal(state.db.getOpenRestingOrders().length, 0);
  assert.equal(state.botData.putNetBought, 0);
});

test('failed orphan-entry cancellation accounts observed fills and keeps the remaining reservation', async (t) => {
  let cancellations = 0;
  const state = runtime(t, {
    cancelOrder: async () => { cancellations++; return null; },
    fetchOrderStatus: async () => null,
  });
  const { order, pendingId } = seedResting(state, { creation_timestamp: Date.now() - 9 * 3600000 });
  assert.equal(state.db.getActiveRules().length, 0, 'No active rule backs this entry; age alone does not invalidate it');
  state.fetchOpenOrders = async () => [{ ...order, filled_amount: '1', average_price: '10' }];
  await assert.rejects(state.manageOpenOrders({}, [], [instrument()], 2000), /Cancellation.*failed/);
  assert.equal(cancellations, 1);
  assert.equal(state.botData.putNetBought, 10);
  assert.equal(state.db.getOpenRestingOrders()[0].filled_amount, 1);
  assert.equal(state.summarizeReservedEntryCapacity(state.db.getOpenRestingOrders()).putBudget, 40);
  assert.notEqual(pending(state, pendingId).status, 'cancelled');
  assert.equal(orders(state).reduce((sum, row) => sum + row.total_value, 0), 10);
});

for (const action of ['buy_put', 'sell_call']) {
  test(`manager retains a valid tracked ${action} beyond eight hours and cancels it when its rule disappears`, async (t) => {
    const cancellations = [];
    const validations = [];
    const validate = ({ order }) => { validations.push(order.order_id); return { valid: true }; };
    const state = runtime(t, {
      validateRestingBuyPutEntryOrder: validate,
      validateRestingSellCallEntryOrder: validate,
      cancelOrder: async (id) => { cancellations.push(id); return { success: true }; },
    });
    const { order, pendingId } = seedResting(state, {
      action,
      instrument_name: action === 'buy_put' ? putName : callName,
      direction: action === 'buy_put' ? 'buy' : 'sell',
      creation_timestamp: Date.now() - 9 * 3600000,
    });
    state.db.replaceActiveRules('fixture', [{ rule_type: 'entry', action, criteria: {} }]);
    state.fetchOpenOrders = async () => [order];

    await state.manageOpenOrders({}, [], [instrument(order.instrument_name)], 2000);
    assert.deepEqual(validations, [order.order_id], 'Old tracked entries still receive current economic validation');
    assert.deepEqual(cancellations, [], 'Age alone preserves the valid order and its queue position');
    assert.equal(state.db.getOpenRestingOrders()[0].order_id, order.order_id);

    state.db.replaceActiveRules('fixture-withdrawn', []);
    await state.manageOpenOrders({}, [], [instrument(order.instrument_name)], 2000);
    assert.deepEqual(cancellations, [order.order_id], 'The age exemption does not protect an orphaned entry');
    assert.equal(state.db.getOpenRestingOrders().length, 1, 'Cancellation ACK still awaits terminal reconciliation');
    assert.notEqual(pending(state, pendingId).status, 'cancelled');
  });
}

test('cancellation races reconcile the final cumulative fill before releasing reservations', async (t) => {
  const terminal = venueOrder({ order_status: 'cancelled', filled_amount: '3', average_price: '13' });
  const state = runtime(t, {
    cancelOrder: async () => ({ result: { order: terminal } }),
    fetchOrderStatus: async () => terminal,
  });
  const { order, pendingId } = seedResting(state, { creation_timestamp: Date.now() - 9 * 3600000 });
  state.fetchOpenOrders = async () => [{ ...order, filled_amount: '1', average_price: '10' }];
  await state.manageOpenOrders({}, [], [instrument()], 2000);
  assert.equal(state.db.getOpenRestingOrders().length, 1, 'ACK alone preserves the reservation');
  state.fetchOpenOrders = async () => [];
  await state.manageOpenOrders({}, [], [instrument()], 2000);
  assert.equal(state.botData.putNetBought, 39);
  assert.equal(state.db.loadBotState().put_net_bought, 39);
  assert.equal(state.db.getOpenRestingOrders().length, 0);
  assert.equal(pending(state, pendingId).status, 'executed');
  assert.equal(orders(state).reduce((sum, row) => sum + row.filled_amount, 0), 3);
  assert.equal(orders(state).reduce((sum, row) => sum + row.total_value, 0), 39);
});

test('a cancellation acknowledgement without a final venue status preserves tracking', async (t) => {
  const state = runtime(t, { cancelOrder: async () => ({ success: true }), fetchOrderStatus: async () => null });
  const { order } = seedResting(state, { creation_timestamp: Date.now() - 9 * 3600000 });
  state.fetchOpenOrders = async () => [order];
  await state.manageOpenOrders({}, [], [instrument()], 2000);
  assert.equal(state.db.getOpenRestingOrders().length, 1);
  assert.equal(state.summarizeReservedEntryCapacity(state.db.getOpenRestingOrders()).putBudget, 50);
});

test('a different venue order with identical economics cannot replace the tracked order identity', async (t) => {
  const state = runtime(t);
  const { order } = seedResting(state);
  state.fetchOpenOrders = async () => [{ ...order, order_id: 'different-owner-order' }];
  await assert.rejects(state.manageOpenOrders({}, [], [instrument()], 2000), /Untracked venue order/);
  assert.equal(state.db.getOpenRestingOrders()[0].order_id, 'order-1');
  assert.equal(state.botData.putNetBought, 0);
  assert.equal(orders(state).length, 0);
});

for (const finalObservation of [null, venueOrder({ filled_amount: '1', average_price: '10' })]) {
  test(`absence from open-order list with ${finalObservation ? 'nonterminal' : 'unknown'} direct status retains the remainder`, async (t) => {
    const state = runtime(t, { fetchOrderStatus: async () => finalObservation });
    seedResting(state);
    if (finalObservation) await state.manageOpenOrders({}, [], [instrument()], 2000);
    else await assert.rejects(state.manageOpenOrders({}, [], [instrument()], 2000), /status unknown/);
    assert.equal(state.db.getOpenRestingOrders().length, 1);
    assert.equal(state.botData.putNetBought, finalObservation ? 10 : 0);
  });
}

test('sell retry pricing preserves the approved floor even when the bid is much lower', () => {
  const state = production(pricing);
  const plan = state.computePostOnlyRetryPrice('sell', { b: 90, a: 110 }, instrument(callName), 100);
  assert.ok(plan.retryPrice >= 100);
});

test('executor refreshes the book after post-only rejection and never submits below the approved sell floor', async (t) => {
  const placed = [];
  let tickerFetches = 0;
  const state = runtime(t, {
    fetchFreshTickerForInstrument: async () => (++tickerFetches === 1 ? { b: 90, a: 110 } : { b: 105, a: 120 }),
    placeOrder: async (...args) => {
      placed.push(args[3]);
      if (placed.length === 1) return { rejected_post_only: true, error: 'post_only would cross' };
      return { result: { order: venueOrder({ instrument_name: callName, direction: 'sell', limit_price: args[3] }) } };
    },
  });
  await state.executeOrder('sell_call', callName, 5, 100, [instrument(callName)], 2000, 'post_only');
  assert.equal(tickerFetches, 2);
  assert.equal(placed.length, 2);
  assert.ok(placed.every((price) => price >= 100));
  assert.ok(placed[1] > 105, 'retry rests above the refreshed best bid');
});

test('receipt persistence rolls back fills, order tracking and budget together on a database failure', async (t) => {
  const state = runtime(t);
  state.db.insertOrder = () => { throw new Error('fixture disk failure'); };
  const record = venueOrder({ filled_amount: '1', average_price: '10' });
  const { accountInitialReceipt } = require('../bot/order-accounting');
  assert.throws(() => accountInitialReceipt({
    db: state.db, botData: state.botData, action: 'buy_put', instrumentName: putName,
    amount: 5, price: 10, orderType: 'gtc', pendingActionId: null,
    instrument: instrument(), spotPrice: 2000,
    order: { result: { order: record } }, record, trades: [{ trade_amount: '1', trade_price: '10' }],
  }), /fixture disk failure/);
  assert.equal(state.botData.putNetBought, 0);
  assert.equal(state.db.loadBotState().put_net_bought, 0);
  assert.equal(state.db.getOpenRestingOrders().length, 0);
  assert.equal(orders(state).length, 0);
});

test('passive exits remain valid only while live size, active intent and approved economics permit them', (t) => {
  const state = runtime(t);
  const order = venueOrder({ instrument_name: callName, limit_price: '10' });
  const tracked = { ...order, action: 'buyback_call', exit_intent: 'profit_capture' };
  const position = { instrument_name: callName, direction: 'short', amount: 5, avg_entry_price: 100 };
  const patient = {
    rule_type: 'exit', action: 'buyback_call', instrument_name: callName,
    criteria: {
      buyback_intent: 'profit_capture', max_buyback_price: 20,
      conditions: [{ field: 'unrealized_pnl_pct', op: 'gte', value: 80 }],
      condition_logic: 'all',
    },
  };
  const fixture = {
    order, tracked, positions: [position], activeRules: [patient], spotPrice: 3500,
    tickerMap: { [callName]: { a: 30, b: 10, option_pricing: { d: 0.6 } } },
  };
  assert.equal(state.getRestingExitInvalidReason(fixture), null, 'valid patient limit preserves capture');
  assert.match(state.getRestingExitInvalidReason({ ...fixture, positions: [] }), /closeable position/);
  assert.match(state.getRestingExitInvalidReason({ ...fixture, positions: [{ ...position, amount: 2 }] }), /closeable position/);
  assert.match(state.getRestingExitInvalidReason({ ...fixture, tickerMap: {} }), /current quote/);
  const threat = { ...patient, criteria: {
    buyback_intent: 'threat_management', allow_below_profit_floor: true,
    conditions: [{ field: 'delta', op: 'gte', value: 0.5 }],
  } };
  assert.match(state.getRestingExitInvalidReason({ ...fixture, activeRules: [patient, threat] }), /urgent exit/);
  const tighter = { ...patient, criteria: { ...patient.criteria, max_buyback_price: 9 } };
  assert.match(state.getRestingExitInvalidReason({ ...fixture, activeRules: [tighter] }), /bounds/);
});

test('manager refreshes live positions and cancels a passive exit after the position disappears', async (t) => {
  const cancelled = [];
  const positionReads = [];
  const state = runtime(t, {
    fetchPositions: async (options) => { positionReads.push(options); return []; },
    cancelOrder: async (id) => { cancelled.push(id); return { success: true }; },
  });
  const { order } = seedResting(state, { instrument_name: callName, action: 'buyback_call', exit_intent: 'profit_capture' });
  state.db.replaceActiveRules('fixture', [{
    rule_type: 'exit', action: 'buyback_call', instrument_name: callName,
    criteria: { buyback_intent: 'profit_capture', conditions: [{ field: 'unrealized_pnl_pct', op: 'gte', value: 80 }] },
  }]);
  state.fetchOpenOrders = async () => [order];
  await state.manageOpenOrders({ [callName]: { a: 30, b: 10 } }, [{
    instrument_name: callName, direction: 'short', amount: 5, avg_entry_price: 100,
  }], [instrument(callName)], 3500);
  assert.equal(positionReads.length, 1);
  assert.equal(positionReads[0].throwOnError, true);
  assert.deepEqual(cancelled, ['order-1']);
  assert.equal(state.db.getOpenRestingOrders().length, 1, 'cancellation awaits final status before releasing capacity');
});

test('incremental observation persistence rolls back cumulative fills and budgets on a database failure', (t) => {
  const state = runtime(t);
  seedResting(state);
  const tracked = state.db.getOpenRestingOrders()[0];
  state.db.insertOrder = () => { throw new Error('fixture disk failure'); };
  const { accountRestingObservation } = require('../bot/order-accounting');
  assert.throws(() => accountRestingObservation({
    db: state.db, botData: state.botData, tracked,
    live: venueOrder({ filled_amount: '1', average_price: '10' }),
  }), /fixture disk failure/);
  assert.equal(state.botData.putNetBought, 0);
  assert.equal(state.db.loadBotState().put_net_bought, 0);
  assert.equal(state.db.getOpenRestingOrders()[0].filled_amount, 0);
  assert.equal(tracked.filled_amount, 0);
  assert.equal(orders(state).length, 0);
});

test('an unknown submission persists its request and blocks another executor send', async (t) => {
  let sends = 0;
  const state = runtime(t, {
    placeOrder: async (...args) => {
      sends++;
      args[9]({ instrument_name: args[0], amount: args[1], direction: args[2], limit_price: args[3], nonce: 'fixture-nonce' });
      return { placement_error: 'timeout after send' };
    },
  });
  await assert.rejects(state.executeOrder('buy_put', putName, 5, 10, [instrument()], 2000, 'gtc'), /outcome unknown/);
  const submission = state.db.db.prepare('SELECT * FROM execution_submissions').get();
  assert.equal(submission.status, 'unknown');
  assert.equal(JSON.parse(submission.request_json).nonce, 'fixture-nonce');
  await assert.rejects(state.executeOrder('buy_put', putName, 5, 10, [instrument()], 2000, 'gtc'), /accounting recovery/);
  assert.equal(sends, 1);
  await assert.rejects(state.manageOpenOrders({}, [], [instrument()], 2000), /accounting recovery/);
});

test('acknowledged submission is released only when its fill accounting transaction commits', async (t) => {
  const state = runtime(t);
  const accounting = require('../bot/order-accounting');
  const submissionId = accounting.beginSubmission(state.db, null, { instrument_name: putName, nonce: 'fixture-nonce',
    subaccount_id: 7, direction: 'buy', amount: 5, limit_price: 10, time_in_force: 'gtc', action: 'buy_put' });
  const record = venueOrder({ nonce: 'fixture-nonce', subaccount_id: 7, filled_amount: '1', average_price: '10' });
  const order = { result: { order: record, trades: [{ trade_id: 'trade-1', order_id: record.order_id,
    subaccount_id: 7, instrument_name: putName, direction: 'buy', trade_amount: '1', trade_price: '10' }] } };
  accounting.noteSubmission(state.db, submissionId, order);
  const payload = {
    db: state.db, botData: state.botData, action: 'buy_put', instrumentName: putName,
    amount: 5, price: 10, orderType: 'gtc', pendingActionId: null, instrument: instrument(),
    spotPrice: 2000, order, record, trades: order.result.trades, submissionId,
  };
  const insertOrder = state.db.insertOrder;
  state.db.insertOrder = () => { throw new Error('fixture disk failure'); };
  assert.throws(() => accounting.accountInitialReceipt(payload), /fixture disk failure/);
  assert.equal(state.db.db.prepare('SELECT status FROM execution_submissions WHERE id = ?').get(submissionId).status, 'acknowledged');
  assert.throws(() => accounting.assertNoUnresolvedSubmission(state.db), /accounting recovery/);
  assert.equal(state.db.getOpenRestingOrders().length, 0);
  assert.equal(state.botData.putNetBought, 0);

  state.db.insertOrder = insertOrder;
  accounting.accountInitialReceipt(payload);
  assert.doesNotThrow(() => accounting.assertNoUnresolvedSubmission(state.db));
  assert.equal(state.db.db.prepare('SELECT status FROM execution_submissions WHERE id = ?').get(submissionId).status, 'accounted');
  assert.equal(state.botData.putNetBought, 10);
  assert.equal(state.db.getOpenRestingOrders()[0].filled_amount, 1);
  assert.throws(() => accounting.accountInitialReceipt(payload), /already accounted/);
  assert.equal(state.botData.putNetBought, 10);
  assert.equal(state.db.loadBotState().put_net_bought, 10);
  assert.equal(orders(state).length, 1);
});

test('venue placement journals before sending, blocks persistence failures and classifies zero-liquidity IOC responses', async () => {
  const events = [];
  const state = production([
    ...pricing, 'floorOrderAmountToVenuePrecision', 'formatVenueOrderAmount',
    'stringifyApiError', 'isIocNoLiquidityError', 'extractOrderRecord', 'placeOrder',
  ], {
    Buffer,
    VENUE_AMOUNT_DECIMALS: 2,
    createWallet: () => ({ address: 'fixture-wallet', signingKey: { sign: () => ({ serialized: 'fixture-signature' }) } }),
    signMessage: async () => 'fixture-header-signature',
    encodeTradeData: () => '0x00',
    encoder: { encode: () => '0x00' },
    ethers: { keccak256: () => '0x00' },
    SUBACCOUNT_ID: 1,
    ACTION_TYPEHASH: '0x00',
    TRADE_MODULE_ADDRESS: 'fixture-module',
    DERIVE_ACCOUNT_ADDRESS: 'fixture-account',
    DOMAIN_SEPARATOR: '0x00',
    API_URL: { PLACE_ORDER: 'fixture:place' },
    axios: { post: async (_url, request) => {
      events.push(['send', request]);
      return { data: { result: { order: venueOrder() } } };
    } },
  });
  await state.placeOrder(putName, 5, 'buy', 10, 'fixture-asset', 'fixture-sub-id', false, 'gtc', instrument(),
    (request) => events.push(['persist', request]));
  assert.deepEqual(events.map(([kind]) => kind), ['persist', 'send']);
  assert.equal(events[0][1], events[1][1]);
  assert.equal(events[0][1].signature, 'fixture-signature');
  events.length = 0;
  const failed = await state.placeOrder(putName, 5, 'buy', 10, 'fixture-asset', 'fixture-sub-id', false, 'gtc', instrument(),
    () => { throw new Error('fixture persistence failure'); });
  assert.match(failed.placement_error, /persistence failure/);
  assert.equal(events.length, 0);
  state.axios.post = async () => {
    throw Object.assign(new Error('venue returned HTTP 400'), {
      response: { status: 400, data: { error: { code: 11009, message: 'zero liquidity for market or ioc/fok order' } } },
    });
  };
  const noLiquidity = await state.placeOrder(putName, 5, 'buy', 10, 'fixture-asset', 'fixture-sub-id', false, 'ioc', instrument(), () => {});
  assert.equal(noLiquidity.zero_fill_rejected, true);
  assert.equal(noLiquidity.placement_error, undefined);
});

for (const orderStatus of ['open', 'cancelled']) {
  test(`replaying a ${orderStatus} observation with a stale tracked copy cannot duplicate accounting`, (t) => {
    const state = runtime(t);
    seedResting(state);
    const stale = state.db.getOpenRestingOrders()[0];
    const live = venueOrder({ order_status: orderStatus, filled_amount: '1', average_price: '10' });
    const { accountRestingObservation } = require('../bot/order-accounting');
    accountRestingObservation({ db: state.db, botData: state.botData, tracked: { ...stale }, live });
    const replay = accountRestingObservation({ db: state.db, botData: state.botData, tracked: { ...stale }, live });
    assert.equal(replay.deltaAmount, 0);
    assert.equal(replay.deltaValue, 0);
    assert.equal(state.botData.putNetBought, 10);
    assert.equal(state.db.loadBotState().put_net_bought, 10);
    assert.equal(orders(state).length, 1);
    assert.equal(orders(state)[0].filled_amount, 1);
    assert.equal(state.db.db.prepare('SELECT filled_amount FROM resting_orders WHERE order_id = ?').get('order-1').filled_amount, 1);
  });
}

test('a conflicting observation after confirmed terminal accounting fails without rewriting fills', (t) => {
  const state = runtime(t);
  seedResting(state);
  const stale = state.db.getOpenRestingOrders()[0];
  const { accountRestingObservation } = require('../bot/order-accounting');
  accountRestingObservation({
    db: state.db, botData: state.botData, tracked: { ...stale },
    live: venueOrder({ order_status: 'cancelled', filled_amount: '1', average_price: '10' }),
  });
  assert.throws(() => accountRestingObservation({
    db: state.db, botData: state.botData, tracked: { ...stale },
    live: venueOrder({ order_status: 'cancelled', filled_amount: '2', average_price: '11' }),
  }), /terminal|conflict|already reconciled/i);
  assert.equal(state.botData.putNetBought, 10);
  assert.equal(state.db.loadBotState().put_net_bought, 10);
  assert.equal(orders(state).length, 1);
  assert.equal(state.db.getOpenRestingOrders().length, 0);
});

function recoveryFixture(t) {
  const state = runtime(t);
  const pendingId = Number(state.db.insertPendingAction({ action: 'buy_put', instrument_name: putName, amount: 5, price: 10 }).lastInsertRowid);
  state.db.updatePendingAction(pendingId, { status: 'confirmed' });
  state.botData.lastAdvisoryRun = 123456;
  state.botData.lastTradeReviewRun = 987654;
  state.db.saveBotState(state.botData);
  const request = {
    instrument_name: putName, subaccount_id: 7, account_address: 'fixture-account',
    nonce: 12345, direction: 'buy', amount: '5', limit_price: '10',
    time_in_force: 'gtc', action: 'buy_put', approved_limit_price: 10,
  };
  // Synthetic values, complete required V2 response fields from the official
  // pre-V3 SDK schemas (OrderResponseSchema and TradeResponseSchema):
  // https://github.com/derivexyz/derive-py/blob/e662f36f6b1ab326e97e595f131a1fa5cf6376a8/specs/openapi-spec.json
  const order = venueOrder({ subaccount_id: 7, nonce: request.nonce, filled_amount: '1', average_price: '10',
    cancel_reason: '', is_transfer: false, label: '', last_update_timestamp: Date.now(),
    max_fee: '10', mmp: false, order_fee: '0.01', order_type: 'limit', quote_id: null,
    signature: 'fixture-signature', signature_expiry_sec: 1800000000, signer: 'fixture-signer', time_in_force: 'gtc' });
  const trades = [{ trade_id: 'trade-1', order_id: order.order_id, subaccount_id: 7,
    instrument_name: putName, direction: 'buy', trade_amount: '1', trade_price: '10',
    expected_rebate: '0', extra_fee: '0', index_price: '2000', is_transfer: false,
    label: '', liquidity_role: 'taker', mark_price: '10', quote_id: null, realized_pnl: '0',
    realized_pnl_excl_fees: '0.01', rfq_id: null, timestamp: Date.now(), trade_fee: '0.01',
    transaction_id: 'fixture-transaction', tx_hash: null, tx_status: 'settled' }];
  const { beginSubmission } = require('../bot/order-accounting');
  const submissionId = beginSubmission(state.db, pendingId, request);
  return {
    state, request, pendingId,
    payload: { db: state.db, submissionId, accountId: '7', accountAddress: 'fixture-account', order, trades },
  };
}

test('recovery rebuilds a partial order from durable evidence once and preserves unrelated bot state', (t) => {
  const { state, pendingId, payload } = recoveryFixture(t);
  const accounting = require('../bot/order-accounting');
  accounting.noteSubmission(state.db, payload.submissionId, { result: { order: payload.order } });
  // Reload the recovery module to represent a new process using persisted state.
  delete require.cache[require.resolve('../bot/reconcile-execution')];
  const { reconcileSubmission } = require('../bot/reconcile-execution');
  const result = reconcileSubmission(payload);
  assert.equal(result.resting, true);
  assert.equal(state.db.getOpenRestingOrders()[0].filled_amount, 1);
  assert.equal(state.db.getOpenRestingOrders()[0].filled_value, 10);
  assert.equal(state.db.loadBotState().put_net_bought, 10);
  assert.equal(state.db.loadBotState().last_advisory_run, 123456);
  assert.equal(state.db.loadBotState().last_trade_review_run, 987654);
  assert.equal(pending(state, pendingId).status, 'resting');
  assert.doesNotThrow(() => accounting.assertNoUnresolvedSubmission(state.db));
  assert.throws(() => reconcileSubmission(payload), /already accounted/);
  assert.equal(orders(state).length, 1);
  assert.equal(state.db.loadBotState().put_net_bought, 10);
});

for (const [name, alter] of [
  ['account', (payload) => ({ ...payload, accountId: '8' })],
  ['account address', (payload) => ({ ...payload, accountAddress: 'another-account' })],
  ['nonce', (payload) => ({ ...payload, order: { ...payload.order, nonce: '54321' } })],
  ['instrument', (payload) => ({ ...payload, order: { ...payload.order, instrument_name: callName } })],
  ['trade order', (payload) => ({ ...payload, trades: [{ ...payload.trades[0], order_id: 'another-order' }] })],
  ['trade account', (payload) => ({ ...payload, trades: [{ ...payload.trades[0], subaccount_id: '8' }] })],
  ['duplicate trades', (payload) => ({ ...payload, trades: [payload.trades[0], payload.trades[0]] })],
  ['incomplete trades', (payload) => ({ ...payload, trades: [] })],
]) {
  test(`recovery rejects conflicting ${name} evidence without releasing the submission`, (t) => {
    const { state, payload } = recoveryFixture(t);
    const { reconcileSubmission } = require('../bot/reconcile-execution');
    assert.throws(() => reconcileSubmission(alter(payload)), /identity|nonce|conflict|trade/i);
    assert.equal(state.db.loadBotState().put_net_bought, 0);
    assert.equal(state.db.getOpenRestingOrders().length, 0);
    assert.equal(orders(state).length, 0);
    assert.equal(state.db.db.prepare('SELECT status FROM execution_submissions WHERE id = ?').get(payload.submissionId).status, 'unknown');
  });
}

test('recovery rejects a different order ID from the saved acknowledgement', (t) => {
  const { state, payload } = recoveryFixture(t);
  require('../bot/order-accounting').noteSubmission(state.db, payload.submissionId, {
    result: { order: { ...payload.order, order_id: 'acknowledged-order' } },
  });
  assert.throws(() => require('../bot/reconcile-execution').reconcileSubmission(payload), /saved acknowledgement/);
  assert.equal(orders(state).length, 0);
  assert.equal(state.db.loadBotState().put_net_bought, 0);
});

test('recovery evidence collection paginates both histories and refreshes the order by durable ID', async (t) => {
  const { request, payload } = recoveryFixture(t);
  const calls = [];
  const expectedTrades = Array.from({ length: 101 }, (_, i) => ({ ...payload.trades[0],
    trade_id: `order-trade-${i}`, trade_amount: i === 100 ? '0.5' : '0.005' }));
  const { collectRecoveryEvidence } = require('../bot/reconcile-execution');
  const evidence = await collectRecoveryEvidence({ request, read: async (method, params) => {
    calls.push([method, params.page]);
    assert.equal(params.subaccount_id, request.subaccount_id);
    if (method === 'get_order_history') {
      assert.equal(params.from_timestamp, 0);
      return { orders: params.page === 1
        ? Array.from({ length: 100 }, (_, i) => ({ order_id: `other-${i}`, nonce: `other-nonce-${i}` }))
        : [payload.order] };
    }
    if (method === 'get_order') {
      assert.equal(params.order_id, payload.order.order_id);
      return { ...payload.order };
    }
    assert.equal(method, 'get_trade_history');
    assert.equal(params.order_id, payload.order.order_id);
    assert.equal(params.instrument_name, request.instrument_name);
    return { trades: expectedTrades.slice((params.page - 1) * 100, params.page * 100) };
  } });
  assert.equal(evidence.order.order_id, payload.order.order_id);
  assert.deepEqual(evidence.trades, expectedTrades);
  assert.deepEqual(calls, [
    ['get_order_history', 1], ['get_order_history', 2], ['get_order', undefined],
    ['get_trade_history', 1], ['get_trade_history', 2],
  ]);
});

test('official V2 pagination stops an exactly full final trade page and recovers it once', async (t) => {
  const { state, request, payload } = recoveryFixture(t);
  const trades = Array.from({ length: 100 }, (_, i) => ({ ...payload.trades[0],
    trade_id: `filtered-trade-${i}`, trade_amount: '0.01' }));
  const calls = [];
  const { collectRecoveryEvidence, reconcileSubmission } = require('../bot/reconcile-execution');
  const evidence = await collectRecoveryEvidence({ request, read: async (method, params) => {
    calls.push(method);
    if (method === 'get_order') return payload.order;
    assert.equal(params.page, 1, 'V2 returns its last page again on page overflow');
    if (method === 'get_order_history') return { orders: [payload.order], pagination: { count: 1, num_pages: 1 } };
    assert.equal(params.order_id, payload.order.order_id);
    return { trades, pagination: { count: 100, num_pages: 1 } };
  } });
  reconcileSubmission({ ...payload, ...evidence });
  assert.ok(Math.abs(state.db.getOpenRestingOrders()[0].filled_amount - 1) < 1e-9);
  assert.ok(Math.abs(state.db.loadBotState().put_net_bought - 10) < 1e-9);
  assert.deepEqual(calls, ['get_order_history', 'get_order', 'get_trade_history']);
});

test('official V2 pagination stops an exactly full final order page without inventing absence evidence', async (t) => {
  const { request } = recoveryFixture(t);
  let calls = 0;
  const { collectRecoveryEvidence } = require('../bot/reconcile-execution');
  await assert.rejects(collectRecoveryEvidence({ request, read: async (method, params) => {
    calls++;
    assert.equal(method, 'get_order_history');
    assert.equal(params.page, 1);
    return { orders: Array.from({ length: 100 }, (_, i) => ({ order_id: `unrelated-${i}`, nonce: `nonce-${i}` })),
      pagination: { count: 100, num_pages: 1 } };
  } }), /absence cannot release/);
  assert.equal(calls, 1);
});

for (const [name, result] of [
  ['missing rows', { trades: [], pagination: { count: 1, num_pages: 1 } }],
  ['invalid count', { trades: [], pagination: { count: -1, num_pages: 0 } }],
  ['short intermediate page', { trades: [], pagination: { count: 101, num_pages: 2 } }],
  ['conflicting order filter', { trades: [{ trade_id: 'wrong-trade', order_id: 'another-order' }], pagination: { count: 1, num_pages: 1 } }],
]) {
  test(`recovery rejects trade history with ${name}`, async (t) => {
    const { state, request, payload } = recoveryFixture(t);
    const { collectRecoveryEvidence } = require('../bot/reconcile-execution');
    await assert.rejects(collectRecoveryEvidence({ request, read: async (method) => {
      if (method === 'get_order_history') return { orders: [payload.order] };
      if (method === 'get_order') return payload.order;
      return result;
    } }), /pagination|conflicting evidence/);
    assert.throws(() => require('../bot/order-accounting').assertNoUnresolvedSubmission(state.db), /accounting recovery/);
    assert.equal(state.db.loadBotState().put_net_bought, 0);
  });
}

test('missing recovery evidence cannot be interpreted as proof that an unknown submission was rejected', async (t) => {
  const { state, request, payload } = recoveryFixture(t);
  const { collectRecoveryEvidence } = require('../bot/reconcile-execution');
  await assert.rejects(collectRecoveryEvidence({ request, read: async () => ({ orders: [] }) }), /absence cannot release/);
  assert.equal(state.db.db.prepare('SELECT status FROM execution_submissions WHERE id = ?').get(payload.submissionId).status, 'unknown');
  assert.equal(state.db.loadBotState().put_net_bought, 0);
});

test('affirmative terminal history plus complete trades can recover a pruned direct order', async (t) => {
  const { state, request, payload } = recoveryFixture(t);
  const { collectRecoveryEvidence, reconcileSubmission } = require('../bot/reconcile-execution');
  const terminal = { ...payload.order, order_status: 'cancelled' };
  const evidence = await collectRecoveryEvidence({ request, read: async (method) => {
    if (method === 'get_order_history') return { orders: [terminal] };
    if (method === 'get_order') throw new Error('direct order pruned');
    assert.equal(method, 'get_trade_history');
    return { trades: payload.trades };
  } });
  reconcileSubmission({ ...payload, ...evidence });
  assert.equal(state.db.loadBotState().put_net_bought, 10);
  assert.equal(state.db.getOpenRestingOrders().length, 0);
  assert.equal(orders(state).length, 1);
  assert.doesNotThrow(() => require('../bot/order-accounting').assertNoUnresolvedSubmission(state.db));
});

test('an open history row cannot recover an order whose current status is unavailable', async (t) => {
  const { state, request, payload } = recoveryFixture(t);
  const { collectRecoveryEvidence } = require('../bot/reconcile-execution');
  await assert.rejects(collectRecoveryEvidence({ request, read: async (method) => {
    if (method === 'get_order_history') return { orders: [payload.order] };
    throw new Error('current order unavailable');
  } }), /current order unavailable/);
  assert.equal(state.db.db.prepare('SELECT status FROM execution_submissions WHERE id = ?').get(payload.submissionId).status, 'unknown');
  assert.equal(orders(state).length, 0);
});

for (const [name, alter] of [
  ['wrong nonce', payload => { payload.record.nonce = 'wrong'; }],
  ['missing nonce', payload => { delete payload.record.nonce; }],
  ['wrong account', payload => { payload.record.subaccount_id = '999'; }],
  ['missing account', payload => { delete payload.record.subaccount_id; }],
  ['wrong order price', payload => { payload.record.limit_price = '11'; }],
  ['missing instrument', payload => { delete payload.record.instrument_name; }],
  ['wrong trade order', payload => { payload.trades[0].order_id = 'other-order'; }],
  ['wrong trade account', payload => { payload.trades[0].subaccount_id = '999'; }],
  ['missing trade identity', payload => { delete payload.trades[0].trade_id; }],
  ['duplicate trade identity', payload => { payload.trades.push({ ...payload.trades[0] }); payload.record.filled_amount = '2'; }],
  ['conflicting cumulative value', payload => { payload.record.average_price = '11'; }],
]) {
  test(`live initial receipt with ${name} preserves its durable recovery block`, (t) => {
    const { state, payload: recovery } = recoveryFixture(t);
    const accounting = require('../bot/order-accounting');
    const payload = { db: state.db, botData: state.botData, action: 'buy_put', instrumentName: putName,
      amount: 5, price: 10, orderType: 'gtc', pendingActionId: null, instrument: instrument(), spotPrice: 2000,
      record: recovery.order, trades: recovery.trades, submissionId: recovery.submissionId };
    alter(payload);
    payload.order = { result: { order: payload.record, trades: payload.trades } };
    accounting.noteSubmission(state.db, recovery.submissionId, payload.order);
    assert.throws(() => accounting.accountInitialReceipt(payload), /identity|nonce|terms|value/i);
    assert.equal(state.db.loadBotState().put_net_bought, 0);
    assert.equal(state.db.getOpenRestingOrders().length, 0);
    assert.equal(orders(state).length, 0);
    assert.throws(() => accounting.assertNoUnresolvedSubmission(state.db), /accounting recovery/);
  });
}
