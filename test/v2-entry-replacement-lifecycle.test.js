'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const Database = require('better-sqlite3');
const { declaration, loadProduction } = require('./helpers/load-production');

const root = path.resolve(__dirname, '..');
const scriptPath = path.join(root, 'script.js');
const source = fs.readFileSync(scriptPath, 'utf8');
const putName = 'ETH-20301127-1600-P';
const callName = 'ETH-20301127-4000-C';
const quiet = { log() {}, warn() {}, error() {} };

function memoryDatabase(t) {
  const filename = path.join(root, 'bot/db.js');
  const localRequire = createRequire(filename);
  const sandbox = {
    __dirname: path.dirname(filename), module: { exports: {} }, console: quiet,
    process: { env: { DATA_DIR: os.tmpdir() } },
    require(name) {
      if (name === 'better-sqlite3') return function InMemoryDatabase() { return new Database(':memory:'); };
      return localRequire(name);
    },
  };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), sandbox, { filename });
  const db = sandbox.module.exports;
  t.after(() => db.close());
  return db;
}

function fixture(t, { actionName = 'buy_put', initialFillPrice = 8 } = {}) {
  const db = memoryDatabase(t);
  const isPutBuy = actionName === 'buy_put';
  const instrumentName = isPutBuy ? putName : callName;
  const botData = { putNetBought: isPutBuy ? initialFillPrice : 0, putBudgetForCycle: 100, putUnspentBuyLimit: 0 };
  db.saveBotState(botData);
  const pendingId = Number(db.insertPendingAction({
    action: actionName, instrument_name: instrumentName, amount: 5, price: 10,
    trigger_details: { preferred_order_type: 'gtc', advisor_limit_price: 10 },
  }).lastInsertRowid);
  db.updatePendingAction(pendingId, { status: 'resting', execution_result: { orderType: 'gtc' } });
  const reviewedOrder = {
    order_id: 'incumbent', pending_action_id: pendingId,
    action: actionName, instrument_name: instrumentName, direction: isPutBuy ? 'buy' : 'sell',
    amount: 5, filled_amount: 1, filled_value: initialFillPrice, average_price: initialFillPrice,
    limit_price: 10, approved_limit_price: 10, status: 'open', order_status: 'open',
    order_type: 'gtc', time_in_force: 'gtc', creation_timestamp: Date.now(),
  };
  db.insertRestingOrder(reviewedOrder);
  db.insertOrder({ action: actionName, success: true, instrument_name: instrumentName,
    pending_action_id: pendingId, intended_amount: 5, filled_amount: 1,
    fill_price: initialFillPrice, total_value: initialFillPrice });

  const state = {
    db, botData, reviewedOrder,
    action: { id: pendingId + 1, action: actionName, instrument_name: instrumentName },
    desiredOrder: { action: actionName, instrument_name: instrumentName, direction: reviewedOrder.direction,
      amount: 4, limit_price: isPutBuy ? 8 : 12, order_type: 'gtc' },
    venueOrders: [{ ...reviewedOrder }],
    terminal: null,
    operations: [],
    cancellations: [],
  };
  const pure = loadProduction(['getActionPolicy', 'isEntryAction', 'inferActionFromOpenOrder',
    'floorOrderAmountToVenuePrecision', 'isVenueOrderAmountTradable', 'parseMaybeJsonObject', 'getEntryOrderFilledValue']);
  const entryModule = path.join(root, 'bot/resting-entry-plan.js');
  const ctx = {
    ...pure,
    ...(fs.existsSync(entryModule) ? require(entryModule) : {}),
    require: createRequire(scriptPath), db, botData, console: quiet,
    notifyOrderLifecycle() {},
    fetchOpenOrders: async (options) => {
      assert.equal(options.throwOnError, true);
      state.operations.push('read-open');
      return state.venueOrders.map(order => ({ ...order }));
    },
    fetchOrderStatus: async (id) => {
      state.operations.push(`read-status:${id}`);
      return state.terminal && { ...state.terminal };
    },
    fetchPositions: async () => { state.operations.push('read-positions'); return []; },
    cancelOrder: async (id) => {
      state.operations.push(`cancel:${id}`);
      state.cancellations.push(id);
      if (state.onCancel) return state.onCancel(id);
      if (state.terminal && ['filled', 'cancelled', 'expired'].includes(state.terminal.order_status)) state.venueOrders = [];
      return { success: true };
    },
  };
  const names = ['readFreshEntryOrderSnapshot', 'prepareEntryOrderReplacement'];
  for (const name of ['accountEntryOrderObservation', 'accountExitOrderObservation']) {
    if (new RegExp(`^const ${name} =`, 'm').test(source)) names.unshift(name);
  }
  vm.createContext(ctx);
  vm.runInContext(`${names.map(name => declaration(source, name)).join('\n')}\nObject.assign(globalThis, { ${names.join(', ')} });`, ctx);
  state.snapshot = () => ctx.readFreshEntryOrderSnapshot(state.action);
  state.prepare = (overrides = {}) => ctx.prepareEntryOrderReplacement({
    action: state.action, reviewedOrder: state.reviewedOrder, desiredOrder: state.desiredOrder,
    approvedNotional: state.desiredOrder.amount * state.desiredOrder.limit_price,
    ...overrides,
  });
  state.orderRows = () => db.db.prepare('SELECT * FROM orders ORDER BY id').all();
  return state;
}

test('fresh entry snapshot books incremental put fills and preserves its remaining reservation', async (t) => {
  const state = fixture(t);
  state.venueOrders[0] = { ...state.reviewedOrder, filled_amount: 2, average_price: 9 };
  const snapshot = await state.snapshot();
  assert.equal(snapshot.orders.length, 1);
  assert.equal(snapshot.orders[0].filled_amount, 2);
  assert.equal(state.botData.putNetBought, 18);
  assert.equal(state.db.loadBotState().put_net_bought, 18);
  assert.equal(state.db.getOpenRestingOrders()[0].amount - state.db.getOpenRestingOrders()[0].filled_amount, 3);
  await state.snapshot();
  assert.equal(state.orderRows().length, 2, 'same cumulative receipt cannot duplicate its fill');
  assert.equal(state.botData.putNetBought, 18);
});

test('buy replacement subtracts both late filled quantity and actual late premium from approval', async (t) => {
  const state = fixture(t);
  state.venueOrders[0] = { ...state.reviewedOrder, filled_amount: 2, average_price: 9 };
  state.terminal = { ...state.reviewedOrder, order_status: 'cancelled', filled_amount: 3, average_price: 28 / 3 };
  const result = await state.prepare();
  assert.equal(result.allowed, true, result.reason);
  assert.equal(result.amount, 1.5, '(approved $32 minus $20 late fills) / $8 is tighter than the two-unit remainder');
  assert.deepEqual(state.cancellations, ['incumbent']);
  assert.equal(state.botData.putNetBought, 28);
  assert.equal(state.db.loadBotState().put_net_bought, 28);
  assert.equal(state.db.getOpenRestingOrders().length, 0);
  assert.equal(state.orderRows().reduce((sum, row) => sum + row.total_value, 0), 28);
});

test('late-fill notional reduction floors replacement quantity to the instrument amount step', async (t) => {
  const state = fixture(t);
  state.venueOrders[0] = { ...state.reviewedOrder, filled_amount: 2, average_price: 9 };
  state.terminal = { ...state.reviewedOrder, order_status: 'cancelled', filled_amount: 3, average_price: 28 / 3 };
  const result = await state.prepare({ instrument: { instrument_name: putName, amount_step: 0.2 } });
  assert.equal(result.allowed, true, result.reason);
  assert.equal(result.amount, 1.4, '$12 of remaining approval permits 1.5 units before the 0.2 venue step');
  assert.ok(result.amount * state.desiredOrder.limit_price <= 12);
  assert.equal(state.botData.putNetBought, 28);
});

test('sell-call replacement subtracts late fills without using an exit closeable-position cap', async (t) => {
  const state = fixture(t, { actionName: 'sell_call', initialFillPrice: 10 });
  state.terminal = { ...state.reviewedOrder, order_status: 'cancelled', filled_amount: 3, average_price: 11 };
  const result = await state.prepare();
  assert.equal(result.allowed, true, result.reason);
  assert.equal(result.amount, 2);
  assert.equal(state.botData.putNetBought, 0);
  assert.equal(state.orderRows().reduce((sum, row) => sum + row.filled_amount, 0), 3);
});

test('cancellation acknowledgement without terminal status refuses replacement and keeps the reservation', async (t) => {
  const state = fixture(t);
  const result = await state.prepare();
  assert.equal(result.allowed, false);
  assert.match(result.reason, /unknown|terminal|defer/i);
  assert.deepEqual(state.cancellations, ['incumbent']);
  assert.equal(state.db.getOpenRestingOrders().length, 1);
  assert.equal(state.botData.putNetBought, 8);
});

test('an order still open after a cancellation request remains tracked', async (t) => {
  const state = fixture(t);
  state.terminal = { ...state.reviewedOrder };
  const result = await state.prepare();
  assert.equal(result.allowed, false);
  assert.match(result.reason, /open|defer/i);
  assert.equal(state.db.getOpenRestingOrders().length, 1);
  assert.equal(state.botData.putNetBought, 8);
});

test('an exactly equivalent entry is preserved without a cancellation', async (t) => {
  const state = fixture(t);
  state.desiredOrder.limit_price = 10;
  const result = await state.prepare();
  assert.equal(result.allowed, false);
  assert.match(result.reason, /equivalent|already resting|keep/i);
  assert.equal(state.cancellations.length, 0);
  assert.equal(state.db.getOpenRestingOrders().length, 1);
});

test('another order identity appearing during review prevents cancellation and replacement', async (t) => {
  const state = fixture(t);
  const laterOrder = { ...state.reviewedOrder, order_id: 'arrived-during-review', pending_action_id: null,
    filled_amount: 0, filled_value: 0, average_price: 0 };
  state.db.insertRestingOrder(laterOrder);
  state.venueOrders.push(laterOrder);
  const result = await state.prepare();
  assert.equal(result.allowed, false);
  assert.match(result.reason, /changed|new|identity|review/i);
  assert.equal(state.cancellations.length, 0);
  assert.equal(state.db.getOpenRestingOrders().length, 2);
  assert.equal(state.botData.putNetBought, 8);
});

test('an entry appearing after cancellation blocks the replacement after terminal accounting', async (t) => {
  const state = fixture(t);
  state.terminal = { ...state.reviewedOrder, order_status: 'cancelled', filled_amount: 2, average_price: 9 };
  const newOrder = { ...state.reviewedOrder, order_id: 'arrived-after-cancel', pending_action_id: null,
    filled_amount: 0, filled_value: 0, average_price: 0 };
  state.onCancel = () => {
    state.db.insertRestingOrder(newOrder);
    state.venueOrders = [newOrder];
    return { success: true };
  };
  const result = await state.prepare();
  assert.equal(result.allowed, false);
  assert.match(result.reason, /remains|appeared|changed|defer/i);
  assert.deepEqual(state.cancellations, ['incumbent']);
  assert.equal(state.db.getOpenRestingOrders().length, 1);
  assert.equal(state.db.getOpenRestingOrders()[0].order_id, 'arrived-after-cancel');
  assert.equal(state.botData.putNetBought, 18);
  assert.equal(state.db.loadBotState().put_net_bought, 18);
});

test('an entry replacement can proceed after cancellation when there is no existing position', async (t) => {
  const state = fixture(t);
  state.terminal = { ...state.reviewedOrder, order_status: 'cancelled' };
  const result = await state.prepare();
  assert.equal(result.allowed, true, result.reason);
  assert.equal(result.amount, 4);
  assert.equal(state.db.getOpenRestingOrders().length, 0);
});

test('a full fill during cancellation leaves no approved replacement quantity', async (t) => {
  const state = fixture(t);
  state.terminal = { ...state.reviewedOrder, order_status: 'filled', filled_amount: 5, average_price: 9.6 };
  const result = await state.prepare();
  assert.equal(result.allowed, false);
  assert.equal(state.db.getOpenRestingOrders().length, 0);
  assert.equal(state.botData.putNetBought, 48);
});

function economicsFixture(t, options = {}) {
  const state = fixture(t, options);
  const name = state.action.instrument_name;
  Object.assign(state, {
    validatorCalls: [], researchCalls: [], marketContextCalls: [], selectionScore: 90,
    baseValidation: { valid: true, score: 0.006, rawScore: 0.008, dte: 60 },
    margin: { initial_margin: 100, maintenance_margin: 200, subaccount_value: 1000, is_under_liquidation: false },
    ticker: { a: 10, b: 7, A: 11, B: 12, M: 8.5, I: 2500, option_pricing: { i: 0.7 } },
    positions: [{ instrument_name: name, direction: options.actionName === 'sell_call' ? 'short' : 'long', amount: 1 }],
    activeRule: { id: 11, action: state.action.action, criteria: { target_score: 0.004 } },
    triggerData: { target_score: 0.005 },
    instruments: [{ instrument_name: name, option_details: { strike: options.actionName === 'sell_call' ? 4000 : 1600 } }],
  });
  state.action.rule_id = state.activeRule.id;
  const pure = loadProduction(['summarizeReservedEntryCapacity', 'parseMaybeJsonObject',
    'getTickerImpliedVol', 'getLatestHourlyDeltaPct', 'isEntryAction', 'inferActionFromOpenOrder']);
  const bindings = {
    ...require('../bot/trade-policy'), ...pure, db: state.db, botData: state.botData,
    validateRestingBuyPutEntryOrder(input) {
      state.validatorCalls.push({ action: 'buy_put', input });
      return state.baseValidation;
    },
    validateRestingSellCallEntryOrder(input) {
      state.validatorCalls.push({ action: 'sell_call', input });
      return state.sellValidation ? state.sellValidation(input) : state.baseValidation;
    },
    buildLiveSellCallMarketContext(tickerMap, now, extra) {
      const context = { tickerMap, now, ...extra };
      state.marketContextCalls.push(context);
      return context;
    },
    classifyBuyPutEdge(input) {
      state.researchCalls.push(input);
      return { selection_score: state.selectionScore };
    },
  };
  const evaluate = vm.compileFunction(`${declaration(source, 'validateEntryReplacementEconomics')}\nreturn validateEntryReplacementEconomics;`,
    Object.keys(bindings), { filename: 'script.js (entry replacement economics)' })(...Object.values(bindings));
  state.economicsBindings = bindings;
  state.validateEconomics = evaluate;
  state.economics = (overrides = {}) => evaluate({
    action: state.action, activeRule: state.activeRule, triggerData: state.triggerData,
    desiredOrder: state.desiredOrder, reviewedOrder: state.reviewedOrder,
    ticker: state.ticker, tickerMap: {}, marginState: state.margin,
    positions: state.positions, instruments: state.instruments, spotPrice: 2500,
    ...overrides,
  });
  return state;
}

test('replacement economics releases only the reviewed put reservation while preserving other entries', (t) => {
  const state = economicsFixture(t, { initialFillPrice: 10 });
  state.db.insertRestingOrder({ ...state.reviewedOrder, order_id: 'other-put', pending_action_id: null,
    amount: 5, filled_amount: 0, filled_value: 0, average_price: 0 });
  state.db.insertRestingOrder({ ...state.reviewedOrder, order_id: 'other-call', pending_action_id: null,
    action: 'sell_call', instrument_name: callName, direction: 'sell', amount: 8,
    filled_amount: 0, filled_value: 0, average_price: 0 });
  const before = state.db.getOpenRestingOrders();
  const result = state.economics();
  assert.equal(result.allowed, true, result.reason);
  const { input } = state.validatorCalls[0];
  assert.equal(input.putBudgetRemaining, 40, '$100 cycle budget minus $10 spent minus the other $50 put reservation');
  assert.equal(input.order.amount, 4);
  assert.equal(input.order.filled_amount, 0, 'desired amount already represents unfilled replacement quantity');
  assert.equal(input.activeRules[0].criteria.target_score, 0.005, 'the stronger queued score requirement still applies');
  assert.equal(input.marginState, state.margin);
  assert.equal(input.positions, state.positions);
  assert.equal(input.tickerMap[putName], state.ticker);
  assert.equal(state.botData.putNetBought, 10);
  assert.deepEqual(state.db.getOpenRestingOrders(), before, 'pre-cancel validation does not release any actual reservation');
});

test('pre-cancel economics permits zero available initial margin for an existing entry in either direction', (t) => {
  for (const actionName of ['buy_put', 'sell_call']) {
    const state = economicsFixture(t, { actionName });
    state.margin.initial_margin = 0;
    const result = state.economics();
    assert.equal(result.allowed, true, `${actionName}: ${result.reason}`);
    for (const { input } of state.validatorCalls) {
      assert.equal(input.marginState, state.margin, 'existing exposure is not charged a second time');
      assert.equal(input.positions, state.positions);
      assert.equal(input.order.amount, 4, 'available margin cannot enlarge the approved remaining quantity');
    }
  }
});

test('negative, liquidating, or incomplete margin blocks replacement economics before valuation', (t) => {
  const state = economicsFixture(t);
  const unavailable = [
    null,
    { ...state.margin, initial_margin: -0.01 },
    { ...state.margin, is_under_liquidation: true },
    { ...state.margin, initial_margin: null },
    { ...state.margin, maintenance_margin: undefined },
    { ...state.margin, subaccount_value: 0 },
    { ...state.margin, is_under_liquidation: undefined },
  ];
  for (const marginState of unavailable) {
    const result = state.economics({ marginState });
    assert.equal(result.allowed, false, JSON.stringify(marginState));
    assert.match(result.reason, /margin/i);
  }
  assert.equal(state.validatorCalls.length, 0);
  assert.equal(state.researchCalls.length, 0);
});

test('persisted composite score floor cannot veto a put replacement and current research is still returned', (t) => {
  const state = economicsFixture(t);
  state.activeRule.criteria.min_edge_score = 80;
  state.selectionScore = 12;
  const tickerMap = { [putName]: { a: 99, b: 98 }, [callName]: { b: 15, a: 16 } };
  const accepted = state.economics({ tickerMap });
  assert.equal(accepted.allowed, true, accepted.reason);
  assert.equal(accepted.validation, state.baseValidation);
  assert.equal(accepted.research.selection_score, 12);
  const research = state.researchCalls[0];
  assert.deepEqual({ score: research.score, rawScore: research.rawScore, dte: research.dte,
    strike: research.strike, spotPrice: research.spotPrice, entryPrice: research.entryPrice,
    askPrice: research.askPrice, bidPrice: research.bidPrice, askAmount: research.askAmount,
    bidAmount: research.bidAmount, markPrice: research.markPrice, impliedVol: research.impliedVol }, {
    score: 0.006, rawScore: 0.008, dte: 60, strike: 1600, spotPrice: 2500,
    entryPrice: 8, askPrice: 10, bidPrice: 7, askAmount: 11, bidAmount: 12, markPrice: 8.5, impliedVol: 0.7,
  });
  assert.equal(research.marketContext.tickerMap[putName], state.ticker, 'fresh candidate quote replaces the stale map entry');
  assert.equal(research.marketContext.tickerMap[callName], tickerMap[callName]);
  state.selectionScore = 1;
  state.ticker = { ...state.ticker, a: 11 };
  const refreshed = state.economics({ tickerMap });
  assert.equal(refreshed.allowed, true, refreshed.reason);
  assert.equal(refreshed.research.selection_score, 1);
  assert.equal(state.researchCalls[1].askPrice, 11);
});

test('valid put replacement returns current research even without a composite score floor', (t) => {
  const state = economicsFixture(t);
  const result = state.economics();
  assert.equal(result.allowed, true, result.reason);
  assert.equal(state.researchCalls.length, 1);
  assert.equal(result.research.selection_score, state.selectionScore);
});

test('unchecked base economics cannot authorize a replacement or produce a positive research override', (t) => {
  const state = economicsFixture(t);
  state.baseValidation = { valid: true, unchecked: true, reason: 'Instrument unavailable' };
  const result = state.economics();
  assert.equal(result.allowed, false);
  assert.equal(result.reason, 'Instrument unavailable');
  assert.equal(state.researchCalls.length, 0);
});

test('sell-call replacement must still pass economics at the current executable bid', (t) => {
  const state = economicsFixture(t, { actionName: 'sell_call' });
  state.ticker.b = 11;
  state.sellValidation = input => input.order.limit_price === 11
    ? { valid: false, reason: 'Current bid misses the active call score' } : state.baseValidation;
  const result = state.economics();
  assert.equal(result.allowed, false);
  assert.match(result.reason, /current bid/i);
  assert.deepEqual(state.validatorCalls.map(({ input }) => input.order.limit_price), [12, 11]);
  for (const { input } of state.validatorCalls) {
    assert.equal(input.order.amount, 4);
    assert.equal(input.marginState, state.margin);
    assert.equal(input.positions, state.positions);
  }
});

test('missing executable put ask or call bid blocks replacement economics before valuation', (t) => {
  for (const actionName of ['buy_put', 'sell_call']) {
    const state = economicsFixture(t, { actionName });
    state.ticker[actionName === 'buy_put' ? 'a' : 'b'] = 0;
    const result = state.economics();
    assert.equal(result.allowed, false);
    assert.match(result.reason, /ask|bid/i);
    assert.equal(state.validatorCalls.length, 0);
  }
});

function finalReplacementFixture(t, { criteria = {}, triggerData = {} } = {}) {
  const state = economicsFixture(t);
  state.activeRule.criteria = { min_score: 0.004, target_score: 0.005, min_edge_score: 80, ...criteria };
  state.triggerData = { ...state.triggerData, ...triggerData };
  state.ticker.option_pricing.d = -0.06;
  state.db.updateRestingOrder('incumbent', 'cancelled', 1, 8);
  state.freshReads = { ticker: 0, margin: 0, positions: 0, book: 0 };
  state.finalPolicyCalls = [];
  const now = Date.parse('2030-11-27T08:00:00Z') - 60 * 24 * 60 * 60 * 1000;
  const { validateFinalOrderPolicy } = require('../bot/trade-policy');
  const bindings = {
    ...state.economicsBindings, ...loadProduction(['getFinalOrderPolicy']),
    Date: { now: () => now },
    db: { ...state.db, getActiveRules: () => [state.activeRule] },
    validateEntryReplacementEconomics: state.validateEconomics,
    fetchFreshTickerForInstrument: async name => { assert.equal(name, putName); state.freshReads.ticker++; return state.ticker; },
    fetchSubaccount: async () => { state.freshReads.margin++; return state.margin; },
    fetchPositions: async options => { assert.equal(options.throwOnError, true); state.freshReads.positions++; return state.positions; },
    fetchOpenOrders: async options => { assert.equal(options.throwOnError, true); state.freshReads.book++; return []; },
    getCallMarginDecision: () => null, // The put policy does not use call margin allocation.
    validateFinalOrderPolicy: input => {
      const result = validateFinalOrderPolicy(input);
      state.finalPolicyCalls.push({ input, result });
      return result;
    },
  };
  const createValidator = vm.compileFunction(`${declaration(source, 'createFinalOrderValidator')}\nreturn createFinalOrderValidator;`,
    Object.keys(bindings), { filename: 'script.js (entry replacement final callback)' })(...Object.values(bindings));
  state.validateFinal = createValidator({ action: state.action, instruments: state.instruments, spotPrice: 2500,
    triggerData: { ...state.triggerData, entry_replacement: true, replacement_order_snapshot: state.reviewedOrder },
    ruleCriteria: state.activeRule.criteria, orderType: 'post_only', tickerMap: { [putName]: { a: 99 } } });
  return state;
}

test('actual final policy permits replacement and maker retry despite a legacy composite score floor', async (t) => {
  const state = finalReplacementFixture(t);
  state.selectionScore = 12;
  const economics = state.economics();
  assert.equal(economics.allowed, true, economics.reason);
  assert.equal(economics.research.selection_score, 12);
  const initial = await state.validateFinal({ price: 8, amount: 4 });
  assert.equal(initial.allowed, true, initial.reason);
  state.selectionScore = 1;
  state.ticker = { ...state.ticker, a: 20, b: 6, I: 2600 };
  const retry = await state.validateFinal({ price: 7.9, amount: 4 });
  assert.equal(retry.allowed, true, retry.reason);
  assert.equal(state.finalPolicyCalls.length, 2);
  assert.ok(state.finalPolicyCalls.every(({ result }) => result.allowed));
  assert.deepEqual(state.freshReads, { ticker: 2, margin: 2, positions: 2, book: 2 });
  assert.deepEqual(state.finalPolicyCalls.map(({ input }) => [input.price, input.ticker.a, input.spotPrice]),
    [[8, 10, 2500], [7.9, 20, 2600]]);
});

test('legacy composite criteria cannot bypass the actual minimum PUT EDGE or queued target', async (t) => {
  const scenarios = [
    { criteria: { min_score: 0 }, expected: 'put_quote_failed' },
    { criteria: { min_score: 0.01 }, expected: 'put_price_failed' },
    { triggerData: { target_score: 0.01 }, expected: 'put_price_failed' },
  ];
  for (const scenario of scenarios) {
    const state = finalReplacementFixture(t, scenario);
    state.selectionScore = 100;
    const result = await state.validateFinal({ price: 8, amount: 4 });
    assert.equal(result.allowed, false);
    assert.equal(result.code, scenario.expected);
    assert.equal(state.finalPolicyCalls[0].result.code, scenario.expected, 'the real final policy rejects the order');
    assert.equal(state.freshReads.book, 0);
  }
});

test('actual final policy blocks a maker retry when fresh PUT EDGE deteriorates', async (t) => {
  const state = finalReplacementFixture(t);
  const initial = await state.validateFinal({ price: 8, amount: 4 });
  assert.equal(initial.allowed, true, initial.reason);
  state.ticker = { ...state.ticker, option_pricing: { ...state.ticker.option_pricing, d: -0.03 } };
  const retry = await state.validateFinal({ price: 7.9, amount: 4 });
  assert.equal(retry.allowed, false);
  assert.equal(retry.code, 'put_price_failed');
  assert.equal(state.finalPolicyCalls[1].input.ticker.option_pricing.d, -0.03);
  assert.equal(state.finalPolicyCalls[1].result.code, 'put_price_failed');
});

test('actual final policy retains spent budget and other put reservations when checking a replacement retry', async (t) => {
  for (const change of ['new-spend', 'other-reservation']) {
    const state = finalReplacementFixture(t);
    const initial = await state.validateFinal({ price: 8, amount: 4 });
    assert.equal(initial.allowed, true, initial.reason);
    if (change === 'new-spend') state.botData.putNetBought = 90;
    else state.db.insertRestingOrder({ ...state.reviewedOrder, order_id: 'other-put', pending_action_id: null,
      amount: 7, filled_amount: 0, filled_value: 0, average_price: 0 });
    const retry = await state.validateFinal({ price: 7.9, amount: 4 });
    assert.equal(retry.allowed, false, change);
    assert.equal(retry.code, 'put_budget_failed', change);
    assert.equal(state.finalPolicyCalls[1].input.putBudgetRemaining, change === 'new-spend' ? 10 : 22);
    assert.equal(state.finalPolicyCalls[1].result.code, 'put_budget_failed');
  }
});

test('actual final policy still requires complete and positive safe margin on every retry', async (t) => {
  const scenarios = [
    { change: () => null, expected: 'margin_unavailable' },
    { change: margin => ({ ...margin, maintenance_margin: undefined }), expected: 'margin_unavailable' },
    { change: margin => ({ ...margin, initial_margin: 0 }), expected: 'unsafe_margin' },
    { change: margin => ({ ...margin, initial_margin: -1 }), expected: 'unsafe_margin' },
    { change: margin => ({ ...margin, is_under_liquidation: true }), expected: 'unsafe_margin' },
  ];
  for (const scenario of scenarios) {
    const state = finalReplacementFixture(t);
    const initial = await state.validateFinal({ price: 8, amount: 4 });
    assert.equal(initial.allowed, true, initial.reason);
    state.margin = scenario.change(state.margin);
    const retry = await state.validateFinal({ price: 7.9, amount: 4 });
    assert.equal(retry.allowed, false);
    assert.equal(retry.code, scenario.expected);
    assert.equal(state.finalPolicyCalls[1].result.code, scenario.expected);
    assert.equal(state.freshReads.margin, 2);
  }
});
