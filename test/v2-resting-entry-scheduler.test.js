'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const vm = require('node:vm');
const { declaration, SCRIPT_SOURCE, loadProduction } = require('./helpers/load-production');
const planning = require('../bot/resting-entry-plan');
const now = Date.parse('2030-09-15T08:00:00Z');
class Clock extends Date {
  constructor(...args) { super(...(args.length ? args : [now])); }
  static now() { return now; }
}
const quiet = { log() {}, warn() {}, error() {} };
const actual = (name, bindings) => vm.compileFunction(
  `${declaration(SCRIPT_SOURCE, name)}; return ${name};`, Object.keys(bindings),
)(...Object.values(bindings));

function fixture(action = 'buy_put', options = {}) {
  const put = action === 'buy_put';
  const name = put ? 'ETH-20301114-1800-P' : 'ETH-20300923-3000-C';
  const competitor = put ? 'ETH-20301114-1700-P' : 'ETH-20300923-3200-C';
  const order = { order_id: 'incumbent', instrument_name: name, action, rule_id: 11,
    direction: put ? 'buy' : 'sell', amount: 2.5, filled_amount: 0, filled_value: 0,
    limit_price: put ? 9.9 : 10.1, time_in_force: 'post_only', status: 'open', order_status: 'open',
    creation_timestamp: now - 10 * 3_600_000, ...options.order };
  const rule = { id: 11, rule_type: 'entry', action, is_active: 1, preferred_order_type: 'post_only', budget_limit: 100,
    criteria: { option_type: put ? 'P' : 'C', delta_range: put ? [-0.12, -0.02] : [0.04, 0.12],
      dte_range: put ? [45, 78] : [5, 12], min_score: put ? 0.005 : 65, ...(put ? {} : { min_bid: 4 }) } };
  const instrument = { instrument_name: name, price_step: 0.1, options: { amount_step: 0.01 },
    option_details: { option_type: put ? 'P' : 'C', strike: put ? 1800 : 3000 } };
  const ticker = { b: put ? 9.9 : 10, a: put ? 20 : 11, M: put ? 15 : 10.5, I: 2500,
    A: 10, B: 10, option_pricing: { d: put ? -0.05 : 0.1, i: 0.6 } };
  if (options.replace) {
    if (put) ticker.option_pricing.d = -0.055;
    else { ticker.b = 12; ticker.a = 13; }
  }
  const rules = options.rules ? options.rules(rule) : [rule];
  const inserted = [], decisions = [], events = [], reservations = [], validationInputs = [];
  const working = options.working || [];
  const botData = { putBudgetForCycle: 100, putUnspentBuyLimit: 0, putNetBought: 0, ...options.budget };
  const liveOrder = { ...order, ...options.liveOrder };
  const db = {
    getPendingActions(status) { events.push(`pending:${status}`); return working.filter(item => item.status === status); },
    insertPendingAction(row) { events.push('queue'); inserted.push(row); working.push({ ...row, status: 'pending' }); return { lastInsertRowid: 31 }; },
  };
  const pure = loadProduction(['validateRestingBuyPutEntryOrder', 'validateRestingSellCallEntryOrder',
    'parseMaybeJsonObject', 'summarizeReservedEntryCapacity', 'getTickerImpliedVol', 'classifyBuyPutEdge'], {
    bindings: { Date: Clock, botData, estimateDisplayedMarginUtilization: () => 0.1,
      getEffectiveCallExposureCapPct: () => 0.45 },
  });
  const bindings = {
    ...pure, Date: Clock, db, botData, console: quiet,
    require(name) {
      assert.equal(name, './bot/resting-entry-plan');
      return { ...planning, buildRestingEntryPlan: args => planning.buildRestingEntryPlan({ ...args, nowMs: now }) };
    },
    getOpenRestingEntryOrders: () => [order, ...reservations],
    readFreshEntryOrderSnapshot: async input => {
      events.push('snapshot');
      assert.deepEqual(input, { action, instrument_name: name });
      if (options.bookError) throw new Error('Fresh entry book unavailable');
      if (options.afterSnapshot) options.afterSnapshot({ botData, reservations, liveOrder });
      return { orders: options.noLiveOrder ? [] : options.multiple ? [liveOrder, { ...liveOrder, order_id: 'other' }] : [liveOrder],
        observedAt: new Clock().toISOString() };
    },
    fetchFreshTickerForInstrument: async requested => {
      events.push('fresh-quote');
      assert.equal(requested, name, 'Maintenance must refresh the incumbent, not the global winner');
      if (options.afterQuote) options.afterQuote({ botData, reservations, liveOrder });
      return options.missingQuote ? null : ticker;
    },
    validateRestingBuyPutEntryOrder: args => { validationInputs.push(args); return pure.validateRestingBuyPutEntryOrder(args); },
    validateRestingSellCallEntryOrder: args => { validationInputs.push(args); return pure.validateRestingSellCallEntryOrder(args); },
    getRecentRejectedAction: () => options.rejection || null,
    logRuleDecisionSafe: row => decisions.push(row),
    cancelOrder: () => assert.fail('Scheduling must not cancel an order before approval'),
    confirmAndExecutePending: () => assert.fail('Scheduling must not run reviewers or execute orders'),
  };
  const input = { entryRules: rules, instruments: [instrument],
    tickerMap: { [competitor]: { b: 100, a: 1, M: 50, option_pricing: { d: put ? -0.05 : 0.1 } } },
    spotPrice: 2500, positions: [], marginState: { initial_margin: 100, subaccount_value: 1000 },
    buyPutContext: { action_pressure: { signal: 'standing_patient_bid' } }, marketContext: {} };
  const run = () => actual('reassessRestingEntryOrders', bindings)(input);
  return { run, bindings, input, order, liveOrder, rule, rules, ticker, botData, inserted, decisions, events, validationInputs, name };
}

for (const action of ['buy_put', 'sell_call']) {
  test(`${action}: canonical incumbent survives a better global contract without queueing another review`, async () => {
    const f = fixture(action);
    const result = await f.run();
    const plan = result.plans.get('incumbent');
    assert.equal(plan.decision, 'keep', plan.reason);
    assert.equal(result.queuedCount, 0);
    assert.equal(result.blockedActions.size, 0);
    assert.equal(f.inserted.length, 0);
    assert.deepEqual(f.events, ['snapshot', 'fresh-quote']);
    assert.equal(f.decisions[0].reason_code, 'resting_entry_keep');
    assert.equal(f.decisions[0].selected_instrument, f.name);
    assert.equal(plan.desiredOrder.amount, 2.5);
    assert.equal(plan.desiredOrder.limit_price, f.order.limit_price);
  });

  test(`${action}: changed incumbent economics queue its concrete replacement without cancelling it`, async () => {
    const f = fixture(action, { replace: true });
    const result = await f.run();
    assert.equal(result.plans.get('incumbent').decision, 'replace', result.plans.get('incumbent').reason);
    assert.equal(result.queuedCount, 1);
    assert.equal(result.blockedActions.has(action), true);
    const candidate = f.inserted[0];
    assert.equal(candidate.instrument_name, f.name);
    assert.equal(candidate.rule_id, 11);
    assert.equal(candidate.trigger_details.entry_replacement, true);
    assert.equal(candidate.trigger_details.replacement_order_id, 'incumbent');
    assert.equal(candidate.trigger_details.replacement_order_snapshot.limit_price, f.order.limit_price);
    assert.equal(candidate.trigger_details.desired_entry_order.limit_price, candidate.price);
    assert.equal(candidate.trigger_details.advisor_limit_price, candidate.price);
    assert.equal(candidate.trigger_details.price_source, 'resting_entry_reassessment');
    assert.equal(f.order.status, 'open');
    assert.equal(f.decisions[0].pending_action_id, 31);
    assert.equal(candidate.price, action === 'buy_put' ? 10.9 : 12.1);
    assert.equal(candidate.amount, action === 'buy_put' ? 2.27 : 2.5);
  });
}

test('the current active rule has priority; an applicable successor replaces a withdrawn or invalid rule', async () => {
  const current = fixture('buy_put', { rules: rule => [{ ...rule, id: 22,
    criteria: { ...rule.criteria, min_score: 0.006 } }, rule] });
  const kept = await current.run();
  assert.equal(kept.plans.get('incumbent').decision, 'keep');
  assert.equal(current.validationInputs[0].activeRules[0].id, 11);
  assert.equal(current.inserted.length, 0);
  for (const withdrawn of [false, true]) {
    const successor = fixture('buy_put', { rules: rule => [
      ...(withdrawn ? [] : [{ ...rule, is_active: 0 }]),
      { ...rule, id: 22, criteria: { ...rule.criteria, min_score: 0.006 } },
    ] });
    const result = await successor.run();
    assert.equal(result.queuedCount, 1, result.plans.get('incumbent').reason);
    assert.equal(successor.inserted[0].rule_id, 22);
    assert.equal(successor.inserted[0].price, 8.3);
  }
});

test('pending or confirmed same-action work prevents duplicate replacement reviews across instruments', async () => {
  for (const action of ['buy_put', 'sell_call']) for (const status of ['pending', 'confirmed']) {
    const f = fixture(action, { replace: true, working: [{ id: 99, action, status, instrument_name: 'another-contract', rule_id: 999 }] });
    const result = await f.run();
    assert.equal(result.queuedCount, 0);
    assert.equal(result.blockedActions.has(action), true);
    assert.equal(result.plans.get('incumbent').decision, 'unresolved');
    assert.match(result.plans.get('incumbent').reason, /already pending/);
    assert.equal(f.order.status, 'open');
  }
  const otherSide = fixture('buy_put', { replace: true,
    working: [{ action: 'sell_call', status: 'pending', instrument_name: 'another-contract' }] });
  assert.equal((await otherSide.run()).queuedCount, 1);
});

test('missing or inconsistent fresh book and missing quote defer both actions without queueing or cancellation', async () => {
  for (const action of ['buy_put', 'sell_call']) for (const issue of ['bookError', 'multiple', 'missingQuote']) {
    const f = fixture(action, { replace: true, [issue]: true });
    const result = await f.run();
    assert.equal(result.queuedCount, 0);
    assert.equal(result.blockedActions.has(action), true);
    assert.equal(result.plans.get('incumbent').decision, 'unresolved');
    assert.equal(f.inserted.length, 0);
    assert.equal(f.validationInputs.length, 0);
    assert.equal(f.order.status, 'open');
  }
});

test('fresh own fills and a newly observed other reservation cap the remaining put authority', async () => {
  const f = fixture('buy_put', { replace: true, order: { amount: 5, filled_amount: 1 },
    liveOrder: { amount: 5, filled_amount: 2 }, budget: { putBudgetForCycle: 50 },
    afterSnapshot: ({ botData }) => { botData.putNetBought = 20; },
    // Another reservation becomes visible after the fresh book was read, while
    // the quote request was in flight. Its budget must remain reserved.
    afterQuote: ({ reservations }) => reservations.push({ order_id: 'new-reservation', action: 'buy_put',
      instrument_name: 'ETH-20301114-1600-P', amount: 2, filled_amount: 0, limit_price: 5 }),
  });
  const result = await f.run();
  assert.equal(result.queuedCount, 1, result.plans.get('incumbent').reason);
  const queued = f.inserted[0];
  assert.equal(queued.amount, 1.83);
  assert.ok(Math.abs(queued.trigger_details.entry_approved_notional - 29.7) < 1e-9);
  assert.equal(queued.trigger_details.replacement_order_snapshot.filled_amount, 2);
  assert.equal(f.validationInputs[0].putBudgetRemaining, 20);
  assert.ok(queued.amount * queued.price + 10 <= 30);
  assert.ok(queued.amount <= 3);
});

test('terminal reconciliation and recent rejection do not create additional replacement authority', async () => {
  const terminal = fixture('buy_put', { noLiveOrder: true });
  const finished = await terminal.run();
  assert.equal(finished.queuedCount, 0);
  assert.equal(finished.plans.size, 0);
  assert.deepEqual(terminal.events, ['snapshot']);
  const rejected = fixture('buy_put', { replace: true, rejection: { reason: 'Price ceiling rejected' } });
  const result = await rejected.run();
  assert.equal(result.queuedCount, 0);
  assert.equal(result.blockedActions.has('buy_put'), true);
  assert.match(result.plans.get('incumbent').reason, /Price ceiling rejected/);
});

function integratedFixture(replace) {
  const f = fixture('buy_put', { replace });
  f.bindings.db.getActiveRulesByType = type => type === 'entry' ? f.rules : [];
  f.bindings.db.getRecentPendingActions = () => [];
  f.bindings.db.getLastExecutedAction = () => { f.events.push('cooldown'); return new Clock(now - 300_000).toISOString(); };
  const scheduler = actual('reassessRestingEntryOrders', f.bindings);
  const bindings = { ...f.bindings,
    reassessRestingEntryOrders: scheduler,
    fetchSubaccount: async () => f.input.marginState,
    buildLiveSellCallMarketContext: () => ({}),
    getBestCurrentBuyPutEdgeCandidate: () => null,
    getBestCurrentSellCallCandidate: () => null,
    buildRollingOptionValueContext: () => f.input.buyPutContext,
    formatCooldownMinutes: () => '55m',
  };
  const tickerMap = new Proxy(f.input.tickerMap, {
    ownKeys(target) { f.events.push('global-candidate-scan'); return Reflect.ownKeys(target); },
  });
  f.evaluate = () => actual('evaluateTradingRules', bindings)([], f.input.instruments, tickerMap, 2500);
  return f;
}

test('actual entry evaluation maintains a canonical incumbent before the new-entry cooldown gate', async () => {
  const f = integratedFixture(false);
  assert.equal(await f.evaluate(), 0);
  assert.ok(f.events.indexOf('fresh-quote') >= 0);
  assert.ok(f.events.indexOf('fresh-quote') < f.events.indexOf('cooldown'));
  assert.equal(f.events.includes('global-candidate-scan'), false);
  assert.deepEqual(f.decisions.map(row => row.reason_code), ['resting_entry_keep', 'action_cooldown']);
});

test('actual entry evaluation queues incumbent repricing despite cooldown and blocks global replacement competition', async () => {
  const f = integratedFixture(true);
  assert.equal(await f.evaluate(), 1);
  assert.equal(f.inserted.length, 1);
  assert.equal(f.events.includes('cooldown'), false);
  assert.equal(f.events.includes('global-candidate-scan'), false);
  assert.equal(f.inserted[0].instrument_name, f.name);
  assert.deepEqual(f.decisions.map(row => row.reason_code), ['resting_entry_reprice']);
});
