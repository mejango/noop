'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const vm = require('node:vm');
const { declaration, SCRIPT_SOURCE, loadProduction } = require('./helpers/load-production');
const planning = require('../bot/resting-exit-plan');
const name = 'ETH-20300918-2800-C';
const quiet = { log() {}, error() {}, warn() {} };

function actual(name, bindings) {
  return vm.compileFunction(`${declaration(SCRIPT_SOURCE, name)}; return ${name};`, Object.keys(bindings))(...Object.values(bindings));
}

function evaluationFixture({ price = 2.2, quantity = 8.26, existing = true, pending = false, filled = 0 } = {}) {
  const position = { instrument_name: name, direction: 'short', amount: quantity, avg_entry_price: 20 };
  const rule = { id: 1912, action: 'buyback_call', rule_type: 'exit', instrument_name: name,
    preferred_order_type: 'post_only', criteria: { buyback_intent: 'profit_capture', max_buyback_price: 3,
      conditions: [{ field: 'unrealized_pnl_pct', op: 'gte', value: 80 }], condition_logic: 'all' } };
  const restingOrder = { order_id: 'old-exit', action: 'buyback_call', instrument_name: name, direction: 'buy',
    amount: 8.26, filled_amount: filled, limit_price: 2.2, time_in_force: 'post_only', exit_intent: 'profit_capture',
    rule_id: 1850, creation_timestamp: Date.now() - 9 * 3600000 };
  const inserted = [], decisions = [], cancellations = [], calls = [];
  const db = {
    getActiveRulesByType: type => type === 'exit' ? [rule] : [],
    hasPendingOrConfirmedActionForRule: () => pending,
    hasPendingActionForRule: () => { throw new Error('Resting rows cannot block changed desired exits'); },
    getRecentPendingActions: () => [],
    insertPendingAction: row => { inserted.push(row); return { lastInsertRowid: 11010 }; },
  };
  const pure = loadProduction(['getCloseablePositionForExit', 'getSyntheticExitIntent',
    'normalizePreferredOrderType', 'hasPendingOrConfirmedActionForRule'], { bindings: { db } });
  const bindings = {
    ...pure, ...planning, db, console: quiet,
    logRuleDecisionSafe: value => decisions.push(value),
    getRuleEvaluationValues: () => ({ unrealized_pnl_pct: 90, mark_price: 4 }),
    getPatientBuybackPlan: () => ({ limitPrice: price, ceilingPrice: 3, priceReason: 'capture_floor_ceiling', capturePct: 89 }),
    refinePatientBuybackPlanPrice: plan => plan,
    getPatientSellPutPlan: () => null,
    evaluateExitConditions: () => true,
    getBuybackCaptureGate: () => ({ allowed: true }),
    getSellPutProtectionGate: () => ({ allowed: true }),
    readFreshExitOrderSnapshot: async () => { calls.push('orders'); return { orders: existing ? [restingOrder] : [], observedAt: Date.now() }; },
    fetchPositions: async options => { assert.equal(options.throwOnError, true); calls.push('positions'); return [position]; },
    getRecentRejectedAction: () => null,
    cancelOrder: (...args) => { cancellations.push(args); throw new Error('Evaluation must not cancel an unreviewed replacement'); },
    getBuybackIntent: () => 'profit_capture',
    getOpenRestingEntryOrders: () => [], fetchSubaccount: async () => null,
    buildLiveSellCallMarketContext: () => ({}), getBestCurrentBuyPutEdgeCandidate: () => null,
    getBestCurrentSellCallCandidate: () => null,
  };
  const evaluate = actual('evaluateTradingRules', bindings);
  const run = () => evaluate([position], [{ instrument_name: name, price_step: 0.1 }], { [name]: { a: 4, b: 1.8, M: 4 } }, 2500);
  return { run, inserted, decisions, cancellations, calls, restingOrder };
}

test('equivalent exit survives rule renewal and elapsed age without another pending action', async () => {
  const fixture = evaluationFixture();
  assert.equal(await fixture.run(), 0);
  assert.equal(fixture.inserted.length, 0);
  assert.equal(fixture.decisions[0].reason_code, 'existing_resting_exit');
  assert.deepEqual(fixture.calls, ['orders', 'positions']);
  assert.equal(fixture.cancellations.length, 0);
});

test('changed desired price queues an explicit replacement while the original remains live', async () => {
  const fixture = evaluationFixture({ price: 1.8 });
  assert.equal(await fixture.run(), 1);
  const candidate = fixture.inserted[0];
  assert.equal(candidate.price, 1.8);
  assert.equal(candidate.trigger_details.replacement_order_id, 'old-exit');
  assert.equal(candidate.trigger_details.replacement_order_snapshot.limit_price, 2.2);
  assert.equal(candidate.trigger_details.desired_exit_order.limit_price, 1.8);
  assert.equal(fixture.cancellations.length, 0);
});

test('partial fills compare the live closeable quantity to the remaining resting quantity', async () => {
  const fixture = evaluationFixture({ quantity: 6.26, filled: 2 });
  assert.equal(await fixture.run(), 0);
  assert.equal(fixture.decisions[0].reason_code, 'existing_resting_exit');
  assert.equal(fixture.inserted.length, 0);
});

test('missing live order permits a new candidate and pending work still deduplicates by rule', async () => {
  const empty = evaluationFixture({ existing: false });
  assert.equal(await empty.run(), 1);
  assert.equal(empty.inserted[0].trigger_details.replacement_order_id, null);
  const pending = evaluationFixture({ pending: true });
  assert.equal(await pending.run(), 0);
  assert.equal(pending.decisions[0].reason_code, 'pending_duplicate');
  assert.equal(pending.calls.length, 0);
});

test('existing exit with unknown route remains blocked before queueing a reviewer candidate', async () => {
  const fixture = evaluationFixture({ price: 1.8 });
  delete fixture.restingOrder.time_in_force;
  assert.equal(await fixture.run(), 0);
  assert.equal(fixture.decisions[0].reason_code, 'existing_exit_unresolved');
  assert.equal(fixture.cancellations.length, 0);
});

test('manager retains a valid patient exit beyond eight hours but still cancels invalid exits', async () => {
  const order = { order_id: 'old-exit', instrument_name: name, direction: 'buy', amount: 8.26,
    filled_amount: 0, limit_price: 2.2, order_status: 'open', creation_timestamp: Date.now() - 9 * 3600000 };
  const tracked = { ...order, action: 'buyback_call', status: 'open', exit_intent: 'profit_capture' };
  let invalid = false, cancels = 0;
  const bindings = {
    ...loadProduction(['isEntryAction']),
    process: { env: {} }, console: quiet,
    db: { getOpenRestingOrders: () => [tracked], getActiveRules: () => [{ id: 1912, rule_type: 'exit', action: 'buyback_call', instrument_name: name }] },
    require: () => ({ assertNoUnresolvedSubmission() {}, accountRestingObservation: () => ({ resting: true, deltaAmount: 0 }) }),
    fetchOpenOrders: async () => [order], botData: {},
    inferActionFromOpenOrder: () => 'buyback_call',
    fetchPositions: async () => [{ instrument_name: name, direction: 'short', amount: 8.26 }],
    getRestingExitInvalidReason: () => invalid ? 'active exit bounds no longer permit price' : null,
    isReduceOnlyExitAction: action => action === 'buyback_call',
    cancelOrder: async () => { cancels++; return { success: true }; },
  };
  const manage = actual('manageOpenOrders', bindings);
  await manage({}, [], [], 2500);
  assert.equal(cancels, 0);
  invalid = true;
  await manage({}, [], [], 2500);
  assert.equal(cancels, 1);
  assert.equal(tracked.status, 'open', 'Cancellation acknowledgement still awaits terminal reconciliation');
});
