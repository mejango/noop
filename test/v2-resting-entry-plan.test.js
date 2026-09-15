'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { buildRestingEntryPlan, normalizeDesiredEntryOrder, compareRestingEntryOrder } = require('../bot/resting-entry-plan');
const nowMs = Date.parse('2030-09-15T08:00:00Z');

function fixture(action = 'buy_put') {
  const put = action === 'buy_put';
  const name = put ? 'ETH-20301114-1800-P' : 'ETH-20300923-3000-C';
  return {
    order: { order_id: 'incumbent', instrument_name: name, action, direction: put ? 'buy' : 'sell',
      amount: 2.5, filled_amount: 0, limit_price: put ? 9.9 : 10.1, time_in_force: 'post_only' },
    rule: { id: 11, rule_type: 'entry', action, is_active: 1, preferred_order_type: 'post_only', budget_limit: 100,
      criteria: { option_type: put ? 'P' : 'C', delta_range: put ? [-0.12, -0.02] : [0.04, 0.12],
        dte_range: put ? [45, 78] : [5, 12], min_score: put ? 0.005 : 65, ...(put ? {} : { min_bid: 4 }) } },
    instrument: { instrument_name: name, price_step: 0.1, options: { amount_step: 0.01 },
      option_details: { option_type: put ? 'P' : 'C', strike: put ? 1800 : 3000 } },
    ticker: { b: put ? 9.9 : 10, a: put ? 20 : 11, I: 2500, option_pricing: { d: put ? -0.05 : 0.1 } },
    spotPrice: 2500, putBudgetRemaining: 100, otherPutReserved: 0, nowMs,
  };
}

test('incumbent put is evaluated directly and keeps equivalent canonical economics', () => {
  const result = buildRestingEntryPlan(fixture());
  assert.equal(result.decision, 'keep');
  assert.equal(result.desiredOrder.limit_price, 9.9);
  assert.equal(result.desiredOrder.amount, 2.5);
});

test('incumbent call comparison uses its final maker price rather than the raw bid', () => {
  const result = buildRestingEntryPlan(fixture('sell_call'));
  assert.equal(result.decision, 'keep');
  assert.equal(result.desiredOrder.limit_price, 10.1);
  assert.equal(result.desiredOrder.amount, 2.5);
});

test('partial fills consume quantity and never refill the original intended amount', () => {
  const input = fixture();
  input.order.amount = 5;
  input.order.filled_amount = 2.5;
  const result = buildRestingEntryPlan(input);
  assert.equal(result.decision, 'keep');
  assert.equal(result.remainingAmount, 2.5);
  assert.equal(result.desiredOrder.amount, 2.5);
  assert.equal(result.approvedNotional, 24.75);
});

test('a higher fresh put ceiling reduces quantity to preserve the unfilled notional', () => {
  const input = fixture();
  input.ticker.option_pricing.d = -0.055;
  const result = buildRestingEntryPlan(input);
  assert.equal(result.decision, 'replace');
  assert.equal(result.desiredOrder.limit_price, 10.9);
  assert.equal(result.desiredOrder.amount, 2.27);
  assert.ok(result.desiredOrder.amount * result.desiredOrder.limit_price <= result.approvedNotional);
});

test('a cheaper put price does not increase the authorized remaining quantity', () => {
  const input = fixture();
  input.ticker.option_pricing.d = -0.04;
  const result = buildRestingEntryPlan(input);
  assert.equal(result.decision, 'replace');
  assert.equal(result.desiredOrder.limit_price, 7.9);
  assert.equal(result.desiredOrder.amount, 2.5);
});

test('other reservations remain reserved and only the incumbent may reuse its own allocation', () => {
  const input = fixture();
  input.putBudgetRemaining = 30;
  input.otherPutReserved = 10;
  const result = buildRestingEntryPlan(input);
  assert.equal(result.decision, 'replace');
  assert.equal(result.desiredOrder.amount, 2.02);
  assert.ok(result.desiredOrder.amount * result.desiredOrder.limit_price <= 20);
});

test('current rule budget and venue amount steps further cap the remaining put size', () => {
  const input = fixture();
  input.rule.budget_limit = 12;
  input.instrument.options.amount_step = 0.1;
  const result = buildRestingEntryPlan(input);
  assert.equal(result.desiredOrder.amount, 1.2);
  assert.equal(result.decision, 'replace');
});

test('effective target and supplied production pricing preserve the minimum edge', () => {
  const input = fixture();
  input.rule.criteria.target_score = 0.007;
  input.effectiveTargetScore = 0.006;
  input.pricing = { limitPrice: 8.33, requiredScore: 0.006 };
  const result = buildRestingEntryPlan(input);
  assert.equal(result.requiredScore, 0.006);
  assert.equal(result.desiredOrder.limit_price, 8.3);
  input.effectiveTargetScore = 0.004;
  input.pricing = { limitPrice: 12.5, requiredScore: 0.004 };
  assert.equal(buildRestingEntryPlan(input).requiredScore, 0.005);
});

test('renewing a rule id does not churn an otherwise equivalent order', () => {
  const input = fixture();
  input.order.rule_id = 10;
  input.rule.id = 99;
  const result = buildRestingEntryPlan(input);
  assert.equal(result.decision, 'keep');
  assert.equal(result.ruleId, 99);
});

test('absent rules, quotes, instruments and unverified budgets defer maintenance', () => {
  for (const changes of [{ rule: null }, { ticker: null }, { instrument: null }, { putBudgetRemaining: undefined }]) {
    assert.equal(buildRestingEntryPlan({ ...fixture(), ...changes }).decision, 'unresolved');
  }
  const input = fixture();
  input.rule.is_active = 0;
  assert.equal(buildRestingEntryPlan(input).decision, 'unresolved');
});

test('hard delta, DTE and current spot conditions invalidate the incumbent intent', () => {
  const delta = fixture(); delta.ticker.option_pricing.d = -0.2;
  assert.equal(buildRestingEntryPlan(delta).decision, 'invalid');
  const dte = fixture(); dte.nowMs += 30 * 86400000;
  assert.equal(buildRestingEntryPlan(dte).decision, 'invalid');
  const spot = fixture(); spot.rule.criteria.market_conditions = [{ field: 'spot_price', op: 'gte', value: 3000 }];
  assert.equal(buildRestingEntryPlan(spot).decision, 'invalid');
});

test('below-minimum remaining budget never produces an untradable replacement', () => {
  assert.equal(buildRestingEntryPlan({ ...fixture(), putBudgetRemaining: 0.1 }).decision, 'invalid');
});

test('call maintenance preserves remainder and defers when fresh bid economics cannot authorize a replacement', () => {
  const input = fixture('sell_call');
  input.order.amount = 5; input.order.filled_amount = 2.5;
  assert.equal(buildRestingEntryPlan(input).desiredOrder.amount, 2.5);
  input.ticker.b = 3;
  assert.equal(buildRestingEntryPlan(input).decision, 'unresolved');
});

test('normalization is idempotent and accepts an explicitly reviewed IOC route', () => {
  for (const action of ['buy_put', 'sell_call']) {
    const input = fixture(action);
    const args = { action, instrumentName: input.order.instrument_name, amount: 2.5, price: 10,
      orderType: 'post_only', instrument: input.instrument, ticker: input.ticker };
    const once = normalizeDesiredEntryOrder(args);
    const twice = normalizeDesiredEntryOrder({ ...args, price: once.limit_price, amount: once.amount });
    assert.deepEqual(twice, once);
    assert.equal(normalizeDesiredEntryOrder({ ...args, orderType: 'ioc' }).order_type, 'ioc');
  }
});

test('one-tick price improvement does not compete with our own BBO', () => {
  const input = fixture();
  const desired = normalizeDesiredEntryOrder({ action: 'buy_put', instrumentName: input.order.instrument_name,
    amount: 2.5, price: 10, orderType: 'post_only', instrument: input.instrument, ticker: input.ticker, existingOrder: input.order });
  assert.equal(desired.limit_price, 9.9);
  const call = fixture('sell_call');
  call.ticker.b = 9.9; call.ticker.a = 10.1;
  const offer = normalizeDesiredEntryOrder({ action: 'sell_call', instrumentName: call.order.instrument_name,
    amount: 2.5, price: 10, orderType: 'post_only', instrument: call.instrument, ticker: call.ticker, existingOrder: call.order });
  assert.equal(offer.limit_price, 10.1);
  const explicitlyReviewedIoc = normalizeDesiredEntryOrder({ action: 'buy_put', instrumentName: input.order.instrument_name,
    amount: 2.5, price: 10, orderType: 'ioc', instrument: input.instrument, ticker: input.ticker, existingOrder: input.order });
  assert.equal(explicitlyReviewedIoc.limit_price, 10);
});

test('comparison detects route, price and remainder changes and preserves unknown terms', () => {
  const input = fixture();
  const desired = buildRestingEntryPlan(input).desiredOrder;
  assert.equal(compareRestingEntryOrder(input.order, desired).decision, 'keep');
  assert.deepEqual(compareRestingEntryOrder(input.order, { ...desired, order_type: 'gtc', limit_price: 9.8, amount: 2 }).differences,
    ['order_type', 'price', 'amount']);
  assert.equal(compareRestingEntryOrder({ ...input.order, time_in_force: null }, desired).decision, 'unresolved');
});

test('malformed cumulative fills cannot create apparent additional entry capacity', () => {
  for (const filled of [-1, 'unavailable', 3, true]) {
    const input = fixture(); input.order.filled_amount = filled;
    assert.equal(buildRestingEntryPlan(input).decision, 'unresolved');
  }
});

test('a call bid touching our own resting ask moves the maker offer strictly above it', () => {
  const input = fixture('sell_call');
  input.ticker.b = 10.1; input.ticker.a = 10.1;
  const result = buildRestingEntryPlan(input);
  assert.equal(result.decision, 'replace');
  assert.ok(result.desiredOrder.limit_price > input.ticker.b);
  assert.ok(result.desiredOrder.limit_price >= input.order.limit_price);
});

test('a cancelled prior order cannot establish that the current BBO is still ours', () => {
  const input = fixture();
  input.order.limit_price = 9.8; input.order.status = 'cancelled'; input.ticker.b = 9.8;
  const desired = normalizeDesiredEntryOrder({ action: 'buy_put', instrumentName: input.order.instrument_name,
    amount: 2.5, price: 9.9, orderType: 'post_only', instrument: input.instrument, ticker: input.ticker, existingOrder: input.order });
  assert.equal(desired.limit_price, 9.9);
  assert.equal(compareRestingEntryOrder(input.order, desired).decision, 'unresolved');
});

test('a proven terminal cancellation can replan only its remaining entry authority', () => {
  for (const status of ['cancelled', 'expired', 'rejected']) {
    const input = fixture();
    input.order.status = status; input.order.amount = 5; input.order.filled_amount = 2.5;
    const result = buildRestingEntryPlan(input);
    assert.equal(result.decision, 'replace');
    assert.deepEqual(result.differences, ['order_status']);
    assert.equal(result.desiredOrder.amount, 2.5);
    input.order.filled_amount = 5;
    assert.equal(buildRestingEntryPlan(input).decision, 'unresolved');
  }
});
