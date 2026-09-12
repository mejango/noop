'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const {
  normalizeDesiredExitOrder,
  compareRestingExitOrder,
  resolveDesiredExitOrderType,
  getDesiredSellPutRemainingAmount,
} = require('../bot/resting-exit-plan');

const instrumentName = 'ETH-20260918-2800-C';
const instrument = {
  instrument_name: instrumentName,
  price_step: 0.1,
  base_asset_sub_id: 'fixture-option',
};

function resting(fields = {}) {
  return {
    order_id: 'resting-buyback', instrument_name: instrumentName,
    action: 'buyback_call', direction: 'buy', order_type: 'post_only',
    exit_intent: 'profit_capture', status: 'open',
    amount: 1, filled_amount: 0, limit_price: 2.2,
    ...fields,
  };
}

function desired(fields = {}) {
  return normalizeDesiredExitOrder({
    action: 'buyback_call', instrumentName, amount: 1, price: 2.2,
    orderType: 'post_only', intent: 'profit_capture', instrument,
    ticker: { b: 2.1, a: 3 },
    ...fields,
  });
}

test('identical desired buyback preserves the existing order', () => {
  assert.deepEqual(compareRestingExitOrder(resting(), desired()), { decision: 'keep', differences: [] });
});

test('partially filled order compares its remaining amount against desired live closeable amount', () => {
  const existing = resting({ amount: '1.50', filled_amount: '0.50' });
  assert.equal(compareRestingExitOrder(existing, desired({ amount: 1 })).decision, 'keep');
  const changed = compareRestingExitOrder(existing, desired({ amount: 1.5 }));
  assert.equal(changed.decision, 'replace');
  assert.deepEqual(changed.differences, ['amount']);
});

test('desired price and amount compare after venue normalization', () => {
  const plan = desired({ price: 2.26, amount: 1.009 });
  assert.equal(plan.limit_price, 2.2);
  assert.equal(plan.amount, 1);
  assert.equal(compareRestingExitOrder(resting(), plan).decision, 'keep');
});

test('sell-put desired prices round upward to preserve the approved exit floor', () => {
  const putName = 'ETH-20261127-1600-P';
  const plan = normalizeDesiredExitOrder({
    action: 'sell_put', instrumentName: putName, amount: 1, price: 2.26,
    orderType: 'gtc', intent: 'monetize_tail_win', instrument: { ...instrument, instrument_name: putName },
    ticker: { b: 2, a: 3 },
  });
  assert.equal(plan.limit_price, 2.3);
  assert.equal(compareRestingExitOrder(resting({
    instrument_name: putName, action: 'sell_put', direction: 'sell',
    order_type: 'gtc', exit_intent: 'monetize_tail_win', limit_price: 2.3,
  }), plan).decision, 'keep');
});

test('round-number and crossing maker adjustments match the actual desired submitted price', () => {
  const round = desired({ price: 2, ticker: { b: 1.5, a: 3 } });
  assert.equal(round.limit_price, 1.9);
  assert.equal(compareRestingExitOrder(resting({ limit_price: 1.9 }), round).decision, 'keep');
  const crossing = desired({ price: 2.3, ticker: { b: 2.1, a: 2.3 } });
  assert.equal(crossing.limit_price, 2.2);
  assert.equal(compareRestingExitOrder(resting(), crossing).decision, 'keep');
});

for (const [label, changes, difference] of [
  ['price', { price: 2.4 }, 'price'],
  ['amount', { amount: 0.7 }, 'amount'],
  ['intent', { intent: 'threat_management' }, 'intent'],
  ['resting route', { orderType: 'gtc' }, 'order_type'],
  ['IOC route', { orderType: 'ioc' }, 'order_type'],
]) {
  test(`a changed desired ${label} requires replacement`, () => {
    const result = compareRestingExitOrder(resting(), desired(changes));
    assert.equal(result.decision, 'replace');
    assert.ok(result.differences.includes(difference));
  });
}

for (const [label, changes] of [
  ['order identity', { order_id: null }],
  ['instrument', { instrument_name: null }],
  ['direction', { direction: null }],
  ['route', { order_type: null }],
  ['unknown route', { order_type: 'unknown' }],
  ['intent', { exit_intent: null }],
  ['remaining quantity', { filled_amount: 'unavailable' }],
  ['price', { limit_price: null }],
]) {
  test(`missing existing ${label} remains unresolved`, () => {
    const result = compareRestingExitOrder(resting(changes), desired());
    assert.equal(result.decision, 'unresolved');
  });
}

test('venue route metadata is accepted and missing strategy action is inferred from the close direction', () => {
  const existing = resting({ action: undefined, order_type: undefined, time_in_force: 'post_only' });
  assert.equal(compareRestingExitOrder(existing, desired()).decision, 'keep');
});

test('a different exit action or instrument is not treated as the same existing order', () => {
  const result = compareRestingExitOrder(resting(), {
    ...desired(), action: 'sell_put', instrument_name: 'ETH-20261127-1600-P', direction: 'sell',
  });
  assert.equal(result.decision, 'replace');
  assert.deepEqual(result.differences, ['action', 'instrument', 'direction']);
});

test('advisory renewal, historical prose and order age do not alter identical desired economics', () => {
  const existing = resting({
    rule_id: 1, advisory_id: 'previous-advisory', pending_action_id: 8,
    creation_timestamp: 1, placed_at: '2000-01-01T00:00:00.000Z',
    reasoning: 'historical market commentary',
  });
  const plan = {
    ...desired(), rule_id: 2, advisory_id: 'renewed-advisory', pending_action_id: 9,
    reasoning: 'new market commentary',
  };
  assert.equal(compareRestingExitOrder(existing, plan).decision, 'keep');
});

test('a standing bid at the BBO does not make the desired order repeatedly outbid itself', () => {
  const existing = resting();
  const plan = desired({
    price: 2.3, existingOrder: existing, priceReason: 'one_tick_above_best_bid',
    ceilingPrice: 2.5, ticker: { b: 2.2, a: 3 },
  });
  assert.equal(plan.limit_price, 2.2);
  assert.equal(compareRestingExitOrder(existing, plan).decision, 'keep');
});

test('a real desired-price change below the same ceiling still replaces the standing bid', () => {
  const existing = resting();
  const plan = desired({
    price: 2.1, existingOrder: existing, priceReason: 'one_tick_above_best_bid',
    ceilingPrice: 2.5, ticker: { b: 2.2, a: 3 },
  });
  assert.equal(plan.limit_price, 2.1);
  assert.equal(compareRestingExitOrder(existing, plan).decision, 'replace');
});

test('a competing higher best bid allows the desired bid to move within its ceiling', () => {
  const existing = resting();
  const plan = desired({
    price: 2.5, existingOrder: existing, priceReason: 'one_tick_above_best_bid',
    ceilingPrice: 2.5, ticker: { b: 2.4, a: 3 },
  });
  assert.equal(plan.limit_price, 2.5);
  assert.equal(compareRestingExitOrder(existing, plan).decision, 'replace');
});

test('an explicit new desired price is not mistaken for an attempt to outbid the standing order', () => {
  const existing = resting();
  const plan = desired({
    price: 2.4, existingOrder: existing, priceReason: 'advisor_limit',
    ceilingPrice: 2.5, ticker: { b: 2.2, a: 3 },
  });
  assert.equal(plan.limit_price, 2.4);
  assert.equal(compareRestingExitOrder(existing, plan).decision, 'replace');
});

test('a lowered approved ceiling cannot preserve an older more expensive own bid', () => {
  const existing = resting();
  const plan = desired({
    price: 2.1, existingOrder: existing, priceReason: 'one_tick_above_best_bid',
    ceilingPrice: 2.1, ticker: { b: 2.2, a: 3 },
  });
  assert.equal(plan.limit_price, 2.1);
  assert.equal(compareRestingExitOrder(existing, plan).decision, 'replace');
});

test('urgent exit intents always choose IOC even when the prior preference was resting', () => {
  for (const [action, intent] of [['buyback_call', 'threat_management'], ['sell_put', 'roll_protection']]) {
    assert.equal(resolveDesiredExitOrderType({
      action, intent, preferredOrderType: 'post_only', price: 2.2, ticker: { b: 2, a: 3 },
    }), 'ioc');
  }
});

test('patient off-market limits become resting orders while executable IOC limits remain IOC', () => {
  assert.equal(resolveDesiredExitOrderType({
    action: 'buyback_call', intent: 'profit_capture', preferredOrderType: 'ioc', price: 2.2, ticker: { a: 3 },
  }), 'post_only');
  assert.equal(resolveDesiredExitOrderType({
    action: 'buyback_call', intent: 'profit_capture', preferredOrderType: 'ioc', price: 2.2, ticker: { a: 2.2 },
  }), 'ioc');
  assert.equal(resolveDesiredExitOrderType({
    action: 'sell_put', intent: 'monetize_tail_win', preferredOrderType: 'ioc', price: 3, ticker: { b: 2.2 },
  }), 'post_only');
});

test('invalid desired terms never compare as an existing equivalent order', () => {
  for (const changes of [{ price: NaN }, { amount: 0 }, { intent: null }, { orderType: 'unknown' }]) {
    assert.throws(() => desired(changes), /incomplete|precision/);
  }
});

test('partial put monetization preserves the approved remainder instead of applying the fraction again', () => {
  const amount = getDesiredSellPutRemainingAmount({
    positionAmount: 9, desiredFraction: 0.25,
    existingOrder: { amount: 2.5, filled_amount: 1, tranche_fraction: 0.25, exit_intent: 'monetize_tail_win' },
  });
  assert.equal(amount, 1.5);
});

test('repricing a partially filled put tranche cannot restore its original quantity', () => {
  const repriced = { amount: 1.5, filled_amount: 0, tranche_fraction: 0.25, exit_intent: 'monetize_tail_win' };
  assert.equal(getDesiredSellPutRemainingAmount({
    positionAmount: 9, desiredFraction: 0.25, existingOrder: repriced,
  }), 1.5);
  assert.equal(getDesiredSellPutRemainingAmount({
    positionAmount: 8.5, desiredFraction: 0.25, existingOrder: { ...repriced, filled_amount: 0.5 },
  }), 1);
});

test('a lower policy fraction or smaller live hedge can shrink the existing put remainder', () => {
  const existingOrder = { amount: 2.5, filled_amount: 1, tranche_fraction: 0.25, exit_intent: 'monetize_tail_win' };
  assert.equal(getDesiredSellPutRemainingAmount({ positionAmount: 9, desiredFraction: 0.1, existingOrder }), 0.9);
  assert.equal(getDesiredSellPutRemainingAmount({ positionAmount: 4, desiredFraction: 0.25, existingOrder }), 1);
});

test('an explicit fraction increase from known prior policy permits a larger desired tranche', () => {
  assert.equal(getDesiredSellPutRemainingAmount({
    positionAmount: 9, desiredFraction: 0.25,
    existingOrder: { amount: 1, filled_amount: 0.5, tranche_fraction: 0.1, exit_intent: 'monetize_tail_win' },
  }), 2.25);
});

test('unknown original put fraction conservatively limits replacements to the existing remainder', () => {
  for (const tranche_fraction of [undefined, null, 'unavailable']) {
    assert.equal(getDesiredSellPutRemainingAmount({
      positionAmount: 9, desiredFraction: 0.25,
      existingOrder: { amount: 2.5, filled_amount: 1, tranche_fraction, exit_intent: 'monetize_tail_win' },
    }), 1.5);
  }
});

test('a newly planned put tranche uses the current policy fraction and venue amount precision', () => {
  assert.equal(getDesiredSellPutRemainingAmount({ positionAmount: 10, desiredFraction: 0.25 }), 2.5);
  assert.equal(getDesiredSellPutRemainingAmount({ positionAmount: 9.99, desiredFraction: 0.25 }), 2.49);
});

test('a completed put tranche has no remainder to replenish during replacement', () => {
  assert.equal(getDesiredSellPutRemainingAmount({
    positionAmount: 7.5, desiredFraction: 0.25,
    existingOrder: { amount: 2.5, filled_amount: 2.5, tranche_fraction: 0.25, exit_intent: 'monetize_tail_win' },
  }), 0);
});
