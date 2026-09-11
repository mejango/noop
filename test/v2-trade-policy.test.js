'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  evaluateConditions,
  evaluateExitConditions,
  resolveConfirmationVotes,
  hasUsableMarginState,
  getPutReplacementCoverage,
  liveRuleValues,
  validateFinalOrderPolicy,
} = require('../bot/trade-policy');

const now = Date.parse('2026-09-01T20:00:00Z');
const policy = {
  putDeltaRange: [-0.12, -0.02], putDteRange: [45, 78],
  callDeltaRange: [0.04, 0.12], callDteRange: [5, 12],
  sellCallMinScore: 65, sellCallMinBid: 4,
  callCapturePct: 80, putRollDte: 25,
  putMonetizationPct: 1000, putMaxTrancheFraction: 0.25,
};
const marginState = {
  initial_margin: 1000, maintenance_margin: 1200,
  subaccount_value: 5000, is_under_liquidation: false,
};
const callName = 'ETH-20260910-3200-C'; // 8.5 DTE at the fixed timestamp.
const putName = 'ETH-20261031-1600-P';
const retiringName = 'ETH-20260920-2000-P';
const tailName = 'ETH-20261031-2000-P';
const instrument = instrument_name => ({ instrument_name });
const longPut = (instrument_name, amount = 100) => ({ instrument_name, amount, direction: 'long', avg_entry_price: 10 });
const positionCall = { instrument_name: callName, amount: 3, direction: 'short', avg_entry_price: 10 };
const condition = (field, op, value) => ({ field, op, value });

function callOrder(overrides = {}) {
  return {
    action: 'sell_call', instrumentName: callName, instrument: instrument(callName),
    price: 8, amount: 1, orderType: 'gtc',
    criteria: { min_score: 65, min_bid: 8, delta_range: [0.04, 0.12], dte_range: [5, 12] },
    ticker: { b: 10, a: 11, M: 10.5, option_pricing: { d: 0.1 } },
    spotPrice: 2500, marginState,
    callMarginDecision: { available: true, entryCapSatisfied: true, marginPerUnit: 100 },
    policy, now, ...overrides,
  };
}

function putOrder(overrides = {}) {
  return {
    action: 'buy_put', instrumentName: putName, instrument: instrument(putName),
    price: 12, amount: 2, orderType: 'post_only',
    criteria: { min_score: 0.004, target_score: 0.0045, delta_range: [-0.12, -0.02], dte_range: [45, 78] },
    triggerData: { advisor_limit_price: 12 },
    ticker: { b: 12, a: 14, M: 13, option_pricing: { d: -0.06 } },
    spotPrice: 2500, marginState, putBudgetRemaining: 100, ruleBudgetLimit: 100,
    policy, now, ...overrides,
  };
}

function buybackOrder(overrides = {}) {
  return {
    action: 'buyback_call', instrumentName: callName, instrument: instrument(callName),
    price: 2, amount: 1, orderType: 'post_only',
    criteria: {
      buyback_intent: 'profit_capture',
      conditions: [condition('unrealized_pnl_pct', 'gte', 80), condition('spot_price', 'gte', 2000), condition('delta', 'lte', 0.12)],
      condition_logic: 'all',
    },
    ticker: { b: 4, a: 5, M: 4.5, option_pricing: { d: 0.1 } },
    positions: [positionCall], spotPrice: 2500, policy, now, ...overrides,
  };
}

function rollOrder(overrides = {}) {
  return {
    action: 'sell_put', instrumentName: retiringName, instrument: instrument(retiringName),
    price: 20, amount: 100, orderType: 'ioc',
    criteria: { put_exit_intent: 'roll_protection', conditions: [condition('dte', 'lte', 25)] },
    ticker: { b: 20, a: 22, M: 21, option_pricing: { d: -0.1 } },
    positions: [longPut(retiringName), longPut(tailName)],
    spotPrice: 2500, policy, now, ...overrides,
  };
}

function monetizeOrder(overrides = {}) {
  return {
    action: 'sell_put', instrumentName: tailName, instrument: instrument(tailName),
    price: 120, amount: 25, orderType: 'post_only',
    criteria: {
      put_exit_intent: 'monetize_tail_win', retain_downside_protection: true,
      tranche_fraction: 0.25, min_exit_price: 120,
      conditions: [condition('unrealized_pnl_pct', 'gt', 1000), condition('spot_price', 'lte', 1800), condition('delta', 'lt', -0.4)],
      condition_logic: 'all',
    },
    ticker: { b: 130, a: 140, M: 135, option_pricing: { d: -0.6 } },
    positions: [longPut(tailName)], spotPrice: 1700, policy, now, ...overrides,
  };
}

const rejectCode = (input, code) => {
  const result = validateFinalOrderPolicy(input);
  assert.equal(result.allowed, false, `Expected rejection ${code}`);
  assert.equal(result.code, code);
};

test('final sell-call price must satisfy both approved edge and premium floor', () => {
  rejectCode(callOrder({ price: 5 }), 'call_price_failed');
  rejectCode(callOrder({ price: 7 }), 'call_price_failed'); // Edge 70 passes; min_bid 8 fails.
  rejectCode(callOrder({ price: 6, criteria: { min_score: 65, min_bid: 4 } }), 'call_price_failed');
  const result = validateFinalOrderPolicy(callOrder());
  assert.equal(result.allowed, true);
  assert.equal(result.approvedBounds.sellFloor, 8);
});

test('valid final call price cannot rescue a live quote that fails approved economics', () => {
  rejectCode(callOrder({ ticker: { b: 6, a: 11, option_pricing: { d: 0.1 } } }), 'call_quote_failed');
});

test('missing and incomplete margin state rejects every new exposure', () => {
  const incomplete = [null, {}, { ...marginState, initial_margin: null }, { ...marginState, maintenance_margin: undefined },
    { ...marginState, subaccount_value: 0 }, { ...marginState, is_under_liquidation: undefined }];
  for (const state of incomplete) {
    assert.equal(hasUsableMarginState(state), false);
    rejectCode(callOrder({ marginState: state }), 'margin_unavailable');
    rejectCode(putOrder({ marginState: state }), 'margin_unavailable');
  }
  assert.equal(hasUsableMarginState(marginState), true);
  rejectCode(callOrder({ marginState: { ...marginState, is_under_liquidation: true } }), 'unsafe_margin');
});

test('fresh call margin cap and quantity capacity are both enforced', () => {
  rejectCode(callOrder({ callMarginDecision: null }), 'call_margin_failed');
  rejectCode(callOrder({ callMarginDecision: { available: true, entryCapSatisfied: false, marginPerUnit: 100 } }), 'call_margin_failed');
  rejectCode(callOrder({ amount: 11 }), 'call_margin_failed');
});

test('zero, negative and absent per-unit margin cannot establish call capacity', () => {
  for (const marginPerUnit of [0, -10, null, undefined, NaN, Infinity]) {
    rejectCode(callOrder({ callMarginDecision: { available: true, entryCapSatisfied: true, marginPerUnit } }), 'call_margin_failed');
  }
});

test('malformed rule ranges cannot bypass range enforcement', () => {
  for (const range of [[], [0.04], [0.04, 0.12, 1], [null, 0.12], [0.04, undefined], [' ', 0.12], [0.12, 0.04]]) {
    const order = callOrder();
    order.criteria.delta_range = range;
    rejectCode(order, 'range_failed');
  }
  const order = callOrder();
  order.criteria.dte_range = [];
  rejectCode(order, 'range_failed');
});

test('malformed market conditions cannot bypass the entry gate', () => {
  for (const malformed of [condition('spot_price', 'gt', 3000), 'spot_price > 3000', 1]) {
    const order = callOrder();
    order.criteria.market_conditions = malformed;
    rejectCode(order, 'conditions_failed');
  }
});

test('entry rechecks fresh delta, rule ranges and market conditions', () => {
  rejectCode(callOrder({ ticker: { b: 10, a: 11, option_pricing: { d: 0.13 } } }), 'range_failed');
  const strictRange = callOrder();
  strictRange.criteria.delta_range = [0.04, 0.09];
  rejectCode(strictRange, 'range_failed');
  const blocked = callOrder();
  blocked.criteria.market_conditions = [condition('spot_price', 'gte', 3000)];
  rejectCode(blocked, 'conditions_failed');
});

test('patient capture can replace PnL only while every other all-condition remains live', () => {
  const values = { unrealized_pnl_pct: 50, spot_price: 2500, delta: 0.1, dte: 8.5 };
  const criteria = { conditions: [condition('unrealized_pnl_pct', 'gte', 80), condition('spot_price', 'gte', 3000)], condition_logic: 'all' };
  assert.equal(evaluateExitConditions(criteria, values, 80), false);
  criteria.conditions[1] = condition('spot_price', 'gte', 2000);
  criteria.conditions.push(condition('delta', 'lte', 0.08));
  assert.equal(evaluateExitConditions(criteria, values, 80), false);
  criteria.conditions[2] = condition('delta', 'lte', 0.12);
  assert.equal(evaluateExitConditions(criteria, values, 80), true);
  assert.equal(evaluateExitConditions(criteria, values, null), false);
});

test('missing conditional facts and malformed operators fail closed', () => {
  assert.equal(evaluateConditions([condition('delta', 'lte', 0.12)], 'all', {}), false);
  assert.equal(evaluateConditions([condition('delta', 'unknown', 0.12)], 'all', { delta: 0.1 }), false);
  assert.equal(evaluateExitConditions({ conditions: [] }, { unrealized_pnl_pct: 90 }, 90), false);
});

test('explicit reviewer rejection always wins, including disagreement and patient-plan prose', () => {
  const reject = { confirm: false, reasoning: 'Patient plan exists, but spot condition fails' };
  assert.equal(resolveConfirmationVotes(reject, reject), 'rejected');
  assert.equal(resolveConfirmationVotes({ confirm: true }, reject), 'rejected');
  assert.equal(resolveConfirmationVotes(reject, { confirm: true }), 'rejected');
  assert.equal(resolveConfirmationVotes(null, reject), 'rejected');
  assert.equal(resolveConfirmationVotes({ confirm: true }, { confirm: true }), 'confirmed');
  assert.equal(resolveConfirmationVotes(null, { confirm: true }), 'confirmed');
  assert.equal(resolveConfirmationVotes(null, null), 'retry');
});

test('replacement hedge requires sufficient quantity at the same or higher strike', () => {
  const retiring = longPut(retiringName);
  assert.equal(getPutReplacementCoverage(retiring, [longPut('ETH-20261130-500-P', 0.01)], 100, now).allowed, false);
  assert.equal(getPutReplacementCoverage(retiring, [longPut(tailName, 0.01)], 100, now).allowed, false);
  assert.equal(getPutReplacementCoverage(retiring, [longPut('ETH-20261130-1500-P', 100)], 100, now).allowed, false);
  assert.equal(getPutReplacementCoverage(retiring, [longPut(tailName, 100)], 100, now).allowed, true);
  assert.equal(getPutReplacementCoverage(retiring, [longPut('ETH-20261130-2200-P', 100)], 100, now).allowed, true);
});

test('replacement coverage aggregates valid later puts and respects partial sale quantity', () => {
  const retiring = longPut(retiringName);
  const positions = [longPut(tailName, 40), longPut('ETH-20261130-2200-P', 60)];
  const full = getPutReplacementCoverage(retiring, positions, 100, now);
  assert.equal(full.allowed, true);
  assert.equal(full.replacementAmount, 100);
  assert.equal(getPutReplacementCoverage(retiring, [longPut(tailName, 25)], 25, now).allowed, true);
  assert.equal(getPutReplacementCoverage(retiring, positions, 101, now).allowed, false);
});

test('same-expiry, earlier, short and call positions cannot supply replacement coverage', () => {
  const retiring = longPut(retiringName);
  const invalid = [retiring, longPut('ETH-20260920-2200-P'), longPut('ETH-20260910-2200-P'),
    { ...longPut(tailName), direction: 'short' }, longPut('ETH-20261130-2200-C')];
  const result = getPutReplacementCoverage(retiring, invalid, 100, now);
  assert.equal(result.allowed, false);
  assert.equal(result.replacementAmount, 0);
});

test('final roll rechecks quantity coverage and live conditions', () => {
  assert.equal(validateFinalOrderPolicy(rollOrder()).allowed, true);
  rejectCode(rollOrder({ positions: [longPut(retiringName), longPut(tailName, 0.01)] }), 'replacement_failed');
  const blocked = rollOrder();
  blocked.criteria.conditions.push(condition('spot_price', 'gte', 3000));
  rejectCode(blocked, 'exit_conditions_failed');
});

test('final put price obeys approved cap and fresh normalized score', () => {
  const accepted = validateFinalOrderPolicy(putOrder());
  assert.equal(accepted.allowed, true);
  assert.equal(accepted.approvedBounds.buyCeiling, 12);
  rejectCode(putOrder({ price: 12.01 }), 'put_price_failed');
  const scoreFail = putOrder({ price: 11 });
  scoreFail.criteria.target_score = 0.006;
  rejectCode(scoreFail, 'put_price_failed');
  rejectCode(putOrder({ ticker: { b: 12, a: 14, option_pricing: { d: -0.04 } } }), 'put_price_failed');
});

test('final put outlay must fit fresh account and rule budgets', () => {
  for (const putBudgetRemaining of [null, undefined, 0, 23.99]) {
    rejectCode(putOrder({ putBudgetRemaining }), 'put_budget_failed');
  }
  rejectCode(putOrder({ ruleBudgetLimit: 23.99 }), 'put_budget_failed');
  assert.equal(validateFinalOrderPolicy(putOrder({ putBudgetRemaining: 24, ruleBudgetLimit: 24 })).allowed, true);
});

test('final profit buyback preserves the actual capture floor and approved price ceiling', () => {
  const accepted = validateFinalOrderPolicy(buybackOrder());
  assert.equal(accepted.allowed, true);
  assert.ok(Math.abs(accepted.approvedBounds.buyCeiling - 2) < 1e-12);
  rejectCode(buybackOrder({ price: 2.01 }), 'capture_failed');
  rejectCode(buybackOrder({ price: 1.6, triggerData: { advisor_limit_price: 1.5 } }), 'capture_failed');
});

test('final patient buyback cannot bypass fresh spot or delta conditions', () => {
  const blockedSpot = buybackOrder();
  blockedSpot.criteria.conditions[1] = condition('spot_price', 'gte', 3000);
  rejectCode(blockedSpot, 'exit_conditions_failed');
  rejectCode(buybackOrder({ ticker: { a: 5, M: 4.5, option_pricing: { d: 0.15 } } }), 'exit_conditions_failed');
  rejectCode(buybackOrder({ ticker: { a: 5, M: 4.5, option_pricing: {} } }), 'exit_conditions_failed');
});

test('live rule PnL uses executable close quotes and leaves missing quotes unknown', () => {
  assert.equal(liveRuleValues(positionCall, { a: 5, M: 1 }, 2500, now).unrealized_pnl_pct, 50);
  assert.equal(liveRuleValues(longPut(tailName), { b: 120, M: 200 }, 1700, now).unrealized_pnl_pct, 1100);
  assert.equal(liveRuleValues(positionCall, { M: 1 }, 2500, now).unrealized_pnl_pct, null);
});

test('monetization requires both fresh value and final price above the strict tail-win threshold', () => {
  assert.equal(validateFinalOrderPolicy(monetizeOrder()).allowed, true);
  rejectCode(monetizeOrder({ price: 110 }), 'monetization_failed');
  rejectCode(monetizeOrder({ spotPrice: 2500, ticker: { b: 20, a: 22, M: 21, option_pricing: { d: -0.1 } } }), 'monetization_failed');
});

test('nonfinite marks cannot manufacture proof of a tail win', () => {
  for (const M of [Infinity, 'Infinity', NaN, null, undefined]) {
    const order = monetizeOrder({ spotPrice: 2500, ticker: { b: 20, a: 22, M, option_pricing: { d: -0.1 } } });
    order.criteria.conditions = [condition('unrealized_pnl_pct', 'gt', 1000)];
    rejectCode(order, 'monetization_failed');
  }
});

test('monetization enforces tranche size, retained position and approved sell floor', () => {
  rejectCode(monetizeOrder({ amount: 25.01 }), 'tranche_failed');
  rejectCode(monetizeOrder({ amount: 100 }), 'tranche_failed');
  const smallerTranche = monetizeOrder({ amount: 11 });
  smallerTranche.criteria.tranche_fraction = 0.1;
  rejectCode(smallerTranche, 'tranche_failed');
  rejectCode(monetizeOrder({ price: 119 }), 'put_exit_price_failed');
  rejectCode(monetizeOrder({ price: 120, triggerData: { advisor_limit_price: 125 } }), 'put_exit_price_failed');
});

test('monetization cannot use final PnL to bypass additional live exit conditions', () => {
  const blocked = monetizeOrder();
  blocked.criteria.conditions[1] = condition('spot_price', 'gte', 1800);
  rejectCode(blocked, 'exit_conditions_failed');
  rejectCode(monetizeOrder({ ticker: { b: 130, a: 140, M: 135, option_pricing: { d: -0.2 } } }), 'exit_conditions_failed');
});

test('exits cannot exceed the live position quantity', () => {
  rejectCode(buybackOrder({ amount: 4 }), 'close_quantity_failed');
  rejectCode(monetizeOrder({ amount: 101 }), 'close_quantity_failed');
});
