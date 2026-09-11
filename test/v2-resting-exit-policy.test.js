'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { loadProduction } = require('./helpers/load-production');
const now = Date.parse('2030-09-11T12:00:00Z');
class Clock extends Date { constructor(...args) { super(...(args.length ? args : [now])); } static now() { return now; } }
const { getRestingExitInvalidReason } = loadProduction(['getRestingExitInvalidReason'], { bindings: { Date: Clock } });
const name = 'ETH-20301127-1600-P';

function fixture() {
  return {
    order: { instrument_name: name, direction: 'sell', amount: 2.5, filled_amount: 0, limit_price: 12 },
    tracked: { action: 'sell_put', exit_intent: 'monetize_tail_win', order_type: 'post_only' },
    activeRules: [{ rule_type: 'exit', action: 'sell_put', instrument_name: name, criteria: {
      put_exit_intent: 'monetize_tail_win', retain_downside_protection: true, tranche_fraction: 0.25,
      min_exit_price: 12, condition_logic: 'all', conditions: [{ field: 'unrealized_pnl_pct', op: 'gt', value: 1000 }],
    } }],
    positions: [{ instrument_name: name, direction: 'long', amount: 10, avg_entry_price: 1 }],
    instruments: [{ instrument_name: name, price_step: 0.1 }],
    tickerMap: { [name]: { b: 12, a: 13, M: 12.5, I: 2500, option_pricing: { d: -0.05 } } },
    spotPrice: 2500,
  };
}

test('resting tail-win floor cannot replace current fair or executable value proof', () => {
  const input = fixture();
  assert.equal(getRestingExitInvalidReason(input), null);
  Object.assign(input.tickerMap[name], { b: 1, a: 2, M: 1.5 });
  assert.match(getRestingExitInvalidReason(input), /no longer satisfies/);
});

test('current fair-value proof can support a patient floor while visible bids are sparse', () => {
  const input = fixture();
  input.tickerMap[name].b = 1;
  assert.equal(getRestingExitInvalidReason(input), null);
});

test('resting tail-win remainder must fit the current position tranche after other exits', () => {
  const input = fixture();
  input.positions[0].amount = 3;
  assert.match(getRestingExitInvalidReason(input), /no longer satisfies/);
});

test('partial fills revalidate only their remainder against the remaining hedge', () => {
  const input = fixture();
  input.order.filled_amount = 1;
  input.positions[0].amount = 9;
  assert.equal(getRestingExitInvalidReason(input), null);
});

test('a tightened active tranche or missing live instrument invalidates a resting put sale', () => {
  const input = fixture();
  input.activeRules[0].criteria.tranche_fraction = 0.1;
  assert.match(getRestingExitInvalidReason(input), /no longer satisfies/);
  input.activeRules[0].criteria.tranche_fraction = 0.25;
  input.instruments = [];
  assert.match(getRestingExitInvalidReason(input), /no longer satisfies/);
});
