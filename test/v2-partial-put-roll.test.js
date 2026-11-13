'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { loadProduction } = require('./helpers/load-production');
const helpers = loadProduction(['getSellPutExitAmount', 'validateAdvisorRuleContract', 'formatSellPutConfirmationContext', 'getStandingRulebookDisciplinePrompt', 'getPutExitDisciplinePrompt']);
const instrument = 'ETH-20991226-1000-P';
const positions = [
  { instrument_name: instrument, instrument, amount: 10, direction: 'long' },
  { instrument_name: 'ETH-20991231-1500-P', instrument: 'ETH-20991231-1500-P', amount: 3, direction: 'long' },
];
const criteria = { put_exit_intent: 'roll_protection', retain_amount: 7, requires_longer_dated_protection: true, conditions: [{ field: 'dte', op: 'lte', value: 25 }] };
const rule = { rule_type: 'exit', action: 'sell_put', instrument_name: instrument, preferred_order_type: 'ioc', criteria };

test('partial roll sells only excess and repeated evaluation preserves remainder', () => {
  const size = (amount, c = criteria) => helpers.getSellPutExitAmount(rule, c, { amount }, {});
  assert.equal(size(10), 3);
  assert.equal(size(8), 1);
  assert.equal(size(7), 0);
  assert.equal(size(6), 0);
  assert.equal(size(10, { ...criteria, retain_amount: undefined }), 10);
  assert.equal(size(10, { ...criteria, retain_amount: 0 }), 10);
  for (const retain_amount of [-1, NaN, Infinity, '7', null]) {
    const c = { ...criteria, retain_amount };
    assert.equal(size(10, c), 0);
    assert.equal(helpers.validateAdvisorRuleContract({ ...rule, criteria: c }).valid, false);
  }
});

test('advisory and confirmation check replacement against partial sale', () => {
  assert.equal(helpers.validateAdvisorRuleContract(rule, { positionSnapshots: positions }).valid, true);
  assert.equal(helpers.validateAdvisorRuleContract({ ...rule, criteria: { ...criteria, retain_amount: 0 } }, { positionSnapshots: positions }).valid, false);
  const context = helpers.formatSellPutConfirmationContext({ action: { ...rule, amount: 3, rule_criteria: criteria }, triggerData: { put_exit_intent: 'roll_protection', current_values: { dte: 19 } }, livePositions: positions });
  assert.match(context, /planned_close_amount=3.0000/);
  assert.match(context, /longer_dated_protection_in_book=yes/);
});

test('advisory and confirmation share partial-roll guidance', () => {
  for (const prompt of [helpers.getStandingRulebookDisciplinePrompt(), helpers.getPutExitDisciplinePrompt()]) {
    assert.match(prompt, /compare hold, partial sale and full sale/);
    assert.match(prompt, /sell into the surge, not after it/);
    assert.match(prompt, /cover counts at any strike/);
    assert.match(prompt, /execution_price gte/);
    assert.match(prompt, /criteria.retain_amount/);
    assert.match(prompt, /including on repeated evaluations/);
  }
});
