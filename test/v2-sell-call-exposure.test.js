'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const { loadProduction } = require('./helpers/load-production');

const { getEntryBookLiquidity, validateAdvisorRuleContract, getCallMarginDisciplinePrompt } = loadProduction(
  ['getEntryBookLiquidity', 'validateAdvisorRuleContract', 'getCallMarginDisciplinePrompt'],
);

const book = { bidPrice: 10, bidAmount: 0.22, askPrice: 11, askAmount: 0.5 };

test('a post_only call sale rests above the bid, so the visible bid size does not cap it', () => {
  assert.equal(getEntryBookLiquidity({ action: 'sell_call', preferred_order_type: 'post_only' }, 'C', book, 10), Infinity);
  assert.equal(getEntryBookLiquidity({ action: 'sell_call' }, 'C', book, 10), Infinity, 'entries default to post_only');
  assert.equal(getEntryBookLiquidity({ action: 'sell_call', preferred_order_type: 'ioc' }, 'C', book, 10), 0.22, 'taking sales stay capped');
  assert.equal(getEntryBookLiquidity({ action: 'buy_put' }, 'P', book, 11), 0.5, 'buy_put at the ask is unchanged');
});

const sellCall = (criteria) => ({
  rule_type: 'entry',
  action: 'sell_call',
  criteria: { option_type: 'C', delta_range: [0.05, 0.11], dte_range: [6, 12], min_score: 82, min_bid: 6, ...criteria },
});

test('sell_call rules may not carry exposure caps the engine does not enforce', () => {
  assert.deepEqual(validateAdvisorRuleContract(sellCall({})), { valid: true });
  const invented = validateAdvisorRuleContract(sellCall({ max_total_short_call_exposure_eth: 2.04, max_projected_margin_utilization_pct: 45 }));
  assert.equal(invented.valid, false);
  assert.match(invented.reason, /max_total_short_call_exposure_eth, max_projected_margin_utilization_pct/);
});

test('call margin policy tells advisors and voters the margin target is the only exposure limit', () => {
  assert.match(getCallMarginDisciplinePrompt(), /Call exposure is governed only by the active margin target; do not add ETH-coverage or other exposure caps\./);
});

test('advisors size calls from risk/reward through budget_limit, with margin as the ceiling', () => {
  const { getAdvisoryRuleContractPrompt } = loadProduction(['getAdvisoryRuleContractPrompt']);
  const contract = getAdvisoryRuleContractPrompt();
  assert.match(contract, /For calls: budget_limit is the most premium \(USD\) this rule may collect; size it from your risk\/reward view/);
  assert.doesNotMatch(contract, /ETH collateral/);
});
