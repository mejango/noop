'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const tradePolicy = require('../bot/trade-policy');
const pricing = require('../bot/order-pricing');
const source = fs.readFileSync(path.join(__dirname, '..', 'script.js'), 'utf8');
const start = source.indexOf('const getFinalOrderPolicy =');
const end = source.indexOf('const generateTradingAdvisory =', start);
assert.ok(start > 0 && end > start, 'review the actual confirmation source boundary');
const confirmationSource = source.slice(start, end);
const fixedTime = Date.parse('2030-12-01T20:00:00Z');
class Clock extends Date { constructor(...args) { super(...(args.length ? args : [fixedTime])); } static now() { return fixedTime; } }
const name = 'ETH-20301210-3200-C';
const instrument = { instrument_name: name, price_step: 0.1, option_details: { option_type: 'C', strike: 3200 } };
const margin = { initial_margin: 1000, maintenance_margin: 1200, subaccount_value: 5000, collaterals_value: 5000, is_under_liquidation: false };
const ticker = { b: 10, a: 11, M: 10.5, I: 2500, option_pricing: { d: 0.1 } };

async function confirm(overrides = {}) {
  const updates = [], submitted = [], validationResults = [];
  let marginReads = 0, reviewerCalls = 0;
  const action = { id: 1, rule_id: 11, action: 'sell_call', instrument_name: name, amount: 1, price: 10, retries: 0,
    rule_criteria: { option_type: 'C', delta_range: [0.04, 0.12], dte_range: [5, 12], min_score: 65, min_bid: 8 },
    trigger_details: { dte: 8.5, delta: 0.1 } };
  const vote = { confirm: true, order_type: 'gtc', limit_price: overrides.price ?? 8, reasoning: 'Approved concrete order' };
  const ctx = {
    ...tradePolicy, ...pricing, Date: Clock, Math, Number, JSON,
    console: { log() {}, error() {} }, process: { env: {} },
    ANTHROPIC_SONNET_MODEL: 'mock', OPENAI_CONFIRMATION_MODEL: 'mock',
    PUT_DELTA_RANGE: [-0.12, -0.02], PUT_EXPIRATION_RANGE: [45, 78], CALL_DELTA_RANGE: [0.04, 0.12], CALL_EXPIRATION_RANGE: [5, 12],
    SELL_CALL_FALLBACK_MIN_SCORE: 65, SELL_CALL_FALLBACK_MIN_BID: 4, CALL_BUYBACK_PROFIT_THRESHOLD: 80,
    PUT_ROLL_DTE_THRESHOLD: 25, PUT_MONETIZATION_PROFIT_THRESHOLD: 1000, PUT_MONETIZATION_MAX_TRANCHE_FRACTION: 0.25,
    botData: { mediumTermMomentum: {}, putBudgetForCycle: 100, putUnspentBuyLimit: 0, putNetBought: 0 },
    db: {
      getPendingActions: () => [action], getOpenRestingOrders: () => [], getActiveTradeLessons: () => [], getRecentTradeReviews: () => [],
      getActiveRules: () => overrides.ruleExpired ? [] : [{ id: 11, action: action.action, criteria: action.rule_criteria }],
      updatePendingAction: (_id, update) => updates.push(update),
    },
    fetchSubaccount: async () => { marginReads++; return overrides.marginMissingAt === marginReads ? null : margin; },
    fetchPositions: async options => { assert.equal(options.throwOnError, true); return []; },
    fetchFreshTickerForInstrument: async () => overrides.tickerMissing ? null : ticker,
    estimateDisplayedMarginUtilization: () => 0.1,
    getCallMarginDecision: (_action, state) => ({ available: Boolean(state), entryCapSatisfied: true, marginPerUnit: 100 }),
    summarizeReservedEntryCapacity: () => ({ putBudget: 0 }),
    isEntryAction: value => ['buy_put', 'sell_call'].includes(value),
    isReduceOnlyExitAction: value => ['buyback_call', 'sell_put'].includes(value),
    parseMaybeJsonObject: value => typeof value === 'string' ? JSON.parse(value) : value,
    normalizePreferredOrderType: () => null, describeActionSemantics: () => '', buildBuybackConfirmationContext: () => null,
    getRecentFailedEntry: () => null, getAllowedOrderTypesForAction: () => ['ioc', 'gtc', 'post_only'],
    adaptOrderTypeFromFailureHistory: (_a, _b, type) => ({ orderType: type }),
    isRestingOrderType: value => ['gtc', 'post_only'].includes(value),
    getActionPolicy: () => ({ direction: 'sell' }), floorOrderAmountToVenuePrecision: value => Math.floor(value * 100) / 100,
    getAnthropicResponseText: response => response.text, extractConfirmationVote: JSON.parse,
    axios: { post: async () => { reviewerCalls++; return { data: { text: JSON.stringify(overrides.anthropicVote || vote) } }; } },
    callOpenAI: async () => { reviewerCalls++; return JSON.stringify(overrides.openaiVote || vote); }, sendTelegram() {},
    executeOrder: async (...args) => {
      const executionContext = args[9];
      assert.equal(typeof executionContext.validateOrder, 'function');
      const checked = await executionContext.validateOrder({ price: args[3], amount: args[2] });
      validationResults.push(checked);
      if (!checked.allowed) return { failed: true, reason: checked.reason };
      if (overrides.executionResult) return overrides.executionResult;
      submitted.push({ price: args[3], amount: args[2], bounds: executionContext.approvedBounds });
      return { resting: true, price: args[3] };
    },
  };
  for (const name of ['formatBuybackConfirmationContext', 'formatBuyPutConfirmationContext', 'formatSellCallConfirmationContext',
    'formatSellPutConfirmationContext', 'formatConfirmationLearningContext', 'getCallMarginContext', 'formatRecentExecutionFrictionContext',
    'getConfirmationScopePrompt', 'getActionOrderTypeHardRule', 'getConfirmationJsonOnlyPrompt', 'getSharedActionPolicyPrompt',
    'getCallMarginDisciplinePrompt', 'getCallBuybackDisciplinePrompt', 'getPutExitDisciplinePrompt']) ctx[name] = () => '';
  vm.createContext(ctx);
  vm.runInContext(`${confirmationSource}\nthis.run = confirmAndExecutePending`, ctx);
  await ctx.run([instrument], { [name]: ticker }, 2500);
  return { updates, submitted, validationResults, reviewerCalls, marginReads };
}

test('actual confirmation cannot submit a reviewer price below rule economics', async () => {
  const result = await confirm({ price: 5 });
  assert.equal(result.submitted.length, 0);
  assert.ok(result.updates.some(update => update.execution_result?.includes('call_price_failed')));
});

test('actual confirmation sends normalized order with immutable economic bounds and revalidation callback', async () => {
  const result = await confirm({ price: 8.01 });
  assert.equal(result.submitted.length, 1);
  assert.equal(result.submitted[0].price, 8.1);
  assert.equal(result.submitted[0].bounds.sellFloor, 8);
  assert.equal(result.marginReads, 3); // confirmation, final plan, execution preflight
});

test('missing margin defers pending order before spending reviewer calls', async () => {
  const result = await confirm({ marginMissingAt: 1 });
  assert.equal(result.submitted.length, 0);
  assert.equal(result.reviewerCalls, 0);
  assert.equal(result.updates.length, 0);
});

test('margin outage after reviewers cannot authorize submission', async () => {
  const result = await confirm({ marginMissingAt: 2 });
  assert.equal(result.submitted.length, 0);
  assert.ok(result.updates.some(update => update.execution_result?.includes('margin_unavailable')));
});

test('execution revalidation catches a later outage after initial confirmation passed', async () => {
  const result = await confirm({ marginMissingAt: 3 });
  assert.equal(result.submitted.length, 0);
  assert.equal(result.validationResults[0].code, 'margin_unavailable');
  assert.equal(result.updates.at(-1).status, 'failed');
});

test('pre-send execution failure clears confirmed status without claiming a submission', async () => {
  const result = await confirm({ executionResult: { failed: true, reason: 'positions unavailable' } });
  assert.equal(result.submitted.length, 0);
  assert.ok(result.updates.some(update => update.status === 'confirmed'));
  assert.equal(result.updates.at(-1).status, 'failed');
  assert.equal(result.updates.at(-1).execution_result, 'positions unavailable');
});

test('changed or withdrawn rule cannot execute after reviewers finish', async () => {
  const result = await confirm({ ruleExpired: true });
  assert.equal(result.submitted.length, 0);
  assert.ok(result.updates.some(update => update.execution_result?.includes('inactive_rule')));
});

test('explicit reviewer veto survives margin-cap keywords and a valid order', async () => {
  const result = await confirm({ openaiVote: { confirm: false, reasoning: 'Margin cap passes but concentration exceeds acceptable risk' } });
  assert.equal(result.submitted.length, 0);
  assert.equal(result.updates.at(-1).status, 'rejected');
});

test('missing ticker defers pending order before reviewer calls', async () => {
  const result = await confirm({ tickerMissing: true });
  assert.equal(result.submitted.length, 0);
  assert.equal(result.reviewerCalls, 0);
});

test('price normalization preserves a price already on a decimal tick', () => {
  assert.equal(pricing.normalizeOrderPriceForVenue(13.6, instrument, 'buy').price, 13.6);
  assert.equal(pricing.normalizeOrderPriceForVenue(13.6, instrument, 'sell').price, 13.6);
});

test('sub-tick buy cap has no valid normalized price', () => {
  assert.equal(pricing.normalizeOrderPriceForVenue(0.05, instrument, 'buy').price, 0);
});

test('maker retries never worsen either direction of the approved price', () => {
  assert.ok(pricing.computePostOnlyRetryPrice('sell', { b: 5, a: 6 }, instrument, 10).retryPrice >= 10);
  assert.ok(pricing.computePostOnlyRetryPrice('buy', { b: 20, a: 21 }, instrument, 10).retryPrice <= 10);
});
