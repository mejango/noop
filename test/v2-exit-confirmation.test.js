'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const { loadProduction, declaration } = require('./helpers/load-production');
const tradePolicy = require('../bot/trade-policy');
const pricing = require('../bot/order-pricing');
const restingPlan = require('../bot/resting-exit-plan');
const source = fs.readFileSync(path.join(__dirname, '..', 'script.js'), 'utf8');
const fixedTime = Date.parse('2030-09-12T12:00:00Z');
class Clock extends Date { constructor(...args) { super(...(args.length ? args : [fixedTime])); } static now() { return fixedTime; } }
const name = 'ETH-20300918-2800-C';
const instrument = { instrument_name: name, price_step: 0.1, option_details: { option_type: 'C', strike: 2800 } };
const margin = { initial_margin: 1000, maintenance_margin: 1200, subaccount_value: 5000, collaterals_value: 5000, is_under_liquidation: false };
const ticker = { b: 2.1, a: 3.1, M: 2.6, I: 2500, option_pricing: { d: 0.04 } };
const position = { instrument_name: name, direction: 'short', amount: 12.5, avg_entry_price: 11 };
const oldReasoning = 'An 8h-old bid at $2.2 is still on the book; cancel and replace it.';

const helpers = loadProduction([
  'getPatientBuybackPlan', 'refinePatientBuybackPlanPrice', 'getBuybackCaptureGate', 'getBuybackCapturePctAtPrice',
  'buildBuybackConfirmationContext', 'formatBuybackConfirmationContext',
  'getSyntheticExitIntent', 'isSyntheticRestingExitIntentAllowed', 'getCloseablePositionForExit',
  'getExistingRestingExitOrder', 'getSellPutExitAmount', 'getBuybackIntent', 'getPutExitIntent',
  'getActionPolicy', 'isReduceOnlyExitAction', 'isEntryAction', 'isRestingOrderType',
  'parseMaybeJsonObject', 'normalizePreferredOrderType', 'describeActionSemantics',
  'getAllowedOrderTypesForAction', 'floorOrderAmountToVenuePrecision',
  ...(source.includes('const formatExitConfirmationSnapshot =') ? ['formatExitConfirmationSnapshot'] : []),
], { bindings: { Date: Clock, ...restingPlan } });

function restingOrder(overrides = {}) {
  return { order_id: 'existing-order', action: 'buyback_call', instrument_name: name,
    direction: 'buy', amount: 12.5, filled_amount: 0, limit_price: 2.2,
    time_in_force: 'post_only', order_type: 'post_only', exit_intent: 'profit_capture',
    status: 'open', order_status: 'open', creation_timestamp: fixedTime - 30 * 60000, ...overrides };
}

async function confirmExit(overrides = {}) {
  const updates = [], prompts = [], submitted = [], operations = [];
  const positions = overrides.positions ?? [position];
  const currentTicker = overrides.ticker ?? ticker;
  const existing = overrides.existing ?? null;
  const restingOrders = existing ? [existing] : [];
  const action = { id: 31, rule_id: 1912, action: 'buyback_call', instrument_name: name,
    amount: position.amount, price: 2.2, retries: 0, status: 'pending', rule_reasoning: oldReasoning,
    rule_criteria: { buyback_intent: 'profit_capture', max_buyback_price: 2.2,
      conditions: [{ field: 'unrealized_pnl_pct', op: 'gte', value: 80 }], condition_logic: 'all' },
    trigger_details: { advisor_limit_price: 2.2, preferred_order_type: 'post_only', buyback_intent: 'profit_capture' },
    ...overrides.action,
  };
  const activeRules = overrides.inactiveRule ? [] : [{ id: action.rule_id, rule_type: 'exit', action: action.action,
    instrument_name: name, criteria: action.rule_criteria, preferred_order_type: 'post_only' }];
  const vote = { confirm: true, order_type: 'post_only', limit_price: 2.2, reasoning: 'Current exit facts support this order' };
  const ctx = {
    ...tradePolicy, ...pricing, ...restingPlan, ...helpers, Date: Clock, Math, Number, JSON,
    liveRuleValues: (position, ticker, spot, now = fixedTime) => tradePolicy.liveRuleValues(position, ticker, spot, now),
    require: createRequire(path.join(__dirname, '..', 'script.js')),
    console: { log() {}, warn() {}, error() {} }, process: { env: {} },
    ANTHROPIC_SONNET_MODEL: 'fixture', OPENAI_CONFIRMATION_MODEL: 'fixture',
    PUT_DELTA_RANGE: [-0.12, -0.02], PUT_EXPIRATION_RANGE: [45, 78], CALL_DELTA_RANGE: [0.04, 0.12], CALL_EXPIRATION_RANGE: [5, 12],
    SELL_CALL_FALLBACK_MIN_SCORE: 65, SELL_CALL_FALLBACK_MIN_BID: 4, CALL_BUYBACK_PROFIT_THRESHOLD: 80,
    PUT_ROLL_DTE_THRESHOLD: 25, PUT_MONETIZATION_PROFIT_THRESHOLD: 1000, PUT_MONETIZATION_MAX_TRANCHE_FRACTION: 0.25,
    PUT_ROLL_MIN_RECOVERY_PCT: 40, PUT_MONETIZATION_MIN_INTRINSIC_FRACTION: 0.95,
    getPutMonetizationThresholdPct: () => 1000, getPutExitIntent: criteria => criteria?.put_exit_intent || null,
    botData: { mediumTermMomentum: {}, putBudgetForCycle: 100, putUnspentBuyLimit: 0, putNetBought: 0 },
    db: {
      getPendingActions: () => [action], getOpenRestingOrders: () => restingOrders,
      getActiveTradeLessons: () => [], getRecentTradeReviews: () => [], getActiveRules: () => activeRules,
      getActiveRulesByType: () => activeRules,
      updatePendingAction: (_id, update) => { updates.push(update); Object.assign(action, update); },
    },
    fetchSubaccount: async () => margin,
    fetchPositions: async options => { assert.equal(options.throwOnError, true); return positions; },
    fetchOpenOrders: async options => { assert.equal(options.throwOnError, true); return restingOrders; },
    fetchFreshTickerForInstrument: async () => currentTicker,
    readFreshExitOrderSnapshot: async () => {
      if (overrides.snapshotUnavailable) throw new Error('Venue order state is unknown');
      return { orders: restingOrders, observedAt: new Clock().toISOString() };
    },
    prepareExitOrderReplacement: async ({ reviewedOrders, desiredOrder }) => {
      operations.push(reviewedOrders.length ? 'replace' : 'replacement-preflight');
      return { allowed: true, amount: desiredOrder.amount, positions };
    },
    estimateDisplayedMarginUtilization: () => 0.1,
    getCallMarginDecision: () => ({ available: true, entryCapSatisfied: true, marginPerUnit: 100 }),
    summarizeReservedEntryCapacity: () => ({ putBudget: 0 }),
    getRecentFailedEntry: () => null,
    adaptOrderTypeFromFailureHistory: (_action, _name, orderType) => ({ orderType }),
    getAnthropicResponseText: response => response.text, extractConfirmationVote: JSON.parse,
    axios: { post: async (_url, body) => {
      operations.push('review-anthropic'); prompts.push(body.messages[0].content);
      return { data: { text: JSON.stringify(overrides.anthropicVote ?? vote) } };
    } },
    callOpenAI: async (_system, prompt) => {
      operations.push('review-openai'); prompts.push(prompt);
      return JSON.stringify(overrides.openaiVote ?? vote);
    },
    sendTelegram() {},
    executeOrder: async (...args) => {
      operations.push('execute');
      const checked = await args[9].validateOrder({ price: args[3], amount: args[2] });
      assert.equal(checked.allowed, true, checked.reason);
      submitted.push({ price: args[3], amount: args[2], context: args[9] });
      return { resting: true, price: args[3] };
    },
  };
  for (const name of ['formatBuyPutConfirmationContext', 'formatSellCallConfirmationContext', 'formatSellPutConfirmationContext',
    'formatConfirmationLearningContext', 'getCallMarginContext', 'formatRecentExecutionFrictionContext',
    'getConfirmationScopePrompt', 'getActionOrderTypeHardRule', 'getConfirmationJsonOnlyPrompt', 'getSharedActionPolicyPrompt',
    'getCallMarginDisciplinePrompt', 'getCallBuybackDisciplinePrompt', 'getPutExitDisciplinePrompt']) ctx[name] = () => '';
  vm.createContext(ctx);
  const declarations = ['getFinalOrderPolicy', 'createFinalOrderValidator', 'confirmAndExecutePending'];
  vm.runInContext(`${declarations.map(name => declaration(source, name)).join('\n')}\nthis.run = confirmAndExecutePending`, ctx);
  await ctx.run([instrument], { [name]: currentTicker }, 2500);
  return { updates, prompts, submitted, operations, action };
}

test('an equivalent resting exit is skipped before either reviewer is called', async () => {
  const result = await confirmExit({ existing: restingOrder() });
  assert.equal(result.prompts.length, 0);
  assert.equal(result.submitted.length, 0);
  assert.notEqual(result.action.status, 'pending');
  assert.notEqual(result.action.status, 'rejected');
});

test('pending exits from an inactive advisory rule are retired before reviewer calls', async () => {
  const result = await confirmExit({ inactiveRule: true });
  assert.equal(result.prompts.length, 0);
  assert.equal(result.submitted.length, 0);
  assert.notEqual(result.action.status, 'pending');
});

test('a new exit presents saved stale-order prose as historical alongside fresh book and closeable size', async () => {
  const result = await confirmExit();
  assert.equal(result.prompts.length, 2);
  assert.equal(result.submitted.length, 1);
  for (const prompt of result.prompts) {
    assert.match(prompt, /historical[^\n]*8h-old bid|historical[^\n]*\n[^\n]*8h-old bid/i);
    assert.match(prompt, /(?:current|fresh|live)[^\n]*(?:resting|open.order|exit orders)[^\n]*(?:none|\[\]|0\b)/i);
    assert.match(prompt, /closeable[^\n]*12\.5/i);
    assert.doesNotMatch(prompt, /Concrete desired exit order: null/);
  }
});

test('a different resting price reaches reviewers with explicit replacement context', async () => {
  const result = await confirmExit({ existing: restingOrder({ limit_price: 2.5 }) });
  assert.equal(result.prompts.length, 2);
  for (const prompt of result.prompts) {
    assert.match(prompt, /replac/i);
    assert.match(prompt, /existing-order/);
    assert.match(prompt, /2\.5/);
    assert.match(prompt, /2\.2/);
  }
  assert.equal(result.submitted.length, 1);
  assert.deepEqual(result.operations, ['review-anthropic', 'review-openai', 'replace', 'execute']);
});

test('an exit without a live closeable position stops before reviewer calls', async () => {
  const result = await confirmExit({ positions: [] });
  assert.equal(result.prompts.length, 0);
  assert.equal(result.submitted.length, 0);
});

test('remaining quantity makes a partially filled order equivalent to the desired close', async () => {
  const result = await confirmExit({ existing: restingOrder({ amount: 15, filled_amount: 2.5 }) });
  assert.equal(result.prompts.length, 0);
  assert.equal(result.submitted.length, 0);
});

test('a reviewer veto of a changed exit leaves the existing order untouched', async () => {
  const result = await confirmExit({ existing: restingOrder({ limit_price: 2.5 }),
    openaiVote: { confirm: false, reasoning: 'Do not replace this order under current conditions' } });
  assert.equal(result.prompts.length, 2);
  assert.deepEqual(result.operations, ['review-anthropic', 'review-openai']);
  assert.equal(result.submitted.length, 0);
  assert.equal(result.action.status, 'rejected');
});

test('unavailable live order state defers confirmation without reviewer calls', async () => {
  const result = await confirmExit({ snapshotUnavailable: true });
  assert.equal(result.prompts.length, 0);
  assert.equal(result.submitted.length, 0);
  assert.equal(result.action.status, 'pending');
});

test('historical trigger capture cannot override refreshed executable capture in the reviewer context', async () => {
  const result = await confirmExit({ action: { trigger_details: {
    advisor_limit_price: 2.2, preferred_order_type: 'post_only', buyback_intent: 'profit_capture',
    conditions_met: [{ field: 'unrealized_pnl_pct', op: 'gte', threshold: 80, actual: 99 }],
  } } });
  assert.equal(result.prompts.length, 2);
  for (const prompt of result.prompts) {
    assert.match(prompt, /Current executable capture[^\n]*71\.82%/);
    assert.doesNotMatch(prompt, /Current executable capture[^\n]*99\.00%/);
  }
});
