'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const Database = require('better-sqlite3');
const { loadProduction, declaration } = require('./helpers/load-production');
const tradePolicy = require('../bot/trade-policy');
const pricing = require('../bot/order-pricing');
const entryPlan = require('../bot/resting-entry-plan');
const source = fs.readFileSync(path.join(__dirname, '..', 'script.js'), 'utf8');
const now = Date.parse('2030-12-01T20:00:00Z');
class Clock extends Date { constructor(...args) { super(...(args.length ? args : [now])); } static now() { return now; } }
const instrumentName = 'ETH-20301210-3200-C';
const instrument = { instrument_name: instrumentName, price_step: 0.1, option_details: { option_type: 'C', strike: 3200 } };
const margin = { initial_margin: 1000, maintenance_margin: 1200, subaccount_value: 5000, collaterals_value: 5000, is_under_liquidation: false };
const ticker = { b: 10, a: 11, M: 10.5, I: 2500, option_pricing: { d: 0.1 } };
const helpers = loadProduction([
  'getActionPolicy', 'isReduceOnlyExitAction', 'isEntryAction', 'isRestingOrderType',
  'parseMaybeJsonObject', 'normalizePreferredOrderType', 'describeActionSemantics',
  'getAllowedOrderTypesForAction', 'floorOrderAmountToVenuePrecision', 'buildBuybackConfirmationContext',
  'isVenueOrderAmountTradable', 'computeDteFromInstrumentName', 'inferActionFromOpenOrder',
  'validateRestingSellCallEntryOrder',
], { bindings: { Date: Clock, estimateDisplayedMarginUtilization: () => 0.1, getEffectiveCallExposureCapPct: () => 0.45 } });

function oldOrder(changes = {}) {
  return { order_id: 'old-entry', action: 'sell_call', instrument_name: instrumentName,
    direction: 'sell', amount: 5, filled_amount: 1, filled_value: 12, average_price: 12,
    limit_price: 12, time_in_force: 'post_only', order_type: 'post_only', status: 'open', order_status: 'open',
    creation_timestamp: now - 30 * 60000, ...changes };
}

// The complete production confirmation and final validator run. Only reviewer,
// venue and replacement transport boundaries are controlled by the fixture.
async function confirmEntry(overrides = {}) {
  const updates = [], prompts = [], submitted = [], operations = [], validations = [], errors = [];
  let replaced = false;
  let reviewsComplete = false;
  const existing = oldOrder(overrides.order);
  const desired = { action: 'sell_call', instrument_name: instrumentName, direction: 'sell', amount: 4,
    limit_price: 11, order_type: 'post_only', time_in_force: 'post_only' };
  const action = { id: 31, rule_id: 1912, action: 'sell_call', instrument_name: instrumentName,
    amount: 4, price: 11, retries: 0, status: 'pending',
    rule_reasoning: 'Historical advisory: keep the existing $12 offer until fresh replacement review.',
    rule_criteria: { option_type: 'C', delta_range: [0.04, 0.12], dte_range: [5, 12], min_score: 65, min_bid: 8 },
    trigger_details: { entry_replacement: true, replacement_order_id: existing.order_id,
      replacement_order_snapshot: { ...existing, ...overrides.queuedOrder }, entry_approved_notional: null,
      desired_entry_order: desired, advisor_limit_price: 11, preferred_order_type: 'post_only' },
    ...overrides.action };
  const rule = { id: action.rule_id, rule_type: 'entry', action: action.action,
    criteria: action.rule_criteria, preferred_order_type: 'post_only' };
  const vote = { confirm: true, order_type: 'post_only', limit_price: 11, reasoning: 'Approve the concrete replacement' };
  const currentOrders = () => replaced ? [] : [{ ...existing }];
  const ctx = {
    ...tradePolicy, ...pricing, ...helpers, Date: Clock, Math, Number, JSON,
    require(name) {
      if (name === './bot/resting-entry-plan') return { ...entryPlan,
        buildRestingEntryPlan: args => entryPlan.buildRestingEntryPlan({ ...args, nowMs: now }) };
      return createRequire(path.join(__dirname, '..', 'script.js'))(name);
    },
    console: { log() {}, warn() {}, error: (...args) => errors.push(args.join(' ')) }, process: { env: {} },
    ANTHROPIC_SONNET_MODEL: 'fixture', OPENAI_CONFIRMATION_MODEL: 'fixture',
    PUT_DELTA_RANGE: [-0.12, -0.02], PUT_EXPIRATION_RANGE: [45, 78], CALL_DELTA_RANGE: [0.04, 0.12], CALL_EXPIRATION_RANGE: [5, 12],
    SELL_CALL_FALLBACK_MIN_SCORE: 65, SELL_CALL_FALLBACK_MIN_BID: 4, CALL_BUYBACK_PROFIT_THRESHOLD: 80,
    PUT_ROLL_DTE_THRESHOLD: 25, PUT_MONETIZATION_PROFIT_THRESHOLD: 1000, PUT_MONETIZATION_MAX_TRANCHE_FRACTION: 0.25,
    botData: { mediumTermMomentum: {}, putBudgetForCycle: 100, putUnspentBuyLimit: 0, putNetBought: 0 },
    db: {
      getPendingActions: () => [action], getOpenRestingOrders: currentOrders,
      getActiveTradeLessons: () => [], getRecentTradeReviews: () => [],
      getActiveRules: () => (replaced && overrides.finalRuleMissing) || (reviewsComplete && overrides.preCancelRuleMissing) ? [] : [rule],
      getActiveRulesByType: () => [rule],
      updatePendingAction: (_id, update) => { updates.push(update); Object.assign(action, update); },
    },
    fetchSubaccount: async () => (replaced && overrides.finalMarginMissing) || (reviewsComplete && overrides.preCancelMarginMissing) ? null : margin,
    fetchPositions: async options => { assert.equal(options.throwOnError, true); return []; },
    fetchOpenOrders: async options => {
      assert.equal(options.throwOnError, true);
      return replaced && overrides.finalBookChanged ? [oldOrder({ order_id: 'new-competing-entry' })] : currentOrders();
    },
    fetchFreshTickerForInstrument: async () => replaced && overrides.finalTicker ? overrides.finalTicker
      : reviewsComplete && overrides.preCancelTicker ? overrides.preCancelTicker : ticker,
    readFreshEntryOrderSnapshot: async () => {
      if (overrides.snapshotUnavailable) throw new Error('Venue entry order state is unknown');
      const orders = currentOrders();
      if (replaced && overrides.finalBookChanged) orders.push(oldOrder({ order_id: 'new-competing-entry' }));
      return { orders, observedAt: new Clock().toISOString() };
    },
    prepareEntryOrderReplacement: async options => {
      operations.push('replace');
      assert.equal(options.reviewedOrder.order_id, existing.order_id);
      assert.ok(options.desiredOrder.amount <= 4, 'Only the unfilled four contracts can be reviewed');
      if (overrides.replacementDenied) return { allowed: false, reason: 'Cancellation terminal state unknown' };
      replaced = true;
      return { allowed: true, amount: overrides.remainingAmount ?? options.desiredOrder.amount };
    },
    estimateDisplayedMarginUtilization: () => 0.1,
    getCallMarginDecision: (_action, state) => ({ available: Boolean(state), entryCapSatisfied: true, marginPerUnit: 100 }),
    summarizeReservedEntryCapacity: () => ({ putBudget: 0 }),
    getRecentFailedEntry: () => null,
    adaptOrderTypeFromFailureHistory: (_action, _name, orderType) => ({ orderType }),
    getAnthropicResponseText: response => response.text, extractConfirmationVote: JSON.parse,
    axios: { post: async (_url, body) => {
      operations.push('review-anthropic'); prompts.push(body.messages[0].content);
      if (overrides.missingReviewer === 'anthropic') throw new Error('fixture reviewer unavailable');
      return { data: { text: JSON.stringify(overrides.anthropicVote ?? vote) } };
    } },
    callOpenAI: async (_system, prompt) => {
      operations.push('review-openai'); prompts.push(prompt); reviewsComplete = true;
      if (overrides.missingReviewer === 'openai') throw new Error('fixture reviewer unavailable');
      return JSON.stringify(overrides.openaiVote ?? vote);
    },
    sendTelegram() {},
    executeOrder: async (...args) => {
      operations.push('execute-preflight');
      const checked = await args[9].validateOrder({ price: args[3], amount: args[2] });
      validations.push(checked);
      if (!checked.allowed) return { failed: true, reason: checked.reason };
      submitted.push({ price: args[3], amount: args[2], context: args[9] });
      return { resting: true, price: args[3] };
    },
  };
  for (const name of ['formatBuybackConfirmationContext', 'formatBuyPutConfirmationContext', 'formatSellCallConfirmationContext',
    'formatSellPutConfirmationContext', 'formatConfirmationLearningContext', 'getCallMarginContext', 'formatRecentExecutionFrictionContext',
    'getConfirmationScopePrompt', 'getActionOrderTypeHardRule', 'getConfirmationJsonOnlyPrompt', 'getSharedActionPolicyPrompt',
    'getCallMarginDisciplinePrompt', 'getCallBuybackDisciplinePrompt', 'getPutExitDisciplinePrompt']) ctx[name] = () => '';
  vm.createContext(ctx);
  const declarations = ['getFinalOrderPolicy', 'createFinalOrderValidator', 'validateEntryReplacementEconomics',
    ...['getEntryOrderFilledValue', 'formatEntryConfirmationSnapshot'].filter(name => source.includes(`const ${name} =`)),
    'confirmAndExecutePending'];
  vm.runInContext(`${declarations.map(name => declaration(source, name)).join('\n')}\nthis.run = confirmAndExecutePending`, ctx);
  await ctx.run([instrument], { [instrumentName]: ticker }, 2500);
  return { updates, prompts, submitted, operations, validations, errors, action, existing, replaced };
}

test('entry reviewers see the existing order, remaining quantity and concrete replacement before cancellation', async () => {
  const result = await confirmEntry();
  assert.deepEqual(result.errors, []);
  assert.equal(result.prompts.length, 2);
  for (const prompt of result.prompts) {
    assert.match(prompt, /replac/i);
    assert.match(prompt, /old-entry/);
    assert.match(prompt, /Original entry at review:[^\n]*"limit_price":12[^\n]*"remaining_amount":4/);
    assert.match(prompt, /"limit_price":10\.1/);
    assert.match(prompt, /remaining[^\n]*4|"amount":4/i);
    assert.match(prompt, /not an additional entry/i);
  }
  assert.equal(result.submitted.length, 1);
  assert.equal(result.submitted[0].amount, 4);
  assert.ok(result.operations.indexOf('replace') > result.operations.indexOf('review-openai'));

  const stale = await confirmEntry({ queuedOrder: { limit_price: 99, filled_amount: 0, filled_value: 0 } });
  assert.deepEqual(stale.errors, []);
  assert.equal(stale.submitted[0].amount, 3, 'The fill between queueing and review consumes one approved contract');
  for (const prompt of stale.prompts) {
    assert.match(prompt, /Original entry at review:[^\n]*"limit_price":12[^\n]*"remaining_amount":4/);
    assert.match(prompt, /Concrete desired entry order:[^\n]*"amount":3/);
    assert.doesNotMatch(prompt, /Original entry at review:[^\n]*"limit_price":99/);
  }
});

test('either reviewer veto leaves the original entry untouched', async () => {
  for (const reviewer of ['anthropicVote', 'openaiVote']) {
    const result = await confirmEntry({ [reviewer]: { confirm: false, reasoning: 'Keep the original offer' } });
    assert.equal(result.prompts.length, 2);
    assert.equal(result.replaced, false);
    assert.equal(result.operations.includes('replace'), false);
    assert.equal(result.submitted.length, 0);
    assert.equal(result.action.status, 'rejected');
  }
});

test('a missing reviewer defers replacement without cancelling the original entry', async () => {
  for (const missingReviewer of ['anthropic', 'openai']) {
    const result = await confirmEntry({ missingReviewer });
    assert.equal(result.prompts.length, 2);
    assert.equal(result.replaced, false);
    assert.equal(result.operations.includes('replace'), false);
    assert.equal(result.submitted.length, 0);
    assert.equal(result.action.status, 'pending');
    assert.equal(result.action.retries, 1);
  }
});

test('unavailable live entry snapshot defers before either reviewer or cancellation', async () => {
  const result = await confirmEntry({ snapshotUnavailable: true });
  assert.equal(result.prompts.length, 0);
  assert.equal(result.submitted.length, 0);
  assert.equal(result.replaced, false);
  assert.equal(result.action.status, 'pending');
});

test('unknown cancellation outcome cannot reach replacement submission', async () => {
  const result = await confirmEntry({ replacementDenied: true });
  assert.equal(result.prompts.length, 2);
  assert.ok(result.operations.includes('replace'));
  assert.equal(result.operations.includes('execute-preflight'), false);
  assert.equal(result.submitted.length, 0);
  assert.equal(result.replaced, false);
});

test('late fills reduce replacement quantity before the final execution guard', async () => {
  const result = await confirmEntry({ remainingAmount: 1.25 });
  assert.deepEqual(result.errors, []);
  assert.equal(result.submitted.length, 1);
  assert.equal(result.submitted[0].amount, 1.25);
  assert.equal(result.validations[0].allowed, true);
});

test('fresh rule, margin, executable quote and overlapping book failures prevent replacement submission', async () => {
  for (const [scenario, reason] of [
    [{ preCancelRuleMissing: true }, /rule changed/i],
    [{ preCancelMarginMissing: true }, /fresh usable margin/i],
    [{ preCancelTicker: { ...ticker, b: 1 } }, /below min_bid|call bid/i],
  ]) {
    const result = await confirmEntry(scenario);
    assert.deepEqual(result.errors, []);
    assert.equal(result.prompts.length, 2);
    assert.equal(result.replaced, false, 'Fresh pre-cancel failures must keep the original entry');
    assert.equal(result.operations.includes('replace'), false);
    assert.equal(result.submitted.length, 0);
    assert.ok(result.updates.some(update => reason.test(String(update.execution_result))), JSON.stringify(result.updates));
  }
  for (const [scenario, code] of [
    [{ finalRuleMissing: true }, 'inactive_rule'], [{ finalMarginMissing: true }, 'margin_unavailable'],
    [{ finalTicker: { ...ticker, b: 1 } }, 'call_quote_failed'], [{ finalBookChanged: true }, 'existing_entry'],
  ]) {
    const result = await confirmEntry(scenario);
    assert.deepEqual(result.errors, []);
    assert.equal(result.prompts.length, 2);
    assert.equal(result.submitted.length, 0, JSON.stringify(scenario));
    assert.ok(result.updates.some(update => String(update.execution_result).includes(code)), JSON.stringify(result.updates));
  }
});

// Exercise the actual cancellation, fill reconciliation and reservation logic
// against the production SQLite schema, with no venue clients or bot startup.
function replacementRuntime(t) {
  const root = path.resolve(__dirname, '..');
  const filename = path.join(root, 'bot/db.js');
  const localRequire = createRequire(filename);
  const sqlite = new Database(':memory:');
  t.after(() => { if (sqlite.open) sqlite.close(); });
  const quiet = { log() {}, warn() {}, error() {} };
  const sandbox = {
    __dirname: path.dirname(filename), process: { env: { DATA_DIR: os.tmpdir() } },
    console: quiet, module: { exports: {} },
    require(name) { return name === 'better-sqlite3' ? function () { return sqlite; } : localRequire(name); },
  };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), sandbox, { filename });
  const db = sandbox.module.exports;
  const name = 'ETH-20310214-1800-P';
  const action = { action: 'buy_put', instrument_name: name };
  const botData = { putNetBought: 0, putBudgetForCycle: 100, putUnspentBuyLimit: 0 };
  db.saveBotState(botData);
  const state = { db, botData, action, venue: [], statuses: new Map(), events: [], alerts: [], onCancel: null };
  const venueOrder = changes => ({ order_id: 'old-put', instrument_name: name, direction: 'buy',
    amount: '5', filled_amount: '0', average_price: '0', limit_price: '8',
    order_status: 'open', time_in_force: 'post_only', ...changes });
  state.seed = changes => {
    const live = venueOrder(changes);
    const pendingId = Number(db.insertPendingAction({ action: action.action, instrument_name: live.instrument_name,
      amount: Number(live.amount), price: Number(live.limit_price) }).lastInsertRowid);
    db.updatePendingAction(pendingId, { status: 'resting' });
    db.insertRestingOrder({ order_id: live.order_id, pending_action_id: pendingId,
      instrument_name: live.instrument_name, action: action.action, direction: 'buy',
      amount: Number(live.amount), limit_price: Number(live.limit_price), filled_amount: 0, filled_value: 0 });
    state.venue.push(live);
    state.statuses.set(live.order_id, live);
    return live;
  };
  const bindings = {
    db, botData,
    require: createRequire(path.join(root, 'script.js')),
    notifyOrderLifecycle: alert => state.alerts.push(alert),
    fetchOpenOrders: async options => {
      assert.equal(options.throwOnError, true);
      state.events.push('open');
      return state.venue.map(row => ({ ...row }));
    },
    fetchOrderStatus: async id => {
      state.events.push(`status:${id}`);
      const row = state.statuses.get(id);
      return row ? { ...row } : null;
    },
    cancelOrder: async id => {
      state.events.push(`cancel:${id}`);
      if (state.onCancel) return state.onCancel(id);
      state.venue = state.venue.filter(order => order.order_id !== id);
      state.statuses.set(id, { ...state.statuses.get(id), order_status: 'cancelled' });
      return { cancelled: true };
    },
  };
  const names = ['VENUE_AMOUNT_DECIMALS', 'VENUE_MIN_ORDER_AMOUNT', 'ACTION_POLICY', 'getActionPolicy', 'isEntryAction', 'inferActionFromOpenOrder',
    'floorOrderAmountToVenuePrecision', 'isVenueOrderAmountTradable', 'getEntryOrderFilledValue',
    'accountEntryOrderObservation', 'readFreshEntryOrderSnapshot', 'prepareEntryOrderReplacement'];
  const execute = vm.compileFunction(`${names.map(name => declaration(source, name)).join('\n')}
    return { readFreshEntryOrderSnapshot, prepareEntryOrderReplacement };`, Object.keys(bindings));
  Object.assign(state, execute(...Object.values(bindings)));
  state.seed();
  state.review = async () => {
    const snapshot = await state.readFreshEntryOrderSnapshot(action);
    assert.equal(snapshot.orders.length, 1);
    return Object.freeze({ ...snapshot.orders[0] });
  };
  state.prepare = (reviewedOrder, approvedNotional = 32) => state.prepareEntryOrderReplacement({ action,
    reviewedOrder, desiredOrder: { ...action, direction: 'buy', amount: 4, limit_price: 10, order_type: 'post_only' },
    approvedNotional });
  state.stored = () => db.db.prepare('SELECT order_id,status,filled_amount,filled_value FROM resting_orders ORDER BY order_id').all();
  state.fills = () => db.db.prepare('SELECT filled_amount,total_value,fill_price FROM orders ORDER BY id').all();
  return state;
}

test('actual replacement accounts late fills once and respects both reviewed quantity and PUT dollar authorization', async t => {
  const state = replacementRuntime(t);
  Object.assign(state.venue[0], { filled_amount: '1', average_price: '8' });
  const reviewed = await state.review();
  Object.assign(state.venue[0], { filled_amount: '2', average_price: '9' });
  state.onCancel = async id => {
    state.venue = [];
    state.statuses.set(id, { ...state.statuses.get(id), filled_amount: '3', average_price: '9', order_status: 'cancelled' });
    return { cancelled: true };
  };
  const result = await state.prepare(reviewed);
  assert.equal(result.allowed, true);
  assert.equal(result.amount, 1.3, '$32 review allowance minus $19 later fills permits $13 of new purchases');
  assert.ok(result.amount <= 4 - (3 - 1), 'Late fills also consume the approved contract count');
  assert.deepEqual(state.fills(), [
    { filled_amount: 1, total_value: 8, fill_price: 8 },
    { filled_amount: 1, total_value: 10, fill_price: 10 },
    { filled_amount: 1, total_value: 9, fill_price: 9 },
  ]);
  assert.equal(state.db.loadBotState().put_net_bought, 27);
  assert.equal(reviewed.filled_amount, '1');
  assert.equal(reviewed.filled_value, 8);
  assert.equal(state.stored()[0].status, 'cancelled');
  const repeat = await state.prepare(reviewed);
  assert.equal(repeat.amount, result.amount);
  assert.equal(state.fills().length, 3);
  assert.equal(state.events.filter(event => event === 'cancel:old-put').length, 1);
  const quantityBound = await state.prepare(reviewed, 100);
  assert.equal(quantityBound.amount, 2, 'With ample dollar authority, the two later fills still consume two of four approved contracts');
  assert.equal(state.fills().length, 3);
});

test('actual cancellation ACK without terminal evidence cannot release the original reservation', async t => {
  for (const missingStatus of [false, true]) {
    const state = replacementRuntime(t);
    const reviewed = await state.review();
    state.onCancel = async id => {
      if (missingStatus) { state.venue = []; state.statuses.delete(id); }
      return { cancelled: true };
    };
    const result = await state.prepare(reviewed);
    assert.equal(result.allowed, false);
    assert.match(result.reason, /unknown|remains open/i);
    assert.equal(state.stored()[0].status, 'open');
    assert.equal(state.stored()[0].filled_amount, 0);
    assert.equal(state.db.loadBotState().put_net_bought, 0);
    assert.deepEqual(state.fills(), []);
  }
});

test('actual action-wide entry snapshots block a competing contract before or after cancellation', async t => {
  for (const afterCancellation of [false, true]) {
    const state = replacementRuntime(t);
    const reviewed = await state.review();
    const competing = { order_id: 'other-put', instrument_name: 'ETH-20310214-1900-P' };
    if (afterCancellation) {
      state.onCancel = async id => {
        state.venue = [];
        state.statuses.set(id, { ...state.statuses.get(id), order_status: 'cancelled' });
        state.seed(competing);
        return { cancelled: true };
      };
    } else state.seed(competing);
    const result = await state.prepare(reviewed);
    assert.equal(result.allowed, false);
    assert.match(result.reason, /another entry|remains on the venue/i);
    assert.equal(state.events.filter(event => event === 'cancel:old-put').length, Number(afterCancellation));
    assert.equal(state.stored().find(order => order.order_id === 'other-put').status, 'open');
  }
});
