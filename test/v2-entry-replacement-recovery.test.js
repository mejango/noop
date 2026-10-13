'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const { declaration, loadProduction } = require('./helpers/load-production');
const tradePolicy = require('../bot/trade-policy');
const entryPlan = require('../bot/resting-entry-plan');

const root = path.resolve(__dirname, '..');
const scriptPath = path.join(root, 'script.js');
const now = Date.parse('2030-09-28T08:00:00Z');
class Clock extends Date {
  constructor(...args) { super(...(args.length ? args : [now])); }
  static now() { return now; }
}
const instrumentName = 'ETH-20301127-1600-P';
const instrument = { instrument_name: instrumentName, price_step: 0.1,
  option_details: { option_type: 'P', strike: 1600 } };
const margin = { initial_margin: 1000, maintenance_margin: 1200, subaccount_value: 5000,
  collaterals_value: 5000, is_under_liquidation: false };
const quiet = { log() {}, warn() {}, error() {} };

// Run the complete confirmation/reconciliation path against the real SQLite
// schema. Restart closes the connection and rebuilds every runtime object from
// disk, so recovery cannot depend on a mutated action or an in-memory snapshot.
function recoveryRuntime(t) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-entry-recovery-'));
  const state = { directory, venue: [], receipt: null, events: [], prompts: [], submissions: [],
    finalChecks: [], errors: [], research: [], finalMargin: undefined, preparingFinal: false,
    ticker: { a: 10, b: 7, M: 8.5, I: 2500, option_pricing: { d: -0.06, i: 0.7 } } };
  t.after(() => { state.db?.close(); fs.rmSync(directory, { recursive: true, force: true }); });

  state.restart = () => {
    state.db?.close();
    const filename = path.join(root, 'bot/db.js');
    const dbContext = { __dirname: path.dirname(filename), module: { exports: {} }, console: quiet,
      process: { env: { DATA_DIR: directory } }, require: createRequire(filename) };
    vm.runInNewContext(fs.readFileSync(filename, 'utf8'), dbContext, { filename });
    const db = state.db = dbContext.module.exports;
    const saved = db.loadBotState();
    const botData = state.botData = { mediumTermMomentum: {}, putNetBought: saved?.put_net_bought ?? 8,
      putBudgetForCycle: 100, putUnspentBuyLimit: 0 };
    const source = fs.readFileSync(scriptPath, 'utf8');
    const helpers = loadProduction(['getActionPolicy', 'isReduceOnlyExitAction', 'isEntryAction',
      'isRestingOrderType', 'parseMaybeJsonObject', 'normalizePreferredOrderType', 'describeActionSemantics',
      'getAllowedOrderTypesForAction', 'floorOrderAmountToVenuePrecision', 'buildBuybackConfirmationContext',
      'isVenueOrderAmountTradable', 'computeDteFromInstrumentName', 'inferActionFromOpenOrder',
      'validateRestingBuyPutEntryOrder', 'summarizeReservedEntryCapacity', 'getTickerImpliedVol',
      'getLatestHourlyDeltaPct', 'getCloseablePositionForExit'], { source, bindings: { Date: Clock } });
    const ctx = { ...tradePolicy, ...require('../bot/order-pricing'), ...require('../bot/put-score'),
      ...helpers, Date: Clock, Math, Number, JSON, db, botData,
      require(name) {
        if (name === './bot/resting-entry-plan') return { ...entryPlan,
          buildRestingEntryPlan: args => entryPlan.buildRestingEntryPlan({ ...args, nowMs: now }) };
        return createRequire(scriptPath)(name);
      },
      console: { ...quiet, error: (...args) => state.errors.push(args.join(' ')) }, process: { env: {} },
      ANTHROPIC_SONNET_MODEL: 'fixture', OPENAI_CONFIRMATION_MODEL: 'fixture',
      PUT_DELTA_RANGE: [-0.12, -0.02], PUT_EXPIRATION_RANGE: [45, 78],
      CALL_DELTA_RANGE: [0.04, 0.12], CALL_EXPIRATION_RANGE: [5, 12],
      SELL_CALL_FALLBACK_MIN_SCORE: 65, SELL_CALL_FALLBACK_MIN_BID: 4,
      CALL_BUYBACK_PROFIT_THRESHOLD: 80, PUT_ROLL_DTE_THRESHOLD: 25,
      PUT_MONETIZATION_PROFIT_THRESHOLD: 1000, PUT_MONETIZATION_MAX_TRANCHE_FRACTION: 0.25,
      PUT_ROLL_MIN_RECOVERY_PCT: 40, PUT_MONETIZATION_MIN_INTRINSIC_FRACTION: 0.95,
      getPutMonetizationThresholdPct: () => 1000,
      getPutExitIntent: criteria => criteria?.put_exit_intent || null,
      estimateDisplayedMarginUtilization: () => 0.1,
      getCallMarginDecision: () => null,
      getRecentFailedEntry: () => null,
      adaptOrderTypeFromFailureHistory: (_action, _name, orderType) => ({ orderType }),
      buildLiveSellCallMarketContext: () => ({}),
      classifyBuyPutEdge: input => { state.research.push(input); return { selection_score: 90 }; },
      fetchFreshTickerForInstrument: async () => ({ ...state.ticker }),
      fetchSubaccount: async () => state.preparingFinal && state.finalMargin !== undefined ? state.finalMargin : margin,
      fetchPositions: async options => { assert.equal(options.throwOnError, true); return []; },
      fetchOpenOrders: async options => {
        assert.equal(options.throwOnError, true);
        state.events.push('book');
        if (state.failBook) throw new Error('temporary book read failure');
        return state.venue.map(order => ({ ...order }));
      },
      fetchOrderStatus: async id => {
        state.events.push(`status:${id}`);
        if (state.failStatus) throw new Error('temporary status read failure');
        return state.receipt && { ...state.receipt };
      },
      cancelOrder: async id => {
        state.events.push(`cancel:${id}`);
        // The request is accepted but neither the open book nor status endpoint
        // can yet prove the terminal state.
        state.venue = [];
        state.receipt = null;
        if (state.onCancel) return state.onCancel(id);
        return { cancelled: true };
      },
      notifyOrderLifecycle() {}, sendTelegram() {},
      getAnthropicResponseText: response => response.text, extractConfirmationVote: JSON.parse,
      axios: { post: async (_url, body) => {
        state.prompts.push(body.messages[0].content);
        return { data: { text: JSON.stringify({ confirm: true, order_type: 'post_only', limit_price: null }) } };
      } },
      callOpenAI: async (_system, prompt) => {
        state.prompts.push(prompt);
        return JSON.stringify({ confirm: true, order_type: 'post_only', limit_price: null });
      },
      validateFinalOrderPolicy: input => {
        const result = tradePolicy.validateFinalOrderPolicy(input);
        state.finalChecks.push({ input, result });
        return result;
      },
      executeOrder: async (...args) => {
        const checked = await args[9].validateOrder({ price: args[3], amount: args[2] });
        if (!checked.allowed) return { failed: true, reason: checked.reason };
        const submitted = { action: args[0], instrument_name: args[1], direction: 'buy', amount: args[2],
          limit_price: args[3], order_id: 'replacement-put', pending_action_id: args[8],
          filled_amount: 0, filled_value: 0, average_price: 0, order_status: 'open', time_in_force: args[6] };
        state.submissions.push(submitted);
        db.insertRestingOrder(submitted);
        state.venue = [submitted];
        return { resting: true, price: args[3], order_id: submitted.order_id };
      },
    };
    for (const name of ['formatBuybackConfirmationContext', 'formatBuyPutConfirmationContext',
      'formatSellCallConfirmationContext', 'formatSellPutConfirmationContext', 'formatConfirmationLearningContext',
      'getCallMarginContext', 'formatRecentExecutionFrictionContext', 'getConfirmationScopePrompt',
      'getActionOrderTypeHardRule', 'getConfirmationJsonOnlyPrompt', 'getSharedActionPolicyPrompt',
      'getCallMarginDisciplinePrompt', 'getCallBuybackDisciplinePrompt', 'getPutExitDisciplinePrompt']) ctx[name] = () => '';
    const names = ['getFinalOrderPolicy', 'createFinalOrderValidator', 'getEntryOrderFilledValue',
      'accountEntryOrderObservation', 'readFreshEntryOrderSnapshot', 'validateEntryReplacementEconomics',
      'prepareEntryOrderReplacement', 'accountExitOrderObservation', 'readFreshExitOrderSnapshot',
      'confirmAndExecutePending'];
    const execute = vm.compileFunction(`${names.map(name => declaration(source, name)).join('\n')}
      return { run: confirmAndExecutePending, prepare: prepareEntryOrderReplacement };`, Object.keys(ctx));
    state.runtime = execute(...Object.values(ctx));
  };
  state.restart();
  state.db.saveBotState(state.botData);
  state.db.replaceActiveRules(null, [{ rule_type: 'entry', action: 'buy_put', instrument_name: instrumentName,
    preferred_order_type: 'post_only', criteria: { option_type: 'P', delta_range: [-0.12, -0.02],
      dte_range: [45, 78], min_score: 0.004, target_score: 0.005 } }]);
  const ruleId = state.db.getActiveRules()[0].id;
  const originalId = Number(state.db.insertPendingAction({ rule_id: ruleId, action: 'buy_put',
    instrument_name: instrumentName, amount: 5, price: 8 }).lastInsertRowid);
  state.db.updatePendingAction(originalId, { status: 'resting' });
  state.original = { order_id: 'old-put', action: 'buy_put', instrument_name: instrumentName, direction: 'buy',
    amount: 5, limit_price: 8, filled_amount: 1, filled_value: 8, average_price: 8,
    order_status: 'open', status: 'open', order_type: 'post_only', time_in_force: 'post_only',
    pending_action_id: originalId, creation_timestamp: now - 30 * 60000 };
  state.db.insertRestingOrder(state.original);
  state.db.insertOrder({ action: 'buy_put', instrument_name: instrumentName, pending_action_id: originalId,
    success: true, intended_amount: 5, filled_amount: 1, fill_price: 8, total_value: 8 });
  state.venue = [{ ...state.original }];
  state.receipt = { ...state.original };
  const trigger = { entry_replacement: true, replacement_order_id: 'old-put',
    replacement_order_snapshot: { ...state.original }, entry_approved_notional: 32,
    advisor_limit_price: 9.9, preferred_order_type: 'post_only', target_score: 0.005 };
  state.replacementId = Number(state.db.insertPendingAction({ rule_id: ruleId, action: 'buy_put',
    instrument_name: instrumentName, amount: 4, price: 9.9, trigger_details: trigger }).lastInsertRowid);
  state.pending = () => state.db.getPendingActionById(state.replacementId);
  state.fills = () => state.db.db.prepare('SELECT filled_amount,total_value FROM orders ORDER BY id').all();
  state.tick = async () => {
    await state.runtime.run([instrument], { [instrumentName]: state.ticker }, 2500);
    assert.deepEqual(state.errors, []);
  };
  state.terminal = () => {
    state.venue = [];
    state.receipt = { ...state.original, order_status: 'cancelled', status: 'cancelled',
      filled_amount: 3, filled_value: 28, average_price: 28 / 3 };
  };
  state.assertPending = () => {
    const pending = state.pending();
    assert.equal(pending.status, 'pending');
    assert.equal(pending.retries, 0, 'venue reconciliation never consumes reviewer attempts');
    assert.equal(pending.amount, 4, 'durable quantity remains the original authority');
    const savedTrigger = JSON.parse(pending.trigger_details);
    assert.deepEqual(savedTrigger.replacement_order_snapshot, trigger.replacement_order_snapshot);
    assert.equal(savedTrigger.entry_approved_notional, 32, 'durable dollars retain their original fill baseline');
  };
  return state;
}

test('cancel ACK survives unknown/open status and restart, then reconciles late put fills exactly once', async t => {
  const state = recoveryRuntime(t);
  await state.tick();
  state.assertPending();
  assert.equal(state.prompts.length, 2);
  assert.equal(state.events.filter(event => event === 'cancel:old-put').length, 1);
  assert.equal(state.db.getOpenRestingOrders().length, 1, 'ACK cannot release the original reservation');
  assert.equal(state.submissions.length, 0);

  state.restart();
  for (let tick = 0; tick < 4; tick++) { await state.tick(); state.assertPending(); }
  assert.equal(state.prompts.length, 2, 'unknown status does not repeatedly ask reviewers');
  state.venue = [{ ...state.original }];
  state.receipt = { ...state.original };
  state.ticker = { ...state.ticker, a: 8.1, b: 8 };
  await state.tick();
  state.assertPending();
  assert.equal(state.events.filter(event => event === 'cancel:old-put').length, 1, 'wait for the existing cancellation');
  assert.equal(state.prompts.length, 2, 'a temporarily equivalent quote cannot discard pending cancellation');

  state.restart();
  state.ticker = { ...state.ticker, a: 10, b: 7 };
  state.terminal();
  await state.tick();
  assert.equal(state.pending().status, 'resting');
  assert.equal(state.prompts.length, 4, 'recovered replacement receives a new pair of reviews');
  assert.equal(state.submissions.length, 1);
  assert.equal(state.submissions[0].limit_price, 9.9);
  assert.equal(state.submissions[0].amount, 1.21, '($32 authority - $20 late fills) / $9.90, floored');
  assert.equal(state.events.filter(event => event === 'cancel:old-put').length, 1);
  assert.deepEqual(state.fills(), [{ filled_amount: 1, total_value: 8 }, { filled_amount: 2, total_value: 20 }]);
  assert.equal(state.botData.putNetBought, 28);
  assert.equal(state.db.loadBotState().put_net_bought, 28);
  assert.equal(state.finalChecks.length, 2, 'fresh final policy runs before execution and immediately before submit');
  assert.ok(state.finalChecks.every(check => check.result.allowed && check.input.putBudgetRemaining === 72));
  assert.ok(state.research.length >= 4, 'replacement economics are re-evaluated after recovery');
  for (const prompt of state.prompts.slice(2)) {
    assert.match(prompt, /original order is already proven terminal/);
    assert.match(prompt, /"remaining_amount":2/);
    assert.match(prompt, /"amount":1\.21/);
  }
  state.restart();
  await state.tick();
  assert.equal(state.submissions.length, 1, 'completed authority cannot submit twice after restart');
  assert.equal(state.fills().length, 2);
  assert.equal(state.db.loadBotState().put_net_bought, 28);
});

test('status transport failures after an accepted cancellation defer without losing durable authority', async t => {
  const state = recoveryRuntime(t);
  state.onCancel = () => { state.failStatus = true; return { cancelled: true }; };
  await state.tick();
  state.assertPending();
  state.restart();
  await state.tick();
  state.assertPending();
  state.failStatus = false;
  state.terminal();
  await state.tick();
  assert.equal(state.submissions.length, 1);
  assert.equal(state.submissions[0].amount, 1.21);
  assert.equal(state.events.filter(event => event === 'cancel:old-put').length, 1);
});

test('restart between durable cancel request and acknowledgement retries cancellation without re-review', async t => {
  const state = recoveryRuntime(t);
  state.onCancel = () => {
    const saved = JSON.parse(state.pending().trigger_details);
    assert.equal(saved.entry_cancellation_requested, true, 'intent commits before sending a cancellation');
    assert.notEqual(saved.entry_cancellation_acknowledged, true);
    state.venue = [{ ...state.original }];
    state.receipt = { ...state.original };
    throw new Error('cancel response lost before acknowledgement was persisted');
  };
  await state.tick();
  state.assertPending();
  assert.equal(state.prompts.length, 2);
  state.restart();
  state.onCancel = null;
  await state.tick();
  state.assertPending();
  assert.equal(state.prompts.length, 2, 'resume cancellation rather than review a second replacement');
  assert.equal(state.events.filter(event => event === 'cancel:old-put').length, 2);
  assert.equal(JSON.parse(state.pending().trigger_details).entry_cancellation_acknowledged, true);
  state.restart();
  state.terminal();
  await state.tick();
  assert.equal(state.pending().status, 'resting');
  assert.equal(state.submissions.length, 1);
  assert.equal(state.submissions[0].amount, 1.21);
  assert.equal(state.prompts.length, 4);
  assert.equal(state.fills().length, 2);
});

test('inactive rule cancels deferred replacement before fresh review or submission', async t => {
  const state = recoveryRuntime(t);
  await state.tick();
  state.assertPending();
  state.restart();
  state.terminal();
  state.db.deactivateRuleById(state.pending().rule_id);
  await state.tick();
  assert.equal(state.pending().status, 'cancelled');
  assert.equal(state.pending().execution_result, 'inactive_rule');
  assert.equal(state.prompts.length, 2);
  assert.equal(state.submissions.length, 0);
});

test('two older entry cancellation waits cannot occupy both slots before a newer reduce-only exit', async t => {
  const state = recoveryRuntime(t);
  await state.tick();
  state.assertPending();
  const callName = 'ETH-20301008-3200-C';
  const insertRule = state.db.db.prepare(`INSERT INTO trading_rules
    (rule_type, action, instrument_name, criteria, is_active, preferred_order_type)
    VALUES (?, ?, ?, ?, 1, ?)`);
  const callRuleId = Number(insertRule.run('entry', 'sell_call', callName, JSON.stringify({
    option_type: 'C', delta_range: [0.04, 0.12], dte_range: [5, 12], min_score: 65, min_bid: 4,
  }), 'post_only').lastInsertRowid);
  const callOrder = { ...state.original, order_id: 'old-call', action: 'sell_call',
    instrument_name: callName, direction: 'sell', pending_action_id: null,
    filled_amount: 0, filled_value: 0, average_price: 0, limit_price: 12 };
  state.db.insertRestingOrder(callOrder);
  const callPendingId = Number(state.db.insertPendingAction({ rule_id: callRuleId, action: 'sell_call',
    instrument_name: callName, amount: 5, price: 11, trigger_details: {
      entry_replacement: true, replacement_order_id: callOrder.order_id,
      replacement_order_snapshot: callOrder, entry_cancellation_requested: true,
      entry_cancellation_acknowledged: true, preferred_order_type: 'post_only',
    } }).lastInsertRowid);
  const exitRuleId = Number(insertRule.run('exit', 'buyback_call', callName,
    JSON.stringify({ conditions: [{ field: 'unrealized_pnl_pct', op: 'gte', value: 80 }] }), 'ioc').lastInsertRowid);
  const exitPendingId = Number(state.db.insertPendingAction({ rule_id: exitRuleId, action: 'buyback_call',
    instrument_name: callName, amount: 1, price: 10 }).lastInsertRowid);
  const setTriggered = state.db.db.prepare('UPDATE pending_actions SET triggered_at = ? WHERE id = ?');
  for (const [index, id] of [state.replacementId, callPendingId, exitPendingId].entries()) {
    setTriggered.run(`2030-09-28T07:0${index}:00Z`, id);
  }
  assert.deepEqual(state.db.getPendingActions('pending').map(action => action.id),
    [state.replacementId, callPendingId, exitPendingId], 'both entry waits precede the exit in durable queue order');
  state.restart();
  await state.tick();
  const exit = state.db.getPendingActionById(exitPendingId);
  assert.equal(exit.status, 'cancelled', 'the later exit must be processed within this tick');
  assert.equal(exit.confirmation_reasoning, 'No live closeable position remains');
  state.assertPending();
  assert.equal(state.db.getPendingActionById(callPendingId).status, 'pending');
  assert.equal(state.db.getPendingActionById(callPendingId).retries, 0);
  assert.equal(state.prompts.length, 2, 'waiting entries and an already-closed position need no extra reviews');
  assert.equal(state.submissions.length, 0);
  assert.equal(state.db.getOpenRestingOrders().length, 2, 'unknown entry reservations remain intact');
});

test('newly ineligible put after cancellation releases pending intent for another candidate', async t => {
  const state = recoveryRuntime(t);
  await state.tick();
  state.assertPending();
  state.restart();
  state.terminal();
  state.ticker = { ...state.ticker, option_pricing: { ...state.ticker.option_pricing, d: -0.15 } };
  await state.tick();
  assert.equal(state.pending().status, 'cancelled');
  assert.match(state.pending().execution_result, /no longer eligible.*delta/i);
  assert.equal(state.db.hasPendingOrConfirmedActionForRule(state.pending().rule_id), false);
  assert.equal(state.prompts.length, 2, 'an invalid fresh candidate cannot reach reviewers');
  assert.equal(state.submissions.length, 0);
  assert.equal(state.db.loadBotState().put_net_bought, 28, 'terminal fills are retained despite abandoning replacement');
  assert.equal(state.fills().length, 2);
});

test('fresh final margin failure blocks recovered replacement after terminal fill accounting', async t => {
  const state = recoveryRuntime(t);
  await state.tick();
  state.assertPending();
  state.restart();
  state.terminal();
  // Zero available initial margin remains valid for review of the incumbent;
  // the actual final new-order policy must reject it after release.
  state.preparingFinal = true;
  state.finalMargin = { ...margin, initial_margin: 0 };
  await state.tick();
  assert.equal(state.pending().status, 'failed');
  assert.match(state.pending().execution_result, /unsafe_margin/);
  assert.equal(state.prompts.length, 4);
  assert.equal(state.submissions.length, 0);
  assert.equal(state.finalChecks.length, 1);
  assert.equal(state.finalChecks[0].result.code, 'unsafe_margin');
  assert.equal(state.db.loadBotState().put_net_bought, 28);
  assert.equal(state.db.getOpenRestingOrders().length, 0);
  assert.equal(state.fills().length, 2);
});
