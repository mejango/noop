'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const vm = require('node:vm');
const { declaration, SCRIPT_SOURCE, loadProduction } = require('./helpers/load-production');
const { normalizeBuyPutScore } = require('../bot/put-score');

const now = Date.parse('2030-09-15T08:00:00Z');
class Clock extends Date {
  constructor(...args) { super(...(args.length ? args : [now])); }
  static now() { return now; }
}
const inferior = 'ETH-20301114-1400-P';
const superior = 'ETH-20301114-1450-P';
const target = 0.00625;
const closeTo = (actual, expected) => assert.ok(Math.abs(actual - expected) < 1e-12, `${actual} != ${expected}`);

function fixture({ reverse = false, criteria = {}, failures = {}, rejections = {},
  budget = {}, resting = [], working = [], lastExecutedAt = null,
  quotes = [[inferior, 20, 1400, -0.05], [superior, 12.5, 1450, -0.05]] } = {}) {
  const inserted = [], decisions = [], frames = [], logs = [];
  const botData = { putBudgetForCycle: 100, putUnspentBuyLimit: 0, putNetBought: 0, ...budget };
  const rule = { id: 71, action: 'buy_put', rule_type: 'entry', preferred_order_type: 'post_only', budget_limit: 100,
    criteria: { option_type: 'P', delta_range: [-0.12, -0.02], dte_range: [45, 78],
      min_score: 0.001, target_score: target, ...criteria } };
  const db = {
    getActiveRulesByType: type => type === 'entry' ? [rule] : [],
    getRecentPendingActions: () => working,
    getPendingActions: status => working.filter(action => action.status === status),
    getLastExecutedAction: () => lastExecutedAt,
    getLastFailedAction: (action, name) => failures[name] || null,
    getLastRejectedAction: (action, name) => rejections[name] || null,
    insertPendingAction: row => { inserted.push(row); return { lastInsertRowid: 123 }; },
  };
  const pure = loadProduction([
    'getBuyPutEntryPricing', 'classifyBuyPutEdge', 'computeDteFromInstrumentName', 'getTickerImpliedVol',
    'normalizeBuyPutValueSignal', 'hasExplicitBuyPutValueSignal', 'isActionableBuyPutSignal',
    'summarizeReservedEntryCapacity', 'floorOrderAmountToVenuePrecision', 'isVenueOrderAmountTradable',
    'normalizePreferredOrderType', 'roundForAdvisory', 'hasPendingOrConfirmedActionForRule',
    'getRecentRejectedAction', 'getRecentFailedEntry', 'isIocZeroFillFailure',
  ], { bindings: { Date: Clock, botData, db } });
  const bindings = {
    ...pure, Date: Clock, botData, db,
    console: { log: message => logs.push(message), warn: message => logs.push(message), error: message => logs.push(message) },
    logRuleDecisionSafe: row => decisions.push(row),
    getOpenRestingEntryOrders: () => resting, fetchSubaccount: async () => null,
    reassessRestingEntryOrders: async () => ({ queuedCount: 0, blockedActions: new Set(), plans: new Map() }),
    buildLiveSellCallMarketContext: () => ({}),
    getBestCurrentBuyPutEdgeCandidate: () => null, getBestCurrentSellCallCandidate: () => null,
    buildRollingOptionValueContext: () => ({ action_pressure: { signal: 'standing_patient_bid' } }),
    getBlockingRestingOrderForEntryCandidate: () => null,
    buildCandidateObservationRows: frame => { frames.push(frame); return []; },
    recordCandidateObservationsSafe: () => {},
  };
  const evaluate = vm.compileFunction(`${declaration(SCRIPT_SOURCE, 'evaluateTradingRules')}; return evaluateTradingRules;`,
    Object.keys(bindings))(...Object.values(bindings));
  const candidates = [...quotes];
  if (reverse) candidates.reverse();
  const instruments = candidates.map(([name, , strike]) => ({ instrument_name: name, price_step: 0.01,
    options: { amount_step: 0.01 }, option_details: { option_type: 'P', strike, expiry: Date.parse('2030-11-14T08:00:00Z') / 1000 } }));
  const tickerMap = Object.fromEntries(candidates.map(([name, ask, , delta]) => [name, {
    a: ask, b: ask * 0.98, M: ask * 0.99, A: 10, B: 10, I: 2500,
    option_pricing: { d: delta, i: 0.6 }, stats: { oi: 100 },
  }]));
  return { inserted, decisions, frames, logs, pure,
    run: () => evaluate([], instruments, tickerMap, 2500) };
}

async function selected(f) {
  assert.equal(await f.run(), 1, f.logs.join('\n'));
  assert.equal(f.inserted.length, 1, f.logs.join('\n'));
  return f.inserted[0];
}

test('normal PUT entry ranks live ask EDGE when patient bids achieve the same target, regardless of input order', async () => {
  for (const reverse of [false, true]) {
    const f = fixture({ reverse });
    const first = f.pure.getBuyPutEntryPricing({ askPrice: 20, absDelta: 0.05, dte: 60, minScore: 0.001, targetScore: target });
    const second = f.pure.getBuyPutEntryPricing({ askPrice: 12.5, absDelta: 0.05, dte: 60, minScore: 0.001, targetScore: target });
    closeTo(first.plannedScore, second.plannedScore);
    closeTo(first.plannedScore, target);
    assert.ok(second.liveScore > first.liveScore);
    const pending = await selected(f);
    assert.equal(pending.instrument_name, superior);
    assert.equal(pending.price, 8);
    assert.ok(pending.amount * pending.price <= 100);
    assert.equal(pending.trigger_details.candidates_evaluated, 2);
    closeTo(pending.trigger_details.live_score, normalizeBuyPutScore(0.05 / 12.5, 60));
    closeTo(pending.trigger_details.planned_score, target);
  }
});

test('pending PUT selection telemetry preserves fractional normalized EDGE instead of rounding it to zero', async () => {
  const f = fixture({ criteria: { target_score: null } });
  const pending = await selected(f);
  const expected = normalizeBuyPutScore(0.05 / 12.5, 60);
  assert.equal(pending.instrument_name, superior);
  assert.ok(expected > 0 && expected < 0.005);
  closeTo(pending.trigger_details.selection_score, expected);
  closeTo(pending.trigger_details.live_score, expected);
  closeTo(f.decisions.find(row => row.reason_code === 'candidate_selected').selection_score, expected);
});

test('equal live PUT EDGE uses instrument-name ties despite different rounded patient scores in either input order', async () => {
  const tiedTarget = 0.0063;
  for (const reverse of [false, true]) {
    const f = fixture({ reverse, criteria: { target_score: tiedTarget },
      quotes: [[inferior, 12.5, 1400, -0.05], [superior, 10, 1450, -0.04]] });
    const first = f.pure.getBuyPutEntryPricing({ askPrice: 12.5, absDelta: 0.05, dte: 60,
      minScore: 0.001, targetScore: tiedTarget });
    const second = f.pure.getBuyPutEntryPricing({ askPrice: 10, absDelta: 0.04, dte: 60,
      minScore: 0.001, targetScore: tiedTarget });
    assert.equal(first.liveScore, second.liveScore);
    closeTo(first.liveScore, 0.004);
    assert.ok(first.plannedScore >= tiedTarget);
    assert.ok(second.plannedScore > first.plannedScore);
    const pending = await selected(f);
    assert.equal(pending.instrument_name, inferior);
    assert.equal(pending.price, 7.93);
    assert.equal(pending.trigger_details.candidates_evaluated, 2);
    closeTo(pending.trigger_details.selection_score, 0.004);
  }
});

test('legacy PUT composite thresholds do not suppress an otherwise valid normalized EDGE entry', async () => {
  const f = fixture({ criteria: { min_edge_score: 1000000 } });
  const pending = await selected(f);
  assert.equal(pending.instrument_name, superior);
  closeTo(pending.trigger_details.planned_score, target);
  assert.equal(pending.trigger_details.candidates_evaluated, 2);
  assert.equal(f.decisions.some(row => row.reason_code === 'no_candidates'), false);
});

const recentFailure = (reason, ageMs = 60_000) => ({
  triggered_at: new Clock(now - ageMs).toISOString(), execution_result: reason,
});

test('local fresh PUT EDGE/cap failures can be repriced on the next tick without instrument cooldown', async () => {
  for (const reason of [
    'Final order rejected (put_price_failed): Final put bid violates approved cap or fresh PUT EDGE',
    'Final put bid violates approved cap or fresh PUT EDGE',
  ]) {
    const f = fixture({ failures: { [superior]: recentFailure(reason) } });
    assert.equal(f.pure.getRecentFailedEntry('buy_put', superior), null);
    const pending = await selected(f);
    assert.equal(pending.instrument_name, superior);
    assert.equal(pending.price, 8);
    assert.ok(pending.trigger_details.planned_score >= target);
    assert.ok(pending.amount * pending.price <= 100);
  }
});

test('persistent failure and reviewer rejection cooldowns are filtered before PUT EDGE ranking', async () => {
  for (const block of ['failure', 'rejection']) {
    for (const reverse of [false, true]) {
      const options = block === 'failure'
        ? { failures: { [superior]: recentFailure('Order placement failed: venue unavailable') } }
        : { rejections: { [superior]: { ...recentFailure(null), confirmation_reasoning: 'Risk review rejected' } } };
      const f = fixture({ reverse, ...options });
      const pending = await selected(f);
      assert.equal(pending.instrument_name, inferior);
      assert.equal(pending.trigger_details.candidates_evaluated, 1);
      const selectedDecision = f.decisions.find(row => row.reason_code === 'candidate_selected');
      const blocked = selectedDecision.context_json.candidate_cooldowns;
      assert.equal(blocked.length, 1);
      assert.equal(blocked[0].instrument_name, superior);
      assert.equal(blocked[0].reason_code, block === 'failure' ? 'recent_failed_entry' : 'recent_rejection');
      assert.ok(blocked[0].remaining_ms > 0);
    }
  }
});

test('all candidates cooling down leaves no pending order and records each blocking reason', async () => {
  const f = fixture({ failures: { [superior]: recentFailure('Order status unknown; reservation retained') },
    rejections: { [inferior]: { ...recentFailure(null), confirmation_reasoning: 'Risk review rejected' } } });
  assert.equal(await f.run(), 0, f.logs.join('\n'));
  assert.equal(f.inserted.length, 0);
  const decision = f.decisions.find(row => row.reason_code === 'all_candidates_cooling_down');
  assert.ok(decision, f.logs.join('\n'));
  assert.equal(decision.context_json.candidate_cooldowns.length, 2);
  assert.equal(decision.context_json.filter_stats.recent_failed_entry, 1);
  assert.equal(decision.context_json.filter_stats.recent_rejection, 1);
});

test('zero-fill and post-only price movement retain retry eligibility; persistent failures expire after 30 minutes', async () => {
  for (const failure of [
    recentFailure('Zero fill (IOC) — no liquidity at $8'),
    recentFailure('post_only rejected: would cross book at $8'),
    recentFailure('Order placement failed: venue unavailable', 30 * 60_000),
  ]) {
    const f = fixture({ failures: { [superior]: failure } });
    assert.equal((await selected(f)).instrument_name, superior);
  }
});

test('fresh-price retry exception is restricted to buy_put local price validation failures', () => {
  for (const reason of [
    'Final order rejected (entry_budget_exceeded): Put budget unavailable',
    'Final order rejected (stale_quote): Quote unavailable',
    'Order status unknown; Final put bid violates approved cap or fresh PUT EDGE',
    'Order placement failed',
  ]) {
    const f = fixture({ failures: { [superior]: recentFailure(reason) } });
    assert.equal(f.pure.getRecentFailedEntry('buy_put', superior)?.reason, reason);
  }
  const priceFailure = 'Final put bid violates approved cap or fresh PUT EDGE';
  const f = fixture({ failures: { [superior]: recentFailure(priceFailure) } });
  assert.equal(f.pure.getRecentFailedEntry('sell_call', superior)?.reason, priceFailure);
});

const recentFill = new Clock(now - 60_000).toISOString();

test('a recent PUT fill permits another eligible entry sized from the remaining cycle budget', async () => {
  const f = fixture({ lastExecutedAt: recentFill, budget: { putNetBought: 70 } });
  const pending = await selected(f);
  assert.equal(pending.instrument_name, superior);
  assert.equal(pending.price, 8);
  assert.equal(pending.amount, 3.75);
  assert.equal(pending.amount * pending.price, 30);
  assert.ok(pending.trigger_details.planned_score >= target);
  assert.equal(f.decisions.some(row => row.reason_code === 'action_cooldown'), false);
});

test('recent PUT fills still leave cycle spending and outstanding reservations unavailable for new orders', async () => {
  for (const options of [
    { budget: { putNetBought: 100 } },
    { budget: { putNetBought: 60 }, resting: [{ order_id: 'other-rule-order', rule_id: 72,
      instrument_name: superior, action: 'buy_put', amount: 7, filled_amount: 2, limit_price: 8 }] },
  ]) {
    const f = fixture({ lastExecutedAt: recentFill, ...options });
    assert.equal(await f.run(), 0, f.logs.join('\n'));
    assert.equal(f.inserted.length, 0);
    const decision = f.decisions.find(row => row.reason_code === 'put_budget_unavailable');
    assert.ok(decision, f.logs.join('\n'));
    assert.equal(decision.context_json.reserved, options.resting ? 40 : 0);
  }
});

test('removing PUT post-fill pacing preserves pending and confirmed duplicate protection across rules', async () => {
  for (const status of ['pending', 'confirmed']) {
    for (const ruleId of [71, 72]) {
      const f = fixture({ lastExecutedAt: recentFill,
        working: [{ id: 55, rule_id: ruleId, action: 'buy_put', instrument_name: superior, status }] });
      assert.equal(await f.run(), 0, f.logs.join('\n'));
      assert.equal(f.inserted.length, 0);
      const reason = ruleId === 71 ? 'pending_duplicate' : 'working_action_duplicate';
      assert.ok(f.decisions.some(row => row.reason_code === reason), f.logs.join('\n'));
    }
  }
});

test('a partially filled resting PUT still prevents another entry after a recent fill', async () => {
  const f = fixture({ lastExecutedAt: recentFill, budget: { putNetBought: 70 },
    resting: [{ order_id: 'incumbent', rule_id: 71, instrument_name: superior,
      action: 'buy_put', amount: 5, filled_amount: 2, limit_price: 8 }] });
  assert.equal(await f.run(), 0, f.logs.join('\n'));
  assert.equal(f.inserted.length, 0);
  assert.ok(f.decisions.some(row => row.reason_code === 'existing_resting_reassessed'), f.logs.join('\n'));
});

test('a recent PUT fill does not bypass delta, expiry, or achievable bid-price requirements', async () => {
  for (const [criteria, filter] of [
    [{ delta_range: [-0.12, -0.08] }, 'deltaOut'],
    [{ dte_range: [45, 55] }, 'dteOut'],
    [{ min_score: 10, target_score: 10 }, 'scoreOut'],
  ]) {
    const f = fixture({ lastExecutedAt: recentFill, criteria });
    assert.equal(await f.run(), 0, f.logs.join('\n'));
    assert.equal(f.inserted.length, 0);
    const decision = f.decisions.find(row => row.reason_code === 'no_candidates');
    assert.ok(decision, f.logs.join('\n'));
    assert.equal(decision.context_json.filter_stats[filter], 2);
  }
});
