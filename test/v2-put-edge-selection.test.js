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

function fixture({ reverse = false, criteria = {}, quotes = [[inferior, 20, 1400, -0.05], [superior, 12.5, 1450, -0.05]] } = {}) {
  const inserted = [], decisions = [], frames = [], logs = [];
  const botData = { putBudgetForCycle: 100, putUnspentBuyLimit: 0, putNetBought: 0 };
  const rule = { id: 71, action: 'buy_put', rule_type: 'entry', preferred_order_type: 'post_only', budget_limit: 100,
    criteria: { option_type: 'P', delta_range: [-0.12, -0.02], dte_range: [45, 78],
      min_score: 0.001, target_score: target, ...criteria } };
  const db = {
    getActiveRulesByType: type => type === 'entry' ? [rule] : [],
    getRecentPendingActions: () => [], getPendingActions: () => [], getLastExecutedAction: () => null,
    insertPendingAction: row => { inserted.push(row); return { lastInsertRowid: 123 }; },
  };
  const pure = loadProduction([
    'getBuyPutEntryPricing', 'classifyBuyPutEdge', 'computeDteFromInstrumentName', 'getTickerImpliedVol',
    'normalizeBuyPutValueSignal', 'hasExplicitBuyPutValueSignal', 'isActionableBuyPutSignal',
    'summarizeReservedEntryCapacity', 'floorOrderAmountToVenuePrecision', 'isVenueOrderAmountTradable',
    'normalizePreferredOrderType', 'roundForAdvisory',
  ], { bindings: { Date: Clock, botData, db } });
  const bindings = {
    ...pure, Date: Clock, botData, db,
    console: { log: message => logs.push(message), warn: message => logs.push(message), error: message => logs.push(message) },
    logRuleDecisionSafe: row => decisions.push(row),
    getOpenRestingEntryOrders: () => [], fetchSubaccount: async () => null,
    reassessRestingEntryOrders: async () => ({ queuedCount: 0, blockedActions: new Set(), plans: new Map() }),
    buildLiveSellCallMarketContext: () => ({}),
    getBestCurrentBuyPutEdgeCandidate: () => null, getBestCurrentSellCallCandidate: () => null,
    buildRollingOptionValueContext: () => ({ action_pressure: { signal: 'standing_patient_bid' } }),
    hasPendingOrConfirmedActionForRule: () => false,
    getBlockingRestingOrderForEntryCandidate: () => null,
    getRecentRejectedAction: () => null, getRecentFailedEntry: () => null,
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
