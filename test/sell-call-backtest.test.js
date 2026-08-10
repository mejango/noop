/**
 * Focused tests for the isolated sell-call research backtester.
 * Run: node test/sell-call-backtest.test.js
 */
'use strict';

const assert = require('assert');
const Database = require('better-sqlite3');
const {
  SELL_CALL_EDGE_REFERENCE_DTE,
  SELL_CALL_EDGE_DTE_EXPONENT,
  normalizeSellCallScore,
} = require('../bot/call-score');
const {
  HOUR_MS,
  CURRENT_EDGE_CONFIG,
  RAW_EQUIVALENT_CONFIG,
  analyzeRolloverDiscontinuity,
  attachTrailingDteBenchmarks,
  buildEconomicExamples,
  buildEconomicPredictionTape,
  buildLabeledExamples,
  constantMaturityPoint,
  currentEdgeScore,
  edgeVariantScore,
  enrichFrames,
  generateVariantConfigs,
  loadHistoricalFrames,
  makeDteNormalizedRawPolicy,
  makeEconomicValuePolicy,
  makeNoCallPolicy,
  makeRawScorePolicy,
  normalizeDteScore,
  normalizeLinearDteScore,
  runBacktest,
  scoreSimpleCall,
  splitChronologicalFrames,
  trainEconomicModels,
  trainOutcomeModels,
  predictEconomicOutcome,
  predictOutcome,
  WalkForwardLearnedPolicy,
} = require('../research/sell-call-backtest');

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    passed++;
    console.log(`  PASS ${name}`);
  } catch (error) {
    failed++;
    console.log(`  FAIL ${name}: ${error.stack || error.message}`);
  }
}

function describe(name, fn) {
  console.log(`\n${name}`);
  fn();
}

function option({
  name = 'ETH-20260109-2200-C',
  expiry,
  strike = 2200,
  type = 'C',
  delta = 0.1,
  bid = 10,
  ask = 11,
  mark = 10.5,
  bidAmount = 10,
  askAmount = 10,
  iv = 0.6,
  oi = 100,
  spot = 2000,
} = {}) {
  return {
    instrument_name: name,
    option_type: type,
    expiry,
    strike,
    delta,
    bid_price: bid,
    ask_price: ask,
    mark_price: mark,
    bid_amount: bidAmount,
    ask_amount: askAmount,
    implied_vol: iv,
    open_interest: oi,
    index_price: spot,
  };
}

function framesFrom(definitions) {
  return enrichFrames(definitions.map((definition) => ({
    timestamp: new Date(definition.timestampMs).toISOString(),
    timestamp_ms: definition.timestampMs,
    spot_price: definition.spot,
    options: definition.options,
  })));
}

describe('current edge compatibility', () => {
  test('reproduces the hard-coded multiplicative score', () => {
    const candidate = {
      raw_score: 75,
      dte: 8,
      bid_price: 7.5,
      spread_pct: 0.09,
      features: {
        market_avg_spread: 0.12,
        score_trend_24h_pct: 5,
        market_best_put_score: 0.002,
        market_skew: 0.04,
        market_oi_delta_24h_pct: 6,
      },
    };
    const expectedMultiplier = 1.05 * 1.02 * 1.08 * 1.18 * 1.12 * 1.1 * 1.05 * 1.04 * 1.08;
    const edge = currentEdgeScore(candidate);
    assert.ok(Math.abs(edge.multiplier - expectedMultiplier) < 1e-12);
    assert.ok(Math.abs(edge.score - 75 * expectedMultiplier) < 1e-9);
  });
});

describe('isolated edge tuning', () => {
  const candidate = {
    raw_score: 100,
    dte: 8,
    bid_price: 8,
    spread_pct: 0.09,
    features: {
      market_avg_spread: 0.12,
      score_trend_24h_pct: 5,
      market_best_put_score: 0.002,
      market_skew: 0.05,
      market_oi_delta_24h_pct: 8,
    },
  };

  test('zero factor weights exactly reproduce raw score', () => {
    const tuned = edgeVariantScore(candidate, RAW_EQUIVALENT_CONFIG);
    assert.strictEqual(tuned.score, candidate.raw_score);
    assert.strictEqual(tuned.multiplier, 1);
  });

  test('unit factor weights reproduce the current composite edge', () => {
    const current = currentEdgeScore(candidate);
    const tuned = edgeVariantScore(candidate, CURRENT_EDGE_CONFIG);
    assert.ok(Math.abs(tuned.score - current.score) < 1e-10);
    assert.ok(Math.abs(tuned.multiplier - current.multiplier) < 1e-10);
  });

  test('chronological split leaves a final non-overlapping holdout', () => {
    const frames = Array.from({ length: 20 }, (_, index) => ({
      timestamp: new Date(Date.UTC(2026, 0, 1, index)).toISOString(),
      timestamp_ms: Date.UTC(2026, 0, 1, index),
    }));
    const split = splitChronologicalFrames(frames, { trainFraction: 0.6, validationFraction: 0.2 });
    assert.deepStrictEqual([split.train.length, split.validation.length, split.test.length], [12, 4, 4]);
    assert.ok(split.train.at(-1).timestamp_ms < split.validation[0].timestamp_ms);
    assert.ok(split.validation.at(-1).timestamp_ms < split.test[0].timestamp_ms);
  });

  test('variant generation is deterministic and includes both declared baselines', () => {
    const first = generateVariantConfigs(100, 42);
    const second = generateVariantConfigs(100, 42);
    assert.deepStrictEqual(first, second);
    assert.deepStrictEqual(first[0], RAW_EQUIVALENT_CONFIG);
    assert.deepStrictEqual(first[1], CURRENT_EDGE_CONFIG);
    assert.strictEqual(new Set(first.map(JSON.stringify)).size, first.length);
  });
});

describe('DTE normalization', () => {
  test('production CALL EDGE matches the researched 0.12 formula', () => {
    for (const dte of [5, 6.5, 8.5, 10, 12]) {
      const researched = normalizeDteScore(73.25, dte, {
        referenceDte: SELL_CALL_EDGE_REFERENCE_DTE,
        exponent: SELL_CALL_EDGE_DTE_EXPONENT,
      });
      assert.ok(Math.abs(normalizeSellCallScore(73.25, dte) - researched) < 1e-12);
    }
  });

  test('zero exponent is exactly raw score', () => {
    assert.strictEqual(normalizeDteScore(100, 12, { referenceDte: 8.5, exponent: 0 }), 100);
    assert.strictEqual(normalizeDteScore(100, 5, { referenceDte: 8.5, exponent: 0 }), 100);
  });

  test('partial time normalization reduces long-DTE score and raises short-DTE score', () => {
    assert.ok(normalizeDteScore(100, 12, { referenceDte: 8.5, exponent: 0.25 }) < 100);
    assert.ok(normalizeDteScore(100, 5, { referenceDte: 8.5, exponent: 0.25 }) > 100);
  });

  test('linear normalization applies one continuous slope around the reference DTE', () => {
    const options = { referenceDte: 8.5, slope: 0.02 };
    assert.strictEqual(normalizeLinearDteScore(100, 8.5, options), 100);
    assert.strictEqual(normalizeLinearDteScore(100, 5, options), 107);
    assert.strictEqual(normalizeLinearDteScore(100, 12, options), 93);
    assert.strictEqual(
      normalizeLinearDteScore(100, 6, options) - normalizeLinearDteScore(100, 7, options),
      normalizeLinearDteScore(100, 10, options) - normalizeLinearDteScore(100, 11, options),
    );
  });

  test('reference DTE only rescales scores and can be offset by the floor', () => {
    const exponent = 0.12;
    const oldReference = 8.5;
    const newReference = 10;
    const oldFloor = 65;
    const newFloor = oldFloor * Math.pow(newReference / oldReference, exponent);
    for (const dte of [4, 7, 10, 14]) {
      const oldMargin = normalizeDteScore(70, dte, { referenceDte: oldReference, exponent }) / oldFloor;
      const newMargin = normalizeDteScore(70, dte, { referenceDte: newReference, exponent }) / newFloor;
      assert.ok(Math.abs(oldMargin - newMargin) < 1e-12);
    }
  });

  test('normalized policy applies an independently configurable DTE window', () => {
    const policy = makeDteNormalizedRawPolicy({ minDte: 6, maxDte: 10, minScore: 1, minBid: 1 });
    const base = { bid_price: 10, raw_score: 100, expiry: 1, instrument_name: 'CALL' };
    assert.strictEqual(policy.select({ candidates: [{ ...base, dte: 5.9 }] }), null);
    assert.ok(policy.select({ candidates: [{ ...base, dte: 6 }] }));
    assert.ok(policy.select({ candidates: [{ ...base, dte: 10 }] }));
    assert.strictEqual(policy.select({ candidates: [{ ...base, dte: 10.1 }] }), null);
  });

  test('normalization reduces a synthetic expiry-roll discontinuity', () => {
    const start = Date.UTC(2026, 0, 3, 0);
    const oldExpiry = Math.floor((start + 5.1 * 24 * HOUR_MS) / 1000);
    const newExpiry = Math.floor((start + 12 * 24 * HOUR_MS) / 1000);
    const frames = Array.from({ length: 30 }, (_, hour) => {
      const timestampMs = start + hour * HOUR_MS;
      const rolled = hour >= 24;
      const dte = rolled ? 12 - hour / 24 : 5.1 - hour / 24;
      const expiry = rolled ? newExpiry : oldExpiry;
      const rawScore = 100 * Math.sqrt(dte / 5.1);
      return {
        timestamp: new Date(timestampMs).toISOString(),
        timestamp_ms: timestampMs,
        candidates: [{
          instrument_name: rolled ? 'NEW-C' : 'OLD-C',
          expiry,
          dte,
          raw_score: rawScore,
          bid_price: 10,
        }],
      };
    });
    const raw = analyzeRolloverDiscontinuity(frames, { referenceDte: 8.5, exponent: 0 });
    const normalized = analyzeRolloverDiscontinuity(frames, { referenceDte: 8.5, exponent: 0.5 });
    assert.strictEqual(raw.rollover_events, 1);
    assert.strictEqual(normalized.rollover_events, 1);
    assert.ok(normalized.mean_positive_peak_jump_pct < raw.mean_positive_peak_jump_pct);
  });
});

describe('simple call-score hypotheses', () => {
  const candidate = {
    raw_score: 100,
    dte: 8.5,
    bid_price: 10,
    ask_price: 12,
    delta: 0.1,
    implied_vol: 0.6,
  };
  const frame = { spot_price: 2000 };

  test('production hypothesis preserves the production score at reference DTE', () => {
    assert.strictEqual(scoreSimpleCall(candidate, frame, { family: 'production' }), 100);
  });

  test('spread haircut measures executable premium left after crossing cost', () => {
    assert.strictEqual(scoreSimpleCall(candidate, frame, { family: 'spread_haircut', spreadHaircut: 0.5 }), 90);
    assert.strictEqual(scoreSimpleCall(candidate, frame, { family: 'spread_haircut', spreadHaircut: 1 }), 80);
  });

  test('additive DTE detrend is a continuous score-point adjustment', () => {
    assert.strictEqual(scoreSimpleCall({ ...candidate, dte: 12 }, frame, { family: 'additive_dte', additiveSlope: 1 }), 96.5);
    assert.strictEqual(scoreSimpleCall({ ...candidate, dte: 5 }, frame, { family: 'additive_dte', additiveSlope: 1 }), 103.5);
  });

  test('expected-move efficiency falls when implied volatility rises', () => {
    const calm = scoreSimpleCall(candidate, frame, { family: 'expected_move' });
    const volatile = scoreSimpleCall({ ...candidate, implied_vol: 1.2 }, frame, { family: 'expected_move' });
    assert.ok(calm > volatile);
    assert.ok(Math.abs(calm / volatile - 2) < 1e-12);
  });

  test('balanced expected-move value is the geometric mean of absolute and risk-scaled value', () => {
    const production = scoreSimpleCall(candidate, frame, { family: 'production' });
    const netMove = scoreSimpleCall(candidate, frame, { family: 'expected_move_net' });
    const balanced = scoreSimpleCall(candidate, frame, { family: 'balanced_expected_move' });
    assert.ok(Math.abs(balanced - Math.sqrt(production * netMove)) < 1e-12);
  });

  test('trailing DTE-relative value uses prior frames and excludes the current quote', () => {
    const start = Date.UTC(2026, 0, 1);
    const frames = [100, 200].map((rawScore, index) => ({
      timestamp_ms: start + index * HOUR_MS,
      spot_price: 2000,
      candidates: [{
        ...candidate,
        instrument_name: `CALL-${index}`,
        expiry: Math.floor((start + 8.5 * 24 * HOUR_MS) / 1000),
        raw_score: rawScore,
        bid_price: rawScore * 0.1,
      }],
    }));
    attachTrailingDteBenchmarks(frames, { minSamples: 1, lookbackDays: 30 });
    assert.strictEqual(scoreSimpleCall(frames[0].candidates[0], frames[0], { family: 'trailing_dte_relative' }), 0);
    assert.strictEqual(scoreSimpleCall(frames[1].candidates[0], frames[1], { family: 'trailing_dte_relative' }), 2);
  });

  test('constant-maturity value interpolates between surrounding expiries', () => {
    const point = constantMaturityPoint({
      timestamp: '2026-01-01T00:00:00.000Z',
      timestamp_ms: Date.UTC(2026, 0, 1),
      spot_price: 2000,
      candidates: [
        { ...candidate, instrument_name: 'LOWER', expiry: 1, dte: 5, raw_score: 50, bid_price: 5 },
        { ...candidate, instrument_name: 'UPPER', expiry: 2, dte: 12, raw_score: 120, bid_price: 12 },
      ],
    }, { family: 'production', exponent: 0, targetDte: 8.5, minDte: 1, maxDte: 21 });
    assert.ok(point);
    assert.strictEqual(point.score, 85);
    assert.strictEqual(point.interpolation_weight, 0.5);
  });
});

describe('counterfactual labels', () => {
  test('uses only a future ask at or after the requested horizon', () => {
    const start = Date.UTC(2026, 0, 1, 0);
    const expiry = Math.floor((start + 8 * 24 * HOUR_MS) / 1000);
    const frames = framesFrom([
      { timestampMs: start, spot: 2000, options: [option({ expiry, bid: 10, ask: 11 })] },
      { timestampMs: start + HOUR_MS, spot: 2000, options: [option({ expiry, bid: 8, ask: 9 })] },
      { timestampMs: start + 2 * HOUR_MS, spot: 2000, options: [option({ expiry, bid: 6, ask: 7 })] },
    ]);
    const examples = buildLabeledExamples(frames, { horizonHours: 2, quoteWindowHours: 1, topPerFrame: 1 });
    const first = examples.find((example) => example.observed_at_ms === start);
    assert.ok(first);
    assert.strictEqual(first.future_exit_price, 7);
    assert.strictEqual(first.label_available_at_ms, start + 2 * HOUR_MS);
    assert.strictEqual(first.capture_return, 0.3);
  });

  test('uses intrinsic value when the horizon crosses expiry', () => {
    const start = Date.UTC(2026, 0, 1, 0);
    const expiryMs = start + 6 * 24 * HOUR_MS;
    const expiry = Math.floor(expiryMs / 1000);
    const frames = framesFrom([
      { timestampMs: start, spot: 2000, options: [option({ expiry, strike: 2100, bid: 10, ask: 11 })] },
      { timestampMs: expiryMs, spot: 2200, options: [] },
    ]);
    const examples = buildLabeledExamples(frames, { horizonHours: 168, quoteWindowHours: 1, topPerFrame: 1 });
    assert.strictEqual(examples.length, 1);
    assert.strictEqual(examples[0].outcome_source, 'expiry_settlement');
    assert.strictEqual(examples[0].future_exit_price, 100);
    assert.strictEqual(examples[0].capture_return, -9);
  });

  test('does not turn a missing future ask into a zero-cost buyback', () => {
    const start = Date.UTC(2026, 0, 1, 0);
    const expiry = Math.floor((start + 8 * 24 * HOUR_MS) / 1000);
    const frames = framesFrom([
      { timestampMs: start, spot: 2000, options: [option({ expiry, bid: 10, ask: 11 })] },
      { timestampMs: start + HOUR_MS, spot: 2000, options: [option({ expiry, bid: 8, ask: null, mark: 8 })] },
    ]);
    const examples = buildLabeledExamples(frames, { horizonHours: 1, quoteWindowHours: 1, topPerFrame: 1 });
    assert.strictEqual(examples.length, 0);
  });
});

describe('economic path outcomes', () => {
  test('matches the replay exit policy and records capital-time plus adverse excursion', () => {
    const start = Date.UTC(2026, 0, 1, 0);
    const expiry = Math.floor((start + 8 * 24 * HOUR_MS) / 1000);
    const frames = framesFrom([
      { timestampMs: start, spot: 2000, options: [option({ expiry, bid: 10, ask: 11 })] },
      { timestampMs: start + HOUR_MS, spot: 2010, options: [option({ expiry, bid: 7, ask: 8 })] },
      { timestampMs: start + 2 * HOUR_MS, spot: 2000, options: [option({ expiry, bid: 1, ask: 1.9 })] },
    ]);
    const examples = buildEconomicExamples(frames, { minBid: 4, profitCapturePct: 0.8, marginRate: 0.15 });
    const first = examples.find((example) => example.observed_at_ms === start);
    assert.ok(first);
    assert.strictEqual(first.exit_reason, 'profit_capture');
    assert.strictEqual(first.close_price, 1.9);
    assert.strictEqual(first.pnl, 8.1);
    assert.strictEqual(first.holding_hours, 2);
    assert.strictEqual(first.margin_per_contract, 300);
    assert.strictEqual(first.max_adverse_excursion, 1);
    assert.ok(Math.abs(first.profit_per_margin_day - 0.324) < 1e-12);
  });

  test('labels every eligible candidate rather than inheriting a raw-score top-N filter', () => {
    const start = Date.UTC(2026, 0, 1, 0);
    const expiry = Math.floor((start + 8 * 24 * HOUR_MS) / 1000);
    const atEntry = [
      option({ name: 'HIGH', expiry, bid: 10, ask: 11, delta: 0.1 }),
      option({ name: 'LOW', expiry, bid: 5, ask: 6, delta: 0.1 }),
    ];
    const atExit = [
      option({ name: 'HIGH', expiry, bid: 1, ask: 1.9, delta: 0.1 }),
      option({ name: 'LOW', expiry, bid: 0.5, ask: 0.9, delta: 0.1 }),
    ];
    const frames = framesFrom([
      { timestampMs: start, spot: 2000, options: atEntry },
      { timestampMs: start + HOUR_MS, spot: 2000, options: atExit },
    ]);
    const examples = buildEconomicExamples(frames, { minBid: 4, profitCapturePct: 0.8 });
    assert.deepStrictEqual(examples.map((example) => example.instrument_name).sort(), ['HIGH', 'LOW']);
    assert.ok(examples.every((example) => example.weight === 0.5));
  });

  test('learns profit-rate direction separately from adverse-path risk', () => {
    const start = Date.UTC(2026, 0, 1);
    const examples = Array.from({ length: 80 }, (_, index) => {
      const value = 3 + index / 20;
      return {
        observed_at_ms: start + index * HOUR_MS,
        label_available_at_ms: start + (index + 1) * HOUR_MS,
        features: { log_raw_score: value, spread_to_bid: index % 5 / 10 },
        profit_per_margin_day: value / 100,
        adverse_per_margin_day: (8 - value) / 100,
        loss: index < 8 ? 1 : 0,
        adverse_breach: index < 20 ? 1 : 0,
        weight: 1,
      };
    });
    const model = trainEconomicModels(examples, { lossIterations: 30 });
    const low = predictEconomicOutcome(model, { log_raw_score: 3.2, spread_to_bid: 0.1 });
    const high = predictEconomicOutcome(model, { log_raw_score: 6.5, spread_to_bid: 0.1 });
    assert.ok(high.expected_profit_per_margin_day > low.expected_profit_per_margin_day);
    assert.ok(high.expected_adverse_per_margin_day < low.expected_adverse_per_margin_day);
    assert.ok(model.targets.primary.includes('margin-day'));
  });

  test('walk-forward predictions admit labels only after maturity and embargo', () => {
    const start = Date.UTC(2026, 0, 1);
    const expiry = Math.floor((start + 8 * 24 * HOUR_MS) / 1000);
    const frames = framesFrom(Array.from({ length: 10 }, (_, hour) => ({
      timestampMs: start + hour * HOUR_MS,
      spot: 2000,
      options: [option({ expiry, bid: 10, ask: 11 })],
    })));
    const example = {
      observed_at_ms: start,
      label_available_at_ms: start + HOUR_MS,
      features: { log_raw_score: Math.log1p(100) },
      profit_per_margin_day: 0.01,
      adverse_per_margin_day: 0.02,
      loss: 0,
      adverse_breach: 0,
      weight: 1,
    };
    const tape = buildEconomicPredictionTape(frames, [example], {
      minSamples: 1,
      minIndependentFrames: 1,
      embargoHours: 6,
      retrainHours: 24,
      readinessCheckHours: 1,
      lossIterations: 5,
    });
    assert.strictEqual(tape.first_prediction_at, new Date(start + 7 * HOUR_MS).toISOString());
    assert.ok(tape.artifacts.every((artifact) => (
      new Date(artifact.label_through).getTime() <= new Date(artifact.training_cutoff).getTime()
    )));
  });

  test('value policy can abstain and ranks by risk-adjusted profit rate', () => {
    const timestampMs = Date.UTC(2026, 0, 1);
    const predictions = new Map([[timestampMs, new Map([
      ['SAFE', { expected_profit_per_margin_day: 0.03, expected_adverse_per_margin_day: 0.01, loss_probability: 0.1, model_version: 'm1' }],
      ['RISKY', { expected_profit_per_margin_day: 0.05, expected_adverse_per_margin_day: 0.20, loss_probability: 0.4, adverse_breach_probability: 0.4, model_version: 'm1' }],
    ])]]);
    const tape = { predictions, options: { minBid: 4 } };
    const candidates = [
      { instrument_name: 'SAFE', bid_price: 5, raw_score: 50 },
      { instrument_name: 'RISKY', bid_price: 8, raw_score: 80 },
    ];
    predictions.get(timestampMs).get('SAFE').adverse_breach_probability = 0.1;
    const policy = makeEconomicValuePolicy(tape, { riskPenalty: 0.25, minUtility: 0, maxLossProbability: 0.35 });
    assert.strictEqual(policy.select({ frame: { timestamp_ms: timestampMs }, candidates }).candidate.instrument_name, 'SAFE');
    const abstain = makeEconomicValuePolicy(tape, { riskPenalty: 0.25, minUtility: 0.1, maxLossProbability: 0.35 });
    assert.strictEqual(abstain.select({ frame: { timestamp_ms: timestampMs }, candidates }), null);
  });
});

describe('models and leakage controls', () => {
  function trainingExamples(count = 200) {
    const start = Date.UTC(2025, 0, 1);
    return Array.from({ length: count }, (_, index) => {
      const raw = 50 + index / 2;
      const spread = 0.03 + (index % 20) / 100;
      return {
        observed_at_ms: start + index * HOUR_MS,
        label_available_at_ms: start + (index + 72) * HOUR_MS,
        capture_return: raw / 200 - spread,
        tail_loss: spread > 0.18 ? 1 : 0,
        weight: 1,
        features: { raw_score: raw, spread_pct: spread },
      };
    });
  }

  test('learns higher capture for stronger raw value', () => {
    const model = trainOutcomeModels(trainingExamples());
    const low = predictOutcome(model, { raw_score: 55, spread_pct: 0.05 }, 0);
    const high = predictOutcome(model, { raw_score: 140, spread_pct: 0.05 }, 0);
    assert.ok(high.expected_capture > low.expected_capture);
  });

  test('does not train before labels mature and the embargo passes', () => {
    const examples = trainingExamples(1);
    const policy = new WalkForwardLearnedPolicy(examples, {
      minSamples: 1,
      minIndependentFrames: 1,
      embargoHours: 6,
      retrainHours: 24,
    });
    policy.onFrame({ timestamp_ms: examples[0].label_available_at_ms, timestamp: new Date(examples[0].label_available_at_ms).toISOString() });
    assert.strictEqual(policy.model, null);
    const readyAt = examples[0].label_available_at_ms + 6 * HOUR_MS;
    policy.nextRetrainAtMs = -Infinity;
    policy.onFrame({ timestamp_ms: readyAt, timestamp: new Date(readyAt).toISOString() });
    assert.ok(policy.model);
    assert.ok(new Date(policy.model.label_through).getTime() <= readyAt - 6 * HOUR_MS);
  });
});

describe('portfolio replay', () => {
  test('accounts for entry premium and an executable profit-capture buyback', () => {
    const start = Date.UTC(2026, 0, 1, 0);
    const expiry = Math.floor((start + 8 * 24 * HOUR_MS) / 1000);
    const frames = framesFrom([
      { timestampMs: start, spot: 2000, options: [option({ expiry, bid: 10, ask: 11 })] },
      { timestampMs: start + HOUR_MS, spot: 2000, options: [option({ expiry, bid: 0.8, ask: 1, mark: 0.9 })] },
    ]);
    const result = runBacktest(frames, makeRawScorePolicy(), {
      startingEth: 1,
      callExposureCap: 0.45,
      marginRate: 0.15,
      marginBudgetPct: 0.45,
      profitCapturePct: 0.8,
    });
    assert.strictEqual(result.trades, 1);
    assert.strictEqual(result.trade_log[0].reason, 'profit_capture');
    assert.ok(Math.abs(result.realized_call_pnl - 4.05) < 1e-9);
    assert.ok(Math.abs(result.ending_nav - 2004.05) < 1e-9);
  });

  test('settles an in-the-money short call at intrinsic value', () => {
    const start = Date.UTC(2026, 0, 1, 0);
    const expiryMs = start + 6 * 24 * HOUR_MS;
    const expiry = Math.floor(expiryMs / 1000);
    const frames = framesFrom([
      { timestampMs: start, spot: 2000, options: [option({ expiry, strike: 2100, bid: 10, ask: 11 })] },
      { timestampMs: expiryMs, spot: 2200, options: [] },
    ]);
    const result = runBacktest(frames, makeRawScorePolicy(), { startingEth: 1, callExposureCap: 0.45 });
    assert.strictEqual(result.trades, 1);
    assert.strictEqual(result.trade_log[0].reason, 'expiry');
    assert.strictEqual(result.trade_log[0].close_price, 100);
    assert.ok(result.realized_call_pnl < 0);
  });

  test('no-call policy exactly tracks the ETH/cash baseline', () => {
    const start = Date.UTC(2026, 0, 1, 0);
    const frames = framesFrom([
      { timestampMs: start, spot: 2000, options: [] },
      { timestampMs: start + HOUR_MS, spot: 2100, options: [] },
    ]);
    const result = runBacktest(frames, makeNoCallPolicy(), { startingEth: 2, startingCash: 100 });
    assert.strictEqual(result.ending_nav, result.eth_baseline_ending_nav);
    assert.strictEqual(result.overlay_pnl, 0);
  });

  test('does not open a position on the terminal replay frame', () => {
    const start = Date.UTC(2026, 0, 1, 0);
    const expiry = Math.floor((start + 8 * 24 * HOUR_MS) / 1000);
    const frames = framesFrom([
      { timestampMs: start, spot: 2000, options: [] },
      { timestampMs: start + HOUR_MS, spot: 2000, options: [option({ expiry, bid: 10, ask: 11 })] },
    ]);
    const result = runBacktest(frames, makeRawScorePolicy(), { startingEth: 1 });
    assert.strictEqual(result.trades, 0);
    assert.strictEqual(result.overlay_pnl, 0);
  });

  test('runs a learned policy with versioned walk-forward models', () => {
    const start = Date.UTC(2026, 0, 1, 0);
    const expiry = Math.floor((start + 8 * 24 * HOUR_MS) / 1000);
    const frames = framesFrom(Array.from({ length: 36 }, (_, hour) => ({
      timestampMs: start + hour * HOUR_MS,
      spot: 2000,
      options: [option({
        expiry,
        bid: 10 - hour * 0.05,
        ask: 10.5 - hour * 0.05,
        mark: 10.25 - hour * 0.05,
      })],
    })));
    const examples = buildLabeledExamples(frames, { horizonHours: 1, quoteWindowHours: 1, topPerFrame: 1 });
    const policy = new WalkForwardLearnedPolicy(examples, {
      minSamples: 8,
      minIndependentFrames: 8,
      embargoHours: 1,
      retrainHours: 8,
      minExpectedCapture: -1,
      maxTailProbability: 1,
      tailIterations: 20,
    });
    const result = runBacktest(frames, policy, { startingEth: 1, callExposureCap: 0.45 });
    assert.ok(result.model_artifacts.length > 0);
    assert.ok(result.model_artifacts.every((artifact) => artifact.version && artifact.capture_model && artifact.tail_model));
    assert.ok(result.model_artifacts.every((artifact) => new Date(artifact.label_through) <= new Date(artifact.training_cutoff)));
  });
});

describe('read-only historical loader', () => {
  test('loads representative chain frames and reconstructs candidates', () => {
    const db = new Database(':memory:');
    db.exec(`
      CREATE TABLE options_snapshots (
        timestamp TEXT, instrument_name TEXT, strike REAL, expiry INTEGER, option_type TEXT,
        delta REAL, ask_price REAL, bid_price REAL, ask_amount REAL, bid_amount REAL,
        mark_price REAL, index_price REAL, implied_vol REAL, open_interest REAL
      );
      CREATE TABLE spot_prices (timestamp TEXT, price REAL);
    `);
    const start = Date.UTC(2026, 0, 1, 0);
    const expiry = Math.floor((start + 8 * 24 * HOUR_MS) / 1000);
    const insertOption = db.prepare('INSERT INTO options_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
    for (let hour = 0; hour < 2; hour++) {
      const timestamp = new Date(start + hour * HOUR_MS).toISOString();
      insertOption.run(timestamp, 'ETH-20260109-2200-C', 2200, expiry, 'C', 0.1, 11, 10, 10, 10, 10.5, 2000, 0.6, 100);
      db.prepare('INSERT INTO spot_prices VALUES (?, ?)').run(timestamp, 2000 + hour * 10);
    }
    const sameHourTimestamp = new Date(start + 15 * 60 * 1000).toISOString();
    insertOption.run(sameHourTimestamp, 'ETH-20260109-2200-C', 2200, expiry, 'C', 0.1, 12, 11, 10, 10, 11.5, 2005, 0.6, 101);
    const loaded = loadHistoricalFrames(db, { days: 'all', cadenceHours: 1 });
    assert.strictEqual(loaded.frames.length, 2);
    assert.strictEqual(loaded.frames[0].timestamp, new Date(start).toISOString());
    assert.strictEqual(loaded.frames[1].timestamp, new Date(start + HOUR_MS).toISOString());
    assert.strictEqual(loaded.frames[0].candidates.length, 1);
    assert.strictEqual(loaded.frames[0].candidates[0].raw_score, 100);
    db.close();
  });
});

console.log(`\n${passed} passed, ${failed} failed`);
if (failed > 0) process.exitCode = 1;
