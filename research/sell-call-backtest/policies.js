'use strict';

const { HOUR_MS } = require('./utils');
const { trainOutcomeModels, predictOutcome } = require('./models');
const strategyFacts = require('../../bot/strategy-facts.json');
const {
  SELL_CALL_EDGE_REFERENCE_DTE,
  SELL_CALL_EDGE_DTE_EXPONENT,
  normalizeSellCallScore,
} = require('../../bot/call-score');
const { callFairEdge } = require('../../bot/call-fair-value');
const { realizedVol } = require('../../bot/vol-surface');

const CURRENT_EDGE_VERSION = `sell-call-edge-dte-${SELL_CALL_EDGE_REFERENCE_DTE}-exponent-${SELL_CALL_EDGE_DTE_EXPONENT}`;
const HISTORICAL_COMPOSITE_EDGE_VERSION = 'sell-call-edge-2026-07-08';

function currentEdgeScore(candidate) {
  const rawScore = Number(candidate?.raw_score || 0);
  const score = normalizeSellCallScore(rawScore, candidate?.dte);
  return {
    score,
    multiplier: rawScore > 0 ? score / rawScore : 0,
    reasons: score > 0 ? ['dte_normalization'] : [],
    version: CURRENT_EDGE_VERSION,
  };
}

// Retained for comparisons with the historical factor-tuning challenger.
function historicalCompositeEdgeScore(candidate) {
  const rawScore = Number(candidate?.raw_score || 0);
  if (!(rawScore > 0)) return { score: 0, multiplier: 0, reasons: [] };
  const dte = Number(candidate.dte);
  const bid = Number(candidate.bid_price);
  const spread = candidate.spread_pct;
  const marketSpread = candidate.features?.market_avg_spread;
  const trend = candidate.features?.score_trend_24h_pct;
  const bestPut = candidate.features?.market_best_put_score;
  const skew = candidate.features?.market_skew;
  const oiTrend = candidate.features?.market_oi_delta_24h_pct;
  let multiplier = 1;
  const reasons = [];

  if (Number.isFinite(dte)) {
    if (dte < 7) {
      multiplier *= 0.95;
      reasons.push('dte_5_7');
    } else if (dte <= 10.5) {
      multiplier *= 1.05;
      reasons.push('dte_7_10_5');
    } else {
      multiplier *= 0.95;
      reasons.push('dte_10_5_12');
    }
  }
  if (rawScore < 65) multiplier *= 0.85;
  else if (rawScore < 86) multiplier *= 1.02;
  else if (rawScore < 90) multiplier *= 1.08;
  else multiplier *= 1.14;
  if (bid >= 7.3) multiplier *= 1.08;

  if (spread != null && spread <= 0.10) multiplier *= spread <= 0.095 ? 1.18 : 1.12;
  else if (spread != null && spread > 0.10) multiplier *= spread > 0.13 ? 0.65 : 0.82;

  if (marketSpread != null && marketSpread <= 0.13) multiplier *= 1.12;
  else if (marketSpread != null && marketSpread > 0.16) multiplier *= 0.75;
  else if (marketSpread != null && marketSpread > 0.13) multiplier *= 0.9;

  if (trend != null && trend < -3) multiplier *= 0.75;
  else if (trend != null && trend >= -3) multiplier *= 1.1;
  if (trend != null && trend > 25 && dte >= 11) multiplier *= 0.9;

  if (bestPut != null && bestPut >= 0.0029) multiplier *= 0.75;
  else if (bestPut != null) multiplier *= 1.05;
  if (skew != null && skew >= 0.073) multiplier *= 0.85;
  else if (skew != null) multiplier *= 1.04;
  if (oiTrend != null && oiTrend >= 5) multiplier *= 1.08;

  return {
    score: rawScore * multiplier,
    multiplier,
    reasons,
    version: HISTORICAL_COMPOSITE_EDGE_VERSION,
  };
}

function makeNoCallPolicy() {
  return {
    name: 'no_call',
    description: 'ETH/cash baseline with no short-call entries',
    select() { return null; },
    getArtifacts() { return []; },
  };
}

function makeRawScorePolicy(options = {}) {
  const minBid = Number(options.minBid ?? 4);
  const minRawScore = Number(options.minRawScore ?? 65);
  return {
    name: 'raw_score',
    description: `Highest raw bid/delta score with bid >= ${minBid} and raw score >= ${minRawScore}`,
    select({ candidates }) {
      const candidate = candidates
        .filter((item) => item.bid_price >= minBid && item.raw_score >= minRawScore)
        .sort((a, b) => b.raw_score - a.raw_score)[0];
      return candidate ? {
        candidate,
        score: candidate.raw_score,
        model_version: 'raw-score-v1',
        diagnostics: { raw_score: candidate.raw_score },
      } : null;
    },
    getArtifacts() { return []; },
  };
}

function makeCurrentEdgePolicy(options = {}) {
  const minBid = Number(options.minBid ?? strategyFacts.sell_call_fallback_min_bid);
  const minEdge = Number(options.minEdge ?? strategyFacts.sell_call_fallback_min_score);
  return {
    name: 'current_edge',
    description: `Production DTE-normalized CALL EDGE with bid >= ${minBid} and edge >= ${minEdge}`,
    select({ candidates }) {
      const ranked = candidates
        .filter((candidate) => candidate.bid_price >= minBid)
        .map((candidate) => ({ candidate, edge: currentEdgeScore(candidate) }))
        .filter((item) => item.edge.score >= minEdge)
        .sort((a, b) => b.edge.score - a.edge.score);
      if (ranked.length === 0) return null;
      return {
        candidate: ranked[0].candidate,
        score: ranked[0].edge.score,
        model_version: CURRENT_EDGE_VERSION,
        diagnostics: ranked[0].edge,
      };
    },
    getArtifacts() { return []; },
  };
}

// Stand-in for the advisor's rolling high-score gate: sell the best CALL EDGE only when it ranks at
// or above minPercentile of the per-frame best edges seen over the prior windowDays.
function makeRollingBestPolicy(options = {}) {
  const minBid = Number(options.minBid ?? strategyFacts.sell_call_fallback_min_bid);
  const windowMs = Number(options.windowDays ?? 6.2) * 24 * HOUR_MS;
  const minPercentile = Number(options.minPercentile ?? 80);
  const history = [];
  let current = null;
  return {
    name: 'rolling_best',
    description: `Best CALL EDGE at >= ${minPercentile}th percentile of its prior ${options.windowDays ?? 6.2}d`,
    onFrame(frame) {
      if (current) history.push(current);
      while (history.length && history[0].t < frame.timestamp_ms - windowMs) history.shift();
      const best = frame.candidates.filter((c) => c.bid_price >= minBid)
        .map((c) => ({ c, edge: currentEdgeScore(c).score })).sort((a, b) => b.edge - a.edge)[0];
      current = best ? { t: frame.timestamp_ms, edge: best.edge } : null;
    },
    select({ candidates }) {
      const best = candidates.filter((c) => c.bid_price >= minBid)
        .map((c) => ({ c, edge: currentEdgeScore(c).score })).sort((a, b) => b.edge - a.edge)[0];
      if (!best || history.length < 24) return null;
      const pct = history.filter((h) => h.edge <= best.edge).length / history.length * 100;
      if (pct < minPercentile) return null;
      return { candidate: best.c, score: best.edge, model_version: 'rolling-best', diagnostics: { percentile: pct } };
    },
    getArtifacts() { return []; },
  };
}

// Sell the call whose bid most exceeds its fair value at forecast realized vol.
// forecast: rv3 | rv7 | max (the larger of the two, cautious when vol is rising).
// rank: edge (dollars over fair) | ratio (bid / fair). minEdge is dollars per contract.
function makeFairValuePolicy(options = {}) {
  const minBid = Number(options.minBid ?? strategyFacts.sell_call_fallback_min_bid);
  const minEdge = Number(options.minEdge ?? 0);
  const forecast = options.forecast || 'rv7';
  const rank = options.rank || 'edge';
  const spots = [];
  let rv = null;
  return {
    name: `fair_value_${forecast}_${rank}_${minEdge}`,
    description: `Bid minus fair value at ${forecast} realized vol, ranked by ${rank}, edge >= $${minEdge}`,
    onFrame(frame) {
      spots.push(frame.spot_price);
      if (spots.length > 24 * 7 + 1) spots.shift();
      const rv3 = realizedVol(spots.slice(-(24 * 3 + 1)));
      const rv7 = spots.length > 24 * 7 ? realizedVol(spots) : null;
      rv = forecast === 'rv3' ? rv3 : forecast === 'rv7' ? rv7 : (rv3 != null && rv7 != null ? Math.max(rv3, rv7) : null);
    },
    select({ frame, candidates }) {
      if (!(rv > 0)) return null;
      const ranked = candidates
        .filter((c) => c.bid_price >= minBid)
        .map((c) => ({ c, v: callFairEdge({ bid: c.bid_price, spot: frame.spot_price, strike: c.strike, dte: c.dte, rv }) }))
        .filter((x) => x.v && x.v.edge >= minEdge && x.v.fair > 0)
        .map((x) => ({ ...x, key: rank === 'ratio' ? x.c.bid_price / x.v.fair : x.v.edge }))
        .sort((a, b) => b.key - a.key);
      if (!ranked.length) return null;
      const { c, v, key } = ranked[0];
      return { candidate: c, score: key, model_version: 'call-fair-value', diagnostics: { rv, ...v } };
    },
    getArtifacts() { return []; },
  };
}

class WalkForwardLearnedPolicy {
  constructor(examples = [], options = {}) {
    this.name = 'learned_walk_forward';
    this.description = 'Regularized expected-capture model constrained by learned tail-loss probability';
    this.examples = [...examples].sort((a, b) => a.label_available_at_ms - b.label_available_at_ms);
    this.options = {
      minBid: Number(options.minBid ?? 4),
      minExpectedCapture: Number(options.minExpectedCapture ?? 0),
      maxTailProbability: Number(options.maxTailProbability ?? 0.20),
      tailPenalty: Number(options.tailPenalty ?? 0.5),
      minSamples: Math.max(1, Math.floor(Number(options.minSamples || 500))),
      minIndependentFrames: Math.max(1, Math.floor(Number(options.minIndependentFrames || 120))),
      maxTrainingSamples: Math.max(1, Math.floor(Number(options.maxTrainingSamples || 20000))),
      retrainHours: Math.max(1, Number(options.retrainHours || 168)),
      trainingWindowDays: Math.max(1, Number(options.trainingWindowDays || 180)),
      embargoHours: Math.max(0, Number(options.embargoHours || 6)),
      captureLambda: Number(options.captureLambda ?? 5),
      tailLambda: Number(options.tailLambda ?? 1),
      tailIterations: Math.max(1, Math.floor(Number(options.tailIterations || 200))),
      tailLearningRate: Number(options.tailLearningRate || 0.08),
    };
    this.model = null;
    this.nextRetrainAtMs = -Infinity;
    this.artifacts = [];
  }

  onFrame(frame) {
    if (frame.timestamp_ms < this.nextRetrainAtMs) return;
    const cutoffMs = frame.timestamp_ms - this.options.embargoHours * HOUR_MS;
    const windowStartMs = frame.timestamp_ms - this.options.trainingWindowDays * 24 * HOUR_MS;
    const matured = this.examples.filter((example) => (
      example.label_available_at_ms <= cutoffMs
      && example.observed_at_ms >= windowStartMs
    ));
    const independentFrames = new Set(matured.map((example) => example.observed_at_ms)).size;
    if (matured.length < this.options.minSamples || independentFrames < this.options.minIndependentFrames) {
      this.nextRetrainAtMs = frame.timestamp_ms + 24 * HOUR_MS;
      return;
    }
    const trainingExamples = matured.length > this.options.maxTrainingSamples
      ? matured.slice(matured.length - this.options.maxTrainingSamples)
      : matured;
    this.model = trainOutcomeModels(trainingExamples, {
      trainedAt: frame.timestamp,
      trainingCutoff: new Date(cutoffMs).toISOString(),
      embargoHours: this.options.embargoHours,
      trainingWindowDays: this.options.trainingWindowDays,
      availableMaturedSamples: matured.length,
      maxTrainingSamples: this.options.maxTrainingSamples,
      captureLambda: this.options.captureLambda,
      tailLambda: this.options.tailLambda,
      tailIterations: this.options.tailIterations,
      tailLearningRate: this.options.tailLearningRate,
    });
    this.artifacts.push(this.model);
    this.nextRetrainAtMs = frame.timestamp_ms + this.options.retrainHours * HOUR_MS;
  }

  select({ candidates }) {
    if (!this.model) return null;
    const ranked = candidates
      .filter((candidate) => candidate.bid_price >= this.options.minBid)
      .map((candidate) => ({
        candidate,
        prediction: predictOutcome(this.model, candidate.features, this.options.tailPenalty),
      }))
      .filter((item) => (
        item.prediction.expected_capture >= this.options.minExpectedCapture
        && item.prediction.tail_probability <= this.options.maxTailProbability
      ))
      .sort((a, b) => b.prediction.utility - a.prediction.utility);
    if (ranked.length === 0) return null;
    return {
      candidate: ranked[0].candidate,
      score: ranked[0].prediction.utility,
      model_version: this.model.version,
      diagnostics: ranked[0].prediction,
    };
  }

  getArtifacts() {
    return this.artifacts;
  }
}

function makeLearnedPolicy(examples, options) {
  return new WalkForwardLearnedPolicy(examples, options);
}

module.exports = {
  CURRENT_EDGE_VERSION,
  HISTORICAL_COMPOSITE_EDGE_VERSION,
  WalkForwardLearnedPolicy,
  currentEdgeScore,
  historicalCompositeEdgeScore,
  makeCurrentEdgePolicy,
  makeLearnedPolicy,
  makeNoCallPolicy,
  makeRawScorePolicy,
  makeRollingBestPolicy,
  makeFairValuePolicy,
};
