'use strict';

const { HOUR_MS } = require('./utils');
const { trainOutcomeModels, predictOutcome } = require('./models');

const CURRENT_EDGE_VERSION = 'sell-call-edge-2026-07-08';

function currentEdgeScore(candidate) {
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
    version: CURRENT_EDGE_VERSION,
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
  const minBid = Number(options.minBid ?? 4);
  const minEdge = Number(options.minEdge ?? 80);
  return {
    name: 'current_edge',
    description: `Current hard-coded CALL EDGE with bid >= ${minBid} and edge >= ${minEdge}`,
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
  WalkForwardLearnedPolicy,
  currentEdgeScore,
  makeCurrentEdgePolicy,
  makeLearnedPolicy,
  makeNoCallPolicy,
  makeRawScorePolicy,
};
