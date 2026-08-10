'use strict';

const { economicCandidateFeatures, attachEconomicContexts } = require('./economic-outcomes');
const { predictEconomicOutcome, trainEconomicModels } = require('./economic-models');
const { HOUR_MS, finite } = require('./utils');

function normalizePredictionOptions(options = {}) {
  return {
    minBid: Math.max(0, Number(options.minBid ?? 4)),
    minSamples: Math.max(1, Math.floor(Number(options.minSamples || 500))),
    minIndependentFrames: Math.max(1, Math.floor(Number(options.minIndependentFrames || 120))),
    maxTrainingSamples: Math.max(1, Math.floor(Number(options.maxTrainingSamples || 20000))),
    retrainHours: Math.max(1, Number(options.retrainHours || 168)),
    readinessCheckHours: Math.max(1, Number(options.readinessCheckHours || 24)),
    trainingWindowDays: Math.max(1, Number(options.trainingWindowDays || 180)),
    embargoHours: Math.max(0, Number(options.embargoHours || 6)),
    profitLambda: Number(options.profitLambda ?? 8),
    adverseLambda: Number(options.adverseLambda ?? 12),
    lossLambda: Number(options.lossLambda ?? 2),
    downsideLambda: Number(options.downsideLambda ?? options.lossLambda ?? 2),
    lossIterations: Math.max(1, Math.floor(Number(options.lossIterations || 120))),
    lossLearningRate: Number(options.lossLearningRate || 0.06),
  };
}

function buildEconomicPredictionTape(frames = [], examples = [], rawOptions = {}) {
  const options = normalizePredictionOptions(rawOptions);
  attachEconomicContexts(frames);
  const orderedExamples = [...examples].sort((a, b) => a.label_available_at_ms - b.label_available_at_ms);
  const predictions = new Map();
  const artifacts = [];
  let nextRetrainAtMs = -Infinity;
  let model = null;
  let predictionCount = 0;

  for (const frame of frames) {
    if (frame.timestamp_ms >= nextRetrainAtMs) {
      const cutoffMs = frame.timestamp_ms - options.embargoHours * HOUR_MS;
      const windowStartMs = frame.timestamp_ms - options.trainingWindowDays * 24 * HOUR_MS;
      const matured = orderedExamples.filter((example) => (
        example.label_available_at_ms <= cutoffMs
        && example.observed_at_ms >= windowStartMs
      ));
      const independentFrames = new Set(matured.map((example) => example.observed_at_ms)).size;
      if (matured.length >= options.minSamples && independentFrames >= options.minIndependentFrames) {
        const trainingExamples = matured.length > options.maxTrainingSamples
          ? matured.slice(matured.length - options.maxTrainingSamples)
          : matured;
        model = trainEconomicModels(trainingExamples, {
          trainedAt: frame.timestamp,
          trainingCutoff: new Date(cutoffMs).toISOString(),
          embargoHours: options.embargoHours,
          trainingWindowDays: options.trainingWindowDays,
          availableMaturedSamples: matured.length,
          maxTrainingSamples: options.maxTrainingSamples,
          profitLambda: options.profitLambda,
          adverseLambda: options.adverseLambda,
          lossLambda: options.lossLambda,
          downsideLambda: options.downsideLambda,
          lossIterations: options.lossIterations,
          lossLearningRate: options.lossLearningRate,
        });
        artifacts.push(model);
        nextRetrainAtMs = frame.timestamp_ms + options.retrainHours * HOUR_MS;
      } else {
        nextRetrainAtMs = frame.timestamp_ms + options.readinessCheckHours * HOUR_MS;
      }
    }
    if (!model) continue;
    const framePredictions = new Map();
    for (const candidate of frame.candidates) {
      if (!(candidate.bid_price >= options.minBid)) continue;
      framePredictions.set(candidate.instrument_name, predictEconomicOutcome(
        model,
        economicCandidateFeatures(candidate, frame),
      ));
      predictionCount++;
    }
    if (framePredictions.size > 0) predictions.set(frame.timestamp_ms, framePredictions);
  }
  return {
    engine: 'economic-call-prediction-tape-v1',
    options,
    predictions,
    artifacts,
    prediction_count: predictionCount,
    first_prediction_at: predictions.size ? new Date(predictions.keys().next().value).toISOString() : null,
  };
}

function economicUtility(prediction, riskPenalty = 0.25) {
  if (!prediction) return null;
  return Number(prediction.expected_profit_per_margin_day || 0)
    - Number(riskPenalty || 0) * Number(prediction.expected_adverse_per_margin_day || 0);
}

function makeEconomicValuePolicy(tape, options = {}, name = 'economic_call_value') {
  const config = {
    minBid: Math.max(0, Number(options.minBid ?? tape?.options?.minBid ?? 4)),
    riskPenalty: Math.max(0, Number(options.riskPenalty ?? 0.25)),
    minUtility: Number(options.minUtility ?? 0),
    minExpectedProfitRate: Number(options.minExpectedProfitRate ?? -Infinity),
    maxLossProbability: Number.isFinite(Number(options.maxLossProbability))
      ? Number(options.maxLossProbability)
      : 0.35,
    maxAdverseBreachProbability: Number.isFinite(Number(options.maxAdverseBreachProbability))
      ? Number(options.maxAdverseBreachProbability)
      : 0.50,
    maxExpectedAdverseRate: Number.isFinite(Number(options.maxExpectedAdverseRate))
      ? Number(options.maxExpectedAdverseRate)
      : Infinity,
  };
  return {
    name,
    description: 'Highest predicted net call P&L per margin-day, constrained by explicit downside estimates',
    select({ frame, candidates }) {
      const framePredictions = tape?.predictions?.get(frame.timestamp_ms);
      if (!framePredictions) return null;
      const ranked = candidates
        .filter((candidate) => candidate.bid_price >= config.minBid)
        .map((candidate) => {
          const prediction = framePredictions.get(candidate.instrument_name);
          return { candidate, prediction, utility: economicUtility(prediction, config.riskPenalty) };
        })
        .filter((item) => item.prediction
          && finite(item.utility) >= config.minUtility
          && item.prediction.expected_profit_per_margin_day >= config.minExpectedProfitRate
          && item.prediction.loss_probability <= config.maxLossProbability
          && item.prediction.adverse_breach_probability <= config.maxAdverseBreachProbability
          && item.prediction.expected_adverse_per_margin_day <= config.maxExpectedAdverseRate)
        .sort((a, b) => b.utility - a.utility
          || b.prediction.expected_profit_per_margin_day - a.prediction.expected_profit_per_margin_day
          || b.candidate.raw_score - a.candidate.raw_score);
      if (ranked.length === 0) return null;
      const selected = ranked[0];
      return {
        candidate: selected.candidate,
        score: selected.utility,
        model_version: selected.prediction.model_version,
        diagnostics: {
          ...selected.prediction,
          economic_utility: selected.utility,
          risk_penalty: config.riskPenalty,
          policy_gates: config,
        },
      };
    },
    getArtifacts() { return []; },
  };
}

module.exports = {
  buildEconomicPredictionTape,
  economicUtility,
  makeEconomicValuePolicy,
  normalizePredictionOptions,
};
