'use strict';

const crypto = require('crypto');
const {
  fitLogisticRegression,
  fitRidgeRegression,
  predictLogistic,
  predictRidge,
} = require('./models');
const { ECONOMIC_FEATURE_NAMES } = require('./economic-outcomes');
const { clamp, finite, round } = require('./utils');

function quantile(values, probability) {
  const sorted = values.map(finite).filter((value) => value != null).sort((a, b) => a - b);
  if (sorted.length === 0) return null;
  const index = (sorted.length - 1) * clamp(Number(probability), 0, 1);
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sorted[lower];
  return sorted[lower] + (sorted[upper] - sorted[lower]) * (index - lower);
}

function targetRows(examples, targetName, bounds) {
  return examples.map((example) => ({
    ...example,
    capture_return: clamp(Number(example[targetName] || 0), bounds.lower, bounds.upper),
  }));
}

function targetBounds(examples, targetName, options = {}) {
  const values = examples.map((example) => example[targetName]);
  const lowerProbability = Number(options.lowerWinsor ?? 0.01);
  const upperProbability = Number(options.upperWinsor ?? 0.99);
  const lower = quantile(values, lowerProbability);
  const upper = quantile(values, upperProbability);
  return {
    lower: Number.isFinite(lower) ? lower : 0,
    upper: Number.isFinite(upper) ? Math.max(upper, lower) : Math.max(0, lower),
    lower_probability: lowerProbability,
    upper_probability: upperProbability,
  };
}

function weightedMean(examples, selector) {
  let sum = 0;
  let weightSum = 0;
  for (const example of examples) {
    const value = finite(selector(example));
    const weight = Math.max(0, finite(example.weight) ?? 1);
    if (value == null || weight === 0) continue;
    sum += value * weight;
    weightSum += weight;
  }
  return weightSum > 0 ? sum / weightSum : null;
}

function economicTrainingMetrics(examples, artifact) {
  let profitSquaredError = 0;
  let adverseSquaredError = 0;
  let lossLogLoss = 0;
  let downsideLogLoss = 0;
  let totalWeight = 0;
  for (const example of examples) {
    const prediction = predictEconomicOutcome(artifact, example.features);
    const weight = Math.max(0, finite(example.weight) ?? 1);
    profitSquaredError += weight * ((prediction.expected_profit_per_margin_day - example.profit_per_margin_day) ** 2);
    adverseSquaredError += weight * ((prediction.expected_adverse_per_margin_day - example.adverse_per_margin_day) ** 2);
    const probability = clamp(prediction.loss_probability, 1e-9, 1 - 1e-9);
    const downsideProbability = clamp(prediction.adverse_breach_probability, 1e-9, 1 - 1e-9);
    lossLogLoss -= weight * (example.loss ? Math.log(probability) : Math.log(1 - probability));
    downsideLogLoss -= weight * (example.adverse_breach
      ? Math.log(downsideProbability)
      : Math.log(1 - downsideProbability));
    totalWeight += weight;
  }
  return {
    profit_rate_rmse: round(Math.sqrt(profitSquaredError / Math.max(totalWeight, 1)), 8),
    adverse_rate_rmse: round(Math.sqrt(adverseSquaredError / Math.max(totalWeight, 1)), 8),
    loss_log_loss: round(lossLogLoss / Math.max(totalWeight, 1), 8),
    adverse_breach_log_loss: round(downsideLogLoss / Math.max(totalWeight, 1), 8),
    mean_profit_per_margin_day: round(weightedMean(examples, (example) => example.profit_per_margin_day), 8),
    mean_adverse_per_margin_day: round(weightedMean(examples, (example) => example.adverse_per_margin_day), 8),
    loss_rate: round(weightedMean(examples, (example) => example.loss), 8),
    adverse_breach_rate: round(weightedMean(examples, (example) => example.adverse_breach), 8),
  };
}

function trainEconomicModels(examples, options = {}) {
  if (!Array.isArray(examples) || examples.length === 0) {
    throw new Error('cannot train economic value models without examples');
  }
  const featureNames = options.featureNames || ECONOMIC_FEATURE_NAMES;
  // Preserve every observed losing tail. Only the positive extreme is trimmed;
  // otherwise a sparse short-call dataset can literally train the losses away.
  const profitBounds = targetBounds(examples, 'profit_per_margin_day', {
    ...options,
    lowerWinsor: 0,
  });
  const adverseBounds = targetBounds(examples, 'adverse_per_margin_day', {
    ...options,
    lowerWinsor: 0,
  });
  const profitModel = fitRidgeRegression(targetRows(examples, 'profit_per_margin_day', profitBounds), {
    featureNames,
    lambda: Number(options.profitLambda ?? 8),
  });
  const adverseModel = fitRidgeRegression(targetRows(examples, 'adverse_per_margin_day', adverseBounds), {
    featureNames,
    lambda: Number(options.adverseLambda ?? 12),
  });
  const lossModel = fitLogisticRegression(examples.map((example) => ({
    ...example,
    tail_loss: example.loss ? 1 : 0,
  })), {
    featureNames,
    lambda: Number(options.lossLambda ?? 2),
    iterations: Math.max(1, Math.floor(Number(options.lossIterations || 120))),
    learningRate: Number(options.lossLearningRate || 0.06),
    standardizer: profitModel.standardizer,
  });
  const downsideModel = fitLogisticRegression(examples.map((example) => ({
    ...example,
    tail_loss: example.adverse_breach ? 1 : 0,
  })), {
    featureNames,
    lambda: Number(options.downsideLambda ?? options.lossLambda ?? 2),
    iterations: Math.max(1, Math.floor(Number(options.lossIterations || 120))),
    learningRate: Number(options.lossLearningRate || 0.06),
    standardizer: profitModel.standardizer,
  });
  const artifact = {
    schema_version: 1,
    model_family: 'economic_call_value_v1',
    feature_names: [...featureNames],
    trained_at: options.trainedAt || new Date().toISOString(),
    train_start: new Date(Math.min(...examples.map((example) => example.observed_at_ms))).toISOString(),
    train_end: new Date(Math.max(...examples.map((example) => example.observed_at_ms))).toISOString(),
    label_through: new Date(Math.max(...examples.map((example) => example.label_available_at_ms))).toISOString(),
    samples: examples.length,
    independent_frames: new Set(examples.map((example) => example.observed_at_ms)).size,
    available_matured_samples: options.availableMaturedSamples ?? examples.length,
    max_training_samples: options.maxTrainingSamples ?? examples.length,
    training_cutoff: options.trainingCutoff || null,
    embargo_hours: options.embargoHours ?? null,
    training_window_days: options.trainingWindowDays ?? null,
    targets: {
      primary: 'net P&L per margin-day under the replay exit policy',
      risk: 'maximum adverse buyback excursion per margin-day',
      loss: 'realized net P&L below zero',
      downside_breach: 'maximum adverse excursion above the configured margin-loss threshold',
      profit_winsor_bounds: profitBounds,
      adverse_winsor_bounds: adverseBounds,
    },
    profit_model: profitModel,
    adverse_model: adverseModel,
    loss_model: lossModel,
    downside_model: downsideModel,
  };
  artifact.training_metrics = economicTrainingMetrics(examples, artifact);
  artifact.version = crypto.createHash('sha256').update(JSON.stringify(artifact)).digest('hex').slice(0, 12);
  return artifact;
}

function predictEconomicOutcome(artifact, features) {
  const profitBounds = artifact.targets.profit_winsor_bounds;
  const adverseBounds = artifact.targets.adverse_winsor_bounds;
  const expectedProfit = clamp(predictRidge(artifact.profit_model, features), profitBounds.lower, profitBounds.upper);
  const expectedAdverse = clamp(predictRidge(artifact.adverse_model, features), 0, Math.max(0, adverseBounds.upper));
  return {
    expected_profit_per_margin_day: expectedProfit,
    expected_adverse_per_margin_day: expectedAdverse,
    loss_probability: predictLogistic(artifact.loss_model, features),
    adverse_breach_probability: predictLogistic(artifact.downside_model, features),
    model_version: artifact.version,
  };
}

module.exports = {
  economicTrainingMetrics,
  predictEconomicOutcome,
  quantile,
  targetBounds,
  trainEconomicModels,
};
