'use strict';

const crypto = require('crypto');
const { clamp, finite, mean, round } = require('./utils');
const { FEATURE_NAMES } = require('./features');

function buildStandardizer(examples, featureNames = FEATURE_NAMES) {
  const means = {};
  const scales = {};
  for (const feature of featureNames) {
    const values = examples.map((example) => finite(example.features?.[feature])).filter((value) => value != null);
    const featureMean = mean(values) ?? 0;
    const variance = values.length > 0
      ? values.reduce((sum, value) => sum + ((value - featureMean) ** 2), 0) / values.length
      : 0;
    means[feature] = featureMean;
    scales[feature] = Math.sqrt(variance) || 1;
  }
  return { feature_names: [...featureNames], means, scales };
}

function vectorize(features, standardizer) {
  return standardizer.feature_names.map((feature) => {
    const value = finite(features?.[feature]);
    const imputed = value == null ? standardizer.means[feature] : value;
    return (imputed - standardizer.means[feature]) / standardizer.scales[feature];
  });
}

function solveLinearSystem(matrix, vector) {
  const size = vector.length;
  const augmented = matrix.map((row, index) => [...row, vector[index]]);
  for (let column = 0; column < size; column++) {
    let pivot = column;
    for (let row = column + 1; row < size; row++) {
      if (Math.abs(augmented[row][column]) > Math.abs(augmented[pivot][column])) pivot = row;
    }
    if (Math.abs(augmented[pivot][column]) < 1e-12) augmented[pivot][column] = 1e-12;
    [augmented[column], augmented[pivot]] = [augmented[pivot], augmented[column]];
    const divisor = augmented[column][column];
    for (let cell = column; cell <= size; cell++) augmented[column][cell] /= divisor;
    for (let row = 0; row < size; row++) {
      if (row === column) continue;
      const factor = augmented[row][column];
      if (factor === 0) continue;
      for (let cell = column; cell <= size; cell++) {
        augmented[row][cell] -= factor * augmented[column][cell];
      }
    }
  }
  return augmented.map((row) => row[size]);
}

function fitRidgeRegression(examples, options = {}) {
  const featureNames = options.featureNames || FEATURE_NAMES;
  const lambda = Number(options.lambda ?? 5);
  const standardizer = buildStandardizer(examples, featureNames);
  const dimension = featureNames.length + 1;
  const xtx = Array.from({ length: dimension }, () => Array(dimension).fill(0));
  const xty = Array(dimension).fill(0);
  for (const example of examples) {
    const target = finite(example.capture_return);
    if (target == null) continue;
    const weight = Math.max(0, finite(example.weight) ?? 1);
    const row = [1, ...vectorize(example.features, standardizer)];
    for (let i = 0; i < dimension; i++) {
      xty[i] += weight * row[i] * target;
      for (let j = 0; j < dimension; j++) xtx[i][j] += weight * row[i] * row[j];
    }
  }
  for (let i = 1; i < dimension; i++) xtx[i][i] += lambda;
  return {
    type: 'ridge_capture_v1',
    lambda,
    standardizer,
    coefficients: solveLinearSystem(xtx, xty),
  };
}

function predictRidge(model, features) {
  const row = [1, ...vectorize(features, model.standardizer)];
  return row.reduce((sum, value, index) => sum + value * model.coefficients[index], 0);
}

function sigmoid(value) {
  if (value >= 0) {
    const exp = Math.exp(-Math.min(value, 40));
    return 1 / (1 + exp);
  }
  const exp = Math.exp(Math.max(value, -40));
  return exp / (1 + exp);
}

function fitLogisticRegression(examples, options = {}) {
  const featureNames = options.featureNames || FEATURE_NAMES;
  const lambda = Number(options.lambda ?? 1);
  const iterations = Math.max(1, Math.floor(Number(options.iterations || 500)));
  const learningRate = Number(options.learningRate || 0.08);
  const standardizer = options.standardizer || buildStandardizer(examples, featureNames);
  const dimension = featureNames.length + 1;
  const positiveRate = clamp(mean(examples.map((example) => Number(example.tail_loss) || 0)) ?? 0.01, 0.001, 0.999);
  const coefficients = Array(dimension).fill(0);
  coefficients[0] = Math.log(positiveRate / (1 - positiveRate));
  for (let iteration = 0; iteration < iterations; iteration++) {
    const gradient = Array(dimension).fill(0);
    let totalWeight = 0;
    for (const example of examples) {
      const target = Number(example.tail_loss) ? 1 : 0;
      const weight = Math.max(0, finite(example.weight) ?? 1);
      const row = [1, ...vectorize(example.features, standardizer)];
      const prediction = sigmoid(row.reduce((sum, value, index) => sum + value * coefficients[index], 0));
      for (let i = 0; i < dimension; i++) gradient[i] += weight * (prediction - target) * row[i];
      totalWeight += weight;
    }
    const denominator = Math.max(1, totalWeight);
    for (let i = 0; i < dimension; i++) {
      const penalty = i === 0 ? 0 : lambda * coefficients[i] / denominator;
      coefficients[i] -= learningRate * ((gradient[i] / denominator) + penalty);
    }
  }
  return {
    type: 'logistic_tail_v1',
    lambda,
    iterations,
    learning_rate: learningRate,
    standardizer,
    coefficients,
  };
}

function predictLogistic(model, features) {
  const row = [1, ...vectorize(features, model.standardizer)];
  return sigmoid(row.reduce((sum, value, index) => sum + value * model.coefficients[index], 0));
}

function regressionMetrics(examples, captureModel, tailModel) {
  if (examples.length === 0) return {};
  let squaredError = 0;
  let logLoss = 0;
  let totalWeight = 0;
  for (const example of examples) {
    const weight = Math.max(0, finite(example.weight) ?? 1);
    const capturePrediction = predictRidge(captureModel, example.features);
    const tailPrediction = clamp(predictLogistic(tailModel, example.features), 1e-9, 1 - 1e-9);
    squaredError += weight * ((capturePrediction - example.capture_return) ** 2);
    logLoss -= weight * (example.tail_loss ? Math.log(tailPrediction) : Math.log(1 - tailPrediction));
    totalWeight += weight;
  }
  return {
    capture_rmse: round(Math.sqrt(squaredError / Math.max(totalWeight, 1)), 6),
    tail_log_loss: round(logLoss / Math.max(totalWeight, 1), 6),
    mean_capture_return: round(mean(examples.map((example) => example.capture_return)), 6),
    tail_loss_rate: round(mean(examples.map((example) => example.tail_loss)), 6),
  };
}

function trainOutcomeModels(examples, options = {}) {
  if (!Array.isArray(examples) || examples.length === 0) throw new Error('cannot train without labeled examples');
  const featureNames = options.featureNames || FEATURE_NAMES;
  const captureModel = fitRidgeRegression(examples, {
    featureNames,
    lambda: options.captureLambda,
  });
  const tailModel = fitLogisticRegression(examples, {
    featureNames,
    lambda: options.tailLambda,
    iterations: options.tailIterations,
    learningRate: options.tailLearningRate,
    standardizer: captureModel.standardizer,
  });
  const artifact = {
    schema_version: 1,
    model_family: 'regularized_capture_and_tail',
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
    capture_model: captureModel,
    tail_model: tailModel,
    training_metrics: regressionMetrics(examples, captureModel, tailModel),
  };
  artifact.version = crypto.createHash('sha256').update(JSON.stringify(artifact)).digest('hex').slice(0, 12);
  return artifact;
}

function predictOutcome(artifact, features, tailPenalty = 1) {
  const expectedCapture = predictRidge(artifact.capture_model, features);
  const tailProbability = predictLogistic(artifact.tail_model, features);
  return {
    expected_capture: expectedCapture,
    tail_probability: tailProbability,
    utility: expectedCapture - Number(tailPenalty) * tailProbability,
    model_version: artifact.version,
  };
}

module.exports = {
  buildStandardizer,
  fitLogisticRegression,
  fitRidgeRegression,
  predictLogistic,
  predictOutcome,
  predictRidge,
  regressionMetrics,
  solveLinearSystem,
  trainOutcomeModels,
  vectorize,
};
