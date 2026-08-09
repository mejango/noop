'use strict';

const crypto = require('crypto');
const { DAY_MS, mean, round } = require('./utils');
const { runBacktest } = require('./simulator');

const FACTOR_NAMES = Object.freeze([
  'dte',
  'raw_band',
  'bid',
  'spread',
  'market_spread',
  'trend',
  'hot_long_dte',
  'put_stress',
  'skew',
  'oi',
]);

const ZERO_WEIGHTS = Object.freeze(Object.fromEntries(FACTOR_NAMES.map((name) => [name, 0])));
const CURRENT_WEIGHTS = Object.freeze(Object.fromEntries(FACTOR_NAMES.map((name) => [name, 1])));
const RAW_EQUIVALENT_CONFIG = Object.freeze({
  min_bid: 4,
  min_edge: 65,
  weights: ZERO_WEIGHTS,
});
const CURRENT_EDGE_CONFIG = Object.freeze({
  min_bid: 4,
  min_edge: 80,
  weights: CURRENT_WEIGHTS,
});

function normalizeVariantConfig(config = {}) {
  return {
    min_bid: Number(config.min_bid ?? 4),
    min_edge: Number(config.min_edge ?? 65),
    weights: Object.fromEntries(FACTOR_NAMES.map((name) => [
      name,
      Number(config.weights?.[name] ?? 0),
    ])),
  };
}

function factorMultipliers(candidate) {
  const rawScore = Number(candidate?.raw_score || 0);
  const dte = Number(candidate?.dte);
  const bid = Number(candidate?.bid_price);
  const spread = candidate?.spread_pct;
  const marketSpread = candidate?.features?.market_avg_spread;
  const trend = candidate?.features?.score_trend_24h_pct;
  const bestPut = candidate?.features?.market_best_put_score;
  const skew = candidate?.features?.market_skew;
  const oiTrend = candidate?.features?.market_oi_delta_24h_pct;
  const factors = Object.fromEntries(FACTOR_NAMES.map((name) => [name, 1]));

  if (Number.isFinite(dte)) {
    if (dte < 7) factors.dte = 0.95;
    else if (dte <= 10.5) factors.dte = 1.05;
    else factors.dte = 0.95;
  }
  if (rawScore < 65) factors.raw_band = 0.85;
  else if (rawScore < 86) factors.raw_band = 1.02;
  else if (rawScore < 90) factors.raw_band = 1.08;
  else factors.raw_band = 1.14;
  if (bid >= 7.3) factors.bid = 1.08;

  if (spread != null && spread <= 0.10) factors.spread = spread <= 0.095 ? 1.18 : 1.12;
  else if (spread != null && spread > 0.10) factors.spread = spread > 0.13 ? 0.65 : 0.82;

  if (marketSpread != null && marketSpread <= 0.13) factors.market_spread = 1.12;
  else if (marketSpread != null && marketSpread > 0.16) factors.market_spread = 0.75;
  else if (marketSpread != null && marketSpread > 0.13) factors.market_spread = 0.9;

  if (trend != null && trend < -3) factors.trend = 0.75;
  else if (trend != null && trend >= -3) factors.trend = 1.1;
  if (trend != null && trend > 25 && dte >= 11) factors.hot_long_dte = 0.9;

  if (bestPut != null && bestPut >= 0.0029) factors.put_stress = 0.75;
  else if (bestPut != null) factors.put_stress = 1.05;
  if (skew != null && skew >= 0.073) factors.skew = 0.85;
  else if (skew != null) factors.skew = 1.04;
  if (oiTrend != null && oiTrend >= 5) factors.oi = 1.08;
  return factors;
}

function edgeVariantScore(candidate, rawConfig = {}) {
  const config = normalizeVariantConfig(rawConfig);
  const rawScore = Number(candidate?.raw_score || 0);
  if (!(rawScore > 0)) return { score: 0, multiplier: 0, factors: {}, contributions: {} };
  const factors = factorMultipliers(candidate);
  const contributions = {};
  let multiplier = 1;
  for (const name of FACTOR_NAMES) {
    contributions[name] = Math.pow(factors[name], config.weights[name]);
    multiplier *= contributions[name];
  }
  return {
    score: rawScore * multiplier,
    multiplier,
    factors,
    contributions,
  };
}

function variantId(config) {
  return crypto.createHash('sha256')
    .update(JSON.stringify(normalizeVariantConfig(config)))
    .digest('hex')
    .slice(0, 12);
}

function makeEdgeVariantPolicy(rawConfig = {}, name = null) {
  const config = normalizeVariantConfig(rawConfig);
  const id = variantId(config);
  return {
    name: name || `edge_variant_${id}`,
    description: `Parameterized CALL EDGE ${id}; bid >= ${config.min_bid}; edge >= ${config.min_edge}`,
    select({ candidates }) {
      const ranked = candidates
        .filter((candidate) => candidate.bid_price >= config.min_bid)
        .map((candidate) => ({ candidate, edge: edgeVariantScore(candidate, config) }))
        .filter((item) => item.edge.score >= config.min_edge)
        .sort((a, b) => b.edge.score - a.edge.score || b.candidate.raw_score - a.candidate.raw_score);
      if (ranked.length === 0) return null;
      return {
        candidate: ranked[0].candidate,
        score: ranked[0].edge.score,
        model_version: `edge-tuning-${id}`,
        diagnostics: { ...ranked[0].edge, tuning_config: config },
      };
    },
    getArtifacts() { return []; },
  };
}

function splitChronologicalFrames(frames, options = {}) {
  const trainFraction = Number(options.trainFraction ?? 0.60);
  const validationFraction = Number(options.validationFraction ?? 0.20);
  if (!(trainFraction > 0) || !(validationFraction > 0) || trainFraction + validationFraction >= 1) {
    throw new Error('train and validation fractions must be positive and sum to less than one');
  }
  if (!Array.isArray(frames) || frames.length < 10) throw new Error('at least 10 frames are required for tuning');
  const trainEnd = Math.max(1, Math.floor(frames.length * trainFraction));
  const validationEnd = Math.max(trainEnd + 1, Math.floor(frames.length * (trainFraction + validationFraction)));
  return {
    train: frames.slice(0, trainEnd),
    validation: frames.slice(trainEnd, validationEnd),
    test: frames.slice(validationEnd),
  };
}

function seededRandom(seed = 20260809) {
  let state = Math.floor(Number(seed)) >>> 0;
  if (state === 0) state = 0x9e3779b9;
  return () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return (state >>> 0) / 0x100000000;
  };
}

function generateVariantConfigs(count = 2500, seed = 20260809) {
  const target = Math.max(2, Math.floor(Number(count || 2500)));
  const random = seededRandom(seed);
  const weightValues = [0, 0.25, 0.5, 0.75, 1, 1.25, 1.5, 2];
  const minBids = [4, 5, 6, 7, 7.3];
  const minEdges = [55, 60, 65, 70, 75, 80, 85, 90, 95];
  const configs = [];
  const seen = new Set();
  const add = (config) => {
    const normalized = normalizeVariantConfig(config);
    const key = JSON.stringify(normalized);
    if (seen.has(key)) return;
    seen.add(key);
    configs.push(normalized);
  };
  add(RAW_EQUIVALENT_CONFIG);
  add(CURRENT_EDGE_CONFIG);
  for (const factor of FACTOR_NAMES) {
    for (const weight of [0.5, 1, 1.5, 2]) {
      add({ min_bid: 4, min_edge: 65, weights: { ...ZERO_WEIGHTS, [factor]: weight } });
    }
  }
  while (configs.length < target) {
    add({
      min_bid: minBids[Math.floor(random() * minBids.length)],
      min_edge: minEdges[Math.floor(random() * minEdges.length)],
      weights: Object.fromEntries(FACTOR_NAMES.map((name) => [
        name,
        weightValues[Math.floor(random() * weightValues.length)],
      ])),
    });
  }
  return configs.slice(0, target);
}

function foldSummary(result, frames) {
  const durationDays = Math.max(1 / 24, (frames.at(-1).timestamp_ms - frames[0].timestamp_ms) / DAY_MS);
  const lossPnl = result.trade_log.reduce((sum, trade) => sum + Math.min(0, Number(trade.pnl || 0)), 0);
  const riskPenalty = Math.abs(lossPnl) * 2 + result.tail_losses * result.starting_nav * 0.02;
  return {
    from: frames[0].timestamp,
    to: frames.at(-1).timestamp,
    frames: frames.length,
    days: round(durationDays, 4),
    overlay_pnl: result.overlay_pnl,
    overlay_return: round(result.overlay_pnl / result.starting_nav, 8),
    risk_adjusted_daily_return: round((result.overlay_pnl - riskPenalty) / result.starting_nav / durationDays, 10),
    trades: result.trades,
    wins: result.wins,
    tail_losses: result.tail_losses,
    max_drawdown: result.max_drawdown,
  };
}

function evaluateConfig(config, frames, simulationConfig, name) {
  return foldSummary(runBacktest(frames, makeEdgeVariantPolicy(config, name), simulationConfig), frames);
}

function developmentObjective(train, validation, config) {
  if (train.trades < 3 || validation.trades < 1) return -Infinity;
  const robustReturn = 0.7 * Math.min(train.risk_adjusted_daily_return, validation.risk_adjusted_daily_return)
    + 0.3 * mean([train.risk_adjusted_daily_return, validation.risk_adjusted_daily_return]);
  const normalized = normalizeVariantConfig(config);
  const complexity = mean(Object.values(normalized.weights).map(Math.abs));
  return robustReturn - complexity * 1e-8;
}

function tuneEdgeVariants(frames, options = {}) {
  const splits = splitChronologicalFrames(frames, options);
  const simulationConfig = options.simulationConfig || {};
  const configs = generateVariantConfigs(options.searchCount, options.seed);
  const development = configs.map((config) => {
    const id = variantId(config);
    const train = evaluateConfig(config, splits.train, simulationConfig, `tune_train_${id}`);
    const validation = evaluateConfig(config, splits.validation, simulationConfig, `tune_validation_${id}`);
    return {
      id,
      config,
      objective: developmentObjective(train, validation, config),
      train,
      validation,
    };
  }).sort((a, b) => b.objective - a.objective
    || (b.train.overlay_pnl + b.validation.overlay_pnl) - (a.train.overlay_pnl + a.validation.overlay_pnl)
    || a.id.localeCompare(b.id));
  const winner = development.find((item) => Number.isFinite(item.objective));
  if (!winner) throw new Error('no tuning variant produced trades in both development folds');

  const namedConfigs = [
    ['raw_score_incumbent', RAW_EQUIVALENT_CONFIG],
    ['current_edge', CURRENT_EDGE_CONFIG],
    ['tuned_edge_preselected', winner.config],
  ];
  const holdout = namedConfigs.map(([name, config]) => ({
    name,
    id: variantId(config),
    config: normalizeVariantConfig(config),
    result: evaluateConfig(config, splits.test, simulationConfig, name),
  }));
  const fullHistory = namedConfigs.map(([name, config]) => ({
    name,
    id: variantId(config),
    result: evaluateConfig(config, frames, simulationConfig, `${name}_full`),
  }));
  const rawHoldout = holdout.find((item) => item.name === 'raw_score_incumbent').result;
  const tunedHoldout = holdout.find((item) => item.name === 'tuned_edge_preselected').result;
  return {
    schema_version: 1,
    engine: 'sell-call-edge-tuner-v1',
    computed_at: new Date().toISOString(),
    selection_rule: 'Winner selected only from train + validation risk-adjusted daily overlay return; final chronological holdout is untouched until selection.',
    searched_variants: configs.length,
    seed: Number(options.seed ?? 20260809),
    split: Object.fromEntries(Object.entries(splits).map(([name, values]) => [name, {
      from: values[0].timestamp,
      to: values.at(-1).timestamp,
      frames: values.length,
    }])),
    simulation_config: simulationConfig,
    incumbent: { name: 'raw_score', config: normalizeVariantConfig(RAW_EQUIVALENT_CONFIG) },
    current_edge: { config: normalizeVariantConfig(CURRENT_EDGE_CONFIG) },
    selected_variant: winner,
    development_leaderboard: development.slice(0, Math.max(1, Number(options.leaderboardSize || 20))),
    holdout,
    full_history: fullHistory,
    conclusion: {
      tuned_beats_raw_on_holdout: tunedHoldout.overlay_pnl > rawHoldout.overlay_pnl
        && tunedHoldout.tail_losses <= rawHoldout.tail_losses,
      tuned_minus_raw_holdout_pnl: round(tunedHoldout.overlay_pnl - rawHoldout.overlay_pnl, 6),
      warning: 'One short holdout is not sufficient for production promotion; repeat across future data and shadow execution.',
    },
  };
}

module.exports = {
  CURRENT_EDGE_CONFIG,
  CURRENT_WEIGHTS,
  FACTOR_NAMES,
  RAW_EQUIVALENT_CONFIG,
  ZERO_WEIGHTS,
  edgeVariantScore,
  factorMultipliers,
  generateVariantConfigs,
  makeEdgeVariantPolicy,
  normalizeVariantConfig,
  splitChronologicalFrames,
  tuneEdgeVariants,
  variantId,
};
