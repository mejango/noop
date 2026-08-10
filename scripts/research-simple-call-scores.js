#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const {
  DAY_MS,
  analyzeSimpleScoreRollover,
  attachTrailingDteBenchmarks,
  fitAdditiveDteSlope,
  loadHistoricalFrames,
  makeSimpleCallScorePolicy,
  mean,
  parseArgv,
  parseNumber,
  round,
  runBacktest,
  splitChronologicalFrames,
  topSimpleScoreSeries,
} = require('../research/sell-call-backtest');

function simulationConfig(args) {
  return {
    startingEth: parseNumber(args['starting-eth'], 5),
    startingCash: parseNumber(args['starting-cash'], 0),
    execution: args.execution || 'bid_ask',
    feeBps: parseNumber(args['fee-bps'], 0),
    settlementFeeBps: parseNumber(args['settlement-fee-bps'], parseNumber(args['fee-bps'], 0)),
    callExposureCap: parseNumber(args['exposure-cap'], 0.45),
    marginRate: parseNumber(args['margin-rate'], 0.15),
    marginBudgetPct: parseNumber(args['margin-budget-pct'], 0.45),
    profitCapturePct: parseNumber(args['profit-capture-pct'], 0.80),
    maxOpenPositions: parseNumber(args['max-open-positions'], 1),
    amountStep: parseNumber(args['amount-step'], 0.01),
  };
}

function durationDays(frames) {
  return Math.max(1 / 24, (frames.at(-1).timestamp_ms - frames[0].timestamp_ms) / DAY_MS);
}

function summarize(result, frames) {
  const lossPnl = result.trade_log.reduce((sum, trade) => sum + Math.min(0, Number(trade.pnl || 0)), 0);
  const riskPenalty = Math.abs(lossPnl) * 2 + result.tail_losses * result.starting_nav * 0.02;
  return {
    overlay_pnl: result.overlay_pnl,
    pnl_per_day: round(result.overlay_pnl / durationDays(frames), 8),
    risk_adjusted_pnl_per_day: round((result.overlay_pnl - riskPenalty) / durationDays(frames), 8),
    trades: result.trades,
    wins: result.wins,
    tail_losses: result.tail_losses,
    max_drawdown: result.max_drawdown,
    max_margin_used: result.max_margin_used,
    return_on_max_margin: result.return_on_max_margin,
    average_holding_hours: result.average_holding_hours,
  };
}

function evaluate(frames, config, threshold, simulation, name) {
  const result = runBacktest(frames, makeSimpleCallScorePolicy({ ...config, minScore: threshold }, name), simulation);
  return summarize(result, frames);
}

function contiguousBlocks(frames, count) {
  const size = Math.floor(frames.length / count);
  return Array.from({ length: count }, (_, index) => frames.slice(
    index * size,
    index === count - 1 ? frames.length : (index + 1) * size,
  ));
}

function quantile(sorted, probability) {
  if (sorted.length === 0) return null;
  const index = (sorted.length - 1) * probability;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sorted[lower];
  return sorted[lower] + (sorted[upper] - sorted[lower]) * (index - lower);
}

function thresholdCandidates(frames, config) {
  const scores = topSimpleScoreSeries(frames, config).map((point) => point.score).sort((a, b) => a - b);
  const probabilities = Array.from({ length: 17 }, (_, index) => 0.10 + index * 0.05);
  const values = probabilities.map((probability) => quantile(scores, probability)).filter(Number.isFinite);
  if (config.family === 'production') values.push(65);
  return [...new Set(values.map((value) => round(value, 8)))].sort((a, b) => a - b);
}

function standardDeviation(values) {
  const average = mean(values);
  return Math.sqrt(mean(values.map((value) => (value - average) ** 2)));
}

function namedThresholds(value) {
  if (!value) return {};
  return Object.fromEntries(String(value).split(',').map((item) => {
    const separator = item.lastIndexOf(':');
    const name = separator > 0 ? item.slice(0, separator) : '';
    const threshold = separator > 0 ? Number(item.slice(separator + 1)) : NaN;
    if (!name || !Number.isFinite(threshold)) throw new Error(`invalid threshold override: ${item}`);
    return [name, threshold];
  }));
}

function tuneThreshold(trainFrames, trainBlocks, config, simulation) {
  const grid = thresholdCandidates(trainFrames, config).map((threshold) => {
    const folds = trainBlocks.map((frames, index) => evaluate(
      frames,
      config,
      threshold,
      simulation,
      `${config.name}_threshold_${threshold}_fold_${index}`,
    ));
    const rates = folds.map((fold) => fold.risk_adjusted_pnl_per_day);
    const hasCoverage = folds.every((fold) => fold.trades >= 1);
    return {
      threshold,
      folds,
      has_coverage: hasCoverage,
      worst_rate: round(Math.min(...rates), 8),
      mean_rate: round(mean(rates), 8),
      stability_adjusted_rate: round(mean(rates) - standardDeviation(rates), 8),
      objective: hasCoverage ? round(0.7 * Math.min(...rates) + 0.3 * mean(rates), 8) : -Infinity,
    };
  }).sort((a, b) => b.objective - a.objective
    || b.stability_adjusted_rate - a.stability_adjusted_rate
    || a.threshold - b.threshold);
  const selected = grid.find((row) => Number.isFinite(row.objective));
  if (!selected) throw new Error(`no threshold gave ${config.name} coverage in every training block`);
  return { selected, grid };
}

function money(value) {
  return `$${Number(value || 0).toFixed(2)}`;
}

function renderMarkdown(report) {
  return [
    '# Simple Call-Score Hypotheses',
    '',
    `Computed: ${report.computed_at}`,
    `History: ${report.historical_window.from} through ${report.historical_window.to}`,
    `Additive DTE slope fitted from training only: ${report.fitted_additive_dte_slope}.`,
    `Preselected on validation: ${report.preselected.name}.`,
    '',
    '| Formula | Threshold | Train P&L | Validation P&L | Holdout P&L | Full P&L | Trades | Rollover peak |',
    '|---|---:|---:|---:|---:|---:|---:|---:|',
    ...report.results.map((row) => `| ${row.name} | ${row.threshold} | ${money(row.train.overlay_pnl)} | ${money(row.validation.overlay_pnl)} | ${money(row.holdout.overlay_pnl)} | ${money(row.full.overlay_pnl)} | ${row.full.trades} | ${row.rollover.mean_positive_peak_jump_pct ?? 'n/a'} |`),
    '',
    `Locked production floor-65 baseline: holdout ${money(report.locked_production.holdout.overlay_pnl)}, full ${money(report.locked_production.full.overlay_pnl)}, rollover peak ${report.locked_production.rollover.mean_positive_peak_jump_pct}.`,
    '',
    'Each formula is a predeclared economic hypothesis. Its threshold is selected only from three contiguous blocks inside the first 60%. Formula selection uses the next 20%; the final 20% is then reported as holdout.',
    '',
    'Historical top-of-book replay cannot prove fills or future upside-tail performance.',
    '',
  ].join('\n');
}

function main() {
  const args = parseArgv(process.argv.slice(2));
  const dataDir = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
  const dbPath = args.db || process.env.DB_PATH || path.join(dataDir, 'noop.db');
  if (!fs.existsSync(dbPath)) throw new Error(`database not found: ${dbPath}`);
  const outputPath = args.out || path.join(dataDir, 'simple-call-score-study.json');
  const markdownPath = args.markdown || outputPath.replace(/\.json$/i, '.md');
  const simulation = simulationConfig(args);
  const thresholdOverrides = namedThresholds(args['threshold-overrides']);
  const common = {
    minBid: parseNumber(args['min-bid'], 4),
    minDte: parseNumber(args['min-dte'], 5),
    maxDte: parseNumber(args['max-dte'], 12),
    referenceDte: parseNumber(args['reference-dte'], 8.5),
    exponent: parseNumber(args.exponent, 0.12),
  };
  const db = new Database(dbPath, { readonly: true, fileMustExist: true });
  db.pragma('query_only = ON');
  try {
    const data = loadHistoricalFrames(db, {
      days: args.days || 'all',
      from: args.from,
      to: args.to,
      cadenceHours: parseNumber(args['cadence-hours'], 1),
      callDteRange: [common.minDte, common.maxDte],
    });
    const split = splitChronologicalFrames(data.frames, { trainFraction: 0.6, validationFraction: 0.2 });
    attachTrailingDteBenchmarks(data.frames, {
      ...common,
      lookbackDays: parseNumber(args['relative-lookback-days'], 30),
      minSamples: parseNumber(args['relative-min-samples'], 24),
    });
    const trainBlocks = contiguousBlocks(split.train, 3);
    const fullBlocks = contiguousBlocks(data.frames, 5);
    const additiveSlope = fitAdditiveDteSlope(split.train, common);
    const configs = [
      { ...common, name: 'production_tuned_floor', family: 'production' },
      { ...common, name: 'additive_dte_detrend', family: 'additive_dte', additiveSlope },
      { ...common, name: 'half_spread_haircut', family: 'spread_haircut', spreadHaircut: 0.5 },
      { ...common, name: 'full_spread_haircut', family: 'spread_haircut', spreadHaircut: 1 },
      { ...common, name: 'geometric_quote_quality', family: 'quote_quality' },
      { ...common, name: 'expected_move_efficiency', family: 'expected_move' },
      { ...common, name: 'net_expected_move_efficiency', family: 'expected_move_net' },
      { ...common, name: 'balanced_expected_move_value', family: 'balanced_expected_move' },
      { ...common, name: 'square_root_premium_rate', family: 'premium_rate' },
      { ...common, name: 'trailing_dte_relative_value', family: 'trailing_dte_relative' },
      { ...common, name: 'trailing_dte_net_relative_value', family: 'trailing_dte_net_relative' },
    ];
    const results = configs.map((config) => {
      const tuning = tuneThreshold(split.train, trainBlocks, config, simulation);
      const threshold = thresholdOverrides[config.name] ?? tuning.selected.threshold;
      return {
        name: config.name,
        formula: config,
        threshold,
        threshold_selection: tuning.selected,
        threshold_grid: tuning.grid,
        train: evaluate(split.train, config, threshold, simulation, `${config.name}_train`),
        validation: evaluate(split.validation, config, threshold, simulation, `${config.name}_validation`),
        holdout: evaluate(split.test, config, threshold, simulation, `${config.name}_holdout`),
        full: evaluate(data.frames, config, threshold, simulation, `${config.name}_full`),
        periods: fullBlocks.map((frames, index) => evaluate(frames, config, threshold, simulation, `${config.name}_period_${index}`)),
        rollover: analyzeSimpleScoreRollover(data.frames, config),
      };
    });
    const validationRanked = [...results].sort((a, b) => b.validation.risk_adjusted_pnl_per_day - a.validation.risk_adjusted_pnl_per_day
      || b.validation.overlay_pnl - a.validation.overlay_pnl
      || a.name.localeCompare(b.name));
    const lockedProductionConfig = { ...common, family: 'production', name: 'production_locked_floor_65' };
    const lockedProduction = {
      threshold: 65,
      train: evaluate(split.train, lockedProductionConfig, 65, simulation, 'locked_production_train'),
      validation: evaluate(split.validation, lockedProductionConfig, 65, simulation, 'locked_production_validation'),
      holdout: evaluate(split.test, lockedProductionConfig, 65, simulation, 'locked_production_holdout'),
      full: evaluate(data.frames, lockedProductionConfig, 65, simulation, 'locked_production_full'),
      rollover: analyzeSimpleScoreRollover(data.frames, lockedProductionConfig),
    };
    const report = {
      schema_version: 1,
      engine: 'simple-call-score-study-v1',
      computed_at: new Date().toISOString(),
      historical_window: data.window,
      frames: data.frames.length,
      cadence_hours: data.cadence_hours,
      split: {
        train: { from: split.train[0].timestamp, to: split.train.at(-1).timestamp },
        validation: { from: split.validation[0].timestamp, to: split.validation.at(-1).timestamp },
        holdout: { from: split.test[0].timestamp, to: split.test.at(-1).timestamp },
      },
      fitted_additive_dte_slope: round(additiveSlope, 8),
      threshold_selection_rule: 'Within the first 60%, maximize 70% worst-block + 30% mean risk-adjusted P&L/day across three contiguous blocks; require at least one trade per block.',
      threshold_overrides: thresholdOverrides,
      formula_selection_rule: 'Preselect the formula with the highest validation risk-adjusted P&L/day, then inspect the final 20% holdout.',
      preselected: { name: validationRanked[0].name, validation: validationRanked[0].validation, holdout: validationRanked[0].holdout },
      locked_production: lockedProduction,
      results,
    };
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    fs.mkdirSync(path.dirname(markdownPath), { recursive: true });
    fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`);
    fs.writeFileSync(markdownPath, `${renderMarkdown(report)}\n`);
    console.log(`Preselected: ${report.preselected.name}`);
    console.log(`JSON: ${outputPath}`);
    console.log(`Markdown: ${markdownPath}`);
  } finally {
    db.close();
  }
}

try {
  main();
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
}
