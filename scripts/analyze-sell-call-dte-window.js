#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const {
  analyzeRolloverDiscontinuity,
  loadHistoricalFrames,
  makeDteNormalizedRawPolicy,
  mean,
  parseArgv,
  parseNumber,
  round,
  runBacktest,
  splitChronologicalFrames,
} = require('../research/sell-call-backtest');

const DEFAULT_WINDOWS = '3-9,3-12,3-14,4-10,4-12,4-14,4-16,5-10,5-12,5-14,5-16,6-10,6-12,6-14,6-16,7-12,7-14,7-16';
const DEFAULT_EXPONENTS = '0,0.04,0.06,0.08,0.10,0.12,0.14,0.16,0.18,0.20';
const DEFAULT_LINEAR_SLOPES = '0.005,0.0075,0.01,0.0125,0.015,0.0175,0.02,0.025,0.03';
const DEFAULT_THRESHOLDS = '55,60,65,70,75';

function numberList(value, fallback) {
  const values = String(value || fallback).split(',').map(Number).filter(Number.isFinite);
  if (values.length === 0) throw new Error('numeric grid cannot be empty');
  return [...new Set(values)].sort((a, b) => a - b);
}

function windowList(value) {
  const windows = String(value || DEFAULT_WINDOWS).split(',').map((item) => {
    const [minDte, maxDte] = item.split('-').map(Number);
    if (!(minDte >= 0) || !(maxDte > minDte)) throw new Error(`invalid DTE window: ${item}`);
    return { name: `${minDte}-${maxDte}`, minDte, maxDte };
  });
  return [...new Map(windows.map((window) => [window.name, window])).values()];
}

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

function policyConfig(window, normalization, minScore, args) {
  return {
    ...window,
    formula: normalization.formula,
    exponent: Number(normalization.exponent ?? 0),
    slope: Number(normalization.slope ?? 0),
    minScore,
    minBid: parseNumber(args['min-bid'], 4),
    referenceDte: parseNumber(args['reference-dte'], 8.5),
  };
}

function evaluate(frames, config, simulation, name) {
  return runBacktest(frames, makeDteNormalizedRawPolicy(config, name), simulation);
}

function standardDeviation(values) {
  if (values.length === 0) return 0;
  const average = mean(values);
  return Math.sqrt(mean(values.map((value) => (value - average) ** 2)));
}

function durationDays(frames) {
  if (frames.length < 2) return 1 / 24;
  return Math.max((frames.at(-1).timestamp_ms - frames[0].timestamp_ms) / 86400000, 1 / 24);
}

function summarize(result) {
  const entryDtes = result.trade_log
    .map((trade) => Number(trade.entry_diagnostics?.dte))
    .filter(Number.isFinite);
  return {
    overlay_pnl: result.overlay_pnl,
    trades: result.trades,
    wins: result.wins,
    tail_losses: result.tail_losses,
    max_drawdown: result.max_drawdown,
    max_margin_used: result.max_margin_used,
    return_on_max_margin: result.return_on_max_margin,
    average_holding_hours: result.average_holding_hours,
    average_entry_dte: round(mean(entryDtes), 4),
    min_entry_dte: entryDtes.length ? round(Math.min(...entryDtes), 4) : null,
    max_entry_dte: entryDtes.length ? round(Math.max(...entryDtes), 4) : null,
  };
}

function contiguousBlocks(frames, count) {
  const size = Math.floor(frames.length / count);
  return Array.from({ length: count }, (_, index) => frames.slice(
    index * size,
    index === count - 1 ? frames.length : (index + 1) * size,
  ));
}

function tuneWindow(window, developmentFrames, developmentBlocks, normalizations, thresholds, simulation, args) {
  const grid = [];
  for (const normalization of normalizations) {
    for (const minScore of thresholds) {
      const config = policyConfig(window, normalization, minScore, args);
      const folds = developmentBlocks.map((frames, index) => {
        const result = evaluate(frames, config, simulation, `tune_${window.name}_${index}`);
        return {
          overlay_pnl: result.overlay_pnl,
          pnl_per_day: result.overlay_pnl / durationDays(frames),
          trades: result.trades,
          tail_losses: result.tail_losses,
        };
      });
      const rates = folds.map((fold) => fold.pnl_per_day);
      grid.push({
        config,
        folds,
        mean_pnl_per_day: round(mean(rates), 8),
        worst_pnl_per_day: round(Math.min(...rates), 8),
        stability_adjusted_rate: round(mean(rates) - standardDeviation(rates), 8),
        development_trades: folds.reduce((sum, fold) => sum + fold.trades, 0),
        development_tail_losses: folds.reduce((sum, fold) => sum + fold.tail_losses, 0),
      });
    }
  }
  grid.sort((a, b) => a.development_tail_losses - b.development_tail_losses
    || b.worst_pnl_per_day - a.worst_pnl_per_day
    || b.stability_adjusted_rate - a.stability_adjusted_rate
    || b.mean_pnl_per_day - a.mean_pnl_per_day
    || a.config.formula.localeCompare(b.config.formula)
    || (a.config.formula === 'linear' ? a.config.slope - b.config.slope : a.config.exponent - b.config.exponent));
  const selected = grid[0];
  return {
    selected,
    development: summarize(evaluate(developmentFrames, selected.config, simulation, `development_${window.name}`)),
    grid,
  };
}

function candidateCoverage(frames, window) {
  const counts = frames.map((frame) => frame.candidates.filter((candidate) => (
    candidate.dte >= window.minDte && candidate.dte <= window.maxDte
  )).length);
  return {
    candidate_rows: counts.reduce((sum, count) => sum + count, 0),
    frames_with_candidates: counts.filter((count) => count > 0).length,
    coverage_pct: round(counts.filter((count) => count > 0).length / frames.length, 8),
  };
}

function renderMarkdown(report) {
  const money = (value) => `$${Number(value || 0).toFixed(2)}`;
  const pct = (value) => value == null ? 'n/a' : `${(Number(value) * 100).toFixed(2)}%`;
  return [
    '# Sell-call DTE Window Study',
    '',
    `Computed: ${report.computed_at}`,
    `History: ${report.historical_window.from} through ${report.historical_window.to}`,
    `Fixed comparison: power exponent ${report.fixed_config.exponent}, linear slope ${report.fixed_config.linearSlope}, floor ${report.fixed_config.minScore}, reference ${report.fixed_config.referenceDte}.`,
    '',
    '## Fixed-policy comparison',
    '',
    '| DTE | Power full P&L | Linear full P&L | Power holdout | Linear holdout | Power peak | Linear peak |',
    '|---:|---:|---:|---:|---:|---:|---:|',
    ...report.windows.map((row) => `| ${row.window.name} | ${money(row.fixed.full.overlay_pnl)} | ${money(row.linear_fixed.full.overlay_pnl)} | ${money(row.fixed.holdout.overlay_pnl)} | ${money(row.linear_fixed.holdout.overlay_pnl)} | ${pct(row.fixed.rollover.mean_positive_peak_jump_pct)} | ${pct(row.linear_fixed.rollover.mean_positive_peak_jump_pct)} |`),
    '',
    '## Development-tuned comparison',
    '',
    '| DTE | Formula | Strength | Floor | Development P&L | Holdout P&L | Full P&L | Full trades |',
    '|---:|---|---:|---:|---:|---:|---:|---:|',
    ...report.windows.map((row) => `| ${row.window.name} | ${row.tuned.config.formula} | ${row.tuned.config.formula === 'linear' ? row.tuned.config.slope : row.tuned.config.exponent} | ${row.tuned.config.minScore} | ${money(row.tuned.development.overlay_pnl)} | ${money(row.tuned.holdout.overlay_pnl)} | ${money(row.tuned.full.overlay_pnl)} | ${row.tuned.full.trades} |`),
    '',
    'Configuration selection uses only the first 80% in four contiguous blocks. The final 20% is reported after selection. Historical quote replay does not prove fills or future performance.',
    '',
  ].join('\n');
}

function main() {
  const args = parseArgv(process.argv.slice(2));
  const dataDir = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
  const dbPath = args.db || process.env.DB_PATH || path.join(dataDir, 'noop.db');
  if (!fs.existsSync(dbPath)) throw new Error(`database not found: ${dbPath}`);
  const outputPath = args.out || path.join(dataDir, 'sell-call-dte-window-study.json');
  const markdownPath = args.markdown || outputPath.replace(/\.json$/i, '.md');
  const windows = windowList(args.windows);
  const exponents = numberList(args.exponents, DEFAULT_EXPONENTS);
  const linearSlopes = numberList(args['linear-slopes'], DEFAULT_LINEAR_SLOPES);
  const thresholds = numberList(args.thresholds, DEFAULT_THRESHOLDS);
  const fixedConfig = {
    exponent: parseNumber(args['fixed-exponent'], 0.12),
    linearSlope: parseNumber(args['fixed-linear-slope'], parseNumber(args['fixed-exponent'], 0.12) / parseNumber(args['reference-dte'], 8.5)),
    minScore: parseNumber(args['fixed-min-score'], 65),
    referenceDte: parseNumber(args['reference-dte'], 8.5),
  };
  const minLoadDte = Math.min(...windows.map((window) => window.minDte));
  const maxLoadDte = Math.max(...windows.map((window) => window.maxDte));
  const simulation = simulationConfig(args);
  const normalizations = [
    ...exponents.map((exponent) => ({ formula: 'power', exponent })),
    ...linearSlopes.map((slope) => ({ formula: 'linear', slope })),
  ];
  const db = new Database(dbPath, { readonly: true, fileMustExist: true });
  db.pragma('query_only = ON');
  try {
    const data = loadHistoricalFrames(db, {
      days: args.days || 'all',
      from: args.from,
      to: args.to,
      cadenceHours: parseNumber(args['cadence-hours'], 1),
      callDteRange: [minLoadDte, maxLoadDte],
    });
    const split = splitChronologicalFrames(data.frames, { trainFraction: 0.6, validationFraction: 0.2 });
    const developmentFrames = [...split.train, ...split.validation];
    const developmentBlocks = contiguousBlocks(developmentFrames, 4);
    const fullBlocks = contiguousBlocks(data.frames, 5);
    const rows = windows.map((window) => {
      const fixed = policyConfig(window, { formula: 'power', exponent: fixedConfig.exponent }, fixedConfig.minScore, args);
      const linearFixed = policyConfig(window, { formula: 'linear', slope: fixedConfig.linearSlope }, fixedConfig.minScore, args);
      const tuning = tuneWindow(window, developmentFrames, developmentBlocks, normalizations, thresholds, simulation, args);
      const developmentGrid = args['report-grid-results']
        ? tuning.grid.map((gridRow, index) => ({
            ...gridRow,
            development: summarize(evaluate(developmentFrames, gridRow.config, simulation, `grid_development_${window.name}_${index}`)),
            holdout: summarize(evaluate(split.test, gridRow.config, simulation, `grid_holdout_${window.name}_${index}`)),
            full: summarize(evaluate(data.frames, gridRow.config, simulation, `grid_full_${window.name}_${index}`)),
            rollover: analyzeRolloverDiscontinuity(data.frames, gridRow.config),
          }))
        : tuning.grid;
      return {
        window,
        coverage: candidateCoverage(data.frames, window),
        fixed: {
          config: fixed,
          train: summarize(evaluate(split.train, fixed, simulation, `fixed_train_${window.name}`)),
          validation: summarize(evaluate(split.validation, fixed, simulation, `fixed_validation_${window.name}`)),
          development: summarize(evaluate(developmentFrames, fixed, simulation, `fixed_development_${window.name}`)),
          holdout: summarize(evaluate(split.test, fixed, simulation, `fixed_holdout_${window.name}`)),
          full: summarize(evaluate(data.frames, fixed, simulation, `fixed_full_${window.name}`)),
          periods: fullBlocks.map((frames, index) => summarize(evaluate(frames, fixed, simulation, `fixed_period_${window.name}_${index}`))),
          rollover: analyzeRolloverDiscontinuity(data.frames, fixed),
        },
        linear_fixed: {
          config: linearFixed,
          train: summarize(evaluate(split.train, linearFixed, simulation, `linear_fixed_train_${window.name}`)),
          validation: summarize(evaluate(split.validation, linearFixed, simulation, `linear_fixed_validation_${window.name}`)),
          development: summarize(evaluate(developmentFrames, linearFixed, simulation, `linear_fixed_development_${window.name}`)),
          holdout: summarize(evaluate(split.test, linearFixed, simulation, `linear_fixed_holdout_${window.name}`)),
          full: summarize(evaluate(data.frames, linearFixed, simulation, `linear_fixed_full_${window.name}`)),
          periods: fullBlocks.map((frames, index) => summarize(evaluate(frames, linearFixed, simulation, `linear_fixed_period_${window.name}_${index}`))),
          rollover: analyzeRolloverDiscontinuity(data.frames, linearFixed),
        },
        tuned: {
          config: tuning.selected.config,
          selection_metrics: tuning.selected,
          development: tuning.development,
          holdout: summarize(evaluate(split.test, tuning.selected.config, simulation, `tuned_holdout_${window.name}`)),
          full: summarize(evaluate(data.frames, tuning.selected.config, simulation, `tuned_full_${window.name}`)),
          development_grid: developmentGrid,
        },
      };
    });
    const report = {
      schema_version: 1,
      engine: 'sell-call-dte-window-study-v1',
      computed_at: new Date().toISOString(),
      historical_window: data.window,
      frames: data.frames.length,
      loaded_dte_range: [minLoadDte, maxLoadDte],
      fixed_config: fixedConfig,
      tuning_grid: { exponents, linear_slopes: linearSlopes, thresholds, development_blocks: developmentBlocks.length },
      split: {
        development: { from: developmentFrames[0].timestamp, to: developmentFrames.at(-1).timestamp },
        holdout: { from: split.test[0].timestamp, to: split.test.at(-1).timestamp },
      },
      windows: rows,
    };
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    fs.mkdirSync(path.dirname(markdownPath), { recursive: true });
    fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`);
    fs.writeFileSync(markdownPath, `${renderMarkdown(report)}\n`);
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
