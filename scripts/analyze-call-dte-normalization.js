#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const {
  analyzeRolloverDiscontinuity,
  loadHistoricalFrames,
  makeDteNormalizedRawPolicy,
  parseArgv,
  parseNumber,
  round,
  runBacktest,
  splitChronologicalFrames,
} = require('../research/sell-call-backtest');

function usage() {
  return [
    'Usage: node scripts/analyze-call-dte-normalization.js [options]',
    '',
    '  --db=/path/noop.db             Read-only database (DB_PATH also supported)',
    '  --days=all|180                 Historical window (default: all)',
    '  --reference-dte=8.5            Score reference DTE',
    '  --exponents=0,0.1,...,0.5      Time normalization strengths',
    '  --thresholds=60,65,70          Normalized score entry floors',
    '  --out=data/call-dte-normalization.json',
    '  --markdown=data/call-dte-normalization.md',
  ].join('\n');
}

function numberList(value, fallback) {
  const values = String(value || fallback).split(',').map(Number).filter(Number.isFinite);
  if (values.length === 0) throw new Error('normalization grid cannot be empty');
  return values;
}

function resultSummary(result) {
  return {
    overlay_pnl: result.overlay_pnl,
    overlay_return: round(result.overlay_pnl / result.starting_nav, 8),
    trades: result.trades,
    wins: result.wins,
    tail_losses: result.tail_losses,
    max_drawdown: result.max_drawdown,
    average_holding_hours: result.average_holding_hours,
  };
}

function evaluate(frames, config, simulationConfig, name) {
  return resultSummary(runBacktest(frames, makeDteNormalizedRawPolicy(config, name), simulationConfig));
}

function spikeScore(row) {
  const train = row.train_smoothness.mean_positive_peak_jump_pct ?? Infinity;
  const validation = row.validation_smoothness.mean_positive_peak_jump_pct ?? Infinity;
  return (train + validation) / 2;
}

function money(value) {
  return `$${Number(value || 0).toFixed(2)}`;
}

function pct(value) {
  return value == null ? 'n/a' : `${(Number(value) * 100).toFixed(2)}%`;
}

function renderMarkdown(report) {
  const lines = [
    '# Call DTE Normalization Study',
    '',
    `Computed: ${report.computed_at}`,
    `Formula: normalized_score = raw_score × (${report.reference_dte} / DTE) ^ exponent`,
    `Selection rule: ${report.selection_rule}`,
    '',
    '## Selected normalization',
    '',
    `Exponent: ${report.selected.config.exponent}; reference DTE: ${report.selected.config.referenceDte}; minimum score: ${report.selected.config.minScore}.`,
    '',
    '| Metric | Raw | Normalized | Change |',
    '|---|---:|---:|---:|',
    `| Development rollover peak | ${pct(report.development_comparison.raw_smoothness.mean_positive_peak_jump_pct)} | ${pct(report.development_comparison.normalized_smoothness.mean_positive_peak_jump_pct)} | ${pct(report.development_comparison.rollover_peak_change_pct)} |`,
    `| Holdout overlay P&L | ${money(report.holdout.raw.overlay_pnl)} | ${money(report.holdout.normalized.overlay_pnl)} | ${money(report.holdout.normalized.overlay_pnl - report.holdout.raw.overlay_pnl)} |`,
    `| Holdout trades | ${report.holdout.raw.trades} | ${report.holdout.normalized.trades} | ${report.holdout.normalized.trades - report.holdout.raw.trades} |`,
    `| Full-history overlay P&L | ${money(report.full_history.raw.overlay_pnl)} | ${money(report.full_history.normalized.overlay_pnl)} | ${money(report.full_history.normalized.overlay_pnl - report.full_history.raw.overlay_pnl)} |`,
    `| Full-history rollover peak | ${pct(report.full_history.raw_smoothness.mean_positive_peak_jump_pct)} | ${pct(report.full_history.normalized_smoothness.mean_positive_peak_jump_pct)} | ${pct(report.full_history.rollover_peak_change_pct)} |`,
    '',
    '## Five-period robustness',
    '',
    '| Period | Raw P&L | Normalized P&L | Difference | Raw trades | Normalized trades |',
    '|---:|---:|---:|---:|---:|---:|',
    ...report.periods.map((period, index) => `| ${index + 1} | ${money(period.raw.overlay_pnl)} | ${money(period.normalized.overlay_pnl)} | ${money(period.normalized.overlay_pnl - period.raw.overlay_pnl)} | ${period.raw.trades} | ${period.normalized.trades} |`),
    '',
    '## Development grid',
    '',
    '| Exponent | Floor | Train P&L | Validation P&L | Holdout P&L | Full P&L | Mean rollover peak | Eligible |',
    '|---:|---:|---:|---:|---:|---:|---:|---|',
    ...report.grid.map((row) => `| ${row.config.exponent} | ${row.config.minScore} | ${money(row.train.overlay_pnl)} | ${money(row.validation.overlay_pnl)} | ${money(row.holdout.overlay_pnl)} | ${money(row.full_history.overlay_pnl)} | ${pct(row.development_spike_score)} | ${row.eligible ? 'yes' : 'no'} |`),
    '',
    'This is an offline research result. Historical top-of-book quotes cannot prove maker fills, and a normalization should remain shadowed before production use.',
    '',
  ];
  return lines.join('\n');
}

function main() {
  const args = parseArgv(process.argv.slice(2));
  if (args.help || args.h) {
    console.log(usage());
    return;
  }
  const dataDir = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
  const dbPath = args.db || process.env.DB_PATH || path.join(dataDir, 'noop.db');
  if (!fs.existsSync(dbPath)) throw new Error(`database not found: ${dbPath}`);
  const outputPath = args.out || path.join(dataDir, 'call-dte-normalization.json');
  const markdownPath = args.markdown || outputPath.replace(/\.json$/i, '.md');
  const referenceDte = parseNumber(args['reference-dte'], 8.5);
  const exponents = numberList(args.exponents, '0,0.1,0.15,0.2,0.25,0.3,0.35,0.4,0.5');
  const thresholds = numberList(args.thresholds, '60,65,70');
  const db = new Database(dbPath, { readonly: true, fileMustExist: true });
  db.pragma('query_only = ON');
  try {
    const data = loadHistoricalFrames(db, {
      days: args.days || 'all',
      from: args.from,
      to: args.to,
      cadenceHours: parseNumber(args['cadence-hours'], 1),
    });
    const splits = splitChronologicalFrames(data.frames, { trainFraction: 0.6, validationFraction: 0.2 });
    const simulationConfig = {
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
    const configs = exponents.flatMap((exponent) => thresholds.map((minScore) => ({
      exponent,
      minScore,
      minBid: 4,
      referenceDte,
    })));
    const rawConfig = { exponent: 0, minScore: 65, minBid: 4, referenceDte };
    const rawTrain = evaluate(splits.train, rawConfig, simulationConfig, 'raw_train');
    const rawValidation = evaluate(splits.validation, rawConfig, simulationConfig, 'raw_validation');
    const grid = configs.map((config) => {
      const train = evaluate(splits.train, config, simulationConfig, 'normalized_train');
      const validation = evaluate(splits.validation, config, simulationConfig, 'normalized_validation');
      const holdout = evaluate(splits.test, config, simulationConfig, 'normalized_grid_holdout');
      const fullHistory = evaluate(data.frames, config, simulationConfig, 'normalized_grid_full');
      const trainSmoothness = analyzeRolloverDiscontinuity(splits.train, config);
      const validationSmoothness = analyzeRolloverDiscontinuity(splits.validation, config);
      const eligible = config.exponent > 0
        && train.overlay_pnl >= rawTrain.overlay_pnl * 0.9
        && validation.overlay_pnl >= rawValidation.overlay_pnl * 0.9
        && train.tail_losses <= rawTrain.tail_losses
        && validation.tail_losses <= rawValidation.tail_losses;
      return {
        config,
        train,
        validation,
        holdout,
        full_history: fullHistory,
        train_smoothness: trainSmoothness,
        validation_smoothness: validationSmoothness,
        development_spike_score: spikeScore({ train_smoothness: trainSmoothness, validation_smoothness: validationSmoothness }),
        eligible,
      };
    }).sort((a, b) => Number(b.eligible) - Number(a.eligible)
      || a.development_spike_score - b.development_spike_score
      || (b.train.overlay_pnl + b.validation.overlay_pnl) - (a.train.overlay_pnl + a.validation.overlay_pnl));
    const selected = grid.find((row) => row.eligible);
    if (!selected) throw new Error('no DTE normalization retained at least 90% of raw P&L in both development folds');

    const rawDevelopmentSmoothness = analyzeRolloverDiscontinuity([...splits.train, ...splits.validation], rawConfig);
    const normalizedDevelopmentSmoothness = analyzeRolloverDiscontinuity([...splits.train, ...splits.validation], selected.config);
    const rawHoldout = evaluate(splits.test, rawConfig, simulationConfig, 'raw_holdout');
    const normalizedHoldout = evaluate(splits.test, selected.config, simulationConfig, 'normalized_holdout');
    const rawFull = evaluate(data.frames, rawConfig, simulationConfig, 'raw_full');
    const normalizedFull = evaluate(data.frames, selected.config, simulationConfig, 'normalized_full');
    const rawFullSmoothness = analyzeRolloverDiscontinuity(data.frames, rawConfig);
    const normalizedFullSmoothness = analyzeRolloverDiscontinuity(data.frames, selected.config);
    const segmentSize = Math.floor(data.frames.length / 5);
    const periods = Array.from({ length: 5 }, (_, index) => {
      const start = index * segmentSize;
      const end = index === 4 ? data.frames.length : (index + 1) * segmentSize;
      const frames = data.frames.slice(start, end);
      return {
        from: frames[0].timestamp,
        to: frames.at(-1).timestamp,
        raw: evaluate(frames, rawConfig, simulationConfig, `raw_period_${index + 1}`),
        normalized: evaluate(frames, selected.config, simulationConfig, `normalized_period_${index + 1}`),
      };
    });
    const report = {
      schema_version: 1,
      engine: 'call-dte-normalization-study-v1',
      computed_at: new Date().toISOString(),
      historical_window: data.window,
      frames: data.frames.length,
      reference_dte: referenceDte,
      formula: 'normalized_score = raw_score * (reference_dte / dte) ^ exponent',
      selection_rule: 'Use train + validation only; retain at least 90% of raw overlay P&L in each development fold, then minimize rollover peak discontinuity. Open the final 20% only after selection.',
      split: {
        train: { from: splits.train[0].timestamp, to: splits.train.at(-1).timestamp, frames: splits.train.length },
        validation: { from: splits.validation[0].timestamp, to: splits.validation.at(-1).timestamp, frames: splits.validation.length },
        holdout: { from: splits.test[0].timestamp, to: splits.test.at(-1).timestamp, frames: splits.test.length },
      },
      raw_config: rawConfig,
      selected,
      development_comparison: {
        raw_smoothness: rawDevelopmentSmoothness,
        normalized_smoothness: normalizedDevelopmentSmoothness,
        rollover_peak_change_pct: round(
          normalizedDevelopmentSmoothness.mean_positive_peak_jump_pct / rawDevelopmentSmoothness.mean_positive_peak_jump_pct - 1,
          8,
        ),
      },
      holdout: { raw: rawHoldout, normalized: normalizedHoldout },
      full_history: {
        raw: rawFull,
        normalized: normalizedFull,
        raw_smoothness: rawFullSmoothness,
        normalized_smoothness: normalizedFullSmoothness,
        rollover_peak_change_pct: round(
          normalizedFullSmoothness.mean_positive_peak_jump_pct / rawFullSmoothness.mean_positive_peak_jump_pct - 1,
          8,
        ),
      },
      periods,
      grid,
    };
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    fs.mkdirSync(path.dirname(markdownPath), { recursive: true });
    fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`);
    fs.writeFileSync(markdownPath, `${renderMarkdown(report)}\n`);
    console.log(`Selected exponent ${selected.config.exponent}, floor ${selected.config.minScore}`);
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
