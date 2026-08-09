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
} = require('../research/sell-call-backtest');

const DEFAULT_EXPONENTS = [0, 0.055, 0.06, 0.1, 0.105, 0.11, 0.115, 0.12, 0.1219, 0.122, 0.125, 0.13, 0.15];

function parseList(value, fallback) {
  const values = String(value || fallback.join(',')).split(',').map(Number).filter(Number.isFinite);
  if (values.length === 0) throw new Error('exponent list cannot be empty');
  return [...new Set(values)].sort((a, b) => a - b);
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

function summarize(result) {
  return {
    overlay_pnl: result.overlay_pnl,
    trades: result.trades,
    wins: result.wins,
    tail_losses: result.tail_losses,
    max_drawdown: result.max_drawdown,
  };
}

function evaluate(frames, exponent, config, policyConfig) {
  return runBacktest(frames, makeDteNormalizedRawPolicy({ ...policyConfig, exponent }, `alpha_${exponent}`), config);
}

function fingerprint(trade) {
  return `${trade.opened_at}|${trade.instrument_name}`;
}

function main() {
  const args = parseArgv(process.argv.slice(2));
  const dataDir = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
  const dbPath = args.db || process.env.DB_PATH || path.join(dataDir, 'noop.db');
  if (!fs.existsSync(dbPath)) throw new Error(`database not found: ${dbPath}`);
  const exponents = parseList(args.exponents, DEFAULT_EXPONENTS);
  const folds = Math.max(2, Math.floor(parseNumber(args.folds, 5)));
  const policyConfig = {
    minBid: parseNumber(args['min-bid'], 4),
    minScore: parseNumber(args['min-score'], 65),
    referenceDte: parseNumber(args['reference-dte'], 8.5),
  };
  const config = simulationConfig(args);
  const db = new Database(dbPath, { readonly: true, fileMustExist: true });
  db.pragma('query_only = ON');
  try {
    const data = loadHistoricalFrames(db, {
      days: args.days || 'all',
      from: args.from,
      to: args.to,
      cadenceHours: parseNumber(args['cadence-hours'], 1),
    });
    const foldSize = Math.floor(data.frames.length / folds);
    const periods = Array.from({ length: folds }, (_, index) => {
      const start = index * foldSize;
      const end = index === folds - 1 ? data.frames.length : (index + 1) * foldSize;
      return data.frames.slice(start, end);
    });
    const rawPeriods = periods.map((frames) => evaluate(frames, 0, config, policyConfig));
    const rawSmoothness = analyzeRolloverDiscontinuity(data.frames, { ...policyConfig, exponent: 0 });
    const fullResults = new Map();
    const rows = exponents.map((exponent) => {
      const full = evaluate(data.frames, exponent, config, policyConfig);
      fullResults.set(exponent, full);
      const periodResults = periods.map((frames, index) => {
        const result = evaluate(frames, exponent, config, policyConfig);
        return {
          from: frames[0].timestamp,
          to: frames.at(-1).timestamp,
          ...summarize(result),
          pnl_vs_raw: round(result.overlay_pnl - rawPeriods[index].overlay_pnl, 6),
        };
      });
      const smoothness = analyzeRolloverDiscontinuity(data.frames, { ...policyConfig, exponent });
      return {
        exponent,
        full: summarize(full),
        rollover_peak: smoothness.mean_positive_peak_jump_pct,
        rollover_reduction: round(1 - smoothness.mean_positive_peak_jump_pct / rawSmoothness.mean_positive_peak_jump_pct, 8),
        periods_at_or_above_raw: periodResults.filter((period) => period.pnl_vs_raw >= 0).length,
        worst_period_vs_raw: Math.min(...periodResults.map((period) => period.pnl_vs_raw)),
        periods: periodResults,
        trade_fingerprints: full.trade_log.map(fingerprint),
      };
    });
    const byExponent = new Map(rows.map((row) => [row.exponent, row]));
    const candidate = byExponent.get(parseNumber(args.candidate, 0.12));
    const cliff = byExponent.get(parseNumber(args.cliff, 0.122));
    const disappearedAtCliff = candidate && cliff
      ? candidate.trade_fingerprints.filter((trade) => !cliff.trade_fingerprints.includes(trade))
      : [];
    const candidateResult = candidate ? fullResults.get(candidate.exponent) : null;
    const disappearedTradeDetails = candidateResult
      ? candidateResult.trade_log.filter((trade) => disappearedAtCliff.includes(fingerprint(trade)))
      : [];
    const report = {
      computed_at: new Date().toISOString(),
      database: dbPath,
      historical_window: data.window,
      frames: data.frames.length,
      folds,
      policy: policyConfig,
      raw_rollover_peak: rawSmoothness.mean_positive_peak_jump_pct,
      rows,
      disappeared_at_cliff: disappearedAtCliff,
      disappeared_trade_details: disappearedTradeDetails,
    };
    console.log(JSON.stringify(report, null, 2));
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
