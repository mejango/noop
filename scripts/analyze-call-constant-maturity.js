#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const {
  analyzeConstantMaturityRollover,
  analyzeSimpleScoreRollover,
  constantMaturitySeries,
  loadHistoricalFrames,
  parseArgv,
  parseNumber,
} = require('../research/sell-call-backtest');

function main() {
  const args = parseArgv(process.argv.slice(2));
  const dataDir = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
  const dbPath = args.db || process.env.DB_PATH || path.join(dataDir, 'noop.db');
  if (!fs.existsSync(dbPath)) throw new Error(`database not found: ${dbPath}`);
  const outputPath = args.out || path.join(dataDir, 'call-constant-maturity-study.json');
  const targetDte = parseNumber(args['target-dte'], 8.5);
  const loadRange = [parseNumber(args['load-min-dte'], 1), parseNumber(args['load-max-dte'], 21)];
  const common = { minBid: parseNumber(args['min-bid'], 4), minDte: 5, maxDte: 12 };
  const constant = { ...common, targetDte, minDte: loadRange[0], maxDte: loadRange[1], family: 'production', exponent: 0 };
  const db = new Database(dbPath, { readonly: true, fileMustExist: true });
  db.pragma('query_only = ON');
  try {
    const data = loadHistoricalFrames(db, {
      days: args.days || 'all',
      from: args.from,
      to: args.to,
      cadenceHours: parseNumber(args['cadence-hours'], 1),
      callDteRange: loadRange,
    });
    const series = constantMaturitySeries(data.frames, constant);
    const report = {
      schema_version: 1,
      engine: 'call-constant-maturity-study-v1',
      computed_at: new Date().toISOString(),
      historical_window: data.window,
      frames: data.frames.length,
      target_dte: targetDte,
      load_dte_range: loadRange,
      comparison: {
        raw_top_5_12: analyzeSimpleScoreRollover(data.frames, { ...common, family: 'production', exponent: 0 }),
        production_power_5_12: analyzeSimpleScoreRollover(data.frames, { ...common, family: 'production', exponent: 0.12 }),
        constant_maturity_raw: analyzeConstantMaturityRollover(data.frames, constant),
      },
      sample: series.slice(-24),
      limitations: [
        'The synthetic series is an analytics benchmark, not a directly tradable option.',
        'Interpolation requires qualifying expiries on both sides of the target tenor.',
      ],
    };
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`);
    console.log(`JSON: ${outputPath}`);
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
