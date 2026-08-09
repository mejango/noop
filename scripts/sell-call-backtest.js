#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const {
  buildLabeledExamples,
  buildReport,
  loadHistoricalFrames,
  makeCurrentEdgePolicy,
  makeLearnedPolicy,
  makeNoCallPolicy,
  makeRawScorePolicy,
  parseArgv,
  parseNumber,
  renderMarkdown,
  runBacktest,
} = require('../research/sell-call-backtest');

function usage() {
  return [
    'Usage: node scripts/sell-call-backtest.js [options]',
    '',
    'Data:',
    '  --db=/path/noop.db             Read-only historical database (DB_PATH is also supported)',
    '  --days=all|180                 Window ending at the database latest timestamp (default: all)',
    '  --from=ISO --to=ISO            Explicit historical window',
    '  --cadence-hours=1              One representative option-chain frame per interval',
    '  --max-frames=10000             Optional frame cap for smoke tests',
    '',
    'Simulation:',
    '  --policies=no_call,raw_score,current_edge,learned_walk_forward',
    '  --starting-eth=5 --starting-cash=0',
    '  --execution=bid_ask|midpoint|mark',
    '  --fee-bps=0 --settlement-fee-bps=0',
    '  --exposure-cap=0.45 --margin-rate=0.15 --margin-budget-pct=0.45',
    '  --profit-capture-pct=0.80 --stop-loss-multiple=3 --max-hold-hours=168',
    '',
    'Learning:',
    '  --label-horizon-hours=72 --quote-window-hours=6 --top-per-frame=8',
    '  --min-train-samples=500 --min-train-frames=120',
    '  --max-train-samples=20000',
    '  --training-window-days=180 --retrain-hours=168 --embargo-hours=6',
    '  --min-expected-capture=0 --max-tail-probability=0.20 --tail-penalty=0.5',
    '',
    'Output:',
    '  --out=data/sell-call-backtest-report.json',
    '  --markdown=data/sell-call-backtest-report.md',
    '  --models=data/sell-call-model-artifacts.json',
  ].join('\n');
}

function selectedPolicies(raw) {
  const valid = new Set(['no_call', 'raw_score', 'current_edge', 'learned_walk_forward']);
  const values = String(raw || 'no_call,raw_score,current_edge,learned_walk_forward')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  const invalid = values.filter((value) => !valid.has(value));
  if (invalid.length) throw new Error(`unknown policies: ${invalid.join(', ')}`);
  return values;
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
  const outputPath = args.out || path.join(dataDir, 'sell-call-backtest-report.json');
  const markdownPath = args.markdown || outputPath.replace(/\.json$/i, '.md');
  const modelsPath = args.models || path.join(path.dirname(outputPath), 'sell-call-model-artifacts.json');
  const db = new Database(dbPath, { readonly: true, fileMustExist: true });
  db.pragma('query_only = ON');
  db.pragma('busy_timeout = 5000');

  try {
    const data = loadHistoricalFrames(db, {
      days: args.days || 'all',
      from: args.from,
      to: args.to,
      cadenceHours: parseNumber(args['cadence-hours'], 1),
      maxFrames: parseNumber(args['max-frames'], null),
    });
    const labelOptions = {
      horizonHours: parseNumber(args['label-horizon-hours'], 72),
      quoteWindowHours: parseNumber(args['quote-window-hours'], 6),
      topPerFrame: parseNumber(args['top-per-frame'], 8),
    };
    const examples = buildLabeledExamples(data.frames, labelOptions);
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
      stopLossMultiple: parseNumber(args['stop-loss-multiple'], null),
      maxHoldHours: parseNumber(args['max-hold-hours'], null),
      entryCooldownHours: parseNumber(args['entry-cooldown-hours'], 0),
      maxOpenPositions: parseNumber(args['max-open-positions'], 1),
      maxContracts: parseNumber(args['max-contracts'], null),
      amountStep: parseNumber(args['amount-step'], 0.01),
      useQuotedDepth: args['ignore-depth'] !== true,
    };
    const factories = {
      no_call: () => makeNoCallPolicy(),
      raw_score: () => makeRawScorePolicy({
        minBid: parseNumber(args['min-bid'], 4),
        minRawScore: parseNumber(args['min-raw-score'], 65),
      }),
      current_edge: () => makeCurrentEdgePolicy({
        minBid: parseNumber(args['min-bid'], 4),
        minEdge: parseNumber(args['min-edge'], 80),
      }),
      learned_walk_forward: () => makeLearnedPolicy(examples, {
        minBid: parseNumber(args['min-bid'], 4),
        minExpectedCapture: parseNumber(args['min-expected-capture'], 0),
        maxTailProbability: parseNumber(args['max-tail-probability'], 0.20),
        tailPenalty: parseNumber(args['tail-penalty'], 0.5),
        minSamples: parseNumber(args['min-train-samples'], 500),
        minIndependentFrames: parseNumber(args['min-train-frames'], 120),
        maxTrainingSamples: parseNumber(args['max-train-samples'], 20000),
        trainingWindowDays: parseNumber(args['training-window-days'], 180),
        retrainHours: parseNumber(args['retrain-hours'], 168),
        embargoHours: parseNumber(args['embargo-hours'], 6),
        captureLambda: parseNumber(args['capture-lambda'], 5),
        tailLambda: parseNumber(args['tail-lambda'], 1),
      }),
    };
    const results = selectedPolicies(args.policies).map((name) => {
      console.log(`Backtesting ${name} over ${data.frames.length} frames...`);
      return runBacktest(data.frames, factories[name](), simulationConfig);
    });
    const report = buildReport({
      data,
      examples,
      results,
      options: { labels: labelOptions, simulation: simulationConfig },
    });
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    fs.mkdirSync(path.dirname(markdownPath), { recursive: true });
    fs.mkdirSync(path.dirname(modelsPath), { recursive: true });
    fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`);
    fs.writeFileSync(markdownPath, renderMarkdown(report));
    const modelArtifacts = results.flatMap((result) => result.model_artifacts || []);
    fs.writeFileSync(modelsPath, `${JSON.stringify({
      schema_version: 1,
      generated_at: report.computed_at,
      historical_window: report.historical_window,
      artifacts: modelArtifacts,
    }, null, 2)}\n`);
    console.log(`JSON: ${outputPath}`);
    console.log(`Markdown: ${markdownPath}`);
    console.log(`Models: ${modelsPath}`);
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
