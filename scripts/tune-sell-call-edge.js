#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const {
  loadHistoricalFrames,
  parseArgv,
  parseNumber,
  tuneEdgeVariants,
} = require('../research/sell-call-backtest');

function usage() {
  return [
    'Usage: node scripts/tune-sell-call-edge.js [options]',
    '',
    '  --db=/path/noop.db             Read-only database (DB_PATH also supported)',
    '  --days=all|180                 Historical window (default: all)',
    '  --cadence-hours=1              Replay cadence (default: 1)',
    '  --search-count=2500            Deterministic formula variants',
    '  --seed=20260809                Search seed',
    '  --train-fraction=0.60          Earliest development segment',
    '  --validation-fraction=0.20     Middle selection segment; remainder is holdout',
    '  --fee-bps=0                    Entry and buyback fees',
    '  --out=data/sell-call-edge-tuning.json',
    '  --markdown=data/sell-call-edge-tuning.md',
  ].join('\n');
}

function percentage(value) {
  return value == null ? 'n/a' : `${(Number(value) * 100).toFixed(3)}%`;
}

function money(value) {
  return value == null ? 'n/a' : `$${Number(value).toFixed(2)}`;
}

function renderResultTable(rows) {
  const lines = [
    '| Formula | Overlay P&L | Overlay return | Trades | Wins | Tail losses | Max drawdown |',
    '|---|---:|---:|---:|---:|---:|---:|',
  ];
  for (const row of rows) {
    lines.push(`| ${row.name} | ${money(row.result.overlay_pnl)} | ${percentage(row.result.overlay_return)} | ${row.result.trades} | ${row.result.wins} | ${row.result.tail_losses} | ${percentage(row.result.max_drawdown)} |`);
  }
  return lines;
}

function renderMarkdown(report) {
  const selected = report.selected_variant;
  const lines = [
    '# Sell-Call Edge Tuning',
    '',
    `Computed: ${report.computed_at}`,
    `Variants searched: ${report.searched_variants}; seed: ${report.seed}`,
    `Selection: ${report.selection_rule}`,
    '',
    '## Chronological split',
    '',
    '| Segment | From | To | Frames |',
    '|---|---|---|---:|',
    ...Object.entries(report.split).map(([name, split]) => `| ${name} | ${split.from} | ${split.to} | ${split.frames} |`),
    '',
    '## Preselected formula',
    '',
    `Variant: ${selected.id}; development objective: ${selected.objective}`,
    '',
    `Minimum bid: ${selected.config.min_bid}; minimum edge: ${selected.config.min_edge}`,
    '',
    '| Factor | Weight |',
    '|---|---:|',
    ...Object.entries(selected.config.weights).map(([name, weight]) => `| ${name} | ${weight} |`),
    '',
    '## Untouched holdout',
    '',
    ...renderResultTable(report.holdout),
    '',
    `Preselected tuned edge minus raw score: ${money(report.conclusion.tuned_minus_raw_holdout_pnl)}.`,
    '',
    `Beat raw score under the declared holdout rule: ${report.conclusion.tuned_beats_raw_on_holdout ? 'yes' : 'no'}.`,
    '',
    '## Full history (descriptive only)',
    '',
    ...renderResultTable(report.full_history),
    '',
    '## Development leaderboard',
    '',
    '| Rank | Variant | Objective | Train P&L | Validation P&L | Train trades | Validation trades |',
    '|---:|---|---:|---:|---:|---:|---:|',
    ...report.development_leaderboard.map((item, index) => `| ${index + 1} | ${item.id} | ${item.objective} | ${money(item.train.overlay_pnl)} | ${money(item.validation.overlay_pnl)} | ${item.train.trades} | ${item.validation.trades} |`),
    '',
    `Warning: ${report.conclusion.warning}`,
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
  const outputPath = args.out || path.join(dataDir, 'sell-call-edge-tuning.json');
  const markdownPath = args.markdown || outputPath.replace(/\.json$/i, '.md');
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
    console.log(`Searching ${parseNumber(args['search-count'], 2500)} variants over ${data.frames.length} hourly frames...`);
    const report = tuneEdgeVariants(data.frames, {
      searchCount: parseNumber(args['search-count'], 2500),
      seed: parseNumber(args.seed, 20260809),
      trainFraction: parseNumber(args['train-fraction'], 0.60),
      validationFraction: parseNumber(args['validation-fraction'], 0.20),
      leaderboardSize: parseNumber(args['leaderboard-size'], 20),
      simulationConfig,
    });
    report.historical_window = data.window;
    report.cadence_hours = data.cadence_hours;
    report.coverage = data.coverage;
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
