#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const {
  buildProfitCorrelationReport,
  renderMarkdownReport,
  DEFAULT_HORIZONS_HOURS,
} = require('../bot/profit-correlation-engine');

function parseArgs(argv) {
  const args = {};
  for (const raw of argv) {
    if (!raw.startsWith('--')) continue;
    const eq = raw.indexOf('=');
    if (eq === -1) {
      args[raw.slice(2)] = true;
    } else {
      args[raw.slice(2, eq)] = raw.slice(eq + 1);
    }
  }
  return args;
}

function parseNumberList(value, fallback) {
  if (!value) return fallback;
  const parsed = String(value)
    .split(',')
    .map((item) => Number(item.trim()))
    .filter((item) => Number.isFinite(item) && item > 0);
  return parsed.length > 0 ? parsed : fallback;
}

function parseActionList(value) {
  if (!value || value === 'all') return ['sell_call', 'buy_put'];
  return String(value)
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function usage() {
  return [
    'Usage: node scripts/profit-correlation-report.js [options]',
    '',
    'Options:',
    '  --db=/path/noop.db              SQLite database path. Defaults to DB_PATH or DATA_DIR/noop.db.',
    '  --out=/path/report.json         JSON output path. Defaults to data/profit-correlation-report.json.',
    '  --markdown=/path/report.md      Markdown output path. Defaults to JSON path with .md extension.',
    '  --days=all|90|180              History window. Defaults to all.',
    '  --actions=all|sell_call,buy_put Actions to analyze. Defaults to all.',
    '  --sample=top-hour|all-candidates Candidate sampling mode. Defaults to top-hour.',
    '  --top-per-hour=8               Candidates per action per hour in top-hour mode.',
    '  --max-samples=250000           Optional max candidates per action.',
    '  --min-samples=30               Minimum samples for feature/interactions.',
    '  --horizons=1,6,24,72           Outcome horizons in hours.',
    '',
    'Examples:',
    '  npm run research:correlate',
    '  DB_PATH=/private/tmp/noop-research.db npm run research:correlate -- --days=all --sample=top-hour',
    '  node scripts/profit-correlation-report.js --db=/private/tmp/noop-research.db --sample=all-candidates --max-samples=100000',
  ].join('\n');
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help || args.h) {
    console.log(usage());
    return;
  }

  const dataDir = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
  const dbPath = args.db || process.env.DB_PATH || path.join(dataDir, 'noop.db');
  const outPath = args.out || path.join(dataDir, 'profit-correlation-report.json');
  const markdownPath = args.markdown || outPath.replace(/\.json$/i, '.md');
  const sampleMode = args.sample || 'top-hour';

  if (!fs.existsSync(dbPath)) {
    throw new Error(`Database not found: ${dbPath}`);
  }
  if (!['top-hour', 'all-candidates'].includes(sampleMode)) {
    throw new Error(`Invalid --sample=${sampleMode}; expected top-hour or all-candidates`);
  }

  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.mkdirSync(path.dirname(markdownPath), { recursive: true });

  const db = new Database(dbPath, { readonly: true, fileMustExist: true });
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');

  const started = Date.now();
  console.log(`Research correlation engine`);
  console.log(`DB: ${dbPath}`);
  console.log(`Window: ${args.days || 'all'} | sample=${sampleMode}`);

  const report = buildProfitCorrelationReport(db, {
    days: args.days || 'all',
    actions: parseActionList(args.actions),
    sampleMode,
    topPerHour: args['top-per-hour'] ? Number(args['top-per-hour']) : undefined,
    maxSamples: args['max-samples'] ? Number(args['max-samples']) : null,
    minSamples: args['min-samples'] ? Number(args['min-samples']) : undefined,
    horizonsHours: parseNumberList(args.horizons, DEFAULT_HORIZONS_HOURS),
  });

  fs.writeFileSync(outPath, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(markdownPath, renderMarkdownReport(report));
  db.close();

  console.log(`JSON: ${outPath}`);
  console.log(`Markdown: ${markdownPath}`);
  console.log(`Done in ${((Date.now() - started) / 1000).toFixed(1)}s`);
}

try {
  main();
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
}
