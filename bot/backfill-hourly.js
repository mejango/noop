#!/usr/bin/env node
/**
 * Atomically rebuild all four hourly tables from retained raw observations.
 * Back up the database first: buckets without retained raw data are removed.
 * The bot and this command use exactly the same aggregate definitions.
 *
 * node bot/backfill-hourly.js --db /path/to/noop.db
 * DATA_DIR=/path/to/data node bot/backfill-hourly.js
 * node bot/backfill-hourly.js --db /path/to/noop.db --from 2026-09-01T00:00:00Z --to 2026-09-02T00:00:00Z
 * Ranges cover complete UTC hours, from inclusive and to exclusive.
 */
const path = require('path');

function parseArguments(args, env = process.env) {
  const options = {};
  for (let index = 0; index < args.length; index++) {
    const argument = args[index];
    if (argument === '--help' || argument === '-h') return { help: true };
    const match = /^(--db|--from|--to|--busy-timeout-ms)(?:=(.*))?$/.exec(argument);
    if (!match) throw new Error(`Unknown argument: ${argument}`);
    const key = match[1].slice(2);
    const value = match[2] === undefined ? args[++index] : match[2];
    if (!value || value.startsWith('--')) throw new Error(`Missing value for --${key}`);
    if (options[key] !== undefined) throw new Error(`Duplicate argument: --${key}`);
    options[key] = value;
  }
  if (!options.db) {
    if (!env.DATA_DIR) throw new Error('An explicit --db path or DATA_DIR is required');
    options.db = path.join(env.DATA_DIR, 'noop.db');
  }
  if (options['busy-timeout-ms'] !== undefined) {
    const value = options['busy-timeout-ms'];
    if (!/^\d+$/.test(value) || !Number.isSafeInteger(Number(value)) || Number(value) > 5000) {
      throw new Error('--busy-timeout-ms must be an integer between 0 and 5000');
    }
    options['busy-timeout-ms'] = Number(value);
  }
  options.db = path.resolve(options.db);
  return options;
}

function main(args = process.argv.slice(2)) {
  const options = parseArguments(args);
  if (options.help) {
    console.log('Usage: node bot/backfill-hourly.js --db /path/to/noop.db [--from UTC_HOUR] [--to UTC_HOUR] [--busy-timeout-ms 250]');
    console.log('DATA_DIR may supply the database directory instead of --db. The range is [from, to).');
    console.log('Rebuild replaces the selected range in one transaction. For an active bot, process one CLOSED hour per invocation; retry SQLITE_BUSY later.');
    return;
  }
  const Database = require('better-sqlite3');
  const { createHourlyRollups } = require('./hourly-rollups');
  const db = new Database(options.db, { fileMustExist: true });
  try {
    db.pragma(`busy_timeout = ${options['busy-timeout-ms'] ?? 250}`);
    console.log(`Rebuilding hourly aggregates: ${options.db}`);
    const result = createHourlyRollups(db).rebuild({ from: options.from, to: options.to });
    console.log(JSON.stringify(result, null, 2));
  } finally {
    db.close();
  }
}

if (require.main === module) {
  try { main(); } catch (error) {
    console.error(`Hourly rebuild failed: ${error.message}`);
    process.exitCode = 1;
  }
}

module.exports = { parseArguments, main };
