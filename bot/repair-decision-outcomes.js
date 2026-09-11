#!/usr/bin/env node
// Reconsider incomplete labels after importing historical observations.
// Usage: node bot/repair-decision-outcomes.js --db /path/to/copy.db [--now ISO]
// DATA_DIR=/path/to/copy is also accepted. No implicit production target.
const path = require('node:path');
const fs = require('node:fs');
const args = process.argv.slice(2);
const readArg = (flag) => {
  const index = args.indexOf(flag);
  if (index === -1) return undefined;
  if (!args[index + 1] || args[index + 1].startsWith('--')) throw new Error(`Missing ${flag} value`);
  return args[index + 1];
};
if (args.includes('--help')) {
  console.log('Usage: node bot/repair-decision-outcomes.js --db /path/to/copy.db [--now ISO]');
  process.exit(0);
}
const target = readArg('--db') || process.env.NOOP_DB_PATH || (process.env.DATA_DIR && path.join(process.env.DATA_DIR, 'noop.db'));
if (!target) throw new Error('Explicit --db or DATA_DIR is required');
if (!fs.existsSync(target)) throw new Error(`Database does not exist: ${target}`);
process.env.NOOP_DB_PATH = path.resolve(target);
process.env.DATA_DIR = path.dirname(process.env.NOOP_DB_PATH);
const db = require('./db');
try {
  console.log(JSON.stringify({ database: process.env.NOOP_DB_PATH, ...db.repairDecisionOutcomes({ now: readArg('--now') }), completeness: db.getDecisionOutcomeCompleteness() }, null, 2));
} finally {
  db.close();
}
