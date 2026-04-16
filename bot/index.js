/**
 * Bot Entry Point
 * Initializes the database and starts the trading bot.
 */
const db = require('./db');
const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
const DB_PATH = path.join(DATA_DIR, 'noop.db');
const WIKI_DIR = process.env.WIKI_DIR || path.join(__dirname, '..', '..', 'knowledge');
const WIKI_META_PATH = path.join(WIKI_DIR, '.meta.json');

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

console.log('='.repeat(70));
console.log('NOOP-C Bot Starting');
console.log(`Data dir: ${DATA_DIR}`);
console.log(`DB path: ${DB_PATH}`);
console.log(`Wiki dir: ${WIKI_DIR}`);
console.log(`Wiki meta path: ${WIKI_META_PATH}`);
console.log(`Wiki dir exists: ${fs.existsSync(WIKI_DIR)}`);
console.log(`Wiki meta exists: ${fs.existsSync(WIKI_META_PATH)}`);
if (fs.existsSync(WIKI_META_PATH)) {
  try {
    const meta = JSON.parse(fs.readFileSync(WIKI_META_PATH, 'utf-8'));
    console.log(`Wiki meta summary: seeded_at=${meta.seeded_at || 'none'}, last_ingest=${meta.last_ingest || 'none'}, last_lint=${meta.last_lint || 'none'}`);
  } catch (e) {
    console.log(`Wiki meta summary read failed: ${e.message}`);
  }
}
console.log('='.repeat(70));

// Export db for script.js to use
global.__noopDb = db;

// Load and run the main bot script
require('../script.js');

// Graceful shutdown
const shutdown = () => {
  console.log('Closing database...');
  db.close();
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
