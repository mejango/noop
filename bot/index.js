/**
 * Bot Entry Point
 * Initializes the database and starts the trading bot.
 */
const db = require('./db');
const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
const ARCHIVE_DIR = path.join(DATA_DIR, 'archive');
const DB_PATH = path.join(DATA_DIR, 'noop.db');

// Ensure data directories exist
[DATA_DIR, ARCHIVE_DIR].forEach(dir => {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
});

console.log('='.repeat(70));
console.log('NOOP-C Bot Starting');
console.log(`Data dir: ${DATA_DIR}`);
console.log(`DB path: ${DB_PATH}`);
console.log(`Archive: ${ARCHIVE_DIR}`);
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
