/**
 * Bot Entry Point
 * Initializes the database and starts the trading bot.
 */
const db = require('./db');
const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
const DB_PATH = path.join(DATA_DIR, 'noop.db');

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

console.log('='.repeat(70));
console.log('NOOP-C Bot Starting');
console.log(`Data dir: ${DATA_DIR}`);
console.log(`DB path: ${DB_PATH}`);
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
