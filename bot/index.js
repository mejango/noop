/**
 * Bot Entry Point
 * Initializes the database, loads config, and starts the trading bot.
 */
const config = require('./config');
const db = require('./db');
const fs = require('fs');
const path = require('path');

// Ensure data directories exist
[config.DATA_DIR, config.ARCHIVE_DIR].forEach(dir => {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
});

console.log('='.repeat(70));
console.log('NOOP-C Bot Starting');
console.log(`Data dir: ${config.DATA_DIR}`);
console.log(`DB path: ${config.DB_PATH}`);
console.log(`Archive: ${config.ARCHIVE_DIR}`);
console.log('='.repeat(70));

// Export db for script.js to use
global.__noopDb = db;
global.__noopConfig = config;

// Load and run the main bot script
require('../script.js');

// Graceful shutdown
const shutdown = () => {
  console.log('Closing database...');
  db.close();
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
