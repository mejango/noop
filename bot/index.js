/**
 * Bot Entry Point
 * Initializes the database and starts the trading bot.
 */
const fs = require('fs');
const path = require('path');
const venue = require('../integrations/derive-v3/profile').readProfile();
if (venue.version === 3) {
  if (process.env.NOOP_V3_ISOLATED_RUNNER !== '1') throw new Error('Start V3 through its dedicated isolated runner');
  require('../integrations/derive-v3/isolation').assertIsolatedDataPaths(venue, process.env, path.resolve(__dirname, '..'));
}
// Validate V3 state isolation before db.js can open or migrate any database.
const db = require('./db');

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
