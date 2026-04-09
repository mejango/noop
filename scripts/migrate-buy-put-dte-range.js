const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
const DB_PATH = path.join(DATA_DIR, 'noop.db');
const TARGET_RANGE = [45, 75];

if (!fs.existsSync(DB_PATH)) {
  console.log(`No database at ${DB_PATH}; skipping buy_put DTE migration`);
  process.exit(0);
}

const db = new Database(DB_PATH);
db.pragma('busy_timeout = 5000');

const selectRules = db.prepare(`
  SELECT id, criteria
  FROM trading_rules
  WHERE is_active = 1 AND rule_type = 'entry' AND action = 'buy_put'
  ORDER BY id ASC
`);

const updateRule = db.prepare(`
  UPDATE trading_rules
  SET criteria = @criteria
  WHERE id = @id
`);

let updated = 0;

for (const rule of selectRules.all()) {
  let criteria;
  try {
    criteria = typeof rule.criteria === 'string' ? JSON.parse(rule.criteria) : rule.criteria;
  } catch {
    continue;
  }

  if (!criteria || typeof criteria !== 'object') continue;

  const currentRange = Array.isArray(criteria.dte_range) ? criteria.dte_range : null;
  if (currentRange && currentRange[0] === TARGET_RANGE[0] && currentRange[1] === TARGET_RANGE[1]) {
    continue;
  }

  criteria.dte_range = TARGET_RANGE;
  updateRule.run({ id: rule.id, criteria: JSON.stringify(criteria) });
  updated++;
}

console.log(`buy_put DTE migration complete: ${updated} active rule(s) updated to [${TARGET_RANGE.join(',')}]`);
db.close();
