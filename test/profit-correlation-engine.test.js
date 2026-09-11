'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { test } = require('node:test');
const Database = require('better-sqlite3');
const { buildProfitCorrelationReport } = require('../bot/profit-correlation-engine');

// Use the real schema, initialized only in an isolated temporary directory.
const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-correlation-schema-'));
const previousDataDir = process.env.DATA_DIR;
process.env.DATA_DIR = dataDir;
const canonical = require('../bot/db');
const schema = canonical.db.prepare("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'").all();
canonical.close();
if (previousDataDir == null) delete process.env.DATA_DIR;
else process.env.DATA_DIR = previousDataDir;
fs.rmSync(dataDir, { recursive: true, force: true });

function fixture(t) {
  const db = new Database(':memory:');
  for (const { sql } of schema) db.exec(sql);
  t.after(() => db.close());
  return db;
}

function addSpot(db, timestamp, price) {
  db.prepare('INSERT INTO spot_prices (timestamp, price) VALUES (?, ?)').run(timestamp, price);
}

function addQuote(db, timestamp, name, bid = 10, ask = 11, delta = 0.1, expiry = '2026-09-18T08:00:00.000Z', strike = 2200) {
  db.prepare(`INSERT INTO options_snapshots
    (timestamp, instrument_name, option_type, strike, expiry, delta, bid_price, ask_price,
      mark_price, bid_amount, ask_amount, bid_delta_value, open_interest, implied_vol)
    VALUES (?, ?, 'C', ?, ?, ?, ?, ?, ?, 5, 5, ?, 100, 0.5)`)
    .run(timestamp, name, strike, new Date(expiry).getTime() / 1000, delta, bid, ask, (bid + ask) / 2, bid / Math.abs(delta));
}

function options(extra = {}) {
  return { actions: ['sell_call'], horizonsHours: [1], lookbackHours: [], minSamples: 10, ...extra };
}

function feature(report, name) {
  const result = report.actions.sell_call.horizons['1'].features.find((item) => item.name === name);
  assert.ok(result, `feature ${name} exists`);
  return result;
}

test('report runs on the canonical schema with observation and outcome timestamps', (t) => {
  const db = fixture(t);
  db.prepare(`INSERT INTO candidate_observations (observed_at, action, instrument_name)
    VALUES ('2026-09-10T10:00:00.000Z', 'sell_call', 'TEST')`).run();
  db.prepare(`INSERT INTO decision_outcomes (observation_id, horizon_hours, due_at)
    VALUES (1, 1, '2026-09-10T11:00:00.000Z')`).run();
  const report = buildProfitCorrelationReport(db, options());
  assert.equal(report.meta.coverage.candidate_observations.rows, 1);
  assert.equal(report.meta.coverage.candidate_observations.first_timestamp, '2026-09-10T10:00:00.000Z');
  assert.equal(report.meta.coverage.decision_outcomes.last_timestamp, '2026-09-10T11:00:00.000Z');
});

test('CLI reads an exported canonical database without changing its journal mode or bytes', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-correlation-cli-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const dbPath = path.join(directory, 'snapshot.db');
  const reportPath = path.join(directory, 'report.json');
  const db = new Database(dbPath);
  for (const { sql } of schema) db.exec(sql);
  db.close();
  const before = fs.readFileSync(dbPath);
  execFileSync(process.execPath, [
    path.join(__dirname, '..', 'scripts', 'profit-correlation-report.js'),
    `--db=${dbPath}`, `--out=${reportPath}`, '--actions=sell_call', '--horizons=1',
  ], { encoding: 'utf8' });
  const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
  assert.equal(report.meta.engine, 'profit-correlation-v2');
  assert.equal(report.meta.coverage.decision_outcomes.rows, 0);
  assert.deepEqual(fs.readFileSync(dbPath), before);
  assert.ok(fs.existsSync(path.join(directory, 'report.md')));
});

for (const rollups of [false, true]) {
  test(`intrahour future observations cannot change entry features (${rollups ? 'rollups' : 'raw history'})`, (t) => {
    const db = fixture(t);
    addSpot(db, '2026-09-10T09:30:00.000Z', 1900);
    addSpot(db, '2026-09-10T10:00:00.000Z', 2000);
    addSpot(db, '2026-09-10T11:00:00.000Z', 2050);
    // A short-dated quote contributes prior market context but cannot be an entry.
    addQuote(db, '2026-09-10T09:30:00.000Z', 'PRIOR', 5, 6, 0.1, '2026-09-12T08:00:00.000Z');
    for (let i = 0; i < 12; i++) {
      addQuote(db, '2026-09-10T10:00:00.000Z', `ENTRY-${i}`, 10 + i / 10, 12, 0.1, undefined, 2200 + i);
      addQuote(db, '2026-09-10T11:00:00.000Z', `ENTRY-${i}`, 4, 5, 0.5);
    }
    if (rollups) {
      db.exec(`INSERT INTO options_hourly (hour, best_call_dv) VALUES
        ('2026-09-10T09:00:00Z', 50), ('2026-09-10T10:00:00Z', 111);
        INSERT INTO spot_prices_hourly (hour, avg_price) VALUES
        ('2026-09-10T09:00:00Z', 1900), ('2026-09-10T10:00:00Z', 2000);`);
    }
    const config = options({ sampleMode: 'all-candidates', maxSamples: 12 });
    const before = buildProfitCorrelationReport(db, config);
    addSpot(db, '2026-09-10T10:59:00.000Z', 3000);
    addQuote(db, '2026-09-10T10:59:00.000Z', 'FUTURE', 500, 600);
    if (rollups) {
      db.exec(`UPDATE options_hourly SET best_call_dv = 5000 WHERE hour = '2026-09-10T10:00:00Z';
        UPDATE spot_prices_hourly SET avg_price = 2500 WHERE hour = '2026-09-10T10:00:00Z';`);
    }
    const after = buildProfitCorrelationReport(db, config);
    assert.deepEqual(feature(after, 'strike_distance_pct'), feature(before, 'strike_distance_pct'));
    assert.deepEqual(feature(after, 'market_best_call_score'), feature(before, 'market_best_call_score'));
    assert.equal(feature(after, 'market_best_call_score').bucket.low_threshold, 50);
    assert.equal(after.actions.sell_call.horizons['1'].overall.mean_spot_return, 0.025);
  });
}

test('top-hour sampling includes exact boundaries and ranks only the first snapshot', (t) => {
  const db = fixture(t);
  addSpot(db, '2026-09-10T10:00:00.000Z', 2000);
  addSpot(db, '2026-09-10T11:00:00.000Z', 2000);
  addQuote(db, '2026-09-10T10:00:00.000Z', 'EARLY', 10, 11);
  addQuote(db, '2026-09-10T10:59:00.000Z', 'LATE', 100, 110);
  addQuote(db, '2026-09-10T11:00:00.000Z', 'EARLY', 4, 5, 0.5);
  addQuote(db, '2026-09-10T11:59:00.000Z', 'LATE', 99, 100, 0.5);
  const report = buildProfitCorrelationReport(db, options({ topPerHour: 1 }));
  assert.equal(report.actions.sell_call.candidates_scanned, 1);
  assert.equal(report.actions.sell_call.sampled_hours, 1);
  assert.equal(report.actions.sell_call.sampled_timestamps, 1);
  assert.equal(report.actions.sell_call.horizons['1'].overall.mean_return, 0.5);
  assert.equal(report.actions.sell_call.candidates_without_prior_market_hour, 1);
});

test('a midhour candidate cutoff still loads complete prior-hour context', (t) => {
  const db = fixture(t);
  const nowMs = new Date('2026-09-11T10:30:00.000Z').getTime();
  const originalNow = Date.now;
  Date.now = () => nowMs;
  t.after(() => { Date.now = originalNow; });
  addSpot(db, '2026-09-10T09:00:00.000Z', 1900);
  addSpot(db, '2026-09-10T09:59:00.000Z', 2000);
  addSpot(db, '2026-09-10T10:30:00.000Z', 2000);
  addQuote(db, '2026-09-10T09:00:00.000Z', 'PRIOR-A', 4, 5, 0.1, '2026-09-12T08:00:00.000Z');
  addQuote(db, '2026-09-10T09:59:00.000Z', 'PRIOR-B', 8, 9, 0.1, '2026-09-12T08:00:00.000Z');
  for (let i = 0; i < 12; i++) {
    addQuote(db, '2026-09-10T10:31:00.000Z', `ENTRY-${i}`);
    addQuote(db, '2026-09-10T11:31:00.000Z', `ENTRY-${i}`, 4, 5, 0.5);
  }
  const report = buildProfitCorrelationReport(db, options({ days: 1, topPerHour: 12 }));
  assert.equal(report.actions.sell_call.candidates_scanned, 12);
  assert.equal(feature(report, 'market_best_call_score').bucket.low_threshold, 80);
});
