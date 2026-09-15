'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const Database = require('better-sqlite3');
const { loadProduction } = require('./helpers/load-production');

const NOW = '2026-09-15T12:00:00.000Z';
const OLD = '2026-09-14T12:00:00.000Z';
const SINCE = '2026-09-13T12:00:00.000Z';
const UNIVERSE = 'observed_all_tenors_abs_delta_0.02_to_0.12';

function fixture(t) {
  const source = fs.readFileSync(path.join(__dirname, '../bot/db.js'), 'utf8');
  const match = /getMarketQualitySummary:\s*db\.prepare\((`[\s\S]*?`)\s*\)/.exec(source);
  assert.ok(match, 'The actual market-quality prepared statement must be found');
  const sql = vm.compileFunction(`return ${match[1]};`)();
  const db = new Database(':memory:');
  t.after(() => db.close());
  db.exec(`CREATE TABLE options_snapshots (
    id INTEGER PRIMARY KEY, timestamp TEXT, instrument_name TEXT,
    option_type TEXT, delta REAL, bid_price REAL, ask_price REAL,
    mark_price REAL, implied_vol REAL, ask_amount REAL, bid_amount REAL,
    expiry REAL, strike REAL, index_price REAL
  )`);
  const insert = db.prepare(`INSERT INTO options_snapshots
    (timestamp,instrument_name,option_type,delta,bid_price,ask_price,mark_price,implied_vol,ask_amount,bid_amount,expiry,strike,index_price)
    VALUES (@timestamp,@instrument_name,@option_type,@delta,@bid_price,@ask_price,@mark_price,@implied_vol,@ask_amount,@bid_amount,@expiry,@strike,@index_price)`);
  let serial = 0;
  const add = (changes = {}) => insert.run({
    timestamp: NOW, instrument_name: `fixture-${++serial}-${changes.option_type || 'P'}`, option_type: 'P',
    delta: -0.05, bid_price: 1, ask_price: 3, mark_price: 10,
    implied_vol: null, ask_amount: null, bid_amount: null,
    expiry: 1800000000, strike: 1600, index_price: 2500,
    ...changes,
  });
  const statement = db.prepare(sql);
  return { db, add, get: (since = SINCE) => statement.all({ since }) };
}

function close(actual, expected) {
  assert.ok(Number.isFinite(actual) && Math.abs(actual - expected) < 1e-10,
    `${actual} differs from ${expected}`);
}

test('market quality describes only the latest observed frame, with explicit timestamp and universe, without rewriting history', t => {
  const f = fixture(t);
  f.add({ timestamp: OLD, option_type: 'C', delta: 0.05, ask_price: 100 });
  f.add({ timestamp: OLD, ask_price: 100 });
  f.add();
  f.add({ delta: -0.3, ask_price: 999 });
  const history = f.db.prepare('SELECT * FROM options_snapshots ORDER BY id').all();
  const changes = f.db.prepare('SELECT total_changes() AS n').get().n;
  const rows = f.get();
  assert.equal(rows.length, 1, 'An older call frame cannot fill in missing current calls');
  assert.equal(rows[0].option_type, 'P');
  assert.equal(rows[0].snapshot_timestamp, NOW);
  assert.equal(rows[0].universe, UNIVERSE);
  assert.equal(rows[0].observed_count, 1);
  close(rows[0].avg_spread, 0.2);
  assert.deepEqual(f.get(OLD), rows, 'Different lookbacks must not relabel the same snapshot as different measurements');
  assert.deepEqual(f.get(NOW), [], 'The since boundary remains exclusive');
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, changes);
  assert.deepEqual(f.db.prepare('SELECT * FROM options_snapshots ORDER BY id').all(), history);
});

test('observed, valid two-sided quotes and measurable spreads have separate denominators', t => {
  const f = fixture(t);
  f.add();
  f.add({ bid_price: 2, ask_price: 2, bid_amount: 0, ask_amount: 0 }); // Real zero spread and depth remain measurements.
  f.add({ mark_price: null, bid_amount: 100, ask_amount: 100 }); // Quoted, but no meaningful spread denominator.
  f.add({ bid_price: 0, ask_price: 0 });
  f.add({ bid_price: 4, ask_price: 2 }); // Crossed quote is invalid.
  f.add({ bid_price: null, ask_price: null, mark_price: null });
  f.add({ delta: -0.13 });
  f.add({ delta: -0.01 });
  const [row] = f.get();
  assert.equal(row.observed_count, 6);
  assert.equal(row.quoted_count, 3);
  assert.equal(row.spread_count, 2);
  assert.equal(row.count, row.spread_count, 'Legacy count continues to describe spread samples');
  close(row.min_spread, 0);
  close(row.max_spread, 0.2);
  close(row.avg_spread, 0.1);
  close(row.median_spread, 0.1);
  assert.equal(row.avg_iv, null);
  assert.equal(row.avg_depth, 0, 'Unknown depth is ignored, while observed zero depth remains zero');
  assert.equal(row.total_depth, 0);
});

test('median spread resists a tiny-mark outlier while the arithmetic mean remains truthful for odd and even populations', t => {
  const f = fixture(t);
  for (const [option_type, spreads] of [['P', [0.1, 0.2, 1000]], ['C', [0.1, 0.2, 0.3, 1000]]]) {
    for (const spread of spreads) {
      const mark = spread === 1000 ? 0.00001 : 10;
      f.add({ option_type, delta: option_type === 'P' ? -0.05 : 0.05,
        bid_price: 1, ask_price: 1 + spread * mark, mark_price: mark });
    }
  }
  const rows = f.get();
  const put = rows.find(row => row.option_type === 'P');
  const call = rows.find(row => row.option_type === 'C');
  close(put.median_spread, 0.2);
  close(call.median_spread, 0.25);
  close(put.avg_spread, 1000.3 / 3);
  close(call.avg_spread, 1000.6 / 4);
  assert.ok(put.avg_spread > 300 && call.avg_spread > 250);
});

test('unquoted and unmeasurable populations remain visible with unknown spread statistics instead of false zeros', t => {
  const f = fixture(t);
  f.add({ bid_price: 0, ask_price: 0 });
  f.add({ bid_price: null, ask_price: null });
  f.add({ option_type: 'C', delta: 0.05, mark_price: null });
  f.add({ option_type: 'C', delta: 0.05, mark_price: 0 });
  const rows = f.get();
  assert.equal(rows.length, 2);
  for (const row of rows) {
    assert.equal(row.observed_count, 2);
    assert.equal(row.quoted_count, row.option_type === 'C' ? 2 : 0);
    assert.equal(row.spread_count, 0);
    assert.equal(row.count, 0);
    for (const key of ['min_spread', 'max_spread', 'avg_spread', 'median_spread']) {
      assert.equal(row[key], null, `${row.option_type} ${key} must remain unknown`);
    }
    assert.equal(row.avg_iv, null);
    assert.equal(row.avg_depth, null);
    assert.equal(row.total_depth, null);
  }
});

test('historical sentiment windows ignore copied snapshot quality without mutating historical inputs', () => {
  const { summarizeSentimentWindowForLLM, summarizeSentimentForAdvisor } = loadProduction([
    'summarizeSentimentWindowForLLM', 'summarizeSentimentForAdvisor',
  ]);
  const windows = Object.fromEntries(['6h', '24h', '7d', '30d'].map(label => [label, {
    fundingRates: [], optionsSkew: [], aggregateOI: [],
    marketQuality: [{ option_type: 'P', count: 999, avg_spread: 987.654321 }],
  }]));
  const before = structuredClone(windows);
  for (const [label, window] of Object.entries(windows)) {
    const summary = summarizeSentimentWindowForLLM(label, window);
    assert.equal(Object.hasOwn(summary, 'market_quality'), false);
    assert.equal(summary.window, label);
  }
  const text = summarizeSentimentForAdvisor(windows, []);
  assert.doesNotMatch(text, /987\.654321|98765\.43|put mkt|call mkt/);
  for (const label of Object.keys(windows)) assert.ok(text.includes(`${label}:`));
  assert.deepEqual(windows, before);
});

test('the advisor receives one timestamped broad snapshot with honest units, real zeros and unknown values', t => {
  const f = fixture(t);
  f.add({ bid_price: 2, ask_price: 2, bid_amount: 0, ask_amount: 0 });
  f.add({ option_type: 'C', delta: 0.05, bid_price: 0, ask_price: 0 });
  const rows = f.get();
  const before = structuredClone(rows);
  const { summarizeMarketQualitySnapshotForLLM, formatMarketQualitySnapshotForAdvisor,
    summarizeSentimentForAdvisor } = loadProduction([
    'summarizeMarketQualitySnapshotForLLM', 'formatMarketQualitySnapshotForAdvisor',
    'summarizeSentimentForAdvisor',
  ]);
  const snapshot = summarizeMarketQualitySnapshotForLLM(rows);
  assert.equal(snapshot.as_of, NOW);
  assert.equal(snapshot.universe, UNIVERSE);
  assert.match(snapshot.scope, /no entry DTE filter/);
  assert.match(snapshot.scope, /not full-exchange coverage/);
  assert.match(snapshot.measurement, /spread = \(ask − bid\) \/ mark/);
  assert.match(snapshot.measurement, /Not a time-window average/);
  const put = snapshot.sides.find(row => row.option_type === 'P');
  const call = snapshot.sides.find(row => row.option_type === 'C');
  assert.equal(put.avg_spread_pct, 0);
  assert.equal(put.median_spread_pct, 0);
  assert.equal(put.avg_depth, 0);
  assert.equal(call.quoted_count, 0);
  assert.equal(call.spread_count, 0);
  assert.equal(call.avg_spread_pct, null);
  assert.equal(call.median_spread_pct, null);
  assert.equal(call.avg_depth, null);

  const formatted = formatMarketQualitySnapshotForAdvisor(rows);
  assert.ok(formatted.includes(NOW));
  assert.match(formatted, /PUT:.*spread\/mark mean 0%, median 0%/);
  assert.match(formatted, /CALL:.*spread\/mark mean n\/a%, median n\/a%/);
  const windows = Object.fromEntries(['6h', '24h', '7d', '30d'].map(label => [label, {}]));
  const text = summarizeSentimentForAdvisor(windows, rows);
  assert.equal(text.split('Broad observed quote snapshot:').length - 1, 1);
  assert.equal(text.split(NOW).length - 1, 1);
  assert.ok(text.endsWith(formatted));
  assert.equal(summarizeMarketQualitySnapshotForLLM([]), null);
  assert.match(formatMarketQualitySnapshotForAdvisor([]), /unavailable/);

  const mixed = rows.map((row, index) => ({ ...row, snapshot_timestamp: index === 0 ? OLD : NOW }));
  assert.equal(summarizeMarketQualitySnapshotForLLM(mixed).as_of, null,
    'Mixed historical frames cannot receive a single fabricated timestamp');
  assert.equal(summarizeMarketQualitySnapshotForLLM(rows.map(row => ({ ...row, snapshot_timestamp: null }))).as_of, null);
  assert.deepEqual(rows, before);
});
