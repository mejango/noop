'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const Database = require('better-sqlite3');
const ts = require('../dashboard/node_modules/typescript');

// Run the actual dashboard getters against isolated, in-memory SQLite tables.
const code = ts.transpileModule(fs.readFileSync(path.join(__dirname, '../dashboard/src/lib/db.ts'), 'utf8'), {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, esModuleInterop: true },
}).outputText;
const EPOCH = Date.parse('2026-09-14T12:00:00.000Z');
const iso = (offsetMs = 0) => new Date(EPOCH + offsetMs).toISOString();
const SINCE = iso(-60_000);

function fixture(t, { putTable = true, callTable = true } = {}) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  for (const [side, present] of [['buy_put', putTable], ['sell_call', callTable]]) {
    if (present) db.exec(`CREATE TABLE ${side}_edge_snapshots (
      timestamp TEXT, instrument_name TEXT, raw_score REAL, edge_score REAL, dte REAL, delta REAL
    )`);
  }
  db.exec(`CREATE TABLE candidate_observations (
    observed_at TEXT, action TEXT, raw_score REAL, dte REAL, delta REAL,
    ask_price REAL, bid_price REAL, metadata TEXT
  )`);
  const dependencies = {
    'better-sqlite3': function () { return db; }, path,
    './strategy-config': { BOT_CONFIG: {} },
    '../../../bot/economic-events': {}, '../../../bot/open-interest': {}, '../../../bot/funding-rates': {},
  };
  const module = { exports: {} };
  new Function('require', 'module', 'exports', 'process', code)(
    name => { assert.ok(Object.hasOwn(dependencies, name), `Unexpected dependency: ${name}`); return dependencies[name]; },
    module, module.exports, { env: { DATA_DIR: '/unused-in-memory-fixture' }, cwd: () => '/unused-in-memory-fixture' },
  );
  const candidate = (side, changes = {}) => {
    const row = {
      timestamp: iso(), raw: 99, dte: side === 'buy_put' ? 60 : 8.5,
      delta: side === 'buy_put' ? -0.05 : 0.05, ask: 10, bid: 5,
      metadata: JSON.stringify({ price_source: 'score_threshold', live_raw_score: side === 'buy_put' ? 0.005 : 100 }),
      ...changes,
    };
    db.prepare('INSERT INTO candidate_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
      .run(row.timestamp, side, row.raw, row.dte, row.delta, row.ask, row.bid, row.metadata);
  };
  const dedicated = (side, timestamp = iso(), raw = side === 'buy_put' ? 0.0032 : 100) => {
    db.prepare(`INSERT INTO ${side}_edge_snapshots VALUES (?, ?, ?, ?, ?, ?)`).run(
      timestamp, 'recorded-market-winner', raw, raw, side === 'buy_put' ? 60 : 8.5, side === 'buy_put' ? -0.05 : 0.05,
    );
  };
  const get = (side, since = SINCE, bucketMs = 0) => side === 'buy_put'
    ? module.exports.getBuyPutEdgeOverTime(since, bucketMs)
    : module.exports.getSellCallEdgeOverTime(since, bucketMs);
  return { candidate, dedicated, get };
}

function close(actual, expected) {
  assert.ok(Math.abs(actual - expected) < Math.max(1, Math.abs(expected)) * 1e-12, `${actual} differs from ${expected}`);
}

test('dedicated PUT and CALL market winners cannot be overridden by candidate or planned-price telemetry', t => {
  const f = fixture(t);
  for (const side of ['buy_put', 'sell_call']) {
    f.dedicated(side);
    f.candidate(side, { raw: 1000, ask: 1, bid: 100 });
    const rows = f.get(side);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].edge_score, side === 'buy_put' ? 0.0032 : 100);
  }
});

test('each side uses its global dedicated epoch even when the query starts later and a dedicated observation is missing', t => {
  const f = fixture(t);
  f.dedicated('buy_put', iso(-2 * 86_400_000));
  f.candidate('buy_put');
  f.candidate('sell_call');
  f.dedicated('sell_call', iso(86_400_000), 120);
  assert.deepEqual(f.get('buy_put'), [], 'A gap after dedicated coverage began must remain a gap');
  const calls = f.get('sell_call');
  assert.equal(calls.length, 2);
  assert.equal(calls[0].edge_score, 100, 'The call legacy epoch is independent of the put epoch');
  assert.equal(calls[1].edge_score, 120);
});

test('legacy candidates are accepted strictly before the dedicated boundary and excluded at or after it', t => {
  const f = fixture(t);
  for (const side of ['buy_put', 'sell_call']) {
    f.candidate(side, { timestamp: iso(-1), ask: 10, bid: 4 });
    f.dedicated(side);
    f.candidate(side, { timestamp: iso(), ask: 1, bid: 100 });
    f.candidate(side, { timestamp: iso(60_000), ask: 1, bid: 100 });
    const rows = f.get(side);
    assert.equal(rows.length, 2);
    assert.equal(rows[0].timestamp, '2026-09-14T11:59:59Z');
    assert.equal(rows[0].edge_score, side === 'buy_put' ? 0.005 : 80);
    assert.equal(rows[1].edge_score, side === 'buy_put' ? 0.0032 : 100);
  }
});

test('absent and empty dedicated tables independently allow legacy live-quote reconstruction', t => {
  for (const options of [{ putTable: false }, { callTable: false }, {}]) {
    const f = fixture(t, options);
    f.candidate('buy_put');
    f.candidate('sell_call');
    assert.equal(f.get('buy_put')[0].edge_score, 0.005);
    assert.equal(f.get('sell_call')[0].edge_score, 100);
  }
});

test('legacy PUT quotes are individually normalized before choosing the maximum, ignoring stored planned RAW', t => {
  const f = fixture(t);
  f.candidate('buy_put', { dte: 45, ask: 10, raw: 99 }); // RAW .005, EDGE .003972...
  f.candidate('buy_put', { dte: 78, ask: 0.05 / 0.0045, raw: 1 }); // Lower RAW, higher EDGE.
  const rows = f.get('buy_put');
  assert.equal(rows.length, 1);
  close(rows[0].edge_score, 0.0045 * Math.pow(78 / 60, 0.8));
  assert.ok(rows[0].edge_score > 0.005 * Math.pow(45 / 60, 0.8));
});

test('legacy CALL quotes are individually normalized before choosing the maximum, ignoring stored RAW', t => {
  const f = fixture(t);
  f.candidate('sell_call', { dte: 12, bid: 5, raw: 99 }); // RAW 100.
  f.candidate('sell_call', { dte: 5, bid: 4.75, raw: 1 }); // RAW 95 wins after normalization.
  const rows = f.get('sell_call');
  assert.equal(rows.length, 1);
  close(rows[0].edge_score, 95 * Math.pow(8.5 / 5, 0.12));
  assert.ok(rows[0].edge_score > 100 * Math.pow(8.5 / 12, 0.12));
});

test('missing or nonpositive executable quotes and out-of-policy deltas or DTE do not manufacture legacy EDGE', t => {
  const f = fixture(t);
  for (const side of ['buy_put', 'sell_call']) {
    for (const price of [null, 0, -1]) f.candidate(side, side === 'buy_put' ? { ask: price } : { bid: price });
    for (const delta of side === 'buy_put' ? [null, 0, 0.05, -0.01, -0.13] : [null, 0, -0.05, 0.03, 0.13]) {
      f.candidate(side, { delta });
    }
    for (const dte of side === 'buy_put' ? [null, 44, 79] : [null, 4, 13]) f.candidate(side, { dte });
    assert.deepEqual(f.get(side), [], `${side} must not fall back to planned RAW or metadata when its quote is unavailable`);
  }
});

test('time buckets average per-evaluation maximum EDGE rather than all candidates or the maximum over the bucket', t => {
  const f = fixture(t);
  f.candidate('buy_put', { ask: 0.05 / 0.003 });
  f.candidate('buy_put', { ask: 0.05 / 0.005 });
  f.candidate('buy_put', { timestamp: iso(5 * 60_000), ask: 0.05 / 0.007 });
  f.candidate('sell_call', { bid: 5 });
  f.candidate('sell_call', { bid: 10 });
  f.candidate('sell_call', { timestamp: iso(5 * 60_000), bid: 15 });
  for (const [side, expected] of [['buy_put', 0.006], ['sell_call', 250]]) {
    const rows = f.get(side, SINCE, 3_600_000);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].timestamp, '2026-09-14T12:00:00Z');
    close(rows[0].edge_score, expected);
  }
});
