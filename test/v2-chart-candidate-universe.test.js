'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const Database = require('better-sqlite3');
const ts = require('../dashboard/node_modules/typescript');

const ROOT = path.resolve(__dirname, '..');
const EPOCH = Date.parse('2030-09-01T12:00:00.000Z');
const HOUR = 3_600_000;
const DAY = 24 * HOUR;
const iso = (offset = 0) => new Date(EPOCH + offset).toISOString();
const SINCE = iso(-1);
class Clock extends Date {
  constructor(...args) { super(...(args.length ? args : [EPOCH + 2 * DAY])); }
  static now() { return EPOCH + 2 * DAY; }
}

function loadTs(file, dependencies) {
  const code = ts.transpileModule(fs.readFileSync(path.join(ROOT, file), 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, esModuleInterop: true },
  }).outputText;
  const module = { exports: {} };
  new Function('require', 'module', 'exports', 'process', 'Date', code)(name => {
    assert.ok(Object.hasOwn(dependencies, name), `Unexpected dependency: ${name}`);
    return dependencies[name];
  }, module, module.exports, { env: { DATA_DIR: '/unused-in-memory-fixture' }, cwd: () => ROOT }, Clock);
  return module.exports;
}

function fixture(t) {
  const db = new Database(':memory:');
  t.after(() => db.close());
  db.exec(`CREATE TABLE options_snapshots (
    id INTEGER PRIMARY KEY, timestamp TEXT, instrument_name TEXT, option_type TEXT,
    strike REAL, expiry REAL, delta REAL, ask_price REAL, bid_price REAL,
    ask_delta_value REAL, bid_delta_value REAL, index_price REAL, mark_price REAL,
    implied_vol REAL, ask_amount REAL, bid_amount REAL
  );
  CREATE INDEX idx_options_timestamp ON options_snapshots(timestamp);
  CREATE TABLE options_hourly (
    hour TEXT, best_put_dv REAL, best_call_dv REAL, avg_iv REAL, avg_spread REAL,
    avg_depth REAL, total_oi REAL, count INTEGER
  );
  CREATE TABLE candidate_observations (
    observed_at TEXT, action TEXT, raw_score REAL, dte REAL, delta REAL,
    ask_price REAL, bid_price REAL, metadata TEXT
  );`);
  // prepareAll contains unrelated getters. Defer preparation until invocation,
  // then run their original SQL unchanged against real, isolated SQLite tables.
  const connection = {
    pragma: value => db.pragma(value),
    prepare: sql => ({
      all: (...args) => db.prepare(sql).all(...args),
      get: (...args) => db.prepare(sql).get(...args),
    }),
  };
  const api = loadTs('dashboard/src/lib/db.ts', {
    'better-sqlite3': function () { return connection; }, path,
    './strategy-config': { BOT_CONFIG: {} },
    '../../../bot/economic-events': {}, '../../../bot/open-interest': {}, '../../../bot/funding-rates': {},
  });
  let next = 0;
  const option = (side, changes = {}) => {
    const timestamp = changes.timestamp ?? iso();
    const dte = changes.dte ?? (side === 'P' ? 60 : 8);
    const row = {
      timestamp, instrument_name: `ETH-20301231-${2000 + ++next}-${side}`, option_type: side,
      strike: 2000, expiry: Date.parse(timestamp) / 1000 + dte * 86400,
      delta: side === 'P' ? -0.05 : 0.05,
      ask_price: 10, bid_price: 5, ask_delta_value: 0.005, bid_delta_value: 100,
      index_price: 2500, mark_price: 7.5, implied_vol: 0.6, ask_amount: 10, bid_amount: 10,
      ...changes,
    };
    delete row.dte;
    const columns = Object.keys(row);
    db.prepare(`INSERT INTO options_snapshots (${columns.join(',')}) VALUES (${columns.map(c => '@' + c).join(',')})`).run(row);
    return row;
  };
  const legacyEdge = (row) => db.prepare('INSERT INTO candidate_observations VALUES (?,?,?,?,?,?,?,?)').run(
    row.timestamp, row.option_type === 'P' ? 'buy_put' : 'sell_call', 999,
    (row.expiry - Date.parse(row.timestamp) / 1000) / 86400, row.delta,
    row.ask_price, row.bid_price, '{}',
  );
  return { db, api, option, legacyEdge };
}

const values = row => [row.best_put_value, row.best_call_value];
const names = rows => rows.map(row => row.instrument_name).sort();
const close = (actual, expected) => assert.ok(Math.abs(actual - expected) < 1e-12, `${actual} differs from ${expected}`);

test('RAW retains eligible runners-up when higher RAW contracts are outside either strategy DTE window', t => {
  const f = fixture(t);
  const put = f.option('P', { ask_delta_value: 0.004 });
  const call = f.option('C', { bid_delta_value: 90 });
  f.option('P', { dte: 25, ask_delta_value: 99 });
  f.option('P', { dte: 79, ask_delta_value: 99 });
  f.option('C', { dte: 2, bid_delta_value: 99999 });
  f.option('C', { dte: 13, bid_delta_value: 99999 });
  assert.deepEqual(values(f.api.getBestOptionsOverTime(SINCE)[0]), [0.004, 90]);
  assert.deepEqual(values(f.api.getBestOptionsBucketed(SINCE, HOUR)[0]), [0.004, 90]);
  assert.deepEqual(names(f.api.getOptionsHeatmap(SINCE)), names([put, call]));
  assert.deepEqual(names(f.api.getOptionsHeatmap(SINCE, 12000, HOUR)), names([put, call]));
});

test('historical quote-time DTE and delta endpoints are inclusive for PUT and CALL', t => {
  const f = fixture(t);
  const included = [];
  for (const [side, dtes, deltas] of [['P', [45, 78], [-0.12, -0.02]], ['C', [5, 12], [0.04, 0.12]]]) {
    for (const dte of dtes) for (const delta of deltas) included.push(f.option(side, { dte, delta }));
    for (const dte of [dtes[0] - 1 / 86400, dtes[1] + 1 / 86400]) f.option(side, { dte });
    for (const delta of [deltas[0] - 0.00001, deltas[1] + 0.00001]) f.option(side, { delta });
  }
  assert.deepEqual(names(f.api.getOptionsHeatmap(SINCE)), names(included));
  assert.deepEqual(names(f.api.getOptionsHeatmap(SINCE, 12000, HOUR)), names(included));
  assert.deepEqual(values(f.api.getBestOptionsOverTime(SINCE)[0]), [0.005, 100]);
});

test('expired, missing-expiry, invalid-delta and unavailable executable quotes leave explicit null RAW gaps', t => {
  const f = fixture(t);
  for (const side of ['P', 'C']) {
    for (const expiry of [null, 0, EPOCH / 1000 - 1]) f.option(side, { expiry });
    for (const delta of [null, 0, side === 'P' ? 0.05 : -0.05]) f.option(side, { delta });
    for (const price of [null, 0, -1]) f.option(side, side === 'P' ? { ask_price: price } : { bid_price: price });
    for (const raw of [null, 0, -1]) f.option(side, side === 'P' ? { ask_delta_value: raw } : { bid_delta_value: raw });
  }
  const raw = f.api.getBestOptionsOverTime(SINCE);
  const bucketed = f.api.getBestOptionsBucketed(SINCE, HOUR);
  assert.equal(raw.length, 1);
  assert.equal(bucketed.length, 1);
  assert.deepEqual(values(raw[0]), [null, null]);
  assert.deepEqual(values(bucketed[0]), [null, null]);
  assert.equal(raw[0].lyra_spot, 2500);
  assert.deepEqual(f.api.getOptionsHeatmap(SINCE), []);
  assert.deepEqual(f.api.getOptionsHeatmap(SINCE, 12000, HOUR), []);
});

test('bucketed RAW preserves maximum aggregation and a null middle bucket without borrowing adjacent candidates', t => {
  const f = fixture(t);
  f.option('P', { ask_delta_value: 0.004 });
  f.option('P', { timestamp: iso(5 * 60_000), ask_delta_value: 0.008 });
  f.option('C', { bid_delta_value: 100 });
  f.option('C', { timestamp: iso(5 * 60_000), bid_delta_value: 300 });
  f.option('P', { timestamp: iso(HOUR), dte: 20, ask_delta_value: 999 });
  f.option('C', { timestamp: iso(HOUR), dte: 30, bid_delta_value: 99999 });
  f.option('P', { timestamp: iso(2 * HOUR), ask_delta_value: 0.006 });
  const rows = f.api.getBestOptionsBucketed(SINCE, HOUR);
  assert.deepEqual(rows.map(values), [[0.008, 300], [null, null], [0.006, null]]);
  assert.equal(rows[1].lyra_spot, 2500);
});

test('fractional quote time and latest sampled frame govern heatmap eligibility, not bucket start or an older quote', t => {
  const f = fixture(t);
  f.option('P', { timestamp: iso(60_000), dte: 60 });
  f.option('C', { timestamp: iso(60_000), dte: 8 });
  const timestamp = iso(HOUR - 500);
  // Half a second below the lower DTE bound. Truncating quote milliseconds
  // or checking the 12:00 bucket label would incorrectly admit these rows.
  f.option('P', { timestamp, expiry: (Date.parse(timestamp) - 500) / 1000 + 45 * 86400, ask_delta_value: 999 });
  f.option('C', { timestamp, expiry: (Date.parse(timestamp) - 500) / 1000 + 5 * 86400, bid_delta_value: 99999 });
  const raw = f.api.getBestOptionsOverTime(SINCE);
  assert.deepEqual(values(raw.at(-1)), [null, null]);
  assert.deepEqual(values(f.api.getBestOptionsBucketed(SINCE, HOUR)[0]), [0.005, 100]);
  assert.equal(f.api.getOptionsHeatmap(SINCE).length, 2);
  assert.deepEqual(f.api.getOptionsHeatmap(SINCE, 12000, HOUR), [], 'Latest sampled frame has no eligible candidates; do not substitute the older frame');
});

test('RAW and EDGE independently choose their maxima from the same eligible PUT and CALL population', t => {
  const f = fixture(t);
  for (const row of [
    f.option('P', { dte: 45, ask_price: 10, ask_delta_value: 0.005 }),
    f.option('P', { dte: 78, ask_price: 0.05 / 0.0045, ask_delta_value: 0.0045 }),
    f.option('C', { dte: 12, bid_price: 5, bid_delta_value: 100 }),
    f.option('C', { dte: 5, bid_price: 4.75, bid_delta_value: 95 }),
  ]) f.legacyEdge(row);
  assert.deepEqual(values(f.api.getBestOptionsOverTime(SINCE)[0]), [0.005, 100]);
  close(f.api.getBuyPutEdgeOverTime(SINCE)[0].edge_score, 0.0045 * (78 / 60) ** 0.8);
  close(f.api.getSellCallEdgeOverTime(SINCE)[0].edge_score, 95 * (8.5 / 5) ** 0.12);
  assert.equal(f.api.getOptionsHeatmap(SINCE).length, 4);
});

test('historical hourly RAW ignores legacy rollup maxima that cannot be filtered to eligible candidates', t => {
  const f = fixture(t);
  f.option('P', { ask_delta_value: 0.004 });
  f.option('C', { bid_delta_value: 90 });
  f.option('P', { dte: 25, ask_delta_value: 99 });
  f.option('C', { dte: 30, bid_delta_value: 99999 });
  f.db.prepare('INSERT INTO options_hourly (hour,best_put_dv,best_call_dv) VALUES (?,?,?)')
    .run(iso(), 99, 99999);
  const before = f.db.prepare('SELECT total_changes() AS n').get().n;
  const rows = f.api.getBestOptionsHourly_rollup(SINCE);
  assert.equal(rows.length, 1);
  assert.deepEqual(values(rows[0]), [0.004, 90]);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, before, 'Read-time filtering does not repair or rewrite stored history');
});

test('90d, 365d and all chart responses cannot reintroduce ineligible legacy hourly values', async t => {
  const f = fixture(t);
  f.option('P', { ask_delta_value: 0.004 });
  f.option('C', { bid_delta_value: 90 });
  f.option('P', { dte: 25, ask_delta_value: 99 });
  f.option('C', { dte: 30, bid_delta_value: 99999 });
  f.db.prepare('INSERT INTO options_hourly (hour,best_put_dv,best_call_dv) VALUES (?,?,?)').run(iso(), 99, 99999);
  const empty = () => [];
  const route = loadTs('dashboard/src/app/api/chart/route.ts', {
    'next/server': { NextResponse: { json: (body, options) => ({ status: options?.status ?? 200, body }) } },
    '@/lib/db': { ...f.api,
      getBestScores: () => ({}), getOptionsCoverage: () => ({}),
      getSpotPrices: empty, getSpotPricesBucketed: empty, getSpotPricesHourly_rollup: empty,
      getLiquidityOverTime: empty, getLiquidityHourly_rollup: empty,
      getFundingRates: empty, getFundingRatesHourlySeries: empty,
      getOISnapshots: empty, getOISnapshotsBucketed: empty,
    },
    '@/lib/limits': { CHART_ROW_LIMITS: { '14d': { prices: 12000, heatmap: 12000 } } },
    '@/lib/response-cache': { cachedJsonRoute: (_request, _key, loader) => loader() },
    '@/lib/dashboard-ranges': { dashboardRangeMs: range => (range === '90d' ? 90 : range === '365d' ? 365 : 730) * DAY },
  });
  for (const range of ['90d', '365d', 'all']) {
    const response = await route.GET({ nextUrl: new URL(`http://fixture/api/chart?range=${range}`) });
    assert.equal(response.status, 200, JSON.stringify(response.body));
    assert.equal(response.body.options.length, 1);
    assert.deepEqual(values(response.body.options[0]), [0.004, 90], range);
  }
});
