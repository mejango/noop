const test = require('node:test');
const { beforeEach, after } = test;
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

// Exercise the production schema and ingestion functions on a disposable file.
const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-hourly-test-'));
const oldDataDir = process.env.DATA_DIR;
process.env.DATA_DIR = dataDir;
const production = require('../bot/db');
if (oldDataDir === undefined) delete process.env.DATA_DIR;
else process.env.DATA_DIR = oldDataDir;
const { db } = production;
const { createHourlyRollups, ALGORITHM_VERSION } = require('../bot/hourly-rollups');
const { parseArguments } = require('../bot/backfill-hourly');
const rollups = createHourlyRollups(db);
const HOUR = '2026-09-11T10:00:00Z';
const NEXT_HOUR = '2026-09-11T11:00:00Z';
const TABLES = ['spot_prices_hourly', 'options_hourly', 'onchain_hourly', 'funding_rates_hourly'];

beforeEach(() => {
  for (const table of [...TABLES, 'spot_prices', 'options_snapshots', 'onchain_data', 'funding_rates', 'hourly_rollup_metadata']) {
    db.prepare(`DELETE FROM ${table}`).run();
  }
});
after(() => {
  production.close();
  fs.rmSync(dataDir, { recursive: true, force: true });
});

function spot(price, timestamp, momentum = {}) {
  return production.insertSpotPrice(price, momentum, {}, timestamp);
}
function option(name, details = {}, optionDetails = {}) {
  return { instrument_name: name, option_details: { option_type: name.endsWith('-P') ? 'P' : 'C', ...optionDetails }, details };
}
function onchain(timestamp, magnitude, dex, direction = 'inflow') {
  production.insertOnchainData({ timestamp, dexLiquidity: { flowAnalysis: { magnitude, direction }, dexes: dex } });
}
function snapshot() {
  return Object.fromEntries(TABLES.map(table => [table, db.prepare(`SELECT * FROM ${table} ORDER BY 1, 2, 3`).all()]));
}

function seedAll() {
  spot(300, '2026-09-11T10:50:00.000Z');
  spot(100, '2026-09-11T10:05:00.000Z');
  spot(200, '2026-09-11T10:30:00.000Z');
  production.insertOptionsSnapshotBatch([option('ETH-P', { delta: -0.05, askDeltaValue: 1, impliedVol: 60, openInterest: 20 })], '2026-09-11T10:05:00.000Z');
  production.insertOptionsSnapshotBatch([option('ETH-P', { delta: -0.05, askDeltaValue: 2, impliedVol: 80, openInterest: 25 })], '2026-09-11T10:30:00.000Z');
  onchain('2026-09-11T10:05:00.000Z', 10, { dex: { totalLiquidity: 50, totalVolume: 100, totalTxCount: 10 } });
  onchain('2026-09-11T10:30:00.000Z', 30, { dex: { totalLiquidity: 60, totalVolume: 130, totalTxCount: 20 } });
  production.insertFundingRates([
    { timestamp: '2026-09-11T10:30:00.000Z', exchange: 'a', symbol: 'ETH', rate: 0.03 },
    { timestamp: '2026-09-11T10:05:00.000Z', exchange: 'a', symbol: 'ETH', rate: 0.01 },
  ]);
}

test('spot OHLC follows observation time, breaks ties by id, and respects hour boundaries', () => {
  spot(300, '2026-09-11T10:50:00.000Z', { shortTermMomentum: 'last', mediumTermMomentum: 'last-medium' });
  spot(101, '2026-09-11T10:00:00.500Z');
  spot(100, HOUR);
  spot(200, '2026-09-11T10:20:00.000Z');
  spot(305, '2026-09-11T10:50:00.000Z', { shortTermMomentum: 'tie-last' });
  spot(999, NEXT_HOUR);
  const row = db.prepare('SELECT * FROM spot_prices_hourly WHERE hour = ?').get(HOUR);
  assert.deepEqual(row, { hour: HOUR, open: 100, high: 305, low: 100, close: 305,
    avg_price: 201.2, short_momentum: 'tie-last', medium_momentum: null, count: 5 });
  assert.equal(db.prepare('SELECT count FROM spot_prices_hourly WHERE hour = ?').get(NEXT_HOUR).count, 1);
});

test('option means use observed rows, keep missing metrics null, and sum latest OI once per instrument', () => {
  production.insertOptionsSnapshotBatch([option('ETH-P', { delta: -0.05, openInterest: 100 })], '2026-09-11T10:05:00.000Z');
  production.insertOptionsSnapshotBatch([
    option('ETH-P', { delta: -0.05, askDeltaValue: 8, askPrice: 11, bidPrice: 9, markPrice: 10, askAmount: 2, bidAmount: 2, impliedVol: 60, openInterest: 120 }),
    option('ETH-C', { delta: 0.08, bidDeltaValue: 7, askPrice: 7, bidPrice: 3, markPrice: 5, askAmount: 3, bidAmount: 5, impliedVol: 80, openInterest: 40 }),
    option('FAR-C', { delta: 0.8, impliedVol: 999, openInterest: 10 }),
  ], '2026-09-11T10:10:00.000Z');
  production.insertOptionsSnapshotBatch([option('ETH-P', { delta: -0.05, askDeltaValue: 10, askPrice: 10, bidPrice: 10, markPrice: 10, askAmount: 0, bidAmount: 0, impliedVol: 70, openInterest: 125 })], '2026-09-11T10:20:00.000Z');
  const row = db.prepare('SELECT * FROM options_hourly').get();
  assert.equal(row.best_put_dv, 10);
  assert.equal(row.best_call_dv, 7);
  assert.equal(row.avg_spread, 1 / 3);
  assert.equal(row.avg_depth, 4);
  assert.equal(row.avg_iv, 70);
  assert.equal(row.total_oi, 175);
  assert.equal(row.count, 3);
  const before = snapshot();
  rollups.rebuild();
  assert.deepEqual(snapshot(), before);
});

test('a missing latest OI is unknown and empty option populations are not zero-filled', () => {
  production.insertOptionsSnapshotBatch([option('ETH-P', { delta: -0.5, openInterest: 100 })], '2026-09-11T10:05:00.000Z');
  production.insertOptionsSnapshotBatch([option('ETH-P', { delta: -0.5 })], '2026-09-11T10:10:00.000Z');
  assert.deepEqual(db.prepare('SELECT * FROM options_hourly').get(), {
    hour: HOUR, best_put_dv: null, best_call_dv: null, avg_spread: null, avg_depth: null, avg_iv: null, total_oi: null, count: 2,
  });
  production.insertOptionsSnapshotBatch([option('ETH-P', { delta: -0.05, askDeltaValue: 0, askAmount: 0, bidAmount: 0, impliedVol: 0, openInterest: 0 })], '2026-09-11T10:20:00.000Z');
  const row = db.prepare('SELECT * FROM options_hourly').get();
  assert.equal(row.total_oi, 0);
  assert.equal(row.avg_depth, 0);
  assert.equal(row.avg_iv, 0);
  assert.equal(row.best_put_dv, 0);
});

test('onchain magnitude is an arithmetic mean and latest stocks retain zero and null', () => {
  onchain('2026-09-11T10:50:00.000Z', 30, { dex: { totalLiquidity: 0, totalVolume: 0, totalTxCount: 0 } }, 'outflow');
  onchain('2026-09-11T10:10:00.000Z', 10, { dex: { totalLiquidity: 20, totalVolume: 100, totalTxCount: 10 } });
  onchain('2026-09-11T10:30:00.000Z', 20, { dex: { totalLiquidity: 30, totalVolume: 130, totalTxCount: 20 } });
  assert.deepEqual(db.prepare('SELECT * FROM onchain_hourly').get(), {
    hour: HOUR, dex: 'dex', tvl: 0, volume: 0, tx_count: 0, avg_magnitude: 20, direction: 'outflow',
  });
  onchain('2026-09-11T10:55:00.000Z', null, { dex: { totalLiquidity: null, totalVolume: 5 } }, null);
  assert.deepEqual(db.prepare('SELECT * FROM onchain_hourly').get(), {
    hour: HOUR, dex: 'dex', tvl: null, volume: 5, tx_count: null, avg_magnitude: 20, direction: null,
  });
});

test('invalid onchain payloads and excluded DEX records are handled identically during repair', () => {
  onchain('2026-09-11T10:10:00.000Z', null, {
    uniswap_v4: { pools: 2, totalLiquidity: 1000 }, bad: { error: 'unavailable' }, empty: null,
    dex: { totalLiquidity: 0, totalVolume: 0 },
  });
  db.prepare('INSERT INTO onchain_data (timestamp, raw_data) VALUES (?, ?)').run('2026-09-11T10:30:00.000Z', '{broken');
  rollups.refreshOnchainHour(HOUR);
  const before = snapshot();
  assert.equal(before.onchain_hourly.length, 1);
  assert.equal(before.onchain_hourly[0].avg_magnitude, null);
  rollups.rebuild();
  assert.deepEqual(snapshot(), before);
});

test('funding averages are isolated by exchange/symbol and duplicate collection does not count twice', () => {
  const item = { timestamp: '2026-09-11T10:30:00.000Z', exchange: 'a', symbol: 'ETH', rate: 0.03 };
  production.insertFundingRates([
    item,
    { ...item, timestamp: '2026-09-11T10:05:00.000Z', rate: 0.01 },
    { ...item, exchange: 'b', rate: 0.5 },
    { ...item, symbol: 'BTC', rate: 0.9 },
    { ...item, timestamp: NEXT_HOUR, rate: 0.7 },
    item,
  ]);
  const row = db.prepare('SELECT * FROM funding_rates_hourly WHERE hour = ? AND exchange = ? AND symbol = ?').get(HOUR, 'a', 'ETH');
  assert.equal(row.avg_rate, 0.02);
  assert.equal(row.count, 2);
  assert.equal(db.prepare('SELECT COUNT(*) AS count FROM funding_rates_hourly').get().count, 4);
  const before = snapshot();
  rollups.rebuild();
  assert.deepEqual(snapshot(), before);
});

test('full repair reproduces every live table, removes stale buckets, and records its version', () => {
  seedAll();
  const before = snapshot();
  for (const table of TABLES) db.prepare(`UPDATE ${table} SET hour = ?`).run('2020-01-01T00:00:00Z');
  const result = rollups.rebuild();
  assert.deepEqual(snapshot(), before);
  assert.deepEqual(result.counts, { spot_prices_hourly: 1, options_hourly: 1, onchain_hourly: 1, funding_rates_hourly: 1 });
  assert.equal(result.algorithmVersion, ALGORITHM_VERSION);
  assert.deepEqual(JSON.parse(db.prepare("SELECT value FROM hourly_rollup_metadata WHERE key = 'last_full_rebuild'").get().value), result);
  rollups.rebuild();
  assert.deepEqual(snapshot(), before);
});

test('repair is atomic across all tables when a late aggregate insert fails', () => {
  seedAll();
  for (const table of TABLES) db.prepare(`UPDATE ${table} SET hour = ?`).run('2020-01-01T00:00:00Z');
  const before = snapshot();
  db.exec("CREATE TRIGGER fail_funding_repair BEFORE INSERT ON funding_rates_hourly BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END");
  try {
    assert.throws(() => rollups.rebuild(), /simulated write failure/);
    assert.deepEqual(snapshot(), before);
    assert.equal(db.prepare('SELECT COUNT(*) AS count FROM hourly_rollup_metadata').get().count, 0);
  } finally { db.exec('DROP TRIGGER fail_funding_repair'); }
});

test('ingestion rolls back raw observations if the hourly write fails', () => {
  db.exec("CREATE TRIGGER fail_spot_hour BEFORE INSERT ON spot_prices_hourly BEGIN SELECT RAISE(ABORT, 'simulated rollup failure'); END");
  try {
    assert.throws(() => spot(100, HOUR), /simulated rollup failure/);
    assert.equal(db.prepare('SELECT COUNT(*) AS count FROM spot_prices').get().count, 0);
  } finally { db.exec('DROP TRIGGER fail_spot_hour'); }
});

test('range repair replaces only whole UTC hours in [from, to)', () => {
  seedAll();
  spot(999, NEXT_HOUR);
  db.prepare('UPDATE spot_prices_hourly SET close = -1 WHERE hour = ?').run(HOUR);
  db.prepare('UPDATE spot_prices_hourly SET close = -2 WHERE hour = ?').run(NEXT_HOUR);
  rollups.rebuild({ from: HOUR, to: NEXT_HOUR });
  assert.equal(db.prepare('SELECT close FROM spot_prices_hourly WHERE hour = ?').get(HOUR).close, 300);
  assert.equal(db.prepare('SELECT close FROM spot_prices_hourly WHERE hour = ?').get(NEXT_HOUR).close, -2);
  assert.equal(db.prepare("SELECT COUNT(*) AS count FROM hourly_rollup_metadata WHERE key = 'last_full_rebuild'").get().count, 0);
  assert.throws(() => rollups.rebuild({ from: '2026-09-11T10:01:00Z' }), /aligned/);
  assert.throws(() => rollups.rebuild({ from: NEXT_HOUR, to: HOUR }), /precede/);
});

test('CLI requires an explicit database, supports DATA_DIR, and refuses missing files', () => {
  assert.throws(() => parseArguments([], {}), /explicit/);
  assert.equal(parseArguments([], { DATA_DIR: dataDir }).db, path.join(dataDir, 'noop.db'));
  assert.equal(parseArguments(['--db', path.join(dataDir, 'explicit.db')], { DATA_DIR: '/ignored' }).db, path.join(dataDir, 'explicit.db'));
  assert.deepEqual(parseArguments(['--help'], {}), { help: true });
  assert.throws(() => parseArguments(['--db'], {}), /Missing/);
  const missing = path.join(dataDir, 'missing.db');
  const result = spawnSync(process.execPath, ['bot/backfill-hourly.js', '--db', missing], { cwd: path.join(__dirname, '..'), encoding: 'utf8' });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /does not exist|unable to open database file/);
  assert.equal(fs.existsSync(missing), false);
});

test('CLI rebuild uses the production aggregation implementation', () => {
  seedAll();
  const before = snapshot();
  for (const table of TABLES) db.prepare(`DELETE FROM ${table}`).run();
  const result = spawnSync(process.execPath, ['bot/backfill-hourly.js', '--db', path.join(dataDir, 'noop.db')], { cwd: path.join(__dirname, '..'), encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(snapshot(), before);
  assert.match(result.stdout, /funding_rates_hourly/);
});

test('bounded repair scans only indexed raw timestamp ranges', () => {
  seedAll();
  const executed = [];
  const traced = new Proxy(db, { get(target, key) {
    if (key === 'prepare') return sql => {
      const statement = target.prepare(sql);
      return new Proxy(statement, { get(stmt, method) {
        if (['all', 'run', 'get'].includes(method)) return (...parameters) => {
          executed.push({ sql, parameters });
          return stmt[method](...parameters);
        };
        const value = stmt[method];
        return typeof value === 'function' ? value.bind(stmt) : value;
      } });
    };
    const value = target[key];
    return typeof value === 'function' ? value.bind(target) : value;
  } });
  createHourlyRollups(traced).rebuild({ from: HOUR, to: NEXT_HOUR });
  const rawRangeQueries = executed.filter(({ sql }) => sql.startsWith('SELECT DISTINCT strftime'));
  assert.equal(rawRangeQueries.length, 4);
  for (const { sql, parameters } of rawRangeQueries) {
    const plans = db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all(...parameters);
    assert.ok(plans.some(row => /SEARCH .*USING .*INDEX.*timestamp>\? AND timestamp<\?/.test(row.detail)), JSON.stringify(plans));
    assert.ok(!plans.some(row => /SCAN (spot_prices|options_snapshots|onchain_data|funding_rates)/.test(row.detail)));
  }
});

test('repair CLI defaults to brief lock waits and validates explicit timeout bounds', () => {
  assert.equal(parseArguments(['--db', '/tmp/example.db', '--busy-timeout-ms', '0'])['busy-timeout-ms'], 0);
  assert.equal(parseArguments(['--db', '/tmp/example.db', '--busy-timeout-ms=250'])['busy-timeout-ms'], 250);
  assert.throws(() => parseArguments(['--db', '/tmp/example.db', '--busy-timeout-ms', '5001']), /between 0 and 5000/);
  assert.throws(() => parseArguments(['--db', '/tmp/example.db', '--busy-timeout-ms', 'NaN']), /between 0 and 5000/);
});
