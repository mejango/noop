'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { EventEmitter } = require('node:events');
const { Worker } = require('node:worker_threads');
const Database = require('better-sqlite3');
const { collectAsset } = require('../bot/asset-market-collector');
const { pingAssetMarkets, startAssetMarketWorkers } = require('../bot/asset-market-workers');

test('collects V3 market quotes on distinct pings and preserves the fullest retry', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-asset-market-'));
  const oldPath = process.env.NOOP_DB_PATH;
  process.env.NOOP_DB_PATH = path.join(dir, 'noop.db');
  const db = require('../bot/db');
  const now = Date.parse('2026-10-23T12:34:00Z');
  const post = async (method, params) => {
    if (method === 'get_all_instruments') return {
      instruments: [
        { instrument_name: 'ZEC-20261030-1100-P', option_details: { expiry: 1793347200, strike: '1100', option_type: 'P' } },
        { instrument_name: 'ZEC-20261030-1400-C', option_details: { expiry: 1793347200, strike: '1400', option_type: 'C' } },
      ], pagination: { count: 2, num_pages: 1 },
    };
    if (params.instrument_type === 'perp') return { tickers: { 'ZEC-PERP': { I: '1226.86', f: '0.0000125' } } };
    return { tickers: {
      'ZEC-20261030-1100-P': { b: '20', a: '22', M: '21', I: '1226.86', B: '4', A: '5', option_pricing: { d: '-0.2', i: '0.7', bi: '0.65', ai: '0.75', f: '1228' }, stats: { oi: '10' } },
      'ZEC-20261030-1400-C': { b: '15', a: '17', M: '16', I: '1226.86', B: '3', A: '2', option_pricing: { d: '0.1', i: '0.6' }, stats: { oi: '12' } },
      'BTC-20261030-1400-C': { b: '1', a: '2' },
    } };
  };
  try {
    const { summary, options } = await collectAsset('ZEC', post, now);
    assert.equal(summary.hour, '2026-10-23T12:34:00.000Z');
    assert.equal(summary.spot, 1226.86);
    assert.equal(summary.funding_rate, 0.0000125);
    assert.equal(summary.quote_status, 'complete');
    assert.equal(summary.instrument_count, 2);
    assert.equal(summary.option_count, 2);
    assert.equal(summary.quoted_count, 2);
    assert.equal(summary.put_oi, 10);
    assert.equal(summary.call_iv, 0.6);
    assert.equal(options.length, 2);
    assert.equal(options[0].bid_iv, 0.65);
    assert.equal(options[0].ask_iv, 0.75);
    assert.equal(options[0].forward, 1228);
    assert.equal(db.insertAssetMarketSnapshot(summary, options), true);
    assert.deepEqual(db.db.prepare('SELECT bid_iv, ask_iv, forward FROM asset_option_quotes WHERE instrument_name = ?').get('ZEC-20261030-1100-P'),
      { bid_iv: 0.65, ask_iv: 0.75, forward: 1228 });
    assert.equal(db.insertAssetMarketSnapshot({ ...summary, quoted_count: 0, quote_status: 'partial' }, []), false);
    assert.equal(db.db.prepare('SELECT count(*) AS n FROM asset_option_quotes').get().n, 2);
    const nextPing = await collectAsset('ZEC', post, now + 5 * 60_000);
    nextPing.summary.short_momentum_main = 'upward';
    nextPing.summary.medium_momentum_main = 'neutral';
    assert.equal(db.insertAssetMarketSnapshot(nextPing.summary, nextPing.options), true);
    assert.equal(db.db.prepare('SELECT count(*) AS n FROM asset_market_snapshots WHERE asset = ?').get('ZEC').n, 2);
    assert.equal(db.db.prepare('SELECT count(*) AS n FROM asset_option_quotes').get().n, 4);
    assert.equal(db.db.prepare('SELECT count(*) AS n FROM spot_prices').get().n, 0);
    assert.equal(db.db.prepare('SELECT count(*) AS n FROM options_snapshots').get().n, 0);
    assert.deepEqual(db.db.prepare('SELECT short_momentum_main,medium_momentum_main FROM asset_market_snapshots WHERE hour = ?')
      .get(nextPing.summary.hour), { short_momentum_main: 'upward', medium_momentum_main: 'neutral' });
    assert.deepEqual(db.getAssetPriceHistory('ZEC', new Date(now + 10 * 60_000).toISOString()).map(row => row.price),
      [1226.86, 1226.86]);
    assert.deepEqual(db.getAssetPriceHistory('BTC', new Date(now + 10 * 60_000).toISOString()), []);
    const liquidity = { asset: 'ZEC', observed_at: summary.hour, collected_at: summary.collected_at,
      venue: 'Maya Protocol', pool_id: 'ZEC.ZEC', chain: 'Zcash', pair: 'ZEC/CACAO',
      tvl_usd: 1000, volume_24h_usd: null, source: 'maya_midgard_balanced_pool_tvl_estimate_v1' };
    db.insertAssetLiquiditySnapshot(liquidity);
    db.insertAssetLiquiditySnapshot({ ...liquidity, tvl_usd: 1100 });
    assert.deepEqual(db.db.prepare('SELECT asset,tvl_usd FROM asset_liquidity_snapshots').all(),
      [{ asset: 'ZEC', tvl_usd: 1100 }]);
    assert.throws(() => db.insertAssetLiquiditySnapshot({ ...liquidity, asset: 'ETH' }), /Invalid asset liquidity/);
    assert.throws(() => db.insertAssetMarketSnapshot(summary, [{ ...options[0], instrument_name: 'BTC-20261030-1100-P' }]));
    assert.equal(db.db.prepare('SELECT count(*) AS n FROM asset_option_quotes').get().n, 4);
  } finally {
    db.close();
    delete require.cache[require.resolve('../bot/db')];
    if (oldPath == null) delete process.env.NOOP_DB_PATH;
    else process.env.NOOP_DB_PATH = oldPath;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('existing asset quote tables gain the new IV and forward fields without losing rows', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-asset-migration-'));
  const file = path.join(dir, 'noop.db');
  const legacy = new Database(file);
  legacy.exec(`CREATE TABLE asset_option_quotes (
    asset TEXT NOT NULL, hour TEXT NOT NULL, instrument_name TEXT NOT NULL,
    PRIMARY KEY (asset, hour, instrument_name)
  );
  INSERT INTO asset_option_quotes VALUES ('BTC', '2026-10-23T12:00:00Z', 'BTC-20261030-80000-P');
  CREATE TABLE asset_market_snapshots (
    asset TEXT NOT NULL, hour TEXT NOT NULL, collected_at TEXT NOT NULL, spot REAL,
    funding_rate REAL, instrument_count INTEGER, option_count INTEGER, quoted_count INTEGER,
    put_oi REAL, call_oi REAL, put_iv REAL, call_iv REAL, quote_status TEXT NOT NULL,
    PRIMARY KEY(asset,hour)
  );
  INSERT INTO asset_market_snapshots (asset,hour,collected_at,spot,quote_status)
    VALUES ('BTC','2026-10-23T12:00:00Z','2026-10-23T12:00:01Z',80000,'partial');`);
  legacy.close();
  const oldPath = process.env.NOOP_DB_PATH;
  process.env.NOOP_DB_PATH = file;
  delete require.cache[require.resolve('../bot/db')];
  try {
    const store = require('../bot/db');
    const row = store.db.prepare('SELECT bid_iv, ask_iv, forward FROM asset_option_quotes WHERE asset = ?').get('BTC');
    assert.deepEqual(row, { bid_iv: null, ask_iv: null, forward: null });
    assert.deepEqual(store.db.prepare(`SELECT short_momentum_main,short_momentum_derivative,
      medium_momentum_main,medium_momentum_derivative FROM asset_market_snapshots WHERE asset = 'BTC'`).get(),
      { short_momentum_main: null, short_momentum_derivative: null,
        medium_momentum_main: null, medium_momentum_derivative: null });
    assert.equal(store.db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='asset_liquidity_snapshots'").get().name,
      'asset_liquidity_snapshots');
    store.close();
  } finally {
    delete require.cache[require.resolve('../bot/db')];
    if (oldPath == null) delete process.env.NOOP_DB_PATH;
    else process.env.NOOP_DB_PATH = oldPath;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('each ETH ping reaches independent asset workers without awaiting their work', () => {
  const created = [];
  class FakeWorker extends EventEmitter {
    constructor(_file, options) {
      super();
      this.asset = options.workerData.asset;
      this.messages = [];
      created.push(this);
    }
    postMessage(message) { this.messages.push(message); }
    terminate() { return Promise.resolve(); }
  }
  const stop = startAssetMarketWorkers({ WorkerClass: FakeWorker });
  try {
    assert.deepEqual(created.map(worker => worker.asset), ['BTC', 'ZEC', 'HYPE', 'SOL']);
    pingAssetMarkets('2026-10-23T12:34:00.000Z');
    for (const worker of created) {
      assert.deepEqual(worker.messages, [{ type: 'ping', timestamp: '2026-10-23T12:34:00.000Z' }]);
    }
    created[0].emit('exit', 1);
    pingAssetMarkets('2026-10-23T12:39:00.000Z');
    assert.equal(created[0].messages.length, 1);
    for (const worker of created.slice(1)) assert.equal(worker.messages.length, 2);
  } finally { stop(); }
  assert.match(require('node:fs').readFileSync(path.join(__dirname, '../script.js'), 'utf8'), /pingAssetMarkets\(tickTimestamp\)/);
});

test('an asset worker initializes its own SQLite connection', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-market-worker-'));
  const env = { ...process.env, DATA_DIR: dir };
  delete env.NOOP_DB_PATH;
  const worker = new Worker(require.resolve('../bot/asset-market-worker'), { workerData: { asset: 'BTC' }, env });
  try {
    const ready = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Asset worker startup timed out')), 10_000);
      worker.once('message', message => { clearTimeout(timer); resolve(message); });
      worker.once('error', error => { clearTimeout(timer); reject(error); });
    });
    assert.deepEqual(ready, { type: 'ready', asset: 'BTC' });
    assert.equal(fs.existsSync(path.join(dir, 'noop.db')), true);
  } finally {
    await worker.terminate();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('empty far expiry records partial coverage and unknown aggregate values', async () => {
  const post = async (method, params) => {
    if (method === 'get_all_instruments') return { instruments: [
      { instrument_name: 'SOL-20270625-200-P', option_details: { expiry: 1813881600, strike: 200, option_type: 'P' } },
    ], pagination: { count: 1, num_pages: 1 } };
    if (params.instrument_type === 'perp') return { tickers: { 'SOL-PERP': { I: '210' } } };
    return { tickers: {} };
  };
  const { summary, options } = await collectAsset('SOL', post, Date.parse('2026-10-23T12:00:00Z'));
  assert.equal(summary.quote_status, 'partial');
  assert.equal(summary.option_count, 0);
  assert.equal(summary.put_oi, null);
  assert.equal(options.length, 0);
  await assert.rejects(collectAsset('ETH', post), /Unsupported/);
});
