'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const Database = require('../dashboard/node_modules/better-sqlite3');
const ts = require('../dashboard/node_modules/typescript');
const { normalizeBuyPutScore } = require('../bot/put-score');
const { normalizeSellCallScore } = require('../bot/call-score');

const root = path.resolve(__dirname, '..');
function load(file, imports) {
  const source = fs.readFileSync(path.join(root, file), 'utf8');
  const code = ts.transpileModule(source, { compilerOptions: {
    module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, esModuleInterop: true,
  } }).outputText;
  const exports = {};
  vm.runInNewContext(code, { exports, process, Date, console, require(name) {
    if (Object.hasOwn(imports, name)) return imports[name];
    throw new Error(`Unexpected import ${name}`);
  } }, { filename: file });
  return exports;
}

const lib = load('dashboard/src/lib/asset-overview.ts', {
  'better-sqlite3': Database, path,
  './dashboard-ranges': { dashboardRangeMs: () => 86400000 },
  '../../../bot/put-score': require('../bot/put-score'),
  '../../../bot/call-score': require('../bot/call-score'),
  '../../../bot/strategy-facts.json': require('../bot/strategy-facts.json'),
});

function fixture() {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE asset_market_snapshots (
    asset TEXT, hour TEXT, spot REAL, funding_rate REAL, put_oi REAL, call_oi REAL,
    put_iv REAL, call_iv REAL, instrument_count INTEGER, option_count INTEGER,
    quoted_count INTEGER, quote_status TEXT,
    short_momentum_main TEXT, short_momentum_derivative TEXT,
    medium_momentum_main TEXT, medium_momentum_derivative TEXT,
    PRIMARY KEY(asset,hour));
    CREATE TABLE asset_option_quotes (
    asset TEXT, hour TEXT, instrument_name TEXT, expiry INTEGER, strike REAL,
    option_type TEXT, bid REAL, ask REAL, mark REAL, index_price REAL, delta REAL,
    implied_vol REAL, open_interest REAL, bid_amount REAL, ask_amount REAL,
    bid_iv REAL, ask_iv REAL, forward REAL,
    PRIMARY KEY(asset,hour,instrument_name));
    CREATE TABLE asset_liquidity_snapshots (
      asset TEXT, observed_at TEXT, collected_at TEXT, venue TEXT, pool_id TEXT,
      chain TEXT, pair TEXT, tvl_usd REAL, volume_24h_usd REAL, source TEXT,
      PRIMARY KEY(asset,observed_at,venue,pool_id));`);
  return db;
}

test('asset overview keeps quotes isolated and uses production raw and edge score formulas', () => {
  const db = fixture();
  const now = Date.parse('2026-10-23T12:00:00Z');
  const hour = '2026-10-23T11:00:00Z';
  const putExpiry = Math.round((Date.parse(hour) + 60 * 86400000) / 1000);
  const callExpiry = Math.round((Date.parse(hour) + 8 * 86400000) / 1000);
  const snapshot = db.prepare(`INSERT INTO asset_market_snapshots
    (asset,hour,spot,funding_rate,put_oi,call_oi,put_iv,call_iv,quote_status)
    VALUES (?,?,?,?,?,?,?,?,?)`);
  snapshot.run('BTC', hour, 60000, 0.0001, 300, 200, 0.6, 0.5, 'complete');
  snapshot.run('ZEC', hour, 300, 0.0002, 9, 8, 0.9, 0.8, 'partial');
  db.prepare(`UPDATE asset_market_snapshots SET
    short_momentum_main = 'BULLISH', short_momentum_derivative = 'RISING',
    medium_momentum_main = 'BEARISH', medium_momentum_derivative = 'FALLING'
    WHERE asset = 'BTC'`).run();
  const quote = db.prepare(`INSERT INTO asset_option_quotes
    (asset,hour,instrument_name,expiry,strike,option_type,bid,ask,mark,index_price,
      delta,implied_vol,open_interest,bid_amount,ask_amount,bid_iv,ask_iv,forward)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  quote.run('BTC', hour, 'BTC-P', putExpiry, 55000, 'P', 90, 100, 95, 60000, -0.08, 0.6, 4, 10, 11, 0.59, 0.61, 60000);
  quote.run('BTC', hour, 'BTC-C', callExpiry, 65000, 'C', 9, 10, 9.5, 60000, 0.08, 0.5, 5, 10, 11, 0.49, 0.51, 60000);
  quote.run('ZEC', hour, 'ZEC-P', putExpiry, 250, 'P', 2, 3, 2.5, 300, -0.09, 0.9, 6, 10, 11, 0.88, 0.92, 300);
  const btc = lib.getAssetOverview('BTC', '24h', db, now);
  assert.equal(btc.range, '24h');
  assert.equal(btc.stats.last_price, 60000);
  assert.equal(btc.stats.short_momentum, 'BULLISH');
  assert.equal(btc.stats.short_derivative, 'RISING');
  assert.equal(btc.stats.medium_momentum, 'BEARISH');
  assert.equal(btc.stats.medium_derivative, 'FALLING');
  assert.equal(btc.chart.prices.length, 1);
  assert.equal(btc.chart.prices[0].short_momentum_main, 'BULLISH');
  assert.equal(btc.chart.prices[0].medium_momentum_derivative, 'FALLING');
  assert.equal(btc.chart.options[0].best_put_value, 0.08 / 100);
  assert.equal(btc.chart.options[0].best_call_value, 9 / 0.08);
  assert.equal(btc.chart.buyPutEdge[0].edge_score, normalizeBuyPutScore(0.08 / 100, 60));
  assert.equal(btc.chart.sellCallEdge[0].edge_score, normalizeSellCallScore(9 / 0.08, 8));
  assert.equal(btc.chart.optionsHeatmap.length, 2);
  assert.equal(btc.chart.sentiment.oiSnapshots[0].quote_status, 'complete');
  assert.deepEqual(Object.keys(JSON.parse(btc.ticks[0].summary)).sort(),
    ['best_call_detail', 'best_put_detail', 'current_best_call', 'current_best_put']);
  assert.equal(lib.getAssetQuoteFrames('ZEC', new Date(now - 86400000).toISOString(), 100, db, false, now)[0].quotes[0].instrument_name, 'ZEC-P');
  db.close();
});

test('missing and partial history stay empty or null', () => {
  const db = fixture();
  const now = Date.parse('2026-10-23T12:00:00Z');
  const empty = lib.getAssetOverview('SOL', '24h', db, now);
  assert.equal(empty.stats.last_price, null);
  assert.equal(empty.chart.options.length, 0);
  assert.equal(empty.ticks.length, 0);
  db.prepare(`INSERT INTO asset_market_snapshots
    (asset,hour,spot,quote_status) VALUES (?,?,?,'partial')`)
    .run('SOL', '2026-10-23T11:00:00Z', 190);
  const partial = lib.getAssetOverview('SOL', '24h', db, now);
  assert.equal(partial.chart.prices.length, 1);
  assert.equal(partial.stats.short_momentum, null);
  assert.equal(partial.stats.medium_derivative, null);
  assert.equal(partial.chart.prices[0].short_momentum_main, null);
  assert.equal(partial.chart.options[0].best_put_value, null);
  assert.equal(partial.chart.optionsCoverage.rowsInRange, 0);
  assert.equal(partial.chart.sentiment.fundingRates.length, 0);
  db.close();
});

test('overview API validates asset and range before reading the asset database', async () => {
  const calls = [];
  const route = load('dashboard/src/app/api/assets/[asset]/overview/route.ts', {
    'next/server': { NextResponse: { json: (value, options) => new Response(JSON.stringify(value), { status: options?.status || 200 }) } },
    '@/lib/asset-overview': { getAssetOverview: (asset, range) => {
      calls.push([asset, range]);
      return { range, stats: {}, chart: {}, ticks: [] };
    } },
    '@/lib/market-assets': { isMarketAsset: asset => ['BTC', 'ZEC', 'HYPE', 'SOL'].includes(asset) },
    '@/lib/dashboard-ranges': { ASSET_DEFAULT_RANGE: '24h', DASHBOARD_RANGES: ['24h', '14d'] },
    '@/lib/response-cache': { cachedJsonRoute: (_request, _key, loader) => loader() },
  });
  const request = { nextUrl: new URL('http://localhost/api/assets/btc/overview?range=14d') };
  const response = await route.GET(request, { params: { asset: 'btc' } });
  assert.equal(response.status, 200);
  assert.deepEqual(calls[0], ['BTC', '14d']);
  assert.equal((await route.GET(request, { params: { asset: 'ETH' } })).status, 404);
  assert.equal((await route.GET(request, { params: { asset: 'DOGE' } })).status, 404);
  assert.equal(calls.length, 1);
});

test('liquidity history is asset isolated and excludes future or invalid TVL rows', () => {
  const db = fixture();
  const now = Date.parse('2026-10-23T12:00:00Z');
  const insert = db.prepare(`INSERT INTO asset_liquidity_snapshots
    (asset,observed_at,collected_at,venue,pool_id,chain,pair,tvl_usd,volume_24h_usd,source)
    VALUES (?,?,?,?,?,?,?,?,?,?)`);
  insert.run('BTC', '2026-10-23T11:00:00Z', '2026-10-23T11:01:00Z', 'MayaChain', 'pool-a', 'MayaChain', 'BTC.BTC', 1000, 100, 'mayachain');
  insert.run('BTC', '2026-10-23T11:00:00Z', '2026-10-23T11:01:00Z', 'MayaChain', 'pool-b', 'MayaChain', 'BTC.BTC', 500, 50, 'mayachain');
  insert.run('ZEC', '2026-10-23T11:00:00Z', '2026-10-23T11:01:00Z', 'MayaChain', 'zec', 'MayaChain', 'ZEC.ZEC', 9999, 999, 'mayachain');
  insert.run('BTC', '2026-10-23T11:30:00Z', '2026-10-23T11:31:00Z', 'MayaChain', 'bad', 'MayaChain', 'BTC.BTC', -20, 200, 'mayachain');
  insert.run('BTC', '2026-10-23T13:00:00Z', '2026-10-23T13:01:00Z', 'MayaChain', 'future', 'MayaChain', 'BTC.BTC', 9000, 900, 'mayachain');
  const btc = lib.getAssetOverview('BTC', '24h', db, now);
  assert.equal(btc.chart.liquidity.length, 1);
  assert.equal(btc.chart.liquidity[0]['MayaChain BTC.BTC'], 1500);
  assert.equal(btc.chart.liquidityVolume.length, 0, 'rolling 24h volume is not plotted as hourly volume');
  db.close();
});
