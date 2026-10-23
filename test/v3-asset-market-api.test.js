'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const os = require('node:os');
const Database = require('../dashboard/node_modules/better-sqlite3');
const ts = require('../dashboard/node_modules/typescript');
const React = require('../dashboard/node_modules/react');
const { renderToStaticMarkup } = require('../dashboard/node_modules/react-dom/server');
const root = path.resolve(__dirname, '..');

function load(file, imports, processOverride = process) {
  const source = fs.readFileSync(path.join(root, file), 'utf8');
  const code = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, esModuleInterop: true, jsx: ts.JsxEmit.ReactJSX },
  }).outputText;
  const exports = {};
  vm.runInNewContext(code, {
    exports, Response, Date, console, process: processOverride,
    require(name) {
      if (Object.hasOwn(imports, name)) return imports[name];
      throw new Error(`Unexpected import: ${name}`);
    },
  }, { filename: file });
  return exports;
}

test('asset API accepts only non-ETH assets and returns honest empty history', async () => {
  const assets = require('../bot/market-assets.json');
  const allowed = load('dashboard/src/lib/market-assets.ts', { '../../../bot/market-assets.json': assets });
  const calls = [];
  const route = load('dashboard/src/app/api/assets/[asset]/route.ts', {
    'next/server': { NextResponse: { json: (value, options) => new Response(JSON.stringify(value), { status: options?.status || 200 }) } },
    '@/lib/db': { getAssetMarketData: (asset, since, rangeMs) => {
      calls.push([asset, since, rangeMs]);
      return { ready: false, latest: null, history: [], quotes: [], observation_count: 0 };
    } },
    '@/lib/market-assets': allowed,
    '@/lib/dashboard-ranges': { ASSET_DEFAULT_RANGE: '24h', DASHBOARD_RANGES: ['14d', '24h'], dashboardRangeMs: () => 14 * 86400000 },
    '@/lib/response-cache': { cachedJsonRoute: (_request, _key, loader) => loader() },
  });
  const request = { nextUrl: new URL('http://localhost/api/assets/btc?range=14d') };
  const btc = await route.GET(request, { params: { asset: 'btc' } });
  assert.equal(btc.status, 200);
  assert.deepEqual(await btc.json(), { asset: 'BTC', range: '14d', ready: false, latest: null, history: [], quotes: [], observation_count: 0 });
  assert.equal(calls[0][0], 'BTC');
  assert.ok(Number.isFinite(Date.parse(calls[0][1])));
  assert.equal(calls[0][2], 14 * 86400000);
  for (const asset of ['ETH', 'DOGE']) {
    assert.equal((await route.GET(request, { params: { asset } })).status, 404);
  }
  assert.equal(calls.length, 1, 'invalid assets never reach the DB');
});

test('dashboard DB getter reads only the selected asset and its captured quotes', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-asset-api-'));
  try {
    const database = new Database(path.join(directory, 'noop.db'));
    database.pragma('journal_mode = WAL');
    database.exec(`
      CREATE TABLE asset_market_snapshots (
        asset TEXT, hour TEXT, collected_at TEXT, spot REAL, funding_rate REAL,
        instrument_count INTEGER, option_count INTEGER, quoted_count INTEGER,
        put_oi REAL, call_oi REAL, put_iv REAL, call_iv REAL, quote_status TEXT,
        PRIMARY KEY(asset,hour)
      );
      CREATE TABLE asset_option_quotes (
        asset TEXT, hour TEXT, instrument_name TEXT, expiry INTEGER, strike REAL,
        option_type TEXT, bid REAL, ask REAL, mark REAL, index_price REAL,
        delta REAL, implied_vol REAL, open_interest REAL, bid_amount REAL, ask_amount REAL,
        PRIMARY KEY(asset,hour,instrument_name)
      );
      INSERT INTO asset_market_snapshots VALUES
        ('BTC','2026-10-23T01:00:00Z','2026-10-23T01:02:00Z',60000,0.0001,100,80,50,1000,1200,0.6,0.5,'partial'),
        ('BTC','2026-10-23T02:00:00Z','2026-10-23T02:02:00Z',61000,0.0002,110,90,60,1100,1300,0.61,0.51,'complete'),
        ('ZEC','2026-10-23T02:00:00Z','2026-10-23T02:03:00Z',300,0.0003,70,50,40,800,900,0.7,0.65,'complete');
      INSERT INTO asset_option_quotes VALUES
        ('BTC','2026-10-23T02:00:00Z','BTC-20261225-60000-P',1766620800,60000,'P',120,122,121,61000,-0.3,0.6,50,10,11),
        ('ZEC','2026-10-23T02:00:00Z','ZEC-20261225-300-P',1766620800,300,'P',15,16,15.5,300,-0.3,0.7,25,5,6);
    `);
    database.close();
    const db = load('dashboard/src/lib/db.ts', {
      '../../../bot/dex-volume': {}, '../../../bot/correlation': {},
      'better-sqlite3': Database, '../../../bot/market-evidence': { HISTORY_SQL: {} },
      '../../../bot/derive-config': {}, path,
      './strategy-config': { BOT_CONFIG: { PUT_ANNUAL_RATE: 0.0333, PERIOD_DAYS: 15 } },
      '../../../bot/economic-events': {}, '../../../bot/open-interest': {},
      '../../../bot/funding-rates': {}, '../../../bot/liquidity-quality': {},
    }, { env: { DATA_DIR: directory } });
    const btc = db.getAssetMarketData('BTC', '2026-10-23T00:00:00Z', 86400000);
    assert.equal(btc.ready, true);
    assert.equal(btc.observation_count, 2);
    assert.equal(btc.history.length, 2);
    assert.equal(btc.latest.spot, 61000);
    assert.deepEqual(Array.from(btc.quotes, (quote) => quote.instrument_name), ['BTC-20261225-60000-P']);
    const zec = db.getAssetMarketData('ZEC', '2026-10-23T00:00:00Z', 86400000);
    assert.equal(zec.observation_count, 1);
    assert.equal(zec.history.length, 1);
    assert.equal(zec.latest.spot, 300);
    assert.deepEqual(Array.from(zec.quotes, (quote) => quote.instrument_name), ['ZEC-20261225-300-P']);
    const writer = new Database(path.join(directory, 'noop.db'));
    const insert = writer.prepare(`INSERT INTO asset_market_snapshots
      (asset,hour,collected_at,spot,quote_status) VALUES ('BTC',?,?,?,'partial')`);
    insert.run('2026-10-23T02:00:00.123Z', '2026-10-23T02:00:00.123Z', 61123);
    const boundary = db.getAssetMarketData('BTC', '2026-10-23T02:00:00.050Z', 86400000);
    assert.equal(boundary.observation_count, 1, 'mixed key range excludes the older exact-second row');
    assert.equal(boundary.latest.hour, '2026-10-23T02:00:00.123Z', 'latest uses chronological order at the legacy boundary');
    assert.deepEqual(Array.from(boundary.history, row => row.hour), ['2026-10-23T02:00:00.123Z']);
    writer.transaction(() => {
      insert.run('2026-10-23T02:00:45.123Z', '2026-10-23T02:00:45.123Z', 61500);
      for (let i = 0; i < 1200; i++) {
        const timestamp = new Date(Date.parse('2026-10-23T03:00:00Z') + i * 60000).toISOString().replace('.000Z', 'Z');
        insert.run(timestamp, timestamp, 62000 + i);
      }
    })();
    writer.close();
    const sampled = db.getAssetMarketData('BTC', '2026-10-23T00:00:00Z', 86400000);
    assert.equal(sampled.observation_count, 1204, 'old hourly and new per-ping rows remain counted');
    assert.ok(sampled.history.length <= 500, 'chart transfer stays bounded');
    assert.ok(sampled.history.some(row => row.hour === '2026-10-23T02:00:45.123Z'), 'chart sampling accepts millisecond timestamps');
    assert.equal(sampled.history.at(-1).hour, sampled.latest.hour, 'chart keeps the latest observation');
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test('asset page server HTML contains its latest market observation before hydration', () => {
  const jsxRuntime = require('../dashboard/node_modules/react/jsx-runtime');
  const initial = { range: '24h', stats: { last_price: 61000, last_price_time: '2026-10-23T02:00:00Z' },
    chart: { prices: [{ timestamp: '2026-10-23T02:00:00Z', price: 61000 }] }, ticks: [] };
  const calls = [];
  const pageImports = {
    'react/jsx-runtime': jsxRuntime,
    'next/navigation': { notFound: () => { throw new Error('not found'); } },
    '@/lib/market-assets': { isMarketAsset: asset => asset === 'BTC' },
    '@/lib/asset-overview': { getAssetOverview: (asset, range) => { calls.push([asset, range]); return initial; } },
    '@/lib/dashboard-ranges': { ASSET_DEFAULT_RANGE: '24h' },
    '@/app/page': { MarketOverview: ({ asset, initialAssetOverview, initialAssetError }) =>
      React.createElement('section', null, asset, ' ', initialAssetOverview?.stats.last_price?.toLocaleString('en-US'),
        ' ', initialAssetOverview?.chart.prices.length, ' observation in the selected range',
        ' ', initialAssetError || '') },
  };
  const page = load('dashboard/src/app/assets/[asset]/page.tsx', pageImports);
  const html = renderToStaticMarkup(page.default({ params: { asset: 'btc' } }));
  assert.match(html, /BTC 61,000/);
  assert.match(html, /1 observation in the selected range/);
  assert.deepEqual(calls, [['BTC', '24h']], 'server loads the selected asset before rendering');
  const unavailablePage = load('dashboard/src/app/assets/[asset]/page.tsx', {
    ...pageImports,
    '@/lib/asset-overview': { getAssetOverview: () => { throw new Error('DB unavailable'); } },
  });
  const unavailableHtml = renderToStaticMarkup(unavailablePage.default({ params: { asset: 'btc' } }));
  assert.match(unavailableHtml, /Market data temporarily unavailable/);
  assert.doesNotMatch(unavailableHtml, /61,000/);
});
