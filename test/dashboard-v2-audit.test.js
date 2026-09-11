'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const os = require('node:os');
const ts = require('../dashboard/node_modules/typescript');
const root = path.resolve(__dirname, '..');

function loadTs(file, overrides = {}, globals = {}) {
  const code = ts.transpileModule(fs.readFileSync(path.join(root, file), 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, esModuleInterop: true },
  }).outputText;
  const result = {};
  vm.runInNewContext(code, {
    exports: result, process, Buffer, Response, Request, Headers, AbortSignal, console, Error,
    require(name) {
      if (Object.hasOwn(overrides, name)) return overrides[name];
      return require(name);
    },
    ...globals,
  }, { filename: file });
  return result;
}

function client(response) {
  return loadTs('dashboard/src/lib/lyra.ts', {
    'viem/accounts': { privateKeyToAccount: () => ({ signMessage: async () => 'test-only' }) },
  }, {
    process: { env: { PRIVATE_KEY: '0x' + '2'.repeat(64) } },
    fetch: async (...args) => ({ ok: true, json: async () => response(...args) }),
  });
}
const jsonResponse = { json: (body, options) => new Response(JSON.stringify(body), { status: options?.status || 200 }) };

test('V2 API errors, malformed balances and wrong account identities reject instead of appearing flat', async () => {
  for (const result of [
    { error: { code: 1, message: 'Bad signature' } },
    { result: { positions: null } },
    { result: { positions: [], failed_to_fetch: true } },
    { result: { positions: [], subaccount_id: 777 } },
    { result: { positions: [{ instrument_name: 'ETH-PERP', amount: 'not-a-number' }] } },
  ]) await assert.rejects(client(() => result).getPositions(), /unavailable/i);
  await assert.rejects(client(() => ({ result: { collaterals: [{ asset_name: 'USDC', amount: '1', mark_price: '1' }] } })).getCollaterals(), /mark_value/);
  await assert.rejects(client(() => ({ result: {} })).getSubaccount(), /initial_margin/);
});

test('a failed account read is never cached; an explicit empty account is accepted', async () => {
  let requests = 0;
  const api = client(() => ++requests === 1 ? { error: { message: 'Unavailable' } } : { result: { positions: [] } });
  await assert.rejects(api.getPositions(), /unavailable/i);
  assert.equal((await api.getPositions()).length, 0);
  assert.equal((await api.getPositions()).length, 0);
  assert.equal(requests, 2);
});

test('V2 account parsing preserves zero and normalizes nested Greeks without inventing missing Greeks', async () => {
  const row = { instrument_name: 'ETH-20261225-2500-P', amount: '1', average_price: '10', mark_price: '0', mark_value: '0', unrealized_pnl: '-10', index_price: '3000', greeks: { delta: '0', theta: '-1' } };
  const [position] = await client(() => ({ result: [row] })).getPositions();
  assert.equal(position.delta, 0);
  assert.equal(position.theta, -1);
  assert.equal(position.gamma, null);
});

test('account route returns explicit unavailable status, and unknown fees stay null', async () => {
  let shouldFail = true;
  const route = loadTs('dashboard/src/app/api/lyra/account/route.ts', {
    'next/server': { NextResponse: jsonResponse },
    '@/lib/lyra': { getPositions: async () => { if (shouldFail) throw new Error('Account unavailable'); return []; }, getCollaterals: async () => [] },
    '@/lib/db': { getOrderTradesSince: () => [{ id: 1, timestamp: new Date().toISOString(), instrument_name: 'ETH-20261225-2500-P', action: 'buy_put', filled_amount: 1, fill_price: 10 }] },
    '@/lib/response-cache': { cachedJsonRoute: (_req, _key, loader) => loader() },
    '@/lib/dashboard-ranges': { dashboardRangeMs: () => 86400000 },
  });
  const request = { url: 'http://localhost/api/lyra/account?range=30d', nextUrl: new URL('http://localhost/api/lyra/account?range=30d') };
  const failed = await route.GET(request);
  assert.equal(failed.status, 502);
  assert.deepEqual(await failed.json(), { error: 'Account unavailable', account_available: false });
  shouldFail = false;
  const ok = await route.GET(request);
  const body = await ok.json();
  assert.equal(body.account_available, true);
  assert.equal(body.trades[0].trade_fee, null);
  assert.equal(body.trades[0].realized_pnl, null);
});

test('AI snapshot stops on unavailable account data', async () => {
  const api = loadTs('dashboard/src/lib/snapshot.ts', {
    './db': new Proxy({}, { get: (_target, key) => key === 'getBotBudget' ? () => ({cycleDays:15}) : () => [] }),
    './lyra': { getPositions: async () => { throw new Error('Account unavailable'); }, getCollaterals: async () => [] },
    './correlation': { buildCorrelationAnalysis: () => ({}) },
    './wiki': { resolveWikiDir: () => '/nonexistent' },
  });
  await assert.rejects(api.buildMarketSnapshot(), /Account unavailable/);
});

test('advisor parameters render from supplied runtime facts and budget configuration', () => {
  const config = require('../bot/config.json');
  const facts = require('../bot/strategy-facts.json');
  const strategy = loadTs('dashboard/src/lib/strategy-config.ts', {}, { process: { env: { BOT_CONFIG_PATH: path.join(root, 'bot/config.json') }, cwd: () => root } });
  const rendered = strategy.describeStrategy({ ...config, PERIOD_DAYS: 17, PUT_ANNUAL_RATE: 0.04 }, { ...facts, put_dte_range: [50, 70] });
  assert.match(rendered, /17-day cycles/);
  assert.match(rendered, /4\.00%/);
  assert.match(rendered, /50–70 DTE/);
  assert.match(rendered, /Spending the put budget does not mean protection is absent/);
});

test('actual dashboard DB getters use C quotes and bounded historical observations', () => {
  const fixtureDir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-dashboard-audit-'));
  const previous = process.env.DATA_DIR;
  process.env.DATA_DIR = fixtureDir;
  const bot = require('../bot/db');
  bot.close();
  if (previous == null) delete process.env.DATA_DIR; else process.env.DATA_DIR = previous;
  const Database = require('better-sqlite3');
  const db = new Database(path.join(fixtureDir, 'noop.db'));
  const now = new Date().toISOString();
  db.prepare('INSERT INTO options_snapshots (timestamp,instrument_name,option_type,bid_price) VALUES (?,?,?,?)').run(now, 'ETH-20261225-4000-C', 'C', 12.5);
  db.prepare('INSERT INTO spot_prices (timestamp,price) VALUES (?,?)').run('2026-03-06T07:59:00.000Z', 2500);
  db.prepare('INSERT INTO spot_prices (timestamp,price) VALUES (?,?)').run('2026-03-07T07:59:00.000Z', 9999);
  const dashboard = loadTs('dashboard/src/lib/db.ts', {
    'better-sqlite3': function() { return db; },
    '../../../bot/economic-events': require('../bot/economic-events'),
    './strategy-config': { BOT_CONFIG: require('../bot/config.json'), CONFIG_PATH: path.join(root, 'bot/config.json') },
  }, { process: { env: { DATA_DIR: fixtureDir }, cwd: () => root } });
  assert.equal(dashboard.getAvgCallPremium7d()[0].avg_premium, 12.5);
  assert.equal(dashboard.getSpotPricesAtOrBefore(['2026-03-06T08:00:00.000Z'], 15 * 60_000)[0].price, 2500);
  assert.equal(dashboard.getSpotPricesAtOrBefore(['2026-03-06T09:00:00.000Z'], 15 * 60_000).length, 0);
  db.close();
  fs.rmSync(fixtureDir, { recursive: true, force: true });
});
