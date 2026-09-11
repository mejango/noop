const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const { test } = require('node:test');
const Database = require('better-sqlite3');
const funding = require('../bot/funding-rates');
const ts = require('../dashboard/node_modules/typescript');
const { loadProduction } = require('./helpers/load-production');

const { FUNDING_SYMBOL, FUNDING_EXCHANGE, fundingRatesFromTickerResult, summarizeFundingRates } = funding;
const observedAt = '2026-09-11T19:00:00.000Z';
const quiet = { log() {}, warn() {}, error() {} };

// The saved official V2 OpenAPI defines result.tickers as a dictionary and
// TickerSlimSchema.f as the nullable decimal hourly funding rate.
function tickerResult(rate, timestamp = observedAt) {
  return { tickers: { 'ETH-PERP': { f: rate, t: Date.parse(timestamp) } } };
}

function actualCollector(post) {
  return loadProduction(['fetchFundingRates'], {
    bindings: { ...funding, API_URL: { GET_TICKERS: 'fixture:tickers' }, axios: { post } },
  }).fetchFundingRates;
}

function actualSummary(rows) {
  return loadProduction(['summarizeSentimentWindowForLLM'], { bindings: { ...funding } })
    .summarizeSentimentWindowForLLM('24h', { fundingRates: rows }).funding_rate;
}

function assertSummary(actual, expected) {
  const { avg, ...other } = actual;
  const { avg: expectedAverage, ...expectedOther } = expected;
  assert.deepEqual(other, expectedOther);
  if (expectedAverage === null) assert.equal(avg, null);
  else assert.ok(Number.isFinite(avg) && Math.abs(avg - expectedAverage) < 1e-15, `average ${avg} differs from ${expectedAverage}`);
}

function memoryDatabase(t) {
  const filename = path.join(__dirname, '../bot/db.js');
  const localRequire = createRequire(filename);
  const sqlite = new Database(':memory:');
  t.after(() => { if (sqlite.open) sqlite.close(); });
  const sandbox = {
    __dirname: path.dirname(filename), process: { env: { DATA_DIR: os.tmpdir() } },
    console: quiet, module: { exports: {} },
    require(name) {
      if (name === 'better-sqlite3') return function InMemoryDatabase() { return sqlite; };
      return localRequire(name);
    },
  };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), sandbox, { filename });
  return sandbox.module.exports;
}

function dashboardDatabase(sqlite) {
  const filename = path.join(__dirname, '../dashboard/src/lib/db.ts');
  const localRequire = createRequire(filename);
  const code = ts.transpileModule(fs.readFileSync(filename, 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, esModuleInterop: true },
  }).outputText;
  const exported = {};
  vm.runInNewContext(code, {
    exports: exported,
    process: { env: { DATA_DIR: os.tmpdir() }, cwd: () => path.join(__dirname, '..') },
    console: quiet,
    require(name) {
      if (name === 'better-sqlite3') return function FixtureDatabase(_filename, options) {
        assert.equal(options.readonly, true);
        return sqlite;
      };
      if (name === './strategy-config') return { BOT_CONFIG: require('../bot/config.json') };
      return localRequire(name);
    },
  }, { filename });
  return exported;
}

test('documented V2 ticker dictionary produces the canonical Derive ETH perpetual funding observation', () => {
  assert.equal(FUNDING_SYMBOL, 'ETH-PERP');
  assert.equal(FUNDING_EXCHANGE, 'derive');
  const rows = fundingRatesFromTickerResult(tickerResult('0.000125'), observedAt);
  assert.deepEqual(rows, [{ timestamp: observedAt, exchange: 'derive', symbol: 'ETH-PERP', rate: 0.000125 }]);
});

test('funding parser preserves real zero and negative funding and retains legacy response support', () => {
  for (const rate of [0, '0', -0.000125, '-0.000125']) {
    assert.equal(fundingRatesFromTickerResult(tickerResult(rate), observedAt)[0].rate, Number(rate));
    assert.equal(fundingRatesFromTickerResult([
      { instrument_name: 'ETH-PERP', funding_rate_info: { funding_rate: rate } },
    ], observedAt)[0].rate, Number(rate));
  }
});

test('missing and malformed rates never become manufactured zero observations', () => {
  for (const rate of [undefined, null, '', ' ', 'not-a-number', NaN, Infinity, -Infinity, 'Infinity', false, true, [], {}]) {
    assert.deepEqual(fundingRatesFromTickerResult(tickerResult(rate), observedAt), [], `invalid rate ${String(rate)}`);
  }
  for (const result of [null, undefined, {}, { tickers: {} }, { tickers: null }, []]) {
    assert.deepEqual(fundingRatesFromTickerResult(result, observedAt), []);
  }
});

test('another perpetual or a similarly named instrument cannot substitute for ETH-PERP', () => {
  assert.deepEqual(fundingRatesFromTickerResult({ tickers: {
    'BTC-PERP': { f: '0.5' }, 'ETH-PERP-OTHER': { f: '0.9' },
  } }, observedAt), []);
  assert.deepEqual(fundingRatesFromTickerResult([
    { instrument_name: 'BTC-PERP', funding_rate_info: { funding_rate: '0.5' } },
  ], observedAt), []);
});

test('source snapshot time is preserved while impossible timestamps and ambiguous identities are rejected', () => {
  const older = new Date(Date.parse(observedAt) - 1000).toISOString();
  assert.equal(fundingRatesFromTickerResult(tickerResult('0.0001', older), observedAt)[0].timestamp, older);
  assert.equal(fundingRatesFromTickerResult({ tickers: { 'ETH-PERP': { f: '0.0001' } } }, observedAt)[0].timestamp, observedAt);
  for (const timestamp of [Date.parse(observedAt) + 1, 0, -1, '', ' ', 'invalid', Infinity, false]) {
    assert.deepEqual(fundingRatesFromTickerResult({ tickers: { 'ETH-PERP': { f: '0.0001', t: timestamp } } }, observedAt), []);
  }
  assert.deepEqual(fundingRatesFromTickerResult({ tickers: {
    'ETH-PERP': { f: '0.0001' }, 'ticker.ETH-PERP.100': { f: '0.0002' },
  } }, observedAt), []);
});

test('the actual collector consumes the official ticker response and requests ETH perpetual data', async () => {
  let calls = 0;
  const fetchFundingRates = actualCollector(async (url, params, options) => {
    calls++;
    assert.equal(url, 'fixture:tickers');
    assert.equal(params.instrument_type, 'perp');
    assert.equal(params.currency, 'ETH');
    assert.ok(Number.isFinite(options.timeout) && options.timeout > 0);
    return { data: { id: 'fixture', result: tickerResult('0.000125', new Date().toISOString()) } };
  });
  const rows = await fetchFundingRates();
  assert.equal(calls, 1);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].rate, 0.000125);
  assert.equal(rows[0].symbol, 'ETH-PERP');
  assert.equal(rows[0].exchange, 'derive');
  assert.ok(Number.isFinite(Date.parse(rows[0].timestamp)));
});

test('the actual collector leaves unavailable or invalid API funding empty', async () => {
  for (const data of [
    { error: { message: 'venue unavailable' } },
    { error: { message: 'venue unavailable' }, result: tickerResult('0.0001') },
    { result: tickerResult(null) },
    { result: tickerResult('not-a-number') },
  ]) {
    assert.deepEqual(await actualCollector(async () => ({ data }))(), []);
  }
  assert.deepEqual(await actualCollector(async () => { throw new Error('fixture transport failure'); })(), []);
});

test('funding summaries accept raw rates and actual database hourly avg_rate rows', () => {
  const expected = { current: 0.0003, avg: 0.0002, trend: 'rising', samples: 2 };
  for (const rows of [
    [{ rate: 0.0001 }, { rate: 0.0003 }],
    [{ avg_rate: '0.0001' }, { avg_rate: '0.0003' }],
  ]) {
    assertSummary(summarizeFundingRates(rows), expected);
    assertSummary(actualSummary(rows), expected);
  }
});

test('unknown funding rows are excluded from averages and cannot imply a known current rate', () => {
  const rows = [{ avg_rate: 0.0001 }, { avg_rate: null }, { avg_rate: '' }, { avg_rate: 0.0003 }];
  assertSummary(summarizeFundingRates(rows), { current: 0.0003, avg: 0.0002, trend: 'rising', samples: 2 });
  const missingLatest = [...rows, { avg_rate: null }];
  const expected = { current: null, avg: 0.0002, trend: 'unknown', samples: 2 };
  assertSummary(summarizeFundingRates(missingLatest), expected);
  assertSummary(actualSummary(missingLatest), expected);
});

test('funding summaries preserve actual zero and negative rates but reject nonnumeric values', () => {
  const expected = { current: -0.0001, avg: 0, trend: 'declining', samples: 3 };
  assert.deepEqual(summarizeFundingRates([{ rate: 0 }, { rate: 0.0001 }, { rate: -0.0001 }]), expected);
  assert.deepEqual(actualSummary([{ avg_rate: 0 }, { avg_rate: 0.0001 }, { avg_rate: -0.0001 }]), expected);
  for (const value of [null, undefined, '', ' ', false, true, [], {}, NaN, Infinity, 'invalid']) {
    const unknown = { current: null, avg: null, trend: 'unknown', samples: 0 };
    assert.deepEqual(summarizeFundingRates([{ rate: value }]), unknown);
    assert.deepEqual(summarizeFundingRates([{ avg_rate: value }]), unknown);
  }
  assert.deepEqual(summarizeFundingRates([]), { current: null, avg: null, trend: 'unknown', samples: 0 });
});

test('collector to production SQLite default getters to actual LLM summary retains funding and isolates its source', async (t) => {
  const db = memoryDatabase(t);
  const fetchFundingRates = actualCollector(async () => ({
    data: { result: tickerResult('0.0003', new Date().toISOString()) },
  }));
  const collected = await fetchFundingRates();
  assert.equal(collected.length, 1);
  const now = Date.parse(collected[0].timestamp);
  const earlier = { ...collected[0], timestamp: new Date(now - 3600000).toISOString(), rate: 0.0001 };
  db.insertFundingRates([
    earlier, ...collected,
    { ...collected[0], exchange: 'unrelated-exchange', timestamp: new Date(now + 1000).toISOString(), rate: 0.5 },
    { ...collected[0], symbol: 'ETHUSDT', timestamp: new Date(now + 2000).toISOString(), rate: 0.9 },
  ]);
  const since = new Date(now - 2 * 3600000).toISOString();
  assert.equal(db.db.prepare('SELECT COUNT(*) count FROM funding_rates').get().count, 4);
  assert.equal(db.getFundingRateLatest().rate, 0.0003);
  assert.ok(Math.abs(db.getFundingRateAvg24h() - 0.0002) < 1e-12);
  for (const rows of [db.getFundingRates(since), db.getFundingRatesHourly(since)]) {
    assert.equal(rows.length, 2);
    assert.deepEqual(rows.map((row) => row.avg_rate), [0.0001, 0.0003]);
    assertSummary(actualSummary(rows), { current: 0.0003, avg: 0.0002, trend: 'rising', samples: 2 });
  }
  const changesBeforeRead = db.db.prepare('SELECT total_changes() count').get().count;
  const dashboard = dashboardDatabase(db.db);
  assert.equal(dashboard.getFundingRateLatest().rate, 0.0003);
  assert.ok(Math.abs(dashboard.getFundingRateAvg24h() - 0.0002) < 1e-12);
  for (const rows of [dashboard.getFundingRates(since), dashboard.getFundingRatesHourlySeries(since)]) {
    assert.equal(rows.length, 2);
    assert.deepEqual(Array.from(rows, (row) => row.rate), [0.0001, 0.0003]);
    assertSummary(actualSummary(rows), { current: 0.0003, avg: 0.0002, trend: 'rising', samples: 2 });
  }
  assert.equal(db.db.prepare('SELECT total_changes() count').get().count, changesBeforeRead, 'dashboard getters do not rewrite historical observations');
  db.insertFundingRates(collected);
  assert.equal(db.db.prepare('SELECT COUNT(*) count FROM funding_rates').get().count, 4, 'repeat collection does not duplicate an exact observation');
});
