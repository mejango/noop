'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const vm = require('node:vm');
const { readAdvisoryMarketSnapshot } = require('../bot/advisory-market-snapshot');
const { declaration, SCRIPT_SOURCE } = require('./helpers/load-production');

const asOf = '2026-09-15T18:55:15.381Z';
const now = () => Date.parse(asOf);
const put = 'ETH-20261127-1600-P';
const call = 'ETH-20260925-2800-C';
const held = 'ETH-20260918-2800-C';
const outside = 'ETH-20261030-1600-P'; // Under 45 DTE at this observation.
const ticker = (delta, ask = 20, bid = 18) => ({
  a: ask, b: bid, M: 19, option_pricing: { d: delta }, quote_received_at: asOf,
});
function fixture(overrides = {}) {
  const requests = [];
  const rows = { [put]: ticker(-0.06), [call]: ticker(0.08), [held]: ticker(0.02), [outside]: ticker(-0.06) };
  return { requests, input: {
    fetchSpot: async () => 2367.7,
    fetchInstruments: async () => Object.keys(rows).map(instrument_name => ({ instrument_name })),
    fetchPositions: async () => [],
    fetchTickers: async expiry => {
      requests.push(expiry);
      return Object.fromEntries(Object.entries(rows).filter(([name]) => name.split('-')[1] === expiry));
    }, now, ...overrides,
  } };
}

test('fresh advisory fetches only entry-window expiries plus held options, once each', async () => {
  const positions = [
    { instrument_name: held, amount: '1', direction: 'short' },
    { instrument_name: put, amount: '2', direction: 'long' },
    { instrument_name: 'ETH-PERP', amount: '0.1', direction: 'long' },
  ];
  const f = fixture({ fetchPositions: async () => positions });
  const result = await readAdvisoryMarketSnapshot(f.input);
  assert.deepEqual(f.requests, ['20260918', '20260925', '20261127']);
  assert.deepEqual(Object.keys(result.tickerMap).sort(), [put, call, held].sort());
  assert.deepEqual(result.positions, positions);
  assert.equal(result.marketTimestamp, asOf);
  assert.equal(result.spotPrice, 2367.7);
  assert.equal(result.quoteAvailability.put.status, 'available');
  assert.equal(result.quoteAvailability.call.status, 'available');
  assert.equal(result.quoteAvailability.put.coverage_status, 'complete');
  assert.deepEqual(result.quoteAvailability.put.expected_instruments, [put]);
});

test('a second read cannot reuse a prior quote when the current feed is empty', async () => {
  const f = fixture();
  const initial = await readAdvisoryMarketSnapshot(f.input);
  f.input.fetchTickers = async () => ({});
  const empty = await readAdvisoryMarketSnapshot(f.input);
  assert.ok(initial.tickerMap[put].a > 0);
  assert.deepEqual(empty.tickerMap, {});
  assert.equal(empty.quoteAvailability.put.status, 'unknown');
  assert.equal(empty.quoteAvailability.put.coverage_status, 'partial');
  assert.equal(empty.quoteAvailability.put.missing_expected_ticker_count, 1);
});

test('zero quotes remain an explicit unavailable observation rather than a failed fetch', async () => {
  const f = fixture({ fetchTickers: async expiry => expiry === '20261127'
    ? { [put]: ticker(-0.06, 0, 0) } : { [call]: ticker(0.08, 0, 0) } });
  const result = await readAdvisoryMarketSnapshot(f.input);
  for (const side of ['put', 'call']) {
    assert.equal(result.quoteAvailability[side].status, 'quotes_unavailable');
    assert.equal(result.quoteAvailability[side].coverage_status, 'complete');
    assert.equal(result.quoteAvailability[side].in_dte_delta_count, 1);
    assert.equal(result.quoteAvailability[side].quoted_count, 0);
  }
});

test('failed account, metadata or expiry requests reject the entire refresh', async () => {
  for (const name of ['fetchSpot', 'fetchInstruments', 'fetchPositions', 'fetchTickers']) {
    const f = fixture({ [name]: async () => { throw new Error(`${name} unavailable`); } });
    await assert.rejects(readAdvisoryMarketSnapshot(f.input), new RegExp(`${name} unavailable`));
  }
});

test('malformed spot, positions, metadata and expiry maps cannot masquerade as empty coverage', async () => {
  const cases = [
    ['fetchSpot', null], ['fetchSpot', 0], ['fetchSpot', NaN],
    ['fetchInstruments', []], ['fetchInstruments', {}],
    ['fetchInstruments', [{ instrument_name: 'invalid' }]],
    ['fetchPositions', null], ['fetchPositions', {}],
    ['fetchPositions', [{ instrument_name: held, direction: 'short', amount: null }]],
    ['fetchPositions', [{ instrument_name: held, direction: 'short', amount: 'bad' }]],
    ['fetchPositions', [{ instrument_name: '', direction: 'short', amount: 1 }]],
    ['fetchTickers', null], ['fetchTickers', []],
    ['fetchTickers', { [put]: null }], ['fetchTickers', { invalid: ticker(-0.06) }],
  ];
  for (const [name, value] of cases) {
    await assert.rejects(readAdvisoryMarketSnapshot(fixture({ [name]: async () => value }).input), /Advisory refresh:/);
  }
  const f = fixture({ fetchTickers: async () => ({ [held]: ticker(0.02) }) });
  await assert.rejects(readAdvisoryMarketSnapshot(f.input), /malformed quotes/);
});

function productionAdapter(name, response) {
  const requests = [];
  const bindings = {
    axios: { post: async (...args) => { requests.push(args); if (response instanceof Error) throw response; return response; } },
    API_URL: { GET_TICKERS: '/tickers', GET_INSTRUMENTS: '/instruments' },
    console: { log() {}, error() {} },
  };
  const fn = vm.compileFunction(`${declaration(SCRIPT_SOURCE, name)}; return ${name};`, Object.keys(bindings))(...Object.values(bindings));
  return { fn, requests };
}

test('real ticker adapter propagates strict read failures and retains ordinary-loop fallback', async () => {
  for (const response of [new Error('transport unavailable'), { data: {} }, { data: { result: { tickers: [] } } }]) {
    const { fn, requests } = productionAdapter('fetchTickersByExpiry', response);
    await assert.rejects(fn('20261127', { throwOnError: true }));
    assert.equal(requests[0][2].timeout, 15000);
    assert.deepEqual(await fn('20261127'), {});
  }
  const { fn } = productionAdapter('fetchTickersByExpiry', { data: { result: { tickers: { [put]: ticker(-0.06) } } } });
  const result = await fn('20261127', { throwOnError: true });
  assert.equal(result[put].a, 20);
  assert.ok(Number.isFinite(Date.parse(result[put].quote_received_at)));
  for (const row of [null, [], 7, 'unavailable']) {
    const malformed = productionAdapter('fetchTickersByExpiry', { data: { result: { tickers: { [put]: row } } } });
    await assert.rejects(malformed.fn('20261127', { throwOnError: true }), /Malformed ticker response/);
  }
});

test('real instrument adapter propagates strict response failures rather than inventing an empty universe', async () => {
  for (const response of [new Error('transport unavailable'), { data: {} }, { data: { result: {} } }]) {
    const { fn, requests } = productionAdapter('fetchAndFilterInstruments', response);
    await assert.rejects(fn(null, { throwOnError: true }));
    assert.equal(requests[0][2].timeout, 15000);
    assert.deepEqual(await fn(null), { putCandidates: [], callCandidates: [] });
  }
});
