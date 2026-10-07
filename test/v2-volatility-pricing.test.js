'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const ts = require('../dashboard/node_modules/typescript');
const mod = { exports: {} };
new Function('module', 'exports', ts.transpileModule(
  fs.readFileSync(`${__dirname}/../dashboard/src/lib/volatility-pricing.ts`, 'utf8'),
  { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } },
).outputText)(mod, mod.exports);
const { ivAtOffset, ivAtTenor, buildVolatilityPricing, summarizePricingSide, nearestVolatilityInstruments, pricingColor, sampleHourly } = mod.exports;
const DAY = 86400000;
const HOUR = 3600000;
const now = Date.parse('2026-10-01T12:00:00Z');
function expiry(dte, iv = 50, spot = 2000) {
  return { dte, expiry: (now + dte * DAY) / 1000, spot, forward: spot, points:
    [-0.2, -0.15, -0.1, -0.05, 0, 0.05, 0.1, 0.15, 0.2].map(offset => ({
      strike: spot * (1 + offset), type: offset < 0 ? 'P' : 'C', delta: 0.25,
      iv, bidIv: iv - 1, askIv: iv + 1, oi: 10,
    })) };
}
const frame = (at = now, iv = 50, spot = 2000) => ({ at, expiries: [1, 3, 7, 14, 30, 45, 60, 90, 180, 365].map(d => expiry(d, iv, spot)) });
const history = (count = 30, iv = 50) => Array.from({ length: count * 24 }, (_, i) => frame(now - (i + 1) * HOUR, iv));

test('bid squares stay red and ask squares stay green, with brightness showing favorable quotes', () => {
  assert.equal(pricingColor(0, 'bid'), 'hsl(0 55% 12%)');
  assert.equal(pricingColor(100, 'bid'), 'hsl(0 55% 37%)');
  assert.equal(pricingColor(0, 'ask'), 'hsl(165 55% 37%)');
  assert.equal(pricingColor(100, 'ask'), 'hsl(165 55% 12%)');
  assert.equal(pricingColor(0, 'mark'), 'hsl(165 55% 27%)');
  assert.equal(pricingColor(50, 'mark'), 'hsl(165 0% 18%)');
  assert.equal(pricingColor(100, 'mark'), 'hsl(38 55% 27%)');
  for (const side of ['mark', 'bid', 'ask']) assert.equal(pricingColor(null, side), '#252525');
});

test('constant maturity interpolates total variance, refusing unbracketed horizons', () => {
  const expiries = [expiry(10, 40), expiry(20, 60)];
  assert.equal(ivAtTenor(expiries, 15, 0), Math.sqrt((40 ** 2 * 10 + 60 ** 2 * 20) / 2 / 15));
  assert.equal(ivAtTenor(expiries, 10, 0), 40);
  assert.equal(ivAtTenor(expiries, 7, 0), null);
  assert.equal(ivAtTenor(expiries, 30, 0), null);
});

test('missing, crossed and wide quotes are not interpolated through or extrapolated', () => {
  const e = expiry(30);
  assert.equal(ivAtOffset(e, 0.5), null);
  e.points[4].askIv = null;
  assert.equal(ivAtOffset(e, 0), null);
  assert.equal(ivAtOffset(e, 0.025), null);
  e.points[4].askIv = 40;
  assert.equal(ivAtOffset(e, 0), null);
  e.points[4].askIv = 100;
  assert.equal(ivAtOffset(e, 0), null);
  e.points[4].bidIv = 50.004;
  e.points[4].askIv = 50.003;
  for (const side of ['mark', 'bid', 'ask']) {
    assert.equal(ivAtOffset(e, 0, side), null, 'rounding cannot hide a crossed raw market');
    assert.equal(ivAtOffset(e, 0.025, side), null);
  }
});

test('cheap, expensive and tied markets produce scores 0, 100 and 50', () => {
  assert.equal(buildVolatilityPricing(frame(now, 40), history()).score, 0);
  assert.equal(buildVolatilityPricing(frame(now, 60), history()).score, 100);
  assert.equal(buildVolatilityPricing(frame(), history()).score, 50);
  assert.equal(buildVolatilityPricing(frame(), history()).provisional, false);
});

test('strike comparison follows spot distance and maturity despite contract roll', () => {
  const past = history().map(f => ({ ...f, expiries: [2, 9, 20, 45, 80, 110].map(d => expiry(d, 50, 1000)) }));
  assert.equal(buildVolatilityPricing(frame(now, 40, 3000), past).score, 0);
  assert.equal(buildVolatilityPricing(frame(now, 40, 3000), past).cells.find(c => c.offset === -0.1).strike, 2700);
});

test('hourly sampling keeps the last observation per hour and excludes incomplete / old data', () => {
  const past = history(1);
  const dense = Array.from({ length: 100 }, (_, i) => frame(now - HOUR + i * 1000, 30));
  const result = buildVolatilityPricing(frame(now, 40), [
    ...past, ...dense, frame(now - HOUR + 59 * 60000, 50),
    frame(now + DAY, 10), frame(now - 31 * DAY, 10), frame(now + 1000, 10),
  ]);
  assert.equal(result.historySamples, 24);
  assert.equal(result.score, 0);
  assert.equal(result.provisional, true);
});

test('insufficient history or narrow coverage never becomes a broad market score', () => {
  const empty = buildVolatilityPricing(frame(), []);
  assert.equal(empty.score, null);
  assert.equal(empty.cells[0].iv, 50);
  assert.equal(buildVolatilityPricing(frame(), history(1).slice(0, 23)).score, null);
  assert.equal(empty.currentIv, 50);
  const narrow = { at: now, expiries: [expiry(7)] };
  const result = buildVolatilityPricing(narrow, history());
  assert.equal(result.score, null);
  assert.equal(result.measured, 9);
});


test('five recorded days show a provisional score instead of a blank meter', () => {
  const result = buildVolatilityPricing(frame(now, 40), history(5));
  assert.equal(result.score, 0);
  assert.equal(result.label, 'Cheap');
  assert.equal(result.provisional, true);
  assert.equal(result.measured, 81);
  assert.equal(result.currentIv, 40);
  assert.equal(buildVolatilityPricing(frame(), history(2)).score, 50);
  assert.equal(buildVolatilityPricing(frame(), history(2)).provisional, true);
});

test('hourly percentiles resolve values between the old 20-point steps', () => {
  const past = Array.from({ length: 120 }, (_, i) => frame(now - (i + 1) * HOUR, 30 + i / 10));
  const result = buildVolatilityPricing(frame(now, 31.05), past);
  const cell = result.cells.find(c => c.dte === 30 && c.offset === 0);
  assert.ok(Math.abs(cell.percentile - 11 / 120 * 100) < 1e-9);
  assert.equal(cell.samples, 120);
  assert.equal(result.total, 81);
});

test('bid and ask priciness each use their own historical quote side', () => {
  const past = history(5);
  past.forEach(f => f.expiries.forEach(e => e.points.forEach(p => { p.bidIv = 40; p.askIv = 60; })));
  const current = frame();
  current.expiries.forEach(e => e.points.forEach(p => { p.bidIv = 47; p.askIv = 55; }));
  const cell = buildVolatilityPricing(current, past).cells.find(c => c.dte === 30 && c.offset === 0);
  assert.equal(cell.percentile, 50);
  assert.equal(cell.bid.iv, 47);
  assert.equal(cell.bid.percentile, 100);
  assert.equal(cell.ask.iv, 55);
  assert.equal(cell.ask.percentile, 0);
  assert.equal(cell.bid.history.at(-1).percentile, 100);
  assert.equal(cell.ask.history.at(-1).percentile, 0);
});

test('cheap marks and bids can coexist with expensive asks, with each summary traceable to its own cells', () => {
  const past = Array.from({ length: 100 }, (_, i) => frame(now - (i + 1) * HOUR, 48 + i / 10));
  const current = frame(now, 49.25);
  current.expiries.forEach(e => e.points.forEach(p => { p.bidIv = 40; p.askIv = 59; }));
  const result = buildVolatilityPricing(current, past);
  assert.equal(result.score, 13);
  assert.equal(result.label, 'Cheap');
  assert.equal(result.currentIv, 49.25);
  for (const [side, expected] of [['mark', 13], ['bid', 0], ['ask', 100]]) {
    const summary = summarizePricingSide(result.cells, side);
    assert.equal(summary.score, expected);
    assert.equal(summary.measured, 25);
    assert.equal(summary.total, 25);
    assert.equal(summary.label, side === 'ask' ? 'Expensive' : 'Cheap');
  }
});

test('each summary requires its own broad rated coverage but can show unrated current IV', () => {
  const result = buildVolatilityPricing(frame(), history(2));
  const cells = result.cells.map(c => ({ ...c,
    ask: { ...c.ask, percentile: c.dte <= 30 ? c.ask.percentile : null },
    bid: { ...c.bid, percentile: c.dte <= 14 ? c.bid.percentile : null },
  }));
  const mark = summarizePricingSide(cells, 'mark');
  const ask = summarizePricingSide(cells, 'ask');
  const bid = summarizePricingSide(cells, 'bid');
  assert.equal(mark.score, 50);
  assert.equal(mark.measured, 25);
  assert.equal(ask.score, 50);
  assert.equal(ask.measured, 15);
  assert.equal(bid.score, null);
  assert.equal(bid.measured, 10);
  assert.equal(bid.total, 25);
  assert.equal(bid.currentIv, 49);
  const noHistory = buildVolatilityPricing(frame(), []);
  for (const side of ['mark', 'bid', 'ask']) {
    const summary = summarizePricingSide(noHistory.cells, side);
    assert.equal(summary.score, null);
    assert.equal(summary.measured, 0);
    assert.equal(summary.currentIv, side === 'mark' ? 50 : side === 'bid' ? 49 : 51);
  }
});

test('live and compact snapshots give identical ranks for an unchanged market at recorder precision', () => {
  const smile = { exports: {} };
  new Function('module', 'exports', ts.transpileModule(
    fs.readFileSync(`${__dirname}/../dashboard/src/lib/vol-smile.ts`, 'utf8'),
    { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } },
  ).outputText)(smile, smile.exports);
  const { buildExpiry, fromCompact } = smile.exports;
  const { buildSmileRows } = require('../bot/iv-smile');
  const quotes = (at, compact) => ({ at, expiries: [2, 10, 20, 40, 80, 120, 200].map(dte => {
    const expiry = (at + dte * DAY) / 1000;
    const date = new Date(expiry * 1000).toISOString().slice(0, 10).replace(/-/g, '');
    const tickers = Object.fromEntries([1500, 1750, 2000, 2250, 2500].map(strike => {
      const type = strike < 2000 ? 'P' : 'C';
      const iv = 0.50001 + (strike - 2000) / 100000;
      return [`ETH-${date}-${strike}-${type}`, {
        I: '2003', option_pricing: { f: '2000', d: type === 'P' ? '-0.25' : '0.25',
          i: String(iv), bi: String(iv - 0.01), ai: String(iv + 0.01) },
      }];
    }));
    return compact ? fromCompact(buildSmileRows(tickers, { [date]: expiry })[0], at)
      : buildExpiry(expiry, tickers, at);
  }) });
  const past = Array.from({ length: 48 }, (_, i) => quotes(now - (i + 1) * HOUR, true));
  const live = buildVolatilityPricing(quotes(now, false), past);
  const recorded = buildVolatilityPricing(quotes(now, true), past);
  assert.equal(live.score, 50);
  assert.equal(recorded.score, 50);
  const ratings = data => data.cells.map(c => [c.iv, c.percentile, c.bid.iv, c.bid.percentile, c.ask.iv, c.ask.percentile]);
  assert.deepEqual(ratings(live), ratings(recorded));
  const interpolated = live.cells.find(c => c.dte === 30 && c.offset === 0.05);
  assert.equal(interpolated.percentile, 50);
  assert.equal(interpolated.bid.percentile, 50);
  assert.equal(interpolated.ask.percentile, 50);
});

test('a missing ask cannot hide a valid bid or manufacture an ask percentile', () => {
  const current = frame();
  const past = history(2);
  [current, ...past].forEach(f => f.expiries.forEach(e => e.points.forEach(p => { p.askIv = null; })));
  const cell = buildVolatilityPricing(current, past).cells.find(c => c.dte === 30 && c.offset === 0);
  assert.equal(cell.bid.iv, 49);
  assert.equal(cell.bid.samples, 48);
  assert.equal(cell.bid.percentile, 50);
  assert.equal(cell.ask.iv, null);
  assert.equal(cell.ask.samples, 0);
  assert.equal(cell.ask.percentile, null);
  assert.equal(cell.iv, null);
});

test('bid and ask maturity interpolation uses the selected side variance', () => {
  const expiries = [expiry(10, 40), expiry(20, 60)];
  assert.equal(ivAtTenor(expiries, 15, 0, 'bid'), Math.sqrt((39 ** 2 * 10 + 59 ** 2 * 20) / 2 / 15));
  assert.equal(ivAtTenor(expiries, 15, 0, 'ask'), Math.sqrt((41 ** 2 * 10 + 61 ** 2 * 20) / 2 / 15));
});

test('history path stays chronological, bounded and ends at the exact current color', () => {
  const result = buildVolatilityPricing(frame(now, 30), history(30));
  const cell = result.cells[0];
  assert.ok(cell.history.length <= 49);
  assert.ok(cell.history.every((p, i, all) => i === 0 || Date.parse(p.at) > Date.parse(all[i - 1].at)));
  assert.equal(cell.history.at(-1).iv, 30);
  assert.equal(cell.history.at(-1).percentile, cell.percentile);
  assert.equal(pricingColor(cell.history.at(-1).percentile), pricingColor(cell.percentile));
});

test('history path retains missing hourly quotes as gaps', () => {
  const past = history(1);
  const missing = past[12];
  missing.expiries.find(e => e.dte === 30).points.forEach(p => { p.askIv = null; });
  past.push(frame(now - 25 * HOUR));
  const result = buildVolatilityPricing(frame(), past);
  const cell = result.cells.find(c => c.dte === 30 && c.offset === 0);
  assert.equal(cell.samples, 24);
  const gap = cell.history.find(p => p.at === new Date(missing.at).toISOString());
  assert.equal(gap.iv, null);
  assert.equal(gap.percentile, null);
  assert.equal(pricingColor(gap.percentile), '#252525');
});

test('missing recorder hours remain gaps rather than being stretched into observations', () => {
  const past = history(1);
  const removed = past.splice(12, 1)[0];
  const result = buildVolatilityPricing(frame(), past);
  const cell = result.cells.find(c => c.dte === 30 && c.offset === 0);
  assert.equal(cell.samples, 23);
  assert.ok(cell.history.some(p => p.at === new Date(removed.at).toISOString() && p.iv === null));
});

test('expanding the exploration grid preserves the headline meter comparison', () => {
  const current = frame();
  current.expiries.forEach(e => e.points.forEach(p => {
    if (Math.abs(p.strike / e.spot - 1) > 0.11 || e.dte > 90) {
      p.iv = 100; p.bidIv = 99; p.askIv = 101;
    }
  }));
  const result = buildVolatilityPricing(current, history(5));
  assert.equal(result.score, 50);
  assert.equal(result.currentIv, 50);
  assert.equal(result.measured, 81);
  assert.equal(result.total, 81);
  for (const side of ['mark', 'bid', 'ask']) {
    const summary = summarizePricingSide(result.cells, side);
    assert.equal(summary.score, 50);
    assert.equal(summary.currentIv, side === 'mark' ? 50 : side === 'bid' ? 49 : 51);
  }
  const excludedTenors = result.cells.map(c => [1, 3, 45, 180].includes(c.dte)
    ? { ...c, iv: 100, percentile: 100, bid: { ...c.bid, iv: 100, percentile: 100 }, ask: { ...c.ask, iv: 100, percentile: 100 } }
    : c);
  for (const side of ['mark', 'bid', 'ask']) {
    assert.equal(summarizePricingSide(excludedTenors, side).score, 50);
  }
});

function quotedExpiry(dte) {
  const e = expiry(dte);
  e.points.forEach(p => { p.name = `ETH-${dte}D-${p.strike}-${p.type}`; p.askPrice = 12; p.askAmount = 3; p.bidPrice = 11; p.bidAmount = 2; });
  return e;
}

test('cell click matches actual buyable contracts by maturity then strike and option side', () => {
  const expiries = [quotedExpiry(7), quotedExpiry(29), quotedExpiry(60)];
  const [call] = nearestVolatilityInstruments(expiries, 30, 2100, 0.05);
  assert.equal(call.name, 'ETH-29D-2100-C');
  assert.equal(call.ask, 12);
  assert.equal(call.askIv, 51);
  assert.equal(call.askAmount, 3);
  assert.equal(call.bid, 11);
  assert.equal(call.bidAmount, 2);
  assert.equal(call.bidIv, 49);
  const [put] = nearestVolatilityInstruments(expiries, 30, 1900, -0.05);
  assert.equal(put.name, 'ETH-29D-1900-P');
  assert.deepEqual(nearestVolatilityInstruments(expiries, 30, 2000, 0).map(p => p.type), ['P', 'C']);
  expiries[1].points.find(p => p.strike === 2100).askAmount = 0;
  assert.notEqual(nearestVolatilityInstruments(expiries, 30, 2100, 0.05)[0].name, call.name);
  assert.deepEqual(nearestVolatilityInstruments([expiry(30)], 30, 2100, 0.05), []);
});

// Exercise the production endpoint with isolated upstream/data-store adapters.
function endpoint({ chain = { at: Date.now(), expiries: frame().expiries }, timestamps = [], fail = false, pending = false, cachedChain, snapshotRows = [], compressed = false } = {}) {
  let picked;
  const route = { exports: {} };
  const source = fs.readFileSync(`${__dirname}/../dashboard/src/app/api/volatility-pricing/route.ts`, 'utf8');
  new Function('module', 'exports', 'require', ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText)(route, route.exports, name => {
    if (name === 'next/server') return { NextResponse: { json: (body, init) => compressed ? new Response(JSON.stringify(body), { status: init?.status ?? 200 }) : ({ body, status: init?.status ?? 200 }) } };
    if (name === '@/lib/response-cache') {
      if (!compressed) return { cachedJsonRoute: (_req, _key, loader) => loader() };
      const cache = { exports: {} };
      const code = ts.transpileModule(fs.readFileSync(`${__dirname}/../dashboard/src/lib/response-cache.ts`, 'utf8'), { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
      new Function('module', 'exports', 'require', code)(cache, cache.exports, require);
      return cache.exports;
    }
    if (name === '@/lib/db') return {
      getSmileSnapshotTimestamps: () => timestamps,
      getSmileSnapshots: ts => { picked = ts; return []; },
      getSmileSnapshotNear: () => snapshotRows,
    };
    if (name === '@/lib/smile-chain') return {
      getCachedChain: () => cachedChain === undefined ? (fail || pending ? null : chain) : cachedChain,
      getChain: async () => { if (pending) return new Promise(() => {}); if (fail) throw new Error('Upstream unavailable'); return chain; },
    };
    if (name === '@/lib/vol-smile') return { fromCompact: row => row.decoded };
    if (name === '@/lib/volatility-pricing') return mod.exports;
    throw new Error(`Unexpected module: ${name}`);
  });
  return { GET: route.exports.GET, picked: () => picked };
}

test('endpoint selects the latest completed hourly snapshots, including earlier hours today', async () => {
  const at = Date.now();
  const lastHour = Math.floor(at / HOUR) * HOUR - HOUR;
  const iso = ms => new Date(ms).toISOString();
  const api = endpoint({ chain: { at, expiries: frame().expiries }, timestamps: [
    iso(lastHour - HOUR), iso(lastHour - HOUR + 30 * 60000), iso(lastHour + 20 * 60000), iso(at),
  ] });
  const response = await api.GET();
  assert.equal(response.status, 200);
  assert.deepEqual(api.picked(), [iso(lastHour - HOUR + 30 * 60000), iso(lastHour + 20 * 60000)]);
  assert.equal(response.body.score, null);
  assert.equal(response.body.historySamples, 0);
  assert.equal(response.body.cells.length, 81);
});

test('endpoint rejects stale, missing and failed upstream quotes', async () => {
  assert.equal((await endpoint({ chain: { at: Date.now() - 6 * 60000, expiries: frame().expiries } }).GET()).status, 503);
  assert.equal((await endpoint({ chain: { at: Date.now(), expiries: [] } }).GET()).status, 503);
  assert.equal((await endpoint({ fail: true }).GET()).status, 503);
});


test('endpoint serves a recorded snapshot while the live chain is still pending', async () => {
  const at = Date.now() - 12 * 60000;
  const snapshotRows = frame(at).expiries.map(decoded => ({ timestamp: new Date(at).toISOString(), decoded }));
  const api = endpoint({ pending: true, snapshotRows });
  const start = performance.now();
  const response = await api.GET();
  assert.ok(performance.now() - start < 1000, 'must not wait for external requests');
  assert.equal(response.status, 200);
  assert.equal(response.body.source, 'snapshot');
  assert.equal(response.body.asOf, new Date(at).toISOString());
  assert.equal(response.body.cells.length, 81);
  assert.deepEqual(response.body.cells[0].instruments, []);
});

test('endpoint stops waiting after three seconds if live and recorded data are unavailable', async () => {
  const start = performance.now();
  const response = await endpoint({ pending: true }).GET();
  assert.equal(response.status, 503);
  assert.ok(performance.now() - start < 5000);
});


test('volatility endpoint compresses repeated history and retains equivalent JSON on cache hits', async () => {
  const api = endpoint({ compressed: true });
  const request = new Request('http://localhost/api/volatility-pricing', { headers: { 'Accept-Encoding': 'gzip' } });
  const response = await api.GET(request);
  assert.equal(response.headers.get('content-encoding'), 'gzip');
  const zipped = Buffer.from(await response.arrayBuffer());
  const raw = require('node:zlib').gunzipSync(zipped);
  assert.equal(JSON.parse(raw).cells.length, 81);
  assert.ok(zipped.length < raw.length / 2);
  const next = await api.GET(new Request('http://localhost/api/volatility-pricing'));
  assert.equal(next.headers.get('x-noop-cache'), 'hit');
  assert.deepEqual(await next.json(), JSON.parse(raw));
});
