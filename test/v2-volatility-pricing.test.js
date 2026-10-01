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
const { ivAtOffset, ivAtTenor, buildVolatilityPricing, nearestVolatilityInstruments, pricingColor, sampleHourly } = mod.exports;
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
  assert.equal(buildVolatilityPricing(current, history(5)).score, 50);
});

function quotedExpiry(dte) {
  const e = expiry(dte);
  e.points.forEach(p => { p.name = `ETH-${dte}D-${p.strike}-${p.type}`; p.askPrice = 12; p.askAmount = 3; });
  return e;
}

test('cell click matches actual buyable contracts by maturity then strike and option side', () => {
  const expiries = [quotedExpiry(7), quotedExpiry(29), quotedExpiry(60)];
  const [call] = nearestVolatilityInstruments(expiries, 30, 2100, 0.05);
  assert.equal(call.name, 'ETH-29D-2100-C');
  assert.equal(call.ask, 12);
  assert.equal(call.askIv, 51);
  assert.equal(call.askAmount, 3);
  const [put] = nearestVolatilityInstruments(expiries, 30, 1900, -0.05);
  assert.equal(put.name, 'ETH-29D-1900-P');
  assert.deepEqual(nearestVolatilityInstruments(expiries, 30, 2000, 0).map(p => p.type), ['P', 'C']);
  expiries[1].points.find(p => p.strike === 2100).askAmount = 0;
  assert.notEqual(nearestVolatilityInstruments(expiries, 30, 2100, 0.05)[0].name, call.name);
  assert.deepEqual(nearestVolatilityInstruments([expiry(30)], 30, 2100, 0.05), []);
});

// Exercise the production endpoint with isolated upstream/data-store adapters.
function endpoint({ chain = { at: Date.now(), expiries: frame().expiries }, timestamps = [], fail = false } = {}) {
  let picked;
  const route = { exports: {} };
  const source = fs.readFileSync(`${__dirname}/../dashboard/src/app/api/volatility-pricing/route.ts`, 'utf8');
  new Function('module', 'exports', 'require', ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText)(route, route.exports, name => {
    if (name === 'next/server') return { NextResponse: { json: (body, init) => ({ body, status: init?.status ?? 200 }) } };
    if (name === '@/lib/db') return {
      getSmileSnapshotTimestamps: () => timestamps,
      getSmileSnapshots: ts => { picked = ts; return []; },
    };
    if (name === '@/lib/smile-chain') return { getChain: async () => { if (fail) throw new Error('Upstream unavailable'); return chain; } };
    if (name === '@/lib/vol-smile') return { fromCompact: () => { throw new Error('No rows expected'); } };
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
  assert.equal((await endpoint({ fail: true }).GET()).status, 502);
});
