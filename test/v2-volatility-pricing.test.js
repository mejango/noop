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
const { ivAtOffset, ivAtTenor, buildVolatilityPricing } = mod.exports;
const DAY = 86400000;
const now = Date.parse('2026-10-01T12:00:00Z');
function expiry(dte, iv = 50, spot = 2000) {
  return { dte, expiry: (now + dte * DAY) / 1000, spot, forward: spot, points:
    [-0.15, -0.1, -0.05, 0, 0.05, 0.1, 0.15].map(offset => ({
      strike: spot * (1 + offset), type: offset < 0 ? 'P' : 'C', delta: 0.25,
      iv, bidIv: iv - 1, askIv: iv + 1, oi: 10,
    })) };
}
const frame = (at = now, iv = 50, spot = 2000) => ({ at, expiries: [1, 7, 14, 30, 60, 90, 120].map(d => expiry(d, iv, spot)) });
const history = (count = 30, iv = 50) => Array.from({ length: count }, (_, i) => frame(now - (i + 1) * DAY, iv));

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
  e.points[3].askIv = null;
  assert.equal(ivAtOffset(e, 0), null);
  assert.equal(ivAtOffset(e, 0.025), null);
  e.points[3].askIv = 40;
  assert.equal(ivAtOffset(e, 0), null);
  e.points[3].askIv = 100;
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
  assert.equal(buildVolatilityPricing(frame(now, 40, 3000), past).cells[0].strike, 2700);
});

test('daily sampling gives dense recording days no extra weight and excludes future / old data', () => {
  const past = history(7);
  const dense = Array.from({ length: 100 }, (_, i) => frame(now - DAY - (i + 1) * 1000, 30));
  const result = buildVolatilityPricing(frame(now, 40), [
    ...past, ...dense, frame(now + DAY, 10), frame(now - 366 * DAY, 10), frame(now - 1000, 10),
  ]);
  assert.equal(result.historyDays, 7);
  assert.equal(result.score, 0); // the last daily observation is 50, not the earlier dense 30s
  assert.equal(result.provisional, true);
});

test('insufficient history or narrow coverage never becomes a broad market score', () => {
  const empty = buildVolatilityPricing(frame(), []);
  assert.equal(empty.score, null);
  assert.equal(empty.cells[0].iv, 50);
  assert.equal(buildVolatilityPricing(frame(), history(1)).score, null);
  assert.equal(empty.currentIv, 50);
  const narrow = { at: now, expiries: [expiry(7)] };
  const result = buildVolatilityPricing(narrow, history());
  assert.equal(result.score, null);
  assert.equal(result.measured, 5);
});


test('five recorded days show a provisional score instead of a blank meter', () => {
  const result = buildVolatilityPricing(frame(now, 40), history(5));
  assert.equal(result.score, 0);
  assert.equal(result.label, 'Cheap');
  assert.equal(result.provisional, true);
  assert.equal(result.measured, 25);
  assert.equal(result.currentIv, 40);
  assert.equal(buildVolatilityPricing(frame(), history(2)).score, 50);
  assert.equal(buildVolatilityPricing(frame(), history(2)).provisional, true);
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

test('endpoint selects the last snapshot per completed day and returns honest empty history', async () => {
  const at = Date.now(), today = new Date(at).toISOString().slice(0, 10);
  const yesterday = new Date(at - DAY).toISOString().slice(0, 10);
  const api = endpoint({ chain: { at, expiries: frame().expiries }, timestamps: [
    `${yesterday}T01:00:00Z`, `${yesterday}T23:00:00Z`, `${today}T00:00:00Z`,
  ] });
  const response = await api.GET();
  assert.equal(response.status, 200);
  assert.deepEqual(api.picked(), [`${yesterday}T23:00:00Z`]);
  assert.equal(response.body.score, null);
  assert.equal(response.body.historyDays, 0);
  assert.equal(response.body.cells.length, 25);
});

test('endpoint rejects stale, missing and failed upstream quotes', async () => {
  assert.equal((await endpoint({ chain: { at: Date.now() - 6 * 60000, expiries: frame().expiries } }).GET()).status, 503);
  assert.equal((await endpoint({ chain: { at: Date.now(), expiries: [] } }).GET()).status, 503);
  assert.equal((await endpoint({ fail: true }).GET()).status, 502);
});
