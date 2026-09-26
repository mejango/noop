'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const ts = require('../dashboard/node_modules/typescript');

const src = fs.readFileSync(path.join(__dirname, '../dashboard/src/lib/vol-smile.ts'), 'utf8');
const mod = { exports: {} };
new Function('module', 'exports', ts.transpileModule(src, {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
}).outputText)(mod, mod.exports);
const { buildExpiry, ivAtDelta, deltaX, expiryStats } = mod.exports;

const tick = (d, i, f = '2000') => ({ option_pricing: { d: String(d), i: String(i), f, bi: String(i - 0.01), ai: String(i + 0.01) }, stats: { oi: '5' } });

test('buildExpiry keeps OTM wings only and drops sub-2Δ noise', () => {
  const e = buildExpiry(1_900_000_000, {
    'ETH-X-1600-P': tick(-0.05, 0.70),
    'ETH-X-1800-P': tick(-0.25, 0.60),
    'ETH-X-2200-P': tick(-0.80, 0.55), // ITM put
    'ETH-X-1800-C': tick(0.75, 0.60),  // ITM call
    'ETH-X-2200-C': tick(0.25, 0.52),
    'ETH-X-2600-C': tick(0.06, 0.58),
    'ETH-X-4000-C': tick(0.01, 0.95),  // deep wing
  }, 1_899_000_000_000);
  assert.deepEqual(e.points.map(p => p.name), ['ETH-X-1600-P', 'ETH-X-1800-P', 'ETH-X-2200-C', 'ETH-X-2600-C']);
  assert.equal(e.forward, 2000);
  assert.equal(e.points[0].iv, 70);
  assert.ok(Math.abs(e.dte - 1_000_000 / 86_400) < 1e-9);
});

test('ivAtDelta interpolates within quoted range and refuses to extrapolate', () => {
  const pts = [{ type: 'C', delta: 0.05, iv: 60 }, { type: 'C', delta: 0.15, iv: 50 }, { type: 'P', delta: -0.10, iv: 70 }];
  assert.equal(ivAtDelta(pts, 'C', 0.10), 55);
  assert.equal(ivAtDelta(pts, 'P', 0.10), 70);
  assert.equal(ivAtDelta(pts, 'C', 0.25), null);
});

test('deltaX maps puts left, calls right, ATM to zero', () => {
  assert.ok(Math.abs(deltaX({ type: 'P', delta: -0.10 }) + 0.4) < 1e-12);
  assert.ok(Math.abs(deltaX({ type: 'C', delta: 0.10 }) - 0.4) < 1e-12);
  assert.equal(deltaX({ type: 'C', delta: 0.5 }), 0);
});

test('expiryStats: risk reversal is call minus put', () => {
  const s = expiryStats([
    { type: 'P', delta: -0.05, iv: 80 }, { type: 'P', delta: -0.30, iv: 60 }, { type: 'P', delta: -0.45, iv: 55 },
    { type: 'C', delta: 0.45, iv: 53 }, { type: 'C', delta: 0.30, iv: 55 }, { type: 'C', delta: 0.05, iv: 65 },
  ]);
  assert.equal(s.atm, 54);
  assert.equal(s.put10, 76);
  assert.equal(s.call10, 63);
  assert.equal(s.rr10, -13);
  assert.ok(s.rr25 < 0);
});
