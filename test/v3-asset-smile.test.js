'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const ts = require('../dashboard/node_modules/typescript');

function load(name, deps = {}) {
  const src = fs.readFileSync(path.join(__dirname, '../dashboard/src/lib', name), 'utf8');
  const mod = { exports: {} };
  const js = ts.transpileModule(src, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
  new Function('require', 'module', 'exports', js)(id => deps[id], mod, mod.exports);
  return mod.exports;
}

const smile = load('vol-smile.ts');
const asset = load('asset-smile.ts', { './vol-smile': smile });

test('saved BTC quotes produce the same OTM smile as a live chain without borrowing ETH values', () => {
  const expiry = Date.UTC(2026, 10, 20, 8) / 1000;
  const at = new Date(expiry * 1000 - 7 * 86400000).toISOString();
  const ticker = (d, iv, strike, type) => ({
    instrument_name: `BTC-20261120-${strike}-${type}`, expiry, strike, option_type: type,
    delta: d, implied_vol: iv, bid_iv: iv - 0.01, ask_iv: iv + 0.01,
    index_price: 100000, forward: 100000, bid: 20, ask: 22, bid_amount: 2, ask_amount: 3, open_interest: 5,
  });
  const quotes = [
    ticker(-0.1, 0.7, 80000, 'P'), ticker(-0.25, 0.6, 90000, 'P'),
    ticker(0.25, 0.52, 110000, 'C'), ticker(0.1, 0.58, 120000, 'C'),
    ticker(-0.8, 0.55, 110000, 'P'), // ITM, excluded
  ];
  const [saved] = asset.assetSmileExpiries({ hour: at, spot: 100000, quotes });
  const live = smile.buildExpiry(expiry, Object.fromEntries(quotes.map(q => [q.instrument_name, {
    I: String(q.index_price), b: String(q.bid), B: String(q.bid_amount), a: String(q.ask), A: String(q.ask_amount),
    option_pricing: { d: String(q.delta), i: String(q.implied_vol), bi: String(q.bid_iv), ai: String(q.ask_iv), f: String(q.forward) },
    stats: { oi: String(q.open_interest) },
  }])), Date.parse(at));
  assert.deepEqual(saved, live);
  assert.ok(saved.points.every(p => p.name.startsWith('BTC-')));
  const compact = asset.compactAssetExpiry(saved);
  assert.equal(compact.points.length, 4);
  assert.equal(compact.points[0][3], 0.7);
});

test('saved quotes without bid/ask IV retain mark IV but cannot claim a two-sided market', () => {
  const expiry = Date.UTC(2026, 10, 20, 8) / 1000;
  const q = (strike, type, delta) => ({
    instrument_name: `SOL-20261120-${strike}-${type}`, expiry, strike, option_type: type,
    delta, implied_vol: 0.6, bid_iv: null, ask_iv: null, index_price: 100, forward: null,
    bid: null, ask: null, bid_amount: null, ask_amount: null, open_interest: null,
  });
  const [saved] = asset.assetSmileExpiries({ hour: '2026-11-13T08:00:00.000Z', spot: 100,
    quotes: [q(80, 'P', -0.1), q(90, 'P', -0.25), q(110, 'C', 0.25), q(120, 'C', 0.1)] });
  assert.equal(saved.forward, 100);
  assert.ok(saved.points.every(p => p.iv === 60 && p.bidIv == null && p.askIv == null));
});
