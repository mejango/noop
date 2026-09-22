// Run: node scripts/liquidity-staleness.test.js
// A replayed DEX sample must leave a hole in the chart series, not a flat line.
const ts = require('typescript');
const fs = require('fs');
const assert = require('assert');

const rows = [];
const src = fs.readFileSync(`${__dirname}/../src/lib/db.ts`, 'utf8');
const js = ts.transpileModule(src, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020, esModuleInterop: true } }).outputText;
const m = { exports: {} };
const stubDb = class { prepare() { return { all: () => rows, get: () => undefined, run: () => {} }; } pragma() {} exec() {} };
new Function('module', 'exports', 'require', js)(m, m.exports, (name) => {
  if (name === 'better-sqlite3') return { default: stubDb, __esModule: true };
  if (name === './strategy-config') return { BOT_CONFIG: {} };
  if (name.includes('economic-events')) return { getEconomicHistory: () => [] };
  if (name.includes('open-interest')) return { HOURLY_OPEN_INTEREST_SQL: '', openInterestHourBounds: () => ({}) };
  if (name.includes('funding-rates')) return { FUNDING_EXCHANGE: '', FUNDING_SYMBOL: '' };
  return require(name);
});
const { getLiquidityOverTime } = m.exports;

const dex = (v3, v4, stale) => JSON.stringify({
  dexLiquidity: { dexes: {
    uniswap_v3: { pools: 2, totalLiquidity: v3, totalVolume: v3 * 10 },
    uniswap_v4: { pools: 1, totalLiquidity: v4, totalVolume: v4 * 10, ...(stale ? { stale: true, staleReason: 'subgraph_unavailable' } : {}) },
  } },
});

// The real Sep 15-21 shape: live, then a frozen replay, then recovery at a new level.
rows.push({ timestamp: '2026-09-14T00:00:00Z', raw_data: dex(740e6, 20.6e6, false) });
rows.push({ timestamp: '2026-09-16T00:00:00Z', raw_data: dex(742e6, 13.45e6, true) });
rows.push({ timestamp: '2026-09-18T00:00:00Z', raw_data: dex(745e6, 13.45e6, true) });
rows.push({ timestamp: '2026-09-22T00:00:00Z', raw_data: dex(750e6, 12.4e6, false) });

const series = getLiquidityOverTime('2026-09-01T00:00:00Z');
assert.strictEqual(series.length, 4);
assert.strictEqual(series[0].uniswap_v4, 20.6e6, 'live sample is kept');
assert.strictEqual(series[1].uniswap_v4, undefined, 'replayed sample must not be served');
assert.strictEqual(series[2].uniswap_v4, undefined, 'replayed sample must not be forward-filled either');
assert.strictEqual(series[3].uniswap_v4, 12.4e6, 'recovery is served');
// V3 stayed live throughout and must be untouched.
assert.deepStrictEqual(series.map((r) => r.uniswap_v3), [740e6, 742e6, 745e6, 750e6]);
// No volume on the stale rows, so the chart cannot dump the whole outage into one bar.
assert.strictEqual(series[1].uniswap_v4_vol, undefined);
assert.strictEqual(series[2].uniswap_v4_vol, undefined);
console.log('liquidity-staleness: ok');
