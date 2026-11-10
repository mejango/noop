'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const ts = require('../dashboard/node_modules/typescript');
const filename = path.join(__dirname, '../dashboard/src/lib/portfolio-chart.ts');
const output = ts.transpileModule(fs.readFileSync(filename, 'utf8'), {
  fileName: filename, compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
}).outputText;
const loaded = { exports: {} };
new Function('module', 'exports', output)(loaded, loaded.exports);
const { buildCombinedPortfolioChart, combinedPortfolioBefore, combinedAvailabilityMessage } = loaded.exports;
const point = (time, value, full = value == null ? null : value + 100) => ({
  timestamp: new Date(time).toISOString(), ts: time, portfolioValue: value, fullPortfolioValue: full,
  sourceValue: 1000, destinationValue: value == null ? null : value - 1000,
  kind: value == null ? 'unavailable' : 'observed', reason: value == null ? 'missing destination' : null,
});

test('combined series preserves sparse observations and explicit gaps without source substitution', () => {
  const input = [point(3000, 1012), point(1000, 1010), point(2000, null)];
  const original = structuredClone(input);
  const rows = buildCombinedPortfolioChart(input);
  assert.deepEqual(rows.map(row => [row.ts, row.combinedPortfolioValueUsd]), [[1000, 1010], [2000, null], [3000, 1012]]);
  assert.deepEqual(rows.map(row => row.combinedFullPortfolioValueUsd), [1110, null, 1112]);
  assert.deepEqual(input, original);
  assert.deepEqual(combinedPortfolioBefore(rows, 1000), { combinedPortfolioValueUsd: null, combinedFullPortfolioValueUsd: null });
  assert.deepEqual(combinedPortfolioBefore(rows, 2000), { combinedPortfolioValueUsd: 1010, combinedFullPortfolioValueUsd: 1110 });
  assert.deepEqual(combinedPortfolioBefore(rows, 3000), { combinedPortfolioValueUsd: null, combinedFullPortfolioValueUsd: null });
  assert.deepEqual(combinedPortfolioBefore(rows, 4000), { combinedPortfolioValueUsd: 1012, combinedFullPortfolioValueUsd: 1112 });
});

test('zero observed value stays zero while nonfinite and unknown account values stay unavailable', () => {
  const rows = buildCombinedPortfolioChart([point(1000, 0, 0), point(2000, NaN, 100), point(3000, null, 100)]);
  assert.equal(rows[0].combinedPortfolioValueUsd, 0);
  assert.equal(rows[0].combinedFullPortfolioValueUsd, 0);
  assert.deepEqual(rows.slice(1).map(row => [row.combinedPortfolioValueUsd, row.combinedFullPortfolioValueUsd]), [[null, null], [null, null]]);
  assert.equal(combinedPortfolioBefore(rows, 4000).combinedPortfolioValueUsd, null);
});

test('unavailable account status uses readable copy without exposing exchange errors', () => {
  assert.equal(combinedAvailabilityMessage({configured:false}), null);
  assert.equal(combinedAvailabilityMessage({configured:true,latestStatus:'complete',reason:'history_partial'}), null);
  assert.equal(combinedAvailabilityMessage({configured:true,latestStatus:'stale'}), 'Latest account data is out of date.');
  assert.equal(combinedAvailabilityMessage({configured:true,latestStatus:'incomplete',reason:'internal_transfer_pending'}), 'An internal transfer is awaiting confirmation.');
  assert.equal(combinedAvailabilityMessage({configured:true,latestStatus:'incomplete',reason:'API exception details'}), 'One account observation is unavailable.');
});
