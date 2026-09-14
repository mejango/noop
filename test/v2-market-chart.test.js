'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const ts = require('../dashboard/node_modules/typescript');

// Execute the actual component's merge callback and spot helpers, without
// copying their implementation or starting React, a server, or a database.
const filename = process.env.MARKET_CHART_SOURCE
  || path.join(__dirname, '../dashboard/src/app/page.tsx');
const source = ts.createSourceFile(filename, fs.readFileSync(filename, 'utf8'), ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
const helperNames = new Set(['normalizeEthSpot', 'median', 'spotBandPctForRange', 'robustSpotBand', 'spotInBand']);
const declarations = [];
let callback;
function visit(node) {
  if (ts.isFunctionDeclaration(node) && helperNames.has(node.name?.text)) declarations.push(node.getText(source));
  if (ts.isVariableDeclaration(node)) {
    if (['ETH_SPOT_MIN', 'ETH_SPOT_MAX'].includes(node.name.getText(source))) declarations.push(`const ${node.getText(source)};`);
    if (node.name.getText(source) === 'merged' && ts.isCallExpression(node.initializer)
      && node.initializer.expression.getText(source) === 'useMemo') callback = node.initializer.arguments[0].getText(source);
  }
  ts.forEachChild(node, visit);
}
visit(source);
assert.ok(callback, 'Production chart merge must be found');
const code = ts.transpileModule(`${declarations.join('\n')}
  return (${callback})();`, { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText;
const merge = new Function('chart', 'stats', 'latestTick', 'chartRange', 'dteDays', code);

const SPOT = '2026-09-14T12:26:17.987Z';
const QUOTE = '2026-09-14T12:26:19.466Z';
const RAW = 0.0037275362318840584;
const EDGE = 0.0032291185081421295;
const preferredRaw = 0.002735830618892508;
function fixture() {
  return {
    chart: {
      prices: [{ timestamp: SPOT, price: 2512.8 }],
      options: [{ timestamp: QUOTE, best_put_value: RAW, best_call_value: 98.6, lyra_spot: 2512.8 }],
      buyPutEdge: [{ timestamp: QUOTE, edge_score: EDGE }],
      sellCallEdge: [{ timestamp: QUOTE, edge_score: 95.8 }],
      optionsHeatmap: [{ timestamp: QUOTE, option_type: 'P', strike: 1900, expiry: 1793347200, delta: -0.07716, ask_price: 20.7, ask_delta_value: RAW }],
    },
    stats: { last_price_time: SPOT, last_price: 2513, lyra_spot: 2512.8 },
    latestTick: {
      current_best_put: EDGE + 0.00001,
      current_best_call: 75,
      best_put_detail: { raw_score: preferredRaw, strike: 1800, delta: -0.08399, price: 30.7, expiry: 1795766400 },
      best_call_detail: { raw_score: 80, strike: 2900, delta: 0.06, price: 4.8, expiry: 1790323200 },
    },
  };
}
const run = ({ chart, stats, latestTick }) => merge(chart, stats, latestTick, '24h', () => 0);

test('latest point keeps snapshot RAW and telemetry EDGE when the tick prefers another contract', () => {
  const input = fixture();
  const before = structuredClone(input);
  const [row] = run(input);
  assert.equal(row.bestPut, RAW);
  assert.equal(row.putEdge, EDGE);
  assert.equal(row.bestCall, 98.6);
  assert.equal(row.callEdge, 95.8);
  assert.deepEqual(input, before, 'Rendering must not change input observations');
});

test('matching spot refresh preserves the raw contract details and updates the spot price', () => {
  const [row] = run(fixture());
  assert.equal(row.price, 2513);
  assert.equal(row.ts, Date.parse(SPOT));
  assert.equal(row.bestPutDetail.strike, 1900);
  assert.equal(row.bestPutDetail.price, 20.7);
});

test('newer spot gets its own row without moving or carrying older option values', () => {
  for (const delay of [500, 300_000]) {
    const input = fixture();
    input.stats.last_price_time = new Date(Date.parse(SPOT) + delay).toISOString();
    const rows = run(input);
    assert.equal(rows.length, 2);
    assert.equal(rows[0].ts, Date.parse(SPOT));
    assert.equal(rows[0].bestPut, RAW);
    assert.equal(rows[1].ts, Date.parse(SPOT) + delay);
    assert.equal(rows[1].price, 2513);
    for (const key of ['bestPut', 'bestCall', 'putEdge', 'callEdge', 'bestPutDetail', 'bestCallDetail']) {
      assert.equal(Object.hasOwn(rows[1], key), false, `New spot must not invent ${key}`);
    }
  }
});

test('older spot refresh does not add duplicate rows or replace the latest option observation', () => {
  for (const delay of [500, 300_000]) {
    const input = fixture();
    input.stats.last_price_time = new Date(Date.parse(SPOT) - delay).toISOString();
    const rows = run(input);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].ts, Date.parse(SPOT));
    assert.equal(rows[0].bestPut, RAW);
    assert.equal(rows[0].price, 2512.8);
  }
});

test('missing tick details and zero tick scores cannot replace observed chart scores', () => {
  for (const latestTick of [null, { current_best_put: 0, current_best_call: 0 }, { current_best_put: EDGE, current_best_call: 95.8 }]) {
    const input = fixture();
    input.latestTick = latestTick;
    assert.equal(run(input)[0].bestPut, RAW);
    assert.equal(run(input)[0].bestCall, 98.6);
  }
});

test('missing chart scores remain missing even when a tick summary has scores', () => {
  const input = fixture();
  Object.assign(input.chart, { optionsHeatmap: [], buyPutEdge: [], sellCallEdge: [] });
  Object.assign(input.chart.options[0], { best_put_value: null, best_call_value: null });
  const [row] = run(input);
  assert.equal(row.bestPut, null);
  assert.equal(row.bestCall, null);
  assert.equal(row.putEdge, undefined);
  assert.equal(row.callEdge, undefined);
  input.chart.options = [];
  assert.equal(run(input)[0].bestPut, undefined);
});
