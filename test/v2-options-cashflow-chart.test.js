'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const ts = require('../dashboard/node_modules/typescript');

// Exercise the complete production module without starting Next.js or a DB.
function loadTypeScript(relativePath) {
  const filename = path.join(__dirname, '..', relativePath);
  const { outputText } = ts.transpileModule(fs.readFileSync(filename, 'utf8'), {
    fileName: filename,
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  });
  const module = { exports: {} };
  const unexpectedDependency = (name) => assert.fail(`Unexpected production dependency: ${name}`);
  new Function('require', 'module', 'exports', outputText)(unexpectedDependency, module, module.exports);
  return module.exports;
}

const { buildOptionsCashflowChart, cashflowDisplayDomain } = loadTypeScript('dashboard/src/lib/options-cashflow-chart.ts');
const FROM = '2026-09-01T00:00:00.000Z';
const TO = '2026-09-08T00:00:00.000Z';
const FIRST = '2026-09-02T00:00:00.000Z';
const SECOND = '2026-09-04T00:00:00.000Z';
const LAST = '2026-09-06T00:00:00.000Z';

function bucket(timestamp = FIRST, overrides = {}) {
  return {
    timestamp, tradeCashflow: 0, tradeRevenue: 0, tradeExpenses: 0,
    callRevenue: 0, callExpenses: 0, putRevenue: 0, putExpenses: 0,
    estimatedSettlementCashflow: 0,
    estimatedCallSettlementExpenses: 0, estimatedPutSettlementRevenue: 0,
    orderCount: 0, endPortfolioValue: null,
    ...overrides,
  };
}

function report(buckets = [], summary = {}, meta = {}) {
  return {
    meta: { from: FROM, to: TO, ...meta },
    summary: {
      openingTradeRevenue: 0, openingTradeExpenses: 0,
      openingEstimatedSettlementCashflow: 0,
      openingValue: 2000, closingValue: 2000,
      ...summary,
    },
    series: { buckets },
  };
}

const at = (rows, timestamp) => {
  const matches = rows.filter(row => row.ts === Date.parse(timestamp));
  assert.equal(matches.length, 1, `Expected exactly one chart point at ${timestamp}`);
  return matches[0];
};

test('earlier settlement estimates carry into the opening and every later cumulative result', () => {
  const input = report([
    bucket(FIRST),
    bucket(SECOND, { tradeCashflow: 50, tradeRevenue: 50, callRevenue: 50, orderCount: 1 }),
  ], { openingTradeRevenue: 700, openingTradeExpenses: 100, openingEstimatedSettlementCashflow: -800 });
  const original = structuredClone(input);
  const rows = buildOptionsCashflowChart(input);
  assert.equal(at(rows, FROM).cumulativeCashflow, 600);
  assert.equal(at(rows, FROM).cumulativeSettlementAdjustedCashflow, -200);
  assert.equal(at(rows, FIRST).cumulativeSettlementAdjustedCashflow, -200);
  assert.equal(at(rows, SECOND).cumulativeCashflow, 650);
  assert.equal(at(rows, SECOND).cumulativeSettlementAdjustedCashflow, -150);
  assert.equal(at(rows, TO).cumulativeSettlementAdjustedCashflow, -150);
  assert.deepEqual(input, original, 'Chart construction must not rewrite the report evidence');
});

test('an expiry-only call loss lowers the settlement-inclusive result while gross cashflow is unchanged', () => {
  const rows = buildOptionsCashflowChart(report([
    bucket(FIRST, { estimatedSettlementCashflow: -500, estimatedCallSettlementExpenses: 500 }),
  ], { openingTradeRevenue: 100 }));
  const expiry = at(rows, FIRST);
  assert.equal(expiry.cumulativeRevenue, 100);
  assert.equal(expiry.cumulativeExpenses, 0);
  assert.equal(expiry.cumulativeCashflow, 100);
  assert.equal(expiry.cumulativeSettlementAdjustedCashflow, -400);
  assert.equal(expiry.periodCallSettlements, -500);
  assert.equal(expiry.periodNet, 0);
  assert.equal(Math.abs(expiry.periodExpenses), 0);
  assert.equal(expiry.orderCount, 0);
});

test('an actual settlement already included in recorded cashflow is counted once with zero estimate', () => {
  const rows = buildOptionsCashflowChart(report([
    bucket(FIRST, { tradeCashflow: -475, tradeExpenses: 475, callExpenses: 475, orderCount: 1 }),
  ], { openingTradeRevenue: 100 }));
  const settled = at(rows, FIRST);
  assert.equal(settled.cumulativeCashflow, -375);
  assert.equal(settled.cumulativeSettlementAdjustedCashflow, -375);
  assert.equal(settled.periodCallExpenses, -475);
  assert.equal(Math.abs(settled.periodCallSettlements), 0);
  assert.equal(at(rows, TO).cumulativeSettlementAdjustedCashflow, -375);

  const carried = buildOptionsCashflowChart(report([bucket(FIRST)], {
    openingTradeRevenue: 100, openingTradeExpenses: 475,
  }));
  assert.equal(at(carried, FROM).cumulativeSettlementAdjustedCashflow, -375);
  assert.equal(at(carried, FIRST).cumulativeSettlementAdjustedCashflow, -375);
});

test('estimated put proceeds are positive and keep separate signs from put purchase expenses', () => {
  const rows = buildOptionsCashflowChart(report([
    bucket(FIRST, { tradeCashflow: -30, tradeExpenses: 30, putExpenses: 30, orderCount: 1 }),
    bucket(SECOND, { estimatedSettlementCashflow: 200, estimatedPutSettlementRevenue: 200 }),
  ], { openingTradeExpenses: 20 }));
  assert.equal(at(rows, FIRST).periodPutExpenses, -30);
  assert.equal(at(rows, SECOND).periodPutSettlements, 200);
  assert.equal(Math.abs(at(rows, SECOND).periodCallSettlements), 0);
  assert.equal(at(rows, SECOND).cumulativeCashflow, -50);
  assert.equal(at(rows, SECOND).cumulativeSettlementAdjustedCashflow, 150);
});

test('observed zero account value replaces the prior value, while null observations carry it forward', () => {
  const rows = buildOptionsCashflowChart(report([
    bucket(FIRST), bucket(SECOND, { endPortfolioValue: 0 }), bucket(LAST),
  ], { openingValue: 2000, closingValue: 9999 }));
  assert.equal(at(rows, FIRST).portfolioValueUsd, 2000);
  assert.equal(at(rows, SECOND).portfolioValueUsd, 0);
  assert.equal(at(rows, LAST).portfolioValueUsd, 0);
  assert.equal(at(rows, TO).portfolioValueUsd, 0);
  const zeroOpening = buildOptionsCashflowChart(report([bucket(FIRST)], { openingValue: 0 }));
  assert.equal(at(zeroOpening, FROM).portfolioValueUsd, 0);
  assert.equal(at(zeroOpening, FIRST).portfolioValueUsd, 0);
});

test('older API responses lacking estimate fields keep the settlement-inclusive curve unknown', () => {
  const missingOpening = report([bucket(FIRST)], { openingTradeRevenue: 100 });
  delete missingOpening.summary.openingEstimatedSettlementCashflow;
  const missingBucket = report([bucket(FIRST), bucket(SECOND)], { openingTradeRevenue: 100 });
  delete missingBucket.series.buckets[1].estimatedSettlementCashflow;
  const noEstimateFields = report([bucket(FIRST)], { openingTradeRevenue: 100 });
  delete noEstimateFields.summary.openingEstimatedSettlementCashflow;
  delete noEstimateFields.series.buckets[0].estimatedSettlementCashflow;
  for (const input of [missingOpening, missingBucket, noEstimateFields]) {
    const rows = buildOptionsCashflowChart(input);
    assert.ok(rows.every(row => row.cumulativeSettlementAdjustedCashflow === null));
    assert.ok(rows.every(row => row.cumulativeCashflow === 100));
  }
});

test('null and nonfinite estimate evidence cannot turn into an apparently complete result', () => {
  for (const value of [null, NaN, Infinity, -Infinity]) {
    const missingOpening = report([bucket(FIRST)], { openingEstimatedSettlementCashflow: value });
    const missingBucket = report([bucket(FIRST, { estimatedSettlementCashflow: value })]);
    for (const input of [missingOpening, missingBucket]) {
      assert.ok(buildOptionsCashflowChart(input).every(row => row.cumulativeSettlementAdjustedCashflow === null));
    }
  }
});

test('the display domain starts at available report buckets instead of inventing earlier chart history', () => {
  const wideReport = report([bucket(FIRST), bucket(LAST)], {}, { from: '1970-01-01T00:00:00.000Z' });
  assert.deepEqual(cashflowDisplayDomain(wideReport), [Date.parse(FIRST), Date.parse(TO)]);
  assert.deepEqual(cashflowDisplayDomain(report([bucket(FIRST)], {}, { to: SECOND })), [Date.parse(FIRST), Date.parse(SECOND)]);
  const narrowerRequest = report([bucket(FROM), bucket(LAST)], {}, { from: FIRST });
  assert.deepEqual(cashflowDisplayDomain(narrowerRequest), [Date.parse(FIRST), Date.parse(TO)]);
  assert.deepEqual(cashflowDisplayDomain(report()), [Date.parse(FROM), Date.parse(TO)]);
});

test('end padding clears every period bar and order count while carrying cumulative balances', () => {
  const rows = buildOptionsCashflowChart(report([
    bucket(LAST, {
      tradeCashflow: 90, tradeRevenue: 130, tradeExpenses: 40,
      callRevenue: 100, putRevenue: 30, callExpenses: 25, putExpenses: 15,
      estimatedSettlementCashflow: -130, estimatedCallSettlementExpenses: 150, estimatedPutSettlementRevenue: 20,
      orderCount: 4, endPortfolioValue: 2500,
    }),
  ], { openingTradeRevenue: 200, openingTradeExpenses: 50, openingEstimatedSettlementCashflow: -10 }));
  const last = at(rows, LAST);
  const end = at(rows, TO);
  assert.equal(last.cumulativeCashflow, 240);
  assert.equal(last.cumulativeSettlementAdjustedCashflow, 100);
  for (const field of [
    'periodRevenue', 'periodExpenses', 'periodNet', 'periodCallRevenue', 'periodPutRevenue',
    'periodCallExpenses', 'periodPutExpenses', 'periodCallSettlements', 'periodPutSettlements', 'orderCount',
  ]) assert.equal(end[field], 0, `${field} must not replay the final period at the display boundary`);
  for (const field of [
    'cumulativeRevenue', 'cumulativeExpenses', 'cumulativeCashflow',
    'cumulativeSettlementAdjustedCashflow', 'portfolioValueUsd',
  ]) assert.equal(end[field], last[field], `${field} must carry unchanged to the display boundary`);
});

test('a real bucket already at the end boundary is neither duplicated nor cleared', () => {
  const rows = buildOptionsCashflowChart(report([
    bucket(TO, { tradeCashflow: 30, tradeRevenue: 30, putRevenue: 30, orderCount: 1 }),
  ]));
  assert.equal(at(rows, TO).periodPutRevenue, 30);
  assert.equal(at(rows, TO).periodNet, 30);
  assert.equal(at(rows, TO).orderCount, 1);
});
