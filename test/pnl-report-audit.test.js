const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const ts = require('../dashboard/node_modules/typescript');

// Load complete production modules, while preventing dashboard DB/network startup.
function loadTypeScript(relativePath, dependencies = {}, env = {}) {
  const filename = path.join(__dirname, '..', relativePath);
  const { outputText } = ts.transpileModule(fs.readFileSync(filename, 'utf8'), {
    fileName: filename,
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  });
  const module = { exports: {} };
  const requireMock = (name) => {
    assert.ok(Object.hasOwn(dependencies, name), `Unexpected production dependency: ${name}`);
    return dependencies[name];
  };
  new Function('require', 'module', 'exports', 'process', outputText)(
    requireMock, module, module.exports, { env },
  );
  return module.exports;
}

const settlement = loadTypeScript('dashboard/src/lib/expiry-settlement.ts');
const FROM = '2026-03-01T00:00:00.000Z';
const TO = '2026-03-08T00:00:00.000Z';
const EXPIRY = '2026-03-06T08:00:00.000Z';
const INSTRUMENT = 'ETH-20260306-2000-C';
const expiryMs = Date.parse(EXPIRY);
const isoAt = (offset) => new Date(expiryMs + offset).toISOString();
const quote = (offset, price = 2500) => ({ timestamp: isoAt(offset), price });

function order(overrides = {}) {
  return {
    id: 1,
    timestamp: '2026-03-02T00:00:00.000Z',
    action: 'sell_call',
    success: 1,
    reason: 'Filled call sale',
    instrument_name: INSTRUMENT,
    strike: 2000,
    expiry: expiryMs / 1000,
    delta: 0.2,
    price: 100,
    intended_amount: 1,
    filled_amount: 1,
    fill_price: 100,
    total_value: 100,
    spot_price: 2000,
    ...overrides,
  };
}

function snapshot(timestamp, value, spot = 2000) {
  return {
    timestamp,
    spot_price: spot,
    usdc_balance: value,
    eth_balance: 0,
    total_unrealized_pnl: 0,
    total_realized_pnl: 0,
    portfolio_value_usd: value,
  };
}

function recordedSettlement(overrides = {}) {
  return {
    event_id: 'exchange-settlement-1',
    event_type: 'settlement',
    timestamp: EXPIRY,
    instrument_name: INSTRUMENT,
    cashflow_usd: '-475',
    source: 'exchange',
    ...overrides,
  };
}

function routeFixture({
  orders = [],
  spots = [],
  events = [],
  exposureHistory,
  reporting = { config: null, rows: [], baseline: null, latest: null,
    coverage: { configured: false, historyPeriodMs: 900000, freshnessMs: 600000, latestStatus: 'unavailable' } },
  snapshots = [],
  baseline,
  from = FROM,
  to = TO,
  env = {},
  openingCashflow = { revenue: 0, expenses: 0, gross_cashflow: 0, order_count: 0 },
} = {}) {
  const spotQueries = [];
  const db = {
    getReportingPortfolio: () => reporting,
    getPortfolioSnapshotsInRange: (start, end) => snapshots.filter(row => row.timestamp >= start && row.timestamp <= end),
    getPortfolioSnapshotBefore: () => baseline,
    getOrdersInRange: (start, end) => orders.filter(row => row.timestamp >= start && row.timestamp <= end),
    getOrderCashflowTotalsBefore: () => ({ ...openingCashflow }),
    getEconomicHistory: (start, end) => ({
      events: events.filter(row => row.timestamp >= start && row.timestamp <= end),
      coverage: { trades: false, settlements: false, transfers: false },
      available: true,
      exposureHistory,
    }),
    getSpotPricesAtOrBefore: (timestamps, maxAgeMs) => {
      spotQueries.push({ timestamps, maxAgeMs });
      return timestamps.flatMap(timestamp => {
        const expiry = Date.parse(timestamp);
        const matches = spots.filter(row => {
          const time = Date.parse(row.timestamp);
          return time <= expiry && time >= expiry - maxAgeMs;
        }).sort((a, b) => Date.parse(b.timestamp) - Date.parse(a.timestamp));
        return matches.length ? [matches[0]] : [];
      });
    },
  };
  const route = loadTypeScript('dashboard/src/app/api/pnl-report/route.ts', {
    'next/server': { NextResponse: { json: (body, options) => ({ status: options?.status ?? 200, body }) } },
    '@/lib/db': db,
    '@/lib/expiry-settlement': settlement,
    '@/lib/response-cache': { cachedJsonRoute: (_request, _key, generate) => generate() },
    '@/lib/dashboard-ranges': { dashboardRangeMs: () => 30 * 24 * 60 * 60 * 1000 },
  }, env);
  const url = new URL('https://dashboard.test/api/pnl-report');
  url.searchParams.set('from', from);
  url.searchParams.set('to', to);
  return {
    spotQueries,
    async report() {
      const response = await route.GET({ url: url.href, nextUrl: url });
      assert.equal(response.status, 200, JSON.stringify(response.body));
      return response.body;
    },
  };
}

test('expiry lookup accepts only valid observations within the preceding 15 minutes', () => {
  const age = settlement.SETTLEMENT_SPOT_MAX_AGE_MS;
  assert.equal(age, 15 * 60 * 1000);
  assert.equal(settlement.spotAtOrBefore([quote(-age)], expiryMs), 2500);
  assert.equal(settlement.spotAtOrBefore([quote(-age - 1)], expiryMs), null);
  assert.equal(settlement.spotAtOrBefore([quote(1)], expiryMs), null);
  assert.equal(settlement.spotAtOrBefore([
    quote(-60_000, 2400), quote(-5_000, 2500), quote(-30_000, 2450), quote(1000, 9000),
    quote(-1, NaN), quote(-2, Infinity), quote(-3, 0), quote(-4, -1),
  ], expiryMs), 2500);
});

test('missing and stale spot history report unknown settlements instead of inventing cashflows', async (t) => {
  for (const [name, spots] of [
    ['missing', []],
    ['stale', [quote(-settlement.SETTLEMENT_SPOT_MAX_AGE_MS - 1)]],
    ['after expiry', [quote(1)]],
  ]) {
    await t.test(name, async () => {
      const derived = settlement.deriveExpirySettlementReport([order()], spots, Date.parse(TO));
      assert.equal(derived.estimates.length, 0);
      assert.equal(derived.missing.length, 1);
      assert.equal(derived.missing[0].instrument_name, INSTRUMENT);
      assert.match(derived.missing[0].reason, /unknown/i);

      const report = await routeFixture({ orders: [order()], spots }).report();
      assert.equal(report.meta.missingSettlementEstimateCount, 1);
      assert.equal(report.meta.settlementEstimateCount, 0);
      assert.equal(report.missingSettlementEstimates.length, 1);
      assert.equal(report.summary.estimatedSettlementCashflow, 0);
      assert.equal(report.summary.netTradeCashflow, 100);
      assert.deepEqual(report.orders.map(row => row.action), ['sell_call']);
    });
  }
});

test('settlement estimates are explicitly uncertain and excluded from recorded activity and cashflow', async () => {
  const fixture = routeFixture({ orders: [order()], spots: [quote(-60_000)] });
  const report = await fixture.report();
  assert.deepEqual(fixture.spotQueries, [{ timestamps: [EXPIRY], maxAgeMs: 15 * 60 * 1000 }]);
  assert.equal(report.meta.settlementEstimateCount, 1);
  const estimate = report.settlementEstimates[0];
  assert.equal(estimate.success, null);
  assert.equal(estimate.estimated, true);
  assert.equal(estimate.source, 'spot_estimate');
  assert.match(estimate.reason, /estimated/i);
  assert.equal(estimate.cashflow, -500);
  assert.equal(report.summary.estimatedSettlementCashflow, -500);
  assert.equal(report.summary.netTradeCashflow, 100);
  assert.equal(report.summary.callNetCashflow, 100);
  assert.equal(report.meta.orderCount, 1);
  assert.deepEqual(report.orders.map(row => row.action), ['sell_call']);
  assert.equal(report.series.buckets.reduce((sum, row) => sum + row.tradeCashflow, 0), 100);
  assert.deepEqual(report.actionBreakdown.map(row => row.action), ['sell_call']);
});

test('worthless expiry remains an estimate that awaits an exchange settlement record', async () => {
  const report = await routeFixture({ orders: [order()], spots: [quote(-60_000, 1900)] }).report();
  assert.equal(report.meta.settlementEstimateCount, 1);
  assert.equal(report.meta.missingSettlementEstimateCount, 0);
  assert.equal(report.settlementEstimates[0].total_value, 0);
  assert.equal(report.settlementEstimates[0].success, null);
  assert.equal(report.orders.length, 1);
});

test('adding newer spot observations cannot alter a fixed historical report', async () => {
  const spots = [quote(-60_000)];
  const fixture = routeFixture({ orders: [order()], spots });
  const before = await fixture.report();
  // More than the old global latest-100k cap; none is eligible for this expiry.
  for (let index = 1; index <= 100_001; index++) spots.push(quote(index * 60_000, 3500));
  const after = await fixture.report();
  delete before.meta.generatedAt;
  delete after.meta.generatedAt;
  assert.deepEqual(after, before);
  assert.deepEqual(fixture.spotQueries, [
    { timestamps: [EXPIRY], maxAgeMs: 15 * 60 * 1000 },
    { timestamps: [EXPIRY], maxAgeMs: 15 * 60 * 1000 },
  ]);
});

test('deposit and withdrawal balance changes are not reported as performance', async (t) => {
  for (const delta of [1000, -1000]) {
    await t.test(delta > 0 ? 'deposit' : 'withdrawal', async () => {
      const report = await routeFixture({
        baseline: snapshot('2026-02-28T23:59:00.000Z', 10_000),
        snapshots: [snapshot('2026-03-07T12:00:00.000Z', 10_000 + delta)],
        events: [{ event_id: `transfer-${delta}`, event_type: 'transfer', timestamp: '2026-03-05T00:00:00.000Z', cashflow_usd: delta }],
      }).report();
      assert.equal(report.summary.portfolioChange, delta);
      assert.equal(report.summary.portfolioReturnPct, null);
      assert.equal(report.summary.maxDrawdown, null);
      assert.equal(report.summary.maxDrawdownPct, null);
      assert.equal(report.meta.performanceAvailable, false);
      assert.match(report.meta.performanceUnavailableReason, /deposits and withdrawals/i);
      assert.equal(report.summary.netTradeCashflow, 0);
      assert.equal(report.orders.length, 0);
    });
  }
});

test('current external ETH insurance settings cannot rewrite historical persisted balances', async () => {
  const report = await routeFixture({
    baseline: snapshot('2026-02-28T23:59:00.000Z', 10_000, 2000),
    snapshots: [snapshot('2026-03-07T12:00:00.000Z', 10_000, 2500)],
    env: { PUT_INSURED_EXTERNAL_ETH: '100' },
  }).report();
  assert.equal(report.summary.openingValue, 10_000);
  assert.equal(report.summary.closingValue, 10_000);
  assert.equal(report.summary.portfolioChange, 0);
  assert.deepEqual(report.series.portfolio.map(row => row.portfolioValue), [10_000, 10_000]);
  assert.equal(report.series.buckets[0].endPortfolioValue, 10_000);
  assert.equal(report.meta.valuationScope, 'derive_subaccount');
  assert.equal(report.meta.insuredExternalEth, 0);
  assert.match(report.meta.externalHoldingsUnavailableReason, /current insurance setting is not applied to historical balances/i);
});

test('full portfolio adds recorded off-exchange ETH at each snapshot spot, backfilling before the first record', async () => {
  const report = await routeFixture({
    baseline: snapshot('2026-02-28T23:59:00.000Z', 10_000, 2000),
    snapshots: [snapshot('2026-03-02T12:00:00.000Z', 10_000, 2000), snapshot('2026-03-07T12:00:00.000Z', 10_000, 2500)],
    exposureHistory: [
      { effective_at: '2026-03-05T00:00:00.000Z', external_eth: '4', source: 'runtime-config' },
      { effective_at: '2026-03-06T00:00:00.000Z', external_eth: '5', source: 'runtime-config' },
    ],
    env: { PUT_INSURED_EXTERNAL_ETH: '100' },
  }).report();
  assert.deepEqual(report.series.portfolio.map(row => row.portfolioValue), [10_000, 10_000, 10_000]);
  assert.deepEqual(report.series.portfolio.map(row => row.fullPortfolioValue), [18_000, 18_000, 22_500]);
  assert.equal(report.meta.insuredExternalEth, 5);
  assert.equal(report.meta.externalEthRecordedFrom, '2026-03-05T00:00:00.000Z');
});

test('recorded settlement cashflow replaces estimates even with unknown filled amount', async (t) => {
  for (const cashflow of [-475, 0, 25]) {
    await t.test(`authoritative signed cashflow ${cashflow}`, async () => {
      const fixture = routeFixture({ orders: [order()], spots: [quote(-60_000)], events: [recordedSettlement({ cashflow_usd: String(cashflow) })] });
      const report = await fixture.report();
      assert.deepEqual(fixture.spotQueries[0].timestamps, []);
      assert.equal(report.meta.settlementEstimateCount, 0);
      assert.equal(report.meta.missingSettlementEstimateCount, 0);
      assert.equal(report.summary.estimatedSettlementCashflow, 0);
      assert.equal(report.summary.netTradeCashflow, 100 + cashflow);
      assert.equal(report.summary.callNetCashflow, 100 + cashflow);
      const actual = report.orders.find(row => row.action === 'settle_call');
      assert.ok(actual);
      assert.equal(actual.success, 1);
      assert.equal(actual.filled_amount, null);
      assert.equal(actual.cashflow, cashflow);
      assert.equal(actual.total_value, Math.abs(cashflow));
      assert.equal(report.meta.orderCount, 2);
      const bucket = report.series.buckets.find(row => row.timestamp.startsWith('2026-03-06'));
      assert.equal(bucket.tradeCashflow, cashflow);
      assert.equal(bucket.tradeRevenue, Math.max(cashflow, 0));
      assert.equal(bucket.tradeExpenses, Math.max(-cashflow, 0));
    });
  }
});

test('a recorded settlement without a USD value is disclosed rather than estimated or counted as zero', async () => {
  const report = await routeFixture({
    orders: [order()], spots: [quote(-60_000)], events: [recordedSettlement({ cashflow_usd: null })],
  }).report();
  assert.equal(report.meta.unvaluedRecordedSettlementCount, 1);
  assert.equal(report.settlementEstimates.length, 0);
  assert.equal(report.orders.length, 1);
  assert.equal(report.summary.netTradeCashflow, 100);
});

test('settlements before the report window contribute only to opening recorded cashflow', async () => {
  const report = await routeFixture({
    from: '2026-03-07T00:00:00.000Z', orders: [order()], spots: [quote(-60_000)], events: [recordedSettlement()],
  }).report();
  assert.equal(report.summary.openingTradeExpenses, 475);
  assert.equal(report.summary.openingGrossCashflow, -475);
  assert.equal(report.summary.openingTradeOrderCount, 1);
  assert.equal(report.summary.netTradeCashflow, 0);
  assert.equal(report.orders.length, 0);
  assert.equal(report.settlementEstimates.length, 0);
});


test('settlement action totals preserve unknown quantities instead of displaying zero fills', async () => {
  const report = await routeFixture({ events: [recordedSettlement()] }).report();
  assert.equal(report.actionBreakdown[0].filledAmount, null);
  assert.match(report.meta.externalHoldingsUnavailableReason, /excluded from this account report/);
});

// The graph may combine these explicit estimate fields with recorded cashflow;
// the API must preserve the independently inspectable recorded components.
test('an ITM call adds an expiry-only loss bucket without changing recorded cashflow or counts', async () => {
  const report = await routeFixture({ orders: [order()], spots: [quote(-60_000)] }).report();
  const expiryBucket = report.series.buckets.find(row => row.timestamp === '2026-03-06T00:00:00.000Z');
  assert.ok(expiryBucket, 'an expiry with no snapshot or recorded order still appears on the graph');
  assert.equal(expiryBucket.estimatedSettlementCashflow, -500);
  assert.equal(expiryBucket.estimatedCallSettlementExpenses, 500);
  assert.equal(expiryBucket.estimatedPutSettlementRevenue, 0);
  assert.equal(expiryBucket.estimateCount, 1);
  assert.equal(expiryBucket.tradeCashflow, 0);
  assert.equal(expiryBucket.tradeExpenses, 0);
  assert.equal(expiryBucket.callExpenses, 0);
  assert.equal(expiryBucket.orderCount, 0);
  assert.equal(expiryBucket.endPortfolioValue, null);
  assert.equal(report.summary.netTradeCashflow, 100);
  assert.equal(report.summary.estimatedSettlementCashflow, -500);
  assert.equal(report.meta.orderCount, 1);
  assert.equal(report.series.buckets.reduce((sum, row) => sum + row.orderCount, 0), 1);
  assert.deepEqual(report.orders.map(row => row.action), ['sell_call']);

  let combined = report.summary.openingGrossCashflow + report.summary.openingEstimatedSettlementCashflow;
  const graphValues = report.series.buckets.map(row => (combined += row.tradeCashflow + row.estimatedSettlementCashflow));
  assert.deepEqual(graphValues, [100, -400], 'the estimate-inclusive graph reaches the call loss at expiry');
});

test('a partial call buyback reduces estimated expiry loss to the remaining exposure', async () => {
  const report = await routeFixture({
    orders: [
      order({ filled_amount: 2, intended_amount: 2, total_value: 200 }),
      order({ id: 2, timestamp: '2026-03-04T12:00:00.000Z', action: 'buyback_call', filled_amount: 0.5, intended_amount: 0.5, total_value: 60, fill_price: 120 }),
    ],
    spots: [quote(-60_000)],
  }).report();
  const expiryBucket = report.series.buckets.find(row => row.estimateCount > 0);
  assert.equal(report.settlementEstimates[0].filled_amount, 1.5);
  assert.equal(expiryBucket.estimatedCallSettlementExpenses, 750);
  assert.equal(expiryBucket.estimatedSettlementCashflow, -750);
  assert.equal(expiryBucket.estimateCount, 1);
  assert.equal(report.summary.netTradeCashflow, 140);
  assert.equal(report.summary.estimatedSettlementCashflow, -750);
  assert.equal(report.summary.netTradeCashflow + report.summary.estimatedSettlementCashflow, -610);
  assert.equal(report.meta.orderCount, 2);
  assert.equal(report.series.buckets.reduce((sum, row) => sum + row.tradeExpenses, 0), 60);
});

test('ITM put proceeds are positive estimate components and do not become recorded revenue', async () => {
  const report = await routeFixture({
    orders: [order({ action: 'buy_put', instrument_name: 'ETH-20260306-3000-P', strike: 3000, total_value: 50 })],
    spots: [quote(-60_000)],
  }).report();
  const expiryBucket = report.series.buckets.find(row => row.estimateCount > 0);
  assert.equal(expiryBucket.estimatedSettlementCashflow, 500);
  assert.equal(expiryBucket.estimatedPutSettlementRevenue, 500);
  assert.equal(expiryBucket.estimatedCallSettlementExpenses, 0);
  assert.equal(expiryBucket.putRevenue, 0);
  assert.equal(expiryBucket.tradeRevenue, 0);
  assert.equal(expiryBucket.orderCount, 0);
  assert.equal(report.summary.putNetCashflow, -50);
  assert.equal(report.summary.netTradeCashflow, -50);
  assert.equal(report.summary.estimatedSettlementCashflow, 500);
  assert.equal(report.summary.netTradeCashflow + report.summary.estimatedSettlementCashflow, 450);
});

test('pre-window estimates carry into the graph baseline without reappearing in current buckets', async () => {
  const report = await routeFixture({
    from: '2026-03-07T00:00:00.000Z',
    to: '2026-03-14T00:00:00.000Z',
    orders: [
      order(),
      order({ id: 2, timestamp: '2026-03-10T00:00:00.000Z', action: 'buy_put', instrument_name: 'ETH-20260313-2700-P', strike: 2700, total_value: 50 }),
    ],
    spots: [quote(-60_000), { timestamp: '2026-03-13T07:59:00.000Z', price: 2500 }],
    openingCashflow: { revenue: 100, expenses: 0, gross_cashflow: 100, order_count: 1 },
  }).report();
  assert.equal(report.summary.openingGrossCashflow, 100);
  assert.equal(report.summary.openingEstimatedSettlementCashflow, -500);
  assert.equal(report.summary.openingTradeOrderCount, 1);
  assert.equal(report.summary.netTradeCashflow, -50);
  assert.equal(report.summary.estimatedSettlementCashflow, 200);
  assert.equal(report.series.buckets.reduce((sum, row) => sum + row.estimatedSettlementCashflow, 0), 200);
  assert.ok(report.series.buckets.every(row => row.timestamp >= report.meta.from));
  assert.equal(report.settlementEstimates.length, 1);
  assert.equal(report.settlementEstimates[0].instrument_name, 'ETH-20260313-2700-P');
  const openingInclusive = report.summary.openingGrossCashflow + report.summary.openingEstimatedSettlementCashflow;
  const closingInclusive = report.series.buckets.reduce((sum, row) => sum + row.tradeCashflow + row.estimatedSettlementCashflow, openingInclusive);
  assert.equal(openingInclusive, -400);
  assert.equal(closingInclusive, -250);
});

test('recorded settlements never also contribute estimated graph values, including zero and unknown USD', async (t) => {
  for (const cashflow of ['-475', '0', null]) {
    await t.test(`recorded cashflow ${cashflow}`, async () => {
      const report = await routeFixture({
        orders: [order()], spots: [quote(-60_000)], events: [recordedSettlement({ cashflow_usd: cashflow })],
      }).report();
      assert.equal(report.summary.openingEstimatedSettlementCashflow, 0);
      assert.equal(report.summary.estimatedSettlementCashflow, 0);
      assert.equal(report.meta.settlementEstimateCount, 0);
      for (const row of report.series.buckets) {
        assert.equal(row.estimatedSettlementCashflow, 0);
        assert.equal(row.estimatedCallSettlementExpenses, 0);
        assert.equal(row.estimatedPutSettlementRevenue, 0);
        assert.equal(row.estimateCount, 0);
      }
      assert.equal(report.summary.netTradeCashflow, cashflow === '-475' ? -375 : 100);
      assert.equal(report.meta.unvaluedRecordedSettlementCount, cashflow == null ? 1 : 0);
      assert.equal(report.meta.orderCount, cashflow == null ? 1 : 2);
    });
  }
});

test('missing estimates and unvalued recorded settlements are reported separately before and within the window', async () => {
  const oldMissing = 'ETH-20260227-2100-C';
  const oldUnvalued = 'ETH-20260227-2200-C';
  const currentMissing = 'ETH-20260306-2300-C';
  const currentUnvalued = 'ETH-20260306-2400-C';
  const report = await routeFixture({
    orders: [
      order({ id: 1, timestamp: '2026-02-20T00:00:00.000Z', instrument_name: oldMissing }),
      order({ id: 2, timestamp: '2026-02-20T00:00:00.000Z', instrument_name: oldUnvalued }),
      order({ id: 3, instrument_name: currentMissing }),
      order({ id: 4, instrument_name: currentUnvalued }),
    ],
    events: [
      recordedSettlement({ event_id: 'old-unvalued', timestamp: '2026-02-27T08:00:00.000Z', instrument_name: oldUnvalued, cashflow_usd: null }),
      recordedSettlement({ event_id: 'current-unvalued', instrument_name: currentUnvalued, cashflow_usd: null }),
    ],
  }).report();
  assert.equal(report.meta.openingMissingSettlementEstimateCount, 1);
  assert.equal(report.meta.missingSettlementEstimateCount, 1);
  assert.equal(report.meta.openingUnvaluedRecordedSettlementCount, 1);
  assert.equal(report.meta.unvaluedRecordedSettlementCount, 1);
  assert.deepEqual(report.missingSettlementEstimates.map(row => row.instrument_name), [currentMissing]);
  assert.equal(report.summary.openingEstimatedSettlementCashflow, 0);
  assert.equal(report.summary.estimatedSettlementCashflow, 0);
  assert.equal(report.settlementEstimates.length, 0);
  assert.equal(report.meta.orderCount, 2);
  assert.equal(report.series.buckets.reduce((sum, row) => sum + row.estimateCount, 0), 0);
});

test('a custom window clamps its first partial bucket without losing recorded or estimated cashflow', async () => {
  const from = '2026-03-06T07:30:00.000Z';
  const report = await routeFixture({
    from,
    to: '2026-03-07T12:00:00.000Z',
    orders: [order({ timestamp: '2026-03-06T07:35:00.000Z' })],
    snapshots: [snapshot('2026-03-06T07:40:00.000Z', 10_100)],
    spots: [quote(-60_000)],
  }).report();
  assert.equal(report.meta.bucketMs, 60 * 60 * 1000);
  assert.equal(report.series.buckets[0].timestamp, from);
  assert.equal(report.series.buckets[0].tradeCashflow, 100);
  assert.equal(report.series.buckets[0].orderCount, 1);
  assert.equal(report.series.buckets[0].endPortfolioValue, 10_100);
  assert.equal(report.series.buckets[1].timestamp, EXPIRY);
  assert.equal(report.series.buckets[1].estimatedSettlementCashflow, -500);
  assert.ok(report.series.buckets.every(row => row.timestamp >= from));
  assert.equal(report.series.buckets.reduce((sum, row) => sum + row.tradeCashflow + row.estimatedSettlementCashflow, 0), -400);
});

test('offsetting call and put estimates retain their gross components and worthless-expiry count', async () => {
  const report = await routeFixture({
    orders: [
      order(),
      order({ id: 2, action: 'buy_put', instrument_name: 'ETH-20260306-3000-P', strike: 3000, total_value: 50 }),
      order({ id: 3, instrument_name: 'ETH-20260306-4000-C', strike: 4000, total_value: 10 }),
    ],
    spots: [quote(-60_000)],
  }).report();
  const expiryBucket = report.series.buckets.find(row => row.estimateCount > 0);
  assert.equal(expiryBucket.estimatedSettlementCashflow, 0);
  assert.equal(expiryBucket.estimatedCallSettlementExpenses, 500);
  assert.equal(expiryBucket.estimatedPutSettlementRevenue, 500);
  assert.equal(expiryBucket.estimateCount, 3);
  assert.equal(expiryBucket.orderCount, 0);
  assert.equal(report.summary.estimatedSettlementCashflow, 0);
  assert.equal(report.meta.settlementEstimateCount, 3);
  assert.equal(report.summary.netTradeCashflow, 60);
});

test('expiry at a window boundary appears once, either in current estimates or the opening estimate baseline', async (t) => {
  for (const [name, from, to, openingExpected, currentExpected] of [
    ['exactly at from', EXPIRY, TO, 0, -500],
    ['one millisecond before from', isoAt(1), TO, -500, 0],
    ['after to', FROM, isoAt(-1), 0, 0],
  ]) {
    await t.test(name, async () => {
      const report = await routeFixture({ orders: [order()], spots: [quote(-60_000)], from, to }).report();
      assert.equal(report.summary.openingEstimatedSettlementCashflow, openingExpected);
      assert.equal(report.summary.estimatedSettlementCashflow, currentExpected);
      assert.equal(report.series.buckets.reduce((sum, row) => sum + row.estimatedSettlementCashflow, 0), currentExpected);
      assert.equal(report.series.buckets.reduce((sum, row) => sum + row.estimateCount, 0), currentExpected === 0 ? 0 : 1);
      assert.ok(report.series.buckets.every(row => row.timestamp >= from));
      if (currentExpected !== 0) assert.equal(report.series.buckets[0].timestamp, EXPIRY);
    });
  }
});

test('covered-call summary pairs call cashflow with the move on one backing ETH per contract', async () => {
  // Sold 1 call at spot 2000 for $100; settled at spot 2500 for -$500. The ETH behind it gained $500.
  const settled = await routeFixture({ orders: [order()], spots: [quote(-60_000)] }).report();
  assert.equal(settled.summary.coveredCall.callPnl, -400);
  assert.equal(settled.summary.coveredCall.ethMove, 500);
  assert.equal(settled.summary.coveredCall.net, 100);
  assert.equal(settled.summary.coveredCall.openContracts, 0);
  assert.equal(settled.summary.coveredCall.unpricedLegs, 0);

  // Still open at the window close: the backing ETH is marked at the closing snapshot spot.
  const open = await routeFixture({
    orders: [order({ instrument_name: 'ETH-20260320-2000-C', expiry: Math.floor(Date.parse('2026-03-20T08:00:00.000Z') / 1000) })],
    snapshots: [snapshot('2026-03-07T12:00:00.000Z', 10_000, 2200)],
  }).report();
  assert.equal(open.summary.coveredCall.callPnl, 100);
  assert.equal(open.summary.coveredCall.ethMove, 200);
  assert.equal(open.summary.coveredCall.openContracts, 1);
});

test('DRY RUN simulated fills are excluded from every cashflow total', async () => {
  const report = await routeFixture({
    orders: [order(), order({ id: 2, reason: 'DRY RUN: simulated sell_call (post_only)', total_value: 9_999 })],
    spots: [quote(-60_000)],
  }).report();
  assert.equal(report.meta.orderCount, 1);
  assert.equal(report.summary.callNetCashflow, 100);
  assert.equal(report.summary.coveredCall.callPnl, -400);
});

const pairedPoint = (timestamp, sourceValue, destinationValue, extra = {}) => ({ timestamp, ts: Date.parse(timestamp),
  sourceValue, destinationValue, combinedValue: sourceValue == null || destinationValue == null ? null : sourceValue + destinationValue,
  kind: 'history', reason: null, ...extra });
function reportingFixture(rows, extra = {}) {
  return { config: { accountId: '25923', destinationAccountId: '120934' }, rows, baseline: null, latest: null,
    coverage: { configured: true, historyPeriodMs: 900000, freshnessMs: 600000,
      firstTimestamp: rows[0]?.timestamp ?? null, lastTimestamp: rows.at(-1)?.timestamp ?? null,
      latestStatus: 'complete', reason: null, historyStatus: 'complete' }, ...extra };
}

test('paired reporting includes ALT assets and external ETH once while preserving source and option accounting', async () => {
  const from = EXPIRY, to = isoAt(900000);
  const source = { from, to, baseline: snapshot(isoAt(-60000), 100), snapshots: [snapshot(to, 95)],
    orders: [order({ timestamp: isoAt(1000), action: 'sell_put', instrument_name: 'ETH-20260320-1800-P', total_value: 20 })],
    spots: [quote(0, 2000), quote(900000, 2100)], exposureHistory: [{ effective_at: from, external_eth: '4' }] };
  const legacy = await routeFixture(source).report();
  // A five dollar internal transfer cancels in combined NAV; the option sale
  // remains the same existing cashflow regardless of reporting holdings.
  const report = await routeFixture({ ...source, reporting: reportingFixture([
    pairedPoint(from, 100, 5), pairedPoint(to, 95, 10),
  ]) }).report();
  assert.deepEqual(report.series.portfolio, legacy.series.portfolio);
  assert.deepEqual(report.series.buckets, legacy.series.buckets);
  const { combined, ...summary } = report.summary;
  const { combined: ignored, ...legacySummary } = legacy.summary;
  assert.deepEqual(summary, legacySummary);
  assert.equal(report.summary.netTradeCashflow, 20);
  assert.deepEqual(report.series.combinedPortfolio.map(row => row.portfolioValue), [105, 105]);
  assert.deepEqual(report.series.combinedPortfolio.map(row => row.fullPortfolioValue), [8105, 8505]);
  assert.deepEqual(combined, { openingValue: 105, closingValue: 105, portfolioChange: 0, highWatermark: 105, lowWatermark: 105 });
});

test('combined coverage never labels earlier source-only history as a two-account opening', async () => {
  const report = await routeFixture({ from: EXPIRY, to: isoAt(900000),
    baseline: snapshot(isoAt(-60000), 100), snapshots: [snapshot(isoAt(900000), 95)],
    reporting: reportingFixture([pairedPoint(isoAt(900000), 95, 10)]),
  }).report();
  assert.equal(report.summary.openingValue, 100);
  assert.equal(report.summary.combined.openingValue, null);
  assert.equal(report.summary.combined.portfolioChange, null);
  assert.deepEqual(report.series.combinedPortfolio.map(row => row.portfolioValue), [null, 105]);
});

test('a baseline keeps its original age and cannot extend combined coverage across missing closes', async () => {
  const reporting = reportingFixture([pairedPoint(isoAt(1800000), 90, 10)], {
    baseline: pairedPoint(isoAt(-840000), 100, 5),
  });
  const report = await routeFixture({ from: EXPIRY, to: isoAt(1800000), reporting }).report();
  assert.equal(report.summary.combined.openingValue, 105);
  assert.equal(report.series.combinedPortfolio[1].portfolioValue, null);
  assert.equal(report.series.combinedPortfolio[1].ts, expiryMs + 60000);
  const stale = await routeFixture({ from: EXPIRY, to: isoAt(1800000),
    reporting: reportingFixture([], { baseline: pairedPoint(isoAt(-900001), 100, 5) }),
  }).report();
  assert.equal(stale.summary.combined.openingValue, null);
  assert.equal(stale.summary.combined.closingValue, null);
});

test('combined chart sampling retains the opening, missing leg and both sides of a gap', async () => {
  const rows = Array.from({ length: 2000 }, (_, i) => pairedPoint(isoAt(i * 900000), 100 + i, 10));
  rows[1000] = pairedPoint(rows[1000].timestamp, 1100, null, { reason: 'historical_account_value_missing' });
  const report = await routeFixture({ from: EXPIRY, to: rows.at(-1).timestamp, reporting: reportingFixture(rows) }).report();
  for (const index of [0, 999, 1000, 1001, 1999]) {
    const point = report.series.combinedPortfolio.find(row => row.ts === rows[index].ts);
    assert.ok(point, `retains point ${index}`);
    assert.equal(point.portfolioValue, rows[index].combinedValue);
  }
  assert.ok(report.series.combinedPortfolio.length < 1000);
});

test('stale current holdings are not a current combined total, but their observed history remains', async () => {
  const current = pairedPoint(EXPIRY, 100, 10, { kind: 'current', accounts: [{ subaccountId: '120934',
    label: 'DRV-NOOP', valueUsd: 10, collaterals: [{ asset: 'DRV', amount: 16, markValue: 4.88 }] }] });
  const reporting = reportingFixture([current], { latest: current });
  reporting.coverage.latestStatus = 'stale';
  const report = await routeFixture({ from: EXPIRY, to: isoAt(600001), reporting }).report();
  assert.equal(report.latestCombinedPortfolio.combinedValue, null);
  assert.equal(report.latestCombinedPortfolio.accounts[0].collaterals[0].amount, 16);
  assert.equal(report.series.combinedPortfolio[0].portfolioValue, 110);
  assert.equal(report.series.combinedPortfolio.at(-1).portfolioValue, null);
  assert.equal(report.summary.combined.closingValue, null);
});

test('aligned historical closes take precedence at ties and zero or negative NAV remains evidence', async () => {
  const report = await routeFixture({ from: EXPIRY, to: isoAt(900000), reporting: reportingFixture([
    pairedPoint(EXPIRY, 1, 1, { kind: 'current' }), pairedPoint(EXPIRY, -5, 5), pairedPoint(isoAt(900000), -10, 5),
  ]) }).report();
  assert.deepEqual(report.series.combinedPortfolio.map(row => row.portfolioValue), [0, -5]);
  assert.deepEqual(report.summary.combined, { openingValue: 0, closingValue: -5, portfolioChange: -5, highWatermark: 0, lowWatermark: -5 });
});

test('full combined value needs a bounded historical ETH mark and never looks ahead', async () => {
  const reporting = reportingFixture([pairedPoint(EXPIRY, 100, 10), pairedPoint(isoAt(900000), 100, 10)]);
  const report = await routeFixture({ from: EXPIRY, to: isoAt(900000), reporting,
    exposureHistory: [{ effective_at: EXPIRY, external_eth: '4' }], spots: [quote(1, 2000)],
  }).report();
  assert.deepEqual(report.series.combinedPortfolio.map(row => row.portfolioValue), [110, 110]);
  assert.deepEqual(report.series.combinedPortfolio.map(row => row.fullPortfolioValue), [null, 8110]);
});

test('unknown external holdings do not produce a full combined total, while recorded zero holdings do', async () => {
  const base = { from: EXPIRY, to: isoAt(900000), reporting: reportingFixture([pairedPoint(EXPIRY, 100, 10)]) };
  const unknown = await routeFixture(base).report();
  assert.equal(unknown.series.combinedPortfolio[0].portfolioValue, 110);
  assert.equal(unknown.series.combinedPortfolio[0].fullPortfolioValue, null);
  const zero = await routeFixture({ ...base, exposureHistory: [{ effective_at: EXPIRY, external_eth: '0' }] }).report();
  assert.equal(zero.series.combinedPortfolio[0].fullPortfolioValue, 110);
});

test('an expired closing observation inserts a gap at expiry so earlier hover points cannot carry it forward', async () => {
  const report = await routeFixture({ from: EXPIRY, to: isoAt(3600000),
    reporting: reportingFixture([pairedPoint(EXPIRY, 100, 10, { kind: 'current' })]),
  }).report();
  assert.equal(report.series.combinedPortfolio[1].ts, expiryMs + 600001);
  assert.equal(report.series.combinedPortfolio[1].portfolioValue, null);
  assert.equal(report.summary.combined.closingValue, null);
});
