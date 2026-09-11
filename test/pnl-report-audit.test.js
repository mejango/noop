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
  snapshots = [],
  baseline,
  from = FROM,
  to = TO,
  env = {},
} = {}) {
  const spotQueries = [];
  const db = {
    getPortfolioSnapshotsInRange: (start, end) => snapshots.filter(row => row.timestamp >= start && row.timestamp <= end),
    getPortfolioSnapshotBefore: () => baseline,
    getOrdersInRange: (start, end) => orders.filter(row => row.timestamp >= start && row.timestamp <= end),
    getOrderCashflowTotalsBefore: () => ({ revenue: 0, expenses: 0, gross_cashflow: 0, order_count: 0 }),
    getEconomicHistory: (start, end) => ({
      events: events.filter(row => row.timestamp >= start && row.timestamp <= end),
      coverage: { trades: false, settlements: false, transfers: false },
      available: true,
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
  assert.match(report.meta.externalHoldingsUnavailableReason, /historical.*unavailable/i);
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
