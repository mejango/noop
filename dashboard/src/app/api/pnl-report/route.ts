import { NextRequest, NextResponse } from 'next/server';
import { getEconomicHistory, getOrderCashflowTotalsBefore, getOrdersInRange, getPortfolioSnapshotBefore, getPortfolioSnapshotsInRange, getSpotPricesAtOrBefore } from '@/lib/db';
import { deriveExpirySettlementReport, getExpiredExposures, parseInstrument, SETTLEMENT_SPOT_MAX_AGE_MS } from '@/lib/expiry-settlement';
import { cachedJsonRoute } from '@/lib/response-cache';
import { dashboardRangeMs } from '@/lib/dashboard-ranges';

export const dynamic = 'force-dynamic';

type SnapshotRow = {
  timestamp: string;
  spot_price: number;
  usdc_balance: number;
  eth_balance: number;
  total_unrealized_pnl: number;
  total_realized_pnl: number;
  portfolio_value_usd: number;
};

type OrderRow = {
  id: number | string;
  timestamp: string;
  action: string;
  success: number;
  reason: string | null;
  instrument_name: string | null;
  strike: number | null;
  expiry: string | number | null;
  delta: number | null;
  price: number | null;
  intended_amount: number | null;
  filled_amount: number | null;
  fill_price: number | null;
  total_value: number | null;
  spot_price: number | null;
  actual_cashflow_usd?: number;
  source?: string;
};

function parseDateParam(value: string | null, fallback: number): Date {
  if (!value) return new Date(fallback);
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? new Date(fallback) : parsed;
}

function resolveWindow(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const range = searchParams.get('range') || '30d';
  const now = Date.now();
  const to = parseDateParam(searchParams.get('to'), now);
  const durationMs = dashboardRangeMs(range, '30d');
  const from = searchParams.get('from')
    ? parseDateParam(searchParams.get('from'), to.getTime() - durationMs)
    : new Date(to.getTime() - durationMs);
  if (from > to) return { range, from: to, to: from };
  return { range, from, to };
}

function signedCashflow(action: string, totalValue: number | null | undefined, actualCashflow?: number): number {
  if (actualCashflow !== undefined) return actualCashflow;
  const value = Number(totalValue ?? 0);
  if (!Number.isFinite(value) || value === 0) return 0;
  switch (action) {
    case 'sell_put':
    case 'sell_call':
      return value;
    case 'buy_put':
    case 'buyback_call':
    case 'settle_call':
      return -value;
    case 'settle_put':
      return value;
    default:
      return 0;
  }
}

// A current insurance-budget setting is not a record of historical external holdings.
const portfolioValue = (row: { portfolio_value_usd?: number } | null | undefined) =>
  Number(row?.portfolio_value_usd ?? 0);

const PERFORMANCE_UNAVAILABLE_REASON = 'Return and drawdown require reconciled deposits and withdrawals with portfolio valuations around those flows. Portfolio change is the raw account balance change.';

const isCallAction = (a: string) => a === 'sell_call' || a === 'buyback_call' || a === 'settle_call';
const isPutAction = (a: string) => a === 'buy_put' || a === 'sell_put' || a === 'settle_put';

function cashflowParts(action: string, totalValue: number | null | undefined, actualCashflow?: number) {
  const value = Number(totalValue ?? 0);
  const zero = {
    revenue: 0,
    expenses: 0,
    putRevenue: 0,
    putExpenses: 0,
    callRevenue: 0,
    callExpenses: 0,
  };
  if (actualCashflow !== undefined) {
    const revenue = Math.max(0, actualCashflow);
    const expenses = Math.max(0, -actualCashflow);
    return {
      revenue, expenses,
      putRevenue: isPutAction(action) ? revenue : 0,
      putExpenses: isPutAction(action) ? expenses : 0,
      callRevenue: isCallAction(action) ? revenue : 0,
      callExpenses: isCallAction(action) ? expenses : 0,
    };
  }
  if (!Number.isFinite(value) || value === 0) return zero;

  switch (action) {
    case 'sell_put':
      return { ...zero, revenue: value, putRevenue: value };
    case 'sell_call':
      return { ...zero, revenue: value, callRevenue: value };
    case 'buy_put':
      return { ...zero, expenses: value, putExpenses: value };
    case 'buyback_call':
    case 'settle_call':
      return { ...zero, expenses: value, callExpenses: value };
    case 'settle_put':
      return { ...zero, revenue: value, putRevenue: value };
    default:
      return zero;
  }
}

function downsampleKeepLast<T>(rows: T[], maxPoints: number, getTs: (row: T) => number): T[] {
  if (rows.length <= maxPoints) return rows;
  const start = getTs(rows[0]);
  const end = getTs(rows[rows.length - 1]);
  const span = Math.max(1, end - start);
  const bucketMs = Math.max(1, Math.ceil(span / maxPoints));
  const buckets = new Map<number, T>();
  for (const row of rows) {
    const key = Math.floor((getTs(row) - start) / bucketMs);
    buckets.set(key, row);
  }
  return Array.from(buckets.values());
}

function chooseBucketMs(durationMs: number): number {
  if (durationMs <= 2 * 24 * 60 * 60 * 1000) return 60 * 60 * 1000;
  if (durationMs <= 45 * 24 * 60 * 60 * 1000) return 24 * 60 * 60 * 1000;
  return 7 * 24 * 60 * 60 * 1000;
}

function bucketKey(ts: number, bucketMs: number): number {
  return Math.floor(ts / bucketMs) * bucketMs;
}

function getPnlResponse(req: NextRequest) {
  try {
    const { range, from, to } = resolveWindow(req);
    const fromIso = from.toISOString();
    const toIso = to.toISOString();

    const rawSnapshots = getPortfolioSnapshotsInRange(fromIso, toIso) as SnapshotRow[];
    const baseline = getPortfolioSnapshotBefore(fromIso) as SnapshotRow | undefined;
    // Resolve each expiry directly; a moving latest-N spot window makes old reports unstable.
    const allOrders = getOrdersInRange('1970-01-01T00:00:00.000Z', toIso) as OrderRow[];
    const economicHistory = getEconomicHistory('1970-01-01T00:00:00.000Z', toIso);
    const recordedSettlements = economicHistory.events.filter(event => event.event_type === 'settlement');
    const settledInstruments = new Set(recordedSettlements.flatMap(event => event.instrument_name ? [event.instrument_name] : []));
    const settlementRows: OrderRow[] = recordedSettlements.flatMap(event => {
      if (event.cashflow_usd == null || event.cashflow_usd === '' || !Number.isFinite(Number(event.cashflow_usd))) return [];
      const cashflow = Number(event.cashflow_usd);
      const parsed = parseInstrument(event.instrument_name);
      return [{
        id: `economic:${event.event_id}`,
        timestamp: event.timestamp,
        action: parsed ? (parsed.optionType === 'C' ? 'settle_call' : 'settle_put') : 'settlement',
        success: 1,
        reason: 'Recorded exchange settlement',
        instrument_name: event.instrument_name,
        strike: parsed?.strike ?? null,
        expiry: parsed ? Math.floor(parsed.expiryMs / 1000) : null,
        delta: null,
        price: null,
        intended_amount: null,
        filled_amount: null,
        fill_price: null,
        total_value: Math.abs(cashflow),
        spot_price: null,
        actual_cashflow_usd: cashflow,
        source: event.source,
      }];
    });
    const expiryTimestamps = Array.from(new Set(getExpiredExposures(allOrders, to.getTime())
      .filter(exposure => !settledInstruments.has(exposure.instrument_name))
      .map(exposure => new Date(exposure.expiryMs).toISOString())));
    const spotRows = getSpotPricesAtOrBefore(expiryTimestamps, SETTLEMENT_SPOT_MAX_AGE_MS);
    const estimatedSettlements = deriveExpirySettlementReport(allOrders, spotRows, to.getTime(), settledInstruments);
    const settlementEstimates = estimatedSettlements.estimates.filter(row => row.timestamp >= fromIso);
    const openingEstimatedSettlementCashflow = estimatedSettlements.estimates
      .filter(row => row.timestamp < fromIso)
      .reduce((sum, row) => sum + signedCashflow(row.action, row.total_value), 0);
    const missingSettlementEstimates = estimatedSettlements.missing.filter(row => row.timestamp >= fromIso);
    const openingMissingSettlementEstimateCount = estimatedSettlements.missing.filter(row => row.timestamp < fromIso).length;
    const unvaluedRecordedSettlements = recordedSettlements.filter(event =>
      event.cashflow_usd == null || event.cashflow_usd === '' || !Number.isFinite(Number(event.cashflow_usd)));
    const orders = [...allOrders.filter(o => o.success === 1 && Number(o.filled_amount ?? 0) > 0), ...settlementRows]
      .filter(o => o.timestamp >= fromIso)
      .sort((a, b) => a.timestamp.localeCompare(b.timestamp));
    const openingCashflow = { ...(getOrderCashflowTotalsBefore(fromIso) || { revenue: 0, expenses: 0, gross_cashflow: 0, order_count: 0 }) };
    for (const settlement of settlementRows) {
      if (settlement.timestamp >= fromIso) continue;
      const parts = cashflowParts(settlement.action, settlement.total_value, settlement.actual_cashflow_usd);
      openingCashflow.revenue += parts.revenue;
      openingCashflow.expenses += parts.expenses;
      openingCashflow.gross_cashflow += settlement.actual_cashflow_usd!;
      openingCashflow.order_count += 1;
    }

    const opening = baseline ?? rawSnapshots[0] ?? null;
    const closing = rawSnapshots[rawSnapshots.length - 1] ?? opening;

    const portfolioSeries = rawSnapshots.map((row) => ({
      timestamp: row.timestamp,
      ts: new Date(row.timestamp).getTime(),
      portfolioValue: portfolioValue(row),
      unrealizedPnl: Number(row.total_unrealized_pnl ?? 0),
      spotPrice: Number(row.spot_price ?? 0),
      usdcBalance: Number(row.usdc_balance ?? 0),
      ethBalance: Number(row.eth_balance ?? 0),
    }));

    const seriesWithOpening = opening && (portfolioSeries.length === 0 || portfolioSeries[0].timestamp !== opening.timestamp)
      ? [{
          timestamp: fromIso,
          ts: from.getTime(),
          portfolioValue: portfolioValue(opening),
          unrealizedPnl: Number(opening.total_unrealized_pnl ?? 0),
          spotPrice: Number(opening.spot_price ?? 0),
          usdcBalance: Number(opening.usdc_balance ?? 0),
          ethBalance: Number(opening.eth_balance ?? 0),
        }, ...portfolioSeries]
      : portfolioSeries;

    let highWatermark = portfolioValue(opening);
    let lowWatermark = highWatermark;
    for (const point of seriesWithOpening) {
      highWatermark = Math.max(highWatermark, point.portfolioValue);
      lowWatermark = Math.min(lowWatermark, point.portfolioValue);
    }

    const netTradeCashflow = orders.reduce((sum, order) => sum + signedCashflow(order.action, order.total_value, order.actual_cashflow_usd), 0);
    const putNetCashflow = orders.reduce((sum, order) =>
      sum + (isPutAction(order.action) ? signedCashflow(order.action, order.total_value, order.actual_cashflow_usd) : 0), 0);
    const callNetCashflow = orders.reduce((sum, order) =>
      sum + (isCallAction(order.action) ? signedCashflow(order.action, order.total_value, order.actual_cashflow_usd) : 0), 0);

    const actionMap = new Map<string, { action: string; count: number; grossValue: number; cashflow: number; filledAmount: number | null }>();
    for (const order of orders) {
      const existing = actionMap.get(order.action) || {
        action: order.action,
        count: 0,
        grossValue: 0,
        cashflow: 0,
        filledAmount: 0,
      };
      existing.count += 1;
      existing.grossValue += Number(order.total_value ?? 0);
      existing.cashflow += signedCashflow(order.action, order.total_value, order.actual_cashflow_usd);
      const filledAmount = order.filled_amount ?? order.intended_amount;
      existing.filledAmount = existing.filledAmount == null || filledAmount == null || !Number.isFinite(Number(filledAmount))
        ? null
        : existing.filledAmount + Number(filledAmount);
      actionMap.set(order.action, existing);
    }

    const durationMs = Math.max(1, to.getTime() - from.getTime());
    const bucketMs = chooseBucketMs(durationMs);
    const bucketMap = new Map<number, {
      bucketTs: number;
      tradeCashflow: number;
      tradeRevenue: number;
      tradeExpenses: number;
      putCashflow: number;
      putRevenue: number;
      putExpenses: number;
      callCashflow: number;
      callRevenue: number;
      callExpenses: number;
      orderCount: number;
      estimatedSettlementCashflow: number;
      estimatedCallSettlementExpenses: number;
      estimatedPutSettlementRevenue: number;
      estimateCount: number;
      endPortfolioValue: number | null;
      endUnrealizedPnl: number | null;
    }>();

    const getBucket = (timestampMs: number) => {
      const key = bucketKey(timestampMs, bucketMs);
      let bucket = bucketMap.get(key);
      if (!bucket) {
        bucket = {
          bucketTs: key,
          tradeCashflow: 0,
          tradeRevenue: 0,
          tradeExpenses: 0,
          putCashflow: 0,
          putRevenue: 0,
          putExpenses: 0,
          callCashflow: 0,
          callRevenue: 0,
          callExpenses: 0,
          orderCount: 0,
          estimatedSettlementCashflow: 0,
          estimatedCallSettlementExpenses: 0,
          estimatedPutSettlementRevenue: 0,
          estimateCount: 0,
          endPortfolioValue: null,
          endUnrealizedPnl: null,
        };
        bucketMap.set(key, bucket);
      }
      return bucket;
    };

    for (const point of portfolioSeries) {
      const bucket = getBucket(point.ts);
      bucket.endPortfolioValue = point.portfolioValue;
      bucket.endUnrealizedPnl = point.unrealizedPnl;
    }

    for (const order of orders) {
      const bucket = getBucket(new Date(order.timestamp).getTime());
      const cashflow = signedCashflow(order.action, order.total_value, order.actual_cashflow_usd);
      const parts = cashflowParts(order.action, order.total_value, order.actual_cashflow_usd);
      bucket.tradeCashflow += cashflow;
      bucket.tradeRevenue += parts.revenue;
      bucket.tradeExpenses += parts.expenses;
      bucket.putRevenue += parts.putRevenue;
      bucket.putExpenses += parts.putExpenses;
      bucket.callRevenue += parts.callRevenue;
      bucket.callExpenses += parts.callExpenses;
      if (isPutAction(order.action)) bucket.putCashflow += cashflow;
      if (isCallAction(order.action)) bucket.callCashflow += cashflow;
      bucket.orderCount += 1;
    }

    // Estimates are a separate display series: they never become recorded fills,
    // change gross cashflow, or overwrite authoritative settlement records.
    for (const estimate of settlementEstimates) {
      const bucket = getBucket(new Date(estimate.timestamp).getTime());
      bucket.estimatedSettlementCashflow += signedCashflow(estimate.action, estimate.total_value);
      if (estimate.action === 'settle_call') bucket.estimatedCallSettlementExpenses += estimate.total_value;
      if (estimate.action === 'settle_put') bucket.estimatedPutSettlementRevenue += estimate.total_value;
      bucket.estimateCount += 1;
    }

    const openingValue = portfolioValue(opening);
    const closingValue = closing ? portfolioValue(closing) : openingValue;
    const openingUnrealized = Number(opening?.total_unrealized_pnl ?? 0);
    const closingUnrealized = Number(closing?.total_unrealized_pnl ?? openingUnrealized);
    const openingSpot = Number(opening?.spot_price ?? 0);
    const closingSpot = Number(closing?.spot_price ?? openingSpot);
    const portfolioChange = closingValue - openingValue;
    const unrealizedChange = closingUnrealized - openingUnrealized;
    const spotChangePct = openingSpot > 0 ? ((closingSpot - openingSpot) / openingSpot) * 100 : 0;

    return NextResponse.json({
      meta: {
        range,
        from: fromIso,
        to: toIso,
        generatedAt: new Date().toISOString(),
        snapshotCount: portfolioSeries.length,
        orderCount: orders.length,
        hasBaseline: Boolean(opening),
        bucketMs,
        insuredExternalEth: 0,
        valuationScope: 'derive_subaccount',
        externalHoldingsUnavailableReason: 'External insured holdings are excluded from this account report; the current insurance setting is not applied to historical balances.',
        performanceAvailable: false,
        performanceUnavailableReason: PERFORMANCE_UNAVAILABLE_REASON,
        settlementEstimateCount: settlementEstimates.length,
        missingSettlementEstimateCount: missingSettlementEstimates.length,
        openingMissingSettlementEstimateCount,
        openingUnvaluedRecordedSettlementCount: unvaluedRecordedSettlements.filter(event => event.timestamp < fromIso).length,
        settlementEstimateMaxSpotAgeMs: SETTLEMENT_SPOT_MAX_AGE_MS,
        economicHistoryAvailable: economicHistory.available,
        accountingCoverage: economicHistory.coverage,
        unvaluedRecordedSettlementCount: unvaluedRecordedSettlements.filter(event => event.timestamp >= fromIso).length,
        cashflowBasis: 'Recorded bot-order cashflows plus valued exchange settlements. External fills and fees are not reconciled; spot estimates are excluded. Gross cashflow is not realized P&L.',
      },
      summary: {
        openingValue,
        closingValue,
        portfolioChange,
        portfolioReturnPct: null,
        openingUnrealized,
        closingUnrealized,
        unrealizedChange,
        openingTradeRevenue: Number(openingCashflow.revenue ?? 0),
        openingTradeExpenses: Number(openingCashflow.expenses ?? 0),
        openingGrossCashflow: Number(openingCashflow.gross_cashflow ?? 0),
        openingTradeOrderCount: Number(openingCashflow.order_count ?? 0),
        netTradeCashflow,
        openingEstimatedSettlementCashflow,
        estimatedSettlementCashflow: settlementEstimates.reduce((sum, row) => sum + signedCashflow(row.action, row.total_value), 0),
        putNetCashflow,
        callNetCashflow,
        openingSpot,
        closingSpot,
        spotChangePct,
        highWatermark,
        lowWatermark,
        maxDrawdown: null,
        maxDrawdownPct: null,
      },
      series: {
        portfolio: downsampleKeepLast(seriesWithOpening, 720, (row) => row.ts),
        buckets: Array.from(bucketMap.values())
          .sort((a, b) => a.bucketTs - b.bucketTs)
          .map((bucket) => ({
            // The first calendar bucket may begin before the requested window.
            timestamp: new Date(Math.max(bucket.bucketTs, from.getTime())).toISOString(),
            tradeCashflow: bucket.tradeCashflow,
            tradeRevenue: bucket.tradeRevenue,
            tradeExpenses: bucket.tradeExpenses,
            putCashflow: bucket.putCashflow,
            putRevenue: bucket.putRevenue,
            putExpenses: bucket.putExpenses,
            callCashflow: bucket.callCashflow,
            callRevenue: bucket.callRevenue,
            callExpenses: bucket.callExpenses,
            orderCount: bucket.orderCount,
            estimatedSettlementCashflow: bucket.estimatedSettlementCashflow,
            estimatedCallSettlementExpenses: bucket.estimatedCallSettlementExpenses,
            estimatedPutSettlementRevenue: bucket.estimatedPutSettlementRevenue,
            estimateCount: bucket.estimateCount,
            endPortfolioValue: bucket.endPortfolioValue,
            endUnrealizedPnl: bucket.endUnrealizedPnl,
          })),
      },
      actionBreakdown: Array.from(actionMap.values()).sort((a, b) => Math.abs(b.cashflow) - Math.abs(a.cashflow)),
      settlementEstimates: settlementEstimates.map(row => ({ ...row, cashflow: signedCashflow(row.action, row.total_value) })),
      missingSettlementEstimates,
      orders: orders
        .slice()
        .reverse()
        .map((order) => ({
          ...order,
          cashflow: signedCashflow(order.action, order.total_value, order.actual_cashflow_usd),
        })),
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export function GET(request: NextRequest) {
  const key = `pnl:${request.nextUrl.searchParams.toString()}`;
  return cachedJsonRoute(request, key, () => getPnlResponse(request), {
    freshMs: 60_000,
    staleMs: 5 * 60_000,
    browserMaxAgeSeconds: 30,
  });
}
