import { NextRequest, NextResponse } from 'next/server';
import { getOrdersInRange, getPortfolioSnapshotBefore, getPortfolioSnapshotsInRange } from '@/lib/db';

export const dynamic = 'force-dynamic';

const RANGE_MS: Record<string, number> = {
  '24h': 24 * 60 * 60 * 1000,
  '3d': 3 * 24 * 60 * 60 * 1000,
  '7d': 7 * 24 * 60 * 60 * 1000,
  '14d': 14 * 24 * 60 * 60 * 1000,
  '30d': 30 * 24 * 60 * 60 * 1000,
  '90d': 90 * 24 * 60 * 60 * 1000,
  '180d': 180 * 24 * 60 * 60 * 1000,
  '1y': 365 * 24 * 60 * 60 * 1000,
};

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
  id: number;
  timestamp: string;
  action: string;
  success: number;
  reason: string | null;
  instrument_name: string | null;
  strike: number | null;
  expiry: string | null;
  delta: number | null;
  price: number | null;
  intended_amount: number | null;
  filled_amount: number | null;
  fill_price: number | null;
  total_value: number | null;
  spot_price: number | null;
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
  const from = searchParams.get('from')
    ? parseDateParam(searchParams.get('from'), to.getTime() - RANGE_MS[range] || to.getTime() - RANGE_MS['30d'])
    : new Date(to.getTime() - (RANGE_MS[range] || RANGE_MS['30d']));
  if (from > to) return { range, from: to, to: from };
  return { range, from, to };
}

function signedCashflow(action: string, totalValue: number | null | undefined): number {
  const value = Number(totalValue ?? 0);
  if (!Number.isFinite(value) || value === 0) return 0;
  switch (action) {
    case 'sell_put':
    case 'sell_call':
      return value;
    case 'buy_put':
    case 'buyback_call':
      return -value;
    default:
      return 0;
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

export function GET(req: NextRequest) {
  try {
    const { range, from, to } = resolveWindow(req);
    const fromIso = from.toISOString();
    const toIso = to.toISOString();

    const rawSnapshots = getPortfolioSnapshotsInRange(fromIso, toIso) as SnapshotRow[];
    const baseline = getPortfolioSnapshotBefore(fromIso) as SnapshotRow | undefined;
    const orders = (getOrdersInRange(fromIso, toIso) as OrderRow[]).filter(o => o.success === 1);

    const opening = baseline ?? rawSnapshots[0] ?? null;
    const closing = rawSnapshots[rawSnapshots.length - 1] ?? opening;

    const portfolioSeries = rawSnapshots.map((row) => ({
      timestamp: row.timestamp,
      ts: new Date(row.timestamp).getTime(),
      portfolioValue: Number(row.portfolio_value_usd ?? 0),
      unrealizedPnl: Number(row.total_unrealized_pnl ?? 0),
      realizedTotal: Number(row.total_realized_pnl ?? 0),
      spotPrice: Number(row.spot_price ?? 0),
      usdcBalance: Number(row.usdc_balance ?? 0),
      ethBalance: Number(row.eth_balance ?? 0),
    }));

    const seriesWithOpening = opening && (portfolioSeries.length === 0 || portfolioSeries[0].timestamp !== opening.timestamp)
      ? [{
          timestamp: fromIso,
          ts: from.getTime(),
          portfolioValue: Number(opening.portfolio_value_usd ?? 0),
          unrealizedPnl: Number(opening.total_unrealized_pnl ?? 0),
          realizedTotal: Number(opening.total_realized_pnl ?? 0),
          spotPrice: Number(opening.spot_price ?? 0),
          usdcBalance: Number(opening.usdc_balance ?? 0),
          ethBalance: Number(opening.eth_balance ?? 0),
        }, ...portfolioSeries]
      : portfolioSeries;

    let peak = opening ? Number(opening.portfolio_value_usd ?? 0) : 0;
    let highWatermark = peak;
    let lowWatermark = peak;
    let maxDrawdown = 0;
    let maxDrawdownPct = 0;
    for (const point of seriesWithOpening) {
      peak = Math.max(peak, point.portfolioValue);
      highWatermark = Math.max(highWatermark, point.portfolioValue);
      lowWatermark = Math.min(lowWatermark, point.portfolioValue);
      const dd = peak - point.portfolioValue;
      const ddPct = peak > 0 ? (dd / peak) * 100 : 0;
      if (dd > maxDrawdown) maxDrawdown = dd;
      if (ddPct > maxDrawdownPct) maxDrawdownPct = ddPct;
    }

    const netTradeCashflow = orders.reduce((sum, order) => sum + signedCashflow(order.action, order.total_value), 0);
    const putNetCashflow = orders.reduce((sum, order) =>
      sum + (order.action === 'buy_put' || order.action === 'sell_put' ? signedCashflow(order.action, order.total_value) : 0), 0);
    const callNetCashflow = orders.reduce((sum, order) =>
      sum + (order.action === 'sell_call' || order.action === 'buyback_call' ? signedCashflow(order.action, order.total_value) : 0), 0);

    const actionMap = new Map<string, { action: string; count: number; grossValue: number; cashflow: number; filledAmount: number }>();
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
      existing.cashflow += signedCashflow(order.action, order.total_value);
      existing.filledAmount += Number(order.filled_amount ?? order.intended_amount ?? 0);
      actionMap.set(order.action, existing);
    }

    const durationMs = Math.max(1, to.getTime() - from.getTime());
    const bucketMs = chooseBucketMs(durationMs);
    const bucketMap = new Map<number, {
      bucketTs: number;
      tradeCashflow: number;
      putCashflow: number;
      callCashflow: number;
      orderCount: number;
      endPortfolioValue: number | null;
      endUnrealizedPnl: number | null;
    }>();

    for (const point of portfolioSeries) {
      const key = bucketKey(point.ts, bucketMs);
      const bucket = bucketMap.get(key) || {
        bucketTs: key,
        tradeCashflow: 0,
        putCashflow: 0,
        callCashflow: 0,
        orderCount: 0,
        endPortfolioValue: null,
        endUnrealizedPnl: null,
      };
      bucket.endPortfolioValue = point.portfolioValue;
      bucket.endUnrealizedPnl = point.unrealizedPnl;
      bucketMap.set(key, bucket);
    }

    for (const order of orders) {
      const ts = new Date(order.timestamp).getTime();
      const key = bucketKey(ts, bucketMs);
      const bucket = bucketMap.get(key) || {
        bucketTs: key,
        tradeCashflow: 0,
        putCashflow: 0,
        callCashflow: 0,
        orderCount: 0,
        endPortfolioValue: null,
        endUnrealizedPnl: null,
      };
      const cashflow = signedCashflow(order.action, order.total_value);
      bucket.tradeCashflow += cashflow;
      if (order.action === 'buy_put' || order.action === 'sell_put') bucket.putCashflow += cashflow;
      if (order.action === 'sell_call' || order.action === 'buyback_call') bucket.callCashflow += cashflow;
      bucket.orderCount += 1;
      bucketMap.set(key, bucket);
    }

    const openingValue = Number(opening?.portfolio_value_usd ?? 0);
    const closingValue = Number(closing?.portfolio_value_usd ?? openingValue);
    const openingUnrealized = Number(opening?.total_unrealized_pnl ?? 0);
    const closingUnrealized = Number(closing?.total_unrealized_pnl ?? openingUnrealized);
    const openingSpot = Number(opening?.spot_price ?? 0);
    const closingSpot = Number(closing?.spot_price ?? openingSpot);
    const portfolioChange = closingValue - openingValue;
    const unrealizedChange = closingUnrealized - openingUnrealized;
    const portfolioReturnPct = openingValue > 0 ? (portfolioChange / openingValue) * 100 : 0;
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
      },
      summary: {
        openingValue,
        closingValue,
        portfolioChange,
        portfolioReturnPct,
        openingUnrealized,
        closingUnrealized,
        unrealizedChange,
        netTradeCashflow,
        putNetCashflow,
        callNetCashflow,
        openingSpot,
        closingSpot,
        spotChangePct,
        highWatermark,
        lowWatermark,
        maxDrawdown,
        maxDrawdownPct,
      },
      series: {
        portfolio: downsampleKeepLast(seriesWithOpening, 720, (row) => row.ts),
        buckets: Array.from(bucketMap.values())
          .sort((a, b) => a.bucketTs - b.bucketTs)
          .map((bucket) => ({
            timestamp: new Date(bucket.bucketTs).toISOString(),
            tradeCashflow: bucket.tradeCashflow,
            putCashflow: bucket.putCashflow,
            callCashflow: bucket.callCashflow,
            orderCount: bucket.orderCount,
            endPortfolioValue: bucket.endPortfolioValue,
            endUnrealizedPnl: bucket.endUnrealizedPnl,
          })),
      },
      actionBreakdown: Array.from(actionMap.values()).sort((a, b) => Math.abs(b.cashflow) - Math.abs(a.cashflow)),
      orders: orders
        .slice()
        .reverse()
        .map((order) => ({
          ...order,
          cashflow: signedCashflow(order.action, order.total_value),
        })),
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
