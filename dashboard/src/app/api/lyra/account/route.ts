import { NextRequest, NextResponse } from 'next/server';
import { getPositions, getCollaterals } from '@/lib/lyra';
import { getOrderTradesSince } from '@/lib/db';
import { cachedJsonRoute } from '@/lib/response-cache';

export const dynamic = 'force-dynamic';

const RANGE_MS: Record<string, number> = {
  '1h': 60 * 60 * 1000,
  '6h': 6 * 60 * 60 * 1000,
  '24h': 24 * 60 * 60 * 1000,
  '3d': 3 * 24 * 60 * 60 * 1000,
  '6.2d': 6.2 * 24 * 60 * 60 * 1000,
  '7d': 7 * 24 * 60 * 60 * 1000,
  '14d': 14 * 24 * 60 * 60 * 1000,
  '30d': 30 * 24 * 60 * 60 * 1000,
  '90d': 90 * 24 * 60 * 60 * 1000,
  '365d': 365 * 24 * 60 * 60 * 1000,
};

function normalizeSpotPrice(value: unknown): number {
  const n = Number(value);
  return Number.isFinite(n) && n >= 100 && n <= 20000 ? n : 0;
}

async function getAccountResponse(request: NextRequest) {
  try {
    const range = request.nextUrl.searchParams.get('range') || '30d';
    const rangeMs = RANGE_MS[range] || RANGE_MS['30d'];
    const since = new Date(Date.now() - rangeMs).toISOString();

    const [positions, collaterals] = await Promise.all([
      getPositions(),
      getCollaterals(),
    ]);

    const trades = getOrderTradesSince(since)
      .map((order) => {
        const tradeTs = new Date(order.timestamp).getTime();
        return {
          trade_id: `order-${order.id}`,
          instrument_name: order.instrument_name,
          direction: order.action === 'buy_put' || order.action === 'buyback_call' ? 'buy' : 'sell',
          trade_amount: Number(order.filled_amount ?? order.intended_amount ?? 0),
          trade_price: Number(order.fill_price ?? order.price ?? 0),
          trade_fee: 0,
          timestamp: tradeTs,
          index_price: normalizeSpotPrice(order.spot_price),
          realized_pnl: 0,
          is_bot: true,
        };
      })
      .filter((trade) =>
        trade.instrument_name
        && Number.isFinite(trade.timestamp)
        && trade.trade_amount > 0
        && trade.trade_price > 0
      );

    return NextResponse.json({
      collaterals: (Array.isArray(collaterals) ? collaterals : []).map((c: Record<string, unknown>) => ({
        asset_name: c.asset_name,
        amount: Number(c.amount ?? 0),
        mark_price: Number(c.mark_price ?? 0),
        mark_value: Number(c.mark_value ?? c.value ?? 0),
        unrealized_pnl: Number(c.unrealized_pnl ?? 0),
      })),
      positions: (Array.isArray(positions) ? positions : []).map((p: Record<string, unknown>) => ({
        instrument_name: p.instrument_name,
        instrument_type: p.instrument_type,
        amount: Number(p.amount ?? 0),
        average_price: Number(p.average_price ?? 0),
        mark_price: Number(p.mark_price ?? 0),
        mark_value: Number(p.mark_value ?? 0),
        unrealized_pnl: Number(p.unrealized_pnl ?? 0),
        delta: Number(p.delta ?? 0),
        gamma: Number(p.gamma ?? 0),
        theta: Number(p.theta ?? 0),
        vega: Number(p.vega ?? 0),
        index_price: Number(p.index_price ?? 0),
        liquidation_price: p.liquidation_price != null ? Number(p.liquidation_price) : null,
      })),
      trades,
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message, collaterals: [], positions: [], trades: [] }, { status: 500 });
  }
}

export function GET(request: NextRequest) {
  const range = request.nextUrl.searchParams.get('range') || '30d';
  return cachedJsonRoute(request, `account:${range}`, () => getAccountResponse(request), {
    freshMs: 30_000,
    staleMs: 2 * 60_000,
    browserMaxAgeSeconds: 15,
  });
}
