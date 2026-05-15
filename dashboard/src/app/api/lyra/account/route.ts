import { NextResponse } from 'next/server';
import { getPositions, getCollaterals } from '@/lib/lyra';
import { getOrderTradesSince } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const thirtyDaysAgo = Date.now() - 30 * 24 * 60 * 60 * 1000;
    const since = new Date(thirtyDaysAgo).toISOString();

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
          index_price: Number(order.spot_price ?? 0),
          realized_pnl: 0,
          is_bot: true,
        };
      })
      .filter((trade) =>
        trade.instrument_name
        && Number.isFinite(trade.timestamp)
        && trade.trade_amount > 0
        && trade.trade_price > 0
        && trade.index_price > 0
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
