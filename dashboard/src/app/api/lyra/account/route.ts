import { NextResponse } from 'next/server';
import { getPositions, getCollaterals, getTradeHistory } from '@/lib/lyra';
import { getLocalTrades } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const thirtyDaysAgo = Date.now() - 30 * 24 * 60 * 60 * 1000;
    const since = new Date(thirtyDaysAgo).toISOString();

    const [positions, collaterals, lyraTradesRaw] = await Promise.all([
      getPositions(),
      getCollaterals(),
      getTradeHistory(thirtyDaysAgo),
    ]);

    // Get local bot trades for cross-referencing
    const localTrades = getLocalTrades(since);

    // Cross-reference Lyra trades with local bot trades (±60s window + instrument match)
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const trades = (Array.isArray(lyraTradesRaw) ? lyraTradesRaw : []).map((t: any) => {
      const tradeTs = typeof t.timestamp === 'number' ? t.timestamp : new Date(t.timestamp).getTime();
      const isBot = localTrades.some(lt => {
        const localTs = new Date(lt.timestamp).getTime();
        return Math.abs(localTs - tradeTs) < 60_000
          && lt.instrument_name === t.instrument_name;
      });

      return {
        trade_id: t.trade_id,
        instrument_name: t.instrument_name,
        direction: t.direction,
        trade_amount: Number(t.trade_amount ?? t.amount ?? 0),
        trade_price: Number(t.trade_price ?? t.price ?? 0),
        trade_fee: Number(t.trade_fee ?? t.fee ?? 0),
        timestamp: tradeTs,
        index_price: Number(t.index_price ?? 0),
        realized_pnl: Number(t.realized_pnl ?? 0),
        is_bot: isBot,
      };
    });

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
