import { NextRequest, NextResponse } from 'next/server';
import { getSpotPrices, getBestOptionsOverTime, getLiquidityOverTime, getBestScores, getOptionsHeatmap } from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET(request: NextRequest) {
  try {
    const range = request.nextUrl.searchParams.get('range') || '7d';
    const rangeMs: Record<string, number> = {
      '1h': 60 * 60 * 1000,
      '6h': 6 * 60 * 60 * 1000,
      '24h': 24 * 60 * 60 * 1000,
      '3d': 3 * 24 * 60 * 60 * 1000,
      '6.2d': 6.2 * 24 * 60 * 60 * 1000,
      '7d': 7 * 24 * 60 * 60 * 1000,
      '30d': 30 * 24 * 60 * 60 * 1000,
      'all': 365 * 24 * 60 * 60 * 1000,
    };
    const ms = rangeMs[range] || rangeMs['7d'];
    const since = new Date(Date.now() - ms).toISOString();

    const prices = getSpotPrices(since, 5000);
    const options = getBestOptionsOverTime(since);
    const liquidity = getLiquidityOverTime(since);
    const bestScores = getBestScores();
    const optionsHeatmap = getOptionsHeatmap(since);

    return NextResponse.json({ prices, options, liquidity, bestScores, optionsHeatmap });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
