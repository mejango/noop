import { NextRequest, NextResponse } from 'next/server';
import { getAssetMarketData } from '@/lib/db';
import { isMarketAsset } from '@/lib/market-assets';
import { DASHBOARD_RANGES, dashboardRangeMs } from '@/lib/dashboard-ranges';
import { cachedJsonRoute } from '@/lib/response-cache';

export const dynamic = 'force-dynamic';

export function GET(request: NextRequest, { params }: { params: { asset: string } }) {
  const asset = params.asset.toUpperCase();
  if (!isMarketAsset(asset)) return NextResponse.json({ error: 'Unknown asset' }, { status: 404 });
  const requestedRange = request.nextUrl.searchParams.get('range') || '14d';
  const range = DASHBOARD_RANGES.includes(requestedRange as typeof DASHBOARD_RANGES[number]) ? requestedRange : '14d';
  return cachedJsonRoute(request, `asset:${asset}:${range}`, () => {
    try {
      const since = new Date(Date.now() - dashboardRangeMs(range, '14d')).toISOString();
      return NextResponse.json({ asset, range, ...getAssetMarketData(asset, since) });
    } catch (error) {
      return NextResponse.json({ error: error instanceof Error ? error.message : 'Market data unavailable' }, { status: 500 });
    }
  }, { freshMs: 60_000, staleMs: 5 * 60_000, browserMaxAgeSeconds: 30 });
}
