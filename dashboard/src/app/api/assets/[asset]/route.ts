import { NextRequest, NextResponse } from 'next/server';
import { getAssetMarketData } from '@/lib/db';
import { isMarketAsset } from '@/lib/market-assets';
import { ASSET_DEFAULT_RANGE, DASHBOARD_RANGES, dashboardRangeMs } from '@/lib/dashboard-ranges';
import { cachedJsonRoute } from '@/lib/response-cache';

export const dynamic = 'force-dynamic';

export function GET(request: NextRequest, { params }: { params: { asset: string } }) {
  const asset = params.asset.toUpperCase();
  if (!isMarketAsset(asset)) return NextResponse.json({ error: 'Unknown asset' }, { status: 404 });
  const requestedRange = request.nextUrl.searchParams.get('range') || ASSET_DEFAULT_RANGE;
  const range = DASHBOARD_RANGES.includes(requestedRange as typeof DASHBOARD_RANGES[number]) ? requestedRange : ASSET_DEFAULT_RANGE;
  return cachedJsonRoute(request, `asset:${asset}:${range}`, () => {
    try {
      const rangeMs = dashboardRangeMs(range, ASSET_DEFAULT_RANGE);
      const since = new Date(Date.now() - rangeMs).toISOString();
      return NextResponse.json({ asset, range, ...getAssetMarketData(asset, since, rangeMs) });
    } catch (error) {
      return NextResponse.json({ error: error instanceof Error ? error.message : 'Market data unavailable' }, { status: 500 });
    }
  }, { freshMs: 60_000, staleMs: 5 * 60_000, browserMaxAgeSeconds: 30 });
}
