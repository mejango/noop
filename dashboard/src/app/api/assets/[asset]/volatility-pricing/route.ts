import { NextResponse } from 'next/server';
import { getAssetQuoteFrames } from '@/lib/asset-overview';
import { assetPricingFrame } from '@/lib/asset-smile';
import { isMarketAsset } from '@/lib/market-assets';
import { buildVolatilityPricing, HISTORY_DAYS } from '@/lib/volatility-pricing';
import { cachedJsonRoute } from '@/lib/response-cache';

export const dynamic = 'force-dynamic';

export function GET(request: Request, { params }: { params: { asset: string } }) {
  const asset = params.asset.toUpperCase();
  if (!isMarketAsset(asset)) return NextResponse.json({ error: 'Unknown asset' }, { status: 404 });
  return cachedJsonRoute(request, `asset:${asset}:volatility-pricing`, () => {
    try {
      const frames = getAssetQuoteFrames(asset, new Date(Date.now() - HISTORY_DAYS * 86_400_000).toISOString(), HISTORY_DAYS * 24 + 1)
        .map(assetPricingFrame).filter(f => Number.isFinite(f.at) && f.expiries.length);
      const current = frames.pop();
      if (!current) return NextResponse.json({ error: 'Options quotes unavailable; retry shortly' }, { status: 503 });
      return NextResponse.json({ ...buildVolatilityPricing(current, frames), source: 'snapshot' });
    } catch (error) {
      return NextResponse.json({ error: error instanceof Error ? error.message : 'Options data unavailable' }, { status: 500 });
    }
  }, { freshMs: 60_000, staleMs: 5 * 60_000, browserMaxAgeSeconds: 30 });
}
