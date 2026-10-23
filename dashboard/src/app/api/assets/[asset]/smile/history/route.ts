import { NextResponse } from 'next/server';
import { getAssetQuoteFrames } from '@/lib/asset-overview';
import { assetSmileExpiries, compactAssetExpiry } from '@/lib/asset-smile';
import { isMarketAsset } from '@/lib/market-assets';

export const dynamic = 'force-dynamic';

const RANGE_MS: Record<string, number> = { '24h': 86_400_000, '3d': 3 * 86_400_000, '7d': 7 * 86_400_000, '30d': 30 * 86_400_000 };

export function GET(request: Request, { params }: { params: { asset: string } }) {
  const asset = params.asset.toUpperCase();
  if (!isMarketAsset(asset)) return NextResponse.json({ error: 'Unknown asset' }, { status: 404 });
  try {
    const requested = new URL(request.url).searchParams.get('range') ?? '24h';
    const range = Object.hasOwn(RANGE_MS, requested) ? requested : '24h';
    const history = getAssetQuoteFrames(asset, new Date(Date.now() - RANGE_MS[range]).toISOString(), 96);
    return NextResponse.json({ range, frames: history.map(f => ({
      t: f.hour, spot: f.spot, expiries: assetSmileExpiries(f).map(compactAssetExpiry),
    })).filter(f => f.expiries.length) });
  } catch (error) {
    return NextResponse.json({ error: error instanceof Error ? error.message : 'Options history unavailable' }, { status: 500 });
  }
}
