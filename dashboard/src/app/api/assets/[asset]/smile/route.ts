import { NextResponse } from 'next/server';
import { getAssetQuoteFrames } from '@/lib/asset-overview';
import { assetSmileExpiries } from '@/lib/asset-smile';
import { isMarketAsset } from '@/lib/market-assets';
import { chartExpiry } from '@/lib/vol-smile';

export const dynamic = 'force-dynamic';

export function GET(_request: Request, { params }: { params: { asset: string } }) {
  const asset = params.asset.toUpperCase();
  if (!isMarketAsset(asset)) return NextResponse.json({ error: 'Unknown asset' }, { status: 404 });
  try {
    const now = Date.now();
    const frames = getAssetQuoteFrames(asset, new Date(now - 25 * 3_600_000).toISOString(), 100)
      .map(frame => ({ hour: frame.hour, expiries: assetSmileExpiries(frame) }))
      .filter(frame => frame.expiries.length);
    const latest = frames[frames.length - 1];
    if (!latest) return NextResponse.json({ asOf: null, expiries: [], history: [], historyScope: 'full' });
    const target = now - 24 * 3_600_000;
    const previous = frames.filter(f => Date.parse(f.hour) <= target).at(-1);
    const history = previous ? previous.expiries.flatMap(e => chartExpiry(e).points.map(p => ({
      name: p.name, expiry: e.expiry, strike: p.strike, type: p.type, delta: p.delta,
      iv: p.iv, timestamp: previous.hour,
    }))) : [];
    return NextResponse.json({ asOf: latest.hour, expiries: latest.expiries.map(chartExpiry), history, historyScope: 'full' });
  } catch (error) {
    return NextResponse.json({ error: error instanceof Error ? error.message : 'Options data unavailable' }, { status: 500 });
  }
}
