import { NextResponse } from 'next/server';
import { getSmileSnapshots, getSmileSnapshotTimestamps } from '@/lib/db';
import { getChain } from '@/lib/smile-chain';
import { fromCompact } from '@/lib/vol-smile';
import { buildVolatilityPricing, type PricingFrame, type VolatilityPricingData } from '@/lib/volatility-pricing';

export const dynamic = 'force-dynamic';
let cached: { at: number; data: VolatilityPricingData } | null = null;

export async function GET() {
  try {
    if (cached && Date.now() - cached.at < 60_000) return NextResponse.json(cached.data);
    const chain = await getChain();
    if (!chain.expiries.length || Date.now() - chain.at > 5 * 60_000) {
      return NextResponse.json({ error: 'Fresh options quotes unavailable' }, { status: 503 });
    }
    const days = new Map<string, string>();
    for (const t of getSmileSnapshotTimestamps(new Date(chain.at - 365 * 86_400_000).toISOString())) {
      if (Date.parse(t) < chain.at && t.slice(0, 10) !== new Date(chain.at).toISOString().slice(0, 10)) days.set(t.slice(0, 10), t);
    }
    const frames = new Map<string, PricingFrame>();
    for (const row of getSmileSnapshots(Array.from(days.values()))) {
      const at = Date.parse(row.timestamp);
      const frame = frames.get(row.timestamp) ?? { at, expiries: [] };
      try { frame.expiries.push(fromCompact(row, at)); } catch { continue; }
      frames.set(row.timestamp, frame);
    }
    const data = buildVolatilityPricing({ at: chain.at, expiries: chain.expiries }, Array.from(frames.values()));
    cached = { at: Date.now(), data };
    return NextResponse.json(data);
  } catch (e: unknown) {
    return NextResponse.json({ error: e instanceof Error ? e.message : 'Options data unavailable' }, { status: 502 });
  }
}
