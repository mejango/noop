import { NextResponse } from 'next/server';
import { getSmileSnapshots, getSmileSnapshotTimestamps } from '@/lib/db';
import { getChain } from '@/lib/smile-chain';
import { fromCompact } from '@/lib/vol-smile';
import { buildVolatilityPricing, sampleHourly, HISTORY_DAYS, type PricingFrame, type VolatilityPricingData } from '@/lib/volatility-pricing';

export const dynamic = 'force-dynamic';
let cached: { at: number; data: VolatilityPricingData } | null = null;

export async function GET() {
  try {
    if (cached && Date.now() - cached.at < 60_000) return NextResponse.json(cached.data);
    const chain = await getChain();
    if (!chain.expiries.length || Date.now() - chain.at > 5 * 60_000) {
      return NextResponse.json({ error: 'Fresh options quotes unavailable' }, { status: 503 });
    }
    const timestamps = sampleHourly(
      getSmileSnapshotTimestamps(new Date(chain.at - HISTORY_DAYS * 86_400_000).toISOString()),
      t => Date.parse(t), chain.at,
    );
    const frames = new Map<string, PricingFrame>();
    for (const row of getSmileSnapshots(timestamps)) {
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
