import { NextResponse } from 'next/server';
import { getSmileSnapshots, getSmileSnapshotTimestamps, getSmileSnapshotNear } from '@/lib/db';
import { getChain, getCachedChain } from '@/lib/smile-chain';
import { fromCompact } from '@/lib/vol-smile';
import { buildVolatilityPricing, sampleHourly, HISTORY_DAYS, type PricingFrame, type VolatilityPricingData } from '@/lib/volatility-pricing';

export const dynamic = 'force-dynamic';
let cached: VolatilityPricingData | null = null;
let historyCache: { at: number; hour: number; frames: PricingFrame[] } | null = null;

function historyAt(at: number) {
  const hour = Math.floor(at / 3_600_000);
  if (historyCache && historyCache.hour === hour && Date.now() - historyCache.at < 300_000) return historyCache.frames;
  const timestamps = sampleHourly(
    getSmileSnapshotTimestamps(new Date(at - HISTORY_DAYS * 86_400_000).toISOString()),
    t => Date.parse(t), at,
  );
  const frames = new Map<string, PricingFrame>();
  for (const row of getSmileSnapshots(timestamps)) {
    const t = Date.parse(row.timestamp);
    const frame = frames.get(row.timestamp) ?? { at: t, expiries: [] };
    try { frame.expiries.push(fromCompact(row, t)); } catch { continue; }
    frames.set(row.timestamp, frame);
  }
  historyCache = { at: Date.now(), hour, frames: Array.from(frames.values()) };
  return historyCache.frames;
}

export async function GET() {
  try {
    // Do not put external API requests on the snapshot response's critical path.
    const refresh = getChain().catch(() => null);
    let chain = getCachedChain();
    let current: PricingFrame | null = chain?.expiries.length && Date.now() - chain.at < 300_000
      ? { at: chain.at, expiries: chain.expiries } : null;
    let source: 'live' | 'snapshot' = 'live';
    if (!current) {
      const rows = getSmileSnapshotNear(new Date());
      const at = Date.parse(rows[0]?.timestamp ?? '');
      if (Number.isFinite(at) && Date.now() - at < 20 * 60_000) {
        const expiries = rows.flatMap(row => {
          try { return [fromCompact(row, at)]; } catch { return []; }
        });
        if (expiries.length) { current = { at, expiries }; source = 'snapshot'; }
      }
    }
    if (!current) {
      // A fresh installation may not have any saved snapshots yet.
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        chain = await Promise.race([refresh, new Promise<null>(resolve => { timer = setTimeout(() => resolve(null), 3000); })]);
      } finally { if (timer) clearTimeout(timer); }
      if (chain?.expiries.length && Date.now() - chain.at < 300_000) current = { at: chain.at, expiries: chain.expiries };
    }
    if (!current) return NextResponse.json({ error: 'Options quotes unavailable; retry shortly' }, { status: 503 });
    const asOf = new Date(current.at).toISOString();
    if (cached?.asOf === asOf && cached.source === source) return NextResponse.json(cached);
    const data = { ...buildVolatilityPricing(current, historyAt(current.at)), source };
    cached = data;
    return NextResponse.json(data);
  } catch (e: unknown) {
    return NextResponse.json({ error: e instanceof Error ? e.message : 'Options data unavailable' }, { status: 502 });
  }
}
