import { NextResponse } from 'next/server';
import { getSmileSnapshots, getSmileSnapshotTimestamps } from '@/lib/db';

export const dynamic = 'force-dynamic';

const RANGE_MS: Record<string, number> = { '24h': 86_400_000, '3d': 3 * 86_400_000, '7d': 7 * 86_400_000, '30d': 30 * 86_400_000 };
const MAX_FRAMES = 96;

// Frames stay compact (tuple JSON); the client decodes with fromCompact.
export function GET(request: Request) {
  const range = new URL(request.url).searchParams.get('range') ?? '24h';
  const ms = RANGE_MS[range] ?? RANGE_MS['24h'];
  const all = getSmileSnapshotTimestamps(new Date(Date.now() - ms).toISOString());
  // Evenly thin to MAX_FRAMES, always keeping the latest snapshot.
  const step = Math.max(1, Math.ceil(all.length / MAX_FRAMES));
  const picked = all.filter((_, i) => (all.length - 1 - i) % step === 0);
  const frames = new Map<string, { t: string; spot: number | null; expiries: unknown[] }>();
  for (const r of getSmileSnapshots(picked)) {
    const f = frames.get(r.timestamp) ?? { t: r.timestamp, spot: r.spot, expiries: [] };
    f.expiries.push({ expiry: r.expiry, forward: r.forward, spot: r.spot, points: JSON.parse(r.points) });
    frames.set(r.timestamp, f);
  }
  return NextResponse.json({ range, frames: Array.from(frames.values()) });
}
