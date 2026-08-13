import { NextResponse } from 'next/server';
import { buildMarketSnapshot } from '@/lib/snapshot';
import { cachedJsonRoute } from '@/lib/response-cache';

export const dynamic = 'force-dynamic';

async function getSnapshotResponse() {
  try {
    const snapshot = await buildMarketSnapshot();
    return NextResponse.json(snapshot);
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export function GET(request: Request) {
  return cachedJsonRoute(request, 'ai-snapshot', getSnapshotResponse, {
    freshMs: 60_000,
    staleMs: 5 * 60_000,
    browserMaxAgeSeconds: 30,
  });
}
