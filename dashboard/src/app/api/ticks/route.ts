import { NextResponse } from 'next/server';
import { getRecentTicks } from '@/lib/db';
import { cachedJsonRoute } from '@/lib/response-cache';

export const dynamic = 'force-dynamic';

function getTicksResponse() {
  try {
    const ticks = getRecentTicks();
    return NextResponse.json(ticks);
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export function GET(request: Request) {
  return cachedJsonRoute(request, 'ticks', getTicksResponse, {
    freshMs: 30_000,
    staleMs: 2 * 60_000,
    browserMaxAgeSeconds: 15,
  });
}
