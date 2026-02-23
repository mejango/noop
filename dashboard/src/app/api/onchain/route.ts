import { NextRequest, NextResponse } from 'next/server';
import { getOnchainData } from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET(request: NextRequest) {
  try {
    const range = request.nextUrl.searchParams.get('range') || '7d';
    const rangeMs: Record<string, number> = {
      '24h': 24 * 60 * 60 * 1000,
      '7d': 7 * 24 * 60 * 60 * 1000,
      '30d': 30 * 24 * 60 * 60 * 1000,
    };
    const ms = rangeMs[range] || rangeMs['7d'];
    const since = new Date(Date.now() - ms).toISOString();
    const data = getOnchainData(since);
    return NextResponse.json(data);
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
