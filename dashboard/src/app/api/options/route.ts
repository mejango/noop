import { NextRequest, NextResponse } from 'next/server';
import { getOptionsSnapshots } from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET(request: NextRequest) {
  try {
    const range = request.nextUrl.searchParams.get('range') || '24h';
    const limit = Number(request.nextUrl.searchParams.get('limit')) || 500;
    const rangeMs: Record<string, number> = {
      '1h': 60 * 60 * 1000,
      '6h': 6 * 60 * 60 * 1000,
      '24h': 24 * 60 * 60 * 1000,
      '7d': 7 * 24 * 60 * 60 * 1000,
    };
    const ms = rangeMs[range] || rangeMs['24h'];
    const since = new Date(Date.now() - ms).toISOString();
    const snapshots = getOptionsSnapshots(since, limit);
    return NextResponse.json(snapshots);
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
