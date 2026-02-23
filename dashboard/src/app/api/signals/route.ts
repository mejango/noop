import { NextRequest, NextResponse } from 'next/server';
import { getSignals } from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET(request: NextRequest) {
  try {
    const range = request.nextUrl.searchParams.get('range') || '30d';
    const limit = Number(request.nextUrl.searchParams.get('limit')) || 100;
    const rangeMs: Record<string, number> = {
      '7d': 7 * 24 * 60 * 60 * 1000,
      '30d': 30 * 24 * 60 * 60 * 1000,
      'all': 365 * 24 * 60 * 60 * 1000,
    };
    const ms = rangeMs[range] || rangeMs['30d'];
    const since = new Date(Date.now() - ms).toISOString();
    const signals = getSignals(since, limit);
    return NextResponse.json(signals);
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
