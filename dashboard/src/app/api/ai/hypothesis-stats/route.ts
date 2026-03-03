import { NextResponse } from 'next/server';
import { getHypothesisStats } from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET() {
  try {
    const since = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
    const stats = getHypothesisStats(since);
    if (!stats) return NextResponse.json({ stats: null });

    const reviewed = stats.total - (stats.pending || 0);
    const convexPostureRate = reviewed > 0
      ? ((stats.confirmed_convex + stats.disproven_bounded) / reviewed)
      : 0;
    const costlyRate = reviewed > 0
      ? (stats.disproven_costly / reviewed)
      : 0;

    return NextResponse.json({
      stats: {
        ...stats,
        reviewed,
        convexPostureRate: +convexPostureRate.toFixed(3),
        costlyRate: +costlyRate.toFixed(3),
      },
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message, stats: null }, { status: 500 });
  }
}
