import { NextResponse } from 'next/server';
import { getStats, getBotBudget } from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET() {
  try {
    const stats = getStats() as Record<string, unknown> || {};
    const budget = getBotBudget();
    return NextResponse.json({ ...stats, budget });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
