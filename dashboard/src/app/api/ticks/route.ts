import { NextResponse } from 'next/server';
import { getRecentTicks } from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET() {
  try {
    const ticks = getRecentTicks();
    return NextResponse.json(ticks);
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
