import { NextResponse } from 'next/server';
import { getJournalEntries } from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET() {
  try {
    const since = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
    const entries = getJournalEntries(since, 50);
    return NextResponse.json({ entries });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message, entries: [] }, { status: 500 });
  }
}
