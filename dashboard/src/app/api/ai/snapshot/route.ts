import { NextResponse } from 'next/server';
import { buildMarketSnapshot } from '@/lib/snapshot';

export const dynamic = 'force-dynamic';

export function GET() {
  try {
    const snapshot = buildMarketSnapshot();
    return NextResponse.json(snapshot);
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
