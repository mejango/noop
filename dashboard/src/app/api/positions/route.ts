import { NextRequest, NextResponse } from 'next/server';
import { getPositions, getTradesForPosition } from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET(request: NextRequest) {
  try {
    const status = request.nextUrl.searchParams.get('status') || undefined;
    const positionId = request.nextUrl.searchParams.get('id');

    if (positionId) {
      const trades = getTradesForPosition(Number(positionId));
      return NextResponse.json({ trades });
    }

    const positions = getPositions(status);
    return NextResponse.json(positions);
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
