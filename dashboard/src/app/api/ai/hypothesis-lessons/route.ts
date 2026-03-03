import { NextResponse } from 'next/server';
import { getActiveLessons } from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET() {
  try {
    const lessons = getActiveLessons();
    return NextResponse.json({ lessons });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message, lessons: [] }, { status: 500 });
  }
}
