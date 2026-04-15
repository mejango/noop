import { NextResponse } from 'next/server';
import { getActiveTradeLessons, getRecentTradeReviews } from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET() {
  try {
    const lessons = getActiveTradeLessons();
    const reviews = getRecentTradeReviews(20).map((review) => ({
      ...review,
      lessons: review.lessons ? JSON.parse(review.lessons) : [],
      order_ids: review.order_ids ? JSON.parse(review.order_ids) : [],
    }));

    return NextResponse.json({ lessons, reviews });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message, lessons: [], reviews: [] }, { status: 500 });
  }
}
