import { NextResponse } from 'next/server';
import {
  getActiveTradeLessons,
  getRecentTradeOrderStats,
  getRecentTradeReviews,
  getTradeReviewSummary,
  hasTable,
} from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET() {
  try {
    const hasTradeReviewsTable = hasTable('trade_reviews');
    const hasTradeLessonsTable = hasTable('trade_lessons');
    const recentOrderStats = getRecentTradeOrderStats() || {
      total_orders: 0,
      instrument_count: 0,
      first_timestamp: null,
      last_timestamp: null,
    };
    const reviewSummary = hasTradeReviewsTable
      ? (getTradeReviewSummary() || { review_count: 0, instrument_count: 0, last_created_at: null })
      : { review_count: 0, instrument_count: 0, last_created_at: null };
    const lessons = hasTradeLessonsTable ? getActiveTradeLessons() : [];
    const reviews = hasTradeReviewsTable
      ? getRecentTradeReviews(20).map((review) => ({
          ...review,
          lessons: review.lessons ? JSON.parse(review.lessons) : [],
          order_ids: review.order_ids ? JSON.parse(review.order_ids) : [],
        }))
      : [];

    return NextResponse.json({
      lessons,
      reviews,
      status: {
        hasTradeReviewsTable,
        hasTradeLessonsTable,
        recentOrderStats,
        reviewSummary,
      },
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({
      error: message,
      lessons: [],
      reviews: [],
      status: null,
    }, { status: 500 });
  }
}
