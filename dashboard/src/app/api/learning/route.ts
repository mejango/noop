import { NextRequest, NextResponse } from 'next/server';
import {
  getActiveTradeLessons,
  getOrdersInRange,
  getRecentTradeOrderStats,
  getRecentTradeReviews,
  getTradeReviewSummary,
  hasTable,
} from '@/lib/db';
import { getTradeHistory } from '@/lib/lyra';

export const dynamic = 'force-dynamic';

type TradeOrder = {
  id: number;
  timestamp: string;
  action: string;
  success: number;
  instrument_name: string | null;
  intended_amount: number | null;
  filled_amount: number | null;
  total_value: number | null;
  spot_price: number | null;
};

type PendingCampaign = {
  id: string;
  instrument_name: string;
  action_family: string;
  opened_at: string | null;
  closed_at: string;
  order_ids: number[];
  pnl_realized: number;
  premium_opened: number;
  premium_closed: number;
  spot_open: number | null;
  spot_close: number | null;
  review_state: 'awaiting_horizon' | 'ready_for_review' | 'reviewed';
  next_review_at: string | null;
  review_window_days: number;
};

const TRADE_REVIEW_WINDOWS_DAYS = [1, 3, 7];

function getTradeCashflow(order: Pick<TradeOrder, 'action' | 'total_value'>) {
  const totalValue = Number(order.total_value ?? 0);
  if (order.action === 'sell_call' || order.action === 'sell_put') return totalValue;
  if (order.action === 'buy_put' || order.action === 'buyback_call') return -totalValue;
  return 0;
}

function getTradeActionFamily(action: string) {
  if (action === 'sell_call' || action === 'buyback_call') return 'short_call_campaign';
  if (action === 'buy_put' || action === 'sell_put') return 'long_put_campaign';
  return null;
}

function getActionFromTradeDirection(instrumentName: string | null, direction: string | null | undefined) {
  if (!instrumentName || !direction) return null;
  const normalized = direction.toLowerCase();
  if (instrumentName.endsWith('-C')) {
    if (normalized === 'sell') return 'sell_call';
    if (normalized === 'buy') return 'buyback_call';
  }
  if (instrumentName.endsWith('-P')) {
    if (normalized === 'buy') return 'buy_put';
    if (normalized === 'sell') return 'sell_put';
  }
  return null;
}

function normalizeLyraTradeForReview(trade: Record<string, unknown>): TradeOrder | null {
  const instrumentName = typeof trade.instrument_name === 'string' ? trade.instrument_name : null;
  const action = getActionFromTradeDirection(instrumentName, typeof trade.direction === 'string' ? trade.direction : null);
  const amount = Math.abs(Number(trade.trade_amount ?? trade.amount ?? 0));
  const price = Number(trade.trade_price ?? trade.price ?? 0);
  const rawTimestamp = trade.timestamp;
  const timestamp = typeof rawTimestamp === 'number'
    ? new Date(rawTimestamp).toISOString()
    : new Date(String(rawTimestamp ?? '')).toISOString();
  if (!action || !instrumentName || !(amount > 0) || Number.isNaN(new Date(timestamp).getTime())) return null;
  return {
    id: -1,
    timestamp,
    action,
    success: 1,
    instrument_name: instrumentName,
    intended_amount: amount,
    filled_amount: amount,
    total_value: amount * price,
    spot_price: Number(trade.index_price ?? 0) || null,
  };
}

function mergeOrdersForTradeReview(localOrders: TradeOrder[], lyraTradesRaw: Record<string, unknown>[]) {
  const merged = [...localOrders];
  for (const trade of lyraTradesRaw) {
    const normalized = normalizeLyraTradeForReview(trade);
    if (!normalized) continue;
    const normalizedTs = new Date(normalized.timestamp).getTime();
    const duplicateLocal = merged.some((order) => {
      if (Number(order.success || 0) !== 1) return false;
      if (order.instrument_name !== normalized.instrument_name) return false;
      if (order.action !== normalized.action) return false;
      const orderTs = new Date(order.timestamp).getTime();
      const orderAmount = Math.abs(Number(order.filled_amount ?? order.intended_amount ?? 0));
      const normalizedAmount = Math.abs(Number(normalized.filled_amount ?? normalized.intended_amount ?? 0));
      return Math.abs(orderTs - normalizedTs) < 60_000 && Math.abs(orderAmount - normalizedAmount) < 0.02;
    });
    if (!duplicateLocal) merged.push(normalized);
  }
  return merged;
}

function serializeOrderForDebug(order: TradeOrder) {
  return {
    timestamp: order.timestamp,
    action: order.action,
    success: order.success,
    instrument_name: order.instrument_name,
    intended_amount: order.intended_amount,
    filled_amount: order.filled_amount,
    total_value: order.total_value,
    spot_price: order.spot_price,
  };
}

function deriveClosedTradeCampaigns(orders: TradeOrder[]): PendingCampaign[] {
  const byInstrument = new Map<string, Array<TradeOrder & { family: string }>>();
  for (const order of orders) {
    if (!order.instrument_name || Number(order.success || 0) !== 1) continue;
    const family = getTradeActionFamily(order.action);
    if (!family) continue;
    const list = byInstrument.get(order.instrument_name) || [];
    list.push({ ...order, family });
    byInstrument.set(order.instrument_name, list);
  }

  const campaigns: PendingCampaign[] = [];
  const EPS = 1e-9;

  for (const [instrumentName, instrumentOrders] of Array.from(byInstrument.entries())) {
    instrumentOrders.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());
    let netExposure = 0;
    let active: Omit<PendingCampaign, 'id' | 'review_state' | 'next_review_at' | 'review_window_days'> | null = null;

    for (const order of instrumentOrders) {
      const qty = Math.abs(Number(order.filled_amount ?? order.intended_amount ?? 0));
      if (!(qty > 0)) continue;

      const isOpen = order.action === 'sell_call' || order.action === 'buy_put';
      const exposureDelta = isOpen ? qty : -qty;

      if (!active && isOpen) {
        active = {
          instrument_name: instrumentName,
          action_family: order.family,
          opened_at: order.timestamp,
          closed_at: order.timestamp,
          order_ids: [],
          pnl_realized: 0,
          premium_opened: 0,
          premium_closed: 0,
          spot_open: Number(order.spot_price || 0) || null,
          spot_close: null,
        };
      }

      if (!active) continue;

      active.order_ids.push(order.id);
      active.pnl_realized += getTradeCashflow(order);
      if (isOpen) {
        active.premium_opened += Number(order.total_value ?? 0);
      } else {
        active.premium_closed += Number(order.total_value ?? 0);
      }

      netExposure += exposureDelta;

      if (netExposure <= EPS) {
        active.closed_at = order.timestamp;
        active.spot_close = Number(order.spot_price || 0) || null;
        campaigns.push({
          ...active,
          id: `${instrumentName}:${order.timestamp}`,
          review_state: 'awaiting_horizon',
          next_review_at: null,
          review_window_days: 1,
        });
        active = null;
        netExposure = 0;
      }
    }
  }

  return campaigns.sort((a, b) => new Date(b.closed_at).getTime() - new Date(a.closed_at).getTime());
}

export async function GET(req: NextRequest) {
  try {
    const hasTradeReviewsTable = hasTable('trade_reviews');
    const hasTradeLessonsTable = hasTable('trade_lessons');
    const recentOrderStats = getRecentTradeOrderStats() || {
      total_orders: 0,
      instrument_count: 0,
      first_timestamp: null,
      last_timestamp: null,
    };
    const now = Date.now();
    const recentOrders = getOrdersInRange(
      new Date(now - 21 * 24 * 60 * 60 * 1000).toISOString(),
      new Date(now).toISOString()
    ) as TradeOrder[];
    const lyraTradesRaw = await getTradeHistory(now - 21 * 24 * 60 * 60 * 1000);
    const debugInstrument = new URL(req.url).searchParams.get('debug_instrument');
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
    const mergedOrders = mergeOrdersForTradeReview(
      recentOrders,
      Array.isArray(lyraTradesRaw) ? lyraTradesRaw as Record<string, unknown>[] : []
    );
    const reviewKeys = new Set(
      reviews.map((review) => `${review.instrument_name}:${review.closed_at}:${review.review_window_days}`)
    );
    const derivedCampaigns = deriveClosedTradeCampaigns(mergedOrders);
    const pendingCampaigns = derivedCampaigns
      .map((campaign) => {
        let reviewState: PendingCampaign['review_state'] = 'awaiting_horizon';
        let nextReviewAt: string | null = null;
        let reviewWindowDays = 1;

        for (const windowDays of TRADE_REVIEW_WINDOWS_DAYS) {
          const horizonEndAt = new Date(new Date(campaign.closed_at).getTime() + windowDays * 24 * 60 * 60 * 1000).toISOString();
          const reviewKey = `${campaign.instrument_name}:${campaign.closed_at}:${windowDays}`;
          if (reviewKeys.has(reviewKey)) {
            reviewState = 'reviewed';
            nextReviewAt = horizonEndAt;
            reviewWindowDays = windowDays;
            continue;
          }
          reviewState = now >= new Date(horizonEndAt).getTime() ? 'ready_for_review' : 'awaiting_horizon';
          nextReviewAt = horizonEndAt;
          reviewWindowDays = windowDays;
          break;
        }

        return {
          ...campaign,
          review_state: reviewState,
          next_review_at: nextReviewAt,
          review_window_days: reviewWindowDays,
        };
      })
      .filter((campaign) => campaign.review_state !== 'reviewed')
      .slice(0, 20);

    return NextResponse.json({
      lessons,
      reviews,
      pendingCampaigns,
      ...(debugInstrument ? {
        debug: {
          instrument: debugInstrument,
          localOrders: recentOrders
            .filter((order) => order.instrument_name === debugInstrument)
            .map(serializeOrderForDebug),
          lyraTrades: (Array.isArray(lyraTradesRaw) ? lyraTradesRaw as Record<string, unknown>[] : [])
            .filter((trade) => trade.instrument_name === debugInstrument)
            .map((trade) => ({
              instrument_name: trade.instrument_name,
              direction: trade.direction,
              timestamp: trade.timestamp,
              trade_amount: Number(trade.trade_amount ?? trade.amount ?? 0),
              trade_price: Number(trade.trade_price ?? trade.price ?? 0),
              index_price: Number(trade.index_price ?? 0),
              normalized: normalizeLyraTradeForReview(trade),
            })),
          mergedOrders: mergedOrders
            .filter((order) => order.instrument_name === debugInstrument)
            .map(serializeOrderForDebug),
          derivedCampaigns: derivedCampaigns
            .filter((campaign) => campaign.instrument_name === debugInstrument),
        },
      } : {}),
      status: {
        hasTradeReviewsTable,
        hasTradeLessonsTable,
        recentOrderStats,
        reviewSummary,
        pendingCampaignSummary: {
          closed_count: pendingCampaigns.length,
          ready_count: pendingCampaigns.filter((campaign) => campaign.review_state === 'ready_for_review').length,
        },
      },
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({
      error: message,
      lessons: [],
      reviews: [],
      pendingCampaigns: [],
      status: null,
    }, { status: 500 });
  }
}
