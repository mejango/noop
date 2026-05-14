import { NextResponse } from 'next/server';
import {
  getActiveTradeLessons,
  getBudgetCycleState,
  getOrdersInRange,
  getRecentTradeOrderStats,
  getRecentTradeReviews,
  getSpotPrices,
  getTradeReviewSummary,
  hasTable,
} from '@/lib/db';
import { getTradeHistory } from '@/lib/lyra';

export const dynamic = 'force-dynamic';

const LEARNING_RECENT_LOOKBACK_DAYS = 5;
const TRADE_REVIEW_LOOKBACK_DAYS = 120;

type TradeOrder = {
  id: number | string;
  timestamp: string;
  action: string;
  success: number;
  instrument_name: string | null;
  intended_amount: number | null;
  filled_amount: number | null;
  total_value: number | null;
  spot_price: number | null;
  fill_price?: number | null;
};

type PendingCampaign = {
  id: string;
  instrument_name: string;
  action_family: string;
  opened_at: string | null;
  closed_at: string;
  order_ids: Array<number | string>;
  pnl_realized: number;
  premium_opened: number;
  premium_closed: number;
  spot_open: number | null;
  spot_close: number | null;
  review_state: 'awaiting_horizon' | 'ready_for_review' | 'reviewed';
  next_review_at: string | null;
  review_window_days: number;
  completed_review_windows: number[];
  close_reason?: 'expiry' | 'offsetting_order';
  expiry_amount?: number | null;
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

function parseExpiryFromInstrument(instrumentName: string | null) {
  const parts = String(instrumentName || '').split('-');
  if (parts.length < 4) return null;
  const expiryKey = parts[1];
  if (!/^\d{8}$/.test(expiryKey)) return null;
  const expiry = new Date(`${expiryKey.slice(0, 4)}-${expiryKey.slice(4, 6)}-${expiryKey.slice(6, 8)}T08:00:00Z`);
  const timestamp = expiry.getTime();
  return Number.isFinite(timestamp) ? timestamp : null;
}

function parseTradeInstrumentParts(instrumentName: string | null) {
  const parts = String(instrumentName || '').split('-');
  if (parts.length !== 4) return null;
  const strike = Number(parts[2]);
  if (!Number.isFinite(strike)) return null;
  return { strike, optionType: parts[3] };
}

function getExpiryCloseAction(instrumentName: string | null) {
  if (instrumentName?.endsWith('-C')) return 'expire_call';
  if (instrumentName?.endsWith('-P')) return 'expire_put';
  return 'expire_option';
}

function buildSyntheticExpiryCloseOrder(
  active: Omit<PendingCampaign, 'id' | 'review_state' | 'next_review_at' | 'review_window_days' | 'completed_review_windows'>,
  instrumentName: string,
  expiryMs: number,
  netExposure: number
): TradeOrder & { family: string } {
  return {
    id: `expiry:${instrumentName}:${new Date(expiryMs).toISOString()}`,
    timestamp: new Date(expiryMs).toISOString(),
    action: getExpiryCloseAction(instrumentName),
    success: 1,
    instrument_name: instrumentName,
    intended_amount: Math.max(0, netExposure),
    filled_amount: Math.max(0, netExposure),
    total_value: 0,
    spot_price: null,
    fill_price: 0,
    family: active.action_family,
  };
}

function closeCampaignAtExpiry(
  campaigns: PendingCampaign[],
  active: Omit<PendingCampaign, 'id' | 'review_state' | 'next_review_at' | 'review_window_days' | 'completed_review_windows'>,
  instrumentName: string,
  expiryMs: number,
  netExposure: number
) {
  const expiryOrder = buildSyntheticExpiryCloseOrder(active, instrumentName, expiryMs, netExposure);
  active.order_ids.push(expiryOrder.id);
  active.pnl_realized += getTradeCashflow(expiryOrder);
  active.premium_closed += Number(expiryOrder.total_value ?? 0);
  active.closed_at = expiryOrder.timestamp;
  active.spot_close = null;
  campaigns.push({
    ...active,
    id: `${instrumentName}:${expiryOrder.timestamp}`,
    review_state: 'awaiting_horizon',
    next_review_at: null,
    review_window_days: 1,
    completed_review_windows: [],
    close_reason: 'expiry',
    expiry_amount: Math.max(0, netExposure),
  });
}

function getSpotAtOrBefore(rows: Array<{ timestamp: string; price: number }>, timestamp: string) {
  const targetMs = new Date(timestamp).getTime();
  if (!Number.isFinite(targetMs) || !Array.isArray(rows) || rows.length === 0) return null;
  let best: number | null = null;
  let bestMs = -Infinity;
  for (const row of rows) {
    const rowMs = new Date(row.timestamp).getTime();
    const price = Number(row.price);
    if (!Number.isFinite(rowMs) || !Number.isFinite(price)) continue;
    if (rowMs <= targetMs && rowMs > bestMs) {
      best = price;
      bestMs = rowMs;
    }
  }
  return best;
}

function applyExpirySettlementToCampaign(campaign: PendingCampaign, spotRows: Array<{ timestamp: string; price: number }>) {
  if (campaign.close_reason !== 'expiry') return campaign;
  const parsed = parseTradeInstrumentParts(campaign.instrument_name);
  const spotClose = campaign.spot_close ?? getSpotAtOrBefore(spotRows, campaign.closed_at);
  const amount = Math.abs(Number(campaign.expiry_amount || 0));
  if (!parsed || spotClose == null || !Number.isFinite(spotClose) || !(amount > 0)) return campaign;
  const intrinsic = parsed.optionType === 'C'
    ? Math.max(0, spotClose - parsed.strike)
    : parsed.optionType === 'P'
      ? Math.max(0, parsed.strike - spotClose)
      : 0;
  const settlementValue = intrinsic * amount;
  const settlementCashflow = campaign.action_family === 'short_call_campaign'
    ? -settlementValue
    : settlementValue;
  return {
    ...campaign,
    pnl_realized: campaign.pnl_realized + settlementCashflow,
    premium_closed: campaign.premium_closed + settlementValue,
    spot_close: spotClose,
  };
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
  const FILL_TIME_WINDOW_MS = 10 * 60_000;
  const AMOUNT_EPSILON = 0.02;
  const VALUE_EPSILON = 0.25;
  const isSameRecoveredFill = (order: TradeOrder, normalized: TradeOrder) => {
    if (Number(order.success || 0) !== 1) return false;
    if (order.instrument_name !== normalized.instrument_name) return false;
    if (order.action !== normalized.action) return false;

    const orderAmount = Math.abs(Number(order.filled_amount ?? 0));
    const normalizedAmount = Math.abs(Number(normalized.filled_amount ?? normalized.intended_amount ?? 0));
    if (!(orderAmount > 0) || !(normalizedAmount > 0)) return false;
    if (Math.abs(orderAmount - normalizedAmount) >= AMOUNT_EPSILON) return false;

    const orderTs = new Date(order.timestamp).getTime();
    const normalizedTs = new Date(normalized.timestamp).getTime();
    if (Math.abs(orderTs - normalizedTs) >= FILL_TIME_WINDOW_MS) return false;

    const orderValue = Math.abs(Number(order.total_value ?? 0));
    const normalizedValue = Math.abs(Number(normalized.total_value ?? 0));
    if (orderValue > 0 && normalizedValue > 0) {
      return Math.abs(orderValue - normalizedValue) < VALUE_EPSILON;
    }

    return true;
  };

  const merged = [...localOrders];
  for (const trade of lyraTradesRaw) {
    const normalized = normalizeLyraTradeForReview(trade);
    if (!normalized) continue;
    const duplicateLocal = merged.some((order) => isSameRecoveredFill(order, normalized));
    if (!duplicateLocal) merged.push(normalized);
  }
  return merged;
}

function deriveClosedTradeCampaigns(orders: TradeOrder[], now = Date.now()): PendingCampaign[] {
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
    const expiryMs = parseExpiryFromInstrument(instrumentName);
    let netExposure = 0;
    let active: Omit<PendingCampaign, 'id' | 'review_state' | 'next_review_at' | 'review_window_days' | 'completed_review_windows'> | null = null;

    for (const order of instrumentOrders) {
      const orderMs = new Date(order.timestamp).getTime();
      if (active && netExposure > EPS && expiryMs != null && Number.isFinite(orderMs) && expiryMs <= Math.min(orderMs, now)) {
        closeCampaignAtExpiry(campaigns, active, instrumentName, expiryMs, netExposure);
        active = null;
        netExposure = 0;
      }

      const qty = Math.abs(Number(order.filled_amount ?? 0));
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
          completed_review_windows: [],
          close_reason: 'offsetting_order',
          expiry_amount: null,
        });
        active = null;
        netExposure = 0;
      }
    }

    if (active && netExposure > EPS) {
      if (expiryMs != null && expiryMs <= now) {
        closeCampaignAtExpiry(campaigns, active, instrumentName, expiryMs, netExposure);
      }
    }
  }

  return campaigns.sort((a, b) => new Date(b.closed_at).getTime() - new Date(a.closed_at).getTime());
}

export async function GET() {
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
    const reviewLookbackStart = now - TRADE_REVIEW_LOOKBACK_DAYS * 24 * 60 * 60 * 1000;
    const recentOrders = getOrdersInRange(
      new Date(reviewLookbackStart).toISOString(),
      new Date(now).toISOString()
    ) as TradeOrder[];
    const spotRows = getSpotPrices(new Date(reviewLookbackStart).toISOString(), 10000) as Array<{ timestamp: string; price: number }>;
    const lyraTradesRaw = await getTradeHistory(reviewLookbackStart);
    const reviewSummary = hasTradeReviewsTable
      ? (getTradeReviewSummary() || { review_count: 0, instrument_count: 0, last_created_at: null })
      : { review_count: 0, instrument_count: 0, last_created_at: null };
    const tradeReviewState = getBudgetCycleState() || {
      last_trade_review_run: 0,
      last_trade_review_success: 0,
      last_trade_review_ready_count: 0,
      last_trade_review_error: null,
      last_trade_review_targets: null,
    };
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
    const derivedCampaigns = deriveClosedTradeCampaigns(mergedOrders, now)
      .map((campaign) => applyExpirySettlementToCampaign(campaign, spotRows));
    const recentCampaignCutoffMs = now - LEARNING_RECENT_LOOKBACK_DAYS * 24 * 60 * 60 * 1000;
    const pendingCampaigns = derivedCampaigns
      .map((campaign) => {
        let reviewState: PendingCampaign['review_state'] = 'awaiting_horizon';
        let nextReviewAt: string | null = null;
        let reviewWindowDays = 1;
        const completedReviewWindows: number[] = [];

        for (const windowDays of TRADE_REVIEW_WINDOWS_DAYS) {
          const horizonEndAt = new Date(new Date(campaign.closed_at).getTime() + windowDays * 24 * 60 * 60 * 1000).toISOString();
          const reviewKey = `${campaign.instrument_name}:${campaign.closed_at}:${windowDays}`;
          if (reviewKeys.has(reviewKey)) {
            reviewState = 'reviewed';
            nextReviewAt = horizonEndAt;
            reviewWindowDays = windowDays;
            completedReviewWindows.push(windowDays);
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
          completed_review_windows: completedReviewWindows,
        };
      })
      .filter((campaign) => new Date(campaign.closed_at).getTime() >= recentCampaignCutoffMs)
      .filter((campaign) => campaign.review_state !== 'reviewed')
      .slice(0, 20);

    return NextResponse.json({
      lessons,
      reviews,
      pendingCampaigns,
      status: {
        hasTradeReviewsTable,
        hasTradeLessonsTable,
        recentOrderStats,
        reviewSummary,
        pendingCampaignSummary: {
          closed_count: pendingCampaigns.length,
          ready_count: pendingCampaigns.filter((campaign) => campaign.review_state === 'ready_for_review').length,
        },
        tradeReviewJob: {
          last_run_at: tradeReviewState.last_trade_review_run
            ? new Date(tradeReviewState.last_trade_review_run).toISOString()
            : null,
          last_success_at: tradeReviewState.last_trade_review_success
            ? new Date(tradeReviewState.last_trade_review_success).toISOString()
            : null,
          ready_count_at_last_run: tradeReviewState.last_trade_review_ready_count ?? 0,
          last_error: tradeReviewState.last_trade_review_error ?? null,
          targets_at_last_run: tradeReviewState.last_trade_review_targets
            ? JSON.parse(tradeReviewState.last_trade_review_targets)
            : [],
          next_due_at: tradeReviewState.last_trade_review_run
            ? new Date(tradeReviewState.last_trade_review_run + 8 * 60 * 60 * 1000).toISOString()
            : null,
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
