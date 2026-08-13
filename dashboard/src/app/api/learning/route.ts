import { NextResponse } from 'next/server';
import {
  getActiveTradeLessons,
  getBudgetCycleState,
  getCanonicalTradeLessons,
  getOrdersInRange,
  getRecentTradeOrderStats,
  getRecentTradeReviews,
  getSpotPrices,
  getTradeLessonEvidence,
  getTradeLessonRevisions,
  getTradeReviewSummary,
  hasTable,
} from '@/lib/db';
import { getTradeHistory } from '@/lib/lyra';
import { cachedJsonRoute } from '@/lib/response-cache';

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

type LearningReview = {
  id: number;
  instrument_name: string;
  action_family: string | null;
  opened_at: string | null;
  closed_at: string;
  review_window_days: number;
  horizon_end_at: string | null;
  order_ids: string[];
  review_status: string;
  review_confidence: number | null;
  summary: string;
  lessons: string[];
  pnl_realized: number | null;
  premium_opened: number | null;
  premium_closed: number | null;
  spot_open: number | null;
  spot_close: number | null;
  spot_min_while_open: number | null;
  spot_max_while_open: number | null;
  spot_min_after_close: number | null;
  spot_max_after_close: number | null;
  created_at: string;
};

const TRADE_REVIEW_WINDOWS_DAYS = [1, 3, 7];

function parseJsonArray(value: string | null | undefined) {
  try {
    const parsed = JSON.parse(value || '[]');
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function getReviewFacts(review: LearningReview) {
  const parsed = parseTradeInstrumentParts(review.instrument_name);
  const expiryMs = parseExpiryFromInstrument(review.instrument_name);
  const expiryAt = expiryMs == null ? null : new Date(expiryMs).toISOString();
  const optionType = parsed?.optionType === 'C' ? 'call' : parsed?.optionType === 'P' ? 'put' : null;
  const distancePct = (spot: number | null) => {
    if (!parsed || spot == null || !(spot > 0)) return null;
    return optionType === 'call'
      ? ((parsed.strike - spot) / spot) * 100
      : ((spot - parsed.strike) / spot) * 100;
  };
  const qualityFlags: string[] = [];
  if (!parsed || expiryMs == null) qualityFlags.push('invalid instrument');
  if (review.spot_open == null) qualityFlags.push('missing open spot');
  if (review.spot_close == null) qualityFlags.push('missing close spot');
  if (expiryMs != null && review.opened_at && new Date(review.opened_at).getTime() > expiryMs) qualityFlags.push('campaign opens after expiry');
  if (expiryMs != null && new Date(review.closed_at).getTime() > expiryMs) qualityFlags.push('campaign closes after expiry');
  if (expiryMs != null && review.horizon_end_at && new Date(review.horizon_end_at).getTime() > expiryMs) {
    qualityFlags.push('review horizon crosses expiry');
  }
  if (review.spot_min_while_open != null && review.spot_max_while_open != null && review.spot_min_while_open > review.spot_max_while_open) {
    qualityFlags.push('invalid open spot range');
  }
  const strikeBreachedWhileOpen = !parsed
    ? null
    : optionType === 'call'
      ? review.spot_max_while_open == null ? null : review.spot_max_while_open >= parsed.strike
      : review.spot_min_while_open == null ? null : review.spot_min_while_open <= parsed.strike;
  const premiumCapturePct = review.action_family === 'short_call_campaign'
    && review.premium_opened != null
    && review.premium_opened > 0
    && review.pnl_realized != null
    ? (review.pnl_realized / review.premium_opened) * 100
    : null;

  return {
    strike: parsed?.strike ?? null,
    option_type: optionType,
    expiry_at: expiryAt,
    otm_at_open_pct: distancePct(review.spot_open),
    otm_at_close_pct: distancePct(review.spot_close),
    strike_breached_while_open: strikeBreachedWhileOpen,
    premium_capture_pct: premiumCapturePct,
    quality_flags: qualityFlags,
  };
}

function getLegacyLessonTitle(lesson: string) {
  const prefix = lesson.match(/^([^:]{4,80}):\s/);
  if (prefix) return prefix[1];
  const firstSentence = lesson.split(/[.!?]/)[0]?.trim() || 'Legacy trade lesson';
  return firstSentence.length > 72 ? `${firstSentence.slice(0, 69)}...` : firstSentence;
}

function getLegacyLessonCategory(lesson: string) {
  const normalized = lesson.toLowerCase();
  if (normalized.includes('strike selection')) return 'strike_selection';
  if (normalized.includes('exit timing')) return 'exit_timing';
  if (normalized.includes('execution')) return 'execution';
  return 'process';
}

const LEGACY_LESSON_META: Record<string, { title: string; category: string; action_family: string | null }> = {
  'short_call.exit_insurance': { title: 'Short-call exit insurance', category: 'exit_timing', action_family: 'short_call_campaign' },
  'short_call.strike_and_sizing': { title: 'Short-call strike and sizing', category: 'strike_selection', action_family: 'short_call_campaign' },
  'tail_put.strike_and_cost': { title: 'Tail-put strike and insurance cost', category: 'strike_selection', action_family: 'long_put_campaign' },
  'tail_put.lifecycle': { title: 'Tail-put lifecycle', category: 'exit_timing', action_family: 'long_put_campaign' },
  'process.decision_quality': { title: 'Decision quality versus outcome', category: 'process', action_family: null },
  'execution.order_hygiene': { title: 'Execution and order hygiene', category: 'execution', action_family: null },
};

function getLegacyLessonKey(lesson: string) {
  const normalized = lesson.toLowerCase();
  const isPut = normalized.includes('put') || normalized.includes('tail hedge') || normalized.includes('protection');
  const isShortCall = normalized.includes('short call') || normalized.includes('call strike') || normalized.includes('buyback');
  if (isShortCall && (normalized.includes('strike selection') || normalized.includes('positioning') || normalized.includes('otm buffer'))) {
    return 'short_call.strike_and_sizing';
  }
  if (isShortCall && (normalized.includes('exit') || normalized.includes('buyback') || normalized.includes('intervention'))) {
    return 'short_call.exit_insurance';
  }
  if (isPut && normalized.includes('strike')) return 'tail_put.strike_and_cost';
  if (isPut && (normalized.includes('exit') || normalized.includes('hold') || normalized.includes('duration'))) return 'tail_put.lifecycle';
  if (normalized.includes('tranche') || normalized.includes('micro') || normalized.includes('round-trip') || normalized.includes('buyback-resell')) {
    return 'execution.order_hygiene';
  }
  return 'process.decision_quality';
}

function groupTradeReviewCampaigns(reviews: Array<LearningReview & { facts: ReturnType<typeof getReviewFacts> }>) {
  const grouped = new Map<string, {
    id: string;
    instrument_name: string;
    action_family: string | null;
    opened_at: string | null;
    closed_at: string;
    pnl_realized: number | null;
    premium_opened: number | null;
    premium_closed: number | null;
    spot_open: number | null;
    spot_close: number | null;
    facts: ReturnType<typeof getReviewFacts>;
    reviews: Array<LearningReview & { facts: ReturnType<typeof getReviewFacts> }>;
  }>();

  for (const review of reviews) {
    const key = `${review.instrument_name}:${review.closed_at}`;
    const campaign = grouped.get(key);
    if (campaign) {
      campaign.reviews.push(review);
      continue;
    }
    grouped.set(key, {
      id: key,
      instrument_name: review.instrument_name,
      action_family: review.action_family,
      opened_at: review.opened_at,
      closed_at: review.closed_at,
      pnl_realized: review.pnl_realized,
      premium_opened: review.premium_opened,
      premium_closed: review.premium_closed,
      spot_open: review.spot_open,
      spot_close: review.spot_close,
      facts: review.facts,
      reviews: [review],
    });
  }

  return Array.from(grouped.values())
    .map((campaign) => {
      campaign.reviews.sort((a, b) => a.review_window_days - b.review_window_days);
      const latestReview = campaign.reviews[campaign.reviews.length - 1];
      return { ...campaign, facts: latestReview.facts, latest_review: latestReview };
    })
    .sort((a, b) => new Date(b.closed_at).getTime() - new Date(a.closed_at).getTime());
}

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

async function getLearningResponse() {
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
    let lyraTradesRaw: Record<string, unknown>[] = [];
    let tradeHistoryError: string | null = null;
    try {
      const recoveredTrades = await getTradeHistory(reviewLookbackStart);
      lyraTradesRaw = Array.isArray(recoveredTrades) ? recoveredTrades as Record<string, unknown>[] : [];
    } catch (error: unknown) {
      tradeHistoryError = error instanceof Error ? error.message : 'Trade-history recovery unavailable';
    }
    const reviewSummary = hasTradeReviewsTable
      ? (getTradeReviewSummary() || { review_count: 0, instrument_count: 0, last_created_at: null })
      : { review_count: 0, instrument_count: 0, last_created_at: null };
    const tradeReviewState = getBudgetCycleState() || {
      last_trade_review_run: 0,
      last_trade_review_success: 0,
      last_trade_review_ready_count: 0,
      last_trade_review_error: null,
      last_trade_review_targets: null,
      last_trade_lesson_run: 0,
      last_trade_lesson_success: 0,
      last_trade_lesson_error: null,
    };
    const canonicalLessonRows = hasTradeLessonsTable ? getCanonicalTradeLessons() : [];
    const legacyLessonRows = hasTradeLessonsTable && canonicalLessonRows.length === 0 ? getActiveTradeLessons() : [];
    const evidenceRows = canonicalLessonRows.length > 0 ? getTradeLessonEvidence() : [];
    const revisionRows = canonicalLessonRows.length > 0 ? getTradeLessonRevisions() : [];
    const evidenceByLesson = new Map<number, { supporting: typeof evidenceRows; contradicting: typeof evidenceRows }>();
    const revisionsByLesson = new Map<number, typeof revisionRows>();
    for (const evidence of evidenceRows) {
      const group = evidenceByLesson.get(evidence.lesson_id) || { supporting: [], contradicting: [] };
      if (evidence.stance === 'contradicting') group.contradicting.push(evidence);
      else if (evidence.stance === 'supporting') group.supporting.push(evidence);
      evidenceByLesson.set(evidence.lesson_id, group);
    }
    for (const revision of revisionRows) {
      const group = revisionsByLesson.get(revision.lesson_id) || [];
      group.push(revision);
      revisionsByLesson.set(revision.lesson_id, group);
    }
    const legacyGroups = new Map<string, typeof legacyLessonRows>();
    for (const lesson of legacyLessonRows) {
      const key = getLegacyLessonKey(lesson.lesson);
      const group = legacyGroups.get(key) || [];
      group.push(lesson);
      legacyGroups.set(key, group);
    }
    const lessons = canonicalLessonRows.length > 0
      ? canonicalLessonRows.map((lesson) => ({
          ...lesson,
          applicability: parseJsonArray(lesson.applicability).map(String),
          evidence: evidenceByLesson.get(lesson.id) || { supporting: [], contradicting: [] },
          revisions: (revisionsByLesson.get(lesson.id) || []).map((revision) => ({
            ...revision,
            applicability: parseJsonArray(revision.applicability).map(String),
          })),
          is_legacy: false,
        }))
      : Array.from(legacyGroups.entries()).map(([lessonKey, group]) => {
          const lesson = group[0];
          const meta = LEGACY_LESSON_META[lessonKey] || {
            title: getLegacyLessonTitle(lesson.lesson),
            category: getLegacyLessonCategory(lesson.lesson),
            action_family: null,
          };
          return {
            ...lesson,
            lesson_key: lessonKey,
            title: meta.title,
            category: meta.category,
            action_family: meta.action_family,
            applicability: [],
            status: 'candidate',
            revision: 1,
            change_summary: null,
            updated_at: lesson.created_at,
            supporting_review_count: 0,
            supporting_campaign_count: 0,
            contradicting_review_count: 0,
            contradicting_campaign_count: 0,
            legacy_evidence_claim: lesson.evidence_count,
            legacy_observation_count: group.length,
            evidence: { supporting: [], contradicting: [] },
            revisions: [],
            is_legacy: true,
          };
        });
    const reviews: Array<LearningReview & { facts: ReturnType<typeof getReviewFacts> }> = hasTradeReviewsTable
      ? getRecentTradeReviews(60).map((review) => {
          const parsedReview: LearningReview = {
            ...review,
            lessons: parseJsonArray(review.lessons).map(String),
            order_ids: parseJsonArray(review.order_ids).map(String),
          };
          return { ...parsedReview, facts: getReviewFacts(parsedReview) };
        })
      : [];
    const campaigns = groupTradeReviewCampaigns(reviews).slice(0, 20);
    const mergedOrders = mergeOrdersForTradeReview(
      recentOrders,
      lyraTradesRaw
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
      campaigns,
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
        learningSummary: {
          canonical_mode: canonicalLessonRows.length > 0,
          canonical_count: canonicalLessonRows.length,
          active_count: canonicalLessonRows.filter((lesson) => lesson.status === 'active').length,
          disputed_count: canonicalLessonRows.filter((lesson) => lesson.status === 'disputed').length,
          candidate_count: canonicalLessonRows.filter((lesson) => lesson.status === 'candidate').length,
          linked_evidence_count: evidenceRows.length,
          campaign_count: campaigns.length,
          legacy_lesson_count: legacyLessonRows.length,
        },
        dataSources: {
          trade_history_recovery: tradeHistoryError ? 'degraded' : 'available',
          trade_history_error: tradeHistoryError,
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
        tradeLessonJob: {
          last_run_at: tradeReviewState.last_trade_lesson_run
            ? new Date(tradeReviewState.last_trade_lesson_run).toISOString()
            : null,
          last_success_at: tradeReviewState.last_trade_lesson_success
            ? new Date(tradeReviewState.last_trade_lesson_success).toISOString()
            : null,
          last_error: tradeReviewState.last_trade_lesson_error ?? null,
          next_due_at: tradeReviewState.last_trade_lesson_run
            ? new Date(
                tradeReviewState.last_trade_lesson_run
                + (tradeReviewState.last_trade_lesson_error ? 30 * 60 * 1000 : 8 * 60 * 60 * 1000)
              ).toISOString()
            : new Date().toISOString(),
        },
      },
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({
      error: message,
      lessons: [],
      campaigns: [],
      pendingCampaigns: [],
      status: null,
    }, { status: 500 });
  }
}

export function GET(request: Request) {
  return cachedJsonRoute(request, 'learning', getLearningResponse, {
    freshMs: 2 * 60_000,
    staleMs: 10 * 60_000,
    browserMaxAgeSeconds: 60,
  });
}
