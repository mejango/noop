/**
 * UNIFIED OPTIONS TRADING BOT for Lyra Finance
 * =============================================
 *
 * Purpose:
 * --------
 * Automated dual-strategy ETH options trading system that:
 *   1. Accumulates long OTM puts as downside insurance.
 *   2. Sells OTM calls opportunistically to collect premium.
 *
 * Core Strategies:
 * ----------------
 * (1) Put Accumulation (Long Puts)
 *   • Targets: out-of-the-money ETH puts
 *     – Delta between -0.08 and -0.02
 *     – 45–78 days to expiry
 *     – Strike < 0.8 × current index price
 *   • Entry Conditions:
 *     – Standard Entry: buy best option if medium-term ≠ upward AND short-term ≠ upward
 *     – Confident Downtrend: buy immediately when medium-term downtrend + short-term downtrend + 3-day downward spike
 *   • Exit Conditions:
 *     – Standard Exit: sell back when medium-term momentum flips to upward
 *     – Confident Uptrend: sell back immediately when medium-term uptrend + short-term uptrend + 7-day upward spike (no expiry restrictions)
 *     – Buffer Protection: 7-day upward spike requires 0.5% buffer above 7-day high to prevent round-tripping
 *
 * (2) Call Selling (Short Calls)
 *   • Targets: out-of-the-money ETH calls
 *     – Delta between +0.10 and +0.30
 *     – 14–30 days to expiry
 *     – Strike > 1.2 × current index price
 *   • Entry Conditions:
 *     – Standard Entry: sell best option if medium-term ≠ upward AND short-term ≠ upward
 *     – Confident Downtrend: sell immediately when medium-term downtrend + short-term downtrend + 3-day downward spike
 *   • Exit Conditions:
 *     – Standard Exit: buy back when medium-term momentum flips to upward (≤7 days to expiry)
 *     – Confident Uptrend: buy back immediately when medium-term uptrend + short-term uptrend + 3-day upward spike (no expiry restrictions)
 *
 * Enhanced Momentum Detection:
 * ----------------------------
 *   • Medium-term: ADX(21) + MACD(12,26,13) computed on 5-minute OHLC candles
 *     – ADX >= 25 required to confirm trend strength
 *     – Direction from MACD vs signal line with acceleration/deceleration detection
 *   • Short-term: multi-timeframe analysis (15min, 1h, 1d)
 *     – Flat/Slanted/Steep classification based on 15-minute momentum change
 *     – Spike detection across 1h, 1d, and 3d timeframes
 *     – Momentum derivatives: flat, slanted, steep with spike annotations
 *
 * Advanced Entry/Exit Strategies:
 * -------------------------------
 *   • Confident Uptrend Setup (Exit Strategy):
 *     – Triggers: medium-term uptrend + short-term uptrend + 7-day upward spike (with 0.5% buffer)
 *     – Action: Buy back ALL sold calls and sell back ALL bought puts
 *     – No expiry restrictions: closes positions regardless of days to expiry
 *   • Confident Downtrend Setup (Entry Strategy):
 *     – Triggers: medium-term downtrend + short-term downtrend + 3-day downward spike
 *     – Action: Buy puts and sell calls immediately from available options
 *     – No historical score requirements: accepts all valid options, chooses best available
 *
 * Timing & Cycles:
 * ----------------
 *   • 10-day trading cycle with Historical Best Buy/Sell system
 *   • Dynamic loop intervals:
 *     – 1 min: urgent (confident uptrend exit OR double-downtrend entry)
 *     – 2 min: accelerated (short-term downward only)
 *     – 5 min: normal
 *
 * Sizing & Risk:
 * --------------
 *   • ETH-collateralized: puts bought on leverage, long puts offset ETH in margin engine
 *   • Put buying has arithmetic budget discipline (3.33% of insured base/yr in 15d cycles)
 *   • Margin state (initial/maintenance/liquidation) fetched from Derive's get_subaccount API
 *   • Sizing capped by both margin health AND put budget discipline; amount quantized to venue step
 *
 * Execution:
 * ----------
 *   • Instruments prefetched from /public/get_instruments
 *   • Enriched with greeks & AMM prices from /public/get_tickers
 *   • Orders submitted via /private/order as LIMIT takers:
 *     – Buys at best ask
 *     – Sells at best bid
 *   • Fee cap = 6% of notional
 *   • exit-only close/trim semantics when closing positions
 *   • time_in_force: IOC for buys and sells.
 *
 * Data & Logging:
 * ---------------
 *   • All data persisted to SQLite (spot prices, options snapshots, onchain data, orders)
 *   • Cycle state persisted in SQLite across restarts
 *   • Enhanced logging with strategy-specific reasons and performance tracking
 *
 * Requirements:
 * -------------
 *   • Node.js environment with ethers.js, axios, technicalindicators, fs, path
 *   • .private_key.txt with trading wallet private key
 *   • Internet access to Lyra API + CoinGecko fallback spot feed
 *
 * Caveats:
 * --------
 *   • Strike gating uses Derive spot/index when available; CoinGecko is only a fallback.
 *   • No global risk checks on vega/theta exposure or per-expiry concentration.
 *   • No backoff/throttling: may hit API limits under heavy load.
 *   • Execution relies on book liquidity at best bid/ask; slippage not bounded.
 */
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { getInstrumentPriceStep, roundToStep, getStepDecimals, normalizePriceToStep, normalizeOrderPriceForVenue, avoidRoundNumberRestingPrice, computePostOnlyRetryPrice } = require('./bot/order-pricing');
const { normalizeDesiredExitOrder, compareRestingExitOrder, resolveDesiredExitOrderType, getDesiredSellPutRemainingAmount } = require('./bot/resting-exit-plan');
const {
  evaluateConditions,
  evaluateExitConditions,
  resolveConfirmationVotes,
  hasUsableMarginState,
  getPutReplacementCoverage,
  liveRuleValues,
  validateFinalOrderPolicy,
} = require('./bot/trade-policy');
const { ADX, MACD } = require('technicalindicators');
const { ethers, AbiCoder } = require('ethers');
const {
  SELL_CALL_EDGE_REFERENCE_DTE,
  SELL_CALL_EDGE_DTE_EXPONENT,
  normalizeSellCallScore,
} = require('./bot/call-score');
const {
  BUY_PUT_EDGE_REFERENCE_DTE,
  BUY_PUT_EDGE_DTE_EXPONENT,
  BUY_PUT_EDGE_MIN_DTE,
  BUY_PUT_EDGE_MAX_DTE,
  getBuyPutDteNormalizationFactor,
  normalizeBuyPutScore,
  getBuyPutPriceForEdgeScore,
} = require('./bot/put-score');
const { isBetterBuyPutCandidate, computeMatchedPutCallSkew } = require('./bot/option-market-quality');
const { fundingRatesFromTickerResult, summarizeFundingRates } = require('./bot/funding-rates');
const { summarizeAdvisoryQuotes, candidateSpreadPct } = require('./bot/advisory-quotes');
const { readAdvisoryMarketSnapshot } = require('./bot/advisory-market-snapshot');
const { runReviewedPublication } = require('./bot/advisory-publication');

const encoder = new AbiCoder();

// SQLite database (loaded via bot/index.js or standalone)
const db = global.__noopDb || null;
const { createEconomicStore, syncV2TradesProgressively } = require('./bot/economic-events');
const { observationUniverse, missingExpiryDates } = require('./bot/observations');
const { buildPortfolioObservation } = require('./bot/portfolio-observation');
const economicStore = db?.db ? createEconomicStore(db.db) : null;
if (db) {
  console.log('SQLite database connected');
} else {
  console.log('Running without SQLite (standalone mode)');
}

// Lyra API endpoints
const API_URL = {
  GET_TICKERS: 'https://api.lyra.finance/public/get_tickers',
  GET_INSTRUMENTS: 'https://api.lyra.finance/public/get_instruments',
  PLACE_ORDER: 'https://api.lyra.finance/private/order',
  GET_OPEN_ORDERS: 'https://api.lyra.finance/private/get_open_orders',
  GET_ORDER_HISTORY: 'https://api.lyra.finance/private/get_order_history',
  GET_TRADE_HISTORY: 'https://api.lyra.finance/private/get_trade_history',
  CANCEL_ORDER: 'https://api.lyra.finance/private/cancel',
  GET_ORDER: 'https://api.lyra.finance/private/get_order',
  GET_SUBACCOUNT: 'https://api.lyra.finance/private/get_subaccount',
}

// CoinGecko API for spot price
const COINGECKO_API = 'https://api.coingecko.com/api/v3';

// DEX APIs for liquidity analysis
const _THEGRAPH_KEY = process.env.THEGRAPH_API_KEY || '9bc783800c9a60b574487c0ee711609a';
const DEX_APIS = {
  UNISWAP_V3: `https://gateway.thegraph.com/api/${_THEGRAPH_KEY}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV`,
  UNISWAP_V4: `https://gateway.thegraph.com/api/${_THEGRAPH_KEY}/subgraphs/id/DiYPVdygkfjDWhbxGSqAQxwBKmfKnkWQojqeM2rkLb3G`
};

const assertGraphQLSuccess = (response) => {
  const errors = response.data?.errors;
  if (!Array.isArray(errors) || errors.length === 0) return;
  const message = errors
    .map(error => error?.message)
    .filter(Boolean)
    .join('; ')
    .slice(0, 240);
  if (message.includes('bad indexers')) {
    throw new Error('GraphQL bad indexers (subgraph unavailable)');
  }
  throw new Error(`GraphQL error: ${message || 'unknown error'}`);
};

// Common configuration
const DERIVE_ACCOUNT_ADDRESS = '0xD87890df93bf74173b51077e5c6cD12121d87903';
const ACTION_TYPEHASH = '0x4d7a9f27c403ff9c0f19bce61d76d82f9aa29f8d6d4b0c5474607d9770d1af17';
const TRADE_MODULE_ADDRESS = '0xB8D20c2B7a1Ad2EE33Bc50eF10876eD3035b5e7b';
const DOMAIN_SEPARATOR = '0xd96e5f90797da7ec8dc4e276260c7f3f87fedf68775fbe1ef116e996fc60441b';

// Common trading parameters (single source of truth: bot/config.json)
const BOT_CONFIG = JSON.parse(fs.readFileSync(path.join(__dirname, 'bot', 'config.json'), 'utf-8'));
const STRATEGY_FACTS = require('./bot/strategy-facts.json');
// Arithmetic discipline: spend 3.33% of insured base per year on puts,
// allocated in PERIOD_DAYS windows. Budget recalculated at each cycle start.
// Formula: insuredBaseValue * PUT_ANNUAL_RATE / (365 / PERIOD_DAYS)
const PUT_ANNUAL_RATE = BOT_CONFIG.PUT_ANNUAL_RATE || 0.0333;
const PUT_INSURED_EXTERNAL_ETH = Math.max(0, Number(process.env.PUT_INSURED_EXTERNAL_ETH || 0));
// Call exposure discipline: target 45% displayed margin utilization. The
// buffer is last-mile safety for estimate drift, not planned entry headroom.
const CALL_EXPOSURE_CAP_PCT = BOT_CONFIG.CALL_EXPOSURE_CAP_PCT || 0.45;
const CALL_EXPOSURE_BUFFER_PCT = Math.max(0, Number(BOT_CONFIG.CALL_EXPOSURE_BUFFER_PCT ?? 0.05));
const CALL_BREAKOUT_OVERRIDE_CAP_PCT = Math.max(
  CALL_EXPOSURE_CAP_PCT,
  BOT_CONFIG.CALL_BREAKOUT_OVERRIDE_CAP_PCT || 0.65
);
const getCallExposureLimitPct = (targetCapPct) => Math.min(
  1,
  Math.max(0, Number(targetCapPct) || 0) + CALL_EXPOSURE_BUFFER_PCT
);
const CALL_EXPOSURE_LIMIT_PCT = getCallExposureLimitPct(CALL_EXPOSURE_CAP_PCT);
const CALL_BREAKOUT_OVERRIDE_LIMIT_PCT = getCallExposureLimitPct(CALL_BREAKOUT_OVERRIDE_CAP_PCT);
const CALL_ENTRY_BUFFER_PCT = BOT_CONFIG.CALL_ENTRY_BUFFER_PCT || 0.05;
const CALL_ENTRY_CAP_PCT = Math.max(0, CALL_EXPOSURE_CAP_PCT - CALL_ENTRY_BUFFER_PCT);
const CALL_BREAKOUT_DERIVATIVES = new Set(['moving', 'slanted', 'steep']);
const SUBACCOUNT_ID = 25923;
// The automatic ledger starts before this process can trade and resumes from the
// same durable boundary after restarts. Older history is an explicit import.
const ECONOMIC_TRACKING_START = economicStore?.startTracking(SUBACCOUNT_ID, Date.now()) || null;

// ─── Telegram Notifications ──────────────────────────────────────────────────
const sendTelegram = async (message) => {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return;
  try {
    await axios.post(`https://api.telegram.org/bot${token}/sendMessage`, {
      chat_id: chatId,
      text: message,
      parse_mode: 'Markdown',
    }, { timeout: 5000 });
  } catch (e) {
    // Retry without Markdown only if Telegram rejected the parse (unescaped _ * [ etc).
    // A timeout usually means it was delivered; resending would duplicate it.
    if (e.response?.status !== 400) return console.log('📱 Telegram failed:', e.message);
    try {
      await axios.post(`https://api.telegram.org/bot${token}/sendMessage`, {
        chat_id: chatId,
        text: message,
      }, { timeout: 5000 });
    } catch (e2) {
      console.log('📱 Telegram failed:', e2.message);
    }
  }
};

const formatAdvisoryRunNotification = ({ advisoryId, trigger, attempt, retryCount = 0 }) => {
  const extraReview = attempt > 1;
  const reason = extraReview
    ? 'Additional review: market data or positions changed during deliberation.'
    : trigger === 'retry'
    ? `Retry after failed or deferred advisory (${retryCount} prior failure${retryCount === 1 ? '' : 's'}).`
    : 'Normal schedule.';
  return [
    extraReview ? '📋 *ADVISORY EXTRA REVIEW*' : '📋 *ADVISORY STARTED*',
    reason,
    `Full AI review: ${attempt}`,
    `Run: \`${advisoryId}\``,
  ].join('\n');
};

const notifyAdvisoryRun = async (details) => {
  try {
    await sendTelegram(formatAdvisoryRunNotification(details));
  } catch (e) {
    // Notification delivery must not fail or retry an otherwise valid advisory.
    console.log('📱 Advisory notification failed:', e.message);
  }
};

const ORDER_NOTIFICATION_LABELS = Object.freeze({
  buy_put: Object.freeze({ order: 'BUY PUT', executed: 'BOUGHT PUT' }),
  sell_put: Object.freeze({ order: 'SELL PUT', executed: 'SOLD PUT' }),
  sell_call: Object.freeze({ order: 'SELL CALL', executed: 'SOLD CALL' }),
  buyback_call: Object.freeze({ order: 'BUYBACK CALL', executed: 'BOUGHT BACK CALL' }),
});

const formatOrderNotificationNumber = (value, decimals = 4) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 'N/A';
  return numeric.toFixed(decimals).replace(/\.?0+$/, '');
};

const formatOrderLifecycleNotification = ({
  stage,
  action,
  instrumentName,
  amount,
  filledAmount = null,
  price,
  totalValue = null,
  orderType = null,
  orderId = null,
  status = null,
}) => {
  const labels = ORDER_NOTIFICATION_LABELS[action] || {
    order: String(action || 'ORDER').replace(/_/g, ' ').toUpperCase(),
    executed: String(action || 'ORDER').replace(/_/g, ' ').toUpperCase(),
  };
  const isExecution = stage === 'executed' || stage === 'partial_fill';
  const stageMeta = {
    posted: { emoji: '📨', title: 'ORDER POSTED' },
    executed: { emoji: '✅', title: 'ORDER EXECUTED' },
    partial_fill: { emoji: '🟡', title: 'ORDER PARTIALLY EXECUTED' },
    cancelled: { emoji: '🗑️', title: 'ORDER CANCELLED' },
    expired: { emoji: '⌛', title: 'ORDER EXPIRED' },
  }[stage] || { emoji: '📋', title: 'ORDER UPDATE' };
  const lines = [
    `${stageMeta.emoji} *${stageMeta.title}: ${isExecution ? labels.executed : labels.order}*`,
    instrumentName,
  ];
  const amountText = formatOrderNotificationNumber(amount);
  const priceText = formatOrderNotificationNumber(price);
  if (isExecution) {
    const filledText = formatOrderNotificationNumber(filledAmount);
    lines.push(`Filled: ${filledText}${amount != null ? ` / ${amountText}` : ''} @ $${priceText}`);
    if (totalValue != null) lines.push(`Total: $${formatOrderNotificationNumber(totalValue)}`);
  } else {
    lines.push(`Amount: ${amountText} @ $${priceText}`);
  }
  if (orderType) lines.push(`Type: ${String(orderType).replace(/_/g, ' ').toUpperCase()}`);
  if (status) lines.push(`Status: ${String(status).replace(/_/g, ' ')}`);
  if (orderId) lines.push(`Order: ${orderId}`);
  return lines.join('\n');
};

const notifyOrderLifecycle = (details) => {
  sendTelegram(formatOrderLifecycleNotification(details));
};

// ETH contract addresses for analysis
const ETH_CONTRACTS = {
  WETH: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
  ETH: '0x0000000000000000000000000000000000000000', // Native ETH in Uniswap V4
  USDC: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', // Correct USDC address
  USDT: '0xdAC17F958D2ee523a2206206994597C13D831ec7',
  WBTC: '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599' // Add WBTC for comprehensive analysis
};


// Time constants for consistent calculations
const TIME_CONSTANTS = {
  WEEK: 7 * 24 * 60 * 60 * 1000,
  THREE_DAYS: 3 * 24 * 60 * 60 * 1000,
  DAY: 24 * 60 * 60 * 1000,
  HOUR: 60 * 60 * 1000,
  MINUTE: 60 * 1000
};

// Dynamic check intervals based on market conditions
const DYNAMIC_INTERVALS = {
  'urgent': 45 * 1000,                   // 45 second - maximum urgency
  'normal': 5 * 60 * 1000                 // 5 minutes - normal interval
};

// Configuration
const BOT_DATA_PATH = process.env.DATA_DIR ? path.join(process.env.DATA_DIR, 'bot_data.json') : './bot_data.json';

const PERIOD = BOT_CONFIG.PERIOD_DAYS * 1000 * 60 * 60 * 24;

// Trading parameters - PUTS
const PUT_EXPIRATION_RANGE = [BUY_PUT_EDGE_MIN_DTE, BUY_PUT_EDGE_MAX_DTE];
const PUT_DELTA_RANGE = STRATEGY_FACTS.put_delta_range; // Negative delta for puts
const BUY_PUT_ADVISORY_DTE_RANGE = [BUY_PUT_EDGE_MIN_DTE, BUY_PUT_EDGE_MAX_DTE];
const ADVISORY_OPTION_VALUE_WINDOW_DAYS = 6.2;
const BUY_PUT_URGENT_SCORE_NUDGE = 1.005;
const BUY_PUT_PATIENT_SCORE_NUDGE = 1.02;
const BUY_PUT_REPRICING_LAG_SCORE_NUDGE = 0.999;
const BUY_PUT_REPRICING_LAG_LOOKBACK_MINUTES = 20;
const BUY_PUT_REPRICING_LAG_SCORE_LOOKBACK_HOURS = 1;
const BUY_PUT_REPRICING_LAG_MIN_SCORE_TREND_PCT = 6;
const BUY_PUT_REPRICING_LAG_MIN_SPOT_DROP_PCT = -0.35;
const BUY_PUT_REPRICING_LAG_NEAR_BEST_PCT = 90;
const BUY_PUT_RECENT_VALUE_LOOKBACK_HOURS = 6;
const BUY_PUT_RECENT_VALUE_MIN_SAMPLES = 4;
const BUY_PUT_RECENT_VALUE_MIN_TREND_PCT = 8;
const BUY_PUT_RECENT_VALUE_MIN_PERCENTILE = 80;
const BUY_PUT_RECENT_VALUE_MIN_ROLLING_BEST_PCT = 70;
const BUY_PUT_EDGE_SHOCKS = [0.30, 0.40, 0.50];
const PUT_ROLL_DTE_THRESHOLD = STRATEGY_FACTS.put_roll_dte_threshold;
const PUT_MONETIZATION_PROFIT_THRESHOLD = STRATEGY_FACTS.put_monetization_profit_threshold_pct;
const PUT_MONETIZATION_MAX_TRANCHE_FRACTION = STRATEGY_FACTS.put_monetization_max_tranche_fraction;
// Rolling a put that only recovers pennies sells a crash ticket for nothing; require real recovery.
const PUT_ROLL_MIN_RECOVERY_PCT = Number(STRATEGY_FACTS.put_roll_min_recovery_pct) || 0;
// Monetization ladder: each successive tranche needs a bigger multiple; once exhausted, hold the rest.
const PUT_MONETIZATION_LADDER_PCT = Array.isArray(STRATEGY_FACTS.put_monetization_ladder_pct) && STRATEGY_FACTS.put_monetization_ladder_pct.length
  ? STRATEGY_FACTS.put_monetization_ladder_pct.map(Number)
  : [PUT_MONETIZATION_PROFIT_THRESHOLD];
const PUT_MONETIZATION_MIN_INTRINSIC_FRACTION = Number(STRATEGY_FACTS.put_monetization_min_intrinsic_fraction) || 0;
const REJECTED_ACTION_BACKOFF_MS = 60 * 60 * 1000;
const MANDELBROT_SPOT_PATH_LOOKBACK_DAYS = 30;
const MANDELBROT_SPOT_PATH_INTERVAL_HOURS = 1;
const MANDELBROT_SPOT_PATH_MAX_POINTS = (MANDELBROT_SPOT_PATH_LOOKBACK_DAYS * 24) + 1;

// Trading parameters - CALLS  
const CALL_EXPIRATION_RANGE = STRATEGY_FACTS.call_dte_range;
const CALL_DELTA_RANGE = STRATEGY_FACTS.call_delta_range; // Positive delta for calls
const SELL_CALL_FALLBACK_MIN_BID = STRATEGY_FACTS.sell_call_fallback_min_bid;
const SELL_CALL_FALLBACK_MIN_SCORE = STRATEGY_FACTS.sell_call_fallback_min_score;
const CANDIDATE_OBSERVATION_TOP_N = 8;

// Call buyback thresholds
const CALL_BUYBACK_PROFIT_THRESHOLD = STRATEGY_FACTS.call_buyback_profit_threshold_pct; // Harvest short calls once at least this much premium is captured

// Journal auto-generation
const JOURNAL_INTERVAL_MS = 8 * 60 * 60 * 1000; // Every 8 hours
const TRADE_REVIEW_INTERVAL_MS = 8 * 60 * 60 * 1000; // Every 8 hours
const TRADE_LESSON_INTERVAL_MS = 8 * 60 * 60 * 1000; // Normal canonical synthesis cadence
const TRADE_LESSON_RETRY_INTERVAL_MS = 30 * 60 * 1000; // Retry failed synthesis promptly
const TRADE_LESSON_SYNTHESIS_TIMEOUT_MS = 90 * 1000;
const TRADE_LESSON_BOOTSTRAP_REVIEW_LIMIT = 48;
const WIKI_LINT_INTERVAL_MS = 24 * 60 * 60 * 1000; // Every 24 hours

// Common bot state structure
const createBotData = () => {
let botData = {
    lastCheck: 0,
    mediumTermMomentum: { main: 'neutral', derivative: null },
    shortTermMomentum: { main: 'neutral', derivative: null },
    lastSpotPrice: null,
    lastSpotPriceTimestamp: null,

    // Account balance (refreshed each tick for call exposure cap)
    ethBalance: 0,

    // Put budget discipline (arithmetic cost commitment per cycle)
    putCycleStart: null,
    putBudgetForCycle: 0,      // USD budget for current cycle (set dynamically at cycle start)
    putNetBought: 0,           // USD spent on puts this cycle
    putUnspentBuyLimit: 0,     // rollover from previous cycles

    // Timing (persisted to survive restarts)
    lastJournalGeneration: 0,
    lastWikiLintRun: 0,
    lastTradeReviewRun: 0,
    lastTradeReviewSuccess: 0,
    lastTradeReviewReadyCount: 0,
    lastTradeReviewError: null,
    lastTradeReviewTargets: [],
    lastHypothesisLessonReviewId: 0,
    lastTradeLessonReviewId: 0,
    lastTradeLessonRun: 0,
    lastTradeLessonSuccess: 0,
    lastTradeLessonError: null,

    // Advisory tracking
    lastAdvisoryRun: 0,
    lastAdvisorySuccess: 0,
    lastAdvisoryError: null,
    advisoryRetryCount: 0,
    nextAdvisoryRetryAt: 0,
    lastAdvisorySpotPrice: null,  // spot price when last advisory ran
    lastAdvisoryTimestamp: 0,     // when last advisory ran
  };

  return botData;
};

const DEFAULT_AMOUNT_STEP = 0.001;
const VENUE_AMOUNT_DECIMALS = 2;
const VENUE_MIN_ORDER_AMOUNT = 0.1;

let botData = createBotData();

// Advisory mutex — prevent overlapping LLM advisory runs
let _advisoryInFlight = false;
let _wikiIngestInFlight = false;
let _wikiLintInFlight = false;

const ADVISORY_RETRY_BACKOFF_MS = [
  5 * 60 * 1000,
  15 * 60 * 1000,
  30 * 60 * 1000,
  60 * 60 * 1000,
  2 * 60 * 60 * 1000,
  4 * 60 * 60 * 1000,
];

const getAdvisoryRetryDelayMs = (retryCount) => {
  const idx = Math.max(0, Math.min(ADVISORY_RETRY_BACKOFF_MS.length - 1, retryCount - 1));
  return ADVISORY_RETRY_BACKOFF_MS[idx];
};

const persistCycleState = () => {
  if (!db) return;
  try { db.saveBotState(botData); }
  catch (e) { console.error('Failed to persist cycle state:', e.message); }
};

const getPutBudgetPortfolioValue = (ethBalance, usdcBalance, spotPrice) => {
  const insuredEth = Number(ethBalance || 0) + PUT_INSURED_EXTERNAL_ETH;
  const ethValue = insuredEth * Number(spotPrice || 0);
  const usdcValue = Number(usdcBalance || 0);
  return usdcValue + ethValue;
};

const getFallbackPutBudgetPortfolioValue = (spotPrice) => {
  if (!db?.getLatestPortfolioSnapshot) return { value: 0, source: null, error: null };
  try {
    const snap = db.getLatestPortfolioSnapshot();
    if (!snap) return { value: 0, source: null, error: null };
    const value = getPutBudgetPortfolioValue(
      Number(snap.eth_balance || 0),
      Number(snap.usdc_balance || 0),
      Number(spotPrice || snap.spot_price || 0)
    );
    return value > 0
      ? { value, source: 'latest_portfolio_snapshot', error: null }
      : { value: 0, source: null, error: 'latest portfolio snapshot has no positive insured value' };
  } catch (e) {
    return { value: 0, source: null, error: e.message };
  }
};

// Recalculate put budget at cycle boundaries.
// Budget = insuredBaseValue * PUT_ANNUAL_RATE / (365 / PERIOD_DAYS)
// Called each tick — resets cycle when PERIOD elapses.
const maybeResetPutCycle = (portfolioValue, options = {}) => {
  const now = Date.now();
  const cycleExpired = botData.putCycleStart && (now - botData.putCycleStart) >= PERIOD;
  const noCycle = !botData.putCycleStart;

  if (noCycle || cycleExpired) {
    // Roll over unspent budget from previous cycle
    if (cycleExpired) {
      const prevRemaining = Math.max(0, botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought);
      botData.putUnspentBuyLimit = prevRemaining;
    }

    const cyclesPerYear = 365 / BOT_CONFIG.PERIOD_DAYS;
    const numericPortfolioValue = Number(portfolioValue) || 0;
    const canUsePortfolioValue = numericPortfolioValue > 0;
    const canReusePreviousBudget = cycleExpired && Number(botData.putBudgetForCycle) > 0;
    if (!canUsePortfolioValue && !canReusePreviousBudget) {
      const skipReason = options.skipReason ? ` (${options.skipReason})` : '';
      console.log(`📋 Put cycle ${noCycle ? 'start' : 'reset'} skipped: no positive insured portfolio value available${skipReason}`);
      return false;
    }

    // Calculate new cycle budget from current portfolio value, or reuse the previous
    // budget if live account reads are temporarily unavailable. Avoid leaving cycles
    // stuck at "ended" because one collateral fetch returned empty.
    const newBudget = canUsePortfolioValue
      ? numericPortfolioValue * PUT_ANNUAL_RATE / cyclesPerYear
      : Number(botData.putBudgetForCycle);

    botData.putCycleStart = now;
    botData.putBudgetForCycle = newBudget;
    botData.putNetBought = 0;
    persistCycleState();

    const insuredBasisNote = PUT_INSURED_EXTERNAL_ETH > 0
      ? ` | +${PUT_INSURED_EXTERNAL_ETH.toFixed(4)} external ETH insured`
      : '';
    const basisText = canUsePortfolioValue
      ? `$${numericPortfolioValue.toFixed(0)} insured base`
      : `previous $${newBudget.toFixed(2)} cycle budget fallback`;
    const sourceText = options.source ? ` via ${options.source}` : '';
    console.log(`📋 Put cycle ${noCycle ? 'started' : 'reset'}: $${newBudget.toFixed(2)} budget (${(PUT_ANNUAL_RATE * 100).toFixed(2)}% of ${basisText} / ${cyclesPerYear.toFixed(1)} cycles/yr)${botData.putUnspentBuyLimit > 0 ? ` + $${botData.putUnspentBuyLimit.toFixed(2)} rollover` : ''}${insuredBasisNote}${sourceText}`);
    return true;
  }
  return false;
};

const getAmountStep = (opt) =>
  Number(opt?.options?.amount_step) || Number(opt?.amount_step) || DEFAULT_AMOUNT_STEP;

const floorOrderAmountToVenuePrecision = (amount) => {
  const numeric = Number(amount);
  if (!(numeric > 0)) return 0;
  const scale = 10 ** VENUE_AMOUNT_DECIMALS;
  return Math.floor((numeric + 1e-12) * scale) / scale;
};

const formatVenueOrderAmount = (amount) =>
  floorOrderAmountToVenuePrecision(amount).toFixed(VENUE_AMOUNT_DECIMALS);

const isVenueOrderAmountTradable = (amount) =>
  floorOrderAmountToVenuePrecision(amount) > VENUE_MIN_ORDER_AMOUNT;

const quantizeDown = (x, step) => {
  if (!Number.isFinite(x) || !Number.isFinite(step) || step <= 0) return 0;
  return Math.max(0, Math.floor(x / step) * step);
};

// Extract the first complete JSON object from a string using balanced braces.
// The greedy regex /\{[\s\S]*\}/ matches from first { to LAST }, which captures
// garbage if the LLM outputs text between JSON blocks. This counts braces instead.
const extractJSON = (text) => {
  if (!text || typeof text !== 'string') return null;
  const tryParseBalancedObject = (source) => {
    if (!source || typeof source !== 'string') return null;
    for (let start = source.indexOf('{'); start !== -1; start = source.indexOf('{', start + 1)) {
      let depth = 0;
      let inString = false;
      let escape = false;
      for (let i = start; i < source.length; i++) {
        const ch = source[i];
        if (escape) { escape = false; continue; }
        if (ch === '\\' && inString) { escape = true; continue; }
        if (ch === '"') { inString = !inString; continue; }
        if (inString) continue;
        if (ch === '{') depth++;
        else if (ch === '}') {
          depth--;
          if (depth === 0) {
            try { return JSON.parse(source.slice(start, i + 1)); }
            catch { break; }
          }
        }
      }
    }
    return null;
  };

  const trimmed = text.trim();
  const fencedBlocks = [...trimmed.matchAll(/```(?:json)?\s*([\s\S]*?)```/gi)]
    .map(match => match[1]?.trim())
    .filter(Boolean);
  const candidates = [
    ...fencedBlocks,
    trimmed.replace(/```(?:json)?/gi, '').replace(/```/g, '').trim(),
    trimmed,
  ];

  for (const candidate of candidates) {
    const parsed = tryParseBalancedObject(candidate);
    if (parsed) return parsed;
  }
  return null;
};

const extractConfirmationVote = (text) => {
  const parsed = extractJSON(text);
  if (parsed && typeof parsed.confirm === 'boolean') return parsed;

  if (!text || typeof text !== 'string') return null;
  const cleaned = text
    .replace(/```json/gi, '')
    .replace(/```/g, '')
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .trim();

  const confirmMatch = cleaned.match(/"confirm"\s*:\s*(true|false)/i) || cleaned.match(/\bconfirm\b[^a-z]{0,10}(true|false)/i);
  const orderTypeMatch = cleaned.match(/"order_type"\s*:\s*"(ioc|gtc|post_only)"/i);
  const nullOrderTypeMatch = cleaned.match(/"order_type"\s*:\s*null/i);
  const limitPriceMatch = cleaned.match(/"limit_price"\s*:\s*(null|-?\d+(?:\.\d+)?)/i);
  const reasoningMatch = cleaned.match(/"reasoning"\s*:\s*"([\s\S]*?)"\s*(?:[,}]|$)/i);

  let confirm = null;
  if (confirmMatch) {
    confirm = confirmMatch[1].toLowerCase() === 'true';
  } else if (/\breject(?:ed)?\b/i.test(cleaned) && !/\bconfirm(?:ed)?\b/i.test(cleaned)) {
    confirm = false;
  } else if (/\bconfirm(?:ed)?\b/i.test(cleaned) && !/\breject(?:ed)?\b/i.test(cleaned)) {
    confirm = true;
  }

  if (typeof confirm !== 'boolean') return null;

  const vote = {
    confirm,
    order_type: orderTypeMatch ? orderTypeMatch[1] : (nullOrderTypeMatch ? null : null),
    limit_price: null,
    reasoning: reasoningMatch ? reasoningMatch[1] : cleaned.slice(0, 500),
  };

  if (limitPriceMatch) {
    vote.limit_price = limitPriceMatch[1].toLowerCase() === 'null' ? null : Number(limitPriceMatch[1]);
  }

  return vote;
};

// Load existing data from SQLite
const loadData = () => {
  if (!db) {
    console.log('⚠️ No SQLite connection - starting with default data');
    return;
  }

  // One-time migration from JSON → SQLite
  try { db.migrateFromJson(BOT_DATA_PATH); } catch (_e) { /* logged inside */ }

  try {
    const state = db.loadBotState();
    if (state) {
      botData.putCycleStart = state.put_cycle_start;
      botData.putNetBought = state.put_net_bought;
      botData.putUnspentBuyLimit = state.put_unspent_buy_limit;
      botData.putBudgetForCycle = state.put_budget_for_cycle || 0;
      botData.lastCheck = state.last_check || 0;
      botData.lastJournalGeneration = state.last_journal_generation || 0;
      botData.lastWikiLintRun = state.last_wiki_lint_run || 0;
      botData.lastTradeReviewRun = state.last_trade_review_run || 0;
      botData.lastTradeReviewSuccess = state.last_trade_review_success || 0;
      botData.lastTradeReviewReadyCount = state.last_trade_review_ready_count || 0;
      botData.lastTradeReviewError = state.last_trade_review_error || null;
      try { botData.lastTradeReviewTargets = state.last_trade_review_targets ? JSON.parse(state.last_trade_review_targets) : []; } catch { botData.lastTradeReviewTargets = []; }
      botData.lastHypothesisLessonReviewId = state.last_hypothesis_lesson_review_id || 0;
      botData.lastTradeLessonReviewId = state.last_trade_lesson_review_id || 0;
      botData.lastTradeLessonRun = state.last_trade_lesson_run || 0;
      botData.lastTradeLessonSuccess = state.last_trade_lesson_success || 0;
      botData.lastTradeLessonError = state.last_trade_lesson_error || null;
      botData.lastAdvisoryRun = state.last_advisory_run || 0;
      botData.lastAdvisorySuccess = state.last_advisory_success || 0;
      botData.lastAdvisoryError = state.last_advisory_error || null;
      botData.advisoryRetryCount = state.advisory_retry_count || 0;
      botData.nextAdvisoryRetryAt = state.next_advisory_retry_at || 0;
      botData.lastAdvisorySpotPrice = state.last_advisory_spot_price || null;
      botData.lastAdvisoryTimestamp = state.last_advisory_timestamp || 0;
      console.log(`✅ Loaded cycle state from SQLite`);
    }
  } catch (e) {
    console.error('❌ Error loading from SQLite:', e.message);
    console.log('⚠️ Starting with default data due to load error');
  }
};


// analyzePastOptionsData — removed (replaced by LLM-driven advisory)

// ADX-gated momentum with proper OHLC resampling.
const getTrueTimeBasedMomentum = (
  priceHistory,
  {
    intervalInMinutes = 10,
    adxPeriod = 50,
    adxMin = 15,
    macd = { fast: 16, slow: 34, signal: 13 },
    minBars = 60,
  } = {}
) => {
  if (!Array.isArray(priceHistory) || priceHistory.length < 2) {
    console.log(`Medium-term momentum: NEUTRAL | insufficient price history (${priceHistory?.length || 0} points)`);
    return { main: 'neutral', derivative: null };
  }

  // ---- 1) Build fixed-interval OHLC candles ----
  const ms = intervalInMinutes * 60 * 1000;
  const toMs = (t) => (typeof t === 'number' ? t : new Date(t).getTime());

  // Ensure chronological order
  const series = priceHistory
    .map(p => ({ price: Number(p.price), ts: toMs(p.timestamp) }))
    .filter(p => Number.isFinite(p.price) && Number.isFinite(p.ts))
    .sort((a, b) => a.ts - b.ts);

  if (series.length < 2) {
    console.log(`Medium-term momentum: NEUTRAL | insufficient valid price data (${series.length} points)`);
    return { main: 'neutral', derivative: null };
  }

  const t0 = Math.floor(series[0].ts / ms) * ms;
  const t1 = Math.floor(series[series.length - 1].ts / ms) * ms;

  let i = 0;
  let lastClose = series[0].price;

  const opens = [];
  const highs = [];
  const lows  = [];
  const closes= [];
  const times = [];

  for (let t = t0; t <= t1; t += ms) {
    const bucketEnd = t + ms;

    // Collect ticks in [t, t+ms)
    let bucketPrices = [];
    let opened = null;

    while (i < series.length) {
      const { price, ts } = series[i];
      if (ts >= t && ts < bucketEnd) {
        if (opened === null) opened = price;
        bucketPrices.push(price);
        i++;
      } else if (ts >= bucketEnd) break;
      else i++;
    }

    if (bucketPrices.length > 0) {
      const o = opened;
      const h = Math.max(...bucketPrices);
      const l = Math.min(...bucketPrices);
      const c = bucketPrices[bucketPrices.length - 1];

      opens.push(o); highs.push(h); lows.push(l); closes.push(c); times.push(t);
      lastClose = c;
    } else {
      // Gap: synthesize a doji using lastClose to keep indicators aligned
      opens.push(lastClose);
      highs.push(lastClose);
      lows.push(lastClose);
      closes.push(lastClose);
      times.push(t);
    }
  }

  // Need enough data for ADX & MACD
  const requiredBars = Math.max(minBars, adxPeriod * 3, macd.slow + macd.signal + 5);
  if (closes.length < requiredBars) {
    console.log(`Medium-term momentum: NEUTRAL | insufficient bars for analysis (${closes.length} bars, need ${requiredBars})`);
    return { main: 'neutral', derivative: null };
  }

  // ---- 2) ADX (trend-strength gate) on OHLC ----
  const adxSeries = ADX.calculate({
    high: highs,
    low: lows,
    close: closes,
    period: adxPeriod,
  });
  if (!adxSeries?.length) {
    console.log(`Medium-term momentum: NEUTRAL | ADX calculation failed (no ADX series)`);
    return { main: 'neutral', derivative: null };
  }

  const lastADX = adxSeries[adxSeries.length - 1].adx ?? 0;
  if (!(lastADX > adxMin)) {
    console.log(`Medium-term momentum: NEUTRAL | ADX too weak (${lastADX.toFixed(1)} < ${adxMin})`);
    return { main: 'neutral', derivative: null };
  }

  // ---- 3) Direction via MACD on CLOSES ----
  const macdSeries = MACD.calculate({
    values: closes,
    fastPeriod: macd.fast,
    slowPeriod: macd.slow,
    signalPeriod: macd.signal,
    SimpleMAOscillator: false,
    SimpleMASignal: false,
  });
  if (!macdSeries?.length) {
    console.log(`Medium-term momentum: NEUTRAL | MACD calculation failed (no MACD series)`);
    return { main: 'neutral', derivative: null };
  }

  const { MACD: m, signal: s } = macdSeries[macdSeries.length - 1];
  if (!Number.isFinite(m) || !Number.isFinite(s)) {
    console.log(`Medium-term momentum: NEUTRAL | invalid MACD values (MACD: ${m}, Signal: ${s})`);
    return { main: 'neutral', derivative: null };
  }

  // Calculate the difference for clearer signals
  const macdDiff = m - s;
  
  // Determine momentum based on MACD vs Signal relationship and their signs
  if (m > 0 && s > 0) {
    // Both positive - upward trend
    if (macdDiff > 0) {
      console.log(`Medium-term momentum: UPWARD ACCELERATING | ADX=${lastADX.toFixed(1)}, MACD diff=${macdDiff.toFixed(6)}`);
      return { main: 'upward', derivative: 'accelerating' };
    } else {
      console.log(`Medium-term momentum: UPWARD DECELERATING | ADX=${lastADX.toFixed(1)}, MACD diff=${macdDiff.toFixed(6)}`);
      return { main: 'upward', derivative: 'decelerating' };
    }
  } else if (m < 0 && s < 0) {
    // Both negative - downward trend
    if (macdDiff < 0) {
      console.log(`Medium-term momentum: DOWNWARD ACCELERATING | ADX=${lastADX.toFixed(1)}, MACD diff=${macdDiff.toFixed(6)}`);
      return { main: 'downward', derivative: 'accelerating' };
    } else {
      console.log(`Medium-term momentum: DOWNWARD DECELERATING | ADX=${lastADX.toFixed(1)}, MACD diff=${macdDiff.toFixed(6)}`);
      return { main: 'downward', derivative: 'decelerating' };
    }
  }
  
  // Trend continuation logic removed
  
  // Everything else is neutral
  console.log(`Medium-term momentum: NEUTRAL | ADX=${lastADX.toFixed(1)}, MACD diff=${macdDiff.toFixed(6)}`);
  return { main: 'neutral', derivative: null };
};

// Helper function to check for spikes in the new format
const hasSpike = (derivative, spikeType) => {
  if (!derivative || !derivative.includes('_with_spikes(')) return false;
  const spikeMatch = derivative.match(/_with_spikes\(([^)]+)\)/);
  if (!spikeMatch) return false;
  const spikes = spikeMatch[1].split(',');
  return spikes.includes(spikeType);
};

// Helper function to filter valid options by delta range
const filterValidOptions = (options, minDelta, maxDelta) => {
  return options.filter(option => {
    if (!option?.details?.delta) return false;
    const delta = parseFloat(option.details.delta);
    return !isNaN(delta) && delta >= minDelta && delta <= maxDelta;
  });
};

// Helper function for consistent option summary logging
const logOptionSummary = (type, count, reason) => {
  console.log(`🎯 Found ${count} ${type} options ${reason}`);
};

// Helper function for consistent entry logging
const logEntryDecision = (type, count, reason) => {
  console.log(`🎯 ${type} ENTRY: ${reason} - ${count} qualified options found`);
};

// Helper function to check for steep momentum with downward spike (for entry conditions)
const hasSteepWithDownwardSpike = (derivative) => {
  if (!derivative || !derivative.startsWith('steep')) return false;
  return hasSpike(derivative, '1h_down') || hasSpike(derivative, '1d_down') || hasSpike(derivative, '3d_down');
};

// Utility function to extract momentum values consistently
const extractMomentumValues = (mediumTermMomentum, shortTermMomentum) => {
  const mainMomentum = typeof mediumTermMomentum === 'object' ? mediumTermMomentum.main : mediumTermMomentum;
  const shortMainMomentum = typeof shortTermMomentum === 'object' ? shortTermMomentum.main : shortTermMomentum;
  const shortDerivative = typeof shortTermMomentum === 'object' ? shortTermMomentum.derivative : null;
  
  return { mainMomentum, shortMainMomentum, shortDerivative };
};

// shouldEnterStandard — removed (replaced by LLM-driven advisory)

// ===== ONCHAIN ANALYSIS FUNCTIONS =====

// DEX Liquidity Analysis
// Load historical liquidity data from SQLite
const loadHistoricalLiquidity = () => {
  if (!db) return [];
  try {
    const since = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
    const rows = db.getRecentOnchain(since);
    return rows.map(row => {
      try {
        const raw = typeof row.raw_data === 'string' ? JSON.parse(row.raw_data) : row.raw_data;
        return raw?.dexLiquidity || null;
      } catch { return null; }
    }).filter(Boolean);
  } catch (error) {
    console.log('⚠️ Failed to load historical liquidity data:', error.message);
    return [];
  }
};

const isTemporaryV4AggregateSample = (dexName, dex) => {
  const poolCount = Number(dex?.pools);
  return dexName === 'uniswap_v4' && Number.isFinite(poolCount) && poolCount > 1;
};

const findLastValidDexSample = (historicalData, dexName) => {
  for (const entry of historicalData || []) {
    const dex = entry?.dexes?.[dexName];
    if (!dex || dex.error || isTemporaryV4AggregateSample(dexName, dex)) continue;
    const totalLiquidity = Number(dex.totalLiquidity);
    if (Number.isFinite(totalLiquidity) && totalLiquidity > 0) {
      return {
        ...dex,
        stale: true,
        staleReason: 'subgraph_unavailable',
      };
    }
  }
  return null;
};

// Calculate liquidity flow direction
const calculateLiquidityFlow = (currentData, historicalData) => {
  if (!historicalData || historicalData.length < 2) {
    return { 
      direction: 'unknown', 
      magnitude: 0, 
      confidence: 0,
      timeframes: {
        hourly: { direction: 'unknown', change: 0 },
        daily: { direction: 'unknown', change: 0 },
        weekly: { direction: 'unknown', change: 0 }
      }
    };
  }
  
  const isComparableDex = (dexName, dex) => {
    if (isTemporaryV4AggregateSample(dexName, dex)) return false;
    if (dex?.error) return false;
    const totalLiquidity = Number(dex?.totalLiquidity);
    return Number.isFinite(totalLiquidity) && totalLiquidity > 0;
  };

  const currentDexNames = new Set(
    Object.entries(currentData.dexes || {})
      .filter(([dexName, dex]) => isComparableDex(dexName, dex))
      .map(([dexName]) => dexName)
  );

  // Calculate total liquidity for each time period using the current valid DEX set.
  const calculateTotalLiquidity = (data) => {
    let total = 0;
    let hasValidData = false;
    let hasFailedDexes = false;
    if (data.dexes) {
      Object.entries(data.dexes).forEach(([dexName, dex]) => {
        if (!currentDexNames.has(dexName)) return;
        if (isTemporaryV4AggregateSample(dexName, dex)) return;
        // Skip DEXes that failed to load (have error property)
        if (dex.error) {
          hasFailedDexes = true;
          return;
        }
        if (dex.totalLiquidity && !isNaN(dex.totalLiquidity)) {
          total += dex.totalLiquidity;
          hasValidData = true;
        }
      });
    }
    // Return null if no valid data was found (all DEXes failed)
    // Also mark as unreliable if any major DEX failed (incomplete data)
    return hasValidData ? { total, hasFailedDexes } : null;
  };
  
  const currentTotalResult = calculateTotalLiquidity(currentData);
  if (currentTotalResult === null || currentTotalResult.total === 0) {
    return { 
      direction: 'unknown', 
      magnitude: 0, 
      confidence: 0,
      timeframes: {
        hourly: { direction: 'unknown', change: 0 },
        daily: { direction: 'unknown', change: 0 },
        weekly: { direction: 'unknown', change: 0 }
      },
      dataReliability: currentTotalResult === null ? 'unreliable' : 'reliable'
    };
  }
  
  const currentTotal = currentTotalResult.total;
  const hasFailedDexes = currentTotalResult.hasFailedDexes;
  
  // Get data from different timeframes
  const oneHourAgo = Date.now() - (60 * 60 * 1000);
  const oneDayAgo = Date.now() - (24 * 60 * 60 * 1000);
  const oneWeekAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
  
  // Find closest entries for each timeframe
  const findClosestEntry = (targetTime) => {
    return historicalData.reduce((closest, entry) => {
      const entryTime = new Date(entry.timestamp).getTime();
      const closestTime = closest ? new Date(closest.timestamp).getTime() : Infinity;
      const targetDiff = Math.abs(entryTime - targetTime);
      const closestDiff = Math.abs(closestTime - targetTime);
      return targetDiff < closestDiff ? entry : closest;
    }, null);
  };
  
  const hourlyEntry = findClosestEntry(oneHourAgo);
  const dailyEntry = findClosestEntry(oneDayAgo);
  const weeklyEntry = findClosestEntry(oneWeekAgo);
  
  // Calculate changes for each timeframe
  const calculateChange = (current, historical) => {
    if (!historical || historical === 0) return 0;
    return (current - historical) / historical;
  };
  
  const hourlyChange = hourlyEntry ? calculateChange(currentTotal, calculateTotalLiquidity(hourlyEntry)?.total || 0) : 0;
  const dailyChange = dailyEntry ? calculateChange(currentTotal, calculateTotalLiquidity(dailyEntry)?.total || 0) : 0;
  const weeklyChange = weeklyEntry ? calculateChange(currentTotal, calculateTotalLiquidity(weeklyEntry)?.total || 0) : 0;
  
  // Determine overall direction based on weighted analysis
  // Weekly trends get highest weight, then daily, then hourly
  const weeklyWeight = 0.5;
  const dailyWeight = 0.3;
  const hourlyWeight = 0.2;
  
  const weightedChange = (weeklyChange * weeklyWeight) + (dailyChange * dailyWeight) + (hourlyChange * hourlyWeight);
  
  let direction = 'stable';
  let magnitude = Math.abs(weightedChange);
  let confidence = 0.5;
  
  if (weightedChange > 0.01) {
    direction = 'inflow';
    confidence = Math.min(0.95, 0.5 + Math.abs(weightedChange) * 20);
  } else if (weightedChange < -0.01) {
    direction = 'outflow';
    confidence = Math.min(0.95, 0.5 + Math.abs(weightedChange) * 20);
  }
  
  // Determine timeframe-specific directions
  const getDirection = (change) => {
    if (change > 0.005) return 'inflow';
    if (change < -0.005) return 'outflow';
    return 'stable';
  };
  
  return {
    direction,
    magnitude,
    confidence,
    weightedChange,
    currentTotal,
    dataReliability: hasFailedDexes ? 'unreliable' : 'reliable',
    timeframes: {
      hourly: { 
        direction: getDirection(hourlyChange), 
        change: hourlyChange,
        total: hourlyEntry ? (calculateTotalLiquidity(hourlyEntry)?.total || 0) : 0
      },
      daily: { 
        direction: getDirection(dailyChange), 
        change: dailyChange,
        total: dailyEntry ? (calculateTotalLiquidity(dailyEntry)?.total || 0) : 0
      },
      weekly: { 
        direction: getDirection(weeklyChange), 
        change: weeklyChange,
        total: weeklyEntry ? (calculateTotalLiquidity(weeklyEntry)?.total || 0) : 0
      }
    }
  };
};

const analyzeDEXLiquidity = async (spotPrice) => {
  try {
    const liquidityData = {
      timestamp: new Date().toISOString(),
      spotPrice: spotPrice,
      dexes: {}
    };
    const historicalData = loadHistoricalLiquidity();

    // Uniswap V3 analysis - specific pools only
    try {
      const uniswapQuery = {
        query: `
                  query {
                    pools(where: {id_in: [
                      "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                      "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"
                    ]}) {
                      id
                      totalValueLockedUSD
                      volumeUSD
                      feeTier
                      liquidity
                      txCount
                      totalValueLockedToken0
                      totalValueLockedToken1
                      token0 {
                        symbol
                        id
                      }
                      token1 {
                        symbol
                        id
                      }
                    }
                  }
        `
      };
      
      const response = await axios.post(DEX_APIS.UNISWAP_V3, uniswapQuery, { timeout: 15000 });
      assertGraphQLSuccess(response);
      if (response.data?.data?.pools && Array.isArray(response.data.data.pools)) {
        // For Uniswap V3, calculate both TVL and in-range liquidity
        const poolCount = response.data.data.pools.length;
        if (poolCount === 0) {
          console.log('⚠️ No V3 pools found with provided IDs - they may not be indexed in the subgraph');
        }
        // Calculate TVL in USD terms (consistent across all DEXes)
        const totalTVLUSD = response.data.data.pools.reduce((sum, pool) => {
          const tvlUsd = parseFloat(pool.totalValueLockedUSD);
          return isNaN(tvlUsd) ? sum : sum + tvlUsd;
        }, 0);
        
        const totalVolume = response.data.data.pools.reduce((sum, pool) => {
          const v = parseFloat(pool.volumeUSD);
          return isNaN(v) ? sum : sum + v;
        }, 0);
        const totalTxCount = response.data.data.pools.reduce((sum, pool) => {
          const t = parseInt(pool.txCount);
          return isNaN(t) ? sum : sum + t;
        }, 0);

        liquidityData.dexes.uniswap_v3 = {
          pools: response.data.data.pools.length,
          totalLiquidity: totalTVLUSD, // TVL in USD terms (consistent)
          totalVolume,
          totalTxCount,
          poolDetails: response.data.data.pools.map(pool => {
            const tvlUsd = parseFloat(pool.totalValueLockedUSD);

            return {
              id: pool.id,
              liquidity: tvlUsd, // Store in USD terms (consistent)
              liquidityUSD: tvlUsd, // Keep USD for reference
              volumeUSD: parseFloat(pool.volumeUSD) || 0,
              feeTier: parseInt(pool.feeTier) || 0,
              activeLiquidity: pool.liquidity || '0',
              txCount: parseInt(pool.txCount) || 0,
              tvlToken0: parseFloat(pool.totalValueLockedToken0) || 0,
              tvlToken1: parseFloat(pool.totalValueLockedToken1) || 0,
              token0: pool.token0,
              token1: pool.token1
            };
          })
        };
      }
    } catch (error) {
      console.log('⚠️ Uniswap V3 liquidity analysis failed:', error.message);
      if (error.response) {
        console.log('⚠️ V3 API Response:', error.response.status, error.response.data);
      }
      liquidityData.dexes.uniswap_v3 = { error: error.message };
    }

    // Uniswap V4 analysis - specific pools only
    try {
      const uniswapV4Query = {
        query: `
                  query {
                    pools(where: {id_in: [
                      "0x72331fcb696b0151904c03584b66dc8365bc63f8a144d89a773384e3a579ca73"
                    ]}) {
                      id
                      totalValueLockedUSD
                      volumeUSD
                      feeTier
                      liquidity
                      txCount
                      totalValueLockedToken0
                      totalValueLockedToken1
                      token0 {
                        symbol
                        id
                      }
                      token1 {
                        symbol
                        id
                      }
                    }
                  }
        `
      };
      
      const response = await axios.post(DEX_APIS.UNISWAP_V4, uniswapV4Query, { timeout: 10000 });
      assertGraphQLSuccess(response);
      if (response.data?.data?.pools && Array.isArray(response.data.data.pools)) {
        // For Uniswap V4, use USD TVL directly (consistent with V3)
        const totalTVLUSD = response.data.data.pools.reduce((sum, pool) => {
          const tvlUsd = parseFloat(pool.totalValueLockedUSD);
          return isNaN(tvlUsd) ? sum : sum + tvlUsd;
        }, 0);
        
        const totalVolumeV4 = response.data.data.pools.reduce((sum, pool) => {
          const v = parseFloat(pool.volumeUSD);
          return isNaN(v) ? sum : sum + v;
        }, 0);
        const totalTxCountV4 = response.data.data.pools.reduce((sum, pool) => {
          const t = parseInt(pool.txCount);
          return isNaN(t) ? sum : sum + t;
        }, 0);

        liquidityData.dexes.uniswap_v4 = {
          pools: response.data.data.pools.length,
          totalLiquidity: totalTVLUSD, // TVL in USD terms (consistent with V3)
          totalVolume: totalVolumeV4,
          totalTxCount: totalTxCountV4,
          poolDetails: response.data.data.pools.map(pool => {
            const tvlUsd = parseFloat(pool.totalValueLockedUSD);

            return {
              id: pool.id,
              liquidity: tvlUsd, // Store in USD terms (consistent)
              liquidityUSD: tvlUsd, // Keep USD for reference
              volumeUSD: parseFloat(pool.volumeUSD) || 0,
              feeTier: parseInt(pool.feeTier) || 0,
              activeLiquidity: pool.liquidity || '0',
              txCount: parseInt(pool.txCount) || 0,
              tvlToken0: parseFloat(pool.totalValueLockedToken0) || 0,
              tvlToken1: parseFloat(pool.totalValueLockedToken1) || 0,
              token0: pool.token0,
              token1: pool.token1
            };
          })
        };
      }
    } catch (error) {
      const fallback = findLastValidDexSample(historicalData, 'uniswap_v4');
      if (fallback) {
        console.log('⚠️ Uniswap V4 subgraph unavailable; using last valid tracked-pool sample');
        liquidityData.dexes.uniswap_v4 = fallback;
      } else {
        console.log('⚠️ Uniswap V4 liquidity analysis failed:', error.message);
        if (error.response) {
          console.log('⚠️ V4 API Response:', error.response.status, error.response.data);
        }
        liquidityData.dexes.uniswap_v4 = { error: error.message };
      }
    }

    // Calculate liquidity flow direction
    const flowAnalysis = calculateLiquidityFlow(liquidityData, historicalData);
    liquidityData.flowAnalysis = flowAnalysis;

    return liquidityData;
  } catch (error) {
    console.log('⚠️ DEX liquidity analysis failed:', error.message);
    return { error: error.message, timestamp: new Date().toISOString() };
  }
};

// hasDowntrendWith7DayDownwardSpikeAndShortTermDowntrend — removed (replaced by LLM-driven advisory)

// Short-term momentum detection (trend continuation logic removed)
const getShortTermMomentum = (priceHistory) => {
  const currentTime = Date.now();
  const fifteenMinutesAgo = currentTime - (15 * TIME_CONSTANTS.MINUTE);
  const thirtyMinutesAgo = currentTime - (30 * TIME_CONSTANTS.MINUTE);
  const oneHourAgo = currentTime - TIME_CONSTANTS.HOUR;
  const twoHoursAgo = currentTime - (2 * TIME_CONSTANTS.HOUR);
  const oneDayAgo = currentTime - TIME_CONSTANTS.DAY;
  const twoDaysAgo = currentTime - (2 * TIME_CONSTANTS.DAY);
  const threeDaysAgo = currentTime - TIME_CONSTANTS.THREE_DAYS;
  const sevenDaysAgo = currentTime - TIME_CONSTANTS.WEEK;

  // Get prices for different timeframes
  const last15MinPrices = priceHistory.filter(p => p.timestamp >= fifteenMinutesAgo);
  const prev15MinPrices = priceHistory.filter(p => p.timestamp >= thirtyMinutesAgo && p.timestamp < fifteenMinutesAgo);
  const last1HourPrices = priceHistory.filter(p => p.timestamp >= oneHourAgo);
  const prev1HourPrices = priceHistory.filter(p => p.timestamp >= twoHoursAgo && p.timestamp < oneHourAgo);
  const last1DayPrices = priceHistory.filter(p => p.timestamp >= oneDayAgo);
  const prev1DayPrices = priceHistory.filter(p => p.timestamp >= twoDaysAgo && p.timestamp < oneDayAgo);
  
  // Get prices for spike detection
  const last1HourPricesForSpike = priceHistory.filter(p => p.timestamp >= oneHourAgo);
  const last1DayPricesForSpike = priceHistory.filter(p => p.timestamp >= oneDayAgo);
  const last3DaysPricesForSpike = priceHistory.filter(p => p.timestamp >= threeDaysAgo);
  const last7DaysPricesForSpike = priceHistory.filter(p => p.timestamp >= sevenDaysAgo);

  let shortTermMomentum = 'neutral';
  let derivative = '';
  let momentumDetails = [];
  let threeDayHigh = 0;
  let threeDayLow = Infinity;
  let sevenDayHigh = 0;
  let sevenDayLow = Infinity;

  if (last15MinPrices.length > 0 && prev15MinPrices.length > 0) {
    // 15-minute averages
    const last15Avg = last15MinPrices.reduce((sum, p) => sum + p.price, 0) / last15MinPrices.length;
    const prev15Avg = prev15MinPrices.reduce((sum, p) => sum + p.price, 0) / prev15MinPrices.length;
    const momentumChange15min = ((last15Avg - prev15Avg) / prev15Avg) * 100;
    
    // 1-hour averages
    let last1HourAvg = 0, prev1HourAvg = 0, momentumChange1Hour = 0;
    if (last1HourPrices.length > 0 && prev1HourPrices.length > 0) {
      last1HourAvg = last1HourPrices.reduce((sum, p) => sum + p.price, 0) / last1HourPrices.length;
      prev1HourAvg = prev1HourPrices.reduce((sum, p) => sum + p.price, 0) / prev1HourPrices.length;
      momentumChange1Hour = ((last1HourAvg - prev1HourAvg) / prev1HourAvg) * 100;
    }
    
    // 1-day averages
    let last1DayAvg = 0, prev1DayAvg = 0, momentumChange1Day = 0;
    if (last1DayPrices.length > 0 && prev1DayPrices.length > 0) {
      last1DayAvg = last1DayPrices.reduce((sum, p) => sum + p.price, 0) / last1DayPrices.length;
      prev1DayAvg = prev1DayPrices.reduce((sum, p) => sum + p.price, 0) / prev1DayPrices.length;
      momentumChange1Day = ((last1DayAvg - prev1DayAvg) / prev1DayAvg) * 100;
    }
    
    // Determine Flat/Moving/Slanted/Steep based on 15-minute data comparison
    const percentChange = Math.abs(momentumChange15min);
    if (percentChange <= 0.2) {
      derivative = 'flat';
    } else if (percentChange <= 0.8) {
      derivative = 'moving';
    } else if (percentChange <= 1.6) {
      derivative = 'slanted';
    } else {
      derivative = 'steep';
    }
    
    // Determine short-term momentum based on weighted average of all timeframes
    const momentumChange = ((last15Avg - prev15Avg) / prev15Avg) * 100;
    let currentMomentum = 'neutral';
    if (Math.abs(momentumChange) < 0.1) { 
      currentMomentum = 'neutral';
    } else if (last15Avg > prev15Avg) {
      currentMomentum = 'upward';
    } else if (last15Avg < prev15Avg) {
      currentMomentum = 'downward';
    }
    
    // Trend continuation logic removed - use current momentum directly
    shortTermMomentum = currentMomentum;
    
    // Spike detection across multiple timeframes - comparing timeframe extremes
    const spikes = [];
    
    // Get 10-minute and 30-minute data for spike detection
    const tenMinutesAgo = currentTime - (10 * TIME_CONSTANTS.MINUTE);
    const last10MinPrices = priceHistory.filter(p => p.timestamp >= tenMinutesAgo);
    const last30MinPrices = priceHistory.filter(p => p.timestamp >= thirtyMinutesAgo);
    
    // 1-hour spike check: compare 1h low/high (excluding last 10min) with 10m low/high
    if (last1HourPricesForSpike.length > 0 && last10MinPrices.length > 0) {
      // Exclude the last 10 minutes from 1-hour data to avoid self-comparison
      const oneHourExcludingLast10Min = last1HourPricesForSpike.filter(p => p.timestamp < tenMinutesAgo);
      
      if (oneHourExcludingLast10Min.length > 0) {
        const oneHourHigh = Math.max(...oneHourExcludingLast10Min.map(p => p.price));
        const oneHourLow = Math.min(...oneHourExcludingLast10Min.map(p => p.price));
        const tenMinHigh = Math.max(...last10MinPrices.map(p => p.price));
        const tenMinLow = Math.min(...last10MinPrices.map(p => p.price));
        
        // Check for breakout with small tolerance to avoid false signals
        const tolerance = 0.001; // 0.1% tolerance
        if (tenMinHigh > oneHourHigh * (1 + tolerance)) {
          spikes.push('1h_up');
        } else if (tenMinLow < oneHourLow * (1 - tolerance)) {
          spikes.push('1h_down');
        }
      }
    }
    
    // 1-day spike check: compare 1d low/high (excluding last 30min) with 30m low/high
    if (last1DayPricesForSpike.length > 0 && last30MinPrices.length > 0) {
      // Exclude the last 30 minutes from 1-day data to avoid self-comparison
      const oneDayExcludingLast30Min = last1DayPricesForSpike.filter(p => p.timestamp < thirtyMinutesAgo);
      
      if (oneDayExcludingLast30Min.length > 0) {
        const oneDayHigh = Math.max(...oneDayExcludingLast30Min.map(p => p.price));
        const oneDayLow = Math.min(...oneDayExcludingLast30Min.map(p => p.price));
        const thirtyMinHigh = Math.max(...last30MinPrices.map(p => p.price));
        const thirtyMinLow = Math.min(...last30MinPrices.map(p => p.price));
        
        const tolerance = 0.001; // 0.1% tolerance
        if (thirtyMinHigh > oneDayHigh * (1 + tolerance)) {
          spikes.push('1d_up');
        } else if (thirtyMinLow < oneDayLow * (1 - tolerance)) {
          spikes.push('1d_down');
        }
      }
    }
    
    // 3-day spike check: compare 3d low/high (excluding last 30min) with 30m low/high
    if (last3DaysPricesForSpike.length > 0 && last30MinPrices.length > 0) {
      // Exclude the last 30 minutes from 3-day data to avoid self-comparison
      const threeDaysExcludingLast30Min = last3DaysPricesForSpike.filter(p => p.timestamp < thirtyMinutesAgo);
      
      if (threeDaysExcludingLast30Min.length > 0) {
        threeDayHigh = Math.max(...threeDaysExcludingLast30Min.map(p => p.price));
        threeDayLow = Math.min(...threeDaysExcludingLast30Min.map(p => p.price));
        const thirtyMinHigh = Math.max(...last30MinPrices.map(p => p.price));
        const thirtyMinLow = Math.min(...last30MinPrices.map(p => p.price));
        
        const tolerance = 0.001; // 0.1% tolerance
        if (thirtyMinHigh > threeDayHigh * (1 + tolerance)) {
          spikes.push('3d_up');
        } else if (thirtyMinLow < threeDayLow * (1 - tolerance)) {
          spikes.push('3d_down');
        }
      }
    }
    
    // 7-day spike check: compare 7d low/high (excluding last 30min) with 30m low/high
    if (last7DaysPricesForSpike.length > 0 && last30MinPrices.length > 0) {
      // Exclude the last 30 minutes from 7-day data to avoid self-comparison
      const sevenDaysExcludingLast30Min = last7DaysPricesForSpike.filter(p => p.timestamp < thirtyMinutesAgo);
      
      if (sevenDaysExcludingLast30Min.length > 0) {
        sevenDayHigh = Math.max(...sevenDaysExcludingLast30Min.map(p => p.price));
        sevenDayLow = Math.min(...sevenDaysExcludingLast30Min.map(p => p.price));
        const thirtyMinHigh = Math.max(...last30MinPrices.map(p => p.price));
        const thirtyMinLow = Math.min(...last30MinPrices.map(p => p.price));
        
        const downwardTolerance = 0.001; // 0.1% tolerance for downward spikes
        const upwardTolerance = 0.01; // 1% buffer for upward spikes to prevent put round-tripping
        
        if (thirtyMinHigh > sevenDayHigh * (1 + upwardTolerance)) {
          spikes.push('7d_up');
        } else if (thirtyMinLow < sevenDayLow * (1 - downwardTolerance)) {
          spikes.push('7d_down');
        }
      }
    }
    // Add spike information to derivative
    if (spikes.length > 0) {
      derivative = derivative + '_with_spikes(' + spikes.join(',') + ')';
    }
    
    // Build momentum details for logging
    momentumDetails.push(`15m: ${momentumChange15min.toFixed(3)}%`);
    if (last1HourPrices.length > 0 && prev1HourPrices.length > 0) {
      momentumDetails.push(`1h: ${momentumChange1Hour.toFixed(3)}%`);
    }
    if (last1DayPrices.length > 0 && prev1DayPrices.length > 0) {
      momentumDetails.push(`1d: ${momentumChange1Day.toFixed(3)}%`);
    }
    
    console.log(`Short-term momentum: ${shortTermMomentum.toUpperCase()}${derivative ? ` ${derivative.toUpperCase()}` : ''} | ${momentumDetails.join(' | ')}`);
  } else {
    console.log(`Short-term momentum: insufficient data (last15min: ${last15MinPrices.length}, prev15min: ${prev15MinPrices.length})`);
  }
  
  return { 
    main: shortTermMomentum, 
    derivative, 
    threeDayHigh, 
    threeDayLow,
    sevenDayHigh, 
    sevenDayLow 
  };
};

const analyzeMomentum = (priceHistory) => {
  if (!priceHistory || priceHistory.length === 0) {
    return { 
      mediumTermMomentum: { main: 'neutral', derivative: null }, 
      shortTermMomentum: { main: 'neutral', derivative: null }
    };
  }
  
  // Get medium-term momentum via ADX-gated MACD on resampled OHLC
  const mediumTermMomentum = getTrueTimeBasedMomentum(priceHistory);
  
  // Get short-term momentum using 30-minute averages
  const shortTermMomentum = getShortTermMomentum(priceHistory);

  return { 
    mediumTermMomentum, 
    shortTermMomentum,
    threeDayHigh: shortTermMomentum.threeDayHigh,
    threeDayLow: shortTermMomentum.threeDayLow,
    sevenDayHigh: shortTermMomentum.sevenDayHigh,
    sevenDayLow: shortTermMomentum.sevenDayLow
  };
};

const determineCheckInterval = (mediumTermMomentum, shortTermMomentum) => {
  // Extract main momentum from the new format
  const mainMomentum = typeof mediumTermMomentum === 'object' ? mediumTermMomentum.main : mediumTermMomentum;
  const shortMainMomentum = typeof shortTermMomentum === 'object' ? shortTermMomentum.main : shortTermMomentum;
  
  // Maximum urgency during downturns (1 minute)
  if (mainMomentum === 'downward' || shortMainMomentum === 'downward') {
    return DYNAMIC_INTERVALS.urgent;
  }

  // Normal interval for other conditions
  return DYNAMIC_INTERVALS.normal;
};

let coinGeckoCooldownUntil = 0;
let coinGeckoCooldownNoticeUntil = 0;
let lastSuccessfulCoinGeckoSpot = null;
const ETH_SPOT_MIN = 100;
const ETH_SPOT_MAX = 20000;

const normalizeEthSpotPrice = (value) => {
  const n = Number(value);
  return Number.isFinite(n) && n >= ETH_SPOT_MIN && n <= ETH_SPOT_MAX ? n : null;
};

const extractTickerSpotPrice = (ticker) => {
  if (!ticker || typeof ticker !== 'object') return null;
  const candidates = [
    ticker.I,
    ticker.index_price,
    ticker.indexPrice,
    ticker.underlying_price,
    ticker.underlyingPrice,
    ticker.spot_price,
    ticker.spotPrice,
    ticker.price,
    ticker.last_price,
    ticker.lastPrice,
    ticker.M,
    ticker.mark_price,
    ticker.markPrice,
  ];
  for (const candidate of candidates) {
    const value = normalizeEthSpotPrice(candidate);
    if (value != null) return value;
  }
  return null;
};

const fetchDeriveSpotPrice = async () => {
  try {
    const response = await axios.post(API_URL.GET_TICKERS, {
      instrument_type: 'perp',
      currency: 'ETH',
    }, { timeout: 5000 });
    const raw = response.data?.result;
    const tickers = Array.isArray(raw)
      ? raw
      : (Array.isArray(raw?.tickers)
        ? raw.tickers
        : (raw?.tickers && typeof raw.tickers === 'object' ? Object.values(raw.tickers) : []));
    const ethPerp = tickers.find(t => t.instrument_name === 'ETH-PERP') || tickers[0];
    return extractTickerSpotPrice(ethPerp);
  } catch (error) {
    const status = error.response?.status;
    console.log(`⚠️ Derive spot fetch failed: ${error.message} | status: ${status || 'N/A'}`);
    return null;
  }
};

// Get current spot price from CoinGecko with cooldown after rate limiting.
const fetchCoinGeckoSpotPrice = async () => {
  const now = Date.now();
  if (coinGeckoCooldownUntil > now) {
    if (coinGeckoCooldownNoticeUntil !== coinGeckoCooldownUntil) {
      console.log(`⏳ CoinGecko cooldown active for ${Math.ceil((coinGeckoCooldownUntil - now) / 1000)}s — skipping spot fetch`);
      coinGeckoCooldownNoticeUntil = coinGeckoCooldownUntil;
    }
    return lastSuccessfulCoinGeckoSpot;
  }

  try {
    const response = await axios.get(`${COINGECKO_API}/simple/price?ids=ethereum&vs_currencies=usd`, { timeout: 5000 });
    const price = Number(response.data?.ethereum?.usd);
    if (price > 0) {
      lastSuccessfulCoinGeckoSpot = price;
      coinGeckoCooldownNoticeUntil = 0;
    }
    return price > 0 ? price : null;
  } catch (error) {
    const status = error.response?.status;
    const retryAfter = error.response?.headers?.['retry-after'];
    const rateLimitRemaining = error.response?.headers?.['x-ratelimit-remaining'];
    if (status === 429) {
      const cooldownMs = Math.max(15000, (Number(retryAfter) || 60) * 1000);
      coinGeckoCooldownUntil = Date.now() + cooldownMs;
      coinGeckoCooldownNoticeUntil = 0;
    }
    console.error(`Error fetching spot price: ${error.message} | status: ${status || 'N/A'} | retry-after: ${retryAfter || 'none'} | ratelimit-remaining: ${rateLimitRemaining ?? 'N/A'}`);
    return lastSuccessfulCoinGeckoSpot;
  }
};

// Fetch ETH funding rate from Derive's own perp ticker (no geo-block, already used by bot)
const fetchFundingRates = async () => {
  try {
    const response = await axios.post(API_URL.GET_TICKERS, {
      instrument_type: 'perp', currency: 'ETH',
    }, { timeout: 5000 });
    if (response.data?.error) return [];
    return fundingRatesFromTickerResult(response.data?.result, new Date().toISOString());
  } catch (e) {
    console.log(`⚠️ Derive funding rate failed: ${e.message}`);
  }
  return [];
};

// Fetch option details
// Fetch all tickers for a given expiry date (batch call — returns AMM prices)
const fetchTickersByExpiry = async (expiryDate, { throwOnError = false } = {}) => {
  try {
    const response = await axios.post(API_URL.GET_TICKERS, {
      instrument_type: 'option',
      currency: 'ETH',
      expiry_date: expiryDate,
    }, throwOnError ? { timeout: 15000 } : undefined);
    if (!response.data.result?.tickers || typeof response.data.result.tickers !== 'object'
      || Array.isArray(response.data.result.tickers)) {
      if (throwOnError) throw new Error(`Ticker response unavailable for ${expiryDate}`);
      console.error(`No tickers found for expiry ${expiryDate}`);
      return {};
    }
    if (throwOnError && (response.data.error || Object.values(response.data.result.tickers).some(ticker =>
      !ticker || typeof ticker !== 'object' || Array.isArray(ticker)))) {
      throw new Error(`Malformed ticker response for ${expiryDate}`);
    }
    const receivedAt = new Date().toISOString();
    return Object.fromEntries(Object.entries(response.data.result.tickers).map(([name, ticker]) => (
      [name, { ...ticker, quote_received_at: receivedAt, quote_source: 'derive-v2/get_tickers' }]
    )));
  } catch (error) {
    const status = error.response?.status;
    console.error(`Error fetching tickers for expiry ${expiryDate}: ${error.message} | status: ${status || 'N/A'}${status === 429 ? ' (RATE LIMITED)' : ''}`);
    if (throwOnError) throw error;
    return {};
  }
};

const getExpiryDateCodeFromInstrumentName = (instrumentName) => {
  const parts = String(instrumentName || '').split('-');
  return /^\d{8}$/.test(parts?.[1] || '') ? parts[1] : null;
};

const fetchFreshTickerForInstrument = async (instrumentName) => {
  const expiryDate = getExpiryDateCodeFromInstrumentName(instrumentName);
  if (!expiryDate) return null;
  const tickers = await fetchTickersByExpiry(expiryDate);
  return tickers?.[instrumentName] || null;
};

// Enrich a candidate instrument using pre-fetched ticker data
const enrichCandidateFromTicker = (instrument, ticker, spotPrice) => {
  if (!ticker) return null;

  const delta = Number(ticker.option_pricing?.d);
  const askPrice = Number(ticker.a);
  const askAmount = Number(ticker.A);
  const bidPrice = Number(ticker.b);
  const bidAmount = Number(ticker.B);
  const markPrice = Number(ticker.M) || null;
  const indexPrice = Number(ticker.I) || spotPrice || null;
  const openInterest = getTickerOpenInterest(ticker);
  const impliedVol = Number(ticker.option_pricing?.i) || null;

  const askDeltaValue = askPrice == 0 ? 0 : Math.abs(delta) / askPrice;
  const bidDeltaValue = bidPrice == 0 ? 0 : bidPrice / Math.abs(delta);
  const expirySeconds = Number(instrument?.option_details?.expiry);
  const dte = computeDteAt(expirySeconds);
  const putEdgeScore = normalizeBuyPutScore(askDeltaValue, dte);
  const callEdgeScore = normalizeSellCallScore(bidDeltaValue, dte);

  return {
    ...instrument,
    details: {
      delta,
      askDeltaValue,
      bidDeltaValue,
      putEdgeScore,
      callEdgeScore,
      dte,
      askPrice,
      askAmount,
      bidPrice,
      bidAmount,
      markPrice,
      indexPrice,
      openInterest,
      impliedVol,
    }
  };
};

const summarizeBestCandidate = (candidate, type) => {
  if (!candidate?.details) return null;
  return {
    score: type === 'put' ? Number(candidate.details.putEdgeScore || 0) : Number(candidate.details.callEdgeScore || 0),
    detail: {
      delta: Number(candidate.details.delta ?? 0),
      price: type === 'put' ? Number(candidate.details.askPrice ?? 0) : Number(candidate.details.bidPrice ?? 0),
      strike: Number(candidate.option_details?.strike ?? 0),
      expiry: Number(candidate.option_details?.expiry ?? 0),
      instrument: candidate.instrument_name || null,
      raw_score: type === 'put'
        ? Number(candidate.details.askDeltaValue || 0)
        : Number(candidate.details.bidDeltaValue || 0),
      dte: candidate.details.dte ?? null,
    },
  };
};

const roundForAdvisory = (value, digits = 4) => (
  Number.isFinite(value) ? Number(Number(value).toFixed(digits)) : null
);

const floorOptionPriceCents = (value) => {
  const numeric = Number(value);
  if (!(numeric > 0)) return null;
  return Math.max(0.01, Math.floor((numeric + 1e-9) * 100) / 100);
};

const getBuyPutEntryPricing = ({ askPrice, absDelta, dte, minScore = null, targetScore = null }) => {
  const liveAsk = Number(askPrice) || 0;
  const delta = Math.abs(Number(absDelta) || 0);
  const liveRawScore = liveAsk > 0 && delta > 0 ? delta / liveAsk : 0;
  const liveScore = normalizeBuyPutScore(liveRawScore, dte);
  const min = Number(minScore ?? 0);
  const target = Number(targetScore ?? 0);
  const requiredScore = Math.max(min > 0 ? min : 0, target > 0 ? target : 0);
  const thresholdPrice = requiredScore > 0 && delta > 0
    ? floorOptionPriceCents(getBuyPutPriceForEdgeScore(delta, requiredScore, dte))
    : null;

  let limitPrice = liveAsk > 0 ? liveAsk : null;
  let priceSource = 'live_ask';
  if (thresholdPrice != null && (!(limitPrice > 0) || thresholdPrice < limitPrice)) {
    limitPrice = thresholdPrice;
    priceSource = 'score_threshold';
  }

  const plannedRawScore = limitPrice > 0 && delta > 0 ? delta / limitPrice : 0;
  const plannedScore = normalizeBuyPutScore(plannedRawScore, dte);
  return {
    liveRawScore,
    liveScore,
    plannedRawScore,
    plannedScore,
    limitPrice,
    thresholdPrice,
    requiredScore,
    priceSource,
  };
};

const parseAdvisoryOptionInstrument = (name) => {
  const parts = String(name || '').split('-');
  if (parts.length !== 4 || !/^\d{8}$/.test(parts[1])) return null;
  const expiry = new Date(`${parts[1].slice(0, 4)}-${parts[1].slice(4, 6)}-${parts[1].slice(6, 8)}T08:00:00Z`);
  const strike = Number(parts[2]);
  return {
    expiry,
    strike: Number.isFinite(strike) ? strike : null,
    optionType: parts[3],
  };
};

const computeDteAt = (expiry, nowMs = Date.now()) => {
  const expiryMs = expiry instanceof Date ? expiry.getTime() : Number(expiry) * 1000;
  if (!Number.isFinite(expiryMs)) return null;
  return Math.max(0, (expiryMs - nowMs) / 86400000);
};

const finiteOrNull = (value) => {
  if (value == null || (typeof value === 'string' && value.trim() === '')) return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
};

const meanFinite = (values = []) => {
  const nums = values.map(finiteOrNull).filter((value) => value != null);
  return nums.length > 0 ? nums.reduce((sum, value) => sum + value, 0) / nums.length : null;
};

const sumFinite = (values = []) => values
  .map(finiteOrNull)
  .filter((value) => value != null)
  .reduce((sum, value) => sum + value, 0);

const getTickerOpenInterest = (ticker) => finiteOrNull(
  ticker?.stats?.oi
  ?? ticker?.stats?.open_interest
  ?? ticker?.open_interest
  ?? ticker?.details?.openInterest
);

const getTickerImpliedVol = (ticker) => finiteOrNull(
  ticker?.option_pricing?.i
  ?? ticker?.details?.impliedVol
  ?? ticker?.implied_vol
);

const getTickerSpreadPct = (ticker) => {
  const pct = candidateSpreadPct({ askPrice: ticker?.a, bidPrice: ticker?.b, markPrice: ticker?.M });
  return pct == null ? null : pct / 100;
};

const getLatestHourlyDeltaPct = (rows = [], lookbackHours = 24, nowMs = Date.now()) => {
  if (!Array.isArray(rows) || !Number.isFinite(nowMs)
    || !Number.isInteger(lookbackHours) || lookbackHours <= 0) return null;
  const hourMs = 60 * 60 * 1000;
  const latestHourMs = Math.floor(nowMs / hourMs) * hourMs - hourMs;
  // Both endpoints must be the requested completed hours. Skipping an unknown
  // endpoint would turn an older observation into an apparently current trend.
  const valueAt = (timestampMs) => {
    const matches = rows.filter((row) => row && new Date(row.hour || row.timestamp).getTime() === timestampMs);
    if (matches.length !== 1) return null;
    const row = matches[0];
    const raw = Object.prototype.hasOwnProperty.call(row, 'value') ? row.value : row.total_oi;
    if (typeof raw !== 'number' && typeof raw !== 'string') return null;
    const value = finiteOrNull(raw);
    return value != null && value >= 0 ? value : null;
  };
  const latest = valueAt(latestHourMs);
  const prior = valueAt(latestHourMs - lookbackHours * hourMs);
  if (latest == null || !(prior > 0)) return null;
  const change = ((latest - prior) / prior) * 100;
  return Number.isFinite(change) ? change : null;
};

const buildLiveSellCallMarketContext = (tickerMap = {}, nowMs = Date.now(), extra = {}) => {
  const rows = [];
  for (const [name, ticker] of Object.entries(tickerMap || {})) {
    const parsed = parseAdvisoryOptionInstrument(name);
    if (!parsed) continue;
    const dte = computeDteAt(parsed.expiry, nowMs);
    const delta = finiteOrNull(ticker?.option_pricing?.d);
    const bidPrice = finiteOrNull(ticker?.b);
    const askPrice = finiteOrNull(ticker?.a);
    const markPrice = finiteOrNull(ticker?.M);
    const bidAmount = finiteOrNull(ticker?.B) ?? 0;
    const askAmount = finiteOrNull(ticker?.A) ?? 0;
    const spreadPct = getTickerSpreadPct(ticker);
    const impliedVol = getTickerImpliedVol(ticker);
    const openInterest = getTickerOpenInterest(ticker);
    if (delta == null) continue;

    rows.push({
      name,
      optionType: parsed.optionType,
      expiry: parsed.expiry.getTime(),
      dte,
      delta,
      bidPrice,
      askPrice,
      markPrice,
      depth: bidAmount + askAmount,
      spreadPct,
      impliedVol,
      openInterest,
    });
  }

  const sellCallRows = rows.filter((row) =>
    row.optionType === 'C'
    && isSellCallCandidateInStrategyRange(row.dte, row.delta)
    && row.bidPrice > 0
  );
  const buyPutRows = rows.filter((row) =>
    row.optionType === 'P'
    && row.delta >= PUT_DELTA_RANGE[0]
    && row.delta <= PUT_DELTA_RANGE[1]
    && row.dte >= BUY_PUT_ADVISORY_DTE_RANGE[0]
    && row.dte <= BUY_PUT_ADVISORY_DTE_RANGE[1]
    && row.askPrice > 0
  );
  const valueRows = [...sellCallRows, ...buyPutRows];
  const bestCallScore = Math.max(0, ...sellCallRows.map((row) =>
    row.bidPrice > 0 && Math.abs(row.delta) > 0 ? row.bidPrice / Math.abs(row.delta) : 0
  ));
  const bestPutScore = Math.max(0, ...buyPutRows.map((row) =>
    row.askPrice > 0 && Math.abs(row.delta) > 0 ? Math.abs(row.delta) / row.askPrice : 0
  ));
  const callIv = meanFinite(sellCallRows.map((row) => row.impliedVol));
  const putIv = meanFinite(buyPutRows.map((row) => row.impliedVol));
  const callOi = sumFinite(sellCallRows.map((row) => row.openInterest));
  const putOi = sumFinite(buyPutRows.map((row) => row.openInterest));
  const matchedSkew = computeMatchedPutCallSkew(buyPutRows, rows.filter((row) => row.optionType === 'C'));

  return {
    market_avg_spread: meanFinite(valueRows.map((row) => row.spreadPct)),
    market_avg_depth: meanFinite(valueRows.map((row) => row.depth)),
    market_best_call_score: bestCallScore > 0 ? bestCallScore : null,
    market_best_put_score: bestPutScore > 0 ? bestPutScore : null,
    market_call_iv: callIv,
    market_put_iv: putIv,
    market_skew: matchedSkew.skew,
    market_skew_basis: 'same_expiry_absolute_delta',
    market_skew_matched_pairs: matchedSkew.matchedPairs,
    market_skew_matched_expiries: matchedSkew.matchedExpiries,
    market_skew_unmatched_puts: matchedSkew.unmatchedPuts,
    market_call_oi: callOi > 0 ? callOi : null,
    market_put_oi: putOi > 0 ? putOi : null,
    market_total_oi: callOi + putOi > 0 ? callOi + putOi : null,
    market_oi_delta_24h_pct: finiteOrNull(extra.market_oi_delta_24h_pct),
    market_pc_oi: callOi > 0 ? putOi / callOi : null,
    sell_call_candidates: sellCallRows.length,
    buy_put_candidates: buyPutRows.length,
  };
};

const computePutShockPayoffMultiples = ({ strike, spotPrice, entryPrice }) => {
  const normalizedStrike = Number(strike);
  const normalizedSpot = Number(spotPrice);
  const normalizedEntry = Number(entryPrice);
  const byShock = {};
  if (!(normalizedStrike > 0) || !(normalizedSpot > 0) || !(normalizedEntry > 0)) {
    for (const shock of BUY_PUT_EDGE_SHOCKS) byShock[`${Math.round(shock * 100)}pct`] = null;
    return { byShock, maxMultiple: null, avgMultiple: null, multiple40Pct: null };
  }
  const multiples = [];
  for (const shock of BUY_PUT_EDGE_SHOCKS) {
    const shockedSpot = normalizedSpot * (1 - shock);
    const payoff = Math.max(normalizedStrike - shockedSpot, 0);
    const multiple = payoff / normalizedEntry;
    byShock[`${Math.round(shock * 100)}pct`] = roundForAdvisory(multiple, 2);
    multiples.push(multiple);
  }
  return {
    byShock,
    maxMultiple: Math.max(...multiples),
    avgMultiple: multiples.reduce((sum, value) => sum + value, 0) / multiples.length,
    multiple40Pct: multiples[BUY_PUT_EDGE_SHOCKS.findIndex((shock) => Math.abs(shock - 0.40) < 1e-9)],
  };
};

// Keep the existing telemetry shape, but rank only by continuous PUT EDGE.
// Crash scenarios are descriptive intrinsic payoffs, not score multipliers.
const classifyBuyPutEdge = ({ score, rawScore = null, dte, strike, spotPrice,
  entryPrice = null, askPrice = null, bidPrice = null, markPrice = null,
  askAmount = null, bidAmount = null, impliedVol = null, marketContext = null } = {}) => {
  const edgeScore = Number(score) > 0 && Number.isFinite(Number(score)) ? Number(score) : 0;
  const numericDte = Number(dte);
  const numericEntry = Number(entryPrice ?? askPrice);
  const normalizationFactor = getBuyPutDteNormalizationFactor(numericDte);
  const shockPayoff = computePutShockPayoffMultiples({ strike, spotPrice, entryPrice: numericEntry });
  const spread = Number(askPrice) > 0 && Number(bidPrice) > 0 && Number(markPrice) > 0
    ? (Number(askPrice) - Number(bidPrice)) / Number(markPrice) : null;
  return {
    recommendation: edgeScore > 0 ? 'scored' : 'unavailable',
    dte_bucket: 'strategy_45_78_dte',
    score_band: 'continuous_put_edge',
    selection_metric: 'put_edge_v1',
    selection_multiplier: normalizationFactor,
    selection_score: edgeScore,
    edge_components: {
      raw_score: roundForAdvisory(Number(rawScore ?? (normalizationFactor > 0 ? edgeScore / normalizationFactor : 0)), 6),
      put_edge_score: roundForAdvisory(edgeScore, 6),
      dte: roundForAdvisory(numericDte, 4),
      reference_dte: BUY_PUT_EDGE_REFERENCE_DTE,
      dte_exponent: BUY_PUT_EDGE_DTE_EXPONENT,
      normalization_factor: roundForAdvisory(normalizationFactor, 6),
      candidate_spread_pct: roundForAdvisory(spread != null ? spread * 100 : null, 2),
      candidate_iv_pct: roundForAdvisory(impliedVol != null ? Number(impliedVol) * 100 : null, 2),
      market_spread_pct: roundForAdvisory(marketContext?.market_avg_spread != null ? marketContext.market_avg_spread * 100 : null, 2),
      market_put_iv_pct: roundForAdvisory(marketContext?.market_put_iv != null ? marketContext.market_put_iv * 100 : null, 2),
      market_skew_pct: roundForAdvisory(marketContext?.market_skew != null ? marketContext.market_skew * 100 : null, 2),
      market_oi_delta_24h_pct: roundForAdvisory(marketContext?.market_oi_delta_24h_pct, 2),
      shock_payoff_multiple_40pct: roundForAdvisory(shockPayoff.multiple40Pct, 2),
      shock_payoff_multiples: shockPayoff.byShock,
      entry_price: roundForAdvisory(numericEntry, 4),
      depth: roundForAdvisory(Number(askAmount || 0) + Number(bidAmount || 0), 2),
    },
    reason: `PUT EDGE = raw score × (DTE / ${BUY_PUT_EDGE_REFERENCE_DTE})^${BUY_PUT_EDGE_DTE_EXPONENT}; delta per premium dollar, not a crash-payoff multiple. Other observations do not alter ranking.`,
  };
};

const getBestCurrentBuyPutCandidate = (tickerMap = {}, nowMs = Date.now(), marketContext = null, spotPrice = null) => {
  let best = null;
  for (const [name, ticker] of Object.entries(tickerMap || {})) {
    const parsed = parseAdvisoryOptionInstrument(name);
    if (!parsed || parsed.optionType !== 'P') continue;
    const dte = computeDteAt(parsed.expiry, nowMs);
    if (!(dte >= BUY_PUT_ADVISORY_DTE_RANGE[0] && dte <= BUY_PUT_ADVISORY_DTE_RANGE[1])) continue;

    const delta = Number(ticker?.option_pricing?.d);
    const askPrice = Number(ticker?.a);
    const askAmount = Number(ticker?.A) || 0;
    const bidPrice = Number(ticker?.b);
    const bidAmount = Number(ticker?.B) || 0;
    const markPrice = Number(ticker?.M);
    if (!(delta >= PUT_DELTA_RANGE[0] && delta <= PUT_DELTA_RANGE[1])) continue;
    if (!(askPrice > 0)) continue;

    const rawScore = Math.abs(delta) / askPrice;
    const score = normalizeBuyPutScore(rawScore, dte);
    const research = classifyBuyPutEdge({
      score,
      rawScore,
      dte,
      strike: parsed.strike,
      spotPrice,
      entryPrice: askPrice,
      askPrice,
      bidPrice,
      markPrice,
      askAmount,
      bidAmount,
      impliedVol: getTickerImpliedVol(ticker),
      marketContext,
    });
    const selectionScore = research.selection_score || score;
    if (isBetterBuyPutCandidate({ instrument: name, selection_score: selectionScore, edge_score: score }, best)) {
      best = {
        instrument: name,
        delta,
        ask_price: askPrice,
        ask_amount: askAmount,
        bid_price: bidPrice,
        bid_amount: bidAmount,
        mark_price: markPrice,
        strike: parsed.strike,
        expiry: Math.floor(parsed.expiry.getTime() / 1000),
        dte,
        score,
        raw_score: rawScore,
        edge_score: score,
        selection_score: selectionScore,
        research,
      };
    }
  }
  return best;
};

const getBestCurrentBuyPutEdgeCandidate = (tickerMap = {}, nowMs = Date.now()) => {
  let best = null;
  for (const [name, ticker] of Object.entries(tickerMap || {})) {
    const parsed = parseAdvisoryOptionInstrument(name);
    if (!parsed || parsed.optionType !== 'P') continue;
    const dte = computeDteAt(parsed.expiry, nowMs);
    if (!(dte >= BUY_PUT_ADVISORY_DTE_RANGE[0] && dte <= BUY_PUT_ADVISORY_DTE_RANGE[1])) continue;
    const delta = Number(ticker?.option_pricing?.d);
    const askPrice = Number(ticker?.a);
    if (!(delta >= PUT_DELTA_RANGE[0] && delta <= PUT_DELTA_RANGE[1]) || !(askPrice > 0)) continue;
    const rawScore = Math.abs(delta) / askPrice;
    const edgeScore = normalizeBuyPutScore(rawScore, dte);
    if (!(edgeScore > 0) || !isBetterBuyPutCandidate({ instrument: name, edge_score: edgeScore }, best)) continue;
    best = {
      instrument: name,
      raw_score: rawScore,
      edge_score: edgeScore,
      ask_price: askPrice,
      bid_price: Number(ticker?.b),
      mark_price: Number(ticker?.M),
      ask_amount: Number(ticker?.A) || 0,
      strike: parsed.strike,
      expiry: Math.floor(parsed.expiry.getTime() / 1000),
      delta,
      dte,
    };
  }
  return best;
};

const getBestCurrentSellCallCandidate = (
  tickerMap = {},
  nowMs = Date.now()
) => {
  let best = null;
  for (const [name, ticker] of Object.entries(tickerMap || {})) {
    const parsed = parseAdvisoryOptionInstrument(name);
    if (!parsed || parsed.optionType !== 'C') continue;
    const dte = computeDteAt(parsed.expiry, nowMs);

    const delta = Number(ticker?.option_pricing?.d);
    const bidPrice = Number(ticker?.b);
    const bidAmount = Number(ticker?.B) || 0;
    const askPrice = Number(ticker?.a);
    const askAmount = Number(ticker?.A) || 0;
    const markPrice = Number(ticker?.M);
    if (!isSellCallCandidateInStrategyRange(dte, delta)) continue;
    if (!(bidPrice > 0)) continue;

    const score = bidPrice / Math.abs(delta);
    const research = buildSellCallEdge({ score, dte });
    const selectionScore = research.selection_score || score;
    if (!best || selectionScore > (best.selection_score || best.score)) {
      best = {
        instrument: name,
        delta,
        bid_price: bidPrice,
        bid_amount: bidAmount,
        ask_price: askPrice,
        ask_amount: askAmount,
        mark_price: markPrice,
        strike: parsed.strike,
        expiry: Math.floor(parsed.expiry.getTime() / 1000),
        dte,
        score,
        selection_score: selectionScore,
        research,
      };
    }
  }
  return best;
};

const classifySpotPriceAction = (momentum = {}, recentSpotPrices = []) => {
  const mainText = (value) => {
    if (!value) return '';
    if (typeof value === 'string') return value.toLowerCase();
    return `${value.main || ''} ${value.derivative || ''}`.toLowerCase();
  };
  const mediumText = mainText(momentum.mediumTerm);
  const shortText = mainText(momentum.shortTerm);
  const combined = `${mediumText} ${shortText}`;

  let slopePct = null;
  const rows = Array.isArray(recentSpotPrices)
    ? recentSpotPrices
        .map((row) => ({ price: Number(row.price), timestamp: new Date(row.timestamp).getTime() }))
        .filter((row) => Number.isFinite(row.price) && Number.isFinite(row.timestamp))
        .sort((a, b) => a.timestamp - b.timestamp)
    : [];
  if (rows.length >= 2 && rows[0].price > 0) {
    slopePct = ((rows[rows.length - 1].price - rows[0].price) / rows[0].price) * 100;
  }

  if (/\bdownward\b|_down\b|down\b/.test(shortText) || /\bdownward\b/.test(mediumText)) {
    return { state: 'downward', slope_pct_6h: roundForAdvisory(slopePct, 2), reason: 'momentum labels point downward' };
  }
  if (/\bupward\b|_up\b|up\b/.test(shortText) || /\bupward\b/.test(mediumText)) {
    return { state: 'upward', slope_pct_6h: roundForAdvisory(slopePct, 2), reason: 'momentum labels point upward' };
  }
  if (Number.isFinite(slopePct)) {
    if (slopePct <= -0.25) return { state: 'downward', slope_pct_6h: roundForAdvisory(slopePct, 2), reason: 'recent spot slope is negative' };
    if (slopePct >= 0.25) return { state: 'upward', slope_pct_6h: roundForAdvisory(slopePct, 2), reason: 'recent spot slope is positive' };
  }
  if (combined.includes('neutral') || combined.includes('flat')) {
    return { state: 'stable', slope_pct_6h: roundForAdvisory(slopePct, 2), reason: 'momentum labels are neutral or flat' };
  }
  return { state: 'unknown', slope_pct_6h: roundForAdvisory(slopePct, 2), reason: 'spot trend is not classifiable' };
};

const computeScoreTrendPct = (samples, currentScore, hours) => {
  if (!(currentScore > 0) || !Array.isArray(samples) || samples.length === 0) return null;
  const cutoff = Date.now() - hours * 60 * 60 * 1000;
  const recent = samples
    .filter((row) => new Date(row.timestamp).getTime() >= cutoff)
    .map((row) => Number(row.score))
    .filter((score) => score > 0);
  if (recent.length === 0) return null;
  const avg = recent.reduce((sum, score) => sum + score, 0) / recent.length;
  return avg > 0 ? roundForAdvisory(((currentScore - avg) / avg) * 100, 2) : null;
};

const summarizeRecentScoreWindow = (samples, currentScore, hours) => {
  if (!(currentScore > 0) || !Array.isArray(samples) || samples.length === 0) {
    return { hours, samples: 0 };
  }
  const cutoff = Date.now() - hours * 60 * 60 * 1000;
  const scores = samples
    .filter((row) => new Date(row.timestamp).getTime() >= cutoff)
    .map((row) => Number(row.score))
    .filter((score) => score > 0)
    .sort((a, b) => a - b);
  if (scores.length === 0) return { hours, samples: 0 };

  const avg = scores.reduce((sum, score) => sum + score, 0) / scores.length;
  const best = scores[scores.length - 1];
  const percentile = (scores.filter((score) => score <= currentScore).length / scores.length) * 100;

  return {
    hours,
    samples: scores.length,
    avg_score: roundForAdvisory(avg, 6),
    best_score: roundForAdvisory(best, 6),
    percentile: roundForAdvisory(percentile, 1),
    current_vs_avg_pct: avg > 0 ? roundForAdvisory(((currentScore - avg) / avg) * 100, 2) : null,
    current_vs_recent_best_pct: best > 0 ? roundForAdvisory((currentScore / best) * 100, 2) : null,
  };
};

const computeSpotMovePct = (recentSpotPrices = [], lookbackMinutes = 20) => {
  const rows = Array.isArray(recentSpotPrices)
    ? recentSpotPrices
        .map((row) => ({ price: Number(row.price), timestamp: new Date(row.timestamp).getTime() }))
        .filter((row) => Number.isFinite(row.price) && row.price > 0 && Number.isFinite(row.timestamp))
        .sort((a, b) => a.timestamp - b.timestamp)
    : [];
  if (rows.length < 2) return null;

  const latest = rows[rows.length - 1];
  const cutoff = latest.timestamp - lookbackMinutes * 60 * 1000;
  let base = rows[0];
  for (const row of rows) {
    if (row.timestamp <= cutoff) {
      base = row;
    } else {
      break;
    }
  }
  if (!(base.price > 0) || base.timestamp === latest.timestamp) return null;
  return roundForAdvisory(((latest.price - base.price) / base.price) * 100, 2);
};

const buildPutRepricingLagContext = ({
  currentScore,
  priorBestScore,
  scoreTrendPct,
  spotMovePct,
  spotAction,
}) => {
  const currentVsPriorBestPct = priorBestScore > 0 ? (currentScore / priorBestScore) * 100 : null;
  const scoreJump = Number(scoreTrendPct) >= BUY_PUT_REPRICING_LAG_MIN_SCORE_TREND_PCT;
  const nearRollingBest = Number(currentVsPriorBestPct) >= BUY_PUT_REPRICING_LAG_NEAR_BEST_PCT;
  const spotDropped = Number(spotMovePct) <= BUY_PUT_REPRICING_LAG_MIN_SPOT_DROP_PCT;
  const downwardSpot = spotAction?.state === 'downward';
  const detected = Boolean(currentScore > 0 && (
    (scoreJump && (spotDropped || downwardSpot))
    || (nearRollingBest && spotDropped)
  ));

  return {
    is_detected: detected,
    score_trend_1h_pct: roundForAdvisory(scoreTrendPct, 2),
    spot_move_20m_pct: roundForAdvisory(spotMovePct, 2),
    current_vs_prior_best_pct: roundForAdvisory(currentVsPriorBestPct, 2),
    thresholds: {
      min_score_trend_1h_pct: BUY_PUT_REPRICING_LAG_MIN_SCORE_TREND_PCT,
      min_spot_move_20m_pct: BUY_PUT_REPRICING_LAG_MIN_SPOT_DROP_PCT,
      near_best_pct: BUY_PUT_REPRICING_LAG_NEAR_BEST_PCT,
    },
    reason: detected
      ? 'Put score jumped or is near the rolling best while spot is dropping, consistent with option ask lagging the new spot.'
      : 'No short-term spot/put repricing lag detected.',
  };
};

const buildRecentRelativePutValueContext = ({
  currentScore,
  priorSamples,
  priorBestScore,
  spotAction,
}) => {
  const recentWindow = summarizeRecentScoreWindow(
    priorSamples,
    currentScore,
    BUY_PUT_RECENT_VALUE_LOOKBACK_HOURS
  );
  const currentVsRollingBestPct = priorBestScore > 0 ? (currentScore / priorBestScore) * 100 : null;
  const enoughSamples = Number(recentWindow.samples) >= BUY_PUT_RECENT_VALUE_MIN_SAMPLES;
  const strongRecentPercentile = Number(recentWindow.percentile) >= BUY_PUT_RECENT_VALUE_MIN_PERCENTILE;
  const strongRecentTrend = Number(recentWindow.current_vs_avg_pct) >= BUY_PUT_RECENT_VALUE_MIN_TREND_PCT;
  const notGloballyCheapOnly = currentVsRollingBestPct == null
    || currentVsRollingBestPct >= BUY_PUT_RECENT_VALUE_MIN_ROLLING_BEST_PCT;
  const compatibleSpot = ['downward', 'stable'].includes(spotAction?.state);
  const detected = Boolean(
    currentScore > 0
    && enoughSamples
    && strongRecentPercentile
    && strongRecentTrend
    && notGloballyCheapOnly
    && compatibleSpot
  );

  return {
    is_detected: detected,
    lookback_hours: BUY_PUT_RECENT_VALUE_LOOKBACK_HOURS,
    samples: recentWindow.samples || 0,
    percentile: recentWindow.percentile ?? null,
    current_vs_recent_avg_pct: recentWindow.current_vs_avg_pct ?? null,
    current_vs_recent_best_pct: recentWindow.current_vs_recent_best_pct ?? null,
    current_vs_rolling_best_pct: roundForAdvisory(currentVsRollingBestPct, 2),
    thresholds: {
      min_samples: BUY_PUT_RECENT_VALUE_MIN_SAMPLES,
      min_trend_pct: BUY_PUT_RECENT_VALUE_MIN_TREND_PCT,
      min_percentile: BUY_PUT_RECENT_VALUE_MIN_PERCENTILE,
      min_rolling_best_pct: BUY_PUT_RECENT_VALUE_MIN_ROLLING_BEST_PCT,
    },
    reason: detected
      ? 'Current buy-put score is materially better than the recent local window while still respectable versus the rolling-window best.'
      : 'No recent-relative buy-put value signal detected.',
  };
};

const isActionableBuyPutSignal = (signal) => (
  signal === 'strict_fresh_best'
  || signal === 'spot_drop_option_repricing_lag'
  || signal === 'recent_relative_value'
);

const BUY_PUT_VALUE_SIGNALS = new Set([
  'strict_fresh_best',
  'spot_drop_option_repricing_lag',
  'recent_relative_value',
  'any_actionable_buy_put',
]);

const normalizeBuyPutValueSignal = (signal) => {
  const normalized = String(signal || '').trim().toLowerCase();
  if (!normalized) return null;
  if (normalized === 'fresh_best') return 'strict_fresh_best';
  if (normalized === 'repricing_lag' || normalized === 'spot_lag') return 'spot_drop_option_repricing_lag';
  if (normalized === 'relative_value' || normalized === 'recent_value') return 'recent_relative_value';
  if (BUY_PUT_VALUE_SIGNALS.has(normalized)) {
    return normalized;
  }
  return null;
};

const hasExplicitBuyPutValueSignal = (signal) => String(signal ?? '').trim().length > 0;
const isKnownBuyPutValueSignal = (signal) => !hasExplicitBuyPutValueSignal(signal) || normalizeBuyPutValueSignal(signal) != null;

const buyPutValueSignalMatches = (requiredSignal, currentSignal) => {
  const required = normalizeBuyPutValueSignal(requiredSignal);
  if (hasExplicitBuyPutValueSignal(requiredSignal) && !required) return false;
  if (!required) return true;
  if (required === 'any_actionable_buy_put') return true;
  return currentSignal === required;
};

const buildRollingOptionValueContext = ({
  tickerMap,
  momentum,
  putBudgetRemaining,
  activeRules = [],
  recentPendingActions = [],
  openRestingOrders = [],
  currentTickTimestamp = null,
  spotPrice = null,
  expectedInstruments = null,
  quoteAvailability = null,
  nowMs = Date.now(),
}) => {
  const before = currentTickTimestamp || new Date().toISOString();
  const quoteSummary = quoteAvailability || summarizeAdvisoryQuotes(tickerMap, {
    nowMs, inputTimestamp: currentTickTimestamp, expectedInstruments,
    putDeltaRange: PUT_DELTA_RANGE, putDteRange: BUY_PUT_ADVISORY_DTE_RANGE,
    callDeltaRange: CALL_DELTA_RANGE, callDteRange: CALL_EXPIRATION_RANGE,
  });
  const since = new Date(nowMs - ADVISORY_OPTION_VALUE_WINDOW_DAYS * 86400000).toISOString();
  let marketOiDelta24hPct = null;
  if (db && typeof db.getOpenInterestHourly === 'function') {
    try {
      marketOiDelta24hPct = getLatestHourlyDeltaPct(
        db.getOpenInterestHourly(new Date(Date.now() - 30 * 60 * 60 * 1000).toISOString()),
        24
      );
    } catch (e) {
      console.log(`📋 Advisory context: open-interest trend query failed: ${e.message}`);
    }
  }
  const sellCallMarketContext = buildLiveSellCallMarketContext(tickerMap, nowMs, {
    market_oi_delta_24h_pct: marketOiDelta24hPct,
  });
  const currentPut = getBestCurrentBuyPutCandidate(tickerMap, nowMs, sellCallMarketContext, spotPrice);
  // History stores the maximum eligible PUT EDGE per observation, independent
  // of quality ranking. Compare that same population and statistic live.
  const currentPutEdge = getBestCurrentBuyPutEdgeCandidate(tickerMap, nowMs);
  const currentCall = getBestCurrentSellCallCandidate(tickerMap, nowMs);
  let priorSamples = [];
  let priorBestDetail = null;
  let priorCallSamples = [];
  let priorCallBestDetail = null;
  let recentSpotPrices = [];

  if (db) {
    try {
      if (typeof db.getBuyPutScoreSamples === 'function') {
        priorSamples = db.getBuyPutScoreSamples({
          since,
          before,
          minDelta: PUT_DELTA_RANGE[0],
          maxDelta: PUT_DELTA_RANGE[1],
          minDte: BUY_PUT_ADVISORY_DTE_RANGE[0],
          maxDte: BUY_PUT_ADVISORY_DTE_RANGE[1],
        });
      }
      if (typeof db.getBestBuyPutScoreDetail === 'function') {
        priorBestDetail = db.getBestBuyPutScoreDetail({
          since,
          before,
          minDelta: PUT_DELTA_RANGE[0],
          maxDelta: PUT_DELTA_RANGE[1],
          minDte: BUY_PUT_ADVISORY_DTE_RANGE[0],
          maxDte: BUY_PUT_ADVISORY_DTE_RANGE[1],
        });
      }
      if (typeof db.getSellCallScoreSamples === 'function') {
        priorCallSamples = db.getSellCallScoreSamples({
          since,
          before,
          minDelta: CALL_DELTA_RANGE[0],
          maxDelta: CALL_DELTA_RANGE[1],
          minDte: CALL_EXPIRATION_RANGE[0],
          maxDte: CALL_EXPIRATION_RANGE[1],
        });
      }
      if (typeof db.getBestSellCallScoreDetail === 'function') {
        priorCallBestDetail = db.getBestSellCallScoreDetail({
          since,
          before,
          minDelta: CALL_DELTA_RANGE[0],
          maxDelta: CALL_DELTA_RANGE[1],
          minDte: CALL_EXPIRATION_RANGE[0],
          maxDte: CALL_EXPIRATION_RANGE[1],
        });
      }
      if (typeof db.getRecentSpotPrices === 'function') {
        recentSpotPrices = db.getRecentSpotPrices(new Date(Date.now() - 6 * 60 * 60 * 1000).toISOString());
      }
    } catch (e) {
      console.log(`📋 Advisory context: rolling option value query failed: ${e.message}`);
    }
  }

  const priorScores = priorSamples.map((row) => Number(row.score)).filter((score) => score > 0);
  const priorBestScore = priorScores.length > 0 ? Math.max(...priorScores) : null;
  const currentScore = currentPutEdge ? finiteOrNull(currentPutEdge.edge_score) : null;
  const currentVsPriorBestPct = currentScore != null && priorBestScore > 0 ? (currentScore / priorBestScore) * 100 : null;
  const percentile = currentScore > 0 && priorScores.length > 0
    ? (priorScores.filter((score) => score <= currentScore).length / priorScores.length) * 100
    : null;
  const freshBest = currentScore == null ? null : currentScore > 0 && priorBestScore > 0 && currentScore > priorBestScore;
  const priorCallScores = priorCallSamples.map((row) => Number(row.score)).filter((score) => score > 0);
  const priorCallBestScore = priorCallScores.length > 0 ? Math.max(...priorCallScores) : null;
  const currentCallScore = currentCall ? finiteOrNull(currentCall.selection_score) : null;
  const currentCallVsPriorBestPct = currentCallScore != null && priorCallBestScore > 0
    ? (currentCallScore / priorCallBestScore) * 100
    : null;
  const callPercentile = currentCallScore > 0 && priorCallScores.length > 0
    ? (priorCallScores.filter((score) => score <= currentCallScore).length / priorCallScores.length) * 100
    : null;
  const callFreshBest = currentCallScore == null ? null : currentCallScore > 0 && priorCallBestScore > 0 && currentCallScore > priorCallBestScore;
  const callTrend1hPct = computeScoreTrendPct(priorCallSamples, currentCallScore, 1);
  const callTrend6hPct = computeScoreTrendPct(priorCallSamples, currentCallScore, 6);
  const callTrend24hPct = computeScoreTrendPct(priorCallSamples, currentCallScore, 24);
  const callResearchContext = currentCall
    ? buildSellCallEdge({ score: currentCall.score, dte: currentCall.dte })
    : null;
  const activeBuyPutRules = activeRules.filter((rule) => rule?.is_active !== 0 && rule?.rule_type === 'entry' && rule?.action === 'buy_put').length;
  const workingBuyPutActions = recentPendingActions.filter((action) =>
    action?.action === 'buy_put' && ['pending', 'confirmed', 'resting'].includes(action?.status)
  ).length;
  const blockingBuyPutActions = recentPendingActions.filter((action) =>
    action?.action === 'buy_put' && ['pending', 'confirmed'].includes(action?.status)
  ).length;
  const restingBuyPutOrders = openRestingOrders.filter((order) =>
    order?.action === 'buy_put' && (!order?.status || order.status === 'open')
  ).length;
  const hasWorkingBuyPut = (workingBuyPutActions + restingBuyPutOrders) > 0;
  const spotAction = classifySpotPriceAction(momentum, recentSpotPrices);
  const budgetAvailable = Number(putBudgetRemaining) > 1;
  const scoreTrend1hPct = computeScoreTrendPct(priorSamples, currentScore, BUY_PUT_REPRICING_LAG_SCORE_LOOKBACK_HOURS);
  const spotMove20mPct = computeSpotMovePct(recentSpotPrices, BUY_PUT_REPRICING_LAG_LOOKBACK_MINUTES);
  const repricingLag = buildPutRepricingLagContext({
    currentScore,
    priorBestScore,
    scoreTrendPct: scoreTrend1hPct,
    spotMovePct: spotMove20mPct,
    spotAction,
  });
  const recentRelativeValue = buildRecentRelativePutValueContext({
    currentScore,
    priorSamples,
    priorBestScore,
    spotAction,
  });
  if (currentScore == null) {
    Object.assign(repricingLag, {
      is_detected: null,
      current_vs_prior_best_pct: null,
      score_trend_1h_pct: null,
      reason: `PUT value comparison unavailable: ${quoteSummary.put.reason}`,
    });
    Object.assign(recentRelativeValue, {
      is_detected: null,
      samples: null,
      percentile: null,
      current_vs_recent_avg_pct: null,
      current_vs_recent_best_pct: null,
      current_vs_rolling_best_pct: null,
      reason: `PUT value comparison unavailable: ${quoteSummary.put.reason}`,
    });
  }
  const repricingLagSignal = Boolean(!freshBest && repricingLag.is_detected);
  const recentRelativeValueSignal = Boolean(!freshBest && !repricingLagSignal && recentRelativeValue.is_detected);
  const actionSignal = freshBest
    ? 'strict_fresh_best'
    : repricingLagSignal
      ? 'spot_drop_option_repricing_lag'
      : recentRelativeValueSignal
        ? 'recent_relative_value'
        : null;
  const requiresDecision = Boolean(actionSignal && budgetAvailable && blockingBuyPutActions === 0);
  const scoreNudge = repricingLagSignal
    ? BUY_PUT_REPRICING_LAG_SCORE_NUDGE
    : spotAction.state === 'downward'
      ? BUY_PUT_URGENT_SCORE_NUDGE
      : BUY_PUT_PATIENT_SCORE_NUDGE;
  const targetScore = currentScore > 0 ? currentScore * scoreNudge : null;
  const suggestedLimitPrice = currentPut && targetScore > 0
    ? floorOptionPriceCents(getBuyPutPriceForEdgeScore(Math.abs(currentPut.delta), targetScore, currentPut.dte))
    : null;
  const executionStyle = currentScore == null ? 'unavailable' : repricingLagSignal
    ? 'spot_lag_near_live_limit'
    : spotAction.state === 'downward'
    ? 'less_patient_limit'
    : spotAction.state === 'stable'
      ? 'patient_limit'
      : 'explain_no_buy_unless_other_facts_override';

  return {
    window_days: ADVISORY_OPTION_VALUE_WINDOW_DAYS,
    quote_availability: quoteSummary,
    buy_put_filters: {
      delta_range: PUT_DELTA_RANGE,
      dte_range: BUY_PUT_ADVISORY_DTE_RANGE,
      raw_score: 'abs(delta) / ask_price',
      edge_score: `raw_score * (DTE / ${BUY_PUT_EDGE_REFERENCE_DTE})^${BUY_PUT_EDGE_DTE_EXPONENT}`,
      selection_metric: 'put_edge_v1',
      selection_price_basis: 'live_ask',
    },
    sell_call_filters: {
      delta_range: CALL_DELTA_RANGE,
      dte_range: CALL_EXPIRATION_RANGE,
      raw_score: 'bid_price / abs(delta)',
      edge_score: `raw_score * (${SELL_CALL_EDGE_REFERENCE_DTE} / DTE)^${SELL_CALL_EDGE_DTE_EXPONENT}`,
    },
    put_value_context: {
      availability: quoteSummary.put,
      comparison_basis: 'maximum_eligible_put_edge_per_observation',
      current_score: roundForAdvisory(currentScore, 6),
      prior_window_best_score: roundForAdvisory(priorBestScore, 6),
      current_vs_prior_best_pct: roundForAdvisory(currentVsPriorBestPct, 2),
      percentile_vs_prior_window: roundForAdvisory(percentile, 1),
      is_strict_fresh_best: freshBest,
      trend_1h_pct: scoreTrend1hPct,
      trend_6h_pct: computeScoreTrendPct(priorSamples, currentScore, 6),
      trend_24h_pct: computeScoreTrendPct(priorSamples, currentScore, 24),
      current_detail: currentPutEdge ? {
        instrument: currentPutEdge.instrument,
        raw_score: roundForAdvisory(currentPutEdge.raw_score, 6),
        put_edge_score: roundForAdvisory(currentPutEdge.edge_score, 6),
        delta: roundForAdvisory(currentPutEdge.delta, 4),
        ask_price: roundForAdvisory(currentPutEdge.ask_price, 4),
        bid_price: roundForAdvisory(currentPutEdge.bid_price, 4),
        mark_price: roundForAdvisory(currentPutEdge.mark_price, 4),
        ask_amount: roundForAdvisory(currentPutEdge.ask_amount, 2),
        spread_pct: roundForAdvisory(candidateSpreadPct({ askPrice: currentPutEdge.ask_price, bidPrice: currentPutEdge.bid_price, markPrice: currentPutEdge.mark_price }), 2),
        spread_basis: 'bid_ask_difference_over_mark',
        quote_received_at: tickerMap?.[currentPutEdge.instrument]?.quote_received_at || null,
        strike: currentPutEdge.strike,
        expiry: currentPutEdge.expiry,
        dte: roundForAdvisory(currentPutEdge.dte, 1),
      } : null,
      selected_candidate_detail: currentPut ? {
        instrument: currentPut.instrument,
        raw_score: roundForAdvisory(currentPut.raw_score, 6),
        put_edge_score: roundForAdvisory(currentPut.edge_score, 6),
        delta: roundForAdvisory(currentPut.delta, 4),
        ask_price: roundForAdvisory(currentPut.ask_price, 4),
        bid_price: roundForAdvisory(currentPut.bid_price, 4),
        mark_price: roundForAdvisory(currentPut.mark_price, 4),
        ask_amount: roundForAdvisory(currentPut.ask_amount, 2),
        spread_pct: roundForAdvisory(candidateSpreadPct({ askPrice: currentPut.ask_price, bidPrice: currentPut.bid_price, markPrice: currentPut.mark_price }), 2),
        spread_basis: 'bid_ask_difference_over_mark',
        quote_received_at: tickerMap?.[currentPut.instrument]?.quote_received_at || null,
        strike: currentPut.strike,
        expiry: currentPut.expiry,
        dte: roundForAdvisory(currentPut.dte, 1),
      } : null,
      research_context: currentPut?.research ? {
        recommendation: currentPut.research.recommendation,
        dte_bucket: currentPut.research.dte_bucket,
        score_band: currentPut.research.score_band,
        edge_components: currentPut.research.edge_components,
        selection_multiplier: roundForAdvisory(currentPut.research.selection_multiplier, 4),
        selection_score: roundForAdvisory(currentPut.research.selection_score, 6),
        selection_metric: 'put_edge_v1',
        reason: currentPut.research.reason,
      } : null,
      prior_window_best_detail: priorBestDetail ? {
        timestamp: priorBestDetail.timestamp,
        instrument: priorBestDetail.instrument_name,
        delta: roundForAdvisory(Number(priorBestDetail.delta), 4),
        ask_price: roundForAdvisory(Number(priorBestDetail.ask_price), 4),
        strike: Number(priorBestDetail.strike),
        expiry: Number(priorBestDetail.expiry),
        dte: roundForAdvisory(Number(priorBestDetail.dte), 1),
        raw_score: roundForAdvisory(Number(priorBestDetail.raw_score), 6),
        put_edge_score: roundForAdvisory(Number(priorBestDetail.score), 6),
      } : null,
      samples: priorScores.length,
    },
    call_value_context: {
      availability: quoteSummary.call,
      current_score: roundForAdvisory(currentCallScore, 2),
      prior_window_best_score: roundForAdvisory(priorCallBestScore, 2),
      current_vs_prior_best_pct: roundForAdvisory(currentCallVsPriorBestPct, 2),
      percentile_vs_prior_window: roundForAdvisory(callPercentile, 1),
      is_strict_fresh_best: callFreshBest,
      trend_1h_pct: callTrend1hPct,
      trend_6h_pct: callTrend6hPct,
      trend_24h_pct: callTrend24hPct,
      market_context: {
        market_avg_spread_pct: roundForAdvisory(sellCallMarketContext.market_avg_spread != null ? sellCallMarketContext.market_avg_spread * 100 : null, 2),
        market_avg_depth: roundForAdvisory(sellCallMarketContext.market_avg_depth, 2),
        market_best_call_score: roundForAdvisory(sellCallMarketContext.market_best_call_score, 2),
        market_best_put_score: roundForAdvisory(sellCallMarketContext.market_best_put_score, 6),
        market_call_iv_pct: roundForAdvisory(sellCallMarketContext.market_call_iv != null ? sellCallMarketContext.market_call_iv * 100 : null, 2),
        market_put_iv_pct: roundForAdvisory(sellCallMarketContext.market_put_iv != null ? sellCallMarketContext.market_put_iv * 100 : null, 2),
        put_call_iv_skew_pct: roundForAdvisory(sellCallMarketContext.market_skew != null ? sellCallMarketContext.market_skew * 100 : null, 2),
        put_call_iv_skew_basis: sellCallMarketContext.market_skew_basis,
        put_call_iv_skew_matched_pairs: sellCallMarketContext.market_skew_matched_pairs,
        put_call_iv_skew_unmatched_puts: sellCallMarketContext.market_skew_unmatched_puts,
        market_oi_delta_24h_pct: roundForAdvisory(sellCallMarketContext.market_oi_delta_24h_pct, 2),
        market_pc_oi: roundForAdvisory(sellCallMarketContext.market_pc_oi, 4),
        market_total_oi: roundForAdvisory(sellCallMarketContext.market_total_oi, 2),
        sell_call_candidates: sellCallMarketContext.sell_call_candidates,
        buy_put_candidates: sellCallMarketContext.buy_put_candidates,
        put_stress_warning: null,
      },
      research_context: callResearchContext ? {
        recommendation: callResearchContext.recommendation,
        preferred: callResearchContext.preferred,
        strong: callResearchContext.strong,
        dte_bucket: callResearchContext.dte_bucket,
        score_band: callResearchContext.score_band,
        edge_components: callResearchContext.edge_components,
        selection_multiplier: roundForAdvisory(callResearchContext.selection_multiplier, 4),
        selection_score: roundForAdvisory(callResearchContext.selection_score, 2),
        reason: callResearchContext.reason,
      } : null,
      current_detail: currentCall ? {
        instrument: currentCall.instrument,
        raw_score: roundForAdvisory(currentCall.score, 2),
        edge_score: roundForAdvisory(currentCall.selection_score, 2),
        delta: roundForAdvisory(currentCall.delta, 4),
        bid_price: roundForAdvisory(currentCall.bid_price, 4),
        ask_price: roundForAdvisory(currentCall.ask_price, 4),
        mark_price: roundForAdvisory(currentCall.mark_price, 4),
        bid_amount: roundForAdvisory(currentCall.bid_amount, 2),
        ask_amount: roundForAdvisory(currentCall.ask_amount, 2),
        spread_pct: roundForAdvisory(candidateSpreadPct({ askPrice: currentCall.ask_price, bidPrice: currentCall.bid_price, markPrice: currentCall.mark_price }), 2),
        spread_basis: 'bid_ask_difference_over_mark',
        quote_received_at: tickerMap?.[currentCall.instrument]?.quote_received_at || null,
        strike: currentCall.strike,
        expiry: currentCall.expiry,
        dte: roundForAdvisory(currentCall.dte, 1),
      } : null,
      prior_window_best_detail: priorCallBestDetail ? {
        timestamp: priorCallBestDetail.timestamp,
        instrument: priorCallBestDetail.instrument_name,
        delta: roundForAdvisory(Number(priorCallBestDetail.delta), 4),
        bid_price: roundForAdvisory(Number(priorCallBestDetail.bid_price), 4),
        strike: Number(priorCallBestDetail.strike),
        expiry: Number(priorCallBestDetail.expiry),
        dte: roundForAdvisory(Number(priorCallBestDetail.dte), 1),
      } : null,
      samples: priorCallScores.length,
    },
    spot_repricing_lag_context: repricingLag,
    recent_relative_value_context: recentRelativeValue,
    put_budget_context: {
      remaining: roundForAdvisory(Number(putBudgetRemaining), 2),
      budget_available: budgetAvailable,
      active_buy_put_rules: activeBuyPutRules,
      working_buy_put_actions: workingBuyPutActions,
      blocking_buy_put_actions: blockingBuyPutActions,
      resting_buy_put_orders: restingBuyPutOrders,
      has_working_buy_put: hasWorkingBuyPut,
    },
    spot_price_action: spotAction,
    action_pressure: {
      signal: actionSignal,
      requires_buy_put_decision: requiresDecision,
      supports_buy_put_review: Boolean(requiresDecision && (repricingLagSignal || ['downward', 'stable'].includes(spotAction.state))),
      execution_style: executionStyle,
      target_score: roundForAdvisory(targetScore, 6),
      suggested_instrument: currentPut?.instrument || null,
      suggested_limit_price: roundForAdvisory(suggestedLimitPrice, 4),
      score_nudge_pct: currentScore == null ? null : roundForAdvisory((scoreNudge - 1) * 100, 2),
      explanation: currentScore == null ? `PUT value review unavailable: ${quoteSummary.put.reason}` : requiresDecision
        ? actionSignal === 'spot_drop_option_repricing_lag'
          ? 'Current buy-put value score is locally spiking while spot is dropping, indicating a possible option-repricing lag before asks reset. This requires explicit review, not automatic execution.'
          : actionSignal === 'recent_relative_value'
            ? 'Current buy-put value score is materially good versus the recent local window, even though it is not a strict rolling-window best. This requires explicit review, not automatic execution.'
            : 'Current buy-put value score is strictly better than the prior rolling-window best while budget remains and no buy_put is pending or confirmed. This requires explicit review, not automatic execution.'
        : 'No fresh-best, spot-lag, or recent-relative buy_put review required.',
    },
  };
};

const formatRollingOptionValueContext = (context) => {
  if (!context?.put_value_context) return 'No rolling option value context available.';
  const put = context.put_value_context;
  const call = context.call_value_context || {};
  const budget = context.put_budget_context || {};
  const action = context.action_pressure || {};
  const spotAction = context.spot_price_action || {};
  const lag = context.spot_repricing_lag_context || {};
  const recent = context.recent_relative_value_context || {};
  const detail = put.current_detail;
  const selectedPut = put.selected_candidate_detail;
  const prior = put.prior_window_best_detail;
  const putResearch = put.research_context || {};
  const callDetail = call.current_detail;
  const callPrior = call.prior_window_best_detail;
  const callResearch = call.research_context || {};
  const callMarket = call.market_context || {};
  const availability = context.quote_availability || {};
  const formatAvailability = (label, value = {}) => `${label} quote availability: ${value.status || 'unknown'}; DTE/delta eligible=${value.in_dte_delta_count ?? 'n/a'}; positive ${value.quote_side || 'quote'}=${value.quoted_count ?? 'n/a'}; missing quotes=${value.missing_quote_count ?? 'n/a'}; coverage=${value.coverage_status || 'unknown'}; reason=${value.reason || 'not recorded'}.`;
  const formatDetection = value => value == null ? 'unavailable' : value ? 'yes' : 'no';
  return [
    `Quote input: tick=${availability.input_timestamp || 'unknown'}; received=${availability.quote_received_at_oldest || 'unknown'} through ${availability.quote_received_at_latest || 'unknown'}; evaluated=${availability.evaluated_at || 'unknown'}. Missing current quotes mean EDGE is unavailable, not zero; no relative-value conclusion follows from missing quotes.`,
    formatAvailability('PUT', put.availability),
    `Buy-put filters: delta ${JSON.stringify(context.buy_put_filters?.delta_range)}; DTE ${JSON.stringify(context.buy_put_filters?.dte_range)}; raw_score=${context.buy_put_filters?.raw_score}; edge_score=${context.buy_put_filters?.edge_score}.`,
    `Current PUT EDGE: ${put.current_score ?? 'unavailable'}${detail ? ` (raw=${detail.raw_score ?? 'n/a'}, ${detail.instrument}, delta=${detail.delta}, ask=$${detail.ask_price}, two-sided spread/mark=${detail.spread_pct ?? 'n/a'}%, DTE=${detail.dte}, quote=${detail.quote_received_at || 'unknown'})` : ''}.`,
    `Prior ${context.window_days}d best PUT EDGE: ${put.prior_window_best_score ?? 'n/a'}${prior ? ` (raw=${prior.raw_score ?? 'n/a'}, ${prior.instrument} at ${prior.timestamp})` : ''}.`,
    `Current PUT vs prior best: ${put.current_vs_prior_best_pct ?? 'n/a'}%; percentile=${put.percentile_vs_prior_window ?? 'n/a'}; strict_fresh_best=${formatDetection(put.is_strict_fresh_best)}; samples=${put.samples}.`,
    `PUT score trend: 1h=${put.trend_1h_pct ?? 'n/a'}%, 6h=${put.trend_6h_pct ?? 'n/a'}%, 24h=${put.trend_24h_pct ?? 'n/a'}%.`,
    `PUT EDGE selector: instrument=${selectedPut?.instrument || 'n/a'}; live-ask PUT EDGE=${selectedPut?.put_edge_score ?? 'n/a'}; selection_score=${putResearch.selection_score ?? 'n/a'}; formula=${putResearch.reason || 'n/a'}.`,
    `PUT crash scenarios (informational, not ranking): intrinsic payoff per premium dollar at 30/40/50% spot declines=${JSON.stringify(putResearch.edge_components?.shock_payoff_multiples || {})}. These are expiration payoffs, not predicted resale prices during an earlier crash.`,
    `Sell-call filters: delta ${JSON.stringify(context.sell_call_filters?.delta_range)}; DTE ${JSON.stringify(context.sell_call_filters?.dte_range)}; raw_score=${context.sell_call_filters?.raw_score}; edge_score=${context.sell_call_filters?.edge_score}.`,
    formatAvailability('CALL', call.availability),
    `Current CALL EDGE: ${call.current_score ?? 'unavailable'}${callDetail ? ` (raw=${callDetail.raw_score ?? 'n/a'}, ${callDetail.instrument}, delta=${callDetail.delta}, bid=$${callDetail.bid_price}, two-sided spread/mark=${callDetail.spread_pct ?? 'n/a'}%, DTE=${callDetail.dte}, quote=${callDetail.quote_received_at || 'unknown'})` : ''}.`,
    `Prior ${context.window_days}d best CALL EDGE: ${call.prior_window_best_score ?? 'n/a'}${callPrior ? ` (${callPrior.instrument} at ${callPrior.timestamp})` : ''}.`,
    `Current CALL vs prior best: ${call.current_vs_prior_best_pct ?? 'n/a'}%; percentile=${call.percentile_vs_prior_window ?? 'n/a'}; strict_fresh_best=${formatDetection(call.is_strict_fresh_best)}; samples=${call.samples ?? 0}.`,
    `CALL score trend: 1h=${call.trend_1h_pct ?? 'n/a'}%, 6h=${call.trend_6h_pct ?? 'n/a'}%, 24h=${call.trend_24h_pct ?? 'n/a'}%.`,
    `CALL market context (not part of CALL EDGE): avg_spread=${callMarket.market_avg_spread_pct ?? 'n/a'}%; best_put_score=${callMarket.market_best_put_score ?? 'n/a'}; matched-expiry/delta skew=${callMarket.put_call_iv_skew_pct ?? 'n/a'}% (${callMarket.put_call_iv_skew_matched_pairs ?? 0} matched puts); oi_24h=${callMarket.market_oi_delta_24h_pct ?? 'n/a'}%; pc_oi=${callMarket.market_pc_oi ?? 'n/a'}.`,
    `CALL EDGE normalization: edge_score=${callResearch.selection_score ?? 'n/a'}; raw_score=${callResearch.edge_components?.raw_score ?? 'n/a'}; DTE=${callResearch.edge_components?.dte ?? 'n/a'}; factor=${callResearch.edge_components?.normalization_factor ?? 'n/a'}; formula=${callResearch.reason || 'n/a'}.`,
    `Spot-lag repricing check: detected=${formatDetection(lag.is_detected)}; score_1h=${lag.score_trend_1h_pct ?? 'n/a'}%; spot_20m=${lag.spot_move_20m_pct ?? 'n/a'}%; near_best=${lag.current_vs_prior_best_pct ?? 'n/a'}%; reason=${lag.reason || 'n/a'}.`,
    `Recent-relative value check: detected=${formatDetection(recent.is_detected)}; lookback=${recent.lookback_hours ?? 'n/a'}h; samples=${recent.samples ?? 'n/a'}; percentile=${recent.percentile ?? 'n/a'}; vs_recent_avg=${recent.current_vs_recent_avg_pct ?? 'n/a'}%; vs_rolling_best=${recent.current_vs_rolling_best_pct ?? 'n/a'}%; reason=${recent.reason || 'n/a'}.`,
    `Put budget: remaining=$${budget.remaining ?? 'n/a'}; active_buy_put_rules=${budget.active_buy_put_rules ?? 0}; blocking_buy_put_actions=${budget.blocking_buy_put_actions ?? 0}; working_buy_put_actions=${budget.working_buy_put_actions ?? 0}; resting_buy_put_orders=${budget.resting_buy_put_orders ?? 0}.`,
    `Spot price action: ${spotAction.state || 'unknown'}${spotAction.slope_pct_6h != null ? ` (${spotAction.slope_pct_6h}% over recent 6h)` : ''}; reason=${spotAction.reason || 'n/a'}.`,
    `Buy-put review: signal=${action.signal || 'none'}; requires_buy_put_decision=${action.requires_buy_put_decision ? 'yes' : 'no'}; supports_buy_put_review=${action.supports_buy_put_review ? 'yes' : 'no'}; execution_style=${action.execution_style || 'n/a'}; target_score=${action.target_score ?? 'n/a'}; suggested_instrument=${action.suggested_instrument || 'n/a'}; suggested_limit_price=$${action.suggested_limit_price ?? 'n/a'}; score_nudge=${action.score_nudge_pct ?? 'n/a'}%.`,
  ].join('\n');
};

// Load private key (prefer env var, fallback to file)
const loadPrivateKey = () => {
  if (process.env.PRIVATE_KEY) {
    return process.env.PRIVATE_KEY.trim();
  }
  try {
    const privateKey = fs.readFileSync('./.private_key.txt', 'utf8').trim();
    if (!privateKey) throw new Error('Private key is empty.');
    return privateKey;
  } catch (error) {
    console.error('Error loading private key:', error.message);
    process.exit(1);
  }
};

// Create wallet
const createWallet = () => {
  return new ethers.Wallet(loadPrivateKey());
};

// Sign message
const signMessage = async (wallet, timestamp) => {
  try {
    return await wallet.signMessage(timestamp.toString());
  } catch (error) {
    console.error('Error generating signature:', error.message);
    throw error;
  }
};

// Encode trade data
function encodeTradeData(order, assetAddress, optionSubId) {
  console.log({
    "asset_address": assetAddress,
    "option_sub_id": optionSubId,
    "order_limit_price": ethers.parseUnits(order.limit_price.toString(), 18), 
    "order_amount": ethers.parseUnits(order.amount.toString(), 18), 
    "order_max_fee": ethers.parseUnits(order.max_fee.toString(), 18), 
    "order_subaccount_id": order.subaccount_id, 
    "order_direction": order.direction === 'buy'
  });
  let encoded_data = encoder.encode( // same as "encoded_data" in public/order_debug
    ['address', 'uint','int', 'int', 'uint', 'uint', 'bool'],
    [
      assetAddress,
      optionSubId,
      ethers.parseUnits(order.limit_price.toString(), 18), 
      ethers.parseUnits(order.amount.toString(), 18), 
      ethers.parseUnits(order.max_fee.toString(), 18), 
      order.subaccount_id, 
      order.direction === 'buy'
    ]);

    let encoded_data_hashed = ethers.keccak256(Buffer.from(encoded_data.slice(2), 'hex')); 
    // console.log({ encoded_data_hashed });
    return encoded_data_hashed; // same as "encoded_data_hashed" in public/order_debug
}

// Place order function
const placeOrder = async (name, amount, direction = 'buy', price, assetAddress, optionSubId, reduceOnly = true, timeInForce = 'ioc', instrument = null, beforeSend = null) => {
  let limitPriceString = String(price);
  try {
    const wallet = createWallet();
    const timestamp = Date.now(); // Current UTC timestamp in ms
    const signature = await signMessage(wallet, timestamp);
    const signatureExpirySec = Math.floor((Date.now() / 1000) + (timeInForce === 'ioc' ? 900 : 86400));
    const orderPrice = normalizeOrderPriceForVenue(price, instrument, direction);
    const step = orderPrice.step;
    const limitPrice = orderPrice.price;
    limitPriceString = limitPrice.toFixed(getStepDecimals(step));

    const order = {
        instrument_name: name,
        subaccount_id: SUBACCOUNT_ID,
        direction,
        limit_price: limitPriceString,
        amount: formatVenueOrderAmount(amount),
        // Give IOC orders extra buffer for signer/exchange clock skew. Derive rejects borderline 300s expiries.
        signature_expiry_sec: signatureExpirySec, // IOC: 15min, GTC/post_only: 24h
        max_fee: Math.max(0.08 * limitPrice, 10.0).toFixed(2).toString(), // Max fee per unit of volume (USDC). Generous ceiling — actual fee is much lower (~0.1% of notional)
        // Noop submits sparse discretionary orders, not continuous maker quotes.
        // Venue-side MMP has been causing opaque sell-order cancellations and poor reconciliation.
        mmp: false,
        nonce: parseInt(`${timestamp}${Math.floor(Math.random() * 1000)}`),
        signer: wallet.address,
        order_type: 'limit',
        reduce_only: reduceOnly,
        time_in_force: timeInForce,
        ...(timeInForce === 'post_only' ? { post_only: true } : {}),
    };

    const tradeModuleData = encodeTradeData(order, assetAddress, optionSubId)

    const actionHash = ethers.keccak256(
        encoder.encode(
          ['bytes32', 'uint256', 'uint256', 'address', 'bytes32', 'uint256', 'address', 'address'], 
          [
            ACTION_TYPEHASH, 
            order.subaccount_id, 
            order.nonce, 
            TRADE_MODULE_ADDRESS, 
            tradeModuleData, 
            order.signature_expiry_sec, 
            DERIVE_ACCOUNT_ADDRESS, 
            order.signer
          ]
        )
    );

    order.signature = wallet.signingKey.sign(
        ethers.keccak256(Buffer.concat([
          Buffer.from("1901", "hex"), 
          Buffer.from(DOMAIN_SEPARATOR.slice(2), "hex"), 
          Buffer.from(actionHash.slice(2), "hex")
        ]))
    ).serialized;

    if (beforeSend) beforeSend(order);
    const response = await axios.post(
      API_URL.PLACE_ORDER,
      order,
      {
        headers: {
          'X-LyraWallet': DERIVE_ACCOUNT_ADDRESS,
          'X-LyraTimestamp': timestamp.toString(),
          'X-LyraSignature': signature,
        },
      }
    );
    
    if (response.data.error) {
      const errMsg = stringifyApiError(response.data.error);
      if (timeInForce === 'ioc' && isIocNoLiquidityError(response.data.error)) {
        console.log(`⚠️ Zero fill: ${name} @ $${limitPriceString} [IOC] — venue found no liquidity inside limit`);
        return { zero_fill_rejected: true, error: errMsg };
      }
      // Detect post_only rejection (would cross the book)
      if (timeInForce === 'post_only' && /(?:post[_ -]?only.*(?:cross|would|take))|(?:(?:cross|would).*post[_ -]?only)/i.test(errMsg)) {
        console.log(`📋 post_only rejected for ${name}: would cross the book`);
        return { rejected_post_only: true, error: errMsg };
      }
      console.error(`Error placing limit order for ${name}:`, errMsg);
      return { placement_error: errMsg, definitive_rejection: isDefinitiveVenueRejection(response.data.error) };
    }
    // Check for cancelled IOC (cancel_reason in response indicates immediate cancel)
    const orderResult = extractOrderRecord(response.data?.result || response.data) || response.data?.result || response.data;
    if (orderResult?.order_status === 'cancelled' && orderResult?.cancel_reason) {
      console.log(`📋 Order ${name} immediately cancelled: ${orderResult.cancel_reason}`);
    }
    console.log(`Order placed successfully:`, JSON.stringify(orderResult).slice(0, 300));
    return response.data;
  } catch (error) {
    const status = error.response?.status;
    const errBody = error.response?.data;
    // Detect post_only rejection from HTTP error
    if (timeInForce === 'post_only' && errBody) {
      const errStr = typeof errBody === 'string' ? errBody : JSON.stringify(errBody);
      if (/(?:post[_ -]?only.*(?:cross|would|take))|(?:(?:cross|would).*post[_ -]?only)/i.test(errStr)) {
        console.log(`📋 post_only rejected for ${name}: would cross the book`);
        return { rejected_post_only: true, error: errStr };
      }
    }
    if (timeInForce === 'ioc' && isIocNoLiquidityError(errBody)) {
      const errStr = stringifyApiError(errBody);
      console.log(`⚠️ Zero fill: ${name} @ $${limitPriceString} [IOC] — venue found no liquidity inside limit`);
      return { zero_fill_rejected: true, error: errStr };
    }
    const bodyStr = errBody ? stringifyApiError(errBody).slice(0, 300) : 'no body';
    console.error(`Error placing limit order for ${name}: ${error.message} | status: ${status || 'N/A'} | body: ${bodyStr}`);
    return { placement_error: `status=${status || 'N/A'} ${error.message} | body: ${bodyStr}`,
      definitive_rejection: isDefinitiveVenueRejection(errBody?.error) };
  }
};

// Fetch all open (resting) orders from Derive
// Malformed-request codes and Derive's rate limiter (HTTP 500, -32000
// "Rate limit exceeded ... Retry after N ms") both mean the order was never accepted.
const isDefinitiveVenueRejection = (apiError) => (
  [-32600, -32602].includes(Number(apiError?.code)) || /rate limit exceeded/i.test(String(apiError?.message || ''))
);

const venueHasNonce = async (nonce, sinceMs) => {
  const wanted = String(nonce);
  const open = await fetchOpenOrders({ throwOnError: true });
  if (open.some(order => String(order.nonce) === wanted)) return true;
  const wallet = createWallet();
  const timestamp = Date.now();
  const signature = await signMessage(wallet, timestamp);
  for (let page = 1; page <= 20; page++) {
    const response = await axios.post(API_URL.GET_ORDER_HISTORY, {
      subaccount_id: SUBACCOUNT_ID, from_timestamp: Math.max(0, sinceMs - 10 * 60 * 1000), page, page_size: 100,
    }, {
      headers: { 'X-LyraWallet': DERIVE_ACCOUNT_ADDRESS,
        'X-LyraTimestamp': timestamp.toString(), 'X-LyraSignature': signature },
      timeout: 10000,
    });
    if (response.data?.error) throw new Error(`Order history unavailable: ${stringifyApiError(response.data.error)}`);
    const records = extractOrderRecords(response.data?.result);
    if (records.some(order => String(order.nonce) === wanted)) return true;
    if (records.length < 100) return false;
  }
  throw new Error('Order history scan incomplete; reconciliation required');
};

const fetchOpenOrders = async (options = {}) => {
  const throwOnError = Boolean(options.throwOnError);
  try {
    const wallet = createWallet();
    const timestamp = Date.now();
    const signature = await signMessage(wallet, timestamp);
    const response = await axios.post(API_URL.GET_OPEN_ORDERS, {
      subaccount_id: SUBACCOUNT_ID,
    }, {
      headers: {
        'X-LyraWallet': DERIVE_ACCOUNT_ADDRESS,
        'X-LyraTimestamp': timestamp.toString(),
        'X-LyraSignature': signature,
      },
      timeout: 10000,
    });
    // Defensive: result could be array directly, or { orders: [...] }, or { result: { orders: [...] } }
    const raw = response.data?.result;
    const orders = Array.isArray(raw) ? raw : raw?.orders;
    if (response.data?.error || !Array.isArray(orders)) throw new Error('Open order state unavailable');
    return orders.map(o => ({
      order_id: o.order_id,
      instrument_name: o.instrument_name,
      direction: o.direction,
      amount: o.amount,
      filled_amount: o.filled_amount,
      limit_price: o.limit_price,
      average_price: o.average_price,
      order_status: o.order_status,
      nonce: o.nonce,
      time_in_force: o.time_in_force,
      creation_timestamp: o.creation_timestamp,
      last_update_timestamp: o.last_update_timestamp,
    }));
  } catch (error) {
    console.error(`❌ fetchOpenOrders failed: ${error.message}`);
    if (throwOnError) throw error;
    return [];
  }
};

const extractOrderRecord = (payload) => {
  if (!payload || typeof payload !== 'object') return null;
  if (payload.order && typeof payload.order === 'object') return payload.order;
  if (payload.result && typeof payload.result === 'object') return extractOrderRecord(payload.result);
  if (payload.order_id || payload.instrument_name || payload.order_status) return payload;
  return null;
};

const extractOrderRecords = (payload) => {
  if (!payload) return [];
  if (Array.isArray(payload)) return payload.map(extractOrderRecord).filter(Boolean);
  if (Array.isArray(payload.orders)) return payload.orders.map(extractOrderRecord).filter(Boolean);
  if (payload.result && typeof payload.result === 'object') return extractOrderRecords(payload.result);
  const single = extractOrderRecord(payload);
  return single ? [single] : [];
};

const stringifyApiError = (err) => {
  if (!err) return 'unknown error';
  if (typeof err === 'string') return err;
  try {
    return JSON.stringify(err);
  } catch {
    return String(err);
  }
};

const isIocNoLiquidityError = (err) => {
  const code = Number(err?.code ?? err?.error?.code);
  if (code === 11009) return true;
  const text = stringifyApiError(err).toLowerCase();
  return text.includes('zero liquidity for market or ioc/fok order')
    || text.includes('no liquidity within the provided limit price');
};

const getOrderTrades = (payload) => {
  if (!payload || typeof payload !== 'object') return [];
  if (Array.isArray(payload.trades)) return payload.trades;
  if (payload.result && typeof payload.result === 'object') return getOrderTrades(payload.result);
  if (payload.order && typeof payload.order === 'object') return getOrderTrades(payload.order);
  return [];
};

const ordersRoughlyMatch = (tracked, exchange) => {
  if (!tracked || !exchange) return false;
  if (tracked.instrument_name !== exchange.instrument_name) return false;
  if (tracked.direction !== exchange.direction) return false;

  const trackedPrice = Number(tracked.limit_price || 0);
  const exchangePrice = Number(exchange.limit_price || 0);
  const trackedAmount = Number(tracked.amount || 0);
  const exchangeAmount = Number(exchange.amount || 0);

  return Math.abs(trackedPrice - exchangePrice) < 1e-9
    && Math.abs(trackedAmount - exchangeAmount) < 1e-6;
};

const fetchOrderHistoryRecord = async (orderId) => {
  const wallet = createWallet();
  const timestamp = Date.now();
  const signature = await signMessage(wallet, timestamp);
  const seen = new Set();
  for (let page = 1; page <= 100; page++) {
    const response = await axios.post(API_URL.GET_ORDER_HISTORY, {
      subaccount_id: SUBACCOUNT_ID, from_timestamp: 0, page, page_size: 100,
    }, {
      headers: { 'X-LyraWallet': DERIVE_ACCOUNT_ADDRESS,
        'X-LyraTimestamp': timestamp.toString(), 'X-LyraSignature': signature },
      timeout: 10000,
    });
    if (response.data?.error) throw new Error(`Order history unavailable: ${stringifyApiError(response.data.error)}`);
    const payload = response.data?.result;
    if (!payload || (!Array.isArray(payload) && !Array.isArray(payload.orders))) {
      throw new Error('Malformed order history response');
    }
    const records = extractOrderRecords(payload);
    const found = records.find((record) => record.order_id === orderId);
    if (found) return found;
    if (records.length < 100) return null;
    const pageKey = records.map(record => record.order_id).join(',');
    if (seen.has(pageKey)) throw new Error('Order history pagination did not advance');
    seen.add(pageKey);
  }
  throw new Error('Order history scan incomplete; reconciliation required');
};

// Fetch a specific order's final status from Derive (for fill reconciliation)
const fetchOrderStatus = async (orderId) => {
  try {
    const wallet = createWallet();
    const timestamp = Date.now();
    const signature = await signMessage(wallet, timestamp);
    const response = await axios.post('https://api.lyra.finance/private/get_order', {
      subaccount_id: SUBACCOUNT_ID,
      order_id: orderId,
    }, {
      headers: {
        'X-LyraWallet': DERIVE_ACCOUNT_ADDRESS,
        'X-LyraTimestamp': timestamp.toString(),
        'X-LyraSignature': signature,
      },
      timeout: 10000,
    });
    const errBody = response.data?.error;
    if (errBody) {
      const code = Number(errBody?.code);
      const message = String(errBody?.message || '');
      if (code === 11006 || message.includes('Does not exist')) {
        const historyRecord = await fetchOrderHistoryRecord(orderId);
        if (historyRecord) {
          return {
            ...historyRecord,
          order_id: historyRecord.order_id,
            order_status: historyRecord.order_status,
            amount: Number(historyRecord.amount || 0),
            filled_amount: historyRecord.filled_amount,
            average_price: historyRecord.average_price,
            cancel_reason: historyRecord.cancel_reason || null,
          };
        }
        return null; // Missing history is uncertainty, not proof of cancellation.
      }
      console.error(`❌ fetchOrderStatus ${orderId} venue error: ${stringifyApiError(errBody)}`);
      return null;
    }
    let raw = extractOrderRecord(response.data?.result || response.data);
    if (!raw) {
      raw = await fetchOrderHistoryRecord(orderId);
      if (!raw) {
        const bodyStr = JSON.stringify(response.data || {}).slice(0, 300);
        console.warn(`⚠️ fetchOrderStatus ${orderId}: no direct order record; history fallback also empty | body: ${bodyStr}`);
        return null;
      }
    }
    return {
      ...raw,
      order_id: raw.order_id,
      order_status: raw.order_status, // 'open'|'filled'|'cancelled'|'expired'
      amount: Number(raw.amount || 0),
      filled_amount: raw.filled_amount,
      average_price: raw.average_price,
      cancel_reason: raw.cancel_reason || null,
    };
  } catch (error) {
    const errBody = error.response?.data;
    const bodyStr = errBody ? (typeof errBody === 'string' ? errBody.slice(0, 300) : JSON.stringify(errBody).slice(0, 300)) : 'no body';
    const code = Number(errBody?.error?.code);
    const message = String(errBody?.error?.message || '');
    if (code === 11006 || message.includes('Does not exist')) {
      const historyRecord = await fetchOrderHistoryRecord(orderId);
      if (historyRecord) {
        return {
          ...historyRecord,
          order_id: historyRecord.order_id,
          order_status: historyRecord.order_status,
          amount: Number(historyRecord.amount || 0),
          filled_amount: historyRecord.filled_amount,
          average_price: historyRecord.average_price,
          cancel_reason: historyRecord.cancel_reason || null,
        };
      }
      return null; // Missing history is uncertainty, not proof of cancellation.
    }
    console.error(`❌ fetchOrderStatus ${orderId} failed: ${error.message} | status: ${error.response?.status || 'N/A'} | body: ${bodyStr}`);
    return null;
  }
};

// Cancel a specific order on Derive
const cancelOrder = async (orderId, instrumentName) => {
  try {
    const wallet = createWallet();
    const timestamp = Date.now();
    const signature = await signMessage(wallet, timestamp);
    const response = await axios.post(API_URL.CANCEL_ORDER, {
      subaccount_id: SUBACCOUNT_ID,
      order_id: orderId,
      instrument_name: instrumentName,
    }, {
      headers: {
        'X-LyraWallet': DERIVE_ACCOUNT_ADDRESS,
        'X-LyraTimestamp': timestamp.toString(),
        'X-LyraSignature': signature,
      },
      timeout: 10000,
    });
    if (response.data?.error) {
      console.error(`❌ cancelOrder ${orderId}: ${stringifyApiError(response.data.error)}`);
      return null;
    }
    console.log(`🗑️ Cancelled order ${orderId} (${instrumentName})`);
    return response.data?.result;
  } catch (error) {
    const errBody = error.response?.data;
    const bodyStr = errBody ? stringifyApiError(errBody).slice(0, 300) : 'no body';
    console.error(`❌ cancelOrder ${orderId} failed: ${error.message} | status: ${error.response?.status || 'N/A'} | body: ${bodyStr}`);
    return null;
  }
};

// Fetch current open positions from Derive
const fetchPositions = async (options = {}) => {
  try {
    const wallet = createWallet();
    const timestamp = Date.now();
    const signature = await signMessage(wallet, timestamp);
    const response = await axios.post('https://api.lyra.finance/private/get_positions', {
      subaccount_id: SUBACCOUNT_ID,
    }, {
      headers: {
        'X-LyraWallet': DERIVE_ACCOUNT_ADDRESS,
        'X-LyraTimestamp': timestamp.toString(),
        'X-LyraSignature': signature,
      },
      timeout: 10000,
    });
    const raw = response.data?.result;
    const positions = Array.isArray(raw) ? raw : raw?.positions;
    if (response.data?.error || !Array.isArray(positions) || positions.some(position =>
      !position?.instrument_name || position.amount == null || position.amount === '' || !Number.isFinite(Number(position.amount)))) {
      throw new Error('Position state unavailable or malformed');
    }
    return positions
      .filter(p => Math.abs(Number(p.amount)) > 0)
      .map(p => ({
        instrument_name: p.instrument_name,
        direction: Number(p.amount) > 0 ? 'long' : 'short',
        amount: Math.abs(Number(p.amount)),
        avg_entry_price: Number(p.average_price) || null,
        mark_price: Number(p.mark_price) || null,
        index_price: Number(p.index_price) || null,
        unrealized_pnl: Number(p.unrealized_pnl) || null,
        theta: Number(p.greeks?.theta) || null,
        delta: Number(p.greeks?.delta) || null,
        vega: Number(p.greeks?.vega) || null,
      }));
  } catch (e) {
    console.log('📋 Failed to fetch positions:', e.message);
    if (options.throwOnError) throw e;
    return [];
  }
};

const fetchTradeHistory = async (fromTimestampMs, toTimestampMs) => {
  try {
    const wallet = createWallet();
    const timestamp = Date.now();
    const signature = await signMessage(wallet, timestamp);
    const body = {
      subaccount_id: SUBACCOUNT_ID,
      from_timestamp: fromTimestampMs,
      page_size: 250,
    };
    if (toTimestampMs) body.to_timestamp = toTimestampMs;
    const response = await axios.post(API_URL.GET_TRADE_HISTORY, body, {
      headers: {
        'X-LyraWallet': DERIVE_ACCOUNT_ADDRESS,
        'X-LyraTimestamp': timestamp.toString(),
        'X-LyraSignature': signature,
      },
      timeout: 15000,
    });
    const raw = response.data?.result;
    return Array.isArray(raw) ? raw : (raw?.trades || []);
  } catch (e) {
    console.log(`📋 Failed to fetch trade history for review recovery: ${e.message}`);
    return [];
  }
};

// Fetch collaterals (USDC/ETH balances) from Derive
const fetchCollaterals = async () => {
  try {
    const wallet = createWallet();
    const timestamp = Date.now();
    const signature = await signMessage(wallet, timestamp);
    const response = await axios.post('https://api.lyra.finance/private/get_collaterals', {
      subaccount_id: SUBACCOUNT_ID,
    }, {
      headers: {
        'X-LyraWallet': DERIVE_ACCOUNT_ADDRESS,
        'X-LyraTimestamp': timestamp.toString(),
        'X-LyraSignature': signature,
      },
      timeout: 10000,
    });
    const raw = response.data?.result;
    const collaterals = Array.isArray(raw) ? raw : (raw?.collaterals || []);
    return collaterals.map(c => ({
      asset_name: c.asset_name,
      amount: Number(c.amount ?? 0),
      mark_price: Number(c.mark_price ?? 0),
      value_usd: Number(c.mark_value ?? c.value ?? 0),
    }));
  } catch (e) {
    console.log('📋 Failed to fetch collaterals:', e.message);
    return [];
  }
};

// Fetch subaccount margin state from Derive (leverage, margin, liquidation)
const fetchSubaccount = async ({ forObservation = false } = {}) => {
  try {
    const wallet = createWallet();
    const timestamp = Date.now();
    const signature = await signMessage(wallet, timestamp);
    const response = await axios.post(API_URL.GET_SUBACCOUNT, {
      subaccount_id: SUBACCOUNT_ID,
    }, {
      headers: {
        'X-LyraWallet': DERIVE_ACCOUNT_ADDRESS,
        'X-LyraTimestamp': timestamp.toString(),
        'X-LyraSignature': signature,
      },
      timeout: 10000,
    });
    const r = response.data?.result;
    if (forObservation) {
      if (response.data?.error || !r || String(r.subaccount_id) !== String(SUBACCOUNT_ID)) {
        throw new Error('Portfolio observation account unavailable or mismatched');
      }
      // A single raw response supplies valuation, positions and collateral.
      // Portfolio observation accepts finite zero/negative equity; new exposure
      // continues through the stricter margin gate below.
      return { ...r, snapshot_received_at: new Date().toISOString() };
    }
    if (!r || !['initial_margin', 'maintenance_margin', 'subaccount_value'].every(field => (
      r[field] !== null && r[field] !== undefined && Number.isFinite(Number(r[field]))
    )) || !(Number(r.subaccount_value) > 0) || typeof r.is_under_liquidation !== 'boolean') {
      throw new Error('Subaccount response is missing required margin fields');
    }
    const collateralRows = Array.isArray(r?.collaterals) ? r.collaterals : [];
    const positionRows = Array.isArray(r?.positions) ? r.positions : [];
    const aggregatedCollateralsMaintenanceMargin = collateralRows.reduce((sum, row) => (
      sum + Math.abs(Number(row?.maintenance_margin ?? 0))
    ), 0);
    const aggregatedPositionsInitialMargin = positionRows.reduce((sum, row) => (
      sum + Math.abs(Number(row?.initial_margin ?? 0))
    ), 0);
    return {
      initial_margin: Number(r?.initial_margin ?? 0),        // available margin (≈ buying power)
      maintenance_margin: Number(r?.maintenance_margin ?? 0), // available before liquidation
      subaccount_value: Number(r?.subaccount_value ?? 0),
      positions_value: Number(r?.positions_value ?? 0),
      collaterals_value: Number(r?.collaterals_value ?? 0),
      collaterals_initial_margin: Number(r?.collaterals_initial_margin ?? 0),
      collaterals_maintenance_margin: Number(r?.collaterals_maintenance_margin ?? 0),
      aggregated_collaterals_maintenance_margin: aggregatedCollateralsMaintenanceMargin,
      positions_initial_margin: Number(r?.positions_initial_margin ?? 0),   // margin consumed by positions
      aggregated_positions_initial_margin: aggregatedPositionsInitialMargin,
      positions_maintenance_margin: Number(r?.positions_maintenance_margin ?? 0),
      open_orders_margin: Number(r?.open_orders_margin ?? 0),
      margin_usage_pct: Number(
        r?.margin_usage_pct ??
        r?.margin_utilization_pct ??
        r?.margin_utilization ??
        NaN
      ),
      is_under_liquidation: r?.is_under_liquidation || false,
    };
  } catch (e) {
    console.log('📋 Failed to fetch subaccount:', e.message);
    return null;
  }
};

// Fetch all instruments once and filter for both strategies
const fetchAndFilterInstruments = async (spotPrice, { throwOnError = false } = {}) => {
  try {
    console.log('🔍 Fetching all instruments...');
    const response = await axios.post(API_URL.GET_INSTRUMENTS, {
      currency: 'ETH',
      expired: false,
      instrument_type: 'option'
    }, throwOnError ? { timeout: 15000 } : undefined);

    if (!response.data.result) {
      if (throwOnError) throw new Error('Instrument response unavailable');
      console.log('No instruments found');
      return { putCandidates: [], callCandidates: [] };
    }

    const instruments = response.data.result;
    console.log(`📊 Found ${instruments.length} total instruments`);

    // Filter for OTM put candidates; delta and value discipline are applied after ticker enrichment.
    const putCandidates = instruments.filter(instrument => {
      const expiration = new Date(instrument.option_details.expiry * 1000);
      const now = new Date();
      const daysToExpiry = Math.ceil((expiration - now) / (1000 * 60 * 60 * 24));
      const strikePrice = parseFloat(instrument.option_details.strike);
      
      return daysToExpiry >= PUT_EXPIRATION_RANGE[0] && 
             daysToExpiry <= PUT_EXPIRATION_RANGE[1] &&
             instrument.instrument_type === 'option' &&
             instrument.option_details.option_type === 'P' &&
             (!spotPrice || strikePrice < spotPrice);
    });

    // Filter for call candidates (positive delta, shorter expiration, strike > 1.10 * spot)
    const callCandidates = instruments.filter(instrument => {
      const expiration = new Date(instrument.option_details.expiry * 1000);
      const now = new Date();
      const daysToExpiry = Math.ceil((expiration - now) / (1000 * 60 * 60 * 24));
      const strikePrice = parseFloat(instrument.option_details?.strike);
      
      return daysToExpiry >= CALL_EXPIRATION_RANGE[0] && 
             daysToExpiry <= CALL_EXPIRATION_RANGE[1] &&
             instrument.instrument_type === 'option' &&
             instrument.option_details.option_type === 'C' &&
             (!spotPrice || strikePrice > spotPrice);
    });

    console.log(`📈 Put candidates: ${putCandidates.length} | Call candidates: ${callCandidates.length}`);
     
    return { instruments, putCandidates, callCandidates };
  } catch (error) {
    const status = error.response?.status;
    console.error(`Error fetching instruments: ${error.message} | status: ${status || 'N/A'}${status === 429 ? ' (RATE LIMITED)' : ''}`);
    if (throwOnError) throw error;
    return { putCandidates: [], callCandidates: [] };
  }
};

// handleBuyingPuts — removed (replaced by LLM-driven trading system)

// executeCallSellOrder — removed (replaced by LLM-driven trading system)

// handleSellingCalls — removed (replaced by LLM-driven trading system)

// executePutBuyOrder — removed (replaced by LLM-driven trading system)

// ─── Hypothesis Review Cycle ──────────────────────────────────────────────────

let _reviewInFlight = false;
const reviewExpiredHypotheses = async () => {
  if (_reviewInFlight) return;
  _reviewInFlight = true;
  try {
    const pending = db.getPendingHypotheses(3); // max 3 per tick
    if (pending.length === 0) return;

  console.log(`🔍 Reviewing ${pending.length} expired hypothesis(es)...`);

  for (const hyp of pending) {
    try {
      // Gather actual market data from the hypothesis window
      const createdAt = hyp.timestamp;
      const deadline = hyp.prediction_deadline;
      const priceData = db.getRecentSpotPrices(createdAt)
        .filter(p => p.timestamp <= deadline)
        .reverse(); // chronological

      const ordersInWindow = db.getOrdersInWindow(createdAt, deadline);
      const totalPnl = ordersInWindow.reduce((sum, o) => {
        const val = o.total_value || 0;
        return sum + (o.action === 'buy_put' ? -val : val);
      }, 0);

      const priceAtStart = priceData.length > 0 ? priceData[0].price : null;
      const priceAtEnd = priceData.length > 0 ? priceData[priceData.length - 1].price : null;
      const minPrice = priceData.length > 0 ? Math.min(...priceData.map(p => p.price)) : null;
      const maxPrice = priceData.length > 0 ? Math.max(...priceData.map(p => p.price)) : null;

      const reviewPrompt = `You are reviewing a past hypothesis for accuracy and risk quality.

## Hypothesis (ID #${hyp.id})
Created: ${createdAt}
Deadline: ${deadline}
Prediction: ${hyp.prediction_target} will go ${hyp.prediction_direction} ${hyp.prediction_value}
Falsification: ${hyp.falsification_criteria}

Content:
${hyp.content}

## What Actually Happened
Price at hypothesis time: $${priceAtStart}
Price at deadline: $${priceAtEnd}
Price range during window: $${minPrice} - $${maxPrice}
Data points: ${priceData.length}

## Trades During Window
${ordersInWindow.length > 0 ? ordersInWindow.map(o => `${o.timestamp}: ${o.action} ${o.instrument_name} amount=${o.filled_amount} price=${o.fill_price} value=$${o.total_value}`).join('\n') : 'No trades executed'}
Total P&L attribution: $${totalPnl.toFixed(4)}

## Scoring Instructions

Score this hypothesis using Spitznagel-aligned categories. The goal is NOT prediction accuracy — it's whether the hypothesis identified a genuine mispricing in protection cost, rich survivable call premium, or another asymmetric options-market setup. Whether ETH moved up or down is a second-order effect: notice it, but judge the hypothesis first by bang-for-buck risk mitigation and paid smart-risk opportunity.

Categories:
- confirmed_convex: Hypothesis identified a genuine mispricing in protection cost, and acting on it would have been asymmetric (bought cheap insurance before it got expensive)
- confirmed_linear: Hypothesis was directionally right but didn't identify convexity or rich premium — the opportunity was symmetric, not asymmetric
- disproven_bounded: Hypothesis was wrong but the implied action (buying cheap puts) had bounded cost — THIS IS FINE, this IS the strategy. Cheap insurance that expires worthless is the expected outcome.
- disproven_costly: Hypothesis led to buying expensive protection (chasing high IV) or missing a cheaper window — overpaid for insurance
- partially_confirmed: Direction right but timing/magnitude was off

IMPORTANT: Most hypotheses SHOULD be disproven_bounded. That means the insurance was cheap and the bleed was small. A high disproven_bounded rate is GOOD — it means the bot is buying cheap protection consistently.

Output ONLY this JSON:
{"status":"<category>","confidence":<0-1>,"verdict":"<2-3 sentence explanation focusing on whether protection was cheap/expensive, call premium was rich/thin, and the bleed or risk was well paid, not just whether the price moved correctly>"}`;

      const response = await axios.post('https://api.anthropic.com/v1/messages', {
        model: ANTHROPIC_SONNET_MODEL,
        thinking: { type: 'disabled' },
        max_tokens: 512,
        messages: [{ role: 'user', content: reviewPrompt }],
      }, {
        headers: {
          'x-api-key': process.env.ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01',
          'content-type': 'application/json',
        },
        timeout: 30000,
      });

      const resultText = getAnthropicResponseText(response.data);
      const result = extractJSON(resultText);
      if (result && result.status && result.verdict) {
        db.updateHypothesisVerdict(hyp.id, {
          status: result.status,
          verdict: result.verdict,
          confidence: result.confidence,
          tradePnl: totalPnl,
          tradeIds: ordersInWindow.map(o => o.id),
        });
        console.log(`📊 Hypothesis #${hyp.id}: ${result.status} (${(result.confidence * 100).toFixed(0)}%)`);
      } else {
        console.log(`📊 Hypothesis #${hyp.id}: failed to parse verdict`);
      }
    } catch (e) {
      console.log(`📊 Hypothesis #${hyp.id} review failed:`, e.message);
    }
  }
  } finally { _reviewInFlight = false; }
};

const extractHypothesisLessons = async () => {
  if (!process.env.ANTHROPIC_API_KEY || !db) return { processed: 0, advancedToId: botData.lastHypothesisLessonReviewId || 0 };
  const reviewed = db.getReviewedHypothesesSinceId
    ? db.getReviewedHypothesesSinceId(botData.lastHypothesisLessonReviewId || 0, 8)
    : db.getReviewedHypotheses(8);
  if (!reviewed || reviewed.length < 4) return { processed: 0, advancedToId: botData.lastHypothesisLessonReviewId || 0 };

  console.log(`🧠 Extracting lessons from ${reviewed.length} hypothesis reviews (after id ${botData.lastHypothesisLessonReviewId || 0})...`);

  const currentLessons = db.getActiveLessons().slice(0, 8);

  const prompt = `You are analyzing hypothesis review outcomes to extract actionable lessons for a Spitznagel-style tail-risk hedging bot.

## Reviewed Hypotheses (most recent first)
${reviewed.map(h => {
  const hypothesis = String(h.content || '').replace(/\s+/g, ' ').slice(0, 85);
  const verdict = String(h.outcome_verdict || '').replace(/\s+/g, ' ').slice(0, 110);
  return `#${h.id} [${h.outcome_status}] conf=${h.outcome_confidence ?? 'n/a'} | hyp=${hypothesis} | verdict=${verdict}`;
}).join('\n')}

## Current Active Lessons
${currentLessons.length > 0 ? currentLessons.map(l => `- ${String(l.lesson || '').slice(0, 140)} (evidence: ${l.evidence_count})`).join('\n') : 'None yet'}

## Instructions
Analyze the pattern of outcomes. Key metric: convex posture rate = (confirmed_convex + disproven_bounded) / total.

Extract at most 3 actionable lessons about:
1. Which conditions reliably produce cheap protection windows (low IV, compressed skew, stable price)?
2. Which signals preceded put price spikes (meaning we should have bought before)?
3. What's the average bleed rate on disproven_bounded hypotheses (lower = better insurance buying)?
4. Which hypothesis types to avoid (high disproven_costly rate — chasing expensive protection)

For each existing lesson, say whether it still holds or should be archived.

Output JSON:
{"new_lessons":[{"lesson":"<text>","evidence_count":<number>}],"archive_ids":[<ids of lessons that no longer hold>]}`;

  try {
    const response = await axios.post('https://api.anthropic.com/v1/messages', {
      model: ANTHROPIC_SONNET_MODEL,
      thinking: { type: 'disabled' },
      max_tokens: 500,
      messages: [{ role: 'user', content: prompt }],
    }, {
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      timeout: 45000,
    });

    const text = getAnthropicResponseText(response.data);
    const result = extractJSON(text);
    if (result && result.new_lessons) {
      for (const lesson of (result.new_lessons || [])) {
        db.insertLesson(lesson.lesson, lesson.evidence_count || 0);
      }
      for (const id of (result.archive_ids || [])) {
        db.archiveLesson(id);
      }
      const advancedToId = reviewed[reviewed.length - 1]?.id || botData.lastHypothesisLessonReviewId || 0;
      botData.lastHypothesisLessonReviewId = advancedToId;
      persistCycleState();
      console.log(`🧠 Extracted ${result.new_lessons?.length || 0} lessons, archived ${result.archive_ids?.length || 0}`);
      return { processed: reviewed.length, advancedToId };
    }
  } catch (e) {
    console.log('🧠 Lesson extraction failed:', e.message);
  }
  return { processed: 0, advancedToId: botData.lastHypothesisLessonReviewId || 0 };
};

const getTradeCashflow = (order) => {
  const totalValue = Number(order.total_value || 0);
  if (order.action === 'sell_call' || order.action === 'sell_put') return totalValue;
  if (order.action === 'buy_put' || order.action === 'buyback_call') return -totalValue;
  return 0;
};

const getTradeActionFamily = (action) => {
  if (action === 'sell_call' || action === 'buyback_call') return 'short_call_campaign';
  if (action === 'buy_put' || action === 'sell_put') return 'long_put_campaign';
  return null;
};

const getExpiryTimestampFromInstrument = (instrumentName) => {
  const expiry = parseExpiryFromInstrument(instrumentName);
  const timestamp = expiry?.getTime?.();
  return Number.isFinite(timestamp) ? timestamp : null;
};

const getExpiryCloseAction = (instrumentName) => {
  if (instrumentName?.endsWith('-C')) return 'expire_call';
  if (instrumentName?.endsWith('-P')) return 'expire_put';
  return 'expire_option';
};

const buildSyntheticExpiryCloseOrder = (active, instrumentName, expiryMs, netExposure) => ({
  id: `expiry:${instrumentName}:${new Date(expiryMs).toISOString()}`,
  timestamp: new Date(expiryMs).toISOString(),
  action: getExpiryCloseAction(instrumentName),
  success: 1,
  reason: 'Synthetic expiry close for trade review',
  instrument_name: instrumentName,
  strike: null,
  expiry: Math.floor(expiryMs / 1000),
  delta: null,
  price: 0,
  intended_amount: Math.max(0, netExposure),
  filled_amount: Math.max(0, netExposure),
  fill_price: 0,
  total_value: 0,
  spot_price: null,
  raw_response: { synthetic: true, reason: 'expired' },
  family: active?.action_family || getTradeActionFamily(active?.open_orders?.[0]?.action),
  _source: 'synthetic_expiry',
});

const parseTradeInstrumentParts = (instrumentName) => {
  const parts = String(instrumentName || '').split('-');
  if (parts.length !== 4) return null;
  const strike = Number(parts[2]);
  if (!Number.isFinite(strike)) return null;
  return { strike, optionType: parts[3] };
};

const getSpotAtOrBefore = (rows, timestamp) => {
  const targetMs = new Date(timestamp).getTime();
  if (!Number.isFinite(targetMs) || !Array.isArray(rows) || rows.length === 0) return null;
  let best = null;
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
};

const buildTradeReviewEvidenceWindow = (campaign, priceWindow) => {
  const closedAtMs = new Date(campaign.closed_at).getTime();
  const requestedHorizonMs = new Date(campaign.horizon_end_at).getTime();
  const expiryMs = getExpiryTimestampFromInstrument(campaign.instrument_name);
  const economicHorizonMs = expiryMs == null
    ? requestedHorizonMs
    : Math.min(requestedHorizonMs, expiryMs);
  const whileOpen = priceWindow.filter((point) => (
    point.timestamp >= campaign.opened_at && point.timestamp <= campaign.closed_at
  ));
  const afterCloseBeforeExpiry = priceWindow.filter((point) => {
    const pointMs = new Date(point.timestamp).getTime();
    return pointMs > closedAtMs && pointMs <= economicHorizonMs;
  });
  const postExpiryContext = expiryMs != null && requestedHorizonMs > expiryMs
    ? priceWindow.filter((point) => {
        const pointMs = new Date(point.timestamp).getTime();
        return pointMs > expiryMs && pointMs <= requestedHorizonMs;
      })
    : [];
  const qualityFlags = [];
  if (!parseTradeInstrumentParts(campaign.instrument_name) || expiryMs == null) qualityFlags.push('invalid_instrument');
  if (campaign.spot_open == null) qualityFlags.push('missing_open_spot');
  if (whileOpen.length === 0) qualityFlags.push('missing_open_window');
  if ((campaign.orders || []).some((order) => !(Number(order.spot_price) > 0))) qualityFlags.push('orders_missing_spot');
  if (expiryMs != null && new Date(campaign.opened_at).getTime() > expiryMs) qualityFlags.push('campaign_opens_after_expiry');
  if (expiryMs != null && closedAtMs > expiryMs) qualityFlags.push('campaign_closes_after_expiry');
  if (expiryMs != null && requestedHorizonMs > expiryMs) qualityFlags.push('review_horizon_crosses_expiry');

  return {
    whileOpen,
    afterCloseBeforeExpiry,
    postExpiryContext,
    economicHorizonAt: Number.isFinite(economicHorizonMs) ? new Date(economicHorizonMs).toISOString() : campaign.horizon_end_at,
    qualityFlags,
  };
};

const getExpirySettlementValue = (campaign, spotClose) => {
  const expiryClose = campaign?.close_orders?.find((order) => order?._source === 'synthetic_expiry');
  if (!expiryClose || !Number.isFinite(spotClose)) return null;
  const parsed = parseTradeInstrumentParts(campaign.instrument_name);
  if (!parsed) return null;
  const amount = Math.abs(Number(expiryClose.filled_amount || expiryClose.intended_amount || 0));
  if (!(amount > 0)) return null;
  const intrinsic = parsed.optionType === 'C'
    ? Math.max(0, spotClose - parsed.strike)
    : parsed.optionType === 'P'
      ? Math.max(0, parsed.strike - spotClose)
      : 0;
  return intrinsic * amount;
};

const closeCampaignAtExpiry = (campaigns, active, instrumentName, expiryMs, netExposure) => {
  const expiryOrder = buildSyntheticExpiryCloseOrder(active, instrumentName, expiryMs, netExposure);
  active.order_ids.push(expiryOrder.id);
  active.orders.push(expiryOrder);
  active.close_orders.push(expiryOrder);
  active.pnl_realized += getTradeCashflow(expiryOrder);
  active.premium_closed += Number(expiryOrder.total_value || 0);
  active.closed_at = expiryOrder.timestamp;
  active.spot_close = null;
  campaigns.push(active);
};

const getActionFromTradeDirection = (instrumentName, direction) => {
  if (!instrumentName || !direction) return null;
  const normalized = String(direction).toLowerCase();
  if (instrumentName.endsWith('-C')) {
    if (normalized === 'sell') return 'sell_call';
    if (normalized === 'buy') return 'buyback_call';
  }
  if (instrumentName.endsWith('-P')) {
    if (normalized === 'buy') return 'buy_put';
    if (normalized === 'sell') return 'sell_put';
  }
  return null;
};

const normalizeLyraTradeForReview = (trade) => {
  const instrumentName = trade?.instrument_name || null;
  const action = getActionFromTradeDirection(instrumentName, trade?.direction);
  if (!action) return null;
  const amount = Math.abs(Number(trade?.trade_amount ?? trade?.amount ?? 0));
  const price = Number(trade?.trade_price ?? trade?.price ?? 0);
  const timestamp = typeof trade?.timestamp === 'number'
    ? new Date(trade.timestamp).toISOString()
    : new Date(trade?.timestamp).toISOString();
  if (!(amount > 0) || !timestamp || Number.isNaN(new Date(timestamp).getTime())) return null;
  return {
    id: `lyra:${trade?.trade_id ?? `${instrumentName}:${timestamp}:${action}:${amount}`}`,
    timestamp,
    action,
    success: 1,
    reason: 'Recovered from Lyra trade history',
    instrument_name: instrumentName,
    strike: null,
    expiry: null,
    delta: null,
    price,
    intended_amount: amount,
    filled_amount: amount,
    fill_price: price > 0 ? price : null,
    total_value: amount * price,
    spot_price: Number(trade?.index_price ?? 0) || null,
    raw_response: trade,
    _source: 'lyra',
  };
};

const mergeOrdersForTradeReview = (localOrders, lyraTrades) => {
  const FILL_TIME_WINDOW_MS = 10 * 60_000;
  const AMOUNT_EPSILON = 0.02;
  const VALUE_EPSILON = 0.25;
  const isSameRecoveredFill = (order, normalized) => {
    if (order._source !== 'local') return false;
    if (order.instrument_name !== normalized.instrument_name) return false;
    if (order.action !== normalized.action) return false;
    if (Number(order.success || 0) !== 1) return false;

    const orderAmount = Math.abs(Number(order.filled_amount || 0));
    const normalizedAmount = Math.abs(Number(normalized.filled_amount || normalized.intended_amount || 0));
    if (!(orderAmount > 0) || !(normalizedAmount > 0)) return false;
    if (Math.abs(orderAmount - normalizedAmount) >= AMOUNT_EPSILON) return false;

    const orderTs = new Date(order.timestamp).getTime();
    const normalizedTs = new Date(normalized.timestamp).getTime();
    if (Math.abs(orderTs - normalizedTs) >= FILL_TIME_WINDOW_MS) return false;

    const orderValue = Math.abs(Number(order.total_value || 0));
    const normalizedValue = Math.abs(Number(normalized.total_value || 0));
    if (orderValue > 0 && normalizedValue > 0) {
      return Math.abs(orderValue - normalizedValue) < VALUE_EPSILON;
    }

    return true;
  };

  const merged = [...localOrders.map((order) => ({ ...order, _source: 'local' }))];
  for (const trade of lyraTrades) {
    const normalized = normalizeLyraTradeForReview(trade);
    if (!normalized) continue;
    const duplicateLocal = merged.some((order) => isSameRecoveredFill(order, normalized));
    if (!duplicateLocal) merged.push(normalized);
  }
  return merged.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());
};

const deriveClosedTradeCampaigns = (orders, now = Date.now()) => {
  const byInstrument = new Map();
  for (const order of orders) {
    if (!order?.instrument_name || Number(order?.success || 0) !== 1) continue;
    const family = getTradeActionFamily(order.action);
    if (!family) continue;
    const list = byInstrument.get(order.instrument_name) || [];
    list.push({ ...order, family });
    byInstrument.set(order.instrument_name, list);
  }

  const campaigns = [];
  const EPS = 1e-9;

  for (const [instrumentName, instrumentOrders] of byInstrument.entries()) {
    instrumentOrders.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    const expiryMs = getExpiryTimestampFromInstrument(instrumentName);
    let netExposure = 0;
    let active = null;

    for (const order of instrumentOrders) {
      const orderMs = new Date(order.timestamp).getTime();
      if (active && netExposure > EPS && expiryMs != null && Number.isFinite(orderMs) && expiryMs <= Math.min(orderMs, now)) {
        closeCampaignAtExpiry(campaigns, active, instrumentName, expiryMs, netExposure);
        active = null;
        netExposure = 0;
      }

      const qty = Math.abs(Number(order.filled_amount || 0));
      if (!(qty > 0)) continue;

      const isOpen = order.action === 'sell_call' || order.action === 'buy_put';
      const exposureDelta = isOpen ? qty : -qty;

      if (!active && isOpen) {
        active = {
          instrument_name: instrumentName,
          action_family: order.family,
          opened_at: order.timestamp,
          closed_at: null,
          order_ids: [],
          orders: [],
          open_orders: [],
          close_orders: [],
          premium_opened: 0,
          premium_closed: 0,
          pnl_realized: 0,
          spot_open: Number(order.spot_price || 0) || null,
          spot_close: null,
        };
      }

      if (!active) continue;

      active.order_ids.push(order.id);
      active.orders.push(order);
      active.pnl_realized += getTradeCashflow(order);
      if (isOpen) {
        active.open_orders.push(order);
        active.premium_opened += Number(order.total_value || 0);
      } else {
        active.close_orders.push(order);
        active.premium_closed += Number(order.total_value || 0);
      }

      netExposure += exposureDelta;

      if (netExposure <= EPS) {
        active.closed_at = order.timestamp;
        active.spot_close = Number(order.spot_price || 0) || null;
        campaigns.push(active);
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

  return campaigns;
};

const TRADE_REVIEW_LOOKBACK_DAYS = 120;
const TRADE_REVIEW_WINDOWS_DAYS = [1, 3, 7];
const collectPendingTradeReviews = (campaigns, now) => {
  const pendingReviews = [];
  for (const campaign of campaigns) {
    const closedAtMs = new Date(campaign.closed_at).getTime();
    for (const reviewWindowDays of TRADE_REVIEW_WINDOWS_DAYS) {
      const horizonEndMs = closedAtMs + reviewWindowDays * 24 * 60 * 60 * 1000;
      const eligible = now >= horizonEndMs;
      const alreadyReviewed = db.hasTradeReview(campaign.instrument_name, campaign.closed_at, reviewWindowDays);
      if (!eligible || alreadyReviewed) continue;
      pendingReviews.push({
        ...campaign,
        review_window_days: reviewWindowDays,
        horizon_end_at: new Date(horizonEndMs).toISOString(),
      });
    }
  }
  return pendingReviews;
};

let _tradeReviewInFlight = false;
const reviewClosedTrades = async () => {
  if (_tradeReviewInFlight || !process.env.ANTHROPIC_API_KEY || !db) return { attempted: false, readyCount: 0, storedCount: 0, error: null };
  _tradeReviewInFlight = true;
  try {
    botData.lastTradeReviewRun = Date.now();
    botData.lastTradeReviewError = null;
    persistCycleState();
    const now = Date.now();
    const fromMs = now - TRADE_REVIEW_LOOKBACK_DAYS * 24 * 60 * 60 * 1000;
    const since = new Date(fromMs).toISOString();
    const recentOrders = db.getOrdersInRange
      ? (db.getOrdersInRange(since, new Date(now).toISOString()) || [])
      : (db.getRecentOrders(since, 1000) || []);
    const lyraTrades = await fetchTradeHistory(fromMs, now);
    const campaigns = deriveClosedTradeCampaigns(mergeOrdersForTradeReview(recentOrders, lyraTrades), now);
    const pendingReviews = collectPendingTradeReviews(campaigns, now);
    botData.lastTradeReviewReadyCount = pendingReviews.length;
    botData.lastTradeReviewTargets = pendingReviews.map((campaign) => ({
      instrument_name: campaign.instrument_name,
      review_window_days: campaign.review_window_days,
      closed_at: campaign.closed_at,
      horizon_end_at: campaign.horizon_end_at,
    }));
    persistCycleState();
    if (pendingReviews.length === 0) {
      console.log('🧾 Trade review: no eligible closed campaigns');
      botData.lastTradeReviewSuccess = Date.now();
      persistCycleState();
      return { attempted: true, readyCount: 0, storedCount: 0, error: null };
    }

    console.log(`🧾 Reviewing ${pendingReviews.length} closed trade window(s)...`);
    console.log(`🧾 Trade review targets: ${pendingReviews.map((campaign) => `${campaign.instrument_name} [${campaign.review_window_days}d]`).join(', ')}`);
    let storedCount = 0;

    for (const campaign of pendingReviews.slice(0, 4)) {
      try {
        const priceWindow = db.getRecentSpotPrices(campaign.opened_at) || [];
        const evidenceWindow = buildTradeReviewEvidenceWindow(campaign, priceWindow);
        const { whileOpen, afterCloseBeforeExpiry, postExpiryContext, economicHorizonAt, qualityFlags } = evidenceWindow;
        const spotAtClose = campaign.spot_close ?? getSpotAtOrBefore(whileOpen, campaign.closed_at);
        const expirySettlementValue = getExpirySettlementValue(campaign, spotAtClose);
        const expirySettlementCashflow = expirySettlementValue == null
          ? 0
          : campaign.action_family === 'short_call_campaign'
            ? -expirySettlementValue
            : expirySettlementValue;
        const effectivePnlRealized = campaign.pnl_realized + expirySettlementCashflow;
        const effectivePremiumClosed = campaign.premium_closed + (expirySettlementValue || 0);
        const closeReason = campaign.close_orders?.some((order) => order?._source === 'synthetic_expiry') ? 'expiry' : 'offsetting order';

        const reviewPrompt = `You are reviewing a CLOSED options trade campaign for a Spitznagel-style ETH tail-hedging bot.

Your job is not to judge by P&L alone. Use hindsight carefully:
- A losing trade can still have been the right decision at the time.
- A profitable trade can still have been the wrong decision if it violated discipline.
- Whether ETH moved up or down is second-order evidence. Care about it only insofar as it reveals whether protection was cheap, premium was rich, liquidity was good, or risk was mispriced.
- Distinguish execution error, sizing error, strike-selection error, and acceptable arithmetic bleed.
- This is a staged hindsight review. Only use post-close information through the specified horizon, not beyond it.
- Never use price movement after the contract expired to evaluate what the closed option would have paid or its hold-to-expiry P&L. Post-expiry prices may be used only as labeled hindsight context for market trajectory, regime judgment, and possible follow-on trades.
- Base factual claims on the supplied values. If data is missing or flagged, state the uncertainty instead of filling it in.
- Do not turn a single campaign into a universal numeric strike or exit threshold.

Short-call review discipline:
- Separate mark-to-market stress from expiry economics. A short call seller receives premium upfront and keeps it unless it is paid back later.
- If a short call expires at or below strike, it expires worthless and the trade keeps the full premium. At exactly strike, the premium is still kept.
- Do not describe a short call as a bad trade merely because its mark expanded near a local high. That can be temporary gamma/IV pain rather than a bad final payoff.
- Treat a call buyback below strike as an insurance purchase: it may be rational to pay to remove tail risk of continuation, but it converts uncertain future upside risk into certain realized cost.
- When comparing a closer strike versus a farther OTM strike, explicitly note the tradeoff: farther OTM reduces forced-intervention risk but also materially reduces premium income. Do not imply that safety is free.
- If post-close spot remains below strike through the economically relevant pre-expiry window, say that holding may have preserved more premium while still weighing the breakout risk visible at the decision time.
- On upside breakouts when short calls were already on, explicitly consider whether selling more calls into emotionally rich bullish premium would have been superior to buying back for insurance, provided the account still had room under the active margin cap.

Campaign:
- Instrument: ${campaign.instrument_name}
- Family: ${campaign.action_family}
- Opened: ${campaign.opened_at}
- Closed: ${campaign.closed_at}
- Close reason: ${closeReason}
- Review horizon: ${campaign.review_window_days} day(s) after close, through ${campaign.horizon_end_at}
- Economically relevant counterfactual window ends: ${economicHorizonAt}
- Realized campaign cashflow: $${effectivePnlRealized.toFixed(4)}
- Premium opened: $${campaign.premium_opened.toFixed(4)}
- Premium closed: $${effectivePremiumClosed.toFixed(4)}
- Spot at open: ${campaign.spot_open != null ? `$${campaign.spot_open}` : 'N/A'}
- Spot at close: ${spotAtClose != null ? `$${spotAtClose}` : 'N/A'}
- Expiry settlement value: ${expirySettlementValue != null ? `$${expirySettlementValue.toFixed(4)}${expirySettlementValue > 0 && campaign.action_family === 'short_call_campaign' ? ' (forced buyback at expiry: call expired ITM, paid intrinsic, already included in realized cashflow)' : ''}` : 'N/A'}
- Spot range while open: ${whileOpen.length > 0 ? `$${Math.min(...whileOpen.map(p => p.price)).toFixed(2)} -> $${Math.max(...whileOpen.map(p => p.price)).toFixed(2)}` : 'N/A'}
- Spot range after close but before expiry: ${afterCloseBeforeExpiry.length > 0 ? `$${Math.min(...afterCloseBeforeExpiry.map(p => p.price)).toFixed(2)} -> $${Math.max(...afterCloseBeforeExpiry.map(p => p.price)).toFixed(2)}` : 'N/A'}
- Post-expiry spot context (not valid for option payoff evaluation): ${postExpiryContext.length > 0 ? `$${Math.min(...postExpiryContext.map(p => p.price)).toFixed(2)} -> $${Math.max(...postExpiryContext.map(p => p.price)).toFixed(2)}` : 'N/A'}
- Data quality flags: ${qualityFlags.length > 0 ? qualityFlags.join(', ') : 'none'}

Orders:
${campaign.orders.map(o => `${o.timestamp} | ${o.action} ${o.instrument_name} | qty=${o.filled_amount || o.intended_amount} | fill=$${o.fill_price || o.price || 0} | total=$${o.total_value || 0} | spot=${Number(o.spot_price) > 0 ? `$${o.spot_price}` : 'N/A'}`).join('\n')}

Review categories:
- disciplined_win: good decision and good execution
- disciplined_loss: good decision at the time, outcome unfavorable or bleed acceptable
- execution_mistake: thesis may have been fine, but execution quality was poor
- risk_mistake: strike, timing, sizing, or exit logic was wrong

Output JSON only:
{
  "status":"disciplined_win|disciplined_loss|execution_mistake|risk_mistake",
  "summary":"2-4 sentence review that explicitly says whether the decision was right at the time",
  "lessons":["short durable lesson 1","short durable lesson 2"]
}`;

        const response = await axios.post('https://api.anthropic.com/v1/messages', {
          model: ANTHROPIC_SONNET_MODEL,
          thinking: { type: 'disabled' },
          max_tokens: 700,
          messages: [{ role: 'user', content: reviewPrompt }],
        }, {
          headers: {
            'x-api-key': process.env.ANTHROPIC_API_KEY,
            'anthropic-version': '2023-06-01',
            'content-type': 'application/json',
          },
          timeout: 30000,
        });

        const text = getAnthropicResponseText(response.data);
        const result = extractJSON(text);
        if (!result?.status || !result?.summary) {
          console.log(`🧾 Trade review parse failed for ${campaign.instrument_name}`);
          continue;
        }

        db.insertTradeReview({
          instrument_name: campaign.instrument_name,
          action_family: campaign.action_family,
          opened_at: campaign.opened_at,
          closed_at: campaign.closed_at,
          review_window_days: campaign.review_window_days,
          horizon_end_at: campaign.horizon_end_at,
          order_ids: campaign.order_ids,
          review_status: result.status,
          review_confidence: null,
          summary: result.summary,
          lessons: Array.isArray(result.lessons) ? result.lessons.slice(0, 3) : [],
          pnl_realized: effectivePnlRealized,
          premium_opened: campaign.premium_opened,
          premium_closed: effectivePremiumClosed,
          spot_open: campaign.spot_open,
          spot_close: spotAtClose,
          spot_min_while_open: whileOpen.length > 0 ? Math.min(...whileOpen.map(p => p.price)) : null,
          spot_max_while_open: whileOpen.length > 0 ? Math.max(...whileOpen.map(p => p.price)) : null,
          spot_min_after_close: afterCloseBeforeExpiry.length > 0 ? Math.min(...afterCloseBeforeExpiry.map(p => p.price)) : null,
          spot_max_after_close: afterCloseBeforeExpiry.length > 0 ? Math.max(...afterCloseBeforeExpiry.map(p => p.price)) : null,
        });
        console.log(`🧾 Trade review stored for ${campaign.instrument_name} [${campaign.review_window_days}d]: ${result.status}`);
        storedCount += 1;
      } catch (e) {
        console.log(`🧾 Trade review failed for ${campaign.instrument_name} [${campaign.review_window_days}d]: ${e.message}`);
      }
    }
    botData.lastTradeReviewSuccess = Date.now();
    botData.lastTradeReviewReadyCount = Math.max(0, pendingReviews.length - storedCount);
    persistCycleState();
    return { attempted: true, readyCount: pendingReviews.length, storedCount, error: null };
  } catch (e) {
    botData.lastTradeReviewError = e.message;
    persistCycleState();
    console.log(`🧾 Trade review scheduler failed: ${e.message}`);
    return { attempted: true, readyCount: botData.lastTradeReviewReadyCount || 0, storedCount: 0, error: e.message };
  } finally {
    _tradeReviewInFlight = false;
  }
};

const TRADE_LESSON_TAXONOMY = Object.freeze([
  Object.freeze({
    lesson_key: 'short_call.exit_insurance',
    title: 'Short-call exit insurance',
    category: 'exit_timing',
    action_family: 'short_call_campaign',
    purpose: 'When a buyback is worth its certain cost given DTE, strike distance, momentum, and portfolio risk.',
  }),
  Object.freeze({
    lesson_key: 'short_call.strike_and_sizing',
    title: 'Short-call strike and sizing',
    category: 'strike_selection',
    action_family: 'short_call_campaign',
    purpose: 'The joint tradeoff among premium income, OTM buffer, volatility, size, margin, and intervention pressure.',
  }),
  Object.freeze({
    lesson_key: 'tail_put.strike_and_cost',
    title: 'Tail-put strike and insurance cost',
    category: 'strike_selection',
    action_family: 'long_put_campaign',
    purpose: 'Whether protection was affordable, sufficiently convex, and sized so normal premium bleed is tolerable.',
  }),
  Object.freeze({
    lesson_key: 'tail_put.lifecycle',
    title: 'Tail-put lifecycle',
    category: 'exit_timing',
    action_family: 'long_put_campaign',
    purpose: 'When protection should remain in force, be monetized, be rolled, or be retired as the insured risk changes.',
  }),
  Object.freeze({
    lesson_key: 'process.decision_quality',
    title: 'Decision quality versus outcome',
    category: 'process',
    action_family: null,
    purpose: 'Whether a decision was sound using information available at the time, independently of realized P&L.',
  }),
  Object.freeze({
    lesson_key: 'execution.order_hygiene',
    title: 'Execution and order hygiene',
    category: 'execution',
    action_family: null,
    purpose: 'Tranche timing, micro-fills, rapid exit/re-entry cycles, liquidity, and other implementation quality.',
  }),
]);

const parseStoredStringArray = (value) => {
  if (Array.isArray(value)) return value;
  try {
    const parsed = JSON.parse(value || '[]');
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
};

const formatTradeLessonForPrompt = (lesson) => {
  const title = lesson.title || lesson.lesson_key || 'Legacy lesson';
  const support = Number(lesson.supporting_campaign_count ?? lesson.evidence_count ?? 0);
  const contradictions = Number(lesson.contradicting_campaign_count || 0);
  const status = lesson.status || (lesson.lesson_key ? 'candidate' : 'legacy');
  const source = lesson.lesson_key ? `[lesson:${lesson.lesson_key}]` : '[lesson:legacy]';
  return `- ${source} ${title} [${status}; ${support} supporting campaign(s); ${contradictions} contradiction(s)]: ${lesson.lesson}`;
};

const formatTradeReviewForLessonSynthesis = (review) => {
  const parsed = parseTradeInstrumentParts(review.instrument_name);
  const lessons = parseStoredStringArray(review.lessons);
  return [
    `#${review.id}`,
    review.instrument_name,
    `[${review.review_status}]`,
    `[${review.review_window_days}d]`,
    `family=${review.action_family || 'unknown'}`,
    `strike=${parsed?.strike ?? 'N/A'}`,
    `spot_open=${review.spot_open ?? 'N/A'}`,
    `spot_close=${review.spot_close ?? 'N/A'}`,
    `open_range=${review.spot_min_while_open ?? 'N/A'}..${review.spot_max_while_open ?? 'N/A'}`,
    `post_close_pre_expiry_range=${review.spot_min_after_close ?? 'N/A'}..${review.spot_max_after_close ?? 'N/A'}`,
    `pnl=$${Number(review.pnl_realized || 0).toFixed(2)}`,
    `summary=${String(review.summary || '').replace(/\s+/g, ' ').slice(0, 220)}`,
    `observations=${lessons.join(' | ').slice(0, 180) || 'none'}`,
  ].join(' ');
};

let _tradeLessonInFlight = false;
const extractTradeLessons = async () => {
  if (_tradeLessonInFlight || !process.env.ANTHROPIC_API_KEY || !db?.upsertCanonicalTradeLesson) {
    return { attempted: false, processed: 0, advancedToId: botData.lastTradeLessonReviewId || 0, error: null };
  }

  _tradeLessonInFlight = true;
  botData.lastTradeLessonRun = Date.now();
  botData.lastTradeLessonError = null;
  persistCycleState();

  try {
    const canonicalLessons = db.getCanonicalTradeLessons?.() || [];
    const legacyLessons = db.getLegacyTradeLessons?.() || [];
    const isBootstrap = canonicalLessons.length === 0;
    const reviews = isBootstrap
      ? (db.getRecentTradeReviews(TRADE_LESSON_BOOTSTRAP_REVIEW_LIMIT) || [])
      : (db.getTradeReviewsSinceId?.(botData.lastTradeLessonReviewId || 0, 12) || []);
    if (reviews.length < 2) {
      botData.lastTradeLessonSuccess = Date.now();
      persistCycleState();
      return { attempted: true, processed: 0, advancedToId: botData.lastTradeLessonReviewId || 0, error: null };
    }

    console.log(`🧠 ${isBootstrap ? 'Bootstrapping' : 'Updating'} canonical trade lessons from ${reviews.length} linked review(s)...`);
    const taxonomyText = TRADE_LESSON_TAXONOMY
      .map((definition) => `- ${definition.lesson_key}: ${definition.title} — ${definition.purpose}`)
      .join('\n');
    const prompt = `You maintain the canonical trade playbook for a Spitznagel-style ETH options bot.

Your output updates a small fixed taxonomy. It must consolidate evidence, surface disagreement, and remain useful to an advisor making a decision now. Do not create a chronological diary.

Canonical lesson keys:
${taxonomyText}

Current canonical lessons:
${canonicalLessons.length > 0 ? canonicalLessons.map(formatTradeLessonForPrompt).join('\n') : 'None — this is the first canonical synthesis.'}

Legacy generated lessons (unlinked observations; use only as hypotheses, never as evidence counts):
${legacyLessons.length > 0 ? legacyLessons.slice(0, 12).map((lesson) => `- ${String(lesson.lesson || '').replace(/\s+/g, ' ').slice(0, 220)}`).join('\n') : 'None'}

Reviewed campaigns (the #ID is the only valid evidence identifier):
${reviews.map(formatTradeReviewForLessonSynthesis).join('\n')}

Requirements:
1. Use only the exact lesson keys listed above. Return one update per relevant key and no duplicate keys.
2. Write a concise operational rule of 1-3 sentences. State conditional factors instead of inventing a universal percentage from a small sample.
3. Link every claim to supporting_review_ids. Counts are computed by code; never output an evidence count.
4. Put genuinely opposing reviews in contradicting_review_ids. Do not silently average incompatible recommendations.
5. Judge decisions using information available at decision time. Post-expiry price is not evidence for an expired option's payoff or hold-to-expiry P&L, but it may support labeled conclusions about market trajectory, regime judgment, and follow-on opportunities.
6. Treat missing or impossible spot data as uncertainty. Never claim spot is below strike when the supplied numbers say otherwise.
7. Use applicability for short phrases describing when the rule matters, such as "spot below strike", "under 24h DTE", or "risk mandate unchanged".
8. change_summary must say what the new review evidence changed or confirmed in under 100 characters.
${isBootstrap ? '9. This is a bootstrap. Cover at least four relevant keys so the legacy list can be replaced safely.' : ''}

Output JSON only:
{"lesson_updates":[{"lesson_key":"short_call.exit_insurance","lesson":"<canonical operational rule>","applicability":["<condition>"],"supporting_review_ids":[1,2],"contradicting_review_ids":[3],"change_summary":"<what changed>"}]}`;

    const response = await axios.post('https://api.anthropic.com/v1/messages', {
      model: ANTHROPIC_SONNET_MODEL,
      thinking: { type: 'disabled' },
      max_tokens: 1800,
      messages: [{ role: 'user', content: prompt }],
    }, {
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      timeout: TRADE_LESSON_SYNTHESIS_TIMEOUT_MS,
    });

    const text = getAnthropicResponseText(response.data);
    const result = extractJSON(text);
    const definitions = new Map(TRADE_LESSON_TAXONOMY.map((definition) => [definition.lesson_key, definition]));
    const seenKeys = new Set();
    const updates = (Array.isArray(result?.lesson_updates) ? result.lesson_updates : [])
      .filter((update) => {
        const key = String(update?.lesson_key || '');
        if (!definitions.has(key) || seenKeys.has(key) || String(update?.lesson || '').trim().length < 20) return false;
        seenKeys.add(key);
        return true;
      });
    if (updates.length === 0 || (isBootstrap && updates.length < 4)) {
      const message = `Canonical synthesis returned ${updates.length} valid update(s); ${isBootstrap ? 'at least 4 required' : 'at least 1 required'}`;
      botData.lastTradeLessonError = message;
      persistCycleState();
      console.log(`🧠 ${message}`);
      return { attempted: true, processed: 0, advancedToId: botData.lastTradeLessonReviewId || 0, error: message };
    }

    const reviewIds = new Set(reviews.map((review) => Number(review.id)).filter(Number.isInteger));
    let storedCount = 0;
    for (const update of updates) {
      const definition = definitions.get(update.lesson_key);
      const stored = db.upsertCanonicalTradeLesson({
        ...update,
        title: definition.title,
        category: definition.category,
        action_family: definition.action_family,
        status: 'candidate',
      }, reviewIds);
      if (stored) storedCount += 1;
    }
    if (storedCount === 0) {
      const message = 'Canonical synthesis produced no storable lesson updates';
      botData.lastTradeLessonError = message;
      persistCycleState();
      console.log(`🧠 ${message}`);
      return { attempted: true, processed: 0, advancedToId: botData.lastTradeLessonReviewId || 0, error: message };
    }

    const highestReviewId = Math.max(...Array.from(reviewIds), botData.lastTradeLessonReviewId || 0);
    botData.lastTradeLessonReviewId = highestReviewId;
    botData.lastTradeLessonSuccess = Date.now();
    botData.lastTradeLessonError = null;
    persistCycleState();
    console.log(`🧠 Canonical trade lessons: ${storedCount} updated from ${reviews.length} linked review(s)`);
    return { attempted: true, processed: reviews.length, advancedToId: highestReviewId, error: null };
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    botData.lastTradeLessonError = message;
    persistCycleState();
    console.log('🧠 Canonical trade lesson extraction failed:', message);
    return { attempted: true, processed: 0, advancedToId: botData.lastTradeLessonReviewId || 0, error: message };
  } finally {
    _tradeLessonInFlight = false;
  }
};

// ─── Wiki Knowledge System ──────────────────────────────────────────────────

const WIKI_DIR = process.env.WIKI_DIR || path.join(__dirname, 'knowledge');
const WIKI_META_PATH = path.join(WIKI_DIR, '.meta.json');
const WIKI_HISTORY_DIR = path.join(WIKI_DIR, '.history');
const WIKI_INDEX_PAGE = 'index.md';
const WIKI_LOG_PAGE = 'log.md';
const WIKI_INDEX_PATH = path.join(WIKI_DIR, WIKI_INDEX_PAGE);
const WIKI_LOG_PATH = path.join(WIKI_DIR, WIKI_LOG_PAGE);
const WIKI_RAW_EVIDENCE_DIR = path.join(WIKI_DIR, 'raw', 'evidence');

// Ensure all wiki subdirectories exist
for (const sub of ['regimes', 'protection', 'revenue', 'indicators', 'strategy', 'raw', 'raw/evidence']) {
  fs.mkdirSync(path.join(WIKI_DIR, sub), { recursive: true });
}

const WIKI_KEY_PAGES = [
  'regimes/current.md',
  'protection/pricing.md',
  'protection/windows.md',
  'revenue/pricing.md',
  'indicators/leading.md',
  'strategy/lessons.md',
  'strategy/playbook.md',
];

// These pages make live-state claims and must be reconciled against the same
// evidence packet. Updating only some of them creates contradictory snapshots.
const WIKI_LIVE_RESEARCH_PAGES = [
  'regimes/current.md',
  'protection/pricing.md',
  'protection/windows.md',
  'protection/convexity.md',
  'revenue/pricing.md',
  'revenue/windows.md',
  'indicators/leading.md',
];

const WIKI_STRATEGY_PAGES = new Set([
  'strategy/lessons.md',
  'strategy/mistakes.md',
  'strategy/playbook.md',
]);

const WIKI_EXPECTED_HEADERS = {
  'regimes/current.md': ['Classification', 'Evidence'],
  'regimes/history.md': ['Regime Transitions'],
  'protection/pricing.md': ['Current IV Environment', 'Cost Assessment'],
  'protection/windows.md': ['Active Windows', 'Historical Windows'],
  'protection/convexity.md': ['Current Convexity Map'],
  'revenue/pricing.md': ['Current Premium Environment', 'Premium Assessment'],
  'revenue/windows.md': ['Active Windows', 'Historical Windows'],
  'revenue/efficiency.md': ['Premium Per Unit Risk'],
  'indicators/leading.md': ['Confirmed Leading Indicators'],
  'indicators/correlations.md': ['Strong Correlations'],
  'indicators/divergences.md': ['Active Divergences'],
  'strategy/lessons.md': ['Active Lessons'],
  'strategy/mistakes.md': ['Costly Patterns'],
  'strategy/playbook.md': ['Core Rules'],
};

const WIKI_ALL_PAGES = [
  'regimes/current.md',
  'regimes/history.md',
  'protection/pricing.md',
  'protection/windows.md',
  'protection/convexity.md',
  'revenue/pricing.md',
  'revenue/windows.md',
  'revenue/efficiency.md',
  'indicators/leading.md',
  'indicators/correlations.md',
  'indicators/divergences.md',
  'strategy/lessons.md',
  'strategy/mistakes.md',
  'strategy/playbook.md',
];

const getWikiPageMetaMap = (meta) => {
  if (!meta.pages || typeof meta.pages !== 'object' || Array.isArray(meta.pages)) meta.pages = {};
  return meta.pages;
};

const updateWikiPageMeta = (meta, pagePath, patch) => {
  const pages = getWikiPageMetaMap(meta);
  pages[pagePath] = { ...(pages[pagePath] || {}), ...patch };
  return pages[pagePath];
};

const summarizeWikiPageChange = (before, after, fallback) => {
  const priorTldr = getWikiTldr(before);
  const nextTldr = getWikiTldr(after);
  if (priorTldr !== nextTldr && nextTldr !== 'Awaiting initial assessment') {
    return `TLDR updated: ${nextTldr}`.slice(0, 220);
  }
  return fallback;
};

const readWikiPage = (pagePath) => {
  try {
    const fullPath = path.join(WIKI_DIR, pagePath);
    return fs.readFileSync(fullPath, 'utf-8');
  } catch {
    return '';
  }
};

const readWikiMeta = () => {
  try {
    return JSON.parse(fs.readFileSync(WIKI_META_PATH, 'utf-8'));
  } catch {
    return {};
  }
};

const writeWikiMeta = (meta) => {
  fs.writeFileSync(WIKI_META_PATH, JSON.stringify(meta, null, 2));
};

const parseWikiMetaTimestamp = (value) => {
  const timestamp = typeof value === 'string' ? new Date(value).getTime() : 0;
  return Number.isFinite(timestamp) ? timestamp : 0;
};

const getWikiLintSchedule = (meta = readWikiMeta(), nowMs = Date.now()) => {
  const pageMeta = meta?.pages && typeof meta.pages === 'object' && !Array.isArray(meta.pages)
    ? meta.pages
    : {};
  const pagesNeedingReview = WIKI_ALL_PAGES.filter((pagePath) => {
    const stored = pageMeta[pagePath] || {};
    const reviewedMs = parseWikiMetaTimestamp(stored.last_reviewed_at);
    const changedMs = parseWikiMetaTimestamp(stored.last_changed_at || stored.last_evidence_at);
    return !reviewedMs || (changedMs > 0 && reviewedMs < changedMs);
  });
  const needsInitialReview = pagesNeedingReview.length > 0;
  const lastAttemptMs = parseWikiMetaTimestamp(meta?.last_lint_attempt);
  const lastSuccessMs = parseWikiMetaTimestamp(meta?.last_lint);
  const lastRunMs = Math.max(lastAttemptMs, lastSuccessMs);
  const lastError = typeof meta?.last_lint_error === 'string' && meta.last_lint_error.trim()
    ? meta.last_lint_error.trim()
    : null;
  const nextDueMs = lastRunMs ? lastRunMs + WIKI_LINT_INTERVAL_MS : 0;
  return {
    due: nextDueMs === 0 || nowMs >= nextDueMs,
    needsInitialReview,
    pagesNeedingReview,
    lastError,
    nextDueAt: nextDueMs ? new Date(nextDueMs).toISOString() : new Date(nowMs).toISOString(),
  };
};

const ensureWikiSupportFiles = () => {
  if (!fs.existsSync(WIKI_INDEX_PATH)) {
    fs.writeFileSync(WIKI_INDEX_PATH, '# Knowledge Index\n\nSystem-maintained index. Awaiting first refresh.\n');
  }
  if (!fs.existsSync(WIKI_LOG_PATH)) {
    fs.writeFileSync(WIKI_LOG_PATH, '# Knowledge Log\n\nAppend-only system log of wiki ingests, seeds, queries, and lint passes.\n\n');
  }
};

const logWikiMetaSummary = (prefix, meta = null) => {
  const effectiveMeta = meta || readWikiMeta();
  console.log(`${prefix} wiki_dir=${WIKI_DIR} meta_path=${WIKI_META_PATH} meta_exists=${fs.existsSync(WIKI_META_PATH)} seeded_at=${effectiveMeta.seeded_at || 'none'} last_ingest=${effectiveMeta.last_ingest || 'none'} last_lint=${effectiveMeta.last_lint || 'none'}`);
};

ensureWikiSupportFiles();

const getWikiTldr = (content = '') => {
  const lines = String(content).split('\n').map((line) => line.trim()).filter(Boolean);
  const tldrLine = lines.find((line) => line.startsWith('**') && line.endsWith('**'));
  if (tldrLine) return tldrLine.replace(/^\*\*/, '').replace(/\*\*$/, '').trim();
  return lines[0] || 'Awaiting initial assessment';
};

const countWikiEvidencePoints = (content = '') => {
  const structured = String(content).match(/\[(?:source:\s*[^\]]+|(?:tick|order|review):#\d+|lesson:[^\]]+)\]/gi) || [];
  if (structured.length > 0) return new Set(structured.map((value) => value.toLowerCase())).size;
  return new Set(String(content).match(/\[\d{4}-\d{2}-\d{2}\]/g) || []).size;
};

const getStructuredWikiMarkers = (content = '') => new Set(
  (String(content).match(/\[(?:source:\s*[^\]]+|(?:tick|order|review):#\d+|lesson:[^\]]+)\]/gi) || [])
    .map((value) => value.toLowerCase()),
);

const refreshWikiIndex = () => {
  const meta = readWikiMeta();
  const pageMeta = getWikiPageMetaMap(meta);
  const groupedPages = new Map();
  for (const page of WIKI_ALL_PAGES) {
    const category = page.split('/')[0];
    if (!groupedPages.has(category)) groupedPages.set(category, []);
    const content = readWikiPage(page);
    groupedPages.get(category).push({
      page,
      summary: getWikiTldr(content),
      placeholder: isPlaceholderWikiPage(content),
      evidenceCount: countWikiEvidencePoints(content),
      metadata: pageMeta[page] || {},
    });
  }

  const lines = [
    '# Knowledge Index',
    '',
    'System-maintained catalog of the compiled trading wiki. Read this first to understand what pages exist and where current knowledge lives.',
    '',
    `Updated: ${new Date().toISOString()}`,
    '',
    '## System Files',
    `- [schema.md](schema.md) — wiki structure, source hierarchy, and update rules`,
    `- [${WIKI_LOG_PAGE}](${WIKI_LOG_PAGE}) — append-only maintenance timeline`,
    `- Raw evidence packets live in [raw/evidence](raw/evidence)${meta.last_evidence_packet ? ` (latest: [${meta.last_evidence_packet}](${meta.last_evidence_packet}))` : ''}`,
    '',
  ];

  for (const [category, pages] of groupedPages.entries()) {
    lines.push(`## ${category}`);
    for (const page of pages) {
      const issues = Array.isArray(page.metadata.issues) ? page.metadata.issues.length : 0;
      const ownership = WIKI_STRATEGY_PAGES.has(page.page) ? 'Learning-owned view' : 'Wiki research';
      lines.push(`- [${page.page}](${page.page}) — ${page.summary} ${page.placeholder ? '(placeholder)' : `(evidence refs: ${page.evidenceCount}; reviewed: ${page.metadata.last_reviewed_at || 'never'}; issues: ${issues}; ${ownership})`}`);
    }
    lines.push('');
  }

  fs.writeFileSync(WIKI_INDEX_PATH, `${lines.join('\n').trim()}\n`);
};

const appendWikiLog = (kind, title, bulletLines = []) => {
  ensureWikiSupportFiles();
  const timestamp = new Date().toISOString();
  const lines = [`## [${timestamp}] ${kind} | ${title}`];
  for (const line of bulletLines) {
    lines.push(`- ${line}`);
  }
  lines.push('');
  fs.appendFileSync(WIKI_LOG_PATH, `${lines.join('\n')}\n`);
};

const recordWikiLintFailure = (meta, message, title = 'wiki lint failed', details = []) => {
  const failedAt = new Date().toISOString();
  const latestMeta = readWikiMeta();
  const failureMeta = latestMeta && Object.keys(latestMeta).length > 0 ? latestMeta : meta;
  failureMeta.last_lint_attempt = failedAt;
  failureMeta.last_lint_error = message;
  writeWikiMeta(failureMeta);
  refreshWikiIndex();
  appendWikiLog('lint', title, details.length > 0 ? details : [`error: ${message}`]);
  logWikiMetaSummary('📚 Wiki lint: recorded failure', failureMeta);
  return { attempted: true, success: false, error: message, attemptedAt: failedAt };
};

const parseTickSummary = (row) => {
  try {
    return typeof row?.summary === 'string' ? JSON.parse(row.summary) : (row?.summary || null);
  } catch {
    return null;
  }
};

const formatWikiNumber = (value, digits = 2, prefix = '') => {
  if (value == null || (typeof value === 'string' && value.trim() === '')) return 'unknown';
  const numeric = Number(value);
  return Number.isFinite(numeric) ? `${prefix}${numeric.toFixed(digits)}` : 'unknown';
};

const formatTickEvidenceLine = (row) => {
  const parsed = parseTickSummary(row) || {};
  const medium = parsed.medium_momentum?.main || parsed.medium_momentum || 'unknown';
  const short = parsed.short_momentum?.main || parsed.short_momentum || 'unknown';
  const source = row.id != null ? `tick:#${row.id}` : `tick:${row.timestamp}`;
  return `[${source}] ${row.timestamp} | spot=${formatWikiNumber(parsed.price, 2, '$')} | medium=${medium} | short=${short} | put_score=${formatWikiNumber(parsed.current_best_put, 4)} | call_score=${formatWikiNumber(parsed.current_best_call, 4)}`;
};

const formatOrderEvidenceLine = (order) => {
  const side = order.success ? 'OK' : 'FAIL';
  const reason = order.reason ? String(order.reason).replace(/\s+/g, ' ').slice(0, 120) : 'n/a';
  const source = order.id != null ? `order:#${order.id}` : `order:${order.timestamp}`;
  return `[${source}] ${order.timestamp} | ${side} | ${order.action} ${order.instrument_name || 'portfolio'} | value=${formatWikiNumber(order.total_value, 2, '$')} | reason=${reason}`;
};

const formatTradeReviewEvidenceLine = (review) => {
  return `[review:#${review.id}] ${review.review_window_days}d [${review.review_status}] | ${String(review.summary || '').replace(/\s+/g, ' ').slice(0, 220)}`;
};

const groupTradeReviewsForWiki = (reviews = [], maxCampaigns = 8) => {
  const campaigns = new Map();
  for (const review of reviews) {
    const key = `${review.instrument_name}|${review.closed_at}`;
    if (!campaigns.has(key)) {
      campaigns.set(key, {
        instrument_name: review.instrument_name,
        action_family: review.action_family,
        closed_at: review.closed_at,
        pnl_realized: review.pnl_realized,
        reviews: [],
      });
    }
    campaigns.get(key).reviews.push(review);
  }
  return [...campaigns.values()]
    .map((campaign) => ({
      ...campaign,
      reviews: campaign.reviews.sort((a, b) => Number(a.review_window_days || 0) - Number(b.review_window_days || 0)),
    }))
    .sort((a, b) => new Date(b.closed_at).getTime() - new Date(a.closed_at).getTime())
    .slice(0, maxCampaigns);
};

const formatTradeReviewCampaignEvidence = (campaign) => {
  const header = `Campaign ${campaign.instrument_name} | family=${campaign.action_family || 'unknown'} | closed=${campaign.closed_at} | pnl=${formatWikiNumber(campaign.pnl_realized, 2, '$')}`;
  return `${header}\n${campaign.reviews.map((review) => `  - ${formatTradeReviewEvidenceLine(review)}`).join('\n')}`;
};

const writeRawEvidencePacket = (journalEntries = []) => {
  if (!db) return null;
  const createdAt = new Date().toISOString();
  const fileName = `${createdAt.replace(/[:.]/g, '-')}_wiki-ingest.md`;
  const fullPath = path.join(WIKI_RAW_EVIDENCE_DIR, fileName);
  const relativePath = path.relative(WIKI_DIR, fullPath).replace(/\\/g, '/');
  const since7d = new Date(Date.now() - 7 * 86400000).toISOString();
  const recentTicks = db.getRecentTicks(6) || [];
  const recentOrders = db.getRecentOrders(since7d, 20) || [];
  const recentTradeReviews = db.getRecentTradeReviews(24) || [];
  const recentTradeCampaigns = groupTradeReviewsForWiki(recentTradeReviews, 8);
  const entriesText = journalEntries
    .map((entry) => `- [${entry.type || entry.entry_type || 'unknown'}] ${String(entry.content || '').replace(/\s+/g, ' ').slice(0, 260)}`)
    .join('\n');
  const content = [
    '# Raw Evidence Packet',
    '',
    `Created: ${createdAt}`,
    'Purpose: immutable evidence bundle for wiki compilation.',
    '',
    '## Source Hierarchy',
    '1. Factual market snapshots and order activity below are primary evidence.',
    '2. Reviewed trade campaigns are evaluated second-order evidence.',
    '3. Journal entries are analyst notes and may contain interpretation; do not treat them as facts without corroboration.',
    '',
    '## Factual Market Snapshots',
    recentTicks.length > 0 ? recentTicks.map(formatTickEvidenceLine).join('\n') : 'No recent tick snapshots.',
    '',
    '## Factual Order Activity (last 7d)',
    recentOrders.length > 0 ? recentOrders.map(formatOrderEvidenceLine).join('\n') : 'No recent orders.',
    '',
    '## Reviewed Trade Campaigns',
    recentTradeCampaigns.length > 0 ? recentTradeCampaigns.map(formatTradeReviewCampaignEvidence).join('\n\n') : 'No reviewed campaigns.',
    '',
    '## New Journal Entries',
    entriesText || 'No journal entries provided.',
    '',
  ].join('\n');
  fs.writeFileSync(fullPath, content);
  return { relativePath, content };
};

const truncateWikiContextAtBoundary = (content, maxChars) => {
  if (content.length <= maxChars) return content;
  const slice = content.slice(0, maxChars);
  const boundary = Math.max(slice.lastIndexOf('\n'), slice.lastIndexOf('. '));
  return `${slice.slice(0, boundary > maxChars * 0.6 ? boundary : maxChars).trim()}\n...[truncated]`;
};

const buildWikiAdvisorExcerpt = (content, maxChars = 1800) => {
  const chunks = String(content).split(/(?=^##\s+)/m);
  const preamble = chunks[0] || '';
  const priority = /##\s*(Classification|Evidence|Confidence|Current|Active|Cost Assessment|Premium Assessment|Core Rules|Falsification)/i;
  const selected = [preamble, ...chunks.slice(1).filter((chunk) => priority.test(chunk))].join('\n').trim();
  return truncateWikiContextAtBoundary(selected || content, maxChars);
};

const queryWikiContext = () => {
  const sections = [];
  const meta = readWikiMeta();
  const pageMeta = getWikiPageMetaMap(meta);
  const indexContent = readWikiPage(WIKI_INDEX_PAGE);
  if (indexContent) {
    const truncatedIndex = indexContent.length > 1200 ? indexContent.slice(0, 1200) + '\n...[truncated]' : indexContent;
    sections.push(`--- ${WIKI_INDEX_PAGE} ---\n${truncatedIndex}`);
  }
  for (const page of WIKI_KEY_PAGES) {
    const content = readWikiPage(page);
    if (!content || content.includes('Awaiting initial assessment')) continue;
    const stored = pageMeta[page] || {};
    const freshness = [
      `changed=${stored.last_changed_at || 'unknown'}`,
      `evidence=${stored.last_evidence_at || 'unknown'}`,
      `reviewed=${stored.last_reviewed_at || 'unreviewed'}`,
      `issues=${Array.isArray(stored.issues) ? stored.issues.length : 0}`,
      WIKI_STRATEGY_PAGES.has(page) ? 'owner=canonical-learning' : 'owner=wiki-research',
    ].join(' | ');
    sections.push(`--- ${page} ---\nMetadata: ${freshness}\n${buildWikiAdvisorExcerpt(content)}`);
  }
  return sections.length > 0 ? sections.join('\n\n') : '';
};

const saveWikiHistory = (pagePath, content) => {
  try {
    fs.mkdirSync(WIKI_HISTORY_DIR, { recursive: true });
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    const safeName = pagePath.replace(/\//g, '__');
    const historyPath = path.join(WIKI_HISTORY_DIR, `${ts}__${safeName}`);
    fs.writeFileSync(historyPath, content);
  } catch (e) {
    console.log('Wiki history save failed:', e.message);
  }
};

const isPlaceholderWikiPage = (content) => {
  return !content || content.includes('Awaiting initial assessment');
};

const getWikiPagesNeedingSeed = () => {
  return WIKI_ALL_PAGES.filter((page) => isPlaceholderWikiPage(readWikiPage(page)));
};

const inferWikiPagesForJournalEntries = (journalEntries = []) => {
  const selected = new Set(WIKI_LIVE_RESEARCH_PAGES);

  for (const entry of journalEntries) {
    const type = entry?.type || entry?.entry_type || '';
    const content = String(entry?.content || '').toLowerCase();

    if (type === 'regime_note') {
      selected.add('regimes/current.md');
      selected.add('regimes/history.md');
      selected.add('protection/pricing.md');
      selected.add('revenue/pricing.md');
    }

    if (type === 'hypothesis') {
      selected.add('protection/windows.md');
      selected.add('revenue/windows.md');
      selected.add('indicators/leading.md');
      selected.add('indicators/divergences.md');
    }

    if (type === 'observation') {
      selected.add('indicators/correlations.md');
      selected.add('indicators/divergences.md');
    }

    if (content.includes('convex') || content.includes('skew') || content.includes('tail')) {
      selected.add('protection/convexity.md');
    }
    if (content.includes('premium') || content.includes('call')) {
      selected.add('revenue/efficiency.md');
    }
    if (content.includes('mistake') || content.includes('discipline') || content.includes('rule')) {
      selected.add('strategy/mistakes.md');
      selected.add('strategy/playbook.md');
    }
  }

  return Array.from(selected).filter((page) => WIKI_ALL_PAGES.includes(page)).slice(0, 10);
};

const buildWikiIngestPagesContext = (pages, selectedPages) => {
  return selectedPages.map((page) => {
    const content = pages[page] || '';
    const truncated = content.length > 1200 ? `${content.slice(0, 1200)}\n...[truncated]` : content;
    return `--- ${page} ---\n${truncated}`;
  }).join('\n\n');
};

const seedWikiFromHistory = async (incomingEntries = []) => {
  if (!process.env.ANTHROPIC_API_KEY || !db) return 0;

  const pagesNeedingSeed = getWikiPagesNeedingSeed();
  if (pagesNeedingSeed.length === 0) {
    console.log('📚 Wiki seed: skipped — all pages already populated');
    return 0;
  }

  console.log(`📚 Wiki seed: bootstrapping ${pagesNeedingSeed.length} page(s): ${pagesNeedingSeed.join(', ')}`);

  const historicalJournal = db.getRecentJournalEntries(200) || [];
  const reviewedHypotheses = db.getReviewedHypotheses(50) || [];
  const activeLessons = db.getActiveLessons() || [];
  const recentTradeReviews = db.getRecentTradeReviews(36) || [];
  const recentTradeCampaigns = groupTradeReviewsForWiki(recentTradeReviews, 12);
  const activeTradeLessons = db.getActiveTradeLessons() || [];
  const recentTicks = db.getRecentTicks(12) || [];
  const recentOrders = db.getRecentOrders(new Date(Date.now() - 14 * 86400000).toISOString(), 30) || [];

  const mergedEntries = [...incomingEntries];
  for (const entry of historicalJournal) {
    const key = `${entry.timestamp}|${entry.entry_type}|${entry.content}`;
    if (!mergedEntries.some(e => `${e.timestamp}|${e.entry_type || e.type}|${e.content}` === key)) {
      mergedEntries.push(entry);
    }
  }

  if (mergedEntries.length === 0) {
    console.log('📚 Wiki seed: skipped — no journal history available');
    return 0;
  }

  const schema = readWikiPage('schema.md');
  const sampleEntries = mergedEntries.slice(0, 60);
  const journalText = sampleEntries
    .map(e => `[${e.entry_type || e.type || 'unknown'}] (${e.timestamp || 'unknown'}) ${String(e.content || '').slice(0, 300)}`)
    .join('\n\n');
  const tickText = recentTicks.map(formatTickEvidenceLine).join('\n');
  const ordersText = recentOrders.map(formatOrderEvidenceLine).join('\n');

  const hypothesesText = reviewedHypotheses.slice(0, 25)
    .map(h => `#${h.id} [${h.outcome_status}] — ${h.content.slice(0, 180)}... VERDICT: ${h.outcome_verdict || 'none'}`)
    .join('\n');

  const lessonsText = activeLessons.length > 0
    ? activeLessons.map(l => `- ${l.lesson} (evidence: ${l.evidence_count})`).join('\n')
    : 'None';

  const prompt = `You are bootstrapping missing knowledge wiki pages for a Spitznagel-style tail-risk hedging bot (ETH options on Lyra/Derive).

Synthesize the historical data below into ${pagesNeedingSeed.length} wiki page(s). Follow the schema. Be concise — each page should be 200-400 words.

## Wiki Schema
${schema}

## Source Hierarchy
1. Factual market snapshots and order activity are primary evidence.
2. Reviewed hypotheses and reviewed trade campaigns are evaluated second-order evidence.
3. Journal entries and existing lessons are analyst interpretation. Do not elevate them to factual claims without corroboration.

## Factual Market Snapshots
${tickText || 'No recent tick evidence available'}

## Factual Order Activity
${ordersText || 'No recent order evidence available'}

## Historical Journal Entries (${mergedEntries.length} total, showing ${sampleEntries.length})
${journalText}

## Reviewed Hypotheses (${reviewedHypotheses.length} with verdicts)
${hypothesesText}

## Active Lessons
${lessonsText}

## Reviewed Trade Campaigns (${recentTradeCampaigns.length} unique campaigns)
${recentTradeCampaigns.map(formatTradeReviewCampaignEvidence).join('\n\n') || 'None'}

## Active Trade Lessons
${activeTradeLessons.length > 0 ? activeTradeLessons.map(formatTradeLessonForPrompt).join('\n') : 'None'}

## Instructions
1. Generate ONLY the missing or placeholder wiki pages listed below
2. Use specific data values and dates from the raw evidence as evidence when available
3. Each page may start with one H1 title; place the bold TLDR line immediately after that optional title
4. Follow the required sections from the schema exactly
5. If journal interpretation conflicts with factual evidence, trust the factual evidence and write uncertainty explicitly
6. Cite material claims with the supplied [tick:#ID], [order:#ID], [review:#ID], or [lesson:key] marker; never invent a source marker
7. Strategy pages are views of canonical [lesson:key] records. Do not invent independent execution rules or promote disputed lessons as settled rules
8. Today's date: ${new Date().toISOString().split('T')[0]}

Output each page as:
<wiki_page path="regimes/current.md">
[full page content]
</wiki_page>

Generate ONLY these ${pagesNeedingSeed.length} page(s): ${pagesNeedingSeed.join(', ')}`;

  const response = await axios.post('https://api.anthropic.com/v1/messages', {
    model: ANTHROPIC_SONNET_MODEL,
    thinking: { type: 'disabled' },
    max_tokens: 8192,
    messages: [{ role: 'user', content: prompt }],
  }, {
    headers: {
      'x-api-key': process.env.ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json',
    },
    timeout: 180000,
  });

  const text = getAnthropicResponseText(response.data);
  const pageRegex = /<wiki_page\s+path="([^"]+)">([\s\S]*?)<\/wiki_page>/g;
  let match;
  let writeCount = 0;
  const writtenPages = [];

  while ((match = pageRegex.exec(text)) !== null) {
    const pagePath = match[1];
    const content = match[2].trim();
    if (!pagesNeedingSeed.includes(pagePath) || content.length < 50) continue;
    fs.writeFileSync(path.join(WIKI_DIR, pagePath), content);
    writeCount++;
    writtenPages.push(pagePath);
    console.log(`📚 Wiki seed: wrote ${pagePath}`);
  }

  const meta = readWikiMeta();
  const seededAt = new Date().toISOString();
  meta.seeded_at = seededAt;
  meta.seeded_pages = writeCount;
  meta.seeded_targets = pagesNeedingSeed;
  meta.seed_journal_entries_used = sampleEntries.length;
  for (const pagePath of writtenPages) {
    updateWikiPageMeta(meta, pagePath, {
      last_checked_at: seededAt,
      last_evidence_at: seededAt,
      last_changed_at: seededAt,
      last_reviewed_at: null,
      change_summary: 'Bootstrapped from historical evidence',
      evidence_packet: null,
      issues: [],
    });
  }
  writeWikiMeta(meta);
  refreshWikiIndex();
  appendWikiLog('seed', 'wiki bootstrap', [
    `pages written: ${writeCount}/${pagesNeedingSeed.length}`,
    `targets: ${pagesNeedingSeed.join(', ')}`,
    `journal entries used: ${sampleEntries.length}`,
  ]);

  console.log(`📚 Wiki seed: ${writeCount}/${pagesNeedingSeed.length} page(s) written`);
  return writeCount;
};

const ingestToWiki = async (journalEntries) => {
  if (!journalEntries || journalEntries.length === 0) return;
  if (!process.env.ANTHROPIC_API_KEY) return;

  if (getWikiPagesNeedingSeed().length > 0) {
    const seeded = await seedWikiFromHistory(journalEntries);
    if (seeded > 0) return;
  }

  console.log('📚 Wiki ingest: processing', journalEntries.length, 'journal entries...');

  // Read schema + targeted page subset to keep prompt compact.
  const schema = readWikiPage('schema.md');
  const pages = {};
  for (const page of WIKI_ALL_PAGES) {
    pages[page] = readWikiPage(page);
  }
  const selectedPages = inferWikiPagesForJournalEntries(journalEntries);
  const recentTradeReviews = db.getRecentTradeReviews(30) || [];
  const recentTradeCampaigns = groupTradeReviewsForWiki(recentTradeReviews, 10);
  const activeTradeLessons = db.getActiveTradeLessons() || [];
  const researchPages = selectedPages.filter((pagePath) => !WIKI_STRATEGY_PAGES.has(pagePath)).slice(0, 8);
  selectedPages.splice(0, selectedPages.length, ...researchPages);
  if (activeTradeLessons.some((lesson) => lesson.lesson_key)) {
    for (const pagePath of WIKI_STRATEGY_PAGES) {
      if (!selectedPages.includes(pagePath)) selectedPages.push(pagePath);
    }
  }
  const pagesContext = buildWikiIngestPagesContext(pages, selectedPages);
  const rawEvidencePacket = writeRawEvidencePacket(journalEntries);

  const entriesText = journalEntries
    .map(e => `[${e.type || e.entry_type || 'unknown'}] ${e.content}`)
    .join('\n\n---\n\n');

  const prompt = `You are maintaining a knowledge wiki for a Spitznagel-style tail-risk hedging bot. Your job is to compile wiki updates from evidence, not to restate speculative notes.

## Wiki Schema
${schema}

## Source Hierarchy
1. Raw evidence packet below is primary truth.
2. Reviewed trade campaigns and active trade lessons are evaluated second-order evidence.
3. Journal entries are analyst notes; use them to guide emphasis, but do not copy speculative language as fact without corroboration.

## Allowed Wiki Pages
${selectedPages.join('\n')}

## Current Wiki Pages (targeted excerpts)
${pagesContext}

## Raw Evidence Packet${rawEvidencePacket?.relativePath ? ` (${rawEvidencePacket.relativePath})` : ''}
${rawEvidencePacket?.content || 'No raw evidence packet available'}

## New Journal Entries
${entriesText}

## Reviewed Trade Campaigns
${recentTradeCampaigns.length > 0 ? recentTradeCampaigns.slice(0, 6).map(formatTradeReviewCampaignEvidence).join('\n\n') : 'None'}

## Active Trade Lessons
${activeTradeLessons.length > 0 ? activeTradeLessons.map(formatTradeLessonForPrompt).join('\n') : 'None'}

## Instructions
1. Reconcile every allowed live-state research page against the same raw evidence packet. Return an update for each page whose TLDR, Current, or Active section disagrees with newer evidence; do not omit a stale dependent page merely to minimize the update set
2. Preserve accurate durable and historical content. In TLDR, Current, and Active sections, replace superseded state instead of appending it; move useful prior observations into the matching Historical section
3. Add date stamps [${new Date().toISOString().split('T')[0]}] to new observations
4. If current data contradicts existing wiki content, use "Previously: X. Updated [date]: Y" only in historical context—never leave the obsolete value in the current TLDR or Active section
5. Keep each page under 2000 words — consolidate older entries if approaching limit
6. Each page may start with one H1 title; place a bold TLDR line reflecting current state immediately after that optional title
7. Prefer a compact update set only after every live-state page has been checked for cross-page consistency
8. Never update ${WIKI_INDEX_PAGE} or ${WIKI_LOG_PAGE}; the system maintains those deterministically
9. If evidence is thin or mixed, say so explicitly instead of over-asserting
10. Every materially new factual claim must cite an exact supplied [tick:#ID], [order:#ID], [review:#ID], or [lesson:key] marker; never invent a source marker
11. Strategy pages are Learning-owned views. They may summarize canonical [lesson:key] records, including status and contradictions, but must not invent independent execution rules or present disputed lessons as settled
12. Strategy pages contain durable conditional rules, not the current spot, skew, score, budget, gate state, or other live snapshot values. Live market state belongs in research pages and the trading advisory

Output your updates as XML blocks. Only include pages that need changes:

<wiki_update path="regimes/current.md">
[full updated page content]
</wiki_update>

If no pages need updating, output: <no_updates/>`;

  try {
    const response = await axios.post('https://api.anthropic.com/v1/messages', {
      model: ANTHROPIC_SONNET_MODEL,
      thinking: { type: 'disabled' },
      max_tokens: 4096,
      messages: [{ role: 'user', content: prompt }],
    }, {
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      timeout: 180000,
    });

    const text = getAnthropicResponseText(response.data);

    if (text.includes('<no_updates/>')) {
      const meta = readWikiMeta();
      const checkedAt = new Date().toISOString();
      meta.last_ingest = checkedAt;
      meta.last_ingest_updates = 0;
      meta.last_evidence_packet = rawEvidencePacket?.relativePath || meta.last_evidence_packet || null;
      for (const pagePath of selectedPages) {
        updateWikiPageMeta(meta, pagePath, {
          last_checked_at: checkedAt,
          evidence_packet: rawEvidencePacket?.relativePath || null,
        });
      }
      writeWikiMeta(meta);
      refreshWikiIndex();
      appendWikiLog('ingest', 'no wiki updates needed', [
        rawEvidencePacket?.relativePath ? `evidence packet: ${rawEvidencePacket.relativePath}` : 'evidence packet: none',
        `selected pages: ${selectedPages.join(', ')}`,
      ]);
      console.log('📚 Wiki ingest: no updates needed');
      return;
    }

    // Parse wiki_update blocks
    const updateRegex = /<wiki_update\s+path="([^"]+)">([\s\S]*?)<\/wiki_update>/g;
    let match;
    let updateCount = 0;
    const updatedPages = [];
    const learningContext = activeTradeLessons.map(formatTradeLessonForPrompt).join('\n');
    const campaignContext = recentTradeCampaigns.map(formatTradeReviewCampaignEvidence).join('\n\n');
    const allowedMarkers = getStructuredWikiMarkers(
      `${Object.values(pages).join('\n')}\n${rawEvidencePacket?.content || ''}\n${learningContext}\n${campaignContext}`,
    );
    const latestTickMarkers = new Set(
      Array.from(getStructuredWikiMarkers(rawEvidencePacket?.content || ''))
        .filter((marker) => marker.startsWith('[tick:')),
    );
    const availableLessonMarkers = new Set(
      Array.from(getStructuredWikiMarkers(learningContext))
        .filter((marker) => marker.startsWith('[lesson:')),
    );

    while ((match = updateRegex.exec(text)) !== null) {
      const pagePath = match[1];
      const newContent = match[2].trim();

      // Validate page path
      if (!selectedPages.includes(pagePath)) {
        console.log(`📚 Wiki ingest: rejected unknown page "${pagePath}"`);
        continue;
      }

      // Safety: reject updates < 50 chars
      if (newContent.length < 50) {
        console.log(`📚 Wiki ingest: rejected update for ${pagePath} — too short (${newContent.length} chars)`);
        continue;
      }

      // Safety: reject updates that shrink page by > 50%
      const existingContent = pages[pagePath] || '';
      if (existingContent.length > 100 && newContent.length < existingContent.length * 0.5) {
        console.log(`📚 Wiki ingest: rejected update for ${pagePath} — shrinks by >50% (${existingContent.length} -> ${newContent.length})`);
        continue;
      }

      // Safety: check expected section headers from schema
      const required = WIKI_EXPECTED_HEADERS[pagePath] || [];
      const missingHeaders = required.filter(h => !newContent.includes(h));
      if (missingHeaders.length > 0) {
        console.log(`📚 Wiki ingest: rejected update for ${pagePath} — missing sections: ${missingHeaders.join(', ')}`);
        continue;
      }

      const outputMarkers = getStructuredWikiMarkers(newContent);
      const unknownMarkers = Array.from(outputMarkers).filter((marker) => !allowedMarkers.has(marker));
      if (unknownMarkers.length > 0) {
        console.log(`📚 Wiki ingest: rejected update for ${pagePath} — invented source marker(s): ${unknownMarkers.slice(0, 5).join(', ')}`);
        continue;
      }
      if (
        WIKI_LIVE_RESEARCH_PAGES.includes(pagePath)
        && latestTickMarkers.size > 0
        && !Array.from(outputMarkers).some((marker) => latestTickMarkers.has(marker))
      ) {
        console.log(`📚 Wiki ingest: rejected update for ${pagePath} — live state lacks an exact marker from the latest tick packet`);
        continue;
      }
      if (
        WIKI_STRATEGY_PAGES.has(pagePath)
        && availableLessonMarkers.size > 0
        && !Array.from(outputMarkers).some((marker) => availableLessonMarkers.has(marker))
      ) {
        console.log(`📚 Wiki ingest: rejected update for ${pagePath} — Learning-owned page lacks a canonical lesson marker`);
        continue;
      }

      // Save history before overwriting
      if (existingContent && !existingContent.includes('Awaiting initial assessment')) {
        saveWikiHistory(pagePath, existingContent);
      }

      // Write updated page
      const fullPath = path.join(WIKI_DIR, pagePath);
      fs.writeFileSync(fullPath, newContent);
      updateCount++;
      updatedPages.push({ pagePath, existingContent, newContent });
      console.log(`📚 Wiki ingest: updated ${pagePath}`);
    }

    // Update meta
    const meta = readWikiMeta();
    const ingestedAt = new Date().toISOString();
    meta.last_ingest = ingestedAt;
    meta.last_ingest_updates = updateCount;
    meta.last_evidence_packet = rawEvidencePacket?.relativePath || meta.last_evidence_packet || null;
    for (const pagePath of selectedPages) {
      updateWikiPageMeta(meta, pagePath, {
        last_checked_at: ingestedAt,
        evidence_packet: rawEvidencePacket?.relativePath || null,
      });
    }
    for (const update of updatedPages) {
      updateWikiPageMeta(meta, update.pagePath, {
        last_evidence_at: ingestedAt,
        last_changed_at: ingestedAt,
        last_reviewed_at: null,
        issues: [],
        change_summary: summarizeWikiPageChange(
          update.existingContent,
          update.newContent,
          'Updated from the latest evidence packet',
        ),
      });
    }
    writeWikiMeta(meta);
    refreshWikiIndex();
    appendWikiLog('ingest', 'wiki ingest applied', [
      rawEvidencePacket?.relativePath ? `evidence packet: ${rawEvidencePacket.relativePath}` : 'evidence packet: none',
      `selected pages: ${selectedPages.join(', ')}`,
      `pages updated: ${updateCount}`,
    ]);

    console.log(`📚 Wiki ingest: ${updateCount} page(s) updated`);
  } catch (e) {
    console.log('📚 Wiki ingest failed:', e.message);
    throw e;
  }
};

const getWikiSectionText = (content, heading) => {
  const escaped = heading.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = String(content).match(new RegExp(`##\\s*${escaped}\\s*\\n([\\s\\S]*?)(?=\\n##|$)`, 'i'));
  return match ? match[1].trim() : '';
};

const getWikiAssessmentValue = (content, heading, allowed) => {
  const section = getWikiSectionText(content, heading).replace(/[*_`#:\-]/g, ' ');
  const match = section.match(new RegExp(`\\b(${allowed.join('|')})\\b`, 'i'));
  return match ? match[1].toLowerCase() : null;
};

const getWikiSignalContext = () => {
  try {
    const regimePage = readWikiPage('regimes/current.md');
    const playbookPage = readWikiPage('strategy/playbook.md');

    if (!regimePage || regimePage.includes('Awaiting initial assessment')) {
      return null;
    }

    const regime = getWikiAssessmentValue(regimePage, 'Classification', ['complacency', 'fear', 'transition', 'recovery']);
    const regimeConfidence = getWikiAssessmentValue(regimePage, 'Confidence', ['high', 'medium', 'low']);

    // Parse protection cost assessment from pricing page
    const pricingPage = readWikiPage('protection/pricing.md');
    const protectionAssessment = pricingPage
      ? getWikiAssessmentValue(pricingPage, 'Cost Assessment', ['cheap', 'fair', 'expensive'])
      : null;

    // Parse revenue/call premium assessment
    const revenuePage = readWikiPage('revenue/pricing.md');
    const revenueAssessment = revenuePage && !revenuePage.includes('Awaiting initial assessment')
      ? getWikiAssessmentValue(revenuePage, 'Premium Assessment', ['cheap', 'fair', 'rich'])
      : null;

    // Parse playbook rules (first 5 bullet points from Core Rules)
    const playbookRules = [];
    if (playbookPage) {
      const rulesSection = getWikiSectionText(playbookPage, 'Core Rules');
      if (rulesSection) {
        const bullets = rulesSection.match(/^[-*]\s+.+/gm);
        if (bullets) {
          playbookRules.push(...bullets.slice(0, 5).map(b => b.replace(/^[-*]\s+/, '')));
        }
      }
    }

    const meta = readWikiMeta();
    const pageMeta = getWikiPageMetaMap(meta);
    const signalPages = {
      regime: 'regimes/current.md',
      protection: 'protection/pricing.md',
      revenue: 'revenue/pricing.md',
      playbook: 'strategy/playbook.md',
    };
    const warnings = [];
    for (const [label, pagePath] of Object.entries(signalPages)) {
      const stored = pageMeta[pagePath] || {};
      if (!stored.last_reviewed_at) warnings.push(`${label} page is not page-level validated`);
      if (Array.isArray(stored.issues) && stored.issues.length > 0) warnings.push(`${label} page has ${stored.issues.length} validation issue(s)`);
    }
    return {
      regime,
      regimeConfidence,
      protectionAssessment,
      revenueAssessment,
      playbookRules,
      freshness: {
        regime: pageMeta['regimes/current.md'] || null,
        protection: pageMeta['protection/pricing.md'] || null,
        revenue: pageMeta['revenue/pricing.md'] || null,
        playbook: pageMeta['strategy/playbook.md'] || null,
      },
      warnings,
    };
  } catch {
    return null;
  }
};

const lintWiki = async ({ forcePageReview = false } = {}) => {
  if (!process.env.ANTHROPIC_API_KEY) {
    console.log('📚 Wiki lint: skipped — no ANTHROPIC_API_KEY');
    return { attempted: false, success: false, error: 'ANTHROPIC_API_KEY unavailable' };
  }

  const meta = readWikiMeta();
  logWikiMetaSummary('📚 Wiki lint: preflight', meta);
  const schedule = getWikiLintSchedule(meta);
  if (!schedule.due && !forcePageReview) {
    console.log(`📚 Wiki lint: skipped — ${schedule.lastError ? 'retry' : 'next review'} at ${schedule.nextDueAt}`);
    return { attempted: false, success: true, error: null, nextDueAt: schedule.nextDueAt };
  }

  const pagesToReview = schedule.needsInitialReview && schedule.pagesNeedingReview.length > 0
    ? schedule.pagesNeedingReview
    : WIKI_ALL_PAGES;
  const isFullReview = pagesToReview.length === WIKI_ALL_PAGES.length;
  console.log(isFullReview
    ? '📚 Wiki lint: auditing all wiki pages...'
    : `📚 Wiki lint: revalidating ${pagesToReview.length} changed/attention page(s) with full wiki context...`);

  // Read all pages
  const pages = {};
  let hasContent = false;
  for (const page of WIKI_ALL_PAGES) {
    pages[page] = readWikiPage(page);
    if (pages[page] && !pages[page].includes('Awaiting initial assessment')) hasContent = true;
  }
  if (!hasContent) {
    console.log('📚 Wiki lint: skipped — wiki not yet seeded');
    return recordWikiLintFailure(meta, 'wiki not yet seeded', 'wiki lint waiting for seed');
  }

  meta.last_lint_attempt = new Date().toISOString();
  meta.last_lint_error = null;
  writeWikiMeta(meta);

  const schema = readWikiPage('schema.md');
  const storedPageMeta = getWikiPageMetaMap(meta);
  const pagesContext = Object.entries(pages)
    .map(([p, content]) => {
      const stored = storedPageMeta[p] || {};
      const freshness = [
        `live_state=${WIKI_LIVE_RESEARCH_PAGES.includes(p)}`,
        `changed=${stored.last_changed_at || 'unknown'}`,
        `evidence=${stored.last_evidence_at || 'unknown'}`,
        `reviewed=${stored.last_reviewed_at || 'unreviewed'}`,
      ].join(' | ');
      return `--- ${p} ---\nMetadata: ${freshness}\n${content}`;
    })
    .join('\n\n');

  const prompt = `You are auditing a knowledge wiki for a Spitznagel-style tail-risk hedging bot. Check for quality issues.

## Wiki Schema
${schema}

## Current Wiki Pages
${pagesContext}

## Audit Scope
Use every page above as cross-page context, but report issues only for these target pages:
${pagesToReview.map((pagePath) => `- ${pagePath}`).join('\n')}

## Audit Checklist
1. **Contradictions**: Do any pages contradict each other?
2. **Staleness**: Are time-sensitive current-state claims old enough to mislead? Do not call stable historical or structural knowledge stale merely because it is old.
3. **Redundancy**: Is the same information repeated across pages?
4. **Missing links**: Do pages reference concepts that should be in another page but aren't?
5. **Quality**: Are TLDRs accurate? Are evidence values specific?
6. **Provenance**: Are material claims linked to supplied source, review, or canonical lesson markers where available?
7. **Ownership**: Do strategy pages faithfully summarize canonical [lesson:key] records without inventing a competing playbook?
8. **Live-state boundaries**: Do strategy pages avoid embedding current spot, skew, score, budget, or gate values that belong in research/advisory state?

## Instructions
Return one compact audit object and do not rewrite page content during validation:
<lint_result>{"issues":[{"page_to_fix":"path","reference_page":"path or null","type":"contradiction|stale|redundant|missing_link|quality","description":"concise actionable finding"}]}</lint_result>

Assign page_to_fix to the page whose content should change. For a cross-page conflict, use metadata and exact evidence markers to identify the older or less-supported page; never flag a newer accurate reference page merely because it exposes stale related content. Put that newer page in reference_page instead. Use at most two issues per target page, keep each description under 240 characters, and do not report findings against pages outside the audit scope. If no issues are found, return <lint_result>{"issues":[]}</lint_result>. Return no prose outside the tag.`;

  try {
    const response = await axios.post('https://api.anthropic.com/v1/messages', {
      model: ANTHROPIC_SONNET_MODEL,
      thinking: { type: 'disabled' },
      max_tokens: 1800,
      messages: [{ role: 'user', content: prompt }],
    }, {
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      timeout: 180000,
    });

    const text = getAnthropicResponseText(response.data);
    const lintMatch = text.match(/<lint_result>([\s\S]*?)<\/lint_result>/);
    let result = null;
    try {
      result = lintMatch ? JSON.parse(lintMatch[1].trim()) : extractJSON(text);
    } catch (parseErr) {
      console.log('📚 Wiki lint: malformed JSON in lint_result:', parseErr.message);
      return recordWikiLintFailure(
        meta,
        `malformed result: ${parseErr.message}`,
        'malformed lint result',
        [`parse error: ${parseErr.message}`],
      );
    }
    if (!result || !Array.isArray(result.issues)) {
      console.log('📚 Wiki lint: no valid structured result returned');
      return recordWikiLintFailure(
        meta,
        'invalid result schema',
        'no valid structured lint result',
        [`expected issues[] in tagged or raw JSON output; received keys: ${result && typeof result === 'object' ? Object.keys(result).join(', ') || 'none' : typeof result}`],
      );
    }
    const normalizedIssues = result.issues.map((issue) => ({
      ...issue,
      page: issue?.page_to_fix || issue?.page,
    }));
    const validIssues = normalizedIssues.filter((issue) => (
      issue && pagesToReview.includes(issue.page) && typeof issue.description === 'string'
    ));
    const ignoredIssueCount = normalizedIssues.length - validIssues.length;
    if (validIssues.length > 0) {
      console.log(`📚 Wiki lint: found ${validIssues.length} in-scope issue(s):`);
      for (const issue of validIssues) {
        console.log(`  - [${issue.type}] ${issue.page}${issue.reference_page ? ` (reference: ${issue.reference_page})` : ''}: ${issue.description}`);
      }
    } else {
      console.log('📚 Wiki lint: no in-scope issues found');
    }
    if (ignoredIssueCount > 0) console.log(`📚 Wiki lint: ignored ${ignoredIssueCount} out-of-scope finding(s)`);

    // Prune history files older than 30 days
    try {
      const PRUNE_AGE_MS = 30 * 24 * 60 * 60 * 1000;
      if (fs.existsSync(WIKI_HISTORY_DIR)) {
        const historyFiles = fs.readdirSync(WIKI_HISTORY_DIR);
        let pruned = 0;
        for (const file of historyFiles) {
          const filePath = path.join(WIKI_HISTORY_DIR, file);
          const stat = fs.statSync(filePath);
          if (Date.now() - stat.mtimeMs > PRUNE_AGE_MS) {
            fs.unlinkSync(filePath);
            pruned++;
          }
        }
        if (pruned > 0) console.log(`📚 Wiki lint: pruned ${pruned} old history file(s)`);
      }
    } catch (e) {
      console.log('📚 Wiki history prune failed:', e.message);
    }

    const reviewedAt = new Date().toISOString();
    const issuesByPage = new Map();
    for (const issue of validIssues) {
      if (!issuesByPage.has(issue.page)) issuesByPage.set(issue.page, []);
      const reference = issue.reference_page && WIKI_ALL_PAGES.includes(issue.reference_page) && issue.reference_page !== issue.page
        ? ` Reference: ${issue.reference_page}.`
        : '';
      issuesByPage.get(issue.page).push(`[${issue.type || 'quality'}] ${issue.description}${reference}`);
    }
    const completionMeta = readWikiMeta();
    let reviewedPageCount = 0;
    for (const pagePath of pagesToReview) {
      if (readWikiPage(pagePath) !== pages[pagePath]) {
        console.log(`📚 Wiki lint: ${pagePath} changed during audit; leaving it unreviewed`);
        continue;
      }
      updateWikiPageMeta(completionMeta, pagePath, {
        last_reviewed_at: reviewedAt,
        issues: issuesByPage.get(pagePath) || [],
      });
      reviewedPageCount++;
    }
    if (isFullReview) completionMeta.last_lint = reviewedAt;
    completionMeta.last_lint_attempt = reviewedAt;
    completionMeta.last_lint_error = null;
    const completedPageMeta = getWikiPageMetaMap(completionMeta);
    const outstandingIssueCount = WIKI_ALL_PAGES.reduce((total, pagePath) => (
      total + (Array.isArray(completedPageMeta[pagePath]?.issues) ? completedPageMeta[pagePath].issues.length : 0)
    ), 0);
    completionMeta.last_lint_issues = outstandingIssueCount;
    completionMeta.last_lint_updates = 0;
    writeWikiMeta(completionMeta);
    refreshWikiIndex();
    appendWikiLog('lint', isFullReview ? 'wiki lint complete' : 'wiki focused revalidation complete', [
      `issues in scope: ${validIssues.length}`,
      `outstanding issues: ${outstandingIssueCount}`,
      `pages reviewed: ${reviewedPageCount}/${pagesToReview.length} scoped (${WIKI_ALL_PAGES.length} total)`,
    ]);
    logWikiMetaSummary('📚 Wiki lint: wrote completion meta', completionMeta);

    console.log(`📚 Wiki lint: complete (${validIssues.length} scoped issue(s), ${reviewedPageCount}/${pagesToReview.length} target page(s), ${outstandingIssueCount} outstanding)`);
    return {
      attempted: true,
      success: true,
      error: null,
      reviewedAt,
      fullReview: isFullReview,
      reviewedPageCount,
      outstandingIssues: outstandingIssueCount,
    };
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e);
    console.log('📚 Wiki lint failed:', message);
    return recordWikiLintFailure(meta, message);
  }
};

// ─── Auto Journal Generation ─────────────────────────────────────────────────

const generateJournalEntries = async (tickSummary, botData) => {
  try {
    // Gather snapshot data
    const stats = db.getStats();
    const recentTicks = db.getRecentTicks(300);
    const since24h = new Date(Date.now() - TIME_CONSTANTS.DAY).toISOString();
    const since7d = new Date(Date.now() - TIME_CONSTANTS.WEEK).toISOString();
    const recentOnchain = db.getRecentOnchain(since24h);
    const recentPrices = db.getRecentSpotPrices(since7d);
    const previousJournal = db.getRecentJournalEntries(20);
    const recentSignals = db.getRecentSignals(since7d, 20);
    const optionsDistribution = db.getOptionsDistribution(since24h);
    const avgCallPremium = db.getAvgCallPremium7d();
    const recentOrders = db.getRecentOrders(since7d, 20);

    const { buildCorrelationAnalysis } = require('./bot/correlation');
    let correlations = null;
    try { correlations = buildCorrelationAnalysis(db); } catch { /* graceful fallback */ }

    // Fetch live account data from Derive
    const [currentPositions, currentCollaterals] = await Promise.all([
      fetchPositions(),
      fetchCollaterals(),
    ]);

    // Market sentiment data
    const fundingLatest = db.getFundingRateLatest();
    const fundingAvg24h = db.getFundingRateAvg24h();
    const optionsSkew = db.getOptionsSkew(since24h);
    const aggregateOI = db.getAggregateOI(since24h);
    const marketQuality = db.getMarketQualitySummary(since24h);

    // Sample arrays to keep prompt compact (~4K tokens of data)
    const sample = (arr, target) => {
      if (!arr || arr.length <= target) return arr || [];
      const step = Math.ceil(arr.length / target);
      return arr.filter((_, i) => i % step === 0);
    };

    const sampledTicks = sample(recentTicks, 24);
    const sampledPrices = sample(recentPrices, 48);
    const sampledOnchain = sample(recentOnchain, 24);

    // Compute put value vs price divergence analysis
    const putPriceDivergence = (() => {
      try {
        // Parse all ticks to get time series of put values and prices
        const parsed = recentTicks
          .map(t => { try { return { timestamp: t.timestamp, ...JSON.parse(t.summary) }; } catch { return null; } })
          .filter(t => t && t.price && t.current_best_put != null)
          .reverse(); // chronological order
        if (parsed.length < 6) return null;

        // Compute rolling changes over windows
        const windows = [6, 12, 24]; // tick windows (~30min, ~1h, ~2h at 5min ticks)
        const divergences = [];
        for (const w of windows) {
          if (parsed.length < w + 1) continue;
          const recent = parsed.slice(-w);
          const prior = parsed.slice(-(w * 2), -w);
          if (prior.length < 3) continue;

          const recentAvgPut = recent.reduce((s, t) => s + t.current_best_put, 0) / recent.length;
          const priorAvgPut = prior.reduce((s, t) => s + t.current_best_put, 0) / prior.length;
          const recentAvgPrice = recent.reduce((s, t) => s + t.price, 0) / recent.length;
          const priorAvgPrice = prior.reduce((s, t) => s + t.price, 0) / prior.length;

          const putChangePct = priorAvgPut > 0 ? ((recentAvgPut - priorAvgPut) / priorAvgPut) * 100 : 0;
          const priceChangePct = priorAvgPrice > 0 ? ((recentAvgPrice - priorAvgPrice) / priorAvgPrice) * 100 : 0;

          divergences.push({
            window_ticks: w,
            approx_hours: (w * 5 / 60).toFixed(1),
            put_value_change_pct: +putChangePct.toFixed(2),
            price_change_pct: +priceChangePct.toFixed(2),
            divergence: +(putChangePct - priceChangePct).toFixed(2),
            signal: putChangePct > 5 && priceChangePct > -1 ? 'PUT_SPIKE_PRICE_FLAT' :
                    putChangePct > 5 && priceChangePct < -1 ? 'PUT_SPIKE_PRICE_DROPPING' :
                    putChangePct < -5 && priceChangePct > -1 ? 'PUT_CHEAP_PRICE_STABLE' : 'NEUTRAL',
          });
        }

        // Find historical spike-then-drop episodes from 7d price data
        const episodes = [];
        const allTicks = recentTicks
          .map(t => { try { return { timestamp: t.timestamp, ...JSON.parse(t.summary) }; } catch { return null; } })
          .filter(t => t && t.price && t.current_best_put != null)
          .reverse();
        // Scan for put spikes (>10% jump over 6 ticks) followed by price drops within 24 ticks
        for (let i = 6; i < allTicks.length - 12; i++) {
          const prevPut = allTicks.slice(i - 6, i).reduce((s, t) => s + t.current_best_put, 0) / 6;
          const currPut = allTicks[i].current_best_put;
          if (prevPut > 0 && ((currPut - prevPut) / prevPut) > 0.10) {
            // Put spiked — check if price dropped within next 24 ticks
            const priceAtSpike = allTicks[i].price;
            const futureWindow = allTicks.slice(i + 1, i + 25);
            const minFuturePrice = Math.min(...futureWindow.map(t => t.price));
            const priceDropPct = ((minFuturePrice - priceAtSpike) / priceAtSpike) * 100;
            if (priceDropPct < -0.5) {
              episodes.push({
                spike_time: allTicks[i].timestamp,
                put_spike_pct: +(((currPut - prevPut) / prevPut) * 100).toFixed(1),
                subsequent_price_drop_pct: +priceDropPct.toFixed(2),
                lag_ticks_to_min: futureWindow.indexOf(futureWindow.find(t => t.price === minFuturePrice)) + 1,
              });
            }
          }
        }

        return {
          current_divergences: divergences,
          historical_spike_then_drop_episodes: episodes.slice(-5), // last 5
          interpretation: 'PUT_SPIKE_PRICE_FLAT = puts getting expensive while price stable (market pricing risk before spot moves). PUT_CHEAP_PRICE_STABLE = cheap protection opportunity.',
        };
      } catch { return null; }
    })();

    // Build snapshot
    const snapshot = {
      current_tick: tickSummary,
      stats,
      recent_ticks_24h: sampledTicks.map(t => {
        try { return { timestamp: t.timestamp, ...JSON.parse(t.summary) }; } catch { return t; }
      }),
      onchain_24h: sampledOnchain,
      prices_7d: sampledPrices.map(p => ({
        timestamp: p.timestamp,
        price: p.price,
        medium_momentum: p.medium_momentum_main || null,
        short_momentum: p.short_momentum_main || null,
      })),
      sizing_note: 'All position sizing is margin-aware — advisory sets budget_limit per rule based on account margin health',
      previous_journal: previousJournal,
      signals_7d: recentSignals.map(s => ({
        timestamp: s.timestamp,
        type: s.signal_type,
        acted_on: s.acted_on,
        details: s.details,
      })),
      options_market: {
        distribution: optionsDistribution,
        avg_call_premium_7d: avgCallPremium?.avg_premium ?? null,
        broad_quote_snapshot: summarizeMarketQualitySnapshotForLLM(marketQuality),
      },
      pool_breakdown: (() => {
        try {
          if (!recentOnchain.length || !recentOnchain[0].raw_data) return null;
          const raw = JSON.parse(recentOnchain[0].raw_data);
          const dexes = raw?.dexLiquidity?.dexes;
          if (!dexes) return null;
          return Object.entries(dexes).map(([name, dex]) => ({
            dex: name,
            total_liquidity: dex.totalLiquidity ?? null,
            pool_count: dex.pools?.length ?? null,
            top_pool: dex.pools?.[0] ? {
              pair: dex.pools[0].pair || dex.pools[0].name || null,
              liquidity: dex.pools[0].liquidity ?? dex.pools[0].totalLiquidity ?? null,
            } : null,
          }));
        } catch { return null; }
      })(),
      cross_correlations: correlations,
      put_price_divergence: putPriceDivergence,
      recent_orders: recentOrders.map(o => ({
        timestamp: o.timestamp,
        action: o.action,
        success: !!o.success,
        instrument_name: o.instrument_name,
        strike: o.strike,
        expiry: o.expiry,
        delta: o.delta,
        fill_price: o.fill_price,
        filled_amount: o.filled_amount,
        total_value: o.total_value,
        spot_price: o.spot_price,
        reason: o.reason,
      })),
      current_positions: currentPositions,
      account: {
        collaterals: currentCollaterals,
      },
      market_sentiment: (() => {
        try {
          const latestSkew = optionsSkew.length > 0 ? optionsSkew[optionsSkew.length - 1] : null;
          const currentSkew = latestSkew && latestSkew.avg_put_iv != null && latestSkew.avg_call_iv != null
            ? +((latestSkew.avg_put_iv - latestSkew.avg_call_iv) * 100).toFixed(2)
            : null;
          const currentOI = aggregateOI.length > 0 ? aggregateOI[aggregateOI.length - 1].total_oi : null;
          const firstOI = aggregateOI.length > 1 ? aggregateOI[0].total_oi : null;
          const oiChange24hPct = currentOI && firstOI && firstOI > 0
            ? +(((currentOI - firstOI) / firstOI) * 100).toFixed(1)
            : null;
          let fundingTrend = null;
          if (fundingLatest && fundingAvg24h != null) {
            fundingTrend = fundingLatest.rate > fundingAvg24h ? 'rising' : fundingLatest.rate < fundingAvg24h ? 'declining' : 'stable';
          }
          return {
            funding_rate: {
              current: fundingLatest?.rate ?? null,
              avg_24h: fundingAvg24h,
              trend: fundingTrend,
            },
            options_skew_pct: currentSkew,
            aggregate_oi: {
              current: currentOI,
              change_24h_pct: oiChange24hPct,
            },
          };
        } catch { return null; }
      })(),
    };

    // Build hypothesis performance summary for prompt injection
    let hypothesisPerformance = '';
    let tradeLearningContext = '';
    try {
      const hypStats = db.getHypothesisStats(30);
      const lessons = db.getActiveLessons();
      const recentVerdicts = db.getReviewedHypotheses(5);
      const tradeLessons = db.getActiveTradeLessons();
      const recentTradeReviews = db.getRecentTradeReviews(5);

      if (hypStats && hypStats.total > 0) {
        const reviewed = hypStats.total - (hypStats.pending || 0);
        const convexPosture = reviewed > 0
          ? (((hypStats.confirmed_convex || 0) + (hypStats.disproven_bounded || 0)) / reviewed * 100).toFixed(0)
          : 'N/A';
        const costlyRate = reviewed > 0
          ? ((hypStats.disproven_costly || 0) / reviewed * 100).toFixed(0)
          : 'N/A';

        hypothesisPerformance = `\n\n=== HYPOTHESIS PERFORMANCE (last 30 days) ===
Total hypotheses: ${hypStats.total} (${reviewed} reviewed, ${hypStats.pending || 0} pending)
Convex posture rate: ${convexPosture}% (confirmed_convex + disproven_bounded) / reviewed
Costly miss rate: ${costlyRate}% (disproven_costly / reviewed)
Breakdown: ${hypStats.confirmed_convex || 0} convex wins, ${hypStats.confirmed_linear || 0} linear wins, ${hypStats.disproven_bounded || 0} bounded losses (OK), ${hypStats.disproven_costly || 0} costly losses (BAD), ${hypStats.partially_confirmed || 0} partial

Recent verdicts:
${recentVerdicts.map(v => `#${v.id} [${v.outcome_status}]: ${v.outcome_verdict || 'no verdict text'}`).join('\n')}

${lessons.length > 0 ? `Active lessons:\n${lessons.map(l => `- ${l.lesson} (evidence: ${l.evidence_count})`).join('\n')}` : ''}

IMPORTANT: A high disproven_bounded rate means the bot is buying cheap insurance that expires worthless — that IS the strategy working. Focus on reducing disproven_costly rate (buying expensive protection), not on increasing prediction accuracy. The best hypothesis identifies when protection is cheap or call premium is richly paid, not where price goes. ETH direction is second-order evidence that matters only through option pricing, liquidity, skew, OI, funding, and realized payoff geometry. Each hypothesis MUST identify what makes the opportunity asymmetric — why is the downside bounded, where is the cheap convexity, or why is the call risk well paid?`;
      }

      if (tradeLessons.length > 0 || recentTradeReviews.length > 0) {
        tradeLearningContext = `\n\n=== TRADE LEARNING ===
Recent trade reviews:
${recentTradeReviews.length > 0 ? recentTradeReviews.map(r => `- ${r.instrument_name} [${r.review_status}] [${r.review_window_days}d]: ${r.summary}`).join('\n') : 'None'}

Active trade lessons:
${tradeLessons.length > 0 ? tradeLessons.map(formatTradeLessonForPrompt).join('\n') : 'None'}

Use these trade lessons to improve strike selection, execution, and exit timing. Judge whether past losing trades were still correct at the time, and whether profitable trades were actually disciplined.`;
      }
    } catch (e) {
      console.log('📓 Failed to build hypothesis performance summary:', e.message);
    }

    const systemPromptBase = `You are the Spitznagel Bot — a tail-risk hedging advisor operating on ETH options with Universa-style principles. You maintain an analytical journal tracking market observations, hypotheses, and regime assessments.

**STRATEGIC PRIORITY — TWO-SIDED ANALYSIS:**
1. **PUT BUYING (primary):** Evaluating OTM PUT BUYING WINDOWS — when is protection cheap, when is convexity high, when should the bot accumulate puts? This is the core mission.
2. **CALL SELLING (secondary):** Evaluating call premium environment — when is premium rich, when should the bot sell calls? Call selling is an independent revenue operation that also has the side effect of financing put protection. Both sides serve the same portfolio but are evaluated on their own merits.

The regime is the SAME for both sides — it's a property of the market, not of a strategy leg. But each side responds differently:
- **Complacency:** Puts are cheap (accumulate aggressively). Call premium is moderate — IV is low, so premiums are thin but selling is low-risk.
- **Greed/euphoria:** Puts are cheap and ignored. Call premiums are RICHEST here — high IV on upside, crowd paying up for calls. Best window for call selling revenue.
- **Fear:** Puts are expensive (don't chase). Call premium may spike but selling is dangerous — realized vol is high.
- **Transition:** Both sides need reassessment — old pricing assumptions break down.

ETH direction is second-order evidence. Notice spot moves, but do not let them outrank the primary question: are options mispriced in a way that improves bang-for-buck risk mitigation or pays us enough to take smart, survivable call risk?

The journal (observation, hypothesis, regime_note) should track:
- Is OTM put protection getting cheaper or more expensive? (IV environment, skew, put delta-value scores)
- Is call premium rich or thin? (call IV, premium/delta ratios, term structure)
- Are macro conditions building toward a crash? (flows, leverage, funding, OI structure)
- What regime are we in and what does it mean for BOTH put buying AND call selling?
- Where is the cheap convexity in the put chain right now?
- Where is the best risk-adjusted premium in the call chain right now?

Analyze the provided snapshot across three time scales:

**Short-term (hours):** Price action, short momentum shifts, spike events — how do they affect put pricing AND call premium?
**Medium-term (days):** Trend direction changes, momentum regime shifts, onchain flow patterns, protection cost trends, premium harvest trends.
**Long-term (week+):** Structural patterns, correlation shifts, regime transitions, compounding geometry of both put protection and call financing.

**Recent trades:** The snapshot includes recent_orders — actual put buys and call sells executed by the bot. Evaluate PUT trades first: was the timing good, was the strike/delta appropriate, did we get good value on protection? Then evaluate CALL trades: was premium rich, was the strike safe, was the risk-adjusted return good?

**IMPORTANT — No position data in journal entries:** The regime_note, hypothesis, and observation entries must focus EXCLUSIVELY on market conditions, price action, flows, and external signals — do NOT mention specific positions, P&L, greeks, or trading actions in journal entries.

Review your previous journal entries. Confirm patterns that held, revise those that didn't, and contradict past assessments when data warrants it.

Output exactly 3 journal entries — one of each type, in this order (do NOT include a suggestion entry):

1. First, a REGIME NOTE classifying the current market state (MARKET CONDITIONS ONLY — no positions or P&L):
<journal type="regime_note">Classify the current regime (complacency, fear, transition, etc.) and assess what it means for BOTH sides:
- **Protection side:** How cheap is put protection relative to tail risk? During complacency, accumulate — puts are cheap when nobody wants them.
- **Revenue side:** How rich is call premium relative to the risk of selling? During fear, premium is juicy but tread carefully.
Focus on IV levels, put/call skew, term structure, realized vs implied vol. Do NOT mention specific positions.</journal>

2. Then, a HYPOTHESIS about PROTECTION OR REVENUE CONDITIONS (not price predictions):
<journal type="hypothesis">Primary hypothesis about protection cost OR call premium trajectory. Frame around mispricing — when puts are cheap or calls are rich relative to what the market should price. The hypothesis can focus on either side but must note the implication for the other side.

Example: "Put skew compressing while realized vol picks up — protection will get more expensive within 24h, making now a cheap window. Call side implication: if put IV rises, call IV likely follows — premium harvest window may also be opening."

IMPORTANT: After your hypothesis prose, include a structured metadata block:
<hypothesis_meta>{"target":"put cost","direction":"cheaper|expensive|above|below","value":0.15,"deadline":"2026-03-04T01:39:00Z","falsification":"If 30-day put IV doesn't rise above 60% within 24h"}</hypothesis_meta>

The metadata must have:
- target: what you're assessing (e.g. "put cost", "put skew", "call premium", "IV term structure", "ETH spot")
- direction: "cheaper", "expensive", "above", or "below"
- value: the numeric threshold
- deadline: ISO timestamp for when to check
- falsification: plain text summary of what would disprove it

Every hypothesis MUST identify what makes the opportunity asymmetric — why is the downside bounded? Where is the cheap convexity or rich premium?</journal>

3. Finally, an OBSERVATION documenting the most notable factual pattern in MARKET DATA (not positions):
<journal type="observation">The single most important factual pattern in the current market data — price action, flows, volatility, or structural signals. Include a secondary observation about the call premium environment if notable (e.g., call IV diverging from put IV, unusual call skew, premium compression). Do NOT discuss positions or P&L here.</journal>

IMPORTANT: Start every journal entry with a single bold TLDR line (e.g., "**TLDR: Put protection costs dropped 15% while call premium stays rich — favorable both-sides window.**"). Follow with detailed analysis. Keep each entry under 350 words — be dense and precise, not verbose. All 3 entries must fit within the response.

## Put Value / Price Divergence
The snapshot includes a put_price_divergence section that detects when put option values move independently of spot price:
- **current_divergences**: Put value vs price changes over multiple windows. A PUT_SPIKE_PRICE_FLAT signal means the options market is pricing in downside risk before spot moves — puts are getting expensive while price holds. PUT_CHEAP_PRICE_STABLE means cheap protection is available.
- **historical_spike_then_drop_episodes**: Past instances where a put value spike preceded a price drop, with timing data. Use these to calibrate how predictive put spikes are for this market.

This is critical for the Spitznagel strategy: we want to buy puts when they're CHEAP (before the market prices in risk), not after a spike. If put spikes reliably lead price drops, the bot should be accumulating protection during PUT_CHEAP_PRICE_STABLE windows.

Ground everything in the data. Focus on: cost of protection (put pricing), revenue opportunity (call premium), crash probability (flow reversals), and portfolio geometry (how put+call positions work together). Treat ETH direction as useful context only after those option-market questions are answered.${hypothesisPerformance}${tradeLearningContext}`;

    // Inject wiki context if available
    const wikiContext = queryWikiContext();
    const wikiSection = wikiContext ? `

=== KNOWLEDGE WIKI (cumulative bot knowledge) ===
${wikiContext}

Use this wiki context to:
1. Build on confirmed patterns rather than rediscovering them
2. Reference specific wiki findings when forming hypotheses
3. Avoid repeating observations already well-documented in the wiki
4. Challenge wiki assessments when current data contradicts them` : '';

    const systemPrompt = systemPromptBase + wikiSection;

    const userMessage = `Here is today's snapshot for journal analysis:\n\n${JSON.stringify(snapshot, null, 2)}\n\nWrite exactly 3 journal entries: one regime_note, one hypothesis, one observation. Use the <journal type="..."> tags. Do NOT write a suggestion entry.`;

    const response = await callAnthropicWithMinuteBoundaryRetry({
      label: 'Journal generation',
      model: ANTHROPIC_SONNET_MODEL,
      maxTokens: 4096,
      system: systemPrompt,
      messages: [{ role: 'user', content: userMessage }],
      timeout: 120000,
    });

    const text = getAnthropicResponseText(response.data);

    // Extract journal entries
    const regex = /<journal\s+type="(observation|hypothesis|regime_note)">([\s\S]*?)<\/journal>/g;
    const metaRegex = /<hypothesis_meta>([\s\S]*?)<\/hypothesis_meta>/;
    const seriesNames = ['spot_return', 'liquidity_flow', 'best_put_dv', 'best_call_dv', 'options_spread', 'options_depth', 'open_interest', 'implied_vol'];
    let match;
    let count = 0;
    const parsedEntries = [];

    while ((match = regex.exec(text)) !== null) {
      const entryType = match[1];
      let content = match[2].trim();
      if (!content) continue;

      const referenced = seriesNames.filter(s =>
        content.toLowerCase().includes(s.replace(/_/g, ' ')) || content.includes(s)
      );

      if (entryType === 'hypothesis') {
        // Extract structured metadata
        const metaMatch = content.match(metaRegex);
        let meta = null;
        if (metaMatch) {
          try {
            meta = JSON.parse(metaMatch[1].trim());
          } catch (e) {
            console.log('📓 Failed to parse hypothesis_meta JSON:', e.message);
          }
          // Strip meta tag from displayed content
          content = content.replace(metaRegex, '').trim();
        }
        db.insertJournalEntryFull(entryType, content, referenced.length > 0 ? referenced : null, meta);
      } else {
        db.insertJournalEntry(entryType, content, referenced.length > 0 ? referenced : null);
      }
      parsedEntries.push({ type: entryType, content });
      count++;
    }

    console.log(`📓 Journal: generated ${count} entries`);
    if (count < 3) {
      console.log(`⚠️ Expected 3 journal entries but only extracted ${count}`);
    }
    return parsedEntries;
  } catch (e) {
    const detail = formatAnthropicErrorForLog(e);
    console.log('📓 Journal generation failed:', detail);
    const wrapped = new Error(detail);
    wrapped.cause = e;
    wrapped.status = e?.response?.status;
    throw wrapped;
  }
};

// ─── OpenAI API Helper ───────────────────────────────────────────────────────
const OPENAI_STRATEGY_MODEL = process.env.OPENAI_STRATEGY_MODEL || 'gpt-6-astra';
const OPENAI_CONFIRMATION_MODEL = process.env.OPENAI_CONFIRMATION_MODEL || 'gpt-5.6-terra';

// maxTokens includes both reasoning and visible output for these models.
const callOpenAI = async (systemPrompt, userPrompt, {
  maxTokens = 16384, timeout = 120000, model = OPENAI_STRATEGY_MODEL, reasoningEffort = 'medium',
} = {}) => {
  if (!process.env.OPENAI_API_KEY) return null;
  try {
    const response = await axios.post('https://api.openai.com/v1/chat/completions', {
      model,
      max_completion_tokens: maxTokens,
      reasoning_effort: reasoningEffort,
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userPrompt },
      ],
    }, {
      headers: {
        'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`,
        'Content-Type': 'application/json',
      },
      timeout,
    });
    const choice = response.data?.choices?.[0];
    if (choice?.finish_reason !== 'stop') {
      console.log(`⚠️ OpenAI ${model}: incomplete response (${choice?.finish_reason || 'missing choice'})`);
      return null;
    }
    return choice.message?.content || '';
  } catch (e) {
    console.log(`⚠️ OpenAI API call failed: ${e.message}`);
    return null;
  }
};

const ANTHROPIC_SONNET_MODEL = process.env.ANTHROPIC_SONNET_MODEL || process.env.ANTHROPIC_MODEL || 'claude-sonnet-5';
const ANTHROPIC_STRATEGY_MODEL = process.env.ANTHROPIC_STRATEGY_MODEL || 'claude-fable-5-1';

// Thinking blocks can precede text; never interpret them as the final answer.
const getAnthropicResponseText = (data) => {
  if (data?.stop_reason !== 'end_turn') return '';
  return (data.content || []).filter(block => block.type === 'text').map(block => block.text).join('');
};

const getAnthropicResponseFailure = (data) => {
  const stopReason = data?.stop_reason || 'missing';
  const outputTokens = data?.usage?.output_tokens ?? 'unknown';
  if (stopReason === 'max_tokens') return `truncated response (max_tokens; output_tokens=${outputTokens})`;
  if (stopReason !== 'end_turn') return `incomplete response (stop_reason=${stopReason}; output_tokens=${outputTokens})`;
  return `empty text response (stop_reason=end_turn; output_tokens=${outputTokens})`;
};

const getAnthropicErrorMessage = (error) => {
  const apiMessage = error?.response?.data?.error?.message;
  if (apiMessage) return apiMessage;
  return error?.message || 'Unknown Anthropic error';
};

const formatAnthropicErrorForLog = (error) => {
  const status = error?.response?.status;
  const data = error?.response?.data;
  const apiError = data?.error || {};
  const type = apiError.type || data?.type || null;
  const message = apiError.message || data?.message || error?.message || 'Unknown Anthropic error';
  const requestId = error?.response?.headers?.['request-id'] || error?.response?.headers?.['anthropic-request-id'] || null;
  const parts = [];
  if (status) parts.push(`status=${status}`);
  if (type) parts.push(`type=${type}`);
  parts.push(`message=${message}`);
  if (requestId) parts.push(`request_id=${requestId}`);
  if (data && !apiError.message && !data.message) {
    try { parts.push(`body=${JSON.stringify(data).slice(0, 1000)}`); } catch { /* ignore */ }
  }
  return parts.join(' | ');
};

const isAnthropicAccelerationLimitError = (error) => {
  const message = String(getAnthropicErrorMessage(error) || '').toLowerCase();
  return message.includes('maximum usage increase rate')
    || message.includes('acceleration limit')
    || (message.includes('input tokens per minute') && message.includes('next minute boundary'));
};

const isRetryableAnthropicServerError = (error) => {
  const status = Number(error?.response?.status || 0);
  const message = String(getAnthropicErrorMessage(error) || '').toLowerCase();
  return status === 500
    || status === 502
    || status === 503
    || status === 504
    || message.includes('internal server error');
};

const waitUntilNextMinuteBoundary = async (label = 'Anthropic retry') => {
  const now = Date.now();
  const nextMinute = Math.ceil(now / 60000) * 60000;
  const delayMs = Math.max(1500, nextMinute - now + 1500);
  console.log(`⏱️ ${label}: waiting ${(delayMs / 1000).toFixed(1)}s for next minute boundary`);
  await new Promise((resolve) => setTimeout(resolve, delayMs));
};

const waitWithBackoff = async (label, attempt, baseDelayMs = 4000) => {
  const delayMs = baseDelayMs * attempt;
  console.log(`⏱️ ${label}: retrying in ${(delayMs / 1000).toFixed(1)}s after transient Anthropic server error`);
  await new Promise((resolve) => setTimeout(resolve, delayMs));
};

const callAnthropicWithMinuteBoundaryRetry = async ({
  label,
  model,
  maxTokens,
  system,
  messages,
  timeout = 120000,
  spreadAfterBoundary = false,
  maxServerErrorRetries = 2,
  thinking = { type: 'disabled' },
}) => {
  const attemptCall = () => axios.post('https://api.anthropic.com/v1/messages', {
    model,
    max_tokens: maxTokens,
    thinking,
    system,
    messages,
  }, {
    headers: {
      'x-api-key': process.env.ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json',
    },
    timeout,
  });

  let serverErrorRetries = 0;
  let accelerationRetries = 0;

  while (true) {
    try {
      return await attemptCall();
    } catch (error) {
      if (isAnthropicAccelerationLimitError(error)) {
        if (accelerationRetries >= 1) throw error;
        accelerationRetries += 1;
        console.log(`⏱️ ${label}: Anthropic acceleration limit hit — ${getAnthropicErrorMessage(error)}`);
        await waitUntilNextMinuteBoundary(label);
        if (spreadAfterBoundary) {
          await new Promise((resolve) => setTimeout(resolve, 2500));
        }
        continue;
      }

      if (isRetryableAnthropicServerError(error) && serverErrorRetries < maxServerErrorRetries) {
        serverErrorRetries += 1;
        console.log(`⏱️ ${label}: transient Anthropic server error (${serverErrorRetries}/${maxServerErrorRetries}) — ${getAnthropicErrorMessage(error)}`);
        await waitWithBackoff(label, serverErrorRetries);
        continue;
      }

      throw error;
    }
  }
};

const summarizeSentimentWindowForLLM = (windowLabel, sentiment) => {
  const skewRows = Array.isArray(sentiment?.optionsSkew) ? sentiment.optionsSkew : [];
  const latestSkew = skewRows.length > 0 ? skewRows[skewRows.length - 1] : null;
  const validSkewRows = skewRows.filter(r => r.avg_put_iv != null && r.avg_call_iv != null);
  const currentSkewPct = latestSkew?.avg_put_iv != null && latestSkew?.avg_call_iv != null
    ? +(((latestSkew.avg_put_iv - latestSkew.avg_call_iv) * 100).toFixed(2))
    : null;
  const avgSkewPct = validSkewRows.length > 0
    ? +((validSkewRows.reduce((sum, row) => sum + (row.avg_put_iv - row.avg_call_iv), 0) / validSkewRows.length) * 100).toFixed(2)
    : null;

  let skewDirection = 'unknown';
  if (currentSkewPct != null && avgSkewPct != null) {
    skewDirection = currentSkewPct > avgSkewPct ? 'widening' : currentSkewPct < avgSkewPct ? 'narrowing' : 'stable';
  }

  const oiRows = Array.isArray(sentiment?.aggregateOI) ? sentiment.aggregateOI : [];
  const currentOI = oiRows.length > 0 ? Number(oiRows[oiRows.length - 1].total_oi) : null;
  const firstOI = oiRows.length > 1 ? Number(oiRows[0].total_oi) : null;
  const oiChangePct = currentOI != null && firstOI > 0
    ? +((((currentOI - firstOI) / firstOI) * 100).toFixed(1))
    : null;

  const funding = summarizeFundingRates(sentiment?.fundingRates);

  return {
    window: windowLabel,
    funding_rate: funding,
    options_skew: {
      current_pct: currentSkewPct,
      avg_pct: avgSkewPct,
      direction: skewDirection,
      samples: validSkewRows.length,
    },
    aggregate_oi: {
      current: currentOI,
      change_pct: oiChangePct,
      samples: oiRows.length,
    },
  };
};

const summarizeMarketQualitySnapshotForLLM = (marketQualityRows = []) => {
  const rows = Array.isArray(marketQualityRows) ? marketQualityRows : [];
  if (rows.length === 0) return null;
  const numeric = (value, multiplier = 1, digits = 2) => {
    if (value == null || value === '') return null;
    const number = Number(value);
    return Number.isFinite(number) ? +((number * multiplier).toFixed(digits)) : null;
  };
  const timestamps = rows.map(row => row.snapshot_timestamp);
  const oneTimestamp = timestamps.every(value => value === timestamps[0])
    && Number.isFinite(Date.parse(timestamps[0]));
  const universe = rows.every(row => row.universe === rows[0].universe)
    ? rows[0].universe || 'unknown' : 'mixed_or_unknown';
  return {
    as_of: oneTimestamp ? timestamps[0] : null,
    universe,
    scope: universe === 'observed_all_tenors_abs_delta_0.02_to_0.12'
      ? 'All recorded tenors with |delta| 0.02–0.12; no entry DTE filter; recorded observations are not full-exchange coverage'
      : 'Snapshot universe unavailable',
    measurement: 'Single latest snapshot; spread = (ask − bid) / mark. Not a time-window average or an eligible-candidate execution estimate.',
    sides: rows.map(row => ({
      option_type: row.option_type,
      observed_count: numeric(row.observed_count, 1, 0),
      quoted_count: numeric(row.quoted_count, 1, 0),
      spread_count: numeric(row.spread_count ?? row.count, 1, 0),
      avg_spread_pct: numeric(row.avg_spread, 100),
      median_spread_pct: numeric(row.median_spread, 100),
      min_spread_pct: numeric(row.min_spread, 100),
      max_spread_pct: numeric(row.max_spread, 100),
      avg_iv_pct: numeric(row.avg_iv, 100, 1),
      avg_depth: numeric(row.avg_depth),
    })),
  };
};

const formatMarketQualitySnapshotForAdvisor = (marketQualityRows = []) => {
  const snapshot = summarizeMarketQualitySnapshotForLLM(marketQualityRows);
  if (!snapshot) return 'Broad observed quote snapshot: unavailable.';
  const show = value => value == null ? 'n/a' : value;
  const lines = [
    `Broad observed quote snapshot: as of ${snapshot.as_of || 'unknown'}; ${snapshot.scope}.`,
    snapshot.measurement,
  ];
  for (const row of snapshot.sides) {
    const side = row.option_type === 'P' ? 'PUT' : row.option_type === 'C' ? 'CALL' : String(row.option_type || 'unknown');
    lines.push(`${side}: recorded ${show(row.observed_count)}, two-sided quoted ${show(row.quoted_count)}, spread samples ${show(row.spread_count)}; `
      + `spread/mark mean ${show(row.avg_spread_pct)}%, median ${show(row.median_spread_pct)}%, range ${show(row.min_spread_pct)}%–${show(row.max_spread_pct)}%; `
      + `mean IV ${show(row.avg_iv_pct)}%, mean visible bid+ask depth ${show(row.avg_depth)} contracts (spread-sample population).`);
  }
  return lines.join('\n');
};

const summarizeSentimentWindowsForLLM = (sentimentWindows) => {
  const windows = sentimentWindows && typeof sentimentWindows === 'object' ? sentimentWindows : {};
  return Object.fromEntries(
    Object.entries(windows).map(([label, data]) => [label, summarizeSentimentWindowForLLM(label, data)])
  );
};

const formatSignedPct = (value, digits = 1) => {
  if (!Number.isFinite(value)) return 'n/a';
  return `${value > 0 ? '+' : ''}${Number(value).toFixed(digits)}%`;
};

const summarizeSentimentForAdvisor = (sentimentWindows, marketQualityRows = []) => {
  const summary = summarizeSentimentWindowsForLLM(sentimentWindows);
  const windowOrder = ['6h', '24h', '7d', '30d'];
  const lines = [];

  for (const label of windowOrder) {
    const row = summary[label];
    if (!row) continue;
    lines.push(
      `${label}: funding ${row.funding_rate.current != null ? row.funding_rate.current : 'n/a'} vs avg ${row.funding_rate.avg != null ? row.funding_rate.avg : 'n/a'} (${row.funding_rate.trend}; Derive ETH-PERP hourly rate), ` +
      `skew ${row.options_skew.current_pct != null ? `${formatSignedPct(row.options_skew.current_pct, 2)} current` : 'n/a'} vs ${row.options_skew.avg_pct != null ? `${formatSignedPct(row.options_skew.avg_pct, 2)} avg` : 'n/a'} (${row.options_skew.direction}), ` +
      `OI ${row.aggregate_oi.current != null ? row.aggregate_oi.current : 'n/a'} (${row.aggregate_oi.change_pct != null ? formatSignedPct(row.aggregate_oi.change_pct, 1) : 'n/a'})`
    );
  }

  lines.push(formatMarketQualitySnapshotForAdvisor(marketQualityRows));
  return lines.join('\n');
};

const summarizeActiveRulesForAdvisor = (activeRules = []) => {
  if (!Array.isArray(activeRules) || activeRules.length === 0) return 'No active rules';
  return activeRules.slice(0, 8).map((rule, index) => {
    const criteria = typeof rule.criteria === 'string'
      ? (() => { try { return JSON.parse(rule.criteria); } catch { return rule.criteria; } })()
      : rule.criteria;
    const criteriaSummary = criteria && typeof criteria === 'object'
      ? Object.entries(criteria)
          .slice(0, 5)
          .map(([key, value]) => `${key}=${Array.isArray(value) ? `[${value.join(',')}]` : typeof value === 'object' ? '{...}' : value}`)
          .join(', ')
      : String(criteria || 'none');
    return `${index + 1}. ${rule.rule_type}/${rule.action} priority=${rule.priority} ${rule.instrument_name ? `instrument=${rule.instrument_name} ` : ''}| ${criteriaSummary}`;
  }).join('\n');
};

const summarizePendingActionsForAdvisor = (pendingActions = []) => {
  if (!Array.isArray(pendingActions) || pendingActions.length === 0) return 'No recent pending actions';
  const counts = pendingActions.reduce((acc, action) => {
    const status = action.status || 'unknown';
    acc[status] = (acc[status] || 0) + 1;
    return acc;
  }, {});
  const headline = `Status counts: ${Object.entries(counts).map(([status, count]) => `${status}=${count}`).join(', ')}`;
  const details = pendingActions.slice(0, 6).map((action, index) =>
    `${index + 1}. ${action.status} ${action.action} ${action.instrument_name || 'portfolio'} @ ${action.triggered_at}`
  );
  return [headline, ...details].join('\n');
};

const summarizeOpenOrdersForAdvisor = (openOrders = []) => {
  if (!Array.isArray(openOrders) || openOrders.length === 0) return 'No open orders';
  const byTif = openOrders.reduce((acc, order) => {
    const tif = order.time_in_force || 'unknown';
    acc[tif] = (acc[tif] || 0) + 1;
    return acc;
  }, {});
  const headline = `Open orders: ${openOrders.length} total | ${Object.entries(byTif).map(([tif, count]) => `${tif}=${count}`).join(', ')}`;
  const details = openOrders.slice(0, 6).map((order, index) =>
    `${index + 1}. ${order.instrument_name} ${order.direction} qty=${order.amount} limit=$${order.limit_price} filled=${order.filled_amount} age=${((Date.now() - order.creation_timestamp) / 3600000).toFixed(1)}h`
  );
  return [headline, ...details].join('\n');
};

const parseOptionInstrumentParts = (name) => {
  const parts = String(name || '').split('-');
  if (parts.length !== 4) return null;
  const strike = Number(parts[2]);
  return {
    expiryKey: parts[1],
    strike: Number.isFinite(strike) ? strike : null,
    optionType: parts[3],
  };
};

const formatMaybeMoney = (value, digits = 2) => (
  Number.isFinite(value) ? `$${Number(value).toFixed(digits)}` : 'n/a'
);

const roundMetric = (value, digits = 4) => (
  Number.isFinite(value) ? Number(Number(value).toFixed(digits)) : null
);

const getExecutableExitPrice = (position, ticker, values) => {
  const bid = Number(ticker?.b) || 0;
  const ask = Number(ticker?.a) || 0;
  if (position?.direction === 'short') return ask || values.mark_price || 0;
  return bid || values.mark_price || 0;
};

const buildPositionAdviceSnapshots = (positions = [], tickerMap = {}, spotPrice = null) => {
  if (!Array.isArray(positions)) return [];
  return positions.map((position) => {
    const instrument = position.instrument_name;
    const parsed = parseOptionInstrumentParts(instrument);
    const ticker = tickerMap?.[instrument] || null;
    const actionForExecutablePnl = position.direction === 'short' && parsed?.optionType === 'C'
      ? 'buyback_call'
      : null;
    const values = getRuleEvaluationValues(position, ticker, spotPrice, actionForExecutablePnl);
    const executableExitPrice = getExecutableExitPrice(position, ticker, values);
    const entryPrice = Number(position?.avg_entry_price) || 0;
    const executableExitPnlPct = entryPrice > 0 && executableExitPrice > 0
      ? (position.direction === 'short'
        ? ((entryPrice - executableExitPrice) / entryPrice) * 100
        : ((executableExitPrice - entryPrice) / entryPrice) * 100)
      : null;
    const strikeDistancePct = parsed?.strike && Number.isFinite(spotPrice) && spotPrice > 0
      ? ((parsed.optionType === 'C' ? parsed.strike - spotPrice : spotPrice - parsed.strike) / spotPrice) * 100
      : null;

    return {
      instrument,
      direction: position.direction,
      amount: roundMetric(Number(position.amount), 4),
      option_type: parsed?.optionType || null,
      strike: parsed?.strike ?? null,
      dte: roundMetric(values.dte, 2),
      strike_distance_pct: roundMetric(strikeDistancePct, 2),
      delta: roundMetric(values.delta, 4),
      theta: roundMetric(values.theta, 4),
      mark_price: roundMetric(values.mark_price, 4),
      bid_price: roundMetric(Number(ticker?.b) || null, 4),
      ask_price: roundMetric(Number(ticker?.a) || null, 4),
      avg_entry_price: roundMetric(entryPrice || null, 4),
      executable_exit_price: roundMetric(executableExitPrice || null, 4),
      executable_exit_pnl_pct: roundMetric(executableExitPnlPct, 2),
      mark_pnl_pct: roundMetric(values.unrealized_pnl_pct, 2),
      unrealized_pnl: roundMetric(Number(position.unrealized_pnl), 2),
    };
  });
};

const buildRulebookRequirements = ({
  putBudgetRemaining = 0,
  accountHealth = {},
  positionSnapshots = [],
} = {}) => {
  const requirements = [];
  const putBudget = Number(putBudgetRemaining);
  if (putBudget > 1) {
    requirements.push({
      action: 'buy_put',
      type: 'entry',
      applies: 'put budget remains',
      instruction: 'Create a patient standing buy_put watcher ranked directly by PUT EDGE with min_score/target_score price criteria. Legacy min_edge_score is ignored; spread, IV/skew, OI, and crash-payoff scenarios are descriptive, not another gate. Do not chase higher delta by itself or require an immediately marketable buy.',
    });
  }

  const margin = accountHealth?.margin || {};
  const callDiscipline = accountHealth?.callMarginDiscipline || {};
  const utilizationPct = Number(callDiscipline.utilizationPct ?? margin.margin_usage_pct);
  const limitPct = Number(callDiscipline.bufferedLimitPct) * 100;
  const hasMarginState = Boolean(accountHealth?.margin);
  const marginAvailable = hasMarginState
    && !margin.is_under_liquidation
    && Number.isFinite(utilizationPct)
    && Number.isFinite(limitPct)
    && utilizationPct < limitPct;
  if (marginAvailable) {
    requirements.push({
      action: 'sell_call',
      type: 'entry',
      applies: 'call margin headroom remains',
      instruction: `Create a standing sell_call watcher only for favorable call premium. Encode value with min_score plus min_bid, DTE, delta, and margin criteria. CALL EDGE is raw bid / abs(delta), lightly normalized by (${SELL_CALL_EDGE_REFERENCE_DTE} / DTE)^${SELL_CALL_EDGE_DTE_EXPONENT} to reduce weekly expiry-roll artifacts.`,
    });
  }

  for (const snapshot of positionSnapshots || []) {
    if (snapshot?.direction === 'long' && snapshot?.option_type === 'P') {
      requirements.push({
        action: 'sell_put',
        type: 'exit',
        instrument_name: snapshot.instrument,
        applies: 'open long put',
        instruction: `Create a reduce-only sell_put watcher with put_exit_intent="roll_protection" only if DTE <= ${PUT_ROLL_DTE_THRESHOLD} and longer-dated put protection is already in the book; a roll may close the aging instrument fully. Use put_exit_intent="monetize_tail_win" only after executable PnL reaches >${PUT_MONETIZATION_PROFIT_THRESHOLD}%. Monetization must include min_exit_price/limit_price, be tranched, and retain downside protection after each sale.`,
      });
    }
    if (snapshot?.direction === 'short' && snapshot?.option_type === 'C') {
      requirements.push({
        action: 'buyback_call',
        type: 'exit',
        instrument_name: snapshot.instrument,
        applies: 'open short call',
        instruction: 'Create a reduce-only buyback_call watcher with buyback_intent="profit_capture" for 80%+ capture or patient target-capture bids, or buyback_intent="threat_management" only for genuine short-call danger. Price rising alone is not enough.',
      });
    }
  }

  return requirements;
};

const formatRulebookRequirements = (requirements = []) => {
  if (!Array.isArray(requirements) || requirements.length === 0) {
    return 'No mandatory standing watchers: no put budget, no call margin headroom, and no open option positions requiring exit watchers.';
  }
  return requirements.map((req, index) => {
    const instrument = req.instrument_name ? ` ${req.instrument_name}` : '';
    return `${index + 1}. ${req.type}/${req.action}${instrument} — applies because ${req.applies}. ${req.instruction}`;
  }).join('\n');
};

const findMissingRulebookRequirements = (agenda = {}, requirements = []) => {
  const entryRules = Array.isArray(agenda?.entry_rules) ? agenda.entry_rules : [];
  const exitRules = Array.isArray(agenda?.exit_rules) ? agenda.exit_rules : [];
  return (requirements || []).filter((req) => {
    if (req.type === 'entry') {
      return !entryRules.some((rule) => rule?.action === req.action);
    }
    return !exitRules.some((rule) =>
      rule?.action === req.action && rule?.instrument_name === req.instrument_name
    );
  });
};

const buildAgendaFromValidatedRules = (rules = []) => ({
  entry_rules: (rules || []).filter((rule) => rule?.rule_type === 'entry'),
  exit_rules: (rules || []).filter((rule) => rule?.rule_type === 'exit'),
});

const buildCanonicalRequiredWatcherRule = (requirement, context = {}) => {
  if (requirement?.type === 'entry' && requirement?.action === 'sell_call') {
    return {
      rule_type: 'entry',
      action: 'sell_call',
      instrument_name: null,
      criteria: {
        option_type: 'C',
        delta_range: CALL_DELTA_RANGE,
        dte_range: CALL_EXPIRATION_RANGE,
        min_bid: SELL_CALL_FALLBACK_MIN_BID,
        min_score: SELL_CALL_FALLBACK_MIN_SCORE,
      },
      budget_limit: null,
      priority: 'low',
      reasoning: `Required sell-call coverage fallback: patient watcher for favorable short-dated call premium only; requires ${CALL_EXPIRATION_RANGE[0]}-${CALL_EXPIRATION_RANGE[1]} DTE, delta ${CALL_DELTA_RANGE[0]}-${CALL_DELTA_RANGE[1]}, bid >= $${SELL_CALL_FALLBACK_MIN_BID.toFixed(2)}, and DTE-normalized CALL EDGE >= ${SELL_CALL_FALLBACK_MIN_SCORE}.`,
      advisory_id: context.advisoryId || null,
      preferred_order_type: 'post_only',
    };
  }

  if (requirement?.type === 'exit' && requirement?.action === 'sell_put' && requirement.instrument_name) {
    const snapshot = (context.positionSnapshots || []).find((item) => item?.instrument === requirement.instrument_name);
    const canRoll = snapshot
      && Number(snapshot.dte) <= PUT_ROLL_DTE_THRESHOLD
      && hasLongerDatedPutProtectionSnapshot(snapshot, context.positionSnapshots || []);
    if (canRoll) {
      return {
        rule_type: 'exit',
        action: 'sell_put',
        instrument_name: requirement.instrument_name,
        criteria: {
          put_exit_intent: 'roll_protection',
          conditions: [
            { field: 'dte', op: 'lte', value: PUT_ROLL_DTE_THRESHOLD },
          ],
          condition_logic: 'all',
          requires_longer_dated_protection: true,
        },
        budget_limit: null,
        priority: 'low',
        reasoning: `Required long-put coverage fallback: reduce-only roll watcher because ${requirement.instrument_name} is inside the ${PUT_ROLL_DTE_THRESHOLD} DTE roll window and longer-dated put protection is already in the book.`,
        advisory_id: context.advisoryId || null,
        preferred_order_type: 'ioc',
      };
    }

    const entryPrice = Number(snapshot?.avg_entry_price);
    const minExitPrice = entryPrice > 0
      ? Number((entryPrice * (1 + PUT_MONETIZATION_PROFIT_THRESHOLD / 100) + 0.01).toFixed(2))
      : null;
    if (!(minExitPrice > 0)) return null;

    return {
      rule_type: 'exit',
      action: 'sell_put',
      instrument_name: requirement.instrument_name,
      criteria: {
        put_exit_intent: 'monetize_tail_win',
        conditions: [
          { field: 'unrealized_pnl_pct', op: 'gt', value: PUT_MONETIZATION_PROFIT_THRESHOLD },
        ],
        condition_logic: 'all',
        min_exit_price: minExitPrice,
        tranche_fraction: PUT_MONETIZATION_MAX_TRANCHE_FRACTION,
        retain_downside_protection: true,
      },
      budget_limit: null,
      priority: 'low',
      reasoning: `Required long-put coverage fallback: dormant tail-win monetization watcher; only sell up to ${(PUT_MONETIZATION_MAX_TRANCHE_FRACTION * 100).toFixed(0)}% at executable PnL > ${PUT_MONETIZATION_PROFIT_THRESHOLD}% with min exit price $${minExitPrice.toFixed(2)}, preserving remaining downside protection.`,
      advisory_id: context.advisoryId || null,
      preferred_order_type: 'post_only',
    };
  }

  if (requirement?.type !== 'exit' || requirement?.action !== 'buyback_call' || !requirement.instrument_name) {
    return null;
  }

  const snapshot = (context.positionSnapshots || []).find((item) => item?.instrument === requirement.instrument_name);
  const entryPrice = Number(snapshot?.avg_entry_price);
  const maxBuybackPrice = entryPrice > 0
    ? floorOptionPriceCents(entryPrice * (1 - CALL_BUYBACK_PROFIT_THRESHOLD / 100))
    : null;
  const criteria = {
    buyback_intent: 'profit_capture',
    conditions: [
      { field: 'unrealized_pnl_pct', op: 'gte', value: CALL_BUYBACK_PROFIT_THRESHOLD },
    ],
    condition_logic: 'all',
    target_capture_pct: CALL_BUYBACK_PROFIT_THRESHOLD,
  };
  if (maxBuybackPrice != null) {
    criteria.max_buyback_price = maxBuybackPrice;
  }

  return {
    rule_type: 'exit',
    action: 'buyback_call',
    instrument_name: requirement.instrument_name,
    criteria,
    budget_limit: null,
    priority: 'low',
    reasoning: maxBuybackPrice != null
      ? `Required short-call coverage fallback: patient synthetic reduce-only buyback watcher at ${CALL_BUYBACK_PROFIT_THRESHOLD}%+ capture; max bid $${maxBuybackPrice.toFixed(2)} from entry $${entryPrice.toFixed(2)}.`
      : `Required short-call coverage fallback: reduce-only buyback watcher only when executable capture reaches ${CALL_BUYBACK_PROFIT_THRESHOLD}%+.`,
    advisory_id: context.advisoryId || null,
    preferred_order_type: maxBuybackPrice != null ? 'post_only' : 'ioc',
  };
};

const parseCriteriaForSummary = (criteria) => {
  if (!criteria) return null;
  if (typeof criteria === 'object') return criteria;
  try { return JSON.parse(criteria); } catch { return null; }
};

const summarizeExitRuleTrigger = (rule) => {
  const criteria = parseCriteriaForSummary(rule?.criteria);
  const conditions = Array.isArray(criteria?.conditions) ? criteria.conditions : [];
  if (conditions.length === 0) return 'no structured trigger';
  const logic = criteria.condition_logic || 'all';
  const body = conditions.map((condition) =>
    `${condition.field} ${condition.op} ${condition.value}`
  ).join(logic === 'any' ? ' OR ' : ' AND ');
  return body || 'no structured trigger';
};

const buildPositionRationale = (snapshot) => {
  const parts = [];
  if (Number.isFinite(snapshot.dte)) parts.push(`${snapshot.dte.toFixed(1)} DTE`);
  if (Number.isFinite(snapshot.strike_distance_pct)) {
    const distanceLabel = snapshot.strike_distance_pct >= 0 ? 'OTM' : 'ITM';
    parts.push(`${Math.abs(snapshot.strike_distance_pct).toFixed(1)}% ${distanceLabel}`);
  }
  if (Number.isFinite(snapshot.executable_exit_pnl_pct)) {
    const exitLabel = snapshot.direction === 'short' ? 'executable capture' : 'executable exit PnL';
    parts.push(`${exitLabel} ${snapshot.executable_exit_pnl_pct.toFixed(1)}%`);
  } else if (Number.isFinite(snapshot.mark_pnl_pct)) {
    parts.push(`mark PnL ${snapshot.mark_pnl_pct.toFixed(1)}%`);
  }
  if (Number.isFinite(snapshot.bid_price) || Number.isFinite(snapshot.ask_price)) {
    parts.push(`bid/ask ${formatMaybeMoney(snapshot.bid_price, 2)}/${formatMaybeMoney(snapshot.ask_price, 2)}`);
  }
  return parts.join(', ') || 'live metrics unavailable';
};

const buildPositionPlanLines = ({
  positionSnapshots = [],
  exitRules = [],
}) => {
  if (!Array.isArray(positionSnapshots) || positionSnapshots.length === 0) return [];
  const activeExitRules = Array.isArray(exitRules) ? exitRules : [];

  return positionSnapshots.map((snapshot) => {
    const matchingRules = activeExitRules.filter((rule) => rule?.instrument_name === snapshot.instrument);
    const rationale = buildPositionRationale(snapshot);
    if (matchingRules.length > 0) {
      const ruleText = matchingRules.map((rule) => {
        const orderType = rule.preferred_order_type ? `, ${rule.preferred_order_type}` : '';
        return `${rule.action} ${rule.priority || 'medium'}${orderType}; trigger: ${summarizeExitRuleTrigger(rule)}`;
      }).join(' | ');
      return `${snapshot.instrument}: follow active exit plan (${ruleText}). Why: ${rationale}.`;
    }

    if (snapshot.direction === 'short' && snapshot.option_type === 'C') {
      return `${snapshot.instrument}: hold / no buyback order. No active buyback_call rule is live, so the bot will not post a bid unless a later advisory adds one. Why: ${rationale}.`;
    }

    if (snapshot.direction === 'long' && snapshot.option_type === 'P') {
      return `${snapshot.instrument}: hold protection. No active sell_put rule is live, so the bot is keeping the hedge unless a later advisory adds a roll or monetization trigger. Why: ${rationale}.`;
    }

    return `${snapshot.instrument}: hold / monitor. No active exit rule is live. Why: ${rationale}.`;
  });
};

const ensureAssessmentHasPositionPlan = ({
  assessment,
  positionSnapshots,
  exitRules,
}) => {
  const base = String(assessment || '').trim() || 'No assessment produced.';
  const snapshots = Array.isArray(positionSnapshots) ? positionSnapshots : [];
  if (snapshots.length === 0) return base;

  const missingSnapshots = snapshots.filter((snapshot) => !base.includes(snapshot.instrument));
  if (missingSnapshots.length === 0 && /thesis breakdown|position plan/i.test(base)) return base;

  const lines = buildPositionPlanLines({
    positionSnapshots: missingSnapshots.length > 0 ? missingSnapshots : snapshots,
    exitRules,
  });
  if (lines.length === 0) return base;
  return `${base}\n\nThesis breakdown:\n${lines.map((line) => `- ${line}`).join('\n')}`;
};

const isPlaceholderAssessmentText = (value) => {
  const normalized = String(value || '').trim().toLowerCase();
  return !normalized || normalized === 'no assessment produced' || normalized === 'synthesized assessment';
};

const selectAssessmentText = (...candidates) => {
  for (const candidate of candidates) {
    if (!isPlaceholderAssessmentText(candidate)) return String(candidate).trim();
  }
  for (const candidate of candidates) {
    const text = String(candidate || '').trim();
    if (text) return text;
  }
  return 'No assessment produced';
};

const buildMandelbrotContextBlock = (mandelbrotContext) => {
  if (!mandelbrotContext) return 'No Mandelbrot regime context available.';
  return JSON.stringify(mandelbrotContext, null, 2);
};

const normalizeTalebSecondOpinion = (payload) => {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return null;

  const critique = [
    payload.critique,
    payload.assessment,
    payload.summary,
    payload.reasoning,
    payload.overall_assessment,
  ].find((value) => typeof value === 'string' && value.trim().length > 0) || null;

  const vetoes = Array.isArray(payload.vetoes) ? payload.vetoes.filter((item) => item && typeof item === 'object' && !Array.isArray(item)) : [];
  const amendments = Array.isArray(payload.amendments) ? payload.amendments.filter((item) => item && typeof item === 'object' && !Array.isArray(item)) : [];
  const additions = Array.isArray(payload.additions) ? payload.additions.filter((item) => item && typeof item === 'object' && !Array.isArray(item)) : [];

  if (!critique && vetoes.length === 0 && amendments.length === 0 && additions.length === 0) {
    return null;
  }

  return { critique, vetoes, amendments, additions };
};

const parseTalebSecondOpinion = (text) => {
  if (!text || typeof text !== 'string') return null;
  const normalized = normalizeTalebSecondOpinion(extractJSON(text));
  if (normalized) return normalized;

  const trimmed = text.trim();
  if (!trimmed) return null;

  return {
    critique: trimmed,
    vetoes: [],
    amendments: [],
    additions: [],
    _parse_fallback: true,
  };
};

const ASSESSMENT_UNSUPPORTED_PATTERNS = [
  /\b(?:put|call|deployment)\s+efficiency\b/i,
  /\bantifragility\s+score\b/i,
];

const assessmentUsesUnsupportedMetricLanguage = (text) => {
  const normalized = String(text || '').trim();
  if (!normalized) return null;
  for (const pattern of ASSESSMENT_UNSUPPORTED_PATTERNS) {
    if (pattern.test(normalized)) return pattern;
  }
  return null;
};

const normalizeSpotPathRow = (row) => {
  if (!row || typeof row !== 'object') return null;
  const timestamp = row.hour || row.timestamp || row.time;
  const ts = Date.parse(timestamp);
  const price = Number(row.avg_price ?? row.close ?? row.price);
  if (!Number.isFinite(ts) || !(price > 0)) return null;
  return { ts, price };
};

const buildHourlySpotPathSamples = (rows = []) => {
  const intervalMs = MANDELBROT_SPOT_PATH_INTERVAL_HOURS * 60 * 60 * 1000;
  const buckets = new Map();
  const normalized = rows.map(normalizeSpotPathRow).filter(Boolean).sort((a, b) => a.ts - b.ts);
  for (const point of normalized) {
    const bucketTs = Math.floor(point.ts / intervalMs) * intervalMs;
    const existing = buckets.get(bucketTs) || { ts: bucketTs, closeTs: -Infinity, price: point.price };
    if (point.ts >= existing.closeTs) {
      existing.closeTs = point.ts;
      existing.price = point.price;
    }
    buckets.set(bucketTs, existing);
  }
  return Array.from(buckets.values())
    .sort((a, b) => a.ts - b.ts)
    .slice(-MANDELBROT_SPOT_PATH_MAX_POINTS)
    .map((point) => ({
      ts: point.ts,
      hour: new Date(point.ts).toISOString().slice(0, 13),
      price: roundForAdvisory(point.price, 2),
    }))
    .filter((point) => Number.isFinite(point.price));
};

const quantile = (values, q) => {
  const sorted = values.filter(Number.isFinite).sort((a, b) => a - b);
  if (sorted.length === 0) return null;
  const index = (sorted.length - 1) * q;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sorted[lower];
  return sorted[lower] + ((sorted[upper] - sorted[lower]) * (index - lower));
};

const average = (values) => {
  const finite = values.filter(Number.isFinite);
  if (finite.length === 0) return null;
  return finite.reduce((sum, value) => sum + value, 0) / finite.length;
};

const getSpotPathPriceNear = (samples, targetTs) => {
  const toleranceMs = MANDELBROT_SPOT_PATH_INTERVAL_HOURS * 2.5 * 60 * 60 * 1000;
  for (let i = samples.length - 1; i >= 0; i--) {
    if (samples[i].ts <= targetTs) {
      return targetTs - samples[i].ts <= toleranceMs ? samples[i].price : null;
    }
  }
  for (const sample of samples) {
    if (sample.ts >= targetTs) {
      return sample.ts - targetTs <= toleranceMs ? sample.price : null;
    }
  }
  return null;
};

const buildMandelbrotSpotPathContext = ({
  spotPrice,
  spotRows = [],
  nowMs = Date.now(),
  source = 'spot_prices_hourly',
}) => {
  const currentSpot = Number(spotPrice);
  const samples = buildHourlySpotPathSamples(spotRows);
  const windows = {
    '1h': 1,
    '6h': 6,
    '24h': 24,
    '7d': 24 * 7,
    '14d': 24 * 14,
    '30d': 24 * 30,
  };
  const returnsPct = {};
  for (const [label, hours] of Object.entries(windows)) {
    const prior = getSpotPathPriceNear(samples, nowMs - hours * 60 * 60 * 1000);
    returnsPct[label] = currentSpot > 0 && prior > 0
      ? roundForAdvisory(((currentSpot - prior) / prior) * 100, 3)
      : null;
  }

  const hourlyMoves = [];
  for (let i = 1; i < samples.length; i++) {
    const previous = samples[i - 1];
    const current = samples[i];
    if (!(previous.price > 0) || !(current.price > 0)) continue;
    hourlyMoves.push({
      hour: current.hour,
      pct: ((current.price - previous.price) / previous.price) * 100,
    });
  }
  const absMoves = hourlyMoves.map((move) => Math.abs(move.pct));
  const totalAbsMove = absMoves.reduce((sum, value) => sum + value, 0);
  const sortedAbsMoves = [...absMoves].sort((a, b) => b - a);
  const topShare = (count) => totalAbsMove > 0
    ? roundForAdvisory((sortedAbsMoves.slice(0, count).reduce((sum, value) => sum + value, 0) / totalAbsMove) * 100, 2)
    : null;
  const largeMoveThreshold = quantile(absMoves, 0.75);
  let adjacentLargeMoveCount = 0;
  if (Number.isFinite(largeMoveThreshold)) {
    for (let i = 1; i < absMoves.length; i++) {
      if (absMoves[i] >= largeMoveThreshold && absMoves[i - 1] >= largeMoveThreshold) {
        adjacentLargeMoveCount += 1;
      }
    }
  }

  return {
    source,
    lookback_days: MANDELBROT_SPOT_PATH_LOOKBACK_DAYS,
    sample_interval_hours: MANDELBROT_SPOT_PATH_INTERVAL_HOURS,
    sample_format: '[hours_from_now, spot_price]',
    current_spot: roundForAdvisory(currentSpot, 2),
    point_count: samples.length,
    first_sample_hour: samples[0]?.hour || null,
    last_sample_hour: samples[samples.length - 1]?.hour || null,
    returns_pct: returnsPct,
    hourly_move_stats_pct: {
      average_abs: roundForAdvisory(average(absMoves), 3),
      median_abs: roundForAdvisory(quantile(absMoves, 0.5), 3),
      p90_abs: roundForAdvisory(quantile(absMoves, 0.9), 3),
      max_up: roundForAdvisory(Math.max(0, ...hourlyMoves.map((move) => move.pct)), 3),
      max_down: roundForAdvisory(Math.min(0, ...hourlyMoves.map((move) => move.pct)), 3),
    },
    jump_counts: {
      abs_gt_0_5pct: absMoves.filter((value) => value > 0.5).length,
      abs_gt_1pct: absMoves.filter((value) => value > 1).length,
      abs_gt_2pct: absMoves.filter((value) => value > 2).length,
      abs_gt_3pct: absMoves.filter((value) => value > 3).length,
    },
    concentration_pct: {
      top_5_hourly_abs_moves_share: topShare(5),
      top_10_hourly_abs_moves_share: topShare(10),
    },
    vol_clustering_proxy: {
      large_move_threshold_p75_abs_pct: roundForAdvisory(largeMoveThreshold, 3),
      adjacent_large_move_pairs: adjacentLargeMoveCount,
    },
    largest_hourly_moves: {
      up: hourlyMoves
        .filter((move) => move.pct > 0)
        .sort((a, b) => b.pct - a.pct)
        .slice(0, 5)
        .map((move) => [move.hour, roundForAdvisory(move.pct, 3)]),
      down: hourlyMoves
        .filter((move) => move.pct < 0)
        .sort((a, b) => a.pct - b.pct)
        .slice(0, 5)
        .map((move) => [move.hour, roundForAdvisory(move.pct, 3)]),
    },
    samples_oldest_to_newest: samples.map((point) => [
      Math.round((point.ts - nowMs) / (60 * 60 * 1000)),
      point.price,
    ]),
  };
};

const generateMandelbrotRegimeContext = async ({
  spotPrice,
  spotPathContext,
  sentiment,
  wikiSignals,
}) => {
  if (!process.env.OPENAI_API_KEY) return null;

  const systemPrompt = `You are a Mandelbrot-style market structure analyst for an ETH options tail-hedging system.

You do NOT propose trades directly. You do NOT predict price direction. You do NOT recommend any particular action set.
Your job is to synthesize current market structure in a Mandelbrotian way and classify whether current behavior looks calm, transitional, clustered-stress, or cascade-risk.
You are writing for a downstream Spitznagel-style strategist who will make the actual trade recommendations.
Use Mandelbrot's finance framing from *The (Mis)Behavior of Markets* and *Fractals and Scaling in Finance*: markets are often discontinuous, concentrated, fat-tailed, and governed by scaling relationships that make Gaussian intuitions unreliable.

Focus on:
- whether skew, funding, open interest, spread/depth, and option pricing suggest unstable distribution geometry
- whether volatility and option-market participation are clustering across the supplied windows
- roughness versus smoothness of the supplied 30-day hourly spot path
- whether movement is concentrated into bursts rather than dispersed smoothly
- whether the process appears mild or wild, smooth or discontinuous

Spot-path discipline:
- Treat the hourly spot path as structural evidence for roughness, burstiness, discontinuity, concentration, and scaling instability.
- Do not reduce the path to a simple upward/downward momentum label.
- Do not frame the regime primarily as directional unless skew, OI, funding, liquidity, or option pricing confirms that directional label matters.

Operational definitions:
- Roughness: the path is jagged, bursty, reversal-heavy, and unevenly distributed through time rather than unfolding gradually.
- Smoothness: the path is relatively continuous, incremental, and evenly dispersed, with less jump concentration and less cross-window instability.
- Mild randomness: disturbances look more diffusion-like and locally bounded; large moves do not dominate the sample.
- Wild randomness: extremes matter disproportionately; jumps and tail episodes dominate realized structure more than Gaussian intuition would suggest.
- Volatility clustering: large moves tend to arrive near other large moves, rather than independently and evenly through time.
- Scaling instability: behavior changes materially across 6h, 24h, 7d, and 30d windows instead of preserving a stable risk geometry.
- Discontinuity: gaps, jumps, and abrupt path changes matter more than smooth drift.
- Concentration: a small number of episodes account for a large share of realized movement, stress, or repricing.

Be skeptical of Gaussian assumptions and of square-root-of-time intuitions. Prefer describing geometry, clustering, concentration, discontinuity, scaling instability, and persistence over forecasting direction.
Use only the supplied market data. Do not invent measurements. Do not suggest trades, thresholds, or portfolio actions.

Return JSON only:
{
  "regime": "calm" | "transitional" | "clustered_stress" | "cascade_risk",
  "confidence": 0.0,
  "roughness_score": 0.0,
  "wildness_score": 0.0,
  "vol_clustering_score": 0.0,
  "scaling_instability_score": 0.0,
  "geometry_notes": ["..."],
  "key_considerations_for_spitznagel": ["..."],
  "market_rationale": "2-4 sentence synthesis of what a Spitznagel-style strategist should pay attention to in current market structure",
  "invalidations": ["..."]
}`;

  const userPrompt = `Assess the market structure from a Mandelbrot lens.

=== SENTIMENT / DISTRIBUTION BY WINDOW ===
${JSON.stringify(summarizeSentimentWindowsForLLM(sentiment?.windows || {}), null, 2)}

=== BROAD QUOTE SNAPSHOT (all recorded tenors, not the entry candidate pool) ===
${JSON.stringify(summarizeMarketQualitySnapshotForLLM(sentiment?.latest?.marketQuality || []), null, 2)}

=== SPOT PATH CONTEXT (30D, HOURLY) ===
${JSON.stringify(spotPathContext || buildMandelbrotSpotPathContext({ spotPrice, spotRows: [] }), null, 2)}

=== WIKI SIGNALS ===
${JSON.stringify(wikiSignals || null, null, 2)}

Return JSON only. Synthesize market characteristics for a downstream strategist; do not include trading recommendations.`;

  try {
    const text = await callOpenAI(systemPrompt, userPrompt, { maxTokens: 8192, timeout: 120000, model: OPENAI_STRATEGY_MODEL });
    if (!text) return null;
    const parsed = extractJSON(text);
    if (!parsed || typeof parsed !== 'object') return null;
    if (!['calm', 'transitional', 'clustered_stress', 'cascade_risk'].includes(parsed.regime)) return null;
    return parsed;
  } catch (e) {
    console.log(`📋 Mandelbrot regime context failed (non-fatal): ${e.message}`);
    return null;
  }
};

// ─── LLM-Driven Trading: Monitoring ──────────────────────────────────────────

const parseExpiryFromInstrument = (name) => {
  if (typeof name !== 'string') return null;
  // "ETH-20260501-1500-P" → Date(2026-05-01T08:00:00Z)
  const parts = name.split('-');
  if (parts.length < 4) return null;
  const d = parts[1]; // "20260501"
  return new Date(`${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6,8)}T08:00:00Z`);
};

const computeDteFromInstrumentName = (instrumentName, nowMs = Date.now()) => {
  const expiry = parseExpiryFromInstrument(instrumentName);
  if (!expiry) return null;
  return Math.max(0, (expiry.getTime() - nowMs) / 86400000);
};

const isSellCallCandidateInStrategyRange = (dte, delta) => (
  Number.isFinite(dte)
  && dte >= CALL_EXPIRATION_RANGE[0]
  && dte <= CALL_EXPIRATION_RANGE[1]
  && Number.isFinite(delta)
  && delta >= CALL_DELTA_RANGE[0]
  && delta <= CALL_DELTA_RANGE[1]
);

const buildSellCallEdge = ({ score, dte } = {}) => {
  const rawScore = Number(score);
  const numericDte = Number(dte);
  const edgeScore = normalizeSellCallScore(rawScore, numericDte);
  const normalizationFactor = rawScore > 0 ? edgeScore / rawScore : 0;
  const passesFloor = edgeScore >= SELL_CALL_FALLBACK_MIN_SCORE;

  return {
    recommendation: passesFloor ? 'acceptable' : 'caution',
    preferred: passesFloor,
    strong: false,
    dte_bucket: Number.isFinite(numericDte) ? 'strategy_5_12_dte' : 'unknown',
    score_band: passesFloor ? 'above_normalized_floor' : 'below_normalized_floor',
    edge_components: {
      raw_score: roundForAdvisory(rawScore, 2),
      dte: roundForAdvisory(numericDte, 4),
      reference_dte: SELL_CALL_EDGE_REFERENCE_DTE,
      dte_exponent: SELL_CALL_EDGE_DTE_EXPONENT,
      normalization_factor: roundForAdvisory(normalizationFactor, 6),
    },
    selection_multiplier: roundForAdvisory(normalizationFactor, 6),
    selection_score: edgeScore,
    reason: `CALL EDGE = raw score × (${SELL_CALL_EDGE_REFERENCE_DTE} / DTE)^${SELL_CALL_EDGE_DTE_EXPONENT}`,
  };
};
const telemetryNumber = (value) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const logRuleDecisionSafe = (decision) => {
  if (!db || typeof db.insertRuleDecision !== 'function') return null;
  try {
    return db.insertRuleDecision(decision);
  } catch (e) {
    console.log(`DB: rule decision write failed: ${e.message}`);
    return null;
  }
};

const recordCandidateObservationsSafe = (observations) => {
  if (!db || typeof db.insertCandidateObservations !== 'function' || !Array.isArray(observations) || observations.length === 0) {
    return null;
  }
  try {
    return db.insertCandidateObservations(observations);
  } catch (e) {
    console.log(`DB: candidate observation write failed: ${e.message}`);
    return null;
  }
};

const recordSellCallEdgeSnapshotSafe = (snapshot) => {
  if (!db || typeof db.insertSellCallEdgeSnapshot !== 'function' || !snapshot) return null;
  try {
    return db.insertSellCallEdgeSnapshot(snapshot);
  } catch (e) {
    console.log(`DB: sell-call edge snapshot write failed: ${e.message}`);
    return null;
  }
};

const recordBuyPutEdgeSnapshotSafe = (snapshot) => {
  if (!db || typeof db.insertBuyPutEdgeSnapshot !== 'function' || !snapshot) return null;
  try {
    return db.insertBuyPutEdgeSnapshot(snapshot);
  } catch (e) {
    console.log(`DB: buy-put edge snapshot write failed: ${e.message}`);
    return null;
  }
};

const getCandidateOptionType = (candidate, criteria = {}) => (
  criteria.option_type
  || candidate?.instrument?.option_details?.option_type
  || (String(candidate?.name || '').endsWith('-P') ? 'P' : String(candidate?.name || '').endsWith('-C') ? 'C' : null)
);

const getCandidateExpiry = (candidate) => {
  const directExpiry = telemetryNumber(candidate?.instrument?.option_details?.expiry);
  if (directExpiry != null) return directExpiry;
  const parsedExpiry = parseExpiryFromInstrument(candidate?.name);
  const parsedSeconds = parsedExpiry ? parsedExpiry.getTime() / 1000 : null;
  return Number.isFinite(parsedSeconds) ? parsedSeconds : null;
};

const buildCandidateObservationRows = ({
  candidates = [],
  rule,
  criteria = {},
  selectedName = null,
  decisionStatus = 'observed',
  selectionReason = null,
  pendingActionId = null,
  spotPrice = null,
  tickTimestamp = null,
  context = null,
} = {}) => {
  if (!Array.isArray(candidates) || candidates.length === 0 || !rule?.action) return [];
  const observedAt = tickTimestamp || new Date().toISOString();
  const dteRange = Array.isArray(criteria.dte_range) ? criteria.dte_range : [];
  const deltaRange = Array.isArray(criteria.delta_range) ? criteria.delta_range : [];

  return candidates.slice(0, CANDIDATE_OBSERVATION_TOP_N).map((candidate, index) => {
    const ticker = candidate?.ticker || {};
    const bidPrice = telemetryNumber(candidate?.bidPrice ?? ticker.b);
    const askPrice = telemetryNumber(candidate?.askPrice ?? ticker.a);
    const markPrice = telemetryNumber(candidate?.markPrice ?? ticker.M);
    const bidAmount = telemetryNumber(candidate?.bidAmount ?? ticker.B);
    const askAmount = telemetryNumber(candidate?.askAmount ?? ticker.A);
    const research = candidate?.sellCallResearch || candidate?.buyPutResearch || null;
    const isSelected = Boolean(selectedName && candidate?.name === selectedName);
    const metadata = {
      ...(context || {}),
      policy_version: STRATEGY_FACTS.policy_version,
      strategy_config: BOT_CONFIG,
      rule_snapshot: { id: rule.id, action: rule.action, criteria },
      quote_received_at: ticker.quote_received_at || null,
      quote_source: ticker.quote_source || null,
      quote_age_ms: Number.isFinite(Date.parse(ticker.quote_received_at))
        && Date.parse(observedAt) >= Date.parse(ticker.quote_received_at)
        ? Date.parse(observedAt) - Date.parse(ticker.quote_received_at) : null,
      price_source: candidate?.priceSource || null,
      score_threshold_price: candidate?.scoreThresholdPrice ?? null,
      candidate_limit_price: candidate?.candidateLimitPrice ?? null,
      raw_score_price_basis: rule.action === 'buy_put' ? 'planned_limit' : 'live_bid',
      planned_score: rule.action === 'buy_put' ? candidate?.score ?? null : null,
      live_score: candidate?.liveScore ?? null,
      live_raw_score: candidate?.liveRawScore ?? null,
      research_reason: research?.reason || null,
      selection_metric: rule.action === 'buy_put' ? 'put_edge_v1' : 'call_edge_v1',
      selection_price_basis: rule.action === 'buy_put' ? 'live_ask' : 'live_bid',
      legacy_min_edge_score_ignored: criteria.min_edge_score ?? null,
      edge_components: research?.edge_components || null,
      buy_put_edge_score: candidate?.buyPutResearch?.selection_score ?? null,
      buy_put_recommendation: candidate?.buyPutResearch?.recommendation || null,
      sell_call_edge_score: candidate?.sellCallResearch?.selection_score ?? null,
      sell_call_recommendation: candidate?.sellCallResearch?.recommendation || null,
    };

    return {
      observed_at: observedAt,
      tick_id: observedAt,
      rule_id: rule.id,
      pending_action_id: isSelected ? pendingActionId : null,
      action: rule.action,
      instrument_name: candidate.name,
      option_type: getCandidateOptionType(candidate, criteria),
      candidate_rank: index + 1,
      selected: isSelected,
      decision_status: isSelected ? decisionStatus : 'not_selected',
      selection_reason: isSelected
        ? selectionReason
        : (selectedName ? `ranked below selected ${selectedName}` : selectionReason),
      spot_price: spotPrice,
      strike: candidate?.strike ?? candidate?.instrument?.option_details?.strike ?? null,
      expiry: getCandidateExpiry(candidate),
      dte: candidate?.dte ?? null,
      delta: candidate?.delta ?? ticker?.option_pricing?.d ?? null,
      bid_price: bidPrice,
      ask_price: askPrice,
      mark_price: markPrice,
      bid_amount: bidAmount,
      ask_amount: askAmount,
      spread_pct: bidPrice != null && askPrice != null && markPrice > 0 ? (askPrice - bidPrice) / markPrice : null,
      depth: (bidAmount || 0) + (askAmount || 0),
      implied_vol: ticker?.option_pricing?.i ?? ticker?.details?.impliedVol ?? null,
      open_interest: ticker?.stats?.oi ?? ticker?.open_interest ?? null,
      raw_score: candidate?.rawScore ?? candidate?.score ?? null,
      selection_score: candidate?.selectionScore ?? candidate?.score ?? null,
      score_band: research?.score_band || null,
      dte_bucket: research?.dte_bucket || null,
      research_recommendation: research?.recommendation || null,
      score_trend_24h_pct: research?.score_trend_24h_pct ?? null,
      spot_ret_6h: context?.spot_ret_6h ?? null,
      spot_ret_24h: context?.spot_ret_24h ?? null,
      rule_min_score: criteria.min_score ?? null,
      rule_min_bid: criteria.min_bid ?? null,
      rule_dte_min: dteRange[0] ?? null,
      rule_dte_max: dteRange[1] ?? null,
      rule_delta_min: deltaRange[0] ?? null,
      rule_delta_max: deltaRange[1] ?? null,
      metadata,
    };
  });
};

const computeCurrentValues = (position, ticker, spotPrice) => {
  const dte = computeDteFromInstrumentName(position.instrument_name);
  const markPrice = Number(ticker?.M) || position.mark_price || 0;
  const entryPrice = position.avg_entry_price || 0;
  const unrealizedPnlPct = entryPrice > 0 ? ((markPrice - entryPrice) / entryPrice) * 100 : 0;
  // For short positions, P&L is inverted
  const adjustedPnlPct = position.direction === 'short' ? -unrealizedPnlPct : unrealizedPnlPct;

  return {
    delta: Number(ticker?.option_pricing?.d) || position.delta || 0,
    mark_price: markPrice,
    spot_price: spotPrice,
    unrealized_pnl_pct: adjustedPnlPct,
    dte: dte,
    iv: Number(ticker?.option_pricing?.i) || 0,
    theta: Number(ticker?.option_pricing?.t) || position.theta || 0,
  };
};

const getRuleEvaluationValues = (position, ticker, spotPrice, action = null) => {
  const values = computeCurrentValues(position, ticker, spotPrice);
  if (action === 'sell_put' && position?.direction === 'long') {
    const executablePrice = Number(ticker?.b) || values.mark_price || 0;
    const entryPrice = Number(position?.avg_entry_price) || 0;
    const adjustedPnlPct = entryPrice > 0 ? ((executablePrice - entryPrice) / entryPrice) * 100 : 0;

    return {
      ...values,
      unrealized_pnl_pct: adjustedPnlPct,
      execution_price: executablePrice,
    };
  }

  if (action !== 'buyback_call' || position?.direction !== 'short') return values;

  const executablePrice = Number(ticker?.a) || values.mark_price || 0;
  const entryPrice = Number(position?.avg_entry_price) || 0;
  const rawPnlPct = entryPrice > 0 ? ((executablePrice - entryPrice) / entryPrice) * 100 : 0;
  const adjustedPnlPct = position.direction === 'short' ? -rawPnlPct : rawPnlPct;

  return {
    ...values,
    unrealized_pnl_pct: adjustedPnlPct,
    execution_price: executablePrice,
  };
};

const parseMaybeJsonObject = (value) => {
  if (!value) return null;
  if (typeof value === 'object' && !Array.isArray(value)) return value;
  if (typeof value !== 'string') return null;
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
};

const isBuybackProfitCaptureCondition = (condition) => {
  if (!condition || condition.field !== 'unrealized_pnl_pct') return false;
  if (!['gte', 'gt'].includes(condition.op)) return false;
  return Number.isFinite(Number(condition.value ?? condition.threshold));
};

const REDUNDANT_BUYBACK_CAPTURE_FIELDS = new Set(['dte', 'mark_price']);

const isThreatManagementBuybackCriteria = (criteria) => (
  criteria?.allow_below_profit_floor === true
  || criteria?.buyback_intent === 'threat_management'
);

const getBuybackProfitCaptureCondition = (criteria) => {
  const parsed = parseMaybeJsonObject(criteria);
  if (!parsed || !Array.isArray(parsed.conditions)) return null;
  const condition = parsed.conditions.find(isBuybackProfitCaptureCondition);
  if (!condition) return null;
  return {
    op: condition.op,
    threshold: Number(condition.value ?? condition.threshold),
  };
};

const conditionPasses = (actual, op, threshold) => {
  if (!Number.isFinite(actual) || !Number.isFinite(threshold)) return false;
  if (op === 'gt') return actual > threshold;
  if (op === 'gte') return actual >= threshold;
  if (op === 'lt') return actual < threshold;
  if (op === 'lte') return actual <= threshold;
  return false;
};

const getRuleIntent = (criteria, keys) => {
  for (const key of keys) {
    const value = String(criteria?.[key] || '').trim();
    if (value) return value;
  }
  return null;
};

const getPutExitIntent = (criteria) => getRuleIntent(criteria, ['put_exit_intent', 'exit_intent']);
const getBuybackIntent = (criteria) => getRuleIntent(criteria, ['buyback_intent']);

const isFiniteNumber = (value) => Number.isFinite(Number(value));

const isRangeWithin = (range, min, max) => (
  Array.isArray(range)
  && range.length === 2
  && isFiniteNumber(range[0])
  && isFiniteNumber(range[1])
  && Number(range[0]) <= Number(range[1])
  && Number(range[0]) >= min
  && Number(range[1]) <= max
);

const getConditions = (criteria) => Array.isArray(criteria?.conditions) ? criteria.conditions : [];

const hasCondition = (criteria, predicate) => getConditions(criteria).some(predicate);

const hasThresholdCondition = (criteria, field, ops, threshold, mode = 'at_least') => (
  hasCondition(criteria, (condition) => {
    const value = Number(condition?.value ?? condition?.threshold);
    if (condition?.field !== field || !ops.includes(condition?.op) || !Number.isFinite(value)) return false;
    return mode === 'at_most' ? value <= threshold : value >= threshold;
  })
);

const hasLongerDatedPutProtection = (position, positions = [], closeAmount = position?.amount) => (
  getPutReplacementCoverage(position, positions, closeAmount).allowed
);

const hasLongerDatedPutProtectionSnapshot = (snapshot, snapshots = []) => (
  getPutReplacementCoverage(snapshot, snapshots).allowed
);

const getTotalLongPutAmount = (positions = []) => (positions || [])
  .filter((position) => position?.direction === 'long' && position?.instrument_name?.endsWith('-P'))
  .reduce((total, position) => total + Math.max(0, Number(position.amount) || 0), 0);

const leavesDownsideProtectionAfterSale = (position, positions = [], sellAmount = 0) => {
  const totalLongPutAmount = getTotalLongPutAmount(positions);
  const amount = Math.max(0, Number(sellAmount) || 0);
  return totalLongPutAmount - amount > 1e-9 && Number(position?.amount) - amount > 1e-9;
};

const getSellPutExitAmount = (rule, criteria, position, values) => {
  const fullAmount = Math.max(0, Number(position?.amount) || 0);
  if (fullAmount <= 0 || rule?.action !== 'sell_put') return fullAmount;

  const intent = getPutExitIntent(criteria);
  const dte = Number(values?.dte);
  const pnlPct = Number(values?.unrealized_pnl_pct);
  const isTailWin = intent === 'monetize_tail_win'
    || (Number.isFinite(dte) && dte > PUT_ROLL_DTE_THRESHOLD
      && Number.isFinite(pnlPct) && pnlPct > PUT_MONETIZATION_PROFIT_THRESHOLD);
  if (!isTailWin) return fullAmount;

  const requestedFraction = Number(criteria?.tranche_fraction ?? criteria?.max_tranche_fraction);
  const fraction = Number.isFinite(requestedFraction) && requestedFraction > 0
    ? Math.min(requestedFraction, PUT_MONETIZATION_MAX_TRANCHE_FRACTION)
    : PUT_MONETIZATION_MAX_TRANCHE_FRACTION;
  return Math.max(0, Math.min(fullAmount * fraction, fullAmount - 1e-9));
};

// null once every ladder rung has been sold: the remainder is held to settlement.
// A tranche counts once its resting order is no longer open, so a partial fill cannot advance the rung
// and cancel its own remainder. A repriced partial tranche can still count twice — that only makes us hold more.
const getPutMonetizationThresholdPct = (instrumentName) => {
  const sold = db && typeof db.countSellPutTranches === 'function' && instrumentName ? db.countSellPutTranches(instrumentName) : 0;
  return sold < PUT_MONETIZATION_LADDER_PCT.length ? PUT_MONETIZATION_LADDER_PCT[sold] : null;
};

const getLongPutIntrinsicValue = (position, spotPrice) => {
  const parsed = parseAdvisoryOptionInstrument(position?.instrument_name);
  return parsed?.optionType === 'P' && Number.isFinite(parsed.strike) && Number(spotPrice) > 0
    ? Math.max(0, parsed.strike - Number(spotPrice)) : 0;
};

const getAdvisorSellPutLimitPrice = (criteria) => {
  const explicit = Number(criteria?.min_exit_price ?? criteria?.limit_price ?? criteria?.target_exit_price);
  return Number.isFinite(explicit) && explicit > 0 ? explicit : null;
};

const getLongPutFairValueProof = (position, values = {}) => {
  const entryPrice = Number(position?.avg_entry_price);
  if (!(entryPrice > 0)) return null;

  const parsed = parseAdvisoryOptionInstrument(position?.instrument_name);
  const spotPrice = Number(values?.spot_price);
  const intrinsicValue = parsed?.optionType === 'P' && Number.isFinite(parsed.strike) && spotPrice > 0
    ? Math.max(0, parsed.strike - spotPrice)
    : 0;
  const markPrice = Number(values?.mark_price);
  const normalizedMarkPrice = Number.isFinite(markPrice) && markPrice > 0 ? markPrice : 0;
  const fairValuePrice = Math.max(
    normalizedMarkPrice,
    intrinsicValue
  );
  if (!(fairValuePrice > 0)) return null;

  return {
    price: fairValuePrice,
    pnlPct: ((fairValuePrice - entryPrice) / entryPrice) * 100,
    source: intrinsicValue > normalizedMarkPrice ? 'intrinsic_value' : 'mark_price',
  };
};

const getPatientSellPutPlan = (rule, criteria, position, values = {}) => {
  if (!rule || rule.action !== 'sell_put') return null;
  if (getPutExitIntent(criteria) !== 'monetize_tail_win') return null;

  const advisorLimit = getAdvisorSellPutLimitPrice(criteria);
  const entryPrice = Number(position?.avg_entry_price);
  if (!(advisorLimit > 0) || !(entryPrice > 0)) return null;
  const thresholdPct = getPutMonetizationThresholdPct(position?.instrument_name);
  if (thresholdPct == null) return null;

  // Settlement pays intrinsic for free, so our resting offer never goes below (most of) it.
  const limitPrice = Math.max(advisorLimit, PUT_MONETIZATION_MIN_INTRINSIC_FRACTION * getLongPutIntrinsicValue(position, values?.spot_price));
  const pnlPct = ((limitPrice - entryPrice) / entryPrice) * 100;
  if (!(pnlPct > thresholdPct)) return null;

  const fairValueProof = getLongPutFairValueProof(position, values);
  if (!(Number(fairValueProof?.pnlPct) > thresholdPct)) return null;

  return {
    limitPrice,
    pnlPct,
    fairValuePrice: fairValueProof.price,
    fairValuePnlPct: fairValueProof.pnlPct,
    fairValueSource: fairValueProof.source,
    preferredOrderType: normalizePreferredOrderType(rule.action, rule.preferred_order_type) || 'post_only',
  };
};

const getBuybackTargetCapturePct = (criteria) => {
  const explicit = Number(criteria?.target_capture_pct ?? criteria?.capture_floor_pct);
  if (Number.isFinite(explicit) && explicit > 0) return explicit;
  const condition = getBuybackProfitCaptureCondition(criteria);
  return Number.isFinite(condition?.threshold) ? condition.threshold : null;
};

const getPatientBuybackPlan = (rule, criteria, position) => {
  if (!rule || rule.action !== 'buyback_call') return null;
  if (getBuybackIntent(criteria) !== 'profit_capture') return null;
  const preferredOrderType = normalizePreferredOrderType(rule.action, rule.preferred_order_type) || 'post_only';

  const targetCapturePct = Number(getBuybackTargetCapturePct(criteria));
  const entryPrice = Number(position?.avg_entry_price);
  if (!Number.isFinite(targetCapturePct) || targetCapturePct < CALL_BUYBACK_PROFIT_THRESHOLD || !(entryPrice > 0)) return null;

  const explicitLimit = Number(criteria?.max_buyback_price ?? criteria?.limit_price);
  const derivedLimit = entryPrice * (1 - targetCapturePct / 100);
  const limitPrice = Number.isFinite(explicitLimit) && explicitLimit > 0
    ? Math.min(explicitLimit, derivedLimit)
    : derivedLimit;
  if (!(limitPrice > 0)) return null;

  const capturePct = ((entryPrice - limitPrice) / entryPrice) * 100;
  if (capturePct + 1e-9 < targetCapturePct) return null;

  return {
    limitPrice,
    ceilingPrice: limitPrice,
    capturePct,
    entryPrice,
    targetCapturePct,
    preferredOrderType,
    priceReason: 'capture_floor_ceiling',
  };
};

const getBuybackCapturePctAtPrice = (entryPrice, buybackPrice) => {
  const entry = Number(entryPrice);
  const price = Number(buybackPrice);
  if (!(entry > 0) || !(price > 0)) return null;
  return ((entry - price) / entry) * 100;
};

const refinePatientBuybackPlanPrice = (plan, ticker, instrument) => {
  if (!plan) return null;

  const entryPrice = Number(plan.entryPrice);
  const targetCapturePct = Number(plan.targetCapturePct);
  const rawCeiling = Number(plan.ceilingPrice ?? plan.limitPrice);
  if (!(entryPrice > 0) || !(targetCapturePct > 0) || !(rawCeiling > 0)) return null;

  const step = getInstrumentPriceStep(instrument, rawCeiling);
  const ceilingPrice = normalizePriceToStep(rawCeiling, step, 'down');
  if (!(ceilingPrice > 0)) return null;

  const build = (price, reason, mode = 'down') => {
    const normalized = normalizePriceToStep(Math.min(Number(price), ceilingPrice), step, mode);
    if (!(normalized > 0) || normalized > ceilingPrice + 1e-9) return null;
    const capturePct = getBuybackCapturePctAtPrice(entryPrice, normalized);
    if (!Number.isFinite(capturePct) || capturePct + 1e-9 < targetCapturePct) return null;
    return {
      ...plan,
      limitPrice: normalized,
      ceilingPrice,
      capturePct,
      priceReason: reason,
      priceStep: step,
    };
  };

  const askPrice = Number(ticker?.a) || 0;
  if (askPrice > 0 && askPrice <= ceilingPrice + 1e-9) {
    const executable = normalizePriceToStep(askPrice, step, 'up');
    return build(executable <= ceilingPrice + 1e-9 ? executable : ceilingPrice, 'live_ask_below_capture_ceiling', 'down')
      || build(ceilingPrice, 'capture_floor_ceiling');
  }

  const bidPrice = Number(ticker?.b) || 0;
  if (bidPrice > 0 && bidPrice < ceilingPrice) {
    return build(Math.min(bidPrice + step, ceilingPrice), 'one_tick_above_best_bid')
      || build(ceilingPrice, 'capture_floor_ceiling');
  }

  const markPrice = Number(ticker?.M) || 0;
  if (markPrice > 0 && markPrice < ceilingPrice) {
    return build(markPrice, 'mark_anchor_below_capture_ceiling')
      || build(ceilingPrice, 'capture_floor_ceiling');
  }

  return build(ceilingPrice, 'capture_floor_ceiling');
};

const validateAdvisorRuleContract = (rule, context = {}) => {
  const criteria = parseMaybeJsonObject(rule?.criteria);
  if (!rule || !criteria) return { valid: false, reason: 'criteria must be a JSON object' };

  if (rule.rule_type === 'entry' && rule.action === 'buy_put') {
    const rawValueSignal = criteria.value_signal ?? criteria.buy_put_signal;
    if (criteria.option_type !== 'P') return { valid: false, reason: 'buy_put requires option_type P' };
    if (!isRangeWithin(criteria.delta_range, PUT_DELTA_RANGE[0], PUT_DELTA_RANGE[1])) return { valid: false, reason: `buy_put delta_range must stay within ${JSON.stringify(PUT_DELTA_RANGE)}` };
    if (!isRangeWithin(criteria.dte_range, BUY_PUT_ADVISORY_DTE_RANGE[0], BUY_PUT_ADVISORY_DTE_RANGE[1])) return { valid: false, reason: `buy_put dte_range must stay within ${JSON.stringify(BUY_PUT_ADVISORY_DTE_RANGE)}` };
    if (Object.prototype.hasOwnProperty.call(criteria, 'max_cost')) return { valid: false, reason: 'buy_put must use budget_limit/min_score/target_score, not max_cost' };
    if (!isKnownBuyPutValueSignal(rawValueSignal)) return { valid: false, reason: `unknown buy_put value_signal: ${rawValueSignal}` };
    if (!(Number(criteria.min_score) > 0)) return { valid: false, reason: 'buy_put requires min_score as a value gate' };
    return { valid: true };
  }

  if (rule.rule_type === 'entry' && rule.action === 'sell_call') {
    if (criteria.option_type !== 'C') return { valid: false, reason: 'sell_call requires option_type C' };
    if (!isRangeWithin(criteria.delta_range, CALL_DELTA_RANGE[0], CALL_DELTA_RANGE[1])) return { valid: false, reason: `sell_call delta_range must stay within ${JSON.stringify(CALL_DELTA_RANGE)}` };
    if (!isRangeWithin(criteria.dte_range, CALL_EXPIRATION_RANGE[0], CALL_EXPIRATION_RANGE[1])) return { valid: false, reason: `sell_call dte_range must stay within ${JSON.stringify(CALL_EXPIRATION_RANGE)}` };
    if (!(Number(criteria.min_score) > 0)) return { valid: false, reason: 'sell_call requires min_score as the DTE-normalized CALL EDGE gate' };
    if (!(Number(criteria.min_bid) > 0)) return { valid: false, reason: 'sell_call requires min_bid as the liquidity/premium floor' };
    const marketConditions = Array.isArray(criteria.market_conditions) ? criteria.market_conditions : [];
    if (marketConditions.some((condition) => condition?.field !== 'spot_price')) return { valid: false, reason: 'sell_call market_conditions may only use spot_price as supporting context' };
    return { valid: true };
  }

  if (rule.rule_type === 'exit' && rule.action === 'sell_put') {
    if (!Array.isArray(criteria.conditions) || criteria.conditions.length === 0) return { valid: false, reason: 'sell_put requires structured conditions' };
    const intent = getPutExitIntent(criteria);
    if (!['roll_protection', 'monetize_tail_win'].includes(intent)) return { valid: false, reason: 'sell_put requires put_exit_intent roll_protection or monetize_tail_win' };

    if (intent === 'roll_protection') {
      if (!hasThresholdCondition(criteria, 'dte', ['lt', 'lte'], PUT_ROLL_DTE_THRESHOLD, 'at_most')) return { valid: false, reason: `roll_protection requires dte <= ${PUT_ROLL_DTE_THRESHOLD}` };
      if (criteria.requires_longer_dated_protection !== true) return { valid: false, reason: 'roll_protection must require longer-dated protection in the book' };
      if (rule.preferred_order_type && rule.preferred_order_type !== 'ioc') return { valid: false, reason: 'roll_protection sell_put must use ioc/non-resting execution' };
      const snapshot = (context.positionSnapshots || []).find((item) => item?.instrument === rule.instrument_name);
      if (snapshot && !hasLongerDatedPutProtectionSnapshot(snapshot, context.positionSnapshots)) return { valid: false, reason: 'roll_protection rejected: no longer-dated long put currently in book' };
    }

    if (intent === 'monetize_tail_win') {
      if (!hasThresholdCondition(criteria, 'unrealized_pnl_pct', ['gt', 'gte'], PUT_MONETIZATION_PROFIT_THRESHOLD)) return { valid: false, reason: `monetize_tail_win requires executable unrealized_pnl_pct > ${PUT_MONETIZATION_PROFIT_THRESHOLD}` };
      if (!(getAdvisorSellPutLimitPrice(criteria) > 0)) return { valid: false, reason: 'monetize_tail_win requires min_exit_price/limit_price so sparse markets cannot force an undersell' };
      const fraction = Number(criteria.tranche_fraction ?? criteria.max_tranche_fraction ?? PUT_MONETIZATION_MAX_TRANCHE_FRACTION);
      if (!(fraction > 0) || fraction > PUT_MONETIZATION_MAX_TRANCHE_FRACTION) return { valid: false, reason: `monetize_tail_win tranche_fraction must be >0 and <=${PUT_MONETIZATION_MAX_TRANCHE_FRACTION}` };
      if (criteria.retain_downside_protection !== true) return { valid: false, reason: 'monetize_tail_win must require retained downside protection' };
    }
    return { valid: true };
  }

  if (rule.rule_type === 'exit' && rule.action === 'buyback_call') {
    if (!Array.isArray(criteria.conditions) || criteria.conditions.length === 0) return { valid: false, reason: 'buyback_call requires structured conditions' };
    const intent = getBuybackIntent(criteria);
    if (!['profit_capture', 'threat_management'].includes(intent)) return { valid: false, reason: 'buyback_call requires buyback_intent profit_capture or threat_management' };

    if (intent === 'profit_capture') {
      if (criteria.allow_below_profit_floor === true) return { valid: false, reason: 'profit_capture cannot allow below profit floor' };
      if ((criteria.condition_logic || 'all') !== 'all') return { valid: false, reason: 'profit_capture requires condition_logic all' };
      if (getConditions(criteria).some((condition) => REDUNDANT_BUYBACK_CAPTURE_FIELDS.has(condition?.field))) return { valid: false, reason: 'profit_capture buyback cannot use dte or mark_price blockers' };
      if (!hasThresholdCondition(criteria, 'unrealized_pnl_pct', ['gt', 'gte'], CALL_BUYBACK_PROFIT_THRESHOLD)) return { valid: false, reason: `profit_capture requires executable unrealized_pnl_pct >= ${CALL_BUYBACK_PROFIT_THRESHOLD}` };
    }

    if (intent === 'threat_management') {
      if (criteria.allow_below_profit_floor !== true) return { valid: false, reason: 'threat_management must set allow_below_profit_floor=true' };
      const fields = new Set(getConditions(criteria).map((condition) => condition?.field));
      if (!fields.has('delta') && !fields.has('spot_price')) return { valid: false, reason: 'threat_management requires delta or spot_price threat evidence' };
      if (!fields.has('dte')) return { valid: false, reason: 'threat_management requires remaining-DTE context' };
    }
    return { valid: true };
  }

  return { valid: false, reason: `unsupported rule contract ${rule.rule_type}/${rule.action}` };
};

const normalizeBuybackCaptureFloor = (rule) => {
  if (!rule || rule.rule_type !== 'exit' || rule.action !== 'buyback_call') return { rule, changed: false };

  const criteria = parseMaybeJsonObject(rule.criteria);
  if (!criteria || !Array.isArray(criteria.conditions)) return { rule, changed: false };
  if (isThreatManagementBuybackCriteria(criteria)) return { rule, changed: false };

  let changed = false;
  let previousFloor = null;
  let hasProfitCaptureCondition = false;
  const removedCaptureBlockers = new Set();
  const conditions = [];
  for (const condition of criteria.conditions) {
    if (REDUNDANT_BUYBACK_CAPTURE_FIELDS.has(condition?.field)) {
      removedCaptureBlockers.add(condition.field);
      changed = true;
      continue;
    }
    if (!isBuybackProfitCaptureCondition(condition)) {
      conditions.push(condition);
      continue;
    }
    hasProfitCaptureCondition = true;
    const threshold = Number(condition.value ?? condition.threshold);
    if (threshold >= CALL_BUYBACK_PROFIT_THRESHOLD) {
      conditions.push(condition);
      continue;
    }
    previousFloor = previousFloor == null ? threshold : Math.min(previousFloor, threshold);
    changed = true;
    conditions.push({ ...condition, value: CALL_BUYBACK_PROFIT_THRESHOLD });
  }

  if (!hasProfitCaptureCondition) {
    conditions.push({ field: 'unrealized_pnl_pct', op: 'gte', value: CALL_BUYBACK_PROFIT_THRESHOLD });
    changed = true;
  }

  const previousLogic = criteria.condition_logic || 'all';
  const conditionLogic = 'all';
  if (previousLogic !== conditionLogic) changed = true;

  if (!changed) return { rule, changed: false };

  const suffixParts = [];
  if (previousFloor != null) {
    suffixParts.push(`capture floor: ${previousFloor}% -> ${CALL_BUYBACK_PROFIT_THRESHOLD}%`);
  } else if (!hasProfitCaptureCondition) {
    suffixParts.push(`added capture floor: ${CALL_BUYBACK_PROFIT_THRESHOLD}%`);
  }
  if (previousLogic !== conditionLogic) {
    suffixParts.push(`condition_logic: ${previousLogic} -> ${conditionLogic}`);
  }
  if (removedCaptureBlockers.size > 0) {
    suffixParts.push(`removed redundant ${Array.from(removedCaptureBlockers).join('/')} capture blocker(s)`);
  }
  const suffix = `normalized buyback ${suffixParts.join(', ')}`;
  return {
    rule: {
      ...rule,
      criteria: { ...criteria, conditions, condition_logic: conditionLogic },
      reasoning: rule.reasoning ? `${rule.reasoning} [${suffix}]` : suffix,
    },
    changed: true,
    reason: suffix,
  };
};

const getBuybackCaptureGate = (rule, criteria, values) => {
  if (!rule || rule.rule_type !== 'exit' || rule.action !== 'buyback_call') {
    return { allowed: true };
  }
  if (isThreatManagementBuybackCriteria(criteria)) {
    return { allowed: true };
  }

  const condition = getBuybackProfitCaptureCondition(criteria);
  if (!condition) {
    return {
      allowed: false,
      reason: `missing executable unrealized_pnl_pct >= ${CALL_BUYBACK_PROFIT_THRESHOLD}% capture floor`,
    };
  }

  const actual = Number(values?.unrealized_pnl_pct);
  const patientCapture = Number(values?.patient_buyback_capture_pct);
  if (
    !conditionPasses(actual, condition.op, condition.threshold)
    && !conditionPasses(patientCapture, condition.op, condition.threshold)
  ) {
    const opText = condition.op === 'gt' ? '>' : condition.op === 'gte' ? '>=' : condition.op;
    return {
      allowed: false,
      reason: `executable capture ${Number.isFinite(actual) ? actual.toFixed(2) : 'N/A'}% does not satisfy ${opText} ${condition.threshold}%`,
    };
  }

  return { allowed: true };
};

const getSellPutProtectionGate = (rule, values, context = {}) => {
  if (!rule || rule.action !== 'sell_put') {
    return { allowed: true };
  }

  const criteria = parseMaybeJsonObject(context.criteria ?? rule.criteria) || {};
  const intent = getPutExitIntent(criteria);
  const dte = Number(values?.dte);
  if (Number.isFinite(dte) && dte <= PUT_ROLL_DTE_THRESHOLD && intent !== 'monetize_tail_win') {
    if ((intent === 'roll_protection' || !intent) && !hasLongerDatedPutProtection(context.position, context.positions || [], context.plannedSellAmount ?? context.position?.amount)) {
      return {
        allowed: false,
        reason: 'roll_protection requires enough later-expiry long puts at the same or higher strike to replace the quantity sold',
      };
    }
    const rollEntry = Number(context.position?.avg_entry_price);
    const rollBid = Number(values?.execution_price) || Number(values?.mark_price);
    if (rollEntry > 0 && !(rollBid >= rollEntry * PUT_ROLL_MIN_RECOVERY_PCT / 100)) {
      return {
        allowed: false,
        reason: `roll_protection bid $${Number.isFinite(rollBid) ? rollBid.toFixed(2) : 'n/a'} recovers less than ${PUT_ROLL_MIN_RECOVERY_PCT}% of cost $${rollEntry.toFixed(2)}; hold the ticket`,
      };
    }
    return { allowed: true };
  }

  const monetizationThresholdPct = getPutMonetizationThresholdPct(context.position?.instrument_name);
  if (monetizationThresholdPct == null) {
    return { allowed: false, reason: `monetization ladder complete (${PUT_MONETIZATION_LADDER_PCT.join('/')}%); remaining protection is held to settlement` };
  }

  const livePnlPct = Number(values?.unrealized_pnl_pct);
  const fairValuePnlPct = Number(values?.patient_sell_put_fair_value_pnl_pct);
  const proofPnlPct = Math.max(
    Number.isFinite(livePnlPct) ? livePnlPct : -Infinity,
    Number.isFinite(fairValuePnlPct) ? fairValuePnlPct : -Infinity
  );
  if (Number.isFinite(proofPnlPct) && proofPnlPct > monetizationThresholdPct) {
    const plannedSellAmount = Number(context.plannedSellAmount ?? context.position?.amount ?? 0);
    if (!leavesDownsideProtectionAfterSale(context.position, context.positions || [], plannedSellAmount)) {
      return {
        allowed: false,
        reason: 'monetize_tail_win would remove all downside protection; tranche or retain protection before selling',
      };
    }
    return { allowed: true };
  }

  const dteText = Number.isFinite(dte) ? dte.toFixed(2) : 'n/a';
  const livePnlText = Number.isFinite(livePnlPct) ? `${livePnlPct.toFixed(2)}%` : 'n/a';
  const fairPnlText = Number.isFinite(fairValuePnlPct) ? `${fairValuePnlPct.toFixed(2)}%` : 'n/a';
  return {
    allowed: false,
    reason: `protective long put exit requires dte <= ${PUT_ROLL_DTE_THRESHOLD} for rolling or extreme current pnl proof > ${monetizationThresholdPct}% for this monetization tranche; actual dte=${dteText}, executable_pnl=${livePnlText}, fair_value_pnl=${fairPnlText}`,
  };
};

const buildBuybackConfirmationContext = (action, triggerData) => {
  if (action?.action !== 'buyback_call') return null;

  const ruleCondition = getBuybackProfitCaptureCondition(action.rule_criteria);
  const triggerCondition = Array.isArray(triggerData?.conditions_met)
    ? triggerData.conditions_met.find(isBuybackProfitCaptureCondition)
    : null;

  const threshold = Number(ruleCondition?.threshold ?? triggerCondition?.threshold ?? triggerCondition?.value);
  const op = ruleCondition?.op || triggerCondition?.op || 'gte';
  const actual = Number(
    triggerCondition?.actual
    ?? triggerData?.current_values?.unrealized_pnl_pct
    ?? triggerData?.unrealized_pnl_pct
  );
  const executionPrice = Number(triggerData?.current_values?.execution_price);
  const patientLimitPrice = Number(triggerData?.advisor_limit_price ?? triggerData?.current_values?.patient_buyback_limit_price);
  const patientCeilingPrice = Number(triggerData?.patient_buyback_ceiling_price ?? triggerData?.current_values?.patient_buyback_ceiling_price);
  const patientCapturePct = Number(triggerData?.patient_buyback_capture_pct ?? triggerData?.current_values?.patient_buyback_capture_pct);

  if (!Number.isFinite(threshold)) return null;
  const actualSatisfied = conditionPasses(actual, op, threshold);
  const patientSatisfied = conditionPasses(patientCapturePct, op, threshold);

  return {
    threshold,
    op,
    actual: Number.isFinite(actual) ? actual : null,
    executionPrice: Number.isFinite(executionPrice) && executionPrice > 0 ? executionPrice : null,
    patientLimitPrice: Number.isFinite(patientLimitPrice) && patientLimitPrice > 0 ? patientLimitPrice : null,
    patientCeilingPrice: Number.isFinite(patientCeilingPrice) && patientCeilingPrice > 0 ? patientCeilingPrice : null,
    patientCapturePct: Number.isFinite(patientCapturePct) ? patientCapturePct : null,
    satisfied: actualSatisfied || patientSatisfied,
    actualSatisfied,
    patientSatisfied,
  };
};

const formatBuybackConfirmationContext = (context, liveMarketPrice) => {
  if (!context) return '';
  const actualText = Number.isFinite(context.actual) ? `${context.actual.toFixed(2)}%` : 'N/A';
  const opText = context.op === 'gt' ? '>' : '>=';
  const liveText = Number(liveMarketPrice) > 0 ? `$${Number(liveMarketPrice).toFixed(4)}` : 'unavailable';
  const executionText = context.executionPrice ? `$${context.executionPrice.toFixed(4)}` : 'N/A';
  const patientText = context.patientLimitPrice
    ? `; patient bid $${context.patientLimitPrice.toFixed(4)} would capture ${Number.isFinite(context.patientCapturePct) ? `${context.patientCapturePct.toFixed(2)}%` : 'N/A'}`
    : '';
  const ceilingText = context.patientCeilingPrice && context.patientCeilingPrice !== context.patientLimitPrice
    ? `; max buyback ceiling $${context.patientCeilingPrice.toFixed(4)}`
    : '';
  return [
    'Advisor-rule buyback context:',
    `- Active buyback_call rule threshold: executable unrealized_pnl_pct ${opText} ${context.threshold}%`,
    `- Current executable capture from trigger details: ${actualText}; live_rule_satisfied=${context.actualSatisfied ? 'yes' : 'no'}; patient_bid_satisfies_rule=${context.patientSatisfied ? 'yes' : 'no'}; rule_satisfied=${context.satisfied ? 'yes' : 'no'}`,
    `- Live buyback ask: ${liveText}; trigger execution_price=${executionText}${patientText}${ceilingText}`,
    `- If the rule names max_buyback_price, treat that cap as a ceiling, not a target: use a patient synthetic reduce-only gtc/post_only limit at or below the named price when profit_capture is the intent, and keep any extra edge available from lower live asks, lower visible bids, or sparse-book price improvement.`,
    `- Confirmation should validate live price, reduce-only semantics, and rule consistency. Do not reject solely because the call is OTM, delta is low, theta remains, or the rule is a profit-harvest/capacity-reset rather than a threat signal. The advisor rule is the source of strategic intent for this pending exit.`,
  ].join('\n');
};

const formatBuyPutConfirmationContext = ({ action, triggerData, ticker, currentPrice, advisorLimitPrice, instrument }) => {
  if (action?.action !== 'buy_put') return '';
  const criteria = parseMaybeJsonObject(action.rule_criteria) || {};
  const triggerScore = Number(triggerData?.score);
  const triggerDelta = Number(triggerData?.delta);
  const liveDelta = Number(ticker?.option_pricing?.d);
  const bestAsk = Number(currentPrice);
  const targetScore = Number(triggerData?.target_score);
  const minScore = Number(criteria.min_score ?? triggerData?.min_score);
  const liveDte = computeDteFromInstrumentName(action.instrument_name);
  const liveScore = bestAsk > 0 && Number.isFinite(liveDelta) && liveDte > 0
    ? normalizeBuyPutScore(Math.abs(liveDelta) / bestAsk, liveDte)
    : null;
  const requiredValueSignal = triggerData?.required_value_signal || criteria.value_signal || criteria.buy_put_signal || null;
  const currentValueSignal = triggerData?.buy_put_signal || null;
  const buyPutResearch = triggerData?.buy_put_research || {};
  const edgeComponents = buyPutResearch?.edge_components || {};
  const shockMultiples = edgeComponents.shock_payoff_multiples || {};
  const isStandingPatientBid = requiredValueSignal === 'any_actionable_buy_put' && currentValueSignal === 'standing_patient_bid';
  const limitPrice = Number(advisorLimitPrice) > 0 && bestAsk > 0
    ? Math.min(Number(advisorLimitPrice), bestAsk)
    : Number(advisorLimitPrice) > 0
      ? Number(advisorLimitPrice)
      : bestAsk;
  // Only re-plan when the limit would cross; a limit already inside the spread is the planned price.
  const makerPlan = instrument && Number(ticker?.a) > 0 && limitPrice >= Number(ticker.a)
    ? computePostOnlyRetryPrice('buy', ticker, instrument, limitPrice)
    : null;
  const plannedPrice = makerPlan?.retryPrice ?? limitPrice;
  const plannedPremium = Number(action?.amount) > 0 && plannedPrice > 0
    ? Number(action.amount) * plannedPrice
    : null;
  const scoreDelta = liveDelta;
  const plannedRawScore = Math.abs(scoreDelta) > 0 && plannedPrice > 0
    ? Math.abs(scoreDelta) / plannedPrice
    : null;
  const plannedScore = plannedRawScore > 0 && liveDte > 0
    ? normalizeBuyPutScore(plannedRawScore, liveDte)
    : null;

  const fmt = (value, digits = 6) => Number.isFinite(value) ? Number(value).toFixed(digits) : 'n/a';
  const fmtPrice = (value) => Number(value) > 0 ? `$${Number(value).toFixed(4)}` : 'n/a';
  return [
    'Buy-put value confirmation context:',
    `- Trigger score: ${fmt(triggerScore)} from pending action; trigger_delta=${fmt(triggerDelta, 4)}, trigger_dte=${fmt(Number(triggerData?.dte), 2)}, trigger_strike=${triggerData?.strike ?? 'n/a'}.`,
    `- Planned execution limit: ${fmtPrice(plannedPrice)}${Number(advisorLimitPrice) > 0 ? `, capped by advisor_limit_price=${fmtPrice(advisorLimitPrice)}` : ''}; planned PUT_EDGE=${fmt(plannedScore)} using fresh_delta=${fmt(liveDelta, 4)}, fresh_dte=${fmt(liveDte, 2)}, and planned limit; live PUT_EDGE=${fmt(liveScore)}.`,
    `- Planned premium outlay (excluding fees): ${fmtPrice(plannedPremium)} for amount=${action?.amount ?? 'n/a'} at the planned execution limit. ${makerPlan ? 'The planned limit, PUT EDGE, and premium above all use the computed post_only bid below.' : 'These are reference-limit economics only; a maker price has not been established.'}`,
    `- Thresholds: min_score=${fmt(minScore)}, target_score=${fmt(targetScore)}. For patient maker bids, planned PUT EDGE at our limit is the economic gate; live PUT EDGE may be below threshold because we are not willing to lift the ask.`,
    '- PUT EDGE is the only ranking score. Legacy min_edge_score on saved buy_put rules is ignored; do not emit it or use it as a ranking, confirmation, or execution gate. Ignore historical composite selection_score and recommendation fields.',
    `- Recorded quote diagnostics (descriptive only; may predate this review): candidate_spread_pct:${fmt(edgeComponents.candidate_spread_pct, 2)}, candidate_iv_pct:${fmt(edgeComponents.candidate_iv_pct, 2)}, market_put_iv_pct:${fmt(edgeComponents.market_put_iv_pct, 2)}, skew_pct:${fmt(edgeComponents.market_skew_pct, 2)}, oi_24h:${fmt(edgeComponents.market_oi_delta_24h_pct, 2)}. No spread, IV/skew, OI, or shock-payoff multipliers or standalone vetoes apply.`,
    `- Recorded 30/40/50% drawdown payoff scenarios: shock30:${fmt(shockMultiples['30pct'], 2)}x, shock40:${fmt(shockMultiples['40pct'] ?? edgeComponents.shock_payoff_multiple_40pct, 2)}x, shock50:${fmt(shockMultiples['50pct'], 2)}x. These are hypothetical intrinsic-value/premium multiples, excluding fees; they are descriptive, not ranking inputs or payoff forecasts. PUT EDGE measures DTE-normalized delta per premium dollar, not literal crash payoff.`,
    `- value_signal=${currentValueSignal || 'n/a'}, required_value_signal=${requiredValueSignal || 'n/a'}. ${isStandingPatientBid ? 'This is a standing patient bid: no spike signal is active, but the bid is still valid if planned_score meets threshold and budget/risk gates remain valid.' : 'A qualifying value_signal plus planned_score meeting the rule threshold is sufficient value evidence for confirmation unless another concrete risk fact rejects it.'}`,
    `- Live reference only: current_best_ask=${fmtPrice(bestAsk)}, live_delta=${fmt(liveDelta, 4)}. If the planned limit is below the live ask, post_only/gtc can rest there as our market; do not reject as "not achievable" merely because it is not immediately marketable.`,
    '- BUY LIMIT CONTRACT: The approved buy limit is a maximum price, not an exact required fill price. A lower tick-aligned bid preserves the contract, reduces premium outlay, and improves PUT EDGE at unchanged delta/DTE. If the limit equals or crosses the ask, the executor can lower a post_only bid below the fresh ask without raising the approved cap. Do not reject solely for crossing at the reference limit or demand a separate advisory approval for a cheaper bid. Keep all value, budget, margin, and live-data checks.',
    makerPlan
      ? `- Computed post_only bid on this book: ${fmtPrice(makerPlan.retryPrice)}, below live ask ${fmtPrice(makerPlan.askPrice)} and no higher than approved cap ${fmtPrice(limitPrice)}; venue tick=${fmtPrice(makerPlan.step)}. If the remaining checks pass, confirm with order_type="post_only" and this limit_price. The executor refreshes the book before placement; a fill is not guaranteed.`
      : '- No computed maker bid is available from the supplied live book/instrument; do not invent a tick or executable price.',
    '- Prior IOC zero fill, if present elsewhere in this prompt, is liquidity/routing context only. Do not reject a valid buy_put solely because the previous IOC did not fill; choose gtc/post_only at the approved limit when making the market is better than chasing the ask.',
    '- Do not invent a different target score or use stale advisory-creation score language to override the current trigger score and planned limit. Descriptive market context does not replace the explicit min_score/target_score price contract or add another PUT ranking or veto. Keep concrete live-quote, price, delta/DTE, size, budget, margin, and active-rule checks.',
  ].join('\n');
};

const formatSellCallConfirmationContext = ({ action, triggerData, ticker, currentPrice }) => {
  if (action?.action !== 'sell_call') return '';
  const criteria = parseMaybeJsonObject(action.rule_criteria) || {};
  const triggerDte = Number(triggerData?.dte);
  const triggerDelta = Number(triggerData?.delta);
  const liveDelta = Number(ticker?.option_pricing?.d);
  const executionBid = Number(currentPrice || action.price);
  const scoreDelta = Number.isFinite(liveDelta) ? liveDelta : triggerDelta;
  const liveRawScore = Math.abs(scoreDelta) > 0 && executionBid > 0
    ? executionBid / Math.abs(scoreDelta)
    : null;
  const liveDte = computeDteFromInstrumentName(action.instrument_name);
  const liveEdgeScore = normalizeSellCallScore(liveRawScore, liveDte);
  const triggerRawScore = Number(triggerData?.raw_score ?? triggerData?.sell_call_research?.edge_components?.raw_score);
  const triggerEdgeScore = Number(triggerData?.selection_score ?? triggerData?.score);
  const configuredMinScore = Number(criteria.min_score);
  const minScore = Number.isFinite(configuredMinScore) && configuredMinScore > 0
    ? configuredMinScore
    : SELL_CALL_FALLBACK_MIN_SCORE;
  const gateStatus = Number.isFinite(liveEdgeScore) && liveEdgeScore + 1e-9 >= minScore ? 'PASS' : 'FAIL';
  const fmt = (value, digits = 4) => Number.isFinite(value) ? Number(value).toFixed(digits) : 'n/a';
  const fmtPrice = (value) => Number(value) > 0 ? `$${Number(value).toFixed(4)}` : 'n/a';

  return [
    'Sell-call value confirmation context:',
    `- CALL EDGE formula: raw_score * (${SELL_CALL_EDGE_REFERENCE_DTE} / DTE)^${SELL_CALL_EDGE_DTE_EXPONENT}; raw_score = bid / abs(delta).`,
    `- Trigger: raw_score=${fmt(triggerRawScore, 2)}, edge_score=${fmt(triggerEdgeScore, 2)}, bid=${fmtPrice(triggerData?.live_price ?? action.price)}, delta=${fmt(triggerDelta, 4)}, DTE=${fmt(triggerDte, 2)}, strike=${triggerData?.strike ?? 'n/a'}.`,
    `- Fresh market: raw_score=${fmt(liveRawScore, 2)}, edge_score=${fmt(liveEdgeScore, 2)}, executable_bid=${fmtPrice(executionBid)}, live_delta=${fmt(liveDelta, 4)}.`,
    `- Rule: min_score=${fmt(minScore, 2)}, min_bid=${fmtPrice(criteria.min_bid)}, edge_gate=${gateStatus} (edge_score=${fmt(liveEdgeScore, 2)} ${gateStatus === 'PASS' ? '>=' : '<'} min_score=${fmt(minScore, 2)}).`,
    '- Confirm only when fresh CALL EDGE, min_bid, delta/DTE, and margin facts still satisfy the rule. Old saved min_edge_score values are not composite gates; they use the normalized fallback floor until the next advisory refresh.',
  ].join('\n');
};
const formatSellPutConfirmationContext = ({ action, triggerData, livePositions, advisorLimitPrice, currentPrice }) => {
  if (action?.action !== 'sell_put') return '';
  const criteria = parseMaybeJsonObject(action.rule_criteria) || {};
  const intent = triggerData?.put_exit_intent || getPutExitIntent(criteria) || 'n/a';
  const currentValues = triggerData?.current_values || {};
  const position = (livePositions || []).find((item) => item?.instrument_name === action.instrument_name);
  const plannedAmount = Number(action.amount);
  const positionAmount = Number(position?.amount);
  const totalLongPuts = getTotalLongPutAmount(livePositions || []);
  const remainingLongPuts = Number.isFinite(plannedAmount)
    ? Math.max(0, totalLongPuts - plannedAmount)
    : null;
  const retainsProtection = position
    ? leavesDownsideProtectionAfterSale(position, livePositions || [], plannedAmount)
    : null;
  const livePnlPct = Number(currentValues.unrealized_pnl_pct);
  const patientPnlPct = Number(triggerData?.patient_sell_put_pnl_pct ?? currentValues.patient_sell_put_pnl_pct);
  const fairValuePnlPct = Number(triggerData?.patient_sell_put_fair_value_pnl_pct ?? currentValues.patient_sell_put_fair_value_pnl_pct);
  const fairValuePrice = Number(triggerData?.patient_sell_put_fair_value_price ?? currentValues.patient_sell_put_fair_value_price);
  const fairValueSource = triggerData?.patient_sell_put_fair_value_source || currentValues.patient_sell_put_fair_value_source || 'n/a';
  const dte = Number(currentValues.dte);
  const liveBid = Number(currentValues.execution_price ?? currentPrice ?? action.price);
  const advisorFloor = Number(advisorLimitPrice ?? triggerData?.patient_sell_put_limit_price ?? currentValues.patient_sell_put_limit_price);
  const trancheFraction = Number(triggerData?.tranche_fraction ?? (
    Number.isFinite(plannedAmount) && positionAmount > 0 ? plannedAmount / positionAmount : NaN
  ));
  const hasLongerDatedProtection = position
    ? hasLongerDatedPutProtection(position, livePositions || [])
    : null;
  const fmt = (value, digits = 2) => Number.isFinite(value) ? Number(value).toFixed(digits) : 'n/a';
  const fmtPrice = (value) => Number(value) > 0 ? `$${Number(value).toFixed(4)}` : 'n/a';
  const baseLines = [
    'Sell-put exit confirmation context:',
    `- Intent=${intent}. sell_put closes an owned long put; it is reduce_only and capital-releasing, not a naked short-put entry.`,
    `- Current exit proof: live_bid=${fmtPrice(liveBid)}, live_unrealized_pnl_pct=${fmt(livePnlPct)}%, fair_value=${fmtPrice(fairValuePrice)} (${fairValueSource}), fair_value_pnl_pct=${fmt(fairValuePnlPct)}%, patient_floor_pnl_pct=${fmt(patientPnlPct)}%, dte=${fmt(dte)}, delta=${fmt(Number(currentValues.delta), 4)}, theta=${fmt(Number(currentValues.theta), 4)}.`,
  ];

  if (intent === 'roll_protection') {
    return [
      ...baseLines,
      `- Roll discipline: planned_close_amount=${fmt(plannedAmount, 4)} of aging_position_amount=${fmt(positionAmount, 4)}, longer_dated_protection_in_book=${hasLongerDatedProtection == null ? 'unknown' : hasLongerDatedProtection ? 'yes' : 'no'}, total_long_puts_after_sale=${remainingLongPuts == null ? 'unknown' : fmt(remainingLongPuts, 4)}.`,
      `- Roll confirmation rule: confirm when DTE <= ${PUT_ROLL_DTE_THRESHOLD}, this closes an owned long put, and later-expiry long puts at the same or higher strike cover at least the quantity sold. Do not apply monetize_tail_win tranche_fraction/profit-threshold rules to roll_protection; a full close of the aging instrument and negative PnL are allowed roll facts.`,
      '- Reject roll_protection only for DTE above the roll window, missing longer-dated protection, missing/unsafe executable price, non-reduce-only/non-closeable execution, or a sale that would remove all book downside protection.',
    ].join('\n');
  }

  return [
    ...baseLines,
    `- Tail-win discipline: planned_sell_amount=${fmt(plannedAmount, 4)} of position_amount=${fmt(positionAmount, 4)}, tranche_fraction=${fmt(trancheFraction, 4)}, retain_downside_protection=${retainsProtection == null ? 'unknown' : retainsProtection ? 'yes' : 'no'}, total_long_puts_after_sale=${remainingLongPuts == null ? 'unknown' : fmt(remainingLongPuts, 4)}.`,
    `- Advisor exit floor: ${fmtPrice(advisorFloor)}. If the visible bid is below this floor in a sparse market, use a patient synthetic reduce-only gtc/post_only limit at or above the floor for monetize_tail_win; zero fill is better than dumping into a thin bid.`,
    `- monetize_tail_win requires current executable or fair-value PnL proof > ${PUT_MONETIZATION_PROFIT_THRESHOLD}% and tranching while retaining protection; the patient floor is price discipline, not trigger proof. Confirm only if selling this chunk is wiser than keeping convexity.`,
  ].join('\n');
};

const getConfirmationScopePrompt = () => 'CONFIRMATION SCOPE: This is a last-mile execution check, not a second scheduled advisory. Treat the active rule and trigger details as the strategic intent. Confirm only if fresh execution facts still satisfy the typed rule, the limit/order type can respect that intent, and hard safety checks pass. Reject for stale or moved-market facts that invalidate the rule, missing live pricing, margin/liquidation danger, reduce-only violations, or action-specific discipline failures; do not invent a new strategy thesis at confirmation time.';

const getOpenRestingEntryOrders = () => {
  if (!db) return [];
  return db.getOpenRestingOrders().filter(order => order.action === 'buy_put' || order.action === 'sell_call');
};

const hasPendingOrConfirmedActionForRule = (ruleId) => {
  if (!db || !ruleId) return false;
  if (typeof db.hasPendingOrConfirmedActionForRule === 'function') {
    return db.hasPendingOrConfirmedActionForRule(ruleId);
  }
  return db.getRecentPendingActions(200).some((action) =>
    Number(action?.rule_id) === Number(ruleId)
    && ['pending', 'confirmed'].includes(action?.status)
  );
};

const getOpenRestingExitOrders = () => {
  if (!db) return [];
  return db.getOpenRestingOrders().filter(order => order.action === 'buyback_call' || order.action === 'sell_put');
};

const inferActionFromOpenOrder = (order, trackedOrder = null) => {
  const explicitAction = trackedOrder?.action || order?.action;
  if (explicitAction) return explicitAction;
  const instrumentName = order?.instrument_name || trackedOrder?.instrument_name || '';
  const direction = String(order?.direction || trackedOrder?.direction || '').toLowerCase();
  if (instrumentName.endsWith('-C')) {
    if (direction === 'sell') return 'sell_call';
    if (direction === 'buy') return 'buyback_call';
  }
  if (instrumentName.endsWith('-P')) {
    if (direction === 'buy') return 'buy_put';
    if (direction === 'sell') return 'sell_put';
  }
  return null;
};

const getBlockingRestingOrderForEntryCandidate = (action, candidate, price, restingOrders = null) => {
  if (!db || !candidate?.name) return null;
  const sameInstrumentOrders = (restingOrders || db.getOpenRestingOrders())
    .filter(order => order.instrument_name === candidate.name);
  if (sameInstrumentOrders.length === 0) return null;

  if (action === 'sell_call') {
    const sellPrice = Number(price);
    const step = getInstrumentPriceStep(candidate.instrument, sellPrice);
    const lowestSafeSell = (order) => normalizePriceToStep(Number(order.limit_price || 0) + step, step, 'up');
    const blockingOrder = sameInstrumentOrders.find((order) => {
      if (order.action !== 'buyback_call') return true;
      const buybackPrice = Number(order.limit_price || 0);
      return !(sellPrice > 0 && buybackPrice > 0 && sellPrice >= lowestSafeSell(order));
    });
    return blockingOrder || null;
  }

  return sameInstrumentOrders[0] || null;
};

const validateRestingSellCallEntryOrder = ({
  order,
  activeRules = [],
  instruments = [],
  tickerMap = {},
  marginState = null,
  positions = [],
  spotPrice = 0,
}) => {
  const instrumentName = order?.instrument_name;
  if (!instrumentName) return { valid: true, unchecked: true, reason: 'missing instrument name' };

  const sellCallRules = (activeRules || []).filter((rule) =>
    rule?.rule_type === 'entry' && rule?.action === 'sell_call'
  );
  if (sellCallRules.length === 0) return { valid: false, reason: 'no active sell_call entry rule' };

  const instrument = (instruments || []).find((item) => item.instrument_name === instrumentName);
  const ticker = tickerMap?.[instrumentName];
  if (!instrument || !ticker) {
    return { valid: true, unchecked: true, reason: 'live instrument/ticker unavailable' };
  }

  const orderLimitPrice = Number(order?.limit_price || 0);
  if (!(orderLimitPrice > 0)) return { valid: false, reason: 'missing order limit price' };

  const dte = computeDteFromInstrumentName(instrumentName);
  if (!Number.isFinite(dte)) return { valid: false, reason: 'unable to compute DTE' };

  const delta = Number(ticker?.option_pricing?.d) || 0;
  const absDelta = Math.abs(delta);
  const strike = Number(instrument?.option_details?.strike || instrumentName.split('-')?.[2] || 0) || 0;
  const score = absDelta > 0 ? orderLimitPrice / absDelta : 0;
  const research = buildSellCallEdge({ score, dte });
  const edgeScore = Number(research?.selection_score || 0);
  const reasons = [];

  for (const rule of sellCallRules) {
    let criteria;
    try { criteria = typeof rule.criteria === 'string' ? JSON.parse(rule.criteria) : rule.criteria; } catch { criteria = null; }
    if (!criteria || typeof criteria !== 'object') {
      reasons.push(`rule ${rule.id}: malformed criteria`);
      continue;
    }

    if (criteria.option_type && criteria.option_type !== 'C') {
      reasons.push(`rule ${rule.id}: option_type ${criteria.option_type} is not C`);
      continue;
    }

    const dteRange = criteria.dte_range;
    if (Array.isArray(dteRange) && (dte < dteRange[0] || dte > dteRange[1])) {
      reasons.push(`rule ${rule.id}: dte ${dte.toFixed(2)} outside ${JSON.stringify(dteRange)}`);
      continue;
    }
    if (!isSellCallCandidateInStrategyRange(dte, delta)) {
      reasons.push(`rule ${rule.id}: outside sell-call strategy range (dte=${dte.toFixed(2)}, delta=${delta.toFixed(4)})`);
      continue;
    }

    const deltaRange = criteria.delta_range;
    if (Array.isArray(deltaRange) && (delta < deltaRange[0] || delta > deltaRange[1])) {
      reasons.push(`rule ${rule.id}: delta ${delta.toFixed(4)} outside ${JSON.stringify(deltaRange)}`);
      continue;
    }

    const maxStrikePct = Number(criteria.max_strike_pct || 0);
    if (maxStrikePct > 0 && Number(spotPrice) > 0 && strike >= maxStrikePct * Number(spotPrice)) {
      reasons.push(`rule ${rule.id}: strike ${strike} outside max_strike_pct ${maxStrikePct}`);
      continue;
    }

    const marketConditions = criteria.market_conditions || null;
    const hasMarketConditions = Array.isArray(marketConditions)
      ? marketConditions.length > 0
      : Boolean(marketConditions);
    if (hasMarketConditions && !evaluateConditions(marketConditions, 'all', { spot_price: spotPrice })) {
      reasons.push(`rule ${rule.id}: market_conditions no longer satisfied`);
      continue;
    }

    const minBid = Number(criteria.min_bid ?? 0);
    if (minBid > 0 && orderLimitPrice < minBid) {
      reasons.push(`rule ${rule.id}: order limit $${orderLimitPrice.toFixed(4)} below min_bid $${minBid.toFixed(4)}`);
      continue;
    }

    const configuredMinScore = Number(criteria.min_score ?? 0);
    const minScore = configuredMinScore > 0 ? configuredMinScore : SELL_CALL_FALLBACK_MIN_SCORE;
    if (edgeScore + 1e-9 < minScore) {
      reasons.push(`rule ${rule.id}: order CALL EDGE ${edgeScore.toFixed(2)} below min_score ${minScore.toFixed(2)}`);
      continue;
    }

    if (marginState) {
      const currentUtilization = estimateDisplayedMarginUtilization(marginState);
      const effectiveCapPct = getEffectiveCallExposureCapPct(positions, spotPrice);
      if (currentUtilization != null && currentUtilization > effectiveCapPct + 1e-9) {
        reasons.push(`rule ${rule.id}: margin utilization ${(currentUtilization * 100).toFixed(2)}% above entry cap ${(effectiveCapPct * 100).toFixed(2)}%`);
        continue;
      }
    }

    return {
      valid: true,
      ruleId: rule.id,
      score: edgeScore,
      rawScore: score,
      selectionScore: edgeScore,
      delta,
      dte,
      orderLimitPrice,
      reason: `rule ${rule.id} still satisfied (raw_score=${score.toFixed(2)}, CALL_EDGE=${edgeScore.toFixed(2)}, delta=${delta.toFixed(4)}, dte=${dte.toFixed(2)})`,
    };
  }

  return {
    valid: false,
    score: edgeScore,
    rawScore: score,
    selectionScore: edgeScore,
    delta,
    dte,
    orderLimitPrice,
    reason: reasons.join('; ') || 'no active sell_call entry rule matched order',
  };
};

const validateRestingBuyPutEntryOrder = ({
  order,
  activeRules = [],
  instruments = [],
  tickerMap = {},
  spotPrice = 0,
  buyPutContext = null,
  putBudgetRemaining = null,
}) => {
  const instrumentName = order?.instrument_name;
  if (!instrumentName) return { valid: true, unchecked: true, reason: 'missing instrument name' };

  const buyPutRules = (activeRules || []).filter((rule) =>
    rule?.rule_type === 'entry' && rule?.action === 'buy_put'
  );
  if (buyPutRules.length === 0) return { valid: false, reason: 'no active buy_put entry rule' };

  const instrument = (instruments || []).find((item) => item.instrument_name === instrumentName);
  const ticker = tickerMap?.[instrumentName];
  if (!instrument || !ticker) {
    return { valid: true, unchecked: true, reason: 'live instrument/ticker unavailable' };
  }

  const orderLimitPrice = Number(order?.limit_price || 0);
  if (!(orderLimitPrice > 0)) return { valid: false, reason: 'missing order limit price' };

  const dte = computeDteFromInstrumentName(instrumentName);
  if (!Number.isFinite(dte)) return { valid: false, reason: 'unable to compute DTE' };

  const delta = Number(ticker?.option_pricing?.d) || 0;
  const absDelta = Math.abs(delta);
  const strike = Number(instrument?.option_details?.strike || instrumentName.split('-')?.[2] || 0) || 0;
  const orderLimitRawScore = absDelta > 0 ? absDelta / orderLimitPrice : 0;
  const orderLimitScore = normalizeBuyPutScore(orderLimitRawScore, dte);
  const orderAmount = Math.max(0, Number(order?.amount || 0) - Number(order?.filled_amount || 0));
  const orderValue = orderLimitPrice * orderAmount;
  const reasons = [];

  for (const rule of buyPutRules) {
    let criteria;
    try { criteria = typeof rule.criteria === 'string' ? JSON.parse(rule.criteria) : rule.criteria; } catch { criteria = null; }
    if (!criteria || typeof criteria !== 'object') {
      reasons.push(`rule ${rule.id}: malformed criteria`);
      continue;
    }

    if (criteria.option_type && criteria.option_type !== 'P') {
      reasons.push(`rule ${rule.id}: option_type ${criteria.option_type} is not P`);
      continue;
    }

    const dteRange = criteria.dte_range;
    if (Array.isArray(dteRange) && (dte < dteRange[0] || dte > dteRange[1])) {
      reasons.push(`rule ${rule.id}: dte ${dte.toFixed(2)} outside ${JSON.stringify(dteRange)}`);
      continue;
    }
    if (dte < BUY_PUT_ADVISORY_DTE_RANGE[0] || dte > BUY_PUT_ADVISORY_DTE_RANGE[1]) {
      reasons.push(`rule ${rule.id}: outside buy-put strategy DTE range (dte=${dte.toFixed(2)})`);
      continue;
    }

    const deltaRange = criteria.delta_range;
    if (Array.isArray(deltaRange) && (delta < deltaRange[0] || delta > deltaRange[1])) {
      reasons.push(`rule ${rule.id}: delta ${delta.toFixed(4)} outside ${JSON.stringify(deltaRange)}`);
      continue;
    }
    if (delta < PUT_DELTA_RANGE[0] || delta > PUT_DELTA_RANGE[1]) {
      reasons.push(`rule ${rule.id}: outside buy-put strategy delta range (delta=${delta.toFixed(4)})`);
      continue;
    }

    const maxStrikePct = Number(criteria.max_strike_pct || 0);
    if (maxStrikePct > 0 && Number(spotPrice) > 0 && strike >= maxStrikePct * Number(spotPrice)) {
      reasons.push(`rule ${rule.id}: strike ${strike} outside max_strike_pct ${maxStrikePct}`);
      continue;
    }

    const marketConditions = criteria.market_conditions || null;
    const hasMarketConditions = Array.isArray(marketConditions)
      ? marketConditions.length > 0
      : Boolean(marketConditions);
    if (hasMarketConditions && !evaluateConditions(marketConditions, 'all', { spot_price: spotPrice })) {
      reasons.push(`rule ${rule.id}: market_conditions no longer satisfied`);
      continue;
    }

    const rawValueSignal = criteria.value_signal ?? criteria.buy_put_signal;
    const valueSignal = normalizeBuyPutValueSignal(rawValueSignal);
    if (hasExplicitBuyPutValueSignal(rawValueSignal) && !valueSignal) {
      reasons.push(`rule ${rule.id}: unknown value_signal=${rawValueSignal}`);
      continue;
    }

    const minScore = Number(criteria.min_score ?? 0);
    if (minScore > 0 && orderLimitScore + 1e-12 < minScore) {
      reasons.push(`rule ${rule.id}: order PUT EDGE ${orderLimitScore.toFixed(6)} below min_score ${minScore.toFixed(6)}`);
      continue;
    }

    let targetScore = Number(criteria.target_score ?? 0) > 0 ? Number(criteria.target_score) : null;
    const contextTargetScore = Number(buyPutContext?.action_pressure?.target_score || 0);
    const contextSignal = buyPutContext?.action_pressure?.signal || null;
    if (isActionableBuyPutSignal(contextSignal) && contextTargetScore > 0 && (targetScore == null || targetScore > contextTargetScore)) {
      targetScore = contextTargetScore;
    }
    if (targetScore != null && orderLimitScore + 1e-12 < targetScore) {
      reasons.push(`rule ${rule.id}: order PUT EDGE ${orderLimitScore.toFixed(6)} below target_score ${targetScore.toFixed(6)}`);
      continue;
    }

    const ruleBudget = Number(rule.budget_limit || 0);
    if (ruleBudget > 0 && orderValue > ruleBudget + 0.01) {
      reasons.push(`rule ${rule.id}: order value $${orderValue.toFixed(2)} above rule budget $${ruleBudget.toFixed(2)}`);
      continue;
    }
    if (botData.putBudgetForCycle > 0) {
      const remaining = Number(putBudgetRemaining);
      if (!(remaining > 0.20)) {
        reasons.push(`rule ${rule.id}: put budget exhausted`);
        continue;
      }
      if (orderValue > remaining + 0.01) {
        reasons.push(`rule ${rule.id}: order value $${orderValue.toFixed(2)} above remaining put budget $${remaining.toFixed(2)}`);
        continue;
      }
    }

    return {
      valid: true,
      ruleId: rule.id,
      score: orderLimitScore,
      rawScore: orderLimitRawScore,
      delta,
      dte,
      orderLimitPrice,
      reason: `rule ${rule.id} still satisfied (raw_score=${orderLimitRawScore.toFixed(6)}, PUT_EDGE=${orderLimitScore.toFixed(6)}, delta=${delta.toFixed(4)}, dte=${dte.toFixed(2)})`,
    };
  }

  return {
    valid: false,
    score: orderLimitScore,
    rawScore: orderLimitRawScore,
    delta,
    dte,
    orderLimitPrice,
    reason: reasons.join('; ') || 'no active buy_put entry rule matched order',
  };
};

const findRestingExitOrderForRule = (restingOrders, rule) => {
  if (!Array.isArray(restingOrders) || !rule) return null;
  return restingOrders.find(order =>
    order?.instrument_name === rule.instrument_name
    && order?.action === rule.action
  ) || null;
};

const isRestingOrderType = (orderType) => orderType === 'gtc' || orderType === 'post_only';

const getCloseablePositionForExit = (action, instrumentName, positions = []) => {
  const position = (positions || []).find(item => item?.instrument_name === instrumentName);
  if (!position) return null;
  if (action === 'buyback_call' && position.direction === 'short') return position;
  if (action === 'sell_put' && position.direction === 'long') return position;
  return null;
};

const getSyntheticExitIntent = (action, triggerData = {}, ruleCriteria = {}) => {
  if (action === 'buyback_call') {
    return triggerData?.buyback_intent || getBuybackIntent(ruleCriteria);
  }
  if (action === 'sell_put') {
    return triggerData?.put_exit_intent || getPutExitIntent(ruleCriteria);
  }
  return null;
};

const isSyntheticRestingExitIntentAllowed = (action, triggerData = {}, ruleCriteria = {}) => {
  const intent = getSyntheticExitIntent(action, triggerData, ruleCriteria);
  return (action === 'buyback_call' && intent === 'profit_capture')
    || (action === 'sell_put' && intent === 'monetize_tail_win');
};

const getExistingRestingExitOrder = (action, instrumentName, restingOrders = []) => {
  const closeDirection = getActionPolicy(action)?.direction;
  return (restingOrders || []).find(order =>
    !['cancelled', 'filled', 'expired', 'rejected'].includes(String(order?.status || order?.order_status || '').toLowerCase())
    && order?.instrument_name === instrumentName
    && (
      order?.action === action
      || (!order?.action && closeDirection && String(order?.direction || '').toLowerCase() === closeDirection)
    )
  ) || null;
};

const getSyntheticReduceOnlyPreflight = ({ action, instrumentName, amount, orderType, triggerData = {}, ruleCriteria = {}, positions = [], restingOrders = [] }) => {
  if (!isReduceOnlyExitAction(action)) {
    return { allowed: true, reduceOnly: false, amount: Number(amount) || 0, synthetic: false, reason: 'entry_action' };
  }

  if (!isRestingOrderType(orderType)) {
    return { allowed: true, reduceOnly: true, amount: Number(amount) || 0, synthetic: false, reason: 'venue_reduce_only_non_resting' };
  }

  if (!isSyntheticRestingExitIntentAllowed(action, triggerData, ruleCriteria)) {
    return { allowed: false, reason: `resting synthetic exit not allowed for ${action} intent ${getSyntheticExitIntent(action, triggerData, ruleCriteria) || 'unknown'}` };
  }

  const closeablePosition = getCloseablePositionForExit(action, instrumentName, positions);
  if (!closeablePosition) {
    return { allowed: false, reason: `no live closeable position for synthetic ${action} ${instrumentName}` };
  }

  const existingResting = getExistingRestingExitOrder(action, instrumentName, restingOrders);
  if (existingResting) {
    return { allowed: false, reason: `existing resting synthetic exit ${existingResting.order_id || 'unknown'} already open for ${instrumentName}` };
  }

  const closeableAmount = Math.max(0, Number(closeablePosition.amount) || 0);
  const requestedAmount = Math.max(0, Number(amount) || 0);
  const cappedAmount = floorOrderAmountToVenuePrecision(Math.min(requestedAmount, closeableAmount));
  if (!(cappedAmount > 0)) {
    return { allowed: false, reason: `synthetic ${action} amount is zero after closeable cap` };
  }

  const capNote = cappedAmount + 1e-9 < requestedAmount
    ? `capped requested ${requestedAmount.toFixed(4)} to live closeable ${cappedAmount.toFixed(4)}`
    : `live closeable ${closeableAmount.toFixed(4)} covers requested ${requestedAmount.toFixed(4)}`;
  return {
    allowed: true,
    reduceOnly: false,
    amount: cappedAmount,
    synthetic: true,
    reason: `synthetic_reduce_only_resting_exit: ${capNote}`,
    closeableAmount,
    originalAmount: requestedAmount,
  };
};

const summarizeReservedEntryCapacity = (restingOrders) => {
  return restingOrders.reduce((acc, order) => {
    if (order.action === 'buy_put') {
      acc.putBudget += Math.max(0, (Number(order.amount) || 0) - (Number(order.filled_amount) || 0)) * (Number(order.limit_price) || 0);
    }
    return acc;
  }, { putBudget: 0 });
};

const getMarginCapacityBase = (marginState) => {
  const collateralMarginBase = Number(marginState?.collaterals_initial_margin ?? 0);
  if (collateralMarginBase > 0) return collateralMarginBase;
  const collateralValue = Number(marginState?.collaterals_value ?? 0);
  if (collateralValue > 0) return collateralValue;
  return Number(marginState?.subaccount_value ?? 0);
};

const getMarginUtilizationBase = (marginState) => {
  const aggregatedMaintenanceBase = Math.abs(Number(marginState?.aggregated_collaterals_maintenance_margin ?? 0));
  if (aggregatedMaintenanceBase > 0) return aggregatedMaintenanceBase;
  const maintenanceBase = Math.abs(Number(marginState?.collaterals_maintenance_margin ?? 0));
  if (maintenanceBase > 0) return maintenanceBase;
  return getMarginCapacityBase(marginState);
};

const normalizeMarginUtilizationValue = (value) => {
  if (!Number.isFinite(value)) return null;
  return Math.max(0, Math.min(1, value));
};

const estimateMarginUtilizationFromComponents = (marginState, additionalOpenOrdersMargin = 0) => {
  const base = getMarginUtilizationBase(marginState);
  if (!(base > 0)) return null;
  const usedMargin = Math.abs(Number(
      marginState?.aggregated_positions_initial_margin ??
      marginState?.positions_initial_margin ??
      0
    ))
    + Math.abs(Number(marginState?.open_orders_margin ?? 0))
    + Math.max(0, Number(additionalOpenOrdersMargin ?? 0));
  return normalizeMarginUtilizationValue(usedMargin / base);
};

const estimateMarginUtilization = (marginState, additionalOpenOrdersMargin = 0) => {
  const componentUtilization = estimateMarginUtilizationFromComponents(marginState, additionalOpenOrdersMargin);
  if (componentUtilization != null) return componentUtilization;

  const base = getMarginCapacityBase(marginState);
  if (!(base > 0)) return null;
  const availableInitialMargin = Number(marginState?.initial_margin ?? NaN);
  if (Number.isFinite(availableInitialMargin)) {
    const projectedAvailable = availableInitialMargin - Math.max(0, Number(additionalOpenOrdersMargin ?? 0));
    return normalizeMarginUtilizationValue(1 - (projectedAvailable / base));
  }

  const explicitMarginUsage = Number(
    marginState?.margin_usage_pct ??
    marginState?.margin_utilization_pct ??
    marginState?.margin_utilization ??
    NaN
  );
  const additionalRatio = Math.max(0, Number(additionalOpenOrdersMargin ?? 0)) / base;
  if (Number.isFinite(explicitMarginUsage)) {
    const normalized = explicitMarginUsage > 1 ? explicitMarginUsage / 100 : explicitMarginUsage;
    return normalizeMarginUtilizationValue(normalized + additionalRatio);
  }

  return estimateMarginUtilizationFromComponents(marginState, additionalOpenOrdersMargin);
};

const estimateDisplayedMarginUtilization = (marginState) => {
  if (!marginState) return null;
  const maintenanceBase = getMarginUtilizationBase(marginState);
  const maintenanceMargin = Number(marginState?.maintenance_margin ?? NaN);
  if (maintenanceBase > 0 && Number.isFinite(maintenanceMargin)) {
    return normalizeMarginUtilizationValue(1 - (maintenanceMargin / maintenanceBase));
  }
  return estimateMarginUtilization(marginState);
};

const estimateProjectedDisplayedMarginUtilization = (marginState, additionalMargin = 0) => {
  if (!marginState) return null;
  const currentDisplayed = estimateDisplayedMarginUtilization(marginState);
  const maintenanceBase = getMarginUtilizationBase(marginState);
  if (currentDisplayed != null && maintenanceBase > 0) {
    return normalizeMarginUtilizationValue(currentDisplayed + (Math.max(0, Number(additionalMargin ?? 0)) / maintenanceBase));
  }
  return estimateMarginUtilization(marginState, additionalMargin);
};

const getShortCallExposure = (positions = []) => positions
  .filter((p) => p.instrument_name?.endsWith('-C') && p.direction === 'short')
  .reduce((sum, p) => sum + Math.abs(Number(p.amount) || 0), 0);

const isCallBreakoutAddWindow = (positions = [], spotPrice = 0) => {
  const currentShortExposure = getShortCallExposure(positions);
  if (!(currentShortExposure > 0) || !(spotPrice > 0)) return false;

  const short = botData.shortTermMomentum || {};
  const medium = botData.mediumTermMomentum || {};
  const threeDayHigh = Number(short.threeDayHigh || 0);
  const sevenDayHigh = Number(short.sevenDayHigh || 0);
  const nearRecentHigh = (threeDayHigh > 0 && spotPrice >= threeDayHigh * 0.997)
    || (sevenDayHigh > 0 && spotPrice >= sevenDayHigh * 0.995);

  return short.main === 'upward'
    && CALL_BREAKOUT_DERIVATIVES.has(short.derivative)
    && medium.main !== 'downward'
    && nearRecentHigh;
};

const getEffectiveCallExposureCapPct = (positions = [], spotPrice = 0) => (
  isCallBreakoutAddWindow(positions, spotPrice)
    ? CALL_BREAKOUT_OVERRIDE_CAP_PCT
    : CALL_EXPOSURE_CAP_PCT
);

const getEffectiveCallExposureLimitPct = (positions = [], spotPrice = 0) => (
  getCallExposureLimitPct(getEffectiveCallExposureCapPct(positions, spotPrice))
);

const getDisplayedMarginHeadroomAtCap = (marginState, capPct = CALL_EXPOSURE_CAP_PCT) => {
  if (!marginState) return null;
  const currentDisplayed = estimateDisplayedMarginUtilization(marginState);
  const utilizationBase = getMarginUtilizationBase(marginState);
  if (currentDisplayed == null || !(utilizationBase > 0)) return null;
  return Math.max(0, (capPct - currentDisplayed) * utilizationBase);
};

const estimateStandardShortCallInitialMarginPerUnit = (strike, spotPrice, premium) => {
  if (!(spotPrice > 0)) return Infinity;
  const otm = Math.max(0, strike - spotPrice);
  const otmBuffer = Math.max(0.15 - (otm / spotPrice), 0.13) * spotPrice;
  // Derive Standard Margin docs express short-call initial margin as collateral credit plus
  // a negative option-margin term. For sizing we use the positive requirement magnitude.
  return Math.max(0, otmBuffer - Math.max(0, premium || 0));
};

const estimateShortCallMarginPerUnit = (marginState, positions, restingOrders, spotPrice, strike = 0, premium = 0) => {
  const shortCallPositions = positions.filter(p => p.instrument_name?.endsWith('-C') && p.direction === 'short');
  const currentShortExposure = shortCallPositions.reduce((sum, p) => sum + Math.abs(Number(p.amount) || 0), 0);
  const empiricalPositionsMargin = Math.abs(Number(
    marginState?.aggregated_positions_initial_margin ??
    marginState?.positions_initial_margin ??
    0
  ));
  if (currentShortExposure > 0 && empiricalPositionsMargin > 0) {
    return empiricalPositionsMargin / currentShortExposure;
  }

  const restingShortExposure = restingOrders
    .filter(order => order.action === 'sell_call')
    .reduce((sum, order) => sum + Math.max(0, Number(order.amount || 0) - Number(order.filled_amount || 0)), 0);
  if (restingShortExposure > 0 && Number(marginState?.open_orders_margin ?? 0) > 0) {
    return Number(marginState.open_orders_margin) / restingShortExposure;
  }

  const documentedEstimate = estimateStandardShortCallInitialMarginPerUnit(strike, spotPrice, premium);
  if (Number.isFinite(documentedEstimate) && documentedEstimate > 0) {
    return documentedEstimate;
  }

  return Math.max((spotPrice || 0) * 0.13, 100);
};

const getCallMarginDecision = (action, marginState, positions, restingOrders, instruments, spotPrice, instrumentName, amount, limitPrice) => {
  if (action !== 'sell_call') return { applicable: false, available: false };
  if (!marginState) return { applicable: true, available: false };
  const effectiveCapPct = getEffectiveCallExposureCapPct(positions, spotPrice);
  const breakoutOverrideActive = effectiveCapPct > CALL_EXPOSURE_CAP_PCT;
  const currentUtilization = estimateDisplayedMarginUtilization(marginState);
  const instrument = instruments.find((item) => item.instrument_name === instrumentName);
  const strike = Number(instrument?.option_details?.strike || instrumentName?.split('-')?.[2] || 0) || 0;
  const normalizedAmount = Math.max(0, Number(amount || 0));
  const normalizedLimitPrice = Number(limitPrice || 0);
  const marginPerUnit = estimateShortCallMarginPerUnit(marginState, positions, restingOrders, spotPrice, strike, normalizedLimitPrice);
  const additionalMargin = normalizedAmount * marginPerUnit;
  const projectedUtilization = estimateProjectedDisplayedMarginUtilization(marginState, additionalMargin);
  const capPct = effectiveCapPct * 100;
  const limitPct = getCallExposureLimitPct(effectiveCapPct) * 100;
  const baseCapPct = CALL_EXPOSURE_CAP_PCT * 100;
  const baseLimitPct = CALL_EXPOSURE_LIMIT_PCT * 100;
  const entryCapPct = CALL_ENTRY_CAP_PCT * 100;
  const bufferPct = CALL_EXPOSURE_BUFFER_PCT * 100;
  const entryCapSatisfied = projectedUtilization != null
    && projectedUtilization <= effectiveCapPct + 1e-9;

  return {
    applicable: true,
    available: projectedUtilization != null,
    currentUtilization,
    projectedUtilization,
    marginPerUnit,
    effectiveCapPct,
    breakoutOverrideActive,
    capPct,
    limitPct,
    baseCapPct,
    baseLimitPct,
    entryCapPct,
    bufferPct,
    entryCapSatisfied,
  };
};

const getCallMarginContext = (action, marginState, positions, restingOrders, instruments, spotPrice, instrumentName, amount, limitPrice) => {
  const decision = getCallMarginDecision(
    action,
    marginState,
    positions,
    restingOrders,
    instruments,
    spotPrice,
    instrumentName,
    amount,
    limitPrice
  );
  if (!decision.applicable) return 'Call margin utilization: not applicable for this action.';
  if (!decision.available) return 'Call margin utilization: unavailable (margin state unavailable).';

  const {
    currentUtilization,
    projectedUtilization,
    marginPerUnit,
    breakoutOverrideActive,
    capPct,
    limitPct,
    baseCapPct,
    baseLimitPct,
    entryCapPct,
    bufferPct,
    entryCapSatisfied,
  } = decision;
  return `Call margin utilization: current_derive_display=${currentUtilization != null ? `${(currentUtilization * 100).toFixed(1)}%` : 'N/A'}, projected_after_trade_display=${projectedUtilization != null ? `${(projectedUtilization * 100).toFixed(1)}%` : 'N/A'}, projected_after_trade_exact=${projectedUtilization != null ? `${(projectedUtilization * 100).toFixed(6)}%` : 'N/A'}, per_contract_estimate=$${marginPerUnit.toFixed(2)}, caution_zone=${entryCapPct.toFixed(1)}%-${baseCapPct.toFixed(1)}%, target_cap=${capPct.toFixed(1)}%, active_entry_cap_exact=${capPct.toFixed(6)}%, buffered_limit=${limitPct.toFixed(1)}%, execution_buffer=${bufferPct.toFixed(1)}pp, base_target_cap=${baseCapPct.toFixed(1)}%, base_buffered_limit=${baseLimitPct.toFixed(1)}%, breakout_override=${breakoutOverrideActive ? 'active' : 'inactive'}, entry_cap_satisfied=${entryCapSatisfied ? 'yes' : 'no'}. Treat ${entryCapPct.toFixed(1)}% as a caution threshold and ${capPct.toFixed(1)}% as the active entry cap. The ${bufferPct.toFixed(1)} percentage point buffer is last-mile safety for estimate drift, not planned sell-call capacity. Confirm only when projected utilization stays at or below the active entry cap; at-or-below means <= and equality is allowed. Reject call sells that exceed the active entry cap, exceed the buffered limit, lack buying power, or are too small to matter. Use entry_cap_satisfied as the authoritative margin gate; do not reinterpret a rounded ${capPct.toFixed(1)}% display as a failure when entry_cap_satisfied=yes.`;
};

const evaluateSellCallRetryMargin = async ({ instrumentName, amount, retryPrice, instruments, spotPrice }) => {
  let marginState = null;
  try { marginState = await fetchSubaccount(); } catch { /* ok */ }
  if (!hasUsableMarginState(marginState)) return { allowed: false, reason: 'fresh margin state unavailable' };
  if (marginState.is_under_liquidation) return { allowed: false, reason: 'account under liquidation' };

  let positions = [];
  try { positions = await fetchPositions({ throwOnError: true }); }
  catch { return { allowed: false, reason: 'fresh positions unavailable' }; }
  const restingOrders = db ? db.getOpenRestingOrders() : [];
  const instrument = instruments.find((item) => item.instrument_name === instrumentName);
  const strike = Number(instrument?.option_details?.strike || instrumentName?.split('-')?.[2] || 0) || 0;
  const normalizedAmount = Math.max(0, Number(amount || 0));
  const normalizedRetryPrice = Number(retryPrice || 0);
  const additionalMargin = normalizedAmount * estimateShortCallMarginPerUnit(
    marginState,
    positions,
    restingOrders,
    spotPrice,
    strike,
    normalizedRetryPrice
  );
  const marginBase = getMarginCapacityBase(marginState);
  const buyingPowerHeadroom = Math.max(0, Number(marginState?.initial_margin || 0));
  const currentUtilization = estimateDisplayedMarginUtilization(marginState);
  const projectedUtilization = estimateProjectedDisplayedMarginUtilization(marginState, additionalMargin);
  const effectiveCapPct = getEffectiveCallExposureCapPct(positions, spotPrice);
  const effectiveLimitPct = getCallExposureLimitPct(effectiveCapPct);
  const requiredBuyingPowerAtCap = marginBase > 0 ? Math.max(0, (1 - effectiveCapPct) * marginBase) : 0;
  const targetCapHeadroom = buyingPowerHeadroom - requiredBuyingPowerAtCap;
  const allowed = additionalMargin <= buyingPowerHeadroom
    && projectedUtilization != null
    && projectedUtilization <= effectiveCapPct;
  return {
    allowed,
    reason: `retry_margin=$${additionalMargin.toFixed(2)}, current_display_utilization=${currentUtilization != null ? `${(currentUtilization * 100).toFixed(1)}%` : 'N/A'}, projected_display_utilization=${projectedUtilization != null ? `${(projectedUtilization * 100).toFixed(1)}%` : 'N/A'}, target_cap=${(effectiveCapPct * 100).toFixed(1)}%, buffered_limit=${(effectiveLimitPct * 100).toFixed(1)}%, target_cap_headroom=$${targetCapHeadroom.toFixed(2)}, buying_power=$${buyingPowerHeadroom.toFixed(2)}`,
  };
};

// Resting entries have their own maintenance pass. Global candidate selection
// and new-entry cooldowns must not decide whether their prices get refreshed.
const reassessRestingEntryOrders = async ({ entryRules, instruments, tickerMap, spotPrice,
  positions, marginState, buyPutContext = null, marketContext = null }) => {
  const { buildRestingEntryPlan } = require('./bot/resting-entry-plan');
  const result = { queuedCount: 0, blockedActions: new Set(), plans: new Map() };
  const trackedOrders = getOpenRestingEntryOrders();
  for (const tracked of trackedOrders) {
    let rule = null;
    const record = (plan, pendingActionId = null) => {
      result.plans.set(tracked.order_id, plan);
      logRuleDecisionSafe({ evaluated_at: new Date().toISOString(), rule_id: rule?.id ?? tracked.rule_id,
        rule_type: 'entry', action: tracked.action, instrument_name: tracked.instrument_name,
        selected_instrument: tracked.instrument_name, spot_price: spotPrice,
        decision_status: pendingActionId != null ? 'triggered' : 'skipped',
        reason_code: pendingActionId != null ? 'resting_entry_reprice' : `resting_entry_${plan.decision}`,
        reason: plan.reason, price: plan.desiredOrder?.limit_price ?? tracked.limit_price,
        amount: plan.desiredOrder?.amount ?? null, pending_action_id: pendingActionId,
        criteria_json: rule?.criteria ?? null,
        context_json: { existing_order_id: tracked.order_id, existing_price: tracked.limit_price,
          desired_order: plan.desiredOrder ?? null, differences: plan.differences ?? [] } });
    };
    try {
      const snapshot = await readFreshEntryOrderSnapshot({ action: tracked.action,
        instrument_name: tracked.instrument_name });
      const order = snapshot.orders.find(item => item.order_id === tracked.order_id);
      if (!order) continue; // Terminal fills were reconciled by the fresh snapshot.
      if (snapshot.orders.length !== 1) throw new Error('Multiple same-action entries need reconciliation');
      const ticker = await fetchFreshTickerForInstrument(order.instrument_name);
      if (!ticker) throw new Error('Fresh incumbent quote unavailable');
      const instrument = instruments.find(item => item.instrument_name === order.instrument_name);
      const currentSpot = Number(ticker.I) > 0 ? Number(ticker.I) : spotPrice;
      const otherPutReserved = summarizeReservedEntryCapacity(getOpenRestingEntryOrders()
        .filter(item => item.order_id !== order.order_id)).putBudget;
      const putBudgetRemaining = botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought;
      const applicableRules = entryRules.filter(item => item.action === order.action)
        .sort((a, b) => Number(Number(b.id) === Number(order.rule_id)) - Number(Number(a.id) === Number(order.rule_id)));
      let plan = { decision: 'unresolved', reason: 'No active rule matches the resting entry' };
      let validation = null;
      let research = null;
      for (const candidateRule of applicableRules) {
        rule = candidateRule;
        const candidatePlan = buildRestingEntryPlan({ order, rule, instrument, ticker,
          spotPrice: currentSpot, putBudgetRemaining, otherPutReserved });
        plan = candidatePlan;
        if (!candidatePlan.desiredOrder) continue;
        const desired = candidatePlan.desiredOrder;
        const common = { order: { ...desired, filled_amount: 0 }, activeRules: [rule], instruments,
          tickerMap: { [order.instrument_name]: ticker }, spotPrice: currentSpot, positions, marginState };
        validation = order.action === 'buy_put'
          ? validateRestingBuyPutEntryOrder({ ...common, putBudgetRemaining: putBudgetRemaining - otherPutReserved })
          : validateRestingSellCallEntryOrder(common);
        if (!validation.valid || validation.unchecked) {
          plan = { decision: 'unresolved', reason: validation.reason || 'Current entry rule could not be checked' };
          continue;
        }
        if (order.action === 'buy_put') {
          research = classifyBuyPutEdge({ score: validation.score, rawScore: validation.rawScore,
            dte: validation.dte, strike: Number(instrument?.option_details?.strike), spotPrice: currentSpot,
            entryPrice: desired.limit_price, askPrice: Number(ticker.a), bidPrice: Number(ticker.b),
            askAmount: Number(ticker.A), bidAmount: Number(ticker.B), markPrice: Number(ticker.M),
            impliedVol: getTickerImpliedVol(ticker), marketContext });
        }
        break;
      }
      if (plan.decision === 'keep') {
        record(plan);
        console.log(`📋 Keep resting ${order.action} ${order.instrument_name}: fresh venue terms aligned @ $${plan.desiredOrder.limit_price} x ${plan.desiredOrder.amount}`);
        continue;
      }
      result.blockedActions.add(order.action);
      if (plan.decision !== 'replace') {
        record(plan);
        console.log(`📋 Defer resting ${order.action} ${order.instrument_name} reassessment: ${plan.reason}`);
        continue;
      }
      const working = [...db.getPendingActions('pending'), ...db.getPendingActions('confirmed')];
      if (working.some(item => item.action === order.action)) {
        record({ ...plan, decision: 'unresolved', reason: 'Entry review already pending; current order retained' });
        continue;
      }
      const rejection = getRecentRejectedAction(order.action, order.instrument_name);
      if (rejection) {
        record({ ...plan, decision: 'unresolved', reason: `Recent replacement rejection: ${rejection.reason || 'review backoff'}` });
        continue;
      }
      const desired = plan.desiredOrder;
      const criteria = parseMaybeJsonObject(rule.criteria);
      const pending = db.insertPendingAction({ rule_id: rule.id, action: order.action,
        instrument_name: order.instrument_name, amount: desired.amount, price: desired.limit_price,
        trigger_details: { entry_replacement: true, replacement_order_id: order.order_id,
          replacement_order_snapshot: order, desired_entry_order: desired,
          entry_approved_notional: plan.approvedNotional, advisor_limit_price: desired.limit_price,
          preferred_order_type: desired.order_type, delta: validation.delta, dte: validation.dte,
          strike: Number(instrument.option_details?.strike), score: validation.score, raw_score: validation.rawScore,
          planned_score: validation.score, selection_score: research?.selection_score ?? validation.score,
          selection_metric: order.action === 'buy_put' ? 'put_edge_v1' : 'call_edge_v1',
          selection_price_basis: 'planned_limit',
          target_score: plan.requiredScore, buy_put_research: research,
          buy_put_signal: order.action === 'buy_put' ? buyPutContext?.action_pressure?.signal || 'standing_patient_bid' : null,
          required_value_signal: criteria.value_signal ?? criteria.buy_put_signal ?? null,
          price_source: 'resting_entry_reassessment', observed_at: snapshot.observedAt } });
      result.queuedCount++;
      record(plan, pending?.lastInsertRowid ?? null);
      console.log(`📋 Review resting ${order.action} ${order.instrument_name}: $${order.limit_price} -> $${desired.limit_price} x ${desired.amount}; current order retained until approval`);
    } catch (error) {
      result.blockedActions.add(tracked.action);
      record({ decision: 'unresolved', reason: error.message });
      console.log(`📋 Defer resting ${tracked.action} ${tracked.instrument_name} reassessment: ${error.message}`);
    }
  }
  return result;
};

const evaluateTradingRules = async (positions, instruments, tickerMap, spotPrice) => {
  let triggeredCount = 0;
  const evaluationTimestamp = new Date().toISOString();
  const logDecision = (rule, decision) => logRuleDecisionSafe({
    evaluated_at: evaluationTimestamp,
    rule_id: rule?.id ?? null,
    rule_type: rule?.rule_type ?? null,
    action: rule?.action ?? null,
    instrument_name: rule?.instrument_name ?? null,
    spot_price: spotPrice,
    ...decision,
  });

  // ── Exit rules ─────────────────────────────────────────────────────────────
  try {
    const exitRules = db.getActiveRulesByType('exit');
    for (const rule of exitRules) {
      try {
        const position = positions.find(p => p.instrument_name === rule.instrument_name);
        if (!position) {
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'no_position',
            reason: 'No live position matched exit rule instrument',
          });
          continue;
        }

        const ticker = tickerMap[rule.instrument_name];
        const values = getRuleEvaluationValues(position, ticker, spotPrice, rule.action);

        let criteria;
        try { criteria = typeof rule.criteria === 'string' ? JSON.parse(rule.criteria) : rule.criteria; } catch { criteria = null; }
        if (!criteria || typeof criteria !== 'object' || !Array.isArray(criteria.conditions)) {
          console.log(`📋 Exit rule ${rule.id}: skipping — criteria missing structured conditions`);
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'malformed_criteria',
            reason: 'Exit criteria missing structured conditions',
            criteria_json: criteria || rule.criteria || null,
          });
          continue;
        }
        const instrument = instruments.find(item => item.instrument_name === rule.instrument_name);
        const patientBuybackPlan = refinePatientBuybackPlanPrice(
          getPatientBuybackPlan(rule, criteria, position),
          ticker,
          instrument
        );
        const patientSellPutPlan = getPatientSellPutPlan(rule, criteria, position, values);
        const plannedValues = {
          ...values,
          ...(patientBuybackPlan ? {
            patient_buyback_capture_pct: patientBuybackPlan.capturePct,
            patient_buyback_limit_price: patientBuybackPlan.limitPrice,
            patient_buyback_ceiling_price: patientBuybackPlan.ceilingPrice,
          } : {}),
          ...(patientSellPutPlan ? {
            patient_sell_put_pnl_pct: patientSellPutPlan.pnlPct,
            patient_sell_put_limit_price: patientSellPutPlan.limitPrice,
            patient_sell_put_fair_value_pnl_pct: patientSellPutPlan.fairValuePnlPct,
            patient_sell_put_fair_value_price: patientSellPutPlan.fairValuePrice,
            patient_sell_put_fair_value_source: patientSellPutPlan.fairValueSource,
          } : {}),
        };
        const triggered = evaluateExitConditions(
          criteria,
          values,
          patientBuybackPlan?.capturePct ?? patientSellPutPlan?.pnlPct ?? null
        );
        if (!triggered) {
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'conditions_not_met',
            reason: 'Exit conditions did not trigger',
            criteria_json: criteria,
            context_json: { current_values: plannedValues },
          });
          continue;
        }
        const plannedSellPutAmount = rule.action === 'sell_put'
          ? getSellPutExitAmount(rule, criteria, position, plannedValues)
          : null;

        const buybackGate = getBuybackCaptureGate(rule, criteria, plannedValues);
        if (!buybackGate.allowed) {
          console.log(`📋 Exit skip: ${rule.action} ${rule.instrument_name} — ${buybackGate.reason}`);
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'buyback_gate',
            reason: buybackGate.reason,
            criteria_json: criteria,
            context_json: { current_values: plannedValues },
          });
          continue;
        }
        const putExitGate = getSellPutProtectionGate(rule, plannedValues, {
          criteria,
          position,
          positions,
          plannedSellAmount: plannedSellPutAmount,
        });
        if (!putExitGate.allowed) {
          console.log(`📋 Exit skip: ${rule.action} ${rule.instrument_name} — ${putExitGate.reason}`);
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'put_exit_gate',
            reason: putExitGate.reason,
            criteria_json: criteria,
            context_json: { current_values: plannedValues },
          });
          continue;
        }

        // Dedup: skip if there's already a pending/confirmed action for this rule
        if (hasPendingOrConfirmedActionForRule(rule.id)) {
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'pending_duplicate',
            reason: 'Rule already has pending or confirmed action',
            criteria_json: criteria,
          });
          continue;
        }

        const askPrice = Number(ticker?.a) || 0;
        const bidPrice = Number(ticker?.b) || 0;
        const markPrice = Number(ticker?.M) || values.mark_price || 0;
        const plannedPrice = patientBuybackPlan?.limitPrice
          || patientSellPutPlan?.limitPrice
          || (rule.action.includes('buy') ? askPrice : bidPrice);

        // Skip selling worthless positions — mark < $0.10 means nothing to recover
        if (rule.action === 'sell_put' && markPrice < 0.10) {
          console.log(`📋 Exit skip: ${rule.instrument_name} mark $${markPrice.toFixed(4)} — worthless, let expire`);
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'worthless_position',
            reason: `Mark ${markPrice.toFixed(4)} below exit value floor`,
            price: markPrice,
            amount: plannedSellPutAmount ?? position.amount,
            criteria_json: criteria,
            context_json: { current_values: plannedValues },
          });
          continue;
        }

        // Compare the executable desired order before spending reviewer calls.
        // A different plan becomes a replacement candidate while its existing
        // quote remains live until confirmation and terminal-fill reconciliation.
        const exitSnapshot = await readFreshExitOrderSnapshot({ action: rule.action, instrument_name: rule.instrument_name });
        if (!Array.isArray(exitSnapshot?.orders)) throw new Error('Fresh exit order snapshot unavailable');
        const existingOrders = exitSnapshot.orders;
        if (existingOrders.length > 1) throw new Error('Multiple existing exits require reconciliation');
        const existingRestingExit = existingOrders[0] || null;
        const currentExitPositions = Array.isArray(exitSnapshot.positions)
          ? exitSnapshot.positions : await fetchPositions({ throwOnError: true });
        const sizingPosition = getCloseablePositionForExit(rule.action, rule.instrument_name, currentExitPositions);
        if (!sizingPosition) continue;
        const baseDesiredAmount = rule.action === 'sell_put'
          ? getSellPutExitAmount(rule, criteria, sizingPosition, plannedValues)
          : Number(sizingPosition.amount);
        const intent = getSyntheticExitIntent(rule.action, {}, criteria);
        const desiredTrancheFraction = rule.action === 'sell_put' && intent === 'monetize_tail_win'
          ? baseDesiredAmount / Number(sizingPosition.amount) : null;
        const desiredAmount = desiredTrancheFraction != null
          ? getDesiredSellPutRemainingAmount({ positionAmount: sizingPosition.amount,
              desiredFraction: desiredTrancheFraction, existingOrder: existingRestingExit })
          : baseDesiredAmount;
        const desiredOrderType = resolveDesiredExitOrderType({ action: rule.action, intent,
          preferredOrderType: normalizePreferredOrderType(rule.action, rule.preferred_order_type),
          price: plannedPrice, ticker });
        const desiredExitOrder = normalizeDesiredExitOrder({ action: rule.action,
          instrumentName: rule.instrument_name, amount: desiredAmount, price: plannedPrice,
          orderType: desiredOrderType, intent, instrument, ticker, existingOrder: existingRestingExit,
          priceReason: patientBuybackPlan?.priceReason, ceilingPrice: patientBuybackPlan?.ceilingPrice });
        // A resting monetization offer is our price into a panic: it may be raised, never chased down.
        if (existingRestingExit?.exit_intent === 'monetize_tail_win' && intent === 'monetize_tail_win'
          && Number(existingRestingExit.limit_price) > desiredExitOrder.limit_price) {
          desiredExitOrder.limit_price = Number(existingRestingExit.limit_price);
        }
        const existingComparison = existingRestingExit
          ? compareRestingExitOrder(existingRestingExit, desiredExitOrder) : null;
        if (existingComparison && existingComparison.decision !== 'replace') {
          const equivalent = existingComparison.decision === 'keep';
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: equivalent ? 'existing_resting_exit' : 'existing_exit_unresolved',
            reason: equivalent ? 'Existing exit matches the normalized desired order'
              : 'Existing exit terms are incomplete; reconciliation required',
            price: desiredExitOrder.limit_price, amount: desiredExitOrder.amount, criteria_json: criteria,
            context_json: { existing_order_id: existingRestingExit.order_id, desired_exit_order: desiredExitOrder },
          });
          continue;
        }

        const recentRejection = getRecentRejectedAction(rule.action, rule.instrument_name);
        if (recentRejection) {
          console.log(`📋 Exit skip: ${rule.action} ${rule.instrument_name} rejected recently; backing off ${formatCooldownMinutes(recentRejection.remaining)} (${recentRejection.reason || 'recent rejection'})`);
          logDecision(rule, {
            decision_status: 'skipped', reason_code: 'recent_rejection',
            reason: recentRejection.reason || 'Recent rejection backoff', criteria_json: criteria,
          });
          continue;
        }

        const price = desiredExitOrder.limit_price;
        const pendingResult = db.insertPendingAction({
          rule_id: rule.id,
          action: rule.action,
          instrument_name: rule.instrument_name,
            amount: desiredExitOrder.amount,
            price: price,
            trigger_details: {
              conditions_met: criteria.conditions.map(c => ({ field: c.field, op: c.op, threshold: c.value, actual: values[c.field] })),
              current_values: plannedValues,
              advisor_limit_price: patientBuybackPlan || patientSellPutPlan ? desiredExitOrder.limit_price : null,
              desired_exit_order: desiredExitOrder,
              replacement_order_id: existingRestingExit?.order_id ?? null,
              replacement_order_snapshot: existingRestingExit ? { ...existingRestingExit } : null,
              patient_buyback_capture_pct: patientBuybackPlan?.capturePct ?? null,
              patient_buyback_ceiling_price: patientBuybackPlan?.ceilingPrice ?? null,
              patient_buyback_price_reason: patientBuybackPlan?.priceReason ?? null,
              patient_sell_put_pnl_pct: patientSellPutPlan?.pnlPct ?? null,
              patient_sell_put_limit_price: patientSellPutPlan?.limitPrice ?? null,
              patient_sell_put_fair_value_pnl_pct: patientSellPutPlan?.fairValuePnlPct ?? null,
              patient_sell_put_fair_value_price: patientSellPutPlan?.fairValuePrice ?? null,
              patient_sell_put_fair_value_source: patientSellPutPlan?.fairValueSource ?? null,
              buyback_intent: rule.action === 'buyback_call' ? getBuybackIntent(criteria) : null,
              put_exit_intent: rule.action === 'sell_put' ? getPutExitIntent(criteria) : null,
              tranche_fraction: desiredTrancheFraction,
              preferred_order_type: desiredExitOrder.order_type,
            },
          });
        const pendingActionId = pendingResult?.lastInsertRowid ?? null;
        logDecision(rule, {
          decision_status: 'triggered',
          reason_code: 'exit_triggered',
          reason: 'Exit rule triggered pending action',
          selected_instrument: rule.instrument_name,
          price,
          amount: desiredExitOrder.amount,
          pending_action_id: pendingActionId,
          criteria_json: criteria,
          context_json: { current_values: plannedValues },
        });
        triggeredCount++;
        console.log(`📋 Exit triggered: ${rule.action} ${rule.instrument_name}`);
      } catch (e) {
        console.log(`📋 Exit rule ${rule.id} error: ${e.message}`);
      }
    }
  } catch (e) {
    console.log(`📋 Exit rules evaluation failed: ${e.message}`);
  }

  // ── Entry rules ────────────────────────────────────────────────────────────
  try {
    const entryRules = db.getActiveRulesByType('entry');
    let openRestingEntryOrders = getOpenRestingEntryOrders();
    let workingEntryActions = [];
    try {
      workingEntryActions = db.getRecentPendingActions(50)
        .filter((action) => ['buy_put', 'sell_call'].includes(action?.action)
          && ['pending', 'confirmed', 'resting'].includes(action?.status));
    } catch { /* ok */ }
    let liveMarginState = null;
    try { liveMarginState = await fetchSubaccount(); } catch { /* ok */ }
    let provisionalCallOrderMargin = 0;
    let entryMarketOiDelta24hPct = null;
    if (db && typeof db.getOpenInterestHourly === 'function') {
      try {
        entryMarketOiDelta24hPct = getLatestHourlyDeltaPct(
          db.getOpenInterestHourly(new Date(Date.now() - 30 * 60 * 60 * 1000).toISOString()),
          24
        );
      } catch { /* optional research context */ }
    }
    const sellCallMarketContext = buildLiveSellCallMarketContext(tickerMap, Date.now(), {
      market_oi_delta_24h_pct: entryMarketOiDelta24hPct,
    });
    const entryBestBuyPutEdge = getBestCurrentBuyPutEdgeCandidate(tickerMap, Date.now());
    if (entryBestBuyPutEdge) {
      recordBuyPutEdgeSnapshotSafe({
        timestamp: evaluationTimestamp,
        instrument_name: entryBestBuyPutEdge.instrument,
        raw_score: entryBestBuyPutEdge.raw_score,
        edge_score: entryBestBuyPutEdge.edge_score,
        ask_price: entryBestBuyPutEdge.ask_price,
        delta: entryBestBuyPutEdge.delta,
        dte: entryBestBuyPutEdge.dte,
        spot_price: spotPrice,
        metadata: {
          formula: `raw_score * (DTE / ${BUY_PUT_EDGE_REFERENCE_DTE})^${BUY_PUT_EDGE_DTE_EXPONENT}`,
          reference_dte: BUY_PUT_EDGE_REFERENCE_DTE,
          dte_exponent: BUY_PUT_EDGE_DTE_EXPONENT,
        },
      });
    }
    const entryBestSellCall = getBestCurrentSellCallCandidate(tickerMap, Date.now());
    if (entryBestSellCall) {
      recordSellCallEdgeSnapshotSafe({
        timestamp: evaluationTimestamp,
        instrument_name: entryBestSellCall.instrument,
        raw_score: entryBestSellCall.score,
        edge_score: entryBestSellCall.selection_score,
        bid_price: entryBestSellCall.bid_price,
        delta: entryBestSellCall.delta,
        dte: entryBestSellCall.dte,
        spot_price: spotPrice,
        metadata: {
          formula: entryBestSellCall.research?.reason,
          normalization: entryBestSellCall.research?.edge_components,
        },
      });
    }
    let buyPutOpportunityContext = null;
    const getBuyPutOpportunityContext = () => {
      if (buyPutOpportunityContext) return buyPutOpportunityContext;
      const putBudgetRemaining = Math.max(0, botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought);
      buyPutOpportunityContext = buildRollingOptionValueContext({
        tickerMap,
        momentum: {
          mediumTerm: botData.mediumTermMomentum,
          shortTerm: botData.shortTermMomentum,
        },
        putBudgetRemaining,
        activeRules: entryRules,
        recentPendingActions: workingEntryActions,
        openRestingOrders: openRestingEntryOrders,
        spotPrice,
      });
      return buyPutOpportunityContext;
    };
    const entryMaintenance = await reassessRestingEntryOrders({ entryRules, instruments, tickerMap, spotPrice,
      positions, marginState: liveMarginState,
      buyPutContext: openRestingEntryOrders.some(order => order.action === 'buy_put') ? getBuyPutOpportunityContext() : null,
      marketContext: sellCallMarketContext });
    triggeredCount += entryMaintenance.queuedCount;
    // Reconciliation may have consumed reservations, and maintenance may have
    // queued a replacement. New-entry gates must see both changes.
    openRestingEntryOrders = getOpenRestingEntryOrders();
    workingEntryActions = [...db.getPendingActions('pending'), ...db.getPendingActions('confirmed')]
      .filter(action => ['buy_put', 'sell_call'].includes(action.action));
    buyPutOpportunityContext = null;
    for (const rule of entryRules) {
      try {
        if (entryMaintenance.blockedActions.has(rule.action)) continue;
        let criteria;
        try { criteria = typeof rule.criteria === 'string' ? JSON.parse(rule.criteria) : rule.criteria; } catch { criteria = null; }
        if (!criteria || typeof criteria !== 'object' || !criteria.option_type) {
          console.log(`📋 Entry rule ${rule.id}: skipping — criteria missing structured fields (need option_type, delta_range, dte_range)`);
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'malformed_criteria',
            reason: 'Entry criteria missing structured fields',
            criteria_json: criteria || rule.criteria || null,
          });
          try {
            const deactivated = db.deactivateRuleById ? db.deactivateRuleById(rule.id) : 0;
            if (deactivated > 0) {
              console.log(`📋 Entry rule ${rule.id}: deactivated malformed persisted rule`);
            }
          } catch (deactivateErr) {
            console.log(`📋 Entry rule ${rule.id}: failed to deactivate malformed rule: ${deactivateErr.message}`);
          }
          continue;
        }

        // Cooldown check: skip if same action was executed within the last hour
        const lastExec = db.getLastExecutedAction(rule.action);
        if (lastExec) {
          const elapsed = Date.now() - new Date(lastExec).getTime();
          if (elapsed < 3600000) {
            console.log(`📋 Entry skip: ${rule.action} action cooldown active for ${formatCooldownMinutes(3600000 - elapsed)}`);
            logDecision(rule, {
              decision_status: 'skipped',
              reason_code: 'action_cooldown',
              reason: `Action cooldown active for ${formatCooldownMinutes(3600000 - elapsed)}`,
              criteria_json: criteria,
            });
            continue; // 1 hour cooldown
          }
        }

        const restingEntryForRule = openRestingEntryOrders.find((order) =>
          Number(order.rule_id) === Number(rule.id)
          && order.action === rule.action
        );
        const hasRestingEntryForRule = Boolean(restingEntryForRule);
        const buyPutContext = rule.action === 'buy_put' ? getBuyPutOpportunityContext() : null;
        const canReplaceRestingBuyPut = Boolean(
          rule.action === 'buy_put'
          && isActionableBuyPutSignal(buyPutContext?.action_pressure?.signal)
        );
        const capacityOrders = canReplaceRestingBuyPut
          ? openRestingEntryOrders.filter((order) => order.action !== 'buy_put')
          : openRestingEntryOrders.filter((order) =>
            !(rule.action === 'buy_put'
              && order.action === 'buy_put'
              && Number(order.rule_id) === Number(rule.id))
          );
        const reservedCapacity = summarizeReservedEntryCapacity(capacityOrders);

        // Put budget discipline: skip if cycle budget exhausted
        if (rule.action === 'buy_put' && botData.putBudgetForCycle > 0) {
          const putRemaining = botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought - reservedCapacity.putBudget;
          if (putRemaining <= 0.20) {
            console.log(
              `📋 Entry skip: buy_put budget unavailable; remaining=$${putRemaining.toFixed(2)}`
              + ` (cycle=$${botData.putBudgetForCycle.toFixed(2)}, rollover=$${botData.putUnspentBuyLimit.toFixed(2)}, spent=$${botData.putNetBought.toFixed(2)}, reserved=$${reservedCapacity.putBudget.toFixed(2)})`
            );
            logDecision(rule, {
              decision_status: 'skipped',
              reason_code: 'put_budget_unavailable',
              reason: `buy_put budget unavailable; remaining=$${putRemaining.toFixed(2)}`,
              criteria_json: criteria,
              context_json: {
                cycle_budget: botData.putBudgetForCycle,
                rollover: botData.putUnspentBuyLimit,
                spent: botData.putNetBought,
                reserved: reservedCapacity.putBudget,
              },
            });
            continue;
          }
        }

        // Call exposure cap: the buffer is safety, not planned entry capacity.
        if (rule.action === 'sell_call') {
          if (!liveMarginState) {
            console.log(`📋 Skip ${rule.action}: margin state unavailable`);
            logDecision(rule, {
              decision_status: 'skipped',
              reason_code: 'margin_state_unavailable',
              reason: 'Unable to fetch margin state for sell_call entry',
              criteria_json: criteria,
            });
            continue;
          }
          const effectiveCapPct = getEffectiveCallExposureCapPct(positions, spotPrice);
          const effectiveLimitPct = getCallExposureLimitPct(effectiveCapPct);
          const currentUtilization = estimateProjectedDisplayedMarginUtilization(liveMarginState, provisionalCallOrderMargin);
          if (currentUtilization == null) {
            console.log(`📋 Skip ${rule.action}: unable to compute margin utilization`);
            logDecision(rule, {
              decision_status: 'skipped',
              reason_code: 'margin_utilization_unavailable',
              reason: 'Unable to compute projected margin utilization',
              criteria_json: criteria,
            });
            continue;
          }
          if (currentUtilization >= effectiveCapPct) {
            console.log(`📋 Call entry cap reached: ${(currentUtilization * 100).toFixed(1)}% >= ${(effectiveCapPct * 100).toFixed(1)}% target cap; ${(effectiveLimitPct * 100).toFixed(1)}% buffer is reserved for execution drift`);
            logDecision(rule, {
              decision_status: 'skipped',
              reason_code: 'call_exposure_cap_reached',
              reason: `Current utilization ${(currentUtilization * 100).toFixed(1)}% >= target cap ${(effectiveCapPct * 100).toFixed(1)}%`,
              criteria_json: criteria,
              context_json: { current_utilization: currentUtilization, target_cap_pct: effectiveCapPct, effective_limit_pct: effectiveLimitPct },
            });
            continue;
          }
        }

        // Dedup: pending/confirmed actions block duplicates. Resting actions flow
        // through the candidate scan so we can keep, adjust, or replace them.
        if (hasPendingOrConfirmedActionForRule(rule.id)) {
          console.log(`📋 Entry skip: ${rule.action} rule ${rule.id} already has pending/confirmed action`);
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'pending_duplicate',
            reason: 'Rule already has pending or confirmed entry action',
            criteria_json: criteria,
          });
          continue;
        }

        if (workingEntryActions.some((action) => action.action === rule.action && action.status !== 'resting')) {
          console.log(`📋 Entry skip: ${rule.action} already has a pending or confirmed entry order`);
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'working_action_duplicate',
            reason: 'Action already has pending or confirmed entry order',
            criteria_json: criteria,
          });
          continue;
        }

        // Scan tickerMap for candidates matching criteria
        const optionType = criteria.option_type; // 'P' or 'C'
        const deltaRange = criteria.delta_range; // [min, max]
        const dteRange = criteria.dte_range; // [min, max]
        const maxStrikePct = criteria.max_strike_pct || null;
        const marketConditions = criteria.market_conditions || null;
        // buy_put cost discipline is handled by total budget_limit/put budget
        // plus score/value filters; per-contract max_cost is deprecated.
        const maxCost = rule.action === 'buy_put' ? null : criteria.max_cost ?? null;
        const minBid = Number(criteria.min_bid ?? 0) > 0 ? Number(criteria.min_bid) : null;
        const minScore = Number(criteria.min_score ?? 0) > 0 ? Number(criteria.min_score) : null;
        // Legacy min_edge_score is retired; continuous PUT/CALL EDGE is the score contract.
        // Saved sell-call rules from the former composite-edge system only have
        // min_edge_score. Run those through the established normalized floor
        // until the next advisory replaces them with an explicit min_score.
        const sellCallMinScore = rule.action === 'sell_call'
          ? (minScore ?? SELL_CALL_FALLBACK_MIN_SCORE)
          : null;
        const rawValueSignal = criteria.value_signal ?? criteria.buy_put_signal;
        const valueSignal = normalizeBuyPutValueSignal(rawValueSignal);
        let targetScore = Number(criteria.target_score ?? 0) > 0 ? Number(criteria.target_score) : null;

        // Empty market_conditions means "no extra market gate"; only evaluate
        // when the advisor supplied at least one condition.
        const hasMarketConditions = Array.isArray(marketConditions)
          ? marketConditions.length > 0
          : Boolean(marketConditions);
        if (hasMarketConditions) {
          const marketValues = { spot_price: spotPrice };
          if (!evaluateConditions(marketConditions, 'all', marketValues)) {
            logDecision(rule, {
              decision_status: 'skipped',
              reason_code: 'market_conditions_not_met',
              reason: 'Entry market conditions did not pass',
              criteria_json: criteria,
              context_json: { market_values: marketValues },
            });
            continue;
          }
        }
        if (rule.action === 'buy_put' && hasExplicitBuyPutValueSignal(rawValueSignal) && !valueSignal) {
          console.log(`📋 Entry skip: buy_put rule ${rule.id} has unknown value_signal=${rawValueSignal}`);
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'unknown_value_signal',
            reason: `Unknown value_signal=${rawValueSignal}`,
            criteria_json: criteria,
          });
          continue;
        }
        const currentBuyPutSignal = rule.action === 'buy_put'
          ? buyPutContext?.action_pressure?.signal || null
          : null;
        if (rule.action === 'buy_put' && valueSignal) {
          if (!buyPutValueSignalMatches(valueSignal, currentBuyPutSignal) && !hasRestingEntryForRule) {
            console.log(`📋 Entry skip: buy_put rule ${rule.id} value_signal=${valueSignal} does not match current signal ${currentBuyPutSignal || 'none'}`);
            logDecision(rule, {
              decision_status: 'skipped',
              reason_code: 'value_signal_mismatch',
              reason: `Required value_signal=${valueSignal} did not match current signal ${currentBuyPutSignal || 'none'}`,
              criteria_json: criteria,
              context_json: { required_value_signal: valueSignal, current_buy_put_signal: currentBuyPutSignal || null },
            });
            continue;
          }
        }
        if (canReplaceRestingBuyPut) {
          const signalTargetScore = Number(buyPutContext?.action_pressure?.target_score || 0);
          if (signalTargetScore > 0 && (targetScore == null || targetScore > signalTargetScore)) {
            targetScore = signalTargetScore;
          }
        }

        let candidates = [];
        let filterStats = { total: 0, noInstrument: 0, wrongType: 0, noExpiry: 0, dteOut: 0, deltaOut: 0, strikeOut: 0, costOut: 0, bidOut: 0, scoreOut: 0 };
        for (const [instrName, ticker] of Object.entries(tickerMap)) {
          try {
            filterStats.total++;

            // Find matching instrument
            const instrument = instruments.find(i => i.instrument_name === instrName);
            if (!instrument) { filterStats.noInstrument++; continue; }

            // Filter by option type
            if (optionType && instrument.option_details?.option_type !== optionType) { filterStats.wrongType++; continue; }

            // Compute DTE from instrument name
            const dte = computeDteFromInstrumentName(instrName);
            if (dte == null) { filterStats.noExpiry++; continue; }

            // Filter by DTE range
            if (dteRange && (dte < dteRange[0] || dte > dteRange[1])) { filterStats.dteOut++; continue; }

            // Filter by delta range
            const delta = Number(ticker?.option_pricing?.d) || 0;
            if (rule.action === 'sell_call' && !isSellCallCandidateInStrategyRange(dte, delta)) {
              if (dte < CALL_EXPIRATION_RANGE[0] || dte > CALL_EXPIRATION_RANGE[1]) filterStats.dteOut++;
              else filterStats.deltaOut++;
              continue;
            }
            if (deltaRange && (delta < deltaRange[0] || delta > deltaRange[1])) { filterStats.deltaOut++; continue; }

            // Filter by max_strike_pct
            const strike = Number(instrument.option_details?.strike) || 0;
            if (maxStrikePct && strike >= maxStrikePct * spotPrice) { filterStats.strikeOut++; continue; }

            const askPrice = Number(ticker?.a) || 0;
            const askAmount = Number(ticker?.A) || 0;
            const bidPrice = Number(ticker?.b) || 0;
            const bidAmount = Number(ticker?.B) || 0;

            // Filter by max_cost where still supported.
            if (maxCost != null && askPrice > maxCost) { filterStats.costOut++; continue; }

            // Filter by min_bid (for sells)
            if (minBid != null && bidPrice < minBid) { filterStats.bidOut++; continue; }

            // Score: puts use the planned bid when making a patient market.
            // Calls still use executable bid premium divided by delta.
            const absDelta = Math.abs(delta);
            let score;
            let rawScore = null;
            let liveScore = null;
            let liveRawScore = null;
            let candidateLimitPrice = null;
            let scoreThresholdPrice = null;
            let priceSource = null;
            if (optionType === 'P') {
              const pricing = getBuyPutEntryPricing({ askPrice, absDelta, dte, minScore, targetScore });
              score = pricing.plannedScore;
              liveScore = pricing.liveScore;
              rawScore = pricing.plannedRawScore;
              liveRawScore = pricing.liveRawScore;
              candidateLimitPrice = pricing.limitPrice;
              scoreThresholdPrice = pricing.thresholdPrice;
              priceSource = pricing.priceSource;
            } else {
              score = absDelta > 0 ? bidPrice / absDelta : 0;
            }
            const sellCallResearch = rule.action === 'sell_call'
              ? buildSellCallEdge({ score, dte })
              : null;
            const buyPutResearch = rule.action === 'buy_put'
              ? classifyBuyPutEdge({
                score: liveScore,
                rawScore: liveRawScore,
                dte,
                strike,
                spotPrice,
                entryPrice: askPrice,
                askPrice,
                bidPrice,
                markPrice: Number(ticker?.M),
                askAmount,
                bidAmount,
                impliedVol: getTickerImpliedVol(ticker),
                marketContext: sellCallMarketContext,
              })
              : null;
            // Rank observed live quotes. Pricing every patient bid to the same
            // target must not rank contracts by incidental rounding differences.
            const selectionScore = rule.action === 'buy_put' ? liveScore : sellCallResearch?.selection_score ?? score;

            if (optionType === 'P' && (!(candidateLimitPrice > 0) || !(liveScore > 0))) { filterStats.scoreOut++; continue; }
            if (rule.action === 'sell_call') {
              if (selectionScore < sellCallMinScore) { filterStats.scoreOut++; continue; }
            } else if (rule.action === 'buy_put') {
              if (minScore != null && score < Number(minScore)) { filterStats.scoreOut++; continue; }
            } else if (minScore != null && score < Number(minScore)) { filterStats.scoreOut++; continue; }
            if (optionType === 'P' && targetScore != null && score < targetScore) { filterStats.scoreOut++; continue; }

            const amountStep = instrument.options?.amount_step || 0.01;

            candidates.push({
              name: instrName,
              instrument,
              ticker,
              delta,
              dte,
              askPrice,
              askAmount,
              bidPrice,
              bidAmount,
              score,
              rawScore,
              liveScore,
              liveRawScore,
              candidateLimitPrice,
              scoreThresholdPrice,
              priceSource,
              selectionScore,
              sellCallResearch,
              buyPutResearch,
              strike,
              amountStep,
            });
          } catch (e) {
            // Skip individual candidate errors
          }
        }

        if (candidates.length === 0) {
          const filtered = Object.entries(filterStats).filter(([k, v]) => k !== 'total' && v > 0).map(([k, v]) => `${k}=${v}`).join(', ');
          console.log(`📋 Rule ${rule.id} (${rule.action}): 0 candidates from ${filterStats.total} tickers — filtered by: ${filtered || 'no tickers'}`);
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'no_candidates',
            reason: `0 candidates from ${filterStats.total} tickers; filtered by ${filtered || 'no tickers'}`,
            candidates_evaluated: 0,
            criteria_json: criteria,
            context_json: { filter_stats: filterStats },
          });
          if (optionType || dteRange) {
            const expirySummary = new Map();
            for (const instrName of Object.keys(tickerMap)) {
              const instrument = instruments.find(i => i.instrument_name === instrName);
              if (!instrument) continue;
              if (optionType && instrument.option_details?.option_type !== optionType) continue;
              const dte = computeDteFromInstrumentName(instrName);
              if (dte == null) continue;
              const expiryKey = instrName.split('-')[1];
              const bucket = expirySummary.get(expiryKey) || { count: 0, minDte: dte, maxDte: dte };
              bucket.count++;
              bucket.minDte = Math.min(bucket.minDte, dte);
              bucket.maxDte = Math.max(bucket.maxDte, dte);
              expirySummary.set(expiryKey, bucket);
            }
            const expiryText = [...expirySummary.entries()]
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([expiryKey, info]) => `${expiryKey}:${info.count} (${info.minDte.toFixed(1)}-${info.maxDte.toFixed(1)} DTE)`)
              .join(', ');
            console.log(`📋 Rule ${rule.id} criteria: option_type=${optionType || 'any'} dte_range=${JSON.stringify(dteRange || null)} expiries=${expiryText || 'none'}`);
          }
          continue;
        }

        // Rank each side by continuous EDGE from its live executable quote.
        candidates.sort((a, b) => {
          const aRank = ['sell_call', 'buy_put'].includes(rule.action) ? (a.selectionScore ?? a.score) : a.score;
          const bRank = ['sell_call', 'buy_put'].includes(rule.action) ? (b.selectionScore ?? b.score) : b.score;
          // For puts, even ties must not depend on patient-price rounding.
          return bRank - aRank || (rule.action === 'buy_put' ? 0 : b.score - a.score)
            || a.name.localeCompare(b.name);
        });
        let blockedByRestingOrder = 0;
        const best = candidates.find((candidate) => {
          const sameActionEntryResting = openRestingEntryOrders.some(order =>
            order.instrument_name === candidate.name && order.action === rule.action
          );
          const candidatePrice = optionType === 'P' ? candidate.askPrice : candidate.bidPrice;
          if (sameActionEntryResting || !getBlockingRestingOrderForEntryCandidate(rule.action, candidate, candidatePrice)) return true;
          blockedByRestingOrder++;
          return false;
        });
        if (!best) {
          console.log(`📋 Rule ${rule.id} (${rule.action}): ${candidates.length} candidate(s) blocked by same-instrument resting orders`);
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'all_candidates_blocked_by_resting',
            reason: `${candidates.length} candidate(s) blocked by same-instrument resting orders`,
            candidates_evaluated: candidates.length,
            criteria_json: criteria,
            context_json: { filter_stats: filterStats },
          });
          recordCandidateObservationsSafe(buildCandidateObservationRows({
            candidates,
            rule,
            criteria,
            selectedName: null,
            decisionStatus: 'skipped',
            selectionReason: 'all candidates blocked by same-instrument resting orders',
            spotPrice,
            tickTimestamp: evaluationTimestamp,
            context: { filter_stats: filterStats, blocked_by_resting_order: candidates.length },
          }));
          continue;
        }
        if (blockedByRestingOrder > 0) {
          console.log(`📋 Rule ${rule.id} (${rule.action}): skipped ${blockedByRestingOrder} candidate(s) with same-instrument resting orders; selected ${best.name}`);
        }
        const candidateTelemetryContext = {
          filter_stats: filterStats,
          selection_metric: rule.action === 'buy_put' ? 'put_edge_v1' : 'call_edge_v1',
          selection_price_basis: rule.action === 'buy_put' ? 'live_ask' : 'live_bid',
          blocked_by_resting_order: blockedByRestingOrder,
          required_value_signal: rule.action === 'buy_put' ? valueSignal || null : null,
          current_buy_put_signal: rule.action === 'buy_put' ? currentBuyPutSignal || null : null,
          target_score: targetScore,
        };
        const recordCandidateFrame = (decisionStatus, selectionReason, pendingActionId = null, extraContext = {}) => recordCandidateObservationsSafe(buildCandidateObservationRows({
          candidates,
          rule,
          criteria,
          selectedName: best.name,
          decisionStatus,
          selectionReason,
          pendingActionId,
          spotPrice,
          tickTimestamp: evaluationTimestamp,
          context: { ...candidateTelemetryContext, ...extraContext },
        }));

        // Calculate amount: min of rule budget_limit, put cycle budget remaining, and book liquidity
        const livePrice = optionType === 'P' ? best.askPrice : best.bidPrice;
        const advisorLimitPrice = rule.action === 'buy_put'
          ? best.scoreThresholdPrice
          : null;
        let price = rule.action === 'buy_put' && Number(best.candidateLimitPrice) > 0
          ? Number(best.candidateLimitPrice)
          : livePrice;
        if (price <= 0) {
          const reason = 'Selected candidate had non-positive execution price';
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'invalid_price',
            reason,
            candidates_evaluated: candidates.length,
            selected_instrument: best.name,
            raw_score: best.score,
            selection_score: best.selectionScore,
            price,
            criteria_json: criteria,
            context_json: { ...candidateTelemetryContext, live_price: livePrice },
          });
          recordCandidateFrame('skipped', reason, null, { live_price: livePrice, price });
          continue;
        }

        let maxByBudget = (rule.budget_limit || Infinity) / price;
        let sellCallMarginDebug = null;
        // For puts: also cap by cycle budget discipline
        if (rule.action === 'buy_put' && botData.putBudgetForCycle > 0) {
          const putRemaining = botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought - reservedCapacity.putBudget;
          maxByBudget = Math.min(maxByBudget, putRemaining / price);
        }
        // For calls: cap by remaining exposure headroom under the active target.
        if (rule.action === 'sell_call' && liveMarginState) {
          const targetCapPct = getEffectiveCallExposureCapPct(positions, spotPrice);
          const effectiveLimitPct = getCallExposureLimitPct(targetCapPct);
          const marginHeadroom = getDisplayedMarginHeadroomAtCap(liveMarginState, targetCapPct);
          const marginPerUnit = estimateShortCallMarginPerUnit(
            liveMarginState,
            positions,
            openRestingEntryOrders,
            spotPrice,
            best.strike,
            best.bidPrice
          );
          if (marginPerUnit > 0 && marginHeadroom != null) {
            const maxQtyAtCap = marginHeadroom / marginPerUnit;
            sellCallMarginDebug = {
              currentUtilization: estimateDisplayedMarginUtilization(liveMarginState),
              marginHeadroom,
              marginPerUnit,
              maxQtyAtCap,
              targetCapPct,
              effectiveLimitPct,
            };
            maxByBudget = Math.min(maxByBudget, maxQtyAtCap);
          }
        }
        const buyPutMakerBid = rule.action === 'buy_put'
          && (
            best.priceSource === 'score_threshold'
            || (Number(best.askPrice) > 0 && price < Number(best.askPrice) - 1e-9)
          );
        const bookLiq = optionType === 'P'
          ? (buyPutMakerBid ? Infinity : best.askAmount)
          : best.bidAmount;
        const step = best.amountStep || 0.01;
        const raw = Math.min(maxByBudget, bookLiq, 20);
        let qty = Math.floor(raw / step) * step;
        let projectedDisplayedUtilization = null;
        if (rule.action === 'sell_call' && liveMarginState && sellCallMarginDebug) {
          projectedDisplayedUtilization = estimateProjectedDisplayedMarginUtilization(
            liveMarginState,
            qty * sellCallMarginDebug.marginPerUnit
          );
          if (sellCallMarginDebug.maxQtyAtCap > 0 && sellCallMarginDebug.maxQtyAtCap < step) {
            console.log(
              `📋 Skip ${rule.action} ${best.name}: desired=${raw.toFixed(3)}, max_safe_qty=${sellCallMarginDebug.maxQtyAtCap.toFixed(3)}, step=${step.toFixed(3)}`
              + `, current=${sellCallMarginDebug.currentUtilization != null ? `${(sellCallMarginDebug.currentUtilization * 100).toFixed(1)}%` : 'N/A'}`
              + `, target_cap=${(sellCallMarginDebug.targetCapPct * 100).toFixed(1)}%, buffered_limit=${(sellCallMarginDebug.effectiveLimitPct * 100).toFixed(1)}% — no tradable size under target cap`
            );
          } else if (qty > 0 && raw - qty >= step / 2) {
            console.log(
              `📋 Resize ${rule.action} ${best.name}: desired=${raw.toFixed(3)} -> ${qty.toFixed(3)}`
              + ` (max_safe=${sellCallMarginDebug.maxQtyAtCap.toFixed(3)}, current=${sellCallMarginDebug.currentUtilization != null ? `${(sellCallMarginDebug.currentUtilization * 100).toFixed(1)}%` : 'N/A'}`
              + `${projectedDisplayedUtilization != null ? `, projected=${(projectedDisplayedUtilization * 100).toFixed(1)}%` : ''}, target_cap=${(sellCallMarginDebug.targetCapPct * 100).toFixed(1)}%, buffered_limit=${(sellCallMarginDebug.effectiveLimitPct * 100).toFixed(1)}%)`
            );
          }
        }
        if (qty < step) {
          const reason = `No tradable size after budget/liquidity/margin caps; desired=${raw.toFixed(4)}, step=${step.toFixed(4)}`;
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'quantity_below_step',
            reason,
            candidates_evaluated: candidates.length,
            selected_instrument: best.name,
            raw_score: best.score,
            selection_score: best.selectionScore,
            price,
            amount: qty,
            criteria_json: criteria,
            context_json: { ...candidateTelemetryContext, raw_quantity: raw, step, sell_call_margin: sellCallMarginDebug },
          });
          recordCandidateFrame('skipped', reason, null, {
            price,
            raw_quantity: raw,
            step,
            sell_call_margin: sellCallMarginDebug,
          });
          continue;
        }
        const venueQty = floorOrderAmountToVenuePrecision(qty);
        if (!isVenueOrderAmountTradable(venueQty)) {
          console.log(
            `📋 Skip ${rule.action} ${best.name}: qty=${qty.toFixed(4)} normalizes to ${venueQty.toFixed(VENUE_AMOUNT_DECIMALS)},`
            + ` not above venue minimum ${VENUE_MIN_ORDER_AMOUNT}`
          );
          const reason = `Venue amount ${venueQty.toFixed(VENUE_AMOUNT_DECIMALS)} not above minimum ${VENUE_MIN_ORDER_AMOUNT}`;
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'venue_quantity_too_small',
            reason,
            candidates_evaluated: candidates.length,
            selected_instrument: best.name,
            raw_score: best.score,
            selection_score: best.selectionScore,
            price,
            amount: venueQty,
            criteria_json: criteria,
            context_json: { ...candidateTelemetryContext, qty_before_venue_rounding: qty },
          });
          recordCandidateFrame('skipped', reason, null, {
            price,
            qty_before_venue_rounding: qty,
            venue_qty: venueQty,
          });
          continue;
        }
        qty = venueQty;

        // The incumbent has already been assessed against normalized venue
        // terms. Do not countermand that result with a raw candidate price.
        const existingResting = openRestingEntryOrders.find(order => order.instrument_name === best.name && order.action === rule.action);
        if (existingResting) {
          const reason = 'Existing entry reassessed independently before candidate selection';
          logDecision(rule, {
            decision_status: 'skipped', reason_code: 'existing_resting_reassessed', reason,
            candidates_evaluated: candidates.length, selected_instrument: best.name,
            price: existingResting.limit_price,
            pending_action_id: existingResting.pending_action_id ?? null,
            context_json: { existing_order_id: existingResting.order_id,
              maintenance: entryMaintenance.plans.get(existingResting.order_id) ?? null },
          });
          recordCandidateFrame('skipped', reason, existingResting.pending_action_id ?? null, {
            price: existingResting.limit_price, existing_order_id: existingResting.order_id,
          });
          continue;
        } else {
          const sameActionResting = openRestingEntryOrders.find(order => order.action === rule.action);
          if (sameActionResting) {
            const canReplaceDifferentResting = Boolean(
              rule.action === 'buy_put'
              && isActionableBuyPutSignal(buyPutContext?.action_pressure?.signal)
              && best.score > 0
            );
            if (!canReplaceDifferentResting) {
              console.log(`📋 Entry skip: ${rule.action} already has a resting entry order (${sameActionResting.instrument_name})`);
              const reason = `Same action already has resting entry order on ${sameActionResting.instrument_name}`;
              logDecision(rule, {
                decision_status: 'skipped',
                reason_code: 'same_action_resting_order',
                reason,
                candidates_evaluated: candidates.length,
                selected_instrument: best.name,
                raw_score: best.score,
                selection_score: best.selectionScore,
                price,
                amount: qty,
                pending_action_id: sameActionResting.pending_action_id ?? null,
                criteria_json: criteria,
                context_json: {
                  ...candidateTelemetryContext,
                  resting_instrument: sameActionResting.instrument_name,
                  resting_order_id: sameActionResting.order_id,
                  resting_price: sameActionResting.limit_price,
                },
              });
              recordCandidateFrame('skipped', reason, sameActionResting.pending_action_id ?? null, {
                price,
                amount: qty,
                resting_instrument: sameActionResting.instrument_name,
                resting_order_id: sameActionResting.order_id,
                resting_price: sameActionResting.limit_price,
              });
              continue;
            }

            const lag = buyPutContext.spot_repricing_lag_context || {};
            const signal = currentBuyPutSignal || (valueSignal === 'any_actionable_buy_put' ? 'standing_patient_bid' : 'unknown');
            console.log(`📋 Replace resting ${rule.action}: cancel ${sameActionResting.order_id} ${sameActionResting.instrument_name} @ $${Number(sameActionResting.limit_price).toFixed(4)} -> ${best.name} @ $${price.toFixed(4)} (${signal}; score_1h=${lag.score_trend_1h_pct ?? 'n/a'}%, spot_20m=${lag.spot_move_20m_pct ?? 'n/a'}%)`);
            const cancelled = await cancelOrder(sameActionResting.order_id, sameActionResting.instrument_name);
            if (!cancelled) {
              console.log(`📋 Keep resting ${rule.action} ${sameActionResting.instrument_name}: cancel failed, re-evaluate next tick`);
              const reason = `Resting ${rule.action} replacement cancel failed`;
              logDecision(rule, {
                decision_status: 'skipped',
                reason_code: 'replacement_cancel_failed',
                reason,
                candidates_evaluated: candidates.length,
                selected_instrument: best.name,
                raw_score: best.score,
                selection_score: best.selectionScore,
                price,
                amount: qty,
                pending_action_id: sameActionResting.pending_action_id ?? null,
                criteria_json: criteria,
                context_json: { ...candidateTelemetryContext, resting_order_id: sameActionResting.order_id },
              });
              recordCandidateFrame('skipped', reason, sameActionResting.pending_action_id ?? null, {
                price,
                amount: qty,
                resting_order_id: sameActionResting.order_id,
              });
              continue;
            }
            console.log(`📋 Replacement waits for terminal fill reconciliation of ${sameActionResting.order_id}`);
          continue;
          }
        }

        const blockingRestingOrder = getBlockingRestingOrderForEntryCandidate(rule.action, best, price);
        if (blockingRestingOrder) {
          console.log(`📋 Skip ${rule.action} ${best.name}: blocking resting ${blockingRestingOrder.action || 'order'} already on book @ $${Number(blockingRestingOrder.limit_price || 0).toFixed(4)}`);
          const reason = `Blocking resting ${blockingRestingOrder.action || 'order'} already on book`;
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'blocking_resting_order',
            reason,
            candidates_evaluated: candidates.length,
            selected_instrument: best.name,
            raw_score: best.score,
            selection_score: best.selectionScore,
            price,
            amount: qty,
            pending_action_id: blockingRestingOrder.pending_action_id ?? null,
            criteria_json: criteria,
            context_json: {
              ...candidateTelemetryContext,
              blocking_action: blockingRestingOrder.action || null,
              blocking_order_id: blockingRestingOrder.order_id || null,
              blocking_price: blockingRestingOrder.limit_price || null,
            },
          });
          recordCandidateFrame('skipped', reason, blockingRestingOrder.pending_action_id ?? null, {
            price,
            amount: qty,
            blocking_action: blockingRestingOrder.action || null,
            blocking_order_id: blockingRestingOrder.order_id || null,
            blocking_price: blockingRestingOrder.limit_price || null,
          });
          continue;
        }

        const recentRejection = getRecentRejectedAction(rule.action, best.name);
        if (recentRejection) {
          console.log(`📋 Skip ${rule.action} ${best.name}: rejected recently, backing off ${formatCooldownMinutes(recentRejection.remaining)} (${recentRejection.reason || 'recent rejection'})`);
          const reason = recentRejection.reason || 'Recent rejection backoff';
          logDecision(rule, {
            decision_status: 'skipped',
            reason_code: 'recent_rejection',
            reason,
            candidates_evaluated: candidates.length,
            selected_instrument: best.name,
            raw_score: best.score,
            selection_score: best.selectionScore,
            price,
            amount: qty,
            criteria_json: criteria,
            context_json: { ...candidateTelemetryContext, rejection_remaining_ms: recentRejection.remaining },
          });
          recordCandidateFrame('skipped', reason, null, {
            price,
            amount: qty,
            rejection_remaining_ms: recentRejection.remaining,
          });
          continue;
        }

        const recentFailedEntry = getRecentFailedEntry(rule.action, best.name);
        if (recentFailedEntry) {
          if (isIocZeroFillFailure(recentFailedEntry)) {
            console.log(`📋 Retry ${rule.action} ${best.name}: recent IOC zero fill is not a cooldown blocker (${recentFailedEntry.reason || 'zero fill'})`);
          } else {
            console.log(`📋 Skip ${rule.action} ${best.name}: failed recently, cooling down (${recentFailedEntry.reason || 'recent execution failure'})`);
            const reason = recentFailedEntry.reason || 'Recent execution failure cooldown';
            logDecision(rule, {
              decision_status: 'skipped',
              reason_code: 'recent_failed_entry',
              reason,
              candidates_evaluated: candidates.length,
              selected_instrument: best.name,
              raw_score: best.score,
              selection_score: best.selectionScore,
              price,
              amount: qty,
              criteria_json: criteria,
              context_json: { ...candidateTelemetryContext, failed_elapsed_ms: recentFailedEntry.elapsed },
            });
            recordCandidateFrame('skipped', reason, null, {
              price,
              amount: qty,
              failed_elapsed_ms: recentFailedEntry.elapsed,
            });
            continue;
          }
        }

        const pendingResult = db.insertPendingAction({
          rule_id: rule.id,
          action: rule.action,
          instrument_name: best.name,
          amount: qty,
          price: price,
          trigger_details: {
            score: rule.action === 'sell_call' ? roundForAdvisory(best.selectionScore, 2) : best.score,
            raw_score: rule.action === 'sell_call' ? roundForAdvisory(best.score, 2) : null,
            delta: best.delta,
            dte: best.dte,
            strike: best.strike,
            candidates_evaluated: candidates.length,
            projected_utilization: projectedDisplayedUtilization,
            live_price: livePrice,
            live_score: rule.action === 'buy_put' ? best.liveScore : null,
            planned_score: rule.action === 'buy_put' ? best.score : null,
            price_source: rule.action === 'buy_put' ? best.priceSource : null,
            selection_score: ['sell_call', 'buy_put'].includes(rule.action) ? roundForAdvisory(best.selectionScore, rule.action === 'buy_put' ? 6 : 2) : null,
            selection_metric: rule.action === 'buy_put' ? 'put_edge_v1' : 'call_edge_v1',
            selection_price_basis: rule.action === 'buy_put' ? 'live_ask' : 'live_bid',
            buy_put_research: rule.action === 'buy_put' && best.buyPutResearch ? {
              recommendation: best.buyPutResearch.recommendation,
              dte_bucket: best.buyPutResearch.dte_bucket,
              score_band: best.buyPutResearch.score_band,
              edge_components: best.buyPutResearch.edge_components,
              selection_multiplier: best.buyPutResearch.selection_multiplier,
              selection_metric: 'put_edge_v1',
              reason: best.buyPutResearch.reason,
            } : null,
            sell_call_research: rule.action === 'sell_call' && best.sellCallResearch ? {
              recommendation: best.sellCallResearch.recommendation,
              preferred: best.sellCallResearch.preferred,
              strong: best.sellCallResearch.strong,
              dte_bucket: best.sellCallResearch.dte_bucket,
              score_band: best.sellCallResearch.score_band,
              edge_components: best.sellCallResearch.edge_components,
              selection_multiplier: best.sellCallResearch.selection_multiplier,
              reason: best.sellCallResearch.reason,
            } : null,
            advisor_limit_price: advisorLimitPrice,
            target_score: targetScore,
            target_score_nudge_pct: targetScore != null && Number(best.liveScore) > 0 ? +(((targetScore / best.liveScore) - 1) * 100).toFixed(2) : null,
            buy_put_signal: rule.action === 'buy_put'
              ? currentBuyPutSignal || (valueSignal === 'any_actionable_buy_put' ? 'standing_patient_bid' : null)
              : null,
            required_value_signal: rule.action === 'buy_put' ? valueSignal || null : null,
            spot_repricing_lag: rule.action === 'buy_put' ? buyPutContext?.spot_repricing_lag_context || null : null,
            recent_relative_value: rule.action === 'buy_put' ? buyPutContext?.recent_relative_value_context || null : null,
            preferred_order_type: currentBuyPutSignal === 'spot_drop_option_repricing_lag' ? 'ioc' : normalizePreferredOrderType(rule.action, rule.preferred_order_type),
          },
        });
        const pendingActionId = pendingResult?.lastInsertRowid ?? null;
        logDecision(rule, {
          decision_status: 'triggered',
          reason_code: 'candidate_selected',
          reason: 'Selected candidate created pending entry action',
          candidates_evaluated: candidates.length,
          selected_instrument: best.name,
          raw_score: best.score,
          selection_score: best.selectionScore,
          price,
          amount: qty,
          pending_action_id: pendingActionId,
          criteria_json: criteria,
          context_json: {
            ...candidateTelemetryContext,
            projected_utilization: projectedDisplayedUtilization,
            live_price: livePrice,
            live_score: rule.action === 'buy_put' ? best.liveScore : null,
            planned_score: rule.action === 'buy_put' ? best.score : null,
            price_source: rule.action === 'buy_put' ? best.priceSource : null,
            buy_put_research: rule.action === 'buy_put' ? best.buyPutResearch || null : null,
            sell_call_research: rule.action === 'sell_call' ? best.sellCallResearch || null : null,
          },
        });
        recordCandidateFrame('triggered', 'selected candidate created pending entry action', pendingActionId, {
          price,
          amount: qty,
          projected_utilization: projectedDisplayedUtilization,
          live_price: livePrice,
        });
        triggeredCount++;
        openRestingEntryOrders.push({
          order_id: `pending-${rule.id}-${best.name}`,
          instrument_name: best.name,
          action: rule.action,
          direction: rule.action === 'buy_put' ? 'buy' : 'sell',
          amount: qty,
          limit_price: price,
          pending_action_id: pendingActionId,
        });
        if (rule.action === 'sell_call' && liveMarginState) {
          provisionalCallOrderMargin += qty * estimateShortCallMarginPerUnit(
            liveMarginState,
            positions,
            openRestingEntryOrders,
            spotPrice,
            best.strike,
            best.bidPrice
          );
        }
        console.log(
          `📋 Entry candidate: ${rule.action} ${best.name} score=${best.score.toFixed(6)}`
          + (rule.action === 'sell_call' && best.sellCallResearch
            ? ` selection=${Number(best.selectionScore || 0).toFixed(2)} research=${best.sellCallResearch.recommendation}/${best.sellCallResearch.dte_bucket}/${best.sellCallResearch.score_band}`
            : '')
        );
      } catch (e) {
        console.log(`📋 Entry rule ${rule.id} error: ${e.message}`);
      }
    }
  } catch (e) {
    console.log(`📋 Entry rules evaluation failed: ${e.message}`);
  }

  return triggeredCount;
};

// ─── LLM-Driven Trading: Open Order Management ──────────────────────────────

const getRestingExitInvalidReason = ({ order, tracked, activeRules = [], positions = [], instruments = [], tickerMap = {}, spotPrice = 0 }) => {
  const action = tracked?.action || inferActionFromOpenOrder(order);
  if (!isReduceOnlyExitAction(action)) return null;
  const position = getCloseablePositionForExit(action, order.instrument_name, positions);
  const remaining = Math.max(0, Number(order.amount) - Number(order.filled_amount || 0));
  if (!position || remaining > Number(position.amount) + 1e-9) return 'resting exit exceeds live closeable position';
  const rules = activeRules.filter(rule => rule.rule_type === 'exit'
    && rule.action === action && rule.instrument_name === order.instrument_name);
  const ticker = tickerMap[order.instrument_name];
  if (!ticker) return 'resting exit cannot be revalidated without a current quote';
  const liveValues = getRuleEvaluationValues(position, ticker, spotPrice, action);
  const limit = Number(order.limit_price);
  const entry = Number(position.avg_entry_price);
  if (!(limit > 0) || !(entry > 0)) return 'resting exit has unavailable price economics';
  const plannedValues = { ...liveValues, execution_price: limit,
    unrealized_pnl_pct: (action === 'buyback_call' ? (entry - limit) : (limit - entry)) / entry * 100 };
  const parsedRules = rules.map(rule => ({ rule, criteria: parseMaybeJsonObject(rule.criteria) }));
  // A triggered urgent exit supersedes a passive quote even when both watchers exist.
  for (const { rule, criteria } of parsedRules) {
    const intent = getSyntheticExitIntent(action, {}, criteria);
    if (['threat_management', 'roll_protection'].includes(intent)
      && Array.isArray(criteria.conditions) && criteria.conditions.length > 0
      && evaluateConditions(criteria.conditions, criteria.condition_logic || 'all', liveValues)) {
      return 'triggered urgent exit supersedes resting exit';
    }
  }
  for (const { rule, criteria } of parsedRules) {
    const intent = getSyntheticExitIntent(action, {}, criteria);
    if (!isSyntheticRestingExitIntentAllowed(action, {}, criteria)) continue;
    if (tracked?.exit_intent && tracked.exit_intent !== intent) continue;
    if (!Array.isArray(criteria.conditions) || criteria.conditions.length === 0
      || !evaluateConditions(criteria.conditions, criteria.condition_logic || 'all', plannedValues)) continue;
    const explicitLimit = Number(rule.limit_price || criteria.limit_price || 0);
    const cap = Number(criteria.max_buyback_price || 0);
    const floor = Number(criteria.min_exit_price || 0);
    if (action === 'buyback_call' && ((cap > 0 && limit > cap + 1e-9) || (explicitLimit > 0 && limit > explicitLimit + 1e-9))) continue;
    if (action === 'sell_put' && ((floor > 0 && limit < floor - 1e-9) || (explicitLimit > 0 && limit < explicitLimit - 1e-9))) continue;
    if (!getBuybackCaptureGate(rule, criteria, plannedValues).allowed) continue;
    if (action === 'sell_put') {
      // A standing floor is the desired sale price, not evidence that the hedge
      // still has a tail win. Reuse submission policy for fresh value proof and
      // the remaining tranche after fills or any other position-size changes.
      const validation = validateFinalOrderPolicy({
        action, instrumentName: order.instrument_name, price: limit, amount: remaining,
        orderType: tracked?.order_type || 'gtc', criteria, ticker,
        instrument: instruments.find(instrument => instrument.instrument_name === order.instrument_name),
        positions, spotPrice: Number(ticker.I) > 0 ? Number(ticker.I) : spotPrice,
        policy: getFinalOrderPolicy(order.instrument_name), now: Date.now(),
      });
      if (!validation.allowed) continue;
    }
    return null;
  }
  return 'resting exit no longer satisfies active exit intent, bounds, or protection requirements';
};

const manageOpenOrders = async (tickerMap, positions = [], instruments = [], spotPrice = 0) => {
  if (process.env.DRY_RUN === '1') return; // No real orders in dry run
  if (!db) return;
  const accounting = require('./bot/order-accounting');
  const resolved = await accounting.resolveAbandonedSubmission(db, { venueHasNonce });
  if (resolved) {
    console.log(`📋 Execution ${resolved.id} (nonce ${resolved.nonce}) never reached the venue — marked rejected`);
    sendTelegram(`♻️ *Execution ${resolved.id} reconciled*: venue has no order for nonce ${resolved.nonce}; trading resumes`);
  }
  accounting.assertNoUnresolvedSubmission(db);

  let openOrders;
  try {
    openOrders = await fetchOpenOrders({ throwOnError: true });
  } catch (e) {
    console.log(`📋 Open orders fetch failed: ${e.message}`);
    throw e;
  }

  // Unknown venue identity/status stops trading without releasing reserved capacity.
  let trackedResting = db.getOpenRestingOrders();
  const byId = new Map();
  for (const order of openOrders) {
    if (!order.order_id || byId.has(order.order_id)) throw new Error('Missing/duplicate venue order identity');
    if (!trackedResting.some(tracked => tracked.order_id === order.order_id)) {
      throw new Error(`Untracked venue order ${order.order_id}; reconcile before trading`);
    }
    byId.set(order.order_id, order);
  }
  for (const tracked of trackedResting) {
    const live = byId.get(tracked.order_id) || await fetchOrderStatus(tracked.order_id);
    if (!live) throw new Error(`Order ${tracked.order_id} status unknown; reservations retained`);
    if (byId.has(tracked.order_id) && !['open', 'untriggered'].includes(live.order_status)) {
      throw new Error('Terminal order in open-order snapshot; refresh required');
    }
    const result = require('./bot/order-accounting').accountRestingObservation({ db, botData, tracked, live });
    if (result.deltaAmount > 0 || !result.resting) notifyOrderLifecycle({
      stage: result.resting ? 'partial_fill' : live.filled_amount > 0 ? 'executed' : 'cancelled',
      action: tracked.action, instrumentName: tracked.instrument_name, amount: tracked.amount,
      filledAmount: result.deltaAmount, price: result.deltaAmount > 0 ? result.deltaValue / result.deltaAmount : tracked.limit_price,
      totalValue: result.deltaValue, orderId: tracked.order_id, status: live.order_status });
  }
  trackedResting = trackedResting.filter(order => order.status === 'open');

  if (openOrders.length === 0) return;

  console.log(`📋 ${openOrders.length} open order(s) on book`);

  // ── Stale/orphan cancellation ────────────────────────────────────────────
  const activeRules = db.getActiveRules();
  const activeExitInstruments = new Set(
    activeRules.filter(r => r.rule_type === 'exit').map(r => r.instrument_name).filter(Boolean)
  );
  // Entry rules match by criteria, not instrument. Check if any entry rule's action matches the order's direction.
  const activeEntryActions = new Set(activeRules.filter(r => r.rule_type === 'entry').map(r => r.action));
  let openOrderMarginState = null;
  const hasOpenSellCall = openOrders.some((order) => inferActionFromOpenOrder(order) === 'sell_call');
  if (hasOpenSellCall) {
    try { openOrderMarginState = await fetchSubaccount(); } catch { /* ok */ }
  }
  let openOrderBuyPutContext = null;
  const hasTrackedRestingBuyPut = openOrders.some((order) => {
    const tracked = trackedResting.find((item) => item.order_id === order.order_id);
    return tracked && inferActionFromOpenOrder(order, tracked) === 'buy_put';
  });
  if (hasTrackedRestingBuyPut) {
    let recentPendingActions = [];
    try { recentPendingActions = db.getRecentPendingActions(50); } catch { /* ok */ }
    openOrderBuyPutContext = buildRollingOptionValueContext({
      tickerMap,
      momentum: {
        mediumTerm: botData.mediumTermMomentum,
        shortTerm: botData.shortTermMomentum,
      },
      putBudgetRemaining: Math.max(0, botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought),
      activeRules,
      recentPendingActions,
      openRestingOrders: trackedResting,
      spotPrice,
    });
  }

  if (openOrders.some(order => ['buyback_call', 'sell_put'].includes(inferActionFromOpenOrder(order)))) {
    positions = await fetchPositions({ throwOnError: true });
  }

  for (const order of openOrders) {
    const ageMs = Date.now() - (order.creation_timestamp || 0);
    const ageHours = ageMs / (1000 * 60 * 60);
    const filled = Number(order.filled_amount || 0);
    const tracked = trackedResting.find((item) => item.order_id === order.order_id);
    const inferredAction = inferActionFromOpenOrder(order, tracked);

    // Tracked entries are reassessed every tick; age alone is not a reason to
    // discard their queue position. Economic and orphan checks still apply.
    const isStale = ageHours > 8 && !(tracked && isEntryAction(inferredAction));

    // Orphan check: is this order still backed by an active rule?
    const matchesExitRule = activeExitInstruments.has(order.instrument_name);
    const isBuyOrder = order.direction === 'buy';
    const matchesEntryAction = (isBuyOrder && order.instrument_name?.endsWith('-P') && activeEntryActions.has('buy_put'))
      || (!isBuyOrder && order.instrument_name?.endsWith('-C') && activeEntryActions.has('sell_call'))
      || (isBuyOrder && order.instrument_name?.endsWith('-C') && activeEntryActions.has('buyback_call'))
      || (!isBuyOrder && order.instrument_name?.endsWith('-P') && activeEntryActions.has('sell_put'));
    const isOrphaned = !matchesExitRule && !matchesEntryAction;
    let invalidRestingEntryReason = null;

    if (!isStale && !isOrphaned && tracked && inferredAction === 'sell_call') {
      const validation = validateRestingSellCallEntryOrder({
        order,
        activeRules,
        instruments,
        tickerMap,
        marginState: openOrderMarginState,
        positions,
        spotPrice,
      });
      if (!validation.valid) {
        invalidRestingEntryReason = `resting sell_call no longer satisfies active rule: ${validation.reason}`;
      }
    }
    if (!isStale && !isOrphaned && tracked && inferredAction === 'buy_put') {
      const validation = validateRestingBuyPutEntryOrder({
        order,
        activeRules,
        instruments,
        tickerMap,
        spotPrice,
        buyPutContext: openOrderBuyPutContext,
        putBudgetRemaining: Math.max(0, botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought),
      });
      if (!validation.valid) {
        invalidRestingEntryReason = `resting buy_put no longer satisfies active rule: ${validation.reason}`;
      }
    }

    const invalidRestingExitReason = getRestingExitInvalidReason({ order, tracked, activeRules, positions, instruments, tickerMap, spotPrice });

    // A valid patient exit remains useful across advisory renewal and elapsed
    // wall time. Changed desired terms are reviewed as replacements below;
    // elapsed age alone must not tear down an otherwise valid standing exit.
    const keepValidRestingExit = isReduceOnlyExitAction(inferredAction) && !invalidRestingExitReason;
    if ((isStale && !keepValidRestingExit) || isOrphaned || invalidRestingEntryReason || invalidRestingExitReason) {
      const reason = invalidRestingEntryReason || invalidRestingExitReason
        || (isStale ? `stale (${ageHours.toFixed(1)}h old)` : 'orphaned (no matching active rule)');
      console.log(`🗑️ Cancelling ${order.instrument_name} order ${order.order_id}: ${reason}`);
      const result = await cancelOrder(order.order_id, order.instrument_name);

      // A cancellation ACK is not a terminal fill receipt. Reconcile next tick.
      if (!result) throw new Error(`Cancellation of ${order.order_id} failed; reservation retained`);
    }
  }
};

const FAILED_ENTRY_ACTION_COOLDOWN_MS = 30 * 60 * 1000;
const formatCooldownMinutes = (ms) => `${Math.max(1, Math.ceil(ms / 60000))}m`;

const getRecentRejectedAction = (action, instrumentName, cooldownMs = REJECTED_ACTION_BACKOFF_MS) => {
  if (!db || !action || !instrumentName) return null;

  let lastRejected = null;
  if (typeof db.getLastRejectedAction === 'function') {
    lastRejected = db.getLastRejectedAction(action, instrumentName);
  } else if (typeof db.getRecentPendingActions === 'function') {
    lastRejected = db.getRecentPendingActions(100).find((item) =>
      item?.status === 'rejected'
      && item?.action === action
      && item?.instrument_name === instrumentName
    );
  }

  if (!lastRejected?.triggered_at) return null;
  const elapsed = Date.now() - new Date(lastRejected.triggered_at).getTime();
  if (elapsed < 0 || elapsed >= cooldownMs) return null;
  return {
    triggeredAt: lastRejected.triggered_at,
    elapsed,
    remaining: cooldownMs - elapsed,
    reason: lastRejected.confirmation_reasoning || lastRejected.execution_result || null,
  };
};

const getRecentFailedEntry = (action, instrumentName, cooldownMs = FAILED_ENTRY_ACTION_COOLDOWN_MS) => {
  if (!db || !action || !instrumentName) return null;
  const lastFailed = db.getLastFailedAction(action, instrumentName);
  if (!lastFailed?.triggered_at) return null;
  const reason = String(lastFailed.execution_result || '');
  if (reason.toLowerCase().includes('invalid limit price') && reason.toLowerCase().includes('decimal places')) {
    return null;
  }
  const elapsed = Date.now() - new Date(lastFailed.triggered_at).getTime();
  if (elapsed < 0 || elapsed >= cooldownMs) return null;
  return {
    triggeredAt: lastFailed.triggered_at,
    elapsed,
    reason: lastFailed.execution_result || null,
  };
};

// Nothing filled and nothing lost in either case — the book just moved. Not a cooldown blocker.
const isIocZeroFillFailure = (failure) => {
  const reason = String(failure?.reason || failure?.execution_result || '').toLowerCase();
  return reason.includes('zero fill (ioc)') || reason.includes('post_only rejected');
};

const formatRecentExecutionFrictionContext = (action, failure) => {
  const reason = failure?.reason || failure?.execution_result;
  if (!reason) return '';
  if (!isIocZeroFillFailure(failure)) {
    return `Recent execution friction on this exact instrument/action: ${reason}`;
  }

  const routingGuidance = action === 'buy_put'
    ? 'For buy_put entries, confirm when the current trigger, advisor limit, budget, and risk facts remain valid; use gtc/post_only at the approved limit when liquidity is sparse, and use ioc only when immediacy is worth another possible zero fill.'
    : isReduceOnlyExitAction(action)
      ? 'For exit actions, confirm when the exit rule remains valid; choose the allowed route for the exit intent, treating zero fill as price/liquidity feedback rather than a failed thesis.'
      : 'For entries, confirm when the current trigger and risk gates remain valid; use route selection to handle thin liquidity.';

  return [
    `Recent execution routing note for this exact instrument/action: ${reason}`,
    String(reason).toLowerCase().includes('post_only rejected')
      ? '- Prior post_only rejection means the book moved through the maker price between quote and submit. It is not a reviewer rejection, not a cooldown blocker, and not evidence that the active rule trigger is invalid.'
      : '- Prior IOC zero fill means no visible/matching liquidity accepted the limit at that instant. It is not a reviewer rejection, not a cooldown blocker, and not evidence that the active rule trigger is invalid.',
    `- ${routingGuidance}`,
  ].join('\n');
};

const adaptOrderTypeFromFailureHistory = (action, instrumentName, proposedOrderType) => {
  const validOrderTypes = getAllowedOrderTypesForAction(action);
  const normalized = validOrderTypes.includes(proposedOrderType) ? proposedOrderType : 'ioc';
  const recentFailed = getRecentFailedEntry(action, instrumentName);
  if (!recentFailed?.reason) return { orderType: normalized, note: null };

  const reason = String(recentFailed.reason || '').toLowerCase();
  if (normalized === 'ioc' && reason.includes('zero fill') && validOrderTypes.includes('gtc')) {
    return {
      orderType: 'gtc',
      note: 'recent ioc zero fill on this instrument; using gtc instead of ioc',
    };
  }

  return { orderType: normalized, note: null };
};

// ─── LLM-Driven Trading: Confirmation & Execution ───────────────────────────

const ACTION_POLICY = Object.freeze({
  buy_put: Object.freeze({
    phase: 'entry',
    direction: 'buy',
    reduceOnly: false,
    allowedOrderTypes: Object.freeze(['ioc', 'gtc', 'post_only']),
    semantics: 'Entry action: buying a put for tail-risk insurance. Bounded premium outlay, long convexity.',
  }),
  sell_call: Object.freeze({
    phase: 'entry',
    direction: 'sell',
    reduceOnly: false,
    allowedOrderTypes: Object.freeze(['ioc', 'gtc', 'post_only']),
    semantics: 'Entry action: selling a call to open short call exposure against ETH-collateralized account capacity.',
  }),
  sell_put: Object.freeze({
    phase: 'exit',
    direction: 'sell',
    reduceOnly: true,
    allowedOrderTypes: Object.freeze(['ioc', 'gtc', 'post_only']),
    semantics: 'Exit-only action: selling an already-owned long put to close or trim it. This must be reduce-only in effect and cannot create a naked short put.',
  }),
  buyback_call: Object.freeze({
    phase: 'exit',
    direction: 'buy',
    reduceOnly: true,
    allowedOrderTypes: Object.freeze(['ioc', 'gtc', 'post_only']),
    semantics: 'Exit-only action: buying back an already-open short call to close or trim it. This must be reduce-only in effect and cannot create a new long call exposure beyond the short being closed.',
  }),
});

const getActionPolicy = (action) => ACTION_POLICY[action] || null;
const describeActionSemantics = (action) => getActionPolicy(action)?.semantics || 'Trade semantics unavailable.';
const isReduceOnlyExitAction = (action) => Boolean(getActionPolicy(action)?.reduceOnly);
const isEntryAction = (action) => getActionPolicy(action)?.phase === 'entry';
const getAllowedOrderTypesForAction = (action) => getActionPolicy(action)?.allowedOrderTypes || ['ioc', 'gtc', 'post_only'];

const ENTRY_ACTIONS = Object.freeze(Object.keys(ACTION_POLICY).filter((action) => ACTION_POLICY[action].phase === 'entry'));
const EXIT_ACTIONS = Object.freeze(Object.keys(ACTION_POLICY).filter((action) => ACTION_POLICY[action].phase === 'exit'));
const ENTRY_ALLOWED_ORDER_TYPES = Object.freeze([...new Set(ENTRY_ACTIONS.flatMap((action) => ACTION_POLICY[action].allowedOrderTypes))]);
const EXIT_ALLOWED_ORDER_TYPES = Object.freeze([...new Set(EXIT_ACTIONS.flatMap((action) => ACTION_POLICY[action].allowedOrderTypes))]);

const formatOrderTypeList = (orderTypes) => orderTypes.map((orderType) => `"${orderType}"`).join(' or ');
const getActionOrderTypeHardRule = (action) => {
  const policy = getActionPolicy(action);
  if (!policy) return '- HARD RULE: use a valid order type for the action.';
  if (action === 'sell_put') {
    return `- HARD RULE: sell_put is an exit-only close/trim of owned long puts. Use exchange reduce_only IOC for roll_protection or urgency. For monetize_tail_win only, gtc/post_only is allowed as synthetic reduce-only: live position must still be long, size must be capped to live closeable amount, and only one exit order may rest per instrument. An explicit replacement is allowed only after the old order is confirmed terminal and all its fills are reconciled; it never adds a second live exit.`;
  }
  if (action === 'buyback_call') {
    return `- HARD RULE: buyback_call is an exit-only close/trim of open short calls. Use exchange reduce_only IOC for threat_management or urgency. For profit_capture only, gtc/post_only is allowed as synthetic reduce-only: live position must still be short, size must be capped to live closeable amount, and only one exit order may rest per instrument. An explicit replacement is allowed only after the old order is confirmed terminal and all its fills are reconciled; it never adds a second live exit.`;
  }
  if (policy.phase === 'exit') {
    return `- HARD RULE: if action is ${EXIT_ACTIONS.join(' or ')}, this is an exit-only close/trim action. Resting exits are allowed only when the synthetic reduce-only guard passes.`;
  }
  return `- HARD RULE: if action is ${ENTRY_ACTIONS.join(' or ')}, this is an entry. Entry actions are not reduce_only exits, and may validly use ${formatOrderTypeList(ENTRY_ALLOWED_ORDER_TYPES)} when patient pricing is preferable.`;
};

const getSharedActionPolicyPrompt = () => [
  `EXIT SEMANTICS: ${describeActionSemantics('sell_put')} ${describeActionSemantics('buyback_call')}`,
  `ORDER-TYPE RULE: Derive rejects exchange reduce_only resting orders. IOC exits stay exchange reduce_only. Resting sell_put/buyback_call exits are allowed only as synthetic reduce-only in narrow patient cases: buyback_call profit_capture and sell_put monetize_tail_win. Synthetic exits must have live closeable position, one open exit order max per instrument, and amount capped to the live closeable amount.`,
  `ENTRY ORDER-TYPE RULE: ${ENTRY_ACTIONS.join(' and ')} are entry actions, not reduce_only exits. Resting order types like gtc and post_only are valid for entries when patience and pricing matter.`,
  getBuyPutEntryPriceDisciplinePrompt(),
].join('\n');

const getMomentumEvidenceDisciplinePrompt = () => [
  'EVIDENCE PRIORITY: This is an options bang-for-buck strategy, not a price-direction strategy.',
  '- Primary evidence: executable bid/ask, spread/depth, IV/skew, DTE, delta, moneyness, OI, funding, candidate score, position PnL, hedge role, and account margin impact.',
  '- Secondary evidence: short-term and medium-term momentum. Use momentum as timing and path-risk context only after option economics justify attention.',
  '- Do not create, preserve, or justify a rule mainly because momentum is upward or downward. Momentum must be confirmed by option pricing, liquidity, skew, OI, funding, or position-specific risk.',
  '- If momentum conflicts with option-market structure, trust executable option economics first and state the mismatch plainly.',
].join('\n');

const getBuyPutEntryPriceDisciplinePrompt = () => [
  'BUY-PUT PRICE DISCIPLINE:',
  '- Rank eligible puts directly by DTE-normalized PUT EDGE. Judge premium against the approved min_score/target_score using fresh delta, DTE, and the proposed executable bid, not the headline IV or the live ask when a cheaper patient bid is proposed.',
  '- IV, skew, spread, OI, crowd sentiment, and 30/40/50% shock scenarios are descriptive diagnostics. High or spiked IV alone does not establish overpayment and cannot add a standalone PUT veto, multiplier, or alternative ranking.',
  '- Ignore legacy min_edge_score and historical composite recommendations. Do not invent another PUT quality threshold or replace the approved price contract with a volatility judgment.',
  '- Keep concrete active-rule, delta/DTE, live-quote, venue-price, quantity, budget, margin, and liquidation checks. A violation of those constraints can reject an entry; diagnostic IV or shock-payoff commentary cannot.',
].join('\n');

const getFreshBestBuyPutDisciplinePrompt = () => [
  'FRESH-BEST BUY-PUT DISCIPLINE:',
  `- The ROLLING OPTION VALUE CONTEXT compares the live buy-put score against the prior ${ADVISORY_OPTION_VALUE_WINDOW_DAYS}d window using buy-put DTE discipline (${BUY_PUT_ADVISORY_DTE_RANGE[0]}-${BUY_PUT_ADVISORY_DTE_RANGE[1]} DTE). A strict fresh best means the highest observed DTE-normalized delta per premium dollar in that window.`,
  `- PUT EDGE is the only ranking score: (abs(delta) / ask) * (DTE / ${BUY_PUT_EDGE_REFERENCE_DTE})^${BUY_PUT_EDGE_DTE_EXPONENT}. Normalize each eligible candidate before ranking; higher PUT EDGE wins. It measures DTE-normalized delta per premium dollar, not literal crash payoff.`,
  '- Do not treat higher delta as a standalone buy-put advantage. Keep the approved delta/DTE ranges and rank eligible puts directly by PUT EDGE.',
  '- Spread, IV/skew, OI, and 30/40/50% drawdown payoff scenarios are descriptive diagnostics. Do not apply another multiplier, reorder candidates, or create a standalone veto from them. Keep concrete live-quote, price, size, budget, margin, and active-rule checks.',
  '- Legacy min_edge_score on saved buy_put rules is ignored; do not emit it or use it as a ranking, confirmation, or execution gate.',
  '- The spot-lag repricing check catches a different cheap-convexity window: spot has dropped and the put score has locally jumped or moved near the rolling best before asks fully recalibrate.',
  `- The recent-relative value check catches a local value window versus the last ${BUY_PUT_RECENT_VALUE_LOOKBACK_HOURS}h, but it is weaker than a ${ADVISORY_OPTION_VALUE_WINDOW_DAYS}d fresh best because the local window may simply be less bad. Use stricter min_score/target_score, budget discipline, and a concrete reason it is true value rather than locally expensive insurance.`,
  '- If requires_buy_put_decision=yes, explicitly evaluate whether to emit a buy_put rule or explain why patience/no-buy is still the better stance. This is a value signal, not an instruction to override discipline.',
  '- Standing buy_put watchers may use criteria.value_signal="any_actionable_buy_put" to let the executor catch strict_fresh_best, spot_drop_option_repricing_lag, recent_relative_value, or a patient maker bid at the rule price without waiting for a new advisory. Signal absence means quote patiently at the score-derived price; it does not mean no possible bid. Use min_score and target_score to keep this wildcard disciplined.',
  '- If value_signal is present, still include option_type, delta_range, dte_range, budget_limit, and min_score/target_score bounds so the watcher respects the approved universe, price and spend constraints.',
  '- If signal=spot_drop_option_repricing_lag, the edge may vanish quickly; prefer ioc or gtc with the supplied near-live target_score instead of a deeply patient post_only bid.',
  '- If signal=recent_relative_value, prefer post_only or gtc with the supplied target_score unless other facts show urgency.',
  '- If you choose buy_put and spot price action is downward, use less_patient_limit pricing: set criteria.target_score to the supplied target_score and prefer "gtc" or "post_only".',
  '- If you choose buy_put and spot price action is stable, use patient_limit pricing: set criteria.target_score to the supplied target_score and prefer "post_only" or "gtc".',
  `- target_score means a maximum bid of abs(delta) * (DTE / ${BUY_PUT_EDGE_REFERENCE_DTE})^${BUY_PUT_EDGE_DTE_EXPONENT} / target_score. The executor uses this cap and the live book to compute a tick-aligned price. It may rest below the ask; for spot-lag repricing it may be near the ask.`,
].join('\n');

const getStandingRulebookDisciplinePrompt = () => [
  'STANDING RULEBOOK DISCIPLINE:',
  '- The advisory output is a standing rulebook for tick-by-tick execution, not only a list of trades that should execute at the current tick.',
  '- Every REQUIRED STANDING RULEBOOK COVERAGE item must have a corresponding rule in the final agenda. If the favorable condition is not true now, encode the condition that would make it favorable later.',
  '- buy_put rules define a price/score where insurance is worth buying while put budget remains. Rank directly by PUT EDGE and use min_score/target_score as the hard price contract. Do not emit min_edge_score for buy_put; legacy values on saved rules are ignored. Spread, IV/skew, OI, and shock scenarios do not add another ranking or veto.',
  `- sell_call rules must include min_score for CALL EDGE, where CALL EDGE = (bid / abs(delta)) * (${SELL_CALL_EDGE_REFERENCE_DTE} / DTE)^${SELL_CALL_EDGE_DTE_EXPONENT}. This light correction reduces the mechanical Sunday jump when the available expiry range rolls forward.`,
  '- min_bid is the executable premium/liquidity floor. min_score is compared with CALL EDGE; do not emit min_edge_score for sell_call.',
  '- If the sell_call thesis is patience or margin is near its cap, encode that stance through a stricter min_score/min_bid, lower priority, or narrower delta/DTE range. The written reasoning and JSON trigger must not contradict each other.',
  '- sell_call market_conditions may only use spot_price as optional supporting context. Express other selectivity through min_score, min_bid, delta_range, dte_range, priority, and reasoning.',
  '- Do not use broad spot_price floors as a proxy for sell_call recovery, stability, or good premium. After a sharp drop, spot can remain above a floor while call selling is still unattractive.',
  `- sell_call rules must stay inside the normal call sale universe: ${CALL_EXPIRATION_RANGE[0]}-${CALL_EXPIRATION_RANGE[1]} DTE and ${CALL_DELTA_RANGE[0]}-${CALL_DELTA_RANGE[1]} delta.`,
  `- sell_put rules must declare put_exit_intent. Use "roll_protection" only when DTE <= ${PUT_ROLL_DTE_THRESHOLD} and the book already holds longer-dated long puts; roll_protection may close the aging instrument fully because the replacement protection is already on. Use "monetize_tail_win" only when executable unrealized_pnl_pct > ${PUT_MONETIZATION_PROFIT_THRESHOLD}; set retain_downside_protection=true, tranche_fraction <= ${PUT_MONETIZATION_MAX_TRANCHE_FRACTION}, and min_exit_price/limit_price. For monetization, never sell all downside protection at once or dump into sparse crash bids.`,
  '- buyback_call rules must declare buyback_intent. Use "profit_capture" for 80%+ executable capture or a patient synthetic reduce-only resting limit whose price would achieve that capture. Use "threat_management" only for genuine short-call danger; price rising alone is not enough.',
  '- Do not omit a required watcher just because it is not currently triggered. Tighten criteria instead.',
].join('\n');

const getCallMarginDisciplinePrompt = () => `CALL DISCIPLINE: Short calls normally target ${(CALL_EXPOSURE_CAP_PCT * 100).toFixed(0)}% inferred Derive margin utilization. The ${(CALL_EXPOSURE_BUFFER_PCT * 100).toFixed(0)} percentage point buffer up to ${(CALL_EXPOSURE_LIMIT_PCT * 100).toFixed(0)}% is last-mile execution safety for estimate drift, not planned sell-call capacity. ${(CALL_ENTRY_CAP_PCT * 100).toFixed(0)}% is a caution threshold, not an automatic rejection line. New entries must stay at or below the active target cap after sizing; at-or-below means <= and equality at the cap is allowed. Do not create dust orders merely to use the execution buffer. When spot is breaking upward and short calls are already on, the active target can widen to ${(CALL_BREAKOUT_OVERRIDE_CAP_PCT * 100).toFixed(0)}% with a buffered limit of ${(CALL_BREAKOUT_OVERRIDE_LIMIT_PCT * 100).toFixed(0)}% only if margin context explicitly shows breakout_override=active. These are discipline limits for new entries, not margin-emergency thresholds. Reject call sells that exceed the active target cap, exceed the buffered limit, lack buying power, or are too small to matter.`;

const getCallBuybackDisciplinePrompt = () => `CALL BUYBACK DISCIPLINE: For buyback_call, keep two intents separate. Intent 1 is profit/capacity reset while the short call is winning: use buyback_intent="profit_capture" and executable unrealized_pnl_pct >= ${CALL_BUYBACK_PROFIT_THRESHOLD}% as the economic trigger, or set a patient max_buyback_price/target_capture_pct where the bid would capture at least ${CALL_BUYBACK_PROFIT_THRESHOLD}% if filled. Do not add DTE or mark_price blockers for this intent; executable capture already uses the live buyback ask. Intent 2 is threat management when the short call is genuinely dangerous and time/range for recovery is running out: use buyback_intent="threat_management" with allow_below_profit_floor=true, and conditions on real threat facts such as delta, spot vs strike, and remaining DTE. Do not prematurely buy back just because price is rising; spot can come back down, and buying back fear premium can make us the sucker of the trade. The short call premium is already collected; mark expansion alone does not erase that. A buyback below strike is paying to remove tail risk of further upside continuation. Confirm or create buybacks only when the position is genuinely threatened, assignment risk is credible, the insurance cost is justified by actual breakout evidence, or an advisor-led take-profit rule names patient pricing. If live executable buyback price already implies strictly better capture than a profit-capture rule, do not bid back up to the threshold. Never confirm a threshold-style buyback when live market price is unavailable. Treat margin context as sizing/redeployment context, not a standalone buyback trigger.`;

const getPutExitDisciplinePrompt = () => `PUT EXIT DISCIPLINE: For sell_put, judge whether rolling or monetizing an owned long hedge is sensible. Never treat sell_put as opening naked short put exposure. Selling an owned long put returns cash and reduces the hedge position, but removing hedge offsets may worsen portfolio margin; assess the remaining portfolio. Use put_exit_intent="roll_protection" only when DTE <= ${PUT_ROLL_DTE_THRESHOLD} and the book already holds enough later-expiry long puts at the same or higher strike to replace the quantity sold. For roll_protection, a full close of the aging instrument is allowed, negative PnL is not a rejection reason, and monetize_tail_win tranche/profit thresholds do not apply because replacement protection is already in the book. Use put_exit_intent="monetize_tail_win" only when executable unrealized_pnl_pct is greater than ${PUT_MONETIZATION_PROFIT_THRESHOLD}; set retain_downside_protection=true, sell in tranches with tranche_fraction <= ${PUT_MONETIZATION_MAX_TRANCHE_FRACTION}, and name min_exit_price/limit_price as the minimum acceptable sell price. For monetization, never sell all protection at once. In severe crash markets, do not undersell a valuable put just because visible bids are sparse; if making the market, choose a responsible floor from intrinsic value, Greeks, IV/skew, spread/depth, DTE, and remaining hedge role. Monetization is a ladder (${PUT_MONETIZATION_LADDER_PCT.join('% / ')}%): each tranche requires a larger multiple and the final remainder is held to settlement, so a tranche that meets its rung is the plan working, not premature. Your discretion is the offer price: read the book, mark, intrinsic and IV, and name the price we are willing to sell this tranche at — we make the market, we do not hit thin bids — never below ${(PUT_MONETIZATION_MIN_INTRINSIC_FRACTION * 100).toFixed(0)}% of intrinsic. If you reject a sell_put, do it because the typed intent's requirements fail, never because the market might go further.`;

const normalizeLearningText = (value) => String(value || '').toLowerCase();

const confirmationLessonMatches = (lesson, includeKeywords = [], excludeKeywords = []) => {
  const text = normalizeLearningText([
    lesson?.lesson_key,
    lesson?.title,
    lesson?.lesson || lesson?.summary,
  ].filter(Boolean).join(' '));
  if (!text) return false;
  if (excludeKeywords.some((keyword) => text.includes(keyword))) return false;
  return includeKeywords.some((keyword) => text.includes(keyword));
};

const getConfirmationLearningScope = (action) => {
  switch (action) {
    case 'sell_call':
      return {
        includeReviews: false,
        reviewFamily: 'short_call_campaign',
        include: ['sell_call', 'sell call', 'selling calls', 'call premium', 'short call entry', 'strike selection', 'premium collection'],
        exclude: ['buyback', 'buy back', 'bought back', 'exit timing', 'close short call', 'closing short call'],
        note: 'sell_call is an entry. Buyback-call exit timing lessons are not valid vetoes for a fresh sell_call entry; use live bid/score/margin facts and the active sell_call rule.',
      };
    case 'buy_put':
      return {
        includeReviews: false,
        reviewFamily: 'long_put_campaign',
        include: ['buy_put', 'buy put', 'buying puts', 'put entry', 'insurance', 'protection cost', 'cheap convexity'],
        exclude: ['sell_put', 'sell put', 'roll_protection', 'roll protection', 'monetize', 'tail win'],
        note: 'buy_put is an entry. Put-exit/monetization lessons are not valid vetoes for a fresh buy_put entry; use live value-signal, score, budget, and rule facts.',
      };
    case 'buyback_call':
      return {
        includeReviews: true,
        reviewFamily: 'short_call_campaign',
        include: ['buyback', 'buy back', 'bought back', 'short call', 'call exit', 'exit timing', 'expiry payoff', 'mark pain'],
        exclude: ['buy_put', 'buy put', 'sell_put', 'sell put'],
        note: 'buyback_call is a short-call exit. Short-call campaign reviews and buyback timing lessons may inform this last-mile exit check.',
      };
    case 'sell_put':
      return {
        includeReviews: true,
        reviewFamily: 'long_put_campaign',
        include: ['sell_put', 'sell put', 'selling put', 'long put', 'put exit', 'roll_protection', 'roll protection', 'monetize', 'tail win', 'downside protection'],
        exclude: ['sell_call', 'sell call', 'buyback', 'buy back'],
        note: 'sell_put is a long-put exit. Long-put campaign reviews and put-exit lessons may inform this last-mile exit check, scoped to the typed put_exit_intent.',
      };
    default:
      return null;
  }
};

const formatConfirmationLearningContext = (action, recentTradeReviews = [], activeTradeLessons = []) => {
  const scope = getConfirmationLearningScope(action);
  if (!scope) return '';

  const relevantReviews = scope.includeReviews
    ? (recentTradeReviews || []).filter((review) => review?.action_family === scope.reviewFamily).slice(0, 3)
    : [];
  const relevantLessons = (activeTradeLessons || [])
    .filter((lesson) => confirmationLessonMatches(lesson, scope.include, scope.exclude))
    .slice(0, 3);

  const omittedCount = Math.max(0, (recentTradeReviews?.length || 0) - relevantReviews.length)
    + Math.max(0, (activeTradeLessons?.length || 0) - relevantLessons.length);

  return [
    'Action-scoped learning context:',
    `- ${scope.note}`,
    `- Omitted unrelated confirmation memories: ${omittedCount}. Do not import omitted lessons or reviews into this action's decision.`,
    relevantReviews.length > 0
      ? `Relevant recent trade reviews:\n${relevantReviews.map((r) => `- ${r.instrument_name} [${r.review_status}] [${r.review_window_days}d]: ${r.summary}`).join('\n')}`
      : '- Relevant recent trade reviews: none for this action scope.',
    relevantLessons.length > 0
      ? `Relevant active trade lessons:\n${relevantLessons.map(formatTradeLessonForPrompt).join('\n')}`
      : '- Relevant active trade lessons: none for this action scope.',
  ].join('\n');
};

const getConfirmationJsonOnlyPrompt = () => 'Return EXACTLY one single-line JSON object. No markdown fences. No prose before or after. Keep reasoning to at most 40 words; name the decisive execution fact without restating the full analysis.';

const normalizePreferredOrderType = (action, preferredOrderType) => {
  if (typeof preferredOrderType !== 'string') return null;
  const normalized = preferredOrderType.trim().toLowerCase();
  if (!normalized) return null;
  return getAllowedOrderTypesForAction(action).includes(normalized) ? normalized : null;
};

const formatPostOnlyContext = ({ attemptedPrice, retryPrice = null, bidPrice = 0, askPrice = 0, step = 0, reason = null }) => {
  const parts = [
    `attempted=$${Number(attemptedPrice || 0).toFixed(4)}`,
    `bid=$${Number(bidPrice || 0).toFixed(4)}`,
    `ask=$${Number(askPrice || 0).toFixed(4)}`,
  ];
  if (retryPrice != null) parts.push(`retry=$${Number(retryPrice).toFixed(4)}`);
  if (step > 0) parts.push(`step=$${step.toFixed(4)}`);
  if (reason) parts.push(`exchange=${reason}`);
  return parts.join(', ');
};

const executeOrder = async (action, instrumentName, amount, price, instruments, spotPrice, orderType = 'ioc', tickerMap = {}, pendingActionId = null, executionContext = {}) => {
  const insertExecutionOrder = (payload) => {
    if (!db) return;
    db.insertOrder({
      ...payload,
      pending_action_id: payload.pending_action_id ?? pendingActionId,
    });
  };

  // DRY_RUN mode: track budget discipline but skip actual order
  if (process.env.DRY_RUN === '1') {
    const totalValue = amount * price;
    if (action === 'buy_put') botData.putNetBought += totalValue;
    persistCycleState();
    insertExecutionOrder({
      action, success: true, reason: `DRY RUN: simulated ${action} (${orderType})`,
      instrument_name: instrumentName, strike: null, expiry: null,
      delta: null, price, intended_amount: amount,
      filled_amount: amount, fill_price: price,
      total_value: totalValue, spot_price: spotPrice, raw_response: `{"dryRun":true,"orderType":"${orderType}"}`,
    });
    console.log(`🔸 DRY RUN: ${action} ${amount} ${instrumentName} @ $${price} [${orderType}] | $${totalValue.toFixed(2)} (put budget: $${botData.putNetBought.toFixed(2)})`);
    sendTelegram(`🧪 *DRY RUN: ${action.toUpperCase()}* ${instrumentName} @ $${price}`);
    return { dryRun: true, action, instrumentName, amount, price, totalValue, orderType };
  }

  // Determine direction and reduceOnly from action type
  const policy = getActionPolicy(action);
  const direction = policy?.direction || 'sell';
  const bound = direction === 'buy' ? executionContext.approvedBounds?.buyCeiling : executionContext.approvedBounds?.sellFloor;
  const approvedPrice = Number(bound) > 0
    ? (direction === 'buy' ? Math.min(Number(price), Number(bound)) : Math.max(Number(price), Number(bound)))
    : Number(price);
  price = approvedPrice;
  let reduceOnly = Boolean(policy?.reduceOnly);

  if (isReduceOnlyExitAction(action) && isRestingOrderType(orderType)) {
    let livePositions;
    let openRestingOrders = db ? db.getOpenRestingOrders() : [];
    try {
      livePositions = await fetchPositions({ throwOnError: true });
      const venueOpenOrders = await fetchOpenOrders({ throwOnError: true });
      openRestingOrders = [...openRestingOrders, ...venueOpenOrders];
    } catch (error) {
      const reason = `Synthetic reduce-only guard blocked resting ${action}: unable to verify current positions and venue open orders (${error.message})`;
      console.error(`❌ ${reason}`);
      insertExecutionOrder({ action, success: false, reason, instrument_name: instrumentName, spot_price: spotPrice, price, intended_amount: amount });
      return { failed: true, reason };
    }
    const syntheticPreflight = getSyntheticReduceOnlyPreflight({
      action,
      instrumentName,
      amount,
      orderType,
      triggerData: executionContext.triggerData || {},
      ruleCriteria: executionContext.ruleCriteria || {},
      positions: livePositions,
      restingOrders: openRestingOrders,
    });

    if (!syntheticPreflight.allowed) {
      const reason = `Synthetic reduce-only guard blocked resting ${action}: ${syntheticPreflight.reason}`;
      console.error(`❌ ${reason}`);
      insertExecutionOrder({ action, success: false, reason, instrument_name: instrumentName, spot_price: spotPrice, price, intended_amount: amount });
      return { failed: true, reason };
    }

    reduceOnly = syntheticPreflight.reduceOnly;
    if (syntheticPreflight.synthetic) {
      if (Math.abs(Number(amount) - syntheticPreflight.amount) > 1e-9) {
        console.log(`📋 Synthetic reduce-only amount cap: ${action} ${instrumentName} ${Number(amount).toFixed(4)} -> ${syntheticPreflight.amount.toFixed(4)}`);
      }
      amount = syntheticPreflight.amount;
      console.log(`📋 Synthetic reduce-only resting exit approved: ${action} ${instrumentName} amount=${amount.toFixed(4)} [${orderType}] (${syntheticPreflight.reason})`);
    }
  }

  if (!(Number(price) > 0)) {
    const reason = `Invalid execution price for ${action}: ${price}`;
    console.error(`❌ ${reason}`);
    insertExecutionOrder({ action, success: false, reason, instrument_name: instrumentName, spot_price: spotPrice });
    return { failed: true, reason };
  }

  // Find instrument to get base_asset_address and base_asset_sub_id
  const instrument = instruments.find(i => i.instrument_name === instrumentName);
  if (!instrument) {
    const reason = `Instrument ${instrumentName} not found during execution`;
    console.error(`❌ ${reason}`);
    return { failed: true, reason };
  }

  const addr = instrument.base_asset_address;
  const subId = instrument.base_asset_sub_id;
  let ticker = tickerMap?.[instrumentName];
  if (orderType === 'post_only') {
    const freshTicker = await fetchFreshTickerForInstrument(instrumentName);
    if (freshTicker) {
      ticker = freshTicker;
    }
    const step = getInstrumentPriceStep(instrument, Number(price));
    const offRoundPrice = avoidRoundNumberRestingPrice(direction, Number(price), step);
    if (ticker) {
      const retryPlan = computePostOnlyRetryPrice(direction, ticker, instrument, price);
      const bidPrice = Number(ticker?.b) || 0;
      const askPrice = Number(ticker?.a) || 0;
      const guaranteedCross = (direction === 'buy' && askPrice > 0 && price >= askPrice)
        || (direction === 'sell' && bidPrice > 0 && price <= bidPrice);
      if (guaranteedCross && retryPlan && Math.abs(retryPlan.retryPrice - price) > 1e-9) {
        console.log(`📋 pre-adjusting post_only ${action} ${instrumentName} from $${price} to maker-safe $${retryPlan.retryPrice} (bid=$${bidPrice.toFixed(4)} ask=$${askPrice.toFixed(4)})`);
        price = retryPlan.retryPrice;
      } else if (Math.abs(offRoundPrice - Number(price)) > 1e-9) {
        console.log(`📋 nudging post_only ${action} ${instrumentName} away from round number $${price} -> $${offRoundPrice}`);
        price = offRoundPrice;
      }
    } else if (Math.abs(offRoundPrice - Number(price)) > 1e-9) {
      console.log(`📋 nudging post_only ${action} ${instrumentName} away from round number $${price} -> $${offRoundPrice}`);
      price = offRoundPrice;
    }
  }

  const venueAmount = floorOrderAmountToVenuePrecision(amount);
  if (!isVenueOrderAmountTradable(venueAmount)) {
    const reason = `Order amount ${Number(amount).toFixed(4)} normalizes to ${venueAmount.toFixed(VENUE_AMOUNT_DECIMALS)}, not above venue minimum ${VENUE_MIN_ORDER_AMOUNT}`;
    console.log(`📋 Skip ${action} ${instrumentName}: ${reason}`);
    insertExecutionOrder({
      action,
      success: false,
      reason,
      instrument_name: instrumentName,
      spot_price: spotPrice,
      price,
      intended_amount: amount,
    });
    return { failed: true, reason };
  }
  amount = venueAmount;

  const venuePrice = normalizeOrderPriceForVenue(price, instrument, direction);
  if (Math.abs(venuePrice.price - Number(price)) > 1e-9) {
    console.log(`📋 normalized ${action} ${instrumentName} limit $${price} -> $${venuePrice.price} (${direction}, step=$${venuePrice.step})`);
    price = venuePrice.price;
  }

  const validateSubmission = async (candidatePrice) => {
    const normalized = normalizeOrderPriceForVenue(candidatePrice, instrument, direction).price;
    if (!(normalized > 0) || !Number.isFinite(normalized)
      || (direction === 'buy' && normalized > approvedPrice + 1e-9)
      || (direction === 'sell' && normalized < approvedPrice - 1e-9)) {
      throw new Error('Normalized order violates approved price bound');
    }
    if (executionContext.validateOrder) {
      const validation = await executionContext.validateOrder({ price: normalized, amount, ticker });
      if (validation === false || validation?.allowed === false || validation?.valid === false) {
        throw new Error(validation?.reason || 'Final execution policy rejected order');
      }
    }
    if (isReduceOnlyExitAction(action)) {
      // An IOC close also conflicts with a synthetic resting exit: filling the
      // IOC first could leave that older quote able to open a reverse position.
      // Refresh after policy reads, on both the initial attempt and maker retry.
      const venueOpenOrders = await fetchOpenOrders({ throwOnError: true });
      const trackedOpenOrders = db ? db.getOpenRestingOrders() : [];
      const blockingExit = getExistingRestingExitOrder(action, instrumentName, [...trackedOpenOrders, ...venueOpenOrders]);
      if (blockingExit) throw new Error(`Existing exit ${blockingExit.order_id || 'unknown'} must be terminal and reconciled before submitting ${action}`);
    }
    return normalized;
  };
  try { price = await validateSubmission(price); } catch (error) {
    return { failed: true, reason: error.message };
  }
  const accounting = require('./bot/order-accounting');
  accounting.assertNoUnresolvedSubmission(db);
  let submissionId = null;
  const beforeSend = (request) => { submissionId = accounting.beginSubmission(db, pendingActionId, { ...request,
    account_address: DERIVE_ACCOUNT_ADDRESS, action,
    exit_intent: getSyntheticExitIntent(action, executionContext.triggerData || {}, executionContext.ruleCriteria || {}),
    approved_limit_price: approvedPrice }); };
  const observeSubmission = (receipt) => {
    const definitive = Boolean(receipt?.zero_fill_rejected || receipt?.rejected_post_only || receipt?.definitive_rejection);
    accounting.noteSubmission(db, submissionId, receipt, definitive);
    if (submissionId != null && (!receipt || receipt.placement_error) && !definitive) {
      throw new Error('Order placement outcome unknown; durable intent retained for reconciliation');
    }
  };
  let order;
  try {
    order = await placeOrder(
      instrumentName,
      formatVenueOrderAmount(amount),
      direction,
      price,
      addr,
      subId,
      reduceOnly,
      orderType,
      instrument,
      beforeSend
    );
  } catch (error) {
    console.error(`❌ Error placing ${action} order for ${instrumentName}:`, error.message);
    if (submissionId != null) throw error;
    const reason = `Order error: ${error.message}`;
    insertExecutionOrder({ action, success: false, reason, instrument_name: instrumentName, spot_price: spotPrice });
    return { failed: true, reason };
  }

  observeSubmission(order);

  if (!order) {
    const reason = 'Order placement failed';
    insertExecutionOrder({ action, success: false, reason, instrument_name: instrumentName, spot_price: spotPrice });
    return { failed: true, reason };
  }

  if (order.zero_fill_rejected) {
    console.log(`⚠️ Zero fill: ${action} ${instrumentName} @ $${price} [IOC] — no liquidity inside limit`);
    insertExecutionOrder({
      action, success: false, reason: `Zero fill (IOC) — no matching orders at $${price}`,
      instrument_name: instrumentName,
      strike: instrument.option_details?.strike || null,
      expiry: instrument.option_details?.expiry || null,
      delta: null, price, intended_amount: amount,
      filled_amount: 0, fill_price: null,
      total_value: 0, spot_price: spotPrice,
      raw_response: order,
    });
    return { zeroFill: true, action, instrumentName, amount, price, orderType };
  }

  if (order.placement_error) {
    const reason = `Venue rejected order: ${order.placement_error}`;
    insertExecutionOrder({ action, success: false, reason, instrument_name: instrumentName, spot_price: spotPrice, price, intended_amount: amount });
    return { failed: true, reason };
  }

  // Detect post_only rejection (order would cross the book)
  if (order.rejected_post_only) {
    let retryTicker = ticker;
    let retryPlan = orderType === 'post_only' ? computePostOnlyRetryPrice(direction, retryTicker, instrument, price) : null;
    if (orderType === 'post_only') {
      const refreshedRetryTicker = await fetchFreshTickerForInstrument(instrumentName);
      if (refreshedRetryTicker) {
        retryTicker = refreshedRetryTicker;
        retryPlan = computePostOnlyRetryPrice(direction, retryTicker, instrument, price);
      }
    }
    const initialContext = formatPostOnlyContext({
      attemptedPrice: price,
      retryPrice: retryPlan?.retryPrice ?? null,
      bidPrice: retryPlan?.bidPrice ?? retryTicker?.b ?? 0,
      askPrice: retryPlan?.askPrice ?? retryTicker?.a ?? 0,
      step: retryPlan?.step ?? getInstrumentPriceStep(instrument, price),
      reason: order.error || null,
    });

    let retryMarginCheck = { allowed: true, reason: null };
    if (orderType === 'post_only' && retryPlan && action === 'sell_call') {
      retryMarginCheck = await evaluateSellCallRetryMargin({
        instrumentName,
        amount,
        retryPrice: retryPlan.retryPrice,
        instruments,
        spotPrice,
      });
    }

    if (orderType === 'post_only' && retryPlan && Math.abs(retryPlan.retryPrice - price) > 1e-9 && retryMarginCheck.allowed) {
      console.log(`📋 post_only rejected: ${action} ${instrumentName} @ $${price} — retrying maker at $${retryPlan.retryPrice} (${initialContext})`);
      let retryOrder = null;
      submissionId = null;
      try {
        ticker = retryTicker;
        retryPlan.retryPrice = await validateSubmission(retryPlan.retryPrice);
        retryOrder = await placeOrder(
          instrumentName,
          amount.toFixed(2),
          direction,
          retryPlan.retryPrice,
          addr,
          subId,
          reduceOnly,
          orderType,
          instrument,
          beforeSend
        );
      } catch (error) {
        console.error(`❌ Error retrying ${action} order for ${instrumentName}:`, error.message);
      }

      observeSubmission(retryOrder);
      if (retryOrder && !retryOrder.rejected_post_only && !retryOrder.placement_error && !retryOrder.zero_fill_rejected) {
        order = retryOrder;
        price = retryPlan.retryPrice;
      } else if (retryOrder?.placement_error) {
        const reason = `Venue rejected maker retry: ${retryOrder.placement_error}`;
        insertExecutionOrder({ action, success: false, reason, instrument_name: instrumentName, spot_price: spotPrice, price: retryPlan.retryPrice, intended_amount: amount });
        return { failed: true, reason };
      } else {
        const finalContext = formatPostOnlyContext({
          attemptedPrice: price,
          retryPrice: retryPlan.retryPrice,
          bidPrice: retryPlan.bidPrice,
          askPrice: retryPlan.askPrice,
          step: retryPlan.step,
          reason: retryOrder?.error || retryOrder?.placement_error || order.error || null,
        });
        console.log(`📋 post_only retry failed: ${action} ${instrumentName} — ${finalContext}`);
        insertExecutionOrder({
          action, success: false,
          reason: `post_only rejected after maker retry: ${finalContext}`,
          instrument_name: instrumentName, spot_price: spotPrice,
          price, intended_amount: amount,
        });
        return {
          postOnlyRejected: true,
          postOnlyBlocked: false,
          action,
          instrumentName,
          amount,
          price,
          orderType,
          context: finalContext,
          retryPrice: retryPlan.retryPrice,
        };
      }
    } else if (orderType === 'post_only' && retryPlan && !retryMarginCheck.allowed) {
      const blockedContext = `${initialContext}, margin_guard=${retryMarginCheck.reason}`;
      console.log(`📋 post_only retry blocked: ${action} ${instrumentName} — ${blockedContext}`);
      insertExecutionOrder({
        action, success: false,
        reason: `post_only retry blocked by margin guard: ${blockedContext}`,
        instrument_name: instrumentName, spot_price: spotPrice,
        price, intended_amount: amount,
      });
      return {
        postOnlyRejected: true,
        postOnlyBlocked: true,
        action,
        instrumentName,
        amount,
        price,
        orderType,
        context: blockedContext,
        retryPrice: retryPlan.retryPrice,
      };
    } else {
      console.log(`📋 post_only rejected: ${action} ${instrumentName} @ $${price} — no maker retry available (${initialContext})`);
      insertExecutionOrder({
        action, success: false,
        reason: `post_only rejected without retry: ${initialContext}`,
        instrument_name: instrumentName, spot_price: spotPrice,
        price, intended_amount: amount,
      });
      return { postOnlyRejected: true, postOnlyBlocked: false, action, instrumentName, amount, price, orderType, context: initialContext };
    }
  }

  const orderRecord = extractOrderRecord(order.result || order);
  const orderTrades = getOrderTrades(order.result || order);
  const result = require('./bot/order-accounting').accountInitialReceipt({
    db, botData, action, instrumentName, amount, price, orderType, pendingActionId,
    instrument, spotPrice, order, record: orderRecord, trades: orderTrades,
    exitIntent: getSyntheticExitIntent(action, executionContext.triggerData || {}, executionContext.ruleCriteria || {}),
    approvedLimitPrice: approvedPrice, submissionId,
  });
  notifyOrderLifecycle({ stage: result.resting ? 'posted' : result.filledAmt > 0 ? 'executed' : 'cancelled',
    action, instrumentName, amount, filledAmount: result.filledAmt || 0, price,
    totalValue: result.totalValue || 0, orderType, orderId: orderRecord.order_id, status: orderRecord.order_status });
  return result;
};

const getFinalOrderPolicy = (instrumentName = null) => ({
  putDeltaRange: PUT_DELTA_RANGE,
  putDteRange: PUT_EXPIRATION_RANGE,
  callDeltaRange: CALL_DELTA_RANGE,
  callDteRange: CALL_EXPIRATION_RANGE,
  sellCallMinScore: SELL_CALL_FALLBACK_MIN_SCORE,
  sellCallMinBid: SELL_CALL_FALLBACK_MIN_BID,
  callCapturePct: CALL_BUYBACK_PROFIT_THRESHOLD,
  putRollDte: PUT_ROLL_DTE_THRESHOLD,
  putRollMinRecoveryPct: PUT_ROLL_MIN_RECOVERY_PCT,
  putMonetizationPct: instrumentName ? getPutMonetizationThresholdPct(instrumentName) : PUT_MONETIZATION_PROFIT_THRESHOLD,
  putMinIntrinsicFraction: PUT_MONETIZATION_MIN_INTRINSIC_FRACTION,
  putMaxTrancheFraction: PUT_MONETIZATION_MAX_TRANCHE_FRACTION,
});

const createFinalOrderValidator = ({ action, instruments, spotPrice, triggerData, ruleCriteria, orderType, tickerMap = {} }) => async ({ price, amount }) => {
  try {
    // Refresh after reviewer latency and again for any maker retry. Reads are
    // independent; no stored trigger value authorizes a later submission.
    const [ticker, marginState, positions] = await Promise.all([
      fetchFreshTickerForInstrument(action.instrument_name),
      fetchSubaccount(),
      fetchPositions({ throwOnError: true }),
    ]);
    const activeRule = db.getActiveRules().find(rule => Number(rule.id) === Number(action.rule_id));
    if (!activeRule || activeRule.action !== action.action
      || JSON.stringify(parseMaybeJsonObject(activeRule.criteria)) !== JSON.stringify(ruleCriteria)) {
      return { allowed: false, code: 'inactive_rule', reason: 'The approved rule is no longer active or has changed' };
    }
    const currentSpot = Number(ticker?.I) > 0 ? Number(ticker.I) : spotPrice;
    const currentRestingOrders = db.getOpenRestingOrders();
    const callMarginDecision = getCallMarginDecision(action.action, marginState, positions, currentRestingOrders,
      instruments, currentSpot, action.instrument_name, amount, price);
    const reservedPutBudget = summarizeReservedEntryCapacity(currentRestingOrders).putBudget;
    const putBudgetRemaining = botData.putBudgetForCycle > 0
      ? botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought - reservedPutBudget
      : null;
    const validation = validateFinalOrderPolicy({
      action: action.action, instrumentName: action.instrument_name, price, amount, orderType,
      criteria: ruleCriteria, triggerData, ticker,
      instrument: instruments.find(instrument => instrument.instrument_name === action.instrument_name),
      positions, spotPrice: currentSpot, marginState, callMarginDecision, putBudgetRemaining,
      ruleBudgetLimit: activeRule.budget_limit, policy: getFinalOrderPolicy(action.instrument_name), now: Date.now(),
    });
    if (validation.allowed && isEntryAction(action.action)) {
      // This callback runs immediately before every submission and maker retry.
      // Reservations remain blocking until their terminal fills are reconciled.
      const venueOrders = await fetchOpenOrders({ throwOnError: true });
      if ([...db.getOpenRestingOrders(), ...venueOrders].some(order => inferActionFromOpenOrder(order) === action.action)) {
        return { allowed: false, code: 'existing_entry', reason: 'An entry for this action remains live or unreconciled' };
      }
    }
    return validation;
  } catch (error) {
    return { allowed: false, code: 'fresh_state_unavailable', reason: `Unable to verify final order: ${error.message}` };
  }
};

const getEntryOrderFilledValue = (order) => {
  const filled = Number(order?.filled_amount);
  if (!Number.isFinite(filled) || filled < 0) throw new Error('Entry cumulative quantity is unavailable');
  if (filled === 0) return 0;
  const value = order.filled_value != null ? Number(order.filled_value) : filled * Number(order.average_price);
  if (!Number.isFinite(value) || !(value > 0)) throw new Error('Entry cumulative value is unavailable');
  return value;
};

const accountEntryOrderObservation = (tracked, live) => {
  const result = require('./bot/order-accounting').accountRestingObservation({ db, botData, tracked, live });
  if (result.deltaAmount > 0) notifyOrderLifecycle({ stage: result.resting ? 'partial_fill' : 'executed',
    action: tracked.action, instrumentName: tracked.instrument_name, amount: tracked.amount,
    filledAmount: result.deltaAmount, price: result.deltaValue / result.deltaAmount,
    totalValue: result.deltaValue, orderId: tracked.order_id, status: live.order_status });
  return result;
};

const readFreshEntryOrderSnapshot = async (action) => {
  if (!isEntryAction(action.action)) throw new Error('Entry snapshot requires an entry action');
  const relevant = order => inferActionFromOpenOrder(order) === action.action;
  const trackedOrders = db.getOpenRestingOrders().filter(relevant);
  const venueOrders = (await fetchOpenOrders({ throwOnError: true })).filter(relevant);
  const byId = new Map();
  for (const order of venueOrders) {
    if (!order.order_id || byId.has(order.order_id) || !['open', 'untriggered'].includes(order.order_status)) {
      throw new Error('Entry order snapshot is inconsistent');
    }
    if (!trackedOrders.some(tracked => tracked.order_id === order.order_id)) {
      throw new Error(`Untracked entry ${order.order_id}; reconcile before replacing`);
    }
    byId.set(order.order_id, order);
  }
  const orders = [];
  for (const tracked of trackedOrders) {
    const live = byId.get(tracked.order_id) || await fetchOrderStatus(tracked.order_id);
    if (!live) throw new Error(`Entry ${tracked.order_id} status unknown; reservations retained`);
    const result = accountEntryOrderObservation(tracked, live);
    if (result.resting) {
      if (!byId.has(tracked.order_id)) throw new Error('Entry book changed during snapshot; refresh required');
      orders.push({ ...tracked, ...live, action: tracked.action, filled_value: result.filledValue });
    }
  }
  return { orders, observedAt: new Date().toISOString() };
};

const validateEntryReplacementEconomics = ({ action, activeRule, triggerData, desiredOrder, reviewedOrder,
  ticker, tickerMap = {}, marginState, positions, instruments, spotPrice }) => {
  if (!hasUsableMarginState(marginState) || marginState.is_under_liquidation || Number(marginState.initial_margin) < 0) {
    return { allowed: false, reason: 'Fresh usable margin is required before reviewing replacement' };
  }
  const criteria = parseMaybeJsonObject(activeRule.criteria);
  const candidate = { ...desiredOrder, filled_amount: 0 };
  const rules = [{ ...activeRule, criteria: { ...criteria,
    ...(action.action === 'buy_put' ? { target_score: Math.max(Number(criteria.target_score) || 0, Number(triggerData.target_score) || 0) } : {}) } }];
  const common = { order: candidate, activeRules: rules, instruments,
    tickerMap: { [action.instrument_name]: ticker }, spotPrice, marginState, positions };
  let validation;
  let research = null;
  if (action.action === 'buy_put') {
    if (!(Number(ticker?.a) > 0)) return { allowed: false, reason: 'Fresh put ask is unavailable' };
    const otherReservations = summarizeReservedEntryCapacity(db.getOpenRestingOrders()
      .filter(order => order.order_id !== reviewedOrder.order_id)).putBudget;
    validation = validateRestingBuyPutEntryOrder({ ...common,
      putBudgetRemaining: botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought - otherReservations });
    if (validation.valid && !validation.unchecked) {
      let marketOiDelta24hPct = null;
      if (typeof db.getOpenInterestHourly === 'function') {
        try { marketOiDelta24hPct = getLatestHourlyDeltaPct(
          db.getOpenInterestHourly(new Date(Date.now() - 30 * 60 * 60 * 1000).toISOString()), 24); } catch { /* optional research context */ }
      }
      const marketContext = buildLiveSellCallMarketContext({ ...tickerMap, [action.instrument_name]: ticker }, Date.now(),
        { market_oi_delta_24h_pct: marketOiDelta24hPct });
      const instrument = instruments.find(item => item.instrument_name === action.instrument_name);
      research = classifyBuyPutEdge({ score: validation.score, rawScore: validation.rawScore,
        dte: validation.dte, strike: Number(instrument?.option_details?.strike), spotPrice,
        entryPrice: desiredOrder.limit_price, askPrice: Number(ticker.a), bidPrice: Number(ticker.b),
        askAmount: Number(ticker.A), bidAmount: Number(ticker.B), markPrice: Number(ticker.M),
        impliedVol: getTickerImpliedVol(ticker), marketContext });
    }
  } else {
    if (!(Number(ticker?.b) > 0)) return { allowed: false, reason: 'Fresh call bid is unavailable' };
    validation = validateRestingSellCallEntryOrder(common);
    if (validation.valid && !validation.unchecked) validation = validateRestingSellCallEntryOrder({ ...common,
      order: { ...candidate, limit_price: Number(ticker.b) } });
  }
  return { allowed: Boolean(validation?.valid && !validation.unchecked),
    reason: validation?.reason || 'Replacement economics unavailable', validation, research };
};

const prepareEntryOrderReplacement = async ({ action, reviewedOrder, desiredOrder, approvedNotional = null, instrument = null }) => {
  if (!reviewedOrder?.order_id || reviewedOrder.action !== action.action
    || reviewedOrder.instrument_name !== action.instrument_name || desiredOrder.instrument_name !== action.instrument_name) {
    return { allowed: false, reason: 'Replacement entry identity conflicts with the reviewed order' };
  }
  const reviewedFilled = Number(reviewedOrder.filled_amount);
  const reviewedValue = getEntryOrderFilledValue(reviewedOrder);
  const remainingApproved = (current) => {
    const fills = Number(current.filled_amount) - reviewedFilled;
    const value = getEntryOrderFilledValue(current) - reviewedValue;
    if (fills < -1e-9 || value < -1e-7) throw new Error('Entry fills regressed during replacement');
    let amount = Math.max(0, Number(desiredOrder.amount) - Math.max(0, fills));
    if (action.action === 'buy_put') {
      if (!Number.isFinite(Number(approvedNotional)) || !(Number(approvedNotional) > 0)) throw new Error('Entry replacement notional authorization unavailable');
      amount = Math.min(amount, Math.max(0, Number(approvedNotional) - Math.max(0, value)) / Number(desiredOrder.limit_price));
    }
    const amountStep = Math.max(Number(instrument?.options?.amount_step ?? instrument?.amount_step) || 0.01, 0.01);
    return floorOrderAmountToVenuePrecision(Math.floor((amount + 1e-9) / amountStep) * amountStep);
  };
  const fresh = await readFreshEntryOrderSnapshot(action);
  if (fresh.orders.some(order => order.order_id !== reviewedOrder.order_id)) {
    return { allowed: false, reason: 'Another entry appeared during review; replacement deferred' };
  }
  const current = fresh.orders.find(order => order.order_id === reviewedOrder.order_id);
  if (current) {
    const comparison = require('./bot/resting-entry-plan').compareRestingEntryOrder(current,
      { ...desiredOrder, amount: remainingApproved(current) });
    if (comparison.decision === 'keep') return { allowed: false, reason: 'Equivalent entry already resting; keep it' };
    if (comparison.decision === 'unresolved') return { allowed: false, reason: 'Existing entry terms are unresolved' };
    await cancelOrder(current.order_id, current.instrument_name);
  }
  const terminal = await fetchOrderStatus(reviewedOrder.order_id);
  if (!terminal) return { allowed: false, reason: 'Entry cancellation status unknown; reservation retained' };
  const accounting = accountEntryOrderObservation({ ...reviewedOrder }, terminal);
  if (accounting.resting) return { allowed: false, reason: 'Entry remains open; replacement deferred' };
  const amount = remainingApproved({ ...terminal, filled_value: accounting.filledValue });
  if (!isVenueOrderAmountTradable(amount)) return { allowed: false, reason: 'No tradable approved entry remainder remains after fills' };
  if ((await readFreshEntryOrderSnapshot(action)).orders.length) return { allowed: false, reason: 'An entry remains on the venue; replacement deferred' };
  return { allowed: true, amount };
};

const accountExitOrderObservation = (tracked, live) => {
  const result = require('./bot/order-accounting').accountRestingObservation({ db, botData, tracked, live });
  if (result.deltaAmount > 0) notifyOrderLifecycle({
    stage: result.resting ? 'partial_fill' : 'executed', action: tracked.action,
    instrumentName: tracked.instrument_name, amount: tracked.amount, filledAmount: result.deltaAmount,
    price: result.deltaValue / result.deltaAmount, totalValue: result.deltaValue,
    orderId: tracked.order_id, status: live.order_status,
  });
  return result;
};

const readFreshExitOrderSnapshot = async (action) => {
  const direction = getActionPolicy(action.action)?.direction;
  const relevant = order => order.instrument_name === action.instrument_name
    && (order.action === action.action || order.direction === direction);
  const trackedOrders = db.getOpenRestingOrders().filter(relevant);
  const venueOrders = (await fetchOpenOrders({ throwOnError: true })).filter(relevant);
  const byId = new Map();
  for (const order of venueOrders) {
    if (!order.order_id || byId.has(order.order_id)
      || !['open', 'untriggered'].includes(order.order_status)) throw new Error('Exit order snapshot is inconsistent');
    if (!trackedOrders.some(tracked => tracked.order_id === order.order_id)) {
      throw new Error(`Untracked exit order ${order.order_id}; reconcile before confirmation`);
    }
    byId.set(order.order_id, order);
  }
  const orders = [];
  for (const tracked of trackedOrders) {
    const live = byId.get(tracked.order_id) || await fetchOrderStatus(tracked.order_id);
    if (!live) throw new Error(`Exit order ${tracked.order_id} status unknown; confirmation deferred`);
    const result = accountExitOrderObservation(tracked, live);
    if (result.resting) {
      if (!byId.has(tracked.order_id)) throw new Error('Exit order appeared after snapshot; refresh required');
      const origin = typeof db.getPendingActionById === 'function' ? db.getPendingActionById(tracked.pending_action_id) : null;
      const originalTrigger = parseMaybeJsonObject(origin?.trigger_details);
      const originalCriteria = parseMaybeJsonObject(origin?.rule_criteria);
      const originalIntent = tracked.action === 'buyback_call'
        ? originalTrigger?.buyback_intent || originalCriteria?.buyback_intent
        : originalTrigger?.put_exit_intent || originalCriteria?.put_exit_intent;
      orders.push({ ...tracked, ...live, exit_intent: tracked.exit_intent || originalIntent || null,
        action: tracked.action, tranche_fraction: originalTrigger?.tranche_fraction ?? originalCriteria?.tranche_fraction ?? null });
    }
  }
  return { orders, observedAt: new Date().toISOString() };
};

const formatExitConfirmationSnapshot = (action, position, snapshot, plan) => {
  const orders = snapshot.orders.map(order => ({
    order_id: order.order_id, action: order.action, direction: order.direction,
    limit_price: Number(order.limit_price), remaining_amount: Number(order.amount) - Number(order.filled_amount),
    order_type: order.time_in_force || order.order_type, exit_intent: order.exit_intent,
  }));
  return [
    `AUTHORITATIVE CURRENT EXIT STATE (venue checked ${snapshot.observedAt}):`,
    `Live closeable position: ${position.direction} ${Number(position.amount)} ${action.instrument_name}; average entry price=$${position.avg_entry_price}.`,
    `Current same-instrument exit orders on the venue: ${orders.length}. ${JSON.stringify(orders)}`,
    `Concrete desired exit order: ${JSON.stringify(plan)}`,
    orders.length
      ? 'Operation: REPLACE the listed order, subject to confirmation. The executor must prove its cancellation/terminal status, reconcile all fills, refresh closeable quantity and revalidate before submitting the replacement. There will be no overlapping exit orders. Review the replacement economics; the listed order is not a proposed duplicate.'
      : 'Operation: PLACE a new exit. No existing exit is currently resting. Historical rule prose cannot establish an existing order.',
  ].join('\n');
};

const prepareExitOrderReplacement = async ({ action, reviewedOrders, desiredOrder }) => {
  const fresh = await readFreshExitOrderSnapshot(action);
  const reviewedIds = new Set(reviewedOrders.map(order => order.order_id));
  if (fresh.orders.some(order => !reviewedIds.has(order.order_id))) {
    return { allowed: false, reason: 'Exit book changed during review; re-evaluate before replacing' };
  }
  let fillsSinceReview = 0;
  for (const reviewed of reviewedOrders) {
    const current = fresh.orders.find(order => order.order_id === reviewed.order_id);
    if (current) {
      const currentFills = Math.max(0, Number(current.filled_amount) - Number(reviewed.filled_amount));
      const remainingApproved = Math.max(0, Number(desiredOrder.amount) - fillsSinceReview - currentFills);
      const comparison = compareRestingExitOrder(current, { ...desiredOrder, amount: remainingApproved });
      if (comparison.decision === 'keep') return { allowed: false, reason: 'Equivalent exit is already resting; keep it' };
      if (comparison.decision === 'unresolved') return { allowed: false, reason: 'Existing exit terms are unresolved' };
      // A successful cancellation request alone never authorizes replacement.
      await cancelOrder(current.order_id, current.instrument_name);
    }
    const terminal = await fetchOrderStatus(reviewed.order_id);
    if (!terminal) return { allowed: false, reason: `Cancellation status unknown for ${reviewed.order_id}` };
    const result = accountExitOrderObservation({ ...reviewed }, terminal);
    if (result.resting) return { allowed: false, reason: `Exit ${reviewed.order_id} is still open; replacement deferred` };
    fillsSinceReview += Math.max(0, Number(terminal.filled_amount) - Number(reviewed.filled_amount));
  }
  // Check the book again after cancellation before reading the remaining position.
  const after = await readFreshExitOrderSnapshot(action);
  if (after.orders.length) return { allowed: false, reason: 'Exit order remains on the venue; replacement deferred' };
  const positions = await fetchPositions({ throwOnError: true });
  const position = getCloseablePositionForExit(action.action, action.instrument_name, positions);
  const amount = floorOrderAmountToVenuePrecision(Math.min(Number(position?.amount) || 0,
    Math.max(0, Number(desiredOrder.amount) - fillsSinceReview)));
  if (!(amount > 0)) return { allowed: false, reason: 'No approved closeable quantity remains after fills' };
  return { allowed: true, amount, positions };
};

const confirmAndExecutePending = async (instruments, tickerMap, spotPrice) => {
  if (!db) return;

  const pending = db.getPendingActions('pending');
  if (pending.length === 0) return;

  console.log(`🔍 ${pending.length} pending action(s) to confirm`);

  let executed = 0;
  let resting = 0;
  for (const action of pending.slice(0, 2)) { // Max 2 per tick
    const activeRule = db.getActiveRules().find(rule => Number(rule.id) === Number(action.rule_id));
    if (!activeRule || activeRule.action !== action.action
      || JSON.stringify(parseMaybeJsonObject(activeRule.criteria)) !== JSON.stringify(parseMaybeJsonObject(action.rule_criteria))) {
      db.updatePendingAction(action.id, { status: 'cancelled', confirmation_reasoning: 'Obsolete pending action: rule is inactive or changed', execution_result: 'inactive_rule' });
      continue;
    }
    // Fetch fresh margin state for each confirmation (margin changes between trades)
    let marginState = null;
    let livePositions = [];
    try {
      [marginState, livePositions] = await Promise.all([fetchSubaccount(), fetchPositions({ throwOnError: true })]);
    } catch (error) {
      console.log(`📋 Defer ${action.action} ${action.instrument_name}: fresh account state unavailable (${error.message})`);
      continue;
    }
    if (isEntryAction(action.action) && !hasUsableMarginState(marginState)) {
      console.log(`📋 Defer ${action.action} ${action.instrument_name}: fresh margin state unavailable`);
      continue;
    }
    const restingOrders = db.getOpenRestingOrders();
    const activeTradeLessons = db.getActiveTradeLessons();
    const recentTradeReviews = db.getRecentTradeReviews(3);
    const displayedMarginUtilization = estimateDisplayedMarginUtilization(marginState);
    const marginStr = marginState
      ? `Margin: buying_power=$${marginState.initial_margin.toFixed(2)}, account_value=$${marginState.subaccount_value.toFixed(2)}, collateral=$${marginState.collaterals_value.toFixed(2)}, derive_display_utilization=${displayedMarginUtilization != null ? `${(displayedMarginUtilization * 100).toFixed(1)}%` : 'N/A'}${marginState.is_under_liquidation ? ' [UNDER LIQUIDATION]' : ''}`
      : 'Margin: unavailable';
    try {
      // Auto-reject after 3 retries
      if (action.retries >= 3) {
        db.updatePendingAction(action.id, { status: 'rejected', confirmation_reasoning: 'Auto-rejected after 3 failed confirmation attempts' });
        console.log(`❌ Auto-rejected: ${action.action} ${action.instrument_name} (3 retries)`);
        continue;
      }

      // Hard safety: reject new entries if under liquidation
      if (marginState?.is_under_liquidation && isEntryAction(action.action)) {
        db.updatePendingAction(action.id, { status: 'rejected', confirmation_reasoning: 'Auto-rejected: account under liquidation' });
        console.log(`🚨 Auto-rejected ${action.action} ${action.instrument_name}: account under liquidation`);
        sendTelegram(`🚨 *LIQUIDATION WARNING* — auto-rejected ${action.action} ${action.instrument_name}`);
        continue;
      }

      // Build context for confirmation
      const ticker = await fetchFreshTickerForInstrument(action.instrument_name);
      if (!ticker) {
        console.log(`📋 Defer ${action.action} ${action.instrument_name}: fresh ticker unavailable`);
        continue;
      }
      const requiredQuote = action.action === 'sell_call' ? ticker.b : ticker.a;
      if (action.action !== 'sell_put' && !(Number(requiredQuote) > 0)) {
        console.log(`📋 Defer ${action.action} ${action.instrument_name}: fresh executable quote unavailable`);
        continue;
      }
      const liveMarketPrice = ticker ? (action.action.includes('buy') ? Number(ticker.a) : Number(ticker.b)) : null;
      const currentPrice = liveMarketPrice;
      const momentum = botData.mediumTermMomentum;

      // Parse trigger details for advisory's preferred order type and value context.
      let triggerData = {};
      try { triggerData = typeof action.trigger_details === 'string' ? JSON.parse(action.trigger_details) : (action.trigger_details || {}); } catch {}
      const ruleCriteria = parseMaybeJsonObject(action.rule_criteria);
      let exitSnapshot = null;
      let exitOrderPlan = null;
      let entrySnapshot = null;
      let entryReviewedOrder = null;
      let entryOrderPlan = null;
      let entryApprovedNotional = null;
      if (isEntryAction(action.action) && triggerData.entry_replacement === true) {
        const sourceOrder = triggerData.replacement_order_snapshot;
        if (!sourceOrder?.order_id || sourceOrder.order_id !== triggerData.replacement_order_id
          || sourceOrder.instrument_name !== action.instrument_name || sourceOrder.action !== action.action) {
          throw new Error('Original entry replacement authority is incomplete');
        }
        entrySnapshot = await readFreshEntryOrderSnapshot(action);
        if (entrySnapshot.orders.some(order => order.order_id !== sourceOrder.order_id)) {
          console.log(`📋 Defer entry replacement ${action.instrument_name}: another entry is active`);
          continue;
        }
        const liveOrder = entrySnapshot.orders.find(order => order.order_id === sourceOrder.order_id);
        entryReviewedOrder = liveOrder;
        if (!entryReviewedOrder) {
          const terminal = await fetchOrderStatus(sourceOrder.order_id);
          if (!terminal) throw new Error('Original entry state is unknown');
          const accounted = accountEntryOrderObservation({ ...sourceOrder }, terminal);
          if (accounted.resting) throw new Error('Entry book changed during replacement review');
          entryReviewedOrder = { ...sourceOrder, ...terminal, filled_value: accounted.filledValue };
        }
        const fillsBeforeReview = Number(entryReviewedOrder.filled_amount) - Number(sourceOrder.filled_amount);
        const valueBeforeReview = getEntryOrderFilledValue(entryReviewedOrder) - getEntryOrderFilledValue(sourceOrder);
        if (fillsBeforeReview < -1e-9 || valueBeforeReview < -1e-7) throw new Error('Entry accounting regressed before review');
        const quantityAuthority = Math.max(0, Math.min(Number(action.amount) - Math.max(0, fillsBeforeReview),
          Number(entryReviewedOrder.amount) - Number(entryReviewedOrder.filled_amount)));
        if (!isVenueOrderAmountTradable(floorOrderAmountToVenuePrecision(quantityAuthority))) {
          db.updatePendingAction(action.id, { status: 'cancelled', execution_result: 'No approved entry remainder remains' });
          continue;
        }
        const module = require('./bot/resting-entry-plan');
        const instrument = instruments.find(item => item.instrument_name === action.instrument_name);
        const otherReserved = summarizeReservedEntryCapacity(db.getOpenRestingOrders()
          .filter(order => order.order_id !== sourceOrder.order_id)).putBudget;
        const planned = module.buildRestingEntryPlan({ order: entryReviewedOrder, rule: activeRule, instrument, ticker,
          spotPrice: Number(ticker.I) > 0 ? Number(ticker.I) : spotPrice,
          putBudgetRemaining: botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought,
          otherPutReserved: otherReserved,
          effectiveTargetScore: Math.max(Number(ruleCriteria.target_score) || 0, Number(triggerData.target_score) || 0) });
        if (!planned.desiredOrder || ['invalid', 'unresolved'].includes(planned.decision)) {
          console.log(`📋 Defer entry replacement ${action.instrument_name}: ${planned.reason}`);
          continue;
        }
        let amount = Math.min(quantityAuthority, planned.desiredOrder.amount);
        if (action.action === 'buy_put') {
          const originalNotional = Number(triggerData.entry_approved_notional);
          if (!(originalNotional > 0)) throw new Error('Original put replacement notional is unavailable');
          entryApprovedNotional = Math.max(0, originalNotional - Math.max(0, valueBeforeReview));
          amount = Math.min(amount, entryApprovedNotional / planned.desiredOrder.limit_price);
        }
        entryOrderPlan = module.normalizeDesiredEntryOrder({ action: action.action, instrumentName: action.instrument_name,
          amount, price: planned.desiredOrder.limit_price, orderType: planned.desiredOrder.order_type, instrument, ticker });
        if (action.action === 'buy_put') entryApprovedNotional = Math.min(entryApprovedNotional, entryOrderPlan.amount * entryOrderPlan.limit_price);
        const economics = validateEntryReplacementEconomics({ action, activeRule, triggerData, desiredOrder: entryOrderPlan,
          reviewedOrder: entryReviewedOrder, ticker, tickerMap, marginState, positions: livePositions, instruments,
          spotPrice: Number(ticker.I) > 0 ? Number(ticker.I) : spotPrice });
        if (!economics.allowed) {
          console.log(`📋 Defer entry replacement ${action.instrument_name}: ${economics.reason}`);
          continue;
        }
        const comparison = liveOrder ? module.compareRestingEntryOrder(liveOrder, entryOrderPlan) : null;
        if (comparison?.decision === 'keep') {
          db.updatePendingAction(action.id, { status: 'cancelled', confirmation_reasoning: 'Equivalent entry already resting; kept without reviewer calls' });
          continue;
        }
        if (comparison?.decision === 'unresolved') continue;
        action.amount = entryOrderPlan.amount;
        action.price = entryOrderPlan.limit_price;
        triggerData = { ...triggerData, desired_entry_order: entryOrderPlan,
          replacement_order_snapshot: entryReviewedOrder, entry_approved_notional: entryApprovedNotional,
          advisor_limit_price: entryOrderPlan.limit_price, preferred_order_type: entryOrderPlan.order_type,
          delta: Number(ticker.option_pricing?.d), dte: computeDteFromInstrumentName(action.instrument_name),
          raw_score: economics.validation?.rawScore ?? null, score: economics.validation?.score ?? null,
          planned_score: economics.validation?.score ?? null,
          selection_score: economics.research?.selection_score ?? economics.validation?.score ?? null,
          selection_metric: action.action === 'buy_put' ? 'put_edge_v1' : 'call_edge_v1',
          selection_price_basis: 'planned_limit',
          buy_put_research: action.action === 'buy_put' ? economics.research : null,
          ...(action.action === 'buy_put' ? {
            live_score: normalizeBuyPutScore(Math.abs(Number(ticker.option_pricing?.d)) / Number(ticker.a),
              computeDteFromInstrumentName(action.instrument_name)),
            buy_put_signal: 'standing_patient_bid',
          } : {}),
          live_price: liveMarketPrice,
          target_score: planned.requiredScore };
      }
      if (isReduceOnlyExitAction(action.action)) {
        exitSnapshot = await readFreshExitOrderSnapshot(action);
        // Read after reconciling the order snapshot so intervening fills also
        // reduce the position used for sizing and reviewer context.
        livePositions = await fetchPositions({ throwOnError: true });
      }
      const livePosition = isReduceOnlyExitAction(action.action)
        ? getCloseablePositionForExit(action.action, action.instrument_name, livePositions)
        : null;
      if (isReduceOnlyExitAction(action.action) && !livePosition) {
        db.updatePendingAction(action.id, { status: 'cancelled', confirmation_reasoning: 'No live closeable position remains' });
        continue;
      }
      if (isReduceOnlyExitAction(action.action)) {
        const currentValues = liveRuleValues(livePosition, ticker, spotPrice);
        const livePatientBuyback = action.action === 'buyback_call'
          ? refinePatientBuybackPlanPrice(getPatientBuybackPlan(activeRule, ruleCriteria, livePosition), ticker,
              instruments.find(instrument => instrument.instrument_name === action.instrument_name))
          : null;
        const livePatientPut = action.action === 'sell_put'
          ? getPatientSellPutPlan(activeRule, ruleCriteria, livePosition, currentValues)
          : null;
        triggerData = {
          ...triggerData,
          conditions_met: (ruleCriteria.conditions || []).map(condition => ({ field: condition.field,
            op: condition.op, threshold: condition.value, actual: currentValues[condition.field] })),
          replacement_order_id: exitSnapshot.orders[0]?.order_id ?? null,
          replacement_order_snapshot: exitSnapshot.orders[0] ?? null,
          advisor_limit_price: livePatientBuyback?.limitPrice ?? livePatientPut?.limitPrice ?? null,
          preferred_order_type: normalizePreferredOrderType(action.action, activeRule.preferred_order_type),
          buyback_intent: action.action === 'buyback_call' ? getBuybackIntent(ruleCriteria) : null,
          put_exit_intent: action.action === 'sell_put' ? getPutExitIntent(ruleCriteria) : null,
          current_values: {
            ...currentValues,
            patient_buyback_capture_pct: livePatientBuyback?.capturePct ?? null,
            patient_buyback_limit_price: livePatientBuyback?.limitPrice ?? null,
            patient_buyback_ceiling_price: livePatientBuyback?.ceilingPrice ?? null,
            patient_sell_put_pnl_pct: livePatientPut?.pnlPct ?? null,
            patient_sell_put_fair_value_pnl_pct: livePatientPut?.fairValuePnlPct ?? null,
          },
          patient_buyback_capture_pct: livePatientBuyback?.capturePct ?? null,
          patient_buyback_ceiling_price: livePatientBuyback?.ceilingPrice ?? null,
          patient_buyback_price_reason: livePatientBuyback?.priceReason ?? null,
          patient_sell_put_pnl_pct: livePatientPut?.pnlPct ?? null,
          patient_sell_put_limit_price: livePatientPut?.limitPrice ?? null,
          patient_sell_put_fair_value_pnl_pct: livePatientPut?.fairValuePnlPct ?? null,
          patient_sell_put_fair_value_price: livePatientPut?.fairValuePrice ?? null,
          patient_sell_put_fair_value_source: livePatientPut?.fairValueSource ?? null,
        };
        const policyExitAmount = action.action === 'sell_put'
          ? getSellPutExitAmount(activeRule, ruleCriteria, livePosition, currentValues) : Number(livePosition.amount);
        const desiredTrancheFraction = action.action === 'sell_put' && triggerData.put_exit_intent === 'monetize_tail_win'
          ? policyExitAmount / Number(livePosition.amount) : null;
        const remainingExitAmount = desiredTrancheFraction != null
          ? getDesiredSellPutRemainingAmount({ positionAmount: livePosition.amount,
              desiredFraction: desiredTrancheFraction, existingOrder: exitSnapshot.orders[0] }) : policyExitAmount;
        triggerData.tranche_fraction = desiredTrancheFraction;
        action.amount = floorOrderAmountToVenuePrecision(Math.min(Number(action.amount), remainingExitAmount));
        if (!evaluateExitConditions(ruleCriteria, currentValues, livePatientBuyback?.capturePct ?? livePatientPut?.pnlPct ?? null)) {
          db.updatePendingAction(action.id, { status: 'rejected', confirmation_reasoning: 'Fresh exit conditions no longer satisfied' });
          continue;
        }
        if (exitSnapshot.orders.length > 1) {
          console.log(`📋 Defer ${action.action} ${action.instrument_name}: multiple existing exits require reconciliation`);
          continue;
        }
        const intent = getSyntheticExitIntent(action.action, triggerData, ruleCriteria);
        let desiredPrice = livePatientBuyback?.limitPrice ?? livePatientPut?.limitPrice ?? currentPrice;
        // Same rule as rule evaluation: a resting monetization offer is raised, never chased down.
        const restingMonetizationPrice = Number(exitSnapshot.orders[0]?.limit_price);
        if (intent === 'monetize_tail_win' && exitSnapshot.orders[0]?.exit_intent === 'monetize_tail_win'
          && restingMonetizationPrice > desiredPrice) desiredPrice = restingMonetizationPrice;
        exitOrderPlan = normalizeDesiredExitOrder({ action: action.action, instrumentName: action.instrument_name,
          amount: action.amount, price: desiredPrice, intent,
          orderType: resolveDesiredExitOrderType({ action: action.action, intent,
            preferredOrderType: triggerData.preferred_order_type, price: desiredPrice, ticker }),
          instrument: instruments.find(instrument => instrument.instrument_name === action.instrument_name),
          ticker, existingOrder: exitSnapshot.orders[0], priceReason: livePatientBuyback?.priceReason,
          ceilingPrice: livePatientBuyback?.ceilingPrice });
        const comparison = exitSnapshot.orders[0] ? compareRestingExitOrder(exitSnapshot.orders[0], exitOrderPlan) : null;
        if (comparison?.decision === 'keep') {
          db.updatePendingAction(action.id, { status: 'cancelled', confirmation_reasoning: 'Equivalent exit already resting; kept without reviewer calls' });
          console.log(`📋 Keep equivalent resting exit ${action.instrument_name} @ $${exitOrderPlan.limit_price} x ${exitOrderPlan.amount}; no reviewers needed`);
          continue;
        }
        if (comparison?.decision === 'unresolved') {
          console.log(`📋 Defer ${action.action} ${action.instrument_name}: existing exit terms unavailable`);
          continue;
        }
        action.amount = exitOrderPlan.amount;
        action.price = exitOrderPlan.limit_price;
        triggerData.desired_exit_order = exitOrderPlan;
        triggerData.preferred_order_type = exitOrderPlan.order_type;
        if (livePatientBuyback || livePatientPut) triggerData.advisor_limit_price = exitOrderPlan.limit_price;
        if (livePatientBuyback) {
          triggerData.patient_buyback_capture_pct = getBuybackCapturePctAtPrice(livePosition.avg_entry_price, exitOrderPlan.limit_price);
          triggerData.current_values.patient_buyback_capture_pct = triggerData.patient_buyback_capture_pct;
          triggerData.current_values.patient_buyback_limit_price = exitOrderPlan.limit_price;
        }
      }
      const advisoryOrderPref = normalizePreferredOrderType(action.action, triggerData.preferred_order_type);
      const advisorEntryLimitPrice = isEntryAction(action.action) && Number(triggerData.advisor_limit_price) > 0
        ? Number(triggerData.advisor_limit_price)
        : null;
      const advisorBuybackLimitPrice = action.action === 'buyback_call' && Number(triggerData.advisor_limit_price) > 0
        ? Number(triggerData.advisor_limit_price)
        : null;
      const advisorSellPutLimitPrice = action.action === 'sell_put' && Number(triggerData.advisor_limit_price) > 0
        ? Number(triggerData.advisor_limit_price)
        : null;

      // Determine what details to show
      let detailsStr;
      if (isReduceOnlyExitAction(action.action)) {
        // Exit: show position info
        detailsStr = `Position exit. ${describeActionSemantics(action.action)} Current values: ${JSON.stringify(triggerData.current_values)}`;
      } else {
        // Entry: show option info
        const triggerDelta = Number(triggerData.delta);
        const liveDelta = Number(ticker?.option_pricing?.d);
        const delta = Number.isFinite(triggerDelta) ? triggerDelta : Number.isFinite(liveDelta) ? liveDelta : null;
        detailsStr = `${describeActionSemantics(action.action)} Delta: ${delta != null ? delta.toFixed(4) : 'N/A'}, Price: $${currentPrice?.toFixed(4) || 'N/A'}`;
      }

      const buybackConfirmationContext = buildBuybackConfirmationContext(action, triggerData);
      const buybackConfirmationPrompt = formatBuybackConfirmationContext(buybackConfirmationContext, liveMarketPrice);
      const pendingBuybackGate = action.action === 'buyback_call'
        ? getBuybackCaptureGate(
            { rule_type: 'exit', action: action.action },
            ruleCriteria,
            triggerData?.current_values || {}
          )
        : { allowed: true };
      if (!pendingBuybackGate.allowed) {
        const rejectReason = `Auto-rejected before LLM: ${pendingBuybackGate.reason}`;
        db.updatePendingAction(action.id, {
          status: 'rejected',
          confirmation_reasoning: rejectReason,
        });
        console.log(`🚫 Rejected: ${action.action} ${action.instrument_name} — ${rejectReason}`);
        continue;
      }
      const pendingSellPutGate = action.action === 'sell_put'
        ? getSellPutProtectionGate(
            { action: action.action, criteria: action.rule_criteria },
            triggerData?.current_values || {},
            {
              criteria: ruleCriteria,
              position: livePositions.find((position) => position.instrument_name === action.instrument_name),
              positions: livePositions,
              plannedSellAmount: Number(action.amount) || 0,
            }
          )
        : { allowed: true };
      if (!pendingSellPutGate.allowed) {
        const rejectReason = `Auto-rejected before LLM: ${pendingSellPutGate.reason}`;
        db.updatePendingAction(action.id, {
          status: 'rejected',
          confirmation_reasoning: rejectReason,
        });
        console.log(`🚫 Rejected: ${action.action} ${action.instrument_name} — ${rejectReason}`);
        continue;
      }
      const buyPutConfirmationPrompt = formatBuyPutConfirmationContext({
        action,
        triggerData,
        ticker,
        currentPrice,
        advisorLimitPrice: advisorEntryLimitPrice,
        instrument: instruments.find((instrument) => instrument.instrument_name === action.instrument_name),
      });
      const sellCallConfirmationPrompt = formatSellCallConfirmationContext({
        action,
        triggerData,
        ticker,
        currentPrice,
      });
      const sellPutConfirmationPrompt = formatSellPutConfirmationContext({
        action,
        triggerData,
        livePositions,
        advisorLimitPrice: advisorSellPutLimitPrice,
        currentPrice,
      });
      const confirmationLearningContext = formatConfirmationLearningContext(
        action.action,
        recentTradeReviews,
        activeTradeLessons
      );
      const recentFailedEntry = getRecentFailedEntry(action.action, action.instrument_name);
      const callMarginDecision = getCallMarginDecision(
        action.action,
        marginState,
        livePositions,
        restingOrders,
        instruments,
        spotPrice,
        action.instrument_name,
        entrySnapshot?.orders.length ? 0 : action.amount,
        currentPrice || action.price
      );
      const callMarginContext = entrySnapshot?.orders.length && action.action === 'sell_call'
        ? `Current sell-call margin includes the existing entry reservation: current utilization=${callMarginDecision.currentUtilization != null ? (callMarginDecision.currentUtilization * 100).toFixed(6) + '%' : 'N/A'}, active cap=${callMarginDecision.effectiveCapPct != null ? (callMarginDecision.effectiveCapPct * 100).toFixed(6) + '%' : 'N/A'}, current reservation satisfies cap=${callMarginDecision.entryCapSatisfied ? 'yes' : 'no'}. The replacement cannot increase the unfilled quantity. Its full incremental margin will be calculated from fresh account state after the original order is terminal; no estimated cancellation credit authorizes submission.`
        : getCallMarginContext(
        action.action,
        marginState,
        livePositions,
        restingOrders,
        instruments,
        spotPrice,
        action.instrument_name,
        entrySnapshot?.orders.length ? 0 : action.amount,
        currentPrice || action.price
      );
      if (action.action === 'sell_call' && !callMarginDecision.available) {
        console.log(`📋 Defer ${action.action} ${action.instrument_name}: fresh margin utilization unavailable`);
        continue;
      }
      if (
        action.action === 'sell_call'
        && callMarginDecision.available
        && !callMarginDecision.entryCapSatisfied
      ) {
        const projectedPct = callMarginDecision.projectedUtilization * 100;
        const capPct = callMarginDecision.effectiveCapPct * 100;
        const rejectReason = `Auto-rejected before LLM: projected sell-call margin utilization ${projectedPct.toFixed(6)}% exceeds active entry cap ${capPct.toFixed(6)}%`;
        db.updatePendingAction(action.id, {
          status: 'rejected',
          confirmation_reasoning: rejectReason,
        });
        console.log(`🚫 Rejected: ${action.action} ${action.instrument_name} — ${rejectReason}`);
        continue;
      }
      const ruleReasoningLine = action.action === 'buy_put'
        ? `Standing rule reasoning at advisory creation (historical; may contain stale score/target language, current buy-put trigger context is authoritative): ${action.rule_reasoning || 'N/A'}`
        : action.action === 'sell_call'
          ? `Standing rule reasoning at advisory creation (historical; may contain stale score/bid/margin language, current sell-call trigger context and margin gate are authoritative): ${action.rule_reasoning || 'N/A'}`
          : `Standing rule reasoning at advisory creation (historical; mentions of existing orders, their age, position size or quotes are not current evidence. The fresh venue snapshot and live position below are authoritative): ${action.rule_reasoning || 'N/A'}`;

      const confirmPrompt = `Trade confirmation:
Action: ${action.action} ${action.instrument_name}
Amount: ${action.amount || 'TBD'}
Best available price: $${currentPrice || action.price || 'N/A'}
	${advisorEntryLimitPrice ? action.action === 'buy_put'
    ? `Advisor target limit price: $${advisorEntryLimitPrice} (derived from target_score=${triggerData.target_score || 'n/a'}; target_score is a limit-price target, not a minimum trigger threshold; do not bid worse than this target).`
    : `Planned sell-call offer: $${advisorEntryLimitPrice} (normalized from the fresh executable bid; any proposed price must still satisfy the current CALL EDGE and min_bid requirements).` : ''}
	${advisorBuybackLimitPrice ? `Advisor buyback limit price: $${advisorBuybackLimitPrice} (patient profit-capture bid; do not pay more than this limit, and never pay more than the live ask).` : ''}
	${advisorSellPutLimitPrice ? `Advisor sell-put minimum exit price: $${advisorSellPutLimitPrice} (tail-win monetization floor; do not sell below this limit merely because the visible bid is sparse).` : ''}
	${detailsStr}
Action semantics: ${describeActionSemantics(action.action)}
Market: spot=$${spotPrice}, momentum=${JSON.stringify(momentum)}
${marginStr}
${callMarginContext}
${buybackConfirmationPrompt}
${buyPutConfirmationPrompt}
${sellCallConfirmationPrompt}
${sellPutConfirmationPrompt}
${ruleReasoningLine}
${entrySnapshot ? `AUTHORITATIVE CURRENT ENTRY STATE (venue checked ${entrySnapshot.observedAt}):
Operation: REPLACE the remaining authorization from ${action.action} order ${entryReviewedOrder.order_id}; this is not an additional entry.
Current live entry book for this action: ${entrySnapshot.orders.length ? JSON.stringify(entrySnapshot.orders.map(order => order.order_id)) : 'none; the original order is already proven terminal'}.
Original entry at review: ${JSON.stringify({ order_id: entryReviewedOrder.order_id, instrument_name: entryReviewedOrder.instrument_name, order_status: entryReviewedOrder.order_status || entryReviewedOrder.status, limit_price: entryReviewedOrder.limit_price, remaining_amount: Number(entryReviewedOrder.amount) - Number(entryReviewedOrder.filled_amount) })}
Concrete desired entry order: ${JSON.stringify(entryOrderPlan)}
${entrySnapshot.orders.length ? 'The existing reservation remains until terminal cancellation and all fills are reconciled. Current margin includes that reservation.' : 'Terminal fills have been reconciled and there is no current order reservation to cancel.'} A full new-order margin and budget check runs after terminal reconciliation and again before submission. Replacement quantity cannot exceed the approved remainder${entryApprovedNotional != null ? `; fills since this review plus replacement spend cannot exceed $${entryApprovedNotional}` : ''}. A rejection leaves any existing order untouched.` : ''}
${exitSnapshot ? formatExitConfirmationSnapshot(action, livePosition, exitSnapshot, exitOrderPlan) : ''}
${exitSnapshot ? 'Fresh exit trigger context' : entrySnapshot ? 'Fresh entry replacement context' : 'Triggered because'}: ${exitSnapshot || entrySnapshot ? JSON.stringify(triggerData) : (action.trigger_details || 'N/A')}
${advisoryOrderPref ? `Historical advisory order type hint for this action: ${advisoryOrderPref}` : ''}
${formatRecentExecutionFrictionContext(action.action, recentFailedEntry)}
${confirmationLearningContext}
${getConfirmationScopePrompt()}
Confirm or reject this trade. If confirming, choose the order execution strategy:
${getActionOrderTypeHardRule(action.action)}
- "ioc" (immediate-or-cancel): take available liquidity within the approved limit and cancel any remainder. Venue taker fees apply to fills.
- "gtc" (good-til-cancelled): a marketable limit can take liquidity immediately; any remainder rests. Maker/taker fees depend on each fill, not the GTC label.
- "post_only": rest as a maker or reject if the price would cross. Use for patient maker intent; filling is not guaranteed. Use the current venue fee schedule, not a fixed cost ratio.

${getConfirmationJsonOnlyPrompt()}
No explanation outside the JSON.
JSON only: { "confirm": true/false, "order_type": "ioc"|"gtc"|"post_only"|null, "limit_price": <number or null for market>, "reasoning": "..." }`;

      // Vote 1: Claude Sonnet (Spitznagel temperament)
      let anthropicVote = null;
      let anthropicFailure = null;
      // ponytail: deterministic preflight already enforces every rule the voters are told to defer to.
      // Skipping them takes 20-50s of quote staleness out of every entry/reprice. Monetization keeps
      // its vote because that is where a model prices our offer into a panic.
      const skipLlmVotes = process.env.SKIP_LLM_CONFIRMATION === '1'
        && !(action.action === 'sell_put' && getPutExitIntent(ruleCriteria) === 'monetize_tail_win');
      if (skipLlmVotes) {
        anthropicVote = { confirm: true, order_type: advisoryOrderPref || (isEntryAction(action.action) ? 'post_only' : null), limit_price: null, reasoning: 'preflight passed; LLM confirmation disabled' };
      } else try {
        const anthropicResp = await axios.post('https://api.anthropic.com/v1/messages', {
          model: ANTHROPIC_SONNET_MODEL,
          thinking: { type: 'disabled' },
          max_tokens: 1024,
          system: `You are a Spitznagel-style risk advisor. Confirm trades that are disciplined and arithmetic. For buy_put, assess overpayment through the approved PUT EDGE price contract at the proposed bid. Reject concrete price-contract or risk-constraint failures.
${getConfirmationScopePrompt()}
${getSharedActionPolicyPrompt()}
MARGIN AWARENESS: Account is ETH-collateralized. Long puts offset ETH exposure in margin. Reject trades that would push initial_margin dangerously low. If the account is under liquidation, reject all new entries.
${getCallMarginDisciplinePrompt()}
${getCallBuybackDisciplinePrompt()}
${getPutExitDisciplinePrompt()}
REGIME AWARENESS: ETH crashes cascade and accelerate. Consider whether selling profitable puts is premature if the crash has further to go. Use the actual Greeks, DTE, and momentum to assess owned-position and short-call risk, with awareness that selloffs can go deeper and faster than expected.
${getConfirmationJsonOnlyPrompt()}`,
          messages: [{ role: 'user', content: confirmPrompt }],
        }, {
          headers: {
            'x-api-key': process.env.ANTHROPIC_API_KEY,
            'anthropic-version': '2023-06-01',
            'content-type': 'application/json',
          },
          timeout: 15000,
        });
        const anthropicText = getAnthropicResponseText(anthropicResp.data);
        if (!anthropicText.trim()) {
          anthropicFailure = getAnthropicResponseFailure(anthropicResp.data);
          console.log(`⚠️ Sonnet confirmation failed: ${anthropicFailure}`);
        } else {
          anthropicVote = extractConfirmationVote(anthropicText);
          if (!anthropicVote) {
            anthropicFailure = `parse error (len=${anthropicText.length})`;
            const preview = anthropicText.replace(/\s+/g, ' ').trim().slice(0, 200);
            console.log(`⚠️ Sonnet confirmation parse preview: ${preview}`);
          }
        }
      } catch (e) {
        if (e.code === 'ECONNABORTED') {
          anthropicFailure = 'timeout';
        } else if (e.response?.status) {
          anthropicFailure = `http ${e.response.status}`;
        } else {
          anthropicFailure = e.message;
        }
        console.log(`⚠️ Sonnet confirmation failed: ${anthropicFailure}`);
      }

      // Vote 2: OpenAI GPT (Taleb temperament)
      let codexVote = null;
      let codexFailure = null;
      if (skipLlmVotes) {
        codexVote = anthropicVote;
      } else try {
        const codexText = await callOpenAI(
          `You are a Taleb-style risk advisor. Your philosophy has TWO sides:
1. BUY CONVEXITY CHEAP: Long puts have limited premium loss and provide nonlinear downside protection. Their intrinsic payoff is bounded by the strike when the underlying reaches zero. For buy_put, judge premium through the approved min_score/target_score at the proposed bid using fresh delta and DTE.
2. SELL THE CROWD'S GREED: Selling calls is routine — exploit mispriced optimism to fund insurance. Confirm call sells when the premium is irrational relative to the actual probability, the strike gives real cushion, and exposure is sized to survive the worst case. Reject when the premium doesn't justify the risk or margin can't absorb an adverse move.
${getConfirmationScopePrompt()}
${getSharedActionPolicyPrompt()}
RUIN AVOIDANCE: The only real constraint. Reject trades that could cause ruin — margin too thin, exposure too concentrated, or sizing that doesn't survive a 2-sigma move. Everything else is about getting paid for risk the crowd misprices.
MARGIN AWARENESS: Account is ETH-collateralized. Long puts offset ETH exposure in margin. Reject if initial_margin is dangerously low or account is under liquidation.
${getCallMarginDisciplinePrompt()}
${getCallBuybackDisciplinePrompt()}
${getPutExitDisciplinePrompt()}
REGIME AWARENESS: ETH crashes cascade fast. Selling puts during an active crash may be selling convexity prematurely. Use actual Greeks, DTE, and momentum to assess owned-position and short-call risk.
${getConfirmationJsonOnlyPrompt()}
Output JSON only: { "confirm": true/false, "order_type": "ioc"|"gtc"|"post_only"|null, "limit_price": <number or null>, "reasoning": "..." }`,
          confirmPrompt,
          { maxTokens: 512, timeout: 15000, model: OPENAI_CONFIRMATION_MODEL, reasoningEffort: 'none' }
        );
        if (codexText) {
          codexVote = extractConfirmationVote(codexText);
        }
        if (!codexVote) {
          codexFailure = 'empty or unparsable response';
        }
      } catch (e) {
        codexFailure = e.message;
        console.log(`⚠️ OpenAI confirmation failed: ${e.message}`);
      }

      const decisionAnthropicVote = anthropicVote;
      const decisionCodexVote = codexVote;
      const decision = resolveConfirmationVotes(anthropicVote, codexVote);
      if (decision === 'retry' || (entrySnapshot && decision !== 'rejected' && (!anthropicVote || !codexVote))) {
        db.updatePendingAction(action.id, { retries: (action.retries || 0) + 1 });
        console.log(`⚠️ Confirmation failed for ${action.instrument_name} (retry ${(action.retries || 0) + 1})`);
        continue;
      }
      const reasoning = [
        anthropicVote ? `Sonnet: ${anthropicVote.confirm ? 'CONFIRM' : 'REJECT'} — ${anthropicVote.reasoning || 'no reason'}` : `Sonnet: FAILED — ${anthropicFailure || 'unknown error'}`,
        codexVote ? `OpenAI: ${codexVote.confirm ? 'CONFIRM' : 'REJECT'} — ${codexVote.reasoning || 'no reason'}` : `OpenAI: FAILED — ${codexFailure || 'unknown error'}`,
      ].join(' | ');

      // Resolve order type from voter consensus (prefer Anthropic's pick, fallback to OpenAI)
      let confirmedOrderType = (
        (decisionAnthropicVote?.confirm ? decisionAnthropicVote.order_type : null)
        || (decisionCodexVote?.confirm ? decisionCodexVote.order_type : null)
        || decisionAnthropicVote?.order_type
        || decisionCodexVote?.order_type
        || advisoryOrderPref
        || 'ioc'
      );
      const validOrderTypes = getAllowedOrderTypesForAction(action.action);
      let orderTypeValidationNote = null;
      if (isReduceOnlyExitAction(action.action) && !validOrderTypes.includes(confirmedOrderType)) {
        orderTypeValidationNote = `reduce_only_exit_forces_ioc_from_${confirmedOrderType || 'null'}`;
        confirmedOrderType = 'ioc';
      }
      const baseOrderType = validOrderTypes.includes(confirmedOrderType) ? confirmedOrderType : 'ioc';

      if (!validOrderTypes.includes(confirmedOrderType)) {
        const failureReason = `Invalid LLM order_type for ${action.action}: ${confirmedOrderType}. Expected one of ${validOrderTypes.join(', ')}.`;
        db.updatePendingAction(action.id, {
          status: 'failed',
          execution_result: failureReason,
          confirmation_reasoning: `${reasoning} | invalid_order_type=${confirmedOrderType}`,
        });
        console.log(`⚠️ ${failureReason} ${action.action} ${action.instrument_name}`);
        continue;
      }

      const orderTypeAdaptation = adaptOrderTypeFromFailureHistory(action.action, action.instrument_name, baseOrderType);
      let orderType = orderTypeAdaptation.orderType;
      let orderTypeNote = [orderTypeValidationNote, orderTypeAdaptation.note].filter(Boolean).join('; ') || null;
      if (
        isReduceOnlyExitAction(action.action)
        && isRestingOrderType(orderType)
        && !isSyntheticRestingExitIntentAllowed(action.action, triggerData, ruleCriteria)
      ) {
        const blockedRestingOrderType = orderType;
        orderType = 'ioc';
        orderTypeNote = orderTypeNote
          ? `${orderTypeNote}; non_synthetic_exit_forces_ioc_from_${blockedRestingOrderType}`
          : `non_synthetic_exit_forces_ioc_from_${blockedRestingOrderType}`;
      }
      if (action.action === 'sell_put' && triggerData.put_exit_intent === 'roll_protection' && orderType !== 'ioc') {
        orderType = 'ioc';
        orderTypeNote = orderTypeNote
          ? `${orderTypeNote}; roll_protection_forces_ioc`
          : 'roll_protection_forces_ioc';
      }
      if (
        action.action === 'buy_put'
        && advisorEntryLimitPrice != null
        && Number(liveMarketPrice) > 0
        && advisorEntryLimitPrice < Number(liveMarketPrice)
        && orderType === 'ioc'
      ) {
        orderType = 'gtc';
        orderTypeNote = orderTypeNote
          ? `${orderTypeNote}; advisor_target_limit_requires_resting_order`
          : 'advisor_target_limit_requires_resting_order';
      }
      const syntheticRestingPreference = isRestingOrderType(advisoryOrderPref)
        ? advisoryOrderPref
        : 'post_only';
      const patientBuybackLimitNeedsResting = action.action === 'buyback_call'
        && orderType === 'ioc'
        && advisorBuybackLimitPrice != null
        && Number(liveMarketPrice) > 0
        && advisorBuybackLimitPrice < Number(liveMarketPrice)
        && isSyntheticRestingExitIntentAllowed(action.action, triggerData, ruleCriteria);
      const patientSellPutLimitNeedsResting = action.action === 'sell_put'
        && orderType === 'ioc'
        && advisorSellPutLimitPrice != null
        && Number(liveMarketPrice) > 0
        && advisorSellPutLimitPrice > Number(liveMarketPrice)
        && isSyntheticRestingExitIntentAllowed(action.action, triggerData, ruleCriteria);
      if (patientBuybackLimitNeedsResting || patientSellPutLimitNeedsResting) {
        orderType = syntheticRestingPreference;
        orderTypeNote = orderTypeNote
          ? `${orderTypeNote}; patient_synthetic_limit_requires_resting_order`
          : 'patient_synthetic_limit_requires_resting_order';
      }
      if (orderTypeNote) {
        console.log(`📋 Order-type override for ${action.action} ${action.instrument_name}: ${baseOrderType} -> ${orderType} (${orderTypeNote})`);
      }

      // Resolve limit price: voter can override, otherwise use current market price
      // Sanity check: voter price must be within 50% of market price (prevents LLM hallucinating insane prices)
      const voterLimitPrice = (
        (decisionAnthropicVote?.confirm ? decisionAnthropicVote.limit_price : null)
        || (decisionCodexVote?.confirm ? decisionCodexVote.limit_price : null)
        || decisionAnthropicVote?.limit_price
        || decisionCodexVote?.limit_price
      );
      const defaultActionPrice = currentPrice || action.price;
      const marketPrice = action.action === 'buy_put' && advisorEntryLimitPrice != null
        ? (Number(defaultActionPrice) > 0 ? Math.min(advisorEntryLimitPrice, Number(defaultActionPrice)) : advisorEntryLimitPrice)
        : action.action === 'buyback_call' && advisorBuybackLimitPrice != null
          ? (Number(defaultActionPrice) > 0 ? Math.min(advisorBuybackLimitPrice, Number(defaultActionPrice)) : advisorBuybackLimitPrice)
          : action.action === 'sell_put' && advisorSellPutLimitPrice != null
            ? (Number(defaultActionPrice) > 0 ? Math.max(advisorSellPutLimitPrice, Number(defaultActionPrice)) : advisorSellPutLimitPrice)
          : (advisorEntryLimitPrice || defaultActionPrice);
      const sanityReferencePrice = action.action === 'sell_put' && advisorSellPutLimitPrice != null
        ? marketPrice
        : (currentPrice || action.price || marketPrice);
      let executionPrice = marketPrice;
      if (typeof voterLimitPrice === 'number' && voterLimitPrice > 0 && sanityReferencePrice > 0) {
        const ratio = voterLimitPrice / sanityReferencePrice;
        if (ratio >= 0.5 && ratio <= 2.0) {
          if (action.action === 'buy_put' && advisorEntryLimitPrice != null && voterLimitPrice > advisorEntryLimitPrice) {
            executionPrice = advisorEntryLimitPrice;
            console.log(`📋 Buy-put limit capped: voter $${voterLimitPrice} is worse than advisor target $${advisorEntryLimitPrice}; using advisor target`);
          } else if (action.action === 'buyback_call' && advisorBuybackLimitPrice != null && voterLimitPrice > advisorBuybackLimitPrice) {
            executionPrice = advisorBuybackLimitPrice;
            console.log(`📋 Buyback limit capped: voter $${voterLimitPrice} is worse than advisor target $${advisorBuybackLimitPrice}; using advisor target`);
          } else if (action.action === 'buyback_call' && voterLimitPrice > marketPrice) {
            executionPrice = marketPrice;
            console.log(`📋 Buyback limit capped: voter $${voterLimitPrice} is worse than live ask $${marketPrice}; using live ask instead`);
          } else if (action.action === 'sell_put' && advisorSellPutLimitPrice != null && voterLimitPrice < advisorSellPutLimitPrice) {
            executionPrice = advisorSellPutLimitPrice;
            console.log(`📋 Sell-put limit floored: voter $${voterLimitPrice} is below advisor exit floor $${advisorSellPutLimitPrice}; using advisor floor`);
          } else {
            executionPrice = voterLimitPrice;
          }
        } else {
          console.log(`⚠️ Voter price $${voterLimitPrice} rejected (${(ratio * 100).toFixed(0)}% of live reference $${sanityReferencePrice}) — using default limit $${marketPrice}`);
        }
      }

      if (decision === 'confirmed' && action.action === 'buyback_call' && !(Number(liveMarketPrice) > 0)) {
        const failureReason = `No live buyback market price for ${action.action} ${action.instrument_name}; refusing stale threshold-based buyback`;
        db.updatePendingAction(action.id, {
          status: 'failed',
          execution_result: failureReason,
          confirmation_reasoning: reasoning,
        });
        console.log(`⚠️ ${failureReason}`);
        continue;
      }

      if (decision === 'confirmed' && !(Number(executionPrice) > 0)) {
        const failureReason = `No executable market price for ${action.action} ${action.instrument_name}`;
        db.updatePendingAction(action.id, {
          status: 'failed',
          execution_result: failureReason,
        });
        console.log(`⚠️ ${failureReason}`);
        continue;
      }

      if (decision === 'confirmed') {
        const executionInstrument = instruments.find(instrument => instrument.instrument_name === action.instrument_name);
        const executionDirection = getActionPolicy(action.action)?.direction;
        executionPrice = normalizeOrderPriceForVenue(executionPrice, executionInstrument, executionDirection).price;
        if (exitSnapshot) {
          const executionTicker = await fetchFreshTickerForInstrument(action.instrument_name);
          if (!executionTicker) continue;
          executionPrice = normalizeDesiredExitOrder({ action: action.action, instrumentName: action.instrument_name,
            amount: action.amount, price: executionPrice, orderType, intent: exitOrderPlan.exit_intent,
            instrument: executionInstrument, ticker: executionTicker }).limit_price;
        }
        let executionAmount = floorOrderAmountToVenuePrecision(Number(action.amount));
        const validateOrder = createFinalOrderValidator({ action, instruments, spotPrice, triggerData, ruleCriteria, orderType, tickerMap });
        if (entrySnapshot) {
          // The original order is still reserved while reviewers decide. Check
          // the replacement's economics without adding the same exposure twice;
          // actual incremental margin is checked only after terminal cancellation.
          const [entryTicker, entryMargin, entryPositions] = await Promise.all([
            fetchFreshTickerForInstrument(action.instrument_name), fetchSubaccount(),
            fetchPositions({ throwOnError: true }),
          ]);
          const currentRule = db.getActiveRules().find(rule => Number(rule.id) === Number(action.rule_id));
          if (!currentRule || currentRule.action !== action.action
            || JSON.stringify(parseMaybeJsonObject(currentRule.criteria)) !== JSON.stringify(ruleCriteria)) {
            db.updatePendingAction(action.id, { status: 'cancelled', confirmation_reasoning: reasoning,
              execution_result: 'Entry replacement rule changed during review; original order retained' });
            continue;
          }
          if (action.action === 'buy_put') executionAmount = Math.min(executionAmount, entryApprovedNotional / executionPrice);
          const desired = require('./bot/resting-entry-plan').normalizeDesiredEntryOrder({
            action: action.action, instrumentName: action.instrument_name, amount: executionAmount,
            price: executionPrice, orderType, instrument: executionInstrument, ticker: entryTicker,
          });
          executionPrice = desired.limit_price;
          executionAmount = desired.amount;
          const economics = validateEntryReplacementEconomics({ action, activeRule: currentRule, triggerData,
            desiredOrder: desired, reviewedOrder: entryReviewedOrder, ticker: entryTicker, tickerMap,
            marginState: entryMargin, positions: entryPositions, instruments,
            spotPrice: Number(entryTicker?.I) > 0 ? Number(entryTicker.I) : spotPrice });
          if (!economics.allowed) {
            db.updatePendingAction(action.id, { status: 'failed', confirmation_reasoning: reasoning,
              execution_result: `Entry replacement economics rejected: ${economics.reason}; original order retained` });
            continue;
          }
          const replacement = await prepareEntryOrderReplacement({ action, reviewedOrder: entryReviewedOrder,
            desiredOrder: desired, approvedNotional: entryApprovedNotional, instrument: executionInstrument });
          if (!replacement.allowed) {
            db.updatePendingAction(action.id, { status: 'cancelled', confirmation_reasoning: reasoning,
              execution_result: replacement.reason });
            continue;
          }
          executionAmount = replacement.amount;
          triggerData.desired_entry_order = { ...desired, amount: executionAmount };
        }
        let finalPolicyCheck = await validateOrder({ price: executionPrice, amount: executionAmount });
        if (!finalPolicyCheck.allowed) {
          db.updatePendingAction(action.id, {
            status: 'failed',
            confirmation_reasoning: reasoning,
            execution_result: `Final order rejected (${finalPolicyCheck.code}): ${finalPolicyCheck.reason}`,
          });
          console.log(`📋 Final order blocked: ${action.action} ${action.instrument_name} — ${finalPolicyCheck.reason}`);
          continue;
        }
        if (exitSnapshot) {
          const replacement = await prepareExitOrderReplacement({ action, reviewedOrders: exitSnapshot.orders,
            desiredOrder: { ...exitOrderPlan, amount: executionAmount, limit_price: executionPrice, order_type: orderType } });
          if (!replacement.allowed) {
            db.updatePendingAction(action.id, { status: 'cancelled', confirmation_reasoning: reasoning, execution_result: replacement.reason });
            console.log(`📋 Exit deferred: ${action.instrument_name} — ${replacement.reason}`);
            continue;
          }
          executionAmount = replacement.amount;
          livePositions = replacement.positions;
          finalPolicyCheck = await validateOrder({ price: executionPrice, amount: executionAmount });
          if (!finalPolicyCheck.allowed) {
            db.updatePendingAction(action.id, { status: 'failed', confirmation_reasoning: reasoning,
              execution_result: `Final order rejected after exit reconciliation (${finalPolicyCheck.code}): ${finalPolicyCheck.reason}` });
            continue;
          }
        }
        db.updatePendingAction(action.id, {
          status: 'confirmed',
          ...(exitSnapshot || entrySnapshot ? { trigger_details: triggerData } : {}),
          confirmation_reasoning: `${reasoning} | order_type=${orderType} limit=$${executionPrice}${orderTypeNote ? ` | order_type_override=${orderTypeNote}` : ''}`,
          confirmed_at: new Date().toISOString(),
        });

        // Execute the trade
        const result = await executeOrder(
          action.action,
          action.instrument_name,
          executionAmount,
          executionPrice,
          instruments,
          spotPrice,
          orderType,
          tickerMap,
          action.id,
          {
            triggerData,
            ruleCriteria,
            livePositions,
            restingOrders,
            validateOrder,
            approvedBounds: finalPolicyCheck.approvedBounds,
          }
        );

        if (result && result.postOnlyRejected) {
          // post_only failed even after one maker retry — mark failed, don't retry again this tick
          db.updatePendingAction(action.id, {
            status: 'failed',
            execution_result: result.postOnlyBlocked
              ? `post_only retry blocked by margin guard: ${result.context}`
              : `post_only rejected: ${result.context || `would cross book at $${executionPrice}`}. Price may have moved — will re-evaluate next tick.`,
          });
          if (result.postOnlyBlocked) {
            console.log(`📋 maker entry skipped: ${action.action} ${action.instrument_name} — ${result.context || 'retry blocked by guard'}`);
          } else {
            console.log(`📋 post_only rejected: ${action.action} ${action.instrument_name} — ${result.context || 'price crossed book'}`);
          }
        } else if (result && result.zeroFill) {
          // IOC got zero fill — mark as failed, will retry next tick
          db.updatePendingAction(action.id, {
            status: 'failed',
            execution_result: `Zero fill (IOC) — no liquidity at $${executionPrice}`,
          });
          console.log(`⚠️ Zero fill: ${action.action} ${action.instrument_name} @ $${executionPrice} — will retry`);
        } else if (result && result.resting) {
          // GTC/post-only order is resting on the book
          const finalOrderPrice = result.price ?? executionPrice;
          db.updatePendingAction(action.id, {
            status: 'resting',
            executed_at: new Date().toISOString(),
            execution_result: JSON.stringify({ ...result, price: finalOrderPrice, note: 'Resting on order book' }),
          });
          resting++;
          console.log(`📋 Resting order: ${action.action} ${action.instrument_name} @ $${finalOrderPrice} [${orderType}] | ${reasoning}`);
        } else if (result && result.failed) {
          db.updatePendingAction(action.id, { status: 'failed', execution_result: result.reason || 'Order placement failed' });
          console.log(`❌ Confirmed but execution failed: ${action.action} ${action.instrument_name} — ${result.reason || 'Order placement failed'}`);
        } else if (result) {
          db.updatePendingAction(action.id, {
            status: 'executed',
            executed_at: new Date().toISOString(),
            execution_result: JSON.stringify(result),
          });
          executed++;
          console.log(`✅ Confirmed & executed: ${action.action} ${action.instrument_name} [${orderType}] | ${reasoning}`);
        } else {
          const failureReason = result?.reason || 'Order placement failed';
          db.updatePendingAction(action.id, { status: 'failed', execution_result: failureReason });
          console.log(`❌ Confirmed but execution failed: ${action.action} ${action.instrument_name} — ${failureReason}`);
        }
      } else {
        db.updatePendingAction(action.id, {
          status: 'rejected',
          confirmation_reasoning: reasoning,
        });
        console.log(`🚫 Rejected: ${action.action} ${action.instrument_name} | ${reasoning}`);
        sendTelegram(`❌ *REJECTED*: ${action.action} ${action.instrument_name}\n${reasoning}`);
      }
    } catch (e) {
      console.error(`❌ Confirmation error for ${action.instrument_name}:`, e.message);
      db.updatePendingAction(action.id, { retries: (action.retries || 0) + 1 });
    }
  }

  if (executed > 0) console.log(`📋 Executed ${executed} trade(s) this tick`);
  if (resting > 0) console.log(`📋 Placed ${resting} resting order(s) this tick`);
};

// ─── LLM-Driven Trading Advisory ─────────────────────────────────────────────

const getAdvisoryDataQualityPrompt = () => [
  '- Missing live quotes make EDGE unavailable, not zero or evidence of poor value. Distinguish no DTE/delta-eligible contracts from eligible contracts without a usable ask (puts) or bid (calls), and from incomplete instrument coverage.',
  '- Use the supplied quote timestamp and coverage counts. A zero or missing venue quote is not a zero-cost offer. Marks and Greeks do not substitute for executable quotes.',
  '- Candidate spreads apply to the named contract. The broad quote snapshot spans all recorded tenors and uses (ask - bid) / mark; its outliers, mean, and median do not describe the eligible candidate pool or actual execution loss. Spread observations do not add an entry veto.',
  '- Current account margin headroom is not a severe-crash survival result. No crash-survival simulation is supplied; do not infer one from margin utilization being below its operating cap.',
].join('\n');

const buildTradingAdvisoryDraft = async (snapshot, advisoryId) => {
  const { positions, spotPrice, tickerMap, instruments, marketTimestamp: currentTickTimestamp, quoteAvailability } = snapshot;
  // ── Gather context ──────────────────────────────────────────────────────────

  // Balances
  let balances = [];
  try { balances = await fetchCollaterals(); } catch (e) {
    console.log('📋 Advisory: failed to fetch collaterals:', e.message);
  }

  // Momentum
  const momentum = {
    mediumTerm: botData.mediumTermMomentum,
    shortTerm: botData.shortTermMomentum,
  };

  // Wiki knowledge
  const wikiContext = queryWikiContext();
  const wikiSignals = getWikiSignalContext();

  // Market sentiment
  const nowMs = Date.parse(currentTickTimestamp);
  const since6h = new Date(nowMs - 6 * 60 * 60 * 1000).toISOString();
  const since24h = new Date(nowMs - 24 * 60 * 60 * 1000).toISOString();
  const since7dSentiment = new Date(nowMs - 7 * 24 * 60 * 60 * 1000).toISOString();
  const since30dSentiment = new Date(nowMs - 30 * 24 * 60 * 60 * 1000).toISOString();
  let sentiment = {};
  if (db) {
    try {
      sentiment = {
        latest: {
          fundingRate: db.getFundingRateLatest(),
          fundingAvg24h: db.getFundingRateAvg24h(),
          marketQuality: db.getMarketQualitySummary(since24h),
        },
        windows: {
          '6h': {
            fundingRates: db.getFundingRates(since6h),
            optionsSkew: db.getOptionsSkew(since6h),
            aggregateOI: db.getAggregateOI(since6h),
          },
          '24h': {
            fundingRates: db.getFundingRates(since24h),
            optionsSkew: db.getOptionsSkew(since24h),
            aggregateOI: db.getAggregateOI(since24h),
          },
          '7d': {
            fundingRates: db.getFundingRates(since7dSentiment),
            optionsSkew: db.getOptionsSkew(since7dSentiment),
            aggregateOI: db.getAggregateOI(since7dSentiment),
          },
          '30d': {
            fundingRates: db.getFundingRates(since30dSentiment),
            optionsSkew: db.getOptionsSkew(since30dSentiment),
            aggregateOI: db.getAggregateOI(since30dSentiment),
          },
        },
      };
    } catch (e) {
      console.log('📋 Advisory: failed to fetch sentiment:', e.message);
    }
  }

  let mandelbrotSpotPathContext = buildMandelbrotSpotPathContext({
    spotPrice,
    spotRows: [],
    nowMs,
    source: 'unavailable',
  });
  if (db) {
    try {
      let spotPathRows = [];
      let source = 'spot_prices_hourly';
      if (typeof db.getSpotPricesHourly === 'function') {
        spotPathRows = db.getSpotPricesHourly(since30dSentiment) || [];
      }
      if (spotPathRows.length === 0 && typeof db.getRecentSpotPrices === 'function') {
        spotPathRows = db.getRecentSpotPrices(since30dSentiment) || [];
        source = 'spot_prices_raw_downsampled';
      }
      mandelbrotSpotPathContext = buildMandelbrotSpotPathContext({
        spotPrice,
        spotRows: spotPathRows,
        nowMs,
        source,
      });
    } catch (e) {
      console.log(`📋 Advisory: failed to fetch Mandelbrot spot path: ${e.message}`);
    }
  }

  // Account health — margin + budget discipline
  const ethBalance = balances.find(b => b.asset_name === 'ETH')?.amount || 0;
  const usdcBalance = balances.find(b => b.asset_name === 'USDC')?.amount || 0;
  const shortCallPositions = positions.filter(p =>
    p.instrument_name?.endsWith('-C') && p.direction === 'short'
  );
  const totalCallExposure = shortCallPositions.reduce((sum, p) => sum + Math.abs(Number(p.amount) || 0), 0);

  let marginState = null;
  try { marginState = await fetchSubaccount(); } catch { /* ok */ }

  const putBudgetRemaining = Math.max(0, botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought);
  const effectiveCallCapPct = getEffectiveCallExposureCapPct(positions, spotPrice);
  const breakoutAddWindow = effectiveCallCapPct > CALL_EXPOSURE_CAP_PCT;
  const insuredPortfolioBaseUsd = getPutBudgetPortfolioValue(ethBalance, usdcBalance, spotPrice);

  const accountHealth = {
    margin_as_of: new Date().toISOString(),
    crash_survival_assessment: 'not_performed',
    ethBalance,
    usdcBalance,
    shortCallExposure: totalCallExposure,
    callMarginDiscipline: {
      capPct: effectiveCallCapPct,
      bufferPct: CALL_EXPOSURE_BUFFER_PCT,
      bufferedLimitPct: getCallExposureLimitPct(effectiveCallCapPct),
      baseCapPct: CALL_EXPOSURE_CAP_PCT,
      baseBufferedLimitPct: CALL_EXPOSURE_LIMIT_PCT,
      breakoutOverrideCapPct: CALL_BREAKOUT_OVERRIDE_CAP_PCT,
      breakoutOverrideBufferedLimitPct: CALL_BREAKOUT_OVERRIDE_LIMIT_PCT,
      breakoutAddWindow,
      currentShortExposure: +totalCallExposure.toFixed(2),
      utilizationPct: marginState ? +(100 * (estimateDisplayedMarginUtilization(marginState) || 0)).toFixed(1) : null,
      marginBase: marginState ? +getMarginCapacityBase(marginState).toFixed(2) : null,
      note: `Base target cap: keep Derive-displayed margin utilization near ${(CALL_EXPOSURE_CAP_PCT * 100).toFixed(0)}%; the ${(CALL_EXPOSURE_BUFFER_PCT * 100).toFixed(0)} percentage point buffer up to ${(CALL_EXPOSURE_LIMIT_PCT * 100).toFixed(0)}% is last-mile execution safety, not planned sell-call capacity. ${(CALL_ENTRY_CAP_PCT * 100).toFixed(0)}% is a caution threshold for new short-call entries. When breakoutAddWindow=true because spot is breaking upward with existing short calls already on, the bot may add into richer upside premium up to a ${(CALL_BREAKOUT_OVERRIDE_CAP_PCT * 100).toFixed(0)}% target / ${(CALL_BREAKOUT_OVERRIDE_LIMIT_PCT * 100).toFixed(0)}% buffered limit instead of reflexively buying back into emotional bullish pricing. utilizationPct mirrors the Derive display metric; projected trade sizing still uses the internal margin estimate.`,
    },
    margin: marginState ? {
      buying_power: +marginState.initial_margin.toFixed(2),              // available margin for new trades
      maintenance_margin: +marginState.maintenance_margin.toFixed(2),    // available before liquidation
      subaccount_value: +marginState.subaccount_value.toFixed(2),
      collaterals_value: +marginState.collaterals_value.toFixed(2),
      collaterals_initial_margin: +marginState.collaterals_initial_margin.toFixed(2),
      positions_margin: +marginState.positions_initial_margin.toFixed(2), // margin consumed by positions
      open_orders_margin: marginState.open_orders_margin,
      is_under_liquidation: marginState.is_under_liquidation,
      margin_usage_pct: getMarginCapacityBase(marginState) > 0
        ? +((100 * (estimateDisplayedMarginUtilization(marginState) || 0))).toFixed(1)
        : 0,
    } : null,
    putBudgetDiscipline: {
      annualRate: PUT_ANNUAL_RATE,
      budgetThisCycle: botData.putBudgetForCycle,
      spent: botData.putNetBought,
      remaining: putBudgetRemaining,
      rollover: botData.putUnspentBuyLimit,
      cycleDays: BOT_CONFIG.PERIOD_DAYS,
      insuredPortfolioBaseUsd: +insuredPortfolioBaseUsd.toFixed(2),
      insuredExternalEth: PUT_INSURED_EXTERNAL_ETH,
      note: `Arithmetic commitment: ${(PUT_ANNUAL_RATE * 100).toFixed(2)}% of insured base per year, allocated in ${BOT_CONFIG.PERIOD_DAYS}-day windows. Budget base = Derive USDC plus total insured ETH marked at spot, where external insured ETH is fixed at ${PUT_INSURED_EXTERNAL_ETH.toFixed(4)}. Funded via leverage on ETH collateral. Spend predictably across the cycle — not all at once. Selling puts realizes cash but does not replenish this cycle's put-buying budget.`,
    },
    note: 'These fields describe current margin headroom; they do not establish severe-crash survival or a modeled liquidation path. Account is ETH-collateralized on Derive. buying_power = available initial margin for new trades. margin_usage_pct mirrors the Derive display metric. Projected trade sizing still uses the bot internal margin estimate. Sizing must respect buying power, put budget discipline, and the active call margin-utilization cap, which can widen during an upside breakout when the bot already has short calls on.',
  };

  // Recent orders
  const since7d = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  let recentOrders = [];
  if (db) {
    try { recentOrders = db.getRecentOrders(since7d, 10); } catch { /* ok */ }
  }
  let recentTradeReviews = [];
  if (db) {
    try { recentTradeReviews = db.getRecentTradeReviews(6); } catch { /* ok */ }
  }

  // Current active rules + recent pending actions
  let activeRules = [];
  let recentPendingActions = [];
  if (db) {
    try { activeRules = db.getActiveRules(); } catch { /* ok */ }
    try { recentPendingActions = db.getRecentPendingActions(10); } catch { /* ok */ }
  }

  // Open orders on the book
  let openOrders = [];
  try { openOrders = await fetchOpenOrders(); } catch { /* ok */ }
  let openRestingOrders = [];
  if (db) {
    try { openRestingOrders = db.getOpenRestingOrders(); } catch { /* ok */ }
  }

  const rollingOptionValueContext = buildRollingOptionValueContext({
    tickerMap,
    momentum,
    putBudgetRemaining,
    activeRules,
    recentPendingActions,
    openRestingOrders,
    currentTickTimestamp,
    spotPrice,
    nowMs,
    expectedInstruments: instruments,
    quoteAvailability,
  });

  // ── Score and rank top 5 puts and calls from tickerMap ──────────────────────

  const parseInstrumentName = (name) => {
    // ETH-20260501-1500-P => { expiry: Date, strike: 1500, optionType: 'P' }
    const parts = name.split('-');
    if (parts.length !== 4) return null;
    const expiryStr = parts[1]; // YYYYMMDD
    const expiry = new Date(
      `${expiryStr.slice(0, 4)}-${expiryStr.slice(4, 6)}-${expiryStr.slice(6, 8)}T08:00:00Z`
    );
    return {
      expiry,
      strike: Number(parts[2]),
      optionType: parts[3], // 'P' or 'C'
      dte: Math.max(0, (expiry.getTime() - nowMs) / (24 * 60 * 60 * 1000)),
    };
  };

  // Score candidates. Puts stay broad for hedge discovery; sell-call candidates
  // use the same strategy universe the executor is allowed to trade.
  const scoredPuts = [];
  const scoredCalls = [];
  const advisorySellCallMarketContext = buildLiveSellCallMarketContext(tickerMap, nowMs, {
    market_oi_delta_24h_pct: rollingOptionValueContext?.call_value_context?.market_context?.market_oi_delta_24h_pct,
  });

  for (const [name, ticker] of Object.entries(tickerMap)) {
    const parsed = parseInstrumentName(name);
    if (!parsed) continue;

    const delta = Number(ticker.option_pricing?.d) || 0;
    const askPrice = Number(ticker.a) || 0;
    const bidPrice = Number(ticker.b) || 0;

    if (parsed.optionType === 'P') {
      if (delta >= PUT_DELTA_RANGE[0] && delta <= PUT_DELTA_RANGE[1]
        && parsed.dte >= BUY_PUT_ADVISORY_DTE_RANGE[0]
        && parsed.dte <= BUY_PUT_ADVISORY_DTE_RANGE[1]
        && askPrice > 0) {
        const rawScore = Math.abs(delta) / askPrice;
        const score = normalizeBuyPutScore(rawScore, parsed.dte);
        const research = classifyBuyPutEdge({
          score,
          rawScore,
          dte: parsed.dte,
          strike: parsed.strike,
          spotPrice,
          entryPrice: askPrice,
          askPrice,
          bidPrice,
          markPrice: Number(ticker.M) || 0,
          askAmount: Number(ticker.A) || 0,
          bidAmount: Number(ticker.B) || 0,
          impliedVol: getTickerImpliedVol(ticker),
          marketContext: advisorySellCallMarketContext,
        });
        scoredPuts.push({
          name,
          delta,
          askPrice,
          bidPrice,
          dte: parsed.dte,
          strike: parsed.strike,
          rawScore,
          score,
          diagnostics: research.edge_components || {},
          shockMultiples: research.edge_components?.shock_payoff_multiples || {},
          spreadPct: candidateSpreadPct({ askPrice, bidPrice, markPrice: ticker.M }),
          quoteReceivedAt: ticker.quote_received_at || null,
        });
      }
    } else if (parsed.optionType === 'C') {
      if (isSellCallCandidateInStrategyRange(parsed.dte, delta) && bidPrice > 0) {
        const score = bidPrice / Math.abs(delta);
        const research = buildSellCallEdge({
          score,
          dte: parsed.dte,
          bidPrice,
          askPrice,
          markPrice: Number(ticker.M) || 0,
          bidAmount: Number(ticker.B) || 0,
          askAmount: Number(ticker.A) || 0,
          impliedVol: getTickerImpliedVol(ticker),
          openInterest: getTickerOpenInterest(ticker),
          marketContext: advisorySellCallMarketContext,
        });
        scoredCalls.push({
          name,
          delta,
          askPrice,
          bidPrice,
          dte: Math.round(parsed.dte),
          strike: parsed.strike,
          score,
          selectionScore: research.selection_score,
          research: research.recommendation,
          dteBucket: research.dte_bucket,
          scoreBand: research.score_band,
          spreadPct: candidateSpreadPct({ askPrice, bidPrice, markPrice: ticker.M }),
          quoteReceivedAt: ticker.quote_received_at || null,
          warnings: [],
        });
      }
    }
  }

  scoredPuts.sort((a, b) => b.score - a.score || a.name.localeCompare(b.name));
  scoredCalls.sort((a, b) => (b.selectionScore || b.score) - (a.selectionScore || a.score));
  const top5Puts = scoredPuts.slice(0, 8);
  const top5Calls = scoredCalls.slice(0, 8);
  const positionAdviceSnapshots = buildPositionAdviceSnapshots(positions, tickerMap, spotPrice);
  const rulebookRequirements = buildRulebookRequirements({
    putBudgetRemaining,
    accountHealth,
    positionSnapshots: positionAdviceSnapshots,
  });

  // ── Step 0: Regime Examiner (OpenAI, Mandelbrot temperament) ──────────────

  console.log('📋 Advisory Step 0: Mandelbrot regime context (OpenAI GPT)...');
  let mandelbrotContext = null;
  try {
    mandelbrotContext = await generateMandelbrotRegimeContext({
      spotPrice,
      spotPathContext: mandelbrotSpotPathContext,
      sentiment,
      wikiSignals,
    });
    if (mandelbrotContext) {
      console.log(`📋 Mandelbrot regime: ${mandelbrotContext.regime} @ ${(Number(mandelbrotContext.confidence || 0) * 100).toFixed(0)}% confidence`);
    } else {
      console.log('📋 Mandelbrot regime unavailable; continuing without it');
    }
  } catch (e) {
    console.log(`📋 Advisory Step 0 failed (non-fatal): ${e.message}`);
  }

  // ── Step 1: Primary Advisor (Claude Fable 5.1, Spitznagel temperament) ───────────

  console.log('📋 Advisory Step 1: Primary advisor (Claude Fable 5.1)...');

  const primarySystemPrompt = `You are a senior options strategist with Mark Spitznagel's temperament. Your philosophy:
- Arithmetic discipline above all: PUT entries follow the direct PUT EDGE price contract, budget, margin, and execution checks
- Patience is the edge: being willing to sit on hands when pricing is unfavorable
- Insurance must be well-priced: judge put premiums through the approved PUT EDGE price contract; never undersell calls
- Tail risk is the real risk: the portfolio must survive a 40%+ drawdown
- Premium collection supplements, not replaces, insurance accumulation

Interpret "Spitznagel" operationally, not stylistically:
- Think in portfolio geometry, not prediction. The main question is whether protection is cheap enough or premium is rich enough to justify action.
- Prefer waiting over paying up. "No action" is often the correct answer when asymmetry is weak.
- Evaluate trades by expected payoff asymmetry under stress, cost discipline, and contribution to long-run convexity, not by recent price moves.
- Calm and fear describe market conditions; the approved PUT EDGE price contract determines whether a proposed put bid is acceptable. Do not substitute urgency or IV labels for that arithmetic.
- Premium harvesting is acceptable only when it does not compromise survival. Calls finance the bleed; they are not the core mission.
- Favor disciplined sizing, repeatable budget use, and survivability over opportunistic aggressiveness.
- Evidence that matters most: option pricing quality, IV/skew regime, tail-risk geometry, margin resilience, and whether the position improves or worsens fragility.
For buy_put specifically, rank directly by DTE-normalized PUT EDGE. The 30/40/50% shock scenarios and spread/IV/skew/OI observations are descriptive; they neither alter ranking nor add a standalone veto. A hypothetical scenario payoff is not an expected return estimate.
${getBuyPutEntryPriceDisciplinePrompt()}

You advise a bot that accumulates OTM ETH puts (long insurance) and sells OTM ETH calls (premium harvesting).

## Output Shape
Your output is a standing rulebook. A rule does not need to fire immediately; it should define the favorable future condition that would make the action worth taking before the next advisory run. Each advisory run replaces the prior rulebook, so required watcher conditions must be reassessed and restated every time.

## Evidence Priority
${getAdvisoryDataQualityPrompt()}
${getMomentumEvidenceDisciplinePrompt()}

## Account Model
The account is ETH-collateralized. Puts are bought on leverage against ETH. Derive's margin engine recognizes that long puts offset ETH exposure, so buying puts can improve margin health. USDC from call premiums pays down margin debt first; excess gets converted to ETH manually.

There are TWO constraints on put buying:
1. **Margin**: initial_margin must stay positive. maintenance_margin at zero = liquidation.
2. **Budget discipline**: We commit to spending 3.33% of insured base per year on puts, allocated in 15-day budget windows. The insured base is Derive USDC plus total insured ETH marked at spot, where off-platform insured ETH is fixed at ${PUT_INSURED_EXTERNAL_ETH.toFixed(4)}. Spend predictably across the cycle. Don't front-load. Don't impulse buy. If nothing is well-priced, let the budget roll over.

${getCallMarginDisciplinePrompt()}

## Call Buyback Philosophy (Spitznagel)
We sell short-dated calls. The arithmetic of buybacks is simple: don't pay fear premiums to exit positions that are working.
- **Premium already collected matters**: A sold call keeps the upfront premium unless it is later paid back. If spot finishes at or below strike, including exactly at strike, the option expires worthless and the trade keeps that premium.
- **Profit capture over panic**: When a meaningful chunk of premium has decayed, locking it in and redeploying is disciplined. Buying back because the mark expanded is different: that is paying for upside insurance. It can be rational, but only if the continuation risk is real enough to justify the cost.
- **Advisor-led limit buybacks are allowed**: When live executable buyback economics are worse than the ${CALL_BUYBACK_PROFIT_THRESHOLD}% capture line but close enough that decay could fill a better bid, it is valid to rest a patient synthetic reduce-only buyback at a price the advisor is happy with. The execution layer will only allow this for profit_capture, with a live short position, capped size, and no duplicate same-instrument exit order. The goal is take-profit plus margin/slot recycling for future call sales, not a generic passive buyback program. If the live executable price already captures more than that, do not bid back up to the threshold; hold/let expire or only bid lower.
- **Let theta work**: Short calls are a time-decay trade. A price move against you does not by itself invalidate the thesis. Distinguish temporary mark stress from final payoff geometry, and use the Greeks plus the position's actual risk profile to judge.
- **Safety is not free**: Farther OTM strikes reduce buyback pressure and assignment risk, but they also pay materially less premium. Optimize for premium income that can still be managed systematically, not for maximum distance from spot at any price.
- **Breakout upside can be a sale, not just a threat**: When spot breaks upward while short calls are already on, the instinct is to buy back into elevated bullish sentiment to relieve margin. That is often when call premium is emotionally richest. Consider whether selling more calls at the higher level is the better use of margin, provided the active cap and ruin constraints still hold.
- **Rolling discipline**: Rolling should improve the position, not just delay a loss. If rolling costs more than it's worth, accepting assignment is the honest response.
- Use your judgment on the specific situation — the Greeks, DTE remaining, how much premium has decayed, and the broader portfolio context all matter more than any fixed threshold.

## Market Regime Awareness
ETH crashes tend to cascade — they accelerate, not slow down. Your decisions should reflect the shape of the moment.

Things to consider in your assessment:
- In calm markets, evaluate accumulation using PUT EDGE at the proposed bid. If the approved min_score/target_score cannot be met, quote patiently within that price contract.
- In crashing markets, our puts become increasingly valuable. The temptation is to sell early. Consider whether the crash has further to go — ETH selloffs often have multiple legs.
- In severe crashes, option books can get sparse and visible bids may stop representing fair value. If monetizing a tail-win put, do not undersell just because the best bid is thin or stale. If we are effectively making the market, name a responsible min_exit_price/limit_price from intrinsic value, Greeks, IV/skew, spread/depth, DTE, and the position's payoff role, then use patient maker-style execution when appropriate.
- In recovery, IV can remain elevated even as price stabilizes. Treat that as descriptive context; judge a new put entry by its approved PUT EDGE price contract.
- The full cycle: cash → cheap puts → crash → puts print → sell at the right time → buy cheap ETH → sell calls → premium → repeat.

Use your judgment. Look at the actual Greeks, DTE, IV/skew, spread/depth, OI, executable bid/ask, position characteristics, and only then momentum as secondary path context. There are no absolute directional rules; hard execution, sizing, DTE, and risk constraints still apply.

## Assessment Writing Constraints
- Every assessment must do two jobs: state the clearest current market observation or thesis from the supplied data, and state the operational stance it implies for the bot.
- The assessment must be an operating note, not just a market summary. Use this format:
  Global stance: <1-3 concise sentences on regime, account posture, and whether to deploy, wait, or manage risk>.

  Thesis breakdown:
  - <exact open instrument name>: <hold / buyback_call / sell_put / roll / let expire / monitor>. Why: <specific DTE, moneyness, bid/ask, executable PnL, margin, or hedge role facts>.
- The Thesis breakdown must include one bullet for every instrument in OPEN POSITIONS REQUIRING ADVICE, even when the right action is no trade. If no exit rule is warranted for a position, say that plainly and explain why.
- If you emit an exit rule for a position, the matching Thesis breakdown bullet must name that rule and the reason. If you do not emit an exit rule for a position, the bullet must say the stance is hold/monitor/let expire/no buyback/no roll as appropriate.
- If the current stance is patience, say so explicitly, but still include the required standing watcher rules with stricter favorable conditions.
- In "assessment", use only facts and metric names explicitly present in this prompt or policy constants stated here.
- Never invent metric labels such as "put efficiency", "call efficiency", "deployment efficiency", "antifragility score", or any unnamed ratio.
- Never invent thresholds. If you mention a percentage threshold, it must be one of the explicit policy constants stated here and you must name it precisely. Otherwise omit threshold language.
- For put-budget commentary, use only the supplied budget fields and plain language such as spent, remaining, rollover, or days left. Do not rename budget usage into a new score, efficiency, or threshold.

Given market data, produce a JSON trading agenda with:
{
  "assessment": "multi-line operating note with Global stance and Thesis breakdown covering every open position",
  "entry_rules": [
    {
      "action": "buy_put" | "sell_call",
      "criteria": {
        "option_type": "P" or "C",
        "delta_range": [min_delta, max_delta],
        "dte_range": [min_dte, max_dte],
        "max_strike_pct": 0.80,
        "min_score": 0.004,
        "target_score": 0.0042,
        "value_signal": "strict_fresh_best" | "spot_drop_option_repricing_lag" | "recent_relative_value" | "any_actionable_buy_put",
        "min_bid": 2.00,
        "market_conditions": [{"field": "spot_price", "op": "lt"|"gt"|"gte"|"lte", "value": 2000}]
      },
      "budget_limit": <max USD to spend on this rule>,
      "priority": "high" | "medium" | "low",
      "preferred_order_type": ${ENTRY_ALLOWED_ORDER_TYPES.map((orderType) => `"${orderType}"`).join(' | ')},
      "reasoning": "why this trade makes sense now"
    }
  ],
  "exit_rules": [
    {
      "action": "sell_put" | "buyback_call",
      "instrument_name": "<specific instrument name from positions>",
      "criteria": {
        "conditions": [{"field": "dte"|"delta"|"unrealized_pnl_pct"|"iv"|"theta"|"spot_price", "op": "lt"|"gt"|"gte"|"lte", "value": <number>}],
        "condition_logic": "any" | "all",
        "put_exit_intent": "roll_protection" | "monetize_tail_win",
        "requires_longer_dated_protection": true,
        "retain_downside_protection": true,
        "tranche_fraction": 0.25,
        "min_exit_price": <minimum acceptable sell price for monetize_tail_win>,
        "buyback_intent": "profit_capture" | "threat_management",
        "allow_below_profit_floor": false,
        "target_capture_pct": 80,
        "max_buyback_price": <number>
      },
      "priority": "high" | "medium" | "low",
      "preferred_order_type": ${EXIT_ALLOWED_ORDER_TYPES.map((orderType) => `"${orderType}"`).join(' | ')},
      "reasoning": "why exit is warranted"
    }
  ]
}

CRITICAL: criteria must be a JSON OBJECT (not a string). Entry criteria uses: option_type, delta_range, dte_range, max_strike_pct, min_score, target_score, value_signal, min_bid, market_conditions. Do not emit min_edge_score. Exit criteria uses typed intents plus conditions. sell_put criteria must include put_exit_intent. buyback_call criteria must include buyback_intent.

Rules:
- Entry criteria MUST include: option_type ("P" or "C"), delta_range [min, max], dte_range [min, max]. Optional: max_strike_pct, min_score, target_score (for buy_put limit pricing), value_signal (for dynamic buy_put value watchers), min_bid (for sell_call), market_conditions. For sell_call, include min_score as the CALL EDGE gate and min_bid as the executable premium floor. For buy_put, rank directly by PUT EDGE and use min_score/target_score for price discipline. Do not emit min_edge_score for either action; legacy min_edge_score on saved buy_put rules is ignored. Do not add spread/IV/skew/OI/shock-payoff gates. For sell_call, market_conditions may only contain spot_price conditions; translate other selectivity into min_score, min_bid, priority, delta_range, or dte_range instead.
- Exit criteria MUST include: conditions (array of {field, op, value}) and condition_logic ("any" or "all"). Fields: dte, delta, unrealized_pnl_pct, iv, theta, spot_price. Ops: gt, lt, gte, lte. Do not use mark_price as a strategy trigger. For sell_put, include put_exit_intent. For monetize_tail_win, also include min_exit_price or limit_price. For buyback_call, include buyback_intent; set allow_below_profit_floor true only for threat_management.
- Entry rules may use preferred_order_type ${formatOrderTypeList(ENTRY_ALLOWED_ORDER_TYPES)}. For exits: roll_protection sell_put and threat_management buyback_call should use preferred_order_type "ioc". Patient monetize_tail_win sell_put and profit_capture buyback_call may use "gtc" or "post_only" as synthetic reduce-only resting exits, with limit_price/min_exit_price/max_buyback_price carrying the price discipline.
- For buy_put: set option_type "P", negative delta_range (e.g. [-0.12, -0.02]). Do not use max_cost/per-contract ask caps; use budget_limit as the total USD spend cap, with min_score/target_score/value_signal for price discipline. Raw score is abs(delta)/ask. PUT EDGE = raw_score * (DTE / ${BUY_PUT_EDGE_REFERENCE_DTE})^${BUY_PUT_EDGE_DTE_EXPONENT}; normalize every eligible candidate before ranking directly by PUT EDGE. It measures DTE-normalized delta per premium dollar, not literal crash payoff. Keep 30/40/50% drawdown payoff scenarios as descriptive intrinsic-value/premium illustrations, excluding fees; do not use spread, IV/skew, OI, or shock scenarios as another ranking multiplier or standalone veto. DTE DISCIPLINE: buy puts at 45-78 DTE. Never buy puts below 35 DTE — short-dated puts bleed theta too fast for tail insurance. dte_range must be within [45, 78].
${getFreshBestBuyPutDisciplinePrompt()}
${getStandingRulebookDisciplinePrompt()}
- For sell_put exits (rolling): roll long puts when DTE reaches ~${PUT_ROLL_DTE_THRESHOLD} only if the book already holds longer-dated long puts. Use put_exit_intent="roll_protection", requires_longer_dated_protection=true, condition dte lte ${PUT_ROLL_DTE_THRESHOLD}. This roll trigger is independent of the ${PUT_MONETIZATION_PROFIT_THRESHOLD}% monetization threshold.
- For sell_call: set option_type "C", positive delta_range (e.g. [0.04, 0.12]), min_bid for the minimum executable bid, and min_score for favorable CALL EDGE. Raw score is bid / abs(delta). CALL EDGE = raw_score * (${SELL_CALL_EDGE_REFERENCE_DTE} / DTE)^${SELL_CALL_EDGE_DTE_EXPONENT}; this light DTE normalization reduces the mechanical weekly spike when the eligible expiry range rolls forward. Rank and gate sell calls by CALL EDGE while retaining raw score for comparison. Do not emit min_edge_score for sell_call. DTE DISCIPLINE: sell calls at 5-12 DTE; dte_range must remain within [5, 12].
- For sell_put exits: IMPORTANT: sell_put means selling an already-owned long put to close, trim, or roll it. It is reduce_only and must never be interpreted as opening a naked short put. Do not sell a long-dated protective put merely because it has recoverable bid/mark value. When DTE is above ${PUT_ROLL_DTE_THRESHOLD}, use put_exit_intent="monetize_tail_win"; require executable unrealized_pnl_pct gt ${PUT_MONETIZATION_PROFIT_THRESHOLD}, retain_downside_protection=true, tranche_fraction <= ${PUT_MONETIZATION_MAX_TRANCHE_FRACTION}, and min_exit_price/limit_price. Sell chunks, never all protection at once, so the book can capture more if ETH keeps dropping or bounces around. In severe crashes with sparse books, visible bids may badly understate fair value; if monetizing, make the market at a disciplined floor from intrinsic value, Greeks, IV/skew, spread/depth, DTE, and payoff role instead of dumping into a stale bid. Selling an owned long put returns cash and reduces the hedge position, but removing hedge offsets may worsen portfolio margin; assess the remaining portfolio. If you avoid triggering a sell_put, do it because removing protection is strategically unwise or value has not delivered extreme asymmetric upside, and include any loss of portfolio hedge offsets in that assessment.
- For buyback_call exits, choose one intent. Intent 1: profit_capture / capacity reset while the short call is working. Use buyback_intent "profit_capture", condition_logic "all", and executable unrealized_pnl_pct gte ${CALL_BUYBACK_PROFIT_THRESHOLD}% as the economic condition. For patient resting attempts before the live ask reaches the capture line, also set target_capture_pct=${CALL_BUYBACK_PROFIT_THRESHOLD}, max_buyback_price to the highest price you are willing to bid, and preferred_order_type "post_only" or "gtc"; the limit price must imply at least ${CALL_BUYBACK_PROFIT_THRESHOLD}% capture if filled. This uses synthetic reduce-only guarded by live position checks and one open exit order per instrument. Do not add dte or mark_price trigger blockers; executable capture already includes the live ask and is the relevant economic threshold. Intent 2: threat_management when price is moving against the short call and time is running out for recovery inside the threatened range. Use buyback_intent "threat_management", set allow_below_profit_floor true, and use real threat conditions such as delta, spot_price vs strike, and remaining DTE. Do not mix these two intents in one rule. Do not prematurely buy back calls just because price is rising; spot can come back down and buying back fear premium can make us the sucker of the trade. The premium was already collected; a buyback below strike is buying upside insurance, not undoing a completed loss. Profit capture, expiry cleanup, margin-harvest capacity resets, genuine assignment risk, or credible breakout continuation are good reasons. Price moving against you alone is not. For unrealized_pnl_pct-based harvesting, think in executable terms: the real buyback cost is the live ask/marketable buy price, not the midpoint mark. The ${CALL_BUYBACK_PROFIT_THRESHOLD}% capture line is a minimum acceptable capture, not a target. If live executable buyback price already implies strictly better capture than a profit-capture rule, do not give back edge by bidding at the threshold. Never create a threshold-style buyback rule without a live buyback market price. Do not use margin utilization by itself as the buyback trigger. Margin release is a benefit of a good profit-harvest close, not a reason to panic-close. On upside breakouts with existing short calls, compare buyback insurance against the alternative of selling richer additional calls if margin still allows. Set conditions that reflect that tradeoff.
- budget_limit is how much USD to allocate to this rule. For puts: must stay within the remaining put budget (arithmetic discipline — we commit to a predictable spend rate per cycle). For calls: size based on margin health and ETH collateral.
- The account is ETH-collateralized. Long puts OFFSET ETH exposure in Derive's margin engine. But the premium cost is real — respect the put budget discipline.
- Put budget is an arithmetic commitment, not a cash constraint. We buy puts on leverage. The budget prevents impulse buying or underspending. Selling owned puts realizes cash but does not replenish the current cycle's put-buying budget.
- For calls: the normal target cap is ${(CALL_EXPOSURE_CAP_PCT * 100).toFixed(0)}% inferred Derive margin utilization; the ${(CALL_EXPOSURE_BUFFER_PCT * 100).toFixed(0)} percentage point buffer up to ${(CALL_EXPOSURE_LIMIT_PCT * 100).toFixed(0)}% is last-mile execution safety, not planned sell-call capacity. ${(CALL_ENTRY_CAP_PCT * 100).toFixed(0)}% is a caution threshold; the code may size down within that zone but must not deliberately fill the buffer with dust orders. In the specific case of an upside breakout with short calls already open, the active target can widen to ${(CALL_BREAKOUT_OVERRIDE_CAP_PCT * 100).toFixed(0)}% / ${(CALL_BREAKOUT_OVERRIDE_LIMIT_PCT * 100).toFixed(0)}% buffered limit so the bot can sell into richer bullish premium rather than paying up for fear-driven buybacks. This override is for breakout add-ons only, not generic leverage creep.
- Entry rules should target the best action-specific score after hard price, DTE, delta, budget, and risk gates pass. Sell calls use DTE-normalized CALL EDGE; buy_put ranks directly by DTE-normalized PUT EDGE without a composite gate.
- Exit rules MUST reference specific instrument_name from current positions
- If the market is unclear, tighten the watcher criteria and lower priority. Do not omit required standing watchers solely because they are not currently triggered.
- Maximum 5 entry rules; exit rules should cover each required open-position watcher from REQUIRED STANDING RULEBOOK COVERAGE.

Order type guidance:
- "ioc" (immediate-or-cancel): take available liquidity within the approved limit and cancel any remainder. Venue taker fees apply to fills.
- "gtc" (good-til-cancelled): a marketable limit can take liquidity immediately; any remainder rests. Maker/taker fees depend on each fill, not the GTC label.
- "post_only": rest as a maker or reject if the price would cross. Use for patient maker intent; filling is not guaranteed. Use the current venue fee schedule, not a fixed cost ratio.
- Prefer post_only for patient maker intent. Choose IOC only when the approved opportunity justifies taking liquidity.
- The confirmation step can override your suggestion, so this is advisory guidance not a hard rule.

- Return ONLY valid JSON, no markdown fences`;

  const sharedAdvisoryInputBlock = `=== CURRENT MARKET STATE ===
Quote snapshot as of ${currentTickTimestamp}. Availability applies to this observation, not the entire advisory interval.
Spot Price: $${spotPrice.toFixed(2)}

=== OPTIONS MARKET STRUCTURE (PRIMARY ADVISORY EVIDENCE) ===
${summarizeSentimentForAdvisor(sentiment?.windows || {}, sentiment?.latest?.marketQuality || [])}

=== ROLLING OPTION VALUE CONTEXT (PRIMARY ENTRY VALUE EVIDENCE) ===
${formatRollingOptionValueContext(rollingOptionValueContext)}

=== MOMENTUM (SECONDARY PATH CONTEXT; NOT A STANDALONE TRADE SIGNAL) ===
Medium-term ${momentum.mediumTerm.main} (${momentum.mediumTerm.derivative || 'n/a'}), Short-term ${momentum.shortTerm.main} (${momentum.shortTerm.derivative || 'n/a'})

=== PORTFOLIO ===
Positions: ${JSON.stringify(positions.map(p => ({
  instrument: p.instrument_name, direction: p.direction, amount: p.amount,
  delta: p.delta, theta: p.theta, unrealized_pnl: p.unrealized_pnl
})), null, 1)}

=== OPEN POSITIONS REQUIRING ADVICE ===
${positionAdviceSnapshots.length > 0 ? JSON.stringify(positionAdviceSnapshots, null, 2) : 'No open positions'}

=== REQUIRED STANDING RULEBOOK COVERAGE ===
${formatRulebookRequirements(rulebookRequirements)}

Balances: ${JSON.stringify(balances, null, 1)}

=== ACCOUNT HEALTH (margin-aware sizing) ===
${JSON.stringify(accountHealth, null, 2)}

=== SOURCE PRIORITY ===
1. Current market, portfolio, account-health, and order-book state in this prompt are primary facts.
2. Recent orders and closed-campaign trade reviews are recent empirical evidence.
3. Momentum labels are secondary path context and must not outrank executable option economics, spread/depth, IV/skew, OI, funding, or position-specific risk.
4. The knowledge wiki is compiled long-term memory. Use it for pattern recognition and discipline, but if live state conflicts with wiki memory, trust the live state and note the mismatch.

=== TOP PUT CANDIDATES (ranked directly by PUT EDGE; rolling context supplies timing) ===
Quote and 30/40/50% drawdown diagnostics are descriptive only; they do not alter ranking or add vetoes. Shock multiples are hypothetical intrinsic value divided by premium, excluding fees.
${top5Puts.length > 0 ? top5Puts.map((p, i) => `${i + 1}. ${p.name} | delta=${p.delta.toFixed(4)} | ask=$${p.askPrice.toFixed(2)} | DTE=${p.dte.toFixed(2)} | raw_score=${p.rawScore.toFixed(6)} | PUT_EDGE=${p.score.toFixed(6)} | spread_pct_of_mark=${p.spreadPct ?? 'unavailable'} | quote_as_of=${p.quoteReceivedAt ?? 'unavailable'} | shock30=${p.shockMultiples['30pct'] ?? 'n/a'}x | shock40=${p.shockMultiples['40pct'] ?? 'n/a'}x | shock50=${p.shockMultiples['50pct'] ?? 'n/a'}x | diagnostics=${JSON.stringify(p.diagnostics)}`).join('\n') : `PUT EDGE unavailable: ${rollingOptionValueContext.put_value_context.availability.reason}`}

=== TOP CALL CANDIDATES (ranked by CALL EDGE within entry DTE/delta limits) ===
${top5Calls.length > 0 ? top5Calls.map((c, i) => `${i + 1}. ${c.name} | delta=${c.delta.toFixed(4)} | bid=$${c.bidPrice.toFixed(2)} | DTE=${c.dte} | raw_score=${c.score.toFixed(4)} | CALL_EDGE=${c.selectionScore.toFixed(4)} | spread_pct_of_mark=${c.spreadPct ?? 'unavailable'} | quote_as_of=${c.quoteReceivedAt ?? 'unavailable'} | research=${c.research}/${c.dteBucket}/${c.scoreBand}`).join('\n') : `CALL EDGE unavailable: ${rollingOptionValueContext.call_value_context.availability.reason}`}

=== RECENT ORDERS (last 7d) ===
${recentOrders.length > 0 ? recentOrders.map(o => `${o.timestamp} | ${o.action} ${o.instrument_name} | ${o.success ? 'OK' : 'FAIL'} | $${o.total_value || '?'}`).join('\n') : 'No recent orders'}

=== TRADE REVIEWS (closed campaigns) ===
${recentTradeReviews.length > 0 ? recentTradeReviews.map(r => `${r.instrument_name} [${r.review_status}] [${r.review_window_days}d] | pnl=$${Number(r.pnl_realized || 0).toFixed(2)} | ${r.summary}`).join('\n') : 'No trade reviews yet'}

=== CURRENT ACTIVE RULES ===
${summarizeActiveRulesForAdvisor(activeRules)}

=== RECENT PENDING ACTIONS ===
${summarizePendingActionsForAdvisor(recentPendingActions)}

=== OPEN ORDERS ON BOOK ===
${summarizeOpenOrdersForAdvisor(openOrders)}
${wikiSignals ? `\n=== WIKI SIGNALS (parsed from knowledge base) ===
Regime: ${wikiSignals.regime || 'unknown'} (confidence: ${wikiSignals.regimeConfidence || 'unknown'})
Protection cost: ${wikiSignals.protectionAssessment || 'unknown'}
Call premium: ${wikiSignals.revenueAssessment || 'unknown'}
Knowledge warnings: ${wikiSignals.warnings?.length ? wikiSignals.warnings.join('; ') : 'none'}
${wikiSignals.playbookRules.length > 0 ? `Playbook rules:\n${wikiSignals.playbookRules.map(r => `- ${r}`).join('\n')}` : ''}` : ''}
=== MANDELBROT MARKET STRUCTURE ARCHIVE ===
${buildMandelbrotContextBlock(mandelbrotContext)}
${wikiContext ? `\n=== KNOWLEDGE WIKI (cumulative bot knowledge) ===\n${wikiContext}` : ''}

Use the Mandelbrot archive as descriptive market-structure context. It highlights what should matter intellectually, but it does not prescribe trade actions.`;

  const primaryUserPrompt = `${sharedAdvisoryInputBlock}

Produce your trading agenda JSON now.`;

  const advisoryAnthropicModel = ANTHROPIC_STRATEGY_MODEL;

  let primaryAgenda = null;
  try {
    const primaryResponse = await callAnthropicWithMinuteBoundaryRetry({
      label: 'Advisory Step 1',
      model: advisoryAnthropicModel,
      maxTokens: 16384,
      thinking: { type: 'adaptive' },
      system: primarySystemPrompt,
      messages: [{ role: 'user', content: primaryUserPrompt }],
      timeout: 120000,
    });

    const primaryText = getAnthropicResponseText(primaryResponse.data);
    try {
      primaryAgenda = extractJSON(primaryText);
      if (primaryAgenda) {
        if (typeof primaryAgenda.assessment === 'string') {
          const unsupportedPattern = assessmentUsesUnsupportedMetricLanguage(primaryAgenda.assessment);
          if (unsupportedPattern) {
            console.log(`📋 Advisory Step 1: assessment contains unsupported wording (${unsupportedPattern}); preserving advisor text`);
          }
        }
        console.log(`📋 Advisory Step 1: got ${primaryAgenda.entry_rules?.length || 0} entry rules, ${primaryAgenda.exit_rules?.length || 0} exit rules`);
      } else {
        throw new Error('No JSON block found in primary response');
      }
    } catch (parseErr) {
      console.log('📋 Advisory Step 1: JSON parse failed:', parseErr.message);
      throw parseErr;
    }
  } catch (e) {
    console.log('📋 Advisory Step 1 FAILED:', getAnthropicErrorMessage(e));
    throw e; // Primary failure is fatal
  }

  // ── Step 2: Second Opinion (OpenAI GPT, Taleb temperament) ─────────────────

  console.log('📋 Advisory Step 2: Taleb review (OpenAI GPT)...');
  let secondOpinion = null;
  try {
    const talebSystem = `You are the Taleb Advisor reviewing a trading agenda from the Spitznagel Advisor.

## Your Temperament
You think like Nassim Taleb. You believe in:
- Antifragility. Position to BENEFIT from disorder, not just survive it.
- Payoff shape. Assess each action's actual option payoff and portfolio exposure; require survival within the stated position, margin, and budget constraints, not unbounded upside from every trade.
- Skin in the game. If a trade goes wrong, the cost must be small and known.
- Fat tails. The market is more volatile than anyone thinks. Events that "shouldn't happen" happen regularly.
- Via negativa. What you DON'T do matters more than what you do. Avoid ruin above all.

Interpret "Taleb" operationally, not stylistically:
- Your job is not to optimize average-case neatness; it is to detect hidden fragility, linear exposure, and ruin-adjacent logic.
- Ask whether a rule gains from disorder, merely survives disorder, or quietly assumes disorder will stop.
- Prefer removing bad exposures to adding clever-looking complexity. Via negativa beats elaborate repair.
- Scrutinize any reasoning that depends on smooth markets, tidy distributions, or continuation of recent calm.
- Treat convexity as real only if downside is small, explicit, and survivable while upside from disorder is meaningfully larger.
- Be especially hostile to paying fear premiums, roll-for-relief logic, and exits that feel safe but worsen long-run asymmetry.
- Evidence that matters most: bounded downside, tail sensitivity, concentration risk, hidden path dependence, and whether the portfolio becomes more fragile if the market gets wilder.

## Shared Evidence Discipline
${getAdvisoryDataQualityPrompt()}
${getMomentumEvidenceDisciplinePrompt()}

## DTE Discipline (Non-Negotiable)
- Buy puts at 45-78 DTE. Never below 35 DTE. Short-dated puts bleed theta — you're paying for time decay, not convexity. Veto any buy_put rule outside [45, 78].
- Roll (sell_put exit) at ~${PUT_ROLL_DTE_THRESHOLD} DTE only when the book already holds longer-dated long puts. When DTE is above ${PUT_ROLL_DTE_THRESHOLD}, only consider monetizing protective puts after executable unrealized_pnl_pct is greater than ${PUT_MONETIZATION_PROFIT_THRESHOLD}%, and only in tranches with retained downside protection.
- Sell calls at 5-12 DTE. Short-dated calls maximize theta harvesting. Veto any sell_call rule with dte above 14.

## Fresh-Best Put Value Discipline
${getBuyPutEntryPriceDisciplinePrompt()}
${getFreshBestBuyPutDisciplinePrompt()}
${getStandingRulebookDisciplinePrompt()}

## Call Buyback Anti-Fragility (Taleb)
Panic buybacks are the opposite of antifragility. The crowd buys back calls when price rises because it FEELS dangerous. That's paying a fear premium — the exact behavior we profit from.
- The asymmetry of short calls is known and bounded. You sold time decay. The question is always: is the position genuinely threatened, or does it just feel that way?
- Scrutinize any buyback rule that triggers on price movement alone. Ask: is the portfolio actually at risk of ruin, or is this noise?
- Do not veto advisor-led take-profit buyback rules merely because they free margin. If live executable economics are worse than the ${CALL_BUYBACK_PROFIT_THRESHOLD}% capture line and the advisor names a patient synthetic reduce-only resting price that would improve the exit if filled, that is disciplined harvesting, not panic. If live economics already beat that line, do not let the agenda bid back up to the threshold.
- Rolling for a net debit is paying to extend exposure. If you can't roll favorably, accepting assignment is the antifragile response — it means you were right about the price level when you sold.
- Use your judgment on what constitutes a real threat vs. noise. The Greeks, remaining DTE, premium captured, and portfolio shape tell the story — not the last candle.

## Market Regime Awareness
ETH crashes cascade — they accelerate, not slow down. Your critique should consider:
- Selling puts during an active crash means selling convexity that could multiply further. Scrutinize the timing.
- For buy_put, verify fresh PUT EDGE at the proposed bid against the approved min_score/target_score; spiked IV is descriptive and is not a standalone veto.
- In recovery (price stabilizing, IV still elevated), selling puts to fearful buyers can capture inflated premiums.
- But these are tendencies, not absolutes. The actual Greeks, DTE, position size, and portfolio shape matter. Use your judgment.
- Ask: is this trade benefiting from disorder (antifragile) or just reacting to it (fragile)?

## Writing Constraints
- In your critique and any replacement assessment language, include a concrete stance. If the correct answer is patience, say so plainly.
- Do not invent metric labels or threshold language that is not explicitly present in the shared advisory input.
- If you discuss put budget, refer only to the supplied budget fields in plain language. Do not rename them into efficiencies, scores, or deployment thresholds.`;

    const talebPrompt = `## Shared Advisory Input
${sharedAdvisoryInputBlock}

## The Agenda to Review
${JSON.stringify(primaryAgenda)}

## Your Task
Critique the agenda using the same advisory input set Spitznagel saw. For each rule, ask:
1. Is the downside truly bounded? What's the worst case?
2. Where is the convexity? Is the asymmetry real or imagined?
3. Are we being antifragile or just hedged?
4. What would the naive crowd do here, and are we positioned opposite them?
5. Is this rule justified by executable option value and market structure, or is it leaning too hard on momentum labels?

Output JSON only:
{
  "critique": "Overall assessment of the agenda",
  "amendments": [{"rule_index": 0, "concern": "...", "suggested_change": {...}, "severity": "medium"}],
  "vetoes": [{"rule_index": 0, "reason": "..."}],
  "additions": []
}`;

    const talebText = await callOpenAI(talebSystem, talebPrompt, { maxTokens: 16384, timeout: 120000, model: OPENAI_STRATEGY_MODEL });
    if (talebText) {
      secondOpinion = parseTalebSecondOpinion(talebText);
      if (secondOpinion) {
        if (secondOpinion._parse_fallback) {
          console.log('📋 Taleb review: parse fallback to raw text');
        }
        console.log(`📋 Taleb review: ${secondOpinion.vetoes?.length || 0} vetoes, ${secondOpinion.amendments?.length || 0} amendments`);
      }
    }
  } catch (e) {
    console.log(`📋 Taleb review failed (non-fatal): ${e.message}`);
  }

  // ── Step 3: Synthesis (Claude Sonnet) ───────────────────────────────────────

  console.log('📋 Advisory Step 3: Synthesis (Claude Sonnet)...');

  let finalAgenda = primaryAgenda; // Default: use primary if synthesis fails
  const synthesisAnthropicModel = ANTHROPIC_SONNET_MODEL;

  const synthesisSystemPrompt = secondOpinion
    ? `You are the Synthesizer on a trading council. You have two advisor inputs. Your job is to produce the final trading agenda.

CRITICAL: criteria must be a JSON OBJECT, not a string.
- Entry criteria: { "option_type": "P"|"C", "delta_range": [min, max], "dte_range": [min, max], ... }
- Exit criteria: { "conditions": [{"field": "dte"|"unrealized_pnl_pct"|..., "op": "lt"|"gt"|"gte"|"lte", "value": number}], "condition_logic": "any"|"all", "put_exit_intent": "roll_protection"|"monetize_tail_win", "min_exit_price": number, "buyback_intent": "profit_capture"|"threat_management" }
- The "assessment" must include both the market observation/thesis and the operational stance. If the stance is patience, say so plainly.
- The "assessment" must include a "Thesis breakdown:" section with one bullet for every current open position, naming each instrument exactly and stating the position-specific stance and rationale.
- In "assessment", use only facts and metric names explicitly present in the advisor inputs or policy constants. Never invent efficiency labels, scores, or thresholds.
${getAdvisoryDataQualityPrompt()}
${getMomentumEvidenceDisciplinePrompt()}
${getFreshBestBuyPutDisciplinePrompt()}
${getStandingRulebookDisciplinePrompt()}
- Preserve advisor-led low-urgency buyback_call profit_capture rules that use a patient synthetic reduce-only resting limit to improve an exit toward at least ${CALL_BUYBACK_PROFIT_THRESHOLD}% call premium capture, unless the economics or risk rationale are internally inconsistent. If live executable capture is already better than the rule target, prefer hold/let expire or a lower bid rather than bidding back up. Do not turn margin utilization or price rising alone into a buyback trigger.

Return the FINAL trading agenda as JSON:
{
  "assessment": "synthesized assessment",
  "entry_rules": [...],
  "exit_rules": [...]
}

Return ONLY valid JSON, no markdown fences.`
    : `You are a risk-management synthesizer for an options trading bot. You have a single advisor opinion to validate. Your job:
- Check for internal consistency (do entry rules match budget constraints?)
- Verify exit rules cover existing positions
- Flag any rules that seem overaggressive for current conditions
- Pass through valid rules, remove or adjust problematic ones

CRITICAL: criteria must be a JSON OBJECT, not a string.
- Entry criteria: { "option_type": "P"|"C", "delta_range": [min, max], "dte_range": [min, max], ... }
- Exit criteria: { "conditions": [{"field": "dte"|"unrealized_pnl_pct"|..., "op": "lt"|"gt"|"gte"|"lte", "value": number}], "condition_logic": "any"|"all", "put_exit_intent": "roll_protection"|"monetize_tail_win", "min_exit_price": number, "buyback_intent": "profit_capture"|"threat_management" }
- The "assessment" must include both the market observation/thesis and the operational stance. If the stance is patience, say so plainly.
- The "assessment" must include a "Thesis breakdown:" section with one bullet for every current open position, naming each instrument exactly and stating the position-specific stance and rationale.
- In "assessment", use only facts and metric names explicitly present in the advisor inputs or policy constants. Never invent efficiency labels, scores, or thresholds.
${getAdvisoryDataQualityPrompt()}
${getMomentumEvidenceDisciplinePrompt()}
${getFreshBestBuyPutDisciplinePrompt()}
${getStandingRulebookDisciplinePrompt()}
- Preserve advisor-led low-urgency buyback_call profit_capture rules that use a patient synthetic reduce-only resting limit to improve an exit toward at least ${CALL_BUYBACK_PROFIT_THRESHOLD}% call premium capture, unless the economics or risk rationale are internally inconsistent. If live executable capture is already better than the rule target, prefer hold/let expire or a lower bid rather than bidding back up. Do not turn margin utilization or price rising alone into a buyback trigger.

Return the FINAL trading agenda as JSON:
{
  "assessment": "synthesized assessment",
  "entry_rules": [...],
  "exit_rules": [...]
}

Return ONLY valid JSON, no markdown fences.`;

  const synthesisUserPrompt = secondOpinion
    ? `You are the Synthesizer on a trading council. You have two advisor inputs:

## Spitznagel Advisor's Agenda
${JSON.stringify(primaryAgenda, null, 2)}

## Taleb Advisor's Review
${JSON.stringify(secondOpinion, null, 2)}

## Rolling Option Value Context
${formatRollingOptionValueContext(rollingOptionValueContext)}

## Required Standing Rulebook Coverage
${formatRulebookRequirements(rulebookRequirements)}

## Mandelbrot Market-Structure Archive
${buildMandelbrotContextBlock(mandelbrotContext)}

## Rules for Synthesis:
- VETOES are binding: if Taleb vetoes a rule, remove it
- AMENDMENTS are suggestions: apply if they improve convexity without breaking margin discipline
- Mandelbrot archive is descriptive context only. It characterizes market structure and regime shape; it does not prescribe trade ideas or rule translations.
- The Spitznagel advisor's sizing limits take precedence (arithmetic discipline)
- Taleb's concerns about fat-tail exposure should be taken seriously
- When advisors agree, high confidence. When they disagree, reduce priority or tighten conditions.

=== ACCOUNT HEALTH ===
${JSON.stringify(accountHealth, null, 2)}
- Current positions: ${positions.length} open
- Spot: $${spotPrice.toFixed(2)}

=== OPEN POSITIONS REQUIRING THESIS BREAKDOWN ===
${positionAdviceSnapshots.length > 0 ? JSON.stringify(positionAdviceSnapshots, null, 2) : 'No open positions'}

Output the FINAL trading agenda JSON (same format as Spitznagel's output — with assessment, entry_rules, exit_rules).`
    : `=== PRIMARY ADVISOR AGENDA ===
${JSON.stringify(primaryAgenda, null, 2)}

=== SECOND OPINION ===
Not available (OpenAI key not set or call failed). Validate and pass through the primary agenda.

=== ROLLING OPTION VALUE CONTEXT ===
${formatRollingOptionValueContext(rollingOptionValueContext)}

=== REQUIRED STANDING RULEBOOK COVERAGE ===
${formatRulebookRequirements(rulebookRequirements)}

=== MANDELBROT MARKET STRUCTURE ARCHIVE ===
${buildMandelbrotContextBlock(mandelbrotContext)}

=== ACCOUNT HEALTH ===
${JSON.stringify(accountHealth, null, 2)}
- Current positions: ${positions.length} open
- Spot: $${spotPrice.toFixed(2)}

=== OPEN POSITIONS REQUIRING THESIS BREAKDOWN ===
${positionAdviceSnapshots.length > 0 ? JSON.stringify(positionAdviceSnapshots, null, 2) : 'No open positions'}

Synthesize the final agenda now.`;

  try {
    await waitUntilNextMinuteBoundary('Advisory Step 3');
    const synthesisResponse = await callAnthropicWithMinuteBoundaryRetry({
      label: 'Advisory Step 3',
      model: synthesisAnthropicModel,
      maxTokens: 3072,
      system: synthesisSystemPrompt,
      messages: [{ role: 'user', content: synthesisUserPrompt }],
      timeout: 120000, // Step 3 writes a full 3072-token agenda; 60s was timing out every run
      spreadAfterBoundary: true,
    });

    const synthesisText = getAnthropicResponseText(synthesisResponse.data);
    try {
      const synthesized = extractJSON(synthesisText);
      if (synthesized) {
        finalAgenda = synthesized;
        if (typeof finalAgenda.assessment === 'string') {
          const unsupportedPattern = assessmentUsesUnsupportedMetricLanguage(finalAgenda.assessment);
          if (unsupportedPattern) {
            console.log(`📋 Advisory Step 3: assessment contains unsupported wording (${unsupportedPattern}); preserving advisor text`);
          }
        }
        console.log(`📋 Advisory Step 3: synthesized ${finalAgenda.entry_rules?.length || 0} entry rules, ${finalAgenda.exit_rules?.length || 0} exit rules`);
      } else {
        console.log('📋 Advisory Step 3: no JSON in synthesis response, using primary agenda');
      }
    } catch (parseErr) {
      console.log('📋 Advisory Step 3: JSON parse failed, using primary agenda:', parseErr.message);
    }
  } catch (e) {
    console.log('📋 Advisory Step 3 FAILED, using primary agenda:', getAnthropicErrorMessage(e));
  }

  finalAgenda.assessment = ensureAssessmentHasPositionPlan({
    assessment: selectAssessmentText(finalAgenda.assessment, primaryAgenda.assessment),
    positionSnapshots: positionAdviceSnapshots,
    exitRules: finalAgenda.exit_rules || [],
  });

  let missingRulebookCoverage = findMissingRulebookRequirements(finalAgenda, rulebookRequirements);
  if (missingRulebookCoverage.length > 0) {
    try {
      console.log(`📋 Advisory ${advisoryId}: requesting rulebook coverage repair for ${missingRulebookCoverage.length} missing watcher(s)`);
      const repairResponse = await callAnthropicWithMinuteBoundaryRetry({
        label: 'Advisory Step 3b',
        model: synthesisAnthropicModel,
        maxTokens: 3072,
        system: `You repair an options bot standing rulebook. Add or amend only the missing watcher rules needed to satisfy REQUIRED STANDING RULEBOOK COVERAGE. The favorable conditions must come from the supplied market, account, and position facts. Preserve valid existing rules unless they contradict risk discipline.
${getAdvisoryDataQualityPrompt()}
${getMomentumEvidenceDisciplinePrompt()}
${getFreshBestBuyPutDisciplinePrompt()}
${getStandingRulebookDisciplinePrompt()}
Return ONLY valid JSON with assessment, entry_rules, and exit_rules. No markdown fences.`,
        messages: [{
          role: 'user',
          content: `Repair this final agenda so it covers all required standing watchers.

=== MISSING REQUIRED WATCHERS ===
${formatRulebookRequirements(missingRulebookCoverage)}

=== CURRENT FINAL AGENDA ===
${JSON.stringify(finalAgenda, null, 2)}

=== ROLLING OPTION VALUE CONTEXT ===
${formatRollingOptionValueContext(rollingOptionValueContext)}

=== ACCOUNT HEALTH ===
${JSON.stringify(accountHealth, null, 2)}

=== OPEN POSITIONS REQUIRING WATCHERS ===
${positionAdviceSnapshots.length > 0 ? JSON.stringify(positionAdviceSnapshots, null, 2) : 'No open positions'}

=== TOP PUT CANDIDATES (ranked directly by PUT EDGE) ===
The 30/40/50% drawdown scenarios and other quote diagnostics are descriptive only, not additional ranking inputs or vetoes.
${top5Puts.length > 0 ? top5Puts.map((p, i) => `${i + 1}. ${p.name} | delta=${p.delta.toFixed(4)} | ask=$${p.askPrice.toFixed(2)} | DTE=${p.dte.toFixed(2)} | raw_score=${p.rawScore.toFixed(6)} | PUT_EDGE=${p.score.toFixed(6)} | spread_pct_of_mark=${p.spreadPct ?? 'unavailable'} | quote_as_of=${p.quoteReceivedAt ?? 'unavailable'} | shock30=${p.shockMultiples['30pct'] ?? 'n/a'}x | shock40=${p.shockMultiples['40pct'] ?? 'n/a'}x | shock50=${p.shockMultiples['50pct'] ?? 'n/a'}x | diagnostics=${JSON.stringify(p.diagnostics)}`).join('\n') : `PUT EDGE unavailable: ${rollingOptionValueContext.put_value_context.availability.reason}`}

=== TOP CALL CANDIDATES ===
${top5Calls.length > 0 ? top5Calls.map((c, i) => `${i + 1}. ${c.name} | delta=${c.delta.toFixed(4)} | bid=$${c.bidPrice.toFixed(2)} | DTE=${c.dte} | raw_score=${c.score.toFixed(4)} | CALL_EDGE=${c.selectionScore.toFixed(4)} | spread_pct_of_mark=${c.spreadPct ?? 'unavailable'} | quote_as_of=${c.quoteReceivedAt ?? 'unavailable'} | research=${c.research}/${c.dteBucket}/${c.scoreBand}`).join('\n') : `CALL EDGE unavailable: ${rollingOptionValueContext.call_value_context.availability.reason}`}

Return the full repaired agenda JSON.`,
        }],
        timeout: 120000,
      });
      const repairedText = getAnthropicResponseText(repairResponse.data);
      const repairedAgenda = extractJSON(repairedText);
      if (repairedAgenda) {
        const previousAgenda = finalAgenda || {};
        finalAgenda = {
          ...previousAgenda,
          ...repairedAgenda,
          assessment: selectAssessmentText(
            repairedAgenda.assessment,
            previousAgenda.assessment,
            primaryAgenda.assessment
          ),
          entry_rules: Array.isArray(repairedAgenda.entry_rules)
            ? repairedAgenda.entry_rules
            : (Array.isArray(previousAgenda.entry_rules) ? previousAgenda.entry_rules : []),
          exit_rules: Array.isArray(repairedAgenda.exit_rules)
            ? repairedAgenda.exit_rules
            : (Array.isArray(previousAgenda.exit_rules) ? previousAgenda.exit_rules : []),
        };
        finalAgenda.assessment = ensureAssessmentHasPositionPlan({
          assessment: selectAssessmentText(finalAgenda.assessment, previousAgenda.assessment, primaryAgenda.assessment),
          positionSnapshots: positionAdviceSnapshots,
          exitRules: finalAgenda.exit_rules || [],
        });
        missingRulebookCoverage = findMissingRulebookRequirements(finalAgenda, rulebookRequirements);
        console.log(`📋 Advisory Step 3b: repaired agenda, missing watchers remaining=${missingRulebookCoverage.length}`);
      } else {
        console.log('📋 Advisory Step 3b: no JSON in repair response');
      }
    } catch (e) {
      console.log('📋 Advisory Step 3b FAILED:', getAnthropicErrorMessage(e));
    }
  }

  if (missingRulebookCoverage.length > 0) {
    const missingText = missingRulebookCoverage
      .map((req) => `${req.type}/${req.action}${req.instrument_name ? ` ${req.instrument_name}` : ''}`)
      .join(', ');
    console.log(`📋 Advisory ${advisoryId}: missing required standing watcher(s): ${missingText}`);
    finalAgenda.assessment = `${finalAgenda.assessment}\n\nRulebook coverage warning: missing ${missingText}.`;
  }

  // ── Persist rules to database ───────────────────────────────────────────────

  const allRules = [];

  // Parse entry rules
  if (finalAgenda.entry_rules && Array.isArray(finalAgenda.entry_rules)) {
    for (const rule of finalAgenda.entry_rules) {
      const rawRule = {
        rule_type: 'entry',
        action: rule.action,
        instrument_name: null,
        criteria: rule.criteria,
        budget_limit: rule.budget_limit ?? null,
        priority: rule.priority || 'medium',
        reasoning: rule.reasoning || null,
        advisory_id: advisoryId,
        preferred_order_type: normalizePreferredOrderType(rule.action, rule.preferred_order_type),
      };
      const validation = validateAdvisorRuleContract(rawRule, { positionSnapshots: positionAdviceSnapshots });
      if (!validation.valid) {
        console.log(`📋 Advisory ${advisoryId}: rejected ${rawRule.rule_type}/${rawRule.action} rule contract — ${validation.reason}`);
        continue;
      }
      allRules.push(rawRule);
    }
  }

  // Parse exit rules
  if (finalAgenda.exit_rules && Array.isArray(finalAgenda.exit_rules)) {
    for (const rule of finalAgenda.exit_rules) {
      const rawRule = {
        rule_type: 'exit',
        action: rule.action,
        instrument_name: rule.instrument_name || null,
        criteria: rule.criteria,
        budget_limit: null,
        priority: rule.priority || 'medium',
        reasoning: rule.reasoning || null,
        advisory_id: advisoryId,
        preferred_order_type: normalizePreferredOrderType(rule.action, rule.preferred_order_type),
      };
      const validation = validateAdvisorRuleContract(rawRule, { positionSnapshots: positionAdviceSnapshots });
      if (!validation.valid) {
        console.log(`📋 Advisory ${advisoryId}: rejected ${rawRule.rule_type}/${rawRule.action} ${rawRule.instrument_name || ''} rule contract — ${validation.reason}`);
        continue;
      }
      const normalized = normalizeBuybackCaptureFloor(rawRule);
      if (normalized.changed) {
        console.log(`📋 Advisory ${advisoryId}: ${normalized.reason} for ${rawRule.instrument_name}`);
      }
      allRules.push(normalized.rule);
    }
  }

  const postValidationMissingCoverage = findMissingRulebookRequirements(
    buildAgendaFromValidatedRules(allRules),
    rulebookRequirements
  );
  let fallbackRulesAdded = 0;
  for (const requirement of postValidationMissingCoverage) {
    const fallbackRule = buildCanonicalRequiredWatcherRule(requirement, {
      advisoryId,
      positionSnapshots: positionAdviceSnapshots,
    });
    if (!fallbackRule) continue;

    const validation = validateAdvisorRuleContract(fallbackRule, { positionSnapshots: positionAdviceSnapshots });
    if (!validation.valid) {
      console.log(`📋 Advisory ${advisoryId}: canonical fallback rejected for ${requirement.action} ${requirement.instrument_name || ''} — ${validation.reason}`);
      continue;
    }
    allRules.push(fallbackRule);
    fallbackRulesAdded++;
    console.log(`📋 Advisory ${advisoryId}: added canonical ${fallbackRule.action} watcher for ${fallbackRule.instrument_name} after validation coverage check`);
  }
  if (fallbackRulesAdded > 0) {
    const remainingMissing = findMissingRulebookRequirements(
      buildAgendaFromValidatedRules(allRules),
      rulebookRequirements
    );
    if (remainingMissing.length > 0) {
      const remainingText = remainingMissing
        .map((req) => `${req.type}/${req.action}${req.instrument_name ? ` ${req.instrument_name}` : ''}`)
        .join(', ');
      console.log(`📋 Advisory ${advisoryId}: required watcher coverage still missing after fallback: ${remainingText}`);
    }
  }

  const persistedAgenda = buildAgendaFromValidatedRules(allRules);
  finalAgenda.assessment = ensureAssessmentHasPositionPlan({
    assessment: selectAssessmentText(finalAgenda.assessment, primaryAgenda.assessment),
    positionSnapshots: positionAdviceSnapshots,
    exitRules: persistedAgenda.exit_rules,
  });

  return { advisoryId, allRules, finalAgenda, primaryAgenda, secondOpinion, mandelbrotContext,
    rollingOptionValueContext, persistedAgenda, spotPrice };
};

const publishTradingAdvisoryDraft = (draft, { inputSnapshot, checkedSnapshot, attempt }) => {
  const { advisoryId, allRules, finalAgenda, primaryAgenda, secondOpinion, mandelbrotContext,
    rollingOptionValueContext, persistedAgenda, spotPrice } = draft;
  const publication = {
    input_as_of: inputSnapshot.marketTimestamp,
    quote_state_checked_at: checkedSnapshot.marketTimestamp,
    review_attempts: attempt,
    input_quote_availability: inputSnapshot.quoteAvailability,
    checked_quote_availability: checkedSnapshot.quoteAvailability,
  };
  const assessmentText = `Market inputs as of ${publication.input_as_of}; quote availability rechecked ${publication.quote_state_checked_at}.\n\n${selectAssessmentText(finalAgenda.assessment, primaryAgenda.assessment)}`;
  const journalEntries = [
    { entry_type: 'advisory', content: assessmentText },
    { entry_type: 'advisory_main', content: assessmentText },
  ];
  if (primaryAgenda?.assessment) journalEntries.push({ entry_type: 'advisory_spitznagel', content: primaryAgenda.assessment });
  if (secondOpinion) journalEntries.push({ entry_type: 'advisory_taleb', content: JSON.stringify(secondOpinion, null, 2) });
  if (mandelbrotContext) journalEntries.push({ entry_type: 'mandelbrot_archive', content: JSON.stringify(mandelbrotContext, null, 2) });
  journalEntries.push({ entry_type: 'advisory_context', content: JSON.stringify({
    advisory_id: advisoryId, rolling_option_value_context: rollingOptionValueContext, publication,
  }, null, 2) });
  if (!db?.publishAdvisory) throw new Error('Advisory publication storage unavailable');
  db.publishAdvisory(advisoryId, allRules, journalEntries);
  console.log(`📋 Advisory ${advisoryId}: published ${allRules.length} rules and matching assessment`);

  const entryCount = persistedAgenda.entry_rules.length;
  const exitCount = persistedAgenda.exit_rules.length;
  const rawEntryCount = finalAgenda.entry_rules?.length || 0;
  const rawExitCount = finalAgenda.exit_rules?.length || 0;
  console.log(`📋 Advisory ${advisoryId}: complete — ${entryCount} active entry rules, ${exitCount} active exit rules (${rawEntryCount} entry/${rawExitCount} exit before validation/fallback)`);

  // Track advisory state for dashboards and retry cadence (persisted to survive restarts)
  botData.lastAdvisorySpotPrice = spotPrice;
  botData.lastAdvisoryTimestamp = Date.now();
  botData.lastAdvisorySuccess = Date.now();
  botData.lastAdvisoryError = null;
  botData.advisoryRetryCount = 0;
  botData.nextAdvisoryRetryAt = 0;
  persistCycleState();

  return { advisoryId, agenda: finalAgenda, rulesCount: allRules.length };
};

const readFreshTradingAdvisorySnapshot = () => readAdvisoryMarketSnapshot({
  fetchSpot: async () => normalizeEthSpotPrice(await fetchDeriveSpotPrice() || await fetchCoinGeckoSpotPrice()),
  fetchInstruments: async () => (await fetchAndFilterInstruments(null, { throwOnError: true })).instruments,
  fetchPositions: () => fetchPositions({ throwOnError: true }),
  fetchTickers: expiry => fetchTickersByExpiry(expiry, { throwOnError: true }),
});

const generateTradingAdvisory = async ({ trigger = 'scheduled' } = {}) => {
  if (!process.env.ANTHROPIC_API_KEY) {
    console.log('📋 Advisory: skipped — no ANTHROPIC_API_KEY');
    return null;
  }
  if (_advisoryInFlight) {
    console.log('📋 Advisory: skipped — already in flight');
    return null;
  }
  _advisoryInFlight = true;
  try {
    const advisoryId = `adv_${Date.now()}`;
    botData.lastAdvisoryRun = Date.now();
    botData.lastAdvisoryError = null;
    persistCycleState();
    console.log(`📋 Advisory ${advisoryId}: starting deliberation with fresh market inputs...`);
    return await runReviewedPublication({
      readSnapshot: readFreshTradingAdvisorySnapshot,
      review: (snapshot, { attempt }) => {
        // Notify only actual reviews, after a valid snapshot and the run mutex.
        // Keep delivery independent of model work and publication success.
        void notifyAdvisoryRun({ advisoryId, trigger, attempt, retryCount: botData.advisoryRetryCount || 0 });
        return buildTradingAdvisoryDraft(snapshot, advisoryId);
      },
      publish: publishTradingAdvisoryDraft,
      maxAttempts: 2,
      onRefresh: ({ comparison }) => console.log(`📋 Advisory ${advisoryId}: refreshing all reviews before publication — ${comparison.reason}`),
    });
  } catch (e) {
    botData.lastAdvisoryError = e.message;
    botData.advisoryRetryCount = (botData.advisoryRetryCount || 0) + 1;
    botData.nextAdvisoryRetryAt = Date.now() + getAdvisoryRetryDelayMs(botData.advisoryRetryCount);
    persistCycleState();
    throw e;
  } finally {
    _advisoryInFlight = false;
  }
};

// ─── Bot Loop ────────────────────────────────────────────────────────────────

let lastEconomicSyncAt = 0;
const syncEconomicEvidence = async (now = Date.now()) => {
  if (!economicStore || now - lastEconomicSyncAt < 15 * 60 * 1000) return;
  lastEconomicSyncAt = now;

  const lastCovered = economicStore.latestCoverage(SUBACCOUNT_ID, 'trades', ECONOMIC_TRACKING_START);
  const from = Math.max(Date.parse(ECONOMIC_TRACKING_START), Date.parse(lastCovered || ECONOMIC_TRACKING_START) - 60000);
  try {
    economicStore.recordExposure(SUBACCOUNT_ID, String(PUT_INSURED_EXTERNAL_ETH), now);
    const result = await syncV2TradesProgressively({
      store: economicStore, accountId: SUBACCOUNT_ID, from, to: now,
      post: async (body) => {
        const wallet = createWallet();
        const timestamp = Date.now();
        const signature = await signMessage(wallet, timestamp);
        const response = await axios.post(API_URL.GET_TRADE_HISTORY, body, {
          headers: { 'X-LyraWallet': DERIVE_ACCOUNT_ADDRESS, 'X-LyraTimestamp': String(timestamp), 'X-LyraSignature': signature },
          timeout: 15000,
        });
        if (response.data?.error) throw new Error(stringifyApiError(response.data.error));
        return response.data?.result;
      },
    });
    console.log(`Accounting evidence: reconciled ${result.count} venue trades (${result.inserted} new)`);
  } catch (error) {
    console.log(`Accounting evidence incomplete: ${error.message}`);
  }
};

const runBot = async () => {
  try {
  const now = Date.now();

  console.log(' ');
  console.log('─'.repeat(60));
  console.log(`🥱 NO OPERATION RUN`);

  // Get spot price — prefer Derive, fall back to CoinGecko only if needed
  let spotPrice = await fetchDeriveSpotPrice();
  if (!spotPrice) {
    spotPrice = await fetchCoinGeckoSpotPrice();
  }
  spotPrice = normalizeEthSpotPrice(spotPrice);

  // Shared timestamp for all DB writes this tick
  const tickTimestamp = new Date().toISOString();

  let onchainAnalysis = null;

  // Fetch instruments once and filter for both strategies
  let instruments = [];
  let putCandidates = [];
  let callCandidates = [];

  try {
    const fetchResult = await fetchAndFilterInstruments(spotPrice);
    instruments = fetchResult.instruments || [];
    putCandidates = fetchResult.putCandidates || [];
    callCandidates = fetchResult.callCandidates || [];
  } catch (error) {
    console.error('❌ Error fetching instruments:', error.message);
    console.log('⚠️ Continuing with empty instrument lists to prevent script exit');
  }

    // Historical options analysis removed (replaced by LLM advisory)

    // Batch-fetch AMM tickers per unique expiry (get_tickers returns AMM prices)
    const allCandidates = [...putCandidates, ...callCandidates];
    console.log(`🔍 Fetching AMM tickers for ${allCandidates.length} total candidates (${putCandidates.length} PUT + ${callCandidates.length} CALL)...`);

    // Extract unique expiry dates from instrument names (e.g. "ETH-20260424-1400-P" → "20260424")
    const expiryDates = [...new Set(allCandidates.map(i => i.instrument_name.split('-')[1]))];
    console.log(`📅 Unique expiry dates: ${expiryDates.join(', ')} (${expiryDates.length} batch calls)`);

    // Fetch tickers for all expiries in parallel
    const tickerResults = await Promise.all(
      expiryDates.map(expiry => fetchTickersByExpiry(expiry))
    );

    // Merge all ticker maps into one keyed by instrument_name
    const tickerMap = {};
    for (const tickers of tickerResults) {
      for (const [name, data] of Object.entries(tickers)) {
        tickerMap[name] = data;
      }
    }
    console.log(`📊 Ticker map contains ${Object.keys(tickerMap).length} instruments`);

    // Continue observing held and evaluated contracts even after they become ITM
    // or leave the entry window. This universe never authorizes a new trade.
    const positions = await fetchPositions();
    const pendingObservationSymbols = db?.getObservationInstruments
      ? db.getObservationInstruments(new Date().toISOString()) : [];
    const observedInstruments = observationUniverse({
      instruments, candidates: allCandidates, positions, pendingSymbols: pendingObservationSymbols,
    });
    const fetchedExpiryDates = new Set(expiryDates);
    const observationExpiries = missingExpiryDates(observedInstruments, fetchedExpiryDates);
    if (observationExpiries.length > 0) {
      const results = await Promise.all(observationExpiries.map(e => fetchTickersByExpiry(e)));
      for (let i = 0; i < results.length; i++) {
        if (Object.keys(results[i]).length) fetchedExpiryDates.add(observationExpiries[i]);
        Object.assign(tickerMap, results[i]);
      }
    }

    // ─── OI Collection: fetch ALL expiry tickers for put/call ratio ────────
    if (db && instruments.length > 0) {
      try {
        // Extract ALL unique expiry dates from the full instruments array
        const allExpiryDates = [...new Set(instruments.map(i => i.instrument_name.split('-')[1]))];
        // Find expiries not already fetched in tickerMap
        const missingExpiries = allExpiryDates.filter(exp => !fetchedExpiryDates.has(exp));
        console.log(`📊 OI: ${allExpiryDates.length} total expiries, ${missingExpiries.length} need fetching`);

        // Fetch tickers for missing expiries
        let extraTickerResults = [];
        if (missingExpiries.length > 0) {
          extraTickerResults = await Promise.all(
            missingExpiries.map(expiry => fetchTickersByExpiry(expiry))
          );
        }

        // Build full ticker map for OI (merge existing + new)
        const oiTickerMap = { ...tickerMap };
        for (const tickers of extraTickerResults) {
          for (const [name, data] of Object.entries(tickers)) {
            oiTickerMap[name] = data;
            tickerMap[name] = data;
          }
        }

        // Aggregate OI by put/call and near/far (<30 DTE vs 30+ DTE)
        const now = Date.now();
        const NEAR_THRESHOLD_MS = 30 * 24 * 60 * 60 * 1000;
        let putOI = 0, callOI = 0;
        let nearPutOI = 0, nearCallOI = 0;
        let farPutOI = 0, farCallOI = 0;
        let counted = 0;

        // Collect OI + IV skew from full ticker map in a single pass
        const observedPutIvs = [], observedCallIvs = [];

        for (const [name, ticker] of Object.entries(oiTickerMap)) {
          const oi = Number(ticker.stats?.oi) || 0;
          const isPut = name.endsWith('-P');
          const isCall = name.endsWith('-C');
          if (!isPut && !isCall) continue;

          // OI aggregation
          if (oi > 0) {
            // Parse expiry from instrument name: ETH-20260424-1400-P
            const parts = name.split('-');
            const expiryStr = parts[1]; // "20260424"
            const expiryDate = new Date(
              `${expiryStr.slice(0, 4)}-${expiryStr.slice(4, 6)}-${expiryStr.slice(6, 8)}T08:00:00Z`
            );
            const isNear = (expiryDate.getTime() - now) < NEAR_THRESHOLD_MS;

            if (isPut) {
              putOI += oi;
              if (isNear) nearPutOI += oi; else farPutOI += oi;
            } else {
              callOI += oi;
              if (isNear) nearCallOI += oi; else farCallOI += oi;
            }
            counted++;
          }

          // IV skew: collect implied vol for instruments in bot's delta range
          const delta = Number(ticker.option_pricing?.d);
          const iv = Number(ticker.option_pricing?.i);
          if (!delta || !iv || iv <= 0) continue;
          const absDelta = Math.abs(delta);

          if (isPut && absDelta >= 0.02 && absDelta <= 0.12) {
            observedPutIvs.push({ expiry: name.split('-')[1], delta, impliedVol: iv });
          } else if (isCall && absDelta >= 0.04 && absDelta <= 0.12) {
            observedCallIvs.push({ expiry: name.split('-')[1], delta, impliedVol: iv });
          }
        }

        const totalOI = putOI + callOI;
        const pcRatio = callOI > 0 ? putOI / callOI : null;
        const matchedIv = require('./bot/option-market-quality').computeMatchedPutCallSkew(observedPutIvs, observedCallIvs);
        const avgPutIv = matchedIv.putIv;
        const avgCallIv = matchedIv.callIv;

        db.insertOISnapshot({
          timestamp: tickTimestamp,
          put_oi: putOI,
          call_oi: callOI,
          near_put_oi: nearPutOI,
          near_call_oi: nearCallOI,
          far_put_oi: farPutOI,
          far_call_oi: farCallOI,
          total_oi: totalOI,
          pc_ratio: pcRatio,
          expiry_count: allExpiryDates.length,
          avg_put_iv: avgPutIv,
          avg_call_iv: avgCallIv,
        });

        const nearOI = nearPutOI + nearCallOI;
        const farOI = farPutOI + farCallOI;
        const skewPct = avgPutIv != null && avgCallIv != null ? ((avgPutIv - avgCallIv) * 100).toFixed(1) : 'N/A';
        console.log(`📊 OI: P/C ${pcRatio?.toFixed(3) || 'N/A'} | total ${totalOI.toFixed(0)} (${counted} instruments) | near ${nearOI.toFixed(0)} / far ${farOI.toFixed(0)} | skew ${skewPct}%`);
      } catch (e) {
        console.log(`⚠️ OI collection failed: ${e.message}`);
      }
    }

  // Prefer option-ticker index price over early spot (more timely, matches options pricing)
  if (Object.keys(tickerMap).length > 0) {
    const firstTicker = Object.values(tickerMap)[0];
    const lyraIndex = normalizeEthSpotPrice(firstTicker.I);
    if (lyraIndex != null) {
      if (spotPrice) {
        console.log(`🔄 Upgrading spot from early quote $${spotPrice.toFixed(2)} → Lyra index $${lyraIndex.toFixed(2)} (Δ${(lyraIndex - spotPrice).toFixed(2)})`);
      } else {
        console.log(`🔄 Using Lyra index price as spot: $${lyraIndex.toFixed(2)}`);
      }
      spotPrice = lyraIndex;
    }
  }

  if (spotPrice) {
    // Display run header
    console.log(`ETH: $${spotPrice.toFixed(2)} | ${new Date().toLocaleString()}`);

    // Load 7-day price history from SQLite, append current tick (not yet inserted)
    let priceHistory = [];
    if (db) {
      try { priceHistory = db.loadPriceHistoryFromDb(); }
      catch (e) { console.log('DB: price history read failed:', e.message); }
    }
    priceHistory.push({ price: spotPrice, timestamp: now });

    // Analyze momentum early to display in header
    const momentumResult = analyzeMomentum(priceHistory);
    botData.mediumTermMomentum = momentumResult.mediumTermMomentum;
    botData.shortTermMomentum = momentumResult.shortTermMomentum;

    // SQLite: persist spot price
    if (db) {
      try { db.insertSpotPrice(spotPrice, momentumResult, botData, tickTimestamp); }
      catch (e) { console.log('DB: spot price write failed:', e.message); }
    }

    // ===== ONCHAIN ANALYSIS =====
    console.log('🔗 Running onchain analysis...');

    onchainAnalysis = {
      error: 'analysis_failed',
      timestamp: new Date().toISOString()
    };

    try {
      // Run onchain analysis functions with individual error handling
      const dexLiquidityResult = await analyzeDEXLiquidity(spotPrice).catch(err => ({ error: err.message, timestamp: new Date().toISOString() }));

      // Compile onchain analysis results
      onchainAnalysis = {
        dexLiquidity: dexLiquidityResult,
        spotPrice: spotPrice,
        momentumData: momentumResult,
        timestamp: new Date().toISOString()
      };

      // SQLite: persist onchain data
      if (db) {
        try {
          db.insertOnchainData(onchainAnalysis);
        }
        catch (e) { console.log('DB: onchain write failed:', e.message); }
      }

      // Fetch and persist funding rates
      if (db) {
        try {
          const fundingRates = await fetchFundingRates();
          if (fundingRates.length > 0) {
            db.insertFundingRates(fundingRates);
            const latest = fundingRates[fundingRates.length - 1];
            console.log(`📈 Hourly funding rate: ${(latest.rate * 100).toFixed(4)}% (${latest.exchange} ${latest.symbol})`);
          }
        } catch (e) { console.log('DB: funding rate write failed:', e.message); }
      }

      // Display key findings with error handling
      console.log('⛓ Onchain Analysis Summary:');
      try {
        if (onchainAnalysis.dexLiquidity && onchainAnalysis.dexLiquidity.dexes) {
          const formatTvlUSD = (value) => {
            const numeric = Number(value);
            if (!Number.isFinite(numeric) || numeric <= 0) return 'N/A';
            if (numeric >= 1e9) return `$${(numeric / 1e9).toFixed(2)}B`;
            if (numeric >= 1e6) return `$${(numeric / 1e6).toFixed(1)}M`;
            if (numeric >= 1e3) return `$${(numeric / 1e3).toFixed(1)}K`;
            return `$${numeric.toFixed(0)}`;
          };

          // Show detailed DEX breakdown
          Object.entries(onchainAnalysis.dexLiquidity.dexes).forEach(([dexName, dexData]) => {
            if (dexData.error) {
              console.log(`${dexName}: ❌ ${dexData.error}`);
            } else {
              // Show top 3 Uniswap V3 pools with tick analysis
              if (dexName === 'uniswap_v3' && dexData.poolDetails && dexData.poolDetails.length > 0) {
                console.log(`🦄 Uniswap V3 Pools:`);
                const topUniswapV3Pools = dexData.poolDetails
                  .sort((a, b) => (b.liquidity || 0) - (a.liquidity || 0))
                  .slice(0, 3);
                topUniswapV3Pools.forEach(pool => {
                  const poolLiquidityUSD = formatTvlUSD(pool.liquidityUSD);
                  console.log(`• ${pool.token0?.symbol || 'Unknown'}/${pool.token1?.symbol || 'Unknown'}: ${poolLiquidityUSD} TVL`);
                });
              }

              // Show top 3 Uniswap V4 pools
              if (dexName === 'uniswap_v4' && dexData.poolDetails && dexData.poolDetails.length > 0) {
                const staleSuffix = dexData.stale ? ' (last valid sample)' : '';
                console.log(`🦄 Uniswap V4 Pools${staleSuffix}:`);
                const topUniswapV4Pools = dexData.poolDetails
                  .sort((a, b) => (b.liquidity || 0) - (a.liquidity || 0))
                  .slice(0, 3);
                topUniswapV4Pools.forEach(pool => {
                  const poolLiquidityUSD = formatTvlUSD(pool.liquidityUSD);
                  console.log(`• ${pool.token0?.symbol || 'Unknown'}/${pool.token1?.symbol || 'Unknown'}: ${poolLiquidityUSD} TVL`);
                });
              }
            }
          });

          // Display liquidity analysis summary
          console.log(`💦 Liquidity Analysis Summary:`);
          let totalLiquidity = 0;
          let totalPools = 0;

          Object.entries(onchainAnalysis.dexLiquidity.dexes).forEach(([dexName, dexData]) => {
            if (dexData.totalLiquidity && !dexData.error) {
              totalLiquidity += dexData.totalLiquidity || 0;
              totalPools += dexData.pools || 0;
            }
          });

          // Display liquidity flow information
          if (onchainAnalysis.dexLiquidity.flowAnalysis) {
            const flow = onchainAnalysis.dexLiquidity.flowAnalysis;
            if (flow.direction !== 'unknown') {
              const directionEmoji = flow.direction === 'inflow' ? '📈' :
                                   flow.direction === 'outflow' ? '📉' : '➡️';
              const magnitudePercent = (flow.magnitude * 100).toFixed(1);
              const confidencePercent = (flow.confidence * 100).toFixed(0);
              const currentTotal = formatTvlUSD(flow.currentTotal);
              console.log(`Liquidity Flow: ${directionEmoji} ${flow.direction.toUpperCase()} (${magnitudePercent}%, confidence: ${confidencePercent}%) - Total: ${currentTotal}`);

              // Show multi-timeframe breakdown
              if (flow.timeframes) {
                Object.entries(flow.timeframes).forEach(([timeframe, tf]) => {
                  if (tf.direction !== 'unknown') {
                    const tfEmoji = tf.direction === 'inflow' ? '📈' :
                                   tf.direction === 'outflow' ? '📉' : '➡️';
                    const changePercent = (tf.change * 100).toFixed(2);
                    const flowDescription = tf.direction === 'inflow' ? 'liquidity entering' :
                                          tf.direction === 'outflow' ? 'liquidity leaving' : 'stable';
                    const tfTotal = formatTvlUSD(tf.total);
                    console.log(`${timeframe}: ${tfEmoji} ${flowDescription} (${changePercent}%) - ${tfTotal}`);
                  }
                });
              }
            }
          }
        }

      } catch (err) {
        console.log('⚠️ Failed to display analysis summary:', err.message);
      }

    } catch (error) {
      console.log('⚠️ Onchain analysis failed completely:', error.message);
      onchainAnalysis = {
        error: error.message,
        timestamp: new Date().toISOString(),
        spotPrice: spotPrice
      };
    }

    console.log('─'.repeat(60));
  } else {
    console.log('⚠️ No spot price available (Derive + CoinGecko fallback both failed)');
  }

    console.log(`📊 ${Object.keys(tickerMap).length} tickers | ${putCandidates.length} put + ${callCandidates.length} call candidates`);

    // ── Put budget cycle management ───────────────────────────────
    if (spotPrice) {
      try {
        let insuredPortfolioValue = 0;
        let budgetSource = 'live_collateral';
        let skipReason = null;

        try {
          const balances = await fetchCollaterals();
          const ethBal = Number(balances.find(b => b.asset_name === 'ETH')?.amount || 0);
          const usdcBal = Number(balances.find(b => b.asset_name === 'USDC')?.amount || 0);
          botData.ethBalance = ethBal;
          insuredPortfolioValue = getPutBudgetPortfolioValue(ethBal, usdcBal, spotPrice);
          if (!(insuredPortfolioValue > 0)) skipReason = 'live collateral returned no positive insured value';
        } catch (e) {
          skipReason = `live collateral fetch failed: ${e.message}`;
        }

        if (!(insuredPortfolioValue > 0)) {
          const fallback = getFallbackPutBudgetPortfolioValue(spotPrice);
          if (fallback.value > 0) {
            insuredPortfolioValue = fallback.value;
            budgetSource = fallback.source;
          } else if (fallback.error) {
            skipReason = skipReason ? `${skipReason}; ${fallback.error}` : fallback.error;
          }
        }

        maybeResetPutCycle(insuredPortfolioValue, { source: budgetSource, skipReason });
      } catch (e) { console.log('📋 Cycle check failed:', e.message); }
    }

    // ── LLM-Driven Trading ─────────────────────────────────────────
    try {
      await manageOpenOrders(tickerMap, positions, instruments, spotPrice);
      await evaluateTradingRules(positions, instruments, tickerMap, spotPrice);
      await confirmAndExecutePending(instruments, tickerMap, spotPrice);
    } catch (error) {
      console.error('❌ Trading system error:', error.message);
      sendTelegram(`❌ *Trading system error*: ${error.message}`);
    }

    await syncEconomicEvidence(Date.now());

    // Persist the observation universe, independently of entry eligibility.
    if (db) {
      try {
        const allOptions = observedInstruments.map(inst => {
          const ticker = tickerMap[inst.instrument_name];
          const enriched = ticker ? enrichCandidateFromTicker(inst, ticker, spotPrice) : null;
          if (enriched) enriched.details = { ...enriched.details, quoteReceivedAt: ticker.quote_received_at, quoteSource: ticker.quote_source };
          return enriched;
        }).filter(Boolean);
        if (allOptions.length > 0) {
          // The snapshot is available only once its component quotes have arrived.
          // Receipt timestamps remain attached to quotes for execution freshness.
          const receivedTimes = observedInstruments.map(inst => Date.parse(tickerMap[inst.instrument_name]?.quote_received_at)).filter(Number.isFinite);
          const snapshotAt = receivedTimes.length ? new Date(Math.max(...receivedTimes)).toISOString() : tickTimestamp;
          db.insertOptionsSnapshotBatch(allOptions, snapshotAt);
        }
      } catch (e) { console.log('DB: options snapshot write failed:', e.message); }

      // One final account response supplies all portfolio snapshot components.
      try {
        const account = await fetchSubaccount({ forObservation: true });
        const cashflow = db.getGrossOptionsCashflow();
        db.insertPortfolioSnapshot(buildPortfolioObservation(account, {
          timestamp: account?.snapshot_received_at,
          spotPrice,
          grossOptionsCashflow: cashflow.gross_options_cashflow,
        }));
      } catch (e) { console.log('DB: portfolio snapshot deferred:', e.message); }

      try {
        if (typeof db.evaluateDueDecisionOutcomes === 'function') {
          const outcomeResult = db.evaluateDueDecisionOutcomes({ now: new Date().toISOString(), limit: 750 });
          if (outcomeResult?.scanned > 0) {
            console.log(`📊 Decision outcomes labeled: scanned=${outcomeResult.scanned} evaluated=${outcomeResult.evaluated} missing=${outcomeResult.missing}`);
          }
        }
      } catch (e) {
        console.log('DB: decision outcome labeling failed:', e.message);
      }

      try {
        if (typeof db.refreshPositionLifecycle === 'function') {
          const lifecycleResult = db.refreshPositionLifecycle();
          if (lifecycleResult?.refreshed > 0) {
            console.log(`📊 Position lifecycle refreshed: ${lifecycleResult.refreshed} instrument/family rows`);
          }
        }
      } catch (e) {
        console.log('DB: position lifecycle refresh failed:', e.message);
      }
    }

    console.log('='.repeat(60));
    console.log(' ');

    // Determine next check interval
    const checkInterval = determineCheckInterval(botData.mediumTermMomentum, botData.shortTermMomentum, botData);

    console.log(`⏰ Next bot check in ${checkInterval / (1000 * 60)} minutes`);

    // Write per-tick summary to database
    if (db) {
      let tickSummary;
      try {
        const enrichedPutCandidates = (putCandidates || [])
          .map((inst) => {
            const ticker = tickerMap[inst.instrument_name];
            return ticker ? enrichCandidateFromTicker(inst, ticker, spotPrice) : null;
          })
          .filter(Boolean);
        const enrichedCallCandidates = (callCandidates || [])
          .map((inst) => {
            const ticker = tickerMap[inst.instrument_name];
            return ticker ? enrichCandidateFromTicker(inst, ticker, spotPrice) : null;
          })
          .filter(Boolean);
        const displayPutCandidates = filterValidOptions(enrichedPutCandidates, PUT_DELTA_RANGE[0], PUT_DELTA_RANGE[1]);
        const displayCallCandidates = filterValidOptions(enrichedCallCandidates, CALL_DELTA_RANGE[0], CALL_DELTA_RANGE[1]);
        const bestCurrentPut = [...displayPutCandidates].sort((a, b) => (b?.details?.putEdgeScore || 0) - (a?.details?.putEdgeScore || 0))[0] || null;
        const bestCurrentCall = [...displayCallCandidates].sort((a, b) => (b?.details?.callEdgeScore || 0) - (a?.details?.callEdgeScore || 0))[0] || null;
        const bestPutSummary = summarizeBestCandidate(bestCurrentPut, 'put');
        const bestCallSummary = summarizeBestCandidate(bestCurrentCall, 'call');
        const historicalBestScores = typeof db.getBestScores === 'function' ? db.getBestScores(7) : null;

        tickSummary = {
          price: spotPrice,
          medium_momentum: botData.mediumTermMomentum,
          short_momentum: botData.shortTermMomentum,
          onchain: {
            liquidity_flow: onchainAnalysis?.dexLiquidity?.flowAnalysis || null,
          },
          instruments: {
            total: instruments.length,
            put_candidates: putCandidates.length,
            call_candidates: callCandidates.length,
          },
          historical: {
            total_data_points: enrichedPutCandidates.length + enrichedCallCandidates.length,
            filtered_data_points: displayPutCandidates.length + displayCallCandidates.length,
            best_put_score: historicalBestScores?.bestPutScore ?? 0,
            best_call_score: historicalBestScores?.bestCallScore ?? 0,
          },
          strategy: {
            mode: 'llm_driven',
            active_rules: db ? db.getActiveRules().length : 0,
          },
          current_best_put: bestPutSummary?.score ?? 0,
          current_best_call: bestCallSummary?.score ?? 0,
          best_put_detail: bestPutSummary?.detail ?? null,
          best_call_detail: bestCallSummary?.detail ?? null,
          historical_best_put_detail: historicalBestScores?.bestPutDetail ?? null,
          historical_best_call_detail: historicalBestScores?.bestCallDetail ?? null,
          next_check_minutes: checkInterval / (1000 * 60),
        };
        db.insertTick(tickTimestamp, JSON.stringify(tickSummary));
      } catch (e) {
        console.log('DB: tick write failed:', e.message);
      }

      if (
        process.env.ANTHROPIC_API_KEY
        && spotPrice
        && botData.nextAdvisoryRetryAt
        && Date.now() >= botData.nextAdvisoryRetryAt
      ) {
        const retryCount = botData.advisoryRetryCount || 0;
        console.log(`📋 Advisory retry due now (${retryCount} prior failure${retryCount === 1 ? '' : 's'}) — reattempting`);
        generateTradingAdvisory({ trigger: 'retry' }).catch(e => {
          console.log(`📋 Scheduled advisory retry failed (non-fatal): ${e.message}`);
        });
      }

      // Auto-generate journal entries every 8 hours
      if (tickSummary && Date.now() - botData.lastJournalGeneration >= JOURNAL_INTERVAL_MS && process.env.ANTHROPIC_API_KEY) {
        // Set timestamp immediately to prevent overlapping ticks from re-triggering
        const prevJournalTs = botData.lastJournalGeneration;
        botData.lastJournalGeneration = Date.now();
        persistCycleState();
        _wikiIngestInFlight = true;
        generateJournalEntries(tickSummary, botData).then(async (entries) => {
          console.log('📓 Journal generation succeeded, next in 8h');
          // Ingest journal entries into wiki (non-fatal)
          try {
            await ingestToWiki(entries);
          } catch (e) {
            console.log('📚 Wiki ingest failed (non-fatal):', e.message);
          } finally {
            _wikiIngestInFlight = false;
          }
          // Generate trading advisory alongside journal
          try { await generateTradingAdvisory({ trigger: 'scheduled' }); }
          catch (e) { console.log('📋 Advisory failed (non-fatal):', e.message); }
          // Review hypotheses and extract lessons on the journal cadence
          await reviewExpiredHypotheses();
          await extractHypothesisLessons();
        }).catch(e => {
          _wikiIngestInFlight = false;
          botData.lastJournalGeneration = prevJournalTs;
          persistCycleState();
          console.log('📓 Journal generation failed (will retry next tick):', e.message);
        });
      }

      // Run at most one validation pass per day. Findings remain queued for
      // manual review; the bot never rewrites Wiki pages in response to lint.
      const wikiLintSchedule = getWikiLintSchedule();
      if (process.env.ANTHROPIC_API_KEY && !_wikiIngestInFlight && !_wikiLintInFlight && wikiLintSchedule.due) {
        _wikiLintInFlight = true;
        lintWiki().then((result) => {
          if (result?.success) {
            if (result.reviewedAt && result.fullReview) {
              botData.lastWikiLintRun = new Date(result.reviewedAt).getTime();
              persistCycleState();
            }
            if (result.fullReview) {
              console.log('📚 Scheduled wiki lint finished, next full audit in 24h');
            } else {
              console.log(`📚 Wiki focused revalidation finished: reviewed=${result.reviewedPageCount || 0} outstanding=${result.outstandingIssues || 0}`);
            }
            return;
          }
          console.log(`📚 Scheduled wiki lint failed; next attempt remains on the daily cadence: ${result?.error || 'unknown error'}`);
        }).catch((e) => {
          console.log('📚 Scheduled wiki lint failed unexpectedly; next attempt remains on the daily cadence:', e.message);
        }).finally(() => {
          _wikiLintInFlight = false;
        });
      }

      // Independent trade review cadence every 8 hours
      if (process.env.ANTHROPIC_API_KEY && !_tradeReviewInFlight && (Date.now() - botData.lastTradeReviewRun >= TRADE_REVIEW_INTERVAL_MS)) {
        const prevTradeReviewRun = botData.lastTradeReviewRun;
        const prevTradeReviewError = botData.lastTradeReviewError;
        botData.lastTradeReviewRun = Date.now();
        botData.lastTradeReviewError = null;
        persistCycleState();
        reviewClosedTrades().then(async (result) => {
          if (!result?.attempted) {
            botData.lastTradeReviewRun = prevTradeReviewRun;
            botData.lastTradeReviewError = prevTradeReviewError;
            persistCycleState();
            return;
          }
          console.log(`🧾 Trade review cycle finished: ready=${result.readyCount} stored=${result.storedCount}`);
          await extractTradeLessons();
        }).catch((e) => {
          botData.lastTradeReviewRun = prevTradeReviewRun;
          botData.lastTradeReviewError = e.message;
          persistCycleState();
          console.log('🧾 Trade review cycle failed (will retry next tick):', e.message);
        });
      }

      // Canonical lesson synthesis has its own retry clock. This lets a failed
      // bootstrap recover promptly instead of waiting for another trade review.
      const tradeLessonDelay = botData.lastTradeLessonError
        ? TRADE_LESSON_RETRY_INTERVAL_MS
        : TRADE_LESSON_INTERVAL_MS;
      if (
        process.env.ANTHROPIC_API_KEY
        && !_tradeLessonInFlight
        && (Date.now() - botData.lastTradeLessonRun >= tradeLessonDelay)
      ) {
        extractTradeLessons().then((result) => {
          if (result?.attempted) {
            console.log(`🧠 Canonical lesson cycle finished: processed=${result.processed} error=${result.error || 'none'}`);
          }
        }).catch((e) => {
          console.log(`🧠 Canonical lesson scheduler failed (non-fatal): ${e.message}`);
        });
      }

    }

  botData.lastCheck = now;
  persistCycleState();
  if (process.connected && typeof process.send === 'function') {
    try { process.send({ type: 'bot_heartbeat', at: Date.now() }, () => {}); } catch { /* supervisor detects missing heartbeats */ }
  }

  // Schedule next run
  setTimeout(runBotWithWatchdog, checkInterval);
  
  } catch (error) {
    console.error('Error in bot loop:', error);
    sendTelegram(`⚠️ *Bot loop error* (retrying 60s): ${error.message}`);
    setTimeout(runBotWithWatchdog, 60000); // Retry in 1 minute on error
  }
};

// Global flag to allow graceful exit
let allowExit = false;

// Global error handlers - let PM2 handle restarts
process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Promise Rejection:', reason);
  sendTelegram(`🚨 *Bot crash* (unhandled rejection): ${reason}`);
  console.log('📦 Letting PM2 handle restart');
  process.exit(1);
});

process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught Exception:', error.message);
  sendTelegram(`🚨 *Bot crash* (uncaught exception): ${error.message}`);
  console.log('📦 Letting PM2 handle restart');
  process.exit(1);
});

// Process exit handler - let PM2 handle restarts
const originalExit = process.exit;
process.exit = function(code) {
  if (allowExit) {
    console.log(`✅ Graceful exit allowed with code: ${code}`);
    originalExit(code);
    return;
  }
  
  console.log(`📦 Process exit with code: ${code} - letting PM2 handle restart`);
  originalExit(code);
};

// Graceful shutdown handler - allow exit for updates

process.on('SIGINT', () => {
  console.log(' ');
  console.log('🛑 Shutting down bot gracefully...');
  allowExit = true;
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log(' ');
  console.log('🛑 Shutting down bot gracefully...');
  allowExit = true;
  process.exit(0);
});

// Allow graceful exit for updates
process.on('SIGUSR1', () => {
  console.log(' ');
  console.log('🔄 Update signal received - shutting down gracefully...');
  allowExit = true;
  process.exit(0);
});

// Watchdog timer to ensure bot keeps running
let lastRunTime = Date.now();
const WATCHDOG_INTERVAL = 10 * 60 * 1000; // 10 minutes

setInterval(() => {
  const timeSinceLastRun = Date.now() - lastRunTime;
  if (timeSinceLastRun > WATCHDOG_INTERVAL) {
    console.log(`📦 WATCHDOG: Bot hasn't run in ${Math.round(timeSinceLastRun / 60000)} minutes - letting PM2 handle restart`);
    process.exit(1);
  }
}, WATCHDOG_INTERVAL);

// Update last run time when bot runs
const originalRunBot = runBot;
const runBotWithWatchdog = async () => {
  lastRunTime = Date.now();
  return originalRunBot();
};

// Start the bot
console.log(' ');
console.log('='.repeat(70));
console.log(`🥱 NO OPERATION`);
console.log(' ');
console.log("Welcome. Let's begin...");
console.log(`ETH-collateralized. Put budget: ${(PUT_ANNUAL_RATE * 100).toFixed(2)}% of insured base/yr in ${BOT_CONFIG.PERIOD_DAYS}d cycles${PUT_INSURED_EXTERNAL_ETH > 0 ? ` (+${PUT_INSURED_EXTERNAL_ETH.toFixed(4)} external ETH insured)` : ''}. Calls sized by margin.`);
console.log('='.repeat(70));
console.log(' ');
loadData();

if (db?.deactivateStaleEmergencyBuybackRules) {
  try {
    const removed = db.deactivateStaleEmergencyBuybackRules();
    if (removed > 0) {
      console.log(`🧹 Deactivated ${removed} stale emergency buyback rule(s)`);
    }
  } catch (e) {
    console.log('🧹 Failed to scrub stale emergency buyback rules:', e.message);
  }
}

sendTelegram('🔄 *NOOP Bot restarted*');

// Defer first run if the bot ran recently (prevents premature runs on redeploy)
const timeSinceLastCheck = Date.now() - botData.lastCheck;
if (botData.lastCheck > 0 && timeSinceLastCheck < DYNAMIC_INTERVALS.normal) {
  const delay = DYNAMIC_INTERVALS.normal - timeSinceLastCheck;
  console.log(`⏳ Last run was ${Math.round(timeSinceLastCheck / 1000)}s ago — deferring first run by ${Math.round(delay / 1000)}s`);
  setTimeout(runBotWithWatchdog, delay);
} else {
  runBotWithWatchdog();
}
