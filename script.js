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
 *     – 45–75 days to expiry
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
 *   • Put buying has arithmetic budget discipline (3.33% of portfolio/yr in 15d cycles)
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
 *   • reduce_only=true when closing positions
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
 *   • Internet access to Lyra API + CoinGecko spot feed
 *
 * Caveats:
 * --------
 *   • Strike gating still uses index/mark from ticker; fallback is CoinGecko spot.
 *   • No global risk checks on vega/theta exposure or per-expiry concentration.
 *   • No backoff/throttling: may hit API limits under heavy load.
 *   • Execution relies on book liquidity at best bid/ask; slippage not bounded.
 */
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const { ADX, MACD } = require('technicalindicators');
const { ethers, AbiCoder } = require('ethers');

const encoder = new AbiCoder();

// SQLite database (loaded via bot/index.js or standalone)
const db = global.__noopDb || null;
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


// Common configuration
const DERIVE_ACCOUNT_ADDRESS = '0xD87890df93bf74173b51077e5c6cD12121d87903';
const ACTION_TYPEHASH = '0x4d7a9f27c403ff9c0f19bce61d76d82f9aa29f8d6d4b0c5474607d9770d1af17';
const TRADE_MODULE_ADDRESS = '0xB8D20c2B7a1Ad2EE33Bc50eF10876eD3035b5e7b';
const DOMAIN_SEPARATOR = '0xd96e5f90797da7ec8dc4e276260c7f3f87fedf68775fbe1ef116e996fc60441b';

// Common trading parameters (single source of truth: bot/config.json)
const BOT_CONFIG = JSON.parse(fs.readFileSync(path.join(__dirname, 'bot', 'config.json'), 'utf-8'));
// Arithmetic discipline: spend 3.33% of portfolio value per year on puts,
// allocated in PERIOD_DAYS windows. Budget recalculated at each cycle start.
// Formula: portfolioValue * PUT_ANNUAL_RATE / (365 / PERIOD_DAYS)
const PUT_ANNUAL_RATE = BOT_CONFIG.PUT_ANNUAL_RATE || 0.0333;
// Call exposure discipline: never exceed 40% of ETH holdings in short calls.
// This is a hard cap — enforced in monitoring before queuing sell_call actions.
const CALL_EXPOSURE_CAP_PCT = BOT_CONFIG.CALL_EXPOSURE_CAP_PCT || 0.40;
const SUBACCOUNT_ID = 25923;

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
    // Retry without Markdown if parsing fails (unescaped _ * [ etc)
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
const PUT_EXPIRATION_RANGE = [45, 75];
const PUT_DELTA_RANGE = [-0.12, -0.02]; // Negative delta for puts

// Trading parameters - CALLS  
const CALL_EXPIRATION_RANGE = [5, 12];
const CALL_DELTA_RANGE = [0.04, 0.12]; // Positive delta for calls

// Call buyback thresholds
const CALL_BUYBACK_PROFIT_THRESHOLD = 80; // Minimum profit percentage for automatic call buyback

// Journal auto-generation
const JOURNAL_INTERVAL_MS = 8 * 60 * 60 * 1000; // Every 8 hours

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

    // Advisory tracking
    lastAdvisorySpotPrice: null,  // spot price when last advisory ran
    lastAdvisoryTimestamp: 0,     // when last advisory ran
  };

  return botData;
};

const DEFAULT_AMOUNT_STEP = 0.01;

let botData = createBotData();

// Advisory mutex — prevent overlapping LLM advisory runs
let _advisoryInFlight = false;

// Price move threshold to force a re-advisory (8% move since last advisory)
const ADVISORY_PRICE_MOVE_THRESHOLD = 0.08;

const persistCycleState = () => {
  if (!db) return;
  try { db.saveBotState(botData); }
  catch (e) { console.error('Failed to persist cycle state:', e.message); }
};

// Recalculate put budget at cycle boundaries.
// Budget = portfolioValue * PUT_ANNUAL_RATE / (365 / PERIOD_DAYS)
// Called each tick — resets cycle when PERIOD elapses.
const maybeResetPutCycle = (portfolioValue) => {
  const now = Date.now();
  const cycleExpired = botData.putCycleStart && (now - botData.putCycleStart) >= PERIOD;
  const noCycle = !botData.putCycleStart;

  if (noCycle || cycleExpired) {
    // Roll over unspent budget from previous cycle
    if (cycleExpired) {
      const prevRemaining = Math.max(0, botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought);
      botData.putUnspentBuyLimit = prevRemaining;
    }

    // Calculate new cycle budget from current portfolio value
    const cyclesPerYear = 365 / BOT_CONFIG.PERIOD_DAYS;
    const newBudget = portfolioValue * PUT_ANNUAL_RATE / cyclesPerYear;

    botData.putCycleStart = now;
    botData.putBudgetForCycle = newBudget;
    botData.putNetBought = 0;
    persistCycleState();

    console.log(`📋 Put cycle ${noCycle ? 'started' : 'reset'}: $${newBudget.toFixed(2)} budget (${(PUT_ANNUAL_RATE * 100).toFixed(2)}% of $${portfolioValue.toFixed(0)} / ${cyclesPerYear.toFixed(1)} cycles/yr)${botData.putUnspentBuyLimit > 0 ? ` + $${botData.putUnspentBuyLimit.toFixed(2)} rollover` : ''}`);
  }
};

const getAmountStep = (opt) =>
  Number(opt?.options?.amount_step) || Number(opt?.amount_step) || DEFAULT_AMOUNT_STEP;

const quantizeDown = (x, step) => {
  if (!Number.isFinite(x) || !Number.isFinite(step) || step <= 0) return 0;
  return Math.max(0, Math.floor(x / step) * step);
};

// Extract the first complete JSON object from a string using balanced braces.
// The greedy regex /\{[\s\S]*\}/ matches from first { to LAST }, which captures
// garbage if the LLM outputs text between JSON blocks. This counts braces instead.
const extractJSON = (text) => {
  if (!text || typeof text !== 'string') return null;
  const start = text.indexOf('{');
  if (start === -1) return null;
  let depth = 0;
  let inString = false;
  let escape = false;
  for (let i = start; i < text.length; i++) {
    const ch = text[i];
    if (escape) { escape = false; continue; }
    if (ch === '\\' && inString) { escape = true; continue; }
    if (ch === '"') { inString = !inString; continue; }
    if (inString) continue;
    if (ch === '{') depth++;
    else if (ch === '}') {
      depth--;
      if (depth === 0) {
        try { return JSON.parse(text.slice(start, i + 1)); }
        catch { return null; }
      }
    }
  }
  return null;
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
  
  // Calculate total liquidity for each time period
  const calculateTotalLiquidity = (data) => {
    let total = 0;
    let hasValidData = false;
    let hasFailedDexes = false;
    if (data.dexes) {
      Object.values(data.dexes).forEach(dex => {
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
      
      const response = await axios.post(DEX_APIS.UNISWAP_V3, uniswapQuery, { timeout: 10000 });
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
      console.log('⚠️ Uniswap V4 liquidity analysis failed:', error.message);
      if (error.response) {
        console.log('⚠️ V4 API Response:', error.response.status, error.response.data);
      }
      liquidityData.dexes.uniswap_v4 = { error: error.message };
    }

    // Calculate liquidity flow direction
    const historicalData = loadHistoricalLiquidity();
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

// Get current spot price from CoinGecko (retries on 429)
const getSpotPrice = async () => {
  const maxRetries = 3;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const response = await axios.get(`${COINGECKO_API}/simple/price?ids=ethereum&vs_currencies=usd`);
      return response.data.ethereum.usd;
    } catch (error) {
      const status = error.response?.status;
      const retryAfter = error.response?.headers?.['retry-after'];
      const rateLimitRemaining = error.response?.headers?.['x-ratelimit-remaining'];
      const rateLimitReset = error.response?.headers?.['x-ratelimit-reset'];
      const is429 = status === 429;
      if (is429 && attempt < maxRetries) {
        const delay = retryAfter ? Number(retryAfter) * 1000 : (attempt + 1) * 5000;
        console.log(`⏳ CoinGecko 429 rate-limited | retry-after: ${retryAfter || 'none'} | remaining: ${rateLimitRemaining ?? 'N/A'} | reset: ${rateLimitReset ?? 'N/A'} | waiting ${delay / 1000}s (attempt ${attempt + 1}/${maxRetries})`);
        await new Promise(r => setTimeout(r, delay));
        continue;
      }
      console.error(`Error fetching spot price: ${error.message} | status: ${status || 'N/A'} | retry-after: ${retryAfter || 'none'} | ratelimit-remaining: ${rateLimitRemaining ?? 'N/A'}`);
      return null;
    }
  }
};

// Fetch ETH funding rate from Derive's own perp ticker (no geo-block, already used by bot)
const fetchFundingRates = async () => {
  try {
    const response = await axios.post(API_URL.GET_TICKERS, {
      instrument_type: 'perp', currency: 'ETH',
    }, { timeout: 5000 });
    const raw = response.data?.result;
    const tickers = Array.isArray(raw) ? raw : [];
    const ethPerp = tickers.find(t => t.instrument_name === 'ETH-PERP');
    if (ethPerp?.funding_rate_info?.funding_rate != null) {
      return [{
        timestamp: new Date().toISOString(),
        exchange: 'derive',
        symbol: 'ETH-PERP',
        rate: Number(ethPerp.funding_rate_info.funding_rate),
      }];
    }
  } catch (e) {
    console.log(`⚠️ Derive funding rate failed: ${e.message}`);
  }
  return [];
};

// Fetch option details
// Fetch all tickers for a given expiry date (batch call — returns AMM prices)
const fetchTickersByExpiry = async (expiryDate) => {
  try {
    const response = await axios.post(API_URL.GET_TICKERS, {
      instrument_type: 'option',
      currency: 'ETH',
      expiry_date: expiryDate,
    });
    if (!response.data.result?.tickers) {
      console.error(`No tickers found for expiry ${expiryDate}`);
      return {};
    }
    return response.data.result.tickers;
  } catch (error) {
    const status = error.response?.status;
    console.error(`Error fetching tickers for expiry ${expiryDate}: ${error.message} | status: ${status || 'N/A'}${status === 429 ? ' (RATE LIMITED)' : ''}`);
    return {};
  }
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
  const openInterest = Number(ticker.stats?.oi) || null;
  const impliedVol = Number(ticker.option_pricing?.i) || null;

  const askDeltaValue = askPrice == 0 ? 0 : Math.abs(delta) / askPrice;
  const bidDeltaValue = bidPrice == 0 ? 0 : bidPrice / Math.abs(delta);

  return {
    ...instrument,
    details: {
      delta,
      askDeltaValue,
      bidDeltaValue,
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
const placeOrder = async (name, amount, direction = 'buy', price, assetAddress, optionSubId, reduceOnly = true, timeInForce = 'ioc') => {
  try {
    const wallet = createWallet();
    const timestamp = Date.now(); // Current UTC timestamp in ms
    const signature = await signMessage(wallet, timestamp);

    const order = {
        instrument_name: name,
        subaccount_id: SUBACCOUNT_ID,
        direction,
        limit_price: price.toString(),
        amount: amount.toString(),
        signature_expiry_sec: Math.floor((Date.now() / 1000) + (timeInForce === 'ioc' ? 300 : 86400)), // IOC: 5min, GTC/post_only: 24h
        max_fee: Math.max(0.08 * price, 10.0).toFixed(2).toString(), // Max fee per unit of volume (USDC). Generous ceiling — actual fee is much lower (~0.1% of notional)
        mmp: direction === 'sell', // Market maker protection during selling
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
      const errMsg = typeof response.data.error === 'string' ? response.data.error : JSON.stringify(response.data.error);
      // Detect post_only rejection (would cross the book)
      if (timeInForce === 'post_only' && (errMsg.includes('post_only') || errMsg.includes('cross') || errMsg.includes('reject'))) {
        console.log(`📋 post_only rejected for ${name}: would cross the book`);
        return { rejected_post_only: true, error: errMsg };
      }
      console.error(`Error placing limit order for ${name}:`, errMsg);
      return null;
    }
    // Check for cancelled IOC (cancel_reason in response indicates immediate cancel)
    const orderResult = response.data?.result || response.data;
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
      if (errStr.includes('post_only') || errStr.includes('cross') || errStr.includes('reject')) {
        console.log(`📋 post_only rejected for ${name}: would cross the book`);
        return { rejected_post_only: true, error: errStr };
      }
    }
    const bodyStr = errBody ? (typeof errBody === 'string' ? errBody.slice(0, 300) : JSON.stringify(errBody).slice(0, 300)) : 'no body';
    console.error(`Error placing limit order for ${name}: ${error.message} | status: ${status || 'N/A'} | body: ${bodyStr}`);
    return null;
  }
};

// Fetch all open (resting) orders from Derive
const fetchOpenOrders = async () => {
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
    const orders = Array.isArray(raw) ? raw : (raw?.orders || []);
    return orders.map(o => ({
      order_id: o.order_id,
      instrument_name: o.instrument_name,
      direction: o.direction,
      amount: o.amount,
      filled_amount: o.filled_amount,
      limit_price: o.limit_price,
      average_price: o.average_price,
      order_status: o.order_status,
      time_in_force: o.time_in_force,
      creation_timestamp: o.creation_timestamp,
      last_update_timestamp: o.last_update_timestamp,
    }));
  } catch (error) {
    console.error(`❌ fetchOpenOrders failed: ${error.message}`);
    return [];
  }
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
    const raw = response.data?.result;
    if (!raw) return null;
    return {
      order_id: raw.order_id,
      order_status: raw.order_status, // 'open'|'filled'|'cancelled'|'expired'
      amount: Number(raw.amount || 0),
      filled_amount: Number(raw.filled_amount || 0),
      average_price: Number(raw.average_price || 0),
      cancel_reason: raw.cancel_reason || null,
    };
  } catch (error) {
    const errBody = error.response?.data;
    const bodyStr = errBody ? (typeof errBody === 'string' ? errBody.slice(0, 300) : JSON.stringify(errBody).slice(0, 300)) : 'no body';
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
      console.error(`❌ cancelOrder ${orderId}: ${response.data.error}`);
      return null;
    }
    console.log(`🗑️ Cancelled order ${orderId} (${instrumentName})`);
    return response.data?.result;
  } catch (error) {
    console.error(`❌ cancelOrder ${orderId} failed: ${error.message}`);
    return null;
  }
};

// Fetch current open positions from Derive
const fetchPositions = async () => {
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
    const positions = Array.isArray(raw) ? raw : (raw?.positions || []);
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
const fetchSubaccount = async () => {
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
    return {
      initial_margin: Number(r?.initial_margin ?? 0),        // available margin (≈ buying power)
      maintenance_margin: Number(r?.maintenance_margin ?? 0), // available before liquidation
      subaccount_value: Number(r?.subaccount_value ?? 0),
      positions_value: Number(r?.positions_value ?? 0),
      collaterals_value: Number(r?.collaterals_value ?? 0),
      collaterals_initial_margin: Number(r?.collaterals_initial_margin ?? 0),
      positions_initial_margin: Number(r?.positions_initial_margin ?? 0),   // margin consumed by positions
      positions_maintenance_margin: Number(r?.positions_maintenance_margin ?? 0),
      open_orders_margin: Number(r?.open_orders_margin ?? 0),
      is_under_liquidation: r?.is_under_liquidation || false,
    };
  } catch (e) {
    console.log('📋 Failed to fetch subaccount:', e.message);
    return null;
  }
};

// Fetch all instruments once and filter for both strategies
const fetchAndFilterInstruments = async (spotPrice) => {
  try {
    console.log('🔍 Fetching all instruments...');
    const response = await axios.post(API_URL.GET_INSTRUMENTS, {
      currency: 'ETH',
      expired: false,
      instrument_type: 'option'
    });

    if (!response.data.result) {
      console.log('No instruments found');
      return { putCandidates: [], callCandidates: [] };
    }

    const instruments = response.data.result;
    console.log(`📊 Found ${instruments.length} total instruments`);

    // Filter for put candidates (negative delta, longer expiration, strike < 0.8 * spot)
    const putCandidates = instruments.filter(instrument => {
      const expiration = new Date(instrument.option_details.expiry * 1000);
      const now = new Date();
      const daysToExpiry = Math.ceil((expiration - now) / (1000 * 60 * 60 * 24));
      const strikePrice = parseFloat(instrument.option_details.strike);
      
      return daysToExpiry >= PUT_EXPIRATION_RANGE[0] && 
             daysToExpiry <= PUT_EXPIRATION_RANGE[1] &&
             instrument.instrument_type === 'option' &&
             instrument.option_details.option_type === 'P' &&
             (!spotPrice || (strikePrice < spotPrice && strikePrice > 0.60 * spotPrice));
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

Score this hypothesis using Spitznagel-aligned categories. The goal is NOT prediction accuracy — it's whether the hypothesis identified a genuine mispricing in protection cost and whether acting on it would have been asymmetric.

Categories:
- confirmed_convex: Hypothesis identified a genuine mispricing in protection cost, and acting on it would have been asymmetric (bought cheap insurance before it got expensive)
- confirmed_linear: Hypothesis was directionally right but didn't identify convexity — the opportunity was symmetric, not asymmetric
- disproven_bounded: Hypothesis was wrong but the implied action (buying cheap puts) had bounded cost — THIS IS FINE, this IS the strategy. Cheap insurance that expires worthless is the expected outcome.
- disproven_costly: Hypothesis led to buying expensive protection (chasing high IV) or missing a cheaper window — overpaid for insurance
- partially_confirmed: Direction right but timing/magnitude was off

IMPORTANT: Most hypotheses SHOULD be disproven_bounded. That means the insurance was cheap and the bleed was small. A high disproven_bounded rate is GOOD — it means the bot is buying cheap protection consistently.

Output ONLY this JSON:
{"status":"<category>","confidence":<0-1>,"verdict":"<2-3 sentence explanation focusing on whether protection was cheap/expensive and the bleed cost, not just whether the price moved correctly>"}`;

      const response = await axios.post('https://api.anthropic.com/v1/messages', {
        model: process.env.ANTHROPIC_MODEL || 'claude-opus-4-6',
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

      const resultText = response.data?.content?.[0]?.text || '';
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
  const reviewedCount = db.countReviewedSinceLastLesson();
  if (reviewedCount < 10) return;

  console.log(`🧠 Extracting lessons from ${reviewedCount} new hypothesis reviews...`);

  const reviewed = db.getReviewedHypotheses(50);
  const currentLessons = db.getActiveLessons();

  const prompt = `You are analyzing hypothesis review outcomes to extract actionable lessons for a Spitznagel-style tail-risk hedging bot.

## Reviewed Hypotheses (most recent first)
${reviewed.map(h => `#${h.id} [${h.outcome_status}] (confidence: ${h.outcome_confidence}) - ${h.content.slice(0, 150)}... VERDICT: ${h.outcome_verdict}`).join('\n\n')}

## Current Active Lessons
${currentLessons.length > 0 ? currentLessons.map(l => `- ${l.lesson} (evidence: ${l.evidence_count}, since: ${l.created_at})`).join('\n') : 'None yet'}

## Instructions
Analyze the pattern of outcomes. Key metric: convex posture rate = (confirmed_convex + disproven_bounded) / total.

Extract 3-5 actionable lessons about:
1. Which conditions reliably produce cheap protection windows (low IV, compressed skew, stable price)?
2. Which signals preceded put price spikes (meaning we should have bought before)?
3. What's the average bleed rate on disproven_bounded hypotheses (lower = better insurance buying)?
4. Which hypothesis types to avoid (high disproven_costly rate — chasing expensive protection)

For each existing lesson, say whether it still holds or should be archived.

Output JSON:
{"new_lessons":[{"lesson":"<text>","evidence_count":<number>}],"archive_ids":[<ids of lessons that no longer hold>]}`;

  try {
    const response = await axios.post('https://api.anthropic.com/v1/messages', {
      model: process.env.ANTHROPIC_MODEL || 'claude-opus-4-6',
      max_tokens: 1024,
      messages: [{ role: 'user', content: prompt }],
    }, {
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      timeout: 30000,
    });

    const text = response.data?.content?.[0]?.text || '';
    const result = extractJSON(text);
    if (result && result.new_lessons) {
      for (const lesson of (result.new_lessons || [])) {
        db.insertLesson(lesson.lesson, lesson.evidence_count || 0);
      }
      for (const id of (result.archive_ids || [])) {
        db.archiveLesson(id);
      }
      console.log(`🧠 Extracted ${result.new_lessons?.length || 0} lessons, archived ${result.archive_ids?.length || 0}`);
    }
  } catch (e) {
    console.log('🧠 Lesson extraction failed:', e.message);
  }
};

// ─── Wiki Knowledge System ──────────────────────────────────────────────────

const WIKI_DIR = process.env.WIKI_DIR || path.join(__dirname, 'knowledge');
const WIKI_META_PATH = path.join(WIKI_DIR, '.meta.json');
const WIKI_HISTORY_DIR = path.join(WIKI_DIR, '.history');

// Ensure all wiki subdirectories exist
for (const sub of ['regimes', 'protection', 'revenue', 'indicators', 'strategy']) {
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

const queryWikiContext = () => {
  const sections = [];
  for (const page of WIKI_KEY_PAGES) {
    const content = readWikiPage(page);
    if (!content || content.includes('Awaiting initial assessment')) continue;
    // Truncate to ~1500 chars to keep total context manageable
    const truncated = content.length > 1500 ? content.slice(0, 1500) + '\n...[truncated]' : content;
    sections.push(`--- ${page} ---\n${truncated}`);
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

const ingestToWiki = async (journalEntries) => {
  if (!journalEntries || journalEntries.length === 0) return;
  if (!process.env.ANTHROPIC_API_KEY) return;

  console.log('📚 Wiki ingest: processing', journalEntries.length, 'journal entries...');

  // Read schema + all pages
  const schema = readWikiPage('schema.md');
  const pages = {};
  for (const page of WIKI_ALL_PAGES) {
    pages[page] = readWikiPage(page);
  }

  const pagesContext = Object.entries(pages)
    .map(([p, content]) => `--- ${p} ---\n${content}`)
    .join('\n\n');

  const entriesText = journalEntries
    .map(e => `[${e.type || e.entry_type || 'unknown'}] ${e.content}`)
    .join('\n\n---\n\n');

  const prompt = `You are maintaining a knowledge wiki for a Spitznagel-style tail-risk hedging bot. Your job is to update wiki pages based on new journal entries.

## Wiki Schema
${schema}

## Current Wiki Pages
${pagesContext}

## New Journal Entries
${entriesText}

## Instructions
1. Analyze which wiki pages need updating based on the new journal entries
2. Preserve existing accurate content — ADD to it, don't replace it
3. Add date stamps [${new Date().toISOString().split('T')[0]}] to new observations
4. If current data contradicts existing wiki content, use "Previously: X. Updated [date]: Y" format
5. Keep each page under 2000 words — consolidate older entries if approaching limit
6. Every page must start with a bold TLDR line reflecting current state

Output your updates as XML blocks. Only include pages that need changes:

<wiki_update path="regimes/current.md">
[full updated page content]
</wiki_update>

If no pages need updating, output: <no_updates/>`;

  try {
    const response = await axios.post('https://api.anthropic.com/v1/messages', {
      model: 'claude-opus-4-6',
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

    const text = response.data?.content?.[0]?.text || '';

    if (text.includes('<no_updates/>')) {
      console.log('📚 Wiki ingest: no updates needed');
      return;
    }

    // Parse wiki_update blocks
    const updateRegex = /<wiki_update\s+path="([^"]+)">([\s\S]*?)<\/wiki_update>/g;
    let match;
    let updateCount = 0;

    while ((match = updateRegex.exec(text)) !== null) {
      const pagePath = match[1];
      const newContent = match[2].trim();

      // Validate page path
      if (!WIKI_ALL_PAGES.includes(pagePath)) {
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
      const expectedHeaders = {
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
      const required = expectedHeaders[pagePath] || [];
      const missingHeaders = required.filter(h => !newContent.includes(h));
      if (missingHeaders.length > 0) {
        console.log(`📚 Wiki ingest: rejected update for ${pagePath} — missing sections: ${missingHeaders.join(', ')}`);
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
      console.log(`📚 Wiki ingest: updated ${pagePath}`);
    }

    // Update meta
    const meta = readWikiMeta();
    meta.last_ingest = new Date().toISOString();
    meta.last_ingest_updates = updateCount;
    writeWikiMeta(meta);

    console.log(`📚 Wiki ingest: ${updateCount} page(s) updated`);
  } catch (e) {
    console.log('📚 Wiki ingest failed:', e.message);
    throw e;
  }
};

const getWikiSignalContext = () => {
  try {
    const regimePage = readWikiPage('regimes/current.md');
    const playbookPage = readWikiPage('strategy/playbook.md');

    if (!regimePage || regimePage.includes('Awaiting initial assessment')) {
      return null;
    }

    // Parse regime classification
    const classMatch = regimePage.match(/##\s*Classification\s*\n+\s*(\w+)/i);
    const regime = classMatch ? classMatch[1].toLowerCase() : null;

    // Parse confidence
    const confMatch = regimePage.match(/##\s*Confidence\s*\n+\s*(\w+)/i);
    const regimeConfidence = confMatch ? confMatch[1].toLowerCase() : null;

    // Parse protection cost assessment from pricing page
    const pricingPage = readWikiPage('protection/pricing.md');
    let protectionAssessment = null;
    if (pricingPage) {
      const costMatch = pricingPage.match(/##\s*Cost Assessment\s*\n+\s*(\w+)/i);
      protectionAssessment = costMatch ? costMatch[1].toLowerCase() : null;
    }

    // Parse revenue/call premium assessment
    const revenuePage = readWikiPage('revenue/pricing.md');
    let revenueAssessment = null;
    if (revenuePage && !revenuePage.includes('Awaiting initial assessment')) {
      const premMatch = revenuePage.match(/##\s*Premium Assessment\s*\n+\s*(\w+)/i);
      revenueAssessment = premMatch ? premMatch[1].toLowerCase() : null;
    }

    // Parse playbook rules (first 5 bullet points from Core Rules)
    const playbookRules = [];
    if (playbookPage) {
      const rulesMatch = playbookPage.match(/##\s*Core Rules\s*\n([\s\S]*?)(?=\n##|$)/i);
      if (rulesMatch) {
        const bullets = rulesMatch[1].match(/^[-*]\s+.+/gm);
        if (bullets) {
          playbookRules.push(...bullets.slice(0, 5).map(b => b.replace(/^[-*]\s+/, '')));
        }
      }
    }

    return { regime, regimeConfidence, protectionAssessment, revenueAssessment, playbookRules };
  } catch {
    return null;
  }
};

const lintWiki = async () => {
  if (!process.env.ANTHROPIC_API_KEY) return;

  // Guard: only run once per 20 hours
  const meta = readWikiMeta();
  const LINT_INTERVAL_MS = 20 * 60 * 60 * 1000; // 20 hours
  if (meta.last_lint && Date.now() - new Date(meta.last_lint).getTime() < LINT_INTERVAL_MS) {
    return;
  }

  console.log('📚 Wiki lint: auditing wiki pages...');

  // Read all pages
  const pages = {};
  let hasContent = false;
  for (const page of WIKI_ALL_PAGES) {
    pages[page] = readWikiPage(page);
    if (pages[page] && !pages[page].includes('Awaiting initial assessment')) hasContent = true;
  }
  if (!hasContent) {
    console.log('📚 Wiki lint: skipped — wiki not yet seeded');
    return;
  }

  const schema = readWikiPage('schema.md');
  const pagesContext = Object.entries(pages)
    .map(([p, content]) => `--- ${p} ---\n${content}`)
    .join('\n\n');

  const prompt = `You are auditing a knowledge wiki for a Spitznagel-style tail-risk hedging bot. Check for quality issues.

## Wiki Schema
${schema}

## Current Wiki Pages
${pagesContext}

## Audit Checklist
1. **Contradictions**: Do any pages contradict each other?
2. **Staleness**: Are any observations older than 7 days without recent updates?
3. **Redundancy**: Is the same information repeated across pages?
4. **Missing links**: Do pages reference concepts that should be in another page but aren't?
5. **Quality**: Are TLDRs accurate? Are evidence values specific?

## Instructions
Return a JSON object with:
{
  "issues": [{"page": "path", "type": "contradiction|stale|redundant|missing_link|quality", "description": "..."}],
  "updates": [{"page": "path", "content": "full updated page content"}]
}

Only include updates for pages that genuinely need fixing. If no issues found, return {"issues":[],"updates":[]}.
Wrap your JSON in a <lint_result> tag.`;

  try {
    const response = await axios.post('https://api.anthropic.com/v1/messages', {
      model: 'claude-sonnet-4-6',
      max_tokens: 4096,
      messages: [{ role: 'user', content: prompt }],
    }, {
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      timeout: 60000,
    });

    const text = response.data?.content?.[0]?.text || '';
    const lintMatch = text.match(/<lint_result>([\s\S]*?)<\/lint_result>/);
    if (!lintMatch) {
      console.log('📚 Wiki lint: no structured result returned');
      meta.last_lint = new Date().toISOString();
      writeWikiMeta(meta);
      return;
    }

    let result;
    try {
      result = JSON.parse(lintMatch[1].trim());
    } catch (parseErr) {
      console.log('📚 Wiki lint: malformed JSON in lint_result:', parseErr.message);
      meta.last_lint = new Date().toISOString();
      writeWikiMeta(meta);
      return;
    }

    if (result.issues?.length > 0) {
      console.log(`📚 Wiki lint: found ${result.issues.length} issue(s):`);
      for (const issue of result.issues) {
        console.log(`  - [${issue.type}] ${issue.page}: ${issue.description}`);
      }
    } else {
      console.log('📚 Wiki lint: no issues found');
    }

    // Apply updates with same safety guards as ingest
    let updateCount = 0;
    for (const update of (result.updates || [])) {
      const pagePath = update.page;
      const newContent = update.content?.trim();
      if (!pagePath || !newContent) continue;
      if (!WIKI_ALL_PAGES.includes(pagePath)) continue;
      if (newContent.length < 50) continue;

      const existingContent = pages[pagePath] || '';
      if (existingContent.length > 100 && newContent.length < existingContent.length * 0.5) continue;

      if (existingContent && !existingContent.includes('Awaiting initial assessment')) {
        saveWikiHistory(pagePath, existingContent);
      }

      fs.writeFileSync(path.join(WIKI_DIR, pagePath), newContent);
      updateCount++;
      console.log(`📚 Wiki lint: updated ${pagePath}`);
    }

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

    meta.last_lint = new Date().toISOString();
    meta.last_lint_issues = result.issues?.length || 0;
    meta.last_lint_updates = updateCount;
    writeWikiMeta(meta);

    console.log(`📚 Wiki lint: complete (${updateCount} updates applied)`);
  } catch (e) {
    console.log('📚 Wiki lint failed:', e.message);
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
        market_quality: (() => {
          try {
            if (!marketQuality.length) return null;
            const byType = {};
            for (const r of marketQuality) {
              const key = r.option_type === 'P' ? 'put' : 'call';
              byType[key] = {
                instruments_in_range: r.count,
                avg_spread_pct: r.avg_spread != null ? +(r.avg_spread * 100).toFixed(2) : null,
                avg_implied_vol_pct: r.avg_iv != null ? +(r.avg_iv * 100).toFixed(1) : null,
                avg_depth: r.avg_depth != null ? +r.avg_depth.toFixed(2) : null,
              };
            }
            return byType;
          } catch { return null; }
        })(),
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
    try {
      const hypStats = db.getHypothesisStats(30);
      const lessons = db.getActiveLessons();
      const recentVerdicts = db.getReviewedHypotheses(5);

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

IMPORTANT: A high disproven_bounded rate means the bot is buying cheap insurance that expires worthless — that IS the strategy working. Focus on reducing disproven_costly rate (buying expensive protection), not on increasing prediction accuracy. The best hypothesis identifies when protection is cheap, not where price goes. Each hypothesis MUST identify what makes the opportunity asymmetric — why is the downside bounded? Where is the cheap convexity?`;
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

Ground everything in the data. Focus on: cost of protection (put pricing), revenue opportunity (call premium), crash probability (flow reversals), and portfolio geometry (how put+call positions work together).${hypothesisPerformance}`;

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

    const response = await axios.post('https://api.anthropic.com/v1/messages', {
      model: process.env.ANTHROPIC_MODEL || 'claude-opus-4-6',
      max_tokens: 4096,
      system: systemPrompt,
      messages: [{ role: 'user', content: userMessage }],
    }, {
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      timeout: 120000,
    });

    const text = response.data?.content?.[0]?.text || '';

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
    console.log('📓 Journal generation failed:', e.message);
    throw e;
  }
};

// ─── OpenAI API Helper ───────────────────────────────────────────────────────
const callOpenAI = async (systemPrompt, userPrompt, { maxTokens = 2048, timeout = 30000, model = 'gpt-4o' } = {}) => {
  if (!process.env.OPENAI_API_KEY) return null;
  try {
    const response = await axios.post('https://api.openai.com/v1/chat/completions', {
      model,
      max_tokens: maxTokens,
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
    return response.data?.choices?.[0]?.message?.content || '';
  } catch (e) {
    console.log(`⚠️ OpenAI API call failed: ${e.message}`);
    return null;
  }
};

// ─── LLM-Driven Trading: Monitoring ──────────────────────────────────────────

const parseExpiryFromInstrument = (name) => {
  // "ETH-20260501-1500-P" → Date(2026-05-01T08:00:00Z)
  const parts = name.split('-');
  if (parts.length < 4) return null;
  const d = parts[1]; // "20260501"
  return new Date(`${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6,8)}T08:00:00Z`);
};

const computeCurrentValues = (position, ticker, spotPrice) => {
  const expiry = parseExpiryFromInstrument(position.instrument_name);
  const dte = expiry ? Math.max(0, (expiry.getTime() - Date.now()) / (86400000)) : null;
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

const evaluateConditions = (conditions, logic, values) => {
  if (!Array.isArray(conditions) || conditions.length === 0) return false;
  const results = conditions.map(c => {
    const v = values[c.field];
    if (v == null) return false;
    if (c.op === 'gt') return v > c.value;
    if (c.op === 'lt') return v < c.value;
    if (c.op === 'gte') return v >= c.value;
    if (c.op === 'lte') return v <= c.value;
    return false;
  });
  return logic === 'all' ? results.every(Boolean) : results.some(Boolean);
};

const getOpenRestingEntryOrders = () => {
  if (!db) return [];
  return db.getOpenRestingOrders().filter(order => order.action === 'buy_put' || order.action === 'sell_call');
};

const summarizeReservedEntryCapacity = (restingOrders) => {
  return restingOrders.reduce((acc, order) => {
    if (order.action === 'buy_put') {
      acc.putBudget += (Number(order.amount) || 0) * (Number(order.limit_price) || 0);
    }
    return acc;
  }, { putBudget: 0 });
};

const restingOrderNeedsAdjustment = (restingOrder, desiredPrice, desiredQty, instrument) => {
  const currentPrice = Number(restingOrder?.limit_price) || 0;
  const currentQty = Number(restingOrder?.amount) || 0;
  const amountStep = instrument?.options?.amount_step || 0.01;
  const priceStep = getInstrumentPriceStep(instrument, desiredPrice);
  const priceDelta = Math.abs(currentPrice - desiredPrice);
  const qtyDelta = Math.abs(currentQty - desiredQty);
  return priceDelta >= Math.max(priceStep / 2, 0.0001) || qtyDelta >= Math.max(amountStep / 2, 0.0001);
};

const getMarginCapacityBase = (marginState) => {
  const collateralMarginBase = Number(marginState?.collaterals_initial_margin ?? 0);
  if (collateralMarginBase > 0) return collateralMarginBase;
  const collateralValue = Number(marginState?.collaterals_value ?? 0);
  if (collateralValue > 0) return collateralValue;
  return Number(marginState?.subaccount_value ?? 0);
};

const estimateMarginUtilization = (marginState, additionalOpenOrdersMargin = 0) => {
  const base = getMarginCapacityBase(marginState);
  if (!(base > 0)) return null;
  const usedMargin = Math.max(0, Number(marginState?.positions_initial_margin ?? 0))
    + Math.max(0, Number(marginState?.open_orders_margin ?? 0))
    + Math.max(0, Number(additionalOpenOrdersMargin ?? 0));
  return usedMargin / base;
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
  const documentedEstimate = estimateStandardShortCallInitialMarginPerUnit(strike, spotPrice, premium);
  if (Number.isFinite(documentedEstimate) && documentedEstimate > 0) {
    return documentedEstimate;
  }

  const shortCallPositions = positions.filter(p => p.instrument_name?.endsWith('-C') && p.direction === 'short');
  const currentShortExposure = shortCallPositions.reduce((sum, p) => sum + Math.abs(Number(p.amount) || 0), 0);
  if (currentShortExposure > 0 && Number(marginState?.positions_initial_margin ?? 0) > 0) {
    return Number(marginState.positions_initial_margin) / currentShortExposure;
  }

  const restingShortExposure = restingOrders
    .filter(order => order.action === 'sell_call')
    .reduce((sum, order) => sum + Math.abs(Number(order.amount) || 0), 0);
  if (restingShortExposure > 0 && Number(marginState?.open_orders_margin ?? 0) > 0) {
    return Number(marginState.open_orders_margin) / restingShortExposure;
  }

  return Math.max((spotPrice || 0) * 0.13, 100);
};

const evaluateTradingRules = async (positions, instruments, tickerMap, spotPrice) => {
  let triggeredCount = 0;

  // ── Exit rules ─────────────────────────────────────────────────────────────
  try {
    const exitRules = db.getActiveRulesByType('exit');
    for (const rule of exitRules) {
      try {
        const position = positions.find(p => p.instrument_name === rule.instrument_name);
        if (!position) continue;

        const ticker = tickerMap[rule.instrument_name];
        const values = computeCurrentValues(position, ticker, spotPrice);

        let criteria;
        try { criteria = typeof rule.criteria === 'string' ? JSON.parse(rule.criteria) : rule.criteria; } catch { criteria = null; }
        if (!criteria || typeof criteria !== 'object' || !Array.isArray(criteria.conditions)) {
          console.log(`📋 Exit rule ${rule.id}: skipping — criteria missing structured conditions`);
          continue;
        }
        const triggered = evaluateConditions(criteria.conditions, criteria.condition_logic, values);
        if (!triggered) continue;

        // Dedup: skip if there's already a pending/confirmed action for this rule
        if (db.hasPendingActionForRule(rule.id)) continue;

        const askPrice = Number(ticker?.a) || 0;
        const bidPrice = Number(ticker?.b) || 0;
        const markPrice = Number(ticker?.M) || values.mark_price || 0;
        const price = rule.action === 'buy_put' || rule.action === 'buy_call' ? askPrice : bidPrice;

        // Skip selling worthless positions — mark < $0.10 means nothing to recover
        if (rule.action === 'sell_put' && markPrice < 0.10) {
          console.log(`📋 Exit skip: ${rule.instrument_name} mark $${markPrice.toFixed(4)} — worthless, let expire`);
          continue;
        }

        db.insertPendingAction({
          rule_id: rule.id,
          action: rule.action,
          instrument_name: rule.instrument_name,
          amount: position.amount,
          price: price,
          trigger_details: {
            conditions_met: criteria.conditions.map(c => ({ field: c.field, op: c.op, threshold: c.value, actual: values[c.field] })),
            current_values: values,
            preferred_order_type: rule.preferred_order_type || null,
          },
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
    let liveMarginState = null;
    try { liveMarginState = await fetchSubaccount(); } catch { /* ok */ }
    let provisionalCallOrderMargin = 0;
    for (const rule of entryRules) {
      try {
        let criteria;
        try { criteria = typeof rule.criteria === 'string' ? JSON.parse(rule.criteria) : rule.criteria; } catch { criteria = null; }
        if (!criteria || typeof criteria !== 'object' || !criteria.option_type) {
          console.log(`📋 Entry rule ${rule.id}: skipping — criteria missing structured fields (need option_type, delta_range, dte_range)`);
          continue;
        }

        // Cooldown check: skip if same action was executed within the last hour
        const lastExec = db.getLastExecutedAction(rule.action);
        if (lastExec) {
          const elapsed = Date.now() - new Date(lastExec).getTime();
          if (elapsed < 3600000) continue; // 1 hour cooldown
        }

        const reservedCapacity = summarizeReservedEntryCapacity(openRestingEntryOrders);

        // Put budget discipline: skip if cycle budget exhausted
        if (rule.action === 'buy_put' && botData.putBudgetForCycle > 0) {
          const putRemaining = botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought - reservedCapacity.putBudget;
          if (putRemaining <= 0.20) continue;
        }

        // Call exposure cap: skip if short call exposure >= 40% of ETH holdings
        if (rule.action === 'sell_call') {
          if (!liveMarginState) {
            console.log(`📋 Skip ${rule.action}: margin state unavailable`);
            continue;
          }
          const currentUtilization = estimateMarginUtilization(liveMarginState, provisionalCallOrderMargin);
          if (currentUtilization == null) {
            console.log(`📋 Skip ${rule.action}: unable to compute margin utilization`);
            continue;
          }
          if (currentUtilization >= CALL_EXPOSURE_CAP_PCT) {
            console.log(`📋 Call margin cap reached: ${(currentUtilization * 100).toFixed(1)}% >= ${(CALL_EXPOSURE_CAP_PCT * 100).toFixed(1)}%`);
            continue;
          }
        }

        // Dedup: skip if already pending or confirmed
        if (db.hasPendingActionForRule(rule.id)) continue;

        // Dedup: skip if we already have a resting order for this action type
        // (prevents GTC duplicate stacking — entry rules match multiple instruments,
        //  but we don't want to queue a new one while an order is on the book)

        // Scan tickerMap for candidates matching criteria
        const optionType = criteria.option_type; // 'P' or 'C'
        const deltaRange = criteria.delta_range; // [min, max]
        const dteRange = criteria.dte_range; // [min, max]
        const maxStrikePct = criteria.max_strike_pct || null;
        const marketConditions = criteria.market_conditions || null;
        const maxCost = criteria.max_cost ?? null;
        const minBid = criteria.min_bid ?? null;
        const minScore = criteria.min_score ?? null;

        // Check market conditions if present
        if (marketConditions) {
          const marketValues = { spot_price: spotPrice };
          if (!evaluateConditions(marketConditions, 'all', marketValues)) continue;
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
            const expiry = parseExpiryFromInstrument(instrName);
            if (!expiry) { filterStats.noExpiry++; continue; }
            const dte = Math.max(0, (expiry.getTime() - Date.now()) / 86400000);

            // Filter by DTE range
            if (dteRange && (dte < dteRange[0] || dte > dteRange[1])) { filterStats.dteOut++; continue; }

            // Filter by delta range
            const delta = Number(ticker?.option_pricing?.d) || 0;
            if (deltaRange && (delta < deltaRange[0] || delta > deltaRange[1])) { filterStats.deltaOut++; continue; }

            // Filter by max_strike_pct
            const strike = Number(instrument.option_details?.strike) || 0;
            if (maxStrikePct && strike >= maxStrikePct * spotPrice) { filterStats.strikeOut++; continue; }

            const askPrice = Number(ticker?.a) || 0;
            const askAmount = Number(ticker?.A) || 0;
            const bidPrice = Number(ticker?.b) || 0;
            const bidAmount = Number(ticker?.B) || 0;

            // Filter by max_cost (for buys)
            if (maxCost != null && askPrice > maxCost) { filterStats.costOut++; continue; }

            // Filter by min_bid (for sells)
            if (minBid != null && bidPrice < minBid) { filterStats.bidOut++; continue; }

            // Score: puts = |delta| / askPrice, calls = bidPrice / |delta|
            const absDelta = Math.abs(delta);
            let score;
            if (optionType === 'P') {
              score = askPrice > 0 ? absDelta / askPrice : 0;
            } else {
              score = absDelta > 0 ? bidPrice / absDelta : 0;
            }

            if (minScore != null && score < minScore) { filterStats.scoreOut++; continue; }

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
          if (optionType || dteRange) {
            const expirySummary = new Map();
            for (const instrName of Object.keys(tickerMap)) {
              const instrument = instruments.find(i => i.instrument_name === instrName);
              if (!instrument) continue;
              if (optionType && instrument.option_details?.option_type !== optionType) continue;
              const expiry = parseExpiryFromInstrument(instrName);
              if (!expiry) continue;
              const expiryKey = instrName.split('-')[1];
              const dte = Math.max(0, (expiry.getTime() - Date.now()) / 86400000);
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

        // Pick best candidate (highest score)
        candidates.sort((a, b) => b.score - a.score);
        const best = candidates[0];

        // Calculate amount: min of rule budget_limit, put cycle budget remaining, and book liquidity
        const price = optionType === 'P' ? best.askPrice : best.bidPrice;
        if (price <= 0) continue;

        let maxByBudget = (rule.budget_limit || Infinity) / price;
        // For puts: also cap by cycle budget discipline
        if (rule.action === 'buy_put' && botData.putBudgetForCycle > 0) {
          const putRemaining = botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought - reservedCapacity.putBudget;
          maxByBudget = Math.min(maxByBudget, putRemaining / price);
        }
        // For calls: cap by remaining exposure headroom (40% of ETH - current short calls)
        if (rule.action === 'sell_call' && liveMarginState) {
          const marginBase = getMarginCapacityBase(liveMarginState);
          const marginUsed = Math.max(0, Number(liveMarginState.positions_initial_margin || 0))
            + Math.max(0, Number(liveMarginState.open_orders_margin || 0))
            + provisionalCallOrderMargin;
          const marginHeadroom = Math.max(0, CALL_EXPOSURE_CAP_PCT * marginBase - marginUsed);
          const marginPerUnit = estimateShortCallMarginPerUnit(
            liveMarginState,
            positions,
            openRestingEntryOrders,
            spotPrice,
            best.strike,
            best.bidPrice
          );
          if (marginPerUnit > 0) {
            maxByBudget = Math.min(maxByBudget, marginHeadroom / marginPerUnit);
          }
        }
        const bookLiq = optionType === 'P' ? best.askAmount : best.bidAmount;
        const step = best.amountStep || 0.01;
        const raw = Math.min(maxByBudget, bookLiq, 20);
        const qty = Math.floor(raw / step) * step;
        if (qty < step) continue;

        // If the best instrument already has a resting entry order, decide whether to keep or adjust it.
        const existingResting = openRestingEntryOrders.find(order => order.instrument_name === best.name && order.action === rule.action);
        if (existingResting) {
          if (!restingOrderNeedsAdjustment(existingResting, price, qty, best.instrument)) {
            console.log(`📋 Keep resting ${rule.action} ${best.name}: existing order already aligned @ $${Number(existingResting.limit_price).toFixed(4)} x ${Number(existingResting.amount).toFixed(2)}`);
            continue;
          }

          console.log(`📋 Adjust resting ${rule.action} ${best.name}: cancel ${existingResting.order_id} @ $${Number(existingResting.limit_price).toFixed(4)} x ${Number(existingResting.amount).toFixed(2)} -> target $${price.toFixed(4)} x ${qty.toFixed(2)}`);
          const cancelled = await cancelOrder(existingResting.order_id, existingResting.instrument_name);
          if (!cancelled) {
            console.log(`📋 Keep resting ${rule.action} ${best.name}: cancel failed, re-evaluate next tick`);
            continue;
          }
          db.updateRestingOrder(existingResting.order_id, 'cancelled', existingResting.filled_amount || 0);
          openRestingEntryOrders = openRestingEntryOrders.filter(order => order.order_id !== existingResting.order_id);
        } else if (db.hasRestingOrderForInstrument(best.name)) {
          console.log(`📋 Skip ${rule.action} ${best.name}: another resting order already on book`);
          continue;
        }

        db.insertPendingAction({
          rule_id: rule.id,
          action: rule.action,
          instrument_name: best.name,
          amount: qty,
          price: price,
          trigger_details: {
            score: best.score,
            delta: best.delta,
            dte: best.dte,
            strike: best.strike,
            candidates_evaluated: candidates.length,
            preferred_order_type: rule.preferred_order_type || null,
          },
        });
        triggeredCount++;
        openRestingEntryOrders.push({
          order_id: `pending-${rule.id}-${best.name}`,
          instrument_name: best.name,
          action: rule.action,
          direction: rule.action === 'buy_put' ? 'buy' : 'sell',
          amount: qty,
          limit_price: price,
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
        console.log(`📋 Entry candidate: ${rule.action} ${best.name} score=${best.score.toFixed(6)}`);
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

const manageOpenOrders = async (tickerMap) => {
  if (process.env.DRY_RUN === '1') return; // No real orders in dry run
  if (!db) return;

  let openOrders;
  try {
    openOrders = await fetchOpenOrders();
  } catch (e) {
    console.log(`📋 Open orders fetch failed: ${e.message}`);
    return;
  }

  // ── Fill reconciliation: detect resting orders that have been filled ──────
  const trackedResting = db.getOpenRestingOrders();
  if (trackedResting.length > 0) {
    const exchangeOrderIds = new Set(openOrders.map(o => o.order_id));
    for (const tracked of trackedResting) {
      if (!exchangeOrderIds.has(tracked.order_id)) {
        // Order disappeared from open orders → query its final status
        let finalStatus = null;
        try {
          finalStatus = await fetchOrderStatus(tracked.order_id);
        } catch (e) {
          console.log(`⚠️ Failed to fetch status for ${tracked.order_id}: ${e.message}`);
        }

        let filledAmt, fillPrice, status;
        if (finalStatus) {
          filledAmt = finalStatus.filled_amount || 0;
          fillPrice = finalStatus.average_price > 0 ? finalStatus.average_price : tracked.limit_price;
          status = finalStatus.order_status; // 'filled', 'cancelled', 'expired'
          console.log(`📋 Order ${tracked.order_id} status: ${status}, filled=${filledAmt}/${tracked.amount} @ $${fillPrice}`);
        } else {
          // API failed — skip reconciliation this tick, retry next time
          console.log(`⚠️ Order ${tracked.order_id} status unknown — skipping, will retry next tick`);
          continue;
        }

        const fillValue = filledAmt * fillPrice;

        // Track put budget discipline on fill
        if (filledAmt > 0) {
          const isPut = tracked.instrument_name?.endsWith('-P');
          if (isPut && tracked.direction === 'buy') botData.putNetBought += fillValue;
          else if (isPut && tracked.direction === 'sell') botData.putNetBought -= fillValue;
          persistCycleState();
        }

        const dbStatus = status === 'cancelled' || status === 'expired' ? 'cancelled' : 'filled';
        db.updateRestingOrder(tracked.order_id, dbStatus, filledAmt);
        db.insertOrder({
          action: tracked.action,
          success: filledAmt > 0,
          reason: `Resting order ${status} — filled ${filledAmt}/${tracked.amount}${finalStatus?.cancel_reason ? ` (${finalStatus.cancel_reason})` : ''}`,
          instrument_name: tracked.instrument_name,
          strike: null, expiry: null, delta: null,
          price: fillPrice, intended_amount: tracked.amount,
          filled_amount: filledAmt, fill_price: filledAmt > 0 ? fillPrice : null,
          total_value: fillValue, spot_price: null,
          raw_response: finalStatus ? JSON.stringify(finalStatus) : null,
        });
        console.log(`${filledAmt > 0 ? '✅' : '🗑️'} Resting order reconciled: ${tracked.action} ${tracked.instrument_name} — ${status}, filled=${filledAmt} ($${fillValue.toFixed(2)})`);
      }
    }
  }

  if (openOrders.length === 0) return;

  console.log(`📋 ${openOrders.length} open order(s) on book`);

  // ── Stale/orphan cancellation ────────────────────────────────────────────
  const activeRules = db.getActiveRules();
  const activeExitInstruments = new Set(
    activeRules.filter(r => r.rule_type === 'exit').map(r => r.instrument_name).filter(Boolean)
  );
  // Entry rules match by criteria, not instrument. Check if any entry rule's action matches the order's direction.
  const activeEntryActions = new Set(activeRules.filter(r => r.rule_type === 'entry').map(r => r.action));

  for (const order of openOrders) {
    const ageMs = Date.now() - (order.creation_timestamp || 0);
    const ageHours = ageMs / (1000 * 60 * 60);
    const filled = Number(order.filled_amount || 0);

    // Cancel stale orders (>8h) or orphaned orders
    const isStale = ageHours > 8;

    // Orphan check: is this order still backed by an active rule?
    const matchesExitRule = activeExitInstruments.has(order.instrument_name);
    const isBuyOrder = order.direction === 'buy';
    const matchesEntryAction = (isBuyOrder && order.instrument_name?.endsWith('-P') && activeEntryActions.has('buy_put'))
      || (!isBuyOrder && order.instrument_name?.endsWith('-C') && activeEntryActions.has('sell_call'))
      || (isBuyOrder && order.instrument_name?.endsWith('-C') && activeEntryActions.has('buyback_call'))
      || (!isBuyOrder && order.instrument_name?.endsWith('-P') && activeEntryActions.has('sell_put'));
    const isOrphaned = !matchesExitRule && !matchesEntryAction;

    if (isStale || isOrphaned) {
      const reason = isStale ? `stale (${ageHours.toFixed(1)}h old)` : 'orphaned (no matching active rule)';
      console.log(`🗑️ Cancelling ${order.instrument_name} order ${order.order_id}: ${reason}`);
      const result = await cancelOrder(order.order_id, order.instrument_name);

      // Update our tracking table
      db.updateRestingOrder(order.order_id, 'cancelled', filled);
    }
  }
};

// ─── LLM-Driven Trading: Confirmation & Execution ───────────────────────────

const getInstrumentPriceStep = (instrument, fallbackPrice = 0) => {
  const configuredStep = Number(
    instrument?.price_step ??
    instrument?.options?.price_step ??
    instrument?.option_details?.price_step ??
    0
  );
  if (configuredStep > 0) return configuredStep;
  if (fallbackPrice >= 10) return 0.1;
  if (fallbackPrice >= 1) return 0.05;
  return 0.01;
};

const roundToStep = (value, step, mode = 'nearest') => {
  if (!(step > 0)) return value;
  const scaled = value / step;
  if (mode === 'up') return Math.ceil(scaled) * step;
  if (mode === 'down') return Math.floor(scaled) * step;
  return Math.round(scaled) * step;
};

const describeActionSemantics = (action) => {
  if (action === 'sell_put') {
    return 'Exit-only action: selling an already-owned long put to close or trim it. This is reduce_only=true and cannot create a naked short put.';
  }
  if (action === 'buyback_call') {
    return 'Exit-only action: buying back an already-open short call to close or trim it. This is reduce_only=true and cannot create a new long call exposure beyond the short being closed.';
  }
  if (action === 'buy_put') {
    return 'Entry action: buying a put for tail-risk insurance. Bounded premium outlay, long convexity.';
  }
  if (action === 'sell_call') {
    return 'Entry action: selling a call to open short call exposure against ETH-collateralized account capacity.';
  }
  return 'Trade semantics unavailable.';
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

const computePostOnlyRetryPrice = (direction, ticker, instrument, attemptedPrice) => {
  const bidPrice = Number(ticker?.b) || 0;
  const askPrice = Number(ticker?.a) || 0;
  const step = getInstrumentPriceStep(instrument, attemptedPrice);

  if (direction === 'sell') {
    const retryPrice = askPrice > bidPrice
      ? Math.max(roundToStep(askPrice, step, 'up'), roundToStep(bidPrice + step, step, 'up'))
      : roundToStep(bidPrice + step, step, 'up');
    return retryPrice > 0 ? { retryPrice, bidPrice, askPrice, step } : null;
  }

  if (askPrice <= 0) return null;
  const belowAsk = askPrice - step;
  const candidate = belowAsk > 0
    ? roundToStep(belowAsk, step, 'down')
    : roundToStep(askPrice * 0.99, step, 'down');
  return candidate > 0 ? { retryPrice: candidate, bidPrice, askPrice, step } : null;
};

const executeOrder = async (action, instrumentName, amount, price, instruments, spotPrice, orderType = 'ioc', tickerMap = {}) => {
  // DRY_RUN mode: track budget discipline but skip actual order
  if (process.env.DRY_RUN === '1') {
    const totalValue = amount * price;
    if (action === 'buy_put') botData.putNetBought += totalValue;
    else if (action === 'sell_put') botData.putNetBought -= totalValue;
    persistCycleState();
    if (db) db.insertOrder({
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
  const direction = (action === 'buy_put' || action === 'buyback_call') ? 'buy' : 'sell';
  const reduceOnly = (action === 'sell_put' || action === 'buyback_call');

  // Find instrument to get base_asset_address and base_asset_sub_id
  const instrument = instruments.find(i => i.instrument_name === instrumentName);
  if (!instrument) {
    console.error(`❌ executeOrder: instrument ${instrumentName} not found`);
    return null;
  }

  const addr = instrument.base_asset_address;
  const subId = instrument.base_asset_sub_id;
  const ticker = tickerMap?.[instrumentName];

  let order;
  try {
    order = await placeOrder(
      instrumentName,
      amount.toFixed(2),
      direction,
      price,
      addr,
      subId,
      reduceOnly,
      orderType
    );
  } catch (error) {
    console.error(`❌ Error placing ${action} order for ${instrumentName}:`, error.message);
    if (db) db.insertOrder({ action, success: false, reason: `Order error: ${error.message}`, instrument_name: instrumentName, spot_price: spotPrice });
    return null;
  }

  if (!order) {
    if (db) db.insertOrder({ action, success: false, reason: 'Order placement failed', instrument_name: instrumentName, spot_price: spotPrice });
    return null;
  }

  // Detect post_only rejection (order would cross the book)
  if (order.rejected_post_only) {
    const retryPlan = orderType === 'post_only' ? computePostOnlyRetryPrice(direction, ticker, instrument, price) : null;
    const initialContext = formatPostOnlyContext({
      attemptedPrice: price,
      retryPrice: retryPlan?.retryPrice ?? null,
      bidPrice: retryPlan?.bidPrice ?? ticker?.b ?? 0,
      askPrice: retryPlan?.askPrice ?? ticker?.a ?? 0,
      step: retryPlan?.step ?? getInstrumentPriceStep(instrument, price),
      reason: order.error || null,
    });

    if (orderType === 'post_only' && retryPlan && Math.abs(retryPlan.retryPrice - price) > 1e-9) {
      console.log(`📋 post_only rejected: ${action} ${instrumentName} @ $${price} — retrying maker at $${retryPlan.retryPrice} (${initialContext})`);
      let retryOrder = null;
      try {
        retryOrder = await placeOrder(
          instrumentName,
          amount.toFixed(2),
          direction,
          retryPlan.retryPrice,
          addr,
          subId,
          reduceOnly,
          orderType
        );
      } catch (error) {
        console.error(`❌ Error retrying ${action} order for ${instrumentName}:`, error.message);
      }

      if (retryOrder && !retryOrder.rejected_post_only) {
        order = retryOrder;
        price = retryPlan.retryPrice;
      } else {
        const finalContext = formatPostOnlyContext({
          attemptedPrice: price,
          retryPrice: retryPlan.retryPrice,
          bidPrice: retryPlan.bidPrice,
          askPrice: retryPlan.askPrice,
          step: retryPlan.step,
          reason: retryOrder?.error || order.error || null,
        });
        console.log(`📋 post_only retry failed: ${action} ${instrumentName} — ${finalContext}`);
        if (db) db.insertOrder({
          action, success: false,
          reason: `post_only rejected after maker retry: ${finalContext}`,
          instrument_name: instrumentName, spot_price: spotPrice,
          price, intended_amount: amount,
        });
        return {
          postOnlyRejected: true,
          action,
          instrumentName,
          amount,
          price,
          orderType,
          context: finalContext,
          retryPrice: retryPlan.retryPrice,
        };
      }
    } else {
      console.log(`📋 post_only rejected: ${action} ${instrumentName} @ $${price} — no maker retry available (${initialContext})`);
      if (db) db.insertOrder({
        action, success: false,
        reason: `post_only rejected without retry: ${initialContext}`,
        instrument_name: instrumentName, spot_price: spotPrice,
        price, intended_amount: amount,
      });
      return { postOnlyRejected: true, action, instrumentName, amount, price, orderType, context: initialContext };
    }
  }

  // Fill accounting from actual trades
  let filledAmt = 0, avgPx = price, totalValue = 0;
  if (order.result?.trades?.length) {
    let totAmt = 0, totVal = 0;
    for (const t of order.result.trades) {
      const ta = Number(t.trade_amount), tp = Number(t.trade_price);
      totAmt += ta; totVal += ta * tp;
    }
    if (totAmt > 0) { filledAmt = totAmt; avgPx = totVal / totAmt; totalValue = totVal; }
  }

  // Zero-fill detection: IOC orders that matched nothing
  if (filledAmt === 0 && orderType === 'ioc') {
    console.log(`⚠️ Zero fill: ${action} ${instrumentName} @ $${price} [IOC] — no liquidity`);
    if (db) db.insertOrder({
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

  // GTC/post-only orders with zero fills are resting on the book — track as open
  if (filledAmt === 0 && (orderType === 'gtc' || orderType === 'post_only')) {
    const orderId = order.result?.order_id || order.result?.order?.order_id || null;
    console.log(`📋 Order resting: ${action} ${instrumentName} @ $${price} [${orderType}] orderId=${orderId}`);
    if (db) db.insertOrder({
      action, success: true, reason: `Resting ${orderType} order placed`,
      instrument_name: instrumentName,
      strike: instrument.option_details?.strike || null,
      expiry: instrument.option_details?.expiry || null,
      delta: null, price, intended_amount: amount,
      filled_amount: 0, fill_price: null,
      total_value: 0, spot_price: spotPrice,
      raw_response: order,
    });
    // Track resting order for fill reconciliation
    if (db && orderId) {
      db.insertRestingOrder({
        order_id: orderId,
        instrument_name: instrumentName,
        action,
        direction,
        amount,
        limit_price: price,
      });
    }
    return { resting: true, orderId, action, instrumentName, amount, price, orderType };
  }

  // Track put budget discipline (arithmetic cost commitment)
  if (action === 'buy_put') {
    botData.putNetBought += totalValue;
  } else if (action === 'sell_put') {
    botData.putNetBought -= totalValue;
  }
  persistCycleState();

  // Log to DB
  if (db) {
    const strike = instrument.option_details?.strike || null;
    const expiry = instrument.option_details?.expiry || null;
    db.insertOrder({
      action, success: true, reason: `LLM-confirmed ${action} [${orderType}]`,
      instrument_name: instrumentName, strike, expiry,
      delta: null, price, intended_amount: amount,
      filled_amount: filledAmt, fill_price: avgPx,
      total_value: totalValue, spot_price: spotPrice,
      raw_response: order,
    });
  }

  console.log(`✅ ${action.toUpperCase()}: ${filledAmt} ${instrumentName} @ $${avgPx.toFixed(4)} [${orderType}] | total=$${totalValue.toFixed(4)}`);
  sendTelegram(`✅ *${action.toUpperCase()}* ${instrumentName}\nAmount: ${filledAmt} @ $${avgPx.toFixed(4)}\nTotal: $${totalValue.toFixed(4)}`);
  return { filledAmt, avgPx, totalValue, order, orderType };
};

const confirmAndExecutePending = async (instruments, tickerMap, spotPrice) => {
  if (!db) return;

  const pending = db.getPendingActions('pending');
  if (pending.length === 0) return;

  console.log(`🔍 ${pending.length} pending action(s) to confirm`);

  let confirmed = 0;
  for (const action of pending.slice(0, 2)) { // Max 2 per tick
    // Fetch fresh margin state for each confirmation (margin changes between trades)
    let marginState = null;
    try { marginState = await fetchSubaccount(); } catch { /* ok */ }
    const marginStr = marginState
      ? `Margin: buying_power=$${marginState.initial_margin.toFixed(2)}, account_value=$${marginState.subaccount_value.toFixed(2)}, collateral=$${marginState.collaterals_value.toFixed(2)}${marginState.is_under_liquidation ? ' [UNDER LIQUIDATION]' : ''}`
      : 'Margin: unavailable';
    try {
      // Auto-reject after 3 retries
      if (action.retries >= 3) {
        db.updatePendingAction(action.id, { status: 'rejected', confirmation_reasoning: 'Auto-rejected after 3 failed confirmation attempts' });
        console.log(`❌ Auto-rejected: ${action.action} ${action.instrument_name} (3 retries)`);
        continue;
      }

      // Hard safety: reject new entries if under liquidation
      if (marginState?.is_under_liquidation && (action.action === 'buy_put' || action.action === 'sell_call')) {
        db.updatePendingAction(action.id, { status: 'rejected', confirmation_reasoning: 'Auto-rejected: account under liquidation' });
        console.log(`🚨 Auto-rejected ${action.action} ${action.instrument_name}: account under liquidation`);
        sendTelegram(`🚨 *LIQUIDATION WARNING* — auto-rejected ${action.action} ${action.instrument_name}`);
        continue;
      }

      // Build context for confirmation
      const ticker = tickerMap[action.instrument_name];
      const currentPrice = ticker ? (action.action.includes('buy') ? Number(ticker.a) : Number(ticker.b)) : action.price;
      const momentum = botData.mediumTermMomentum;

      // Determine what details to show
      let detailsStr;
      if (action.action === 'sell_put' || action.action === 'buyback_call') {
        // Exit: show position info
        detailsStr = `Position exit. ${describeActionSemantics(action.action)} Trigger: ${action.trigger_details || 'N/A'}`;
      } else {
        // Entry: show option info
        const delta = ticker ? Number(ticker.option_pricing?.d) : null;
        detailsStr = `${describeActionSemantics(action.action)} Delta: ${delta?.toFixed(4) || 'N/A'}, Price: $${currentPrice?.toFixed(4) || 'N/A'}`;
      }

      // Parse trigger details for advisory's preferred order type
      let triggerData = {};
      try { triggerData = typeof action.trigger_details === 'string' ? JSON.parse(action.trigger_details) : (action.trigger_details || {}); } catch {}
      const advisoryOrderPref = triggerData.preferred_order_type;

      const confirmPrompt = `Trade confirmation:
Action: ${action.action} ${action.instrument_name}
Amount: ${action.amount || 'TBD'}
Best available price: $${currentPrice || action.price || 'N/A'}
${detailsStr}
Rule reasoning: ${action.rule_reasoning || 'N/A'}
Triggered because: ${action.trigger_details || 'N/A'}
Action semantics: ${describeActionSemantics(action.action)}
Market: spot=$${spotPrice}, momentum=${JSON.stringify(momentum)}
${marginStr}
${advisoryOrderPref ? `Advisory suggested order type: ${advisoryOrderPref}` : ''}
Confirm or reject this trade. If confirming, choose the order execution strategy:
- "ioc" (immediate-or-cancel): fill now at market or cancel. TAKER fee = $0.50 base + 0.03% of notional (~$1/contract for ETH options). Use only when the opportunity is exceptional and might vanish.
- "gtc" (good-til-cancelled): rest on the order book at your limit_price until filled. MAKER fee = 0.01% of notional (~$0.16/contract). 6x cheaper than IOC. Use when you want a specific price and can wait.
- "post_only": like GTC but rejected if it would cross the book (guaranteed maker fee 0.01%). 6x cheaper than IOC. Best for patient limit orders.

JSON only: { "confirm": true/false, "order_type": "ioc"|"gtc"|"post_only", "limit_price": <number or null for market>, "reasoning": "..." }`;

      // Vote 1: Claude Haiku (Spitznagel temperament)
      let haikuVote = null;
      try {
        const haikuResp = await axios.post('https://api.anthropic.com/v1/messages', {
          model: 'claude-haiku-4-5-20251001',
          max_tokens: 256,
          system: `You are a Spitznagel-style risk advisor. Confirm trades that are disciplined and arithmetic. Reject trades that overpay for insurance or chase expensive protection. Be conservative — when in doubt, reject.
EXIT SEMANTICS: sell_put means selling an already-owned long put to close or trim it. It is reduce_only=true. It does NOT open naked short put exposure. buyback_call means buying back an existing short call to close or trim it. It is reduce_only=true.
MARGIN AWARENESS: Account is ETH-collateralized. Long puts offset ETH exposure in margin. Reject trades that would push initial_margin dangerously low. If the account is under liquidation, reject all new entries.
CALL DISCIPLINE: Short calls are hard-capped at ${(CALL_EXPOSURE_CAP_PCT * 100).toFixed(0)}% inferred Derive margin utilization. Reject call sells that would push margin usage too high or are too small to matter.
CALL BUYBACK DISCIPLINE: For buyback_call — don't panic buyback. Buying back a short call because price rose is paying the crowd's fear premium. Ask: is the position genuinely threatened, or does it just feel that way? Confirm buybacks that lock in meaningful profit or exit a position that's truly challenged. Reject buybacks driven by price action alone when theta is still working and the position isn't under real threat. Use the Greeks and remaining DTE to judge the actual risk.
PUT EXIT DISCIPLINE: For sell_put — evaluate it as monetizing or rolling an existing long put. Do not analyze it as short put selling or naked downside exposure. The question is whether closing this owned hedge now is prudent, not whether opening short put risk is acceptable.
REGIME AWARENESS: ETH crashes cascade and accelerate. Consider whether selling profitable puts is premature if the crash has further to go. Consider whether buying puts at spiked IV overpays for insurance. Use the actual Greeks, DTE, and momentum to judge — no rigid rules, just awareness that selloffs go deeper and faster than expected.`,
          messages: [{ role: 'user', content: confirmPrompt }],
        }, {
          headers: {
            'x-api-key': process.env.ANTHROPIC_API_KEY,
            'anthropic-version': '2023-06-01',
            'content-type': 'application/json',
          },
          timeout: 15000,
        });
        const haikuText = haikuResp.data?.content?.[0]?.text || '';
        haikuVote = extractJSON(haikuText);
      } catch (e) {
        console.log(`⚠️ Haiku confirmation failed: ${e.message}`);
      }

      // Vote 2: OpenAI GPT (Taleb temperament)
      let codexVote = null;
      try {
        const codexText = await callOpenAI(
          `You are a Taleb-style risk advisor. Your philosophy has TWO sides:
1. BUY CONVEXITY CHEAP: Long puts are insurance — bounded cost, unbounded upside. Confirm puts that are cheap relative to the tail risk they cover. Reject puts that overpay for protection (high IV, crowd panic).
2. SELL THE CROWD'S GREED: Selling calls is routine — exploit mispriced optimism to fund insurance. Confirm call sells when the premium is irrational relative to the actual probability, the strike gives real cushion, and exposure is sized to survive the worst case. Reject when the premium doesn't justify the risk or margin can't absorb an adverse move.
EXIT SEMANTICS: sell_put means selling an already-owned long put to close or trim it. It is reduce_only=true and cannot create a naked short put. buyback_call means buying back an existing short call to close or trim it. It is reduce_only=true.
RUIN AVOIDANCE: The only real constraint. Reject trades that could cause ruin — margin too thin, exposure too concentrated, or sizing that doesn't survive a 2-sigma move. Everything else is about getting paid for risk the crowd misprices.
MARGIN AWARENESS: Account is ETH-collateralized. Long puts offset ETH exposure in margin. Reject if initial_margin is dangerously low or account is under liquidation.
CALL DISCIPLINE: Short calls are hard-capped at ${(CALL_EXPOSURE_CAP_PCT * 100).toFixed(0)}% inferred Derive margin utilization. Reject call sells that would push margin usage too high or are too small to matter.
CALL BUYBACK DISCIPLINE: Panic buybacks are fragile. The crowd buys back calls when price rises because it feels dangerous — that's paying a fear premium. Confirm buybacks only when the position is genuinely threatened or profit is worth locking in. Reject buybacks driven by price noise when theta is still working.
PUT EXIT DISCIPLINE: For sell_put, judge whether monetizing or rolling an owned long hedge is sensible. Never treat sell_put as opening naked short put exposure.
REGIME AWARENESS: ETH crashes cascade fast. Selling puts during an active crash may be selling convexity prematurely. Buying puts at spiked IV overpays alongside the crowd. Use actual Greeks, DTE, and momentum to judge.
Output JSON only: { "confirm": true/false, "order_type": "ioc"|"gtc"|"post_only", "limit_price": <number or null>, "reasoning": "..." }`,
          confirmPrompt,
          { maxTokens: 256, timeout: 15000, model: 'gpt-4o-mini' }
        );
        if (codexText) {
          codexVote = extractJSON(codexText);
        }
      } catch (e) {
        console.log(`⚠️ OpenAI confirmation failed: ${e.message}`);
      }

      // Voting logic
      let decision;
      if (haikuVote && codexVote) {
        // Both voted
        decision = (haikuVote.confirm && codexVote.confirm) ? 'confirmed' : 'rejected';
      } else if (haikuVote) {
        // Single advisor fallback
        decision = haikuVote.confirm ? 'confirmed' : 'rejected';
      } else if (codexVote) {
        decision = codexVote.confirm ? 'confirmed' : 'rejected';
      } else {
        // Both failed — increment retries
        db.updatePendingAction(action.id, { retries: (action.retries || 0) + 1 });
        console.log(`⚠️ Confirmation failed for ${action.instrument_name} (retry ${(action.retries || 0) + 1})`);
        continue;
      }

      const reasoning = [
        haikuVote ? `Haiku: ${haikuVote.confirm ? 'CONFIRM' : 'REJECT'} — ${haikuVote.reasoning || 'no reason'}` : 'Haiku: FAILED',
        codexVote ? `OpenAI: ${codexVote.confirm ? 'CONFIRM' : 'REJECT'} — ${codexVote.reasoning || 'no reason'}` : 'OpenAI: FAILED',
      ].join(' | ');

      // Resolve order type from voter consensus (prefer haiku's pick, fallback to codex)
      const confirmedOrderType = (haikuVote?.order_type || codexVote?.order_type || 'ioc');
      const validOrderTypes = ['ioc', 'gtc', 'post_only'];
      const orderType = validOrderTypes.includes(confirmedOrderType) ? confirmedOrderType : 'ioc';

      // Resolve limit price: voter can override, otherwise use current market price
      // Sanity check: voter price must be within 50% of market price (prevents LLM hallucinating insane prices)
      const voterLimitPrice = haikuVote?.limit_price || codexVote?.limit_price;
      const marketPrice = currentPrice || action.price;
      let executionPrice = marketPrice;
      if (typeof voterLimitPrice === 'number' && voterLimitPrice > 0 && marketPrice > 0) {
        const ratio = voterLimitPrice / marketPrice;
        if (ratio >= 0.5 && ratio <= 2.0) {
          executionPrice = voterLimitPrice;
        } else {
          console.log(`⚠️ Voter price $${voterLimitPrice} rejected (${(ratio * 100).toFixed(0)}% of market $${marketPrice}) — using market price`);
        }
      }

      if (decision === 'confirmed') {
        db.updatePendingAction(action.id, {
          status: 'confirmed',
          confirmation_reasoning: `${reasoning} | order_type=${orderType} limit=$${executionPrice}`,
          confirmed_at: new Date().toISOString(),
        });

        // Execute the trade
        const result = await executeOrder(
          action.action,
          action.instrument_name,
          action.amount || 0.01,
          executionPrice,
          instruments,
          spotPrice,
          orderType,
          tickerMap
        );

        if (result && result.postOnlyRejected) {
          // post_only failed even after one maker retry — mark failed, don't retry again this tick
          db.updatePendingAction(action.id, {
            status: 'failed',
            execution_result: `post_only rejected: ${result.context || `would cross book at $${executionPrice}`}. Price may have moved — will re-evaluate next tick.`,
          });
          console.log(`📋 post_only rejected: ${action.action} ${action.instrument_name} — ${result.context || 'price crossed book'}`);
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
            status: 'executed',
            executed_at: new Date().toISOString(),
            execution_result: JSON.stringify({ ...result, price: finalOrderPrice, note: 'Resting on order book' }),
          });
          confirmed++;
          console.log(`📋 Resting order: ${action.action} ${action.instrument_name} @ $${finalOrderPrice} [${orderType}] | ${reasoning}`);
        } else if (result) {
          db.updatePendingAction(action.id, {
            status: 'executed',
            executed_at: new Date().toISOString(),
            execution_result: JSON.stringify(result),
          });
          confirmed++;
          console.log(`✅ Confirmed & executed: ${action.action} ${action.instrument_name} [${orderType}] | ${reasoning}`);
        } else {
          db.updatePendingAction(action.id, { status: 'failed', execution_result: 'Order placement failed' });
          console.log(`❌ Confirmed but execution failed: ${action.action} ${action.instrument_name}`);
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

  if (confirmed > 0) console.log(`📋 Executed ${confirmed} trade(s) this tick`);
};

// ─── LLM-Driven Trading Advisory ─────────────────────────────────────────────

const generateTradingAdvisory = async (positions, spotPrice, tickerMap) => {
  if (!process.env.ANTHROPIC_API_KEY) {
    console.log('📋 Advisory: skipped — no ANTHROPIC_API_KEY');
    return null;
  }

  // Mutex: prevent overlapping advisory runs
  if (_advisoryInFlight) {
    console.log('📋 Advisory: skipped — already in flight');
    return null;
  }
  _advisoryInFlight = true;

  try {
  const advisoryId = `adv_${Date.now()}`;
  console.log(`📋 Advisory ${advisoryId}: starting 3-step deliberation...`);

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
  const since24h = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  let sentiment = {};
  if (db) {
    try {
      sentiment = {
        fundingRate: db.getFundingRateLatest(),
        fundingAvg24h: db.getFundingRateAvg24h(),
        optionsSkew: db.getOptionsSkew(since24h),
        aggregateOI: db.getAggregateOI(since24h),
        marketQuality: db.getMarketQualitySummary(since24h),
      };
    } catch (e) {
      console.log('📋 Advisory: failed to fetch sentiment:', e.message);
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

  const accountHealth = {
    ethBalance,
    usdcBalance,
    shortCallExposure: totalCallExposure,
    callMarginDiscipline: {
      capPct: CALL_EXPOSURE_CAP_PCT,
      currentShortExposure: +totalCallExposure.toFixed(2),
      utilizationPct: marginState ? +(100 * (estimateMarginUtilization(marginState) || 0)).toFixed(1) : null,
      marginBase: marginState ? +getMarginCapacityBase(marginState).toFixed(2) : null,
      note: `Hard cap: keep inferred Derive margin utilization below ${(CALL_EXPOSURE_CAP_PCT * 100).toFixed(0)}%. We infer utilization conservatively as (positions_initial_margin + open_orders_margin) / collateral margin base.`,
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
        ? +((100 * (estimateMarginUtilization(marginState) || 0))).toFixed(1)
        : 0,
    } : null,
    putBudgetDiscipline: {
      annualRate: PUT_ANNUAL_RATE,
      budgetThisCycle: botData.putBudgetForCycle,
      spent: botData.putNetBought,
      remaining: putBudgetRemaining,
      rollover: botData.putUnspentBuyLimit,
      cycleDays: BOT_CONFIG.PERIOD_DAYS,
      note: `Arithmetic commitment: ${(PUT_ANNUAL_RATE * 100).toFixed(2)}% of portfolio value per year, allocated in ${BOT_CONFIG.PERIOD_DAYS}-day windows. Funded via leverage on ETH collateral. Spend predictably across the cycle — not all at once.`,
    },
    note: 'Account is ETH-collateralized on Derive. buying_power = available margin for new trades (initial_margin from API). margin_usage_pct is inferred conservatively as (positions_initial_margin + open_orders_margin) / collateral margin base. Sizing must respect buying power, put budget discipline, AND the call margin-utilization cap.',
  };

  // Recent orders
  const since7d = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  let recentOrders = [];
  if (db) {
    try { recentOrders = db.getRecentOrders(since7d, 10); } catch { /* ok */ }
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
      dte: Math.max(0, (expiry.getTime() - Date.now()) / (24 * 60 * 60 * 1000)),
    };
  };

  // Score candidates with wide ranges — the advisory sees more of the market
  // and can recommend tighter ranges in its rules if it wants
  const scoredPuts = [];
  const scoredCalls = [];

  for (const [name, ticker] of Object.entries(tickerMap)) {
    const parsed = parseInstrumentName(name);
    if (!parsed) continue;

    const delta = Number(ticker.option_pricing?.d) || 0;
    const askPrice = Number(ticker.a) || 0;
    const bidPrice = Number(ticker.b) || 0;

    if (parsed.optionType === 'P') {
      // Wide filter: delta -0.15 to -0.01, DTE 14-120 (advisory decides what's interesting)
      if (delta >= -0.15 && delta <= -0.01 && parsed.dte >= 14 && parsed.dte <= 120 && askPrice > 0) {
        const score = Math.abs(delta) / askPrice;
        scoredPuts.push({ name, delta, askPrice, bidPrice, dte: Math.round(parsed.dte), strike: parsed.strike, score });
      }
    } else if (parsed.optionType === 'C') {
      // Wide filter: delta 0.01 to 0.15, DTE 3-45 (advisory decides what's interesting)
      if (delta >= 0.01 && delta <= 0.15 && parsed.dte >= 3 && parsed.dte <= 45 && bidPrice > 0) {
        const score = bidPrice / Math.abs(delta);
        scoredCalls.push({ name, delta, askPrice, bidPrice, dte: Math.round(parsed.dte), strike: parsed.strike, score });
      }
    }
  }

  scoredPuts.sort((a, b) => b.score - a.score);
  scoredCalls.sort((a, b) => b.score - a.score);
  const top5Puts = scoredPuts.slice(0, 8);
  const top5Calls = scoredCalls.slice(0, 8);

  // ── Step 1: Primary Advisor (Claude Opus, Spitznagel temperament) ───────────

  console.log('📋 Advisory Step 1: Primary advisor (Claude Opus)...');

  const primarySystemPrompt = `You are a senior options strategist with Mark Spitznagel's temperament. Your philosophy:
- Arithmetic discipline above all: every trade must have positive expected value in crash scenarios
- Patience is the edge: being willing to sit on hands when pricing is unfavorable
- Insurance must be well-priced: never overpay for puts, never undersell calls
- Tail risk is the real risk: the portfolio must survive a 40%+ drawdown
- Premium collection supplements, not replaces, insurance accumulation

You advise a bot that accumulates OTM ETH puts (long insurance) and sells OTM ETH calls (premium harvesting).

## Account Model
The account is ETH-collateralized. Puts are bought on leverage against ETH. Derive's margin engine recognizes that long puts offset ETH exposure, so buying puts can improve margin health. USDC from call premiums pays down margin debt first; excess gets converted to ETH manually.

There are TWO constraints on put buying:
1. **Margin**: initial_margin must stay positive. maintenance_margin at zero = liquidation.
2. **Budget discipline**: We commit to spending 3.33% of portfolio value per year on puts, allocated in 15-day budget windows. The budget for each cycle is calculated from current portfolio value at cycle start. Spend predictably across the cycle. Don't front-load. Don't impulse buy. If nothing is well-priced, let the budget roll over.

For calls: **hard cap at ${(CALL_EXPOSURE_CAP_PCT * 100).toFixed(0)}% inferred Derive margin utilization**. The bot enforces this conservatively using documented margin fields. Within that cap, put skin in the game: each call trade should be meaningful, not tiny nibbles. Size against margin headroom, not ETH balance.

## Call Buyback Philosophy (Spitznagel)
We sell short-dated calls. The arithmetic of buybacks is simple: don't pay fear premiums to exit positions that are working.
- **Profit capture over panic**: When a meaningful chunk of premium has decayed, locking it in and redeploying is disciplined. Buying back at a loss because the price moved against you is not — it's paying the crowd's fear premium.
- **Let theta work**: Short calls are a time-decay trade. A price move against you doesn't change the thesis unless the position is genuinely threatened. Use the Greeks and the position's actual risk profile to judge, not the price action alone.
- **Rolling discipline**: Rolling should improve the position, not just delay a loss. If rolling costs more than it's worth, accepting assignment is the honest response.
- Use your judgment on the specific situation — the Greeks, DTE remaining, how much premium has decayed, and the broader portfolio context all matter more than any fixed threshold.

## Market Regime Awareness
ETH crashes tend to cascade — they accelerate, not slow down. Your decisions should reflect the shape of the moment.

Things to consider in your assessment:
- In calm markets, insurance is cheap. That's when to accumulate it. If puts are expensive, patience is the edge.
- In crashing markets, our puts become increasingly valuable. The temptation is to sell early. Consider whether the crash has further to go — ETH selloffs often have multiple legs.
- In recovery, fear lingers and IV stays elevated even as price stabilizes. Panickers overpay for protection they no longer need as urgently. This can be an opportunity.
- The full cycle: cash → cheap puts → crash → puts print → sell at the right time → buy cheap ETH → sell calls → premium → repeat.

Use your judgment. Look at the actual Greeks, DTE, IV, momentum, and position characteristics. There are no absolute rules — only the principle that well-priced insurance is bought in calm and sold in fear, and that ETH selloffs tend to be deeper and faster than anyone expects.

Given market data, produce a JSON trading agenda with:
{
  "assessment": "1-3 sentence market assessment and overall stance",
  "entry_rules": [
    {
      "action": "buy_put" | "sell_call",
      "criteria": {
        "option_type": "P" or "C",
        "delta_range": [min_delta, max_delta],
        "dte_range": [min_dte, max_dte],
        "max_strike_pct": 0.80,
        "min_score": 0.004,
        "max_cost": 15.00,
        "min_bid": 2.00,
        "market_conditions": [{"field": "spot_price", "op": "lt"|"gt"|"gte"|"lte", "value": 2000}]
      },
      "budget_limit": <max USD to spend on this rule>,
      "priority": "high" | "medium" | "low",
      "preferred_order_type": "ioc" | "gtc" | "post_only",
      "reasoning": "why this trade makes sense now"
    }
  ],
  "exit_rules": [
    {
      "action": "sell_put" | "buyback_call",
      "instrument_name": "<specific instrument name from positions>",
      "criteria": {
        "conditions": [{"field": "dte"|"delta"|"mark_price"|"unrealized_pnl_pct"|"iv"|"theta"|"spot_price", "op": "lt"|"gt"|"gte"|"lte", "value": <number>}],
        "condition_logic": "any" | "all"
      },
      "priority": "high" | "medium" | "low",
      "preferred_order_type": "ioc" | "gtc" | "post_only",
      "reasoning": "why exit is warranted"
    }
  ]
}

CRITICAL: criteria must be a JSON OBJECT (not a string). Entry criteria uses: option_type, delta_range, dte_range, max_strike_pct, min_score, max_cost, min_bid, market_conditions. Exit criteria uses: conditions (array of field/op/value objects) and condition_logic ("any" or "all").

Rules:
- Entry criteria MUST include: option_type ("P" or "C"), delta_range [min, max], dte_range [min, max]. Optional: max_strike_pct, min_score, max_cost (for buys), min_bid (for sells), market_conditions.
- Exit criteria MUST include: conditions (array of {field, op, value}), condition_logic ("any" or "all"). Fields: dte, delta, mark_price, unrealized_pnl_pct, iv, theta, spot_price. Ops: gt, lt, gte, lte.
- For buy_put: set option_type "P", negative delta_range (e.g. [-0.08, -0.02]), max_cost for the max ask price. DTE DISCIPLINE: buy puts at 45-75 DTE. Never buy puts below 35 DTE — short-dated puts bleed theta too fast for tail insurance. dte_range must be within [45, 75].
- For sell_put exits (rolling): roll long puts when DTE reaches ~25. Use exit condition dte lte 25 to trigger the roll. This preserves convexity while avoiding terminal theta decay.
- For sell_call: set option_type "C", positive delta_range (e.g. [0.02, 0.10]), min_bid for the minimum bid price. DTE DISCIPLINE: sell calls at 5-12 DTE. Short-dated calls maximize theta decay harvesting. dte_range must be within [5, 12].
- For sell_put exits: use conditions on dte (e.g. dte lte 25) and/or unrealized_pnl_pct. IMPORTANT: sell_put means selling an already-owned long put to close or roll it. It is reduce_only and must never be interpreted as opening a naked short put. Do NOT generate sell_put rules for positions with mark price below $0.10 — selling worthless puts recovers nothing (we already paid for them). Let them expire. Selling a long put does NOT release margin on Derive.
- For buyback_call exits: use conditions on unrealized_pnl_pct, dte, and/or delta. Think about what actually threatens the position vs. what's just noise. Profit capture, genuine assignment risk, and expiry cleanup are good reasons. Price moving against you alone is not — that's panic buying the crowd's fear premium. Set conditions that reflect the position's actual risk profile.
- budget_limit is how much USD to allocate to this rule. For puts: must stay within the remaining put budget (arithmetic discipline — we commit to a predictable spend rate per cycle). For calls: size based on margin health and ETH collateral.
- The account is ETH-collateralized. Long puts OFFSET ETH exposure in Derive's margin engine. But the premium cost is real — respect the put budget discipline.
- Put budget is an arithmetic commitment, not a cash constraint. We buy puts on leverage. The budget prevents impulse buying or underspending.
- For calls: hard cap at ${(CALL_EXPOSURE_CAP_PCT * 100).toFixed(0)}% inferred Derive margin utilization. The code enforces this conservatively from margin fields. Size meaningfully, but do not exceed the margin-utilization cap.
- Entry rules should target the highest-scoring candidates when possible
- Exit rules MUST reference specific instrument_name from current positions
- If the market is unclear, it is ALWAYS correct to produce fewer rules or none
- Maximum 5 entry rules and 5 exit rules

Order type guidance (fee matters — maker is 6x cheaper than taker):
- "ioc" (immediate-or-cancel): fill instantly or cancel. TAKER fee = $0.50 base + 0.03% of notional (~$1/contract for ETH options). Only use when the opportunity is exceptional and might vanish.
- "gtc" (good-til-cancelled): rest on the order book. MAKER fee = 0.01% of notional (~$0.16/contract). Use when you want to name your price and wait.
- "post_only": like GTC but rejected if it would cross the book (guaranteed maker fee 0.01%). Best for patient entries — cheapest execution.
- DEFAULT to post_only or gtc. Only suggest ioc when urgency genuinely justifies paying 6x more in fees.
- The confirmation step can override your suggestion, so this is advisory guidance not a hard rule.

- Return ONLY valid JSON, no markdown fences`;

  const primaryUserPrompt = `=== CURRENT MARKET STATE ===
Spot Price: $${spotPrice.toFixed(2)}
Momentum: Medium-term ${momentum.mediumTerm.main} (${momentum.mediumTerm.derivative || 'n/a'}), Short-term ${momentum.shortTerm.main} (${momentum.shortTerm.derivative || 'n/a'})

=== PORTFOLIO ===
Positions: ${JSON.stringify(positions.map(p => ({
  instrument: p.instrument_name, direction: p.direction, amount: p.amount,
  delta: p.delta, theta: p.theta, unrealized_pnl: p.unrealized_pnl
})), null, 1)}

Balances: ${JSON.stringify(balances, null, 1)}

=== ACCOUNT HEALTH (margin-aware sizing) ===
${JSON.stringify(accountHealth, null, 2)}

=== MARKET SENTIMENT ===
${JSON.stringify(sentiment, null, 2)}

=== TOP PUT CANDIDATES (by delta/ask ratio, wide scan) ===
${top5Puts.length > 0 ? top5Puts.map((p, i) => `${i + 1}. ${p.name} | delta=${p.delta.toFixed(4)} | ask=$${p.askPrice.toFixed(2)} | DTE=${p.dte} | score=${p.score.toFixed(4)}`).join('\n') : 'No qualifying puts found'}

=== TOP CALL CANDIDATES (by bid/delta ratio, wide scan) ===
${top5Calls.length > 0 ? top5Calls.map((c, i) => `${i + 1}. ${c.name} | delta=${c.delta.toFixed(4)} | bid=$${c.bidPrice.toFixed(2)} | DTE=${c.dte} | score=${c.score.toFixed(4)}`).join('\n') : 'No qualifying calls found'}

=== RECENT ORDERS (last 7d) ===
${recentOrders.length > 0 ? recentOrders.map(o => `${o.timestamp} | ${o.action} ${o.instrument_name} | ${o.success ? 'OK' : 'FAIL'} | $${o.total_value || '?'}`).join('\n') : 'No recent orders'}

=== CURRENT ACTIVE RULES ===
${activeRules.length > 0 ? JSON.stringify(activeRules.map(r => ({ type: r.rule_type, action: r.action, criteria: r.criteria, priority: r.priority })), null, 1) : 'No active rules'}

=== RECENT PENDING ACTIONS ===
${recentPendingActions.length > 0 ? JSON.stringify(recentPendingActions.map(a => ({ action: a.action, instrument: a.instrument_name, status: a.status, triggered: a.triggered_at })), null, 1) : 'No recent pending actions'}

=== OPEN ORDERS ON BOOK ===
${openOrders.length > 0 ? openOrders.map(o => `${o.instrument_name} | ${o.direction} ${o.amount} @ $${o.limit_price} | filled=${o.filled_amount} | ${o.time_in_force} | age=${((Date.now() - o.creation_timestamp) / 3600000).toFixed(1)}h`).join('\n') : 'No open orders'}
${wikiSignals ? `\n=== WIKI SIGNALS (parsed from knowledge base) ===
Regime: ${wikiSignals.regime || 'unknown'} (confidence: ${wikiSignals.regimeConfidence || 'unknown'})
Protection cost: ${wikiSignals.protectionAssessment || 'unknown'}
Call premium: ${wikiSignals.revenueAssessment || 'unknown'}
${wikiSignals.playbookRules.length > 0 ? `Playbook rules:\n${wikiSignals.playbookRules.map(r => `- ${r}`).join('\n')}` : ''}` : ''}
${wikiContext ? `\n=== KNOWLEDGE WIKI (cumulative bot knowledge) ===\n${wikiContext}` : ''}

Produce your trading agenda JSON now.`;

  let primaryAgenda = null;
  try {
    const primaryResponse = await axios.post('https://api.anthropic.com/v1/messages', {
      model: 'claude-opus-4-6',
      max_tokens: 3000,
      system: primarySystemPrompt,
      messages: [{ role: 'user', content: primaryUserPrompt }],
    }, {
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      timeout: 120000,
    });

    const primaryText = primaryResponse.data?.content?.[0]?.text || '';
    try {
      primaryAgenda = extractJSON(primaryText);
      if (primaryAgenda) {
        console.log(`📋 Advisory Step 1: got ${primaryAgenda.entry_rules?.length || 0} entry rules, ${primaryAgenda.exit_rules?.length || 0} exit rules`);
      } else {
        throw new Error('No JSON block found in primary response');
      }
    } catch (parseErr) {
      console.log('📋 Advisory Step 1: JSON parse failed:', parseErr.message);
      throw parseErr;
    }
  } catch (e) {
    console.log('📋 Advisory Step 1 FAILED:', e.message);
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
- Convexity. Every trade should have bounded downside and unbounded upside.
- Skin in the game. If a trade goes wrong, the cost must be small and known.
- Fat tails. The market is more volatile than anyone thinks. Events that "shouldn't happen" happen regularly.
- Via negativa. What you DON'T do matters more than what you do. Avoid ruin above all.

## DTE Discipline (Non-Negotiable)
- Buy puts at 45-75 DTE. Never below 35 DTE. Short-dated puts bleed theta — you're paying for time decay, not convexity. Veto any buy_put rule outside [45, 75].
- Roll (sell_put exit) at ~25 DTE to avoid terminal theta decay while preserving convexity.
- Sell calls at 5-12 DTE. Short-dated calls maximize theta harvesting. Veto any sell_call rule with dte above 14.

## Call Buyback Anti-Fragility (Taleb)
Panic buybacks are the opposite of antifragility. The crowd buys back calls when price rises because it FEELS dangerous. That's paying a fear premium — the exact behavior we profit from.
- The asymmetry of short calls is known and bounded. You sold time decay. The question is always: is the position genuinely threatened, or does it just feel that way?
- Scrutinize any buyback rule that triggers on price movement alone. Ask: is the portfolio actually at risk of ruin, or is this noise?
- Rolling for a net debit is paying to extend exposure. If you can't roll favorably, accepting assignment is the antifragile response — it means you were right about the price level when you sold.
- Use your judgment on what constitutes a real threat vs. noise. The Greeks, remaining DTE, premium captured, and portfolio shape tell the story — not the last candle.

## Market Regime Awareness
ETH crashes cascade — they accelerate, not slow down. Your critique should consider:
- Selling puts during an active crash means selling convexity that could multiply further. Scrutinize the timing.
- Buying puts when IV is spiked means overpaying for insurance alongside the crowd. Question the arithmetic.
- In recovery (price stabilizing, IV still elevated), selling puts to fearful buyers can capture inflated premiums.
- But these are tendencies, not absolutes. The actual Greeks, DTE, position size, and portfolio shape matter. Use your judgment.
- Ask: is this trade benefiting from disorder (antifragile) or just reacting to it (fragile)?`;

    const talebPrompt = `## The Agenda to Review
${JSON.stringify(primaryAgenda)}

## Market Context
Spot: $${spotPrice}, Positions: ${JSON.stringify(positions.slice(0, 5))}, Momentum: ${JSON.stringify(momentum)}
Account: ETH-collateralized. ${marginState ? `Initial margin: $${marginState.initial_margin.toFixed(2)}, Maintenance: $${marginState.maintenance_margin.toFixed(2)}, Account value: $${marginState.subaccount_value.toFixed(2)}` : 'Margin data unavailable'}

## Your Task
Critique the agenda. For each rule, ask:
1. Is the downside truly bounded? What's the worst case?
2. Where is the convexity? Is the asymmetry real or imagined?
3. Are we being antifragile or just hedged?
4. What would the naive crowd do here, and are we positioned opposite them?

Output JSON only:
{
  "critique": "Overall assessment of the agenda",
  "amendments": [{"rule_index": 0, "concern": "...", "suggested_change": {...}, "severity": "medium"}],
  "vetoes": [{"rule_index": 0, "reason": "..."}],
  "additions": []
}`;

    const talebText = await callOpenAI(talebSystem, talebPrompt, { maxTokens: 2048, timeout: 60000 });
    if (talebText) {
      secondOpinion = extractJSON(talebText);
      if (secondOpinion) {
        console.log(`📋 Taleb review: ${secondOpinion.vetoes?.length || 0} vetoes, ${secondOpinion.amendments?.length || 0} amendments`);
      }
    }
  } catch (e) {
    console.log(`📋 Taleb review failed (non-fatal): ${e.message}`);
  }

  // ── Step 3: Synthesis (Claude Sonnet) ───────────────────────────────────────

  console.log('📋 Advisory Step 3: Synthesis (Claude Sonnet)...');

  let finalAgenda = primaryAgenda; // Default: use primary if synthesis fails

  const synthesisSystemPrompt = secondOpinion
    ? `You are the Synthesizer on a trading council. You have two advisor inputs. Your job is to produce the final trading agenda.

CRITICAL: criteria must be a JSON OBJECT, not a string.
- Entry criteria: { "option_type": "P"|"C", "delta_range": [min, max], "dte_range": [min, max], ... }
- Exit criteria: { "conditions": [{"field": "dte"|"unrealized_pnl_pct"|..., "op": "lt"|"gt"|"gte"|"lte", "value": number}], "condition_logic": "any"|"all" }

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
- Exit criteria: { "conditions": [{"field": "dte"|"unrealized_pnl_pct"|..., "op": "lt"|"gt"|"gte"|"lte", "value": number}], "condition_logic": "any"|"all" }

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

## Rules for Synthesis:
- VETOES are binding: if Taleb vetoes a rule, remove it
- AMENDMENTS are suggestions: apply if they improve convexity without breaking margin discipline
- The Spitznagel advisor's sizing limits take precedence (arithmetic discipline)
- Taleb's concerns about fat-tail exposure should be taken seriously
- When advisors agree, high confidence. When they disagree, reduce priority or tighten conditions.

=== ACCOUNT HEALTH ===
${JSON.stringify(accountHealth, null, 2)}
- Current positions: ${positions.length} open
- Spot: $${spotPrice.toFixed(2)}

Output the FINAL trading agenda JSON (same format as Spitznagel's output — with assessment, entry_rules, exit_rules).`
    : `=== PRIMARY ADVISOR AGENDA ===
${JSON.stringify(primaryAgenda, null, 2)}

=== SECOND OPINION ===
Not available (OpenAI key not set or call failed). Validate and pass through the primary agenda.

=== ACCOUNT HEALTH ===
${JSON.stringify(accountHealth, null, 2)}
- Current positions: ${positions.length} open
- Spot: $${spotPrice.toFixed(2)}

Synthesize the final agenda now.`;

  try {
    const synthesisResponse = await axios.post('https://api.anthropic.com/v1/messages', {
      model: 'claude-opus-4-6',
      max_tokens: 2048,
      system: synthesisSystemPrompt,
      messages: [{ role: 'user', content: synthesisUserPrompt }],
    }, {
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      timeout: 60000,
    });

    const synthesisText = synthesisResponse.data?.content?.[0]?.text || '';
    try {
      const synthesized = extractJSON(synthesisText);
      if (synthesized) {
        finalAgenda = synthesized;
        console.log(`📋 Advisory Step 3: synthesized ${finalAgenda.entry_rules?.length || 0} entry rules, ${finalAgenda.exit_rules?.length || 0} exit rules`);
      } else {
        console.log('📋 Advisory Step 3: no JSON in synthesis response, using primary agenda');
      }
    } catch (parseErr) {
      console.log('📋 Advisory Step 3: JSON parse failed, using primary agenda:', parseErr.message);
    }
  } catch (e) {
    console.log('📋 Advisory Step 3 FAILED, using primary agenda:', e.message);
  }

  // ── Persist rules to database ───────────────────────────────────────────────

  const allRules = [];

  // Parse entry rules
  if (finalAgenda.entry_rules && Array.isArray(finalAgenda.entry_rules)) {
    for (const rule of finalAgenda.entry_rules) {
      allRules.push({
        rule_type: 'entry',
        action: rule.action,
        instrument_name: null,
        criteria: rule.criteria,
        budget_limit: rule.budget_limit ?? null,
        priority: rule.priority || 'medium',
        reasoning: rule.reasoning || null,
        advisory_id: advisoryId,
        preferred_order_type: rule.preferred_order_type || null,
      });
    }
  }

  // Parse exit rules
  if (finalAgenda.exit_rules && Array.isArray(finalAgenda.exit_rules)) {
    for (const rule of finalAgenda.exit_rules) {
      allRules.push({
        rule_type: 'exit',
        action: rule.action,
        instrument_name: rule.instrument_name || null,
        criteria: rule.criteria,
        budget_limit: null,
        priority: rule.priority || 'medium',
        reasoning: rule.reasoning || null,
        advisory_id: advisoryId,
        preferred_order_type: rule.preferred_order_type || null,
      });
    }
  }

  // Write rules to database
  if (db && allRules.length > 0) {
    try {
      db.replaceActiveRules(advisoryId, allRules);
      console.log(`📋 Advisory ${advisoryId}: persisted ${allRules.length} rules to database`);
    } catch (e) {
      console.log('📋 Advisory: failed to persist rules:', e.message);
    }
  }

  // Journal the assessment
  const assessmentText = finalAgenda.assessment || primaryAgenda.assessment || 'No assessment produced';
  if (db) {
    try {
      db.insertJournalEntry('advisory', assessmentText);
    } catch (e) {
      console.log('📋 Advisory: failed to journal assessment:', e.message);
    }
  }

  const entryCount = finalAgenda.entry_rules?.length || 0;
  const exitCount = finalAgenda.exit_rules?.length || 0;
  console.log(`📋 Advisory ${advisoryId}: complete — ${entryCount} entry rules, ${exitCount} exit rules`);

  // Track advisory state for price-triggered re-advisory (persisted to survive restarts)
  botData.lastAdvisorySpotPrice = spotPrice;
  botData.lastAdvisoryTimestamp = Date.now();
  persistCycleState();

  return { advisoryId, agenda: finalAgenda, rulesCount: allRules.length };
  } finally {
    _advisoryInFlight = false;
  }
};

// ─── Bot Loop ────────────────────────────────────────────────────────────────

const runBot = async () => {
  try {
  const now = Date.now();

  console.log(' ');
  console.log('─'.repeat(60));
  console.log(`🥱 NO OPERATION RUN`);

  // Get spot price — try CoinGecko first
  let spotPrice = await getSpotPrice();

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

    // Fetch position tickers for exit monitoring
    const positions = await fetchPositions();
    if (positions.length > 0) {
      const posExpiries = [...new Set(positions.map(p => p.instrument_name.split('-')[1]))];
      const missing = posExpiries.filter(exp => !expiryDates.includes(exp));
      if (missing.length > 0) {
        const results = await Promise.all(missing.map(e => fetchTickersByExpiry(e)));
        for (const tickers of results)
          for (const [name, data] of Object.entries(tickers))
            tickerMap[name] = data;
      }
    }

    // ─── OI Collection: fetch ALL expiry tickers for put/call ratio ────────
    if (db && instruments.length > 0) {
      try {
        // Extract ALL unique expiry dates from the full instruments array
        const allExpiryDates = [...new Set(instruments.map(i => i.instrument_name.split('-')[1]))];
        // Find expiries not already fetched in tickerMap
        const missingExpiries = allExpiryDates.filter(exp => !expiryDates.includes(exp));
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
        let putIvSum = 0, putIvCount = 0;
        let callIvSum = 0, callIvCount = 0;

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
            putIvSum += iv;
            putIvCount++;
          } else if (isCall && absDelta >= 0.04 && absDelta <= 0.12) {
            callIvSum += iv;
            callIvCount++;
          }
        }

        const totalOI = putOI + callOI;
        const pcRatio = callOI > 0 ? putOI / callOI : null;
        const avgPutIv = putIvCount > 0 ? putIvSum / putIvCount : null;
        const avgCallIv = callIvCount > 0 ? callIvSum / callIvCount : null;

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

  // Prefer Lyra index price over CoinGecko (more timely, matches options pricing)
  if (Object.keys(tickerMap).length > 0) {
    const firstTicker = Object.values(tickerMap)[0];
    const lyraIndex = Number(firstTicker.I);
    if (lyraIndex > 0) {
      if (spotPrice) {
        console.log(`🔄 Upgrading spot from CoinGecko $${spotPrice.toFixed(2)} → Lyra index $${lyraIndex.toFixed(2)} (Δ${(lyraIndex - spotPrice).toFixed(2)})`);
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
            console.log(`📈 Funding rate: ${(latest.rate * 100).toFixed(4)}% (${latest.exchange} ${latest.symbol})`);
          }
        } catch (e) { console.log('DB: funding rate write failed:', e.message); }
      }

      // Display key findings with error handling
      console.log('⛓ Onchain Analysis Summary:');
      try {
        if (onchainAnalysis.dexLiquidity && onchainAnalysis.dexLiquidity.dexes) {
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
                  const poolLiquidityUSD = pool.liquidityUSD ? `$${(pool.liquidityUSD/1000000).toFixed(1)}M` : 'N/A';
                  console.log(`• ${pool.token0?.symbol || 'Unknown'}/${pool.token1?.symbol || 'Unknown'}: ${poolLiquidityUSD} TVL`);
                });
              }

              // Show top 3 Uniswap V4 pools
              if (dexName === 'uniswap_v4' && dexData.poolDetails && dexData.poolDetails.length > 0) {
                console.log(`🦄 Uniswap V4 Pools:`);
                const topUniswapV4Pools = dexData.poolDetails
                  .sort((a, b) => (b.liquidity || 0) - (a.liquidity || 0))
                  .slice(0, 3);
                topUniswapV4Pools.forEach(pool => {
                  const poolLiquidityUSD = pool.liquidityUSD ? `$${(pool.liquidityUSD/1000000).toFixed(1)}M` : 'N/A';
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
              const currentTotal = flow.currentTotal ? `${flow.currentTotal.toFixed(2)} ETH` : 'N/A';
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
                    const tfTotal = tf.total ? `$${(tf.total/1000000).toFixed(1)}M` : 'N/A';
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
    console.log('⚠️ No spot price available (CoinGecko + Lyra fallback both failed)');
  }

    console.log(`📊 ${Object.keys(tickerMap).length} tickers | ${putCandidates.length} put + ${callCandidates.length} call candidates`);

    // ── Put budget cycle management ───────────────────────────────
    if (spotPrice) {
      try {
        let balances = [];
        try { balances = await fetchCollaterals(); } catch { /* ok */ }
        const ethBal = Number(balances.find(b => b.asset_name === 'ETH')?.amount || 0);
        const usdcBal = Number(balances.find(b => b.asset_name === 'USDC')?.amount || 0);
        botData.ethBalance = ethBal;
        const portfolioValue = ethBal * spotPrice + usdcBal;
        if (portfolioValue > 0) maybeResetPutCycle(portfolioValue);
      } catch (e) { console.log('📋 Cycle check failed:', e.message); }
    }

    // ── LLM-Driven Trading ─────────────────────────────────────────
    try {
      await manageOpenOrders(tickerMap);
      await evaluateTradingRules(positions, instruments, tickerMap, spotPrice);
      await confirmAndExecutePending(instruments, tickerMap, spotPrice);
    } catch (error) {
      console.error('❌ Trading system error:', error.message);
      sendTelegram(`❌ *Trading system error*: ${error.message}`);
    }

    // SQLite: persist options snapshots (candidates only — heatmap/chart data)
    if (db) {
      try {
        const allOptions = [...(putCandidates || []), ...(callCandidates || [])].map(inst => {
          const ticker = tickerMap[inst.instrument_name];
          return ticker ? enrichCandidateFromTicker(inst, ticker, spotPrice) : null;
        }).filter(Boolean);
        if (allOptions.length > 0) {
          db.insertOptionsSnapshotBatch(allOptions, tickTimestamp);
        }
      } catch (e) { console.log('DB: options snapshot write failed:', e.message); }

      // Portfolio P&L snapshot
      try {
        let balances = [];
        try { balances = await fetchCollaterals(); } catch { /* ok */ }
        const usdcBal = Number(balances.find(b => b.asset_name === 'USDC')?.amount || 0);
        const ethBal = Number(balances.find(b => b.asset_name === 'ETH')?.amount || 0);

        const unrealizedPnl = positions.reduce((sum, p) => sum + (Number(p.unrealized_pnl) || 0), 0);
        const realizedData = db.getRealizedPnL();

        // Portfolio value = subaccount_value from Derive API (includes collateral + positions mark-to-market)
        // Fallback to collateral-only if subaccount API unavailable
        let portfolioValue = usdcBal + (ethBal * spotPrice);
        try {
          const sub = await fetchSubaccount();
          if (sub && sub.subaccount_value > 0) {
            portfolioValue = sub.subaccount_value;
          }
        } catch { /* use fallback */ }

        db.insertPortfolioSnapshot({
          timestamp: tickTimestamp,
          spot_price: spotPrice,
          usdc_balance: usdcBal,
          eth_balance: ethBal,
          positions_json: positions.map(p => ({
            instrument: p.instrument_name,
            direction: p.direction,
            amount: p.amount,
            unrealized_pnl: p.unrealized_pnl,
          })),
          total_unrealized_pnl: unrealizedPnl,
          total_realized_pnl: realizedData.net_realized_pnl || 0,
          portfolio_value_usd: portfolioValue,
        });
      } catch (e) { console.log('DB: portfolio snapshot failed:', e.message); }
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
          strategy: {
            mode: 'llm_driven',
            active_rules: db ? db.getActiveRules().length : 0,
          },
          next_check_minutes: checkInterval / (1000 * 60),
        };
        db.insertTick(tickTimestamp, JSON.stringify(tickSummary));
      } catch (e) {
        console.log('DB: tick write failed:', e.message);
      }

      // Price-triggered re-advisory: if price moved >8% since last advisory, recalibrate
      if (process.env.ANTHROPIC_API_KEY && spotPrice && botData.lastAdvisorySpotPrice) {
        const priceMoveRatio = Math.abs(spotPrice - botData.lastAdvisorySpotPrice) / botData.lastAdvisorySpotPrice;
        const minAdvisoryGap = 10 * 60 * 1000; // 10 min minimum between advisories
        if (priceMoveRatio >= ADVISORY_PRICE_MOVE_THRESHOLD && (Date.now() - botData.lastAdvisoryTimestamp) > minAdvisoryGap) {
          const moveDirection = spotPrice > botData.lastAdvisorySpotPrice ? 'up' : 'down';
          console.log(`📋 Price moved ${(priceMoveRatio * 100).toFixed(1)}% ${moveDirection} since last advisory ($${botData.lastAdvisorySpotPrice.toFixed(0)} → $${spotPrice.toFixed(0)}) — forcing re-advisory`);
          generateTradingAdvisory(positions, spotPrice, tickerMap).catch(e => {
            console.log(`📋 Price-triggered advisory failed (non-fatal): ${e.message}`);
          });
        }
      }

      // Auto-generate journal entries every 8 hours
      if (tickSummary && Date.now() - botData.lastJournalGeneration >= JOURNAL_INTERVAL_MS && process.env.ANTHROPIC_API_KEY) {
        // Set timestamp immediately to prevent overlapping ticks from re-triggering
        const prevJournalTs = botData.lastJournalGeneration;
        botData.lastJournalGeneration = Date.now();
        persistCycleState();
        generateJournalEntries(tickSummary, botData).then(async (entries) => {
          console.log('📓 Journal generation succeeded, next in 8h');
          // Ingest journal entries into wiki (non-fatal)
          try { await ingestToWiki(entries); } catch (e) {
            console.log('📚 Wiki ingest failed (non-fatal):', e.message);
          }
          // Lint wiki if enough time has passed (non-fatal)
          try { await lintWiki(); } catch (e) {
            console.log('📚 Wiki lint failed (non-fatal):', e.message);
          }
          // Generate trading advisory alongside journal
          try { await generateTradingAdvisory(positions, spotPrice, tickerMap); }
          catch (e) { console.log('📋 Advisory failed (non-fatal):', e.message); }
          // Extract lessons after successful journal generation (not every tick)
          return extractHypothesisLessons();
        }).catch(e => {
          // Roll back so it retries next tick
          botData.lastJournalGeneration = prevJournalTs;
          persistCycleState();
          console.log('📓 Journal generation failed (will retry next tick):', e.message);
        });
      }

      // Review expired hypotheses each tick (max 3, guarded against overlap)
      if (process.env.ANTHROPIC_API_KEY) {
        reviewExpiredHypotheses().catch(e => {
          console.log('📊 Hypothesis review failed:', e.message);
        });
      }
    }

  botData.lastCheck = now;
  persistCycleState();

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
console.log(`ETH-collateralized. Put budget: ${(PUT_ANNUAL_RATE * 100).toFixed(2)}% of portfolio/yr in ${BOT_CONFIG.PERIOD_DAYS}d cycles. Calls sized by margin.`);
console.log('='.repeat(70));
console.log(' ');
loadData();

// First-boot: generate advisory if no active rules exist
const _bootAdvisory = async () => {
  if (db && db.getActiveRules().length === 0 && process.env.ANTHROPIC_API_KEY) {
    console.log('📋 No active trading rules — generating first advisory...');
    try {
      const positions = await fetchPositions();
      const spotPrice = await getSpotPrice();
      if (spotPrice) {
        const fetchResult = await fetchAndFilterInstruments(spotPrice);
        const expiryDates = [...new Set([...(fetchResult.putCandidates || []), ...(fetchResult.callCandidates || [])].map(i => i.instrument_name.split('-')[1]))];
        const tickerResults = await Promise.all(expiryDates.map(e => fetchTickersByExpiry(e)));
        const tickerMap = {};
        for (const tickers of tickerResults) for (const [name, data] of Object.entries(tickers)) tickerMap[name] = data;
        await generateTradingAdvisory(positions, spotPrice, tickerMap);
      }
    } catch (e) { console.log('📋 First-boot advisory failed (non-fatal):', e.message); }
  }
};
_bootAdvisory();

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
