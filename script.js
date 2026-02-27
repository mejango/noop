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
 *     – 50–90 days to expiry
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
 * Budgeting & Risk:
 * -----------------
 *   • Separate budgets for puts ($1200) and calls ($1200)
 *   • Budgets tracked by actual filled amounts from order responses
 *   • Buyback/sellback updates subtract/add correctly from committed budget
 *   • Sizing capped by budget and book liquidity; amount quantized to venue step
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

// Common trading parameters
const PUT_BUYING_BASE_FUNDING_LIMIT = 0;
const CALL_SELLING_BASE_FUNDING_LIMIT = 0;
const SUBACCOUNT_ID = 25923;

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

const PERIOD = 10 * 1000 * 60 * 60 * 24;

// Trading parameters - PUTS
const PUT_EXPIRATION_RANGE = [50, 90];
const PUT_DELTA_RANGE = [-0.12, -0.02]; // Negative delta for puts

// Trading parameters - CALLS  
const CALL_EXPIRATION_RANGE = [5, 9];
const CALL_DELTA_RANGE = [0.04, 0.12]; // Positive delta for calls

// Call buyback thresholds
const CALL_BUYBACK_PROFIT_THRESHOLD = 80; // Minimum profit percentage for automatic call buyback

// Journal auto-generation
const JOURNAL_INTERVAL_MS = 8 * 60 * 60 * 1000; // Every 8 hours
let lastJournalGeneration = 0;

// Common bot state structure
const createBotData = () => {
let botData = {
    lastCheck: 0,
    mediumTermMomentum: { main: 'neutral', derivative: null },
    shortTermMomentum: { main: 'neutral', derivative: null },
    lastSpotPrice: null,
    lastSpotPriceTimestamp: null,
    
    // Put strategy data
    putCycleStart: null,
    putNetBought: 0,
    putUnspentBuyLimit: 0,

    // Call strategy data
    callCycleStart: null,
    callNetSold: 0,
    callUnspentSellLimit: 0,
  };

  return botData;
};

const DEFAULT_AMOUNT_STEP = 0.01;

let botData = createBotData();

const persistCycleState = () => {
  if (!db) return;
  try { db.saveBotState(botData); }
  catch (e) { console.error('Failed to persist cycle state:', e.message); }
};

const getAmountStep = (opt) =>
  Number(opt?.options?.amount_step) || Number(opt?.amount_step) || DEFAULT_AMOUNT_STEP;

const quantizeDown = (x, step) => {
  if (!Number.isFinite(x) || !Number.isFinite(step) || step <= 0) return 0;
  return Math.max(0, Math.floor(x / step) * step);
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
      botData.callCycleStart = state.call_cycle_start;
      botData.callNetSold = state.call_net_sold;
      botData.callUnspentSellLimit = state.call_unspent_sell_limit;
      console.log(`✅ Loaded cycle state from SQLite`);
    }
  } catch (e) {
    console.error('❌ Error loading from SQLite:', e.message);
    console.log('⚠️ Starting with default data due to load error');
  }
};


const analyzePastOptionsData = (days = 6.2) => {
  const now = new Date();
  const cutoffDate = new Date(now.getTime() - (days * 24 * 60 * 60 * 1000));

  if (!db) {
    return { bestPutScore: 0, bestCallScore: 0, totalDataPoints: 0, filteredDataPoints: 0, excludedUpwardPeriods: 0, dateRange: { from: cutoffDate.toISOString(), to: now.toISOString() } };
  }

  const rows = db.getRecentTicks(5000).filter(r => r.timestamp > cutoffDate.toISOString());

  let allOptionsData = [];
  for (const row of rows) {
    try {
      const summary = typeof row.summary === 'string' ? JSON.parse(row.summary) : row.summary;
      allOptionsData.push({
        timestamp: row.timestamp,
        bestPutScore: summary.current_best_put || 0,
        bestCallScore: summary.current_best_call || 0,
        mediumTermMomentum: summary.medium_momentum || null,
        shortTermMomentum: summary.short_momentum || null,
      });
    } catch { /* skip unparseable rows */ }
  }

  const filteredOptionsData = allOptionsData.filter(data => {
    if (!data.mediumTermMomentum && !data.shortTermMomentum) return true;

    const { mainMomentum, shortMainMomentum, shortDerivative } = extractMomentumValues(
      data.mediumTermMomentum, data.shortTermMomentum
    );

    const hasConfidentDowntrend = hasDowntrendWith7DayDownwardSpikeAndShortTermDowntrend(
      data.mediumTermMomentum, data.shortTermMomentum
    );
    const hasStandardEntry = shouldEnterStandard(mainMomentum, shortMainMomentum, shortDerivative);

    return hasConfidentDowntrend || hasStandardEntry;
  });

  let bestPutScore = 0;
  let bestCallScore = 0;
  filteredOptionsData.forEach(data => {
    if (data.bestPutScore > bestPutScore) bestPutScore = data.bestPutScore;
    if (data.bestCallScore > bestCallScore) bestCallScore = data.bestCallScore;
  });

  return {
    bestPutScore,
    bestCallScore,
    totalDataPoints: allOptionsData.length,
    filteredDataPoints: filteredOptionsData.length,
    excludedUpwardPeriods: allOptionsData.length - filteredOptionsData.length,
    dateRange: { from: cutoffDate.toISOString(), to: now.toISOString() },
  };
};

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

// Helper function to determine standard entry conditions
const shouldEnterStandard = (mainMomentum, shortMainMomentum, shortDerivative) => {
  return (
    // Case 1: Short term is downward with steep spike (regardless of medium term)
    (shortMainMomentum === 'downward' && hasSteepWithDownwardSpike(shortDerivative)) ||
    // Case 2: Medium term is not upward AND (short term is not upward OR short term is upward but flat)
    (mainMomentum !== 'upward' && (
      shortMainMomentum !== 'upward' ||
      (shortDerivative && shortDerivative.startsWith('flat'))
    ))
  );
};

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

// Helper function to check for downtrend with 3-day downward spike and short-term downtrend (new entry strategy)
const hasDowntrendWith7DayDownwardSpikeAndShortTermDowntrend = (mediumTermMomentum, shortTermMomentum) => {
  // Check if medium-term momentum is downward
  const mainMomentum = typeof mediumTermMomentum === 'object' ? mediumTermMomentum.main : mediumTermMomentum;
  if (mainMomentum !== 'downward') return false;
  
  // Check if short-term momentum is downward
  const shortMainMomentum = typeof shortTermMomentum === 'object' ? shortTermMomentum.main : shortTermMomentum;
  if (shortMainMomentum !== 'downward') return false;
  
  // Check if there's a 3-day downward spike
  const shortDerivative = typeof shortTermMomentum === 'object' ? shortTermMomentum.derivative : null;
  if (!shortDerivative) return false;
  
  return hasSpike(shortDerivative, '7d_down');
};

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

// Get current spot price from CoinGecko
const getSpotPrice = async () => {
  try {
    const response = await axios.get(`${COINGECKO_API}/simple/price?ids=ethereum&vs_currencies=usd`);
    const spotPrice = response.data.ethereum.usd;
    return spotPrice;
  } catch (error) {
    console.error('Error fetching spot price:', error.message);
    return null;
  }
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
    console.error(`Error fetching tickers for expiry ${expiryDate}:`, error.message);
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
const placeOrder = async (name, amount, direction = 'buy', price, assetAddress, optionSubId, reduceOnly = true) => {
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
        signature_expiry_sec: Math.floor((Date.now() / 1000) + 600), // must be >5min from now
        max_fee: (0.08 * price * amount).toFixed(2).toString(), // Max fee as 8% of limit price
        mmp: direction === 'sell', // Market maker protection during selling
        nonce: parseInt(`${timestamp}${Math.floor(Math.random() * 1000)}`),
        signer: wallet.address,
        order_type: 'limit',
        reduce_only: reduceOnly,
        time_in_force: 'ioc'
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
      console.error(`Error placing limit order for ${name}:`, response.data.error);
      return null;
    }
    console.log(`Order placed successfully:`, response.data);
    return response.data;
  } catch (error) {
    console.error(`Error placing limit order for ${name}:`, error.message);
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
    console.error('Error fetching instruments:', error);
    return { putCandidates: [], callCandidates: [] };
  }
};

const handleBuyingPuts = async (putOptionsWithDetails, historicalData, spotPrice) => {
  if (putOptionsWithDetails.length === 0) {
    return [];
  }

  const now = Date.now();

  // once at startup
  if (!botData.putCycleStart) {
    botData.putCycleStart = now;
    persistCycleState();
  }

  const timeSinceCycleStart = now - botData.putCycleStart;
  const isCommitPhaseOver = timeSinceCycleStart >= PERIOD;
  
  console.log(' ');
  console.log('⛳️ PUT STRATEGY');
  const putTotalBudget = PUT_BUYING_BASE_FUNDING_LIMIT + botData.putUnspentBuyLimit;
  console.log(`💰 PUT Budget: $${putTotalBudget.toFixed(2)} | Bought: $${botData.putNetBought.toFixed(2)} | Available: $${(putTotalBudget - botData.putNetBought).toFixed(2)}`);
  const cycleTimeLeft = Math.max(0, PERIOD - timeSinceCycleStart);
  const cycleDaysLeft = (cycleTimeLeft / (1000 * 60 * 60 * 24)).toFixed(2);
  
  console.log(`⏰ Cycle timing: ${(timeSinceCycleStart / (1000 * 60 * 60 * 24)).toFixed(2)} days elapsed | Cycle ends in: ${cycleDaysLeft} days`);

  // Determine buy decision based on momentum (no more phases)
  const { mainMomentum, shortMainMomentum, shortDerivative } = extractMomentumValues(
    botData.mediumTermMomentum, 
    botData.shortTermMomentum
  );
  
  // Confident downtrend setup: medium-term downtrend + short-term downtrend + 3-day downward spike
  const shouldEnterConfidentDowntrend = hasDowntrendWith7DayDownwardSpikeAndShortTermDowntrend(botData.mediumTermMomentum, botData.shortTermMomentum);
  
  // Standard entry conditions
  const shouldEnterStandardConditions = shouldEnterStandard(mainMomentum, shortMainMomentum, shortDerivative);
  
  // Buy if either confident downtrend OR standard conditions are met
  const shouldBuy = shouldEnterConfidentDowntrend || shouldEnterStandardConditions;
  
  // Log the decision reasoning
  if (shouldEnterConfidentDowntrend) {
    console.log(`🚀 PUT BUYING ALLOWED: Confident downtrend setup`);
  } else if (shouldEnterStandardConditions) {
    console.log(`✅ PUT BUYING ALLOWED: Standard conditions met`);
  } else {
    console.log(`⏸️ PUT BUYING PAUSED: Standard conditions not met`);
  }
  
  // Find best put option from current candidates
  let bestScore = historicalData.bestPutScore;
  console.log(`Current best PUT score is: ${bestScore.toFixed(6)}`);

  // Apply delta range filter to pre-fetched options with error handling
  const validPutOptions = filterValidOptions(putOptionsWithDetails, PUT_DELTA_RANGE[0], PUT_DELTA_RANGE[1]);

  console.log(`✅ Found ${validPutOptions.length} valid PUT options (delta range: ${PUT_DELTA_RANGE[0]} to ${PUT_DELTA_RANGE[1]})`);

  // Sort by score (highest first) and filter by historical best with error handling
  const qualifiedPutOptions = validPutOptions
    .map(option => {
      try {
        if (!option.details || !option.details.askDeltaValue) {
          console.log(`⚠️ Skipping PUT option with missing score: ${option.instrument_name}`);
          return null;
        }
        
        const score = parseFloat(option.details.askDeltaValue);
        if (isNaN(score)) {
          console.log(`⚠️ Skipping PUT option with invalid score: ${option.instrument_name}`);
          return null;
        }
        
        return {
          ...option,
          score: score
        };
      } catch (error) {
        console.log(`⚠️ Error processing PUT option score ${option?.instrument_name || 'unknown'}: ${error.message}`);
        return null;
      }
    })
    .filter(option => {
      if (option === null) return false;
      
      // For confident downtrend setup, accept all valid options regardless of historical best
      if (shouldEnterConfidentDowntrend) {
        return true;
      }
      
      // For standard conditions, only accept options better than historical best
      return option.score > bestScore;
    })
    .sort((a, b) => b.score - a.score);

  // Log all options that didn't meet the historical best score (only for standard conditions)
  if (!shouldEnterConfidentDowntrend) {
    validPutOptions.forEach(option => {
      try {
        const score = parseFloat(option.details?.askDeltaValue || 0);
        if (score && score <= bestScore) {
          const price = option.details?.askPrice || 'N/A';
          console.log(`=> ${option.instrument_name} | Delta: ${option.details.delta} | Price: $${price} | Score: ${score.toFixed(6)}`);
        }
      } catch (error) {
        console.log(`⚠️ Error logging PUT option ${option?.instrument_name || 'unknown'}: ${error.message}`);
      }
    });
  }

  const filterReason = shouldEnterConfidentDowntrend ? 'CONFIDENT DOWNSIDE SETUP (all valid options)' : `better than historical best (${bestScore.toFixed(6)})`;
  logOptionSummary('PUT', qualifiedPutOptions.length, filterReason);
  
  // Buy multiple options within budget constraints
  if (shouldBuy && qualifiedPutOptions.length > 0) {
    const entryReason = shouldEnterConfidentDowntrend ? 'CONFIDENT DOWNSIDE SETUP' : 'STANDARD ENTRY CONDITIONS';
    logEntryDecision('PUT', qualifiedPutOptions.length, entryReason);
    
    const remainingBudget = PUT_BUYING_BASE_FUNDING_LIMIT + botData.putUnspentBuyLimit - botData.putNetBought;
    console.log(`💰 Available budget: $${remainingBudget.toFixed(2)}`);

    // Check if remaining budget is sufficient (>$10)
    for (const option of qualifiedPutOptions) {
      console.log(`🎯 NEW BEST PUT: ${option.instrument_name} | Delta: ${option.details.delta} | Score: ${option.score.toFixed(6)} | Previous best score: ${bestScore.toFixed(6)}`);
        
      // Check if we have budget remaining
      if (remainingBudget <= 10) {
        console.log(`💸 Budget exhausted, skipping remaining options`);
        break;
      }

      const buyReason = shouldEnterConfidentDowntrend ? 'Confident Downside Setup' : 'Historical Best Buy';
      console.log(`💸 BUYING PUT: ${option.instrument_name} | Delta: ${option.details.delta} | Score: ${option.score.toFixed(6)} | Reason: ${buyReason}`);
      const success = await executePutBuyOrder(option, buyReason, spotPrice);
        
      if (success) {
        // Update remaining budget after successful purchase
        const newRemainingBudget = PUT_BUYING_BASE_FUNDING_LIMIT + botData.putUnspentBuyLimit - botData.putNetBought;
        console.log(`💰 Remaining budget after purchase: $${newRemainingBudget.toFixed(2)}`);
      }
    }
  } 

  // Reset budget limits if commit phase is over
  if (isCommitPhaseOver) {
    const totalBudget = PUT_BUYING_BASE_FUNDING_LIMIT + botData.putUnspentBuyLimit;
    const unspentAmount = totalBudget - botData.putNetBought;
    
    if (unspentAmount > 0) {
      botData.putUnspentBuyLimit = unspentAmount;
      console.log(`🔄 Carrying over $${unspentAmount} to next PUT cycle`);
      } else {
      botData.putUnspentBuyLimit = 0;
      console.log(`💯 All PUT spent in this cycle`);
    }
    
    botData.putNetBought = 0;
    botData.putCycleStart = now;
    persistCycleState();
  }

  return validPutOptions;
};

const executeCallSellOrder = async (option, reason, spotPrice) => {
    const sellLimit = CALL_SELLING_BASE_FUNDING_LIMIT + botData.callUnspentSellLimit;
    const remainingSellCapacity = sellLimit - botData.callNetSold; // signed (negative means you've earned extra room)
  
    console.log("💳 Call sell order with", { sellLimit, remainingSellCapacity });
    
    const bidPx = Number(option?.details?.bidPrice);
    const bidAmt = Number(option?.details?.bidAmount);
    if (!Number.isFinite(bidPx) || bidPx <= 0 || !Number.isFinite(bidAmt) || bidAmt <= 0) {
      console.log(`⚠️ Skip ${option.instrument_name}: invalid bid price/amount`);
      return false;
    }
  
    const step = getAmountStep(option);              // uses options.amount_step || 0.01
    const maxByCap = remainingSellCapacity / bidPx;  // can be negative
    const maxOrderAmount = 20;                       // maximum order amount for calls
    const raw = Math.max(0, Math.min(maxByCap, bidAmt, maxOrderAmount));
    const qty = quantizeDown(raw, step);
    if (qty === 0) {
      console.log(`⚠️ Size rounds to 0 (step ${step}), skipping ${option.instrument_name}`);
      return false;
    }
  
    console.log(
      `$$$ ${reason}: ${option.instrument_name} bid=$${bidPx} | step=${step} | ` +
      `sellLimit=$${sellLimit} | callNetSold=$${botData.callNetSold} | remaining=$${remainingSellCapacity.toFixed(4)} | ` +
      `maxByCap=${Math.max(0, maxByCap).toFixed(4)} | book=${bidAmt} | qty=${qty}`
    );
  
    let order;
    try {
      order = await placeOrder(
        option.instrument_name,
        qty.toFixed(2),
        'sell',
        bidPx,
        option.base_asset_address,
        option.base_asset_sub_id,
        false
      );
    } catch (error) {
      console.error(`❌ Error placing CALL sell order for ${option.instrument_name}:`, error.message);
      if (db) db.insertOrder({ action: 'sell_call', success: false, reason: `Order placement error: ${error.message}`, instrument_name: option.instrument_name, spot_price: spotPrice });
      return false;
    }

    if (!order) {
      if (db) db.insertOrder({ action: 'sell_call', success: false, reason: 'Order placement failed', instrument_name: option.instrument_name, spot_price: spotPrice });
      return false;
    }

    // Fill accounting (prefer actual fills)
    let filledAmt = qty, avgPx = bidPx, gross = filledAmt * avgPx;
    if (order.result?.trades?.length) {
      let totAmt = 0, totVal = 0;
      for (const t of order.result.trades) {
        const ta = Number(t.trade_amount), tp = Number(t.trade_price);
        totAmt += ta; totVal += ta * tp;
      }
      if (totAmt > 0) { filledAmt = totAmt; avgPx = totVal / totAmt; gross = totVal; }
    }

    botData.callNetSold += gross; // can go negative later from buybacks (earned capacity)
    persistCycleState();

    if (db) db.insertOrder({
      action: 'sell_call', success: true, reason,
      instrument_name: option.instrument_name, strike: option.option_details.strike,
      expiry: option.option_details.expiry, delta: option.details.delta,
      price: bidPx, intended_amount: qty, filled_amount: filledAmt,
      fill_price: avgPx, total_value: gross, spot_price: spotPrice,
      raw_response: order,
    });

    console.log(`✅ SOLD ${filledAmt} @ $${avgPx} | callNetSold now $${botData.callNetSold.toFixed(4)}`);

    return true;
  };
  
const handleSellingCalls = async (callOptionsWithDetails, historicalData, spotPrice) => {
  if (callOptionsWithDetails.length === 0) {
    return [];
  }

  const now = Date.now();

  // once at startup
  if (!botData.callCycleStart) {
    botData.callCycleStart = now;
    persistCycleState();
  }

  const timeSinceCycleStart = now - botData.callCycleStart;
  const isCommitPhaseOver = timeSinceCycleStart >= PERIOD;


  console.log(' ');
  console.log('📞 CALL STRATEGY');
  const callTotalBudget = CALL_SELLING_BASE_FUNDING_LIMIT + botData.callUnspentSellLimit;
  console.log(`💰 CALL Goal: $${callTotalBudget.toFixed(2)} | Sold: $${botData.callNetSold.toFixed(2)} | Available: $${(callTotalBudget - botData.callNetSold).toFixed(2)}`);
  const callCycleTimeLeft = Math.max(0, PERIOD - timeSinceCycleStart);
  const callCycleDaysLeft = (callCycleTimeLeft / (1000 * 60 * 60 * 24)).toFixed(2);
  
  console.log(`⏰ Cycle timing: ${(timeSinceCycleStart / (1000 * 60 * 60 * 24)).toFixed(2)} days elapsed | Cycle ends in: ${callCycleDaysLeft} days`);

  // Determine sell decision based on momentum (no more phases)
  const { mainMomentum, shortMainMomentum, shortDerivative } = extractMomentumValues(
    botData.mediumTermMomentum, 
    botData.shortTermMomentum
  );
  
  // Confident downtrend setup: medium-term downtrend + short-term downtrend + 3-day downward spike
  const shouldEnterConfidentDowntrend = hasDowntrendWith7DayDownwardSpikeAndShortTermDowntrend(botData.mediumTermMomentum, botData.shortTermMomentum);
  
  // Standard entry conditions
  const shouldEnterStandardConditions = shouldEnterStandard(mainMomentum, shortMainMomentum, shortDerivative);
  
  // Sell if either confident downtrend OR standard conditions are met
  const shouldSell = shouldEnterConfidentDowntrend || shouldEnterStandardConditions;

  // Find best call option from current candidates
  let bestScore = historicalData.bestCallScore;
  console.log(`Current best CALL score is: ${bestScore.toFixed(6)}`);

  // Apply delta range filter to pre-fetched options with error handling
  const validCallOptions = filterValidOptions(callOptionsWithDetails, CALL_DELTA_RANGE[0], CALL_DELTA_RANGE[1]);

  console.log(`✅ Found ${validCallOptions.length} valid CALL options (delta range: ${CALL_DELTA_RANGE[0]} to ${CALL_DELTA_RANGE[1]})`);

  // Sort by score (highest first) and filter by historical best with error handling
  const qualifiedCallOptions = validCallOptions
    .map(option => {
      try {
        if (!option.details || !option.details.bidDeltaValue) {
          console.log(`⚠️ Skipping CALL option with missing score: ${option.instrument_name}`);
          return null;
        }
        
        const score = parseFloat(option.details.bidDeltaValue);
        if (isNaN(score)) {
          console.log(`⚠️ Skipping CALL option with invalid score: ${option.instrument_name}`);
          return null;
        }
        
        return {
          ...option,
          score: score
        };
      } catch (error) {
        console.log(`⚠️ Error processing CALL option score ${option?.instrument_name || 'unknown'}: ${error.message}`);
        return null;
      }
    })
    .filter(option => {
      if (option === null) return false;
      
      // For confident downtrend setup, accept all valid options regardless of historical best
      if (shouldEnterConfidentDowntrend) {
        return true;
      }
      
      // For standard conditions, only accept options better than historical best
      return option.score > bestScore;
    })
    .sort((a, b) => b.score - a.score);

  // Log all options that didn't meet the historical best score (only for standard conditions)
  if (!shouldEnterConfidentDowntrend) {
    validCallOptions.forEach(option => {
      try {
        const score = parseFloat(option.details?.bidDeltaValue || 0);
        if (score && score <= bestScore) {
          const price = option.details?.bidPrice || 'N/A';
          console.log(`=> ${option.instrument_name} | Delta: ${option.details.delta} | Price: $${price} | Score: ${score.toFixed(6)}`);
        }
      } catch (error) {
        console.log(`⚠️ Error logging CALL option ${option?.instrument_name || 'unknown'}: ${error.message}`);
      }
    });
  }

    const filterReason = shouldEnterConfidentDowntrend ? 'CONFIDENT DOWNSIDE SETUP (all valid options)' : `better than historical best (${bestScore.toFixed(6)})`;
  logOptionSummary('CALL', qualifiedCallOptions.length, filterReason);
  
  // Sell multiple options within budget constraints
  if (shouldSell && qualifiedCallOptions.length > 0) {
    const entryReason = shouldEnterConfidentDowntrend ? 'CONFIDENT DOWNSIDE SETUP' : 'STANDARD ENTRY CONDITIONS';
    logEntryDecision('CALL', qualifiedCallOptions.length, entryReason);
    
    const remainingBudget = CALL_SELLING_BASE_FUNDING_LIMIT + botData.callUnspentSellLimit - botData.callNetSold;
    console.log(`💰 Available budget: $${remainingBudget.toFixed(2)}`);

    // Check if remaining budget is sufficient (>$10)
    if (remainingBudget <= 10) {
      console.log(`💸 Insufficient budget for CALL trades ($${remainingBudget.toFixed(2)} remaining, need >$10)`);
    } else {
      for (const option of qualifiedCallOptions) {
        console.log(`📞 NEW BEST CALL: ${option.instrument_name} | Delta: ${option.details.delta} | Score: ${option.score.toFixed(6)} | Previous best score: ${bestScore.toFixed(6)}`);
        
        // Check if we have budget remaining
        if (remainingBudget <= 0) {
          console.log(`💸 Budget exhausted, skipping remaining options`);
          break;
        }

        const sellReason = shouldEnterConfidentDowntrend ? 'Confident Downside Setup' : 'Historical Best Sell';
        console.log(`💰 SELLING CALL: ${option.instrument_name} | Score: ${option.score.toFixed(6)} | Reason: ${sellReason}`);
        const success = await executeCallSellOrder(option, sellReason, spotPrice);
        
        if (success) {
          // Update remaining budget after successful sale
          const newRemainingBudget = CALL_SELLING_BASE_FUNDING_LIMIT + botData.callUnspentSellLimit - botData.callNetSold;
          console.log(`💰 Remaining budget after sale: $${newRemainingBudget.toFixed(2)}`);
        }
      }
    }
  } 

  // Reset budget limits if commit phase is over
  if (isCommitPhaseOver) {
    const totalBudget = CALL_SELLING_BASE_FUNDING_LIMIT + botData.callUnspentSellLimit;
    const unspentAmount = totalBudget - botData.callNetSold;
    
    if (unspentAmount > 0) {
      botData.callUnspentSellLimit = unspentAmount;
      console.log(`🔄 Carrying over $${unspentAmount} to next CALL cycle`);
    } else {
      botData.callUnspentSellLimit = 0;
      console.log(`💯 All CALL budget spent in this cycle`);
    }
    
    botData.callNetSold = 0;
    botData.callCycleStart = now;
    persistCycleState();
  }

  return validCallOptions;
};

const executePutBuyOrder = async (option, reason, spotPrice) => {
    const buyLimit = PUT_BUYING_BASE_FUNDING_LIMIT + botData.putUnspentBuyLimit;
    const remainingBuyCapacity = buyLimit - botData.putNetBought; // signed (negative means you've earned extra room)
  
    console.log("💳 Put buy order with", { buyLimit, remainingBuyCapacity });

    const askPx = Number(option?.details?.askPrice);
    const askAmt = Number(option?.details?.askAmount);
    if (!Number.isFinite(askPx) || askPx <= 0 || !Number.isFinite(askAmt) || askAmt <= 0) {
      console.log(`⚠️ Skip ${option.instrument_name}: invalid ask price/amount`);
      return false;
    }
  
    const step = getAmountStep(option);
    const maxByCap = remainingBuyCapacity / askPx;           // can be negative
    const raw = Math.max(0, Math.min(maxByCap, askAmt));     // clamp to >= 0
    const qty = quantizeDown(raw, step);                     // enforce 0.01 step
    if (qty === 0) {
      console.log(`⚠️ Size rounds to 0 (step ${step}), skipping ${option.instrument_name}`);
      return false;
    }
  
    console.log(
      `$$$ ${reason}: ${option.instrument_name} ask=$${askPx} | step=${step} | ` +
      `buyLimit=$${buyLimit} | putNetBought=$${botData.putNetBought} | remaining=$${remainingBuyCapacity.toFixed(4)} | ` +
      `maxByCap=${Math.max(0, maxByCap).toFixed(4)} | book=${askAmt} | qty=${qty}`
    );
  
    let order;
    try {
      order = await placeOrder(
        option.instrument_name,
        qty.toFixed(2),
        'buy',
        askPx,
        option.base_asset_address,
        option.base_asset_sub_id,
        false
      );
    } catch (error) {
      console.error(`❌ Error placing PUT buy order for ${option.instrument_name}:`, error.message);
      if (db) db.insertOrder({ action: 'buy_put', success: false, reason: `Order placement error: ${error.message}`, instrument_name: option.instrument_name, spot_price: spotPrice });
      return false;
    }

    if (!order) {
      if (db) db.insertOrder({ action: 'buy_put', success: false, reason: 'Order placement failed', instrument_name: option.instrument_name, spot_price: spotPrice });
      return false;
    }

    // Fill accounting (use actual fills if present)
    let filledAmt = qty, avgPx = askPx, cost = filledAmt * avgPx;
    if (order.result?.trades?.length) {
      let totAmt = 0, totVal = 0;
      for (const t of order.result.trades) {
        const ta = Number(t.trade_amount), tp = Number(t.trade_price);
        totAmt += ta; totVal += ta * tp;
      }
      filledAmt = totAmt; avgPx = totVal / totAmt; cost = totVal;
    }

    botData.putNetBought += cost; // stays signed (sellbacks can drive it negative = earned capacity)
    persistCycleState();

    if (db) db.insertOrder({
      action: 'buy_put', success: true, reason,
      instrument_name: option.instrument_name, strike: option.option_details.strike,
      expiry: option.option_details.expiry, delta: option.details.delta,
      price: askPx, intended_amount: qty, filled_amount: filledAmt,
      fill_price: avgPx, total_value: cost, spot_price: spotPrice,
      raw_response: order,
    });

    console.log(`✅ BOUGHT ${filledAmt} @ $${avgPx} | putNetBought now $${botData.putNetBought.toFixed(4)}`);

    return true;
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

    const { buildCorrelationAnalysis } = require('./bot/correlation');
    let correlations = null;
    try { correlations = buildCorrelationAnalysis(db); } catch { /* graceful fallback */ }

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
      budget: {
        putNetBought: botData.putNetBought,
        putUnspentBuyLimit: botData.putUnspentBuyLimit,
        callNetSold: botData.callNetSold,
        callUnspentSellLimit: botData.callUnspentSellLimit,
      },
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
    };

    const systemPrompt = `You are the Spitznagel Bot — a tail-risk hedging advisor operating on ETH options with Universa-style principles. You maintain an analytical journal tracking market observations, hypotheses, and regime assessments.

Analyze the provided snapshot across three time scales:

**Short-term (hours):** Price action, short momentum shifts, spike events, intraday patterns.
**Medium-term (days):** Trend direction changes, momentum regime shifts, onchain flow patterns, protection cost trends.
**Long-term (week+):** Structural patterns, correlation shifts, regime transitions, compounding geometry.

Review your previous journal entries. Confirm patterns that held, revise those that didn't, and contradict past assessments when data warrants it.

Output 2-5 journal entries using these tags:
<journal type="observation">Factual patterns identified in the data</journal>
<journal type="hypothesis">Testable predictions grounded in data</journal>
<journal type="regime_note">Market state assessments and regime classifications</journal>

IMPORTANT: Start every journal entry with a single bold TLDR line summarizing the key takeaway in plain language (e.g., "**TLDR: Put protection costs dropped 15% while ETH consolidated — cheap insurance window.**"). Follow the TLDR with the detailed analysis.

## Put Value / Price Divergence
The snapshot includes a put_price_divergence section that detects when put option values move independently of spot price:
- **current_divergences**: Put value vs price changes over multiple windows. A PUT_SPIKE_PRICE_FLAT signal means the options market is pricing in downside risk before spot moves — puts are getting expensive while price holds. PUT_CHEAP_PRICE_STABLE means cheap protection is available.
- **historical_spike_then_drop_episodes**: Past instances where a put value spike preceded a price drop, with timing data. Use these to calibrate how predictive put spikes are for this market.

This is critical for the Spitznagel strategy: we want to buy puts when they're CHEAP (before the market prices in risk), not after a spike. If put spikes reliably lead price drops, the bot should be accumulating protection during PUT_CHEAP_PRICE_STABLE windows.

Ground everything in the data. Focus on: cost of protection (put pricing), crash probability (flow reversals), and portfolio geometry (spot-options relationship).`;

    const userMessage = `Here is today's daily snapshot for journal analysis:\n\n${JSON.stringify(snapshot, null, 2)}\n\nAnalyze across short, medium, and long time scales. Review previous journal entries and build on, revise, or contradict them as warranted.`;

    const response = await axios.post('https://api.anthropic.com/v1/messages', {
      model: process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-6',
      max_tokens: 2048,
      system: systemPrompt,
      messages: [{ role: 'user', content: userMessage }],
    }, {
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      timeout: 60000,
    });

    const text = response.data?.content?.[0]?.text || '';

    // Extract journal entries (same pattern as chat route)
    const regex = /<journal\s+type="(observation|hypothesis|regime_note)">([\s\S]*?)<\/journal>/g;
    const seriesNames = ['spot_return', 'liquidity_flow', 'best_put_dv', 'best_call_dv', 'options_spread', 'options_depth', 'open_interest', 'implied_vol'];
    let match;
    let count = 0;

    while ((match = regex.exec(text)) !== null) {
      const entryType = match[1];
      const content = match[2].trim();
      if (content) {
        const referenced = seriesNames.filter(s =>
          content.toLowerCase().includes(s.replace(/_/g, ' ')) || content.includes(s)
        );
        db.insertJournalEntry(entryType, content, referenced.length > 0 ? referenced : null);
        count++;
      }
    }

    console.log(`📓 Journal: generated ${count} entries`);
  } catch (e) {
    console.log('📓 Journal generation failed:', e.message);
  }
};

// ─── Bot Loop ────────────────────────────────────────────────────────────────

const runBot = async () => {
  try {
  const now = Date.now();

  console.log(' ');
  console.log('─'.repeat(60));
  console.log(`🥱 NO OPERATION RUN`);

  // Get spot price and update momentum
  const spotPrice = await getSpotPrice();

  // Shared timestamp for all DB writes this tick
  const tickTimestamp = new Date().toISOString();

  let onchainAnalysis = null;

  if (spotPrice) {
    // Display run header
    console.log(`ETH: $${spotPrice?.toFixed(2) || 'N/A'} | ${new Date().toLocaleString()}`);

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
  }

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
  
  // Analyze past 6.2 days of options data once for both strategies
  const historicalData = analyzePastOptionsData(6.2);
  console.log(`👴🏼 Historical analysis (${historicalData.totalDataPoints} total data points from past 6.2 days):`);
  console.log(`   📊 Filtered data points (excluding upward momentum): ${historicalData.filteredDataPoints}`);
  console.log(`   🚫 Excluded upward momentum periods: ${historicalData.excludedUpwardPeriods}`);
  console.log(`   Best PUT score (filtered): ${historicalData.bestPutScore.toFixed(6)}`);
  console.log(`   Best CALL score (filtered): ${historicalData.bestCallScore.toFixed(6)}`);
  console.log(`   3-day high: $${botData.shortTermMomentum?.threeDayHigh?.toFixed(2) || 'N/A'}`);
  console.log(`   3-day low: $${botData.shortTermMomentum?.threeDayLow?.toFixed(2) || 'N/A'}`);
  console.log(`   7-day high: $${botData.shortTermMomentum?.sevenDayHigh?.toFixed(2) || 'N/A'}`);
  console.log(`   7-day low: $${botData.shortTermMomentum?.sevenDayLow?.toFixed(2) || 'N/A'}`);
    
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

    // Enrich candidates from the ticker map
    const putOptionsWithDetails = [];
    const callOptionsWithDetails = [];
    let successfulFetches = 0;
    let failedFetches = 0;

    for (const instrument of putCandidates) {
      const ticker = tickerMap[instrument.instrument_name];
      const enriched = enrichCandidateFromTicker(instrument, ticker, spotPrice);
      if (enriched) {
        putOptionsWithDetails.push(enriched);
        successfulFetches++;
      } else {
        failedFetches++;
      }
    }

    for (const instrument of callCandidates) {
      const ticker = tickerMap[instrument.instrument_name];
      const enriched = enrichCandidateFromTicker(instrument, ticker, spotPrice);
      if (enriched) {
        callOptionsWithDetails.push(enriched);
        successfulFetches++;
      } else {
        failedFetches++;
      }
    }

    console.log(`✅ Successfully enriched ${successfulFetches} options from AMM tickers (${failedFetches} missing)`);

    // Run both strategies with pre-fetched option details
    let processedPutOptions = [];
    let processedCallOptions = [];
    
    try {
      processedPutOptions = await handleBuyingPuts(putOptionsWithDetails, historicalData, spotPrice);
    } catch (error) {
      console.error('❌ Error in handleBuyingPuts:', error.message);
      console.log('⚠️ Continuing with empty PUT results to prevent script exit');
    }
    
    try {
      processedCallOptions = await handleSellingCalls(callOptionsWithDetails, historicalData, spotPrice);
    } catch (error) {
      console.error('❌ Error in handleSellingCalls:', error.message);
      console.log('⚠️ Continuing with empty CALL results to prevent script exit');
    }
    
    // Log best scores from this run for historical analysis
    const bestPutScore = processedPutOptions.length > 0 ? 
      Math.max(...processedPutOptions.map(option => option.details?.askDeltaValue || 0)) : 0;
    
    const bestCallScore = processedCallOptions.length > 0 ?
      Math.max(...processedCallOptions.map(option => option.details?.bidDeltaValue || 0)) : 0;

    // Find the actual best options for detail info
    const bestPutOption = processedPutOptions.length > 0
      ? processedPutOptions.reduce((best, o) => (o.details?.askDeltaValue || 0) > (best.details?.askDeltaValue || 0) ? o : best)
      : null;
    const bestCallOption = processedCallOptions.length > 0
      ? processedCallOptions.reduce((best, o) => (o.details?.bidDeltaValue || 0) > (best.details?.bidDeltaValue || 0) ? o : best)
      : null;

    // SQLite: persist options snapshots
    if (db) {
      try {
        const allOptions = [...(putOptionsWithDetails || []), ...(callOptionsWithDetails || [])];
        if (allOptions.length > 0) {
          db.insertOptionsSnapshotBatch(allOptions, tickTimestamp);
        }
      } catch (e) { console.log('DB: options snapshot write failed:', e.message); }
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
          historical: {
            total_data_points: historicalData.totalDataPoints,
            filtered_data_points: historicalData.filteredDataPoints,
            best_put_score: historicalData.bestPutScore,
            best_call_score: historicalData.bestCallScore,
          },
          strategy: {
            put_valid: processedPutOptions.length,
            call_valid: processedCallOptions.length,
          },
          current_best_put: bestPutScore,
          current_best_call: bestCallScore,
          best_put_detail: bestPutOption ? {
            delta: bestPutOption.details?.delta || null,
            price: bestPutOption.details?.askPrice || null,
            strike: bestPutOption.option_details?.strike || null,
            expiry: bestPutOption.option_details?.expiry || null,
            instrument: bestPutOption.instrument_name || null,
          } : null,
          best_call_detail: bestCallOption ? {
            delta: bestCallOption.details?.delta || null,
            price: bestCallOption.details?.bidPrice || null,
            strike: bestCallOption.option_details?.strike || null,
            expiry: bestCallOption.option_details?.expiry || null,
            instrument: bestCallOption.instrument_name || null,
          } : null,
          next_check_minutes: checkInterval / (1000 * 60),
        };
        db.insertTick(tickTimestamp, JSON.stringify(tickSummary));
      } catch (e) {
        console.log('DB: tick write failed:', e.message);
      }

      // Auto-generate journal entries once per day
      if (tickSummary && Date.now() - lastJournalGeneration >= JOURNAL_INTERVAL_MS && process.env.ANTHROPIC_API_KEY) {
        lastJournalGeneration = Date.now();
        generateJournalEntries(tickSummary, botData).catch(e => {
          console.log('📓 Journal generation failed:', e.message);
        });
      }
    }

  botData.lastCheck = now;

  // Schedule next run
  setTimeout(runBotWithWatchdog, checkInterval);
  
  } catch (error) {
    console.error('Error in bot loop:', error);
    setTimeout(runBotWithWatchdog, 60000); // Retry in 1 minute on error
  }
};

// Global flag to allow graceful exit
let allowExit = false;

// Global error handlers - let PM2 handle restarts
process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Promise Rejection:', reason);
  console.log('📦 Letting PM2 handle restart');
  process.exit(1);
});

process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught Exception:', error.message);
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
console.log(`Every ${PERIOD / (1000 * 60 * 60 * 24)} days, buy $${PUT_BUYING_BASE_FUNDING_LIMIT} worth of cheapest FOTM puts and sell $${CALL_SELLING_BASE_FUNDING_LIMIT} worth of most lucrative OTM calls`);
console.log('='.repeat(70));
console.log(' ');
loadData();
runBotWithWatchdog(); 