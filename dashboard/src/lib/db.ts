import Database from 'better-sqlite3';
import path from 'path';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), '..', 'data');
const DB_PATH = path.join(DATA_DIR, 'noop.db');

// Bot constants (must match script.js)
const PUT_BUYING_BASE_FUNDING_LIMIT = 0;
const CALL_SELLING_BASE_FUNDING_LIMIT = 0;
const PERIOD_MS = 10 * 1000 * 60 * 60 * 24; // 10 days
const MEASUREMENT_WINDOW_DAYS = 6.2;

let db: Database.Database | null = null;

function getDb(): Database.Database {
  if (!db) {
    db = new Database(DB_PATH, { readonly: true });
    db.pragma('journal_mode = WAL');
  }
  return db;
}

// ─── Prepared Statements (created once, reused) ─────────────────────────────

let _stmts: ReturnType<typeof prepareAll> | null = null;

function getStmts() {
  if (!_stmts) _stmts = prepareAll(getDb());
  return _stmts;
}

function prepareAll(d: Database.Database) {
  return {
    getStats: d.prepare(`
      SELECT price as last_price, timestamp as last_price_time,
        short_momentum_main as short_momentum, short_momentum_derivative as short_derivative,
        medium_momentum_main as medium_momentum, medium_momentum_derivative as medium_derivative,
        three_day_high, three_day_low, seven_day_high, seven_day_low
      FROM spot_prices ORDER BY timestamp DESC LIMIT 1
    `),

    getLyraSpot: d.prepare(`
      SELECT index_price as lyra_spot FROM options_snapshots
      WHERE index_price IS NOT NULL
      ORDER BY timestamp DESC LIMIT 1
    `),

    getSpotPrices: d.prepare(`
      SELECT * FROM spot_prices
      WHERE timestamp > ?
      ORDER BY timestamp ASC
      LIMIT ?
    `),

    getOptionsHeatmap: d.prepare(`
      SELECT timestamp, option_type, instrument_name, strike, delta, ask_price, bid_price,
        index_price, expiry, ask_delta_value, bid_delta_value,
        mark_price, implied_vol, ask_amount, bid_amount
      FROM options_snapshots
      WHERE timestamp > ?
      ORDER BY timestamp ASC
    `),

    getBestOptionsOverTime: d.prepare(`
      SELECT timestamp,
        MAX(CASE WHEN (option_type = 'P' OR instrument_name LIKE '%-P')
          AND delta <= -0.02 AND delta >= -0.12
          THEN ask_delta_value END) as best_put_value,
        MAX(CASE WHEN (option_type = 'C' OR instrument_name LIKE '%-C')
          AND delta >= 0.04 AND delta <= 0.12
          THEN bid_delta_value END) as best_call_value,
        MAX(index_price) as lyra_spot
      FROM options_snapshots
      WHERE timestamp > ?
      GROUP BY timestamp
      ORDER BY timestamp ASC
    `),

    getLiquidityRawData: d.prepare(`
      SELECT timestamp, raw_data
      FROM onchain_data
      WHERE timestamp > ?
      ORDER BY timestamp ASC
    `),

    getBestScoresAgg: d.prepare(`
      SELECT
        MAX(CASE WHEN option_type = 'P' OR instrument_name LIKE '%-P' THEN ask_delta_value END) as best_put_score,
        MAX(CASE WHEN option_type = 'C' OR instrument_name LIKE '%-C' THEN bid_delta_value END) as best_call_score
      FROM options_snapshots
      WHERE timestamp > ?
    `),

    getBestPutDetail: d.prepare(`
      SELECT instrument_name, delta, ask_price, strike, expiry
      FROM options_snapshots
      WHERE timestamp > ? AND (option_type = 'P' OR instrument_name LIKE '%-P') AND ask_delta_value = ?
      LIMIT 1
    `),

    getBestCallDetail: d.prepare(`
      SELECT instrument_name, delta, bid_price, strike, expiry
      FROM options_snapshots
      WHERE timestamp > ? AND (option_type = 'C' OR instrument_name LIKE '%-C') AND bid_delta_value = ?
      LIMIT 1
    `),

    getRecentTicks: d.prepare(`
      SELECT id, timestamp, summary FROM bot_ticks
      ORDER BY timestamp DESC LIMIT ?
    `),

    getOnchainData: d.prepare(`
      SELECT timestamp, spot_price, liquidity_flow_direction, liquidity_flow_magnitude,
        liquidity_flow_confidence
      FROM onchain_data
      WHERE timestamp > ?
      ORDER BY timestamp DESC
    `),

    getSignals: d.prepare(`
      SELECT id, timestamp, signal_type, details, acted_on
      FROM strategy_signals
      WHERE timestamp > ?
      ORDER BY timestamp DESC
      LIMIT ?
    `),

    getOptionsDistribution: d.prepare(`
      SELECT option_type, COUNT(*) as count,
        AVG(delta) as avg_delta, MIN(delta) as min_delta, MAX(delta) as max_delta,
        AVG(ask_price) as avg_ask, AVG(bid_price) as avg_bid,
        AVG(ask_price - bid_price) as avg_spread, AVG(mark_price) as avg_mark,
        MIN(strike) as min_strike, MAX(strike) as max_strike,
        AVG(ask_delta_value) as avg_ask_dv, AVG(bid_delta_value) as avg_bid_dv
      FROM options_snapshots WHERE timestamp > ? GROUP BY option_type
    `),

    getAvgCallPremium7d: d.prepare(`
      SELECT AVG(bid_price) as avg_premium
      FROM options_snapshots
      WHERE option_type = 'call' AND timestamp > ? AND bid_price > 0
    `),

    getLatestOnchainRawData: d.prepare(`
      SELECT raw_data
      FROM onchain_data
      WHERE timestamp > ?
      ORDER BY timestamp DESC
      LIMIT 1
    `),

    getOnchainWithRawData: d.prepare(`
      SELECT timestamp, spot_price, liquidity_flow_direction, liquidity_flow_magnitude,
        liquidity_flow_confidence, raw_data
      FROM onchain_data
      WHERE timestamp > ?
      ORDER BY timestamp DESC
      LIMIT ?
    `),

    getMarketQualitySummary: d.prepare(`
      SELECT
        option_type,
        COUNT(*) as count,
        AVG(CASE WHEN mark_price > 0 THEN (ask_price - bid_price) / mark_price END) as avg_spread,
        MIN(CASE WHEN mark_price > 0 THEN (ask_price - bid_price) / mark_price END) as min_spread,
        MAX(CASE WHEN mark_price > 0 THEN (ask_price - bid_price) / mark_price END) as max_spread,
        AVG(implied_vol) as avg_iv,
        AVG(ask_amount + bid_amount) as avg_depth,
        SUM(ask_amount + bid_amount) as total_depth
      FROM options_snapshots
      WHERE timestamp = (SELECT MAX(timestamp) FROM options_snapshots WHERE timestamp > ?)
        AND mark_price > 0
        AND ask_price > 0
        AND bid_price > 0
        AND ABS(delta) BETWEEN 0.02 AND 0.12
      GROUP BY option_type
    `),

    getJournalEntries: d.prepare(`
      SELECT id, timestamp, entry_type, content, series_referenced, created_at
      FROM ai_journal
      WHERE timestamp > ?
      ORDER BY timestamp DESC
      LIMIT ?
    `),

    // ─── Hourly Series for Correlation Engine ─────────────────────────────

    getSpotPricesHourly: d.prepare(`
      SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
             AVG(price) as avg_price
      FROM spot_prices
      WHERE timestamp > ?
      GROUP BY hour
      ORDER BY hour ASC
    `),

    getOnchainHourly: d.prepare(`
      WITH hourly_agg AS (
        SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
               AVG(liquidity_flow_magnitude) as avg_magnitude
        FROM onchain_data WHERE timestamp > ?
        GROUP BY hour
      ),
      direction_counts AS (
        SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
               liquidity_flow_direction as direction,
               COUNT(*) as cnt
        FROM onchain_data WHERE timestamp > ?
        GROUP BY hour, liquidity_flow_direction
      ),
      top_direction AS (
        SELECT hour, direction FROM (
          SELECT hour, direction, ROW_NUMBER() OVER (PARTITION BY hour ORDER BY cnt DESC) as rn
          FROM direction_counts
        ) WHERE rn = 1
      )
      SELECT h.hour, h.avg_magnitude, t.direction
      FROM hourly_agg h
      LEFT JOIN top_direction t ON h.hour = t.hour
      ORDER BY h.hour ASC
    `),

    getBestPutDvHourly: d.prepare(`
      SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
             MAX(ask_delta_value) as value
      FROM options_snapshots
      WHERE timestamp > ?
        AND (option_type = 'P' OR instrument_name LIKE '%-P')
        AND delta <= -0.02 AND delta >= -0.12
      GROUP BY hour
      ORDER BY hour ASC
    `),

    getBestCallDvHourly: d.prepare(`
      SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
             MAX(bid_delta_value) as value
      FROM options_snapshots
      WHERE timestamp > ?
        AND (option_type = 'C' OR instrument_name LIKE '%-C')
        AND delta >= 0.04 AND delta <= 0.12
      GROUP BY hour
      ORDER BY hour ASC
    `),

    getOptionsSpreadHourly: d.prepare(`
      SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
             AVG((ask_price - bid_price) / mark_price) as value
      FROM options_snapshots
      WHERE timestamp > ?
        AND ask_price > 0 AND bid_price > 0 AND mark_price > 0
        AND ((delta <= -0.02 AND delta >= -0.12) OR (delta >= 0.04 AND delta <= 0.12))
      GROUP BY hour
      ORDER BY hour ASC
    `),

    getOptionsDepthHourly: d.prepare(`
      SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
             AVG(ask_amount + bid_amount) as value
      FROM options_snapshots
      WHERE timestamp > ?
        AND ((delta <= -0.02 AND delta >= -0.12) OR (delta >= 0.04 AND delta <= 0.12))
      GROUP BY hour
      ORDER BY hour ASC
    `),

    getOpenInterestHourly: d.prepare(`
      SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
             SUM(open_interest) as value
      FROM options_snapshots
      WHERE timestamp > ? AND open_interest IS NOT NULL AND open_interest > 0
      GROUP BY hour
      ORDER BY hour ASC
    `),

    getImpliedVolHourly: d.prepare(`
      SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
             AVG(implied_vol) as value
      FROM options_snapshots
      WHERE timestamp > ? AND implied_vol IS NOT NULL
        AND ((delta <= -0.02 AND delta >= -0.12) OR (delta >= 0.04 AND delta <= 0.12))
      GROUP BY hour
      ORDER BY hour ASC
    `),

    getLocalTrades: d.prepare(`
      SELECT instrument_name, direction, amount, price, timestamp
      FROM trades
      WHERE timestamp > ?
      ORDER BY timestamp DESC
    `),

    getBotState: d.prepare('SELECT * FROM bot_state WHERE id = 1'),

    // ─── Hourly Rollup Queries ──────────────────────────────────────────
    getSpotPricesHourlyRollup: d.prepare(`
      SELECT hour as timestamp, open as price, high, low, close,
        avg_price, short_momentum as short_momentum_main,
        medium_momentum as medium_momentum_main
      FROM spot_prices_hourly WHERE hour > ? ORDER BY hour ASC
    `),

    getBestOptionsHourlyRollup: d.prepare(`
      SELECT hour as timestamp, best_put_dv as best_put_value, best_call_dv as best_call_value
      FROM options_hourly WHERE hour > ? ORDER BY hour ASC
    `),

    getLiquidityHourlyRollup: d.prepare(`
      SELECT hour as timestamp, dex, tvl, volume, tx_count
      FROM onchain_hourly WHERE hour > ? ORDER BY hour ASC
    `),
  };
}

// ─── Public API ─────────────────────────────────────────────────────────────

export function getStats() {
  return getStmts().getStats.get();
}

export function getLyraSpot() {
  return getStmts().getLyraSpot.get() as { lyra_spot: number } | undefined;
}

export function getSpotPrices(since: string, limit = 2000) {
  return getStmts().getSpotPrices.all(since, limit);
}

export function getOptionsHeatmap(since: string) {
  return getStmts().getOptionsHeatmap.all(since);
}

export function getBestOptionsOverTime(since: string) {
  return getStmts().getBestOptionsOverTime.all(since);
}

export function getLiquidityOverTime(since: string) {
  const rows = getStmts().getLiquidityRawData.all(since) as { timestamp: string; raw_data: string }[];

  return rows.map(row => {
    const entry: Record<string, number | string> = { timestamp: row.timestamp };
    try {
      const data = JSON.parse(row.raw_data);
      const dexes = data?.dexLiquidity?.dexes;
      if (dexes) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        for (const [name, dex] of Object.entries(dexes) as [string, any][]) {
          if (!dex.error && dex.totalLiquidity && !isNaN(dex.totalLiquidity)) {
            entry[name] = dex.totalLiquidity;
          }
          if (dex.totalVolume != null && !isNaN(dex.totalVolume)) {
            entry[`${name}_vol`] = dex.totalVolume;
          }
          if (dex.totalTxCount != null && !isNaN(dex.totalTxCount)) {
            entry[`${name}_txCount`] = dex.totalTxCount;
          }
          if (Array.isArray(dex.poolDetails)) {
            const activeSum = dex.poolDetails.reduce((sum: number, p: { activeLiquidity?: string }) => {
              const v = Number(p.activeLiquidity);
              return isNaN(v) ? sum : sum + v;
            }, 0);
            if (activeSum > 0) entry[`${name}_active`] = activeSum;
            const firstPool = dex.poolDetails[0];
            if (firstPool?.feeTier) entry[`${name}_fee`] = firstPool.feeTier;
          }
        }
      }
    } catch { /* skip malformed rows */ }
    return entry;
  }).filter(r => Object.keys(r).length > 1);
}

export function getBestScores() {
  const s = getStmts();
  const since = new Date(Date.now() - MEASUREMENT_WINDOW_DAYS * 24 * 60 * 60 * 1000).toISOString();
  const row = s.getBestScoresAgg.get(since) as { best_put_score: number | null; best_call_score: number | null } | undefined;

  const bestPutDetail = s.getBestPutDetail.get(since, row?.best_put_score ?? 0) as { instrument_name: string; delta: number; ask_price: number; strike: number; expiry: number } | undefined;
  const bestCallDetail = s.getBestCallDetail.get(since, row?.best_call_score ?? 0) as { instrument_name: string; delta: number; bid_price: number; strike: number; expiry: number } | undefined;

  return {
    bestPutScore: row?.best_put_score ?? 0,
    bestCallScore: row?.best_call_score ?? 0,
    windowDays: MEASUREMENT_WINDOW_DAYS,
    bestPutDetail: bestPutDetail ? {
      delta: bestPutDetail.delta,
      price: bestPutDetail.ask_price,
      strike: bestPutDetail.strike,
      expiry: bestPutDetail.expiry,
      instrument: bestPutDetail.instrument_name,
    } : null,
    bestCallDetail: bestCallDetail ? {
      delta: bestCallDetail.delta,
      price: bestCallDetail.bid_price,
      strike: bestCallDetail.strike,
      expiry: bestCallDetail.expiry,
      instrument: bestCallDetail.instrument_name,
    } : null,
  };
}

export function getRecentTicks(limit = 50) {
  return getStmts().getRecentTicks.all(limit);
}

export function getOnchainData(since: string) {
  return getStmts().getOnchainData.all(since);
}

export function getSignals(since: string, limit = 50) {
  return getStmts().getSignals.all(since, limit);
}

export function getOptionsDistribution(since: string) {
  return getStmts().getOptionsDistribution.all(since);
}

export function getAvgCallPremium7d() {
  const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  return getStmts().getAvgCallPremium7d.all(since) as { avg_premium: number | null }[];
}

export function getLatestOnchainRawData(since: string) {
  return getStmts().getLatestOnchainRawData.get(since) as { raw_data: string } | undefined;
}

export function getOnchainWithRawData(since: string, limit = 5) {
  return getStmts().getOnchainWithRawData.all(since, limit) as { timestamp: string; raw_data: string; [key: string]: unknown }[];
}

export function getMarketQualitySummary(since: string) {
  return getStmts().getMarketQualitySummary.all(since) as {
    option_type: string;
    count: number;
    avg_spread: number | null;
    min_spread: number | null;
    max_spread: number | null;
    avg_iv: number | null;
    avg_depth: number | null;
    total_depth: number | null;
  }[];
}

// ─── AI Journal ─────────────────────────────────────────────────────────────

export function getJournalEntries(since: string, limit = 20) {
  try {
    return getStmts().getJournalEntries.all(since, limit);
  } catch {
    return []; // table may not exist yet
  }
}

// ─── Hourly Series for Correlation Engine ───────────────────────────────────

export function getSpotPricesHourly(since: string) {
  return getStmts().getSpotPricesHourly.all(since) as { hour: string; avg_price: number }[];
}

export function getOnchainHourly(since: string) {
  return getStmts().getOnchainHourly.all(since, since) as { hour: string; avg_magnitude: number | null; direction: string | null }[];
}

export function getBestPutDvHourly(since: string) {
  return getStmts().getBestPutDvHourly.all(since) as { hour: string; value: number | null }[];
}

export function getBestCallDvHourly(since: string) {
  return getStmts().getBestCallDvHourly.all(since) as { hour: string; value: number | null }[];
}

export function getOptionsSpreadHourly(since: string) {
  return getStmts().getOptionsSpreadHourly.all(since) as { hour: string; value: number | null }[];
}

export function getOptionsDepthHourly(since: string) {
  return getStmts().getOptionsDepthHourly.all(since) as { hour: string; value: number | null }[];
}

export function getOpenInterestHourly(since: string) {
  return getStmts().getOpenInterestHourly.all(since) as { hour: string; value: number | null }[];
}

export function getImpliedVolHourly(since: string) {
  return getStmts().getImpliedVolHourly.all(since) as { hour: string; value: number | null }[];
}

// ─── Hourly Rollup Public Functions ─────────────────────────────────────────

export function getSpotPricesHourly_rollup(since: string) {
  return getStmts().getSpotPricesHourlyRollup.all(since) as {
    timestamp: string; price: number; high: number; low: number; close: number;
    avg_price: number; short_momentum_main: string | null; medium_momentum_main: string | null;
  }[];
}

export function getBestOptionsHourly_rollup(since: string) {
  return getStmts().getBestOptionsHourlyRollup.all(since) as {
    timestamp: string; best_put_value: number | null; best_call_value: number | null;
  }[];
}

export function getLiquidityHourly_rollup(since: string) {
  const rows = getStmts().getLiquidityHourlyRollup.all(since) as {
    timestamp: string; dex: string; tvl: number; volume: number; tx_count: number;
  }[];

  // Pivot dex rows into { timestamp, dexName: tvl, dexName_vol: volume, ... } shape
  const byHour = new Map<string, Record<string, number | string>>();
  for (const row of rows) {
    if (!byHour.has(row.timestamp)) {
      byHour.set(row.timestamp, { timestamp: row.timestamp });
    }
    const entry = byHour.get(row.timestamp)!;
    if (row.tvl != null && !isNaN(row.tvl)) entry[row.dex] = row.tvl;
    if (row.volume != null && !isNaN(row.volume)) entry[`${row.dex}_vol`] = row.volume;
    if (row.tx_count != null && !isNaN(row.tx_count)) entry[`${row.dex}_txCount`] = row.tx_count;
  }

  return Array.from(byHour.values()).filter(r => Object.keys(r).length > 1);
}

export function getLocalTrades(since: string) {
  try {
    return getStmts().getLocalTrades.all(since) as {
      instrument_name: string; direction: string; amount: number; price: number; timestamp: string;
    }[];
  } catch {
    return []; // table may not exist yet
  }
}

export function getBotBudget() {
  const empty = {
    putTotalBudget: 0, putSpent: 0, putRemaining: 0, putDaysLeft: 0,
    callTotalBudget: 0, callSpent: 0, callRemaining: 0, callDaysLeft: 0,
    cycleDays: PERIOD_MS / (1000 * 60 * 60 * 24),
  };

  try {
    const row = getStmts().getBotState.get() as {
      put_cycle_start: number | null;
      put_net_bought: number;
      put_unspent_buy_limit: number;
      call_cycle_start: number | null;
      call_net_sold: number;
      call_unspent_sell_limit: number;
    } | undefined;

    if (!row) return empty;

    const now = Date.now();

    const putTotalBudget = PUT_BUYING_BASE_FUNDING_LIMIT + (row.put_unspent_buy_limit || 0);
    const putSpent = row.put_net_bought || 0;
    const putRemaining = Math.max(0, putTotalBudget - putSpent);
    const putCycleStart = row.put_cycle_start || now;
    const putCycleElapsed = now - putCycleStart;
    const putDaysLeft = Math.max(0, (PERIOD_MS - putCycleElapsed) / (1000 * 60 * 60 * 24));

    const callTotalBudget = CALL_SELLING_BASE_FUNDING_LIMIT + (row.call_unspent_sell_limit || 0);
    const callSpent = row.call_net_sold || 0;
    const callRemaining = Math.max(0, callTotalBudget - callSpent);
    const callCycleStart = row.call_cycle_start || now;
    const callCycleElapsed = now - callCycleStart;
    const callDaysLeft = Math.max(0, (PERIOD_MS - callCycleElapsed) / (1000 * 60 * 60 * 24));

    return {
      putTotalBudget,
      putSpent,
      putRemaining,
      putDaysLeft: +putDaysLeft.toFixed(1),
      callTotalBudget,
      callSpent,
      callRemaining,
      callDaysLeft: +callDaysLeft.toFixed(1),
      cycleDays: PERIOD_MS / (1000 * 60 * 60 * 24),
    };
  } catch {
    return empty;
  }
}
