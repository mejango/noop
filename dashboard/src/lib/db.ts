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

export function getStats() {
  const d = getDb();
  return d.prepare(`
    SELECT
      (SELECT price FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as last_price,
      (SELECT timestamp FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as last_price_time,
      (SELECT short_momentum_main FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as short_momentum,
      (SELECT short_momentum_derivative FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as short_derivative,
      (SELECT medium_momentum_main FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as medium_momentum,
      (SELECT medium_momentum_derivative FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as medium_derivative,
      (SELECT three_day_high FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as three_day_high,
      (SELECT three_day_low FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as three_day_low,
      (SELECT seven_day_high FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as seven_day_high,
      (SELECT seven_day_low FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as seven_day_low
  `).get();
}

export function getSpotPrices(since: string, limit = 2000) {
  const d = getDb();
  return d.prepare(`
    SELECT * FROM spot_prices
    WHERE timestamp > ?
    ORDER BY timestamp ASC
    LIMIT ?
  `).all(since, limit);
}


export function getOptionsHeatmap(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT timestamp, option_type, instrument_name, strike, delta, ask_price, bid_price,
      index_price, expiry, ask_delta_value, bid_delta_value,
      mark_price, implied_vol, ask_amount, bid_amount
    FROM options_snapshots
    WHERE timestamp > ?
    ORDER BY timestamp ASC
  `).all(since);
}

export function getBestOptionsOverTime(since: string) {
  const d = getDb();
  // Only consider options within the bot's trading delta range
  // PUTs: delta between -0.12 and -0.02, CALLs: delta between 0.04 and 0.12
  return d.prepare(`
    SELECT timestamp,
      MAX(CASE WHEN (option_type = 'P' OR instrument_name LIKE '%-P')
        AND delta <= -0.02 AND delta >= -0.12
        THEN ask_delta_value END) as best_put_value,
      MAX(CASE WHEN (option_type = 'C' OR instrument_name LIKE '%-C')
        AND delta >= 0.04 AND delta <= 0.12
        THEN bid_delta_value END) as best_call_value
    FROM options_snapshots
    WHERE timestamp > ?
    GROUP BY timestamp
    ORDER BY timestamp ASC
  `).all(since);
}

export function getLiquidityOverTime(since: string) {
  const d = getDb();
  const rows = d.prepare(`
    SELECT timestamp, raw_data
    FROM onchain_data
    WHERE timestamp > ?
    ORDER BY timestamp ASC
  `).all(since) as { timestamp: string; raw_data: string }[];

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
            // Sum active liquidity across pools (store as string for bigint safety)
            const activeSum = dex.poolDetails.reduce((sum: number, p: { activeLiquidity?: string }) => {
              const v = Number(p.activeLiquidity);
              return isNaN(v) ? sum : sum + v;
            }, 0);
            if (activeSum > 0) entry[`${name}_active`] = activeSum;
            // Store first pool's fee tier for tooltip display
            const firstPool = dex.poolDetails[0];
            if (firstPool?.feeTier) entry[`${name}_fee`] = firstPool.feeTier;
          }
        }
      }
    } catch { /* skip malformed rows */ }
    return entry;
  }).filter(r => Object.keys(r).length > 1); // must have at least one dex value
}

export function getBestScores() {
  const d = getDb();
  const since = new Date(Date.now() - MEASUREMENT_WINDOW_DAYS * 24 * 60 * 60 * 1000).toISOString();
  const row = d.prepare(`
    SELECT
      MAX(CASE WHEN option_type = 'P' OR instrument_name LIKE '%-P' THEN ask_delta_value END) as best_put_score,
      MAX(CASE WHEN option_type = 'C' OR instrument_name LIKE '%-C' THEN bid_delta_value END) as best_call_score
    FROM options_snapshots
    WHERE timestamp > ?
  `).get(since) as { best_put_score: number | null; best_call_score: number | null } | undefined;

  // Fetch detail rows for best put and best call
  const bestPutDetail = d.prepare(`
    SELECT instrument_name, delta, ask_price, strike, expiry
    FROM options_snapshots
    WHERE timestamp > ? AND (option_type = 'P' OR instrument_name LIKE '%-P') AND ask_delta_value = ?
    LIMIT 1
  `).get(since, row?.best_put_score ?? 0) as { instrument_name: string; delta: number; ask_price: number; strike: number; expiry: number } | undefined;

  const bestCallDetail = d.prepare(`
    SELECT instrument_name, delta, bid_price, strike, expiry
    FROM options_snapshots
    WHERE timestamp > ? AND (option_type = 'C' OR instrument_name LIKE '%-C') AND bid_delta_value = ?
    LIMIT 1
  `).get(since, row?.best_call_score ?? 0) as { instrument_name: string; delta: number; bid_price: number; strike: number; expiry: number } | undefined;

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
  const d = getDb();
  return d.prepare(`
    SELECT id, timestamp, summary FROM bot_ticks
    ORDER BY timestamp DESC LIMIT ?
  `).all(limit);
}

export function getOnchainData(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT timestamp, spot_price, liquidity_flow_direction, liquidity_flow_magnitude,
      liquidity_flow_confidence
    FROM onchain_data
    WHERE timestamp > ?
    ORDER BY timestamp DESC
  `).all(since);
}

export function getSignals(since: string, limit = 50) {
  const d = getDb();
  return d.prepare(`
    SELECT id, timestamp, signal_type, details, acted_on
    FROM strategy_signals
    WHERE timestamp > ?
    ORDER BY timestamp DESC
    LIMIT ?
  `).all(since, limit);
}

export function getOptionsDistribution(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT option_type, COUNT(*) as count,
      AVG(delta) as avg_delta, MIN(delta) as min_delta, MAX(delta) as max_delta,
      AVG(ask_price) as avg_ask, AVG(bid_price) as avg_bid,
      AVG(ask_price - bid_price) as avg_spread, AVG(mark_price) as avg_mark,
      MIN(strike) as min_strike, MAX(strike) as max_strike,
      AVG(ask_delta_value) as avg_ask_dv, AVG(bid_delta_value) as avg_bid_dv
    FROM options_snapshots WHERE timestamp > ? GROUP BY option_type
  `).all(since);
}

export function getAvgCallPremium7d() {
  const d = getDb();
  const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  return d.prepare(`
    SELECT AVG(bid_price) as avg_premium
    FROM options_snapshots
    WHERE option_type = 'call' AND timestamp > ? AND bid_price > 0
  `).all(since) as { avg_premium: number | null }[];
}

export function getOnchainWithRawData(since: string, limit = 5) {
  const d = getDb();
  return d.prepare(`
    SELECT timestamp, spot_price, liquidity_flow_direction, liquidity_flow_magnitude,
      liquidity_flow_confidence, raw_data
    FROM onchain_data
    WHERE timestamp > ?
    ORDER BY timestamp DESC
    LIMIT ?
  `).all(since, limit) as { timestamp: string; raw_data: string; [key: string]: unknown }[];
}

export function getMarketQualitySummary(since: string) {
  const d = getDb();
  // Aggregate spread, IV, and depth from latest options snapshot batch
  // Only considers instruments within the bot's delta range
  const rows = d.prepare(`
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
  `).all(since) as {
    option_type: string;
    count: number;
    avg_spread: number | null;
    min_spread: number | null;
    max_spread: number | null;
    avg_iv: number | null;
    avg_depth: number | null;
    total_depth: number | null;
  }[];
  return rows;
}

// ─── AI Journal ─────────────────────────────────────────────────────────────

export function getJournalEntries(since: string, limit = 20) {
  const d = getDb();
  try {
    return d.prepare(`
      SELECT id, timestamp, entry_type, content, series_referenced, created_at
      FROM ai_journal
      WHERE timestamp > ?
      ORDER BY timestamp DESC
      LIMIT ?
    `).all(since, limit);
  } catch {
    return []; // table may not exist yet
  }
}

// ─── Hourly Series for Correlation Engine ───────────────────────────────────

export function getSpotPricesHourly(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           AVG(price) as avg_price
    FROM spot_prices
    WHERE timestamp > ?
    GROUP BY hour
    ORDER BY hour ASC
  `).all(since) as { hour: string; avg_price: number }[];
}

export function getOnchainHourly(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           AVG(liquidity_flow_magnitude) as avg_magnitude,
           -- Most common direction in the hour
           (SELECT liquidity_flow_direction FROM onchain_data o2
            WHERE strftime('%Y-%m-%dT%H:00:00Z', o2.timestamp) = strftime('%Y-%m-%dT%H:00:00Z', onchain_data.timestamp)
              AND o2.timestamp > ?
            GROUP BY liquidity_flow_direction
            ORDER BY COUNT(*) DESC LIMIT 1) as direction
    FROM onchain_data
    WHERE timestamp > ?
    GROUP BY hour
    ORDER BY hour ASC
  `).all(since, since) as { hour: string; avg_magnitude: number | null; direction: string | null }[];
}

export function getBestPutDvHourly(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           MAX(ask_delta_value) as value
    FROM options_snapshots
    WHERE timestamp > ?
      AND (option_type = 'P' OR instrument_name LIKE '%-P')
      AND delta <= -0.02 AND delta >= -0.12
    GROUP BY hour
    ORDER BY hour ASC
  `).all(since) as { hour: string; value: number | null }[];
}

export function getBestCallDvHourly(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           MAX(bid_delta_value) as value
    FROM options_snapshots
    WHERE timestamp > ?
      AND (option_type = 'C' OR instrument_name LIKE '%-C')
      AND delta >= 0.04 AND delta <= 0.12
    GROUP BY hour
    ORDER BY hour ASC
  `).all(since) as { hour: string; value: number | null }[];
}

export function getOptionsSpreadHourly(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           AVG((ask_price - bid_price) / mark_price) as value
    FROM options_snapshots
    WHERE timestamp > ?
      AND ask_price > 0 AND bid_price > 0 AND mark_price > 0
      AND ((delta <= -0.02 AND delta >= -0.12) OR (delta >= 0.04 AND delta <= 0.12))
    GROUP BY hour
    ORDER BY hour ASC
  `).all(since) as { hour: string; value: number | null }[];
}

export function getOptionsDepthHourly(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           AVG(ask_amount + bid_amount) as value
    FROM options_snapshots
    WHERE timestamp > ?
      AND ((delta <= -0.02 AND delta >= -0.12) OR (delta >= 0.04 AND delta <= 0.12))
    GROUP BY hour
    ORDER BY hour ASC
  `).all(since) as { hour: string; value: number | null }[];
}

export function getOpenInterestHourly(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           SUM(open_interest) as value
    FROM options_snapshots
    WHERE timestamp > ? AND open_interest IS NOT NULL AND open_interest > 0
    GROUP BY hour
    ORDER BY hour ASC
  `).all(since) as { hour: string; value: number | null }[];
}

export function getImpliedVolHourly(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           AVG(implied_vol) as value
    FROM options_snapshots
    WHERE timestamp > ? AND implied_vol IS NOT NULL
      AND ((delta <= -0.02 AND delta >= -0.12) OR (delta >= 0.04 AND delta <= 0.12))
    GROUP BY hour
    ORDER BY hour ASC
  `).all(since) as { hour: string; value: number | null }[];
}


export function getBotBudget() {
  const empty = {
    putTotalBudget: 0, putSpent: 0, putRemaining: 0, putDaysLeft: 0,
    callTotalBudget: 0, callSpent: 0, callRemaining: 0, callDaysLeft: 0,
    cycleDays: PERIOD_MS / (1000 * 60 * 60 * 24),
  };

  try {
    const d = getDb();
    const row = d.prepare('SELECT * FROM bot_state WHERE id = 1').get() as {
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
