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

export function getOptionsSnapshots(since: string, limit = 500) {
  const d = getDb();
  return d.prepare(`
    SELECT * FROM options_snapshots
    WHERE timestamp > ?
    ORDER BY timestamp DESC
    LIMIT ?
  `).all(since, limit);
}

export function getOnchainData(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT id, timestamp, spot_price, liquidity_flow_direction, liquidity_flow_magnitude,
      liquidity_flow_confidence, whale_count, whale_total_txns, exhaustion_score,
      exhaustion_alert_level
    FROM onchain_data
    WHERE timestamp > ?
    ORDER BY timestamp DESC
  `).all(since);
}

export function getSignals(since: string, limit = 100) {
  const d = getDb();
  return d.prepare(`
    SELECT * FROM strategy_signals
    WHERE timestamp > ?
    ORDER BY timestamp DESC
    LIMIT ?
  `).all(since, limit);
}

export function getOptionsHeatmap(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT timestamp, option_type, instrument_name, strike, delta, ask_price, bid_price,
      index_price, expiry, ask_delta_value, bid_delta_value
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
        for (const [name, dex] of Object.entries(dexes) as [string, { totalLiquidity?: number; error?: string }][]) {
          if (!dex.error && dex.totalLiquidity && !isNaN(dex.totalLiquidity)) {
            entry[name] = dex.totalLiquidity;
          }
        }
      }
    } catch { /* skip malformed rows */ }
    return entry;
  }).filter(r => Object.keys(r).length > 1); // must have at least one dex value
}

export function getTradeMarkers(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT t.timestamp, t.direction, t.amount, t.price, t.total_value, t.order_type,
      p.instrument_name, p.strike
    FROM trades t
    LEFT JOIN positions p ON t.position_id = p.id
    WHERE t.timestamp > ?
    ORDER BY t.timestamp ASC
  `).all(since);
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
