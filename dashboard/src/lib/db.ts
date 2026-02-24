import Database from 'better-sqlite3';
import path from 'path';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), '..', 'data');
const DB_PATH = path.join(DATA_DIR, 'noop.db');

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
      (SELECT COUNT(*) FROM positions WHERE status = 'open' AND direction = 'buy') as open_puts,
      (SELECT COUNT(*) FROM positions WHERE status = 'open' AND direction = 'sell') as open_calls,
      (SELECT COUNT(*) FROM positions) as total_positions,
      (SELECT COUNT(*) FROM trades) as total_trades,
      (SELECT price FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as last_price,
      (SELECT timestamp FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as last_price_time,
      (SELECT short_momentum_main FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as short_momentum,
      (SELECT short_momentum_derivative FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as short_derivative,
      (SELECT medium_momentum_main FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as medium_momentum,
      (SELECT medium_momentum_derivative FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as medium_derivative,
      (SELECT three_day_high FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as three_day_high,
      (SELECT three_day_low FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as three_day_low,
      (SELECT seven_day_high FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as seven_day_high,
      (SELECT seven_day_low FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as seven_day_low,
      (SELECT SUM(total_cost) FROM positions WHERE status = 'open' AND direction = 'buy') as open_put_cost,
      (SELECT SUM(total_cost) FROM positions WHERE status = 'open' AND direction = 'sell') as open_call_revenue
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

export function getPositions(status?: string) {
  const d = getDb();
  if (status) {
    return d.prepare('SELECT * FROM positions WHERE status = ? ORDER BY opened_at DESC').all(status);
  }
  return d.prepare('SELECT * FROM positions ORDER BY opened_at DESC').all();
}

export function getTradesForPosition(positionId: number) {
  const d = getDb();
  return d.prepare('SELECT * FROM trades WHERE position_id = ? ORDER BY timestamp ASC').all(positionId);
}

export function getTrades(since: string, limit = 200) {
  const d = getDb();
  return d.prepare(`
    SELECT t.*, p.strike, p.expiry, p.direction as position_direction, p.status as position_status
    FROM trades t
    LEFT JOIN positions p ON t.position_id = p.id
    WHERE t.timestamp > ?
    ORDER BY t.timestamp DESC
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

export function getBestOptionsOverTime(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT timestamp,
      MAX(CASE WHEN option_type = 'P' OR instrument_name LIKE '%-P' THEN ask_delta_value END) as best_put_value,
      MAX(CASE WHEN option_type = 'C' OR instrument_name LIKE '%-C' THEN bid_delta_value END) as best_call_value
    FROM options_snapshots
    WHERE timestamp > ?
    GROUP BY timestamp
    ORDER BY timestamp ASC
  `).all(since);
}

export function getLiquidityOverTime(since: string) {
  const d = getDb();
  return d.prepare(`
    SELECT timestamp, liquidity_flow_magnitude,
      CASE WHEN liquidity_flow_direction = 'inflow' THEN liquidity_flow_magnitude
           WHEN liquidity_flow_direction = 'outflow' THEN -liquidity_flow_magnitude
           ELSE 0 END as signed_liquidity
    FROM onchain_data
    WHERE timestamp > ?
    ORDER BY timestamp ASC
  `).all(since);
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
