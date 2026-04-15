import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), '..', 'data');
const DB_PATH = path.join(DATA_DIR, 'noop.db');

// Bot constants (single source of truth: bot/config.json)
const CONFIG_PATH = process.env.BOT_CONFIG_PATH || path.join(process.cwd(), '..', 'bot', 'config.json');
const BOT_CONFIG = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf-8'));
// Budget is now dynamic (calculated per cycle in bot_state), not static config constants
const PUT_ANNUAL_RATE = BOT_CONFIG.PUT_ANNUAL_RATE || 0.0333;
const PERIOD_MS = BOT_CONFIG.PERIOD_DAYS * 1000 * 60 * 60 * 24;
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
      SELECT * FROM (
        SELECT timestamp, option_type, instrument_name, strike, delta, ask_price, bid_price,
          index_price, expiry, ask_delta_value, bid_delta_value,
          mark_price, implied_vol, ask_amount, bid_amount
        FROM options_snapshots
        WHERE timestamp > ?
          AND (
            ((option_type = 'P' OR instrument_name LIKE '%-P') AND delta <= -0.02 AND delta >= -0.12)
            OR
            ((option_type = 'C' OR instrument_name LIKE '%-C') AND delta >= 0.04 AND delta <= 0.12)
          )
        ORDER BY timestamp DESC
        LIMIT ?
      )
      ORDER BY timestamp ASC
    `),
    getOptionsCoverageAll: d.prepare(`
      SELECT
        MIN(timestamp) as first_timestamp,
        MAX(timestamp) as last_timestamp,
        COUNT(*) as total_rows
      FROM options_snapshots
    `),
    getOptionsCoverageSince: d.prepare(`
      SELECT
        MIN(timestamp) as first_timestamp,
        MAX(timestamp) as last_timestamp,
        COUNT(*) as total_rows
      FROM options_snapshots
      WHERE timestamp >= ?
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
        MAX(CASE WHEN (option_type = 'P' OR instrument_name LIKE '%-P')
          AND delta <= -0.02 AND delta >= -0.12
          THEN ask_delta_value END) as best_put_score,
        MAX(CASE WHEN (option_type = 'C' OR instrument_name LIKE '%-C')
          AND delta >= 0.04 AND delta <= 0.12
          THEN bid_delta_value END) as best_call_score
      FROM options_snapshots
      WHERE timestamp > ?
    `),

    getBestPutDetail: d.prepare(`
      SELECT instrument_name, delta, ask_price, strike, expiry
      FROM options_snapshots
      WHERE timestamp > ?
        AND (option_type = 'P' OR instrument_name LIKE '%-P')
        AND delta <= -0.02 AND delta >= -0.12
        AND ask_delta_value = ?
      LIMIT 1
    `),

    getBestCallDetail: d.prepare(`
      SELECT instrument_name, delta, bid_price, strike, expiry
      FROM options_snapshots
      WHERE timestamp > ?
        AND (option_type = 'C' OR instrument_name LIKE '%-C')
        AND delta >= 0.04 AND delta <= 0.12
        AND bid_delta_value = ?
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
      SELECT id, timestamp, entry_type, content, series_referenced, created_at,
        prediction_deadline, outcome_status, outcome_verdict, outcome_confidence,
        trade_pnl_attribution, trades_in_window
      FROM ai_journal
      WHERE timestamp > ?
      ORDER BY timestamp DESC
      LIMIT ?
    `),

    getHypothesisStats: d.prepare(`
      SELECT
        COUNT(*) as total,
        SUM(CASE WHEN outcome_status = 'confirmed_convex' THEN 1 ELSE 0 END) as confirmed_convex,
        SUM(CASE WHEN outcome_status = 'confirmed_linear' THEN 1 ELSE 0 END) as confirmed_linear,
        SUM(CASE WHEN outcome_status = 'disproven_bounded' THEN 1 ELSE 0 END) as disproven_bounded,
        SUM(CASE WHEN outcome_status = 'disproven_costly' THEN 1 ELSE 0 END) as disproven_costly,
        SUM(CASE WHEN outcome_status = 'partially_confirmed' THEN 1 ELSE 0 END) as partially_confirmed,
        SUM(CASE WHEN outcome_status = 'pending' THEN 1 ELSE 0 END) as pending
      FROM ai_journal
      WHERE entry_type = 'hypothesis'
        AND timestamp > ?
    `),

    getActiveLessons: d.prepare(`
      SELECT id, lesson, evidence_count, created_at
      FROM hypothesis_lessons
      WHERE is_active = 1
      ORDER BY created_at DESC
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

    // ─── Market Sentiment Queries ───────────────────────────────────────
    getFundingRates: d.prepare(`
      SELECT timestamp, exchange, symbol, rate
      FROM funding_rates
      WHERE timestamp > ? AND symbol = ?
      ORDER BY timestamp ASC
    `),

    getFundingRatesLatest: d.prepare(`
      SELECT rate, timestamp FROM funding_rates
      WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1
    `),

    getFundingRatesHourly: d.prepare(`
      SELECT hour as timestamp, avg_rate as rate
      FROM funding_rates_hourly
      WHERE hour > ? AND symbol = ?
      ORDER BY hour ASC
    `),

    getFundingRateAvg24h: d.prepare(`
      SELECT AVG(rate) as avg_rate FROM funding_rates
      WHERE symbol = ? AND timestamp > ?
    `),

    getOptionsSkew: d.prepare(`
      SELECT timestamp, avg_put_iv, avg_call_iv
      FROM oi_snapshots
      WHERE timestamp > ?
        AND (avg_put_iv IS NOT NULL OR avg_call_iv IS NOT NULL)
      ORDER BY timestamp ASC
    `),

    getAggregateOI: d.prepare(`
      SELECT timestamp, SUM(open_interest) as total_oi
      FROM options_snapshots
      WHERE timestamp > ? AND open_interest IS NOT NULL AND open_interest > 0
      GROUP BY timestamp
      ORDER BY timestamp ASC
    `),

    getOISnapshots: d.prepare(`
      SELECT timestamp, put_oi, call_oi, near_put_oi, near_call_oi,
        far_put_oi, far_call_oi, total_oi, pc_ratio, expiry_count,
        avg_put_iv, avg_call_iv
      FROM oi_snapshots
      WHERE timestamp > ?
      ORDER BY timestamp ASC
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

    // ─── Trading Ops Queries ────────────────────────────────────────────
    getActiveTradingRules: d.prepare(`
      SELECT id, rule_type, action, instrument_name, criteria, budget_limit,
        priority, reasoning, created_at, advisory_id
      FROM trading_rules WHERE is_active = 1
      ORDER BY priority DESC, id ASC
    `),

    getRecentPendingActions: d.prepare(`
      SELECT pa.id, pa.rule_id, pa.action, pa.instrument_name, pa.amount, pa.price,
        pa.trigger_details, pa.status, pa.retries, pa.triggered_at,
        pa.confirmation_reasoning, pa.confirmed_at, pa.executed_at, pa.execution_result,
        tr.reasoning as rule_reasoning, tr.priority as rule_priority, tr.criteria as rule_criteria
      FROM pending_actions pa
      LEFT JOIN trading_rules tr ON pa.rule_id = tr.id
      ORDER BY pa.triggered_at DESC
      LIMIT ?
    `),

    getRecentOrders: d.prepare(`
      SELECT id, timestamp, action, success, reason, instrument_name,
        strike, expiry, delta, price, intended_amount, filled_amount,
        fill_price, total_value, spot_price
      FROM orders ORDER BY timestamp DESC LIMIT ?
    `),

    getOpsStats: d.prepare(`
      SELECT
        (SELECT COUNT(*) FROM trading_rules WHERE is_active = 1) as active_rules,
        (SELECT COUNT(*) FROM pending_actions WHERE status = 'pending') as pending_count,
        (SELECT COUNT(*) FROM pending_actions WHERE status = 'confirmed') as confirmed_count,
        (SELECT COUNT(*) FROM pending_actions WHERE status = 'executed') as executed_count,
        (SELECT COUNT(*) FROM pending_actions WHERE status = 'rejected') as rejected_count,
        (SELECT COUNT(*) FROM pending_actions WHERE status = 'failed') as failed_count,
        (SELECT COUNT(*) FROM orders WHERE success = 1 AND timestamp > datetime('now', '-24 hours')) as orders_24h,
        (SELECT advisory_id FROM trading_rules WHERE is_active = 1 LIMIT 1) as current_advisory_id,
        (SELECT created_at FROM trading_rules WHERE is_active = 1 ORDER BY id ASC LIMIT 1) as advisory_created_at
    `),

    getBudgetCycleState: d.prepare(`
      SELECT put_cycle_start, put_net_bought, put_unspent_buy_limit,
        put_budget_for_cycle, last_advisory_spot_price, last_advisory_timestamp
      FROM bot_state WHERE id = 1
    `),

    getLatestAdvisoryAssessment: d.prepare(`
      SELECT content, timestamp FROM ai_journal
      WHERE entry_type = 'advisory'
      ORDER BY timestamp DESC LIMIT 1
    `),

    getRecentTradeReviews: d.prepare(`
      SELECT id, instrument_name, action_family, opened_at, closed_at, review_window_days, horizon_end_at, order_ids,
        review_status, review_confidence, summary, lessons, pnl_realized,
        premium_opened, premium_closed, spot_open, spot_close,
        spot_min_while_open, spot_max_while_open, spot_min_after_close, spot_max_after_close,
        created_at
      FROM trade_reviews
      WHERE is_active = 1
      ORDER BY closed_at DESC
      LIMIT ?
    `),

    getActiveTradeLessons: d.prepare(`
      SELECT id, lesson, evidence_count, created_at
      FROM trade_lessons
      WHERE is_active = 1
      ORDER BY created_at DESC
    `),

    // Portfolio P&L
    getPortfolioHistory: d.prepare(`
      SELECT timestamp, spot_price, usdc_balance, eth_balance,
        total_unrealized_pnl, total_realized_pnl, portfolio_value_usd
      FROM portfolio_snapshots
      WHERE timestamp > ?
      ORDER BY timestamp ASC
    `),
    getPortfolioSnapshotsInRange: d.prepare(`
      SELECT timestamp, spot_price, usdc_balance, eth_balance,
        total_unrealized_pnl, total_realized_pnl, portfolio_value_usd
      FROM portfolio_snapshots
      WHERE timestamp >= ? AND timestamp <= ?
      ORDER BY timestamp ASC
    `),
    getPortfolioSnapshotBefore: d.prepare(`
      SELECT timestamp, spot_price, usdc_balance, eth_balance,
        total_unrealized_pnl, total_realized_pnl, portfolio_value_usd
      FROM portfolio_snapshots
      WHERE timestamp < ?
      ORDER BY timestamp DESC
      LIMIT 1
    `),
    getLatestPortfolioSnapshot: d.prepare(`
      SELECT * FROM portfolio_snapshots ORDER BY id DESC LIMIT 1
    `),
    getOrdersInRange: d.prepare(`
      SELECT id, timestamp, action, success, reason, instrument_name,
        strike, expiry, delta, price, intended_amount, filled_amount,
        fill_price, total_value, spot_price
      FROM orders
      WHERE timestamp >= ? AND timestamp <= ?
      ORDER BY timestamp ASC
    `),
    getRealizedPnL: d.prepare(`
      SELECT
        COALESCE(SUM(CASE WHEN action IN ('sell_put','buyback_call') AND success = 1 THEN total_value ELSE 0 END), 0)
        - COALESCE(SUM(CASE WHEN action IN ('buy_put','sell_call') AND success = 1 THEN total_value ELSE 0 END), 0)
        as net_realized_pnl,
        COALESCE(SUM(CASE WHEN action = 'buy_put' AND success = 1 THEN total_value ELSE 0 END), 0) as total_put_cost,
        COALESCE(SUM(CASE WHEN action = 'sell_put' AND success = 1 THEN total_value ELSE 0 END), 0) as total_put_revenue,
        COALESCE(SUM(CASE WHEN action = 'sell_call' AND success = 1 THEN total_value ELSE 0 END), 0) as total_call_revenue,
        COALESCE(SUM(CASE WHEN action = 'buyback_call' AND success = 1 THEN total_value ELSE 0 END), 0) as total_call_cost,
        COUNT(CASE WHEN success = 1 THEN 1 END) as successful_orders,
        COUNT(*) as total_orders
      FROM orders
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

export function getOptionsHeatmap(since: string, limit = 12000, bucketMs = 0) {
  if (!(bucketMs > 0)) {
    return getStmts().getOptionsHeatmap.all(since, limit);
  }
  const bucketSeconds = Math.max(1, Math.floor(bucketMs / 1000));
  return getDb().prepare(`
    WITH bucketed AS (
      SELECT
        timestamp,
        option_type,
        instrument_name,
        strike,
        delta,
        ask_price,
        bid_price,
        index_price,
        expiry,
        ask_delta_value,
        bid_delta_value,
        mark_price,
        implied_vol,
        ask_amount,
        bid_amount,
        (CAST(strftime('%s', timestamp) AS INTEGER) / @bucket_seconds) * @bucket_seconds AS bucket_epoch,
        ROW_NUMBER() OVER (
          PARTITION BY instrument_name, (CAST(strftime('%s', timestamp) AS INTEGER) / @bucket_seconds)
          ORDER BY timestamp DESC
        ) AS rn
      FROM options_snapshots
      WHERE timestamp > @since
        AND (
          ((option_type = 'P' OR instrument_name LIKE '%-P') AND delta <= -0.02 AND delta >= -0.12)
          OR
          ((option_type = 'C' OR instrument_name LIKE '%-C') AND delta >= 0.04 AND delta <= 0.12)
        )
    ),
    normalized AS (
      SELECT
        datetime(bucket_epoch, 'unixepoch') || 'Z' AS timestamp,
        option_type,
        instrument_name,
        strike,
        delta,
        ask_price,
        bid_price,
        index_price,
        expiry,
        ask_delta_value,
        bid_delta_value,
        mark_price,
        implied_vol,
        ask_amount,
        bid_amount
      FROM bucketed
      WHERE rn = 1
    )
    SELECT
      timestamp,
      option_type,
      instrument_name,
      strike,
      delta,
      ask_price,
      bid_price,
      index_price,
      expiry,
      ask_delta_value,
      bid_delta_value,
      mark_price,
      implied_vol,
      ask_amount,
      bid_amount
    FROM normalized
    ORDER BY timestamp ASC, instrument_name ASC
  `).all({
    since,
    bucket_seconds: bucketSeconds,
  });
}

export function getOptionsCoverage(since: string) {
  const all = getStmts().getOptionsCoverageAll.get() as {
    first_timestamp: string | null;
    last_timestamp: string | null;
    total_rows: number;
  } | undefined;
  const window = getStmts().getOptionsCoverageSince.get(since) as {
    first_timestamp: string | null;
    last_timestamp: string | null;
    total_rows: number;
  } | undefined;
  return {
    firstTimestamp: all?.first_timestamp ?? null,
    lastTimestamp: all?.last_timestamp ?? null,
    totalRows: all?.total_rows ?? 0,
    firstInRange: window?.first_timestamp ?? null,
    lastInRange: window?.last_timestamp ?? null,
    rowsInRange: window?.total_rows ?? 0,
  };
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

export function getHypothesisStats(since: string) {
  try {
    return getStmts().getHypothesisStats.get(since) as {
      total: number; confirmed_convex: number; confirmed_linear: number;
      disproven_bounded: number; disproven_costly: number;
      partially_confirmed: number; pending: number;
    } | undefined;
  } catch {
    return undefined;
  }
}

export function getActiveLessons() {
  try {
    return getStmts().getActiveLessons.all() as {
      id: number; lesson: string; evidence_count: number; created_at: string;
    }[];
  } catch {
    return [];
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

// ─── Market Sentiment ───────────────────────────────────────────────────────

export function getFundingRates(since: string, symbol = 'ETHUSDT') {
  try {
    return getStmts().getFundingRates.all(since, symbol) as {
      timestamp: string; exchange: string; symbol: string; rate: number;
    }[];
  } catch { return []; }
}

export function getFundingRatesHourlySeries(since: string, symbol = 'ETHUSDT') {
  try {
    return getStmts().getFundingRatesHourly.all(since, symbol) as {
      timestamp: string; rate: number;
    }[];
  } catch { return []; }
}

export function getFundingRateLatest(symbol = 'ETHUSDT') {
  try {
    return getStmts().getFundingRatesLatest.get(symbol) as { rate: number; timestamp: string } | undefined;
  } catch { return undefined; }
}

export function getFundingRateAvg24h(symbol = 'ETHUSDT') {
  try {
    const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    return (getStmts().getFundingRateAvg24h.get(symbol, since) as { avg_rate: number | null })?.avg_rate ?? null;
  } catch { return null; }
}

export function getOptionsSkew(since: string) {
  try {
    return getStmts().getOptionsSkew.all(since) as {
      timestamp: string; avg_put_iv: number | null; avg_call_iv: number | null;
    }[];
  } catch { return []; }
}

export function getAggregateOI(since: string) {
  try {
    return getStmts().getAggregateOI.all(since) as {
      timestamp: string; total_oi: number;
    }[];
  } catch { return []; }
}

export function getOISnapshots(since: string) {
  try {
    return getStmts().getOISnapshots.all(since) as {
      timestamp: string; put_oi: number; call_oi: number;
      near_put_oi: number; near_call_oi: number;
      far_put_oi: number; far_call_oi: number;
      total_oi: number; pc_ratio: number | null;
      expiry_count: number;
      avg_put_iv: number | null; avg_call_iv: number | null;
    }[];
  } catch { return []; }
}

export function getOISnapshotsBucketed(since: string, bucketMs: number) {
  if (!(bucketMs > 0)) return getOISnapshots(since);
  const bucketSeconds = Math.max(1, Math.floor(bucketMs / 1000));
  try {
    return getDb().prepare(`
      WITH bucketed AS (
        SELECT
          timestamp,
          put_oi,
          call_oi,
          near_put_oi,
          near_call_oi,
          far_put_oi,
          far_call_oi,
          total_oi,
          pc_ratio,
          expiry_count,
          avg_put_iv,
          avg_call_iv,
          (CAST(strftime('%s', timestamp) AS INTEGER) / @bucket_seconds) * @bucket_seconds AS bucket_epoch,
          ROW_NUMBER() OVER (
            PARTITION BY (CAST(strftime('%s', timestamp) AS INTEGER) / @bucket_seconds)
            ORDER BY timestamp DESC
          ) AS rn
        FROM oi_snapshots
        WHERE timestamp > @since
      )
      SELECT
        datetime(bucket_epoch, 'unixepoch') || 'Z' AS timestamp,
        put_oi,
        call_oi,
        near_put_oi,
        near_call_oi,
        far_put_oi,
        far_call_oi,
        total_oi,
        pc_ratio,
        expiry_count,
        avg_put_iv,
        avg_call_iv
      FROM bucketed
      WHERE rn = 1
      ORDER BY timestamp ASC
    `).all({ since, bucket_seconds: bucketSeconds }) as {
      timestamp: string; put_oi: number; call_oi: number;
      near_put_oi: number; near_call_oi: number;
      far_put_oi: number; far_call_oi: number;
      total_oi: number; pc_ratio: number | null;
      expiry_count: number;
      avg_put_iv: number | null; avg_call_iv: number | null;
    }[];
  } catch { return []; }
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
      put_budget_for_cycle: number;
      put_net_bought: number;
      put_unspent_buy_limit: number;
      call_cycle_start: number | null;
      call_net_sold: number;
      call_unspent_sell_limit: number;
    } | undefined;

    if (!row) return empty;

    const now = Date.now();

    // Use stored budget, or compute from portfolio value if not yet set
    let cycleBudget = row.put_budget_for_cycle || 0;
    if (cycleBudget === 0) {
      try {
        const snap = getStmts().getLatestPortfolioSnapshot.get() as { portfolio_value_usd: number } | undefined;
        if (snap && snap.portfolio_value_usd > 0) {
          const cyclesPerYear = 365 / (PERIOD_MS / (1000 * 60 * 60 * 24));
          cycleBudget = snap.portfolio_value_usd * PUT_ANNUAL_RATE / cyclesPerYear;
        }
      } catch { /* ok */ }
    }
    const putTotalBudget = cycleBudget + (row.put_unspent_buy_limit || 0);
    const putSpent = row.put_net_bought || 0;
    const putRemaining = Math.max(0, putTotalBudget - putSpent);
    const putCycleStart = row.put_cycle_start || now;
    const putCycleElapsed = now - putCycleStart;
    const putDaysLeft = Math.max(0, (PERIOD_MS - putCycleElapsed) / (1000 * 60 * 60 * 24));

    const callTotalBudget = 0; // calls are margin-sized, no fixed budget
    const callSpent = row.call_net_sold || 0;
    const callRemaining = 0;
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

// ─── Trading Ops ─────────────────────────────────────────────────────────────

export function getActiveTradingRules() {
  try {
    return getStmts().getActiveTradingRules.all() as {
      id: number; rule_type: string; action: string; instrument_name: string | null;
      criteria: string; budget_limit: number | null; priority: string;
      reasoning: string | null; created_at: string; advisory_id: string | null;
    }[];
  } catch { return []; }
}

export function getRecentPendingActions(limit = 30) {
  try {
    return getStmts().getRecentPendingActions.all(limit) as {
      id: number; rule_id: number | null; action: string; instrument_name: string;
      amount: number | null; price: number | null; trigger_details: string | null;
      status: string; retries: number; triggered_at: string;
      confirmation_reasoning: string | null; confirmed_at: string | null;
      executed_at: string | null; execution_result: string | null;
      rule_reasoning: string | null; rule_priority: string | null; rule_criteria: string | null;
    }[];
  } catch { return []; }
}

export function getRecentOrders(limit = 20) {
  try {
    return getStmts().getRecentOrders.all(limit) as {
      id: number; timestamp: string; action: string; success: number;
      reason: string | null; instrument_name: string | null;
      strike: number | null; expiry: string | null; delta: number | null;
      price: number | null; intended_amount: number | null; filled_amount: number | null;
      fill_price: number | null; total_value: number | null; spot_price: number | null;
    }[];
  } catch { return []; }
}

export function getOpsStats() {
  try {
    return getStmts().getOpsStats.get() as {
      active_rules: number; pending_count: number; confirmed_count: number;
      executed_count: number; rejected_count: number; failed_count: number;
      orders_24h: number; current_advisory_id: string | null;
      advisory_created_at: string | null;
    } | undefined;
  } catch { return undefined; }
}

export function getLatestAdvisoryAssessment() {
  try {
    return getStmts().getLatestAdvisoryAssessment.get() as {
      content: string; timestamp: string;
    } | undefined;
  } catch { return undefined; }
}

export function getBudgetCycleState() {
  try {
    return getStmts().getBudgetCycleState.get() as {
      put_cycle_start: number | null;
      put_net_bought: number;
      put_unspent_buy_limit: number;
      put_budget_for_cycle: number;
      last_advisory_spot_price: number | null;
      last_advisory_timestamp: number;
    } | undefined;
  } catch { return undefined; }
}

export function getRecentTradeReviews(limit = 20) {
  try {
    return getStmts().getRecentTradeReviews.all(limit) as {
      id: number;
      instrument_name: string;
      action_family: string | null;
      opened_at: string | null;
      closed_at: string;
      review_window_days: number;
      horizon_end_at: string | null;
      order_ids: string | null;
      review_status: string;
      review_confidence: number | null;
      summary: string;
      lessons: string | null;
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
    }[];
  } catch { return []; }
}

export function getActiveTradeLessons() {
  try {
    return getStmts().getActiveTradeLessons.all() as {
      id: number;
      lesson: string;
      evidence_count: number;
      created_at: string;
    }[];
  } catch { return []; }
}

export function getPortfolioHistory(since: string) {
  try {
    return getStmts().getPortfolioHistory.all(since) as {
      timestamp: string; spot_price: number; usdc_balance: number; eth_balance: number;
      total_unrealized_pnl: number; total_realized_pnl: number; portfolio_value_usd: number;
    }[];
  } catch { return []; }
}

export function getPortfolioSnapshotsInRange(from: string, to: string) {
  try {
    return getStmts().getPortfolioSnapshotsInRange.all(from, to) as {
      timestamp: string; spot_price: number; usdc_balance: number; eth_balance: number;
      total_unrealized_pnl: number; total_realized_pnl: number; portfolio_value_usd: number;
    }[];
  } catch { return []; }
}

export function getPortfolioSnapshotBefore(ts: string) {
  try {
    return getStmts().getPortfolioSnapshotBefore.get(ts) as {
      timestamp: string; spot_price: number; usdc_balance: number; eth_balance: number;
      total_unrealized_pnl: number; total_realized_pnl: number; portfolio_value_usd: number;
    } | undefined;
  } catch { return undefined; }
}

export function getLatestPortfolioSnapshot() {
  try {
    return getStmts().getLatestPortfolioSnapshot.get() as {
      timestamp: string; spot_price: number; usdc_balance: number; eth_balance: number;
      total_unrealized_pnl: number; total_realized_pnl: number; portfolio_value_usd: number;
      positions_json: string;
    } | undefined;
  } catch { return undefined; }
}

export function getOrdersInRange(from: string, to: string) {
  try {
    return getStmts().getOrdersInRange.all(from, to) as {
      id: number; timestamp: string; action: string; success: number;
      reason: string | null; instrument_name: string | null;
      strike: number | null; expiry: string | null; delta: number | null;
      price: number | null; intended_amount: number | null; filled_amount: number | null;
      fill_price: number | null; total_value: number | null; spot_price: number | null;
    }[];
  } catch { return []; }
}

export function getRealizedPnL() {
  try {
    return getStmts().getRealizedPnL.get() as {
      net_realized_pnl: number; total_put_cost: number; total_put_revenue: number;
      total_call_revenue: number; total_call_cost: number;
      successful_orders: number; total_orders: number;
    } | undefined;
  } catch { return undefined; }
}
