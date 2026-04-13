const Database = require('better-sqlite3');
const path = require('path');
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
const DB_PATH = path.join(DATA_DIR, 'noop.db');
const fs = require('fs');

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

const db = new Database(DB_PATH);

// Enable WAL mode for concurrent read/write (bot writes, dashboard reads)
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');

// ─── Schema ───────────────────────────────────────────────────────────────────

db.exec(`
  CREATE TABLE IF NOT EXISTS spot_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    price REAL NOT NULL,
    short_momentum_main TEXT,
    short_momentum_derivative TEXT,
    medium_momentum_main TEXT,
    medium_momentum_derivative TEXT,
    three_day_high REAL,
    three_day_low REAL,
    seven_day_high REAL,
    seven_day_low REAL
  );

  CREATE TABLE IF NOT EXISTS options_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    instrument_name TEXT NOT NULL,
    strike REAL,
    expiry INTEGER,
    option_type TEXT,
    delta REAL,
    ask_price REAL,
    bid_price REAL,
    ask_amount REAL,
    bid_amount REAL,
    mark_price REAL,
    index_price REAL,
    ask_delta_value REAL,
    bid_delta_value REAL
  );

  CREATE TABLE IF NOT EXISTS onchain_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    spot_price REAL,
    liquidity_flow_direction TEXT,
    liquidity_flow_magnitude REAL,
    liquidity_flow_confidence REAL,
    exhaustion_score REAL,
    exhaustion_alert_level TEXT,
    raw_data TEXT
  );

  CREATE TABLE IF NOT EXISTS strategy_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    details TEXT,
    acted_on INTEGER NOT NULL DEFAULT 0
  );

  -- Indices for common queries
  CREATE INDEX IF NOT EXISTS idx_spot_prices_timestamp ON spot_prices(timestamp);
  CREATE INDEX IF NOT EXISTS idx_options_snapshots_timestamp ON options_snapshots(timestamp);
  CREATE INDEX IF NOT EXISTS idx_options_snapshots_instrument ON options_snapshots(instrument_name);
  CREATE INDEX IF NOT EXISTS idx_onchain_data_timestamp ON onchain_data(timestamp);
  CREATE INDEX IF NOT EXISTS idx_strategy_signals_type ON strategy_signals(signal_type);
  CREATE INDEX IF NOT EXISTS idx_strategy_signals_timestamp ON strategy_signals(timestamp);

  CREATE INDEX IF NOT EXISTS idx_options_snapshots_ts_type
    ON options_snapshots(timestamp, option_type);
  CREATE INDEX IF NOT EXISTS idx_options_snapshots_ts_delta
    ON options_snapshots(timestamp, delta);

  -- Hourly rollup tables (Phase 1)
  CREATE TABLE IF NOT EXISTS spot_prices_hourly (
    hour TEXT PRIMARY KEY,
    open REAL, high REAL, low REAL, close REAL,
    avg_price REAL,
    short_momentum TEXT, medium_momentum TEXT,
    count INTEGER
  );

  CREATE TABLE IF NOT EXISTS options_hourly (
    hour TEXT PRIMARY KEY,
    best_put_dv REAL, best_call_dv REAL,
    avg_spread REAL, avg_depth REAL, avg_iv REAL,
    total_oi REAL, count INTEGER
  );

  CREATE TABLE IF NOT EXISTS onchain_hourly (
    hour TEXT NOT NULL,
    dex TEXT NOT NULL,
    tvl REAL, volume REAL, tx_count INTEGER,
    avg_magnitude REAL, direction TEXT,
    PRIMARY KEY (hour, dex)
  );
  CREATE INDEX IF NOT EXISTS idx_onchain_hourly_hour ON onchain_hourly(hour);

  -- LLM-driven trading tables
  CREATE TABLE IF NOT EXISTS trading_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_type TEXT NOT NULL,
    action TEXT NOT NULL,
    instrument_name TEXT,
    criteria TEXT NOT NULL,
    budget_limit REAL,
    priority TEXT DEFAULT 'medium',
    reasoning TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    is_active INTEGER DEFAULT 1,
    advisory_id TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_trading_rules_active ON trading_rules(is_active);
  CREATE INDEX IF NOT EXISTS idx_trading_rules_type ON trading_rules(rule_type, is_active);

  CREATE TABLE IF NOT EXISTS pending_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id INTEGER REFERENCES trading_rules(id),
    action TEXT NOT NULL,
    instrument_name TEXT NOT NULL,
    amount REAL,
    price REAL,
    trigger_details TEXT,
    status TEXT DEFAULT 'pending',
    retries INTEGER DEFAULT 0,
    triggered_at TEXT DEFAULT (datetime('now')),
    confirmation_reasoning TEXT,
    confirmed_at TEXT,
    executed_at TEXT,
    execution_result TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_pending_actions_status ON pending_actions(status);
`);

// Idempotent migrations for new columns
try { db.exec('ALTER TABLE options_snapshots ADD COLUMN open_interest REAL'); } catch {}
try { db.exec('ALTER TABLE options_snapshots ADD COLUMN implied_vol REAL'); } catch {}
try { db.exec('ALTER TABLE oi_snapshots ADD COLUMN avg_put_iv REAL'); } catch {}
try { db.exec('ALTER TABLE oi_snapshots ADD COLUMN avg_call_iv REAL'); } catch {}
try { db.exec('ALTER TABLE trading_rules ADD COLUMN preferred_order_type TEXT'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_advisory_spot_price REAL'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_advisory_timestamp INTEGER NOT NULL DEFAULT 0'); } catch {}

// Resting (GTC/post_only) orders we've placed — for fill reconciliation
db.exec(`
  CREATE TABLE IF NOT EXISTS resting_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL UNIQUE,
    instrument_name TEXT NOT NULL,
    action TEXT NOT NULL,
    direction TEXT NOT NULL,
    amount REAL NOT NULL,
    limit_price REAL NOT NULL,
    placed_at TEXT DEFAULT (datetime('now')),
    filled_amount REAL DEFAULT 0,
    status TEXT DEFAULT 'open'
  );
  CREATE INDEX IF NOT EXISTS idx_resting_orders_status ON resting_orders(status);
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS bot_ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    summary TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_bot_ticks_timestamp ON bot_ticks(timestamp);

  CREATE TABLE IF NOT EXISTS bot_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    put_cycle_start INTEGER,
    put_net_bought REAL NOT NULL DEFAULT 0,
    put_unspent_buy_limit REAL NOT NULL DEFAULT 0,
    put_budget_for_cycle REAL NOT NULL DEFAULT 0,
    call_cycle_start INTEGER,
    call_net_sold REAL NOT NULL DEFAULT 0,
    call_unspent_sell_limit REAL NOT NULL DEFAULT 0,
    last_check INTEGER NOT NULL DEFAULT 0,
    last_journal_generation INTEGER NOT NULL DEFAULT 0,
    last_advisory_spot_price REAL,
    last_advisory_timestamp INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS ai_journal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    entry_type TEXT NOT NULL,
    content TEXT NOT NULL,
    series_referenced TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );
  CREATE INDEX IF NOT EXISTS idx_ai_journal_timestamp ON ai_journal(timestamp);

  CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    action TEXT NOT NULL,
    success INTEGER NOT NULL,
    reason TEXT,
    instrument_name TEXT,
    strike REAL,
    expiry TEXT,
    delta REAL,
    price REAL,
    intended_amount REAL,
    filled_amount REAL,
    fill_price REAL,
    total_value REAL,
    spot_price REAL,
    raw_response TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_orders_timestamp ON orders(timestamp);
`);

// Hypothesis tracking columns (idempotent)
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_check INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_journal_generation INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN put_budget_for_cycle REAL NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN prediction_target TEXT'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN prediction_direction TEXT'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN prediction_value REAL'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN prediction_deadline TEXT'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN falsification_criteria TEXT'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN outcome_status TEXT DEFAULT \'pending\''); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN outcome_verdict TEXT'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN outcome_confidence REAL'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN outcome_reviewed_at TEXT'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN trade_pnl_attribution REAL'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN trades_in_window TEXT'); } catch {}

// Portfolio P&L snapshots — taken each tick to track performance over time
db.exec(`
  CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    spot_price REAL NOT NULL,
    usdc_balance REAL DEFAULT 0,
    eth_balance REAL DEFAULT 0,
    positions_json TEXT,
    total_unrealized_pnl REAL DEFAULT 0,
    total_realized_pnl REAL DEFAULT 0,
    portfolio_value_usd REAL DEFAULT 0
  );
  CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_timestamp ON portfolio_snapshots(timestamp);
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS hypothesis_lessons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lesson TEXT NOT NULL,
    evidence_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    archived_at TEXT,
    is_active INTEGER NOT NULL DEFAULT 1
  );

  CREATE TABLE IF NOT EXISTS funding_rates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    rate REAL NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_funding_rates_timestamp ON funding_rates(timestamp);
  CREATE INDEX IF NOT EXISTS idx_funding_rates_ts_sym ON funding_rates(timestamp, symbol);

  CREATE TABLE IF NOT EXISTS funding_rates_hourly (
    hour TEXT NOT NULL,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    avg_rate REAL,
    count INTEGER,
    PRIMARY KEY (hour, exchange, symbol)
  );

  CREATE TABLE IF NOT EXISTS oi_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    put_oi REAL, call_oi REAL,
    near_put_oi REAL, near_call_oi REAL,
    far_put_oi REAL, far_call_oi REAL,
    total_oi REAL, pc_ratio REAL,
    expiry_count INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_oi_snapshots_timestamp ON oi_snapshots(timestamp);
`);

// ─── Prepared Statements ──────────────────────────────────────────────────────

const stmts = {
  insertSpotPrice: db.prepare(`
    INSERT INTO spot_prices (timestamp, price, short_momentum_main, short_momentum_derivative,
      medium_momentum_main, medium_momentum_derivative, three_day_high, three_day_low,
      seven_day_high, seven_day_low)
    VALUES (@timestamp, @price, @short_momentum_main, @short_momentum_derivative,
      @medium_momentum_main, @medium_momentum_derivative, @three_day_high, @three_day_low,
      @seven_day_high, @seven_day_low)
  `),

  insertOptionsSnapshot: db.prepare(`
    INSERT INTO options_snapshots (timestamp, instrument_name, strike, expiry, option_type,
      delta, ask_price, bid_price, ask_amount, bid_amount, mark_price, index_price,
      ask_delta_value, bid_delta_value, open_interest, implied_vol)
    VALUES (@timestamp, @instrument_name, @strike, @expiry, @option_type,
      @delta, @ask_price, @bid_price, @ask_amount, @bid_amount, @mark_price, @index_price,
      @ask_delta_value, @bid_delta_value, @open_interest, @implied_vol)
  `),

  insertOnchainData: db.prepare(`
    INSERT INTO onchain_data (timestamp, spot_price, liquidity_flow_direction,
      liquidity_flow_magnitude, liquidity_flow_confidence,
      exhaustion_score, exhaustion_alert_level, raw_data)
    VALUES (@timestamp, @spot_price, @liquidity_flow_direction,
      @liquidity_flow_magnitude, @liquidity_flow_confidence,
      @exhaustion_score, @exhaustion_alert_level, @raw_data)
  `),

  insertSignal: db.prepare(`
    INSERT INTO strategy_signals (timestamp, signal_type, details, acted_on)
    VALUES (@timestamp, @signal_type, @details, @acted_on)
  `),

  markSignalActed: db.prepare(`
    UPDATE strategy_signals SET acted_on = 1 WHERE id = @id
  `),

  // Read queries
  getRecentSpotPrices: db.prepare(`
    SELECT * FROM spot_prices WHERE timestamp > @since ORDER BY timestamp DESC
  `),

  getRecentSignals: db.prepare(`
    SELECT * FROM strategy_signals WHERE timestamp > @since ORDER BY timestamp DESC LIMIT @limit
  `),

  getRecentOnchain: db.prepare(`
    SELECT * FROM onchain_data WHERE timestamp > @since ORDER BY timestamp DESC
  `),

  getRecentOptionsSnapshots: db.prepare(`
    SELECT * FROM options_snapshots WHERE timestamp > @since ORDER BY timestamp DESC
  `),

  getOptionsSnapshotsByInstrument: db.prepare(`
    SELECT * FROM options_snapshots
    WHERE instrument_name = @instrument_name AND timestamp > @since
    ORDER BY timestamp DESC
  `),

  getStats: db.prepare(`
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
  `),

  // Bot state persistence
  upsertBotState: db.prepare(`
    INSERT INTO bot_state (id, put_cycle_start, put_net_bought, put_unspent_buy_limit, put_budget_for_cycle,
      call_cycle_start, call_net_sold, call_unspent_sell_limit, last_check, last_journal_generation,
      last_advisory_spot_price, last_advisory_timestamp, updated_at)
    VALUES (1, @put_cycle_start, @put_net_bought, @put_unspent_buy_limit, @put_budget_for_cycle,
      @call_cycle_start, @call_net_sold, @call_unspent_sell_limit, @last_check, @last_journal_generation,
      @last_advisory_spot_price, @last_advisory_timestamp, datetime('now'))
    ON CONFLICT(id) DO UPDATE SET
      put_cycle_start = @put_cycle_start,
      put_net_bought = @put_net_bought,
      put_unspent_buy_limit = @put_unspent_buy_limit,
      put_budget_for_cycle = @put_budget_for_cycle,
      call_cycle_start = @call_cycle_start,
      call_net_sold = @call_net_sold,
      call_unspent_sell_limit = @call_unspent_sell_limit,
      last_check = @last_check,
      last_journal_generation = @last_journal_generation,
      last_advisory_spot_price = @last_advisory_spot_price,
      last_advisory_timestamp = @last_advisory_timestamp,
      updated_at = datetime('now')
  `),

  getBotState: db.prepare(`
    SELECT * FROM bot_state WHERE id = 1
  `),

  getSpotPriceHistory7d: db.prepare(`
    SELECT price, timestamp FROM spot_prices
    WHERE timestamp > @since
    ORDER BY timestamp ASC
  `),

  insertTick: db.prepare(`
    INSERT INTO bot_ticks (timestamp, summary) VALUES (@timestamp, @summary)
  `),

  getRecentTicks: db.prepare(`
      SELECT id, timestamp, summary FROM bot_ticks ORDER BY timestamp DESC LIMIT @limit
  `),

  getBestScoresAgg: db.prepare(`
    SELECT
      MAX(CASE WHEN (option_type = 'P' OR instrument_name LIKE '%-P')
        AND delta <= -0.02 AND delta >= -0.12
        THEN ask_delta_value END) as best_put_score,
      MAX(CASE WHEN (option_type = 'C' OR instrument_name LIKE '%-C')
        AND delta >= 0.04 AND delta <= 0.12
        THEN bid_delta_value END) as best_call_score
    FROM options_snapshots
    WHERE timestamp > @since
  `),

  getBestPutDetail: db.prepare(`
    SELECT instrument_name, delta, ask_price, strike, expiry
    FROM options_snapshots
    WHERE timestamp > @since
      AND (option_type = 'P' OR instrument_name LIKE '%-P')
      AND delta <= -0.02 AND delta >= -0.12
      AND ask_delta_value = @score
    LIMIT 1
  `),

  getBestCallDetail: db.prepare(`
    SELECT instrument_name, delta, bid_price, strike, expiry
    FROM options_snapshots
    WHERE timestamp > @since
      AND (option_type = 'C' OR instrument_name LIKE '%-C')
      AND delta >= 0.04 AND delta <= 0.12
      AND bid_delta_value = @score
    LIMIT 1
  `),

  // AI journal
  insertJournalEntry: db.prepare(`
    INSERT INTO ai_journal (timestamp, entry_type, content, series_referenced)
    VALUES (@timestamp, @entry_type, @content, @series_referenced)
  `),

  getRecentJournalEntries: db.prepare(`
    SELECT id, timestamp, entry_type, content, series_referenced, created_at
    FROM ai_journal
    ORDER BY timestamp DESC
    LIMIT @limit
  `),

  insertJournalEntryFull: db.prepare(`
    INSERT INTO ai_journal (timestamp, entry_type, content, series_referenced,
      prediction_target, prediction_direction, prediction_value, prediction_deadline, falsification_criteria)
    VALUES (@timestamp, @entry_type, @content, @series_referenced,
      @prediction_target, @prediction_direction, @prediction_value, @prediction_deadline, @falsification_criteria)
  `),

  getPendingHypotheses: db.prepare(`
    SELECT id, timestamp, content, prediction_target, prediction_direction,
      prediction_value, prediction_deadline, falsification_criteria
    FROM ai_journal
    WHERE entry_type = 'hypothesis'
      AND outcome_status = 'pending'
      AND prediction_deadline IS NOT NULL
      AND prediction_deadline < @now
    ORDER BY prediction_deadline ASC
    LIMIT @limit
  `),

  updateHypothesisVerdict: db.prepare(`
    UPDATE ai_journal SET
      outcome_status = @outcome_status,
      outcome_verdict = @outcome_verdict,
      outcome_confidence = @outcome_confidence,
      outcome_reviewed_at = @outcome_reviewed_at,
      trade_pnl_attribution = @trade_pnl_attribution,
      trades_in_window = @trades_in_window
    WHERE id = @id
  `),

  getReviewedHypotheses: db.prepare(`
    SELECT id, timestamp, content, prediction_target, prediction_direction,
      prediction_value, outcome_status, outcome_verdict, outcome_confidence,
      trade_pnl_attribution, outcome_reviewed_at
    FROM ai_journal
    WHERE entry_type = 'hypothesis'
      AND outcome_status != 'pending'
    ORDER BY outcome_reviewed_at DESC
    LIMIT @limit
  `),

  getHypothesisStats: db.prepare(`
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
      AND timestamp > @since
  `),

  getOrdersInWindow: db.prepare(`
    SELECT id, timestamp, action, instrument_name, filled_amount, fill_price,
      total_value, spot_price, success
    FROM orders
    WHERE timestamp BETWEEN @start AND @end
      AND success = 1
    ORDER BY timestamp ASC
  `),

  insertLesson: db.prepare(`
    INSERT INTO hypothesis_lessons (lesson, evidence_count)
    VALUES (@lesson, @evidence_count)
  `),

  getActiveLessons: db.prepare(`
    SELECT id, lesson, evidence_count, created_at
    FROM hypothesis_lessons
    WHERE is_active = 1
    ORDER BY created_at DESC
  `),

  archiveLesson: db.prepare(`
    UPDATE hypothesis_lessons SET is_active = 0, archived_at = datetime('now')
    WHERE id = @id
  `),

  countReviewedSinceLastLesson: db.prepare(`
    SELECT COUNT(*) as count
    FROM ai_journal
    WHERE entry_type = 'hypothesis'
      AND outcome_status != 'pending'
      AND outcome_reviewed_at > COALESCE(
        (SELECT MAX(created_at) FROM hypothesis_lessons), '1970-01-01')
  `),

  getOptionsDistribution: db.prepare(`
    SELECT option_type, COUNT(*) as count,
      AVG(delta) as avg_delta, MIN(delta) as min_delta, MAX(delta) as max_delta,
      AVG(ask_price) as avg_ask, AVG(bid_price) as avg_bid,
      AVG(ask_price - bid_price) as avg_spread, AVG(mark_price) as avg_mark,
      MIN(strike) as min_strike, MAX(strike) as max_strike,
      AVG(ask_delta_value) as avg_ask_dv, AVG(bid_delta_value) as avg_bid_dv
    FROM options_snapshots WHERE timestamp > @since GROUP BY option_type
  `),

  // Hourly aggregations for correlation engine
  getSpotPricesHourly: db.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           AVG(price) as avg_price
    FROM spot_prices WHERE timestamp > @since GROUP BY hour ORDER BY hour ASC
  `),

  getOnchainHourly: db.prepare(`
    WITH hourly_agg AS (
      SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
             AVG(liquidity_flow_magnitude) as avg_magnitude
      FROM onchain_data WHERE timestamp > @since
      GROUP BY hour
    ),
    direction_counts AS (
      SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
             liquidity_flow_direction as direction,
             COUNT(*) as cnt
      FROM onchain_data WHERE timestamp > @since
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

  getBestPutDvHourly: db.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           MAX(ask_delta_value) as value
    FROM options_snapshots
    WHERE timestamp > @since AND (option_type = 'P' OR instrument_name LIKE '%-P')
      AND delta <= -0.02 AND delta >= -0.12
    GROUP BY hour ORDER BY hour ASC
  `),

  getBestCallDvHourly: db.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           MAX(bid_delta_value) as value
    FROM options_snapshots
    WHERE timestamp > @since AND (option_type = 'C' OR instrument_name LIKE '%-C')
      AND delta >= 0.04 AND delta <= 0.12
    GROUP BY hour ORDER BY hour ASC
  `),

  getOptionsSpreadHourly: db.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           AVG((ask_price - bid_price) / mark_price) as value
    FROM options_snapshots
    WHERE timestamp > @since
      AND ask_price > 0 AND bid_price > 0 AND mark_price > 0
      AND ((delta <= -0.02 AND delta >= -0.12) OR (delta >= 0.04 AND delta <= 0.12))
    GROUP BY hour ORDER BY hour ASC
  `),

  getOptionsDepthHourly: db.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           AVG(ask_amount + bid_amount) as value
    FROM options_snapshots
    WHERE timestamp > @since
      AND ((delta <= -0.02 AND delta >= -0.12) OR (delta >= 0.04 AND delta <= 0.12))
    GROUP BY hour ORDER BY hour ASC
  `),

  getOpenInterestHourly: db.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           SUM(open_interest) as value
    FROM options_snapshots
    WHERE timestamp > @since AND open_interest IS NOT NULL AND open_interest > 0
    GROUP BY hour ORDER BY hour ASC
  `),

  getImpliedVolHourly: db.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           AVG(implied_vol) as value
    FROM options_snapshots
    WHERE timestamp > @since AND implied_vol IS NOT NULL
      AND ((delta <= -0.02 AND delta >= -0.12) OR (delta >= 0.04 AND delta <= 0.12))
    GROUP BY hour ORDER BY hour ASC
  `),

  insertOrder: db.prepare(`
    INSERT INTO orders (timestamp, action, success, reason, instrument_name, strike, expiry,
      delta, price, intended_amount, filled_amount, fill_price, total_value, spot_price, raw_response)
    VALUES (@timestamp, @action, @success, @reason, @instrument_name, @strike, @expiry,
      @delta, @price, @intended_amount, @filled_amount, @fill_price, @total_value, @spot_price, @raw_response)
  `),

  getRecentOrders: db.prepare(`
    SELECT * FROM orders WHERE timestamp > @since ORDER BY timestamp DESC LIMIT @limit
  `),

  // Hourly rollup upserts
  upsertSpotHourly: db.prepare(`
    INSERT INTO spot_prices_hourly (hour, open, high, low, close, avg_price, short_momentum, medium_momentum, count)
    VALUES (@hour, @price, @price, @price, @price, @price, @short_momentum, @medium_momentum, 1)
    ON CONFLICT(hour) DO UPDATE SET
      high = MAX(spot_prices_hourly.high, @price),
      low = MIN(spot_prices_hourly.low, @price),
      close = @price,
      avg_price = (spot_prices_hourly.avg_price * spot_prices_hourly.count + @price) / (spot_prices_hourly.count + 1),
      short_momentum = @short_momentum,
      medium_momentum = @medium_momentum,
      count = spot_prices_hourly.count + 1
  `),

  upsertOptionsHourly: db.prepare(`
    INSERT INTO options_hourly (hour, best_put_dv, best_call_dv, avg_spread, avg_depth, avg_iv, total_oi, count)
    VALUES (@hour, @best_put_dv, @best_call_dv, @avg_spread, @avg_depth, @avg_iv, @total_oi, 1)
    ON CONFLICT(hour) DO UPDATE SET
      best_put_dv = MAX(options_hourly.best_put_dv, @best_put_dv),
      best_call_dv = MAX(options_hourly.best_call_dv, @best_call_dv),
      avg_spread = (options_hourly.avg_spread * options_hourly.count + @avg_spread) / (options_hourly.count + 1),
      avg_depth = (options_hourly.avg_depth * options_hourly.count + @avg_depth) / (options_hourly.count + 1),
      avg_iv = (options_hourly.avg_iv * options_hourly.count + @avg_iv) / (options_hourly.count + 1),
      total_oi = @total_oi,
      count = options_hourly.count + 1
  `),

  upsertOnchainHourly: db.prepare(`
    INSERT INTO onchain_hourly (hour, dex, tvl, volume, tx_count, avg_magnitude, direction)
    VALUES (@hour, @dex, @tvl, @volume, @tx_count, @avg_magnitude, @direction)
    ON CONFLICT(hour, dex) DO UPDATE SET
      tvl = @tvl,
      volume = @volume,
      tx_count = @tx_count,
      avg_magnitude = (onchain_hourly.avg_magnitude + @avg_magnitude) / 2,
      direction = @direction
  `),

  // Funding rates
  insertFundingRate: db.prepare(`
    INSERT INTO funding_rates (timestamp, exchange, symbol, rate)
    VALUES (@timestamp, @exchange, @symbol, @rate)
  `),

  getLatestFundingTimestamp: db.prepare(`
    SELECT MAX(timestamp) as latest FROM funding_rates WHERE exchange = @exchange AND symbol = @symbol
  `),

  upsertFundingRateHourly: db.prepare(`
    INSERT INTO funding_rates_hourly (hour, exchange, symbol, avg_rate, count)
    VALUES (@hour, @exchange, @symbol, @rate, 1)
    ON CONFLICT(hour, exchange, symbol) DO UPDATE SET
      avg_rate = (funding_rates_hourly.avg_rate * funding_rates_hourly.count + @rate) / (funding_rates_hourly.count + 1),
      count = funding_rates_hourly.count + 1
  `),

  getFundingRatesHourly: db.prepare(`
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           AVG(rate) as avg_rate
    FROM funding_rates
    WHERE timestamp > @since AND symbol = @symbol
    GROUP BY hour ORDER BY hour ASC
  `),

  // Market sentiment queries (funding, skew, OI, quality)
  getFundingRateLatest: db.prepare(`
    SELECT rate, timestamp FROM funding_rates
    WHERE symbol = @symbol ORDER BY timestamp DESC LIMIT 1
  `),

  getFundingRateAvg24h: db.prepare(`
    SELECT AVG(rate) as avg_rate FROM funding_rates
    WHERE symbol = @symbol AND timestamp > @since
  `),

  getOptionsSkew: db.prepare(`
    SELECT timestamp, avg_put_iv, avg_call_iv
    FROM oi_snapshots
    WHERE timestamp > @since
      AND (avg_put_iv IS NOT NULL OR avg_call_iv IS NOT NULL)
    ORDER BY timestamp ASC
  `),

  getAggregateOI: db.prepare(`
    SELECT timestamp, SUM(open_interest) as total_oi
    FROM options_snapshots
    WHERE timestamp > @since AND open_interest IS NOT NULL AND open_interest > 0
    GROUP BY timestamp
    ORDER BY timestamp ASC
  `),

  getMarketQualitySummary: db.prepare(`
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
    WHERE timestamp = (SELECT MAX(timestamp) FROM options_snapshots WHERE timestamp > @since)
      AND mark_price > 0
      AND ask_price > 0
      AND bid_price > 0
      AND ABS(delta) BETWEEN 0.02 AND 0.12
    GROUP BY option_type
  `),

  insertOISnapshot: db.prepare(`
    INSERT INTO oi_snapshots (timestamp, put_oi, call_oi, near_put_oi, near_call_oi,
      far_put_oi, far_call_oi, total_oi, pc_ratio, expiry_count, avg_put_iv, avg_call_iv)
    VALUES (@timestamp, @put_oi, @call_oi, @near_put_oi, @near_call_oi,
      @far_put_oi, @far_call_oi, @total_oi, @pc_ratio, @expiry_count, @avg_put_iv, @avg_call_iv)
  `),

  // 7-day average premium for call selling elevation check
  getAvgCallPremium7d: db.prepare(`
    SELECT AVG(bid_price) as avg_premium
    FROM options_snapshots
    WHERE option_type = 'call'
      AND timestamp > @since
      AND bid_price > 0
  `),

  // ─── Trading Rules & Pending Actions ────────────────────────────────────────
  deactivateAllRules: db.prepare(`
    UPDATE trading_rules SET is_active = 0 WHERE is_active = 1
  `),

  insertTradingRule: db.prepare(`
    INSERT INTO trading_rules (rule_type, action, instrument_name, criteria, budget_limit, priority, reasoning, advisory_id, is_active, preferred_order_type)
    VALUES (@rule_type, @action, @instrument_name, @criteria, @budget_limit, @priority, @reasoning, @advisory_id, 1, @preferred_order_type)
  `),

  getActiveRules: db.prepare(`
    SELECT * FROM trading_rules WHERE is_active = 1 ORDER BY priority DESC, id ASC
  `),

  getActiveRulesByType: db.prepare(`
    SELECT * FROM trading_rules WHERE is_active = 1 AND rule_type = @rule_type ORDER BY priority DESC, id ASC
  `),

  deactivateStaleEmergencyBuybackRules: db.prepare(`
    UPDATE trading_rules
    SET is_active = 0
    WHERE is_active = 1
      AND action = 'buyback_call'
      AND (
        reasoning LIKE '%margin emergency%'
        OR reasoning LIKE '%MUST execute before any other portfolio action%'
      )
  `),

  insertPendingAction: db.prepare(`
    INSERT INTO pending_actions (rule_id, action, instrument_name, amount, price, trigger_details, status)
    VALUES (@rule_id, @action, @instrument_name, @amount, @price, @trigger_details, 'pending')
  `),

  updatePendingAction: db.prepare(`
    UPDATE pending_actions SET
      status = COALESCE(@status, status),
      confirmation_reasoning = COALESCE(@confirmation_reasoning, confirmation_reasoning),
      confirmed_at = COALESCE(@confirmed_at, confirmed_at),
      executed_at = COALESCE(@executed_at, executed_at),
      execution_result = COALESCE(@execution_result, execution_result),
      retries = COALESCE(@retries, retries)
    WHERE id = @id
  `),

  getPendingActionsByStatus: db.prepare(`
    SELECT pa.*, tr.reasoning as rule_reasoning, tr.criteria as rule_criteria
    FROM pending_actions pa
    LEFT JOIN trading_rules tr ON pa.rule_id = tr.id
    WHERE pa.status = @status
    ORDER BY pa.triggered_at ASC
  `),

  getRecentPendingActions: db.prepare(`
    SELECT * FROM pending_actions ORDER BY triggered_at DESC LIMIT @limit
  `),

  hasPendingActionForRule: db.prepare(`
    SELECT COUNT(*) as count FROM pending_actions
    WHERE rule_id = @rule_id AND status IN ('pending', 'confirmed')
  `),

  getLastExecutedAction: db.prepare(`
    SELECT executed_at FROM pending_actions
    WHERE action = @action AND status = 'executed'
    ORDER BY executed_at DESC LIMIT 1
  `),

  // Resting order tracking
  insertRestingOrder: db.prepare(`
    INSERT OR IGNORE INTO resting_orders (order_id, instrument_name, action, direction, amount, limit_price)
    VALUES (@order_id, @instrument_name, @action, @direction, @amount, @limit_price)
  `),
  getOpenRestingOrders: db.prepare(`
    SELECT * FROM resting_orders WHERE status = 'open'
  `),
  updateRestingOrder: db.prepare(`
    UPDATE resting_orders SET status = @status, filled_amount = @filled_amount WHERE order_id = @order_id
  `),
  updateRestingOrderId: db.prepare(`
    UPDATE resting_orders
    SET order_id = @new_order_id
    WHERE order_id = @old_order_id
  `),
  hasRestingOrderForInstrument: db.prepare(`
    SELECT COUNT(*) as count FROM resting_orders
    WHERE instrument_name = @instrument_name AND status = 'open'
  `),

  // Portfolio P&L
  insertPortfolioSnapshot: db.prepare(`
    INSERT INTO portfolio_snapshots (timestamp, spot_price, usdc_balance, eth_balance, positions_json,
      total_unrealized_pnl, total_realized_pnl, portfolio_value_usd)
    VALUES (@timestamp, @spot_price, @usdc_balance, @eth_balance, @positions_json,
      @total_unrealized_pnl, @total_realized_pnl, @portfolio_value_usd)
  `),
  getPortfolioHistory: db.prepare(`
    SELECT timestamp, spot_price, usdc_balance, eth_balance,
      total_unrealized_pnl, total_realized_pnl, portfolio_value_usd
    FROM portfolio_snapshots
    WHERE timestamp > @since
    ORDER BY timestamp ASC
  `),
  getLatestPortfolioSnapshot: db.prepare(`
    SELECT * FROM portfolio_snapshots ORDER BY id DESC LIMIT 1
  `),
  getRealizedPnL: db.prepare(`
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

// ─── Helper Functions ─────────────────────────────────────────────────────────

const insertSpotPrice = (spotPrice, momentumResult, botData, timestamp) => {
  const shortMomentum = typeof momentumResult.shortTermMomentum === 'object'
    ? momentumResult.shortTermMomentum : { main: momentumResult.shortTermMomentum };
  const medMomentum = typeof momentumResult.mediumTermMomentum === 'object'
    ? momentumResult.mediumTermMomentum : { main: momentumResult.mediumTermMomentum };

  const ts = timestamp || new Date().toISOString();
  const result = stmts.insertSpotPrice.run({
    timestamp: ts,
    price: spotPrice,
    short_momentum_main: shortMomentum.main || null,
    short_momentum_derivative: shortMomentum.derivative || null,
    medium_momentum_main: medMomentum.main || null,
    medium_momentum_derivative: medMomentum.derivative || null,
    three_day_high: botData.shortTermMomentum?.threeDayHigh || null,
    three_day_low: botData.shortTermMomentum?.threeDayLow || null,
    seven_day_high: botData.shortTermMomentum?.sevenDayHigh || null,
    seven_day_low: botData.shortTermMomentum?.sevenDayLow || null,
  });

  // Upsert into hourly rollup
  try {
    stmts.upsertSpotHourly.run({
      hour: toHourKey(ts),
      price: spotPrice,
      short_momentum: shortMomentum.main || null,
      medium_momentum: medMomentum.main || null,
    });
  } catch (e) { /* rollup failure should not block raw insert */ }

  return result;
};

const toNum = (v) => v != null && v !== '' ? Number(v) : null;
const toHourKey = (ts) => ts.slice(0, 13) + ':00:00Z';

const insertOptionsSnapshotBatch = (options, timestamp) => {
  const insert = db.transaction((opts) => {
    for (const opt of opts) {
      stmts.insertOptionsSnapshot.run({
        timestamp,
        instrument_name: opt.instrument_name || '',
        strike: toNum(opt.option_details?.strike),
        expiry: opt.option_details?.expiry || null,
        option_type: opt.option_details?.option_type || (opt.instrument_name?.includes('-P') ? 'P' : 'C'),
        delta: toNum(opt.details?.delta),
        ask_price: toNum(opt.details?.askPrice),
        bid_price: toNum(opt.details?.bidPrice),
        ask_amount: toNum(opt.details?.askAmount),
        bid_amount: toNum(opt.details?.bidAmount),
        mark_price: toNum(opt.details?.markPrice),
        index_price: toNum(opt.details?.indexPrice),
        ask_delta_value: toNum(opt.details?.askDeltaValue),
        bid_delta_value: toNum(opt.details?.bidDeltaValue),
        open_interest: toNum(opt.details?.openInterest),
        implied_vol: toNum(opt.details?.impliedVol),
      });
    }
  });
  insert(options);

  // Upsert hourly rollup from batch aggregates
  try {
    const hour = toHourKey(timestamp);
    let bestPutDv = 0, bestCallDv = 0;
    let spreadSum = 0, spreadCount = 0;
    let depthSum = 0, depthCount = 0;
    let ivSum = 0, ivCount = 0;
    let totalOi = 0;

    for (const opt of options) {
      const delta = toNum(opt.details?.delta);
      const askDv = toNum(opt.details?.askDeltaValue);
      const bidDv = toNum(opt.details?.bidDeltaValue);
      const askPrice = toNum(opt.details?.askPrice);
      const bidPrice = toNum(opt.details?.bidPrice);
      const markPrice = toNum(opt.details?.markPrice);
      const askAmount = toNum(opt.details?.askAmount);
      const bidAmount = toNum(opt.details?.bidAmount);
      const iv = toNum(opt.details?.impliedVol);
      const oi = toNum(opt.details?.openInterest);
      const isPut = (opt.option_details?.option_type === 'P') || opt.instrument_name?.includes('-P');
      const isCall = (opt.option_details?.option_type === 'C') || opt.instrument_name?.includes('-C');

      // Best put DV (delta -0.02 to -0.12)
      if (isPut && delta != null && delta <= -0.02 && delta >= -0.12 && askDv != null && askDv > bestPutDv) {
        bestPutDv = askDv;
      }
      // Best call DV (delta 0.04 to 0.12)
      if (isCall && delta != null && delta >= 0.04 && delta <= 0.12 && bidDv != null && bidDv > bestCallDv) {
        bestCallDv = bidDv;
      }
      // Spread (within bot's delta range)
      if (delta != null && ((delta <= -0.02 && delta >= -0.12) || (delta >= 0.04 && delta <= 0.12))) {
        if (askPrice > 0 && bidPrice > 0 && markPrice > 0) {
          spreadSum += (askPrice - bidPrice) / markPrice;
          spreadCount++;
        }
        if (askAmount != null && bidAmount != null) {
          depthSum += askAmount + bidAmount;
          depthCount++;
        }
        if (iv != null) {
          ivSum += iv;
          ivCount++;
        }
      }
      if (oi != null && oi > 0) totalOi += oi;
    }

    stmts.upsertOptionsHourly.run({
      hour,
      best_put_dv: bestPutDv,
      best_call_dv: bestCallDv,
      avg_spread: spreadCount > 0 ? spreadSum / spreadCount : 0,
      avg_depth: depthCount > 0 ? depthSum / depthCount : 0,
      avg_iv: ivCount > 0 ? ivSum / ivCount : 0,
      total_oi: totalOi,
    });
  } catch (e) { /* rollup failure should not block raw insert */ }
};

const insertOnchainData = (analysis) => {
  const flow = analysis.dexLiquidity?.flowAnalysis || {};
  const ts = analysis.timestamp || new Date().toISOString();

  stmts.insertOnchainData.run({
    timestamp: ts,
    spot_price: analysis.spotPrice || null,
    liquidity_flow_direction: flow.direction || null,
    liquidity_flow_magnitude: toNum(flow.magnitude),
    liquidity_flow_confidence: toNum(flow.confidence),
    exhaustion_score: null,
    exhaustion_alert_level: null,
    raw_data: JSON.stringify(analysis),
  });

  // Upsert per-DEX hourly rollup
  try {
    const hour = toHourKey(ts);
    const dexes = analysis.dexLiquidity?.dexes;
    if (dexes) {
      for (const [name, dex] of Object.entries(dexes)) {
        if (dex.error) continue;
        stmts.upsertOnchainHourly.run({
          hour,
          dex: name,
          tvl: toNum(dex.totalLiquidity) || 0,
          volume: toNum(dex.totalVolume) || 0,
          tx_count: toNum(dex.totalTxCount) || 0,
          avg_magnitude: toNum(flow.magnitude) || 0,
          direction: flow.direction || null,
        });
      }
    }
  } catch (e) { /* rollup failure should not block raw insert */ }
};

const insertSignal = (signalType, details, actedOn = false) => {
  const result = stmts.insertSignal.run({
    timestamp: new Date().toISOString(),
    signal_type: signalType,
    details: typeof details === 'string' ? details : JSON.stringify(details),
    acted_on: actedOn ? 1 : 0,
  });
  return result.lastInsertRowid;
};

// Read helpers
const getRecentSpotPrices = (since) => stmts.getRecentSpotPrices.all({ since });
const getRecentSignals = (since, limit = 50) => stmts.getRecentSignals.all({ since, limit });
const getRecentOnchain = (since) => stmts.getRecentOnchain.all({ since });
const getRecentOptionsSnapshots = (since) => stmts.getRecentOptionsSnapshots.all({ since });
const getStats = () => stmts.getStats.get();
const insertTick = (timestamp, summary) => {
  stmts.insertTick.run({ timestamp, summary: typeof summary === 'string' ? summary : JSON.stringify(summary) });
};
const getRecentTicks = (limit = 50) => stmts.getRecentTicks.all({ limit });
const getBestScores = (windowDays = 7) => {
  const since = new Date(Date.now() - windowDays * 86400000).toISOString();
  const row = stmts.getBestScoresAgg.get({ since }) || {};
  const bestPutDetail = row.best_put_score != null
    ? stmts.getBestPutDetail.get({ since, score: row.best_put_score })
    : null;
  const bestCallDetail = row.best_call_score != null
    ? stmts.getBestCallDetail.get({ since, score: row.best_call_score })
    : null;
  return {
    bestPutScore: row.best_put_score || 0,
    bestCallScore: row.best_call_score || 0,
    windowDays,
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
};

const getOptionsDistribution = (since) => stmts.getOptionsDistribution.all({ since });

// Hourly helpers for correlation engine
const getSpotPricesHourly = (since) => stmts.getSpotPricesHourly.all({ since });
const getOnchainHourly = (since) => stmts.getOnchainHourly.all({ since });
const getBestPutDvHourly = (since) => stmts.getBestPutDvHourly.all({ since });
const getBestCallDvHourly = (since) => stmts.getBestCallDvHourly.all({ since });
const getOptionsSpreadHourly = (since) => stmts.getOptionsSpreadHourly.all({ since });
const getOptionsDepthHourly = (since) => stmts.getOptionsDepthHourly.all({ since });
const getOpenInterestHourly = (since) => stmts.getOpenInterestHourly.all({ since });
const getImpliedVolHourly = (since) => stmts.getImpliedVolHourly.all({ since });

const insertOrder = (data) => {
  stmts.insertOrder.run({
    timestamp: data.timestamp || new Date().toISOString(),
    action: data.action || 'unknown',
    success: data.success ? 1 : 0,
    reason: data.reason || null,
    instrument_name: data.instrument_name || null,
    strike: toNum(data.strike),
    expiry: data.expiry || null,
    delta: toNum(data.delta),
    price: toNum(data.price),
    intended_amount: toNum(data.intended_amount),
    filled_amount: toNum(data.filled_amount),
    fill_price: toNum(data.fill_price),
    total_value: toNum(data.total_value),
    spot_price: toNum(data.spot_price),
    raw_response: data.raw_response ? (typeof data.raw_response === 'string' ? data.raw_response : JSON.stringify(data.raw_response)) : null,
  });
};

const getRecentOrders = (since, limit = 50) => stmts.getRecentOrders.all({ since, limit });

const getAvgCallPremium7d = () => {
  const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  return stmts.getAvgCallPremium7d.get({ since });
};

const insertOISnapshot = (data) => {
  stmts.insertOISnapshot.run({
    timestamp: data.timestamp || new Date().toISOString(),
    put_oi: data.put_oi || 0,
    call_oi: data.call_oi || 0,
    near_put_oi: data.near_put_oi || 0,
    near_call_oi: data.near_call_oi || 0,
    far_put_oi: data.far_put_oi || 0,
    far_call_oi: data.far_call_oi || 0,
    total_oi: data.total_oi || 0,
    pc_ratio: data.pc_ratio || null,
    expiry_count: data.expiry_count || 0,
    avg_put_iv: data.avg_put_iv ?? null,
    avg_call_iv: data.avg_call_iv ?? null,
  });
};

const insertFundingRates = (rates) => {
  const insert = db.transaction((items) => {
    for (const item of items) {
      // Deduplicate: check if we already have this exact timestamp
      const latest = stmts.getLatestFundingTimestamp.get({ exchange: item.exchange, symbol: item.symbol });
      if (latest?.latest && new Date(item.timestamp) <= new Date(latest.latest)) continue;

      stmts.insertFundingRate.run({
        timestamp: item.timestamp,
        exchange: item.exchange,
        symbol: item.symbol,
        rate: item.rate,
      });

      // Hourly rollup
      try {
        stmts.upsertFundingRateHourly.run({
          hour: toHourKey(item.timestamp),
          exchange: item.exchange,
          symbol: item.symbol,
          rate: item.rate,
        });
      } catch (e) { /* rollup failure should not block raw insert */ }
    }
  });
  insert(rates);
};

const getFundingRatesHourly = (since, symbol = 'ETHUSDT') => {
  return stmts.getFundingRatesHourly.all({ since, symbol });
};

const getFundingRateLatest = (symbol = 'ETHUSDT') => {
  try { return stmts.getFundingRateLatest.get({ symbol }); } catch { return null; }
};

const getFundingRateAvg24h = (symbol = 'ETHUSDT') => {
  try {
    const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
    return stmts.getFundingRateAvg24h.get({ symbol, since })?.avg_rate ?? null;
  } catch { return null; }
};

const getOptionsSkew = (since) => {
  try { return stmts.getOptionsSkew.all({ since }); } catch { return []; }
};

const getAggregateOI = (since) => {
  try { return stmts.getAggregateOI.all({ since }); } catch { return []; }
};

const getMarketQualitySummary = (since) => {
  try { return stmts.getMarketQualitySummary.all({ since }); } catch { return []; }
};

// ─── AI Journal Helpers ──────────────────────────────────────────────────────

const insertJournalEntry = (entryType, content, seriesReferenced = null) => {
  stmts.insertJournalEntry.run({
    timestamp: new Date().toISOString(),
    entry_type: entryType,
    content,
    series_referenced: seriesReferenced ? JSON.stringify(seriesReferenced) : null,
  });
};

const getRecentJournalEntries = (limit = 20) => stmts.getRecentJournalEntries.all({ limit });

const insertJournalEntryFull = (entryType, content, seriesReferenced = null, meta = null) => {
  stmts.insertJournalEntryFull.run({
    timestamp: new Date().toISOString(),
    entry_type: entryType,
    content,
    series_referenced: seriesReferenced ? JSON.stringify(seriesReferenced) : null,
    prediction_target: meta?.target || null,
    prediction_direction: meta?.direction || null,
    prediction_value: meta?.value != null ? Number(meta.value) : null,
    prediction_deadline: meta?.deadline || null,
    falsification_criteria: meta?.falsification || null,
  });
};

const getPendingHypotheses = (limit = 3) => {
  return stmts.getPendingHypotheses.all({ now: new Date().toISOString(), limit });
};

const updateHypothesisVerdict = (id, verdict) => {
  stmts.updateHypothesisVerdict.run({
    id,
    outcome_status: verdict.status,
    outcome_verdict: verdict.verdict,
    outcome_confidence: verdict.confidence,
    outcome_reviewed_at: new Date().toISOString(),
    trade_pnl_attribution: verdict.tradePnl != null ? verdict.tradePnl : null,
    trades_in_window: verdict.tradeIds ? JSON.stringify(verdict.tradeIds) : null,
  });
};

const getReviewedHypotheses = (limit = 30) => {
  return stmts.getReviewedHypotheses.all({ limit });
};

const getHypothesisStats = (sinceDays = 30) => {
  const since = new Date(Date.now() - sinceDays * 24 * 60 * 60 * 1000).toISOString();
  return stmts.getHypothesisStats.get({ since });
};

const getOrdersInWindow = (start, end) => {
  return stmts.getOrdersInWindow.all({ start, end });
};

const insertLesson = (lesson, evidenceCount) => {
  stmts.insertLesson.run({ lesson, evidence_count: evidenceCount });
};

const getActiveLessons = () => {
  return stmts.getActiveLessons.all();
};

const archiveLesson = (id) => {
  stmts.archiveLesson.run({ id });
};

const countReviewedSinceLastLesson = () => {
  return stmts.countReviewedSinceLastLesson.get()?.count || 0;
};

// ─── Trading Rules & Pending Actions Helpers ─────────────────────────────────

const replaceActiveRules = (advisoryId, rules) => {
  const replace = db.transaction((items) => {
    stmts.deactivateAllRules.run();
    for (const rule of items) {
      stmts.insertTradingRule.run({
        rule_type: rule.rule_type,
        action: rule.action,
        instrument_name: rule.instrument_name || null,
        criteria: typeof rule.criteria === 'string' ? rule.criteria : JSON.stringify(rule.criteria),
        budget_limit: rule.budget_limit ?? null,
        priority: rule.priority || 'medium',
        reasoning: rule.reasoning || null,
        advisory_id: advisoryId,
        preferred_order_type: rule.preferred_order_type || null,
      });
    }
  });
  replace(rules);
};

const insertPendingAction = (action) => {
  return stmts.insertPendingAction.run({
    rule_id: action.rule_id ?? null,
    action: action.action,
    instrument_name: action.instrument_name,
    amount: action.amount ?? null,
    price: action.price ?? null,
    trigger_details: action.trigger_details ? (typeof action.trigger_details === 'string' ? action.trigger_details : JSON.stringify(action.trigger_details)) : null,
  });
};

const updatePendingAction = (id, fields) => {
  stmts.updatePendingAction.run({
    id,
    status: fields.status ?? null,
    confirmation_reasoning: fields.confirmation_reasoning ?? null,
    confirmed_at: fields.confirmed_at ?? null,
    executed_at: fields.executed_at ?? null,
    execution_result: fields.execution_result ? (typeof fields.execution_result === 'string' ? fields.execution_result : JSON.stringify(fields.execution_result)) : null,
    retries: fields.retries ?? null,
  });
};

const getActiveRules = () => stmts.getActiveRules.all();
const getActiveRulesByType = (ruleType) => stmts.getActiveRulesByType.all({ rule_type: ruleType });
const deactivateStaleEmergencyBuybackRules = () => stmts.deactivateStaleEmergencyBuybackRules.run().changes || 0;
const getPendingActions = (status) => stmts.getPendingActionsByStatus.all({ status });
const getRecentPendingActions = (limit = 20) => stmts.getRecentPendingActions.all({ limit });
const hasPendingActionForRule = (ruleId) => (stmts.hasPendingActionForRule.get({ rule_id: ruleId })?.count || 0) > 0;
const getLastExecutedAction = (action) => stmts.getLastExecutedAction.get({ action })?.executed_at || null;

// ─── Resting Order Helpers ──────────────────────────────────────────────────

const insertRestingOrder = (order) => {
  stmts.insertRestingOrder.run({
    order_id: order.order_id,
    instrument_name: order.instrument_name,
    action: order.action,
    direction: order.direction,
    amount: order.amount,
    limit_price: order.limit_price,
  });
};

const getOpenRestingOrders = () => stmts.getOpenRestingOrders.all();

const updateRestingOrder = (orderId, status, filledAmount) => {
  stmts.updateRestingOrder.run({ order_id: orderId, status, filled_amount: filledAmount ?? 0 });
};

const updateRestingOrderId = (oldOrderId, newOrderId) => {
  stmts.updateRestingOrderId.run({ old_order_id: oldOrderId, new_order_id: newOrderId });
};

const hasRestingOrderForInstrument = (instrumentName) => {
  return (stmts.hasRestingOrderForInstrument.get({ instrument_name: instrumentName })?.count || 0) > 0;
};

// ─── Portfolio P&L ───────────────────────────────────────────────────────────

const insertPortfolioSnapshot = (snapshot) => {
  stmts.insertPortfolioSnapshot.run({
    timestamp: snapshot.timestamp || new Date().toISOString(),
    spot_price: snapshot.spot_price || 0,
    usdc_balance: snapshot.usdc_balance || 0,
    eth_balance: snapshot.eth_balance || 0,
    positions_json: typeof snapshot.positions_json === 'string' ? snapshot.positions_json : JSON.stringify(snapshot.positions_json || []),
    total_unrealized_pnl: snapshot.total_unrealized_pnl || 0,
    total_realized_pnl: snapshot.total_realized_pnl || 0,
    portfolio_value_usd: snapshot.portfolio_value_usd || 0,
  });
};

const getPortfolioHistory = (since) => stmts.getPortfolioHistory.all({ since });
const getLatestPortfolioSnapshot = () => stmts.getLatestPortfolioSnapshot.get() || null;
const getRealizedPnL = () => stmts.getRealizedPnL.get() || {};

// ─── Bot State Helpers ────────────────────────────────────────────────────────

const saveBotState = (botData) => {
  stmts.upsertBotState.run({
    put_cycle_start: botData.putCycleStart || null,
    put_net_bought: botData.putNetBought || 0,
    put_unspent_buy_limit: botData.putUnspentBuyLimit || 0,
    put_budget_for_cycle: botData.putBudgetForCycle || 0,
    call_cycle_start: null,
    call_net_sold: 0,
    call_unspent_sell_limit: 0,
    last_check: botData.lastCheck || 0,
    last_journal_generation: botData.lastJournalGeneration || 0,
    last_advisory_spot_price: botData.lastAdvisorySpotPrice || null,
    last_advisory_timestamp: botData.lastAdvisoryTimestamp || 0,
  });
};

const loadBotState = () => {
  return stmts.getBotState.get() || null;
};

const loadPriceHistoryFromDb = () => {
  const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  const rows = stmts.getSpotPriceHistory7d.all({ since });
  // Normalize timestamps to Unix ms for compatibility with momentum functions
  return rows.map(r => ({
    price: r.price,
    timestamp: new Date(r.timestamp).getTime(),
  }));
};

const migrateFromJson = (jsonPath) => {
  try {
    const existing = stmts.getBotState.get();
    if (existing) return; // already migrated

    if (!fs.existsSync(jsonPath)) return; // no JSON to migrate

    const raw = fs.readFileSync(jsonPath, 'utf-8');
    const data = JSON.parse(raw);

    stmts.upsertBotState.run({
      put_cycle_start: data.putCycleStart || null,
      put_net_bought: data.putNetBought || 0,
      put_unspent_buy_limit: data.putUnspentBuyLimit || 0,
      call_cycle_start: data.callCycleStart || null,
      call_net_sold: data.callNetSold || 0,
      call_unspent_sell_limit: data.callUnspentSellLimit || 0,
    });

    // Rename JSON to .migrated
    const migratedPath = jsonPath + '.migrated';
    fs.renameSync(jsonPath, migratedPath);
    console.log(`Migrated bot_data.json cycle state to SQLite (renamed to ${migratedPath})`);
  } catch (e) {
    console.error('migrateFromJson failed (will fall back to JSON):', e.message);
  }
};

// Graceful close
const close = () => db.close();

module.exports = {
  db,
  insertSpotPrice,
  insertOptionsSnapshotBatch,
  insertOnchainData,
  insertSignal,
  markSignalActed: (id) => stmts.markSignalActed.run({ id }),
  getRecentSpotPrices,
  getRecentSignals,
  getRecentOnchain,
  getRecentOptionsSnapshots,
  getStats,
  getOptionsDistribution,
  getAvgCallPremium7d,
  getSpotPricesHourly,
  getOnchainHourly,
  getBestPutDvHourly,
  getBestCallDvHourly,
  getOptionsSpreadHourly,
  getOptionsDepthHourly,
  getOpenInterestHourly,
  getImpliedVolHourly,
  insertTick,
  getRecentTicks,
  getBestScores,
  insertOrder,
  getRecentOrders,
  insertJournalEntry,
  getRecentJournalEntries,
  insertJournalEntryFull,
  getPendingHypotheses,
  updateHypothesisVerdict,
  getReviewedHypotheses,
  getHypothesisStats,
  getOrdersInWindow,
  insertLesson,
  getActiveLessons,
  archiveLesson,
  countReviewedSinceLastLesson,
  saveBotState,
  loadBotState,
  loadPriceHistoryFromDb,
  migrateFromJson,
  insertOISnapshot,
  insertFundingRates,
  getFundingRatesHourly,
  getFundingRateLatest,
  getFundingRateAvg24h,
  getOptionsSkew,
  getAggregateOI,
  getMarketQualitySummary,
  // Trading rules & pending actions
  replaceActiveRules,
  insertPendingAction,
  updatePendingAction,
  getActiveRules,
  getActiveRulesByType,
  deactivateStaleEmergencyBuybackRules,
  getPendingActions,
  getRecentPendingActions,
  hasPendingActionForRule,
  getLastExecutedAction,
  // Resting orders
  insertRestingOrder,
  getOpenRestingOrders,
  updateRestingOrder,
  updateRestingOrderId,
  hasRestingOrderForInstrument,
  // Portfolio P&L
  insertPortfolioSnapshot,
  getPortfolioHistory,
  getLatestPortfolioSnapshot,
  getRealizedPnL,
  close,
};
