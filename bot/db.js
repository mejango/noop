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
try { db.exec('ALTER TABLE bot_state ADD COLUMN advisory_retry_count INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN next_advisory_retry_at INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_advisory_spot_price REAL'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_advisory_timestamp INTEGER NOT NULL DEFAULT 0'); } catch {}

// Resting (GTC/post_only) orders we've placed — for fill reconciliation
db.exec(`
  CREATE TABLE IF NOT EXISTS resting_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL UNIQUE,
    pending_action_id INTEGER REFERENCES pending_actions(id),
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
try { db.exec('ALTER TABLE resting_orders ADD COLUMN pending_action_id INTEGER REFERENCES pending_actions(id)'); } catch {}
try {
  const restingActions = db.prepare(`
    SELECT id, execution_result
    FROM pending_actions
    WHERE status = 'executed' AND execution_result IS NOT NULL
  `).all();
  const linkRestingOrder = db.prepare(`
    UPDATE resting_orders
    SET pending_action_id = @pending_action_id
    WHERE order_id = @order_id AND pending_action_id IS NULL
  `);
  const getRestingOrder = db.prepare(`
    SELECT status, filled_amount FROM resting_orders WHERE order_id = @order_id
  `);
  const updateActionStatus = db.prepare(`
    UPDATE pending_actions
    SET status = @status,
      execution_result = COALESCE(@execution_result, execution_result)
    WHERE id = @id
  `);

  for (const action of restingActions) {
    let parsed = null;
    try { parsed = JSON.parse(action.execution_result); } catch { parsed = null; }
    const orderId = parsed?.orderId || parsed?.order_id;
    if (!parsed?.resting || !orderId) continue;

    linkRestingOrder.run({ pending_action_id: action.id, order_id: orderId });
    const restingOrder = getRestingOrder.get({ order_id: orderId });
    if (!restingOrder) continue;
    const filledAmount = Number(restingOrder.filled_amount || 0);
    if (restingOrder.status === 'open') {
      updateActionStatus.run({ id: action.id, status: 'resting', execution_result: null });
    } else if (filledAmount > 0 || restingOrder.status === 'filled') {
      updateActionStatus.run({
        id: action.id,
        status: 'executed',
        execution_result: JSON.stringify({
          orderId,
          orderStatus: restingOrder.status,
          filledAmount,
          note: 'Legacy resting order reconciled',
        }),
      });
    } else if (filledAmount <= 0 && (restingOrder.status === 'cancelled' || restingOrder.status === 'expired')) {
      updateActionStatus.run({
        id: action.id,
        status: 'cancelled',
        execution_result: JSON.stringify({
          orderId,
          orderStatus: restingOrder.status,
          filledAmount,
          note: 'Legacy resting order reconciled',
        }),
      });
    }
  }
} catch {}

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
    last_wiki_lint_run INTEGER NOT NULL DEFAULT 0,
    last_trade_review_run INTEGER NOT NULL DEFAULT 0,
    last_trade_review_success INTEGER NOT NULL DEFAULT 0,
    last_trade_review_ready_count INTEGER NOT NULL DEFAULT 0,
    last_trade_review_error TEXT,
    last_trade_review_targets TEXT,
    last_hypothesis_lesson_review_id INTEGER NOT NULL DEFAULT 0,
    last_trade_lesson_review_id INTEGER NOT NULL DEFAULT 0,
    last_advisory_run INTEGER NOT NULL DEFAULT 0,
    last_advisory_success INTEGER NOT NULL DEFAULT 0,
    last_advisory_error TEXT,
    advisory_retry_count INTEGER NOT NULL DEFAULT 0,
    next_advisory_retry_at INTEGER NOT NULL DEFAULT 0,
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
try { db.exec('ALTER TABLE orders ADD COLUMN pending_action_id INTEGER'); } catch {}

// Hypothesis tracking columns (idempotent)
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_check INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_journal_generation INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_wiki_lint_run INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_trade_review_run INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_trade_review_success INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_trade_review_ready_count INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_trade_review_error TEXT'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_trade_review_targets TEXT'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_hypothesis_lesson_review_id INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_trade_lesson_review_id INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_advisory_run INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_advisory_success INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec('ALTER TABLE bot_state ADD COLUMN last_advisory_error TEXT'); } catch {}
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
    expiry_count INTEGER,
    avg_put_iv REAL,
    avg_call_iv REAL
  );
  CREATE INDEX IF NOT EXISTS idx_oi_snapshots_timestamp ON oi_snapshots(timestamp);

  CREATE TABLE IF NOT EXISTS trade_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_name TEXT NOT NULL,
    action_family TEXT NOT NULL,
    opened_at TEXT,
    closed_at TEXT NOT NULL,
    review_window_days INTEGER NOT NULL DEFAULT 1,
    horizon_end_at TEXT,
    order_ids TEXT NOT NULL,
    review_status TEXT NOT NULL,
    review_confidence REAL,
    summary TEXT NOT NULL,
    lessons TEXT,
    pnl_realized REAL,
    premium_opened REAL,
    premium_closed REAL,
    spot_open REAL,
    spot_close REAL,
    spot_min_while_open REAL,
    spot_max_while_open REAL,
    spot_min_after_close REAL,
    spot_max_after_close REAL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    is_active INTEGER NOT NULL DEFAULT 1,
    UNIQUE(instrument_name, closed_at, review_window_days)
  );
  CREATE INDEX IF NOT EXISTS idx_trade_reviews_closed_at ON trade_reviews(closed_at);

  CREATE TABLE IF NOT EXISTS trade_lessons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lesson TEXT NOT NULL,
    evidence_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    archived_at TEXT,
    is_active INTEGER NOT NULL DEFAULT 1
  );

  CREATE TABLE IF NOT EXISTS candidate_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_at TEXT NOT NULL,
    tick_id TEXT,
    rule_id INTEGER,
    pending_action_id INTEGER,
    action TEXT NOT NULL,
    instrument_name TEXT NOT NULL,
    option_type TEXT,
    candidate_rank INTEGER,
    selected INTEGER NOT NULL DEFAULT 0,
    decision_status TEXT,
    selection_reason TEXT,
    spot_price REAL,
    strike REAL,
    expiry INTEGER,
    dte REAL,
    delta REAL,
    bid_price REAL,
    ask_price REAL,
    mark_price REAL,
    bid_amount REAL,
    ask_amount REAL,
    spread_pct REAL,
    depth REAL,
    implied_vol REAL,
    open_interest REAL,
    raw_score REAL,
    selection_score REAL,
    score_band TEXT,
    dte_bucket TEXT,
    research_recommendation TEXT,
    score_trend_24h_pct REAL,
    spot_ret_6h REAL,
    spot_ret_24h REAL,
    rule_min_score REAL,
    rule_min_bid REAL,
    rule_dte_min REAL,
    rule_dte_max REAL,
    rule_delta_min REAL,
    rule_delta_max REAL,
    metadata TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_candidate_observations_observed_at ON candidate_observations(observed_at);
  CREATE INDEX IF NOT EXISTS idx_candidate_observations_action ON candidate_observations(action, observed_at);
  CREATE INDEX IF NOT EXISTS idx_candidate_observations_instrument ON candidate_observations(instrument_name, observed_at);
  CREATE INDEX IF NOT EXISTS idx_candidate_observations_pending ON candidate_observations(pending_action_id);
  CREATE INDEX IF NOT EXISTS idx_options_snapshots_instrument_ts ON options_snapshots(instrument_name, timestamp);

  CREATE TABLE IF NOT EXISTS decision_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_id INTEGER NOT NULL REFERENCES candidate_observations(id),
    horizon_hours INTEGER NOT NULL,
    due_at TEXT NOT NULL,
    evaluated_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    future_quote_at TEXT,
    future_spot_at TEXT,
    future_spot REAL,
    spot_return REAL,
    future_bid REAL,
    future_ask REAL,
    future_mark REAL,
    future_score REAL,
    sell_entry_pnl REAL,
    sell_capture_pct REAL,
    buy_entry_pnl REAL,
    buy_capture_pct REAL,
    spot_min REAL,
    spot_max REAL,
    error TEXT,
    UNIQUE(observation_id, horizon_hours)
  );
  CREATE INDEX IF NOT EXISTS idx_decision_outcomes_due ON decision_outcomes(status, due_at);
  CREATE INDEX IF NOT EXISTS idx_decision_outcomes_observation ON decision_outcomes(observation_id);

  CREATE TABLE IF NOT EXISTS rule_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evaluated_at TEXT NOT NULL,
    rule_id INTEGER,
    rule_type TEXT,
    action TEXT,
    instrument_name TEXT,
    decision_status TEXT NOT NULL,
    reason_code TEXT,
    reason TEXT,
    candidates_evaluated INTEGER,
    selected_instrument TEXT,
    raw_score REAL,
    selection_score REAL,
    price REAL,
    amount REAL,
    spot_price REAL,
    pending_action_id INTEGER,
    criteria_json TEXT,
    context_json TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_rule_decisions_time ON rule_decisions(evaluated_at);
  CREATE INDEX IF NOT EXISTS idx_rule_decisions_rule ON rule_decisions(rule_id, evaluated_at);
  CREATE INDEX IF NOT EXISTS idx_rule_decisions_status ON rule_decisions(decision_status, reason_code);

  CREATE TABLE IF NOT EXISTS position_lifecycle (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_name TEXT NOT NULL,
    action_family TEXT NOT NULL,
    option_type TEXT,
    strike REAL,
    expiry INTEGER,
    opened_at TEXT,
    closed_at TEXT,
    status TEXT NOT NULL,
    opened_amount REAL DEFAULT 0,
    closed_amount REAL DEFAULT 0,
    net_amount REAL DEFAULT 0,
    premium_opened REAL DEFAULT 0,
    premium_closed REAL DEFAULT 0,
    net_credit REAL DEFAULT 0,
    avg_open_price REAL,
    avg_close_price REAL,
    spot_open REAL,
    spot_close REAL,
    order_ids TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(instrument_name, action_family)
  );
  CREATE INDEX IF NOT EXISTS idx_position_lifecycle_status ON position_lifecycle(status, action_family);
  CREATE INDEX IF NOT EXISTS idx_position_lifecycle_opened ON position_lifecycle(opened_at);
`);
try { db.exec('ALTER TABLE oi_snapshots ADD COLUMN avg_put_iv REAL'); } catch {}
try { db.exec('ALTER TABLE oi_snapshots ADD COLUMN avg_call_iv REAL'); } catch {}
try { db.exec('ALTER TABLE trade_reviews ADD COLUMN review_window_days INTEGER NOT NULL DEFAULT 1'); } catch {}
try { db.exec('ALTER TABLE trade_reviews ADD COLUMN horizon_end_at TEXT'); } catch {}

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
      call_cycle_start, call_net_sold, call_unspent_sell_limit, last_check, last_journal_generation, last_wiki_lint_run,
      last_trade_review_run, last_trade_review_success, last_trade_review_ready_count, last_trade_review_error, last_trade_review_targets,
      last_hypothesis_lesson_review_id, last_trade_lesson_review_id,
      last_advisory_run, last_advisory_success, last_advisory_error, advisory_retry_count, next_advisory_retry_at,
      last_advisory_spot_price, last_advisory_timestamp, updated_at)
    VALUES (1, @put_cycle_start, @put_net_bought, @put_unspent_buy_limit, @put_budget_for_cycle,
      @call_cycle_start, @call_net_sold, @call_unspent_sell_limit, @last_check, @last_journal_generation, @last_wiki_lint_run,
      @last_trade_review_run, @last_trade_review_success, @last_trade_review_ready_count, @last_trade_review_error, @last_trade_review_targets,
      @last_hypothesis_lesson_review_id, @last_trade_lesson_review_id,
      @last_advisory_run, @last_advisory_success, @last_advisory_error, @advisory_retry_count, @next_advisory_retry_at,
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
      last_wiki_lint_run = @last_wiki_lint_run,
      last_trade_review_run = @last_trade_review_run,
      last_trade_review_success = @last_trade_review_success,
      last_trade_review_ready_count = @last_trade_review_ready_count,
      last_trade_review_error = @last_trade_review_error,
      last_trade_review_targets = @last_trade_review_targets,
      last_hypothesis_lesson_review_id = @last_hypothesis_lesson_review_id,
      last_trade_lesson_review_id = @last_trade_lesson_review_id,
      last_advisory_run = @last_advisory_run,
      last_advisory_success = @last_advisory_success,
      last_advisory_error = @last_advisory_error,
      advisory_retry_count = @advisory_retry_count,
      next_advisory_retry_at = @next_advisory_retry_at,
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

  getBuyPutScoreSamples: db.prepare(`
    SELECT
      timestamp,
      MAX(ask_delta_value) as score
    FROM options_snapshots
    WHERE timestamp > @since
      AND timestamp < @before
      AND (option_type = 'P' OR instrument_name LIKE '%-P')
      AND delta <= @max_delta
      AND delta >= @min_delta
      AND ask_price > 0
      AND ask_delta_value > 0
      AND expiry IS NOT NULL
      AND ((expiry - strftime('%s', timestamp)) / 86400.0) >= @min_dte
      AND ((expiry - strftime('%s', timestamp)) / 86400.0) <= @max_dte
    GROUP BY timestamp
    ORDER BY timestamp ASC
  `),

  getBestBuyPutScoreDetail: db.prepare(`
    SELECT
      timestamp,
      instrument_name,
      delta,
      ask_price,
      strike,
      expiry,
      ask_delta_value as score,
      ((expiry - strftime('%s', timestamp)) / 86400.0) as dte
    FROM options_snapshots
    WHERE timestamp > @since
      AND timestamp < @before
      AND (option_type = 'P' OR instrument_name LIKE '%-P')
      AND delta <= @max_delta
      AND delta >= @min_delta
      AND ask_price > 0
      AND ask_delta_value > 0
      AND expiry IS NOT NULL
      AND ((expiry - strftime('%s', timestamp)) / 86400.0) >= @min_dte
      AND ((expiry - strftime('%s', timestamp)) / 86400.0) <= @max_dte
    ORDER BY ask_delta_value DESC
    LIMIT 1
  `),

  getSellCallScoreSamples: db.prepare(`
    SELECT
      timestamp,
      MAX(bid_delta_value) as score
    FROM options_snapshots
    WHERE timestamp > @since
      AND timestamp < @before
      AND (option_type = 'C' OR instrument_name LIKE '%-C')
      AND delta >= @min_delta
      AND delta <= @max_delta
      AND bid_price > 0
      AND bid_delta_value > 0
      AND expiry IS NOT NULL
      AND ((expiry - strftime('%s', timestamp)) / 86400.0) >= @min_dte
      AND ((expiry - strftime('%s', timestamp)) / 86400.0) <= @max_dte
    GROUP BY timestamp
    ORDER BY timestamp ASC
  `),

  getBestSellCallScoreDetail: db.prepare(`
    SELECT
      timestamp,
      instrument_name,
      delta,
      bid_price,
      strike,
      expiry,
      bid_delta_value as score,
      ((expiry - strftime('%s', timestamp)) / 86400.0) as dte
    FROM options_snapshots
    WHERE timestamp > @since
      AND timestamp < @before
      AND (option_type = 'C' OR instrument_name LIKE '%-C')
      AND delta >= @min_delta
      AND delta <= @max_delta
      AND bid_price > 0
      AND bid_delta_value > 0
      AND expiry IS NOT NULL
      AND ((expiry - strftime('%s', timestamp)) / 86400.0) >= @min_dte
      AND ((expiry - strftime('%s', timestamp)) / 86400.0) <= @max_dte
    ORDER BY bid_delta_value DESC
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

  getReviewedHypothesesSinceId: db.prepare(`
    SELECT id, timestamp, content, prediction_target, prediction_direction,
      prediction_value, outcome_status, outcome_verdict, outcome_confidence,
      trade_pnl_attribution, outcome_reviewed_at
    FROM ai_journal
    WHERE entry_type = 'hypothesis'
      AND outcome_status != 'pending'
      AND id > @after_id
    ORDER BY id ASC
    LIMIT @limit
  `),

  countReviewedSinceLastLesson: db.prepare(`
    SELECT COUNT(*) as count
    FROM ai_journal
    WHERE entry_type = 'hypothesis'
      AND outcome_status != 'pending'
      AND outcome_reviewed_at > COALESCE(
        (SELECT MAX(created_at) FROM hypothesis_lessons), '1970-01-01')
  `),

  getTradeReviewByInstrumentClosedAt: db.prepare(`
    SELECT id
    FROM trade_reviews
    WHERE instrument_name = @instrument_name
      AND closed_at = @closed_at
      AND review_window_days = @review_window_days
    LIMIT 1
  `),

  insertTradeReview: db.prepare(`
    INSERT INTO trade_reviews (
      instrument_name, action_family, opened_at, closed_at, review_window_days, horizon_end_at, order_ids,
      review_status, review_confidence, summary, lessons, pnl_realized,
      premium_opened, premium_closed, spot_open, spot_close,
      spot_min_while_open, spot_max_while_open, spot_min_after_close, spot_max_after_close
    ) VALUES (
      @instrument_name, @action_family, @opened_at, @closed_at, @review_window_days, @horizon_end_at, @order_ids,
      @review_status, @review_confidence, @summary, @lessons, @pnl_realized,
      @premium_opened, @premium_closed, @spot_open, @spot_close,
      @spot_min_while_open, @spot_max_while_open, @spot_min_after_close, @spot_max_after_close
    )
  `),

  getRecentTradeReviews: db.prepare(`
    SELECT id, instrument_name, action_family, opened_at, closed_at, review_window_days, horizon_end_at, order_ids,
      review_status, review_confidence, summary, lessons, pnl_realized,
      premium_opened, premium_closed, spot_open, spot_close,
      spot_min_while_open, spot_max_while_open, spot_min_after_close, spot_max_after_close,
      created_at
    FROM trade_reviews
    WHERE is_active = 1
    ORDER BY closed_at DESC
    LIMIT @limit
  `),

  getTradeReviewsSinceId: db.prepare(`
    SELECT id, instrument_name, action_family, opened_at, closed_at, review_window_days, horizon_end_at, order_ids,
      review_status, review_confidence, summary, lessons, pnl_realized,
      premium_opened, premium_closed, spot_open, spot_close,
      spot_min_while_open, spot_max_while_open, spot_min_after_close, spot_max_after_close,
      created_at
    FROM trade_reviews
    WHERE is_active = 1
      AND id > @after_id
    ORDER BY id ASC
    LIMIT @limit
  `),

  countReviewedSinceLastTradeLesson: db.prepare(`
    SELECT COUNT(*) as count
    FROM trade_reviews
    WHERE created_at > COALESCE(
      (SELECT MAX(created_at) FROM trade_lessons), '1970-01-01')
  `),

  insertTradeLesson: db.prepare(`
    INSERT INTO trade_lessons (lesson, evidence_count)
    VALUES (@lesson, @evidence_count)
  `),

  getActiveTradeLessons: db.prepare(`
    SELECT id, lesson, evidence_count, created_at
    FROM trade_lessons
    WHERE is_active = 1
    ORDER BY created_at DESC
  `),

  archiveTradeLesson: db.prepare(`
    UPDATE trade_lessons SET is_active = 0, archived_at = datetime('now')
    WHERE id = @id
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
      pending_action_id,
      delta, price, intended_amount, filled_amount, fill_price, total_value, spot_price, raw_response)
    VALUES (@timestamp, @action, @success, @reason, @instrument_name, @strike, @expiry,
      @pending_action_id,
      @delta, @price, @intended_amount, @filled_amount, @fill_price, @total_value, @spot_price, @raw_response)
  `),

  getRecentOrders: db.prepare(`
    SELECT * FROM orders WHERE timestamp > @since ORDER BY timestamp DESC LIMIT @limit
  `),

  getOrdersInRange: db.prepare(`
    SELECT * FROM orders
    WHERE timestamp >= @from AND timestamp <= @to
    ORDER BY timestamp ASC
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

  deactivateRuleById: db.prepare(`
    UPDATE trading_rules
    SET is_active = 0
    WHERE id = @id
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
    WHERE rule_id = @rule_id AND status IN ('pending', 'confirmed', 'resting')
  `),

  hasPendingOrConfirmedActionForRule: db.prepare(`
    SELECT COUNT(*) as count FROM pending_actions
    WHERE rule_id = @rule_id AND status IN ('pending', 'confirmed')
  `),

  getLastExecutedAction: db.prepare(`
    SELECT executed_at FROM pending_actions
    WHERE action = @action AND status = 'executed'
    ORDER BY executed_at DESC LIMIT 1
  `),

  getLastFailedAction: db.prepare(`
    SELECT triggered_at, execution_result FROM pending_actions
    WHERE action = @action
      AND instrument_name = @instrument_name
      AND status = 'failed'
    ORDER BY triggered_at DESC LIMIT 1
  `),

  getLastRejectedAction: db.prepare(`
    SELECT triggered_at, confirmation_reasoning, execution_result FROM pending_actions
    WHERE action = @action
      AND instrument_name = @instrument_name
      AND status = 'rejected'
    ORDER BY triggered_at DESC LIMIT 1
  `),

  // Resting order tracking
  insertRestingOrder: db.prepare(`
    INSERT OR IGNORE INTO resting_orders (order_id, pending_action_id, instrument_name, action, direction, amount, limit_price)
    VALUES (@order_id, @pending_action_id, @instrument_name, @action, @direction, @amount, @limit_price)
  `),
  getOpenRestingOrders: db.prepare(`
    SELECT ro.*, pa.rule_id AS rule_id
    FROM resting_orders ro
    LEFT JOIN pending_actions pa ON ro.pending_action_id = pa.id
    WHERE ro.status = 'open'
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
      COALESCE(SUM(CASE WHEN action IN ('sell_put','sell_call') AND success = 1 THEN total_value ELSE 0 END), 0)
      - COALESCE(SUM(CASE WHEN action IN ('buy_put','buyback_call') AND success = 1 THEN total_value ELSE 0 END), 0)
      as net_realized_pnl,
      COALESCE(SUM(CASE WHEN action = 'buy_put' AND success = 1 THEN total_value ELSE 0 END), 0) as total_put_cost,
      COALESCE(SUM(CASE WHEN action = 'sell_put' AND success = 1 THEN total_value ELSE 0 END), 0) as total_put_revenue,
      COALESCE(SUM(CASE WHEN action = 'sell_call' AND success = 1 THEN total_value ELSE 0 END), 0) as total_call_revenue,
      COALESCE(SUM(CASE WHEN action = 'buyback_call' AND success = 1 THEN total_value ELSE 0 END), 0) as total_call_cost,
      COUNT(CASE WHEN success = 1 THEN 1 END) as successful_orders,
      COUNT(*) as total_orders
    FROM orders
  `),

  insertCandidateObservation: db.prepare(`
    INSERT INTO candidate_observations (
      observed_at, tick_id, rule_id, pending_action_id, action, instrument_name, option_type,
      candidate_rank, selected, decision_status, selection_reason, spot_price, strike, expiry, dte, delta,
      bid_price, ask_price, mark_price, bid_amount, ask_amount, spread_pct, depth, implied_vol, open_interest,
      raw_score, selection_score, score_band, dte_bucket, research_recommendation, score_trend_24h_pct,
      spot_ret_6h, spot_ret_24h, rule_min_score, rule_min_bid, rule_dte_min, rule_dte_max,
      rule_delta_min, rule_delta_max, metadata
    ) VALUES (
      @observed_at, @tick_id, @rule_id, @pending_action_id, @action, @instrument_name, @option_type,
      @candidate_rank, @selected, @decision_status, @selection_reason, @spot_price, @strike, @expiry, @dte, @delta,
      @bid_price, @ask_price, @mark_price, @bid_amount, @ask_amount, @spread_pct, @depth, @implied_vol, @open_interest,
      @raw_score, @selection_score, @score_band, @dte_bucket, @research_recommendation, @score_trend_24h_pct,
      @spot_ret_6h, @spot_ret_24h, @rule_min_score, @rule_min_bid, @rule_dte_min, @rule_dte_max,
      @rule_delta_min, @rule_delta_max, @metadata
    )
  `),

  insertDecisionOutcome: db.prepare(`
    INSERT OR IGNORE INTO decision_outcomes (observation_id, horizon_hours, due_at)
    VALUES (@observation_id, @horizon_hours, @due_at)
  `),

  getDueDecisionOutcomes: db.prepare(`
    SELECT
      o.id as outcome_id,
      o.horizon_hours,
      o.due_at,
      c.id as observation_id,
      c.observed_at,
      c.action,
      c.instrument_name,
      c.option_type,
      c.spot_price,
      c.bid_price,
      c.ask_price,
      c.mark_price,
      c.delta
    FROM decision_outcomes o
    JOIN candidate_observations c ON c.id = o.observation_id
    WHERE o.status = 'pending'
      AND o.due_at <= @now
    ORDER BY o.due_at ASC
    LIMIT @limit
  `),

  getOutcomeFutureQuote: db.prepare(`
    SELECT timestamp, bid_price, ask_price, mark_price, delta, bid_delta_value, ask_delta_value
    FROM options_snapshots
    WHERE instrument_name = @instrument_name
      AND timestamp >= @due_at
      AND timestamp < @until
    ORDER BY timestamp ASC
    LIMIT 1
  `),

  getOutcomeFutureSpot: db.prepare(`
    SELECT timestamp, price
    FROM spot_prices
    WHERE timestamp >= @due_at
      AND timestamp < @until
      AND price BETWEEN 100 AND 20000
    ORDER BY timestamp ASC
    LIMIT 1
  `),

  getOutcomeSpotPath: db.prepare(`
    SELECT MIN(price) as spot_min, MAX(price) as spot_max
    FROM spot_prices
    WHERE timestamp >= @observed_at
      AND timestamp <= @due_at
      AND price BETWEEN 100 AND 20000
  `),

  updateDecisionOutcome: db.prepare(`
    UPDATE decision_outcomes SET
      evaluated_at = @evaluated_at,
      status = @status,
      future_quote_at = @future_quote_at,
      future_spot_at = @future_spot_at,
      future_spot = @future_spot,
      spot_return = @spot_return,
      future_bid = @future_bid,
      future_ask = @future_ask,
      future_mark = @future_mark,
      future_score = @future_score,
      sell_entry_pnl = @sell_entry_pnl,
      sell_capture_pct = @sell_capture_pct,
      buy_entry_pnl = @buy_entry_pnl,
      buy_capture_pct = @buy_capture_pct,
      spot_min = @spot_min,
      spot_max = @spot_max,
      error = @error
    WHERE id = @id
  `),

  insertRuleDecision: db.prepare(`
    INSERT INTO rule_decisions (
      evaluated_at, rule_id, rule_type, action, instrument_name, decision_status,
      reason_code, reason, candidates_evaluated, selected_instrument, raw_score,
      selection_score, price, amount, spot_price, pending_action_id, criteria_json, context_json
    ) VALUES (
      @evaluated_at, @rule_id, @rule_type, @action, @instrument_name, @decision_status,
      @reason_code, @reason, @candidates_evaluated, @selected_instrument, @raw_score,
      @selection_score, @price, @amount, @spot_price, @pending_action_id, @criteria_json, @context_json
    )
  `),

  getLifecycleOrderRows: db.prepare(`
    SELECT id, timestamp, action, instrument_name, strike, expiry, filled_amount, fill_price, total_value, spot_price
    FROM orders
    WHERE success = 1
      AND COALESCE(filled_amount, 0) > 0
      AND action IN ('buy_put', 'sell_put', 'sell_call', 'buyback_call')
    ORDER BY timestamp ASC
  `),

  upsertPositionLifecycle: db.prepare(`
    INSERT INTO position_lifecycle (
      instrument_name, action_family, option_type, strike, expiry, opened_at, closed_at, status,
      opened_amount, closed_amount, net_amount, premium_opened, premium_closed, net_credit,
      avg_open_price, avg_close_price, spot_open, spot_close, order_ids, updated_at
    ) VALUES (
      @instrument_name, @action_family, @option_type, @strike, @expiry, @opened_at, @closed_at, @status,
      @opened_amount, @closed_amount, @net_amount, @premium_opened, @premium_closed, @net_credit,
      @avg_open_price, @avg_close_price, @spot_open, @spot_close, @order_ids, datetime('now')
    )
    ON CONFLICT(instrument_name, action_family) DO UPDATE SET
      option_type = @option_type,
      strike = @strike,
      expiry = @expiry,
      opened_at = @opened_at,
      closed_at = @closed_at,
      status = @status,
      opened_amount = @opened_amount,
      closed_amount = @closed_amount,
      net_amount = @net_amount,
      premium_opened = @premium_opened,
      premium_closed = @premium_closed,
      net_credit = @net_credit,
      avg_open_price = @avg_open_price,
      avg_close_price = @avg_close_price,
      spot_open = @spot_open,
      spot_close = @spot_close,
      order_ids = @order_ids,
      updated_at = datetime('now')
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

const toNum = (v) => {
  if (v == null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};
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
const getBuyPutScoreSamples = ({
  since,
  before,
  minDelta = -0.12,
  maxDelta = -0.02,
  minDte = 45,
  maxDte = 75,
}) => stmts.getBuyPutScoreSamples.all({
  since,
  before,
  min_delta: minDelta,
  max_delta: maxDelta,
  min_dte: minDte,
  max_dte: maxDte,
});
const getBestBuyPutScoreDetail = ({
  since,
  before,
  minDelta = -0.12,
  maxDelta = -0.02,
  minDte = 45,
  maxDte = 75,
}) => stmts.getBestBuyPutScoreDetail.get({
  since,
  before,
  min_delta: minDelta,
  max_delta: maxDelta,
  min_dte: minDte,
  max_dte: maxDte,
}) || null;

const getSellCallScoreSamples = ({
  since,
  before,
  minDelta = 0.04,
  maxDelta = 0.12,
  minDte = 5,
  maxDte = 12,
}) => stmts.getSellCallScoreSamples.all({
  since,
  before,
  min_delta: minDelta,
  max_delta: maxDelta,
  min_dte: minDte,
  max_dte: maxDte,
});
const getBestSellCallScoreDetail = ({
  since,
  before,
  minDelta = 0.04,
  maxDelta = 0.12,
  minDte = 5,
  maxDte = 12,
}) => stmts.getBestSellCallScoreDetail.get({
  since,
  before,
  min_delta: minDelta,
  max_delta: maxDelta,
  min_dte: minDte,
  max_dte: maxDte,
}) || null;

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
    pending_action_id: data.pending_action_id ?? null,
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
const getOrdersInRange = (from, to) => stmts.getOrdersInRange.all({ from, to });

const DECISION_OUTCOME_HORIZONS_HOURS = [1, 6, 24, 48, 72];
const addHoursIso = (iso, hours) => new Date(new Date(iso).getTime() + hours * 60 * 60 * 1000).toISOString();

const insertCandidateObservations = (observations = [], horizons = DECISION_OUTCOME_HORIZONS_HOURS) => {
  if (!Array.isArray(observations) || observations.length === 0) return { inserted: 0, outcomes: 0 };
  let inserted = 0;
  let outcomes = 0;
  const insert = db.transaction((items) => {
    for (const item of items) {
      if (!item?.action || !item?.instrument_name) continue;
      const observedAt = item.observed_at || new Date().toISOString();
      const result = stmts.insertCandidateObservation.run({
        observed_at: observedAt,
        tick_id: item.tick_id || null,
        rule_id: item.rule_id ?? null,
        pending_action_id: item.pending_action_id ?? null,
        action: item.action,
        instrument_name: item.instrument_name,
        option_type: item.option_type || null,
        candidate_rank: item.candidate_rank ?? null,
        selected: item.selected ? 1 : 0,
        decision_status: item.decision_status || null,
        selection_reason: item.selection_reason || null,
        spot_price: toNum(item.spot_price),
        strike: toNum(item.strike),
        expiry: item.expiry ?? null,
        dte: toNum(item.dte),
        delta: toNum(item.delta),
        bid_price: toNum(item.bid_price),
        ask_price: toNum(item.ask_price),
        mark_price: toNum(item.mark_price),
        bid_amount: toNum(item.bid_amount),
        ask_amount: toNum(item.ask_amount),
        spread_pct: toNum(item.spread_pct),
        depth: toNum(item.depth),
        implied_vol: toNum(item.implied_vol),
        open_interest: toNum(item.open_interest),
        raw_score: toNum(item.raw_score),
        selection_score: toNum(item.selection_score),
        score_band: item.score_band || null,
        dte_bucket: item.dte_bucket || null,
        research_recommendation: item.research_recommendation || null,
        score_trend_24h_pct: toNum(item.score_trend_24h_pct),
        spot_ret_6h: toNum(item.spot_ret_6h),
        spot_ret_24h: toNum(item.spot_ret_24h),
        rule_min_score: toNum(item.rule_min_score),
        rule_min_bid: toNum(item.rule_min_bid),
        rule_dte_min: toNum(item.rule_dte_min),
        rule_dte_max: toNum(item.rule_dte_max),
        rule_delta_min: toNum(item.rule_delta_min),
        rule_delta_max: toNum(item.rule_delta_max),
        metadata: item.metadata ? (typeof item.metadata === 'string' ? item.metadata : JSON.stringify(item.metadata)) : null,
      });
      inserted++;
      const observationId = result.lastInsertRowid;
      for (const horizon of horizons) {
        if (!(Number(horizon) > 0)) continue;
        const dueAt = addHoursIso(observedAt, Number(horizon));
        stmts.insertDecisionOutcome.run({
          observation_id: observationId,
          horizon_hours: Number(horizon),
          due_at: dueAt,
        });
        outcomes++;
      }
    }
  });
  insert(observations);
  return { inserted, outcomes };
};

const evaluateDueDecisionOutcomes = ({ now = new Date().toISOString(), limit = 500, quoteWindowHours = 6 } = {}) => {
  const due = stmts.getDueDecisionOutcomes.all({ now, limit });
  let evaluated = 0;
  let missing = 0;
  const update = db.transaction((items) => {
    for (const row of items) {
      const until = addHoursIso(row.due_at, quoteWindowHours);
      const quote = stmts.getOutcomeFutureQuote.get({
        instrument_name: row.instrument_name,
        due_at: row.due_at,
        until,
      }) || null;
      const spot = stmts.getOutcomeFutureSpot.get({
        due_at: row.due_at,
        until,
      }) || null;
      const path = stmts.getOutcomeSpotPath.get({
        observed_at: row.observed_at,
        due_at: row.due_at,
      }) || {};
      const entryBid = toNum(row.bid_price);
      const entryAsk = toNum(row.ask_price);
      const futureBid = toNum(quote?.bid_price);
      const futureAsk = toNum(quote?.ask_price);
      const futureMark = toNum(quote?.mark_price);
      const absDelta = Math.abs(toNum(quote?.delta) || toNum(row.delta) || 0);
      const isCall = row.action === 'sell_call' || row.option_type === 'C' || row.instrument_name?.endsWith('-C');
      const futureScore = isCall
        ? (toNum(quote?.bid_delta_value) ?? (futureBid != null && absDelta > 0 ? futureBid / absDelta : null))
        : (toNum(quote?.ask_delta_value) ?? (futureAsk != null && absDelta > 0 ? absDelta / futureAsk : null));
      const futureSpot = toNum(spot?.price);
      const entrySpot = toNum(row.spot_price);
      const sellEntryPnl = entryBid != null && futureAsk != null ? entryBid - futureAsk : null;
      const buyEntryPnl = entryAsk != null && futureBid != null ? futureBid - entryAsk : null;
      const status = quote || spot ? 'evaluated' : 'missing';
      if (status === 'evaluated') evaluated++;
      else missing++;
      stmts.updateDecisionOutcome.run({
        id: row.outcome_id,
        evaluated_at: now,
        status,
        future_quote_at: quote?.timestamp || null,
        future_spot_at: spot?.timestamp || null,
        future_spot: futureSpot,
        spot_return: futureSpot != null && entrySpot > 0 ? (futureSpot / entrySpot) - 1 : null,
        future_bid: futureBid,
        future_ask: futureAsk,
        future_mark: futureMark,
        future_score: futureScore,
        sell_entry_pnl: sellEntryPnl,
        sell_capture_pct: sellEntryPnl != null && entryBid > 0 ? sellEntryPnl / entryBid : null,
        buy_entry_pnl: buyEntryPnl,
        buy_capture_pct: buyEntryPnl != null && entryAsk > 0 ? buyEntryPnl / entryAsk : null,
        spot_min: toNum(path.spot_min),
        spot_max: toNum(path.spot_max),
        error: status === 'missing' ? `No quote or spot found within ${quoteWindowHours}h after due_at` : null,
      });
    }
  });
  update(due);
  return { scanned: due.length, evaluated, missing };
};

const insertRuleDecision = (decision = {}) => {
  if (!decision.decision_status) return null;
  return stmts.insertRuleDecision.run({
    evaluated_at: decision.evaluated_at || new Date().toISOString(),
    rule_id: decision.rule_id ?? null,
    rule_type: decision.rule_type || null,
    action: decision.action || null,
    instrument_name: decision.instrument_name || null,
    decision_status: decision.decision_status,
    reason_code: decision.reason_code || null,
    reason: decision.reason || null,
    candidates_evaluated: decision.candidates_evaluated ?? null,
    selected_instrument: decision.selected_instrument || null,
    raw_score: toNum(decision.raw_score),
    selection_score: toNum(decision.selection_score),
    price: toNum(decision.price),
    amount: toNum(decision.amount),
    spot_price: toNum(decision.spot_price),
    pending_action_id: decision.pending_action_id ?? null,
    criteria_json: decision.criteria_json
      ? (typeof decision.criteria_json === 'string' ? decision.criteria_json : JSON.stringify(decision.criteria_json))
      : null,
    context_json: decision.context_json
      ? (typeof decision.context_json === 'string' ? decision.context_json : JSON.stringify(decision.context_json))
      : null,
  });
};

const parseLifecycleInstrument = (instrumentName) => {
  const parts = String(instrumentName || '').split('-');
  if (parts.length !== 4 || !/^\d{8}$/.test(parts[1])) return {};
  const expiryDate = new Date(`${parts[1].slice(0, 4)}-${parts[1].slice(4, 6)}-${parts[1].slice(6, 8)}T08:00:00Z`);
  return {
    strike: toNum(parts[2]),
    expiry: Number.isFinite(expiryDate.getTime()) ? Math.floor(expiryDate.getTime() / 1000) : null,
    optionType: parts[3],
  };
};

const refreshPositionLifecycle = ({ nowMs = Date.now() } = {}) => {
  const rows = stmts.getLifecycleOrderRows.all();
  const groups = new Map();
  for (const row of rows) {
    const actionFamily = ['sell_call', 'buyback_call'].includes(row.action)
      ? 'short_call'
      : ['buy_put', 'sell_put'].includes(row.action)
        ? 'long_put'
        : null;
    if (!actionFamily || !row.instrument_name) continue;
    const key = `${actionFamily}:${row.instrument_name}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  const upsert = db.transaction((items) => {
    for (const [key, group] of items) {
      const [actionFamily, instrumentName] = key.split(':');
      const parsed = parseLifecycleInstrument(instrumentName);
      const openActions = actionFamily === 'short_call' ? new Set(['sell_call']) : new Set(['buy_put']);
      const closeActions = actionFamily === 'short_call' ? new Set(['buyback_call']) : new Set(['sell_put']);
      const openRows = group.filter((row) => openActions.has(row.action));
      const closeRows = group.filter((row) => closeActions.has(row.action));
      const openedAmount = openRows.reduce((sum, row) => sum + (toNum(row.filled_amount) || 0), 0);
      const closedAmount = closeRows.reduce((sum, row) => sum + (toNum(row.filled_amount) || 0), 0);
      const premiumOpened = openRows.reduce((sum, row) => sum + (toNum(row.total_value) || 0), 0);
      const premiumClosed = closeRows.reduce((sum, row) => sum + (toNum(row.total_value) || 0), 0);
      const netAmount = openedAmount - closedAmount;
      const expiryMs = parsed.expiry ? parsed.expiry * 1000 : null;
      const expired = expiryMs != null && expiryMs <= nowMs;
      const status = netAmount <= 1e-9
        ? 'closed'
        : expired
          ? 'expired'
          : 'open';
      const openedAt = openRows[0]?.timestamp || group[0]?.timestamp || null;
      const closedAt = status === 'closed'
        ? (closeRows.at(-1)?.timestamp || null)
        : status === 'expired' && expiryMs != null
          ? new Date(expiryMs).toISOString()
          : null;
      const netCredit = actionFamily === 'short_call'
        ? premiumOpened - premiumClosed
        : premiumClosed - premiumOpened;
      stmts.upsertPositionLifecycle.run({
        instrument_name: instrumentName,
        action_family: actionFamily,
        option_type: parsed.optionType || null,
        strike: parsed.strike ?? toNum(group.find((row) => row.strike != null)?.strike),
        expiry: parsed.expiry ?? null,
        opened_at: openedAt,
        closed_at: closedAt,
        status,
        opened_amount: openedAmount,
        closed_amount: closedAmount,
        net_amount: netAmount,
        premium_opened: premiumOpened,
        premium_closed: premiumClosed,
        net_credit: netCredit,
        avg_open_price: openedAmount > 0 ? premiumOpened / openedAmount : null,
        avg_close_price: closedAmount > 0 ? premiumClosed / closedAmount : null,
        spot_open: toNum(openRows.find((row) => row.spot_price != null)?.spot_price),
        spot_close: toNum(closeRows.at(-1)?.spot_price),
        order_ids: JSON.stringify(group.map((row) => row.id)),
      });
    }
  });
  upsert([...groups.entries()]);
  return { refreshed: groups.size };
};

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

const getFundingRates = (since, symbol = 'ETHUSDT') => {
  return getFundingRatesHourly(since, symbol);
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
const getReviewedHypothesesSinceId = (afterId = 0, limit = 20) => {
  return stmts.getReviewedHypothesesSinceId.all({ after_id: afterId, limit });
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

const hasTradeReview = (instrumentName, closedAt, reviewWindowDays) => {
  return !!stmts.getTradeReviewByInstrumentClosedAt.get({
    instrument_name: instrumentName,
    closed_at: closedAt,
    review_window_days: reviewWindowDays,
  });
};

const insertTradeReview = (review) => {
  stmts.insertTradeReview.run({
    instrument_name: review.instrument_name,
    action_family: review.action_family,
    opened_at: review.opened_at || null,
    closed_at: review.closed_at,
    review_window_days: review.review_window_days || 1,
    horizon_end_at: review.horizon_end_at || null,
    order_ids: JSON.stringify(review.order_ids || []),
    review_status: review.review_status,
    review_confidence: review.review_confidence ?? null,
    summary: review.summary,
    lessons: review.lessons ? JSON.stringify(review.lessons) : null,
    pnl_realized: review.pnl_realized ?? null,
    premium_opened: review.premium_opened ?? null,
    premium_closed: review.premium_closed ?? null,
    spot_open: review.spot_open ?? null,
    spot_close: review.spot_close ?? null,
    spot_min_while_open: review.spot_min_while_open ?? null,
    spot_max_while_open: review.spot_max_while_open ?? null,
    spot_min_after_close: review.spot_min_after_close ?? null,
    spot_max_after_close: review.spot_max_after_close ?? null,
  });
};

const getRecentTradeReviews = (limit = 20) => {
  return stmts.getRecentTradeReviews.all({ limit });
};
const getTradeReviewsSinceId = (afterId = 0, limit = 20) => {
  return stmts.getTradeReviewsSinceId.all({ after_id: afterId, limit });
};

const countReviewedSinceLastTradeLesson = () => {
  return stmts.countReviewedSinceLastTradeLesson.get()?.count || 0;
};

const insertTradeLesson = (lesson, evidenceCount) => {
  stmts.insertTradeLesson.run({ lesson, evidence_count: evidenceCount });
};

const getActiveTradeLessons = () => {
  return stmts.getActiveTradeLessons.all();
};

const archiveTradeLesson = (id) => {
  stmts.archiveTradeLesson.run({ id });
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
const deactivateRuleById = (id) => stmts.deactivateRuleById.run({ id }).changes || 0;
const getPendingActions = (status) => stmts.getPendingActionsByStatus.all({ status });
const getRecentPendingActions = (limit = 20) => stmts.getRecentPendingActions.all({ limit });
const hasPendingActionForRule = (ruleId) => (stmts.hasPendingActionForRule.get({ rule_id: ruleId })?.count || 0) > 0;
const hasPendingOrConfirmedActionForRule = (ruleId) => (stmts.hasPendingOrConfirmedActionForRule.get({ rule_id: ruleId })?.count || 0) > 0;
const getLastExecutedAction = (action) => stmts.getLastExecutedAction.get({ action })?.executed_at || null;
const getLastFailedAction = (action, instrumentName) => stmts.getLastFailedAction.get({ action, instrument_name: instrumentName }) || null;
const getLastRejectedAction = (action, instrumentName) => stmts.getLastRejectedAction.get({ action, instrument_name: instrumentName }) || null;

// ─── Resting Order Helpers ──────────────────────────────────────────────────

const insertRestingOrder = (order) => {
  stmts.insertRestingOrder.run({
    order_id: order.order_id,
    pending_action_id: order.pending_action_id ?? null,
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
    last_wiki_lint_run: botData.lastWikiLintRun || 0,
    last_trade_review_run: botData.lastTradeReviewRun || 0,
    last_trade_review_success: botData.lastTradeReviewSuccess || 0,
    last_trade_review_ready_count: botData.lastTradeReviewReadyCount || 0,
    last_trade_review_error: botData.lastTradeReviewError || null,
    last_trade_review_targets: botData.lastTradeReviewTargets ? JSON.stringify(botData.lastTradeReviewTargets) : null,
    last_hypothesis_lesson_review_id: botData.lastHypothesisLessonReviewId || 0,
    last_trade_lesson_review_id: botData.lastTradeLessonReviewId || 0,
    last_advisory_run: botData.lastAdvisoryRun || 0,
    last_advisory_success: botData.lastAdvisorySuccess || 0,
    last_advisory_error: botData.lastAdvisoryError || null,
    advisory_retry_count: botData.advisoryRetryCount || 0,
    next_advisory_retry_at: botData.nextAdvisoryRetryAt || 0,
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
  getBuyPutScoreSamples,
  getBestBuyPutScoreDetail,
  getSellCallScoreSamples,
  getBestSellCallScoreDetail,
  insertOrder,
  getRecentOrders,
  getOrdersInRange,
  insertCandidateObservations,
  evaluateDueDecisionOutcomes,
  insertRuleDecision,
  refreshPositionLifecycle,
  insertJournalEntry,
  getRecentJournalEntries,
  insertJournalEntryFull,
  getPendingHypotheses,
  updateHypothesisVerdict,
  getReviewedHypotheses,
  getReviewedHypothesesSinceId,
  getHypothesisStats,
  getOrdersInWindow,
  insertLesson,
  getActiveLessons,
  archiveLesson,
  countReviewedSinceLastLesson,
  hasTradeReview,
  insertTradeReview,
  getRecentTradeReviews,
  getTradeReviewsSinceId,
  countReviewedSinceLastTradeLesson,
  insertTradeLesson,
  getActiveTradeLessons,
  archiveTradeLesson,
  saveBotState,
  loadBotState,
  loadPriceHistoryFromDb,
  migrateFromJson,
  insertOISnapshot,
  insertFundingRates,
  getFundingRates,
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
  deactivateRuleById,
  getPendingActions,
  getRecentPendingActions,
  hasPendingActionForRule,
  hasPendingOrConfirmedActionForRule,
  getLastExecutedAction,
  getLastFailedAction,
  getLastRejectedAction,
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
