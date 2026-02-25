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
    call_cycle_start INTEGER,
    call_net_sold REAL NOT NULL DEFAULT 0,
    call_unspent_sell_limit REAL NOT NULL DEFAULT 0,
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
      ask_delta_value, bid_delta_value)
    VALUES (@timestamp, @instrument_name, @strike, @expiry, @option_type,
      @delta, @ask_price, @bid_price, @ask_amount, @bid_amount, @mark_price, @index_price,
      @ask_delta_value, @bid_delta_value)
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
    INSERT INTO bot_state (id, put_cycle_start, put_net_bought, put_unspent_buy_limit,
      call_cycle_start, call_net_sold, call_unspent_sell_limit, updated_at)
    VALUES (1, @put_cycle_start, @put_net_bought, @put_unspent_buy_limit,
      @call_cycle_start, @call_net_sold, @call_unspent_sell_limit, datetime('now'))
    ON CONFLICT(id) DO UPDATE SET
      put_cycle_start = @put_cycle_start,
      put_net_bought = @put_net_bought,
      put_unspent_buy_limit = @put_unspent_buy_limit,
      call_cycle_start = @call_cycle_start,
      call_net_sold = @call_net_sold,
      call_unspent_sell_limit = @call_unspent_sell_limit,
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
    SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
           AVG(liquidity_flow_magnitude) as avg_magnitude,
           AVG(exhaustion_score) as avg_exhaustion,
           (SELECT liquidity_flow_direction FROM onchain_data o2
            WHERE strftime('%Y-%m-%dT%H:00:00Z', o2.timestamp) = strftime('%Y-%m-%dT%H:00:00Z', onchain_data.timestamp)
              AND o2.timestamp > @since
            GROUP BY liquidity_flow_direction
            ORDER BY COUNT(*) DESC LIMIT 1) as direction
    FROM onchain_data WHERE timestamp > @since GROUP BY hour ORDER BY hour ASC
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

  // 7-day average premium for call selling elevation check
  getAvgCallPremium7d: db.prepare(`
    SELECT AVG(bid_price) as avg_premium
    FROM options_snapshots
    WHERE option_type = 'call'
      AND timestamp > @since
      AND bid_price > 0
  `),
};

// ─── Helper Functions ─────────────────────────────────────────────────────────

const insertSpotPrice = (spotPrice, momentumResult, botData, timestamp) => {
  const shortMomentum = typeof momentumResult.shortTermMomentum === 'object'
    ? momentumResult.shortTermMomentum : { main: momentumResult.shortTermMomentum };
  const medMomentum = typeof momentumResult.mediumTermMomentum === 'object'
    ? momentumResult.mediumTermMomentum : { main: momentumResult.mediumTermMomentum };

  return stmts.insertSpotPrice.run({
    timestamp: timestamp || new Date().toISOString(),
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
};

const toNum = (v) => v != null && v !== '' ? Number(v) : null;

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
      });
    }
  });
  insert(options);
};

const insertOnchainData = (analysis) => {
  const flow = analysis.dexLiquidity?.flowAnalysis || {};
  const exhaustion = analysis.exhaustionAnalysis || {};

  stmts.insertOnchainData.run({
    timestamp: analysis.timestamp || new Date().toISOString(),
    spot_price: analysis.spotPrice || null,
    liquidity_flow_direction: flow.direction || null,
    liquidity_flow_magnitude: toNum(flow.magnitude),
    liquidity_flow_confidence: toNum(flow.confidence),
    exhaustion_score: toNum(exhaustion.metrics?.compositeScore ?? exhaustion.metrics?.overallExhaustionScore),
    exhaustion_alert_level: exhaustion.alertLevel || null,
    raw_data: JSON.stringify(analysis),
  });
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

const getOptionsDistribution = (since) => stmts.getOptionsDistribution.all({ since });

// Hourly helpers for correlation engine
const getSpotPricesHourly = (since) => stmts.getSpotPricesHourly.all({ since });
const getOnchainHourly = (since) => stmts.getOnchainHourly.all({ since });
const getBestPutDvHourly = (since) => stmts.getBestPutDvHourly.all({ since });
const getBestCallDvHourly = (since) => stmts.getBestCallDvHourly.all({ since });

const getAvgCallPremium7d = () => {
  const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  return stmts.getAvgCallPremium7d.get({ since });
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

// ─── Bot State Helpers ────────────────────────────────────────────────────────

const saveBotState = (botData) => {
  stmts.upsertBotState.run({
    put_cycle_start: botData.putCycleStart || null,
    put_net_bought: botData.putNetBought || 0,
    put_unspent_buy_limit: botData.putUnspentBuyLimit || 0,
    call_cycle_start: botData.callCycleStart || null,
    call_net_sold: botData.callNetSold || 0,
    call_unspent_sell_limit: botData.callUnspentSellLimit || 0,
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
  insertTick,
  getRecentTicks,
  insertJournalEntry,
  getRecentJournalEntries,
  saveBotState,
  loadBotState,
  loadPriceHistoryFromDb,
  migrateFromJson,
  close,
};
