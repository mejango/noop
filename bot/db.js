const Database = require('better-sqlite3');
const path = require('path');
const { DB_PATH, DATA_DIR } = require('./config');
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

  CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_name TEXT NOT NULL,
    base_asset_address TEXT,
    base_asset_sub_id TEXT,
    direction TEXT NOT NULL,
    strike REAL,
    expiry INTEGER,
    amount REAL NOT NULL DEFAULT 0,
    avg_price REAL,
    total_cost REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'open',
    pnl REAL,
    rolled_to_id INTEGER REFERENCES positions(id),
    rolled_from_id INTEGER REFERENCES positions(id),
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    close_reason TEXT
  );

  CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id INTEGER REFERENCES positions(id),
    instrument_name TEXT NOT NULL,
    direction TEXT NOT NULL,
    amount REAL NOT NULL,
    price REAL NOT NULL,
    total_value REAL NOT NULL,
    fee REAL,
    order_type TEXT NOT NULL,
    reason TEXT,
    timestamp TEXT NOT NULL,
    order_response TEXT
  );

  CREATE TABLE IF NOT EXISTS onchain_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    spot_price REAL,
    liquidity_flow_direction TEXT,
    liquidity_flow_magnitude REAL,
    liquidity_flow_confidence REAL,
    whale_count INTEGER,
    whale_total_txns INTEGER,
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
  CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
  CREATE INDEX IF NOT EXISTS idx_positions_instrument ON positions(instrument_name);
  CREATE INDEX IF NOT EXISTS idx_trades_position ON trades(position_id);
  CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
  CREATE INDEX IF NOT EXISTS idx_onchain_data_timestamp ON onchain_data(timestamp);
  CREATE INDEX IF NOT EXISTS idx_strategy_signals_type ON strategy_signals(signal_type);
  CREATE INDEX IF NOT EXISTS idx_strategy_signals_timestamp ON strategy_signals(timestamp);

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

  insertPosition: db.prepare(`
    INSERT INTO positions (instrument_name, base_asset_address, base_asset_sub_id, direction,
      strike, expiry, amount, avg_price, total_cost, status, opened_at, rolled_from_id)
    VALUES (@instrument_name, @base_asset_address, @base_asset_sub_id, @direction,
      @strike, @expiry, @amount, @avg_price, @total_cost, 'open', @opened_at, @rolled_from_id)
  `),

  updatePositionAmount: db.prepare(`
    UPDATE positions SET amount = amount + @additional_amount,
      total_cost = total_cost + @additional_cost,
      avg_price = (total_cost + @additional_cost) / (amount + @additional_amount)
    WHERE id = @id
  `),

  closePosition: db.prepare(`
    UPDATE positions SET status = @status, pnl = @pnl, closed_at = @closed_at,
      close_reason = @close_reason
    WHERE id = @id
  `),

  setRolledTo: db.prepare(`
    UPDATE positions SET rolled_to_id = @rolled_to_id WHERE id = @id
  `),

  insertTrade: db.prepare(`
    INSERT INTO trades (position_id, instrument_name, direction, amount, price, total_value,
      fee, order_type, reason, timestamp, order_response)
    VALUES (@position_id, @instrument_name, @direction, @amount, @price, @total_value,
      @fee, @order_type, @reason, @timestamp, @order_response)
  `),

  insertOnchainData: db.prepare(`
    INSERT INTO onchain_data (timestamp, spot_price, liquidity_flow_direction,
      liquidity_flow_magnitude, liquidity_flow_confidence, whale_count, whale_total_txns,
      exhaustion_score, exhaustion_alert_level, raw_data)
    VALUES (@timestamp, @spot_price, @liquidity_flow_direction,
      @liquidity_flow_magnitude, @liquidity_flow_confidence, @whale_count, @whale_total_txns,
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
  getOpenPositions: db.prepare(`
    SELECT * FROM positions WHERE status = 'open' ORDER BY opened_at DESC
  `),

  getOpenPositionByInstrument: db.prepare(`
    SELECT * FROM positions WHERE instrument_name = @instrument_name AND status = 'open' LIMIT 1
  `),

  getRecentSpotPrices: db.prepare(`
    SELECT * FROM spot_prices WHERE timestamp > @since ORDER BY timestamp DESC
  `),

  getRecentSignals: db.prepare(`
    SELECT * FROM strategy_signals WHERE timestamp > @since ORDER BY timestamp DESC LIMIT @limit
  `),

  getPositionsByStatus: db.prepare(`
    SELECT * FROM positions WHERE status = @status ORDER BY opened_at DESC
  `),

  getAllPositions: db.prepare(`
    SELECT * FROM positions ORDER BY opened_at DESC
  `),

  getTradesForPosition: db.prepare(`
    SELECT * FROM trades WHERE position_id = @position_id ORDER BY timestamp ASC
  `),

  getRecentTrades: db.prepare(`
    SELECT t.*, p.strike, p.expiry, p.direction as position_direction
    FROM trades t LEFT JOIN positions p ON t.position_id = p.id
    WHERE t.timestamp > @since ORDER BY t.timestamp DESC LIMIT @limit
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
      (SELECT COUNT(*) FROM positions WHERE status = 'open' AND direction = 'buy') as open_puts,
      (SELECT COUNT(*) FROM positions WHERE status = 'open' AND direction = 'sell') as open_calls,
      (SELECT COUNT(*) FROM positions) as total_positions,
      (SELECT COUNT(*) FROM trades) as total_trades,
      (SELECT price FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as last_price,
      (SELECT timestamp FROM spot_prices ORDER BY timestamp DESC LIMIT 1) as last_price_time
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

  getOpenPositionsByDirection: db.prepare(`
    SELECT * FROM positions WHERE status = 'open' AND direction = @direction
    ORDER BY opened_at DESC
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

const insertOptionsSnapshotBatch = (options, timestamp) => {
  const insert = db.transaction((opts) => {
    for (const opt of opts) {
      stmts.insertOptionsSnapshot.run({
        timestamp,
        instrument_name: opt.instrument_name || '',
        strike: opt.option_details?.strike ? Number(opt.option_details.strike) : null,
        expiry: opt.option_details?.expiry || null,
        option_type: opt.option_details?.type || (opt.instrument_name?.includes('-P') ? 'put' : 'call'),
        delta: opt.details?.delta ? Number(opt.details.delta) : null,
        ask_price: opt.details?.askPrice ? Number(opt.details.askPrice) : null,
        bid_price: opt.details?.bidPrice ? Number(opt.details.bidPrice) : null,
        ask_amount: opt.details?.askAmount ? Number(opt.details.askAmount) : null,
        bid_amount: opt.details?.bidAmount ? Number(opt.details.bidAmount) : null,
        mark_price: opt.details?.markPrice ? Number(opt.details.markPrice) : null,
        index_price: opt.details?.indexPrice ? Number(opt.details.indexPrice) : null,
        ask_delta_value: opt.details?.askDeltaValue ? Number(opt.details.askDeltaValue) : null,
        bid_delta_value: opt.details?.bidDeltaValue ? Number(opt.details.bidDeltaValue) : null,
      });
    }
  });
  insert(options);
};

const createPosition = (data) => {
  const result = stmts.insertPosition.run({
    instrument_name: data.instrument_name,
    base_asset_address: data.base_asset_address || null,
    base_asset_sub_id: data.base_asset_sub_id || null,
    direction: data.direction,
    strike: data.strike ? Number(data.strike) : null,
    expiry: data.expiry || null,
    amount: data.amount,
    avg_price: data.avg_price,
    total_cost: data.total_cost,
    opened_at: data.opened_at || new Date().toISOString(),
    rolled_from_id: data.rolled_from_id || null,
  });
  return result.lastInsertRowid;
};

const addToPosition = (positionId, additionalAmount, additionalCost) => {
  stmts.updatePositionAmount.run({
    id: positionId,
    additional_amount: additionalAmount,
    additional_cost: additionalCost,
  });
};

const closePosition = (positionId, status, pnl, reason) => {
  stmts.closePosition.run({
    id: positionId,
    status,
    pnl: pnl || null,
    closed_at: new Date().toISOString(),
    close_reason: reason || null,
  });
};

const insertTrade = (data) => {
  const result = stmts.insertTrade.run({
    position_id: data.position_id || null,
    instrument_name: data.instrument_name,
    direction: data.direction,
    amount: data.amount,
    price: data.price,
    total_value: data.total_value,
    fee: data.fee || null,
    order_type: data.order_type,
    reason: data.reason || null,
    timestamp: data.timestamp || new Date().toISOString(),
    order_response: data.order_response ? JSON.stringify(data.order_response) : null,
  });
  return result.lastInsertRowid;
};

const insertOnchainData = (analysis) => {
  const flow = analysis.dexLiquidity?.flowAnalysis || {};
  const whaleData = analysis.whaleMovements || {};
  const exhaustion = analysis.exhaustionAnalysis || {};

  stmts.insertOnchainData.run({
    timestamp: analysis.timestamp || new Date().toISOString(),
    spot_price: analysis.spotPrice || null,
    liquidity_flow_direction: flow.direction || null,
    liquidity_flow_magnitude: flow.magnitude || null,
    liquidity_flow_confidence: flow.confidence || null,
    whale_count: whaleData.uniqueWallets || whaleData.count || null,
    whale_total_txns: whaleData.totalTransactions || whaleData.totalTxns || null,
    exhaustion_score: exhaustion.metrics?.compositeScore || null,
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
const getOpenPositions = () => stmts.getOpenPositions.all();
const getOpenPositionByInstrument = (instrumentName) =>
  stmts.getOpenPositionByInstrument.get({ instrument_name: instrumentName });
const getRecentSpotPrices = (since) => stmts.getRecentSpotPrices.all({ since });
const getRecentSignals = (since, limit = 50) => stmts.getRecentSignals.all({ since, limit });
const getPositionsByStatus = (status) => stmts.getPositionsByStatus.all({ status });
const getAllPositions = () => stmts.getAllPositions.all();
const getTradesForPosition = (positionId) => stmts.getTradesForPosition.all({ position_id: positionId });
const getRecentTrades = (since, limit = 100) => stmts.getRecentTrades.all({ since, limit });
const getRecentOnchain = (since) => stmts.getRecentOnchain.all({ since });
const getRecentOptionsSnapshots = (since) => stmts.getRecentOptionsSnapshots.all({ since });
const getStats = () => stmts.getStats.get();
const getAvgCallPremium7d = () => {
  const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  return stmts.getAvgCallPremium7d.get({ since });
};

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

const loadOpenPositionsAsArrays = () => {
  const buyPositions = stmts.getOpenPositionsByDirection.all({ direction: 'buy' });
  const sellPositions = stmts.getOpenPositionsByDirection.all({ direction: 'sell' });

  const boughtPuts = buyPositions.map(pos => {
    const trades = stmts.getTradesForPosition.all({ position_id: pos.id });
    return {
      instrument_name: pos.instrument_name,
      base_asset_address: pos.base_asset_address,
      base_asset_sub_id: pos.base_asset_sub_id,
      strike: pos.strike,
      expiration: pos.expiry,
      totalAmount: pos.amount,
      orders: trades.map(t => ({
        amount: t.amount,
        buyPrice: t.price,
        buyTimestamp: t.timestamp,
        delta: null,
        totalCost: t.total_value,
      })),
    };
  });

  const soldCalls = sellPositions.map(pos => {
    const trades = stmts.getTradesForPosition.all({ position_id: pos.id });
    return {
      instrument_name: pos.instrument_name,
      base_asset_address: pos.base_asset_address,
      base_asset_sub_id: pos.base_asset_sub_id,
      strike: pos.strike,
      expiration: pos.expiry,
      totalAmount: pos.amount,
      orders: trades.map(t => ({
        amount: t.amount,
        sellPrice: t.price,
        sellTimestamp: t.timestamp,
        delta: null,
        totalRevenue: t.total_value,
      })),
    };
  });

  return { boughtPuts, soldCalls };
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
  createPosition,
  addToPosition,
  closePosition,
  setRolledTo: (positionId, rolledToId) => stmts.setRolledTo.run({ id: positionId, rolled_to_id: rolledToId }),
  insertTrade,
  insertOnchainData,
  insertSignal,
  markSignalActed: (id) => stmts.markSignalActed.run({ id }),
  getOpenPositions,
  getOpenPositionByInstrument,
  getRecentSpotPrices,
  getRecentSignals,
  getPositionsByStatus,
  getAllPositions,
  getTradesForPosition,
  getRecentTrades,
  getRecentOnchain,
  getRecentOptionsSnapshots,
  getStats,
  getAvgCallPremium7d,
  saveBotState,
  loadBotState,
  loadPriceHistoryFromDb,
  loadOpenPositionsAsArrays,
  migrateFromJson,
  close,
};
