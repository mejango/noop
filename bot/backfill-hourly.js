#!/usr/bin/env node
/**
 * One-time backfill script to populate hourly rollup tables from existing raw data.
 * Safe to re-run (uses INSERT OR REPLACE / ON CONFLICT).
 *
 * Usage: node bot/backfill-hourly.js
 */

const Database = require('better-sqlite3');
const path = require('path');
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
const DB_PATH = path.join(DATA_DIR, 'noop.db');

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');

console.log('Backfilling hourly rollup tables from raw data...');
console.log(`Database: ${DB_PATH}\n`);

// ─── 1. Backfill spot_prices_hourly ─────────────────────────────────────────

console.log('1/3  spot_prices_hourly...');

const spotBackfill = db.prepare(`
  INSERT OR REPLACE INTO spot_prices_hourly (hour, open, high, low, close, avg_price, short_momentum, medium_momentum, count)
  SELECT
    strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
    -- open = first price in the hour (min rowid within group)
    (SELECT sp2.price FROM spot_prices sp2
     WHERE strftime('%Y-%m-%dT%H:00:00Z', sp2.timestamp) = strftime('%Y-%m-%dT%H:00:00Z', sp.timestamp)
     ORDER BY sp2.timestamp ASC LIMIT 1) as open,
    MAX(price) as high,
    MIN(price) as low,
    -- close = last price in the hour (max rowid within group)
    (SELECT sp3.price FROM spot_prices sp3
     WHERE strftime('%Y-%m-%dT%H:00:00Z', sp3.timestamp) = strftime('%Y-%m-%dT%H:00:00Z', sp.timestamp)
     ORDER BY sp3.timestamp DESC LIMIT 1) as close,
    AVG(price) as avg_price,
    -- momentum: take the last value in the hour
    (SELECT sp4.short_momentum_main FROM spot_prices sp4
     WHERE strftime('%Y-%m-%dT%H:00:00Z', sp4.timestamp) = strftime('%Y-%m-%dT%H:00:00Z', sp.timestamp)
     ORDER BY sp4.timestamp DESC LIMIT 1) as short_momentum,
    (SELECT sp5.medium_momentum_main FROM spot_prices sp5
     WHERE strftime('%Y-%m-%dT%H:00:00Z', sp5.timestamp) = strftime('%Y-%m-%dT%H:00:00Z', sp.timestamp)
     ORDER BY sp5.timestamp DESC LIMIT 1) as medium_momentum,
    COUNT(*) as count
  FROM spot_prices sp
  GROUP BY strftime('%Y-%m-%dT%H:00:00Z', timestamp)
`);

const spotResult = spotBackfill.run();
console.log(`   ${spotResult.changes} hourly buckets written`);

// ─── 2. Backfill options_hourly ─────────────────────────────────────────────

console.log('2/3  options_hourly...');

const optionsBackfill = db.prepare(`
  INSERT OR REPLACE INTO options_hourly (hour, best_put_dv, best_call_dv, avg_spread, avg_depth, avg_iv, total_oi, count)
  SELECT
    strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
    MAX(CASE WHEN (option_type = 'P' OR instrument_name LIKE '%-P')
      AND delta <= -0.02 AND delta >= -0.12
      THEN ask_delta_value END) as best_put_dv,
    MAX(CASE WHEN (option_type = 'C' OR instrument_name LIKE '%-C')
      AND delta >= 0.04 AND delta <= 0.12
      THEN bid_delta_value END) as best_call_dv,
    AVG(CASE WHEN ask_price > 0 AND bid_price > 0 AND mark_price > 0
      AND ((delta <= -0.02 AND delta >= -0.12) OR (delta >= 0.04 AND delta <= 0.12))
      THEN (ask_price - bid_price) / mark_price END) as avg_spread,
    AVG(CASE WHEN ((delta <= -0.02 AND delta >= -0.12) OR (delta >= 0.04 AND delta <= 0.12))
      THEN ask_amount + bid_amount END) as avg_depth,
    AVG(CASE WHEN implied_vol IS NOT NULL
      AND ((delta <= -0.02 AND delta >= -0.12) OR (delta >= 0.04 AND delta <= 0.12))
      THEN implied_vol END) as avg_iv,
    SUM(CASE WHEN open_interest IS NOT NULL AND open_interest > 0 THEN open_interest ELSE 0 END) as total_oi,
    COUNT(DISTINCT timestamp) as count
  FROM options_snapshots
  GROUP BY strftime('%Y-%m-%dT%H:00:00Z', timestamp)
`);

const optionsResult = optionsBackfill.run();
console.log(`   ${optionsResult.changes} hourly buckets written`);

// ─── 3. Backfill onchain_hourly ─────────────────────────────────────────────

console.log('3/3  onchain_hourly...');

// Onchain data stores per-DEX info in raw_data JSON — we need to parse it row by row
const onchainRows = db.prepare(`
  SELECT timestamp, raw_data, liquidity_flow_direction, liquidity_flow_magnitude
  FROM onchain_data
  ORDER BY timestamp ASC
`).all();

const upsertOnchain = db.prepare(`
  INSERT OR REPLACE INTO onchain_hourly (hour, dex, tvl, volume, tx_count, avg_magnitude, direction)
  VALUES (@hour, @dex, @tvl, @volume, @tx_count, @avg_magnitude, @direction)
`);

const insertOnchainBatch = db.transaction((rows) => {
  let count = 0;
  for (const row of rows) {
    const hour = row.timestamp.slice(0, 13) + ':00:00Z';
    try {
      const data = JSON.parse(row.raw_data);
      const dexes = data?.dexLiquidity?.dexes;
      if (!dexes) continue;
      for (const [name, dex] of Object.entries(dexes)) {
        if (dex.error) continue;
        const tvl = Number(dex.totalLiquidity) || 0;
        const volume = Number(dex.totalVolume) || 0;
        const txCount = Number(dex.totalTxCount) || 0;
        if (tvl === 0 && volume === 0) continue;
        upsertOnchain.run({
          hour,
          dex: name,
          tvl,
          volume,
          tx_count: txCount,
          avg_magnitude: Number(row.liquidity_flow_magnitude) || 0,
          direction: row.liquidity_flow_direction || null,
        });
        count++;
      }
    } catch { /* skip malformed JSON */ }
  }
  return count;
});

const onchainCount = insertOnchainBatch(onchainRows);
console.log(`   ${onchainCount} DEX-hour rows written`);

// ─── Done ───────────────────────────────────────────────────────────────────

console.log('\nBackfill complete.');
db.close();
