'use strict';

const { DAY_MS, finite } = require('./utils');
const { enrichFrames } = require('./features');

const OPTIONAL_OPTION_COLUMNS = [
  'strike',
  'expiry',
  'option_type',
  'delta',
  'ask_price',
  'bid_price',
  'ask_amount',
  'bid_amount',
  'mark_price',
  'index_price',
  'implied_vol',
  'open_interest',
];

function tableExists(db, tableName) {
  return Boolean(db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1").get(tableName));
}

function tableColumns(db, tableName) {
  if (!tableExists(db, tableName)) return new Set();
  return new Set(db.prepare(`PRAGMA table_info(${tableName})`).all().map((row) => row.name));
}

function getCoverage(db) {
  const coverage = {};
  for (const [table, timestampColumn] of [
    ['options_snapshots', 'timestamp'],
    ['spot_prices', 'timestamp'],
    ['candidate_observations', 'observed_at'],
    ['decision_outcomes', 'due_at'],
    ['orders', 'timestamp'],
    ['portfolio_snapshots', 'timestamp'],
  ]) {
    if (!tableExists(db, table)) {
      coverage[table] = { exists: false, rows: 0, first_timestamp: null, last_timestamp: null };
      continue;
    }
    const row = db.prepare(`
      SELECT COUNT(*) AS rows, MIN(${timestampColumn}) AS first_timestamp, MAX(${timestampColumn}) AS last_timestamp
      FROM ${table}
    `).get();
    coverage[table] = { exists: true, ...row };
  }
  return coverage;
}

function resolveWindow(db, options = {}) {
  if (!tableExists(db, 'options_snapshots')) throw new Error('options_snapshots table is missing');
  const bounds = db.prepare('SELECT MIN(timestamp) AS first_timestamp, MAX(timestamp) AS last_timestamp FROM options_snapshots').get();
  if (!bounds?.last_timestamp) throw new Error('options_snapshots contains no historical rows');
  const databaseFirstMs = new Date(bounds.first_timestamp).getTime();
  const databaseLastMs = new Date(bounds.last_timestamp).getTime();
  const requestedStartMs = options.from ? new Date(options.from).getTime() : null;
  const requestedEndMs = options.to ? new Date(options.to).getTime() : databaseLastMs;
  const days = options.days === 'all' || options.days == null ? null : Number(options.days);
  const startFromDays = days > 0 ? requestedEndMs - days * DAY_MS : databaseFirstMs;
  const startMs = Math.max(databaseFirstMs, Number.isFinite(requestedStartMs) ? requestedStartMs : startFromDays);
  const endMs = Math.min(databaseLastMs, Number.isFinite(requestedEndMs) ? requestedEndMs : databaseLastMs);
  if (!(startMs <= endMs)) throw new Error('requested historical window is empty');
  return {
    from: new Date(startMs).toISOString(),
    to: new Date(endMs).toISOString(),
    database_first: bounds.first_timestamp,
    database_last: bounds.last_timestamp,
  };
}

function loadSpotRows(db, from, to) {
  if (!tableExists(db, 'spot_prices')) return [];
  const rows = db.prepare(`
    SELECT timestamp, price
    FROM spot_prices
    WHERE timestamp >= @from AND timestamp <= @to
      AND price BETWEEN 100 AND 20000
    ORDER BY timestamp ASC
  `).all({ from, to });
  const prior = db.prepare(`
    SELECT timestamp, price
    FROM spot_prices
    WHERE timestamp < @from AND price BETWEEN 100 AND 20000
    ORDER BY timestamp DESC
    LIMIT 1
  `).get({ from });
  if (prior) rows.unshift(prior);
  return rows.map((row) => ({
    timestamp_ms: new Date(row.timestamp).getTime(),
    price: finite(row.price),
  })).filter((row) => Number.isFinite(row.timestamp_ms) && row.price > 0);
}

function attachPriorSpot(frames, spots) {
  let spotIndex = 0;
  let latestSpot = null;
  for (const frame of frames) {
    while (spotIndex < spots.length && spots[spotIndex].timestamp_ms <= frame.timestamp_ms) {
      latestSpot = spots[spotIndex].price;
      spotIndex++;
    }
    frame.spot_price = latestSpot;
  }
}

function loadHistoricalFrames(db, options = {}) {
  const cadenceHours = Math.max(1, Math.floor(Number(options.cadenceHours || 1)));
  const cadenceSeconds = cadenceHours * 60 * 60;
  const maxFrames = Number(options.maxFrames) > 0 ? Math.floor(Number(options.maxFrames)) : null;
  const window = resolveWindow(db, options);
  const columns = tableColumns(db, 'options_snapshots');
  if (!columns.has('timestamp') || !columns.has('instrument_name')) {
    throw new Error('options_snapshots is missing timestamp or instrument_name');
  }
  const selectedColumns = OPTIONAL_OPTION_COLUMNS.map((column) => (
    columns.has(column) ? `o.${column} AS ${column}` : `NULL AS ${column}`
  ));
  const limitSql = maxFrames ? 'LIMIT @max_frames' : '';
  const sql = `
    WITH frame_times AS (
      SELECT MIN(timestamp) AS timestamp
      FROM options_snapshots
      WHERE timestamp >= @from AND timestamp <= @to
      GROUP BY CAST(CAST(strftime('%s', timestamp) AS INTEGER) / @cadence_seconds AS INTEGER)
      ORDER BY timestamp ASC
      ${limitSql}
    )
    SELECT o.timestamp, o.instrument_name, ${selectedColumns.join(', ')}
    FROM options_snapshots o
    JOIN frame_times f ON f.timestamp = o.timestamp
    WHERE o.instrument_name IS NOT NULL
    ORDER BY o.timestamp ASC, o.instrument_name ASC
  `;
  const params = {
    from: window.from,
    to: window.to,
    cadence_seconds: cadenceSeconds,
    max_frames: maxFrames,
  };
  const rawFrames = [];
  let current = null;
  for (const row of db.prepare(sql).iterate(params)) {
    if (!current || current.timestamp !== row.timestamp) {
      current = {
        timestamp: row.timestamp,
        timestamp_ms: new Date(row.timestamp).getTime(),
        options: [],
      };
      rawFrames.push(current);
    }
    current.options.push(row);
  }
  const spots = loadSpotRows(db, window.from, window.to);
  attachPriorSpot(rawFrames, spots);
  const frames = enrichFrames(rawFrames, { callDteRange: options.callDteRange });
  return {
    frames,
    window,
    coverage: getCoverage(db),
    cadence_hours: cadenceHours,
  };
}

module.exports = {
  getCoverage,
  loadHistoricalFrames,
  resolveWindow,
  tableColumns,
  tableExists,
};
