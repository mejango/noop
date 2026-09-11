/**
 * Canonical hourly aggregates, shared by ingestion and historical repair.
 * Raw timestamps are UTC ISO strings. Recompute only the touched hour so late
 * observations, missing metrics and uneven option batch sizes stay reproducible.
 *
 * Options: means are weighted by observed rows in the strategy's delta bands;
 * OI is the sum of each instrument's latest observation, never a sum over time.
 * An incomplete latest OI population is unknown (NULL), not a partial total.
 * Onchain TVL, cumulative volume and transaction counts are latest stock values;
 * flow magnitude is the arithmetic mean of available observations for that DEX.
 */
const ALGORITHM_VERSION = 1;
const HOUR_MS = 60 * 60 * 1000;
const RAW_TABLES = {
  spot_prices_hourly: 'spot_prices',
  options_hourly: 'options_snapshots',
  onchain_hourly: 'onchain_data',
  funding_rates_hourly: 'funding_rates',
};

function hourBounds(timestamp) {
  const parsed = new Date(timestamp).getTime();
  if (!Number.isFinite(parsed)) throw new Error(`Invalid hourly timestamp: ${timestamp}`);
  const startMs = Math.floor(parsed / HOUR_MS) * HOUR_MS;
  const startIso = new Date(startMs).toISOString();
  return {
    hour: startIso.slice(0, 19) + 'Z',
    // Omitting the suffix includes both :00Z and :00.000Z at the boundary.
    start: startIso.slice(0, 19),
    end: new Date(startMs + HOUR_MS).toISOString().slice(0, 19),
  };
}

function rangeBoundary(value, name) {
  if (value == null) return null;
  const parsed = new Date(value).getTime();
  if (!Number.isFinite(parsed) || parsed % HOUR_MS !== 0) {
    throw new Error(`${name} must be a valid timestamp aligned to a UTC hour`);
  }
  return hourBounds(value).hour;
}

function finiteNumber(value) {
  if (value == null || value === '') return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function createHourlyRollups(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS hourly_rollup_metadata (
    key TEXT PRIMARY KEY, value TEXT NOT NULL
  )`);
  const setMetadata = db.prepare('INSERT OR REPLACE INTO hourly_rollup_metadata (key, value) VALUES (?, ?)');
  const spotRows = db.prepare(`SELECT price, short_momentum_main, medium_momentum_main
    FROM spot_prices WHERE timestamp >= @start AND timestamp < @end
    ORDER BY julianday(timestamp), id`);
  const insertSpot = db.prepare(`INSERT OR REPLACE INTO spot_prices_hourly
    (hour, open, high, low, close, avg_price, short_momentum, medium_momentum, count)
    VALUES (@hour, @open, @high, @low, @close, @avg_price, @short_momentum, @medium_momentum, @count)`);
  const optionAggregate = db.prepare(`
    WITH observations AS (
      SELECT *, ROW_NUMBER() OVER (
        PARTITION BY instrument_name ORDER BY julianday(timestamp) DESC, id DESC
      ) AS recency
      FROM options_snapshots WHERE timestamp >= @start AND timestamp < @end
    )
    SELECT
      MAX(CASE WHEN (option_type = 'P' OR instrument_name LIKE '%-P')
        AND delta BETWEEN -0.12 AND -0.02 THEN ask_delta_value END) AS best_put_dv,
      MAX(CASE WHEN (option_type = 'C' OR instrument_name LIKE '%-C')
        AND delta BETWEEN 0.04 AND 0.12 THEN bid_delta_value END) AS best_call_dv,
      AVG(CASE WHEN (delta BETWEEN -0.12 AND -0.02 OR delta BETWEEN 0.04 AND 0.12)
        AND ask_price > 0 AND bid_price > 0 AND mark_price > 0
        THEN (ask_price - bid_price) / mark_price END) AS avg_spread,
      AVG(CASE WHEN (delta BETWEEN -0.12 AND -0.02 OR delta BETWEEN 0.04 AND 0.12)
        AND ask_amount >= 0 AND bid_amount >= 0 THEN ask_amount + bid_amount END) AS avg_depth,
      AVG(CASE WHEN (delta BETWEEN -0.12 AND -0.02 OR delta BETWEEN 0.04 AND 0.12)
        THEN implied_vol END) AS avg_iv,
      CASE WHEN SUM(CASE WHEN recency = 1 AND (open_interest IS NULL OR open_interest < 0) THEN 1 ELSE 0 END) > 0
        THEN NULL ELSE SUM(CASE WHEN recency = 1 THEN open_interest END) END AS total_oi,
      COUNT(DISTINCT timestamp) AS count
    FROM observations`);
  const insertOptions = db.prepare(`INSERT OR REPLACE INTO options_hourly
    (hour, best_put_dv, best_call_dv, avg_spread, avg_depth, avg_iv, total_oi, count)
    VALUES (@hour, @best_put_dv, @best_call_dv, @avg_spread, @avg_depth, @avg_iv, @total_oi, @count)`);
  const onchainRows = db.prepare(`SELECT raw_data, liquidity_flow_direction, liquidity_flow_magnitude
    FROM onchain_data WHERE timestamp >= @start AND timestamp < @end
    ORDER BY julianday(timestamp), id`);
  const insertOnchain = db.prepare(`INSERT OR REPLACE INTO onchain_hourly
    (hour, dex, tvl, volume, tx_count, avg_magnitude, direction)
    VALUES (@hour, @dex, @tvl, @volume, @tx_count, @avg_magnitude, @direction)`);
  const fundingAggregate = db.prepare(`SELECT AVG(rate) AS avg_rate, COUNT(*) AS count
    FROM funding_rates WHERE timestamp >= @start AND timestamp < @end
      AND exchange = @exchange AND symbol = @symbol`);
  const fundingPairs = db.prepare(`SELECT DISTINCT exchange, symbol FROM funding_rates
    WHERE timestamp >= @start AND timestamp < @end ORDER BY exchange, symbol`);
  const insertFunding = db.prepare(`INSERT OR REPLACE INTO funding_rates_hourly
    (hour, exchange, symbol, avg_rate, count) VALUES (@hour, @exchange, @symbol, @avg_rate, @count)`);
  const deleteFunding = db.prepare(`DELETE FROM funding_rates_hourly
    WHERE hour = @hour AND exchange = @exchange AND symbol = @symbol`);
  const deleteHour = Object.fromEntries(Object.keys(RAW_TABLES).map(table => [
    table, db.prepare(`DELETE FROM ${table} WHERE hour = ?`),
  ]));
  // Separate bounded SQL keeps range repair on timestamp/hour indexes. An
  // optional `@start IS NULL OR ...` forces SQLite to scan the entire raw table.
  function rangeStatements(prefix, column, lower, upper, suffix = '') {
    return {
      all: db.prepare(`${prefix}${suffix}`),
      from: db.prepare(`${prefix} WHERE ${column} >= @${lower}${suffix}`),
      to: db.prepare(`${prefix} WHERE ${column} < @${upper}${suffix}`),
      range: db.prepare(`${prefix} WHERE ${column} >= @${lower} AND ${column} < @${upper}${suffix}`),
    };
  }
  const deleteRange = Object.fromEntries(Object.keys(RAW_TABLES).map(table => [
    table, rangeStatements(`DELETE FROM ${table}`, 'hour', 'from', 'to'),
  ]));
  const rawHours = Object.fromEntries(Object.entries(RAW_TABLES).map(([table, rawTable]) => [
    table, rangeStatements(`SELECT DISTINCT strftime('%Y-%m-%dT%H:00:00Z', timestamp) AS hour FROM ${rawTable}`,
      'timestamp', 'start', 'end', ' ORDER BY hour'),
  ]));

  function refreshSpotHour(timestamp) {
    const bounds = hourBounds(timestamp);
    const rows = spotRows.all(bounds);
    if (!rows.length) {
      deleteHour.spot_prices_hourly.run(bounds.hour);
      return 0;
    }
    const first = rows[0];
    const last = rows[rows.length - 1];
    let sum = 0, high = -Infinity, low = Infinity;
    for (const row of rows) {
      sum += row.price;
      high = Math.max(high, row.price);
      low = Math.min(low, row.price);
    }
    insertSpot.run({ hour: bounds.hour, open: first.price, high, low, close: last.price,
      avg_price: sum / rows.length, short_momentum: last.short_momentum_main,
      medium_momentum: last.medium_momentum_main, count: rows.length });
    return 1;
  }

  function refreshOptionsHour(timestamp) {
    const bounds = hourBounds(timestamp);
    const aggregate = optionAggregate.get(bounds);
    if (!aggregate.count) {
      deleteHour.options_hourly.run(bounds.hour);
      return 0;
    }
    insertOptions.run({ hour: bounds.hour, ...aggregate });
    return 1;
  }

  function refreshOnchainHour(timestamp) {
    const bounds = hourBounds(timestamp);
    const dexHours = new Map();
    for (const row of onchainRows.all(bounds)) {
      let data;
      try { data = JSON.parse(row.raw_data); } catch { continue; }
      const dexes = data?.dexLiquidity?.dexes;
      if (!dexes || typeof dexes !== 'object' || Array.isArray(dexes)) continue;
      for (const [name, dex] of Object.entries(dexes)) {
        if (!dex || typeof dex !== 'object' || Array.isArray(dex) || dex.error) continue;
        // Preserve the historical repair exclusion for multi-pool v4 responses.
        if (name === 'uniswap_v4' && finiteNumber(dex.pools) > 1) continue;
        const aggregate = dexHours.get(name) || { sum: 0, count: 0 };
        const magnitude = finiteNumber(row.liquidity_flow_magnitude);
        if (magnitude != null) { aggregate.sum += magnitude; aggregate.count++; }
        aggregate.latest = { tvl: finiteNumber(dex.totalLiquidity), volume: finiteNumber(dex.totalVolume),
          tx_count: finiteNumber(dex.totalTxCount), direction: row.liquidity_flow_direction ?? null };
        dexHours.set(name, aggregate);
      }
    }
    deleteHour.onchain_hourly.run(bounds.hour);
    for (const [dex, aggregate] of dexHours) {
      insertOnchain.run({ hour: bounds.hour, dex, ...aggregate.latest,
        avg_magnitude: aggregate.count ? aggregate.sum / aggregate.count : null });
    }
    return dexHours.size;
  }

  function refreshFundingHour(timestamp, exchange, symbol) {
    if (typeof exchange !== 'string' || !exchange || typeof symbol !== 'string' || !symbol) {
      throw new Error('Funding rollups require exchange and symbol');
    }
    const params = { ...hourBounds(timestamp), exchange, symbol };
    const aggregate = fundingAggregate.get(params);
    if (!aggregate.count) {
      deleteFunding.run(params);
      return 0;
    }
    insertFunding.run({ hour: params.hour, exchange, symbol, ...aggregate });
    return 1;
  }

  const rebuildTransaction = db.transaction(({ from, to }) => {
    const bounds = { from, to, start: from?.slice(0, 19) ?? null, end: to?.slice(0, 19) ?? null };
    const counts = {};
    const mode = from == null ? (to == null ? 'all' : 'to') : (to == null ? 'from' : 'range');
    const parameters = mode === 'all' ? [] : [bounds];
    for (const table of Object.keys(RAW_TABLES)) {
      const hours = rawHours[table][mode].all(...parameters);
      if (hours.some(row => !row.hour)) throw new Error(`Cannot rebuild ${table}: invalid raw timestamps`);
      deleteRange[table][mode].run(...parameters);
      let count = 0;
      for (const { hour } of hours) {
        if (table === 'spot_prices_hourly') count += refreshSpotHour(hour);
        else if (table === 'options_hourly') count += refreshOptionsHour(hour);
        else if (table === 'onchain_hourly') count += refreshOnchainHour(hour);
        else for (const pair of fundingPairs.all(hourBounds(hour))) {
          count += refreshFundingHour(hour, pair.exchange, pair.symbol);
        }
      }
      counts[table] = count;
    }
    const result = { algorithmVersion: ALGORITHM_VERSION, from, to, counts, completedAt: new Date().toISOString() };
    setMetadata.run('last_rebuild', JSON.stringify(result));
    if (from == null && to == null) setMetadata.run('last_full_rebuild', JSON.stringify(result));
    return result;
  });

  return {
    refreshSpotHour: db.transaction(refreshSpotHour),
    refreshOptionsHour: db.transaction(refreshOptionsHour),
    refreshOnchainHour: db.transaction(refreshOnchainHour),
    refreshFundingHour: db.transaction(refreshFundingHour),
    rebuild({ from, to } = {}) {
      from = rangeBoundary(from, 'from');
      to = rangeBoundary(to, 'to');
      if (from != null && to != null && from >= to) throw new Error('from must precede to');
      return rebuildTransaction.immediate({ from, to });
    },
  };
}

module.exports = { createHourlyRollups, ALGORITHM_VERSION };
