const DEFAULT_HORIZONS_HOURS = [1, 6, 12, 24, 48, 72, 168];
const DEFAULT_LOOKBACK_HOURS = [1, 6, 24];
const DEFAULT_MIN_SAMPLES = 30;
const DEFAULT_TOP_PER_HOUR = 8;
const DEFAULT_QUOTE_WINDOW_HOURS = 6;

const ACTION_CONFIG = {
  sell_call: {
    optionType: 'C',
    minDelta: 0.04,
    maxDelta: 0.12,
    minDte: 5,
    maxDte: 12,
    scoreColumn: 'bid_delta_value',
    entryPriceField: 'bid_price',
    exitPriceField: 'ask_price',
    outcomeName: 'sell_entry_pnl',
    returnName: 'sell_return_on_premium',
    favorableOutcome: (pnl) => pnl > 0,
    tailLoss: (entry, future) => future > entry * 2,
    description: 'Short call candidate: edge is premium kept after future buyback ask.',
  },
  buy_put: {
    optionType: 'P',
    minDelta: -0.12,
    maxDelta: -0.02,
    minDte: 45,
    maxDte: 78,
    scoreColumn: 'ask_delta_value',
    entryPriceField: 'ask_price',
    exitPriceField: 'bid_price',
    outcomeName: 'buy_entry_pnl',
    returnName: 'buy_return_on_premium',
    favorableOutcome: (pnl) => pnl > 0,
    tailLoss: (entry, future) => future < entry * 0.5,
    description: 'Long put candidate: edge is future sellable bid over entry ask.',
  },
};

const FEATURE_LABELS = {
  raw_score: 'candidate raw value score',
  dte: 'days to expiry',
  abs_delta: 'absolute delta',
  strike_distance_pct: 'strike distance from spot',
  bid_price: 'candidate bid',
  ask_price: 'candidate ask',
  mark_price: 'candidate mark',
  spread_pct: 'candidate spread pct',
  depth: 'candidate visible depth',
  implied_vol: 'candidate implied volatility',
  open_interest: 'candidate open interest',
  market_best_call_score: 'market best call score',
  market_best_put_score: 'market best put score',
  market_avg_spread: 'market average spread',
  market_avg_depth: 'market average depth',
  market_total_oi: 'market total open interest',
  market_call_oi: 'market call open interest',
  market_put_oi: 'market put open interest',
  market_pc_oi: 'market put/call open-interest ratio',
  market_call_iv: 'market call IV',
  market_put_iv: 'market put IV',
  market_skew: 'put-call IV skew',
  funding_rate: 'perp funding rate',
  liquidity_flow: 'signed liquidity flow',
};

function finite(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function round(value, digits = 6) {
  const n = finite(value);
  return n == null ? null : Number(n.toFixed(digits));
}

function toIso(ms) {
  return new Date(ms).toISOString();
}

function addHours(iso, hours) {
  return toIso(new Date(iso).getTime() + hours * 60 * 60 * 1000);
}

function toHour(iso) {
  const date = new Date(iso);
  date.setUTCMinutes(0, 0, 0);
  return date.toISOString().replace('.000Z', 'Z');
}

function priorHour(hourIso, hours) {
  return toIso(new Date(hourIso).getTime() - hours * 60 * 60 * 1000).replace('.000Z', 'Z');
}

function pctChange(current, previous) {
  const c = finite(current);
  const p = finite(previous);
  if (c == null || p == null || p === 0) return null;
  return (c - p) / Math.abs(p);
}

function difference(current, previous) {
  const c = finite(current);
  const p = finite(previous);
  if (c == null || p == null) return null;
  return c - p;
}

function median(values) {
  const finiteValues = values.map(finite).filter((v) => v != null).sort((a, b) => a - b);
  if (finiteValues.length === 0) return null;
  const mid = Math.floor(finiteValues.length / 2);
  return finiteValues.length % 2 === 0
    ? (finiteValues[mid - 1] + finiteValues[mid]) / 2
    : finiteValues[mid];
}

function quantile(values, q) {
  const finiteValues = values.map(finite).filter((v) => v != null).sort((a, b) => a - b);
  if (finiteValues.length === 0) return null;
  const pos = (finiteValues.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  if (finiteValues[base + 1] == null) return finiteValues[base];
  return finiteValues[base] + rest * (finiteValues[base + 1] - finiteValues[base]);
}

function mean(values) {
  const finiteValues = values.map(finite).filter((v) => v != null);
  if (finiteValues.length === 0) return null;
  return finiteValues.reduce((sum, value) => sum + value, 0) / finiteValues.length;
}

function pearson(xs, ys) {
  const pairs = [];
  const length = Math.min(xs.length, ys.length);
  for (let i = 0; i < length; i++) {
    const x = finite(xs[i]);
    const y = finite(ys[i]);
    if (x != null && y != null) pairs.push([x, y]);
  }
  if (pairs.length < 10) return null;
  let sumX = 0;
  let sumY = 0;
  let sumXY = 0;
  let sumX2 = 0;
  let sumY2 = 0;
  for (const [x, y] of pairs) {
    sumX += x;
    sumY += y;
    sumXY += x * y;
    sumX2 += x * x;
    sumY2 += y * y;
  }
  const n = pairs.length;
  const denom = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));
  return denom === 0 ? null : (n * sumXY - sumX * sumY) / denom;
}

function ranks(values) {
  const indexed = values
    .map((value, index) => ({ value: finite(value), index }))
    .filter((item) => item.value != null)
    .sort((a, b) => a.value - b.value);
  const result = Array(values.length).fill(null);
  let i = 0;
  while (i < indexed.length) {
    let j = i + 1;
    while (j < indexed.length && indexed[j].value === indexed[i].value) j++;
    const rank = (i + j + 1) / 2;
    for (let k = i; k < j; k++) result[indexed[k].index] = rank;
    i = j;
  }
  return result;
}

function spearman(xs, ys) {
  const pairs = [];
  const length = Math.min(xs.length, ys.length);
  for (let i = 0; i < length; i++) {
    const x = finite(xs[i]);
    const y = finite(ys[i]);
    if (x != null && y != null) pairs.push({ x, y });
  }
  if (pairs.length < 10) return null;
  return pearson(ranks(pairs.map((p) => p.x)), ranks(pairs.map((p) => p.y)));
}

function summarizeOutcomes(samples) {
  if (!samples.length) {
    return {
      samples: 0,
      mean_pnl: null,
      median_pnl: null,
      mean_return: null,
      median_return: null,
      win_rate: null,
      tail_loss_rate: null,
      mean_spot_return: null,
    };
  }
  return {
    samples: samples.length,
    mean_pnl: round(mean(samples.map((s) => s.outcome.pnl)), 6),
    median_pnl: round(median(samples.map((s) => s.outcome.pnl)), 6),
    mean_return: round(mean(samples.map((s) => s.outcome.return_on_premium)), 6),
    median_return: round(median(samples.map((s) => s.outcome.return_on_premium)), 6),
    win_rate: round(samples.filter((s) => s.outcome.win).length / samples.length, 6),
    tail_loss_rate: round(samples.filter((s) => s.outcome.tail_loss).length / samples.length, 6),
    mean_spot_return: round(mean(samples.map((s) => s.outcome.spot_return)), 6),
  };
}

function getFeatureNames(samples) {
  const names = new Set();
  for (const sample of samples) {
    for (const [name, value] of Object.entries(sample.features || {})) {
      if (finite(value) != null) names.add(name);
    }
  }
  return [...names].sort();
}

function directionalBucketStats(samples, featureName, q = 0.2) {
  const rows = samples
    .map((sample) => ({ sample, value: finite(sample.features[featureName]) }))
    .filter((row) => row.value != null)
    .sort((a, b) => a.value - b.value);
  if (rows.length < 10) return null;
  const bucketSize = Math.max(5, Math.floor(rows.length * q));
  if (bucketSize * 2 > rows.length) return null;
  const lowRows = rows.slice(0, bucketSize).map((row) => row.sample);
  const highRows = rows.slice(rows.length - bucketSize).map((row) => row.sample);
  const low = summarizeOutcomes(lowRows);
  const high = summarizeOutcomes(highRows);
  const highMinusLow = (high.mean_return ?? 0) - (low.mean_return ?? 0);
  const better = highMinusLow >= 0 ? 'higher' : 'lower';
  return {
    low_threshold: round(rows[bucketSize - 1].value, 6),
    high_threshold: round(rows[rows.length - bucketSize].value, 6),
    low,
    high,
    better,
    edge_delta_return: round(Math.abs(highMinusLow), 6),
    signed_high_minus_low_return: round(highMinusLow, 6),
  };
}

function scoreFeature(feature, sampleCount) {
  const bucketEffect = Math.abs(feature.bucket?.signed_high_minus_low_return ?? 0);
  const rankEffect = Math.abs(feature.spearman_return ?? 0);
  const coverage = Math.min(1, feature.samples / Math.max(1, sampleCount));
  const sampleWeight = Math.min(2, Math.sqrt(feature.samples / 100));
  return (bucketEffect * 2 + rankEffect) * coverage * sampleWeight;
}

function analyzeFeatures(samples, minSamples) {
  const featureNames = getFeatureNames(samples);
  const output = [];
  for (const featureName of featureNames) {
    const rows = samples.filter((sample) => finite(sample.features[featureName]) != null);
    if (rows.length < minSamples) continue;
    const values = rows.map((sample) => sample.features[featureName]);
    const pnl = rows.map((sample) => sample.outcome.pnl);
    const returns = rows.map((sample) => sample.outcome.return_on_premium);
    const spotReturns = rows.map((sample) => sample.outcome.spot_return);
    const bucket = directionalBucketStats(rows, featureName);
    const feature = {
      name: featureName,
      label: FEATURE_LABELS[featureName] || featureName.replaceAll('_', ' '),
      samples: rows.length,
      coverage: round(rows.length / samples.length, 6),
      pearson_pnl: round(pearson(values, pnl), 6),
      spearman_pnl: round(spearman(values, pnl), 6),
      pearson_return: round(pearson(values, returns), 6),
      spearman_return: round(spearman(values, returns), 6),
      pearson_spot_return: round(pearson(values, spotReturns), 6),
      bucket,
    };
    feature.edge_score = round(scoreFeature(feature, samples.length), 6);
    output.push(feature);
  }
  return output.sort((a, b) => (b.edge_score || 0) - (a.edge_score || 0));
}

function featurePredicate(feature) {
  if (!feature?.bucket) return null;
  const direction = feature.bucket.better;
  const threshold = direction === 'higher' ? feature.bucket.high_threshold : feature.bucket.low_threshold;
  if (threshold == null) return null;
  return {
    name: feature.name,
    label: feature.label,
    direction,
    threshold,
    test: (sample) => {
      const value = finite(sample.features[feature.name]);
      if (value == null) return false;
      return direction === 'higher' ? value >= threshold : value <= threshold;
    },
  };
}

function analyzeInteractions(samples, features, minSamples) {
  const predicates = features.slice(0, 14).map(featurePredicate).filter(Boolean);
  const interactions = [];
  for (let i = 0; i < predicates.length; i++) {
    for (let j = i + 1; j < predicates.length; j++) {
      const a = predicates[i];
      const b = predicates[j];
      const matched = samples.filter((sample) => a.test(sample) && b.test(sample));
      if (matched.length < minSamples) continue;
      const stats = summarizeOutcomes(matched);
      const base = summarizeOutcomes(samples);
      const edge = (stats.mean_return ?? 0) - (base.mean_return ?? 0);
      interactions.push({
        features: [
          { name: a.name, label: a.label, direction: a.direction, threshold: a.threshold },
          { name: b.name, label: b.label, direction: b.direction, threshold: b.threshold },
        ],
        samples: matched.length,
        coverage: round(matched.length / samples.length, 6),
        stats,
        base_mean_return: base.mean_return,
        edge_vs_base_return: round(edge, 6),
        edge_score: round(Math.abs(edge) * Math.min(2, Math.sqrt(matched.length / 100)), 6),
      });
    }
  }
  return interactions.sort((a, b) => Math.abs(b.edge_vs_base_return || 0) - Math.abs(a.edge_vs_base_return || 0));
}

function getCoverage(db) {
  const tables = ['options_snapshots', 'spot_prices', 'onchain_data', 'funding_rates', 'candidate_observations', 'decision_outcomes'];
  const coverage = {};
  const tableExists = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?");
  for (const table of tables) {
    if (!tableExists.get(table)) {
      coverage[table] = { exists: false };
      continue;
    }
    const row = db.prepare(`SELECT MIN(timestamp) as first_timestamp, MAX(timestamp) as last_timestamp, COUNT(*) as rows FROM ${table}`).get();
    if (table === 'candidate_observations') {
      const c = db.prepare('SELECT MIN(observed_at) as first_timestamp, MAX(observed_at) as last_timestamp, COUNT(*) as rows FROM candidate_observations').get();
      coverage[table] = { exists: true, ...c };
    } else if (table === 'decision_outcomes') {
      const c = db.prepare('SELECT MIN(due_at) as first_timestamp, MAX(due_at) as last_timestamp, COUNT(*) as rows FROM decision_outcomes').get();
      coverage[table] = { exists: true, ...c };
    } else {
      coverage[table] = { exists: true, ...row };
    }
  }
  return coverage;
}

function buildSinceClause(sinceIso, alias = '') {
  if (!sinceIso) return { sql: '', params: {} };
  const prefix = alias ? `${alias}.` : '';
  return { sql: ` AND ${prefix}timestamp >= @since`, params: { since: sinceIso } };
}

function tableHasRows(db, tableName) {
  const table = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(tableName);
  if (!table) return false;
  try {
    return (db.prepare(`SELECT COUNT(*) as count FROM ${tableName}`).get()?.count || 0) > 0;
  } catch {
    return false;
  }
}

function loadMarketHourly(db, sinceIso) {
  const { sql, params } = buildSinceClause(sinceIso);
  const market = new Map();
  const putFilter = "(option_type = 'P' OR instrument_name LIKE '%-P') AND delta <= -0.02 AND delta >= -0.12";
  const callFilter = "(option_type = 'C' OR instrument_name LIKE '%-C') AND delta >= 0.04 AND delta <= 0.12";
  const anyFilter = `((${putFilter}) OR (${callFilter}))`;
  const sinceHour = sinceIso ? toHour(sinceIso) : null;

  const optionsRows = tableHasRows(db, 'options_hourly')
    ? db.prepare(`
      SELECT
        hour,
        best_put_dv as market_best_put_score,
        best_call_dv as market_best_call_score,
        avg_spread as market_avg_spread,
        avg_depth as market_avg_depth,
        avg_iv as market_avg_iv,
        total_oi as market_total_oi
      FROM options_hourly
      WHERE (@since_hour IS NULL OR hour >= @since_hour)
      ORDER BY hour ASC
    `).all({ since_hour: sinceHour })
    : db.prepare(`
      SELECT
        strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
        MAX(CASE WHEN ${putFilter} THEN COALESCE(ask_delta_value, ABS(delta) / NULLIF(ask_price, 0)) END) as market_best_put_score,
        MAX(CASE WHEN ${callFilter} THEN COALESCE(bid_delta_value, bid_price / NULLIF(ABS(delta), 0)) END) as market_best_call_score,
        AVG(CASE WHEN ${anyFilter} AND ask_price > 0 AND bid_price > 0 AND mark_price > 0 THEN (ask_price - bid_price) / mark_price END) as market_avg_spread,
        AVG(CASE WHEN ${anyFilter} THEN COALESCE(ask_amount, 0) + COALESCE(bid_amount, 0) END) as market_avg_depth,
        SUM(CASE WHEN open_interest IS NOT NULL AND open_interest > 0 THEN open_interest ELSE 0 END) as market_total_oi,
        SUM(CASE WHEN ${callFilter} AND open_interest IS NOT NULL AND open_interest > 0 THEN open_interest ELSE 0 END) as market_call_oi,
        SUM(CASE WHEN ${putFilter} AND open_interest IS NOT NULL AND open_interest > 0 THEN open_interest ELSE 0 END) as market_put_oi,
        AVG(CASE WHEN ${callFilter} AND implied_vol IS NOT NULL THEN implied_vol END) as market_call_iv,
        AVG(CASE WHEN ${putFilter} AND implied_vol IS NOT NULL THEN implied_vol END) as market_put_iv
      FROM options_snapshots
      WHERE timestamp IS NOT NULL ${sql}
      GROUP BY hour
      ORDER BY hour ASC
    `).all(params);

  for (const row of optionsRows) {
    const entry = market.get(row.hour) || { hour: row.hour };
    for (const [key, value] of Object.entries(row)) {
      if (key !== 'hour') entry[key] = finite(value);
    }
    entry.market_pc_oi = entry.market_call_oi > 0 ? entry.market_put_oi / entry.market_call_oi : null;
    entry.market_skew = entry.market_put_iv != null && entry.market_call_iv != null
      ? entry.market_put_iv - entry.market_call_iv
      : null;
    market.set(row.hour, entry);
  }

  const spotRows = tableHasRows(db, 'spot_prices_hourly')
    ? db.prepare(`
      SELECT hour, avg_price as spot_price, low as spot_low, high as spot_high
      FROM spot_prices_hourly
      WHERE (@since_hour IS NULL OR hour >= @since_hour)
    `).all({ since_hour: sinceHour })
    : db.prepare(`
      SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
        AVG(price) as spot_price,
        MIN(price) as spot_low,
        MAX(price) as spot_high
      FROM spot_prices
      WHERE price BETWEEN 100 AND 20000 ${sql}
      GROUP BY hour
    `).all(params);
  for (const row of spotRows) {
    const entry = market.get(row.hour) || { hour: row.hour };
    entry.spot_price = finite(row.spot_price);
    entry.spot_low = finite(row.spot_low);
    entry.spot_high = finite(row.spot_high);
    market.set(row.hour, entry);
  }

  const tableExists = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?");
  if (tableHasRows(db, 'funding_rates_hourly')) {
    const fundingRows = db.prepare(`
      SELECT hour, AVG(avg_rate) as funding_rate
      FROM funding_rates_hourly
      WHERE (@since_hour IS NULL OR hour >= @since_hour)
      GROUP BY hour
    `).all({ since_hour: sinceHour });
    for (const row of fundingRows) {
      const entry = market.get(row.hour) || { hour: row.hour };
      entry.funding_rate = finite(row.funding_rate);
      market.set(row.hour, entry);
    }
  } else if (tableExists.get('funding_rates')) {
    const fundingRows = db.prepare(`
        SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour, AVG(rate) as funding_rate
        FROM funding_rates
        WHERE timestamp IS NOT NULL ${sql}
        GROUP BY hour
      `).all(params);
    for (const row of fundingRows) {
      const entry = market.get(row.hour) || { hour: row.hour };
      entry.funding_rate = finite(row.funding_rate);
      market.set(row.hour, entry);
    }
  }

  if (tableHasRows(db, 'onchain_hourly')) {
    const onchainRows = db.prepare(`
      SELECT hour,
        AVG(CASE
          WHEN direction = 'outflow' THEN -ABS(COALESCE(avg_magnitude, 0))
          WHEN direction = 'inflow' THEN ABS(COALESCE(avg_magnitude, 0))
          ELSE 0
        END) as liquidity_flow,
        NULL as liquidity_confidence
      FROM onchain_hourly
      WHERE (@since_hour IS NULL OR hour >= @since_hour)
      GROUP BY hour
    `).all({ since_hour: sinceHour });
    for (const row of onchainRows) {
      const entry = market.get(row.hour) || { hour: row.hour };
      entry.liquidity_flow = finite(row.liquidity_flow);
      entry.liquidity_confidence = finite(row.liquidity_confidence);
      market.set(row.hour, entry);
    }
  } else if (tableExists.get('onchain_data')) {
    const onchainRows = db.prepare(`
        SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
          AVG(CASE
            WHEN liquidity_flow_direction = 'outflow' THEN -ABS(COALESCE(liquidity_flow_magnitude, 0))
            WHEN liquidity_flow_direction = 'inflow' THEN ABS(COALESCE(liquidity_flow_magnitude, 0))
            ELSE 0
          END) as liquidity_flow,
          AVG(liquidity_flow_confidence) as liquidity_confidence
        FROM onchain_data
        WHERE timestamp IS NOT NULL ${sql}
        GROUP BY hour
      `).all(params);
    for (const row of onchainRows) {
      const entry = market.get(row.hour) || { hour: row.hour };
      entry.liquidity_flow = finite(row.liquidity_flow);
      entry.liquidity_confidence = finite(row.liquidity_confidence);
      market.set(row.hour, entry);
    }
  }

  if (tableExists.get('oi_snapshots')) {
    const oiRows = db.prepare(`
      SELECT strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
        AVG(total_oi) as oi_snapshot_total,
        AVG(put_oi) as oi_snapshot_put_oi,
        AVG(call_oi) as oi_snapshot_call_oi,
        AVG(pc_ratio) as oi_snapshot_pc,
        AVG(avg_put_iv) as oi_snapshot_put_iv,
        AVG(avg_call_iv) as oi_snapshot_call_iv
      FROM oi_snapshots
      WHERE timestamp IS NOT NULL ${sql}
      GROUP BY hour
    `).all(params);
    for (const row of oiRows) {
      const entry = market.get(row.hour) || { hour: row.hour };
      entry.oi_snapshot_total = finite(row.oi_snapshot_total);
      entry.oi_snapshot_put_oi = finite(row.oi_snapshot_put_oi);
      entry.oi_snapshot_call_oi = finite(row.oi_snapshot_call_oi);
      entry.oi_snapshot_pc = finite(row.oi_snapshot_pc);
      entry.oi_snapshot_put_iv = finite(row.oi_snapshot_put_iv);
      entry.oi_snapshot_call_iv = finite(row.oi_snapshot_call_iv);
      const snapshotSkew = entry.oi_snapshot_put_iv != null && entry.oi_snapshot_call_iv != null
        ? entry.oi_snapshot_put_iv - entry.oi_snapshot_call_iv
        : null;
      if (snapshotSkew != null) entry.market_skew = snapshotSkew;
      if (entry.oi_snapshot_total != null) entry.market_total_oi = entry.oi_snapshot_total;
      if (entry.oi_snapshot_put_oi != null) entry.market_put_oi = entry.oi_snapshot_put_oi;
      if (entry.oi_snapshot_call_oi != null) entry.market_call_oi = entry.oi_snapshot_call_oi;
      if (entry.oi_snapshot_pc != null) entry.market_pc_oi = entry.oi_snapshot_pc;
      if (entry.oi_snapshot_put_iv != null) entry.market_put_iv = entry.oi_snapshot_put_iv;
      if (entry.oi_snapshot_call_iv != null) entry.market_call_iv = entry.oi_snapshot_call_iv;
      market.set(row.hour, entry);
    }
  }

  return market;
}

function getMarketValue(marketByHour, hour, key) {
  return finite(marketByHour.get(hour)?.[key]);
}

function addMarketFeatures(features, marketByHour, hour, lookbackHours) {
  const current = marketByHour.get(hour) || {};
  const keys = [
    'market_best_call_score',
    'market_best_put_score',
    'market_avg_spread',
    'market_avg_depth',
    'market_total_oi',
    'market_call_oi',
    'market_put_oi',
    'market_pc_oi',
    'market_call_iv',
    'market_put_iv',
    'market_skew',
    'funding_rate',
    'liquidity_flow',
  ];

  for (const key of keys) {
    features[key] = finite(current[key]);
  }

  for (const hours of lookbackHours) {
    const prior = marketByHour.get(priorHour(hour, hours)) || {};
    features[`spot_ret_${hours}h`] = pctChange(current.spot_price, prior.spot_price);
    features[`market_best_call_score_delta_${hours}h`] = pctChange(current.market_best_call_score, prior.market_best_call_score);
    features[`market_best_put_score_delta_${hours}h`] = pctChange(current.market_best_put_score, prior.market_best_put_score);
    features[`market_spread_delta_${hours}h`] = pctChange(current.market_avg_spread, prior.market_avg_spread);
    features[`market_depth_delta_${hours}h`] = pctChange(current.market_avg_depth, prior.market_avg_depth);
    features[`market_oi_delta_${hours}h`] = pctChange(current.market_total_oi, prior.market_total_oi);
    features[`market_skew_delta_${hours}h`] = difference(current.market_skew, prior.market_skew);
    features[`funding_delta_${hours}h`] = difference(current.funding_rate, prior.funding_rate);
    features[`liquidity_flow_delta_${hours}h`] = difference(current.liquidity_flow, prior.liquidity_flow);
  }
}

function getCandidateSelect(action, sinceIso) {
  const cfg = ACTION_CONFIG[action];
  const scoreExpr = cfg.optionType === 'C'
    ? 'COALESCE(bid_delta_value, bid_price / NULLIF(ABS(delta), 0))'
    : 'COALESCE(ask_delta_value, ABS(delta) / NULLIF(ask_price, 0))';
  const optionFilter = cfg.optionType === 'C'
    ? "(option_type = 'C' OR instrument_name LIKE '%-C')"
    : "(option_type = 'P' OR instrument_name LIKE '%-P')";
  const sinceClause = sinceIso ? 'AND timestamp >= @since' : '';
  const params = {
    since: sinceIso,
    min_delta: cfg.minDelta,
    max_delta: cfg.maxDelta,
    min_dte: cfg.minDte,
    max_dte: cfg.maxDte,
  };
  const select = `
      SELECT
        timestamp as observed_at,
        strftime('%Y-%m-%dT%H:00:00Z', timestamp) as hour,
        instrument_name,
        option_type,
        strike,
        expiry,
        (expiry - strftime('%s', timestamp)) / 86400.0 as dte,
        delta,
        ask_price,
        bid_price,
        mark_price,
        ask_amount,
        bid_amount,
        implied_vol,
        open_interest,
        ${scoreExpr} as raw_score,
        CASE WHEN ask_price > 0 AND bid_price > 0 AND mark_price > 0 THEN (ask_price - bid_price) / mark_price END as spread_pct,
        COALESCE(ask_amount, 0) + COALESCE(bid_amount, 0) as depth
      FROM options_snapshots
      WHERE timestamp IS NOT NULL
        ${sinceClause}
        AND ${optionFilter}
        AND delta >= @min_delta
        AND delta <= @max_delta
        AND ask_price > 0
        AND bid_price > 0
        AND ABS(delta) > 0
        AND expiry IS NOT NULL
        AND (expiry - strftime('%s', timestamp)) / 86400.0 >= @min_dte
        AND (expiry - strftime('%s', timestamp)) / 86400.0 <= @max_dte
  `;
  return { select, params };
}

function getAllCandidatesSql(action, sinceIso) {
  const { select, params } = getCandidateSelect(action, sinceIso);
  return {
    sql: `
      SELECT *, 1 as candidate_rank
      FROM (${select})
      WHERE raw_score IS NOT NULL
      ORDER BY observed_at ASC, raw_score DESC
    `,
    params,
  };
}

function getTopHourCandidateStatement(db, action, sinceIso) {
  const { select, params } = getCandidateSelect(action, sinceIso);
  const sql = `
    SELECT *, ROW_NUMBER() OVER (ORDER BY raw_score DESC, depth DESC) as candidate_rank
    FROM (${select} AND timestamp >= @hour_start AND timestamp < @hour_end)
    WHERE raw_score IS NOT NULL
    ORDER BY raw_score DESC, depth DESC
    LIMIT @top_per_hour
  `;
  return {
    stmt: db.prepare(sql),
    baseParams: params,
  };
}

function* iterateCandidates(db, action, opts, marketByHour) {
  if (opts.sampleMode === 'all-candidates') {
    const candidateQuery = getAllCandidatesSql(action, opts.sinceIso);
    for (const row of db.prepare(candidateQuery.sql).iterate(candidateQuery.params)) {
      yield row;
    }
    return;
  }

  const { stmt, baseParams } = getTopHourCandidateStatement(db, action, opts.sinceIso);
  const hours = [...marketByHour.keys()].sort();
  for (const hour of hours) {
    const hourStart = hour;
    const hourEnd = addHours(hour, 1);
    const rows = stmt.all({
      ...baseParams,
      hour_start: hourStart,
      hour_end: hourEnd,
      top_per_hour: opts.topPerHour,
    });
    for (const row of rows) yield row;
  }
}

function getCandidateSql(action, sinceIso, sampleMode, topPerHour) {
  const candidateQuery = getAllCandidatesSql(action, sinceIso);
  if (sampleMode === 'all-candidates') {
    return candidateQuery;
  }
  return {
    sql: candidateQuery.sql,
    params: { ...candidateQuery.params, top_per_hour: topPerHour },
  };
}

function prepareOutcomeQueries(db) {
  return {
    futureQuote: db.prepare(`
      SELECT timestamp, bid_price, ask_price, mark_price, delta, ask_delta_value, bid_delta_value
      FROM options_snapshots
      WHERE instrument_name = @instrument_name
        AND timestamp >= @due_at
        AND timestamp < @until
      ORDER BY timestamp ASC
      LIMIT 1
    `),
    futureSpot: db.prepare(`
      SELECT timestamp, price
      FROM spot_prices
      WHERE timestamp >= @due_at
        AND timestamp < @until
        AND price BETWEEN 100 AND 20000
      ORDER BY timestamp ASC
      LIMIT 1
    `),
    priorOption: db.prepare(`
      SELECT timestamp, bid_price, ask_price, mark_price, delta, ask_delta_value, bid_delta_value,
        ask_amount, bid_amount, implied_vol, open_interest,
        CASE WHEN ask_price > 0 AND bid_price > 0 AND mark_price > 0 THEN (ask_price - bid_price) / mark_price END as spread_pct,
        COALESCE(ask_amount, 0) + COALESCE(bid_amount, 0) as depth
      FROM options_snapshots
      WHERE instrument_name = @instrument_name
        AND timestamp <= @before
      ORDER BY timestamp DESC
      LIMIT 1
    `),
  };
}

function candidateScore(row, cfg) {
  if (cfg.optionType === 'C') {
    return finite(row.raw_score) ?? (finite(row.bid_price) != null && Math.abs(finite(row.delta) || 0) > 0
      ? finite(row.bid_price) / Math.abs(finite(row.delta))
      : null);
  }
  return finite(row.raw_score) ?? (finite(row.ask_price) != null && Math.abs(finite(row.delta) || 0) > 0
    ? Math.abs(finite(row.delta)) / finite(row.ask_price)
    : null);
}

function addOptionDeltaFeatures(features, row, cfg, queries, lookbackHours) {
  const currentScore = candidateScore(row, cfg);
  for (const hours of lookbackHours) {
    const prior = queries.priorOption.get({
      instrument_name: row.instrument_name,
      before: addHours(row.observed_at, -hours),
    });
    if (!prior) continue;
    const priorScore = cfg.optionType === 'C'
      ? (finite(prior.bid_delta_value) ?? (finite(prior.bid_price) != null && Math.abs(finite(prior.delta) || 0) > 0 ? finite(prior.bid_price) / Math.abs(finite(prior.delta)) : null))
      : (finite(prior.ask_delta_value) ?? (finite(prior.ask_price) != null && Math.abs(finite(prior.delta) || 0) > 0 ? Math.abs(finite(prior.delta)) / finite(prior.ask_price) : null));
    features[`score_delta_${hours}h`] = pctChange(currentScore, priorScore);
    features[`bid_delta_${hours}h`] = pctChange(row.bid_price, prior.bid_price);
    features[`ask_delta_${hours}h`] = pctChange(row.ask_price, prior.ask_price);
    features[`mark_delta_${hours}h`] = pctChange(row.mark_price, prior.mark_price);
    features[`iv_delta_${hours}h`] = difference(row.implied_vol, prior.implied_vol);
    features[`oi_delta_${hours}h`] = pctChange(row.open_interest, prior.open_interest);
    features[`depth_delta_${hours}h`] = pctChange(row.depth, prior.depth);
    features[`spread_delta_${hours}h`] = difference(row.spread_pct, prior.spread_pct);
  }
}

function buildFeatures(row, action, marketByHour, queries, lookbackHours) {
  const cfg = ACTION_CONFIG[action];
  const spot = getMarketValue(marketByHour, row.hour, 'spot_price');
  const strike = finite(row.strike);
  const delta = finite(row.delta);
  const features = {
    raw_score: candidateScore(row, cfg),
    dte: finite(row.dte),
    abs_delta: delta == null ? null : Math.abs(delta),
    strike_distance_pct: spot && strike ? (strike / spot) - 1 : null,
    bid_price: finite(row.bid_price),
    ask_price: finite(row.ask_price),
    mark_price: finite(row.mark_price),
    spread_pct: finite(row.spread_pct),
    depth: finite(row.depth),
    implied_vol: finite(row.implied_vol),
    open_interest: finite(row.open_interest),
  };
  addMarketFeatures(features, marketByHour, row.hour, lookbackHours);
  addOptionDeltaFeatures(features, row, cfg, queries, lookbackHours);
  return features;
}

function buildOutcome(row, action, horizonHours, cfg, queries, quoteWindowHours, marketByHour) {
  const dueAt = addHours(row.observed_at, horizonHours);
  const until = addHours(dueAt, quoteWindowHours);
  const quote = queries.futureQuote.get({
    instrument_name: row.instrument_name,
    due_at: dueAt,
    until,
  });
  if (!quote) return null;
  const spot = queries.futureSpot.get({ due_at: dueAt, until });
  const entryPrice = finite(row[cfg.entryPriceField]);
  const futureExitPrice = finite(quote[cfg.exitPriceField]);
  if (!(entryPrice > 0) || futureExitPrice == null) return null;
  const pnl = action === 'sell_call'
    ? entryPrice - futureExitPrice
    : futureExitPrice - entryPrice;
  const currentSpot = getMarketValue(marketByHour, row.hour, 'spot_price');
  const futureSpot = finite(spot?.price);
  return {
    horizon_hours: horizonHours,
    future_quote_at: quote.timestamp,
    future_spot_at: spot?.timestamp || null,
    pnl,
    return_on_premium: pnl / entryPrice,
    win: cfg.favorableOutcome(pnl),
    tail_loss: cfg.tailLoss(entryPrice, futureExitPrice),
    spot_return: futureSpot != null && currentSpot > 0 ? (futureSpot / currentSpot) - 1 : null,
    future_bid: finite(quote.bid_price),
    future_ask: finite(quote.ask_price),
    future_mark: finite(quote.mark_price),
  };
}

function buildRecommendations(action, horizonReport) {
  const recommendations = [];
  const bestFeatures = horizonReport.features.slice(0, 8);
  for (const feature of bestFeatures) {
    if (!feature.bucket || feature.bucket.edge_delta_return == null) continue;
    recommendations.push({
      type: 'feature',
      action,
      horizon_hours: horizonReport.horizon_hours,
      feature: feature.name,
      direction: feature.bucket.better,
      threshold: feature.bucket.better === 'higher' ? feature.bucket.high_threshold : feature.bucket.low_threshold,
      edge_delta_return: feature.bucket.edge_delta_return,
      samples: feature.samples,
      text: `${feature.bucket.better} ${feature.label} showed better ${action} outcomes at ${horizonReport.horizon_hours}h`,
    });
  }
  for (const interaction of horizonReport.interactions.slice(0, 5)) {
    if ((interaction.edge_vs_base_return ?? 0) <= 0) continue;
    recommendations.push({
      type: 'interaction',
      action,
      horizon_hours: horizonReport.horizon_hours,
      features: interaction.features,
      edge_vs_base_return: interaction.edge_vs_base_return,
      samples: interaction.samples,
      text: `Combination edge: ${interaction.features.map((f) => `${f.direction} ${f.label}`).join(' + ')}`,
    });
  }
  return recommendations;
}

function analyzeAction(samplesByHorizon, action, minSamples) {
  const horizons = {};
  const recommendations = [];
  for (const [horizon, samples] of Object.entries(samplesByHorizon)) {
    const overall = summarizeOutcomes(samples);
    const features = analyzeFeatures(samples, minSamples);
    const interactions = analyzeInteractions(samples, features, minSamples);
    const horizonReport = {
      horizon_hours: Number(horizon),
      overall,
      features: features.slice(0, 40),
      interactions: interactions.slice(0, 30),
    };
    horizons[horizon] = horizonReport;
    recommendations.push(...buildRecommendations(action, horizonReport));
  }
  return {
    action,
    description: ACTION_CONFIG[action].description,
    horizons,
    recommendations: recommendations
      .sort((a, b) => Math.abs(b.edge_delta_return ?? b.edge_vs_base_return ?? 0) - Math.abs(a.edge_delta_return ?? a.edge_vs_base_return ?? 0))
      .slice(0, 25),
  };
}

function normalizeOptions(options = {}) {
  const days = options.days === 'all' || options.days == null ? null : Number(options.days);
  const sinceIso = days && days > 0 ? addHours(new Date().toISOString(), -days * 24) : null;
  const actions = Array.isArray(options.actions) && options.actions.length
    ? options.actions.filter((action) => ACTION_CONFIG[action])
    : Object.keys(ACTION_CONFIG);
  return {
    actions,
    sinceIso,
    days: days || 'all',
    horizonsHours: options.horizonsHours || DEFAULT_HORIZONS_HOURS,
    lookbackHours: options.lookbackHours || DEFAULT_LOOKBACK_HOURS,
    minSamples: Number(options.minSamples || DEFAULT_MIN_SAMPLES),
    topPerHour: Number(options.topPerHour || DEFAULT_TOP_PER_HOUR),
    sampleMode: options.sampleMode || 'top-hour',
    maxSamples: options.maxSamples != null ? Number(options.maxSamples) : null,
    quoteWindowHours: Number(options.quoteWindowHours || DEFAULT_QUOTE_WINDOW_HOURS),
  };
}

function buildProfitCorrelationReport(db, options = {}) {
  const opts = normalizeOptions(options);
  const startedAt = new Date().toISOString();
  const marketByHour = loadMarketHourly(db, opts.sinceIso);
  const queries = prepareOutcomeQueries(db);
  const coverage = getCoverage(db);
  const actions = {};

  for (const action of opts.actions) {
    const cfg = ACTION_CONFIG[action];
    const samplesByHorizon = Object.fromEntries(opts.horizonsHours.map((h) => [String(h), []]));
    let candidatesScanned = 0;
    let candidatesWithAnyOutcome = 0;
    for (const row of iterateCandidates(db, action, opts, marketByHour)) {
      if (opts.maxSamples && candidatesScanned >= opts.maxSamples) break;
      candidatesScanned++;
      const features = buildFeatures(row, action, marketByHour, queries, opts.lookbackHours);
      let hadOutcome = false;
      for (const horizon of opts.horizonsHours) {
        const outcome = buildOutcome(row, action, horizon, cfg, queries, opts.quoteWindowHours, marketByHour);
        if (!outcome) continue;
        hadOutcome = true;
        samplesByHorizon[String(horizon)].push({
          action,
          observed_at: row.observed_at,
          hour: row.hour,
          instrument_name: row.instrument_name,
          candidate_rank: finite(row.candidate_rank),
          features,
          outcome,
        });
      }
      if (hadOutcome) candidatesWithAnyOutcome++;
    }
    actions[action] = {
      ...analyzeAction(samplesByHorizon, action, opts.minSamples),
      candidates_scanned: candidatesScanned,
      candidates_with_any_outcome: candidatesWithAnyOutcome,
    };
  }

  return {
    meta: {
      computed_at: new Date().toISOString(),
      started_at: startedAt,
      engine: 'profit-correlation-v1',
      days: opts.days,
      sample_mode: opts.sampleMode,
      top_per_hour: opts.sampleMode === 'top-hour' ? opts.topPerHour : null,
      max_samples_per_action: opts.maxSamples,
      horizons_hours: opts.horizonsHours,
      lookback_hours: opts.lookbackHours,
      min_samples: opts.minSamples,
      quote_window_hours: opts.quoteWindowHours,
      note: 'Historical research only. Reconstructed candidates are sampled from options_snapshots and should be validated before changing live trading gates.',
      coverage,
    },
    actions,
  };
}

function renderMarkdownReport(report) {
  const lines = [];
  lines.push('# Profit Correlation Report');
  lines.push('');
  lines.push(`Computed: ${report.meta.computed_at}`);
  lines.push(`Window: ${report.meta.days}; sample mode: ${report.meta.sample_mode}`);
  lines.push('');
  for (const actionReport of Object.values(report.actions)) {
    lines.push(`## ${actionReport.action}`);
    lines.push('');
    lines.push(actionReport.description);
    lines.push('');
    lines.push(`Candidates scanned: ${actionReport.candidates_scanned}; with outcome: ${actionReport.candidates_with_any_outcome}`);
    lines.push('');
    for (const horizon of Object.values(actionReport.horizons)) {
      lines.push(`### ${horizon.horizon_hours}h`);
      lines.push('');
      lines.push(`Samples: ${horizon.overall.samples}; mean return: ${round(horizon.overall.mean_return, 4)}; win rate: ${round(horizon.overall.win_rate, 4)}; tail loss: ${round(horizon.overall.tail_loss_rate, 4)}`);
      lines.push('');
      lines.push('Top features:');
      for (const feature of horizon.features.slice(0, 10)) {
        const direction = feature.bucket?.better || 'n/a';
        const threshold = direction === 'higher' ? feature.bucket?.high_threshold : feature.bucket?.low_threshold;
        lines.push(`- ${feature.label}: prefer ${direction}${threshold != null ? ` than ${threshold}` : ''}; edge=${feature.bucket?.edge_delta_return ?? null}; spearman=${feature.spearman_return}; n=${feature.samples}`);
      }
      lines.push('');
      if (horizon.interactions.length > 0) {
        const positiveInteractions = horizon.interactions
          .filter((interaction) => (interaction.edge_vs_base_return ?? 0) > 0)
          .slice(0, 5);
        const negativeInteractions = horizon.interactions
          .filter((interaction) => (interaction.edge_vs_base_return ?? 0) < 0)
          .slice(0, 5);
        if (positiveInteractions.length > 0) {
          lines.push('Best interactions:');
          for (const interaction of positiveInteractions) {
            lines.push(`- ${interaction.features.map((f) => `${f.direction} ${f.label}`).join(' + ')}: edge_vs_base=${interaction.edge_vs_base_return}; n=${interaction.samples}`);
          }
          lines.push('');
        }
        if (negativeInteractions.length > 0) {
          lines.push('Avoid interactions:');
          for (const interaction of negativeInteractions) {
            lines.push(`- ${interaction.features.map((f) => `${f.direction} ${f.label}`).join(' + ')}: edge_vs_base=${interaction.edge_vs_base_return}; n=${interaction.samples}`);
          }
          lines.push('');
        }
      }
    }
  }
  return `${lines.join('\n')}\n`;
}

module.exports = {
  ACTION_CONFIG,
  DEFAULT_HORIZONS_HOURS,
  buildProfitCorrelationReport,
  renderMarkdownReport,
  pearson,
  spearman,
  summarizeOutcomes,
};
