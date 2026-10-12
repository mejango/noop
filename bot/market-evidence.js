'use strict';

// Shared, read-only market evidence. No execution rules or database side effects.
const { zoneMetrics, riskReversal25, pickZoneExpiry, realizedVol, percentile } = require('./vol-surface');
const { invalidDexReason, dexForAdvisor, LIQUIDITY_DATA_NOTICE } = require('./liquidity-quality');
const zones = require('./strategy-facts.json');
const { volumeSummary } = require('./dex-volume');
const HOUR = 3600000;
const WINDOWS = { '1h': 1, '24h': 24, '7d': 168, '30d': 720, '90d': 2160 };
const SMILE_FEATURES = {
  call_atm: 'Call-zone ATM IV', put_atm: 'Put-zone ATM IV',
  call_wing_premium: 'Call 10-delta wing premium over ATM',
  put_wing_premium: 'Put 10-delta wing premium over ATM',
  call_rr25: 'Call-zone 25-delta call-minus-put risk reversal',
  put_rr25: 'Put-zone 25-delta call-minus-put risk reversal',
  term_slope: 'Put-zone minus call-zone ATM IV',
  call_vrp: 'Call ATM IV minus completed 7d realized volatility',
  put_vrp: 'Put ATM IV minus completed 30d realized volatility',
};
const HISTORY_SQL = {
  smile: `WITH picks AS (SELECT MAX(timestamp) ts FROM iv_smile_snapshots
    WHERE timestamp >= @since AND timestamp <= @before GROUP BY substr(timestamp,1,13))
    SELECT s.timestamp,s.expiry,s.points FROM iv_smile_snapshots s JOIN picks p ON s.timestamp=p.ts ORDER BY s.timestamp,s.expiry`,
  liquidity: `WITH picks AS (SELECT MAX(timestamp) ts FROM onchain_data
    WHERE timestamp >= @since AND timestamp <= @before GROUP BY substr(timestamp,1,13))
    SELECT o.timestamp,o.raw_data FROM onchain_data o JOIN picks p ON o.timestamp=p.ts ORDER BY o.timestamp`,
  near: `SELECT timestamp,raw_data FROM onchain_data WHERE timestamp >= @since AND timestamp <= @before
    ORDER BY ABS(julianday(timestamp)-julianday(@target)) LIMIT 1`,
};
const number = v => typeof v === 'number' && Number.isFinite(v) ? v : null;
const iso = ms => new Date(ms).toISOString();
const round = v => v == null ? null : Number(v.toFixed(3));

function completedCloses(spotRows, atMs, hours) {
  const end = Math.floor(atMs / HOUR) * HOUR - HOUR;
  const map = spotRows instanceof Map ? spotRows : new Map(spotRows.map(r => [Date.parse(r.hour), r.close]));
  const closes = [];
  for (let h = end - hours * HOUR; h <= end; h += HOUR) {
    const value = number(map.get(h));
    if (!(value > 0)) return []; // Exact requested window; gaps are never bridged.
    closes.push(value);
  }
  return closes;
}

function buildSmileHistory(rows, spotRows, beforeMs = Date.now()) {
  const snapshots = new Map();
  for (const row of rows) {
    const at = Date.parse(row.timestamp);
    if (!Number.isFinite(at) || at > beforeMs) continue;
    try {
      const points = (typeof row.points === 'string' ? JSON.parse(row.points) : row.points)
        .filter(p => number(p[2]) != null && number(p[3]) > 0)
        .map(([, call, delta, iv]) => ({ type: call ? 'C' : 'P', delta, iv: iv * 100 }));
      if (!snapshots.has(at)) snapshots.set(at, []);
      snapshots.get(at).push({ expiry: row.expiry, points });
    } catch { /* A corrupt smile row supplies no evidence. */ }
  }
  const spotMap = new Map(spotRows.map(r => [Date.parse(r.hour), r.close]));
  const history = [];
  for (const [at, expiries] of [...snapshots].sort((a, b) => a[0] - b[0])) {
    const zone = (type, range) => {
      const pick = pickZoneExpiry(expiries.filter(e => e.points.some(p => p.type === type)).map(e => e.expiry), at, range);
      if (!pick) return null;
      const points = expiries.find(e => e.expiry === pick.expiry).points;
      return { expiry: pick.expiry, ...zoneMetrics(points, type), rr25: riskReversal25(points) };
    };
    const call = zone('C', zones.call_dte_range), put = zone('P', zones.put_dte_range);
    const rv7 = realizedVol(completedCloses(spotMap, at, 168));
    const rv30 = realizedVol(completedCloses(spotMap, at, 720));
    history.push({ at, hour: iso(Math.floor(at / HOUR) * HOUR),
      call_expiry: call?.expiry ?? null, put_expiry: put?.expiry ?? null,
      call_atm: call?.atm ?? null, put_atm: put?.atm ?? null,
      call_wing_premium: call?.wing_premium ?? null, put_wing_premium: put?.wing_premium ?? null,
      call_rr25: call?.rr25 ?? null, put_rr25: put?.rr25 ?? null,
      term_slope: call?.atm != null && put?.atm != null ? put.atm - call.atm : null,
      call_vrp: call?.atm != null && rv7 != null ? call.atm - rv7 : null,
      put_vrp: put?.atm != null && rv30 != null ? put.atm - rv30 : null });
  }
  return history;
}

function sameExpiries(a, b, key) {
  const sides = key.startsWith('call_') ? ['call'] : key.startsWith('put_') ? ['put'] : ['call', 'put'];
  return sides.every(side => a[`${side}_expiry`] != null && a[`${side}_expiry`] === b[`${side}_expiry`]);
}

function smileChangeSeries(history) {
  return Object.fromEntries(Object.keys(SMILE_FEATURES).map(key => [`${key}_change`, history.map((row, i) => {
    const prev = history[i - 1];
    const consecutive = prev && Date.parse(row.hour) - Date.parse(prev.hour) === HOUR;
    return { hour: row.hour, value: consecutive && sameExpiries(row, prev, key)
      && number(row[key]) != null && number(prev[key]) != null ? row[key] - prev[key] : null };
  })]));
}

function coverage(rows, nowMs, hours, value = r => r) {
  const xs = rows.filter(r => r.at >= Math.floor(nowMs / HOUR) * HOUR - hours * HOUR && r.at < Math.floor(nowMs / HOUR) * HOUR && value(r) != null);
  return { observed_hours: new Set(xs.map(r => Math.floor(r.at / HOUR))).size, expected_hours: hours,
    first_at: xs.length ? iso(xs[0].at) : null, last_at: xs.length ? iso(xs[xs.length - 1].at) : null };
}

function summarizeSmile(history, nowMs) {
  const latest = history.at(-1);
  if (!latest || nowMs - latest.at > HOUR) return { status: 'unavailable', reason: 'missing_or_stale_snapshot', features: {} };
  const features = {};
  for (const key of Object.keys(SMILE_FEATURES)) {
    const trends = {};
    for (const [label, hours] of Object.entries(WINDOWS)) {
      const target = latest.at - hours * HOUR;
      const past = history.filter(r => Math.abs(r.at - target) <= HOUR && sameExpiries(latest, r, key))
        .sort((a, b) => Math.abs(a.at - target) - Math.abs(b.at - target))[0];
      trends[label] = number(latest[key]) != null && number(past?.[key]) != null ? round(latest[key] - past[key]) : null;
    }
    const windows = {};
    for (const days of [7, 30, 90]) {
      const samples = history.filter(r => r.at >= Math.floor(nowMs/HOUR)*HOUR - days*24*HOUR && r.at < Math.floor(nowMs/HOUR)*HOUR);
      const covered = coverage(history,nowMs,days*24,r=>r[key]);
      const sufficient = covered.observed_hours >= covered.expected_hours * 0.75;
      windows[`${days}d`] = { ...covered, status: sufficient ? 'sufficient_coverage' : 'insufficient_coverage',
        percentile: sufficient ? percentile(latest[key], samples.map(r => r[key])) : null };
    }
    features[key] = { value: round(latest[key]), changes_vol_points: trends, windows };
  }
  return { status: 'valid', as_of: iso(latest.at), call_expiry: latest.call_expiry, put_expiry: latest.put_expiry,
    features, notice: 'Changes require the same expiry; rolls and missing hours are excluded from change correlations. Percentiles compare rolling DTE zones and can include different expiries; they are descriptive, not constant-maturity estimates. Window percentiles require at least 75% valid completed hourly coverage. RV requires every completed hourly close in the named window.' };
}

function parseLiquidityRows(rows, nowMs) {
  return rows.flatMap(row => {
    try {
      const at = Date.parse(row.timestamp), raw = typeof row.raw_data === 'string' ? JSON.parse(row.raw_data) : row.raw_data;
      return Number.isFinite(at) && at <= nowMs ? [{ at, dexes: raw?.dexLiquidity?.dexes || {} }] : [];
    } catch { return []; }
  }).sort((a, b) => a.at - b.at);
}
function poolId(p) { return p.id || p.poolId || p.address; }
function poolSignature(dex) {
  const pools = dex?.poolDetails || [];
  if (!pools.length || pools.some(p => !poolId(p) || !p.token0 || !p.token1)) return null;
  return JSON.stringify(pools.map(p => [String(poolId(p)).toLowerCase(), p.token0.id || p.token0.address || p.token0.symbol,
    p.token1.id || p.token1.address || p.token1.symbol, p.feeTier]).sort((a,b) => a[0].localeCompare(b[0])));
}
function summarizeLiquidity(rows, nowMs) {
  const samples = parseLiquidityRows(rows, nowMs), latest = samples.at(-1);
  const sourceTime = dex => typeof dex?.sourceTimestamp === 'number' ? dex.sourceTimestamp * 1000 : Date.parse(dex?.sourceTimestamp);
  const freshAt = (dex,at) => Number.isFinite(sourceTime(dex)) && at-sourceTime(dex) <= 15*60000 && sourceTime(dex)-at <= 60000;
  return { as_of: latest ? iso(latest.at) : null, notice: LIQUIDITY_DATA_NOTICE,
    interpretation: 'USD TVL change is not net deposit/withdrawal flow. Token reserves can also change through swaps. Selected pools do not represent protocol-wide liquidity.',
    dexes: ['uniswap_v3', 'uniswap_v4'].map(name => {
      const dex = latest?.dexes[name];
      const current = dexForAdvisor(name, dex);
      const signature = poolSignature(dex);
      const sourceMs = typeof dex?.sourceTimestamp === 'number' ? dex.sourceTimestamp * 1000 : Date.parse(dex?.sourceTimestamp);
      const reason = invalidDexReason(name, dex) || (!signature ? 'missing_pool_identity' : null)
        || (!latest || nowMs - latest.at > 15 * 60000 || !Number.isFinite(sourceMs) || nowMs - sourceMs > 15 * 60000 || sourceMs > nowMs + 60000 ? 'missing_or_stale_source' : null);
      if (reason) return { ...current, status: 'unavailable', unavailable_reason: reason, total_liquidity: null, top_pool: null, changes: {}, coverage: {} };
      const comparable = samples.filter(s => !invalidDexReason(name, s.dexes[name]) && freshAt(s.dexes[name],s.at)
        && dex.tvlDefinition === s.dexes[name].tvlDefinition && signature === poolSignature(s.dexes[name]));
      const changes = {};
      for (const [label, hours] of Object.entries(WINDOWS)) {
        const target = latest.at - hours * HOUR;
        const past = comparable.filter(s => s.at < latest.at && Math.abs(s.at - target) <= 15 * 60000)
          .sort((a,b) => Math.abs(a.at-target)-Math.abs(b.at-target))[0];
        changes[label] = !past ? { usd_pct: null, reason: 'no_comparable_baseline', pools: [] } : {
          usd_pct: round((dex.totalLiquidity / past.dexes[name].totalLiquidity - 1) * 100),
          baseline_at: iso(past.at), pools: dex.poolDetails.map(pool => {
            const previous = past.dexes[name].poolDetails.find(p => poolId(p) === poolId(pool));
            const change = key => pool[key] != null && previous?.[key] != null ? Number(pool[key]) - Number(previous[key]) : null;
            return { id: poolId(pool), token0_delta: change('tvlToken0'), token1_delta: change('tvlToken1') };
          }) };
      }
      return { ...current, reserves: dex.poolDetails.map(p => ({ id: poolId(p), token0: p.token0.symbol, token1: p.token1.symbol,
        token0_amount: p.tvlToken0, token1_amount: p.tvlToken1 })), changes,
        coverage: Object.fromEntries([7,30,90].map(days => [`${days}d`, coverage(comparable,nowMs,days*24)])) };
    }) };
}

const historyCache = new WeakMap();
function readSmileHistory(db, nowMs = Date.now(), days = 90) {
  if (!db.getSmileHistoryRows || !db.getSpotHourlyCloses) return [];
  const cached = historyCache.get(db);
  if (cached && cached.days >= days && nowMs >= cached.at && nowMs - cached.at < 300000) return cached.history.filter(r => r.at >= nowMs - (days*24+1)*HOUR);
  const since = iso(nowMs - (days * 24 + 1) * HOUR);
  const rows = db.getSmileHistoryRows({ since, before: iso(nowMs) });
  const spots = db.getSpotHourlyCloses(iso(nowMs - (days + 31) * 24 * HOUR));
  const history = buildSmileHistory(rows, spots, nowMs);
  historyCache.set(db, { at: nowMs, days, history });
  return history;
}
function buildMarketEvidence(db, nowMs = Date.now()) {
  const rows = db.getLiquidityHistoryRows?.({ since: iso(nowMs - 90 * 24 * HOUR - HOUR), before: iso(nowMs) }) || [];
  for (const hours of Object.values(WINDOWS)) {
    const target = nowMs - hours * HOUR;
    const near = db.getLiquidityNear?.({ since: iso(target - 15*60000), before: iso(target + 15*60000), target: iso(target) });
    if (near) rows.push(near);
  }
  const correlations = require('./correlation').buildCorrelationAnalysis(db,nowMs);
  const outcomes = new Set(['spot_return','best_put_dv','best_call_dv']);
  const relevant = correlations.pairs.filter(p => (p.series_a.endsWith('_change') || p.series_b.endsWith('_change') || p.series_a === 'liquidity_flow' || p.series_b === 'liquidity_flow') && (outcomes.has(p.series_a) || outcomes.has(p.series_b)));
  const liquidity = summarizeLiquidity(rows,nowMs);
  if (db.getDexVolumeHistory) {
    const volumes=volumeSummary(db,nowMs);
    for (const dex of liquidity.dexes) {
      const volume=volumes[dex.dex==='uniswap_v3' ? 'uniswap_v3_onchain' : dex.dex];
      if (volume) Object.assign(dex,{volume_source:volume.volumeSource,volume_24h_usd:volume.volume24hUSD,
        volume_window_start:volume.volumeWindowStart,volume_window_end:volume.volumeWindowEnd,
        volume_observed_hours:volume.volumeObservedHours,volume_expected_hours:volume.volumeExpectedHours,
        volume_unavailable_reason:volume.volumeUnavailableReason});
    }
  }
  return { schema: 'market_history_v1',
    correlations: { pairs: relevant.slice(0,24), notice: correlations.notice, coverage: correlations.coverage }, computed_at: iso(nowMs),
    liquidity, smile: summarizeSmile(readSmileHistory(db,nowMs),nowMs),
    interpretation: '7d describes recent conditions; 30d/90d can support slower patterns only with sufficient coverage. Missing history is unknown. These are descriptive observations, not causal evidence or permission to override EDGE, budgets or risk limits.' };
}
function formatMarketEvidence(e) {
  return e ? JSON.stringify(e) : 'Historical market evidence unavailable.';
}
module.exports = { HISTORY_SQL, SMILE_FEATURES, completedCloses, buildSmileHistory, smileChangeSeries,
  readSmileHistory, summarizeSmile, summarizeLiquidity, buildMarketEvidence, formatMarketEvidence };
