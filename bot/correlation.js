const { SMILE_FEATURES, readSmileHistory, smileChangeSeries } = require('./market-evidence');
// ─── Pearson Correlation ────────────────────────────────────────────────────

function pearson(xs, ys) {
  const pairs = [];
  const len = Math.min(xs.length, ys.length);
  for (let i = 0; i < len; i++) {
    if (xs[i] != null && ys[i] != null) {
      pairs.push([xs[i], ys[i]]);
    }
  }
  if (pairs.length < 10) return null;

  const n = pairs.length;
  let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0;
  for (const [x, y] of pairs) {
    sumX += x;
    sumY += y;
    sumXY += x * y;
    sumX2 += x * x;
    sumY2 += y * y;
  }

  const denom = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));
  if (denom === 0) return null;

  return (n * sumXY - sumX * sumY) / denom;
}

// ─── Lead/Lag Analysis ──────────────────────────────────────────────────────

const LAG_OFFSETS = [1, 3, 6, 12, 24]; // hours

function computeLeadLag(a, b) {
  const contemp = pearson(a, b);
  if (contemp == null) return null;

  let bestOffset = 0;
  let bestR = Math.abs(contemp);
  let bestRSigned = contemp;

  for (const offset of LAG_OFFSETS) {
    // a leads b: shift a forward
    const rALead = pearson(a.slice(0, a.length - offset), b.slice(offset));
    if (rALead != null && Math.abs(rALead) > bestR) {
      bestR = Math.abs(rALead);
      bestRSigned = rALead;
      bestOffset = offset;
    }

    // b leads a: shift b forward
    const rBLead = pearson(a.slice(offset), b.slice(0, b.length - offset));
    if (rBLead != null && Math.abs(rBLead) > bestR) {
      bestR = Math.abs(rBLead);
      bestRSigned = rBLead;
      bestOffset = -offset;
    }
  }

  return {
    best_offset: bestOffset,
    best_r: bestRSigned,
    contemporaneous_r: contemp,
    a_leads: bestOffset > 0,
  };
}

// ─── Series Extraction ──────────────────────────────────────────────────────

const SERIES_DEFS = [
  {
    name: 'spot_return',
    description: 'ETH hourly % price change',
    extract: (db, since) => {
      const rows = db.getSpotPricesHourly(since);
      const returns = [];
      for (let i = 1; i < rows.length; i++) {
        const prev = rows[i - 1].avg_price;
        const curr = rows[i].avg_price;
        returns.push({ hour: rows[i].hour, value: prev > 0 && Date.parse(rows[i].hour) - Date.parse(rows[i - 1].hour) === 3600000 ? ((curr - prev) / prev) * 100 : null });
      }
      return returns;
    },
  },
  {
    name: 'liquidity_flow',
    description: 'Validated selected-pool signed USD TVL change; prices and swaps also affect TVL, not deposit/withdrawal flow',
    extract: (db, since) => {
      const rows = db.getOnchainHourly(since);
      return rows.map(r => ({
        hour: r.hour,
        value: r.avg_magnitude != null && r.direction != null
          ? (r.direction === 'outflow' ? -r.avg_magnitude : r.avg_magnitude)
          : null,
      }));
    },
  },
  {
    name: 'best_put_dv',
    description: 'Best PUT delta-value score (higher = cheaper protection)',
    extract: (db, since) => db.getBestPutDvHourly(since),
  },
  {
    name: 'best_call_dv',
    description: 'Best CALL delta-value score (higher = richer premium to sell)',
    extract: (db, since) => db.getBestCallDvHourly(since),
  },
  {
    name: 'options_spread',
    description: 'Avg bid-ask spread as % of mark price (higher = worse liquidity)',
    extract: (db, since) => db.getOptionsSpreadHourly(since),
  },
  {
    name: 'options_depth',
    description: 'Avg order book depth (ask_amount + bid_amount) in ETH',
    extract: (db, since) => db.getOptionsDepthHourly(since),
  },
  {
    name: 'open_interest',
    description: 'Latest OI per observed instrument in completed hours; coverage can change',
    extract: (db, since) => db.getOpenInterestHourly(since),
  },
  {
    name: 'implied_vol',
    description: 'Avg implied volatility within trading delta range',
    extract: (db, since) => db.getImpliedVolHourly(since),
  },
  {
    name: 'funding_rate',
    description: 'Derive ETH-PERP hourly funding rate (positive=longs pay, negative=shorts pay)',
    extract: (db, since) => {
      if (!db.getFundingRatesHourly) return [];
      return db.getFundingRatesHourly(since).map(r => ({
        hour: r.hour,
        value: r.avg_rate != null ? r.avg_rate * 100 : null, // convert to percentage
      }));
    },
  },
];

const SMILE_DEFS = Object.entries(SMILE_FEATURES).map(([key, description]) => ({
  name: `${key}_change`, description: `${description}: hourly change in vol points, same expiry, no gap bridging`,
}));

/** @returns {{ hours: string[], series: Record<string, (number|null)[]> }} */
function alignHourly(raw, sinceMs, beforeMs) {
  const hours = [];
  // Completed hours only; missing hours retain null slots so lag offsets are elapsed hours.
  for (let at = Math.ceil(sinceMs / 3600000) * 3600000; at < Math.floor(beforeMs / 3600000) * 3600000; at += 3600000) {
    hours.push(new Date(at).toISOString());
  }
  const series = {};
  for (const [name, rows] of Object.entries(raw)) {
    const map = new Map(rows.map(r => [new Date(Date.parse(r.hour)).toISOString(), r.value]));
    series[name] = hours.map(h => Number.isFinite(map.get(h)) ? map.get(h) : null);
  }
  return { hours, series };
}

function extractAllSeries(db, days, nowMs = Date.now()) {
  const sinceMs = nowMs - days * 86400000;
  const since = new Date(sinceMs).toISOString();
  const raw = {};
  for (const definition of SERIES_DEFS) raw[definition.name] = definition.extract(db, since);
  Object.assign(raw, smileChangeSeries(readSmileHistory(db, nowMs, 90)));
  return alignHourly(raw, sinceMs, nowMs);
}

const countPairs = (a,b) => a.reduce((n,x,i) => n + (Number.isFinite(x) && Number.isFinite(b[i]) ? 1 : 0),0);
function sliceWindow(data, hours) {
  return { hours: data.hours.slice(-hours), series: Object.fromEntries(Object.entries(data.series).map(([key,values]) => [key,values.slice(-hours)])) };
}

function analyzeAligned(data, descriptions, nowMs = Date.now()) {
  const names = Object.keys(data.series), d7 = sliceWindow(data,168), d30 = sliceWindow(data,720);
  const pairs = [], leadingIndicators = [];
  for (let i = 0; i < names.length; i++) for (let j = i+1; j < names.length; j++) {
    const a = names[i], b = names[j];
    const n7=countPairs(d7.series[a],d7.series[b]), n30=countPairs(d30.series[a],d30.series[b]), n90=countPairs(data.series[a],data.series[b]);
    const r7 = n7>=126 ? pearson(d7.series[a],d7.series[b]) : null;
    const r30 = n30>=540 ? pearson(d30.series[a],d30.series[b]) : null;
    const r90 = n90>=1620 ? pearson(data.series[a],data.series[b]) : null;
    if ([r7,r30,r90].every(r => r == null || Math.abs(r)<0.3)) continue;
    const lagData = r30 != null ? d30 : d7;
    const ll = r30 != null || r7 != null ? computeLeadLag(lagData.series[a],lagData.series[b]) : null;
    if (ll) {
      ll.lookback_days = lagData === d30 ? 30 : 7;
      const offset = ll.best_offset;
      ll.aligned_pairs = offset >= 0 ? countPairs(lagData.series[a].slice(0,lagData.hours.length-offset), lagData.series[b].slice(offset))
        : countPairs(lagData.series[a].slice(-offset),lagData.series[b].slice(0,lagData.hours.length+offset));
    }
    const mid = Math.floor(d30.hours.length/2);
    const first = pearson(d30.series[a].slice(0,mid),d30.series[b].slice(0,mid));
    const second = pearson(d30.series[a].slice(mid),d30.series[b].slice(mid));
    const stability = first == null || second == null ? 'insufficient_split_history'
      : first*second > 0 && Math.abs(first)>=0.3 && Math.abs(second)>=0.3 ? 'same_sign_in_both_halves' : 'unstable_across_halves';
    pairs.push({ series_a:a, series_b:b, r_7d:r7, r_30d:r30, r_90d:r90,
      n_7d:countPairs(d7.series[a],d7.series[b]), n_30d:countPairs(d30.series[a],d30.series[b]),
      n_90d:countPairs(data.series[a],data.series[b]), lead_lag:ll,
      stability_30d:{ status:stability, first_half_r:first, second_half_r:second } });
    if (ll && Math.abs(ll.best_r)>Math.abs(ll.contemporaneous_r)+0.05 && ll.best_offset!==0) {
      leadingIndicators.push({ leader:ll.best_offset>0?a:b, follower:ll.best_offset>0?b:a,
        offset_hours:Math.abs(ll.best_offset), lagged_r:ll.best_r, contemporaneous_r:ll.contemporaneous_r,
        aligned_pairs:ll.aligned_pairs, lookback_days:ll.lookback_days, status:'exploratory_lag_search' });
    }
  }
  return { pairs, leading_indicators:leadingIndicators, series_descriptions:descriptions,
    coverage:Object.fromEntries(names.map(name => [name,Object.fromEntries([['7d',d7],['30d',d30],['90d',data]].map(([window,d]) => [window,{ observed_hours:d.series[name].filter(Number.isFinite).length, expected_hours:window==='7d'?168:window==='30d'?720:2160 }]))])),
    computed_at:new Date(nowMs).toISOString(),
    notice:'Descriptive correlations with aligned-pair counts; 7d/30d/90d coefficients require at least 75% paired hourly coverage in the named window. Missing hours remain missing. Smile changes omit expiry rolls. Best option scores can change with candidate composition and are not realized P&L. Lag searches and split-half consistency are exploratory, not out-of-sample validation, causality, or authority to override EDGE/risk rules.' };
}
const correlationCache = new WeakMap();
function buildCorrelationAnalysis(db, nowMs = Date.now()) {
  const cached = correlationCache.get(db);
  if (cached && nowMs >= cached.at && nowMs-cached.at<300000) return cached.data;
  const descriptions = Object.fromEntries([...SERIES_DEFS,...SMILE_DEFS].map(s => [s.name,s.description]));
  let result;
  try { result = analyzeAligned(extractAllSeries(db,90,nowMs),descriptions,nowMs); }
  catch { result = { pairs:[], leading_indicators:[],series_descriptions:descriptions,computed_at:new Date(nowMs).toISOString(),notice:'Correlation history unavailable.' }; }
  correlationCache.set(db,{ at:nowMs,data:result });
  return result;
}
module.exports = { buildCorrelationAnalysis, alignHourly, analyzeAligned, pearson, computeLeadLag, SMILE_DEFS };
