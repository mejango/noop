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
        returns.push({ hour: rows[i].hour, value: prev > 0 ? ((curr - prev) / prev) * 100 : null });
      }
      return returns;
    },
  },
  {
    name: 'liquidity_flow',
    description: 'DEX liquidity signed flow (+ inflow, - outflow)',
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
    name: 'exhaustion_score',
    description: 'Market exhaustion score (0=fresh, 1=fully exhausted)',
    extract: (db, since) => {
      const rows = db.getOnchainHourly(since);
      return rows.map(r => ({ hour: r.hour, value: r.avg_exhaustion }));
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
];

function extractAllSeries(db, days) {
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

  const raw = {};
  const allHours = new Set();

  for (const s of SERIES_DEFS) {
    const data = s.extract(db, since);
    const map = new Map();
    for (const d of data) {
      map.set(d.hour, d.value);
      allHours.add(d.hour);
    }
    raw[s.name] = map;
  }

  const hours = Array.from(allHours).sort();
  const series = {};

  for (const s of SERIES_DEFS) {
    const map = raw[s.name];
    series[s.name] = hours.map(h => map.get(h) ?? null);
  }

  return { hours, series };
}

// ─── Build Correlation Analysis ─────────────────────────────────────────────

function buildCorrelationAnalysis(db) {
  const names = SERIES_DEFS.map(s => s.name);
  const descriptions = {};
  for (const s of SERIES_DEFS) {
    descriptions[s.name] = s.description;
  }

  let data7d, data30d;
  try {
    data7d = extractAllSeries(db, 7);
    data30d = extractAllSeries(db, 30);
  } catch {
    return {
      pairs: [],
      leading_indicators: [],
      series_descriptions: descriptions,
      computed_at: new Date().toISOString(),
    };
  }

  const pairs = [];
  const leadingIndicators = [];

  for (let i = 0; i < names.length; i++) {
    for (let j = i + 1; j < names.length; j++) {
      const a = names[i];
      const b = names[j];

      const r7 = pearson(data7d.series[a], data7d.series[b]);
      const r30 = pearson(data30d.series[a], data30d.series[b]);

      // Skip weak correlations
      if ((r7 == null || Math.abs(r7) < 0.3) && (r30 == null || Math.abs(r30) < 0.3)) {
        continue;
      }

      // Lead/lag on 30d data (more data = more reliable)
      const ll = computeLeadLag(data30d.series[a], data30d.series[b]);

      pairs.push({ series_a: a, series_b: b, r_7d: r7, r_30d: r30, lead_lag: ll });

      // Identify leading indicators: lagged r exceeds contemporaneous by >= 0.05
      if (ll && Math.abs(ll.best_r) > Math.abs(ll.contemporaneous_r) + 0.05 && ll.best_offset !== 0) {
        const leader = ll.best_offset > 0 ? a : b;
        const follower = ll.best_offset > 0 ? b : a;
        leadingIndicators.push({
          leader,
          follower,
          offset_hours: Math.abs(ll.best_offset),
          lagged_r: ll.best_r,
          contemporaneous_r: ll.contemporaneous_r,
        });
      }
    }
  }

  return {
    pairs,
    leading_indicators: leadingIndicators,
    series_descriptions: descriptions,
    computed_at: new Date().toISOString(),
  };
}

module.exports = { buildCorrelationAnalysis };
