import { extractAllSeries, getRegisteredSeries, type AlignedSeries } from './series-registry';

// ─── Cache ──────────────────────────────────────────────────────────────────

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let _corrCache: { data: any; ts: number } | null = null;
const CORRELATION_TTL = 300_000; // 5 minutes

// ─── Pearson Correlation ────────────────────────────────────────────────────

function pearson(xs: (number | null)[], ys: (number | null)[]): number | null {
  // Collect aligned non-null pairs
  const pairs: [number, number][] = [];
  const len = Math.min(xs.length, ys.length);
  for (let i = 0; i < len; i++) {
    if (xs[i] != null && ys[i] != null) {
      pairs.push([xs[i] as number, ys[i] as number]);
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

interface LeadLagResult {
  best_offset: number;       // positive = a leads b by N hours
  best_r: number;
  contemporaneous_r: number;
  a_leads: boolean;          // true if a leads b (positive offset won)
}

function computeLeadLag(
  a: (number | null)[],
  b: (number | null)[]
): LeadLagResult | null {
  const contemp = pearson(a, b);
  if (contemp == null) return null;

  let bestOffset = 0;
  let bestR = Math.abs(contemp);
  let bestRSigned = contemp;

  for (const offset of LAG_OFFSETS) {
    // a leads b: shift a forward (compare a[i] with b[i + offset])
    const aLeadA = a.slice(0, a.length - offset);
    const aLeadB = b.slice(offset);
    const rALead = pearson(aLeadA, aLeadB);
    if (rALead != null && Math.abs(rALead) > bestR) {
      bestR = Math.abs(rALead);
      bestRSigned = rALead;
      bestOffset = offset;
    }

    // b leads a: shift b forward (compare b[i] with a[i + offset])
    const bLeadB = b.slice(0, b.length - offset);
    const bLeadA = a.slice(offset);
    const rBLead = pearson(bLeadA, bLeadB);
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

// ─── Correlation Snapshot ───────────────────────────────────────────────────

export interface PairCorrelation {
  series_a: string;
  series_b: string;
  r_7d: number | null;
  r_30d: number | null;
  lead_lag: LeadLagResult | null;
}

export interface LeadingIndicator {
  leader: string;
  follower: string;
  offset_hours: number;
  lagged_r: number;
  contemporaneous_r: number;
}

export interface CorrelationSnapshot {
  pairs: PairCorrelation[];
  leading_indicators: LeadingIndicator[];
  series_descriptions: Record<string, string>;
  computed_at: string;
}

export function buildCorrelationAnalysis(): CorrelationSnapshot {
  if (_corrCache && Date.now() - _corrCache.ts < CORRELATION_TTL) return _corrCache.data;
  const result = _buildCorrelationUncached();
  _corrCache = { data: result, ts: Date.now() };
  return result;
}

function sliceLast7d(data30d: AlignedSeries): AlignedSeries {
  // 7 days = 168 hours; take the tail of the 30d data
  const n = Math.min(168, data30d.hours.length);
  const start = data30d.hours.length - n;
  const hours = data30d.hours.slice(start);
  const series: Record<string, (number | null)[]> = {};
  for (const [name, values] of Object.entries(data30d.series)) {
    series[name] = values.slice(start);
  }
  return { hours, series };
}

function _buildCorrelationUncached(): CorrelationSnapshot {
  const registry = getRegisteredSeries();
  const names = registry.map((s) => s.name);
  const descriptions: Record<string, string> = {};
  for (const s of registry) {
    descriptions[s.name] = s.description;
  }

  let data7d: AlignedSeries;
  let data30d: AlignedSeries;
  try {
    data30d = extractAllSeries(30);
    data7d = sliceLast7d(data30d);
  } catch {
    return {
      pairs: [],
      leading_indicators: [],
      series_descriptions: descriptions,
      computed_at: new Date().toISOString(),
    };
  }

  const pairs: PairCorrelation[] = [];
  const leadingIndicators: LeadingIndicator[] = [];

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
