import {
  getBestCallDvHourly,
  getBestPutDvHourly,
  getFundingRatesHourlySeries,
  getImpliedVolHourly,
  getOISnapshots,
  getOpenInterestHourly,
  getOptionsDepthHourly,
  getOptionsSpreadHourly,
  getSpotPricesHourly,
} from './db';

type Numeric = number | null;

type SeriesRow = {
  hour: string;
  value: Numeric;
};

type SignalRecord = {
  hour: string;
  spot: Numeric;
  putScore: Numeric;
  callScore: Numeric;
  spread: Numeric;
  depth: Numeric;
  oi: Numeric;
  iv: Numeric;
  funding: Numeric;
  skew: Numeric;
};

type SignalContext = {
  row: SignalRecord;
  prev6: SignalRecord | null;
  thresholds: SignalThresholds;
};

type SignalThresholds = {
  putScoreP75: number | null;
  callScoreP75: number | null;
  callScoreP65: number | null;
  spreadP50: number | null;
  spreadP75: number | null;
  depthP50: number | null;
  fundingP75: number | null;
};

type HorizonStat = {
  horizonHours: number;
  samples: number;
  hitRate: number | null;
  medianForwardChange: number | null;
  costlyFalsePositiveRate: number | null;
};

type SignalDefinition = {
  id: string;
  label: string;
  target: string;
  description: string;
  outcomeMetric: keyof SignalRecord;
  changeFormat: 'pct' | 'pp';
  horizons: number[];
  detects: (ctx: SignalContext) => boolean;
  isHit: (current: number, future: number) => boolean;
  isCostlyFalsePositive: (current: number, future: number) => boolean;
};

export type SignalPrior = {
  id: string;
  label: string;
  target: string;
  description: string;
  samples: number;
  status: 'insufficient' | 'watch' | 'developing';
  activeNow: boolean;
  lastFired: string | null;
  bestHorizonHours: number | null;
  hitRate: number | null;
  medianForwardChange: number | null;
  costlyFalsePositiveRate: number | null;
  changeFormat: 'pct' | 'pp';
  horizons: HorizonStat[];
};

export type SignalAnalytics = {
  meta: {
    computedAt: string;
    windowDays: number;
    minSamples: number;
    hours: number;
    note: string;
  };
  priors: SignalPrior[];
};

const HORIZONS = [6, 24, 72];
const MIN_SAMPLES = 10;

function toHour(ts: string) {
  const date = new Date(ts);
  date.setUTCMinutes(0, 0, 0);
  return date.toISOString().replace('.000Z', 'Z');
}

function finite(value: unknown): number | null {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function pctChange(current: Numeric, previous: Numeric) {
  if (current == null || previous == null || previous === 0) return null;
  return (current - previous) / Math.abs(previous);
}

function absReturn(current: Numeric, previous: Numeric) {
  const change = pctChange(current, previous);
  return change == null ? null : Math.abs(change);
}

function median(values: number[]) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

function quantile(values: number[], q: number) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const pos = (sorted.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  if (sorted[base + 1] == null) return sorted[base];
  return sorted[base] + rest * (sorted[base + 1] - sorted[base]);
}

function makeMap(rows: SeriesRow[]) {
  const map = new Map<string, number>();
  for (const row of rows) {
    const value = finite(row.value);
    if (value != null) map.set(toHour(row.hour), value);
  }
  return map;
}

function bucketAverage<T>(
  rows: T[],
  getTimestamp: (row: T) => string,
  getValue: (row: T) => Numeric
) {
  const buckets = new Map<string, { sum: number; count: number }>();
  for (const row of rows) {
    const value = getValue(row);
    if (value == null) continue;
    const hour = toHour(getTimestamp(row));
    const bucket = buckets.get(hour) || { sum: 0, count: 0 };
    bucket.sum += value;
    bucket.count += 1;
    buckets.set(hour, bucket);
  }
  const result = new Map<string, number>();
  buckets.forEach((bucket, hour) => {
    if (bucket.count > 0) result.set(hour, bucket.sum / bucket.count);
  });
  return result;
}

function values(records: SignalRecord[], key: keyof SignalRecord) {
  return records.map((row) => finite(row[key])).filter((value): value is number => value != null);
}

const SIGNALS: SignalDefinition[] = [
  {
    id: 'put_score_improving_spot_flat',
    label: 'Put Score Improving / Spot Flat',
    target: 'cheap protection window',
    description: 'Best put score improved while spot stayed relatively flat, suggesting options repriced cheaper before spot moved.',
    outcomeMetric: 'putScore',
    changeFormat: 'pct',
    horizons: HORIZONS,
    detects: ({ row, prev6, thresholds }) => {
      if (!prev6 || row.putScore == null || prev6.putScore == null) return false;
      const putChange = pctChange(row.putScore, prev6.putScore);
      const spotMove = absReturn(row.spot, prev6.spot);
      const spreadOk = thresholds.spreadP75 == null || row.spread == null || row.spread <= thresholds.spreadP75;
      return putChange != null && putChange >= 0.12 && spotMove != null && spotMove <= 0.02 && spreadOk;
    },
    isHit: (current, future) => future <= current * 1.05,
    isCostlyFalsePositive: (current, future) => future > current * 1.15,
  },
  {
    id: 'cheap_put_liquid_book',
    label: 'Cheap Put / Liquid Book',
    target: 'executable cheap convexity',
    description: 'Best put score is in the upper quartile while spread and depth are acceptable.',
    outcomeMetric: 'putScore',
    changeFormat: 'pct',
    horizons: HORIZONS,
    detects: ({ row, thresholds }) => {
      if (row.putScore == null || thresholds.putScoreP75 == null) return false;
      const spreadOk = thresholds.spreadP50 == null || row.spread == null || row.spread <= thresholds.spreadP50;
      const depthOk = thresholds.depthP50 == null || row.depth == null || row.depth >= thresholds.depthP50;
      return row.putScore >= thresholds.putScoreP75 && spreadOk && depthOk;
    },
    isHit: (current, future) => future <= current * 1.05,
    isCostlyFalsePositive: (current, future) => future > current * 1.15,
  },
  {
    id: 'skew_widening_oi_rising',
    label: 'Skew Widening / OI Rising',
    target: 'protection repricing risk',
    description: 'Put-call IV skew widened while open interest rose, suggesting protection demand is building.',
    outcomeMetric: 'putScore',
    changeFormat: 'pct',
    horizons: HORIZONS,
    detects: ({ row, prev6 }) => {
      if (!prev6 || row.skew == null || prev6.skew == null || row.oi == null || prev6.oi == null) return false;
      const oiChange = pctChange(row.oi, prev6.oi);
      return row.skew - prev6.skew >= 0.75 && oiChange != null && oiChange >= 0.03;
    },
    isHit: (current, future) => future <= current * 0.95,
    isCostlyFalsePositive: (current, future) => future > current * 1.10,
  },
  {
    id: 'rich_call_premium_with_depth',
    label: 'Rich Call Premium / Depth',
    target: 'paid smart call risk',
    description: 'Call bid-per-delta score is rich while book depth is reasonable.',
    outcomeMetric: 'callScore',
    changeFormat: 'pct',
    horizons: HORIZONS,
    detects: ({ row, thresholds }) => {
      if (row.callScore == null || thresholds.callScoreP75 == null) return false;
      const depthOk = thresholds.depthP50 == null || row.depth == null || row.depth >= thresholds.depthP50;
      const spreadOk = thresholds.spreadP75 == null || row.spread == null || row.spread <= thresholds.spreadP75;
      return row.callScore >= thresholds.callScoreP75 && depthOk && spreadOk;
    },
    isHit: (current, future) => future <= current * 1.10,
    isCostlyFalsePositive: (current, future) => future > current * 1.25,
  },
  {
    id: 'funding_positive_call_score_rich',
    label: 'Positive Funding / Rich Calls',
    target: 'crowded upside premium',
    description: 'Funding is high relative to recent history while call premium is already rich.',
    outcomeMetric: 'callScore',
    changeFormat: 'pct',
    horizons: HORIZONS,
    detects: ({ row, thresholds }) => {
      if (row.funding == null || row.callScore == null || thresholds.fundingP75 == null || thresholds.callScoreP65 == null) return false;
      return row.funding >= thresholds.fundingP75 && row.callScore >= thresholds.callScoreP65;
    },
    isHit: (current, future) => future <= current * 1.10,
    isCostlyFalsePositive: (current, future) => future > current * 1.25,
  },
  {
    id: 'liquidity_improving',
    label: 'Liquidity Improving',
    target: 'execution quality',
    description: 'Spreads tightened or depth increased over the prior 6h.',
    outcomeMetric: 'spread',
    changeFormat: 'pct',
    horizons: [6, 24],
    detects: ({ row, prev6 }) => {
      if (!prev6) return false;
      const spreadChange = pctChange(row.spread, prev6.spread);
      const depthChange = pctChange(row.depth, prev6.depth);
      return (spreadChange != null && spreadChange <= -0.10) || (depthChange != null && depthChange >= 0.15);
    },
    isHit: (current, future) => future <= current * 1.05,
    isCostlyFalsePositive: (current, future) => future > current * 1.20,
  },
];

function buildRecords(since: string) {
  const spotRows = getSpotPricesHourly(since).map((row) => ({ hour: row.hour, value: row.avg_price }));
  const putScore = makeMap(getBestPutDvHourly(since));
  const callScore = makeMap(getBestCallDvHourly(since));
  const spread = makeMap(getOptionsSpreadHourly(since));
  const depth = makeMap(getOptionsDepthHourly(since));
  const oi = makeMap(getOpenInterestHourly(since));
  const iv = makeMap(getImpliedVolHourly(since));
  const funding = makeMap(getFundingRatesHourlySeries(since).map((row) => ({ hour: row.timestamp, value: row.rate })));
  const skew = bucketAverage(
    getOISnapshots(since),
    (row) => row.timestamp,
    (row) => {
      const putIv = finite(row.avg_put_iv);
      const callIv = finite(row.avg_call_iv);
      return putIv != null && callIv != null ? (putIv - callIv) * 100 : null;
    }
  );
  const spot = makeMap(spotRows);

  const hours = new Set<string>();
  for (const map of [spot, putScore, callScore, spread, depth, oi, iv, funding, skew]) {
    map.forEach((_, hour) => hours.add(hour));
  }

  return Array.from(hours).sort().map((hour) => ({
    hour,
    spot: spot.get(hour) ?? null,
    putScore: putScore.get(hour) ?? null,
    callScore: callScore.get(hour) ?? null,
    spread: spread.get(hour) ?? null,
    depth: depth.get(hour) ?? null,
    oi: oi.get(hour) ?? null,
    iv: iv.get(hour) ?? null,
    funding: funding.get(hour) ?? null,
    skew: skew.get(hour) ?? null,
  }));
}

function buildThresholds(records: SignalRecord[]): SignalThresholds {
  return {
    putScoreP75: quantile(values(records, 'putScore'), 0.75),
    callScoreP75: quantile(values(records, 'callScore'), 0.75),
    callScoreP65: quantile(values(records, 'callScore'), 0.65),
    spreadP50: quantile(values(records, 'spread'), 0.50),
    spreadP75: quantile(values(records, 'spread'), 0.75),
    depthP50: quantile(values(records, 'depth'), 0.50),
    fundingP75: quantile(values(records, 'funding'), 0.75),
  };
}

function forwardChange(current: number, future: number, format: 'pct' | 'pp') {
  if (format === 'pp') return future - current;
  if (current === 0) return null;
  return (future - current) / Math.abs(current);
}

function statusFor(samples: number, hitRate: number | null, costlyRate: number | null): SignalPrior['status'] {
  if (samples < MIN_SAMPLES || hitRate == null) return 'insufficient';
  if (hitRate >= 0.60 && (costlyRate ?? 1) <= 0.25) return 'developing';
  return 'watch';
}

function summarizeSignal(def: SignalDefinition, records: SignalRecord[], thresholds: SignalThresholds): SignalPrior {
  const events: number[] = [];
  for (let i = 0; i < records.length; i++) {
    const ctx = { row: records[i], prev6: records[i - 6] ?? null, thresholds };
    if (def.detects(ctx)) events.push(i);
  }

  const horizons = def.horizons.map((horizonHours) => {
    const hits: boolean[] = [];
    const costly: boolean[] = [];
    const changes: number[] = [];
    for (const idx of events) {
      const future = records[idx + horizonHours];
      if (!future) continue;
      const currentValue = finite(records[idx][def.outcomeMetric]);
      const futureValue = finite(future[def.outcomeMetric]);
      if (currentValue == null || futureValue == null) continue;
      hits.push(def.isHit(currentValue, futureValue));
      costly.push(def.isCostlyFalsePositive(currentValue, futureValue));
      const change = forwardChange(currentValue, futureValue, def.changeFormat);
      if (change != null) changes.push(change);
    }
    return {
      horizonHours,
      samples: hits.length,
      hitRate: hits.length > 0 ? hits.filter(Boolean).length / hits.length : null,
      medianForwardChange: median(changes),
      costlyFalsePositiveRate: costly.length > 0 ? costly.filter(Boolean).length / costly.length : null,
    };
  });

  const best = [...horizons]
    .filter((row) => row.samples >= MIN_SAMPLES && row.hitRate != null)
    .sort((a, b) => (b.hitRate ?? 0) - (a.hitRate ?? 0))[0]
    ?? horizons.slice().sort((a, b) => b.samples - a.samples)[0]
    ?? null;

  const latestIdx = records.length - 1;
  const activeNow = latestIdx >= 0
    ? def.detects({ row: records[latestIdx], prev6: records[latestIdx - 6] ?? null, thresholds })
    : false;
  const lastFired = events.length > 0 ? records[events[events.length - 1]].hour : null;
  const status = statusFor(best?.samples ?? 0, best?.hitRate ?? null, best?.costlyFalsePositiveRate ?? null);

  return {
    id: def.id,
    label: def.label,
    target: def.target,
    description: def.description,
    samples: events.length,
    status,
    activeNow,
    lastFired,
    bestHorizonHours: best?.horizonHours ?? null,
    hitRate: best?.hitRate ?? null,
    medianForwardChange: best?.medianForwardChange ?? null,
    costlyFalsePositiveRate: best?.costlyFalsePositiveRate ?? null,
    changeFormat: def.changeFormat,
    horizons,
  };
}

export function buildSignalAnalytics(windowDays = 90): SignalAnalytics {
  const since = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();
  const records = buildRecords(since);
  const thresholds = buildThresholds(records);
  const priors = SIGNALS.map((signal) => summarizeSignal(signal, records, thresholds))
    .sort((a, b) => {
      if (a.status !== b.status) {
        const order = { developing: 0, watch: 1, insufficient: 2 };
        return order[a.status] - order[b.status];
      }
      return (b.samples ?? 0) - (a.samples ?? 0);
    });

  return {
    meta: {
      computedAt: new Date().toISOString(),
      windowDays,
      minSamples: MIN_SAMPLES,
      hours: records.length,
      note: 'Dashboard-only statistical priors. These are not injected into advisor prompts until sample counts mature.',
    },
    priors,
  };
}
