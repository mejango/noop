import { NextRequest, NextResponse } from 'next/server';
import {
  getSpotPrices, getBestOptionsOverTime, getLiquidityOverTime, getBestScores, getOptionsHeatmap,
  getFundingRates, getFundingRatesHourlySeries, getOISnapshots, getOISnapshotsBucketed,
  getOptionsCoverage, getSpotPricesHourly_rollup, getBestOptionsHourly_rollup, getLiquidityHourly_rollup,
  getSpotPricesBucketed, getBestOptionsBucketed,
} from '@/lib/db';
import { CHART_ROW_LIMITS } from '@/lib/limits';

export const dynamic = 'force-dynamic';

// Downsample bucket sizes per range (0 = raw ticks, no downsampling)
const BUCKET_MS: Record<string, number> = {
  '1h':   0,
  '6h':   0,
  '24h':  0,
  '3d':   15 * 60 * 1000,        // 15 min
  '6.2d': 30 * 60 * 1000,        // 30 min
  '7d':   30 * 60 * 1000,        // 30 min
  '14d':  60 * 60 * 1000,        // 1 hour
  '30d':  2 * 60 * 60 * 1000,    // 2 hours
  '90d':  4 * 60 * 60 * 1000,    // 4 hours
  '365d': 24 * 60 * 60 * 1000,   // 1 day
  'all':  4 * 60 * 60 * 1000,    // 4 hours
};

// Heavier scatter plots need more aggressive quantization than line charts.
const HEATMAP_BUCKET_MS: Record<string, number> = {
  '1h':   0,
  '6h':   15 * 60 * 1000,        // 15 min
  '24h':  30 * 60 * 1000,        // 30 min
  '3d':   60 * 60 * 1000,        // 1 hour
  '6.2d': 2 * 60 * 60 * 1000,    // 2 hours
  '7d':   2 * 60 * 60 * 1000,    // 2 hours
  '14d':  4 * 60 * 60 * 1000,    // 4 hours
  '30d':  6 * 60 * 60 * 1000,    // 6 hours
  '90d':  12 * 60 * 60 * 1000,   // 12 hours
  '365d': 2 * 24 * 60 * 60 * 1000, // 2 days
  'all':  2 * 24 * 60 * 60 * 1000, // 2 days
};

type TimestampedRow = Record<string, unknown> & { timestamp: string };

const ROLLUP_RANGES = new Set(['90d', '365d', 'all']);
const RAW_BUCKETED_RANGES = new Set(['30d']);
const ROLLUP_TAIL_OVERLAP_MS = 60 * 60 * 1000;

function rowTime(row: TimestampedRow): number {
  const time = new Date(row.timestamp).getTime();
  return Number.isFinite(time) ? time : 0;
}

function latestRowTime(rows: TimestampedRow[]): number | null {
  let latest: number | null = null;
  for (const row of rows) {
    const time = rowTime(row);
    if (time && (latest == null || time > latest)) latest = time;
  }
  return latest;
}

function rawTailSince(rows: TimestampedRow[], fallbackSince: string): string {
  const fallback = new Date(fallbackSince).getTime();
  const latest = latestRowTime(rows);
  if (latest == null) return fallbackSince;
  return new Date(Math.max(fallback, latest - ROLLUP_TAIL_OVERLAP_MS)).toISOString();
}

function mergeByTimestamp<T extends TimestampedRow>(baseRows: T[], tailRows: T[]): T[] {
  const rows = new Map<string, T>();
  for (const row of baseRows) rows.set(row.timestamp, row);
  for (const row of tailRows) rows.set(row.timestamp, row);
  return Array.from(rows.values()).sort((a, b) => rowTime(a) - rowTime(b));
}

function appendRawTail<T extends TimestampedRow>(
  baseRows: T[],
  since: string,
  getTail: (tailSince: string) => T[],
): T[] {
  return mergeByTimestamp(baseRows, getTail(rawTailSince(baseRows, since)));
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function downsample<T extends Record<string, any>>(
  rows: T[],
  bucketMs: number,
  tsKey: string,
  numericKeys: string[],
  lastValueKeys: string[] = [],
): T[] {
  if (rows.length === 0 || bucketMs <= 0) return rows;
  const buckets = new Map<number, T[]>();
  for (const row of rows) {
    const t = new Date(row[tsKey]).getTime();
    const bucket = Math.floor(t / bucketMs) * bucketMs;
    if (!buckets.has(bucket)) buckets.set(bucket, []);
    buckets.get(bucket)!.push(row);
  }
  const result: T[] = [];
  Array.from(buckets.entries()).forEach(([bucket, group]) => {
    const out = { ...group[group.length - 1], [tsKey]: new Date(bucket).toISOString() } as T;
    for (const key of numericKeys) {
      const vals = group.map(r => r[key]).filter(v => v != null && !isNaN(v));
      if (vals.length > 0) {
        (out as Record<string, unknown>)[key] = vals.reduce((a: number, b: number) => a + b, 0) / vals.length;
      }
    }
    for (const key of lastValueKeys) {
      (out as Record<string, unknown>)[key] = group[group.length - 1][key];
    }
    result.push(out);
  });
  return result;
}

// Downsample liquidity rows (dynamic keys per dex)
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function downsampleLiquidity(rows: Record<string, any>[], bucketMs: number): Record<string, any>[] {
  if (rows.length === 0 || bucketMs <= 0) return rows;
  const buckets = new Map<number, typeof rows>();
  for (const row of rows) {
    const t = new Date(row.timestamp).getTime();
    const bucket = Math.floor(t / bucketMs) * bucketMs;
    if (!buckets.has(bucket)) buckets.set(bucket, []);
    buckets.get(bucket)!.push(row);
  }
  const result: typeof rows = [];
  Array.from(buckets.entries()).forEach(([bucket, group]) => {
    const out: Record<string, unknown> = { timestamp: new Date(bucket).toISOString() };
    const allKeys = new Set<string>();
    for (const r of group) for (const k of Object.keys(r)) if (k !== 'timestamp') allKeys.add(k);
    Array.from(allKeys).forEach(key => {
      const vals = group.map(r => r[key]).filter(v => v != null && typeof v === 'number' && !isNaN(v));
      if (vals.length > 0) {
        out[key] = vals.reduce((a: number, b: number) => a + b, 0) / vals.length;
      }
    });
    result.push(out);
  });
  return result;
}

export function GET(request: NextRequest) {
  try {
    const range = request.nextUrl.searchParams.get('range') || '14d';
    const rangeMs: Record<string, number> = {
      '1h': 60 * 60 * 1000,
      '6h': 6 * 60 * 60 * 1000,
      '24h': 24 * 60 * 60 * 1000,
      '3d': 3 * 24 * 60 * 60 * 1000,
      '6.2d': 6.2 * 24 * 60 * 60 * 1000,
      '7d': 7 * 24 * 60 * 60 * 1000,
      '14d': 14 * 24 * 60 * 60 * 1000,
      '30d': 30 * 24 * 60 * 60 * 1000,
      '90d': 90 * 24 * 60 * 60 * 1000,
      '365d': 365 * 24 * 60 * 60 * 1000,
      'all': 365 * 24 * 60 * 60 * 1000,
    };
    const ms = rangeMs[range] || rangeMs['14d'];
    const since = new Date(Date.now() - ms).toISOString();
    const bestScores = getBestScores();
    const bucketMs = BUCKET_MS[range] || 0;
    const heatmapBucketMs = HEATMAP_BUCKET_MS[range] || bucketMs;
    const optionsCoverage = getOptionsCoverage(since);
    const useRollups = ROLLUP_RANGES.has(range);
    const useBucketedRaw = RAW_BUCKETED_RANGES.has(range);

    const limits = CHART_ROW_LIMITS[range] || CHART_ROW_LIMITS['14d'];
    const prices = useBucketedRaw
      ? getSpotPricesBucketed(since, bucketMs)
      : useRollups
        ? appendRawTail(
            getSpotPricesHourly_rollup(since) as TimestampedRow[],
            since,
            (tailSince) => getSpotPricesBucketed(tailSince, bucketMs) as TimestampedRow[],
          )
        : getSpotPrices(since, limits.prices);
    const options = useBucketedRaw
      ? getBestOptionsBucketed(since, bucketMs)
      : useRollups
        ? appendRawTail(
            getBestOptionsHourly_rollup(since) as TimestampedRow[],
            since,
            (tailSince) => getBestOptionsBucketed(tailSince, bucketMs) as TimestampedRow[],
          )
        : getBestOptionsOverTime(since);
    const liquidity = useRollups
      ? appendRawTail(
          getLiquidityHourly_rollup(since) as TimestampedRow[],
          since,
          (tailSince) => getLiquidityOverTime(tailSince) as TimestampedRow[],
        )
      : getLiquidityOverTime(since);
    const optionsHeatmap = getOptionsHeatmap(since, limits.heatmap, heatmapBucketMs);

    // Sentiment data
    const fundingRates = useRollups ? getFundingRatesHourlySeries(since) : getFundingRates(since);
    const oiSnapshots = useRollups ? getOISnapshotsBucketed(since, bucketMs) : getOISnapshots(since);
    const optionsSkew = oiSnapshots.map((row) => ({
      timestamp: row.timestamp,
      avg_put_iv: row.avg_put_iv ?? null,
      avg_call_iv: row.avg_call_iv ?? null,
    }));
    const aggregateOI = oiSnapshots.map((row) => ({
      timestamp: row.timestamp,
      total_oi: row.total_oi,
    }));

    const sentiment = { fundingRates, optionsSkew, aggregateOI, oiSnapshots };

    if (bucketMs > 0) {
      const dsPrices = downsample(
        prices as Record<string, unknown>[],
        bucketMs, 'timestamp', ['price'], ['short_momentum_main', 'medium_momentum_main'],
      );
      const dsOptions = downsample(
        options as Record<string, unknown>[],
        bucketMs, 'timestamp', ['best_put_value', 'best_call_value', 'lyra_spot'],
      );
      const dsLiquidity = downsampleLiquidity(
        liquidity as Record<string, unknown>[],
        bucketMs,
      );
      const dsFunding = downsample(
        fundingRates as Record<string, unknown>[],
        bucketMs, 'timestamp', ['rate'],
      );
      const dsSkew = downsample(
        optionsSkew as Record<string, unknown>[],
        bucketMs, 'timestamp', ['avg_put_iv', 'avg_call_iv'],
      );
      const dsOI = downsample(
        aggregateOI as Record<string, unknown>[],
        bucketMs, 'timestamp', ['total_oi'],
      );
      const dsOISnapshots = downsample(
        oiSnapshots as Record<string, unknown>[],
        bucketMs, 'timestamp',
        ['put_oi', 'call_oi', 'near_put_oi', 'near_call_oi', 'far_put_oi', 'far_call_oi', 'total_oi', 'pc_ratio'],
        ['expiry_count'],
      );
      return NextResponse.json({
        prices: dsPrices, options: dsOptions, liquidity: dsLiquidity,
        bestScores, optionsHeatmap,
        sentiment: { fundingRates: dsFunding, optionsSkew: dsSkew, aggregateOI: dsOI, oiSnapshots: dsOISnapshots },
        optionsCoverage: {
          ...optionsCoverage,
          requestedSince: since,
          hasGapBeforeRange: Boolean(optionsCoverage.firstInRange && new Date(optionsCoverage.firstInRange).getTime() - new Date(since).getTime() > 6 * 60 * 60 * 1000),
        },
        tier: 'downsampled',
      });
    }

    return NextResponse.json({
      prices, options, liquidity, bestScores, optionsHeatmap, sentiment,
      optionsCoverage: {
        ...optionsCoverage,
        requestedSince: since,
        hasGapBeforeRange: Boolean(optionsCoverage.firstInRange && new Date(optionsCoverage.firstInRange).getTime() - new Date(since).getTime() > 6 * 60 * 60 * 1000),
      },
      tier: 'raw',
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
