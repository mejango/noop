import { NextRequest, NextResponse } from 'next/server';
import {
  getSpotPrices, getBestOptionsOverTime, getLiquidityOverTime, getBestScores, getOptionsHeatmap,
} from '@/lib/db';

export const dynamic = 'force-dynamic';

// Downsample bucket sizes per range (0 = raw ticks, no downsampling)
const BUCKET_MS: Record<string, number> = {
  '1h':   0,
  '6h':   0,
  '24h':  0,
  '3d':   15 * 60 * 1000,        // 15 min
  '6.2d': 30 * 60 * 1000,        // 30 min
  '7d':   30 * 60 * 1000,        // 30 min
  '30d':  2 * 60 * 60 * 1000,    // 2 hours
  '90d':  4 * 60 * 60 * 1000,    // 4 hours
  'all':  4 * 60 * 60 * 1000,    // 4 hours
};

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
    const range = request.nextUrl.searchParams.get('range') || '7d';
    const rangeMs: Record<string, number> = {
      '1h': 60 * 60 * 1000,
      '6h': 6 * 60 * 60 * 1000,
      '24h': 24 * 60 * 60 * 1000,
      '3d': 3 * 24 * 60 * 60 * 1000,
      '6.2d': 6.2 * 24 * 60 * 60 * 1000,
      '7d': 7 * 24 * 60 * 60 * 1000,
      '30d': 30 * 24 * 60 * 60 * 1000,
      '90d': 90 * 24 * 60 * 60 * 1000,
      'all': 365 * 24 * 60 * 60 * 1000,
    };
    const ms = rangeMs[range] || rangeMs['7d'];
    const since = new Date(Date.now() - ms).toISOString();
    const bestScores = getBestScores();
    const bucketMs = BUCKET_MS[range] || 0;

    // Fetch raw data — use higher limit for longer ranges since we'll downsample
    const rowLimit = bucketMs > 0 ? 50000 : 5000;
    const prices = getSpotPrices(since, rowLimit);
    const options = getBestOptionsOverTime(since);
    const liquidity = getLiquidityOverTime(since);
    const optionsHeatmap = bucketMs === 0 ? getOptionsHeatmap(since) : [];

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
      return NextResponse.json({ prices: dsPrices, options: dsOptions, liquidity: dsLiquidity, bestScores, optionsHeatmap, tier: 'downsampled' });
    }

    return NextResponse.json({ prices, options, liquidity, bestScores, optionsHeatmap, tier: 'raw' });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
