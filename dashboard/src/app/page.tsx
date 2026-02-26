'use client';

import { useState, useMemo, useCallback, useEffect, useRef, ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { usePolling, useIsMobile } from '@/lib/hooks';
import { formatUSD, momentumColor, dteDays } from '@/lib/format';
import { chartColors, chartAxis, chartTooltip } from '@/lib/chart';
import Card from '@/components/Card';
import {
  ComposedChart, Line, Scatter, XAxis, YAxis, Tooltip,
  ResponsiveContainer, Legend, ReferenceLine, ScatterChart, ReferenceArea,
} from 'recharts';

/** Hook: hover shows tooltip, click pins it, next click anywhere unpins */
function usePinnableTooltip() {
  const [pinned, setPinned] = useState(false);
  const cache = useRef<ReactNode>(null);

  useEffect(() => {
    if (!pinned) return;
    const dismiss = () => setPinned(false);
    document.addEventListener('click', dismiss, { once: true });
    return () => document.removeEventListener('click', dismiss);
  }, [pinned]);

  return {
    pinned,
    /** Props for the div wrapping the chart */
    containerProps: {
      onClick: () => { if (!pinned) setPinned(true); },
      style: pinned ? { pointerEvents: 'none' as const } : undefined,
    },
    /** Spread onto <Tooltip> to force active when pinned */
    tooltipActive: pinned ? { active: true as const } : {},
    /** Wrap tooltip content return value to cache last valid content */
    wrap(content: ReactNode): ReactNode {
      if (content != null) cache.current = content;
      return content ?? (pinned ? cache.current : null);
    },
  };
}

interface Budget {
  putTotalBudget: number;
  putSpent: number;
  putRemaining: number;
  putDaysLeft: number;
  callTotalBudget: number;
  callSpent: number;
  callRemaining: number;
  callDaysLeft: number;
  cycleDays: number;
}

interface Stats {
  last_price: number;
  last_price_time: string;
  short_momentum: string;
  short_derivative: string;
  medium_momentum: string;
  medium_derivative: string;
  three_day_high: number;
  three_day_low: number;
  seven_day_high: number;
  seven_day_low: number;
  budget: Budget;
}

interface SpotPrice {
  timestamp: string;
  price: number;
  short_momentum_main: string;
  short_momentum_derivative: string;
  medium_momentum_main: string;
  medium_momentum_derivative: string;
}

interface OptionsPoint {
  timestamp: string;
  best_put_value: number | null;
  best_call_value: number | null;
}

interface LiquidityPoint {
  timestamp: string;
  [dex: string]: string | number;
}

interface OptionDetail {
  delta: number | null;
  price: number | null;
  strike: number | null;
  expiry: number | null;
  instrument: string | null;
}

interface BestScores {
  bestPutScore: number;
  bestCallScore: number;
  windowDays: number;
  bestPutDetail: OptionDetail | null;
  bestCallDetail: OptionDetail | null;
}

interface HeatmapSnapshot {
  timestamp: string;
  option_type: string;
  instrument_name: string;
  strike: number;
  delta: number | null;
  ask_price: number | null;
  bid_price: number | null;
  index_price: number | null;
  expiry: number | null;
  ask_delta_value: number | null;
  bid_delta_value: number | null;
}

interface HeatmapDot {
  ts: number;
  pctOtm: number;
  absDelta: number;
  premium: number;
  value: number | null;
  strike: number;
  delta: number | null;
  bid: number | null;
  ask: number | null;
  dte: number | null;
  intensity: number; // 0-1 normalized
}

interface TickSummary {
  id: number;
  timestamp: string;
  summary: string;
}

interface TickData {
  price: number;
  medium_momentum: { main: string; derivative: string | null } | string;
  short_momentum: { main: string; derivative: string | null } | string;
  onchain: {
    liquidity_flow: { direction: string; magnitude: number; confidence: number } | null;
    market_health: string | null;
  };
  instruments: { total: number; put_candidates: number; call_candidates: number };
  historical: { total_data_points: number; filtered_data_points: number; best_put_score: number; best_call_score: number };
  strategy: { put_valid: number; call_valid: number };
  current_best_put: number;
  current_best_call: number;
  best_put_detail: { delta: number | null; price: number | null; strike: number | null; expiry: number | null; instrument: string | null } | null;
  best_call_detail: { delta: number | null; price: number | null; strike: number | null; expiry: number | null; instrument: string | null } | null;
  next_check_minutes: number;
}

interface ChartData {
  prices: SpotPrice[];
  options: OptionsPoint[];
  liquidity: LiquidityPoint[];
  bestScores: BestScores;
  optionsHeatmap: HeatmapSnapshot[];
}

const emptyBudget: Budget = {
  putTotalBudget: 0, putSpent: 0, putRemaining: 0, putDaysLeft: 0,
  callTotalBudget: 0, callSpent: 0, callRemaining: 0, callDaysLeft: 0, cycleDays: 10,
};

const emptyStats: Stats = {
  last_price: 0, last_price_time: '', short_momentum: '', short_derivative: '',
  medium_momentum: '', medium_derivative: '', three_day_high: 0, three_day_low: 0,
  seven_day_high: 0, seven_day_low: 0,
  budget: emptyBudget,
};

const emptyChart: ChartData = { prices: [], options: [], liquidity: [], bestScores: { bestPutScore: 0, bestCallScore: 0, windowDays: 6.2, bestPutDetail: null, bestCallDetail: null }, optionsHeatmap: [] };
const ranges = ['1h', '6h', '24h', '3d', '6.2d', '7d', '30d'] as const;

const CHART_MARGINS = { top: 10, right: 10, left: 10, bottom: 0 };
const CHART_MARGINS_MOBILE = { top: 10, right: 10, left: 0, bottom: 0 };

// Momentum color helpers for bar cells (derivative-aware shading)
const momentumBarColorMedium = (m: string | undefined | null, derivative: string | undefined | null) => {
  if (m === 'upward') return derivative === 'accelerating' ? '#4ade80' : '#166534';
  if (m === 'downward') return derivative === 'accelerating' ? '#f87171' : '#7f1d1d';
  return '#555';
};
const momentumBarColorShort = (m: string | undefined | null, derivative: string | undefined | null) => {
  const base = derivative?.replace(/_with_spikes\(.*\)$/, '') ?? '';
  if (m === 'upward') {
    if (base === 'steep') return '#4ade80';
    if (base === 'slanted') return '#22c55e';
    if (base === 'moving') return '#16a34a';
    return '#166534'; // flat or unknown
  }
  if (m === 'downward') {
    if (base === 'steep') return '#f87171';
    if (base === 'slanted') return '#ef4444';
    if (base === 'moving') return '#dc2626';
    return '#991b1b'; // flat or unknown
  }
  return '#555';
};

// Color interpolation for heatmap intensity (0=dim, 1=bright)
const lerpColor = (a: [number, number, number], b: [number, number, number], t: number): string => {
  const r = Math.round(a[0] + (b[0] - a[0]) * t);
  const g = Math.round(a[1] + (b[1] - a[1]) * t);
  const bl = Math.round(a[2] + (b[2] - a[2]) * t);
  return `rgb(${r},${g},${bl})`;
};
const callColorDim: [number, number, number] = [60, 90, 170];    // muted blue
const callColorBright: [number, number, number] = [100, 160, 255]; // bright blue
const putColorDim: [number, number, number] = [160, 100, 50];   // muted amber
const putColorBright: [number, number, number] = [255, 160, 50]; // bright orange

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const HeatmapDotShape = ({ cx, cy, payload, type }: any) => {
  if (!cx || !cy || !payload) return null;
  const t = payload.intensity ?? 0;
  const fill = type === 'call'
    ? lerpColor(callColorDim, callColorBright, t)
    : lerpColor(putColorDim, putColorBright, t);
  return <circle cx={cx} cy={cy} r={3.5} fill={fill} fillOpacity={0.75 + t * 0.25} />;
};

export default function OverviewPage() {
  const [range, setRange] = useState<string>('6h');
  const mobile = useIsMobile();
  const margins = mobile ? CHART_MARGINS_MOBILE : CHART_MARGINS;
  const { data: stats } = usePolling<Stats>('/api/stats', emptyStats);
  const { data: chart, loading } = usePolling<ChartData>(`/api/chart?range=${range}`, emptyChart);
  const { data: ticks } = usePolling<TickSummary[]>('/api/ticks', []);
  const pinPrice = usePinnableTooltip();
  const pinLiquidity = usePinnableTooltip();
  const pinPut = usePinnableTooltip();
  const pinCall = usePinnableTooltip();

  // Parse latest tick for current option values
  const latestTick = useMemo<TickData | null>(() => {
    if (ticks.length === 0) return null;
    try { return JSON.parse(ticks[0].summary); } catch { return null; }
  }, [ticks]);

  // Shared X-axis tick formatter
  const xTickFormatter = useCallback((ts: number) => {
    const d = new Date(ts);
    return range === '1h' || range === '6h'
      ? d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
      : d.toLocaleDateString([], { month: 'short', day: 'numeric' });
  }, [range]);

  // Merge all data series by snapping to nearest price point via binary search
  const merged = useMemo(() => {
    type OptionDetail = {
      delta: number | null;
      price: number | null;
      strike: number;
      expiry: number | null;
      dte: number | null;
    };
    type Row = {
      ts: number;
      price?: number;
      momentum?: string;
      shortMomentum?: string;
      mediumDerivative?: string;
      shortDerivative?: string;
      momentumVal?: number;
      bestPut?: number | null;
      bestCall?: number | null;
      bestPutDetail?: OptionDetail;
      bestCallDetail?: OptionDetail;
    };

    // Build rows from price data (primary time axis)
    const rows: Row[] = chart.prices.map(p => {
      const m = p.medium_momentum_main || 'neutral';
      return {
        ts: new Date(p.timestamp).getTime(),
        price: p.price,
        momentum: m,
        shortMomentum: p.short_momentum_main || 'neutral',
        mediumDerivative: p.medium_momentum_derivative || undefined,
        shortDerivative: p.short_momentum_derivative || undefined,
        momentumVal: 1, // all bars same height, color shows direction
      };
    });
    rows.sort((a, b) => a.ts - b.ts);
    if (rows.length === 0) return rows;

    // Binary search: find index of nearest row by timestamp
    const snapToNearest = (targetTs: number): number => {
      let lo = 0, hi = rows.length - 1;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (rows[mid].ts < targetTs) lo = mid + 1;
        else hi = mid;
      }
      if (lo > 0 && Math.abs(rows[lo - 1].ts - targetTs) < Math.abs(rows[lo].ts - targetTs))
        return lo - 1;
      return lo;
    };

    // Build best option details per timestamp from heatmap data
    // Only consider options within the bot's trading delta range
    // PUTs: delta between -0.12 and -0.02, CALLs: delta between 0.04 and 0.12
    const bestPutByTs = new Map<string, HeatmapSnapshot>();
    const bestCallByTs = new Map<string, HeatmapSnapshot>();
    for (const snap of chart.optionsHeatmap) {
      const isPut = snap.option_type === 'P' || snap.option_type === 'put' || snap.instrument_name?.includes('-P');
      const isCall = snap.option_type === 'C' || snap.option_type === 'call' || snap.instrument_name?.includes('-C');
      const d = snap.delta ?? 0;
      if (isPut && d <= -0.02 && d >= -0.12 && (snap.ask_delta_value ?? 0) > 0) {
        const prev = bestPutByTs.get(snap.timestamp);
        if (!prev || (snap.ask_delta_value ?? 0) > (prev.ask_delta_value ?? 0)) {
          bestPutByTs.set(snap.timestamp, snap);
        }
      }
      if (isCall && d >= 0.04 && d <= 0.12 && (snap.bid_delta_value ?? 0) > 0) {
        const prev = bestCallByTs.get(snap.timestamp);
        if (!prev || (snap.bid_delta_value ?? 0) > (prev.bid_delta_value ?? 0)) {
          bestCallByTs.set(snap.timestamp, snap);
        }
      }
    }

    const makeDetail = (snap: HeatmapSnapshot, isPut: boolean): OptionDetail => {
      const now = Date.now();
      const dte = snap.expiry ? Math.max(0, Math.ceil((snap.expiry * 1000 - now) / (1000 * 60 * 60 * 24))) : null;
      return {
        delta: snap.delta,
        price: isPut ? snap.ask_price : snap.bid_price,
        strike: snap.strike,
        expiry: snap.expiry,
        dte,
      };
    };

    // Snap options data to nearest price point
    for (const o of chart.options) {
      const idx = snapToNearest(new Date(o.timestamp).getTime());
      rows[idx].bestPut = o.best_put_value;
      rows[idx].bestCall = o.best_call_value;
      // Attach option details from heatmap data
      const putSnap = bestPutByTs.get(o.timestamp);
      if (putSnap) rows[idx].bestPutDetail = makeDetail(putSnap, true);
      const callSnap = bestCallByTs.get(o.timestamp);
      if (callSnap) rows[idx].bestCallDetail = makeDetail(callSnap, false);
    }

    return rows;
  }, [chart]);

  // Data for momentum bar (only points with momentum data)
  const momentumData = useMemo(() =>
    merged.filter(d => d.momentum !== undefined),
    [merged]
  );

  // Data for liquidity chart: build directly from API data (not snapped to spot prices)
  const { liquidityData, dexNames } = useMemo(() => {
    const nameSet = new Set<string>();
    const data = chart.liquidity.map(l => {
      const flat: Record<string, number> = { ts: new Date(l.timestamp as string).getTime() };
      for (const [key, val] of Object.entries(l)) {
        if (key !== 'timestamp' && typeof val === 'number') {
          flat[key] = val;
          nameSet.add(key);
        }
      }
      return flat;
    }).filter(d => Object.keys(d).length > 1);
    return { liquidityData: data, dexNames: Array.from(nameSet).sort() };
  }, [chart.liquidity]);

  // Heatmap data: split by option type, calculate % OTM and normalize premium
  const { callHeatmap, putHeatmap } = useMemo(() => {
    const calls: HeatmapDot[] = [];
    const puts: HeatmapDot[] = [];

    // Build a quick spot-price lookup from merged data
    const spotByTs = new Map<number, number>();
    for (const r of merged) {
      if (r.price) spotByTs.set(r.ts, r.price);
    }
    // Find nearest spot price for a given timestamp
    const findSpot = (ts: number, indexPrice: number | null): number | null => {
      if (indexPrice && indexPrice > 0) return indexPrice;
      const exact = spotByTs.get(ts);
      if (exact) return exact;
      // Snap to nearest merged row
      let bestDist = Infinity, bestPrice: number | null = null;
      spotByTs.forEach((v, k) => {
        const d = Math.abs(k - ts);
        if (d < bestDist) { bestDist = d; bestPrice = v; }
      });
      return bestPrice;
    };

    for (const snap of chart.optionsHeatmap) {
      const ts = new Date(snap.timestamp).getTime();
      const spot = findSpot(ts, snap.index_price);
      if (!spot || !snap.strike) continue;

      const isCall = snap.option_type === 'C' || snap.option_type === 'call' || snap.instrument_name?.includes('-C');
      const isPut = snap.option_type === 'P' || snap.option_type === 'put' || snap.instrument_name?.includes('-P');
      if (!isCall && !isPut) continue;

      const pctOtm = isCall
        ? (snap.strike - spot) / spot * 100
        : (spot - snap.strike) / spot * 100;
      if (pctOtm < 0) continue; // skip ITM

      // Premium: for calls use bid_price (what we'd sell at), for puts use ask_price (what we'd buy at)
      const premium = isCall ? (snap.bid_price ?? 0) : (snap.ask_price ?? 0);
      if (premium <= 0) continue;

      const dte = snap.expiry ? Math.max(0, Math.ceil((snap.expiry * 1000 - ts) / (1000 * 60 * 60 * 24))) : null;

      if (snap.delta == null) continue; // need delta for Y-axis

      const value = isCall ? (snap.bid_delta_value ?? null) : (snap.ask_delta_value ?? null);

      const dot: HeatmapDot = {
        ts,
        pctOtm: +pctOtm.toFixed(2),
        absDelta: +Math.abs(snap.delta).toFixed(4),
        premium,
        value,
        strike: snap.strike,
        delta: snap.delta,
        bid: snap.bid_price,
        ask: snap.ask_price,
        dte,
        intensity: 0, // normalized below
      };

      if (isCall) calls.push(dot);
      else puts.push(dot);
    }

    // Normalize intensity within each set
    const normalize = (dots: HeatmapDot[]) => {
      if (dots.length === 0) return;
      // Group by delta band (0.02-wide buckets)
      const bandSize = 0.02;
      const bands = new Map<number, HeatmapDot[]>();
      for (const d of dots) {
        const band = Math.floor(d.absDelta / bandSize) * bandSize;
        if (!bands.has(band)) bands.set(band, []);
        bands.get(band)!.push(d);
      }
      // Normalize intensity by value within each band
      for (const group of Array.from(bands.values())) {
        const values = group.map(d => d.value ?? 0);
        const min = Math.min(...values);
        const max = Math.max(...values);
        const range = max - min || 1;
        for (const d of group) {
          d.intensity = ((d.value ?? 0) - min) / range;
        }
      }
    };
    normalize(calls);
    normalize(puts);

    return { callHeatmap: calls, putHeatmap: puts };
  }, [chart.optionsHeatmap, merged]);

  // Shared X-axis domain from main chart's time range
  const xDomain = merged.length > 0
    ? [merged[0].ts, merged[merged.length - 1].ts]
    : [0, 1];

  // Filter sub-chart data to match the selected time range
  const filteredLiquidity = useMemo(() =>
    liquidityData.filter(d => d.ts >= xDomain[0] && d.ts <= xDomain[1]),
    [liquidityData, xDomain]
  );
  const filteredCallHeatmap = useMemo(() =>
    callHeatmap.filter(d => d.ts >= xDomain[0] && d.ts <= xDomain[1]),
    [callHeatmap, xDomain]
  );
  const filteredPutHeatmap = useMemo(() =>
    putHeatmap.filter(d => d.ts >= xDomain[0] && d.ts <= xDomain[1]),
    [putHeatmap, xDomain]
  );

  return (
    <div className="space-y-6">
      {/* Left: Range + Best Options | Right: Momentum */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
        <Card title="Price Range" subtitle="High / Low" className="flex flex-col">
          <div className="flex-1 flex flex-col justify-center gap-2 text-sm">
            <div className="flex items-center gap-3">
              <span className="text-xs text-gray-500 whitespace-nowrap">3d</span>
              <span className="text-emerald-400">{formatUSD(stats.three_day_high)}</span>
              <span className="text-gray-600">/</span>
              <span className="text-red-400">{formatUSD(stats.three_day_low)}</span>
            </div>
            <div className="flex items-center gap-3">
              <span className="text-xs text-gray-500 whitespace-nowrap">7d</span>
              <span className="text-emerald-400">{formatUSD(stats.seven_day_high)}</span>
              <span className="text-gray-600">/</span>
              <span className="text-red-400">{formatUSD(stats.seven_day_low)}</span>
            </div>
          </div>
        </Card>

        <Card title="Options Value" subtitle={`Best (${chart.bestScores.windowDays}d) / Current`} className="flex flex-col overflow-visible">
          <div className="flex-1 flex flex-col justify-center gap-2 text-sm">
            <div className="flex items-center gap-3">
              <span className="text-xs text-gray-500 whitespace-nowrap">PUT</span>
              <span className="relative group/pb">
                <span className="text-red-400 font-medium cursor-help">{Number(chart.bestScores.bestPutScore) > 0 ? Number(chart.bestScores.bestPutScore).toFixed(6) : '--'}</span>
                {chart.bestScores.bestPutDetail && (
                  <div className="absolute left-0 bottom-full mb-1 hidden group-hover/pb:block z-20 pointer-events-none">
                    <div className="bg-[#1a1a1a] border border-white/15 rounded-lg px-3 py-2 text-xs whitespace-nowrap shadow-lg">
                      <div className="text-gray-400 mb-1">Best PUT ({chart.bestScores.windowDays}d)</div>
                      <div>Delta: <span className="text-white">{chart.bestScores.bestPutDetail.delta != null ? Number(chart.bestScores.bestPutDetail.delta).toFixed(4) : 'N/A'}</span></div>
                      <div>Price: <span className="text-white">{chart.bestScores.bestPutDetail.price != null ? Number(chart.bestScores.bestPutDetail.price).toFixed(6) : 'N/A'}</span></div>
                      <div>Strike: <span className="text-white">{chart.bestScores.bestPutDetail.strike != null ? `$${Number(chart.bestScores.bestPutDetail.strike).toFixed(0)}` : 'N/A'}</span></div>
                      <div>DTE: <span className="text-white">{dteDays(chart.bestScores.bestPutDetail.expiry) ?? 'N/A'}</span></div>
                      {chart.bestScores.bestPutDetail.instrument && <div className="text-gray-400 mt-1">{chart.bestScores.bestPutDetail.instrument}</div>}
                    </div>
                  </div>
                )}
              </span>
              <span className="text-gray-600">/</span>
              <span className="relative group/pc">
                <span className="text-red-400 cursor-help">{Number(latestTick?.current_best_put ?? 0) > 0 ? Number(latestTick!.current_best_put).toFixed(6) : '--'}</span>
                {latestTick?.best_put_detail && (
                  <div className="absolute left-0 bottom-full mb-1 hidden group-hover/pc:block z-20 pointer-events-none">
                    <div className="bg-[#1a1a1a] border border-white/15 rounded-lg px-3 py-2 text-xs whitespace-nowrap shadow-lg">
                      <div className="text-gray-400 mb-1">Current best PUT</div>
                      <div>Delta: <span className="text-white">{latestTick.best_put_detail.delta != null ? Number(latestTick.best_put_detail.delta).toFixed(4) : 'N/A'}</span></div>
                      <div>Price: <span className="text-white">{latestTick.best_put_detail.price != null ? Number(latestTick.best_put_detail.price).toFixed(6) : 'N/A'}</span></div>
                      <div>Strike: <span className="text-white">{latestTick.best_put_detail.strike != null ? `$${Number(latestTick.best_put_detail.strike).toFixed(0)}` : 'N/A'}</span></div>
                      <div>DTE: <span className="text-white">{dteDays(latestTick.best_put_detail.expiry) ?? 'N/A'}</span></div>
                      {latestTick.best_put_detail.instrument && <div className="text-gray-400 mt-1">{latestTick.best_put_detail.instrument}</div>}
                    </div>
                  </div>
                )}
              </span>
            </div>
            <div className="flex items-center gap-3">
              <span className="text-xs text-gray-500 whitespace-nowrap">CALL</span>
              <span className="relative group/cb">
                <span className="text-cyan-400 font-medium cursor-help">{Number(chart.bestScores.bestCallScore) > 0 ? Number(chart.bestScores.bestCallScore).toFixed(2) : '--'}</span>
                {chart.bestScores.bestCallDetail && (
                  <div className="absolute left-0 bottom-full mb-1 hidden group-hover/cb:block z-20 pointer-events-none">
                    <div className="bg-[#1a1a1a] border border-white/15 rounded-lg px-3 py-2 text-xs whitespace-nowrap shadow-lg">
                      <div className="text-gray-400 mb-1">Best CALL ({chart.bestScores.windowDays}d)</div>
                      <div>Delta: <span className="text-white">{chart.bestScores.bestCallDetail.delta != null ? Number(chart.bestScores.bestCallDetail.delta).toFixed(4) : 'N/A'}</span></div>
                      <div>Price: <span className="text-white">{chart.bestScores.bestCallDetail.price != null ? Number(chart.bestScores.bestCallDetail.price).toFixed(6) : 'N/A'}</span></div>
                      <div>Strike: <span className="text-white">{chart.bestScores.bestCallDetail.strike != null ? `$${Number(chart.bestScores.bestCallDetail.strike).toFixed(0)}` : 'N/A'}</span></div>
                      <div>DTE: <span className="text-white">{dteDays(chart.bestScores.bestCallDetail.expiry) ?? 'N/A'}</span></div>
                      {chart.bestScores.bestCallDetail.instrument && <div className="text-gray-400 mt-1">{chart.bestScores.bestCallDetail.instrument}</div>}
                    </div>
                  </div>
                )}
              </span>
              <span className="text-gray-600">/</span>
              <span className="relative group/cc">
                <span className="text-cyan-400 cursor-help">{Number(latestTick?.current_best_call ?? 0) > 0 ? Number(latestTick!.current_best_call).toFixed(2) : '--'}</span>
                {latestTick?.best_call_detail && (
                  <div className="absolute left-0 bottom-full mb-1 hidden group-hover/cc:block z-20 pointer-events-none">
                    <div className="bg-[#1a1a1a] border border-white/15 rounded-lg px-3 py-2 text-xs whitespace-nowrap shadow-lg">
                      <div className="text-gray-400 mb-1">Current best CALL</div>
                      <div>Delta: <span className="text-white">{latestTick.best_call_detail.delta != null ? Number(latestTick.best_call_detail.delta).toFixed(4) : 'N/A'}</span></div>
                      <div>Price: <span className="text-white">{latestTick.best_call_detail.price != null ? Number(latestTick.best_call_detail.price).toFixed(6) : 'N/A'}</span></div>
                      <div>Strike: <span className="text-white">{latestTick.best_call_detail.strike != null ? `$${Number(latestTick.best_call_detail.strike).toFixed(0)}` : 'N/A'}</span></div>
                      <div>DTE: <span className="text-white">{dteDays(latestTick.best_call_detail.expiry) ?? 'N/A'}</span></div>
                      {latestTick.best_call_detail.instrument && <div className="text-gray-400 mt-1">{latestTick.best_call_detail.instrument}</div>}
                    </div>
                  </div>
                )}
              </span>
            </div>
          </div>
        </Card>

        <Card title="Momentum" className="sm:col-span-2 flex flex-col overflow-hidden">
          <div className="flex-1 flex flex-col justify-center gap-2 min-w-0">
            <div className="flex items-center gap-3 min-w-0">
              <span className="text-xs text-gray-500 w-20 shrink-0">Medium term</span>
              <span className={`text-sm font-medium ${momentumColor(stats.medium_momentum)}`}>
                {stats.medium_momentum || 'neutral'}
              </span>
              {stats.medium_derivative && <span className="text-xs text-gray-500 truncate">({stats.medium_derivative})</span>}
            </div>
            <div className="flex items-center gap-3 min-w-0">
              <span className="text-xs text-gray-500 w-20 shrink-0">Short term</span>
              <span className={`text-sm font-medium ${momentumColor(stats.short_momentum)}`}>
                {stats.short_momentum || 'neutral'}
              </span>
              {stats.short_derivative && <span className="text-xs text-gray-500 truncate">({stats.short_derivative})</span>}
            </div>
          </div>
        </Card>
      </div>

      {/* Time Range Selector */}
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex gap-1 overflow-x-auto hide-scrollbar">
          {ranges.map(r => (
            <button
              key={r}
              onClick={() => setRange(r)}
              className={`px-2 md:px-3 py-1 rounded text-xs md:text-sm shrink-0 ${
                range === r
                  ? 'bg-white/10 text-white border border-white/20'
                  : 'text-gray-400 hover:bg-white/10 hover:text-white'
              }`}
            >
              {r}
            </button>
          ))}
        </div>
        <div className="flex gap-3 text-xs text-gray-500">
          <span className="flex items-center gap-1"><span className="w-3 h-0.5 inline-block" style={{ background: chartColors.primary }} /> ETH</span>
          <span className="flex items-center gap-1"><span className="w-3 h-0.5 inline-block" style={{ background: chartColors.red, opacity: 0.7 }} /> PUT</span>
          <span className="flex items-center gap-1"><span className="w-3 h-0.5 inline-block" style={{ background: chartColors.secondary, opacity: 0.7 }} /> CALL</span>
        </div>
      </div>

      {/* Big Combined Chart */}
      <Card>
        {loading && merged.length === 0 ? (
          <div className="h-[300px] md:h-[500px] flex items-center justify-center text-gray-500">Loading...</div>
        ) : merged.length === 0 ? (
          <div className="h-[300px] md:h-[500px] flex items-center justify-center text-gray-500">No data yet — bot is collecting</div>
        ) : (
          <div {...pinPrice.containerProps}>
          <ResponsiveContainer width="100%" height={mobile ? 300 : 500}>
            <ComposedChart data={merged} margin={margins}>
              <XAxis
                dataKey="ts"
                type="number"
                domain={['dataMin', 'dataMax']}
                tickFormatter={xTickFormatter}
                stroke={chartAxis.stroke}
                tick={chartAxis.tick}
              />
              {/* Left Y-axis: ETH price */}
              <YAxis
                yAxisId="price"
                domain={['auto', 'auto']}
                tickFormatter={(v) => `$${v}`}
                stroke={chartAxis.stroke}
                tick={chartAxis.tick}
                width={mobile ? 45 : 70}
              />
              {/* Hidden axes for PUT/CALL overlay */}
              <YAxis yAxisId="putVal" orientation="right" hide domain={['auto', 'auto']} />
              <YAxis yAxisId="callVal" orientation="right" hide domain={['auto', 'auto']} />
              <Tooltip
                                {...chartTooltip}
                {...pinPrice.tooltipActive}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                content={({ active, payload, label }: any) => {
                  if (!active || !payload?.length) return pinPrice.wrap(null);
                  const row = payload[0]?.payload;
                  if (!row) return pinPrice.wrap(null);
                  const bestPut = row.bestPut;
                  const bestCall = row.bestCall;
                  const fmtPut = bestPut != null && Number(bestPut) > 0 ? Number(bestPut).toFixed(6) : 'N/A';
                  const fmtCall = bestCall != null && Number(bestCall) > 0 ? Number(bestCall).toFixed(2) : 'N/A';
                  const { bestPutScore, bestCallScore, windowDays } = chart.bestScores;
                  const pd = row.bestPutDetail;
                  const cd = row.bestCallDetail;
                  return pinPrice.wrap(
                    <div style={{ ...chartTooltip.contentStyle, padding: '8px 12px' }}>
                      <div className="text-xs text-gray-400 mb-1">{new Date(label as number).toLocaleString()}</div>
                      <div className="text-sm" style={{ color: chartColors.primary }}>ETH: {row.price != null ? formatUSD(row.price) : 'N/A'}</div>
                      <div className="text-sm" style={{ color: chartColors.red }}>PUT Value: {fmtPut}{pd ? <span className="text-xs text-gray-400 ml-2">Ask {Number(pd.price).toFixed(4)} ETH</span> : null}</div>
                      {pd && (
                        <div className="text-xs text-gray-500 pl-2 mb-0.5">
                          Strike ${Number(pd.strike).toFixed(0)} | Delta {Number(pd.delta).toFixed(3)} | DTE {pd.dte ?? 'N/A'}
                        </div>
                      )}
                      <div className="text-sm" style={{ color: chartColors.secondary }}>CALL Value: {fmtCall}{cd ? <span className="text-xs text-gray-400 ml-2">Bid {Number(cd.price).toFixed(4)} ETH</span> : null}</div>
                      {cd && (
                        <div className="text-xs text-gray-500 pl-2 mb-0.5">
                          Strike ${Number(cd.strike).toFixed(0)} | Delta {Number(cd.delta).toFixed(3)} | DTE {cd.dte ?? 'N/A'}
                        </div>
                      )}
                      <div className="border-t border-white/10 mt-1.5 pt-1.5 text-xs text-gray-500">
                        Best scores ({windowDays}d window):
                        <span style={{ color: chartColors.red }}> P {Number(bestPutScore).toFixed(6)}</span>
                        {' / '}
                        <span style={{ color: chartColors.secondary }}>C {Number(bestCallScore).toFixed(2)}</span>
                      </div>
                    </div>
                  );
                }}
              />
              <Legend content={() => null} />

              {/* 7d high/low reference lines */}
              {stats.seven_day_high > 0 && (
                <ReferenceLine yAxisId="price" y={stats.seven_day_high} stroke={chartColors.refHigh} strokeDasharray="3 3" strokeOpacity={0.4} />
              )}
              {stats.seven_day_low > 0 && stats.seven_day_low < Infinity && (
                <ReferenceLine yAxisId="price" y={stats.seven_day_low} stroke={chartColors.refLow} strokeDasharray="3 3" strokeOpacity={0.4} />
              )}

              {/* Best score reference lines (dotted) */}
              {chart.bestScores.bestPutScore > 0 && (
                <ReferenceLine yAxisId="putVal" y={chart.bestScores.bestPutScore} stroke={chartColors.red} strokeDasharray="4 4" strokeOpacity={0.5} />
              )}
              {chart.bestScores.bestCallScore > 0 && (
                <ReferenceLine yAxisId="callVal" y={chart.bestScores.bestCallScore} stroke={chartColors.secondary} strokeDasharray="4 4" strokeOpacity={0.5} />
              )}

              {/* ETH price line */}
              <Line yAxisId="price" type="monotone" dataKey="price" stroke={chartColors.primary} dot={false} strokeWidth={2} connectNulls isAnimationActive={false} />
              {/* PUT/CALL value overlays */}
              <Line yAxisId="putVal" type="stepAfter" dataKey="bestPut" stroke={chartColors.red} strokeWidth={1} strokeOpacity={0.7} dot={false} connectNulls={false} isAnimationActive={false} />
              <Line yAxisId="callVal" type="stepAfter" dataKey="bestCall" stroke={chartColors.secondary} strokeWidth={1} strokeOpacity={0.7} dot={false} connectNulls={false} isAnimationActive={false} />
            </ComposedChart>
          </ResponsiveContainer>
          </div>
        )}
      </Card>

      {/* Momentum Bar — two rows: medium (top) + short (bottom) */}
      {momentumData.length > 0 && (() => {
        const MomentumTooltipBar = ({ data }: { data: typeof momentumData }) => {
          // eslint-disable-next-line react-hooks/rules-of-hooks
          const [hover, setHover] = useState<{ idx: number; x: number; y: number } | null>(null);
          // eslint-disable-next-line react-hooks/rules-of-hooks
          const [pinned, setPinned] = useState<{ idx: number; x: number; y: number } | null>(null);
          const active = pinned ?? hover;
          const hovered = active ? data[active.idx] : null;

          const onCellEnter = (i: number, e: React.MouseEvent) => {
            if (!pinned) setHover({ idx: i, x: e.clientX, y: e.clientY });
          };
          const onCellMove = (i: number, e: React.MouseEvent) => {
            if (!pinned) setHover({ idx: i, x: e.clientX, y: e.clientY });
          };
          const onCellClick = (i: number, e: React.MouseEvent) => {
            e.stopPropagation();
            if (pinned) { setPinned(null); } else { setPinned({ idx: i, x: e.clientX, y: e.clientY }); }
          };

          const yAxisWidth = mobile ? 35 : 55;
          const leftPad = margins.left + yAxisWidth;

          return (
            <div style={{ paddingLeft: leftPad, paddingRight: margins.right }}>
              <div className="relative" style={{ height: 28 }}>
                <span className="absolute right-full pr-1 top-1/2 -translate-y-1/2 text-[10px] text-gray-500 whitespace-nowrap">medium term</span>
                <div className="flex overflow-hidden h-full">
                  {data.map((d, i) => (
                    <div
                      key={i}
                      className="flex-1"
                      style={{ background: momentumBarColorMedium(d.momentum, d.mediumDerivative) }}
                      onMouseEnter={(e) => onCellEnter(i, e)}
                      onMouseMove={(e) => onCellMove(i, e)}
                      onMouseLeave={() => { if (!pinned) setHover(null); }}
                      onClick={(e) => onCellClick(i, e)}
                    />
                  ))}
                </div>
              </div>
              <div className="relative -mt-px" style={{ height: 28 }}>
                <span className="absolute right-full pr-1 top-1/2 -translate-y-1/2 text-[10px] text-gray-500 whitespace-nowrap">short term</span>
                <div className="flex overflow-hidden h-full">
                  {data.map((d, i) => (
                    <div
                      key={i}
                      className="flex-1"
                      style={{ background: momentumBarColorShort(d.shortMomentum, d.shortDerivative) }}
                      onMouseEnter={(e) => onCellEnter(i, e)}
                      onMouseMove={(e) => onCellMove(i, e)}
                      onMouseLeave={() => { if (!pinned) setHover(null); }}
                      onClick={(e) => onCellClick(i, e)}
                    />
                  ))}
                </div>
              </div>
              {pinned && createPortal(
                <div className="fixed inset-0" style={{ zIndex: 9998 }} onClick={() => setPinned(null)} />,
                document.body
              )}
              {hovered && active && createPortal(
                <div
                  className="fixed pointer-events-none"
                  style={{ top: active.y - 12, left: active.x, transform: 'translate(-50%, -100%)', zIndex: 9999 }}
                >
                  <div className="bg-[#1a1a1a] border border-white/15 rounded-lg px-3 py-2 text-xs whitespace-nowrap shadow-lg">
                    <div className="text-gray-400 mb-1">{new Date(hovered.ts).toLocaleString()}</div>
                    <div className="flex items-center gap-2">
                      <span className="w-2 h-2 rounded-sm inline-block" style={{ background: momentumBarColorMedium(hovered.momentum, hovered.mediumDerivative) }} />
                      <span className="text-white">Medium: {hovered.momentum}</span>
                      <span className="text-gray-500">{hovered.mediumDerivative ?? 'n/a'}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="w-2 h-2 rounded-sm inline-block" style={{ background: momentumBarColorShort(hovered.shortMomentum, hovered.shortDerivative) }} />
                      <span className="text-white">Short: {hovered.shortMomentum}</span>
                      <span className="text-gray-500">{hovered.shortDerivative ?? 'n/a'}</span>
                    </div>
                  </div>
                </div>,
                document.body
              )}
            </div>
          );
        };
        return (
        <Card>
          <div className="flex flex-wrap items-center justify-between gap-1 mb-1">
            <span className="text-xs font-medium text-gray-400">Momentum</span>
            <div className="flex gap-3 text-xs text-gray-500">
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: '#4ade80' }} /> bright = accelerating</span>
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: '#166534' }} /> dim = decelerating</span>
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block border border-white/10" style={{ background: '#555' }} /> neutral</span>
            </div>
          </div>
          <div style={{ marginLeft: margins.left + (mobile ? 45 : 70) - 44, marginRight: margins.right }}>
            <MomentumTooltipBar data={momentumData} />
            {/* Time axis */}
            <div className="flex items-center gap-1">
              <span className="w-10 shrink-0" />
              <div className="flex justify-between flex-1 mt-1">
                {(() => {
                  const tickCount = mobile ? 4 : 6;
                  const len = momentumData.length;
                  if (len === 0) return null;
                  const ticks: number[] = [];
                  for (let i = 0; i < tickCount; i++) {
                    ticks.push(momentumData[Math.round(i * (len - 1) / (tickCount - 1))].ts);
                  }
                  return ticks.map((ts, i) => (
                    <span key={i} className="text-[10px] text-gray-500">{xTickFormatter(ts)}</span>
                  ));
                })()}
              </div>
            </div>
          </div>
        </Card>
        );
      })()}

      {/* DEX Liquidity (TVL) */}
      {filteredLiquidity.length > 0 && (() => {
        const dexColors: Record<string, string> = {
          uniswap_v3: '#ff007a', // Uniswap pink
          uniswap_v4: '#fc72ff', // Uniswap V4 purple-pink
        };
        const fallbackColors = ['#3b82f6', '#10b981', '#f59e0b', '#8b5cf6'];
        const getColor = (name: string, i: number) => dexColors[name] || fallbackColors[i % fallbackColors.length];
        const formatDexName = (name: string) => name.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

        // Normalize each DEX to % change from its first value so they share one Y-axis
        const baselines: Record<string, number> = {};
        for (const name of dexNames) {
          const first = filteredLiquidity.find(d => d[name] != null);
          baselines[name] = first ? first[name] : 1;
        }
        const normalizedData = filteredLiquidity.map(d => {
          const out: Record<string, number> = { ts: d.ts };
          for (const name of dexNames) {
            if (d[name] != null) {
              out[`${name}_pct`] = ((d[name] - baselines[name]) / baselines[name]) * 100;
              out[name] = d[name]; // keep raw for tooltip
            }
          }
          return out;
        });

        return (
          <Card>
            <div className="flex flex-wrap items-center justify-between gap-1 mb-1">
              <span className="text-xs font-medium text-gray-400">DEX Liquidity (TVL)</span>
              <div className="flex gap-3 text-xs text-gray-500">
                {dexNames.map((name, i) => (
                  <span key={name} className="flex items-center gap-1">
                    <span className="w-3 h-0.5 inline-block" style={{ background: getColor(name, i) }} /> {formatDexName(name)}
                  </span>
                ))}
              </div>
            </div>
            <div {...pinLiquidity.containerProps}>
            <ResponsiveContainer width="100%" height={200}>
              <ComposedChart data={normalizedData} margin={margins}>
                <XAxis dataKey="ts" type="number" domain={xDomain} tickFormatter={xTickFormatter} stroke={chartAxis.stroke} tick={chartAxis.tick} />
                <YAxis
                  domain={['auto', 'auto']}
                  tickFormatter={(v) => `${v > 0 ? '+' : ''}${v.toFixed(1)}%`}
                  stroke={chartAxis.stroke}
                  tick={chartAxis.tick}
                  width={mobile ? 40 : 55}
                />
                <Tooltip
                                    {...chartTooltip}
                  {...pinLiquidity.tooltipActive}
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  labelFormatter={(ts: any) => new Date(ts as number).toLocaleString()}
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  formatter={(_val: any, key: any) => {
                    // key is "uniswap_v3_pct" — extract raw value from the same data point
                    const rawName = (key as string).replace(/_pct$/, '');
                    return [null, formatDexName(rawName)]; // placeholder, custom content handles it
                  }}
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  content={({ active, payload, label }: any) => {
                    if (!active || !payload?.length) return pinLiquidity.wrap(null);
                    return pinLiquidity.wrap(
                      <div style={{ ...chartTooltip.contentStyle, padding: '8px 12px' }}>
                        <div className="text-xs text-gray-400 mb-1">{new Date(label as number).toLocaleString()}</div>
                        {dexNames.map((name, i) => {
                          const raw = payload[0]?.payload?.[name];
                          const pct = payload[0]?.payload?.[`${name}_pct`];
                          if (raw == null) return null;
                          return (
                            <div key={name} className="text-xs" style={{ color: getColor(name, i) }}>
                              {formatDexName(name)}: ${Number(raw).toLocaleString(undefined, { maximumFractionDigits: 0 })}
                              <span className="text-gray-500 ml-1">({pct > 0 ? '+' : ''}{pct?.toFixed(2)}%)</span>
                            </div>
                          );
                        })}
                      </div>
                    );
                  }}
                />
                {dexNames.map((name, i) => (
                  <Line key={name} type="stepAfter" dataKey={`${name}_pct`} stroke={getColor(name, i)} strokeWidth={1.5} dot={false} connectNulls isAnimationActive={false} />
                ))}
              </ComposedChart>
            </ResponsiveContainer>
            </div>
          </Card>
        );
      })()}

      {/* Put Market Heatmap */}
      {filteredPutHeatmap.length > 0 && (
        <Card>
          <div className="flex flex-wrap items-center justify-between gap-1 mb-1">
            <span className="text-xs font-medium text-gray-400">Put Market</span>
            <div className="flex gap-3 text-xs text-gray-500">
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: lerpColor(putColorDim, putColorBright, 0.8) }} /> better buy</span>
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: lerpColor(putColorDim, putColorBright, 0.2) }} /> worse buy</span>
            </div>
          </div>
          <div {...pinPut.containerProps}>
          <ResponsiveContainer width="100%" height={360}>
            <ScatterChart margin={margins}>
              <XAxis
                dataKey="ts"
                type="number"
                domain={xDomain}
                tickFormatter={xTickFormatter}
                stroke={chartAxis.stroke}
                tick={chartAxis.tick}
              />
              <YAxis
                dataKey="absDelta"
                name="Delta"
                domain={[0, 'auto']}
                tickFormatter={(v) => v.toFixed(2)}
                stroke={chartAxis.stroke}
                tick={chartAxis.tick}
                width={mobile ? 35 : 55}
              />
              <Tooltip
                                {...chartTooltip}
                {...pinPut.tooltipActive}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                content={({ active, payload }: any) => {
                  if (!active || !payload?.[0]?.payload) return pinPut.wrap(null);
                  const d = payload[0].payload as HeatmapDot;
                  return pinPut.wrap(
                    <div style={{ ...chartTooltip.contentStyle, padding: '8px 12px' }}>
                      <div className="text-xs text-gray-400">{new Date(d.ts).toLocaleString()}</div>
                      <div className="text-sm">Strike: <span className="text-white font-medium">${d.strike.toFixed(0)}</span></div>
                      <div className="text-sm">Delta: <span style={{ color: 'rgb(255,160,50)' }}>{d.delta?.toFixed(3) ?? 'N/A'}</span></div>
                      <div className="text-sm">Value: <span style={{ color: 'rgb(255,160,50)' }}>{d.value != null ? d.value.toFixed(6) : 'N/A'}</span></div>
                      <div className="text-sm">% OTM: <span className="text-gray-300">{d.pctOtm.toFixed(1)}%</span></div>
                      {d.dte != null && <div className="text-sm">DTE: <span className="text-gray-300">{d.dte}</span></div>}
                      <div className="text-sm">Bid: <span className="text-gray-300">{d.bid?.toFixed(4) ?? 'N/A'}</span></div>
                      <div className="text-sm">Ask: <span style={{ color: 'rgb(255,160,50)' }}>{d.ask?.toFixed(4) ?? 'N/A'}</span></div>
                    </div>
                  );
                }}
              />
              {/* Band showing bot's active put delta range: 0.02–0.12 (abs) */}
              <ReferenceArea y1={0.02} y2={0.12} fill="#FFA032" fillOpacity={0.12} stroke="#FFA032" strokeOpacity={0.15} />
              <Scatter
                data={filteredPutHeatmap}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                shape={(props: any) => <HeatmapDotShape {...props} type="put" />}
                isAnimationActive={false}
              />
            </ScatterChart>
          </ResponsiveContainer>
          </div>
        </Card>
      )}

      {/* Call Market Heatmap */}
      {filteredCallHeatmap.length > 0 && (
        <Card>
          <div className="flex flex-wrap items-center justify-between gap-1 mb-1">
            <span className="text-xs font-medium text-gray-400">Call Market</span>
            <div className="flex gap-3 text-xs text-gray-500">
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: lerpColor(callColorDim, callColorBright, 0.8) }} /> better sell</span>
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: lerpColor(callColorDim, callColorBright, 0.2) }} /> worse sell</span>
            </div>
          </div>
          <div {...pinCall.containerProps}>
          <ResponsiveContainer width="100%" height={360}>
            <ScatterChart margin={margins}>
              <XAxis
                dataKey="ts"
                type="number"
                domain={xDomain}
                tickFormatter={xTickFormatter}
                stroke={chartAxis.stroke}
                tick={chartAxis.tick}
              />
              <YAxis
                dataKey="absDelta"
                name="Delta"
                domain={[0, 'auto']}
                tickFormatter={(v) => v.toFixed(2)}
                stroke={chartAxis.stroke}
                tick={chartAxis.tick}
                width={mobile ? 35 : 55}
              />
              <Tooltip
                                {...chartTooltip}
                {...pinCall.tooltipActive}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                content={({ active, payload }: any) => {
                  if (!active || !payload?.[0]?.payload) return pinCall.wrap(null);
                  const d = payload[0].payload as HeatmapDot;
                  return pinCall.wrap(
                    <div style={{ ...chartTooltip.contentStyle, padding: '8px 12px' }}>
                      <div className="text-xs text-gray-400">{new Date(d.ts).toLocaleString()}</div>
                      <div className="text-sm">Strike: <span className="text-white font-medium">${d.strike.toFixed(0)}</span></div>
                      <div className="text-sm">Delta: <span style={{ color: 'rgb(100,160,255)' }}>{d.delta?.toFixed(3) ?? 'N/A'}</span></div>
                      <div className="text-sm">Value: <span style={{ color: 'rgb(100,160,255)' }}>{d.value != null ? d.value.toFixed(2) : 'N/A'}</span></div>
                      <div className="text-sm">% OTM: <span className="text-gray-300">{d.pctOtm.toFixed(1)}%</span></div>
                      {d.dte != null && <div className="text-sm">DTE: <span className="text-gray-300">{d.dte}</span></div>}
                      <div className="text-sm">Bid: <span style={{ color: 'rgb(100,160,255)' }}>{d.bid?.toFixed(4) ?? 'N/A'}</span></div>
                      <div className="text-sm">Ask: <span className="text-gray-300">{d.ask?.toFixed(4) ?? 'N/A'}</span></div>
                    </div>
                  );
                }}
              />
              {/* Band showing bot's active call delta range: 0.04–0.12 */}
              <ReferenceArea y1={0.04} y2={0.12} fill="#64A0FF" fillOpacity={0.12} stroke="#64A0FF" strokeOpacity={0.15} />
              <Scatter
                data={filteredCallHeatmap}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                shape={(props: any) => <HeatmapDotShape {...props} type="call" />}
                isAnimationActive={false}
              />
            </ScatterChart>
          </ResponsiveContainer>
          </div>
        </Card>
      )}

      {/* Data Table: spot price, best put, best call */}
      {/* Tick Log Table */}
      <Card>
        <div className="flex items-center justify-between mb-2">
          <span className="text-xs font-medium text-gray-400">Tick Log</span>
          <span className="text-xs text-gray-600">{ticks.length} ticks</span>
        </div>
        <div className="overflow-auto max-h-[300px] md:max-h-[400px]">
          <table className="w-full text-xs md:text-sm">
            <thead className="sticky top-0 bg-[#111] z-10">
              <tr className="text-xs text-gray-500 border-b border-white/5">
                <th className="text-left py-2 px-3 font-medium">Time</th>
                <th className="text-right py-2 px-3 font-medium">Price</th>
                <th className="text-right py-2 px-3 font-medium" style={{ color: chartColors.red }}>PUT Now / Best</th>
                <th className="text-right py-2 px-3 font-medium" style={{ color: chartColors.secondary }}>CALL Now / Best</th>
                <th className="text-right py-2 px-3 font-medium">Instruments</th>
                <th className="text-right py-2 px-3 font-medium">Valid</th>
                <th className="text-right py-2 px-3 font-medium">Flow</th>
                <th className="text-right py-2 px-3 font-medium">Health</th>
                <th className="text-right py-2 px-3 font-medium">Next</th>
              </tr>
            </thead>
            <tbody>
              {ticks.length === 0 ? (
                <tr><td colSpan={9} className="py-4 text-center text-gray-500 text-xs">No tick data yet</td></tr>
              ) : ticks.map((tick) => {
                let d: TickData | null = null;
                try { d = JSON.parse(tick.summary); } catch { /* skip */ }
                if (!d) return null;
                const flow = d.onchain?.liquidity_flow;
                return (
                  <tr key={tick.id} className="border-b border-white/[0.03] hover:bg-white/[0.02]">
                    <td className="py-1.5 px-3 text-gray-400 text-xs whitespace-nowrap">
                      {new Date(tick.timestamp).toLocaleString()}
                    </td>
                    <td className="py-1.5 px-3 text-right text-juice-orange tabular-nums">
                      {formatUSD(d.price)}
                    </td>
                    <td className="py-1.5 px-3 text-right tabular-nums text-xs">
                      <span className="relative inline-block group/pn">
                        <span style={{ color: chartColors.red }} className="cursor-help">{Number(d.current_best_put ?? 0) > 0 ? Number(d.current_best_put).toFixed(6) : '--'}</span>
                        {d.best_put_detail && (
                          <div className="absolute right-0 top-full mt-1 hidden group-hover/pn:block z-20 pointer-events-none">
                            <div className="bg-[#1a1a1a] border border-white/15 rounded-lg px-3 py-2 text-xs whitespace-nowrap shadow-lg">
                              <div className="text-gray-400 mb-1">Current best PUT</div>
                              <div>Delta: <span className="text-white">{Number(d.best_put_detail.delta).toFixed(4)}</span></div>
                              <div>Price: <span className="text-white">{Number(d.best_put_detail.price).toFixed(6)}</span></div>
                              <div>Strike: <span className="text-white">${Number(d.best_put_detail.strike).toFixed(0)}</span></div>
                              <div>DTE: <span className="text-white">{dteDays(d.best_put_detail.expiry) ?? 'N/A'}</span></div>
                              {d.best_put_detail.instrument && <div className="text-gray-400 mt-1">{d.best_put_detail.instrument}</div>}
                            </div>
                          </div>
                        )}
                      </span>
                      <span className="text-gray-600"> / </span>
                      <span className="relative inline-block group/pb">
                        <span className="text-gray-500 cursor-help">{Number(d.historical?.best_put_score) > 0 ? Number(d.historical.best_put_score).toFixed(6) : '--'}</span>
                        {chart.bestScores.bestPutDetail && (
                          <div className="absolute right-0 top-full mt-1 hidden group-hover/pb:block z-20 pointer-events-none">
                            <div className="bg-[#1a1a1a] border border-white/15 rounded-lg px-3 py-2 text-xs whitespace-nowrap shadow-lg">
                              <div className="text-gray-400 mb-1">Best PUT ({chart.bestScores.windowDays}d)</div>
                              <div>Delta: <span className="text-white">{Number(chart.bestScores.bestPutDetail.delta).toFixed(4)}</span></div>
                              <div>Price: <span className="text-white">{Number(chart.bestScores.bestPutDetail.price).toFixed(6)}</span></div>
                              <div>Strike: <span className="text-white">${Number(chart.bestScores.bestPutDetail.strike).toFixed(0)}</span></div>
                              <div>DTE: <span className="text-white">{dteDays(chart.bestScores.bestPutDetail.expiry) ?? 'N/A'}</span></div>
                              {chart.bestScores.bestPutDetail.instrument && <div className="text-gray-400 mt-1">{chart.bestScores.bestPutDetail.instrument}</div>}
                            </div>
                          </div>
                        )}
                      </span>
                    </td>
                    <td className="py-1.5 px-3 text-right tabular-nums text-xs">
                      <span className="relative inline-block group/cn">
                        <span style={{ color: chartColors.secondary }} className="cursor-help">{Number(d.current_best_call ?? 0) > 0 ? Number(d.current_best_call).toFixed(2) : '--'}</span>
                        {d.best_call_detail && (
                          <div className="absolute right-0 top-full mt-1 hidden group-hover/cn:block z-20 pointer-events-none">
                            <div className="bg-[#1a1a1a] border border-white/15 rounded-lg px-3 py-2 text-xs whitespace-nowrap shadow-lg">
                              <div className="text-gray-400 mb-1">Current best CALL</div>
                              <div>Delta: <span className="text-white">{Number(d.best_call_detail.delta).toFixed(4)}</span></div>
                              <div>Price: <span className="text-white">{Number(d.best_call_detail.price).toFixed(6)}</span></div>
                              <div>Strike: <span className="text-white">${Number(d.best_call_detail.strike).toFixed(0)}</span></div>
                              <div>DTE: <span className="text-white">{dteDays(d.best_call_detail.expiry) ?? 'N/A'}</span></div>
                              {d.best_call_detail.instrument && <div className="text-gray-400 mt-1">{d.best_call_detail.instrument}</div>}
                            </div>
                          </div>
                        )}
                      </span>
                      <span className="text-gray-600"> / </span>
                      <span className="relative inline-block group/cb">
                        <span className="text-gray-500 cursor-help">{Number(d.historical?.best_call_score) > 0 ? Number(d.historical.best_call_score).toFixed(2) : '--'}</span>
                        {chart.bestScores.bestCallDetail && (
                          <div className="absolute right-0 top-full mt-1 hidden group-hover/cb:block z-20 pointer-events-none">
                            <div className="bg-[#1a1a1a] border border-white/15 rounded-lg px-3 py-2 text-xs whitespace-nowrap shadow-lg">
                              <div className="text-gray-400 mb-1">Best CALL ({chart.bestScores.windowDays}d)</div>
                              <div>Delta: <span className="text-white">{Number(chart.bestScores.bestCallDetail.delta).toFixed(4)}</span></div>
                              <div>Price: <span className="text-white">{Number(chart.bestScores.bestCallDetail.price).toFixed(6)}</span></div>
                              <div>Strike: <span className="text-white">${Number(chart.bestScores.bestCallDetail.strike).toFixed(0)}</span></div>
                              <div>DTE: <span className="text-white">{dteDays(chart.bestScores.bestCallDetail.expiry) ?? 'N/A'}</span></div>
                              {chart.bestScores.bestCallDetail.instrument && <div className="text-gray-400 mt-1">{chart.bestScores.bestCallDetail.instrument}</div>}
                            </div>
                          </div>
                        )}
                      </span>
                    </td>
                    <td className="py-1.5 px-3 text-right text-xs text-gray-400">
                      {d.instruments.total} <span className="text-gray-600">({d.instruments.put_candidates}P/{d.instruments.call_candidates}C)</span>
                    </td>
                    <td className="py-1.5 px-3 text-right text-xs text-white">
                      {d.strategy.put_valid}P / {d.strategy.call_valid}C
                    </td>
                    <td className="py-1.5 px-3 text-right text-xs text-white">
                      {flow ? flow.direction : '--'}
                    </td>
                    <td className="py-1.5 px-3 text-right text-xs">
                      <span className={d.onchain.market_health === 'normal' ? 'text-emerald-400' : d.onchain.market_health ? 'text-yellow-400' : 'text-gray-600'}>
                        {d.onchain.market_health || '--'}
                      </span>
                    </td>
                    <td className="py-1.5 px-3 text-right text-xs text-gray-500">
                      {Number(d.next_check_minutes ?? 0).toFixed(0)}m
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}
