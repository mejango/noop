'use client';

import { useState, useMemo, useRef, useCallback } from 'react';
import { usePolling } from '@/lib/hooks';
import { formatUSD, timeAgo, momentumColor, momentumBg } from '@/lib/format';
import { chartColors, chartAxis, chartTooltip } from '@/lib/chart';
import Card from '@/components/Card';
import {
  ComposedChart, Line, Area, Scatter, Bar, Cell, XAxis, YAxis, Tooltip,
  ResponsiveContainer, Legend, ReferenceLine, BarChart, ScatterChart, ReferenceArea,
} from 'recharts';

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
  open_puts: number;
  open_calls: number;
  total_positions: number;
  total_trades: number;
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
  open_put_cost: number;
  open_call_revenue: number;
  budget: Budget;
}

interface SpotPrice {
  timestamp: string;
  price: number;
  short_momentum_main: string;
  medium_momentum_main: string;
}

interface OptionsPoint {
  timestamp: string;
  best_put_value: number | null;
  best_call_value: number | null;
}

interface LiquidityPoint {
  timestamp: string;
  signed_liquidity: number;
}

interface TradeMarker {
  timestamp: string;
  direction: string;
  amount: number;
  price: number;
  total_value: number;
  instrument_name: string;
  strike: number;
}

interface BestScores {
  bestPutScore: number;
  bestCallScore: number;
  windowDays: number;
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
  premium: number;
  strike: number;
  delta: number | null;
  bid: number | null;
  ask: number | null;
  intensity: number; // 0-1 normalized
}

interface ChartData {
  prices: SpotPrice[];
  options: OptionsPoint[];
  liquidity: LiquidityPoint[];
  trades: TradeMarker[];
  bestScores: BestScores;
  optionsHeatmap: HeatmapSnapshot[];
}

const emptyBudget: Budget = {
  putTotalBudget: 0, putSpent: 0, putRemaining: 0, putDaysLeft: 0,
  callTotalBudget: 0, callSpent: 0, callRemaining: 0, callDaysLeft: 0, cycleDays: 10,
};

const emptyStats: Stats = {
  open_puts: 0, open_calls: 0, total_positions: 0, total_trades: 0,
  last_price: 0, last_price_time: '', short_momentum: '', short_derivative: '',
  medium_momentum: '', medium_derivative: '', three_day_high: 0, three_day_low: 0,
  seven_day_high: 0, seven_day_low: 0, open_put_cost: 0, open_call_revenue: 0,
  budget: emptyBudget,
};

const emptyChart: ChartData = { prices: [], options: [], liquidity: [], trades: [], bestScores: { bestPutScore: 0, bestCallScore: 0, windowDays: 6.2 }, optionsHeatmap: [] };
const ranges = ['1h', '6h', '24h', '3d', '7d', '30d'] as const;

const CHART_MARGINS = { top: 10, right: 120, left: 10, bottom: 0 };
const PAGE_SIZE = 100;

// Custom star shape for trade markers
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const StarDot = (props: any) => {
  const { cx, cy } = props;
  if (!cx || !cy) return null;
  return (
    <svg x={cx - 8} y={cy - 8} width={16} height={16} viewBox="0 0 24 24" fill={chartColors.trade} stroke="#a16207" strokeWidth={1}>
      <polygon points="12,2 15.09,8.26 22,9.27 17,14.14 18.18,21.02 12,17.77 5.82,21.02 7,14.14 2,9.27 8.91,8.26" />
    </svg>
  );
};

// Momentum color helper for bar cells
const momentumBarColor = (m: string | undefined | null) =>
  m === 'upward' ? '#10b981' : m === 'downward' ? '#ef4444' : 'rgba(255,255,255,0.25)';

// Color interpolation for heatmap intensity (0=dim, 1=bright)
const lerpColor = (a: [number, number, number], b: [number, number, number], t: number): string => {
  const r = Math.round(a[0] + (b[0] - a[0]) * t);
  const g = Math.round(a[1] + (b[1] - a[1]) * t);
  const bl = Math.round(a[2] + (b[2] - a[2]) * t);
  return `rgb(${r},${g},${bl})`;
};
const callColorDim: [number, number, number] = [20, 40, 80];   // dark blue
const callColorBright: [number, number, number] = [92, 235, 223]; // juice-cyan
const putColorDim: [number, number, number] = [80, 20, 20];    // dark red
const putColorBright: [number, number, number] = [248, 113, 113]; // red-400

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const HeatmapDotShape = ({ cx, cy, payload, type }: any) => {
  if (!cx || !cy || !payload) return null;
  const t = payload.intensity ?? 0;
  const fill = type === 'call'
    ? lerpColor(callColorDim, callColorBright, t)
    : lerpColor(putColorDim, putColorBright, t);
  return <circle cx={cx} cy={cy} r={3} fill={fill} fillOpacity={0.6 + t * 0.4} />;
};

export default function OverviewPage() {
  const [range, setRange] = useState<string>('7d');
  const [visibleCount, setVisibleCount] = useState(PAGE_SIZE);
  const tableRef = useRef<HTMLDivElement>(null);
  const { data: stats } = usePolling<Stats>('/api/stats', emptyStats);
  const { data: chart, loading } = usePolling<ChartData>(`/api/chart?range=${range}`, emptyChart);

  // Best scores from the bot's 6.2-day measurement window (always fixed, independent of chart range)
  const putPeak = chart.bestScores.bestPutScore;
  const callPeak = chart.bestScores.bestCallScore;

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
      momentumVal?: number;
      bestPut?: number | null;
      bestCall?: number | null;
      bestPutDetail?: OptionDetail;
      bestCallDetail?: OptionDetail;
      liquidity?: number;
      trade?: number;
      tradeInfo?: string;
    };

    // Build rows from price data (primary time axis)
    const rows: Row[] = chart.prices.map(p => {
      const m = p.medium_momentum_main || 'neutral';
      return {
        ts: new Date(p.timestamp).getTime(),
        price: p.price,
        momentum: m,
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
    const bestPutByTs = new Map<string, HeatmapSnapshot>();
    const bestCallByTs = new Map<string, HeatmapSnapshot>();
    for (const snap of chart.optionsHeatmap) {
      const isPut = snap.option_type === 'P' || snap.option_type === 'put' || snap.instrument_name?.includes('-P');
      const isCall = snap.option_type === 'C' || snap.option_type === 'call' || snap.instrument_name?.includes('-C');
      if (isPut && (snap.ask_delta_value ?? 0) > 0) {
        const prev = bestPutByTs.get(snap.timestamp);
        if (!prev || (snap.ask_delta_value ?? 0) > (prev.ask_delta_value ?? 0)) {
          bestPutByTs.set(snap.timestamp, snap);
        }
      }
      if (isCall && (snap.bid_delta_value ?? 0) > 0) {
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

    // Snap liquidity data to nearest price point
    for (const l of chart.liquidity) {
      const idx = snapToNearest(new Date(l.timestamp).getTime());
      rows[idx].liquidity = l.signed_liquidity;
    }

    // Snap trade markers to nearest price point
    for (const t of chart.trades) {
      const idx = snapToNearest(new Date(t.timestamp).getTime());
      rows[idx].trade = rows[idx].price;
      rows[idx].tradeInfo = `${t.direction === 'buy' ? 'Bought' : 'Sold'} ${t.instrument_name} @ $${t.price.toFixed(2)}`;
    }

    return rows;
  }, [chart]);

  const tradePoints = useMemo(() =>
    merged.filter(d => d.trade != null).map(d => ({ ts: d.ts, trade: d.trade })),
    [merged]
  );

  // Data for momentum bar (only points with momentum data)
  const momentumData = useMemo(() =>
    merged.filter(d => d.momentum !== undefined),
    [merged]
  );

  // Data for liquidity bar (only points with liquidity data)
  const liquidityData = useMemo(() =>
    merged.filter(d => d.liquidity != null),
    [merged]
  );

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

      const dot: HeatmapDot = {
        ts,
        pctOtm: +pctOtm.toFixed(2),
        premium,
        strike: snap.strike,
        delta: snap.delta,
        bid: snap.bid_price,
        ask: snap.ask_price,
        intensity: 0, // normalized below
      };

      if (isCall) calls.push(dot);
      else puts.push(dot);
    }

    // Normalize intensity within each set
    const normalize = (dots: HeatmapDot[]) => {
      if (dots.length === 0) return;
      const premiums = dots.map(d => d.premium);
      const min = Math.min(...premiums);
      const max = Math.max(...premiums);
      const range = max - min || 1;
      for (const d of dots) {
        d.intensity = (d.premium - min) / range;
      }
    };
    normalize(calls);
    normalize(puts);

    return { callHeatmap: calls, putHeatmap: puts };
  }, [chart.optionsHeatmap, merged]);

  // Data for options value sub-chart (only points with options data)
  const optionsData = useMemo(() =>
    merged.filter(d => (d.bestPut != null && d.bestPut > 0) || (d.bestCall != null && d.bestCall > 0)),
    [merged]
  );

  // Table data: latest first, only rows with a price
  const tableData = useMemo(() =>
    [...merged].reverse().filter(d => d.price != null),
    [merged]
  );

  const handleTableScroll = useCallback(() => {
    const el = tableRef.current;
    if (!el) return;
    if (el.scrollTop + el.clientHeight >= el.scrollHeight - 50) {
      setVisibleCount(prev => Math.min(prev + PAGE_SIZE, tableData.length));
    }
  }, [tableData.length]);

  // Shared X-axis props for sub-charts (synced with main chart)
  const xDomain = merged.length > 0
    ? [merged[0].ts, merged[merged.length - 1].ts]
    : [0, 1];

  return (
    <div className="space-y-6">
      {/* Price + Momentum + Range Header */}
      <div className="flex flex-wrap items-start gap-4">
        <Card className="flex-1 min-w-[200px]">
          <div className="text-3xl font-bold tracking-tight text-juice-orange">{formatUSD(stats.last_price)}</div>
          <div className="text-xs text-gray-500 mt-1">ETH spot {timeAgo(stats.last_price_time)}</div>
        </Card>

        <Card title="Momentum" className="flex-1 min-w-[200px]">
          <div className="flex gap-2 flex-wrap">
            <div className={`rounded px-3 py-1.5 ${momentumBg(stats.medium_momentum)}`}>
              <div className="text-xs text-gray-500">Medium</div>
              <div className={`text-sm font-medium ${momentumColor(stats.medium_momentum)}`}>
                {stats.medium_momentum || 'neutral'}
                {stats.medium_derivative && <span className="text-xs ml-1 opacity-70">({stats.medium_derivative})</span>}
              </div>
            </div>
            <div className={`rounded px-3 py-1.5 ${momentumBg(stats.short_momentum)}`}>
              <div className="text-xs text-gray-500">Short</div>
              <div className={`text-sm font-medium ${momentumColor(stats.short_momentum)}`}>
                {stats.short_momentum || 'neutral'}
                {stats.short_derivative && <span className="text-xs ml-1 opacity-70">({stats.short_derivative})</span>}
              </div>
            </div>
          </div>
        </Card>

        <Card title="Price Range" className="flex-1 min-w-[200px]">
          <div className="grid grid-cols-2 gap-2 text-sm">
            <div>
              <span className="text-gray-500">3d H/L:</span>{' '}
              <span className="text-emerald-400">{formatUSD(stats.three_day_high)}</span>{' / '}
              <span className="text-red-400">{formatUSD(stats.three_day_low)}</span>
            </div>
            <div>
              <span className="text-gray-500">7d H/L:</span>{' '}
              <span className="text-emerald-400">{formatUSD(stats.seven_day_high)}</span>{' / '}
              <span className="text-red-400">{formatUSD(stats.seven_day_low)}</span>
            </div>
          </div>
        </Card>

        <Card title="Budget" className="flex-1 min-w-[200px]">
          <div className="grid grid-cols-2 gap-2 text-sm">
            <div>
              <span className="text-gray-500">PUT:</span>{' '}
              <span className="text-white">{formatUSD(stats.budget.putRemaining)}</span>
              <span className="text-gray-500 text-xs ml-1">/ {formatUSD(stats.budget.putTotalBudget)}</span>
            </div>
            <div>
              <span className="text-gray-500">CALL:</span>{' '}
              <span className="text-white">{formatUSD(stats.budget.callRemaining)}</span>
              <span className="text-gray-500 text-xs ml-1">/ {formatUSD(stats.budget.callTotalBudget)}</span>
            </div>
            <div className="col-span-2 text-xs text-gray-500">
              {stats.budget.putDaysLeft > 0 ? `${stats.budget.putDaysLeft}d left` : 'cycle ended'} in {stats.budget.cycleDays}d cycle
            </div>
          </div>
        </Card>
      </div>

      {/* Time Range Selector */}
      <div className="flex items-center justify-between">
        <div className="flex gap-1">
          {ranges.map(r => (
            <button
              key={r}
              onClick={() => { setRange(r); setVisibleCount(PAGE_SIZE); }}
              className={`px-3 py-1 rounded text-sm transition-all duration-200 ${
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
          <span className="flex items-center gap-1"><span style={{ color: chartColors.trade }}>&#9733;</span> Trade</span>
        </div>
      </div>

      {/* Big Combined Chart */}
      <Card>
        {loading && merged.length === 0 ? (
          <div className="h-[500px] flex items-center justify-center text-gray-500">Loading...</div>
        ) : merged.length === 0 ? (
          <div className="h-[500px] flex items-center justify-center text-gray-500">No data yet — bot is collecting</div>
        ) : (
          <ResponsiveContainer width="100%" height={500}>
            <ComposedChart data={merged} margin={CHART_MARGINS} syncId="main">
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
                width={70}
              />
              <Tooltip
                {...chartTooltip}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                labelFormatter={(ts: any) => new Date(ts as number).toLocaleString()}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                formatter={(val: any, name: any) => {
                  if (name === 'price') return [formatUSD(Number(val)), 'ETH'];
                  return [val, name];
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

              {/* ETH price line */}
              <Line yAxisId="price" type="monotone" dataKey="price" stroke={chartColors.primary} dot={false} strokeWidth={2} connectNulls />
              {/* Trade markers (stars) */}
              {tradePoints.length > 0 && (
                <Scatter yAxisId="price" data={tradePoints} dataKey="trade" shape={<StarDot />} />
              )}
            </ComposedChart>
          </ResponsiveContainer>
        )}
      </Card>

      {/* Options Value Sub-Chart */}
      {optionsData.length > 0 && (
        <Card>
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs font-medium text-gray-400">Options Value</span>
            <div className="flex gap-3 text-xs text-gray-500">
              <span className="flex items-center gap-1"><span className="w-3 h-0.5 inline-block" style={{ background: chartColors.red }} /> PUT (delta/ask)</span>
              <span className="flex items-center gap-1"><span className="w-3 h-0.5 inline-block" style={{ background: chartColors.secondary }} /> CALL (bid/delta)</span>
              {putPeak > 0 && <span className="flex items-center gap-1"><span className="w-3 h-0.5 inline-block border-dashed border-t" style={{ borderColor: chartColors.red }} /> threshold {putPeak.toFixed(4)}</span>}
              {callPeak > 0 && <span className="flex items-center gap-1"><span className="w-3 h-0.5 inline-block border-dashed border-t" style={{ borderColor: chartColors.secondary }} /> threshold {callPeak.toFixed(1)}</span>}
            </div>
          </div>
          <ResponsiveContainer width="100%" height={160}>
            <ComposedChart data={optionsData} margin={CHART_MARGINS} syncId="main">
              <XAxis dataKey="ts" type="number" domain={xDomain} tickFormatter={xTickFormatter} stroke={chartAxis.stroke} tick={chartAxis.tick} />
              <YAxis
                yAxisId="putVal"
                domain={['auto', 'auto']}
                tickFormatter={(v) => Number(v).toFixed(6)}
                stroke={chartAxis.stroke}
                tick={{ fill: chartColors.red, fontSize: 10 }}
                width={70}
              />
              <YAxis
                yAxisId="callVal"
                orientation="right"
                domain={['auto', 'auto']}
                tickFormatter={(v) => Number(v).toFixed(0)}
                stroke={chartAxis.stroke}
                tick={{ fill: chartColors.secondary, fontSize: 10 }}
                width={50}
              />
              <Tooltip
                {...chartTooltip}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                content={({ active, payload }: any) => {
                  if (!active || !payload?.length) return null;
                  const row = payload[0]?.payload;
                  if (!row) return null;
                  const bestPut = row.bestPut;
                  const bestCall = row.bestCall;
                  const fmtPut = bestPut != null && bestPut > 0 ? bestPut.toFixed(6) : 'N/A';
                  const fmtCall = bestCall != null && bestCall > 0 ? bestCall.toFixed(2) : 'N/A';
                  const fmtPutPeak = putPeak > 0 ? putPeak.toFixed(4) : 'N/A';
                  const fmtCallPeak = callPeak > 0 ? callPeak.toFixed(1) : 'N/A';
                  return (
                    <div style={{ ...chartTooltip.contentStyle, padding: '8px 12px' }}>
                      <div className="text-xs text-gray-400 mb-1">{new Date(row.ts).toLocaleString()}</div>
                      <div className="text-sm" style={{ color: chartColors.red }}>PUT Value: {fmtPut}</div>
                      <div className="text-sm" style={{ color: chartColors.secondary }}>CALL Value: {fmtCall}</div>
                      <div className="text-xs text-gray-500 mt-1 border-t border-white/10 pt-1">
                        <span style={{ color: chartColors.red }}>Best PUT ({chart.bestScores.windowDays}d): {fmtPutPeak}</span>
                        {' / '}
                        <span style={{ color: chartColors.secondary }}>Best CALL: {fmtCallPeak}</span>
                      </div>
                    </div>
                  );
                }}
              />
              {putPeak > 0 && (
                <ReferenceLine yAxisId="putVal" y={putPeak} stroke={chartColors.red} strokeDasharray="4 4" strokeOpacity={0.4} />
              )}
              {callPeak > 0 && (
                <ReferenceLine yAxisId="callVal" y={callPeak} stroke={chartColors.secondary} strokeDasharray="4 4" strokeOpacity={0.4} />
              )}
              <Line yAxisId="putVal" type="stepAfter" dataKey="bestPut" stroke={chartColors.red} strokeWidth={1.5} dot={{ r: 2, fill: chartColors.red, strokeWidth: 0 }} connectNulls={false} isAnimationActive={false} />
              <Line yAxisId="callVal" type="stepAfter" dataKey="bestCall" stroke={chartColors.secondary} strokeWidth={1.5} dot={{ r: 2, fill: chartColors.secondary, strokeWidth: 0 }} connectNulls={false} isAnimationActive={false} />
            </ComposedChart>
          </ResponsiveContainer>
        </Card>
      )}

      {/* Momentum Bar */}
      {momentumData.length > 0 && (
        <Card>
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs font-medium text-gray-400">Momentum</span>
            <div className="flex gap-3 text-xs text-gray-500">
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: '#10b981' }} /> upward</span>
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: '#ef4444' }} /> downward</span>
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block border border-white/10" style={{ background: 'rgba(255,255,255,0.05)' }} /> neutral</span>
            </div>
          </div>
          <ResponsiveContainer width="100%" height={48}>
            <BarChart data={momentumData} margin={CHART_MARGINS} syncId="main">
              <XAxis dataKey="ts" type="number" domain={xDomain} hide />
              <YAxis domain={[0, 1]} hide />
              <Tooltip
                {...chartTooltip}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                labelFormatter={(ts: any) => new Date(ts as number).toLocaleString()}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                formatter={(_v: any, _n: any, props: any) => {
                  const m = props.payload?.momentum || 'neutral';
                  return [m, 'Momentum'];
                }}
              />
              <Bar dataKey="momentumVal" isAnimationActive={false} maxBarSize={4}>
                {momentumData.map((d, i) => (
                  <Cell key={i} fill={momentumBarColor(d.momentum)} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </Card>
      )}

      {/* Liquidity Flow */}
      {liquidityData.length > 0 && (
        <Card>
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs font-medium text-gray-400">Liquidity Flow</span>
            <div className="flex gap-3 text-xs text-gray-500">
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: chartColors.blue }} /> inflow</span>
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: '#ef4444' }} /> outflow</span>
            </div>
          </div>
          <ResponsiveContainer width="100%" height={64}>
            <ComposedChart data={liquidityData} margin={CHART_MARGINS} syncId="main">
              <XAxis dataKey="ts" type="number" domain={xDomain} hide />
              <YAxis hide />
              <ReferenceLine y={0} stroke="rgba(255,255,255,0.1)" />
              <Tooltip
                {...chartTooltip}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                labelFormatter={(ts: any) => new Date(ts as number).toLocaleString()}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                formatter={(val: any) => [Number(val).toFixed(2), 'Liquidity']}
              />
              <Area type="stepAfter" dataKey="liquidity" stroke={chartColors.blue} strokeWidth={1} fill={chartColors.blue} fillOpacity={0.15} connectNulls dot={{ r: 2, strokeWidth: 0, fill: chartColors.blue }} />
            </ComposedChart>
          </ResponsiveContainer>
        </Card>
      )}

      {/* Call Market Heatmap */}
      {callHeatmap.length > 0 && (
        <Card>
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs font-medium text-gray-400">Call Market</span>
            <div className="flex gap-3 text-xs text-gray-500">
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: lerpColor(callColorDim, callColorBright, 0.2) }} /> cheap</span>
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: lerpColor(callColorDim, callColorBright, 0.8) }} /> rich</span>
              <span className="text-gray-600">bid premium</span>
            </div>
          </div>
          <ResponsiveContainer width="100%" height={200}>
            <ScatterChart margin={CHART_MARGINS} syncId="main">
              <XAxis
                dataKey="ts"
                type="number"
                domain={xDomain}
                tickFormatter={xTickFormatter}
                stroke={chartAxis.stroke}
                tick={chartAxis.tick}
              />
              <YAxis
                dataKey="pctOtm"
                name="% OTM"
                domain={[0, 'auto']}
                tickFormatter={(v) => `${v}%`}
                stroke={chartAxis.stroke}
                tick={chartAxis.tick}
                width={55}
              />
              <Tooltip
                {...chartTooltip}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                content={({ active, payload }: any) => {
                  if (!active || !payload?.[0]?.payload) return null;
                  const d = payload[0].payload as HeatmapDot;
                  return (
                    <div style={{ ...chartTooltip.contentStyle, padding: '8px 12px' }}>
                      <div className="text-xs text-gray-400">{new Date(d.ts).toLocaleString()}</div>
                      <div className="text-sm">Strike: <span className="text-white font-medium">${d.strike.toFixed(0)}</span></div>
                      <div className="text-sm">% OTM: <span className="text-cyan-300">{d.pctOtm.toFixed(1)}%</span></div>
                      {d.delta != null && <div className="text-sm">Delta: <span className="text-gray-300">{d.delta.toFixed(3)}</span></div>}
                      <div className="text-sm">Bid: <span className="text-cyan-300">{d.bid?.toFixed(4) ?? 'N/A'}</span></div>
                      <div className="text-sm">Ask: <span className="text-gray-300">{d.ask?.toFixed(4) ?? 'N/A'}</span></div>
                    </div>
                  );
                }}
              />
              {/* Faint band showing bot's active delta trading range (~2-12% OTM as rough equivalent) */}
              <ReferenceArea y1={2} y2={12} fill="#5CEBDF" fillOpacity={0.04} />
              <Scatter
                data={callHeatmap}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                shape={(props: any) => <HeatmapDotShape {...props} type="call" />}
                isAnimationActive={false}
              />
            </ScatterChart>
          </ResponsiveContainer>
        </Card>
      )}

      {/* Put Market Heatmap */}
      {putHeatmap.length > 0 && (
        <Card>
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs font-medium text-gray-400">Put Market</span>
            <div className="flex gap-3 text-xs text-gray-500">
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: lerpColor(putColorDim, putColorBright, 0.2) }} /> cheap</span>
              <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm inline-block" style={{ background: lerpColor(putColorDim, putColorBright, 0.8) }} /> rich</span>
              <span className="text-gray-600">ask premium</span>
            </div>
          </div>
          <ResponsiveContainer width="100%" height={200}>
            <ScatterChart margin={CHART_MARGINS} syncId="main">
              <XAxis
                dataKey="ts"
                type="number"
                domain={xDomain}
                tickFormatter={xTickFormatter}
                stroke={chartAxis.stroke}
                tick={chartAxis.tick}
              />
              <YAxis
                dataKey="pctOtm"
                name="% OTM"
                domain={[0, 'auto']}
                tickFormatter={(v) => `${v}%`}
                stroke={chartAxis.stroke}
                tick={chartAxis.tick}
                width={55}
              />
              <Tooltip
                {...chartTooltip}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                content={({ active, payload }: any) => {
                  if (!active || !payload?.[0]?.payload) return null;
                  const d = payload[0].payload as HeatmapDot;
                  return (
                    <div style={{ ...chartTooltip.contentStyle, padding: '8px 12px' }}>
                      <div className="text-xs text-gray-400">{new Date(d.ts).toLocaleString()}</div>
                      <div className="text-sm">Strike: <span className="text-white font-medium">${d.strike.toFixed(0)}</span></div>
                      <div className="text-sm">% OTM: <span className="text-red-300">{d.pctOtm.toFixed(1)}%</span></div>
                      {d.delta != null && <div className="text-sm">Delta: <span className="text-gray-300">{d.delta.toFixed(3)}</span></div>}
                      <div className="text-sm">Bid: <span className="text-gray-300">{d.bid?.toFixed(4) ?? 'N/A'}</span></div>
                      <div className="text-sm">Ask: <span className="text-red-300">{d.ask?.toFixed(4) ?? 'N/A'}</span></div>
                    </div>
                  );
                }}
              />
              {/* Faint band showing bot's active delta trading range (~2-12% OTM as rough equivalent) */}
              <ReferenceArea y1={2} y2={12} fill="#f87171" fillOpacity={0.04} />
              <Scatter
                data={putHeatmap}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                shape={(props: any) => <HeatmapDotShape {...props} type="put" />}
                isAnimationActive={false}
              />
            </ScatterChart>
          </ResponsiveContainer>
        </Card>
      )}

      {/* Data Table: spot price, best put, best call */}
      {tableData.length > 0 && (
        <Card>
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs font-medium text-gray-400">Price & Options History</span>
            <span className="text-xs text-gray-600">{tableData.length} entries</span>
          </div>
          <div
            ref={tableRef}
            onScroll={handleTableScroll}
            className="overflow-auto"
            style={{ maxHeight: 400 }}
          >
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-[#111] z-10">
                <tr className="text-xs text-gray-500 border-b border-white/5">
                  <th className="text-left py-2 px-3 font-medium">Time</th>
                  <th className="text-right py-2 px-3 font-medium">Spot Price</th>
                  <th className="text-right py-2 px-3 font-medium" style={{ color: chartColors.red }}>Best PUT Value</th>
                  <th className="text-right py-2 px-3 font-medium" style={{ color: chartColors.secondary }}>Best CALL Value</th>
                </tr>
              </thead>
              <tbody>
                {tableData.slice(0, visibleCount).map((d, i) => {
                  const putDetail = d.bestPutDetail;
                  const callDetail = d.bestCallDetail;
                  const putTitle = putDetail
                    ? `Delta: ${putDetail.delta?.toFixed(4) ?? 'N/A'}\nPrice: ${putDetail.price?.toFixed(6) ?? 'N/A'}\nStrike: $${putDetail.strike.toFixed(0)}\nDTE: ${putDetail.dte ?? 'N/A'}`
                    : '';
                  const callTitle = callDetail
                    ? `Delta: ${callDetail.delta?.toFixed(4) ?? 'N/A'}\nPrice: ${callDetail.price?.toFixed(6) ?? 'N/A'}\nStrike: $${callDetail.strike.toFixed(0)}\nDTE: ${callDetail.dte ?? 'N/A'}`
                    : '';
                  return (
                    <tr key={i} className="border-b border-white/[0.03] hover:bg-white/[0.02]">
                      <td className="py-1.5 px-3 text-gray-400 text-xs whitespace-nowrap">
                        {new Date(d.ts).toLocaleString()}
                      </td>
                      <td className="py-1.5 px-3 text-right text-juice-orange tabular-nums">
                        {formatUSD(d.price!)}
                      </td>
                      <td
                        className="py-1.5 px-3 text-right tabular-nums cursor-help"
                        style={{ color: d.bestPut && d.bestPut > 0 ? chartColors.red : '#444' }}
                        title={putTitle}
                      >
                        {d.bestPut === undefined ? '--' : d.bestPut && d.bestPut > 0 ? d.bestPut.toFixed(6) : 'N/A'}
                      </td>
                      <td
                        className="py-1.5 px-3 text-right tabular-nums cursor-help"
                        style={{ color: d.bestCall && d.bestCall > 0 ? chartColors.secondary : '#444' }}
                        title={callTitle}
                      >
                        {d.bestCall === undefined ? '--' : d.bestCall && d.bestCall > 0 ? d.bestCall.toFixed(2) : 'N/A'}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {visibleCount < tableData.length && (
              <div className="text-center py-3 text-xs text-gray-600">
                Showing {visibleCount} of {tableData.length} — scroll for more
              </div>
            )}
          </div>
        </Card>
      )}
    </div>
  );
}
