'use client';

import { useState, useMemo } from 'react';
import { usePolling } from '@/lib/hooks';
import { formatUSD, timeAgo, momentumColor, momentumBg } from '@/lib/format';
import { chartColors, chartAxis, chartTooltip } from '@/lib/chart';
import Card from '@/components/Card';
import {
  ComposedChart, Line, Area, Scatter, XAxis, YAxis, Tooltip,
  ResponsiveContainer, Legend, ReferenceLine,
} from 'recharts';

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
}

interface SpotPrice {
  timestamp: string;
  price: number;
  short_momentum: string;
  medium_momentum: string;
}

interface OptionsPoint {
  timestamp: string;
  best_put_price: number | null;
  best_call_price: number | null;
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

interface ChartData {
  prices: SpotPrice[];
  options: OptionsPoint[];
  liquidity: LiquidityPoint[];
  trades: TradeMarker[];
}

const emptyStats: Stats = {
  open_puts: 0, open_calls: 0, total_positions: 0, total_trades: 0,
  last_price: 0, last_price_time: '', short_momentum: '', short_derivative: '',
  medium_momentum: '', medium_derivative: '', three_day_high: 0, three_day_low: 0,
  seven_day_high: 0, seven_day_low: 0, open_put_cost: 0, open_call_revenue: 0,
};

const emptyChart: ChartData = { prices: [], options: [], liquidity: [], trades: [] };
const ranges = ['1h', '6h', '24h', '3d', '7d', '30d'] as const;

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

export default function OverviewPage() {
  const [range, setRange] = useState<string>('7d');
  const { data: stats } = usePolling<Stats>('/api/stats', emptyStats);
  const { data: chart, loading } = usePolling<ChartData>(`/api/chart?range=${range}`, emptyChart);

  // Merge all data series by timestamp into a single array for the chart
  const merged = useMemo(() => {
    const map = new Map<number, {
      ts: number;
      price?: number;
      momentum?: string;
      bestPut?: number;
      bestCall?: number;
      liquidity?: number;
      trade?: number;
      tradeInfo?: string;
    }>();

    // Add prices
    for (const p of chart.prices) {
      const ts = new Date(p.timestamp).getTime();
      map.set(ts, {
        ts,
        price: p.price,
        momentum: p.medium_momentum,
      });
    }

    // Merge best option premiums (actual dollar prices)
    for (const o of chart.options) {
      const ts = new Date(o.timestamp).getTime();
      const existing = map.get(ts) || { ts };
      if (o.best_put_price != null) existing.bestPut = o.best_put_price;
      if (o.best_call_price != null) existing.bestCall = o.best_call_price;
      map.set(ts, existing);
    }

    // Merge liquidity
    for (const l of chart.liquidity) {
      const ts = new Date(l.timestamp).getTime();
      const existing = map.get(ts) || { ts };
      existing.liquidity = l.signed_liquidity;
      map.set(ts, existing);
    }

    // Sort by timestamp
    const sorted = Array.from(map.values()).sort((a, b) => a.ts - b.ts);

    // Mark trades on nearest price point
    for (const t of chart.trades) {
      const tTs = new Date(t.timestamp).getTime();
      let closest = sorted[0];
      let minDiff = Infinity;
      for (const s of sorted) {
        const diff = Math.abs(s.ts - tTs);
        if (diff < minDiff) { minDiff = diff; closest = s; }
      }
      if (closest) {
        closest.trade = closest.price;
        closest.tradeInfo = `${t.direction === 'buy' ? 'Bought' : 'Sold'} ${t.instrument_name} @ $${t.price.toFixed(2)}`;
      }
    }

    return sorted;
  }, [chart]);

  // Separate trade points for the scatter
  const tradePoints = useMemo(() =>
    merged.filter(d => d.trade != null).map(d => ({ ts: d.ts, trade: d.trade })),
    [merged]
  );

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
      </div>

      {/* Time Range Selector */}
      <div className="flex items-center justify-between">
        <div className="flex gap-1">
          {ranges.map(r => (
            <button
              key={r}
              onClick={() => setRange(r)}
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
          <span className="flex items-center gap-1"><span className="w-3 h-0.5 inline-block" style={{ background: chartColors.red }} /> PUT Premium</span>
          <span className="flex items-center gap-1"><span className="w-3 h-0.5 inline-block" style={{ background: chartColors.secondary }} /> CALL Premium</span>
          <span className="flex items-center gap-1"><span className="w-3 h-0.5 inline-block" style={{ background: chartColors.blue, opacity: 0.5 }} /> Liquidity</span>
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
            <ComposedChart data={merged} margin={{ top: 10, right: 60, left: 10, bottom: 0 }}>
              <XAxis
                dataKey="ts"
                type="number"
                domain={['dataMin', 'dataMax']}
                tickFormatter={(ts) => {
                  const d = new Date(ts);
                  return range === '1h' || range === '6h'
                    ? d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                    : d.toLocaleDateString([], { month: 'short', day: 'numeric' });
                }}
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
              {/* Right Y-axis: option premiums in $ */}
              <YAxis
                yAxisId="premium"
                orientation="right"
                domain={['auto', 'auto']}
                tickFormatter={(v) => `$${v}`}
                stroke={chartAxis.stroke}
                tick={chartAxis.tickSecondary}
                width={55}
              />
              <Tooltip
                {...chartTooltip}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                labelFormatter={(ts: any) => new Date(ts as number).toLocaleString()}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                formatter={(val: any, name: any) => {
                  if (name === 'price') return [formatUSD(Number(val)), 'ETH'];
                  if (name === 'bestPut') return [formatUSD(Number(val)), 'Best PUT Premium'];
                  if (name === 'bestCall') return [formatUSD(Number(val)), 'Best CALL Premium'];
                  if (name === 'liquidity') return [Number(val).toFixed(2), 'Liquidity Flow'];
                  return [val, name];
                }}
              />
              <Legend content={() => null} />

              {/* 7d high/low reference lines */}
              {stats.seven_day_high > 0 && (
                <ReferenceLine
                  yAxisId="price"
                  y={stats.seven_day_high}
                  stroke={chartColors.refHigh}
                  strokeDasharray="3 3"
                  strokeOpacity={0.4}
                />
              )}
              {stats.seven_day_low > 0 && stats.seven_day_low < Infinity && (
                <ReferenceLine
                  yAxisId="price"
                  y={stats.seven_day_low}
                  stroke={chartColors.refLow}
                  strokeDasharray="3 3"
                  strokeOpacity={0.4}
                />
              )}

              {/* Liquidity flow area */}
              <Area
                yAxisId="premium"
                type="monotone"
                dataKey="liquidity"
                fill={chartColors.blue}
                fillOpacity={0.1}
                stroke={chartColors.blue}
                strokeWidth={0}
                connectNulls={false}
                dot={false}
              />

              {/* ETH price line */}
              <Line
                yAxisId="price"
                type="monotone"
                dataKey="price"
                stroke={chartColors.primary}
                dot={false}
                strokeWidth={2}
                connectNulls
              />

              {/* Best PUT score */}
              <Line
                yAxisId="premium"
                type="monotone"
                dataKey="bestPut"
                stroke={chartColors.red}
                dot={false}
                strokeWidth={1}
                strokeOpacity={0.7}
                connectNulls
              />

              {/* Best CALL score */}
              <Line
                yAxisId="premium"
                type="monotone"
                dataKey="bestCall"
                stroke={chartColors.secondary}
                dot={false}
                strokeWidth={1}
                strokeOpacity={0.7}
                connectNulls
              />

              {/* Trade markers (stars) */}
              {tradePoints.length > 0 && (
                <Scatter
                  yAxisId="price"
                  data={tradePoints}
                  dataKey="trade"
                  shape={<StarDot />}
                />
              )}
            </ComposedChart>
          </ResponsiveContainer>
        )}
      </Card>

      {/* Momentum Timeline */}
      {merged.length > 0 && (
        <Card title="Momentum Timeline">
          <div className="overflow-x-auto">
            <div className="flex gap-0.5 min-w-[600px]" style={{ height: 32 }}>
              {merged.filter(d => d.momentum !== undefined).map((d, i) => (
                <div
                  key={i}
                  className={`flex-1 rounded-sm ${
                    d.momentum === 'upward' ? 'bg-emerald-800/50' :
                    d.momentum === 'downward' ? 'bg-red-800/50' : 'bg-white/5'
                  }`}
                  title={`${new Date(d.ts).toLocaleString()}: ${d.momentum || 'neutral'}`}
                />
              ))}
            </div>
            <div className="flex justify-between text-xs text-gray-500 mt-1">
              <span>{merged.length > 0 ? new Date(merged[0].ts).toLocaleString() : ''}</span>
              <div className="flex gap-3">
                <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm bg-emerald-800/50 inline-block" /> upward</span>
                <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm bg-red-800/50 inline-block" /> downward</span>
                <span className="flex items-center gap-1"><span className="w-3 h-2 rounded-sm bg-white/5 inline-block border border-white/10" /> neutral</span>
              </div>
              <span>{merged.length > 0 ? new Date(merged[merged.length - 1].ts).toLocaleString() : ''}</span>
            </div>
          </div>
        </Card>
      )}
    </div>
  );
}
