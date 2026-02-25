'use client';

import { useState } from 'react';
import { usePolling } from '@/lib/hooks';
import { formatUSD } from '@/lib/format';
import { chartColors, chartAxis, chartTooltip } from '@/lib/chart';
import Card from '@/components/Card';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';

interface SpotPrice {
  id: number;
  timestamp: string;
  price: number;
  short_momentum_main: string;
  short_momentum_derivative: string;
  medium_momentum_main: string;
  medium_momentum_derivative: string;
  three_day_high: number;
  three_day_low: number;
  seven_day_high: number;
  seven_day_low: number;
}

const ranges = ['1h', '6h', '24h', '3d', '7d', '30d'] as const;

export default function PricesPage() {
  const [range, setRange] = useState<string>('7d');
  const { data: prices, loading } = usePolling<SpotPrice[]>(`/api/prices?range=${range}`, []);

  const chartData = prices.map(p => ({
    time: new Date(p.timestamp).toLocaleString(),
    price: p.price,
    ts: new Date(p.timestamp).getTime(),
    momentum: p.medium_momentum_main,
  }));

  const latestPrice = prices.length > 0 ? prices[prices.length - 1] : null;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold text-juice-orange">Price & Momentum</h1>
        <div className="flex gap-1">
          {ranges.map(r => (
            <button
              key={r}
              onClick={() => setRange(r)}
              className={`px-3 py-1 rounded text-sm ${
                range === r
                  ? 'bg-white/10 text-white border border-white/20'
                  : 'text-gray-400 hover:bg-white/10 hover:text-white'
              }`}
            >
              {r}
            </button>
          ))}
        </div>
      </div>

      <Card>
        {loading && prices.length === 0 ? (
          <div className="h-[400px] flex items-center justify-center text-gray-500">Loading...</div>
        ) : prices.length === 0 ? (
          <div className="h-[400px] flex items-center justify-center text-gray-500">No price data yet</div>
        ) : (
          <ResponsiveContainer width="100%" height={400}>
            <LineChart data={chartData}>
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
              <YAxis
                domain={['auto', 'auto']}
                tickFormatter={(v) => `$${v}`}
                stroke={chartAxis.stroke}
                tick={chartAxis.tick}
                width={70}
              />
              <Tooltip
                {...chartTooltip}
                labelFormatter={(ts) => new Date(ts as number).toLocaleString()}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                formatter={(val: any) => [formatUSD(Number(val) || 0), 'ETH']}
              />
              <Line type="monotone" dataKey="price" stroke={chartColors.primary} dot={false} strokeWidth={1.5} />
              {latestPrice?.seven_day_high && (
                <ReferenceLine y={latestPrice.seven_day_high} stroke={chartColors.refHigh} strokeDasharray="3 3" label={{ value: `7d H: ${formatUSD(latestPrice.seven_day_high)}`, fill: chartColors.refHigh, fontSize: 10, position: 'right' }} />
              )}
              {latestPrice?.seven_day_low && (
                <ReferenceLine y={latestPrice.seven_day_low} stroke={chartColors.refLow} strokeDasharray="3 3" label={{ value: `7d L: ${formatUSD(latestPrice.seven_day_low)}`, fill: chartColors.refLow, fontSize: 10, position: 'right' }} />
              )}
            </LineChart>
          </ResponsiveContainer>
        )}
      </Card>

      {/* Momentum timeline */}
      {prices.length > 0 && (
        <Card title="Momentum Timeline">
          <div className="overflow-x-auto">
            <div className="flex gap-0.5 min-w-[600px]" style={{ height: 40 }}>
              {chartData.map((d, i) => (
                <div
                  key={i}
                  className={`flex-1 rounded-sm ${
                    d.momentum === 'upward' ? 'bg-emerald-800/50' :
                    d.momentum === 'downward' ? 'bg-red-800/50' : 'bg-white/5'
                  }`}
                  title={`${d.time}: ${d.momentum || 'neutral'}`}
                />
              ))}
            </div>
            <div className="flex justify-between text-xs text-gray-500 mt-1">
              <span>{chartData[0]?.time}</span>
              <span>{chartData[chartData.length - 1]?.time}</span>
            </div>
          </div>
        </Card>
      )}
    </div>
  );
}
