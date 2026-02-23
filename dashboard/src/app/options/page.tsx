'use client';

import { useState, useMemo } from 'react';
import { usePolling } from '@/lib/hooks';
import { formatUSD, formatNum } from '@/lib/format';
import { chartColors, chartAxis, chartTooltip } from '@/lib/chart';
import Card from '@/components/Card';
import { ScatterChart, Scatter, XAxis, YAxis, Tooltip, ResponsiveContainer, ZAxis } from 'recharts';

interface OptionsSnapshot {
  id: number;
  timestamp: string;
  instrument_name: string;
  strike: number;
  expiry: number;
  option_type: string;
  delta: number;
  ask_price: number;
  bid_price: number;
  ask_delta_value: number;
  bid_delta_value: number;
}

const ranges = ['1h', '6h', '24h', '7d'] as const;

export default function OptionsPage() {
  const [range, setRange] = useState<string>('24h');
  const [optionType, setOptionType] = useState<'put' | 'call'>('put');
  const { data: snapshots, loading } = usePolling<OptionsSnapshot[]>(`/api/options?range=${range}&limit=1000`, []);

  // Get the most recent timestamp's worth of options
  const latestSnapshots = useMemo(() => {
    if (snapshots.length === 0) return [];
    const latest = snapshots[0]?.timestamp;
    if (!latest) return [];
    return snapshots.filter(s =>
      s.timestamp === latest &&
      s.option_type === optionType &&
      s.strike > 0
    );
  }, [snapshots, optionType]);

  // Scatter data: strike vs score
  const scatterData = latestSnapshots.map(s => ({
    strike: s.strike,
    score: optionType === 'put' ? (s.ask_delta_value || 0) : (s.bid_delta_value || 0),
    delta: s.delta,
    askPrice: s.ask_price,
    bidPrice: s.bid_price,
    name: s.instrument_name,
    expiry: s.expiry ? new Date(s.expiry * 1000).toLocaleDateString() : '--',
  })).filter(d => d.score > 0);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold text-juice-orange">Options Surface</h1>
        <div className="flex gap-4">
          <div className="flex gap-1">
            {(['put', 'call'] as const).map(t => (
              <button
                key={t}
                onClick={() => setOptionType(t)}
                className={`px-3 py-1 rounded text-sm capitalize transition-all duration-200 ${
                  optionType === t
                    ? 'bg-white/10 text-white border border-white/20'
                    : 'text-gray-400 hover:bg-white/10 hover:text-white'
                }`}
              >
                {t}s
              </button>
            ))}
          </div>
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
        </div>
      </div>

      <Card title={`${optionType === 'put' ? 'Put' : 'Call'} Score by Strike (latest snapshot)`}>
        {loading && snapshots.length === 0 ? (
          <div className="h-[400px] flex items-center justify-center text-gray-500">Loading...</div>
        ) : scatterData.length === 0 ? (
          <div className="h-[400px] flex items-center justify-center text-gray-500">No options data</div>
        ) : (
          <ResponsiveContainer width="100%" height={400}>
            <ScatterChart>
              <XAxis
                dataKey="strike"
                name="Strike"
                tickFormatter={(v) => `$${v}`}
                stroke={chartAxis.stroke}
                tick={chartAxis.tick}
              />
              <YAxis
                dataKey="score"
                name="Score"
                stroke={chartAxis.stroke}
                tick={chartAxis.tick}
              />
              <ZAxis dataKey="delta" range={[20, 200]} name="Delta" />
              <Tooltip
                {...chartTooltip}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                formatter={(val: any, name: any) => {
                  const v = Number(val) || 0;
                  if (name === 'Strike') return [formatUSD(v), name];
                  if (name === 'Score') return [formatNum(v, 6), name];
                  return [formatNum(v, 4), name];
                }}
                labelFormatter={() => ''}
              />
              <Scatter data={scatterData} fill={optionType === 'put' ? chartColors.red : chartColors.tertiary} />
            </ScatterChart>
          </ResponsiveContainer>
        )}
      </Card>

      {/* Table of latest options */}
      <Card title="Latest Options">
        {latestSnapshots.length === 0 ? (
          <p className="text-gray-500 text-sm">No data</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-gray-500 text-xs border-b border-white/10">
                  <th className="text-left py-2 px-2">Instrument</th>
                  <th className="text-right px-2">Strike</th>
                  <th className="text-right px-2">Expiry</th>
                  <th className="text-right px-2">Delta</th>
                  <th className="text-right px-2">Ask</th>
                  <th className="text-right px-2">Bid</th>
                  <th className="text-right px-2">Score</th>
                </tr>
              </thead>
              <tbody>
                {latestSnapshots.slice(0, 30).map(s => (
                  <tr key={s.id} className="border-b border-white/5 hover:bg-white/5 transition-colors">
                    <td className="py-1.5 px-2 text-xs">{s.instrument_name}</td>
                    <td className="text-right px-2">{formatUSD(s.strike)}</td>
                    <td className="text-right px-2 text-xs">{s.expiry ? new Date(s.expiry * 1000).toLocaleDateString() : '--'}</td>
                    <td className="text-right px-2">{formatNum(s.delta, 4)}</td>
                    <td className="text-right px-2">{formatUSD(s.ask_price)}</td>
                    <td className="text-right px-2">{formatUSD(s.bid_price)}</td>
                    <td className="text-right px-2">
                      {formatNum(optionType === 'put' ? s.ask_delta_value : s.bid_delta_value, 6)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
}
