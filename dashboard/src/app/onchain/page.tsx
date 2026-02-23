'use client';

import { useState } from 'react';
import { usePolling } from '@/lib/hooks';
import { formatNum } from '@/lib/format';
import Card from '@/components/Card';
import Badge from '@/components/Badge';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, BarChart, Bar } from 'recharts';

interface OnchainEntry {
  id: number;
  timestamp: string;
  spot_price: number;
  liquidity_flow_direction: string;
  liquidity_flow_magnitude: number;
  liquidity_flow_confidence: number;
  whale_count: number;
  whale_total_txns: number;
  exhaustion_score: number;
  exhaustion_alert_level: string;
}

const ranges = ['24h', '7d', '30d'] as const;

const alertColors: Record<string, string> = {
  HEALTHY: 'green',
  CAUTION: 'yellow',
  WARNING: 'yellow',
  CRITICAL: 'red',
};

export default function OnchainPage() {
  const [range, setRange] = useState<string>('7d');
  const { data: onchain } = usePolling<OnchainEntry[]>(`/api/onchain?range=${range}`, []);

  const sorted = [...onchain].sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());

  const flowData = sorted.map(d => ({
    time: new Date(d.timestamp).toLocaleString(),
    ts: new Date(d.timestamp).getTime(),
    magnitude: d.liquidity_flow_direction === 'outflow' ? -(d.liquidity_flow_magnitude || 0) : (d.liquidity_flow_magnitude || 0),
    confidence: d.liquidity_flow_confidence || 0,
    direction: d.liquidity_flow_direction,
  }));

  const whaleData = sorted.map(d => ({
    time: new Date(d.timestamp).toLocaleString(),
    ts: new Date(d.timestamp).getTime(),
    wallets: d.whale_count || 0,
    txns: d.whale_total_txns || 0,
  }));

  const exhaustionData = sorted.map(d => ({
    time: new Date(d.timestamp).toLocaleString(),
    ts: new Date(d.timestamp).getTime(),
    score: d.exhaustion_score || 0,
    alert: d.exhaustion_alert_level || 'HEALTHY',
  }));

  const latest = onchain.length > 0 ? onchain[0] : null;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold">On-Chain Analysis</h1>
        <div className="flex gap-1">
          {ranges.map(r => (
            <button
              key={r}
              onClick={() => setRange(r)}
              className={`px-3 py-1 rounded text-sm ${range === r ? 'bg-zinc-700 text-zinc-100' : 'text-zinc-400 hover:bg-zinc-800'}`}
            >
              {r}
            </button>
          ))}
        </div>
      </div>

      {/* Summary cards */}
      {latest && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Card>
            <div className="text-lg font-bold capitalize">{latest.liquidity_flow_direction || '--'}</div>
            <div className="text-xs text-zinc-500">Liquidity Flow</div>
          </Card>
          <Card>
            <div className="text-lg font-bold">{formatNum((latest.liquidity_flow_magnitude || 0) * 100, 1)}%</div>
            <div className="text-xs text-zinc-500">Flow Magnitude</div>
          </Card>
          <Card>
            <div className="text-lg font-bold">{latest.whale_count || 0}</div>
            <div className="text-xs text-zinc-500">Whale Wallets</div>
          </Card>
          <Card>
            <Badge label={latest.exhaustion_alert_level || 'HEALTHY'} color={alertColors[latest.exhaustion_alert_level] || 'green'} />
            <div className="text-xs text-zinc-500 mt-1">Alert Level</div>
          </Card>
        </div>
      )}

      {/* Liquidity Flow */}
      <Card title="Liquidity Flow">
        {flowData.length === 0 ? (
          <div className="h-[250px] flex items-center justify-center text-zinc-500">No data</div>
        ) : (
          <ResponsiveContainer width="100%" height={250}>
            <AreaChart data={flowData}>
              <XAxis dataKey="ts" type="number" domain={['dataMin', 'dataMax']}
                tickFormatter={(ts) => new Date(ts).toLocaleDateString([], { month: 'short', day: 'numeric' })}
                stroke="#52525b" tick={{ fill: '#71717a', fontSize: 11 }} />
              <YAxis stroke="#52525b" tick={{ fill: '#71717a', fontSize: 11 }}
                tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} />
              <Tooltip contentStyle={{ backgroundColor: '#18181b', border: '1px solid #3f3f46', borderRadius: 8, fontSize: 12 }}
                labelFormatter={(ts) => new Date(ts as number).toLocaleString()} />
              <Area type="monotone" dataKey="magnitude" stroke="#a78bfa" fill="#a78bfa" fillOpacity={0.2} />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </Card>

      {/* Whale Activity */}
      <Card title="Whale Activity">
        {whaleData.length === 0 ? (
          <div className="h-[200px] flex items-center justify-center text-zinc-500">No data</div>
        ) : (
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={whaleData}>
              <XAxis dataKey="ts" type="number" domain={['dataMin', 'dataMax']}
                tickFormatter={(ts) => new Date(ts).toLocaleDateString([], { month: 'short', day: 'numeric' })}
                stroke="#52525b" tick={{ fill: '#71717a', fontSize: 11 }} />
              <YAxis stroke="#52525b" tick={{ fill: '#71717a', fontSize: 11 }} />
              <Tooltip contentStyle={{ backgroundColor: '#18181b', border: '1px solid #3f3f46', borderRadius: 8, fontSize: 12 }}
                labelFormatter={(ts) => new Date(ts as number).toLocaleString()} />
              <Bar dataKey="wallets" fill="#facc15" name="Whale Wallets" />
            </BarChart>
          </ResponsiveContainer>
        )}
      </Card>

      {/* Exhaustion Score */}
      <Card title="Exhaustion Score">
        {exhaustionData.length === 0 ? (
          <div className="h-[200px] flex items-center justify-center text-zinc-500">No data</div>
        ) : (
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={exhaustionData}>
              <XAxis dataKey="ts" type="number" domain={['dataMin', 'dataMax']}
                tickFormatter={(ts) => new Date(ts).toLocaleDateString([], { month: 'short', day: 'numeric' })}
                stroke="#52525b" tick={{ fill: '#71717a', fontSize: 11 }} />
              <YAxis stroke="#52525b" tick={{ fill: '#71717a', fontSize: 11 }} />
              <Tooltip contentStyle={{ backgroundColor: '#18181b', border: '1px solid #3f3f46', borderRadius: 8, fontSize: 12 }}
                labelFormatter={(ts) => new Date(ts as number).toLocaleString()} />
              <Area type="monotone" dataKey="score" stroke="#f87171" fill="#f87171" fillOpacity={0.15} />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </Card>
    </div>
  );
}
