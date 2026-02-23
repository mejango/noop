'use client';

import { useState } from 'react';
import { usePolling } from '@/lib/hooks';
import { formatUSD, formatNum } from '@/lib/format';
import Card from '@/components/Card';
import Badge from '@/components/Badge';

interface Trade {
  id: number;
  position_id: number;
  instrument_name: string;
  direction: string;
  amount: number;
  price: number;
  total_value: number;
  fee: number | null;
  order_type: string;
  reason: string | null;
  timestamp: string;
  strike: number;
  expiry: number;
  position_direction: string;
  position_status: string;
}

const ranges = ['7d', '30d', '90d', 'all'] as const;

export default function TradesPage() {
  const [range, setRange] = useState<string>('30d');
  const { data: trades, loading } = usePolling<Trade[]>(`/api/trades?range=${range}&limit=500`, []);

  const downloadCSV = () => {
    const headers = ['timestamp', 'instrument', 'direction', 'amount', 'price', 'total_value', 'fee', 'order_type', 'reason'];
    const rows = trades.map(t =>
      [t.timestamp, t.instrument_name, t.direction, t.amount, t.price, t.total_value, t.fee || '', t.order_type, t.reason || ''].join(',')
    );
    const csv = [headers.join(','), ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `noop-c-trades-${range}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold text-juice-orange">Trade Log</h1>
        <div className="flex gap-4">
          <button
            onClick={downloadCSV}
            className="px-3 py-1 rounded text-sm bg-white/5 text-gray-300 border border-white/10 hover:bg-white/10 hover:text-white transition-all duration-200"
          >
            Export CSV
          </button>
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

      <Card>
        {loading && trades.length === 0 ? (
          <p className="text-gray-500 text-sm">Loading...</p>
        ) : trades.length === 0 ? (
          <p className="text-gray-500 text-sm">No trades found</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-gray-500 text-xs border-b border-white/10">
                  <th className="text-left py-2 px-2">Time</th>
                  <th className="text-left px-2">Instrument</th>
                  <th className="text-left px-2">Dir</th>
                  <th className="text-right px-2">Amt</th>
                  <th className="text-right px-2">Price</th>
                  <th className="text-right px-2">Value</th>
                  <th className="text-left px-2">Type</th>
                  <th className="text-left px-2">Reason</th>
                </tr>
              </thead>
              <tbody>
                {trades.map(t => (
                  <tr key={t.id} className="border-b border-white/5 hover:bg-white/5 transition-colors">
                    <td className="py-1.5 px-2 text-xs text-gray-400">{new Date(t.timestamp).toLocaleString()}</td>
                    <td className="px-2 text-xs">{t.instrument_name}</td>
                    <td className="px-2">
                      <Badge label={t.direction} color={t.direction === 'buy' ? 'green' : 'red'} />
                    </td>
                    <td className="text-right px-2">{formatNum(t.amount)}</td>
                    <td className="text-right px-2">{formatUSD(t.price)}</td>
                    <td className="text-right px-2">{formatUSD(t.total_value)}</td>
                    <td className="px-2">
                      <Badge label={t.order_type} />
                    </td>
                    <td className="px-2 text-gray-400 text-xs truncate max-w-[200px]">{t.reason || '--'}</td>
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
