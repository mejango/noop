'use client';

import { useState } from 'react';
import { usePolling } from '@/lib/hooks';
import { formatUSD, formatNum, dteDays, timeAgo } from '@/lib/format';
import Card from '@/components/Card';
import Badge from '@/components/Badge';

interface Position {
  id: number;
  instrument_name: string;
  direction: string;
  strike: number;
  expiry: number;
  amount: number;
  avg_price: number;
  total_cost: number;
  status: string;
  pnl: number | null;
  rolled_to_id: number | null;
  rolled_from_id: number | null;
  opened_at: string;
  closed_at: string | null;
  close_reason: string | null;
}

interface Trade {
  id: number;
  instrument_name: string;
  direction: string;
  amount: number;
  price: number;
  total_value: number;
  fee: number | null;
  order_type: string;
  reason: string | null;
  timestamp: string;
}

const statusColors: Record<string, string> = {
  open: 'blue',
  rolled: 'yellow',
  closed: 'green',
  expired: 'gray',
};

export default function PositionsPage() {
  const [filter, setFilter] = useState<string>('all');
  const { data: positions } = usePolling<Position[]>(
    filter === 'all' ? '/api/positions' : `/api/positions?status=${filter}`,
    []
  );
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [trades, setTrades] = useState<Trade[]>([]);

  const loadTrades = async (posId: number) => {
    if (expandedId === posId) { setExpandedId(null); return; }
    const res = await fetch(`/api/positions?id=${posId}`);
    const json = await res.json();
    setTrades(json.trades || []);
    setExpandedId(posId);
  };

  const filters = ['all', 'open', 'expired', 'closed', 'rolled'];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold">Positions</h1>
        <div className="flex gap-1">
          {filters.map(f => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`px-3 py-1 rounded text-sm capitalize ${filter === f ? 'bg-zinc-700 text-zinc-100' : 'text-zinc-400 hover:bg-zinc-800'}`}
            >
              {f}
            </button>
          ))}
        </div>
      </div>

      {positions.length === 0 ? (
        <Card>
          <p className="text-zinc-500 text-sm">No positions found</p>
        </Card>
      ) : (
        <div className="space-y-2">
          {positions.map(pos => {
            const dte = dteDays(pos.expiry);
            const isExpanded = expandedId === pos.id;
            return (
              <div key={pos.id}>
                <button
                  onClick={() => loadTrades(pos.id)}
                  className="w-full text-left px-4 py-3 rounded-lg bg-zinc-900/50 border border-zinc-800 hover:border-zinc-700 transition-colors"
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <Badge label={pos.status} color={statusColors[pos.status] || 'zinc'} />
                      <Badge label={pos.direction === 'buy' ? 'PUT' : 'CALL'} color={pos.direction === 'buy' ? 'red' : 'green'} />
                      <span className="font-mono text-sm">{pos.instrument_name}</span>
                    </div>
                    <div className="flex items-center gap-4 text-sm text-zinc-400">
                      <span>Strike: {formatUSD(pos.strike)}</span>
                      <span>Qty: {formatNum(pos.amount)}</span>
                      <span>Avg: {formatUSD(pos.avg_price)}</span>
                      <span>Cost: {formatUSD(pos.total_cost)}</span>
                      {dte !== null && pos.status === 'open' && (
                        <Badge label={`${dte}d`} color={dte <= 7 ? 'red' : dte <= 30 ? 'yellow' : 'blue'} />
                      )}
                      {pos.pnl !== null && (
                        <span className={pos.pnl >= 0 ? 'text-green-400' : 'text-red-400'}>
                          PnL: {formatUSD(pos.pnl)}
                        </span>
                      )}
                    </div>
                  </div>
                  <div className="flex items-center gap-4 mt-1 text-xs text-zinc-500">
                    <span>Opened {timeAgo(pos.opened_at)}</span>
                    {pos.closed_at && <span>Closed {timeAgo(pos.closed_at)}</span>}
                    {pos.close_reason && <span>Reason: {pos.close_reason}</span>}
                    {pos.rolled_to_id && <span>Rolled to #{pos.rolled_to_id}</span>}
                    {pos.rolled_from_id && <span>Rolled from #{pos.rolled_from_id}</span>}
                  </div>
                </button>

                {isExpanded && (
                  <div className="ml-8 mt-1 mb-2 space-y-1">
                    {trades.length === 0 ? (
                      <p className="text-zinc-500 text-xs py-2">No trades for this position</p>
                    ) : trades.map(t => (
                      <div key={t.id} className="flex items-center gap-3 text-xs px-3 py-1.5 rounded bg-zinc-800/50 border border-zinc-800">
                        <span className="text-zinc-500 w-24">{new Date(t.timestamp).toLocaleDateString()}</span>
                        <Badge label={t.order_type} color="zinc" />
                        <Badge label={t.direction} color={t.direction === 'buy' ? 'green' : 'red'} />
                        <span>Qty: {formatNum(t.amount)}</span>
                        <span>@ {formatUSD(t.price)}</span>
                        <span>= {formatUSD(t.total_value)}</span>
                        {t.fee && <span className="text-zinc-500">Fee: {formatUSD(t.fee)}</span>}
                        {t.reason && <span className="text-zinc-500 truncate">{t.reason}</span>}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
