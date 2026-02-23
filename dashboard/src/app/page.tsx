'use client';

import { usePolling } from '@/lib/hooks';
import { formatUSD, formatNum, timeAgo, dteDays, momentumColor, momentumBg } from '@/lib/format';
import Card from '@/components/Card';
import Badge from '@/components/Badge';

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
  opened_at: string;
  closed_at: string | null;
  close_reason: string | null;
}

interface Signal {
  id: number;
  timestamp: string;
  signal_type: string;
  details: string;
  acted_on: number;
}

const emptyStats: Stats = {
  open_puts: 0, open_calls: 0, total_positions: 0, total_trades: 0,
  last_price: 0, last_price_time: '', short_momentum: '', short_derivative: '',
  medium_momentum: '', medium_derivative: '', three_day_high: 0, three_day_low: 0,
  seven_day_high: 0, seven_day_low: 0, open_put_cost: 0, open_call_revenue: 0,
};

export default function OverviewPage() {
  const { data: stats } = usePolling<Stats>('/api/stats', emptyStats);
  const { data: positions } = usePolling<Position[]>('/api/positions?status=open', []);
  const { data: signals } = usePolling<Signal[]>('/api/signals?range=7d&limit=20', []);

  return (
    <div className="space-y-6">
      {/* Price + Momentum Header */}
      <div className="flex flex-wrap items-start gap-4">
        <Card className="flex-1 min-w-[200px]">
          <div className="text-3xl font-bold tracking-tight">{formatUSD(stats.last_price)}</div>
          <div className="text-xs text-zinc-500 mt-1">ETH spot {timeAgo(stats.last_price_time)}</div>
        </Card>

        <Card title="Momentum" className="flex-1 min-w-[200px]">
          <div className="flex gap-2 flex-wrap">
            <div className={`rounded px-3 py-1.5 ${momentumBg(stats.medium_momentum)}`}>
              <div className="text-xs text-zinc-500">Medium</div>
              <div className={`text-sm font-medium ${momentumColor(stats.medium_momentum)}`}>
                {stats.medium_momentum || 'neutral'}
                {stats.medium_derivative && <span className="text-xs ml-1 opacity-70">({stats.medium_derivative})</span>}
              </div>
            </div>
            <div className={`rounded px-3 py-1.5 ${momentumBg(stats.short_momentum)}`}>
              <div className="text-xs text-zinc-500">Short</div>
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
              <span className="text-zinc-500">3d H/L:</span>{' '}
              <span className="text-green-400">{formatUSD(stats.three_day_high)}</span>{' / '}
              <span className="text-red-400">{formatUSD(stats.three_day_low)}</span>
            </div>
            <div>
              <span className="text-zinc-500">7d H/L:</span>{' '}
              <span className="text-green-400">{formatUSD(stats.seven_day_high)}</span>{' / '}
              <span className="text-red-400">{formatUSD(stats.seven_day_low)}</span>
            </div>
          </div>
        </Card>
      </div>

      {/* Strategy Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <Card>
          <div className="text-2xl font-bold">{stats.open_puts}</div>
          <div className="text-xs text-zinc-500">Open Puts</div>
        </Card>
        <Card>
          <div className="text-2xl font-bold">{stats.open_calls}</div>
          <div className="text-xs text-zinc-500">Open Calls</div>
        </Card>
        <Card>
          <div className="text-2xl font-bold">{formatUSD(stats.open_put_cost)}</div>
          <div className="text-xs text-zinc-500">Put Cost Basis</div>
        </Card>
        <Card>
          <div className="text-2xl font-bold">{stats.total_trades}</div>
          <div className="text-xs text-zinc-500">Total Trades</div>
        </Card>
      </div>

      {/* Open Positions */}
      <Card title="Open Positions">
        {positions.length === 0 ? (
          <p className="text-zinc-500 text-sm">No open positions</p>
        ) : (
          <div className="space-y-2">
            {positions.map((pos) => {
              const dte = dteDays(pos.expiry);
              const dteColor = dte !== null && dte <= 7 ? 'red' : dte !== null && dte <= 30 ? 'yellow' : 'blue';
              return (
                <div key={pos.id} className="flex items-center justify-between px-3 py-2 rounded bg-zinc-800/50 border border-zinc-800">
                  <div className="flex items-center gap-3">
                    <Badge label={pos.direction === 'buy' ? 'PUT' : 'CALL'} color={pos.direction === 'buy' ? 'red' : 'green'} />
                    <span className="text-sm font-mono">{pos.instrument_name}</span>
                  </div>
                  <div className="flex items-center gap-4 text-sm">
                    <span className="text-zinc-400">Qty: {formatNum(pos.amount)}</span>
                    <span className="text-zinc-400">Avg: {formatUSD(pos.avg_price)}</span>
                    <span className="text-zinc-400">Cost: {formatUSD(pos.total_cost)}</span>
                    {dte !== null && <Badge label={`${dte}d DTE`} color={dteColor} />}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </Card>

      {/* Recent Signals */}
      <Card title="Recent Signals">
        {signals.length === 0 ? (
          <p className="text-zinc-500 text-sm">No signals yet</p>
        ) : (
          <div className="space-y-1">
            {signals.map((sig) => (
              <div key={sig.id} className="flex items-center gap-3 text-sm py-1">
                <span className="text-zinc-500 text-xs">{timeAgo(sig.timestamp)}</span>
                <Badge
                  label={sig.signal_type}
                  color={sig.signal_type.includes('crash') ? 'red' : sig.signal_type.includes('roll') ? 'yellow' : 'blue'}
                />
                <span className="text-zinc-300 truncate">{sig.details}</span>
                {sig.acted_on ? <Badge label="acted" color="green" /> : null}
              </div>
            ))}
          </div>
        )}
      </Card>
    </div>
  );
}
