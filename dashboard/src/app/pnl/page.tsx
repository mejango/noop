'use client';

import Link from 'next/link';
import { useEffect, useMemo, useState } from 'react';
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  Line,
  ComposedChart,
  Bar,
} from 'recharts';
import { usePolling } from '@/lib/hooks';
import { formatUSD } from '@/lib/format';

type RangeKey = '7d' | '14d' | '30d' | '90d' | '180d' | '1y' | 'custom';

type Report = {
  meta: {
    range: string;
    from: string;
    to: string;
    generatedAt: string;
    snapshotCount: number;
    orderCount: number;
    hasBaseline: boolean;
    bucketMs: number;
  };
  summary: {
    openingValue: number;
    closingValue: number;
    portfolioChange: number;
    portfolioReturnPct: number;
    openingUnrealized: number;
    closingUnrealized: number;
    unrealizedChange: number;
    netTradeCashflow: number;
    putNetCashflow: number;
    callNetCashflow: number;
    openingSpot: number;
    closingSpot: number;
    spotChangePct: number;
    highWatermark: number;
    lowWatermark: number;
    maxDrawdown: number;
    maxDrawdownPct: number;
  };
  series: {
    portfolio: Array<{
      timestamp: string;
      ts: number;
      portfolioValue: number;
      unrealizedPnl: number;
      realizedTotal: number;
      spotPrice: number;
      usdcBalance: number;
      ethBalance: number;
    }>;
    buckets: Array<{
      timestamp: string;
      tradeCashflow: number;
      putCashflow: number;
      callCashflow: number;
      orderCount: number;
      endPortfolioValue: number | null;
      endUnrealizedPnl: number | null;
    }>;
  };
  actionBreakdown: Array<{
    action: string;
    count: number;
    grossValue: number;
    cashflow: number;
    filledAmount: number;
  }>;
  orders: Array<{
    id: number;
    timestamp: string;
    action: string;
    instrument_name: string | null;
    filled_amount: number | null;
    fill_price: number | null;
    total_value: number | null;
    spot_price: number | null;
    cashflow: number;
  }>;
};

const PRESET_MS: Record<Exclude<RangeKey, 'custom'>, number> = {
  '7d': 7 * 24 * 60 * 60 * 1000,
  '14d': 14 * 24 * 60 * 60 * 1000,
  '30d': 30 * 24 * 60 * 60 * 1000,
  '90d': 90 * 24 * 60 * 60 * 1000,
  '180d': 180 * 24 * 60 * 60 * 1000,
  '1y': 365 * 24 * 60 * 60 * 1000,
};

const emptyReport: Report = {
  meta: {
    range: '30d',
    from: '',
    to: '',
    generatedAt: '',
    snapshotCount: 0,
    orderCount: 0,
    hasBaseline: false,
    bucketMs: 0,
  },
  summary: {
    openingValue: 0,
    closingValue: 0,
    portfolioChange: 0,
    portfolioReturnPct: 0,
    openingUnrealized: 0,
    closingUnrealized: 0,
    unrealizedChange: 0,
    netTradeCashflow: 0,
    putNetCashflow: 0,
    callNetCashflow: 0,
    openingSpot: 0,
    closingSpot: 0,
    spotChangePct: 0,
    highWatermark: 0,
    lowWatermark: 0,
    maxDrawdown: 0,
    maxDrawdownPct: 0,
  },
  series: { portfolio: [], buckets: [] },
  actionBreakdown: [],
  orders: [],
};

function toLocalInputValue(date: Date) {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function getPresetWindow(range: Exclude<RangeKey, 'custom'>) {
  const to = new Date();
  const from = new Date(to.getTime() - PRESET_MS[range]);
  return { from, to };
}

function fmtPct(n: number) {
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`;
}

function fmtSignedUsd(n: number) {
  return `${n >= 0 ? '+' : ''}${formatUSD(n)}`;
}

function dateLabel(ts: string, bucketMs = 0) {
  const d = new Date(ts);
  if (bucketMs > 0 && bucketMs <= 60 * 60 * 1000) {
    return d.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  }
  return d.toLocaleDateString([], { month: 'short', day: 'numeric' });
}

export default function PnlReportPage() {
  const [initialWindow] = useState(() => getPresetWindow('30d'));
  const [range, setRange] = useState<RangeKey>('30d');
  const [fromInput, setFromInput] = useState(toLocalInputValue(initialWindow.from));
  const [toInput, setToInput] = useState(toLocalInputValue(initialWindow.to));
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  const queryUrl = useMemo(() => {
    const from = new Date(fromInput);
    const to = new Date(toInput);
    const params = new URLSearchParams({
      range,
      from: Number.isNaN(from.getTime()) ? initialWindow.from.toISOString() : from.toISOString(),
      to: Number.isNaN(to.getTime()) ? initialWindow.to.toISOString() : to.toISOString(),
    });
    return `/api/pnl-report?${params.toString()}`;
  }, [fromInput, initialWindow.from, initialWindow.to, range, toInput]);

  const { data: report, loading, error } = usePolling<Report>(queryUrl, emptyReport, 60_000);

  const portfolioSeries = useMemo(() => report.series.portfolio.map((point) => ({
    ...point,
    label: dateLabel(point.timestamp, report.meta.bucketMs),
  })), [report]);

  const bucketSeries = useMemo(() => report.series.buckets.map((point) => ({
    ...point,
    label: dateLabel(point.timestamp, report.meta.bucketMs),
  })), [report]);

  const handlePreset = (next: Exclude<RangeKey, 'custom'>) => {
    const window = getPresetWindow(next);
    setRange(next);
    setFromInput(toLocalInputValue(window.from));
    setToInput(toLocalInputValue(window.to));
  };

  const summaryCards = [
    { label: 'Portfolio Change', value: fmtSignedUsd(report.summary.portfolioChange), tone: report.summary.portfolioChange >= 0 ? 'text-emerald-600' : 'text-red-600' },
    { label: 'Return', value: fmtPct(report.summary.portfolioReturnPct), tone: report.summary.portfolioReturnPct >= 0 ? 'text-emerald-600' : 'text-red-600' },
    { label: 'Trade Cashflow', value: fmtSignedUsd(report.summary.netTradeCashflow), tone: report.summary.netTradeCashflow >= 0 ? 'text-emerald-600' : 'text-red-600' },
    { label: 'Unrealized Change', value: fmtSignedUsd(report.summary.unrealizedChange), tone: report.summary.unrealizedChange >= 0 ? 'text-emerald-600' : 'text-red-600' },
    { label: 'Max Drawdown', value: `${fmtSignedUsd(-report.summary.maxDrawdown)} / ${report.summary.maxDrawdownPct.toFixed(2)}%`, tone: 'text-red-600' },
    { label: 'Spot Move', value: `${formatUSD(report.summary.openingSpot)} → ${formatUSD(report.summary.closingSpot)} (${fmtPct(report.summary.spotChangePct)})`, tone: 'text-zinc-900' },
  ];

  return (
    <div className="min-h-screen bg-[#f6f1e8] text-zinc-900 print:bg-white">
      <style jsx global>{`
        @media print {
          body { background: #fff !important; }
          @page { size: auto; margin: 14mm; }
        }
      `}</style>

      <div className="max-w-7xl mx-auto px-4 md:px-8 py-6 md:py-8 space-y-6">
        <div className="flex flex-col gap-4 print:hidden">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="text-xs uppercase tracking-[0.24em] text-amber-700">Noop Reporting</p>
              <h1 className="text-3xl md:text-4xl font-semibold tracking-tight">P&amp;L Sheet</h1>
              <p className="text-sm text-zinc-600 mt-1">Range-scoped portfolio change, trade cashflow, and risk path.</p>
            </div>
            <div className="flex items-center gap-2">
              <Link href="/" className="px-3 py-2 border border-zinc-300 text-sm hover:bg-white transition-colors">
                Back
              </Link>
              <button
                onClick={() => window.print()}
                className="px-3 py-2 bg-zinc-900 text-white text-sm hover:bg-zinc-700 transition-colors"
              >
                Export PDF
              </button>
            </div>
          </div>

          <div className="bg-white border border-black/10 p-4 flex flex-col gap-4">
            <div className="flex flex-wrap gap-2">
              {(['7d', '14d', '30d', '90d', '180d', '1y'] as const).map((preset) => (
                <button
                  key={preset}
                  onClick={() => handlePreset(preset)}
                  className={`px-3 py-1.5 text-sm border transition-colors ${
                    range === preset ? 'bg-zinc-900 text-white border-zinc-900' : 'bg-transparent border-zinc-300 hover:bg-zinc-50'
                  }`}
                >
                  {preset}
                </button>
              ))}
            </div>

            <div className="grid grid-cols-1 md:grid-cols-[1fr_1fr_auto] gap-3">
              <label className="text-sm text-zinc-700">
                From
                <input
                  type="datetime-local"
                  value={fromInput}
                  onChange={(e) => { setRange('custom'); setFromInput(e.target.value); }}
                  className="mt-1 w-full border border-zinc-300 bg-white px-3 py-2"
                />
              </label>
              <label className="text-sm text-zinc-700">
                To
                <input
                  type="datetime-local"
                  value={toInput}
                  onChange={(e) => { setRange('custom'); setToInput(e.target.value); }}
                  className="mt-1 w-full border border-zinc-300 bg-white px-3 py-2"
                />
              </label>
              <div className="text-sm text-zinc-600 self-end pb-2">
                {report.meta.snapshotCount} snapshots
                {' · '}
                {report.meta.orderCount} filled orders
              </div>
            </div>
          </div>
        </div>

        <div className="hidden print:block border-b border-black/10 pb-4">
          <p className="text-xs uppercase tracking-[0.24em] text-amber-700">Noop Reporting</p>
          <h1 className="text-3xl font-semibold tracking-tight">P&amp;L Sheet</h1>
          <p className="text-sm text-zinc-600 mt-1">
            {new Date(report.meta.from).toLocaleString()} to {new Date(report.meta.to).toLocaleString()}
          </p>
        </div>

        {error && (
          <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3">
            Failed to load report: {error}
          </div>
        )}

        <div className="bg-white border border-black/10 p-5">
          <div className="flex flex-wrap items-end justify-between gap-3 mb-4">
            <div>
              <p className="text-xs uppercase tracking-[0.24em] text-zinc-500">Range Summary</p>
              <h2 className="text-2xl font-semibold mt-1">
                {new Date(report.meta.from).toLocaleDateString()} → {new Date(report.meta.to).toLocaleDateString()}
              </h2>
            </div>
            <div className="text-sm text-zinc-600">
              Generated {report.meta.generatedAt ? new Date(report.meta.generatedAt).toLocaleString() : 'now'}
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
            {summaryCards.map((card) => (
              <div key={card.label} className="border border-zinc-200 p-4 bg-[#fcfaf6]">
                <p className="text-xs uppercase tracking-[0.18em] text-zinc-500">{card.label}</p>
                <p className={`text-xl font-semibold mt-2 ${card.tone}`}>{card.value}</p>
              </div>
            ))}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-3 text-sm">
            <div className="border border-zinc-200 p-4 bg-[#fcfaf6]">
              <p className="text-xs uppercase tracking-[0.18em] text-zinc-500 mb-2">Balances</p>
              <div className="space-y-1">
                <div className="flex justify-between"><span>Opening Value</span><span>{formatUSD(report.summary.openingValue)}</span></div>
                <div className="flex justify-between"><span>Closing Value</span><span>{formatUSD(report.summary.closingValue)}</span></div>
                <div className="flex justify-between"><span>High Watermark</span><span>{formatUSD(report.summary.highWatermark)}</span></div>
                <div className="flex justify-between"><span>Low Watermark</span><span>{formatUSD(report.summary.lowWatermark)}</span></div>
              </div>
            </div>
            <div className="border border-zinc-200 p-4 bg-[#fcfaf6]">
              <p className="text-xs uppercase tracking-[0.18em] text-zinc-500 mb-2">Leg Cashflow</p>
              <div className="space-y-1">
                <div className="flex justify-between"><span>Puts</span><span className={report.summary.putNetCashflow >= 0 ? 'text-emerald-600' : 'text-red-600'}>{fmtSignedUsd(report.summary.putNetCashflow)}</span></div>
                <div className="flex justify-between"><span>Calls</span><span className={report.summary.callNetCashflow >= 0 ? 'text-emerald-600' : 'text-red-600'}>{fmtSignedUsd(report.summary.callNetCashflow)}</span></div>
                <div className="flex justify-between"><span>Opening Unrealized</span><span>{formatUSD(report.summary.openingUnrealized)}</span></div>
                <div className="flex justify-between"><span>Closing Unrealized</span><span>{formatUSD(report.summary.closingUnrealized)}</span></div>
              </div>
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
          <div className="bg-white border border-black/10 p-5">
            <div className="mb-3">
              <p className="text-xs uppercase tracking-[0.24em] text-zinc-500">Equity Curve</p>
              <h3 className="text-xl font-semibold mt-1">Portfolio value vs. spot</h3>
            </div>
            <div className="h-[320px]">
              {mounted ? (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={portfolioSeries}>
                    <defs>
                      <linearGradient id="equityFill" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#d97706" stopOpacity={0.35} />
                        <stop offset="95%" stopColor="#d97706" stopOpacity={0.04} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid stroke="#e7e5e4" strokeDasharray="3 3" />
                    <XAxis dataKey="label" minTickGap={24} tick={{ fill: '#57534e', fontSize: 12 }} />
                    <YAxis yAxisId="equity" tickFormatter={(v) => `$${Math.round(v)}`} tick={{ fill: '#57534e', fontSize: 12 }} width={72} />
                    <YAxis yAxisId="spot" orientation="right" tickFormatter={(v) => `$${Math.round(v)}`} tick={{ fill: '#57534e', fontSize: 12 }} width={60} />
                    <Tooltip formatter={(value) => formatUSD(Number(value ?? 0))} />
                    <Legend />
                    <Area yAxisId="equity" type="monotone" dataKey="portfolioValue" name="Portfolio Value" stroke="#a16207" fill="url(#equityFill)" strokeWidth={2.5} />
                    <Line yAxisId="spot" type="monotone" dataKey="spotPrice" name="ETH Spot" stroke="#1d4ed8" dot={false} strokeWidth={1.75} />
                  </AreaChart>
                </ResponsiveContainer>
              ) : (
                <div className="h-full bg-[#fcfaf6] border border-zinc-200" />
              )}
            </div>
          </div>

          <div className="bg-white border border-black/10 p-5">
            <div className="mb-3">
              <p className="text-xs uppercase tracking-[0.24em] text-zinc-500">P&amp;L Components</p>
              <h3 className="text-xl font-semibold mt-1">Cashflow and unrealized path</h3>
            </div>
            <div className="h-[320px]">
              {mounted ? (
                <ResponsiveContainer width="100%" height="100%">
                  <ComposedChart data={bucketSeries}>
                    <CartesianGrid stroke="#e7e5e4" strokeDasharray="3 3" />
                    <XAxis dataKey="label" minTickGap={24} tick={{ fill: '#57534e', fontSize: 12 }} />
                    <YAxis yAxisId="cash" tickFormatter={(v) => `$${Math.round(v)}`} tick={{ fill: '#57534e', fontSize: 12 }} width={72} />
                    <YAxis yAxisId="u" orientation="right" tickFormatter={(v) => `$${Math.round(v)}`} tick={{ fill: '#57534e', fontSize: 12 }} width={70} />
                    <Tooltip formatter={(value) => formatUSD(Number(value ?? 0))} />
                    <Legend />
                    <Bar yAxisId="cash" dataKey="tradeCashflow" name="Trade Cashflow" fill="#0f766e" radius={[4, 4, 0, 0]} />
                    <Line yAxisId="u" type="monotone" dataKey="endUnrealizedPnl" name="Unrealized P&L" stroke="#dc2626" dot={false} strokeWidth={2} />
                  </ComposedChart>
                </ResponsiveContainer>
              ) : (
                <div className="h-full bg-[#fcfaf6] border border-zinc-200" />
              )}
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-[0.95fr_1.05fr] gap-6">
          <div className="bg-white border border-black/10 p-5">
            <div className="mb-3">
              <p className="text-xs uppercase tracking-[0.24em] text-zinc-500">Action Breakdown</p>
              <h3 className="text-xl font-semibold mt-1">Filled orders by action</h3>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-zinc-500 border-b border-zinc-200">
                    <th className="py-2 pr-3 font-medium">Action</th>
                    <th className="py-2 pr-3 font-medium text-right">Count</th>
                    <th className="py-2 pr-3 font-medium text-right">Gross</th>
                    <th className="py-2 pr-3 font-medium text-right">Cashflow</th>
                    <th className="py-2 font-medium text-right">Filled</th>
                  </tr>
                </thead>
                <tbody>
                  {report.actionBreakdown.map((row) => (
                    <tr key={row.action} className="border-b border-zinc-100">
                      <td className="py-2 pr-3 font-medium">{row.action}</td>
                      <td className="py-2 pr-3 text-right tabular-nums">{row.count}</td>
                      <td className="py-2 pr-3 text-right tabular-nums">{formatUSD(row.grossValue)}</td>
                      <td className={`py-2 pr-3 text-right tabular-nums ${row.cashflow >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>{fmtSignedUsd(row.cashflow)}</td>
                      <td className="py-2 text-right tabular-nums">{row.filledAmount.toFixed(2)}</td>
                    </tr>
                  ))}
                  {report.actionBreakdown.length === 0 && !loading && (
                    <tr>
                      <td colSpan={5} className="py-6 text-center text-zinc-500">No filled orders in this range.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          <div className="bg-white border border-black/10 p-5">
            <div className="mb-3">
              <p className="text-xs uppercase tracking-[0.24em] text-zinc-500">Execution Log</p>
              <h3 className="text-xl font-semibold mt-1">Recent filled trades</h3>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-zinc-500 border-b border-zinc-200">
                    <th className="py-2 pr-3 font-medium">Time</th>
                    <th className="py-2 pr-3 font-medium">Action</th>
                    <th className="py-2 pr-3 font-medium">Instrument</th>
                    <th className="py-2 pr-3 font-medium text-right">Fill</th>
                    <th className="py-2 font-medium text-right">Cashflow</th>
                  </tr>
                </thead>
                <tbody>
                  {report.orders.slice(0, 24).map((order) => (
                    <tr key={order.id} className="border-b border-zinc-100 align-top">
                      <td className="py-2 pr-3 whitespace-nowrap">{new Date(order.timestamp).toLocaleString()}</td>
                      <td className="py-2 pr-3 font-medium">{order.action}</td>
                      <td className="py-2 pr-3 text-zinc-600">{order.instrument_name || '—'}</td>
                      <td className="py-2 pr-3 text-right tabular-nums">
                        {order.filled_amount != null ? order.filled_amount.toFixed(2) : '--'}
                        {order.fill_price != null ? <span className="text-zinc-500"> @ {formatUSD(order.fill_price)}</span> : null}
                      </td>
                      <td className={`py-2 text-right tabular-nums ${order.cashflow >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>
                        {fmtSignedUsd(order.cashflow)}
                      </td>
                    </tr>
                  ))}
                  {report.orders.length === 0 && !loading && (
                    <tr>
                      <td colSpan={5} className="py-6 text-center text-zinc-500">No filled trades in this range.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
