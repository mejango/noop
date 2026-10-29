'use client';

import { useState } from 'react';
import { LineChart, Line, ResponsiveContainer, XAxis, YAxis, Tooltip } from 'recharts';
import Card from '@/components/Card';
import { usePolling } from '@/lib/hooks';
import { formatUSD } from '@/lib/format';
import { chartAxis, chartColors, chartTooltip } from '@/lib/chart';
import { ASSET_DEFAULT_RANGE, DASHBOARD_RANGES } from '@/lib/dashboard-ranges';
import type { AssetMarketSnapshot, AssetOptionQuote } from '@/lib/db';

type MarketResponse = {
  asset: string;
  range: string;
  ready: boolean;
  latest: AssetMarketSnapshot | null;
  history: AssetMarketSnapshot[];
  quotes: AssetOptionQuote[];
  observation_count: number;
};

function formatPercent(value: number | null | undefined, digits = 2) {
  return value == null ? '—' : `${(value * 100).toFixed(digits)}%`;
}

function formatNumber(value: number | null | undefined, digits = 0) {
  return value == null ? '—' : Number(value).toLocaleString('en-US', { maximumFractionDigits: digits });
}

function timeLabel(hour: string) {
  return new Date(hour).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', timeZone: 'UTC' });
}

function collectedLabel(timestamp: string) {
  return new Date(timestamp).toLocaleString('en-US', { year: 'numeric', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', second: '2-digit', timeZone: 'UTC' });
}

type ChartLine = { key: keyof AssetMarketSnapshot; label: string; color: string };

function HistoryChart({ title, rows, lines, format }: {
  title: string;
  rows: AssetMarketSnapshot[];
  lines: ChartLine[];
  format: (value: number) => string;
}) {
  const present = rows.some((row) => lines.some((line) => row[line.key] != null));
  return (
    <Card title={title}>
      {present ? (
        <div className="h-56" role="img" aria-label={`${title} history`}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={rows} margin={{ top: 8, right: 8, left: 0, bottom: 4 }}>
              <XAxis dataKey="hour" tickFormatter={timeLabel} minTickGap={38} stroke={chartAxis.stroke} tick={chartAxis.tick} />
              <YAxis width={65} tickFormatter={format} stroke={chartAxis.stroke} tick={chartAxis.tick} domain={['auto', 'auto']} />
              <Tooltip {...chartTooltip} labelFormatter={(label) => timeLabel(String(label))}
                formatter={(value, name) => [format(Number(value)), String(name)]} />
              {lines.map((line) => <Line key={line.key} type="monotone" dataKey={line.key} name={line.label}
                stroke={line.color} strokeWidth={2} dot={false} connectNulls={false} isAnimationActive={false} />)}
            </LineChart>
          </ResponsiveContainer>
        </div>
      ) : <p className="text-sm text-gray-500">No {title.toLowerCase()} observations in this range.</p>}
    </Card>
  );
}

export default function AssetMarketDashboard({ asset, initialData, initialError }: {
  asset: string; initialData: MarketResponse; initialError?: string;
}) {
  const [range, setRange] = useState(ASSET_DEFAULT_RANGE);
  const url = `/api/assets/${asset.toLowerCase()}?range=${range}`;
  const { data, dataUrl, error, loading } = usePolling<MarketResponse>(url, initialData, 60_000);
  const current = data.asset === asset && (dataUrl === url || (!dataUrl && range === initialData.range));
  const refreshError = error || (!dataUrl && initialError);
  const latest = current ? data.latest : null;
  const history = current ? data.history : [];
  const quotes = current ? data.quotes : [];

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-xl font-bold">{asset} market</h1>
          <p className="text-xs text-gray-500">Derive market observations, beginning when collection starts.</p>
        </div>
        <div className="flex flex-wrap gap-1" aria-label="History range">
          {DASHBOARD_RANGES.map((value) => <button key={value} type="button" onClick={() => setRange(value)}
            aria-pressed={range === value}
            className={`border px-2 py-1 text-xs ${range === value ? 'border-juice-orange text-juice-orange' : 'border-white/25 text-gray-400'}`}>{value}</button>)}
        </div>
      </div>

      {refreshError && <p role="alert" className="text-sm text-amber-300">{refreshError}</p>}
      {!current && <p role="status" className="text-sm text-gray-400">{loading ? 'Loading market history…' : 'Market history unavailable.'}</p>}
      {current && !data.ready && !refreshError && <p role="status" className="text-sm text-gray-400">Collection has not started yet.</p>}
      {current && data.ready && !latest && <p role="status" className="text-sm text-gray-400">No {asset} observations yet. History will appear after the first collection.</p>}

      {latest && (
        <>
          <p className="text-sm text-white">
            {formatNumber(data.observation_count)} {data.observation_count === 1 ? 'observation' : 'observations'} in the selected range. Last collected {collectedLabel(latest.collected_at)} UTC.
            {history.length < data.observation_count && ` Showing ${formatNumber(history.length)} chart points.`}
          </p>
          <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
            <Card title="Spot"><p className="text-xl text-white">{latest.spot == null ? '—' : formatUSD(latest.spot)}</p></Card>
            <Card title="Funding"><p className="text-xl text-white">{formatPercent(latest.funding_rate, 4)}</p></Card>
            <Card title="Open interest"><p className="text-sm text-white">P {formatNumber(latest.put_oi)} / C {formatNumber(latest.call_oi)}</p></Card>
            <Card title="Implied volatility"><p className="text-sm text-white">P {formatPercent(latest.put_iv)} / C {formatPercent(latest.call_iv)}</p></Card>
          </div>
          <p className="text-xs text-gray-400">
            {formatNumber(latest.quoted_count)} quoted options from {formatNumber(latest.option_count)} observed options. Quote status: {latest.quote_status}.
          </p>
          {history.length === 0 && <p className="text-sm text-gray-500">No observations in the selected range.</p>}
          <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
            <HistoryChart title="Spot" rows={history} lines={[{ key: 'spot', label: 'USD', color: chartColors.primary }]} format={formatUSD} />
            <HistoryChart title="Funding" rows={history} lines={[{ key: 'funding_rate', label: 'Rate', color: chartColors.secondary }]} format={(value) => formatPercent(value, 3)} />
            <HistoryChart title="Open interest" rows={history} lines={[{ key: 'put_oi', label: 'Puts', color: chartColors.red }, { key: 'call_oi', label: 'Calls', color: chartColors.blue }]} format={(value) => formatNumber(value)} />
            <HistoryChart title="Implied volatility" rows={history} lines={[{ key: 'put_iv', label: 'Puts', color: chartColors.red }, { key: 'call_iv', label: 'Calls', color: chartColors.blue }]} format={(value) => formatPercent(value)} />
          </div>
          <Card title="Latest option quotes" subtitle={`${quotes.length} shown, latest observation`}>
            {quotes.length ? <div className="overflow-x-auto"><table className="w-full text-xs text-left">
              <thead className="text-gray-400"><tr><th className="p-2">Instrument</th><th className="p-2 text-right">Bid</th><th className="p-2 text-right">Ask</th><th className="p-2 text-right">IV</th><th className="p-2 text-right">OI</th></tr></thead>
              <tbody>{quotes.map((quote) => <tr key={quote.instrument_name} className="border-t border-white/25">
                <td className="p-2">{quote.instrument_name}</td>
                <td className="p-2 text-right">{formatNumber(quote.bid, 4)}</td>
                <td className="p-2 text-right">{formatNumber(quote.ask, 4)}</td>
                <td className="p-2 text-right">{formatPercent(quote.implied_vol)}</td>
                <td className="p-2 text-right">{formatNumber(quote.open_interest)}</td>
              </tr>)}</tbody>
            </table></div> : <p className="text-sm text-gray-500">No option quotes captured in the latest observation.</p>}
          </Card>
        </>
      )}
    </div>
  );
}
