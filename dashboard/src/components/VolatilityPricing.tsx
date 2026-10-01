'use client';

import { useEffect, useRef, useState } from 'react';
import { useLiveTimeAgo, usePolling } from '@/lib/hooks';
import { TENORS, OFFSETS, type VolatilityPricingData } from '@/lib/volatility-pricing';

const EMPTY: VolatilityPricingData = {
  asOf: '', score: null, label: '', qualifier: '', spot: null, cells: [],
  historyDays: 0, historyFrom: null, historyTo: null, provisional: false, measured: 0, total: 25,
};
const money = (v: number) => v.toLocaleString(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
const offsetLabel = (v: number) => v === 0 ? 'At spot' : `${v > 0 ? '+' : '−'}${Math.round(Math.abs(v) * 100)}%`;
const date = (v: string) => new Date(v).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' });

export default function VolatilityPricing() {
  const { data, error, loading, refetch } = usePolling('/api/volatility-pricing', EMPTY, 60_000);
  const age = useLiveTimeAgo(data.asOf);
  const [open, setOpen] = useState(false);
  const dialog = useRef<HTMLDialogElement>(null);
  const stale = !!data.asOf && Date.now() - Date.parse(data.asOf) > 5 * 60_000;
  const unavailable = !!error || stale;
  const score = unavailable ? null : data.score;
  const status = unavailable ? 'Quotes unavailable' : loading && !data.asOf ? 'Loading volatility…'
    : score != null ? data.label : data.historyDays < 7 ? 'Building history' : 'Limited quote coverage';
  const historyLabel = data.historyDays ? `Compared with ${data.historyDays} recorded days${data.provisional ? ' · Provisional' : ''}` : 'Waiting for full-chain history';

  useEffect(() => {
    if (open) dialog.current?.showModal();
    else dialog.current?.close();
  }, [open]);

  return (
    <div className="glass sm:col-span-2 overflow-hidden">
      <button type="button" onClick={() => setOpen(true)} aria-haspopup="dialog"
        className="w-full h-full text-left p-4 flex flex-col gap-2 hover:bg-white/[0.025] transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-juice-orange">
        <div className="flex items-center justify-between gap-3">
          <h3 className="text-sm font-semibold text-juice-orange">Volatility Pricing</h3>
          <span className="text-[10px] text-gray-500">View breakdown ↗</span>
        </div>
        <div className="flex items-baseline justify-between gap-3">
          <span className={`text-lg font-medium ${score != null && score <= 25 ? 'text-emerald-300' : score != null && score >= 75 ? 'text-amber-400' : 'text-gray-300'}`}>{status}</span>
          <span className="text-xl tabular-nums text-gray-200">{score ?? '—'}<span className="text-xs text-gray-500"> / 100</span></span>
        </div>
        <div className="relative h-2 rounded-full bg-gradient-to-r from-emerald-400/80 via-gray-600 to-amber-500/70"
          role={score != null ? 'meter' : undefined} aria-label="Volatility pricing: low means historically cheap"
          aria-valuemin={score != null ? 0 : undefined} aria-valuemax={score != null ? 100 : undefined}
          aria-valuenow={score ?? undefined} aria-valuetext={score != null ? `${status}, ${score} out of 100` : undefined}>
          {score != null && <span className="absolute top-1/2 w-1 h-4 rounded bg-white shadow -translate-x-1/2 -translate-y-1/2" style={{ left: `${score}%` }} />}
        </div>
        <div className="flex justify-between text-[10px] text-gray-500"><span>Cheap</span><span>Typical</span><span>Expensive</span></div>
        <div className="text-xs text-gray-500">{unavailable ? 'Refresh to get current options quotes' : historyLabel}</div>
        {score != null && <div className="text-[10px] text-gray-400">{data.qualifier} · {data.measured}/{data.total} cells · {age}</div>}
      </button>

      <dialog ref={dialog} onCancel={() => setOpen(false)} onClose={() => setOpen(false)}
        onClick={e => { if (e.target === e.currentTarget) setOpen(false); }}
        aria-labelledby="volatility-pricing-title"
        className="w-[min(850px,calc(100vw-32px))] max-h-[85vh] overflow-y-auto rounded-lg border border-gray-700 bg-[#181818] text-gray-200 p-0 backdrop:bg-black/75">
        <div className="p-5 md:p-6">
          <div className="flex justify-between items-start gap-4 mb-4">
            <div>
              <h2 id="volatility-pricing-title" className="text-lg text-juice-orange font-semibold">Volatility Pricing</h2>
              <p className="text-sm text-gray-400 mt-1">{status}{score != null ? ` · ${score} / 100` : ''}</p>
            </div>
            <button type="button" onClick={() => setOpen(false)} aria-label="Close volatility breakdown" className="px-3 py-1 rounded border border-gray-700 hover:bg-gray-800">✕</button>
          </div>
          {unavailable && <div role="alert" className="mb-4 flex items-center justify-between gap-4 text-sm text-amber-400">
            <span>Current quotes unavailable.{data.asOf ? ` The last snapshot is ${age}.` : ' No snapshot has loaded.'}</span>
            <button type="button" onClick={refetch} className="border border-gray-600 rounded px-3 py-1" disabled={loading}>{loading ? 'Refreshing…' : 'Retry'}</button>
          </div>}
          <p className="text-xs text-gray-400 mb-4">{historyLabel}. Brighter green means historically cheaper volatility. Each cell shows IV, then its pricing percentile (0 = cheapest, 100 = most expensive).</p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-separate border-spacing-1">
              <caption className="sr-only">Implied volatility and historical pricing percentile by strike distance and days to expiry</caption>
              <thead><tr><th className="text-left font-normal text-gray-500 p-2">Strike / spot</th>{TENORS.map(d => <th key={d} scope="col" className="font-normal text-gray-400 p-2 whitespace-nowrap">{d} DTE</th>)}</tr></thead>
              <tbody>{OFFSETS.map(offset => <tr key={offset}>
                <th scope="row" className="text-left font-normal p-2 whitespace-nowrap">
                  <div>{offsetLabel(offset)}</div>
                  {data.spot != null && <div className="text-[10px] text-gray-500 mt-1">{money(data.spot * (1 + offset))}</div>}
                </th>
                {TENORS.map(dte => {
                  const cell = data.cells.find(c => c.offset === offset && c.dte === dte);
                  const p = unavailable ? null : cell?.percentile;
                  return <td key={dte} className="rounded p-3 text-center min-w-[88px] tabular-nums"
                    style={{ backgroundColor: p != null ? `hsl(165 55% ${12 + (100 - p) * 0.25}%)` : '#252525' }}
                    title={cell ? `${cell.samples} daily observations. ${cell.iv == null ? 'No reliable quoted interpolation at this strike and maturity.' : p == null ? 'Historical percentile unavailable.' : 'Percentile vs comparable strike distance and maturity.'}` : 'No data yet'}>
                    <div className="text-sm">{!unavailable && cell?.iv != null ? `${cell.iv.toFixed(1)}%` : '—'}</div>
                    <div className="text-[10px] text-gray-300 mt-1">{p != null ? `${Math.round(p)} / 100` : 'Unrated'}</div>
                    <div className="text-[9px] text-gray-400 mt-1">{cell?.samples ?? 0} days</div>
                  </td>;
                })}
              </tr>)}</tbody>
            </table>
          </div>
          <div className="mt-5 space-y-2 text-xs text-gray-500 leading-relaxed">
            <p>Cheap means low relative to recorded history; it does not predict returns or establish fair value.</p>
            {data.historyFrom && data.historyTo && <p>History: {date(data.historyFrom)} – {date(data.historyTo)} (UTC), up to one year. {data.measured}/{data.total} cells rated.</p>}
            {data.asOf && <p>Quotes: {new Date(data.asOf).toLocaleString()} · {age}</p>}
            <details>
              <summary className="cursor-pointer text-gray-400">How the score works</summary>
              <div className="mt-2 space-y-2">
                <p>Equal weight across comparable cells; the meter is their median historical percentile. At least 13 of 25 cells across three strike levels and three maturities must be rated. Each cell needs seven recorded days; histories under 30 days are provisional.</p>
                <p>Strikes track distance from spot. DTE means days to expiry; values are interpolated between listed strikes and expiries, using quote-backed mark IV. Missing or wide quotes stay unrated. No extrapolation.</p>
              </div>
            </details>
          </div>
        </div>
      </dialog>
    </div>
  );
}
