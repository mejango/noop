'use client';

import { useEffect, useRef, useState } from 'react';
import { useLiveTimeAgo, usePolling } from '@/lib/hooks';
import { TENORS, OFFSETS, MIN_HISTORY_DAYS, pricingColor, type VolatilityPricingData } from '@/lib/volatility-pricing';

const EMPTY: VolatilityPricingData = {
  asOf: '', score: null, label: '', qualifier: '', spot: null, currentIv: null, cells: [],
  historyDays: 0, historyFrom: null, historyTo: null, provisional: false, measured: 0, total: 25,
};
const money = (v: number) => v.toLocaleString(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
const offsetLabel = (v: number) => v === 0 ? 'At spot' : `${v > 0 ? '+' : '−'}${Math.round(Math.abs(v) * 100)}%`;
const date = (v: string) => new Date(v).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' });

export default function VolatilityPricing() {
  const { data, error, loading, refetch } = usePolling('/api/volatility-pricing', EMPTY, 60_000);
  const age = useLiveTimeAgo(data.asOf);
  const [open, setOpen] = useState(false);
  const [selected, setSelected] = useState<{ offset: number; dte: number } | null>(null);
  const [copied, setCopied] = useState<string | null>(null);
  const dialog = useRef<HTMLDialogElement>(null);
  const contractPanel = useRef<HTMLElement>(null);
  const stale = !!data.asOf && Date.now() - Date.parse(data.asOf) > 5 * 60_000;
  const unavailable = !!error || stale;
  const score = unavailable ? null : data.score;
  const status = unavailable ? 'Quotes unavailable' : loading && !data.asOf ? 'Loading volatility…'
    : score != null ? data.label : data.currentIv != null ? 'Current IV' : data.historyDays < MIN_HISTORY_DAYS ? 'Building history' : 'Limited quote coverage';
  const historyLabel = data.historyDays ? `${data.historyDays} days${data.provisional ? ' (provisional)' : ''}` : 'No history yet';
  const selectedCell = selected ? data.cells.find(c => c.offset === selected.offset && c.dte === selected.dte) : null;

  useEffect(() => {
    if (open) dialog.current?.showModal();
    else dialog.current?.close();
  }, [open]);

  useEffect(() => {
    if (selected) contractPanel.current?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }, [selected]);

  async function copyInstrument(name: string) {
    try { await navigator.clipboard.writeText(name); setCopied(name); }
    catch { setCopied('failed'); }
  }

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
          <span className="text-xl tabular-nums text-gray-200">{score != null ? <>{score}<span className="text-xs text-gray-500"> / 100</span></> : !unavailable && data.currentIv != null ? `${data.currentIv.toFixed(1)}%` : '—'}</span>
        </div>
        {score != null ? <>
          <div className="relative h-2 rounded-full bg-gradient-to-r from-emerald-400/80 via-gray-600 to-amber-500/70"
            role="meter" aria-label="Volatility pricing: low means historically cheap"
            aria-valuemin={0} aria-valuemax={100} aria-valuenow={score} aria-valuetext={`${status}, ${score} out of 100`}>
            <span className="absolute top-1/2 w-1 h-4 rounded bg-white shadow -translate-x-1/2 -translate-y-1/2" style={{ left: `${score}%` }} />
          </div>
          <div className="flex justify-between text-[10px] text-gray-500"><span>Cheap</span><span>Typical</span><span>Expensive</span></div>
        </> : !unavailable && data.currentIv != null ? <p className="text-xs text-gray-400">
          {data.historyDays < MIN_HISTORY_DAYS ? `Comparison needs ${MIN_HISTORY_DAYS} days.` : 'Limited quote coverage.'}
        </p> : null}
        <div className="text-xs text-gray-500">{unavailable ? 'Refresh to get current options quotes' : historyLabel}</div>
        {score != null && <div className="flex flex-wrap gap-x-4 gap-y-1 text-[10px] text-gray-400">
          <span>{data.qualifier}</span>
          {data.currentIv != null && <span>IV {data.currentIv.toFixed(1)}%</span>}
          <span>{age}</span>
        </div>}
      </button>

      <dialog ref={dialog} onCancel={() => setOpen(false)} onClose={() => setOpen(false)}
        onClick={e => { if (e.target === e.currentTarget) setOpen(false); }}
        aria-labelledby="volatility-pricing-title"
        className="w-[min(850px,calc(100vw-32px))] max-h-[85vh] overflow-y-auto rounded-lg border border-gray-700 bg-[#181818] text-gray-200 p-0 backdrop:bg-black/75">
        <div className="p-5 md:p-6">
          <div className="flex justify-between items-start gap-4 mb-4">
            <div>
              <h2 id="volatility-pricing-title" className="text-lg text-juice-orange font-semibold">Volatility Pricing</h2>
              <p className="flex gap-4 text-sm text-gray-400 mt-1"><span>{status}</span>{score != null && <span>{score} / 100</span>}</p>
            </div>
            <button type="button" onClick={() => setOpen(false)} aria-label="Close volatility breakdown" className="px-3 py-1 rounded border border-gray-700 hover:bg-gray-800">✕</button>
          </div>
          {unavailable && <div role="alert" className="mb-4 flex items-center justify-between gap-4 text-sm text-amber-400">
            <span>Quotes unavailable.{data.asOf ? ` Last update: ${age}.` : ''}</span>
            <button type="button" onClick={refetch} className="border border-gray-600 rounded px-3 py-1" disabled={loading}>{loading ? 'Refreshing…' : 'Retry'}</button>
          </div>}
          <p className="text-xs text-gray-400 mb-4">IV / percentile. Brighter = cheaper. History → today. Click for contracts.</p>
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
                  const active = selected?.offset === offset && selected?.dte === dte;
                  return <td key={dte} className="p-0 min-w-[88px] tabular-nums">
                    <button type="button" disabled={unavailable || !cell}
                      onClick={() => { setSelected({ offset, dte }); setCopied(null); }} aria-pressed={active}
                      aria-label={`${offsetLabel(offset)}, ${dte} DTE: view contracts`}
                      className={`w-full rounded p-3 text-center focus-visible:outline focus-visible:outline-2 focus-visible:outline-white disabled:cursor-default ${active ? 'ring-2 ring-inset ring-white' : 'hover:ring-1 hover:ring-inset hover:ring-white/50'}`}
                      style={{ backgroundColor: pricingColor(p ?? null) }}>
                    <div className="text-sm">{!unavailable && cell?.iv != null ? `${cell.iv.toFixed(1)}%` : '—'}</div>
                    <div className="text-[10px] text-gray-300 mt-1">{p != null ? `${Math.round(p)} / 100` : 'Unrated'}</div>
                    <div className="flex h-2 gap-px mt-2 rounded-sm overflow-hidden" role="img"
                      aria-label={`${cell?.samples ?? 0} recorded days, oldest to newest, followed by today`}>
                      {(cell?.history ?? []).map((point, index, points) => <span key={point.at} className="flex-1 min-w-0"
                        style={{ backgroundColor: pricingColor(unavailable ? null : point.percentile) }}
                        title={`${index === points.length - 1 ? 'Today' : date(point.at)}: ${point.iv != null ? `${point.iv.toFixed(1)}% IV` : 'No quote'}${point.percentile != null ? `, ${Math.round(point.percentile)}/100` : ''}`} />)}
                    </div>
                    </button>
                  </td>;
                })}
              </tr>)}</tbody>
            </table>
          </div>
          {selected && <section ref={contractPanel} aria-label="Selected volatility contracts" aria-live="polite" className="mt-4 rounded border border-gray-700 p-3">
            <div className="flex justify-between gap-3 text-xs text-gray-400 mb-3">
              <h3>Closest contracts: {offsetLabel(selected.offset)} / {selected.dte} DTE</h3>
              <button type="button" onClick={() => setSelected(null)} aria-label="Close contract details">✕</button>
            </div>
            {unavailable ? <p className="text-xs text-amber-400">Quotes unavailable.</p>
              : selectedCell?.instruments?.length ? <div className="space-y-3">
                {selectedCell.instruments.map(instrument => <div key={instrument.name} className="space-y-2 text-xs">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span className="text-gray-200 break-all">{instrument.name}</span>
                    <button type="button" onClick={() => copyInstrument(instrument.name)} className="rounded border border-gray-600 px-2 py-1 hover:bg-gray-800">{copied === instrument.name ? 'Copied' : 'Copy'}</button>
                  </div>
                  <div className="flex flex-wrap gap-x-4 gap-y-1 text-gray-400">
                    <span>{instrument.type === 'P' ? 'Put' : 'Call'}</span>
                    <span>Strike {money(instrument.strike)}</span>
                    <span>{instrument.dte.toFixed(1)} DTE</span>
                    <span>{date(new Date(instrument.expiry * 1000).toISOString())}</span>
                    <span className="text-gray-200">Ask {instrument.ask.toLocaleString(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 2 })}</span>
                    <span>Ask IV {instrument.askIv.toFixed(1)}%</span>
                    <span>Size {instrument.askAmount}</span>
                  </div>
                </div>)}
                {copied === 'failed' && <p role="status" className="text-xs text-amber-400">Copy unavailable.</p>}
              </div> : <p className="text-xs text-gray-500">No quoted contract.</p>}
          </section>}
          <div className="mt-5 space-y-2 text-xs text-gray-500 leading-relaxed">
            {data.historyFrom && data.historyTo && <p>History: {date(data.historyFrom)} – {date(data.historyTo)} (UTC)</p>}
            {data.asOf && <p>Updated {age}</p>}
            <details>
              <summary className="cursor-pointer text-gray-400">Method</summary>
              <div className="mt-2 space-y-2">
                <p>Median historical percentile. Minimum {MIN_HISTORY_DAYS} days; provisional under 30. Coverage: {data.measured}/{data.total} cells.</p>
                <p>Strikes relative to spot. DTE = days to expiry. IV interpolated from quoted strikes and expiries.</p>
                <p>History colors use the same reference period as today. Contracts match nearest quoted expiry, then strike.</p>
              </div>
            </details>
          </div>
        </div>
      </dialog>
    </div>
  );
}
