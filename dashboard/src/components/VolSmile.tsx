'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  ResponsiveContainer, ScatterChart, Scatter, XAxis, YAxis, ZAxis, Tooltip, ReferenceLine, ReferenceArea,
  ComposedChart, Line, Bar, Cell,
} from 'recharts';
import { usePolling } from '@/lib/hooks';
import { chartColors, chartAxis, chartTooltip } from '@/lib/chart';
import { deltaX, expiryStats, fromCompact, ivAtDelta, type CompactSmileRow, type SmileExpiry, type SmilePoint, type SmileStats } from '@/lib/vol-smile';

// Fixed slots: an expiry keeps its color while selected, whatever else is toggled.
const SLOT_COLORS = [chartColors.primary, chartColors.secondary, '#c084fc', '#fde047'];
const PUT_ZONE = '#f87171';
const CALL_ZONE = '#10b981';
const DELTA_TICKS = [-0.4, -0.25, 0, 0.25, 0.4];
const DELTA_TICK_LABEL: Record<string, string> = { '-0.4': '10Δ P', '-0.25': '25Δ P', '0': 'ATM', '0.25': '25Δ C', '0.4': '10Δ C' };

type Zone = { delta: [number, number]; dte: [number, number] };
type HistoryRow = { name: string; expiry: number; strike: number; type: 'P' | 'C'; delta: number; iv: number; timestamp: string };
type SmileResponse = {
  asOf?: string; expiries: SmileExpiry[]; history: HistoryRow[]; historyScope?: 'full' | 'zones';
  zones?: { put: Zone; call: Zone }; error?: string;
};
type Frame = { t: number; spot: number | null; expiries: SmileExpiry[] };
type Axis = 'delta' | 'strike' | 'usd';
const RANGES = ['24h', '3d', '7d', '30d'] as const;
const SPEEDS = [{ label: '1×', ms: 500 }, { label: '4×', ms: 125 }];
type Position = { instrument_name: string; amount: number };
type Row = SmileExpiry & { label: string; stats: SmileStats; inCall: boolean; inPut: boolean; oi: number };
type Plotted = SmilePoint & { x: number; label: string; held: number | null; iv24: number | null; color: string };

const EMPTY: SmileResponse = { expiries: [], history: [] };
const NO_EXPIRIES: SmileExpiry[] = [];
const spotOf = (expiries: SmileExpiry[]) => expiries.find(e => e.spot != null)?.spot ?? expiries[0]?.forward ?? null;
const toHistory = (f: Frame): HistoryRow[] => f.expiries.flatMap(e => e.points.map(p => ({
  name: p.name, expiry: e.expiry, strike: p.strike, type: p.type, delta: p.delta, iv: p.iv, timestamp: new Date(f.t).toISOString(),
})));
const fmtTime = (ms: number) => new Date(ms).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
const fmtPct = (v: number | null | undefined, d = 1) => (v == null ? '—' : `${v.toFixed(d)}%`);
const fmtSigned = (v: number | null | undefined, d = 1) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(d)}`);
const inRange = (v: number, [a, b]: [number, number]) => v >= Math.min(a, b) && v <= Math.max(a, b);

function Stat({ label, value, sub, tone }: { label: string; value: string; sub?: string; tone?: 'good' | 'bad' }) {
  return (
    <div className="rounded bg-gray-800/50 px-2 py-1.5 min-w-0">
      <div className="text-[10px] text-gray-500 truncate">{label}</div>
      <div className="text-sm text-gray-200 tabular-nums">{value}</div>
      {sub && <div className={`text-[10px] tabular-nums truncate ${tone === 'good' ? 'text-emerald-400' : tone === 'bad' ? 'text-red-400' : 'text-gray-500'}`}>{sub}</div>}
    </div>
  );
}

export default function VolSmile({ positions = [] }: { positions?: Position[] }) {
  const { data, error } = usePolling<SmileResponse>('/api/smile', EMPTY, 60_000);
  const [picked, setPicked] = useState<{ expiry: number; slot: number }[] | null>(null);
  const [axis, setAxis] = useState<Axis>('delta');
  const [view, setView] = useState<'chart' | 'table'>('chart');
  const [showHistory, setShowHistory] = useState(true);

  // ── Playback: frames from iv_smile_snapshots, decoded client-side ──
  const [replay, setReplay] = useState(false);
  const [range, setRange] = useState<(typeof RANGES)[number]>('24h');
  const [frames, setFrames] = useState<Frame[] | null>(null);
  const [frameIdx, setFrameIdx] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [speedMs, setSpeedMs] = useState(SPEEDS[0].ms);
  const [framesError, setFramesError] = useState<string | null>(null);

  useEffect(() => {
    if (!replay) return;
    let alive = true;
    setFrames(null);
    setFramesError(null);
    fetch(`/api/smile/history?range=${range}`)
      .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((j: { frames?: { t: string; spot: number | null; expiries: CompactSmileRow[] }[] }) => {
        if (!alive) return;
        const decoded = (j.frames ?? []).map(f => {
          const t = Date.parse(f.t);
          return { t, spot: f.spot, expiries: f.expiries.map(e => fromCompact(e, t)) };
        });
        setFrames(decoded);
        setFrameIdx(0);
        setPlaying(decoded.length > 1);
      })
      .catch((e: Error) => alive && setFramesError(e.message));
    return () => { alive = false; };
  }, [replay, range]);

  useEffect(() => {
    if (!playing || !frames) return;
    if (frameIdx >= frames.length - 1) { setPlaying(false); return; }
    const id = setTimeout(() => setFrameIdx(i => i + 1), speedMs);
    return () => clearTimeout(id);
  }, [playing, frameIdx, frames, speedMs]);

  const frame = replay && frames?.length ? frames[Math.min(frameIdx, frames.length - 1)] : null;
  const startFrame = replay && frames?.length ? frames[0] : null;
  const expiries = frame ? frame.expiries : (data.expiries ?? NO_EXPIRIES);
  const spot = frame ? (frame.spot ?? spotOf(frame.expiries)) : spotOf(data.expiries ?? []);
  // Reference overlay: start of the replay, or 24h ago when live.
  const reference = useMemo(() => (startFrame ? toHistory(startFrame) : (data.history ?? [])), [startFrame, data.history]);
  const refSpot = startFrame ? (startFrame.spot ?? spotOf(startFrame.expiries)) : spot;
  const refLabel = replay ? 'vs start' : '24h';

  const rows: Row[] = useMemo(() => expiries.map((e) => {
    const d = new Date(e.expiry * 1000);
    return {
      ...e,
      label: `${Math.round(e.dte)}d · ${d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', timeZone: 'UTC' })}`,
      stats: expiryStats(e.points),
      inCall: !!data.zones && inRange(e.dte, data.zones.call.dte),
      inPut: !!data.zones && inRange(e.dte, data.zones.put.dte),
      oi: e.points.reduce((s, p) => s + p.oi, 0),
    };
  }), [expiries, data.zones]);

  const callRow = rows.find(r => r.inCall) ?? rows[0];
  const putRow = rows.find(r => r.inPut) ?? rows[rows.length - 1];

  // Default view: front expiry, the bot's call expiry, ~30d, the bot's put expiry.
  const selection = useMemo(() => {
    if (picked) return picked;
    const want = [rows[0], callRow, rows.find(r => r.dte >= 25), putRow].filter((r): r is Row => !!r);
    return Array.from(new Set(want.map(r => r.expiry))).slice(0, SLOT_COLORS.length).map((expiry, slot) => ({ expiry, slot }));
  }, [picked, rows, callRow, putRow]);

  const toggle = (expiry: number) => {
    const cur = selection;
    if (cur.some(s => s.expiry === expiry)) return setPicked(cur.filter(s => s.expiry !== expiry));
    const free = SLOT_COLORS.findIndex((_, i) => !cur.some(s => s.slot === i));
    if (free >= 0) setPicked([...cur, { expiry, slot: free }]);
  };
  const colorOf = (expiry: number) => {
    const s = selection.find(x => x.expiry === expiry);
    return s ? SLOT_COLORS[s.slot] : null;
  };

  const held = useMemo(() => new Map(positions.filter(p => Number(p.amount) !== 0).map(p => [p.instrument_name, Number(p.amount)])), [positions]);
  const hist24 = useMemo(() => new Map(reference.map(h => [h.name, h])), [reference]);
  const histAt = reference[0]?.timestamp;

  // Strike axes are relative to spot (not each expiry's forward) so playback shows the smile moving with spot.
  const xOf = (p: { type: 'P' | 'C'; delta: number; strike: number }, s: number | null) =>
    (axis === 'delta' ? deltaX(p) : axis === 'usd' || !s ? p.strike : (p.strike / s - 1) * 100);

  const series = useMemo(() => selection.map(({ expiry, slot }) => {
    const row = rows.find(r => r.expiry === expiry);
    if (!row) return null;
    const color = SLOT_COLORS[slot];
    const pts: Plotted[] = row.points
      .map(p => ({ ...p, x: xOf(p, spot), label: row.label, held: replay ? null : held.get(p.name) ?? null, iv24: hist24.get(p.name)?.iv ?? null, color }))
      .sort((a, b) => a.x - b.x);
    const past = reference.filter(h => h.expiry === expiry).map(h => ({ ...h, x: xOf(h, refSpot) })).sort((a, b) => a.x - b.x);
    return { row, color, pts, past };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }).filter(<T,>(s: T | null): s is T => s != null), [selection, rows, axis, held, hist24, reference, spot, refSpot, replay]);

  // During playback the axes are fixed across all frames, so motion is the data moving, not the scale.
  const replayDomain = useMemo(() => {
    if (!frames?.length || !replay) return null;
    const want = new Set(selection.map(s => s.expiry));
    let x0 = Infinity, x1 = -Infinity, y0 = Infinity, y1 = -Infinity;
    for (const f of frames) {
      const s = f.spot ?? spotOf(f.expiries);
      for (const e of f.expiries) {
        if (!want.has(e.expiry)) continue;
        for (const p of e.points) {
          const x = xOf(p, s);
          x0 = Math.min(x0, x); x1 = Math.max(x1, x); y0 = Math.min(y0, p.iv); y1 = Math.max(y1, p.iv);
        }
      }
    }
    if (!Number.isFinite(x0)) return null;
    return { x: [x0, x1] as [number, number], y: [Math.max(0, Math.floor(y0 / 5) * 5), Math.ceil(y1 / 5) * 5] as [number, number] };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [frames, replay, selection, axis]);

  const enterReplay = () => { setPicked(selection); setReplay(true); };
  const exitReplay = () => { setReplay(false); setPlaying(false); setFrames(null); };

  if (error && !rows.length && !replay) return <div className="mt-3 pt-3 border-t border-white/5 text-[10px] text-gray-600">Volatility smile unavailable: {error}</div>;
  if (!replay && (!rows.length || !callRow || !putRow)) return null;

  const cs = callRow?.stats, ps = putRow?.stats;
  const callPrev = callRow && reference.length ? ivAtDelta(reference.filter(h => h.expiry === callRow.expiry), 'C', 0.10) : null;
  const putPrev = putRow && reference.length ? ivAtDelta(reference.filter(h => h.expiry === putRow.expiry), 'P', 0.10) : null;
  const zones = data.zones;
  const heldPts = series.flatMap(s => s.pts.filter(p => p.held != null)).sort((a, b) => a.x - b.x).map((p, i) => ({ ...p, below: i % 2 === 1 }));

  // Term-structure panel data, one row per expiry.
  const term = rows.map(r => ({ label: `${Math.round(r.dte)}d`, expiry: r.expiry, atm: r.stats.atm, rr25: r.stats.rr25, inCall: r.inCall, inPut: r.inPut }));
  const zoneBand = (key: 'inCall' | 'inPut') => {
    const inZone = term.filter(t => t[key]);
    return inZone.length ? [inZone[0].label, inZone[inZone.length - 1].label] as const : null;
  };
  const callBand = zoneBand('inCall'), putBand = zoneBand('inPut');

  const chip = (active: boolean) => `px-1.5 py-0.5 rounded border transition-colors ${active ? 'border-white/20 text-gray-200 bg-white/5' : 'border-white/5 text-gray-500 hover:text-gray-300'}`;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const smileTooltip = ({ active, payload }: any) => {
    const p: Plotted | undefined = active ? payload?.find((x: { payload?: Plotted }) => x.payload?.name && x.payload?.oi != null)?.payload : undefined;
    if (!p) return null;
    return (
      <div style={{ ...chartTooltip.contentStyle, padding: '8px 12px' }} className="text-xs space-y-0.5">
        <div className="flex items-center gap-1.5 text-gray-200">
          <span className="w-2 h-2 rounded-full" style={{ background: p.color }} />
          {p.label} · {p.strike} {p.type === 'P' ? 'put' : 'call'}
        </div>
        <div className="text-gray-300 tabular-nums">
          IV {p.iv.toFixed(1)}%
          {p.iv24 != null && <span className={p.iv - p.iv24 >= 0 ? 'text-emerald-400' : 'text-red-400'}> ({fmtSigned(p.iv - p.iv24)} {refLabel})</span>}
        </div>
        <div className="text-gray-500 tabular-nums">bid {fmtPct(p.bidIv)} · ask {fmtPct(p.askIv)}</div>
        <div className="text-gray-500 tabular-nums">Δ {p.delta.toFixed(3)} · OI {p.oi.toFixed(1)}</div>
        {p.held != null && <div className="text-gray-200">You hold {p.held > 0 ? '+' : ''}{p.held} ({p.held < 0 ? 'short' : 'long'})</div>}
      </div>
    );
  };

  return (
    <div className="mt-3 pt-3 border-t border-white/5">
      <div className="flex flex-wrap items-center justify-between gap-2 mb-2">
        <div className="text-[10px] text-gray-500 flex items-center gap-1.5">
          <span className="w-2 h-2 rounded-full inline-block" style={{ background: SLOT_COLORS[2] }} />
          Volatility Smile
          <span className="text-gray-600">
            {frame
              ? `· replay · ${fmtTime(frame.t)}`
              : `· live Derive chain${data.asOf ? ` · ${new Date(data.asOf).toLocaleTimeString()}` : ''}`}
          </span>
        </div>
        <div className="flex flex-wrap gap-1 text-[10px]">
          <button className={chip(view === 'chart' && axis === 'delta')} onClick={() => { setView('chart'); setAxis('delta'); }}>by delta</button>
          <button className={chip(view === 'chart' && axis === 'strike')} onClick={() => { setView('chart'); setAxis('strike'); }}>% from spot</button>
          <button className={chip(view === 'chart' && axis === 'usd')} onClick={() => { setView('chart'); setAxis('usd'); }}>$ strike</button>
          <button className={chip(view === 'table')} onClick={() => setView('table')}>table</button>
          {histAt && <button className={chip(showHistory)} onClick={() => setShowHistory(v => !v)}>{replay ? 'start' : '24h ago'}</button>}
          <button className={chip(replay)} onClick={replay ? exitReplay : enterReplay}>{replay ? '● back to live' : '▶ replay'}</button>
        </div>
      </div>

      {replay && (
        <div className="mb-2">
          <div className="flex flex-wrap items-center gap-1 text-[10px]">
            <button
              className={chip(playing)}
              aria-label={playing ? 'Pause' : 'Play'}
              disabled={!frames || frames.length < 2}
              onClick={() => {
                if (!frames) return;
                if (!playing && frameIdx >= frames.length - 1) setFrameIdx(0);
                setPlaying(v => !v);
              }}
            >{playing ? '❚❚ pause' : '▶ play'}</button>
            {SPEEDS.map(sp => <button key={sp.label} className={chip(speedMs === sp.ms)} onClick={() => setSpeedMs(sp.ms)}>{sp.label}</button>)}
            <span className="w-2" />
            {RANGES.map(r => <button key={r} className={chip(range === r)} onClick={() => setRange(r)}>{r}</button>)}
            {frame && spot != null && refSpot != null && (
              <span className="ml-auto text-gray-400 tabular-nums">
                frame {frameIdx + 1}/{frames!.length} · spot ${spot.toFixed(0)}
                <span className={spot >= refSpot ? 'text-emerald-400' : 'text-red-400'}> ({fmtSigned((spot / refSpot - 1) * 100)}% vs start)</span>
              </span>
            )}
          </div>
          {frames && frames.length > 1 && (
            // Spot track doubles as the scrubber: a native range input laid over the sparkline.
            <div className="relative h-10 mt-1">
              <ResponsiveContainer width="100%" height={40}>
                <ComposedChart data={frames.map((f, i) => ({ i, spot: f.spot }))} margin={{ top: 4, right: 0, bottom: 4, left: 0 }}>
                  <XAxis type="number" dataKey="i" domain={[0, frames.length - 1]} hide />
                  <YAxis domain={['auto', 'auto']} hide />
                  <Line dataKey="spot" stroke={chartColors.primary} strokeWidth={1.5} dot={false} isAnimationActive={false} connectNulls />
                  <ReferenceLine x={frameIdx} stroke="#ddd" strokeWidth={1.5} />
                </ComposedChart>
              </ResponsiveContainer>
              <input
                type="range" min={0} max={frames.length - 1} value={frameIdx} aria-label="Replay position"
                onChange={e => { setPlaying(false); setFrameIdx(Number(e.target.value)); }}
                className="absolute inset-0 w-full h-full opacity-0 cursor-ew-resize"
              />
            </div>
          )}
          {frames && (
            <div className="flex justify-between text-[10px] text-gray-600 tabular-nums">
              {frames.length > 0 && <span>{fmtTime(frames[0].t)}</span>}
              {frames.length > 0 && <span>{fmtTime(frames[frames.length - 1].t)}</span>}
            </div>
          )}
        </div>
      )}

      {!rows.length || !callRow || !putRow || !cs || !ps ? (
        <div className="text-[11px] text-gray-500 py-8 text-center">
          {framesError
            ? `Replay unavailable: ${framesError}`
            : !frames
              ? 'Loading snapshots…'
              : 'No full-chain snapshots in this range yet. The bot records one every 15 minutes, so playback fills in from its next deploy onward.'}
        </div>
      ) : (<>

      {/* Headline: what the bot trades, right now */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-1.5 mb-2">
        <Stat label={`ATM IV · ${callRow.label}`} value={fmtPct(cs.atm)} sub={`${putRow.label}: ${fmtPct(ps.atm)}`} />
        <Stat
          label={`10Δ call IV · sell zone ${callRow.label}`}
          value={fmtPct(cs.call10)}
          sub={`${fmtSigned(cs.call10 != null && cs.atm != null ? cs.call10 - cs.atm : null)} vs ATM${callPrev != null && cs.call10 != null ? ` · ${fmtSigned(cs.call10 - callPrev)} ${refLabel}` : ''}`}
          tone={cs.call10 != null && cs.atm != null && cs.call10 > cs.atm ? 'good' : undefined}
        />
        <Stat
          label={`10Δ put IV · buy zone ${putRow.label}`}
          value={fmtPct(ps.put10)}
          sub={`${fmtSigned(ps.put10 != null && ps.atm != null ? ps.put10 - ps.atm : null)} vs ATM${putPrev != null && ps.put10 != null ? ` · ${fmtSigned(ps.put10 - putPrev)} ${refLabel}` : ''}`}
          tone={ps.put10 != null && ps.atm != null && ps.put10 - ps.atm < 3 ? 'good' : undefined}
        />
        <Stat
          label={`25Δ risk reversal · ${callRow.label}`}
          value={fmtSigned(cs.rr25)}
          sub={cs.rr25 == null ? undefined : cs.rr25 > 0 ? 'calls richer than puts' : 'puts richer than calls'}
          tone={cs.rr25 != null && cs.rr25 > 0 ? 'good' : undefined}
        />
      </div>

      {/* Expiry selector — color stays with the expiry */}
      <div className="flex flex-wrap gap-1 mb-1 text-[10px]">
        {rows.map(r => {
          const c = colorOf(r.expiry);
          return (
            <button key={r.expiry} onClick={() => toggle(r.expiry)} className={chip(!!c)} title={r.inCall ? 'bot call-selling window' : r.inPut ? 'bot put-buying window' : undefined}>
              <span className="inline-block w-1.5 h-1.5 rounded-full mr-1 align-middle" style={{ background: c ?? '#444' }} />
              {Math.round(r.dte)}d
              {r.inCall && <span className="ml-1 text-emerald-500">C</span>}
              {r.inPut && <span className="ml-1 text-red-400">P</span>}
            </button>
          );
        })}
      </div>

      {view === 'chart' ? (
        <>
          {/* Legend: identity is never color-alone */}
          <div className="flex flex-wrap gap-x-3 gap-y-0.5 text-[10px] text-gray-400 mb-0.5">
            {series.map(s => (
              <span key={s.row.expiry} className="flex items-center gap-1">
                <span className="w-3 h-0.5 inline-block" style={{ background: s.color }} />{s.row.label}
              </span>
            ))}
            {showHistory && histAt && (
              <span className="text-gray-600">
                - - - {replay ? 'start of replay' : data.historyScope === 'full' ? '24h ago' : '24h ago (bot zones only)'}
              </span>
            )}
            {heldPts.length > 0 && <span className="text-gray-600">◯ your positions</span>}
            <span className="text-gray-600">dot size = open interest</span>
          </div>
          <ResponsiveContainer width="100%" height={260}>
            <ScatterChart margin={{ top: 14, right: 12, bottom: 4, left: 0 }}>
              <XAxis
                {...chartAxis} type="number" dataKey="x"
                domain={axis === 'delta' ? [-0.5, 0.5] : replayDomain?.x ?? ['dataMin', 'dataMax']}
                allowDataOverflow={!!replayDomain}
                ticks={axis === 'delta' ? DELTA_TICKS : undefined}
                tickFormatter={(v: number) => (axis === 'delta'
                  ? DELTA_TICK_LABEL[String(v)] ?? ''
                  : axis === 'usd' ? `$${v.toFixed(0)}` : `${v > 0 ? '+' : ''}${v.toFixed(0)}%`)}
              />
              <YAxis {...chartAxis} type="number" dataKey="iv" domain={replayDomain?.y ?? ['auto', 'auto']} allowDataOverflow={!!replayDomain}
                width={44} tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
              <ZAxis type="number" dataKey="oi" range={[10, 110]} />
              {axis === 'delta' && zones && (
                <>
                  <ReferenceArea x1={Math.abs(zones.put.delta[1]) - 0.5} x2={Math.abs(zones.put.delta[0]) - 0.5} fill={PUT_ZONE} fillOpacity={0.07}
                    label={{ value: `buy puts ${zones.put.dte.join('–')}d`, position: 'insideTop', fill: PUT_ZONE, fontSize: 9, opacity: 0.8 }} />
                  <ReferenceArea x1={0.5 - zones.call.delta[1]} x2={0.5 - zones.call.delta[0]} fill={CALL_ZONE} fillOpacity={0.07}
                    label={{ value: `sell calls ${zones.call.dte.join('–')}d`, position: 'insideTop', fill: CALL_ZONE, fontSize: 9, opacity: 0.8 }} />
                </>
              )}
              {axis === 'usd' && replay && refSpot != null && showHistory && (
                <ReferenceLine x={refSpot} stroke="#555" strokeDasharray="2 4"
                  label={{ value: `start $${refSpot.toFixed(0)}`, position: 'insideBottom', fill: '#666', fontSize: 9 }} />
              )}
              <ReferenceLine x={axis === 'usd' ? spot ?? 0 : 0} stroke={axis === 'delta' ? '#555' : '#999'} strokeDasharray="4 4"
                label={{
                  value: axis === 'delta' ? '← puts · calls →' : `← puts · spot${spot != null ? ` $${spot.toFixed(0)}` : ''} · calls →`,
                  position: 'insideTop', fill: '#888', fontSize: 10,
                }} />
              {showHistory && series.map(s => s.past.length > 1 && (
                <Scatter key={`h${s.row.expiry}`} data={s.past} legendType="none" isAnimationActive={false}
                  line={{ stroke: s.color, strokeWidth: 1.5, strokeDasharray: '4 3', strokeOpacity: 0.45 }}
                  shape={() => <g />} />
              ))}
              {series.map(s => (
                <Scatter key={s.row.expiry} name={s.row.label} data={s.pts} fill={s.color} fillOpacity={0.75}
                  line={{ stroke: s.color, strokeWidth: 2 }} isAnimationActive={false} />
              ))}
              {heldPts.length > 0 && (
                <Scatter data={heldPts} legendType="none" isAnimationActive={false}
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  shape={((p: any) => (
                    <g>
                      <circle cx={p.cx} cy={p.cy} r={8} fill="none" stroke="#fff" strokeWidth={1.5} />
                      {/* Alternate above/below so neighbouring positions don't overprint */}
                      <text x={p.cx} y={p.payload.below ? p.cy + 19 : p.cy - 12} textAnchor="middle" fontSize={9} fill="#ddd">
                        {p.payload.held > 0 ? '+' : ''}{p.payload.held}
                      </text>
                    </g>
                  )) as never} />
              )}
              <Tooltip {...chartTooltip} cursor={{ stroke: '#555', strokeDasharray: '3 3' }} content={smileTooltip} />
            </ScatterChart>
          </ResponsiveContainer>

          {/* Term structure + skew by expiry: two measures, two charts, shared x */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-2">
            {([
              { key: 'atm', title: 'ATM IV term structure', hint: 'upward = later expiries pricier' },
              { key: 'rr25', title: '25Δ risk reversal by expiry', hint: 'call IV − put IV · green = calls rich' },
            ] as const).map(panel => (
              <div key={panel.key}>
                <div className="text-[10px] text-gray-500 mb-0.5 flex">
                  {panel.title}<span className="text-gray-600 ml-auto">{panel.hint}</span>
                </div>
                <ResponsiveContainer width="100%" height={110}>
                  <ComposedChart data={term} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
                    <XAxis {...chartAxis} dataKey="label" interval="preserveStartEnd" tick={{ ...chartAxis.tick, fontSize: 9 }} />
                    <YAxis {...chartAxis} width={40} domain={['auto', 'auto']} tickFormatter={(v: number) => (panel.key === 'atm' ? `${v.toFixed(0)}%` : fmtSigned(v, 0))} />
                    {callBand && <ReferenceArea x1={callBand[0]} x2={callBand[1]} fill={CALL_ZONE} fillOpacity={0.07} />}
                    {putBand && <ReferenceArea x1={putBand[0]} x2={putBand[1]} fill={PUT_ZONE} fillOpacity={0.07} />}
                    {panel.key === 'atm' ? (
                      <Line dataKey="atm" stroke={SLOT_COLORS[2]} strokeWidth={2} isAnimationActive={false} connectNulls
                        // eslint-disable-next-line @typescript-eslint/no-explicit-any
                        dot={((p: any) => {
                          const c = colorOf(p.payload.expiry);
                          return <circle key={p.index} cx={p.cx} cy={p.cy} r={c ? 4 : 2.5} fill={c ?? SLOT_COLORS[2]} stroke="#262626" strokeWidth={1.5}
                            style={{ cursor: 'pointer' }} onClick={() => toggle(p.payload.expiry)} />;
                        }) as never} />
                    ) : (
                      <>
                        <ReferenceLine y={0} stroke="#555" />
                        <Bar dataKey="rr25" isAnimationActive={false} radius={[2, 2, 0, 0]}
                          onClick={(d: { payload?: { expiry: number } }) => d.payload && toggle(d.payload.expiry)} style={{ cursor: 'pointer' }}>
                          {term.map(t => <Cell key={t.expiry} fill={(t.rr25 ?? 0) >= 0 ? CALL_ZONE : PUT_ZONE} fillOpacity={colorOf(t.expiry) ? 0.9 : 0.45} />)}
                        </Bar>
                      </>
                    )}
                    <Tooltip {...chartTooltip}
                      // eslint-disable-next-line @typescript-eslint/no-explicit-any
                      content={({ active, payload }: any) => {
                        const t = active ? payload?.[0]?.payload : null;
                        if (!t) return null;
                        return (
                          <div style={{ ...chartTooltip.contentStyle, padding: '6px 10px' }} className="text-xs tabular-nums">
                            <div className="text-gray-300">{rows.find(r => r.expiry === t.expiry)?.label}</div>
                            <div className="text-gray-400">ATM {fmtPct(t.atm)} · RR25 {fmtSigned(t.rr25)}</div>
                            <div className="text-gray-600">click to {colorOf(t.expiry) ? 'hide' : 'show'} on smile</div>
                          </div>
                        );
                      }} />
                  </ComposedChart>
                </ResponsiveContainer>
              </div>
            ))}
          </div>
        </>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-[11px] tabular-nums">
            <thead className="text-gray-500">
              <tr className="text-right">
                <th className="text-left font-normal py-1">Expiry</th>
                <th className="font-normal">Fwd</th><th className="font-normal">ATM</th>
                <th className="font-normal">25Δ P</th><th className="font-normal">25Δ C</th><th className="font-normal">RR25</th>
                <th className="font-normal">10Δ P</th><th className="font-normal">10Δ C</th><th className="font-normal">RR10</th>
                <th className="font-normal">OI</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(r => {
                const c = colorOf(r.expiry);
                return (
                  <tr key={r.expiry} onClick={() => toggle(r.expiry)} className="text-right text-gray-300 border-t border-white/5 cursor-pointer hover:bg-white/5">
                    <td className="text-left py-1">
                      <span className="inline-block w-1.5 h-1.5 rounded-full mr-1.5" style={{ background: c ?? '#444' }} />
                      {r.label}
                      {r.inCall && <span className="ml-1 text-emerald-500">sell C</span>}
                      {r.inPut && <span className="ml-1 text-red-400">buy P</span>}
                    </td>
                    <td>{r.forward.toFixed(0)}</td>
                    <td>{fmtPct(r.stats.atm)}</td>
                    <td>{fmtPct(r.stats.put25)}</td><td>{fmtPct(r.stats.call25)}</td>
                    <td className={(r.stats.rr25 ?? 0) > 0 ? 'text-emerald-400' : 'text-gray-400'}>{fmtSigned(r.stats.rr25)}</td>
                    <td>{fmtPct(r.stats.put10)}</td><td>{fmtPct(r.stats.call10)}</td>
                    <td className={(r.stats.rr10 ?? 0) > 0 ? 'text-emerald-400' : 'text-gray-400'}>{fmtSigned(r.stats.rr10)}</td>
                    <td>{r.oi.toFixed(0)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      </>)}

      <div className="text-[10px] text-gray-600 mt-1.5">
        Reading it: the right side is calls, and a rising right wing means rich call premium (good for selling). The left side is puts, and a steep left wing means expensive protection.
        {axis === 'delta'
          ? ' The delta axis lines every expiry up on the same moneyness, so it isolates skew from spot moves.'
          : axis === 'usd'
            ? ' $ strike with a moving spot line: in replay, watch whether the smile follows spot or stays pinned to strikes.'
            : ' Strike is shown as % from spot.'}
      </div>
    </div>
  );
}
