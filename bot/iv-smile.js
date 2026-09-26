'use strict';

// Compact full-chain smile snapshots for the dashboard's volatility-smile playback.
// Point tuple layout (keep in sync with dashboard/src/lib/vol-smile.ts `fromCompact`):
//   [strike, isCall (0|1), delta, iv, bidIv|null, askIv|null, oi]   — IVs as decimals
// Same selection as the live chart: OTM vs the expiry's forward, |delta| >= 0.02.

const SMILE_SNAPSHOT_INTERVAL_MS = 15 * 60 * 1000;

const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : null; };
const r4 = (v) => (v == null ? null : Math.round(v * 1e4) / 1e4);

function buildSmileRows(tickerMap, expiryByDate) {
  const byDate = new Map();
  for (const [name, t] of Object.entries(tickerMap || {})) {
    const [, date, strikeStr, typeStr] = name.split('-');
    if (!expiryByDate[date] || (typeStr !== 'P' && typeStr !== 'C')) continue;
    const entry = byDate.get(date) || { forwards: [], spots: [], points: [] };
    byDate.set(date, entry);
    const op = t?.option_pricing;
    const f = num(op?.f) ?? num(t?.I);
    if (f > 0) entry.forwards.push(f);
    const spot = num(t?.I);
    if (spot > 0) entry.spots.push(spot);
    const strike = num(strikeStr), delta = num(op?.d), iv = num(op?.i);
    if (!strike || delta == null || !(iv > 0) || !(f > 0)) continue;
    const isPut = typeStr === 'P';
    if (isPut !== (strike < f) || Math.abs(delta) < 0.02) continue;
    const bi = num(op?.bi), ai = num(op?.ai);
    entry.points.push([strike, isPut ? 0 : 1, r4(delta), r4(iv), bi > 0 ? r4(bi) : null, ai > 0 ? r4(ai) : null, num(t?.stats?.oi) ?? 0]);
  }
  const median = (xs) => { const s = [...xs].sort((a, b) => a - b); return s[Math.floor(s.length / 2)]; };
  const rows = [];
  for (const [date, e] of byDate) {
    if (e.points.length < 4 || !e.forwards.length) continue;
    e.points.sort((a, b) => a[0] - b[0]);
    rows.push({
      expiry: expiryByDate[date],
      forward: median(e.forwards),
      spot: e.spots.length ? median(e.spots) : null,
      points: JSON.stringify(e.points),
    });
  }
  return rows.sort((a, b) => a.expiry - b.expiry);
}

module.exports = { buildSmileRows, SMILE_SNAPSHOT_INTERVAL_MS };
