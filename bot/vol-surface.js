'use strict';

// Volatility-surface evidence for the advisors: where IV sits in the two expiries the bot trades,
// against realized vol and against its own last 30 days. Pure functions; callers supply the data.

const HOURS_PER_YEAR = 24 * 365; // ETH trades around the clock
const MIN_PERCENTILE_SAMPLES = 24;
const DAY_MS = 86_400_000;

const finite = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : null);
const round = (v, d = 1) => (v == null ? null : Number(v.toFixed(d)));

// Linear interpolation of IV at |delta| = target on one side; null outside the quoted range.
function ivAtAbsDelta(points, type, target) {
  const side = points.filter(p => p.type === type).map(p => ({ d: Math.abs(p.delta), iv: p.iv })).sort((a, b) => a.d - b.d);
  for (let i = 0; i < side.length; i++) {
    if (side[i].d === target) return side[i].iv;
    if (i > 0 && side[i - 1].d < target && side[i].d > target) {
      const w = (target - side[i - 1].d) / (side[i].d - side[i - 1].d);
      return side[i - 1].iv + w * (side[i].iv - side[i - 1].iv);
    }
  }
  return null;
}

// One-sided ATM: the quote on `type`'s side nearest 50Δ. One-sided so live values stay comparable
// with options_snapshots history, which only holds OTM calls (call zone) or OTM puts (put zone).
function sideAtm(points, type) {
  let best = null;
  for (const p of points) if (p.type === type && (!best || Math.abs(p.delta) > Math.abs(best.delta))) best = p;
  return best && Math.abs(best.delta) >= 0.3 ? best.iv : null;
}

function zoneMetrics(points, type) {
  const atm = sideAtm(points, type);
  const wing10 = ivAtAbsDelta(points, type, 0.10);
  return { atm, wing10, wing_premium: atm != null && wing10 != null ? wing10 - atm : null };
}

function riskReversal25(points) {
  const c = ivAtAbsDelta(points, 'C', 0.25), p = ivAtAbsDelta(points, 'P', 0.25);
  return c != null && p != null ? c - p : null;
}

// Annualized close-to-close realized vol, in vol points.
function realizedVol(closes) {
  const r = [];
  for (let i = 1; i < closes.length; i++) if (closes[i] > 0 && closes[i - 1] > 0) r.push(Math.log(closes[i] / closes[i - 1]));
  if (r.length < 12) return null;
  const mean = r.reduce((s, x) => s + x, 0) / r.length;
  const variance = r.reduce((s, x) => s + (x - mean) ** 2, 0) / (r.length - 1);
  return Math.sqrt(variance * HOURS_PER_YEAR) * 100;
}

// Share of samples at or below value, 0–100.
function percentile(value, samples) {
  const xs = samples.filter(x => x != null);
  if (value == null || xs.length < MIN_PERCENTILE_SAMPLES) return null;
  return Math.round((xs.filter(x => x <= value).length / xs.length) * 100);
}

// Nearest expiry whose DTE falls inside [lo, hi].
function pickZoneExpiry(expiries, atMs, [lo, hi]) {
  return expiries
    .map(expiry => ({ expiry, dte: (expiry * 1000 - atMs) / DAY_MS }))
    .filter(e => e.dte >= lo && e.dte <= hi)
    .sort((a, b) => a.dte - b.dte)[0] || null;
}

// Hourly closes ending at or before atMs, newest last.
function closesUpTo(spotRows, atMs, hours) {
  const out = [];
  for (const row of spotRows) {
    if (Date.parse(row.hour) > atMs) break;
    out.push(row.close);
  }
  return out.slice(-(hours + 1));
}

// Per-hour history of the same measures, from bot-candidate rows sampled once an hour.
function buildSurfaceHistory(rows, spotRows, zones) {
  const byTs = new Map();
  for (const r of rows) {
    const type = r.option_type === 'P' ? 'P' : r.option_type === 'C' ? 'C' : null;
    const delta = finite(r.delta), iv = finite(r.implied_vol);
    if (!type || delta == null || !(iv > 0) || !r.expiry) continue;
    if (!byTs.has(r.timestamp)) byTs.set(r.timestamp, []);
    byTs.get(r.timestamp).push({ expiry: r.expiry, type, delta, iv: iv * 100 });
  }
  const history = [];
  for (const [ts, pts] of byTs) {
    const atMs = Date.parse(ts);
    const zone = (type, range) => {
      const pick = pickZoneExpiry([...new Set(pts.filter(p => p.type === type).map(p => p.expiry))], atMs, range);
      return pick ? zoneMetrics(pts.filter(p => p.expiry === pick.expiry), type) : null;
    };
    const call = zone('C', zones.call_dte_range);
    const put = zone('P', zones.put_dte_range);
    const rv7 = realizedVol(closesUpTo(spotRows, atMs, 24 * 7));
    history.push({
      at: atMs,
      call_atm: call?.atm ?? null,
      call_wing_premium: call?.wing_premium ?? null,
      put_atm: put?.atm ?? null,
      put_wing_premium: put?.wing_premium ?? null,
      term_slope: call?.atm != null && put?.atm != null ? put.atm - call.atm : null,
      call_vrp: call?.atm != null && rv7 != null ? call.atm - rv7 : null,
    });
  }
  return history;
}

// current: { atMs, expiries: [{ expiry, points: [{type, delta, iv (vol pts)}] }] } from the live chain.
function buildVolSurface({ current, spotRows, history, zones }) {
  const expiryList = current.expiries.map(e => e.expiry);
  const zoneOf = (type, range) => {
    const pick = pickZoneExpiry(expiryList, current.atMs, range);
    if (!pick) return null;
    const pts = current.expiries.find(e => e.expiry === pick.expiry).points;
    return { expiry: pick.expiry, dte: pick.dte, ...zoneMetrics(pts, type), rr25: riskReversal25(pts) };
  };
  const call = zoneOf('C', zones.call_dte_range);
  const put = zoneOf('P', zones.put_dte_range);
  const rv7 = realizedVol(closesUpTo(spotRows, current.atMs, 24 * 7));
  const rv30 = realizedVol(closesUpTo(spotRows, current.atMs, 24 * 30));
  const pct = (key, value) => percentile(value, history.map(h => h[key]));
  // How each measure moved: now minus the history sample nearest 24h / 7d ago. A percentile says
  // where vol sits in its month; the move says which way it is going, which the level alone hides.
  const then = (key, ageMs) => {
    const target = current.atMs - ageMs, tolerance = Math.max(2 * 3_600_000, ageMs / 12);
    let best = null;
    for (const h of history) {
      if (h.at == null || h[key] == null || Math.abs(h.at - target) > tolerance) continue;
      if (!best || Math.abs(h.at - target) < Math.abs(best.at - target)) best = h;
    }
    return best ? best[key] : null;
  };
  const move = (key, value) => {
    const change = (ageMs) => { const past = then(key, ageMs); return value != null && past != null ? round(value - past) : null; };
    return { d24h: change(DAY_MS), d7d: change(7 * DAY_MS) };
  };
  const termSlope = call?.atm != null && put?.atm != null ? put.atm - call.atm : null;
  const callVrp = call?.atm != null && rv7 != null ? call.atm - rv7 : null;
  const describe = (z, prefix) => z && {
    expiry: new Date(z.expiry * 1000).toISOString().slice(0, 10),
    dte: round(z.dte, 1),
    atm_iv: round(z.atm), atm_iv_pctl: pct(`${prefix}_atm`, z.atm), atm_iv_move: move(`${prefix}_atm`, z.atm),
    wing10_iv: round(z.wing10),
    wing_premium: round(z.wing_premium), wing_premium_pctl: pct(`${prefix}_wing_premium`, z.wing_premium),
    wing_premium_move: move(`${prefix}_wing_premium`, z.wing_premium),
    rr25: round(z.rr25),
  };
  return {
    history_samples: history.length,
    realized_vol: { rv7d: round(rv7), rv30d: round(rv30) },
    call_zone: describe(call, 'call'),
    put_zone: describe(put, 'put'),
    call_iv_minus_rv7d: round(callVrp), call_iv_minus_rv7d_pctl: pct('call_vrp', callVrp), call_iv_minus_rv7d_move: move('call_vrp', callVrp),
    put_iv_minus_rv30d: round(put?.atm != null && rv30 != null ? put.atm - rv30 : null),
    term_slope: round(termSlope), term_slope_pctl: pct('term_slope', termSlope), term_slope_move: move('term_slope', termSlope),
  };
}

function formatVolSurfaceForAdvisor(s) {
  if (!s || (!s.call_zone && !s.put_zone)) return 'Volatility surface unavailable (no quotes in the call or put DTE window).';
  const f = (v, signed = false) => (v == null ? 'n/a' : `${signed && v > 0 ? '+' : ''}${v}`);
  const p = (v) => (v == null ? '' : ` (pctl ${v})`);
  const m = (mv) => (mv && (mv.d24h != null || mv.d7d != null) ? ` [Δ24h ${f(mv.d24h, true)}, Δ7d ${f(mv.d7d, true)}]` : '');
  const zone = (label, z, wingLabel, vrp) => (z
    ? `${label} ${z.expiry} (${z.dte}d): ATM ${f(z.atm_iv)}${p(z.atm_iv_pctl)}${m(z.atm_iv_move)} | 10Δ ${wingLabel} ${f(z.wing10_iv)} | wing premium over ATM ${f(z.wing_premium, true)}${p(z.wing_premium_pctl)}${m(z.wing_premium_move)} | 25Δ RR (call−put) ${f(z.rr25, true)}${vrp}`
    : `${label}: no expiry in window.`);
  return [
    `IV and realized vol in annualized vol points. pctl = percentile vs hourly samples over the last 30d (n=${s.history_samples}); Δ24h/Δ7d = now minus the same zone measure that long ago (the zone's expiry can roll in between). ATM is one-sided (nearest-50Δ call for the call zone, put for the put zone) to match history.`,
    `Realized vol (hourly closes): 7d ${f(s.realized_vol.rv7d)} | 30d ${f(s.realized_vol.rv30d)}.`,
    zone('CALL zone', s.call_zone, 'call', ` | ATM IV − RV7d ${f(s.call_iv_minus_rv7d, true)}${p(s.call_iv_minus_rv7d_pctl)}${m(s.call_iv_minus_rv7d_move)}`),
    zone('PUT zone', s.put_zone, 'put', ` | ATM IV − RV30d ${f(s.put_iv_minus_rv30d, true)}`),
    `Term structure: put-zone ATM − call-zone ATM ${f(s.term_slope, true)}${p(s.term_slope_pctl)}${m(s.term_slope_move)}; negative means the front expiry is priced above the back (inverted).`,
    'Definitions, not rules: a high call wing premium and positive IV − RV mean calls are paid above realized movement; a low put-zone ATM or wing-premium percentile means protection is cheap relative to its own month.',
  ].join('\n');
}

module.exports = {
  ivAtAbsDelta, sideAtm, zoneMetrics, riskReversal25, realizedVol, percentile, pickZoneExpiry,
  buildSurfaceHistory, buildVolSurface, formatVolSurfaceForAdvisor,
};
