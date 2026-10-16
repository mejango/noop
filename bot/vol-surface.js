'use strict';

// Volatility-surface evidence for the advisors: where IV sits in the two expiries the bot trades,
// against realized vol and against all of its own recorded history. Pure functions; callers supply the data.

const HOURS_PER_YEAR = 24 * 365; // ETH trades around the clock
const MIN_PERCENTILE_SAMPLES = 24;
const MIN_HISTORY_HOURS = 24 * 7; // a percentile needs at least a week of distinct hourly samples
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

// Require every completed close in the named window; no current-hour lookahead.
function closesUpTo(spotRows, atMs, hours) {
  const end = Math.floor(atMs / 3_600_000) * 3_600_000 - 3_600_000;
  const map = new Map(spotRows.map(r => [Date.parse(r.hour), r.close]));
  const closes = [];
  for (let at = end - hours * 3_600_000; at <= end; at += 3_600_000) {
    const close = finite(map.get(at));
    if (!(close > 0)) return [];
    closes.push(close);
  }
  return closes;
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
      return pick ? { expiry: pick.expiry, ...zoneMetrics(pts.filter(p => p.expiry === pick.expiry), type) } : null;
    };
    const call = zone('C', zones.call_dte_range);
    const put = zone('P', zones.put_dte_range);
    const rv7 = realizedVol(closesUpTo(spotRows, atMs, 24 * 7));
    history.push({
      at: atMs, call_expiry: call?.expiry ?? null, put_expiry: put?.expiry ?? null,
      call_wing10: call?.wing10 ?? null, put_wing10: put?.wing10 ?? null,
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
  const completedAt = Math.floor(current.atMs / 3_600_000) * 3_600_000;
  const priorHistory = history.filter(h => h.at < completedAt);
  // Each measure is ranked against every recorded hour that has it, so measures with longer
  // records (10Δ wings, from bot candidates) reach further back than ATM-based ones.
  const span = {};
  const pct = (key, value) => {
    const valid = priorHistory.filter(h => h[key] != null);
    const hours = new Set(valid.map(h => Math.floor(h.at / 3_600_000))).size;
    const first = valid.reduce((min, h) => Math.min(min, h.at), Infinity);
    span[key] = { hours, since: valid.length ? new Date(first).toISOString().slice(0, 10) : null };
    return hours >= MIN_HISTORY_HOURS ? percentile(value, valid.map(h => h[key])) : null;
  };
  // How each measure moved: now minus the history sample nearest 24h / 7d ago. A percentile says
  // where vol sits in its month; the move says which way it is going, which the level alone hides.
  const then = (key, ageMs) => {
    const target = current.atMs - ageMs, tolerance = 3_600_000;
    let best = null;
    for (const h of history) {
      const sides = key.startsWith('call_') ? ['call'] : key.startsWith('put_') ? ['put'] : ['call','put'];
      const comparable = sides.every(side => (side === 'call' ? call : put)?.expiry != null
        && h[`${side}_expiry`] === (side === 'call' ? call : put).expiry);
      if (!comparable || h.at == null || h.at > current.atMs || h[key] == null || Math.abs(h.at - target) > tolerance) continue;
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
    wing10_iv: round(z.wing10), wing10_iv_pctl: pct(`${prefix}_wing10`, z.wing10), wing10_iv_move: move(`${prefix}_wing10`, z.wing10),
    wing_premium: round(z.wing_premium), wing_premium_pctl: pct(`${prefix}_wing_premium`, z.wing_premium),
    wing_premium_move: move(`${prefix}_wing_premium`, z.wing_premium),
    rr25: round(z.rr25),
  };
  const result = {
    realized_vol: { rv7d: round(rv7), rv30d: round(rv30) },
    call_zone: describe(call, 'call'),
    put_zone: describe(put, 'put'),
    call_iv_minus_rv7d: round(callVrp), call_iv_minus_rv7d_pctl: pct('call_vrp', callVrp), call_iv_minus_rv7d_move: move('call_vrp', callVrp),
    put_iv_minus_rv30d: round(put?.atm != null && rv30 != null ? put.atm - rv30 : null),
    term_slope: round(termSlope), term_slope_pctl: pct('term_slope', termSlope), term_slope_move: move('term_slope', termSlope),
  };
  return { ...result, history_span: span };
}

function formatVolSurfaceForAdvisor(s) {
  if (!s || (!s.call_zone && !s.put_zone)) return 'Volatility surface unavailable (no quotes in the call or put DTE window).';
  const f = (v, signed = false) => (v == null ? 'n/a' : `${signed && v > 0 ? '+' : ''}${v}`);
  const span = s.history_span || {};
  const p = (v, key) => {
    if (v != null) return ` (pctl ${v} of ${span[key]?.hours ?? '?'}h since ${span[key]?.since ?? '?'})`;
    return span[key]?.hours ? ` (pctl n/a: ${span[key].hours}h recorded, needs ${MIN_HISTORY_HOURS})` : '';
  };
  const m = (mv) => (mv && (mv.d24h != null || mv.d7d != null) ? ` [Δ24h ${f(mv.d24h, true)}, Δ7d ${f(mv.d7d, true)}]` : '');
  const zone = (label, z, prefix, wingLabel, vrp) => (z
    ? `${label} ${z.expiry} (${z.dte}d): 10Δ ${wingLabel} IV ${f(z.wing10_iv)}${p(z.wing10_iv_pctl, `${prefix}_wing10`)}${m(z.wing10_iv_move)} | ATM ${f(z.atm_iv)}${p(z.atm_iv_pctl, `${prefix}_atm`)}${m(z.atm_iv_move)} | wing premium over ATM ${f(z.wing_premium, true)}${p(z.wing_premium_pctl, `${prefix}_wing_premium`)}${m(z.wing_premium_move)} | 25Δ RR (call−put) ${f(z.rr25, true)}${vrp}`
    : `${label}: no expiry in window.`);
  return [
    `IV and realized vol in annualized vol points. pctl = share of ALL recorded hourly samples of the same measure at or below now (span and start date shown per measure; 10Δ wings reach back further than ATM measures). This is the answer to how cheap or expensive volatility is versus everything observed. Percentiles are rolling-zone comparisons, not constant-maturity estimates. Δ24h/Δ7d = now minus the same zone measure that long ago (changes require the same expiry; unavailable baselines and expiry rolls are n/a). ATM is one-sided (nearest-50Δ call for the call zone, put for the put zone) to match history.`,
    `Realized vol (complete windows of completed hourly closes): 7d ${f(s.realized_vol.rv7d)} | 30d ${f(s.realized_vol.rv30d)}.`,
    zone('CALL zone', s.call_zone, 'call', 'call', ` | ATM IV − RV7d ${f(s.call_iv_minus_rv7d, true)}${p(s.call_iv_minus_rv7d_pctl, 'call_vrp')}${m(s.call_iv_minus_rv7d_move)}`),
    zone('PUT zone', s.put_zone, 'put', 'put', ` | ATM IV − RV30d ${f(s.put_iv_minus_rv30d, true)}`),
    `Term structure: put-zone ATM − call-zone ATM ${f(s.term_slope, true)}${p(s.term_slope_pctl, 'term_slope')}${m(s.term_slope_move)}; negative means the front expiry is priced above the back (inverted).`,
    'Definitions, not rules: a high call-zone 10Δ IV percentile, high wing premium and positive IV − RV mean calls are paid above realized movement; a low put-zone 10Δ IV or ATM percentile means protection is cheap versus all recorded history.',
  ].join('\n');
}

// Every listed expiry's ATM IV and 25Δ risk reversal, with the change since the same expiry ~24h earlier.
// ATM = mean of the put and call nearest 50Δ, matching the dashboard's term-structure chart.
// expiries / pastExpiries: [{ expiry (unix s), points: [{type, delta, iv (vol pts)}] }]; pastExpiries may be empty.
function twoSidedAtm(pts) {
  const sides = [sideAtm(pts, 'P'), sideAtm(pts, 'C')].filter(v => v != null);
  return sides.length ? sides.reduce((s, v) => s + v, 0) / sides.length : null;
}

function buildTermCurve(expiries, atMs, pastExpiries = []) {
  const atm = twoSidedAtm;
  const past = new Map(pastExpiries.map(e => [e.expiry, e.points]));
  const diff = (now, then) => (now != null && then != null ? round(now - then) : null);
  return expiries
    .filter(e => e.expiry * 1000 > atMs)
    .sort((a, b) => a.expiry - b.expiry)
    .map(e => {
      const a = atm(e.points), rr = riskReversal25(e.points), prev = past.get(e.expiry);
      return {
        expiry: new Date(e.expiry * 1000).toISOString().slice(0, 10),
        dte: round((e.expiry * 1000 - atMs) / DAY_MS, 1),
        atm_iv: round(a), atm_iv_d24h: prev ? diff(a, atm(prev)) : null,
        rr25: round(rr), rr25_d24h: prev ? diff(rr, riskReversal25(prev)) : null,
      };
    });
}

const TERM_TENORS = [7, 14, 30, 60, 90];

// The curve at fixed maturities, so snapshots days apart compare like for like despite expiry rolls.
// snapshots: [{ label, atMs, expiries }] (expiries as in buildTermCurve; empty when that snapshot is missing).
// ponytail: linear in IV between neighbouring expiries, no extrapolation; total-variance interpolation if the front gets sparse.
function buildTermHistory(snapshots, tenors = TERM_TENORS) {
  const at = (expiries, atMs, metric, tenor) => {
    const pts = expiries
      .map(e => ({ dte: (e.expiry * 1000 - atMs) / DAY_MS, v: metric(e.points) }))
      .filter(p => p.dte > 0 && p.v != null)
      .sort((a, b) => a.dte - b.dte);
    for (let i = 0; i < pts.length; i++) {
      if (pts[i].dte === tenor) return pts[i].v;
      if (i > 0 && pts[i - 1].dte < tenor && pts[i].dte > tenor) {
        const w = (tenor - pts[i - 1].dte) / (pts[i].dte - pts[i - 1].dte);
        return pts[i - 1].v + w * (pts[i].v - pts[i - 1].v);
      }
    }
    return null;
  };
  return {
    labels: snapshots.map(s => s.label),
    rows: tenors.map(tenor => ({
      tenor,
      atm: snapshots.map(s => round(at(s.expiries, s.atMs, twoSidedAtm, tenor))),
      rr25: snapshots.map(s => round(at(s.expiries, s.atMs, riskReversal25, tenor))),
    })),
  };
}

// curve: { rows: buildTermCurve(...), history: buildTermHistory(...) | null }
function formatTermCurveForAdvisor(curve) {
  const { rows, history } = curve || {};
  if (!rows?.length) return 'Term curve unavailable (no recent full-chain smile).';
  const f = (v, signed = false) => (v == null ? 'n/a' : `${signed && v > 0 ? '+' : ''}${v}`);
  const grid = history?.rows?.length ? [
    '',
    `Curve history at fixed maturities (interpolated between listed expiries; n/a = no snapshot or outside the listed range). Columns: ${history.labels.join(', ')}.`,
    `tenor | ATM IV ${history.labels.join(' / ')} | 25Δ RR ${history.labels.join(' / ')}`,
    ...history.rows.map(r => `${r.tenor}d | ${r.atm.map(v => f(v)).join(' / ')} | ${r.rr25.map(v => f(v, true)).join(' / ')}`),
  ] : [];
  return [
    'Raw shape, no percentiles: every listed expiry, front to back. ATM = mean of the put and call nearest 50Δ. 25Δ RR = call IV − put IV (negative: puts richer). Δ24h compares the same expiry about 24h earlier (n/a if not listed then). Contango = ATM rising with DTE; front above back = stress.',
    'expiry | dte | ATM IV | ATM Δ24h | 25Δ RR | RR Δ24h',
    ...rows.map(r => `${r.expiry} | ${r.dte}d | ${f(r.atm_iv)} | ${f(r.atm_iv_d24h, true)} | ${f(r.rr25, true)} | ${f(r.rr25_d24h, true)}`),
    ...grid,
  ].join('\n');
}

module.exports = {
  ivAtAbsDelta, sideAtm, zoneMetrics, riskReversal25, realizedVol, percentile, pickZoneExpiry,
  buildSurfaceHistory, buildVolSurface, formatVolSurfaceForAdvisor, buildTermCurve, buildTermHistory, formatTermCurveForAdvisor,
};
