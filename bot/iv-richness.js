'use strict';

// Per-strike IV richness: where each candidate's fill-side IV sits within that same strike's own
// recent history. Raw scores (bid/delta, delta/ask) drift with theta as an option ages and jump when
// the DTE window rolls to a new expiry; IV does neither, so "is this strike rich/cheap vs itself"
// is comparable across instruments. Ranking among qualifying candidates stays with CALL/PUT EDGE.
// History comes from iv_smile_snapshots (bot/iv-smile.js tuple layout:
//   [strike, isCall, delta, iv, bidIv, askIv, oi]).

const MIN_RICHNESS_SAMPLES = 24; // 6h of 15-minute snapshots
const RICHNESS_QUALIFY_PERCENTILE = 80;

const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : null; };
const round = (v, d = 1) => (v == null ? null : Number(v.toFixed(d)));

// side 'bid' sells (calls): higher IV is better. side 'ask' buys (puts): lower IV is better.
const IV_INDEX = { bid: 4, ask: 5 };

function strikeIvHistory(smileRows, { expiry, strike, isCall, side }) {
  const out = [];
  for (const row of smileRows || []) {
    if (Number(row.expiry) !== Number(expiry)) continue;
    let points;
    try { points = typeof row.points === 'string' ? JSON.parse(row.points) : row.points; } catch { continue; }
    const p = (points || []).find(pt => Number(pt[0]) === Number(strike) && Boolean(pt[1]) === Boolean(isCall));
    const iv = num(p?.[IV_INDEX[side]]);
    if (iv > 0) out.push(iv);
  }
  return out;
}

// value_percentile: 100 = best this strike has been for us in the window, 0 = worst.
function assessStrike(history, currentIv, side) {
  if (!(currentIv > 0) || history.length < MIN_RICHNESS_SAMPLES) {
    return { samples: history.length, value_percentile: null, iv_vs_median_pts: null };
  }
  const beaten = history.filter(iv => (side === 'bid' ? iv <= currentIv : iv >= currentIv)).length;
  const sorted = [...history].sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)];
  return {
    samples: history.length,
    value_percentile: round(beaten / history.length * 100),
    iv_vs_median_pts: round((currentIv - median) * 100, 2),
  };
}

// candidates: [{ instrument, expiry (sec), strike, isCall, iv (current fill-side IV), edge_score }]
// Gate on richness vs the strike's own history, then pick the highest EDGE among those that pass.
function assessRichness(candidates, smileRows, side, qualifyPercentile = RICHNESS_QUALIFY_PERCENTILE) {
  const assessed = (candidates || []).map(c => ({
    instrument: c.instrument,
    edge_score: c.edge_score,
    current_iv_pct: round(c.iv > 0 ? c.iv * 100 : null, 2),
    ...assessStrike(strikeIvHistory(smileRows, { ...c, side }), c.iv, side),
  }));
  const measured = assessed.filter(c => c.value_percentile != null);
  const qualified = measured.filter(c => c.value_percentile >= qualifyPercentile);
  const byEdge = (a, b) => b.edge_score - a.edge_score;
  const richest = [...measured].sort((a, b) => b.value_percentile - a.value_percentile)[0] || null;
  return {
    side,
    qualify_percentile: qualifyPercentile,
    min_samples: MIN_RICHNESS_SAMPLES,
    candidates: assessed.length,
    measured: measured.length,
    qualified: qualified.length,
    best_qualified: qualified.sort(byEdge)[0] || null,
    richest,
  };
}

module.exports = { assessRichness, strikeIvHistory, assessStrike, MIN_RICHNESS_SAMPLES, RICHNESS_QUALIFY_PERCENTILE };

if (require.main === module) {
  const assert = require('assert');
  const rows = Array.from({ length: 30 }, (_, i) => ({ expiry: 100, points: JSON.stringify([[3000, 1, 0.08, 0.6, 0.55 + i / 1000, 0.65, 0]]) }));
  const rich = assessRichness([{ instrument: 'C', expiry: 100, strike: 3000, isCall: true, iv: 0.60, edge_score: 5 }], rows, 'bid');
  assert.equal(rich.best_qualified.value_percentile, 100);
  const poor = assessRichness([{ instrument: 'C', expiry: 100, strike: 3000, isCall: true, iv: 0.54, edge_score: 5 }], rows, 'bid');
  assert.equal(poor.qualified, 0);
  const thin = assessRichness([{ instrument: 'C', expiry: 100, strike: 3000, isCall: true, iv: 0.6, edge_score: 5 }], rows.slice(0, 5), 'bid');
  assert.equal(thin.measured, 0);
  const cheapPut = assessRichness([{ instrument: 'P', expiry: 100, strike: 3000, isCall: false, iv: 0.40, edge_score: 1 }],
    rows.map(r => ({ ...r, points: r.points.replace('[3000,1,', '[3000,0,') })), 'ask');
  assert.equal(cheapPut.best_qualified.value_percentile, 100);
  console.log('iv-richness ok');
}
