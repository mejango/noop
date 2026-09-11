'use strict';

// Preserve the configured quality ranking, then prefer better continuous PUT
// EDGE within the same quality bucket. Instrument name settles exact ties so
// the exchange's ticker insertion order cannot decide which option wins.
function isBetterBuyPutCandidate(candidate, incumbent) {
  if (!incumbent) return true;
  const qualityDifference = (candidate.selection_score ?? candidate.edge_score ?? candidate.score)
    - (incumbent.selection_score ?? incumbent.edge_score ?? incumbent.score);
  if (qualityDifference !== 0) return qualityDifference > 0;
  const edgeDifference = (candidate.edge_score ?? candidate.score)
    - (incumbent.edge_score ?? incumbent.score);
  if (edgeDifference !== 0) return edgeDifference > 0;
  return String(candidate.instrument) < String(incumbent.instrument);
}

const finiteNumber = (value) => {
  if (value == null || (typeof value === 'string' && !value.trim())) return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
};

// Interpolate only between observed call deltas at this exact expiry. A sparse
// or missing surface is unknown, rather than a cross-tenor substitute for skew.
function callIvAtDelta(rows, targetDelta) {
  let lower = null;
  for (const row of rows) {
    if (row.delta === targetDelta) return row.impliedVol;
    if (row.delta > targetDelta) {
      if (!lower) return null;
      const fraction = (targetDelta - lower.delta) / (row.delta - lower.delta);
      return lower.impliedVol + fraction * (row.impliedVol - lower.impliedVol);
    }
    lower = row;
  }
  return null;
}

function computeMatchedPutCallSkew(putRows = [], callRows = []) {
  const callsByExpiry = new Map();
  for (const row of callRows) {
    const delta = finiteNumber(row.delta);
    const impliedVol = finiteNumber(row.impliedVol);
    if (row.expiry == null || !(delta > 0 && delta < 1) || !(impliedVol > 0)) continue;
    const expiry = String(row.expiry);
    if (!callsByExpiry.has(expiry)) callsByExpiry.set(expiry, new Map());
    const byDelta = callsByExpiry.get(expiry);
    if (!byDelta.has(delta)) byDelta.set(delta, []);
    byDelta.get(delta).push(impliedVol);
  }
  const surfaces = new Map([...callsByExpiry].map(([expiry, byDelta]) => [
    expiry,
    [...byDelta].sort(([left], [right]) => left - right).map(([delta, vols]) => ({
      delta,
      // Duplicate delta observations must not make ticker order significant.
      impliedVol: vols.sort((left, right) => left - right).reduce((sum, iv) => sum + iv, 0) / vols.length,
    })),
  ]));

  const pairs = [];
  for (const row of putRows) {
    const delta = finiteNumber(row.delta);
    const putIv = finiteNumber(row.impliedVol);
    if (row.expiry == null || !(delta < 0 && delta > -1) || !(putIv > 0)) continue;
    const expiry = String(row.expiry);
    const callIv = callIvAtDelta(surfaces.get(expiry) || [], Math.abs(delta));
    if (callIv == null) continue;
    pairs.push({ expiry, delta, putIv, callIv });
  }
  pairs.sort((left, right) => left.expiry.localeCompare(right.expiry)
    || left.delta - right.delta || left.putIv - right.putIv || left.callIv - right.callIv);
  const mean = (field) => pairs.length
    ? pairs.reduce((sum, pair) => sum + pair[field], 0) / pairs.length
    : null;
  const putIv = mean('putIv');
  const callIv = mean('callIv');
  return {
    skew: pairs.length ? putIv - callIv : null,
    putIv,
    callIv,
    matchedPairs: pairs.length,
    matchedExpiries: new Set(pairs.map((pair) => pair.expiry)).size,
    unmatchedPuts: putRows.length - pairs.length,
  };
}

module.exports = { isBetterBuyPutCandidate, computeMatchedPutCallSkew };
