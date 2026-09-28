'use strict';

// Sell-call value as money, not rank: what the bid pays above the call's fair value if ETH
// moves at the vol we expect it to realize. A seller's profit is implied minus realized vol,
// so this is the edge itself. Fair value already prices time to expiry, so no DTE fudge.
// Black-Scholes with zero rates; for 5-12 DTE the funding/forward difference is negligible.

const DAY_MS = 86_400_000;
const YEAR_DAYS = 365; // ETH trades around the clock; matches realizedVol's 24*365 hours

function normCdf(x) {
  // Abramowitz-Stegun 26.2.17, |error| < 7.5e-8
  const t = 1 / (1 + 0.2316419 * Math.abs(x));
  const d = 0.3989422804014327 * Math.exp(-x * x / 2);
  const p = d * t * (0.31938153 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
  return x >= 0 ? 1 - p : p;
}

// vol as a decimal (0.5 = 50%), years to expiry.
function bsCall(spot, strike, years, vol) {
  if (!(spot > 0) || !(strike > 0)) return null;
  if (!(years > 0) || !(vol > 0)) return Math.max(spot - strike, 0);
  const sd = vol * Math.sqrt(years);
  const d1 = (Math.log(spot / strike) + sd * sd / 2) / sd;
  return spot * normCdf(d1) - strike * normCdf(d1 - sd);
}

// Bisection: call price is monotone in vol. Null when the price sits outside [intrinsic, 500% vol].
function impliedVol(price, spot, strike, years) {
  if (!(price > 0) || !(years > 0)) return null;
  let lo = 0.01, hi = 5;
  if (price < bsCall(spot, strike, years, lo) || price > bsCall(spot, strike, years, hi)) return null;
  for (let i = 0; i < 60; i++) {
    const mid = (lo + hi) / 2;
    if (bsCall(spot, strike, years, mid) < price) lo = mid; else hi = mid;
  }
  return (lo + hi) / 2;
}

// rv: forecast realized vol in vol points (as realizedVol returns). Edge is dollars per contract.
function callFairEdge({ bid, spot, strike, dte, rv }) {
  const years = Number(dte) / YEAR_DAYS;
  if (!(bid > 0) || !(spot > 0) || !(strike > 0) || !(years > 0) || !(rv > 0)) return null;
  const fair = bsCall(spot, strike, years, rv / 100);
  const bidIv = impliedVol(bid, spot, strike, years);
  return {
    fair,
    edge: bid - fair,
    bid_iv: bidIv == null ? null : bidIv * 100,
    iv_minus_rv: bidIv == null ? null : bidIv * 100 - rv,
  };
}

module.exports = { DAY_MS, normCdf, bsCall, impliedVol, callFairEdge };

if (require.main === module) {
  const assert = require('assert');
  // Textbook: S=K=100, 1y, 20% vol, r=0 -> 7.9656
  assert.ok(Math.abs(bsCall(100, 100, 1, 0.2) - 7.9656) < 1e-3);
  const iv = impliedVol(bsCall(2700, 3100, 12 / 365, 0.5), 2700, 3100, 12 / 365);
  assert.ok(Math.abs(iv - 0.5) < 1e-6);
  const e = callFairEdge({ bid: 9, spot: 2700, strike: 3100, dte: 12, rv: 40 });
  assert.ok(e.edge > 0 && e.iv_minus_rv > 0, 'bid above 40%-vol fair value is positive edge');
  assert.equal(callFairEdge({ bid: 9, spot: 2700, strike: 3100, dte: 12, rv: null }), null);
  console.log('call-fair-value ok');
}
