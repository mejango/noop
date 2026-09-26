'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const {
  ivAtAbsDelta, sideAtm, realizedVol, percentile, pickZoneExpiry,
  buildSurfaceHistory, buildVolSurface, formatVolSurfaceForAdvisor,
} = require('../bot/vol-surface');

const HOUR = 3_600_000;
const DAY = 24 * HOUR;
const NOW = Date.parse('2026-09-26T12:00:00Z');
const zones = { call_dte_range: [5, 12], put_dte_range: [45, 78] };
const callExpiry = (NOW + 6 * DAY) / 1000;
const putExpiry = (NOW + 60 * DAY) / 1000;

test('realized vol annualizes hourly log-return stdev', () => {
  const closes = Array.from({ length: 49 }, (_, i) => 100 * Math.exp(i % 2 ? 0.01 : 0));
  const rv = realizedVol(closes);
  // Alternating ±1% log returns: stdev ≈ 0.01 → 0.01 · √8760 ≈ 93.6 vol points (sample stdev, n=48).
  assert.ok(Math.abs(rv - 0.01 * Math.sqrt(48 / 47) * Math.sqrt(8760) * 100) < 1e-9);
  assert.equal(realizedVol([100, 101]), null, 'too few returns');
});

test('wing and ATM reads refuse to extrapolate or guess', () => {
  const pts = [{ type: 'C', delta: 0.45, iv: 40 }, { type: 'C', delta: 0.2, iv: 44 }, { type: 'C', delta: 0.05, iv: 55 }];
  assert.equal(sideAtm(pts, 'C'), 40);
  assert.equal(sideAtm([{ type: 'C', delta: 0.2, iv: 44 }], 'C'), null, 'no quote near 50Δ');
  assert.ok(Math.abs(ivAtAbsDelta(pts, 'C', 0.10) - (55 - (0.05 / 0.15) * 11)) < 1e-9);
  assert.equal(ivAtAbsDelta(pts, 'C', 0.02), null);
  assert.equal(percentile(5, [1, 2, 3]), null, 'too few samples');
  assert.equal(percentile(5, Array.from({ length: 40 }, (_, i) => i)), 15);
});

test('zone expiry is the nearest one inside the DTE window', () => {
  const exps = [(NOW + 2 * DAY) / 1000, callExpiry, (NOW + 13 * DAY) / 1000];
  assert.equal(pickZoneExpiry(exps, NOW, [5, 12]).expiry, callExpiry);
  assert.equal(pickZoneExpiry([(NOW + 20 * DAY) / 1000], NOW, [5, 12]), null);
});

test('surface compares live zones against their own hourly history and realized vol', () => {
  // 48 hourly history samples with call ATM IV drifting 30 → 40 and put ATM flat at 50.
  const rows = [];
  for (let h = 48; h >= 1; h--) {
    const ts = new Date(NOW - h * HOUR).toISOString();
    const callAtm = 30 + (48 - h) * (10 / 47);
    rows.push({ timestamp: ts, expiry: callExpiry, option_type: 'C', delta: 0.48, implied_vol: callAtm / 100 });
    rows.push({ timestamp: ts, expiry: callExpiry, option_type: 'C', delta: 0.05, implied_vol: (callAtm + 8) / 100 });
    rows.push({ timestamp: ts, expiry: callExpiry, option_type: 'C', delta: 0.15, implied_vol: (callAtm + 4) / 100 });
    rows.push({ timestamp: ts, expiry: putExpiry, option_type: 'P', delta: -0.47, implied_vol: 0.5 });
  }
  const spotRows = Array.from({ length: 24 * 38 }, (_, i) => ({
    hour: new Date(NOW - (24 * 38 - i) * HOUR).toISOString(), close: 2000 * Math.exp((i % 2 ? 1 : -1) * 0.002),
  }));
  const history = buildSurfaceHistory(rows, spotRows, zones);
  assert.equal(history.length, 48);
  assert.ok(history.every(h => h.term_slope > 0));

  const current = {
    atMs: NOW,
    expiries: [
      { expiry: callExpiry, points: [
        { type: 'P', delta: -0.25, iv: 41 }, { type: 'C', delta: 0.48, iv: 45 }, { type: 'C', delta: 0.25, iv: 46 },
        { type: 'C', delta: 0.15, iv: 49 }, { type: 'C', delta: 0.05, iv: 56 },
      ] },
      { expiry: putExpiry, points: [{ type: 'P', delta: -0.47, iv: 50 }, { type: 'P', delta: -0.05, iv: 62 }, { type: 'P', delta: -0.2, iv: 54 }] },
    ],
  };
  const s = buildVolSurface({ current, spotRows, history, zones });
  assert.equal(s.call_zone.expiry, '2026-10-02');
  assert.equal(s.call_zone.atm_iv, 45);
  assert.equal(s.call_zone.atm_iv_pctl, 100, 'above every sample of the drifting history');
  assert.equal(s.call_zone.rr25, 5);
  assert.equal(s.put_zone.atm_iv_pctl, 100, 'ties count as at-or-below');
  assert.equal(s.term_slope, 5);
  assert.ok(s.realized_vol.rv7d > 0);
  assert.equal(s.call_iv_minus_rv7d, +(45 - s.realized_vol.rv7d).toFixed(1));

  const text = formatVolSurfaceForAdvisor(s);
  assert.match(text, /CALL zone 2026-10-02 \(6d\): ATM 45 \(pctl 100\)/);
  assert.match(text, /n=48/);
  assert.match(formatVolSurfaceForAdvisor(null), /unavailable/);
});
