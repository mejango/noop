'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const {
  ivAtAbsDelta, sideAtm, realizedVol, percentile, pickZoneExpiry,
  buildSurfaceHistory, buildVolSurface, formatVolSurfaceForAdvisor, buildTermCurve, formatTermCurveForAdvisor,
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
  assert.equal(s.call_zone.atm_iv_pctl, null, '48h cannot support a 30d percentile');
  assert.equal(s.call_zone.rr25, 5);
  assert.equal(s.put_zone.atm_iv_pctl, null, '48h cannot support a 30d percentile');
  assert.equal(s.term_slope, 5);
  assert.ok(s.realized_vol.rv7d > 0);
  assert.equal(s.call_iv_minus_rv7d, +(45 - s.realized_vol.rv7d).toFixed(1));

  // History call ATM 24h ago ≈ 30 + 24 × 10/47 = 35.1; put ATM flat at 50; no sample 7d back.
  assert.equal(s.call_zone.atm_iv_move.d24h, +(45 - (30 + 24 * (10 / 47))).toFixed(1));
  assert.equal(s.call_zone.atm_iv_move.d7d, null, 'only 48h of history');
  assert.equal(s.put_zone.atm_iv_move.d24h, 0);

  const text = formatVolSurfaceForAdvisor(s);
  assert.match(text, /CALL zone 2026-10-02 \(6d\): 10Δ call IV 52\.5 \(pctl n\/a: 48h recorded, needs 168\) \[Δ24h \+11\.4, Δ7d n\/a\] \| ATM 45 \(pctl n\/a: 48h recorded, needs 168\) \[Δ24h \+9\.9, Δ7d n\/a\]/);
  assert.match(formatVolSurfaceForAdvisor(null), /unavailable/);
});

test('percentiles rank against all recorded history, and wings reach further back than ATM', () => {
  // 15 days of history (the put expiry stays inside the 45-78 DTE zone) with put 10Δ IV stepping
  // 60 → 74 a day at a time; only the last 8 days also carry an ATM quote, as smiles do.
  const rows = [];
  for (let h = 15 * 24; h >= 1; h--) {
    const ts = new Date(NOW - h * HOUR).toISOString();
    const wing = 60 + Math.floor((15 * 24 - h) / 24);
    rows.push({ timestamp: ts, expiry: putExpiry, option_type: 'P', delta: -0.12, implied_vol: (wing - 2) / 100 });
    rows.push({ timestamp: ts, expiry: putExpiry, option_type: 'P', delta: -0.08, implied_vol: (wing + 2) / 100 });
    if (h <= 8 * 24) rows.push({ timestamp: ts, expiry: putExpiry, option_type: 'P', delta: -0.47, implied_vol: 0.5 });
  }
  const history = buildSurfaceHistory(rows, [], zones);
  const current = { atMs: NOW, expiries: [{ expiry: putExpiry, points: [
    { type: 'P', delta: -0.47, iv: 50 }, { type: 'P', delta: -0.12, iv: 63 }, { type: 'P', delta: -0.08, iv: 67 },
  ] }] };
  const s = buildVolSurface({ current, spotRows: [], history, zones });
  // 65 is at or above the first 6 days (60..65) of 15.
  assert.equal(s.put_zone.wing10_iv, 65);
  assert.equal(s.put_zone.wing10_iv_pctl, 40);
  assert.equal(s.history_span.put_wing10.hours, 15 * 24);
  assert.equal(s.history_span.put_atm.hours, 8 * 24);
  assert.equal(s.put_zone.atm_iv_pctl, 100, 'flat ATM history: everything at or below');
  assert.match(formatVolSurfaceForAdvisor(s), /10Δ put IV 65 \(pctl 40 of 360h since 2026-09-11\)/);
});

test('production surface history loads the full record once, then appends only new completed hours', () => {
  const { loadProduction } = require('./helpers/load-production');
  const { buildSmileHistory } = require('../bot/market-evidence');
  const start = Date.parse('2026-09-26T00:00:00Z');
  const expiry = (start + 70 * DAY) / 1000;
  const smiles = [];
  for (let at = start; at <= Date.parse('2026-10-01T11:45:00Z'); at += 15 * 60000) {
    smiles.push({ timestamp: new Date(at).toISOString(), expiry, points: JSON.stringify([[1600, 0, -0.08, 0.7], [1500, 0, -0.12, 0.66]]) });
  }
  const smileQueries = [];
  let candidateLoads = 0;
  let now = Date.parse('2026-10-01T10:30:00Z');
  const { readAllSurfaceHistory } = loadProduction(['readAllSurfaceHistory'], { bindings: {
    Date: class extends Date { static now() { return now; } },
    buildSurfaceHistory, buildSmileHistory,
    db: {
      getSmileHistoryStart: () => new Date(start).toISOString(),
      getVolSurfaceHistoryRows: () => { candidateLoads++; return []; },
      getSpotHourlyCloses: () => [],
      getSmileHistoryRows: ({ since, before }) => {
        smileQueries.push({ since, before });
        // Same pick as HISTORY_SQL.smile: the last snapshot of each hour in range.
        const inRange = smiles.filter(r => r.timestamp >= since && r.timestamp <= before);
        const last = new Map(inRange.map(r => [r.timestamp.slice(0, 13), r.timestamp]));
        return inRange.filter(r => last.get(r.timestamp.slice(0, 13)) === r.timestamp);
      },
    },
  } });

  const first = readAllSurfaceHistory();
  assert.equal(first.length, 5 * 24 + 10, 'every completed hour from 09-26 00:00 through 10-01 09:00');
  assert.ok(first.every(h => h.put_wing10 != null));
  assert.equal(readAllSurfaceHistory(), first, 'same hour: no query, same rows');
  assert.equal(smileQueries.length, 1);

  now = Date.parse('2026-10-01T11:05:00Z');
  const second = readAllSurfaceHistory();
  assert.equal(second.length, first.length + 1);
  assert.deepEqual(smileQueries[1], { since: '2026-10-01T10:00:00.000Z', before: '2026-10-01T10:59:59.999Z' });
  assert.equal(candidateLoads, 1, 'pre-smile candidate history loads once');
});

test('term curve lists every live expiry front to back with same-expiry 24h changes', () => {
  const pts = (atm, put25, call25) => [
    { type: 'P', delta: -0.48, iv: atm + 1 }, { type: 'C', delta: 0.52, iv: atm - 1 },
    { type: 'P', delta: -0.2, iv: put25 - 1 }, { type: 'P', delta: -0.3, iv: put25 + 1 },
    { type: 'C', delta: 0.2, iv: call25 - 1 }, { type: 'C', delta: 0.3, iv: call25 + 1 },
  ];
  const expired = (NOW - DAY) / 1000;
  const rows = buildTermCurve([
    { expiry: putExpiry, points: pts(50, 55, 52) },
    { expiry: callExpiry, points: pts(39, 42, 40) },
    { expiry: expired, points: pts(30, 30, 30) },
  ], NOW, [{ expiry: callExpiry, points: pts(41, 43, 40) }]);
  assert.deepEqual(rows.map(r => r.dte), [6, 60]);
  assert.deepEqual(rows[0], { expiry: '2026-10-02', dte: 6, atm_iv: 39, atm_iv_d24h: -2, rr25: -2, rr25_d24h: 1 });
  assert.equal(rows[1].atm_iv_d24h, null);
  const text = formatTermCurveForAdvisor(rows);
  assert.match(text, /2026-10-02 \| 6d \| 39 \| -2 \| -2 \| \+1/);
  assert.match(text, /2026-11-25 \| 60d \| 50 \| n\/a \| -3 \| n\/a/);
  assert.match(formatTermCurveForAdvisor(null), /unavailable/);
});
