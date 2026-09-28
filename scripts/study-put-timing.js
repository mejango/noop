#!/usr/bin/env node
'use strict';

// Does waiting for a rolling-window signal buy more crash protection per dollar than buying on a
// schedule? Every policy spends the same budget each 15-day period on the best PUT EDGE put.
// Measured at the fill: protection per dollar, never later P&L (one crash would decide that).
//
//   DB_PATH=/private/tmp/noop-research.db node scripts/study-put-timing.js

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const facts = require('../bot/strategy-facts.json');
const botConfig = require('../bot/config.json');
const { normalizeBuyPutScore } = require('../bot/put-score');
const { loadHistoricalFrames } = require('../research/sell-call-backtest');

const HOUR_MS = 3_600_000;
const DAY_MS = 24 * HOUR_MS;
const PERIOD_MS = botConfig.PERIOD_DAYS * DAY_MS;
const WINDOWS_DAYS = [3, 6.2, 14, 30]; // is 6.2 special?
const BUDGET = 100; // dollars per period, identical for every policy
const CRASH = 0.30;

const dbPath = process.env.DB_PATH || '/private/tmp/noop-research.db';
const outPath = path.join(__dirname, '..', 'data', 'put-timing-study.md');

// Best eligible put by production PUT EDGE, with the protection it buys per dollar.
function bestPut(frame) {
  let best = null;
  for (const o of frame.options) {
    if (!(o.instrument_name || '').endsWith('-P') || !(o.ask_price > 0) || !(o.expiry > 0)) continue;
    const dte = (o.expiry * 1000 - frame.timestamp_ms) / DAY_MS;
    if (dte < facts.put_dte_range[0] || dte > facts.put_dte_range[1]) continue;
    if (!(o.delta >= facts.put_delta_range[0] && o.delta <= facts.put_delta_range[1])) continue;
    const edge = normalizeBuyPutScore(Math.abs(o.delta) / o.ask_price, dte);
    if (!best || edge > best.edge) {
      best = {
        edge, ask: o.ask_price, delta: Math.abs(o.delta), dte, iv: o.implied_vol,
        // Payoff at expiry if spot fell 30% from here, per premium dollar.
        crashCover: Math.max(o.strike - frame.spot_price * (1 - CRASH), 0) / o.ask_price,
      };
    }
  }
  return best;
}

function main() {
  const db = new Database(dbPath, { readonly: true, fileMustExist: true });
  console.log('Loading hourly frames…');
  const { frames, window } = loadHistoricalFrames(db, { days: 'all', cadenceHours: 1 });
  db.close();
  const series = frames.map((f) => ({ t: f.timestamp_ms, best: f.spot_price > 0 ? bestPut(f) : null })).filter((x) => x.best);
  console.log(`${series.length} hours with an eligible put, ${window.from} → ${window.to}`);

  // Rolling-window percentile of the current best against the prior window (causal).
  const rolling = (windowMs) => {
    const out = [];
    let lo = 0;
    for (let i = 0; i < series.length; i++) {
      while (series[lo].t < series[i].t - windowMs) lo++;
      const prior = series.slice(lo, i).map((x) => x.best.edge);
      out.push(prior.length >= 24 ? {
        pct: prior.filter((e) => e <= series[i].best.edge).length / prior.length * 100,
        fresh: series[i].best.edge > Math.max(...prior),
      } : null);
    }
    return out;
  };
  const byWindow = Object.fromEntries(WINDOWS_DAYS.map((d) => [d, rolling(d * DAY_MS)]));
  const pctl = byWindow[6.2];

  const start = series[0].t + Math.max(...WINDOWS_DAYS) * DAY_MS; // all policies start once every window has history
  const periods = [];
  for (let p = start; p < series[series.length - 1].t; p += PERIOD_MS) {
    const idx = series.map((x, i) => i).filter((i) => series[i].t >= p && series[i].t < p + PERIOD_MS);
    if (idx.length >= 24 * 10) periods.push(idx); // skip thin periods (data gaps)
  }

  const policies = {
    period_start: (idx) => [[idx[0], 1]],
    daily: (idx) => {
      const days = new Map();
      for (const i of idx) { const d = Math.floor((series[i].t - series[idx[0]].t) / DAY_MS); if (!days.has(d)) days.set(d, i); }
      return [...days.values()].map((i) => [i, 1 / days.size]);
    },
    ...Object.fromEntries(WINDOWS_DAYS.map((d) => [`fresh_best_${d}d`, (idx) => [[idx.find((i) => byWindow[d][i]?.fresh) ?? idx[idx.length - 1], 1]]])),
    pctl80_6_2d: (idx) => [[idx.find((i) => pctl[i]?.pct >= 80) ?? idx[idx.length - 1], 1]],
    hindsight_best: (idx) => [[idx.reduce((a, b) => (series[b].best.edge > series[a].best.edge ? b : a)), 1]],
  };

  const score = (periodIdx) => Object.fromEntries(Object.entries(policies).map(([name, pick]) => {
    let dollars = 0, delta = 0, cover = 0, edgeW = 0, ivW = 0, ivD = 0, fallback = 0;
    for (const idx of periodIdx) {
      const buys = pick(idx);
      if (/fresh|pctl/.test(name) && buys[0][0] === idx[idx.length - 1]) fallback++;
      for (const [i, share] of buys) {
        const b = series[i].best, spend = BUDGET * share;
        dollars += spend;
        delta += spend / b.ask * b.delta;
        cover += spend * b.crashCover;
        edgeW += spend * b.edge;
        if (b.iv > 0) { ivW += spend * b.iv; ivD += spend; }
      }
    }
    return [name, {
      delta_per_100: +(delta / dollars * 100).toFixed(3),
      crash_payoff_per_dollar: +(cover / dollars).toFixed(2),
      avg_put_edge: +(edgeW / dollars).toFixed(6),
      avg_mark_iv: ivD ? +(ivW / ivD * 100).toFixed(1) : null,
      periods: periodIdx.length,
      signal_never_fired: /fresh|pctl/.test(name) ? fallback : null,
    }];
  }));

  const half = Math.floor(periods.length / 2);
  const results = { all: score(periods), first_half: score(periods.slice(0, half)), second_half: score(periods.slice(half)) };
  const table = (r) => [
    '| policy | ETH delta per $100 | payoff at −30% per $1 | avg PUT EDGE | avg mark IV | periods | signal never fired |',
    '|---|---|---|---|---|---|---|',
    ...Object.entries(r).map(([n, v]) => `| ${n} | ${v.delta_per_100} | ${v.crash_payoff_per_dollar} | ${v.avg_put_edge} | ${v.avg_mark_iv ?? '—'} | ${v.periods} | ${v.signal_never_fired ?? '—'} |`),
  ].join('\n');
  const md = [
    '# Put timing study',
    '',
    `Hourly history ${window.from} → ${window.to}. Each ${botConfig.PERIOD_DAYS}-day period spends $${BUDGET} on the best PUT EDGE put (${facts.put_dte_range.join('–')} DTE, delta ${facts.put_delta_range.join(' to ')}) at the ask. Signal policies buy at their first trigger, else at period end. hindsight_best looks ahead and is the ceiling for any timing rule.`,
    '',
    '## All periods', table(results.all), '',
    '## First half', table(results.first_half), '',
    '## Second half', table(results.second_half), '',
  ].join('\n');
  fs.writeFileSync(outPath, md);
  console.log(md);
}

main();
