#!/usr/bin/env node
'use strict';

// Recalibrate CALL EDGE's DTE exponent: edge = bid/|delta| * (8.5/DTE)^e.
// 1. Stability: how much the best edge jumps when a new weekly enters the window, and how
//    much it slides as that expiry ages. A calibrated exponent flattens both.
// 2. Trading: for each exponent, choose the floor on the first 70% of history, then open the
//    final 30% once and compare with production (e=0.12, floor 65).
//
//   DB_PATH=/private/tmp/noop-research.db node scripts/study-call-dte-exponent.js

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const strategyFacts = require('../bot/strategy-facts.json');
const botConfig = require('../bot/config.json');
const { SELL_CALL_EDGE_REFERENCE_DTE: REF } = require('../bot/call-score');
const { loadHistoricalFrames, runBacktest } = require('../research/sell-call-backtest');

const EXPONENTS = [0.12, 0.4, 0.5, 0.55, 0.6, 0.65, 0.7, 0.8];
const FLOORS = [45, 50, 55, 60, 65, 70, 75, 80, 90];
const MIN_BID = strategyFacts.sell_call_fallback_min_bid;
const STARTING_ETH = 5;
const SIM = {
  startingEth: STARTING_ETH,
  callExposureCap: botConfig.CALL_EXPOSURE_CAP_PCT,
  maxOpenPositions: 3,
  maxContracts: STARTING_ETH * botConfig.CALL_EXPOSURE_CAP_PCT * strategyFacts.sell_call_max_tranche_fraction,
  profitCapturePct: strategyFacts.call_buyback_profit_threshold_pct / 100,
  takerFeePerContract: 1.25,
  execution: 'bid_ask',
};
const outPath = path.join(__dirname, '..', 'data', 'call-dte-exponent-study.md');

const edgeOf = (c, e) => c.raw_score * Math.pow(REF / c.dte, e);

function policy(e, floor) {
  return {
    name: `edge_e${e}_floor${floor}`,
    select({ candidates }) {
      const best = candidates.filter((c) => c.bid_price >= MIN_BID)
        .map((c) => ({ c, edge: edgeOf(c, e) })).filter((x) => x.edge >= floor)
        .sort((a, b) => b.edge - a.edge)[0];
      return best ? { candidate: best.c, score: best.edge, model_version: 'dte-exponent-study' } : null;
    },
  };
}

// Best edge per hour and its expiry; rollover = the best's expiry moves to a later one.
function stability(frames, e) {
  const hours = frames.map((f) => {
    const best = f.candidates.filter((c) => c.bid_price >= MIN_BID)
      .map((c) => ({ edge: edgeOf(c, e), expiry: c.expiry, dte: c.dte })).sort((a, b) => b.edge - a.edge)[0];
    return best ? { t: f.timestamp_ms, ...best } : null;
  }).filter(Boolean);
  const jumps = [];
  const moves = [];
  for (let i = 1; i < hours.length; i++) {
    if (hours[i].t - hours[i - 1].t > 2 * 3_600_000) continue;
    const r = Math.abs(Math.log(hours[i].edge / hours[i - 1].edge));
    if (hours[i].expiry > hours[i - 1].expiry) jumps.push(hours[i].edge / hours[i - 1].edge);
    else moves.push(r);
  }
  // Slide: slope of log(edge) on log(DTE) within each expiry's reign as best (0 = no drift).
  const byExp = new Map();
  for (const h of hours) (byExp.get(h.expiry) || byExp.set(h.expiry, []).get(h.expiry)).push(h);
  const slopes = [];
  for (const pts of byExp.values()) {
    if (pts.length < 48) continue;
    const xs = pts.map((p) => Math.log(p.dte)), ys = pts.map((p) => Math.log(p.edge));
    const mx = xs.reduce((s, x) => s + x, 0) / xs.length, my = ys.reduce((s, y) => s + y, 0) / ys.length;
    const den = xs.reduce((s, x) => s + (x - mx) ** 2, 0);
    if (den > 0.02) slopes.push(xs.reduce((s, x, i) => s + (x - mx) * (ys[i] - my), 0) / den);
  }
  const med = (a) => (a.length ? [...a].sort((x, y) => x - y)[Math.floor(a.length / 2)] : null);
  return {
    rollovers: jumps.length,
    median_rollover_jump_pct: +((med(jumps) - 1) * 100).toFixed(1),
    typical_hourly_move_pct: +((Math.exp(med(moves)) - 1) * 100).toFixed(1),
    median_weekly_slide_slope: +med(slopes).toFixed(2),
  };
}

function sim(frames, e, floor) {
  const r = runBacktest(frames, policy(e, floor), SIM);
  return {
    overlay_pnl: +r.overlay_pnl.toFixed(2),
    trades: r.trades,
    worst: r.trade_log.length ? +Math.min(...r.trade_log.map((t) => t.pnl)).toFixed(2) : null,
    tail_losses: r.tail_losses,
    avg_entry_dte: r.trade_log.length ? +(r.trade_log.reduce((s, t) => s + (Date.parse(t.expiry) - Date.parse(t.opened_at)) / 86_400_000, 0) / r.trade_log.length).toFixed(1) : null,
  };
}

function main() {
  const db = new Database(process.env.DB_PATH || '/private/tmp/noop-research.db', { readonly: true, fileMustExist: true });
  console.log('Loading hourly frames…');
  const { frames, window } = loadHistoricalFrames(db, { days: 'all', cadenceHours: 1 });
  db.close();
  const cut = Math.floor(frames.length * 0.7);
  const selection = frames.slice(0, cut), holdout = frames.slice(cut);

  const rows = EXPONENTS.map((e) => {
    const grid = FLOORS.map((floor) => ({ floor, ...sim(selection, e, floor) }));
    // Highest selection P&L; ties to the higher floor (fewer, better-paid sales).
    const chosen = [...grid].sort((a, b) => b.overlay_pnl - a.overlay_pnl || b.floor - a.floor)[0];
    const hold = sim(holdout, e, chosen.floor);
    const stab = stability(frames, e);
    console.log(`e=${e}: floor ${chosen.floor}`, stab, hold);
    return { e, stab, grid, chosen, hold };
  });
  const prod = sim(holdout, 0.12, strategyFacts.sell_call_fallback_min_score);

  const md = [
    '# CALL EDGE DTE exponent study',
    '',
    `History ${window.from} → ${window.to}, hourly. edge = bid/|delta| × (${REF}/DTE)^e. Floors chosen on the first 70% (before ${frames[cut].timestamp}); holdout opened once. Overlay: ${STARTING_ETH} ETH, ${SIM.callExposureCap * 100}% cap, ${SIM.maxContracts.toFixed(2)}-contract tranches, ${SIM.maxOpenPositions} open, ${SIM.profitCapturePct * 100}% capture, $${SIM.takerFeePerContract} taker, bid ≥ $${MIN_BID}.`,
    '',
    '## Stability of the best CALL EDGE',
    '| exponent | weekly rollovers | median jump at rollover | typical hourly move | weekly slide slope (0 = flat) |',
    '|---|---|---|---|---|',
    ...rows.map((r) => `| ${r.e} | ${r.stab.rollovers} | ${r.stab.median_rollover_jump_pct}% | ${r.stab.typical_hourly_move_pct}% | ${r.stab.median_weekly_slide_slope} |`),
    '',
    '## Holdout (floor chosen on selection)',
    '| exponent | floor | overlay P&L $ | trades | worst trade $ | tail losses | avg DTE at entry |',
    '|---|---|---|---|---|---|---|',
    `| 0.12 (production, floor 65) | 65 | ${prod.overlay_pnl} | ${prod.trades} | ${prod.worst} | ${prod.tail_losses} | ${prod.avg_entry_dte} |`,
    ...rows.map((r) => `| ${r.e} | ${r.chosen.floor} | ${r.hold.overlay_pnl} | ${r.hold.trades} | ${r.hold.worst} | ${r.hold.tail_losses} | ${r.hold.avg_entry_dte} |`),
    '',
    '## Selection-period floor grid',
    ...rows.flatMap((r) => [`### e = ${r.e}`, '| floor | overlay P&L $ | trades | worst $ | avg DTE |', '|---|---|---|---|---|',
      ...r.grid.map((g) => `| ${g.floor} | ${g.overlay_pnl} | ${g.trades} | ${g.worst} | ${g.avg_entry_dte} |`), '']),
  ].join('\n');
  fs.writeFileSync(outPath, md);
  console.log(`\n${md}`);
}

main();
