#!/usr/bin/env node
'use strict';

// Does "bid minus fair value at forecast realized vol" pick better call sales than the rolling
// high-score gate? Parameters are chosen on the earlier 70% of history only; the final 30% is
// opened once, for the chosen parameters and the baselines.
//
//   DB_PATH=/private/tmp/noop-research.db node scripts/study-call-fair-value.js

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const strategyFacts = require('../bot/strategy-facts.json');
const botConfig = require('../bot/config.json');
const {
  loadHistoricalFrames, runBacktest, makeCurrentEdgePolicy, makeRollingBestPolicy, makeFairValuePolicy,
} = require('../research/sell-call-backtest');

const HOUR_MS = 3_600_000;
const WARMUP_HOURS = 24 * 8; // covers the 7d realized vol and the 6.2d rolling window
const SELECT_SHARE = 0.7;

const dbPath = process.env.DB_PATH || '/private/tmp/noop-research.db';
const outPath = path.join(__dirname, '..', 'data', 'call-fair-value-study.json');
const mdPath = outPath.replace(/\.json$/, '.md');

// Production-like overlay: 45% cap, ~1/3-cap tranches, up to three rungs, 60% capture, no stops.
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

function run(frames, startIndex, endIndex, makePolicy) {
  const policy = makePolicy();
  // Seed rolling state with the hours before the split so no policy starts blind.
  const warmFrom = Math.max(0, startIndex - WARMUP_HOURS);
  if (typeof policy.onFrame === 'function') for (let i = warmFrom; i < startIndex; i++) policy.onFrame(frames[i]);
  const r = runBacktest(frames.slice(startIndex, endIndex), policy, SIM);
  return {
    policy: r.policy,
    overlay_pnl: +r.overlay_pnl.toFixed(2),
    realized_call_pnl: +r.realized_call_pnl.toFixed(2),
    trades: r.trades,
    win_rate: r.win_rate == null ? null : +r.win_rate.toFixed(3),
    worst_trade: r.trade_log.length ? +Math.min(...r.trade_log.map((t) => t.pnl)).toFixed(2) : null,
    tail_losses: r.tail_losses,
    max_drawdown: +r.max_drawdown.toFixed(4),
    premium: +r.total_premium_received.toFixed(2),
  };
}

function main() {
  const db = new Database(dbPath, { readonly: true, fileMustExist: true });
  console.log('Loading hourly frames…');
  const { frames, window } = loadHistoricalFrames(db, { days: 'all', cadenceHours: 1 });
  db.close();
  const cut = Math.floor(frames.length * SELECT_SHARE);
  const split = frames[cut].timestamp;
  console.log(`${frames.length} frames ${window.from} → ${window.to}; selection < ${split} ≤ holdout`);

  const baselines = {
    current_edge: () => makeCurrentEdgePolicy(),
    rolling_best_p80: () => makeRollingBestPolicy({ minPercentile: 80 }),
    rolling_best_fresh: () => makeRollingBestPolicy({ minPercentile: 100 }),
  };
  const grid = [];
  for (const forecast of ['rv3', 'rv7', 'max']) {
    for (const rank of ['edge', 'ratio']) {
      for (const minEdge of [0, 1, 2, 4]) grid.push({ forecast, rank, minEdge });
    }
  }

  const selection = {};
  for (const [name, make] of Object.entries(baselines)) {
    selection[name] = run(frames, WARMUP_HOURS, cut, make);
    console.log('select', name, selection[name].overlay_pnl);
  }
  const gridResults = grid.map((g) => {
    const r = run(frames, WARMUP_HOURS, cut, () => makeFairValuePolicy(g));
    console.log('select', r.policy, r.overlay_pnl);
    return { params: g, ...r };
  });
  // Choose by selection-period overlay P&L; ties go to the higher edge floor (fewer, surer trades).
  const chosen = [...gridResults].sort((a, b) => b.overlay_pnl - a.overlay_pnl || b.params.minEdge - a.params.minEdge)[0];

  const holdout = {};
  for (const [name, make] of Object.entries(baselines)) holdout[name] = run(frames, cut, frames.length, make);
  holdout.fair_value_chosen = run(frames, cut, frames.length, () => makeFairValuePolicy(chosen.params));
  console.log('holdout', holdout);

  const report = {
    generated_at: new Date().toISOString(),
    window, split_at: split, frames: frames.length,
    simulation: SIM,
    selection: { baselines: selection, fair_value_grid: gridResults, chosen: chosen.params },
    holdout,
  };
  fs.writeFileSync(outPath, `${JSON.stringify(report, null, 2)}\n`);
  const row = (name, r) => `| ${name} | ${r.overlay_pnl} | ${r.realized_call_pnl} | ${r.trades} | ${r.win_rate ?? '—'} | ${r.worst_trade ?? '—'} | ${r.tail_losses} | ${(r.max_drawdown * 100).toFixed(1)}% |`;
  const head = '| policy | overlay P&L $ | realized call P&L $ | trades | win rate | worst trade $ | tail losses | max DD |\n|---|---|---|---|---|---|---|---|';
  const md = [
    '# Call fair-value study',
    '',
    `History ${window.from} → ${window.to}, hourly frames. Selection before ${split}; holdout after, opened once.`,
    `Overlay: ${STARTING_ETH} ETH, ${SIM.callExposureCap * 100}% cap, tranches of ${SIM.maxContracts.toFixed(2)}, up to ${SIM.maxOpenPositions} open, ${SIM.profitCapturePct * 100}% capture, $${SIM.takerFeePerContract} taker per buyback.`,
    '',
    `Chosen on selection: forecast=${chosen.params.forecast}, rank=${chosen.params.rank}, min edge=$${chosen.params.minEdge}.`,
    '',
    '## Holdout',
    head,
    ...Object.entries(holdout).map(([n, r]) => row(n, r)),
    '',
    '## Selection period',
    head,
    ...Object.entries(selection).map(([n, r]) => row(n, r)),
    ...gridResults.map((r) => row(r.policy, r)),
    '',
  ].join('\n');
  fs.writeFileSync(mdPath, md);
  console.log(`\n${md}`);
}

main();
