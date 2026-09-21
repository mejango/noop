#!/usr/bin/env node
'use strict';

// Labels trade_reviews prose with System One judgments (TypeSafe/Jev) so the
// qualitative record becomes numeric columns joinable to realized P&L.
// Offline research only: never touches live trading.
//
// Usage: TYPESAFE_API_KEY=... node scripts/research-review-judgments.js [--db=/path/noop.db] [--out=data/review-judgments.json] [--limit=N]
// Re-runs skip reviews already present in --out (delete the file to relabel).

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const { noul, score, TypeSafeClient } = require('@typesafe-ai/sdk');

const args = Object.fromEntries(process.argv.slice(2).filter((a) => a.startsWith('--')).map((a) => {
  const eq = a.indexOf('=');
  return eq === -1 ? [a.slice(2), true] : [a.slice(2, eq), a.slice(eq + 1)];
}));
const dbPath = args.db || process.env.DB_PATH || path.join(process.env.DATA_DIR || path.join(__dirname, '..', 'data'), 'noop.db');
const outPath = args.out || path.join(__dirname, '..', 'data', 'review-judgments.json');
const limit = Number(args.limit) || Infinity;

const PRESENCE = { true: 'The review states this or clearly implies it', false: 'The review gives no indication of this' };

// Hand-written first round. The cookbook's loop gets most of its gain from the
// first proposal, so start here and add questions once the split table shows
// which dimensions move capture.
const QUESTIONS = {
  thesis_intact_at_close: noul('Was the original thesis for the position still intact when it was closed?', PRESENCE),
  exit_driven_by_drawdown: noul('Was the close driven by mark-to-market drawdown or discomfort rather than a rule, expiry, or a change in thesis?', PRESENCE),
  held_to_plan: noul('Was the position held through expiry or to its planned exit rather than closed early?', PRESENCE),
  strike_too_close: noul('Does the review attribute the outcome to the strike being too close to spot?', PRESENCE),
  entry_timing_blamed: noul('Does the review attribute the outcome mainly to entry timing?', PRESENCE),
  regime_misread: noul('Does the review say the regime or momentum was misjudged at entry?', PRESENCE),
  execution_friction: noul('Does the review report execution problems such as partial fills, zero fills, slippage, or repeated repricing?', PRESENCE),
  good_decision_regardless: noul('Does the review judge the decision itself as sound regardless of how the outcome turned out?', PRESENCE),
  premium_at_entry: score('How does the review characterize the premium or implied volatility available at entry?', [
    'Not discussed',
    'Cheap or thin premium',
    'Fair or unremarkable premium',
    'Rich or elevated premium',
  ]),
  momentum_at_entry: score('What spot momentum does the review describe at entry?', [
    'Downward',
    'Flat or neutral',
    'Upward',
  ]),
  attribution_confidence: score('How confidently does the review attribute the outcome to a specific cause?', [
    'No cause attributed',
    'Tentative or hedged attribution',
    'Confident, specific attribution',
  ]),
};

const expectedLevel = (answer) => Object.entries(answer.probabilities)
  .reduce((sum, [level, p]) => sum + Number(level) * p, 0);

const featuresFrom = (answers) => {
  const row = {};
  for (const [key, answer] of Object.entries(answers)) {
    if (answer.type === 'noul') row[key] = answer.noul;
    else { row[key] = expectedLevel(answer); row[`${key}_confidence`] = answer.confidence; }
  }
  return row;
};

const capturePct = (review) => {
  if (review.pnl_realized == null || !(review.premium_opened > 0)) return null;
  return (review.pnl_realized / review.premium_opened) * 100;
};

async function main() {
  if (!process.env.TYPESAFE_API_KEY) throw new Error('TYPESAFE_API_KEY not configured');
  const db = new Database(dbPath, { readonly: true });
  const reviews = db.prepare(`
    SELECT id, instrument_name, action_family, opened_at, closed_at, review_status, review_confidence,
           summary, lessons, pnl_realized, premium_opened, premium_closed, spot_open, spot_close,
           spot_min_while_open, spot_max_while_open
    FROM trade_reviews r WHERE is_active = 1
      -- one review per campaign: the longest window has the most hindsight
      AND review_window_days = (SELECT MAX(review_window_days) FROM trade_reviews
                                WHERE instrument_name = r.instrument_name AND closed_at = r.closed_at AND is_active = 1)
    ORDER BY closed_at
  `).all();
  db.close();

  const existing = fs.existsSync(outPath) ? JSON.parse(fs.readFileSync(outPath, 'utf8')) : [];
  const done = new Set(existing.map((row) => row.review_id));
  const pending = reviews.filter((review) => !done.has(review.id)).slice(0, limit);
  console.log(`reviews: ${reviews.length} total, ${done.size} labeled, ${pending.length} to label (db: ${dbPath})`);

  const client = new TypeSafeClient();
  const rows = [...existing];
  let tokens = 0;
  for (const review of pending) {
    const { answers, usage } = await client.systemOne({
      state: {
        action_family: review.action_family,
        instrument: review.instrument_name,
        review_status: review.review_status,
        summary: review.summary,
        lessons: review.lessons || '',
      },
      questions: QUESTIONS,
    });
    tokens += usage.input_tokens;
    rows.push({
      review_id: review.id,
      instrument_name: review.instrument_name,
      action_family: review.action_family,
      closed_at: review.closed_at,
      review_status: review.review_status,
      pnl_realized: review.pnl_realized,
      premium_opened: review.premium_opened,
      capture_pct: capturePct(review),
      spot_move_pct: review.spot_open > 0 && review.spot_close != null ? ((review.spot_close - review.spot_open) / review.spot_open) * 100 : null,
      ...featuresFrom(answers),
    });
    fs.writeFileSync(outPath, `${JSON.stringify(rows, null, 2)}\n`); // checkpoint per review; a crash loses nothing billed
    process.stdout.write(`labeled #${review.id} ${review.instrument_name} (${review.action_family})\n`);
  }
  console.log(`input tokens this run: ${tokens}`);
  fs.writeFileSync(outPath.replace(/\.json$/, '.md'), renderSplits(rows));
  console.log(`wrote ${outPath} and ${outPath.replace(/\.json$/, '.md')}`);
}

// For each feature: mean P&L and capture when the judgment holds vs not.
// Two-group split, not a model: at tens of reviews that is the honest tool.
// ponytail: swap for the correlation engine's feature ranking once n is in the hundreds.
function renderSplits(rows) {
  const mean = (values) => (values.length ? values.reduce((a, b) => a + b, 0) / values.length : null);
  const fmt = (value) => (value == null ? '—' : value.toFixed(1));
  const lines = ['# Trade review judgments', '', `Reviews labeled: ${rows.length}`, ''];
  for (const family of [...new Set(rows.map((row) => row.action_family))]) {
    const group = rows.filter((row) => row.action_family === family);
    lines.push(`## ${family} (n=${group.length})`, '', '| feature | split | n | mean pnl | mean capture % | Δ pnl |', '|---|---|---|---|---|---|');
    for (const [key, question] of Object.entries(QUESTIONS)) {
      const isNoul = question.type === 'noul';
      const cut = isNoul ? 0.5 : (question.criteria.length - 1) / 2;
      const yes = group.filter((row) => row[key] != null && row[key] >= cut);
      const no = group.filter((row) => row[key] != null && row[key] < cut);
      const pnl = (set) => mean(set.map((row) => row.pnl_realized).filter((v) => v != null));
      const cap = (set) => mean(set.map((row) => row.capture_pct).filter((v) => v != null));
      const delta = pnl(yes) != null && pnl(no) != null ? pnl(yes) - pnl(no) : null;
      lines.push(`| ${key} | ${isNoul ? 'yes' : `≥${cut}`} | ${yes.length} | ${fmt(pnl(yes))} | ${fmt(cap(yes))} | ${fmt(delta)} |`);
      lines.push(`| | ${isNoul ? 'no' : `<${cut}`} | ${no.length} | ${fmt(pnl(no))} | ${fmt(cap(no))} | |`);
    }
    lines.push('');
  }
  return `${lines.join('\n')}\n`;
}

main().catch((error) => { console.error(error.message); process.exit(1); });
