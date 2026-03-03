# Hypothesis Feedback Loop Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a closed-loop system that tracks hypothesis outcomes with Spitznagel-aligned scoring, links them to trade P&L, and feeds performance data back into generation.

**Architecture:** Add structured metadata columns to `ai_journal`, a `hypothesis_lessons` table, a review cycle function in the bot loop, and performance summary injection into the generation prompt. Dashboard gets verdict badges + analytics tab.

**Tech Stack:** Node.js (bot), Next.js 14 (dashboard), better-sqlite3, recharts, Claude API (claude-sonnet-4-6)

---

### Task 1: Schema Migration — Add Hypothesis Metadata Columns

**Files:**
- Modify: `bot/db.js:112-114` (idempotent migrations section)
- Modify: `bot/db.js:277-280` (insertJournalEntry prepared statement)
- Modify: `bot/db.js:667-674` (insertJournalEntry helper function)

**Step 1: Add idempotent ALTER TABLE migrations**

In `bot/db.js` after line 114 (existing migrations), add:

```js
// Hypothesis tracking columns (idempotent)
try { db.exec('ALTER TABLE ai_journal ADD COLUMN prediction_target TEXT'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN prediction_direction TEXT'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN prediction_value REAL'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN prediction_deadline TEXT'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN falsification_criteria TEXT'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN outcome_status TEXT DEFAULT \'pending\''); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN outcome_verdict TEXT'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN outcome_confidence REAL'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN outcome_reviewed_at TEXT'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN trade_pnl_attribution REAL'); } catch {}
try { db.exec('ALTER TABLE ai_journal ADD COLUMN trades_in_window TEXT'); } catch {}
```

**Step 2: Create hypothesis_lessons table**

After the ALTER TABLE migrations, add:

```js
db.exec(`
  CREATE TABLE IF NOT EXISTS hypothesis_lessons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lesson TEXT NOT NULL,
    evidence_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    archived_at TEXT,
    is_active INTEGER NOT NULL DEFAULT 1
  );
`);
```

**Step 3: Add new prepared statements**

In `bot/db.js` in the `stmts` object (after `getRecentJournalEntries` around line 287), add:

```js
insertJournalEntryFull: db.prepare(`
  INSERT INTO ai_journal (timestamp, entry_type, content, series_referenced,
    prediction_target, prediction_direction, prediction_value, prediction_deadline, falsification_criteria)
  VALUES (@timestamp, @entry_type, @content, @series_referenced,
    @prediction_target, @prediction_direction, @prediction_value, @prediction_deadline, @falsification_criteria)
`),

getPendingHypotheses: db.prepare(`
  SELECT id, timestamp, content, prediction_target, prediction_direction,
    prediction_value, prediction_deadline, falsification_criteria
  FROM ai_journal
  WHERE entry_type = 'hypothesis'
    AND outcome_status = 'pending'
    AND prediction_deadline IS NOT NULL
    AND prediction_deadline < @now
  ORDER BY prediction_deadline ASC
  LIMIT @limit
`),

updateHypothesisVerdict: db.prepare(`
  UPDATE ai_journal SET
    outcome_status = @outcome_status,
    outcome_verdict = @outcome_verdict,
    outcome_confidence = @outcome_confidence,
    outcome_reviewed_at = @outcome_reviewed_at,
    trade_pnl_attribution = @trade_pnl_attribution,
    trades_in_window = @trades_in_window
  WHERE id = @id
`),

getReviewedHypotheses: db.prepare(`
  SELECT id, timestamp, content, prediction_target, prediction_direction,
    prediction_value, outcome_status, outcome_verdict, outcome_confidence,
    trade_pnl_attribution, outcome_reviewed_at
  FROM ai_journal
  WHERE entry_type = 'hypothesis'
    AND outcome_status != 'pending'
  ORDER BY outcome_reviewed_at DESC
  LIMIT @limit
`),

getHypothesisStats: db.prepare(`
  SELECT
    COUNT(*) as total,
    SUM(CASE WHEN outcome_status = 'confirmed_convex' THEN 1 ELSE 0 END) as confirmed_convex,
    SUM(CASE WHEN outcome_status = 'confirmed_linear' THEN 1 ELSE 0 END) as confirmed_linear,
    SUM(CASE WHEN outcome_status = 'disproven_bounded' THEN 1 ELSE 0 END) as disproven_bounded,
    SUM(CASE WHEN outcome_status = 'disproven_costly' THEN 1 ELSE 0 END) as disproven_costly,
    SUM(CASE WHEN outcome_status = 'partially_confirmed' THEN 1 ELSE 0 END) as partially_confirmed,
    SUM(CASE WHEN outcome_status = 'pending' THEN 1 ELSE 0 END) as pending
  FROM ai_journal
  WHERE entry_type = 'hypothesis'
    AND timestamp > @since
`),

getOrdersInWindow: db.prepare(`
  SELECT id, timestamp, action, instrument_name, filled_amount, fill_price,
    total_value, spot_price, success
  FROM orders
  WHERE timestamp BETWEEN @start AND @end
    AND success = 1
  ORDER BY timestamp ASC
`),

insertLesson: db.prepare(`
  INSERT INTO hypothesis_lessons (lesson, evidence_count)
  VALUES (@lesson, @evidence_count)
`),

getActiveLessons: db.prepare(`
  SELECT id, lesson, evidence_count, created_at
  FROM hypothesis_lessons
  WHERE is_active = 1
  ORDER BY created_at DESC
`),

archiveLesson: db.prepare(`
  UPDATE hypothesis_lessons SET is_active = 0, archived_at = datetime('now')
  WHERE id = @id
`),

countReviewedSinceLastLesson: db.prepare(`
  SELECT COUNT(*) as count
  FROM ai_journal
  WHERE entry_type = 'hypothesis'
    AND outcome_status != 'pending'
    AND outcome_reviewed_at > COALESCE(
      (SELECT MAX(created_at) FROM hypothesis_lessons), '1970-01-01')
`),
```

**Step 4: Add helper functions and exports**

After the existing `getRecentJournalEntries` helper (~line 676), add:

```js
const insertJournalEntryFull = (entryType, content, seriesReferenced = null, meta = null) => {
  stmts.insertJournalEntryFull.run({
    timestamp: new Date().toISOString(),
    entry_type: entryType,
    content,
    series_referenced: seriesReferenced ? JSON.stringify(seriesReferenced) : null,
    prediction_target: meta?.target || null,
    prediction_direction: meta?.direction || null,
    prediction_value: meta?.value != null ? Number(meta.value) : null,
    prediction_deadline: meta?.deadline || null,
    falsification_criteria: meta?.falsification || null,
  });
};

const getPendingHypotheses = (limit = 3) => {
  return stmts.getPendingHypotheses.all({ now: new Date().toISOString(), limit });
};

const updateHypothesisVerdict = (id, verdict) => {
  stmts.updateHypothesisVerdict.run({
    id,
    outcome_status: verdict.status,
    outcome_verdict: verdict.verdict,
    outcome_confidence: verdict.confidence,
    outcome_reviewed_at: new Date().toISOString(),
    trade_pnl_attribution: verdict.tradePnl != null ? verdict.tradePnl : null,
    trades_in_window: verdict.tradeIds ? JSON.stringify(verdict.tradeIds) : null,
  });
};

const getReviewedHypotheses = (limit = 30) => {
  return stmts.getReviewedHypotheses.all({ limit });
};

const getHypothesisStats = (sinceDays = 30) => {
  const since = new Date(Date.now() - sinceDays * 24 * 60 * 60 * 1000).toISOString();
  return stmts.getHypothesisStats.get({ since });
};

const getOrdersInWindow = (start, end) => {
  return stmts.getOrdersInWindow.all({ start, end });
};

const insertLesson = (lesson, evidenceCount) => {
  stmts.insertLesson.run({ lesson, evidence_count: evidenceCount });
};

const getActiveLessons = () => {
  return stmts.getActiveLessons.all();
};

const archiveLesson = (id) => {
  stmts.archiveLesson.run({ id });
};

const countReviewedSinceLastLesson = () => {
  return stmts.countReviewedSinceLastLesson.get()?.count || 0;
};
```

**Step 5: Update module.exports**

Add to the `module.exports` object at the bottom:

```js
insertJournalEntryFull,
getPendingHypotheses,
updateHypothesisVerdict,
getReviewedHypotheses,
getHypothesisStats,
getOrdersInWindow,
insertLesson,
getActiveLessons,
archiveLesson,
countReviewedSinceLastLesson,
```

**Step 6: Commit**

```bash
git add bot/db.js
git commit -m "feat: add hypothesis tracking schema and DB helpers"
```

---

### Task 2: Update Journal Generation — Structured Metadata Extraction

**Files:**
- Modify: `script.js:2019-2051` (system prompt)
- Modify: `script.js:2069-2085` (entry extraction logic)

**Step 1: Update the system prompt**

In `script.js`, update the system prompt (line 2019-2049) to add hypothesis metadata instructions. After the existing hypothesis instruction block (around line 2035), add to the hypothesis section:

```
2. Then, a HYPOTHESIS with a testable prediction:
<journal type="hypothesis">State what you expect to happen next based on the data, with a specific timeframe and falsification condition (e.g., "if X doesn't happen within Y hours, this hypothesis is wrong").

IMPORTANT: After your hypothesis prose, include a structured metadata block:
<hypothesis_meta>{"target":"ETH spot","direction":"below|above|within_range","value":2000.00,"deadline":"2026-03-04T01:39:00Z","falsification":"If price doesn't breach $2000 within 18h"}</hypothesis_meta>

The metadata must have:
- target: what you're predicting about (e.g. "ETH spot", "put cost", "liquidity flow")
- direction: "above", "below", or "within_range"
- value: the numeric threshold
- deadline: ISO timestamp for when to check
- falsification: plain text summary of what would disprove it

Every hypothesis MUST identify what makes the opportunity asymmetric — why is the downside bounded? Where is the cheap convexity?
</journal>
```

**Step 2: Update the extraction logic**

In `script.js`, update the extraction section (~line 2069-2085). Replace the while loop:

```js
const text = response.data?.content?.[0]?.text || '';

// Extract journal entries
const regex = /<journal\s+type="(observation|hypothesis|regime_note)">([\s\S]*?)<\/journal>/g;
const metaRegex = /<hypothesis_meta>([\s\S]*?)<\/hypothesis_meta>/;
const seriesNames = ['spot_return', 'liquidity_flow', 'best_put_dv', 'best_call_dv', 'options_spread', 'options_depth', 'open_interest', 'implied_vol'];
let match;
let count = 0;

while ((match = regex.exec(text)) !== null) {
  const entryType = match[1];
  let content = match[2].trim();
  if (!content) continue;

  const referenced = seriesNames.filter(s =>
    content.toLowerCase().includes(s.replace(/_/g, ' ')) || content.includes(s)
  );

  if (entryType === 'hypothesis') {
    // Extract structured metadata
    const metaMatch = content.match(metaRegex);
    let meta = null;
    if (metaMatch) {
      try {
        meta = JSON.parse(metaMatch[1].trim());
      } catch (e) {
        console.log('📓 Failed to parse hypothesis_meta JSON:', e.message);
      }
      // Strip meta tag from displayed content
      content = content.replace(metaRegex, '').trim();
    }
    db.insertJournalEntryFull(entryType, content, referenced.length > 0 ? referenced : null, meta);
  } else {
    db.insertJournalEntry(entryType, content, referenced.length > 0 ? referenced : null);
  }
  count++;
}
```

**Step 3: Commit**

```bash
git add script.js
git commit -m "feat: extract structured metadata from hypothesis entries"
```

---

### Task 3: Hypothesis Review Cycle

**Files:**
- Modify: `script.js` — add `reviewExpiredHypotheses()` function and call it from bot loop

**Step 1: Add the review function**

In `script.js`, before `generateJournalEntries` (~line 1862), add:

```js
// ─── Hypothesis Review Cycle ──────────────────────────────────────────────────

const reviewExpiredHypotheses = async () => {
  const pending = db.getPendingHypotheses(3); // max 3 per tick
  if (pending.length === 0) return;

  console.log(`🔍 Reviewing ${pending.length} expired hypothesis(es)...`);

  for (const hyp of pending) {
    try {
      // Gather actual market data from the hypothesis window
      const createdAt = hyp.timestamp;
      const deadline = hyp.prediction_deadline;
      const priceData = db.getRecentSpotPrices(createdAt)
        .filter(p => p.timestamp <= deadline)
        .reverse(); // chronological

      const ordersInWindow = db.getOrdersInWindow(createdAt, deadline);
      const totalPnl = ordersInWindow.reduce((sum, o) => {
        // Rough P&L: for puts bought, value is negative (cost); mark-to-market would need current prices
        // For now, track total_value spent/received
        const val = o.total_value || 0;
        return sum + (o.action === 'buy_put' ? -val : val);
      }, 0);

      const priceAtStart = priceData.length > 0 ? priceData[0].price : null;
      const priceAtEnd = priceData.length > 0 ? priceData[priceData.length - 1].price : null;
      const minPrice = priceData.length > 0 ? Math.min(...priceData.map(p => p.price)) : null;
      const maxPrice = priceData.length > 0 ? Math.max(...priceData.map(p => p.price)) : null;

      const reviewPrompt = `You are reviewing a past hypothesis for accuracy and risk quality.

## Hypothesis (ID #${hyp.id})
Created: ${createdAt}
Deadline: ${deadline}
Prediction: ${hyp.prediction_target} will go ${hyp.prediction_direction} ${hyp.prediction_value}
Falsification: ${hyp.falsification_criteria}

Content:
${hyp.content}

## What Actually Happened
Price at hypothesis time: $${priceAtStart}
Price at deadline: $${priceAtEnd}
Price range during window: $${minPrice} - $${maxPrice}
Data points: ${priceData.length}

## Trades During Window
${ordersInWindow.length > 0 ? ordersInWindow.map(o => `${o.timestamp}: ${o.action} ${o.instrument_name} amount=${o.filled_amount} price=${o.fill_price} value=$${o.total_value}`).join('\n') : 'No trades executed'}
Total P&L attribution: $${totalPnl.toFixed(4)}

## Scoring Instructions

Score this hypothesis using Spitznagel-aligned categories. The goal is NOT prediction accuracy — it's whether the hypothesis identified an asymmetric opportunity.

Categories:
- confirmed_convex: Prediction correct AND position/opportunity was asymmetric (bounded downside, convex upside)
- confirmed_linear: Prediction correct but the risk profile was symmetric
- disproven_bounded: Prediction wrong BUT loss was small/bounded — the strategy worked as intended
- disproven_costly: Prediction wrong AND the position was expensive (overpaid for insurance)
- partially_confirmed: Direction right but timing/magnitude was off

Output ONLY this JSON:
{"status":"<category>","confidence":<0-1>,"verdict":"<2-3 sentence explanation focusing on the risk profile, not just whether the price moved correctly>"}`;

      const response = await axios.post('https://api.anthropic.com/v1/messages', {
        model: process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-6',
        max_tokens: 512,
        messages: [{ role: 'user', content: reviewPrompt }],
      }, {
        headers: {
          'x-api-key': process.env.ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01',
          'content-type': 'application/json',
        },
        timeout: 30000,
      });

      const resultText = response.data?.content?.[0]?.text || '';
      // Extract JSON from response (may be wrapped in markdown code block)
      const jsonMatch = resultText.match(/\{[\s\S]*"status"[\s\S]*"verdict"[\s\S]*\}/);
      if (jsonMatch) {
        const result = JSON.parse(jsonMatch[0]);
        db.updateHypothesisVerdict(hyp.id, {
          status: result.status,
          verdict: result.verdict,
          confidence: result.confidence,
          tradePnl: totalPnl,
          tradeIds: ordersInWindow.map(o => o.id),
        });
        console.log(`📊 Hypothesis #${hyp.id}: ${result.status} (${(result.confidence * 100).toFixed(0)}%)`);
      } else {
        console.log(`📊 Hypothesis #${hyp.id}: failed to parse verdict`);
      }
    } catch (e) {
      console.log(`📊 Hypothesis #${hyp.id} review failed:`, e.message);
    }
  }
};
```

**Step 2: Hook into the bot loop**

In `script.js`, after the journal generation block (~line 2445), add:

```js
      // Review expired hypotheses each tick
      if (process.env.ANTHROPIC_API_KEY) {
        reviewExpiredHypotheses().catch(e => {
          console.log('📊 Hypothesis review failed:', e.message);
        });
      }
```

**Step 3: Commit**

```bash
git add script.js
git commit -m "feat: add hypothesis review cycle to bot loop"
```

---

### Task 4: Lesson Extraction

**Files:**
- Modify: `script.js` — add `extractHypothesisLessons()` function, call after reviews

**Step 1: Add the lesson extraction function**

In `script.js`, after `reviewExpiredHypotheses`, add:

```js
const extractHypothesisLessons = async () => {
  // Only run when 10+ hypotheses reviewed since last lesson extraction
  const reviewedCount = db.countReviewedSinceLastLesson();
  if (reviewedCount < 10) return;

  console.log(`🧠 Extracting lessons from ${reviewedCount} new hypothesis reviews...`);

  const reviewed = db.getReviewedHypotheses(50);
  const currentLessons = db.getActiveLessons();

  const prompt = `You are analyzing hypothesis review outcomes to extract actionable lessons for a Spitznagel-style tail-risk hedging bot.

## Reviewed Hypotheses (most recent first)
${reviewed.map(h => `#${h.id} [${h.outcome_status}] (confidence: ${h.outcome_confidence}) - ${h.content.slice(0, 150)}... VERDICT: ${h.outcome_verdict}`).join('\n\n')}

## Current Active Lessons
${currentLessons.length > 0 ? currentLessons.map(l => `- ${l.lesson} (evidence: ${l.evidence_count}, since: ${l.created_at})`).join('\n') : 'None yet'}

## Instructions
Analyze the pattern of outcomes. Key metric: convex posture rate = (confirmed_convex + disproven_bounded) / total.

Extract 3-5 actionable lessons about:
1. Which types of hypotheses produce the best risk profiles (high convex posture)
2. Which types to avoid (high disproven_costly rate)
3. What data signals are most predictive
4. Timing patterns (are shorter or longer windows better?)

For each existing lesson, say whether it still holds or should be archived.

Output JSON:
{"new_lessons":[{"lesson":"<text>","evidence_count":<number>}],"archive_ids":[<ids of lessons that no longer hold>]}`;

  try {
    const response = await axios.post('https://api.anthropic.com/v1/messages', {
      model: process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-6',
      max_tokens: 1024,
      messages: [{ role: 'user', content: prompt }],
    }, {
      headers: {
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
      },
      timeout: 30000,
    });

    const text = response.data?.content?.[0]?.text || '';
    const jsonMatch = text.match(/\{[\s\S]*"new_lessons"[\s\S]*\}/);
    if (jsonMatch) {
      const result = JSON.parse(jsonMatch[0]);
      for (const lesson of (result.new_lessons || [])) {
        db.insertLesson(lesson.lesson, lesson.evidence_count || 0);
      }
      for (const id of (result.archive_ids || [])) {
        db.archiveLesson(id);
      }
      console.log(`🧠 Extracted ${result.new_lessons?.length || 0} lessons, archived ${result.archive_ids?.length || 0}`);
    }
  } catch (e) {
    console.log('🧠 Lesson extraction failed:', e.message);
  }
};
```

**Step 2: Call after hypothesis reviews**

In the bot loop, after the `reviewExpiredHypotheses` call, add:

```js
      // Extract lessons after hypothesis reviews
      if (process.env.ANTHROPIC_API_KEY) {
        extractHypothesisLessons().catch(e => {
          console.log('🧠 Lesson extraction failed:', e.message);
        });
      }
```

**Step 3: Commit**

```bash
git add script.js
git commit -m "feat: add hypothesis lesson extraction after reviews"
```

---

### Task 5: Inject Performance Summary into Generation Prompt

**Files:**
- Modify: `script.js:1864-2051` (generateJournalEntries function)

**Step 1: Build performance summary**

In `generateJournalEntries`, after building the `snapshot` object (~line 2017) and before the `systemPrompt` (~line 2019), add:

```js
    // Build hypothesis performance summary for prompt injection
    let hypothesisPerformance = '';
    try {
      const stats = db.getHypothesisStats(30);
      const lessons = db.getActiveLessons();
      const recentVerdicts = db.getReviewedHypotheses(5);

      if (stats && stats.total > 0) {
        const reviewed = stats.total - (stats.pending || 0);
        const convexPosture = reviewed > 0
          ? (((stats.confirmed_convex || 0) + (stats.disproven_bounded || 0)) / reviewed * 100).toFixed(0)
          : 'N/A';
        const costlyRate = reviewed > 0
          ? ((stats.disproven_costly || 0) / reviewed * 100).toFixed(0)
          : 'N/A';

        hypothesisPerformance = `\n\n=== HYPOTHESIS PERFORMANCE (last 30 days) ===
Total hypotheses: ${stats.total} (${reviewed} reviewed, ${stats.pending || 0} pending)
Convex posture rate: ${convexPosture}% (confirmed_convex + disproven_bounded) / reviewed
Costly miss rate: ${costlyRate}% (disproven_costly / reviewed)
Breakdown: ${stats.confirmed_convex || 0} convex wins, ${stats.confirmed_linear || 0} linear wins, ${stats.disproven_bounded || 0} bounded losses (OK), ${stats.disproven_costly || 0} costly losses (BAD), ${stats.partially_confirmed || 0} partial

Recent verdicts:
${recentVerdicts.map(v => `#${v.id} [${v.outcome_status}]: ${v.outcome_verdict || 'no verdict text'}`).join('\n')}

${lessons.length > 0 ? `Active lessons:\n${lessons.map(l => `- ${l.lesson} (evidence: ${l.evidence_count})`).join('\n')}` : ''}

IMPORTANT: Double down on hypothesis types with high convex posture rates. Avoid types with high costly miss rates. Each hypothesis MUST identify what makes the opportunity asymmetric — why is the downside bounded? Where is the cheap convexity?`;
      }
    } catch (e) {
      console.log('📓 Failed to build hypothesis performance summary:', e.message);
    }
```

**Step 2: Append to system prompt**

In the system prompt string, append `${hypothesisPerformance}` at the end (before the closing backtick).

Change the last line of the systemPrompt (line 2049):

```js
Ground everything in the data. Focus on: cost of protection (put pricing), crash probability (flow reversals), and portfolio geometry (spot-options relationship).${hypothesisPerformance}`;
```

**Step 3: Commit**

```bash
git add script.js
git commit -m "feat: inject hypothesis performance summary into generation prompt"
```

---

### Task 6: Dashboard Read Helpers

**Files:**
- Modify: `dashboard/src/lib/db.ts:181-187` (getJournalEntries query)
- Modify: `dashboard/src/lib/db.ts:446-454` (getJournalEntries export)

**Step 1: Update getJournalEntries query**

In `dashboard/src/lib/db.ts`, update the `getJournalEntries` prepared statement (~line 181) to include verdict columns:

```ts
getJournalEntries: d.prepare(`
  SELECT id, timestamp, entry_type, content, series_referenced, created_at,
    prediction_deadline, outcome_status, outcome_verdict, outcome_confidence,
    trade_pnl_attribution, trades_in_window
  FROM ai_journal
  WHERE timestamp > ?
  ORDER BY timestamp DESC
  LIMIT ?
`),
```

**Step 2: Add new prepared statements**

After `getJournalEntries` in the `prepareAll` function, add:

```ts
getHypothesisStats: d.prepare(`
  SELECT
    COUNT(*) as total,
    SUM(CASE WHEN outcome_status = 'confirmed_convex' THEN 1 ELSE 0 END) as confirmed_convex,
    SUM(CASE WHEN outcome_status = 'confirmed_linear' THEN 1 ELSE 0 END) as confirmed_linear,
    SUM(CASE WHEN outcome_status = 'disproven_bounded' THEN 1 ELSE 0 END) as disproven_bounded,
    SUM(CASE WHEN outcome_status = 'disproven_costly' THEN 1 ELSE 0 END) as disproven_costly,
    SUM(CASE WHEN outcome_status = 'partially_confirmed' THEN 1 ELSE 0 END) as partially_confirmed,
    SUM(CASE WHEN outcome_status = 'pending' THEN 1 ELSE 0 END) as pending
  FROM ai_journal
  WHERE entry_type = 'hypothesis'
    AND timestamp > ?
`),

getActiveLessons: d.prepare(`
  SELECT id, lesson, evidence_count, created_at
  FROM hypothesis_lessons
  WHERE is_active = 1
  ORDER BY created_at DESC
`),
```

**Step 3: Add export functions**

After `getJournalEntries` export (~line 454), add:

```ts
export function getHypothesisStats(since: string) {
  try {
    return getStmts().getHypothesisStats.get(since) as {
      total: number; confirmed_convex: number; confirmed_linear: number;
      disproven_bounded: number; disproven_costly: number;
      partially_confirmed: number; pending: number;
    } | undefined;
  } catch {
    return undefined;
  }
}

export function getActiveLessons() {
  try {
    return getStmts().getActiveLessons.all() as {
      id: number; lesson: string; evidence_count: number; created_at: string;
    }[];
  } catch {
    return [];
  }
}
```

**Step 4: Commit**

```bash
git add dashboard/src/lib/db.ts
git commit -m "feat: add hypothesis stats and lessons read helpers to dashboard"
```

---

### Task 7: Dashboard API Endpoints

**Files:**
- Modify: `dashboard/src/app/api/ai/journal/route.ts`
- Create: `dashboard/src/app/api/ai/hypothesis-stats/route.ts`
- Create: `dashboard/src/app/api/ai/hypothesis-lessons/route.ts`

**Step 1: Journal endpoint already returns new columns**

The journal endpoint at `dashboard/src/app/api/ai/journal/route.ts` doesn't need changes — it calls `getJournalEntries` which now returns the verdict fields. No modification needed.

**Step 2: Create hypothesis-stats endpoint**

Create `dashboard/src/app/api/ai/hypothesis-stats/route.ts`:

```ts
import { NextResponse } from 'next/server';
import { getHypothesisStats } from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET() {
  try {
    const since = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
    const stats = getHypothesisStats(since);
    if (!stats) return NextResponse.json({ stats: null });

    const reviewed = stats.total - (stats.pending || 0);
    const convexPostureRate = reviewed > 0
      ? ((stats.confirmed_convex + stats.disproven_bounded) / reviewed)
      : 0;
    const costlyRate = reviewed > 0
      ? (stats.disproven_costly / reviewed)
      : 0;

    return NextResponse.json({
      stats: {
        ...stats,
        reviewed,
        convexPostureRate: +convexPostureRate.toFixed(3),
        costlyRate: +costlyRate.toFixed(3),
      },
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message, stats: null }, { status: 500 });
  }
}
```

**Step 3: Create hypothesis-lessons endpoint**

Create `dashboard/src/app/api/ai/hypothesis-lessons/route.ts`:

```ts
import { NextResponse } from 'next/server';
import { getActiveLessons } from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET() {
  try {
    const lessons = getActiveLessons();
    return NextResponse.json({ lessons });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message, lessons: [] }, { status: 500 });
  }
}
```

**Step 4: Commit**

```bash
git add dashboard/src/app/api/ai/hypothesis-stats/route.ts dashboard/src/app/api/ai/hypothesis-lessons/route.ts
git commit -m "feat: add hypothesis-stats and hypothesis-lessons API endpoints"
```

---

### Task 8: Dashboard — Verdict Badges on Journal Entries

**Files:**
- Modify: `dashboard/src/components/AdvisorDrawer.tsx:12-19` (JournalEntry interface)
- Modify: `dashboard/src/components/AdvisorDrawer.tsx:244-268` (journal entry rendering)

**Step 1: Update JournalEntry interface**

```ts
interface JournalEntry {
  id: number;
  timestamp: string;
  entry_type: string;
  content: string;
  series_referenced: string | null;
  created_at: string;
  prediction_deadline: string | null;
  outcome_status: string | null;
  outcome_verdict: string | null;
  outcome_confidence: number | null;
  trade_pnl_attribution: number | null;
  trades_in_window: string | null;
}
```

**Step 2: Add verdict badge styles**

After `TYPE_STYLES` (~line 39), add:

```ts
const VERDICT_STYLES: Record<string, { label: string; color: string }> = {
  confirmed_convex: { label: 'Convex Win', color: 'bg-green-500/20 text-green-400' },
  confirmed_linear: { label: 'Linear Win', color: 'bg-blue-500/20 text-blue-400' },
  partially_confirmed: { label: 'Partial', color: 'bg-blue-500/20 text-blue-400' },
  disproven_bounded: { label: 'Bounded Loss', color: 'bg-gray-500/20 text-gray-400' },
  disproven_costly: { label: 'Costly Miss', color: 'bg-red-500/20 text-red-400' },
};

function timeUntil(ts: string): string {
  const diff = new Date(ts).getTime() - Date.now();
  if (diff <= 0) return 'reviewing...';
  const hrs = Math.floor(diff / 3600000);
  const mins = Math.floor((diff % 3600000) / 60000);
  if (hrs > 0) return `verdict in ${hrs}h ${mins}m`;
  return `verdict in ${mins}m`;
}
```

**Step 3: Update journal entry rendering**

In the journal entry map (~line 244-268), update to include verdict badges. After the type badge and ID span (line 250-253), add:

```tsx
{entry.entry_type === 'hypothesis' && entry.outcome_status && entry.outcome_status !== 'pending' && (
  <span className={`text-[10px] font-bold px-1.5 py-0.5 ${VERDICT_STYLES[entry.outcome_status]?.color || 'bg-white/10 text-gray-400'}`}>
    {VERDICT_STYLES[entry.outcome_status]?.label || entry.outcome_status}
  </span>
)}
{entry.entry_type === 'hypothesis' && (!entry.outcome_status || entry.outcome_status === 'pending') && entry.prediction_deadline && (
  <span className="text-[10px] text-gray-500 italic">
    {timeUntil(entry.prediction_deadline)}
  </span>
)}
```

After the ReactMarkdown block and before the series_referenced section, add an expandable verdict:

```tsx
{entry.entry_type === 'hypothesis' && entry.outcome_verdict && (
  <details className="text-xs text-gray-400 border-t border-white/5 pt-1.5 mt-1.5">
    <summary className="cursor-pointer hover:text-gray-300 transition-colors">
      Verdict ({(entry.outcome_confidence ? (entry.outcome_confidence * 100).toFixed(0) : '?')}% confidence)
      {entry.trade_pnl_attribution != null && (
        <span className={entry.trade_pnl_attribution >= 0 ? 'text-green-400 ml-2' : 'text-red-400 ml-2'}>
          P&L: {entry.trade_pnl_attribution >= 0 ? '+' : ''}{entry.trade_pnl_attribution.toFixed(4)}
        </span>
      )}
    </summary>
    <p className="mt-1 text-gray-500 leading-relaxed">{entry.outcome_verdict}</p>
  </details>
)}
```

**Step 4: Commit**

```bash
git add dashboard/src/components/AdvisorDrawer.tsx
git commit -m "feat: add verdict badges and expandable verdicts to journal entries"
```

---

### Task 9: Dashboard — Hypothesis Analytics Tab

**Files:**
- Modify: `dashboard/src/components/AdvisorDrawer.tsx` — add "Analytics" tab

**Step 1: Add state for analytics data**

In the component, after existing state declarations (~line 56), add:

```ts
const [analyticsTab, setAnalyticsTab] = useState(false);
const [hypStats, setHypStats] = useState<{
  total: number; reviewed: number; pending: number;
  confirmed_convex: number; confirmed_linear: number;
  disproven_bounded: number; disproven_costly: number;
  partially_confirmed: number;
  convexPostureRate: number; costlyRate: number;
} | null>(null);
const [lessons, setLessons] = useState<{ id: number; lesson: string; evidence_count: number; created_at: string }[]>([]);
```

**Step 2: Add fetch function**

After `fetchJournal`, add:

```ts
const fetchAnalytics = useCallback(async () => {
  try {
    const [statsRes, lessonsRes] = await Promise.all([
      fetch('/api/ai/hypothesis-stats'),
      fetch('/api/ai/hypothesis-lessons'),
    ]);
    if (statsRes.ok) {
      const data = await statsRes.json();
      setHypStats(data.stats);
    }
    if (lessonsRes.ok) {
      const data = await lessonsRes.json();
      setLessons(data.lessons || []);
    }
  } catch { /* silent */ }
}, []);
```

**Step 3: Add useEffect to fetch analytics**

```ts
useEffect(() => {
  if (open && tab === 'journal' && analyticsTab) {
    fetchAnalytics();
  }
}, [open, tab, analyticsTab, fetchAnalytics]);
```

**Step 4: Add analytics toggle inside journal tab**

At the top of the journal tab content (inside the `tab === 'journal'` conditional, before the entries list), add a sub-toggle:

```tsx
<div className="flex gap-1 mb-2 border-b border-white/5 pb-2">
  <button
    onClick={() => setAnalyticsTab(false)}
    className={`text-[10px] px-2 py-0.5 transition-colors ${!analyticsTab ? 'bg-white/10 text-white' : 'text-gray-500 hover:text-gray-300'}`}
  >
    Entries
  </button>
  <button
    onClick={() => setAnalyticsTab(true)}
    className={`text-[10px] px-2 py-0.5 transition-colors ${analyticsTab ? 'bg-white/10 text-white' : 'text-gray-500 hover:text-gray-300'}`}
  >
    Analytics
  </button>
</div>
```

**Step 5: Add analytics view**

When `analyticsTab` is true, render:

```tsx
{analyticsTab ? (
  <div className="space-y-4">
    {hypStats ? (
      <>
        {/* Headline metric */}
        <div className="text-center py-3">
          <div className="text-3xl font-bold text-juice-orange">
            {(hypStats.convexPostureRate * 100).toFixed(0)}%
          </div>
          <div className="text-[10px] text-gray-500 mt-1">Convex Posture Rate</div>
          <div className="text-[10px] text-gray-600">{hypStats.reviewed} reviewed / {hypStats.pending} pending</div>
        </div>

        {/* Outcome breakdown */}
        <div className="space-y-1.5">
          <div className="text-[10px] text-gray-500 font-bold tracking-wide">OUTCOME BREAKDOWN</div>
          {[
            { label: 'Convex Wins', count: hypStats.confirmed_convex, color: 'bg-green-500' },
            { label: 'Linear Wins', count: hypStats.confirmed_linear, color: 'bg-blue-500' },
            { label: 'Bounded Losses', count: hypStats.disproven_bounded, color: 'bg-gray-500' },
            { label: 'Costly Misses', count: hypStats.disproven_costly, color: 'bg-red-500' },
            { label: 'Partial', count: hypStats.partially_confirmed, color: 'bg-blue-400' },
          ].map(item => (
            <div key={item.label} className="flex items-center gap-2">
              <div className={`w-2 h-2 ${item.color} rounded-full shrink-0`} />
              <span className="text-xs text-gray-400 flex-1">{item.label}</span>
              <span className="text-xs text-white font-mono">{item.count}</span>
              {hypStats.reviewed > 0 && (
                <div className="w-16 h-1.5 bg-white/5 rounded-full overflow-hidden">
                  <div className={`h-full ${item.color}/60 rounded-full`}
                    style={{ width: `${(item.count / hypStats.reviewed) * 100}%` }} />
                </div>
              )}
            </div>
          ))}
        </div>

        {/* Costly miss rate warning */}
        {hypStats.costlyRate > 0.2 && (
          <div className="text-[10px] text-red-400 bg-red-500/10 px-2 py-1.5 border border-red-500/20">
            Costly miss rate is {(hypStats.costlyRate * 100).toFixed(0)}% — review hypothesis types being generated.
          </div>
        )}
      </>
    ) : (
      <p className="text-gray-500 text-xs">No hypothesis data yet. Hypotheses will be reviewed after their deadlines pass.</p>
    )}

    {/* Active lessons */}
    {lessons.length > 0 && (
      <div className="space-y-1.5">
        <div className="text-[10px] text-gray-500 font-bold tracking-wide">ACTIVE LESSONS</div>
        {lessons.map(l => (
          <div key={l.id} className="text-xs text-gray-400 bg-white/5 px-2 py-1.5 border-l-2 border-juice-orange/50">
            {l.lesson}
            <span className="text-[10px] text-gray-600 block mt-0.5">
              Evidence: {l.evidence_count} | Since: {timeAgo(l.created_at)}
            </span>
          </div>
        ))}
      </div>
    )}
  </div>
) : (
  /* existing entries list goes here */
)}
```

**Step 6: Commit**

```bash
git add dashboard/src/components/AdvisorDrawer.tsx
git commit -m "feat: add hypothesis analytics sub-tab with stats and lessons"
```

---

### Task 10: Verify and Test End-to-End

**Step 1: Build the dashboard**

```bash
cd /Users/jango/Documents/noop-c/dashboard && npm run build
```

Expected: Build succeeds with no type errors.

**Step 2: Verify the bot starts without errors**

```bash
cd /Users/jango/Documents/noop-c && node -e "const db = require('./bot/db'); console.log('DB loaded, tables created'); db.close();"
```

Expected: "DB loaded, tables created" — confirms schema migrations run.

**Step 3: Verify new columns exist**

```bash
cd /Users/jango/Documents/noop-c && node -e "const db = require('./bot/db'); const row = db.getRecentJournalEntries(1); console.log('Columns:', Object.keys(row[0] || {})); db.close();"
```

Expected: Should include `prediction_deadline`, `outcome_status`, etc. alongside existing columns.

**Step 4: Verify API endpoints**

```bash
cd /Users/jango/Documents/noop-c/dashboard && npx next build && timeout 10 npx next start &
sleep 3
curl -s http://localhost:3000/api/ai/hypothesis-stats | head -c 200
curl -s http://localhost:3000/api/ai/hypothesis-lessons | head -c 200
kill %1
```

**Step 5: Final commit**

```bash
git add -A
git commit -m "chore: verify hypothesis feedback loop builds cleanly"
```
