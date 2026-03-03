# Hypothesis Feedback Loop Design

**Date:** 2026-03-03
**Goal:** Build a closed-loop system that tracks hypothesis outcomes, scores them through a Spitznagel risk lens, links them to trade P&L, and feeds performance data back into generation — improving hypothesis quality over time.

**Philosophy:** This is NOT about maximizing prediction accuracy. It's about refining a thesis with asymmetric payoffs. A hypothesis can be wrong 60% of the time and still be enormously profitable if the wins are convex and the losses are bounded. The key metric is convex posture rate, not batting average.

---

## 1. Structured Hypothesis Metadata

Add columns to `ai_journal` table for hypothesis entries only:

```sql
prediction_target TEXT,           -- e.g. "ETH spot price"
prediction_direction TEXT,        -- "above" | "below" | "within_range"
prediction_value REAL,            -- e.g. 2000.00
prediction_deadline TEXT,         -- ISO timestamp
falsification_criteria TEXT,      -- plain text
outcome_status TEXT DEFAULT 'pending',  -- see verdict categories below
outcome_verdict TEXT,             -- Claude's written verdict
outcome_confidence REAL,          -- 0-1
outcome_reviewed_at TEXT,
trade_pnl_attribution REAL,      -- P&L of trades during hypothesis window
trades_in_window TEXT             -- JSON array of trade IDs
```

Hypothesis generation prompt updated to emit structured JSON:

```
<hypothesis_meta>
{"target":"ETH spot","direction":"below","value":2000,"deadline":"2026-03-04T01:39:00Z","falsification":"If price doesn't breach $2000 within 18h"}
</hypothesis_meta>
```

Bot extracts this alongside the prose content.

## 2. Review Cycle

`reviewExpiredHypotheses()` runs each bot tick (5 min), checks for pending hypotheses past deadline.

**Per expired hypothesis:**
1. Gather actual price data from prediction window (spot_prices table)
2. Gather on-chain data and momentum state at deadline
3. Find trades made during the window (trades table), calculate P&L
4. Call Claude with a focused review prompt: hypothesis + actual data → verdict
5. Store verdict back into hypothesis row

**Rate limit:** Max 3 reviews per tick.

### Verdict Categories (Spitznagel-Aligned)

| Outcome | Meaning |
|---------|---------|
| `confirmed_convex` | Prediction correct AND payoff was asymmetric |
| `confirmed_linear` | Prediction correct but position had symmetric risk |
| `disproven_bounded` | Wrong, but loss was small/bounded (strategy working) |
| `disproven_costly` | Wrong AND position was expensive (overpaid for insurance) |
| `partially_confirmed` | Direction right, timing/magnitude off |

**Key metric:** Convex posture rate = (confirmed_convex + disproven_bounded) / total reviewed

## 3. Feedback Loop into Generation

### Per-generation injection

Build a hypothesis performance summary injected into the journal generation prompt:

```
=== HYPOTHESIS PERFORMANCE (last 30 days) ===
Total reviewed: 34
Convex posture rate: 71%
Costly miss rate: 12%

Best-performing hypothesis types:
- Put-price divergence signals: 83% convex posture
- Liquidity flow regime shifts: 57%
- Pure directional predictions: 25% ← AVOID

Recent verdicts (last 5): ...

Lessons extracted:
- Hypotheses grounded in put-price divergence outperform pure momentum reads
- Timing predictions >24h have lower accuracy but similar convex posture rate
- ...
```

### Lesson extraction

After every 10 reviewed hypotheses, a separate Claude call analyzes the full verdict history and distills 3-5 actionable patterns. Stored in a `hypothesis_lessons` table.

### Prompt instruction addition

> "Double down on hypothesis types with high convex posture rates. Avoid types with high costly miss rates. Each hypothesis must identify what makes the opportunity asymmetric — why is the downside bounded? Where is the cheap convexity?"

### Drift correction

Every 30 days, comprehensive review checks whether lessons still hold. Stale lessons archived (not deleted).

## 4. Dashboard

### 4a. Inline Verdict Badges

On each hypothesis in Journal tab:
- Green badge: `confirmed_convex`
- Blue badge: `confirmed_linear` / `partially_confirmed`
- Gray badge: `disproven_bounded` (strategy working as intended)
- Red badge: `disproven_costly`
- No badge + countdown: `pending`
- Expandable verdict section with reasoning, confidence, linked trades + P&L

### 4b. Hypothesis Analytics (new tab or section)

- **Convex posture rate** — headline metric with sparkline trend
- **Outcome breakdown** — donut chart of five verdict categories
- **Performance by hypothesis type** — bar chart of convex posture rate per category
- **Active lessons** — current distilled lessons with dates and evidence counts
- **Timeline** — scatter plot of hypotheses colored by outcome

### 4c. API Endpoints

- `GET /api/ai/journal` — update to include verdict fields
- `GET /api/ai/hypothesis-stats` — aggregate stats for analytics
- `GET /api/ai/hypothesis-lessons` — active lessons

## 5. Key Files to Modify

| File | Changes |
|------|---------|
| `bot/db.js` | Schema migration, new tables, write helpers |
| `script.js` | `reviewExpiredHypotheses()`, updated `generateJournalEntries()`, lesson extraction |
| `dashboard/src/lib/db.ts` | Read helpers for verdicts, stats, lessons |
| `dashboard/src/app/api/ai/journal/route.ts` | Include verdict fields |
| `dashboard/src/app/api/ai/hypothesis-stats/route.ts` | New endpoint |
| `dashboard/src/app/api/ai/hypothesis-lessons/route.ts` | New endpoint |
| `dashboard/src/components/AdvisorDrawer.tsx` | Verdict badges, countdown, analytics tab |
