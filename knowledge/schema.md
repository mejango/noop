# Wiki Schema

This file defines the structure, update rules, and format conventions for the NOOP-C knowledge wiki.

## System-Owned Files

### index.md
- System-maintained catalog of the wiki
- Lists all pages with one-line summaries
- Read this first when orienting to the wiki
- The system updates this deterministically; LLM page-update prompts should not rewrite it

### log.md
- Append-only maintenance timeline
- Records ingests, seed passes, lint passes, and notable knowledge-system events
- The system updates this deterministically; LLM page-update prompts should not rewrite it

### raw/evidence/
- Immutable evidence packets written by the system
- Packets contain factual market snapshots, factual order activity, and supporting analyst notes
- Prefer these packets over re-derived claims when compiling wiki updates

## Source Hierarchy

When sources disagree, trust them in this order:

1. **Raw evidence packets and structured factual data** — market snapshots, order activity, account state
2. **Reviewed outcomes** — trade reviews and hypothesis verdicts
3. **Journal entries and active lessons** — analyst interpretation and distilled heuristics
4. **Existing wiki text** — prior synthesis, useful but revisable

Rules:
- Never elevate journal interpretation into fact unless corroborated by higher-priority evidence
- When higher-priority evidence contradicts existing wiki text, revise transparently
- If evidence is mixed or thin, state uncertainty instead of over-asserting

## Page Types & Required Sections

### regimes/current.md
- **TLDR** (bold, 1 line)
- **Classification** (complacency | fear | transition | recovery)
- **Evidence** (specific data values supporting classification)
- **Falsification** (what would change the classification)
- **Confidence** (high | medium | low with reasoning)

### regimes/history.md
- **Regime Transitions** (table: date | from | to | trigger | duration)
- **Patterns** (recurring transition sequences)

### protection/pricing.md
- **TLDR** (bold, 1 line)
- **Current IV Environment** (absolute levels + percentile)
- **Skew Analysis** (put-call IV spread, direction)
- **Term Structure** (near vs far IV, contango/backwardation)
- **Cost Assessment** (cheap | fair | expensive with evidence)

### protection/windows.md
- **TLDR** (bold, 1 line)
- **Active Windows** (current cheap protection opportunities)
- **Historical Windows** (past windows with dates, durations, outcomes)
- **Window Indicators** (what signals precede cheap windows)

### protection/convexity.md
- **TLDR** (bold, 1 line)
- **Current Convexity Map** (where in the put chain is convexity highest)
- **Strike-Delta Sweet Spots** (optimal delta ranges for convexity)
- **Convexity Shifts** (how the map has changed recently)

### indicators/leading.md
- **TLDR** (bold, 1 line)
- **Confirmed Leading Indicators** (signals that reliably precede cost changes)
- **Experimental Indicators** (signals under observation)
- **Failed Indicators** (signals that didn't hold up)

### indicators/correlations.md
- **TLDR** (bold, 1 line)
- **Strong Correlations** (consistently observed cross-series relationships)
- **Weakening Correlations** (relationships losing reliability)
- **New Correlations** (recently detected patterns)

### indicators/divergences.md
- **TLDR** (bold, 1 line)
- **Active Divergences** (current put-price divergence signals)
- **Historical Divergence Episodes** (past divergences and their resolutions)
- **Divergence Playbook** (how to act on different divergence types)

### revenue/pricing.md
- **TLDR** (bold, 1 line)
- **Current Premium Environment** (call IV levels, premium richness)
- **Skew & IV Context** (how call IV relates to put IV, what skew says about opportunity)
- **Premium Assessment** (cheap | fair | rich with evidence)

### revenue/windows.md
- **TLDR** (bold, 1 line)
- **Active Windows** (current rich-premium opportunities for call selling)
- **Historical Windows** (past premium windows with dates, durations, premium captured)
- **Window Indicators** (what signals precede rich premium windows)

### revenue/efficiency.md
- **TLDR** (bold, 1 line)
- **Premium Per Unit Risk** (premium/delta ratios, risk-adjusted returns)
- **Strike Selection Patterns** (which strike-delta combos yield best risk-adjusted premium)
- **Buyback Patterns** (when buybacks capture value vs destroy it, timing insights)

### strategy/lessons.md
- **TLDR** (bold, 1 line)
- **Active Lessons** (currently valid actionable insights)
- **Archived Lessons** (lessons that no longer hold, with reason)
- **Evidence Tracker** (which lessons have the most supporting evidence)

### strategy/mistakes.md
- **TLDR** (bold, 1 line)
- **Costly Patterns** (mistakes that led to disproven_costly outcomes)
- **Near Misses** (close calls that should inform future behavior)
- **Anti-Patterns** (conditions where the bot should NOT buy)

### strategy/playbook.md
- **TLDR** (bold, 1 line)
- **Core Rules** (distilled actionable rules from all experience)
- **Regime-Specific Actions** (what to do in each regime)
- **Sizing Guidelines** (when to increase/decrease position sizes)
- **Timing Rules** (when in the cycle to be aggressive vs patient)

## Update Rules

1. **Preserve accurate content** — never delete observations that are still valid
2. **Add with dates** — new observations include `[YYYY-MM-DD]` date stamps
3. **Revise transparently** — when updating, use "Previously: X. Updated [YYYY-MM-DD]: Y" format
4. **Word limit** — each page should stay under 2000 words. If approaching limit, consolidate older entries
5. **Bold TLDR** — every page starts with a bold one-line summary of current state
6. **Evidence required** — claims must reference specific data values (IV levels, percentages, dates)
7. **Falsification criteria** — regime assessments and hypotheses include what would disprove them
8. **No speculation without evidence** — wiki captures confirmed patterns, not guesses

## Format Conventions

- Use markdown headers (##) for sections
- Use **bold** for the TLDR line
- Use `code` formatting for specific numeric values
- Use tables for structured historical data
- Use bullet lists for evidence and indicators
- Date format: YYYY-MM-DD
