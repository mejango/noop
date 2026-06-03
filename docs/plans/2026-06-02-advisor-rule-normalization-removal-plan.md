# Advisor Rule Normalization Removal Plan

## Goal

Move strategy semantics out of executor-side normalizers and into strict advisor rule contracts. The executor should only normalize mechanical venue details such as tick size, amount step, order type compatibility, malformed JSON handling, and stale/out-of-range market data.

Suspicious strategy normalizers should become temporary migration aids, then validators, then be deleted once advisor output is consistently clean.

## Current Problem

The bot needs standing watchers because advisory runs are scheduled. Price, IV, skew, and option value can move faster than the next advisory tick. Watchers are therefore correct and necessary.

The problem is that some watchers are currently expressed through generic condition JSON. That lets the advisor encode "later" with loose proxies such as broad spot floors, DTE blockers, mark-price blockers, or vague value-signal strings. Downstream code then repairs strategy intent after the fact. That makes live behavior harder to audit because the persisted/executed rule is not purely advisor-authored.

## Target Design

Every standing watcher should be a typed opportunity contract. Price can swing up, down, or sideways between advisory runs, but each watcher should still trigger on the economic state that makes the action worth taking.

### buy_put: tail_hedge_accumulation

Purpose: accumulate convex downside protection when value is good.

Allowed fields:

- `option_type: "P"`
- `delta_range`
- `dte_range`, normally 45-75 DTE
- `budget_limit`, as total USD spend cap
- `min_score`
- `target_score`
- `value_signal`

Allowed `value_signal` values:

- `strict_fresh_best`
- `spot_drop_option_repricing_lag`
- `recent_relative_value`
- `any_actionable_buy_put`

Rules:

- Unknown `value_signal` fails closed.
- `any_actionable_buy_put` means `strict_fresh_best` or `spot_drop_option_repricing_lag`; it does not include `recent_relative_value`.
- `recent_relative_value` is a weaker local-value signal and should only be used explicitly with stricter score/target constraints and a concrete value rationale.
- Do not use per-contract `max_cost`.
- Do not use broad spot floors as value gates.

### sell_call: premium_harvest

Purpose: harvest greed premium without compromising survival or overextending margin.

Allowed fields:

- `option_type: "C"`
- `delta_range`, normally 0.04-0.12
- `dte_range`, normally 5-12 DTE
- `min_bid`
- `min_score`, where call score is `bid / abs(delta)`

Optional market context:

- Upward market conditions may be useful when greed premium appears, but they must never replace `min_score` and `min_bid`.
- A broad `spot_price >= X` condition is not evidence of good premium by itself.
- Margin context is used for sizing and safety, not as a standalone reason to sell weak premium.

### sell_put: roll_protection

Purpose: refresh aging downside protection without leaving the book naked.

Trigger:

- Existing long put reaches `dte <= 25`.

Required safety:

- The book must already hold longer-dated downside protection, or the roll workflow must secure replacement protection before or alongside selling the aging put.
- Selling the old put should not remove all downside protection from the book.

### sell_put: monetize_tail_win

Purpose: harvest explosive convex payoff while preserving further downside convexity.

Trigger:

- Executable `unrealized_pnl_pct > 500`.

Required safety:

- Sell in tranches, not all at once.
- Always retain some downside protection after each sale.
- Confirmation advisors should judge whether the crash may have more legs before selling each tranche.
- No short-window momentum rule should force sale. Market context informs judgment, but the core trigger is extreme executable payoff plus protection retention.

### buyback_call: profit_capture

Purpose: harvest decayed short-call premium and recycle capacity when the short call is working.

Allowed trigger forms:

- Executable close when `unrealized_pnl_pct >= 80`.
- Patient resting bid where the limit price would achieve at least 80% capture if filled.

Rules:

- Do not add DTE blockers.
- Do not add mark-price blockers.
- Do not bid back up to the 80% line if live executable economics are already better. Hold, let expire, or name a lower patient bid.
- Margin release is a benefit of a good close, not a standalone buyback trigger.

### buyback_call: threat_management

Purpose: reduce genuine short-call danger, not react emotionally to price rising.

Allowed evidence:

- Short DTE / limited time for recovery.
- Spot near or above strike.
- Delta materially elevated.
- Assignment or continuation risk is credible.
- Buyback cost is justified versus the risk of holding.
- Selling richer calls is not the better response under current margin constraints.

Rules:

- Price rising alone is not enough.
- Do not buy back fear premium just because spot moved up; that can make the bot the sucker of the trade.
- This is the only buyback-call intent allowed below the 80% profit-capture floor.

## Suspicious Normalizers To Retire

### `normalizeBuybackCaptureFloor`

Location: `script.js`

Current behavior:

- Adds or raises `unrealized_pnl_pct >= 80` for `buyback_call`.
- Removes `dte` and `mark_price` blockers.
- Forces `condition_logic: "all"`.

Why it exists:

- The generic exit schema lets profit-capture buybacks include unrelated blockers.

Removal path:

1. Add strict `buyback_call_profit_capture` and `buyback_call_threat_management` schema validation.
2. Shadow-log rules that would be rejected.
3. Once advisors consistently emit valid typed buyback rules, change this function from mutator to validator/rejector.
4. Delete after no shadow failures for a sustained run window.

### `normalizeBuyPutValueSignal`

Location: `script.js`

Current behavior:

- Aliases older or looser signal names.
- Unknown signals normalize to `null`, which currently disables the signal gate.

Why it exists:

- The advisor schema did not always enforce exact signal enums.

Removal path:

1. Keep only exact allowed enum values in the prompt and validator.
2. Fail closed on unknown non-empty `value_signal`.
3. Remove compatibility aliases once no active or newly generated rules rely on them.

### Buy-put `max_cost` ignore

Location: `script.js`

Current behavior:

- Ignores `max_cost` for `buy_put`.

Why it exists:

- Per-contract cost caps conflicted with total-budget and score/target-score based buying.

Removal path:

1. Reject `max_cost` on `buy_put` rules at validation time.
2. Delete the special ignore once advisors stop emitting it.

### Sell-call strategy range clamp

Location: `script.js`

Current behavior:

- Forces `sell_call` candidates into the normal DTE/delta universe even if advisor criteria are broader.

Why it exists:

- Advisor rules could otherwise be broad enough to sell calls outside the intended strategy range.

Removal path:

1. Validate `sell_call` rules require DTE and delta ranges within policy.
2. Keep runtime clamp as a hard safety guard until validation is proven.
3. After sustained clean output, decide whether to keep this as a safety invariant or remove it as redundant.

## Validator Shape

Before persistence, validate the final synthesized agenda by action and intent:

- `buy_put` must match `tail_hedge_accumulation`.
- `sell_call` must match `premium_harvest`.
- `sell_put` must declare `roll_protection` or `monetize_tail_win`.
- `buyback_call` must declare `profit_capture` or `threat_management`.

Invalid semantic rules should be rejected or sent to repair. They should not be silently rewritten.

## Shadow Mode

During migration, keep current suspicious normalizers active but add logs/metrics:

- `would_reject_reason`
- `rule_action`
- `rule_intent`
- `instrument_name`
- `advisory_id`
- original criteria
- normalized criteria

Use this to answer:

- Which advisors or synthesis steps still emit invalid rules?
- Which rule types fail most often?
- Are failures harmless schema drift or real strategy mistakes?
- Has output stayed clean long enough to remove a normalizer?

## Deletion Criteria

A suspicious normalizer can be removed only after:

1. Typed schema validation exists for the affected rule type.
2. Shadow mode shows no invalid advisor output for a sustained live window.
3. Existing active rules in the DB have been replaced by clean advisory rules.
4. Tests cover both accepted and rejected examples.
5. The normalizer has been converted to a validator/rejector for at least one deployment cycle without blocking valid trades.

## Non-Suspicious Normalizations To Keep

These are mechanical safety checks and should remain:

- Venue tick-size price rounding.
- Amount-step rounding.
- Order-type compatibility with action phase.
- Malformed JSON extraction/rejection.
- Spot price sanity bounds.
- Executable bid/ask based PnL calculations.
- Fresh market-price sanity checks before execution.
