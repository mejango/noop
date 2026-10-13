# V2 resting entry repricing

Subsequent change: [direct PUT EDGE selection](v2-put-edge-selection-2026-09-15.md)
removes the composite quality requirement described below. The repricing and
reconciliation safeguards in this report remain in place.

## Problem and production evidence

Production `99c72c1` maintained resting entry validity but only compared its
price and quantity when that instrument won the new-entry candidate scan.
Action cooldowns, capacity gates and a different winning contract could bypass
repricing entirely. The old comparison also compared an unnormalized candidate
price with a venue-normalized maker order, creating unnecessary cancellation.

Read-only production decisions on September 15 at 14:42–14:44 UTC selected
November 27 puts at strikes 1500 or 1800 while retaining the 1600 put at $16.20.
The log reported `same_action_resting_order`; it did not prove that the incumbent
had been repriced.

## Change

`reassessRestingEntryOrders` independently refreshes and reconciles each tracked
buy-put and sell-call order, obtains a fresh quote for that instrument, and
derives desired terms from an active entry rule. This runs before new-entry
cooldowns, capacity gates, duplicate gates and candidate selection. The original
rule is preferred when active and applicable; a compatible successor rule can
maintain an order across advisory renewal.

The planner compares direction, instrument, route, tick-aligned price and
unfilled quantity. Identical terms keep the existing queue position without
reviewer calls. A one-tick improvement caused by the incumbent's own best quote
does not repeatedly outbid itself. A changed valid plan queues an explicit
replacement; pending reviews and recent rejection backoff suppress duplicates.
Unknown order state, quotes or rule compatibility defer maintenance and block
another same-action entry.

Put repricing cannot increase remaining contracts or the original remaining
notional. Other live reservations, current spent budget and the active rule's
budget also constrain quantity. Call repricing preserves remaining contracts;
fresh margin checks govern replacement. Current rule score targets, delta/DTE,
market conditions and composite put quality requirements still apply.

Both reviewers receive the existing order and concrete replacement plan. Their
approval is required before repricing cancels the old order. A veto, missing
reviewer or failed pre-cancellation economics check leaves it in place. After
approval, cancellation must reach a proven terminal state; late fills reduce
replacement quantity and put spending authority. Fresh rule, quote, margin,
budget and action-wide order-book checks run before submission and maker retries.
An acknowledgement alone never releases a reservation.

Tracked entries no longer expire merely because they are eight hours old.
Existing immediate cancellation of unsafe economics and orphaned orders remains
active. Such cancellation can precede repricing: an invalid order must not stay
executable while reviewers deliberate. The separate strategy policy for rotating
an actionable put entry into a different instrument is unchanged; it cannot
countermand a queued or unresolved incumbent repricing.

## Verification and rollout

Tests exercise actual production declarations, the pure planner, and SQLite
fill accounting. Cases include incumbent maintenance when another contract
wins, cooldown ordering, identical maker terms, partial fills, budget caps,
reviewer failures, unknown cancellation, terminal-order recovery, concurrent
same-action orders and final revalidation.

The full V2 test command passes 1,013 tests (481 legacy trading, 486 Node test
runner, 46 backtest). The dashboard production build and whitespace checks pass.

A read-only replay at 15:10 UTC used the production 1600 put's $16.20 × 2.34
order, fresh delta −0.06177 and ask $23.20. The price/quantity planner proposed
$19.90 × 1.90 at the active 0.0036 PUT EDGE target: $37.81, below the original
$37.908 remaining notional. This isolated replay did not submit or cancel an
order; the live replacement must also pass the rule's composite quality gate
and both reviewers.

No schema migration or historical repair is required. New replacement metadata
uses the existing pending-action tracking fields. Rollback is a code revert;
the normal order reconciler remains authoritative for live orders and fills.

## October 6 follow-up: cancellation recovery and quote churn

Live evidence showed replacement actions being marked canceled when the venue
had acknowledged cancellation but could not immediately return terminal order
status. New-entry selection then repeatedly chose a put under a 30-minute
failure cooldown, leaving no resting bid.

Replacement requests now stay pending through uncertain or still-open order
status. The existing trigger metadata records cancellation intent before the
request and acknowledgement afterward, preserving the original quantity, fill
baseline and put notional authority across process restarts. A recovered request
without an acknowledgement retries cancellation only; an acknowledged request
waits for terminal proof. Venue reconciliation does not consume reviewer retry
allowances. After terminal reconciliation, fresh planning, reviewer/preflight
approval, price, budget, margin and duplicate-entry checks still govern submission.
Late fills reduce both remaining quantity and put spending authority. Hard
ineligibility retires the replacement intent so another instrument can qualify.
Risk-reducing exits take priority over entries in the bounded confirmation queue,
so two waiting replacements cannot occupy both slots ahead of an exit.

Candidate cooldowns apply before ranking. Exact local buy-put final-price/EDGE
validation failures can be recalculated on the next observation instead of
blocking that instrument for 30 minutes. Persistent failures, reviewer backoffs
and all final execution constraints remain enforced.

Discretionary repricing requires an order age of at least 120 seconds and a
price change of at least the greater of two venue ticks or 1% of its current
price. These are initial operational defaults, not a profitability claim. An
existing order must still satisfy current price/EDGE and budget constraints;
hard violations, required budget reductions, route changes, crossed quotes and
terminal recovery bypass the hold. Missing creation timestamps bypass only the
age test. Pure quantity changes retain the existing behavior.

Cancellation alerts distinguish request acknowledgement from terminal
confirmation and include the reason and next step. Accounting emits terminal
transitions once; notification context is in memory and falls back to the venue
reason after restart. The order and replacement state remain durable.

Validation: `npm test` passes, including production-function selection, planner,
notification and accounting regressions. Seven recovery tests exercise the real
SQLite schema across close/reopen and cover unknown/still-open cancellation,
restart before acknowledgement, exact late-fill deductions, single submission,
fresh final margin rejection, rule ineligibility and exit priority. Syntax and
whitespace checks pass. This follow-up is a local code change; it has not been
deployed and does not repair previously canceled replacement records.
