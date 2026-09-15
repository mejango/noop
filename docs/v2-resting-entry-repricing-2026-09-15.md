# V2 resting entry repricing

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
