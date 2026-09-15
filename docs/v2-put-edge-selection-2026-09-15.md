# V2 direct PUT EDGE selection

## Behavior

Buy-put candidates now rank directly by the existing continuous PUT EDGE:

`abs(delta) / ask × (DTE / 60)^0.8`

The formula, normalization exponent, and score thresholds have not changed.
Every eligible candidate is normalized before comparison. Global selection,
rolling value context, the advisory candidate list, and rule-level candidate
ranking use live ask-based EDGE. The rule scan can select a different winner
when its allowed delta, DTE, strike or other hard constraints narrow the universe.

Patient order pricing remains separate: `min_score` and `target_score` determine
the maximum premium, and the executor computes a venue-valid price beneath that
cap. Planned EDGE at that price must satisfy the rule. Ranking every hypothetical
patient bid at a shared target would mostly compare rounding noise; live ask
EDGE provides the common observed-price comparison instead.

The former spread, IV/skew, open-interest and shock-payoff multipliers no longer
change PUT ranking. Saved `min_edge_score` values are ignored by normal entries,
resting-order reassessment, replacement confirmation economics and final order
validation. Advisors and both execution reviewers receive the same instruction.
New rule prompts no longer request that retired field.

This intentionally removes the composite model's additional selectivity. Quote
validity, delta/DTE and strike limits, active rule conditions, minimum/target
EDGE, budget, margin, quantity, reviewer approval and reconciliation safeguards
continue to apply. Call scoring is unchanged.

## Data and interpretation

PUT EDGE measures DTE-normalized delta per premium dollar. It is not a literal
crash payoff or expected return. The 30/40/50% drawdown scenarios remain separate
diagnostics: hypothetical expiration intrinsic value divided by premium,
excluding fees. They do not predict an earlier resale price or add a veto.

New selection metadata identifies `put_edge_v1` and its price basis. Candidate
selection uses `live_ask`; incumbent replacement economics use `planned_limit`.
Candidate observations retain the existing planned-limit raw-score field and
identify that basis explicitly, alongside live raw/normalized scores. PUT scores
in pending-action and advisory summaries retain six decimals rather than being
rounded to zero on the former composite scale.

No database migration or historical row rewrite is required. Historical
composite selection scores remain historical values; do not concatenate them
with new direct selection scores as one continuous metric. Dedicated PUT EDGE
chart history and its score scale are unchanged.

## Verification

Tests exercise actual production selection, normal entry creation, resting
replacement maintenance, confirmation prompts and final execution policy.
They cover conflicting legacy composite scores, equal patient targets with
different live asks, decimal precision, extreme diagnostic values, ignored
saved composite thresholds, and preserved core price/risk vetoes. The normal
entry regressions also fail against the previous implementation as expected.

The full V2 suite passes 1,029 tests: 481 legacy trading, 502 Node test runner,
and 46 backtest tests. The dashboard production build also passes. Rollback is a code revert; live orders and fills remain
subject to normal exchange reconciliation.
