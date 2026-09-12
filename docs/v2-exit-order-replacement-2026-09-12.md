# V2 exit-order deduplication and replacement

Production inspection at `04db9ab` explained the repeated September 12 buyback rejections for `ETH-20260918-2800-C`. The $2.20 order was recorded as cancelled at 02:46:36 UTC. Rule 1912, created shortly before that cancellation, still described an existing “8.0h-old” bid. New pending actions repeatedly presented that saved narrative without an authoritative current exit-order snapshot or explicit live closeable quantity. The reviewer treated historical prose as a current duplicate order.

## Resulting behavior

- Evaluation compares the normalized desired exit with the venue order: instrument, action/direction, price, remaining quantity, route and intent. Equivalent orders remain in place without another pending action or model calls. Confirmation repeats this check for actions already queued.
- Changed terms create an explicit replacement candidate. The old order remains until review and fresh policy validation pass. Cancellation must produce terminal order evidence; cumulative fills are accounted before sizing and validating the replacement. Any fills since review reduce the approved remaining quantity.
- Every exit submission, including an urgent IOC, checks for a conflicting exit again after final policy reads. Failed cancellation, unknown status, an untracked order or a changed book cannot authorize overlapping exits.
- Reviewer context contains the fresh venue snapshot, live position direction/quantity, and a concrete desired order. Saved rule prose is explicitly historical. Fresh trigger values replace stale capture arithmetic. Explicit model vetoes remain binding.
- A new rule ID or eight hours of age alone does not cancel a still-valid patient exit. Position, bounds, protection and urgent-exit checks remain active; venue expiry is still reconciled normally.
- Own best bids do not trigger repeated self-outbidding. Repricing a partially filled put-sale tranche does not silently enlarge its approved remainder.

The change uses existing operational tables. There is no schema migration, historical backfill, balance repair or alteration of closed historical actions. Fresh confirmation context is saved for actions being executed so subsequent order comparisons retain the actual intent and tranche policy.

## Verification

Regressions execute production declarations and shared modules. Native SQLite tests exercise the real transactional order-accounting code, including unknown status, cancellation acknowledgments, late and partial fills, changed books, rollback and exactly-once fill notifications. Model and venue requests are mocked; tests do not submit live orders. Production rollout is verified through the deployed commit, process logs and read-only state checks.
