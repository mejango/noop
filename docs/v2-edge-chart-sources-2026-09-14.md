# V2 EDGE ranking and chart sources

Both current market selectors calculate DTE-normalized EDGE for each eligible
contract before comparing candidates. This includes the dedicated chart
observations. Regression tests use pairs where the lower RAW score wins on
EDGE, in both ticker orders, for puts and calls. No trading-selection change
was required.

The PUT chart nevertheless mixed dedicated market observations with entry-rule
candidate telemetry. A candidate's raw score can describe a proposed patient
bid instead of the observed market ask. Normalizing that score and taking MAX
could override the genuine market winner with a hypothetical order price.

Read-only production evidence at `2026-09-14T11:05:32.830Z`:

- ETH-20261127-1900-P had an ask of $41.70 and a proposed bid of $36.92.
- The planned bid produced EDGE 0.0036009723981698947.
- The dedicated market maximum was 0.003188199225565326.
- Another candidate had a zero ask but a positive planned-bid score; it also
  contributed to the old chart despite unavailable market pricing.

Executing the corrected query against production in SQLite read-only mode
changed 328 of 853 matched PUT points in the captured 24-hour chart. Twelve
CALL points changed only by small differences in DTE evaluation timing. CALL
candidate scores use the observed bid, so the patient-price defect was specific
to puts.

Both chart getters now use the same source policy:

1. Dedicated market observations are authoritative from the first recorded
   timestamp for that side, independently of the requested chart window.
   Later gaps remain missing; rule candidates cannot replace or supplement them.
2. Earlier fallback history reconstructs RAW from the recorded quote and delta:
   abs(delta)/ask for puts, bid/abs(delta) for calls. Nonpositive or missing
   quotes and out-of-policy deltas or DTE are excluded. Stored candidate RAW
   and proposed order prices are not used for the market chart.
3. Each fallback candidate is normalized before MAX per observation. Wider
   chart buckets average those per-observation maxima.

Dedicated PUT coverage begins August 14, 2026; CALL coverage begins August 2.
Earlier candidate history is a limited sample of recorded rule candidates,
not a reconstruction of the complete option chain. In addition, CALL winners
recorded before the August 9 normalization change were selected under an older
ranking formula. Recomputing their EDGE does not recover discarded contracts.
These historical coverage limitations remain; no backfill is claimed.

Validation: 946 tests pass, including eight actual-getter SQLite regressions
and the two-sided ranking reversal test. The dashboard production build passes.
Desktop/mobile previews use the corrected production query output.

The changes are read-only reporting corrections. No schema, saved historical
records, or trading rules were changed.
