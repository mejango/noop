# V2 chart candidate eligibility — 2026-09-15

## Problem and scope

The main chart's RAW lines filtered delta but not expiry. Retained observations include contracts that have left the entry window, so an ineligible contract could keep winning RAW while EDGE selected an eligible alternative. For example, the October 30 puts fell below 45 DTE at 08:00 UTC on September 15, yet continued to win PUT RAW later that morning.

Both PUT and CALL charts now apply the existing entry windows to each quote at its observation time:

| Side | Delta | DTE | Required quote and RAW value |
| --- | --- | --- | --- |
| PUT | −0.12 to −0.02 | 45 to 78 | Positive ask and ask delta value |
| CALL | 0.04 to 0.12 | 5 to 12 | Positive bid and bid delta value |

Bounds are inclusive. Missing expiry and invalid quotes cannot supply a winning value. Fractional seconds are retained when checking the DTE boundary. A sample without an eligible contract produces a null value for that side.

RAW and EDGE still select their own winners independently. Their formulas are unchanged, and their lines can still have different shapes. This is the minimum market-candidate eligibility filter; the chart does not simulate account-specific execution gates or historical rule state.

## Implementation

`dashboard/src/lib/db.ts` shares eligibility predicates across tick RAW, bucketed RAW, candidate heatmaps, current best scores, and best-score details. Sampled heatmaps check the selected quote's timestamp, not the bucket label, and do not fall back to stale candidates from earlier in the bucket.

The old `options_hourly` summaries lack expiry, so long-range chart RAW values are now aggregated from retained observations. This preserves hourly MAX RAW aggregation before the route's existing range downsampling. The options bucket expression explicitly floors the quotient because the SQLite driver binds the bucket parameter as a real number. Spot bucketing is unchanged.

No historical rows, rollups, schema, trading policy, or research-only hourly series are changed. Historical charts can change because read-time filtering now excludes ineligible observations. Retained observations are not a reconstruction of contracts that were never recorded.

## Validation

- Eight regression tests execute the production TypeScript queries against SQLite, including both sides, historical and fractional-second boundaries, invalid candidates, independent RAW/EDGE winners, bucket gaps, heatmap sampling, and the actual 90d/365d/all chart route paths.
- The full V2 suites pass: 481 legacy trading, 510 Node tests, and 46 backtest tests (1,037 total).
- The dashboard production build passes.
- Read-only production benchmarks on SQLite 3.51.2: 12h RAW 27 ms, 24h RAW 36 ms, 30d hourly RAW 785 ms, all retained hourly RAW 4,673 ms (4,896 rows), and 12h sampled heatmap 3 ms. These measure individual queries, not full HTTP responses. Long-range first loads and stale-cache refreshes cost more than the former expiry-free summaries; synchronous SQLite queries briefly delay other dashboard requests. The existing response cache coalesces requests per range; the bot runs separately.
