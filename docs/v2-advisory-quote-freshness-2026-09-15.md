# V2 advisory quote availability and publication freshness

## Observed failure

Advisory `adv_1789498516860` started at 2026-09-15 18:55:16 UTC and published at 19:00:01 UTC. Its input snapshot at 18:55:15 contained four PUT contracts inside the entry DTE/delta bounds, but none had a positive ask. Marks and Greeks were present. The old rolling context represented the absent executable score as `0`, and the candidate text said “No qualifying puts found.”

Quotes recovered at 18:58:40, before publication. The November 27 1600 PUT had ask 24.1 and delta -0.0627, giving PUT EDGE approximately 0.003028. The advisor still published its earlier unavailable-quote assessment because asynchronous journal work passed a previously captured tick into advisory generation and no final availability check ran.

The broad spread headline was a separate data-scope problem: a mean over the latest recorded snapshot, all tenors, with absolute delta 0.02–0.12. It was repeated under historical windows and could be dominated by tiny marks and extreme asks. It did not measure the spreads of entry-eligible contracts or actual execution loss.

## Forward behavior

- PUT and CALL current scores and relative comparisons are null when executable entry quotes are absent. Coverage explicitly distinguishes available quotes, eligible contracts without quotes, no eligible contracts in a covered universe, and unknown coverage. Candidate identities, quote counts, missing metadata and receipt times accompany the summary.
- Every advisory starts with fresh spot, instrument metadata, positions and quotes. Quotes cover entry-window expiries plus every held option expiry. Strict refresh failures abort; no quote map from a previous tick is reused.
- After the complete model review, a second fresh read checks eligibility, instrument coverage, executable quote identities, held positions and held-option quote availability. A change reruns the entire review once using the newly fetched snapshot. Another change, a failed read, or a failed review defers publication and uses the existing retry backoff.
- Held-option coverage includes bid, ask, mark and Greek/IV availability even when that contract is outside the entry window. This protects expiring-position commentary as well as entry candidate commentary.
- Active rules and their matching journal artifacts publish in one SQLite transaction. Failure preserves the preceding rulebook and assessment and cannot set advisory success.
- Publication records both input time and final availability-check time. The dashboard shows those times and preserves unavailable scores.
- Broad spread data appears once, with timestamp, recorded universe, separate observed/quoted/spread sample counts, mean and median. Named candidates carry their own spreads and quote timestamps. A valid spread needs positive, uncrossed bid and ask and a positive mark.
- All main advisory stages distinguish current margin headroom from a severe-crash survival analysis; no such simulation is supplied by this change.

The final check deliberately compares availability and position identity, not every numeric price movement. It does not make a multi-minute assessment tick-current, and it does not replace the executor's fresh economics, margin and position checks. Under unstable quotes, the extra review can increase latency and model usage; a second availability change leaves the existing rulebook in place.

## Telegram run notices

Each full advisory review sends a start notice to the existing Telegram chat. Notices distinguish the normal eight-hour schedule, retries after failed/deferred advisories, and additional reviews after market data or position changes. The run ID and review number let the two passes be grouped together.

Notifications start after a valid initial snapshot and the advisory mutex check. Missing API keys, an already-running advisory, and failed initial data reads do not send a notice. Telegram delivery runs independently: a delivery failure cannot abort publication or cause another AI review. These notices do not change scheduling or add model calls.

## Data and strategy scope

V2 only. No migrations, historical row repairs, historical score backfills, or changes to PUT/CALL formulas, ranking, entry bounds, execution price gates or cooldowns. Normal advisory publication still retires the prior active rules and appends new rules and journal entries, now atomically. Existing misleading historical advisory text remains unchanged.

## Validation

- 57 focused tests exercise actual quote/context/formatter declarations, strict fetch adapters, snapshot coverage, publication orchestration, the real advisory wrapper and publisher, Telegram run notices and delivery failures, and SQLite rollback after a mid-publication journal failure.
- Full suite: 1,081 passed (481 trading, 554 Node test cases, 46 backtest cases).
- Dashboard production build passed, including TypeScript validation.
- Read-only replay of the three captured November PUT contracts: the initial absent score becomes null; recovered quotes produce 0.003028 for the 1600 PUT; the first draft is discarded and only the second review publishes. This replay covers those captured contracts, not full-exchange historical coverage.

The 24-hour chart's earlier RAW discontinuity is consistent with October 30 crossing below the 45-DTE entry boundary at 08:00 UTC. RAW and EDGE retain independent eligible winners. This change does not rewrite chart history or force their lines to follow the same contract.
