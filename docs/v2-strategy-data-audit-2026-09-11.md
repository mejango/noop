**V2 strategy and data audit — 2026-09-11**

Scope: committed V2 at `7864bc91a16144b1f6f9f1ec38da9af3db375759`, the last commit pushed to `main` in this conversation. All findings below were checked against an isolated archive of that commit. V3 and the new Strategy framework are excluded. The deployed process SHA, production environment variables, exchange account, and production database were not accessed; this is an audit of the production V2 code, not proof that every failure has occurred live.

The highest-impact problems are inconsistent enforcement after an order is changed, incomplete reconciliation of real fills, and data/reporting paths that lose the distinction between unknown, zero, cashflow, and profit. The strategy has many checks, but their ordering and repeated implementations leave gaps. More advisor instructions will not repair those gaps.

The local database contains one old spot observation and no trades or option snapshots. It cannot establish historical profitability, tail protection, or incident frequency. Reproductions used mocked venue responses, the actual extracted production functions, and temporary databases; no trading calls were made.

**Production flow inspected**

`venue quotes + collateral + positions → momentum/value research → advisor rules → candidate/exit gates → pending actions → two model confirmations → final price/order-type selection → execution/retry → resting-order reconciliation → SQLite → dashboard, trade reviews, and research`

Operational state and observations share SQLite. Knowledge pages and lessons provide additional model context. Several consumers separately reconstruct policy, P&L, and historical aggregates. That duplication is already causing observable disagreement.

**Priority findings**

**1. High — Final sell orders can violate the rule that approved them.**

The confirmation layer accepts reviewer prices within 0.5–2 times a reference quote but does not recheck a sell-call's minimum bid or CALL EDGE at the final price. Reproduction: live bid $10, delta 0.1, 8.5 DTE, minimum bid $8 and minimum edge 65; both reviewers choose $5 GTC and the executor receives $5, whose edge is 50. An unchanged book might fill better, but the submitted lower bound no longer enforces the rule. [Final price selection](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L11489).

Separately, the post-only sell retry calculates cached best bid plus a tick without preserving the original sell floor. Actual execution reproduction submitted [$100, $90.01] after the first order was rejected, despite a $100 approved floor. The buy retry already preserves its ceiling. [Retry price](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L10580), [retry submission](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L10820).

Fix: construct the normalized order, enforce its price/score/size/margin/budget invariants, and repeat the same checks after every mutation. Carry immutable approved minimum/maximum prices through retries. The recent maker-price prompt improvement helps describe the order; it does not replace this final gate.

**2. High — A later margin-data outage can let an already-pending call proceed unchecked.**

Initial candidate creation does require margin data. But confirmation only rejects an excessive margin calculation when the calculation is available, and retry explicitly permits unavailable margin. A candidate approved earlier can therefore execute after a later account fetch fails. The actual confirmation function proceeded with `marginState=null` in an isolated reproduction. [Confirmation](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L11168), [retry check](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L8695).

Fix: keep the action pending until a fresh valid account snapshot supports the new exposure. Exchange acceptance does not establish compliance with the strategy's tighter utilization limit.

**3. High — V2 can stop tracking real orders and miss their subsequent fills.**

Three paths break the connection between exchange state, spend, and local status:

- A GTC order is recorded as resting only when its initial fill is exactly zero. A five-contract put purchase with one immediate fill and four still open books the first fill but creates no resting record. [Initial receipt](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L10951).
- Stale/orphan cancellation marks the local order cancelled even if `cancelOrder` returns null, and does not book fills already visible in the open-order response. A reproduced order with one fill, failed cancellation, and an open remainder left spent at $0 with no trade row. [Cancellation](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L10182).
- An unavailable/empty history lookup after “Does not exist” becomes a fictitious zero-fill cancellation. The history search checks only 100 orders over seven days; an error is indistinguishable from absence. [History](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L3137), [synthetic status](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L3186).

Fix: preserve every nonterminal remainder and every unresolved order. Apply cumulative fill deltas exactly once, transactionally with budget/order/action changes. Release reservations only after confirmed terminal state with reconciled fills.

**4. High — A patient exit can block a changed risk-management exit, and can outlive its position.**

Exit deduplication matches instrument/action, not intent or remaining closeable size. A resting profit-capture bid can block a new `threat_management` IOC. The manager revalidates entry rules but does not equivalently revalidate exits against current position and intent. These synthetic resting exits are explicitly submitted without native reduce-only protection. A reproduced changed-rule/no-position scenario caused no cancellation and still blocked the new exit. [Matcher](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L8341), [skip](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L8872), [manager](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L10140), [synthetic flags](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L8420).

Fix: reconcile resting exits against live closeable quantity and current intent each tick. A permitted urgent exit should replace an obsolete patient order after cancellation/fill reconciliation. A vanished position must invalidate its synthetic close order.

**5. High — Patient-order exceptions bypass more than the intended price condition.**

Exit triggering uses `evaluateConditions(...) || patientBuybackPlan || patientSellPutPlan`. A patient plan can therefore override unrelated conditions. Reproduction: an 80%-capture rule also requiring spot ≥ $3,000 produced a patient trigger at spot $2,500. The patient-buyback confirmation override can then override both model rejections. [Trigger](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L8795), [planner](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L7427), [vote override](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L11369).

Fix: substitute hypothetical patient-fill economics only for the matching price/capture condition. Preserve every other rule condition. Any permitted reviewer override should name a narrow, typed reason rather than treating the whole rejection as disposable.

**6. High — Option history omits the contracts needed to evaluate tail outcomes.**

The bot stores quotes only for currently eligible entry candidates. Puts must remain below spot, calls above spot, and both within entry DTE windows. Contracts can disappear from history precisely when puts become valuable, short calls become dangerous, or positions approach expiry. Held-position quotes are fetched but are not included in this write. [Entry filters](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L3462), [call filters](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L3476), [persistence](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L13155), [position quotes](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L12807).

The outcome labeler compounds this: spot alone finalizes an outcome as evaluated; missing data finalizes it as missing immediately. A valid option quote arriving one minute later is never considered, despite a configured six-hour window. Reproduced with the committed schema and implementation. [Due selection](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/db.js#L1677), [finalization](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/db.js#L2242).

Fix: maintain an observation universe independent of entry eligibility, including held instruments and candidates with outstanding evaluation horizons. Keep incomplete labels pending until their actual deadline, with separate spot/option completeness. Audit missingness by moneyness and DTE before trusting historical conclusions.

**7. High — “Realized P&L” is premium cashflow, and fees can be invented as zero.**

Both bot and dashboard sum sale premiums minus purchase costs, including open positions, and label the result realized P&L. Opening a short call for $100 reports $100 realized profit immediately. A different P&L path adds estimated settlements, so the panels need not agree. The account route fabricates zero trade fees, which are displayed. [Bot cashflow](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/db.js#L1589), [dashboard cashflow](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/dashboard/src/lib/db.ts#L628), [label](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/dashboard/src/components/AdvisorDrawer.tsx#L1947), [zero fees](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/dashboard/src/app/api/lyra/account/route.ts#L34).

Fix now: call this metric “Gross options cashflow” and show unknown fees as unavailable. The durable fix is one reconciled fills/fees/settlements source feeding both reporting and bot state. Derive's V2 [trade-history API](https://docs.derive.xyz/reference/post_private-get-trade-history) supports subaccount/order/time filtering and pagination; use venue records for reconciliation rather than treating local notifications as a complete ledger.

**8. High, deployment-dependent — The bundled container can keep a healthy dashboard beside a dead bot.**

`start.sh` backgrounds the bot and waits only on the dashboard. The bot's fatal handlers and watchdog exit expecting PM2, but this container launches the shell wrapper without PM2. An isolated stub reproduction had the bot exit 17 while the dashboard and wrapper remained alive; the wrapper eventually returned zero. [Startup](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/start.sh#L12), [container command](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/Dockerfile#L50), [watchdog](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L13478).

Fix: supervise both children and make bot failure restart/fail the service, with bot heartbeat freshness in health reporting. This affects the bundled deployment; a separately supervised bot service may already recover correctly. Confirm the actual production topology.

**Accuracy and strategy-design findings**

**9. Medium — Estimated historical settlements can change or disappear as more data arrives.**

The report uses all orders with only the latest 100,000 spot rows, independent of the requested historical period. Settlement reconstruction accepts arbitrarily old earlier spot observations and silently omits events when no earlier quote remains. The same March 6 call produced a $500 settlement using February 28 spot, then no settlement when available history began March 7. [Inputs](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/dashboard/src/app/api/pnl-report/route.ts#L147), [rolling sample](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/dashboard/src/lib/db.ts#L71), [settlement lookup](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/dashboard/src/lib/expiry-settlement.ts#L44).

Fix: persist actual settlement events. Until then, explicitly label estimates, require a bounded quote age, and select observations around each expiry. Historical events must not depend on a moving sample of recent data.

**10. Medium — Deposits and withdrawals are reported as return and drawdown.**

Return and drawdown are computed from raw portfolio-balance changes. A $1,000 deposit into an unchanged $10,000 portfolio becomes +10% return; a withdrawal becomes drawdown. Current external-ETH configuration is also applied retrospectively rather than as dated exposure changes. [Return](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/dashboard/src/app/api/pnl-report/route.ts#L300), [drawdown](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/dashboard/src/app/api/pnl-report/route.ts#L195).

Fix: distinguish portfolio value change from strategy performance. Record capital flows and historical exposure settings before calculating adjusted returns/drawdowns.

**11. Medium, policy gap — “Replacement protection” proves existence, not adequate coverage.**

Any positive longer-dated put qualifies as replacement and can authorize a full aging-put exit. The actual helper accepted 0.01 contracts at a $500 strike as replacement for 100 contracts at $2,000. This is insufficient evidence for the claim that protection remains adequate, even if reducing protection is sometimes intended. [Replacement test](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L7313), [roll gate](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L7694).

Fix: define the V2 strategy's minimum post-roll protection in quantities or named stress payoffs, and check it explicitly. Do not silently equate “a later put exists” with coverage continuity.

**12. Medium — The “fresh best” comparison uses different current and historical rankings.**

Quality-based selection may be intentional; the concrete defect is comparing its selected candidate with a differently defined historical maximum. Composite selection uses a fixed base multiplied by coarse quality buckets. Two puts with economic edge 0.00422535 and 0.00625 tied at 195.054; swapping input order changed the selected contract. Current “fresh best” uses that selected candidate's edge, while historical comparison uses maximum edge. Against historical 0.005, input ordering alone flipped the signal. [Composite](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L2100), [selection](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L2197), [historical maximum](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/db.js#L811).

Fix: compare the same current/historical statistic and use an explicit deterministic tie-break. Keep quality-selected execution candidates distinct from market-best measurements.

**13. Medium — The put “skew” penalty also measures the difference between expiries.**

The signal compares average 45–78-day put IV against 5–12-day call IV. A surface with zero put/call skew at each expiry—40% at seven days and 60% at 60 days—produced 20 points of apparent skew and a penalty. [Population/measurement](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L1869), [penalty](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L2064).

Fix: compare matched expiries and comparable absolute deltas, or explicitly name this a cross-expiry IV spread and stop interpreting it as put-specific richness.

**14. Medium — The dashboard advisor describes a different strategy.**

Its active system prompt says roughly 0.01-delta puts, 60–90 DTE, 6% annual spend and ten-day cycles. Committed V2 uses 3.33%, fifteen-day cycles, and a 45–78-day put window. It also equates exhausted purchasing budget with being unprotected, ignoring existing hedges. [Prompt](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/dashboard/src/app/api/ai/chat/route.ts#L30), [budget language](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/dashboard/src/app/api/ai/chat/route.ts#L50), [configuration](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/config.json#L2).

Fix: generate this factual strategy context from shared configuration and computed holdings. Spending authorization and existing coverage are different quantities.

**15. Medium — Account retrieval failure can look like an empty account.**

The V2 client checks HTTP status but accepts HTTP-200 API error objects. Account mapping substitutes empty arrays for invalid shapes and returns a successful, cacheable response. Reproduced with an invalid-signature error. Network failures also become empty account data in AI snapshots. [Client](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/dashboard/src/lib/lyra.ts#L84), [mapping](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/dashboard/src/app/api/lyra/account/route.ts#L49), [snapshot fallback](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/dashboard/src/lib/snapshot.ts#L78).

Fix: validate API errors and response shapes, retain explicit freshness/availability fields, and distinguish unknown positions from a confirmed flat account.

**Research and data-organization findings**

The following research tools are committed V2 tools; they are not claims about processes known to be scheduled in the production container.

**16. Medium — Correlation features can see later observations in the same hour.**

Whole-hour aggregates are joined onto intrahour decisions. A 10:01 candidate received a spot feature of 2,500 after adding a 10:59 observation of 3,000 to its initial 2,000 observation. This changed strike distance from +10% to −12%. [Feature construction](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/profit-correlation-engine.js#L557), [join](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/profit-correlation-engine.js#L775).

Fix: use timestamp-bounded observations or completed prior-hour aggregates. Recompute affected analyses after removing lookahead.

**17. Medium — Backtests treat zero or unknown bid depth as unlimited liquidity.**

With quoted-depth enforcement enabled, `bid_amount <= 0` or missing becomes Infinity. A synthetic replay filled 2.25 contracts and reported $20.25 profit with zero/null depth, compared with 0.05 contracts and $0.45 profit at depth 0.05. [Simulator](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/research/sell-call-backtest/simulator.js#L174).

Fix: zero depth prevents a simulated fill; unknown depth needs an explicit, reported assumption. Keep this distinct from assumptions about future maker fills.

**18. Medium — The research “current strategy” baseline is stale, and a main report command crashes.**

The default current-edge comparison still uses an older composite/floor 80, whereas production uses `raw × (8.5/DTE)^0.12` and fallback floor 65. An identical candidate scored 148.735 in the research baseline and 75.548 live. [Policy](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/research/sell-call-backtest/policies.js#L95), [production label](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/research/sell-call-backtest/report.js#L60), [live score](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/call-score.js#L7).

Separately, correlation-report metadata queries `MIN/MAX(timestamp)` before selecting the correct columns for candidate/outcome tables. A full report against the committed canonical schema fails with `no such column: timestamp`. [Metadata query](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/profit-correlation-engine.js#L343).

Fix: share the production scoring implementation/defaults or name the baseline as historical. Add a report smoke test against the real schema, with a declared time column per table.

**19. Medium — Rebuilding hourly history changes the meaning of the data.**

Live rollups average per-tick averages and retain the last total OI. Backfill averages all option rows and sums OI across observations. Identical raw data reproduced live `{avg_iv:150,total_oi:60}` versus rebuilt `{avg_iv:175,total_oi:70}`. OI is a stock measurement and summing repeated observations inflates it. [Live aggregation](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/db.js#L1341), [backfill](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/backfill-hourly.js#L69).

Raw writes and rollups are also separate, with rollup errors swallowed, despite the documented atomic-write intent. [Write path](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/db.js#L1792), [principle](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/PRINCIPLES.md#L29).

Fix: define and share aggregation semantics, retain sufficient counts/sums to reproduce them, and surface repairable rollup failures. Validate that rebuilding from raw observations preserves results.

**20. Medium — The seven-day call-premium metric queries the wrong enum.**

Bot and dashboard query `option_type='call'`; ingestion writes `C`/`P`. A normal C quote therefore yields null and deprives advisory context of the intended comparison. [Bot query](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/db.js#L1448), [dashboard query](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/dashboard/src/lib/db.ts#L212), [ingestion](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/db.js#L1836).

Fix: use one option-type representation. Scope the premium comparison to a consistent maturity/delta population so changes in instrument mix do not masquerade as richer premiums.

**21. Medium — Much of the test suite verifies copied policy instead of production policy.**

An isolated source-read interceptor changed the real production `evaluateConditions` to immediately return false. All 482 trading-system tests still passed. A direct comparison also found different results between the production patient-maker helper and its test copy for the same JSON-string rule criteria. [Copied helpers](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/test/trading-system.test.js#L48), [copied conditions](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/test/trading-system.test.js#L410), [real conditions](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L7219).

Fix: extract small import-safe policy/pricing modules and test those directly. Add lifecycle tests around actual confirmation/execution/reconciliation with mocked venue responses. The separate model/API suite already tests actual helper source, so this is not a claim that every existing test is ineffective.

**Additional simplifications worth making**

- Keep SQLite, but separate concepts: immutable observations/events; orders and fill reconciliation; derived budget/position state; rebuildable research/reporting aggregates. Today `orders`, `resting_orders`, `pending_actions`, `bot_state`, and inferred `position_lifecycle` can disagree without a single reconciliation contract.
- Make rule identity, quote age, source, and policy version explicit in decision evidence. A shared tick timestamp is not enough to establish that asynchronous market/account inputs were equally fresh.
- Stop rebuilding every instrument lifecycle from all successful orders on every tick. Update affected instruments from newly reconciled events and periodically verify the projection. [Full scan](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/bot/db.js#L2310), [every-tick call](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/script.js#L13213).
- Preserve raw evidence, but move old observations into a deliberate archive/partition plan before they dominate the operational database. Hourly tables reduce chart work; they do not reduce the size or backup cost of the never-pruned raw tables. [Retention policy](https://github.com/mejango/noop/blob/7864bc91a16144b1f6f9f1ec38da9af3db375759/PRINCIPLES.md#L38).
- Centralize strategy facts and calculations used by the bot, dashboard prompts, research baselines, and tests. The stale chat instructions, null premium query, and divergent backtest score are concrete reasons to do this.

These are targeted V2 cleanups. None requires finishing or redesigning V3.

**Recommended fix order**

1. Enforce final order bounds and fresh margin; repair partial-fill/cancellation/unknown-status reconciliation; revalidate synthetic exits.
2. Remove broad patient-rule bypasses and confirm the actual container supervises the bot.
3. Correct misleading P&L/fee/availability labels, then reconcile fills, settlements and capital flows into one accounting source.
4. Preserve the full outcome observation universe, repair incomplete labels, and eliminate research lookahead/liquidity assumptions before further score tuning.
5. Consolidate shared policy helpers and aggregate definitions; then evaluate ranking/skew/roll-coverage policy using repaired data.

**Validation record**

The isolated committed V2 snapshot passed 482 trading tests, 10 model/API tests, and 35 backtest tests under Node 20.19.1. Source-level and mocked-function reproductions above reveal gaps those suites do not cover. The rollup discrepancy was reproduced using the real committed database writer and backfill command against a temporary database. No production database, live account, order, bot configuration, or deployment was modified. This audit report is the only repository artifact created by this audit.
