# OOF V3 productization — implementation plan

Status: implementation sequence derived from the [operating specification](../oof-operating-spec.md), 11 September 2026. This plan does not authorize a production deployment or alter the V2 strategy. Thresholds such as 25 DTE, 80% capture, and 45% margin belong to a Strategy release, not the shared OOF execution layer.

## Architecture choice

OOF is the shared V3 platform; Noop remains the existing operation and one Strategy offering on it. The `noop-v2-reference` package keeps its name and becomes the initial Noop Strategy reference for OOF. This naming decision was recorded 14 September 2026 and does not broaden the current ETH/action scope, change activation funding or economic policy, or enable live execution.

Begin with a modular application and isolated account workers. Share market ingestion and source history; keep customer ledgers, signing authority, budgets, and private Strategy state separately identified. Proposed modules are `strategy-contract`, `strategy-runtime`, `market-history`, `mandates`, `execution`, `capital-ledger`, `customer-api`, and `juicebox-adapter`. These names describe boundaries to extract, not a requirement for separate services or an assertion that they exist.

Multiple Strategies are separate offerings on OOF. The initial account model still runs one active Strategy release per separately funded customer mandate and Derive subaccount; a catalog of Strategies does not combine them in one account. The `noop.strategy/v1` wire identifier remains unchanged for v1 compatibility and does not name the platform.

The Strategy's output is a conditional rule contract. OOF owns the scheduler, reservations, signed-operation lifecycle, financial ledger, and customer control plane. The Derive integration is the venue adapter. Dashboard and partner interfaces consume the same normalized projections and do not need a trading-capable key.

Reference extraction characterizes V2's economic decisions offline. It does not waive the new execution contract: any legacy execution mode that cannot meet close-only or other required guarantees must be identified and excluded from live activation until a validated adapter implementation exists.

## Milestone 0 — product contracts

Deliverables prepared in this step:

- [Operating specification](../oof-operating-spec.md): decisions, authority, customer mandates, and product boundaries.
- [Strategy contract](../oof-strategy-contract.md): inputs, output rules, evaluation, runtime, and versioning.
- [Capital/performance specification](../oof-capital-and-performance.md): activation, conversion, exits, valuation, and proposed designer compensation.
- This dependency-ordered plan and release acceptance criteria.

These artifacts are a reviewable design baseline. An example parameter or fee rate in them is not consent to change the running bot or charge a customer.

## Milestone 1 — Strategy contract and reference extraction

Implementation status, 11 September 2026: the [first offline batch](../../strategy/README.md) supplies machine-readable schemas, a strict validator, exact decimal condition evaluation, immutable in-memory control replay, and a bounded captured-rule V2 adapter with a different-parameter fixture. Source-based tests cover extracted pure helpers. Full advisory/research orchestration is not yet extracted, and scheduling produces previews pending later risk, reservation, and execution work. Run `npm run test:strategy` and `npm run strategy:replay`.

Implement the typed contract validator, deterministic condition evaluator, and a `noop-v2-reference` package. Extract the existing research/selection, rule-generation, confirmation, fallback, and policy-validation behavior into the package. Keep the existing production entry point and parameters unchanged while comparing recorded decisions offline.

The initial package preserves existing behavior including the 25-DTE roll, 80% capture, current premium budget, existing monetization rules, 45% normal target, and the existing breakout/buffer settings. A later customer-facing release may choose different settings. Name the two releases separately if their behavior differs.

| Current source | Extraction work |
| --- | --- |
| `bot/config.json`; `script.js` policy/constants | Put budget basis/rate/window, call targets, buffer, and breakout behavior become release configuration. |
| `bot/put-score.js`; `bot/call-score.js` | Version score algorithms and features as Strategy implementations. |
| `validateAdvisorRuleContract`, put-exit guards, buyback normalization | Separate contract shape/executable bounds from this Strategy's economic validators. Do not silently restore legacy numbers. |
| Required/fallback watchers and advisory/confirmation prompts | Consume the exact selected policy; prompts are part of release provenance. |
| Margin estimation and sizing | Name the metric and manager; distinguish entry targets, stress policy, venue checks, and emergency policy. |
| Dashboard and DB-derived scores | Store algorithm/release provenance; preserve historical interpretations. |

Acceptance:

- Actual source-based reference replay produces the established decisions or a reviewed explanation of each intentional difference.
- A second illustrative release using a 35-DTE roll, 70% call capture, and 30% entry-utilization target passes through generation, validation, scheduling, and reporting without inheriting 25/80/45. Those sample values are a test fixture, not a recommended trading policy.
- Unknown actions, stale revisions, unbounded quantities/prices, unsupported fields, invalid units, and attempted policy mutation are rejected.
- Financial values remain exact; evaluation is bounded and cannot execute arbitrary code supplied in a condition string.
- No SDK, credential, database, or bot-loop side effect occurs when importing a Strategy for validation/replay.

## Milestone 2 — mandate state, ledger, and account exclusivity

Implementation status, 11 September 2026: the [second offline batch](../../product/README.md) adds a private SQLite mandate/event store, atomic decision and private-state commits, account exclusivity, exact financial receipts, scoped cash reservations, durable uncertain attempts, and atomic fill recovery. Run `npm --prefix product test` and `npm --prefix product run demo`. The gateway requires `offline_simulation`; it does not enable live dispatch. Production authentication, global deployment/account coordination, credential fencing, venue evidence/nonce integration and reconciliation of unsupported outcomes remain acceptance work. The V2 runtime and database are not connected to this package.

Introduce explicit customer, mandate, release, account, and valuation identities. Separate shared market observations from account operations and private Strategy state. Replace singleton assumptions only in the new product path; preserve the V2 database as historical evidence.

Build a durable operation ledger and dispatch queue with idempotent local accounting. Acceptance, fills, local accounting, and L1 settlement are separate observations. Normalize deposits, withdrawals, option settlement, interest, debt, spot conversion, fees, and reserves. The existing order cash-flow query is not net performance.

Relevant starting points: `bot/db.js` (mixed market and account tables, singleton `bot_state`, `getRealizedPnL`); `integrations/derive-v3/accounting.js`, `index.js`, and `state.js` (transactional fills and uncertain-send protection).

Acceptance:

- Two mandates cannot read or mutate each other's state, keys, reservations, or withdrawal recipients.
- Crash tests cover before send, after send, after ACK, after a partial fill, after local commit, and before settlement; no fill or fee is booked twice.
- Filled orders discovered after an uncertain send are imported against authoritative evidence and the originating mandate rather than simply clearing a journal flag.
- Known-accounting recovery after a crash succeeds without guessing. An unprovable outcome continues to stop new exposure.
- Ownership of the account writer is enforced at the signing boundary. A database lease alone is insufficient if an old worker can continue signing after failover; stale workers must be fenced from credentials/dispatch.

## Milestone 3 — history service and isolated Strategy runtime

Publish as-of history queries, immutable decision snapshots, dataset/feature versions, and private state with compare-and-swap revisions. Retain event time and first-available time so corrected or delayed data cannot leak into earlier replays. Preserve raw rows and explicit resolution tiers per `PRINCIPLES.md`.

Run arbitrary algorithms in isolated workers with declared resource and network permissions. Host-controlled input snapshots and output/state commits bind each run to one mandate revision. Record non-deterministic inputs/results so execution decisions remain auditable. Keep signing authority out of the runtime.

Acceptance:

- Future data, later corrections, or newly recalculated historical scores are unavailable in an earlier as-of run.
- Crashes and concurrent runs cannot commit stale private state or duplicate conditional-rule revisions.
- Designer code cannot read host credentials or another mandate's financial/private state.
- A missing feature, timeout, or algorithm failure produces a visible no-decision result; it does not erase managed positions or grant fallback trading authority.

## Milestone 4 — conditional execution and bounded resources

Build the account-specific scheduler around the agreed rule contract. Atomically reserve premium budget, executable quantity, closeable positions, margin capacity, and cash needed by pending commitments. Recheck actual account and venue state before signing.

Existing seams: `evaluateTradingRules`, `confirmAndExecutePending`, `manageOpenOrders`, and `executeOrder` in `script.js`; `integrations/derive-v3/legacy-transport.js` is a migration seam, not the final public Strategy interface.

Acceptance:

- Multiple individually valid rules cannot collectively overspend or close the same contracts twice.
- Partial fills consume only their confirmed allocations; cancellation timeout does not release resources early.
- Rule expiry, replacement, and customer exit are resolved without losing already accepted venue operations.
- A replacement-hedge dependency becomes satisfied only at the declared filled quantity. An ACK or resting buy is not a hedge.
- Economic corrections are rejected or returned to the Strategy. Venue rounding stays within accepted limits.
- Customer exit intent has priority over new Strategy entries and is not vetoed by a Strategy-specific profit threshold.

## Milestone 5 — funding, spot recycling, and customer exit

Implement 10% activation from confirmed credited ETH and the accepted exposure reference. Add explicit `buy_spot_eth` and `sell_spot_eth` capabilities, a capital-availability calculation, debt/settlement handling, and an asynchronous withdrawal state machine. These spot capabilities are new automation: the current V2 prompt describes excess conversion as manual.

Treat later deposits, changed protected exposure, and fee realization according to explicitly selected terms. Full unwind is required; partial withdrawals are an optional extension whose residual-mandate and valuation terms must be selected before implementation. Do not infer a perpetual 10% maintenance margin requirement from the activation rule.

Acceptance:

- 20 ETH declared exposure with 2 ETH correctly credited can activate; an uncredited deposit or 1.99 ETH credited does not satisfy the stated requirement. Exact asset decimals and any declared rounding policy govern production.
- External ETH never funds a call liability, fee, withdrawal, or margin calculation.
- Put proceeds cannot be consumed twice or used before the venue recognizes them; ETH purchases do not consume reserved settlement/withdrawal funds.
- Closing positions, servicing liabilities, selling ETH where authorized, and withdrawing to the permitted destination reconcile across partial fills and delayed settlement.
- Full unwind uses its accepted exit policy even when ordinary Strategy roll, profit, budget, or retained-protection conditions would require holding. Authorization, price bounds, accounting, resources, and venue checks still apply.
- Liquidation, negative equity, bad debt, and residual assets produce explicit resolution states and ledger events. Closure requires evidence that remaining claims are resolved; activation does not grant authority to collect additional customer funding.
- The customer sees estimated, executable, and withdrawable values separately. Full exit requests, and partial requests if supported, remain visible until completion or an explicit actionable block.

## Milestone 6 — performance and designer compensation

Implement the selected accounting and fee policy from the capital specification. Validate the proposed ETH high-water/loss-recovery convention before choosing rates or enabling fees. Persist fee basis, Strategy allocation epochs, accrued obligations, cash availability, and actual distributions separately.

Acceptance:

- Gross call premiums and borrowed ETH purchases cannot generate fictitious feeable performance.
- Open liabilities, financing, trade fees, and unsettled operations are handled by the named valuation policy.
- Deposits, partial withdrawals, losses/recovery, full exit/re-entry, and Strategy changes follow the published hurdle and attribution convention.
- Old and new designers cannot both charge the same performance period. Changing a release does not silently reset losses or existing fee obligations.
- Customer statements reproduce the calculations and distinguish recorded gains, estimated NAV, and paid fees.

## Milestone 7 — customer and designer pilots

Expose the customer and designer resources from the operating specification through one application API. Give the dashboard read-only projections. Run isolated pilot mandates using the reference release and a distinctly configured second release before allowing external designers' algorithms to operate customer capital.

Advance only with evidence of correct authority, live full/partial fills, cancellation races, restart recovery, spot conversion, interest/option settlement, exits, and account isolation. The current [V3 audit](../derive-v3-audit-2026-09-11.md) identifies uncompleted live lifecycle checks. Verify mainnet deployment, manager economics, scope behavior, and settlement independently of testnet success.

Measure customer value using net ETH accumulation, protection cost and stress response, coverage over time, available exit liquidity, execution quality, and support/recovery burden. Compare Strategy releases on the same available data and realistic execution assumptions. A good backtest is not live evidence.

## Milestone 8 — Juicebox funding and earned-revenue routing

Implement a small allocation/return adapter and optional operator/designer funding projects. Reference JB's split-hook interface, project authority, payout/allowance semantics, and `addToBalanceOf`; do not reproduce terminal accounting in OOF.

Acceptance:

- Funding reaches the intended account and returned assets credit the intended project exactly once.
- A Derive outage does not block ordinary JB pay/cash-out flows.
- Only earned, available fee assets enter business-revenue splits; customer collateral is independently accounted for.
- Cross-chain funding is a separate lifecycle, not an assumption that Derive positions or shares are bridged by Suckers.

## Implementation handoff

The first offline batch implements the contract/evaluator, reference configuration, alternative fixture and source-based replay foundation. The second adds durable mandate decisions, quantity/cash accounting and simulated operation recovery. Continue with host-built as-of snapshots and isolated Strategy execution, then wire condition/dependency evaluation to resource reservations and fresh venue risk checks before implementing the production signing adapter. Preserve remaining reference extraction and live lifecycle acceptance work. Strategy-specific thresholds are exercised through generation, admission, scheduling preview and reporting while production startup remains independent.
