# Derive V3 implementation audit — 11 September 2026

The audit found material defects in the initial integration. The fixes preserve V2 as the default and keep all V3 execution isolated. The code supports continued testnet rehearsal; it is not approved for a production handoff yet.

## Findings resolved

| Priority | Finding | Fix and regression evidence |
| --- | --- | --- |
| High | Every numeric RPC error was treated as a definitive rejection, allowing another order after a confirmation timeout. | Only documented pre-book rejections clear an intent. Codes 9000/9001, internal errors and duplicate-nonce responses retain the block, including when replaying old journals. SDK transport regressions cover these outcomes. |
| High | Venue ACK or nonce discovery cleared recovery protection before Noop saved fills and budgets. | ACK stays unresolved until explicit accounting completion. CLI discovery clears only confirmed terminal zero-fill orders. Database failure and restart tests exercise the guard. |
| High | Partial GTC fills could leave their remaining orders untracked; resting fills and budgets were not committed atomically. | Initial and subsequent fills commit local accounting in SQLite transactions. Cumulative filled quantity and cost prevent duplicate budget charges. Native SQLite tests cover partial fills, changing average prices and rollback. |
| High | A cancellation request could close local tracking before the venue confirmed its final fills. | V3 keeps the order tracked until a terminal observation and defers replacement orders until reconciliation. Tests cover cancellation failure, uncertain state and fills racing cancellation. |
| High | Malformed responses and caller catches could turn unavailable exposure or margin into an empty account. | Validate V3 response identities, arrays and numeric fields; propagate failures before retry sizing and confirmation. Failed reads no longer create zero-balance portfolio snapshots. |
| Medium | SDK metadata/signing failures were journaled as submitted orders despite never sending an order. | Build and sign before the durable intent, then send exactly once. Tests use the actual SDK with an instrument lookup failure. |
| Medium | V3 top-level Greeks were dropped; zero Greeks became null. | Normalize the documented fields and preserve zero. Source-based regressions cover the mapping. |
| High | Cutover checked only regular open orders and accepted incomplete snapshots. | Require regular, trigger and algorithmic order lists, exact V2 provenance, valid balances/risk metadata and a schema-2 comparison report. |
| Medium | Dashboard routes could bypass isolation; writer checks had a lock/read race. | Enforce account-specific paths at every dashboard state entry point; reject symlinked state; acquire the writer lock before preflight and journal reads. |
| High | The ordinary bot entry opened its database before rejecting an accidental V3 launch. | Validate the runner and account-specific state paths before importing `bot/db.js`; an actual-entry regression proves no database is loaded on rejection. |

Implementation evidence is in `test/derive-v3*.test.js`. The API review used the pinned SDK, the saved official V3 OpenAPI/error catalog, and Derive's [position schema](https://docs.derive.xyz/reference/private-get_positions), [error codes](https://docs.derive.xyz/error-codes), and [migration changes](https://docs.derive.xyz/migrating/breaking-changes).

## Verification and limits

The final audit passed 482 existing trading checks, 8 model/API tests, 56 V3 regression tests, and the isolated dashboard production build. Put and call signing hashes again matched `private/order_debug`, with zero orders submitted. Both archived live smoke receipts also pass the tightened receipt validator. Read-only checks confirmed testnet subaccount 78645 remained flat with zero open orders and recent trades. The two earlier cancelled smoke orders were reconciled as terminal zero-fill orders; the recovery journal has no unresolved intents. No orders, deposits, production restarts or deployments were performed during this audit.

Filled orders discovered after an uncertain send still require explicit accounting recovery; the CLI intentionally does not guess or import them automatically. An ACK followed by a crash before its accounting-completion marker also stops restart for review, even if SQLite already committed. Live fills/partial fills, sustained strategy execution, settlement delays, final mainnet deployments and margin equivalence remain release conditions. The process manager must keep V2 stopped during the eventual cutover; the local V3 lock does not coordinate separate machines.
