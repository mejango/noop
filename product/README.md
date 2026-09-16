# OOF product foundation

**OOF** is the new V3 platform for multiple Strategies. **Noop** remains the name of the existing ETH protection and accumulation setup, and is one Strategy that can run on OOF alongside others. OOF owns shared data, mandate authorization, execution, accounting and recovery; each Strategy owns its accepted economic policy.

This package persists customer mandates, accepted Strategy decisions, economic receipts, and uncertain order attempts. It is the second offline implementation batch in the [OOF V3 product plan](../docs/plans/oof-v3-productization.md). It imports no live V2 entry point and uses no V2 database, wallet, SDK, or execution configuration.

The initial architecture still assigns one active Strategy release to each separately funded mandate/subaccount. Supporting many Strategies on OOF does not combine their trading authority or collateral. Noop's reference implementation stays in `strategies/noop-v2-reference/`; this package is named `@oof/product-foundation` and remains in `product/`.

The gateway implements **only `offline_simulation`**. A missing mode or any other mode is rejected. Its callbacks are trusted local fixtures; the mode flag is not a network sandbox. Do not attach a real signer or venue transport: condition scheduling, fresh account evidence, inventory/margin checks, venue nonces, and production signing authority remain separate implementation work.

## Run

From the repository root, with its existing dependencies installed:

```sh
npm --prefix product test
npm --prefix product run demo
```

The demo uses the frozen reference Strategy fixture and a new private temporary SQLite directory. It simulates a lost order response, restarts, rejects a blind retry, imports two partial fills, repeats terminal evidence, and settles the option. It removes its own temporary directory afterward. No testnet account is contacted; `offline-fixture` is a synthetic deployment name. The fixture's active mandate does not implement customer onboarding or the 10% activation rule.

Starting with synthetic 2 ETH and 10 USDC, the demo books 1 USDC of put premium and 0.01 USDC of fees exactly once. A later synthetic 0.5 USDC settlement leaves 2 ETH, 9.49 USDC, and no option position. These are journal balances, not a marked NAV or a Strategy performance claim.

## Modules and authority

| Module | Responsibility |
| --- | --- |
| `ledger/events.js` | Strict normalized receipts, exact decimal postings, event identity and constrained reversals. |
| `ledger/store.js` | Private SQLite storage, mandate/account identity, atomic decision/private-state acceptance, receipts and balance verification. |
| `ledger/operations.js` | Scoped reservations, durable attempts, simulated signing/transport boundary, writer leases and atomic recovery. |
| `demo.js` | Executable synthetic end-to-end lifecycle with restart and replay. |

`require('./product')` exports `openLedger`, `normalizeEvent`, `createOperations`, and `createGateway`. Importing it does not load the native database binding or open files other than source modules. The SQLite dependency is the repository's existing `better-sqlite3`; this package does not change production startup or root scripts.

`openLedger({ filename, ledgerId, clock? })` requires an explicit normalized absolute filename ending in `.sqlite` or `.db`, inside an existing private directory. Use a canonical real path, directory permissions `0700`, and database/sidecar permissions `0600`. That directory may contain only this database and its named SQLite sidecars. Symlinks, hardlinks, unrelated files, foreign or unmarked databases, mismatched ledger IDs, and unsupported schemas are rejected. The application marker and ledger ID prevent accidental adoption of another database; they are not an authorization system.

The host registers `{ customer_id, release, mandate, catalog }`, then constructs a scope using `store.scope({ customer_id, mandate_id })`. The scope offers `getMandate`, `getControlState`, `acceptDecision`, `appendEvent`, `listEvents`, `getBalances`, and `verify`. Returned data cannot retarget a scope. Customer authentication must happen before the host creates it; a customer-supplied ID is not authentication. Strategy algorithms must receive snapshots and return decisions, not receive ledger mutation capabilities.

`store.privileged()` exposes the shared transaction, database and verification capabilities for trusted platform composition. Never expose the store or this capability to designers, customers, or arbitrary Strategy code. This batch provides application scoping, not OS process isolation or an arbitrary-code sandbox. It also stores private-state version/content references, not a private-state blob service.

Account exclusivity uses network, chain, venue, deployment, and subaccount ID within one ledger. Owner, manager, and risk universe remain pinned authority metadata; changing them cannot allocate the same account twice. Derive's public margin endpoint identifies an account using only `subaccount_id`, supporting this owner-independent identity choice. A trusted deployment registry must canonicalize deployment aliases before production registration; multiple separate ledger files are not globally coordinated. See the [official subaccount API](https://docs.derive.xyz/api-reference/subaccounts/publicmargin_watch).

## Journal behavior

Receipts have a strict envelope:

```js
{
  event_id: 'local-event-1',
  source_event_id: 'venue-source-1',
  kind: 'deposit',
  occurred_at: '2030-01-01T00:00:00.000Z',
  evidence_ref: 'retained-evidence-1',
  payload: { transfer_id: 'transfer-1', asset: 'ETH', amount: '2' }
}
```

The scope supplies the account. The importer must authenticate the source, retain its evidence and map source IDs consistently. An `evidence_ref` string alone proves nothing. Local IDs, source IDs, and business identities are checked within the mandate. An exact repeat changes no balance; a changed local alias can resolve to the same receipt, while conflicting source/business content is rejected. Aliases cannot later identify a different event.

Supported receipts cover deposits, withdrawals, individual option/spot fills, borrowing, principal repayment, interest accrual/payment, additional fees, full signed-position settlement, and exact reversal. Cash assets are ETH and USDC; option quantities use explicit ETH option identifiers. Every event balances **per asset** with exact decimal strings. Unlike units are never added into an alleged total value.

| Economic fact | Treatment |
| --- | --- |
| Customer deposit | Cash and contributed capital; no revenue. |
| Borrowing | Cash and an equal principal liability; no investment gain. |
| Short option fill | Cash proceeds and a negative option quantity; premium is not declared profit. |
| Accrued financing | Expense and liability, followed by repayment of that liability without expensing it again. |
| Put sale and spot purchase | Separate confirmed fills; no inferred conversion or double use of proceeds. |
| Option settlement | Independent evidence removes the exact remaining signed position and records its signed cash payment, including zero-payoff expiry. |

Fill amounts are incremental executions and exclude their separately listed fees. Standalone fee events must describe additional charges, not fees already included in a fill. The importer owns that canonical mapping. Authoritative negative cash is recorded as a fact; it does not fabricate a loan. The simulated dispatch gateway then blocks further submissions until cash is reconciled. Repayment cannot clear more debt than has been recorded, and settlement cannot remove a different position quantity; missing evidence must be imported first.

Corrections negate the original stored receipt; callers cannot submit arbitrary postings. A reversal that would invalidate later events touching the same account/asset is rejected. Complex corrections, including coordinated repair of execution projections, require a later reviewed reconciliation workflow. There is no generic force-clear or delete-receipt method.

SQLite uses WAL, `synchronous=FULL`, foreign keys and immediate transactions. Decision receipt, active intents and private-state compare-and-swap commit together. Economic receipt, postings and materialized balances commit together. Recovery additionally commits fill identity, cumulative accounting and reservation release in the same transaction. Hash-linked receipts and deterministic replay detect mismatched projections on reopening and before mutations; they are not externally anchored protection against a privileged attacker rewriting the entire database.

History verification currently replays complete mandate history, including deterministic control transitions. It does not rerun an external designer algorithm or authenticate historical market data. This deliberately favors correctness over throughput; checkpoints, indexed projections, backup/restore drills and operational schema migrations remain production work.

## Operation and recovery contract

The trusted host constructs `createOperations(store.privileged()).forScope(scope)` to queue an option operation against an already accepted, active intent revision. An operation pins action, instrument, quantity, limit, fees and an explicit USDC reservation. Queueing checks lifetime quantity, fee, outlay and attempt limits, along with available cash. The simulated payload also carries the accepted time-in-force, expiry and close-only flag. These fields still require validation and enforcement by a future venue adapter.

```js
const gateway = createGateway(store.privileged(), {
  mode: 'offline_simulation',
  sign: simulatedSign,
  send: simulatedSend,
  verifyEvidence: verifyRetainedFixture,
});
const lease = gateway.acquire(scope, { worker_id: 'worker-a', ttl_ms: 1000 });
await gateway.submit(lease, operationId);
// After an uncertain result: recover the same attempt; never submit it again.
await gateway.recover(lease, operationId, authenticatedFixture);
```

The host clock controls expiring leases and monotonic fence tokens. Submission and recovery pin caller authority before asynchronous callbacks. A durable `submission_unknown` claim is written before signing; errors during signing, sending, or response handling retain the claim and reservations. The gateway checks the current fence around asynchronous boundaries and rechecks accepted intent state and cash after signing. An ACK records acceptance only; it books no fill or fee.

The nonce is an account-local durable simulation counter, not a Derive signature nonce. Signed bytes are neither returned to a worker nor written into public operation state. Real credential fencing needs a separate signer/dispatch process and venue-enforced constraints: a SQLite lease and a final local check do not by themselves revoke an old process's credentials or prevent a delayed signed payload from arriving.

Recovery requires matching account, attempt, nonce, payload digest and venue order identity, plus an injected trusted evidence verifier. It imports each fill once. Partial fills consume only confirmed cash; open/unknown work keeps its remaining reservation. Terminal status releases it only when the complete cumulative quantity is accounted. Missing fill pages, contradictory receipts, unsupported fee assets, or execution outside accepted bounds keep the account unresolved for reconciliation; actual receipts must never be altered to make them pass bounds.

An unacknowledged zero-fill attempt may resolve as `not_accepted`/`rejected` only with verified irreversible nonacceptance: nonce invalidation, actual signature expiry, or definitive rejection of that payload. A current `not found` result is insufficient. Post-ACK rejection is conservatively unsupported in this batch. Terminal cancellation is supported through recovery evidence; sending a venue cancellation is not implemented. An unattempted queued operation can be cancelled locally. Any unresolved account attempt blocks new queued exposure and submissions, including an acknowledged open order.

## Validation and next boundary

The OOF naming change preserves the existing `noop.strategy/v1` wire identifier, pinned release artifacts, ledger application marker and database schema. These are compatibility identifiers, not the platform's public name. Renaming the platform must not invalidate accepted mandates or silently change persisted receipt identities; a future wire/schema change needs an explicit versioned migration.

Tests cover exact accounting and identity conflicts, customer/account separation, stale decision/private-state commits, read-only refusal of foreign databases, corrupted projections, a real SIGKILL before SQLite commit, process lease contention, asynchronous lease mutation/expiry, lost responses, partial history, recovery rollback, duplicate evidence, and later settlement. Import tests deny live V2, SDK, wallet, network and native database access.

This completes the durable offline core of Milestone 2, not all of its production acceptance criteria. Next implement host-built as-of market/account snapshots and Strategy runtime isolation, then connect condition/dependency evaluation to atomic premium-budget, cash, inventory and margin reservations. Only after those gates and the real signing/evidence adapter pass isolated V3 lifecycle tests should this path gain live dispatch. Funding activation, customer exit, marked ETH performance, designer fees and Juicebox routing remain later milestones. The 25-DTE roll, 80% call capture and 45% margin target remain properties of a Strategy release throughout.
