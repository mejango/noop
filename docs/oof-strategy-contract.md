# OOF Strategy contract

Status: product contract, 2026-09-11. An [initial offline implementation](../strategy/README.md) now supplies strict schemas, condition evaluation, control-state replay, and a bounded V2 reference adapter. Runtime isolation, durable account execution, and customer-capital handling remain future implementation; this specification does not enable V3 trading. Read alongside the [operating specification](oof-operating-spec.md).

## Responsibility boundary

OOF is the shared V3 platform. Noop remains the existing operation and the name of one Strategy offering; the `noop-v2-reference` package preserves that Strategy's identity. Other designers can publish separate Strategies on OOF within the supported mandate and action scope. This naming decision was recorded 14 September 2026; the economic contract and implementation status remain unchanged.

A Strategy is a versioned program that turns market evidence and account state into bounded executable conditions for an ETH protection and accumulation mandate. Designers may use arbitrary algorithms, models, and private data inside an isolated research runtime. OOF owns authorization, order submission, accounting, and recovery.

The 25-DTE put roll, 80% call-profit capture, and 45% margin utilization are **Strategy parameters**, not OOF-wide constants. A different Strategy can use different values or different decision models. Premium spending rates, option selection, retained protection, call sizing, crash monetization, ETH purchase staging, and planned liability reduction also belong to the selected Strategy and its customer-accepted configuration.

OOF universally enforces the accepted configuration, instrument and account identity, valid authorization, fresh required evidence, sufficient funding, venue constraints, correct accounting, and controlled execution. It does not silently substitute its preferred economic rule for a valid Strategy output. An invalid proposal is rejected with a reason; it is not repaired into an economically different trade.

The contract supports these logical components:

1. Immutable Strategy release and configurable parameter declarations.
2. Customer mandate that accepts a release, parameters, account scope, and authority.
3. Point-in-time input bundle plus private Strategy state.
4. Decision containing standing intents and explicit edits to earlier intents.
5. Deterministic validation, condition evaluation, reservations, and execution.
6. Execution and accounting events delivered back to the Strategy.

## Identity and versioning

The initial interface identifier remains `noop.strategy/v1` for v1 compatibility with existing releases and recorded decisions. It is a retained wire-contract identifier, not the OOF platform name; the naming decision does not create a new contract version. Its definition must be published as machine-readable schemas before implementation is considered complete. Unknown schema versions, fields outside declared extension objects, enum values, and units are rejected.

| Identifier | Meaning |
| --- | --- |
| `strategy_id` | Stable identity of a designer's Strategy family. |
| `strategy_release_id` | Immutable release containing code, dependencies, schema versions, parameter declarations, and declared capabilities. |
| `release_digest` | Cryptographic digest of the release artifact and manifest. |
| `mandate_id` | Customer allocation and its accounting domain. |
| `mandate_revision` | Immutable revision of customer-accepted release, parameters, limits, and authority. |
| `input_bundle_id` | Content-addressed evidence and account-state bundle supplied to a run. |
| `decision_id` | Unique decision identifier, immutable once accepted. |
| `intent_id` / `intent_revision` | Stable standing instruction and its immutable revision. |
| `execution_id` | OOF-owned lifecycle for fulfilling an intent, possibly across explicitly bounded partial fills and order attempts. |
| `order_attempt_id` | One durably recorded venue submission attempt. |

Canonical serialization, digest algorithm, decimal encoding, and timestamp precision are pinned by the schema version. Reusing an identifier with different content is a conflict. Replaying identical content returns the existing result without creating another execution.

The customer account is identified by network, chain, venue deployment, owner, subaccount, manager, and risk universe where applicable. A Strategy cannot change these through an intent. Credentials are never part of a Strategy input or output.

## Release manifest and accepted parameters

A release declares:

- Runtime artifact digest, dependency lock, entrypoint, resource limits, and supported contract versions.
- Required and optional datasets, minimum history, freshness constraints, and acceptable data-quality states.
- Scheduled and event-driven invocation triggers, with rate limits.
- Private-state schema and state migration capabilities.
- Typed parameter schemas, units, permitted ranges, defaults, and plain-language descriptions.
- Supported actions, instruments, venues, and required adapter capabilities.
- Its protection, premium-spending, call-risk, ETH-recycling, and failure-handling policies.
- Evaluation methodology and benchmark versions. Performance claims remain evidence, not execution authority.

The selected parameter values are materialized in the mandate; runtime behavior cannot depend on a changing release default. Defaults are suggestions until accepted by the customer. Strategy parameters may be made more restrictive by a customer's accepted configuration; the release must declare which combinations it supports. Incompatible combinations fail activation.

Every economic parameter requires an unambiguous definition. In particular:

| Parameter family | Definition required from the release and mandate |
| --- | --- |
| Time-based put budget | Spending currency, accrual rate, time basis, protected-exposure valuation, revaluation timing, carry-forward, proceeds recycling, and treatment of fees and reserved orders. |
| Call margin ceiling | Exact numerator and denominator, current versus stressed measure, treatment of working orders, and behavior after market movement causes a breach. `45%` alone is insufficient. |
| Call sizing | Eligible ETH inventory, quantity or stressed-liability constraints, and whether externally held exposure influences sizing. External exposure is never counted as venue collateral. |
| Put sale | Eligible lots, retained coverage, replacement dependencies if any, expiry treatment, and crash monetization policy. A 25-DTE trigger is optional. |
| Call repurchase | Capture calculation including costs, optional target such as 80%, and separately specified calculated risk-reduction conditions. |
| Spot ETH purchases | Spendable proceeds, liability and liquidity reserves, price conditions, tranche limits, and the certified route used to acquire and custody ETH. |
| Failure response | When to pause, cancel working orders, request a new decision, or perform a previously authorized risk-reduction action. No undefined "emergency override." |

OOF verifies any declared economic limit against its own authoritative account and ledger data. A designer-reported score, margin estimate, or available balance cannot substitute for that verification. Platform and venue capacity limits may be stricter than a mandate, but must be named and visible; the platform cannot treat a looser Strategy parameter as permission to violate them.

## Strategy input

Each invocation receives a read-only, tenant-scoped bundle:

| Input | Required content |
| --- | --- |
| Invocation context | Contract version, release and mandate revisions, invocation reason, sequence, and evaluation clock. |
| Market snapshot | Instrument definitions and versioned metadata; executable quotes and depth where available; spot and collateral prices; source timestamps, availability timestamps, sequence/checkpoint, freshness, and quality flags. |
| Shared history | Dataset versions and content-addressed ranges, with reproducible point-in-time query semantics. |
| Account snapshot | Venue positions and liabilities, balances, net value, margin results, open orders, pending settlement, and reconciliation watermark. |
| OOF ledger | Accounted fills, fees, financing, cash flows, premium accrual and usage, outstanding reservations, inventory lots, and unresolved execution state. |
| Mandate | Protected ETH amount and its definition, accepted parameters, authority, release identity, lifecycle state, and pending withdrawal or configuration changes. |
| Active intents | Accepted revisions, reservation usage, execution status, dependencies, and cancellation or replacement in progress. |
| Private state | Strategy-owned state version and scoped persistent-data references. |
| External research | Declared private data or model outputs, with provenance and availability times when used to justify executable conditions. |

Missing data is distinct from zero. A stale price is distinct from a current price. Instrument delisting, feed gaps, contradictory snapshots, and an unavailable margin computation are represented explicitly. The Strategy may produce a no-action decision with an explanation; it cannot manufacture defaults that satisfy required execution checks.

Shared history records both `observed_at` and `available_at`. A replay at time T only reads records available by T, including the revision of a record known then. Later corrections, final candle values, and reconstructed order books do not leak into earlier decisions. Research derived from a rolling window pins the dataset, window endpoints, feature implementation, and resulting digest.

Live condition evaluation uses a fresh evaluation bundle. A decision's historical inputs explain the proposal but do not authorize execution indefinitely. An old favorable quote cannot satisfy a current price condition.

## Arbitrary computation and private state

The research runtime can execute designer code, query permitted datasets, call approved research services, and maintain its own state. It receives no trading or withdrawal key, unrestricted host filesystem, shared tenant storage, or direct access to OOF's signing service. Outbound access is mediated and recorded according to declared capabilities. Runtime, memory, storage, output size, and invocation frequency are bounded independently of economic parameters.

Research may be probabilistic. Reproducibility comes from recording the release, inputs, state versions, model identifiers, relevant responses, and randomness where available; it does not require pretending an external model always returns the same result. Execution interpretation must be deterministic even when the research is not.

An invocation returns a proposed private-state update using compare-and-swap against its input state version. OOF commits decision acceptance, active-intent edits, and that state update atomically in its control ledger. A rejected or replayed invocation cannot advance state twice. A private external datastore cannot participate in that transaction; integrations must use an idempotent outbox or supply immutable referenced results rather than claim atomic external writes.

Strategies receive subsequent execution events with stable IDs and may process them repeatedly without double-counting. State retention, export, and visibility are defined in the designer agreement. Isolation does not imply that a runtime operator can never inspect secrets; no such confidentiality guarantee is assumed here.

## Decision and standing intents

A decision declares its input bundle and expected control-state revision. Concurrent or stale writers fail compare-and-swap and must read the new state before proposing again. All of a decision's control operations validate and commit together or none do. This control transaction does not make subsequent venue orders atomic. Decision acceptance means the instructions were recorded; it does not mean an order was sent or filled.

The decision contains explicit operations:

- `upsert_intent`: introduce a new intent or replace its revision using an expected prior revision.
- `cancel_intent`: stop further fulfillment of an existing intent and reconcile any working venue orders.
- `no_action`: record an explanation without changing existing intents.

Omitting an intent from a later decision does **not** cancel it. Expiry and cancellation are explicit. A separate bounded `replace_set` operation may be introduced later, but is not implicitly inferred in v1.

Every actionable intent specifies:

- Its identity, revision, action, purpose, and customer-accepted authority references.
- A concrete instrument or route identifier, or an explicit bounded candidate list with deterministic selection and tie-break rules.
- Maximum lifetime quantity and spend or liability bounds, denomination, and source budget or inventory lot references.
- Order side, exact limit bounds, permitted time-in-force, reduce-only requirement, and fee/slippage ceiling.
- Activation and expiry timestamps, quote-age bound, evaluation cadence, and maximum attempts and cumulative fills.
- A typed condition tree, dependencies, priority, and any shared conflict group.
- Reservation policy and required account reconciliation state.
- A reason and optional evidence metadata, neither of which changes executable semantics.

The v1 action vocabulary is:

| Action | Economic intent and essential constraint |
| --- | --- |
| `buy_put` | Acquire downside protection using an accepted premium budget. |
| `sell_put` | Reduce identified long-put inventory; purpose may be renewal, crash monetization, or another declared reason. No accidental short-put creation. |
| `sell_call` | Create call liabilities within the accepted call policy, quantity limits, and verified available capacity. |
| `buyback_call` | Reduce identified short calls with economic close-only execution and an accepted capture or risk-reduction reason. |
| `buy_spot_eth` | Acquire deliverable ETH with reconciled spendable funds through an enabled route. A perpetual position does not satisfy this action. |
| `sell_spot_eth` | Convert identified ETH inventory to fund liabilities or another accepted mandate purpose through an enabled route. |

Cancellation is a control operation, not a new economic strategy action. Deposits, withdrawals, fee transfers, and changes to the protected amount are customer or platform lifecycle operations; a Strategy cannot add arbitrary transfer destinations. A venue capability unavailable at deployment causes activation or the affected action to fail explicitly. In particular, spot conversion must not be assumed to exist merely because the options API is available.

Close-only semantics and the venue's native `reduce_only` flag are distinct. Prefer native enforcement where supported. Emulating a close-only resting order is a separate adapter capability that must establish equivalent quantity protection across partial fills, cancellation races, and every possible account writer. Local inventory reservations or polling alone do not establish that guarantee. If equivalence cannot be established, reject that execution mode and expose the difference from V2; do not silently permit a position reversal.

## Deterministic condition language

Conditions are a bounded JSON expression tree, not JavaScript, free-form language, or a reference to code that runs in the signer. Version 1 supports:

- Boolean `all`, `any`, and `not`.
- Typed comparisons `lt`, `lte`, `eq`, `gte`, and `gt`.
- `ref` operands from a registered field catalog, and typed `literal` operands.
- A finite set of registered deterministic derived fields. Unbounded iteration, remote calls, mutation, and arbitrary expression evaluation are prohibited.

The field catalog pins units, precision, source, freshness, missing-value behavior, and calculation version. For example, call capture must identify which premium and fill lots supply its basis and whether fees are included; `profit_pct` without a defined basis is invalid. DTE uses the pinned evaluation clock and venue expiry timestamp.

All monetary quantities are decimal strings with explicit currency or contract units. Comparisons use checked fixed-point or exact decimal arithmetic; no implicit ETH/USD conversion or binary-float tolerance. Each conversion identifies an approved valuation source and snapshot. Rounding toward a venue tick or amount step may only preserve or tighten the accepted price and quantity bounds; otherwise the action is rejected.

Missing or stale required operands evaluate to `unknown`, not `false` or zero. Boolean operators use three-valued logic: `not unknown` remains `unknown`; `all` with a false operand is false; `any` with a true operand is true; otherwise unresolved operands preserve unknown. Only a fully true condition plus all universal checks permits an order. Required account, valuation, and funding checks cannot be bypassed by wrapping them in a permissive `any` branch.

A designer can use its own score to rank candidates or declare a typed, timestamped custom feature. Such a feature needs pinned provenance and validity before it can participate in conditions. Optional evidence scores alone never authorize trading or relax an account, funding, or accepted-risk limit.

### Illustrative decision

This is valid JSON illustrating the contract shape, not an executable order or a complete machine-readable schema. The account, instrument, timestamps, numbers, and digest references are placeholders. No value below is an OOF-wide policy.

```json
{
  "contract_version": "noop.strategy/v1",
  "strategy_release_id": "example-protection/1.0.0",
  "mandate_id": "example-mandate",
  "mandate_revision": 4,
  "input_bundle_id": "sha256:EXAMPLE_INPUT_DIGEST",
  "decision_id": "example-decision-104",
  "expected_control_revision": 103,
  "private_state": {
    "expected_version": 18,
    "proposed_version": 19,
    "content_ref": "state:example-mandate/19"
  },
  "operations": [
    {
      "op": "upsert_intent",
      "expected_intent_revision": null,
      "intent": {
        "intent_id": "renewal-put-104",
        "intent_revision": 1,
        "action": "buy_put",
        "purpose": "protection_renewal",
        "authority_ref": "mandate:example-mandate/4",
        "instrument_ref": "instrument:EXAMPLE_APPROVED_PUT",
        "active_from": "2030-01-01T12:00:00Z",
        "expires_at": "2030-01-01T12:05:00Z",
        "evaluation_interval_ms": 1000,
        "max_quote_age_ms": 2000,
        "max_attempts": 1,
        "priority": 10,
        "conflict_group": "protection-renewal",
        "budget_ref": "premium-budget:example-mandate/current",
        "reservation_policy": "reserve_on_activation",
        "quantity": { "max_total": "0.1", "unit": "contract" },
        "order": {
          "side": "buy",
          "time_in_force": "ioc",
          "reduce_only": false,
          "limit_price": { "value": "100", "unit": "USDC/contract" },
          "max_total_outlay": { "value": "10.10", "unit": "USDC" },
          "max_total_fees": { "value": "0.10", "unit": "USDC" }
        },
        "requires_reconciled_account": true,
        "dependencies": [],
        "when": {
          "op": "lte",
          "left": { "ref": "instrument.best_ask" },
          "right": { "literal": "100", "unit": "USDC/contract" }
        },
        "reason": "Renew protection within the accepted premium allocation.",
        "evidence": { "research_ref": "research:example-decision-104" }
      }
    }
  ]
}
```

The example uses a normalized premium per contract. The adapter derives that unit from instrument metadata and the venue's native quantity, price, and multiplier conventions; it must not apply a multiplier twice. The executor calculates worst-case outlay and fees and rejects this intent if its stated caps cannot cover the order. The example does not assume that one contract is one ETH.

## Dependencies, reservations, and order lifecycle

Intents form a bounded directed acyclic graph. An intent may depend on an identified predecessor's `filled_and_accounted` amount, `cancelled_and_reconciled` state, or a named ledger event such as settlement becoming withdrawable. An order acknowledgement is never sufficient proof of a fill, spendable proceeds, replacement protection, or released margin.

For a renewal policy that requires replacement protection, the replacement purchase reaches the declared accounted fill threshold before the old put is sold. For ETH recycling, the put-sale proceeds must be accounted and eligible for spending, and OOF must reserve required liabilities and existing commitments before submitting the ETH purchase. A venue-supported atomic package requires its own tested adapter; a graph of ordinary orders provides no atomicity guarantee.

Reservation behavior is explicit. `reserve_on_activation` reserves a bounded budget or inventory allocation when an intent becomes active. `reserve_on_trigger` competes for remaining capacity when its conditions turn true, using declared priority and a stable tie-break. All simultaneous accepted orders and unresolved submissions count against capacity. Mutually exclusive candidates share one bounded conflict-group allocation; they cannot each spend the whole budget in parallel.

The control lifecycle is:

`proposed → accepted → waiting → reserved → submitting → acknowledged → partially_filled / filled → reconciled → completed`

Additional explicit states include `rejected`, `expired`, `cancel_requested`, `cancelled`, and `submission_unknown`. Expiring an intent stops new submissions and initiates cancellation of working orders; it does not declare those orders gone. Partial fills remain owned and accounted after expiry or cancellation.

Before signing, the account worker verifies the active mandate revision, checks that it holds the current writer lease and fencing token, loads current account and order state, re-evaluates conditions and worst-case limits, and commits the reservation and order attempt durably. An external deposit, withdrawal, fill, settlement, or position change invalidates any calculation that depended on the old state.

The order-attempt record is durable before transmission. An ambiguous transmission enters `submission_unknown`; the worker reconciles the existing attempt against venue state and history before releasing reservations or allowing conflicting orders. It does not assume failure and generate a new economic submission. Venue nonces are not a replacement for application idempotency.

Acknowledgements, cumulative fills and costs, fees, funding, settlements, and local inventory updates are independent events. Event IDs and cumulative quantities make replay idempotent. Only durable local accounting advances the corresponding reconciliation watermark. Observed and accounted exposure remain distinguishable during recovery, and both constrain new orders.

Replacement uses compare-and-swap on intent revision. If an old revision has a working order, OOF first cancels it, obtains terminal venue evidence, accounts intervening fills, and recalculates the unfilled authorized quantity before the successor submits. A failed cancel or late fill cannot cause both revisions to spend the same allocation. Orders belonging to an older mandate revision remain tracked until closed and accounted; a configuration change cannot erase liabilities.

## Strategy changes and customer exits

Publishing a release does not upgrade customer accounts. A mandate revision records explicit customer acceptance, new parameters, state migration, and the disposition of existing intents and positions. The initial product should require acceptance per release change. A future delegated upgrade policy would need a separate, explicit customer grant.

A safe switch pauses new entries, reconciles outstanding operations, handles working intents according to the accepted switch plan, and transfers accounting context to the new release. The customer may choose to retain existing positions if the incoming Strategy supports them. Unsupported positions require an explicit unwind or continued management plan; they are not hidden from the new Strategy.

An exit request takes priority over new Strategy entries. OOF revokes standing entry authority, cancels working orders, and runs the customer-authorized unwind lifecycle. Private Strategy code cannot veto an exit, set a new destination, or turn the exit into a new risk position. A full unwind uses the separately accepted exit policy, without ordinary Strategy premium budgets, retained-protection or replacement requirements, roll timing, call-profit thresholds, or entry targets blocking closure. Identity, authorization, bounded execution prices, available resources, accounting, and venue admissibility still apply. Execution limits and treatment of illiquid positions belong to the disclosed exit policy. Initiating an exit at any time does not imply an instantaneous guaranteed-value redemption.

## Validation and acceptance cases

These are required behavioral cases for a later implementation, not claims that tests exist today.

| Case | Expected result |
| --- | --- |
| Two accepted Strategies choose different put-roll DTE, call capture, or margin limits. | Both validate if each is supported by the venue and its own accepted configuration. No hidden V2 economic threshold overrides either. |
| A release changes its default margin or spending rate. | Existing mandates retain their materialized values and release; accepting the new configuration creates a new revision. |
| A strong opportunity score accompanies an over-budget order. | Reject the order; retain the score only as research evidence. |
| Conditions are true but the required quote is stale or the account is unreconciled. | Wait or reject with a specific reason; do not sign. |
| A corrected candle was published after a historical decision. | Replay uses the earlier available version, not the correction. |
| The same accepted decision or fill event arrives twice. | Return the prior acceptance or ignore the already accounted event; no duplicate trade, state update, or budget charge. |
| The same decision ID arrives with different content. | Reject the conflict. |
| A conditional put buy and another entry compete for the same funds. | Atomically reserve within the shared available amount; cumulative spend cannot exceed it. |
| A put sale is acknowledged but unfilled. | No dependent ETH purchase and no replacement dependency marked satisfied. |
| A put sale partially fills and is cancelled. | Account actual cumulative proceeds and costs once; only the eligible accounted amount can fund the dependent purchase. |
| A cancel races with a fill during intent replacement. | Account the late fill and reduce replacement quantity before submitting. |
| An order response is lost after transmission. | Retain the unresolved attempt and reservations; reconcile rather than blindly resubmit. |
| A signer sees a stale worker fencing token or mandate revision. | Reject signing even if the worker's market conditions are satisfied. |
| A Strategy proposes spot ETH but the deployment has no validated spot adapter. | Reject that capability at activation or the action before execution; never substitute leveraged ETH exposure. |
| A Strategy crashes or returns malformed JSON. | Preserve ledger and accepted intent semantics, apply their stated expiry, and invoke the accepted failure policy. |
| A customer exits while the Strategy emits a new entry. | The exit lifecycle wins the control-state comparison; reject the stale entry. |
| A full customer unwind would breach the Strategy's normal retained-protection or profit-capture rule. | Apply the accepted customer-exit policy; ordinary holding rules cannot veto closure. Keep authorization, price, accounting, resource, and venue checks. |
| A proposed resting close lacks native close-only enforcement and equivalent adapter guarantees. | Reject that execution mode; never infer that a local position snapshot prevents a future reversal. |

## Mapping the current V2 implementation

The current Noop bot is the source of the first Noop Strategy release on OOF, represented initially by `noop-v2-reference`. Noop is one Strategy among many possible OOF offerings; its implementation does not define the permanent platform contract.

| Existing Noop component | Proposed home |
| --- | --- |
| Advisors, research context, opportunity scores, and knowledge files | First Strategy runtime, shared datasets, and scoped private state. |
| Standing rules and opportunity watchers | Typed intents and deterministic condition evaluation. |
| Put DTE ranges, 25-DTE renewal, 80% capture, call-margin targets and overrides | Explicit first-release parameters and economic policy; defaults documented as that release's choices. |
| Premium cycle and carry-forward accounting | Ledger implements the accepted Strategy budget definition. |
| Rule normalizers that change economic meaning | Replace with release-specific proposal validation; executor only performs semantics-preserving venue formatting. |
| Pending actions, resting orders, partial fills, and durable V3 recovery | Account worker, reservations, order-attempt journal, and event ledger. |
| External insured ETH setting | Explicit protected-exposure input, separate from collateral and owned inventory. |
| Manual conversion of excess proceeds to ETH | New bounded `buy_spot_eth` workflow with a validated custody and execution route. |

The existing [rule-contract plan](plans/2026-06-02-advisor-rule-normalization-removal-plan.md) already argues for typed opportunity contracts and rejecting economic rewrites. Its concrete strategy thresholds become part of the initial Strategy release. The [V3 audit](derive-v3-audit-2026-09-11.md) supplies recovery lessons and tested migration infrastructure, but does not by itself establish tenant isolation, arbitrary-code isolation, distributed writer fencing, or this new Strategy interface.

Implementation starts with schemas, a deterministic evaluator, an offline V2 adapter, and historical replay. It then progresses through shadow decisions and isolated testnet accounts before customer capital or a published designer marketplace. No current production setting changes as a side effect of adopting this specification.
