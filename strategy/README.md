# OOF offline Strategy implementation

This is the first implementation batch from the [OOF productization plan](../docs/plans/oof-v3-productization.md). OOF is the V3 platform for multiple Strategies; Noop is the existing ETH protection and accumulation setup and one Strategy on OOF. This shared layer validates versioned Strategy contracts and previews conditional decisions without loading Noop's bot, database, credentials, SDK, or network clients.

The initial wire identifier remains `noop.strategy/v1` for compatibility with pinned releases and accepted mandates. It names the existing contract version, not the OOF platform or the only Strategy it can support. Noop-specific policy, feature names and release IDs remain in `strategies/noop-v2-reference/`.

## Run

```sh
npm run test:strategy
npm run strategy:replay
```

The replay command compares the reference policy and an illustrative alternative on independent synthetic scenarios:

| Observation | V2 reference | Alternative fixture |
| --- | --- | --- |
| Put at 30 DTE, replacement protection already owned | Wait for 25 DTE | Eligible under 35-DTE policy |
| Short call with 75% gross premium captured | Wait for 80% | Eligible under 70% policy |
| Candidate projects 35% V2 displayed-margin utilization | Within 45% entry target | Blocked by 30% entry target |

Here, eligible means `ready_for_risk_checks`. The preview has no fund reservations, inventory locks, verified venue simulation, or signing authority. These scenarios demonstrate configuration and control behavior; they are not historical fills or performance results.

To replay a supplied captured-rule input:

```sh
npm run strategy:replay -- --input /path/to/replay.json --strategy reference
```

The file contains `{ "mandate": ..., "input_bundle": ..., "control_state": ..., "evaluation_bundle": ... }`, with the last two properties optional. Omit `control_state` for the first decision. Supply `evaluation_bundle` for a separate producer snapshot after decision acceptance. The selected release must match the installed reference or example artifact. Output includes the decision, acceptance receipt, next in-memory control state, and schedule preview. Files must be valid UTF-8 JSON, at most 4 MiB. The command only reads the supplied file and writes its result to stdout.

Candidate margin evidence binds the exact order and account/control context. Admitting a decision changes that context. Without fresh evaluation evidence, a captured call-sale input therefore remains blocked in the preview; the CLI does not relabel its old projection as current. Only the explicitly synthetic demo constructs synthetic projection evidence for its new state. Neither form is a certified venue risk calculation.

For an example input shape, use `createReplayFixture({action: 'sell_call'})` from [the fixture module](../strategies/noop-v2-reference/fixtures.js). Fixtures use invented account identities and pinned timestamps, never a funded account.

## Modules and contract boundaries

| Module | Implemented behavior |
| --- | --- |
| [contract.js](contract.js) and [schemas](schemas/README.md) | Machine-readable release, mandate, input, and decision schemas; strict structural and semantic validation; SHA-256 identities; exact bounded quantities, prices, and references. |
| [decimal.js](decimal.js) | BigInt decimal parsing, comparison, addition, subtraction, and multiplication. No floating-point conversion for financial bounds. |
| [conditions.js](conditions.js) and [fields.js](fields.js) | Bounded typed condition trees, versioned field definitions, units, precision, freshness, and three-valued evaluation. |
| [replay.js](replay.js) | Immutable in-memory acceptance, decision replay/conflict detection, mandate-content pinning, control/private-state CAS, retained revisions, explicit cancellation, and scheduling previews. |
| [V2 reference package](../strategies/noop-v2-reference/README.md) | Captured-rule adapter, extracted pure policy helpers, fallback generation, policy prompts/reports, and alternative parameters. |

The release contains parameter declarations; the mandate contains every accepted value. Changes to defaults do not change an existing mandate. Control state pins the accepted mandate content, including its parameters, authority, and account. Lifecycle status is a current control projection and is excluded from that content digest; authenticated lifecycle transitions belong to the later customer control plane.

Global input fields contain account-scoped evidence. Instrument-dependent quotes, DTE, position values, and candidate projections belong under `instrument_fields[instrument_ref]` (or a declared spot route). The scheduler never borrows another instrument's observations. Input hashes include the complete bundle, and observation/availability timestamps exclude later information from an earlier input.

Both admission and scheduling use an explicit Strategy economic validator. Its context includes `{mandate, release, phase, now}`; `phase` is `admission` or `schedule`. Admission accepts bounded standing rules before their conditions become true. Scheduling uses the current supplied clock. The control layer snapshots and freezes JSON inputs before invoking policy code, so a callback cannot alter the already validated decision or account data.

`no_action` and omission preserve existing intents. An explicit cancellation retains the instruction as `cancel_requested`; a replacement retains prior revisions as `replacement_pending_reconciliation`. The preview cannot resolve either state, satisfy an execution dependency, or assume an order is gone. Those transitions require the future durable ledger. Expiry likewise stops further scheduling without asserting that working venue orders have been cancelled.

## Deliberate scope

The current parameter schema supports named bounded decimal parameters, concrete ETH option instruments, and declared ETH spot routes. Parameter booleans/enums, broader assets, candidate-set selection, a history-query service, designer runtime isolation, live risk admission, customer funding/withdrawals, and fees remain later work. Runtime limits in a release are declarations; this batch does not launch arbitrary designer programs or enforce an operating-system sandbox.

The V2 adapter covers a bounded captured-rule boundary and source-tested pure helpers. Full advisory/research orchestration and live synthetic resting exits have not been migrated. Unsupported legacy economic fields and execution modes must fail explicitly instead of being dropped or converted silently. The package's coverage notes distinguish extracted helpers from those remaining paths.

V2 research scores intentionally preserve the source's `Number`/`Math.pow` calculations under their versioned definitions. Executable quantities, premium limits, inventory comparisons, and call-price ceilings use decimal arithmetic. V2 gross premium capture and displayed-margin estimates have their own field definitions; they are not relabeled as net economic profit or authoritative V3 margin simulation.

The [OOF product foundation](../product/README.md) now supplies the durable mandate/account ledger and simulated recovery. Next come runtime isolation and a scheduler that can reserve resources and dispatch through the V3 adapter. This offline package is not connected to the running Noop V2 operation.
