# Noop V2 reference Strategy for OOF: offline extraction

Noop remains the existing ETH protection and accumulation setup. This package makes its reference policy one Strategy on **OOF**, the new V3 platform for multiple Strategies. Shared authorization, data, execution and accounting belong to OOF; Noop's economic decisions belong to this Strategy and its accepted configuration. The `noop-v2-reference` identity, `noop_v2.*` features and frozen source provenance retain their Noop names.

This package establishes a replay boundary around **captured V2 research rules**, selected pure V2 helpers, materialized customer parameters, and finite execution bindings. It does not run the production bot, call a model, sign, submit orders, open a database, read account credentials, or fetch market data. The running V2 implementation is unchanged.

`createReferenceStrategy()` preserves the reference defaults. `createExampleStrategy()` creates the separate illustrative `example-35-70-30` release. Its roll window is 35 DTE, call capture is 70%, and normal call entry target is 30%. All other parameters remain explicitly materialized, including the 65% breakout target and 5 percentage-point execution buffer. These example values are test inputs.

## Public interface

```js
const { createReferenceStrategy, createExampleStrategy } = require('./strategies/noop-v2-reference');
const { createReplayFixture } = require('./strategies/noop-v2-reference/fixtures');

const strategy = createExampleStrategy();
const { mandate, inputBundle } = createReplayFixture({ strategy, action: 'sell_put' });
const decision = strategy.generateDecision(inputBundle, { mandate });
const policy = strategy.reportPolicy({ mandate });
```

| API | Purpose |
| --- | --- |
| `strategy.release` / `strategy.fieldCatalog` | Frozen manifest and versioned field definitions. The artifact manifest pins package and transitive contract files. |
| `strategy.policyFor(mandate)` | Validate a complete set of accepted parameters and selected release identity. A missing parameter is an error; current release defaults do not repair it. |
| `strategy.legacyFor(mandate, researchState)` | Pure extracted helpers, with policy injected and explicit copied breakout research state. These preserve V2 numerical behavior for comparison. |
| `strategy.buildPrompts({ mandate })` | Selected-policy generation, confirmation, and reporting text. No model request is made. |
| `strategy.buildFallbackRules(context, { mandate })` | V2 required watcher discovery and canonical fallback output. This is raw research output, not an authorized order. The V2 canonical helper has no automatic buy-put fallback. |
| `strategy.calculatePremiumAuthorization(values, { mandate })` | Exact one-cycle budget from a pinned insured-base value, carried authorization, net put cost, and reservations. This is not cash availability or a clock/ledger implementation. |
| `strategy.adaptRecordedRules(rules, bindings, inputBundle, { mandate })` | Translate supported, explicit legacy rules into bounded intent operations. Every rule needs a finite host binding. Unsupported rules fail rather than silently losing criteria. |
| `strategy.generateDecision(inputBundle, { mandate })` | Validate the bundle, translate captured research, and validate the proposed decision and static economic policy. |
| `strategy.validateEconomicPolicy(intent, inputBundle, { mandate, phase, now })` | `{ valid, reasons }`. `phase: 'admission'` validates static economic semantics; `phase: 'schedule'` additionally checks current evidence, price policy, available authorization, and planned call target. `now` is the explicit scheduling clock, defaulting to the bundle clock. |
| `marginProjectionBinding(intent, inputBundle)` | Bind offline call-margin evidence to this exact instrument, quantity, price/order, account snapshot, fields, control revision, and outstanding intents. A different candidate or account context requires new projection evidence. |

Runtime limits in the manifest are declarations for a future isolated runtime. This in-process package does not enforce process isolation or make a claim about running arbitrary third-party code safely.

## Captured research boundary

`inputBundle.extensions.reference_v2` contains:

- `recorded_rules_json`: the original research-rule JSON as a string. This explicitly preserves the distinction between legacy binary-number research values and new exact financial bindings. Source payloads containing unsupported fields must be deliberately translated before replay.
- `bindings`: a map keyed by rule ID (or array index when the rule has no ID). Each entry supplies identity, authority/resource references, concrete instrument, finite quantity, exact price/fee/outlay or liability bounds, lifetime, order mode, cadence, attempts, dependencies, and reservation policy. The Strategy supplies action semantics and conditions. Example bindings are in `fixtures.js`.
- Optional `cancel_intents`: explicit intent IDs, expected revisions, and reasons. Omitting an existing rule does not cancel it.
- For call entries, `margin_projections`: a map keyed by intent ID containing the candidate and account-context digests produced by `marginProjectionBinding`. The projection value remains in that instrument's registered field evidence. The helper does not calculate a margin simulation or certify the producer.

The host chooses resolved instruments at this boundary. Full candidate-universe search and the large historical research/advisory pipeline remain upstream. No bounded intent is inferred from a legacy `null` instrument selector without a concrete binding. Raw legacy rules, including their old defaults and Number arithmetic, cannot enter a signer directly.

Admission accepts dormant standing watchers. At 30 DTE and 75% call capture, the default reference admits the corresponding watchers but leaves them waiting. The example release can make them ready for subsequent risk checks. Scheduling uses fresh per-instrument evidence plus independently checked account reconciliation. The offline scheduler does not reserve capital or authorize a trade.

## Coverage and deliberate differences

| Area | Implemented boundary and evidence | Remaining work or deliberate difference |
| --- | --- | --- |
| Policy and parameter propagation | Budget, selection ranges, score exponents, put roll/monetization, call profit targets, normal/breakout margin and buffers are immutable release/customer parameters. Generation, copied validation/normalization, fallback, pricing, prompts and reporting consume them. | Existing V2 startup, live thresholds and dashboard are unchanged. |
| Research scores and price selection | V2 put/call normalization, patient put pricing, call/put patient exit plans, tranche sizing and call-range selection are pure, source-compared functions. | Composite history/IV/skew/OI/crash-payoff analysis, ranking across an entire live chain, and the statistical/advisory pipeline are not extracted. Captured outputs remain required. |
| Research/rule generation | Required rulebook coverage, complete canonical fallback branches, source contract validation and capture normalization are extracted. | No LLM calls or stochastic output equivalence. `value_signal`, `market_conditions`, unknown rule keys and unsupported conditions need explicit adapters. Prompt fragments are preserved; the entire advisory prompt/context assembler is not extracted. |
| Confirmation | V2 discipline prompts, buyback confirmation context and pure patient-price/value helpers are source-compared. Exact contract and economic checks run separately. | Model confirmation, lesson retrieval, repricing/retries, orderbook routing and resting-order recovery are not extracted. |
| Margin | Pure V2 displayed-margin estimators, breakout detector and headroom helpers preserve the 45/65 targets and 5-point buffer. Candidate-bound offline planned-entry checks enforce the selected target. | The 5-point buffer is preserved in those source helpers/prompts, but is **not additional planned entry capacity**. Live last-mile estimate-drift checks, authoritative V3 manager simulation and sizing are not implemented here. A preview at 46% fails the 45% planned target even though the old final execution buffer was 50%. |
| Option profit metrics | Separate `noop_v2.*` fields preserve V2 gross average-premium capture/P&L. They do not reuse the default net-fee field definitions. | Positions, fees and contract multipliers require a trusted producer and later ledger. Gross research metrics are not designer-fee performance. |
| Put budget | Exact `insured_base * annual_rate * period_days / 365`, one supplied cycle, with legacy net-put-cost and carry semantics. Division floors to 30 decimal places. | This explicit rounding replaces binary arithmetic for authorization. No automatic stale-value budget reuse, clock advancement, settlement crediting, or durable budget ledger. A profitable put sale can increase V2 authorization but does not by itself certify spendable funds. |
| Exit execution | The bounded adapter supports native reduce-only IOC exits. Profit, retained inventory, roll replacement and exact limit checks remain selected Strategy economics. | Legacy `post_only` is not silently converted to `gtc`; synthetic resting exits, patient fair-value-only monetization and threat-management interpretation are rejected by this adapter pending separate execution/evidence support. Their pure legacy helpers still run for historical comparison. |
| Strict adaptation | Unknown root/criteria fields, conflicting economic aliases, ambiguous `any` exit logic, unsupported order modes and inconsistent bindings are rejected. | V2 sometimes ignored extra keys or used alias precedence. Source helpers retain those behaviors for honest comparison; the bounded adapter deliberately does not grant authority from that ambiguity. |
| Control lifecycle | Decisions make explicit upserts and cancellations; accepted release/mandate/parameter identities are pinned. | Durable reservations, fills, partial fills, debt/settlement, retries, exits/withdrawals and live account isolation are later implementation milestones. |

`provenance.json` identifies each copied declaration and its source hash. Only two V2 prompt literals change: the embedded `80%+` text now interpolates the selected capture parameter. `test/strategy-reference.test.js` independently evaluates bounded, explicitly allowlisted actual source declarations in a VM; it never executes the full production script. Other tests verify exact bounds, alternate policy, dormant admission, candidate binding, unsafe rule rejection, and import capabilities. No claim of full V2 decision equivalence is made beyond this coverage.

After reviewing a package or transitive contract change, run `node strategies/noop-v2-reference/update-artifact.cjs` to regenerate the checked-in source pins. A changed artifact changes the release digest; an existing mandate selecting the previous digest cannot silently adopt it. The explicit update command writes only this package's artifact manifest and is not imported by the Strategy.
