# OOF capital and performance specification

Status: proposed product specification, 2026-09-11. This document defines the intended ledger and customer lifecycle; it does not change live V2 execution or implement custody, withdrawals, or fee collection. Read alongside the [operating specification](oof-operating-spec.md) and [Strategy contract](oof-strategy-contract.md).

OOF is the shared V3 platform for customer mandates and multiple Strategy offerings. Noop retains its name as the existing operation and one Strategy on OOF. The naming decision, recorded 14 September 2026, leaves this specification's ETH scope, 10% activation funding, account model, and proposed fee policy unchanged.

The user-defined activation requirement is 10% of the ETH exposure the customer designates for protection. Put-roll timing, call-profit capture, margin limits, premium budgets, and borrowing policies belong to the selected Strategy and accepted mandate. The fee mechanism below is a proposal; no fee rate has been selected.

## Capital, exposure, and customer claims

This specification interprets the customer's designated 20 ETH of exposure as including the 2 ETH activation deposit, leaving 18 ETH external. The 2 ETH remains customer capital: it can become collateral, cash, options, receivables, or ETH acquired through the program, and can incur losses and expenses.

The following quantities must be distinct in storage, APIs, and reporting:

| Quantity | Meaning |
| --- | --- |
| `protected_exposure_eth` | Customer-designated reference exposure, with a dated source and mandate revision; initially 20 ETH in the example. |
| `external_exposure_eth` | ETH exposure outside OOF's account, initially 18 ETH; observed or declared, with freshness recorded. |
| `activation_deposit_eth` | Initial customer contribution, 2 ETH in the example. This is a capital transfer, not a service fee. |
| `spot_eth_balance` | Actual ETH owned inside the operating account. It need not equal the initial deposit or economic equity. |
| `equity_eth` | Operating-account economic value after liabilities and costs, expressed in ETH. |
| `withdrawable_eth_estimate` | Estimated ETH deliverable after permitted unwinds, settlement, conversion, costs, and fees; accompanied by assumptions and quote age. |
| `protection_scenarios` | Estimated portfolio outcomes under specified ETH prices, volatility, dates, and liquidity assumptions. |

External ETH is an exposure input. It is neither a receivable owned by OOF nor available trading collateral. Derive manager selection determines which posted assets are supported and how much margin credit they receive; the adapter must discover and validate those settings for the deployed account. [Derive managers and risk universes](https://docs.derive.xyz/trading/managers-and-risk-universes).

The 10% rule applies at activation. It does not establish a promised drawdown floor, a permanent leverage ratio, automatic top-up obligations, or ongoing access to the external ETH. Changes to protected exposure, minimum viable account size, admission above exactly 10%, reactivation funding, and behavior after operating losses require explicit product decisions. Until specified, a Strategy cannot infer additional customer funding authority from the activation rule.

The reference exposure should remain a versioned mandate input. ETH purchases, ETH sales, withdrawals, and changes to an external wallet do not silently change it. Whether accumulated ETH automatically increases future budget and protection targets must be declared by the Strategy and accepted by the customer.

## Account lifecycle

Proposed initial account model: one Derive subaccount per customer mandate, operating one immutable `strategy_release_id` and digest at a time. A release change produces a new `mandate_revision`; economic positions, commitments, fee hurdles, and unsettled operations persist across that change.

| State | Entry evidence and permitted work |
| --- | --- |
| `draft` | Exposure and Strategy terms can be previewed; no operating authority. |
| `funding_pending` | Customer authorized funding; record deposit identifiers and intended account. Pending funds are shown separately and cannot fund orders. |
| `activation_ready` | Correct account credited, deposit and asset identity reconciled, initial 10% contribution verified, accepted mandate present, permissions and supported operations validated. |
| `active` | Strategy can propose actions; OOF validates each action against current account state, accepted limits, outstanding commitments, and exit status. |
| `entry_paused` | Customer pause, unavailable data, depleted authorized capacity, or an unresolved operation blocks new exposure. Continue observation and specifically permitted reconciliation or risk management. A pause does not itself promise liquidation. |
| `exit_requested` | Persist an idempotent customer request and its scope. Stop new entry proposals immediately and begin cancellation/reconciliation. |
| `unwinding` | Cancel entries, account for all fills including cancellation races, close or resize exposure, resolve debt and conversion needs. Only actions belonging to the exit plan or required account-risk management are permitted. |
| `withdrawal_ready` | All outgoing assets and liabilities for the requested amount are reconciled; the remaining book, if any, satisfies the accepted residual mandate. Present the current net amount and authorized destination. |
| `withdrawal_pending` | Signed withdrawal submitted; preserve operation UUID, batch state, recipient, quantity, and on-chain evidence. An acknowledgement is not completion. |
| `closed` | Customer proceeds paid and reconciled; positions, balances, commitments, and liabilities resolved by transfers, settlement, or an explicitly authorized disposition. Retain history, loss carry-forward, and reactivation linkage. |
| `resolution_required` | Liquidation, negative equity, bad debt, or unsupported/residual assets prevent the ordinary exit path. Stop new entries, record venue events and remaining claims, and expose the specific resolution needed. Do not erase debt or assign away customer assets to force closure. |
| `recovery_required` | Missing or conflicting evidence prevents a safe transition. Expose the actual unresolved state; do not fabricate a flat balance or retry an uncertain financial action as a new action. |

These are economic lifecycle states, not a substitute for the independent order, transfer, and reconciliation state machines. Persist transitions and reasons in the same account event log. Derive exposes operation settlement separately through `public/get_transaction`; batch status may be absent before batching and has explicit error states. [Derive operation settlement](https://docs.derive.xyz/api-reference/system/publicget_transaction).

Customers can initiate an exit at any time. The resulting amount and time to payment depend on fills, prices, liabilities, available liquidity, and settlement. A full exit cancels Strategy entry authority immediately but keeps narrowly scoped unwind authority until completion; the customer's ownership and recovery path must not depend on a Strategy designer being online. Derive's signed withdrawals specify the recipient and have separate scope and recipient-allowlist requirements. [Derive transfers and withdrawals](https://docs.derive.xyz/trading/transfers-withdrawals).

Full unwind follows an accepted customer-exit policy, independently of ordinary Strategy premium budgets, retained coverage, replacement dependencies, roll timing, call-profit capture, and entry targets. It still requires authorized quantities and recipients, bounded execution prices, available resources, correct accounting, and venue admissibility. It cannot be blocked solely because the selected Strategy would prefer to hold or renew protection.

The 10% activation rule grants no authority to collect additional funds or reach external holdings. Whether customer liability is limited to contributed capital must be established from the selected account structure, venue rules, and customer terms before onboarding; this specification does not assert limited liability. Liquidation and its costs are economic events in the same ledger. Negative equity, unresolved debt, and dust or unsupported residuals remain visible with a resolution path. Any write-off, residual transfer, or claim disposition requires explicit applicable authority and durable evidence; a zero withdrawable amount does not prove that liabilities are discharged.

Partial exits are a proposed extension. They require a new accepted residual mandate if the existing scope cannot be supported. Never mechanically scale all options by a withdrawal percentage and assume margin or protection remains adequate. A failed partial-exit feasibility check can offer a smaller amount or full unwind; it must not silently change the Strategy's economic limits.

## Budget and proceeds ledger

A premium budget is an authorization to spend, not a balance. Track it independently from cash, ETH, debt, and economic PnL. For each Strategy budget window, store the budget denomination, exposure basis, basis timestamp, accrual schedule, carry-forward rule, authorized capacity, spent amount, and outstanding reservations.

```text
available_authorized_premium = accrued_authorization
                             + permitted_carry_forward
                             - accounted_premium_spend
                             - unfilled_order_reservations
```

A fill consumes the corresponding reservation and creates actual spend once. A confirmed cancellation releases only the unfilled reservation. Expired authorizations, negative available capacity, and fill corrections remain visible; clamping a report to zero must not erase an overrun. A price conversion used for budget enforcement is captured with the event, not reconstructed using today's ETH price.

Selling a put changes cash and position inventory. It replenishes spending authorization only if the accepted Strategy explicitly says so. Time-based budgets, cycle length, carry-forward, renewal requirements, crash-sale tranches, and reinvestment schedules are Strategy terms. The existing 25-DTE roll, 80% call-profit capture, and 45% margin target are possible release parameters, not universal OOF limits.

Available proceeds for ETH purchases must exclude committed or needed liquidity. Evaluate the proposed post-purchase account against outstanding orders, borrowing and settlement liabilities, premium commitments, withdrawal requests, and the accepted liquidity policy. Buying ETH is a spot-conversion action with its own quote, slippage bounds, execution evidence, and recovery state; an ETH perpetual is not a substitute for acquired spot ETH.

The same cash-allocation engine handles ETH sales intended to fund short-call obligations. Record the option obligation and the spot sale as distinct linked events. Strategy authorization to fund obligations from deposited ETH gives no authority to sell the customer's external holdings. Account risk must remain supportable before expiry; a plan to sell ETH later is not evidence that current margin is adequate.

Borrowing authority, ceilings, repayment priority, and debt-versus-ETH-purchase allocation are declared and validated terms. If a release omits borrowing permission, the executor must not infer permission from available venue credit. The exact spot/conversion venue and ETH deposit/withdrawal route remain adapter implementation decisions to validate on the deployed V3 environment.

## Economic accounting

Use decimal or fixed-point arithmetic with asset-specific precision. Record source quantities before conversions, the pricing source and time, operation identity, account identity, Strategy release, mandate revision, and causally linked intents. Each external economic event must produce one balanced ledger posting, including after replay and restarts.

Compute the performance-fee basis in ETH:

```text
V = value_of_owned_assets_and_receivables
    + signed_value_of_open_derivative_positions
    - borrowing_principal_and_accrued_financing
    - unpaid_trading_conversion_settlement_and_operating_costs
    - already_crystallized_but_unpaid_performance_fees
```

All terms use one valuation timestamp and are converted into ETH. Positive and negative balances must be represented once: if borrowing principal is already represented as a negative cash balance, do not subtract it again. Likewise, do not add option unrealized PnL to a valuation already including option market value. Option settlement replaces the expiring position with its settlement cash or receivable; it must not count both.

`V` is before the current uncrystallized performance-fee accrual and after every other economic cost. Customer net NAV equals `V` minus that current fee accrual. Paying an already booked fee or liability reduces both cash and the payable and is not a second expense.

Short-call proceeds increase cash while creating a marked liability. They are not immediately earned profit. Purchasing a put exchanges cash for an asset; paying premium is not, by itself, an equal economic loss at inception. Realized financing must reconcile to history, with an accrual for financing since the latest settlement where required; settled and accrued amounts must not overlap. Derive publishes paid and received interest events through its interest-history endpoint. [Derive interest history](https://docs.derive.xyz/api-reference/history/privateget_interest_history).

Valuation has three separate outputs:

1. **Economic NAV:** a documented, independently checkable method for positions and receivables, with all liabilities included.
2. **Executable exit estimate:** depth-aware closing and conversion quotes, fees, slippage assumptions, and freshness for the requested size.
3. **Venue margin:** the manager's account-risk calculation, used for admissibility and monitoring.

These numbers need not match. Do not treat margin equity or a displayed midpoint as a guaranteed payout. A Strategy's own predicted fair value cannot be the sole valuation used to pay that designer. Stale or unpriceable positions block fee crystallization and mark the performance estimate incomplete; they do not acquire a fabricated zero liability. Valuation method, acceptable quote ages, and uncertainty treatment must be selected before customer capital is admitted.

## Proposed ETH high-water performance fee

This proposal uses **absolute ETH loss carry-forward per continuing customer mandate**, rather than a per-unit hurdle that discards part of the prior loss when capital is withdrawn. The choice is deliberate: deposits and withdrawals cannot erase the mandate's unrecovered ETH loss. The remaining capital must recover the same absolute deficit, which can make fee recovery slower after a partial withdrawal. This commercial tradeoff still requires acceptance.

Let:

- `V` be the reconciled ETH fee basis above.
- `H` be the ETH fee hurdle, initially the accepted contribution.
- `f` be the disclosed aggregate performance-fee rate, with `0 <= f < 1`; its value and the split among designer and OOF are undecided.
- `A = f * max(V - H, 0)` be the reversible current fee accrual.
- `N = V - A` be the customer net operating NAV.

Apply events in one serialized account ledger:

| Event | Exact treatment |
| --- | --- |
| Initial funded activation `D` | `V = D; H = D`. Separately post actual onboarding costs to equity. |
| Further contribution `D` | `V := V + D; H := H + D`. A pending deposit has no effect until credited and reconciled. This preserves `V - H`. |
| Mark-to-market or operating result | Update `V`; leave `H` unchanged. Recalculate the reversible `A`. |
| Fee crystallization | With a complete admissible valuation, calculate `A`. If `A > 0`, book that payable once, set `V := V - A`, and set `H := V`. If there is no gain, leave `H` unchanged. |
| Withdrawal `W` | First complete the required valuation/crystallization checkpoint. Then `V := V - W; H := H - W`. `W` is net customer property transferred out of operating equity, not a fee or expense. |
| Payment of booked performance fee | Cash and the corresponding fee payable both decrease. `V` and `H` do not change. |
| Full exit below hurdle | Transfer the remaining net equity; preserve the positive residual `H` as unrecovered ETH loss. Closing or reopening an account does not delete it. |

The proposed withdrawal checkpoint crystallizes the whole remaining mandate, including a partial withdrawal, before the capital leaves. It avoids arbitrary assignment of accrued profit to the withdrawn portion. Actual exit costs must be booked before the checkpoint; a quote is not sufficient evidence of a completed unwind. Periodic checkpoints, fee liquidity requirements, and whether unrealized gains can crystallize while other positions remain open are product decisions still to select. If those conditions are not satisfied, the withdrawal remains pending or proceeds through a complete unwind; the system cannot silently waive or invent a fee.

Accrual is reversible; crystallization is an explicit ledger event. A temporary profitable mark that subsequently disappears reverses its uncrystallized accrual. The proposal does not provide a clawback of legitimately crystallized fees after later market losses; the unchanged hurdle ensures the losses must be recovered before further fees. Corrections of erroneous valuations require correcting ledger entries and a fee-recovery policy before launch.

Version changes preserve `H`. Full exit and later reactivation of the same mandate preserve its residual loss carry-forward. Account replacement, mandate splitting, and switching designers must not reset that economic lineage implicitly. How future fees are attributed across multiple designers is undecided; initial fee-bearing mandates should use one designer until that attribution is specified. Fee rates and recipients cannot be changed retroactively for outstanding mandates.

### Worked examples

The following uses a **hypothetical 20% fee solely to demonstrate arithmetic**, with all values in ETH. It is not a selected commercial rate. Valuations are assumed complete and any exit costs already included in `V`.

**Gain and fee payment.** Activate with `V = H = 2`. The account reaches `V = 2.4`; gain over hurdle is `0.4`, so `A = 0.08` and customer net NAV is `2.32`. Crystallization books the `0.08` payable and leaves `V = H = 2.32`. Paying that booked payable creates no second expense. A full exit now returns `2.32`; both `V` and `H` become zero.

**Loss and recovery.** Following that checkpoint, equity falls from `2.32` to `1.92`; `H` remains `2.32`. Recovery to `2.32` earns no new fee. At `V = 2.52`, only `0.20` is above the hurdle: fee `0.04`, post-fee `V = H = 2.48`.

**Top-up during a loss.** Start from an independent mandate with `V = 1.6, H = 2`, a `0.4` unrecovered loss. A further `1 ETH` contribution produces `V = 2.6, H = 3`, preserving the `0.4` deficit. Recovery to `3` has no fee. At `3.2`, the fee is `0.04`, leaving `V = H = 3.16`. The added principal itself never becomes a fee-bearing gain.

**Partial withdrawal after a gain.** From `V = 2.4, H = 2`, crystallize `0.08`, leaving `V = H = 2.32`. Withdraw `0.58`: `V = H = 1.74`. The customer has `0.58` withdrawn and `1.74` remaining, totaling `2.32` after fees. If the remaining equity later reaches `1.94`, the new fee is `0.04`.

**Partial withdrawal during a loss.** From `V = 1.6, H = 2`, withdraw `0.4` with no fee: `V = 1.2, H = 1.6`. The absolute `0.4` historical deficit survives. A `0.8` top-up gives `V = 2, H = 2.4`; no fee is due until value exceeds `2.4`. This differs deliberately from reducing the old hurdle pro rata to `1.5`, which would forgive part of the loss.

**Full withdrawal during a loss and reactivation.** From `V = 1.6, H = 2`, withdraw all `1.6`: `V = 0, H = 0.4`. If the same mandate later contributes `2`, its ledger starts at `V = 2, H = 2.4`, subject separately to the as-yet-unselected reactivation funding policy. The historical loss has not become fee-bearing recovery because an account was closed.

## Reporting and designer evaluation

Report capital-flow-adjusted ETH gain as customer net operating NAV plus cumulative customer withdrawals minus cumulative customer contributions. An outgoing amount becomes a withdrawal in that equation when the ledger removes it from operating equity and records an unconditional customer claim; display that claim separately as pending until delivery is confirmed. Include it exactly once. Failed withdrawals restore the operating balance or remain an outstanding claim; they cannot disappear between categories.

This ETH gain is different from ETH inventory growth. Buying ETH with borrowed cash can increase `spot_eth_balance` without increasing equity. Show inventory, cash, debt, option assets and liabilities, costs, and resulting net equity alongside one another. Display USD results separately so ETH-denominated improvement cannot conceal a dollar drawdown during a crash.

Absolute gain is not a fair rate comparison across differently sized or differently timed accounts. A marketplace also needs net-of-fee time-weighted returns with external flows segmented at their actual timestamps, exposure-relative protection outcomes, stressed coverage, drawdowns, uncovered intervals, liquidation events, capacity, and operational reliability. Mark estimates and incomplete data explicitly. Do not annualize very short histories as established performance.

For protected-portfolio reporting, external holdings are tracked separately and added only under stated balance and valuation assumptions. Do not award a Strategy fees on the customer's external ETH price movement or unrelated external trading. A stronger “protection effectiveness” benchmark can be evaluated alongside fee performance without becoming a hidden fee hurdle.

## Existing code and implementation requirements

The current V2 put-budget machinery is in [`script.js`](../script.js), including `getPutBudgetPortfolioValue`, `maybeResetPutCycle`, and `PUT_INSURED_EXTERNAL_ETH`. These provide migration behavior to characterize, not a customer fee ledger. The current budget basis includes Derive USDC plus held and declared external ETH marked at spot; preserve that explicitly in the legacy Strategy and do not silently reinterpret it as the product's declared ETH exposure.

[`bot/db.js`](../bot/db.js) `getRealizedPnL` currently aggregates option sale cash flows minus option buy cash flows. That can be useful as an options cash-flow metric, but it is unsuitable for designer compensation: it does not by itself value open short liabilities, financing, all external capital flows, or full settlement economics. Customer fees need the independent ledger specified here.

The audited [V3 execution recovery](derive-v3-audit-2026-09-11.md) and [migration runbook](derive-v3-migration.md) supply isolation and order-accounting foundations. They do not yet implement this customer lifecycle, spot recycling, complete financing/settlement reconciliation, or performance fees.

Before customer activation, require invariant tests for balanced postings; exactly-once replay; credited deposit identity; deposits and withdrawals preserving `V - H`; losses never crystallizing a fee; accrued-fee reversals; liability-payment neutrality; full-exit carry-forward; partial-fill/cancel races; accrued-versus-settled interest; expired-option replacement; unavailable valuations blocking fees; and crash/restart recovery during funding, unwind, fee booking, and withdrawal. Portfolio simulations must include short-call obligations during a rally and put monetization during a crash, with finite liquidity and outstanding debt.

## Decisions still required

- Minimum viable deposit, accepted funding assets, treatment of contributions above the 10% activation requirement, ongoing underfunding responses, exposure changes, and reactivation rules.
- The valuation method used for fees, reference ETH price, quote freshness, treatment of illiquid positions, checkpoint schedule, and whether open-position gains can crystallize before full unwind.
- Performance-fee rate, OOF/designer split, acceptance of absolute rather than proportional loss carry-forward, lineage across designers, fee liquidity, and correction recovery.
- Which partial-exit behavior is initially supported, residual-mandate acceptance, exit execution price controls, conversion route, and payment finality evidence.
- How strategy-owned ETH accumulation changes future protection targets, if at all; this must be an accepted Strategy policy rather than inferred from spot balance.

Juicebox revenue distribution can consume booked and actually transferable service revenue after these rules are satisfied. Customer principal, uncrystallized fee estimates, and assets reserved for liabilities are not designer revenue available for a Juicebox split.
