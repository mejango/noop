# DRV spot accumulation implementation

The runtime supports DRV purchases on Derive behind a default-off gate. Deployment does not enable financial actions. The [strategy design](../dip-buy-design-2026-10-16.md) describes allocation and source protection.

## Plan refinement

- **Objective:** Allocate all net eligible future settled protective-put sale proceeds to DRV, while preserving operating cash. Provide a separate, explicitly enabled startup sample with a lifetime allowance of 5 USDC from the source balance.
- **System fit:** Canonical source fills fund one ledger; settled internal transfers fund ALT cash; guarded IOC orders and account-specific reconciliation record settled purchases. Existing option accounting retains its own history and campaigns.
- **Reuse and simplicity:** Reuse shared signing, fee/price previews, the existing execution journal and complete recovery collection. Sample mode uses the same executor and an immutable operator-funding record, without fabricated sales or a separate sender.
- **Evidence and unknowns:** V3 instrument/account previews establish venue compatibility at observation time. Actual sends still require fresh ownership, permissions, fee, cash and margin evidence. A transfer without a saved operation UUID remains reserved for authoritative recovery.
- **Verification:** Exercise allocation conservation, distinct operator funding, source guards, terminal/partial/unsettled outcomes, restart behavior, definitive versus ambiguous refusals, and unchanged option execution. Run the full repository suite and inspect deployment identity and feature settings separately.
- **Resource budget:** Keep the existing bot cadence and dependencies. Use bounded account reads and scoped diagnostic copies, retain private operational evidence outside the public repository, and deploy only the reviewed feature scope.

## Funding and execution

Every owned `sell_put` fill from either `roll_protection` or `monetize_tail_win` qualifies after settlement, net of its actual sale fees. Call premiums, expiry settlement, unfilled orders and unrelated balances do not create sale credits. The persisted allocation policy assigns 100% to DRV and rejects incompatible historical policy state.

The source remains the configured PRIME options subaccount. DRV uses an explicitly configured, same-owner ALT destination because permissions do not change a subaccount's risk universe. The session key must allow both accounts, spot orderbook trading, account information and transfers to existing subaccounts. The executor never creates accounts or withdraws funds.

Transfers retain four current put-budget cycles plus outstanding source cash commitments. Existing-account transfers use a zero fee cap. A matching operation UUID and settled receipts in both accounts are required before transferred cash becomes available for a DRV order. Pre-existing destination cash is excluded from accumulation funding.

Buy `DRV-USDC` using IOC limit orders, venue quantity/tick increments and a ceiling 50 bps above a fresh ask. Size includes the signed fee cap. Exact private quotes and positive post-trade margins must pass. Price, depth, cash or evidence failures defer; they do not authorize chasing a price or borrowing USDC. DRV holdings do not become insured ETH.

Final preview and authentication precede an atomic reservation/send claim immediately before HTTP. The shared order journal and source-transfer guard prevent overlapping commitments. Missing history alone cannot reject an ALT request. Explicit, matching definitive order refusals may release a pristine zero-spend reservation; unknown responses and unsettled fills remain reserved.

## Runtime settings

| Setting | Behavior |
|---|---|
| `SPOT_ACCUMULATION_MODE` | `off` by default; `dry_run` computes proposals without new allocation writes or executable signatures; `live` permits guarded future-sale-funded sends; `sample` permits the explicit one-time balance-funded test. |
| `SPOT_ACCUMULATION_DESTINATION_ID` | Required distinct same-owner ALT subaccount ID. Source identity comes from the existing V3 Derive configuration. |
| `SPOT_ACCUMULATION_FROM` | Explicit ISO eligibility boundary within V3 history and no later than startup. Sample mode can choose its first startup time when no boundary is stored. Persisted identity/boundary cannot silently change on restart. |
| `SPOT_ACCUMULATION_MAX_BATCH_USDC` | Positive explicit cap required in ordinary live mode. Sample mode pins this to 5 and rejects any different override. The separate sample allowance also caps lifetime sample funding. |

Derive maintenance and global `DRY_RUN=1` block financial sends. `off` continues recovery of existing durable attempts while disabling new attempts. The bot reconciles before source trading and advances accumulation only after its source-options cycle succeeds. Ordinary sale credits require complete settled history; the operator-funded sample does not invent source trade history.

## User-enabled startup sample

Deploy and verify the reviewed revision with mode `off`. The operator can then explicitly enable the financial sample in the deployment settings:

```text
SPOT_ACCUMULATION_MODE=sample
SPOT_ACCUMULATION_DESTINATION_ID=<your_same_owner_ALT_subaccount_id>
```

Apply the settings and restart the service. Source cash must still cover its four-cycle float and real commitments; exact fee, price and margin previews must pass. Transfer settlement may require later bot ticks before an order can be placed.

The first eligible sample tick creates one immutable `spot_operator_funding` allowance for 5 USDC. First-time sample funding requires no ordinary sale credits or prior accumulation execution history. `total_net_usdc` continues to represent sale proceeds only; `operator_funding_usdc` and `total_funding_usdc` expose the separate allowance. Restart cannot grant another allowance, and setup balances do not count toward it.

The first positive terminal, fully settled DRV purchase completes the sample, including a partial fill. Further ticks and restarts return `sample_complete` without new sends. Definitive zero-fill refusals may retry within the original allowance. Ambiguous or unsettled outcomes retain their reservations. Any unspent test funding remains allocated with its operator provenance.

Set `SPOT_ACCUMULATION_MODE=off` to stop new attempts. Keep the ledger, reservations and execution journal for recovery. Never delete state, reset the boundary, infer transfer success from balances alone or blindly resend a transfer whose acknowledgement was lost. Sample mode does not enable recurring purchases. A later ordinary live configuration needs its explicit batch cap and preserves previous identity, funding and spending.

## Code ownership and diagnostics

- `bot/spot-accumulation.js` owns exact allocation, spot sizing and transfer proposals.
- `bot/spot-accumulation-store.js` owns source credits, operator funding, disjoint balances, transfers, reservations and settled spending.
- `bot/spot-accumulation-runtime.js` owns sequential advancement and recovery; `bot/spot-accumulation-config.js` owns configuration and canonical source readers.
- `bot/spot-accumulation-venue.js` owns strict account/market evidence and guarded venue actions, using `bot/derive-signing.js`, `bot/derive-transfer.js` and `bot/derive-errors.js`.
- `bot/order-accounting.js` retains the shared submission journal and retires obsolete confirmed intents only when no execution evidence exists. DRV does not enter option campaigns or put-spend counters.
- `scripts/derive-spot-preflight.js` performs authenticated read-only diagnostics and optional unsigned previews. `scripts/spot-accumulation.js` performs offline initial-rollout projection. Neither tool submits financial actions.

Private operational diagnostics are retained outside the public release. Do not publish account histories, balances, advisor records, request authentication or credentials in test fixtures or evidence reports.

## Validation

The isolated release passed the complete `npm test` command under Node 20.19.1: 512 trading-system checks, 749 Node regression tests, 118 accumulation/signing/transfer/preflight tests, 46 backtest checks and the dashboard standalone checks. Independent review covered sample lifetime conservation, restart completion, source priority, durable pre-send claims and refusal recovery. Synthetic test data remains separate from production observations.

Read-only venue trials verified account compatibility, transfer encoding and unsigned DRV quote/margin behavior. These observations do not prove an actual transfer, fill or deployment. Verify the deployed revision, packaged diagnostics, default-off settings and a healthy bot cycle before operator activation; inspect actual settled receipts after the operator runs a sample.

Official references: [risk universes](https://docs.derive.xyz/trading/managers-and-risk-universes), [session keys](https://docs.derive.xyz/authentication/session-keys), [internal transfers](https://docs.derive.xyz/trading/transfers-withdrawals), [private order preview](https://docs.derive.xyz/api-reference/orderbook/privateorder_quote), [margin simulation](https://docs.derive.xyz/api-reference/subaccounts/privateget_margin), [transfer history](https://docs.derive.xyz/api-reference/history/privateget_erc20_transfer_history).
