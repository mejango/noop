# Put proceeds into DRV on Derive

The strategy earmarks 100% of net eligible future settled protective-put sale proceeds for DRV. It includes both protection rolls and tail-win monetization. A default-off implementation moves eligible USDC internally from the configured PRIME options account to a same-owner ALT account and buys `DRV-USDC` there.

Keep four current put-budget cycles plus outstanding commitments in the source account. Transfer and purchase fees count inside the allocation. Existing destination balances are not sale credits. Use only available cash with verified ownership, permissions and positive post-action margins; there is no withdrawal, external swap, ETH purchase route or new borrowing.

Use IOC limit buys with venue quantity increments and a ceiling 50 bps above a fresh ask. Thin liquidity can cause partial fills or deferral. The source options strategy retains priority, and DRV does not become insured ETH or silently change its protection formula.

One durable ledger tracks eligible credits, pending transfers, allocated destination cash, purchase reservations and settled spending. Final validation precedes the atomic pre-send claim. Only matching settled transfer receipts fund purchases; only reconciled settled fills consume purchase allocations. Unknown outcomes remain reserved across restarts. A missing order is not affirmative rejection, and a lost transfer acknowledgement must not trigger a blind resend.

An explicit startup sample permits a separate lifetime allowance of 5 USDC from the source balance. Its immutable operator-funding record is separate from sale credits, cannot be replenished on restart, and stops new sample sends after the first positive settled purchase. Pre-existing setup cash remains excluded. New routine funding comes only from eligible future sale proceeds; any remainder of the already-authorized sample allowance keeps its original provenance.

The [implementation and operating guide](plans/2026-11-09-drv-accumulation-plan.md) documents configuration, the user-enabled startup sample, recovery and validation. Deployment keeps the feature off until the operator explicitly enables it. Private account diagnostics and advisor records are not published with the implementation.
