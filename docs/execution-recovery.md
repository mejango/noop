# V2 execution accounting recovery

Order submissions are recorded in SQLite before sending. The submission remains unresolved until its fills, remaining order, budget and action status commit together. A network timeout, incomplete receipt or database failure stops further placement; restarting the process does not clear that block. An order missing from an API response is not treated as cancelled.

To recover an unresolved submission:

1. Stop the bot using its normal process supervisor. The recovery command updates the existing strategy database, so the bot must not retain an older in-memory budget while recovery runs.
2. Use the submission ID printed in the trading error. Run from the project directory, with the same `DATA_DIR` and signing credentials as the bot:

   ```sh
   node bot/reconcile-execution.js --submission 12 --bot-stopped
   ```

3. The command reads venue order and trade history. It checks owner/subaccount, signed nonce, order ID, instrument, direction, quantity, price and cumulative fills before committing local accounting. It neither submits nor cancels an order. Successful recovery updates any remaining resting order and releases the submission block. Restart the bot only after success.

Missing evidence, conflicting receipts, incomplete pagination and duplicate trade IDs leave the submission blocked. The command never clears a submission merely because an order cannot be found. Resolve the missing venue evidence before retrying; there is no force-clear option. Repeating recovery for an accounted submission refuses to book it again.

Resting orders are reconciled by exact venue ID on every observation. Partial fills use the venue's cumulative quantity and average price to book only the new quantity/value. A cancellation acknowledgement preserves the remaining reservation until the final cumulative status is observed. Existing production orders with unknown prior accounting or no local tracking record need evidence reconciliation before trading resumes; they are not silently adopted or guessed.

Premium accounting uses actual trade prices and quantities. Raw order/trade responses are retained for the separate economic ledger's fee and settlement reconciliation; unknown fees are not fabricated.

For an order created before the submission journal, use `node bot/reconcile-legacy-order.js --order ORDER_ID --bot-stopped` with the same database/signing environment. This requires exact original local receipts and complete venue order/trade evidence; it reconstructs the already-booked baseline before applying any new fill delta. Unproved manual orders or ambiguous prior accounting remain blocked. See the [V2 remediation runbook](v2-audit-remediation-2026-09-11.md) for backup and migration details.
