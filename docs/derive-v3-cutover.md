# Derive V3 cutover — 2026-10-06

Production deployment `e0b92c36-ace7-4cbe-935a-6f1e7ce1055e` is running V3
in maintenance mode in Singapore. Trading remains disabled pending the order
reconciliation described below. Deployment used a local working-tree snapshot;
this revision records the deployed migration and its verification evidence.

## Execution record

- Production backup: `/data/archive/pre-derive-v3-20261006.db`, 3,712,483,328 bytes;
  SQLite integrity check passed. SHA-256:
  `104325af79675d6277d3642bba5c3ed8476c464a8dd395f41e16fa6ea5000e0a`.
- Authenticated owner: `0x823b92d6a4b2AED4b15675c7917c9f922ea8ADAD`.
  Subaccount `25923` remains owned by that wallet. The original production
  session signer authenticates V3 reads.
- Deployed preflight passed at 19:04 UTC: 904 ETH option instruments, valid
  account data, no open venue orders, and no liquidation flag. All five position
  quantities match the last V2 snapshot. ETH collateral matches at V3 precision;
  USDC is approximately $0.043 higher. No post-cutover trades were returned.
- Non-executing `private/order_debug` check passed at 19:05 UTC: the deployed
  order builder's signature recovers the production signer from the venue's
  own typed-data digest, and the signing domain matches. No order was placed.
- Maintenance ticks now send supervisor heartbeats, preventing periodic
  container restarts. The combined image includes the preflight script.
- Local orders `544b3605-acc7-4597-8122-370f9e8ae038` and
  `17612d7f-ae4b-48cf-8cce-588d5976496d` remain marked open. V3 returns
  `11006 / Order does not exist` for both; V2 still returns HTTP 503. Their local
  status has deliberately not been overwritten. All execution submissions are
  already accounted or rejected. One pending replacement action is preserved.
- The previous testnet account `78645` now returns `14000 / Account not found`.
  Testnet order placement has not been performed. Mainnet signing was checked
  without executing an order.
- Full read-only continuity comparisons completed at 19:18 UTC and again
  after the final deployment at 19:30 UTC: all 41 table
  row hashes and the complete schema are identical to the pre-migration backup.
  Original tracking start remains `2026-09-11T19:33:23.753Z`; complete V2 trade
  coverage reaches `2026-10-06T17:55:19.841Z`. No V3-prefixed ledger rows exist.
  The adapter retains the legacy ledger ID/source strings and quote-source label,
  and preserves the existing cursor and any pending history work.
- Evidence reports are retained under `/data/archive/derive-v3-*-20261006.json`.
  The encrypted local backup completed at 19:36 UTC:
  `.derive-v3/production/pre-derive-v3-20261006.enc` (711,562,813 encrypted bytes).
  AES-GCM authentication and the decrypted 3,712,483,328-byte database SHA-256
  both passed. The recovery key is in macOS Keychain, service
  `noop.derive-v3.backup.20261006`, account `jango`. Metadata, verification reports,
  and `restore-backup.js` are beside the encrypted file. Earlier `.partial`
  attempts are not backups; use the verified `.enc` file.

  To restore to a NEW local file (this intentionally creates plaintext):

  ```sh
  node .derive-v3/production/restore-backup.js .derive-v3/production/pre-derive-v3-20261006.enc /path/to/new-restored.db
  ```

  The helper refuses to overwrite an existing destination and verifies the
  restored database hash. The temporary HTTPS download is disabled by moving its
  fixed artifact to `.enc.retained` and disabling its staged access credentials;
  the server-side encrypted artifact and original verified database backup remain
  on the persistent volume.


The bot still defaults to V2 for an unchanged deployment. Setting
`DERIVE_API_VERSION=v3` requires an explicit owner, subaccount, and history boundary,
and defaults to maintenance mode. Both services use the same configuration.

## Collect data while trading is paused

Set `DERIVE_MAINTENANCE=true` and `DERIVE_COLLECT_DATA=true` to resume the
existing market, options, smile, open-interest, onchain and portfolio snapshot
writers and settled-trade ingestion. This uses the same database, schema, ledger
identifiers and normalized formats. The existing session signer authenticates V3
reads; the production read-only preflight passed again at 19:49 UTC.

Collection mode skips order reconciliation, placement/cancellation, budget-cycle
resets, pending-action execution, decision/lifecycle relabeling, and advisory
publication. Direct placement and cancellation guards remain active. Set
`DERIVE_COLLECT_DATA=false` to return to the full maintenance pause. Unresolved
V2 orders remain preserved until their terminal state can be established.

## During the outage

1. Stop the production bot worker (keep the dashboard available where deployed
   separately), or deploy this revision with `DERIVE_MAINTENANCE=true`. Setting
   that variable on the old revision has no effect. The new maintenance loop keeps
   the process alive but skips trading, reconciliation, observations, and recurring
   trading error notifications. It does not cancel venue orders.
2. Preserve the production database and logs. Use the existing online SQLite
   backup utility on the service with the production volume:

   ```sh
   node scripts/archive-v2-data.js --db /data/noop.db --out /data/archive/pre-derive-v3.db
   ```

   Retain the archive off the running volume too. This preserves local evidence;
   it is not an exhaustive venue trade export. If V2 history is unavailable, record
   the missing interval rather than marking it complete. Do not copy only a live
   `.db` file while ignoring its WAL.
3. Inventory local open/resting orders, pending actions, and unresolved execution
   submissions. Preserve their IDs and receipts. An unavailable API does not prove
   an order was canceled or the account is flat. Do not clear these tables.

## Configure the cutover while paused

Set these on BOTH bot and dashboard services, preserving existing secrets and
the non-US deployment region:

```dotenv
DERIVE_API_VERSION=v3
DERIVE_NETWORK=mainnet
DERIVE_MAINTENANCE=true
DERIVE_WALLET=<confirmed migrated EOA or multisig owner, not the old smart wallet>
DERIVE_SUBACCOUNT_ID=<confirmed migrated subaccount>
DERIVE_HISTORY_FROM=<confirmed start of V3 history, ISO UTC timestamp>
```

The history boundary describes API availability, not a new database epoch.
The existing tracking start and coverage cursor are retained. If V2 coverage has
not reached the boundary, automatic ingestion stops and preserves pending history
work rather than skipping that interval. The boundary must precede the first V3
activity; do not use a later restart time. The old checklist's truncated owner
address is insufficient to populate this configuration. The subaccount may retain
its ID, but that must be confirmed rather than inferred.

The mainnet URL and signing domain are selected automatically. Optional
`DERIVE_API_URL` and `DERIVE_DOMAIN_SEPARATOR` overrides must match that network.
For an isolated testnet rehearsal use `DERIVE_NETWORK=testnet`, testnet identity,
testnet signer, and a separate `DATA_DIR`; never run the testnet bot on the
production database. The checked-in `.env.example` is documentation; these Node
entry points expect exported environment variables or deployment configuration.

`PRIVATE_KEY` remains the signing key. The configured wallet is the owner, which
can differ from an authorized session key signer. Confirm the key's V3 trading
permissions before enabling orders; registering a session key is separate work.

## Read-only checks before enabling trading

Run from the repository root or the deployed image with the confirmed environment:

```sh
node scripts/derive-preflight.js --public
node scripts/derive-preflight.js --out /data/archive/derive-v3-account-check.json
```

The second command reads the private account and open orders and writes a new
mode-0600 report without overwriting an existing file. Neither command starts the
bot, changes the database, submits orders, or cancels orders. A successful report
is deliberately not a trading readiness certificate.

Compare the returned positions, collateral assets and quantities, debt, margins,
liquidation status, owner/subaccount, and open orders with the pre-cutover records
and Derive app. Investigate any unexpected migration collateral/debt changes.
Public API success alone does not establish that private trading has reopened.

Reconcile every carried order and uncertain submission using venue evidence.
The submission recovery command checks the nonce format and owner against the configured API; it
cannot silently reinterpret V2 receipts as V3 receipts. Do not bypass that check
or edit old receipt identities. Legacy recovery writes local accounting and
requires the bot stopped. If V2 evidence is no longer available, keep the item
unresolved until its migrated disposition is proved.

Generate a fresh advisory and review pending actions before resuming. Production retains the same subaccount ID, database path, schema, history,
ledger identifiers, and normalized record formats. Do not create a separate V3
database or rewrite prior rows. Raw venue receipts remain original evidence;
API-specific status fields are interpreted at ingestion, not by rewriting history.

## Enable and observe

After Derive confirms trading is open, account/order reconciliation is complete,
and the signer has been validated on testnet, set `DERIVE_MAINTENANCE=false` on the
bot and restart it. Observe the first account read, advisory, submission receipt,
fill reconciliation, and settled trade ingestion before leaving it unattended.
Keep the dashboard configuration aligned.

If checks fail, return to maintenance mode or stop the bot. Do not fall back to
V2 trading after the venue retires it, clear unresolved orders, or manufacture
zero balances to resume execution.

## Prepared changes and verification

- Shared version/network/account configuration across bot, dashboard, and recovery.
- V3 auth headers, mainnet/testnet signing domains, and exact nanosecond string nonces.
- Complete paginated `get_all_instruments` reads, including the dashboard smile chart.
- Explicit V3 `batch_status === 'Settled'` accounting with unchanged legacy event IDs and normalized record format.
- Maintenance loop, direct order placement guard, and read-only preflight.
- Local testnet artifacts excluded from git and Docker build contexts.

At 18:53 UTC on October 6, the new public listing client successfully read 620
testnet and 904 mainnet ETH options. Those observations establish public API
availability only. Local tests verify the production order signature by recovering
its signer from independently constructed EIP-712 data. The full `npm test` suite and dashboard `npm run build` passed. Authenticated venue
checks subsequently passed on the deployed mainnet service as recorded above;
testnet order placement remains unverified.

The original plan's session-key deadline, withdrawal timeline, and rewards
deadline are operational follow-ups that should be reconfirmed in current venue
announcements; this code preparation does not establish those dates.

Sources checked October 6: [official breaking changes](https://docs.derive.xyz/migrating/breaking-changes.md),
[action signing](https://docs.derive.xyz/authentication/action-signing.md),
and [OpenAPI schema](https://docs.derive.xyz/openapi.json).
