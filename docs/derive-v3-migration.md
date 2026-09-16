# V3 migration and parallel testnet rehearsal

V2 remains the default. `npm run bot`, `npm start`, and the normal dashboard continue to select V2 unless `NOOP_VENUE` is explicitly changed. No running production process was restarted or stopped during this work.

The V3 runner uses the existing strategy through a compatibility adapter. It has its own SDK dependencies, credentials, SQLite database, knowledge directory, execution journal, and writer lock. The testnet dashboard uses port 3001 and a separate Next build directory. The production deployment was not changed.

**Current testnet account**

- Network: Ethereum Sepolia / Derive V3 testnet.
- Owner: `0x42C86eD5c471296b65370C2135cA5D07C727ED1e`.
- Subaccount: `78645`; manager: `1` (`SM`); risk universe: `1`.
- Funding: 1 Sepolia ETH supplied by the user; 1,000 mock USDC minted; 100 mock USDC deposited.
- Key: `.derive-v3/testnet/public/testnet.key`, mode `0600`.
- Public configuration: `.derive-v3/testnet/account.json`.
- Reports/journal: `.derive-v3/testnet/78645/`.

All `.derive-v3` files are ignored by Git. Never copy this test wallet or its database into production. The V3 clients never fall back to `PRIVATE_KEY` or `.private_key.txt`. Testnet and mainnet use different credential-variable prefixes.

**Install and repeat the checks**

```sh
npm run v3:install
npm ci --prefix dashboard # needed for the dashboard source regression tests
npm run test:v3
npm run v3:public
npm run v3:account
npm run v3:signing
DERIVE_V3_TESTNET_EXECUTION=enabled npm run v3:order
npm run v3:account
```

The local account configuration is loaded automatically by testnet commands. Explicit environment variables override it. `v3:public` needs no key. `v3:signing` signs a put and call but sends them only to `private/order_debug`; it submits no option orders. `v3:order` places a post-only ETH-PERP buy and cancels it immediately, requiring an initially empty account and a flat account afterward. It caps quantity at 0.1 ETH and mock notional at $500; its signed fee ceiling is $10 per ETH. A fill or uncertain result makes the test fail and requires reconciliation.

`v3:init`, `v3:deposit`, and `v3:discover` support new account setup. Do not rerun initialization for the existing wallet: key creation refuses to overwrite an existing file. Deposits create a NEW subaccount, so do not repeat a successful deposit merely because crediting is delayed. The deposit helper verifies chain ID, deployed ActionManager code, and the expected underlying mock USDC. Faucet minting was performed directly against the mock token's `mint(address,uint256)` function, as used by the official testnet developer page.

**Run the strategy alongside V2**

In a separate shell/process:

```sh
# No order placement or cancellation by default.
npm run bot:v3:testnet

# In another shell, view only this test account and database.
npm run dashboard:v3:testnet
```

The testnet bot overrides `DATA_DIR` and `WIKI_DIR` to `.derive-v3/testnet/78645/data` and `knowledge`. It does not seed from production. It also removes production Telegram settings from its own environment. The dashboard expects the testnet bot to have created its database; public/account/signing/order smoke tests do not create the strategy database.

To allow the full strategy to trade mock capital, start a separate testnet process with `DERIVE_V3_TESTNET_EXECUTION=enabled`. Strategy model calls still require the usual model API credentials and can incur model-provider charges. Execution-disabled mode blocks both placements and cancellations; it is observation, not a shadow fill simulator. Do not disable cancellations on an account that has outstanding orders you expect the bot to manage.

These runners were prepared but no continuous strategy process was left running. Successful API tests do not establish strategy profitability, capacity, or long-running operational reliability.

**What changed in the integration**

The default V2 request/signing path is retained. V3 replaces instrument discovery with paginated `get_all_instruments`, normalizes perp tickers, uses the SDK's action signing and nanosecond string nonces, separates owner from signer, and reads complete paginated history. Existing IOC/GTC/post-only and reduce-only intent passes through the adapter; the bot's requested signature lifetime is preserved.

V3 account failures stop the relevant decision rather than becoming empty positions or zero balances. Missing terminal orders use history; disappearance alone is not evidence of cancellation. The SDK completes validation, metadata lookup and signing before the execution journal records intent immediately before send. Acknowledgements retain operation/batch fields and do not clear the journal until Noop saves accounting. Transport failures, confirmation-timeout RPC codes 9000/9001, and malformed acknowledgements block further placements across restarts. There is no automatic retry of an uncertain order.

Initial fills save budgets, trade rows, pending actions and remaining resting exposure in one SQLite transaction. Resting orders account for incremental partial fills using both cumulative quantity and cumulative cost; terminal reconciliation does not charge earlier fills twice. Cancellation requests leave orders tracked until their terminal state is observed. Database failures roll back the in-memory budget as well as the transaction.

```sh
# Stop the testnet strategy writer first, then inspect unresolved intents.
npm run v3:reconcile
```

Reconciliation clears an intent automatically only when its exact nonce is confirmed cancelled or expired with zero fills for the configured subaccount. Finding an open or filled order does not establish that its accounting was saved: those intents remain blocked and are listed in `accounting_required`. An absent nonce also remains unresolved. Preserve the journal and database and reconcile venue fills, local trade rows, remaining orders and budgets before explicitly completing recovery; an automated importer for fills discovered after an uncertain send is not implemented. Never delete an intent merely to resume execution. The smoke test refuses to place another order while any position or open order exists.

`writer.lock` is an exclusive filesystem lock within an account's V3 directory. After a hard kill, confirm the old process has stopped before removing its stale lock. This lock does not coordinate different machines with separate disks and does not stop an existing V2 deployment. Keep a single production writer in the process manager.

**Testnet incompatibilities found and handled**

1. SDK package `3.0.13` bundles an older Sepolia ActionManager and USDC. [deployment.js](../integrations/derive-v3/deployment.js) pins the current documented ActionManager and verified mock token. Revalidate these when testnet is redeployed; do not propagate this override to mainnet. The failed old-manager deposit stopped at gas estimation; it did not move deposited funds.
2. Option ticker requests require an expiry, even though the generic request schema makes the field optional.
3. The minimum ETH-PERP order and required fee ceiling exceeded the first smoke-test limits. The tests now validate venue size/price metadata and retain explicit local bounds.
4. Cancelled orders may immediately disappear from `get_order`; history or the explicit cancellation acknowledgement supplies terminal state.
5. Public margin simulation works on the tested deployment despite the “coming soon” documentation entry. This does not establish full SM/PM2 risk parity with V2.

Sources: [migration changes](https://docs.derive.xyz/migrating/breaking-changes), [contracts](https://docs.derive.xyz/getting-started/contracts), [SDK](https://github.com/derivexyz/derive-ts), [action signing](https://docs.derive.xyz/authentication/action-signing).

**Production handoff when V3 is ready**

This is a controlled process handoff, not a mid-request endpoint change. Testnet preparation does not require stopping V2. The eventual cutover does require a brief pause while the old writer stops and account/state reconciliation completes.

Before that window, confirm Derive's mainnet launch, ownership migration, supported instruments, signing/deployment parameters, lending/margin behavior for our book, and treatment of outstanding orders. Rehearse actual fills, partial fills, reconnects, and settlement delays with the intended strategy before authorizing production. The current successful live test covered placement/cancellation with no fill; it did not exercise those additional lifecycle cases.

At the window:

1. Disable new V2 work, cancel/reconcile resting orders, resolve pending actions, then stop its writer through the existing process manager. Do not liquidate the portfolio as a substitute for Derive's state migration. Confirm no second V2 instance can restart.
2. Capture the final V2 account snapshot using `npm run v3:snapshot:v2`. This command is explicitly read-only and is the only new capture tool that intentionally uses the production V2 credential. It was not run during testnet testing.
3. Configure `NOOP_VENUE=v3-mainnet`, `DERIVE_V3_MAINNET_RELEASE=verified`, the migrated `DERIVE_V3_MAINNET_OWNER_ADDRESS`, `DERIVE_V3_MAINNET_SUBACCOUNT_ID`, and a dedicated `DERIVE_V3_MAINNET_KEY_FILE` or `DERIVE_V3_MAINNET_PRIVATE_KEY`. Prefer a narrowly scoped session key. Leave `DERIVE_V3_MAINNET_EXECUTION=disabled` during preparation.
4. Capture the V3 mainnet account with `npm run v3:snapshot:mainnet`. Compare fresh snapshots:

```sh
node integrations/derive-v3/handoff.js \
  .derive-v3/handoff/v2-snapshot.json \
  .derive-v3/handoff/v3-mainnet-snapshot.json \
  > .derive-v3/handoff/check.json
```

The check rejects stale V3 or malformed snapshots, outstanding regular/trigger/algo orders, liquidations, or position/collateral quantity differences. It pins the source V2 identity and requires explicit account availability and risk metadata. If a source endpoint cannot enumerate an order class, capture fails closed; the launch-specific migration procedure must establish that the account is drained. The final frozen V2 snapshot can predate the venue's migration window, provided V2 remained stopped afterward. Legitimate interest, fees, or migration adjustments must be understood and reconciled; do not edit snapshots merely to make the check pass. It does not expect identical market valuations or margin requirements between engines.

5. Preserve the stopped V2 ledger and knowledge with:

```sh
npm run v3:prepare-state -- /absolute/path/to/v2/noop.db /absolute/path/to/v2/knowledge
```

This uses SQLite's backup API, including committed WAL state, and refuses unresolved local orders/actions or an existing destination ledger. It copies budgets, rules, history, and knowledge into the V3 mainnet account directory. Existing rows in orders, portfolio snapshots, resting orders, and pending actions receive `venue=v2`; future rows default to `v3-mainnet`. It never writes the source database. If preparation fails halfway, inspect/archive the partial destination before retrying; do not delete the production source.

6. Review preserved rules and budgets against the migrated book. Set `DERIVE_V3_MAINNET_HANDOFF_FILE` to the absolute `check.json` path, refresh the snapshots/comparison if more than five minutes elapsed, and set `DERIVE_V3_MAINNET_EXECUTION=enabled`. Start `npm run bot:v3:mainnet`. Startup requires prepared state and a fresh successful schema-2 comparison for that exact owner/subaccount; reports created before the audit are refused. Activation is recorded so ordinary restarts do not depend on an expired comparison file; account checks and the writer lock still apply. The writer lock is acquired before preflight/journal reads, state paths reject symlinks, and the V3 SQLite writer uses `synchronous=FULL`.
7. Run the dashboard with the same mainnet profile and `DATA_DIR`/`WIKI_DIR` returned by state preparation. Observe the first order through acknowledgement, fills, account changes, and batch/L1 settlement. Keep the stopped V2 ledger intact as historical evidence.

For containers, persist `/app/.derive-v3` with a dedicated volume and mount the relevant signing key explicitly. The testnet process needs its own service; do not repoint the current V2 service's environment. The Dockerfiles include isolated V3 dependencies, but no image was deployed by this task.

A rollback is operational: stop V3, reconcile its actions, and verify whether V2 is still available and represents the same account state. Never reactivate V2 merely by changing the URL after V3 has traded. Derive's final migration schedule and state rules remain external release conditions.

**Validation record, 11 September 2026**

- Existing suite: 482 trading-system checks and 8 model/API tests passed.
- Dedicated migration tests: 17 passed, including SDK signature verification, account isolation, pagination, uncertain-send recovery, concurrency guards, terminal-order lookup, and budget/history preservation during state copy.
- V3 public market data: 704 ETH options; 42 matching tickers for the sampled expiry.
- Public SM margin simulation: successful response.
- Authenticated account/history reads: passed on subaccount 78645.
- ETH put and call signing: local encoded data and hashes matched `private/order_debug`.
- Post-only order `59c8db18-ca53-4d2b-8315-7af5a6d0b47e`: placed, cancelled, zero fill; final account check had no positions or open orders.
- Isolated Next dashboard production build: passed. No production dashboard build cache was overwritten.
- [Testnet deposit transaction](https://sepolia.etherscan.io/tx/0x0417b1089f9799e8210abddb87e4120f10c169583c85334e0518dd0c6aaa292f).

These results establish a working parallel testnet integration and a gated handoff path. They are not mainnet release approval or proof of complete strategy/risk equivalence.

The subsequent [implementation audit](derive-v3-audit-2026-09-11.md) found and fixed material execution/accounting issues; the original smoke results alone were insufficient to establish recovery correctness.
