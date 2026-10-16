# Dip Buy: put proceeds into ETH, fully automated

Status: designed 2026-10-16. Queued to build; the first live run happens on the first real batch (see Rollout).

## Intent

When the tail puts pay off, noop turns the proceeds into ETH without a human in the loop. Put sales usually happen during a drop, so the sale itself marks the dip. noop pools put-sale proceeds, withdraws them from Derive, market-buys native ETH on a deep DEX, and counts that ETH as part of what the strategy protects.

Success means the following, all with no manual step after the one-time setup:
- every filled put sale credits the pool;
- once the pool can fund a batch, noop withdraws, buys ETH and settles;
- the Derive account keeps its USDC float and its 10% collateral floor;
- a restart at any point never sends a withdrawal twice and never loses track of funds.

## Why not Derive spot

As of 2026-10-16, the Derive V3 ETH-USDC book had no bids and a single 1 ETH ask. 24h volume was 0, and the last 30 days saw 27 taker trades (19.6 ETH) filled 0.6–1.9% off the index. It cannot absorb a crash-time buy. ETH-PERP is deep but gives leveraged exposure rather than ETH the account owns.

## Rules (agreed with the user)

| Rule | Value |
|---|---|
| Funding | Proceeds of every filled `sell_put` (both `roll_protection` and `monetize_tail_win`) credit the pool. |
| Trigger | Buy immediately once withdrawn USDC arrives in the dip wallet (a market order). There is no ladder and no limit pricing. |
| Minimum batch | $250 of USDC per withdrawal and buy. |
| USDC float | Derive keeps at least 4× the current put-cycle budget in USDC. noop only withdraws USDC above that. The cycle budget scales with protected ETH, so the float does too. The user's target band is 4–5×; 4× is the hard floor. |
| Collateral floor | Derive collateral value (ETH plus USDC) must stay at or above 10% of the value of total protected ETH. |
| Floor breach | Buy anyway, then deposit just enough of the new ETH into Derive to restore 10%. The rest stays in the dip wallet. If collateral is already below 10%, the whole buy goes to Derive until the floor holds. |
| Holding | The dip wallet holds native ETH. Any WETH it receives is unwrapped. The only exception is the Derive top-up, which uses whichever asset Derive credits as ETH collateral (WETH is the registered underlying; native ETH is used if Derive accepts it). |
| Protected ETH | Derive ETH + `PUT_INSURED_EXTERNAL_ETH` (20 today) + the dip wallet's live ETH balance. |

## Money flow

1. **Pool.** A filled put sale adds `fill_price × filled_amount` (net of fees) to the `dip_pool` ledger in `noop.db`.
2. **Gate.** Each tick computes `withdrawable = min(pool balance, Derive USDC − 4 × cycle budget)`. The step proceeds only if this is at least $250.
3. **Withdraw.** `private/withdraw` sends `withdrawable` USDC to the dip wallet on Ethereum L1. The session key holds the `withdraw` scope, and the dip wallet is a whitelisted recipient. The amount is in native decimals (6 for USDC). `max_fee_usd` is capped, starting at 1.5. noop tracks completion with `private/get_withdrawal_history`.
4. **Buy.** Once the USDC is in the wallet, noop asks CoW Protocol for a quote of USDC for native ETH (`buyToken` set to the native-ETH sentinel), then signs (EIP-712, `ethers`) and posts a market order with a 0.5% slippage cap. The order costs no gas and is protected from MEV.
5. **Top up.** If Derive collateral is below 10% of total protected ETH (counting the new ETH), the dip wallet sends the shortfall to Derive's registered ETH deposit address with a plain transfer, paid for with the seed gas.
6. **Account.** noop debits the pool, refreshes protected ETH, logs each step and sends a Telegram notice for withdraw, fill, top-up and failure.

## Components

| Unit | Responsibility | Depends on |
|---|---|---|
| `bot/dip-pool.js` | Pure math: pool credit and debit, withdrawable amount, top-up split. No I/O. | nothing |
| `bot/derive-withdraw.js` | Signs and sends `private/withdraw`; polls withdrawal history. | existing Derive signer and config |
| `bot/cow-swap.js` | Quote, build, sign, post and poll a CoW order (native-ETH output, slippage cap). | `ethers`, CoW API |
| `bot/dip-wallet.js` | ETH, WETH and USDC balances, one-time USDC approval to the CoW vault relayer, WETH unwrap, ETH deposit transfer, low-gas warning. | `ethers`, `ETHEREUM_RPC_URL` |
| `dip_buys` table | One row per batch: amounts, ids, tx hashes, state, reason, timestamps. | `bot/db.js` |
| `script.js` tick hook | Advances each open batch by at most one step per tick. | the units above |

### State machine (one row per batch)

`pooled → withdrawing → awaiting_funds → ordered → filled → topping_up → settled`, with `failed` (and a reason) reachable from any state.

- Every transition is persisted before its side effect is confirmed, and every step can be retried safely. Before a withdrawal is sent, noop stores its nonce and signature. On restart it checks withdrawal history for that nonce instead of signing again, so a withdrawal is never duplicated.
- **Withdrawal stalls:** the batch stays in `withdrawing` and polling continues. Only an explicit failure from Derive returns the amount to the pool.
- **No quote, or the order expires unfilled:** noop re-quotes with backoff, at most 3 tries, and never exceeds the slippage cap. After that the batch becomes `failed`, the USDC stays in the dip wallet, and a Telegram alert goes out.
- **Top-up transfer fails** (for example, out of gas): the batch stays in `topping_up` and noop alerts. The floor is still recorded as owed.

### Protected-ETH integration

The put-budget code (`script.js:546`, `insuredEth`) adds the dip wallet's ETH balance, cached per tick. On an RPC failure noop uses the last good reading, so a read error never shrinks the budget. The advisor's budget note and the journal snapshot each get one line for dip-wallet ETH.

## Configuration

| Variable | Purpose |
|---|---|
| `DIP_BUY_ENABLED` | Master switch (default off). |
| `DIP_BUY_DRY_RUN` | Run the full loop and log each step without signing or sending anything (default on). |
| `DIP_WALLET_PRIVATE_KEY` | The dip hot wallet. It holds only the pool and the seed gas. |
| `DIP_BUY_MIN_USD` | 250 |
| `DIP_BUY_FLOAT_CYCLES` | 4 |
| `DIP_BUY_COLLATERAL_FLOOR` | 0.10 |
| `DIP_BUY_MAX_SLIPPAGE_BPS` | 50 |
| `DIP_BUY_MAX_WITHDRAW_FEE_USD` | 1.5 |

## One-time setup (user)

1. Create the dip wallet, set `DIP_WALLET_PRIVATE_KEY` in Railway, and seed it with gas ETH (about 0.01–0.02 ETH).
2. From the owner wallet: whitelist the dip wallet as a withdrawal recipient, and give noop's session key the `withdraw` scope. (Its current admin key expires by 2026-11-03; the trading-scoped replacement needs `withdraw` added.)
3. Register a Derive ETH deposit address for the subaccount (`public/register_deposit_address`).
4. noop sends the USDC approval to CoW's vault relayer once, using the seed gas.

## Rollout

The user chose not to run live tests ahead of time. The first live run is the first real batch, after a dip has paid the puts.

1. **Build and ship in dry run** (`DIP_BUY_ENABLED=1`, `DIP_BUY_DRY_RUN=1`). Every time the pool could fund a batch, noop logs the full plan (withdrawable amount, quote, split, top-up) and sends it to Telegram, but signs nothing.
2. **Go live** by setting `DIP_BUY_DRY_RUN=0`. Until the first batch reaches `settled`, the first live batch is capped at `DIP_BUY_MIN_USD` × 2, and every step sends a Telegram notice. The two facts nobody has verified yet (withdrawal settlement time, and how Derive credits a deposit to its address) are first observed on that batch, and the state machine tolerates both taking hours.

The spec does not settle when to flip `DIP_BUY_DRY_RUN` to 0. The user decides: live as soon as setup is done, or only once a dry-run plan looks right.

## Testing

- **`bot/dip-pool.js`:** pool math, the $250 minimum, the 4× float, the pool cap, the top-up split (including the case where collateral is already below 10%).
- **State machine:** every transition against stubbed Derive, CoW and RPC clients; a restart in every state; withdrawals never duplicated; failure paths return funds correctly.
- **Signing:** the CoW order EIP-712 hash against a known vector; the withdraw signature against the existing Derive signer tests.

## Out of scope

Limit or ladder orders, other chains, other DEXs, sweeping to cold storage, and advisor-chosen prices. None of these needs a change to the state machine.
