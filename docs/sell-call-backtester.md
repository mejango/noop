# Sell-Call Backtester

The sell-call backtester is an offline, read-only research component. Nothing in the live bot imports it, and it never writes to the trading database. It shares the pure production call-score function without loading the live bot runtime.

It compares four policies over the same chronological option-chain replay:

- `no_call`: ETH and cash without a call overlay
- `raw_score`: highest eligible `bid / abs(delta)` candidate
- `current_edge`: the production CALL EDGE formula, `bid / abs(delta) * (8.5 / DTE)^0.12`, with default minimum bid 4 and edge 65
- `learned_walk_forward`: regularized expected-capture and tail-loss models retrained only from outcomes available before each replay timestamp

## Run

Export the deployed database through the existing authenticated research snapshot endpoint, then run:

```sh
DB_PATH=/private/tmp/noop-research.db npm run research:backtest:calls -- --days=all
```

For a quick smoke test:

```sh
DB_PATH=/private/tmp/noop-research.db npm run research:backtest:calls -- --days=30 --max-frames=500
```

Outputs default to:

- `data/sell-call-backtest-report.json`
- `data/sell-call-backtest-report.md`
- `data/sell-call-model-artifacts.json`

Use `--help` for execution, portfolio, label, and learning controls.

## Tune CALL EDGE without test leakage

The separate edge tuner searches deterministic strengths for the historical July composite CALL EDGE factors and entry thresholds. Its `historical_composite_edge` baseline preserves that formula and its floor of 80; it is not the current production score. The tuner treats raw score as its incumbent and divides history chronologically into train, validation, and a final untouched holdout. The winning formula is selected using train and validation only; the holdout is opened once after selection.

```sh
DB_PATH=/private/tmp/noop-research.db npm run research:tune:call-edge -- --days=all --search-count=2500
```

Outputs default to:

- `data/sell-call-edge-tuning.json`
- `data/sell-call-edge-tuning.md`

The tuner does not edit or load the live bot runtime. A tuned formula remains a research challenger until it repeats out of sample and in shadow execution.

## Learn economic call value from realized paths

The economic call-value study is the newer research-only challenger for the production DTE-normalized `bid / abs(delta)` score. Instead of treating delta as the complete definition of risk, it reconstructs the actual future path for every eligible call and labels:

- net P&L under the configured profit-capture, stop, maximum-hold, or expiry rule
- P&L per reserved-margin day
- maximum adverse buyback excursion per margin-day
- realized loss and adverse-excursion breach probabilities

It uses premium, delta, DTE, moneyness, IV, spread, depth, open interest, market skew, OI/score trends, and causal 6h/24h/72h spot context. Weekly walk-forward models may train only on path labels that completed before the prediction timestamp plus an embargo.

```sh
DB_PATH=/private/tmp/noop-research.db npm run research:study:economic-call-value -- --days=all
```

Outputs default to:

- `data/economic-call-value-study.json`
- `data/economic-call-value-study.md`
- `data/economic-call-value-models.json`

Value/risk gates are generated from the chronological training window, selected on validation, and evaluated once on the final holdout. Entries near every fold boundary are purged so a position can follow its normal exit policy rather than being artificially closed at the split. Promotion requires a material holdout P&L improvement, no worse P&L per margin-day, and no worse realized or tail losses. The component does not load the live bot runtime.

## Study weekly DTE rollover normalization

Raw `bid / abs(delta)` compares otherwise similar calls with different time remaining. The DTE study tests a partial constant-delta time normalization:

```text
normalized score = raw score * (8.5 / DTE) ^ exponent
```

An exponent of zero is exactly raw score. Larger exponents progressively remove the approximately square-root-of-time premium difference that appears when the eligible weekly expiry rolls forward. The study chooses an exponent using train and validation only, requires at least 90% of raw overlay P&L in both, and then reports the untouched final holdout.

```sh
DB_PATH=/private/tmp/noop-research.db npm run research:analyze:call-dte -- --days=all
```

Production now uses exponent 0.12. The study remains available for evaluating alternative exponents separately from the live chart and selector.

## Leakage controls

The learned policy labels hypothetical call sales using a future buyback ask or expiry settlement. A label is admitted to training only after that quote or settlement existed, plus the configured embargo. Models retrain on a rolling historical window and each artifact records its training interval, label cutoff, feature schema, sample count, and coefficients.

Training is bounded to the most recent 20,000 matured examples by default so repeated walk-forward retraining remains operational on long histories. The cap is configurable with `--max-train-samples` and is recorded in each artifact.

## Accounting

The simulator maintains cash, ETH collateral, short-call liabilities, exposure, approximate reserved margin, fees, entry premium, buyback cost, expiry payoff, and marked portfolio NAV. It reports both total portfolio return and incremental overlay P&L versus holding the same ETH and cash without calls.

The default `bid_ask` execution mode sells at the historical bid and buys back at the historical ask. This is deliberately conservative for crossing orders, but top-of-book history cannot establish whether a hypothetical maker order would have filled. `midpoint` and `mark` modes are sensitivity analyses, not execution claims.

Entries are capped by finite, positive quoted bid depth by default. Non-settlement exits are capped by quoted ask depth, with partial fills reducing position quantity, allocated entry fees, and reserved margin proportionally. Zero, missing, or invalid depth prevents a fill. `--ignore-depth` explicitly assumes unlimited entry and exit liquidity for sensitivity analysis; each result records `config.useQuotedDepth`, and the Markdown report states the active assumption. Expiry settlement does not require order-book depth.

Any unfilled balance at the end of the replay remains open and marked in NAV using the existing liability estimate. Reports distinguish realized and unrealized call P&L and expose the remaining contracts and margin. Partial exit fills are grouped by entry in `trade_log`; `closed=false` identifies an incompletely closed entry, and `exit_fills` preserves each fill. Trade counts and win rates include completed entries only. Holding time is quantity-weighted across exit fills, preserving reserved-margin-day accounting. The version 2 report schema records these distinctions.

## Limitations

- Margin is configurable and approximate; historical venue liquidation state cannot be reconstructed exactly.
- Hourly sampling may miss intrahour fills and adverse excursions.
- The current-edge baseline shares the production score and defaults, but does not reproduce all live execution and risk gates.
- Missing quotes leave the position marked at the existing liability estimate; explicit depth-ignored simulations retain approximate intrinsic-value closes when a close quote is missing.
- Model comparisons must be judged across multiple market regimes and effective timestamp groups, not raw option-row counts.
- Backtest results remain research artifacts until a challenger also succeeds in live shadow mode.
