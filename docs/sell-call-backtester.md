# Sell-Call Backtester

The sell-call backtester is an offline, read-only research component. Nothing in the live bot imports it, and it never writes to the trading database.

It compares four policies over the same chronological option-chain replay:

- `no_call`: ETH and cash without a call overlay
- `raw_score`: highest eligible `bid / abs(delta)` candidate
- `current_edge`: the current hard-coded composite CALL EDGE formula
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

The separate edge tuner searches deterministic strengths for each existing CALL EDGE factor and its entry thresholds. It treats raw score as the incumbent and divides history chronologically into train, validation, and a final untouched holdout. The winning formula is selected using train and validation only; the holdout is opened once after selection.

```sh
DB_PATH=/private/tmp/noop-research.db npm run research:tune:call-edge -- --days=all --search-count=2500
```

Outputs default to:

- `data/sell-call-edge-tuning.json`
- `data/sell-call-edge-tuning.md`

The tuner does not edit or import the live scoring path. A tuned formula remains a research challenger until it repeats out of sample and in shadow execution.

## Study weekly DTE rollover normalization

Raw `bid / abs(delta)` compares otherwise similar calls with different time remaining. The DTE study tests a partial constant-delta time normalization:

```text
normalized score = raw score * (8.5 / DTE) ^ exponent
```

An exponent of zero is exactly raw score. Larger exponents progressively remove the approximately square-root-of-time premium difference that appears when the eligible weekly expiry rolls forward. The study chooses an exponent using train and validation only, requires at least 90% of raw overlay P&L in both, and then reports the untouched final holdout.

```sh
DB_PATH=/private/tmp/noop-research.db npm run research:analyze:call-dte -- --days=all
```

This remains separate from the live chart and selector until the backtest and shadow evidence justify a production change.

## Leakage controls

The learned policy labels hypothetical call sales using a future buyback ask or expiry settlement. A label is admitted to training only after that quote or settlement existed, plus the configured embargo. Models retrain on a rolling historical window and each artifact records its training interval, label cutoff, feature schema, sample count, and coefficients.

Training is bounded to the most recent 20,000 matured examples by default so repeated walk-forward retraining remains operational on long histories. The cap is configurable with `--max-train-samples` and is recorded in each artifact.

## Accounting

The simulator maintains cash, ETH collateral, short-call liabilities, exposure, approximate reserved margin, fees, entry premium, buyback cost, expiry payoff, and marked portfolio NAV. It reports both total portfolio return and incremental overlay P&L versus holding the same ETH and cash without calls.

The default `bid_ask` execution mode sells at the historical bid and buys back at the historical ask. This is deliberately conservative for crossing orders, but top-of-book history cannot establish whether a hypothetical maker order would have filled. `midpoint` and `mark` modes are sensitivity analyses, not execution claims.

## Limitations

- Margin is configurable and approximate; historical venue liquidation state cannot be reconstructed exactly.
- Hourly sampling may miss intrahour fills and adverse excursions.
- The current-edge baseline reproduces the production multipliers, while rolling score trend and OI change are reconstructed from the sampled historical frames.
- Missing pre-expiry quotes force an approximate intrinsic-value close at the end of the test.
- Model comparisons must be judged across multiple market regimes and effective timestamp groups, not raw option-row counts.
- Backtest results remain research artifacts until a challenger also succeeds in live shadow mode.
