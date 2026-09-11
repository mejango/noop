# Profit Correlation Engine

The profit correlation engine is an offline research tool for turning historical option snapshots into decision priors. It does not place orders and is not injected into live trading gates by itself.

Run:

```sh
npm run research:correlate
```

Run against an exported Railway snapshot:

```sh
DB_PATH=/private/tmp/noop-research.db npm run research:correlate -- --days=all
```

Deep run over all candidate rows instead of top hourly candidates:

```sh
DB_PATH=/private/tmp/noop-research.db npm run research:correlate:deep -- --max-samples=250000
```

Outputs:

- `data/profit-correlation-report.json`
- `data/profit-correlation-report.md`

By default, `research:correlate` scans all available history, ranks the top 8 candidates per action at the first recorded snapshot of each hour, and labels forward outcomes at `1h, 6h, 12h, 24h, 48h, 72h, 168h`. It does not choose the best quote retrospectively across the hour. It scores:

- raw option value score, DTE, delta, strike distance, bid/ask/mark, spread, depth, IV, OI
- same-instrument score/bid/ask/mark/IV/OI/depth/spread deltas over `1h, 6h, 24h`
- market spot returns, best call/put score deltas, spread/depth/OI/skew deltas, funding deltas, liquidity-flow deltas
- feature interactions that beat or trail the baseline outcome

Entry spot and strike distance use the latest recorded spot at or before the candidate timestamp. Market features and their changes use completed hours ending before that timestamp. A bounded `--days` run loads the extra preceding hours needed for those features, without adding those warm-up observations to the candidate sample. Reports record these timing conventions and counts of sampled hours, sampled timestamps, and candidates missing prior context.

The `profit-correlation-v2` report is not directly comparable with older reports that used whole candidate-hour market aggregates or selected the best quotes from anywhere in that hour. Regenerate those reports before using their feature recommendations.

Interpretation:

- `mean_return` is outcome PnL divided by entry premium.
- For `sell_call`, positive return means the future buyback ask was below the entry bid.
- For `buy_put`, positive return means the future sellable bid was above the entry ask.
- `tail_loss_rate` is an adverse move proxy. For short calls, it means future ask exceeded `2x` entry premium. For long puts, it means future bid dropped below `0.5x` entry premium.

Use the report as a candidate-selection research input. Promote a finding into live strategy only after checking sample count, stability across horizons, and whether the feature is actually available before execution.
