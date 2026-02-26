# Principles

## Data Chunking & Long-Term Scale

This dashboard will run for years. Raw data grows linearly with time — options snapshots alone can produce 100k+ rows/day. Without downsampling, queries over 30d+ windows become unusably slow and charts become unreadable noise.

### Strategy: Time-Resolution Tiers

Store and serve data at decreasing resolution as it ages:

| Age | Resolution | Source |
|-----|-----------|--------|
| < 24h | Raw (every tick, ~10min intervals) | Live tables |
| 1–7d | 1-hour aggregates | Materialized at write time |
| 7–30d | 4-hour aggregates | Rolled up from hourly |
| 30d+ | Daily aggregates | Rolled up from 4-hour |

### Implementation Approach

1. **Rollup tables** — Create `_hourly`, `_4h`, and `_daily` summary tables for spot_prices, options_snapshots, and onchain_data. Each stores pre-aggregated min/max/avg/count per bucket.

2. **Rollup on write** — When the bot inserts new data, also upsert into the current hourly bucket. A periodic job (daily) rolls hourly → 4h → daily.

3. **API serves the right tier** — The `/api/chart` endpoint picks the tier based on the requested range:
   - `1h`, `6h` → raw
   - `24h`, `3d` → hourly
   - `7d` → 4-hour
   - `30d`, `90d`, `1y` → daily

4. **Client stays dumb** — The frontend receives pre-bucketed data and renders it directly. No client-side downsampling for large ranges.

5. **Retain raw data** — Never delete raw rows. They're needed for correlation engine accuracy and historical audits. Just don't serve them for large time ranges.

### What Gets Aggregated

For each bucket (hour/4h/day), store:

- **Spot prices**: open, high, low, close, avg, momentum mode (most frequent direction)
- **Options snapshots**: best put/call delta-value, avg spread, avg depth, avg IV, total OI
- **Onchain data**: avg magnitude, dominant flow direction, total volume delta per DEX
- **Liquidity**: last TVL per DEX (step-after semantics), sum of volume deltas

### Chart Rendering at Scale

- At daily resolution, 1 year = 365 points — fast to query and render.
- Volume bars should aggregate (sum deltas within bucket), not average.
- Heatmap scatter plots (put/call market, market quality) should thin to best-per-bucket at large scales — showing every instrument at 30d would be thousands of dots.
- Momentum bars at daily resolution show the dominant direction for that day.

### Migration Path

Rollup tables can be backfilled from existing raw data. The schema change is additive (new tables, no changes to existing ones), so it's safe to deploy incrementally.
