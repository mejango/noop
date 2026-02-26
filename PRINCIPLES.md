# Principles

## Data Chunking & Long-Term Scale

This dashboard will run for years. Raw data grows linearly with time — options snapshots alone can produce 100k+ rows/day. Without downsampling, queries over 30d+ windows become unusably slow and charts become unreadable noise.

### Strategy: Time-Resolution Tiers

Store and serve data at decreasing resolution as the requested range grows:

**Phase 1 (current):**

| Range | Resolution | Source |
|-------|-----------|--------|
| 1h, 6h | Raw (~10min ticks) | Live tables |
| 24h, 3d, 6.2d, 7d, 30d | 1-hour aggregates | `*_hourly` rollup tables |

**Phase 2 (when data exceeds 6+ months):**

| Range | Resolution | Source |
|-------|-----------|--------|
| 90d+ | 4-hour aggregates | Rolled up from hourly |
| 1y+ | Daily aggregates | Rolled up from 4-hour |

### Implementation Approach

1. **Rollup tables** — `spot_prices_hourly`, `options_hourly`, and `onchain_hourly` store pre-aggregated OHLC/avg/count per hour bucket. Phase 2 adds `_4h` and `_daily`.

2. **Rollup on write** — When the bot inserts new data, it also upserts into the current hourly bucket atomically.

3. **API serves the right tier** — The `/api/chart` endpoint picks the tier based on the requested range:
   - `1h`, `6h` → raw tables
   - `24h`, `3d`, `6.2d`, `7d`, `30d`, `all` → hourly rollup tables
   - Heatmap scatter data is only served for raw-tier ranges (individual instrument data is lost at hourly resolution).

4. **Client stays dumb** — The frontend receives pre-bucketed data and renders it directly. No client-side downsampling for large ranges.

5. **Retain raw data** — Never delete raw rows. They're needed for correlation engine accuracy and historical audits. Just don't serve them for large time ranges.

6. **Backfill** — Run `node bot/backfill-hourly.js` once to populate rollup tables from existing raw data. The script is idempotent (safe to re-run).

### What Gets Aggregated

For each hourly bucket, store:

- **Spot prices**: open, high, low, close, avg, momentum mode (most frequent direction)
- **Options snapshots**: best put/call delta-value, avg spread, avg depth, avg IV, total OI
- **Onchain data**: avg magnitude, dominant flow direction, total volume delta per DEX
- **Liquidity**: last TVL per DEX (step-after semantics), sum of volume deltas

### Chart Rendering at Scale

- At hourly resolution, 30 days = 720 points — fast to query and render.
- Volume bars should aggregate (sum deltas within bucket), not average.
- Heatmap scatter plots (put/call market, market quality) are only shown for raw-tier ranges (1h, 6h). At hourly+ resolution, individual instrument data is aggregated away.
- Momentum bars at hourly resolution show the last recorded direction for that hour.

### Migration Path

Rollup tables can be backfilled from existing raw data via `node bot/backfill-hourly.js`. The schema change is additive (new tables, no changes to existing ones), so it's safe to deploy incrementally.
