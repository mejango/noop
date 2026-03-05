# Market Sentiment Panel

## Goal
Add a market sentiment section to the dashboard and AI advisor that surfaces funding rates, options skew, aggregate open interest, and volume/TVL ratio — signals analogous to what perps traders use to gauge leveraged positioning.

## 1. New Data: ETH Funding Rate

- **Source**: Binance public API `GET /fapi/v1/fundingRate?symbol=ETHUSDT&limit=100`
- **No auth required**
- **Collection**: Every bot tick in `script.js`, fetch latest funding rate and store
- **Table**: `funding_rates` (`timestamp TEXT, exchange TEXT, symbol TEXT, rate REAL`)
- **Hourly rollup**: `funding_rates_hourly` (`hour TEXT, exchange TEXT, symbol TEXT, avg_rate REAL, count INTEGER`)
- Binance publishes funding every 8h; we poll each tick and deduplicate by timestamp

## 2. Derived Signals (from existing data)

Computed on-the-fly via new DB queries:

| Signal | Computation | Source Table |
|--------|-------------|--------------|
| Options Skew | `avg(put IV) - avg(call IV)` for instruments in bot delta range | `options_snapshots` |
| Aggregate OI | `SUM(open_interest)` grouped by timestamp | `options_snapshots` |
| Volume/TVL Ratio | `volume / tvl` per DEX from raw_data JSON | `onchain_data` |

## 3. Dashboard Chart

New "Market Sentiment" `<Card>` below existing charts:

- **Funding Rate line** — center-zeroed, shows Binance ETH funding rate over time
- **Options Skew line** — center-zeroed, put IV minus call IV
- **Aggregate OI area** — secondary Y axis, total open interest
- **Spot price** — faded reference line for context

Uses existing `recharts` `<ComposedChart>`, existing range picker, existing downsampling.

## 4. AI Advisor Integration

Add `market_sentiment` section to `buildMarketSnapshot()`:

```json
{
  "market_sentiment": {
    "_description": "Market sentiment indicators. funding_rate from Binance perps (positive=longs pay shorts, negative=shorts pay longs). options_skew = avg put IV minus avg call IV (positive=fear/downside demand). aggregate_oi = total options open interest. volume_tvl_ratio = DEX trading activity relative to liquidity.",
    "funding_rate": {
      "current": -0.0143,
      "avg_24h": -0.005,
      "trend": "declining"
    },
    "options_skew": {
      "current": 5.2,
      "avg_24h": 3.1,
      "direction": "widening"
    },
    "aggregate_oi": {
      "current": 15234,
      "change_24h_pct": 12.5
    },
    "volume_tvl_ratio": {
      "current": 0.15,
      "avg_7d": 0.08
    }
  }
}
```

Update system prompt with interpretation guidance:
- Negative funding during rally = shorts paying longs = bullish (not leverage-driven)
- Positive funding spike = overleveraged longs = fragile rally
- Skew widening = market buying downside protection = fear
- OI rising + price rising = new money entering (conviction)
- OI rising + price flat = positioning buildup (potential breakout)
- High volume/TVL = active repositioning

## 5. Files to Change

| File | Change |
|------|--------|
| `script.js` | Add `fetchFundingRate()`, create `funding_rates` table, insert each tick |
| `dashboard/src/lib/db.ts` | Add `getFundingRates()`, `getOptionsSkew()`, `getAggregateOI()`, `getVolumeTvlRatio()` |
| `dashboard/src/app/api/chart/route.ts` | Include sentiment series in response |
| `dashboard/src/lib/snapshot.ts` | Add `market_sentiment` section |
| `dashboard/src/app/api/ai/chat/route.ts` | Update system prompt with sentiment interpretation |
| `dashboard/src/app/page.tsx` | Add Market Sentiment chart card |
