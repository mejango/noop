import {
  getStats,
  getBotBudget,
  getBestScores,
  getRecentTicks,
  getOnchainData,
  getSignals,
  getJournalEntries,
  getOptionsDistribution,
  getAvgCallPremium7d,
  getLatestOnchainRawData,
  getMarketQualitySummary,
} from './db';
import { buildCorrelationAnalysis } from './correlation';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let _cache: { data: any; ts: number } | null = null;
const TTL = 60_000;

export function buildMarketSnapshot() {
  if (_cache && Date.now() - _cache.ts < TTL) return _cache.data;
  const result = _buildUncached();
  _cache = { data: result, ts: Date.now() };
  return result;
}

function _buildUncached() {
  const now = new Date();
  const since24h = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  const since7d = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  const since30d = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();

  const stats = (getStats() as Record<string, unknown>) || {};
  const budget = getBotBudget();
  const bestScores = getBestScores();
  const onchain = getOnchainData(since24h) as Record<string, unknown>[];
  const latestRawRow = getLatestOnchainRawData(since24h);
  const signals = getSignals(since7d, 30) as Record<string, unknown>[];

  // bot_ticks may not exist yet — handle gracefully
  let ticks: Record<string, unknown>[] = [];
  try {
    ticks = getRecentTicks(5) as Record<string, unknown>[];
  } catch { /* table may not exist */ }

  // Parse latest tick summary if available
  let latestTickParsed: Record<string, unknown> | null = null;
  if (ticks.length > 0 && ticks[0].summary) {
    try {
      latestTickParsed = JSON.parse(ticks[0].summary as string);
    } catch { /* ignore malformed JSON */ }
  }

  return {
    _meta: {
      _description: 'Snapshot metadata. generated_at is ISO-8601 UTC.',
      generated_at: now.toISOString(),
    },

    price: {
      _description: 'Latest ETH spot price and range data. price is USD. Highs/lows are rolling windows.',
      current: stats.last_price ?? null,
      last_updated: stats.last_price_time ?? null,
      three_day_high: stats.three_day_high ?? null,
      three_day_low: stats.three_day_low ?? null,
      seven_day_high: stats.seven_day_high ?? null,
      seven_day_low: stats.seven_day_low ?? null,
    },

    momentum: {
      _description: 'Momentum indicators. main is "upward"|"downward"|"neutral". derivative indicates acceleration (+) or deceleration (-).',
      short: {
        main: stats.short_momentum ?? null,
        derivative: stats.short_derivative ?? null,
      },
      medium: {
        main: stats.medium_momentum ?? null,
        derivative: stats.medium_derivative ?? null,
      },
    },

    budget: {
      _description: 'Bot trading budget for the current cycle. All values in USD. cycleDays is the full cycle length (10d). daysLeft is time remaining. spent is cumulative this cycle.',
      put: {
        total: budget.putTotalBudget,
        spent: budget.putSpent,
        remaining: budget.putRemaining,
        days_left: budget.putDaysLeft,
      },
      call: {
        total: budget.callTotalBudget,
        spent: budget.callSpent,
        remaining: budget.callRemaining,
        days_left: budget.callDaysLeft,
      },
      cycle_days: budget.cycleDays,
    },

    options_market: {
      _description: 'Best options scores over the measurement window. Higher delta-value = better risk/reward. distribution shows put/call aggregate stats from last 24h. avg_call_premium_7d is rolling 7d average call bid price. market_quality shows spread, IV, and depth for instruments within the bot delta range (0.02-0.12 abs delta).',
      best_put_score: bestScores.bestPutScore,
      best_call_score: bestScores.bestCallScore,
      window_days: bestScores.windowDays,
      best_put_detail: bestScores.bestPutDetail,
      best_call_detail: bestScores.bestCallDetail,
      distribution: (() => {
        try { return getOptionsDistribution(since24h); } catch { return []; }
      })(),
      avg_call_premium_7d: (() => {
        try {
          const rows = getAvgCallPremium7d();
          return rows.length > 0 ? rows[0].avg_premium : null;
        } catch { return null; }
      })(),
      market_quality: (() => {
        try {
          const rows = getMarketQualitySummary(since24h);
          if (!rows.length) return null;
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const byType: Record<string, any> = {};
          for (const r of rows) {
            const key = (r.option_type === 'P' ? 'put' : 'call');
            byType[key] = {
              instruments_in_range: r.count,
              avg_spread_pct: r.avg_spread != null ? +(r.avg_spread * 100).toFixed(2) : null,
              min_spread_pct: r.min_spread != null ? +(r.min_spread * 100).toFixed(2) : null,
              max_spread_pct: r.max_spread != null ? +(r.max_spread * 100).toFixed(2) : null,
              avg_implied_vol_pct: r.avg_iv != null ? +(r.avg_iv * 100).toFixed(1) : null,
              avg_depth: r.avg_depth != null ? +r.avg_depth.toFixed(2) : null,
              total_depth: r.total_depth != null ? +r.total_depth.toFixed(2) : null,
            };
          }
          return byType;
        } catch { return null; }
      })(),
    },

    onchain_metrics: {
      _description: 'On-chain data from last 24h. liquidity_flow_direction is "inflow"|"outflow"|"neutral", magnitude 0-1, confidence 0-1. pool_breakdown shows per-DEX liquidity from latest raw_data.',
      data_points: onchain.length,
      latest: onchain.length > 0 ? onchain[0] : null,
      history: onchain,
      pool_breakdown: (() => {
        try {
          if (!latestRawRow?.raw_data) return null;
          const raw = JSON.parse(latestRawRow.raw_data);
          const dexes = raw?.dexLiquidity?.dexes;
          if (!dexes) return null;
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          return Object.entries(dexes).map(([name, dex]: [string, any]) => {
            const pools = dex.poolDetails ?? dex.pools ?? [];
            const topPool = pools[0] ?? null;
            return {
              dex: name,
              total_liquidity: dex.totalLiquidity ?? null,
              total_volume_usd: dex.totalVolume ?? null,
              total_tx_count: dex.totalTxCount ?? null,
              pool_count: pools.length || null,
              top_pool: topPool ? {
                pair: topPool.token0?.symbol && topPool.token1?.symbol
                  ? `${topPool.token0.symbol}/${topPool.token1.symbol}`
                  : topPool.pair || topPool.name || null,
                liquidity: topPool.liquidity ?? topPool.liquidityUSD ?? topPool.totalLiquidity ?? null,
                volume_usd: topPool.volumeUSD ?? null,
                fee_tier_bps: topPool.feeTier ?? null,
                active_liquidity: topPool.activeLiquidity ?? null,
              } : null,
            };
          });
        } catch { return null; }
      })(),
    },

    strategy_signals: {
      _description: 'Strategy signals from last 7 days. signal_type describes the signal (e.g. "momentum_shift"). details is a JSON string with signal-specific data. acted_on=1 means the bot traded on this signal.',
      count: signals.length,
      signals: signals,
    },

    bot_ticks: {
      _description: 'Recent bot tick summaries (last 5). Each tick is a periodic evaluation cycle. summary contains JSON with the bot decision details.',
      count: ticks.length,
      latest_parsed: latestTickParsed,
      ticks: ticks,
    },

    cross_correlations: (() => {
      try {
        return buildCorrelationAnalysis();
      } catch {
        return { pairs: [], leading_indicators: [], series_descriptions: {}, computed_at: now.toISOString() };
      }
    })(),

    ai_journal: {
      _description: 'AI analytical journal. Persistent observations and hypotheses from past conversations.',
      recent_entries: getJournalEntries(since30d, 20),
    },
  };
}
