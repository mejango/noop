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
  getOnchainWithRawData,
} from './db';
import { buildCorrelationAnalysis } from './correlation';

export function buildMarketSnapshot() {
  const now = new Date();
  const since24h = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  const since7d = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  const since30d = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();

  const stats = (getStats() as Record<string, unknown>) || {};
  const budget = getBotBudget();
  const bestScores = getBestScores();
  const onchain = getOnchainData(since24h) as Record<string, unknown>[];
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
      _description: 'Best options scores over the measurement window. Higher delta-value = better risk/reward. distribution shows put/call aggregate stats from last 24h. avg_call_premium_7d is rolling 7d average call bid price.',
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
    },

    onchain_metrics: {
      _description: 'On-chain data from last 24h. liquidity_flow_direction is "inflow"|"outflow"|"neutral", magnitude 0-1, confidence 0-1. exhaustion_score 0-1 (1=fully exhausted). exhaustion_alert_level is "low"|"medium"|"high". pool_breakdown shows per-DEX liquidity from latest raw_data.',
      data_points: onchain.length,
      latest: onchain.length > 0 ? onchain[0] : null,
      history: onchain,
      pool_breakdown: (() => {
        try {
          const rows = getOnchainWithRawData(since24h, 1);
          if (!rows.length || !rows[0].raw_data) return null;
          const raw = JSON.parse(rows[0].raw_data);
          const dexes = raw?.dexLiquidity?.dexes;
          if (!dexes) return null;
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          return Object.entries(dexes).map(([name, dex]: [string, any]) => ({
            dex: name,
            total_liquidity: dex.totalLiquidity ?? null,
            pool_count: dex.pools?.length ?? null,
            top_pool: dex.pools?.[0] ? {
              pair: dex.pools[0].pair || dex.pools[0].name || null,
              liquidity: dex.pools[0].liquidity ?? dex.pools[0].totalLiquidity ?? null,
            } : null,
          }));
        } catch { return null; }
      })(),
    },

    strategy_signals: {
      _description: 'Strategy signals from last 7 days. signal_type describes the signal (e.g. "momentum_shift", "exhaustion_alert"). details is a JSON string with signal-specific data. acted_on=1 means the bot traded on this signal.',
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
