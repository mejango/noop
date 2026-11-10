export type CombinedPortfolioPoint = {
  timestamp: string;
  ts: number;
  portfolioValue: number | null;
  fullPortfolioValue: number | null;
  sourceValue: number | null;
  destinationValue: number | null;
  kind: string;
  reason: string | null;
};

export type CombinedAccountsMeta = {
  configured: boolean;
  sourceAccountId: string | number | null;
  destinationAccountId: string | number | null;
  firstTimestamp: string | null;
  lastTimestamp: string | null;
  latestStatus: string;
  reason: string | null;
  historyStatus: string;
};

export type CombinedPortfolioSummary = {
  openingValue: number | null;
  closingValue: number | null;
  portfolioChange: number | null;
  highWatermark: number | null;
  lowWatermark: number | null;
};

export type LatestCombinedPortfolio = {
  timestamp: string;
  combinedValue: number | null;
  sourceValue: number | null;
  destinationValue: number | null;
  reason: string | null;
  accounts: Array<{
    subaccountId: string | number;
    label: string;
    valueUsd: number | null;
    collaterals: Array<{ asset: string; amount: string | number; markPrice: number | null; markValue: number | null }>;
  }>;
};

// Keep this sparse series separate from source-only observations. Explicit nulls
// are evidence gaps, not zero balances or permission to reuse an earlier value.
export function buildCombinedPortfolioChart(points: CombinedPortfolioPoint[] = []) {
  return points.filter(point => Number.isFinite(point.ts)).map(point => ({
    ...point,
    combinedPortfolioValueUsd: Number.isFinite(point.portfolioValue) ? point.portfolioValue : null,
    combinedFullPortfolioValueUsd: Number.isFinite(point.portfolioValue) && Number.isFinite(point.fullPortfolioValue)
      ? point.fullPortfolioValue : null,
  })).sort((a, b) => a.ts - b.ts);
}

export function combinedPortfolioBefore(points: ReturnType<typeof buildCombinedPortfolioChart>, before: number) {
  let low = 0;
  let high = points.length;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    if (points[middle].ts < before) low = middle + 1;
    else high = middle;
  }
  const last = points[low - 1];
  return {
    combinedPortfolioValueUsd: last?.combinedPortfolioValueUsd ?? null,
    combinedFullPortfolioValueUsd: last?.combinedFullPortfolioValueUsd ?? null,
  };
}

export function combinedHistoryLabel(meta?: CombinedAccountsMeta) {
  if (!meta?.configured) return null;
  if (!meta.firstTimestamp) return 'Combined account history is not available yet.';
  const start = new Date(meta.firstTimestamp).toLocaleString('en-US', {
    month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit', timeZone: 'UTC',
  });
  return `Combined account history starts ${start} UTC. Earlier lines cover NOOP only.`;
}

export function combinedAvailabilityMessage(meta?: CombinedAccountsMeta) {
  if (!meta?.configured || meta.latestStatus === 'complete') return null;
  if (meta.latestStatus === 'stale') return 'Latest account data is out of date.';
  if (meta.reason?.toLowerCase().includes('transfer')) return 'An internal transfer is awaiting confirmation.';
  return 'One account observation is unavailable.';
}

export function collateralAmounts(collaterals: LatestCombinedPortfolio['accounts'][number]['collaterals']) {
  return collaterals.filter(row => Number.isFinite(Number(row.amount)) && Number(row.amount) !== 0)
    .map(row => `${Number(row.amount).toLocaleString('en-US', { maximumFractionDigits: 8 })} ${row.asset}`).join(', ');
}
