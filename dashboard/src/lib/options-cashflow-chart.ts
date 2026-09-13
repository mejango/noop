export type CashflowBucket = {
  timestamp: string;
  tradeCashflow: number;
  tradeRevenue?: number;
  tradeExpenses?: number;
  putRevenue?: number;
  putExpenses?: number;
  callRevenue?: number;
  callExpenses?: number;
  estimatedSettlementCashflow?: number;
  estimatedCallSettlementExpenses?: number;
  estimatedPutSettlementRevenue?: number;
  orderCount: number;
  endPortfolioValue: number | null;
};

type CashflowReport = {
  meta: { from: string; to: string };
  summary: {
    openingTradeRevenue: number;
    openingTradeExpenses: number;
    openingEstimatedSettlementCashflow?: number;
    openingValue: number;
    closingValue: number;
  };
  series: { buckets: CashflowBucket[] };
};

export function buildOptionsCashflowChart(report: CashflowReport) {
  const { summary } = report;
  let cumulativeRevenue = summary.openingTradeRevenue;
  let cumulativeExpenses = summary.openingTradeExpenses;
  let cumulativeEstimates = summary.openingEstimatedSettlementCashflow ?? 0;
  const estimatesAvailable = Number.isFinite(summary.openingEstimatedSettlementCashflow)
    && report.series.buckets.every(bucket => Number.isFinite(bucket.estimatedSettlementCashflow));
  let lastPortfolioValue = summary.openingValue;
  const emptyPeriod = {
    periodRevenue: 0, periodExpenses: 0, periodNet: 0,
    periodCallRevenue: 0, periodPutRevenue: 0, periodCallExpenses: 0, periodPutExpenses: 0,
    periodCallSettlements: 0, periodPutSettlements: 0, orderCount: 0,
  };
  const opening = {
    ...emptyPeriod,
    ts: Date.parse(report.meta.from), cumulativeRevenue, cumulativeExpenses,
    cumulativeCashflow: cumulativeRevenue - cumulativeExpenses,
    cumulativeSettlementAdjustedCashflow: estimatesAvailable ? cumulativeRevenue - cumulativeExpenses + cumulativeEstimates : null,
    portfolioValueUsd: lastPortfolioValue,
  };
  const rows = report.series.buckets.map(bucket => {
    const periodRevenue = bucket.tradeRevenue ?? Math.max(0, bucket.tradeCashflow);
    const periodExpenseAmount = bucket.tradeExpenses ?? Math.max(0, -bucket.tradeCashflow);
    cumulativeRevenue += periodRevenue;
    cumulativeExpenses += periodExpenseAmount;
    cumulativeEstimates += bucket.estimatedSettlementCashflow ?? 0;
    if (bucket.endPortfolioValue != null && Number.isFinite(bucket.endPortfolioValue)) {
      lastPortfolioValue = bucket.endPortfolioValue;
    }
    return {
      ts: Date.parse(bucket.timestamp), cumulativeRevenue, cumulativeExpenses,
      cumulativeCashflow: cumulativeRevenue - cumulativeExpenses,
      cumulativeSettlementAdjustedCashflow: estimatesAvailable ? cumulativeRevenue - cumulativeExpenses + cumulativeEstimates : null,
      periodRevenue, periodExpenses: -periodExpenseAmount, periodNet: bucket.tradeCashflow,
      periodCallRevenue: bucket.callRevenue ?? 0, periodPutRevenue: bucket.putRevenue ?? 0,
      periodCallExpenses: -(bucket.callExpenses ?? 0), periodPutExpenses: -(bucket.putExpenses ?? 0),
      periodCallSettlements: -(bucket.estimatedCallSettlementExpenses ?? 0),
      periodPutSettlements: bucket.estimatedPutSettlementRevenue ?? 0,
      orderCount: bucket.orderCount, portfolioValueUsd: lastPortfolioValue,
    };
  });
  if (Number.isFinite(opening.ts) && (!rows.length || rows[0].ts > opening.ts)) rows.unshift(opening);
  const to = Date.parse(report.meta.to);
  if (Number.isFinite(to) && (!rows.length || rows[rows.length - 1].ts < to)) {
    rows.push({ ...(rows[rows.length - 1] ?? opening), ...emptyPeriod, ts: to });
  }
  return rows;
}

export function cashflowDisplayDomain(report: CashflowReport): [number, number] {
  const from = Date.parse(report.meta.from);
  const to = Date.parse(report.meta.to);
  if (!Number.isFinite(from) || !Number.isFinite(to)) return [0, 1];
  const firstBucket = Date.parse(report.series.buckets[0]?.timestamp);
  const start = Number.isFinite(firstBucket) ? Math.max(from, firstBucket) : from;
  return [start, Math.max(start + 1, to)];
}
