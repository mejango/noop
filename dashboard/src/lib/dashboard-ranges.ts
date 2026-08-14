const HOUR_MS = 60 * 60 * 1000;
const DAY_MS = 24 * HOUR_MS;

export const DASHBOARD_RANGES = [
  '1h',
  '6h',
  '24h',
  '3d',
  '6.2d',
  '7d',
  '14d',
  '30d',
  '90d',
  '365d',
] as const;

export type DashboardRange = (typeof DASHBOARD_RANGES)[number];

export const DASHBOARD_RANGE_MS = {
  '1h': HOUR_MS,
  '6h': 6 * HOUR_MS,
  '24h': DAY_MS,
  '3d': 3 * DAY_MS,
  '6.2d': 6.2 * DAY_MS,
  '7d': 7 * DAY_MS,
  '14d': 14 * DAY_MS,
  '30d': 30 * DAY_MS,
  '90d': 90 * DAY_MS,
  '365d': 365 * DAY_MS,
} satisfies Record<DashboardRange, number>;

const RANGE_ALIASES: Record<string, DashboardRange> = {
  '1y': '365d',
  all: '365d',
};

export function dashboardRangeMs(range: string, fallback: DashboardRange): number {
  const normalized = RANGE_ALIASES[range] || range;
  return DASHBOARD_RANGE_MS[normalized as DashboardRange] ?? DASHBOARD_RANGE_MS[fallback];
}
