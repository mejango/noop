import {
  getSpotPricesHourly,
  getOnchainHourly,
  getBestPutDvHourly,
  getBestCallDvHourly,
} from './db';

export interface SeriesConfig {
  name: string;
  description: string;
  category: 'price' | 'liquidity' | 'options' | 'macro' | 'custom';
  extract: (sinceISO: string) => { hour: string; value: number | null }[];
}

const SERIES: SeriesConfig[] = [
  {
    name: 'spot_return',
    description: 'ETH hourly % price change',
    category: 'price',
    extract: (since) => {
      const rows = getSpotPricesHourly(since);
      const returns: { hour: string; value: number | null }[] = [];
      for (let i = 1; i < rows.length; i++) {
        const prev = rows[i - 1].avg_price;
        const curr = rows[i].avg_price;
        returns.push({
          hour: rows[i].hour,
          value: prev > 0 ? ((curr - prev) / prev) * 100 : null,
        });
      }
      return returns;
    },
  },
  {
    name: 'liquidity_flow',
    description: 'DEX liquidity signed flow (+ inflow, - outflow)',
    category: 'liquidity',
    extract: (since) => {
      const rows = getOnchainHourly(since);
      return rows.map((r) => ({
        hour: r.hour,
        value:
          r.avg_magnitude != null && r.direction != null
            ? r.direction === 'outflow'
              ? -r.avg_magnitude
              : r.avg_magnitude
            : null,
      }));
    },
  },
  {
    name: 'exhaustion_score',
    description: 'Market exhaustion score (0=fresh, 1=fully exhausted)',
    category: 'liquidity',
    extract: (since) => {
      const rows = getOnchainHourly(since);
      return rows.map((r) => ({
        hour: r.hour,
        value: r.avg_exhaustion,
      }));
    },
  },
  {
    name: 'best_put_dv',
    description: 'Best PUT delta-value score (higher = cheaper protection)',
    category: 'options',
    extract: (since) => getBestPutDvHourly(since),
  },
  {
    name: 'best_call_dv',
    description: 'Best CALL delta-value score (higher = richer premium to sell)',
    category: 'options',
    extract: (since) => getBestCallDvHourly(since),
  },
];

export function getRegisteredSeries(): SeriesConfig[] {
  return SERIES;
}

export function registerSeries(config: SeriesConfig): void {
  SERIES.push(config);
}

export interface AlignedSeries {
  hours: string[];
  series: Record<string, (number | null)[]>;
}

export function extractAllSeries(days: number): AlignedSeries {
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

  // Extract all series
  const raw: Record<string, Map<string, number | null>> = {};
  const allHours = new Set<string>();

  for (const s of SERIES) {
    const data = s.extract(since);
    const map = new Map<string, number | null>();
    for (const d of data) {
      map.set(d.hour, d.value);
      allHours.add(d.hour);
    }
    raw[s.name] = map;
  }

  // Align on sorted hourly grid
  const hours = Array.from(allHours).sort();
  const series: Record<string, (number | null)[]> = {};

  for (const s of SERIES) {
    const map = raw[s.name];
    series[s.name] = hours.map((h) => map.get(h) ?? null);
  }

  return { hours, series };
}
