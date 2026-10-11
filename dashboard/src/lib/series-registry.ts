import * as marketHistoryDb from './db';
import { readSmileHistory, smileChangeSeries } from '../../../bot/market-evidence';
import { alignHourly, SMILE_DEFS } from '../../../bot/correlation';
import {
  getSpotPricesHourly,
  getOnchainHourly,
  getBestPutDvHourly,
  getBestCallDvHourly,
  getOptionsSpreadHourly,
  getOptionsDepthHourly,
  getOpenInterestHourly,
  getImpliedVolHourly,
  getFundingRates,
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
          value: prev > 0 && Date.parse(rows[i].hour) - Date.parse(rows[i - 1].hour) === 3600000 ? ((curr - prev) / prev) * 100 : null,
        });
      }
      return returns;
    },
  },
  {
    name: 'liquidity_flow',
    description: 'Validated selected-pool signed USD TVL change; prices and swaps also affect TVL',
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
  {
    name: 'options_spread',
    description: 'Avg bid-ask spread as % of mark price (higher = worse liquidity)',
    category: 'options',
    extract: (since) => getOptionsSpreadHourly(since),
  },
  {
    name: 'options_depth',
    description: 'Avg order book depth (ask_amount + bid_amount) in ETH',
    category: 'options',
    extract: (since) => getOptionsDepthHourly(since),
  },
  {
    name: 'open_interest',
    description: 'Latest OI per observed instrument in completed hours; coverage can change',
    category: 'options',
    extract: (since) => getOpenInterestHourly(since),
  },
  {
    name: 'implied_vol',
    description: 'Avg implied volatility within trading delta range',
    category: 'options',
    extract: (since) => getImpliedVolHourly(since),
  },
  {
    name: 'funding_rate',
    description: 'Derive ETH-PERP hourly funding rate (positive=longs pay, negative=shorts pay)',
    category: 'macro',
    extract: (since) => {
      const rows = getFundingRates(since);
      // Aggregate to hourly
      const hourMap = new Map<string, { sum: number; count: number }>();
      for (const r of rows) {
        const hour = r.timestamp.slice(0, 13) + ':00:00Z';
        const entry = hourMap.get(hour) || { sum: 0, count: 0 };
        entry.sum += r.rate * 100; // percentage
        entry.count++;
        hourMap.set(hour, entry);
      }
      return Array.from(hourMap.entries())
        .map(([hour, { sum, count }]) => ({ hour, value: sum / count }))
        .sort((a, b) => a.hour.localeCompare(b.hour));
    },
  },
];

export function getRegisteredSeries(): SeriesConfig[] {
  return [...SERIES, ...SMILE_DEFS.map((s: { name: string; description: string }) => ({ ...s, category: 'options' as const, extract: () => [] }))];
}

export function registerSeries(config: SeriesConfig): void {
  SERIES.push(config);
}

export interface AlignedSeries {
  hours: string[];
  series: Record<string, (number | null)[]>;
}

export function extractAllSeries(days: number): AlignedSeries {
  const nowMs = Date.now(), sinceMs = nowMs - days * 86400000;
  const since = new Date(sinceMs).toISOString();
  const raw: Record<string, { hour: string; value: number | null }[]> = marketHistoryDb.getCorrelationHistory({ since:new Date(Math.floor(sinceMs/3600000)*3600000).toISOString(), before:new Date(Math.floor(nowMs/3600000)*3600000).toISOString() });
  for (const s of SERIES) if (!(s.name in raw)) raw[s.name]=s.extract(since);
  Object.assign(raw, smileChangeSeries(readSmileHistory(marketHistoryDb,nowMs,90)));
  return alignHourly(raw,sinceMs,nowMs);
}
