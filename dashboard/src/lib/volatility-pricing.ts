import type { SmileExpiry } from './vol-smile';

export const TENORS = [1, 3, 7, 14, 30, 45, 60, 90, 180];
export const OFFSETS = [-0.2, -0.15, -0.1, -0.05, 0, 0.05, 0.1, 0.15, 0.2];
export const MIN_HISTORY_SAMPLES = 24;
export const HISTORY_DAYS = 30;
const HOUR = 3_600_000;
const SUMMARY_TENORS = [7, 14, 30, 60, 90];
const DAY = 86_400_000;

export type PricingFrame = { at: number; expiries: SmileExpiry[] };
export type VolatilityInstrument = {
  name: string; type: 'P' | 'C'; strike: number; expiry: number; dte: number;
  ask: number; askAmount: number; askIv: number;
  bid: number | null; bidAmount: number | null; bidIv: number | null;
};
export type PricingHistoryPoint = { at: string; from?: string; iv: number | null; percentile: number | null };
export const pricingColor = (percentile: number | null) => percentile == null
  ? '#252525' : `hsl(165 55% ${12 + (100 - percentile) * 0.25}%)`;

export type PricingQuote = {
  iv: number | null;
  percentile: number | null; samples: number;
  history: PricingHistoryPoint[];
};
export type PricingCell = PricingQuote & {
  dte: number; offset: number; strike: number | null;
  bid: PricingQuote; ask: PricingQuote; instruments: VolatilityInstrument[];
};
export type VolatilityPricingData = {
  asOf: string; score: number | null; label: string; qualifier: string;
  historyDays: number; historySamples: number; historyFrom: string | null; historyTo: string | null;
  provisional: boolean; measured: number; total: number; spot: number | null; currentIv: number | null;
  cells: PricingCell[];
};

const median = (values: number[]) => {
  const s = [...values].sort((a, b) => a - b);
  const i = Math.floor(s.length / 2);
  return s.length % 2 ? s[i] : (s[i - 1] + s[i]) / 2;
};

export type IvSide = 'mark' | 'bid' | 'ask';

// Quote-backed IV, sampled at fixed strike/spot distance. Never extrapolate
// beyond quoted strikes or bridge missing quotes across the smile.
export function ivAtOffset(e: SmileExpiry, offset: number, side: IvSide = 'mark'): number | null {
  if (!(e.spot && e.spot > 0)) return null;
  const strike = e.spot * (1 + offset);
  const points = e.points.filter(p => Number.isFinite(p.strike) && p.strike > 0)
    .sort((a, b) => a.strike - b.strike);
  const value = (p: typeof points[number]) => side === 'bid' ? p.bidIv : side === 'ask' ? p.askIv : p.iv;
  const valid = (p: typeof points[number]) => {
    const iv = value(p);
    if (iv == null || !Number.isFinite(iv) || iv <= 0) return false;
    const twoSided = p.bidIv != null && p.askIv != null && p.bidIv > 0 && p.askIv > 0;
    if (side === 'mark' && !twoSided) return false;
    return !twoSided || (p.askIv! >= p.bidIv! && (p.askIv! - p.bidIv!) / p.iv <= 0.5);
  };
  for (let i = 0; i < points.length; i++) {
    const p = points[i];
    if (p.strike === strike) return valid(p) ? value(p) : null;
    const lo = points[i - 1];
    if (lo && lo.strike < strike && p.strike > strike) {
      if (!valid(lo) || !valid(p)) return null;
      const w = (strike - lo.strike) / (p.strike - lo.strike);
      return value(lo)! + w * (value(p)! - value(lo)!);
    }
  }
  return null;
}

// Constant maturity via total-variance interpolation between listed expiries.
// Matching these horizons in history avoids expiry-roll and ageing artifacts.
export function ivAtTenor(expiries: SmileExpiry[], dte: number, offset: number, side: IvSide = 'mark'): number | null {
  const sorted = expiries.filter(e => e.dte > 0).sort((a, b) => a.dte - b.dte);
  for (let i = 0; i < sorted.length; i++) {
    const hi = sorted[i], lo = sorted[i - 1];
    if (Math.abs(hi.dte - dte) < 1e-6) return ivAtOffset(hi, offset, side);
    if (lo && lo.dte < dte && hi.dte > dte) {
      const a = ivAtOffset(lo, offset, side), b = ivAtOffset(hi, offset, side);
      if (a == null || b == null) return null;
      const w = (dte - lo.dte) / (hi.dte - lo.dte);
      return Math.sqrt(((1 - w) * a * a * lo.dte + w * b * b * hi.dte) / dte);
    }
  }
  return null;
}

// Closest listed maturity, then strike. Only quotes with an actual ask and size
// can be shown as buyable. ATM offers both sides; wings keep their option type.
export function nearestVolatilityInstruments(expiries: SmileExpiry[], dte: number, strike: number, offset: number): VolatilityInstrument[] {
  const types: ('P' | 'C')[] = offset === 0 ? ['P', 'C'] : offset < 0 ? ['P'] : ['C'];
  return types.flatMap(type => {
    const candidates = expiries.filter(e => e.dte > 0).flatMap(e => e.points
      .filter(p => p.type === type && p.name && Number.isFinite(p.askPrice) && p.askPrice! > 0
        && Number.isFinite(p.askAmount) && p.askAmount! > 0 && p.askIv != null && p.askIv > 0)
      .map(p => ({ name: p.name, type, strike: p.strike, expiry: e.expiry, dte: e.dte,
        ask: p.askPrice!, askAmount: p.askAmount!, askIv: p.askIv!,
        bid: p.bidPrice != null && p.bidPrice > 0 && p.bidAmount != null && p.bidAmount > 0 ? p.bidPrice : null,
        bidAmount: p.bidAmount ?? null, bidIv: p.bidIv })));
    candidates.sort((a, b) => Math.abs(a.dte - dte) - Math.abs(b.dte - dte)
      || Math.abs(a.strike - strike) - Math.abs(b.strike - strike) || a.name.localeCompare(b.name));
    return candidates.length ? [candidates[0]] : [];
  });
}

// Keep the last snapshot in each completed UTC hour. No extra weight for a
// busy recorder, no partial current-hour observation mixed into history.
export function sampleHourly<T>(items: T[], at: (item: T) => number, now: number): T[] {
  const hours = new Map<number, T>();
  for (const item of items) {
    const t = at(item), hour = Math.floor(t / HOUR);
    if (!Number.isFinite(t) || t < now - HISTORY_DAYS * DAY || hour >= Math.floor(now / HOUR)) continue;
    const prior = hours.get(hour);
    if (!prior || at(prior) < t) hours.set(hour, item);
  }
  return Array.from(hours.values()).sort((a, b) => at(a) - at(b));
}

// Bound the rendered strip while all hourly samples still determine the score.
// Missing hours stay gaps; the current point stays exact and separate.
function colorPath(points: PricingHistoryPoint[]): PricingHistoryPoint[] {
  const past = points.slice(0, -1), size = Math.max(1, Math.ceil(past.length / 48));
  const out: PricingHistoryPoint[] = [];
  for (let i = 0; i < past.length; i += size) {
    const chunk = past.slice(i, i + size);
    const quoted = chunk.every(p => p.iv != null);
    const rated = chunk.every(p => p.percentile != null);
    out.push({ at: chunk[chunk.length - 1].at, from: chunk[0].at,
      iv: quoted ? chunk.reduce((sum, p) => sum + p.iv!, 0) / chunk.length : null,
      percentile: rated ? chunk.reduce((sum, p) => sum + p.percentile!, 0) / chunk.length : null });
  }
  return [...out, points[points.length - 1]];
}

export function buildVolatilityPricing(current: PricingFrame, history: PricingFrame[]): VolatilityPricingData {
  const frames = sampleHourly(history, f => f.at, current.at);
  const byHour = new Map(frames.map(f => [Math.floor(f.at / HOUR), f]));
  const hours: number[] = [];
  if (frames.length) for (let h = Math.floor(frames[0].at / HOUR); h < Math.floor(current.at / HOUR); h++) hours.push(h);
  const spot = current.expiries.find(e => e.spot != null && e.spot > 0)?.spot ?? null;
  const cells = OFFSETS.flatMap(offset => TENORS.map(dte => {
    const assess = (side: IvSide): PricingQuote => {
      const iv = ivAtTenor(current.expiries, dte, offset, side);
      const observed = hours.map(h => {
        const f = byHour.get(h);
        return { at: new Date(f?.at ?? h * HOUR).toISOString(), iv: f ? ivAtTenor(f.expiries, dte, offset, side) : null };
      });
      const past = observed.map(p => p.iv).filter((v): v is number => v != null).sort((a, b) => a - b);
      // One fixed baseline for the whole color strip and today's cell.
      // Midrank ties: an unchanged market reads 50, not expensive.
      const rank = (value: number | null) => {
        if (value == null || past.length < MIN_HISTORY_SAMPLES) return null;
        const bound = (upper: boolean) => {
          let lo = 0, hi = past.length;
          while (lo < hi) {
            const mid = (lo + hi) >>> 1;
            if (past[mid] < value || (upper && past[mid] === value)) lo = mid + 1;
            else hi = mid;
          }
          return lo;
        };
        return 50 * (bound(false) + bound(true)) / past.length;
      };
      const percentile = rank(iv);
      const history = colorPath([...observed, { at: new Date(current.at).toISOString(), iv }]
        .map(p => ({ ...p, percentile: rank(p.iv) })));
      return { iv, percentile, samples: past.length, history };
    };
    const mark = assess('mark'), bid = assess('bid'), ask = assess('ask');
    const strike = spot == null ? null : spot * (1 + offset);
    return { dte, offset, strike, ...mark, bid, ask,
      instruments: strike == null ? [] : nearestVolatilityInstruments(current.expiries, dte, strike, offset),
    };
  }));
  const measured = cells.filter(c => c.percentile != null);
  // Keep the headline comparable as the exploration grid expands.
  const summary = measured.filter(c => SUMMARY_TENORS.includes(c.dte) && Math.abs(c.offset) <= 0.1);
  const broad = summary.length >= 13 && new Set(summary.map(c => c.dte)).size >= 3
    && new Set(summary.map(c => c.offset)).size >= 3;
  const score = broad ? Math.round(median(summary.map(c => c.percentile!))) : null;
  const label = score == null ? 'Insufficient history / coverage' : score <= 25 ? 'Cheap' : score >= 75 ? 'Expensive' : 'Typical';
  const cheap = summary.filter(c => c.percentile! <= 25).length;
  const expensive = summary.filter(c => c.percentile! >= 75).length;
  const qualifier = score == null ? `Need ${MIN_HISTORY_SAMPLES} hourly observations and broad quoted coverage`
    : cheap >= summary.length * 0.7 ? 'Cheap broadly'
    : expensive >= summary.length * 0.7 ? 'Expensive broadly'
    : cheap > 0 && expensive > 0 ? 'Mixed across strikes and maturities' : 'Broadly similar pricing';
  return {
    asOf: new Date(current.at).toISOString(), score, label, qualifier, spot, cells,
    currentIv: cells.some(c => c.iv != null) ? median(cells.filter(c => c.iv != null).map(c => c.iv!)) : null,
    historyDays: frames.length ? Math.ceil((current.at - frames[0].at) / DAY) : 0, historySamples: frames.length, historyFrom: frames.length ? new Date(frames[0].at).toISOString() : null,
    historyTo: frames.length ? new Date(frames[frames.length - 1].at).toISOString() : null,
    provisional: frames.length < 7 * 24, measured: measured.length, total: cells.length,
  };
}
