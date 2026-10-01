import type { SmileExpiry } from './vol-smile';

export const TENORS = [7, 14, 30, 60, 90];
export const OFFSETS = [-0.1, -0.05, 0, 0.05, 0.1];
const MIN_DAYS = 7;
const DAY = 86_400_000;

export type PricingFrame = { at: number; expiries: SmileExpiry[] };
export type PricingCell = {
  dte: number; offset: number; strike: number | null; iv: number | null;
  percentile: number | null; samples: number;
};
export type VolatilityPricingData = {
  asOf: string; score: number | null; label: string; qualifier: string;
  historyDays: number; historyFrom: string | null; historyTo: string | null;
  provisional: boolean; measured: number; total: number; spot: number | null;
  cells: PricingCell[];
};

const median = (values: number[]) => {
  const s = [...values].sort((a, b) => a - b);
  const i = Math.floor(s.length / 2);
  return s.length % 2 ? s[i] : (s[i - 1] + s[i]) / 2;
};

// Quote-backed mark IV, sampled at fixed strike/spot distance. Never extrapolate
// beyond quoted strikes or bridge missing quotes across the smile.
export function ivAtOffset(e: SmileExpiry, offset: number): number | null {
  if (!(e.spot && e.spot > 0)) return null;
  const strike = e.spot * (1 + offset);
  const points = e.points.filter(p => Number.isFinite(p.strike) && p.strike > 0)
    .sort((a, b) => a.strike - b.strike);
  const valid = (p: typeof points[number]) => Number.isFinite(p.iv) && p.iv > 0
    && p.bidIv != null && p.askIv != null && p.bidIv > 0 && p.askIv >= p.bidIv
    && (p.askIv - p.bidIv) / p.iv <= 0.5;
  for (let i = 0; i < points.length; i++) {
    const p = points[i];
    if (p.strike === strike) return valid(p) ? p.iv : null;
    const lo = points[i - 1];
    if (lo && lo.strike < strike && p.strike > strike) {
      if (!valid(lo) || !valid(p)) return null;
      const w = (strike - lo.strike) / (p.strike - lo.strike);
      return lo.iv + w * (p.iv - lo.iv);
    }
  }
  return null;
}

// Constant maturity via total-variance interpolation between listed expiries.
// Matching these horizons in history avoids expiry-roll and ageing artifacts.
export function ivAtTenor(expiries: SmileExpiry[], dte: number, offset: number): number | null {
  const sorted = expiries.filter(e => e.dte > 0).sort((a, b) => a.dte - b.dte);
  for (let i = 0; i < sorted.length; i++) {
    const hi = sorted[i], lo = sorted[i - 1];
    if (Math.abs(hi.dte - dte) < 1e-6) return ivAtOffset(hi, offset);
    if (lo && lo.dte < dte && hi.dte > dte) {
      const a = ivAtOffset(lo, offset), b = ivAtOffset(hi, offset);
      if (a == null || b == null) return null;
      const w = (dte - lo.dte) / (hi.dte - lo.dte);
      return Math.sqrt(((1 - w) * a * a * lo.dte + w * b * b * hi.dte) / dte);
    }
  }
  return null;
}

export function buildVolatilityPricing(current: PricingFrame, history: PricingFrame[]): VolatilityPricingData {
  // One observation per completed UTC day: dense recording days get no extra weight.
  const daily = new Map<string, PricingFrame>();
  const today = new Date(current.at).toISOString().slice(0, 10);
  for (const f of [...history].sort((a, b) => a.at - b.at)) {
    if (!Number.isFinite(f.at) || f.at >= current.at || f.at < current.at - 365 * DAY) continue;
    const day = new Date(f.at).toISOString().slice(0, 10);
    if (day !== today) daily.set(day, f);
  }
  const frames = Array.from(daily.values());
  const spot = current.expiries.find(e => e.spot != null && e.spot > 0)?.spot ?? null;
  const cells = OFFSETS.flatMap(offset => TENORS.map(dte => {
    const iv = ivAtTenor(current.expiries, dte, offset);
    const past = frames.map(f => ivAtTenor(f.expiries, dte, offset)).filter((v): v is number => v != null);
    // Midrank ties: an unchanged market reads 50, not expensive.
    const percentile = iv != null && past.length >= MIN_DAYS
      ? 100 * past.reduce((n, v) => n + (v < iv ? 1 : v === iv ? 0.5 : 0), 0) / past.length : null;
    return { dte, offset, strike: spot == null ? null : spot * (1 + offset), iv, percentile, samples: past.length };
  }));
  const measured = cells.filter(c => c.percentile != null);
  // A broad label needs most of the grid, spanning both strikes and horizons.
  const broad = measured.length >= 13 && new Set(measured.map(c => c.dte)).size >= 3
    && new Set(measured.map(c => c.offset)).size >= 3;
  const score = broad ? Math.round(median(measured.map(c => c.percentile!))) : null;
  const label = score == null ? 'Insufficient history / coverage' : score <= 25 ? 'Cheap' : score >= 75 ? 'Expensive' : 'Typical';
  const cheap = measured.filter(c => c.percentile! <= 25).length;
  const expensive = measured.filter(c => c.percentile! >= 75).length;
  const qualifier = score == null ? 'Need 7 daily observations and broad quoted coverage'
    : cheap >= measured.length * 0.7 ? 'Cheap broadly'
    : expensive >= measured.length * 0.7 ? 'Expensive broadly'
    : cheap > 0 && expensive > 0 ? 'Mixed across strikes and maturities' : 'Broadly similar pricing';
  return {
    asOf: new Date(current.at).toISOString(), score, label, qualifier, spot, cells,
    historyDays: frames.length, historyFrom: frames.length ? new Date(frames[0].at).toISOString() : null,
    historyTo: frames.length ? new Date(frames[frames.length - 1].at).toISOString() : null,
    provisional: measured.some(c => c.samples < 30), measured: measured.length, total: cells.length,
  };
}
