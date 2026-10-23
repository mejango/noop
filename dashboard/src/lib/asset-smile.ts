import { CHART_MIN_ABS_DELTA, SMILE_MIN_ABS_DELTA, type CompactSmileRow, type SmileExpiry, type SmilePoint } from './vol-smile';
import type { PricingFrame } from './volatility-pricing';
import type { AssetQuote, AssetQuoteFrame } from './asset-overview';

const positive = (n: number | null) => n != null && Number.isFinite(n) && n > 0;
const median = (values: number[]) => {
  const sorted = values.sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
};

// Match the ETH smile's OTM and IV rules, using only this asset's saved quotes.
export function assetSmileExpiries(frame: AssetQuoteFrame): SmileExpiry[] {
  const at = Date.parse(frame.hour);
  if (!Number.isFinite(at)) return [];
  const groups = new Map<number, AssetQuote[]>();
  for (const q of frame.quotes) {
    if (!positive(q.expiry) || q.expiry! * 1000 <= at) continue;
    const group = groups.get(q.expiry!) ?? [];
    group.push(q);
    groups.set(q.expiry!, group);
  }
  return Array.from(groups, ([expiry, quotes]) => {
    const forwards = quotes.map(q => q.forward).filter((v): v is number => positive(v));
    const spots = quotes.map(q => q.index_price).filter((v): v is number => positive(v));
    const forward = forwards.length ? median(forwards) : spots.length ? median(spots) : frame.spot;
    if (!positive(forward)) return null;
    const points: SmilePoint[] = quotes.flatMap(q => {
      const type: 'P' | 'C' | null = q.option_type?.toUpperCase().startsWith('P') ? 'P' : q.option_type?.toUpperCase().startsWith('C') ? 'C' : null;
      if (!type || !positive(q.strike) || !positive(q.implied_vol) || q.delta == null || !Number.isFinite(q.delta)
        || Math.abs(q.delta) < SMILE_MIN_ABS_DELTA || (type === 'P') !== (q.strike! < forward!)) return [];
      return [{
        name: q.instrument_name, strike: q.strike!, type, delta: q.delta, iv: q.implied_vol! * 100,
        bidIv: positive(q.bid_iv) ? q.bid_iv! * 100 : null,
        askIv: positive(q.ask_iv) ? q.ask_iv! * 100 : null,
        oi: q.open_interest ?? 0,
        bidPrice: q.bid, askPrice: q.ask, bidAmount: q.bid_amount, askAmount: q.ask_amount,
      }];
    }).sort((a, b) => a.strike - b.strike);
    if (points.length < 4) return null;
    return { expiry, dte: (expiry * 1000 - at) / 86_400_000, forward: forward!, spot: positive(frame.spot) ? frame.spot : spots.length ? median(spots) : null, points };
  }).filter((e): e is SmileExpiry => e != null).sort((a, b) => a.expiry - b.expiry);
}

export function assetPricingFrame(frame: AssetQuoteFrame): PricingFrame {
  return { at: Date.parse(frame.hour), expiries: assetSmileExpiries(frame) };
}

export function compactAssetExpiry(expiry: SmileExpiry): CompactSmileRow {
  return {
    expiry: expiry.expiry, forward: expiry.forward, spot: expiry.spot,
    points: expiry.points.filter(p => Math.abs(p.delta) >= CHART_MIN_ABS_DELTA)
      .map(p => [p.strike, p.type === 'C' ? 1 : 0, p.delta, p.iv / 100,
        p.bidIv == null ? null : p.bidIv / 100, p.askIv == null ? null : p.askIv / 100, p.oi]),
  };
}
