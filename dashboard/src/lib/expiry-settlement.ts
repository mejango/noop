// Spot-based expiry cashflows are estimates, never exchange settlement records.
// They are displayed separately from recorded activity until official settlement data exists.

export const SETTLEMENT_SPOT_MAX_AGE_MS = 15 * 60 * 1000;

export type SettlementInput = {
  timestamp: string;
  action: string;
  success: number;
  instrument_name: string | null;
  filled_amount: number | null;
};

export type SettlementOrder = {
  id: string;
  timestamp: string;
  action: 'settle_call' | 'settle_put';
  success: null;
  estimated: true;
  source: 'spot_estimate';
  reason: string;
  instrument_name: string;
  strike: number;
  expiry: number;
  delta: null;
  price: number;
  intended_amount: number;
  filled_amount: number;
  fill_price: number;
  total_value: number;
  spot_price: number;
};

export type MissingSettlementEstimate = {
  instrument_name: string;
  timestamp: string;
  amount: number;
  reason: string;
};

type SpotRow = { timestamp: string; price: number };

const OPEN_ACTIONS = new Set(['sell_call', 'buy_put']);
const CLOSE_ACTIONS = new Set(['buyback_call', 'sell_put']);

export function parseInstrument(name: string | null) {
  const parts = String(name || '').split('-');
  if (parts.length !== 4 || !/^\d{8}$/.test(parts[1]) || !['C', 'P'].includes(parts[3])) return null;
  const strike = Number(parts[2]);
  const expiryMs = Date.parse(`${parts[1].slice(0, 4)}-${parts[1].slice(4, 6)}-${parts[1].slice(6, 8)}T08:00:00Z`);
  if (!(strike > 0) || !Number.isFinite(strike) || !Number.isFinite(expiryMs)) return null;
  return { strike, expiryMs, optionType: parts[3] as 'C' | 'P' };
}

export function spotAtOrBefore(rows: SpotRow[], ms: number, maxAgeMs = SETTLEMENT_SPOT_MAX_AGE_MS) {
  let best: number | null = null;
  let bestMs = -Infinity;
  for (const row of rows) {
    const rowMs = Date.parse(row.timestamp);
    const price = Number(row.price);
    if (rowMs <= ms && rowMs >= ms - maxAgeMs && rowMs > bestMs && Number.isFinite(price) && price > 0) {
      best = price;
      bestMs = rowMs;
    }
  }
  return best;
}

export function getExpiredExposures(orders: SettlementInput[], now = Date.now()) {
  const net = new Map<string, number>();
  for (const o of orders) {
    if (o.success !== 1 || !o.instrument_name) continue;
    const qty = Math.abs(Number(o.filled_amount ?? 0));
    if (!(qty > 0) || !Number.isFinite(qty)) continue;
    const parsed = parseInstrument(o.instrument_name);
    const orderMs = Date.parse(o.timestamp);
    if (!parsed || !Number.isFinite(orderMs) || orderMs > parsed.expiryMs || orderMs > now) continue;
    if (OPEN_ACTIONS.has(o.action)) net.set(o.instrument_name, (net.get(o.instrument_name) ?? 0) + qty);
    else if (CLOSE_ACTIONS.has(o.action)) net.set(o.instrument_name, (net.get(o.instrument_name) ?? 0) - qty);
  }
  return Array.from(net.entries()).flatMap(([instrument_name, amount]) => {
    const parsed = parseInstrument(instrument_name)!;
    return amount > 1e-9 && parsed.expiryMs <= now ? [{ instrument_name, amount, ...parsed }] : [];
  });
}

export function deriveExpirySettlementReport(
  orders: SettlementInput[],
  spotRows: SpotRow[],
  now = Date.now(),
  settledInstruments: ReadonlySet<string> = new Set()
): { estimates: SettlementOrder[]; missing: MissingSettlementEstimate[] } {
  const estimates: SettlementOrder[] = [];
  const missing: MissingSettlementEstimate[] = [];
  for (const exposure of getExpiredExposures(orders, now)) {
    const { instrument_name: name, amount, strike, expiryMs, optionType } = exposure;
    if (settledInstruments.has(name)) continue;
    const ts = new Date(expiryMs).toISOString();
    const spot = spotAtOrBefore(spotRows, expiryMs);
    if (spot == null) {
      missing.push({
        instrument_name: name,
        timestamp: ts,
        amount,
        reason: 'No spot observation within 15 minutes before expiry; settlement value is unknown.',
      });
      continue;
    }
    const intrinsic = optionType === 'C' ? Math.max(0, spot - strike) : Math.max(0, strike - spot);
    // Keep zero estimates too: estimated worthless expiry still needs an official settlement record.
    estimates.push({
      id: `estimate:${name}:${ts}`,
      timestamp: ts,
      action: optionType === 'C' ? 'settle_call' : 'settle_put',
      success: null,
      estimated: true,
      source: 'spot_estimate',
      reason: 'Estimated expiry cashflow from nearby spot; official exchange settlement unavailable.',
      instrument_name: name,
      strike,
      expiry: Math.floor(expiryMs / 1000),
      delta: null,
      price: intrinsic,
      intended_amount: amount,
      filled_amount: amount,
      fill_price: intrinsic,
      total_value: intrinsic * amount,
      spot_price: spot,
    });
  }
  return {
    estimates: estimates.sort((a, b) => a.timestamp.localeCompare(b.timestamp)),
    missing: missing.sort((a, b) => a.timestamp.localeCompare(b.timestamp)),
  };
}

// Compatibility helper for callers interested only in nonzero estimated cashflows.
export function deriveExpirySettlements(orders: SettlementInput[], spotRows: SpotRow[], now = Date.now()): SettlementOrder[] {
  return deriveExpirySettlementReport(orders, spotRows, now).estimates.filter(row => row.total_value > 0);
}
