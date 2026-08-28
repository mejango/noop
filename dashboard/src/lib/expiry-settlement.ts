// Synthesizes the exchange's expiry settlement as cashflow rows so P&L sees it.
// Net short call expiring ITM = forced buyback at intrinsic; net long put ITM = cash received.
// ponytail: settlement price = last spot at/before 08:00 UTC expiry, not Derive's official
// settlement index. Upgrade path: read settlement_price from Lyra get_instrument.

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
  success: 1;
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

const OPEN_ACTIONS = new Set(['sell_call', 'buy_put']);
const CLOSE_ACTIONS = new Set(['buyback_call', 'sell_put']);

export function parseInstrument(name: string | null) {
  const parts = String(name || '').split('-');
  if (parts.length !== 4 || !/^\d{8}$/.test(parts[1])) return null;
  const strike = Number(parts[2]);
  const expiryMs = Date.parse(`${parts[1].slice(0, 4)}-${parts[1].slice(4, 6)}-${parts[1].slice(6, 8)}T08:00:00Z`);
  if (!Number.isFinite(strike) || !Number.isFinite(expiryMs)) return null;
  return { strike, expiryMs, optionType: parts[3] as 'C' | 'P' | string };
}

export function spotAtOrBefore(rows: Array<{ timestamp: string; price: number }>, ms: number) {
  let best: number | null = null;
  let bestMs = -Infinity;
  for (const row of rows) {
    const rowMs = Date.parse(row.timestamp);
    if (rowMs <= ms && rowMs > bestMs && Number.isFinite(Number(row.price))) { best = Number(row.price); bestMs = rowMs; }
  }
  return best;
}

export function deriveExpirySettlements(
  orders: SettlementInput[],
  spotRows: Array<{ timestamp: string; price: number }>,
  now = Date.now()
): SettlementOrder[] {
  const net = new Map<string, number>();
  for (const o of orders) {
    if (o.success !== 1 || !o.instrument_name) continue;
    const qty = Math.abs(Number(o.filled_amount ?? 0));
    if (!(qty > 0)) continue;
    const parsed = parseInstrument(o.instrument_name);
    if (!parsed || Date.parse(o.timestamp) > parsed.expiryMs) continue; // orders after expiry belong to nothing
    if (OPEN_ACTIONS.has(o.action)) net.set(o.instrument_name, (net.get(o.instrument_name) ?? 0) + qty);
    else if (CLOSE_ACTIONS.has(o.action)) net.set(o.instrument_name, (net.get(o.instrument_name) ?? 0) - qty);
  }

  const out: SettlementOrder[] = [];
  for (const [name, exposure] of Array.from(net.entries())) {
    if (!(exposure > 1e-9)) continue;
    const parsed = parseInstrument(name)!;
    if (parsed.expiryMs > now) continue;
    const spot = spotAtOrBefore(spotRows, parsed.expiryMs);
    if (spot == null) continue;
    const intrinsic = parsed.optionType === 'C' ? Math.max(0, spot - parsed.strike)
      : parsed.optionType === 'P' ? Math.max(0, parsed.strike - spot) : 0;
    if (!(intrinsic > 0)) continue; // expired worthless: nothing to settle
    const ts = new Date(parsed.expiryMs).toISOString();
    out.push({
      id: `settle:${name}:${ts}`,
      timestamp: ts,
      action: parsed.optionType === 'C' ? 'settle_call' : 'settle_put',
      success: 1,
      reason: parsed.optionType === 'C' ? 'Forced buyback at expiry (ITM settlement)' : 'ITM put settled at expiry',
      instrument_name: name,
      strike: parsed.strike,
      expiry: Math.floor(parsed.expiryMs / 1000),
      delta: null,
      price: intrinsic,
      intended_amount: exposure,
      filled_amount: exposure,
      fill_price: intrinsic,
      total_value: intrinsic * exposure,
      spot_price: spot,
    });
  }
  return out.sort((a, b) => a.timestamp.localeCompare(b.timestamp));
}
