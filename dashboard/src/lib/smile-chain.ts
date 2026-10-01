import { buildExpiry, type SmileExpiry } from './vol-smile';

const API = 'https://api.lyra.finance/public';
const CHAIN_TTL_MS = 60_000;

async function post<T>(method: string, body: object): Promise<T> {
  const res = await fetch(`${API}/${method}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'User-Agent': 'noop-dashboard/1.0' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(15_000),
    cache: 'no-store',
  });
  if (!res.ok) throw new Error(`${method} HTTP ${res.status}`);
  const json = await res.json();
  if (json.error) throw new Error(`${method}: ${json.error.message}`);
  return json.result as T;
}

type Instrument = { instrument_name: string; option_details: { expiry: number } };

async function loadChain() {
  const instruments = await post<Instrument[]>('get_instruments', { currency: 'ETH', expired: false, instrument_type: 'option' });
  const expiries = new Map<string, number>();
  for (const i of instruments) expiries.set(i.instrument_name.split('-')[1], i.option_details.expiry);
  const chains = await Promise.all(Array.from(expiries, async ([date, expiry]) => {
    try {
      const r = await post<{ tickers: Record<string, never> }>('get_tickers', { instrument_type: 'option', currency: 'ETH', expiry_date: date });
      return buildExpiry(expiry, r?.tickers && !Array.isArray(r.tickers) ? r.tickers : {});
    } catch { return null; } // one bad expiry shouldn't blank the chart
  }));
  return chains.filter((c): c is SmileExpiry => c != null).sort((a, b) => a.expiry - b.expiry);
}

// ponytail: module-level cache, fine for one dashboard instance
let cached: { at: number; expiries: SmileExpiry[] } | null = null;
let inFlight: Promise<SmileExpiry[]> | null = null;

export function getCachedChain() {
  return cached;
}

export async function getChain() {
  if (cached && Date.now() - cached.at < CHAIN_TTL_MS) return cached;
  inFlight ??= loadChain().finally(() => { inFlight = null; });
  const expiries = await inFlight;
  if (expiries.length) cached = { at: Date.now(), expiries };
  return cached ?? { at: Date.now(), expiries };
}
