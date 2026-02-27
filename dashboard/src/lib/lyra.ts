import { privateKeyToAccount } from 'viem/accounts';
import fs from 'fs';
import path from 'path';

const DERIVE_WALLET = '0xD87890df93bf74173b51077e5c6cD12121d87903';
const SUBACCOUNT_ID = 25923;
const BASE_URL = 'https://api.lyra.finance';
const CACHE_TTL = 30_000; // 30s

// ─── Auth ────────────────────────────────────────────────────────────────────

function loadPrivateKey(): `0x${string}` {
  if (process.env.PRIVATE_KEY) {
    const key = process.env.PRIVATE_KEY.trim();
    return (key.startsWith('0x') ? key : `0x${key}`) as `0x${string}`;
  }
  try {
    const keyPath = path.join(process.cwd(), '..', '.private_key.txt');
    const key = fs.readFileSync(keyPath, 'utf8').trim();
    return (key.startsWith('0x') ? key : `0x${key}`) as `0x${string}`;
  } catch {
    throw new Error('No private key found (set PRIVATE_KEY env or create ../.private_key.txt)');
  }
}

async function getAuthHeaders(): Promise<Record<string, string>> {
  const account = privateKeyToAccount(loadPrivateKey());
  const timestamp = Date.now();
  const signature = await account.signMessage({ message: timestamp.toString() });
  return {
    'X-LyraWallet': DERIVE_WALLET,
    'X-LyraTimestamp': timestamp.toString(),
    'X-LyraSignature': signature,
    'Content-Type': 'application/json',
    'User-Agent': 'noop-dashboard/1.0',
  };
}

// ─── Cache ───────────────────────────────────────────────────────────────────

const cache = new Map<string, { data: unknown; ts: number }>();

function getCached<T>(key: string): T | null {
  const entry = cache.get(key);
  if (entry && Date.now() - entry.ts < CACHE_TTL) return entry.data as T;
  return null;
}

function setCache(key: string, data: unknown) {
  cache.set(key, { data, ts: Date.now() });
}

// ─── API calls ───────────────────────────────────────────────────────────────

async function lyraPost<T>(endpoint: string, body: Record<string, unknown>): Promise<T> {
  const headers = await getAuthHeaders();
  const res = await fetch(`${BASE_URL}${endpoint}`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Lyra API ${endpoint} ${res.status}: ${text}`);
  }
  const json = await res.json();
  return json.result ?? json;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function getPositions(): Promise<any[]> {
  const cached = getCached<unknown[]>('positions');
  if (cached) return cached;

  const result = await lyraPost<{ positions: unknown[] }>('/private/get_positions', {
    subaccount_id: SUBACCOUNT_ID,
  });
  const positions = result.positions ?? result;
  setCache('positions', positions);
  return positions as unknown[];
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function getCollaterals(): Promise<any[]> {
  const cached = getCached<unknown[]>('collaterals');
  if (cached) return cached;

  const result = await lyraPost<{ collaterals: unknown[] }>('/private/get_collaterals', {
    subaccount_id: SUBACCOUNT_ID,
  });
  const collaterals = result.collaterals ?? result;
  setCache('collaterals', collaterals);
  return collaterals as unknown[];
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function getTradeHistory(fromMs: number, toMs?: number): Promise<any[]> {
  const cacheKey = `trades_${fromMs}_${toMs ?? 'now'}`;
  const cached = getCached<unknown[]>(cacheKey);
  if (cached) return cached;

  const body: Record<string, unknown> = {
    subaccount_id: SUBACCOUNT_ID,
    from_timestamp: fromMs,
    page_size: 100,
  };
  if (toMs) body.to_timestamp = toMs;

  const result = await lyraPost<{ trades: unknown[] }>('/private/get_trade_history', body);
  const trades = result.trades ?? result;
  setCache(cacheKey, trades);
  return trades as unknown[];
}
