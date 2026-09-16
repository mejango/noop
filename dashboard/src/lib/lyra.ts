import { privateKeyToAccount } from 'viem/accounts';
import fs from 'fs';
import path from 'path';
import { loadKey } from '../../../integrations/derive-v3/profile';
import { VENUE, IS_V3 } from './venue';
const DERIVE_WALLET = VENUE.version === 3 ? VENUE.ownerAddress : '0xD87890df93bf74173b51077e5c6cD12121d87903';
const SUBACCOUNT_ID = VENUE.version === 3 ? VENUE.subaccountId : 25923;
const BASE_URL = VENUE.version === 3 ? VENUE.httpUrl : 'https://api.lyra.finance';
const CACHE_TTL = 30_000; // 30s
const REQUEST_TIMEOUT_MS = 15_000;
let cachedPrivateKey: `0x${string}` | null = null;

// ─── Auth ────────────────────────────────────────────────────────────────────

function loadPrivateKey(): `0x${string}` {
  if (cachedPrivateKey) return cachedPrivateKey;
  if (IS_V3) {
    cachedPrivateKey = loadKey(VENUE) as `0x${string}`;
    return cachedPrivateKey;
  }
  if (process.env.PRIVATE_KEY) {
    const key = process.env.PRIVATE_KEY.trim();
    cachedPrivateKey = (key.startsWith('0x') ? key : `0x${key}`) as `0x${string}`;
    return cachedPrivateKey;
  }
  try {
    const keyPath = path.join(process.cwd(), '..', '.private_key.txt');
    const key = fs.readFileSync(keyPath, 'utf8').trim();
    cachedPrivateKey = (key.startsWith('0x') ? key : `0x${key}`) as `0x${string}`;
    return cachedPrivateKey;
  } catch {
    throw new Error('No private key found (set PRIVATE_KEY env or create ../.private_key.txt)');
  }
}

async function getAuthHeaders(): Promise<Record<string, string>> {
  const account = privateKeyToAccount(loadPrivateKey());
  const timestamp = Date.now();
  const signature = await account.signMessage({ message: timestamp.toString() });
  return {
    [`X-${IS_V3 ? 'Derive' : 'Lyra'}Wallet`]: DERIVE_WALLET || '',
    [`X-${IS_V3 ? 'Derive' : 'Lyra'}Timestamp`]: timestamp.toString(),
    [`X-${IS_V3 ? 'Derive' : 'Lyra'}Signature`]: signature,
    'Content-Type': 'application/json',
    'User-Agent': 'noop-dashboard/1.0',
  };
}

// ─── Cache ───────────────────────────────────────────────────────────────────

const cache = new Map<string, { data: unknown; ts: number }>();
const inFlight = new Map<string, Promise<unknown>>();

function getCached<T>(key: string, ttlMs = CACHE_TTL): T | null {
  const entry = cache.get(key);
  if (entry && Date.now() - entry.ts < ttlMs) return entry.data as T;
  return null;
}

function setCache(key: string, data: unknown) {
  cache.set(key, { data, ts: Date.now() });
}

async function cachedRequest<T>(key: string, loader: () => Promise<T>, ttlMs = CACHE_TTL): Promise<T> {
  const cached = getCached<T>(key, ttlMs);
  if (cached != null) return cached;
  const pending = inFlight.get(key);
  if (pending) return pending as Promise<T>;
  const request = loader()
    .then((data) => {
      setCache(key, data);
      return data;
    })
    .finally(() => inFlight.delete(key));
  inFlight.set(key, request);
  return request;
}

// ─── API calls ───────────────────────────────────────────────────────────────

function validateV3AccountResult(endpoint: string, value: unknown): void {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Invalid V3 account response');
  const result = value as Record<string, unknown>;
  if (result.failed_to_fetch || result.subaccount_id !== SUBACCOUNT_ID) throw new Error('V3 account identity or availability mismatch');
  const numeric = (row: Record<string, unknown>, fields: string[]) => {
    for (const field of fields) {
      const value = row[field];
      if ((typeof value !== 'string' && typeof value !== 'number') || String(value).trim() === '' || !Number.isFinite(Number(value))) {
        throw new Error(`Invalid V3 account ${field}`);
      }
    }
  };
  const rows = (field: string, name: string, fields: string[]) => {
    if (!Array.isArray(result[field])) throw new Error(`Invalid V3 account ${field}`);
    for (const value of result[field] as unknown[]) {
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`Invalid V3 account ${field} row`);
      const row = value as Record<string, unknown>;
      if (typeof row[name] !== 'string' || !row[name]) throw new Error(`Invalid V3 account ${name}`);
      numeric(row, fields);
    }
  };
  if (endpoint === '/private/get_positions' || endpoint === '/private/get_subaccount') {
    rows('positions', 'instrument_name', ['amount', 'average_price', 'mark_price', 'mark_value', 'unrealized_pnl', 'index_price', 'delta', 'gamma', 'theta', 'vega', 'initial_margin']);
  }
  if (endpoint === '/private/get_collaterals' || endpoint === '/private/get_subaccount') {
    rows('collaterals', 'asset_name', ['amount', 'mark_price', 'mark_value', 'unrealized_pnl', 'maintenance_margin']);
  }
  if (endpoint === '/private/get_subaccount') {
    if (!Array.isArray(result.open_orders) || typeof result.is_under_liquidation !== 'boolean' || result.failed_to_fetch !== false) {
      throw new Error('Invalid V3 portfolio availability');
    }
    numeric(result, ['initial_margin', 'maintenance_margin', 'subaccount_value', 'collaterals_value', 'collaterals_initial_margin', 'collaterals_maintenance_margin', 'positions_initial_margin', 'open_orders_margin']);
  }
}

async function lyraPost<T>(endpoint: string, body: Record<string, unknown>): Promise<T> {
  if (IS_V3 && (!DERIVE_WALLET || !SUBACCOUNT_ID)) throw new Error('V3 dashboard requires its dedicated owner and subaccount');
  const headers = await getAuthHeaders();
  const res = await fetch(`${BASE_URL}${endpoint}`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    redirect: 'error',
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Lyra API ${endpoint} ${res.status}: ${text}`);
  }
  const json = await res.json();
  const result = json.result ?? json;
  if (IS_V3) {
    if (json.error) throw new Error('V3 account data unavailable');
    validateV3AccountResult(endpoint, result);
  }
  return result;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function getPositions(): Promise<any[]> {
  return cachedRequest('positions', async () => {
    const result = await lyraPost<{ positions: unknown[] }>('/private/get_positions', {
      subaccount_id: SUBACCOUNT_ID,
    });
    return (result.positions ?? result) as unknown[];
  });
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function getCollaterals(): Promise<any[]> {
  return cachedRequest('collaterals', async () => {
    const result = await lyraPost<{ collaterals: unknown[] }>('/private/get_collaterals', {
      subaccount_id: SUBACCOUNT_ID,
    });
    return (result.collaterals ?? result) as unknown[];
  });
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function getTradeHistory(fromMs: number, toMs?: number): Promise<any[]> {
  const hourMs = 60 * 60 * 1000;
  const cacheKey = `trades_${Math.floor(fromMs / hourMs)}_${toMs ? Math.floor(toMs / hourMs) : 'now'}`;
  return cachedRequest(cacheKey, async () => {
    const body: Record<string, unknown> = {
      subaccount_id: SUBACCOUNT_ID,
      from_timestamp: fromMs,
      page_size: 100,
    };
    if (toMs) body.to_timestamp = toMs;
    if (!IS_V3) {
      const result = await lyraPost<{ trades: unknown[] }>('/private/get_trade_history', body);
      return (result.trades ?? result) as unknown[];
    }
    const trades: unknown[] = [];
    let previous = '';
    for (let page = 1; page <= 100; page++) {
      const result = await lyraPost<{ trades: unknown[]; pagination?: { num_pages: number; count?: number } }>('/private/get_trade_history', { ...body, page });
      if (!Array.isArray(result.trades)) throw new Error('Invalid V3 trade history');
      if (result.pagination && (!Number.isInteger(result.pagination.num_pages) || result.pagination.num_pages < 0 || (result.pagination.num_pages === 0 && result.trades.length > 0))) {
        throw new Error('Invalid V3 history pagination');
      }
      if (result.pagination && result.trades.length === 0 && page < result.pagination.num_pages) throw new Error('V3 history contains an empty page before completion');
      const fingerprint = JSON.stringify(result.trades);
      if (result.trades.length && fingerprint === previous) throw new Error('V3 history pagination did not advance');
      previous = fingerprint;
      trades.push(...result.trades);
      if (result.pagination ? page >= result.pagination.num_pages : result.trades.length < 100) return trades;
    }
    throw new Error('V3 history incomplete: pagination limit exceeded');
  }, 5 * 60_000);
}

export async function getSubaccount(): Promise<{
  initial_margin: number;
  maintenance_margin: number;
  subaccount_value: number;
  collaterals_value: number;
  collaterals_initial_margin: number;
  collaterals_maintenance_margin: number;
  aggregated_collaterals_maintenance_margin: number;
  positions_initial_margin: number;
  aggregated_positions_initial_margin: number;
  open_orders_margin: number;
  margin_usage_pct: number | null;
}> {
  return cachedRequest<{
    initial_margin: number;
    maintenance_margin: number;
    subaccount_value: number;
    collaterals_value: number;
    collaterals_initial_margin: number;
    collaterals_maintenance_margin: number;
    positions_initial_margin: number;
    aggregated_collaterals_maintenance_margin: number;
    aggregated_positions_initial_margin: number;
    open_orders_margin: number;
    margin_usage_pct: number | null;
  }>('subaccount', async () => {
    const result = await lyraPost<Record<string, unknown>>('/private/get_subaccount', {
      subaccount_id: SUBACCOUNT_ID,
    });
    const collateralRows = Array.isArray(result?.collaterals) ? result.collaterals as Record<string, unknown>[] : [];
    const positionRows = Array.isArray(result?.positions) ? result.positions as Record<string, unknown>[] : [];
    const collateralsInitialMargin = Number(result?.collaterals_initial_margin ?? 0);
    const collateralsMaintenanceMargin = Math.abs(Number(result?.collaterals_maintenance_margin ?? 0));
    const initialMargin = Number(result?.initial_margin ?? 0);
    const maintenanceMargin = Number(result?.maintenance_margin ?? 0);
    const positionsInitialMargin = Math.abs(Number(result?.positions_initial_margin ?? 0));
    const aggregatedCollateralsMaintenanceMargin = collateralRows.reduce((sum, row) => (
      sum + Math.abs(Number(row?.maintenance_margin ?? 0))
    ), 0);
    const aggregatedPositionsInitialMargin = positionRows.reduce((sum, row) => (
      sum + Math.abs(Number(row?.initial_margin ?? 0))
    ), 0);
    const openOrdersMargin = Math.abs(Number(result?.open_orders_margin ?? 0));
    const explicitUsage = Number(
      result?.margin_usage_pct ??
      result?.margin_utilization_pct ??
      result?.margin_utilization ??
      NaN
    );
    const maintenanceBase = aggregatedCollateralsMaintenanceMargin || collateralsMaintenanceMargin;
    const positionsBase = aggregatedPositionsInitialMargin || positionsInitialMargin;
    const marginUsagePct = maintenanceBase > 0 && Number.isFinite(maintenanceMargin)
      ? +((1 - (maintenanceMargin / maintenanceBase)) * 100).toFixed(1)
      : maintenanceBase > 0
        ? +(((positionsBase + openOrdersMargin) / maintenanceBase) * 100).toFixed(1)
      : collateralsInitialMargin > 0
        ? +((1 - initialMargin / collateralsInitialMargin) * 100).toFixed(1)
      : Number.isFinite(explicitUsage)
        ? +(explicitUsage > 1 ? explicitUsage : explicitUsage * 100).toFixed(1)
        : null;

    return {
      initial_margin: initialMargin,
      maintenance_margin: maintenanceMargin,
      subaccount_value: Number(result?.subaccount_value ?? 0),
      collaterals_value: Number(result?.collaterals_value ?? 0),
      collaterals_initial_margin: collateralsInitialMargin,
      collaterals_maintenance_margin: collateralsMaintenanceMargin,
      aggregated_collaterals_maintenance_margin: aggregatedCollateralsMaintenanceMargin,
      positions_initial_margin: positionsInitialMargin,
      aggregated_positions_initial_margin: aggregatedPositionsInitialMargin,
      open_orders_margin: openOrdersMargin,
      margin_usage_pct: marginUsagePct,
    };
  });
}
