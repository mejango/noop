import { privateKeyToAccount } from 'viem/accounts';
import fs from 'fs';
import path from 'path';

const DERIVE_WALLET = '0xD87890df93bf74173b51077e5c6cD12121d87903';
const SUBACCOUNT_ID = 25923;
const BASE_URL = 'https://api.lyra.finance';
const CACHE_TTL = 30_000; // 30s
const REQUEST_TIMEOUT_MS = 15_000;
let cachedPrivateKey: `0x${string}` | null = null;

// ─── Auth ────────────────────────────────────────────────────────────────────

function loadPrivateKey(): `0x${string}` {
  if (cachedPrivateKey) return cachedPrivateKey;
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
    'X-LyraWallet': DERIVE_WALLET,
    'X-LyraTimestamp': timestamp.toString(),
    'X-LyraSignature': signature,
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

function record(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`Account data unavailable: invalid ${label}`);
  return value as Record<string, unknown>;
}

function requireNumbers(row: Record<string, unknown>, fields: string[], label: string): void {
  for (const field of fields) {
    const value = row[field];
    if ((typeof value !== 'number' && typeof value !== 'string') || String(value).trim() === '' || !Number.isFinite(Number(value))) {
      throw new Error(`Account data unavailable: invalid ${label}.${field}`);
    }
  }
}

function accountRows(result: unknown, field: string, name: string, numbers: string[]): Record<string, unknown>[] {
  const rows = Array.isArray(result) ? result : record(result, field)[field];
  if (!Array.isArray(rows)) throw new Error(`Account data unavailable: missing ${field}`);
  return rows.map((value) => {
    const row = record(value, field);
    if (typeof row[name] !== 'string' || !row[name]) throw new Error(`Account data unavailable: missing ${name}`);
    requireNumbers(row, numbers, field);
    return row;
  });
}

async function lyraPost<T>(endpoint: string, body: Record<string, unknown>): Promise<T> {
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
  const json = record(await res.json(), 'API response');
  if (json.error) throw new Error(`Account data unavailable: ${endpoint} returned an API error`);
  const result = json.result ?? json;
  if (!Array.isArray(result)) {
    const row = record(result, 'API result');
    if (row.failed_to_fetch === true || row.error) throw new Error('Account data unavailable');
    if (row.subaccount_id != null && Number(row.subaccount_id) !== SUBACCOUNT_ID) throw new Error('Account data unavailable: subaccount mismatch');
  }
  return result as T;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function getPositions(): Promise<any[]> {
  return cachedRequest('positions', async () => {
    const result = await lyraPost<{ positions: unknown[] }>('/private/get_positions', {
      subaccount_id: SUBACCOUNT_ID,
    });
    return accountRows(result, 'positions', 'instrument_name', ['amount', 'average_price', 'mark_price', 'mark_value', 'unrealized_pnl', 'index_price'])
      .map((position) => {
        const greeks = position.greeks && typeof position.greeks === 'object' ? position.greeks as Record<string, unknown> : {};
        const normalized = { ...position };
        for (const field of ['delta', 'gamma', 'theta', 'vega']) {
          const value = position[field] ?? greeks[field];
          normalized[field] = value == null || value === '' || !Number.isFinite(Number(value)) ? null : Number(value);
        }
        return normalized;
      });
  });
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function getCollaterals(): Promise<any[]> {
  return cachedRequest('collaterals', async () => {
    const result = await lyraPost<{ collaterals: unknown[] }>('/private/get_collaterals', {
      subaccount_id: SUBACCOUNT_ID,
    });
    return accountRows(result, 'collaterals', 'asset_name', ['amount', 'mark_price'])
      .map((collateral) => {
        const normalized = { ...collateral, mark_value: collateral.mark_value ?? collateral.value };
        requireNumbers(normalized, ['mark_value'], 'collaterals');
        return normalized;
      });
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
    const result = await lyraPost<{ trades: unknown[] }>('/private/get_trade_history', body);
    return (result.trades ?? result) as unknown[];
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
    record(result, 'subaccount');
    requireNumbers(result, ['initial_margin', 'maintenance_margin', 'subaccount_value', 'collaterals_value', 'collaterals_initial_margin', 'collaterals_maintenance_margin', 'positions_initial_margin', 'open_orders_margin'], 'subaccount');
    const collateralRows = accountRows(result, 'collaterals', 'asset_name', ['amount', 'maintenance_margin']);
    const positionRows = accountRows(result, 'positions', 'instrument_name', ['amount', 'initial_margin']);
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
