import { gzipSync } from 'node:zlib';

type CachePolicy = {
  freshMs: number;
  staleMs: number;
  browserMaxAgeSeconds?: number;
};

type CachedResponse = {
  json: string;
  gzip: ArrayBuffer | null;
  freshUntil: number;
  staleUntil: number;
};

type ResponseCacheState = {
  entries: Map<string, CachedResponse>;
  pending: Map<string, Promise<CachedResponse>>;
};

const globalCache = globalThis as typeof globalThis & { __noopResponseCache?: ResponseCacheState };
const state = globalCache.__noopResponseCache || {
  entries: new Map<string, CachedResponse>(),
  pending: new Map<string, Promise<CachedResponse>>(),
};
globalCache.__noopResponseCache = state;

const MAX_ENTRIES = 80;
const GZIP_THRESHOLD_BYTES = 1_024;

function cacheControl(policy: CachePolicy): string {
  const maxAge = Math.max(0, policy.browserMaxAgeSeconds ?? Math.floor(policy.freshMs / 1000));
  const staleSeconds = Math.max(0, Math.floor(policy.staleMs / 1000));
  return `public, max-age=${maxAge}, s-maxage=${Math.max(maxAge, Math.floor(policy.freshMs / 1000))}, stale-while-revalidate=${staleSeconds}`;
}

function responseFromEntry(
  request: Request,
  entry: CachedResponse,
  policy: CachePolicy,
  cacheStatus: 'hit' | 'miss' | 'stale',
): Response {
  const acceptsGzip = /(?:^|,)\s*gzip\s*(?:,|$)/i.test(request.headers.get('accept-encoding') || '');
  const compressed = acceptsGzip && entry.gzip;
  const body = compressed ? entry.gzip : entry.json;
  const headers = new Headers({
    'Cache-Control': cacheControl(policy),
    'Content-Type': 'application/json; charset=utf-8',
    'Vary': 'Accept-Encoding',
    'X-Noop-Cache': cacheStatus,
  });
  if (compressed) headers.set('Content-Encoding', 'gzip');
  return new Response(body, { status: 200, headers });
}

async function populate(
  key: string,
  loader: () => Response | Promise<Response>,
  policy: CachePolicy,
): Promise<CachedResponse> {
  const existing = state.pending.get(key);
  if (existing) return existing;

  const pending = Promise.resolve()
    .then(loader)
    .then(async (response) => {
      if (!response.ok) throw new Error(`Uncacheable response (${response.status})`);
      const json = await response.text();
      const byteLength = Buffer.byteLength(json);
      const now = Date.now();
      const compressed = byteLength >= GZIP_THRESHOLD_BYTES ? gzipSync(json, { level: 6 }) : null;
      const entry: CachedResponse = {
        json,
        gzip: compressed
          ? compressed.buffer.slice(compressed.byteOffset, compressed.byteOffset + compressed.byteLength) as ArrayBuffer
          : null,
        freshUntil: now + policy.freshMs,
        staleUntil: now + policy.freshMs + policy.staleMs,
      };
      state.entries.delete(key);
      state.entries.set(key, entry);
      while (state.entries.size > MAX_ENTRIES) {
        const oldest = state.entries.keys().next().value as string | undefined;
        if (!oldest) break;
        state.entries.delete(oldest);
      }
      return entry;
    })
    .finally(() => state.pending.delete(key));
  state.pending.set(key, pending);
  return pending;
}

export async function cachedJsonRoute(
  request: Request,
  key: string,
  loader: () => Response | Promise<Response>,
  policy: CachePolicy,
): Promise<Response> {
  const now = Date.now();
  const entry = state.entries.get(key);
  if (entry && now < entry.freshUntil) return responseFromEntry(request, entry, policy, 'hit');
  if (entry && now < entry.staleUntil) {
    void populate(key, loader, policy).catch(() => {});
    return responseFromEntry(request, entry, policy, 'stale');
  }

  try {
    const populated = await populate(key, loader, policy);
    return responseFromEntry(request, populated, policy, 'miss');
  } catch {
    // Preserve the original status/body for initial failures instead of hiding
    // it behind the cache abstraction.
    return loader();
  }
}

export function invalidateResponseCache(prefix: string): void {
  Array.from(state.entries.keys()).forEach((key) => {
    if (key.startsWith(prefix)) state.entries.delete(key);
  });
}
