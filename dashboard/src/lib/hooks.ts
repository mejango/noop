'use client';

import { useState, useEffect, useCallback, useRef } from 'react';

export function usePolling<T>(url: string, initialData: T, interval = 60_000): { data: T; loading: boolean; error: string | null; refetch: () => void; fetchTick: number } {
  const [data, setData] = useState<T>(initialData);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [fetchTick, setFetchTick] = useState(0);

  const fetchData = useCallback(async () => {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
      setError(null);
      setFetchTick(t => t + 1);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Fetch failed');
    } finally {
      setLoading(false);
    }
  }, [url]);

  useEffect(() => {
    fetchData();
    const id = setInterval(fetchData, interval);
    return () => clearInterval(id);
  }, [fetchData, interval]);

  return { data, loading, error, refetch: fetchData, fetchTick };
}

/** Live-ticking "Xs ago" / "Xm ago" string that updates every second. */
export function useLiveTimeAgo(ts: string | null | undefined): string {
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  if (!ts) return '--';
  const diff = now - new Date(ts).getTime();
  const secs = Math.max(0, Math.floor(diff / 1000));
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

/** Countdown seconds until next poll. Returns formatted string like "42s" or "1m 12s". */
export function useCountdown(intervalMs: number, deps: unknown[] = []): string {
  const [remaining, setRemaining] = useState(intervalMs);
  const lastResetRef = useRef(Date.now());

  useEffect(() => {
    lastResetRef.current = Date.now();
    setRemaining(intervalMs);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  useEffect(() => {
    const id = setInterval(() => {
      const elapsed = Date.now() - lastResetRef.current;
      setRemaining(Math.max(0, intervalMs - elapsed));
    }, 1000);
    return () => clearInterval(id);
  }, [intervalMs]);

  const secs = Math.ceil(remaining / 1000);
  if (secs >= 60) {
    const m = Math.floor(secs / 60);
    const s = secs % 60;
    return s > 0 ? `${m}m ${s}s` : `${m}m`;
  }
  return `${secs}s`;
}

/** Returns true when viewport is narrower than the given breakpoint (default 768px). */
export function useIsMobile(breakpoint = 768): boolean {
  const [isMobile, setIsMobile] = useState(false);

  useEffect(() => {
    const mql = window.matchMedia(`(max-width: ${breakpoint - 1}px)`);
    setIsMobile(mql.matches);
    const handler = (e: MediaQueryListEvent) => setIsMobile(e.matches);
    mql.addEventListener('change', handler);
    return () => mql.removeEventListener('change', handler);
  }, [breakpoint]);

  return isMobile;
}
