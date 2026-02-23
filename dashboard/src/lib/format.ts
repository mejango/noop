export function formatUSD(n: number | null | undefined): string {
  if (n == null || isNaN(n)) return '$--.--';
  return `$${n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

export function formatNum(n: number | null | undefined, decimals = 2): string {
  if (n == null || isNaN(n)) return '--';
  return n.toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

export function timeAgo(ts: string | null | undefined): string {
  if (!ts) return '--';
  const diff = Date.now() - new Date(ts).getTime();
  const secs = Math.floor(diff / 1000);
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

export function dteDays(expiryUnix: number | null | undefined): number | null {
  if (!expiryUnix) return null;
  const ms = expiryUnix * 1000 - Date.now();
  return Math.max(0, Math.ceil(ms / (24 * 60 * 60 * 1000)));
}

export function momentumColor(main: string | null | undefined): string {
  if (!main) return 'text-zinc-400';
  if (main === 'upward') return 'text-green-400';
  if (main === 'downward') return 'text-red-400';
  return 'text-zinc-400';
}

export function momentumBg(main: string | null | undefined): string {
  if (!main) return 'bg-zinc-800';
  if (main === 'upward') return 'bg-green-900/30';
  if (main === 'downward') return 'bg-red-900/30';
  return 'bg-zinc-800';
}
