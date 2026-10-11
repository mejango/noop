// Compare feeds against the latest chart observation, so zooming into history
// does not turn an old but complete window into an outage warning.
export function liquidityFeedGaps(rows: Record<string, number>[], names: string[]) {
  const latest = rows.reduce((time, row) => Math.max(time, row.ts), 0);
  const toleranceMs = 6 * 60 * 60 * 1000;
  return names.flatMap(name => {
    const last = rows.reduce((time, row) =>
      Number.isFinite(row[name]) ? Math.max(time, row.ts) : time, 0);
    return latest && (!last || latest - last > toleranceMs)
      ? [{ name, lastSample: last || null }]
      : [];
  });
}
