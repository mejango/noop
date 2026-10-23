import Database from 'better-sqlite3';
import path from 'path';
import { dashboardRangeMs, type DashboardRange } from './dashboard-ranges';
import { normalizeBuyPutScore, BUY_PUT_EDGE_MIN_DTE, BUY_PUT_EDGE_MAX_DTE } from '../../../bot/put-score';
import { normalizeSellCallScore } from '../../../bot/call-score';
import strategyFacts from '../../../bot/strategy-facts.json';

export type AssetQuote = {
  instrument_name: string;
  expiry: number | null;
  strike: number | null;
  option_type: string;
  bid: number | null;
  ask: number | null;
  mark: number | null;
  index_price: number | null;
  delta: number | null;
  implied_vol: number | null;
  open_interest: number | null;
  bid_amount: number | null;
  ask_amount: number | null;
  bid_iv: number | null;
  ask_iv: number | null;
  forward: number | null;
};

export type AssetQuoteFrame = { hour: string; spot: number | null; quotes: AssetQuote[] };
type Snapshot = {
  hour: string; spot: number | null; funding_rate: number | null;
  short_momentum_main: string | null; short_momentum_derivative: string | null;
  medium_momentum_main: string | null; medium_momentum_derivative: string | null;
  put_oi: number | null; call_oi: number | null; put_iv: number | null; call_iv: number | null;
  instrument_count: number | null; option_count: number | null; quoted_count: number | null;
  quote_status: string | null;
};
type QuoteRow = AssetQuote & { hour: string; spot: number | null };

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), '..', 'data');
let sharedDb: Database.Database | null = null;
function getDb(): Database.Database {
  if (!sharedDb) sharedDb = new Database(path.join(DATA_DIR, 'noop.db'), { readonly: true });
  return sharedDb;
}

// Sample across the full requested period. Every selected frame retains all of its
// quotes, so the best contract is never an artifact of a per-frame quote limit.
export function getAssetQuoteFrames(
  asset: string, since: string, maxFrames: number, database: Database.Database = getDb(),
  entryOnly = false, now = Date.now(),
): AssetQuoteFrame[] {
  const sinceMs = Date.parse(since);
  if (!Number.isFinite(sinceMs) || maxFrames < 1) return [];
  const frameCap = Math.min(720, Math.floor(maxFrames));
  const bucketSeconds = Math.max(1, Math.ceil((now - sinceMs) / 1000 / frameCap));
  const entryFilter = entryOnly ? `AND (
      (q.option_type = 'P' AND q.delta BETWEEN ${strategyFacts.put_delta_range[0]} AND ${strategyFacts.put_delta_range[1]}
       AND q.ask > 0 AND (q.expiry - unixepoch(s.hour)) / 86400.0 BETWEEN ${BUY_PUT_EDGE_MIN_DTE} AND ${BUY_PUT_EDGE_MAX_DTE})
      OR
      (q.option_type = 'C' AND q.delta BETWEEN ${strategyFacts.call_delta_range[0]} AND ${strategyFacts.call_delta_range[1]}
       AND q.bid > 0 AND (q.expiry - unixepoch(s.hour)) / 86400.0 BETWEEN ${strategyFacts.call_dte_range[0]} AND ${strategyFacts.call_dte_range[1]})
    )` : '';
  const rows = database.prepare(`WITH sample AS (
      SELECT hour, spot, ROW_NUMBER() OVER (
        PARTITION BY CAST(unixepoch(hour) / ? AS INTEGER)
        ORDER BY julianday(hour) DESC, hour DESC
      ) AS sample_rank
      FROM asset_market_snapshots
      WHERE asset = ? AND hour >= substr(?, 1, 19) AND julianday(hour) >= julianday(?)
    ), selected AS (
    SELECT s.hour, s.spot, q.instrument_name, q.expiry, q.strike, q.option_type,
      q.bid, q.ask, q.mark, q.index_price, q.delta, q.implied_vol,
      q.open_interest, q.bid_amount, q.ask_amount, q.bid_iv, q.ask_iv, q.forward,
      ROW_NUMBER() OVER (PARTITION BY s.hour, q.option_type ORDER BY
        CASE WHEN q.option_type = 'P' THEN ABS(q.delta) / q.ask
          WHEN q.option_type = 'C' THEN q.bid / ABS(q.delta) ELSE 0 END DESC
      ) AS quote_rank
    FROM sample s LEFT JOIN asset_option_quotes q ON q.asset = ? AND q.hour = s.hour
      ${entryFilter}
    WHERE s.sample_rank = 1)
    SELECT hour,spot,instrument_name,expiry,strike,option_type,bid,ask,mark,index_price,
      delta,implied_vol,open_interest,bid_amount,ask_amount,bid_iv,ask_iv,forward
    FROM selected WHERE ${entryOnly ? 'quote_rank <= 100' : '1 = 1'}
    ORDER BY julianday(hour), hour, instrument_name`)
    .all(bucketSeconds, asset, since, since, asset) as (QuoteRow & { instrument_name: string | null })[];
  const frames: AssetQuoteFrame[] = [];
  for (const row of rows) {
    let frame = frames[frames.length - 1];
    if (!frame || frame.hour !== row.hour) {
      frame = { hour: row.hour, spot: row.spot, quotes: [] };
      frames.push(frame);
    }
    if (row.instrument_name) {
      frame.quotes.push({
        instrument_name: row.instrument_name, expiry: row.expiry, strike: row.strike,
        option_type: row.option_type, bid: row.bid, ask: row.ask, mark: row.mark,
        index_price: row.index_price, delta: row.delta, implied_vol: row.implied_vol,
        open_interest: row.open_interest, bid_amount: row.bid_amount, ask_amount: row.ask_amount,
        bid_iv: row.bid_iv, ask_iv: row.ask_iv, forward: row.forward,
      });
    }
  }
  return frames;
}

function sampledSnapshots(database: Database.Database, asset: string, since: string, maxRows: number, now: number): Snapshot[] {
  const bucketSeconds = Math.max(1, Math.ceil((now - Date.parse(since)) / 1000 / maxRows));
  return database.prepare(`WITH sample AS (
      SELECT *, ROW_NUMBER() OVER (
        PARTITION BY CAST(unixepoch(hour) / ? AS INTEGER)
        ORDER BY julianday(hour) DESC, hour DESC
      ) AS sample_rank
      FROM asset_market_snapshots
      WHERE asset = ? AND hour >= substr(?, 1, 19) AND julianday(hour) >= julianday(?)
    )
    SELECT hour,spot,funding_rate,short_momentum_main,short_momentum_derivative,
      medium_momentum_main,medium_momentum_derivative,put_oi,call_oi,put_iv,call_iv,
      instrument_count,option_count,quoted_count,quote_status
    FROM sample WHERE sample_rank = 1 ORDER BY julianday(hour), hour`)
    .all(bucketSeconds, asset, since, since) as Snapshot[];
}

type Scored = { raw: number; edge: number; quote: AssetQuote };
function candidate(quote: AssetQuote, frameMs: number): { side: 'P' | 'C'; raw: number; edge: number } | null {
  const delta = quote.delta;
  const expiry = quote.expiry;
  if (delta == null || expiry == null) return null;
  const dte = (expiry * 1000 - frameMs) / 86400000;
  if (quote.option_type === 'P'
    && delta >= strategyFacts.put_delta_range[0] && delta <= strategyFacts.put_delta_range[1]
    && dte >= BUY_PUT_EDGE_MIN_DTE && dte <= BUY_PUT_EDGE_MAX_DTE && (quote.ask ?? 0) > 0) {
    const raw = Math.abs(delta) / quote.ask!;
    return { side: 'P', raw, edge: normalizeBuyPutScore(raw, dte) };
  }
  if (quote.option_type === 'C'
    && delta >= strategyFacts.call_delta_range[0] && delta <= strategyFacts.call_delta_range[1]
    && dte >= strategyFacts.call_dte_range[0] && dte <= strategyFacts.call_dte_range[1]
    && (quote.bid ?? 0) > 0) {
    const raw = quote.bid! / Math.abs(delta);
    return { side: 'C', raw, edge: normalizeSellCallScore(raw, dte) };
  }
  return null;
}

function optionDetail(scored: Scored | null) {
  if (!scored) return null;
  const q = scored.quote;
  return { delta: q.delta, price: q.option_type === 'P' ? q.ask : q.bid,
    strike: q.strike, expiry: q.expiry, instrument: q.instrument_name, raw_score: scored.raw };
}

function getAssetLiquidity(database: Database.Database, asset: string, since: string, now: number) {
  const bucketSeconds = Math.max(1, Math.ceil((now - Date.parse(since)) / 1000 / 500));
  const rows = database.prepare(`WITH sample AS (
      SELECT DISTINCT observed_at, ROW_NUMBER() OVER (
        PARTITION BY CAST(unixepoch(observed_at) / ? AS INTEGER)
        ORDER BY julianday(observed_at) DESC, observed_at DESC
      ) AS sample_rank
      FROM asset_liquidity_snapshots
      WHERE asset = ? AND julianday(observed_at) >= julianday(?)
        AND julianday(observed_at) <= julianday(?)
    )
    SELECT l.observed_at, l.venue, l.chain, l.pair, l.tvl_usd
    FROM sample s JOIN asset_liquidity_snapshots l
      ON l.asset = ? AND l.observed_at = s.observed_at
    WHERE s.sample_rank = 1 AND l.tvl_usd >= 0
    ORDER BY julianday(l.observed_at), l.observed_at`)
    .all(bucketSeconds, asset, since, new Date(now).toISOString(), asset) as {
      observed_at: string; venue: string; chain: string | null; pair: string; tvl_usd: number;
    }[];
  const points = new Map<string, Record<string, string | number>>();
  for (const row of rows) {
    if (!Number.isFinite(row.tvl_usd) || !row.venue || !row.pair) continue;
    const chain = row.chain && row.chain.toLowerCase() !== row.venue.toLowerCase() ? `${row.chain} ` : '';
    const series = `${row.venue} ${chain}${row.pair}`;
    let point = points.get(row.observed_at);
    if (!point) {
      point = { timestamp: row.observed_at };
      points.set(row.observed_at, point);
    }
    point[series] = Number(point[series] || 0) + row.tvl_usd;
  }
  return Array.from(points.values());
}

export function getAssetOverview(
  asset: string, range: DashboardRange, database: Database.Database = getDb(), now = Date.now(),
) {
  const rangeMs = dashboardRangeMs(range, '24h');
  const since = new Date(now - rangeMs).toISOString();
  const snapshots = sampledSnapshots(database, asset, since, 500, now);
  const frameCap = range === '1h' ? 60 : range === '6h' ? 120 : range === '24h' ? 144 : 300;
  const frames = getAssetQuoteFrames(asset, since, frameCap, database, true, now);
  const latest = database.prepare(`SELECT hour,spot,short_momentum_main,short_momentum_derivative,
      medium_momentum_main,medium_momentum_derivative
    FROM asset_market_snapshots WHERE asset = ?
    ORDER BY julianday(hour) DESC, hour DESC LIMIT 1`).get(asset) as Snapshot | undefined;
  const highs = database.prepare(`SELECT
      MAX(CASE WHEN julianday(hour) >= julianday(?) THEN spot END) AS three_day_high,
      MIN(CASE WHEN julianday(hour) >= julianday(?) THEN spot END) AS three_day_low,
      MAX(spot) AS seven_day_high, MIN(spot) AS seven_day_low
    FROM asset_market_snapshots WHERE asset = ? AND julianday(hour) >= julianday(?)`)
    .get(new Date(now - 3 * 86400000).toISOString(), new Date(now - 3 * 86400000).toISOString(),
      asset, new Date(now - 7 * 86400000).toISOString()) as Record<string, number | null>;
  const coverage = database.prepare(`SELECT MIN(hour) AS firstTimestamp, MAX(hour) AS lastTimestamp,
      COUNT(*) AS totalRows,
      MIN(CASE WHEN julianday(hour) >= julianday(?) THEN hour END) AS firstInRange,
      MAX(CASE WHEN julianday(hour) >= julianday(?) THEN hour END) AS lastInRange,
      COALESCE(SUM(CASE WHEN julianday(hour) >= julianday(?) THEN 1 ELSE 0 END), 0) AS rowsInRange
    FROM asset_option_quotes WHERE asset = ?`).get(since, since, since, asset) as Record<string, string | number | null>;
  const options: { timestamp: string; best_put_value: number | null; best_call_value: number | null; lyra_spot: number | null }[] = [];
  const buyPutEdge: { timestamp: string; edge_score: number }[] = [];
  const sellCallEdge: { timestamp: string; edge_score: number }[] = [];
  const optionsHeatmap: Record<string, string | number | null>[] = [];
  let newestScores: { put: Scored | null; call: Scored | null } | null = null;
  for (const frame of frames) {
    let putRaw: Scored | null = null;
    let callRaw: Scored | null = null;
    let putEdge: Scored | null = null;
    let callEdge: Scored | null = null;
    const frameMs = Date.parse(frame.hour);
    for (const quote of frame.quotes) {
      const score = candidate(quote, frameMs);
      if (!score) continue;
      const valued: Scored = { raw: score.raw, edge: score.edge, quote };
      if (score.side === 'P') {
        if (!putRaw || valued.raw > putRaw.raw) putRaw = valued;
        if (!putEdge || valued.edge > putEdge.edge) putEdge = valued;
      } else {
        if (!callRaw || valued.raw > callRaw.raw) callRaw = valued;
        if (!callEdge || valued.edge > callEdge.edge) callEdge = valued;
      }
      optionsHeatmap.push({
        timestamp: frame.hour, option_type: quote.option_type, instrument_name: quote.instrument_name,
        strike: quote.strike, delta: quote.delta, ask_price: quote.ask, bid_price: quote.bid,
        index_price: quote.index_price, expiry: quote.expiry,
        ask_delta_value: quote.option_type === 'P' ? score.raw : null,
        bid_delta_value: quote.option_type === 'C' ? score.raw : null,
        mark_price: quote.mark, implied_vol: quote.implied_vol,
        ask_amount: quote.ask_amount, bid_amount: quote.bid_amount,
      });
    }
    options.push({ timestamp: frame.hour, best_put_value: putRaw?.raw ?? null,
      best_call_value: callRaw?.raw ?? null, lyra_spot: frame.spot });
    if (putEdge) buyPutEdge.push({ timestamp: frame.hour, edge_score: putEdge.edge });
    if (callEdge) sellCallEdge.push({ timestamp: frame.hour, edge_score: callEdge.edge });
    newestScores = { put: putEdge, call: callEdge };
  }
  const boundedHeatmap = optionsHeatmap.length <= 12000 ? optionsHeatmap : optionsHeatmap
    .filter((_, index) => index % Math.ceil(optionsHeatmap.length / 12000) === 0).slice(0, 12000);
  const oiSnapshots = snapshots.map(s => ({ timestamp: s.hour, put_oi: s.put_oi,
    call_oi: s.call_oi, total_oi: s.put_oi != null && s.call_oi != null ? s.put_oi + s.call_oi : null,
    pc_ratio: s.call_oi ? (s.put_oi ?? 0) / s.call_oi : null,
    near_put_oi: null, near_call_oi: null, far_put_oi: null, far_call_oi: null, expiry_count: null,
    quote_status: s.quote_status }));
  const chart = {
    prices: snapshots.filter(s => s.spot != null).map(s => ({
      timestamp: s.hour, price: s.spot,
      short_momentum_main: s.short_momentum_main,
      short_momentum_derivative: s.short_momentum_derivative,
      medium_momentum_main: s.medium_momentum_main,
      medium_momentum_derivative: s.medium_momentum_derivative,
    })),
    options, buyPutEdge, sellCallEdge, optionsHeatmap: boundedHeatmap,
    liquidity: getAssetLiquidity(database, asset, since, now), liquidityVolume: [],
    sentiment: {
      fundingRates: snapshots.filter(s => s.funding_rate != null).map(s => ({ timestamp: s.hour, rate: s.funding_rate })),
      optionsSkew: snapshots.map(s => ({ timestamp: s.hour, avg_put_iv: s.put_iv, avg_call_iv: s.call_iv })),
      aggregateOI: oiSnapshots.filter(s => s.total_oi != null).map(s => ({ timestamp: s.timestamp, total_oi: s.total_oi })),
      oiSnapshots,
    },
    optionsCoverage: { ...coverage, requestedSince: since,
      hasGapBeforeRange: Boolean(coverage.firstInRange && Date.parse(String(coverage.firstInRange)) - Date.parse(since) > 6 * 3600000) },
    tier: 'sampled',
  };
  const stats = { last_price: latest?.spot ?? null, last_price_time: latest?.hour ?? null,
    lyra_spot: latest?.spot ?? null,
    short_momentum: latest?.short_momentum_main ?? null,
    short_derivative: latest?.short_momentum_derivative ?? null,
    medium_momentum: latest?.medium_momentum_main ?? null,
    medium_derivative: latest?.medium_momentum_derivative ?? null,
    ...highs };
  const ticks = newestScores && frames.length ? [{ id: 0, timestamp: frames[frames.length - 1].hour,
    summary: JSON.stringify({ current_best_put: newestScores.put?.edge ?? null,
      current_best_call: newestScores.call?.edge ?? null,
      best_put_detail: optionDetail(newestScores.put), best_call_detail: optionDetail(newestScores.call) }) }] : [];
  return { range, stats, chart, ticks };
}
