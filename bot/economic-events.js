'use strict';

// Reconciled venue evidence is separate from order intent and operational spend.
// Native quantities remain decimal strings; unknown valuation is never zero.
const KINDS = new Set(['trade', 'fee', 'settlement', 'transfer']);
const DATASETS = new Set(['trades', 'settlements', 'transfers']);
const accountIdentity = value => {
  const text = String(value);
  if (!/^[1-9]\d*$/.test(text) || !Number.isSafeInteger(Number(text))) throw new Error('Invalid or unsafe account identity');
  return text;
};
const decimal = (value, optional = false) => {
  if (value == null && optional) return null;
  const text = String(value);
  if (!/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/.test(text)) throw new Error(`Invalid decimal: ${text}`);
  const negative = text.startsWith('-');
  const [whole, fraction = ''] = (negative ? text.slice(1) : text).split('.');
  const tail = fraction.replace(/0+$/, '');
  const normalized = whole + (tail ? `.${tail}` : '');
  return negative && normalized !== '0' ? `-${normalized}` : normalized;
};
const iso = value => {
  if (value == null || value === '') throw new Error('Event timestamp is required');
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) throw new Error('Invalid event timestamp');
  return date.toISOString();
};
function multiply(a, b) {
  const parts = [decimal(a), decimal(b)].map(s => {
    const [whole, fraction = ''] = s.split('.');
    return { units: BigInt(whole + fraction), scale: fraction.length };
  });
  const product = parts[0].units * parts[1].units;
  const scale = parts[0].scale + parts[1].scale;
  const sign = product < 0n ? '-' : '';
  const digits = (product < 0n ? -product : product).toString().padStart(scale + 1, '0');
  return decimal(sign + (scale ? `${digits.slice(0, -scale)}.${digits.slice(-scale)}` : digits));
}
function normalizeEvent(input) {
  if (!input || !KINDS.has(input.event_type)) throw new Error('Unsupported economic event type');
  if (!input.event_id || input.account_id == null || !input.source || !input.currency) throw new Error('Economic evidence requires identity, account, source and currency');
  const accountId = accountIdentity(input.account_id);
  if (!input.raw_json) throw new Error('Economic evidence requires its original source record');
  const event = {
    event_id: String(input.event_id), account_id: accountId, event_type: input.event_type,
    timestamp: iso(input.timestamp), instrument_name: input.instrument_name || null,
    currency: String(input.currency), amount: decimal(input.amount),
    cashflow_usd: decimal(input.cashflow_usd, true),
    realized_pnl_usd: decimal(input.realized_pnl_usd, true), fee_usd: decimal(input.fee_usd, true),
    source: String(input.source), raw_json: typeof input.raw_json === 'string' ? input.raw_json : JSON.stringify(input.raw_json),
  };
  JSON.parse(event.raw_json);
  if (event.event_type === 'transfer' && event.realized_pnl_usd != null) throw new Error('Capital transfers are not realized profit');
  return event;
}
function normalizeV2Trade(trade, accountId) {
  if (!trade?.trade_id || !trade.instrument_name || !['buy', 'sell'].includes(trade.direction)) throw new Error('Trade history is missing durable trade identity or direction');
  if (accountIdentity(trade.subaccount_id) !== accountIdentity(accountId)) throw new Error('Trade account mismatch');
  if (trade.is_transfer !== false) throw new Error('Transfer trade identity is missing or requires explicit transfer accounting');
  // Pinned official V2 schema requires this field; only settled is terminal success.
  if (trade.tx_status !== 'settled') {
    throw new Error('Trade settlement status is unresolved or unsupported');
  }
  const amount = decimal(trade.trade_amount ?? trade.amount);
  const price = decimal(trade.trade_price ?? trade.price);
  if (amount.startsWith('-') || amount === '0' || price.startsWith('-')) throw new Error('Invalid executed trade amount or price');
  const premium = multiply(amount, price);
  // V2 option premiums are quoted in USD. Perpetual/other trades require their own valuation semantics.
  if (!/^ETH-\d{8}-[\d.]+-[CP]$/.test(trade.instrument_name)) throw new Error('Unsupported trade instrument in options accounting');
  return normalizeEvent({
    event_id: `v2:${accountId}:trade:${trade.trade_id}`, account_id: accountId, event_type: 'trade',
    timestamp: trade.timestamp, instrument_name: trade.instrument_name, currency: 'USD', amount,
    cashflow_usd: trade.direction === 'buy' && premium !== '0' ? `-${premium}` : premium,
    realized_pnl_usd: trade.realized_pnl_excl_fees ?? null,
    fee_usd: trade.trade_fee ?? null,
    source: 'derive-v2/private/get_trade_history', raw_json: trade,
  });
}
function createEconomicStore(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS economic_events (
    event_id TEXT PRIMARY KEY, account_id TEXT NOT NULL, event_type TEXT NOT NULL,
    timestamp TEXT NOT NULL, instrument_name TEXT, currency TEXT NOT NULL, amount TEXT NOT NULL,
    cashflow_usd TEXT, realized_pnl_usd TEXT, fee_usd TEXT, source TEXT NOT NULL, raw_json TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_economic_events_account_time ON economic_events(account_id,timestamp);
  CREATE TABLE IF NOT EXISTS economic_coverage (
    account_id TEXT NOT NULL, dataset TEXT NOT NULL, from_timestamp TEXT NOT NULL, to_timestamp TEXT NOT NULL,
    complete INTEGER NOT NULL, error TEXT, evidence_reference TEXT,
    PRIMARY KEY(account_id,dataset,from_timestamp,to_timestamp)
  );
  CREATE TABLE IF NOT EXISTS exposure_history (
    account_id TEXT NOT NULL, effective_at TEXT NOT NULL, external_eth TEXT NOT NULL, source TEXT NOT NULL,
    PRIMARY KEY(account_id,effective_at)
  );
  CREATE TABLE IF NOT EXISTS economic_trade_sync_work (
    account_id TEXT PRIMARY KEY, to_timestamp TEXT NOT NULL, windows_json TEXT NOT NULL
  );`);
  if (!db.prepare('PRAGMA table_info(economic_coverage)').all().some(column => column.name === 'evidence_reference')) {
    db.exec('ALTER TABLE economic_coverage ADD COLUMN evidence_reference TEXT');
  }
  db.exec(`CREATE TABLE IF NOT EXISTS economic_tracking_start (
    account_id TEXT PRIMARY KEY, started_at TEXT NOT NULL
  )`);
  const startTracking = (accountId, startedAt) => {
    const account = accountIdentity(accountId);
    db.prepare('INSERT OR IGNORE INTO economic_tracking_start (account_id,started_at) VALUES (?,?)').run(account,iso(startedAt));
    return db.prepare('SELECT started_at FROM economic_tracking_start WHERE account_id=?').get(account).started_at;
  };
  const keys = ['event_id','account_id','event_type','timestamp','instrument_name','currency','amount','cashflow_usd','realized_pnl_usd','fee_usd','source','raw_json'];
  const insert = db.prepare(`INSERT INTO economic_events (${keys.join(',')}) VALUES (${keys.map(k => `@${k}`).join(',')})`);
  const get = db.prepare('SELECT * FROM economic_events WHERE event_id = ?');
  const coverage = db.prepare(`INSERT INTO economic_coverage
    (account_id,dataset,from_timestamp,to_timestamp,complete,error,evidence_reference)
    VALUES (@account_id,@dataset,@from_timestamp,@to_timestamp,@complete,@error,@evidence_reference)
    ON CONFLICT(account_id,dataset,from_timestamp,to_timestamp) DO UPDATE SET
      complete=excluded.complete,error=excluded.error,
      evidence_reference=COALESCE(excluded.evidence_reference,economic_coverage.evidence_reference)`);
  function record(rows) {
    let inserted = 0;
    for (const input of rows) {
      const row = normalizeEvent(input);
      const previous = get.get(row.event_id);
      if (previous) {
        if (keys.filter(k => k !== 'raw_json').some(k => previous[k] !== row[k])) throw new Error(`Conflicting economic event ${row.event_id}; reconcile correction explicitly`);
      } else { insert.run(row); inserted++; }
    }
    return inserted;
  }
  const recordEvents = db.transaction(record);
  const recordBatch = db.transaction((rows, evidence) => {
    if (!evidence || !DATASETS.has(evidence.dataset)) throw new Error('Coverage dataset required');
    const account = accountIdentity(evidence.account_id);
    const from = iso(evidence.from_timestamp), to = iso(evidence.to_timestamp);
    if (from > to) throw new Error('Invalid coverage interval');
    for (const row of rows) {
      if (String(row.account_id) !== account || iso(row.timestamp) < from || iso(row.timestamp) > to) throw new Error('Event outside declared coverage');
      const allowed = evidence.dataset === 'trades' ? ['trade','fee'] : evidence.dataset === 'settlements' ? ['settlement'] : ['transfer'];
      if (!allowed.includes(row.event_type)) throw new Error('Event does not belong to coverage dataset');
    }
    const reference = evidence.evidence_reference ?? null;
    if (reference != null && (typeof reference !== 'string' || !reference.trim())) throw new Error('Invalid coverage evidence reference');
    const inserted = record(rows);
    coverage.run({account_id: account,dataset:evidence.dataset,from_timestamp:from,to_timestamp:to,
      complete:evidence.complete === true ? 1 : 0,error:evidence.error || null,evidence_reference:reference});
    return inserted;
  });
  function recordExposure(accountId, externalEth, effectiveAt, source = 'runtime-config') {
    accountId = accountIdentity(accountId);
    const amount = decimal(externalEth);
    if (amount.startsWith('-')) throw new Error('External exposure cannot be negative');
    const last = db.prepare('SELECT external_eth FROM exposure_history WHERE account_id=? ORDER BY effective_at DESC LIMIT 1').get(String(accountId));
    if (last?.external_eth !== amount) db.prepare('INSERT INTO exposure_history VALUES (?,?,?,?)').run(String(accountId),iso(effectiveAt),amount,source);
  }
  function latestCoverage(accountId, dataset, from = 0) {
    const start = iso(from);
    let cursor = start;
    const rows = db.prepare('SELECT from_timestamp,to_timestamp FROM economic_coverage WHERE account_id=? AND dataset=? AND complete=1 ORDER BY from_timestamp').all(accountIdentity(accountId),dataset);
    for (const row of rows) {
      if (row.from_timestamp > cursor) break;
      if (row.to_timestamp > cursor) cursor = row.to_timestamp;
    }
    return cursor === start ? null : cursor;
  }
  function getTradeSyncWork(accountId) {
    const row = db.prepare('SELECT * FROM economic_trade_sync_work WHERE account_id=?').get(accountIdentity(accountId));
    return row ? { to: Date.parse(row.to_timestamp), windows: JSON.parse(row.windows_json) } : null;
  }
  function saveTradeSyncWork(accountId, work) {
    accountId = accountIdentity(accountId);
    if (!work?.windows.length) {
      db.prepare('DELETE FROM economic_trade_sync_work WHERE account_id=?').run(accountId);
    } else {
      db.prepare(`INSERT INTO economic_trade_sync_work (account_id,to_timestamp,windows_json) VALUES (?,?,?)
        ON CONFLICT(account_id) DO UPDATE SET to_timestamp=excluded.to_timestamp,windows_json=excluded.windows_json`)
        .run(accountId,iso(work.to),JSON.stringify(work.windows));
    }
  }
  function tradeRangeCovered(accountId, from, to) {
    return coversRange(db.prepare(`SELECT * FROM economic_coverage WHERE account_id=? AND dataset='trades'
      AND complete=1 AND from_timestamp<=? AND to_timestamp>=?`).all(accountIdentity(accountId),iso(to),iso(from)),from,to);
  }
  return { recordEvents, recordBatch, recordExposure, startTracking, latestCoverage, getTradeSyncWork, saveTradeSyncWork, tradeRangeCovered };
}
function paginationInteger(value, field) {
  if ((typeof value !== 'number' && (typeof value !== 'string' || !/^\d+$/.test(value)))
      || !Number.isSafeInteger(Number(value)) || Number(value) < 0) throw new Error(`Invalid history pagination ${field}`);
  return Number(value);
}
async function syncV2Trades({ store, accountId, post, from = 0, to = Date.now(), maxPages = 20, pageSize = 1000 }) {
  accountId = accountIdentity(accountId);
  if (!Number.isInteger(maxPages) || maxPages < 1 || !Number.isInteger(pageSize) || pageSize < 1 || pageSize > 1000) {
    throw new Error('Invalid trade history pagination limits');
  }
  const fromMs = new Date(from).getTime(), toMs = new Date(to).getTime();
  if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || fromMs > toMs) throw new Error('Invalid trade history window');
  const rows = [];
  const seen = new Map();
  let expectedPagination = null;
  const evidence = {account_id:String(accountId),dataset:'trades',from_timestamp:iso(fromMs),to_timestamp:iso(toMs)};
  try {
    for (let page = 1; page <= maxPages; page++) {
      const result = await post({subaccount_id:Number(accountId),from_timestamp:fromMs,to_timestamp:toMs,page,page_size:pageSize});
      if (!result || result.error || !Array.isArray(result.trades)) throw new Error('Invalid trade history response');
      if (accountIdentity(result.subaccount_id) !== accountId) throw new Error('Trade history response account mismatch');
      if (result.pagination != null && (typeof result.pagination !== 'object' || Array.isArray(result.pagination))) throw new Error('Invalid history pagination');
      const metadata = {};
      for (const field of ['num_pages','count']) {
        const nested = result.pagination?.[field];
        const flat = result[field];
        if (nested != null && flat != null && paginationInteger(nested,field) !== paginationInteger(flat,field)) throw new Error(`Conflicting history pagination ${field}`);
        const value = nested ?? flat;
        metadata[field] = value == null ? null : paginationInteger(value,field);
      }
      if (expectedPagination && (metadata.num_pages !== expectedPagination.num_pages || metadata.count !== expectedPagination.count)) {
        throw new Error('History pagination changed between pages');
      }
      expectedPagination = metadata;
      let newCount = 0;
      for (const row of result.trades) {
        const event = normalizeV2Trade(row,accountId);
        const fingerprint = JSON.stringify(Object.fromEntries(Object.entries(event).filter(([key]) => key !== 'raw_json')));
        if (seen.has(event.event_id)) {
          if (seen.get(event.event_id) !== fingerprint) throw new Error('Conflicting repeated trade identity');
          throw new Error('Trade history repeated an identity across its pages');
        }
        seen.set(event.event_id,fingerprint);rows.push(event);newCount++;
      }
      const numPages = metadata.num_pages;
      if (metadata.count != null && rows.length > metadata.count) throw new Error('History row count exceeds pagination count');
      if (numPages === 0 && (rows.length > 0 || (metadata.count ?? 0) > 0)) throw new Error('History has rows but declares zero pages');
      if (numPages != null && numPages > maxPages) throw Object.assign(new Error('Trade history pagination incomplete'), {code:'HISTORY_PAGE_LIMIT'});
      if (page > 1 && newCount === 0 && (numPages != null || result.trades.length > 0)) throw new Error('Trade history pagination repeated without progress');
      if ((numPages != null && page >= numPages) || (numPages == null && result.trades.length < pageSize)) {
        if (metadata.count != null && rows.length !== metadata.count) throw new Error('History unique row count does not match pagination count');
        return { inserted:store.recordBatch(rows,{...evidence,complete:true,
          evidence_reference:`derive-v2/private/get_trade_history account=${accountId} from=${fromMs} to=${toMs} pages=${page} unique_trades=${rows.length}`}), count:rows.length, complete:true };
      }
      if (newCount === 0) throw new Error('Trade history pagination repeated without progress');
    }
    throw Object.assign(new Error('Trade history pagination incomplete'), {code:'HISTORY_PAGE_LIMIT'});
  } catch (error) {
    store.recordBatch([],{...evidence,complete:false,error:error.message});
    throw error;
  }
}
async function syncV2TradesProgressively(options) {
  const accountId = accountIdentity(options.accountId);
  const from = new Date(options.from ?? 0).getTime(), to = new Date(options.to ?? Date.now()).getTime();
  const budget = options.maxRequests ?? 20;
  if (!Number.isFinite(from) || !Number.isFinite(to) || from > to || !Number.isInteger(budget) || budget < 1) throw new Error('Invalid progressive trade history window or budget');
  let requests = 0, inserted = 0, count = 0;
  const originalPost = options.post;
  const post = async body => {
    if (requests >= budget) throw new Error('Trade history request budget reached; completed prefix and pending windows retained for next sync');
    requests++;
    return originalPost(body);
  };
  const {store} = options;
  let work = store.getTradeSyncWork(accountId);
  // Resume durable splits only when the skipped prefix is already proven and
  // the new request extends the same time interval. Explicit narrower imports
  // start a fresh work plan while preserving all recorded events and coverage.
  const firstPending = work?.windows[0]?.[0];
  const canResume = work && firstPending >= from && work.to <= to
    && (firstPending === from || store.tradeRangeCovered(accountId,from,firstPending));
  if (!canResume) work = {to,windows:[[from,to]]};
  else if (work.to < to) { work.windows.push([work.to,to]); work.to = to; }
  store.saveTradeSyncWork(accountId,work);
  while (work.windows.length) {
    const [start,end] = work.windows[0];
    try {
      const result = await syncV2Trades({...options,accountId,from:start,to:end,post,maxPages:Math.min(options.maxPages ?? 4,budget)});
      inserted += result.inserted; count += result.count;
      work.windows.shift();
    } catch (error) {
      if (error.code !== 'HISTORY_PAGE_LIMIT' || end - start <= 1) throw error;
      const midpoint = Math.floor(start + (end - start) / 2);
      work.windows.splice(0,1,[start,midpoint],[midpoint,end]);
    }
    // Split progress survives a request-budget exhaustion even before the first
    // complete leaf, and a crash after an event batch merely replays that leaf.
    store.saveTradeSyncWork(accountId,work);
  }
  return {complete:true,inserted,count,requests};
}

function coversRange(rows, from, to) {
  let cursor = iso(from);
  const end = iso(to);
  if (cursor > end) return false;
  for (const row of [...rows].filter(r => Number(r.complete) === 1).sort((a,b) => a.from_timestamp.localeCompare(b.from_timestamp))) {
    if (row.from_timestamp > cursor) return false;
    if (row.to_timestamp > cursor) cursor = row.to_timestamp;
    if (cursor >= end) return true;
  }
  return false;
}
function getEconomicHistory(db, accountId, from, to) {
  const start = iso(from), end = iso(to), account = String(accountId);
  const empty = {events:[],coverage:{trades:false,settlements:false,transfers:false},available:false,exposureHistory:[],exposureKnown:false};
  const tableExists = name => Boolean(db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name));
  if (!tableExists('economic_events') || !tableExists('economic_coverage')) return empty;
  const events = db.prepare('SELECT * FROM economic_events WHERE account_id=? AND timestamp>=? AND timestamp<=? ORDER BY timestamp,event_id').all(account,start,end);
  const coverageRows = db.prepare('SELECT * FROM economic_coverage WHERE account_id=? AND to_timestamp>=? AND from_timestamp<=?').all(account,start,end);
  const coverage = Object.fromEntries([...DATASETS].map(dataset => [dataset,coversRange(coverageRows.filter(r => r.dataset === dataset),start,end)]));
  let exposureHistory = [];
  if (tableExists('exposure_history')) {
    const prior = db.prepare('SELECT * FROM exposure_history WHERE account_id=? AND effective_at<=? ORDER BY effective_at DESC LIMIT 1').get(account,start);
    exposureHistory = [...(prior ? [prior] : []),...db.prepare('SELECT * FROM exposure_history WHERE account_id=? AND effective_at>? AND effective_at<=? ORDER BY effective_at').all(account,start,end)];
  }
  return {events,coverage,available:true,exposureHistory,exposureKnown:Boolean(exposureHistory[0]?.effective_at <= start)};
}
module.exports = { createEconomicStore, normalizeEvent, normalizeV2Trade, syncV2Trades, syncV2TradesProgressively, decimal, multiply, coversRange, getEconomicHistory };
