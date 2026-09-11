'use strict';

// Reconciled venue evidence is separate from order intent and operational spend.
// Native quantities remain decimal strings; unknown valuation is never zero.
const KINDS = new Set(['trade', 'fee', 'settlement', 'transfer']);
const DATASETS = new Set(['trades', 'settlements', 'transfers']);
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
  const accountId = String(input.account_id);
  if (!/^\d+$/.test(accountId)) throw new Error('Invalid account identity');
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
  if (trade.subaccount_id != null && String(trade.subaccount_id) !== String(accountId)) throw new Error('Trade account mismatch');
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
    complete INTEGER NOT NULL, error TEXT,
    PRIMARY KEY(account_id,dataset,from_timestamp,to_timestamp)
  );
  CREATE TABLE IF NOT EXISTS exposure_history (
    effective_at TEXT PRIMARY KEY, external_eth TEXT NOT NULL, source TEXT NOT NULL
  );`);
  const keys = ['event_id','account_id','event_type','timestamp','instrument_name','currency','amount','cashflow_usd','realized_pnl_usd','fee_usd','source','raw_json'];
  const insert = db.prepare(`INSERT INTO economic_events (${keys.join(',')}) VALUES (${keys.map(k => `@${k}`).join(',')})`);
  const get = db.prepare('SELECT * FROM economic_events WHERE event_id = ?');
  const coverage = db.prepare(`INSERT INTO economic_coverage VALUES (@account_id,@dataset,@from_timestamp,@to_timestamp,@complete,@error)
    ON CONFLICT(account_id,dataset,from_timestamp,to_timestamp) DO UPDATE SET complete=excluded.complete,error=excluded.error`);
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
    const account = String(evidence.account_id);
    const from = iso(evidence.from_timestamp), to = iso(evidence.to_timestamp);
    if (from > to) throw new Error('Invalid coverage interval');
    for (const row of rows) {
      if (String(row.account_id) !== account || iso(row.timestamp) < from || iso(row.timestamp) > to) throw new Error('Event outside declared coverage');
      const allowed = evidence.dataset === 'trades' ? ['trade','fee'] : evidence.dataset === 'settlements' ? ['settlement'] : ['transfer'];
      if (!allowed.includes(row.event_type)) throw new Error('Event does not belong to coverage dataset');
    }
    const inserted = record(rows);
    coverage.run({account_id: account,dataset:evidence.dataset,from_timestamp:from,to_timestamp:to,complete:evidence.complete === true ? 1 : 0,error:evidence.error || null});
    return inserted;
  });
  function recordExposure(externalEth, effectiveAt, source = 'runtime-config') {
    const amount = decimal(externalEth);
    if (amount.startsWith('-')) throw new Error('External exposure cannot be negative');
    const last = db.prepare('SELECT external_eth FROM exposure_history ORDER BY effective_at DESC LIMIT 1').get();
    if (last?.external_eth !== amount) db.prepare('INSERT INTO exposure_history VALUES (?,?,?)').run(iso(effectiveAt),amount,source);
  }
  function latestCoverage(accountId, dataset) {
    return db.prepare('SELECT MAX(to_timestamp) AS until FROM economic_coverage WHERE account_id=? AND dataset=? AND complete=1').get(String(accountId),dataset)?.until || null;
  }
  return { recordEvents, recordBatch, recordExposure, latestCoverage };
}
async function syncV2Trades({ store, accountId, post, from = 0, to = Date.now(), maxPages = 20, pageSize = 1000 }) {
  const fromMs = new Date(from).getTime(), toMs = new Date(to).getTime();
  if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || fromMs > toMs) throw new Error('Invalid trade history window');
  const rows = [];
  const seen = new Set();
  const evidence = {account_id:String(accountId),dataset:'trades',from_timestamp:iso(fromMs),to_timestamp:iso(toMs)};
  try {
    for (let page = 1; page <= maxPages; page++) {
      const result = await post({subaccount_id:Number(accountId),from_timestamp:fromMs,to_timestamp:toMs,page,page_size:pageSize});
      if (!result || result.error || !Array.isArray(result.trades)) throw new Error('Invalid trade history response');
      let newCount = 0;
      for (const row of result.trades) {
        const event = normalizeV2Trade(row,accountId);
        if (!seen.has(event.event_id)) {seen.add(event.event_id);rows.push(event);newCount++;}
      }
      const numPages = result.pagination?.num_pages ?? result.num_pages;
      if (numPages != null && (!Number.isInteger(Number(numPages)) || Number(numPages) < 0)) throw new Error('Invalid history pagination');
      if ((numPages != null && page >= Number(numPages)) || (numPages == null && result.trades.length < pageSize)) {
        return { inserted:store.recordBatch(rows,{...evidence,complete:true}), count:rows.length, complete:true };
      }
      if (newCount === 0) throw new Error('Trade history pagination repeated without progress');
    }
    throw new Error('Trade history pagination incomplete');
  } catch (error) {
    store.recordBatch([],{...evidence,complete:false,error:error.message});
    throw error;
  }
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
    const prior = db.prepare('SELECT * FROM exposure_history WHERE effective_at<=? ORDER BY effective_at DESC LIMIT 1').get(start);
    exposureHistory = [...(prior ? [prior] : []),...db.prepare('SELECT * FROM exposure_history WHERE effective_at>? AND effective_at<=? ORDER BY effective_at').all(start,end)];
  }
  return {events,coverage,available:true,exposureHistory,exposureKnown:Boolean(exposureHistory[0]?.effective_at <= start)};
}
module.exports = { createEconomicStore, normalizeEvent, normalizeV2Trade, syncV2Trades, decimal, multiply, coversRange, getEconomicHistory };
