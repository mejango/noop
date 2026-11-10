'use strict';

// Reporting-only account evidence. Never changes the source execution snapshot,
// options ledger, budget, or accumulation allocation.
const { getSpotAccumulationIdentity } = require('./spot-accumulation-identity');
const { buildReportingAccount, finiteNumber } = require('./portfolio-observation');
const PERIOD_MS = 900000;
const MAX_SKEW_MS = 30000;
const FRESHNESS_MS = 600000;
const hasTable = (db, name) => !!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name);
const iso = ms => new Date(ms).toISOString();
function reportingConfig(sqlite, derive, now) {
  // Empty environment deliberately ignores activation mode and unpersisted IDs.
  const config = getSpotAccumulationIdentity({ env: {}, derive, sqlite, nowMs: now });
  return config ? { config, scope: `${config.network}:${config.apiVersion}:${config.accountId}:${config.destinationAccountId}`,
    fundingScope: `${config.network}:${config.apiVersion}:${config.accountId}` } : null;
}
function initialize(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS reporting_portfolio_pairs (
    scope TEXT NOT NULL,timestamp TEXT NOT NULL,kind TEXT NOT NULL,pair_json TEXT NOT NULL,evidence_json TEXT NOT NULL,
    PRIMARY KEY(scope,timestamp,kind));
    CREATE TABLE IF NOT EXISTS reporting_portfolio_sync (scope TEXT PRIMARY KEY,state_json TEXT NOT NULL);`);
}
function transferPending(db, config) {
  return hasTable(db, 'spot_transfers') && !!db.prepare("SELECT 1 FROM spot_transfers WHERE scope=? AND status<>'settled' LIMIT 1").get(config.fundingScope);
}
function savePair(db, scope, pair, evidence) {
  // A later read failure is not evidence that an already observed NAV vanished.
  // Preserve complete history and its provenance; gaps may be repaired by proof.
  db.prepare(`INSERT INTO reporting_portfolio_pairs VALUES(?,?,?,?,?)
    ON CONFLICT(scope,timestamp,kind) DO UPDATE SET pair_json=excluded.pair_json,evidence_json=excluded.evidence_json
    WHERE excluded.kind<>'history' OR json_extract(excluded.pair_json,'$.combinedValue') IS NOT NULL
      OR json_extract(reporting_portfolio_pairs.pair_json,'$.combinedValue') IS NULL`)
    .run(scope, pair.timestamp, pair.kind, JSON.stringify(pair), JSON.stringify(evidence));
}
function pair(timestamp, sourceValue, destinationValue, kind, reason = null, extra = {}) {
  const combined = reason == null && sourceValue != null && destinationValue != null ? sourceValue + destinationValue : null;
  if (combined != null && !Number.isFinite(combined)) throw new Error('Combined NAV is not finite');
  return { timestamp: iso(timestamp), ts: timestamp, sourceValue, destinationValue, combinedValue: combined,
    kind, reason, sourceReceivedAt: null, destinationReceivedAt: null, ...extra };
}

async function fetchHistory(read, accountId, start, end) {
  const rows = [], timestamps = new Set(); let expectedCount, expectedPages;
  for (let page = 1; page <= 8; page++) {
    const result = await read('private/get_subaccount_value_history', { subaccount_id: Number(accountId),
      start_timestamp: start, end_timestamp: end, period: PERIOD_MS / 1000, page, page_size: 1000 });
    const list = result?.subaccount_value_history, pagination = result?.pagination;
    if (!Array.isArray(list) || !Number.isSafeInteger(pagination?.count) || pagination.count < 0
      || !Number.isSafeInteger(pagination.num_pages) || pagination.num_pages < 0 || pagination.num_pages > 8
      || (pagination.count > 0 && pagination.num_pages < 1)
      || pagination.num_pages !== Math.ceil(pagination.count / 1000)) throw new Error('Invalid or unbounded NAV history pagination');
    if (page === 1) { expectedCount = pagination.count; expectedPages = pagination.num_pages; }
    if (pagination.count !== expectedCount || pagination.num_pages !== expectedPages
      || list.length > 1000 || (page < expectedPages && list.length !== 1000)) throw new Error('NAV history pagination changed or is incomplete');
    for (const raw of list) {
      if (String(raw?.subaccount_id) !== String(accountId)) throw new Error('NAV history account mismatch');
      const timestamp = typeof raw.timestamp === 'number' || (typeof raw.timestamp === 'string' && /^\d+$/.test(raw.timestamp))
        ? Number(raw.timestamp) : typeof raw.timestamp === 'string' ? Date.parse(raw.timestamp) : NaN;
      if (!Number.isSafeInteger(timestamp) || timestamp % PERIOD_MS !== 0 || timestamp < start || timestamp > end
        || timestamps.has(timestamp)) throw new Error('NAV history timestamps are duplicated or outside the requested grid');
      timestamps.add(timestamp);
      rows.push({ timestamp, value: finiteNumber(raw.subaccount_value, 'historical subaccount value'), raw });
    }
    if (page >= expectedPages) break;
  }
  if (rows.length !== expectedCount) throw new Error('NAV history row count is incomplete');
  return rows.sort((a, b) => a.timestamp - b.timestamp);
}

function createPortfolioReportingCollector({ sqlite, derive, read, now = Date.now }) {
  let busy = false;
  async function collect({ sourceAccount, spotPrice } = {}) {
    if (busy) return { status: 'in_flight' };
    busy = true;
    try {
      const started = now(), identity = reportingConfig(sqlite, derive, started);
      if (!identity) return { status: 'unconfigured' };
      initialize(sqlite);
      const { config, scope } = identity;
      let source = null, destination = null, destinationRaw = null, reason = null;
      try { source = buildReportingAccount(sourceAccount, { subaccountId: config.accountId, label: 'NOOP', spotPrice }); }
      catch (error) { reason = `source_unavailable: ${error.message}`; }
      try {
        destinationRaw = await read('private/get_subaccount', { subaccount_id: Number(config.destinationAccountId) });
        destinationRaw = { ...destinationRaw, snapshot_received_at: iso(now()) };
        destination = buildReportingAccount(destinationRaw, { subaccountId: config.destinationAccountId, label: 'DRV-NOOP', spotPrice });
      } catch (error) { reason ||= `destination_unavailable: ${error.message}`; }
      const observedAt = now();
      if (!reason && (Math.abs(Date.parse(source.receivedAt) - Date.parse(destination.receivedAt)) > MAX_SKEW_MS
        || observedAt - Date.parse(source.receivedAt) > MAX_SKEW_MS || observedAt < Date.parse(source.receivedAt))) reason = 'account_receipt_skew';
      if (transferPending(sqlite, identity)) reason = 'internal_transfer_unresolved';
      const live = pair(observedAt, source?.valueUsd ?? null, destination?.valueUsd ?? null, 'current', reason,
        { sourceReceivedAt: source?.receivedAt ?? null, destinationReceivedAt: destination?.receivedAt ?? null,
          accounts: [source, destination].filter(Boolean) });
      savePair(sqlite, scope, live, { source: sourceAccount ?? null, destination: destinationRaw });

      const saved = sqlite.prepare('SELECT state_json FROM reporting_portfolio_sync WHERE scope=?').get(scope);
      const state = saved ? JSON.parse(saved.state_json) : {};
      if (started - (state.attemptedAt ?? 0) >= PERIOD_MS) {
        const end = Math.floor(started / PERIOD_MS) * PERIOD_MS;
        const boundary = Math.ceil(Date.parse(derive.historyFrom) / PERIOD_MS) * PERIOD_MS;
        const start = Math.max(boundary, (state.through ?? boundary) - PERIOD_MS);
        let update = { ...state, attemptedAt: started };
        try {
          const destinationRows = await fetchHistory(read, config.destinationAccountId, start, end);
          // The destination can start much later than source V3 history. Never
          // manufacture zero destination value before its first actual point.
          if (destinationRows.length) {
            const first = state.firstTimestamp == null ? destinationRows[0].timestamp : Math.max(state.firstTimestamp, start);
            const sourceRows = await fetchHistory(read, config.accountId, first, end);
            const sources = new Map(sourceRows.map(row => [row.timestamp, row]));
            const destinations = new Map(destinationRows.map(row => [row.timestamp, row]));
            let contiguousThrough = first - PERIOD_MS, missingPeriod = false;
            sqlite.transaction(() => {
              for (let timestamp = first; timestamp <= end; timestamp += PERIOD_MS) {
                const s = sources.get(timestamp), d = destinations.get(timestamp);
                const missing = !s || !d ? 'historical_account_value_missing' : null;
                if (missing) missingPeriod = true;
                if (!missingPeriod) contiguousThrough = timestamp;
                savePair(sqlite, scope, pair(timestamp, s?.value ?? null, d?.value ?? null, 'history', missing),
                  { method: 'private/get_subaccount_value_history', period: 900, source: s?.raw ?? null, destination: d?.raw ?? null });
              }
            }).immediate();
            update = { ...update, firstTimestamp: state.firstTimestamp ?? first, through: Math.max(state.through ?? first - PERIOD_MS, contiguousThrough),
              status: missingPeriod ? 'incomplete' : 'complete', reason: missingPeriod ? 'historical_account_value_missing' : null };
          } else {
            update = { ...update, status: 'empty', reason: 'destination_history_unavailable' };
            savePair(sqlite, scope, pair(end, null, null, 'history', 'destination_history_unavailable'), { method: 'private/get_subaccount_value_history', rows: [] });
          }
        } catch (error) {
          update = { ...update, status: 'incomplete', reason: error.message };
          savePair(sqlite, scope, pair(end, null, null, 'history', 'history_collection_failed'), { error: error.message });
        }
        sqlite.prepare('INSERT OR REPLACE INTO reporting_portfolio_sync VALUES(?,?)').run(scope, JSON.stringify(update));
      }
      return { status: reason ? 'incomplete' : 'observed', reason };
    } finally { busy = false; }
  }
  return { collect };
}

function readReportingPortfolio(sqlite, { derive, from, to, now = Date.now() }) {
  const identity = reportingConfig(sqlite, derive, now);
  const result = { config: identity?.config ?? null, scope: identity?.scope ?? null,
    coverage: { configured: !!identity, firstTimestamp: null, lastTimestamp: null,
      latestStatus: 'unavailable', reason: identity ? 'reporting_observations_unavailable' : 'unconfigured', historyStatus: 'unavailable',
      historyPeriodMs: PERIOD_MS, freshnessMs: FRESHNESS_MS }, rows: [], baseline: null, latest: null };
  if (!identity || !hasTable(sqlite, 'reporting_portfolio_pairs')) return result;
  const { scope } = identity;
  const decode = row => JSON.parse(row.pair_json);
  result.rows = sqlite.prepare('SELECT pair_json FROM reporting_portfolio_pairs WHERE scope=? AND timestamp>=? AND timestamp<=? ORDER BY timestamp,kind')
    .all(scope, iso(new Date(from).getTime()), iso(new Date(to).getTime())).map(decode);
  const baseline = sqlite.prepare("SELECT pair_json FROM reporting_portfolio_pairs WHERE scope=? AND timestamp<? ORDER BY timestamp DESC,kind DESC LIMIT 1")
    .get(scope, iso(new Date(from).getTime()));
  if (baseline) result.baseline = decode(baseline);
  const bounds = sqlite.prepare("SELECT MIN(timestamp) first,MAX(timestamp) last FROM reporting_portfolio_pairs WHERE scope=? AND json_extract(pair_json,'$.combinedValue') IS NOT NULL").get(scope);
  result.coverage.firstTimestamp = bounds.first; result.coverage.lastTimestamp = bounds.last;
  const raw = sqlite.prepare("SELECT pair_json FROM reporting_portfolio_pairs WHERE scope=? AND kind='current' ORDER BY timestamp DESC LIMIT 1").get(scope);
  if (raw) {
    result.latest = decode(raw);
    const stale = now - result.latest.ts > FRESHNESS_MS || now < result.latest.ts;
    result.coverage.latestStatus = stale ? 'stale' : result.latest.combinedValue == null ? 'incomplete' : 'complete';
    result.coverage.reason = stale ? 'reporting_observation_stale' : result.latest.reason;
  }
  if (hasTable(sqlite, 'reporting_portfolio_sync')) {
    const sync = sqlite.prepare('SELECT state_json FROM reporting_portfolio_sync WHERE scope=?').get(scope);
    if (sync) result.coverage.historyStatus = JSON.parse(sync.state_json).status;
  }
  return result;
}

module.exports = { createPortfolioReportingCollector, readReportingPortfolio, PERIOD_MS, MAX_SKEW_MS, FRESHNESS_MS };
