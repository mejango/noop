'use strict';

const { toUnits, fromUnits } = require('./spot-accumulation');
const { getEconomicHistory } = require('./economic-events');
const { SAMPLE_BUDGET_USDC } = require('./spot-accumulation-store');

function getSpotAccumulationSettings({ env = process.env, derive, sqlite, nowMs = Date.now() }) {
  const mode = env.SPOT_ACCUMULATION_MODE || 'off';
  if (!['off', 'dry_run', 'live', 'sample'].includes(mode)) throw new Error('Invalid SPOT_ACCUMULATION_MODE');
  const hasConfig = sqlite?.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='spot_accumulation_config'").get();
  const scope = `${derive.network}:${derive.version}:${derive.subaccountId}`;
  const row = hasConfig ? sqlite.prepare('SELECT config_json FROM spot_accumulation_config WHERE scope=?').get(scope) : null;
  const stored = row ? JSON.parse(row.config_json) : null;
  // Existing reservations must still be recoverable when new sends are disabled.
  if (mode === 'off' && !stored) return { mode, config: null };
  if (!sqlite || derive.version !== 'v3') throw new Error('Spot accumulation requires a V3 account and SQLite');
  const destination = String(env.SPOT_ACCUMULATION_DESTINATION_ID || stored?.destinationAccountId || '');
  if (!/^[1-9]\d*$/.test(destination) || !Number.isSafeInteger(Number(destination))
    || Number(destination) === derive.subaccountId) throw new Error('Explicit distinct spot destination required');
  const from = env.SPOT_ACCUMULATION_FROM || stored?.activationTimestamp
    || (mode === 'sample' ? new Date(nowMs).toISOString() : null);
  const boundary = from == null ? NaN : Date.parse(from);
  if (!Number.isFinite(boundary) || boundary > nowMs || boundary < Date.parse(derive.historyFrom)) {
    throw new Error('Explicit spot activation boundary within V3 history is required');
  }
  const owner = String(derive.wallet).toLowerCase();
  if (!/^0x[\da-f]{40}$/.test(owner)) throw new Error('Invalid spot owner');
  if (stored && (stored.network !== derive.network || stored.apiVersion !== derive.version
    || String(stored.accountId) !== String(derive.subaccountId) || stored.accountAddress !== owner
    || String(stored.destinationAccountId) !== destination || stored.activationTimestamp !== new Date(boundary).toISOString())) {
    throw new Error('Stored accumulation identity or boundary conflicts with configuration');
  }
  const maxBatch = env.SPOT_ACCUMULATION_MAX_BATCH_USDC;
  let maxBatchUsdc = maxBatch == null || maxBatch === '' ? null : fromUnits(toUnits(maxBatch, 'maximum accumulation batch', false));
  if (mode === 'sample') {
    if (maxBatchUsdc && maxBatchUsdc !== SAMPLE_BUDGET_USDC) throw new Error('Startup sample budget is fixed at 5 USDC');
    maxBatchUsdc = SAMPLE_BUDGET_USDC;
  }
  return { mode, config: { network: derive.network, apiVersion: derive.version,
    accountId: String(derive.subaccountId), destinationAccountId: destination, accountAddress: owner,
    activationTimestamp: new Date(boundary).toISOString(), ...(maxBatchUsdc ? { maxBatchUsdc } : {}) } };
}

function readSpotSourceHistory({ sqlite, config, nowMs = Date.now() }) {
  const tables = new Set(sqlite.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map(row => row.name));
  const empty = { events: [], orders: [], complete: false, throughTimestamp: null };
  if (!['economic_events', 'economic_coverage', 'orders'].every(name => tables.has(name))) return empty;
  return sqlite.transaction(() => {
    const latest = sqlite.prepare(`SELECT MAX(to_timestamp) AS through FROM economic_coverage
      WHERE account_id=? AND dataset='trades' AND complete=1 AND to_timestamp>=? AND to_timestamp<=?`)
      .get(config.accountId, config.activationTimestamp, new Date(nowMs).toISOString())?.through;
    if (!latest) return empty;
    const history = getEconomicHistory(sqlite, config.accountId, config.activationTimestamp, latest);
    const orders = sqlite.prepare("SELECT * FROM orders WHERE action='sell_put' AND success=1").all();
    return { events: history.events, orders, complete: history.coverage.trades === true, throughTimestamp: latest };
  })();
}

function readSpotPendingActions({ sqlite }) {
  // Resting liabilities come from current venue orders. Pending/confirmed
  // intents have not yet handed that ownership to the resting-order tracker.
  return sqlite.prepare("SELECT * FROM pending_actions WHERE status IN ('pending','confirmed') ORDER BY triggered_at,id").all();
}

module.exports = { getSpotAccumulationSettings, readSpotSourceHistory, readSpotPendingActions };
