'use strict';

const { toUnits, fromUnits } = require('./spot-accumulation');
const { getEconomicHistory } = require('./economic-events');
const { SAMPLE_BUDGET_USDC } = require('./spot-accumulation-store');
const { getSpotAccumulationIdentity } = require('./spot-accumulation-identity');

function getSpotAccumulationSettings({ env = process.env, derive, sqlite, nowMs = Date.now() }) {
  const mode = env.SPOT_ACCUMULATION_MODE || 'off';
  if (!['off', 'dry_run', 'live', 'sample'].includes(mode)) throw new Error('Invalid SPOT_ACCUMULATION_MODE');
  const identity = getSpotAccumulationIdentity({ env, derive, sqlite, mode, nowMs });
  if (!identity) return { mode, config: null };
  const maxBatch = env.SPOT_ACCUMULATION_MAX_BATCH_USDC;
  let maxBatchUsdc = maxBatch == null || maxBatch === '' ? null : fromUnits(toUnits(maxBatch, 'maximum accumulation batch', false));
  if (mode === 'sample') {
    if (maxBatchUsdc && maxBatchUsdc !== SAMPLE_BUDGET_USDC) throw new Error('Startup sample budget is fixed at 5 USDC');
    maxBatchUsdc = SAMPLE_BUDGET_USDC;
  }
  return { mode, config: { ...identity, ...(maxBatchUsdc ? { maxBatchUsdc } : {}) } };
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
