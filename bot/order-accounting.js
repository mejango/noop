'use strict';

const OPEN = new Set(['open', 'untriggered']);
const TERMINAL = new Set(['filled', 'cancelled', 'expired', 'rejected']);
const numeric = (value, name) => {
  if (value == null || value === '' || !Number.isFinite(Number(value)) || Number(value) < 0) {
    throw new Error(`Unknown or invalid order ${name}; reconciliation required`);
  }
  return Number(value);
};

function receiptState(record, expected) {
  if (!record?.order_id || (expected.order_id && record.order_id !== expected.order_id)
    || (record.instrument_name && record.instrument_name !== expected.instrument_name)
    || (record.direction && record.direction !== expected.direction)
    || (record.amount != null && Math.abs(numeric(record.amount, 'amount') - Number(expected.amount)) > 1e-9)) {
    throw new Error('Order receipt identity mismatch; reconciliation required');
  }
  if (!OPEN.has(record.order_status) && !TERMINAL.has(record.order_status)) {
    throw new Error(`Unknown order lifecycle ${record.order_status}; reconciliation required`);
  }
  const filled = numeric(record.filled_amount, 'filled amount');
  if (filled > Number(expected.amount) + 1e-9
    || (record.order_status === 'filled' && Math.abs(filled - Number(expected.amount)) > 1e-9)
    || (record.order_status === 'rejected' && filled > 0)
    || (OPEN.has(record.order_status) && filled >= Number(expected.amount))) {
    throw new Error('Invalid cumulative order fills; reconciliation required');
  }
  return { filled, resting: OPEN.has(record.order_status) };
}

function commit(db, botData, fn) {
  if (!db?.db?.transaction) throw new Error('Transactional execution database required');
  const previousBudget = botData.putNetBought;
  try { return db.db.transaction(fn)(); }
  catch (error) { botData.putNetBought = previousBudget; throw error; }
}

// An acknowledged venue order and its local fill accounting must commit together.
// This small durable latch also survives process death after sending an order.
function executionStore(db) {
  if (!db?.db?.prepare) throw new Error('Execution database unavailable');
  db.db.exec(`CREATE TABLE IF NOT EXISTS execution_submissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pending_action_id INTEGER,
    instrument_name TEXT NOT NULL,
    nonce TEXT NOT NULL,
    request_json TEXT NOT NULL,
    response_json TEXT,
    status TEXT NOT NULL DEFAULT 'unknown',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
  )`);
  db.db.exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_single_unresolved ON execution_submissions ((1)) WHERE status IN ('unknown','acknowledged')");
  return db.db;
}

function assertNoUnresolvedSubmission(db) {
  const row = executionStore(db).prepare("SELECT id,instrument_name,nonce FROM execution_submissions WHERE status IN ('unknown','acknowledged') ORDER BY id LIMIT 1").get();
  if (row) throw new Error(`Execution ${row.id} (${row.instrument_name}, nonce ${row.nonce}) requires accounting recovery before trading`);
}

function beginSubmission(db, pendingActionId, request) {
  assertNoUnresolvedSubmission(db);
  return Number(executionStore(db).prepare(`INSERT INTO execution_submissions
    (pending_action_id,instrument_name,nonce,request_json) VALUES (?,?,?,?)`)
    .run(pendingActionId ?? null, request.instrument_name, String(request.nonce), JSON.stringify(request)).lastInsertRowid);
}

function noteSubmission(db, submissionId, result, definitive = false) {
  if (submissionId == null) return;
  const status = definitive ? 'rejected' : result?.result ? 'acknowledged' : 'unknown';
  executionStore(db).prepare('UPDATE execution_submissions SET status=?, response_json=? WHERE id=?')
    .run(status, JSON.stringify(result), submissionId);
}

function accountInitialReceipt({ db, botData, action, instrumentName, amount, price, orderType,
  pendingActionId, instrument = {}, spotPrice, order, record, trades = [], exitIntent = null, approvedLimitPrice = price, submissionId = null }) {
  const state = receiptState(record, { instrument_name: instrumentName, amount, direction: action.startsWith('buy') ? 'buy' : 'sell' });
  if (state.resting && orderType === 'ioc') throw new Error('IOC receipt remains open; reconciliation required');
  let filledAmt = 0, totalValue = 0;
  for (const trade of trades) {
    const quantity = numeric(trade.trade_amount, 'trade amount');
    const fillPrice = numeric(trade.trade_price, 'trade price');
    if (!(quantity > 0) || !(fillPrice > 0)) throw new Error('Invalid trade receipt; reconciliation required');
    filledAmt += quantity;
    totalValue += quantity * fillPrice;
  }
  if (Math.abs(state.filled - filledAmt) > 1e-9 || !Number.isFinite(totalValue)) {
    throw new Error('Order fills do not match trade receipts; reconciliation required');
  }
  const avgPx = filledAmt > 0 ? totalValue / filledAmt : Number(price);
  const result = state.resting
    ? { resting: true, orderId: record.order_id, action, instrumentName, amount, price, orderType, filledAmt, avgPx, totalValue }
    : filledAmt > 0 ? { filledAmt, avgPx, totalValue, order, orderType, orderId: record.order_id }
      : { zeroFill: true, action, instrumentName, amount, price, orderType, orderId: record.order_id };
  commit(db, botData, () => {
    if (submissionId != null) {
      const changed = executionStore(db).prepare("UPDATE execution_submissions SET status='accounted' WHERE id=? AND status IN ('unknown','acknowledged')").run(submissionId).changes;
      if (changed !== 1) throw new Error('Submission already accounted or missing; refusing duplicate fill accounting');
    }
    if (action === 'buy_put') botData.putNetBought += totalValue;
    db.saveBotState(botData);
    if (state.resting) db.insertRestingOrder({ order_id: record.order_id,
      pending_action_id: pendingActionId, instrument_name: instrumentName, action,
      direction: record.direction || (action.startsWith('buy') ? 'buy' : 'sell'), amount,
      limit_price: price, filled_amount: filledAmt, filled_value: totalValue,
      exit_intent: exitIntent, approved_limit_price: approvedLimitPrice });
    if (filledAmt > 0 || !state.resting) db.insertOrder({ action, success: filledAmt > 0,
      reason: `${record.order_status} [${orderType}]`, instrument_name: instrumentName,
      pending_action_id: pendingActionId, strike: instrument.option_details?.strike || null,
      expiry: instrument.option_details?.expiry || null, delta: null, price,
      intended_amount: amount, filled_amount: filledAmt, fill_price: filledAmt > 0 ? avgPx : null,
      total_value: totalValue, spot_price: spotPrice, raw_response: order });
    if (pendingActionId != null) db.updatePendingAction(pendingActionId, {
      status: state.resting ? 'resting' : filledAmt > 0 ? 'executed' : 'failed',
      executed_at: new Date().toISOString(), execution_result: result });
  });
  return result;
}

function accountRestingObservation({ db, botData, tracked, live }) {
  const result = commit(db, botData, () => {
    // The database watermark, not a caller's snapshot, owns idempotence.
    const saved = db.db.prepare('SELECT * FROM resting_orders WHERE order_id = ?').get(tracked.order_id);
    if (!saved) throw new Error('Missing resting order accounting record');
    const state = receiptState(live, saved);
    const accountedAmount = numeric(saved.filled_amount ?? 0, 'accounted amount');
    const accountedValue = numeric(saved.filled_value ?? (accountedAmount === 0 ? 0 : null), 'accounted value');
    const averagePrice = state.filled > 0 ? numeric(live.average_price, 'average price') : 0;
    const totalValue = state.filled * averagePrice;
    const deltaAmount = state.filled - accountedAmount;
    const deltaValue = totalValue - accountedValue;
    if (!Number.isFinite(totalValue) || deltaAmount < -1e-9 || deltaValue < -1e-7
      || (state.filled > 0 && !(averagePrice > 0))
      || (deltaAmount <= 1e-9 && Math.abs(deltaValue) > 1e-7)) {
      throw new Error('Regressing or inconsistent cumulative order fills; reconciliation required');
    }
    const status = state.resting ? 'open' : live.order_status === 'filled' ? 'filled' : 'cancelled';
    if (saved.status !== 'open') {
      if (status !== saved.status || Math.abs(deltaAmount) > 1e-9 || Math.abs(deltaValue) > 1e-7) {
        throw new Error('Conflicting terminal order evidence; reconciliation required');
      }
      return { resting: false, deltaAmount: 0, deltaValue: 0, status, filledAmount: state.filled, filledValue: totalValue };
    }
    if (state.resting && deltaAmount <= 1e-9) return { resting: true, deltaAmount: 0, deltaValue: 0,
      status, filledAmount: state.filled, filledValue: totalValue };
    if (saved.action === 'buy_put') botData.putNetBought += Math.max(0, deltaValue);
    db.saveBotState(botData);
    db.updateRestingOrder(saved.order_id, status, state.filled, totalValue);
    if (saved.pending_action_id != null) db.updatePendingAction(saved.pending_action_id, {
      status: state.resting ? 'resting' : state.filled > 0 ? 'executed' : 'cancelled',
      ...(!state.resting ? { executed_at: new Date().toISOString() } : {}),
      execution_result: { orderId: saved.order_id, orderStatus: live.order_status,
        filledAmount: state.filled, fillPrice: averagePrice, resting: state.resting } });
    if (deltaAmount > 1e-9 || (!state.resting && state.filled === 0)) db.insertOrder({
      action: saved.action, success: deltaAmount > 0,
      reason: `Resting order ${live.order_status}; incremental fill ${deltaAmount}, cumulative ${state.filled}/${saved.amount}`,
      instrument_name: saved.instrument_name, pending_action_id: saved.pending_action_id ?? null,
      strike: null, expiry: null, delta: null, price: saved.limit_price,
      intended_amount: saved.amount, filled_amount: Math.max(0, deltaAmount),
      fill_price: deltaAmount > 1e-9 ? deltaValue / deltaAmount : null,
      total_value: Math.max(0, deltaValue), spot_price: null, raw_response: live });
    return { resting: state.resting, deltaAmount: Math.max(0, deltaAmount), deltaValue: Math.max(0, deltaValue),
      status, filledAmount: state.filled, filledValue: totalValue };
  });
  Object.assign(tracked, { status: result.status, filled_amount: result.filledAmount, filled_value: result.filledValue });
  return result;
}

module.exports = { accountInitialReceipt, accountRestingObservation, assertNoUnresolvedSubmission, beginSubmission, noteSubmission };
