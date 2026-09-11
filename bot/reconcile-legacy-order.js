'use strict';

// Recovery for orders created before the durable submission journal existed.
// Importing this file opens no database and makes no network requests.
const { accountRestingObservation } = require('./order-accounting');
const ACTIONS = { buy_put: ['buy', 'P'], sell_call: ['sell', 'C'], buyback_call: ['buy', 'C'], sell_put: ['sell', 'P'] };
const TERMINAL = new Set(['filled', 'cancelled', 'expired', 'rejected']);
const close = (a, b) => Math.abs(a - b) <= 1e-7;
function number(value, label) {
  if (value == null || value === '' || !Number.isFinite(Number(value)) || Number(value) < 0) {
    throw new Error(`Missing or invalid ${label}`);
  }
  return Number(value);
}
function parse(value) {
  for (let depth = 0; depth < 2 && typeof value === 'string'; depth++) {
    try { value = JSON.parse(value); } catch { return null; }
  }
  return value && typeof value === 'object' ? value : null;
}
function receipt(value) {
  const raw = parse(value);
  const result = raw?.result || raw;
  const order = result?.order || result;
  return { order: order?.order_id ? order : null, trades: result?.trades };
}
function assertIdentity(order, expected, accountId) {
  if (!order?.order_id || order.order_id !== expected.order_id
    || String(order.subaccount_id) !== String(accountId)
    || order.instrument_name !== expected.instrument_name || order.direction !== expected.direction
    || !close(number(order.amount, 'order amount'), number(expected.amount, 'expected amount'))
    || !close(number(order.limit_price, 'order limit'), number(expected.limit_price, 'expected limit'))
    || !(Number(order.amount) > 0) || !(Number(order.limit_price) > 0)) {
    throw new Error('Venue and local order identity/account/terms conflict');
  }
  if (expected.nonce != null && String(order.nonce) !== String(expected.nonce)) throw new Error('Order nonce conflicts');
}
function tradeEvidence(trades, order, accountId) {
  if (!Array.isArray(trades)) throw new Error('Complete trade evidence is required');
  const byId = new Map();
  let amount = 0;
  let value = 0;
  for (const trade of trades) {
    if (!trade?.trade_id || byId.has(String(trade.trade_id)) || trade.order_id !== order.order_id
      || String(trade.subaccount_id) !== String(accountId)
      || trade.instrument_name !== order.instrument_name || trade.direction !== order.direction) {
      throw new Error('Trade identity is missing, duplicated, or conflicting');
    }
    const quantity = number(trade.trade_amount, 'trade amount');
    const price = number(trade.trade_price, 'trade price');
    if (!(quantity > 0) || !(price > 0)) throw new Error('Invalid trade amount/price');
    byId.set(String(trade.trade_id), { quantity, price });
    amount += quantity;
    value += quantity * price;
  }
  return { byId, amount, value };
}
function assertCumulative(order, evidence) {
  const filled = number(order.filled_amount, 'cumulative fill');
  const amount = number(order.amount, 'order amount');
  const value = filled > 0 ? filled * number(order.average_price, 'average fill price') : 0;
  if (!close(evidence.amount, filled) || !close(evidence.value, value) || filled > amount + 1e-9
    || (!TERMINAL.has(order.order_status) && !['open', 'untriggered'].includes(order.order_status))
    || (order.order_status === 'filled' && !close(filled, amount))
    || (order.order_status === 'rejected' && filled !== 0)
    || (['open', 'untriggered'].includes(order.order_status) && filled >= amount)) {
    throw new Error('Incomplete or conflicting cumulative venue evidence');
  }
}

function historicalBaseline(db, order, accountId, tracked, liveEvidence) {
  const rows = db.db.prepare('SELECT * FROM orders WHERE raw_response IS NOT NULL ORDER BY id').all()
    .map(row => ({ row, ...receipt(row.raw_response) })).filter(item => item.order?.order_id === order.order_id);
  const action = tracked?.action || rows[0]?.row.action;
  const semantics = ACTIONS[action];
  if (!semantics || semantics[0] !== order.direction || !order.instrument_name.endsWith(`-${semantics[1]}`)) {
    throw new Error('Original bot action is unavailable or conflicts with venue evidence');
  }
  if (!tracked && rows.length === 0) throw new Error('Untracked order has no original bot receipt; manual adoption is not supported');
  let bookedAmount = 0;
  let bookedValue = 0;
  const bookedTradeIds = new Set();
  const pendingIds = new Set(tracked?.pending_action_id != null ? [tracked.pending_action_id] : []);
  for (const { row, order: historical, trades } of rows) {
    assertIdentity(historical, order, accountId);
    if (row.action !== action || row.instrument_name !== order.instrument_name
      || !close(number(row.intended_amount, 'logged intended amount'), Number(order.amount))) {
      throw new Error('Historical bot action/quantity conflicts');
    }
    const quantity = number(row.filled_amount, 'booked fill amount');
    const value = number(row.total_value, 'booked fill value');
    if ((quantity > 0 && (row.success !== 1 || !close(value, quantity * number(row.fill_price, 'booked fill price'))))
      || (quantity === 0 && value !== 0)) throw new Error('Historical booked fill fields conflict');
    if (Array.isArray(trades)) {
      const historicalTrades = tradeEvidence(trades, historical, accountId);
      if (!close(historicalTrades.amount, quantity) || !close(historicalTrades.value, value)) {
        throw new Error('Historical booked fills do not match original trades');
      }
      for (const [id, trade] of historicalTrades.byId) {
        const live = liveEvidence.byId.get(id);
        if (bookedTradeIds.has(id) || !live || !close(live.quantity, trade.quantity) || !close(live.price, trade.price)) {
          throw new Error('Historical trades overlap or conflict with complete venue history');
        }
        bookedTradeIds.add(id);
      }
    } else {
      // The old resting-order writer logged a single cumulative status receipt.
      // Multiple cumulative receipts cannot prove non-overlapping bookings.
      if (rows.length !== 1 || !close(number(historical.filled_amount, 'historical cumulative fill'), quantity)
        || !close(quantity > 0 ? quantity * number(historical.average_price, 'historical average price') : 0, value)) {
        throw new Error('Historical cumulative receipt cannot prove prior booking');
      }
    }
    bookedAmount += quantity;
    bookedValue += value;
    if (row.pending_action_id != null) pendingIds.add(row.pending_action_id);
  }
  if (pendingIds.size > 1) throw new Error('Conflicting original pending actions');
  if (tracked && Number(tracked.filled_amount) > 0 && !close(Number(tracked.filled_amount), bookedAmount)) {
    throw new Error('Legacy partial watermark has no matching historical booking evidence');
  }
  if (bookedAmount > liveEvidence.amount + 1e-7 || bookedValue > liveEvidence.value + 1e-7
    || (close(bookedAmount, liveEvidence.amount) && !close(bookedValue, liveEvidence.value))) {
    throw new Error('Historical booking exceeds or conflicts with venue cumulative fills');
  }
  return { action, bookedAmount, bookedValue, pendingActionId: [...pendingIds][0] ?? null, sourceOrderIds: rows.map(item => item.row.id) };
}

function reconcileLegacyOrder({ db, orderId, accountId, order, trades }) {
  if (!orderId || accountId == null) throw new Error('Exact order ID and configured account are required');
  const identity = { ...order, order_id: orderId };
  assertIdentity(order, identity, accountId);
  const evidence = tradeEvidence(trades, order, accountId);
  assertCumulative(order, evidence);
  return db.db.transaction(() => {
    const hasJournal = db.db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_submissions'").get();
    if (hasJournal && db.db.prepare("SELECT 1 FROM execution_submissions WHERE status IN ('unknown','acknowledged') LIMIT 1").get()) {
      throw new Error('Resolve the journaled submission using reconcile-execution.js first');
    }
    db.db.exec(`CREATE TABLE IF NOT EXISTS legacy_order_recoveries (
      order_id TEXT PRIMARY KEY, account_id TEXT NOT NULL, baseline_amount REAL NOT NULL,
      baseline_value REAL NOT NULL, source_order_ids TEXT NOT NULL, evidence_json TEXT NOT NULL,
      recovered_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    )`);
    let tracked = db.db.prepare('SELECT * FROM resting_orders WHERE order_id=?').get(orderId);
    if (tracked) assertIdentity(order, tracked, accountId);
    const previous = db.db.prepare('SELECT * FROM legacy_order_recoveries WHERE order_id=?').get(orderId);
    if (previous && (!tracked || previous.account_id !== String(accountId))) throw new Error('Recovery identity is inconsistent');
    if (!previous) {
      const baseline = historicalBaseline(db, order, accountId, tracked, evidence);
      if (tracked?.filled_value != null && !close(Number(tracked.filled_value), baseline.bookedValue)) {
        throw new Error('Known accounting watermark conflicts with historical booking');
      }
      if (!tracked) {
        db.insertRestingOrder({ order_id: orderId, pending_action_id: baseline.pendingActionId,
          instrument_name: order.instrument_name, action: baseline.action, direction: order.direction,
          amount: Number(order.amount), limit_price: Number(order.limit_price),
          filled_amount: baseline.bookedAmount, filled_value: baseline.bookedValue });
      } else {
        // Restore only the proven booked watermark. Current status is reconciled
        // below; a legacy cancellation flag was not affirmative terminal evidence.
        db.db.prepare("UPDATE resting_orders SET filled_amount=?, filled_value=?, status='open' WHERE order_id=?")
          .run(baseline.bookedAmount, baseline.bookedValue, orderId);
      }
      db.db.prepare(`INSERT INTO legacy_order_recoveries
        (order_id,account_id,baseline_amount,baseline_value,source_order_ids,evidence_json) VALUES (?,?,?,?,?,?)`)
        .run(orderId, String(accountId), baseline.bookedAmount, baseline.bookedValue,
          JSON.stringify(baseline.sourceOrderIds), JSON.stringify({ order, trades }));
      tracked = db.db.prepare('SELECT * FROM resting_orders WHERE order_id=?').get(orderId);
    }
    const state = db.loadBotState();
    const spent = number(state?.put_net_bought, 'saved put budget');
    // Recovery preserves all other state, including cycle dates and margin data.
    const recoveryDb = { ...db, saveBotState(data) {
      db.db.prepare('UPDATE bot_state SET put_net_bought=? WHERE id=1').run(data.putNetBought);
    } };
    const result = accountRestingObservation({ db: recoveryDb, botData: { putNetBought: spent }, tracked, live: order });
    return { orderId, ...result, alreadyRecovered: Boolean(previous) };
  }).immediate();
}

async function collectLegacyEvidence({ read, orderId, accountId }) {
  let order;
  try {
    const result = await read('get_order', { subaccount_id: accountId, order_id: orderId });
    order = result?.order || result;
  } catch (error) {
    const pages = new Set();
    for (let page = 1; page <= 100; page++) {
      const result = await read('get_order_history', { subaccount_id: accountId, from_timestamp: 0, page, page_size: 100 });
      const rows = Array.isArray(result) ? result : result?.orders;
      if (!Array.isArray(rows)) throw new Error('Order history unavailable');
      const matches = rows.filter(row => row.order_id === orderId);
      if (matches.length > 1) throw new Error('Duplicate venue order identity');
      if (matches.length) { order = matches[0]; break; }
      const lastPage = Number(result?.pagination?.num_pages);
      if ((Number.isSafeInteger(lastPage) && lastPage >= 0 && page >= lastPage) || rows.length < 100) break;
      const key = rows.map(row => row.order_id).join(',');
      if (pages.has(key)) throw new Error('Order history pagination did not advance');
      pages.add(key);
    }
    if (!order || !TERMINAL.has(order.order_status)) throw error;
  }
  if (!order?.order_id || order.order_id !== orderId || String(order.subaccount_id) !== String(accountId)) {
    throw new Error('Exact venue order evidence is unavailable');
  }
  const trades = [];
  const pages = new Set();
  for (let page = 1; page <= 100; page++) {
    const result = await read('get_trade_history', { subaccount_id: accountId, order_id: orderId, instrument_name: order.instrument_name,
      from_timestamp: 0, page, page_size: 100 });
    const rows = Array.isArray(result) ? result : result?.trades;
    if (!Array.isArray(rows)) throw new Error('Trade history unavailable');
    trades.push(...rows.filter(trade => trade.order_id === orderId));
    const lastPage = Number(result?.pagination?.num_pages);
    if ((Number.isSafeInteger(lastPage) && lastPage >= 0 && page >= lastPage) || rows.length < 100) return { order, trades };
    const key = rows.map(trade => trade.trade_id).join(',');
    if (pages.has(key)) throw new Error('Trade history pagination did not advance');
    pages.add(key);
  }
  throw new Error('Trade history scan incomplete');
}

async function main(argv = process.argv.slice(2)) {
  const at = argv.indexOf('--order');
  const orderId = at < 0 ? null : argv[at + 1];
  if (!argv.includes('--bot-stopped') || !orderId || orderId.startsWith('--')) {
    throw new Error('Stop the bot, then run: node bot/reconcile-legacy-order.js --order ORDER_ID --bot-stopped');
  }
  const fs = require('node:fs');
  const path = require('node:path');
  // Read literal deployment identity without importing the bot's startup code.
  const source = fs.readFileSync(path.join(__dirname, '../script.js'), 'utf8');
  const accountAddress = /^const DERIVE_ACCOUNT_ADDRESS = '(0x[\da-fA-F]{40})';$/m.exec(source)?.[1];
  const accountId = Number(/^const SUBACCOUNT_ID = (\d+);$/m.exec(source)?.[1]);
  if (!accountAddress || !Number.isSafeInteger(accountId) || accountId <= 0) throw new Error('Configured account identity unavailable');
  const databasePath = process.env.NOOP_DB_PATH || path.join(process.env.DATA_DIR || path.join(__dirname, '../data'), 'noop.db');
  if (!fs.existsSync(databasePath) || !fs.statSync(databasePath).isFile()) throw new Error('Existing bot database is required for legacy recovery');
  const db = require('./db');
  try {
    const { Wallet } = require('ethers');
    const wallet = new Wallet((process.env.PRIVATE_KEY || fs.readFileSync('./.private_key.txt', 'utf8')).trim());
    const axios = require('axios');
    const read = async (method, params) => {
      if (!['get_order', 'get_order_history', 'get_trade_history'].includes(method)) throw new Error('Only read-only recovery endpoints are allowed');
      const timestamp = Date.now();
      const response = await axios.post(`https://api.lyra.finance/private/${method}`, params, {
        headers: { 'X-LyraWallet': accountAddress, 'X-LyraTimestamp': String(timestamp),
          'X-LyraSignature': await wallet.signMessage(String(timestamp)) }, timeout: 15000 });
      if (response.data?.error || !response.data?.result) throw new Error(`Venue ${method} evidence unavailable`);
      return response.data.result;
    };
    const evidence = await collectLegacyEvidence({ read, orderId, accountId });
    console.log(JSON.stringify(reconcileLegacyOrder({ db, orderId, accountId, ...evidence }), null, 2));
  } finally { db.close(); }
}

if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode = 1; });
module.exports = { reconcileLegacyOrder, collectLegacyEvidence, main };
