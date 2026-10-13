'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const Database = require('better-sqlite3');
const { declaration, loadProduction, SCRIPT_SOURCE } = require('./helpers/load-production');

const root = path.resolve(__dirname, '..');
const quiet = { log() {}, warn() {}, error() {} };
const instrumentName = 'ETH-20301127-2100-P';
const order = { order_id: 'entry-1', action: 'buy_put', instrument_name: instrumentName,
  direction: 'buy', amount: 1.18, filled_amount: 0, filled_value: 0, average_price: 0,
  limit_price: 34.5, status: 'open', order_status: 'open', creation_timestamp: Date.now() };
const cancellation = { action: 'buy_put', amount: order.amount, price: order.limit_price,
  reason: 'Repricing entry from $34.50 to $34.80.',
  nextStep: 'Revalidate the replacement price, remaining size and budget before submitting.' };

function memoryDatabase(t) {
  const filename = path.join(root, 'bot/db.js');
  const localRequire = createRequire(filename);
  const context = { __dirname: path.dirname(filename), module: { exports: {} }, console: quiet,
    process: { env: { DATA_DIR: os.tmpdir() } }, require(name) {
      if (name === 'better-sqlite3') return function InMemoryDatabase() { return new Database(':memory:'); };
      return localRequire(name);
    } };
  vm.runInNewContext(fs.readFileSync(filename, 'utf8'), context, { filename });
  t.after(() => context.module.exports.close());
  return context.module.exports;
}

function fixture(t, orderOverrides = {}) {
  const messages = [];
  const db = memoryDatabase(t);
  const botData = { putNetBought: 0, putBudgetForCycle: 100, putUnspentBuyLimit: 0 };
  db.saveBotState(botData);
  db.insertRestingOrder({ ...order, ...orderOverrides });
  const context = { ...loadProduction(['isEntryAction', 'isReduceOnlyExitAction', 'inferActionFromOpenOrder']),
    require: createRequire(path.join(root, 'script.js')), db, botData, console: quiet, process: { env: {} },
    DERIVE_CONFIG: { maintenance: false, headerPrefix: 'Test' }, SUBACCOUNT_ID: 1,
    DERIVE_ACCOUNT_ADDRESS: 'fixture', API_URL: { CANCEL_ORDER: 'fixture:cancel' },
    createWallet: () => ({}), signMessage: async () => 'fixture-signature', stringifyApiError: JSON.stringify,
    axios: { post: async () => ({ data: { result: { success: true } } }) },
    sendTelegram: message => messages.push(message), fetchOpenOrders: async () => [{ ...order, ...orderOverrides }],
    fetchOrderStatus: async () => null, getRestingExitInvalidReason: () => null,
    buildRollingOptionValueContext: () => ({}), venueHasNonce: async () => false,
  };
  const names = ['ORDER_NOTIFICATION_LABELS', 'formatOrderNotificationNumber', 'formatOrderLifecycleNotification',
    'orderCancellationNotificationContext', 'notifyOrderLifecycle', 'cancelOrder',
    'accountEntryOrderObservation', 'accountExitOrderObservation', 'manageOpenOrders'];
  vm.createContext(context);
  vm.runInContext(`${names.map(name => declaration(SCRIPT_SOURCE, name)).join('\n')}\nObject.assign(globalThis, { ${names.join(', ')} });`, context);
  return { context, messages, db };
}

test('cancellation ACK explains the reason and pending confirmation without claiming a terminal status', async t => {
  const { context, messages } = fixture(t);
  await context.cancelOrder(order.order_id, instrumentName, cancellation);
  assert.equal(messages.length, 1);
  assert.match(messages[0], /CANCELLATION REQUEST ACKNOWLEDGED: BUY PUT/);
  assert.match(messages[0], /Status: awaiting terminal confirmation/);
  assert.match(messages[0], /Reason: Repricing entry from \$34\.50 to \$34\.80/);
  assert.match(messages[0], /reserved capacity stays held/);
  assert.doesNotMatch(messages[0], /ORDER CANCELLED|Status: cancelled/);
  assert.equal(context.db.getOpenRestingOrders().length, 1, 'Acknowledgement cannot release reserved capacity');

  await context.cancelOrder(order.order_id, instrumentName, cancellation);
  assert.equal(messages.length, 1, 'Request retries do not repeat Telegram alerts');
});

test('only confirmed terminal entry transitions alert once, including stale repeated snapshots', async t => {
  const { context, messages } = fixture(t);
  await context.cancelOrder(order.order_id, instrumentName, cancellation);
  const terminal = { ...order, order_status: 'cancelled', cancel_reason: 'user_request' };
  const first = context.accountEntryOrderObservation({ ...order }, terminal);
  const repeated = context.accountEntryOrderObservation({ ...order }, terminal);
  assert.equal(first.statusChanged, true);
  assert.equal(repeated.statusChanged, false);
  assert.equal(messages.length, 2);
  assert.match(messages[1], /ORDER CANCELLED: BUY PUT/);
  assert.match(messages[1], /Reason: Repricing entry from \$34\.50 to \$34\.80/);
  assert.match(messages[1], /Next: Revalidate the replacement price, remaining size and budget before submitting/);
  assert.doesNotMatch(messages[1], /awaiting terminal confirmation|Await terminal confirmation/);
  assert.equal(context.orderCancellationNotificationContext.size, 0);
});

test('exit snapshots report a confirmed zero-fill cancellation once and clear pending alert context', async t => {
  const exit = { ...order, action: 'sell_put', direction: 'sell' };
  const { context, messages } = fixture(t, exit);
  await context.cancelOrder(exit.order_id, instrumentName, { ...cancellation, action: exit.action,
    reason: 'Exit no longer satisfies the active rule.' });
  const terminal = { ...exit, order_status: 'cancelled' };
  context.accountExitOrderObservation({ ...exit }, terminal);
  context.accountExitOrderObservation({ ...exit }, terminal);
  assert.equal(messages.length, 2);
  assert.match(messages[1], /ORDER CANCELLED: SELL PUT/);
  assert.match(messages[1], /Reason: Exit no longer satisfies the active rule/);
  assert.equal(context.orderCancellationNotificationContext.size, 0);
  assert.equal(context.botData.putNetBought, 0);
});

test('failed or empty cancellation responses never send an acknowledgement alert', async t => {
  for (const reply of [null, { error: { message: 'Rejected' } }, { result: null }]) {
    const { context, messages } = fixture(t);
    context.axios.post = async () => ({ data: reply });
    assert.ok(await context.cancelOrder(order.order_id, instrumentName, cancellation) == null);
    assert.deepEqual(messages, []);
    assert.equal(context.orderCancellationNotificationContext.size, 0);
  }
  const { context, messages } = fixture(t);
  context.axios.post = async () => { throw new Error('Request timed out'); };
  assert.equal(await context.cancelOrder(order.order_id, instrumentName, cancellation), null);
  assert.deepEqual(messages, []);
});

test('terminal notifications after restart use only available venue evidence', t => {
  const { context, messages } = fixture(t);
  context.accountEntryOrderObservation({ ...order }, { ...order, order_status: 'cancelled' });
  assert.match(messages[0], /cancellation reason unavailable/);
  assert.doesNotMatch(messages[0], /manually|user requested|Repricing/);
});

test('the order manager propagates a missing-rule cancellation reason and waits for confirmation', async t => {
  const { context, messages } = fixture(t);
  await context.manageOpenOrders({}, [], [], 2500);
  assert.equal(messages.length, 1);
  assert.match(messages[0], /Reason: orphaned \(no matching active rule\)/);
  assert.match(messages[0], /Wait for a matching active rule before placing another order/);
  assert.equal(context.db.getOpenRestingOrders().length, 1);
  context.fetchOpenOrders = async () => [];
  context.fetchOrderStatus = async () => ({ ...order, order_status: 'cancelled' });
  await context.manageOpenOrders({}, [], [], 2500);
  await context.manageOpenOrders({}, [], [], 2500);
  assert.equal(messages.length, 2);
  assert.match(messages[1], /ORDER CANCELLED/);
  assert.match(messages[1], /Reason: orphaned \(no matching active rule\)/);
});

test('the order manager explains the failed economic condition on an invalid put bid', async t => {
  const { context, messages } = fixture(t);
  context.db.replaceActiveRules('fixture', [{ rule_type: 'entry', action: 'buy_put', criteria: {} }]);
  context.validateRestingBuyPutEntryOrder = () => ({ valid: false, reason: 'PUT EDGE 62 below minimum 65' });
  await context.manageOpenOrders({}, [], [], 2500);
  assert.equal(messages.length, 1);
  assert.match(messages[0], /Reason: resting buy_put no longer satisfies active rule: PUT EDGE 62 below minimum 65/);
  assert.match(messages[0], /Re-evaluate eligible orders against current rules and available capacity/);
});

test('expiration and late fills retain accurate lifecycle status and incremental accounting', t => {
  const { context, messages } = fixture(t);
  const live = { ...order, filled_amount: 0.25, average_price: 34 };
  const partial = context.accountEntryOrderObservation({ ...order }, live);
  assert.equal(partial.statusChanged, false);
  assert.equal(partial.deltaAmount, 0.25);
  const expired = context.accountEntryOrderObservation({ ...order }, { ...live, order_status: 'expired' });
  assert.equal(expired.statusChanged, true);
  assert.equal(expired.deltaAmount, 0);
  assert.equal(messages.length, 2);
  assert.match(messages[0], /PARTIALLY EXECUTED/);
  assert.match(messages[1], /ORDER EXPIRED/);
  assert.doesNotMatch(messages[1], /Filled: 0/);
  assert.equal(context.botData.putNetBought, 8.5);
});

test('an already-accounted full fill transitioning to terminal reports execution once, never cancellation', async t => {
  for (const reconcile of ['entry', 'manager']) {
    const { context, messages, db } = fixture(t);
    // Covers migrated/recovered rows whose complete fills were already booked
    // before their final lifecycle status arrived.
    const value = order.amount * 34;
    db.updateRestingOrder(order.order_id, 'open', order.amount, value);
    context.botData.putNetBought = value;
    db.saveBotState(context.botData);
    const terminal = { ...order, order_status: 'filled', filled_amount: order.amount, average_price: 34 };
    if (reconcile === 'entry') {
      context.accountEntryOrderObservation({ ...order }, terminal);
      context.accountEntryOrderObservation({ ...order }, terminal);
    } else {
      context.fetchOpenOrders = async () => [];
      context.fetchOrderStatus = async () => terminal;
      await context.manageOpenOrders({}, [], [], 2500);
      await context.manageOpenOrders({}, [], [], 2500);
    }
    assert.equal(messages.length, 1);
    assert.match(messages[0], /ORDER EXECUTED: BOUGHT PUT/);
    assert.match(messages[0], /Filled: 1\.18 \/ 1\.18 @ \$34/);
    assert.doesNotMatch(messages[0], /CANCELLED|Filled: 0/);
    assert.equal(context.botData.putNetBought, value, 'Already-accounted fills never change the budget again');
  }
});
