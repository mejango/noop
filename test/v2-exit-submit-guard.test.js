'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const vm = require('node:vm');
const { declaration, SCRIPT_SOURCE, loadProduction } = require('./helpers/load-production');
const pricing = require('../bot/order-pricing');
const name = 'ETH-20300918-2800-C';
const instrument = { instrument_name: name, price_step: 0.1, base_asset_address: 'fixture-asset',
  base_asset_sub_id: '1', option_details: { option_type: 'call' } };
const existing = { order_id: 'competing-exit', instrument_name: name, direction: 'buy',
  amount: 2, filled_amount: 0, limit_price: 2.2, order_status: 'open' };

function fixture({ openOrders, trackedOrders = [], retry = false }) {
  const events = [], placements = [];
  let reads = 0, tickers = 0;
  const pure = loadProduction(['getActionPolicy', 'isReduceOnlyExitAction', 'isRestingOrderType',
    'getExistingRestingExitOrder', 'getSyntheticReduceOnlyPreflight', 'floorOrderAmountToVenuePrecision',
    'formatVenueOrderAmount', 'isVenueOrderAmountTradable', 'getSyntheticExitIntent', 'formatPostOnlyContext']);
  const bindings = {
    ...pure, ...pricing, process: { env: {} }, console: { log() {}, error() {}, warn() {} },
    db: { getOpenRestingOrders: () => trackedOrders, insertOrder() {} },
    botData: {}, DERIVE_ACCOUNT_ADDRESS: 'fixture-account',
    require: () => ({ assertNoUnresolvedSubmission() {}, noteSubmission() {} }),
    fetchPositions: async () => [{ instrument_name: name, direction: 'short', amount: 2 }],
    fetchFreshTickerForInstrument: async () => ({ b: 1.8, a: ++tickers > 1 ? 2.1 : 4 }),
    fetchOpenOrders: async options => { assert.equal(options.throwOnError, true); events.push('orders'); return openOrders(++reads); },
    placeOrder: async (...args) => { placements.push(args); return retry ? { rejected_post_only: true } : { zero_fill_rejected: true }; },
  };
  const execute = vm.compileFunction(`${declaration(SCRIPT_SOURCE, 'executeOrder')}; return executeOrder;`, Object.keys(bindings))(...Object.values(bindings));
  const run = orderType => execute('buyback_call', name, 2, 2.2, [instrument], 2500, orderType,
    { [name]: { a: 4, b: 1.8 } }, null, {
      triggerData: { buyback_intent: 'profit_capture' },
      validateOrder: async () => { events.push('policy'); return { allowed: true }; },
    });
  return { run, events, placements };
}

test('IOC exit checks the live book after final policy reads and refuses a new competing exit', async () => {
  const state = fixture({ openOrders: () => [existing] });
  const result = await state.run('ioc');
  assert.equal(result.failed, true);
  assert.match(result.reason, /must be terminal and reconciled/);
  assert.deepEqual(state.events, ['policy', 'orders']);
  assert.equal(state.placements.length, 0);
});

test('IOC exit keeps a locally unresolved reservation even when the live open list is empty', async () => {
  const state = fixture({ openOrders: () => [], trackedOrders: [{ ...existing, action: 'buyback_call', status: 'open' }] });
  const result = await state.run('ioc');
  assert.equal(result.failed, true);
  assert.equal(state.placements.length, 0);
});

test('resting exit repeats the duplicate check after its initial synthetic preflight and policy reads', async () => {
  const state = fixture({ openOrders: read => read === 1 ? [] : [existing] });
  const result = await state.run('post_only');
  assert.equal(result.failed, true);
  assert.deepEqual(state.events, ['orders', 'policy', 'orders']);
  assert.equal(state.placements.length, 0);
});

test('maker retry cannot overlap an exit that appeared after the first post-only rejection', async () => {
  const state = fixture({ openOrders: read => read < 3 ? [] : [existing], retry: true });
  const result = await state.run('post_only');
  assert.equal(result.postOnlyRejected, true);
  assert.equal(state.placements.length, 1);
  assert.deepEqual(state.events, ['orders', 'policy', 'orders', 'policy', 'orders']);
});

test('exit submission defers when the final open-order read fails', async () => {
  const state = fixture({ openOrders: () => { throw new Error('venue order state unavailable'); } });
  const result = await state.run('ioc');
  assert.equal(result.failed, true);
  assert.match(result.reason, /venue order state unavailable/);
  assert.equal(state.placements.length, 0);
});
