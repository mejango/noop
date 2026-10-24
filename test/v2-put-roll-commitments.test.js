'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const vm = require('node:vm');
const { declaration, SCRIPT_SOURCE, loadProduction } = require('./helpers/load-production');
const pricing = require('../bot/order-pricing');

const NOW = Date.parse('2030-09-01T20:00:00Z');
class Clock extends Date { static now() { return NOW; } }
const RETIRING = 'ETH-20300920-2000-P', REPLACEMENT = 'ETH-20301031-2000-P';
const criteria = { put_exit_intent: 'roll_protection', conditions: [{ field: 'dte', op: 'lte', value: 25 }] };
const position = (instrument_name, amount) => ({ instrument_name, amount, direction: 'long', avg_entry_price: 10 });
const pendingSale = (overrides = {}) => ({ order_id: 'replacement-sale', instrument_name: REPLACEMENT,
  action: 'sell_put', direction: 'sell', amount: 1, filled_amount: 0, limit_price: 120, order_status: 'open', ...overrides });

function fixture() {
  const state = { positions: [position(RETIRING, 4), position(REPLACEMENT, 4)], venue: [], tracked: [],
    bookReads: 0, events: [], placements: [] };
  const instruments = [RETIRING, REPLACEMENT].map(instrument_name => ({ instrument_name,
    price_step: 0.1, base_asset_address: 'fixture-asset', base_asset_sub_id: '1', option_details: { option_type: 'P', strike: 2000 } }));
  const ticker = { b: 5, a: 6, M: 5.5, I: 2500, option_pricing: { d: -0.1 } };
  const db = { getActiveRules: () => [{ id: 1, action: 'sell_put', criteria }],
    getOpenRestingOrders: () => state.tracked, insertOrder() {} };
  const bindings = {
    Date: Clock, db, botData: { putBudgetForCycle: 100, putUnspentBuyLimit: 0, putNetBought: 0 },
    fetchOpenOrders: async options => {
      assert.equal(options.throwOnError, true); state.events.push('orders'); state.bookReads++;
      return state.readBook ? state.readBook(state.bookReads) : state.venue;
    },
    fetchPositions: async options => {
      assert.equal(options.throwOnError, true); state.events.push('positions');
      return state.positions.map(p => ({ ...p }));
    },
    fetchFreshTickerForInstrument: async () => ticker,
    fetchSubaccount: async () => ({ initial_margin: 1000, maintenance_margin: 1200, subaccount_value: 5000, is_under_liquidation: false }),
    getCallMarginDecision: () => null, summarizeReservedEntryCapacity: () => ({ putBudget: 0 }),
  };
  const helpers = loadProduction(['createFinalOrderValidator', 'getActionPolicy', 'isReduceOnlyExitAction', 'isRestingOrderType',
    'getExistingRestingExitOrder', 'getSyntheticReduceOnlyPreflight', 'floorOrderAmountToVenuePrecision',
    'formatVenueOrderAmount', 'isVenueOrderAmountTradable', 'getSyntheticExitIntent', 'formatPostOnlyContext'], { bindings });
  state.validate = helpers.createFinalOrderValidator({ action: { action: 'sell_put', instrument_name: RETIRING, rule_id: 1 },
    instruments, spotPrice: 2500, triggerData: {}, ruleCriteria: criteria, orderType: 'ioc' });
  const execution = { ...helpers, ...pricing, ...bindings, process: { env: {} },
    console: { log() {}, error() {}, warn() {} }, DERIVE_ACCOUNT_ADDRESS: 'fixture-account',
    require: () => ({ assertNoUnresolvedSubmission() {}, noteSubmission() {} }),
    placeOrder: async (...args) => { state.placements.push(args); return { zero_fill_rejected: true }; },
  };
  const execute = vm.compileFunction(`${declaration(SCRIPT_SOURCE, 'executeOrder')}; return executeOrder;`, Object.keys(execution))(...Object.values(execution));
  state.run = (amount = 4, triggerData = { put_exit_intent: 'roll_protection' }) => execute('sell_put', RETIRING, amount, 5, instruments, 2500, 'ioc',
    { [RETIRING]: ticker }, null, { triggerData, ruleCriteria: criteria,
      validateOrder: state.validate });
  return state;
}

test('legacy final validator uses pending replacement sales and reads orders before positions', async () => {
  const state = fixture();
  state.venue = [pendingSale()]; state.tracked = [pendingSale()];
  assert.equal((await state.validate({ price: 5, amount: 4 })).code, 'replacement_failed');
  assert.equal((await state.validate({ price: 5, amount: 3 })).allowed, true);
  assert.deepEqual(state.events, ['orders', 'positions', 'orders', 'positions']);
  assert.equal((await state.run(3)).zeroFill, true);
  assert.equal(state.placements.length, 1, 'usable remaining protection still permits a smaller roll');
});

test('a fill between book and position reads cannot manufacture replacement capacity', async () => {
  const state = fixture();
  state.readBook = () => {
    state.positions = [position(RETIRING, 4), position(REPLACEMENT, 3)];
    return [pendingSale({ amount: 2, filled_amount: 1 })];
  };
  assert.equal((await state.validate({ price: 5, amount: 3 })).code, 'replacement_failed');
  assert.equal((await state.validate({ price: 5, amount: 2 })).allowed, true);
  assert.deepEqual(state.events, ['orders', 'positions', 'orders', 'positions']);
});

test('only terminal reconciliation releases a tracked replacement-sale reservation', async () => {
  const state = fixture();
  state.tracked = [pendingSale()]; // Venue no longer lists it, but accounting is unresolved.
  assert.equal((await state.validate({ price: 5, amount: 4 })).code, 'replacement_failed');
  state.tracked = []; // Terminal cancellation was reconciled without fills.
  assert.equal((await state.validate({ price: 5, amount: 4 })).allowed, true);
});

test('missing and malformed fresh books block rolls before submission', async () => {
  for (const result of [null, {}, [pendingSale({ direction: undefined, action: undefined })], [pendingSale({ filled_amount: undefined })]]) {
    const state = fixture(); state.readBook = () => result;
    const rejected = await state.run();
    assert.equal(rejected.failed, true);
    assert.equal(state.placements.length, 0);
  }
  const state = fixture(); state.readBook = () => { throw new Error('venue unavailable'); };
  assert.equal((await state.run()).failed, true);
  assert.equal(state.placements.length, 0);
});

test('last pre-send book can revoke a previously approved roll when a replacement sale appears', async () => {
  const state = fixture();
  state.readBook = read => read < 3 ? [] : [pendingSale()];
  assert.equal((await state.validate({ price: 5, amount: 4 })).allowed, true);
  const result = await state.run();
  assert.equal(result.failed, true);
  assert.match(result.reason, /uncommitted/);
  assert.equal(state.placements.length, 0);
  assert.equal(state.bookReads, 3, 'the final validator reuses the exact last book');
  assert.deepEqual(state.events, ['orders', 'positions', 'orders', 'positions', 'orders', 'positions']);
});

test('last pre-send refresh pairs a filled replacement sale with reduced held positions', async () => {
  const state = fixture();
  state.readBook = read => {
    if (read === 2) state.positions = [position(RETIRING, 4), position(REPLACEMENT, 3)];
    return []; // The one-unit sale filled and is no longer open.
  };
  const result = await state.run();
  assert.equal(result.failed, true);
  assert.equal(state.placements.length, 0);
  assert.deepEqual(state.events, ['orders', 'positions', 'orders', 'positions']);
});

test('last pre-send guard still blocks an exit on the retiring instrument', async () => {
  const state = fixture();
  state.readBook = read => read === 1 ? [] : [pendingSale({ instrument_name: RETIRING })];
  const result = await state.run();
  assert.equal(result.failed, true);
  assert.match(result.reason, /must be terminal and reconciled/);
  assert.equal(state.placements.length, 0);
});

test('historical trigger intent cannot bypass the active rule final roll check', async () => {
  const state = fixture();
  state.readBook = read => read === 1 ? [] : [pendingSale()];
  const result = await state.run(4, { put_exit_intent: 'monetize_tail_win' });
  assert.equal(result.failed, true);
  assert.match(result.reason, /uncommitted/);
  assert.equal(state.placements.length, 0);
});
