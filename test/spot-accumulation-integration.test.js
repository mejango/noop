'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const Database = require('better-sqlite3');
const { Wallet } = require('ethers');
const { getDeriveConfig } = require('../bot/derive-config');
const { USDC_ASSET_ADDRESS } = require('../bot/derive-transfer');
const { normalizeV2Trade } = require('../bot/economic-events');
const { toUnits, fromUnits } = require('../bot/spot-accumulation');
const { createSpotVenue } = require('../bot/spot-accumulation-venue');
const { createSpotAccumulationRuntime } = require('../bot/spot-accumulation-runtime');
const accounting = require('../bot/order-accounting');

const NOW = Date.parse('2026-11-10T16:00:00Z');
const OWNER = '0x' + '11'.repeat(20);
const UUID = '12345678-1234-1234-1234-123456789012';

function fixture(t) {
  const db = { db: new Database(':memory:') };
  t.after(() => db.db.close());
  const wallet = Wallet.createRandom(); // Test-only signer; no credentials or network.
  const config = { network: 'mainnet', apiVersion: 'v3', accountId: '25923', destinationAccountId: '120934',
    accountAddress: OWNER, activationTimestamp: '2026-11-10T15:00:00.000Z', maxBatchUsdc: '50' };
  const derive = { ...getDeriveConfig({ DERIVE_API_VERSION: 'v3', DERIVE_NETWORK: 'mainnet', DERIVE_WALLET: OWNER,
    DERIVE_SUBACCOUNT_ID: '25923', DERIVE_HISTORY_FROM: config.activationTimestamp, DERIVE_MAINTENANCE: 'false' }),
    accountAddress: OWNER, destinationSubaccountId: 120934 };
  const state = { mode: 'live', source: toUnits('1000'), destination: toUnits('5.01'), calls: [], tickerReads: 0,
    staleTickerRead: 0, settled: true, loseTransferAck: false, transfer: null, order: null, trades: [],
    orderError: null, orderErrorHttp: false, orderTimeout: false };
  const post = async (method, p) => {
    state.calls.push({ method, params: structuredClone(p) });
    switch (method) {
      case 'public/get_instrument': return { instrument_name: 'DRV-USDC', instrument_type: 'erc20', is_active: true,
        base_currency: 'DRV', quote_currency: 'USDC', risk_universe_id: 3, base_asset_address: '0x' + '33'.repeat(20),
        base_asset_sub_id: '0', tick_size: '0.0001', amount_step: '1', minimum_amount: '10', maximum_amount: '100000' };
      case 'public/get_ticker': return { a: '1', A: '1000', t: ++state.tickerReads === state.staleTickerRead ? NOW - 30001 : NOW };
      case 'private/session_keys': return { public_session_keys: [{ public_session_key: wallet.address,
        expiry_sec: NOW / 1000 + 3600, subaccount_ids: [25923, 120934], offchain_scopes: ['account_info'],
        protocol_scopes: ['trade:orderbook:option', 'trade:orderbook:spot', 'transfer:existing_subaccount'] }] };
      case 'private/get_account': return { fee_info: { spot_taker_fee: '0.0009' } };
      case 'private/get_subaccount': return { subaccount_id: p.subaccount_id, risk_universe_id: p.subaccount_id === 25923 ? 1 : 3,
        is_under_liquidation: false, initial_margin: '1000', maintenance_margin: '1100',
        collaterals: [{ asset_name: 'USDC', currency: 'USDC', amount: fromUnits(p.subaccount_id === 25923 ? state.source : state.destination) }] };
      case 'public/get_wallet': return { wallet: OWNER };
      case 'private/get_open_orders': return [];
      case 'private/order_quote':
        assert.ok((p.max_fee.split('.')[1] || '').length <= 12, 'venue max_fee supports at most 12 decimals');
        return { is_valid: p.max_fee !== '0', post_initial_margin: '900', suggested_max_fee: '0.001021184967' };
      case 'private/get_margin': return { is_valid_trade: true, post_initial_margin: '900', post_maintenance_margin: '950' };
      case 'public/get_all_currencies': return [{ currency: 'USDC', spot: [{ name: 'USDC', address: USDC_ASSET_ADDRESS,
        erc20: { decimals: 6, underlying_erc20: '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' } }] }];
      case 'private/transfer_spot':
        assert.equal(db.db.inTransaction, false, 'durable claim commits before financial handoff');
        assert.equal(db.db.prepare("SELECT status FROM spot_transfers WHERE json_extract(request_json,'$.nonce')=?").get(p.nonce)?.status, 'unknown');
        state.transfer = p;
        state.source -= toUnits(p.amount); state.destination += toUnits(p.amount);
        if (state.loseTransferAck) throw new Error('financial transport timeout');
        return { op_uuid: UUID, operation_id: 42 };
      case 'public/get_transaction': return { status: state.settled ? 'Settled' : 'Executing', transaction_hash: '0xabc' };
      case 'private/get_erc20_transfer_history': return { transfers: [{ operation_id: UUID, batch_uuid: 'batch', batch_status: 'Settled',
        asset: 'USDC', amount: state.transfer.amount, fee: '0', from_subaccount_id: 25923, to_subaccount_id: 120934,
        from_wallet: OWNER, to_wallet: OWNER, timestamp: NOW, tx_hash: '0xabc', is_outgoing: p.subaccount_id === 25923 }] };
      case 'private/order':
        assert.equal(db.db.inTransaction, false, 'order claim commits before financial handoff');
        assert.equal(db.db.prepare('SELECT status FROM execution_submissions WHERE nonce=?').get(p.nonce)?.status, 'unknown');
        assert.ok((p.max_fee.split('.')[1] || '').length <= 12);
        state.order = { ...p, order_id: 'drv-order', filled_amount: p.amount, average_price: '1', order_status: 'filled' };
        state.trades = [{ trade_id: 'drv-fill', order_id: 'drv-order', subaccount_id: 120934, instrument_name: 'DRV-USDC',
          direction: 'buy', is_transfer: false, trade_amount: p.amount, trade_price: '1', trade_fee: '0.01', batch_status: 'Settled' }];
        state.destination -= toUnits(p.amount) + toUnits('0.01');
        return { order: state.order };
      case 'private/get_order_history': return { orders: state.order ? [state.order] : [] };
      case 'private/get_order': return state.order;
      case 'private/get_trade_history': return { trades: state.trades };
      default: throw new Error(`Unexpected endpoint ${method}`);
    }
  };
  // Exercise the adapter's actual authentication/HTTP response parser, including
  // returned RPC errors and HTTP failures. This transport is entirely in memory.
  const http = { post: async (url, params) => {
    const method = url.slice(derive.baseUrl.length + 1);
    if (method === 'private/order' && (state.orderError || state.orderTimeout)) {
      state.calls.push({ method, params: structuredClone(params) });
      assert.equal(db.db.prepare('SELECT status FROM execution_submissions WHERE nonce=?').get(params.nonce)?.status, 'unknown');
      if (state.orderTimeout) throw new Error('financial HTTP timeout');
      if (state.orderErrorHttp) {
        throw Object.assign(new Error('HTTP 400'), { response: { status: 400, data: { error: state.orderError } } });
      }
      return { data: { error: state.orderError } };
    }
    return { data: { result: await post(method, params) } };
  } };
  const venue = createSpotVenue({ config: derive, wallet, http, now: () => NOW,
    assertCanSend: () => { if (state.mode !== 'live') throw new Error('financial sends disabled'); } });
  const create = () => createSpotAccumulationRuntime({ db, config, venue, now: () => NOW,
    mode: () => state.mode, canSend: () => state.mode === 'live' });
  const raw = { trade_id: 'put-fill', order_id: 'put-order', subaccount_id: 25923,
    instrument_name: 'ETH-20261225-1600-P', direction: 'sell', is_transfer: false, batch_status: 'Settled',
    trade_amount: '1', trade_price: '101', trade_fee: '1', timestamp: NOW - 1000 };
  const args = () => ({ cycleBudget: '100', pendingActions: [], optionsCycleSucceeded: true,
    sourceHistory: { complete: true, throughTimestamp: new Date(NOW).toISOString(),
      events: [normalizeV2Trade(raw, 25923, 'v3')], orders: [{ action: 'sell_put', success: 1,
        instrument_name: raw.instrument_name, raw_response: JSON.stringify({ result: { order: raw } }) }] } });
  const sends = () => state.calls.filter(c => ['private/order', 'private/transfer_spot'].includes(c.method));
  return { db, state, create, runtime: create(), args, sends };
}

test('real runtime and adapter transfer only credited funds, settle a DRV buy and preserve setup cash', async t => {
  const h = fixture(t);
  const empty = h.args(); empty.sourceHistory.events = []; empty.sourceHistory.orders = [];
  assert.equal((await h.runtime.advance(empty)).status, 'deferred');
  assert.equal(h.sends().length, 0);
  assert.equal((await h.runtime.advance(h.args())).status, 'transfer_reconciled');
  assert.equal(h.runtime.snapshot().funding.destination_available_usdc, '50');
  assert.equal((await h.runtime.advance(h.args())).status, 'order_reconciled');
  const summary = h.runtime.snapshot();
  assert.deepEqual(h.sends().map(c => c.method), ['private/transfer_spot', 'private/order']);
  assert.equal(summary.credits.length, 1);
  assert.equal(summary.assets.DRV.settled_spend_usdc, '49.01');
  assert.equal(summary.funding.source_available_usdc, '50');
  assert.equal(summary.funding.destination_available_usdc, '0.99');
  assert.equal(fromUnits(h.state.destination - toUnits(summary.funding.destination_available_usdc)), '5.01');
  assert.equal(h.db.db.prepare('SELECT status FROM execution_submissions').get().status, 'accounted');
  assert.equal(h.sends()[1].params.max_fee, '0.001021184968');
  const settledTrade = summary.reservations[0].evidence.trades[0];
  assert.equal(settledTrade.fee_currency, 'USDC');
  assert.equal(Object.hasOwn(settledTrade.raw_venue_trade, 'fee_currency'), false);
});

test('a stale final transfer preview sends nothing and leaves no stranded reservation or journal', async t => {
  const h = fixture(t); h.state.staleTickerRead = 4;
  const result = await h.runtime.advance(h.args());
  assert.match(result.reason, /stale/);
  assert.equal(h.sends().length, 0);
  assert.equal(h.runtime.snapshot().transfers.length, 0);
  assert.equal(h.runtime.snapshot().reservations.length, 0);
  const table = h.db.db.prepare("SELECT 1 FROM sqlite_master WHERE name='execution_submissions'").get();
  if (table) assert.equal(h.db.db.prepare('SELECT count(*) n FROM execution_submissions').get().n, 0);
  assert.doesNotThrow(() => h.runtime.assertSourceClear());
  h.state.staleTickerRead = 0;
  assert.equal((await h.create().advance(h.args())).status, 'transfer_reconciled');
  assert.equal(h.sends().length, 1);
});

test('a failed final DRV preview retains funded ALT cash without creating an unknown order', async t => {
  const h = fixture(t);
  assert.equal((await h.runtime.advance(h.args())).status, 'transfer_reconciled');
  h.state.staleTickerRead = h.state.tickerReads + 4;
  assert.match((await h.runtime.advance(h.args())).reason, /stale/);
  assert.equal(h.sends().length, 1);
  assert.equal(h.runtime.snapshot().reservations.length, 0);
  assert.equal(h.runtime.snapshot().funding.destination_available_usdc, '50');
  const table = h.db.db.prepare("SELECT 1 FROM sqlite_master WHERE name='execution_submissions'").get();
  if (table) assert.equal(h.db.db.prepare('SELECT count(*) n FROM execution_submissions').get().n, 0);
  h.state.staleTickerRead = 0;
  assert.equal((await h.create().advance(h.args())).status, 'order_reconciled');
  assert.equal(h.sends().length, 2);
});

test('acknowledged transfer recovers with sends off; a lost financial acknowledgement stays reserved', async t => {
  const h = fixture(t); h.state.settled = false;
  assert.equal((await h.runtime.advance(h.args())).status, 'deferred');
  assert.equal(h.runtime.snapshot().funding.source_reserved_usdc, '50');
  h.state.mode = 'off'; h.state.settled = true;
  const restarted = h.create();
  assert.equal((await restarted.recover()).status, 'reconciled');
  assert.equal(restarted.snapshot().funding.destination_available_usdc, '50');
  assert.equal(h.sends().length, 1);

  const unknown = fixture(t); unknown.state.loseTransferAck = true;
  assert.match((await unknown.runtime.advance(unknown.args())).reason, /transport timeout/);
  unknown.state.mode = 'off';
  assert.match((await unknown.create().recover()).reason, /authoritative operation recovery/);
  assert.equal(unknown.runtime.snapshot().funding.source_reserved_usdc, '50');
  assert.equal(unknown.sends().length, 1);
});

test('explicit IOC no-liquidity RPC and HTTP refusals release only the rejected order for a later retry', async t => {
  for (const orderErrorHttp of [false, true]) {
    const h = fixture(t);
    assert.equal((await h.runtime.advance(h.args())).status, 'transfer_reconciled');
    h.state.orderError = { code: 11009, message: 'zero liquidity for market or ioc/fok order' };
    h.state.orderErrorHttp = orderErrorHttp;
    assert.equal((await h.runtime.advance(h.args())).status, 'order_rejected');
    const snapshot = h.runtime.snapshot();
    assert.equal(snapshot.reservations.length, 1);
    assert.equal(snapshot.reservations[0].status, 'terminal');
    assert.equal(snapshot.reservations[0].settled_spend_usdc, '0');
    assert.equal(snapshot.funding.destination_available_usdc, '50');
    const journal = h.db.db.prepare('SELECT status,nonce,response_json FROM execution_submissions').get();
    assert.equal(journal.status, 'rejected');
    assert.equal(JSON.parse(journal.response_json).response.error.code, 11009);
    assert.equal(JSON.parse(journal.response_json).request.nonce, journal.nonce);
    assert.doesNotThrow(() => accounting.assertNoUnresolvedSubmission(h.db));
    assert.equal(h.state.destination, toUnits('55.01'));
    h.state.orderError = null;
    assert.equal((await h.create().advance(h.args())).status, 'order_reconciled');
    assert.equal(h.sends().length, 3);
    assert.equal(new Set(h.sends().filter(c => c.method === 'private/order').map(c => c.params.nonce)).size, 2);
  }
});

test('unknown RPC errors and HTTP timeouts keep the DRV reservation and shared journal latched', async t => {
  for (const failure of [
    { orderError: { code: 98765, message: 'unknown exchange condition' } },
    { orderError: { code: 98765, message: 'unknown exchange condition' }, orderErrorHttp: true },
    { orderTimeout: true },
  ]) {
    const h = fixture(t);
    assert.equal((await h.runtime.advance(h.args())).status, 'transfer_reconciled');
    Object.assign(h.state, failure);
    assert.equal((await h.runtime.advance(h.args())).status, 'deferred');
    const reserved = h.runtime.snapshot();
    assert.equal(reserved.reservations[0].status, 'unknown');
    assert.ok(toUnits(reserved.assets.DRV.reserved_usdc) > 0n);
    assert.equal(h.db.db.prepare('SELECT status FROM execution_submissions').get().status, 'unknown');
    assert.throws(() => accounting.assertNoUnresolvedSubmission(h.db), /requires accounting recovery/);
    h.state.mode = 'off';
    assert.match((await h.create().recover()).reason, /No order evidence/);
    assert.deepEqual(h.runtime.snapshot(), reserved);
    assert.equal(h.sends().length, 2);
  }
});
