'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const Database = require('better-sqlite3');
const { normalizeV2Trade } = require('../bot/economic-events');
const { createSpotAccumulationRuntime } = require('../bot/spot-accumulation-runtime');
const accounting = require('../bot/order-accounting');
const NOW = Date.parse('2026-11-10T01:00:00Z');
const owner = '0x' + '11'.repeat(20);
const config = { network: 'mainnet', apiVersion: 'v3', accountId: '25923', destinationAccountId: '120934',
  accountAddress: owner, activationTimestamp: '2026-11-10T00:00:00.000Z', maxBatchUsdc: '50' };
function history() {
  const raw = { trade_id: 'put-fill', order_id: 'put-order', subaccount_id: 25923,
    instrument_name: 'ETH-20261225-1600-P', direction: 'sell', is_transfer: false, batch_status: 'Settled',
    trade_amount: '1', trade_price: '101', trade_fee: '1', timestamp: NOW - 1000 };
  return { complete: true, throughTimestamp: new Date(NOW).toISOString(), events: [normalizeV2Trade(raw, 25923, 'v3')],
    orders: [{ action: 'sell_put', success: 1, instrument_name: raw.instrument_name, raw_response: JSON.stringify({ result: { order: raw } }) }] };
}
function harness(t) {
  const db = { db: new Database(':memory:') }; t.after(() => db.db.close());
  const state = { mode: 'live', canSend: true, transfers: 0, orders: 0, signed: 0, previews: 0,
    transferSettled: true, orderSettled: true, transferLostAck: false, orderLostAck: false, nonce: 100 };
  const venue = {
    async readSnapshot() {
      return { source: { subaccount_id: 25923, owner, risk_universe_id: 1, received_at: NOW,
        usdc_balance: '1000', committed_usdc: '10', cycle_budget_usdc: '100' },
      destination: { subaccount_id: 120934, owner, risk_universe_id: 3, received_at: NOW, usdc_balance: '1005.01', committed_usdc: '0' },
      instruments: { DRV: { instrument_name: 'DRV-USDC', instrument_type: 'erc20', is_active: true,
        base_currency: 'DRV', quote_currency: 'USDC', risk_universe_id: 3, tick_size: '0.0001', amount_step: '1', minimum_amount: '10', maximum_amount: '1000000' } },
      drvTicker: { a: '1', A: '1000', t: NOW, received_at: NOW }, fees: { DRV: { max_fee_per_unit: '0.01' } },
      transfer: { max_fee_usdc: '0' }, keyExpirySec: NOW / 1000 + 10000 };
    },
    async previewOrder() { state.previews++; return { valid: true }; },
    async previewTransfer() { state.previews++; return { valid: true }; },
    async prepareOrder(order) { state.signed++; return { ...order, nonce: String(state.nonce++), signer: owner, signature: '0x' + 'aa'.repeat(65) }; },
    async prepareTransfer(plan) { state.signed++; return { subaccount_id: plan.from_subaccount_id, to_subaccount_id: plan.to_subaccount_id,
      asset_name: 'USDC', sub_id: 0, new_subaccount_manager: 0, amount: plan.amount, max_fee_usd: '0', nonce: String(state.nonce++),
      signer: owner, signature: '0x' + 'aa'.repeat(65), signature_expiry_sec: NOW / 1000 + 900 }; },
    async submitOrder(request, beforeSend) { beforeSend(); state.orders++; if (state.orderLostAck) throw new Error('order timeout'); return { result: { order: { order_id: 'drv-order-' + request.nonce } } }; },
    async submitTransfer(request, beforeSend) { beforeSend(); state.transfers++; if (state.transferLostAck) throw new Error('transfer timeout'); return { op_uuid: 'op-' + state.transfers, operation_id: state.transfers }; },
    async collectTransfer(request, ack) {
      const row = { operation_id: ack.op_uuid, batch_uuid: 'batch', batch_status: 'Settled', asset: 'USDC',
        amount: request.amount, fee: '0', from_subaccount_id: 25923, to_subaccount_id: 120934,
        from_wallet: owner, to_wallet: owner, timestamp: NOW, tx_hash: '0xabc' };
      return { operation: { op_uuid: ack.op_uuid, status: state.transferSettled ? 'Settled' : 'Executing', transaction_hash: '0xabc' },
        history: { sourceAccountId: '25923', destinationAccountId: '120934', source: [{ ...row, is_outgoing: true }], destination: [{ ...row, is_outgoing: false }] } };
    },
    async collectOrder(request) {
      return { order: { ...request, order_id: 'drv-order-' + request.nonce, filled_amount: request.amount, average_price: '1', order_status: 'filled' },
        trades: [{ trade_id: 'drv-fill-' + request.nonce, order_id: 'drv-order-' + request.nonce, subaccount_id: 120934,
          instrument_name: 'DRV-USDC', direction: 'buy', is_transfer: false, trade_amount: request.amount,
          trade_price: '1', trade_fee: '0.01', fee_currency: 'USDC', batch_status: state.orderSettled ? 'Settled' : 'Executing' }] };
    },
  };
  const create = () => createSpotAccumulationRuntime({ db, config, venue, mode: () => state.mode, canSend: () => state.canSend, now: () => NOW });
  const args = () => ({ cycleBudget: '100', pendingActions: [], sourceHistory: history(), optionsCycleSucceeded: true });
  return { db, state, venue, create, runtime: create(), args };
}

test('off, failed source cycle and global pause never initialize or sign', async t => {
  const h = harness(t); const before = h.db.db.serialize();
  h.state.mode = 'off'; assert.equal((await h.runtime.advance(h.args())).status, 'off');
  h.state.mode = 'live'; assert.match((await h.runtime.advance({ ...h.args(), optionsCycleSucceeded: false })).reason, /cycle/);
  h.state.canSend = false; assert.match((await h.runtime.advance(h.args())).reason, /disabled/);
  assert.equal(h.state.signed, 0); assert.deepEqual(h.db.db.serialize(), before);
});

test('dry run previews all net future proceeds without writes, signatures or seed spending', async t => {
  const h = harness(t); h.state.mode = 'dry_run'; const before = h.db.db.serialize();
  const result = await h.runtime.advance(h.args());
  assert.equal(result.status, 'dry_run'); assert.equal(result.credits.total_net_usdc, '100');
  assert.equal(result.plan.routes.DRV.transfer.amount, '50'); assert.equal(result.plan.routes.ETH.order, undefined);
  assert.equal(h.state.previews, 1); assert.equal(h.state.signed, 0); assert.deepEqual(h.db.db.serialize(), before);
  const empty = h.args(); empty.sourceHistory.events = []; empty.sourceHistory.orders = [];
  assert.equal((await h.runtime.advance(empty)).status, 'deferred');
  assert.equal(h.state.signed, 0);
});

test('bounded transfer funds allocation then DRV purchase, retaining the unallocated seed and future credits', async t => {
  const h = harness(t);
  assert.equal((await h.runtime.advance(h.args())).status, 'transfer_reconciled');
  assert.deepEqual(h.runtime.snapshot().funding, { source_available_usdc: '50', source_reserved_usdc: '0',
    destination_funded_usdc: '50', destination_available_usdc: '50', transfer_fees_usdc: '0' });
  assert.equal((await h.runtime.advance(h.args())).status, 'order_reconciled');
  assert.equal(h.state.transfers, 1); assert.equal(h.state.orders, 1);
  assert.equal(h.runtime.snapshot().assets.DRV.settled_spend_usdc, '49.01');
  assert.equal(h.runtime.snapshot().funding.destination_available_usdc, '0.99');
  assert.equal(h.runtime.snapshot().credits.length, 1);
  assert.equal(h.db.db.prepare('SELECT status FROM execution_submissions').get().status, 'accounted');
  assert.equal(h.db.db.prepare("SELECT 1 FROM sqlite_master WHERE name IN ('orders','bot_state')").get(), undefined);
});

test('unknown transfer remains reserved after restart and disabling, never resent', async t => {
  const h = harness(t); h.state.transferLostAck = true;
  assert.match((await h.runtime.advance(h.args())).reason, /timeout/);
  assert.equal(h.runtime.snapshot().funding.source_reserved_usdc, '50');
  assert.throws(() => h.runtime.assertSourceClear(), /reserved/);
  h.state.mode = 'off'; const restarted = h.create();
  assert.match((await restarted.recover()).reason, /operation recovery/);
  h.state.mode = 'live'; assert.equal((await restarted.advance(h.args())).status, 'deferred');
  assert.equal(h.state.transfers, 1); assert.equal(h.state.signed, 1);
});

test('acknowledged transfer reconciles after restart even when new sends are off', async t => {
  const h = harness(t); h.state.transferSettled = false;
  assert.equal((await h.runtime.advance(h.args())).status, 'deferred');
  h.state.transferSettled = true; h.state.mode = 'off';
  const restarted = h.create(); assert.equal((await restarted.recover()).status, 'reconciled');
  assert.equal(restarted.snapshot().funding.destination_available_usdc, '50');
  assert.doesNotThrow(() => restarted.assertSourceClear()); assert.equal(h.state.transfers, 1);
});

test('terminal-but-unsettled DRV stays journal-latched and recovers without another send', async t => {
  const h = harness(t); await h.runtime.advance(h.args()); h.state.orderSettled = false;
  assert.equal((await h.runtime.advance(h.args())).status, 'deferred');
  assert.equal(h.runtime.snapshot().reservations[0].status, 'pending');
  assert.throws(() => accounting.assertNoUnresolvedSubmission(h.db), /recovery/);
  h.state.mode = 'off'; h.state.orderSettled = true;
  assert.equal((await h.create().recover()).status, 'reconciled');
  assert.doesNotThrow(() => accounting.assertNoUnresolvedSubmission(h.db)); assert.equal(h.state.orders, 1);
});

test('lost order acknowledgement recovers only affirmative matching evidence', async t => {
  const h = harness(t); await h.runtime.advance(h.args()); h.state.orderLostAck = true;
  assert.match((await h.runtime.advance(h.args())).reason, /timeout/);
  const collect = h.venue.collectOrder; h.venue.collectOrder = async () => { throw new Error('no order evidence'); };
  assert.match((await h.create().recover()).reason, /no order evidence/);
  assert.equal(h.runtime.snapshot().reservations[0].status, 'unknown');
  h.venue.collectOrder = collect;
  assert.equal((await h.create().recover()).status, 'reconciled'); assert.equal(h.state.orders, 1);
});

test('foreign option journal blocks accumulation without editing option accounting', async t => {
  const h = harness(t); accounting.beginSubmission(h.db, 1, { action: 'buy_put', instrument_name: 'ETH-20261225-1600-P', nonce: 'x', subaccount_id: 25923 });
  const before = h.db.db.serialize();
  assert.match((await h.runtime.advance(h.args())).reason, /Unresolved execution/);
  assert.equal(h.state.signed, 0); assert.deepEqual(h.db.db.serialize(), before);
});

test('coverage, future evidence, exact preview and mode changes fail before send', async t => {
  const h = harness(t);
  for (const change of [s => { s.complete = false; }, s => { s.throughTimestamp = new Date(NOW + 1).toISOString(); },
    s => { s.events[0].timestamp = new Date(NOW + 1).toISOString(); }]) {
    const args = h.args(); change(args.sourceHistory);
    assert.match((await h.runtime.advance(args)).reason, /history/);
  }
  h.venue.previewTransfer = async () => ({ valid: false });
  assert.match((await h.runtime.advance(h.args())).reason, /preview/); assert.equal(h.state.signed, 0);
  h.venue.previewTransfer = async () => { h.state.canSend = false; return { valid: true }; };
  assert.match((await h.runtime.advance(h.args())).reason, /disabled/); assert.equal(h.state.signed, 0);
});

test('overlapping ticks defer while the first awaits a preview', async t => {
  const h = harness(t); let release;
  h.venue.previewTransfer = () => new Promise(resolve => { release = resolve; });
  const first = h.runtime.advance(h.args());
  while (!release) await new Promise(resolve => setImmediate(resolve));
  assert.equal((await h.runtime.advance(h.args())).reason, 'accumulation_in_flight');
  release({ valid: true }); assert.equal((await first).status, 'transfer_reconciled');
  assert.equal(h.state.transfers, 1);
});

test('saved order acknowledgement cannot be replaced with another matching-looking order', async t => {
  const h = harness(t); await h.runtime.advance(h.args()); h.state.orderSettled = false;
  await h.runtime.advance(h.args());
  const before = h.runtime.snapshot(); const collect = h.venue.collectOrder;
  h.venue.collectOrder = async request => {
    const result = await collect(request); result.order.order_id = 'different-order';
    for (const trade of result.trades) trade.order_id = 'different-order';
    return result;
  };
  assert.match((await h.create().recover()).reason, /saved acknowledgement/);
  assert.deepEqual(h.runtime.snapshot(), before);
});

for (const kind of ['Transfer', 'Order']) {
  test(`final ${kind.toLowerCase()} authentication failure leaves no attempted-send artifact`, async t => {
    const h = harness(t);
    if (kind === 'Order') await h.runtime.advance(h.args());
    const before = h.runtime.snapshot();
    h.venue[`submit${kind}`] = async () => { throw new Error('final authentication failed'); };
    assert.match((await h.runtime.advance(h.args())).reason, /authentication failed/);
    assert.deepEqual(h.runtime.snapshot().transfers, before.transfers);
    assert.deepEqual(h.runtime.snapshot().reservations, before.reservations);
    assert.equal(h.runtime.snapshot().funding.source_reserved_usdc, '0');
    assert.equal(h.state.orders, 0); assert.equal(h.state.transfers, kind === 'Order' ? 1 : 0);
    assert.equal(h.db.db.prepare("SELECT 1 FROM sqlite_master WHERE name='execution_submissions'").get(), undefined);
  });

  test(`option submission appearing during final ${kind.toLowerCase()} preview blocks the atomic claim`, async t => {
    const h = harness(t);
    if (kind === 'Order') await h.runtime.advance(h.args());
    const before = h.runtime.snapshot();
    h.venue[`submit${kind}`] = async (request, beforeSend) => {
      accounting.beginSubmission(h.db, 99, { action: 'buy_put', instrument_name: 'ETH-20261225-1600-P', nonce: 'racing-option', subaccount_id: 25923 });
      beforeSend();
      assert.fail('conflicting financial send must not run');
    };
    assert.match((await h.runtime.advance(h.args())).reason, /Unresolved execution/);
    assert.deepEqual(h.runtime.snapshot().transfers, before.transfers);
    assert.deepEqual(h.runtime.snapshot().reservations, before.reservations);
    assert.equal(h.runtime.snapshot().funding.source_reserved_usdc, '0');
    assert.equal(h.db.db.prepare('SELECT count(*) n FROM execution_submissions').get().n, 1);
    assert.equal(h.db.db.prepare('SELECT nonce FROM execution_submissions').get().nonce, 'racing-option');
    assert.equal(h.state.orders, 0); assert.equal(h.state.transfers, kind === 'Order' ? 1 : 0);
  });
}

test('final send mode guard is inside the same immediate transaction as reservation and claim', async t => {
  const h = harness(t); let final = false, checkedInTransaction = false;
  const runtime = createSpotAccumulationRuntime({ db: h.db, config, venue: h.venue,
    mode: () => h.state.mode, now: () => NOW, canSend: () => {
      if (!final) return true;
      checkedInTransaction = h.db.db.inTransaction;
      return false;
    } });
  h.venue.submitTransfer = async (request, beforeSend) => { final = true; beforeSend(); assert.fail('disabled send'); };
  assert.match((await runtime.advance(h.args())).reason, /disabled/);
  assert.equal(checkedInTransaction, true);
  assert.equal(runtime.snapshot().transfers.length, 0);
  assert.equal(runtime.snapshot().funding.source_reserved_usdc, '0');
});

function refuse(request, response = { error: { code: 11009, message: 'IOC order has no liquidity' } }) {
  const error = new Error('Explicit venue order refusal');
  error.orderRejection = { method: 'private/order', request, response };
  return error;
}

test('direct IOC refusal atomically releases zero-spend reservation and permits a later fresh nonce', async t => {
  const h = harness(t); await h.runtime.advance(h.args());
  const send = h.venue.submitOrder;
  h.venue.submitOrder = async (request, beforeSend) => { beforeSend(); h.state.orders++; throw refuse(request); };
  const rejected = await h.runtime.advance(h.args());
  assert.equal(rejected.status, 'order_rejected');
  assert.equal(h.runtime.snapshot().reservations[0].status, 'terminal');
  assert.equal(h.runtime.snapshot().assets.DRV.settled_spend_usdc, '0');
  assert.equal(h.runtime.snapshot().funding.destination_available_usdc, '50');
  assert.equal(h.db.db.prepare('SELECT status FROM execution_submissions').get().status, 'rejected');
  assert.doesNotThrow(() => accounting.assertNoUnresolvedSubmission(h.db));
  h.venue.submitOrder = send;
  assert.equal((await h.create().advance(h.args())).status, 'order_reconciled');
  assert.equal(h.state.orders, 2);
  assert.equal(h.db.db.prepare('SELECT COUNT(DISTINCT nonce) n FROM execution_submissions').get().n, 2);
});

for (const scenario of ['wrong request', 'unknown RPC error', 'accepted identity', 'prior ACK']) {
  test(`order refusal with ${scenario} cannot release reserved cash`, async t => {
    const h = harness(t); await h.runtime.advance(h.args());
    h.venue.submitOrder = async (request, beforeSend) => {
      beforeSend(); h.state.orders++;
      const proofRequest = scenario === 'wrong request' ? { ...request, nonce: 'another' } : request;
      const response = scenario === 'unknown RPC error' ? { error: { code: 999999, message: 'unknown outcome' } }
        : scenario === 'accepted identity' ? { error: { code: 11009, message: 'no liquidity' }, result: { order_id: 'accepted-order' } }
          : { error: { code: 11009, message: 'IOC order has no liquidity' } };
      if (scenario === 'prior ACK') {
        h.db.db.prepare("UPDATE execution_submissions SET status='acknowledged',response_json=?").run(JSON.stringify({ result: { order: { order_id: 'accepted-order' } } }));
      }
      throw refuse(proofRequest, response);
    };
    assert.equal((await h.runtime.advance(h.args())).status, 'deferred');
    assert.equal(h.runtime.snapshot().reservations[0].status, 'unknown');
    assert.equal(h.runtime.snapshot().assets.DRV.settled_spend_usdc, '0');
    assert.notEqual(h.runtime.snapshot().funding.destination_available_usdc, '50');
    assert.throws(() => accounting.assertNoUnresolvedSubmission(h.db), /recovery/);
  });
}

function sampleHarness(t) {
  const h = harness(t); h.state.mode = 'sample';
  const read = h.venue.readSnapshot;
  h.venue.readSnapshot = async input => {
    const snapshot = await read(input);
    // Keep this lifecycle fixture's $1 price while allowing a bounded $5 order.
    snapshot.instruments.DRV.minimum_amount = '1';
    return snapshot;
  };
  h.create = () => createSpotAccumulationRuntime({ db: h.db, config: { ...config, maxBatchUsdc: '5' }, venue: h.venue,
    mode: () => h.state.mode, canSend: () => h.state.canSend, now: () => NOW });
  h.runtime = h.create();
  h.args = () => ({ cycleBudget: '100', pendingActions: [], optionsCycleSucceeded: true });
  return h;
}

test('startup sample grants only five operator dollars, excludes sale credits and setup cash, and completes once', async t => {
  const h = sampleHarness(t);
  assert.equal((await h.runtime.advance({ ...h.args(), sourceHistory: history() })).status, 'transfer_reconciled');
  let snapshot = h.runtime.snapshot();
  assert.equal(snapshot.total_net_usdc, '0'); assert.equal(snapshot.operator_funding_usdc, '5');
  assert.equal(snapshot.total_funding_usdc, '5'); assert.equal(snapshot.credits.length, 0);
  assert.equal(snapshot.transfers[0].amount, '5');
  assert.equal(snapshot.funding.destination_available_usdc, '5');
  const result = await h.create().advance(h.args());
  assert.equal(result.status, 'sample_complete');
  snapshot = h.runtime.snapshot();
  assert.equal(snapshot.assets.DRV.settled_spend_usdc, '4.01');
  assert.equal(snapshot.funding.destination_available_usdc, '0.99');
  assert.equal(snapshot.operator_funding_usdc, '5');
  const before = h.db.db.serialize();
  for (let run = 0; run < 3; run++) assert.equal((await h.create().advance(h.args())).status, 'sample_complete');
  assert.equal(h.state.transfers, 1); assert.equal(h.state.orders, 1);
  assert.deepEqual(h.db.db.serialize(), before);
});

test('sample requires actual options success and live send permission before granting funds', async t => {
  const h = sampleHarness(t); const before = h.db.db.serialize();
  assert.match((await h.runtime.advance({ ...h.args(), optionsCycleSucceeded: false })).reason, /cycle/);
  h.state.canSend = false;
  assert.match((await h.runtime.advance(h.args())).reason, /disabled/);
  assert.deepEqual(h.db.db.serialize(), before); assert.equal(h.state.signed, 0);
});

test('sample refuses prior ordinary credits and cannot widen its fixed allowance through config', async t => {
  const h = harness(t); await h.runtime.advance(h.args()); h.state.mode = 'sample';
  const sample = createSpotAccumulationRuntime({ db: h.db, config: { ...config, maxBatchUsdc: '5' }, venue: h.venue,
    mode: () => h.state.mode, canSend: () => true, now: () => NOW });
  assert.match((await sample.advance(h.args())).reason, /ordinary put-sale credits/);
  assert.equal(sample.snapshot().operator_funding_usdc, '0');
  assert.equal(h.state.transfers, 1); assert.equal(h.state.orders, 0);
  assert.match((await h.runtime.advance(h.args())).reason, /fixed lifetime budget/);
});

test('sample unknown transfer remains five dollars across restarts and never receives another grant', async t => {
  const h = sampleHarness(t); h.state.transferLostAck = true;
  assert.match((await h.runtime.advance(h.args())).reason, /timeout/);
  assert.equal(h.runtime.snapshot().funding.source_reserved_usdc, '5');
  for (let run = 0; run < 2; run++) assert.match((await h.create().advance(h.args())).reason, /operation recovery/);
  assert.equal(h.runtime.snapshot().operator_funding_usdc, '5');
  assert.equal(h.state.transfers, 1); assert.equal(h.state.orders, 0);
});

test('sample unresolved fill recovers before completion without another purchase', async t => {
  const h = sampleHarness(t); await h.runtime.advance(h.args()); h.state.orderSettled = false;
  assert.equal((await h.runtime.advance(h.args())).status, 'deferred');
  assert.equal(h.runtime.snapshot().reservations[0].status, 'pending');
  assert.equal(h.runtime.snapshot().operator_funding_usdc, '5');
  h.state.orderSettled = true;
  assert.equal((await h.create().advance(h.args())).status, 'sample_complete');
  assert.equal(h.state.transfers, 1); assert.equal(h.state.orders, 1);
});

test('sample terminal partial fill completes the sample and leaves unused allocated cash untouched', async t => {
  const h = sampleHarness(t); await h.runtime.advance(h.args());
  const collect = h.venue.collectOrder;
  h.venue.collectOrder = async request => {
    const result = await collect(request);
    result.order.order_status = 'cancelled'; result.order.filled_amount = '2';
    result.trades[0].trade_amount = '2'; return result;
  };
  assert.equal((await h.runtime.advance(h.args())).status, 'sample_complete');
  assert.equal(h.runtime.snapshot().assets.DRV.settled_spend_usdc, '2.01');
  assert.equal(h.runtime.snapshot().funding.destination_available_usdc, '2.99');
  assert.equal((await h.create().advance(h.args())).status, 'sample_complete');
  assert.equal(h.state.orders, 1);
});

test('sample can retry a definitive zero-fill refusal using only the original five dollars', async t => {
  const h = sampleHarness(t); await h.runtime.advance(h.args()); const send = h.venue.submitOrder;
  h.venue.submitOrder = async (request, beforeSend) => { beforeSend(); h.state.orders++; throw refuse(request); };
  assert.equal((await h.runtime.advance(h.args())).status, 'order_rejected');
  assert.equal(h.runtime.snapshot().funding.destination_available_usdc, '5');
  h.venue.submitOrder = send;
  assert.equal((await h.create().advance(h.args())).status, 'sample_complete');
  assert.equal(h.runtime.snapshot().operator_funding_usdc, '5'); assert.equal(h.state.transfers, 1);
  assert.equal(h.state.orders, 2);
});

test('mode changes during sample preview cannot switch the prepared plan to routine execution', async t => {
  const h = sampleHarness(t);
  h.venue.previewTransfer = async () => { h.state.mode = 'live'; return { valid: true }; };
  assert.match((await h.runtime.advance(h.args())).reason, /disabled/);
  assert.equal(h.runtime.snapshot().transfers.length, 0); assert.equal(h.state.transfers, 0);
});

test('sample completion by a concurrent runtime blocks another prepared sample at the atomic claim', async t => {
  const h = sampleHarness(t); await h.runtime.advance(h.args());
  const send = h.venue.submitOrder;
  h.venue.submitOrder = async (request, beforeSend) => {
    h.venue.submitOrder = send;
    assert.equal((await h.create().advance(h.args())).status, 'sample_complete');
    beforeSend();
    assert.fail('second sample must not reach financial HTTP');
  };
  assert.match((await h.runtime.advance(h.args())).reason, /already completed/);
  assert.equal(h.state.orders, 1); assert.equal(h.state.transfers, 1);
  assert.equal(h.runtime.snapshot().reservations.length, 1);
  assert.equal(h.runtime.snapshot().operator_funding_usdc, '5');
});
