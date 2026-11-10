'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const Database = require('better-sqlite3');
const { normalizeV2Trade } = require('../bot/economic-events');
const { SAMPLE_BUDGET_USDC, projectCredits, createSpotAccumulationStore } = require('../bot/spot-accumulation-store');
const config = { accountId: '25923', activationTimestamp: '2026-11-09T00:00:00.000Z', network: 'mainnet', apiVersion: 'v3', destinationAccountId: '120934', accountAddress: '0x' + '22'.repeat(20) };
function source(id = 'put-1', overrides = {}) {
  const raw = { trade_id: id, order_id: 'put-order', subaccount_id: 25923, instrument_name: 'ETH-20261127-1600-P', direction: 'sell',
    is_transfer: false, batch_status: 'Settled', trade_amount: '1', trade_price: '100', trade_fee: '0', timestamp: Date.parse('2026-11-09T00:01:00Z'), ...overrides };
  return normalizeV2Trade(raw, 25923, 'v3');
}
function owned() { return { id: 7, action: 'sell_put', success: 1, instrument_name: 'ETH-20261127-1600-P',
  raw_response: JSON.stringify({ result: { order: { order_id: 'put-order', subaccount_id: 25923, direction: 'sell', instrument_name: 'ETH-20261127-1600-P' } } }) }; }
const project = (events, orders = [owned()], changes = {}) => projectCredits({ ...config, events, orders, ...changes });
function harness(t) { const db = new Database(':memory:'); t.after(() => db.close()); return { db, store: createSpotAccumulationStore(db, config, { readOnly: false }) }; }
function request(asset = 'DRV') { return { instrument_name: `${asset}-USDC`, subaccount_id: asset === 'ETH' ? 25923 : 120934,
  direction: 'buy', amount: '1', limit_price: '40', max_fee: '2', nonce: '123', time_in_force: asset === 'ETH' ? 'gtc' : 'ioc', reduce_only: false }; }
function order(overrides = {}) { return { ...request(), order_id: 'spot-order', filled_amount: '0.5', average_price: '39', order_status: 'open', ...overrides }; }
function fill(overrides = {}) { return { trade_id: 'spot-fill', order_id: 'spot-order', subaccount_id: 120934,
  instrument_name: 'DRV-USDC', direction: 'buy', is_transfer: false, trade_amount: '0.5', trade_price: '39',
  trade_fee: '0.1', fee_currency: 'USDC', batch_status: 'Settled', ...overrides }; }

function transferRequest(overrides = {}) { return { subaccount_id: 25923, to_subaccount_id: 120934,
  new_subaccount_manager: 0, asset_name: 'USDC', sub_id: 0, amount: '100', max_fee_usd: '0', nonce: '900',
  signer: '0x' + '11'.repeat(20), signature: '0x' + 'aa'.repeat(65), signature_expiry_sec: 1800000000, ...overrides }; }
function transferEvidence(opUuid = 'transfer-operation', amount = '100') {
  const row = { operation_id: opUuid, batch_uuid: 'batch-one', batch_status: 'Settled', asset: 'USDC', amount, fee: '0',
    from_subaccount_id: 25923, to_subaccount_id: 120934, from_wallet: config.accountAddress, to_wallet: config.accountAddress,
    timestamp: 1794182400000, tx_hash: '0xabc' };
  return { operation: { op_uuid: opUuid, status: 'Settled', transaction_hash: '0xabc', error_log: null },
    history: { sourceAccountId: '25923', destinationAccountId: '120934', source: [{ ...row, is_outgoing: true }],
      destination: [{ ...row, is_outgoing: false }] } };
}
function fund(store, amount = '100', id = 'funding', nonce = '900') {
  store.reserveTransfer({ id, amount, request: transferRequest({ amount, nonce }) });
  store.claimTransferSend({ id });
  store.acknowledgeTransfer({ id, result: { op_uuid: id, operation_id: 1 } });
  return store.reconcileTransfer({ id, ...transferEvidence(id, amount) });
}
function refusal(overrides = {}) { return { method: 'private/order', request: request(),
  response: { error: { code: 11009, message: 'zero liquidity for market or ioc/fok order' } }, ...overrides }; }

test('settled put credits deduct fees once and allocate all net and tiny proceeds to DRV', () => {
  const row = source('one', { trade_price: '1', trade_fee: '0.1' });
  const result = project([row, row]);
  assert.equal(result.complete, true); assert.equal(result.total_net_usdc, '0.9');
  assert.deepEqual(result.allocated_usdc, { ETH: '0', DRV: '0.9' }); assert.equal(result.credits.length, 1);
  const tiny = project([source('a', { trade_price: '0.000000000000000001' }), source('b', { trade_price: '0.000000000000000001' })]);
  assert.deepEqual(tiny.allocated_usdc, { ETH: '0', DRV: '0.000000000000000002' });
  assert.equal(tiny.remainder_usdc, '0');
});

test('source boundary is prospective, delayed receipt uses trade time, and unrelated trades do not credit', () => {
  const old = source('old', { timestamp: Date.parse('2026-11-08T23:59:59Z') });
  const bought = source('bought', { direction: 'buy' });
  const called = source('call', { instrument_name: 'ETH-20261127-1600-C' });
  const result = project([old, bought, called, source()]);
  assert.equal(result.credits.length, 1); assert.equal(result.ignored_count, 3);
  assert.equal(result.total_net_usdc, '100');
});

test('unknown fees, unresolved settlement, wrong account and missing ownership cannot authorize credits', () => {
  const noFee = source('missing', { trade_fee: undefined });
  const notSettled = source(); const raw = JSON.parse(notSettled.raw_json); raw.batch_status = 'Executing'; notSettled.raw_json = JSON.stringify(raw);
  for (const result of [project([noFee]), project([notSettled]), project([source()], []), project([source()], [owned()], { accountId: '1' })]) {
    assert.equal(result.complete, false); assert.equal(result.total_net_usdc, '0'); assert.equal(result.deferred.length, 1);
  }
  const forged = { ...source(), cashflow_usd: '999' };
  assert.equal(project([forged]).total_net_usdc, '0');
});

test('conflicting duplicate source ID invalidates the whole identity instead of leaving the first credit spendable', () => {
  const result = project([source(), source('put-1', { trade_price: '101' })]);
  assert.equal(result.complete, false); assert.equal(result.total_net_usdc, '0'); assert.equal(result.credits.length, 0);
});

test('read-only view does not initialize a deployed database or persist activation state', t => {
  const db = new Database(':memory:'); t.after(() => db.close());
  db.pragma('query_only = ON');
  const store = createSpotAccumulationStore(db, config);
  assert.equal(store.snapshot().initialized, false);
  assert.equal(db.prepare('SELECT count(*) n FROM sqlite_master').get().n, 0);
  assert.throws(() => store.credit({ events: [source()], orders: [owned()] }), /Read-only/);
  assert.throws(() => store.authorizeStartupSample(), /Read-only/);
});

test('startup sample is a separate immutable five-dollar allowance, never a put-sale credit', t => {
  const { db, store } = harness(t);
  assert.equal(SAMPLE_BUDGET_USDC, '5');
  assert.equal(store.snapshot().operator_funding, null);
  assert.equal(store.snapshot().total_funding_usdc, '0');
  const authorized = store.authorizeStartupSample();
  assert.deepEqual(authorized.credits, []);
  assert.equal(authorized.total_net_usdc, '0');
  assert.equal(authorized.operator_funding_usdc, '5');
  assert.equal(authorized.total_funding_usdc, '5');
  assert.equal(authorized.operator_funding.kind, 'operator_startup_sample_v1');
  assert.equal(authorized.operator_funding.source_account_id, '25923');
  assert.equal(authorized.operator_funding.destination_account_id, '120934');
  assert.equal(authorized.operator_funding.account_address, config.accountAddress);
  assert.ok(Number.isFinite(Date.parse(authorized.operator_funding.authorized_at)));
  assert.equal(authorized.funding.source_available_usdc, '5');
  assert.equal(authorized.funding.destination_available_usdc, '0');
  assert.equal(authorized.assets.ETH.allocated_usdc, '0');
  const reopened = createSpotAccumulationStore(db, config, { readOnly: false });
  assert.deepEqual(reopened.authorizeStartupSample(), authorized);
  assert.equal(db.prepare('SELECT count(*) n FROM spot_operator_funding').get().n, 1);
  assert.equal(db.prepare('SELECT count(*) n FROM spot_credits').get().n, 0);
});

test('sample spending includes fees, requires settled funding, and never replenishes on restart', t => {
  const { db, store } = harness(t);
  store.authorizeStartupSample();
  const buy = { ...request(), amount: '10', limit_price: '0.4', max_fee: '0.01' };
  assert.throws(() => store.reserve({ id: 'unfunded', asset: 'DRV', amount: '4.1', request: buy }), /funded destination/);
  assert.throws(() => store.reserveTransfer({ id: 'too-large', amount: '5.01', request: transferRequest({ amount: '5.01' }) }), /exceeds source/);
  store.reserveTransfer({ id: 'sample', amount: '5', request: transferRequest({ amount: '5' }) });
  store.claimTransferSend({ id: 'sample' });
  const restarted = createSpotAccumulationStore(db, config, { readOnly: false });
  assert.equal(restarted.authorizeStartupSample().funding.source_reserved_usdc, '5');
  assert.equal(restarted.snapshot().funding.source_available_usdc, '0');
  assert.throws(() => restarted.claimTransferSend({ id: 'sample' }), /never resend/);
  restarted.acknowledgeTransfer({ id: 'sample', result: { op_uuid: 'sample', operation_id: 1 } });
  restarted.reconcileTransfer({ id: 'sample', ...transferEvidence('sample', '5') });
  assert.throws(() => restarted.reserve({ id: 'over-budget', asset: 'DRV', amount: '5.01', request: buy }), /exceeds/);
  restarted.reserve({ id: 'sample-buy', asset: 'DRV', amount: '4.1', request: buy });
  restarted.reconcile({ id: 'sample-buy',
    order: { ...order(), ...buy, filled_amount: '10', average_price: '0.39', order_status: 'filled' },
    trades: [fill({ trade_amount: '10', trade_price: '0.39', trade_fee: '0.01' })] });
  const done = restarted.authorizeStartupSample();
  assert.equal(done.total_net_usdc, '0');
  assert.equal(done.total_funding_usdc, '5');
  assert.equal(done.funding.source_available_usdc, '0');
  assert.equal(done.funding.destination_available_usdc, '1.09');
  assert.equal(done.assets.DRV.settled_spend_usdc, '3.91');
  assert.deepEqual(createSpotAccumulationStore(db, config).snapshot(), done);
  // Later routine funding remains independently identified as actual sale credit.
  const ordinary = restarted.credit({ events: [source()], orders: [owned()] });
  assert.equal(ordinary.total_net_usdc, '100');
  assert.equal(ordinary.operator_funding_usdc, '5');
  assert.equal(ordinary.total_funding_usdc, '105');
  assert.equal(ordinary.funding.source_available_usdc, '100');
  assert.equal(ordinary.funding.destination_available_usdc, '1.09');
  assert.throws(() => restarted.authorizeStartupSample(), /put-sale credits/);
});

test('new sample authority cannot overwrite existing credits or execution history', t => {
  const { db, store } = harness(t);
  store.credit({ events: [source()], orders: [owned()] });
  assert.throws(() => store.authorizeStartupSample(), /put-sale credits/);
  assert.equal(db.prepare('SELECT count(*) n FROM spot_operator_funding').get().n, 0);
  db.prepare('DELETE FROM spot_credits').run();
  db.prepare('INSERT INTO spot_reservations(scope,id,asset,amount,request_json,status) VALUES(?,?,?,?,?,?)')
    .run('mainnet:v3:25923', 'old-zero', 'DRV', '0', JSON.stringify(request()), 'terminal');
  assert.throws(() => store.authorizeStartupSample(), /pristine execution/);
  assert.equal(db.prepare('SELECT count(*) n FROM spot_operator_funding').get().n, 0);
});

test('old read-only stores need no migration and modified sample authority fails closed', t => {
  const { db, store } = harness(t);
  db.exec('DROP TABLE spot_operator_funding');
  const before = db.serialize();
  const old = createSpotAccumulationStore(db, config);
  assert.equal(old.snapshot().operator_funding_usdc, '0');
  assert.equal(old.snapshot().total_funding_usdc, '0');
  assert.deepEqual(db.serialize(), before);
  const upgraded = createSpotAccumulationStore(db, config, { readOnly: false });
  upgraded.authorizeStartupSample();
  const saved = db.prepare('SELECT funding_json FROM spot_operator_funding').get();
  const changed = { ...JSON.parse(saved.funding_json), amount_usdc: '6' };
  db.prepare('UPDATE spot_operator_funding SET funding_json=?').run(JSON.stringify(changed));
  assert.throws(() => store.snapshot(), /fixed sample authority/);
  assert.throws(() => upgraded.authorizeStartupSample(), /fixed sample authority/);
});

test('stored credits replay exactly and immutable activation boundary prevents retroactive backfill', t => {
  const { db, store } = harness(t);
  assert.equal(store.credit({ events: [source()], orders: [owned()] }).inserted, 1);
  assert.equal(store.credit({ events: [source()], orders: [owned()] }).inserted, 0);
  assert.throws(() => store.credit({ events: [source('new'), source('put-1', { trade_price: '101' })], orders: [owned()] }), /Conflicting/);
  assert.equal(store.snapshot().credits.length, 1);
  assert.throws(() => createSpotAccumulationStore(db, { ...config, activationTimestamp: '2026-11-08T00:00:00Z' }), /boundary changed/);
  const testnet = createSpotAccumulationStore(db, { ...config, network: 'testnet' });
  assert.equal(testnet.snapshot().assets.DRV.available_usdc, '0');
});

test('DRV reservations cannot exceed their full allocation or release unknown outcomes', t => {
  const { store } = harness(t); store.credit({ events: [source()], orders: [owned()] }); fund(store);
  assert.throws(() => store.reserve({ id: 'too-large', asset: 'DRV', amount: '101', request: request() }), /exceeds/);
  assert.throws(() => store.reserve({ id: 'too-small', asset: 'DRV', amount: '40', request: request() }), /fee cap/);
  store.reserve({ id: 'drv-1', asset: 'DRV', amount: '42', request: request() });
  assert.equal(store.snapshot().assets.DRV.available_usdc, '58');
  assert.equal(store.snapshot().assets.ETH.available_usdc, '0');
  assert.throws(() => store.reserve({ id: 'drv-2', asset: 'DRV', amount: '42', request: { ...request(), nonce: '124' } }), /unresolved/);
  assert.throws(() => store.reconcile({ id: 'drv-1', order: null, trades: [] }), /evidence conflicts/);
  assert.equal(store.snapshot().assets.DRV.reserved_usdc, '42');
});

test('partial and terminal-but-unsettled fills keep cash reserved until every fill settles', t => {
  const { db, store } = harness(t); store.credit({ events: [source()], orders: [owned()] }); fund(store);
  store.reserve({ id: 'drv-1', asset: 'DRV', amount: '42', request: request() });
  store.reconcile({ id: 'drv-1', order: order(), trades: [fill()] });
  assert.deepEqual(store.snapshot().assets.DRV, { allocated_usdc: '100', settled_spend_usdc: '19.6', reserved_usdc: '22.4', available_usdc: '58' });
  const pending = fill({ trade_id: 'spot-fill-2', batch_status: 'Executing' });
  const closed = order({ filled_amount: '1', order_status: 'filled' });
  store.reconcile({ id: 'drv-1', order: closed, trades: [fill(), pending] });
  const reopened = createSpotAccumulationStore(db, config, { readOnly: false });
  assert.equal(reopened.snapshot().assets.DRV.available_usdc, '58');
  assert.equal(reopened.snapshot().reservations[0].status, 'pending');
  const settled = { ...pending, batch_status: 'Settled' };
  reopened.reconcile({ id: 'drv-1', order: closed, trades: [fill(), settled] });
  assert.deepEqual(reopened.snapshot().assets.DRV, { allocated_usdc: '100', settled_spend_usdc: '39.2', reserved_usdc: '0', available_usdc: '60.8' });
  reopened.reconcile({ id: 'drv-1', order: closed, trades: [fill(), settled] });
  assert.equal(reopened.snapshot().assets.DRV.settled_spend_usdc, '39.2');
});

test('settled evidence cannot disappear, mutate fees, or silently switch fee currency', t => {
  const { store } = harness(t); store.credit({ events: [source()], orders: [owned()] }); fund(store);
  store.reserve({ id: 'drv-1', asset: 'DRV', amount: '42', request: request() });
  store.reconcile({ id: 'drv-1', order: order(), trades: [fill()] });
  assert.throws(() => store.reconcile({ id: 'drv-1', order: order({ filled_amount: '0' }), trades: [] }), /conflicts/);
  assert.throws(() => store.reconcile({ id: 'drv-1', order: order(), trades: [fill({ trade_fee: '0.2' })] }), /changed/);
  assert.throws(() => store.reconcile({ id: 'drv-1', order: order(), trades: [fill({ fee_currency: 'ETH' })] }), /currency/);
  assert.equal(store.snapshot().assets.DRV.settled_spend_usdc, '19.6');
});

test('confirmed zero-fill terminal receipt releases a reservation and partial cancellation releases only unused cash', t => {
  const { store } = harness(t); store.credit({ events: [source()], orders: [owned()] }); fund(store);
  store.reserve({ id: 'drv-1', asset: 'DRV', amount: '42', request: request() });
  store.reconcile({ id: 'drv-1', order: order({ filled_amount: '0', order_status: 'cancelled' }), trades: [] });
  assert.equal(store.snapshot().assets.DRV.available_usdc, '100');
  assert.throws(() => store.reserve({ id: 'duplicate-nonce', asset: 'DRV', amount: '42', request: request() }), /nonce already reserved/);
  store.reserve({ id: 'drv-2', asset: 'DRV', amount: '42', request: { ...request(), nonce: '124' } });
  store.reconcile({ id: 'drv-2', order: order({ nonce: '124', order_status: 'cancelled' }), trades: [fill()] });
  assert.equal(store.snapshot().assets.DRV.available_usdc, '80.4');
});


test('observed unsettled fills cannot disappear into a zero-fill cancellation', t => {
  const { store } = harness(t); store.credit({ events: [source()], orders: [owned()] }); fund(store);
  store.reserve({ id: 'drv-1', asset: 'DRV', amount: '42', request: request() });
  store.reconcile({ id: 'drv-1', order: order(), trades: [fill({ batch_status: 'Executing' })] });
  assert.throws(() => store.reconcile({ id: 'drv-1', order: order({ filled_amount: '0', order_status: 'cancelled' }), trades: [] }), /disappeared/);
  assert.equal(store.snapshot().assets.DRV.reserved_usdc, '42');
});


test('conflicting source identity is rejected even when another variant would be filtered out', () => {
  const sold = source();
  for (const changed of [source('put-1', { direction: 'buy' }), source('put-1', { instrument_name: 'ETH-20261127-1600-C' })]) {
    for (const rows of [[sold, changed], [changed, sold]]) {
      const result = project(rows);
      assert.equal(result.complete, false); assert.equal(result.total_net_usdc, '0');
      assert.equal(result.credits.length, 0); assert.match(result.deferred[0].reason, /Conflicting duplicate/);
    }
  }
});

test('cumulative average price must be known and agree with exact fill notional before releasing cash', t => {
  const { store } = harness(t); store.credit({ events: [source()], orders: [owned()] }); fund(store);
  store.reserve({ id: 'drv-1', asset: 'DRV', amount: '42', request: request() });
  for (const value of [undefined, null, '', '0', '-1', 'NaN', 'Infinity', '1', '39.00001']) {
    assert.throws(() => store.reconcile({ id: 'drv-1', order: order({ filled_amount: '1', order_status: 'filled', average_price: value }), trades: [fill({ trade_amount: '1' })] }), /average price/);
    assert.equal(store.snapshot().assets.DRV.reserved_usdc, '42');
    assert.equal(store.snapshot().assets.DRV.settled_spend_usdc, '0');
  }
  // A venue-rounded cumulative average within 1e-6 USDC is accepted.
  store.reconcile({ id: 'drv-1', order: order({ filled_amount: '1', order_status: 'filled', average_price: '39.000000000001' }), trades: [fill({ trade_amount: '1' })] });
  assert.equal(store.snapshot().assets.DRV.settled_spend_usdc, '39.1');
  assert.equal(store.snapshot().assets.DRV.available_usdc, '60.9');
});


test('DRV-only policy rejects ETH, wrong destination and non-IOC reservations', t => {
  const { store } = harness(t); store.credit({ events: [source()], orders: [owned()] }); fund(store);
  assert.throws(() => store.reserve({ id: 'eth', asset: 'ETH', amount: '42', request: request('ETH') }), /Only DRV/);
  assert.throws(() => store.reserve({ id: 'source', asset: 'DRV', amount: '42', request: { ...request(), subaccount_id: 25923 } }), /identity conflicts/);
  assert.throws(() => store.reserve({ id: 'resting', asset: 'DRV', amount: '42', request: { ...request(), time_in_force: 'gtc' } }), /identity conflicts/);
  assert.equal(store.snapshot().assets.ETH.allocated_usdc, '0');
  assert.equal(store.snapshot().assets.DRV.available_usdc, '100');
  assert.equal(store.snapshot().reservations.length, 0);
});

test('old 50/50 configuration fails closed without reinterpreting credits or reservations', t => {
  const { db, store } = harness(t); store.credit({ events: [source()], orders: [owned()] }); fund(store);
  store.reserve({ id: 'drv-1', asset: 'DRV', amount: '42', request: request() });
  const current = store.snapshot().config;
  assert.equal(current.allocationPolicy, 'drv-only-v1');
  const { allocationPolicy, ...legacy } = current;
  db.prepare('UPDATE spot_accumulation_config SET config_json=?').run(JSON.stringify(legacy));
  db.prepare('UPDATE spot_reservations SET asset=?, request_json=?').run('ETH', JSON.stringify(request('ETH')));
  const before = db.serialize();
  for (const readOnly of [true, false]) {
    assert.throws(() => createSpotAccumulationStore(db, config, { readOnly }), /configuration\/boundary changed/);
  }
  assert.throws(() => store.snapshot(), /configuration\/boundary changed/);
  assert.deepEqual(db.serialize(), before);
  assert.throws(() => createSpotAccumulationStore(db, { ...config, allocationPolicy: 'eth-drv-50-50-v1' }), /allocation policy/);
});

test('only settled allocated transfers fund ALT; source credit and the setup balance do not', t => {
  const { store } = harness(t);
  assert.equal(store.snapshot().funding.destination_funded_usdc, '0');
  store.credit({ events: [source()], orders: [owned()] });
  assert.equal(store.snapshot().funding.source_available_usdc, '100');
  assert.equal(store.snapshot().funding.destination_available_usdc, '0');
  // An unrelated 5.01 USDC venue balance is deliberately not an input to this ledger.
  assert.throws(() => store.reserve({ id: 'unfunded', asset: 'DRV', amount: '42', request: request() }), /funded destination/);
  fund(store, '40');
  assert.deepEqual(store.snapshot().funding, { source_available_usdc: '60', source_reserved_usdc: '0',
    destination_funded_usdc: '40', destination_available_usdc: '40', transfer_fees_usdc: '0' });
  assert.equal(store.snapshot().assets.DRV.available_usdc, '100');
  assert.throws(() => store.reserve({ id: 'over-funded', asset: 'DRV', amount: '42', request: request() }), /funded destination/);
  store.reserve({ id: 'funded', asset: 'DRV', amount: '21', request: { ...request(), amount: '0.5' } });
  assert.equal(store.snapshot().funding.destination_available_usdc, '19');
  assert.equal(store.snapshot().funding.source_available_usdc, '60');
});

test('transfer reservations require exact signed existing-account zero-fee requests and immutable replay', t => {
  const { store } = harness(t); store.credit({ events: [source()], orders: [owned()] });
  for (const changes of [{ subaccount_id: 1 }, { to_subaccount_id: 25923 }, { asset_name: 'ETH' },
    { new_subaccount_manager: 3 }, { sub_id: 1 }, { max_fee_usd: '0.1' }, { amount: '99' },
    { signature: null }, { signer: null }, { nonce: '' }, { signature_expiry_sec: null }]) {
    assert.throws(() => store.reserveTransfer({ id: 'bad', amount: '100', request: transferRequest(changes) }));
  }
  assert.throws(() => store.reserveTransfer({ id: 'large', amount: '101', request: transferRequest({ amount: '101' }) }), /exceeds source/);
  const prepared = store.reserveTransfer({ id: 'transfer', amount: '40', request: transferRequest({ amount: '40' }) });
  assert.equal(prepared.funding.source_available_usdc, '60');
  assert.equal(prepared.funding.source_reserved_usdc, '40');
  assert.equal(prepared.funding.destination_available_usdc, '0');
  assert.equal(prepared.assets.DRV.available_usdc, '60');
  assert.equal(prepared.transfers[0].status, 'prepared');
  store.reserveTransfer({ id: 'transfer', amount: '40', request: transferRequest({ amount: '40' }) });
  assert.throws(() => store.reserveTransfer({ id: 'transfer', amount: '40', request: transferRequest({ amount: '40', nonce: '901' }) }), /Conflicting/);
  assert.throws(() => store.reserveTransfer({ id: 'same-nonce', amount: '40', request: transferRequest({ amount: '40' }) }), /nonce already/);
  assert.throws(() => store.reserveTransfer({ id: 'another', amount: '40', request: transferRequest({ amount: '40', nonce: '901' }) }), /unresolved/);
});

test('signed transfer is durable before send and UUID-less timeout stays frozen through restart', t => {
  const { db, store } = harness(t); store.credit({ events: [source()], orders: [owned()] });
  const signed = transferRequest();
  store.reserveTransfer({ id: 'transfer', amount: '100', request: signed });
  assert.throws(() => store.acknowledgeTransfer({ id: 'transfer', result: { op_uuid: 'operation', operation_id: 1 } }), /not claimed/);
  assert.deepEqual(store.claimTransferSend({ id: 'transfer' }), signed);
  const reopened = createSpotAccumulationStore(db, config, { readOnly: false });
  assert.equal(reopened.snapshot().transfers[0].status, 'unknown');
  assert.deepEqual(reopened.snapshot().transfers[0].request, signed);
  assert.throws(() => reopened.claimTransferSend({ id: 'transfer' }), /never resend/);
  assert.throws(() => reopened.reconcileTransfer({ id: 'transfer', ...transferEvidence() }), /UUID required/);
  assert.throws(() => reopened.acknowledgeTransfer({ id: 'transfer', result: { operation_id: 1 } }), /identity/);
  assert.equal(reopened.snapshot().funding.source_reserved_usdc, '100');
  assert.equal(reopened.snapshot().funding.destination_available_usdc, '0');
});

test('transfer settlement requires acknowledged UUID on both account histories, never numeric operation ID', t => {
  const { store } = harness(t); store.credit({ events: [source()], orders: [owned()] });
  store.reserveTransfer({ id: 'transfer', amount: '100', request: transferRequest() }); store.claimTransferSend({ id: 'transfer' });
  const result = { op_uuid: 'transfer-operation', operation_id: 42 };
  store.acknowledgeTransfer({ id: 'transfer', result }); store.acknowledgeTransfer({ id: 'transfer', result });
  assert.throws(() => store.acknowledgeTransfer({ id: 'transfer', result: { ...result, op_uuid: 'another' } }), /changed/);
  const numeric = transferEvidence(); numeric.history.source[0].operation_id = 42;
  assert.throws(() => store.reconcileTransfer({ id: 'transfer', ...numeric }), /Unique/);
  const evidence = transferEvidence();
  store.reconcileTransfer({ id: 'transfer', ...evidence });
  store.reconcileTransfer({ id: 'transfer', ...evidence });
  assert.equal(store.snapshot().funding.source_available_usdc, '0');
  assert.equal(store.snapshot().funding.source_reserved_usdc, '0');
  assert.equal(store.snapshot().funding.destination_available_usdc, '100');
  assert.throws(() => store.claimTransferSend({ id: 'transfer' }), /never resend/);
  assert.throws(() => store.reserveTransfer({ id: 'duplicate', amount: '100', request: transferRequest() }), /nonce already/);
  assert.throws(() => store.reserveTransfer({ id: 'same-number', amount: '100', request: transferRequest({ nonce: '0900' }) }), /nonce already/);
  const changed = transferEvidence(); changed.history.destination[0].amount = '99';
  assert.throws(() => store.reconcileTransfer({ id: 'transfer', ...changed }), /evidence changed/);
});

test('unsettled, failed, incomplete or conflicting transfer evidence never creates destination allocation', t => {
  const { store } = harness(t); store.credit({ events: [source()], orders: [owned()] });
  store.reserveTransfer({ id: 'transfer', amount: '100', request: transferRequest() }); store.claimTransferSend({ id: 'transfer' });
  store.acknowledgeTransfer({ id: 'transfer', result: { op_uuid: 'transfer-operation', operation_id: 42 } });
  for (const status of [null, 'Executing', 'Reverted']) {
    const pending = transferEvidence(); pending.operation.status = status;
    store.reconcileTransfer({ id: 'transfer', ...pending });
  }
  const mutations = [
    evidence => { evidence.operation.op_uuid = 'wrong'; },
    evidence => { evidence.operation.error_log = 'revert'; },
    evidence => { evidence.history.sourceAccountId = 1; },
    evidence => { evidence.history.destination = []; },
    evidence => { evidence.history.source.push({ ...evidence.history.source[0] }); },
    evidence => { evidence.history.destination[0].batch_status = 'Executing'; },
    evidence => { evidence.history.destination[0].is_outgoing = true; },
    evidence => { evidence.history.destination[0].asset = 'ETH'; },
    evidence => { evidence.history.destination[0].from_subaccount_id = 1; },
    evidence => { evidence.history.destination[0].to_wallet = '0x' + '33'.repeat(20); },
    evidence => { evidence.history.destination[0].amount = '99'; },
    evidence => { evidence.history.source[0].fee = '0.01'; },
    evidence => { delete evidence.history.source[0].fee; },
    evidence => { evidence.history.source[0].batch_uuid = 'another'; },
    evidence => { evidence.history.source[0].tx_hash = '0xdef'; },
    evidence => { evidence.history.source[0].timestamp = null; },
  ];
  for (const mutate of mutations) {
    const evidence = transferEvidence(); mutate(evidence);
    assert.throws(() => store.reconcileTransfer({ id: 'transfer', ...evidence }));
    assert.equal(store.snapshot().funding.source_reserved_usdc, '100');
    assert.equal(store.snapshot().funding.destination_available_usdc, '0');
  }
  const noHashes = transferEvidence(); noHashes.operation.transaction_hash = null;
  for (const row of [...noHashes.history.source, ...noHashes.history.destination]) delete row.tx_hash;
  store.reconcileTransfer({ id: 'transfer', ...noHashes });
  assert.equal(store.snapshot().funding.destination_available_usdc, '100');
});

test('transfer and spot pools conserve every credit through partial fills and repeated funding', t => {
  const { store } = harness(t); store.credit({ events: [source()], orders: [owned()] });
  fund(store, '50', 'first');
  store.reserve({ id: 'drv', asset: 'DRV', amount: '42', request: request() });
  store.reconcile({ id: 'drv', order: order(), trades: [fill()] });
  fund(store, '50', 'second', '901');
  const pending = store.snapshot();
  assert.equal(pending.funding.destination_available_usdc, '58');
  assert.equal(pending.funding.source_available_usdc, '0');
  assert.equal(pending.assets.DRV.settled_spend_usdc, '19.6');
  assert.equal(pending.assets.DRV.reserved_usdc, '22.4');
  store.reconcile({ id: 'drv', order: order({ order_status: 'cancelled' }), trades: [fill()] });
  assert.equal(store.snapshot().funding.destination_available_usdc, '80.4');
});

test('initialization is idempotent and changed owner, destination or funding policy fails before mutation', t => {
  const { db, store } = harness(t);
  createSpotAccumulationStore(db, config, { readOnly: false });
  assert.equal(db.prepare('SELECT count(*) n FROM spot_accumulation_config').get().n, 1);
  const before = db.serialize();
  for (const changes of [{ destinationAccountId: '1' }, { accountAddress: '0x' + '33'.repeat(20) }]) {
    assert.throws(() => createSpotAccumulationStore(db, { ...config, ...changes }, { readOnly: false }), /boundary changed/);
    assert.deepEqual(db.serialize(), before);
  }
  const { fundingPolicy, ...legacy } = store.snapshot().config;
  db.prepare('UPDATE spot_accumulation_config SET config_json=?').run(JSON.stringify(legacy));
  db.exec('DROP TABLE spot_transfers');
  const old = db.serialize();
  assert.throws(() => createSpotAccumulationStore(db, config, { readOnly: false }), /boundary changed/);
  assert.deepEqual(db.serialize(), old);
});

test('spot reservation can share an outer atomic submission transaction', t => {
  const { db, store } = harness(t); store.credit({ events: [source()], orders: [owned()] }); fund(store);
  assert.throws(() => db.transaction(() => {
    store.reserve({ id: 'drv', asset: 'DRV', amount: '42', request: request() });
    throw new Error('journal insert failed');
  }).immediate(), /journal insert failed/);
  assert.equal(store.snapshot().reservations.length, 0);
  assert.equal(store.snapshot().funding.destination_available_usdc, '100');
});

test('direct bound IOC refusal releases funded reservation once and preserves durable evidence', t => {
  const { db, store } = harness(t); store.credit({ events: [source()], orders: [owned()] }); fund(store);
  store.reserve({ id: 'drv', asset: 'DRV', amount: '42', request: request() });
  const proof = refusal();
  store.rejectOrder({ id: 'drv', error: proof });
  const reopened = createSpotAccumulationStore(db, config, { readOnly: false });
  reopened.rejectOrder({ id: 'drv', error: proof });
  const result = reopened.snapshot();
  assert.equal(result.reservations[0].status, 'terminal');
  assert.deepEqual(result.reservations[0].evidence, { rejection: proof });
  assert.equal(result.assets.DRV.settled_spend_usdc, '0');
  assert.equal(result.funding.destination_available_usdc, '100');
  assert.throws(() => reopened.reserve({ id: 'reuse', asset: 'DRV', amount: '42', request: request() }), /nonce already/);
  assert.throws(() => reopened.rejectOrder({ id: 'drv', error: refusal({ response: { error: { code: 11009, message: 'different refusal' } } }) }), /cannot replace/);
  assert.throws(() => reopened.reconcile({ id: 'drv', order: order(), trades: [fill()] }), /cannot be replaced/);
});

test('unknown errors, accepted response identity and mismatched requests cannot release an order', t => {
  const { store } = harness(t); store.credit({ events: [source()], orders: [owned()] }); fund(store);
  store.reserve({ id: 'drv', asset: 'DRV', amount: '42', request: request() });
  for (const proof of [new Error('timeout'), refusal({ method: 'private/transfer_spot' }),
    refusal({ response: { error: { code: 999999, message: 'unknown outcome' } } }),
    refusal({ response: { error: { code: 11009 }, result: { order: { order_id: 'accepted' } } } }),
    refusal({ request: { ...request(), nonce: '124' } })]) {
    assert.throws(() => store.rejectOrder({ id: 'drv', error: proof }));
    assert.equal(store.snapshot().funding.destination_available_usdc, '58');
    assert.equal(store.snapshot().reservations[0].status, 'unknown');
  }
  assert.throws(() => store.rejectOrder({ id: 'missing', error: refusal() }), /conflicts/);
});

test('direct refusal cannot replace known zero-fill acceptance, unsettled fill or settled spend', t => {
  for (const scenario of ['accepted', 'unsettled', 'settled']) {
    const { store } = harness(t); store.credit({ events: [source()], orders: [owned()] }); fund(store);
    store.reserve({ id: 'drv', asset: 'DRV', amount: '42', request: request() });
    store.reconcile({ id: 'drv', order: scenario === 'accepted' ? order({ filled_amount: '0' }) : order(),
      trades: scenario === 'accepted' ? [] : [fill(scenario === 'unsettled' ? { batch_status: 'Executing' } : {})] });
    assert.throws(() => store.rejectOrder({ id: 'drv', error: refusal() }), /cannot replace/);
    assert.equal(store.snapshot().funding.destination_available_usdc, '58');
    assert.equal(store.snapshot().reservations[0].status, 'pending');
  }
});

test('refusal release rolls back with the outer journal transaction and read-only access cannot release', t => {
  const { db, store } = harness(t); store.credit({ events: [source()], orders: [owned()] }); fund(store);
  store.reserve({ id: 'drv', asset: 'DRV', amount: '42', request: request() });
  assert.throws(() => db.transaction(() => {
    store.rejectOrder({ id: 'drv', error: refusal() });
    throw new Error('journal write failed');
  }).immediate(), /journal write failed/);
  assert.equal(store.snapshot().funding.destination_available_usdc, '58');
  assert.equal(store.snapshot().reservations[0].evidence, null);
  assert.throws(() => createSpotAccumulationStore(db, config).rejectOrder({ id: 'drv', error: refusal() }), /Read-only/);
});
