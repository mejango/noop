'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Wallet } = require('ethers');
const { DeriveV3, validatePortfolio } = require('../integrations/derive-v3');
const { validateAccountRows } = require('../integrations/derive-v3/legacy-transport');
const sdk = require('../integrations/derive-v3/node_modules/@derivexyz/derive-ts');

const wallet = Wallet.createRandom();
const profile = { name: 'v3-testnet', version: 3, network: 'testnet', prefix: 'DERIVE_V3_TESTNET_',
  ownerAddress: wallet.address, subaccountId: 987654320, executionEnabled: true };
const portfolio = { subaccount_id: profile.subaccountId, manager_id: 1, risk_universe_id: 1,
  failed_to_fetch: false, is_under_liquidation: false,
  initial_margin: '100', maintenance_margin: '100', subaccount_value: '100', positions: [], collaterals: [] };
const params = () => ({ subaccountId: profile.subaccountId, instrumentName: 'ETH-PERP', direction: 'buy',
  amount: '0.01', limitPrice: '1000', maxFee: '10', reduceOnly: false, timeInForce: 'post_only' });
const receipt = wire => ({ order: { ...wire, order_id: 'audit-order', order_status: 'open', filled_amount: '0', average_price: '0' }, trades: [] });

function harness() {
  const client = new sdk.DeriveClient({ network: 'testnet', sessionKey: wallet, ownerAddress: wallet.address });
  const calls = [];
  const events = [];
  const hooks = { instrument: () => ({ base_asset_address: '0x0000000000000000000000000000000000000001', base_asset_sub_id: '0' }), order: receipt };
  client.send = async (method, wire) => {
    calls.push(method);
    if (method === 'private/get_subaccounts') return { subaccount_ids: [profile.subaccountId] };
    if (method === 'private/get_subaccount') return portfolio;
    if (method === 'public/get_instrument') return hooks.instrument();
    if (method === 'private/order') return hooks.order(wire);
    throw new Error(`Unexpected offline test route: ${method}`);
  };
  const adapter = new DeriveV3({ profile, client, journal: event => events.push(event) });
  return { adapter, client, calls, events, hooks };
}

test('SDK instrument lookup failure is pre-send and does not create an unreconcilable intent', async () => {
  const h = harness();
  const goodInstrument = h.hooks.instrument;
  h.hooks.instrument = () => { throw new Error('instrument lookup timed out'); };
  await assert.rejects(h.adapter.place(params()), /instrument lookup timed out/);
  assert.equal(h.calls.includes('private/order'), false);
  assert.deepEqual(h.events, []);
  assert.equal(h.adapter.pendingOrder, null);
  h.hooks.instrument = goodInstrument;
  await h.adapter.place(params());
  assert.equal(h.calls.filter(m => m === 'private/order').length, 1);
  assert.deepEqual(h.events.map(e => e.event), ['order_intent', 'order_ack']);
  h.adapter.markAccounted('audit-order');
  await h.client.close();
});

test('SDK decimal validation completes before recording an order intent', async () => {
  const h = harness();
  await assert.rejects(h.adapter.place({ ...params(), amount: 'not-a-decimal' }));
  assert.deepEqual(h.events, []);
  assert.equal(h.calls.includes('private/order'), false);
  assert.equal(h.adapter.pendingOrder, null);
  await h.client.close();
});

test('venue acknowledgement remains blocked until application accounting succeeds', async () => {
  const h = harness();
  await h.adapter.place(params());
  assert.ok(h.adapter.pendingOrder);
  await assert.rejects(h.adapter.place(params()), /Unresolved/);
  assert.throws(() => h.adapter.markAccounted('different-order'), /mismatch/);
  assert.equal(h.adapter.markAccounted('audit-order'), true);
  assert.equal(h.adapter.pendingOrder, null);
  assert.equal(h.adapter.markAccounted(), false);
  assert.deepEqual(h.events.map(e => e.event), ['order_intent', 'order_ack', 'order_accounted']);
  await h.adapter.place(params());
  assert.equal(h.calls.filter(m => m === 'private/order').length, 2);
  await h.client.close();
});

test('failure to persist accounting completion retains the execution block', async () => {
  const h = harness();
  await h.adapter.place(params());
  h.adapter.journal = () => { throw new Error('fsync failed'); };
  assert.throws(() => h.adapter.markAccounted(), /fsync failed/);
  assert.ok(h.adapter.pendingOrder);
  await assert.rejects(h.adapter.place(params()), /Unresolved/);
  await h.client.close();
});

test('uncertain sends and internal/numeric transport errors cannot be marked accounted or retried', async () => {
  for (const error of [new Error('timeout after send'), ...[-32603, 9000, 9001, 11017].map(code => Object.assign(new Error('uncertain RPC outcome'), { code })),
    new DOMException('request aborted after send', 'AbortError')]) {
    const h = harness();
    h.hooks.order = () => { throw error; };
    await assert.rejects(h.adapter.place(params()));
    assert.deepEqual(h.events.map(e => e.event), ['order_intent', 'order_unknown']);
    assert.throws(() => h.adapter.markAccounted(), /unknown/);
    await assert.rejects(h.adapter.place(params()), /Unresolved/);
    assert.equal(h.calls.filter(m => m === 'private/order').length, 1);
    await h.client.close();
  }
});

test('missing trades or incorrect receipt identity cannot masquerade as zero fills', async () => {
  const mutations = [
    r => ({ ...r, order: { ...r.order, order_status: 'filled', filled_amount: '0.01' } }),
    r => ({ ...r, order: { ...r.order, nonce: 'wrong-nonce' } }),
    r => ({ ...r, order: { ...r.order, subaccount_id: 42 } }),
    r => ({ ...r, order: { ...r.order, filled_amount: '-1' } }),
    r => ({ order: r.order }),
  ];
  for (const mutate of mutations) {
    const h = harness();
    h.hooks.order = wire => mutate(receipt(wire));
    await assert.rejects(h.adapter.place(params()), /acknowledgement/);
    assert.deepEqual(h.events.map(e => e.event), ['order_intent', 'order_unknown']);
    assert.throws(() => h.adapter.markAccounted(), /unknown/);
    await h.client.close();
  }
});

test('documented maker and maximum-fee rejections permit another attempt with a new nonce', async () => {
  for (const code of [11008, 11023]) {
    const h = harness();
    h.hooks.order = () => { throw Object.assign(new Error('documented pre-book rejection'), { code }); };
    await assert.rejects(h.adapter.place(params()));
    assert.equal(h.adapter.pendingOrder, null);
    assert.equal(h.adapter.markAccounted(), false);
    h.hooks.order = receipt;
    await h.adapter.place(params());
    const intents = h.events.filter(e => e.event === 'order_intent');
    assert.notEqual(intents[0].nonce, intents[1].nonce);
    assert.equal(h.events[1].event, 'order_rejected');
    await h.client.close();
  }
});

test('portfolio flags and margin amounts cannot silently default to a healthy account', () => {
  for (const invalid of [
    { failed_to_fetch: undefined }, { is_under_liquidation: undefined },
    { is_under_liquidation: 'false' }, { initial_margin: true }, { initial_margin: [] },
    { initial_margin: 'Infinity' }, { risk_universe_id: -1 }, { manager_id: -1 },
    { subaccount_id: String(profile.subaccountId) },
    { positions: [{ instrument_name: 'ETH-PERP', amount: 'NaN', initial_margin: '0', maintenance_margin: '0' }] },
  ]) assert.throws(() => validatePortfolio({ ...portfolio, ...invalid }, profile));
  assert.equal(validatePortfolio(portfolio, profile), portfolio);
});

test('a complete IOC fill retains exact quantity accounting and requires the application completion marker', async () => {
  const h = harness();
  h.hooks.order = wire => ({ order: { ...wire, order_id: 'audit-order', order_status: 'filled', filled_amount: wire.amount, average_price: wire.limit_price },
    trades: [{ order_id: 'audit-order', subaccount_id: wire.subaccount_id, instrument_name: wire.instrument_name,
      direction: wire.direction, trade_amount: wire.amount, trade_price: wire.limit_price }] });
  const result = await h.adapter.place({ ...params(), timeInForce: 'ioc' });
  assert.equal(result.order.filled_amount, '0.01');
  assert.ok(h.adapter.pendingOrder);
  h.adapter.markAccounted(result.order.order_id);
  await h.client.close();
});

test('separate account read routes reject absent arrays and wrong identities', () => {
  for (const [method, field] of [['private/get_positions', 'positions'], ['private/get_collaterals', 'collaterals'], ['private/get_open_orders', 'orders']]) {
    assert.throws(() => validateAccountRows(method, {}, profile), /malformed/);
    assert.throws(() => validateAccountRows(method, { subaccount_id: profile.subaccountId, [field]: null }, profile), /malformed/);
    assert.throws(() => validateAccountRows(method, { subaccount_id: 42, [field]: [] }, profile), /wrong-subaccount/);
    assert.deepEqual(validateAccountRows(method, { subaccount_id: profile.subaccountId, [field]: [] }, profile)[field], []);
  }
});

test('V3 position Greeks are mapped for the strategy and malformed position quantities fail closed', () => {
  const position = { instrument_name: 'ETH-PERP', amount: '-1', average_price: '3000', mark_price: '3100', index_price: '3100',
    unrealized_pnl: '-100', delta: '1', theta: '0', vega: '0' };
  const value = { subaccount_id: profile.subaccountId, positions: [position] };
  const normalized = validateAccountRows('private/get_positions', value, profile);
  assert.deepEqual(normalized.positions[0].greeks, { delta: '1', theta: '0', vega: '0' });
  assert.equal(normalized.positions[0].amount, '-1');
  for (const amount of ['NaN', '', '0x100', 'Infinity']) {
    assert.throws(() => validateAccountRows('private/get_positions', { ...value, positions: [{ ...position, amount }] }, profile), /amount/);
  }
  assert.throws(() => validateAccountRows('private/get_positions', { ...value, positions: [{ ...position, delta: undefined }] }, profile), /delta/);
});

test('wrong-account and overfilled open orders cannot bypass exposure accounting', () => {
  const order = { subaccount_id: profile.subaccountId, order_id: 'audit-order', instrument_name: 'ETH-PERP',
    direction: 'buy', order_status: 'open', amount: '1', filled_amount: '0.5', limit_price: '3000' };
  const value = { subaccount_id: profile.subaccountId, orders: [order] };
  assert.deepEqual(validateAccountRows('private/get_open_orders', value, profile), value);
  assert.throws(() => validateAccountRows('private/get_open_orders', { ...value, orders: [{ ...order, subaccount_id: 42 }] }, profile), /identity/);
  assert.throws(() => validateAccountRows('private/get_open_orders', { ...value, orders: [{ ...order, filled_amount: '2' }] }, profile), /quantities/);
});
