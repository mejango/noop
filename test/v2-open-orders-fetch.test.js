'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const { loadProduction } = require('./helpers/load-production');

function fixture(data, transportError) {
  const logs = [];
  const { fetchOpenOrders } = loadProduction(['fetchOpenOrders'], { bindings: {
    API_URL: { GET_OPEN_ORDERS: 'fixture://open-orders' },
    SUBACCOUNT_ID: 7, DERIVE_ACCOUNT_ADDRESS: 'fixture-wallet',
    createWallet: () => ({}), signMessage: async () => 'fixture-signature',
    console: { error: message => logs.push(message) },
    axios: { post: async (url, params, config) => {
      assert.equal(url, 'fixture://open-orders');
      assert.deepEqual(params, { subaccount_id: 7 });
      assert.equal(config.timeout, 10000);
      if (transportError) throw transportError;
      return { data };
    } },
  } });
  return { fetchOpenOrders, logs };
}

test('open-order fetch accepts empty and populated supported response envelopes', async () => {
  const order = {
    order_id: 'order-1', instrument_name: 'ETH-20301211-2500-P', direction: 'sell',
    amount: '2', filled_amount: '0.5', limit_price: '100', average_price: '100',
    order_status: 'open', nonce: '123', time_in_force: 'gtc',
    creation_timestamp: 1234, last_update_timestamp: 1235,
  };
  for (const orders of [[], [order]]) {
    for (const result of [orders, { subaccount_id: 7, orders }]) {
      const subject = fixture({ id: 1, result });
      assert.deepEqual(await subject.fetchOpenOrders({ throwOnError: true }), orders);
      assert.deepEqual(subject.logs, []);
    }
  }
});

test('open-order venue errors retain their cause and reject even alongside an orders array', async () => {
  for (const result of [undefined, { orders: [] }]) {
    const subject = fixture({ error: { code: -32603, message: 'Internal error' }, result });
    await assert.rejects(subject.fetchOpenOrders({ throwOnError: true }),
      /Open order state unavailable: .*"code":-32603.*"message":"Internal error"/);
    assert.match(subject.logs[0], /-32603.*Internal error/);
  }
});

test('open-order error details are bounded', async () => {
  const subject = fixture({ error: { code: 9000, message: 'Order confirmation timeout ' + 'x'.repeat(1000) } });
  await assert.rejects(subject.fetchOpenOrders({ throwOnError: true }), error => {
    assert.match(error.message, /9000.*Order confirmation timeout/);
    assert.equal(error.message.length, 'Open order state unavailable: '.length + 300);
    return true;
  });
});

test('malformed open-order responses remain unavailable rather than becoming an empty book', async () => {
  for (const data of [undefined, {}, { result: null }, { result: {} }, { result: { orders: null } }, { result: { orders: {} } }]) {
    const subject = fixture(data);
    await assert.rejects(subject.fetchOpenOrders({ throwOnError: true }), /^Error: Open order state unavailable$/);
  }
});

test('strict fetch propagates transport errors while optional fetch preserves its existing fallback', async () => {
  const transportError = Object.assign(new Error('Request failed with status code 503'), { response: { status: 503 } });
  const transport = fixture(undefined, transportError);
  await assert.rejects(transport.fetchOpenOrders({ throwOnError: true }), error => error === transportError);
  for (const subject of [transport, fixture({ error: { code: -32603, message: 'Internal error' } }), fixture({})]) {
    assert.deepEqual(await subject.fetchOpenOrders(), []);
    assert.ok(subject.logs.length > 0);
  }
});
