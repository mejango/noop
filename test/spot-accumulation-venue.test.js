'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { Wallet } = require('ethers');
const { getDeriveConfig } = require('../bot/derive-config');
const { createSpotVenue, sourceCashCommitments } = require('../bot/spot-accumulation-venue');
const { USDC_ASSET_ADDRESS } = require('../bot/derive-transfer');
const NOW = 1794324267506;
const OWNER = '0x1111111111111111111111111111111111111111';
const wallet = Wallet.createRandom(); // Test-only identity, never a production credential.
const config = { ...getDeriveConfig({ DERIVE_API_VERSION: 'v3', DERIVE_NETWORK: 'mainnet', DERIVE_WALLET: OWNER,
  DERIVE_SUBACCOUNT_ID: '25923', DERIVE_HISTORY_FROM: '2026-11-10T00:00:00Z', DERIVE_MAINTENANCE: 'false' }),
  accountAddress: OWNER, destinationSubaccountId: 120934 };
const order = { subaccount_id: 120934, instrument_name: 'DRV-USDC', direction: 'buy', amount: '10', limit_price: '0.1005',
  max_fee: '0.002', order_type: 'limit', time_in_force: 'ioc', reduce_only: false, signature_expiry_sec: Math.ceil(NOW / 1000) + 800 };
const transfer = { from_subaccount_id: 25923, to_subaccount_id: 120934, asset_name: 'USDC', amount: '600', max_fee_usdc: '0', new_subaccount_manager: 0 };
const testJournal = () => {};
function fixture(change = (_method, _params, value) => value, options = {}) {
  const calls = [];
  let canSend = true, actionSignatures = 0;
  const signingWallet = { address: wallet.address, signMessage: wallet.signMessage.bind(wallet), signingKey: { sign(hash) {
    actionSignatures++; return wallet.signingKey.sign(hash);
  } } };
  const post = async (method, p) => {
    calls.push({ method, p: structuredClone(p) });
    let result;
    switch (method) {
      case 'public/get_instrument': result = { instrument_name: 'DRV-USDC', instrument_type: 'erc20', is_active: true,
        base_currency: 'DRV', quote_currency: 'USDC', risk_universe_id: 3, base_asset_address: USDC_ASSET_ADDRESS,
        base_asset_sub_id: '0', tick_size: '0.0001', amount_step: '1', minimum_amount: '10', maximum_amount: '100000' }; break;
      case 'public/get_ticker': result = { a: '0.1', A: '1000', t: NOW }; break;
      case 'private/session_keys': result = { public_session_keys: [{ public_session_key: wallet.address,
        expiry_sec: Math.ceil(NOW / 1000) + 3600, subaccount_ids: [25923, 120934], offchain_scopes: ['account_info'],
        protocol_scopes: ['trade:orderbook:spot', 'transfer:existing_subaccount'] }] }; break;
      case 'private/get_account': result = { fee_info: { spot_taker_fee: '0.0009' } }; break;
      case 'private/get_subaccount': result = { subaccount_id: p.subaccount_id, risk_universe_id: p.subaccount_id === 25923 ? 1 : 3,
        is_under_liquidation: false, initial_margin: '1000', maintenance_margin: '1100', collaterals: [{ asset_name: 'USDC', currency: 'USDC', amount: '1000' }] }; break;
      case 'public/get_wallet': result = { wallet: OWNER }; break;
      case 'private/get_open_orders': result = []; break;
      case 'private/order_quote': result = { is_valid: p.max_fee !== '0', post_initial_margin: '900', suggested_max_fee: '0.001' }; break;
      case 'private/get_margin': result = { is_valid_trade: true, post_initial_margin: '900', post_maintenance_margin: '950' }; break;
      case 'public/get_all_currencies': result = [{ currency: 'USDC', spot: [{ name: 'USDC', address: USDC_ASSET_ADDRESS,
        erc20: { decimals: 6, underlying_erc20: '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' } }] }]; break;
      case 'private/order': result = { order: { order_id: 'new-order' } }; break;
      case 'private/transfer_spot': result = { op_uuid: '12345678-1234-1234-1234-123456789012', operation_id: 42 }; break;
      case 'public/get_transaction': result = { status: 'Settled', transaction_hash: '0xabc' }; break;
      case 'private/get_erc20_transfer_history': result = { transfers: [{ operation_id: '12345678-1234-1234-1234-123456789012', account: p.subaccount_id }] }; break;
      case 'private/get_order_history': result = { orders: [{ ...order, order_id: 'recovered', nonce: '1234', order_status: 'filled', filled_amount: '10' }] }; break;
      case 'private/get_order': result = { ...order, order_id: 'recovered', nonce: '1234', order_status: 'filled', filled_amount: '10' }; break;
      case 'private/get_trade_history': result = { trades: [{ trade_id: 'trade1', order_id: 'recovered', subaccount_id: 120934,
        instrument_name: 'DRV-USDC', direction: 'buy', trade_amount: '10', trade_price: '0.1005', trade_fee: '0.01', batch_status: 'Settled' }] }; break;
      default: throw new Error(`Unexpected endpoint ${method}`);
    }
    return change(method, p, result);
  };
  return { calls, post, get signatures() { return actionSignatures; }, setCanSend(value) { canSend = value; },
    venue: createSpotVenue({ config, wallet: signingWallet, post, now: () => NOW,
      assertCanSend: () => { if (!canSend) throw new Error('live mode disabled'); }, ...options }) };
}
const snapshot = io => io.venue.readSnapshot({ cycleBudget: '100', pendingActions: [] });

test('cash commitments reserve remaining buys and queued option debits exactly once', () => {
  const open = { order_id: 'o1', subaccount_id: 25923, instrument_name: 'ETH-option-P', direction: 'buy', amount: '2', filled_amount: '0.5', limit_price: '20', max_fee: '1' };
  const pending = { id: 1, status: 'confirmed', action: 'buy_put', instrument_name: 'ETH-option-P', amount: 2, price: 20, max_fee: '1', order_id: 'o1' };
  const queued = { id: 2, status: 'pending', action: 'buyback_call', amount: '0.5', price: '10', max_fee: '0.2' };
  assert.equal(sourceCashCommitments({ openOrders: [open], pendingActions: [pending, queued], subaccountId: 25923 }), '36.6');
  for (const bad of [{ ...open, max_fee: undefined }, { ...open, limit_price: undefined }, { ...open, filled_amount: '3' }]) {
    assert.throws(() => sourceCashCommitments({ openOrders: [bad], pendingActions: [], subaccountId: 25923 }));
  }
  assert.throws(() => sourceCashCommitments({ openOrders: [], pendingActions: [{ ...queued, max_fee: undefined }], subaccountId: 25923 }), /fee cap/);
  assert.throws(() => sourceCashCommitments({ openOrders: [open, open], pendingActions: [], subaccountId: 25923 }), /Duplicate/);
  assert.throws(() => sourceCashCommitments({ openOrders: [open], pendingActions: [{ ...pending, instrument_name: 'wrong' }], subaccountId: 25923 }), /conflicts/);
  const savedAction = { ...pending, order_id: undefined, max_fee: undefined, execution_result: JSON.stringify({ orderId: 'o1' }) };
  assert.equal(sourceCashCommitments({ openOrders: [open], pendingActions: [savedAction], subaccountId: 25923 }), '31.5',
    'canonical saved option orderId reuses the live order fee and does not double reserve');
  assert.throws(() => sourceCashCommitments({ openOrders: [open], pendingActions: [{ ...savedAction,
    execution_result: JSON.stringify({ orderId: 'o1', order_id: 'another-order' }) }], subaccountId: 25923 }), /Conflicting pending order links/);
  assert.throws(() => sourceCashCommitments({ openOrders: [{ ...open, instrument_name: 'wrong' }], pendingActions: [savedAction], subaccountId: 25923 }), /conflicts/);
  assert.throws(() => sourceCashCommitments({ openOrders: [{ ...open, direction: 'sell' }], pendingActions: [savedAction], subaccountId: 25923 }), /conflicts/);
});

test('read snapshot and exact previews use unsigned request auth only, preserve account identity and fee-inclusive cash', async () => {
  const io = fixture();
  const result = await snapshot(io);
  assert.equal(result.source.owner, OWNER);
  assert.equal(result.destination.subaccount_id, 120934);
  assert.equal(result.source.usdc_balance, '1000');
  assert.equal(result.source.committed_usdc, '0');
  assert.equal(result.fees.DRV.max_fee_per_unit, '0.001000000001');
  assert.equal((await io.venue.previewOrder(order)).valid, true);
  assert.equal((await io.venue.previewTransfer(transfer)).valid, true);
  assert.equal(io.signatures, 0);
  assert.ok(io.calls.every(c => c.method.startsWith('public/get_') || c.method.startsWith('private/get_')
    || ['private/session_keys', 'private/order_quote'].includes(c.method)));
  for (const quote of io.calls.filter(c => c.method === 'private/order_quote')) {
    assert.equal(quote.p.signature, '0x'); assert.equal(quote.p.nonce, '0'); assert.equal(quote.p.subaccount_id, 120934);
  }
  assert.ok(io.calls.some(c => c.method === 'private/get_margin' && c.p.subaccount_id === 120934
    && c.p.simulated_collateral_changes[1].amount === '-1.025'));
  assert.ok(io.calls.some(c => c.method === 'private/get_margin' && c.p.subaccount_id === 25923
    && c.p.simulated_collateral_changes[0].amount === '-600'));
});

test('unknown scope, owner, stale price, missing fees and nonpositive margins defer', async () => {
  const changes = [
    ['private/session_keys', value => { delete value.public_session_keys[0].subaccount_ids; return value; }],
    ['private/session_keys', value => { value.public_session_keys[0].protocol_scopes = ['unknown']; return value; }],
    ['private/session_keys', value => { value.public_session_keys[0].offchain_scopes = []; return value; }],
    ['public/get_wallet', () => ({ wallet: wallet.address })],
    ['public/get_ticker', value => ({ ...value, t: NOW - 30001 })],
    ['private/order_quote', value => ({ ...value, suggested_max_fee: undefined })],
  ];
  for (const margin of [undefined, null, '', '0', '-1', 'Infinity', true]) {
    changes.push(['private/get_subaccount', value => ({ ...value, initial_margin: margin })]);
    changes.push(['private/get_subaccount', value => ({ ...value, maintenance_margin: margin })]);
  }
  for (const [endpoint, modify] of changes) {
    const io = fixture((method, _p, value) => method === endpoint ? modify(value) : value);
    await assert.rejects(snapshot(io)); assert.equal(io.signatures, 0);
  }
});

test('fresh preview protects source float and destination cash, metadata, fee and margin bounds', async () => {
  const io = fixture(); await snapshot(io);
  await assert.rejects(io.venue.previewTransfer({ ...transfer, amount: '600.000001' }), /operating reserves/);
  for (const change of [{ new_subaccount_manager: 3 }, { max_fee_usdc: '0.01' }, { amount: '0.0000001' }, { to_subaccount_id: 25923 }]) {
    await assert.rejects(io.venue.previewTransfer({ ...transfer, ...change }));
  }
  for (const change of [{ subaccount_id: 25923 }, { instrument_name: 'ETH-USDC' }, { time_in_force: 'gtc' },
    { amount: '1001' }, { amount: '10.5' }, { limit_price: '0.1006' }, { max_fee: '0.001' }, { max_fee: '0.0010000000001' }, { max_fee: '100' }]) {
    await assert.rejects(io.venue.previewOrder({ ...order, ...change }));
  }
  for (const endpoint of ['private/order_quote', 'private/get_margin']) {
    const invalid = fixture((method, params, value) => method === endpoint && params.max_fee !== '0' ? { ...value, post_initial_margin: '0' } : value);
    await snapshot(invalid); await assert.rejects(invalid.venue.previewOrder(order), /margin/);
  }
});

test('financial preparation and send are explicitly gated, bind exact terms, and cannot be replayed', async () => {
  const disabled = fixture(undefined, { assertCanSend: undefined }); await snapshot(disabled);
  await assert.rejects(disabled.venue.prepareOrder(order), /disabled/);
  await assert.rejects(disabled.venue.prepareTransfer(transfer), /disabled/);
  assert.equal(disabled.signatures, 0);
  const maintenance = fixture(undefined, { config: { ...config, maintenance: true } }); await snapshot(maintenance);
  await assert.rejects(maintenance.venue.prepareOrder(order), /maintenance/);

  const io = fixture(); await snapshot(io);
  const signed = await io.venue.prepareOrder(order);
  assert.equal(signed.signature.length, 132); assert.notEqual(signed.nonce, '0');
  await assert.rejects(io.venue.submitOrder(signed), /journal callback/);
  await assert.rejects(io.venue.submitOrder(signed, async () => {}), /journal callback/);
  await assert.rejects(io.venue.submitOrder({ ...signed, amount: '11' }, testJournal), /exact freshly prepared/);
  io.setCanSend(false); await assert.rejects(io.venue.submitOrder(signed, testJournal), /live mode disabled/);
  assert.ok(!io.calls.some(c => c.method === 'private/order'));
  io.setCanSend(true); await io.venue.submitOrder(signed, testJournal);
  await assert.rejects(io.venue.submitOrder(signed, testJournal), /exact freshly prepared/);
  assert.equal(io.calls.filter(c => c.method === 'private/order').length, 1);
});

test('transfer signing, exact send and UUID recovery preserve both account routes', async () => {
  const io = fixture(); await snapshot(io);
  const signed = await io.venue.prepareTransfer(transfer);
  assert.equal(signed.subaccount_id, 25923); assert.equal(signed.to_subaccount_id, 120934);
  assert.equal(signed.max_fee_usd, '0'); assert.equal(signed.signature.length, 132);
  await assert.rejects(io.venue.submitTransfer({ ...signed, amount: '1' }, testJournal), /exact freshly prepared/);
  const ack = await io.venue.submitTransfer(signed, testJournal);
  await assert.rejects(io.venue.submitTransfer(signed, testJournal), /exact freshly prepared/);
  const recovered = await io.venue.collectTransfer(signed, ack);
  assert.equal(recovered.operation.op_uuid, ack.op_uuid);
  assert.equal(recovered.history.sourceAccountId, 25923); assert.equal(recovered.history.destinationAccountId, 120934);
  assert.deepEqual(io.calls.filter(c => c.method === 'private/get_erc20_transfer_history').map(c => c.p.subaccount_id), [25923, 120934]);
  await assert.rejects(io.venue.collectTransfer(signed, null), /reservation remains frozen/);
  await assert.rejects(io.venue.collectOrder({ ...order, subaccount_id: 25923 }), /destination/);
});

test('pinned authenticated HTTP origin has no redirect or unrelated mutation escape', async () => {
  assert.throws(() => createSpotVenue({ config: { ...config, baseUrl: 'https://example.com' }, wallet }), /does not match/);
  const io = fixture(); const calls = [];
  const venue = createSpotVenue({ config, wallet, now: () => NOW, http: { post: async (url, params, options) => {
    calls.push({ url, options }); return { data: { result: await io.post(url.slice(config.baseUrl.length + 1), params) } };
  } } });
  await venue.readSnapshot({ cycleBudget: '100', pendingActions: [] });
  assert.ok(calls.every(c => c.url.startsWith('https://api.derive.xyz/v3/') && c.options.maxRedirects === 0));
  assert.ok(calls.filter(c => c.url.includes('/private/')).every(c => c.options.headers['X-DeriveWallet'] === OWNER));
  await assert.rejects(venue.submitOrder({ ...order, signature: 'fake' }, testJournal), /disabled/);
});

test('DRV trade recovery resolves omitted quote fee units with raw evidence and rejects contradictory units', async () => {
  const io = fixture();
  const result = await io.venue.collectOrder({ ...order, nonce: '1234' });
  assert.equal(result.trades[0].fee_currency, 'USDC');
  assert.equal(result.trades[0].fee_currency_source, 'verified_DRV-USDC_quote_currency_and_Derive_trade_fee_contract');
  assert.equal(result.trades[0].raw_venue_trade.fee_currency, undefined);
  assert.ok(io.calls.filter(c => c.method.startsWith('private/get_')).every(c => c.p.subaccount_id === 120934));
  const conflict = fixture((method, _params, value) => method === 'private/get_trade_history'
    ? { trades: value.trades.map(trade => ({ ...trade, fee_currency: 'DRV' })) } : value);
  await assert.rejects(conflict.venue.collectOrder({ ...order, nonce: '1234' }), /fee currency/);
  assert.equal(io.signatures, 0);
});

test('journal hook runs only after final preview, authentication and mode guard, immediately before HTTP', async () => {
  for (const route of ['Order', 'Transfer']) {
    for (const failure of [null, 'preview', 'authentication', 'mode', 'journal']) {
      let sending = false, finalMarginRead = false;
      const events = [];
      const io = fixture((method, params, result) => {
        if (sending && method === 'private/get_margin') {
          finalMarginRead = true;
          if (failure === 'preview') return { ...result, post_maintenance_margin: '0' };
        }
        return result;
      });
      const transportWallet = { address: wallet.address, signingKey: wallet.signingKey, signMessage: async value => {
        events.push('authentication');
        if (sending && finalMarginRead && failure === 'authentication') throw new Error('authentication failed');
        return wallet.signMessage(value);
      } };
      const venue = createSpotVenue({ config, wallet: transportWallet, now: () => NOW,
        assertCanSend: () => { events.push('guard'); if (sending && finalMarginRead && failure === 'mode') throw new Error('mode changed'); },
        http: { post: async (url, params) => {
          const method = url.slice(config.baseUrl.length + 1);
          if (['private/order', 'private/transfer_spot'].includes(method)) events.push('mutation HTTP');
          return { data: { result: await io.post(method, params) } };
        } } });
      await venue.readSnapshot({ cycleBudget: '100', pendingActions: [] });
      const signed = await venue[`prepare${route}`](route === 'Order' ? order : transfer);
      sending = true; events.length = 0;
      const journal = () => { events.push('journal'); if (failure === 'journal') throw new Error('journal refused'); };
      const submit = venue[`submit${route}`](signed, journal);
      if (failure) {
        await assert.rejects(submit);
        assert.ok(!events.includes('mutation HTTP'), `${route}: ${failure} must not send`);
        assert.equal(events.includes('journal'), failure === 'journal', `${route}: ${failure} must not reserve`);
      } else {
        await submit;
        assert.deepEqual(events.slice(-4), ['authentication', 'guard', 'journal', 'mutation HTTP']);
      }
    }
  }
});

test('only direct accepted-identity-free order RPC refusals carry durable rejection evidence', async () => {
  const noLiquidity = { error: { code: 11009, message: 'Zero liquidity for market or IOC/FOK order' } };
  const scenarios = [
    { route: 'Order', response: noLiquidity, definitive: true },
    { route: 'Order', response: noLiquidity, httpError: true, definitive: true },
    { route: 'Order', response: { error: { code: -32602, message: 'Invalid params' } }, definitive: true },
    { route: 'Order', response: { error: { code: 14038, message: 'Session expiry' } }, definitive: true },
    { route: 'Order', response: { error: { code: -32000, message: 'Rate limit exceeded' } }, definitive: true },
    { route: 'Order', response: { error: { code: 98765, message: 'Unknown state' } }, definitive: false },
    { route: 'Order', response: { ...noLiquidity, result: { order: { order_id: 'accepted' } } }, definitive: false },
    { route: 'Order', response: { error: { ...noLiquidity.error, data: { order_id: 'accepted' } } }, definitive: false },
    { route: 'Transfer', response: noLiquidity, definitive: false },
  ];
  for (const scenario of scenarios) {
    const io = fixture();
    let claims = 0;
    const venue = createSpotVenue({ config, wallet, now: () => NOW, assertCanSend: () => {}, http: { post: async (url, params) => {
      const method = url.slice(config.baseUrl.length + 1);
      if (['private/order', 'private/transfer_spot'].includes(method)) {
        if (scenario.httpError) throw Object.assign(new Error('HTTP 400'), { response: { status: 400, data: scenario.response } });
        return { data: scenario.response };
      }
      return { data: { result: await io.post(method, params) } };
    } } });
    await venue.readSnapshot({ cycleBudget: '100', pendingActions: [] });
    const request = await venue[`prepare${scenario.route}`](scenario.route === 'Order' ? order : transfer);
    await assert.rejects(venue[`submit${scenario.route}`](request, () => { claims++; }), error => {
      assert.deepEqual(error.venueResponse, scenario.response);
      assert.equal(!!error.orderRejection, scenario.definitive);
      if (scenario.definitive) {
        assert.equal(error.orderRejection.method, 'private/order');
        assert.deepEqual(error.orderRejection.request, request);
        assert.notEqual(error.orderRejection.request, request);
      }
      return true;
    });
    assert.equal(claims, 1);
  }
  const failedRead = fixture((method, _params, result) => method === 'private/order_quote' ? noLiquidity : result);
  await assert.rejects(snapshot(failedRead), error => {
    assert.deepEqual(error.venueResponse, noLiquidity);
    assert.equal(error.orderRejection, undefined);
    return true;
  });
  assert.equal(failedRead.signatures, 0);
});
