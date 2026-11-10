'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { preflightSpot, createReadOnlyPost, main } = require('../scripts/derive-spot-preflight');
const { getDeriveConfig } = require('../bot/derive-config');
const NOW = 1794242400000;
const OWNER = '0x1111111111111111111111111111111111111111';
const SIGNER = '0x2222222222222222222222222222222222222222';
const config = getDeriveConfig({ DERIVE_API_VERSION: 'v3', DERIVE_NETWORK: 'mainnet', DERIVE_WALLET: OWNER,
  DERIVE_SUBACCOUNT_ID: '25923', DERIVE_HISTORY_FROM: '2026-10-06T00:00:00Z' });
const quote = { subaccount_id: 99, instrument_name: 'DRV-USDC', direction: 'buy', amount: '10', limit_price: '0.3',
  max_fee: '0.002', time_in_force: 'ioc', signature_expiry_sec: NOW / 1000 + 900 };
function fakeRead({ transfer = false, spot = true, accountAllowed = true, owner = OWNER } = {}) {
  const calls = [];
  return { calls, post: async (method, p) => {
    calls.push({ method, p });
    switch (method) {
      case 'public/get_instrument': return { instrument_name: p.instrument_name, instrument_type: 'erc20', is_active: true, risk_universe_id: 3, tick_size: '0.0001' };
      case 'public/get_ticker': return { a: '0.3', A: '1000', t: NOW };
      case 'private/session_keys': return { public_session_keys: [{ public_session_key: SIGNER, expiry_sec: NOW / 1000 + 3600,
        protocol_scopes: [...(spot ? ['trade:orderbook:spot'] : []), ...(transfer ? ['transfer:existing_subaccount'] : [])], subaccount_ids: accountAllowed ? [25923, 99] : [88] }] };
      case 'private/get_account': return { fee_info: { spot_maker_fee: '0.001', spot_taker_fee: '0.0009' }, secret: 'never copy unknown fields' };
      case 'private/get_subaccount': return { subaccount_id: p.subaccount_id, risk_universe_id: p.subaccount_id === 25923 ? 1 : 3, is_under_liquidation: false, initial_margin: '1000', maintenance_margin: '1100', collaterals: [{ asset_name: 'USDC', amount: '1000' }] };
      case 'public/get_wallet': return { wallet: owner };
      case 'private/get_open_orders': return [];
      case 'private/order_quote': return { is_valid: true, post_initial_margin: '800', estimated_fill_amount: '10', estimated_order_status: 'filled' };
      case 'private/get_margin': return { is_valid_trade: true, post_initial_margin: '800', post_maintenance_margin: '900' };
      default: throw new Error(`Unexpected method ${method}`);
    }
  } };
}

test('private scope probe records missing ALT/transfer authority without declaring live readiness', async () => {
  const io = fakeRead();
  const report = await preflightSpot({ config, signer: SIGNER, now: () => NOW, post: io.post });
  assert.equal(report.ready_to_trade, false);
  assert.equal(report.mutations, false);
  assert.ok(report.blockers.includes('internal_transfer_scope_unverified'));
  assert.ok(report.blockers.includes('alt_destination_not_configured'));
  assert.ok(report.blockers.includes('private_order_preview_not_requested'));
  assert.ok(!report.blockers.includes('eth_reference_or_expiry_unverified'));
  assert.equal(report.eth_reference, undefined);
  assert.equal(report.feeds, undefined);
  assert.deepEqual(report.previews, []);
  assert.ok(!io.calls.some(c => c.method === 'private/order_quote' || c.method === 'public/get_latest_signed_feeds'));
  assert.deepEqual(io.calls.filter(c => c.method === 'public/get_instrument').map(c => c.p.instrument_name), ['DRV-USDC']);
  assert.ok(!JSON.stringify(report).includes('never copy'));
});

test('configured ALT receives only an unsigned DRV IOC preview and fee-inclusive margin simulation', async () => {
  const io = fakeRead({ transfer: true });
  const report = await preflightSpot({ config, signer: SIGNER, now: () => NOW, post: io.post,
    destinationSubaccountId: 99, quotes: [quote] });
  assert.equal(report.ready_to_trade, false);
  assert.equal(report.previews[0].response.estimated_fill_amount, '10');
  assert.equal(report.previews[0].request.subaccount_id, 99);
  assert.equal(report.previews[0].request.instrument_name, 'DRV-USDC');
  assert.equal(report.previews[0].request.time_in_force, 'ioc');
  assert.equal(report.previews[0].request.signature, '0x');
  assert.equal(report.previews[0].request.nonce, '0');
  assert.equal(report.previews[0].request.reduce_only, false);
  const simulation = io.calls.find(c => c.method === 'private/get_margin').p;
  assert.equal(simulation.subaccount_id, 99);
  assert.deepEqual(simulation.simulated_collateral_changes, [
    { asset_name: 'DRV', amount: '10' }, { asset_name: 'USDC', amount: '-3.02' },
  ]);
  assert.deepEqual(report.blockers, ['transfer_timeout_recovery_unverified', 'collateral_settlement_timing_unverified', 'live_activation_not_performed']);
});

test('ownership or session account mismatch prevents even the private preview', async () => {
  for (const options of [{ owner: SIGNER }, { accountAllowed: false }]) {
    const io = fakeRead(options);
    const report = await preflightSpot({ config, signer: SIGNER, now: () => NOW, post: io.post, destinationSubaccountId: 99, quotes: [quote] });
    assert.ok(report.blockers.includes('account_99_eligibility_unverified'));
    assert.equal(report.previews[0].deferred, 'account_or_scope_unverified');
    assert.ok(!io.calls.some(c => c.method === 'private/order_quote'));
  }
});

test('source must stay in PRIME and destination must match active DRV metadata', async () => {
  for (const changedAccount of [25923, 99]) {
    const io = fakeRead();
    const post = async (method, params) => {
      const result = await io.post(method, params);
      return method === 'private/get_subaccount' && params.subaccount_id === changedAccount
        ? { ...result, risk_universe_id: changedAccount === 25923 ? 3 : 1 } : result;
    };
    const report = await preflightSpot({ config, signer: SIGNER, now: () => NOW, post, destinationSubaccountId: 99 });
    assert.ok(report.blockers.includes(`account_${changedAccount}_eligibility_unverified`));
  }
  for (const bad of [{ instrument_name: 'ETH-USDC' }, { instrument_type: 'perp' }, { is_active: false }, { risk_universe_id: null }]) {
    const io = fakeRead();
    const post = async (method, params) => {
      const result = await io.post(method, params);
      return method === 'public/get_instrument' ? { ...result, ...bad } : result;
    };
    const report = await preflightSpot({ config, signer: SIGNER, now: () => NOW, post, destinationSubaccountId: 99, quotes: [quote] });
    assert.ok(report.blockers.includes('drv_instrument_unverified'));
    assert.ok(report.blockers.includes('account_99_eligibility_unverified'));
    assert.ok(!io.calls.some(c => c.method === 'private/order_quote'));
  }
});

test('missing spot scope defers a configured destination preview', async () => {
  const io = fakeRead({ transfer: true, spot: false });
  const report = await preflightSpot({ config, signer: SIGNER, now: () => NOW, post: io.post,
    destinationSubaccountId: 99, quotes: [quote] });
  assert.ok(report.blockers.includes('spot_trading_scope_unverified'));
  assert.equal(report.previews[0].deferred, 'account_or_scope_unverified');
  assert.ok(!io.calls.some(c => c.method === 'private/order_quote'));
});

test('preview request strips executable signatures and rejects unrelated instruments/accounts', async () => {
  const io = fakeRead();
  const report = await preflightSpot({ config, signer: SIGNER, now: () => NOW, post: io.post,
    destinationSubaccountId: 99, quotes: [{ ...quote, signature: 'caller-action-signature', nonce: '777', extra_fee: '99' }] });
  assert.equal(report.previews[0].request.signature, '0x');
  assert.equal(report.previews[0].request.extra_fee, undefined);
  for (const changed of [{ subaccount_id: 5 }, { subaccount_id: 25923 }, { instrument_name: 'ETH-USDC' },
    { time_in_force: 'gtc' }, { time_in_force: 'post_only' }]) {
    await assert.rejects(preflightSpot({ config, signer: SIGNER, now: () => NOW, post: io.post,
      destinationSubaccountId: 99, quotes: [{ ...quote, ...changed }] }), /DRV-USDC IOC.*configured destination/);
  }
  await assert.rejects(preflightSpot({ config, signer: SIGNER, now: () => NOW, post: io.post,
    quotes: [quote] }), /configured destination/);
  await assert.rejects(preflightSpot({ config, signer: SIGNER, now: () => NOW, post: io.post,
    destinationSubaccountId: 25923, quotes: [quote] }), /distinct positive/);
});

test('read transport refuses mutation before signing or network and pins credential origin', async () => {
  let signed = 0, calls = 0;
  const wallet = { signMessage: async () => { signed++; return 'request-auth-only'; } };
  const post = createReadOnlyPost({ config, wallet, now: () => NOW, http: { post: async () => { calls++; return { data: { result: {} } }; } } });
  for (const method of ['private/order', 'private/transfer_spot', 'private/cancel', 'private/set_session_key']) {
    await assert.rejects(post(method, {}), /not read-only/);
  }
  assert.equal(signed, 0); assert.equal(calls, 0);
  await post('private/get_account', { wallet: OWNER });
  assert.equal(signed, 1); assert.equal(calls, 1);
  assert.throws(() => createReadOnlyPost({ config: { ...config, baseUrl: 'https://example.com' }, wallet }), /does not match/);
});


test('malformed or nonpositive account and preview margins fail closed despite valid flags', async () => {
  const cases = [
    ['private/get_subaccount', 'initial_margin', 'account_25923_eligibility_unverified'],
    ['private/get_subaccount', 'maintenance_margin', 'account_25923_eligibility_unverified'],
    ['private/order_quote', 'post_initial_margin', 'preview_DRV-USDC_unverified'],
    ['private/get_margin', 'post_initial_margin', 'preview_DRV-USDC_unverified'],
    ['private/get_margin', 'post_maintenance_margin', 'preview_DRV-USDC_unverified'],
  ];
  for (const [endpoint, field, blocker] of cases) {
    for (const bad of [undefined, null, '', 'broken', 'Infinity', '1e309', '0', '-1', true]) {
      const io = fakeRead();
      const post = async (method, params) => {
        const result = await io.post(method, params);
        return method === endpoint ? { ...result, [field]: bad } : result;
      };
      const report = await preflightSpot({ config, signer: SIGNER, now: () => NOW, post, destinationSubaccountId: 99, quotes: [quote] });
      assert.ok(report.blockers.includes(blocker), `${endpoint}.${field}=${String(bad)}`);
      if (endpoint === 'private/get_subaccount') assert.ok(!io.calls.some(c => c.method === 'private/order_quote'));
    }
  }
});

test('CLI rejects duplicate arguments and report aliases/sidecars before configuration or network', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-spot-preflight-'));
  const previousDataDir = process.env.DATA_DIR;
  const previousDbPath = process.env.NOOP_DB_PATH;
  try {
    process.env.DATA_DIR = dir;
    delete process.env.NOOP_DB_PATH;
    const database = path.join(dir, 'noop.db');
    const quotes = path.join(dir, 'quotes.json');
    fs.writeFileSync(database, 'database must stay intact');
    fs.writeFileSync(quotes, '[]');
    const inputAlias = path.join(dir, 'quote-alias.json');
    fs.symlinkSync(quotes, inputAlias);
    await assert.rejects(main(['--out', path.join(dir, 'first.json'), '--out', path.join(dir, 'second.json')]), /Usage/);
    for (const output of [database, `${database}-wal`, `${database}-shm`, quotes, inputAlias]) {
      await assert.rejects(main(['--quotes', quotes, '--out', output]), /SQLite sidecars/);
    }
    await assert.rejects(main(['--out', path.join(dir, 'unrelated-wal')]), /reserved SQLite sidecar suffix/);
    process.env.NOOP_DB_PATH = path.join(dir, 'custom.db');
    await assert.rejects(main(['--out', `${process.env.NOOP_DB_PATH}-journal`]), /SQLite sidecars/);
    assert.equal(fs.readFileSync(database, 'utf8'), 'database must stay intact');
    assert.equal(fs.readFileSync(quotes, 'utf8'), '[]');
    assert.equal(fs.existsSync(path.join(dir, 'unrelated-wal')), false);
  } finally {
    if (previousDataDir === undefined) delete process.env.DATA_DIR; else process.env.DATA_DIR = previousDataDir;
    if (previousDbPath === undefined) delete process.env.NOOP_DB_PATH; else process.env.NOOP_DB_PATH = previousDbPath;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
