'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { Wallet, AbiCoder, keccak256, concat, parseUnits, recoverAddress } = require('ethers');
const { readProfile, loadKey } = require('../integrations/derive-v3/profile');
const { DeriveV3, nextNonce, validatePortfolio } = require('../integrations/derive-v3');
const { openJournal, stateDirectory, acquireWriter } = require('../integrations/derive-v3/state');
const sdk = require('../integrations/derive-v3/node_modules/@derivexyz/derive-ts');
const { sdkNetwork, TESTNET_CONTRACTS } = require('../integrations/derive-v3/deployment');
const { compareSnapshots } = require('../integrations/derive-v3/handoff');
const { V2_IDENTITY } = require('../integrations/derive-v3/profile');

const wallet = Wallet.createRandom();
const profile = readProfile({ NOOP_VENUE: 'v3-testnet', DERIVE_V3_TESTNET_OWNER_ADDRESS: wallet.address,
  DERIVE_V3_TESTNET_SUBACCOUNT_ID: '987654321', DERIVE_V3_TESTNET_EXECUTION: 'enabled' });
const portfolio = () => ({ subaccount_id: profile.subaccountId, manager_id: 1, risk_universe_id: 1,
  initial_margin: '100', maintenance_margin: '110', subaccount_value: '120', positions: [], collaterals: [],
  failed_to_fetch: false, is_under_liquidation: false });
const orderParams = () => ({ subaccountId: profile.subaccountId, instrumentName: 'ETH-PERP', direction: 'buy',
  amount: '0.01', limitPrice: '1000', maxFee: '1', reduceOnly: false, timeInForce: 'post_only' });
function clientWith(place = async p => ({ order: {
  order_id: 'test-order', nonce: p.nonce, subaccount_id: p.subaccountId,
  instrument_name: p.instrumentName, direction: p.direction, time_in_force: p.timeInForce,
  amount: p.amount, limit_price: p.limitPrice, filled_amount: '0', average_price: '0', order_status: 'open',
}, trades: [] })) {
  return {
    send: async method => method === 'private/get_subaccounts' ? { subaccount_ids: [profile.subaccountId] } : portfolio(),
    orders: { place, cancel: async () => ({ order_status: 'cancelled' }) },
  };
}

test('V2 remains the default even with V3 credentials present', () => {
  assert.deepEqual(readProfile({ DERIVE_V3_TESTNET_PRIVATE_KEY: wallet.privateKey }), { name: 'v2', version: 2 });
  assert.throws(() => readProfile({ NOOP_VENUE: 'v3' }), /Unknown/);
  assert.throws(() => readProfile({ NOOP_VENUE: 'v3-mainnet' }), /gated/);
});

test('V3 never falls back to the V2 private key or permits arbitrary hosts', () => {
  assert.throws(() => loadKey(profile, { PRIVATE_KEY: wallet.privateKey }), /dedicated V3 key/);
  assert.equal(readProfile({ NOOP_VENUE: 'v3-testnet', DERIVE_API_URL: 'https://example.org' }).httpUrl,
    'https://testnet.api.derive.xyz/v3');
  assert.equal(loadKey(profile, { DERIVE_V3_TESTNET_PRIVATE_KEY: wallet.privateKey }), wallet.privateKey);
});

test('testnet custody override does not change signing domains or the SDK mainnet preset', () => {
  const network = sdkNetwork('testnet');
  assert.equal(network.contracts.actionManager, TESTNET_CONTRACTS.actionManager);
  assert.notEqual(network.contracts.actionManager, sdk.NETWORKS.testnet.contracts.actionManager);
  assert.equal(sdk.domainSeparator(network), sdk.domainSeparator(sdk.NETWORKS.testnet));
  assert.equal(sdkNetwork('mainnet'), sdk.NETWORKS.mainnet);
});

test('handoff comparison rejects wrong networks, stale snapshots, resting orders and changed quantities', () => {
  const now = Date.now();
  const v2 = { timestamp: new Date(now).toISOString(), venue: 'v2', owner: V2_IDENTITY.owner,
    account: { ...portfolio(), subaccount_id: V2_IDENTITY.subaccount_id }, open_orders: [], trigger_orders: [], algo_orders: [] };
  const v3 = { ...v2, venue: 'v3-mainnet', owner: wallet.address, account: portfolio() };
  assert.equal(compareSnapshots(v2, v3, now).account_comparison_passed, true);
  assert.equal(compareSnapshots(v2, { ...v3, venue: 'v3-testnet' }, now).account_comparison_passed, false);
  assert.equal(compareSnapshots(v2, { ...v3, open_orders: [{}] }, now).account_comparison_passed, false);
  assert.equal(compareSnapshots(v2, v3, now + 300001).account_comparison_passed, false);
  const changed = { ...v3, account: { ...portfolio(), positions: [{ instrument_name: 'ETH-PERP', amount: '0.01' }] } };
  assert.ok(compareSnapshots(v2, changed, now).blockers.some(b => b.includes('ETH-PERP')));
});

test('nonce generation preserves exact nanosecond integers and uniqueness', () => {
  const now = Date.now();
  const nonces = Array.from({ length: 1000 }, () => nextNonce(now));
  assert.equal(new Set(nonces).size, 1000);
  assert.ok(BigInt(nonces[0]) > BigInt(Number.MAX_SAFE_INTEGER));
  assert.ok(BigInt(nonces[0]) >= BigInt(now) * 1000000n);
  assert.ok(BigInt(nonces.at(-1)) < BigInt(now + 2) * 1000000n);
  assert.equal(JSON.parse(JSON.stringify({ nonce: nonces[0] })).nonce, nonces[0]);
});

test('pinned SDK signs the expected Sepolia action with distinct owner and session signer', async () => {
  const owner = Wallet.createRandom().address;
  const c = new sdk.DeriveClient({ network: 'testnet', sessionKey: wallet, ownerAddress: owner });
  const instrument = { base_asset_address: '0x0000000000000000000000000000000000000001', base_asset_sub_id: '0' };
  let wire;
  c.send = async (method, params) => {
    if (method === 'public/get_instrument') return instrument;
    assert.equal(method, 'private/order'); wire = params; return { order: params };
  };
  const nonce = nextNonce(); const expiry = Math.floor(Date.now() / 1000) + 900;
  await c.orders.place({ ...orderParams(), nonce, signatureExpirySec: expiry, rejectPostOnly: true });
  const domain = '0x24d674cd5f2b9d564691c51e9d88f649b99246a2244dd74ce27b96578d773e85';
  assert.equal(sdk.domainSeparator(c.network), domain);
  const abi = AbiCoder.defaultAbiCoder();
  const data = abi.encode(['address', 'uint256', 'int256', 'int256', 'uint256', 'uint256', 'bool'],
    [instrument.base_asset_address, 0, parseUnits('1000', 18), parseUnits('0.01', 18), parseUnits('1', 18), profile.subaccountId, true]);
  const actionHash = keccak256(abi.encode(['bytes32', 'uint256', 'uint256', 'address', 'bytes32', 'uint256', 'address', 'address'],
    [sdk.ACTION_TYPEHASH, profile.subaccountId, nonce, c.network.modules.trade, keccak256(data), expiry, owner, wallet.address]));
  assert.equal(recoverAddress(keccak256(concat(['0x1901', domain, actionHash])), wire.signature), wallet.address);
  assert.equal(wire.nonce, nonce);
  assert.equal(wire.reduce_only, false);
  assert.equal(wire.time_in_force, 'post_only');
  assert.equal(wire.reject_post_only, true);
  assert.notEqual(sdk.domainSeparator(sdk.NETWORKS.mainnet), domain);
});

test('V3 read client denies all writes and cross-account reads', async () => {
  const a = new DeriveV3({ profile, client: clientWith() });
  await assert.rejects(a.read('private/order'), /allowed read/);
  await assert.rejects(a.read('private/withdraw'), /allowed read/);
  await assert.rejects(a.read('private/get_subaccount', { subaccount_id: 42 }), /mismatch/);
  await assert.rejects(a.read('private/get_subaccounts', { wallet: Wallet.createRandom().address }), /mismatch/);
});

test('pagination consumes every page and rejects silently repeated or truncated history', async () => {
  const pages = [];
  const a = new DeriveV3({ profile, client: { send: async (_, params) => {
    pages.push(params.page); return { trades: [{ id: params.page }], pagination: { num_pages: 3 } };
  } } });
  assert.deepEqual(await a.pages('private/get_trade_history', {}, 'trades', { pageSize: 100 }), [{ id: 1 }, { id: 2 }, { id: 3 }]);
  assert.deepEqual(pages, [1, 2, 3]);
  a.client.send = async () => ({ trades: [{ id: 1 }] });
  await assert.rejects(a.pages('private/get_trade_history', {}, 'trades', { pageSize: 1 }), /did not advance/);
  a.client.send = async (_, p) => ({ trades: [{ id: p.page }] });
  await assert.rejects(a.pages('private/get_trade_history', {}, 'trades', { pageSize: 1, maxPages: 2 }), /limit/);
});

test('unavailable portfolios cannot become zero positions or healthy margin', () => {
  assert.throws(() => validatePortfolio({ ...portfolio(), failed_to_fetch: true }, profile), /unavailable/);
  assert.throws(() => validatePortfolio({ ...portfolio(), initial_margin: null }, profile), /initial_margin/);
  assert.throws(() => validatePortfolio({ ...portfolio(), subaccount_id: 42 }, profile), /mismatch/);
  assert.throws(() => validatePortfolio({ ...portfolio(), risk_universe_id: 0 }, profile), /trading manager/);
});

test('terminal orders fall back to history and absence remains unknown', async () => {
  let records = [{ order_id: 'id', order_status: 'cancelled', filled_amount: '0' }];
  const a = new DeriveV3({ profile, client: { send: async method => {
    if (method === 'private/get_order') throw Object.assign(new Error('Does not exist'), { code: 11006 });
    return { orders: records, pagination: { num_pages: 1 } };
  } } });
  assert.equal((await a.orderStatus('id')).order_status, 'cancelled');
  records = [];
  assert.equal(await a.orderStatus('id'), null);
});

test('production state preparation preserves budgets and tags historical versus new orders', async () => {
  const os = require('node:os');
  const Database = require('better-sqlite3');
  const { prepareState } = require('../integrations/derive-v3/prepare-state');
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-handoff-'));
  const p = { ...profile, name: 'v3-mainnet', network: 'mainnet', executionEnabled: false, subaccountId: 910000000 + process.pid };
  const dest = stateDirectory(p);
  try {
    const dbPath = path.join(temp, 'noop.db');
    const db = new Database(dbPath);
    db.pragma('journal_mode = WAL');
    db.exec("CREATE TABLE resting_orders(status TEXT); CREATE TABLE pending_actions(status TEXT); CREATE TABLE orders(value INTEGER); CREATE TABLE portfolio_snapshots(value INTEGER); CREATE TABLE bot_state(budget INTEGER); INSERT INTO bot_state VALUES(42); INSERT INTO orders VALUES(7);");
    const wiki = path.join(temp, 'knowledge'); fs.mkdirSync(wiki); fs.writeFileSync(path.join(wiki, 'lesson.md'), 'Preserved lesson');
    try { await prepareState(p, dbPath, wiki); } finally { db.close(); }
    const copy = new Database(path.join(dest, 'data/noop.db'));
    try {
      assert.equal(copy.prepare('SELECT budget FROM bot_state').get().budget, 42);
      assert.equal(copy.prepare('SELECT venue FROM orders').get().venue, 'v2');
      copy.exec('INSERT INTO orders(value) VALUES(8)');
      assert.equal(copy.prepare('SELECT venue FROM orders WHERE value=8').get().venue, 'v3-mainnet');
    } finally { copy.close(); }
    assert.equal(fs.readFileSync(path.join(dest, 'knowledge/lesson.md'), 'utf8'), 'Preserved lesson');
    await assert.rejects(prepareState(p, dbPath, wiki), /refusing to overwrite/);
  } finally { fs.rmSync(temp, { recursive: true, force: true }); fs.rmSync(dest, { recursive: true, force: true }); }
});

test('disabled execution and ownership mismatches prevent placement', async () => {
  let sends = 0;
  const c = clientWith(async () => { sends++; });
  const a = new DeriveV3({ profile: { ...profile, executionEnabled: false }, client: c });
  await assert.rejects(a.place(orderParams()), /disabled/);
  await assert.rejects(a.cancel('id', 'ETH-PERP'), /disabled/);
  const b = new DeriveV3({ profile, client: c });
  c.send = async () => ({ subaccount_ids: [] });
  await assert.rejects(b.place(orderParams()), /not accessible/);
  assert.equal(sends, 0);
});

test('unknown placement blocks subsequent sends, but cancellation remains possible', async () => {
  const events = [];
  const a = new DeriveV3({ profile, client: clientWith(async () => { throw new Error('timeout'); }), journal: e => events.push(e) });
  await assert.rejects(a.place(orderParams()), /timeout/);
  assert.deepEqual(events.map(e => e.event), ['order_intent', 'order_unknown']);
  await assert.rejects(a.place(orderParams()), /Unresolved/);
  assert.equal((await a.cancel('id', 'ETH-PERP')).order_status, 'cancelled');
});

test('definitive RPC rejection can be retried with a new nonce', async () => {
  const nonces = [];
  const a = new DeriveV3({ profile, client: clientWith(async p => {
    nonces.push(p.nonce); throw Object.assign(new Error('no liquidity'), { code: 11009 });
  }) });
  await assert.rejects(a.place(orderParams()), /no liquidity/);
  await assert.rejects(a.place(orderParams()), /no liquidity/);
  assert.equal(a.pendingOrder, null); assert.notEqual(nonces[0], nonces[1]);
});

test('concurrent calls cannot pass the placement guard while account preflight is running', async () => {
  let unblock;
  const c = clientWith();
  const base = c.send;
  c.send = async (...args) => { await new Promise(resolve => { unblock = resolve; }); return base(...args); };
  const a = new DeriveV3({ profile, client: c });
  const first = a.place(orderParams());
  await assert.rejects(a.place(orderParams()), /in progress/);
  c.send = base; unblock(); await first;
});

test('journal restores unresolved intents after restart and rejects a different account owner', () => {
  const testProfile = { ...profile, subaccountId: 900000000 + process.pid };
  const dir = stateDirectory(testProfile);
  try {
    const journal = openJournal(testProfile);
    journal.append({ event: 'order_intent', nonce: '123' });
    assert.deepEqual(openJournal(testProfile).unresolved, ['123']);
    journal.append({ event: 'order_ack', nonce: '123' });
    assert.deepEqual(openJournal(testProfile).unresolved, ['123']);
    journal.append({ event: 'order_reconciled', nonce: '123' });
    assert.deepEqual(openJournal(testProfile).unresolved, ['123']);
    journal.append({ event: 'order_rejected', nonce: '123', code: 9000 });
    assert.deepEqual(openJournal(testProfile).unresolved, ['123']);
    journal.append({ event: 'order_accounted', nonce: '123' });
    assert.deepEqual(openJournal(testProfile).unresolved, []);
    assert.throws(() => openJournal({ ...testProfile, ownerAddress: Wallet.createRandom().address }), /identity mismatch/);
    const release = acquireWriter(testProfile);
    assert.throws(() => acquireWriter(testProfile), /writer lock/);
    release();
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test('actual legacy placeOrder keeps V2 wire fields and routes V3 through the SDK seam', async () => {
  const source = fs.readFileSync(path.join(__dirname, '../script.js'), 'utf8');
  const fn = source.slice(source.indexOf('const placeOrder ='), source.indexOf('// Fetch all open (resting) orders'));
  const captures = [];
  const context = vm.createContext({
    console: { log() {}, error() {} }, Buffer, Date, Math, ethers: require('ethers'), encoder: AbiCoder.defaultAbiCoder(),
    createWallet: () => wallet, signMessage: (w, t) => w.signMessage(String(t)),
    normalizeOrderPriceForVenue: p => ({ price: p, step: 0.1, priceString: String(p) }),
    getStepDecimals: () => 1, formatVenueOrderAmount: value => String(value),
    encodeTradeData: () => '0x' + '11'.repeat(32),
    ACTION_TYPEHASH: sdk.ACTION_TYPEHASH, DOMAIN_SEPARATOR: '0xd96e5f90797da7ec8dc4e276260c7f3f87fedf68775fbe1ef116e996fc60441b',
    TRADE_MODULE_ADDRESS: sdk.NETWORKS.testnet.modules.trade, DERIVE_ACCOUNT_ADDRESS: wallet.address, SUBACCOUNT_ID: 25923,
    API_URL: { PLACE_ORDER: 'https://api.lyra.finance/private/order' }, V3_VENUE: null,
    derivePost: async (...args) => { captures.push(args); return { data: { result: { order_id: 'v2-order' } } }; },
    extractOrderRecord: p => p, stringifyApiError: JSON.stringify, isIocNoLiquidityError: () => false,
  });
  vm.runInContext(`${fn}\nthis.place = placeOrder;`, context);
  const v2 = await context.place('ETH-PERP', 0.1, 'buy', 1000, wallet.address, 0, false, 'ioc');
  assert.equal(v2.result.order_id, 'v2-order');
  assert.equal(captures[0][0], 'https://api.lyra.finance/private/order');
  assert.equal(captures[0][1].subaccount_id, 25923);
  assert.equal(captures[0][1].reduce_only, false);
  assert.equal(captures[0][1].time_in_force, 'ioc');
  assert.ok(captures[0][2].headers['X-LyraSignature']);
  context.V3_VENUE = { placeOrder: async p => ({ data: { result: { order_id: 'v3-order', reduce_only: p.reduce_only } } }) };
  const v3 = await context.place('ETH-PERP', 0.1, 'sell', 1000, wallet.address, 0, true, 'post_only');
  assert.equal(v3.result.order_id, 'v3-order'); assert.equal(v3.result.reduce_only, true);
  assert.equal(captures.length, 1);
  context.isIocNoLiquidityError = () => true;
  context.V3_VENUE = { placeOrder: async () => { throw Object.assign(new Error('zero liquidity'), {
    response: { data: { error: { code: 11009 } } },
  }); } };
  const emptyIoc = await context.place('ETH-PERP', 0.1, 'buy', 1000, wallet.address, 0, false, 'ioc');
  assert.equal(emptyIoc.zero_fill_rejected, true);
});
