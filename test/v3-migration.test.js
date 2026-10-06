'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { ethers } = require('ethers');
const { getDeriveConfig, authHeaders, orderNonce, fetchInstruments } = require('../bot/derive-config');
const { normalizeV2Trade, createEconomicStore, syncV2Trades } = require('../bot/economic-events');
const { preflight } = require('../scripts/derive-preflight');
const { loadProduction, SCRIPT_SOURCE, declaration } = require('./helpers/load-production');
const vm = require('node:vm');
const Database = require('better-sqlite3');
const owner = '0x' + '1'.repeat(40);
const env = { DERIVE_API_VERSION: 'v3', DERIVE_NETWORK: 'testnet', DERIVE_WALLET: owner,
  DERIVE_SUBACCOUNT_ID: '78645', DERIVE_HISTORY_FROM: '2026-10-06T17:55:00Z' };

test('V3 configuration fails closed on missing identity, mismatched networks and bad pause values', () => {
  assert.equal(getDeriveConfig({}).version, 'v2');
  assert.equal(getDeriveConfig(env).maintenance, true);
  for (const change of [{ DERIVE_WALLET: '' }, { DERIVE_SUBACCOUNT_ID: '' }, { DERIVE_HISTORY_FROM: '' },
    { DERIVE_API_URL: 'https://api.lyra.finance' }, { DERIVE_DOMAIN_SEPARATOR: getDeriveConfig({}).domainSeparator },
    { DERIVE_MAINTENANCE: '0' }, { DERIVE_NETWORK: 'typo' }]) {
    assert.throws(() => getDeriveConfig({ ...env, ...change }));
  }
  assert.deepEqual(authHeaders(getDeriveConfig(env), 123, 'sig'), {
    'X-DeriveWallet': owner, 'X-DeriveTimestamp': '123', 'X-DeriveSignature': 'sig',
  });
  for (const network of ['mainnet', 'testnet']) {
    assert.equal(getDeriveConfig({ ...env, DERIVE_NETWORK: network }).domainSeparator,
      ethers.TypedDataEncoder.hashDomain({ name: 'Matching', version: '1.0', chainId: network === 'mainnet' ? 1 : 11155111,
        verifyingContract: '0xeB8d770ec18DB98Db922E9D83260A585b9F0DeAD' }));
  }
});

test('nanosecond nonces preserve all digits in JSON and remain distinct in the same millisecond', () => {
  const now = Date.now(), nonces = Array.from({ length: 1000 }, () => orderNonce('v3', now));
  assert.equal(new Set(nonces).size, 1000);
  for (const nonce of nonces) {
    assert.match(nonce, /^\d{19}$/);
    assert.equal(JSON.parse(JSON.stringify({ nonce })).nonce, nonce);
    assert.ok(BigInt(nonce) >= BigInt(now) * 1000000n);
  }
});

test('instrument listing consumes every page and rejects repeated or incomplete evidence', async () => {
  const calls = [];
  const rows = await fetchInstruments(async (method, params) => {
    calls.push([method, params.page]);
    return { instruments: [{ instrument_name: String(params.page) }], pagination: { count: 2, num_pages: 2 } };
  }, 'v3', {});
  assert.equal(rows.length, 2);
  assert.deepEqual(calls, [['get_all_instruments', 1], ['get_all_instruments', 2]]);
  for (const result of [{ instruments: [], pagination: { count: 1, num_pages: 1 } },
    { instruments: [{ instrument_name: 'same' }], pagination: { count: 2, num_pages: 2 } }, {}]) {
    await assert.rejects(fetchInstruments(async () => result, 'v3', {}));
  }
  await assert.rejects(fetchInstruments(async () => { throw new Error('503'); }, 'v3', {}), /503/);
});

test('V3 normalizes into the existing V2 ledger format without duplicate identities or historical rewrites', async () => {
  const trade = { trade_id: 'same', subaccount_id: 78645, is_transfer: false, tx_status: 'settled', batch_status: 'Settled',
    instrument_name: 'ETH-20261127-1600-P', direction: 'buy', trade_amount: '0.3', trade_price: '0.2', timestamp: Date.now() };
  const legacy = { ...trade }, nativeV3 = { ...trade };
  delete legacy.batch_status;
  delete nativeV3.tx_status;
  const v2 = normalizeV2Trade(legacy, 78645), v3 = normalizeV2Trade(nativeV3, 78645, 'v3');
  assert.equal(v2.event_id, v3.event_id);
  const normalized = ({ raw_json, ...record }) => record;
  assert.deepEqual(normalized(v2), normalized(v3));
  assert.deepEqual(JSON.parse(v2.raw_json), legacy);
  assert.deepEqual(JSON.parse(v3.raw_json), nativeV3);
  assert.equal(v3.cashflow_usd, '-0.06');
  assert.equal(v3.source, 'derive-v2/private/get_trade_history');
  for (const status of [null, 'settled', 'Batching', 'SettledError']) {
    assert.throws(() => normalizeV2Trade({ ...trade, batch_status: status }, 78645, 'v3'), /unresolved/);
  }
  const db = new Database(':memory:');
  try {
    const store = createEconomicStore(db);
    store.recordEvents([v2]);
    const original = db.prepare('SELECT * FROM economic_events').all();
    const schema = db.prepare("SELECT sql FROM sqlite_master ORDER BY name").all();
    const result = await syncV2Trades({ store, version: 'v3', accountId: 78645, from: trade.timestamp - 1, to: trade.timestamp + 1,
      post: async () => ({ subaccount_id: 78645, trades: [nativeV3], pagination: { count: 1, num_pages: 1 } }) });
    assert.equal(result.inserted, 0);
    assert.deepEqual(db.prepare('SELECT * FROM economic_events').all(), original);
    assert.deepEqual(db.prepare("SELECT sql FROM sqlite_master ORDER BY name").all(), schema);
    assert.match(db.prepare('SELECT evidence_reference FROM economic_coverage').get().evidence_reference, /derive-v2/);
  } finally { db.close(); }
});

test('maintenance tick schedules a heartbeat without contacting the venue or recording observations', async () => {
  let delay;
  const heartbeats = [];
  const runBot = vm.compileFunction(`${declaration(SCRIPT_SOURCE, 'runBot')}; return runBot;`,
    ['DERIVE_CONFIG', 'setTimeout', 'runBotWithWatchdog', 'console', 'process'])(getDeriveConfig(env),
    (_callback, ms) => { delay = ms; }, () => {}, { log() {} },
    { connected: true, send: message => heartbeats.push(message) });
  await runBot();
  assert.equal(delay, 60000);
  assert.equal(heartbeats.length, 1);
  assert.equal(heartbeats[0].type, 'bot_heartbeat');
  assert.ok(Date.now() - heartbeats[0].at < 1000);
});

test('cutover cannot skip an unfinished V2 history cursor or replace its pending work', async () => {
  let writes = 0;
  const store = { latestCoverage: () => '2026-10-06T17:40:00Z', recordExposure: () => { writes++; } };
  const sync = vm.compileFunction(`${declaration(SCRIPT_SOURCE, 'syncEconomicEvidence')}; return syncEconomicEvidence;`,
    ['economicStore', 'lastEconomicSyncAt', 'SUBACCOUNT_ID', 'ECONOMIC_TRACKING_START', 'DERIVE_CONFIG', 'console'])(
    store, 0, 25923, '2026-09-11T00:00:00Z', getDeriveConfig(env), { log() {} });
  await sync(Date.parse('2026-10-06T19:00:00Z'));
  assert.equal(writes, 0);
});

test('production V3 order signs the exact string nonce and wire amounts with the selected domain', async () => {
  const wallet = ethers.Wallet.createRandom(), config = getDeriveConfig({ ...env, DERIVE_MAINTENANCE: 'false' });
  const encoder = new ethers.AbiCoder();
  const encodeSource = SCRIPT_SOURCE.slice(SCRIPT_SOURCE.indexOf('function encodeTradeData('), SCRIPT_SOURCE.indexOf('// Place order function'));
  const encodeTradeData = vm.compileFunction(`${encodeSource}; return encodeTradeData;`, ['ethers', 'encoder', 'Buffer', 'console'])(ethers, encoder, Buffer, { log() {} });
  let sent;
  const { placeOrder } = loadProduction(['placeOrder'], { bindings: {
    DERIVE_CONFIG: config, SUBACCOUNT_ID: config.subaccountId, DERIVE_ACCOUNT_ADDRESS: owner,
    DOMAIN_SEPARATOR: config.domainSeparator, ethers, encoder, Buffer, encodeTradeData,
    createWallet: () => wallet, signMessage: async (_wallet, timestamp) => wallet.signMessage(String(timestamp)),
    axios: { post: async (url, order, options) => { sent = { url, order, options }; return { data: { result: { order } } }; } },
    API_URL: { PLACE_ORDER: `${config.baseUrl}/private/order` }, console: { log() {}, error() {} },
  } });
  await placeOrder('ETH-20261127-1600-P', 0.3, 'buy', 10, owner, '123', false, 'ioc', { tick_size: '0.1' });
  assert.ok(sent);
  const { order, options } = sent;
  assert.equal(typeof order.nonce, 'string');
  assert.equal(options.headers['X-DeriveWallet'], owner);
  const encodedData = encoder.encode(['address', 'uint256', 'int256', 'int256', 'uint256', 'uint256', 'bool'],
    [owner, 123, ethers.parseUnits(order.limit_price, 18), ethers.parseUnits(order.amount, 18), ethers.parseUnits(order.max_fee, 18), config.subaccountId, true]);
  const types = { Action: [
    { name: 'subaccountId', type: 'uint256' }, { name: 'nonce', type: 'uint256' }, { name: 'module', type: 'address' },
    { name: 'data', type: 'bytes' }, { name: 'expiry', type: 'uint256' }, { name: 'owner', type: 'address' }, { name: 'signer', type: 'address' },
  ] };
  const recovered = ethers.verifyTypedData({ name: 'Matching', version: '1.0', chainId: 11155111,
    verifyingContract: '0xeB8d770ec18DB98Db922E9D83260A585b9F0DeAD' }, types,
  { subaccountId: config.subaccountId, nonce: order.nonce, module: '0xB8D20c2B7a1Ad2EE33Bc50eF10876eD3035b5e7b', data: encodedData,
    expiry: order.signature_expiry_sec, owner, signer: wallet.address }, order.signature);
  assert.equal(recovered, wallet.address);
  const paused = vm.compileFunction(`${declaration(SCRIPT_SOURCE, 'placeOrder')}; return placeOrder;`, ['DERIVE_CONFIG'])(getDeriveConfig(env));
  await assert.rejects(paused(), /maintenance/);
});

test('preflight never treats 503 or mismatched account evidence as readiness', async () => {
  const config = getDeriveConfig(env);
  await assert.rejects(preflight({ config, post: async () => { throw new Error('503'); } }), /503/);
  await assert.rejects(preflight({ config, post: async method => method.startsWith('public/')
    ? { instruments: [{ instrument_name: 'ETH-option' }], pagination: { count: 1, num_pages: 1 } }
    : { subaccount_id: 999, positions: [], collaterals: [] } }), /mismatched/);
});

test('collection mode appends observations and accounting without touching orders, decisions or advisory', async () => {
  const writes = [], heartbeats = [], delays = [];
  const instrument = { instrument_name: 'ETH-20261127-1600-P' };
  const forbidden = () => { throw new Error('Trading/advisory capability invoked during collection'); };
  const config = getDeriveConfig({ ...env, DERIVE_COLLECT_DATA: 'true' });
  const account = { subaccount_id: 78645 };
  const bindings = {
    DERIVE_CONFIG: config, console: { log() {}, error: forbidden },
    process: { connected: true, send: m => heartbeats.push(m), env: { ANTHROPIC_API_KEY: 'test' } },
    setTimeout: (_fn, ms) => delays.push(ms), runBotWithWatchdog() {},
    fetchDeriveSpotPrice: async () => null, fetchCoinGeckoSpotPrice: async () => null,
    normalizeEthSpotPrice: () => null,
    fetchAndFilterInstruments: async () => ({ instruments: [], putCandidates: [instrument], callCandidates: [] }),
    fetchTickersByExpiry: async () => ({ [instrument.instrument_name]: { quote_received_at: new Date().toISOString(), quote_source: 'derive-v2/get_tickers' } }),
    fetchPositions: async () => [], observationUniverse: () => [instrument], missingExpiryDates: () => [],
    manageOpenOrders: forbidden, evaluateTradingRules: forbidden, confirmAndExecutePending: forbidden,
    maybeResetPutCycle: forbidden, generateTradingAdvisory: forbidden, sendTelegram: forbidden,
    syncEconomicEvidence: async () => writes.push('accounting'),
    enrichCandidateFromTicker: () => ({ details: {} }), fetchSubaccount: async () => account,
    buildPortfolioObservation: a => { assert.equal(a, account); return { timestamp: new Date().toISOString() }; },
    determineCheckInterval: () => 60000, botData: {}, persistCycleState: () => writes.push('cycle'),
    filterValidOptions: x => x, PUT_DELTA_RANGE: [0, 1], CALL_DELTA_RANGE: [0, 1],
    summarizeBestCandidate: () => null,
    db: {
      getObservationInstruments: () => [],
      insertOptionsSnapshotBatch: rows => { assert.equal(rows.length, 1); writes.push('options'); },
      getGrossOptionsCashflow: () => ({ gross_options_cashflow: 0 }),
      insertPortfolioSnapshot: () => writes.push('portfolio'),
      evaluateDueDecisionOutcomes: forbidden, refreshPositionLifecycle: forbidden,
      getActiveRules: () => [], insertTick: () => writes.push('tick'),
    },
  };
  const run = vm.compileFunction(`${declaration(SCRIPT_SOURCE, 'runBot')}; return runBot;`, Object.keys(bindings))(...Object.values(bindings));
  await run();
  assert.deepEqual(writes, ['accounting', 'options', 'portfolio', 'tick', 'cycle']);
  assert.equal(heartbeats.length, 1);
  assert.deepEqual(delays, [60000]);
  for (const name of ['placeOrder', 'cancelOrder']) {
    const invoke = vm.compileFunction(`${declaration(SCRIPT_SOURCE, name)}; return ${name};`, ['DERIVE_CONFIG'])(config);
    await assert.rejects(invoke(), /maintenance/);
  }
});
