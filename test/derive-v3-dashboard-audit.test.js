'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const ts = require('../dashboard/node_modules/typescript');
const { assertIsolatedDataPaths } = require('../integrations/derive-v3/isolation');

const profile = { version: 3, network: 'testnet', ownerAddress: `0x${'1'.repeat(40)}`, subaccountId: 777, httpUrl: 'https://testnet.api.derive.xyz/v3' };
const source = fs.readFileSync(path.resolve(__dirname, '../script.js'), 'utf8');

function dashboardClient(response) {
  const source = fs.readFileSync(path.resolve(__dirname, '../dashboard/src/lib/lyra.ts'), 'utf8');
  const code = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, esModuleInterop: true } }).outputText;
  const exports = {};
  const context = {
    exports, process: { env: {} }, AbortSignal,
    fetch: async (url, options) => ({ ok: true, json: async () => ({ result: await response(url, JSON.parse(options.body)) }) }),
    require(name) {
      if (name === './venue') return { VENUE: profile, IS_V3: true };
      if (name.endsWith('/profile')) return { loadKey: () => `0x${'2'.repeat(64)}` };
      if (name === 'viem/accounts') return { privateKeyToAccount: () => ({ signMessage: async () => 'test-signature' }) };
      if (name === 'path') return path;
      if (name === 'fs') return {};
      throw new Error(`Unexpected test dependency: ${name}`);
    },
  };
  vm.runInNewContext(code, context);
  return exports;
}

function strategyFunction(name, context) {
  const start = source.indexOf(`const ${name} = `);
  assert.ok(start >= 0);
  const end = source.indexOf('\n};', start) + 3;
  return vm.runInNewContext(`${source.slice(start, end)}\n${name}`, context);
}

test('all V3 dashboard state paths match the selected account and reject linked state', () => {
  const root = fs.mkdtempSync('/private/tmp/noop-v3-dashboard-');
  const accountDir = path.join(root, '.derive-v3/testnet/777');
  const env = { DATA_DIR: path.join(accountDir, 'data'), WIKI_DIR: path.join(accountDir, 'knowledge') };
  try {
    assert.doesNotThrow(() => assertIsolatedDataPaths(profile, env, root));
    assert.throws(() => assertIsolatedDataPaths(profile, {}, root), /DATA_DIR/);
    assert.throws(() => assertIsolatedDataPaths(profile, { ...env, DATA_DIR: path.join(root, 'data') }, root), /DATA_DIR/);
    assert.throws(() => assertIsolatedDataPaths(profile, { ...env, WIKI_DIR: path.join(root, 'knowledge') }, root), /WIKI_DIR/);
    assert.throws(() => assertIsolatedDataPaths({ ...profile, subaccountId: 778 }, env, root), /DATA_DIR/);
    fs.mkdirSync(path.dirname(accountDir), { recursive: true });
    fs.mkdirSync(path.join(root, 'v2-data'));
    fs.symlinkSync(path.join(root, 'v2-data'), accountDir);
    assert.throws(() => assertIsolatedDataPaths(profile, env, root), /symlinked/);
    fs.unlinkSync(accountDir);
    fs.symlinkSync(path.join(root, 'missing-directory'), accountDir);
    assert.throws(() => assertIsolatedDataPaths(profile, env, root), /symlinked/);
    fs.unlinkSync(accountDir);
    fs.mkdirSync(env.DATA_DIR, { recursive: true });
    fs.symlinkSync(path.join(root, 'missing-database'), path.join(env.DATA_DIR, 'noop.db'));
    assert.throws(() => assertIsolatedDataPaths(profile, env, root), /symlinked/);
    fs.unlinkSync(path.join(env.DATA_DIR, 'noop.db'));
    fs.mkdirSync(path.join(env.WIKI_DIR, 'strategy'), { recursive: true });
    fs.symlinkSync(path.join(root, 'missing-wiki-page'), path.join(env.WIKI_DIR, 'strategy/playbook.md'));
    assert.throws(() => assertIsolatedDataPaths(profile, env, root), /symlinked/);
    assert.doesNotThrow(() => assertIsolatedDataPaths({ version: 2 }, {}, root));
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});

test('V3 dashboard rejects account failures and malformed exposure instead of showing flat balances', async () => {
  for (const result of [
    { subaccount_id: 777 },
    { subaccount_id: 778, positions: [] },
    { subaccount_id: 777, positions: [], failed_to_fetch: true },
    { subaccount_id: 777, positions: [{ instrument_name: 'ETH-PERP', amount: 'NaN' }] },
  ]) await assert.rejects(dashboardClient(async () => result).getPositions(), /V3/);
  await assert.rejects(dashboardClient(async () => ({ subaccount_id: 777, collaterals: null })).getCollaterals(), /V3/);
  await assert.rejects(dashboardClient(async () => ({ subaccount_id: 777, collaterals: [], positions: [], open_orders: [], failed_to_fetch: false, is_under_liquidation: false })).getSubaccount(), /V3 account initial_margin/);
  assert.equal((await dashboardClient(async () => ({ subaccount_id: 777, positions: [] })).getPositions()).length, 0);
});

test('V3 dashboard history requires complete, valid pagination', async () => {
  for (const pagination of [{ num_pages: -1 }, { num_pages: 0.5 }, { num_pages: '1' }, { num_pages: 0 }]) {
    await assert.rejects(dashboardClient(async () => ({ subaccount_id: 777, trades: [{ trade_id: 'one' }], pagination })).getTradeHistory(1), /pagination/);
  }
  await assert.rejects(dashboardClient(async () => ({ subaccount_id: 777, trades: [], pagination: { num_pages: 2 } })).getTradeHistory(1), /empty page/);
  const trades = await dashboardClient(async (url, body) => ({ subaccount_id: 777, trades: [{ trade_id: `trade-${body.page}` }], pagination: { num_pages: 2 } })).getTradeHistory(1);
  assert.deepEqual(Array.from(trades, row => row.trade_id), ['trade-1', 'trade-2']);
});

test('actual strategy retry and confirmation stop on unavailable V3 risk data', async () => {
  const failure = new Error('account read failed');
  const retry = strategyFunction('evaluateSellCallRetryMargin', { V3_VENUE: {}, fetchSubaccount: async () => { throw failure; } });
  await assert.rejects(retry({}), /account read failed/);
  const legacyRetry = strategyFunction('evaluateSellCallRetryMargin', { V3_VENUE: null, fetchSubaccount: async () => { throw failure; } });
  assert.equal((await legacyRetry({})).allowed, true);
  for (const method of ['fetchSubaccount', 'fetchPositions']) {
    const confirm = strategyFunction('confirmAndExecutePending', {
      V3_VENUE: {}, console: { log() {} },
      db: { getPendingActions: () => [{ id: 1 }] },
      fetchSubaccount: async () => ({}), fetchPositions: async () => [],
      [method]: async () => { throw failure; },
    });
    await assert.rejects(confirm([], {}, 3000), /account read failed/);
  }
});

test('actual strategy position mapping preserves valid V3 zero Greeks', async () => {
  const fetchPositions = strategyFunction('fetchPositions', {
    V3_VENUE: {}, SUBACCOUNT_ID: 777, DERIVE_ACCOUNT_ADDRESS: profile.ownerAddress,
    createWallet: () => ({}), signMessage: async () => 'test-signature',
    derivePost: async () => ({ data: { result: { positions: [{ instrument_name: 'ETH-PERP', amount: '1', greeks: { delta: '0', theta: '0', vega: '0' } }] } } }),
  });
  const [position] = await fetchPositions();
  assert.equal(position.delta, 0);
  assert.equal(position.theta, 0);
  assert.equal(position.vega, 0);
});
