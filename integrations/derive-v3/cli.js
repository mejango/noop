#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { readProfile, loadKey } = require('./profile');
const { DeriveV3 } = require('./index');
const { stateDirectory, openJournal, acquireWriter } = require('./state');
const { testnetEnvironment, saveTestnetAccount } = require('./testnet-config');

function testnetProfile() {
  const env = testnetEnvironment();
  for (const [key, value] of Object.entries(env)) if (value != null) process.env[key] = value;
  return readProfile();
}

async function publicSmoke(adapter) {
  const universes = await adapter.read('public/get_risk_universes');
  if (!Array.isArray(universes)) throw new Error('Invalid risk universe response');
  const instruments = await adapter.instruments();
  if (!instruments.length) throw new Error('No ETH option instruments returned');
  const expiryDate = Number(instruments.map(i => i.instrument_name.split('-')[1]).sort()[0]);
  const tickers = await adapter.read('public/get_tickers', { currency: 'ETH', instrument_type: 'option', expiry_date: expiryDate });
  if (!tickers?.tickers || !Object.keys(tickers.tickers).length) throw new Error('No ETH option tickers returned');
  const matching = instruments.filter(i => tickers.tickers[i.instrument_name]);
  if (!matching.length) throw new Error('Instrument/ticker names do not match');
  for (const i of matching) {
    if (!i.option_details || !i.base_asset_address || !/^\d+$/.test(i.base_asset_sub_id)) throw new Error('Missing order-signing instrument metadata');
  }
  let margin;
  try {
    const result = await adapter.read('public/get_margin', {
      margin_type: 'SM', simulated_collaterals: [{ asset_name: 'USDC', amount: '100' }], simulated_positions: [],
    });
    margin = { available: true, result };
  } catch (error) {
    margin = { available: false, code: error.code ?? null, message: error.message };
  }
  return {
    timestamp: new Date().toISOString(), venue: adapter.profile.name,
    sdk_package: require('./package.json').dependencies['@derivexyz/derive-ts'],
    sdk_reported_version: require('@derivexyz/derive-ts').SDK_VERSION,
    checks: { public_data: 'passed', authenticated_account: 'not_run', signing: 'not_run', order_lifecycle: 'not_run' },
    option_count: instruments.length, sampled_expiry: expiryDate, ticker_count: Object.keys(tickers.tickers).length,
    matched_count: matching.length,
    eth_managers: universes.flatMap(u => u.managers.filter(m => m.instruments.includes('ETH-OPTION')).map(m => ({
      universe: u.risk_universe_id, manager: m.manager_id, margin_type: m.margin_type,
    }))), margin_simulation: margin,
  };
}

async function accountSmoke(adapter) {
  const account = await adapter.account();
  const id = adapter.profile.subaccountId;
  const orders = await adapter.read('private/get_open_orders', { subaccount_id: id });
  const trades = await adapter.history('private/get_trade_history', { from_timestamp: Date.now() - 86400000 });
  return { subaccount_id: id, manager_id: account.manager_id, risk_universe_id: account.risk_universe_id,
    margin_type: account.margin_type, initial_margin: account.initial_margin, maintenance_margin: account.maintenance_margin,
    positions: account.positions.length, open_orders: orders.orders.length, recent_trades: trades.length };
}

async function signingSmoke(adapter) {
  await adapter.account();
  const instruments = await adapter.instruments();
  const { OrdersApi, SignedAction, domainSeparator } = require('@derivexyz/derive-ts');
  const { AbiCoder, parseUnits } = require('ethers');
  const samples = ['P', 'C'].map(type => instruments.find(i => i.option_details?.option_type === type));
  if (samples.some(i => !i)) throw new Error('Need both a put and a call for signing checks');
  const checks = [];
  for (const instrument of samples) {
    // Use the public SDK namespace with a read-only transport: build a real
    // signed order, but send it only to order_debug, never to private/order.
    const ctx = {
      network: adapter.client.network, credentials: () => adapter.client.credentials(),
      send: async (method, wire) => {
        if (method !== 'private/order') throw new Error('Unexpected signing-test route');
        const debug = await adapter.read('private/order_debug', wire);
        const data = AbiCoder.defaultAbiCoder().encode(
          ['address', 'uint256', 'int256', 'int256', 'uint256', 'uint256', 'bool'],
          [instrument.base_asset_address, instrument.base_asset_sub_id, parseUnits(wire.limit_price, 18),
            parseUnits(wire.amount, 18), parseUnits(wire.max_fee, 18), wire.subaccount_id, true]);
        const action = new SignedAction({ subaccountId: wire.subaccount_id, nonce: wire.nonce,
          module: ctx.network.modules.trade, data, expirySec: wire.signature_expiry_sec,
          owner: adapter.profile.ownerAddress, signer: wire.signer }, domainSeparator(ctx.network));
        if (debug.typed_data_hash !== action.digest() || debug.encoded_data !== data || debug.action_hash !== action.actionHash()) {
          throw new Error('V3 server and local signing hashes differ');
        }
        checks.push({ instrument: instrument.instrument_name, result: 'server_hashes_match', typed_data_hash: debug.typed_data_hash });
        return debug;
      },
    };
    const orders = new OrdersApi(ctx, adapter.client.marketData);
    await orders.place({ subaccountId: adapter.profile.subaccountId, instrumentName: instrument.instrument_name,
      direction: 'buy', amount: instrument.minimum_amount, limitPrice: '1', maxFee: '10',
      timeInForce: 'ioc', reduceOnly: true });
  }
  return { checks, orders_submitted: 0 };
}

async function orderSmoke(adapter) {
  if (adapter.profile.network !== 'testnet') throw new Error('Order smoke is testnet-only');
  const before = await accountSmoke(adapter);
  if (before.positions || before.open_orders) throw new Error('Order smoke requires an empty testnet account; reconcile existing exposure first');
  const instrument = await adapter.read('public/get_instrument', { instrument_name: 'ETH-PERP' });
  const ticker = await adapter.read('public/get_ticker', { instrument_name: 'ETH-PERP' });
  const { parseUnits, formatUnits } = require('ethers');
  const bid = parseUnits(String(ticker.b), 18);
  const step = parseUnits(instrument.tick_size, 18);
  // Small post-only buy well below the current bid; always cancel immediately after acknowledgement.
  const minimumPrice = parseUnits(String(ticker.minp || '0'), 18);
  const price = ((minimumPrice > bid * 99n / 100n ? minimumPrice : bid * 99n / 100n) + step - 1n) / step * step;
  const amount = instrument.minimum_amount;
  if (price <= 0n || price >= bid || parseUnits(amount, 18) > parseUnits('0.1', 18)) throw new Error('Unexpected smoke order size/price');
  if (parseUnits(amount, 18) * price / 10n ** 18n > parseUnits('500', 18)) throw new Error('Smoke order exceeds $500 testnet notional cap');
  const result = await adapter.place({ subaccountId: adapter.profile.subaccountId,
    instrumentName: 'ETH-PERP', direction: 'buy', amount, limitPrice: formatUnits(price, 18), maxFee: '10',
    timeInForce: 'post_only', reduceOnly: false, rejectPostOnly: true, label: 'noop-v3-smoke' });
  const order = result.order || result;
  if (!order.order_id) throw new Error('Order acknowledgement missing ID; inspect execution journal');
  let cancelled;
  try {
    cancelled = await adapter.cancel(order.order_id, 'ETH-PERP');
  } finally {
    // Terminal orders can disappear from get_order immediately; use history or
    // the explicit cancellation acknowledgement, never infer cancellation from absence.
    const final = await adapter.orderStatus(order.order_id) || cancelled;
    const record = final?.order || final;
    if (!record) throw new Error('Smoke order status is unknown; reconcile its journal entry');
    if (!['cancelled', 'filled', 'expired'].includes(record.order_status)) throw new Error('Smoke order is not terminal; cancel it before continuing');
    if (Number(record.filled_amount) > 0) throw new Error('Smoke order filled: testnet exposure requires reconciliation before running another test');
  }
  const after = await accountSmoke(adapter);
  if (after.positions || after.open_orders) throw new Error('Smoke test left positions or open orders; reconcile before continuing');
  adapter.markAccounted(order.order_id);
  return { order_id: order.order_id, result: 'placed_and_cancelled_without_fill' };
}

async function reconcileOrders(adapter, journal) {
  const open = await adapter.read('private/get_open_orders', { subaccount_id: adapter.profile.subaccountId });
  if (!Array.isArray(open?.orders)) throw new Error('Malformed open orders during reconciliation');
  const history = await adapter.history('private/get_order_history', {}, 'orders');
  const report = { resolved: [], unresolved: [], accounting_required: [] };
  for (const nonce of journal.unresolved) {
    const found = [...open.orders, ...history].find(o => o.nonce === nonce);
    if (found) {
      journal.append({ event: 'order_reconciled', nonce, order: found });
      // Discovery is not accounting. Only a confirmed terminal zero-fill order
      // can be cleared without importing fills, costs and resting exposure.
      if (['cancelled', 'expired'].includes(found.order_status)
        && found.subaccount_id === adapter.profile.subaccountId && typeof found.order_id === 'string'
        && found.order_id && typeof found.filled_amount === 'string' && /^0(?:\.0+)?$/.test(found.filled_amount)) {
        journal.append({ event: 'order_accounted', nonce, order_id: found.order_id, reason: 'verified_terminal_zero_fill' });
        report.resolved.push(found.order_id);
        continue;
      }
      report.accounting_required.push({ nonce, order_id: found.order_id, status: found.order_status, filled_amount: found.filled_amount });
    }
    report.unresolved.push(nonce); // Absence is not proof of rejection.
  }
  return report;
}

async function main() {
  const command = process.argv[2] || 'public';
  const profile = testnetProfile();
  if (command === 'init') {
    const { Wallet } = require('ethers');
    const dir = stateDirectory(profile);
    const keyFile = path.join(dir, 'testnet.key');
    const wallet = Wallet.createRandom();
    fs.writeFileSync(keyFile, wallet.privateKey, { flag: 'wx', mode: 0o600 });
    saveTestnetAccount({ owner_address: wallet.address, key_file: keyFile });
    console.log(JSON.stringify({ owner_address: wallet.address, key_file: keyFile,
      next: 'Fund this wallet with Sepolia ETH and Derive testnet USDC, then run deposit with explicit owner/key configuration.' }, null, 2));
    return;
  }
  if (command === 'discover') {
    const { DeriveClient } = require('@derivexyz/derive-ts');
    if (!profile.ownerAddress) throw new Error('Initialize the testnet wallet first');
    const client = new DeriveClient({ network: require('./deployment').sdkNetwork('testnet'),
      sessionKey: loadKey(profile), ownerAddress: profile.ownerAddress });
    try {
      const { subaccount_ids } = await client.send('private/get_subaccounts', { wallet: profile.ownerAddress });
      const accounts = [];
      for (const id of subaccount_ids) {
        const account = await client.send('private/get_subaccount', { subaccount_id: id });
        if (account.risk_universe_id !== 0 && !account.failed_to_fetch) accounts.push(account);
      }
      if (accounts.length !== 1) throw new Error(`Found ${accounts.length} trading subaccounts; wait for credit or configure the intended ID explicitly`);
      const account = accounts[0];
      saveTestnetAccount({ owner_address: profile.ownerAddress, key_file: profile.keyFile, subaccount_id: account.subaccount_id });
      console.log(JSON.stringify({ subaccount_id: account.subaccount_id, manager_id: account.manager_id,
        risk_universe_id: account.risk_universe_id, subaccount_value: account.subaccount_value }, null, 2));
    } finally { await client.close(); }
    return;
  }
  if (!['public', 'account', 'signing', 'order', 'deposit', 'reconcile'].includes(command)) throw new Error('Use init, discover, public, account, signing, order, deposit, or reconcile');
  if (command === 'deposit') {
    if (!profile.ownerAddress) throw new Error('Set the testnet owner address');
    const { DeriveClient } = require('@derivexyz/derive-ts');
    const { Wallet, JsonRpcProvider, Contract, parseUnits } = require('ethers');
    const provider = new JsonRpcProvider('https://ethereum-sepolia-rpc.publicnode.com');
    let client, release;
    try {
      if ((await provider.getNetwork()).chainId !== 11155111n) throw new Error('Deposit RPC is not Sepolia');
      const signer = new Wallet(loadKey(profile), provider);
      if (signer.address.toLowerCase() !== profile.ownerAddress.toLowerCase()) throw new Error('Deposit requires the dedicated testnet owner key');
      const network = require('./deployment').sdkNetwork('testnet');
      if (await provider.getCode(network.contracts.actionManager) === '0x') throw new Error('Testnet ActionManager has no code');
      client = new DeriveClient({ network, wallet: signer });
      const universes = await client.marketData.getRiskUniverses();
      const manager = universes.flatMap(u => u.managers).find(m => m.margin_type === 'SM' && m.instruments.includes('ETH-OPTION') && m.collaterals.some(c => c.name === 'USDC'));
      if (!manager) throw new Error('No suitable testnet manager');
      const usdc = manager.collaterals.find(c => c.name === 'USDC');
      if (usdc.erc20.underlying_erc20.toLowerCase() !== network.contracts.usdc.toLowerCase()) throw new Error('Testnet USDC deployment changed; revalidate before depositing');
      const token = new Contract(usdc.erc20.underlying_erc20, ['function balanceOf(address) view returns (uint256)'], provider);
      const amount = '100';
      if (await provider.getBalance(signer.address) === 0n || await token.balanceOf(signer.address) < parseUnits(amount, usdc.erc20.decimals)) {
        throw new Error(`Fund ${signer.address} with Sepolia ETH and at least 100 testnet USDC (${usdc.erc20.underlying_erc20}); use https://testnet.app.derive.xyz/developers`);
      }
      release = acquireWriter(profile);
      const deposit = await client.deposits.contractCall.depositToNewSubaccount({ signer, asset: usdc.address,
        erc20: usdc.erc20.underlying_erc20, amount, managerId: manager.manager_id });
      console.log(JSON.stringify({ ...deposit, manager_id: manager.manager_id, next: 'Wait for credit, discover the non-fallback subaccount in the testnet UI, then set DERIVE_V3_TESTNET_SUBACCOUNT_ID.' }, null, 2));
    } finally {
      try { release?.(); await client?.close(); } finally { provider.destroy(); }
    }
    return;
  }
  let release;
  let adapter;
  try {
    if (command === 'order' || command === 'reconcile') release = acquireWriter(profile);
    // Acquire the writer before reading its journal; another writer may finish
    // between a pre-lock read and lock acquisition.
    const journal = command === 'public' ? null : openJournal(profile);
    adapter = new DeriveV3({ profile, authenticated: command !== 'public', journal: journal?.append });
    adapter.pendingOrder = journal?.unresolved[0] || null;
    let report;
    if (command === 'public') report = await publicSmoke(adapter);
    if (command === 'account') report = await accountSmoke(adapter);
    if (command === 'signing') report = await signingSmoke(adapter);
    if (command === 'order') report = await orderSmoke(adapter);
    if (command === 'reconcile') {
      report = await reconcileOrders(adapter, journal);
    }
    const filename = path.join(stateDirectory(profile), `${command}-report.json`);
    fs.writeFileSync(filename, JSON.stringify(report, null, 2), { mode: 0o600 });
    console.log(JSON.stringify({ ...report, report_file: filename }, null, 2));
  } finally { release?.(); await adapter?.close(); }
}

if (require.main === module) main().catch(error => {
  // SDK exceptions can embed signed request payloads; emit only a terse diagnostic.
  console.error(`${error.name || 'Error'}: ${String(error.message).split(' — ')[0]}`);
  process.exitCode = 1;
});

module.exports = { publicSmoke, accountSmoke, orderSmoke, reconcileOrders };
