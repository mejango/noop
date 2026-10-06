#!/usr/bin/env node
'use strict';

// Read-only: never starts the bot, opens its database, or submits/cancels orders.
const fs = require('node:fs');
const path = require('node:path');
const axios = require('axios');
const { Wallet } = require('ethers');
const { getDeriveConfig, authHeaders, fetchInstruments } = require('../bot/derive-config');

async function preflight({ config, post, privateReads = true }) {
  const instruments = await fetchInstruments((method, params) => post(`public/${method}`, params), config.version,
    { currency: 'ETH', expired: false, instrument_type: 'option' });
  if (!instruments.length) throw new Error('No live ETH options available');
  const report = { checked_at: new Date().toISOString(), version: config.version, network: config.network,
    base_url: config.baseUrl, maintenance: config.maintenance, instruments: instruments.length, private_reads: privateReads };
  if (!privateReads) return report;
  const account = await post('private/get_subaccount', { subaccount_id: config.subaccountId });
  if (!account || account.failed_to_fetch || account.error || Number(account.subaccount_id) !== config.subaccountId
    || !Array.isArray(account.positions) || !Array.isArray(account.collaterals)) throw new Error('Account evidence unavailable or mismatched');
  if (config.version === 'v3' && !Array.isArray(account.currency)) throw new Error('Expected V3 account currency array');
  for (const field of ['subaccount_value', 'initial_margin', 'maintenance_margin']) {
    if (account[field] == null || String(account[field]).trim() === '' || !Number.isFinite(Number(account[field]))) throw new Error(`Invalid account ${field}`);
  }
  // Retain complete raw evidence for human comparison with the V2 snapshot.
  const openOrders = await post('private/get_open_orders', { subaccount_id: config.subaccountId });
  if (!Array.isArray(openOrders) && !Array.isArray(openOrders?.orders)) throw new Error('Open-order evidence unavailable');
  return { ...report, wallet: config.wallet, subaccount_id: config.subaccountId, account, open_orders: openOrders,
    ready_to_trade: false, note: 'Read checks only. Compare collateral, positions, local pending submissions and orders; validate signing separately before enabling trading.' };
}

async function main(argv = process.argv.slice(2)) {
  const config = getDeriveConfig();
  const privateReads = !argv.includes('--public');
  const outputIndex = argv.indexOf('--out');
  const output = outputIndex >= 0 ? argv[outputIndex + 1] : null;
  if (outputIndex >= 0 && (!output || output.startsWith('--'))) throw new Error('--out requires a new report filename');
  const wallet = privateReads ? new Wallet((process.env.PRIVATE_KEY || fs.readFileSync(path.join(__dirname, '../.private_key.txt'), 'utf8')).trim()) : null;
  const post = async (method, body) => {
    const timestamp = Date.now();
    const headers = method.startsWith('private/') ? authHeaders(config, timestamp, await wallet.signMessage(String(timestamp))) : {};
    const response = await axios.post(`${config.baseUrl}/${method}`, body, { headers, timeout: 15000, maxRedirects: 0 });
    if (response.data?.error || response.data?.result == null) throw new Error(`${method}: venue evidence unavailable`);
    return response.data.result;
  };
  const report = await preflight({ config, post, privateReads });
  if (output) fs.writeFileSync(output, JSON.stringify(report, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
  console.log(JSON.stringify({ ...report, account: report.account ? (output ? '[saved in report]' : '[omitted; use --out to retain evidence]') : undefined,
    open_orders: report.open_orders ? (output ? '[saved in report]' : '[omitted; use --out to retain evidence]') : undefined }, null, 2));
}

if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode = 1; });
module.exports = { preflight };
