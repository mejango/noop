#!/usr/bin/env node
'use strict';

// Import-safe diagnostics only. This module cannot sign a trading action or call a mutation endpoint.
const fs = require('node:fs');
const path = require('node:path');
const axios = require('axios');
const { Wallet } = require('ethers');
const { getDeriveConfig, authHeaders } = require('../bot/derive-config');
const { checkedSqlitePath } = require('../bot/sqlite-paths');
const { toUnits, fromUnits, SCALE } = require('../bot/spot-accumulation');

// The existing NOOP options account stays in PRIME; DRV destination eligibility
// comes from its instrument metadata, not from changing the source universe.
const SOURCE_RISK_UNIVERSE_ID = 1;

const READ_METHODS = new Set([
  'private/get_subaccount', 'private/get_account', 'private/session_keys',
  'private/get_open_orders', 'private/get_margin', 'private/order_quote',
  'public/get_instrument', 'public/get_ticker', 'public/get_wallet',
]);
const positiveMargin = value => (typeof value === 'string' || typeof value === 'number')
  && String(value).trim() !== '' && Number.isFinite(Number(value)) && Number(value) > 0;
const positiveId = value => Number.isSafeInteger(Number(value)) && Number(value) > 0;
const sameAddress = (a, b) => typeof a === 'string' && typeof b === 'string' && a.toLowerCase() === b.toLowerCase();
const errorInfo = error => ({ code: error?.response?.data?.error?.code ?? error?.code ?? null,
  message: error?.response?.data?.error?.message || (error?.response ? 'Venue HTTP request failed' : 'Venue evidence unavailable') });
const pick = (value, keys) => Object.fromEntries(keys.filter(key => value?.[key] !== undefined).map(key => [key, value[key]]));

function createReadOnlyPost({ config, wallet, http = axios, now = Date.now }) {
  // Validate origin through the existing configuration owner before using credentials.
  const verified = getDeriveConfig({ DERIVE_API_VERSION: config.version, DERIVE_NETWORK: config.network,
    DERIVE_API_URL: config.baseUrl, DERIVE_DOMAIN_SEPARATOR: config.domainSeparator,
    DERIVE_WALLET: config.wallet, DERIVE_SUBACCOUNT_ID: String(config.subaccountId), DERIVE_HISTORY_FROM: config.historyFrom });
  if (verified.version !== 'v3') throw new Error('Spot preflight requires Derive V3');
  return async (method, params) => {
    if (!READ_METHODS.has(method)) throw new Error('Preflight endpoint is not read-only');
    const timestamp = now();
    const headers = method.startsWith('private/')
      ? authHeaders(verified, timestamp, await wallet.signMessage(String(timestamp))) : {};
    const response = await http.post(`${verified.baseUrl}/${method}`, params, { headers, timeout: 15000, maxRedirects: 0 });
    if (response.data?.error || response.data?.result == null) {
      const error = new Error('Venue evidence unavailable'); error.code = response.data?.error?.code; throw error;
    }
    return response.data.result;
  };
}

function quoteRequest(input, destinationSubaccountId, signer) {
  if (!input || !positiveId(destinationSubaccountId) || Number(input.subaccount_id) !== Number(destinationSubaccountId)
    || input.instrument_name !== 'DRV-USDC' || input.direction !== 'buy' || input.time_in_force !== 'ioc'
    || !['amount', 'limit_price', 'max_fee'].every(k => typeof input[k] === 'string' && /^\d+(?:\.\d+)?$/.test(input[k]) && Number.isFinite(Number(input[k])) && Number(input[k]) > 0)
    || !Number.isSafeInteger(input.signature_expiry_sec) || input.signature_expiry_sec <= 0) {
    throw new Error('Preview requires explicit positive DRV-USDC IOC terms for the configured destination');
  }
  return { ...pick(input, ['subaccount_id', 'instrument_name', 'direction', 'amount', 'limit_price', 'max_fee', 'time_in_force', 'signature_expiry_sec']),
    order_type: 'limit', reduce_only: false, signer, nonce: '0', signature: '0x' };
}

async function preflightSpot({ config, post, now = Date.now, signer = null, destinationSubaccountId = null, quotes = [] }) {
  if (config.version !== 'v3' || !positiveId(config.subaccountId)) throw new Error('Explicit V3 source account required');
  if (destinationSubaccountId != null && (!positiveId(destinationSubaccountId) || Number(destinationSubaccountId) === Number(config.subaccountId))) {
    throw new Error('Destination must be a distinct positive subaccount ID');
  }
  const read = async (method, params) => {
    if (!READ_METHODS.has(method)) throw new Error('Preflight endpoint is not read-only');
    try { const result = await post(method, params); return result?.error ? { error: { code: result.error.code ?? null, message: 'Venue evidence unavailable' } } : result; }
    catch (error) { return { error: errorInfo(error) }; }
  };
  const report = { checked_at: new Date(now()).toISOString(), ready_to_trade: false,
    source_subaccount_id: Number(config.subaccountId), destination_subaccount_id: destinationSubaccountId == null ? null : Number(destinationSubaccountId),
    request_signer: signer, credentials_exported: false, mutations: false, blockers: [], instruments: {}, accounts: {}, previews: [] };
  report.instruments.DRV = await read('public/get_instrument', { instrument_name: 'DRV-USDC' });
  const drvInstrument = report.instruments.DRV;
  const drvInstrumentVerified = drvInstrument.instrument_name === 'DRV-USDC' && drvInstrument.instrument_type === 'erc20'
    && drvInstrument.is_active === true && positiveId(drvInstrument.risk_universe_id) && !drvInstrument.error;
  if (!drvInstrumentVerified) report.blockers.push('drv_instrument_unverified');
  report.drv_ticker = await read('public/get_ticker', { instrument_name: 'DRV-USDC' });
  const [session, account] = await Promise.all([
    read('private/session_keys', { wallet: config.wallet }), read('private/get_account', { wallet: config.wallet }),
  ]);
  const key = session.public_session_keys?.find(k => sameAddress(k.public_session_key, signer));
  report.session = session.error ? session : { matching_key: key ? pick(key, ['public_session_key', 'offchain_scopes', 'protocol_scopes', 'expiry_sec', 'subaccount_ids']) : null };
  report.fee_info = account.error ? account : account.fee_info || null;
  const protocol = new Set(key?.protocol_scopes || []);
  const canSpot = protocol.has('admin') || protocol.has('trade:all') || protocol.has('trade:orderbook:spot');
  const canTransfer = protocol.has('admin') || protocol.has('transfer:existing_subaccount');
  if (!key || Number(key.expiry_sec) <= Math.floor(now() / 1000)) report.blockers.push('current_session_key_unverified_or_expired');
  if (!canSpot) report.blockers.push('spot_trading_scope_unverified');
  if (!canTransfer) report.blockers.push('internal_transfer_scope_unverified');
  if (destinationSubaccountId == null) report.blockers.push('alt_destination_not_configured');
  const accountIds = new Set([Number(config.subaccountId), ...(destinationSubaccountId == null ? [] : [Number(destinationSubaccountId)])]);
  const eligible = new Set();
  for (const id of accountIds) {
    const [state, ownerResult, ordersResult] = await Promise.all([
      read('private/get_subaccount', { subaccount_id: id }), read('public/get_wallet', { subaccount_id: id }),
      read('private/get_open_orders', { subaccount_id: id }),
    ]);
    const owner = typeof ownerResult === 'string' ? ownerResult : ownerResult.wallet;
    const orders = Array.isArray(ordersResult) ? ordersResult : ordersResult.orders;
    const scopeAllows = key && (key.subaccount_ids == null || (Array.isArray(key.subaccount_ids) && (key.subaccount_ids.length === 0 || key.subaccount_ids.map(Number).includes(id))));
    const isSource = id === Number(config.subaccountId);
    const expectedUniverse = isSource ? SOURCE_RISK_UNIVERSE_ID : Number(drvInstrument.risk_universe_id);
    report.accounts[id] = state.error ? state : {
      ...pick(state, ['subaccount_id', 'manager_id', 'risk_universe_id', 'margin_type', 'is_under_liquidation', 'initial_margin', 'maintenance_margin']),
      owner_verified: sameAddress(owner, config.wallet), session_account_allowed: !!scopeAllows,
      collaterals: state.collaterals?.map(c => pick(c, ['asset_name', 'currency', 'amount'])),
      open_orders: orders?.map(o => pick(o, ['order_id', 'instrument_name', 'direction', 'amount', 'filled_amount', 'limit_price', 'max_fee', 'order_status'])) || ordersResult,
    };
    if (Number(state.subaccount_id) !== id || !Array.isArray(state.collaterals) || !sameAddress(owner, config.wallet)
      || !scopeAllows || (!isSource && !drvInstrumentVerified) || Number(state.risk_universe_id) !== expectedUniverse || state.is_under_liquidation !== false
      || !positiveMargin(state.initial_margin) || !positiveMargin(state.maintenance_margin)
      || !Array.isArray(orders) || state.error) report.blockers.push(`account_${id}_eligibility_unverified`);
    else eligible.add(id);
  }
  if (!report.fee_info || report.fee_info.error) report.blockers.push('account_fees_unverified');
  if (quotes.length === 0) report.blockers.push('private_order_preview_not_requested');
  for (const input of quotes) {
    const request = quoteRequest(input, destinationSubaccountId, signer);
    if (!eligible.has(Number(request.subaccount_id)) || !canSpot) {
      report.previews.push({ request, deferred: 'account_or_scope_unverified' }); continue;
    }
    const preview = await read('private/order_quote', request);
    const cost = (toUnits(request.amount) * (toUnits(request.limit_price) + toUnits(request.max_fee)) + SCALE - 1n) / SCALE;
    const margin = await read('private/get_margin', { subaccount_id: request.subaccount_id,
      simulated_collateral_changes: [
        { asset_name: request.instrument_name.split('-')[0], amount: request.amount },
        { asset_name: 'USDC', amount: `-${fromUnits(cost)}` },
      ] });
    report.previews.push({ request, response: preview, worst_case_margin: margin, note: 'Non-binding preview only; no executable signature.' });
    if (preview?.is_valid !== true || !positiveMargin(preview.post_initial_margin)
      || margin?.is_valid_trade !== true || !positiveMargin(margin.post_initial_margin) || !positiveMargin(margin.post_maintenance_margin)) report.blockers.push(`preview_${request.instrument_name}_unverified`);
  }
  report.blockers.push('transfer_timeout_recovery_unverified', 'collateral_settlement_timing_unverified', 'live_activation_not_performed');
  return report;
}

async function main(argv = process.argv.slice(2)) {
  const allowed = new Set(['--out', '--destination', '--quotes']);
  const options = {};
  for (let i = 0; i < argv.length; i += 2) {
    if (!allowed.has(argv[i]) || Object.hasOwn(options, argv[i]) || !argv[i + 1] || argv[i + 1].startsWith('--')) throw new Error('Usage: derive-spot-preflight.js [--destination ID] [--quotes FILE] [--out NEW_FILE]');
    options[argv[i]] = argv[i + 1];
  }
  // Validate report destinations before credentials or any network access. Reports must
  // never occupy a database, a source input, or a SQLite sidecar (including aliases).
  const defaultDatabase = path.join(__dirname, '../data/noop.db');
  const configuredDatabase = process.env.NOOP_DB_PATH || path.join(process.env.DATA_DIR || path.dirname(defaultDatabase), 'noop.db');
  const output = options['--out'] ? checkedSqlitePath(options['--out'], {
    protectedPaths: [defaultDatabase, configuredDatabase, ...(options['--quotes'] ? [options['--quotes']] : [])],
    mustBeNew: true, label: 'Preflight report',
  }) : null;
  const config = getDeriveConfig();
  const wallet = new Wallet((process.env.PRIVATE_KEY || fs.readFileSync(path.join(__dirname, '../.private_key.txt'), 'utf8')).trim());
  const quotes = options['--quotes'] ? JSON.parse(fs.readFileSync(options['--quotes'], 'utf8')) : [];
  if (!Array.isArray(quotes)) throw new Error('Quotes file must contain an array of explicit preview requests');
  const report = await preflightSpot({ config, signer: wallet.address, post: createReadOnlyPost({ config, wallet }),
    destinationSubaccountId: options['--destination'] ?? null, quotes });
  const serialized = JSON.stringify(report, null, 2) + '\n';
  if (output) fs.writeFileSync(output, serialized, { flag: 'wx', mode: 0o600 });
  console.log(serialized);
}

if (require.main === module) main().catch(() => { console.error('Spot preflight failed. Check explicit V3 account configuration, credential availability and command arguments.'); process.exitCode = 1; });
module.exports = { preflightSpot, createReadOnlyPost, main };
