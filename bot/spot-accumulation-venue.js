'use strict';

// No environment, database, credential-file or network access at import time.
const axios = require('axios');
const { getDeriveConfig, authHeaders, orderNonce } = require('./derive-config');
const { signOrder } = require('./derive-signing');
const { isDefinitiveOrderRejection, rateLimitWaitMs } = require('./derive-errors');
const { collectRecoveryEvidence } = require('./reconcile-execution');
const { POLICY, SCALE, toUnits, fromUnits } = require('./spot-accumulation');

const READ_METHODS = new Set(['private/get_subaccount', 'private/get_account', 'private/session_keys',
  'private/get_open_orders', 'private/get_margin', 'private/order_quote', 'private/get_order',
  'private/get_order_history', 'private/get_trade_history', 'private/get_erc20_transfer_history',
  'public/get_instrument', 'public/get_ticker', 'public/get_wallet', 'public/get_all_currencies', 'public/get_transaction']);
const WRITE_METHODS = new Set(['private/order', 'private/transfer_spot']);
// V3 decimal monetary fields accept twelve places; the fee cap must strictly
// exceed the current private quote suggestion (cap zero still returns a quote).
const FEE_QUANTUM = 10n ** 6n;
const sameAddress = (a, b) => typeof a === 'string' && typeof b === 'string' && a.toLowerCase() === b.toLowerCase();
const positiveId = x => /^[1-9]\d*$/.test(String(x)) && Number.isSafeInteger(Number(x));
const ceil = (a, b) => (a + b - 1n) / b;
const cost = (amount, price, fee) => ceil(toUnits(amount, 'amount', false) * (toUnits(price, 'price', false) + toUnits(fee, 'max fee')), SCALE);
const money = (x, label) => toUnits(typeof x === 'number' && Number.isFinite(x) ? String(x) : x, label);
function positiveMargin(value) {
  try { return money(value, 'margin') > 0n; } catch { return false; }
}
function object(value) {
  if (typeof value !== 'string') return value || {};
  try { return JSON.parse(value); } catch { return {}; }
}

/** Reserve every outstanding cash debit once; missing executable terms defer. */
function sourceCashCommitments({ openOrders, pendingActions, subaccountId }) {
  if (!Array.isArray(openOrders) || !Array.isArray(pendingActions)) throw new Error('Cash commitment evidence is incomplete');
  const orders = new Map();
  let total = 0n;
  for (const row of openOrders) {
    if (!row?.order_id || (row.subaccount_id != null && Number(row.subaccount_id) !== Number(subaccountId))) throw new Error('Open order identity is invalid');
    if (orders.has(String(row.order_id))) throw new Error('Duplicate open order identity');
    if (!['buy', 'sell'].includes(row.direction)) throw new Error('Open order direction is unknown');
    orders.set(String(row.order_id), row);
    if (row.direction !== 'buy') continue;
    const amount = money(row.amount, 'open order amount');
    const filled = money(row.filled_amount, 'open order filled amount');
    if (filled > amount) throw new Error('Open order fill exceeds amount');
    const price = money(row.limit_price, 'open order price');
    const fee = money(row.max_fee, 'open order fee cap');
    if (price === 0n) throw new Error('Open order price is missing');
    total += ceil((amount - filled) * (price + fee), SCALE);
  }
  const pendingIds = new Set();
  for (const row of pendingActions) {
    if (!['buy_put', 'buyback_call'].includes(row.action) || !['pending', 'confirmed', 'resting'].includes(row.status)) continue;
    if (row.subaccount_id != null && Number(row.subaccount_id) !== Number(subaccountId)) throw new Error('Pending action account mismatch');
    if (row.id == null || pendingIds.has(String(row.id))) throw new Error('Pending action identity is missing or duplicated');
    pendingIds.add(String(row.id));
    const execution = object(row.execution_result);
    const links = [row.order_id, execution.orderId, execution.order_id, execution.order?.order_id,
      execution.result?.order?.order_id].filter(value => value != null).map(String);
    if (links.some(value => !value) || new Set(links).size > 1) throw new Error('Conflicting pending order links');
    const linked = links[0];
    const linkedOrder = linked && orders.get(String(linked));
    const linkedByPending = [...orders.values()].find(order => String(order.pending_action_id) === String(row.id));
    if (linked && linkedByPending && String(linkedByPending.order_id) !== linked) throw new Error('Conflicting pending order links');
    if (linkedOrder || linkedByPending) {
      const order = linkedOrder || linkedByPending;
      if (order.direction !== 'buy' || order.instrument_name !== row.instrument_name) throw new Error('Linked pending order conflicts with action');
      continue;
    }
    const details = object(row.trigger_details);
    const request = execution.request || details.request || {};
    const amount = money(row.amount, 'pending action amount');
    const price = money(request.limit_price ?? row.limit_price ?? row.price, 'pending action price');
    const fee = money(request.max_fee ?? row.max_fee ?? details.max_fee, 'pending action fee cap');
    if (amount === 0n || price === 0n) throw new Error('Pending cash debit lacks positive terms');
    total += ceil(amount * (price + fee), SCALE);
  }
  return fromUnits(total);
}

function createSpotVenue({ config, wallet, post: injectedPost, http = axios, now = Date.now,
  sleep = ms => new Promise(resolve => setTimeout(resolve, ms)),
  assertCanSend = () => { throw new Error('Spot financial sends are disabled'); } }) {
  const owner = config.accountAddress || config.wallet;
  const sourceId = Number(config.sourceSubaccountId ?? config.subaccountId);
  const destinationId = Number(config.destinationSubaccountId);
  const verified = getDeriveConfig({ DERIVE_API_VERSION: config.version, DERIVE_NETWORK: config.network,
    DERIVE_API_URL: config.baseUrl, DERIVE_DOMAIN_SEPARATOR: config.domainSeparator, DERIVE_WALLET: owner,
    DERIVE_SUBACCOUNT_ID: String(sourceId), DERIVE_HISTORY_FROM: config.historyFrom,
    DERIVE_MAINTENANCE: String(config.maintenance !== false) });
  if (verified.version !== 'v3' || !positiveId(destinationId) || destinationId === sourceId
    || !/^0x[\da-f]{40}$/i.test(wallet?.address || '')) throw new Error('Explicit V3 source, distinct destination and signer required');
  if (config.wallet && !sameAddress(config.wallet, owner)) throw new Error('Configured owner identities disagree');
  const guard = async () => {
    if (config.maintenance !== false) throw new Error('Derive maintenance blocks financial actions');
    if (await assertCanSend() === false) throw new Error('Spot financial sends are disabled');
  };
  function rpcFailure(method, request, response) {
    const error = new Error(`Derive ${method} RPC refusal (${response.error?.code ?? 'unknown'})`);
    // Keep structured evidence without copying authentication headers/credentials.
    error.venueResponse = structuredClone(response);
    const evidence = { method, request: structuredClone(request), response: error.venueResponse };
    if (isDefinitiveOrderRejection(evidence)) error.orderRejection = evidence;
    return error;
  }
  async function call(method, params, mutation = false, beforeSendSync) {
    if (!(mutation ? WRITE_METHODS : READ_METHODS).has(method)) throw new Error('Venue endpoint is not allowed');
    if (injectedPost) {
      if (mutation) {
        await guard();
        beforeSendSync();
      }
      let result;
      try { result = await injectedPost(method, params); }
      catch (error) {
        if (error.response?.data?.error) throw rpcFailure(method, params, error.response.data);
        throw error;
      }
      if (result?.error) throw rpcFailure(method, params, result);
      return result;
    }
    const timestamp = now();
    const headers = method.startsWith('private/') ? authHeaders(verified, timestamp, await wallet.signMessage(String(timestamp))) : {};
    const url = `${verified.baseUrl}/${method}`;
    const options = { headers, timeout: 15000, maxRedirects: 0 };
    if (mutation) {
      await guard();
      // The caller atomically reserves and claims immediately before handoff.
      // No await or further fallible preparation may intervene after this hook.
      beforeSendSync();
    }
    let response;
    try { response = await http.post(url, params, options); }
    catch (error) {
      if (error.response?.data?.error) throw rpcFailure(method, params, error.response.data);
      throw error;
    }
    if (response.data?.error) throw rpcFailure(method, params, response.data);
    if (response.data?.result == null) throw new Error(`Derive ${method} evidence unavailable`);
    return response.data.result;
  }
  // Reads are idempotent, so a rate-limited read waits as Derive asks and tries again, twice at
  // most. Orders and transfers never pass through here: a refused send is never repeated.
  const read = async (method, params) => {
    for (let attempt = 0; ; attempt++) {
      try { return await call(method, params); }
      catch (error) {
        const waitMs = rateLimitWaitMs(error.venueResponse?.error);
        if (waitMs == null || attempt >= 2) throw error;
        await sleep(waitMs);
      }
    }
  };
  let inputs = null;
  const prepared = new Map();
  function validateSession(session) {
    const keys = session?.public_session_keys?.filter(key => sameAddress(key.public_session_key, wallet.address));
    if (!keys || keys.length !== 1) throw new Error('Session signer is missing or ambiguous');
    const key = keys[0];
    if (!Number.isSafeInteger(key.expiry_sec) || key.expiry_sec - POLICY.keyMarginSec < Math.ceil(now() / 1000) + POLICY.minSignatureHorizonSec
      || !Array.isArray(key.protocol_scopes) || !Array.isArray(key.offchain_scopes)
      || !key.offchain_scopes.some(scope => ['account_info', 'admin'].includes(scope))) throw new Error('Session expiry or account-info scope is unverified');
    if (!Object.hasOwn(key, 'subaccount_ids') || (key.subaccount_ids !== null && (!Array.isArray(key.subaccount_ids)
      || key.subaccount_ids.some(id => !positiveId(id))))) throw new Error('Session account scope is unknown');
    if (key.subaccount_ids?.length && ![sourceId, destinationId].every(id => key.subaccount_ids.map(Number).includes(id))) throw new Error('Session does not allow both configured accounts');
    const scopes = new Set(key.protocol_scopes);
    if (!['admin', 'trade:all', 'trade:orderbook:spot'].some(scope => scopes.has(scope))
      || !['admin', 'transfer:existing_subaccount'].some(scope => scopes.has(scope))) throw new Error('Session spot or internal-transfer scope is missing');
    return key;
  }
  async function account(id, expectedUniverse, pendingActions) {
    const [state, owned, open] = await Promise.all([read('private/get_subaccount', { subaccount_id: id }),
      read('public/get_wallet', { subaccount_id: id }), read('private/get_open_orders', { subaccount_id: id })]);
    const rows = Array.isArray(open) ? open : open?.orders;
    if (Number(state?.subaccount_id) !== id || Number(state.risk_universe_id) !== expectedUniverse
      || !sameAddress(typeof owned === 'string' ? owned : owned?.wallet, owner)
      || state.is_under_liquidation !== false || state.failed_to_fetch === true
      || !positiveMargin(state.initial_margin) || !positiveMargin(state.maintenance_margin)
      || !Array.isArray(state.collaterals)) throw new Error(`Account ${id} ownership, universe or positive margin is unverified`);
    const cash = state.collaterals.filter(row => row.asset_name === 'USDC');
    if (cash.length !== 1 || (cash[0].currency != null && cash[0].currency !== 'USDC')) throw new Error(`Account ${id} gross USDC evidence is ambiguous`);
    return { subaccount_id: id, risk_universe_id: expectedUniverse, owner,
      usdc_balance: fromUnits(toUnits(cash[0].amount, 'gross USDC balance')),
      committed_usdc: sourceCashCommitments({ openOrders: rows, pendingActions, subaccountId: id }),
      received_at: now(), is_under_liquidation: false, state, open_orders: rows };
  }
  function instrumentBounds(instrument) {
    if (instrument?.instrument_name !== 'DRV-USDC' || instrument.instrument_type !== 'erc20' || instrument.is_active !== true
      || instrument.base_currency !== 'DRV' || instrument.quote_currency !== 'USDC' || !positiveId(instrument.risk_universe_id)
      || !/^0x[\da-f]{40}$/i.test(instrument.base_asset_address || '') || String(instrument.base_asset_sub_id) !== '0') throw new Error('Active DRV spot instrument identity is unverified');
    const bounds = { tick: toUnits(instrument.tick_size, 'tick', false), step: toUnits(instrument.amount_step, 'amount step', false),
      minimum: toUnits(instrument.minimum_amount, 'minimum amount', false), maximum: toUnits(instrument.maximum_amount, 'maximum amount', false) };
    bounds.minimum = ceil(bounds.minimum, bounds.step) * bounds.step;
    if (bounds.minimum > bounds.maximum) throw new Error('Invalid DRV quantity bounds');
    return bounds;
  }
  function freshTicker(ticker, bounds) {
    if (!Number.isSafeInteger(ticker?.t) || ticker.t > now() || now() - ticker.t > POLICY.maxAgeMs) throw new Error('DRV ticker is stale');
    const ask = toUnits(ticker.a, 'DRV ask', false), depth = toUnits(ticker.A, 'DRV ask depth', false);
    const price = ask * BigInt(10000 + POLICY.drvSlippageBps) / 10000n / bounds.tick * bounds.tick;
    if (price < ask || depth < bounds.minimum) throw new Error('DRV ask or minimum depth is unavailable');
    return { ask, depth, price };
  }
  function unsignedOrder(terms) {
    if (Number(terms?.subaccount_id) !== destinationId || terms.instrument_name !== 'DRV-USDC' || terms.direction !== 'buy'
      || terms.time_in_force !== 'ioc' || (terms.order_type != null && terms.order_type !== 'limit')
      || (terms.reduce_only != null && terms.reduce_only !== false)) throw new Error('Only the configured DRV IOC buy is allowed');
    if (toUnits(terms.max_fee, 'order fee cap') % FEE_QUANTUM !== 0n) throw new Error('Order fee cap exceeds venue precision');
    return { subaccount_id: destinationId, instrument_name: 'DRV-USDC', direction: 'buy',
      amount: fromUnits(toUnits(terms.amount, 'order amount', false)), limit_price: fromUnits(toUnits(terms.limit_price, 'order price', false)),
      max_fee: fromUnits(toUnits(terms.max_fee, 'order fee cap')), order_type: 'limit', time_in_force: 'ioc', reduce_only: false,
      signature_expiry_sec: terms.signature_expiry_sec, signer: wallet.address, nonce: '0', signature: '0x' };
  }
  async function readSnapshot({ cycleBudget, pendingActions } = {}) {
    if (!Array.isArray(pendingActions)) throw new Error('Current pending actions are required');
    const budget = fromUnits(money(cycleBudget, 'put cycle budget'));
    if (toUnits(budget) === 0n) throw new Error('Positive established put-cycle budget required');
    inputs = { cycleBudget: budget, pendingActions: structuredClone(pendingActions) };
    const [instrument, ticker, session, accountInfo] = await Promise.all([
      read('public/get_instrument', { instrument_name: 'DRV-USDC' }), read('public/get_ticker', { instrument_name: 'DRV-USDC' }),
      read('private/session_keys', { wallet: owner }), read('private/get_account', { wallet: owner })]);
    const key = validateSession(session), bounds = instrumentBounds(instrument), market = freshTicker(ticker, bounds);
    if (!accountInfo?.fee_info || accountInfo.fee_info.spot_taker_fee == null) throw new Error('Account spot fees are unverified');
    toUnits(accountInfo.fee_info.spot_taker_fee, 'spot taker fee');
    const [source, destination] = await Promise.all([account(sourceId, 1, pendingActions), account(destinationId, Number(instrument.risk_universe_id), [])]);
    source.cycle_budget_usdc = budget;
    const expiry = Math.min(Math.ceil(now() / 1000) + 900, key.expiry_sec - POLICY.keyMarginSec);
    const feeRequest = unsignedOrder({ subaccount_id: destinationId, instrument_name: 'DRV-USDC', direction: 'buy',
      amount: fromUnits(bounds.minimum), limit_price: fromUnits(market.price), max_fee: '0', time_in_force: 'ioc', signature_expiry_sec: expiry });
    const quote = await read('private/order_quote', feeRequest);
    const fee = (toUnits(quote?.suggested_max_fee, 'suggested per-unit max fee') * BigInt(10000 + POLICY.drvFeeHeadroomBps) / 10000n / FEE_QUANTUM + 1n) * FEE_QUANTUM;
    return { source, destination, instruments: { DRV: instrument }, drvTicker: { ...ticker, received_at: now() },
      fees: { DRV: { max_fee_per_unit: fromUnits(fee) } }, keyExpirySec: key.expiry_sec, transfer: { max_fee_usdc: '0' },
      session: key, fee_info: accountInfo.fee_info };
  }
  async function freshSnapshot() {
    if (!inputs) throw new Error('readSnapshot with current cycle budget and pending actions is required');
    return readSnapshot(inputs);
  }
  function expiry(expirySec, keyExpiry) {
    if (!Number.isSafeInteger(expirySec) || expirySec < Math.ceil(now() / 1000) + POLICY.minSignatureHorizonSec
      || expirySec > Math.ceil(now() / 1000) + 900 || expirySec > keyExpiry - POLICY.keyMarginSec) throw new Error('Action expiry is outside the current session horizon');
  }
  async function previewOrder(terms) {
    const request = unsignedOrder(terms), snapshot = await freshSnapshot();
    expiry(request.signature_expiry_sec, snapshot.keyExpirySec);
    const bounds = instrumentBounds(snapshot.instruments.DRV), market = freshTicker(snapshot.drvTicker, bounds);
    const amount = toUnits(request.amount), price = toUnits(request.limit_price), debit = cost(request.amount, request.limit_price, request.max_fee);
    if (amount < bounds.minimum || amount > bounds.maximum || amount > market.depth || amount % bounds.step !== 0n
      || price % bounds.tick !== 0n || price < market.ask || price > market.price) throw new Error('Order violates fresh DRV depth, quantity or price bounds');
    if (debit > toUnits(snapshot.destination.usdc_balance) - toUnits(snapshot.destination.committed_usdc)) throw new Error('Order exceeds uncommitted destination USDC');
    const [quote, margin] = await Promise.all([read('private/order_quote', request), read('private/get_margin', {
      subaccount_id: destinationId, simulated_collateral_changes: [{ asset_name: 'DRV', amount: request.amount }, { asset_name: 'USDC', amount: `-${fromUnits(debit)}` }] })]);
    // The same refusal as before, now naming which condition failed.
    const failed = [quote?.is_valid !== true && 'quote_invalid', !positiveMargin(quote?.post_initial_margin) && 'quote_margin',
      toUnits(quote?.suggested_max_fee, 'exact suggested fee') >= toUnits(request.max_fee) && 'fee_above_cap',
      margin?.is_valid_trade !== true && 'margin_invalid_trade', !positiveMargin(margin?.post_initial_margin) && 'post_initial_margin',
      !positiveMargin(margin?.post_maintenance_margin) && 'post_maintenance_margin'].filter(Boolean);
    if (failed.length) throw new Error(`Exact order fees or post-trade margin are unverified (${failed.join(', ')})`);
    return { valid: true, request, quote, margin, snapshot };
  }
  async function prepareOrder(terms) {
    await guard();
    const preview = await previewOrder(terms);
    await guard();
    const request = { ...preview.request, nonce: orderNonce('v3', now()) };
    request.signature = signOrder({ config: verified, owner, order: request, assetAddress: preview.snapshot.instruments.DRV.base_asset_address,
      assetSubId: '0', signer: wallet });
    prepared.set(request.signature, JSON.stringify(request));
    return request;
  }
  function requireBeforeSend(callback) {
    if (typeof callback !== 'function' || callback.constructor?.name === 'AsyncFunction') throw new Error('A synchronous durable before-send journal callback is required');
  }
  async function submitOrder(request, beforeSendSync) {
    requireBeforeSend(beforeSendSync);
    await guard();
    if (prepared.get(request?.signature) !== JSON.stringify(request)) throw new Error('Order is not the exact freshly prepared request');
    await previewOrder(request);
    prepared.delete(request.signature); // A timeout is never permission to resend.
    return call('private/order', request, true, beforeSendSync);
  }
  function transferRequest(terms, keyExpiry) {
    if (Number(terms?.from_subaccount_id ?? terms?.subaccount_id) !== sourceId || Number(terms.to_subaccount_id) !== destinationId
      || terms.asset_name !== 'USDC' || terms.new_subaccount_manager !== 0 || (terms.sub_id != null && Number(terms.sub_id) !== 0)
      || toUnits(terms.max_fee_usdc ?? terms.max_fee_usd, 'existing account transfer fee') !== 0n) throw new Error('Only zero-fee USDC transfers between the configured existing accounts are allowed');
    const amount = toUnits(terms.amount, 'transfer amount', false);
    if (amount % (10n ** 12n) !== 0n) throw new Error('USDC transfer exceeds native precision');
    const expiration = terms.signature_expiry_sec ?? Math.min(Math.ceil(now() / 1000) + 900, keyExpiry - POLICY.keyMarginSec);
    expiry(expiration, keyExpiry);
    return { subaccount_id: sourceId, to_subaccount_id: destinationId, asset_name: 'USDC', sub_id: 0, new_subaccount_manager: 0,
      amount: fromUnits(amount), max_fee_usd: '0', signature_expiry_sec: expiration, signer: wallet.address };
  }
  async function previewTransfer(terms) {
    const snapshot = await freshSnapshot(), request = transferRequest(terms, snapshot.keyExpirySec);
    const remaining = toUnits(snapshot.source.usdc_balance) - toUnits(snapshot.source.committed_usdc) - toUnits(request.amount);
    if (remaining < BigInt(POLICY.floatCycles) * toUnits(snapshot.source.cycle_budget_usdc)) throw new Error('Transfer would spend source operating reserves');
    const margin = await read('private/get_margin', { subaccount_id: sourceId,
      simulated_collateral_changes: [{ asset_name: 'USDC', amount: `-${request.amount}` }] });
    if (margin?.is_valid_trade !== true || !positiveMargin(margin.post_initial_margin) || !positiveMargin(margin.post_maintenance_margin)) throw new Error('Source post-transfer margins are unverified');
    const { resolveUsdcTransferAsset } = require('./derive-transfer');
    const asset = resolveUsdcTransferAsset(await read('public/get_all_currencies', {}));
    return { valid: true, request, margin, asset };
  }
  async function prepareTransfer(terms) {
    await guard();
    const preview = await previewTransfer(terms);
    await guard();
    const { prepareTransfer: signTransfer } = require('./derive-transfer');
    const request = signTransfer({ config: verified, owner, destinationSubaccountId: destinationId, asset: preview.asset, signer: wallet, nowMs: now(),
      transfer: { ...preview.request, nonce: orderNonce('v3', now()) } });
    prepared.set(request.signature, JSON.stringify(request));
    return request;
  }
  async function submitTransfer(request, beforeSendSync) {
    requireBeforeSend(beforeSendSync);
    await guard();
    if (prepared.get(request?.signature) !== JSON.stringify(request)) throw new Error('Transfer is not the exact freshly prepared request');
    await previewTransfer(request);
    prepared.delete(request.signature);
    return call('private/transfer_spot', request, true, beforeSendSync);
  }
  async function collectOrder(request) {
    if (Number(request?.subaccount_id) !== destinationId || request.instrument_name !== 'DRV-USDC' || request.direction !== 'buy') throw new Error('Recovery requires the configured DRV destination');
    const instrument = await read('public/get_instrument', { instrument_name: 'DRV-USDC' });
    instrumentBounds(instrument);
    const evidence = await collectRecoveryEvidence({ request, read: (method, params) => read(`private/${method}`, params) });
    // V3 trade history omits fee_currency. Derive's trade_fee contract is quote
    // denominated; resolve that unit only for this explicitly verified DRV/USDC
    // instrument, retaining the original receipt and the inference provenance.
    // https://docs.derive.xyz/api-reference/orderbook/privateorder
    const trades = evidence.trades.map(trade => {
      if (trade.instrument_name !== instrument.instrument_name || Number(trade.subaccount_id) !== destinationId || trade.direction !== 'buy'
        || (Object.hasOwn(trade, 'fee_currency') && trade.fee_currency !== 'USDC')) throw new Error('DRV trade fee currency or account conflicts with verified quote units');
      return { ...trade, fee_currency: 'USDC', fee_currency_source: Object.hasOwn(trade, 'fee_currency')
        ? 'venue_trade' : 'verified_DRV-USDC_quote_currency_and_Derive_trade_fee_contract', raw_venue_trade: trade };
    });
    return { ...evidence, trades };
  }
  async function collectTransfer(request, ack) {
    // No matching by amount/time when a network timeout loses the operation UUID.
    const response = ack?.result || ack;
    if (typeof response?.op_uuid !== 'string' || !/^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i.test(response.op_uuid)) throw new Error('Transfer operation UUID is missing; reservation remains frozen');
    if (Number(request?.subaccount_id) !== sourceId || Number(request.to_subaccount_id) !== destinationId) throw new Error('Transfer recovery account mismatch');
    const operation = await read('public/get_transaction', { op_uuid: response.op_uuid });
    const end = now();
    async function history(id) {
      const result = await read('private/get_erc20_transfer_history', { subaccount_id: id, start_timestamp: Date.parse(config.historyFrom), end_timestamp: end });
      if (!Array.isArray(result?.transfers) || result.transfers.length >= 1000) throw new Error('Transfer history coverage is incomplete');
      return result.transfers;
    }
    const [source, destination] = await Promise.all([history(sourceId), history(destinationId)]);
    return { operation: { ...operation, op_uuid: response.op_uuid }, history: { sourceAccountId: sourceId, destinationAccountId: destinationId, source, destination } };
  }
  return { readSnapshot, previewOrder, prepareOrder, submitOrder, collectOrder, previewTransfer, prepareTransfer, submitTransfer, collectTransfer };
}

module.exports = { createSpotVenue, sourceCashCommitments };
