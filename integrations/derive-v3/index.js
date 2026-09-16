'use strict';

const { randomInt } = require('node:crypto');
const { readProfile, loadKey } = require('./profile');

const READ_METHODS = new Set([
  'public/get_risk_universes', 'public/get_all_instruments', 'public/get_all_live_instruments',
  'public/get_tickers', 'public/get_ticker', 'public/get_instrument', 'public/get_all_currencies',
  'public/get_margin', 'private/get_margin', 'private/order_debug',
  'private/get_subaccounts', 'private/get_subaccount', 'private/get_positions',
  'private/get_collaterals', 'private/get_open_orders', 'private/get_trigger_orders', 'private/get_algo_orders', 'private/get_order',
  'private/get_order_history', 'private/get_trade_history', 'private/get_option_settlement_history',
  'private/get_interest_history', 'private/get_funding_history', 'private/get_deposit_history',
  'private/get_withdrawal_history', 'private/session_keys',
]);

// Explicit pre-book rejections from https://docs.derive.xyz/error-codes.
// 9000/9001 are confirmation timeouts after acceptance; duplicate-nonce,
// internal, unknown and protocol-layer failures also require reconciliation.
const DEFINITIVE_ORDER_REJECTION_CODES = new Set([
  -32700, -32600, -32601, -32602, -32000, 401, 403,
  10004, 10007, 10010, 10015, 10016,
  11000, 11007, 11008, 11009, 11011, 11012, 11013, 11014, 11015,
  11018, 11019, 11020, 11021, 11022, 11023, 11024, 11025, 11027, 11029,
  12001, 12002, 12003,
  14000, 14001, 14013, 14014, 14020, 14021, 14023, 14026, 14030, 14031, 14033,
  16001, 16002, 17000, 17001, 17002, 17003,
]);

function validateOrderReceipt(result, params) {
  const { parseUnits } = require('ethers');
  const order = result?.order;
  const decimal = (value, field) => {
    if (typeof value !== 'string' || !/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(value)) throw new Error(`Malformed order acknowledgement: ${field}`);
    try { return parseUnits(value, 18); } catch { throw new Error(`Malformed order acknowledgement: ${field}`); }
  };
  if (typeof order?.order_id !== 'string' || !order.order_id || !Array.isArray(result.trades)) {
    throw new Error('Malformed order acknowledgement; reconcile the submitted nonce');
  }
  if (order.nonce !== params.nonce || order.subaccount_id !== params.subaccountId
    || order.instrument_name !== params.instrumentName || order.direction !== params.direction
    || order.time_in_force !== params.timeInForce) throw new Error('Order acknowledgement identity mismatch');
  const amount = decimal(order.amount, 'amount');
  const filled = decimal(order.filled_amount, 'filled_amount');
  if (amount !== parseUnits(String(params.amount), 18) || filled < 0n || filled > amount
    || decimal(order.average_price, 'average_price') < 0n
    || decimal(order.limit_price, 'limit_price') !== parseUnits(String(params.limitPrice), 18)) {
    throw new Error('Order acknowledgement quantity or price mismatch');
  }
  const status = order.order_status;
  if (!['open', 'filled', 'rejected', 'cancelled', 'expired'].includes(status)
    || (status === 'filled' && filled !== amount)
    || (status === 'rejected' && filled !== 0n)
    || (['ioc', 'fok'].includes(params.timeInForce) && status === 'open')) {
    throw new Error('Order acknowledgement has an unexpected status');
  }
  let tradeAmount = 0n;
  for (const trade of result.trades) {
    const quantity = decimal(trade.trade_amount, 'trade_amount');
    if (quantity <= 0n || decimal(trade.trade_price, 'trade_price') < 0n
      || trade.order_id !== order.order_id || trade.subaccount_id !== params.subaccountId
      || trade.instrument_name !== params.instrumentName || trade.direction !== params.direction) {
      throw new Error('Order acknowledgement trade mismatch');
    }
    tradeAmount += quantity;
  }
  if (tradeAmount !== filled) throw new Error('Order acknowledgement fills do not match its trades');
  return order;
}

let lastNonce = 0n;
function nextNonce(now = Date.now()) {
  const candidate = BigInt(now) * 1000000n + BigInt(randomInt(1000000));
  lastNonce = candidate > lastNonce ? candidate : lastNonce + 1n;
  return lastNonce.toString();
}

function requireAccount(profile) {
  if (!profile.ownerAddress || !profile.subaccountId) throw new Error('V3 owner address and subaccount ID are required');
}

function validatePortfolio(value, profile) {
  if (!value || value.failed_to_fetch !== false || typeof value.is_under_liquidation !== 'boolean'
    || !Array.isArray(value.positions) || !Array.isArray(value.collaterals)) {
    throw new Error('V3 portfolio is unavailable or malformed; execution must stop');
  }
  if (value.subaccount_id !== profile.subaccountId) throw new Error('V3 portfolio subaccount mismatch');
  for (const key of ['initial_margin', 'maintenance_margin', 'subaccount_value']) {
    if (typeof value[key] !== 'string' || !/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(value[key]) || !Number.isFinite(Number(value[key]))) {
      throw new Error(`V3 portfolio is missing ${key}`);
    }
  }
  if (!Number.isSafeInteger(value.manager_id) || value.manager_id < 0
    || !Number.isSafeInteger(value.risk_universe_id) || value.risk_universe_id <= 0) {
    throw new Error('V3 portfolio lacks a trading manager/risk universe');
  }
  for (const [field, identity] of [['positions', 'instrument_name'], ['collaterals', 'asset_name']]) {
    for (const row of value[field]) {
      if (typeof row?.[identity] !== 'string' || !row[identity]) throw new Error(`V3 portfolio has a malformed ${field} row`);
      for (const key of ['amount', 'initial_margin', 'maintenance_margin']) {
        if (typeof row[key] !== 'string' || !/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(row[key]) || !Number.isFinite(Number(row[key]))) {
          throw new Error(`V3 portfolio ${field} row is missing ${key}`);
        }
      }
    }
  }
  return value;
}

class DeriveV3 {
  constructor({ profile = readProfile(), authenticated = false, client, journal = () => {} } = {}) {
    if (profile.version !== 3) throw new Error('DeriveV3 requires an explicit V3 profile');
    this.profile = profile;
    this.journal = journal;
    this.pendingOrder = null;
    this.acknowledgedOrder = null;
    this.placing = false;
    if (client) {
      this.client = client; // Test injection; production always uses the pinned official SDK.
    } else {
      const { DeriveClient } = require('@derivexyz/derive-ts');
      const options = { network: require('./deployment').sdkNetwork(profile.network), requestTimeoutMs: 15000 };
      if (authenticated) {
        requireAccount(profile);
        options.sessionKey = loadKey(profile);
        options.ownerAddress = profile.ownerAddress;
      }
      this.client = new DeriveClient(options);
      if (this.client.network.httpUrl.replace(/\/$/, '') !== profile.httpUrl) throw new Error('Unexpected SDK host');
    }
  }

  async read(method, params = {}) {
    if (!READ_METHODS.has(method)) throw new Error(`Not an allowed read method: ${method}`);
    if (method.startsWith('private/')) {
      requireAccount(this.profile);
      if (params.subaccount_id != null && params.subaccount_id !== this.profile.subaccountId) throw new Error('Subaccount mismatch');
      if (params.wallet && params.wallet.toLowerCase() !== this.profile.ownerAddress.toLowerCase()) throw new Error('Owner mismatch');
    }
    return this.client.send(method, params);
  }

  async pages(method, params, field, { maxPages = 100, pageSize = 100 } = {}) {
    const rows = [];
    let previous = null;
    for (let page = 1; page <= maxPages; page++) {
      const result = await this.read(method, { ...params, page, page_size: pageSize });
      const batch = result?.[field];
      if (!Array.isArray(batch)) throw new Error(`${method}: missing ${field} array`);
      const fingerprint = JSON.stringify(batch);
      if (batch.length && fingerprint === previous) throw new Error(`${method}: pagination did not advance`);
      previous = fingerprint;
      rows.push(...batch);
      const pagination = result.pagination;
      if (pagination?.num_pages != null) {
        if (!Number.isSafeInteger(pagination.num_pages) || pagination.num_pages < 0) throw new Error('Invalid pagination metadata');
        if (page >= pagination.num_pages) return rows;
        if (!batch.length) throw new Error(`${method}: empty page before end of history`);
      } else if (batch.length < pageSize) return rows;
    }
    throw new Error(`${method}: pagination limit reached; refusing incomplete history`);
  }

  instruments(params = {}) {
    return this.pages('public/get_all_instruments', { currency: 'ETH', instrument_type: 'option', expired: false, ...params }, 'instruments');
  }

  history(method, params = {}, field = 'trades') {
    return this.pages(method, { ...params, subaccount_id: this.profile.subaccountId }, field);
  }

  async orderStatus(orderId) {
    try {
      const result = await this.read('private/get_order', { subaccount_id: this.profile.subaccountId, order_id: orderId });
      return result.order || result;
    } catch (error) {
      if (error.code !== 11006) throw error;
      const history = await this.history('private/get_order_history', {}, 'orders');
      return history.find(order => order.order_id === orderId) || null;
    }
  }

  async account() {
    requireAccount(this.profile);
    const owned = await this.read('private/get_subaccounts', { wallet: this.profile.ownerAddress });
    if (!owned?.subaccount_ids?.includes(this.profile.subaccountId)) throw new Error('Configured V3 subaccount is not accessible to this identity');
    return validatePortfolio(await this.read('private/get_subaccount', { subaccount_id: this.profile.subaccountId }), this.profile);
  }

  assertExecution() {
    requireAccount(this.profile);
    if (!this.profile.executionEnabled) throw new Error(`${this.profile.prefix}EXECUTION is disabled`);
    if (this.pendingOrder) throw new Error(`Unresolved order ${this.pendingOrder}: reconcile before submitting another order`);
    if (this.placing) throw new Error('A V3 placement is already in progress');
  }

  async place(params) {
    this.assertExecution();
    this.placing = true;
    try {
      return await this.placeOnce(params);
    } finally { this.placing = false; }
  }

  async placeOnce(params) {
    if (params.subaccountId !== this.profile.subaccountId) throw new Error('Order subaccount mismatch');
    if (!['buy', 'sell'].includes(params.direction) || !['ioc', 'gtc', 'post_only', 'fok'].includes(params.timeInForce)) {
      throw new Error('Explicit valid direction and timeInForce required');
    }
    if (typeof params.reduceOnly !== 'boolean' || params.maxFee == null) throw new Error('Explicit reduceOnly and maxFee required');
    const account = await this.account();
    if (account.is_under_liquidation) throw new Error('V3 account is under liquidation');
    if (!params.reduceOnly && Number(account.initial_margin) <= 0) throw new Error('No V3 initial margin available');
    const nonce = nextNonce();
    const order = { ...params, nonce, mmp: false,
      signatureExpirySec: params.signatureExpirySec ?? Math.floor(Date.now() / 1000) + 900 };
    // The SDK validates decimals, fetches instrument metadata and signs before
    // sending. Finish those fallible read/local steps before recording a send.
    let send;
    if (this.client.marketData && typeof this.client.credentials === 'function') {
      const { OrdersApi } = require('@derivexyz/derive-ts');
      const builder = new OrdersApi({
        network: this.client.network,
        credentials: () => this.client.credentials(),
        send: async (method, wire) => {
          if (method !== 'private/order') throw new Error('Unexpected order preparation route');
          return wire;
        },
      }, this.client.marketData);
      const wire = await builder.place(order);
      send = () => this.client.send('private/order', wire);
    } else {
      // Lightweight injected clients implement orders.place as the send itself.
      send = () => this.client.orders.place(order);
    }
    // Persist intent BEFORE send. Never automatically retry an uncertain state-changing request.
    this.journal({ event: 'order_intent', nonce, instrument: params.instrumentName, direction: params.direction,
      amount: String(params.amount), limit_price: String(params.limitPrice), max_fee: String(params.maxFee), reduce_only: params.reduceOnly });
    this.pendingOrder = nonce;
    try {
      const result = await send();
      const acknowledgedOrder = validateOrderReceipt(result, order);
      this.journal({ event: 'order_ack', nonce, result });
      this.acknowledgedOrder = { nonce, orderId: acknowledgedOrder.order_id };
      return result;
    } catch (error) {
      // A received RPC rejection is definitive; transport failure leaves the intent unresolved.
      const definitive = DEFINITIVE_ORDER_REJECTION_CODES.has(error.code);
      this.journal({ event: definitive ? 'order_rejected' : 'order_unknown', nonce, code: error.code ?? null });
      if (definitive) this.pendingOrder = null;
      throw error;
    }
  }

  markAccounted(orderId = null) {
    if (!this.pendingOrder) return false;
    if (!this.acknowledgedOrder || this.acknowledgedOrder.nonce !== this.pendingOrder) {
      throw new Error('Cannot mark an unknown V3 placement accounted; reconcile it first');
    }
    if (orderId != null && orderId !== this.acknowledgedOrder.orderId) throw new Error('Accounting order ID mismatch');
    this.journal({ event: 'order_accounted', nonce: this.pendingOrder, order_id: this.acknowledgedOrder.orderId });
    this.pendingOrder = null;
    this.acknowledgedOrder = null;
    return true;
  }

  async cancel(orderId, instrumentName) {
    requireAccount(this.profile);
    if (!this.profile.executionEnabled) throw new Error(`${this.profile.prefix}EXECUTION is disabled`);
    // Cancellation stays available while an earlier placement is unresolved.
    const result = await this.client.orders.cancel({ subaccountId: this.profile.subaccountId, orderId, instrumentName });
    this.journal({ event: 'cancel_ack', order_id: orderId, result });
    return result;
  }

  async close() { await this.client.close?.(); }
}

module.exports = { DeriveV3, nextNonce, validatePortfolio, validateOrderReceipt,
  isDefinitiveOrderRejection: code => DEFINITIVE_ORDER_REJECTION_CODES.has(code) };
