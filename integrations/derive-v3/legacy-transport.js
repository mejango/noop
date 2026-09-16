'use strict';

// Compatibility seam for the existing strategy. This only loads on an explicit V3 profile.
const { DeriveV3, validatePortfolio } = require('./index');
const { openJournal } = require('./state');

function validateAccountRows(method, result, profile) {
  const field = {
    'private/get_positions': 'positions',
    'private/get_collaterals': 'collaterals',
    'private/get_open_orders': 'orders',
  }[method];
  if (!field) return result;
  if (!result || result.failed_to_fetch || result.subaccount_id !== profile.subaccountId || !Array.isArray(result[field])) {
    throw new Error(`${method}: unavailable, malformed, or wrong-subaccount V3 response`);
  }
  const decimal = (value, name) => {
    if (typeof value !== 'string' || !/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(value) || !Number.isFinite(Number(value))) {
      throw new Error(`${method}: missing or invalid ${name}`);
    }
  };
  const text = (value, name) => {
    if (typeof value !== 'string' || !value) throw new Error(`${method}: missing ${name}`);
  };
  const rows = result[field].map(row => {
    if (!row || typeof row !== 'object') throw new Error(`${method}: malformed row`);
    decimal(row.amount, 'amount');
    if (field === 'orders') {
      text(row.order_id, 'order_id');
      text(row.instrument_name, 'instrument_name');
      if (row.subaccount_id !== profile.subaccountId || !['buy', 'sell'].includes(row.direction)
        || row.order_status !== 'open') throw new Error(`${method}: invalid order identity or status`);
      decimal(row.filled_amount, 'filled_amount');
      decimal(row.limit_price, 'limit_price');
      if (Number(row.amount) <= 0 || Number(row.filled_amount) < 0 || Number(row.filled_amount) > Number(row.amount)) {
        throw new Error(`${method}: invalid order quantities`);
      }
      return row;
    }
    text(row[field === 'positions' ? 'instrument_name' : 'asset_name'], 'asset/instrument name');
    decimal(row.mark_price, 'mark_price');
    if (field === 'collaterals') {
      decimal(row.mark_value, 'mark_value');
      return row;
    }
    for (const key of ['average_price', 'index_price', 'unrealized_pnl', 'delta', 'theta', 'vega']) decimal(row[key], key);
    // V3 exposes Greeks directly on PositionResponse; the existing strategy
    // consumes the V2 greeks object.
    return { ...row, greeks: { delta: row.delta, theta: row.theta, vega: row.vega } };
  });
  return { ...result, [field]: rows };
}

function createLegacyTransport(profile) {
  const journal = openJournal(profile);
  const adapter = new DeriveV3({ profile, authenticated: true, journal: journal.append });
  adapter.pendingOrder = journal.unresolved[0] || null;

  async function post(url, body = {}) {
    const parsed = new URL(url);
    if (parsed.origin !== 'https://api.lyra.finance') throw new Error('Unexpected legacy venue URL');
    const method = parsed.pathname.slice(1);
    // Ignore old auth entirely: the SDK authenticates only to the fixed V3 host.
    const params = { ...body };
    if (params.subaccount_id != null) params.subaccount_id = profile.subaccountId;
    let result;
    try {
      if (method === 'public/get_instruments') {
        result = await adapter.instruments(params);
      } else if (method === 'private/cancel') {
        result = await adapter.cancel(params.order_id, params.instrument_name);
      } else if (method === 'private/get_trade_history') {
        result = { trades: await adapter.history(method, params) };
      } else if (method === 'private/get_order_history') {
        result = { orders: await adapter.history(method, params, 'orders') };
      } else {
        result = await adapter.read(method, params);
      }
      result = validateAccountRows(method, result, profile);
      if (method === 'private/get_subaccount') validatePortfolio(result, profile);
      if (method === 'public/get_tickers' && params.instrument_type === 'perp') {
        if (!result?.tickers || typeof result.tickers !== 'object') throw new Error('Malformed V3 perp tickers');
        result = Object.entries(result.tickers).map(([name, ticker]) => ({
          ...ticker, instrument_name: name,
          funding_rate_info: ticker.f == null ? null : { funding_rate: ticker.f },
        }));
      }
      return { data: { result } };
    } catch (error) {
      if (typeof error.code === 'number') error.response = { data: { error: { code: error.code, message: error.message } } };
      throw error;
    }
  }

  async function placeOrder(order) {
    try {
      const result = await adapter.place({
        subaccountId: profile.subaccountId, instrumentName: order.instrument_name,
        direction: order.direction, amount: String(order.amount), limitPrice: String(order.limit_price),
        maxFee: String(order.max_fee), orderType: 'limit', timeInForce: order.time_in_force,
        reduceOnly: order.reduce_only, rejectPostOnly: order.time_in_force === 'post_only',
        signatureExpirySec: order.signature_expiry_sec,
      });
      return { data: { result } };
    } catch (error) {
      if (typeof error.code === 'number') error.response = { data: { error: { code: error.code, message: error.message } } };
      throw error;
    }
  }
  return { post, placeOrder, adapter, acknowledgeAccounting: orderId => adapter.markAccounted(orderId) };
}

module.exports = { createLegacyTransport, validateAccountRows };
