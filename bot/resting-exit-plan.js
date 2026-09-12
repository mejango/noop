'use strict';

const { getInstrumentPriceStep, normalizeOrderPriceForVenue, avoidRoundNumberRestingPrice,
  computePostOnlyRetryPrice } = require('./order-pricing');

const positive = (value) => Number.isFinite(Number(value)) && Number(value) > 0;
const amountAtVenuePrecision = value => Math.floor((Number(value) + 1e-9) * 100) / 100;

function resolveDesiredExitOrderType({ action, intent, preferredOrderType, price, ticker }) {
  const patient = (action === 'buyback_call' && intent === 'profit_capture')
    || (action === 'sell_put' && intent === 'monetize_tail_win');
  if (!patient) return 'ioc';
  const preferred = ['ioc', 'gtc', 'post_only'].includes(preferredOrderType) ? preferredOrderType : 'post_only';
  const patientLimit = action === 'buyback_call'
    ? positive(ticker?.a) && Number(price) < Number(ticker.a)
    : positive(ticker?.b) && Number(price) > Number(ticker.b);
  return preferred === 'ioc' && patientLimit ? 'post_only' : preferred;
}

function getDesiredSellPutRemainingAmount({ positionAmount, desiredFraction, existingOrder = null }) {
  if (!positive(positionAmount) || !positive(desiredFraction) || Number(desiredFraction) > 1) {
    throw new Error('Desired put tranche is incomplete');
  }
  let quantity = Number(positionAmount) * Number(desiredFraction);
  if (existingOrder?.exit_intent === 'monetize_tail_win') {
    const remaining = Number(existingOrder.amount) - Number(existingOrder.filled_amount ?? 0);
    if (!Number.isFinite(remaining) || remaining < 0) throw new Error('Existing put tranche is unknown');
    const originalFraction = Number(existingOrder.tranche_fraction);
    // Repricing or renewing the same tranche does not authorize selling another
    // fraction of the now-smaller hedge. Only an explicit increase of a known
    // policy fraction can enlarge the original approved remainder.
    if (!positive(originalFraction) || Number(desiredFraction) <= originalFraction + 1e-9) {
      quantity = Math.min(quantity, remaining);
    }
  }
  return amountAtVenuePrecision(quantity);
}

function normalizeDesiredExitOrder({ action, instrumentName, amount, price, orderType, intent,
  instrument, ticker, existingOrder = null, priceReason = null, ceilingPrice = null }) {
  const direction = action === 'buyback_call' ? 'buy' : action === 'sell_put' ? 'sell' : null;
  if (!direction || !instrumentName || !positive(amount) || !positive(price)
    || !['ioc', 'gtc', 'post_only'].includes(orderType) || !intent) {
    throw new Error('Desired exit terms are incomplete');
  }
  let limit = Number(price);
  const ownPrice = Number(existingOrder?.limit_price);
  // Our standing bid can itself be the BBO. Do not repeatedly outbid ourselves
  // just because the patient planner normally improves the visible bid by a tick.
  if (direction === 'buy' && priceReason === 'one_tick_above_best_bid'
    && existingOrder?.instrument_name === instrumentName && existingOrder?.direction === direction
    && existingOrder?.exit_intent === intent && positive(ownPrice)
    && Math.abs(ownPrice - Number(ticker?.b)) < 1e-9
    && ownPrice <= Number(ceilingPrice) + 1e-9 && ownPrice <= limit + 1e-9) {
    limit = ownPrice;
  }
  if (orderType === 'post_only') {
    const step = getInstrumentPriceStep(instrument, limit);
    const offRoundPrice = avoidRoundNumberRestingPrice(direction, limit, step);
    const retry = computePostOnlyRetryPrice(direction, ticker, instrument, limit);
    const crosses = direction === 'buy'
      ? positive(ticker?.a) && limit >= Number(ticker.a)
      : positive(ticker?.b) && limit <= Number(ticker.b);
    limit = crosses && retry && Math.abs(retry.retryPrice - limit) > 1e-9 ? retry.retryPrice : offRoundPrice;
  }
  limit = normalizeOrderPriceForVenue(limit, instrument, direction).price;
  const quantity = amountAtVenuePrecision(amount);
  if (!(limit > 0) || !(quantity > 0)) throw new Error('Desired exit is below venue precision');
  return { action, instrument_name: instrumentName, direction, amount: quantity,
    limit_price: limit, order_type: orderType, exit_intent: intent };
}

function compareRestingExitOrder(existing, desired) {
  if (!existing || !desired) return { decision: 'unresolved', differences: ['missing_order'] };
  const orderType = existing.time_in_force || existing.order_type;
  const remaining = Number(existing.amount) - Number(existing.filled_amount ?? 0);
  const action = existing.action || (existing.direction === 'buy' && existing.instrument_name?.endsWith('-C')
    ? 'buyback_call' : existing.direction === 'sell' && existing.instrument_name?.endsWith('-P') ? 'sell_put' : null);
  if (!existing.order_id || !existing.instrument_name || !['buy', 'sell'].includes(existing.direction)
    || !positive(existing.limit_price) || !positive(remaining)
    || !['gtc', 'post_only'].includes(orderType) || !existing.exit_intent || !action) {
    return { decision: 'unresolved', differences: ['incomplete_existing_terms'] };
  }
  const differences = [];
  for (const [field, oldValue, newValue] of [
    ['action', action, desired.action], ['instrument', existing.instrument_name, desired.instrument_name],
    ['direction', existing.direction, desired.direction], ['intent', existing.exit_intent, desired.exit_intent],
    ['order_type', orderType, desired.order_type],
  ]) if (oldValue !== newValue) differences.push(field);
  if (Math.abs(Number(existing.limit_price) - desired.limit_price) > 1e-9) differences.push('price');
  if (Math.abs(amountAtVenuePrecision(remaining) - desired.amount) > 1e-9) differences.push('amount');
  return { decision: differences.length ? 'replace' : 'keep', differences };
}

module.exports = { normalizeDesiredExitOrder, compareRestingExitOrder, resolveDesiredExitOrderType, getDesiredSellPutRemainingAmount };
