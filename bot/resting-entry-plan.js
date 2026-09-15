'use strict';

const facts = require('./strategy-facts.json');
const { getBuyPutPriceForEdgeScore, normalizeBuyPutScore } = require('./put-score');
const { normalizeSellCallScore } = require('./call-score');
const { liveRuleValues, evaluateConditions } = require('./trade-policy');
const { getInstrumentPriceStep, normalizeOrderPriceForVenue, avoidRoundNumberRestingPrice,
  computePostOnlyRetryPrice } = require('./order-pricing');

const EPS = 1e-9;
const finite = value => (typeof value === 'number' || typeof value === 'string' && value.trim() !== '')
  && Number.isFinite(Number(value));
const positive = value => finite(value) && Number(value) > 0;
const route = order => order?.time_in_force || order?.order_type;
const remaining = order => Number(order?.amount) - Number(order?.filled_amount ?? 0);
const knownRemainder = order => positive(order?.amount) && finite(order?.filled_amount ?? 0)
  && Number(order?.filled_amount ?? 0) >= 0 && remaining(order) > 0;
const directionFor = action => action === 'buy_put' ? 'buy' : action === 'sell_call' ? 'sell' : null;
const amountAtVenuePrecision = (amount, instrument) => {
  const step = Math.max(Number(instrument?.options?.amount_step ?? instrument?.amount_step) || 0.01, 0.01);
  const stepped = Math.floor((Number(amount) + EPS) / step) * step;
  return Math.floor((stepped + EPS) * 100) / 100;
};

function normalizeDesiredEntryOrder({ action, instrumentName, amount, price, orderType, instrument, ticker, existingOrder }) {
  const direction = directionFor(action);
  if (!direction || !instrumentName || instrument?.instrument_name !== instrumentName
    || !positive(amount) || !positive(price) || !['ioc', 'gtc', 'post_only'].includes(orderType)) {
    throw new Error('Desired entry terms are incomplete');
  }
  let limit = Number(price);
  const step = getInstrumentPriceStep(instrument, limit);
  const oldPrice = Number(existingOrder?.limit_price);
  const ownSide = direction === 'buy' ? Number(ticker?.b) : Number(ticker?.a);
  const improvesOwnQuote = direction === 'buy' ? limit - oldPrice : oldPrice - limit;
  const existingStatus = String(existingOrder?.order_status || existingOrder?.status || '').toLowerCase();
  if (orderType !== 'ioc' && existingOrder?.instrument_name === instrumentName && existingOrder?.direction === direction
    && !['cancelled', 'filled', 'expired', 'rejected'].includes(existingStatus)
    && positive(oldPrice) && Math.abs(ownSide - oldPrice) <= EPS
    && improvesOwnQuote > EPS && improvesOwnQuote <= step + EPS) {
    limit = oldPrice;
  }
  limit = normalizeOrderPriceForVenue(limit, instrument, direction).price;
  if (orderType === 'post_only') {
    const opposite = direction === 'buy' ? ticker?.a : ticker?.b;
    if (!positive(opposite)) throw new Error('A current opposite-side quote is required for a maker entry');
    const crosses = direction === 'buy' ? limit >= Number(opposite) : limit <= Number(opposite);
    if (crosses) {
      const retry = computePostOnlyRetryPrice(direction, ticker, instrument, limit);
      if (!retry) throw new Error('No valid maker price within the proposed entry bound');
      limit = retry.retryPrice;
    } else {
      limit = avoidRoundNumberRestingPrice(direction, limit, step);
    }
  }
  const quantity = amountAtVenuePrecision(amount, instrument);
  if (!(limit > 0) || quantity < 0.1 - EPS) throw new Error('Desired entry is below venue price or amount minimum');
  return { action, instrument_name: instrumentName, direction, amount: quantity,
    limit_price: limit, order_type: orderType };
}

function compareRestingEntryOrder(existing, desired) {
  const oldRoute = route(existing);
  const oldAmount = remaining(existing);
  const action = existing?.action || (existing?.direction === 'buy' && existing?.instrument_name?.endsWith('-P')
    ? 'buy_put' : existing?.direction === 'sell' && existing?.instrument_name?.endsWith('-C') ? 'sell_call' : null);
  const existingStatus = String(existing?.order_status || existing?.status || '').toLowerCase();
  if (!existing?.order_id || !desired || !positive(existing.limit_price) || !knownRemainder(existing)
    || ['cancelled', 'filled', 'expired', 'rejected'].includes(existingStatus)
    || !['gtc', 'post_only'].includes(oldRoute) || !directionFor(action)) {
    return { decision: 'unresolved', differences: ['incomplete_existing_terms'] };
  }
  const differences = [];
  for (const [field, before, after] of [
    ['action', action, desired.action], ['instrument', existing.instrument_name, desired.instrument_name],
    ['direction', existing.direction, desired.direction], ['order_type', oldRoute, desired.order_type],
  ]) if (before !== after) differences.push(field);
  if (Math.abs(Number(existing.limit_price) - desired.limit_price) > EPS) differences.push('price');
  if (Math.abs(oldAmount - desired.amount) > EPS) differences.push('amount');
  return { decision: differences.length ? 'replace' : 'keep', differences };
}

function buildRestingEntryPlan({ order, rule, instrument, ticker, spotPrice, putBudgetRemaining,
  otherPutReserved = 0, pricing = null, effectiveTargetScore = null, nowMs = Date.now() }) {
  const unresolved = reason => ({ decision: 'unresolved', reason });
  const invalid = reason => ({ decision: 'invalid', reason });
  if (!rule || Number(rule.is_active) === 0 || rule.rule_type !== 'entry') return unresolved('No active entry rule is associated with this order');
  const action = rule.action;
  const direction = directionFor(action);
  const name = order?.instrument_name;
  if (!direction || order?.action && order.action !== action) return invalid('Order and rule action disagree');
  if (!name || instrument?.instrument_name !== name || !ticker || !positive(spotPrice)) return unresolved('Current instrument, quote or spot is unavailable');
  if (!order?.order_id || !knownRemainder(order) || !positive(order.limit_price)) return unresolved('Current order remainder or limit is unavailable');
  if (order.direction !== direction || !name.endsWith(action === 'buy_put' ? '-P' : '-C')) return invalid('Order direction or option type disagrees with entry action');
  let criteria = rule.criteria;
  try { if (typeof criteria === 'string') criteria = JSON.parse(criteria); } catch { return unresolved('Current rule criteria are malformed'); }
  if (!criteria || typeof criteria !== 'object' || Array.isArray(criteria)) return unresolved('Current rule criteria are unavailable');
  const values = liveRuleValues({ instrument_name: name, direction: direction === 'buy' ? 'long' : 'short' }, ticker, spotPrice, nowMs);
  if (!finite(values.delta) || !finite(values.dte)) return unresolved('Fresh delta or expiry is unavailable');
  const type = action === 'buy_put' ? 'P' : 'C';
  if (criteria.option_type && criteria.option_type !== type) return invalid('Rule option type disagrees with the instrument');
  const strategyDelta = action === 'buy_put' ? facts.put_delta_range : facts.call_delta_range;
  const strategyDte = action === 'buy_put' ? facts.put_dte_range : facts.call_dte_range;
  for (const [value, range, label] of [[values.delta, strategyDelta, 'strategy delta'], [values.dte, strategyDte, 'strategy DTE'],
    [values.delta, criteria.delta_range ?? strategyDelta, 'rule delta'], [values.dte, criteria.dte_range ?? strategyDte, 'rule DTE']]) {
    if (!Array.isArray(range) || range.length !== 2 || !range.every(finite) || Number(range[0]) > Number(range[1])) return unresolved(`Malformed ${label} range`);
    if (value < Number(range[0]) || value > Number(range[1])) return invalid(`Current ${label} is outside the approved range`);
  }
  const strike = Number(instrument.option_details?.strike ?? name.split('-')[2]);
  if (positive(criteria.max_strike_pct) && strike >= Number(criteria.max_strike_pct) * Number(spotPrice)) return invalid('Current strike/spot relationship violates the rule');
  if (criteria.market_conditions != null && (!Array.isArray(criteria.market_conditions)
    || !evaluateConditions(criteria.market_conditions, 'all', { spot_price: Number(spotPrice) }))) return invalid('Current market conditions do not satisfy the rule');
  const orderType = ['gtc', 'post_only'].includes(rule.preferred_order_type) ? rule.preferred_order_type : route(order);
  if (!['gtc', 'post_only'].includes(orderType)) return unresolved('Current resting execution route is unavailable');
  const oldRemaining = remaining(order);
  const originalRemainingNotional = oldRemaining * Number(order.limit_price);
  let price, requiredScore = null, priceBound = null;
  if (action === 'buy_put') {
    if (!positive(ticker.a) || !positive(criteria.min_score)) return unresolved('Fresh ask and approved PUT EDGE minimum are required');
    const target = effectiveTargetScore ?? criteria.target_score;
    requiredScore = Math.max(Number(criteria.min_score), positive(target) ? Number(target) : 0,
      positive(pricing?.requiredScore) ? Number(pricing.requiredScore) : 0);
    priceBound = getBuyPutPriceForEdgeScore(Math.abs(values.delta), requiredScore, values.dte);
    price = Math.min(Number(ticker.a), priceBound, positive(pricing?.limitPrice) ? Number(pricing.limitPrice) : Infinity);
    if (!finite(putBudgetRemaining) || !finite(otherPutReserved) || Number(otherPutReserved) < 0) return unresolved('Verified put budget and other reservations are required');
  } else {
    if (!positive(ticker.b)) return unresolved('Fresh executable call bid is unavailable');
    requiredScore = positive(criteria.min_score) ? Number(criteria.min_score) : facts.sell_call_fallback_min_score;
    const minBid = positive(criteria.min_bid) ? Number(criteria.min_bid) : facts.sell_call_fallback_min_bid;
    priceBound = Math.max(minBid, requiredScore / normalizeSellCallScore(1 / Math.abs(values.delta), values.dte));
    if (Number(ticker.b) + EPS < priceBound) return unresolved('Fresh call bid/edge does not support a replacement entry');
    price = Number(ticker.b);
  }
  let desiredOrder;
  try {
    desiredOrder = normalizeDesiredEntryOrder({ action, instrumentName: name, amount: oldRemaining, price,
      orderType, instrument, ticker, existingOrder: order });
  } catch (error) { return unresolved(error.message); }
  if (action === 'buy_put') {
    if (desiredOrder.limit_price > priceBound + EPS
      || normalizeBuyPutScore(Math.abs(values.delta) / desiredOrder.limit_price, values.dte) + 1e-12 < requiredScore) return invalid('Normalized put bid violates the current score ceiling');
    const available = Number(putBudgetRemaining) - Number(otherPutReserved);
    const ruleBudget = positive(rule.budget_limit) ? Number(rule.budget_limit) : Infinity;
    const cappedAmount = Math.min(oldRemaining, originalRemainingNotional / desiredOrder.limit_price,
      available / desiredOrder.limit_price, ruleBudget / desiredOrder.limit_price);
    desiredOrder.amount = amountAtVenuePrecision(Math.max(0, cappedAmount), instrument);
  } else if (desiredOrder.limit_price + EPS < priceBound) return invalid('Normalized call offer violates the current price floor');
  if (desiredOrder.amount < 0.1 - EPS) return invalid('No tradable remaining quantity fits the current entry constraints');
  // Confirmation may have proven that the original order became terminal
  // before review. Its unfilled authority can still be planned, but the order
  // must never be classified as an equivalent live quote.
  const terminalRemainder = ['cancelled', 'expired', 'rejected'].includes(String(order.order_status || order.status || '').toLowerCase());
  const comparison = terminalRemainder
    ? { decision: 'replace', differences: ['order_status'] }
    : compareRestingEntryOrder(order, desiredOrder);
  return { ...comparison, reason: comparison.decision === 'keep' ? 'Existing entry already matches the desired venue terms'
    : comparison.decision === 'unresolved' ? 'Existing entry terms cannot be compared reliably' : 'Current entry economics require revised venue terms',
    desiredOrder, ruleId: rule.id, remainingAmount: oldRemaining, approvedNotional: originalRemainingNotional,
    requiredScore, priceBound };
}

module.exports = { normalizeDesiredEntryOrder, compareRestingEntryOrder, buildRestingEntryPlan };
