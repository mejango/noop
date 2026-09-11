'use strict';

// Import-safe V2 execution policy. No credentials, network, database or bot startup.
const { normalizeBuyPutScore, getBuyPutPriceForEdgeScore } = require('./put-score');
const { normalizeSellCallScore } = require('./call-score');

const finite = value => ['number', 'string'].includes(typeof value)
  && !(typeof value === 'string' && !value.trim()) && Number.isFinite(Number(value));
const positive = value => finite(value) && Number(value) > 0;
const EPSILON = 1e-9;

function evaluateConditions(conditions, logic = 'all', values = {}) {
  if (!Array.isArray(conditions) || conditions.length === 0) return false;
  if (!['all', 'any'].includes(logic || 'all')) return false;
  const results = conditions.map(condition => {
    if (!condition || !finite(values[condition.field]) || !finite(condition.value ?? condition.threshold)) return false;
    const actual = Number(values[condition.field]);
    const expected = Number(condition.value ?? condition.threshold);
    if (condition.op === 'gt') return actual > expected;
    if (condition.op === 'gte') return actual >= expected;
    if (condition.op === 'lt') return actual < expected;
    if (condition.op === 'lte') return actual <= expected;
    return false;
  });
  return (logic || 'all') === 'all' ? results.every(Boolean) : results.some(Boolean);
}

function evaluateExitConditions(criteria, values, patientPnlPct = null) {
  if (!criteria || !Array.isArray(criteria.conditions)) return false;
  if (evaluateConditions(criteria.conditions, criteria.condition_logic, values)) return true;
  if (!finite(patientPnlPct)) return false;
  // Patient price may replace the economic capture condition only. Spot, delta,
  // DTE and every other condition remain observations of the current market.
  return evaluateConditions(criteria.conditions, criteria.condition_logic, {
    ...values,
    unrealized_pnl_pct: Number(patientPnlPct),
  });
}

function resolveConfirmationVotes(...votes) {
  const available = votes.filter(vote => vote && typeof vote.confirm === 'boolean');
  if (!available.length) return 'retry';
  // An explicit rejection always wins. Neither prose nor a patient-price plan
  // can turn a rejection into approval. A missing provider retains the existing
  // single-reviewer fallback, after the deterministic preflight has passed.
  return available.every(vote => vote.confirm) ? 'confirmed' : 'rejected';
}

function hasUsableMarginState(state) {
  return Boolean(state && finite(state.initial_margin) && finite(state.maintenance_margin)
    && positive(state.subaccount_value) && typeof state.is_under_liquidation === 'boolean');
}

function optionIdentity(position, now = Date.now()) {
  const name = position?.instrument_name || position?.instrument;
  const match = /^ETH-(\d{4})(\d{2})(\d{2})-(\d+(?:\.\d+)?)-([PC])$/.exec(name || '');
  if (!match) return null;
  const expiry = Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]), 8);
  return { name, type: match[5], strike: Number(match[4]), expiry, dte: (expiry - now) / 86400000 };
}

function getPutReplacementCoverage(position, positions = [], closeAmount = position?.amount, now = Date.now()) {
  const retiring = optionIdentity(position, now);
  const required = Number(closeAmount);
  if (!retiring || retiring.type !== 'P' || position?.direction !== 'long' || !positive(required)
    || !positive(position.amount) || required > Number(position.amount) + EPSILON) {
    return { allowed: false, reason: 'missing valid retiring long-put quantity', requiredAmount: required, replacementAmount: 0 };
  }
  const replacementAmount = positions.reduce((sum, candidate) => {
    const replacement = optionIdentity(candidate, now);
    if (!replacement || replacement.type !== 'P' || replacement.name === retiring.name
      || candidate.direction !== 'long' || !positive(candidate.amount)
      || replacement.expiry <= retiring.expiry || replacement.strike < retiring.strike) return sum;
    return sum + Number(candidate.amount);
  }, 0);
  return {
    allowed: replacementAmount + EPSILON >= required,
    requiredAmount: required,
    replacementAmount,
    minimumStrike: retiring.strike,
    reason: `later-expiry puts at strike >= ${retiring.strike}: ${replacementAmount} contracts available, ${required} required`,
  };
}

function liveRuleValues(position, ticker, spotPrice, now = Date.now()) {
  const identity = optionIdentity(position, now);
  const mark = finite(ticker?.M) ? Number(ticker.M) : null;
  const execution = position?.direction === 'short' ? ticker?.a : ticker?.b;
  const entry = Number(position?.avg_entry_price);
  const pnl = positive(execution) && entry > 0
    ? (position.direction === 'short' ? (entry - Number(execution)) : (Number(execution) - entry)) / entry * 100
    : null;
  return {
    delta: finite(ticker?.option_pricing?.d) ? Number(ticker.option_pricing.d) : null,
    mark_price: mark,
    execution_price: positive(execution) ? Number(execution) : null,
    spot_price: positive(spotPrice) ? Number(spotPrice) : null,
    unrealized_pnl_pct: pnl,
    dte: identity?.dte ?? null,
    iv: finite(ticker?.option_pricing?.i) ? Number(ticker.option_pricing.i) : null,
    theta: finite(ticker?.option_pricing?.t) ? Number(ticker.option_pricing.t) : null,
  };
}

function validateFinalOrderPolicy({ action, instrumentName, price, amount, orderType, criteria,
  triggerData = {}, ticker, instrument, positions = [], spotPrice, marginState,
  callMarginDecision, putBudgetRemaining, ruleBudgetLimit, policy, now = Date.now() }) {
  const reject = (code, reason) => ({ allowed: false, code, reason });
  if (!criteria || !policy) return reject('missing_rule', 'Missing rule or strategy policy');
  const identity = optionIdentity({ instrument_name: instrumentName }, now);
  if (!identity || !instrument || instrument.instrument_name !== instrumentName || !(identity.dte > 0)) {
    return reject('invalid_instrument', 'Missing, expired or mismatched live instrument');
  }
  if (!positive(price) || !positive(amount)) return reject('invalid_order', 'Price and quantity must be finite and positive');
  if (!['ioc', 'gtc', 'post_only'].includes(orderType)) return reject('invalid_order_type', 'Unknown order type');
  if (!ticker || !positive(spotPrice)) return reject('missing_market', 'Missing fresh ticker or spot price');
  const entry = ['buy_put', 'sell_call'].includes(action);
  const expectedType = ['buy_put', 'sell_put'].includes(action) ? 'P' : 'C';
  if (!['buy_put', 'sell_put', 'sell_call', 'buyback_call'].includes(action) || identity.type !== expectedType) {
    return reject('invalid_action', 'Action and instrument type disagree');
  }
  const delta = finite(ticker.option_pricing?.d) ? Number(ticker.option_pricing.d) : null;
  const bid = positive(ticker.b) ? Number(ticker.b) : null;
  const ask = positive(ticker.a) ? Number(ticker.a) : null;
  const outlay = Number(price) * Number(amount);
  let buyCeiling = null;
  let sellFloor = null;

  if (entry) {
    if (!hasUsableMarginState(marginState)) return reject('margin_unavailable', 'Fresh complete margin state is required for new exposure');
    if (marginState.is_under_liquidation || !(Number(marginState.initial_margin) > 0)) {
      return reject('unsafe_margin', 'Account is under liquidation or lacks initial margin');
    }
    const deltaRange = action === 'buy_put' ? policy.putDeltaRange : policy.callDeltaRange;
    const dteRange = action === 'buy_put' ? policy.putDteRange : policy.callDteRange;
    for (const [value, range, name] of [[delta, deltaRange, 'strategy delta'], [identity.dte, dteRange, 'strategy DTE'],
      [delta, criteria.delta_range || deltaRange, 'rule delta'], [identity.dte, criteria.dte_range || dteRange, 'rule DTE']]) {
      if (!finite(value) || !Array.isArray(range) || range.length !== 2 || !range.every(finite)
        || Number(range[0]) > Number(range[1]) || value < range[0] || value > range[1]) {
        return reject('range_failed', `Fresh ${name} is outside the approved range`);
      }
    }
    if (positive(criteria.max_strike_pct) && identity.strike >= Number(criteria.max_strike_pct) * Number(spotPrice)) {
      return reject('strike_failed', 'Fresh strike/spot relationship violates the rule');
    }
    if (criteria.market_conditions != null && !Array.isArray(criteria.market_conditions)) {
      return reject('conditions_failed', 'Malformed entry market conditions');
    }
    if (Array.isArray(criteria.market_conditions) && criteria.market_conditions.length
      && !evaluateConditions(criteria.market_conditions, 'all', { spot_price: spotPrice })) {
      return reject('conditions_failed', 'Fresh market conditions no longer satisfy the entry rule');
    }
  }

  if (action === 'sell_call') {
    const minScore = positive(criteria.min_score) ? Number(criteria.min_score) : policy.sellCallMinScore;
    const minBid = positive(criteria.min_bid) ? Number(criteria.min_bid) : policy.sellCallMinBid;
    if (!bid || bid < minBid || normalizeSellCallScore(bid / Math.abs(delta), identity.dte) + EPSILON < minScore) {
      return reject('call_quote_failed', 'Fresh call bid/edge no longer satisfies the rule');
    }
    const scoreFloor = minScore / normalizeSellCallScore(1 / Math.abs(delta), identity.dte);
    sellFloor = Math.max(minBid, scoreFloor);
    if (Number(price) + EPSILON < sellFloor) return reject('call_price_failed', 'Final sell-call limit violates min_bid or CALL EDGE');
    if (!callMarginDecision?.available || !callMarginDecision.entryCapSatisfied
      || !positive(callMarginDecision.marginPerUnit)
      || Number(amount) * Number(callMarginDecision.marginPerUnit) > Number(marginState.initial_margin) + EPSILON) {
      return reject('call_margin_failed', 'Final call quantity exceeds verified margin capacity or the active cap');
    }
  }

  if (action === 'buy_put') {
    if (!ask || !positive(criteria.min_score)) return reject('put_quote_failed', 'Fresh put ask and an approved minimum edge are required');
    const requiredScore = Math.max(Number(criteria.min_score), Number(triggerData.target_score ?? criteria.target_score) || 0);
    buyCeiling = getBuyPutPriceForEdgeScore(Math.abs(delta), requiredScore, identity.dte);
    const approvedCap = Number(triggerData.advisor_limit_price);
    if (approvedCap > 0) buyCeiling = Math.min(buyCeiling, approvedCap);
    if (!(normalizeBuyPutScore(Math.abs(delta) / Number(price), identity.dte) + EPSILON >= requiredScore)
      || Number(price) > buyCeiling + EPSILON) return reject('put_price_failed', 'Final put bid violates approved cap or fresh PUT EDGE');
    if (!finite(putBudgetRemaining) || Number(putBudgetRemaining) < outlay - EPSILON
      || (finite(ruleBudgetLimit) && Number(ruleBudgetLimit) > 0 && outlay > Number(ruleBudgetLimit) + EPSILON)) {
      return reject('put_budget_failed', 'Final put outlay exceeds the verified available budget');
    }
  }

  if (!entry) {
    const position = positions.find(item => item.instrument_name === instrumentName);
    const expectedDirection = action === 'sell_put' ? 'long' : 'short';
    if (!position || position.direction !== expectedDirection || !positive(position.amount)
      || Number(amount) > Number(position.amount) + EPSILON) return reject('close_quantity_failed', 'Final exit exceeds the live closeable position');
    const entryPrice = Number(position.avg_entry_price);
    const values = liveRuleValues(position, ticker, spotPrice, now);
    if (!(entryPrice > 0)) return reject('missing_cost_basis', 'Live position cost basis is unavailable');
    const finalPnl = (action === 'buyback_call' ? entryPrice - Number(price) : Number(price) - entryPrice) / entryPrice * 100;
    if (action === 'buyback_call') {
      if (!ask) return reject('missing_buyback_ask', 'Fresh buyback ask is required');
      const threat = criteria.buyback_intent === 'threat_management' && criteria.allow_below_profit_floor === true;
      if (!threat) {
        const capture = Math.max(policy.callCapturePct, Number(criteria.target_capture_pct ?? criteria.capture_floor_pct) || 0);
        buyCeiling = entryPrice * (1 - capture / 100);
        const cap = Number(triggerData.advisor_limit_price ?? criteria.max_buyback_price ?? criteria.limit_price);
        if (cap > 0) buyCeiling = Math.min(buyCeiling, cap);
        if (finalPnl + EPSILON < capture || Number(price) > buyCeiling + EPSILON) return reject('capture_failed', 'Final buyback price violates the capture floor or approved ceiling');
      }
      if (!evaluateConditions(criteria.conditions, criteria.condition_logic, { ...values, unrealized_pnl_pct: threat ? values.unrealized_pnl_pct : finalPnl })) {
        return reject('exit_conditions_failed', 'Fresh buyback facts do not satisfy every approved exit condition');
      }
    } else {
      const intent = criteria.put_exit_intent || criteria.exit_intent;
      if (intent === 'roll_protection') {
        if (identity.dte > policy.putRollDte || orderType !== 'ioc') return reject('roll_failed', 'Roll is outside its DTE window or is not IOC');
        const coverage = getPutReplacementCoverage(position, positions, amount, now);
        if (!coverage.allowed) return reject('replacement_failed', coverage.reason);
        if (!bid || !evaluateConditions(criteria.conditions, criteria.condition_logic, values)) return reject('exit_conditions_failed', 'Fresh roll conditions or executable bid are unavailable');
      } else if (intent === 'monetize_tail_win') {
        const fairPrice = Math.max(values.mark_price || 0, identity.strike - Number(spotPrice));
        const currentProofPnl = Math.max(Number(values.unrealized_pnl_pct) || -Infinity, (fairPrice - entryPrice) / entryPrice * 100);
        if (!(currentProofPnl > policy.putMonetizationPct) || !(finalPnl > policy.putMonetizationPct)) return reject('monetization_failed', 'Fresh fair/executable value and final limit must satisfy the tail-win floor');
        const requestedFraction = Number(criteria.tranche_fraction ?? criteria.max_tranche_fraction) || policy.putMaxTrancheFraction;
        if (!(requestedFraction > 0) || Number(amount) > Number(position.amount) * Math.min(requestedFraction, policy.putMaxTrancheFraction) + EPSILON
          || Number(amount) >= Number(position.amount)) return reject('tranche_failed', 'Final put sale exceeds its retained-protection tranche');
        sellFloor = Number(triggerData.advisor_limit_price ?? criteria.min_exit_price ?? criteria.limit_price ?? criteria.target_exit_price);
        if (!(sellFloor > 0) || Number(price) + EPSILON < sellFloor) return reject('put_exit_price_failed', 'Final put sale violates the approved exit floor');
        if (!evaluateConditions(criteria.conditions, criteria.condition_logic, { ...values, unrealized_pnl_pct: finalPnl })) return reject('exit_conditions_failed', 'Fresh put facts do not satisfy every approved exit condition');
      } else return reject('unknown_exit_intent', 'Missing typed put exit intent');
    }
  }
  return { allowed: true, approvedBounds: { buyCeiling, sellFloor } };
}

module.exports = { evaluateConditions, evaluateExitConditions, resolveConfirmationVotes, hasUsableMarginState,
  getPutReplacementCoverage, liveRuleValues, validateFinalOrderPolicy };
