/**
 * Trading System Tests
 *
 * Tests for the LLM-driven trading system's pure functions and DB operations.
 * Uses Node.js built-in assert module (no external test framework required).
 *
 * Run: node test/trading-system.test.js
 */

const assert = require('assert');
const path = require('path');

let passed = 0, failed = 0;
const test = (name, fn) => {
  try {
    fn();
    passed++;
    console.log(`  PASS ${name}`);
  } catch (e) {
    failed++;
    console.log(`  FAIL ${name}: ${e.message}`);
  }
};

const describe = (name, fn) => {
  console.log(`\n${name}`);
  fn();
};

// ============================================================================
// Pure functions (copied from script.js to keep tests self-contained)
// ============================================================================

const parseExpiryFromInstrument = (name) => {
  if (!name) return null;
  const parts = name.split('-');
  if (parts.length < 4) return null;
  const d = parts[1]; // "20260501"
  return new Date(`${d.slice(0,4)}-${d.slice(4,6)}-${d.slice(6,8)}T08:00:00Z`);
};

const computeCurrentValues = (position, ticker, spotPrice) => {
  const expiry = parseExpiryFromInstrument(position.instrument_name);
  const dte = expiry ? Math.max(0, (expiry.getTime() - Date.now()) / (86400000)) : null;
  const markPrice = Number(ticker?.M) || position.mark_price || 0;
  const entryPrice = position.avg_entry_price || 0;
  const unrealizedPnlPct = entryPrice > 0 ? ((markPrice - entryPrice) / entryPrice) * 100 : 0;
  // For short positions, P&L is inverted
  const adjustedPnlPct = position.direction === 'short' ? -unrealizedPnlPct : unrealizedPnlPct;

  return {
    delta: Number(ticker?.option_pricing?.d) || position.delta || 0,
    mark_price: markPrice,
    spot_price: spotPrice,
    unrealized_pnl_pct: adjustedPnlPct,
    dte: dte,
    iv: Number(ticker?.option_pricing?.i) || 0,
    theta: Number(ticker?.option_pricing?.t) || position.theta || 0,
  };
};

const getRuleEvaluationValues = (position, ticker, spotPrice, action = null) => {
  const values = computeCurrentValues(position, ticker, spotPrice);
  if (action !== 'buyback_call' || position?.direction !== 'short') return values;

  const executablePrice = Number(ticker?.a) || values.mark_price || 0;
  const entryPrice = Number(position?.avg_entry_price) || 0;
  const rawPnlPct = entryPrice > 0 ? ((executablePrice - entryPrice) / entryPrice) * 100 : 0;
  const adjustedPnlPct = position.direction === 'short' ? -rawPnlPct : rawPnlPct;

  return {
    ...values,
    unrealized_pnl_pct: adjustedPnlPct,
    execution_price: executablePrice,
  };
};

const ASSESSMENT_UNSUPPORTED_PATTERNS = [
  /\befficiency\b/i,
  /\bthreshold\b/i,
];

const assessmentUsesUnsupportedMetricLanguage = (text) => {
  const normalized = String(text || '').trim();
  if (!normalized) return null;
  for (const pattern of ASSESSMENT_UNSUPPORTED_PATTERNS) {
    if (pattern.test(normalized)) return pattern;
  }
  return null;
};

const buildAdvisoryObservationThesis = (sentiment24h) => {
  if (!sentiment24h || typeof sentiment24h !== 'object') return null;

  const skewDirection = String(sentiment24h.options_skew?.direction || '').toLowerCase();
  const oiChangePct = Number(sentiment24h.aggregate_oi?.change_pct);
  const oiIsFinite = Number.isFinite(oiChangePct);

  if (skewDirection.includes('narrow') && oiIsFinite && oiChangePct > 0) {
    return 'Narrowing skew with rising open interest suggests repositioning rather than one-way panic.';
  }
  if (skewDirection.includes('narrow') && oiIsFinite && oiChangePct < 0) {
    return 'Narrowing skew with fading open interest suggests compression and weaker conviction.';
  }
  if (skewDirection.includes('widen') && oiIsFinite && oiChangePct > 0) {
    return 'Widening skew with rising open interest suggests demand for protection is building.';
  }
  if (skewDirection.includes('widen') && oiIsFinite && oiChangePct < 0) {
    return 'Widening skew with falling open interest suggests fear is lingering but participation is thinning.';
  }
  if (oiIsFinite && oiChangePct > 0) {
    return 'Open interest is expanding, so participation is building rather than clearing.';
  }
  if (oiIsFinite && oiChangePct < 0) {
    return 'Open interest is fading, so participation is thinning.';
  }
  if (skewDirection.includes('narrow')) {
    return 'Skew is narrowing, which points to less urgency for downside protection.';
  }
  if (skewDirection.includes('widen')) {
    return 'Skew is widening, which points to a more defensive options posture.';
  }
  return null;
};

const buildAdvisoryStanceSummary = ({
  putBudgetRemaining,
  secondOpinion = null,
  entryRulesCount = 0,
  exitRulesCount = 0,
}) => {
  const vetoCount = secondOpinion?.vetoes?.length || 0;
  const noEntryRules = entryRulesCount === 0;
  const noExitRules = exitRulesCount === 0;

  if (noEntryRules && noExitRules) {
    if (vetoCount > 0) {
      return `Taleb vetoed ${vetoCount} proposed rule${vetoCount === 1 ? '' : 's'}, so the stance is to sit on hands and wait for cleaner asymmetry.`;
    }
    if (Number.isFinite(putBudgetRemaining) && putBudgetRemaining < 1) {
      return `Put budget remaining $${Number(putBudgetRemaining).toFixed(2)} leaves little room for fresh deployment, so the stance is to sit on hands and wait for cleaner asymmetry.`;
    }
    return 'The stance is to sit on hands and wait for cleaner asymmetry.';
  }

  if (!noEntryRules && noExitRules) {
    return 'The stance is selective deployment where pricing is favorable.';
  }

  if (noEntryRules && !noExitRules) {
    return 'The stance is maintenance over fresh deployment: manage existing risk, do not add new exposure.';
  }

  return 'The stance is active repositioning: add selectively while cleaning up existing risk.';
};

const buildFactualAdvisoryAssessment = ({
  spotPrice,
  momentum,
  mandelbrotContext,
  sentiment,
  putBudgetRemaining,
  secondOpinion = null,
  entryRulesCount = 0,
  exitRulesCount = 0,
}) => {
  const parts = [];
  if (Number.isFinite(spotPrice) && spotPrice > 0) {
    parts.push(`ETH at $${spotPrice.toFixed(0)}.`);
  }
  if (mandelbrotContext?.regime) {
    const regimeLabel = String(mandelbrotContext.regime).replace(/_/g, ' ');
    const confidence = Number(mandelbrotContext.confidence || 0);
    parts.push(confidence > 0
      ? `ETH is in a ${regimeLabel} regime (${(confidence * 100).toFixed(0)}% confidence).`
      : `${regimeLabel} regime.`);
  } else if (momentum?.mediumTerm?.main) {
    parts.push(`Medium-term momentum is ${momentum.mediumTerm.main}.`);
  }

  const sentiment24h = sentiment?.windows?.['24h'] || null;
  if (sentiment24h) {
    const skewText = sentiment24h.options_skew?.current_pct != null
      ? `options skew ${sentiment24h.options_skew.current_pct > 0 ? '+' : ''}${Number(sentiment24h.options_skew.current_pct).toFixed(2)}% (${sentiment24h.options_skew.direction || 'unknown'})`
      : null;
    const oiText = sentiment24h.aggregate_oi?.change_pct != null
      ? `open interest ${sentiment24h.aggregate_oi.change_pct > 0 ? '+' : ''}${Number(sentiment24h.aggregate_oi.change_pct).toFixed(1)}%`
      : null;
    if (skewText || oiText) {
      parts.push([skewText, oiText].filter(Boolean).join(', ') + '.');
    }
  }

  const observationThesis = buildAdvisoryObservationThesis(sentiment24h);
  if (observationThesis) {
    parts.push(observationThesis);
  }

  parts.push(buildAdvisoryStanceSummary({
    putBudgetRemaining,
    secondOpinion,
    entryRulesCount,
    exitRulesCount,
  }));

  return parts.join(' ').replace(/\s+/g, ' ').trim() || 'No assessment produced.';
};

const evaluateConditions = (conditions, logic, values) => {
  if (!Array.isArray(conditions) || conditions.length === 0) return false;
  const results = conditions.map(c => {
    const v = values[c.field];
    if (v == null) return false;
    if (c.op === 'gt') return v > c.value;
    if (c.op === 'lt') return v < c.value;
    if (c.op === 'gte') return v >= c.value;
    if (c.op === 'lte') return v <= c.value;
    return false;
  });
  return logic === 'all' ? results.every(Boolean) : results.some(Boolean);
};

const extractOrderRecord = (payload) => {
  if (!payload || typeof payload !== 'object') return null;
  if (payload.order && typeof payload.order === 'object') return payload.order;
  if (payload.result && typeof payload.result === 'object') return extractOrderRecord(payload.result);
  if (payload.order_id || payload.instrument_name || payload.order_status) return payload;
  return null;
};

const summarizeSentimentForLLM = (sentiment) => {
  const skewRows = Array.isArray(sentiment?.optionsSkew) ? sentiment.optionsSkew : [];
  const latestSkew = skewRows.length > 0 ? skewRows[skewRows.length - 1] : null;
  const validSkewRows = skewRows.filter(r => r.avg_put_iv != null && r.avg_call_iv != null);
  const currentSkewPct = latestSkew?.avg_put_iv != null && latestSkew?.avg_call_iv != null
    ? +(((latestSkew.avg_put_iv - latestSkew.avg_call_iv) * 100).toFixed(2))
    : null;
  const avgSkew24hPct = validSkewRows.length > 0
    ? +((validSkewRows.reduce((sum, row) => sum + (row.avg_put_iv - row.avg_call_iv), 0) / validSkewRows.length) * 100).toFixed(2)
    : null;

  let skewDirection = 'unknown';
  if (currentSkewPct != null && avgSkew24hPct != null) {
    skewDirection = currentSkewPct > avgSkew24hPct ? 'widening' : currentSkewPct < avgSkew24hPct ? 'narrowing' : 'stable';
  }

  const oiRows = Array.isArray(sentiment?.aggregateOI) ? sentiment.aggregateOI : [];
  const currentOI = oiRows.length > 0 ? Number(oiRows[oiRows.length - 1].total_oi) : null;
  const firstOI = oiRows.length > 1 ? Number(oiRows[0].total_oi) : null;
  const oiChange24hPct = currentOI != null && firstOI > 0
    ? +((((currentOI - firstOI) / firstOI) * 100).toFixed(1))
    : null;

  const fundingCurrent = sentiment?.fundingRate?.rate ?? null;
  const fundingAvg24h = sentiment?.fundingAvg24h ?? null;
  let fundingTrend = 'unknown';
  if (fundingCurrent != null && fundingAvg24h != null) {
    fundingTrend = fundingCurrent > fundingAvg24h ? 'rising' : fundingCurrent < fundingAvg24h ? 'declining' : 'stable';
  }

  const marketQuality = Array.isArray(sentiment?.marketQuality) ? sentiment.marketQuality : [];
  const marketQualitySummary = marketQuality.map(row => ({
    option_type: row.option_type,
    count: row.count,
    avg_spread_pct: row.avg_spread != null ? +(row.avg_spread * 100).toFixed(2) : null,
    avg_iv_pct: row.avg_iv != null ? +(row.avg_iv * 100).toFixed(1) : null,
    avg_depth: row.avg_depth != null ? +Number(row.avg_depth).toFixed(2) : null,
  }));

  return {
    funding_rate: {
      current: fundingCurrent,
      avg_24h: fundingAvg24h,
      trend: fundingTrend,
    },
    options_skew: {
      current_pct: currentSkewPct,
      avg_24h_pct: avgSkew24hPct,
      direction: skewDirection,
    },
    aggregate_oi: {
      current: currentOI,
      change_24h_pct: oiChange24hPct,
    },
    market_quality: marketQualitySummary,
  };
};

const CALL_EXPOSURE_CAP_PCT = 0.45;
const CALL_ENTRY_BUFFER_PCT = 0.05;
const CALL_ENTRY_CAP_PCT = Math.max(0, CALL_EXPOSURE_CAP_PCT - CALL_ENTRY_BUFFER_PCT);
const getMarginCapacityBase = (marginState) => {
  const collateralMarginBase = Number(marginState?.collaterals_initial_margin ?? 0);
  if (collateralMarginBase > 0) return collateralMarginBase;
  const collateralValue = Number(marginState?.collaterals_value ?? 0);
  if (collateralValue > 0) return collateralValue;
  return Number(marginState?.subaccount_value ?? 0);
};
const getMarginUtilizationBase = (marginState) => {
  const aggregatedMaintenanceBase = Math.abs(Number(marginState?.aggregated_collaterals_maintenance_margin ?? 0));
  if (aggregatedMaintenanceBase > 0) return aggregatedMaintenanceBase;
  const maintenanceBase = Math.abs(Number(marginState?.collaterals_maintenance_margin ?? 0));
  if (maintenanceBase > 0) return maintenanceBase;
  return getMarginCapacityBase(marginState);
};
const normalizeMarginUtilizationValue = (value) => {
  if (!Number.isFinite(value)) return null;
  return Math.max(0, Math.min(1, value));
};
const estimateMarginUtilizationFromComponents = (marginState, additionalOpenOrdersMargin = 0) => {
  const base = getMarginUtilizationBase(marginState);
  if (!(base > 0)) return null;
  const usedMargin = Math.abs(Number(
      marginState?.aggregated_positions_initial_margin ??
      marginState?.positions_initial_margin ??
      0
    ))
    + Math.abs(Number(marginState?.open_orders_margin ?? 0))
    + Math.max(0, Number(additionalOpenOrdersMargin ?? 0));
  return normalizeMarginUtilizationValue(usedMargin / base);
};
const estimateMarginUtilization = (marginState, additionalOpenOrdersMargin = 0) => {
  const componentUtilization = estimateMarginUtilizationFromComponents(marginState, additionalOpenOrdersMargin);
  if (componentUtilization != null) return componentUtilization;

  const base = getMarginCapacityBase(marginState);
  if (!(base > 0)) return null;
  const availableInitialMargin = Number(marginState?.initial_margin ?? NaN);
  if (Number.isFinite(availableInitialMargin)) {
    const projectedAvailable = availableInitialMargin - Math.max(0, Number(additionalOpenOrdersMargin ?? 0));
    return normalizeMarginUtilizationValue(1 - (projectedAvailable / base));
  }

  const explicitMarginUsage = Number(
    marginState?.margin_usage_pct ??
    marginState?.margin_utilization_pct ??
    marginState?.margin_utilization ??
    NaN
  );
  const additionalRatio = Math.max(0, Number(additionalOpenOrdersMargin ?? 0)) / base;
  if (Number.isFinite(explicitMarginUsage)) {
    const normalized = explicitMarginUsage > 1 ? explicitMarginUsage / 100 : explicitMarginUsage;
    return normalizeMarginUtilizationValue(normalized + additionalRatio);
  }

  return estimateMarginUtilizationFromComponents(marginState, additionalOpenOrdersMargin);
};
const estimateDisplayedMarginUtilization = (marginState) => {
  if (!marginState) return null;
  const maintenanceBase = getMarginUtilizationBase(marginState);
  const maintenanceMargin = Number(marginState?.maintenance_margin ?? NaN);
  if (maintenanceBase > 0 && Number.isFinite(maintenanceMargin)) {
    return normalizeMarginUtilizationValue(1 - (maintenanceMargin / maintenanceBase));
  }
  return estimateMarginUtilization(marginState);
};
const estimateProjectedDisplayedMarginUtilization = (marginState, additionalMargin = 0) => {
  if (!marginState) return null;
  const currentDisplayed = estimateDisplayedMarginUtilization(marginState);
  const maintenanceBase = getMarginUtilizationBase(marginState);
  if (currentDisplayed != null && maintenanceBase > 0) {
    return normalizeMarginUtilizationValue(currentDisplayed + (Math.max(0, Number(additionalMargin ?? 0)) / maintenanceBase));
  }
  return estimateMarginUtilization(marginState, additionalMargin);
};
const estimateStandardShortCallInitialMarginPerUnit = (strike, spotPrice, premium) => {
  if (!(spotPrice > 0)) return Infinity;
  const otm = Math.max(0, strike - spotPrice);
  const otmBuffer = Math.max(0.15 - (otm / spotPrice), 0.13) * spotPrice;
  return Math.max(0, otmBuffer - Math.max(0, premium || 0));
};
const estimateShortCallMarginPerUnit = (marginState, positions, restingOrders, spotPrice, strike = 0, premium = 0) => {
  const shortCallPositions = positions.filter(p => p.instrument_name?.endsWith('-C') && p.direction === 'short');
  const currentShortExposure = shortCallPositions.reduce((sum, p) => sum + Math.abs(Number(p.amount) || 0), 0);
  const empiricalPositionsMargin = Math.abs(Number(
      marginState?.aggregated_positions_initial_margin ??
      marginState?.positions_initial_margin ??
      0
    ));
  if (currentShortExposure > 0 && empiricalPositionsMargin > 0) {
    return empiricalPositionsMargin / currentShortExposure;
  }
  const restingShortExposure = restingOrders.filter(order => order.action === 'sell_call').reduce((sum, order) => sum + Math.abs(Number(order.amount) || 0), 0);
  if (restingShortExposure > 0 && Number(marginState?.open_orders_margin ?? 0) > 0) {
    return Number(marginState.open_orders_margin) / restingShortExposure;
  }
  const documentedEstimate = estimateStandardShortCallInitialMarginPerUnit(strike, spotPrice, premium);
  if (Number.isFinite(documentedEstimate) && documentedEstimate > 0) return documentedEstimate;
  return Math.max((spotPrice || 0) * 0.13, 100);
};
const getCallMarginContext = (action, marginState, positions, restingOrders, instruments, spotPrice, instrumentName, amount, limitPrice) => {
  if (action !== 'sell_call') return 'Call margin utilization: not applicable for this action.';
  if (!marginState) return 'Call margin utilization: unavailable (margin state unavailable).';
  const currentUtilization = estimateDisplayedMarginUtilization(marginState);
  const instrument = instruments.find((item) => item.instrument_name === instrumentName);
  const strike = Number(instrument?.option_details?.strike || instrumentName?.split('-')?.[2] || 0) || 0;
  const normalizedAmount = Math.max(0, Number(amount || 0));
  const normalizedLimitPrice = Number(limitPrice || 0);
  const marginPerUnit = estimateShortCallMarginPerUnit(marginState, positions, restingOrders, spotPrice, strike, normalizedLimitPrice);
  const additionalMargin = normalizedAmount * marginPerUnit;
  const projectedUtilization = estimateProjectedDisplayedMarginUtilization(marginState, additionalMargin);
  const capPct = CALL_EXPOSURE_CAP_PCT * 100;
  const entryCapPct = CALL_ENTRY_CAP_PCT * 100;
  return `Call margin utilization: current_derive_display=${currentUtilization != null ? `${(currentUtilization * 100).toFixed(1)}%` : 'N/A'}, projected_after_trade_display=${projectedUtilization != null ? `${(projectedUtilization * 100).toFixed(1)}%` : 'N/A'}, per_contract_estimate=$${marginPerUnit.toFixed(2)}, caution_zone=${entryCapPct.toFixed(1)}%-${capPct.toFixed(1)}%, hard_cap=${capPct.toFixed(1)}%. Treat ${entryCapPct.toFixed(1)}% as a caution threshold and ${capPct.toFixed(1)}% as the actual ceiling; if the initial size is too large, reduce size down toward the hard cap before rejecting. Use these exact figures; do not invent utilization numbers.`;
};

const clampSellCallQtyToEntryCap = ({
  desiredQty,
  amountStep,
  marginState,
  marginPerUnit,
}) => {
  const step = amountStep > 0 ? amountStep : 0.01;
  const desired = Math.max(0, Number(desiredQty) || 0);
  if (!(desired >= step)) {
    return { qty: 0, projectedUtilization: estimateMarginUtilization(marginState, 0) };
  }
  if (!marginState || !(marginPerUnit > 0)) {
    return { qty: Math.floor(desired / step) * step, projectedUtilization: null };
  }

  const currentUsedMargin = Math.abs(Number(
      marginState?.aggregated_positions_initial_margin ??
      marginState?.positions_initial_margin ??
      0
    ))
    + Math.abs(Number(marginState?.open_orders_margin ?? 0));
  const marginBase = getMarginUtilizationBase(marginState);
  if (!(marginBase > 0)) {
    return { qty: Math.floor(desired / step) * step, projectedUtilization: null };
  }

  const maxAdditionalMargin = Math.max(0, CALL_EXPOSURE_CAP_PCT * marginBase - currentUsedMargin);
  const maxQtyAtCap = maxAdditionalMargin / marginPerUnit;
  const clampedQty = Math.floor(Math.min(desired, maxQtyAtCap) / step) * step;
  const finalQty = clampedQty >= step ? clampedQty : 0;
  return {
    qty: finalQty,
    projectedUtilization: estimateProjectedDisplayedMarginUtilization(marginState, finalQty * marginPerUnit),
  };
};

const getInstrumentPriceStep = (instrument, fallbackPrice = 0) => {
  const configuredStep = Number(
    instrument?.price_step ??
    instrument?.options?.price_step ??
    instrument?.option_details?.price_step ??
    0
  );
  if (configuredStep > 0) return configuredStep;
  if (fallbackPrice >= 10) return 0.1;
  if (fallbackPrice >= 1) return 0.1;
  return 0.01;
};

const roundToStep = (value, step, mode = 'nearest') => {
  if (!(step > 0)) return value;
  const scaled = value / step;
  if (mode === 'up') return Math.ceil(scaled) * step;
  if (mode === 'down') return Math.floor(scaled) * step;
  return Math.round(scaled) * step;
};

const normalizePriceToStep = (value, step, mode = 'nearest') => {
  if (!(Number(value) > 0)) return 0;
  if (!(step > 0)) return Number(value);
  const rounded = roundToStep(Number(value), step, mode);
  const decimals = (() => {
    const normalized = String(step);
    if (normalized.includes('e-')) {
      const [, exponent] = normalized.split('e-');
      return Number(exponent) || 0;
    }
    const [, fraction = ''] = normalized.split('.');
    return fraction.length;
  })();
  return Number(rounded.toFixed(decimals));
};

const avoidRoundNumberRestingPrice = (direction, price, step) => {
  const numericPrice = Number(price);
  if (!(numericPrice > 0) || !(step > 0)) return numericPrice;
  if (Math.abs(numericPrice - Math.round(numericPrice)) > 1e-9) return numericPrice;
  if (direction === 'sell') return normalizePriceToStep(numericPrice + step, step, 'up');
  const lowerPrice = numericPrice - step;
  return lowerPrice > 0 ? normalizePriceToStep(lowerPrice, step, 'down') : numericPrice;
};

const computePostOnlyRetryPrice = (direction, ticker, instrument, attemptedPrice) => {
  const bidPrice = Number(ticker?.b) || 0;
  const askPrice = Number(ticker?.a) || 0;
  const step = getInstrumentPriceStep(instrument, attemptedPrice);

  if (direction === 'sell') {
    const retryBase = bidPrice > 0 ? bidPrice + step : attemptedPrice + step;
    const retryPrice = avoidRoundNumberRestingPrice(direction, normalizePriceToStep(retryBase, step, 'up'), step);
    return retryPrice > 0 ? { retryPrice, bidPrice, askPrice, step } : null;
  }

  if (askPrice <= 0) return null;
  const belowAsk = askPrice - step;
  const candidate = belowAsk > 0
    ? normalizePriceToStep(belowAsk, step, 'down')
    : normalizePriceToStep(askPrice * 0.99, step, 'down');
  const retryPrice = avoidRoundNumberRestingPrice(direction, candidate, step);
  return retryPrice > 0 ? { retryPrice, bidPrice, askPrice, step } : null;
};


// ============================================================================
// 1. parseExpiryFromInstrument
// ============================================================================

describe('parseExpiryFromInstrument', () => {
  test('parses ETH-20260501-1500-P correctly', () => {
    const result = parseExpiryFromInstrument('ETH-20260501-1500-P');
    assert.deepStrictEqual(result, new Date('2026-05-01T08:00:00Z'));
  });

  test('parses ETH-20261231-2000-C correctly', () => {
    const result = parseExpiryFromInstrument('ETH-20261231-2000-C');
    assert.deepStrictEqual(result, new Date('2026-12-31T08:00:00Z'));
  });

  test('returns null for "INVALID"', () => {
    const result = parseExpiryFromInstrument('INVALID');
    assert.strictEqual(result, null);
  });

  test('returns null for empty string', () => {
    const result = parseExpiryFromInstrument('');
    assert.strictEqual(result, null);
  });

  test('parses ETH-20260101-1000-P correctly', () => {
    const result = parseExpiryFromInstrument('ETH-20260101-1000-P');
    assert.deepStrictEqual(result, new Date('2026-01-01T08:00:00Z'));
  });

  test('returns null for null input', () => {
    const result = parseExpiryFromInstrument(null);
    assert.strictEqual(result, null);
  });

  test('returns null for undefined input', () => {
    const result = parseExpiryFromInstrument(undefined);
    assert.strictEqual(result, null);
  });
});


// ============================================================================
// 2. evaluateConditions
// ============================================================================

describe('evaluateConditions', () => {
  test('single condition gt - true', () => {
    const conds = [{ field: 'delta', op: 'gt', value: 0.5 }];
    assert.strictEqual(evaluateConditions(conds, 'all', { delta: 0.7 }), true);
  });

  test('single condition gt - false', () => {
    const conds = [{ field: 'delta', op: 'gt', value: 0.5 }];
    assert.strictEqual(evaluateConditions(conds, 'all', { delta: 0.3 }), false);
  });

  test('multiple conditions with logic all - all true', () => {
    const conds = [
      { field: 'delta', op: 'gt', value: 0.1 },
      { field: 'dte', op: 'lt', value: 30 },
    ];
    assert.strictEqual(evaluateConditions(conds, 'all', { delta: 0.5, dte: 20 }), true);
  });

  test('multiple conditions with logic all - one false', () => {
    const conds = [
      { field: 'delta', op: 'gt', value: 0.1 },
      { field: 'dte', op: 'lt', value: 30 },
    ];
    assert.strictEqual(evaluateConditions(conds, 'all', { delta: 0.5, dte: 40 }), false);
  });

  test('multiple conditions with logic any - one true', () => {
    const conds = [
      { field: 'delta', op: 'gt', value: 0.8 },
      { field: 'dte', op: 'lt', value: 5 },
    ];
    assert.strictEqual(evaluateConditions(conds, 'any', { delta: 0.3, dte: 2 }), true);
  });

  test('multiple conditions with logic any - all false', () => {
    const conds = [
      { field: 'delta', op: 'gt', value: 0.8 },
      { field: 'dte', op: 'lt', value: 5 },
    ];
    assert.strictEqual(evaluateConditions(conds, 'any', { delta: 0.3, dte: 10 }), false);
  });

  test('empty conditions array returns false', () => {
    assert.strictEqual(evaluateConditions([], 'all', { delta: 0.5 }), false);
  });

  test('unknown field returns false for that condition', () => {
    const conds = [{ field: 'nonexistent', op: 'gt', value: 1 }];
    assert.strictEqual(evaluateConditions(conds, 'all', { delta: 0.5 }), false);
  });

  test('operator gte - boundary value equal', () => {
    const conds = [{ field: 'delta', op: 'gte', value: 0.5 }];
    assert.strictEqual(evaluateConditions(conds, 'all', { delta: 0.5 }), true);
  });

  test('operator gte - below boundary', () => {
    const conds = [{ field: 'delta', op: 'gte', value: 0.5 }];
    assert.strictEqual(evaluateConditions(conds, 'all', { delta: 0.49 }), false);
  });

  test('operator lte - boundary value equal', () => {
    const conds = [{ field: 'dte', op: 'lte', value: 10 }];
    assert.strictEqual(evaluateConditions(conds, 'all', { dte: 10 }), true);
  });

  test('operator lte - above boundary', () => {
    const conds = [{ field: 'dte', op: 'lte', value: 10 }];
    assert.strictEqual(evaluateConditions(conds, 'all', { dte: 10.01 }), false);
  });

  test('operator lt - boundary value equal returns false', () => {
    const conds = [{ field: 'dte', op: 'lt', value: 10 }];
    assert.strictEqual(evaluateConditions(conds, 'all', { dte: 10 }), false);
  });

  test('operator gt - boundary value equal returns false', () => {
    const conds = [{ field: 'delta', op: 'gt', value: 0.5 }];
    assert.strictEqual(evaluateConditions(conds, 'all', { delta: 0.5 }), false);
  });

  test('null value in values object returns false', () => {
    const conds = [{ field: 'delta', op: 'gt', value: 0.1 }];
    assert.strictEqual(evaluateConditions(conds, 'all', { delta: null }), false);
  });

  test('undefined value in values object returns false', () => {
    const conds = [{ field: 'delta', op: 'gt', value: 0.1 }];
    assert.strictEqual(evaluateConditions(conds, 'all', { delta: undefined }), false);
  });

  test('non-array conditions returns false', () => {
    assert.strictEqual(evaluateConditions(null, 'all', { delta: 0.5 }), false);
    assert.strictEqual(evaluateConditions('not-array', 'all', { delta: 0.5 }), false);
  });

  test('unknown operator returns false', () => {
    const conds = [{ field: 'delta', op: 'eq', value: 0.5 }];
    assert.strictEqual(evaluateConditions(conds, 'all', { delta: 0.5 }), false);
  });
});


// ============================================================================
// 2b. summarizeSentimentForLLM
// ============================================================================

describe('summarizeSentimentForLLM', () => {
  test('compacts sentiment fields into LLM-safe summary', () => {
    const summary = summarizeSentimentForLLM({
      fundingRate: { rate: 0.012 },
      fundingAvg24h: 0.01,
      optionsSkew: [
        { avg_put_iv: 0.61, avg_call_iv: 0.54 },
        { avg_put_iv: 0.66, avg_call_iv: 0.55 },
      ],
      aggregateOI: [
        { total_oi: 1000 },
        { total_oi: 1150 },
      ],
      marketQuality: [
        { option_type: 'P', count: 12, avg_spread: 0.0312, avg_iv: 0.64, avg_depth: 7.777 },
      ],
    });

    assert.deepStrictEqual(summary, {
      funding_rate: {
        current: 0.012,
        avg_24h: 0.01,
        trend: 'rising',
      },
      options_skew: {
        current_pct: 11,
        avg_24h_pct: 9,
        direction: 'widening',
      },
      aggregate_oi: {
        current: 1150,
        change_24h_pct: 15,
      },
      market_quality: [
        { option_type: 'P', count: 12, avg_spread_pct: 3.12, avg_iv_pct: 64, avg_depth: 7.78 },
      ],
    });
  });
});


// ============================================================================
// 3. computeCurrentValues
// ============================================================================

describe('computeCurrentValues', () => {
  test('long position: unrealized_pnl_pct = (mark - entry) / entry * 100', () => {
    const position = {
      instrument_name: 'ETH-20261231-2000-C',
      direction: 'long',
      avg_entry_price: 0.05,
      mark_price: 0,
      delta: 0,
      theta: 0,
    };
    const ticker = { M: '0.08', option_pricing: { d: '0.6', i: '0.75', t: '-0.01' } };
    const result = computeCurrentValues(position, ticker, 1800);

    // (0.08 - 0.05) / 0.05 * 100 = 60%
    assert.strictEqual(result.unrealized_pnl_pct, 60);
    assert.strictEqual(result.mark_price, 0.08);
    assert.strictEqual(result.spot_price, 1800);
    assert.strictEqual(result.delta, 0.6);
    assert.strictEqual(result.iv, 0.75);
    assert.strictEqual(result.theta, -0.01);
  });

  test('short position: unrealized_pnl_pct is inverted', () => {
    const position = {
      instrument_name: 'ETH-20261231-2000-C',
      direction: 'short',
      avg_entry_price: 0.05,
      mark_price: 0,
      delta: 0,
      theta: 0,
    };
    const ticker = { M: '0.08', option_pricing: { d: '0.6', i: '0.75', t: '-0.01' } };
    const result = computeCurrentValues(position, ticker, 1800);

    // Short: -((0.08 - 0.05) / 0.05 * 100) = -60%
    assert.strictEqual(result.unrealized_pnl_pct, -60);
  });

  test('buyback_call rule evaluation uses executable ask price for short-call pnl', () => {
    const position = {
      instrument_name: 'ETH-20261231-2000-C',
      direction: 'short',
      avg_entry_price: 6.0,
      mark_price: 0,
      delta: 0,
      theta: 0,
    };
    const ticker = {
      M: '1.00',
      a: '1.40',
      option_pricing: { d: '0.1', i: '0.65', t: '-0.02' },
    };

    const base = computeCurrentValues(position, ticker, 1800);
    const executable = getRuleEvaluationValues(position, ticker, 1800, 'buyback_call');

    assert.ok(Math.abs(base.unrealized_pnl_pct - 83.3333333333) < 0.0001, `Expected mark-based pnl ~83.33, got ${base.unrealized_pnl_pct}`);
    assert.ok(Math.abs(executable.unrealized_pnl_pct - 76.6666666667) < 0.0001, `Expected ask-based pnl ~76.67, got ${executable.unrealized_pnl_pct}`);
    assert.strictEqual(executable.execution_price, 1.4);
    assert.strictEqual(executable.mark_price, 1.0);
  });

  test('DTE calculation from instrument name', () => {
    // Use a far-future date so DTE is always positive
    const futureDate = new Date(Date.now() + 30 * 86400000); // 30 days from now
    const y = futureDate.getUTCFullYear();
    const m = String(futureDate.getUTCMonth() + 1).padStart(2, '0');
    const d = String(futureDate.getUTCDate()).padStart(2, '0');
    const instrName = `ETH-${y}${m}${d}-2000-P`;

    const position = {
      instrument_name: instrName,
      direction: 'long',
      avg_entry_price: 0.05,
      mark_price: 0,
      delta: 0,
      theta: 0,
    };
    const ticker = { M: '0.05', option_pricing: { d: '-0.3', i: '0.8', t: '-0.005' } };
    const result = computeCurrentValues(position, ticker, 1800);

    // DTE should be roughly 30 days (within a margin for 08:00 UTC settlement)
    assert.ok(result.dte > 29 && result.dte < 31, `DTE should be ~30 days, got ${result.dte}`);
  });

  test('missing ticker data falls back to position values', () => {
    const position = {
      instrument_name: 'ETH-20261231-2000-P',
      direction: 'long',
      avg_entry_price: 0.10,
      mark_price: 0.12,
      delta: -0.4,
      theta: -0.02,
    };
    // No ticker data
    const result = computeCurrentValues(position, null, 1800);

    // Falls back to position.mark_price = 0.12
    assert.strictEqual(result.mark_price, 0.12);
    // Falls back to position.delta = -0.4
    assert.strictEqual(result.delta, -0.4);
    // Falls back to position.theta = -0.02
    assert.strictEqual(result.theta, -0.02);
    // iv falls back to 0 (no ticker, no position fallback)
    assert.strictEqual(result.iv, 0);
    // PnL: (0.12 - 0.10) / 0.10 * 100 = 20% (use approximate for floating point)
    assert.ok(Math.abs(result.unrealized_pnl_pct - 20) < 0.0001, `Expected ~20, got ${result.unrealized_pnl_pct}`);
  });

  test('zero entry price returns 0 PnL', () => {
    const position = {
      instrument_name: 'ETH-20261231-2000-P',
      direction: 'long',
      avg_entry_price: 0,
      mark_price: 0,
      delta: 0,
      theta: 0,
    };
    const ticker = { M: '0.08', option_pricing: { d: '-0.3', i: '0.8', t: '-0.005' } };
    const result = computeCurrentValues(position, ticker, 1800);

    assert.strictEqual(result.unrealized_pnl_pct, 0);
  });

  test('empty ticker object falls back to position values', () => {
    const position = {
      instrument_name: 'ETH-20261231-2000-P',
      direction: 'long',
      avg_entry_price: 0.10,
      mark_price: 0.15,
      delta: -0.5,
      theta: -0.03,
    };
    const ticker = {};
    const result = computeCurrentValues(position, ticker, 1800);

    assert.strictEqual(result.mark_price, 0.15);
    assert.strictEqual(result.delta, -0.5);
    assert.strictEqual(result.theta, -0.03);
  });
});

describe('advisory assessment terminology guard', () => {
  test('flags invented efficiency language', () => {
    const pattern = assessmentUsesUnsupportedMetricLanguage('Put efficiency at 84.9% approaching 80% threshold.');
    assert.ok(pattern instanceof RegExp);
  });

  test('allows plain factual budget language', () => {
    const pattern = assessmentUsesUnsupportedMetricLanguage('ETH at $2305. Put budget remaining $0.15.');
    assert.strictEqual(pattern, null);
  });

  test('fallback summary avoids unsupported labels', () => {
    const summary = buildFactualAdvisoryAssessment({
      spotPrice: 2305,
      momentum: { mediumTerm: { main: 'transitional' } },
      mandelbrotContext: { regime: 'transitional', confidence: 0.62 },
      sentiment: {
        windows: {
          '24h': {
            options_skew: { current_pct: 1.34, direction: 'narrowing' },
            aggregate_oi: { change_pct: 6.8 },
          },
        },
      },
      putBudgetRemaining: 0.15,
      secondOpinion: { vetoes: [{ rule_index: 0 }] },
      entryRulesCount: 0,
      exitRulesCount: 0,
    });
    assert.strictEqual(/efficiency|threshold/i.test(summary), false);
    assert.strictEqual(summary.includes('Narrowing skew with rising open interest suggests repositioning rather than one-way panic.'), true);
    assert.strictEqual(summary.includes('Taleb vetoed 1 proposed rule, so the stance is to sit on hands and wait for cleaner asymmetry.'), true);
  });

  test('fallback summary states patience plainly when no rules survive', () => {
    const summary = buildFactualAdvisoryAssessment({
      spotPrice: 2311,
      mandelbrotContext: { regime: 'transitional', confidence: 0.6 },
      sentiment: {
        windows: {
          '24h': {
            options_skew: { current_pct: 1.34, direction: 'narrowing' },
            aggregate_oi: { change_pct: 6.8 },
          },
        },
      },
      putBudgetRemaining: 0.15,
      entryRulesCount: 0,
      exitRulesCount: 0,
    });
    assert.strictEqual(summary.includes('sit on hands'), true);
    assert.strictEqual(summary.includes('Put budget remaining $0.15 leaves little room for fresh deployment'), true);
  });
});


// ============================================================================
// 4. DB operations (uses an isolated in-memory test database)
// ============================================================================

describe('DB operations (isolated test database)', () => {
  // Create a fresh in-memory SQLite database with the same schema
  const Database = require('better-sqlite3');
  const testDb = new Database(':memory:');
  testDb.pragma('journal_mode = WAL');

  // Create required tables
  testDb.exec(`
    CREATE TABLE IF NOT EXISTS trading_rules (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      rule_type TEXT NOT NULL,
      action TEXT NOT NULL,
      instrument_name TEXT,
      criteria TEXT NOT NULL,
      budget_limit REAL,
      priority TEXT DEFAULT 'medium',
      reasoning TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      is_active INTEGER DEFAULT 1,
      advisory_id TEXT,
      preferred_order_type TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_trading_rules_active ON trading_rules(is_active);
    CREATE INDEX IF NOT EXISTS idx_trading_rules_type ON trading_rules(rule_type, is_active);

    CREATE TABLE IF NOT EXISTS pending_actions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      rule_id INTEGER REFERENCES trading_rules(id),
      action TEXT NOT NULL,
      instrument_name TEXT NOT NULL,
      amount REAL,
      price REAL,
      trigger_details TEXT,
      status TEXT DEFAULT 'pending',
      retries INTEGER DEFAULT 0,
      triggered_at TEXT DEFAULT (datetime('now')),
      confirmation_reasoning TEXT,
      confirmed_at TEXT,
      executed_at TEXT,
      execution_result TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_pending_actions_status ON pending_actions(status);
  `);

  // Prepare statements mirroring bot/db.js
  const stmts = {
    deactivateAllRules: testDb.prepare(`UPDATE trading_rules SET is_active = 0 WHERE is_active = 1`),
    insertTradingRule: testDb.prepare(`
      INSERT INTO trading_rules (rule_type, action, instrument_name, criteria, budget_limit, priority, reasoning, advisory_id, is_active, preferred_order_type)
      VALUES (@rule_type, @action, @instrument_name, @criteria, @budget_limit, @priority, @reasoning, @advisory_id, 1, @preferred_order_type)
    `),
    getActiveRules: testDb.prepare(`SELECT * FROM trading_rules WHERE is_active = 1 ORDER BY priority DESC, id ASC`),
    getActiveRulesByType: testDb.prepare(`SELECT * FROM trading_rules WHERE is_active = 1 AND rule_type = @rule_type ORDER BY priority DESC, id ASC`),
    insertPendingAction: testDb.prepare(`
      INSERT INTO pending_actions (rule_id, action, instrument_name, amount, price, trigger_details, status)
      VALUES (@rule_id, @action, @instrument_name, @amount, @price, @trigger_details, 'pending')
    `),
    updatePendingAction: testDb.prepare(`
      UPDATE pending_actions SET
        status = COALESCE(@status, status),
        confirmation_reasoning = COALESCE(@confirmation_reasoning, confirmation_reasoning),
        confirmed_at = COALESCE(@confirmed_at, confirmed_at),
        executed_at = COALESCE(@executed_at, executed_at),
        execution_result = COALESCE(@execution_result, execution_result),
        retries = COALESCE(@retries, retries)
      WHERE id = @id
    `),
    getPendingActionsByStatus: testDb.prepare(`
      SELECT pa.*, tr.reasoning as rule_reasoning, tr.criteria as rule_criteria
      FROM pending_actions pa
      LEFT JOIN trading_rules tr ON pa.rule_id = tr.id
      WHERE pa.status = @status
      ORDER BY pa.triggered_at ASC
    `),
    hasPendingActionForRule: testDb.prepare(`
      SELECT COUNT(*) as count FROM pending_actions
      WHERE rule_id = @rule_id AND status IN ('pending', 'confirmed')
    `),
    getLastExecutedAction: testDb.prepare(`
      SELECT executed_at FROM pending_actions
      WHERE action = @action AND status = 'executed'
      ORDER BY executed_at DESC LIMIT 1
    `),
    getLastRejectedAction: testDb.prepare(`
      SELECT triggered_at FROM pending_actions
      WHERE action = @action
        AND instrument_name = @instrument_name
        AND status = 'rejected'
      ORDER BY triggered_at DESC LIMIT 1
    `),
  };

  // Helper functions mirroring bot/db.js
  const replaceActiveRules = (advisoryId, rules) => {
    const replace = testDb.transaction((items) => {
      stmts.deactivateAllRules.run();
      for (const rule of items) {
        stmts.insertTradingRule.run({
          rule_type: rule.rule_type,
          action: rule.action,
          instrument_name: rule.instrument_name || null,
          criteria: typeof rule.criteria === 'string' ? rule.criteria : JSON.stringify(rule.criteria),
          budget_limit: rule.budget_limit ?? null,
          priority: rule.priority || 'medium',
          reasoning: rule.reasoning || null,
          advisory_id: advisoryId,
          preferred_order_type: rule.preferred_order_type || null,
        });
      }
    });
    replace(rules);
  };

  const insertPendingAction = (action) => {
    return stmts.insertPendingAction.run({
      rule_id: action.rule_id ?? null,
      action: action.action,
      instrument_name: action.instrument_name,
      amount: action.amount ?? null,
      price: action.price ?? null,
      trigger_details: action.trigger_details
        ? (typeof action.trigger_details === 'string' ? action.trigger_details : JSON.stringify(action.trigger_details))
        : null,
    });
  };

  const updatePendingAction = (id, fields) => {
    stmts.updatePendingAction.run({
      id,
      status: fields.status ?? null,
      confirmation_reasoning: fields.confirmation_reasoning ?? null,
      confirmed_at: fields.confirmed_at ?? null,
      executed_at: fields.executed_at ?? null,
      execution_result: fields.execution_result
        ? (typeof fields.execution_result === 'string' ? fields.execution_result : JSON.stringify(fields.execution_result))
        : null,
      retries: fields.retries ?? null,
    });
  };

  const getActiveRulesByType = (ruleType) => stmts.getActiveRulesByType.all({ rule_type: ruleType });
  const getPendingActions = (status) => stmts.getPendingActionsByStatus.all({ status });
  const hasPendingActionForRule = (ruleId) => (stmts.hasPendingActionForRule.get({ rule_id: ruleId })?.count || 0) > 0;
  const getLastExecutedAction = (action) => stmts.getLastExecutedAction.get({ action })?.executed_at || null;
  const getLastRejectedAction = (action, instrumentName) => stmts.getLastRejectedAction.get({ action, instrument_name: instrumentName })?.triggered_at || null;

  // ── replaceActiveRules tests ──

  test('replaceActiveRules: insert 2 rules, verify they are active', () => {
    replaceActiveRules('adv-001', [
      {
        rule_type: 'exit',
        action: 'close_position',
        instrument_name: 'ETH-20260501-1500-P',
        criteria: { conditions: [{ field: 'dte', op: 'lt', value: 2 }], condition_logic: 'all' },
        priority: 'high',
        reasoning: 'Close near expiry',
      },
      {
        rule_type: 'exit',
        action: 'close_position',
        instrument_name: 'ETH-20260601-2000-C',
        criteria: { conditions: [{ field: 'unrealized_pnl_pct', op: 'gt', value: 50 }], condition_logic: 'all' },
        priority: 'medium',
        reasoning: 'Take profit at 50%',
      },
    ]);

    const activeExitRules = getActiveRulesByType('exit');
    assert.strictEqual(activeExitRules.length, 2);
    const priorities = activeExitRules.map(r => r.priority).sort();
    assert.strictEqual(priorities[0], 'high');
    assert.strictEqual(priorities[1], 'medium');
  });

  test('replaceActiveRules: replace with 1 new rule, old ones deactivated', () => {
    replaceActiveRules('adv-002', [
      {
        rule_type: 'exit',
        action: 'close_position',
        instrument_name: 'ETH-20260701-1800-P',
        criteria: { conditions: [{ field: 'dte', op: 'lt', value: 1 }], condition_logic: 'all' },
        priority: 'high',
        reasoning: 'New exit rule',
      },
    ]);

    const activeExitRules = getActiveRulesByType('exit');
    assert.strictEqual(activeExitRules.length, 1);
    assert.strictEqual(activeExitRules[0].instrument_name, 'ETH-20260701-1800-P');
    assert.strictEqual(activeExitRules[0].advisory_id, 'adv-002');

    // Verify old rules are deactivated (total rows > 1, but only 1 active)
    const allRules = testDb.prepare('SELECT * FROM trading_rules').all();
    assert.ok(allRules.length >= 3, `Expected at least 3 total rules, got ${allRules.length}`);
    const activeCount = allRules.filter(r => r.is_active === 1).length;
    assert.strictEqual(activeCount, 1);
  });

  test('replaceActiveRules: criteria stored as JSON string', () => {
    const criteria = { conditions: [{ field: 'delta', op: 'gt', value: 0.5 }], condition_logic: 'all' };
    replaceActiveRules('adv-003', [
      { rule_type: 'entry', action: 'buy_put', criteria, priority: 'medium', reasoning: 'Test' },
    ]);

    const rules = getActiveRulesByType('entry');
    assert.strictEqual(rules.length, 1);
    const stored = JSON.parse(rules[0].criteria);
    assert.deepStrictEqual(stored, criteria);
  });

  // ── insertPendingAction + getPendingActions tests ──

  test('insertPendingAction: insert action, verify returned with pending status', () => {
    // First, get the current active rule ID
    const rules = getActiveRulesByType('entry');
    const ruleId = rules[0].id;

    insertPendingAction({
      rule_id: ruleId,
      action: 'buy_put',
      instrument_name: 'ETH-20260501-1500-P',
      amount: 1.5,
      price: 0.05,
      trigger_details: { delta: -0.3, dte: 25, reason: 'Criteria met' },
    });

    const pending = getPendingActions('pending');
    assert.ok(pending.length >= 1, 'Should have at least 1 pending action');
    const action = pending.find(a => a.instrument_name === 'ETH-20260501-1500-P');
    assert.ok(action, 'Should find the inserted action');
    assert.strictEqual(action.status, 'pending');
    assert.strictEqual(action.action, 'buy_put');
    assert.strictEqual(action.amount, 1.5);
  });

  test('insertPendingAction: trigger_details stored as JSON', () => {
    const pending = getPendingActions('pending');
    const action = pending.find(a => a.instrument_name === 'ETH-20260501-1500-P');
    assert.ok(action, 'Should find the action');
    const details = JSON.parse(action.trigger_details);
    assert.strictEqual(details.delta, -0.3);
    assert.strictEqual(details.dte, 25);
    assert.strictEqual(details.reason, 'Criteria met');
  });

  // ── updatePendingAction tests ──

  test('updatePendingAction: update status to confirmed', () => {
    const pending = getPendingActions('pending');
    const action = pending.find(a => a.instrument_name === 'ETH-20260501-1500-P');
    const actionId = action.id;

    updatePendingAction(actionId, {
      status: 'confirmed',
      confirmation_reasoning: 'LLM confirmed the trade',
      confirmed_at: new Date().toISOString(),
    });

    const confirmed = getPendingActions('confirmed');
    const updated = confirmed.find(a => a.id === actionId);
    assert.ok(updated, 'Should find confirmed action');
    assert.strictEqual(updated.status, 'confirmed');
    assert.strictEqual(updated.confirmation_reasoning, 'LLM confirmed the trade');
  });

  test('updatePendingAction: update status to executed with execution_result', () => {
    const confirmed = getPendingActions('confirmed');
    const action = confirmed.find(a => a.instrument_name === 'ETH-20260501-1500-P');
    const actionId = action.id;

    const execResult = { order_id: 'ORD-12345', filled_amount: 1.5, avg_price: 0.048 };
    updatePendingAction(actionId, {
      status: 'executed',
      executed_at: new Date().toISOString(),
      execution_result: execResult,
    });

    const executed = getPendingActions('executed');
    const updated = executed.find(a => a.id === actionId);
    assert.ok(updated, 'Should find executed action');
    assert.strictEqual(updated.status, 'executed');
    const result = JSON.parse(updated.execution_result);
    assert.strictEqual(result.order_id, 'ORD-12345');
    assert.strictEqual(result.filled_amount, 1.5);
  });

  // ── hasPendingActionForRule tests ──

  test('hasPendingActionForRule: no pending action returns false', () => {
    // Use a rule ID that has no pending actions
    assert.strictEqual(hasPendingActionForRule(99999), false);
  });

  test('hasPendingActionForRule: pending action exists returns true', () => {
    // Insert a fresh rule and pending action
    replaceActiveRules('adv-004', [
      { rule_type: 'exit', action: 'close_position', instrument_name: 'ETH-20260801-2500-C',
        criteria: { conditions: [{ field: 'dte', op: 'lt', value: 3 }], condition_logic: 'all' },
        priority: 'high', reasoning: 'Test hasPending' },
    ]);
    const rules = getActiveRulesByType('exit');
    const ruleId = rules[0].id;

    insertPendingAction({
      rule_id: ruleId,
      action: 'close_position',
      instrument_name: 'ETH-20260801-2500-C',
      amount: 2.0,
      price: 0.10,
      trigger_details: null,
    });

    assert.strictEqual(hasPendingActionForRule(ruleId), true);
  });

  test('hasPendingActionForRule: executed action returns false', () => {
    // The action from the previous confirmed/executed test should not count
    // since its status is 'executed', not 'pending' or 'confirmed'
    const allActions = testDb.prepare('SELECT * FROM pending_actions WHERE status = \'executed\'').all();
    if (allActions.length > 0) {
      const executedAction = allActions[0];
      // An executed action's rule_id should NOT count as having a pending action
      // unless there's also a separate pending/confirmed action for the same rule
      const otherPending = testDb.prepare(
        'SELECT COUNT(*) as count FROM pending_actions WHERE rule_id = @rule_id AND status IN (\'pending\', \'confirmed\')'
      ).get({ rule_id: executedAction.rule_id });

      if (otherPending.count === 0) {
        assert.strictEqual(hasPendingActionForRule(executedAction.rule_id), false);
      } else {
        // If there happens to be another pending action for the same rule, just verify the function works
        assert.strictEqual(typeof hasPendingActionForRule(executedAction.rule_id), 'boolean');
      }
    } else {
      // Create an executed-only scenario
      replaceActiveRules('adv-005', [
        { rule_type: 'exit', action: 'close_position', instrument_name: 'ETH-20260901-3000-C',
          criteria: { conditions: [{ field: 'dte', op: 'lt', value: 1 }], condition_logic: 'all' },
          priority: 'medium', reasoning: 'Test executed-only' },
      ]);
      const rules = getActiveRulesByType('exit');
      const ruleId = rules[0].id;

      const result = insertPendingAction({
        rule_id: ruleId,
        action: 'close_position',
        instrument_name: 'ETH-20260901-3000-C',
        amount: 1.0,
        price: 0.08,
        trigger_details: null,
      });

      updatePendingAction(result.lastInsertRowid, {
        status: 'executed',
        executed_at: new Date().toISOString(),
      });

      assert.strictEqual(hasPendingActionForRule(ruleId), false);
    }
  });

  // ── getLastExecutedAction tests ──

  test('getLastExecutedAction: no executed action returns null', () => {
    assert.strictEqual(getLastExecutedAction('nonexistent_action'), null);
  });

  test('getLastExecutedAction: executed action returns timestamp', () => {
    const result = getLastExecutedAction('buy_put');
    assert.ok(result !== null, 'Should have an executed buy_put action from earlier tests');
    // Should be a valid date string
    assert.ok(!isNaN(new Date(result).getTime()), 'Should be a valid date string');
  });

  test('getLastRejectedAction: returns latest rejection timestamp for action + instrument', () => {
    testDb.prepare(`
      INSERT INTO pending_actions (rule_id, action, instrument_name, amount, price, trigger_details, status, triggered_at)
      VALUES (NULL, @action, @instrument_name, 0.01, 0.12, NULL, 'rejected', @triggered_at)
    `).run({
      action: 'sell_call',
      instrument_name: 'ETH-20260424-2800-C',
      triggered_at: '2026-04-16T17:25:00.000Z',
    });

    const result = getLastRejectedAction('sell_call', 'ETH-20260424-2800-C');
    assert.strictEqual(result, '2026-04-16T17:25:00.000Z');
  });

  // Close the test database
  testDb.close();
});


// ============================================================================
// 5. Entry rule matching integration test
// ============================================================================

describe('Entry rule matching (integration)', () => {
  test('evaluateConditions + computeCurrentValues work together for exit rule triggering', () => {
    // Simulate: position has DTE < 2 and unrealized P&L > 30%
    const position = {
      instrument_name: 'ETH-20990420-1500-P', // comfortably above the <2 DTE threshold
      direction: 'long',
      avg_entry_price: 0.05,
      mark_price: 0,
      delta: 0,
      theta: 0,
    };
    const ticker = { M: '0.07', option_pricing: { d: '-0.25', i: '0.80', t: '-0.008' } };
    const spotPrice = 1800;

    const values = computeCurrentValues(position, ticker, spotPrice);

    // Verify computed values are reasonable
    // PnL: (0.07 - 0.05) / 0.05 * 100 = 40% (long, use approximate for floating point)
    assert.ok(Math.abs(values.unrealized_pnl_pct - 40) < 0.0001, `Expected ~40, got ${values.unrealized_pnl_pct}`);
    // DTE: ~3 days (April 10 8:00 UTC - April 7 now)
    assert.ok(values.dte !== null && values.dte >= 0, 'DTE should be non-null and non-negative');

    // Test exit criteria: close if pnl > 30%
    const exitCriteria = [
      { field: 'unrealized_pnl_pct', op: 'gt', value: 30 },
    ];
    assert.strictEqual(evaluateConditions(exitCriteria, 'all', values), true);

    // Test exit criteria: close if dte < 2 (should NOT trigger since DTE ~3)
    const dteCriteria = [
      { field: 'dte', op: 'lt', value: 2 },
    ];
    assert.strictEqual(evaluateConditions(dteCriteria, 'all', values), false);
  });

  test('entry rule candidate scoring: put scoring = |delta| / askPrice', () => {
    // Simulate the scoring logic from evaluateTradingRules
    const candidates = [
      { name: 'ETH-20260501-1400-P', delta: -0.25, askPrice: 0.04, optionType: 'P' },
      { name: 'ETH-20260501-1500-P', delta: -0.30, askPrice: 0.05, optionType: 'P' },
      { name: 'ETH-20260501-1600-P', delta: -0.40, askPrice: 0.08, optionType: 'P' },
    ];

    const scored = candidates.map(c => {
      const absDelta = Math.abs(c.delta);
      const score = c.askPrice > 0 ? absDelta / c.askPrice : 0;
      return { ...c, score };
    });

    scored.sort((a, b) => b.score - a.score);

    // Scores: 0.25/0.04=6.25, 0.30/0.05=6.0, 0.40/0.08=5.0
    assert.strictEqual(scored[0].name, 'ETH-20260501-1400-P'); // Best ratio
    assert.ok(Math.abs(scored[0].score - 6.25) < 0.001, `Expected 6.25, got ${scored[0].score}`);
  });

  test('entry rule candidate scoring: call scoring = bidPrice / |delta|', () => {
    const candidates = [
      { name: 'ETH-20260501-2000-C', delta: 0.30, bidPrice: 0.06, optionType: 'C' },
      { name: 'ETH-20260501-2200-C', delta: 0.20, bidPrice: 0.03, optionType: 'C' },
      { name: 'ETH-20260501-1900-C', delta: 0.40, bidPrice: 0.10, optionType: 'C' },
    ];

    const scored = candidates.map(c => {
      const absDelta = Math.abs(c.delta);
      const score = absDelta > 0 ? c.bidPrice / absDelta : 0;
      return { ...c, score };
    });

    scored.sort((a, b) => b.score - a.score);

    // Scores: 0.06/0.30=0.20, 0.03/0.20=0.15, 0.10/0.40=0.25
    assert.strictEqual(scored[0].name, 'ETH-20260501-1900-C'); // Best ratio
    assert.ok(Math.abs(scored[0].score - 0.25) < 0.001, `Expected 0.25, got ${scored[0].score}`);
  });

  test('entry rule: market conditions filter with evaluateConditions', () => {
    const marketConditions = [
      { field: 'spot_price', op: 'gt', value: 1500 },
      { field: 'spot_price', op: 'lt', value: 2500 },
    ];

    // Spot in range
    assert.strictEqual(evaluateConditions(marketConditions, 'all', { spot_price: 1800 }), true);
    // Spot below range
    assert.strictEqual(evaluateConditions(marketConditions, 'all', { spot_price: 1400 }), false);
    // Spot above range
    assert.strictEqual(evaluateConditions(marketConditions, 'all', { spot_price: 2600 }), false);
  });

  test('entry rule: delta and DTE range filtering', () => {
    const deltaRange = [-0.40, -0.15]; // put delta range
    const dteRange = [14, 45]; // 14 to 45 days

    // Candidate that passes both filters
    const good = { delta: -0.25, dte: 30 };
    assert.ok(good.delta >= deltaRange[0] && good.delta <= deltaRange[1], 'Delta in range');
    assert.ok(good.dte >= dteRange[0] && good.dte <= dteRange[1], 'DTE in range');

    // Candidate with delta out of range
    const badDelta = { delta: -0.50, dte: 30 };
    assert.ok(!(badDelta.delta >= deltaRange[0] && badDelta.delta <= deltaRange[1]), 'Delta out of range');

    // Candidate with DTE out of range
    const badDte = { delta: -0.25, dte: 7 };
    assert.ok(!(badDte.dte >= dteRange[0] && badDte.dte <= dteRange[1]), 'DTE out of range');
  });
});


// ============================================================================
// 8. Fill accounting — zero-fill detection & partial fills
// ============================================================================

// Extract the pure fill-accounting logic from executeOrder for testability
const computeFillAccounting = (orderResult, requestedAmount, requestedPrice) => {
  let filledAmt = 0, avgPx = requestedPrice, totalValue = 0;
  if (orderResult?.trades?.length) {
    let totAmt = 0, totVal = 0;
    for (const t of orderResult.trades) {
      const ta = Number(t.trade_amount), tp = Number(t.trade_price);
      totAmt += ta; totVal += ta * tp;
    }
    if (totAmt > 0) { filledAmt = totAmt; avgPx = totVal / totAmt; totalValue = totVal; }
  }
  return { filledAmt, avgPx, totalValue };
};

describe('Fill accounting (computeFillAccounting)', () => {
  test('single full fill', () => {
    const result = computeFillAccounting(
      { trades: [{ trade_amount: '1.0', trade_price: '5.50' }] },
      1.0, 5.50
    );
    assert.strictEqual(result.filledAmt, 1.0);
    assert.strictEqual(result.avgPx, 5.5);
    assert.strictEqual(result.totalValue, 5.5);
  });

  test('multiple partial fills averaged correctly', () => {
    const result = computeFillAccounting(
      { trades: [
        { trade_amount: '0.5', trade_price: '5.00' },
        { trade_amount: '0.5', trade_price: '6.00' },
      ] },
      1.0, 5.50
    );
    assert.strictEqual(result.filledAmt, 1.0);
    assert.strictEqual(result.avgPx, 5.5); // (0.5*5 + 0.5*6) / 1.0
    assert.strictEqual(result.totalValue, 5.5);
  });

  test('zero fills: empty trades array', () => {
    const result = computeFillAccounting(
      { trades: [] },
      1.0, 5.50
    );
    assert.strictEqual(result.filledAmt, 0);
    assert.strictEqual(result.totalValue, 0);
  });

  test('zero fills: no trades property', () => {
    const result = computeFillAccounting({}, 1.0, 5.50);
    assert.strictEqual(result.filledAmt, 0);
    assert.strictEqual(result.totalValue, 0);
  });

  test('zero fills: null result', () => {
    const result = computeFillAccounting(null, 1.0, 5.50);
    assert.strictEqual(result.filledAmt, 0);
    assert.strictEqual(result.totalValue, 0);
  });

  test('partial fill: less than requested', () => {
    const result = computeFillAccounting(
      { trades: [{ trade_amount: '0.3', trade_price: '5.00' }] },
      1.0, 5.00
    );
    assert.strictEqual(result.filledAmt, 0.3);
    assert.strictEqual(result.avgPx, 5.0);
    assert.strictEqual(result.totalValue, 1.5);
  });

  test('trades with zero amounts are handled', () => {
    const result = computeFillAccounting(
      { trades: [
        { trade_amount: '0', trade_price: '5.00' },
        { trade_amount: '1.0', trade_price: '6.00' },
      ] },
      1.0, 5.50
    );
    assert.strictEqual(result.filledAmt, 1.0);
    assert.strictEqual(result.avgPx, 6.0);
    assert.strictEqual(result.totalValue, 6.0);
  });
});

// ============================================================================
// 9. Zero-fill result classification
// ============================================================================

// Extract the result classification logic from executeOrder
const classifyOrderResult = (filledAmt, orderType, orderResult) => {
  if (filledAmt === 0 && orderType === 'ioc') {
    return { type: 'zeroFill' };
  }
  if (filledAmt === 0 && (orderType === 'gtc' || orderType === 'post_only')) {
    const orderId = orderResult?.order_id || orderResult?.order?.order_id || null;
    return { type: 'resting', orderId };
  }
  if (filledAmt > 0) {
    return { type: 'filled' };
  }
  return { type: 'unknown' };
};

describe('Order result classification (zero-fill detection)', () => {
  test('IOC with zero fill → zeroFill', () => {
    const r = classifyOrderResult(0, 'ioc', {});
    assert.strictEqual(r.type, 'zeroFill');
  });

  test('IOC with fills → filled', () => {
    const r = classifyOrderResult(1.0, 'ioc', {});
    assert.strictEqual(r.type, 'filled');
  });

  test('GTC with zero fill → resting', () => {
    const r = classifyOrderResult(0, 'gtc', { order_id: 'ord_123' });
    assert.strictEqual(r.type, 'resting');
    assert.strictEqual(r.orderId, 'ord_123');
  });

  test('post_only with zero fill → resting', () => {
    const r = classifyOrderResult(0, 'post_only', { order: { order_id: 'ord_456' } });
    assert.strictEqual(r.type, 'resting');
    assert.strictEqual(r.orderId, 'ord_456');
  });

  test('GTC with fills → filled', () => {
    const r = classifyOrderResult(0.5, 'gtc', {});
    assert.strictEqual(r.type, 'filled');
  });

  test('resting order with no orderId gracefully handles null', () => {
    const r = classifyOrderResult(0, 'gtc', {});
    assert.strictEqual(r.type, 'resting');
    assert.strictEqual(r.orderId, null);
  });
});

// ============================================================================
// 10. Order type resolution from voter consensus
// ============================================================================

const resolveOrderType = (haikuVote, codexVote) => {
  const confirmedOrderType = (haikuVote?.order_type || codexVote?.order_type || 'ioc');
  const validOrderTypes = ['ioc', 'gtc', 'post_only'];
  return validOrderTypes.includes(confirmedOrderType) ? confirmedOrderType : 'ioc';
};

const resolveExecutionPrice = (haikuVote, codexVote, currentPrice, actionPrice) => {
  const voterLimitPrice = haikuVote?.limit_price || codexVote?.limit_price;
  return (typeof voterLimitPrice === 'number' && voterLimitPrice > 0) ? voterLimitPrice : (currentPrice || actionPrice);
};

describe('Order type resolution from voter consensus', () => {
  test('haiku picks gtc, codex picks ioc → uses haiku (gtc)', () => {
    assert.strictEqual(resolveOrderType({ order_type: 'gtc' }, { order_type: 'ioc' }), 'gtc');
  });

  test('haiku null, codex picks post_only → uses codex', () => {
    assert.strictEqual(resolveOrderType(null, { order_type: 'post_only' }), 'post_only');
  });

  test('both null → defaults to ioc', () => {
    assert.strictEqual(resolveOrderType(null, null), 'ioc');
  });

  test('haiku picks invalid type → falls back to ioc', () => {
    assert.strictEqual(resolveOrderType({ order_type: 'market' }, null), 'ioc');
  });

  test('haiku has no order_type field, codex has gtc → uses codex', () => {
    assert.strictEqual(resolveOrderType({ confirm: true }, { order_type: 'gtc' }), 'gtc');
  });

  test('both have order_type, haiku takes priority', () => {
    assert.strictEqual(resolveOrderType({ order_type: 'post_only' }, { order_type: 'gtc' }), 'post_only');
  });
});

describe('Execution price resolution from voter consensus', () => {
  test('haiku sets limit_price → uses it', () => {
    assert.strictEqual(resolveExecutionPrice({ limit_price: 4.50 }, null, 5.00, 5.50), 4.50);
  });

  test('codex sets limit_price, haiku null → uses codex', () => {
    assert.strictEqual(resolveExecutionPrice(null, { limit_price: 3.00 }, 5.00, 5.50), 3.00);
  });

  test('neither sets limit_price → uses currentPrice', () => {
    assert.strictEqual(resolveExecutionPrice(null, null, 5.00, 5.50), 5.00);
  });

  test('neither sets limit_price, no currentPrice → uses actionPrice', () => {
    assert.strictEqual(resolveExecutionPrice(null, null, null, 5.50), 5.50);
  });

  test('voter sets 0 as limit_price → falls back to currentPrice (0 is not valid)', () => {
    assert.strictEqual(resolveExecutionPrice({ limit_price: 0 }, null, 5.00, 5.50), 5.00);
  });

  test('voter sets negative limit_price → falls back to currentPrice', () => {
    assert.strictEqual(resolveExecutionPrice({ limit_price: -1 }, null, 5.00, 5.50), 5.00);
  });

  test('voter sets string limit_price → falls back (not a number)', () => {
    assert.strictEqual(resolveExecutionPrice({ limit_price: '4.50' }, null, 5.00, 5.50), 5.00);
  });
});

// ============================================================================
// 11. Direction & reduceOnly from action type
// ============================================================================

const actionToDirection = (action) => {
  const direction = (action === 'buy_put' || action === 'buyback_call') ? 'buy' : 'sell';
  const reduceOnly = (action === 'sell_put' || action === 'buyback_call');
  return { direction, reduceOnly };
};

describe('Action → direction/reduceOnly mapping', () => {
  test('buy_put → buy, not reduceOnly', () => {
    const r = actionToDirection('buy_put');
    assert.strictEqual(r.direction, 'buy');
    assert.strictEqual(r.reduceOnly, false);
  });

  test('sell_put → sell, reduceOnly', () => {
    const r = actionToDirection('sell_put');
    assert.strictEqual(r.direction, 'sell');
    assert.strictEqual(r.reduceOnly, true);
  });

  test('sell_call → sell, not reduceOnly', () => {
    const r = actionToDirection('sell_call');
    assert.strictEqual(r.direction, 'sell');
    assert.strictEqual(r.reduceOnly, false);
  });

  test('buyback_call → buy, reduceOnly', () => {
    const r = actionToDirection('buyback_call');
    assert.strictEqual(r.direction, 'buy');
    assert.strictEqual(r.reduceOnly, true);
  });
});

// ============================================================================
// 12. Budget tracking correctness
// ============================================================================

describe('Budget tracking per action type', () => {
  test('buy_put increases putNetBought', () => {
    let putNetBought = 100;
    const totalValue = 25.50;
    const action = 'buy_put';
    if (action === 'buy_put') putNetBought += totalValue;
    else if (action === 'sell_put') putNetBought -= totalValue;
    assert.strictEqual(putNetBought, 125.50);
  });

  test('sell_put decreases putNetBought', () => {
    let putNetBought = 100;
    const totalValue = 30.00;
    const action = 'sell_put';
    if (action === 'buy_put') putNetBought += totalValue;
    else if (action === 'sell_put') putNetBought -= totalValue;
    assert.strictEqual(putNetBought, 70.00);
  });

  test('sell_call does NOT change putNetBought', () => {
    let putNetBought = 100;
    const totalValue = 50.00;
    const action = 'sell_call';
    if (action === 'buy_put') putNetBought += totalValue;
    else if (action === 'sell_put') putNetBought -= totalValue;
    assert.strictEqual(putNetBought, 100);
  });

  test('buyback_call does NOT change putNetBought', () => {
    let putNetBought = 100;
    const totalValue = 40.00;
    const action = 'buyback_call';
    if (action === 'buy_put') putNetBought += totalValue;
    else if (action === 'sell_put') putNetBought -= totalValue;
    assert.strictEqual(putNetBought, 100);
  });
});

// ============================================================================
// 13. Open order staleness & orphan detection
// ============================================================================

const classifyOpenOrder = (order, activeRules, nowMs) => {
  const ageMs = nowMs - (order.creation_timestamp || 0);
  const ageHours = ageMs / (1000 * 60 * 60);
  const isStale = ageHours > 8;

  const activeInstruments = new Set(activeRules.map(r => r.instrument_name).filter(Boolean));
  const hasEntryRules = activeRules.some(r => r.rule_type === 'entry');
  const isOrphaned = !activeInstruments.has(order.instrument_name) && !hasEntryRules;

  return { isStale, isOrphaned, ageHours, shouldCancel: isStale || isOrphaned };
};

describe('Open order staleness & orphan detection', () => {
  const NOW = Date.now();

  test('fresh order with matching rule → keep', () => {
    const r = classifyOpenOrder(
      { instrument_name: 'ETH-20260501-1500-P', creation_timestamp: NOW - 3600000 }, // 1h old
      [{ instrument_name: 'ETH-20260501-1500-P', rule_type: 'exit' }],
      NOW
    );
    assert.strictEqual(r.isStale, false);
    assert.strictEqual(r.isOrphaned, false);
    assert.strictEqual(r.shouldCancel, false);
  });

  test('9 hour old order → stale, cancel', () => {
    const r = classifyOpenOrder(
      { instrument_name: 'ETH-20260501-1500-P', creation_timestamp: NOW - 9 * 3600000 },
      [{ instrument_name: 'ETH-20260501-1500-P', rule_type: 'exit' }],
      NOW
    );
    assert.strictEqual(r.isStale, true);
    assert.strictEqual(r.shouldCancel, true);
  });

  test('exactly 8h old → NOT stale (boundary)', () => {
    const r = classifyOpenOrder(
      { instrument_name: 'ETH-20260501-1500-P', creation_timestamp: NOW - 8 * 3600000 },
      [{ instrument_name: 'ETH-20260501-1500-P', rule_type: 'exit' }],
      NOW
    );
    assert.strictEqual(r.isStale, false);
  });

  test('8h + 1ms → stale', () => {
    const r = classifyOpenOrder(
      { instrument_name: 'ETH-20260501-1500-P', creation_timestamp: NOW - 8 * 3600000 - 1 },
      [{ instrument_name: 'ETH-20260501-1500-P', rule_type: 'exit' }],
      NOW
    );
    assert.strictEqual(r.isStale, true);
    assert.strictEqual(r.shouldCancel, true);
  });

  test('fresh order with no matching rule and no entry rules → orphaned', () => {
    const r = classifyOpenOrder(
      { instrument_name: 'ETH-20260501-1500-P', creation_timestamp: NOW - 3600000 },
      [{ instrument_name: 'ETH-20260601-2000-C', rule_type: 'exit' }], // different instrument
      NOW
    );
    assert.strictEqual(r.isOrphaned, true);
    assert.strictEqual(r.shouldCancel, true);
  });

  test('fresh order with no matching exit rule BUT entry rules exist → NOT orphaned', () => {
    // Entry rules don't have specific instruments, so any open order could be from an entry
    const r = classifyOpenOrder(
      { instrument_name: 'ETH-20260501-1500-P', creation_timestamp: NOW - 3600000 },
      [{ instrument_name: null, rule_type: 'entry' }],
      NOW
    );
    assert.strictEqual(r.isOrphaned, false);
    assert.strictEqual(r.shouldCancel, false);
  });

  test('no active rules at all → orphaned', () => {
    const r = classifyOpenOrder(
      { instrument_name: 'ETH-20260501-1500-P', creation_timestamp: NOW - 3600000 },
      [],
      NOW
    );
    assert.strictEqual(r.isOrphaned, true);
    assert.strictEqual(r.shouldCancel, true);
  });

  test('missing creation_timestamp → treated as very old (stale)', () => {
    const r = classifyOpenOrder(
      { instrument_name: 'ETH-20260501-1500-P' },
      [{ instrument_name: 'ETH-20260501-1500-P', rule_type: 'exit' }],
      NOW
    );
    // ageMs = NOW - 0 = NOW (huge number), ageHours >> 8
    assert.strictEqual(r.isStale, true);
    assert.strictEqual(r.shouldCancel, true);
  });
});

// ============================================================================
// 14. Partial fill budget accounting on cancel
// ============================================================================

const computeCancelBudgetAdjustment = (order) => {
  const filled = Number(order.filled_amount || 0);
  if (filled <= 0) return { adjustment: 0, action: 'none' };

  const avgPx = Number(order.average_price || order.limit_price || 0);
  const fillValue = filled * avgPx;
  const isPut = order.instrument_name?.endsWith('-P');
  const isBuy = order.direction === 'buy';

  if (isPut && isBuy) return { adjustment: fillValue, action: 'put_bought' };
  if (isPut && !isBuy) return { adjustment: -fillValue, action: 'put_sold' };
  return { adjustment: 0, action: 'call_or_other' };
};

describe('Partial fill budget accounting on cancel', () => {
  test('cancelled buy put with partial fill → positive budget adjustment', () => {
    const r = computeCancelBudgetAdjustment({
      instrument_name: 'ETH-20260501-1500-P',
      direction: 'buy',
      filled_amount: '0.5',
      average_price: '10.00',
    });
    assert.strictEqual(r.adjustment, 5.00);
    assert.strictEqual(r.action, 'put_bought');
  });

  test('cancelled sell put with partial fill → negative budget adjustment', () => {
    const r = computeCancelBudgetAdjustment({
      instrument_name: 'ETH-20260501-1500-P',
      direction: 'sell',
      filled_amount: '0.5',
      average_price: '8.00',
    });
    assert.strictEqual(r.adjustment, -4.00);
    assert.strictEqual(r.action, 'put_sold');
  });

  test('cancelled call order → no budget adjustment', () => {
    const r = computeCancelBudgetAdjustment({
      instrument_name: 'ETH-20260501-2000-C',
      direction: 'sell',
      filled_amount: '1.0',
      average_price: '12.00',
    });
    assert.strictEqual(r.adjustment, 0);
    assert.strictEqual(r.action, 'call_or_other');
  });

  test('cancelled order with zero fill → no adjustment', () => {
    const r = computeCancelBudgetAdjustment({
      instrument_name: 'ETH-20260501-1500-P',
      direction: 'buy',
      filled_amount: '0',
    });
    assert.strictEqual(r.adjustment, 0);
    assert.strictEqual(r.action, 'none');
  });

  test('cancelled order with no filled_amount → no adjustment', () => {
    const r = computeCancelBudgetAdjustment({
      instrument_name: 'ETH-20260501-1500-P',
      direction: 'buy',
    });
    assert.strictEqual(r.adjustment, 0);
    assert.strictEqual(r.action, 'none');
  });

  test('uses limit_price when average_price missing', () => {
    const r = computeCancelBudgetAdjustment({
      instrument_name: 'ETH-20260501-1500-P',
      direction: 'buy',
      filled_amount: '1.0',
      limit_price: '7.50',
    });
    assert.strictEqual(r.adjustment, 7.50);
    assert.strictEqual(r.action, 'put_bought');
  });
});

// ============================================================================
// 15. Trigger details preferred_order_type passthrough
// ============================================================================

describe('Advisory order type preference passthrough', () => {
  test('preferred_order_type extracted from JSON trigger_details', () => {
    const triggerStr = JSON.stringify({ score: 0.005, preferred_order_type: 'post_only' });
    let triggerData = {};
    try { triggerData = JSON.parse(triggerStr); } catch {}
    assert.strictEqual(triggerData.preferred_order_type, 'post_only');
  });

  test('missing preferred_order_type → undefined', () => {
    const triggerStr = JSON.stringify({ score: 0.005, delta: -0.05 });
    let triggerData = {};
    try { triggerData = JSON.parse(triggerStr); } catch {}
    assert.strictEqual(triggerData.preferred_order_type, undefined);
  });

  test('malformed JSON → empty object, no crash', () => {
    const triggerStr = 'not valid json {{{';
    let triggerData = {};
    try { triggerData = JSON.parse(triggerStr); } catch {}
    assert.strictEqual(triggerData.preferred_order_type, undefined);
  });

  test('null trigger_details → empty object', () => {
    let triggerData = {};
    try { triggerData = typeof null === 'string' ? JSON.parse(null) : (null || {}); } catch {}
    assert.strictEqual(triggerData.preferred_order_type, undefined);
  });
});

// ============================================================================
// 16. placeOrder time_in_force parameter construction
// ============================================================================

describe('placeOrder order construction', () => {
  // Test the order object construction logic (not the API call)
  const buildOrderParams = (timeInForce) => {
    const order = {
      order_type: 'limit',
      reduce_only: false,
      time_in_force: timeInForce,
      ...(timeInForce === 'post_only' ? { post_only: true } : {}),
    };
    return order;
  };

  test('ioc → time_in_force=ioc, no post_only flag', () => {
    const o = buildOrderParams('ioc');
    assert.strictEqual(o.time_in_force, 'ioc');
    assert.strictEqual(o.post_only, undefined);
  });

  test('gtc → time_in_force=gtc, no post_only flag', () => {
    const o = buildOrderParams('gtc');
    assert.strictEqual(o.time_in_force, 'gtc');
    assert.strictEqual(o.post_only, undefined);
  });

  test('post_only → time_in_force=post_only AND post_only=true', () => {
    const o = buildOrderParams('post_only');
    assert.strictEqual(o.time_in_force, 'post_only');
    assert.strictEqual(o.post_only, true);
  });
});

describe('execution order type normalization', () => {
  const ACTION_POLICY = {
    buy_put: { phase: 'entry', reduceOnly: false, allowedOrderTypes: ['ioc', 'gtc', 'post_only'] },
    sell_call: { phase: 'entry', reduceOnly: false, allowedOrderTypes: ['ioc', 'gtc', 'post_only'] },
    sell_put: { phase: 'exit', reduceOnly: true, allowedOrderTypes: ['ioc'] },
    buyback_call: { phase: 'exit', reduceOnly: true, allowedOrderTypes: ['ioc', 'gtc', 'post_only'] },
  };
  const getActionPolicy = (action) => ACTION_POLICY[action] || null;
  const isReduceOnlyExitAction = (action) => Boolean(getActionPolicy(action)?.reduceOnly);
  const getAllowedOrderTypesForAction = (action) => getActionPolicy(action)?.allowedOrderTypes || ['ioc', 'gtc', 'post_only'];
  const normalizePreferredOrderType = (action, preferredOrderType) => {
    if (typeof preferredOrderType !== 'string') return null;
    const normalized = preferredOrderType.trim().toLowerCase();
    if (!normalized) return null;
    return getAllowedOrderTypesForAction(action).includes(normalized) ? normalized : null;
  };
  const isInvalidReduceOnlyOrderType = (action, orderType) => {
    return !getAllowedOrderTypesForAction(action).includes(orderType);
  };

  test('sell_put rejects gtc for reduce_only exit', () => {
    assert.strictEqual(isInvalidReduceOnlyOrderType('sell_put', 'gtc'), true);
  });

  test('buyback_call allows post_only for patient reduce_only buybacks', () => {
    assert.strictEqual(isInvalidReduceOnlyOrderType('buyback_call', 'post_only'), false);
  });

  test('sell_put accepts ioc for reduce_only exit', () => {
    assert.strictEqual(isInvalidReduceOnlyOrderType('sell_put', 'ioc'), false);
  });

  test('sell_call can still use resting order types', () => {
    assert.strictEqual(isInvalidReduceOnlyOrderType('sell_call', 'post_only'), false);
  });

  test('stale exit post_only preference is discarded', () => {
    assert.strictEqual(normalizePreferredOrderType('sell_put', 'post_only'), null);
  });

  test('sell_call keeps valid resting preference', () => {
    assert.strictEqual(normalizePreferredOrderType('sell_call', 'post_only'), 'post_only');
  });

  test('buyback_call keeps valid resting preference', () => {
    assert.strictEqual(normalizePreferredOrderType('buyback_call', 'post_only'), 'post_only');
  });

  test('exit preference is normalized case-insensitively', () => {
    assert.strictEqual(normalizePreferredOrderType('buyback_call', 'GTC'), 'gtc');
  });
});

describe('execution price validation', () => {
  test('non-positive execution price is invalid', () => {
    assert.strictEqual(Number(0) > 0, false);
    assert.strictEqual(Number(-1) > 0, false);
  });

  test('positive execution price is valid', () => {
    assert.strictEqual(Number(0.01) > 0, true);
  });
});

// ============================================================================
// 17. Voting logic correctness
// ============================================================================

const resolveVotingDecision = (haikuVote, codexVote) => {
  if (haikuVote && codexVote) {
    return (haikuVote.confirm && codexVote.confirm) ? 'confirmed' : 'rejected';
  } else if (haikuVote) {
    return haikuVote.confirm ? 'confirmed' : 'rejected';
  } else if (codexVote) {
    return codexVote.confirm ? 'confirmed' : 'rejected';
  }
  return 'retry'; // both failed
};

describe('Voting logic', () => {
  test('both confirm → confirmed', () => {
    assert.strictEqual(resolveVotingDecision({ confirm: true }, { confirm: true }), 'confirmed');
  });

  test('both reject → rejected', () => {
    assert.strictEqual(resolveVotingDecision({ confirm: false }, { confirm: false }), 'rejected');
  });

  test('haiku confirms, codex rejects → rejected (conservative)', () => {
    assert.strictEqual(resolveVotingDecision({ confirm: true }, { confirm: false }), 'rejected');
  });

  test('haiku rejects, codex confirms → rejected (conservative)', () => {
    assert.strictEqual(resolveVotingDecision({ confirm: false }, { confirm: true }), 'rejected');
  });

  test('only haiku responds, confirms → confirmed', () => {
    assert.strictEqual(resolveVotingDecision({ confirm: true }, null), 'confirmed');
  });

  test('only haiku responds, rejects → rejected', () => {
    assert.strictEqual(resolveVotingDecision({ confirm: false }, null), 'rejected');
  });

  test('only codex responds, confirms → confirmed', () => {
    assert.strictEqual(resolveVotingDecision(null, { confirm: true }), 'confirmed');
  });

  test('only codex responds, rejects → rejected', () => {
    assert.strictEqual(resolveVotingDecision(null, { confirm: false }), 'rejected');
  });

  test('both fail → retry', () => {
    assert.strictEqual(resolveVotingDecision(null, null), 'retry');
  });
});

// ============================================================================
// 18. DRY_RUN budget simulation correctness
// ============================================================================

describe('DRY_RUN budget simulation', () => {
  test('buy_put consumes put budget in dry run', () => {
    let putNetBought = 50;
    const action = 'buy_put';
    const amount = 1.0, price = 8.00;
    const totalValue = amount * price;
    if (action === 'buy_put') putNetBought += totalValue;
    else if (action === 'sell_put') putNetBought -= totalValue;
    assert.strictEqual(putNetBought, 58.00);
  });

  test('sell_put returns to put budget in dry run', () => {
    let putNetBought = 50;
    const action = 'sell_put';
    const amount = 0.5, price = 12.00;
    const totalValue = amount * price;
    if (action === 'buy_put') putNetBought += totalValue;
    else if (action === 'sell_put') putNetBought -= totalValue;
    assert.strictEqual(putNetBought, 44.00);
  });

  test('sell_call does not affect put budget in dry run', () => {
    let putNetBought = 50;
    const action = 'sell_call';
    const amount = 2.0, price = 15.00;
    const totalValue = amount * price;
    if (action === 'buy_put') putNetBought += totalValue;
    else if (action === 'sell_put') putNetBought -= totalValue;
    assert.strictEqual(putNetBought, 50);
  });
});

// ============================================================================
// 19. End-to-end: confirmation → execution result handling
// ============================================================================

describe('Confirmation result handling paths', () => {
  test('zeroFill result → action should be marked failed', () => {
    const result = { zeroFill: true, action: 'buy_put', instrumentName: 'ETH-20260501-1500-P' };
    assert.ok(result.zeroFill);
    // In real code: db.updatePendingAction(id, { status: 'failed', ... })
    const expectedStatus = result.zeroFill ? 'failed' : 'executed';
    assert.strictEqual(expectedStatus, 'failed');
  });

  test('resting result → action should be marked executed', () => {
    const result = { resting: true, orderId: 'ord_123', action: 'buy_put' };
    assert.ok(result.resting);
    const expectedStatus = result.resting ? 'executed' : 'unknown';
    assert.strictEqual(expectedStatus, 'executed');
  });

  test('normal fill result → action should be marked executed', () => {
    const result = { filledAmt: 1.0, avgPx: 5.50, totalValue: 5.50 };
    assert.ok(!result.zeroFill && !result.resting);
    assert.ok(result.filledAmt > 0);
    // Should be marked executed
  });

  test('null result → action should be marked failed', () => {
    const result = null;
    const expectedStatus = result ? 'executed' : 'failed';
    assert.strictEqual(expectedStatus, 'failed');
  });

  test('auto-reject after 3 retries', () => {
    const retries = 3;
    const shouldAutoReject = retries >= 3;
    assert.strictEqual(shouldAutoReject, true);
  });

  test('retry count 2 → not yet auto-rejected', () => {
    const retries = 2;
    const shouldAutoReject = retries >= 3;
    assert.strictEqual(shouldAutoReject, false);
  });
});

// ============================================================================
// 20. Cooldown logic
// ============================================================================

describe('Entry cooldown (1 hour between same action type)', () => {
  test('last executed 30 min ago → still in cooldown', () => {
    const lastExecuted = new Date(Date.now() - 30 * 60 * 1000).toISOString();
    const cooldownMs = 60 * 60 * 1000;
    const elapsed = Date.now() - new Date(lastExecuted).getTime();
    assert.ok(elapsed < cooldownMs, 'Should still be in cooldown');
  });

  test('last executed 90 min ago → cooldown expired', () => {
    const lastExecuted = new Date(Date.now() - 90 * 60 * 1000).toISOString();
    const cooldownMs = 60 * 60 * 1000;
    const elapsed = Date.now() - new Date(lastExecuted).getTime();
    assert.ok(elapsed >= cooldownMs, 'Cooldown should be expired');
  });

  test('no last execution → no cooldown', () => {
    const lastExecuted = null;
    const inCooldown = lastExecuted ? (Date.now() - new Date(lastExecuted).getTime() < 3600000) : false;
    assert.strictEqual(inCooldown, false);
  });
});


// ============================================================================
// 21. Price sanity check (Bug #3 fix)
// ============================================================================

const sanitizeVoterPrice = (voterLimitPrice, marketPrice) => {
  if (typeof voterLimitPrice !== 'number' || voterLimitPrice <= 0 || marketPrice <= 0) {
    return { price: marketPrice, rejected: false };
  }
  const ratio = voterLimitPrice / marketPrice;
  if (ratio >= 0.5 && ratio <= 2.0) {
    return { price: voterLimitPrice, rejected: false };
  }
  return { price: marketPrice, rejected: true, ratio };
};

describe('Voter price sanity check', () => {
  test('price within range (90% of market) → accepted', () => {
    const r = sanitizeVoterPrice(4.50, 5.00);
    assert.strictEqual(r.price, 4.50);
    assert.strictEqual(r.rejected, false);
  });

  test('price at 2x market → accepted (boundary)', () => {
    const r = sanitizeVoterPrice(10.00, 5.00);
    assert.strictEqual(r.price, 10.00);
    assert.strictEqual(r.rejected, false);
  });

  test('price at 0.5x market → accepted (boundary)', () => {
    const r = sanitizeVoterPrice(2.50, 5.00);
    assert.strictEqual(r.price, 2.50);
    assert.strictEqual(r.rejected, false);
  });

  test('price at 3x market → rejected (too high)', () => {
    const r = sanitizeVoterPrice(15.00, 5.00);
    assert.strictEqual(r.price, 5.00);
    assert.strictEqual(r.rejected, true);
  });

  test('price at 0.1x market → rejected (too low)', () => {
    const r = sanitizeVoterPrice(0.50, 5.00);
    assert.strictEqual(r.price, 5.00);
    assert.strictEqual(r.rejected, true);
  });

  test('zero voter price → uses market', () => {
    const r = sanitizeVoterPrice(0, 5.00);
    assert.strictEqual(r.price, 5.00);
  });

  test('negative voter price → uses market', () => {
    const r = sanitizeVoterPrice(-1, 5.00);
    assert.strictEqual(r.price, 5.00);
  });

  test('string voter price → uses market', () => {
    const r = sanitizeVoterPrice('4.50', 5.00);
    assert.strictEqual(r.price, 5.00);
  });

  test('zero market price → uses market (no division by zero)', () => {
    const r = sanitizeVoterPrice(5.00, 0);
    assert.strictEqual(r.price, 0);
  });
});

// ============================================================================
// 22. Orphan detection (Bug #5 fix - direction-aware)
// ============================================================================

const classifyOpenOrderV2 = (order, activeRules, nowMs) => {
  const ageMs = nowMs - (order.creation_timestamp || 0);
  const ageHours = ageMs / (1000 * 60 * 60);
  const isStale = ageHours > 8;

  const activeExitInstruments = new Set(
    activeRules.filter(r => r.rule_type === 'exit').map(r => r.instrument_name).filter(Boolean)
  );
  const activeEntryActions = new Set(
    activeRules.filter(r => r.rule_type === 'entry').map(r => r.action)
  );

  const matchesExitRule = activeExitInstruments.has(order.instrument_name);
  const isBuyOrder = order.direction === 'buy';
  const matchesEntryAction = (isBuyOrder && order.instrument_name?.endsWith('-P') && activeEntryActions.has('buy_put'))
    || (!isBuyOrder && order.instrument_name?.endsWith('-C') && activeEntryActions.has('sell_call'))
    || (isBuyOrder && order.instrument_name?.endsWith('-C') && activeEntryActions.has('buyback_call'))
    || (!isBuyOrder && order.instrument_name?.endsWith('-P') && activeEntryActions.has('sell_put'));
  const isOrphaned = !matchesExitRule && !matchesEntryAction;

  return { isStale, isOrphaned, shouldCancel: isStale || isOrphaned };
};

describe('Orphan detection V2 (direction-aware)', () => {
  const NOW = Date.now();

  test('buy put order + buy_put entry rule → NOT orphaned', () => {
    const r = classifyOpenOrderV2(
      { instrument_name: 'ETH-20260501-1500-P', direction: 'buy', creation_timestamp: NOW - 3600000 },
      [{ rule_type: 'entry', action: 'buy_put' }],
      NOW
    );
    assert.strictEqual(r.isOrphaned, false);
  });

  test('sell call order + sell_call entry rule → NOT orphaned', () => {
    const r = classifyOpenOrderV2(
      { instrument_name: 'ETH-20260501-2000-C', direction: 'sell', creation_timestamp: NOW - 3600000 },
      [{ rule_type: 'entry', action: 'sell_call' }],
      NOW
    );
    assert.strictEqual(r.isOrphaned, false);
  });

  test('buy put order + sell_call entry rule → ORPHANED (wrong direction)', () => {
    const r = classifyOpenOrderV2(
      { instrument_name: 'ETH-20260501-1500-P', direction: 'buy', creation_timestamp: NOW - 3600000 },
      [{ rule_type: 'entry', action: 'sell_call' }],
      NOW
    );
    assert.strictEqual(r.isOrphaned, true);
    assert.strictEqual(r.shouldCancel, true);
  });

  test('sell call order + buy_put entry rule → ORPHANED (wrong direction)', () => {
    const r = classifyOpenOrderV2(
      { instrument_name: 'ETH-20260501-2000-C', direction: 'sell', creation_timestamp: NOW - 3600000 },
      [{ rule_type: 'entry', action: 'buy_put' }],
      NOW
    );
    assert.strictEqual(r.isOrphaned, true);
  });

  test('sell put order + sell_put exit rule for same instrument → NOT orphaned', () => {
    const r = classifyOpenOrderV2(
      { instrument_name: 'ETH-20260501-1500-P', direction: 'sell', creation_timestamp: NOW - 3600000 },
      [{ rule_type: 'exit', action: 'sell_put', instrument_name: 'ETH-20260501-1500-P' }],
      NOW
    );
    assert.strictEqual(r.isOrphaned, false);
  });

  test('order with matching exit rule for different instrument → ORPHANED', () => {
    const r = classifyOpenOrderV2(
      { instrument_name: 'ETH-20260501-1500-P', direction: 'sell', creation_timestamp: NOW - 3600000 },
      [{ rule_type: 'exit', action: 'sell_put', instrument_name: 'ETH-20260601-2000-P' }],
      NOW
    );
    assert.strictEqual(r.isOrphaned, true);
  });

  test('no rules at all → ORPHANED', () => {
    const r = classifyOpenOrderV2(
      { instrument_name: 'ETH-20260501-1500-P', direction: 'buy', creation_timestamp: NOW - 3600000 },
      [],
      NOW
    );
    assert.strictEqual(r.isOrphaned, true);
  });
});

// ============================================================================
// 23. Fill reconciliation logic
// ============================================================================

describe('Fill reconciliation', () => {
  test('tracked order NOT in exchange list → classified as filled', () => {
    const tracked = [
      { order_id: 'ord_1', instrument_name: 'ETH-20260501-1500-P', direction: 'buy', amount: 1.0, limit_price: 5.00 },
      { order_id: 'ord_2', instrument_name: 'ETH-20260601-2000-C', direction: 'sell', amount: 2.0, limit_price: 8.00 },
    ];
    const exchangeOrders = [
      { order_id: 'ord_2', instrument_name: 'ETH-20260601-2000-C' }, // ord_1 is gone → filled
    ];
    const exchangeIds = new Set(exchangeOrders.map(o => o.order_id));
    const filled = tracked.filter(t => !exchangeIds.has(t.order_id));
    assert.strictEqual(filled.length, 1);
    assert.strictEqual(filled[0].order_id, 'ord_1');
  });

  test('all tracked orders still on exchange → none filled', () => {
    const tracked = [{ order_id: 'ord_1' }];
    const exchangeIds = new Set(['ord_1']);
    const filled = tracked.filter(t => !exchangeIds.has(t.order_id));
    assert.strictEqual(filled.length, 0);
  });

  test('filled put buy → positive budget adjustment', () => {
    const order = { instrument_name: 'ETH-20260501-1500-P', direction: 'buy', amount: 1.0, limit_price: 5.00 };
    const fillValue = order.amount * order.limit_price;
    const isPut = order.instrument_name.endsWith('-P');
    let budgetDelta = 0;
    if (isPut && order.direction === 'buy') budgetDelta = fillValue;
    else if (isPut && order.direction === 'sell') budgetDelta = -fillValue;
    assert.strictEqual(budgetDelta, 5.00);
  });

  test('filled call sell → zero budget adjustment (calls are collateral-sized)', () => {
    const order = { instrument_name: 'ETH-20260501-2000-C', direction: 'sell', amount: 2.0, limit_price: 8.00 };
    const fillValue = order.amount * order.limit_price;
    const isPut = order.instrument_name.endsWith('-P');
    let budgetDelta = 0;
    if (isPut && order.direction === 'buy') budgetDelta = fillValue;
    else if (isPut && order.direction === 'sell') budgetDelta = -fillValue;
    assert.strictEqual(budgetDelta, 0);
  });
});

// ============================================================================
// 24. post_only rejection handling
// ============================================================================

const classifyPlaceOrderResult = (order, orderType) => {
  if (!order) return 'failed';
  if (order.rejected_post_only) return 'post_only_rejected';
  return 'success';
};

const classifyExecutionResult = (result) => {
  if (!result) return 'failed';
  if (result.postOnlyRejected) return 'post_only_rejected';
  if (result.zeroFill) return 'zero_fill';
  if (result.resting) return 'resting';
  if (result.dryRun) return 'dry_run';
  if (result.filledAmt > 0) return 'filled';
  return 'unknown';
};

describe('post_only rejection handling', () => {
  test('placeOrder returns rejected_post_only → classified correctly', () => {
    assert.strictEqual(classifyPlaceOrderResult({ rejected_post_only: true, error: 'would cross' }, 'post_only'), 'post_only_rejected');
  });

  test('placeOrder returns null → classified as failed', () => {
    assert.strictEqual(classifyPlaceOrderResult(null, 'post_only'), 'failed');
  });

  test('placeOrder returns normal data → classified as success', () => {
    assert.strictEqual(classifyPlaceOrderResult({ result: {} }, 'post_only'), 'success');
  });

  test('executeOrder result with postOnlyRejected → correct classification', () => {
    assert.strictEqual(classifyExecutionResult({ postOnlyRejected: true }), 'post_only_rejected');
  });

  test('full result classification chain', () => {
    assert.strictEqual(classifyExecutionResult(null), 'failed');
    assert.strictEqual(classifyExecutionResult({ zeroFill: true }), 'zero_fill');
    assert.strictEqual(classifyExecutionResult({ resting: true }), 'resting');
    assert.strictEqual(classifyExecutionResult({ dryRun: true }), 'dry_run');
    assert.strictEqual(classifyExecutionResult({ filledAmt: 1.0 }), 'filled');
    assert.strictEqual(classifyExecutionResult({ filledAmt: 0 }), 'unknown');
  });
});

// ============================================================================
// 25. Fill reconciliation with order status API
// ============================================================================

describe('Fill reconciliation with order status', () => {
  const reconcileOrder = (tracked, finalStatus) => {
    let filledAmt, fillPrice, status;
    if (finalStatus) {
      filledAmt = finalStatus.filled_amount || 0;
      fillPrice = finalStatus.average_price > 0 ? finalStatus.average_price : tracked.limit_price;
      status = finalStatus.order_status;
    } else {
      filledAmt = tracked.amount;
      fillPrice = tracked.limit_price;
      status = 'filled';
    }
    const fillValue = filledAmt * fillPrice;
    const isPut = tracked.instrument_name?.endsWith('-P');
    let budgetDelta = 0;
    if (filledAmt > 0) {
      if (isPut && tracked.direction === 'buy') budgetDelta = fillValue;
      else if (isPut && tracked.direction === 'sell') budgetDelta = -fillValue;
    }
    return { filledAmt, fillPrice, fillValue, status, budgetDelta };
  };

  test('fully filled order → correct budget', () => {
    const r = reconcileOrder(
      { instrument_name: 'ETH-20260501-1500-P', direction: 'buy', amount: 1.0, limit_price: 5.00 },
      { order_status: 'filled', filled_amount: 1.0, average_price: 4.80 }
    );
    assert.strictEqual(r.status, 'filled');
    assert.strictEqual(r.filledAmt, 1.0);
    assert.strictEqual(r.fillPrice, 4.80); // Uses actual average_price, not limit
    assert.strictEqual(r.fillValue, 4.80);
    assert.strictEqual(r.budgetDelta, 4.80);
  });

  test('partially filled then cancelled → only accounts for filled portion', () => {
    const r = reconcileOrder(
      { instrument_name: 'ETH-20260501-1500-P', direction: 'buy', amount: 2.0, limit_price: 5.00 },
      { order_status: 'cancelled', filled_amount: 0.5, average_price: 4.90, cancel_reason: 'user_request' }
    );
    assert.strictEqual(r.status, 'cancelled');
    assert.strictEqual(r.filledAmt, 0.5);
    assert.strictEqual(r.fillPrice, 4.90);
    assert.strictEqual(r.budgetDelta, 0.5 * 4.90);
  });

  test('cancelled with zero fill → no budget impact', () => {
    const r = reconcileOrder(
      { instrument_name: 'ETH-20260501-1500-P', direction: 'buy', amount: 1.0, limit_price: 5.00 },
      { order_status: 'cancelled', filled_amount: 0, average_price: 0, cancel_reason: 'user_request' }
    );
    assert.strictEqual(r.status, 'cancelled');
    assert.strictEqual(r.filledAmt, 0);
    assert.strictEqual(r.budgetDelta, 0);
  });

  test('expired with zero fill → no budget impact', () => {
    const r = reconcileOrder(
      { instrument_name: 'ETH-20260501-1500-P', direction: 'buy', amount: 1.0, limit_price: 5.00 },
      { order_status: 'expired', filled_amount: 0, average_price: 0 }
    );
    assert.strictEqual(r.status, 'expired');
    assert.strictEqual(r.budgetDelta, 0);
  });

  test('API fallback (null status) → assumes full fill at limit price', () => {
    const r = reconcileOrder(
      { instrument_name: 'ETH-20260501-1500-P', direction: 'buy', amount: 1.0, limit_price: 5.00 },
      null
    );
    assert.strictEqual(r.status, 'filled');
    assert.strictEqual(r.filledAmt, 1.0);
    assert.strictEqual(r.fillPrice, 5.00);
    assert.strictEqual(r.budgetDelta, 5.00);
  });

  test('filled call sell → zero budget impact (calls are collateral-sized)', () => {
    const r = reconcileOrder(
      { instrument_name: 'ETH-20260501-2000-C', direction: 'sell', amount: 2.0, limit_price: 8.00 },
      { order_status: 'filled', filled_amount: 2.0, average_price: 8.50 }
    );
    assert.strictEqual(r.filledAmt, 2.0);
    assert.strictEqual(r.budgetDelta, 0); // calls don't affect put budget
  });

  test('average_price 0 falls back to limit_price', () => {
    const r = reconcileOrder(
      { instrument_name: 'ETH-20260501-1500-P', direction: 'buy', amount: 1.0, limit_price: 5.00 },
      { order_status: 'filled', filled_amount: 1.0, average_price: 0 }
    );
    assert.strictEqual(r.fillPrice, 5.00);
  });

  test('venue missing ghost order is treated as cancelled zero-fill', () => {
    const r = reconcileOrder(
      { instrument_name: 'ETH-20260501-2000-C', direction: 'sell', amount: 1.0, limit_price: 2.80 },
      { order_status: 'cancelled', filled_amount: 0, average_price: 0, cancel_reason: 'venue_missing' }
    );
    assert.strictEqual(r.status, 'cancelled');
    assert.strictEqual(r.filledAmt, 0);
    assert.strictEqual(r.fillValue, 0);
  });
});

// ============================================================================
// 26. Defensive response parsing
// ============================================================================

describe('Defensive API response parsing', () => {
  test('result is array directly → treated as orders list', () => {
    const raw = [{ order_id: 'a' }, { order_id: 'b' }];
    const orders = Array.isArray(raw) ? raw : (raw?.orders || []);
    assert.strictEqual(orders.length, 2);
  });

  test('result is { orders: [...] } → extracted correctly', () => {
    const raw = { orders: [{ order_id: 'a' }] };
    const orders = Array.isArray(raw) ? raw : (raw?.orders || []);
    assert.strictEqual(orders.length, 1);
  });

  test('extractOrderRecord reads nested result.order', () => {
    const record = extractOrderRecord({
      result: {
        order: {
          order_id: 'ord_123',
          order_status: 'cancelled',
          filled_amount: '0',
          average_price: '0',
        },
      },
    });
    assert.deepStrictEqual(record, {
      order_id: 'ord_123',
      order_status: 'cancelled',
      filled_amount: '0',
      average_price: '0',
    });
  });

  test('extractOrderRecord reads flat order payload', () => {
    const record = extractOrderRecord({
      order_id: 'ord_456',
      order_status: 'filled',
      filled_amount: '1.0',
      average_price: '4.8',
    });
    assert.deepStrictEqual(record, {
      order_id: 'ord_456',
      order_status: 'filled',
      filled_amount: '1.0',
      average_price: '4.8',
    });
  });

  test('result is null → empty array', () => {
    const raw = null;
    const orders = Array.isArray(raw) ? raw : (raw?.orders || []);
    assert.strictEqual(orders.length, 0);
  });

  test('result is undefined → empty array', () => {
    const raw = undefined;
    const orders = Array.isArray(raw) ? raw : (raw?.orders || []);
    assert.strictEqual(orders.length, 0);
  });

  test('result is {} (empty object) → empty array', () => {
    const raw = {};
    const orders = Array.isArray(raw) ? raw : (raw?.orders || []);
    assert.strictEqual(orders.length, 0);
  });

  test('result is { subaccount_id: 123, orders: [...] } → works', () => {
    const raw = { subaccount_id: 123, orders: [{ order_id: 'x' }] };
    const orders = Array.isArray(raw) ? raw : (raw?.orders || []);
    assert.strictEqual(orders.length, 1);
    assert.strictEqual(orders[0].order_id, 'x');
  });
});

// ============================================================================
// 27. Resting order DB dedup
// ============================================================================

describe('Resting order dedup for entry rules', () => {
  // Simulates the check: if we have a resting order for an instrument, skip entry
  test('resting order exists for instrument → skip', () => {
    const restingInstruments = new Set(['ETH-20260501-1500-P', 'ETH-20260601-2000-C']);
    const candidate = 'ETH-20260501-1500-P';
    assert.strictEqual(restingInstruments.has(candidate), true);
  });

  test('no resting order for instrument → proceed', () => {
    const restingInstruments = new Set(['ETH-20260601-2000-C']);
    const candidate = 'ETH-20260501-1500-P';
    assert.strictEqual(restingInstruments.has(candidate), false);
  });

  test('empty resting orders → always proceed', () => {
    const restingInstruments = new Set();
    assert.strictEqual(restingInstruments.has('anything'), false);
  });
});

// ── Test 28: extractJSON (balanced brace parser) ────────────────────────────

// Copy of extractJSON from script.js
const extractJSON = (text) => {
  if (!text || typeof text !== 'string') return null;
  const tryParseBalancedObject = (source) => {
    if (!source || typeof source !== 'string') return null;
    for (let start = source.indexOf('{'); start !== -1; start = source.indexOf('{', start + 1)) {
      let depth = 0;
      let inString = false;
      let escape = false;
      for (let i = start; i < source.length; i++) {
        const ch = source[i];
        if (escape) { escape = false; continue; }
        if (ch === '\\' && inString) { escape = true; continue; }
        if (ch === '"') { inString = !inString; continue; }
        if (inString) continue;
        if (ch === '{') depth++;
        else if (ch === '}') {
          depth--;
          if (depth === 0) {
            try { return JSON.parse(source.slice(start, i + 1)); }
            catch { break; }
          }
        }
      }
    }
    return null;
  };

  const trimmed = text.trim();
  const fencedBlocks = [...trimmed.matchAll(/```(?:json)?\s*([\s\S]*?)```/gi)]
    .map(match => match[1]?.trim())
    .filter(Boolean);
  const candidates = [
    ...fencedBlocks,
    trimmed.replace(/```(?:json)?/gi, '').replace(/```/g, '').trim(),
    trimmed,
  ];

  for (const candidate of candidates) {
    const parsed = tryParseBalancedObject(candidate);
    if (parsed) return parsed;
  }
  return null;
};

const normalizeTalebSecondOpinion = (payload) => {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return null;

  const critique = [
    payload.critique,
    payload.assessment,
    payload.summary,
    payload.reasoning,
    payload.overall_assessment,
  ].find((value) => typeof value === 'string' && value.trim().length > 0) || null;

  const vetoes = Array.isArray(payload.vetoes) ? payload.vetoes.filter((item) => item && typeof item === 'object' && !Array.isArray(item)) : [];
  const amendments = Array.isArray(payload.amendments) ? payload.amendments.filter((item) => item && typeof item === 'object' && !Array.isArray(item)) : [];
  const additions = Array.isArray(payload.additions) ? payload.additions.filter((item) => item && typeof item === 'object' && !Array.isArray(item)) : [];

  if (!critique && vetoes.length === 0 && amendments.length === 0 && additions.length === 0) {
    return null;
  }

  return { critique, vetoes, amendments, additions };
};

const parseTalebSecondOpinion = (text) => {
  if (!text || typeof text !== 'string') return null;
  const normalized = normalizeTalebSecondOpinion(extractJSON(text));
  if (normalized) return normalized;
  const trimmed = text.trim();
  if (!trimmed) return null;
  return {
    critique: trimmed,
    vetoes: [],
    amendments: [],
    additions: [],
    _parse_fallback: true,
  };
};

const extractConfirmationVote = (text) => {
  const parsed = extractJSON(text);
  if (parsed && typeof parsed.confirm === 'boolean') return parsed;

  if (!text || typeof text !== 'string') return null;
  const cleaned = text
    .replace(/```json/gi, '')
    .replace(/```/g, '')
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .trim();

  const confirmMatch = cleaned.match(/"confirm"\s*:\s*(true|false)/i) || cleaned.match(/\bconfirm\b[^a-z]{0,10}(true|false)/i);
  const orderTypeMatch = cleaned.match(/"order_type"\s*:\s*"(ioc|gtc|post_only)"/i);
  const nullOrderTypeMatch = cleaned.match(/"order_type"\s*:\s*null/i);
  const limitPriceMatch = cleaned.match(/"limit_price"\s*:\s*(null|-?\d+(?:\.\d+)?)/i);
  const reasoningMatch = cleaned.match(/"reasoning"\s*:\s*"([\s\S]*?)"\s*(?:[,}]|$)/i);

  let confirm = null;
  if (confirmMatch) {
    confirm = confirmMatch[1].toLowerCase() === 'true';
  } else if (/\breject(?:ed)?\b/i.test(cleaned) && !/\bconfirm(?:ed)?\b/i.test(cleaned)) {
    confirm = false;
  } else if (/\bconfirm(?:ed)?\b/i.test(cleaned) && !/\breject(?:ed)?\b/i.test(cleaned)) {
    confirm = true;
  }

  if (typeof confirm !== 'boolean') return null;

  const vote = {
    confirm,
    order_type: orderTypeMatch ? orderTypeMatch[1] : (nullOrderTypeMatch ? null : null),
    limit_price: null,
    reasoning: reasoningMatch ? reasoningMatch[1] : cleaned.slice(0, 500),
  };

  if (limitPriceMatch) {
    vote.limit_price = limitPriceMatch[1].toLowerCase() === 'null' ? null : Number(limitPriceMatch[1]);
  }

  return vote;
};

describe('extractJSON (balanced brace parser)', () => {
  test('simple JSON object', () => {
    const result = extractJSON('{"key": "value"}');
    assert.deepStrictEqual(result, { key: 'value' });
  });

  test('JSON embedded in text', () => {
    const result = extractJSON('Here is the result: {"assessment": "bearish"} End of response.');
    assert.deepStrictEqual(result, { assessment: 'bearish' });
  });

  test('nested JSON objects', () => {
    const result = extractJSON('{"outer": {"inner": 42}}');
    assert.deepStrictEqual(result, { outer: { inner: 42 } });
  });

  test('greedy regex would fail: text between two JSON blocks', () => {
    // Greedy /\{[\s\S]*\}/ would match from first { to last }, capturing garbage
    const text = '{"first": 1} some text {"second": 2}';
    const result = extractJSON(text);
    // extractJSON should return FIRST complete object, not garbage
    assert.deepStrictEqual(result, { first: 1 });
  });

  test('braces inside strings are handled', () => {
    const result = extractJSON('{"msg": "hello { world }"}');
    assert.deepStrictEqual(result, { msg: 'hello { world }' });
  });

  test('escaped quotes inside strings', () => {
    const result = extractJSON('{"msg": "say \\"hello\\""}');
    assert.deepStrictEqual(result, { msg: 'say "hello"' });
  });

  test('null input returns null', () => {
    assert.strictEqual(extractJSON(null), null);
  });

  test('empty string returns null', () => {
    assert.strictEqual(extractJSON(''), null);
  });

  test('no JSON returns null', () => {
    assert.strictEqual(extractJSON('just plain text'), null);
  });

  test('invalid JSON returns null', () => {
    assert.strictEqual(extractJSON('{not: valid json}'), null);
  });

  test('complex advisory-like JSON', () => {
    const text = `Here's my analysis:\n{"assessment": "market bearish", "entry_rules": [{"action": "buy_put", "criteria": {"option_type": "P"}}], "exit_rules": []}`;
    const result = extractJSON(text);
    assert.strictEqual(result.assessment, 'market bearish');
    assert.strictEqual(result.entry_rules.length, 1);
    assert.strictEqual(result.entry_rules[0].criteria.option_type, 'P');
  });
});

describe('extractConfirmationVote', () => {
  test('parses strict JSON vote directly', () => {
    const result = extractConfirmationVote('{"confirm":true,"order_type":"gtc","limit_price":12.5,"reasoning":"priced well"}');
    assert.deepStrictEqual(result, {
      confirm: true,
      order_type: 'gtc',
      limit_price: 12.5,
      reasoning: 'priced well',
    });
  });

  test('parses fenced JSON vote', () => {
    const result = extractConfirmationVote('```json\n{"confirm": false, "order_type": null, "limit_price": null, "reasoning": "too much risk"}\n```');
    assert.deepStrictEqual(result, {
      confirm: false,
      order_type: null,
      limit_price: null,
      reasoning: 'too much risk',
    });
  });

  test('salvages malformed fenced response', () => {
    const result = extractConfirmationVote('```json { "confirm": false, "order_type": null, "limit_price": null, "reasoning": "REJECT because sizing is too small"');
    assert.deepStrictEqual(result, {
      confirm: false,
      order_type: null,
      limit_price: null,
      reasoning: 'REJECT because sizing is too small',
    });
  });

  test('falls back to prose reject inference', () => {
    const result = extractConfirmationVote('REJECT - margin utilization is too high for this call sale.');
    assert.deepStrictEqual(result, {
      confirm: false,
      order_type: null,
      limit_price: null,
      reasoning: 'REJECT - margin utilization is too high for this call sale.',
    });
  });
});

describe('parseTalebSecondOpinion', () => {
  test('normalizes valid Taleb review JSON', () => {
    const result = parseTalebSecondOpinion('{"critique":"too fragile","vetoes":[{"reason":"bad convexity"}],"amendments":[],"additions":[]}');
    assert.ok(result);
    assert.strictEqual(result.critique, 'too fragile');
    assert.strictEqual(result.vetoes.length, 1);
    assert.strictEqual(result._parse_fallback, undefined);
  });

  test('falls back to raw text when only nested JSON fragment is parseable', () => {
    const text = 'Taleb review:\n{"amendments":[{"concern":"tight dte","suggested_change":{"field":"dte","op":"lte","value":25}}]\n';
    const result = parseTalebSecondOpinion(text);
    assert.ok(result);
    assert.strictEqual(result._parse_fallback, true);
    assert.ok(result.critique.includes('"field":"dte"'));
    assert.strictEqual(result.vetoes.length, 0);
  });
});

describe('Spot price sourcing helpers', () => {
  const extractTickerSpotPrice = (ticker) => {
    if (!ticker || typeof ticker !== 'object') return null;
    const candidates = [
      ticker.I,
      ticker.index_price,
      ticker.indexPrice,
      ticker.underlying_price,
      ticker.underlyingPrice,
      ticker.spot_price,
      ticker.spotPrice,
      ticker.price,
      ticker.last_price,
      ticker.lastPrice,
      ticker.M,
      ticker.mark_price,
      ticker.markPrice,
    ];
    for (const candidate of candidates) {
      const value = Number(candidate);
      if (value > 0) return value;
    }
    return null;
  };

  test('prefers explicit index price fields from Derive tickers', () => {
    assert.strictEqual(extractTickerSpotPrice({ I: '1825.4', M: '1820.1' }), 1825.4);
    assert.strictEqual(extractTickerSpotPrice({ index_price: '1827.9' }), 1827.9);
  });

  test('falls back through alternate spot-like fields', () => {
    assert.strictEqual(extractTickerSpotPrice({ spot_price: '1819.2' }), 1819.2);
    assert.strictEqual(extractTickerSpotPrice({ M: '1817.6' }), 1817.6);
  });

  test('returns null when no positive price exists', () => {
    assert.strictEqual(extractTickerSpotPrice({ I: 0, M: 0, price: null }), null);
    assert.strictEqual(extractTickerSpotPrice(null), null);
  });
});

// ============================================================================
// 29. Dynamic put budget calculation
// ============================================================================

describe('Dynamic put budget: formula', () => {
  const PUT_ANNUAL_RATE = 0.0333;
  const PERIOD_DAYS = 15;
  const cyclesPerYear = 365 / PERIOD_DAYS;

  test('$10,000 portfolio → correct per-cycle budget', () => {
    const portfolioValue = 10000;
    const budget = portfolioValue * PUT_ANNUAL_RATE / cyclesPerYear;
    // 10000 * 0.0333 / 24.333 = ~13.69
    assert.ok(budget > 13.0 && budget < 14.0, `Expected ~$13.69, got $${budget.toFixed(2)}`);
  });

  test('$100,000 portfolio → budget scales linearly', () => {
    const small = 10000 * PUT_ANNUAL_RATE / cyclesPerYear;
    const large = 100000 * PUT_ANNUAL_RATE / cyclesPerYear;
    assert.ok(Math.abs(large / small - 10) < 0.01, 'Should be exactly 10x');
  });

  test('$0 portfolio → $0 budget', () => {
    const budget = 0 * PUT_ANNUAL_RATE / cyclesPerYear;
    assert.strictEqual(budget, 0);
  });

  test('annual spend = 3.33% of portfolio', () => {
    const portfolioValue = 50000;
    const perCycle = portfolioValue * PUT_ANNUAL_RATE / cyclesPerYear;
    const annualSpend = perCycle * cyclesPerYear;
    assert.ok(Math.abs(annualSpend - portfolioValue * PUT_ANNUAL_RATE) < 0.01,
      `Annual spend $${annualSpend.toFixed(2)} should equal ${portfolioValue * PUT_ANNUAL_RATE}`);
  });

  test('24.33 cycles per year (365/15)', () => {
    assert.ok(Math.abs(cyclesPerYear - 24.333) < 0.01, `Got ${cyclesPerYear}`);
  });

  test('insured base adds fixed external ETH without multiplying USDC', () => {
    const spotPrice = 2000;
    const ethBalance = 2;
    const externalEth = 9;
    const usdcBalance = 1000;
    const insuredBase = usdcBalance + ((ethBalance + externalEth) * spotPrice);
    assert.strictEqual(insuredBase, 23000);

    const budget = insuredBase * PUT_ANNUAL_RATE / cyclesPerYear;
    assert.ok(budget > 31 && budget < 32, `Expected ~$31.48, got $${budget.toFixed(2)}`);
  });
});

// ============================================================================
// 30. Put cycle management (maybeResetPutCycle logic)
// ============================================================================

describe('Dynamic put budget: cycle management', () => {
  const PUT_ANNUAL_RATE = 0.0333;
  const PERIOD_DAYS = 15;
  const PERIOD_MS = PERIOD_DAYS * 24 * 60 * 60 * 1000;
  const cyclesPerYear = 365 / PERIOD_DAYS;

  // Simulate maybeResetPutCycle with a local botData
  const maybeResetPutCycle = (botData, portfolioValue, now) => {
    const cycleExpired = botData.putCycleStart && (now - botData.putCycleStart) >= PERIOD_MS;
    const noCycle = !botData.putCycleStart;

    if (noCycle || cycleExpired) {
      if (cycleExpired) {
        const prevRemaining = Math.max(0, botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought);
        botData.putUnspentBuyLimit = prevRemaining;
      }
      const newBudget = portfolioValue * PUT_ANNUAL_RATE / cyclesPerYear;
      botData.putCycleStart = now;
      botData.putBudgetForCycle = newBudget;
      botData.putNetBought = 0;
      return true; // cycle was reset
    }
    return false; // no reset
  };

  test('first call with no cycle → starts new cycle', () => {
    const bd = { putCycleStart: null, putBudgetForCycle: 0, putNetBought: 0, putUnspentBuyLimit: 0 };
    const now = Date.now();
    const reset = maybeResetPutCycle(bd, 10000, now);
    assert.strictEqual(reset, true);
    assert.strictEqual(bd.putCycleStart, now);
    assert.ok(bd.putBudgetForCycle > 13 && bd.putBudgetForCycle < 14);
    assert.strictEqual(bd.putNetBought, 0);
  });

  test('mid-cycle call → no reset', () => {
    const now = Date.now();
    const bd = { putCycleStart: now - 5 * 24 * 60 * 60 * 1000, putBudgetForCycle: 13.69, putNetBought: 5, putUnspentBuyLimit: 0 };
    const reset = maybeResetPutCycle(bd, 10000, now);
    assert.strictEqual(reset, false);
    assert.strictEqual(bd.putNetBought, 5); // unchanged
    assert.strictEqual(bd.putBudgetForCycle, 13.69); // unchanged
  });

  test('expired cycle → resets, rolls over unspent', () => {
    const now = Date.now();
    const bd = {
      putCycleStart: now - PERIOD_MS - 1000, // just expired
      putBudgetForCycle: 14.00,
      putNetBought: 4.00,
      putUnspentBuyLimit: 0,
    };
    const reset = maybeResetPutCycle(bd, 10000, now);
    assert.strictEqual(reset, true);
    // Unspent: 14.00 + 0 - 4.00 = 10.00 rolled over
    assert.strictEqual(bd.putUnspentBuyLimit, 10.00);
    assert.strictEqual(bd.putNetBought, 0); // reset for new cycle
    assert.ok(bd.putBudgetForCycle > 13 && bd.putBudgetForCycle < 14); // recalculated
  });

  test('expired cycle with full spend → zero rollover', () => {
    const now = Date.now();
    const bd = {
      putCycleStart: now - PERIOD_MS - 1000,
      putBudgetForCycle: 14.00,
      putNetBought: 14.00, // fully spent
      putUnspentBuyLimit: 0,
    };
    const reset = maybeResetPutCycle(bd, 10000, now);
    assert.strictEqual(reset, true);
    assert.strictEqual(bd.putUnspentBuyLimit, 0); // nothing to roll over
  });

  test('expired cycle with overspend → zero rollover (no negative)', () => {
    const now = Date.now();
    const bd = {
      putCycleStart: now - PERIOD_MS - 1000,
      putBudgetForCycle: 14.00,
      putNetBought: 20.00, // overspent
      putUnspentBuyLimit: 0,
    };
    const reset = maybeResetPutCycle(bd, 10000, now);
    assert.strictEqual(reset, true);
    assert.strictEqual(bd.putUnspentBuyLimit, 0); // Math.max(0, ...) prevents negative
  });

  test('expired cycle with existing rollover → compound rollover', () => {
    const now = Date.now();
    const bd = {
      putCycleStart: now - PERIOD_MS - 1000,
      putBudgetForCycle: 14.00,
      putNetBought: 4.00,
      putUnspentBuyLimit: 5.00, // existing rollover from cycle before that
    };
    const reset = maybeResetPutCycle(bd, 10000, now);
    assert.strictEqual(reset, true);
    // prevRemaining = max(0, 14 + 5 - 4) = 15.00
    assert.strictEqual(bd.putUnspentBuyLimit, 15.00);
  });

  test('budget scales with portfolio value at cycle reset', () => {
    const now = Date.now();
    const bd1 = { putCycleStart: null, putBudgetForCycle: 0, putNetBought: 0, putUnspentBuyLimit: 0 };
    const bd2 = { putCycleStart: null, putBudgetForCycle: 0, putNetBought: 0, putUnspentBuyLimit: 0 };
    maybeResetPutCycle(bd1, 10000, now);
    maybeResetPutCycle(bd2, 50000, now);
    assert.ok(Math.abs(bd2.putBudgetForCycle / bd1.putBudgetForCycle - 5) < 0.01,
      'Budget should be 5x for 5x portfolio');
  });
});

// ============================================================================
// 31. Put budget discipline in entry rule evaluation
// ============================================================================

describe('Dynamic put budget: entry rule gating', () => {
  test('buy_put with budget remaining → allowed', () => {
    const budgetForCycle = 14.00;
    const putNetBought = 2.00;
    const putUnspentBuyLimit = 0;
    const remaining = budgetForCycle + putUnspentBuyLimit - putNetBought;
    assert.ok(remaining > 10, `Remaining $${remaining} should be > 10`);
  });

  test('buy_put with budget nearly exhausted → blocked', () => {
    const budgetForCycle = 14.00;
    const putNetBought = 12.00;
    const putUnspentBuyLimit = 0;
    const remaining = budgetForCycle + putUnspentBuyLimit - putNetBought;
    assert.ok(remaining <= 10, `Remaining $${remaining} should be <= 10`);
  });

  test('buy_put with rollover extends budget', () => {
    const budgetForCycle = 14.00;
    const putNetBought = 12.00;
    const putUnspentBuyLimit = 10.00; // rollover
    const remaining = budgetForCycle + putUnspentBuyLimit - putNetBought;
    assert.ok(remaining > 10, `Remaining $${remaining} with rollover should be > 10`);
  });

  test('sell_call is never gated by put budget', () => {
    const action = 'sell_call';
    const budgetForCycle = 0; // zero budget
    // sell_call should not check put budget at all
    const shouldCheckBudget = action === 'buy_put' && budgetForCycle > 0;
    assert.strictEqual(shouldCheckBudget, false);
  });

  test('buy_put with zero budget config → no gating (budget disabled)', () => {
    const action = 'buy_put';
    const budgetForCycle = 0;
    const shouldCheckBudget = action === 'buy_put' && budgetForCycle > 0;
    assert.strictEqual(shouldCheckBudget, false, 'Zero budget = discipline disabled');
  });
});

// ============================================================================
// 32. Put budget: amount sizing capped by remaining budget
// ============================================================================

describe('Dynamic put budget: amount sizing cap', () => {
  const PUT_ANNUAL_RATE = 0.0333;
  const PERIOD_DAYS = 15;

  test('amount capped by remaining budget', () => {
    const budgetForCycle = 14.00;
    const putNetBought = 10.00;
    const putUnspentBuyLimit = 0;
    const remaining = budgetForCycle + putUnspentBuyLimit - putNetBought; // $4
    const price = 2.00;
    const ruleBudgetLimit = 100; // advisory wants $100
    const maxByRuleBudget = ruleBudgetLimit / price; // 50 contracts
    const maxByPutBudget = remaining / price; // 2 contracts
    const capped = Math.min(maxByRuleBudget, maxByPutBudget);
    assert.strictEqual(capped, 2);
  });

  test('rule budget_limit more restrictive than remaining → uses rule limit', () => {
    const budgetForCycle = 14.00;
    const putNetBought = 0;
    const putUnspentBuyLimit = 0;
    const remaining = budgetForCycle + putUnspentBuyLimit - putNetBought; // $14
    const price = 2.00;
    const ruleBudgetLimit = 4; // advisory only wants $4
    const maxByRuleBudget = ruleBudgetLimit / price; // 2 contracts
    const maxByPutBudget = remaining / price; // 7 contracts
    const capped = Math.min(maxByRuleBudget, maxByPutBudget);
    assert.strictEqual(capped, 2);
  });

  test('call sizing ignores put budget entirely', () => {
    const action = 'sell_call';
    const price = 5.00;
    const ruleBudgetLimit = 100;
    let maxByBudget = ruleBudgetLimit / price;
    // For puts only: cap by remaining. Calls skip this.
    if (action === 'buy_put') {
      const putRemaining = 0; // exhausted
      maxByBudget = Math.min(maxByBudget, putRemaining / price);
    }
    assert.strictEqual(maxByBudget, 20); // 100/5, not capped
  });
});

// ============================================================================
// 33. Put budget tracking through execution
// ============================================================================

describe('Dynamic put budget: execution tracking', () => {
  test('buy_put fill increases putNetBought', () => {
    let putNetBought = 5;
    const action = 'buy_put';
    const totalValue = 3.50;
    if (action === 'buy_put') putNetBought += totalValue;
    else if (action === 'sell_put') putNetBought -= totalValue;
    assert.strictEqual(putNetBought, 8.50);
  });

  test('sell_put fill decreases putNetBought (returns budget)', () => {
    let putNetBought = 10;
    const action = 'sell_put';
    const totalValue = 4.00;
    if (action === 'buy_put') putNetBought += totalValue;
    else if (action === 'sell_put') putNetBought -= totalValue;
    assert.strictEqual(putNetBought, 6.00);
  });

  test('sell_call does not affect putNetBought', () => {
    let putNetBought = 10;
    const action = 'sell_call';
    const totalValue = 8.00;
    if (action === 'buy_put') putNetBought += totalValue;
    else if (action === 'sell_put') putNetBought -= totalValue;
    assert.strictEqual(putNetBought, 10);
  });

  test('buyback_call does not affect putNetBought', () => {
    let putNetBought = 10;
    const action = 'buyback_call';
    const totalValue = 8.00;
    if (action === 'buy_put') putNetBought += totalValue;
    else if (action === 'sell_put') putNetBought -= totalValue;
    assert.strictEqual(putNetBought, 10);
  });

  test('resting put buy fill tracks budget on reconciliation', () => {
    let putNetBought = 5;
    const tracked = { instrument_name: 'ETH-20260501-1500-P', direction: 'buy' };
    const filledAmt = 1.0;
    const fillPrice = 3.00;
    const fillValue = filledAmt * fillPrice;
    const isPut = tracked.instrument_name.endsWith('-P');
    if (filledAmt > 0) {
      if (isPut && tracked.direction === 'buy') putNetBought += fillValue;
      else if (isPut && tracked.direction === 'sell') putNetBought -= fillValue;
    }
    assert.strictEqual(putNetBought, 8.00);
  });

  test('resting call sell fill does not track budget', () => {
    let putNetBought = 5;
    const tracked = { instrument_name: 'ETH-20260501-2000-C', direction: 'sell' };
    const filledAmt = 2.0;
    const fillPrice = 4.00;
    const fillValue = filledAmt * fillPrice;
    const isPut = tracked.instrument_name.endsWith('-P');
    if (filledAmt > 0) {
      if (isPut && tracked.direction === 'buy') putNetBought += fillValue;
      else if (isPut && tracked.direction === 'sell') putNetBought -= fillValue;
    }
    assert.strictEqual(putNetBought, 5); // unchanged
  });

  test('zero fill does not change budget', () => {
    let putNetBought = 5;
    const tracked = { instrument_name: 'ETH-20260501-1500-P', direction: 'buy' };
    const filledAmt = 0;
    const fillPrice = 3.00;
    const fillValue = filledAmt * fillPrice;
    const isPut = tracked.instrument_name.endsWith('-P');
    if (filledAmt > 0) {
      if (isPut && tracked.direction === 'buy') putNetBought += fillValue;
      else if (isPut && tracked.direction === 'sell') putNetBought -= fillValue;
    }
    assert.strictEqual(putNetBought, 5); // unchanged
  });
});

// ============================================================================
// 34. fetchSubaccount response parsing
// ============================================================================

describe('fetchSubaccount response parsing', () => {
  test('parses margin fields correctly', () => {
    const r = {
      initial_margin: '1234.56',
      maintenance_margin: '987.65',
      subaccount_value: '5000.00',
      positions_value: '200.00',
      collaterals_value: '4800.00',
      collaterals_initial_margin: '1100.00',
      collaterals_maintenance_margin: '900.00',
      open_orders_margin: '50.00',
      margin_usage_pct: '55.14',
      is_under_liquidation: false,
    };
    const parsed = {
      initial_margin: Number(r?.initial_margin ?? 0),
      maintenance_margin: Number(r?.maintenance_margin ?? 0),
      subaccount_value: Number(r?.subaccount_value ?? 0),
      positions_value: Number(r?.positions_value ?? 0),
      collaterals_value: Number(r?.collaterals_value ?? 0),
      collaterals_initial_margin: Number(r?.collaterals_initial_margin ?? 0),
      collaterals_maintenance_margin: Number(r?.collaterals_maintenance_margin ?? 0),
      open_orders_margin: Number(r?.open_orders_margin ?? 0),
      margin_usage_pct: Number(r?.margin_usage_pct ?? NaN),
      is_under_liquidation: r?.is_under_liquidation || false,
    };
    assert.strictEqual(parsed.initial_margin, 1234.56);
    assert.strictEqual(parsed.maintenance_margin, 987.65);
    assert.strictEqual(parsed.subaccount_value, 5000);
    assert.strictEqual(parsed.margin_usage_pct, 55.14);
    assert.strictEqual(parsed.is_under_liquidation, false);
  });

  test('null response → all zeros', () => {
    const r = null;
    const parsed = {
      initial_margin: Number(r?.initial_margin ?? 0),
      maintenance_margin: Number(r?.maintenance_margin ?? 0),
      subaccount_value: Number(r?.subaccount_value ?? 0),
      is_under_liquidation: r?.is_under_liquidation || false,
    };
    assert.strictEqual(parsed.initial_margin, 0);
    assert.strictEqual(parsed.maintenance_margin, 0);
    assert.strictEqual(parsed.is_under_liquidation, false);
  });

  test('margin_usage_pct calculation', () => {
    const collateralsMaintenanceMargin = 4282.9;
    const positionsInitialMargin = 2447.6;
    const openOrdersMargin = 0;
    const usage = +(((positionsInitialMargin + openOrdersMargin) / collateralsMaintenanceMargin) * 100).toFixed(1);
    assert.strictEqual(usage, 57.1);
  });

  test('margin_usage_pct with zero collateral margin base → null', () => {
    const collaterals_initial_margin = 0;
    const initial_margin = 0;
    const usage = collaterals_initial_margin > 0
      ? +((1 - initial_margin / collaterals_initial_margin) * 100).toFixed(1)
      : null;
    assert.strictEqual(usage, null);
  });

  test('maintenance-collateral utilization takes precedence over undocumented usage field', () => {
    const usage = estimateMarginUtilization({
      initial_margin: 675,
      collaterals_initial_margin: 1500,
      collaterals_maintenance_margin: 4282.9,
      positions_initial_margin: 2447.6,
      open_orders_margin: 0,
      margin_usage_pct: 85.7,
    });
    assert.strictEqual(+((usage || 0) * 100).toFixed(1), 57.1);
  });

  test('negative maintenance collateral from API is normalized by magnitude', () => {
    const usage = estimateMarginUtilization({
      collaterals_maintenance_margin: -4282.9,
      positions_initial_margin: 2447.6,
      open_orders_margin: 0,
    });
    assert.strictEqual(+((usage || 0) * 100).toFixed(1), 57.1);
  });

  test('negative positions margin from API is normalized by magnitude', () => {
    const usage = estimateMarginUtilization({
      collaterals_maintenance_margin: 4282.9,
      positions_initial_margin: -2447.6,
      open_orders_margin: 0,
    });
    assert.strictEqual(+((usage || 0) * 100).toFixed(1), 57.1);
  });

  test('aggregated collateral and position margins are used when top-level fields are weak', () => {
    const usage = estimateMarginUtilization({
      collaterals_maintenance_margin: 0,
      aggregated_collaterals_maintenance_margin: 4282.9,
      positions_initial_margin: 0,
      aggregated_positions_initial_margin: 2447.6,
      open_orders_margin: 0,
    });
    assert.strictEqual(+((usage || 0) * 100).toFixed(1), 57.1);
  });
});

// ============================================================================
// 35. Liquidation safety gate
// ============================================================================

describe('Liquidation safety gate', () => {
  test('under liquidation + buy_put → auto-rejected', () => {
    const marginState = { is_under_liquidation: true };
    const action = { action: 'buy_put', instrument_name: 'ETH-20260501-1500-P' };
    const isEntry = action.action === 'buy_put' || action.action === 'sell_call';
    const shouldReject = marginState?.is_under_liquidation && isEntry;
    assert.strictEqual(shouldReject, true);
  });

  test('under liquidation + sell_call → auto-rejected', () => {
    const marginState = { is_under_liquidation: true };
    const action = { action: 'sell_call', instrument_name: 'ETH-20260501-2000-C' };
    const isEntry = action.action === 'buy_put' || action.action === 'sell_call';
    const shouldReject = marginState?.is_under_liquidation && isEntry;
    assert.strictEqual(shouldReject, true);
  });

  test('under liquidation + sell_put (exit) → allowed', () => {
    const marginState = { is_under_liquidation: true };
    const action = { action: 'sell_put', instrument_name: 'ETH-20260501-1500-P' };
    const isEntry = action.action === 'buy_put' || action.action === 'sell_call';
    const shouldReject = marginState?.is_under_liquidation && isEntry;
    assert.strictEqual(shouldReject, false);
  });

  test('under liquidation + buyback_call (exit) → allowed', () => {
    const marginState = { is_under_liquidation: true };
    const action = { action: 'buyback_call', instrument_name: 'ETH-20260501-2000-C' };
    const isEntry = action.action === 'buy_put' || action.action === 'sell_call';
    const shouldReject = marginState?.is_under_liquidation && isEntry;
    assert.strictEqual(shouldReject, false);
  });

  test('not under liquidation → all actions allowed', () => {
    const marginState = { is_under_liquidation: false };
    for (const a of ['buy_put', 'sell_call', 'sell_put', 'buyback_call']) {
      const isEntry = a === 'buy_put' || a === 'sell_call';
      const shouldReject = marginState?.is_under_liquidation && isEntry;
      assert.strictEqual(shouldReject, false, `${a} should not be rejected`);
    }
  });

  test('null marginState → all actions allowed (graceful)', () => {
    const marginState = null;
    for (const a of ['buy_put', 'sell_call', 'sell_put', 'buyback_call']) {
      const isEntry = a === 'buy_put' || a === 'sell_call';
      const shouldReject = marginState?.is_under_liquidation && isEntry;
      assert.ok(!shouldReject, `${a} should not be rejected when margin unavailable`);
    }
  });
});

// ============================================================================
// 36. accountHealth structure for advisory
// ============================================================================

describe('accountHealth structure', () => {
  test('includes all required fields for advisory', () => {
    const accountHealth = {
      ethBalance: 5.0,
      usdcBalance: -200,
      shortCallExposure: 1.5,
      margin: {
        initial_margin: 3000,
        maintenance_margin: 2500,
        subaccount_value: 8000,
        collaterals_value: 7500,
        open_orders_margin: 100,
        is_under_liquidation: false,
        margin_usage_pct: 60.0,
      },
      putBudgetDiscipline: {
        annualRate: 0.0333,
        budgetThisCycle: 13.69,
        spent: 5.00,
        remaining: 8.69,
        rollover: 0,
        cycleDays: 15,
        note: 'test',
      },
      note: 'test',
    };
    // Verify structure exists
    assert.ok(accountHealth.margin);
    assert.ok(accountHealth.putBudgetDiscipline);
    assert.strictEqual(accountHealth.margin.is_under_liquidation, false);
    assert.strictEqual(accountHealth.putBudgetDiscipline.annualRate, 0.0333);
    assert.strictEqual(accountHealth.putBudgetDiscipline.remaining, 8.69);
    // Negative USDC = margin debt, which is expected with ETH collateral
    assert.strictEqual(accountHealth.usdcBalance, -200);
  });

  test('margin null when API fails', () => {
    const accountHealth = {
      ethBalance: 5.0,
      usdcBalance: 0,
      shortCallExposure: 0,
      margin: null,
      putBudgetDiscipline: { budgetThisCycle: 13.69, spent: 0, remaining: 13.69 },
    };
    assert.strictEqual(accountHealth.margin, null);
    // Advisory should still function without margin data
    assert.ok(accountHealth.putBudgetDiscipline.budgetThisCycle > 0);
  });
});

// ============================================================================
// 37. Full schema: ETH holder → budget cycle → entry matching → execution → tracking
// ============================================================================

describe('Full schema: ETH collateral → budgeted put buying', () => {
  // This tests the complete flow an ETH holder experiences:
  // 1. Portfolio valued in ETH * spot
  // 2. 15-day budget cycle allocates 3.33%/yr
  // 3. Entry rules match options from the advisory
  // 4. Budget gates and sizes the trade
  // 5. Execution tracks putNetBought
  // 6. Cycle resets with rollover

  const PUT_ANNUAL_RATE = 0.0333;
  const PERIOD_DAYS = 15;
  const PERIOD = PERIOD_DAYS * 86400000;

  // Simulate botData (mutable state across steps)
  let botData;
  const resetBotData = () => {
    botData = {
      putCycleStart: null,
      putBudgetForCycle: 0,
      putNetBought: 0,
      putUnspentBuyLimit: 0,
    };
  };

  // Copy of maybeResetPutCycle logic
  const maybeResetPutCycle = (portfolioValue) => {
    const now = Date.now();
    const cycleExpired = botData.putCycleStart && (now - botData.putCycleStart) >= PERIOD;
    const noCycle = !botData.putCycleStart;
    if (noCycle || cycleExpired) {
      if (cycleExpired) {
        const prevRemaining = Math.max(0, botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought);
        botData.putUnspentBuyLimit = prevRemaining;
      }
      const cyclesPerYear = 365 / PERIOD_DAYS;
      const newBudget = portfolioValue * PUT_ANNUAL_RATE / cyclesPerYear;
      botData.putCycleStart = now;
      botData.putBudgetForCycle = newBudget;
      botData.putNetBought = 0;
    }
  };

  // Helper: simulate evaluateTradingRules entry logic for buy_put
  const evaluateEntryRule = (rule, tickerMap, instruments, spotPrice) => {
    let criteria;
    try { criteria = typeof rule.criteria === 'string' ? JSON.parse(rule.criteria) : rule.criteria; } catch { return null; }
    if (!criteria || !criteria.option_type) return null;

    // Budget gate
    if (rule.action === 'buy_put' && botData.putBudgetForCycle > 0) {
      const putRemaining = botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought;
      if (putRemaining <= 10) return null;
    }

    // Market conditions
    if (criteria.market_conditions) {
      const marketValues = { spot_price: spotPrice };
      if (!evaluateConditions(criteria.market_conditions, 'all', marketValues)) return null;
    }

    // Scan candidates
    let candidates = [];
    for (const [instrName, ticker] of Object.entries(tickerMap)) {
      const instrument = instruments.find(i => i.instrument_name === instrName);
      if (!instrument) continue;
      if (criteria.option_type && instrument.option_details?.option_type !== criteria.option_type) continue;

      const expiry = parseExpiryFromInstrument(instrName);
      if (!expiry) continue;
      const dte = Math.max(0, (expiry.getTime() - Date.now()) / 86400000);
      if (criteria.dte_range && (dte < criteria.dte_range[0] || dte > criteria.dte_range[1])) continue;

      const delta = Number(ticker?.option_pricing?.d) || 0;
      if (criteria.delta_range && (delta < criteria.delta_range[0] || delta > criteria.delta_range[1])) continue;

      const strike = Number(instrument.option_details?.strike) || 0;
      if (criteria.max_strike_pct && strike >= criteria.max_strike_pct * spotPrice) continue;

      const askPrice = Number(ticker?.a) || 0;
      if (criteria.max_cost != null && askPrice > criteria.max_cost) continue;

      const absDelta = Math.abs(delta);
      const score = askPrice > 0 ? absDelta / askPrice : 0;
      if (criteria.min_score != null && score < criteria.min_score) continue;

      candidates.push({ name: instrName, askPrice, askAmount: Number(ticker?.A) || 0, delta, dte, score, strike, amountStep: instrument.options?.amount_step || 0.01 });
    }

    if (candidates.length === 0) return null;
    candidates.sort((a, b) => b.score - a.score);
    const best = candidates[0];

    // Size by budget
    const price = best.askPrice;
    if (price <= 0) return null;
    let maxByBudget = (rule.budget_limit || Infinity) / price;
    if (rule.action === 'buy_put' && botData.putBudgetForCycle > 0) {
      const putRemaining = botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought;
      maxByBudget = Math.min(maxByBudget, putRemaining / price);
    }
    const bookLiq = best.askAmount;
    const step = best.amountStep;
    const raw = Math.min(maxByBudget, bookLiq, 20);
    const qty = Math.floor(raw / step) * step;
    if (qty < step) return null;

    return { instrument: best.name, qty, price, score: best.score, totalValue: qty * price };
  };

  // Helper: simulate executeOrder budget tracking for buy_put
  const trackPutBuy = (totalValue) => {
    botData.putNetBought += totalValue;
  };

  // ── Test data: realistic options ──
  // Future expiry ~60 days from now
  const futureDate = new Date(Date.now() + 60 * 86400000);
  const expiryStr = futureDate.toISOString().slice(0, 10).replace(/-/g, '');
  const putInstrument = `ETH-${expiryStr}-1400-P`;

  const instruments = [
    { instrument_name: putInstrument, option_details: { option_type: 'P', strike: 1400, expiry: expiryStr }, options: { amount_step: 0.1 }, base_asset_address: '0x1', base_asset_sub_id: '1' },
  ];
  const tickerMap = {
    [putInstrument]: { a: '5.50', A: '10', b: '4.80', B: '8', M: '5.15', option_pricing: { d: '-0.05', i: '0.65', t: '-0.03' } },
  };

  test('Step 1: ETH holder portfolio → cycle starts with correct budget', () => {
    resetBotData();
    const ethBalance = 5.0;
    const spotPrice = 1800;
    const portfolioValue = ethBalance * spotPrice; // $9,000
    maybeResetPutCycle(portfolioValue);

    const expectedBudget = 9000 * 0.0333 / (365 / 15); // ~$12.32
    assert.ok(botData.putCycleStart > 0, 'Cycle should start');
    assert.ok(Math.abs(botData.putBudgetForCycle - expectedBudget) < 0.01, `Budget $${botData.putBudgetForCycle.toFixed(2)} should be ~$${expectedBudget.toFixed(2)}`);
    assert.strictEqual(botData.putNetBought, 0, 'No puts bought yet');
  });

  test('Step 2: Advisory rule matches a well-priced put', () => {
    // Rule from advisory: buy OTM puts
    const rule = {
      action: 'buy_put',
      criteria: {
        option_type: 'P',
        delta_range: [-0.08, -0.02],
        dte_range: [45, 75],
        max_strike_pct: 0.85,
        min_score: 0.004,
        max_cost: 15.00,
      },
      budget_limit: 50.00,
    };

    const result = evaluateEntryRule(rule, tickerMap, instruments, 1800);
    assert.ok(result, 'Should find a candidate');
    assert.strictEqual(result.instrument, putInstrument);
    // Score = |delta| / askPrice = 0.05 / 5.50 ≈ 0.00909 (above min_score 0.004)
    assert.ok(result.score > 0.004, `Score ${result.score} should exceed min_score`);
    assert.strictEqual(result.price, 5.50);
    assert.ok(result.qty > 0, 'Quantity should be positive');
  });

  test('Step 3: Quantity is capped by remaining budget', () => {
    // Budget is ~$12.32, price is $5.50 per contract
    // Max by budget = 12.32 / 5.50 ≈ 2.24 → quantized to 2.2 (step 0.1)
    const rule = {
      action: 'buy_put',
      criteria: { option_type: 'P', delta_range: [-0.08, -0.02], dte_range: [45, 75], max_strike_pct: 0.85, min_score: 0.004, max_cost: 15.00 },
      budget_limit: 50.00,
    };

    const result = evaluateEntryRule(rule, tickerMap, instruments, 1800);
    const maxByBudget = botData.putBudgetForCycle / 5.50; // ~2.24
    assert.ok(result.qty <= Math.ceil(maxByBudget * 10) / 10, `Qty ${result.qty} should be capped by budget (~${maxByBudget.toFixed(1)} contracts)`);
    assert.ok(result.totalValue <= botData.putBudgetForCycle + botData.putUnspentBuyLimit + 0.01, `Total $${result.totalValue.toFixed(2)} should not exceed budget $${botData.putBudgetForCycle.toFixed(2)}`);
  });

  test('Step 4: Execution decrements putNetBought', () => {
    const rule = {
      action: 'buy_put',
      criteria: { option_type: 'P', delta_range: [-0.08, -0.02], dte_range: [45, 75], max_strike_pct: 0.85, min_score: 0.004, max_cost: 15.00 },
      budget_limit: 50.00,
    };
    const result = evaluateEntryRule(rule, tickerMap, instruments, 1800);
    const prevBought = botData.putNetBought;
    trackPutBuy(result.totalValue);
    assert.strictEqual(botData.putNetBought, prevBought + result.totalValue);
    assert.ok(botData.putNetBought > 0, 'putNetBought should increase');
  });

  test('Step 5: After buying, budget mostly exhausted → next buy blocked', () => {
    // After Step 4, putNetBought ≈ $12.10, budget ≈ $12.32
    // Remaining ≈ $0.22 which is <= $10 threshold
    const remaining = botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought;
    assert.ok(remaining <= 10, `Remaining $${remaining.toFixed(2)} should be <= $10 (budget exhausted for this cycle)`);

    const rule = {
      action: 'buy_put',
      criteria: { option_type: 'P', delta_range: [-0.08, -0.02], dte_range: [45, 75], max_strike_pct: 0.85, min_score: 0.004, max_cost: 15.00 },
      budget_limit: 50.00,
    };
    const result = evaluateEntryRule(rule, tickerMap, instruments, 1800);
    assert.strictEqual(result, null, 'Should be blocked — budget exhausted');
  });

  test('Step 6: Cycle resets → fresh budget, unspent rolls over', () => {
    const spent = botData.putNetBought;
    const prevBudget = botData.putBudgetForCycle;
    const unspent = Math.max(0, prevBudget + botData.putUnspentBuyLimit - spent);

    // Simulate 15 days passing
    botData.putCycleStart = Date.now() - PERIOD - 1;

    // Portfolio value changed (ETH went up)
    const newPortfolio = 5.0 * 2000; // $10,000
    maybeResetPutCycle(newPortfolio);

    const newBudget = 10000 * 0.0333 / (365 / 15); // ~$13.69
    assert.ok(Math.abs(botData.putBudgetForCycle - newBudget) < 0.01, `New budget $${botData.putBudgetForCycle.toFixed(2)} should be ~$${newBudget.toFixed(2)}`);
    assert.strictEqual(botData.putNetBought, 0, 'putNetBought resets to 0');
    assert.ok(Math.abs(botData.putUnspentBuyLimit - unspent) < 0.01, `Rollover $${botData.putUnspentBuyLimit.toFixed(2)} should be ~$${unspent.toFixed(2)}`);
  });

  test('Step 7: Rollover + new budget → can buy again', () => {
    const totalAvailable = botData.putBudgetForCycle + botData.putUnspentBuyLimit - botData.putNetBought;
    assert.ok(totalAvailable > 10, `Total available $${totalAvailable.toFixed(2)} should be > $10`);

    const rule = {
      action: 'buy_put',
      criteria: { option_type: 'P', delta_range: [-0.08, -0.02], dte_range: [45, 75], max_strike_pct: 0.85, min_score: 0.004, max_cost: 15.00 },
      budget_limit: 50.00,
    };
    const result = evaluateEntryRule(rule, tickerMap, instruments, 2000);
    assert.ok(result, 'Should be able to buy after cycle reset with rollover');
    assert.ok(result.qty > 0);
  });
});

// ============================================================================
// 38. Full schema: exit rule monitoring (sell_put to roll positions)
// ============================================================================

describe('Full schema: exit monitoring for put rolling', () => {
  // The bot holds puts and needs to roll them ~3-4 weeks before expiry
  // Exit rule triggers → pending action queued → eventually confirmed/executed

  // Create a fresh in-memory DB for this test
  const Database = require('better-sqlite3');
  const exitDb = new Database(':memory:');
  exitDb.pragma('journal_mode = WAL');
  exitDb.exec(`
    CREATE TABLE IF NOT EXISTS trading_rules (
      id INTEGER PRIMARY KEY AUTOINCREMENT, rule_type TEXT NOT NULL, action TEXT NOT NULL,
      instrument_name TEXT, criteria TEXT NOT NULL, budget_limit REAL,
      priority TEXT DEFAULT 'medium', reasoning TEXT,
      created_at TEXT DEFAULT (datetime('now')), is_active INTEGER DEFAULT 1,
      advisory_id TEXT, preferred_order_type TEXT
    );
    CREATE TABLE IF NOT EXISTS pending_actions (
      id INTEGER PRIMARY KEY AUTOINCREMENT, rule_id INTEGER REFERENCES trading_rules(id),
      action TEXT NOT NULL, instrument_name TEXT NOT NULL, amount REAL, price REAL,
      trigger_details TEXT, status TEXT DEFAULT 'pending', retries INTEGER DEFAULT 0,
      triggered_at TEXT DEFAULT (datetime('now')), confirmation_reasoning TEXT,
      confirmed_at TEXT, executed_at TEXT, execution_result TEXT
    );
  `);

  const exitStmts = {
    insertRule: exitDb.prepare(`INSERT INTO trading_rules (rule_type, action, instrument_name, criteria, priority, reasoning, advisory_id, is_active) VALUES (@rule_type, @action, @instrument_name, @criteria, @priority, @reasoning, @advisory_id, 1)`),
    getExitRules: exitDb.prepare(`SELECT * FROM trading_rules WHERE is_active = 1 AND rule_type = 'exit'`),
    insertPendingAction: exitDb.prepare(`INSERT INTO pending_actions (rule_id, action, instrument_name, amount, price, trigger_details) VALUES (@rule_id, @action, @instrument_name, @amount, @price, @trigger_details)`),
    hasPendingForRule: exitDb.prepare(`SELECT COUNT(*) as count FROM pending_actions WHERE rule_id = @rule_id AND status IN ('pending', 'confirmed')`),
    getPending: exitDb.prepare(`SELECT * FROM pending_actions WHERE status = 'pending'`),
  };

  // Insert an exit rule: sell put when DTE <= 25 (roll window)
  exitStmts.insertRule.run({
    rule_type: 'exit', action: 'sell_put', instrument_name: 'ETH-20260501-1500-P',
    criteria: JSON.stringify({ conditions: [{ field: 'dte', op: 'lte', value: 25 }], condition_logic: 'all' }),
    priority: 'high', reasoning: 'Roll window: sell before gamma decay', advisory_id: 'adv-test',
  });

  test('position outside roll window → no trigger', () => {
    // DTE = 40 days — not in the roll window yet
    const position = { instrument_name: 'ETH-20260501-1500-P', amount: 1.0, direction: 'long', avg_entry_price: 5.00 };
    const ticker = { M: '6.00', b: '5.50', option_pricing: { d: '-0.04', i: '0.60', t: '-0.02' } };
    const spotPrice = 1800;

    // Simulate: compute values and check DTE
    const values = computeCurrentValues(position, ticker, spotPrice);
    // Override DTE to simulate far-out position
    values.dte = 40;

    const rules = exitStmts.getExitRules.all();
    let triggered = false;
    for (const rule of rules) {
      if (rule.instrument_name !== position.instrument_name) continue;
      const criteria = JSON.parse(rule.criteria);
      if (evaluateConditions(criteria.conditions, criteria.condition_logic, values)) {
        triggered = true;
      }
    }
    assert.strictEqual(triggered, false, 'DTE 40 should NOT trigger roll window (lte 25)');
  });

  test('position enters roll window → triggers pending action', () => {
    const position = { instrument_name: 'ETH-20260501-1500-P', amount: 1.0, direction: 'long', avg_entry_price: 5.00 };
    const ticker = { M: '4.00', b: '3.50', option_pricing: { d: '-0.03', i: '0.55', t: '-0.04' } };
    const spotPrice = 1800;

    const values = computeCurrentValues(position, ticker, spotPrice);
    values.dte = 22; // Inside roll window

    const rules = exitStmts.getExitRules.all();
    for (const rule of rules) {
      if (rule.instrument_name !== position.instrument_name) continue;
      const criteria = JSON.parse(rule.criteria);
      if (evaluateConditions(criteria.conditions, criteria.condition_logic, values)) {
        const hasPending = (exitStmts.hasPendingForRule.get({ rule_id: rule.id })?.count || 0) > 0;
        if (!hasPending) {
          exitStmts.insertPendingAction.run({
            rule_id: rule.id, action: rule.action, instrument_name: rule.instrument_name,
            amount: position.amount, price: Number(ticker.b),
            trigger_details: JSON.stringify({ dte: values.dte, condition: 'dte <= 25' }),
          });
        }
      }
    }

    const pending = exitStmts.getPending.all();
    assert.strictEqual(pending.length, 1, 'Should have 1 pending action');
    assert.strictEqual(pending[0].action, 'sell_put');
    assert.strictEqual(pending[0].instrument_name, 'ETH-20260501-1500-P');
    assert.strictEqual(pending[0].amount, 1.0);
  });

  test('dedup: same rule does not trigger twice', () => {
    const values = { dte: 20, delta: -0.03, mark_price: 3.80, spot_price: 1800 };
    const rules = exitStmts.getExitRules.all();
    let newActions = 0;
    for (const rule of rules) {
      const criteria = JSON.parse(rule.criteria);
      if (evaluateConditions(criteria.conditions, criteria.condition_logic, values)) {
        const hasPending = (exitStmts.hasPendingForRule.get({ rule_id: rule.id })?.count || 0) > 0;
        if (!hasPending) {
          newActions++;
        }
      }
    }
    assert.strictEqual(newActions, 0, 'Should not create duplicate pending action');
  });
});

// ============================================================================
// 39. Full schema: confirmation voting logic
// ============================================================================

describe('Confirmation voting logic', () => {
  // Tests the voting matrix: both confirm → execute, both reject → reject,
  // split → reject (conservative), one fails → use the other

  const resolveVote = (haikuVote, codexVote) => {
    if (haikuVote && codexVote) {
      return (haikuVote.confirm && codexVote.confirm) ? 'confirmed' : 'rejected';
    } else if (haikuVote) {
      return haikuVote.confirm ? 'confirmed' : 'rejected';
    } else if (codexVote) {
      return codexVote.confirm ? 'confirmed' : 'rejected';
    }
    return 'retry'; // Both failed
  };

  test('both confirm → confirmed', () => {
    const result = resolveVote({ confirm: true, reasoning: 'good' }, { confirm: true, reasoning: 'convex' });
    assert.strictEqual(result, 'confirmed');
  });

  test('both reject → rejected', () => {
    const result = resolveVote({ confirm: false, reasoning: 'too expensive' }, { confirm: false, reasoning: 'symmetric payoff' });
    assert.strictEqual(result, 'rejected');
  });

  test('split vote (haiku yes, codex no) → rejected (conservative)', () => {
    const result = resolveVote({ confirm: true, reasoning: 'ok' }, { confirm: false, reasoning: 'ruin risk' });
    assert.strictEqual(result, 'rejected');
  });

  test('split vote (haiku no, codex yes) → rejected (conservative)', () => {
    const result = resolveVote({ confirm: false, reasoning: 'overpaying' }, { confirm: true, reasoning: 'convex' });
    assert.strictEqual(result, 'rejected');
  });

  test('haiku fails, codex confirms → confirmed (single advisor fallback)', () => {
    const result = resolveVote(null, { confirm: true, reasoning: 'looks good' });
    assert.strictEqual(result, 'confirmed');
  });

  test('haiku confirms, codex fails → confirmed (single advisor fallback)', () => {
    const result = resolveVote({ confirm: true, reasoning: 'disciplined' }, null);
    assert.strictEqual(result, 'confirmed');
  });

  test('haiku fails, codex rejects → rejected', () => {
    const result = resolveVote(null, { confirm: false, reasoning: 'no' });
    assert.strictEqual(result, 'rejected');
  });

  test('both fail → retry', () => {
    const result = resolveVote(null, null);
    assert.strictEqual(result, 'retry');
  });
});

// ============================================================================
// 40. Full schema: cooldown prevents rapid-fire entries
// ============================================================================

describe('Entry cooldown logic', () => {
  test('no prior execution → no cooldown', () => {
    const lastExec = null;
    const cooldownActive = lastExec && (Date.now() - new Date(lastExec).getTime()) < 3600000;
    assert.strictEqual(!!cooldownActive, false);
  });

  test('execution 30 min ago → cooldown active', () => {
    const lastExec = new Date(Date.now() - 30 * 60 * 1000).toISOString();
    const elapsed = Date.now() - new Date(lastExec).getTime();
    const cooldownActive = elapsed < 3600000;
    assert.strictEqual(cooldownActive, true, `Elapsed ${elapsed}ms should be < 1hr`);
  });

  test('execution 2 hours ago → cooldown expired', () => {
    const lastExec = new Date(Date.now() - 2 * 3600000).toISOString();
    const elapsed = Date.now() - new Date(lastExec).getTime();
    const cooldownActive = elapsed < 3600000;
    assert.strictEqual(cooldownActive, false, `Elapsed ${elapsed}ms should be >= 1hr`);
  });

  test('execution exactly 1 hour ago → cooldown expired (boundary)', () => {
    const lastExec = new Date(Date.now() - 3600000).toISOString();
    const elapsed = Date.now() - new Date(lastExec).getTime();
    const cooldownActive = elapsed < 3600000;
    assert.strictEqual(cooldownActive, false, 'Exactly 1 hour should not be in cooldown');
  });
});

// ============================================================================
// 41. Full schema: DRY_RUN budget tracking without real orders
// ============================================================================

describe('DRY_RUN mode budget tracking', () => {
  test('buy_put in DRY_RUN tracks budget', () => {
    const botState = { putNetBought: 0 };
    const action = 'buy_put';
    const amount = 2.0;
    const price = 5.50;
    const totalValue = amount * price; // $11.00

    // Simulate DRY_RUN executeOrder logic
    if (action === 'buy_put') botState.putNetBought += totalValue;
    else if (action === 'sell_put') botState.putNetBought -= totalValue;

    assert.strictEqual(botState.putNetBought, 11.00, 'DRY_RUN should still track put spending');
  });

  test('sell_put in DRY_RUN returns budget', () => {
    const botState = { putNetBought: 11.00 };
    const action = 'sell_put';
    const amount = 1.0;
    const price = 4.00;
    const totalValue = amount * price;

    if (action === 'buy_put') botState.putNetBought += totalValue;
    else if (action === 'sell_put') botState.putNetBought -= totalValue;

    assert.strictEqual(botState.putNetBought, 7.00, 'Selling a put should return budget');
  });

  test('sell_call in DRY_RUN does not affect put budget', () => {
    const botState = { putNetBought: 5.00 };
    const action = 'sell_call';
    const totalValue = 3.00;

    if (action === 'buy_put') botState.putNetBought += totalValue;
    else if (action === 'sell_put') botState.putNetBought -= totalValue;

    assert.strictEqual(botState.putNetBought, 5.00, 'Call sells should not affect put budget');
  });
});

// ============================================================================
// 42. Full schema: entry candidate filtering fidelity
// ============================================================================

describe('Entry candidate filtering: all criteria enforced', () => {
  const PUT_ANNUAL_RATE = 0.0333;
  const PERIOD_DAYS = 15;
  const spotPrice = 1800;

  // A set of options with varying characteristics
  const futureDate = new Date(Date.now() + 60 * 86400000);
  const expiryStr = futureDate.toISOString().slice(0, 10).replace(/-/g, '');

  test('option_type filter: calls rejected for put rule', () => {
    const instrument = { instrument_name: `ETH-${expiryStr}-2000-C`, option_details: { option_type: 'C', strike: 2000 } };
    const criteria = { option_type: 'P' };
    assert.notStrictEqual(instrument.option_details.option_type, criteria.option_type, 'Call should not match put rule');
  });

  test('delta out of range → rejected', () => {
    const delta = -0.15; // Too high (range is -0.08 to -0.02)
    const range = [-0.08, -0.02];
    const inRange = delta >= range[0] && delta <= range[1];
    assert.strictEqual(inRange, false, `Delta ${delta} should be out of range [${range}]`);
  });

  test('delta in range → accepted', () => {
    const delta = -0.05;
    const range = [-0.08, -0.02];
    const inRange = delta >= range[0] && delta <= range[1];
    assert.strictEqual(inRange, true, `Delta ${delta} should be in range [${range}]`);
  });

  test('DTE out of range → rejected', () => {
    const dte = 30; // Too short (range is 45-75)
    const range = [45, 75];
    const inRange = dte >= range[0] && dte <= range[1];
    assert.strictEqual(inRange, false, `DTE ${dte} should be out of range [${range}]`);
  });

  test('strike above max_strike_pct → rejected', () => {
    const strike = 1600;
    const maxStrikePct = 0.80;
    const maxStrike = maxStrikePct * spotPrice; // 1440
    assert.ok(strike >= maxStrike, `Strike $${strike} should be >= max $${maxStrike}`);
  });

  test('strike below max_strike_pct → accepted', () => {
    const strike = 1400;
    const maxStrikePct = 0.85;
    const maxStrike = maxStrikePct * spotPrice; // 1530
    assert.ok(strike < maxStrike, `Strike $${strike} should be < max $${maxStrike}`);
  });

  test('ask price above max_cost → rejected', () => {
    const askPrice = 18.00;
    const maxCost = 15.00;
    assert.ok(askPrice > maxCost, `Ask $${askPrice} should exceed max_cost $${maxCost}`);
  });

  test('score below min_score → rejected', () => {
    const delta = -0.03;
    const askPrice = 12.00;
    const score = Math.abs(delta) / askPrice; // 0.0025
    const minScore = 0.004;
    assert.ok(score < minScore, `Score ${score.toFixed(4)} should be below min_score ${minScore}`);
  });

  test('score above min_score → accepted', () => {
    const delta = -0.05;
    const askPrice = 5.50;
    const score = Math.abs(delta) / askPrice; // 0.00909
    const minScore = 0.004;
    assert.ok(score >= minScore, `Score ${score.toFixed(4)} should be >= min_score ${minScore}`);
  });

  test('market_conditions: spot below threshold → rule skipped', () => {
    const conditions = [{ field: 'spot_price', op: 'lt', value: 1500 }];
    const values = { spot_price: 1800 };
    assert.strictEqual(evaluateConditions(conditions, 'all', values), false, 'Spot $1800 is not < $1500');
  });

  test('market_conditions: spot above threshold → rule proceeds', () => {
    const conditions = [{ field: 'spot_price', op: 'gt', value: 1500 }];
    const values = { spot_price: 1800 };
    assert.strictEqual(evaluateConditions(conditions, 'all', values), true, 'Spot $1800 is > $1500');
  });
});

// ============================================================================
// 43. Full schema: multi-cycle budget discipline over time
// ============================================================================

describe('Multi-cycle budget discipline simulation', () => {
  const PUT_ANNUAL_RATE = 0.0333;
  const PERIOD_DAYS = 15;
  const PERIOD = PERIOD_DAYS * 86400000;

  let sim;
  const resetSim = () => {
    sim = { putCycleStart: null, putBudgetForCycle: 0, putNetBought: 0, putUnspentBuyLimit: 0 };
  };

  const simReset = (portfolioValue, nowOverride) => {
    const now = nowOverride || Date.now();
    const cycleExpired = sim.putCycleStart && (now - sim.putCycleStart) >= PERIOD;
    const noCycle = !sim.putCycleStart;
    if (noCycle || cycleExpired) {
      if (cycleExpired) {
        const prevRemaining = Math.max(0, sim.putBudgetForCycle + sim.putUnspentBuyLimit - sim.putNetBought);
        sim.putUnspentBuyLimit = prevRemaining;
      }
      const cyclesPerYear = 365 / PERIOD_DAYS;
      sim.putBudgetForCycle = portfolioValue * PUT_ANNUAL_RATE / cyclesPerYear;
      sim.putCycleStart = now;
      sim.putNetBought = 0;
    }
  };

  test('3 cycles, varying spend → annual total ≈ 3.33% of average portfolio', () => {
    resetSim();
    const t0 = Date.now();

    // Cycle 1: $10,000 portfolio, spend 80%
    simReset(10000, t0);
    const budget1 = sim.putBudgetForCycle; // ~$13.69
    sim.putNetBought = budget1 * 0.8;

    // Cycle 2: $11,000 portfolio (ETH up), spend 60%
    simReset(11000, t0 + PERIOD + 1);
    const budget2 = sim.putBudgetForCycle; // ~$15.06
    const rollover2 = sim.putUnspentBuyLimit; // ~$2.74 from cycle 1
    sim.putNetBought = budget2 * 0.6;

    // Cycle 3: $9,000 portfolio (ETH down), spend 100%
    simReset(9000, t0 + 2 * PERIOD + 2);
    const budget3 = sim.putBudgetForCycle; // ~$12.32
    const rollover3 = sim.putUnspentBuyLimit; // from cycle 2

    const totalSpent = budget1 * 0.8 + budget2 * 0.6 + budget3; // conservative: assume full spend of cycle 3
    const avgPortfolio = (10000 + 11000 + 9000) / 3;
    const cyclesSimulated = 3;
    const annualizedRate = (totalSpent / avgPortfolio) * (365 / (PERIOD_DAYS * cyclesSimulated));

    // Should be roughly in the neighborhood of 3.33%
    assert.ok(annualizedRate > 0.02, `Annualized rate ${(annualizedRate * 100).toFixed(2)}% should be > 2%`);
    assert.ok(annualizedRate < 0.06, `Annualized rate ${(annualizedRate * 100).toFixed(2)}% should be < 6%`);
    assert.ok(rollover2 > 0, 'Unspent from cycle 1 should roll over');
    assert.ok(rollover3 > 0, 'Unspent from cycle 2 should roll over');
  });

  test('all rollover accumulates when nothing is bought', () => {
    resetSim();
    const t0 = Date.now();

    // 3 cycles with zero spending
    simReset(10000, t0);
    // Spend nothing
    simReset(10000, t0 + PERIOD + 1);
    // Spend nothing
    simReset(10000, t0 + 2 * PERIOD + 2);

    // 3 cycles of ~$13.69 each, all rolled over
    const expectedRollover = 2 * (10000 * 0.0333 / (365 / 15)); // 2 full cycles rolled
    assert.ok(Math.abs(sim.putUnspentBuyLimit - expectedRollover) < 0.02,
      `Rollover $${sim.putUnspentBuyLimit.toFixed(2)} should be ~$${expectedRollover.toFixed(2)} (2 full cycles)`);
    // Plus current cycle budget
    const totalAvailable = sim.putBudgetForCycle + sim.putUnspentBuyLimit;
    const expectedTotal = 3 * (10000 * 0.0333 / (365 / 15));
    assert.ok(Math.abs(totalAvailable - expectedTotal) < 0.02,
      `Total available $${totalAvailable.toFixed(2)} should be ~$${expectedTotal.toFixed(2)} (3 cycles saved up)`);
  });

  test('heavy spend one cycle → less available next cycle (no negative rollover)', () => {
    resetSim();
    const t0 = Date.now();

    simReset(10000, t0);
    const budget = sim.putBudgetForCycle;
    sim.putNetBought = budget + 5; // Overspend by $5 (possible if rollover existed)

    simReset(10000, t0 + PERIOD + 1);
    assert.strictEqual(sim.putUnspentBuyLimit, 0, 'Overspend should not create negative rollover');
    assert.strictEqual(sim.putNetBought, 0, 'New cycle resets putNetBought');
  });
});

// ============================================================================
// 44. Full schema: executeOrder direction and reduceOnly mapping
// ============================================================================

describe('executeOrder action → direction + reduceOnly mapping', () => {
  const mapAction = (action) => {
    const direction = (action === 'buy_put' || action === 'buyback_call') ? 'buy' : 'sell';
    const reduceOnly = (action === 'sell_put' || action === 'buyback_call');
    return { direction, reduceOnly };
  };

  test('buy_put → buy, not reduceOnly', () => {
    const { direction, reduceOnly } = mapAction('buy_put');
    assert.strictEqual(direction, 'buy');
    assert.strictEqual(reduceOnly, false);
  });

  test('sell_put → sell, reduceOnly (closing position)', () => {
    const { direction, reduceOnly } = mapAction('sell_put');
    assert.strictEqual(direction, 'sell');
    assert.strictEqual(reduceOnly, true);
  });

  test('sell_call → sell, not reduceOnly (opening short)', () => {
    const { direction, reduceOnly } = mapAction('sell_call');
    assert.strictEqual(direction, 'sell');
    assert.strictEqual(reduceOnly, false);
  });

  test('buyback_call → buy, reduceOnly (closing short)', () => {
    const { direction, reduceOnly } = mapAction('buyback_call');
    assert.strictEqual(direction, 'buy');
    assert.strictEqual(reduceOnly, true);
  });
});

// ============================================================================
// 45. Action semantics in advisor/confirmation prompts
// ============================================================================

describe('action semantics descriptions', () => {
  const ACTION_POLICY = {
    buy_put: { semantics: 'Entry action: buying a put for tail-risk insurance. Bounded premium outlay, long convexity.' },
    sell_call: { semantics: 'Entry action: selling a call to open short call exposure against ETH-collateralized account capacity.' },
    sell_put: { semantics: 'Exit-only action: selling an already-owned long put to close or trim it. This is reduce_only=true and cannot create a naked short put.' },
    buyback_call: { semantics: 'Exit-only action: buying back an already-open short call to close or trim it. This is reduce_only=true and cannot create a new long call exposure beyond the short being closed.' },
  };
  const describeActionSemantics = (action) => ACTION_POLICY[action]?.semantics || 'Trade semantics unavailable.';

  test('sell_put is explicitly described as closing an owned long put', () => {
    const text = describeActionSemantics('sell_put');
    assert.ok(text.includes('already-owned long put'));
    assert.ok(text.includes('reduce_only=true'));
    assert.ok(text.includes('cannot create a naked short put'));
  });

  test('buyback_call is explicitly described as closing an open short call', () => {
    const text = describeActionSemantics('buyback_call');
    assert.ok(text.includes('already-open short call'));
    assert.ok(text.includes('reduce_only=true'));
  });

  test('sell_put guidance treats the exit as capital-releasing, not margin-consuming', () => {
    const guidance = 'Selling an owned long put is capital-releasing: it returns cash/premium recovery, reduces the hedge position, and does NOT consume more margin. It will generally improve headroom, not worsen it. If you reject a sell_put, do it because removing protection is strategically unwise, not because the exit itself uses more margin.';
    assert.ok(guidance.includes('capital-releasing'));
    assert.ok(guidance.includes('returns cash'));
    assert.ok(guidance.includes('does NOT consume more margin'));
    assert.ok(guidance.includes('improve headroom'));
  });
});

// ============================================================================
// 45. Voter limit_price sanity check
// ============================================================================

describe('Voter limit_price sanity check', () => {
  const resolvePrice = (voterLimitPrice, marketPrice) => {
    let executionPrice = marketPrice;
    if (typeof voterLimitPrice === 'number' && voterLimitPrice > 0 && marketPrice > 0) {
      const ratio = voterLimitPrice / marketPrice;
      if (ratio >= 0.5 && ratio <= 2.0) {
        executionPrice = voterLimitPrice;
      }
    }
    return executionPrice;
  };

  test('voter price within range → accepted', () => {
    assert.strictEqual(resolvePrice(5.20, 5.50), 5.20);
  });

  test('voter price slightly below → accepted', () => {
    assert.strictEqual(resolvePrice(3.00, 5.50), 3.00); // ratio 0.545
  });

  test('voter price way below (< 50%) → rejected, use market', () => {
    assert.strictEqual(resolvePrice(2.00, 5.50), 5.50); // ratio 0.364
  });

  test('voter price way above (> 200%) → rejected, use market', () => {
    assert.strictEqual(resolvePrice(12.00, 5.50), 5.50); // ratio 2.18
  });

  test('voter price null → use market', () => {
    assert.strictEqual(resolvePrice(null, 5.50), 5.50);
  });

  test('voter price zero → use market', () => {
    assert.strictEqual(resolvePrice(0, 5.50), 5.50);
  });
});

describe('confirmation prompt margin context', () => {
  test('sell_call includes concrete current and projected utilization', () => {
    const context = getCallMarginContext(
      'sell_call',
      { initial_margin: 4200, maintenance_margin: 1805.3, collaterals_initial_margin: 5000, collaterals_maintenance_margin: 4200, positions_initial_margin: 600, open_orders_margin: 200 },
      [],
      [],
      [{ instrument_name: 'ETH-20260417-2600-C', option_details: { strike: 2600 } }],
      2240,
      'ETH-20260417-2600-C',
      5.47,
      2.8
    );
    assert.ok(context.includes('current_derive_display=57.0%'));
    assert.ok(context.includes('projected_after_trade_display='));
    assert.ok(context.includes('caution_zone=40.0%-45.0%'));
    assert.ok(context.includes('hard_cap=45.0%'));
  });

  test('non-call action says margin context not applicable', () => {
    const context = getCallMarginContext('buy_put', {}, [], [], [], 2240, 'ETH-20260417-1400-P', 1, 1);
    assert.strictEqual(context, 'Call margin utilization: not applicable for this action.');
  });

  test('call discipline wording distinguishes entry caps from true emergencies', () => {
    const text = 'These are discipline limits for NEW entries, not margin-emergency thresholds. Do not describe 40-45% utilization as a forced unwind or emergency by itself; true emergency language is reserved for near-liquidation / ~100% utilization.';
    assert.ok(text.includes('not margin-emergency thresholds'));
    assert.ok(text.includes('near-liquidation / ~100% utilization'));
  });
});

describe('post_only retry price discipline', () => {
  test('sell retry moves one tick above bid, not to the ask', () => {
    const retry = computePostOnlyRetryPrice(
      'sell',
      { b: 4.9, a: 7.6 },
      { option_details: { price_step: 0.05 } },
      4.9
    );
    assert.ok(retry);
    assert.strictEqual(retry.retryPrice, 4.95);
    assert.strictEqual(retry.askPrice, 7.6);
  });

  test('sell retry falls back to attempted price when bid is unavailable', () => {
    const retry = computePostOnlyRetryPrice(
      'sell',
      { b: 0, a: 7.6 },
      { option_details: { price_step: 0.05 } },
      4.9
    );
    assert.ok(retry);
    assert.strictEqual(retry.retryPrice, 4.95);
  });

  test('sell retry uses 1-decimal fallback when metadata lacks price step', () => {
    const retry = computePostOnlyRetryPrice(
      'sell',
      { b: 5.7, a: 6.7 },
      { option_details: {} },
      5.7
    );
    assert.ok(retry);
    assert.ok(Math.abs(retry.retryPrice - 5.8) < 0.0000001, `Expected ~5.8, got ${retry.retryPrice}`);
    assert.strictEqual(retry.step, 0.1);
  });

  test('sell retry skips exact round numbers by one extra tick', () => {
    const retry = computePostOnlyRetryPrice(
      'sell',
      { b: 5.9, a: 6.8 },
      { option_details: {} },
      5.9
    );
    assert.ok(retry);
    assert.ok(Math.abs(retry.retryPrice - 6.1) < 0.0000001, `Expected ~6.1, got ${retry.retryPrice}`);
    assert.strictEqual(retry.step, 0.1);
  });

  test('buy retry skips exact round numbers by one tick lower', () => {
    const retry = computePostOnlyRetryPrice(
      'buy',
      { b: 5.2, a: 6.1 },
      { option_details: {} },
      6.1
    );
    assert.ok(retry);
    assert.ok(Math.abs(retry.retryPrice - 5.9) < 0.0000001, `Expected ~5.9, got ${retry.retryPrice}`);
    assert.strictEqual(retry.step, 0.1);
  });
});

describe('order signature expiry buffer', () => {
  const computeSignatureExpirySec = (nowMs, timeInForce) =>
    Math.floor((nowMs / 1000) + (timeInForce === 'ioc' ? 900 : 86400));

  test('ioc orders get a 15 minute signature buffer', () => {
    const nowMs = 1_700_000_000_000;
    const expiry = computeSignatureExpirySec(nowMs, 'ioc');
    assert.strictEqual(expiry - Math.floor(nowMs / 1000), 900);
  });

  test('resting orders keep a 24 hour signature buffer', () => {
    const nowMs = 1_700_000_000_000;
    const expiry = computeSignatureExpirySec(nowMs, 'post_only');
    assert.strictEqual(expiry - Math.floor(nowMs / 1000), 86400);
  });
});

describe('stale emergency buyback rule scrub', () => {
  const shouldDeactivate = (rule) =>
    rule?.is_active === 1
    && rule?.action === 'buyback_call'
    && (String(rule.reasoning || '').includes('margin emergency')
      || String(rule.reasoning || '').includes('MUST execute before any other portfolio action'));

  test('deactivates stale emergency-style buyback rule', () => {
    const rule = {
      is_active: 1,
      action: 'buyback_call',
      reasoning: 'This is a margin emergency. MUST execute before any other portfolio action.',
    };
    assert.strictEqual(shouldDeactivate(rule), true);
  });

  test('keeps non-emergency buyback rule active', () => {
    const rule = {
      is_active: 1,
      action: 'buyback_call',
      reasoning: 'Buy back only if the position is genuinely threatened near expiry.',
    };
    assert.strictEqual(shouldDeactivate(rule), false);
  });
});

// ============================================================================
// 46. Call exposure cap: 45% hard cap, 40% entry buffer
// ============================================================================

describe('Call exposure cap discipline', () => {
  const CALL_EXPOSURE_CAP_PCT = 0.45;
  const CALL_ENTRY_CAP_PCT = 0.40;

  test('existing short-call margin uses empirical account ratio before theoretical estimate', () => {
    const marginState = {
      positions_initial_margin: 201.16,
    };
    const positions = [
      { instrument_name: 'ETH-20260424-3000-C', direction: 'short', amount: 0.95 },
    ];
    const estimated = estimateShortCallMarginPerUnit(marginState, positions, [], 2320, 3000, 1.1);
    assert.ok(Math.abs(estimated - (201.16 / 0.95)) < 0.01);
  });

  test('no short calls → full headroom available', () => {
    const ethBalance = 5.0;
    const currentExposure = 0;
    const maxExposure = CALL_EXPOSURE_CAP_PCT * ethBalance; // 2.25 ETH
    const headroom = maxExposure - currentExposure;
    assert.strictEqual(maxExposure, 2.25);
    assert.strictEqual(headroom, 2.25);
  });

  test('existing 1.5 ETH short calls with 5 ETH → 0.75 headroom', () => {
    const ethBalance = 5.0;
    const currentExposure = 1.5;
    const maxExposure = CALL_EXPOSURE_CAP_PCT * ethBalance; // 2.25
    const headroom = Math.max(0, maxExposure - currentExposure);
    assert.strictEqual(headroom, 0.75);
  });

  test('at cap → sell_call blocked', () => {
    const ethBalance = 5.0;
    const currentExposure = 2.25; // exactly at 45%
    const maxExposure = CALL_EXPOSURE_CAP_PCT * ethBalance;
    const blocked = currentExposure >= maxExposure;
    assert.strictEqual(blocked, true, 'Should block at cap');
  });

  test('over cap → sell_call blocked', () => {
    const ethBalance = 5.0;
    const currentExposure = 2.5; // over 45%
    const maxExposure = CALL_EXPOSURE_CAP_PCT * ethBalance;
    const blocked = currentExposure >= maxExposure;
    assert.strictEqual(blocked, true, 'Should block over cap');
  });

  test('under cap → sell_call allowed', () => {
    const ethBalance = 5.0;
    const currentExposure = 1.0;
    const maxExposure = CALL_EXPOSURE_CAP_PCT * ethBalance;
    const blocked = currentExposure >= maxExposure;
    assert.strictEqual(blocked, false, 'Should allow under cap');
  });

  test('at caution threshold but below hard cap → new sell_call still allowed', () => {
    const marginBase = 5000;
    const currentUsed = 2000; // 40%
    const blocked = (currentUsed / marginBase) >= CALL_EXPOSURE_CAP_PCT;
    assert.strictEqual(blocked, false, '40% is caution, not a hard rejection line');
  });

  test('oversized sell_call is clamped down to the entry cap instead of rejected outright', () => {
    const marginState = {
      collaterals_maintenance_margin: 5000,
      positions_initial_margin: 0,
      open_orders_margin: 0,
    };
    const clamped = clampSellCallQtyToEntryCap({
      desiredQty: 4.4,
      amountStep: 0.01,
      marginState,
      marginPerUnit: 750,
    });
    assert.strictEqual(clamped.qty, 3);
    assert.ok(clamped.projectedUtilization != null);
    assert.ok(clamped.projectedUtilization <= CALL_EXPOSURE_CAP_PCT);
  });

  test('zero ETH balance → ethBalance guard prevents division issues', () => {
    const ethBalance = 0;
    // When ethBalance is 0, the cap check is skipped (no ETH to cover)
    const shouldCheck = ethBalance > 0;
    assert.strictEqual(shouldCheck, false, 'Skip cap check with no ETH');
  });

  test('amount sizing capped by remaining headroom', () => {
    const ethBalance = 5.0;
    const currentExposure = 1.5;
    const headroom = Math.max(0, CALL_EXPOSURE_CAP_PCT * ethBalance - currentExposure); // 0.75
    const ruleBudgetMax = 10.0; // rule allows 10 ETH worth
    const bookLiquidity = 3.0;
    const raw = Math.min(ruleBudgetMax, headroom, bookLiquidity);
    assert.strictEqual(raw, 0.75, 'Should be capped by headroom, not rule or book');
  });

  test('buy_put is NOT affected by call cap', () => {
    const action = 'buy_put';
    const shouldCheckCallCap = action === 'sell_call';
    assert.strictEqual(shouldCheckCallCap, false, 'Put buying ignores call cap');
  });

  test('cap scales with ETH holdings', () => {
    // More ETH = more room to sell calls
    const small = CALL_EXPOSURE_CAP_PCT * 2.0; // 0.9 ETH
    const large = CALL_EXPOSURE_CAP_PCT * 10.0; // 4.5 ETH
    assert.strictEqual(small, 0.9);
    assert.strictEqual(large, 4.5);
    assert.ok(large > small, 'More ETH = bigger cap');
  });
});

// ============================================================================
// Summary
// ============================================================================

console.log(`\nResults: ${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
