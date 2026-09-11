'use strict';

// Pure V2 venue price normalization shared by planning, submission and tests.
const getInstrumentPriceStep = (instrument, fallbackPrice = 0) => {
  const configuredStep = Number(
    instrument?.price_step ??
    instrument?.options?.price_step ??
    instrument?.option_details?.price_step ??
    0
  );
  const isOption = Boolean(instrument?.option_details?.option_type || instrument?.base_asset_sub_id);
  if (isOption) {
    // Derive option orders currently reject >1 decimal place even when some metadata
    // is missing or too fine, e.g. "limit price 0.85 must not have more than 1 decimals".
    return Math.max(configuredStep || 0, 0.1);
  }
  if (configuredStep > 0) return configuredStep;
  return fallbackPrice >= 1 ? 0.1 : 0.01;
};

const roundToStep = (value, step, mode = 'nearest') => {
  if (!(step > 0)) return value;
  const scaledValue = value / step;
  const nearestInteger = Math.round(scaledValue);
  const scaled = Math.abs(scaledValue - nearestInteger) <= Math.max(1, Math.abs(scaledValue)) * Number.EPSILON * 4
    ? nearestInteger
    : scaledValue;
  if (mode === 'up') return Math.ceil(scaled) * step;
  if (mode === 'down') return Math.floor(scaled) * step;
  return Math.round(scaled) * step;
};

const getStepDecimals = (step) => {
  if (!(step > 0)) return 8;
  const normalized = String(step);
  if (normalized.includes('e-')) {
    const [, exponent] = normalized.split('e-');
    return Number(exponent) || 0;
  }
  const [, fraction = ''] = normalized.split('.');
  return fraction.length;
};

const normalizePriceToStep = (value, step, mode = 'nearest') => {
  if (!(Number(value) > 0)) return 0;
  if (!(step > 0)) return Number(value);
  const rounded = roundToStep(Number(value), step, mode);
  const decimals = getStepDecimals(step);
  return Number(rounded.toFixed(decimals));
};

const normalizeOrderPriceForVenue = (price, instrument, direction = 'buy') => {
  const step = getInstrumentPriceStep(instrument, Number(price));
  const mode = direction === 'buy' ? 'down' : direction === 'sell' ? 'up' : 'nearest';
  const normalized = normalizePriceToStep(price, step, mode);
  return {
    price: normalized,
    step,
    mode,
  };
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
    if (!(Number(attemptedPrice) > 0)) return null;
    const retryBase = Math.max(Number(attemptedPrice), bidPrice > 0 ? bidPrice + step : Number(attemptedPrice) + step);
    const retryPrice = avoidRoundNumberRestingPrice(direction, normalizePriceToStep(retryBase, step, 'up'), step);
    return retryPrice > 0 ? { retryPrice, bidPrice, askPrice, step } : null;
  }

  if (askPrice <= 0 || !(Number(attemptedPrice) > 0)) return null;
  const belowAsk = askPrice - step;
  const candidate = belowAsk > 0
    ? normalizePriceToStep(belowAsk, step, 'down')
    : normalizePriceToStep(askPrice * 0.99, step, 'down');
  // A refreshed ask must never make a buy retry exceed the approved bid.
  const cappedCandidate = normalizePriceToStep(Math.min(candidate, Number(attemptedPrice)), step, 'down');
  const retryPrice = avoidRoundNumberRestingPrice(direction, cappedCandidate, step);
  return retryPrice > 0 && retryPrice < askPrice && retryPrice <= Number(attemptedPrice)
    ? { retryPrice, bidPrice, askPrice, step }
    : null;
};

module.exports = { getInstrumentPriceStep, roundToStep, getStepDecimals, normalizePriceToStep, normalizeOrderPriceForVenue, avoidRoundNumberRestingPrice, computePostOnlyRetryPrice };
