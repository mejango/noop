'use strict';

const { compareDecimals, normalizeDecimal } = require('../../strategy/decimal');

const PARAMETER_DEFINITIONS = Object.freeze(Object.fromEntries(Object.entries({
  put_roll_dte: ['day', '0', '365', '25'],
  call_profit_capture_pct: ['percent', '0', '99.999', '80'],
  call_entry_margin_ratio: ['ratio', '0', '1', '0.45'],
  call_breakout_margin_ratio: ['ratio', '0', '1', '0.65'],
  call_execution_buffer_ratio: ['ratio', '0', '1', '0.05'],
  call_entry_caution_buffer_ratio: ['ratio', '0', '1', '0.05'],
  put_annual_rate: ['ratio', '0', '1', '0.0333'],
  put_budget_period_days: ['day', '1', '365', '15'],
  put_monetization_profit_pct: ['percent', '0', '1000000', '1000'],
  put_monetization_tranche_ratio: ['ratio', '0.0000000001', '0.9999999999', '0.25'],
  put_min_dte: ['day', '0.0001', '365', '45'],
  put_max_dte: ['day', '0.0001', '365', '78'],
  put_min_delta: ['ratio', '-1', '-0.00000001', '-0.12'],
  put_max_delta: ['ratio', '-1', '-0.00000001', '-0.02'],
  call_min_dte: ['day', '0.0001', '365', '5'],
  call_max_dte: ['day', '0.0001', '365', '12'],
  call_min_delta: ['ratio', '0.00000001', '1', '0.04'],
  call_max_delta: ['ratio', '0.00000001', '1', '0.12'],
  call_fallback_min_bid: ['USDC/contract', '0.00000001', '1000000', '4'],
  call_fallback_min_score: ['count', '0.00000001', '1000000000', '65'],
  put_score_reference_dte: ['day', '0.0001', '365', '60'],
  put_score_dte_exponent: ['ratio', '0', '10', '0.8'],
  call_score_reference_dte: ['day', '0.0001', '365', '8.5'],
  call_score_dte_exponent: ['ratio', '0', '10', '0.12'],
}).map(([name, [unit, minimum, maximum, defaultValue]]) => [name, Object.freeze({
  type: 'decimal', unit, minimum, maximum, default: defaultValue,
  description: `Accepted reference Strategy parameter: ${name}.`,
})])));

const DEFAULT_PARAMETERS = Object.freeze(Object.fromEntries(
  Object.entries(PARAMETER_DEFINITIONS).map(([name, definition]) => [name, definition.default])
));

function materializeParameters(overrides = {}, { requireComplete = false } = {}) {
  if (!overrides || typeof overrides !== 'object' || Array.isArray(overrides)) throw new TypeError('parameters must be an object');
  for (const name of Object.keys(overrides)) {
    if (!Object.hasOwn(PARAMETER_DEFINITIONS, name)) throw new TypeError(`Unknown reference parameter: ${name}`);
  }
  const result = {};
  for (const [name, definition] of Object.entries(PARAMETER_DEFINITIONS)) {
    if (requireComplete && !Object.hasOwn(overrides, name)) throw new TypeError(`Missing materialized parameter: ${name}`);
    const value = Object.hasOwn(overrides, name) ? overrides[name] : definition.default;
    if (compareDecimals(value, definition.minimum) < 0 || compareDecimals(value, definition.maximum) > 0) {
      throw new RangeError(`Reference parameter outside accepted bounds: ${name}`);
    }
    result[name] = normalizeDecimal(value);
  }
  for (const [minimum, maximum] of [
    ['put_min_dte', 'put_max_dte'], ['call_min_dte', 'call_max_dte'],
    ['put_min_delta', 'put_max_delta'], ['call_min_delta', 'call_max_delta'],
    ['call_entry_margin_ratio', 'call_breakout_margin_ratio'],
  ]) {
    if (compareDecimals(result[minimum], result[maximum]) > 0) throw new RangeError(`${minimum} must not exceed ${maximum}`);
  }
  return Object.freeze(result);
}

module.exports = { PARAMETER_DEFINITIONS, DEFAULT_PARAMETERS, materializeParameters };
