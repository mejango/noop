'use strict';

const stringifyApiError = (err) => {
  if (!err) return 'unknown error';
  if (typeof err === 'string') return err;
  try {
    return JSON.stringify(err);
  } catch {
    return String(err);
  }
};

// Malformed-request codes and Derive's rate limiter (HTTP 500, -32000
// "Rate limit exceeded ... Retry after N ms") mean the order was never accepted.
const isDefinitiveVenueRejection = (apiError) => (
  // 14038: signature outlives the session key; the venue refused it outright.
  [-32600, -32602, 14038].includes(Number(apiError?.code)) || /rate limit exceeded/i.test(String(apiError?.message || ''))
);

// How long Derive asks a rate-limited caller to wait, or null when the error is not its rate
// limiter. Bounded so a caller never stalls on a malformed hint.
const rateLimitWaitMs = (apiError) => {
  if (!/rate limit exceeded/i.test(String(apiError?.message || ''))) return null;
  const hinted = Number(/retry after (\d+) ms/i.exec(String(apiError?.data || ''))?.[1]);
  return Math.min(5000, Math.max(250, Number.isFinite(hinted) ? hinted : 1000));
};

const isIocNoLiquidityError = (err) => {
  const code = Number(err?.code ?? err?.error?.code);
  if (code === 11009) return true;
  const text = stringifyApiError(err).toLowerCase();
  return text.includes('zero liquidity for market or ioc/fok order')
    || text.includes('no liquidity within the provided limit price');
};

// This additional contract is for direct order-submission refusals, never
// transfer errors, quote failures, transport failures or history absence.
function isDefinitiveOrderRejection(evidence) {
  const response = evidence?.response;
  if (evidence?.method !== 'private/order' || !evidence.request || typeof evidence.request !== 'object'
    || !response || typeof response !== 'object' || Array.isArray(response)
    || !response.error || typeof response.error !== 'object' || Array.isArray(response.error)
    || response.result != null || response.order != null) return false;
  const seen = new Set(), pending = [response];
  while (pending.length) {
    const value = pending.pop();
    if (!value || typeof value !== 'object' || seen.has(value)) continue;
    seen.add(value);
    if (value.order_id != null) return false;
    pending.push(...Object.values(value));
  }
  return isDefinitiveVenueRejection(response.error)
    || (evidence.request.time_in_force === 'ioc' && isIocNoLiquidityError(response.error));
}

module.exports = { rateLimitWaitMs, stringifyApiError, isDefinitiveVenueRejection, isIocNoLiquidityError, isDefinitiveOrderRejection };
