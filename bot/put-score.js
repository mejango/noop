'use strict';

// DTE normalization for abs(delta) / ask. Production history showed that the
// raw score rises mechanically as a put ages, then resets when the available
// long-dated expiry rolls forward. A 60-DTE reference and 0.8 exponent remove
// most of that calendar effect while retaining the market-driven signal.
const BUY_PUT_EDGE_REFERENCE_DTE = 60;
const BUY_PUT_EDGE_DTE_EXPONENT = 0.8;
const BUY_PUT_EDGE_MIN_DTE = 45;
const BUY_PUT_EDGE_MAX_DTE = 78;

const getBuyPutDteNormalizationFactor = (dte) => {
  const days = Number(dte);
  if (!(days > 0)) return 0;
  return Math.pow(days / BUY_PUT_EDGE_REFERENCE_DTE, BUY_PUT_EDGE_DTE_EXPONENT);
};

const normalizeBuyPutScore = (rawScore, dte) => {
  const raw = Number(rawScore);
  const factor = getBuyPutDteNormalizationFactor(dte);
  if (!(raw > 0) || !(factor > 0)) return 0;
  return raw * factor;
};

const getBuyPutPriceForEdgeScore = (absDelta, edgeScore, dte) => {
  const delta = Math.abs(Number(absDelta));
  const edge = Number(edgeScore);
  const factor = getBuyPutDteNormalizationFactor(dte);
  if (!(delta > 0) || !(edge > 0) || !(factor > 0)) return null;
  return (delta * factor) / edge;
};

module.exports = {
  BUY_PUT_EDGE_REFERENCE_DTE,
  BUY_PUT_EDGE_DTE_EXPONENT,
  BUY_PUT_EDGE_MIN_DTE,
  BUY_PUT_EDGE_MAX_DTE,
  getBuyPutDteNormalizationFactor,
  normalizeBuyPutScore,
  getBuyPutPriceForEdgeScore,
};
