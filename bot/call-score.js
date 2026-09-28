'use strict';

// DTE correction for bid / abs(delta), referenced to the 5-12 DTE window's midpoint.
// Calibrated so the best candidate's score does not drift as an expiry ages from 12 to
// 5 DTE (scripts/study-call-dte-exponent.js, Feb-Sep 2026): the weekly slide is flat at
// 0.6, where 0.12 left a +31% jump at each weekly rollover and a steady slide after it.
// The dashboard's lib/db.ts keeps a copy; change both together.
const SELL_CALL_EDGE_REFERENCE_DTE = 8.5;
const SELL_CALL_EDGE_DTE_EXPONENT = 0.6;

const normalizeSellCallScore = (rawScore, dte) => {
  const raw = Number(rawScore);
  const days = Number(dte);
  if (!(raw > 0) || !(days > 0)) return 0;
  return raw * Math.pow(SELL_CALL_EDGE_REFERENCE_DTE / days, SELL_CALL_EDGE_DTE_EXPONENT);
};

module.exports = {
  SELL_CALL_EDGE_REFERENCE_DTE,
  SELL_CALL_EDGE_DTE_EXPONENT,
  normalizeSellCallScore,
};
