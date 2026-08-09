'use strict';

// A light DTE correction for bid / abs(delta). The reference is the midpoint
// of the 5-12 DTE strategy window. A dense Railway-history backtest found 0.12
// to be the two-decimal upper edge of the stable trading plateau; the first
// decision/P&L cliff appeared at 0.1220.
const SELL_CALL_EDGE_REFERENCE_DTE = 8.5;
const SELL_CALL_EDGE_DTE_EXPONENT = 0.12;

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
