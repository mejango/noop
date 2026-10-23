'use strict';

const { analyzeMomentum } = require('./market-momentum');
const quiet = { log() {} };
const HOUR = 3600000;

function assetMomentum(store, asset, observedAt, spot) {
  const at = Date.parse(observedAt);
  if (!Number.isFinite(at) || !Number.isFinite(spot) || spot <= 0) return {};
  const history = store.getAssetPriceHistory(asset, observedAt);
  const prices = [...history, { price: spot, timestamp: at }];
  const shortReady = history.some(row => row.timestamp >= at - 30 * 60000 && row.timestamp < at - 15 * 60000);
  // ETH's ADX/MACD needs 150 ten-minute bars; wait for real history too, rather
  // than presenting its insufficient-data neutral fallback as a market signal.
  const mediumHistory = history.filter(row => row.timestamp >= at - 25 * HOUR);
  const mediumReady = mediumHistory.length >= 50 && mediumHistory[0].timestamp <= at - 25 * HOUR + 10 * 60000;
  if (!shortReady && !mediumReady) return {};
  const result = analyzeMomentum(prices, { now: at, logger: quiet });
  return {
    short_momentum_main: shortReady ? result.shortTermMomentum.main : null,
    short_momentum_derivative: shortReady ? result.shortTermMomentum.derivative : null,
    medium_momentum_main: mediumReady ? result.mediumTermMomentum.main : null,
    medium_momentum_derivative: mediumReady ? result.mediumTermMomentum.derivative : null,
  };
}

module.exports = { assetMomentum };
