'use strict';

// One home for the put-buying budget rule.
// PUT_PROTECTED_ETH is the total ETH the strategy protects, including the ETH
// held as Derive collateral. Each PERIOD_DAYS window may buy PUT_WINDOW_RATE of
// that ETH at spot, so about four windows of spend (~3%) are held in puts at any
// time. Call premium pays for the puts.

const positive = value => (Number.isFinite(Number(value)) && Number(value) > 0 ? Number(value) : 0);

function assertPutBudgetConfig(config) {
  if (!(positive(config?.PUT_PROTECTED_ETH) > 0)) throw new Error('bot/config.json PUT_PROTECTED_ETH must be a positive ETH amount');
  if (!(positive(config?.PUT_WINDOW_RATE) > 0 && config.PUT_WINDOW_RATE < 1)) throw new Error('bot/config.json PUT_WINDOW_RATE must be a fraction between 0 and 1');
}

const protectedValueUsd = (config, spot) => positive(config.PUT_PROTECTED_ETH) * positive(spot);

const putWindowBudgetUsd = (config, spot) => protectedValueUsd(config, spot) * positive(config.PUT_WINDOW_RATE);

// ETH counted outside Derive, so Derive ETH plus off-Derive ETH equals the protected amount.
const offDeriveEth = (config, deriveEth) => Math.max(0, positive(config.PUT_PROTECTED_ETH) - positive(deriveEth));

const describePutBudget = config => `${positive(config.PUT_PROTECTED_ETH)} ETH protected in total (including the ETH held as Derive collateral). `
  + `Each ${config.PERIOD_DAYS}-day window may buy puts costing ${(positive(config.PUT_WINDOW_RATE) * 100).toFixed(2)}% of that ETH at spot, `
  + `so about ${(positive(config.PUT_WINDOW_RATE) * 4 * 100).toFixed(0)}% of protected value is held in puts across four windows. `
  + 'Call premium pays for the puts. Unspent budget rolls over.';

module.exports = { assertPutBudgetConfig, protectedValueUsd, putWindowBudgetUsd, offDeriveEth, describePutBudget };
