'use strict';

// derive-ts 3.0.13 ships older Sepolia custody addresses. Pin the deployment
// published at https://docs.derive.xyz/getting-started/contracts on 2026-09-11.
// USDC is the underlying ERC-20 returned by public/get_risk_universes that day.
const TESTNET_CONTRACTS = Object.freeze({
  actionManager: '0x842C2306A17354f58Cb00853Ce3B71fB27F83557',
  usdc: '0x73Efab09362052D26FB93A730Be4F8a5EdC833af',
});

function sdkNetwork(name) {
  const { NETWORKS } = require('@derivexyz/derive-ts');
  const preset = NETWORKS[name];
  if (!preset) throw new Error('Unsupported Derive network');
  return name === 'testnet' ? { ...preset, contracts: { ...preset.contracts, ...TESTNET_CONTRACTS } } : preset;
}

module.exports = { TESTNET_CONTRACTS, sdkNetwork };
