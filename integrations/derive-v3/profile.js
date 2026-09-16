'use strict';

const V2_IDENTITY = Object.freeze({ owner: '0xD87890df93bf74173b51077e5c6cD12121d87903', subaccount_id: 25923 });

// This module deliberately has no SDK dependency. V2 never loads the V3 SDK.
function readProfile(env = process.env) {
  const name = env.NOOP_VENUE || 'v2';
  if (name === 'v2') return Object.freeze({ name, version: 2 });
  if (!['v3-testnet', 'v3-mainnet'].includes(name)) throw new Error(`Unknown NOOP_VENUE: ${name}`);
  if (name === 'v3-mainnet' && env.DERIVE_V3_MAINNET_RELEASE !== 'verified') {
    throw new Error('V3 mainnet is gated: verify release, migration and reconciliation before enabling it');
  }
  const network = name === 'v3-testnet' ? 'testnet' : 'mainnet';
  const prefix = network === 'testnet' ? 'DERIVE_V3_TESTNET_' : 'DERIVE_V3_MAINNET_';
  const execution = env[`${prefix}EXECUTION`] || 'disabled';
  if (!['enabled', 'disabled'].includes(execution)) throw new Error(`${prefix}EXECUTION must be enabled or disabled`);
  const id = env[`${prefix}SUBACCOUNT_ID`];
  const subaccountId = id == null || id === '' ? null : Number(id);
  if (subaccountId !== null && (!/^\d+$/.test(id) || !Number.isSafeInteger(subaccountId) || subaccountId <= 0)) {
    throw new Error(`${prefix}SUBACCOUNT_ID must be a positive safe integer`);
  }
  const ownerAddress = env[`${prefix}OWNER_ADDRESS`] || null;
  if (ownerAddress && !/^0x[0-9a-fA-F]{40}$/.test(ownerAddress)) throw new Error(`${prefix}OWNER_ADDRESS is invalid`);
  return Object.freeze({
    name, version: 3, network, prefix, subaccountId, ownerAddress,
    executionEnabled: execution === 'enabled',
    keyEnv: `${prefix}PRIVATE_KEY`,
    keyFile: env[`${prefix}KEY_FILE`] || null,
    httpUrl: network === 'testnet' ? 'https://testnet.api.derive.xyz/v3' : 'https://api.derive.xyz/v3',
  });
}

function loadKey(profile, env = process.env) {
  // Never fall back to PRIVATE_KEY or the production .private_key.txt.
  const fs = require('node:fs');
  const key = env[profile.keyEnv] || (profile.keyFile ? fs.readFileSync(profile.keyFile, 'utf8').trim() : null);
  if (!key) throw new Error(`Set ${profile.keyEnv} or ${profile.prefix}KEY_FILE (dedicated V3 key only)`);
  const normalized = key.startsWith('0x') ? key : `0x${key}`;
  if (!/^0x[0-9a-fA-F]{64}$/.test(normalized)) throw new Error('Invalid dedicated V3 signing key');
  return normalized;
}

module.exports = { readProfile, loadKey, V2_IDENTITY };
