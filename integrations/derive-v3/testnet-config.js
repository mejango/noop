'use strict';

const fs = require('node:fs');
const path = require('node:path');
const filename = path.resolve(__dirname, '../../.derive-v3/testnet/account.json');

function testnetEnvironment(env = process.env) {
  if (env.NOOP_VENUE && env.NOOP_VENUE !== 'v3-testnet') throw new Error('This command is testnet-only');
  const saved = fs.existsSync(filename) ? JSON.parse(fs.readFileSync(filename, 'utf8')) : {};
  return { DERIVE_V3_TESTNET_OWNER_ADDRESS: saved.owner_address,
    DERIVE_V3_TESTNET_SUBACCOUNT_ID: saved.subaccount_id == null ? undefined : String(saved.subaccount_id),
    DERIVE_V3_TESTNET_KEY_FILE: saved.key_file,
    ...env, NOOP_VENUE: 'v3-testnet' };
}

function saveTestnetAccount(account) {
  fs.mkdirSync(path.dirname(filename), { recursive: true, mode: 0o700 });
  fs.writeFileSync(filename, JSON.stringify(account, null, 2), { mode: 0o600 });
}

module.exports = { testnetEnvironment, saveTestnetAccount };
