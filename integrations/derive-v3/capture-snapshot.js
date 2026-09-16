'use strict';

// Explicit, read-only capture for the eventual handoff. Never starts either bot.
const fs = require('node:fs');
const path = require('node:path');
const { Wallet } = require('ethers');
const { readProfile, V2_IDENTITY } = require('./profile');
const { DeriveV3 } = require('./index');
const { validateSnapshot, SNAPSHOT_SCHEMA_VERSION } = require('./handoff');

function orderList(result, method) {
  const orders = Array.isArray(result) ? result : result?.orders;
  if (!Array.isArray(orders)) throw new Error(`${method}: missing complete order list`);
  return orders;
}

async function capture(mode) {
  const timestamp = new Date().toISOString();
  let account, openOrders, triggerOrders, algoOrders, owner;
  if (mode === 'v2') {
    owner = V2_IDENTITY.owner;
    const key = process.env.PRIVATE_KEY || fs.readFileSync(path.resolve(__dirname, '../../.private_key.txt'), 'utf8').trim();
    const signer = new Wallet(key);
    async function read(method) {
      const timestamp = Date.now().toString();
      const response = await fetch(`https://api.lyra.finance/private/${method}`, {
        method: 'POST', redirect: 'error', signal: AbortSignal.timeout(15000),
        headers: { 'Content-Type': 'application/json', 'User-Agent': 'noop-handoff/1.0',
          'X-LyraWallet': owner, 'X-LyraTimestamp': timestamp, 'X-LyraSignature': await signer.signMessage(timestamp) },
        body: JSON.stringify({ subaccount_id: V2_IDENTITY.subaccount_id }),
      });
      const json = await response.json();
      if (!response.ok || json.error || !json.result) throw new Error(`V2 ${method} failed`);
      return json.result;
    }
    account = await read('get_subaccount');
    [openOrders, triggerOrders, algoOrders] = await Promise.all(
      ['get_open_orders', 'get_trigger_orders', 'get_algo_orders'].map(async method => orderList(await read(method), method)));
  } else if (mode === 'v3-mainnet') {
    const profile = readProfile();
    if (profile.name !== mode) throw new Error('Set NOOP_VENUE=v3-mainnet and the verified mainnet profile');
    const adapter = new DeriveV3({ profile, authenticated: true });
    try {
      account = await adapter.account();
      [openOrders, triggerOrders, algoOrders] = await Promise.all(
        ['get_open_orders', 'get_trigger_orders', 'get_algo_orders'].map(async method => orderList(
          await adapter.read(`private/${method}`, { subaccount_id: profile.subaccountId }), method)));
      owner = profile.ownerAddress;
    } finally { await adapter.close(); }
  } else throw new Error('Use v2 or v3-mainnet');
  const snapshot = validateSnapshot({ schema_version: SNAPSHOT_SCHEMA_VERSION, timestamp, venue: mode, owner,
    account, open_orders: openOrders, trigger_orders: triggerOrders, algo_orders: algoOrders });
  const dir = path.resolve(__dirname, '../../.derive-v3/handoff');
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  if (fs.realpathSync(dir) !== dir) throw new Error('Handoff snapshot directory must not contain symlinks');
  const filename = path.join(dir, `${mode}-snapshot.json`);
  try {
    if (fs.lstatSync(filename).isSymbolicLink()) throw new Error('Handoff snapshot must not be a symlink');
  } catch (error) { if (error.code !== 'ENOENT') throw error; }
  fs.writeFileSync(filename, JSON.stringify(snapshot, null, 2), { mode: 0o600 });
  console.log(JSON.stringify({ file: filename, positions: account.positions.length, open_orders: openOrders.length,
    trigger_orders: triggerOrders.length, algo_orders: algoOrders.length }));
}

if (require.main === module) capture(process.argv[2]).catch(error => { console.error(error.message.split(' — ')[0]); process.exitCode = 1; });

module.exports = { capture, orderList };
