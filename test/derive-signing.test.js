'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { AbiCoder, Wallet, keccak256, parseUnits, recoverAddress } = require('ethers');
const { getDeriveConfig } = require('../bot/derive-config');
const { encodeTradeData, buildActionHashes, buildOrderHashes, signOrder } = require('../bot/derive-signing');

// Public synthetic test key only. No production credential or network access.
const signer = new Wallet(`0x${'11'.repeat(32)}`);
const owner = '0x2222222222222222222222222222222222222222';
const assetAddress = '0x3333333333333333333333333333333333333333';
const assetSubId = '123456789012345678901';

// The pre-extraction script.js algorithm, with literal constants and an
// independent ABI coder. Keep this separate from the implementation under test.
function legacyHashes(config, order) {
  const encoder = new AbiCoder();
  const encodedData = encoder.encode(['address', 'uint', 'int', 'int', 'uint', 'uint', 'bool'], [
    assetAddress, assetSubId, parseUnits(order.limit_price.toString(), 18),
    parseUnits(order.amount.toString(), 18), parseUnits(order.max_fee.toString(), 18),
    order.subaccount_id, order.direction === 'buy',
  ]);
  const encodedDataHash = keccak256(Buffer.from(encodedData.slice(2), 'hex'));
  const actionHash = keccak256(encoder.encode(
    ['bytes32', 'uint256', 'uint256', 'address', 'bytes32', 'uint256', 'address', 'address'], [
      '0x4d7a9f27c403ff9c0f19bce61d76d82f9aa29f8d6d4b0c5474607d9770d1af17',
      order.subaccount_id, order.nonce, '0xB8D20c2B7a1Ad2EE33Bc50eF10876eD3035b5e7b',
      encodedDataHash, order.signature_expiry_sec, owner, order.signer,
    ],
  ));
  const typedDataHash = keccak256(Buffer.concat([
    Buffer.from('1901', 'hex'), Buffer.from(config.domainSeparator.slice(2), 'hex'), Buffer.from(actionHash.slice(2), 'hex'),
  ]));
  return { encodedData, encodedDataHash, actionHash, typedDataHash };
}

for (const [version, network] of [['v2', 'mainnet'], ['v3', 'mainnet'], ['v3', 'testnet']]) {
  const config = getDeriveConfig({ DERIVE_API_VERSION: version, DERIVE_NETWORK: network,
    DERIVE_WALLET: owner, DERIVE_SUBACCOUNT_ID: '25923', DERIVE_HISTORY_FROM: '2026-10-06T00:00:00Z' });
  for (const direction of ['buy', 'sell']) {
    test(`${version} ${network} ${direction} option encoding and signatures match the legacy algorithm`, () => {
      const order = Object.freeze({ instrument_name: 'ETH-20261127-1600-P', subaccount_id: 25923,
        direction, limit_price: '12.3', amount: '1.25', max_fee: '10.00',
        nonce: version === 'v2' ? 1794242400000123 : '1794242400000123456',
        signature_expiry_sec: 1794243300, signer: signer.address,
        time_in_force: 'ioc', reduce_only: direction === 'sell', order_type: 'limit', mmp: false });
      const before = JSON.stringify(order);
      const expected = legacyHashes(config, order);
      const args = { config, owner, order, assetAddress, assetSubId };
      assert.deepEqual(buildOrderHashes(args), expected);
      assert.equal(encodeTradeData(order, assetAddress, assetSubId), expected.encodedData);
      const signature = signOrder({ ...args, signer });
      assert.equal(signature, signer.signingKey.sign(expected.typedDataHash).serialized);
      assert.equal(recoverAddress(expected.typedDataHash, signature), signer.address);
      assert.equal(JSON.stringify(order), before);
    });
  }
}

test('action envelope commits to the supplied account, nonce, owner, signer, module and expiry', () => {
  const args = { config: { domainSeparator: `0x${'44'.repeat(32)}` }, owner,
    action: { subaccount_id: 25923, nonce: '1794242400000123456', signature_expiry_sec: 1794243300, signer: signer.address },
    moduleAddress: '0xB8D20c2B7a1Ad2EE33Bc50eF10876eD3035b5e7b', encodedDataHash: keccak256('0x1234') };
  const base = buildActionHashes(args).typedDataHash;
  for (const actionChange of [{ subaccount_id: 120934 }, { nonce: '1794242400000123457' },
    { signature_expiry_sec: 1794243301 }, { signer: owner }]) {
    assert.notEqual(buildActionHashes({ ...args, action: { ...args.action, ...actionChange } }).typedDataHash, base);
  }
  for (const change of [{ owner: signer.address }, { moduleAddress: assetAddress },
    { encodedDataHash: keccak256('0x1235') }, { config: { domainSeparator: `0x${'55'.repeat(32)}` } }]) {
    assert.notEqual(buildActionHashes({ ...args, ...change }).typedDataHash, base);
  }
});

test('DRV IOC signs the explicit ALT account, zero asset sub-ID and exact spot fee units', () => {
  const config = getDeriveConfig({ DERIVE_API_VERSION: 'v3', DERIVE_NETWORK: 'mainnet',
    DERIVE_WALLET: owner, DERIVE_SUBACCOUNT_ID: '25923', DERIVE_HISTORY_FROM: '2026-10-06T00:00:00Z' });
  const drvAsset = '0x1Abdb2896e57047fD989af7B3Ee6B7e81830F8D2';
  const order = Object.freeze({ instrument_name: 'DRV-USDC', subaccount_id: 120934,
    direction: 'buy', amount: '10', limit_price: '0.3123', max_fee: '0.002',
    nonce: '1794242400000123456', signature_expiry_sec: 1794243300,
    signer: signer.address, time_in_force: 'ioc', reduce_only: false, order_type: 'limit' });
  const args = { config, owner, order, assetAddress: drvAsset, assetSubId: '0' };
  const hashes = buildOrderHashes(args);
  const decoded = new AbiCoder().decode(['address', 'uint256', 'int256', 'int256', 'uint256', 'uint256', 'bool'], hashes.encodedData);
  assert.equal(decoded[0].toLowerCase(), drvAsset.toLowerCase());
  assert.equal(decoded[1], 0n);
  assert.equal(decoded[2], 312300000000000000n);
  assert.equal(decoded[3], 10000000000000000000n);
  assert.equal(decoded[4], 2000000000000000n);
  assert.equal(decoded[5], 120934n);
  assert.equal(decoded[6], true);
  const signature = signOrder({ ...args, signer });
  assert.equal(recoverAddress(hashes.typedDataHash, signature), signer.address);
  assert.notEqual(buildOrderHashes({ ...args, order: { ...order, subaccount_id: config.subaccountId } }).typedDataHash, hashes.typedDataHash);
  assert.notEqual(buildOrderHashes({ ...args, assetSubId: '1' }).typedDataHash, hashes.typedDataHash);
  assert.equal(order.signature, undefined);
});
