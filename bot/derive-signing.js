'use strict';

// Pure action encoding/signing. No credentials, environment, database or network I/O.
const { AbiCoder, keccak256, parseUnits } = require('ethers');
const encoder = new AbiCoder();
const ACTION_TYPEHASH = '0x4d7a9f27c403ff9c0f19bce61d76d82f9aa29f8d6d4b0c5474607d9770d1af17';
const TRADE_MODULE_ADDRESS = '0xB8D20c2B7a1Ad2EE33Bc50eF10876eD3035b5e7b';

function encodeTradeData(order, assetAddress, assetSubId) {
  return encoder.encode(['address', 'uint', 'int', 'int', 'uint', 'uint', 'bool'], [
    assetAddress, assetSubId,
    parseUnits(order.limit_price.toString(), 18),
    parseUnits(order.amount.toString(), 18),
    parseUnits(order.max_fee.toString(), 18),
    order.subaccount_id, order.direction === 'buy',
  ]);
}

function buildActionHashes({ config, owner, action, moduleAddress, encodedDataHash }) {
  const actionHash = keccak256(encoder.encode(
    ['bytes32', 'uint256', 'uint256', 'address', 'bytes32', 'uint256', 'address', 'address'],
    [ACTION_TYPEHASH, action.subaccount_id, action.nonce, moduleAddress, encodedDataHash,
      action.signature_expiry_sec, owner, action.signer],
  ));
  const typedDataHash = keccak256(Buffer.concat([
    Buffer.from('1901', 'hex'), Buffer.from(config.domainSeparator.slice(2), 'hex'), Buffer.from(actionHash.slice(2), 'hex'),
  ]));
  return { actionHash, typedDataHash };
}

function buildOrderHashes({ config, owner, order, assetAddress, assetSubId }) {
  const encodedData = encodeTradeData(order, assetAddress, assetSubId);
  const encodedDataHash = keccak256(encodedData);
  return { encodedData, encodedDataHash, ...buildActionHashes({ config, owner, action: order,
    moduleAddress: TRADE_MODULE_ADDRESS, encodedDataHash }) };
}

function signOrder({ signer, ...args }) {
  return signer.signingKey.sign(buildOrderHashes(args).typedDataHash).serialized;
}

module.exports = { ACTION_TYPEHASH, TRADE_MODULE_ADDRESS, encodeTradeData, buildActionHashes, buildOrderHashes, signOrder };
