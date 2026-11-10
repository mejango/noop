'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { Wallet, recoverAddress } = require('ethers');
const { getDeriveConfig } = require('../bot/derive-config');
const { USDC_ASSET_ADDRESS, resolveUsdcTransferAsset, buildTransferHashes, prepareTransfer } = require('../bot/derive-transfer');

const OWNER = '0x823b92d6a4b2AED4b15675c7917c9f922ea8ADAD';
const asset = { name: 'USDC', address: USDC_ASSET_ADDRESS,
  erc20: { decimals: 6, underlying_erc20: '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' } };
const config = getDeriveConfig({ DERIVE_API_VERSION: 'v3', DERIVE_NETWORK: 'mainnet', DERIVE_WALLET: OWNER,
  DERIVE_SUBACCOUNT_ID: '25923', DERIVE_HISTORY_FROM: '2026-10-06T00:00:00Z' });
const transfer = { subaccount_id: 25923, to_subaccount_id: 120934, new_subaccount_manager: 0, asset_name: 'USDC', sub_id: 0,
  amount: '1.234567', max_fee_usd: '0', nonce: '1794324267506000000', signer: '0xDDdC9bF7C0fb4f7DB4859C04bA394209F74A70dE',
  signature_expiry_sec: 1794324327 };
const args = () => ({ config, owner: OWNER, asset, destinationSubaccountId: 120934, transfer: { ...transfer } });

test('transfer hashes match Derive non-executing mainnet debug response byte for byte', () => {
  // Observed 2026-11-10 using a 65-byte all-zero dummy signature. No action signed or executed.
  const result = buildTransferHashes(args());
  assert.equal(result.encodedDataHash, '0x8a36867dd57dca13868746d6b6f43102e03bcb3c553388e23991594d1c05efb2');
  assert.equal(result.actionHash, '0x95c0d8aa54408ab4ce5dac2e17c9d3818ec0bd4fb09531d0c4935cbe2906a989');
  assert.equal(result.typedDataHash, '0x8fbea7ea2e75c1fede795ba0a0ad2a1d0f3d14e6ac412634283c2a9930d9bace');
  assert.equal(result.encodedData.length, 2 + 192 * 2);
  assert.equal(result.request.max_fee_usd, '0');
  assert.equal(result.request.signature, undefined);
});

test('metadata resolution requires one deposit-enabled protocol USDC asset', () => {
  assert.deepEqual(resolveUsdcTransferAsset([{ currency: 'USDC', spot: [asset] }]), asset);
  for (const currencies of [[], [{ currency: 'USDC', spot: [asset, asset] }],
    [{ currency: 'USDC', spot: [{ ...asset, address: asset.erc20.underlying_erc20 }] }],
    [{ currency: 'USDC', spot: [{ ...asset, erc20: { ...asset.erc20, decimals: 18 } }] }]]) {
    assert.throws(() => resolveUsdcTransferAsset(currencies));
  }
});

test('transfer encoding rejects fees, route changes, creation and invalid amounts before signing', () => {
  const invalid = [
    { max_fee_usd: '0.000001' }, { max_fee_usd: '-1' }, { max_fee_usd: 0 }, { max_fee_usd: '0.0000000000000' },
    { new_subaccount_manager: 3 }, { subaccount_id: 7 }, { to_subaccount_id: 25923 },
    { asset_name: 'ETH' }, { sub_id: 1 }, { amount: '0' }, { amount: '-1' },
    { amount: '0.0000001' }, { amount: '1e2' }, { nonce: 1794324267506000000 }, { nonce: '0' },
    { signature_expiry_sec: NaN },
  ];
  for (const patch of invalid) {
    const input = args(); Object.assign(input.transfer, patch);
    assert.throws(() => buildTransferHashes(input), JSON.stringify(patch));
  }
  assert.throws(() => buildTransferHashes({ ...args(), owner: asset.address }), /owner mismatch/);
});

test('preparation signs only the validated action with an explicitly supplied test wallet', () => {
  const signer = Wallet.createRandom();
  const input = args(); input.transfer.signer = signer.address; input.transfer.signature = 'ignored';
  const request = prepareTransfer({ ...input, signer, nowMs: 1794324267506 });
  assert.equal(recoverAddress(buildTransferHashes(input).typedDataHash, request.signature), signer.address);
  assert.equal(request.amount, '1.234567');
  assert.equal(request.new_subaccount_manager, 0);
  assert.throws(() => prepareTransfer({ ...input, signer: Wallet.createRandom(), nowMs: 1794324267506 }), /signer mismatch/);
  assert.throws(() => prepareTransfer({ ...input, signer, nowMs: 1794324327000 }), /expiry/);
  const oldNonce = { ...input, transfer: { ...input.transfer, nonce: '1794310000000000000' } };
  assert.throws(() => prepareTransfer({ ...oldNonce, signer, nowMs: 1794324267506 }), /nonce outside/);
});

test('preparation preserves the exact approved integer amount for runtime intent matching', () => {
  const signer = Wallet.createRandom();
  const input = args(); input.transfer.amount = '1'; input.transfer.signer = signer.address;
  const prepared = prepareTransfer({ ...input, signer, nowMs: 1794324267506 });
  assert.equal(prepared.amount, input.transfer.amount);
  assert.equal(prepared.amount, '1');
  assert.equal(buildTransferHashes(input).request.amount, '1');
});
