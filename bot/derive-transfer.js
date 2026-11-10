'use strict';

// Pure codec for the existing, same-owner USDC route. No network or credential I/O.
// Official SDK: derivexyz/derive-ts@f15f1b8776a7535f2609bf9130265d1a67704077,
// src/codecs/transfer.ts and src/api/spotTransfers.ts. Existing-account transfers
// are free; enforce a zero USD fee cap rather than assuming a USD/USDC exchange rate.
const { AbiCoder, getAddress, keccak256, parseUnits } = require('ethers');
const { buildActionHashes } = require('./derive-signing');

const TRANSFER_MODULE_ADDRESS = '0x01259207A40925b794C8ac320456F7F6c8FE2636';
const USDC_ASSET_ADDRESS = '0x57B03E14d409ADC7fAb6CFc44b5886CAD2D5f02b';
const encoder = AbiCoder.defaultAbiCoder();

function address(value, field) {
  try { return getAddress(value); } catch { throw new Error(`Invalid ${field}`); }
}

function positiveId(value, field) {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`Invalid ${field}`);
  return value;
}

function validateUsdcAsset(asset) {
  if (asset?.name !== 'USDC' || asset?.erc20?.decimals !== 6
    || address(asset?.address, 'USDC protocol asset') !== USDC_ASSET_ADDRESS) {
    throw new Error('USDC protocol asset metadata mismatch');
  }
  const underlying = address(asset.erc20.underlying_erc20, 'USDC underlying asset');
  if (underlying === '0x0000000000000000000000000000000000000000') throw new Error('USDC deposit asset unavailable');
  return { name: 'USDC', address: USDC_ASSET_ADDRESS,
    erc20: { decimals: 6, underlying_erc20: underlying } };
}

function resolveUsdcTransferAsset(currencies) {
  if (!Array.isArray(currencies)) throw new Error('Currency metadata unavailable');
  const matches = currencies.filter(c => c?.currency === 'USDC');
  if (matches.length !== 1 || !Array.isArray(matches[0].spot)) throw new Error('Ambiguous USDC currency metadata');
  const enabled = matches[0].spot.filter(s => s?.erc20?.underlying_erc20 != null || s?.erc20?.decimals > 0);
  if (enabled.length !== 1) throw new Error('Ambiguous USDC deposit asset metadata');
  return validateUsdcAsset(enabled[0]);
}

function transferRequest({ config, owner, transfer, asset, destinationSubaccountId }) {
  if (config?.version !== 'v3' || config?.network !== 'mainnet') throw new Error('USDC transfer requires V3 mainnet');
  const expectedOwner = address(config.wallet, 'configured owner');
  if (address(owner, 'owner') !== expectedOwner) throw new Error('Transfer owner mismatch');
  const source = positiveId(config.subaccountId, 'configured source subaccount');
  const destination = positiveId(destinationSubaccountId, 'configured destination subaccount');
  if (transfer?.subaccount_id !== source || transfer?.to_subaccount_id !== destination || source === destination) {
    throw new Error('Transfer route mismatch');
  }
  if (transfer.new_subaccount_manager !== 0 || transfer.sub_id !== 0 || transfer.asset_name !== 'USDC') {
    throw new Error('Only existing-subaccount USDC transfers are supported');
  }
  if (typeof transfer.max_fee_usd !== 'string' || !/^0(?:\.0{1,12})?$/.test(transfer.max_fee_usd)) {
    throw new Error('Existing-subaccount transfer fee cap must be zero USD');
  }
  if (typeof transfer.amount !== 'string' || !/^(?:0|[1-9]\d*)(?:\.\d{1,6})?$/.test(transfer.amount)) {
    throw new Error('USDC transfer amount must use at most six decimal places');
  }
  const amount = parseUnits(transfer.amount, 18);
  if (amount <= 0n || amount >= 2n ** 256n) throw new Error('Transfer amount must be positive uint256');
  if (typeof transfer.nonce !== 'string' || !/^[1-9]\d*$/.test(transfer.nonce)
    || BigInt(transfer.nonce) >= 2n ** 256n) throw new Error('Transfer nonce must be a uint256 decimal string');
  positiveId(transfer.signature_expiry_sec, 'signature expiry');
  validateUsdcAsset(asset);
  return { subaccount_id: source, to_subaccount_id: destination, new_subaccount_manager: 0,
    asset_name: 'USDC', sub_id: 0, amount: transfer.amount, max_fee_usd: '0',
    nonce: transfer.nonce, signer: address(transfer.signer, 'signer'), signature_expiry_sec: transfer.signature_expiry_sec };
}

function buildTransferHashes(args) {
  const request = transferRequest(args);
  const encodedData = encoder.encode(['uint256', 'uint256', 'address', 'uint256', 'uint256', 'uint256'],
    [request.to_subaccount_id, 0, USDC_ASSET_ADDRESS, 0, parseUnits(request.amount, 18), 0]);
  const encodedDataHash = keccak256(encodedData);
  return { request, encodedData, encodedDataHash, ...buildActionHashes({ config: args.config, owner: args.owner,
    action: request, moduleAddress: TRANSFER_MODULE_ADDRESS, encodedDataHash }) };
}

function prepareTransfer({ signer, nowMs = Date.now(), ...args }) {
  const hashes = buildTransferHashes(args);
  if (address(signer?.address, 'signing wallet') !== hashes.request.signer) throw new Error('Transfer signer mismatch');
  if (!Number.isSafeInteger(nowMs) || nowMs < 0 || hashes.request.signature_expiry_sec <= Math.floor(nowMs / 1000)) {
    throw new Error('Transfer signature expiry is not in the future');
  }
  const nonceMs = BigInt(hashes.request.nonce) / 1000000n;
  const clock = BigInt(nowMs);
  if (nonceMs < clock - 3600000n || nonceMs > clock + 3600000n) throw new Error('Transfer nonce outside one-hour window');
  return { ...hashes.request, signature: signer.signingKey.sign(hashes.typedDataHash).serialized };
}

module.exports = { TRANSFER_MODULE_ADDRESS, USDC_ASSET_ADDRESS, resolveUsdcTransferAsset, buildTransferHashes, prepareTransfer };
