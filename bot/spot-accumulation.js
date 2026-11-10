'use strict';

// Pure proposals only. This module has no signer, network client, database or sender.
const { parseUnits, formatUnits } = require('ethers');
const { decimal } = require('./economic-events');
const { KEY_MARGIN_SEC } = require('./signature-expiry');
const SCALE = 10n ** 18n;
const ALLOCATION_POLICY = 'drv-only-v1';
const POLICY = Object.freeze({ floatCycles: 4, maxAgeMs: 30000,
  minSignatureHorizonSec: 10, keyMarginSec: KEY_MARGIN_SEC, drvSlippageBps: 50 });
const min = (...values) => values.reduce((a, b) => a < b ? a : b);
const ceilDiv = (a, b) => (a + b - 1n) / b;
const floorStep = (value, step) => value / step * step;

function toUnits(value, label = 'amount', allowZero = true) {
  try {
    if (typeof value !== 'string') throw new Error('decimal string required');
    const result = parseUnits(decimal(value), 18);
    if (result < 0n || (!allowZero && result === 0n)) throw new Error('negative or zero');
    return result;
  } catch { throw new Error(`Invalid ${label}: expected ${allowZero ? 'nonnegative' : 'positive'} decimal with at most 18 places`); }
}
const fromUnits = value => decimal(formatUnits(value, 18));
function splitNetProceeds(net, remainder = '0') {
  const carry = toUnits(remainder, 'split remainder');
  if (carry > 1n) throw new Error('Invalid split remainder: expected at most one atomic unit');
  const total = toUnits(net, 'net proceeds') + carry;
  // Retain the projection shape while assigning every new unit (and legacy carry) to DRV.
  return { ETH: '0', DRV: fromUnits(total), remainder: '0' };
}

function timestampMs(value) {
  if (typeof value === 'number') return Number.isSafeInteger(value) ? value : NaN;
  return typeof value === 'string' && value ? Date.parse(value) : NaN;
}
function requireFresh(value, nowMs, label) {
  const time = timestampMs(value);
  if (!Number.isFinite(time) || nowMs < time || nowMs - time > POLICY.maxAgeMs) throw new Error(`${label}_stale_or_missing`);
  return time;
}
function seconds(value, label) {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${label}_missing_or_invalid`);
  return value;
}
function validateAccount(account, nowMs, label) {
  if (!account || !/^[1-9]\d*$/.test(String(account.subaccount_id))
    || !Number.isSafeInteger(Number(account.subaccount_id))
    || !/^0x[\da-fA-F]{40}$/.test(account.owner || '')
    || !Number.isSafeInteger(account.risk_universe_id)) throw new Error(`${label}_identity_missing_or_invalid`);
  requireFresh(account.received_at, nowMs, label);
  if (account.is_under_liquidation === true) throw new Error(`${label}_under_liquidation`);
  return { balance: toUnits(account.usdc_balance, `${label} USDC balance`),
    committed: toUnits(account.committed_usdc, `${label} USDC commitments`) };
}
function validateInstrument(instrument, asset, account) {
  if (!instrument || instrument.is_active !== true || instrument.instrument_type !== 'erc20'
    || instrument.instrument_name !== `${asset}-USDC` || instrument.base_currency !== asset
    || instrument.quote_currency !== 'USDC') throw new Error('spot_instrument_missing_or_invalid');
  if (instrument.risk_universe_id !== account.risk_universe_id) throw new Error('risk_universe_mismatch');
  const tick = toUnits(instrument.tick_size, 'price tick', false);
  const step = toUnits(instrument.amount_step, 'quantity step', false);
  const minimum = toUnits(instrument.minimum_amount, 'minimum amount', false);
  const maximum = toUnits(instrument.maximum_amount, 'maximum amount', false);
  if (minimum > maximum || ceilDiv(minimum, step) * step > maximum) throw new Error('invalid_instrument_quantity_bounds');
  return { tick, step, minimum: ceilDiv(minimum, step) * step, maximum };
}
function keyExpiry(keyExpirySec, nowSec) {
  const expiry = seconds(keyExpirySec, 'key_expiry') - POLICY.keyMarginSec;
  if (expiry < nowSec + POLICY.minSignatureHorizonSec) throw new Error('key_expiry_too_close');
  return expiry;
}
function sizeOrder({ budget, price, fee, bounds, depth = bounds.maximum }) {
  const quantity = floorStep(min(budget * SCALE / (price + fee), bounds.maximum, depth), bounds.step);
  if (quantity < bounds.minimum) throw new Error('below_minimum_after_fees');
  const spend = ceilDiv(quantity * (price + fee), SCALE);
  return { quantity, spend, feeReserve: ceilDiv(quantity * fee, SCALE) };
}
const deferred = reason => ({ status: 'deferred', reason });
const capture = fn => { try { return fn(); } catch (error) { return deferred(error.message); } };
function inflight(input, asset) {
  if (typeof input.inflight?.[asset] !== 'boolean') throw new Error('inflight_state_missing');
  if (input.inflight[asset]) throw new Error('existing_asset_commitment');
}
function drvOrderPlan(account, sized, price, fee, expiry, reference) {
  return { status: 'ready_for_preview', source_spend_usdc: '0',
    max_spend_usdc: fromUnits(sized.spend), fee_reserve_usdc: fromUnits(sized.feeReserve), reference,
    order: { instrument_name: 'DRV-USDC', subaccount_id: account.subaccount_id,
      direction: 'buy', amount: fromUnits(sized.quantity), limit_price: fromUnits(price),
      max_fee: fromUnits(fee), order_type: 'limit', time_in_force: 'ioc',
      reduce_only: false, signature_expiry_sec: expiry },
    required_checks: ['private_scope', 'exact_order_quote_and_fees', 'post_trade_margin', 'fresh_reservations'] };
}

/**
 * All monetary inputs are decimal strings. Account balances are gross USDC;
 * committed_usdc includes each unavailable commitment exactly once. Allocations
 * are unreserved amounts only, from the durable ledger, not total historical credits.
 * A proposal is not authorization: caller must preview exact terms before reserving.
 */
function buildSpotPlan(input = {}) {
  const result = { dry_run: true, executable: false, source_available_usdc: '0',
    routes: { ETH: { status: 'disabled', reason: 'ETH_accumulation_disabled' } } };
  const nowMs = input.nowMs;
  if (!Number.isSafeInteger(nowMs) || nowMs <= 0) {
    result.routes.DRV = deferred('nowMs_missing_or_invalid');
    return result;
  }
  let sourceFree;
  try {
    const source = validateAccount(input.source, nowMs, 'source');
    const float = BigInt(POLICY.floatCycles) * toUnits(input.source.cycle_budget_usdc, 'put cycle budget', false);
    sourceFree = source.balance - source.committed - float;
    if (sourceFree < 0n) sourceFree = 0n;
    result.source_available_usdc = fromUnits(sourceFree);
    result.source_float_usdc = fromUnits(float);
  } catch (error) {
    result.routes.DRV = deferred(error.message);
    return result;
  }
  result.routes.DRV = capture(() => {
    inflight(input, 'DRV');
    if (!input.destination) throw new Error('ALT_destination_unavailable');
    const dest = validateAccount(input.destination, nowMs, 'destination');
    if (String(input.destination.subaccount_id) === String(input.source.subaccount_id)
      || input.destination.owner.toLowerCase() !== input.source.owner.toLowerCase()) throw new Error('destination_identity_mismatch');
    const bounds = validateInstrument(input.instruments?.DRV, 'DRV', input.destination);
    const ticker = input.drvTicker;
    requireFresh(ticker?.received_at, nowMs, 'DRV_ticker_receipt');
    requireFresh(ticker?.t, nowMs, 'DRV_ticker');
    const ask = toUnits(ticker?.a, 'DRV ask', false);
    const depth = toUnits(ticker?.A, 'DRV ask depth', false);
    const price = floorStep(ask * BigInt(10000 + POLICY.drvSlippageBps) / 10000n, bounds.tick);
    if (price < ask) throw new Error('DRV_ask_outside_tick_aligned_ceiling');
    const fee = toUnits(input.fees?.DRV?.max_fee_per_unit, 'DRV max fee per unit');
    const expiry = Math.min(Math.ceil(nowMs / 1000) + 900, keyExpiry(input.keyExpirySec, Math.ceil(nowMs / 1000)));
    const allocatedDest = toUnits(input.allocations?.DRV?.destination_usdc, 'DRV destination allocation');
    const destFree = dest.balance > dest.committed ? dest.balance - dest.committed : 0n;
    const destBudget = min(allocatedDest, destFree);
    const minimumCost = ceilDiv(bounds.minimum * (price + fee), SCALE);
    if (destBudget >= minimumCost) {
      return drvOrderPlan(input.destination, sizeOrder({ budget: destBudget, price, fee, bounds, depth }), price, fee, expiry,
        { source: 'derive/public/get_ticker', ask: fromUnits(ask), timestamp_ms: ticker.t });
    }
    // Do not transfer until a minimum buy is affordable; no new account or transfer is performed here.
    if (floorStep(depth, bounds.step) < bounds.minimum) throw new Error('DRV_depth_below_minimum');
    const sourceBudget = min(toUnits(input.allocations?.DRV?.source_usdc, 'DRV source allocation'), sourceFree);
    const transferFee = toUnits(input.transfer?.max_fee_usdc, 'transfer max fee');
    if (sourceBudget <= transferFee) throw new Error('no_available_DRV_allocation_or_source_cash');
    const amount = floorStep(sourceBudget - transferFee, 10n ** 12n); // USDC native precision, never round a transfer upward.
    if (amount === 0n || amount + destBudget < minimumCost) throw new Error('below_minimum_after_transfer_fee');
    return { status: 'transfer_preview_required', source_spend_usdc: fromUnits(amount + transferFee),
      transfer: { from_subaccount_id: input.source.subaccount_id, to_subaccount_id: input.destination.subaccount_id,
        asset_name: 'USDC', amount: fromUnits(amount), max_fee_usdc: fromUnits(transferFee), new_subaccount_manager: 0 },
      required_checks: ['private_transfer_scope', 'source_post_transfer_margin', 'transfer_fee_semantics',
        'authoritative_timeout_recovery', 'destination_settlement_before_buy'] };
  });
  return result;
}

module.exports = { ALLOCATION_POLICY, POLICY, SCALE, toUnits, fromUnits, splitNetProceeds, buildSpotPlan };
