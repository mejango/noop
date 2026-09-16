'use strict';

const { canonicalize, contentDigest } = require('../../strategy/canonical');
const { normalizeDecimal, compareDecimals, addDecimals, subtractDecimals } = require('../../strategy/decimal');

/*
 * Offline, account-bound quantity/cash journal, not a marked NAV or profit
 * calculation. The store supplies the account; an event cannot change it.
 *
 * Common envelope (all fields required; additional fields rejected):
 *   { event_id, source_event_id, kind, occurred_at, evidence_ref, payload }
 * occurred_at is an exact UTC ISO timestamp with milliseconds. evidence_ref is
 * a reference to retained evidence; this pure module cannot fetch or attest it.
 * A sha256: reference must contain exactly 64 lower-case hexadecimal digits.
 * All economic values are bounded plain decimal strings, normalized exactly.
 * Cash assets are ETH and USDC. Option units are normalized contract quantities
 * under an explicit ETH-YYYYMMDD-STRIKE-P/C identifier; never multiply a fill's
 * already-normalized quantity by a contract multiplier a second time.
 *
 * Payloads:
 * deposit/withdrawal { asset, amount, transfer_id }
 * option_fill { fill_id, order_attempt_id, instrument, side, quantity,
 *               gross_premium, quote_asset:'USDC', fees:[{asset,amount}] }
 * spot_fill { fill_id, order_attempt_id, side, base_asset:'ETH',
 *             quote_asset:'USDC', quantity, gross_quote, fees:[{asset,amount}] }
 * borrow { loan_id, asset, amount }
 * repay { loan_id, asset, principal, interest }
 * interest_accrual { loan_id, asset, amount, direction:'payable'|'receivable' }
 * interest_payment { loan_id, asset, amount, direction:'paid'|'received' }
 * fee { fee_id, asset, amount }
 * option_settlement { settlement_id, instrument, position_quantity,
 *                     cash_amount, quote_asset:'USDC' }
 * reversal { reverses_event_id, reason } -- only through reverseNormalizedEvent
 *
 * A fill is one authoritative execution, never cumulative order totals. Its
 * gross consideration excludes fees; fees are journaled once. Standalone fee
 * events represent additional charges, never fees already embedded in a fill.
 * Repayments and interest payments clear previously accrued liabilities; they
 * do not book an expense again. Positive liability account balances, negative
 * receivables or unexplained negative cash require reconciliation by the
 * transactional store/control plane. Authoritative cash movements are still
 * facts; a negative balance never implicitly grants permission to borrow.
 * Settlement removes the full signed position_quantity; the store must compare
 * this quantity with the actual remaining instrument position in the same
 * transaction. Zero-payoff expiries are settlements too. Actual venue debt is a
 * separate explicit borrowing event, never inferred from a cash shortfall.
 *
 * Result {event, postings, source_key, economic_ref, digest}: postings balance
 * to zero PER ASSET, with assets debit-positive and liabilities credit-negative.
 * source_key is the supplied source_event_id. economic_ref additionally binds
 * business identity, so renaming a source cannot book a fill twice. Both require
 * account-scoped uniqueness in storage. digest excludes only local event_id;
 * aliases of an otherwise identical source event can therefore be idempotent.
 * Corrections are an exact reversal of a stored event followed by a separately
 * evidenced replacement, atomically when both are required. No arbitrary
 * posting interface, marks, author fee calculation, or signer is provided here.
 */

const KINDS = Object.freeze([
  'deposit', 'withdrawal', 'option_fill', 'spot_fill', 'borrow', 'repay',
  'interest_accrual', 'interest_payment', 'fee', 'option_settlement',
]);
const COMMON_FIELDS = ['event_id', 'source_event_id', 'kind', 'occurred_at', 'evidence_ref', 'payload'];
const IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$/;
const DIGEST_PATTERN = /^sha256:[a-f0-9]{64}$/;
const TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

function objectWithFields(value, fields, name) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${name} must be a plain object`);
  }
  for (const field of fields) {
    if (!Object.prototype.hasOwnProperty.call(value, field)) throw new TypeError(`${name}.${field} is required`);
  }
  for (const field of Object.keys(value)) {
    if (!fields.includes(field)) throw new TypeError(`${name}.${field} is forbidden`);
  }
}

function identifier(value, name) {
  if (typeof value !== 'string' || !IDENTIFIER_PATTERN.test(value)) throw new TypeError(`${name} must be a bounded identifier`);
  if (value.startsWith('sha256:') && !DIGEST_PATTERN.test(value)) throw new TypeError(`${name} has an invalid sha256 digest`);
  return value;
}

function timestamp(value, name) {
  if (typeof value !== 'string' || !TIMESTAMP_PATTERN.test(value)) throw new TypeError(`${name} must be a UTC ISO timestamp with milliseconds`);
  const millis = Date.parse(value);
  if (!Number.isFinite(millis) || new Date(millis).toISOString() !== value) throw new TypeError(`${name} is not a valid calendar timestamp`);
  return value;
}

function asset(value, name) {
  if (!['ETH', 'USDC'].includes(value)) throw new TypeError(`${name} must be ETH or USDC`);
  return value;
}

function decimal(value, name, sign = 'positive') {
  let normalized;
  try { normalized = normalizeDecimal(value); } catch (error) { throw new TypeError(`${name}: ${error.message}`); }
  const compared = compareDecimals(normalized, '0');
  if (sign === 'positive' && compared <= 0) throw new TypeError(`${name} must be positive`);
  if (sign === 'nonnegative' && compared < 0) throw new TypeError(`${name} must be nonnegative`);
  if (sign === 'nonzero' && compared === 0) throw new TypeError(`${name} must be nonzero`);
  return normalized;
}

function instrument(value) {
  if (typeof value !== 'string') throw new TypeError('payload.instrument must identify an ETH option');
  const matched = /^ETH-(\d{4})(\d{2})(\d{2})-((?:0|[1-9][0-9]*)(?:\.[0-9]+)?)-([PC])$/.exec(value);
  if (!matched) throw new TypeError('payload.instrument must identify an ETH option');
  timestamp(`${matched[1]}-${matched[2]}-${matched[3]}T00:00:00.000Z`, 'payload.instrument expiry');
  const strike = decimal(matched[4], 'payload.instrument strike');
  if (strike !== matched[4]) throw new TypeError('payload.instrument strike must use canonical decimal encoding');
  return value;
}

function cashFields(payload) {
  payload.asset = asset(payload.asset, 'payload.asset');
  payload.amount = decimal(payload.amount, 'payload.amount');
}

function fees(value) {
  if (!Array.isArray(value) || value.length > 2) throw new TypeError('payload.fees must be an array of at most two assets');
  const seen = new Set();
  return value.map((entry, index) => {
    const name = `payload.fees[${index}]`;
    objectWithFields(entry, ['asset', 'amount'], name);
    const feeAsset = asset(entry.asset, `${name}.asset`);
    if (seen.has(feeAsset)) throw new TypeError('payload.fees must aggregate each asset once');
    seen.add(feeAsset);
    return { asset: feeAsset, amount: decimal(entry.amount, `${name}.amount`, 'nonnegative') };
  }).sort((a, b) => a.asset < b.asset ? -1 : a.asset > b.asset ? 1 : 0);
}

function envelope(input) {
  // Check for non-JSON values, accessors, prototypes, cycles, oversized inputs,
  // unsafe keys and hidden properties BEFORE reading any user-controlled field.
  const event = JSON.parse(canonicalize(input));
  objectWithFields(event, COMMON_FIELDS, 'event');
  identifier(event.event_id, 'event.event_id');
  identifier(event.source_event_id, 'event.source_event_id');
  identifier(event.evidence_ref, 'event.evidence_ref');
  timestamp(event.occurred_at, 'event.occurred_at');
  if (typeof event.kind !== 'string') throw new TypeError('event.kind must be a string');
  return event;
}

function negative(value) { return subtractDecimals('0', value); }

function finish(event, rawPostings, economicRef) {
  // Aggregate duplicate account/asset entries. Sorted projections are stable
  // across fee ordering and preserve exact arithmetic with no floating point.
  const entries = new Map();
  for (const posting of rawPostings) {
    const key = `${posting.account}\0${posting.asset}`;
    const previous = entries.get(key);
    entries.set(key, { ...posting, amount: addDecimals(previous?.amount || '0', posting.amount) });
  }
  const postings = [...entries.values()].filter(posting => posting.amount !== '0')
    .sort((a, b) => a.account < b.account ? -1 : a.account > b.account ? 1 : a.asset < b.asset ? -1 : a.asset > b.asset ? 1 : 0);
  const totals = new Map();
  for (const posting of postings) totals.set(posting.asset, addDecimals(totals.get(posting.asset) || '0', posting.amount));
  if ([...totals.values()].some(amount => amount !== '0')) throw new Error('Internal error: unbalanced economic event');
  const { event_id: ignored, ...sourceEvent } = event;
  return {
    event, postings, source_key: event.source_event_id,
    economic_ref: economicRef, digest: contentDigest(sourceEvent),
  };
}

function normalizeEvent(input) {
  const event = envelope(input);
  const payload = event.payload;
  const postings = [];
  const post = (account, postingAsset, amount) => postings.push({ account, asset: postingAsset, amount });
  const cash = (postingAsset, amount) => post('assets:cash', postingAsset, amount);
  const recordFees = () => {
    for (const fee of payload.fees) {
      cash(fee.asset, negative(fee.amount));
      post('expenses:fees', fee.asset, fee.amount);
    }
  };
  let economicRef = `source:${event.source_event_id}`;

  switch (event.kind) {
    case 'deposit':
    case 'withdrawal': {
      objectWithFields(payload, ['asset', 'amount', 'transfer_id'], 'payload');
      cashFields(payload);
      identifier(payload.transfer_id, 'payload.transfer_id');
      const deposit = event.kind === 'deposit';
      cash(payload.asset, deposit ? payload.amount : negative(payload.amount));
      post(deposit ? 'capital:contributions' : 'capital:withdrawals', payload.asset, deposit ? negative(payload.amount) : payload.amount);
      economicRef = `${event.kind}:${payload.transfer_id}`;
      break;
    }
    case 'option_fill':
    case 'spot_fill': {
      const option = event.kind === 'option_fill';
      objectWithFields(payload, option
        ? ['fill_id', 'order_attempt_id', 'instrument', 'side', 'quantity', 'gross_premium', 'quote_asset', 'fees']
        : ['fill_id', 'order_attempt_id', 'side', 'base_asset', 'quote_asset', 'quantity', 'gross_quote', 'fees'], 'payload');
      identifier(payload.fill_id, 'payload.fill_id');
      identifier(payload.order_attempt_id, 'payload.order_attempt_id');
      if (!['buy', 'sell'].includes(payload.side)) throw new TypeError('payload.side must be buy or sell');
      if (payload.quote_asset !== 'USDC') throw new TypeError('payload.quote_asset must be USDC');
      payload.quantity = decimal(payload.quantity, 'payload.quantity');
      payload.fees = fees(payload.fees);
      let quantityAsset;
      let gross;
      if (option) {
        instrument(payload.instrument);
        quantityAsset = `OPTION:${payload.instrument}`;
        payload.gross_premium = decimal(payload.gross_premium, 'payload.gross_premium', 'nonnegative');
        gross = payload.gross_premium;
      } else {
        if (payload.base_asset !== 'ETH') throw new TypeError('payload.base_asset must be ETH');
        quantityAsset = 'ETH';
        payload.gross_quote = decimal(payload.gross_quote, 'payload.gross_quote');
        gross = payload.gross_quote;
      }
      const buy = payload.side === 'buy';
      const quantity = buy ? payload.quantity : negative(payload.quantity);
      const consideration = buy ? negative(gross) : gross;
      const clearing = option ? 'clearing:options' : 'clearing:spot';
      post(option ? 'assets:options' : 'assets:cash', quantityAsset, quantity);
      post(clearing, quantityAsset, negative(quantity));
      cash('USDC', consideration);
      post(clearing, 'USDC', negative(consideration));
      recordFees();
      economicRef = `fill:${payload.fill_id}`;
      break;
    }
    case 'borrow': {
      objectWithFields(payload, ['loan_id', 'asset', 'amount'], 'payload');
      identifier(payload.loan_id, 'payload.loan_id');
      cashFields(payload);
      cash(payload.asset, payload.amount);
      post(`liabilities:principal:${payload.loan_id}`, payload.asset, negative(payload.amount));
      break;
    }
    case 'repay': {
      objectWithFields(payload, ['loan_id', 'asset', 'principal', 'interest'], 'payload');
      identifier(payload.loan_id, 'payload.loan_id');
      asset(payload.asset, 'payload.asset');
      payload.principal = decimal(payload.principal, 'payload.principal', 'nonnegative');
      payload.interest = decimal(payload.interest, 'payload.interest', 'nonnegative');
      const total = addDecimals(payload.principal, payload.interest);
      if (total === '0') throw new TypeError('repay must clear principal or accrued interest');
      cash(payload.asset, negative(total));
      post(`liabilities:principal:${payload.loan_id}`, payload.asset, payload.principal);
      post(`liabilities:interest:${payload.loan_id}`, payload.asset, payload.interest);
      break;
    }
    case 'interest_accrual': {
      objectWithFields(payload, ['loan_id', 'asset', 'amount', 'direction'], 'payload');
      identifier(payload.loan_id, 'payload.loan_id');
      cashFields(payload);
      if (!['payable', 'receivable'].includes(payload.direction)) throw new TypeError('payload.direction must be payable or receivable');
      if (payload.direction === 'payable') {
        post(`liabilities:interest:${payload.loan_id}`, payload.asset, negative(payload.amount));
        post('expenses:interest', payload.asset, payload.amount);
      } else {
        post(`assets:interest_receivable:${payload.loan_id}`, payload.asset, payload.amount);
        post('income:interest', payload.asset, negative(payload.amount));
      }
      break;
    }
    case 'interest_payment': {
      objectWithFields(payload, ['loan_id', 'asset', 'amount', 'direction'], 'payload');
      identifier(payload.loan_id, 'payload.loan_id');
      cashFields(payload);
      if (!['paid', 'received'].includes(payload.direction)) throw new TypeError('payload.direction must be paid or received');
      if (payload.direction === 'paid') {
        cash(payload.asset, negative(payload.amount));
        post(`liabilities:interest:${payload.loan_id}`, payload.asset, payload.amount);
      } else {
        cash(payload.asset, payload.amount);
        post(`assets:interest_receivable:${payload.loan_id}`, payload.asset, negative(payload.amount));
      }
      break;
    }
    case 'fee': {
      objectWithFields(payload, ['fee_id', 'asset', 'amount'], 'payload');
      identifier(payload.fee_id, 'payload.fee_id');
      cashFields(payload);
      cash(payload.asset, negative(payload.amount));
      post('expenses:fees', payload.asset, payload.amount);
      economicRef = `fee:${payload.fee_id}`;
      break;
    }
    case 'option_settlement': {
      objectWithFields(payload, ['settlement_id', 'instrument', 'position_quantity', 'cash_amount', 'quote_asset'], 'payload');
      identifier(payload.settlement_id, 'payload.settlement_id');
      instrument(payload.instrument);
      if (payload.quote_asset !== 'USDC') throw new TypeError('payload.quote_asset must be USDC');
      payload.position_quantity = decimal(payload.position_quantity, 'payload.position_quantity', 'nonzero');
      payload.cash_amount = decimal(payload.cash_amount, 'payload.cash_amount', 'signed');
      if (payload.cash_amount !== '0' && Math.sign(compareDecimals(payload.cash_amount, '0')) !== Math.sign(compareDecimals(payload.position_quantity, '0'))) {
        throw new TypeError('settlement cash direction must match the signed option position');
      }
      const optionAsset = `OPTION:${payload.instrument}`;
      post('assets:options', optionAsset, negative(payload.position_quantity));
      post('clearing:settlement', optionAsset, payload.position_quantity);
      cash('USDC', payload.cash_amount);
      post('clearing:settlement', 'USDC', negative(payload.cash_amount));
      economicRef = `settlement:${payload.settlement_id}:${payload.instrument}`;
      break;
    }
    case 'reversal':
      throw new TypeError('Reversal requires its stored original through reverseNormalizedEvent');
    default:
      throw new TypeError(`Unsupported economic event kind: ${event.kind}`);
  }
  return finish(event, postings, economicRef);
}

function reverseNormalizedEvent(originalNormalized, input) {
  // Re-derive from the original event rather than trusting supplied posting
  // arrays. The store must retrieve this original and bind its recorded digest.
  const safeOriginal = JSON.parse(canonicalize(originalNormalized));
  const original = normalizeEvent(safeOriginal.event);
  if (canonicalize(safeOriginal) !== canonicalize(original)) throw new TypeError('Original normalized event does not match its canonical event');
  const event = envelope(input);
  if (event.kind !== 'reversal') throw new TypeError('Reversal event kind is required');
  objectWithFields(event.payload, ['reverses_event_id', 'reason'], 'payload');
  identifier(event.payload.reverses_event_id, 'payload.reverses_event_id');
  if (event.payload.reverses_event_id !== original.event.event_id) throw new TypeError('Reversal must identify its stored original event');
  if (event.event_id === original.event.event_id || event.source_event_id === original.source_key) throw new TypeError('Reversal requires its own event and source identities');
  if (typeof event.payload.reason !== 'string' || event.payload.reason.trim().length === 0 || event.payload.reason.length > 1024) throw new TypeError('Reversal requires a bounded reason');
  return finish(event, original.postings.map(posting => ({ ...posting, amount: negative(posting.amount) })), `reversal:${original.event.event_id}`);
}

module.exports = { KINDS, normalizeEvent, reverseNormalizedEvent };
