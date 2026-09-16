'use strict';

// The contract uses finite decimal strings. No Number conversion or rounding is
// permitted for economic quantities, even when the values look small.
const MAX_INTEGER_DIGITS = 60;
const MAX_SCALE = 30;
const MAX_TEXT_LENGTH = MAX_INTEGER_DIGITS + MAX_SCALE + 2;
const DECIMAL_PATTERN = /^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$/;

function reduce(coefficient, scale) {
  if (coefficient === 0n) return { coefficient: 0n, scale: 0 };
  while (scale > 0 && coefficient % 10n === 0n) {
    coefficient /= 10n;
    scale -= 1;
  }
  return { coefficient, scale };
}

function parseDecimal(value) {
  if (typeof value !== 'string' || value.length > MAX_TEXT_LENGTH || !DECIMAL_PATTERN.test(value)) {
    throw new TypeError('Decimal must be a bounded plain decimal string without exponent or leading zeros');
  }
  const unsigned = value[0] === '-' ? value.slice(1) : value;
  const [integer, fraction = ''] = unsigned.split('.');
  if (integer.length > MAX_INTEGER_DIGITS) throw new RangeError('Decimal integer digits exceed limit');
  if (fraction.length > MAX_SCALE) throw new RangeError('Decimal scale exceeds limit');
  const coefficient = BigInt(`${value[0] === '-' ? '-' : ''}${integer}${fraction}`);
  return reduce(coefficient, fraction.length);
}

function formatDecimal(coefficient, scale) {
  const normalized = reduce(coefficient, scale);
  if (normalized.scale > MAX_SCALE) throw new RangeError('Decimal result scale exceeds limit');
  const negative = normalized.coefficient < 0n;
  const digits = (negative ? -normalized.coefficient : normalized.coefficient).toString();
  if (Math.max(1, digits.length - normalized.scale) > MAX_INTEGER_DIGITS) {
    throw new RangeError('Decimal result integer digits exceed limit');
  }
  const padded = digits.padStart(normalized.scale + 1, '0');
  const text = normalized.scale === 0 ? padded
    : `${padded.slice(0, -normalized.scale)}.${padded.slice(-normalized.scale)}`;
  return `${negative ? '-' : ''}${text}`;
}

function normalizeDecimal(value) {
  const parsed = parseDecimal(value);
  return formatDecimal(parsed.coefficient, parsed.scale);
}

function aligned(left, right) {
  const a = parseDecimal(left);
  const b = parseDecimal(right);
  const scale = Math.max(a.scale, b.scale);
  return {
    left: a.coefficient * 10n ** BigInt(scale - a.scale),
    right: b.coefficient * 10n ** BigInt(scale - b.scale),
    scale,
  };
}

function compareDecimals(left, right) {
  const values = aligned(left, right);
  return values.left < values.right ? -1 : values.left > values.right ? 1 : 0;
}

function addDecimals(left, right) {
  const values = aligned(left, right);
  return formatDecimal(values.left + values.right, values.scale);
}

function subtractDecimals(left, right) {
  const values = aligned(left, right);
  return formatDecimal(values.left - values.right, values.scale);
}

function multiplyDecimals(left, right) {
  const a = parseDecimal(left);
  const b = parseDecimal(right);
  return formatDecimal(a.coefficient * b.coefficient, a.scale + b.scale);
}

module.exports = {
  MAX_INTEGER_DIGITS,
  MAX_SCALE,
  parseDecimal,
  normalizeDecimal,
  compareDecimals,
  addDecimals,
  subtractDecimals,
  multiplyDecimals,
};
