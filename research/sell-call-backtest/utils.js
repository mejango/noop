'use strict';

const HOUR_MS = 60 * 60 * 1000;
const DAY_MS = 24 * HOUR_MS;

function finite(value) {
  if (value == null || value === '') return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function mean(values) {
  const numbers = values.map(finite).filter((value) => value != null);
  if (numbers.length === 0) return null;
  return numbers.reduce((sum, value) => sum + value, 0) / numbers.length;
}

function median(values) {
  const numbers = values.map(finite).filter((value) => value != null).sort((a, b) => a - b);
  if (numbers.length === 0) return null;
  const midpoint = Math.floor(numbers.length / 2);
  return numbers.length % 2 === 0
    ? (numbers[midpoint - 1] + numbers[midpoint]) / 2
    : numbers[midpoint];
}

function round(value, digits = 6) {
  const number = finite(value);
  return number == null ? null : Number(number.toFixed(digits));
}

function isoToMs(value) {
  const timestamp = new Date(value).getTime();
  return Number.isFinite(timestamp) ? timestamp : null;
}

function addHours(value, hours) {
  const timestamp = typeof value === 'number' ? value : isoToMs(value);
  return timestamp == null ? null : timestamp + Number(hours) * HOUR_MS;
}

function lowerBound(items, target, selector = (item) => item) {
  let low = 0;
  let high = items.length;
  while (low < high) {
    const midpoint = (low + high) >> 1;
    if (selector(items[midpoint]) < target) low = midpoint + 1;
    else high = midpoint;
  }
  return low;
}

function maxDrawdown(equityCurve = []) {
  let peak = null;
  let worst = 0;
  for (const point of equityCurve) {
    const nav = finite(point?.nav);
    if (!(nav > 0)) continue;
    peak = peak == null ? nav : Math.max(peak, nav);
    if (peak > 0) worst = Math.max(worst, (peak - nav) / peak);
  }
  return worst;
}

function parseArgv(argv = []) {
  const result = {};
  for (const raw of argv) {
    if (!raw.startsWith('--')) continue;
    const separator = raw.indexOf('=');
    if (separator === -1) result[raw.slice(2)] = true;
    else result[raw.slice(2, separator)] = raw.slice(separator + 1);
  }
  return result;
}

function parseNumber(value, fallback) {
  const number = finite(value);
  return number == null ? fallback : number;
}

module.exports = {
  DAY_MS,
  HOUR_MS,
  addHours,
  clamp,
  finite,
  isoToMs,
  lowerBound,
  maxDrawdown,
  mean,
  median,
  parseArgv,
  parseNumber,
  round,
};
