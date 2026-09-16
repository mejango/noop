'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  parseDecimal, normalizeDecimal, compareDecimals,
  addDecimals, subtractDecimals, multiplyDecimals,
} = require('../strategy/decimal');
const { validateCondition, evaluateCondition, DEFAULT_FIELD_CATALOG } = require('../strategy/conditions');

const NOW = '2030-01-01T12:00:05.000Z';
const field = (value, unit, overrides = {}) => ({
  value, unit, observed_at: NOW, available_at: NOW, quality: 'valid', ...overrides,
});
const literal = (value, unit = 'USDC') => ({ literal: value, unit });
const comparison = (op, left, right) => ({ op, left, right });
const fixed = (value) => comparison('eq', literal(value, 'boolean'), literal(true, 'boolean'));
const askUnder100 = comparison('lte', { ref: 'instrument.best_ask' }, literal('100', 'USDC/contract'));
const evaluate = (condition, fields = {}, options = {}) => evaluateCondition(condition, {
  fields, now: NOW, ...options,
});
const ask = (value, overrides) => ({ 'instrument.best_ask': field(value, 'USDC/contract', overrides) });

test('decimal parsing and canonical strings preserve exact signed values', () => {
  assert.deepEqual(parseDecimal('-123.045'), { coefficient: -123045n, scale: 3 });
  assert.equal(normalizeDecimal('0.000'), '0');
  assert.equal(normalizeDecimal('-0.000'), '0');
  assert.equal(normalizeDecimal('10.0100'), '10.01');
  assert.equal(normalizeDecimal('-10.0100'), '-10.01');
});

test('decimal arithmetic does not round through binary floating point', () => {
  assert.equal(addDecimals('0.1', '0.2'), '0.3');
  assert.equal(subtractDecimals('1', '0.999999999999999999999999999999'), '0.000000000000000000000000000001');
  assert.equal(multiplyDecimals('999999999999999999999999', '0.000001'), '999999999999999999.999999');
  assert.equal(addDecimals('-0.1', '0.1'), '0');
  assert.equal(subtractDecimals('-1.25', '2.75'), '-4');
  assert.equal(multiplyDecimals('-1.25', '-2.4'), '3');
  assert.equal(multiplyDecimals('0', '-3.125'), '0');
});

test('decimal comparison remains exact beyond JS number precision', () => {
  assert.equal(compareDecimals('9007199254740993', '9007199254740992'), 1);
  assert.equal(compareDecimals('0.100000000000000000000000000001', '0.1'), 1);
  assert.equal(compareDecimals('-2.000', '-1.999'), -1);
  assert.equal(compareDecimals('1.2500', '1.25'), 0);
  assert.equal(compareDecimals('-0', '0'), 0);
});

test('decimal inputs reject ambiguous syntax and non-string numbers', () => {
  for (const value of ['', ' ', ' 1', '1 ', '+1', '01', '-01', '.1', '1.', '1e2', 'NaN', 'Infinity', '--1', '1_000', 1, NaN, null, undefined, {}, []]) {
    assert.throws(() => parseDecimal(value), undefined, `accepted ${String(value)}`);
  }
});

test('decimal size limits allow boundary values but reject oversized results', () => {
  const maxInteger = '9'.repeat(60);
  const smallest = `0.${'0'.repeat(29)}1`;
  assert.equal(normalizeDecimal(maxInteger), maxInteger);
  assert.equal(normalizeDecimal(smallest), smallest);
  assert.throws(() => parseDecimal('1'.repeat(61)));
  assert.throws(() => parseDecimal(`0.${'0'.repeat(30)}1`));
  assert.throws(() => addDecimals(maxInteger, '1'));
  assert.throws(() => multiplyDecimals(maxInteger, '10'));
  assert.throws(() => multiplyDecimals(smallest, '0.1'));
  assert.equal(multiplyDecimals('0.00000000000000000001', '10000000000000000000'), '0.1');
  assert.equal(multiplyDecimals('0.00000000000000000002', '0.00000000005'), smallest);
});

test('all supported comparisons use exact decimals and explicit units', () => {
  for (const [op, expected] of [['lt', false], ['lte', true], ['eq', true], ['gte', true], ['gt', false]]) {
    assert.equal(evaluate(comparison(op, literal('0.10'), literal('0.1'))).value, expected);
  }
  assert.equal(evaluate(askUnder100, ask('99.999999999999999999999999999999')).value, true);
  assert.equal(evaluate(askUnder100, ask('100.000000000000000000000000000001')).value, false);
});

test('condition validation rejects currency mismatches and ordered boolean comparisons', () => {
  assert.throws(() => validateCondition(comparison('lt', literal('1', 'ETH'), literal('1', 'USDC'))));
  assert.throws(() => validateCondition(comparison('gte', literal(true, 'boolean'), literal(false, 'boolean'))));
  assert.throws(() => validateCondition(comparison('eq', literal('true', 'boolean'), literal(true, 'boolean'))));
  assert.throws(() => validateCondition(comparison('eq', literal(1), literal('1'))));
  assert.throws(() => validateCondition(comparison('eq', literal('1', 'unregistered'), literal('1', 'unregistered'))));
  assert.throws(() => validateCondition(comparison('eq', { ref: 'unknown.field' }, literal('1'))));
});

test('unknown operands follow three-valued boolean logic', () => {
  const unknown = askUnder100;
  assert.equal(evaluate(unknown).value, 'unknown');
  assert.equal(evaluate({ op: 'not', arg: unknown }).value, 'unknown');
  assert.equal(evaluate({ op: 'all', args: [fixed(false), unknown] }).value, false);
  assert.equal(evaluate({ op: 'all', args: [fixed(true), unknown] }).value, 'unknown');
  assert.equal(evaluate({ op: 'any', args: [fixed(true), unknown] }).value, true);
  assert.equal(evaluate({ op: 'any', args: [fixed(false), unknown] }).value, 'unknown');
  assert.equal(evaluate({ op: 'not', arg: fixed(false) }).value, true);
  assert.equal(evaluate({ op: 'all', args: [fixed(true), fixed(true)] }).value, true);
});

test('false boolean evidence and zero decimal evidence are present values', () => {
  const reconciled = comparison('eq', { ref: 'account.reconciled' }, literal(true, 'boolean'));
  assert.equal(evaluate(reconciled, { 'account.reconciled': field(false, 'boolean') }).value, false);
  assert.equal(evaluate(reconciled, { 'account.reconciled': field(true, 'boolean') }).value, true);
  assert.equal(evaluate(askUnder100, ask('0')).value, true);
});

test('a permissive branch cannot hide malformed or unregistered condition children', () => {
  for (const malformed of [
    { op: 'run', code: 'true' },
    comparison('eq', { ref: 'arbitrary.value' }, literal('1')),
    { op: 'all', args: [] },
    { op: 'not' },
    { ...fixed(true), ignored_execution_override: true },
  ]) {
    assert.throws(() => evaluate({ op: 'any', args: [fixed(true), malformed] }));
    assert.throws(() => evaluate({ op: 'all', args: [fixed(false), malformed] }));
  }
});

test('quote freshness includes the boundary and respects a tighter per-intent age', () => {
  const atBoundary = ask('99', { observed_at: '2030-01-01T12:00:03.000Z', available_at: '2030-01-01T12:00:03.000Z' });
  assert.equal(evaluate(askUnder100, atBoundary).value, true);
  assert.equal(evaluate(askUnder100, atBoundary, { maxQuoteAgeMs: 1999 }).value, 'unknown');
  assert.equal(evaluate(askUnder100, ask('99', { observed_at: '2030-01-01T12:00:02.999Z' })).value, 'unknown');
  assert.equal(evaluate(askUnder100, ask('99', { observed_at: '2030-01-01T12:00:02.999Z' }), { maxQuoteAgeMs: 999999 }).value, 'unknown');
});

test('observation and availability times both prevent future evidence from authorizing a trade', () => {
  for (const overrides of [
    { observed_at: '2030-01-01T12:00:05.001Z' },
    { available_at: '2030-01-01T12:00:05.001Z' },
    { observed_at: '2030-01-01T12:00:04.000Z', available_at: '2030-01-01T12:00:03.000Z' },
  ]) {
    assert.equal(evaluate(askUnder100, ask('99', overrides)).value, 'unknown');
  }
});

test('missing, malformed, invalid, and incorrectly denominated evidence is unknown', () => {
  for (const evidence of [
    undefined, null, '99', {},
    field('99', 'ETH'), field(99, 'USDC/contract'), field('1e2', 'USDC/contract'),
    field('99', 'USDC/contract', { quality: 'missing' }),
    field('99', 'USDC/contract', { quality: 'invalid' }),
    field('99', 'USDC/contract', { observed_at: undefined }),
    field('99', 'USDC/contract', { available_at: undefined }),
  ]) {
    const result = evaluate(askUnder100, { 'instrument.best_ask': evidence });
    assert.equal(result.value, 'unknown');
    assert.ok(Array.isArray(result.reasons) && result.reasons.length > 0);
  }
});

test('timestamps use real UTC calendar dates and a single canonical precision', () => {
  for (const invalid of [
    '2030-02-29T12:00:00Z', '2030-04-31T12:00:00Z', '2030-01-01T24:00:00Z',
    '2030-01-01T12:00:60Z', '2030-01-01T12:00:00+00:00', '2030-01-01',
    '2030-01-01T12:00:05.0Z', '2030-01-01T12:00:05.0000Z',
  ]) {
    assert.throws(() => evaluate(fixed(true), {}, { now: invalid }), undefined, invalid);
    assert.equal(evaluate(askUnder100, ask('99', { observed_at: invalid })).value, 'unknown', invalid);
  }
  assert.equal(evaluate(fixed(true), {}, { now: '2032-02-29T12:00:00Z' }).value, true);
});

test('registered custom features pin their own units and freshness', () => {
  const catalog = {
    'research.edge': { unit: 'ratio', scope: 'instrument', max_age_ms: 10000, description: 'Versioned research edge', calculation_version: 'edge/v1' },
  };
  const when = comparison('gt', { ref: 'research.edge' }, literal('0.125', 'ratio'));
  const fields = { 'research.edge': field('0.126', 'ratio', { observed_at: '2030-01-01T11:59:55.000Z' }) };
  assert.equal(evaluate(when, fields, { catalog, maxQuoteAgeMs: 1 }).value, true);
  fields['research.edge'].observed_at = '2030-01-01T11:59:54.999Z';
  assert.equal(evaluate(when, fields, { catalog }).value, 'unknown');
  assert.throws(() => evaluate(when, fields));
});

test('catalog definitions and evaluator options fail validation instead of changing interpretation', () => {
  for (const catalog of [
    [], new Map(),
    { 'custom.x': { unit: 'USDC', scope: 'instrument', max_age_ms: -1, description: 'Bad', calculation_version: 'v1' } },
    { 'custom.x': { unit: 'USD', scope: 'instrument', max_age_ms: 10, description: 'Bad', calculation_version: 'v1' } },
    { 'custom.x': { unit: 'USDC', scope: 'instrument', max_age_ms: 10, description: 'Unversioned' } },
    { 'custom.x': { unit: 'USDC', max_age_ms: 10, description: 'Missing scope', calculation_version: 'v1' } },
    { 'custom.x': { unit: 'USDC', scope: 'global', max_age_ms: 10, description: 'Invalid scope', calculation_version: 'v1' } },
  ]) assert.throws(() => validateCondition(fixed(true), catalog));
  for (const maxQuoteAgeMs of [-1, 1.1, NaN, Infinity, '1000']) {
    assert.throws(() => evaluate(fixed(true), {}, { maxQuoteAgeMs }));
  }
});

test('condition structure rejects cycles, excessive branching and excessive depth', () => {
  const cyclic = { op: 'not' };
  cyclic.arg = cyclic;
  assert.throws(() => validateCondition(cyclic));
  assert.throws(() => validateCondition({ op: 'any', args: Array.from({ length: 33 }, () => fixed(true)) }));
  let deep = fixed(true);
  for (let i = 0; i < 17; i++) deep = { op: 'not', arg: deep };
  assert.throws(() => validateCondition(deep));
  const many = { op: 'all', args: Array.from({ length: 5 }, () => ({ op: 'all', args: Array.from({ length: 30 }, () => fixed(true)) })) };
  assert.throws(() => validateCondition(many));
});

test('prototype payloads, non-plain objects and accessors never become condition code', () => {
  assert.throws(() => validateCondition(JSON.parse('{"op":"not","arg":null,"__proto__":{"polluted":true}}')));
  assert.throws(() => validateCondition(Object.create(fixed(true))));
  class Condition { constructor() { Object.assign(this, fixed(true)); } }
  assert.throws(() => validateCondition(new Condition()));
  let reads = 0;
  const accessor = { get op() { reads++; return 'eq'; }, left: literal('1'), right: literal('1') };
  assert.throws(() => validateCondition(accessor));
  assert.equal(reads, 0);
  const catalogAccessor = {};
  Object.defineProperty(catalogAccessor, 'custom.x', { enumerable: true, get() { reads++; return {}; } });
  assert.throws(() => validateCondition(fixed(true), catalogAccessor));
  assert.equal(reads, 0);
  assert.equal({}.polluted, undefined);
});

test('malformed evidence accessors are not invoked and cannot authorize an order', () => {
  let reads = 0;
  const evidence = field('99', 'USDC/contract');
  Object.defineProperty(evidence, 'value', { enumerable: true, get() { reads++; return '99'; } });
  assert.equal(evaluate(askUnder100, { 'instrument.best_ask': evidence }).value, 'unknown');
  assert.equal(reads, 0);
});

test('default catalog preserves risk-critical field denominations', () => {
  const expected = {
    'instrument.best_ask': 'USDC/contract',
    'instrument.best_bid': 'USDC/contract',
    'instrument.dte': 'day',
    'instrument.delta': 'ratio',
    'position.call_profit_capture_pct': 'percent',
    'position.put_executable_pnl_pct': 'percent',
    'portfolio.has_longer_dated_put': 'boolean',
    'account.projected_initial_margin_utilization': 'ratio',
    'account.reconciled': 'boolean',
  };
  for (const [ref, unit] of Object.entries(expected)) assert.equal(DEFAULT_FIELD_CATALOG[ref].unit, unit, ref);
});
