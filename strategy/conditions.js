'use strict';

const { parseDecimal, compareDecimals, MAX_SCALE } = require('./decimal');
const { DEFAULT_FIELD_CATALOG, REGISTERED_UNITS } = require('./fields');

const LIMITS = Object.freeze({ maxDepth: 16, maxNodes: 128, maxChildren: 32, maxFields: 256, maxAgeMs: 31536000000 });
const UNIT_SET = new Set(REGISTERED_UNITS);
const FORBIDDEN_KEYS = new Set(['__proto__', 'prototype', 'constructor']);
const COMPARISONS = new Set(['lt', 'lte', 'eq', 'gte', 'gt']);
const UNKNOWN = 'unknown';

function fail(path, message) {
  throw new TypeError(`${path}: ${message}`);
}

function plainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);
}

// Descriptor inspection deliberately avoids invoking getters on untrusted data.
// Callers should parse wire JSON first; this also rejects unsafe direct JS values.
function cloneJson(value, path, limits = {}, ancestors = new Set(), state = { nodes: 0 }, depth = 0) {
  const maxNodes = limits.maxNodes || 4096;
  const maxDepth = limits.maxDepth || 36;
  state.nodes += 1;
  if (state.nodes > maxNodes || depth > maxDepth) fail(path, 'JSON structure exceeds bounds');
  if (value === null || typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    if (value.length > 2048) fail(path, 'string exceeds bound');
    return value;
  }
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value !== 'object') fail(path, 'value is not JSON');
  if (!Array.isArray(value) && !plainObject(value)) fail(path, 'expected a plain JSON object');
  if (ancestors.has(value)) fail(path, 'cyclic JSON is prohibited');
  const descriptors = Object.getOwnPropertyDescriptors(value);
  if (Object.getOwnPropertySymbols(value).length > 0) fail(path, 'symbol keys are prohibited');
  const keys = Object.keys(descriptors);
  if (keys.length > 1024) fail(path, 'object or array exceeds bounds');
  ancestors.add(value);
  let copy;
  if (Array.isArray(value)) {
    if (Object.getPrototypeOf(value) !== Array.prototype) fail(path, 'expected a plain JSON array');
    const length = descriptors.length.value;
    if (length > 1024 || keys.length !== length + 1) fail(path, 'sparse or extended arrays are prohibited');
    copy = [];
    for (let index = 0; index < length; index += 1) {
      const descriptor = descriptors[index];
      if (!descriptor || !Object.hasOwn(descriptor, 'value') || !descriptor.enumerable) fail(`${path}[${index}]`, 'accessor or sparse array is prohibited');
      copy.push(cloneJson(descriptor.value, `${path}[${index}]`, limits, ancestors, state, depth + 1));
    }
  } else {
    copy = Object.create(null);
    for (const key of keys) {
      if (FORBIDDEN_KEYS.has(key)) fail(path, `prohibited key ${key}`);
      const descriptor = descriptors[key];
      if (!Object.hasOwn(descriptor, 'value') || !descriptor.enumerable) fail(`${path}.${key}`, 'accessors and hidden properties are prohibited');
      copy[key] = cloneJson(descriptor.value, `${path}.${key}`, limits, ancestors, state, depth + 1);
    }
  }
  ancestors.delete(value);
  return copy;
}

function keysExactly(object, required, optional, path) {
  if (!plainObject(object)) fail(path, 'expected a plain object');
  const allowed = new Set([...required, ...optional]);
  for (const key of Object.keys(object)) if (!allowed.has(key)) fail(path, `unknown key ${key}`);
  for (const key of required) if (!Object.hasOwn(object, key)) fail(path, `missing ${key}`);
}

function boundedAge(value, path) {
  if (!Number.isSafeInteger(value) || value < 0 || value > LIMITS.maxAgeMs) fail(path, 'expected a bounded nonnegative integer age in milliseconds');
}

function referenceName(value, path) {
  if (typeof value !== 'string' || value.length > 128 || !/^[A-Za-z][A-Za-z0-9_]*(?:\.[A-Za-z][A-Za-z0-9_]*)+$/.test(value)
    || value.split('.').some(part => FORBIDDEN_KEYS.has(part))) fail(path, 'invalid registered field reference');
}

function catalogFor(input) {
  const catalog = cloneJson(input, 'catalog');
  if (!plainObject(catalog) || Object.keys(catalog).length > LIMITS.maxFields) fail('catalog', 'expected a bounded field map');
  for (const [name, entry] of Object.entries(catalog)) {
    const path = `catalog.${name}`;
    referenceName(name, path);
    keysExactly(entry, ['unit', 'max_age_ms', 'description', 'calculation_version', 'scope'], ['source', 'precision', 'quote'], path);
    if (!UNIT_SET.has(entry.unit)) fail(path, 'unregistered unit');
    boundedAge(entry.max_age_ms, `${path}.max_age_ms`);
    if (!['account', 'instrument'].includes(entry.scope)) fail(path, 'scope must be account or instrument');
    for (const key of ['description', 'calculation_version', ...(Object.hasOwn(entry, 'source') ? ['source'] : [])]) {
      if (typeof entry[key] !== 'string' || !entry[key].trim() || entry[key].length > 1024) fail(`${path}.${key}`, 'expected bounded nonempty text');
    }
    if (Object.hasOwn(entry, 'precision') && (!Number.isSafeInteger(entry.precision) || entry.precision < 0 || entry.precision > MAX_SCALE)) fail(path, 'invalid decimal precision');
    if (Object.hasOwn(entry, 'quote') && typeof entry.quote !== 'boolean') fail(path, 'quote flag must be boolean');
    if (entry.unit === 'boolean' && (Object.hasOwn(entry, 'precision') || entry.quote === true)) fail(path, 'boolean fields cannot declare decimal precision or be quotes');
  }
  return catalog;
}

function operandUnit(operand, catalog, path) {
  if (!plainObject(operand)) fail(path, 'expected operand object');
  if (Object.hasOwn(operand, 'ref')) {
    keysExactly(operand, ['ref'], [], path);
    referenceName(operand.ref, `${path}.ref`);
    if (!Object.hasOwn(catalog, operand.ref)) fail(path, `unregistered ref ${operand.ref}`);
    return catalog[operand.ref].unit;
  }
  keysExactly(operand, ['literal', 'unit'], [], path);
  if (!UNIT_SET.has(operand.unit)) fail(path, 'unregistered unit');
  if (operand.unit === 'boolean') {
    if (typeof operand.literal !== 'boolean') fail(path, 'boolean literal must be a boolean');
  } else {
    try { parseDecimal(operand.literal); } catch (error) { fail(path, error.message); }
  }
  return operand.unit;
}

function prepareCondition(condition, inputCatalog) {
  const catalog = catalogFor(inputCatalog);
  const tree = cloneJson(condition, 'condition');
  let nodes = 0;
  function visit(node, path, depth) {
    nodes += 1;
    if (nodes > LIMITS.maxNodes || depth > LIMITS.maxDepth) fail(path, 'condition exceeds node or depth limit');
    if (!plainObject(node)) fail(path, 'expected condition object');
    if (node.op === 'all' || node.op === 'any') {
      keysExactly(node, ['op', 'args'], [], path);
      if (!Array.isArray(node.args) || node.args.length < 1 || node.args.length > LIMITS.maxChildren) fail(path, 'args must be a nonempty bounded condition array');
      node.args.forEach((child, index) => visit(child, `${path}.args[${index}]`, depth + 1));
    } else if (node.op === 'not') {
      keysExactly(node, ['op', 'arg'], [], path);
      visit(node.arg, `${path}.arg`, depth + 1);
    } else if (COMPARISONS.has(node.op)) {
      keysExactly(node, ['op', 'left', 'right'], [], path);
      const leftUnit = operandUnit(node.left, catalog, `${path}.left`);
      const rightUnit = operandUnit(node.right, catalog, `${path}.right`);
      if (leftUnit !== rightUnit) fail(path, `unit mismatch ${leftUnit} and ${rightUnit}`);
      if (leftUnit === 'boolean' && node.op !== 'eq') fail(path, 'boolean operands support equality only');
    } else {
      fail(path, 'unknown condition operator');
    }
  }
  visit(tree, 'condition', 1);
  return { tree, catalog };
}

function validateCondition(condition, catalog = DEFAULT_FIELD_CATALOG) {
  prepareCondition(condition, catalog);
  return true;
}

function timestamp(value, path) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(value)) fail(path, 'expected an ISO UTC timestamp with seconds or millisecond precision');
  const time = Date.parse(value);
  const normalized = value.includes('.') ? value : value.replace('Z', '.000Z');
  if (!Number.isFinite(time) || new Date(time).toISOString() !== normalized) fail(path, 'invalid calendar timestamp');
  return time;
}

function evaluationOptions(options) {
  if (!plainObject(options)) fail('options', 'expected a plain object');
  const descriptors = Object.getOwnPropertyDescriptors(options);
  if (Object.getOwnPropertySymbols(options).length > 0) fail('options', 'symbol keys are prohibited');
  const safe = Object.create(null);
  for (const [key, descriptor] of Object.entries(descriptors)) {
    if (!['fields', 'now', 'catalog', 'maxQuoteAgeMs'].includes(key)) fail('options', `unknown key ${key}`);
    if (!Object.hasOwn(descriptor, 'value') || !descriptor.enumerable) fail('options', 'accessors and hidden properties are prohibited');
    safe[key] = descriptor.value;
  }
  if (!Object.hasOwn(safe, 'fields') || !Object.hasOwn(safe, 'now')) fail('options', 'fields and now are required');
  const now = timestamp(safe.now, 'options.now');
  if (safe.maxQuoteAgeMs !== undefined) boundedAge(safe.maxQuoteAgeMs, 'options.maxQuoteAgeMs');
  return { fields: safe.fields, now, catalog: safe.catalog === undefined ? DEFAULT_FIELD_CATALOG : safe.catalog, maxQuoteAgeMs: safe.maxQuoteAgeMs };
}

function evaluateCondition(condition, options) {
  const context = evaluationOptions(options);
  // Full validation precedes evaluation; a permissive sibling cannot conceal an
  // invalid instruction. Evidence failures, in contrast, remain three-valued.
  const { tree, catalog } = prepareCondition(condition, context.catalog);
  const reasons = new Set();
  const cache = new Map();
  function unknown(name, reason) {
    reasons.add(`${name}: ${reason}`);
    return { unknown: true };
  }
  function field(name) {
    if (cache.has(name)) return cache.get(name);
    let result;
    try {
      if (!plainObject(context.fields) || Object.getOwnPropertySymbols(context.fields).length > 0) fail('fields', 'invalid evidence map');
      const descriptor = Object.getOwnPropertyDescriptor(context.fields, name);
      if (!descriptor) result = unknown(name, 'missing evidence');
      else {
        if (!Object.hasOwn(descriptor, 'value') || !descriptor.enumerable) fail('evidence', 'accessor or hidden evidence is prohibited');
        const evidence = cloneJson(descriptor.value, name, { maxNodes: 32, maxDepth: 3 });
        keysExactly(evidence, ['quality'], ['value', 'unit', 'observed_at', 'available_at'], name);
        if (evidence.quality !== 'valid') {
          result = unknown(name, ['missing', 'invalid'].includes(evidence.quality) ? `quality is ${evidence.quality}` : 'unregistered quality');
        } else {
          keysExactly(evidence, ['quality', 'value', 'unit', 'observed_at', 'available_at'], [], name);
          const definition = catalog[name];
          if (evidence.unit !== definition.unit) fail(name, 'evidence unit does not match catalog');
          if (evidence.unit === 'boolean') {
            if (typeof evidence.value !== 'boolean') fail(name, 'expected boolean evidence');
          } else {
            const parsed = parseDecimal(evidence.value);
            if (definition.precision !== undefined && parsed.scale > definition.precision) fail(name, 'evidence exceeds registered precision');
          }
          const observed = timestamp(evidence.observed_at, `${name}.observed_at`);
          const available = timestamp(evidence.available_at, `${name}.available_at`);
          const maxAge = definition.quote === true && context.maxQuoteAgeMs !== undefined
            ? Math.min(definition.max_age_ms, context.maxQuoteAgeMs) : definition.max_age_ms;
          if (observed > context.now) result = unknown(name, 'observation is in the future');
          else if (available > context.now) result = unknown(name, 'evidence was not yet available');
          else if (available < observed) result = unknown(name, 'availability precedes observation');
          else if (context.now - observed > maxAge) result = unknown(name, 'evidence is stale');
          else result = { unknown: false, value: evidence.value, unit: evidence.unit };
        }
      }
    } catch (error) {
      result = unknown(name, `invalid evidence (${error.message})`);
    }
    cache.set(name, result);
    return result;
  }
  function operand(value) {
    return Object.hasOwn(value, 'ref') ? field(value.ref) : { unknown: false, value: value.literal, unit: value.unit };
  }
  function evaluate(node) {
    if (node.op === 'not') {
      const value = evaluate(node.arg);
      return value === UNKNOWN ? UNKNOWN : !value;
    }
    if (node.op === 'all' || node.op === 'any') {
      const values = node.args.map(evaluate);
      if (node.op === 'all') return values.includes(false) ? false : values.includes(UNKNOWN) ? UNKNOWN : true;
      return values.includes(true) ? true : values.includes(UNKNOWN) ? UNKNOWN : false;
    }
    const left = operand(node.left);
    const right = operand(node.right);
    if (left.unknown || right.unknown) return UNKNOWN;
    const comparison = left.unit === 'boolean' ? (left.value === right.value ? 0 : 1) : compareDecimals(left.value, right.value);
    switch (node.op) {
      case 'lt': return comparison < 0;
      case 'lte': return comparison <= 0;
      case 'eq': return comparison === 0;
      case 'gte': return comparison >= 0;
      case 'gt': return comparison > 0;
      default: throw new Error('Unreachable validated condition operator');
    }
  }
  return { value: evaluate(tree), reasons: [...reasons] };
}

module.exports = { validateCondition, evaluateCondition, DEFAULT_FIELD_CATALOG, REGISTERED_UNITS, LIMITS };
