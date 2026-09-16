'use strict';

const { createHash } = require('node:crypto');

// noop.strategy/v1: sorted UTF-16 property names, JSON string escaping, safe
// integer JSON numbers only. Decimal economics remain strings, including scale.
// The limits are parser/control-plane capacity limits, not economic policies.
function canonicalize(value) {
  const seen = new Set();
  let nodes = 0;
  let bytes = 0;
  function bounded(text) {
    bytes += Buffer.byteLength(text);
    if (bytes > 4 * 1024 * 1024) throw new Error('JSON byte capacity exceeded');
    return text;
  }
  function walk(item, depth) {
    if (++nodes > 100000 || depth > 64) throw new Error('JSON capacity exceeded');
    if (item === null || typeof item === 'boolean') return bounded(JSON.stringify(item));
    if (typeof item === 'string') {
      if (item.length > 1048576 || /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?:^|[^\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(item)) {
        throw new Error('Invalid or oversized JSON string');
      }
      return bounded(JSON.stringify(item));
    }
    if (typeof item === 'number') {
      if (!Number.isSafeInteger(item) || Object.is(item, -0)) throw new Error('JSON numbers must be safe integers; use decimal strings');
      return bounded(String(item));
    }
    if (typeof item !== 'object') throw new Error('Non-JSON value');
    if (seen.has(item)) throw new Error('Cyclic JSON value');
    const proto = Object.getPrototypeOf(item);
    if (Array.isArray(item) && proto !== Array.prototype) throw new Error('Non-plain JSON array');
    if (!Array.isArray(item) && proto !== Object.prototype && proto !== null) throw new Error('Non-plain JSON object');
    if (Object.getOwnPropertySymbols(item).length) throw new Error('Symbol JSON key');
    seen.add(item);
    const keys = Object.keys(item);
    const ownNames = Object.getOwnPropertyNames(item).filter(key => !(Array.isArray(item) && key === 'length'));
    if (ownNames.length !== keys.length) throw new Error('Hidden JSON property');
    for (const key of keys) {
      if (['__proto__', 'prototype', 'constructor'].includes(key)) throw new Error('Unsafe JSON key');
      const descriptor = Object.getOwnPropertyDescriptor(item, key);
      if (!descriptor || !Object.prototype.hasOwnProperty.call(descriptor, 'value')) throw new Error('JSON accessor forbidden');
      if (!Array.isArray(item)) walk(key, depth + 1);
    }
    let output;
    if (Array.isArray(item)) {
      if (keys.length !== item.length || keys.some((key, index) => key !== String(index))) throw new Error('Sparse or extended JSON array');
      bounded('[]' + ','.repeat(Math.max(0, item.length - 1)));
      output = '[' + item.map(entry => walk(entry, depth + 1)).join(',') + ']';
    } else {
      bounded('{}' + ':'.repeat(keys.length) + ','.repeat(Math.max(0, keys.length - 1)));
      output = '{' + keys.sort().map(key => JSON.stringify(key) + ':' + walk(item[key], depth + 1)).join(',') + '}';
    }
    seen.delete(item);
    return output;
  }
  const output = walk(value, 0);
  return output;
}

function contentDigest(value) {
  return 'sha256:' + createHash('sha256').update(canonicalize(value)).digest('hex');
}

module.exports = { canonicalize, contentDigest };
