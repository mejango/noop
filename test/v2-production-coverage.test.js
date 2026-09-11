'use strict';

const assert = require('node:assert/strict');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const test = require('node:test');
const { loadProduction } = require('./helpers/load-production');

test('production helper loading does not import the trading entry point', () => {
  const helpers = loadProduction(['evaluateConditions', 'computePostOnlyRetryPrice']);
  assert.equal(typeof helpers.evaluateConditions, 'function');
  assert.equal(typeof helpers.computePostOnlyRetryPrice, 'function');
  assert.equal(require.cache[path.resolve(__dirname, '../script.js')], undefined);
});

test('disabling the production condition evaluator fails existing regression tests', () => {
  const child = spawnSync(process.execPath, [path.join(__dirname, 'helpers/production-mutation-probe.js')], {
    encoding: 'utf8', timeout: 40000, maxBuffer: 4 * 1024 * 1024,
  });
  assert.ifError(child.error);
  assert.equal(child.status, 0, `${child.stdout}\n${child.stderr}`);
  assert.match(child.stdout, /Production mutation detected/);
});
