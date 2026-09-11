'use strict';

// Run with the same Node/dependencies as npm test. No production files change:
// the child process deliberately disables the real condition evaluator in
// memory, then checks that the existing trading suite notices the regression.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

if (process.argv.includes('--child')) {
  const Module = require('node:module');
  const originalLoad = Module._load;
  Module._load = function (name, ...rest) {
    const exports = originalLoad.call(this, name, ...rest);
    if (/[\\/]bot[\\/]trade-policy(?:\.js)?$/.test(name)) {
      return { ...exports, evaluateConditions: () => false };
    }
    return exports;
  };
  const originalRead = fs.readFileSync;
  fs.readFileSync = function (file, ...args) {
    const value = originalRead.call(this, file, ...args);
    if (String(file) === path.resolve(__dirname, '../../script.js') && typeof value === 'string') {
      return value.replace('const evaluateConditions = (conditions, logic, values) => {',
        'const evaluateConditions = (conditions, logic, values) => { return false;');
    }
    return value;
  };
  require('../trading-system.test');
} else {
  const child = spawnSync(process.execPath, [__filename, '--child'], {
    encoding: 'utf8', timeout: 30000, maxBuffer: 4 * 1024 * 1024,
  });
  assert.ifError(child.error);
  assert.equal(child.status, 1, `Disabled production conditions escaped the suite:\n${child.stdout}\n${child.stderr}`);
  assert.match(child.stdout, /FAIL single condition gt - true:/,
    `The real condition regression must be detected, not an unrelated startup error:\n${child.stdout}\n${child.stderr}`);
  const summary = child.stdout.match(/Results: \d+ passed, \d+ failed/);
  assert.ok(summary, 'The trading suite must finish under mutation');
  console.log(`Production mutation detected. ${summary[0]}`);
}
