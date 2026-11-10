'use strict';
// Gate: the dashboard image copies an explicit list of bot/ files (Dockerfile),
// so every bot module the dashboard imports must be on that list or the
// production dashboard build fails.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const walk = dir => fs.readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
  const full = path.join(dir, entry.name);
  return entry.isDirectory() ? walk(full) : /\.tsx?$/.test(entry.name) ? [full] : [];
});

test('every bot module imported by the dashboard is copied into the dashboard image', () => {
  const dashboardStage = fs.readFileSync(path.join(root, 'Dockerfile'), 'utf8').split(/^FROM /m)
    .find(stage => stage.includes('COPY dashboard/'));
  const copied = new Set([...dashboardStage.matchAll(/\bbot\/[\w.-]+/g)].map(match => match[0]));
  const imported = new Set();
  for (const file of walk(path.join(root, 'dashboard/src'))) {
    for (const match of fs.readFileSync(file, 'utf8').matchAll(/from '((?:\.\.\/)+bot\/[\w.-]+)'/g)) {
      const resolved = path.relative(root, path.resolve(path.dirname(file), match[1]));
      imported.add(fs.existsSync(path.join(root, resolved + '.js')) ? resolved + '.js' : resolved);
    }
  }
  assert.ok(imported.has('bot/put-budget.js'));
  assert.deepEqual([...imported].filter(file => !copied.has(file)), []);
});
