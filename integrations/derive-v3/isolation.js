'use strict';

const fs = require('node:fs');
const path = require('node:path');

function lstatIfPresent(file) {
  try { return fs.lstatSync(file); } catch (error) { if (error.code === 'ENOENT') return null; throw error; }
}

function assertNoSymlinkTree(file) {
  const stat = lstatIfPresent(file);
  if (!stat) return;
  if (stat.isSymbolicLink()) throw new Error('V3 state cannot use symlinked files or directories');
  if (stat.isDirectory()) for (const entry of fs.readdirSync(file)) assertNoSymlinkTree(path.join(file, entry));
}

// Shared with the dashboard without loading the SDK or opening a database.
function assertIsolatedDataPaths(profile, env, root) {
  if (profile.version !== 3) return;
  if (!profile.ownerAddress || !profile.subaccountId) throw new Error('V3 state requires its dedicated owner and subaccount');
  const accountDir = path.resolve(root, '.derive-v3', profile.network, String(profile.subaccountId));
  for (const [name, suffix] of [['DATA_DIR', 'data'], ['WIKI_DIR', 'knowledge']]) {
    const expected = path.join(accountDir, suffix);
    if (!env[name] || path.resolve(env[name]) !== expected) {
      throw new Error(`V3 state ${name} must be ${expected}`);
    }
    // Check existing ancestors too: a new data directory inside a linked account
    // directory would otherwise pass before the database is created.
    let current = expected;
    while (!lstatIfPresent(current) && path.dirname(current) !== current) current = path.dirname(current);
    if (fs.lstatSync(current).isSymbolicLink() || fs.realpathSync(current) !== current) throw new Error(`V3 state ${name} cannot use symlinked state`);
    assertNoSymlinkTree(expected);
  }
}

module.exports = { assertIsolatedDataPaths };
