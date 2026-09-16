'use strict';

const fs = require('node:fs');
const path = require('node:path');
const ROOT = path.resolve(__dirname, '../../.derive-v3');

function stateDirectory(profile) {
  // Deliberately not DATA_DIR: a sidecar must never share the production ledger.
  const dir = path.join(ROOT, profile.network, String(profile.subaccountId || 'public'));
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  // Reject symlinks anywhere beneath the workspace root.
  if (fs.realpathSync(dir) !== dir) throw new Error('V3 state directory must not contain symlinks');
  return dir;
}

function openJournal(profile) {
  const filename = path.join(stateDirectory(profile), 'execution.jsonl');
  if (fs.existsSync(filename) && fs.lstatSync(filename).isSymbolicLink()) throw new Error('V3 journal must not be a symlink');
  const identity = { venue: profile.name, owner: profile.ownerAddress?.toLowerCase(), subaccount_id: profile.subaccountId };
  let unresolved = new Set();
  if (fs.existsSync(filename)) {
    for (const line of fs.readFileSync(filename, 'utf8').split('\n').filter(Boolean)) {
      const row = JSON.parse(line); // Corrupt/partial journals fail closed.
      for (const key of Object.keys(identity)) {
        if (row[key] !== identity[key]) throw new Error('V3 journal identity mismatch');
      }
      if (row.event === 'order_intent') unresolved.add(row.nonce);
      // Venue acknowledgement/discovery does not prove Noop saved the fill,
      // budget and remaining resting exposure. Only an accounting checkpoint
      // (or a definitive rejection) permits a restart to submit another order.
      if (row.event === 'order_accounted'
        || (row.event === 'order_rejected' && require('./index').isDefinitiveOrderRejection(row.code))) unresolved.delete(row.nonce);
    }
  }
  const append = (event) => {
    const fd = fs.openSync(filename, 'a', 0o600);
    try {
      fs.writeSync(fd, `${JSON.stringify({ ...event, ...identity, timestamp: new Date().toISOString() })}\n`);
      fs.fsyncSync(fd);
    } finally { fs.closeSync(fd); }
  };
  return { append, unresolved: [...unresolved], filename };
}

function acquireWriter(profile) {
  const filename = path.join(stateDirectory(profile), 'writer.lock');
  try {
    fs.writeFileSync(filename, JSON.stringify({ pid: process.pid, venue: profile.name, owner: profile.ownerAddress }), { flag: 'wx', mode: 0o600 });
  } catch (error) {
    if (error.code === 'EEXIST') throw new Error(`V3 writer lock exists: ${filename}; verify the old process has stopped before removing it`);
    throw error;
  }
  return () => fs.unlinkSync(filename);
}

module.exports = { stateDirectory, openJournal, acquireWriter };
