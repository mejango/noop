const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const Database = require('better-sqlite3');
const { fingerprint, compare } = require('../scripts/verify-db-continuity');

test('continuity verification detects changed historical values even when row counts match', t => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-continuity-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const file = path.join(dir, 'fixture.db');
  const db = new Database(file);
  db.exec("CREATE TABLE history(id INTEGER PRIMARY KEY, amount TEXT); INSERT INTO history VALUES(1,'0.123456789123456789')");
  db.close();
  const before = fingerprint(file);
  assert.equal(compare(before, fingerprint(file)).identical, true);
  const edit = new Database(file);
  edit.exec("UPDATE history SET amount='0.123456789123456788' WHERE id=1");
  edit.close();
  assert.deepEqual(compare(before, fingerprint(file)).differences, ['history']);
  const schema = new Database(file);
  schema.exec('ALTER TABLE history ADD COLUMN unexpected TEXT');
  schema.close();
  assert.ok(compare(before, fingerprint(file)).differences.includes('schema'));
});
