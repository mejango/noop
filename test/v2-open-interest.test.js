'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { HOURLY_OPEN_INTEREST_SQL, openInterestHourBounds } = require('../bot/open-interest');

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-oi-'));
process.env.DATA_DIR = dir;
delete process.env.NOOP_DB_PATH;
const store = require('../bot/db');
const db = store.db;
const insert = db.prepare('INSERT INTO options_snapshots (timestamp,instrument_name,open_interest) VALUES (?,?,?)');
const since = '2026-09-10T00:00:00Z';
const nowMs = Date.parse('2026-09-11T02:37:00Z');
test.after(() => { store.close(); fs.rmSync(dir, { recursive: true, force: true }); });
test.beforeEach(() => db.exec('DELETE FROM options_snapshots'));
const read = () => store.getOpenInterestHourly(since, nowMs);

test('unequal observation counts do not inflate OI and each instrument uses its latest value', () => {
  insert.run('2026-09-10T01:05:00Z', 'put', 100);
  insert.run('2026-09-10T01:06:00Z', 'call', 50);
  for (let minute = 0; minute < 60; minute++) {
    insert.run(`2026-09-10T02:${String(minute).padStart(2, '0')}:00Z`, 'put', 100);
  }
  insert.run('2026-09-10T02:01:00Z', 'call', 50);
  assert.deepEqual(read(), [
    { hour: '2026-09-10T01:00:00Z', value: 150 },
    { hour: '2026-09-10T02:00:00Z', value: 150 },
  ]);
  insert.run('2026-09-10T02:59:01Z', 'put', 120);
  assert.equal(read()[1].value, 170);
});

test('newest missing or invalid OI makes the observed hourly population unknown', () => {
  for (const invalid of [null, -1, 'invalid', Infinity]) {
    db.exec('DELETE FROM options_snapshots');
    insert.run('2026-09-10T01:00:00Z', 'put', 100);
    insert.run('2026-09-10T01:30:00Z', 'put', invalid);
    insert.run('2026-09-10T01:40:00Z', 'call', 50);
    assert.deepEqual(read(), [{ hour: '2026-09-10T01:00:00Z', value: null }]);
  }
});

test('zero OI is a known stock and does not resurrect an older positive observation', () => {
  insert.run('2026-09-10T01:00:00Z', 'put', 100);
  insert.run('2026-09-10T01:01:00Z', 'put', 0);
  assert.deepEqual(read(), [{ hour: '2026-09-10T01:00:00Z', value: 0 }]);
});

test('actual timestamp orders observations; later IDs break equal-time ties', () => {
  insert.run('2026-09-10T01:00:00.001Z', 'put', 120);
  insert.run('2026-09-10T01:00:00Z', 'put', 100);
  assert.equal(read()[0].value, 120);
  insert.run('2026-09-10T01:00:00.001Z', 'put', 130);
  assert.equal(read()[0].value, 130);
});

test('only complete UTC hours inside the requested window are returned', () => {
  insert.run('2026-09-10T00:59:00Z', 'put', 999);
  insert.run('2026-09-10T01:00:00.000Z', 'put', 100);
  insert.run('2026-09-10T01:00:00Z', 'call', 50);
  insert.run('2026-09-11T01:59:59.999Z', 'put', 150);
  insert.run('2026-09-11T02:00:00.000Z', 'put', 999);
  assert.deepEqual(store.getOpenInterestHourly('2026-09-10T00:30:00Z', nowMs), [
    { hour: '2026-09-10T01:00:00Z', value: 150 },
    { hour: '2026-09-11T01:00:00Z', value: 150 },
  ]);
});

test('reading OI does not write history, uses the timestamp index, and rejects invalid bounds', () => {
  const before = db.prepare('SELECT total_changes() AS n').get().n;
  assert.deepEqual(read(), []);
  assert.equal(db.prepare('SELECT total_changes() AS n').get().n, before);
  const plan = db.prepare('EXPLAIN QUERY PLAN ' + HOURLY_OPEN_INTEREST_SQL).all(openInterestHourBounds(since, nowMs));
  assert.ok(plan.some(row => /SEARCH options_snapshots USING INDEX.*timestamp>\? AND timestamp<\?/.test(row.detail)), JSON.stringify(plan));
  assert.throws(() => store.getOpenInterestHourly('invalid', nowMs), /valid timestamps/);
  assert.throws(() => store.getOpenInterestHourly(since, NaN), /valid timestamps/);
});
