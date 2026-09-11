'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const { execFileSync } = require('node:child_process');
const Database = require('better-sqlite3');
const { archiveV2Data, parseArguments } = require('../scripts/archive-v2-data');
const { createHourlyRollups } = require('../bot/hourly-rollups');
const { loadHistoricalFrames } = require('../research/sell-call-backtest/data-source');

const schemaDir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-archive-schema-'));
const oldDataDir = process.env.DATA_DIR;
process.env.DATA_DIR = schemaDir;
const production = require('../bot/db');
const schema = production.db.prepare("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'").all();
production.close();
if (oldDataDir == null) delete process.env.DATA_DIR;
else process.env.DATA_DIR = oldDataDir;
fs.rmSync(schemaDir, { recursive: true, force: true });

function fixture(t, wal = false) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-archive-test-'));
  const dbPath = path.join(directory, 'source.db');
  const outPath = path.join(directory, 'archive.db');
  const db = new Database(dbPath);
  if (wal) {
    db.pragma('journal_mode = WAL');
    db.pragma('wal_autocheckpoint = 0');
  }
  for (const { sql } of schema) db.exec(sql);
  t.after(() => {
    if (db.open) db.close();
    fs.rmSync(directory, { recursive: true, force: true });
  });
  return { directory, dbPath, outPath, db };
}

const sha256 = (filename) => crypto.createHash('sha256').update(fs.readFileSync(filename)).digest('hex');

test('archives committed WAL evidence without changing source bytes, and raw data remains rebuildable', async (t) => {
  const { directory, dbPath, outPath, db } = fixture(t, true);
  db.exec(`INSERT INTO spot_prices (timestamp, price) VALUES
    ('2026-09-10T10:00:00Z', 1900), ('2026-09-10T10:00:00.500Z', 2100);
    INSERT INTO options_snapshots (timestamp, instrument_name, option_type, strike, expiry,
      delta, bid_price, ask_price, mark_price, bid_amount, ask_amount, bid_delta_value, open_interest)
    VALUES ('2026-09-10T10:00:00.500Z', 'ETH-20260918-2200-C', 'C', 2200,
      strftime('%s', '2026-09-18T08:00:00Z'), 0.1, 10, 11, 10.5, 5, 5, 100, 50);`);
  assert.ok(fs.statSync(`${dbPath}-wal`).size > 0);
  const sourceBefore = sha256(dbPath);
  const walBefore = sha256(`${dbPath}-wal`);
  const result = await archiveV2Data({ dbPath, outPath });
  assert.equal(sha256(dbPath), sourceBefore);
  assert.equal(sha256(`${dbPath}-wal`), walBefore);
  const manifest = JSON.parse(fs.readFileSync(result.manifest, 'utf8'));
  assert.equal(result.sha256, sha256(outPath));
  assert.equal(manifest.archive.sha256, result.sha256);
  assert.equal(manifest.archive.bytes, fs.statSync(outPath).size);
  assert.equal(manifest.archive.integrity_check, 'ok');
  assert.ok(manifest.archive.pages > 0);
  assert.equal(manifest.source.journal_mode, 'wal');
  assert.equal(manifest.tables.spot_prices.rows, 2);
  assert.deepEqual(manifest.tables.spot_prices.time_bounds.timestamp, {
    first: '2026-09-10T10:00:00.000Z', last: '2026-09-10T10:00:00.500Z',
  });
  assert.equal(manifest.tables.options_snapshots.rows, 1);
  assert.equal(manifest.schema.sha256.length, 64);
  assert.equal(manifest.policy_reference.files['bot/call-score.js'].sha256.length, 64);
  assert.equal(manifest.policy_reference.normalization.call.exponent, 0.12);

  const archived = new Database(outPath, { readonly: true, fileMustExist: true });
  assert.equal(archived.pragma('journal_mode', { simple: true }), 'delete');
  assert.equal(loadHistoricalFrames(archived, { days: 'all' }).frames.length, 1);
  archived.close();
  assert.equal(fs.existsSync(`${outPath}-wal`), false);

  // Rebuild on a working copy, preserving the immutable archive and its checksum.
  const restorePath = path.join(directory, 'restore.db');
  fs.copyFileSync(outPath, restorePath);
  const restored = new Database(restorePath);
  createHourlyRollups(restored).rebuild();
  assert.equal(restored.prepare('SELECT avg_price FROM spot_prices_hourly').get().avg_price, 2000);
  assert.equal(restored.prepare('SELECT best_call_dv FROM options_hourly').get().best_call_dv, 100);
  restored.close();
  assert.equal(sha256(outPath), result.sha256);
  assert.equal(sha256(dbPath), sourceBefore);
});

test('refuses existing destinations, manifests, and the source or its sidecar paths', async (t) => {
  const { directory, dbPath, outPath, db } = fixture(t);
  db.close();
  const before = sha256(dbPath);
  await assert.rejects(archiveV2Data({ dbPath, outPath: dbPath }), /differ from the source/);
  await assert.rejects(archiveV2Data({ dbPath, outPath: `${dbPath}-wal` }), /differ from the source/);
  fs.symlinkSync(directory, path.join(directory, 'alias'));
  await assert.rejects(archiveV2Data({ dbPath, outPath: path.join(directory, 'alias', 'source.db-wal') }), /differ from the source/);
  fs.writeFileSync(outPath, 'existing archive');
  await assert.rejects(archiveV2Data({ dbPath, outPath }), /Refusing to overwrite/);
  assert.equal(fs.readFileSync(outPath, 'utf8'), 'existing archive');
  fs.unlinkSync(outPath);
  fs.writeFileSync(`${outPath}.manifest.json`, 'existing manifest');
  await assert.rejects(archiveV2Data({ dbPath, outPath }), /Refusing to overwrite/);
  assert.equal(fs.existsSync(outPath), false);
  assert.equal(fs.readFileSync(`${outPath}.manifest.json`, 'utf8'), 'existing manifest');
  assert.equal(sha256(dbPath), before);
});

test('failed invalid-source backup removes its new outputs and preserves the input', async (t) => {
  const { directory, outPath } = fixture(t);
  const invalidPath = path.join(directory, 'invalid.db');
  fs.writeFileSync(invalidPath, 'not a database');
  const before = sha256(invalidPath);
  await assert.rejects(archiveV2Data({ dbPath: invalidPath, outPath }), /not a database/);
  assert.equal(fs.existsSync(outPath), false);
  assert.equal(fs.existsSync(`${outPath}.manifest.json`), false);
  assert.equal(sha256(invalidPath), before);
});

test('CLI requires explicit paths and creates a new verified archive', (t) => {
  assert.throws(() => parseArguments([]), /Both --db and --out/);
  assert.throws(() => parseArguments(['--db=a.db']), /Both --db and --out/);
  assert.throws(() => parseArguments(['--db=a.db', '--out=b.db', '--prune']), /Unknown argument/);
  assert.deepEqual(parseArguments(['--db', 'a.db', '--out=b.db']), { dbPath: 'a.db', outPath: 'b.db' });
  const { dbPath, outPath, db } = fixture(t);
  db.close();
  const result = JSON.parse(execFileSync(process.execPath, [
    path.join(__dirname, '..', 'scripts', 'archive-v2-data.js'), `--db=${dbPath}`, `--out=${outPath}`,
  ], { encoding: 'utf8' }));
  assert.equal(result.archive, fs.realpathSync(outPath));
  assert.equal(result.sha256, sha256(outPath));
});
