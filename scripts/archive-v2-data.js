#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { execFileSync } = require('node:child_process');
const Database = require('better-sqlite3');

const ROOT = path.join(__dirname, '..');
const TIME_COLUMNS = new Set(['timestamp', 'observed_at', 'quote_received_at', 'due_at', 'evaluated_at', 'created_at', 'updated_at', 'opened_at', 'closed_at', 'occurred_at', 'applied_at', 'hour']);
const POLICY_FILES = ['bot/strategy-facts.json', 'bot/call-score.js', 'bot/put-score.js', 'bot/trade-policy.js', 'bot/hourly-rollups.js'];
const quote = (identifier) => `"${identifier.replaceAll('"', '""')}"`;
const digest = (value) => crypto.createHash('sha256').update(value).digest('hex');

function parseArguments(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index++) {
    if (argv[index] === '--help') return { help: true };
    const match = /^(--db|--out)(?:=(.*))?$/.exec(argv[index]);
    if (!match) throw new Error(`Unknown argument: ${argv[index]}`);
    const value = match[2] ?? argv[++index];
    if (!value || value.startsWith('--')) throw new Error(`${match[1]} requires a path`);
    const key = match[1] === '--db' ? 'dbPath' : 'outPath';
    if (args[key]) throw new Error(`Duplicate argument: ${match[1]}`);
    args[key] = value;
  }
  if (!args.dbPath || !args.outPath) throw new Error('Both --db and --out are required; no live database path is selected by default');
  return args;
}

function snapshotMetadata(db) {
  const schema = db.prepare("SELECT type, name, tbl_name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type, name").all();
  const tables = {};
  for (const { name } of schema.filter((row) => row.type === 'table' && !row.name.startsWith('sqlite_'))) {
    const columns = db.prepare(`PRAGMA table_info(${quote(name)})`).all();
    const times = columns.filter((column) => TIME_COLUMNS.has(column.name));
    const fields = times.flatMap((column, index) => [
      `strftime('%Y-%m-%dT%H:%M:%fZ', MIN(julianday(${quote(column.name)}))) AS first_${index}`,
      `strftime('%Y-%m-%dT%H:%M:%fZ', MAX(julianday(${quote(column.name)}))) AS last_${index}`,
    ]);
    const row = db.prepare(`SELECT COUNT(*) AS rows${fields.length ? ', ' + fields.join(', ') : ''} FROM ${quote(name)}`).get();
    tables[name] = { rows: row.rows, time_bounds: Object.fromEntries(times.map((column, index) => [column.name, { first: row[`first_${index}`], last: row[`last_${index}`] }])) };
  }
  return {
    schema: { sha256: digest(JSON.stringify(schema)), user_version: db.pragma('user_version', { simple: true }), application_id: db.pragma('application_id', { simple: true }), definitions: schema },
    tables,
  };
}

function policyReference() {
  const calls = require('../bot/call-score');
  const puts = require('../bot/put-score');
  let revision = null;
  try { revision = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: ROOT, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim(); } catch { /* Packaged deployments may omit git. */ }
  return {
    source: 'Local archiver code; this does not establish which policy produced historical rows.',
    git_revision: revision,
    files: Object.fromEntries(POLICY_FILES.map((file) => [file, { sha256: digest(fs.readFileSync(path.join(ROOT, file))) }])),
    strategy_facts: JSON.parse(fs.readFileSync(path.join(ROOT, 'bot/strategy-facts.json'), 'utf8')),
    normalization: {
      call: { reference_dte: calls.SELL_CALL_EDGE_REFERENCE_DTE, exponent: calls.SELL_CALL_EDGE_DTE_EXPONENT },
      put: { reference_dte: puts.BUY_PUT_EDGE_REFERENCE_DTE, exponent: puts.BUY_PUT_EDGE_DTE_EXPONENT },
    },
    archiver_sha256: digest(fs.readFileSync(__filename)),
  };
}

async function fileDigest(filename) {
  const hash = crypto.createHash('sha256');
  for await (const chunk of fs.createReadStream(filename)) hash.update(chunk);
  return hash.digest('hex');
}

async function archiveV2Data({ dbPath, outPath }) {
  if (!dbPath || !outPath) throw new Error('Explicit dbPath and outPath are required');
  const sourcePath = fs.realpathSync(path.resolve(dbPath));
  if (!fs.statSync(sourcePath).isFile()) throw new Error('Source must be an existing SQLite file');
  const requestedDestination = path.resolve(outPath);
  fs.mkdirSync(path.dirname(requestedDestination), { recursive: true });
  const destination = path.join(fs.realpathSync(path.dirname(requestedDestination)), path.basename(requestedDestination));
  const manifestPath = `${destination}.manifest.json`;
  const sourceFiles = new Set(['', '-wal', '-shm', '-journal'].map((suffix) => sourcePath + suffix));
  if ([destination, manifestPath].some((file) => sourceFiles.has(file))) throw new Error('Archive paths must differ from the source and its SQLite sidecars');
  const outputFiles = [destination, manifestPath, ...['-wal', '-shm', '-journal'].map((suffix) => destination + suffix)];
  for (const file of outputFiles) {
    try { fs.lstatSync(file); } catch (error) { if (error.code === 'ENOENT') continue; throw error; }
    throw new Error(`Refusing to overwrite existing path: ${file}`);
  }
  const reserved = [];
  let source;
  let snapshot;
  const startedAt = new Date().toISOString();
  try {
    // Exclusive creation prevents two archive runs from overwriting each other.
    for (const file of [destination, manifestPath]) {
      const fd = fs.openSync(file, 'wx', 0o600);
      reserved.push(file);
      fs.closeSync(fd);
    }
    source = new Database(sourcePath, { readonly: true, fileMustExist: true });
    const sourceJournalMode = source.pragma('journal_mode', { simple: true });
    const backup = await source.backup(destination);
    source.close();
    source = null;

    snapshot = new Database(destination, { fileMustExist: true });
    // Only the destination is changed: make the archive one self-contained file.
    snapshot.pragma('journal_mode = DELETE');
    const integrity = snapshot.pragma('integrity_check').map((row) => row.integrity_check);
    if (integrity.length !== 1 || integrity[0] !== 'ok') throw new Error(`Archive integrity check failed: ${integrity.join('; ')}`);
    const metadata = snapshotMetadata(snapshot);
    const sqliteVersion = snapshot.prepare('SELECT sqlite_version() AS version').get().version;
    snapshot.close();
    snapshot = null;

    const manifest = {
      manifest_version: 1,
      created_at: new Date().toISOString(),
      started_at: startedAt,
      method: 'sqlite_online_backup',
      source: { path: sourcePath, journal_mode: sourceJournalMode, modified_or_pruned: false },
      archive: { file: path.basename(destination), bytes: fs.statSync(destination).size, sha256: await fileDigest(destination), integrity_check: 'ok', sqlite_version: sqliteVersion, pages: backup.totalPages },
      ...metadata,
      policy_reference: policyReference(),
    };
    fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
    for (const file of reserved) {
      const fd = fs.openSync(file, 'r');
      try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    }
    const directory = fs.openSync(path.dirname(destination), 'r');
    try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
    return { archive: destination, manifest: manifestPath, sha256: manifest.archive.sha256 };
  } catch (error) {
    if (snapshot?.open) snapshot.close();
    if (source?.open) source.close();
    for (const file of reserved) fs.rmSync(file, { force: true });
    if (reserved.includes(destination)) {
      for (const suffix of ['-wal', '-shm', '-journal']) fs.rmSync(destination + suffix, { force: true });
    }
    throw error;
  }
}

if (require.main === module) {
  (async () => {
    const args = parseArguments(process.argv.slice(2));
    if (args.help) {
      console.log('Usage: node scripts/archive-v2-data.js --db=/existing/noop.db --out=/new/archive.db\nCreates a verified SQLite snapshot and .manifest.json; never overwrites or prunes the source.');
      return;
    }
    console.log(JSON.stringify(await archiveV2Data(args), null, 2));
  })().catch((error) => { console.error(error.message); process.exitCode = 1; });
}

module.exports = { archiveV2Data, parseArguments };
