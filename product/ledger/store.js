'use strict';

// Explicitly opened product storage. Importing this module neither loads the
// native SQLite binding nor opens a database, and never consults live bot paths.
const fs = require('node:fs');
const path = require('node:path');
const { canonicalize, contentDigest } = require('../../strategy/canonical');
const { validateRelease, validateMandate, timestamp } = require('../../strategy/contract');
const { DEFAULT_FIELD_CATALOG } = require('../../strategy/conditions');
const { createControlState, acceptDecision: acceptPureDecision } = require('../../strategy/replay');
const { addDecimals, compareDecimals } = require('../../strategy/decimal');

const APPLICATION_ID = 0x4e4f4f50;
const SCHEMA_VERSION = 1;
const clone = value => JSON.parse(canonicalize(value));
const assert = (condition, message) => { if (!condition) throw new Error(message); };
function identifier(value, name) {
  assert(typeof value === 'string' && /^(?!(?:constructor|prototype|__proto__)$)[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}$/.test(value), `Invalid ${name}`);
  return value;
}
function accountKey(account) {
  // Derive subaccount IDs identify the venue account independently of owner
  // changes. Owner, manager and risk universe remain pinned mandate authority
  // metadata and cannot manufacture a second allocation of the same account.
  return contentDigest([account.network, account.chain_id, account.venue, account.deployment,
    account.subaccount_id]);
}
function safeFile(filename, { absent = false } = {}) {
  let stat;
  try { stat = fs.lstatSync(filename); } catch (error) { if (absent && error.code === 'ENOENT') return null; throw error; }
  assert(stat.isFile() && !stat.isSymbolicLink() && stat.nlink === 1, 'Ledger files must be regular files without symlinks or hardlinks');
  assert((stat.mode & 0o077) === 0, 'Ledger files must have private permissions (0600)');
  return stat;
}
function checkPath(filename) {
  assert(typeof filename === 'string' && path.isAbsolute(filename) && path.normalize(filename) === filename, 'An explicit normalized absolute ledger filename is required');
  assert(/^[A-Za-z0-9][A-Za-z0-9._-]*\.(?:sqlite|db)$/.test(path.basename(filename)), 'Ledger filename must end in .sqlite or .db');
  const directory = path.dirname(filename);
  const directoryStat = fs.lstatSync(directory);
  assert(directoryStat.isDirectory() && !directoryStat.isSymbolicLink(), 'Ledger directory must be an existing real directory');
  assert(fs.realpathSync(directory) === directory, 'Ledger directory path must not traverse symlinks');
  assert((directoryStat.mode & 0o077) === 0, 'Ledger directory must have private permissions (0700)');
  const names = new Set([path.basename(filename), `${path.basename(filename)}-wal`, `${path.basename(filename)}-shm`, `${path.basename(filename)}-journal`]);
  assert(fs.readdirSync(directory).every(name => names.has(name)), 'Use a dedicated ledger directory without unrelated files');
  const existing = safeFile(filename, { absent: true });
  for (const suffix of ['-wal', '-shm', '-journal']) {
    const sidecar = safeFile(filename + suffix, { absent: true });
    assert(existing || !sidecar, 'Cannot create ledger alongside orphaned SQLite sidecars');
  }
  return existing;
}
function requireMarker(db, ledgerId) {
  assert(db.pragma('application_id', { simple: true }) === APPLICATION_ID, 'Foreign database: product application marker required');
  assert(db.pragma('user_version', { simple: true }) === SCHEMA_VERSION, 'Unsupported product ledger schema');
  const marker = db.prepare('SELECT value FROM ledger_metadata WHERE key = ?').get('ledger_id');
  assert(marker && marker.value === ledgerId, 'Ledger identity mismatch');
}

const SCHEMA = `
CREATE TABLE ledger_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
CREATE TABLE mandates (
  mandate_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, account_key TEXT NOT NULL UNIQUE,
  account_json TEXT NOT NULL, release_json TEXT NOT NULL, mandate_json TEXT NOT NULL,
  catalog_json TEXT NOT NULL, control_json TEXT NOT NULL, registration_digest TEXT NOT NULL
) STRICT;
CREATE TABLE decisions (
  mandate_id TEXT NOT NULL REFERENCES mandates(mandate_id), decision_id TEXT NOT NULL,
  digest TEXT NOT NULL, input_json TEXT NOT NULL, decision_json TEXT NOT NULL,
  result_json TEXT NOT NULL, recorded_at TEXT NOT NULL,
  PRIMARY KEY (mandate_id, decision_id)
) STRICT;
CREATE TABLE events (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT, mandate_id TEXT NOT NULL REFERENCES mandates(mandate_id),
  event_id TEXT NOT NULL, source_key TEXT NOT NULL, economic_ref TEXT NOT NULL, digest TEXT NOT NULL,
  event_json TEXT NOT NULL, recorded_at TEXT NOT NULL, previous_hash TEXT NOT NULL, receipt_hash TEXT NOT NULL,
  UNIQUE (mandate_id, event_id), UNIQUE (mandate_id, source_key), UNIQUE (mandate_id, economic_ref)
) STRICT;
CREATE TABLE postings (
  event_sequence INTEGER NOT NULL REFERENCES events(sequence), posting_index INTEGER NOT NULL,
  account TEXT NOT NULL, asset TEXT NOT NULL, amount TEXT NOT NULL,
  PRIMARY KEY (event_sequence, posting_index)
) STRICT;
CREATE TABLE event_aliases (
  mandate_id TEXT NOT NULL REFERENCES mandates(mandate_id), event_id TEXT NOT NULL,
  event_sequence INTEGER NOT NULL REFERENCES events(sequence), digest TEXT NOT NULL,
  PRIMARY KEY (mandate_id, event_id)
) STRICT;
CREATE TABLE balances (
  mandate_id TEXT NOT NULL REFERENCES mandates(mandate_id), account TEXT NOT NULL, asset TEXT NOT NULL,
  amount TEXT NOT NULL, PRIMARY KEY (mandate_id, account, asset)
) STRICT;
CREATE TRIGGER events_immutable_update BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT, 'Economic receipts are immutable'); END;
CREATE TRIGGER events_immutable_delete BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT, 'Economic receipts are immutable'); END;
CREATE TRIGGER postings_immutable_update BEFORE UPDATE ON postings BEGIN SELECT RAISE(ABORT, 'Economic postings are immutable'); END;
CREATE TRIGGER postings_immutable_delete BEFORE DELETE ON postings BEGIN SELECT RAISE(ABORT, 'Economic postings are immutable'); END;
CREATE TRIGGER decisions_immutable_update BEFORE UPDATE ON decisions BEGIN SELECT RAISE(ABORT, 'Decision receipts are immutable'); END;
CREATE TRIGGER decisions_immutable_delete BEFORE DELETE ON decisions BEGIN SELECT RAISE(ABORT, 'Decision receipts are immutable'); END;
CREATE TRIGGER aliases_immutable_update BEFORE UPDATE ON event_aliases BEGIN SELECT RAISE(ABORT, 'Economic event aliases are immutable'); END;
CREATE TRIGGER aliases_immutable_delete BEFORE DELETE ON event_aliases BEGIN SELECT RAISE(ABORT, 'Economic event aliases are immutable'); END;
`;

function openLedger({ filename, ledgerId, clock = () => new Date().toISOString() } = {}) {
  identifier(ledgerId, 'ledgerId');
  assert(typeof clock === 'function', 'Ledger clock must be a function');
  const existing = checkPath(filename);
  // Native code is deliberately loaded only after an explicit isolated path.
  const Database = require('better-sqlite3');
  if (existing) {
    const header = Buffer.alloc(100);
    const fd = fs.openSync(filename, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    try {
      const length = fs.readSync(fd, header, 0, header.length, 0);
      assert(length === 100 && header.subarray(0, 16).toString('binary') === 'SQLite format 3\u0000'
        && header.readUInt32BE(68) === APPLICATION_ID, 'Foreign database: product application marker required');
    } finally { fs.closeSync(fd); }
    // Read-only discovery never changes foreign databases, their journal mode,
    // schema or permissions. No existing unmarked/empty SQLite DB is adopted.
    let probe;
    try { probe = new Database(filename, { readonly: true, fileMustExist: true }); requireMarker(probe, ledgerId); }
    finally { if (probe) probe.close(); }
    checkPath(filename);
  } else {
    const fd = fs.openSync(filename, fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | fs.constants.O_NOFOLLOW, 0o600);
    fs.closeSync(fd);
  }
  let db;
  try {
    db = new Database(filename, { fileMustExist: true });
    db.pragma('busy_timeout = 5000');
    db.pragma('foreign_keys = ON');
    db.pragma('synchronous = FULL');
    if (!existing) {
      db.transaction(() => {
        db.exec(SCHEMA);
        db.prepare('INSERT INTO ledger_metadata (key,value) VALUES (?,?)').run('ledger_id', ledgerId);
        db.pragma(`application_id = ${APPLICATION_ID}`);
        db.pragma(`user_version = ${SCHEMA_VERSION}`);
      }).immediate();
    }
    requireMarker(db, ledgerId);
    db.pragma('journal_mode = WAL');
    for (const suffix of ['', '-wal', '-shm']) if (fs.existsSync(filename + suffix)) fs.chmodSync(filename + suffix, 0o600);
    if (!existing) {
      const directoryFd = fs.openSync(path.dirname(filename), fs.constants.O_RDONLY);
      try { fs.fsyncSync(directoryFd); } finally { fs.closeSync(directoryFd); }
    }
  } catch (error) { if (db) db.close(); throw error; }

  let closed = false;
  let validatingPolicy = false;
  const ensureOpen = () => assert(!closed, 'Ledger is closed');
  const now = () => { const value = clock(); timestamp(value); return value; };
  const transaction = fn => {
    ensureOpen();
    assert(!validatingPolicy, 'Strategy policy callbacks cannot reenter ledger transactions');
    return db.transaction(fn).immediate();
  };
  function readMandate(mandateId) {
    ensureOpen();
    const row = db.prepare('SELECT * FROM mandates WHERE mandate_id = ?').get(mandateId);
    assert(row, 'Unknown mandate');
    return {
      customer_id: row.customer_id, release: JSON.parse(row.release_json), mandate: JSON.parse(row.mandate_json),
      catalog: JSON.parse(row.catalog_json), controlState: JSON.parse(row.control_json),
    };
  }
  function registerMandate({ customer_id, release, mandate, catalog = DEFAULT_FIELD_CATALOG }) {
    identifier(customer_id, 'customer_id');
    [release, mandate, catalog] = [release, mandate, catalog].map(clone);
    validateRelease(release); validateMandate(mandate, { release });
    const controlState = createControlState(mandate);
    const digest = contentDigest({ customer_id, release, mandate, catalog });
    return transaction(() => {
      const prior = db.prepare('SELECT customer_id,registration_digest FROM mandates WHERE mandate_id = ?').get(mandate.mandate_id);
      if (prior) {
        assert(prior.customer_id === customer_id && prior.registration_digest === digest, 'Mandate ID already registered with different content');
        return { status: 'replayed', mandate_id: mandate.mandate_id };
      }
      const key = accountKey(mandate.account);
      assert(!db.prepare('SELECT 1 FROM mandates WHERE account_key = ?').get(key), 'Account is already assigned to another mandate');
      db.prepare(`INSERT INTO mandates (mandate_id,customer_id,account_key,account_json,release_json,mandate_json,catalog_json,control_json,registration_digest)
        VALUES (?,?,?,?,?,?,?,?,?)`).run(mandate.mandate_id, customer_id, key, canonicalize(mandate.account), canonicalize(release), canonicalize(mandate), canonicalize(catalog), canonicalize(controlState), digest);
      return { status: 'registered', mandate_id: mandate.mandate_id };
    });
  }
  function receipt(row, status = 'recorded') {
    return { status, sequence: row.sequence, mandate_id: row.mandate_id, event_id: row.event_id,
      source_event_id: row.source_key, digest: row.digest, recorded_at: row.recorded_at,
      previous_hash: row.previous_hash, receipt_hash: row.receipt_hash, event: JSON.parse(row.event_json) };
  }
  function loadNormalized(mandateId, event, visiting = new Set()) {
    const { normalizeEvent, reverseNormalizedEvent } = require('./events');
    if (event.kind !== 'reversal') return normalizeEvent(event);
    const originalId = event.payload?.reverses_event_id;
    assert(!visiting.has(originalId), 'Cyclic event reversal');
    visiting.add(originalId);
    const original = db.prepare('SELECT event_json FROM events WHERE mandate_id = ? AND event_id = ?').get(mandateId, originalId);
    assert(original, 'Reversal requires an original event in this mandate');
    assert(JSON.parse(original.event_json).kind !== 'reversal', 'A reversal cannot itself be reversed');
    return reverseNormalizedEvent(loadNormalized(mandateId, JSON.parse(original.event_json), visiting), event);
  }
  function appendEvent(mandateId, event) {
    event = clone(event);
    return transaction(() => {
      readMandate(mandateId);
      verify(mandateId);
      const normalized = loadNormalized(mandateId, event);
      const alias = db.prepare(`SELECT e.* FROM event_aliases a JOIN events e ON e.sequence = a.event_sequence
        WHERE a.mandate_id = ? AND a.event_id = ?`).get(mandateId, normalized.event.event_id);
      const priorRows = db.prepare(`SELECT * FROM events WHERE mandate_id = ? AND
        (event_id = ? OR source_key = ? OR economic_ref = ?)`).all(mandateId, normalized.event.event_id, normalized.source_key, normalized.economic_ref);
      if (alias && !priorRows.some(row => row.sequence === alias.sequence)) priorRows.push(alias);
      if (priorRows.length) {
        assert(priorRows.length === 1 && priorRows[0].digest === normalized.digest, 'Economic event identity reused with conflicting content');
        db.prepare('INSERT OR IGNORE INTO event_aliases (mandate_id,event_id,event_sequence,digest) VALUES (?,?,?,?)')
          .run(mandateId, normalized.event.event_id, priorRows[0].sequence, normalized.digest);
        return receipt(priorRows[0], 'replayed');
      }
      if (normalized.event.kind === 'reversal') {
        const original = db.prepare('SELECT sequence FROM events WHERE mandate_id = ? AND event_id = ?')
          .get(mandateId, normalized.event.payload.reverses_event_id);
        // Corrections cannot undo capital or inventory already consumed by
        // later events. More complex corrections require a reviewed migration;
        // arbitrary compensating postings are intentionally not an API.
        for (const posting of normalized.postings) {
          assert(!db.prepare(`SELECT 1 FROM postings p JOIN events e ON e.sequence = p.event_sequence
            WHERE e.mandate_id = ? AND e.sequence > ? AND p.account = ? AND p.asset = ? LIMIT 1`)
            .get(mandateId, original.sequence, posting.account, posting.asset), 'Reversal would invalidate subsequent economic history');
        }
      }
      if (normalized.event.kind === 'option_settlement') {
        const held = db.prepare("SELECT amount FROM balances WHERE mandate_id = ? AND account = 'assets:options' AND asset = ?")
          .get(mandateId, `OPTION:${normalized.event.payload.instrument}`)?.amount || '0';
        assert(compareDecimals(held, normalized.event.payload.position_quantity) === 0, 'Settlement requires the exact remaining signed option position; reconcile missing evidence first');
      }
      for (const posting of normalized.postings) {
        if (!posting.account.startsWith('liabilities:') && !posting.account.startsWith('assets:interest_receivable:')) continue;
        const held = db.prepare('SELECT amount FROM balances WHERE mandate_id = ? AND account = ? AND asset = ?')
          .get(mandateId, posting.account, posting.asset)?.amount || '0';
        const resulting = compareDecimals(addDecimals(held, posting.amount), '0');
        assert(posting.account.startsWith('liabilities:') ? resulting <= 0 : resulting >= 0,
          'Repayment or correction exceeds recorded financing evidence; reconcile missing evidence first');
      }
      const previous = db.prepare('SELECT receipt_hash FROM events WHERE mandate_id = ? ORDER BY sequence DESC LIMIT 1').get(mandateId);
      const recorded_at = now();
      const previous_hash = previous?.receipt_hash || contentDigest({ ledger_id: ledgerId, mandate_id: mandateId });
      const receipt_hash = contentDigest({ mandate_id: mandateId, event: normalized.event, postings: normalized.postings, recorded_at, previous_hash });
      const result = db.prepare(`INSERT INTO events (mandate_id,event_id,source_key,economic_ref,digest,event_json,recorded_at,previous_hash,receipt_hash)
        VALUES (?,?,?,?,?,?,?,?,?)`).run(mandateId, normalized.event.event_id, normalized.source_key, normalized.economic_ref, normalized.digest,
        canonicalize(normalized.event), recorded_at, previous_hash, receipt_hash);
      const sequence = Number(result.lastInsertRowid);
      assert(Number.isSafeInteger(sequence), 'Ledger sequence exhausted');
      db.prepare('INSERT INTO event_aliases (mandate_id,event_id,event_sequence,digest) VALUES (?,?,?,?)')
        .run(mandateId, normalized.event.event_id, sequence, normalized.digest);
      const addPosting = db.prepare('INSERT INTO postings (event_sequence,posting_index,account,asset,amount) VALUES (?,?,?,?,?)');
      const readBalance = db.prepare('SELECT amount FROM balances WHERE mandate_id = ? AND account = ? AND asset = ?');
      const writeBalance = db.prepare(`INSERT INTO balances (mandate_id,account,asset,amount) VALUES (?,?,?,?)
        ON CONFLICT(mandate_id,account,asset) DO UPDATE SET amount = excluded.amount`);
      normalized.postings.forEach((posting, index) => {
        addPosting.run(sequence, index, posting.account, posting.asset, posting.amount);
        const previousAmount = readBalance.get(mandateId, posting.account, posting.asset)?.amount || '0';
        writeBalance.run(mandateId, posting.account, posting.asset, addDecimals(previousAmount, posting.amount));
      });
      return receipt(db.prepare('SELECT * FROM events WHERE sequence = ?').get(sequence));
    });
  }
  function balances(mandateId) {
    ensureOpen();
    return db.prepare('SELECT account,asset,amount FROM balances WHERE mandate_id = ? ORDER BY account,asset').all(mandateId);
  }
  function verify(mandateId) {
    return transaction(() => {
      const registration = readMandate(mandateId);
      validateRelease(registration.release); validateMandate(registration.mandate, registration);
      const row = db.prepare('SELECT * FROM mandates WHERE mandate_id = ?').get(mandateId);
      assert(contentDigest({ customer_id: registration.customer_id, release: registration.release,
        mandate: registration.mandate, catalog: registration.catalog }) === row.registration_digest, 'Mandate registration digest is corrupt');
      assert(accountKey(registration.mandate.account) === row.account_key && canonicalize(registration.mandate.account) === row.account_json, 'Mandate account projection is corrupt');
      const decisions = db.prepare('SELECT * FROM decisions WHERE mandate_id = ? ORDER BY rowid').all(mandateId);
      let replayedControl = createControlState(registration.mandate);
      for (const decision of decisions) {
        assert(contentDigest(JSON.parse(decision.decision_json)) === decision.digest, 'Decision receipt digest is corrupt');
        assert(registration.controlState.decisions[decision.decision_id]?.digest === decision.digest, 'Decision control receipt is corrupt');
        const replayed = acceptPureDecision({ state: replayedControl, decision: JSON.parse(decision.decision_json),
          inputBundle: JSON.parse(decision.input_json), release: registration.release, mandate: registration.mandate,
          catalog: registration.catalog, validateEconomicPolicy: () => ({ valid: true, reasons: [] }) });
        assert(canonicalize(replayed) === decision.result_json, 'Stored decision result differs from deterministic control replay');
        replayedControl = replayed.state;
      }
      assert(decisions.length === registration.controlState.control_revision, 'Decision receipt count differs from control state');
      const expectedControl = decisions.length ? JSON.parse(decisions.at(-1).result_json).state : createControlState(registration.mandate);
      assert(canonicalize(expectedControl) === row.control_json, 'Control state differs from its committed decision receipt');
      const projected = new Map();
      let previousHash = contentDigest({ ledger_id: ledgerId, mandate_id: mandateId });
      const rows = db.prepare('SELECT * FROM events WHERE mandate_id = ? ORDER BY sequence').all(mandateId);
      for (const eventRow of rows) {
        const normalized = loadNormalized(mandateId, JSON.parse(eventRow.event_json));
        assert(normalized.event.event_id === eventRow.event_id && normalized.source_key === eventRow.source_key
          && normalized.economic_ref === eventRow.economic_ref && normalized.digest === eventRow.digest, 'Economic receipt identity or digest is corrupt');
        const posted = db.prepare('SELECT account,asset,amount FROM postings WHERE event_sequence = ? ORDER BY posting_index').all(eventRow.sequence);
        assert(canonicalize(posted) === canonicalize(normalized.postings), 'Economic postings differ from their immutable receipt');
        assert(eventRow.previous_hash === previousHash, 'Economic receipt chain is corrupt');
        const hash = contentDigest({ mandate_id: mandateId, event: normalized.event, postings: posted, recorded_at: eventRow.recorded_at, previous_hash: previousHash });
        assert(hash === eventRow.receipt_hash, 'Economic receipt hash is corrupt');
        previousHash = hash;
        for (const posting of posted) {
          const key = canonicalize([posting.account, posting.asset]);
          const value = projected.get(key) || { account: posting.account, asset: posting.asset, amount: '0' };
          value.amount = addDecimals(value.amount, posting.amount); projected.set(key, value);
        }
      }
      const expected = [...projected.values()].sort((a, b) => a.account < b.account ? -1 : a.account > b.account ? 1 : a.asset < b.asset ? -1 : a.asset > b.asset ? 1 : 0);
      assert(canonicalize(expected) === canonicalize(balances(mandateId)), 'Materialized account balances are corrupt');
      const aliases = db.prepare(`SELECT a.*,e.mandate_id AS original_mandate,e.digest AS original_digest
        FROM event_aliases a JOIN events e ON e.sequence = a.event_sequence WHERE a.mandate_id = ?`).all(mandateId);
      for (const alias of aliases) assert(alias.mandate_id === alias.original_mandate && alias.digest === alias.original_digest, 'Economic alias receipt is corrupt');
      for (const event of rows) assert(aliases.some(alias => alias.event_id === event.event_id && alias.event_sequence === event.sequence), 'Economic receipt is missing its immutable alias');
      return { valid: true, mandate_id: mandateId, events: rows.length, decisions: decisions.length, receipt_hash: previousHash };
    });
  }
  function scope({ customer_id, mandate_id }) {
    identifier(customer_id, 'customer_id'); identifier(mandate_id, 'mandate_id');
    function readScoped() {
      ensureOpen();
      // The same failure for absent and differently-owned IDs avoids data
      // disclosure. Authentication remains the application's responsibility.
      assert(db.prepare('SELECT 1 FROM mandates WHERE customer_id = ? AND mandate_id = ?').get(customer_id, mandate_id), 'Mandate scope is unavailable');
      return readMandate(mandate_id);
    }
    readScoped();
    return Object.freeze({
      getMandate() { return readScoped().mandate; },
      getControlState() { return readScoped().controlState; },
      acceptDecision({ decision, inputBundle, validateEconomicPolicy }) {
        // Pin caller objects before entering a callback-enabled transaction.
        decision = clone(decision); inputBundle = clone(inputBundle);
        return transaction(() => {
          const registration = readScoped();
          verify(mandate_id);
          assert(decision.mandate_id === mandate_id && inputBundle.mandate_id === mandate_id, 'Decision or input belongs to another mandate scope');
          const prior = db.prepare('SELECT * FROM decisions WHERE mandate_id = ? AND decision_id = ?').get(mandate_id, decision.decision_id);
          if (prior) {
            assert(prior.digest === contentDigest(decision) && prior.input_json === canonicalize(inputBundle), 'Decision ID reused with different content or input');
            return { ...JSON.parse(prior.result_json), status: 'replayed' };
          }
          const guardedPolicy = typeof validateEconomicPolicy === 'function' ? (...args) => {
            validatingPolicy = true;
            try { return validateEconomicPolicy(...args); } finally { validatingPolicy = false; }
          } : validateEconomicPolicy;
          const result = acceptPureDecision({ state: registration.controlState, decision, inputBundle,
            release: registration.release, mandate: registration.mandate, catalog: registration.catalog, validateEconomicPolicy: guardedPolicy });
          db.prepare('INSERT INTO decisions (mandate_id,decision_id,digest,input_json,decision_json,result_json,recorded_at) VALUES (?,?,?,?,?,?,?)')
            .run(mandate_id, decision.decision_id, contentDigest(decision), canonicalize(inputBundle), canonicalize(decision), canonicalize(result), now());
          const changed = db.prepare('UPDATE mandates SET control_json = ? WHERE mandate_id = ? AND control_json = ?')
            .run(canonicalize(result.state), mandate_id, canonicalize(registration.controlState));
          assert(changed.changes === 1, 'Control state changed during decision validation');
          return clone(result);
        });
      },
      appendEvent(event) { readScoped(); return appendEvent(mandate_id, event); },
      listEvents({ after_sequence = 0, limit = 1000 } = {}) {
        readScoped();
        assert(Number.isSafeInteger(after_sequence) && after_sequence >= 0 && Number.isSafeInteger(limit) && limit > 0 && limit <= 1000, 'Invalid event page');
        return db.prepare('SELECT * FROM events WHERE mandate_id = ? AND sequence > ? ORDER BY sequence LIMIT ?').all(mandate_id, after_sequence, limit).map(row => receipt(row));
      },
      getBalances() { readScoped(); return balances(mandate_id); },
      verify() { readScoped(); return verify(mandate_id); },
    });
  }

  // This is explicitly a trusted platform composition API, NOT a sandbox or an
  // authentication boundary. Never give the store itself to Strategy runtimes.
  const privileged = Object.freeze({ db, transaction, readMandate, now, appendEvent, verify });
  try {
    for (const row of db.prepare('SELECT mandate_id FROM mandates').all()) verify(row.mandate_id);
  } catch (error) { db.close(); closed = true; throw error; }
  return Object.freeze({ filename, ledgerId, registerMandate, scope,
    privileged() { ensureOpen(); return privileged; },
    close() { if (!closed) { db.close(); closed = true; } },
  });
}

module.exports = { openLedger, accountKey, APPLICATION_ID, SCHEMA_VERSION };
