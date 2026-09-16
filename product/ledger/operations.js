'use strict';

// Host-side, offline execution foundation. These capabilities belong to a trusted
// gateway, never to a Strategy sandbox. A real venue adapter still needs nonce,
// authorization, margin, instrument and signature integration before live use.
const { canonicalize, contentDigest } = require('../../strategy/canonical');
const { compareDecimals: cmp, addDecimals: add, subtractDecimals: sub, multiplyDecimals: mul,
  normalizeDecimal } = require('../../strategy/decimal');

const ACTIONS = Object.freeze({ buy_put: 'buy', sell_put: 'sell', sell_call: 'sell', buyback_call: 'buy' });
const TERMINAL = new Set(['filled', 'cancelled', 'rejected', 'not_accepted']);
const ID = /^[A-Za-z0-9][A-Za-z0-9._:/@-]{0,199}$/;
function fail(message) { throw new Error(message); }
function identifier(value, label) { if (typeof value !== 'string' || !ID.test(value)) fail(`Invalid ${label}`); return value; }
function exact(value, keys, label) {
  canonicalize(value);
  if (!value || Array.isArray(value) || typeof value !== 'object') fail(`Invalid ${label}`);
  if (Object.keys(value).some(key => !keys.includes(key))) fail(`Unknown ${label} field`);
}
function decimal(value, label, positive = false) {
  const result = normalizeDecimal(value);
  if (cmp(result, '0') < 0 || (positive && result === '0')) fail(`Invalid ${label}`);
  return result;
}
function copy(value) { return JSON.parse(canonicalize(value)); }
function snapshot(value) {
  const result = copy(value);
  function freeze(item) { if (item && typeof item === 'object') { Object.values(item).forEach(freeze); Object.freeze(item); } }
  freeze(result);
  return result;
}

function initialize(platform) {
  if (!platform || !platform.db || typeof platform.transaction !== 'function' || typeof platform.now !== 'function') fail('Trusted ledger platform required');
  platform.db.exec(`
    CREATE TABLE IF NOT EXISTS operation_leases (
      mandate_id TEXT PRIMARY KEY REFERENCES mandates(mandate_id),
      worker_id TEXT NOT NULL, fence INTEGER NOT NULL, expires_ms INTEGER NOT NULL,
      clock_ms INTEGER NOT NULL, next_nonce TEXT NOT NULL DEFAULT '0'
    );
    CREATE TABLE IF NOT EXISTS operation_queue (
      mandate_id TEXT NOT NULL REFERENCES mandates(mandate_id), operation_id TEXT NOT NULL,
      operation_json TEXT NOT NULL, digest TEXT NOT NULL, status TEXT NOT NULL,
      created_at TEXT NOT NULL, PRIMARY KEY(mandate_id, operation_id)
    );
    CREATE TABLE IF NOT EXISTS operation_attempts (
      mandate_id TEXT NOT NULL, operation_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
      nonce TEXT NOT NULL, fence INTEGER NOT NULL, payload_json TEXT NOT NULL, payload_digest TEXT NOT NULL,
      stage TEXT NOT NULL, order_id TEXT, venue_status TEXT,
      cumulative_quantity TEXT NOT NULL DEFAULT '0', accounted_quantity TEXT NOT NULL DEFAULT '0',
      accounted_fees TEXT NOT NULL DEFAULT '0', resolved INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY(mandate_id, operation_id), UNIQUE(mandate_id, nonce), UNIQUE(mandate_id, attempt_id),
      FOREIGN KEY(mandate_id, operation_id) REFERENCES operation_queue(mandate_id, operation_id)
    );
    CREATE TABLE IF NOT EXISTS operation_reservations (
      mandate_id TEXT NOT NULL, operation_id TEXT NOT NULL, asset TEXT NOT NULL,
      original_amount TEXT NOT NULL, remaining_amount TEXT NOT NULL,
      PRIMARY KEY(mandate_id, operation_id, asset),
      FOREIGN KEY(mandate_id, operation_id) REFERENCES operation_queue(mandate_id, operation_id)
    );
    CREATE TABLE IF NOT EXISTS operation_fills (
      mandate_id TEXT NOT NULL, fill_id TEXT NOT NULL, operation_id TEXT NOT NULL,
      digest TEXT NOT NULL, event_json TEXT NOT NULL, PRIMARY KEY(mandate_id, fill_id),
      FOREIGN KEY(mandate_id, operation_id) REFERENCES operation_queue(mandate_id, operation_id)
    );
    CREATE TABLE IF NOT EXISTS operation_evidence (
      mandate_id TEXT NOT NULL, evidence_id TEXT NOT NULL, operation_id TEXT NOT NULL,
      digest TEXT NOT NULL, evidence_json TEXT NOT NULL, PRIMARY KEY(mandate_id, evidence_id)
    );
    CREATE TABLE IF NOT EXISTS operation_journal (
      sequence INTEGER PRIMARY KEY, mandate_id TEXT NOT NULL, operation_id TEXT NOT NULL,
      stage TEXT NOT NULL, recorded_at TEXT NOT NULL
    );
  `);
  return platform;
}

function authorize(platform, scope) {
  exact(scope, ['customer_id', 'mandate_id'], 'scope');
  identifier(scope.customer_id, 'customer_id'); identifier(scope.mandate_id, 'mandate_id');
  const record = platform.readMandate(scope.mandate_id);
  if (!record || record.customer_id !== scope.customer_id) fail('Mandate scope denied');
  return record;
}
function instant(platform) {
  const text = platform.now();
  const ms = Date.parse(text);
  if (typeof text !== 'string' || !Number.isSafeInteger(ms) || new Date(ms).toISOString() !== text) fail('Invalid host clock');
  return { text, ms };
}
function journal(platform, mandateId, operationId, stage) {
  platform.db.prepare('INSERT INTO operation_journal(mandate_id,operation_id,stage,recorded_at) VALUES (?,?,?,?)')
    .run(mandateId, operationId, stage, instant(platform).text);
}
function rowFor(platform, scope, operationId) {
  authorize(platform, scope); identifier(operationId, 'operation_id');
  const row = platform.db.prepare('SELECT * FROM operation_queue WHERE mandate_id=? AND operation_id=?').get(scope.mandate_id, operationId);
  if (!row) fail('Unknown scoped operation');
  return row;
}
function inspect(platform, scope, operationId) {
  const row = rowFor(platform, scope, operationId);
  const attempt = platform.db.prepare('SELECT * FROM operation_attempts WHERE mandate_id=? AND operation_id=?').get(scope.mandate_id, operationId);
  return snapshot({ operation: JSON.parse(row.operation_json), status: row.status,
    attempt: attempt ? { attempt_id: attempt.attempt_id, nonce: attempt.nonce, payload_digest: attempt.payload_digest,
      stage: attempt.stage, order_id: attempt.order_id, venue_status: attempt.venue_status,
      cumulative_quantity: attempt.cumulative_quantity, accounted_quantity: attempt.accounted_quantity,
      accounted_fees: attempt.accounted_fees, resolved: attempt.resolved === 1 } : null,
    reservations: platform.db.prepare('SELECT asset,original_amount,remaining_amount FROM operation_reservations WHERE mandate_id=? AND operation_id=? ORDER BY asset').all(scope.mandate_id, operationId) });
}
function unresolved(platform, mandateId) {
  return platform.db.prepare('SELECT operation_id FROM operation_attempts WHERE mandate_id=? AND resolved=0 LIMIT 1').get(mandateId);
}

function verifyOperations(platform, mandateId) {
  platform.verify(mandateId);
  const operations = platform.db.prepare('SELECT * FROM operation_queue WHERE mandate_id=?').all(mandateId);
  for (const row of operations) {
    const operation = JSON.parse(row.operation_json);
    if (operation.operation_id !== row.operation_id || contentDigest(operation) !== row.digest) fail('Corrupt immutable operation payload');
    const attempt = platform.db.prepare('SELECT * FROM operation_attempts WHERE mandate_id=? AND operation_id=?').get(mandateId, row.operation_id);
    const fills = platform.db.prepare('SELECT * FROM operation_fills WHERE mandate_id=? AND operation_id=?').all(mandateId, row.operation_id);
    const evidences = platform.db.prepare('SELECT * FROM operation_evidence WHERE mandate_id=? AND operation_id=? ORDER BY rowid').all(mandateId, row.operation_id);
    const lastStage = platform.db.prepare('SELECT stage FROM operation_journal WHERE mandate_id=? AND operation_id=? ORDER BY sequence DESC LIMIT 1').get(mandateId, row.operation_id)?.stage;
    if (lastStage !== (row.status === 'submission_unknown' && attempt?.stage === 'sending' ? 'sending' : row.status)) fail('Corrupt operation lifecycle journal');
    let quantity = '0', fees = '0', consumed = '0';
    for (const fillRow of fills) {
      const event = JSON.parse(fillRow.event_json);
      const { event_id: ignored, ...source } = event;
      if (contentDigest(source) !== fillRow.digest || event.payload.fill_id !== fillRow.fill_id || event.payload.order_attempt_id !== attempt?.attempt_id) fail('Corrupt accounted fill projection');
      const receipt = platform.db.prepare('SELECT event_json FROM events WHERE mandate_id=? AND economic_ref=?').get(mandateId, `fill:${fillRow.fill_id}`);
      const normalized = require('./events').normalizeEvent(event);
      if (!receipt || require('./events').normalizeEvent(JSON.parse(receipt.event_json)).digest !== normalized.digest) fail('Accounted fill lacks matching economic receipt');
      quantity = add(quantity, event.payload.quantity);
      const eventFees = event.payload.fees.reduce((sum, fee) => add(sum, fee.amount), '0');
      fees = add(fees, eventFees); consumed = add(consumed, add(event.payload.side === 'buy' ? event.payload.gross_premium : '0', eventFees));
    }
    for (const evidenceRow of evidences) if (contentDigest(JSON.parse(evidenceRow.evidence_json)) !== evidenceRow.digest) fail('Corrupt immutable recovery evidence');
    if (!attempt) {
      if (!['queued', 'cancelled_before_submission'].includes(row.status) || fills.length || evidences.length) fail('Corrupt operation attempt projection');
    } else {
      const payload = JSON.parse(attempt.payload_json);
      if (contentDigest(payload) !== attempt.payload_digest || payload.operation_id !== row.operation_id
        || payload.mandate_id !== mandateId || payload.attempt_id !== attempt.attempt_id || payload.nonce !== attempt.nonce
        || payload.action !== operation.action || payload.instrument !== operation.instrument_ref
        || payload.quantity !== operation.quantity || payload.limit_price !== operation.limit_price || payload.max_fee_usdc !== operation.max_fee_usdc
        || canonicalize(payload.account) !== canonicalize(platform.readMandate(mandateId).mandate.account)) fail('Corrupt prepared operation identity');
      if (quantity !== attempt.accounted_quantity || fees !== attempt.accounted_fees) fail('Corrupt accounted quantity or fees');
      const latest = evidences.length ? JSON.parse(evidences.at(-1).evidence_json) : null;
      const shouldResolve = latest && TERMINAL.has(latest.status) && latest.complete_fill_set && cmp(quantity, latest.cumulative_filled_quantity) === 0;
      if (!!attempt.resolved !== !!shouldResolve || (attempt.resolved && (row.status !== 'accounted' || attempt.stage !== 'accounted'))) fail('Corrupt attempt resolution flag');
      if (latest && (attempt.venue_status !== latest.status || cmp(attempt.cumulative_quantity, latest.cumulative_filled_quantity) !== 0 || attempt.order_id !== latest.order_id)) fail('Corrupt venue evidence projection');
      if (!latest && (attempt.cumulative_quantity !== '0' || attempt.venue_status !== null || fills.length)) fail('Corrupt unevidenced attempt state');
      if (!latest && !((row.status === 'submission_unknown' && ['signing', 'sending'].includes(attempt.stage))
        || (row.status === 'acknowledged' && attempt.stage === 'acknowledged' && attempt.order_id))) fail('Corrupt submission phase');
      if (latest && !attempt.resolved && (row.status !== 'recovery_required' || attempt.stage !== 'recovery_required')) fail('Corrupt unresolved phase');
    }
    const reservations = platform.db.prepare('SELECT * FROM operation_reservations WHERE mandate_id=? AND operation_id=?').all(mandateId, row.operation_id);
    if (reservations.length !== operation.reservations.length) fail('Corrupt reservation count');
    for (const reserved of reservations) {
      const original = operation.reservations.find(item => item.asset === reserved.asset);
      const expected = row.status === 'cancelled_before_submission' || attempt?.resolved ? '0' : sub(original?.amount || '0', consumed);
      if (!original || cmp(reserved.original_amount, original.amount) !== 0 || cmp(reserved.remaining_amount, expected) !== 0 || cmp(expected, '0') < 0) fail('Corrupt reservation projection');
    }
  }
}

function validateOperation(platform, scope, operation) {
  exact(operation, ['operation_id', 'intent_id', 'intent_revision', 'mandate_revision', 'action', 'instrument_ref',
    'quantity', 'limit_price', 'max_fee_usdc', 'reservations'], 'operation');
  for (const key of ['operation_id', 'intent_id', 'instrument_ref']) identifier(operation[key], key);
  for (const key of ['intent_revision', 'mandate_revision']) if (!Number.isSafeInteger(operation[key]) || operation[key] < 1) fail(`Invalid ${key}`);
  const { mandate, controlState } = authorize(platform, scope);
  platform.verify(scope.mandate_id);
  if (operation.mandate_revision !== mandate.mandate_revision) fail('Stale mandate revision');
  if (mandate.status !== 'active') fail('Mandate is not active; exit dispatch needs a separate accepted policy');
  if (!Object.hasOwn(ACTIONS, operation.action) || !mandate.authority.actions.includes(operation.action)) fail('Action not authorized');
  if (!mandate.authority.instrument_refs.includes(operation.instrument_ref)) fail('Instrument not authorized');
  const record = controlState.intents[operation.intent_id];
  if (!record || record.status !== 'active' || record.intent.intent_revision !== operation.intent_revision) fail('Intent is not an active accepted revision');
  const intent = record.intent;
  if (intent.action !== operation.action || intent.instrument_ref !== operation.instrument_ref) fail('Operation differs from accepted intent');
  const now = instant(platform).ms;
  if (now < Date.parse(intent.active_from) || now >= Date.parse(intent.expires_at)) fail('Intent is outside its active window');
  // Instrument metadata comes from a persisted, accepted input, not the worker.
  const acceptedInput = platform.db.prepare('SELECT input_json,decision_json FROM decisions WHERE mandate_id=? ORDER BY rowid DESC').all(scope.mandate_id)
    .find(row => JSON.parse(row.decision_json).operations.some(item => item.op === 'upsert_intent'
      && item.intent.intent_id === operation.intent_id && item.intent.intent_revision === operation.intent_revision));
  const instrument = acceptedInput && JSON.parse(acceptedInput.input_json).instruments[operation.instrument_ref];
  if (!instrument || instrument.quote_asset !== 'USDC' || instrument.base_asset !== 'ETH'
    || instrument.kind !== (operation.action.includes('put') ? 'put' : 'call')) fail('Unsupported catalog instrument');
  const quantity = decimal(operation.quantity, 'quantity', true);
  const limit = decimal(operation.limit_price, 'limit_price', true);
  const fees = decimal(operation.max_fee_usdc, 'max_fee_usdc');
  if (cmp(quantity, intent.quantity.max_total) > 0 || cmp(fees, intent.order.max_total_fees.value) > 0
    || (ACTIONS[operation.action] === 'buy' ? cmp(limit, intent.order.limit_price.value) > 0 : cmp(limit, intent.order.limit_price.value) < 0)) fail('Operation exceeds accepted intent bounds');
  if (!Array.isArray(operation.reservations) || operation.reservations.length !== 1) fail('One explicit USDC reservation required');
  const reservation = operation.reservations[0];
  exact(reservation, ['asset', 'amount'], 'reservation');
  if (reservation.asset !== 'USDC') fail('Option reservation must use USDC');
  const reserved = decimal(reservation.amount, 'reservation amount');
  const minimum = ACTIONS[operation.action] === 'buy' ? add(mul(quantity, limit), fees) : fees;
  if (cmp(reserved, minimum) < 0) fail('Reservation does not cover bounded premium and fees');
  let committedQuantity = quantity, committedFees = fees, committedOutlay = minimum, attempts = 1;
  const previous = platform.db.prepare(`SELECT q.operation_id,q.operation_json,q.status,a.resolved,a.accounted_quantity,a.accounted_fees
    FROM operation_queue q LEFT JOIN operation_attempts a USING(mandate_id,operation_id)
    WHERE q.mandate_id=? AND q.operation_id<>? AND q.status<>'cancelled_before_submission'`).all(scope.mandate_id, operation.operation_id);
  for (const row of previous) {
    const prior = JSON.parse(row.operation_json);
    if (prior.intent_id !== operation.intent_id || prior.intent_revision !== operation.intent_revision) continue;
    attempts += 1;
    if (row.resolved) {
      committedQuantity = add(committedQuantity, row.accounted_quantity);
      committedFees = add(committedFees, row.accounted_fees);
      for (const fill of platform.db.prepare('SELECT event_json FROM operation_fills WHERE mandate_id=? AND operation_id=?').all(scope.mandate_id, row.operation_id)) {
        const payload = JSON.parse(fill.event_json).payload;
        committedOutlay = add(committedOutlay, add(payload.side === 'buy' ? payload.gross_premium : '0', payload.fees.reduce((sum, fee) => add(sum, fee.amount), '0')));
      }
    } else {
      committedQuantity = add(committedQuantity, prior.quantity); committedFees = add(committedFees, prior.max_fee_usdc);
      committedOutlay = add(committedOutlay, add(ACTIONS[prior.action] === 'buy' ? mul(prior.quantity, prior.limit_price) : '0', prior.max_fee_usdc));
    }
  }
  if (attempts > intent.max_attempts || cmp(committedQuantity, intent.quantity.max_total) > 0
    || cmp(committedFees, intent.order.max_total_fees.value) > 0
    || (intent.order.max_total_outlay && cmp(committedOutlay, intent.order.max_total_outlay.value) > 0)) fail('Intent cumulative attempt, quantity, fee or outlay bound exceeded');
}

function assertReservedCash(platform, mandateId) {
  if (platform.db.prepare("SELECT amount FROM balances WHERE mandate_id=? AND account='assets:cash'").all(mandateId)
    .some(row => cmp(row.amount, '0') < 0)) fail('Negative account cash requires reconciliation before dispatch');
  const rows = platform.db.prepare('SELECT asset,remaining_amount FROM operation_reservations WHERE mandate_id=?').all(mandateId);
  const byAsset = new Map();
  for (const row of rows) byAsset.set(row.asset, add(byAsset.get(row.asset) || '0', row.remaining_amount));
  for (const [asset, amount] of byAsset) {
    const cash = platform.db.prepare("SELECT amount FROM balances WHERE mandate_id=? AND account='assets:cash' AND asset=?").get(mandateId, asset)?.amount || '0';
    if (cmp(cash, amount) < 0) fail('Reserved cash is no longer funded; account reconciliation required');
  }
}

function createOperations(platform) {
  initialize(platform);
  return Object.freeze({ forScope(inputScope) {
    const scope = snapshot(inputScope); authorize(platform, scope); platform.transaction(() => verifyOperations(platform, scope.mandate_id));
    return Object.freeze({
      queue(input) {
        const operation = copy(input);
        return platform.transaction(() => {
          authorize(platform, scope);
          verifyOperations(platform, scope.mandate_id);
          const existing = platform.db.prepare('SELECT digest FROM operation_queue WHERE mandate_id=? AND operation_id=?').get(scope.mandate_id, operation.operation_id);
          const digest = contentDigest(operation);
          if (existing) { if (existing.digest !== digest) fail('Operation idempotency conflict'); return inspect(platform, scope, operation.operation_id); }
          validateOperation(platform, scope, operation);
          if (unresolved(platform, scope.mandate_id)) fail('Account recovery required before queueing new exposure');
          for (const reservation of operation.reservations) {
            const balance = platform.db.prepare("SELECT amount FROM balances WHERE mandate_id=? AND account='assets:cash' AND asset=?").get(scope.mandate_id, reservation.asset)?.amount || '0';
            const reserved = platform.db.prepare('SELECT remaining_amount FROM operation_reservations WHERE mandate_id=? AND asset=?').all(scope.mandate_id, reservation.asset).reduce((total, item) => add(total, item.remaining_amount), '0');
            if (cmp(sub(balance, reserved), reservation.amount) < 0) fail('Insufficient unreserved cash');
          }
          platform.db.prepare('INSERT INTO operation_queue VALUES (?,?,?,?,?,?)').run(scope.mandate_id, operation.operation_id, canonicalize(operation), digest, 'queued', instant(platform).text);
          const reserve = platform.db.prepare('INSERT INTO operation_reservations VALUES (?,?,?,?,?)');
          for (const item of operation.reservations) reserve.run(scope.mandate_id, operation.operation_id, item.asset, normalizeDecimal(item.amount), normalizeDecimal(item.amount));
          journal(platform, scope.mandate_id, operation.operation_id, 'queued');
          return inspect(platform, scope, operation.operation_id);
        });
      },
      get(operationId) { return platform.transaction(() => { authorize(platform, scope); verifyOperations(platform, scope.mandate_id); return inspect(platform, scope, operationId); }); },
      list() { return platform.transaction(() => { authorize(platform, scope); verifyOperations(platform, scope.mandate_id); return platform.db.prepare('SELECT operation_id FROM operation_queue WHERE mandate_id=? ORDER BY operation_id').all(scope.mandate_id).map(row => inspect(platform, scope, row.operation_id)); }); },
      reservations() { return platform.transaction(() => { authorize(platform, scope); verifyOperations(platform, scope.mandate_id); return snapshot(platform.db.prepare('SELECT operation_id,asset,original_amount,remaining_amount FROM operation_reservations WHERE mandate_id=? ORDER BY operation_id,asset').all(scope.mandate_id)); }); },
      cancelQueued(operationId) {
        return platform.transaction(() => {
          authorize(platform, scope); verifyOperations(platform, scope.mandate_id);
          const row = rowFor(platform, scope, operationId);
          if (row.status === 'cancelled_before_submission') return inspect(platform, scope, operationId);
          if (row.status !== 'queued') fail('Submitted operations require authoritative reconciliation');
          platform.db.prepare("UPDATE operation_queue SET status='cancelled_before_submission' WHERE mandate_id=? AND operation_id=?").run(scope.mandate_id, operationId);
          platform.db.prepare("UPDATE operation_reservations SET remaining_amount='0' WHERE mandate_id=? AND operation_id=?").run(scope.mandate_id, operationId);
          journal(platform, scope.mandate_id, operationId, 'cancelled_before_submission');
          return inspect(platform, scope, operationId);
        });
      },
    });
  } });
}

function leaseScope(lease) { return { customer_id: lease.customer_id, mandate_id: lease.mandate_id }; }
function guard(platform, lease) {
  exact(lease, ['customer_id', 'mandate_id', 'worker_id', 'fence_token', 'expires_at'], 'lease');
  authorize(platform, leaseScope(lease));
  verifyOperations(platform, lease.mandate_id);
  const current = platform.db.prepare('SELECT * FROM operation_leases WHERE mandate_id=?').get(lease.mandate_id);
  const { ms } = instant(platform);
  if (!current || ms < current.clock_ms) fail('Host clock moved backwards or lease missing');
  if (current.worker_id !== lease.worker_id || current.fence !== lease.fence_token || ms >= current.expires_ms) fail('Stale or expired gateway fence');
  platform.db.prepare('UPDATE operation_leases SET clock_ms=? WHERE mandate_id=?').run(ms, lease.mandate_id);
  return current;
}

function createGateway(platform, { mode, sign, send, verifyEvidence } = {}) {
  if (mode !== 'offline_simulation') fail('Only explicit offline_simulation gateway mode is implemented; live dispatch is unavailable');
  if (typeof sign !== 'function' || typeof send !== 'function' || typeof verifyEvidence !== 'function') fail('Gateway requires injected signer, transport and authoritative evidence verifier');
  initialize(platform);
  function check(lease) { return platform.transaction(() => guard(platform, lease)); }
  return Object.freeze({
    mode: 'offline_simulation',
    acquire(inputScope, request) {
      const scope = snapshot(inputScope);
      exact(request, ['worker_id', 'ttl_ms'], 'lease request'); identifier(request.worker_id, 'worker_id');
      if (!Number.isSafeInteger(request.ttl_ms) || request.ttl_ms < 1 || request.ttl_ms > 300000) fail('Invalid lease duration');
      return platform.transaction(() => {
        authorize(platform, scope);
        verifyOperations(platform, scope.mandate_id);
        const now = instant(platform);
        const old = platform.db.prepare('SELECT * FROM operation_leases WHERE mandate_id=?').get(scope.mandate_id);
        if (old && now.ms < old.clock_ms) fail('Host clock moved backwards');
        if (old && now.ms < old.expires_ms) fail('Account writer lease is held');
        const fence = (old?.fence || 0) + 1;
        if (!Number.isSafeInteger(fence) || !Number.isSafeInteger(now.ms + request.ttl_ms)) fail('Lease counter capacity exceeded');
        platform.db.prepare(`INSERT INTO operation_leases(mandate_id,worker_id,fence,expires_ms,clock_ms) VALUES (?,?,?,?,?)
          ON CONFLICT(mandate_id) DO UPDATE SET worker_id=excluded.worker_id,fence=excluded.fence,expires_ms=excluded.expires_ms,clock_ms=excluded.clock_ms`)
          .run(scope.mandate_id, request.worker_id, fence, now.ms + request.ttl_ms, now.ms);
        return snapshot({ ...scope, worker_id: request.worker_id, fence_token: fence, expires_at: new Date(now.ms + request.ttl_ms).toISOString() });
      });
    },
    renew(lease, ttlMs) {
      if (!Number.isSafeInteger(ttlMs) || ttlMs < 1 || ttlMs > 300000) fail('Invalid lease duration');
      return platform.transaction(() => {
        guard(platform, lease); const now = instant(platform);
        const expiry = new Date(now.ms + ttlMs).toISOString();
        platform.db.prepare('UPDATE operation_leases SET expires_ms=? WHERE mandate_id=?').run(now.ms + ttlMs, lease.mandate_id);
        return snapshot({ ...lease, expires_at: expiry });
      });
    },
    async submit(lease, operationId) {
      // Caller-owned lease clones must not change account/fence across awaits.
      lease = snapshot(lease);
      const scope = leaseScope(lease);
      const prepared = platform.transaction(() => {
        guard(platform, lease);
        const row = rowFor(platform, scope, operationId);
        if (row.status !== 'queued') fail('Operation already attempted or cancelled; recover instead of resubmitting');
        validateOperation(platform, scope, JSON.parse(row.operation_json));
        assertReservedCash(platform, scope.mandate_id);
        if (unresolved(platform, scope.mandate_id)) fail('Account recovery required before submission');
        const old = platform.db.prepare('SELECT next_nonce FROM operation_leases WHERE mandate_id=?').get(scope.mandate_id);
        const nonce = (BigInt(old.next_nonce) + 1n).toString();
        platform.db.prepare('UPDATE operation_leases SET next_nonce=? WHERE mandate_id=?').run(nonce, scope.mandate_id);
        const operation = JSON.parse(row.operation_json);
        const payload = { account: authorize(platform, scope).mandate.account, attempt_id: `${operationId}/attempt/1`, nonce,
          operation_id: operationId, mandate_id: scope.mandate_id, mandate_revision: operation.mandate_revision,
          action: operation.action, instrument: operation.instrument_ref, side: ACTIONS[operation.action],
          quantity: operation.quantity, limit_price: operation.limit_price, max_fee_usdc: operation.max_fee_usdc,
          reduce_only: ['sell_put', 'buyback_call'].includes(operation.action) };
        payload.time_in_force = authorize(platform, scope).controlState.intents[operation.intent_id].intent.order.time_in_force;
        payload.expires_at = authorize(platform, scope).controlState.intents[operation.intent_id].intent.expires_at;
        const digest = contentDigest(payload);
        journal(platform, scope.mandate_id, operationId, 'prepared');
        platform.db.prepare(`INSERT INTO operation_attempts(mandate_id,operation_id,attempt_id,nonce,fence,payload_json,payload_digest,stage)
          VALUES (?,?,?,?,?,?,?,'signing')`).run(scope.mandate_id, operationId, payload.attempt_id, nonce, lease.fence_token, canonicalize(payload), digest);
        platform.db.prepare("UPDATE operation_queue SET status='submission_unknown' WHERE mandate_id=? AND operation_id=?").run(scope.mandate_id, operationId);
        journal(platform, scope.mandate_id, operationId, 'submission_unknown');
        return snapshot({ payload, payload_digest: digest });
      });
      // The durable unknown claim already blocks reentrancy and lease takeovers.
      // Never return signed bytes to the caller or persist them in public state.
      check(lease);
      const signed = await sign(prepared.payload, snapshot({ payload_digest: prepared.payload_digest, fence_token: lease.fence_token }));
      platform.transaction(() => {
        guard(platform, lease);
        validateOperation(platform, scope, JSON.parse(rowFor(platform, scope, operationId).operation_json));
        assertReservedCash(platform, scope.mandate_id);
        const current = platform.db.prepare('SELECT stage,resolved FROM operation_attempts WHERE mandate_id=? AND operation_id=?').get(scope.mandate_id, operationId);
        if (!current || current.stage !== 'signing' || current.resolved) fail('Submission claim changed during signing');
        platform.db.prepare("UPDATE operation_attempts SET stage='sending' WHERE mandate_id=? AND operation_id=?").run(scope.mandate_id, operationId);
        journal(platform, scope.mandate_id, operationId, 'sending');
      });
      // No await occurs between the final fence guard and invoking transport.
      check(lease);
      const ack = await send(signed, prepared.payload, snapshot({ payload_digest: prepared.payload_digest, fence_token: lease.fence_token }));
      return platform.transaction(() => {
        guard(platform, lease);
        platform.verify(scope.mandate_id);
        exact(ack, ['order_id', 'attempt_id', 'nonce', 'payload_digest'], 'acknowledgement');
        identifier(ack.order_id, 'order_id');
        if (ack.attempt_id !== prepared.payload.attempt_id || ack.nonce !== prepared.payload.nonce || ack.payload_digest !== prepared.payload_digest) fail('Acknowledgement identity mismatch; recovery required');
        const current = platform.db.prepare('SELECT stage,resolved FROM operation_attempts WHERE mandate_id=? AND operation_id=?').get(scope.mandate_id, operationId);
        if (!current || current.stage !== 'sending' || current.resolved) fail('Submission claim changed during transport; recovery required');
        platform.db.prepare("UPDATE operation_attempts SET stage='acknowledged',order_id=? WHERE mandate_id=? AND operation_id=?").run(ack.order_id, scope.mandate_id, operationId);
        platform.db.prepare("UPDATE operation_queue SET status='acknowledged' WHERE mandate_id=? AND operation_id=?").run(scope.mandate_id, operationId);
        journal(platform, scope.mandate_id, operationId, 'acknowledged');
        return inspect(platform, scope, operationId);
      });
    },
    async recover(lease, operationId, inputEvidence) {
      lease = snapshot(lease);
      const scope = leaseScope(lease); check(lease);
      const evidence = snapshot(inputEvidence);
      const before = inspect(platform, scope, operationId);
      if (!before.attempt) fail('Operation has no submission attempt');
      // The injected trusted adapter authenticates authoritative source records.
      // A caller-supplied boolean, a message string, or an ACK is never proof.
      if (await verifyEvidence(evidence, snapshot({ scope, operation: before.operation, attempt: before.attempt })) !== true) fail('Authoritative recovery evidence not verified');
      return platform.transaction(() => {
        guard(platform, lease);
        const row = rowFor(platform, scope, operationId);
        const operation = JSON.parse(row.operation_json);
        const attempt = platform.db.prepare('SELECT * FROM operation_attempts WHERE mandate_id=? AND operation_id=?').get(scope.mandate_id, operationId);
        exact(evidence, ['evidence_id', 'account', 'attempt_id', 'nonce', 'payload_digest', 'order_id', 'status',
          'cumulative_filled_quantity', 'fills', 'complete_fill_set', 'not_accepted_proof'], 'recovery evidence');
        identifier(evidence.evidence_id, 'evidence_id');
        if (canonicalize(evidence.account) !== canonicalize(authorize(platform, scope).mandate.account)
          || evidence.attempt_id !== attempt.attempt_id || evidence.nonce !== attempt.nonce || evidence.payload_digest !== attempt.payload_digest) fail('Recovery identity mismatch');
        if (!['open', ...TERMINAL].includes(evidence.status)) fail('Unsupported recovery status');
        if (typeof evidence.complete_fill_set !== 'boolean' || !Array.isArray(evidence.fills) || evidence.fills.length > 10000) fail('Invalid fill evidence completeness');
        const cumulative = decimal(evidence.cumulative_filled_quantity, 'cumulative fill quantity');
        if (cmp(cumulative, operation.quantity) > 0) fail('Cumulative fill exceeds operation');
        const evidenceDigest = contentDigest(evidence);
        const previous = platform.db.prepare('SELECT digest,operation_id FROM operation_evidence WHERE mandate_id=? AND evidence_id=?').get(scope.mandate_id, evidence.evidence_id);
        if (previous) {
          if (previous.operation_id !== operationId || previous.digest !== evidenceDigest) fail('Recovery evidence idempotency conflict');
          return inspect(platform, scope, operationId);
        }
        if (attempt.resolved) fail('Resolved attempt only accepts exact evidence replay');
        if (cmp(cumulative, attempt.cumulative_quantity) < 0) fail('Cumulative fill evidence regressed');
        if (TERMINAL.has(attempt.venue_status) && evidence.status !== attempt.venue_status) fail('Terminal venue state cannot change');
        if (['rejected', 'not_accepted'].includes(evidence.status)) {
          exact(evidence.not_accepted_proof, ['kind', 'evidence_ref'], 'not-accepted proof');
          if (!['nonce_invalidated', 'signature_expired', 'definitive_rejection'].includes(evidence.not_accepted_proof.kind)) fail('Proof must establish irreversible nonacceptance, not current absence');
          identifier(evidence.not_accepted_proof.evidence_ref, 'not-accepted proof reference');
          // A verifier must prove no delayed signed payload can still execute:
          // nonce invalidation, actual signature expiry or an immutable rejection
          // of that nonce/payload. A point-in-time "not found" is insufficient.
          // Post-ACK venue rejections are deliberately unsupported in this batch.
          if (evidence.order_id !== null || attempt.order_id !== null || cumulative !== '0' || evidence.fills.length || attempt.accounted_quantity !== '0' || !evidence.complete_fill_set) fail('Invalid not-accepted evidence');
        } else {
          identifier(evidence.order_id, 'order_id');
          if (attempt.order_id && evidence.order_id !== attempt.order_id) fail('Venue order identity mismatch');
          if (Object.hasOwn(evidence, 'not_accepted_proof')) fail('Unexpected rejection proof');
        }
        if (evidence.status === 'filled' && cmp(cumulative, operation.quantity) !== 0) fail('Filled terminal evidence must cover full order quantity');
        let accounted = attempt.accounted_quantity;
        let fees = attempt.accounted_fees;
        let consume = '0';
        const seenFills = new Set();
        for (const event of evidence.fills) {
          if (event.kind !== 'option_fill') fail('Recovery only imports option fills; settlement is independent');
          const fill = event.payload;
          if (!fill || fill.order_attempt_id !== attempt.attempt_id || fill.instrument !== operation.instrument_ref || fill.side !== ACTIONS[operation.action]
            || fill.quote_asset !== 'USDC') fail('Fill does not match attempted operation');
          identifier(fill.fill_id, 'fill_id');
          if (seenFills.has(fill.fill_id)) fail('Duplicate fill within evidence'); seenFills.add(fill.fill_id);
          const digestable = copy(event); delete digestable.event_id;
          const digest = contentDigest(digestable);
          const old = platform.db.prepare('SELECT digest,operation_id FROM operation_fills WHERE mandate_id=? AND fill_id=?').get(scope.mandate_id, fill.fill_id);
          if (old) { if (old.operation_id !== operationId || old.digest !== digest) fail('Fill idempotency conflict'); continue; }
          const quantity = decimal(fill.quantity, 'fill quantity', true);
          const premium = decimal(fill.gross_premium, 'fill gross premium');
          const bounded = mul(quantity, operation.limit_price);
          if ((fill.side === 'buy' && cmp(premium, bounded) > 0) || (fill.side === 'sell' && cmp(premium, bounded) < 0)) fail('Fill violates bounded limit price');
          if (!Array.isArray(fill.fees) || fill.fees.some(fee => fee.asset !== 'USDC')) fail('Unsupported fee asset in option recovery');
          const fillFees = fill.fees.reduce((total, fee) => add(total, decimal(fee.amount, 'fill fee')), '0');
          accounted = add(accounted, quantity); fees = add(fees, fillFees);
          if (cmp(accounted, cumulative) > 0 || cmp(fees, operation.max_fee_usdc) > 0) fail('Accounted fill exceeds declared cumulative quantity or fee bound');
          consume = add(consume, add(fill.side === 'buy' ? premium : '0', fillFees));
          platform.appendEvent(scope.mandate_id, event);
          platform.db.prepare('INSERT INTO operation_fills VALUES (?,?,?,?,?)').run(scope.mandate_id, fill.fill_id, operationId, digest, canonicalize(event));
        }
        if (evidence.complete_fill_set && cmp(accounted, cumulative) !== 0) fail('Complete fill evidence is missing economic events');
        const reservation = platform.db.prepare("SELECT remaining_amount FROM operation_reservations WHERE mandate_id=? AND operation_id=? AND asset='USDC'").get(scope.mandate_id, operationId);
        if (cmp(consume, reservation.remaining_amount) > 0) fail('Fill cost exceeds remaining reservation');
        const resolved = TERMINAL.has(evidence.status) && evidence.complete_fill_set && cmp(accounted, cumulative) === 0;
        platform.db.prepare("UPDATE operation_reservations SET remaining_amount=? WHERE mandate_id=? AND operation_id=? AND asset='USDC'")
          .run(resolved ? '0' : sub(reservation.remaining_amount, consume), scope.mandate_id, operationId);
        platform.db.prepare(`UPDATE operation_attempts SET order_id=?,venue_status=?,cumulative_quantity=?,accounted_quantity=?,accounted_fees=?,resolved=?,stage=?
          WHERE mandate_id=? AND operation_id=?`).run(evidence.order_id, evidence.status, cumulative, accounted, fees, resolved ? 1 : 0,
          resolved ? 'accounted' : 'recovery_required', scope.mandate_id, operationId);
        platform.db.prepare('UPDATE operation_queue SET status=? WHERE mandate_id=? AND operation_id=?').run(resolved ? 'accounted' : 'recovery_required', scope.mandate_id, operationId);
        platform.db.prepare('INSERT INTO operation_evidence VALUES (?,?,?,?,?)').run(scope.mandate_id, evidence.evidence_id, operationId, evidenceDigest, canonicalize(evidence));
        journal(platform, scope.mandate_id, operationId, resolved ? 'accounted' : 'recovery_required');
        return inspect(platform, scope, operationId);
      });
    },
  });
}

module.exports = { createOperations, createGateway };
