'use strict';

// Import-safe bookkeeping. No venue client, signing, or implicit database opening.
const { normalizeV2Trade, decimal } = require('./economic-events');
const { isDefinitiveOrderRejection } = require('./derive-errors');
const { splitNetProceeds, toUnits, fromUnits, SCALE, ALLOCATION_POLICY } = require('./spot-accumulation');
const TERMINAL = new Set(['filled', 'cancelled', 'expired', 'rejected']);
const FUNDING_POLICY = 'settled-internal-transfer-v1';
const SAMPLE_BUDGET_USDC = '5';
const account = value => {
  const text = String(value);
  if (!/^[1-9]\d*$/.test(text) || !Number.isSafeInteger(Number(text))) throw new Error('Invalid account identity');
  return text;
};
const timestamp = value => {
  if (value == null || value === '') throw new Error('Missing timestamp');
  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) throw new Error('Invalid timestamp');
  return new Date(ms).toISOString();
};
const parse = value => typeof value === 'string' ? JSON.parse(value) : value;
const identity = value => {
  if (typeof value !== 'string' || !value.trim()) throw new Error('Missing durable identity');
  return value;
};
const nonce = value => {
  const text = String(value ?? '');
  if (!/^\d+$/.test(text) || (typeof value === 'number' && !Number.isSafeInteger(value))
    || BigInt(text) > (1n << 64n) - 1n) throw new Error('Invalid request nonce');
  return BigInt(text).toString();
};
function configuration(input) {
  if (input.allocationPolicy != null && input.allocationPolicy !== ALLOCATION_POLICY) throw new Error('Unsupported accumulation allocation policy');
  if (input.fundingPolicy != null && input.fundingPolicy !== FUNDING_POLICY) throw new Error('Unsupported accumulation funding policy');
  if (!['mainnet', 'testnet'].includes(input.network) || !['v2', 'v3'].includes(input.apiVersion || 'v3')) throw new Error('Explicit venue network/version required');
  if (input.accountAddress != null && !/^0x[0-9a-f]{40}$/i.test(input.accountAddress)) throw new Error('Invalid owner address');
  return { network: input.network, apiVersion: input.apiVersion || 'v3', accountId: account(input.accountId),
    allocationPolicy: ALLOCATION_POLICY, fundingPolicy: FUNDING_POLICY,
    accountAddress: input.accountAddress?.toLowerCase() || null,
    activationTimestamp: timestamp(input.activationTimestamp),
    destinationAccountId: input.destinationAccountId == null ? null : account(input.destinationAccountId) };
}
function receipt(row) {
  const raw = parse(row.raw_response);
  const result = raw?.result || raw;
  return result?.order || result;
}
function fingerprint(credit) {
  return JSON.stringify([credit.event_id, credit.account_id, credit.timestamp, credit.instrument_name,
    credit.venue_order_id, credit.gross_usdc, credit.sale_fee_usdc, credit.net_usdc]);
}

function eventFingerprint(event) {
  let raw;
  try { raw = parse(event.raw_json); } catch { raw = null; }
  // Inspect every repeated source identity before filtering for eligible put sales.
  // A changed direction/instrument must not hide a conflict in an ignored row.
  return JSON.stringify([
    ...['event_id', 'account_id', 'event_type', 'timestamp', 'instrument_name', 'currency', 'amount',
      'cashflow_usd', 'fee_usd', 'realized_pnl_usd', 'source'].map(key => event[key]),
    raw ? ['trade_id', 'order_id', 'subaccount_id', 'timestamp', 'instrument_name', 'direction', 'is_transfer',
      'trade_amount', 'amount', 'trade_price', 'price', 'trade_fee', 'fee_currency', 'realized_pnl_excl_fees', 'batch_status', 'tx_status'].map(key => raw[key]) : event.raw_json,
  ]);
}

function projectCredits({ events, orders, ...input }) {
  const config = configuration(input);
  if (!Array.isArray(events) || !Array.isArray(orders)) throw new Error('Complete event/order arrays required');
  const owned = new Map();
  for (const row of orders) {
    if (row?.action !== 'sell_put' || ![true, 1].includes(row.success)) continue;
    try {
      const order = receipt(row);
      if (order?.order_id && String(order.subaccount_id) === config.accountId && order.direction === 'sell'
        && order.instrument_name === row.instrument_name && /^ETH-\d{8}-[\d.]+-P$/.test(row.instrument_name)) {
        owned.set(order.order_id, order);
      }
    } catch { /* Malformed local receipts never establish ownership. */ }
  }
  const credits = [], deferred = [], seen = new Map();
  const variants = new Map(), conflictingIds = new Set();
  for (const event of events) {
    if (!event?.event_id) continue;
    const key = eventFingerprint(event);
    if (variants.has(event.event_id) && variants.get(event.event_id) !== key) conflictingIds.add(event.event_id);
    variants.set(event.event_id, key);
  }
  for (const eventId of conflictingIds) deferred.push({ event_id: eventId, reason: 'Conflicting duplicate economic event' });
  let ignoredCount = 0, total = 0n;
  for (const event of events) {
    if (conflictingIds.has(event?.event_id)) continue;
    if (event?.event_type !== 'trade' || !/^ETH-\d{8}-[\d.]+-P$/.test(event.instrument_name || '')) { ignoredCount++; continue; }
    try {
      const trade = parse(event.raw_json);
      if (trade?.direction !== 'sell') { ignoredCount++; continue; }
      if (timestamp(event.timestamp) < config.activationTimestamp) { ignoredCount++; continue; }
      const normalized = normalizeV2Trade(trade, config.accountId, config.apiVersion);
      for (const key of ['event_id', 'account_id', 'event_type', 'timestamp', 'instrument_name', 'currency', 'amount', 'cashflow_usd', 'fee_usd', 'source']) {
        if (normalized[key] !== event[key]) throw new Error(`Canonical event ${key} conflicts with settled raw trade`);
      }
      const order = owned.get(trade.order_id);
      if (!order || order.instrument_name !== trade.instrument_name) throw new Error('Owned sell_put receipt missing or conflicting');
      const gross = toUnits(normalized.cashflow_usd, 'gross sale proceeds');
      const fee = toUnits(normalized.fee_usd, 'sale fee');
      if (fee > gross) throw new Error('Sale fee exceeds proceeds');
      const credit = { event_id: normalized.event_id, account_id: config.accountId, timestamp: normalized.timestamp,
        instrument_name: normalized.instrument_name, venue_order_id: trade.order_id,
        gross_usdc: fromUnits(gross), sale_fee_usdc: fromUnits(fee), net_usdc: fromUnits(gross - fee) };
      const key = fingerprint(credit);
      if (seen.has(credit.event_id)) {
        if (seen.get(credit.event_id) !== key) throw new Error('Conflicting duplicate source trade');
        continue;
      }
      seen.set(credit.event_id, key); credits.push(credit); total += gross - fee;
    } catch (error) { deferred.push({ event_id: event?.event_id ?? null, reason: error.message }); }
  }
  // A conflicting duplicate must not leave an earlier copy available to spend.
  const invalidIds = new Set(deferred.map(row => row.event_id));
  const accepted = credits.filter(row => !invalidIds.has(row.event_id));
  total = accepted.reduce((sum, row) => sum + toUnits(row.net_usdc, 'net proceeds'), 0n);
  const split = splitNetProceeds(fromUnits(total));
  return { credits: accepted, total_net_usdc: fromUnits(total), allocated_usdc: { ETH: split.ETH, DRV: split.DRV },
    remainder_usdc: split.remainder, ignored_count: ignoredCount, deferred, complete: deferred.length === 0 };
}

function createSpotAccumulationStore(db, input, { readOnly = true } = {}) {
  const config = configuration(input);
  const configJson = JSON.stringify(config);
  const scope = `${config.network}:${config.apiVersion}:${config.accountId}`;
  const startupFunding = { kind: 'operator_startup_sample_v1', currency: 'USDC', amount_usdc: SAMPLE_BUDGET_USDC,
    account_address: config.accountAddress, source_account_id: config.accountId,
    destination_account_id: config.destinationAccountId, activation_timestamp: config.activationTimestamp };
  const startupFundingJson = JSON.stringify(startupFunding);
  const exists = () => Boolean(db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='spot_accumulation_config'").get());
  checkConfig(); // Refuse incompatible state before any schema mutation.
  if (!readOnly) db.exec(`
    CREATE TABLE IF NOT EXISTS spot_accumulation_config (scope TEXT PRIMARY KEY, config_json TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS spot_credits (scope TEXT NOT NULL, event_id TEXT NOT NULL, credit_json TEXT NOT NULL,
      PRIMARY KEY(scope,event_id));
    CREATE TABLE IF NOT EXISTS spot_operator_funding (scope TEXT PRIMARY KEY, funding_json TEXT NOT NULL,
      authorized_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
    CREATE TABLE IF NOT EXISTS spot_reservations (scope TEXT NOT NULL, id TEXT NOT NULL, asset TEXT NOT NULL,
      amount TEXT NOT NULL, request_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'unknown',
      settled_spend TEXT NOT NULL DEFAULT '0', evidence_json TEXT, PRIMARY KEY(scope,id));
    CREATE TABLE IF NOT EXISTS spot_transfers (scope TEXT NOT NULL, id TEXT NOT NULL, amount TEXT NOT NULL,
      request_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'prepared', op_uuid TEXT,
      response_json TEXT, evidence_json TEXT, source_debit TEXT NOT NULL DEFAULT '0',
      destination_credit TEXT NOT NULL DEFAULT '0', fee TEXT NOT NULL DEFAULT '0', PRIMARY KEY(scope,id));
  `);
  function checkConfig(write = false) {
    if (write && readOnly) throw new Error('Read-only accumulation store');
    if (!exists()) return false;
    let row = db.prepare('SELECT config_json FROM spot_accumulation_config WHERE scope=?').get(scope);
    if (!row && write) {
      db.prepare('INSERT INTO spot_accumulation_config VALUES (?,?)').run(scope, configJson);
      row = { config_json: configJson };
    }
    if (row && row.config_json !== configJson) throw new Error('Accumulation configuration/boundary changed');
    return Boolean(row);
  }
  // Opening a read-only view never creates tables or persists an activation boundary.
  if (!readOnly) db.transaction(() => checkConfig(true)).immediate();
  else checkConfig();
  function operatorFunding() {
    // Old initialized databases remain readable without a schema mutation.
    if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='spot_operator_funding'").get()) return null;
    const row = db.prepare('SELECT * FROM spot_operator_funding WHERE scope=?').get(scope);
    if (!row) return null;
    if (row.funding_json !== startupFundingJson) throw new Error('Operator startup funding conflicts with the fixed sample authority');
    return { ...startupFunding, authorized_at: timestamp(row.authorized_at) };
  }
  function snapshot() {
    const initialized = checkConfig();
    const credits = initialized ? db.prepare('SELECT credit_json FROM spot_credits WHERE scope=? ORDER BY event_id').all(scope).map(row => JSON.parse(row.credit_json)) : [];
    const rows = initialized ? db.prepare('SELECT * FROM spot_reservations WHERE scope=? ORDER BY id').all(scope) : [];
    const transfers = initialized ? db.prepare('SELECT * FROM spot_transfers WHERE scope=? ORDER BY id').all(scope) : [];
    const net = credits.reduce((sum, row) => sum + toUnits(row.net_usdc, 'net credit'), 0n);
    const operator = initialized ? operatorFunding() : null;
    const operatorAmount = operator ? toUnits(operator.amount_usdc, 'operator sample funding') : 0n;
    const totalFunding = net + operatorAmount;
    const split = splitNetProceeds(fromUnits(totalFunding));
    let sourceReserved = 0n, sourceDebited = 0n, destinationFunded = 0n, transferFees = 0n;
    for (const transfer of transfers) {
      sourceDebited += toUnits(transfer.source_debit, 'source debit');
      destinationFunded += toUnits(transfer.destination_credit, 'destination credit');
      transferFees += toUnits(transfer.fee, 'transfer fee');
      if (transfer.status !== 'settled') sourceReserved += toUnits(transfer.amount, 'transfer reservation');
    }
    if (sourceDebited !== destinationFunded + transferFees || sourceDebited + sourceReserved > totalFunding) throw new Error('Transfer conservation violated');
    const assets = Object.fromEntries(['ETH', 'DRV'].map(asset => {
      let reserved = 0n, spent = 0n;
      for (const row of rows.filter(row => row.asset === asset)) {
        const settled = toUnits(row.settled_spend, 'settled spend');
        spent += settled;
        if (row.status !== 'terminal') reserved += toUnits(row.amount, 'reserved amount') - settled;
      }
      const allocation = toUnits(split[asset], 'asset allocation');
      if (asset === 'DRV') reserved += sourceReserved;
      const fees = asset === 'DRV' ? transferFees : 0n;
      if (spent + reserved + fees > allocation) throw new Error('Accumulation conservation violated');
      return [asset, { allocated_usdc: split[asset], settled_spend_usdc: fromUnits(spent),
        reserved_usdc: fromUnits(reserved), available_usdc: fromUnits(allocation - spent - reserved - fees) }];
    }));
    const destinationAvailable = destinationFunded - toUnits(assets.DRV.settled_spend_usdc, 'spot spend')
      - (toUnits(assets.DRV.reserved_usdc, 'reserved funds') - sourceReserved);
    if (destinationAvailable < 0n) throw new Error('Destination funding conservation violated');
    return { initialized, config, credits, total_net_usdc: fromUnits(net), operator_funding: operator,
      operator_funding_usdc: fromUnits(operatorAmount), total_funding_usdc: fromUnits(totalFunding),
      remainder_usdc: split.remainder, assets,
      funding: { source_available_usdc: fromUnits(totalFunding - sourceDebited - sourceReserved),
        source_reserved_usdc: fromUnits(sourceReserved), destination_funded_usdc: fromUnits(destinationFunded),
        destination_available_usdc: fromUnits(destinationAvailable), transfer_fees_usdc: fromUnits(transferFees) },
      transfers: transfers.map(row => ({ id: row.id, amount: row.amount, status: row.status, op_uuid: row.op_uuid,
        request: JSON.parse(row.request_json), result: row.response_json ? JSON.parse(row.response_json) : null,
        evidence: row.evidence_json ? JSON.parse(row.evidence_json) : null,
        source_debit_usdc: row.source_debit, destination_credit_usdc: row.destination_credit, fee_usdc: row.fee })),
      reservations: rows.map(row => ({ id: row.id, asset: row.asset, amount: row.amount,
        status: row.status, settled_spend_usdc: row.settled_spend, request: JSON.parse(row.request_json),
        evidence: row.evidence_json ? JSON.parse(row.evidence_json) : null })) };
  }
  function authorizeStartupSample() {
    checkConfig(true);
    if (config.apiVersion !== 'v3' || !config.accountAddress || !config.destinationAccountId
      || config.destinationAccountId === config.accountId) throw new Error('Explicit V3 owner and separate sample destination required');
    return db.transaction(() => {
      const before = snapshot();
      if (before.credits.length) throw new Error('Startup sample cannot use existing put-sale credits');
      // This immutable row is a one-time operator allowance, never an invented
      // sale event. Replaying startup retains the original spent/reserved cash.
      if (before.operator_funding) return before;
      if (before.transfers.length || before.reservations.length) throw new Error('Startup sample requires pristine execution state');
      db.prepare('INSERT INTO spot_operator_funding (scope,funding_json) VALUES (?,?)').run(scope, startupFundingJson);
      return snapshot();
    }).immediate();
  }
  function credit({ events, orders }) {
    checkConfig(true);
    const projection = projectCredits({ ...config, events, orders });
    if (!projection.complete) throw new Error(`Unresolved credit evidence: ${projection.deferred.map(row => row.reason).join('; ')}`);
    return db.transaction(() => {
      let inserted = 0;
      for (const item of projection.credits) {
        const previous = db.prepare('SELECT credit_json FROM spot_credits WHERE scope=? AND event_id=?').get(scope, item.event_id);
        if (previous) {
          if (fingerprint(JSON.parse(previous.credit_json)) !== fingerprint(item)) throw new Error('Conflicting recorded source trade');
          continue;
        }
        db.prepare('INSERT INTO spot_credits VALUES (?,?,?)').run(scope, item.event_id, JSON.stringify(item)); inserted++;
      }
      return { inserted, ...snapshot() };
    }).immediate();
  }
  function reserveTransfer({ id, amount, request }) {
    checkConfig(true); identity(id);
    const cap = toUnits(amount, 'transfer reservation', false);
    if (!config.accountAddress || !config.destinationAccountId || config.destinationAccountId === config.accountId
      || account(request?.subaccount_id) !== config.accountId
      || account(request?.to_subaccount_id) !== config.destinationAccountId
      || request.asset_name !== 'USDC' || request.sub_id !== 0 || request.new_subaccount_manager !== 0
      || toUnits(request.amount, 'transfer amount', false) !== cap
      || toUnits(request.max_fee_usd, 'transfer fee cap') !== 0n) throw new Error('Transfer request identity or amount conflicts');
    nonce(request.nonce);
    if (!/^0x[0-9a-f]{40}$/i.test(request.signer || '')
      || !/^0x[0-9a-f]{130}$/i.test(request.signature || '')
      || !Number.isSafeInteger(request.signature_expiry_sec) || request.signature_expiry_sec <= 0) throw new Error('Complete signed transfer request required');
    const requestJson = JSON.stringify(request);
    return db.transaction(() => {
      const saved = db.prepare('SELECT * FROM spot_transfers WHERE scope=? AND id=?').get(scope, id);
      if (saved) {
        if (saved.amount !== fromUnits(cap) || saved.request_json !== requestJson) throw new Error('Conflicting transfer replay');
        return snapshot();
      }
      const summary = snapshot();
      if (summary.transfers.some(row => nonce(row.request.nonce) === nonce(request.nonce))) throw new Error('Transfer nonce already reserved');
      if (summary.transfers.some(row => row.status !== 'settled')) throw new Error('Transfer already unresolved');
      if (cap > toUnits(summary.funding.source_available_usdc, 'source allocation')) throw new Error('Transfer exceeds source allocation');
      db.prepare('INSERT INTO spot_transfers(scope,id,amount,request_json) VALUES (?,?,?,?)').run(scope, id, fromUnits(cap), requestJson);
      return snapshot();
    }).immediate();
  }
  function claimTransferSend({ id }) {
    checkConfig(true); identity(id);
    return db.transaction(() => {
      const row = db.prepare('SELECT * FROM spot_transfers WHERE scope=? AND id=?').get(scope, id);
      if (!row || row.status !== 'prepared') throw new Error('Transfer send already claimed or unavailable; never resend');
      // Commit unknown before the network boundary. A crash here may freeze an unsent
      // request, but cannot cause a second transfer when submission outcome is unknown.
      db.prepare("UPDATE spot_transfers SET status='unknown' WHERE scope=? AND id=?").run(scope, id);
      return JSON.parse(row.request_json);
    }).immediate();
  }
  function acknowledgeTransfer({ id, result }) {
    checkConfig(true); identity(id);
    identity(result?.op_uuid);
    if (!/^\d+$/.test(String(result?.operation_id ?? ''))
      || (typeof result.operation_id === 'number' && !Number.isSafeInteger(result.operation_id))) throw new Error('Transfer acknowledgement identity missing');
    return db.transaction(() => {
      const row = db.prepare('SELECT * FROM spot_transfers WHERE scope=? AND id=?').get(scope, id);
      if (!row || row.status === 'prepared') throw new Error('Transfer was not claimed for submission');
      if (row.op_uuid) {
        if (row.op_uuid !== result.op_uuid || row.response_json !== JSON.stringify(result)) throw new Error('Transfer acknowledgement changed');
        return snapshot();
      }
      if (db.prepare('SELECT id FROM spot_transfers WHERE scope=? AND op_uuid=?').get(scope, result.op_uuid)) throw new Error('Transfer operation already allocated');
      db.prepare("UPDATE spot_transfers SET status='acknowledged',op_uuid=?,response_json=? WHERE scope=? AND id=?")
        .run(result.op_uuid, JSON.stringify(result), scope, id);
      return snapshot();
    }).immediate();
  }
  function reconcileTransfer({ id, operation, history }) {
    checkConfig(true); identity(id);
    return db.transaction(() => {
      const row = db.prepare('SELECT * FROM spot_transfers WHERE scope=? AND id=?').get(scope, id);
      if (!row?.op_uuid || operation?.op_uuid !== row.op_uuid) throw new Error('Known transfer operation UUID required');
      const evidence = { operation, history };
      if (row.status === 'settled') {
        if (row.evidence_json !== JSON.stringify(evidence)) throw new Error('Settled transfer evidence changed');
        return snapshot();
      }
      // Operation failure or absence is not proof that the source was not debited.
      // There is deliberately no release or resend transition for unknown outcomes.
      if (operation.status !== 'Settled') return snapshot();
      if (operation.error_log) throw new Error('Settled transfer has operation errors');
      if (String(history?.sourceAccountId) !== config.accountId
        || String(history?.destinationAccountId) !== config.destinationAccountId
        || !Array.isArray(history?.source) || !Array.isArray(history?.destination)) throw new Error('Account-bound transfer histories required');
      const matching = entries => entries.filter(item => item?.operation_id === row.op_uuid);
      const source = matching(history.source), destination = matching(history.destination);
      if (source.length !== 1 || destination.length !== 1) throw new Error('Unique settled transfer evidence required on both accounts');
      const request = JSON.parse(row.request_json), amount = toUnits(request.amount, 'transfer amount');
      for (const [entry, outgoing] of [[source[0], true], [destination[0], false]]) {
        if (entry.batch_status !== 'Settled' || entry.is_outgoing !== outgoing || entry.asset !== 'USDC'
          || String(entry.from_subaccount_id) !== config.accountId || String(entry.to_subaccount_id) !== config.destinationAccountId
          || entry.from_wallet?.toLowerCase() !== config.accountAddress || entry.to_wallet?.toLowerCase() !== config.accountAddress
          || toUnits(entry.amount, 'transfer history amount') !== amount
          || toUnits(entry.fee, 'transfer history fee') !== 0n) throw new Error('Transfer settlement identity, amount or fee conflicts');
        identity(entry.batch_uuid);
        if (!Number.isSafeInteger(entry.timestamp) || entry.timestamp <= 0) throw new Error('Transfer history timestamp missing');
      }
      if (source[0].batch_uuid !== destination[0].batch_uuid) throw new Error('Transfer settlement batches conflict');
      const hashes = [operation.transaction_hash, source[0].tx_hash, destination[0].tx_hash].filter(Boolean);
      if (hashes.some(hash => typeof hash !== 'string') || new Set(hashes.map(hash => hash.toLowerCase())).size > 1) throw new Error('Transfer settlement transaction hashes conflict');
      if (amount > toUnits(row.amount, 'transfer reservation')) throw new Error('Transfer settlement exceeds reservation');
      db.prepare("UPDATE spot_transfers SET status='settled',source_debit=?,destination_credit=?,fee='0',evidence_json=? WHERE scope=? AND id=?")
        .run(fromUnits(amount), fromUnits(amount), JSON.stringify(evidence), scope, id);
      return snapshot();
    }).immediate();
  }
  function reserve({ id, asset, amount, request }) {
    checkConfig(true); identity(id);
    if (asset !== 'DRV') throw new Error('Only DRV accumulation reservations are supported');
    const cap = toUnits(amount, 'reservation', false);
    const targetAccount = config.destinationAccountId;
    if (!targetAccount || account(request?.subaccount_id) !== targetAccount || request?.instrument_name !== `${asset}-USDC`
      || request.direction !== 'buy' || request.reduce_only !== false
      || request.time_in_force !== 'ioc'
      || (request.extra_fee != null && toUnits(request.extra_fee, 'builder fee') !== 0n)) throw new Error('Reservation order identity conflicts');
    nonce(request.nonce);
    const quantity = toUnits(request.amount, 'order amount', false), price = toUnits(request.limit_price, 'limit price', false);
    const feeCap = toUnits(request.max_fee, 'fee cap');
    const worstCost = (quantity * (price + feeCap) + SCALE - 1n) / SCALE;
    if (cap < worstCost) throw new Error('Reservation cannot cover order and fee cap');
    const requestJson = JSON.stringify(request);
    return db.transaction(() => {
      const saved = db.prepare('SELECT * FROM spot_reservations WHERE scope=? AND id=?').get(scope, id);
      if (saved) {
        if (saved.asset !== asset || saved.amount !== fromUnits(cap) || saved.request_json !== requestJson) throw new Error('Conflicting reservation replay');
        return snapshot();
      }
      const summary = snapshot();
      if (summary.reservations.some(row => String(row.request.subaccount_id) === targetAccount && nonce(row.request.nonce) === nonce(request.nonce))) throw new Error('Order nonce already reserved');
      if (summary.reservations.some(row => row.asset === asset && row.status !== 'terminal')) throw new Error('Asset already has an unresolved reservation');
      if (cap > toUnits(summary.funding.destination_available_usdc, 'funded destination allocation')) throw new Error('Reservation exceeds funded destination allocation');
      db.prepare('INSERT INTO spot_reservations(scope,id,asset,amount,request_json) VALUES (?,?,?,?,?)').run(scope, id, asset, fromUnits(cap), requestJson);
      return snapshot();
    }).immediate();
  }
  function rejectOrder({ id, error }) {
    checkConfig(true); identity(id);
    if (!isDefinitiveOrderRejection(error)) throw new Error('Definitive direct order refusal evidence required');
    return db.transaction(() => {
      const row = db.prepare('SELECT * FROM spot_reservations WHERE scope=? AND id=?').get(scope, id);
      if (!row || row.request_json !== JSON.stringify(error.request)) throw new Error('Order refusal conflicts with reserved request');
      const evidence = JSON.stringify({ rejection: error });
      if (row.status === 'terminal' && row.evidence_json === evidence && toUnits(row.settled_spend, 'prior spend') === 0n) return snapshot();
      if (row.status !== 'unknown' || row.evidence_json || toUnits(row.settled_spend, 'prior spend') !== 0n) throw new Error('Order refusal cannot replace accepted order or fill evidence');
      db.prepare("UPDATE spot_reservations SET status='terminal',settled_spend='0',evidence_json=? WHERE scope=? AND id=?")
        .run(evidence, scope, id);
      return snapshot();
    }).immediate();
  }
  function reconcile({ id, order, trades }) {
    checkConfig(true); identity(id);
    return db.transaction(() => {
      const row = db.prepare('SELECT * FROM spot_reservations WHERE scope=? AND id=?').get(scope, id);
      if (!row) throw new Error('Unknown reservation');
      const request = JSON.parse(row.request_json);
      if (!order?.order_id || String(order.nonce) !== String(request.nonce)
        || String(order.subaccount_id) !== String(request.subaccount_id)
        || order.instrument_name !== request.instrument_name || order.direction !== 'buy'
        || decimal(order.amount) !== decimal(request.amount) || decimal(order.limit_price) !== decimal(request.limit_price)
        || (!TERMINAL.has(order.order_status) && !['open', 'untriggered'].includes(order.order_status))) throw new Error('Order evidence conflicts with reserved request');
      if (!Array.isArray(trades)) throw new Error('Complete cumulative trade evidence required');
      let filled = 0n, settledSpend = 0n, totalNotionalProduct = 0n, allSettled = true;
      const ids = new Set();
      for (const trade of trades) {
        if (!trade?.trade_id || ids.has(trade.trade_id) || trade.order_id !== order.order_id
          || String(trade.subaccount_id) !== String(request.subaccount_id) || trade.instrument_name !== request.instrument_name
          || trade.direction !== 'buy' || trade.is_transfer !== false) throw new Error('Trade evidence identity conflicts');
        ids.add(trade.trade_id);
        const quantity = toUnits(trade.trade_amount, 'fill amount', false), price = toUnits(trade.trade_price, 'fill price', false);
        if (price > toUnits(request.limit_price, 'limit price')) throw new Error('Fill exceeds reserved limit');
        filled += quantity;
        totalNotionalProduct += quantity * price;
        if (trade.batch_status === 'Settled') {
          // This preparation ledger only supports explicitly quote-denominated fees.
          if (trade.fee_currency !== 'USDC') throw new Error('Settled spot fee currency must be verified as USDC');
          const fee = toUnits(trade.trade_fee, 'settled fee');
          if (fee > (quantity * toUnits(request.max_fee, 'fee cap') + SCALE - 1n) / SCALE) throw new Error('Settled fee exceeds signed cap');
          const product = quantity * price;
          if (product % SCALE) throw new Error('Fill precision exceeds exact accounting scale');
          settledSpend += product / SCALE + fee;
        } else allSettled = false;
      }
      const requested = toUnits(request.amount, 'requested amount', false);
      if (filled !== toUnits(order.filled_amount, 'cumulative filled amount') || filled > requested
        || (order.order_status === 'filled' && filled !== requested)
        || (order.order_status === 'rejected' && filled !== 0n)
        || (!TERMINAL.has(order.order_status) && filled === requested)) throw new Error('Incomplete cumulative fill evidence');
      if (filled > 0n) {
        const average = toUnits(order.average_price, 'cumulative average price', false);
        const delta = totalNotionalProduct - filled * average;
        // Same 1e-6 USDC tolerance as option receipt accounting, evaluated exactly.
        const tolerance = toUnits('0.000001', 'notional rounding tolerance') * SCALE;
        if ((delta < 0n ? -delta : delta) > tolerance) throw new Error('Cumulative average price conflicts with trade notional');
      }
      if (settledSpend < toUnits(row.settled_spend, 'prior settled spend') || settledSpend > toUnits(row.amount, 'reservation')) throw new Error('Settled spend conflicts with reservation');
      if (row.evidence_json) {
        const prior = JSON.parse(row.evidence_json);
        if (prior.rejection) throw new Error('Terminal order refusal cannot be replaced by order evidence');
        if (prior.order.order_id !== order.order_id) throw new Error('Order identity changed');
        for (const old of prior.trades) {
          const current = trades.find(item => item.trade_id === old.trade_id);
          const keys = ['trade_amount', 'trade_price', ...(old.batch_status === 'Settled' ? ['trade_fee', 'fee_currency', 'batch_status'] : [])];
          if (!current || keys.some(key => current[key] !== old[key])) throw new Error('Observed fill changed or disappeared');
        }
        if (row.status === 'terminal' && JSON.stringify(prior) !== JSON.stringify({ order, trades })) throw new Error('Terminal evidence changed');
      }
      const status = TERMINAL.has(order.order_status) && allSettled ? 'terminal' : 'pending';
      db.prepare('UPDATE spot_reservations SET status=?,settled_spend=?,evidence_json=? WHERE scope=? AND id=?')
        .run(status, fromUnits(settledSpend), JSON.stringify({ order, trades }), scope, id);
      return snapshot();
    }).immediate();
  }
  return { snapshot, credit, authorizeStartupSample, reserve, reconcile, rejectOrder, reserveTransfer, claimTransferSend, acknowledgeTransfer, reconcileTransfer };
}

module.exports = { SAMPLE_BUDGET_USDC, projectCredits, createSpotAccumulationStore };
