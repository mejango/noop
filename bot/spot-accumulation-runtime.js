'use strict';

// One sequential owner for accumulation. All venue I/O is injected; importing
// this module never opens a database, starts a timer, or signs an action.
const { randomUUID } = require('node:crypto');
const { buildSpotPlan, toUnits, fromUnits } = require('./spot-accumulation');
const { createSpotAccumulationStore, projectCredits, SAMPLE_BUDGET_USDC } = require('./spot-accumulation-store');
const accounting = require('./order-accounting');
const { isDefinitiveOrderRejection } = require('./derive-errors');

function createSpotAccumulationRuntime({ db, config, venue, mode = () => 'off', canSend = () => false,
  now = Date.now, notify = () => {} }) {
  if (!db?.db?.transaction || !venue) throw new Error('Accumulation database and venue required');
  let busy = false;
  const currentMode = () => {
    const value = typeof mode === 'function' ? mode() : mode;
    if (!['off', 'dry_run', 'live', 'sample'].includes(value)) throw new Error('Invalid accumulation mode');
    return value;
  };
  const store = (writable = false) => createSpotAccumulationStore(db.db, config, { readOnly: !writable });
  const journalRows = () => db.db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_submissions'").get()
    ? db.db.prepare("SELECT * FROM execution_submissions WHERE status IN ('unknown','acknowledged') ORDER BY id").all() : [];
  const assertJournalClear = () => {
    if (journalRows().length) throw new Error('Unresolved execution submission; reconciliation required');
  };
  function assertSourceClear() {
    if (store().snapshot().transfers.some(row => row.status !== 'settled')) {
      throw new Error('Accumulation transfer unresolved; source funds remain reserved');
    }
  }
  function assertSendAllowed(expectedMode) {
    const selected = currentMode();
    if (!['live', 'sample'].includes(selected) || selected !== expectedMode || canSend() !== true) throw new Error('Accumulation sends disabled');
    const cap = toUnits(config.maxBatchUsdc, 'live batch cap', false);
    if (selected === 'sample' && cap !== toUnits(SAMPLE_BUDGET_USDC)) throw new Error('Startup sample requires its fixed lifetime budget');
  }
  const sampleComplete = snapshot => snapshot.reservations.some(row => row.status === 'terminal'
    && toUnits(row.settled_spend_usdc) > 0n);
  function assertSampleCanSpend() {
    const snapshot = store().snapshot();
    if (snapshot.credits.length || toUnits(snapshot.operator_funding_usdc) !== toUnits(SAMPLE_BUDGET_USDC)) {
      throw new Error('Startup sample requires only its explicit operator funding');
    }
    if (sampleComplete(snapshot)) throw new Error('Startup sample purchase already completed');
  }
  async function guarded(fn) {
    if (busy) return { status: 'deferred', reason: 'accumulation_in_flight' };
    busy = true;
    try { return await fn(); }
    catch (error) {
      const result = { status: 'deferred', reason: error.message };
      try { await notify(result); } catch { /* Reporting cannot change durable funds. */ }
      return result;
    } finally { busy = false; }
  }
  function ownJournal(row, reservation) {
    const request = JSON.parse(row.request_json);
    if (request.action !== 'buy_drv' || request.accumulation_reservation_id !== reservation.id
      || request.account_address?.toLowerCase() !== config.accountAddress?.toLowerCase()
      || String(request.subaccount_id) !== String(config.destinationAccountId)) throw new Error('Spot journal identity conflicts');
    for (const key of ['nonce', 'instrument_name', 'direction', 'amount', 'limit_price', 'max_fee', 'time_in_force']) {
      if (String(request[key]) !== String(reservation.request[key])) throw new Error('Spot journal terms conflict');
    }
    return request;
  }
  async function recoverInternal() {
    const snapshot = store().snapshot();
    if (!snapshot.initialized) {
      if (journalRows().some(row => JSON.parse(row.request_json).action === 'buy_drv')) throw new Error('Spot journal has no allocation store');
      return { status: 'idle' };
    }
    const writer = store(true);
    for (const transfer of snapshot.transfers.filter(row => row.status !== 'settled')) {
      // Without the operation UUID, absence/amount/time are not authoritative.
      if (!transfer.op_uuid) throw new Error(`Transfer ${transfer.id} requires authoritative operation recovery; reservation retained`);
      const evidence = await venue.collectTransfer(transfer.request, transfer.result);
      writer.reconcileTransfer({ id: transfer.id, ...evidence });
    }
    const reservations = writer.snapshot().reservations.filter(row => row.status !== 'terminal');
    for (const reservation of reservations) {
      const matches = journalRows().filter(row => JSON.parse(row.request_json).accumulation_reservation_id === reservation.id);
      if (matches.length !== 1) throw new Error('Spot reservation requires one matching unresolved submission');
      const row = matches[0]; ownJournal(row, reservation);
      const evidence = await venue.collectOrder(reservation.request);
      if (row.response_json) {
        const saved = JSON.parse(row.response_json);
        const acknowledged = saved?.result?.order || saved?.result || saved?.order || saved;
        if (acknowledged?.order_id && acknowledged.order_id !== evidence?.order?.order_id) {
          throw new Error('Recovered order conflicts with saved acknowledgement');
        }
      }
      db.db.transaction(() => {
        const result = writer.reconcile({ id: reservation.id, ...evidence });
        if (result.reservations.find(item => item.id === reservation.id)?.status === 'terminal') {
          const changed = db.db.prepare("UPDATE execution_submissions SET status='accounted' WHERE id=? AND status IN ('unknown','acknowledged')").run(row.id).changes;
          if (changed !== 1) throw new Error('Spot submission accounting changed concurrently');
        }
      }).immediate();
    }
    // An orphan journal must remain blocking, never be inferred as zero-fill.
    const remaining = journalRows();
    if (remaining.length) throw new Error('Unresolved execution submission; reconciliation required');
    assertSourceClear();
    return { status: 'reconciled' };
  }
  function historyProjection(history) {
    const observedAt = now();
    const through = Date.parse(history?.throughTimestamp);
    if (history?.complete !== true || !Number.isFinite(through) || through > observedAt
      || through < Date.parse(config.activationTimestamp) || !Array.isArray(history.events)
      || history.events.some(row => !Number.isFinite(Date.parse(row.timestamp)) || Date.parse(row.timestamp) > through)) {
      throw new Error('Complete settled source history through a valid watermark required');
    }
    const projection = projectCredits({ ...config, events: history.events, orders: history.orders });
    if (!projection.complete) throw new Error(`Unresolved source credits: ${projection.deferred.map(row => row.reason).join('; ')}`);
    return projection;
  }
  function requireMatchingPrepared(prepared, proposal, keys) {
    for (const key of keys) if (String(prepared?.[key]) !== String(proposal[key])) throw new Error(`Prepared ${key} differs from preview`);
  }
  async function advanceInternal({ cycleBudget, pendingActions = [], sourceHistory, optionsCycleSucceeded = false } = {}) {
    const selectedMode = currentMode();
    const sampleMode = selectedMode === 'sample';
    if (selectedMode === 'off') return { status: 'off' };
    if (optionsCycleSucceeded !== true) return { status: 'deferred', reason: 'source_options_cycle_not_verified' };
    if (selectedMode === 'live' || sampleMode) { assertSendAllowed(selectedMode); await recoverInternal(); }
    assertJournalClear(); assertSourceClear();
    // Sample authority is a separate persisted grant, never a fabricated sale
    // or permission to consume the unrelated ALT setup balance.
    const projected = sampleMode ? null : historyProjection(sourceHistory);
    let snapshot = store().snapshot();
    if (sampleMode) {
      if (snapshot.credits.length) throw new Error('Startup sample cannot use ordinary put-sale credits');
      snapshot = store(true).authorizeStartupSample();
      if (sampleComplete(snapshot)) return { status: 'sample_complete',
        operator_funding_usdc: snapshot.operator_funding_usdc,
        settled_spend_usdc: snapshot.assets.DRV.settled_spend_usdc };
    }
    if (selectedMode === 'live') snapshot = store(true).credit({ events: sourceHistory.events, orders: sourceHistory.orders });
    let sourceAvailable = toUnits(snapshot.funding.source_available_usdc);
    if (selectedMode === 'dry_run') {
      const prior = new Map(snapshot.credits.map(row => [row.event_id, row]));
      for (const credit of projected.credits) {
        const previous = prior.get(credit.event_id);
        if (previous && JSON.stringify(previous) !== JSON.stringify(credit)) throw new Error('Conflicting recorded source trade');
        if (!previous) sourceAvailable += toUnits(credit.net_usdc);
      }
    }
    let destinationAvailable = toUnits(snapshot.funding.destination_available_usdc);
    if (config.maxBatchUsdc != null) {
      const cap = toUnits(config.maxBatchUsdc, 'batch cap', false);
      if (sourceAvailable > cap) sourceAvailable = cap;
      if (destinationAvailable > cap) destinationAvailable = cap;
    }
    const inputs = await venue.readSnapshot({ cycleBudget, pendingActions });
    if (String(inputs?.source?.subaccount_id) !== String(config.accountId)
      || String(inputs?.destination?.subaccount_id) !== String(config.destinationAccountId)
      || inputs.source.owner?.toLowerCase() !== config.accountAddress?.toLowerCase()
      || inputs.destination.owner?.toLowerCase() !== config.accountAddress?.toLowerCase()) throw new Error('Venue snapshot account identity conflicts');
    const plan = buildSpotPlan({ ...inputs, nowMs: now(), allocations: { DRV: {
      source_usdc: fromUnits(sourceAvailable), destination_usdc: fromUnits(destinationAvailable) } },
      inflight: { DRV: snapshot.reservations.some(row => row.status !== 'terminal')
        || snapshot.transfers.some(row => row.status !== 'settled') } });
    const route = plan.routes.DRV;
    if (!['ready_for_preview', 'transfer_preview_required'].includes(route.status)) return { status: 'deferred', reason: route.reason, plan };
    const isOrder = route.status === 'ready_for_preview';
    const preview = isOrder ? await venue.previewOrder(route.order) : await venue.previewTransfer(route.transfer);
    if (preview?.valid !== true) throw new Error('Exact accumulation preview did not validate');
    if (selectedMode === 'dry_run') return { status: 'dry_run', plan, preview, credits: projected };
    assertSendAllowed(selectedMode); assertJournalClear(); assertSourceClear();
    const writer = store(true);
    if (isOrder) {
      const request = await venue.prepareOrder(route.order);
      requireMatchingPrepared(request, route.order, ['subaccount_id', 'instrument_name', 'direction', 'amount', 'limit_price', 'max_fee', 'time_in_force', 'reduce_only', 'signature_expiry_sec']);
      assertSendAllowed(selectedMode); assertJournalClear(); assertSourceClear();
      const id = randomUUID();
      let submissionId = null;
      const beforeSend = () => {
        if (submissionId != null) throw new Error('Spot order send already claimed');
        submissionId = db.db.transaction(() => {
          assertSendAllowed(selectedMode); assertJournalClear(); assertSourceClear();
          if (sampleMode) assertSampleCanSpend();
          writer.reserve({ id, asset: 'DRV', amount: route.max_spend_usdc, request });
          return accounting.beginSubmission(db, null, { ...request, action: 'buy_drv',
            account_address: config.accountAddress, accumulation_reservation_id: id });
        }).immediate();
      };
      // The adapter finishes its final preview/authentication before this sync
      // hook. Only the immediate HTTP handoff follows the durable claim.
      let receipt;
      try {
        receipt = await venue.submitOrder(request, beforeSend);
      } catch (error) {
        const proof = error.orderRejection;
        if (submissionId == null || !isDefinitiveOrderRejection(proof)
          || JSON.stringify(proof.request) !== JSON.stringify(request)) throw error;
        // Only the direct refusal of this send can release its reservation.
        // A timeout, later history absence, or any prior ACK remains recovery work.
        db.db.transaction(() => {
          const row = db.db.prepare('SELECT * FROM execution_submissions WHERE id=?').get(submissionId);
          if (!row || row.status !== 'unknown' || row.response_json != null) {
            throw new Error('Order rejection conflicts with existing submission evidence');
          }
          ownJournal(row, { id, request });
          writer.rejectOrder({ id, error: proof });
          const changed = db.db.prepare("UPDATE execution_submissions SET status='rejected',response_json=? WHERE id=? AND status='unknown' AND response_json IS NULL")
            .run(JSON.stringify(proof), submissionId).changes;
          if (changed !== 1) throw new Error('Order rejection state changed concurrently');
        }).immediate();
        return { status: 'order_rejected', reason: error.message, id, plan };
      }
      if (submissionId == null) throw new Error('Order transport omitted durable before-send callback');
      accounting.noteSubmission(db, submissionId, receipt?.result ? receipt : { result: receipt });
      await recoverInternal();
      return { status: sampleMode && sampleComplete(writer.snapshot()) ? 'sample_complete' : 'order_reconciled', id, plan };
    }
    const request = await venue.prepareTransfer(route.transfer);
    requireMatchingPrepared(request, { subaccount_id: route.transfer.from_subaccount_id,
      to_subaccount_id: route.transfer.to_subaccount_id, amount: route.transfer.amount,
      asset_name: 'USDC', max_fee_usd: '0', new_subaccount_manager: 0, sub_id: 0 },
    ['subaccount_id', 'to_subaccount_id', 'amount', 'asset_name', 'max_fee_usd', 'new_subaccount_manager', 'sub_id']);
    if (toUnits(route.transfer.max_fee_usdc) !== 0n || toUnits(route.source_spend_usdc) !== toUnits(request.amount)) {
      throw new Error('Only verified zero-fee existing-account transfers are supported');
    }
    assertSendAllowed(selectedMode); assertJournalClear(); assertSourceClear();
    const id = randomUUID(); let claimed = false;
    const beforeSend = () => {
      if (claimed) throw new Error('Transfer send already claimed');
      db.db.transaction(() => {
        assertSendAllowed(selectedMode); assertJournalClear(); assertSourceClear();
        if (sampleMode) assertSampleCanSpend();
        writer.reserveTransfer({ id, amount: route.source_spend_usdc, request });
        writer.claimTransferSend({ id });
      }).immediate();
      claimed = true;
    };
    const result = await venue.submitTransfer(request, beforeSend);
    if (!claimed) throw new Error('Transfer transport omitted durable before-send callback');
    writer.acknowledgeTransfer({ id, result });
    await recoverInternal();
    return { status: 'transfer_reconciled', id, plan };
  }
  return { recover: () => guarded(recoverInternal), advance: input => guarded(() => advanceInternal(input)),
    assertSourceClear, snapshot: () => store().snapshot() };
}

module.exports = { createSpotAccumulationRuntime };
