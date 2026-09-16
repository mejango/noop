'use strict';

// Synthetic receipts and transport only. This demo never reads a wallet or a
// live database, and removes only the private temporary directory it creates.
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { openLedger, createOperations, createGateway } = require('./index');
const { createReplayFixture } = require('../strategies/noop-v2-reference/fixtures');
const { contentDigest } = require('../strategy/canonical');

async function runDemo() {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'oof-product-demo-')));
  fs.chmodSync(directory, 0o700);
  const filename = path.join(directory, 'ledger.sqlite');
  let time = Date.parse('2030-01-01T00:00:00.000Z');
  const now = () => new Date(time).toISOString();
  let store;
  try {
    const fixture = createReplayFixture({ action: 'buy_put', mandateId: 'demo-mandate' });
    const scope = { customer_id: 'demo-customer', mandate_id: fixture.mandate.mandate_id };
    let api, gateway;
    let sends = 0;
    const evidenceDigests = new Map();
    const connect = () => {
      store = openLedger({ filename, ledgerId: 'demo-ledger', clock: now });
      if (!api) store.registerMandate({ customer_id: scope.customer_id, mandate: fixture.mandate,
        release: fixture.release, catalog: fixture.strategy.fieldCatalog });
      api = createOperations(store.privileged()).forScope(scope);
      gateway = createGateway(store.privileged(), {
        mode: 'offline_simulation',
        sign: async payload => `synthetic-signature:${payload.nonce}`,
        send: async () => { sends += 1; throw new Error('Simulated response loss'); },
        // This is a local fixture oracle, not a verifier for real venue evidence.
        verifyEvidence: async evidence => evidenceDigests.get(evidence.evidence_id) === contentDigest(evidence),
      });
    };
    const cash = asset => store.scope(scope).getBalances()
      .find(row => row.account === 'assets:cash' && row.asset === asset)?.amount || '0';
    const event = (id, kind, payload) => ({ event_id: `event:${id}`, source_event_id: `simulation:${id}`,
      kind, occurred_at: now(), evidence_ref: `fixture:${id}`, payload });
    connect();
    // The fixture mandate is already active. Real funding/10% activation is a
    // separate product flow; these amounts do not implement that authorization.
    store.scope(scope).appendEvent(event('eth-deposit', 'deposit', { transfer_id: 'eth-deposit', asset: 'ETH', amount: '2' }));
    store.scope(scope).appendEvent(event('usdc-deposit', 'deposit', { transfer_id: 'usdc-deposit', asset: 'USDC', amount: '10' }));
    const decision = fixture.strategy.generateDecision(fixture.inputBundle, { mandate: fixture.mandate });
    store.scope(scope).acceptDecision({ decision, inputBundle: fixture.inputBundle,
      validateEconomicPolicy: fixture.strategy.validateEconomicPolicy });
    api.queue({ operation_id: 'demo-order', intent_id: fixture.binding.intent_id, intent_revision: 1,
      mandate_revision: 1, action: 'buy_put', instrument_ref: fixture.instrumentRef,
      quantity: '0.1', limit_price: '10', max_fee_usdc: '0.01', reservations: [{ asset: 'USDC', amount: '1.01' }] });
    const oldLease = gateway.acquire(scope, { worker_id: 'before-restart', ttl_ms: 1000 });
    try { await gateway.submit(oldLease, 'demo-order'); throw new Error('Expected simulated response loss'); }
    catch (error) { if (error.message !== 'Simulated response loss') throw error; }
    store.close();
    time += 2000;
    connect();
    const afterRestart = api.get('demo-order');
    const lease = gateway.acquire(scope, { worker_id: 'after-restart', ttl_ms: 1000 });
    let retryBlocked = false;
    try { await gateway.submit(lease, 'demo-order'); }
    catch (error) { if (!/already attempted/.test(error.message)) throw error; retryBlocked = true; }
    if (!retryBlocked) throw new Error('An uncertain submission must not be retried');

    const { attempt } = afterRestart;
    const fill = (id, quantity, gross, fee) => event(id, 'option_fill', {
      fill_id: id, order_attempt_id: attempt.attempt_id, instrument: fixture.instrumentRef,
      side: 'buy', quantity, gross_premium: gross, quote_asset: 'USDC', fees: [{ asset: 'USDC', amount: fee }],
    });
    const firstFill = fill('fill-1', '0.04', '0.4', '0.004');
    const evidence = (id, status, quantity, fills) => ({ evidence_id: id,
      account: fixture.mandate.account, attempt_id: attempt.attempt_id, nonce: attempt.nonce,
      payload_digest: attempt.payload_digest, order_id: 'synthetic-order-1', status,
      cumulative_filled_quantity: quantity, fills, complete_fill_set: true });
    const recover = async receipt => {
      evidenceDigests.set(receipt.evidence_id, contentDigest(receipt));
      return gateway.recover(lease, 'demo-order', receipt);
    };
    const partial = await recover(evidence('partial-history', 'open', '0.04', [firstFill]));
    const partialCash = cash('USDC');
    const completeEvidence = evidence('complete-history', 'filled', '0.1', [firstFill, fill('fill-2', '0.06', '0.6', '0.006')]);
    const complete = await recover(completeEvidence);
    const cashAfterFills = cash('USDC');
    const eventsAfterFills = store.scope(scope).listEvents().length;
    await recover(completeEvidence);
    if (cash('USDC') !== cashAfterFills || store.scope(scope).listEvents().length !== eventsAfterFills) {
      throw new Error('Evidence replay changed economic balances');
    }

    // Execution reconciliation and expiry settlement are separate observations.
    // Settle from synthetic venue evidence after the instrument expires.
    time = Date.parse(fixture.inputBundle.instruments[fixture.instrumentRef].expiry) + 1000;
    const settlement = event('expiry', 'option_settlement', { settlement_id: 'synthetic-expiry',
      instrument: fixture.instrumentRef, position_quantity: '0.1', cash_amount: '0.5', quote_asset: 'USDC' });
    store.scope(scope).appendEvent(settlement);
    store.close();
    connect();
    store.scope(scope).appendEvent(settlement);
    const verification = store.scope(scope).verify();
    return {
      mode: 'offline_simulation',
      account: { network: fixture.mandate.account.network, deployment: fixture.mandate.account.deployment },
      restart: { status: afterRestart.status, retry_blocked: retryBlocked, simulated_sends: sends },
      partial_fill: { usdc: partialCash, reserved_usdc: partial.reservations[0].remaining_amount },
      completed_order: { status: complete.status, accounted_quantity: complete.attempt.accounted_quantity,
        usdc: cashAfterFills, reserved_usdc: complete.reservations[0].remaining_amount },
      settled: { eth: cash('ETH'), usdc: cash('USDC'), option_quantity: store.scope(scope).getBalances()
        .find(row => row.account === 'assets:options' && row.asset === `OPTION:${fixture.instrumentRef}`)?.amount || '0' },
      ledger: { valid: verification.valid, events: verification.events, decisions: verification.decisions },
    };
  } finally {
    if (store) store.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
}

module.exports = { runDemo };
if (require.main === module) {
  const args = process.argv.slice(2);
  if (args.length === 1 && args[0] === '--help') {
    process.stdout.write('Usage: node product/demo.js\nRuns a temporary OOF ledger and recovery simulation using the Noop reference Strategy.\n');
  } else if (args.length) {
    process.stderr.write('Unknown arguments. This demo accepts no wallet, database, or execution configuration.\n');
    process.exitCode = 1;
  } else {
    runDemo().then(result => process.stdout.write(`${JSON.stringify(result, null, 2)}\n`), error => {
      process.stderr.write(`OOF simulation failed: ${error.message}\n`);
      process.exitCode = 1;
    });
  }
}
