'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { buildSpotPlan, splitNetProceeds, toUnits, SCALE } = require('../bot/spot-accumulation');
const NOW = 1794241487000;
const owner = '0x823b92d6a4b2AED4b15675c7917c9f922ea8ADAD';
const instrument = (asset, universe, tick, step, minimum, maximum) => ({
  instrument_name: `${asset}-USDC`, instrument_type: 'erc20', is_active: true,
  base_currency: asset, quote_currency: 'USDC', risk_universe_id: universe,
  tick_size: tick, amount_step: step, minimum_amount: minimum, maximum_amount: maximum, base_asset_sub_id: '0',
});
function input() {
  return {
    nowMs: NOW,
    source: { subaccount_id: 25923, owner, risk_universe_id: 1, received_at: NOW, usdc_balance: '3000', committed_usdc: '100', cycle_budget_usdc: '100' },
    destination: { subaccount_id: 99, owner, risk_universe_id: 3, received_at: NOW, usdc_balance: '0', committed_usdc: '0' },
    allocations: { ETH: { source_usdc: '0' }, DRV: { source_usdc: '2000', destination_usdc: '0' } },
    instruments: { ETH: instrument('ETH', 1, '0.01', '0.01', '0.1', '1000'), DRV: instrument('DRV', 3, '0.0001', '1', '10', '1000000') },
    feeds: Object.fromEntries(['ETH', 'USDC'].map(currency => [currency, { currency, price: currency === 'ETH' ? '2000' : '0.999',
      timestamp: NOW / 1000 - 1, received_at: NOW, deadline: NOW / 1000 + 3600, confidence: '1' }])),
    drvTicker: { a: '0.3', A: '10000', t: NOW, received_at: NOW },
    fees: { ETH: { max_fee_per_unit: '7' }, DRV: { max_fee_per_unit: '0.01' } },
    keyExpirySec: NOW / 1000 + 86400, transfer: { max_fee_usdc: '1.5' }, inflight: { ETH: false, DRV: false },
  };
}
test('all proceeds go to DRV without losing atomic units, including a legacy split carry', () => {
  let remainder = '0', eth = 0n, drv = 0n;
  for (const value of ['0.000000000000000001', '0.000000000000000001', '99.999999999999999999']) {
    const split = splitNetProceeds(value, remainder); remainder = split.remainder;
    eth += toUnits(split.ETH); drv += toUnits(split.DRV);
  }
  assert.equal(eth, 0n); assert.equal(remainder, '0');
  assert.equal(drv, 100n * SCALE + 1n);
  assert.deepEqual(splitNetProceeds('1', '0.000000000000000001'), { ETH: '0', DRV: '1.000000000000000001', remainder: '0' });
  for (const invalid of ['-1', 'Infinity', 'NaN', '1e20', '0.0000000000000000001', undefined, 0.1, 9007199254740993]) {
    assert.throws(() => splitNetProceeds(invalid));
  }
  assert.throws(() => splitNetProceeds('1', '0.000000000000000002'));
});
test('DRV-only plan ignores legacy ETH allocation and cannot produce an ETH order', () => {
  const args = input(); args.allocations.ETH.source_usdc = '100000';
  const before = JSON.stringify(args); const plan = buildSpotPlan(args);
  assert.equal(JSON.stringify(args), before);
  assert.equal(plan.executable, false); assert.equal(plan.dry_run, true);
  assert.deepEqual(plan.routes.ETH, { status: 'disabled', reason: 'ETH_accumulation_disabled' });
  assert.equal(plan.routes.DRV.status, 'transfer_preview_required');
  assert.equal(plan.routes.DRV.transfer.amount, '1998.5');
  delete args.allocations.ETH; delete args.inflight.ETH; delete args.fees.ETH;
  delete args.feeds; delete args.instruments.ETH;
  assert.deepEqual(buildSpotPlan(args), plan);
  args.nowMs = NaN;
  assert.equal(buildSpotPlan(args).routes.ETH.status, 'disabled');
  assert.equal(buildSpotPlan(args).routes.ETH.order, undefined);
});
test('existing commitments are honored without creating another DRV proposal', () => {
  const args = input(); args.inflight.DRV = true;
  const plan = buildSpotPlan(args);
  assert.equal(plan.routes.DRV.reason, 'existing_asset_commitment');
  assert.equal(plan.routes.ETH.order, undefined);
});
test('candidate transfer preserves four cycle budgets and existing source commitments', () => {
  const args = input(); args.source.usdc_balance = '1600';
  const plan = buildSpotPlan(args);
  const drv = toUnits(plan.routes.DRV.source_spend_usdc);
  assert.equal(drv, toUnits('1100'));
  assert.equal(toUnits(args.source.usdc_balance) - drv, toUnits('500'));
  args.source.usdc_balance = '400';
  const blocked = buildSpotPlan(args); assert.equal(blocked.source_available_usdc, '0');
  assert.equal(blocked.routes.ETH.status, 'disabled'); assert.equal(blocked.routes.DRV.status, 'deferred');
});
test('DRV spends only allocated existing ALT cash first and caps quantity to visible depth', () => {
  const args = input(); args.destination.usdc_balance = '900'; args.destination.committed_usdc = '100';
  args.allocations.DRV.destination_usdc = '10'; args.drvTicker.A = '12.9';
  const drv = buildSpotPlan(args).routes.DRV;
  assert.equal(drv.status, 'ready_for_preview'); assert.equal(drv.order.time_in_force, 'ioc');
  assert.equal(drv.order.instrument_name, 'DRV-USDC');
  assert.equal(drv.order.limit_price, '0.3015'); assert.equal(drv.order.amount, '12');
  assert.equal(drv.max_spend_usdc, '3.738'); assert.equal(drv.source_spend_usdc, '0');
});
test('transfer dust cannot cause an unaffordable buy after fees', () => {
  const args = input(); args.allocations.DRV.source_usdc = '4.614999999999999999';
  assert.equal(buildSpotPlan(args).routes.DRV.reason, 'below_minimum_after_transfer_fee');
  args.allocations.DRV.source_usdc = '4.615';
  assert.equal(buildSpotPlan(args).routes.DRV.transfer.amount, '3.115');
});
test('missing ALT, source debt, mismatched owners and stale DRV depth fail closed', () => {
  for (const mutate of [a => { a.destination = null; }, a => { a.source.usdc_balance = '-1'; },
    a => { a.destination.owner = '0x1111111111111111111111111111111111111111'; },
    a => { a.drvTicker.t -= 31000; }, a => { a.drvTicker.A = '9'; },
    a => { a.instruments.DRV.instrument_type = 'perp'; }, a => { a.destination.risk_universe_id = 1; },
    a => { delete a.transfer.max_fee_usdc; }, a => { delete a.destination.committed_usdc; },
    a => { delete a.fees.DRV.max_fee_per_unit; }, a => { delete a.inflight.DRV; },
    a => { delete a.keyExpirySec; }, a => { a.keyExpirySec = NOW / 1000 + 65; },
    a => { a.source.received_at -= 31000; }]) {
    const args = input(); mutate(args); assert.equal(buildSpotPlan(args).routes.DRV.status, 'deferred');
    assert.equal(buildSpotPlan(args).routes.ETH.status, 'disabled');
  }
});
test('unknown or zero cycle budget cannot erase the operating float', () => {
  for (const value of ['0', undefined]) {
    const args = input(); args.source.cycle_budget_usdc = value;
    const plan = buildSpotPlan(args);
    assert.equal(plan.routes.ETH.status, 'disabled');
    assert.equal(plan.routes.DRV.status, 'deferred');
    assert.match(plan.routes.DRV.reason, /put cycle budget/);
  }
});
test('DRV order signature remains bounded by the session key', () => {
  const args = input(); args.destination.usdc_balance = '10'; args.allocations.DRV.destination_usdc = '10';
  args.keyExpirySec = NOW / 1000 + 120;
  const plan = buildSpotPlan(args);
  assert.equal(plan.routes.DRV.order.signature_expiry_sec, args.keyExpirySec - 60);
  args.keyExpirySec = NOW / 1000 + 65;
  assert.equal(buildSpotPlan(args).routes.DRV.reason, 'key_expiry_too_close');
});
