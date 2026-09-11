'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { buildPortfolioObservation } = require('../bot/portfolio-observation');

const receipt = '2026-09-11T10:05:00.123Z';
const options = { timestamp: receipt, spotPrice: '2300' };
const account = changes => ({ subaccount_value: '12345.67', positions: [], collaterals: [], ...changes });
const position = changes => ({ instrument_name: 'ETH-20261127-1600-P', amount: '2', unrealized_pnl: '10', ...changes });

test('one complete account response supplies canonical positions, balances and equity at receipt', () => {
  const raw = account({
    positions: [position(), position({ instrument_name: 'ETH-20261030-3200-C', amount: '-3', unrealized_pnl: '-2.5' })],
    collaterals: [{ asset_name: 'USDC', amount: '500.25' }, { asset_name: 'ETH', amount: '2.75' }],
  });
  const original = structuredClone(raw);
  assert.deepEqual(buildPortfolioObservation(raw, { ...options, grossOptionsCashflow: '40.25' }), {
    timestamp: receipt, spot_price: 2300, usdc_balance: 500.25, eth_balance: 2.75,
    positions_json: [
      { instrument: 'ETH-20261127-1600-P', direction: 'long', amount: 2, unrealized_pnl: 10 },
      { instrument: 'ETH-20261030-3200-C', direction: 'short', amount: 3, unrealized_pnl: -2.5 },
    ],
    total_unrealized_pnl: 7.5, total_realized_pnl: null, gross_options_cashflow: 40.25, portfolio_value_usd: 12345.67,
  });
  assert.deepEqual(raw, original);
});

test('explicitly flat accounts and confirmed absent supported balances are zero, realized profit remains unknown', () => {
  const result = buildPortfolioObservation(account(), options);
  assert.deepEqual(result.positions_json, []);
  assert.equal(result.total_unrealized_pnl, 0);
  assert.equal(result.usdc_balance, 0);
  assert.equal(result.eth_balance, 0);
  assert.equal(result.total_realized_pnl, null);
  assert.equal(result.gross_options_cashflow, null);
  assert.equal(buildPortfolioObservation(account(), { ...options, grossOptionsCashflow: 0 }).gross_options_cashflow, 0);
});

test('zero and negative equity, balances and P&L retain their actual values', () => {
  for (const equity of [0, -100, '0', '-100']) {
    const result = buildPortfolioObservation(account({ subaccount_value: equity,
      positions: [position({ unrealized_pnl: '0' })],
      collaterals: [{ asset_name: 'USDC', amount: '-20' }, { asset_name: 'ETH', amount: '0' }],
    }), options);
    assert.equal(result.portfolio_value_usd, Number(equity));
    assert.equal(result.usdc_balance, -20);
    assert.equal(result.eth_balance, 0);
    assert.equal(result.positions_json[0].unrealized_pnl, 0);
    assert.equal(result.total_unrealized_pnl, 0);
  }
});

test('missing or malformed arrays cannot become a flat account or zero balance', () => {
  for (const value of [undefined, null, {}, '', false]) {
    assert.throws(() => buildPortfolioObservation(account({ positions: value }), options), /explicit positions and collaterals/);
    assert.throws(() => buildPortfolioObservation(account({ collaterals: value }), options), /explicit positions and collaterals/);
  }
  for (const value of [null, [], false]) assert.throws(() => buildPortfolioObservation(value, options), /subaccount response/);
});

test('required numerical fields reject absent values, nonnumeric coercions and infinities', () => {
  for (const value of [undefined, null, '', ' ', false, true, [], {}, NaN, Infinity, '-Infinity', 'NaN', '0x10']) {
    assert.throws(() => buildPortfolioObservation(account({ subaccount_value: value }), options), /subaccount value/);
    assert.throws(() => buildPortfolioObservation(account({ positions: [position({ amount: value })] }), options), /position amount/);
    assert.throws(() => buildPortfolioObservation(account({ positions: [position({ unrealized_pnl: value })] }), options), /unrealized P&L/);
    assert.throws(() => buildPortfolioObservation(account({ collaterals: [{ asset_name: 'ETH', amount: value }] }), options), /collateral amount/);
  }
});

test('flat venue rows may omit unrealized P&L but nonzero positions may not', () => {
  const result = buildPortfolioObservation(account({ positions: [position({ amount: '0', unrealized_pnl: null })] }), options);
  assert.deepEqual(result.positions_json, []);
  assert.equal(result.total_unrealized_pnl, 0);
  assert.throws(() => buildPortfolioObservation(account({ positions: [position({ amount: '-0.1', unrealized_pnl: null })] }), options), /unrealized P&L/);
});

test('instrument and asset identities are mandatory and duplicate rows cannot inflate the observation', () => {
  for (const name of [undefined, null, '', ' ', 'ETH\n', 123, {}]) {
    assert.throws(() => buildPortfolioObservation(account({ positions: [position({ instrument_name: name })] }), options), /instrument name/);
    assert.throws(() => buildPortfolioObservation(account({ collaterals: [{ asset_name: name, amount: '1' }] }), options), /asset name/);
  }
  assert.throws(() => buildPortfolioObservation(account({ positions: [position(), position()] }), options), /Duplicate position/);
  assert.throws(() => buildPortfolioObservation(account({ collaterals: [{ asset_name: 'ETH', amount: '1' }, { asset_name: 'ETH', amount: '2' }] }), options), /Duplicate collateral/);
});

test('mixed endpoint shapes and normalized short amounts are rejected instead of reinterpreted', () => {
  assert.throws(() => buildPortfolioObservation({ result: account() }, options), /subaccount response/);
  assert.throws(() => buildPortfolioObservation(account({ positions: [{ instrument: 'ETH-PERP', amount: 1, unrealized_pnl: 0 }] }), options), /instrument name/);
  assert.throws(() => buildPortfolioObservation(account({ positions: [position({ direction: 'short', amount: 2 })] }), options), /direction conflicts/);
  assert.throws(() => buildPortfolioObservation(account({ collaterals: [{ asset: 'ETH', amount: 1 }] }), options), /asset name/);
  const result = buildPortfolioObservation(account({ positions: [position({ direction: 'short', amount: -2, unrealized_pnl: 0 })] }), options);
  assert.equal(result.positions_json[0].direction, 'short');
});

test('other valid collateral assets do not imply ETH or USDC holdings', () => {
  const result = buildPortfolioObservation(account({ collaterals: [{ asset_name: 'BTC', amount: '1' }] }), options);
  assert.equal(result.eth_balance, 0);
  assert.equal(result.usdc_balance, 0);
});

test('receipt time is explicit and normalized without using account or wall-clock defaults', () => {
  assert.equal(buildPortfolioObservation(account({ timestamp: '2020-01-01T00:00:00Z' }), { ...options, timestamp: Date.parse(receipt) }).timestamp, receipt);
  assert.equal(buildPortfolioObservation(account(), { ...options, timestamp: new Date(receipt) }).timestamp, receipt);
  for (const timestamp of [undefined, null, '', ' ', false, [], {}, NaN, 'invalid']) {
    assert.throws(() => buildPortfolioObservation(account(), { ...options, timestamp }), /receipt timestamp/);
  }
});

test('unknown spot or cashflow never falls back to zero and aggregate overflow fails closed', () => {
  for (const spotPrice of [undefined, null, 0, -1, NaN, Infinity, false, '']) {
    assert.throws(() => buildPortfolioObservation(account(), { ...options, spotPrice }), /spot price/);
  }
  for (const grossOptionsCashflow of ['', false, {}, NaN, Infinity]) {
    assert.throws(() => buildPortfolioObservation(account(), { ...options, grossOptionsCashflow }), /gross options cashflow/);
  }
  assert.throws(() => buildPortfolioObservation(account({ positions: [
    position({ unrealized_pnl: Number.MAX_VALUE }),
    position({ instrument_name: 'ETH-PERP', unrealized_pnl: Number.MAX_VALUE }),
  ] }), options), /sum is not finite/);
});
