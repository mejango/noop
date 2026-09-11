'use strict';

// A portfolio observation must come from one complete subaccount response.
// Missing account state is a gap, not a zero balance or an empty portfolio.
function finiteNumber(value, field) {
  if ((typeof value !== 'number' && typeof value !== 'string')
      || (typeof value === 'string' && !/^[+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:e[+-]?\d+)?$/i.test(value.trim()))) {
    throw new Error(`Portfolio observation requires numeric ${field}`);
  }
  const result = Number(value);
  if (!Number.isFinite(result)) throw new Error(`Portfolio observation requires finite ${field}`);
  return result;
}

function identifier(value, field) {
  if (typeof value !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(value)) {
    throw new Error(`Portfolio observation requires valid ${field}`);
  }
  return value;
}

function receiptTimestamp(value) {
  if (!(value instanceof Date) && typeof value !== 'number' && typeof value !== 'string') {
    throw new Error('Portfolio observation requires its receipt timestamp');
  }
  if (typeof value === 'string' && !value.trim()) throw new Error('Portfolio observation requires its receipt timestamp');
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) throw new Error('Portfolio observation requires a valid receipt timestamp');
  return date.toISOString();
}

function buildPortfolioObservation(account, { timestamp, spotPrice, grossOptionsCashflow } = {}) {
  if (!account || typeof account !== 'object' || Array.isArray(account)
      || !Array.isArray(account.positions) || !Array.isArray(account.collaterals)) {
    throw new Error('Portfolio observation requires explicit positions and collaterals arrays from one subaccount response');
  }
  const observedAt = receiptTimestamp(timestamp);
  const spot = finiteNumber(spotPrice, 'spot price');
  if (spot <= 0) throw new Error('Portfolio observation requires a positive spot price');
  // Zero or negative equity is valid account evidence and must remain visible.
  const equity = finiteNumber(account.subaccount_value, 'subaccount value');
  const balances = { USDC: 0, ETH: 0 };
  const seenAssets = new Set();
  for (const collateral of account.collaterals) {
    const asset = identifier(collateral?.asset_name, 'collateral asset name');
    const amount = finiteNumber(collateral?.amount, `${asset} collateral amount`);
    if (seenAssets.has(asset)) throw new Error(`Duplicate collateral asset in portfolio observation: ${asset}`);
    seenAssets.add(asset);
    if (Object.hasOwn(balances, asset)) balances[asset] = amount;
  }

  const positions = [];
  const seenInstruments = new Set();
  let unrealized = 0;
  for (const position of account.positions) {
    const instrument = identifier(position?.instrument_name, 'position instrument name');
    const signedAmount = finiteNumber(position?.amount, `${instrument} position amount`);
    if (seenInstruments.has(instrument)) throw new Error(`Duplicate position in portfolio observation: ${instrument}`);
    seenInstruments.add(instrument);
    if (signedAmount === 0) continue;
    const direction = signedAmount > 0 ? 'long' : 'short';
    // Accept the raw signed venue amount; refuse an already-normalized position
    // whose positive absolute amount would silently reverse a short position.
    if (position.direction != null && position.direction !== direction) {
      throw new Error(`Position direction conflicts with signed amount: ${instrument}`);
    }
    const pnl = finiteNumber(position.unrealized_pnl, `${instrument} unrealized P&L`);
    unrealized += pnl;
    if (!Number.isFinite(unrealized)) throw new Error('Portfolio unrealized P&L sum is not finite');
    positions.push({ instrument, direction, amount: Math.abs(signedAmount), unrealized_pnl: pnl });
  }

  return {
    timestamp: observedAt,
    spot_price: spot,
    usdc_balance: balances.USDC,
    eth_balance: balances.ETH,
    positions_json: positions,
    total_unrealized_pnl: unrealized,
    total_realized_pnl: null,
    gross_options_cashflow: grossOptionsCashflow == null ? null : finiteNumber(grossOptionsCashflow, 'gross options cashflow'),
    portfolio_value_usd: equity,
  };
}

module.exports = { buildPortfolioObservation };
