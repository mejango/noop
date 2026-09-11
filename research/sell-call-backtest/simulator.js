'use strict';

const { HOUR_MS, clamp, finite, maxDrawdown, mean, round } = require('./utils');

function normalizeConfig(config = {}) {
  return {
    startingEth: Math.max(0, Number(config.startingEth ?? 5)),
    startingCash: Number(config.startingCash ?? 0),
    callExposureCap: clamp(Number(config.callExposureCap ?? 0.45), 0, 1),
    maxOpenPositions: Math.max(1, Math.floor(Number(config.maxOpenPositions || 1))),
    maxContracts: Number(config.maxContracts) > 0 ? Number(config.maxContracts) : Infinity,
    amountStep: Math.max(0.0001, Number(config.amountStep || 0.01)),
    useQuotedDepth: config.useQuotedDepth !== false,
    execution: ['bid_ask', 'midpoint', 'mark'].includes(config.execution) ? config.execution : 'bid_ask',
    feeBps: Math.max(0, Number(config.feeBps || 0)),
    settlementFeeBps: Math.max(0, Number(config.settlementFeeBps ?? config.feeBps ?? 0)),
    marginRate: Math.max(0, Number(config.marginRate ?? 0.15)),
    marginBudgetPct: clamp(Number(config.marginBudgetPct ?? 0.45), 0, 1),
    profitCapturePct: clamp(Number(config.profitCapturePct ?? 0.80), 0, 1),
    stopLossMultiple: Number(config.stopLossMultiple) > 1 ? Number(config.stopLossMultiple) : null,
    maxHoldHours: Number(config.maxHoldHours) > 0 ? Number(config.maxHoldHours) : null,
    entryCooldownHours: Math.max(0, Number(config.entryCooldownHours || 0)),
  };
}

function priceForExecution(quote, side, execution) {
  if (!quote) return null;
  const bid = finite(quote.bid_price);
  const ask = finite(quote.ask_price);
  const mark = finite(quote.mark_price);
  if (execution === 'midpoint' && bid != null && ask != null) return (bid + ask) / 2;
  if (execution === 'mark' && mark != null && mark >= 0) return mark;
  if (side === 'sell') return bid;
  return ask;
}

function feeFor(value, bps) {
  return Math.abs(value) * Number(bps || 0) / 10000;
}

function floorAmount(value, step) {
  if (!(value > 0)) return 0;
  return Math.floor((value + 1e-12) / step) * step;
}

function optionLiability(position, frame, execution) {
  if (frame.timestamp_ms >= position.expiry_ms) {
    return Math.max(Number(frame.spot_price || 0) - position.strike, 0) * position.quantity;
  }
  const quote = frame.quotes.get(position.instrument_name);
  const closePrice = priceForExecution(quote, 'buy', execution);
  if (closePrice != null && closePrice >= 0) return closePrice * position.quantity;
  return position.entry_price * position.quantity;
}

function portfolioNav(account, positions, frame, execution) {
  const ethValue = account.eth * Number(frame.spot_price || 0);
  const liabilities = positions.reduce((sum, position) => sum + optionLiability(position, frame, execution), 0);
  return account.cash + ethValue - liabilities;
}

function closePosition({ account, position, frame, config, reason, forcedPrice = null, settlement = false }) {
  const quote = frame.quotes.get(position.instrument_name);
  const quantity = settlement || !config.useQuotedDepth
    ? position.quantity
    : floorAmount(Math.min(position.quantity, Math.max(0, finite(quote?.ask_amount) ?? 0)), config.amountStep);
  if (!(quantity > 0)) return null;
  const fraction = quantity / position.quantity;
  const entryGross = position.entry_gross * fraction;
  const entryFee = position.entry_fee * fraction;
  const marginReserved = position.margin_reserved * fraction;
  let closePrice = forcedPrice;
  let approximate = false;
  if (closePrice == null) closePrice = priceForExecution(quote, 'buy', config.execution);
  if (!settlement && config.useQuotedDepth && !(closePrice > 0)) return null;
  if (closePrice == null || closePrice < 0) {
    closePrice = Math.max(Number(frame.spot_price || 0) - position.strike, 0);
    approximate = true;
  }
  const closeGross = closePrice * quantity;
  const closeFee = feeFor(closeGross, settlement ? config.settlementFeeBps : config.feeBps);
  account.cash -= closeGross + closeFee;
  const pnl = entryGross - closeGross - entryFee - closeFee;
  return {
    instrument_name: position.instrument_name,
    opened_at: position.opened_at,
    closed_at: frame.timestamp,
    quantity,
    remaining_quantity: Math.max(0, round(position.quantity - quantity, 12)),
    strike: position.strike,
    expiry: new Date(position.expiry_ms).toISOString(),
    entry_price: position.entry_price,
    close_price: closePrice,
    entry_gross: entryGross,
    close_gross: closeGross,
    entry_fee: entryFee,
    exit_fee: closeFee,
    fees: entryFee + closeFee,
    pnl,
    return_on_premium: entryGross > 0 ? pnl / entryGross : null,
    margin_reserved: marginReserved,
    holding_hours: (frame.timestamp_ms - position.opened_at_ms) / HOUR_MS,
    reason,
    tail_loss: closePrice > position.entry_price * 2,
    approximate_exit: approximate,
    model_version: position.model_version,
    entry_score: position.entry_score,
    entry_diagnostics: position.entry_diagnostics,
  };
}

// Keep one trade per entry so partial fills do not inflate trade counts or wins.
function summarizeExitFills(fills) {
  const byEntry = new Map();
  for (const fill of fills) {
    const key = `${fill.instrument_name}:${fill.opened_at}`;
    let trade = byEntry.get(key);
    if (!trade) {
      trade = { ...fill, exit_fills: [] };
      for (const field of ['quantity', 'entry_gross', 'close_gross', 'entry_fee', 'exit_fee', 'fees', 'pnl', 'margin_reserved', 'holding_hours']) trade[field] = 0;
      byEntry.set(key, trade);
    }
    for (const field of ['quantity', 'entry_gross', 'close_gross', 'entry_fee', 'exit_fee', 'fees', 'pnl', 'margin_reserved']) trade[field] += fill[field];
    trade.holding_hours += fill.holding_hours * fill.quantity;
    trade.closed_at = fill.closed_at;
    trade.reason = fill.reason;
    trade.remaining_quantity = fill.remaining_quantity;
    trade.tail_loss = trade.tail_loss || fill.tail_loss;
    trade.approximate_exit = trade.approximate_exit || fill.approximate_exit;
    trade.exit_fills.push(fill);
  }
  return [...byEntry.values()].map((trade) => ({
    ...trade,
    closed: trade.remaining_quantity === 0,
    close_price: trade.close_gross / trade.quantity,
    return_on_premium: trade.entry_gross > 0 ? trade.pnl / trade.entry_gross : null,
    holding_hours: trade.holding_hours / trade.quantity,
  }));
}

function runBacktest(frames = [], policy, rawConfig = {}) {
  if (!Array.isArray(frames) || frames.length === 0) throw new Error('cannot backtest without historical frames');
  if (!policy || typeof policy.select !== 'function') throw new Error('policy must implement select()');
  const config = normalizeConfig(rawConfig);
  const account = { cash: config.startingCash, eth: config.startingEth };
  const positions = [];
  const exitFills = [];
  const equity = [];
  let totalPremium = 0;
  let totalFees = 0;
  let maxMarginUsed = 0;
  let lastEntryAtMs = -Infinity;
  const firstSpot = Number(frames[0].spot_price || 0);
  if (!(firstSpot > 0)) throw new Error('first historical frame has no valid spot price');
  const startingNav = account.cash + account.eth * firstSpot;

  for (let frameIndex = 0; frameIndex < frames.length; frameIndex++) {
    const frame = frames[frameIndex];
    const isFinalFrame = frameIndex === frames.length - 1;
    if (!(frame.spot_price > 0)) continue;
    if (typeof policy.onFrame === 'function') policy.onFrame(frame);

    for (let index = positions.length - 1; index >= 0; index--) {
      const position = positions[index];
      const quote = frame.quotes.get(position.instrument_name);
      const closePrice = priceForExecution(quote, 'buy', config.execution);
      let reason = null;
      let settlementPrice = null;
      let settlement = false;
      if (frame.timestamp_ms >= position.expiry_ms) {
        reason = 'expiry';
        settlementPrice = Math.max(frame.spot_price - position.strike, 0);
        settlement = true;
      } else if (closePrice != null && closePrice <= position.entry_price * (1 - config.profitCapturePct)) {
        reason = 'profit_capture';
      } else if (closePrice != null && config.stopLossMultiple && closePrice >= position.entry_price * config.stopLossMultiple) {
        reason = 'stop_loss';
      } else if (config.maxHoldHours && frame.timestamp_ms - position.opened_at_ms >= config.maxHoldHours * HOUR_MS) {
        reason = 'max_hold';
      } else if (isFinalFrame) {
        reason = 'end_of_backtest';
      }
      if (!reason) continue;
      const trade = closePosition({
        account,
        position,
        frame,
        config,
        reason,
        forcedPrice: settlementPrice,
        settlement,
      });
      if (!trade) continue;
      totalFees += trade.exit_fee;
      exitFills.push(trade);
      position.quantity = trade.remaining_quantity;
      position.entry_gross -= trade.entry_gross;
      position.entry_fee -= trade.entry_fee;
      position.margin_reserved -= trade.margin_reserved;
      if (position.quantity === 0) positions.splice(index, 1);
    }

    const openExposure = positions.reduce((sum, position) => sum + position.quantity, 0);
    const exposureAvailable = Math.max(0, account.eth * config.callExposureCap - openExposure);
    const currentMargin = positions.reduce((sum, position) => sum + position.margin_reserved, 0);
    const navBeforeEntry = portfolioNav(account, positions, frame, config.execution);
    const marginAvailable = Math.max(0, navBeforeEntry * config.marginBudgetPct - currentMargin);
    const canEnter = !isFinalFrame
      && positions.length < config.maxOpenPositions
      && exposureAvailable >= config.amountStep
      && marginAvailable > 0
      && frame.timestamp_ms - lastEntryAtMs >= config.entryCooldownHours * HOUR_MS;

    if (canEnter) {
      const openNames = new Set(positions.map((position) => position.instrument_name));
      const candidates = frame.candidates.filter((candidate) => !openNames.has(candidate.instrument_name));
      const selection = policy.select({ frame, candidates, positions: [...positions], account: { ...account } });
      const candidate = selection?.candidate;
      if (candidate) {
        const entryPrice = priceForExecution(candidate, 'sell', config.execution);
        const marginPerContract = Math.max(frame.spot_price * config.marginRate, Number(entryPrice || 0));
        // Unknown or empty depth cannot establish executable entry liquidity.
        const quotedDepth = config.useQuotedDepth
          ? Math.max(0, finite(candidate.bid_amount) ?? 0)
          : Infinity;
        const quantity = floorAmount(Math.min(
          exposureAvailable,
          config.maxContracts,
          quotedDepth,
          marginPerContract > 0 ? marginAvailable / marginPerContract : 0,
        ), config.amountStep);
        if (entryPrice > 0 && quantity >= config.amountStep) {
          const entryGross = entryPrice * quantity;
          const entryFee = feeFor(entryGross, config.feeBps);
          const marginReserved = quantity * marginPerContract;
          account.cash += entryGross - entryFee;
          totalPremium += entryGross;
          totalFees += entryFee;
          positions.push({
            instrument_name: candidate.instrument_name,
            strike: Number(candidate.strike || 0),
            expiry_ms: Number(candidate.expiry) * 1000,
            quantity,
            entry_price: entryPrice,
            entry_gross: entryGross,
            entry_fee: entryFee,
            margin_reserved: marginReserved,
            opened_at: frame.timestamp,
            opened_at_ms: frame.timestamp_ms,
            model_version: selection.model_version || policy.name,
            entry_score: selection.score,
            entry_diagnostics: selection.diagnostics || null,
          });
          lastEntryAtMs = frame.timestamp_ms;
          maxMarginUsed = Math.max(maxMarginUsed, currentMargin + marginReserved);
        }
      }
    }

    const marginUsed = positions.reduce((sum, position) => sum + position.margin_reserved, 0);
    equity.push({
      timestamp: frame.timestamp,
      nav: portfolioNav(account, positions, frame, config.execution),
      spot_price: frame.spot_price,
      cash: account.cash,
      open_positions: positions.length,
      margin_used: marginUsed,
    });
  }

  const finalFrame = frames[frames.length - 1];
  // Final-frame liquidity was already consumed once above. Unfilled inventory
  // remains marked in NAV instead of being recorded as another fictitious fill.
  const endingNav = portfolioNav(account, positions, finalFrame, config.execution);
  const endingMargin = positions.reduce((sum, position) => sum + position.margin_reserved, 0);
  if (equity.length === 0 || equity[equity.length - 1].timestamp !== finalFrame.timestamp) {
    equity.push({ timestamp: finalFrame.timestamp, nav: endingNav, spot_price: finalFrame.spot_price, cash: account.cash, open_positions: positions.length, margin_used: endingMargin });
  } else {
    equity[equity.length - 1] = { ...equity[equity.length - 1], nav: endingNav, cash: account.cash, open_positions: positions.length, margin_used: endingMargin };
  }
  const baselineEndingNav = config.startingCash + config.startingEth * finalFrame.spot_price;
  const trades = summarizeExitFills(exitFills);
  const closedTrades = trades.filter((trade) => trade.closed);
  const wins = closedTrades.filter((trade) => trade.pnl > 0).length;
  const realizedPnl = trades.reduce((sum, trade) => sum + trade.pnl, 0);
  return {
    policy: policy.name,
    description: policy.description || null,
    config,
    started_at: frames[0].timestamp,
    ended_at: finalFrame.timestamp,
    frames: frames.length,
    starting_nav: round(startingNav, 6),
    ending_nav: round(endingNav, 6),
    total_return: round((endingNav / startingNav) - 1, 8),
    eth_baseline_ending_nav: round(baselineEndingNav, 6),
    eth_baseline_return: round((baselineEndingNav / startingNav) - 1, 8),
    overlay_pnl: round(endingNav - baselineEndingNav, 6),
    realized_call_pnl: round(realizedPnl, 6),
    unrealized_call_pnl: round(endingNav - baselineEndingNav - realizedPnl, 6),
    open_positions: positions.length,
    open_contracts: round(positions.reduce((sum, position) => sum + position.quantity, 0), 12),
    ending_margin_used: round(endingMargin, 6),
    total_premium_received: round(totalPremium, 6),
    total_fees: round(totalFees, 6),
    max_margin_used: round(maxMarginUsed, 6),
    return_on_max_margin: maxMarginUsed > 0 ? round(realizedPnl / maxMarginUsed, 8) : null,
    max_drawdown: round(maxDrawdown(equity), 8),
    trades: closedTrades.length,
    exit_fills: exitFills.length,
    wins,
    win_rate: closedTrades.length > 0 ? round(wins / closedTrades.length, 8) : null,
    tail_losses: trades.filter((trade) => trade.tail_loss).length,
    approximate_exits: trades.filter((trade) => trade.approximate_exit).length,
    average_holding_hours: round(mean(closedTrades.map((trade) => trade.holding_hours)), 4),
    trade_log: trades,
    equity_curve: equity,
    model_artifacts: typeof policy.getArtifacts === 'function' ? policy.getArtifacts() : [],
  };
}

module.exports = {
  closePosition,
  floorAmount,
  normalizeConfig,
  optionLiability,
  portfolioNav,
  priceForExecution,
  runBacktest,
};
