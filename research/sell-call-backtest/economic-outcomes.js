'use strict';

const { priceForExecution } = require('./simulator');
const {
  DAY_MS,
  HOUR_MS,
  finite,
  lowerBound,
  round,
} = require('./utils');

// These are deliberately economic transformations rather than another stack of
// hand-tuned score multipliers. The model can learn their relative importance,
// while every input is known at the decision timestamp.
const ECONOMIC_FEATURE_NAMES = Object.freeze([
  'log_raw_score',
  'dte',
  'abs_delta',
  'strike_distance_pct',
  'bid_to_spot',
  'spread_to_bid',
  'implied_vol',
  'moneyness_in_expected_moves',
  'log_depth',
  'log_open_interest',
  'market_avg_spread',
  'market_skew',
  'market_oi_delta_24h_pct',
  'score_trend_24h_pct',
  'spot_return_6h',
  'spot_return_24h',
  'spot_return_72h',
  'spot_realized_move_24h',
  'spot_drawdown_from_24h_high',
]);

function priorFrameAtOrBefore(history, timestampMs) {
  const index = lowerBound(history, timestampMs + 1, (point) => point.timestamp_ms) - 1;
  return index >= 0 ? history[index] : null;
}

function attachEconomicContexts(frames = []) {
  const history = [];
  for (const frame of frames) {
    const spot = finite(frame.spot_price);
    const ret = (hours) => {
      const prior = priorFrameAtOrBefore(history, frame.timestamp_ms - hours * HOUR_MS);
      return spot > 0 && prior?.spot_price > 0 ? (spot / prior.spot_price) - 1 : null;
    };
    const day = history.filter((point) => point.timestamp_ms >= frame.timestamp_ms - DAY_MS);
    const dayWithCurrent = spot > 0
      ? [...day, { timestamp_ms: frame.timestamp_ms, spot_price: spot }]
      : day;
    let squaredLogMoves = 0;
    for (let index = 1; index < dayWithCurrent.length; index++) {
      const previous = dayWithCurrent[index - 1].spot_price;
      const current = dayWithCurrent[index].spot_price;
      if (previous > 0 && current > 0) squaredLogMoves += Math.log(current / previous) ** 2;
    }
    const dayHigh = dayWithCurrent.reduce((high, point) => Math.max(high, Number(point.spot_price || 0)), 0);
    frame.economic_context = {
      spot_return_6h: ret(6),
      spot_return_24h: ret(24),
      spot_return_72h: ret(72),
      spot_realized_move_24h: dayWithCurrent.length >= 3 ? Math.sqrt(squaredLogMoves) : null,
      spot_drawdown_from_24h_high: spot > 0 && dayHigh > 0 ? (spot / dayHigh) - 1 : null,
    };
    if (spot > 0) history.push({ timestamp_ms: frame.timestamp_ms, spot_price: spot });
  }
  return frames;
}

function economicCandidateFeatures(candidate, frame) {
  const bid = finite(candidate?.bid_price);
  const ask = finite(candidate?.ask_price);
  const spot = finite(frame?.spot_price);
  const dte = finite(candidate?.dte);
  const iv = finite(candidate?.implied_vol);
  const strikeDistance = finite(candidate?.features?.strike_distance_pct);
  const expectedMove = iv > 0 && dte > 0 ? iv * Math.sqrt(dte / 365) : null;
  const spread = bid > 0 && ask != null ? Math.max(0, ask - bid) : null;
  const context = frame?.economic_context || {};
  return {
    log_raw_score: finite(candidate?.raw_score) > 0 ? Math.log1p(candidate.raw_score) : null,
    dte,
    abs_delta: finite(candidate?.features?.abs_delta) ?? Math.abs(finite(candidate?.delta) || 0),
    strike_distance_pct: strikeDistance,
    bid_to_spot: bid > 0 && spot > 0 ? bid / spot : null,
    spread_to_bid: spread != null && bid > 0 ? Math.min(5, spread / bid) : null,
    implied_vol: iv,
    moneyness_in_expected_moves: strikeDistance != null && expectedMove > 0
      ? strikeDistance / expectedMove
      : null,
    log_depth: finite(candidate?.depth) >= 0 ? Math.log1p(candidate.depth) : null,
    log_open_interest: finite(candidate?.open_interest) >= 0 ? Math.log1p(candidate.open_interest) : null,
    market_avg_spread: finite(candidate?.features?.market_avg_spread),
    market_skew: finite(candidate?.features?.market_skew),
    market_oi_delta_24h_pct: finite(candidate?.features?.market_oi_delta_24h_pct),
    score_trend_24h_pct: finite(candidate?.features?.score_trend_24h_pct),
    spot_return_6h: finite(context.spot_return_6h),
    spot_return_24h: finite(context.spot_return_24h),
    spot_return_72h: finite(context.spot_return_72h),
    spot_realized_move_24h: finite(context.spot_realized_move_24h),
    spot_drawdown_from_24h_high: finite(context.spot_drawdown_from_24h_high),
  };
}

function normalizeEconomicOutcomeOptions(options = {}) {
  return {
    execution: ['bid_ask', 'midpoint', 'mark'].includes(options.execution) ? options.execution : 'bid_ask',
    feeBps: Math.max(0, Number(options.feeBps || 0)),
    settlementFeeBps: Math.max(0, Number(options.settlementFeeBps ?? options.feeBps ?? 0)),
    marginRate: Math.max(0, Number(options.marginRate ?? 0.15)),
    profitCapturePct: Math.min(1, Math.max(0, Number(options.profitCapturePct ?? 0.80))),
    stopLossMultiple: Number(options.stopLossMultiple) > 1 ? Number(options.stopLossMultiple) : null,
    maxHoldHours: Number(options.maxHoldHours) > 0 ? Number(options.maxHoldHours) : null,
    minBid: Math.max(0, Number(options.minBid ?? 4)),
    maxCandidatesPerFrame: Number(options.maxCandidatesPerFrame) > 0
      ? Math.floor(Number(options.maxCandidatesPerFrame))
      : null,
    severeLossOnMargin: Math.max(0, Number(options.severeLossOnMargin ?? 0.02)),
  };
}

function feeFor(value, bps) {
  return Math.abs(Number(value || 0)) * Number(bps || 0) / 10000;
}

function resolveEconomicOutcome(frames, observedIndex, candidate, rawOptions = {}) {
  const options = normalizeEconomicOutcomeOptions(rawOptions);
  const observedFrame = frames[observedIndex];
  if (!observedFrame || !candidate) return null;
  const entryPrice = priceForExecution(candidate, 'sell', options.execution);
  const spot = finite(observedFrame.spot_price);
  const expiryMs = finite(candidate.expiry) * 1000;
  if (!(entryPrice > 0) || !(spot > 0) || !(expiryMs > observedFrame.timestamp_ms)) return null;

  const margin = Math.max(spot * options.marginRate, entryPrice);
  const entryFee = feeFor(entryPrice, options.feeBps);
  const immediateBuyback = priceForExecution(candidate, 'buy', options.execution);
  let maxClosePrice = immediateBuyback != null && immediateBuyback >= 0
    ? Math.max(entryPrice, immediateBuyback)
    : entryPrice;
  let minClosePrice = entryPrice;
  let exit = null;

  for (let index = observedIndex + 1; index < frames.length; index++) {
    const frame = frames[index];
    const holdingHours = (frame.timestamp_ms - observedFrame.timestamp_ms) / HOUR_MS;
    const expired = frame.timestamp_ms >= expiryMs;
    const quote = frame.quotes.get(candidate.instrument_name);
    let closePrice = expired
      ? Math.max(Number(frame.spot_price || 0) - Number(candidate.strike || 0), 0)
      : priceForExecution(quote, 'buy', options.execution);
    if (closePrice != null && closePrice >= 0) {
      maxClosePrice = Math.max(maxClosePrice, closePrice);
      minClosePrice = Math.min(minClosePrice, closePrice);
    }

    let reason = null;
    let approximate = false;
    if (expired) reason = 'expiry';
    else if (closePrice != null && closePrice <= entryPrice * (1 - options.profitCapturePct)) reason = 'profit_capture';
    else if (closePrice != null && options.stopLossMultiple && closePrice >= entryPrice * options.stopLossMultiple) reason = 'stop_loss';
    else if (options.maxHoldHours && holdingHours >= options.maxHoldHours) reason = 'max_hold';
    if (!reason) continue;

    if (closePrice == null || closePrice < 0) {
      closePrice = Math.max(Number(frame.spot_price || 0) - Number(candidate.strike || 0), 0);
      approximate = true;
      maxClosePrice = Math.max(maxClosePrice, closePrice);
      minClosePrice = Math.min(minClosePrice, closePrice);
    }
    exit = { frame, closePrice, holdingHours, reason, approximate, settlement: expired };
    break;
  }
  // Outcomes that have not naturally matured by the dataset boundary are not labels.
  if (!exit) return null;

  const exitFee = feeFor(exit.closePrice, exit.settlement ? options.settlementFeeBps : options.feeBps);
  const pnl = entryPrice - exit.closePrice - entryFee - exitFee;
  const holdingDays = Math.max(exit.holdingHours / 24, 1 / 24);
  const pnlOnMargin = margin > 0 ? pnl / margin : null;
  const profitPerMarginDay = margin > 0 ? pnl / (margin * holdingDays) : null;
  const maxAdverse = Math.max(0, maxClosePrice - entryPrice) + entryFee;
  const maxFavorable = Math.max(0, entryPrice - minClosePrice) - entryFee;
  const adverseOnMargin = margin > 0 ? maxAdverse / margin : null;
  const adversePerMarginDay = margin > 0 ? maxAdverse / (margin * holdingDays) : null;
  const lossOnMargin = margin > 0 ? Math.max(0, -pnl) / margin : null;

  return {
    observed_at: observedFrame.timestamp,
    observed_at_ms: observedFrame.timestamp_ms,
    label_available_at: exit.frame.timestamp,
    label_available_at_ms: exit.frame.timestamp_ms,
    instrument_name: candidate.instrument_name,
    features: economicCandidateFeatures(candidate, observedFrame),
    entry_bid: candidate.bid_price,
    entry_price: entryPrice,
    close_price: exit.closePrice,
    entry_fee: entryFee,
    exit_fee: exitFee,
    spot_price: spot,
    margin_per_contract: margin,
    holding_hours: exit.holdingHours,
    holding_days: holdingDays,
    pnl,
    pnl_on_margin: pnlOnMargin,
    profit_per_margin_day: profitPerMarginDay,
    max_adverse_excursion: maxAdverse,
    max_favorable_excursion: Math.max(0, maxFavorable),
    adverse_on_margin: adverseOnMargin,
    adverse_per_margin_day: adversePerMarginDay,
    loss_on_margin: lossOnMargin,
    loss: pnl < 0 ? 1 : 0,
    severe_loss: lossOnMargin >= options.severeLossOnMargin ? 1 : 0,
    adverse_breach: adverseOnMargin >= options.severeLossOnMargin ? 1 : 0,
    tail_loss: exit.closePrice > entryPrice * 2 ? 1 : 0,
    exit_reason: exit.reason,
    approximate_exit: exit.approximate,
  };
}

function buildEconomicExamples(frames = [], rawOptions = {}) {
  const options = normalizeEconomicOutcomeOptions(rawOptions);
  attachEconomicContexts(frames);
  const examples = [];
  for (let frameIndex = 0; frameIndex < frames.length; frameIndex++) {
    const candidates = frames[frameIndex].candidates
      .filter((candidate) => candidate.bid_price >= options.minBid);
    const selected = options.maxCandidatesPerFrame
      ? candidates.slice(0, options.maxCandidatesPerFrame)
      : candidates;
    const frameWeight = selected.length > 0 ? 1 / selected.length : 0;
    for (const candidate of selected) {
      const outcome = resolveEconomicOutcome(frames, frameIndex, candidate, options);
      if (outcome) examples.push({ ...outcome, weight: frameWeight });
    }
  }
  return examples.sort((a, b) => a.observed_at_ms - b.observed_at_ms
    || a.instrument_name.localeCompare(b.instrument_name));
}

function economicExampleSummary(examples = []) {
  const independentFrames = new Set(examples.map((example) => example.observed_at_ms)).size;
  const losses = examples.filter((example) => example.loss).length;
  const severeLosses = examples.filter((example) => example.severe_loss).length;
  const adverseBreaches = examples.filter((example) => example.adverse_breach).length;
  const approximate = examples.filter((example) => example.approximate_exit).length;
  const average = (name) => examples.length
    ? examples.reduce((sum, example) => sum + Number(example[name] || 0), 0) / examples.length
    : null;
  return {
    examples: examples.length,
    independent_frames: independentFrames,
    loss_rate: examples.length ? round(losses / examples.length, 8) : null,
    severe_loss_rate: examples.length ? round(severeLosses / examples.length, 8) : null,
    adverse_breach_rate: examples.length ? round(adverseBreaches / examples.length, 8) : null,
    approximate_exit_rate: examples.length ? round(approximate / examples.length, 8) : null,
    mean_pnl: round(average('pnl'), 8),
    mean_profit_per_margin_day: round(average('profit_per_margin_day'), 8),
    mean_adverse_per_margin_day: round(average('adverse_per_margin_day'), 8),
    mean_holding_hours: round(average('holding_hours'), 4),
  };
}

module.exports = {
  ECONOMIC_FEATURE_NAMES,
  attachEconomicContexts,
  buildEconomicExamples,
  economicCandidateFeatures,
  economicExampleSummary,
  normalizeEconomicOutcomeOptions,
  resolveEconomicOutcome,
};
