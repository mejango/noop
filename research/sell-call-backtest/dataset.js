'use strict';

const { HOUR_MS, lowerBound } = require('./utils');

function findFrameAtOrAfter(frames, targetMs, maxMs = Infinity) {
  const index = lowerBound(frames, targetMs, (frame) => frame.timestamp_ms);
  if (index >= frames.length || frames[index].timestamp_ms > maxMs) return null;
  return frames[index];
}

function resolveFutureExit(frames, candidate, observedFrame, options = {}) {
  const horizonHours = Number(options.horizonHours || 72);
  const quoteWindowHours = Number(options.quoteWindowHours || 6);
  const dueMs = observedFrame.timestamp_ms + horizonHours * HOUR_MS;
  const expiryMs = Number(candidate.expiry) * 1000;

  if (Number.isFinite(expiryMs) && expiryMs <= dueMs) {
    const settlementFrame = findFrameAtOrAfter(frames, expiryMs, expiryMs + quoteWindowHours * HOUR_MS);
    if (!settlementFrame?.spot_price) return null;
    return {
      available_at_ms: settlementFrame.timestamp_ms,
      due_at_ms: expiryMs,
      future_exit_price: Math.max(settlementFrame.spot_price - Number(candidate.strike || 0), 0),
      future_spot: settlementFrame.spot_price,
      source: 'expiry_settlement',
    };
  }

  const futureFrame = findFrameAtOrAfter(frames, dueMs, dueMs + quoteWindowHours * HOUR_MS);
  if (!futureFrame) return null;
  const quote = futureFrame.quotes.get(candidate.instrument_name);
  if (!Number.isFinite(quote?.ask_price) || quote.ask_price < 0) return null;
  return {
    available_at_ms: futureFrame.timestamp_ms,
    due_at_ms: dueMs,
    future_exit_price: quote.ask_price,
    future_spot: futureFrame.spot_price,
    source: 'future_ask',
  };
}

function buildLabeledExamples(frames = [], options = {}) {
  const topPerFrame = Math.max(1, Math.floor(Number(options.topPerFrame || 8)));
  const examples = [];
  for (const frame of frames) {
    const candidates = frame.candidates.slice(0, topPerFrame);
    const frameWeight = candidates.length > 0 ? 1 / candidates.length : 0;
    for (const candidate of candidates) {
      if (!(candidate.bid_price > 0)) continue;
      const future = resolveFutureExit(frames, candidate, frame, options);
      if (!future) continue;
      const pnl = candidate.bid_price - future.future_exit_price;
      const captureReturn = pnl / candidate.bid_price;
      examples.push({
        observed_at: frame.timestamp,
        observed_at_ms: frame.timestamp_ms,
        label_available_at_ms: future.available_at_ms,
        due_at_ms: future.due_at_ms,
        instrument_name: candidate.instrument_name,
        features: { ...candidate.features },
        entry_bid: candidate.bid_price,
        future_exit_price: future.future_exit_price,
        future_spot: future.future_spot,
        outcome_source: future.source,
        pnl,
        capture_return: captureReturn,
        tail_loss: future.future_exit_price > candidate.bid_price * 2 ? 1 : 0,
        weight: frameWeight,
      });
    }
  }
  return examples.sort((a, b) => a.observed_at_ms - b.observed_at_ms || a.instrument_name.localeCompare(b.instrument_name));
}

module.exports = {
  buildLabeledExamples,
  findFrameAtOrAfter,
  resolveFutureExit,
};
