'use strict';

const { DAY_MS, finite, lowerBound, mean } = require('./utils');

const CALL_DELTA_RANGE = [0.04, 0.12];
const CALL_DTE_RANGE = [5, 12];
const PUT_DELTA_RANGE = [-0.12, -0.02];
const PUT_DTE_RANGE = [45, 75];

const FEATURE_NAMES = Object.freeze([
  'raw_score',
  'dte',
  'abs_delta',
  'strike_distance_pct',
  'bid_price',
  'spread_pct',
  'depth',
  'implied_vol',
  'open_interest',
  'market_avg_spread',
  'market_best_put_score',
  'market_skew',
  'market_oi_delta_24h_pct',
  'score_trend_24h_pct',
]);

function optionType(option) {
  if (option?.option_type === 'C' || option?.option_type === 'P') return option.option_type;
  if (String(option?.instrument_name || '').endsWith('-C')) return 'C';
  if (String(option?.instrument_name || '').endsWith('-P')) return 'P';
  return null;
}

function computeDte(option, timestampMs) {
  const expirySeconds = finite(option?.expiry);
  if (!(expirySeconds > 0) || !Number.isFinite(timestampMs)) return null;
  return Math.max(0, ((expirySeconds * 1000) - timestampMs) / DAY_MS);
}

function spreadPct(option) {
  const bid = finite(option?.bid_price);
  const ask = finite(option?.ask_price);
  const mark = finite(option?.mark_price);
  return bid != null && ask != null && mark > 0 ? (ask - bid) / mark : null;
}

function isEligibleCall(option, timestampMs) {
  const delta = finite(option?.delta);
  const dte = computeDte(option, timestampMs);
  return optionType(option) === 'C'
    && finite(option?.bid_price) > 0
    && finite(option?.ask_price) > 0
    && delta >= CALL_DELTA_RANGE[0]
    && delta <= CALL_DELTA_RANGE[1]
    && dte >= CALL_DTE_RANGE[0]
    && dte <= CALL_DTE_RANGE[1];
}

function isEligiblePut(option, timestampMs) {
  const delta = finite(option?.delta);
  const dte = computeDte(option, timestampMs);
  return optionType(option) === 'P'
    && finite(option?.ask_price) > 0
    && delta >= PUT_DELTA_RANGE[0]
    && delta <= PUT_DELTA_RANGE[1]
    && dte >= PUT_DTE_RANGE[0]
    && dte <= PUT_DTE_RANGE[1];
}

function normalizeOption(option) {
  return {
    instrument_name: String(option?.instrument_name || ''),
    option_type: optionType(option),
    strike: finite(option?.strike),
    expiry: finite(option?.expiry),
    delta: finite(option?.delta),
    bid_price: finite(option?.bid_price),
    ask_price: finite(option?.ask_price),
    mark_price: finite(option?.mark_price),
    bid_amount: finite(option?.bid_amount),
    ask_amount: finite(option?.ask_amount),
    implied_vol: finite(option?.implied_vol),
    open_interest: finite(option?.open_interest),
    index_price: finite(option?.index_price),
  };
}

function priorPointAtOrBefore(history, timestampMs) {
  const index = lowerBound(history, timestampMs + 1, (point) => point.timestamp_ms) - 1;
  return index >= 0 ? history[index] : null;
}

function enrichFrames(rawFrames = []) {
  const frames = rawFrames
    .map((frame) => ({
      timestamp: frame.timestamp,
      timestamp_ms: Number.isFinite(frame.timestamp_ms) ? frame.timestamp_ms : new Date(frame.timestamp).getTime(),
      spot_price: finite(frame.spot_price),
      options: (frame.options || []).map(normalizeOption),
    }))
    .filter((frame) => Number.isFinite(frame.timestamp_ms))
    .sort((a, b) => a.timestamp_ms - b.timestamp_ms);

  const history = [];
  for (const frame of frames) {
    const indexedSpots = frame.options
      .map((option) => option.index_price)
      .filter((spot) => spot >= 100 && spot <= 20000);
    if (!(frame.spot_price > 0)) frame.spot_price = mean(indexedSpots);

    const calls = frame.options.filter((option) => isEligibleCall(option, frame.timestamp_ms));
    const puts = frame.options.filter((option) => isEligiblePut(option, frame.timestamp_ms));
    const valueOptions = [...calls, ...puts];
    const bestRawScore = Math.max(0, ...calls.map((option) => option.bid_price / Math.abs(option.delta)));
    const bestPutScore = Math.max(0, ...puts.map((option) => Math.abs(option.delta) / option.ask_price));
    const marketAvgSpread = mean(valueOptions.map(spreadPct));
    const callIv = mean(calls.map((option) => option.implied_vol));
    const putIv = mean(puts.map((option) => option.implied_vol));
    const marketSkew = putIv != null && callIv != null ? putIv - callIv : null;
    const totalOi = frame.options
      .map((option) => finite(option.open_interest))
      .filter((value) => value != null && value > 0)
      .reduce((sum, value) => sum + value, 0);

    const recentScores = history
      .filter((point) => point.timestamp_ms >= frame.timestamp_ms - DAY_MS)
      .map((point) => point.best_raw_score)
      .filter((score) => score > 0);
    const priorScoreMean = mean(recentScores);
    const scoreTrend24hPct = bestRawScore > 0 && priorScoreMean > 0
      ? ((bestRawScore - priorScoreMean) / priorScoreMean) * 100
      : null;
    const oiPrior = priorPointAtOrBefore(history, frame.timestamp_ms - DAY_MS);
    const marketOiDelta24hPct = totalOi > 0 && oiPrior?.total_oi > 0
      ? ((totalOi - oiPrior.total_oi) / oiPrior.total_oi) * 100
      : null;

    frame.market = {
      market_avg_spread: marketAvgSpread,
      market_best_call_score: bestRawScore || null,
      market_best_put_score: bestPutScore || null,
      market_call_iv: callIv,
      market_put_iv: putIv,
      market_skew: marketSkew,
      market_total_oi: totalOi || null,
      market_oi_delta_24h_pct: marketOiDelta24hPct,
      score_trend_24h_pct: scoreTrend24hPct,
    };
    frame.quotes = new Map(frame.options.map((option) => [option.instrument_name, option]));
    frame.candidates = calls.map((option) => {
      const dte = computeDte(option, frame.timestamp_ms);
      const rawScore = option.bid_price / Math.abs(option.delta);
      const candidateSpread = spreadPct(option);
      const depth = (option.bid_amount || 0) + (option.ask_amount || 0);
      const features = {
        raw_score: rawScore,
        dte,
        abs_delta: Math.abs(option.delta),
        strike_distance_pct: frame.spot_price > 0 && option.strike > 0
          ? (option.strike / frame.spot_price) - 1
          : null,
        bid_price: option.bid_price,
        spread_pct: candidateSpread,
        depth,
        implied_vol: option.implied_vol,
        open_interest: option.open_interest,
        market_avg_spread: marketAvgSpread,
        market_best_put_score: bestPutScore || null,
        market_skew: marketSkew,
        market_oi_delta_24h_pct: marketOiDelta24hPct,
        score_trend_24h_pct: scoreTrend24hPct,
      };
      return {
        ...option,
        dte,
        raw_score: rawScore,
        spread_pct: candidateSpread,
        depth,
        features,
      };
    });
    frame.candidates.sort((a, b) => b.raw_score - a.raw_score || b.depth - a.depth);
    history.push({ timestamp_ms: frame.timestamp_ms, best_raw_score: bestRawScore, total_oi: totalOi });
  }
  return frames;
}

module.exports = {
  CALL_DELTA_RANGE,
  CALL_DTE_RANGE,
  FEATURE_NAMES,
  PUT_DELTA_RANGE,
  PUT_DTE_RANGE,
  computeDte,
  enrichFrames,
  isEligibleCall,
  isEligiblePut,
  optionType,
  spreadPct,
};
