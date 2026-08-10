'use strict';

const { DAY_MS, mean, median, round } = require('./utils');
const { normalizeDteScore } = require('./dte-normalization');

const REFERENCE_DTE = 8.5;
const PRODUCTION_EXPONENT = 0.12;

function finitePositive(value) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : null;
}

function scoreSimpleCall(candidate, frame, options = {}) {
  const family = options.family || 'production';
  const rawScore = finitePositive(candidate?.raw_score);
  const dte = finitePositive(candidate?.dte);
  const bid = finitePositive(candidate?.bid_price);
  const ask = finitePositive(candidate?.ask_price);
  const delta = finitePositive(Math.abs(Number(candidate?.delta)));
  const spot = finitePositive(frame?.spot_price);
  if (rawScore == null || dte == null || bid == null || delta == null) return 0;

  const referenceDte = finitePositive(options.referenceDte) || REFERENCE_DTE;
  const exponent = Number.isFinite(Number(options.exponent)) ? Number(options.exponent) : PRODUCTION_EXPONENT;
  const productionScore = normalizeDteScore(rawScore, dte, { referenceDte, exponent });

  if (family === 'production') return productionScore;

  if (family === 'additive_dte') {
    const slope = Number(options.additiveSlope || 0);
    return Math.max(0, rawScore - slope * (dte - referenceDte));
  }

  if (family === 'spread_haircut') {
    if (ask == null) return 0;
    const haircut = Math.max(0, Number(options.spreadHaircut ?? 0.5));
    const executablePremium = Math.max(0, bid - haircut * Math.max(0, ask - bid));
    return normalizeDteScore(executablePremium / delta, dte, { referenceDte, exponent });
  }

  if (family === 'quote_quality') {
    if (ask == null) return 0;
    return productionScore * Math.sqrt(Math.min(1, bid / ask));
  }

  if (family === 'expected_move') {
    const impliedVol = finitePositive(candidate?.implied_vol);
    if (spot == null || impliedVol == null) return 0;
    const expectedMove = spot * impliedVol * Math.sqrt(dte / 365);
    return expectedMove > 0 ? rawScore / expectedMove : 0;
  }

  if (family === 'expected_move_net') {
    const impliedVol = finitePositive(candidate?.implied_vol);
    if (spot == null || impliedVol == null || ask == null) return 0;
    const executablePremium = Math.max(0, bid - 0.5 * Math.max(0, ask - bid));
    const expectedMove = spot * impliedVol * Math.sqrt(dte / 365);
    return expectedMove > 0 ? (executablePremium / delta) / expectedMove : 0;
  }

  if (family === 'balanced_expected_move') {
    const impliedVol = finitePositive(candidate?.implied_vol);
    if (spot == null || impliedVol == null || ask == null) return 0;
    const executablePremium = Math.max(0, bid - 0.5 * Math.max(0, ask - bid));
    const netRaw = executablePremium / delta;
    const expectedMove = spot * impliedVol * Math.sqrt(dte / 365);
    const netMoveEfficiency = expectedMove > 0 ? netRaw / expectedMove : 0;
    return Math.sqrt(Math.max(0, productionScore * netMoveEfficiency));
  }

  if (family === 'premium_rate') {
    if (spot == null) return 0;
    return (rawScore / spot) * Math.sqrt(365 / dte);
  }

  if (family === 'trailing_dte_relative') {
    const bucket = Math.floor(dte);
    const baseline = finitePositive(frame?.simple_score_context?.raw_dte_median?.[bucket]);
    return baseline == null ? 0 : rawScore / baseline;
  }

  if (family === 'trailing_dte_net_relative') {
    if (ask == null) return 0;
    const bucket = Math.floor(dte);
    const baseline = finitePositive(frame?.simple_score_context?.net_dte_median?.[bucket]);
    const netRaw = Math.max(0, bid - 0.5 * Math.max(0, ask - bid)) / delta;
    return baseline == null ? 0 : netRaw / baseline;
  }

  throw new Error(`unknown simple call score family: ${family}`);
}

function attachTrailingDteBenchmarks(frames = [], options = {}) {
  const lookbackMs = Math.max(1, Number(options.lookbackDays ?? 30)) * DAY_MS;
  const minSamples = Math.max(1, Math.floor(Number(options.minSamples ?? 24)));
  const minDte = Number(options.minDte ?? 5);
  const maxDte = Number(options.maxDte ?? 12);
  const minBid = Number(options.minBid ?? 4);
  const rawHistory = new Map();
  const netHistory = new Map();

  for (const frame of frames) {
    const cutoff = frame.timestamp_ms - lookbackMs;
    for (const history of [rawHistory, netHistory]) {
      for (const [bucket, points] of history) {
        let start = 0;
        while (start < points.length && points[start].timestamp_ms < cutoff) start++;
        if (start > 0) points.splice(0, start);
        if (points.length === 0) history.delete(bucket);
      }
    }

    const rawDteMedian = {};
    const netDteMedian = {};
    for (let bucket = Math.floor(minDte); bucket <= Math.floor(maxDte); bucket++) {
      const rawPoints = rawHistory.get(bucket) || [];
      const netPoints = netHistory.get(bucket) || [];
      if (rawPoints.length >= minSamples) rawDteMedian[bucket] = median(rawPoints.map((point) => point.score));
      if (netPoints.length >= minSamples) netDteMedian[bucket] = median(netPoints.map((point) => point.score));
    }
    frame.simple_score_context = {
      raw_dte_median: rawDteMedian,
      net_dte_median: netDteMedian,
      lookback_days: lookbackMs / DAY_MS,
    };

    const bestRawByBucket = new Map();
    const bestNetByBucket = new Map();
    for (const candidate of frame.candidates) {
      if (!(candidate.bid_price >= minBid) || candidate.dte < minDte || candidate.dte > maxDte) continue;
      const bucket = Math.floor(candidate.dte);
      const raw = finitePositive(candidate.raw_score);
      const ask = finitePositive(candidate.ask_price);
      const delta = finitePositive(Math.abs(Number(candidate.delta)));
      if (raw != null) bestRawByBucket.set(bucket, Math.max(bestRawByBucket.get(bucket) || 0, raw));
      if (ask != null && delta != null) {
        const net = Math.max(0, candidate.bid_price - 0.5 * Math.max(0, ask - candidate.bid_price)) / delta;
        if (net > 0) bestNetByBucket.set(bucket, Math.max(bestNetByBucket.get(bucket) || 0, net));
      }
    }
    for (const [bucket, score] of bestRawByBucket) {
      if (!rawHistory.has(bucket)) rawHistory.set(bucket, []);
      rawHistory.get(bucket).push({ timestamp_ms: frame.timestamp_ms, score });
    }
    for (const [bucket, score] of bestNetByBucket) {
      if (!netHistory.has(bucket)) netHistory.set(bucket, []);
      netHistory.get(bucket).push({ timestamp_ms: frame.timestamp_ms, score });
    }
  }
  return frames;
}

function makeSimpleCallScorePolicy(options = {}, name = null) {
  const config = {
    ...options,
    family: options.family || 'production',
    minBid: Number(options.minBid ?? 4),
    minScore: Number(options.minScore ?? 65),
    minDte: Number(options.minDte ?? 5),
    maxDte: Number(options.maxDte ?? 12),
  };
  return {
    name: name || `simple_${config.family}`,
    description: `Simple ${config.family} call score with floor ${config.minScore}`,
    select({ frame, candidates }) {
      const ranked = candidates
        .filter((candidate) => candidate.bid_price >= config.minBid
          && candidate.dte >= config.minDte
          && candidate.dte <= config.maxDte)
        .map((candidate) => ({ candidate, score: scoreSimpleCall(candidate, frame, config) }))
        .filter((item) => item.score >= config.minScore)
        .sort((a, b) => b.score - a.score || b.candidate.raw_score - a.candidate.raw_score);
      if (ranked.length === 0) return null;
      return {
        candidate: ranked[0].candidate,
        score: ranked[0].score,
        model_version: `simple-call-${config.family}-v1`,
        diagnostics: {
          family: config.family,
          score: ranked[0].score,
          raw_score: ranked[0].candidate.raw_score,
          dte: ranked[0].candidate.dte,
          config,
        },
      };
    },
    getArtifacts() { return []; },
  };
}

function topSimpleScoreSeries(frames = [], options = {}) {
  return frames.map((frame) => {
    const ranked = frame.candidates
      .filter((candidate) => candidate.bid_price >= Number(options.minBid ?? 4)
        && candidate.dte >= Number(options.minDte ?? 5)
        && candidate.dte <= Number(options.maxDte ?? 12))
      .map((candidate) => ({ candidate, score: scoreSimpleCall(candidate, frame, options) }))
      .filter((item) => item.score > 0)
      .sort((a, b) => b.score - a.score || b.candidate.raw_score - a.candidate.raw_score);
    if (ranked.length === 0) return null;
    const nearestExpiry = Math.min(...ranked.map((item) => Number(item.candidate.expiry)).filter((expiry) => expiry > 0));
    return {
      timestamp: frame.timestamp,
      timestamp_ms: frame.timestamp_ms,
      score: ranked[0].score,
      dte: ranked[0].candidate.dte,
      raw_score: ranked[0].candidate.raw_score,
      nearest_expiry: Number.isFinite(nearestExpiry) ? nearestExpiry : null,
    };
  }).filter(Boolean);
}

function analyzeSimpleScoreRollover(frames = [], options = {}) {
  const series = topSimpleScoreSeries(frames, options);
  const jumps = [];
  let priorNearest = null;
  for (let index = 0; index < series.length; index++) {
    const point = series[index];
    if (priorNearest != null && point.nearest_expiry > priorNearest) {
      const priorMedian = median(series.slice(Math.max(0, index - 24), index).map((item) => item.score));
      const throughMs = point.timestamp_ms + 6 * 60 * 60 * 1000;
      const postScores = [];
      for (let future = index; future < series.length && series[future].timestamp_ms <= throughMs; future++) {
        postScores.push(series[future].score);
      }
      if (priorMedian > 0 && postScores.length > 0) {
        jumps.push(Math.max(0, Math.max(...postScores) / priorMedian - 1));
      }
    }
    if (point.nearest_expiry != null && (priorNearest == null || point.nearest_expiry > priorNearest)) {
      priorNearest = point.nearest_expiry;
    }
  }
  return {
    rollover_events: jumps.length,
    mean_positive_peak_jump_pct: round(mean(jumps), 8),
    median_positive_peak_jump_pct: round(median(jumps), 8),
    max_positive_peak_jump_pct: jumps.length ? round(Math.max(...jumps), 8) : null,
  };
}

function constantMaturityPoint(frame, options = {}) {
  const targetDte = Number(options.targetDte ?? REFERENCE_DTE);
  const minDte = Number(options.minDte ?? 1);
  const maxDte = Number(options.maxDte ?? 21);
  const minBid = Number(options.minBid ?? 4);
  const family = options.family || 'production';
  const byExpiry = new Map();
  for (const candidate of frame.candidates) {
    if (!(candidate.bid_price >= minBid) || candidate.dte < minDte || candidate.dte > maxDte) continue;
    const score = scoreSimpleCall(candidate, frame, { ...options, family });
    if (!(score > 0)) continue;
    const expiry = Number(candidate.expiry);
    const prior = byExpiry.get(expiry);
    if (!prior || score > prior.score) byExpiry.set(expiry, { expiry, dte: candidate.dte, score });
  }
  const points = [...byExpiry.values()].sort((a, b) => a.dte - b.dte);
  if (points.length === 0) return null;
  const lower = [...points].reverse().find((point) => point.dte <= targetDte) || null;
  const upper = points.find((point) => point.dte >= targetDte) || null;
  if (!lower || !upper) return null;
  const span = upper.dte - lower.dte;
  const weight = span > 0 ? (targetDte - lower.dte) / span : 0;
  return {
    timestamp: frame.timestamp,
    timestamp_ms: frame.timestamp_ms,
    score: lower.score + weight * (upper.score - lower.score),
    target_dte: targetDte,
    lower_dte: lower.dte,
    upper_dte: upper.dte,
    lower_expiry: lower.expiry,
    upper_expiry: upper.expiry,
    interpolation_weight: weight,
  };
}

function constantMaturitySeries(frames = [], options = {}) {
  return frames.map((frame) => constantMaturityPoint(frame, options)).filter(Boolean);
}

function analyzeConstantMaturityRollover(frames = [], options = {}) {
  const series = constantMaturitySeries(frames, options);
  const jumps = [];
  let priorPair = null;
  for (let index = 0; index < series.length; index++) {
    const point = series[index];
    const pair = `${point.lower_expiry}|${point.upper_expiry}`;
    if (priorPair != null && pair !== priorPair) {
      const priorMedian = median(series.slice(Math.max(0, index - 24), index).map((item) => item.score));
      const throughMs = point.timestamp_ms + 6 * 60 * 60 * 1000;
      const postScores = [];
      for (let future = index; future < series.length && series[future].timestamp_ms <= throughMs; future++) {
        postScores.push(series[future].score);
      }
      if (priorMedian > 0 && postScores.length > 0) {
        jumps.push(Math.max(0, Math.max(...postScores) / priorMedian - 1));
      }
    }
    priorPair = pair;
  }
  return {
    points: series.length,
    coverage_pct: round(series.length / frames.length, 8),
    rollover_events: jumps.length,
    mean_positive_peak_jump_pct: round(mean(jumps), 8),
    median_positive_peak_jump_pct: round(median(jumps), 8),
    max_positive_peak_jump_pct: jumps.length ? round(Math.max(...jumps), 8) : null,
  };
}

function fitAdditiveDteSlope(frames = [], options = {}) {
  const points = topSimpleScoreSeries(frames, { ...options, family: 'production', exponent: 0 })
    .map((point) => ({ x: point.dte, y: point.raw_score }));
  if (points.length < 2) return 0;
  const xMean = mean(points.map((point) => point.x));
  const yMean = mean(points.map((point) => point.y));
  const covariance = mean(points.map((point) => (point.x - xMean) * (point.y - yMean)));
  const variance = mean(points.map((point) => (point.x - xMean) ** 2));
  return variance > 0 ? Math.max(0, covariance / variance) : 0;
}

module.exports = {
  PRODUCTION_EXPONENT,
  REFERENCE_DTE,
  analyzeSimpleScoreRollover,
  analyzeConstantMaturityRollover,
  attachTrailingDteBenchmarks,
  constantMaturityPoint,
  constantMaturitySeries,
  fitAdditiveDteSlope,
  makeSimpleCallScorePolicy,
  scoreSimpleCall,
  topSimpleScoreSeries,
};
