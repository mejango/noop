'use strict';

const { DAY_MS, mean, median, round } = require('./utils');

const DEFAULT_DTE_REFERENCE = 8.5;

function normalizeDteScore(rawScore, dte, options = {}) {
  const score = Number(rawScore);
  const days = Number(dte);
  const referenceDte = Number(options.referenceDte ?? DEFAULT_DTE_REFERENCE);
  const exponent = Number(options.exponent ?? 0.25);
  if (!(score > 0) || !(days > 0) || !(referenceDte > 0) || !Number.isFinite(exponent)) return 0;
  return score * Math.pow(referenceDte / days, exponent);
}

function makeDteNormalizedRawPolicy(options = {}, name = 'dte_normalized_raw') {
  const config = {
    minBid: Number(options.minBid ?? 4),
    minScore: Number(options.minScore ?? 65),
    referenceDte: Number(options.referenceDte ?? DEFAULT_DTE_REFERENCE),
    exponent: Number(options.exponent ?? 0.25),
  };
  return {
    name,
    description: `Raw bid/delta normalized to ${config.referenceDte} DTE with exponent ${config.exponent}`,
    select({ candidates }) {
      const ranked = candidates
        .filter((candidate) => candidate.bid_price >= config.minBid)
        .map((candidate) => ({
          candidate,
          normalizedScore: normalizeDteScore(candidate.raw_score, candidate.dte, config),
        }))
        .filter((item) => item.normalizedScore >= config.minScore)
        .sort((a, b) => b.normalizedScore - a.normalizedScore || b.candidate.raw_score - a.candidate.raw_score);
      if (ranked.length === 0) return null;
      return {
        candidate: ranked[0].candidate,
        score: ranked[0].normalizedScore,
        model_version: `dte-normalized-${config.referenceDte}-${config.exponent}`,
        diagnostics: {
          raw_score: ranked[0].candidate.raw_score,
          normalized_score: ranked[0].normalizedScore,
          dte: ranked[0].candidate.dte,
          reference_dte: config.referenceDte,
          exponent: config.exponent,
        },
      };
    },
    getArtifacts() { return []; },
  };
}

function topScoreSeries(frames = [], options = {}) {
  return frames.map((frame) => {
    const ranked = frame.candidates
      .map((candidate) => ({
        candidate,
        score: normalizeDteScore(candidate.raw_score, candidate.dte, options),
      }))
      .filter((item) => item.score > 0)
      .sort((a, b) => b.score - a.score || b.candidate.raw_score - a.candidate.raw_score);
    if (ranked.length === 0) return null;
    const nearestExpiry = Math.min(...frame.candidates.map((candidate) => Number(candidate.expiry)).filter((expiry) => expiry > 0));
    return {
      timestamp: frame.timestamp,
      timestamp_ms: frame.timestamp_ms,
      score: ranked[0].score,
      raw_score: ranked[0].candidate.raw_score,
      dte: ranked[0].candidate.dte,
      selected_expiry: Number(ranked[0].candidate.expiry),
      nearest_expiry: Number.isFinite(nearestExpiry) ? nearestExpiry : null,
      instrument_name: ranked[0].candidate.instrument_name,
    };
  }).filter(Boolean);
}

function analyzeRolloverDiscontinuity(frames = [], options = {}) {
  const series = topScoreSeries(frames, options);
  const events = [];
  let priorNearest = null;
  for (let index = 0; index < series.length; index++) {
    const point = series[index];
    if (priorNearest != null && point.nearest_expiry > priorNearest) {
      const priorScores = series
        .slice(Math.max(0, index - 24), index)
        .map((item) => item.score)
        .filter((score) => score > 0);
      const throughMs = point.timestamp_ms + 6 * 60 * 60 * 1000;
      const postScores = [];
      for (let future = index; future < series.length && series[future].timestamp_ms <= throughMs; future++) {
        postScores.push(series[future].score);
      }
      const priorMedian = median(priorScores);
      const peak = postScores.length ? Math.max(...postScores) : point.score;
      if (priorMedian > 0) {
        events.push({
          timestamp: point.timestamp,
          utc_day: new Date(point.timestamp_ms).toLocaleDateString('en-US', { weekday: 'short', timeZone: 'UTC' }),
          prior_nearest_expiry: new Date(priorNearest * 1000).toISOString(),
          new_nearest_expiry: new Date(point.nearest_expiry * 1000).toISOString(),
          prior_24h_median: round(priorMedian, 6),
          score_at_roll: round(point.score, 6),
          next_6h_peak: round(peak, 6),
          jump_pct: round((point.score / priorMedian) - 1, 8),
          peak_jump_pct: round((peak / priorMedian) - 1, 8),
          selected_dte: round(point.dte, 4),
          instrument_name: point.instrument_name,
        });
      }
    }
    if (point.nearest_expiry != null && (priorNearest == null || point.nearest_expiry > priorNearest)) {
      priorNearest = point.nearest_expiry;
    }
  }
  const positivePeakJumps = events.map((event) => Math.max(0, event.peak_jump_pct));
  return {
    points: series.length,
    rollover_events: events.length,
    sunday_events: events.filter((event) => event.utc_day === 'Sun').length,
    mean_jump_pct: round(mean(events.map((event) => event.jump_pct)), 8),
    median_jump_pct: round(median(events.map((event) => event.jump_pct)), 8),
    mean_positive_peak_jump_pct: round(mean(positivePeakJumps), 8),
    median_positive_peak_jump_pct: round(median(positivePeakJumps), 8),
    max_peak_jump_pct: events.length ? round(Math.max(...events.map((event) => event.peak_jump_pct)), 8) : null,
    events,
  };
}

module.exports = {
  DEFAULT_DTE_REFERENCE,
  analyzeRolloverDiscontinuity,
  makeDteNormalizedRawPolicy,
  normalizeDteScore,
  topScoreSeries,
};
