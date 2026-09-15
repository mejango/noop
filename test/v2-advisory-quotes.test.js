'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { summarizeAdvisoryQuotes, candidateSpreadPct } = require('../bot/advisory-quotes');
const { loadProduction } = require('./helpers/load-production');

const nowMs = Date.parse('2026-09-15T18:55:15.381Z');
const putName = 'ETH-20261127-1600-P';
const callName = 'ETH-20260925-2800-C';
const quote = (delta, ask = 0, bid = 0, receivedAt = '2026-09-15T18:55:15.381Z') => ({
  option_pricing: { d: delta }, a: ask, b: bid, M: 20,
  A: ask > 0 ? 10 : 0, B: bid > 0 ? 10 : 0, quote_received_at: receivedAt,
});
const expectedInstruments = [putName, callName];

test('eligible zero-sided quotes are unavailable, not absent instruments', () => {
  const summary = summarizeAdvisoryQuotes({ [putName]: quote(-0.06), [callName]: quote(0.06) }, { nowMs, expectedInstruments });
  for (const side of [summary.put, summary.call]) {
    assert.equal(side.status, 'quotes_unavailable');
    assert.equal(side.coverage_status, 'complete');
    assert.equal(side.in_dte_delta_count, 1);
    assert.equal(side.quoted_count, 0);
    assert.equal(side.missing_quote_count, 1);
    assert.equal(side.missing_expected_ticker_count, 0);
  }
  assert.equal(summary.put.quote_side, 'ask');
  assert.equal(summary.call.quote_side, 'bid');
});

test('entry-side quotes count without requiring the opposite side or changing eligibility', () => {
  const summary = summarizeAdvisoryQuotes({ [putName]: quote(-0.06, 20), [callName]: quote(0.06, 0, 5) }, { nowMs, expectedInstruments });
  assert.equal(summary.put.status, 'available');
  assert.equal(summary.call.status, 'available');
  assert.equal(summary.put.quoted_count, 1);
  assert.equal(summary.call.quoted_count, 1);
});

test('missing expected ticker and missing Greeks prevent a no-eligible claim', () => {
  const missing = summarizeAdvisoryQuotes({}, { nowMs, expectedInstruments });
  assert.equal(missing.put.status, 'unknown');
  assert.equal(missing.put.coverage_status, 'partial');
  assert.equal(missing.put.missing_expected_ticker_count, 1);
  for (const delta of [null, undefined, '', Infinity, NaN, true]) {
    const result = summarizeAdvisoryQuotes({ [putName]: quote(delta, 10) }, { nowMs, expectedInstruments: [putName] });
    assert.equal(result.put.status, 'unknown');
    assert.equal(result.put.unknown_delta_count, 1);
  }
});

test('only a covered instrument universe can prove no DTE/delta-eligible candidates', () => {
  const tickers = { [putName]: quote(-0.2, 40) };
  const complete = summarizeAdvisoryQuotes(tickers, { nowMs, expectedInstruments: [putName] });
  assert.equal(complete.put.status, 'no_eligible_candidates');
  assert.equal(complete.put.coverage_status, 'complete');
  const unknown = summarizeAdvisoryQuotes(tickers, { nowMs });
  assert.equal(unknown.put.status, 'unknown');
  assert.equal(unknown.put.expected_in_dte_count, null);
});

test('partial universe preserves available observed quotes while disclosing missing coverage', () => {
  const summary = summarizeAdvisoryQuotes({ [putName]: quote(-0.06, 20) }, {
    nowMs, expectedInstruments: [putName, 'ETH-20261127-1800-P'],
  });
  assert.equal(summary.put.status, 'available');
  assert.equal(summary.put.coverage_status, 'partial');
  assert.equal(summary.put.missing_expected_ticker_count, 1);
});

test('sorted identity fields expose quote and coverage changes even when counts match', () => {
  const other = 'ETH-20261127-1800-P';
  const absent = 'ETH-20261127-1500-P';
  const unknown = 'ETH-20261127-1700-P';
  const options = { nowMs, expectedInstruments: [other, absent, putName, unknown] };
  const before = summarizeAdvisoryQuotes({ [other]: quote(-0.1), [putName]: quote(-0.06, 20), [unknown]: quote(null) }, options).put;
  const after = summarizeAdvisoryQuotes({ [unknown]: quote(null), [putName]: quote(-0.06), [other]: quote(-0.1, 40) }, options).put;
  assert.equal(before.quoted_count, after.quoted_count);
  assert.deepEqual(before.eligible_instruments, [putName, other]);
  assert.deepEqual(before.quoted_instruments, [putName]);
  assert.deepEqual(after.quoted_instruments, [other]);
  assert.deepEqual(before.missing_expected_instruments, [absent]);
  assert.deepEqual(before.unknown_delta_instruments, [unknown]);
  assert.deepEqual(before.expected_instruments, [absent, putName, unknown, other]);
});

test('fractional DTE excludes October quotes below 45 days during the captured outage', () => {
  const october = 'ETH-20261030-1600-P';
  const summary = summarizeAdvisoryQuotes({ [october]: quote(-0.04, 10), [putName]: quote(-0.06) }, {
    nowMs, expectedInstruments: [october, putName],
  });
  assert.equal(summary.put.in_dte_count, 1);
  assert.equal(summary.put.in_dte_delta_count, 1);
  assert.equal(summary.put.quoted_count, 0);
  assert.equal(summary.put.expected_in_dte_count, 1);
  assert.equal(summary.put.status, 'quotes_unavailable');
});

test('quote receipt range and input timestamp retain their separate meanings', () => {
  const summary = summarizeAdvisoryQuotes({
    [putName]: quote(-0.06, 20, 19, '2026-09-15T18:55:12Z'),
    [callName]: quote(0.06, 6, 5, '2026-09-15T18:55:14Z'),
    'ETH-20261127-1800-P': quote(-0.1, 40, 39, 'invalid'),
  }, { nowMs, inputTimestamp: '2026-09-15T18:55:10Z' });
  assert.equal(summary.input_timestamp, '2026-09-15T18:55:10.000Z');
  assert.equal(summary.evaluated_at, '2026-09-15T18:55:15.381Z');
  assert.equal(summary.quote_received_at_oldest, '2026-09-15T18:55:12.000Z');
  assert.equal(summary.quote_received_at_latest, '2026-09-15T18:55:14.000Z');
  assert.equal(summary.timestamped_quote_count, 2);
  assert.equal(summary.put.timestamped_quote_count, 1);
});

test('candidate spread requires finite positive two-sided quotes and mark', () => {
  assert.equal(candidateSpreadPct({ askPrice: 22, bidPrice: 18, markPrice: 20 }), 20);
  assert.equal(candidateSpreadPct({ askPrice: 20, bidPrice: 20, markPrice: 20 }), 0);
  for (const value of [0, -1, null, undefined, '', NaN, Infinity, true]) {
    for (const key of ['askPrice', 'bidPrice', 'markPrice']) {
      assert.equal(candidateSpreadPct({ askPrice: 22, bidPrice: 18, markPrice: 20, [key]: value }), null);
    }
  }
  assert.equal(candidateSpreadPct({ askPrice: 18, bidPrice: 22, markPrice: 20 }), null);
});

test('actual live-market spread helper excludes one-sided and crossed books without changing its ratio units', () => {
  const { getTickerSpreadPct } = loadProduction(['getTickerSpreadPct'], { bindings: { candidateSpreadPct } });
  assert.equal(getTickerSpreadPct({ a: 22, b: 18, M: 20 }), 0.2);
  for (const ticker of [{ a: 22, b: 0, M: 20 }, { a: 0, b: 18, M: 20 }, { a: 18, b: 22, M: 20 }, { a: 22, b: 18, M: 0 }]) {
    assert.equal(getTickerSpreadPct(ticker), null);
  }
});

class FixedDate extends Date {
  constructor(...args) { super(...(args.length ? args : [nowMs])); }
  static now() { return nowMs; }
}
function rolling(tickerMap) {
  const api = loadProduction(['buildRollingOptionValueContext', 'formatRollingOptionValueContext'], {
    bindings: {
      summarizeAdvisoryQuotes, candidateSpreadPct,
      ...require('../bot/option-market-quality'),
      Date: FixedDate,
      db: {
        getBuyPutScoreSamples: () => [{ timestamp: new Date(nowMs - 1800000).toISOString(), score: 0.004 }],
        getSellCallScoreSamples: () => [{ timestamp: new Date(nowMs - 1800000).toISOString(), score: 100 }],
      },
    },
  });
  const context = api.buildRollingOptionValueContext({
    tickerMap, expectedInstruments, momentum: { shortTerm: { main: 'downward' } },
    putBudgetRemaining: 100, currentTickTimestamp: new Date(nowMs).toISOString(), spotPrice: 2367.70,
  });
  return { context, prompt: api.formatRollingOptionValueContext(context) };
}

test('actual rolling context preserves unavailable PUT/CALL scores and every relative comparison as null', () => {
  const { context, prompt } = rolling({ [putName]: quote(-0.06), [callName]: quote(0.06) });
  for (const side of [context.put_value_context, context.call_value_context]) {
    for (const field of ['current_score', 'current_vs_prior_best_pct', 'percentile_vs_prior_window', 'trend_1h_pct', 'trend_6h_pct', 'trend_24h_pct', 'current_detail', 'is_strict_fresh_best']) {
      assert.equal(side[field], null, field);
    }
    assert.equal(side.samples, 1);
    assert.equal(side.availability.status, 'quotes_unavailable');
  }
  assert.equal(context.spot_repricing_lag_context.current_vs_prior_best_pct, null);
  assert.equal(context.spot_repricing_lag_context.is_detected, null);
  assert.equal(context.recent_relative_value_context.current_vs_rolling_best_pct, null);
  assert.equal(context.recent_relative_value_context.is_detected, null);
  assert.equal(context.action_pressure.signal, null);
  assert.equal(context.action_pressure.target_score, null);
  assert.equal(context.action_pressure.requires_buy_put_decision, false);
  assert.match(prompt, /Current PUT EDGE: unavailable/);
  assert.match(prompt, /Current CALL EDGE: unavailable/);
  assert.match(prompt, /DTE\/delta eligible=1; positive ask=0/);
  assert.doesNotMatch(prompt, /Current PUT EDGE: 0/);
  assert.match(prompt, /strict_fresh_best=unavailable/);
});

test('actual rolling context retains one-sided entry scores but leaves spreads unavailable', () => {
  const { context } = rolling({ [putName]: quote(-0.06, 20), [callName]: quote(0.06, 0, 5) });
  assert.ok(context.put_value_context.current_score > 0);
  assert.ok(context.call_value_context.current_score > 0);
  assert.equal(context.put_value_context.current_detail.spread_pct, null);
  assert.equal(context.put_value_context.selected_candidate_detail.spread_pct, null);
  assert.equal(context.call_value_context.current_detail.spread_pct, null);
  assert.equal(context.put_value_context.current_detail.quote_received_at, new Date(nowMs).toISOString());
  assert.equal(context.put_value_context.current_detail.spread_basis, 'bid_ask_difference_over_mark');
});
