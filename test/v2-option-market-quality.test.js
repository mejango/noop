'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { isBetterBuyPutCandidate, computeMatchedPutCallSkew } = require('../bot/option-market-quality');
const putScore = require('../bot/put-score');
const callScore = require('../bot/call-score');

const closeTo = (actual, expected) => assert.ok(Math.abs(actual - expected) < 1e-12, `${actual} != ${expected}`);
const put = (expiry, delta, impliedVol) => ({ expiry, delta, impliedVol });
const call = put;

test('skew matches expiry and absolute delta rather than the strategy tenor averages', () => {
  const result = computeMatchedPutCallSkew(
    [put('60d', -0.06, 0.6)],
    [call('7d', 0.06, 0.4), call('60d', 0.06, 0.6)]
  );
  closeTo(result.skew, 0);
  assert.equal(result.matchedPairs, 1);
  assert.equal(result.matchedExpiries, 1);
});

test('skew interpolates comparable absolute delta within the same expiry', () => {
  const result = computeMatchedPutCallSkew(
    [put('60d', -0.06, 0.55)],
    [call('60d', 0.08, 0.6), call('60d', 0.04, 0.4)]
  );
  closeTo(result.callIv, 0.5);
  closeTo(result.skew, 0.05);
});

test('missing or unbracketed call surface remains unknown without extrapolation', () => {
  const result = computeMatchedPutCallSkew(
    [put('60d', -0.06, 0.6), put('70d', -0.06, 0.6)],
    [call('7d', 0.06, 0.4), call('60d', 0.08, 0.4), call('60d', 0.1, 0.4)]
  );
  assert.equal(result.skew, null);
  assert.equal(result.putIv, null);
  assert.equal(result.callIv, null);
  assert.equal(result.matchedPairs, 0);
  assert.equal(result.unmatchedPuts, 2);
});

test('matched means weight puts and their comparable calls identically', () => {
  const result = computeMatchedPutCallSkew(
    [put('60d', -0.04, 0.65), put('60d', -0.08, 0.7), put('70d', -0.06, 0.9)],
    [call('60d', 0.04, 0.45), call('60d', 0.08, 0.5)]
  );
  closeTo(result.skew, 0.2);
  closeTo(result.putIv, 0.675);
  closeTo(result.callIv, 0.475);
  assert.equal(result.unmatchedPuts, 1);
});

test('invalid IV observations do not become zero-IV matches', () => {
  for (const value of [null, '', ' ', NaN, Infinity, 0, -1]) {
    assert.equal(computeMatchedPutCallSkew([put('60d', -0.06, 0.6)], [call('60d', 0.06, value)]).skew, null);
    assert.equal(computeMatchedPutCallSkew([put('60d', -0.06, value)], [call('60d', 0.06, 0.4)]).skew, null);
  }
});

test('duplicate delta rows and ticker order do not change matched skew', () => {
  const puts = [put('60d', -0.06, 0.6), put('60d', -0.08, 0.7)];
  const calls = [call('60d', 0.04, 0.4), call('60d', 0.04, 0.6), call('60d', 0.08, 0.6)];
  assert.deepEqual(computeMatchedPutCallSkew(puts, calls), computeMatchedPutCallSkew(puts.reverse(), calls.reverse()));
});

test('quality ties prefer continuous economics, then deterministic instrument order', () => {
  const cheaper = { instrument: 'ETH-20261031-1600-P', selection_score: 195, edge_score: 0.00625 };
  const dearer = { instrument: 'ETH-20261031-1650-P', selection_score: 195, edge_score: 0.00422535 };
  assert.equal(isBetterBuyPutCandidate(cheaper, dearer), true);
  assert.equal(isBetterBuyPutCandidate(dearer, cheaper), false);
  assert.equal(isBetterBuyPutCandidate(cheaper, { ...dearer, edge_score: cheaper.edge_score }), true);
  assert.equal(isBetterBuyPutCandidate(dearer, { ...cheaper, selection_score: 196 }), false);
});

// Execute the current production declarations for integration coverage. This
// never loads script.js as a module or starts its bot/API/private-key paths.
const source = fs.readFileSync(path.join(__dirname, '..', 'script.js'), 'utf8');
const between = (start, end) => {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from);
  assert.ok(from >= 0 && to > from, `Production declaration block missing: ${start}`);
  return source.slice(from, to);
};
const nowMs = Date.parse('2026-09-01T08:00:00Z');
class FixedDate extends Date {
  constructor(...args) { super(...(args.length ? args : [nowMs])); }
  static now() { return nowMs; }
}
function productionContext(samples = []) {
  const sharedFactsPath = path.join(__dirname, '..', 'bot', 'strategy-facts.js');
  const context = vm.createContext({
    ...putScore,
    ...callScore,
    STRATEGY_FACTS: require('../bot/strategy-facts.json'),
    ...(fs.existsSync(sharedFactsPath) ? require(sharedFactsPath) : {}),
    isBetterBuyPutCandidate,
    computeMatchedPutCallSkew,
    Date: FixedDate,
    console,
    db: { getBuyPutScoreSamples: () => samples },
  });
  const constants = source.match(/^const (?:PUT_DELTA_RANGE|BUY_PUT_[A-Z0-9_]+|ADVISORY_OPTION_VALUE_WINDOW_DAYS|CALL_EXPIRATION_RANGE|CALL_DELTA_RANGE|SELL_CALL_FALLBACK_MIN_SCORE) = [^;\n]+;/gm) || [];
  vm.runInContext([
    ...constants,
    between('const roundForAdvisory =', '// Load private key'),
    between('const isSellCallCandidateInStrategyRange =', 'const telemetryNumber ='),
    'globalThis.api = { buildLiveSellCallMarketContext, classifyBuyPutEdge, getBestCurrentBuyPutCandidate, getBestCurrentBuyPutEdgeCandidate, buildRollingOptionValueContext, formatRollingOptionValueContext };',
  ].join('\n'), context);
  return context.api;
}

const nameAt = (days, strike, type) => `ETH-${new Date(nowMs + days * 86400000).toISOString().slice(0, 10).replaceAll('-', '')}-${strike}-${type}`;
const ticker = (ask = 10, bid = ask * 0.98, delta = -0.06, iv = 0.6) => ({
  a: ask, b: bid, M: (ask + bid) / 2, A: 10, B: 10,
  option_pricing: { d: delta, i: iv },
});

test('production quality selector resolves same-bucket candidates by economics in either input order', () => {
  const api = productionContext();
  const dearer = nameAt(60, 1600, 'P');
  const cheaper = nameAt(60, 1650, 'P');
  const entries = [[dearer, ticker(12)], [cheaper, ticker(10)]];
  for (const ordered of [entries, [...entries].reverse()]) {
    assert.equal(api.getBestCurrentBuyPutCandidate(Object.fromEntries(ordered), nowMs, null, 2500).instrument, cheaper);
  }
  const exactTies = [[dearer, ticker(10)], [cheaper, ticker(10)]];
  for (const ordered of [exactTies, [...exactTies].reverse()]) {
    assert.equal(api.getBestCurrentBuyPutCandidate(Object.fromEntries(ordered), nowMs, null, 2500).instrument, dearer);
    assert.equal(api.getBestCurrentBuyPutEdgeCandidate(Object.fromEntries(ordered), nowMs).instrument, dearer);
  }
});

test('production rolling comparison uses market maximum while identifying the quality-selected order separately', () => {
  const api = productionContext([{ timestamp: new Date(nowMs - 1800000).toISOString(), score: 0.006 }]);
  const selected = nameAt(60, 1600, 'P');
  const maximum = nameAt(60, 1650, 'P');
  const entries = [[selected, ticker(12)], [maximum, ticker(8, 2)]];
  for (const ordered of [entries, [...entries].reverse()]) {
    const result = api.buildRollingOptionValueContext({
      tickerMap: Object.fromEntries(ordered), momentum: { shortTerm: 'neutral' }, putBudgetRemaining: 100,
      currentTickTimestamp: new Date(nowMs).toISOString(), spotPrice: 2500,
    });
    assert.equal(result.put_value_context.current_score, 0.0075);
    assert.equal(result.put_value_context.current_detail.instrument, maximum);
    assert.equal(result.put_value_context.selected_candidate_detail.instrument, selected);
    assert.equal(result.put_value_context.selected_candidate_detail.put_edge_score, 0.005);
    assert.equal(result.put_value_context.current_vs_prior_best_pct, 125);
    assert.equal(result.put_value_context.is_strict_fresh_best, true);
    assert.equal(result.put_value_context.trend_1h_pct, 25);
    assert.equal(result.action_pressure.suggested_instrument, selected);
    assert.equal(result.action_pressure.signal, 'strict_fresh_best');
    assert.ok(result.action_pressure.suggested_limit_price < 12);
    const prompt = api.formatRollingOptionValueContext(result);
    assert.ok(prompt.includes(`PUT composite selector: instrument=${selected}; PUT EDGE=0.005`));
    assert.ok(prompt.includes(`suggested_instrument=${selected}`));
  }
});

test('production market context and quality flags ignore pure term structure but retain matched skew risk', () => {
  const api = productionContext();
  const putName = nameAt(60, 1600, 'P');
  const callName = nameAt(60, 3000, 'C');
  const tickers = {
    [putName]: ticker(10),
    [nameAt(7, 3000, 'C')]: ticker(10, 9.8, 0.06, 0.4),
    [callName]: ticker(10, 9.8, 0.06, 0.6),
  };
  const matched = api.buildLiveSellCallMarketContext(tickers, nowMs);
  assert.equal(matched.market_call_iv, 0.4);
  assert.equal(matched.market_put_iv, 0.6);
  assert.equal(matched.market_skew, 0);
  assert.equal(matched.market_skew_matched_pairs, 1);
  assert.equal(api.getBestCurrentBuyPutCandidate(tickers, nowMs, matched, 2500).research.skew_caution, false);

  tickers[callName] = ticker(10, 9.8, 0.06, 0.4);
  const elevated = api.buildLiveSellCallMarketContext(tickers, nowMs);
  closeTo(elevated.market_skew, 0.2);
  assert.equal(api.getBestCurrentBuyPutCandidate(tickers, nowMs, elevated, 2500).research.skew_caution, true);

  delete tickers[callName];
  const missing = api.buildLiveSellCallMarketContext(tickers, nowMs);
  assert.equal(missing.market_skew, null);
  assert.equal(api.getBestCurrentBuyPutCandidate(tickers, nowMs, missing, 2500).research.skew_caution, false);
});
