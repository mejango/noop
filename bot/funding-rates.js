'use strict';

const FUNDING_EXCHANGE = 'derive';
const FUNDING_SYMBOL = 'ETH-PERP';

function finiteRate(value) {
  if (!['number', 'string'].includes(typeof value) || String(value).trim() === '') return null;
  const rate = Number(value);
  return Number.isFinite(rate) ? rate : null;
}

// V2 PublicGetTickersResultSchema contains a ticker dictionary. TickerSlimSchema
// uses f for the current hourly funding rate and t for snapshot time in ms.
// Source: derive-py e662f36f6b1ab326e97e595f131a1fa5cf6376a8,
// PublicGetTickersResultSchema and TickerSlimSchema in the pinned V2 OpenAPI.
function fundingRatesFromTickerResult(result, observedAt = new Date().toISOString()) {
  const receivedAt = Date.parse(observedAt);
  if (!Number.isFinite(receivedAt)) return [];
  const source = result?.tickers ?? result;
  let matches;
  if (Array.isArray(source)) {
    matches = source.filter(ticker => ticker?.instrument_name === FUNDING_SYMBOL);
  } else if (source && typeof source === 'object') {
    matches = Object.entries(source).filter(([key, ticker]) =>
      (key === FUNDING_SYMBOL || /^ticker\.ETH-PERP(?:\.\d+)?$/.test(key))
      && ticker && typeof ticker === 'object'
      && (ticker.instrument_name == null || ticker.instrument_name === FUNDING_SYMBOL)
    ).map(([, ticker]) => ticker);
  } else return [];
  if (matches.length !== 1) return [];
  const ticker = matches[0];
  const value = Object.hasOwn(ticker, 'f') ? ticker.f : ticker.funding_rate_info?.funding_rate;
  const rate = finiteRate(value);
  if (rate == null) return [];
  let timestamp = new Date(receivedAt).toISOString();
  if (ticker.t != null) {
    const sourceTime = finiteRate(ticker.t);
    if (!Number.isSafeInteger(sourceTime) || sourceTime <= 0 || sourceTime > receivedAt) return [];
    timestamp = new Date(sourceTime).toISOString();
  }
  return [{ timestamp, exchange: FUNDING_EXCHANGE, symbol: FUNDING_SYMBOL, rate }];
}

function summarizeFundingRates(rows) {
  const data = Array.isArray(rows) ? rows : [];
  const rates = data.map(row => row && typeof row === 'object'
    ? finiteRate(Object.hasOwn(row, 'rate') ? row.rate : row.avg_rate)
    : null);
  const knownRates = rates.filter(rate => rate != null);
  const current = rates.length ? rates[rates.length - 1] : null;
  const avg = knownRates.length ? knownRates.reduce((sum, rate) => sum + rate / knownRates.length, 0) : null;
  const trend = current == null || avg == null ? 'unknown'
    : current > avg ? 'rising' : current < avg ? 'declining' : 'stable';
  return { current, avg, trend, samples: knownRates.length };
}

module.exports = { FUNDING_EXCHANGE, FUNDING_SYMBOL, fundingRatesFromTickerResult, summarizeFundingRates };
