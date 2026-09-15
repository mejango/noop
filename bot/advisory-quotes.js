'use strict';

const facts = require('./strategy-facts.json');
const { instrumentFromName } = require('./observations');

const finite = value => value == null || (typeof value === 'string' && value.trim() === '') || typeof value === 'boolean'
  ? null : Number.isFinite(Number(value)) ? Number(value) : null;
const positive = value => finite(value) > 0;
const timestamp = value => {
  const ms = value == null ? NaN : Date.parse(value);
  return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
};
const timestampRange = rows => {
  const values = rows.map(row => timestamp(row?.quote_received_at)).filter(Boolean).sort();
  return {
    quote_received_at_oldest: values[0] || null,
    quote_received_at_latest: values.at(-1) || null,
    timestamped_quote_count: values.length,
  };
};

// Descriptive percentage of mark, never an entry gate. A missing book side
// cannot be interpreted as a zero-price executable quote or a measured spread.
function candidateSpreadPct({ askPrice, bidPrice, markPrice } = {}) {
  if (![askPrice, bidPrice, markPrice].every(positive) || Number(askPrice) < Number(bidPrice)) return null;
  return (Number(askPrice) - Number(bidPrice)) / Number(markPrice) * 100;
}

function summarizeAdvisoryQuotes(tickerMap = {}, {
  nowMs = Date.now(),
  inputTimestamp = null,
  expectedInstruments = null,
  putDeltaRange = facts.put_delta_range,
  putDteRange = facts.put_dte_range,
  callDeltaRange = facts.call_delta_range,
  callDteRange = facts.call_dte_range,
} = {}) {
  const tickers = tickerMap && typeof tickerMap === 'object' ? tickerMap : {};
  const expectedSupplied = Array.isArray(expectedInstruments);
  const expected = expectedSupplied
    ? [...new Set(expectedInstruments.map(row => typeof row === 'string' ? row : row?.instrument_name))]
    : [];
  const malformedExpected = expected.some(name => !instrumentFromName(name));
  const withinDte = (name, type, range) => {
    const instrument = instrumentFromName(name);
    if (!instrument || instrument.option_details.option_type !== type) return false;
    const dte = (instrument.option_details.expiry * 1000 - nowMs) / 86400000;
    return dte >= range[0] && dte <= range[1];
  };
  const summarizeSide = (type, deltaRange, dteRange, priceKey, priceName) => {
    const rows = Object.entries(tickers).filter(([name]) => withinDte(name, type, dteRange));
    const unknownDeltaInstruments = rows.filter(([, ticker]) => finite(ticker?.option_pricing?.d) == null).map(([name]) => name).sort();
    const unknownDeltaCount = unknownDeltaInstruments.length;
    const eligible = rows.filter(([, ticker]) => {
      const delta = finite(ticker?.option_pricing?.d);
      return delta != null && delta >= deltaRange[0] && delta <= deltaRange[1];
    });
    const quoted = eligible.filter(([, ticker]) => positive(ticker?.[priceKey]));
    const expectedNames = expected.filter(name => withinDte(name, type, dteRange)).sort();
    const missingExpected = expectedNames.filter(name => !tickers[name] || typeof tickers[name] !== 'object');
    const coverage = !expectedSupplied || malformedExpected ? 'unknown'
      : missingExpected.length || unknownDeltaCount ? 'partial' : 'complete';
    let status;
    let reason;
    if (quoted.length) {
      status = 'available';
      reason = `${quoted.length} of ${eligible.length} observed DTE/delta-eligible contracts have a positive ${priceName}.`;
    } else if (eligible.length) {
      status = 'quotes_unavailable';
      reason = `${eligible.length} observed contracts meet DTE/delta bounds, but none has a positive ${priceName}; current EDGE is unavailable.`;
    } else if (coverage === 'complete') {
      status = 'no_eligible_candidates';
      reason = 'The covered instrument universe has no contracts meeting the DTE/delta bounds.';
    } else {
      status = 'unknown';
      reason = 'No quoted eligible candidate was observed, and incomplete instrument or delta coverage prevents concluding that none exists.';
    }
    return {
      status,
      reason,
      coverage_status: coverage,
      quote_side: priceName,
      in_dte_count: rows.length,
      in_dte_delta_count: eligible.length,
      quoted_count: quoted.length,
      missing_quote_count: eligible.length - quoted.length,
      unknown_delta_count: unknownDeltaCount,
      expected_in_dte_count: expectedSupplied ? expectedNames.length : null,
      missing_expected_ticker_count: expectedSupplied ? missingExpected.length : null,
      eligible_instruments: eligible.map(([name]) => name).sort(),
      quoted_instruments: quoted.map(([name]) => name).sort(),
      missing_expected_instruments: expectedSupplied ? missingExpected : null,
      unknown_delta_instruments: unknownDeltaInstruments,
      expected_instruments: expectedSupplied ? expectedNames : null,
      ...timestampRange(rows.map(([, ticker]) => ticker)),
    };
  };
  return {
    evaluated_at: new Date(nowMs).toISOString(),
    input_timestamp: timestamp(inputTimestamp),
    expected_instruments_supplied: expectedSupplied,
    ...timestampRange(Object.values(tickers)),
    put: summarizeSide('P', putDeltaRange, putDteRange, 'a', 'ask'),
    call: summarizeSide('C', callDeltaRange, callDteRange, 'b', 'bid'),
  };
}

module.exports = { summarizeAdvisoryQuotes, candidateSpreadPct };
