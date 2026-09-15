'use strict';

const facts = require('./strategy-facts.json');
const { instrumentFromName } = require('./observations');
const { summarizeAdvisoryQuotes } = require('./advisory-quotes');

// A fresh advisory covers every entry-eligible expiry and every held option.
// It does not reuse the quote map captured before asynchronous journal work.
async function readAdvisoryMarketSnapshot({ fetchSpot, fetchInstruments, fetchPositions, fetchTickers, now = Date.now }) {
  const [spotPrice, instruments, positions] = await Promise.all([
    fetchSpot(), fetchInstruments(), fetchPositions(),
  ]);
  if (!(Number.isFinite(spotPrice) && spotPrice > 0)) throw new Error('Advisory refresh: spot price unavailable');
  if (!Array.isArray(instruments) || instruments.length === 0) throw new Error('Advisory refresh: instrument coverage unavailable');
  if (!Array.isArray(positions)) throw new Error('Advisory refresh: positions unavailable');
  if (positions.some(position => !position || Array.isArray(position)
    || typeof position.instrument_name !== 'string' || !position.instrument_name.trim()
    || position.amount == null || position.amount === '' || !Number.isFinite(Number(position.amount))
    || !['long', 'short'].includes(position.direction))) throw new Error('Advisory refresh: malformed position state');
  const observedAt = now();
  const names = new Set(positions.map(position => position.instrument_name).filter(Boolean));
  for (const instrument of instruments) {
    const parsed = instrumentFromName(instrument.instrument_name);
    if (!parsed) throw new Error('Advisory refresh: malformed instrument metadata');
    const dte = (parsed.option_details.expiry * 1000 - observedAt) / 86400000;
    const range = parsed.option_details.option_type === 'P' ? facts.put_dte_range : facts.call_dte_range;
    if (dte >= range[0] && dte <= range[1]) names.add(instrument.instrument_name);
  }
  const expiries = [...new Set([...names].filter(name => instrumentFromName(name)).map(name => name.split('-')[1]))].sort();
  const responses = await Promise.all(expiries.map(expiry => fetchTickers(expiry)));
  const tickerMap = {};
  for (let i = 0; i < responses.length; i++) {
    const rows = responses[i];
    if (!rows || typeof rows !== 'object' || Array.isArray(rows)) throw new Error(`Advisory refresh: invalid quotes for ${expiries[i]}`);
    for (const [name, ticker] of Object.entries(rows)) {
      if (!instrumentFromName(name) || name.split('-')[1] !== expiries[i] || !ticker || typeof ticker !== 'object' || Array.isArray(ticker)) {
        throw new Error(`Advisory refresh: malformed quotes for ${expiries[i]}`);
      }
      tickerMap[name] = ticker;
    }
  }
  const checkedAt = now();
  const marketTimestamp = new Date(checkedAt).toISOString();
  const quoteAvailability = summarizeAdvisoryQuotes(tickerMap, {
    nowMs: checkedAt, inputTimestamp: marketTimestamp, expectedInstruments: instruments,
  });
  return { positions, spotPrice, tickerMap, instruments, marketTimestamp, quoteAvailability };
}

module.exports = { readAdvisoryMarketSnapshot };
