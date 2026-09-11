'use strict';

// The observation universe outlives entry eligibility. Fallback metadata is for
// recording quotes only; it must never authorize submission to an instrument.
function instrumentFromName(name) {
  const match = /^ETH-(\d{4})(\d{2})(\d{2})-([\d.]+)-([CP])$/.exec(String(name));
  if (!match) return null;
  const expiry = Date.parse(`${match[1]}-${match[2]}-${match[3]}T08:00:00Z`) / 1000;
  const strike = Number(match[4]);
  if (!Number.isFinite(expiry) || !(strike > 0)) return null;
  return {instrument_name:name,instrument_type:'option',option_details:{expiry,strike,option_type:match[5]}};
}
function observationUniverse({instruments = [], candidates = [], positions = [], pendingSymbols = []}) {
  const known = new Map(instruments.map(i => [i.instrument_name,i]));
  const names = new Set([...candidates.map(i => i.instrument_name), ...positions.filter(p => Math.abs(Number(p.amount)) > 0).map(p => p.instrument_name), ...pendingSymbols]);
  return [...names].map(name => known.get(name) || instrumentFromName(name)).filter(Boolean);
}
function missingExpiryDates(instruments, fetchedDates) {
  return [...new Set(instruments.map(i => i.instrument_name.split('-')[1]))].filter(date => /^\d{8}$/.test(date) && !fetchedDates.has(date));
}
module.exports = { observationUniverse, instrumentFromName, missingExpiryDates };
