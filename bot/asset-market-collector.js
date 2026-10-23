'use strict';

const assets = require('./market-assets.json').filter(asset => asset !== 'ETH');
const { fetchInstruments } = require('./derive-config');
const BASE_URL = 'https://api.derive.xyz/v3/public';
const MAX_EXPIRIES = 32;
const MAX_INSTRUMENT_PAGES = 4;

const number = value => {
  if (value == null || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

async function publicPost(method, body) {
  const response = await fetch(`${BASE_URL}/${method}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(15_000),
  });
  if (!response.ok) throw new Error(`${method} HTTP ${response.status}`);
  const json = await response.json();
  if (json.error) throw new Error(`${method}: ${JSON.stringify(json.error)}`);
  return json.result;
}

function tickerMap(result) {
  return result?.tickers && !Array.isArray(result.tickers) ? result.tickers : {};
}

function marketObservationTime(now) {
  return new Date(now).toISOString();
}

async function collectAsset(asset, post = publicPost, now = Date.now()) {
  if (!assets.includes(asset)) throw new Error(`Unsupported market asset: ${asset}`);
  // `hour` is the legacy snapshot key; new observations use the full ETH-ping time.
  const hour = marketObservationTime(now);
  const summary = { asset, hour, collected_at: new Date(now).toISOString(), spot: null,
    funding_rate: null, instrument_count: null, option_count: null, quoted_count: null,
    put_oi: null, call_oi: null, put_iv: null, call_iv: null, quote_status: 'unavailable' };
  try {
    const perp = tickerMap(await post('get_tickers', { instrument_type: 'perp', currency: asset }))[`${asset}-PERP`];
    const spot = number(perp?.I);
    summary.spot = spot > 0 ? spot : null;
    summary.funding_rate = number(perp?.f);
  } catch (error) {
    console.warn(`Asset ${asset} perp fetch failed: ${error.message}`);
  }

  let instruments;
  try {
    instruments = await fetchInstruments((method, params) => {
      if (params.page > MAX_INSTRUMENT_PAGES) throw new Error('Instrument page limit exceeded');
      return post(method, params);
    }, 'v3', { currency: asset, expired: false, instrument_type: 'option' });
  } catch (error) {
    console.warn(`Asset ${asset} instruments fetch failed: ${error.message}`);
    summary.collected_at = new Date().toISOString();
    return { summary, options: [] };
  }
  summary.instrument_count = instruments.length;
  const byName = new Map(instruments.filter(i => i.instrument_name?.startsWith(`${asset}-`)).map(i => [i.instrument_name, i]));
  const dates = [...new Set([...byName.keys()].map(name => name.split('-')[1]).filter(date => /^\d{8}$/.test(date)))].sort();
  const options = [];
  let failures = 0;
  for (let offset = 0; offset < Math.min(dates.length, MAX_EXPIRIES); offset += 4) {
    const batch = dates.slice(offset, offset + 4);
    const results = await Promise.all(batch.map(async date => {
      try {
        return tickerMap(await post('get_tickers', { instrument_type: 'option', currency: asset, expiry_date: date }));
      } catch (error) {
        failures++;
        console.warn(`Asset ${asset} ${date} ticker fetch failed: ${error.message}`);
        return {};
      }
    }));
    for (const tickers of results) {
      for (const [name, ticker] of Object.entries(tickers)) {
        const instrument = byName.get(name);
        if (!instrument || !ticker || typeof ticker !== 'object') continue;
        const details = instrument.option_details || {};
        const type = details.option_type || name.split('-').at(-1);
        if (type !== 'P' && type !== 'C') continue;
        options.push({ instrument_name: name, expiry: number(details.expiry), strike: number(details.strike),
          option_type: type, bid: number(ticker.b), ask: number(ticker.a), mark: number(ticker.M),
          index_price: number(ticker.I), delta: number(ticker.option_pricing?.d),
          implied_vol: number(ticker.option_pricing?.i), open_interest: number(ticker.stats?.oi),
          bid_amount: number(ticker.B), ask_amount: number(ticker.A) });
      }
    }
  }
  summary.option_count = options.length;
  summary.quoted_count = options.filter(o => o.bid != null && o.ask != null).length;
  for (const type of ['P', 'C']) {
    const rows = options.filter(o => o.option_type === type);
    const oi = rows.map(o => o.open_interest).filter(v => v != null && v >= 0);
    const iv = rows.map(o => o.implied_vol).filter(v => v != null && v > 0);
    summary[type === 'P' ? 'put_oi' : 'call_oi'] = oi.length ? oi.reduce((a, b) => a + b, 0) : null;
    summary[type === 'P' ? 'put_iv' : 'call_iv'] = iv.length ? iv.reduce((a, b) => a + b, 0) / iv.length : null;
  }
  summary.quote_status = !failures && dates.length <= MAX_EXPIRIES && options.length === instruments.length
    ? 'complete' : 'partial';
  summary.collected_at = new Date().toISOString();
  return { summary, options };
}

module.exports = { collectAsset, marketObservationTime };
