'use strict';

// Fixed pools keep a changing search ranking from silently changing the series.
const SOURCES = Object.freeze({
  BTC: { chain: 'Bitcoin', venue: 'Maya Protocol', pool: 'BTC.BTC', pair: 'BTC/CACAO',
    url: 'https://midgard.mayachain.info/v2/pool/BTC.BTC', kind: 'maya' },
  ZEC: { chain: 'Zcash', venue: 'Maya Protocol', pool: 'ZEC.ZEC', pair: 'ZEC/CACAO',
    url: 'https://midgard.mayachain.info/v2/pool/ZEC.ZEC', kind: 'maya' },
  SOL: { chain: 'Solana', network: 'solana', venue: 'Orca', pool: 'Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE', pair: 'WSOL/USDC',
    token: 'So11111111111111111111111111111111111111112',
    quoteToken: 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', dex: 'orca',
    url: 'https://api.geckoterminal.com/api/v2/networks/solana/pools/Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE?include=base_token,quote_token', kind: 'gecko' },
  HYPE: { chain: 'HyperEVM', network: 'hyperevm', venue: 'HyperSwap V3', pool: '0xe712d505572b3f84c1b4deb99e1beab9dd0e23c9', pair: 'USDC/WHYPE',
    token: '0x5555555555555555555555555555555555555555',
    quoteToken: '0xb88339cb7199b77e23db6e890353e22632ba630f', dex: 'hyperswap-v3',
    url: 'https://api.geckoterminal.com/api/v2/networks/hyperevm/pools/0xe712d505572b3f84c1b4deb99e1beab9dd0e23c9?include=base_token,quote_token', kind: 'gecko' },
});

const positive = value => {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
};

function parseMaya(asset, data) {
  if (data?.asset !== `${asset}.${asset}` || String(data.status).toLowerCase() !== 'available') {
    throw new Error('Maya pool identity or status mismatch');
  }
  const depth = positive(data.assetDepth);
  const price = positive(data.assetPriceUSD);
  if (!depth || !price) throw new Error('Maya pool reserves unavailable');
  // Midgard assetDepth is in 1e8 units. A balanced Maya pool holds equal USD
  // value on its asset and CACAO sides, so twice asset-side value estimates TVL.
  return { tvl_usd: 2 * (depth / 1e8) * price,
    volume_24h_usd: null, source: 'maya_midgard_balanced_pool_tvl_estimate_v1' };
}

function parseGecko(source, data) {
  const pool = data?.data;
  const id = String(pool?.id || '').toLowerCase();
  const tokens = [pool?.relationships?.base_token?.data?.id, pool?.relationships?.quote_token?.data?.id]
    .map(value => String(value || '').toLowerCase());
  if (id !== `${source.network}_${source.pool.toLowerCase()}`
    || !tokens.includes(`${source.network}_${source.token.toLowerCase()}`)
    || !tokens.includes(`${source.network}_${source.quoteToken.toLowerCase()}`)
    || String(pool?.relationships?.dex?.data?.id || '').toLowerCase() !== source.dex) {
    throw new Error('GeckoTerminal pool identity mismatch');
  }
  const tvl = positive(pool.attributes.reserve_in_usd);
  if (!tvl) throw new Error('GeckoTerminal pool reserves unavailable');
  return { tvl_usd: tvl, volume_24h_usd: positive(pool.attributes.volume_usd?.h24),
    source: 'geckoterminal_pool_reserve_usd_v1' };
}

async function collectAssetLiquidity(asset, observedAt, get = fetch) {
  const source = SOURCES[asset];
  if (!source || !Number.isFinite(Date.parse(observedAt))) throw new Error('Invalid asset liquidity request');
  const response = await get(source.url, { headers: { Accept: 'application/json;version=20230203' },
    signal: AbortSignal.timeout(12_000) });
  if (!response.ok) throw new Error(`Asset liquidity HTTP ${response.status}`);
  const data = await response.json();
  const values = source.kind === 'maya' ? parseMaya(asset, data) : parseGecko(source, data);
  return { asset, observed_at: new Date(observedAt).toISOString(), collected_at: new Date().toISOString(),
    chain: source.chain, venue: source.venue, pool_id: source.pool, pair: source.pair, ...values };
}

module.exports = { collectAssetLiquidity, SOURCES, parseMaya, parseGecko };
