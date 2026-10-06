'use strict';

const NETWORKS = {
  v2: { baseUrl: 'https://api.lyra.finance', domainSeparator: '0xd96e5f90797da7ec8dc4e276260c7f3f87fedf68775fbe1ef116e996fc60441b' },
  mainnet: { baseUrl: 'https://api.derive.xyz/v3', domainSeparator: '0xda616dfabb88681b08e1592820a41d55ddc62d68de110e327ae99d734506fe19' },
  testnet: { baseUrl: 'https://testnet.api.derive.xyz/v3', domainSeparator: '0x24d674cd5f2b9d564691c51e9d88f649b99246a2244dd74ce27b96578d773e85' },
};

function getDeriveConfig(env = process.env) {
  const version = env.DERIVE_API_VERSION || 'v2';
  if (!['v2', 'v3'].includes(version)) throw new Error('DERIVE_API_VERSION must be v2 or v3');
  const network = env.DERIVE_NETWORK || 'mainnet';
  if (!['mainnet', 'testnet'].includes(network)) throw new Error('DERIVE_NETWORK must be mainnet or testnet');
  const defaults = NETWORKS[version === 'v2' ? 'v2' : network];
  const baseUrl = (env.DERIVE_API_URL || defaults.baseUrl).replace(/\/+$/, '');
  const domainSeparator = env.DERIVE_DOMAIN_SEPARATOR || defaults.domainSeparator;
  // Restrict authenticated requests to the selected venue and matching domain.
  if (baseUrl !== defaults.baseUrl || domainSeparator.toLowerCase() !== defaults.domainSeparator) {
    throw new Error('Derive URL/domain does not match the selected version and network');
  }
  const wallet = env.DERIVE_WALLET || (version === 'v2' ? '0xD87890df93bf74173b51077e5c6cD12121d87903' : '');
  const rawId = env.DERIVE_SUBACCOUNT_ID || (version === 'v2' ? '25923' : '');
  if (!/^0x[\da-fA-F]{40}$/.test(wallet)) throw new Error('DERIVE_WALLET must be the confirmed account owner');
  if (!/^[1-9]\d*$/.test(rawId) || !Number.isSafeInteger(Number(rawId))) throw new Error('DERIVE_SUBACCOUNT_ID must be explicitly confirmed for V3');
  if (env.DERIVE_MAINTENANCE && !['true', 'false'].includes(env.DERIVE_MAINTENANCE)) throw new Error('DERIVE_MAINTENANCE must be true or false');
  const historyFrom = env.DERIVE_HISTORY_FROM || null;
  if (version === 'v3' && (!historyFrom || !Number.isFinite(Date.parse(historyFrom)))) throw new Error('DERIVE_HISTORY_FROM must specify the V3 history boundary as an ISO timestamp');
  return { version, network, baseUrl, domainSeparator, wallet, subaccountId: Number(rawId),
    historyFrom, maintenance: env.DERIVE_MAINTENANCE ? env.DERIVE_MAINTENANCE === 'true' : version === 'v3', headerPrefix: version === 'v3' ? 'X-Derive' : 'X-Lyra' };
}

function authHeaders(config, timestamp, signature) {
  return { [`${config.headerPrefix}Wallet`]: config.wallet,
    [`${config.headerPrefix}Timestamp`]: String(timestamp), [`${config.headerPrefix}Signature`]: signature };
}

let lastNonce = 0n;
function orderNonce(version, now = Date.now()) {
  if (version === 'v2') return Number(`${now}${Math.floor(Math.random() * 1000)}`);
  const candidate = BigInt(now) * 1000000n + BigInt(require('node:crypto').randomInt(1000000));
  lastNonce = candidate > lastNonce ? candidate : lastNonce + 1n;
  return lastNonce.toString();
}

// Reject incomplete/repeated pages instead of trading a partial instrument universe.
async function fetchInstruments(post, version, params) {
  if (version === 'v2') {
    const rows = await post('get_instruments', params);
    if (!Array.isArray(rows)) throw new Error('Instrument response unavailable');
    return rows;
  }
  const rows = [], seen = new Set();
  let expected;
  for (let page = 1; page <= 1000; page++) {
    const result = await post('get_all_instruments', { ...params, page, page_size: 1000 });
    const pagination = result?.pagination;
    if (!Array.isArray(result?.instruments) || !Number.isSafeInteger(pagination?.count) || pagination.count < 0
      || !Number.isSafeInteger(pagination?.num_pages) || pagination.num_pages < 0) throw new Error('Invalid instrument pagination');
    const fingerprint = `${pagination.count}:${pagination.num_pages}`;
    if (expected && expected !== fingerprint) throw new Error('Instrument pagination changed');
    expected = fingerprint;
    for (const row of result.instruments) {
      if (!row.instrument_name || seen.has(row.instrument_name)) throw new Error('Missing or repeated instrument identity');
      seen.add(row.instrument_name); rows.push(row);
    }
    if (page >= pagination.num_pages) {
      if (rows.length !== pagination.count || (pagination.num_pages === 0 && rows.length)) throw new Error('Incomplete instrument listing');
      return rows;
    }
    if (!result.instruments.length) throw new Error('Instrument pagination did not advance');
  }
  throw new Error('Instrument pagination exceeded safety limit');
}

module.exports = { getDeriveConfig, authHeaders, orderNonce, fetchInstruments };
