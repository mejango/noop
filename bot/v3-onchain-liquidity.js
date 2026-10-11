'use strict';
const axios = require('axios');
const { Interface, formatUnits } = require('ethers');
const USDC = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48';
const WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2';
const POOLS = [
  { id: '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640', fee: 500 },
  { id: '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8', fee: 3000 },
];
const token = new Interface(['function balanceOf(address) view returns(uint256)', 'function decimals() view returns(uint8)']);
const pool = new Interface(['function token0() view returns(address)', 'function token1() view returns(address)', 'function fee() view returns(uint24)', 'function liquidity() view returns(uint128)', 'function slot0() view returns(uint160,int24,uint16,uint16,uint16,uint8,bool)']);

async function fetchV3OnchainLiquidity(spotPrice, options = {}) {
  const ethPrice = Number(spotPrice);
  if (!Number.isFinite(ethPrice) || ethPrice <= 0) throw new Error('Missing ETH USD spot valuation');
  const now = options.nowMs ?? Date.now();
  const post = options.post || ((url, body) => axios.post(url, body, { timeout: 12000 }));
  const urls = options.rpcUrls || [...new Set([process.env.ETHEREUM_RPC_URL, process.env.ETHEREUM_RPC_FALLBACK_URL,
    'https://ethereum-rpc.publicnode.com', 'https://eth.llamarpc.com'].filter(Boolean))];
  for (const url of urls) {
    try {
      const rpc = async (method, params) => {
        const response = await post(url, { jsonrpc: '2.0', id: 1, method, params });
        if (response.data?.error || response.data?.result == null) throw new Error('RPC read failed');
        return response.data.result;
      };
      if (BigInt(await rpc('eth_chainId', [])) !== 1n) throw new Error('Wrong chain');
      const block = await rpc('eth_getBlockByNumber', ['latest', false]);
      const timestamp = Number(BigInt(block.timestamp)) * 1000;
      const blockNumber = Number(BigInt(block.number));
      if (!Number.isSafeInteger(blockNumber) || !Number.isFinite(timestamp) || timestamp > now + 60000 || now - timestamp > 15 * 60000) throw new Error('Stale block');
      const read = async (address, abi, method, args = []) => abi.decodeFunctionResult(method,
        await rpc('eth_call', [{ to: address, data: abi.encodeFunctionData(method, args) }, block.number]));
      const [usdcDecimals, wethDecimals] = await Promise.all([read(USDC, token, 'decimals'), read(WETH, token, 'decimals')]);
      if (usdcDecimals[0] !== 6n || wethDecimals[0] !== 18n) throw new Error('Unexpected token decimals');
      const poolDetails = [];
      for (const expected of POOLS) {
        const [token0, token1, fee, active, slot0, raw0, raw1] = await Promise.all([
          read(expected.id, pool, 'token0'), read(expected.id, pool, 'token1'), read(expected.id, pool, 'fee'),
          read(expected.id, pool, 'liquidity'), read(expected.id, pool, 'slot0'),
          read(USDC, token, 'balanceOf', [expected.id]), read(WETH, token, 'balanceOf', [expected.id]),
        ]);
        if (token0[0].toLowerCase() !== USDC || token1[0].toLowerCase() !== WETH || fee[0] !== BigInt(expected.fee) || slot0[0] === 0n) throw new Error('Unexpected pool identity');
        const amount0 = Number(formatUnits(raw0[0], 6));
        const amount1 = Number(formatUnits(raw1[0], 18));
        const tvl = amount0 + amount1 * ethPrice;
        if (![amount0, amount1, tvl].every(Number.isFinite) || amount0 < 0 || amount1 < 0 || tvl <= 0) throw new Error('Invalid pool balances');
        poolDetails.push({ id: expected.id, liquidity: tvl, liquidityUSD: tvl, feeTier: expected.fee,
          activeLiquidity: active[0].toString(), sqrtPriceX96: slot0[0].toString(), tick: Number(slot0[1]),
          tvlToken0: amount0, tvlToken1: amount1, rawToken0: raw0[0].toString(), rawToken1: raw1[0].toString(),
          token0: { symbol: 'USDC', id: USDC }, token1: { symbol: 'WETH', id: WETH } });
      }
      return { pools: POOLS.length, totalLiquidity: poolDetails.reduce((sum, p) => sum + p.liquidityUSD, 0),
        source: 'ethereum_pool_token_balances', tvlDefinition: 'pool_token_balances_v1', sourceBlock: blockNumber,
        sourceBlockHash: block.hash, sourceTimestamp: new Date(timestamp).toISOString(), observedAt: new Date(now).toISOString(),
        usdValuation: { ethPriceUSD: ethPrice, usdcPriceUSD: 1, source: 'observed_eth_spot_and_usdc_parity' }, poolDetails };
    } catch { /* Independent provider failover, with no stale balance replay. */ }
  }
  throw new Error('V3 onchain balances unavailable from configured Ethereum RPC providers');
}

// Indexed swap activity stays separate from the authoritative reserve read.
function attachV3SubgraphActivity(sample, indexedPools) {
  if (!Array.isArray(indexedPools) || indexedPools.length !== POOLS.length) throw new Error('Incomplete V3 volume pool set');
  const byId = new Map(indexedPools.map(p => [String(p.id).toLowerCase(), p]));
  if (byId.size !== POOLS.length || POOLS.some(p => !byId.has(p.id))) throw new Error('Wrong V3 volume pool set');
  const values = POOLS.map(p => byId.get(p.id));
  const volumes = values.map(p => p.volumeUSD == null ? NaN : Number(p.volumeUSD));
  const counts = values.map(p => p.txCount == null ? NaN : Number(p.txCount));
  if (volumes.some(n => !Number.isFinite(n) || n < 0) || counts.some(n => !Number.isSafeInteger(n) || n < 0)) throw new Error('Invalid V3 activity');
  return { ...sample, totalVolume: volumes.reduce((a, b) => a + b, 0), totalTxCount: counts.reduce((a, b) => a + b, 0),
    volumeSource: 'uniswap_v3_subgraph', volumeObservedAt: new Date().toISOString() };
}
module.exports = { fetchV3OnchainLiquidity, attachV3SubgraphActivity, POOLS, USDC, WETH, token, pool };
