'use strict';

const axios = require('axios');
const { Interface, AbiCoder, keccak256, formatUnits } = require('ethers');

const POOL_ID = '0x72331fcb696b0151904c03584b66dc8365bc63f8a144d89a773384e3a579ca73';
const MANAGER = '0x000000000004444c5dc75cb358380d2e3de08a90';
const LENS = '0x0000001b173C3bbF3984D417d8614E3eed34865B';
const KEY = ['0x0000000000000000000000000000000000000000', '0xdac17f958d2ee523a2206206994597c13d831ec7', 500, 10, '0x0000000000000000000000000000000000000000'];
const lens = new Interface(['function getPoolTVL(address,(address,address,uint24,int24,address)) view returns ((uint256 coreAmount0,uint256 coreAmount1,uint256 hookReserves0,uint256 hookReserves1,uint256 hookEffective0,uint256 hookEffective1,uint160 sqrtPriceX96,int24 tick,uint128 activeLiquidity,uint256 blockNumber,address statsProvider,uint16 hookPermissions,bool hasCustomAccounting,uint8 statsStatus))']);
if (keccak256(AbiCoder.defaultAbiCoder().encode(['address', 'address', 'uint24', 'int24', 'address'], KEY)) !== POOL_ID) throw new Error('V4 pool key mismatch');

// No signer or transactions. Every read is pinned to a single numbered block.
// ReservesLens reconstructs principal across initialized ticks, not just the
// active range, and excludes uncollected fees, protocol fees and donations.
// Source: Uniswap/v4-periphery src/interfaces/IReservesLens.sol.
async function fetchV4OnchainLiquidity(spotPrice, options = {}) {
  const ethPrice = Number(spotPrice);
  if (!Number.isFinite(ethPrice) || ethPrice <= 0) throw new Error('Missing ETH USD spot valuation');
  const now = options.nowMs ?? Date.now();
  const post = options.post || ((url, body) => axios.post(url, body, { timeout: 12000 }));
  const urls = options.rpcUrls || [...new Set([
    process.env.ETHEREUM_RPC_URL, process.env.ETHEREUM_RPC_FALLBACK_URL,
    'https://ethereum-rpc.publicnode.com', 'https://eth.llamarpc.com',
  ].filter(Boolean))];
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
      const raw = await rpc('eth_call', [{ to: LENS, data: lens.encodeFunctionData('getPoolTVL', [MANAGER, KEY]), gas: '0x1c9c380' }, block.number]);
      const result = lens.decodeFunctionResult('getPoolTVL', raw)[0];
      if (result.blockNumber !== BigInt(blockNumber) || result.sqrtPriceX96 === 0n || result.hasCustomAccounting || result.statsStatus !== 0n) throw new Error('Invalid pool snapshot');
      const amount0 = Number(formatUnits(result.coreAmount0, 18));
      const amount1 = Number(formatUnits(result.coreAmount1, 6));
      const tvl = amount0 * ethPrice + amount1;
      if (![amount0, amount1, tvl].every(Number.isFinite) || amount0 < 0 || amount1 < 0 || tvl <= 0) throw new Error('Invalid reserves');
      return {
        pools: 1, totalLiquidity: tvl, source: 'ethereum_reserves_lens',
        tvlDefinition: 'core_liquidity_principal_v1', sourceBlock: blockNumber,
        sourceBlockHash: block.hash, sourceTimestamp: new Date(timestamp).toISOString(),
        observedAt: new Date(now).toISOString(),
        usdValuation: { ethPriceUSD: ethPrice, usdtPriceUSD: 1, source: 'observed_eth_spot_and_usdt_parity' },
        poolDetails: [{
          id: POOL_ID, liquidity: tvl, liquidityUSD: tvl, feeTier: 500,
          activeLiquidity: result.activeLiquidity.toString(),
          tvlToken0: amount0, tvlToken1: amount1,
          rawToken0: result.coreAmount0.toString(), rawToken1: result.coreAmount1.toString(),
          sqrtPriceX96: result.sqrtPriceX96.toString(), tick: Number(result.tick),
          token0: { symbol: 'ETH', id: KEY[0] }, token1: { symbol: 'USDT', id: KEY[1] },
        }],
      };
    } catch { /* Try the independent RPC provider; never log credentials or replay balances. */ }
  }
  throw new Error('V4 onchain reserves unavailable from configured Ethereum RPC providers');
}

module.exports = { fetchV4OnchainLiquidity, POOL_ID, MANAGER, LENS, KEY, lens };
