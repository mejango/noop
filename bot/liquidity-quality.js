'use strict';

const LIQUIDITY_DATA_NOTICE = 'Legacy Uniswap v3 and v4 subgraph TVL is quarantined as unverified; V4 recorded negative token balances, and V3 materially disagreed with direct balances and GeckoTerminal. New v4 readings are onchain core liquidity principal, excluding uncollected fees and donations. Do not interpret a source change or missing history as liquidity withdrawal. V3 readings use actual pool token balances, including accrued fees and donations; legacy V3 subgraph history has a separate definition. USD values use the observed ETH spot price and USDT/USDC at $1; TVL changes also reflect token prices. Prior journal/wiki conclusions based on legacy v3/v4 TVL require reassessment.';

function invalidDexReason(name, dex) {
  if (!dex || typeof dex !== 'object') return 'missing_sample';
  if (dex.error || dex.invalid) return dex.invalidReason || dex.error || 'invalid_sample';
  if (dex.stale) return 'stale_sample';
  if (name === 'uniswap_v4' && Number(dex.pools) > 1) return 'incompatible_pool_set';
  for (const pool of dex.poolDetails || []) {
    for (const key of ['tvlToken0', 'tvlToken1']) {
      if (pool[key] != null && (!Number.isFinite(Number(pool[key])) || Number(pool[key]) < 0)) {
        return 'invalid_token_balances';
      }
    }
  }
  if (name === 'uniswap_v3' && dex.tvlDefinition !== 'pool_token_balances_v1') return 'unverified_legacy_v3_tvl';
  if (name === 'uniswap_v4' && dex.tvlDefinition !== 'core_liquidity_principal_v1') return 'unverified_legacy_v4_tvl';
  if (dex.totalLiquidity == null || !Number.isFinite(Number(dex.totalLiquidity)) || Number(dex.totalLiquidity) <= 0) return 'invalid_tvl';
  return null;
}

function liquidityChartSeries(name, dex) {
  return name === 'uniswap_v3' && dex?.tvlDefinition === 'pool_token_balances_v1' ? 'uniswap_v3_onchain' : name;
}

function liquidityDefinition(dex) {
  return dex?.tvlDefinition || 'subgraph_token_balances';
}

function unknownLiquidityFlow(excludedDexes = []) {
  return {
    algorithm: 'matched_pool_definitions_v1', direction: 'unknown', magnitude: null, confidence: 0, currentTotal: null,
    dataReliability: 'unreliable', excludedDexes,
    timeframes: Object.fromEntries(['hourly', 'daily', 'weekly'].map(name =>
      [name, { direction: 'unknown', change: null, total: null, dexes: [] }])),
  };
}

function calculateValidatedLiquidityFlow(currentData, historicalData, nowMs = Date.now()) {
  const current = currentData?.dexes || {};
  const validNames = Object.keys(current).filter(name => !invalidDexReason(name, current[name]));
  const excluded = Object.keys(current).filter(name => !validNames.includes(name));
  if (!validNames.length || !historicalData?.length) return unknownLiquidityFlow(excluded);
  const currentTotal = validNames.reduce((sum, name) => sum + Number(current[name].totalLiquidity), 0);
  const direction = change => change > 0.005 ? 'inflow' : change < -0.005 ? 'outflow' : 'stable';
  const timeframes = {};
  let weighted = 0;
  let weight = 0;
  for (const [name, hours, importance] of [['hourly', 1, 0.2], ['daily', 24, 0.3], ['weekly', 168, 0.5]]) {
    const target = nowMs - hours * 3600000;
    const historical = historicalData.reduce((best, sample) => {
      const time = Date.parse(sample.timestamp);
      if (!Number.isFinite(time) || time >= nowMs) return best;
      return !best || Math.abs(time - target) < Math.abs(Date.parse(best.timestamp) - target) ? sample : best;
    }, null);
    const comparable = historical && Math.abs(Date.parse(historical.timestamp) - target) <= 15 * 60000
      ? validNames.filter(dex => !invalidDexReason(dex, historical.dexes?.[dex])
        && liquidityDefinition(current[dex]) === liquidityDefinition(historical.dexes[dex]))
      : [];
    if (!comparable.length) {
      timeframes[name] = { direction: 'unknown', change: null, total: null, dexes: [] };
      continue;
    }
    const before = comparable.reduce((sum, dex) => sum + Number(historical.dexes[dex].totalLiquidity), 0);
    const after = comparable.reduce((sum, dex) => sum + Number(current[dex].totalLiquidity), 0);
    const change = (after - before) / before;
    timeframes[name] = { direction: direction(change), change, total: before, dexes: comparable };
    weighted += change * importance;
    weight += importance;
  }
  if (!weight) return { ...unknownLiquidityFlow(excluded), currentTotal };
  const weightedChange = weighted / weight;
  const complete = excluded.length === 0 && Object.values(timeframes).every(tf => tf.dexes.length === validNames.length);
  return {
    algorithm: 'matched_pool_definitions_v1',
    direction: weightedChange > 0.01 ? 'inflow' : weightedChange < -0.01 ? 'outflow' : 'stable',
    magnitude: Math.abs(weightedChange), confidence: Math.min(0.95, 0.5 + Math.abs(weightedChange) * 20) * weight,
    weightedChange, currentTotal, excludedDexes: excluded,
    dataReliability: complete ? 'reliable' : excluded.length ? 'unreliable' : 'partial', timeframes,
  };
}

function dexForAdvisor(name, dex) {
  const reason = invalidDexReason(name, dex);
  const pools = dex?.poolDetails || [];
  const pool = pools[0];
  return {
    dex: name, status: reason ? 'unavailable' : 'valid', unavailable_reason: reason,
    total_liquidity: reason ? null : Number(dex.totalLiquidity),
    total_volume_usd: reason ? null : dex.totalVolume ?? null,
    total_tx_count: reason ? null : dex.totalTxCount ?? null,
    volume_24h_usd: dex?.volume24hUSD ?? null,
    volume_window_start: dex?.volumeWindowStart ?? null, volume_window_end: dex?.volumeWindowEnd ?? null,
    volume_observed_hours: dex?.volumeObservedHours ?? null, volume_expected_hours: dex?.volumeExpectedHours ?? null,
    volume_source: dex?.volumeSource || (dex?.totalVolume != null ? 'uniswap_subgraph' : null),
    volume_unavailable_reason: dex?.volumeUnavailableReason ?? null,
    source: dex?.source || 'uniswap_subgraph', tvl_definition: liquidityDefinition(dex),
    source_block: dex?.sourceBlock ?? null, source_timestamp: dex?.sourceTimestamp ?? null,
    usd_valuation: dex?.usdValuation ?? null, pool_count: pools.length || dex?.pools || null,
    top_pool: pool ? {
      pair: pool.token0?.symbol && pool.token1?.symbol ? `${pool.token0.symbol}/${pool.token1.symbol}` : null,
      liquidity: reason ? null : pool.liquidityUSD ?? pool.liquidity ?? null,
      token0_amount: reason ? null : pool.tvlToken0 ?? null,
      token1_amount: reason ? null : pool.tvlToken1 ?? null,
      active_liquidity: reason ? null : pool.activeLiquidity ?? null,
      fee_tier_bps: pool.feeTier == null ? null : Number(pool.feeTier) / 100,
    } : null,
  };
}

function sanitizeOnchainRow(row) {
  if (!row?.raw_data) return row;
  try {
    const raw = typeof row.raw_data === 'string' ? JSON.parse(row.raw_data) : structuredClone(row.raw_data);
    const dexes = raw?.dexLiquidity?.dexes;
    let invalid = false;
    for (const [name, dex] of Object.entries(dexes || {})) {
      const reason = invalidDexReason(name, dex);
      if (!reason || reason === 'missing_sample') continue;
      invalid = true;
      dexes[name] = { error: reason, invalid: true, invalidReason: reason,
        source: dex?.source || 'uniswap_subgraph', tvlDefinition: liquidityDefinition(dex),
        pools: dex?.pools ?? null };
    }
    if (!invalid) return row;
    const excluded = Object.keys(dexes).filter(name => dexes[name]?.invalid);
    const previous = raw.dexLiquidity.flowAnalysis;
    // A flow computed before a venue was quarantined remains untrusted even
    // when it already bears the current algorithm marker.
    if (previous?.algorithm !== 'matched_pool_definitions_v1'
      || excluded.some(name => !previous.excludedDexes?.includes(name))) {
      raw.dexLiquidity.flowAnalysis = unknownLiquidityFlow(excluded);
    }
    raw.dexLiquidity.dataQualityNotice = LIQUIDITY_DATA_NOTICE;
    const flow = raw.dexLiquidity.flowAnalysis;
    return { ...row, raw_data: JSON.stringify(raw), liquidity_flow_direction: flow.direction,
      liquidity_flow_magnitude: flow.magnitude, liquidity_flow_confidence: flow.confidence };
  } catch { return row; }
}

module.exports = { liquidityChartSeries, invalidDexReason, liquidityDefinition, unknownLiquidityFlow, calculateValidatedLiquidityFlow, dexForAdvisor, sanitizeOnchainRow, LIQUIDITY_DATA_NOTICE };
