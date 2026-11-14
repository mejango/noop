'use strict';

const { parentPort, workerData } = require('node:worker_threads');
const assets = require('./market-assets.json');
if (!parentPort || !assets.slice(1).includes(workerData?.asset)) throw new Error('Invalid asset market worker');

const store = require('./db');
const { collectAsset } = require('./asset-market-collector');
const { collectAssetLiquidity, liquidityDue } = require('./asset-dex-liquidity');
const { assetMomentum } = require('./asset-market-momentum');
const asset = workerData.asset;
let liquidityAt = null;
// Derive allows 25 public requests per 5 seconds per IP (docs.derive.xyz/rate-limits). Every
// worker firing on the same ping exceeded it and starved the ETH loop and the DRV accumulation
// job, so each market starts in its own 5-second slot after the ping. Rows keep the ping's time.
const startDelayMs = assets.indexOf(asset) * 5_000;
parentPort.postMessage({ type: 'ready', asset });

parentPort.on('message', message => {
  if (message?.type !== 'ping') return;
  const at = Date.parse(message.timestamp);
  if (!Number.isFinite(at)) return;
  // Each ping starts its own async request chain; another asset or a slow request cannot stall ETH.
  const collect = () => void collectAsset(asset, undefined, at)
    .then(({ summary, options }) => {
      Object.assign(summary, assetMomentum(store, asset, summary.hour, summary.spot));
      if (store.insertAssetMarketSnapshot(summary, options)) {
        console.log(`Asset ${asset} snapshot ${summary.hour}: ${summary.option_count ?? 0} options (${summary.quote_status})`);
      }
    })
    .catch(error => console.error(`Asset ${asset} collection failed: ${error.message}`));
  if (startDelayMs) setTimeout(collect, startDelayMs); else collect();
  if (!liquidityDue(asset, at, liquidityAt)) return;
  liquidityAt = at;
  void collectAssetLiquidity(asset, message.timestamp)
    .then(sample => store.insertAssetLiquiditySnapshot(sample))
    .catch(error => console.warn(`Asset ${asset} liquidity unavailable: ${error.message}`));
});
