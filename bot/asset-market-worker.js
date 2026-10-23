'use strict';

const { parentPort, workerData } = require('node:worker_threads');
const assets = require('./market-assets.json');
if (!parentPort || !assets.slice(1).includes(workerData?.asset)) throw new Error('Invalid asset market worker');

const store = require('./db');
const { collectAsset } = require('./asset-market-collector');
const { collectAssetLiquidity } = require('./asset-dex-liquidity');
const { assetMomentum } = require('./asset-market-momentum');
const asset = workerData.asset;
parentPort.postMessage({ type: 'ready', asset });

parentPort.on('message', message => {
  if (message?.type !== 'ping') return;
  const at = Date.parse(message.timestamp);
  if (!Number.isFinite(at)) return;
  // Each ping starts its own async request chain; another asset or a slow request cannot stall ETH.
  void collectAsset(asset, undefined, at)
    .then(({ summary, options }) => {
      Object.assign(summary, assetMomentum(store, asset, summary.hour, summary.spot));
      if (store.insertAssetMarketSnapshot(summary, options)) {
        console.log(`Asset ${asset} snapshot ${summary.hour}: ${summary.option_count ?? 0} options (${summary.quote_status})`);
      }
    })
    .catch(error => console.error(`Asset ${asset} collection failed: ${error.message}`));
  void collectAssetLiquidity(asset, message.timestamp)
    .then(sample => store.insertAssetLiquiditySnapshot(sample))
    .catch(error => console.warn(`Asset ${asset} liquidity unavailable: ${error.message}`));
});
