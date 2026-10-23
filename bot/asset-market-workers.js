'use strict';

const path = require('node:path');
const { Worker } = require('node:worker_threads');
const assets = require('./market-assets.json').filter(asset => asset !== 'ETH');

let dispatch = null;

function pingAssetMarkets(timestamp) {
  dispatch?.(timestamp);
}

function startAssetMarketWorkers({ WorkerClass = Worker, retryMs = 5000 } = {}) {
  if (dispatch) throw new Error('Asset market workers already started');
  const workers = new Map();
  let stopped = false;
  const spawn = asset => {
    if (stopped) return;
    let worker;
    try {
      worker = new WorkerClass(path.join(__dirname, 'asset-market-worker.js'), { workerData: { asset } });
    } catch (error) {
      console.error(`Asset ${asset} worker could not start: ${error.message}`);
      const timer = setTimeout(() => spawn(asset), retryMs);
      timer.unref?.();
      return;
    }
    workers.set(asset, worker);
    worker.on('message', message => {
      if (message?.type === 'ready') console.log(`Asset ${asset} worker ready`);
    });
    worker.on('error', error => console.error(`Asset ${asset} worker failed: ${error.message}`));
    worker.on('exit', code => {
      if (workers.get(asset) !== worker) return;
      workers.delete(asset);
      if (!stopped) {
        console.error(`Asset ${asset} worker exited (${code}); restarting`);
        const timer = setTimeout(() => spawn(asset), retryMs);
        timer.unref?.();
      }
    });
  };
  assets.forEach(spawn);
  dispatch = timestamp => {
    for (const [asset, worker] of workers) {
      try { worker.postMessage({ type: 'ping', timestamp }); }
      catch (error) { console.error(`Asset ${asset} ping failed: ${error.message}`); }
    }
  };
  return () => {
    stopped = true;
    dispatch = null;
    for (const worker of workers.values()) void worker.terminate().catch(() => {});
    workers.clear();
  };
}

module.exports = { pingAssetMarkets, startAssetMarketWorkers };
