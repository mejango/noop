'use strict';

// Volume is a completed-hour measurement, never a cumulative reserve counter.
const HOUR = 3600000;
const SOURCE = 'geckoterminal_ohlcv';
const VOLUME_POOLS = [
  { dex: 'uniswap_v3_onchain', id: '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640', tokens: ['USDC','WETH'], fee: '0.05%', providerDex: 'uniswap_v3' },
  { dex: 'uniswap_v3_onchain', id: '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8', tokens: ['USDC','WETH'], fee: '0.3%', providerDex: 'uniswap_v3' },
  { dex: 'uniswap_v4', id: '0x72331fcb696b0151904c03584b66dc8365bc63f8a144d89a773384e3a579ca73', tokens: ['ETH','USDT'], fee: '0.05%', providerDex: 'uniswap-v4-ethereum' },
];
const iso = ms => new Date(ms).toISOString();
const completedHour = now => Math.floor(now/HOUR)*HOUR;

function validatePool(pool, body) {
  const data = body?.data, a = data?.attributes;
  const parts = String(a?.name || '').split(/\s*\/\s*|\s+/).filter(Boolean);
  const tokens = parts.slice(0,2).sort().join('/');
  if (String(a?.address).toLowerCase() !== pool.id || data?.relationships?.dex?.data?.id !== pool.providerDex
    || tokens !== [...pool.tokens].sort().join('/') || Number.parseFloat(parts[2]) !== Number.parseFloat(pool.fee)) {
    throw new Error(`GeckoTerminal pool identity mismatch: ${pool.id}`);
  }
}
function parseCandles(pool, body, sinceMs, beforeMs, observedAt) {
  const candles = body?.data?.attributes?.ohlcv_list;
  if (!Array.isArray(candles)) throw new Error('Missing GeckoTerminal OHLCV candles');
  const seen = new Set();
  const rows = [];
  for (const candle of candles) {
    if (!Array.isArray(candle) || candle.length !== 6 || !candle.every(x => typeof x === 'number' && Number.isFinite(x))
      || !Number.isSafeInteger(candle[0]) || candle[0] % 3600 !== 0 || candle.some(x => x < 0) || seen.has(candle[0])) {
      throw new Error('Invalid or duplicate GeckoTerminal hourly candle');
    }
    seen.add(candle[0]);
    const at = candle[0]*1000;
    if (at >= sinceMs && at+HOUR <= beforeMs) rows.push({ pool_id:pool.id,dex:pool.dex,hour:iso(at),volume_usd:candle[5],source:SOURCE,observed_at:observedAt });
  }
  return { rows, oldest: candles.length ? Math.min(...candles.map(c=>c[0]))*1000 : null, count:candles.length };
}

function createVolumeStore(database) {
  database.exec(`CREATE TABLE IF NOT EXISTS dex_volume_hourly (
    pool_id TEXT NOT NULL, dex TEXT NOT NULL, hour TEXT NOT NULL,
    volume_usd REAL NOT NULL CHECK(volume_usd >= 0), source TEXT NOT NULL, observed_at TEXT NOT NULL,
    PRIMARY KEY(pool_id,hour));
    CREATE INDEX IF NOT EXISTS idx_dex_volume_hour ON dex_volume_hourly(hour);
    CREATE TABLE IF NOT EXISTS dex_volume_sync (
      pool_id TEXT PRIMARY KEY, history_since TEXT, updated_at TEXT, last_error TEXT);`);
  const insert = database.prepare(`INSERT INTO dex_volume_hourly VALUES (@pool_id,@dex,@hour,@volume_usd,@source,@observed_at)
    ON CONFLICT(pool_id,hour) DO UPDATE SET volume_usd=excluded.volume_usd,source=excluded.source,observed_at=excluded.observed_at`);
  const batch = database.transaction(rows => { for (const row of rows) insert.run(row); });
  const state = database.prepare('SELECT * FROM dex_volume_sync WHERE pool_id=?');
  const saveState = database.prepare(`INSERT INTO dex_volume_sync VALUES (@pool_id,@history_since,@updated_at,@last_error)
    ON CONFLICT(pool_id) DO UPDATE SET history_since=excluded.history_since,updated_at=excluded.updated_at,last_error=excluded.last_error`);
  return {
    saveDexVolumeBatch:rows=>batch(rows),
    getDexVolumeSyncState:id=>state.get(id),
    saveDexVolumeSyncState:row=>saveState.run(row),
    getDexVolumeHistory:(since,before)=>readVolumeRows(database,since,before),
  };
}
function readVolumeRows(database, since, before = iso(completedHour(Date.now()))) {
  try { return database.prepare('SELECT * FROM dex_volume_hourly WHERE hour >= ? AND hour < ? ORDER BY hour,pool_id').all(since,before); }
  catch (error) { if (/no such table: dex_volume_hourly/.test(error.message)) return []; throw error; }
}
/** @returns {Record<string, number|string>[]} */
function aggregateVolume(rows) {
  const byHour = new Map();
  for (const row of rows) {
    const pool = VOLUME_POOLS.find(p=>p.id===row.pool_id && p.dex===row.dex);
    if (!pool || row.source!==SOURCE || !Number.isFinite(row.volume_usd) || row.volume_usd<0) continue;
    if (!byHour.has(row.hour)) byHour.set(row.hour,new Map());
    byHour.get(row.hour).set(pool.id,row.volume_usd);
  }
  return [...byHour].sort(([a],[b])=>a.localeCompare(b)).map(([hour,volumes]) => {
    const row = { timestamp:iso(Date.parse(hour)+HOUR) };
    for (const dex of ['uniswap_v3_onchain','uniswap_v4']) {
      const pools = VOLUME_POOLS.filter(p=>p.dex===dex);
      if (pools.every(p=>volumes.has(p.id))) {
        row[`${dex}_volDelta`]=pools.reduce((sum,p)=>sum+volumes.get(p.id),0);
        row[`${dex}_volHours`]=1;
      }
    }
    return row;
  }).filter(row=>Object.keys(row).length>1);
}
function volumeSummary(store, nowMs = Date.now()) {
  const before = completedHour(nowMs), since=before-24*HOUR;
  const rows=aggregateVolume(store.getDexVolumeHistory(iso(since),iso(before)));
  return Object.fromEntries(['uniswap_v3_onchain','uniswap_v4'].map(dex => {
    const valid=rows.filter(r=>r[`${dex}_volDelta`]!=null);
    return [dex,{volumeSource:SOURCE,volume24hUSD:valid.length===24 ? valid.reduce((sum,r)=>sum+r[`${dex}_volDelta`],0) : null,
      volumeObservedHours:valid.length,volumeExpectedHours:24,volumeWindowStart:iso(since),volumeWindowEnd:iso(before),
      volumeUnavailableReason:valid.length===24 ? null : 'Incomplete completed-hour volume coverage'}];
  }));
}

function createRequester({ fetchImpl=fetch, sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms)), clock=Date.now } = {}) {
  let lastRequest=0;
  return async url => {
    for (let attempt=0;attempt<3;attempt++) {
      const delay=Math.max(0,6000-(clock()-lastRequest));
      if (delay) await sleep(delay);
      lastRequest=clock();
      const response=await fetchImpl(url,{headers:{Accept:'application/json'},signal:AbortSignal.timeout(15000)});
      if (response.ok) return response.json();
      if (attempt<2 && [429,502,503,504].includes(response.status)) {
        const retryAfter=response.headers?.get('retry-after');
        const seconds=Number(retryAfter);
        const retryMs=retryAfter && Number.isFinite(seconds) ? seconds*1000 : retryAfter ? Date.parse(retryAfter)-clock() : 60000;
        await sleep(Math.max(30000,Math.min(60000,Number.isFinite(retryMs)?retryMs:60000)));
        continue;
      }
      throw new Error(`GeckoTerminal HTTP ${response.status}`);
    }
  };
}
const requestJson=createRequester();
const attempts = new WeakMap();
async function refreshDexVolume(store, { nowMs=Date.now(), days=90, getJson=requestJson, force=false } = {}) {
  const prior=attempts.get(store);
  if (prior?.pending) return prior.pending;
  if (!force && prior && nowMs-prior.at<5*60000) return prior.result;
  const run = async () => {
    const results=[];
    // Sequential pool reads keep the public API comfortably below its request limit.
    const pools=[...VOLUME_POOLS].sort((a,b)=>Number(Boolean(store.getDexVolumeSyncState(a.id)?.history_since))-Number(Boolean(store.getDexVolumeSyncState(b.id)?.history_since)));
    for (const pool of pools) {
      const state=store.getDexVolumeSyncState(pool.id);
      const target=completedHour(nowMs)-days*24*HOUR;
      const needsBackfill=!state?.history_since || Date.parse(state.history_since)>target;
      const since=needsBackfill ? target : completedHour(nowMs)-48*HOUR;
      const base=`https://api.geckoterminal.com/api/v2/networks/eth/pools/${pool.id}`;
      try {
        validatePool(pool,await getJson(base));
        let cursor=completedHour(nowMs)-1, pages=0, count=0;
        while (cursor>=since) {
          if (++pages>Math.ceil(days*24/1000)+2) throw new Error('Volume pagination exceeded bounded history');
          const body=await getJson(`${base}/ohlcv/hour?aggregate=1&limit=1000&currency=usd&include_empty_intervals=true&before_timestamp=${Math.floor(cursor/1000)}`);
          const parsed=parseCandles(pool,body,since,completedHour(nowMs),iso(nowMs));
          if (parsed.oldest!=null && parsed.oldest>cursor) throw new Error('Volume pagination did not advance');
          store.saveDexVolumeBatch(parsed.rows);count+=parsed.rows.length;
          if (parsed.oldest==null || parsed.oldest<=since || parsed.count<1000) break;
          cursor=parsed.oldest-1;
        }
        store.saveDexVolumeSyncState({pool_id:pool.id,history_since:needsBackfill?iso(target):state.history_since,updated_at:iso(nowMs),last_error:null});
        results.push({pool:pool.id,status:'ok',hours:count,pages});
      } catch(error) {
        store.saveDexVolumeSyncState({pool_id:pool.id,history_since:state?.history_since??null,updated_at:state?.updated_at??null,last_error:error.message});
        results.push({pool:pool.id,status:'unavailable',reason:error.message});
      }
    }
    return results;
  };
  const entry={at:nowMs,pending:null,result:null};
  attempts.set(store,entry);
  entry.pending=run();
  try { entry.result=await entry.pending;return entry.result; }
  finally { entry.pending=null; }
}
module.exports={HOUR,SOURCE,VOLUME_POOLS,validatePool,parseCandles,createVolumeStore,readVolumeRows,aggregateVolume,volumeSummary,refreshDexVolume,createRequester};
