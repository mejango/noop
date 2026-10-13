'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const Database=require('better-sqlite3');
const {VOLUME_POOLS,HOUR,SOURCE,validatePool,parseCandles,createVolumeStore,aggregateVolume,volumeSummary,refreshDexVolume}=require('../bot/dex-volume');
const NOW=Date.parse('2026-10-12T12:30:00Z'), END=Math.floor(NOW/HOUR)*HOUR;
const iso=ms=>new Date(ms).toISOString();
const candle=(at,v=10)=>[at/1000,100,110,90,105,v];
const body=rows=>({data:{attributes:{ohlcv_list:rows}}});
const meta=p=>({data:{attributes:{address:p.id,name:`${p.tokens.join(' / ')} ${p.fee}`},relationships:{dex:{data:{id:p.providerDex}}}}});
const row=(p,at,v=10)=>({pool_id:p.id,dex:p.dex,hour:iso(at),volume_usd:v,source:SOURCE,observed_at:iso(NOW)});

test('volume pool selection matches the actual onchain collector pool set',()=>{
  const {POOLS}=require('../bot/v3-onchain-liquidity');
  const {POOL_ID}=require('../bot/v4-onchain-liquidity');
  assert.deepEqual(VOLUME_POOLS.slice(0,2).map(p=>p.id),POOLS.map(p=>p.id));
  assert.equal(VOLUME_POOLS[2].id,POOL_ID);
});
test('provider metadata must match exact pool, DEX, currencies and fee',()=>{
  for(const p of VOLUME_POOLS) validatePool(p,meta(p));
  for(const change of [{address:'other'},{name:'ETH / USDT 0.3%'},{name:'WBTC / USDT 0.05%'}]) {
    const value=meta(VOLUME_POOLS[2]);Object.assign(value.data.attributes,change);
    assert.throws(()=>validatePool(VOLUME_POOLS[2],value),/identity mismatch/);
  }
});
test('hourly volume excludes incomplete and out-of-range candles and keeps genuine zero',()=>{
  const parsed=parseCandles(VOLUME_POOLS[2],body([candle(END,999),candle(END-HOUR,0),candle(END-2*HOUR,40)]),END-HOUR,END,iso(NOW));
  assert.equal(parsed.rows.length,1);
  assert.equal(parsed.rows[0].volume_usd,0);
  assert.equal(parsed.rows[0].hour,iso(END-HOUR));
  for(const invalid of [body([[1,1,2,1,1,2]]),body([candle(END-HOUR,-1)]),body([candle(END-HOUR),candle(END-HOUR)]),{}]) {
    assert.throws(()=>parseCandles(VOLUME_POOLS[0],invalid,0,END,iso(NOW)));
  }
});
test('V3 aggregates matching hours from both pools; missing pools stay unknown',()=>{
  const values=aggregateVolume([row(VOLUME_POOLS[0],END-2*HOUR,20),row(VOLUME_POOLS[1],END-2*HOUR,30),row(VOLUME_POOLS[0],END-HOUR,40),row(VOLUME_POOLS[2],END-HOUR,0)]);
  assert.equal(values[0].uniswap_v3_onchain_volDelta,50);
  assert.equal(values[1].uniswap_v3_onchain_volDelta,undefined);
  assert.equal(values[1].uniswap_v4_volDelta,0);
});
test('idempotent upserts correct revisions without double counting or touching liquidity',()=>{
  const db=new Database(':memory:');
  try {
    const store=createVolumeStore(db);
    store.saveDexVolumeBatch([row(VOLUME_POOLS[2],END-HOUR,10)]);
    store.saveDexVolumeBatch([row(VOLUME_POOLS[2],END-HOUR,12)]);
    const values=store.getDexVolumeHistory(iso(END-2*HOUR),iso(END));
    assert.equal(values.length,1);assert.equal(values[0].volume_usd,12);
    assert.equal(db.prepare("SELECT count(*) n FROM sqlite_master WHERE name='onchain_data'").get().n,0);
  } finally {db.close();}
});
test('24h volume requires all completed hours and both V3 pools',()=>{
  const db=new Database(':memory:');
  try {
    const store=createVolumeStore(db);
    store.saveDexVolumeBatch(VOLUME_POOLS.flatMap(p=>Array.from({length:24},(_,i)=>row(p,END-(i+1)*HOUR,10))));
    assert.equal(volumeSummary(store,NOW).uniswap_v3_onchain.volume24hUSD,480);
    assert.equal(volumeSummary(store,NOW).uniswap_v4.volume24hUSD,240);
    db.prepare('DELETE FROM dex_volume_hourly WHERE pool_id=? AND hour=?').run(VOLUME_POOLS[0].id,iso(END-HOUR));
    assert.equal(volumeSummary(store,NOW).uniswap_v3_onchain.volume24hUSD,null);
    assert.equal(volumeSummary(store,NOW).uniswap_v3_onchain.volumeObservedHours,23);
  } finally {db.close();}
});
test('backfill paginates, respects hour boundaries, and later revises only recent hours',async()=>{
  const db=new Database(':memory:');
  try {
    const store=createVolumeStore(db),calls=[];
    const getJson=async url=>{
      calls.push(url);
      const p=VOLUME_POOLS.find(p=>url.includes(p.id));
      if(!url.includes('/ohlcv/')) return meta(p);
      const cursor=Number(new URL(url).searchParams.get('before_timestamp'))*1000;
      const end=Math.floor(cursor/HOUR)*HOUR;
      const rows=Array.from({length:1000},(_,i)=>candle(end-i*HOUR));
      return body(rows);
    };
    const result=await refreshDexVolume(store,{nowMs:NOW,days:90,getJson,force:true});
    assert.ok(result.every(r=>r.status==='ok' && r.pages===3 && r.hours===2160));
    assert.equal(store.getDexVolumeHistory(iso(END-90*24*HOUR),iso(END)).length,6480);
    calls.length=0;
    await refreshDexVolume(store,{nowMs:NOW+5*60000,days:90,getJson,force:true});
    assert.equal(calls.filter(u=>u.includes('/ohlcv/')).length,3);
    assert.ok(calls.filter(u=>u.includes('/ohlcv/')).every(u=>u.includes('currency=usd')&&u.includes('include_empty_intervals=true')));
  }finally {db.close();}
});
test('outages preserve validated old candles without fabricating new zero bars',async()=>{
  const db=new Database(':memory:');
  try {
    const store=createVolumeStore(db);store.saveDexVolumeBatch([row(VOLUME_POOLS[2],END-HOUR,10)]);
    const result=await refreshDexVolume(store,{nowMs:NOW,getJson:async()=>{throw new Error('HTTP 429');},force:true});
    assert.ok(result.every(r=>r.status==='unavailable'));
    assert.equal(store.getDexVolumeHistory(iso(END-24*HOUR),iso(END)).length,1);
    assert.equal(volumeSummary(store,NOW).uniswap_v4.volume24hUSD,null);
    assert.equal(store.getDexVolumeSyncState(VOLUME_POOLS[2].id).history_since,null);
  }finally {db.close();}
});

test('public API requests are paced and retry rate limits with bounded waits',async()=>{
  const {createRequester}=require('../bot/dex-volume');
  let now=10000,calls=0;const waits=[];
  const request=createRequester({clock:()=>now,sleep:async ms=>{waits.push(ms);now+=ms;},fetchImpl:async()=>{
    calls++;
    return calls===1 ? {ok:false,status:429,headers:{get:()=> '10'}} : {ok:true,json:async()=>({ok:true})};
  }});
  assert.deepEqual(await request('https://fixture'),{ok:true});
  assert.deepEqual(await request('https://fixture'),{ok:true});
  assert.deepEqual(waits,[30000,6000]);
  const failed=createRequester({clock:()=>now,sleep:async ms=>{now+=ms;},fetchImpl:async()=>({ok:false,status:429,headers:{get:()=> '9999'}})});
  await assert.rejects(()=>failed('https://fixture'),/HTTP 429/);
});

test('chart API preserves hourly totals in long ranges independently of liquidity history',async()=>{
  const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
  const ts=require('../dashboard/node_modules/typescript');
  const values=[{timestamp:iso(END-2*HOUR),uniswap_v3_onchain_volDelta:10,uniswap_v4_volDelta:5},
    {timestamp:iso(END-HOUR),uniswap_v3_onchain_volDelta:20,uniswap_v4_volDelta:0}];
  const db=new Proxy({getLiquidityVolumeOverTime:()=>values,getOptionsCoverage:()=>({})},
    {get:(target,key)=>key in target?target[key]:()=>[]});
  const dependencies={
    'next/server':{NextResponse:{json:body=>({body})}},'@/lib/db':db,
    '@/lib/limits':{CHART_ROW_LIMITS:{'14d':{prices:12000,heatmap:12000}}},
    '@/lib/response-cache':{cachedJsonRoute:(_r,_k,loader)=>loader()},
    '@/lib/dashboard-ranges':{dashboardRangeMs:()=>90*24*HOUR},
  };
  const code=ts.transpileModule(fs.readFileSync(path.join(__dirname,'../dashboard/src/app/api/chart/route.ts'),'utf8'),
    {compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
  const exports={};
  vm.runInNewContext(code,{exports,Date,Set,Map,console,require:name=>{
    assert.ok(name in dependencies,name);return dependencies[name];
  }});
  const response=await exports.GET({nextUrl:new URL('https://fixture/api/chart?range=90d')});
  assert.equal(response.body.liquidity.length,0);
  assert.equal(response.body.liquidityVolume,values,'volume candles are neither averaged nor differenced as counters');
});
