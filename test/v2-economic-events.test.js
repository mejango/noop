'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const Database = require('better-sqlite3');
const {createEconomicStore,normalizeV2Trade,syncV2Trades,syncV2TradesProgressively,multiply,getEconomicHistory,coversRange} = require('../bot/economic-events');
const trade = (id='one') => ({trade_id:id,subaccount_id:25923,is_transfer:false,tx_status:'settled',instrument_name:'ETH-20261127-1600-P',direction:'buy',trade_amount:'0.3',trade_price:'0.2',timestamp:Date.parse('2026-09-11T10:00:00Z')});
const from='2026-09-11T09:00:00Z',to='2026-09-11T11:00:00Z';
function harness(t) {const db=new Database(':memory:');t.after(()=>db.close());return {db,store:createEconomicStore(db)};}
test('venue trades preserve exact premium and unknown fee/PnL without manufacturing profit', t=>{
 const {db,store}=harness(t); const event=normalizeV2Trade(trade(),25923);
 assert.equal(event.cashflow_usd,'-0.06');assert.equal(event.fee_usd,null);assert.equal(event.realized_pnl_usd,null);
 assert.equal(multiply('9007199254740993.123456789','0.000000001'),'9007199.254740993123456789');
 store.recordEvents([event,event]);assert.equal(db.prepare('select count(*) n from economic_events').get().n,1);
 assert.equal(getEconomicHistory(db,25923,from,to).coverage.trades,false);
});
test('conflicting identities and mixed accounts roll back economic batches',t=>{
 const {db,store}=harness(t);const event=normalizeV2Trade(trade(),25923);store.recordEvents([event]);
 assert.throws(()=>store.recordEvents([normalizeV2Trade(trade('two'),25923),{...event,cashflow_usd:'-99'}]),/Conflicting/);
 assert.equal(db.prepare('select count(*) n from economic_events').get().n,1);
 assert.throws(()=>store.recordBatch([event],{account_id:'1',dataset:'trades',from_timestamp:from,to_timestamp:to,complete:true}),/outside/);
 assert.throws(()=>normalizeV2Trade({...trade(),subaccount_id:2},25923),/mismatch/);
});
test('pagination failure cannot mark a window complete or partially persist fills',async t=>{
 const {db,store}=harness(t);
 await assert.rejects(syncV2Trades({store,accountId:25923,from,to,pageSize:1,post:async body=>body.page===1?{subaccount_id:25923,trades:[trade()],num_pages:2}:Promise.reject(new Error('timeout'))}),/timeout/);
 assert.equal(db.prepare('select count(*) n from economic_events').get().n,0);
 assert.equal(getEconomicHistory(db,25923,from,to).coverage.trades,false);
});
test('complete paginated history is idempotent and does not imply settlements or transfers coverage',async t=>{
 const {db,store}=harness(t);const post=async body=>({subaccount_id:25923,trades:[trade(body.page===1?'one':'two')],num_pages:2});
 const args={store,accountId:25923,from,to,pageSize:1,post};
 assert.equal((await syncV2Trades(args)).inserted,2);assert.equal((await syncV2Trades(args)).inserted,0);
 assert.deepEqual(getEconomicHistory(db,25923,from,to).coverage,{trades:true,settlements:false,transfers:false});
 assert.equal(getEconomicHistory(db,5,from,to).events.length,0);
});
test('coverage must span the whole interval without gaps',()=>{
 assert.equal(coversRange([{complete:1,from_timestamp:from,to_timestamp:'2026-09-11T10:00:00.000Z'},{complete:1,from_timestamp:'2026-09-11T10:01:00.000Z',to_timestamp:to}],from,to),false);
 assert.equal(coversRange([{complete:1,from_timestamp:'2026-09-11T09:00:00.000Z',to_timestamp:'2026-09-11T10:00:00.000Z'},{complete:1,from_timestamp:'2026-09-11T10:00:00.000Z',to_timestamp:'2026-09-11T11:00:00.000Z'}],from,to),true);
});
test('external exposure changes are prospective and never rewrite old portfolio history',t=>{
 const {db,store}=harness(t);store.recordExposure(25923,'20',from);store.recordExposure(25923,'20','2026-09-11T09:30:00Z');store.recordExposure(25923,'30',to);
 assert.equal(db.prepare('select count(*) n from exposure_history').get().n,2);
 assert.equal(getEconomicHistory(db,25923,'2026-09-10',to).exposureKnown,false);
 assert.equal(getEconomicHistory(db,25923,from,to).exposureKnown,true);
});

test('repeated last pages and conflicting duplicate trades never establish coverage',async t=>{
 const {db,store}=harness(t);
 await assert.rejects(syncV2Trades({store,accountId:25923,from,to,pageSize:1,post:async()=>({subaccount_id:25923,trades:[trade()],num_pages:2})}),/repeated/);
 assert.equal(getEconomicHistory(db,25923,from,to).coverage.trades,false);
 await assert.rejects(syncV2Trades({store,accountId:25923,from,to,post:async()=>({subaccount_id:25923,trades:[trade(),{...trade(),trade_price:'99'}],num_pages:1})}),/Conflicting/);
});
test('failed or transfer records cannot be booked as executed option cashflow',()=>{
 assert.throws(()=>normalizeV2Trade({...trade(),is_transfer:true},25923),/Transfer/);
 assert.throws(()=>normalizeV2Trade({...trade(),tx_status:'reverted'},25923),/unresolved/);
});
test('external exposure is scoped to the operating account',t=>{
 const {db,store}=harness(t);store.recordExposure(25923,'20',from);
 assert.deepEqual(getEconomicHistory(db,999,from,to).exposureHistory,[]);
 assert.equal(getEconomicHistory(db,999,from,to).exposureKnown,false);
});

test('unsafe accounts are rejected before requests or empty coverage writes',async t=>{
 const {db,store}=harness(t);let called=false;
 await assert.rejects(syncV2Trades({store,accountId:'9007199254740993',from,to,post:async()=>{called=true;return {subaccount_id:25923,trades:[]};}}),/account/);
 assert.equal(called,false);
 assert.throws(()=>store.recordBatch([],{account_id:'',dataset:'trades',from_timestamp:from,to_timestamp:to,complete:true}),/account/);
});
test('large histories retain a contiguous completed prefix under bounded request budgets',async t=>{
 const {store}=harness(t);
 const post=async body=>{
  if (body.to_timestamp-body.from_timestamp>1000) return {subaccount_id:25923,trades:[{...trade(String(body.from_timestamp)),timestamp:body.from_timestamp}],num_pages:10};
  return {subaccount_id:25923,trades:[],num_pages:0};
 };
 await assert.rejects(syncV2TradesProgressively({store,accountId:25923,from:0,to:8000,post,maxPages:1,maxRequests:5}),/budget/);
 assert.equal(store.latestCoverage(25923,'trades'),'1970-01-01T00:00:02.000Z');
});

test('pagination count and page declarations remain consistent before complete coverage',async t=>{
 const {db,store}=harness(t);
 const cases=[
  async()=>({subaccount_id:25923,trades:[trade()],pagination:{num_pages:1,count:2}}),
  async body=>({subaccount_id:25923,trades:[trade(String(body.page))],pagination:{num_pages:body.page===1?2:1,count:2}}),
  async body=>({subaccount_id:25923,trades:[trade(String(body.page))],pagination:{num_pages:2,count:body.page===1?2:3}}),
  async()=>({subaccount_id:25923,trades:[trade()],pagination:{num_pages:0,count:1}}),
  async()=>({subaccount_id:25923,trades:[],pagination:{num_pages:0,count:'invalid'}}),
 ];
 for(const post of cases){
  await assert.rejects(syncV2Trades({store,accountId:25923,from,to,pageSize:1,post}),/pagination|pages/);
  assert.equal(getEconomicHistory(db,25923,from,to).coverage.trades,false);
 }
 assert.equal(db.prepare('SELECT COUNT(*) n FROM economic_events').get().n,0);
});

test('partially overlapping pages cannot hide a missing trade behind deduplication',async t=>{
 const {db,store}=harness(t);
 await assert.rejects(syncV2Trades({store,accountId:25923,from,to,pageSize:2,
  post:async body=>({subaccount_id:25923,trades:body.page===1?[trade('A'),trade('B')]:[trade('B'),trade('C')],pagination:{num_pages:2,count:4}})
 }),/repeated/);
 assert.equal(getEconomicHistory(db,25923,from,to).coverage.trades,false);
 assert.equal(db.prepare('SELECT COUNT(*) n FROM economic_events').get().n,0);
});

test('a complete counted history preserves the query provenance',async t=>{
 const {db,store}=harness(t);
 const result=await syncV2Trades({store,accountId:25923,from,to,pageSize:2,
  post:async body=>({subaccount_id:25923,trades:body.page===1?[trade('A'),trade('B')]:[trade('C')],pagination:{num_pages:2,count:3}})
 });
 assert.equal(result.count,3);
 assert.equal(getEconomicHistory(db,25923,from,to).coverage.trades,true);
 const coverage=db.prepare('SELECT evidence_reference FROM economic_coverage').get();
 assert.match(coverage.evidence_reference,/derive-v2\/private\/get_trade_history account=25923/);
 assert.match(coverage.evidence_reference,/unique_trades=3/);
});

test('coverage migration preserves rows and imported evidence references',t=>{
 const db=new Database(':memory:');t.after(()=>db.close());
 db.exec(`CREATE TABLE economic_coverage (
  account_id TEXT NOT NULL,dataset TEXT NOT NULL,from_timestamp TEXT NOT NULL,to_timestamp TEXT NOT NULL,
  complete INTEGER NOT NULL,error TEXT,PRIMARY KEY(account_id,dataset,from_timestamp,to_timestamp)
 );`);
 db.prepare('INSERT INTO economic_coverage VALUES (?,?,?,?,?,?)').run('25923','trades',new Date(from).toISOString(),new Date(to).toISOString(),0,'legacy gap');
 const {importEvidence}=require('../scripts/import-economic-events');
 importEvidence(db,{schema_version:1,events:[],coverage:{account_id:25923,dataset:'transfers',from_timestamp:from,to_timestamp:to,complete:true,evidence_reference:'sha256:exhaustive-venue-export'}});
 const old=db.prepare("SELECT * FROM economic_coverage WHERE dataset='trades'").get();
 assert.equal(old.error,'legacy gap');assert.equal(old.evidence_reference,null);
 const imported=db.prepare("SELECT * FROM economic_coverage WHERE dataset='transfers'").get();
 assert.equal(imported.complete,1);assert.equal(imported.evidence_reference,'sha256:exhaustive-venue-export');
 assert.equal(createEconomicStore(db).recordBatch([],{account_id:25923,dataset:'transfers',from_timestamp:from,to_timestamp:to,complete:true}),0);
 assert.equal(db.prepare("SELECT evidence_reference FROM economic_coverage WHERE dataset='transfers'").get().evidence_reference,'sha256:exhaustive-venue-export');
});

test('durable splits make progress across retries even before any leaf can complete',async t=>{
 const {db}=harness(t);
 const makePost=()=>{
  let requests=0;
  return {count:()=>requests,post:async body=>{
   requests++;
   return body.to_timestamp-body.from_timestamp>1000
    ?{subaccount_id:25923,trades:[{...trade(String(body.from_timestamp)),timestamp:body.from_timestamp}],num_pages:100}
    :{subaccount_id:25923,trades:[],num_pages:0};
  }};
 };
 const first=makePost();
 await assert.rejects(syncV2TradesProgressively({store:createEconomicStore(db),accountId:25923,from:0,to:8000,post:first.post,maxRequests:2}),/budget/);
 assert.equal(first.count(),2);
 assert.equal(createEconomicStore(db).latestCoverage(25923,'trades'),null);
 assert.deepEqual(createEconomicStore(db).getTradeSyncWork(25923).windows[0],[0,2000]);
 const second=makePost();
 await assert.rejects(syncV2TradesProgressively({store:createEconomicStore(db),accountId:25923,from:0,to:8000,post:second.post,maxRequests:2}),/budget/);
 assert.equal(second.count(),2);
 assert.equal(createEconomicStore(db).latestCoverage(25923,'trades'),'1970-01-01T00:00:01.000Z');
 for(let attempt=0;attempt<10&&createEconomicStore(db).getTradeSyncWork(25923);attempt++){
  const next=makePost();
  try{await syncV2TradesProgressively({store:createEconomicStore(db),accountId:25923,from:0,to:8000,post:next.post,maxRequests:2});}catch(error){assert.match(error.message,/budget/);}
  assert.ok(next.count()<=2);
 }
 assert.equal(createEconomicStore(db).getTradeSyncWork(25923),null);
 assert.equal(createEconomicStore(db).latestCoverage(25923,'trades'),'1970-01-01T00:00:08.000Z');
});

test('resumed work skips only proven coverage and appends a later requested endpoint',async t=>{
 const {store}=harness(t);
 store.recordBatch([],{account_id:25923,dataset:'trades',from_timestamp:0,to_timestamp:2000,complete:true});
 store.saveTradeSyncWork(25923,{to:4000,windows:[[2000,3000],[3000,4000]]});
 const requested=[];
 const result=await syncV2TradesProgressively({store,accountId:25923,from:1000,to:5000,
  post:async body=>{requested.push([body.from_timestamp,body.to_timestamp]);return {subaccount_id:25923,trades:[],pagination:{num_pages:0,count:0}};}
 });
 assert.equal(result.complete,true);
 assert.deepEqual(requested,[[2000,3000],[3000,4000],[4000,5000]]);
 assert.equal(store.latestCoverage(25923,'trades'),'1970-01-01T00:00:05.000Z');
 assert.equal(store.getTradeSyncWork(25923),null);
});

test('a saved pending window cannot bypass an uncovered prefix',async t=>{
 const {store}=harness(t);
 store.saveTradeSyncWork(25923,{to:4000,windows:[[2000,4000]]});
 const requested=[];
 await syncV2TradesProgressively({store,accountId:25923,from:0,to:4000,
  post:async body=>{requested.push([body.from_timestamp,body.to_timestamp]);return {subaccount_id:25923,trades:[],num_pages:0};}
 });
 assert.deepEqual(requested,[[0,4000]]);
});

test('a request budget below the default page cap still persists narrower windows',async t=>{
 const {store}=harness(t);
 const post=async body=>body.to_timestamp-body.from_timestamp>1000
  ?{subaccount_id:25923,trades:[{...trade(String(body.from_timestamp)),timestamp:body.from_timestamp}],num_pages:3}
  :{subaccount_id:25923,trades:[],num_pages:0};
 await assert.rejects(syncV2TradesProgressively({store,accountId:25923,from:0,to:8000,post,maxRequests:2}),/budget/);
 assert.deepEqual(store.getTradeSyncWork(25923).windows[0],[0,2000]);
 await assert.rejects(syncV2TradesProgressively({store,accountId:25923,from:0,to:8000,post,maxRequests:2}),/budget/);
 assert.equal(store.latestCoverage(25923,'trades'),'1970-01-01T00:00:01.000Z');
});

test('official V2 trade identity and final settlement fields are required',async t=>{
 const {store}=harness(t);
 assert.throws(()=>normalizeV2Trade({...trade(),is_transfer:undefined},25923),/identity/);
 assert.throws(()=>normalizeV2Trade({...trade(),tx_status:undefined},25923),/unresolved/);
 assert.throws(()=>normalizeV2Trade({...trade(),tx_status:'confirmed'},25923),/unsupported/);
 assert.throws(()=>normalizeV2Trade({...trade(),subaccount_id:undefined},25923),/account/);
 await assert.rejects(syncV2Trades({store,accountId:25923,from,to,post:async()=>({subaccount_id:999,trades:[],num_pages:0})}),/account mismatch/);
 await assert.rejects(syncV2Trades({store,accountId:25923,from,to,post:async()=>({trades:[],num_pages:0})}),/account identity/);
});

test('automatic ledger tracking begins prospectively and retains its boundary across restarts', async t=>{
 const {db,store}=harness(t);
 const start=store.startTracking(25923,from);
 assert.equal(start,new Date(from).toISOString());
 const restarted=createEconomicStore(db);
 assert.equal(restarted.startTracking(25923,to),start);
 assert.equal(restarted.latestCoverage(25923,'trades',start),null);
 const requested=[];
 await syncV2TradesProgressively({store:restarted,accountId:25923,from:start,to,post:async body=>{
   requested.push(body);return {subaccount_id:25923,trades:[trade()],pagination:{num_pages:1,count:1}};
 }});
 assert.equal(requested[0].from_timestamp,Date.parse(start));
 assert.equal(restarted.latestCoverage(25923,'trades',start),new Date(to).toISOString());
 assert.equal(restarted.latestCoverage(25923,'trades'),null,'prospective coverage cannot claim epoch history');
 assert.equal(getEconomicHistory(db,25923,'2026-09-10T00:00:00Z',to).coverage.trades,false);
 assert.equal(getEconomicHistory(db,25923,start,to).coverage.trades,true);
 assert.notEqual(restarted.startTracking(7,to),start,'tracking boundaries are account scoped');
});
