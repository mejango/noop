'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const Database = require('better-sqlite3');
const {createEconomicStore,normalizeV2Trade,syncV2Trades,multiply,getEconomicHistory,coversRange} = require('../bot/economic-events');
const trade = (id='one') => ({trade_id:id,subaccount_id:25923,instrument_name:'ETH-20261127-1600-P',direction:'buy',trade_amount:'0.3',trade_price:'0.2',timestamp:Date.parse('2026-09-11T10:00:00Z')});
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
 await assert.rejects(syncV2Trades({store,accountId:25923,from,to,pageSize:1,post:async body=>body.page===1?{trades:[trade()],num_pages:2}:Promise.reject(new Error('timeout'))}),/timeout/);
 assert.equal(db.prepare('select count(*) n from economic_events').get().n,0);
 assert.equal(getEconomicHistory(db,25923,from,to).coverage.trades,false);
});
test('complete paginated history is idempotent and does not imply settlements or transfers coverage',async t=>{
 const {db,store}=harness(t);const post=async body=>({trades:[trade(body.page===1?'one':'two')],num_pages:2});
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
 const {db,store}=harness(t);store.recordExposure('20',from);store.recordExposure('20','2026-09-11T09:30:00Z');store.recordExposure('30',to);
 assert.equal(db.prepare('select count(*) n from exposure_history').get().n,2);
 assert.equal(getEconomicHistory(db,25923,'2026-09-10',to).exposureKnown,false);
 assert.equal(getEconomicHistory(db,25923,from,to).exposureKnown,true);
});
