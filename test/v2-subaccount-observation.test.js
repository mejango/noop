'use strict';
const {test}=require('node:test');
const assert=require('node:assert/strict');
const {loadProduction}=require('./helpers/load-production');
function harness(response){
 let calls=0;
 const {fetchSubaccount}=loadProduction(['fetchSubaccount'],{bindings:{
  axios:{post:async()=>{calls++;return {data:response};}},
  API_URL:{GET_SUBACCOUNT:'mock://subaccount'},SUBACCOUNT_ID:25923,DERIVE_ACCOUNT_ADDRESS:'mock-wallet',
  createWallet:()=>({}),signMessage:async()=>'mock-signature',console:{log:()=>{}},
 }});
 return {fetchSubaccount,calls:()=>calls};
}
test('portfolio observation preserves one raw account response and stamps its receipt',async()=>{
 const row={subaccount_id:25923,subaccount_value:'0',positions:[],collaterals:[]};
 const h=harness({result:row});const before=Date.now();
 const result=await h.fetchSubaccount({forObservation:true});
 assert.equal(h.calls(),1);assert.equal(result.subaccount_value,'0');
 assert.equal(result.positions,row.positions);assert.equal(result.collaterals,row.collaterals);
 assert.ok(Date.parse(result.snapshot_received_at)>=before);
 assert.ok(Date.parse(result.snapshot_received_at)<=Date.now());
});
test('nonpositive account equity remains observable while the default margin gate stays closed',async()=>{
 const h=harness({result:{subaccount_id:25923,subaccount_value:'-50',initial_margin:'-10',maintenance_margin:'-1',is_under_liquidation:true,positions:[],collaterals:[]}});
 assert.equal((await h.fetchSubaccount({forObservation:true})).subaccount_value,'-50');
 assert.equal(await h.fetchSubaccount(),null);
});
test('account mismatch and API errors never yield portfolio evidence',async()=>{
 assert.equal(await harness({result:{subaccount_id:999,subaccount_value:'100',positions:[],collaterals:[]}}).fetchSubaccount({forObservation:true}),null);
 assert.equal(await harness({error:{message:'invalid signature'},result:{subaccount_id:25923}}).fetchSubaccount({forObservation:true}),null);
});
