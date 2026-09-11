'use strict';
const {test}=require('node:test');const assert=require('node:assert/strict');
const {observationUniverse,instrumentFromName,missingExpiryDates}=require('../bot/observations');
test('ITM and aging held/observed contracts survive loss of entry eligibility',()=>{
 const held='ETH-20261001-3000-C',observed='ETH-20261101-1600-P',entry='ETH-20261127-1600-P';
 const result=observationUniverse({candidates:[instrumentFromName(entry)],positions:[{instrument_name:held,amount:'-1'}],pendingSymbols:[observed,held]});
 assert.deepEqual(result.map(i=>i.instrument_name),[entry,held,observed]);
 assert.equal(result[1].option_details.strike,3000);
 assert.deepEqual(missingExpiryDates(result,new Set(['20261127'])),['20261001','20261101']);
});
test('observation-only fallback never supplies venue submission identity',()=>{
 const inst=instrumentFromName('ETH-20261127-1600-P');assert.equal(inst.base_asset_address,undefined);
 assert.equal(instrumentFromName('not-an-instrument'),null);
});
