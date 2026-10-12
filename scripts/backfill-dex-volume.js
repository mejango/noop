'use strict';
const Database=require('better-sqlite3');
const path=require('path');
const {createVolumeStore,refreshDexVolume,volumeSummary}=require('../bot/dex-volume');
(async()=>{
  const database=new Database(process.env.NOOP_DB_PATH || path.join(process.env.DATA_DIR || path.join(__dirname,'../data'),'noop.db'));
  database.pragma('busy_timeout=5000');
  database.pragma('journal_mode=WAL');
  try {
    const store=createVolumeStore(database);
    const results=await refreshDexVolume(store,{force:true});
    console.log(JSON.stringify({source:'geckoterminal_ohlcv',results,summary:volumeSummary(store)},null,2));
    if (results.some(r=>r.status!=='ok')) process.exitCode=1;
  } finally {database.close();}
})().catch(error=>{console.error(error.message);process.exitCode=1;});
