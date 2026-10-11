'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const Database = require('better-sqlite3');
const { HISTORY_SQL, completedCloses, buildSmileHistory, smileChangeSeries, summarizeSmile, summarizeLiquidity } = require('../bot/market-evidence');
const { alignHourly, analyzeAligned } = require('../bot/correlation');
const H = 3600000, NOW = Date.parse('2026-10-11T12:30:00Z');
const iso = ms => new Date(ms).toISOString();
const expiry = days => (NOW + days*24*H)/1000;
const points = iv => JSON.stringify([[1,1,.45,iv],[2,1,.25,iv+.02],[3,1,.1,iv+.1],[4,0,-.45,iv+.01],[5,0,-.25,iv+.05],[6,0,-.1,iv+.15]]);
const smile = (at, ex=expiry(7), iv=.4) => ({ timestamp:iso(at),expiry:ex,points:points(iv) });
const dex = (at, usd=100, amount=10, id='pool', definition='pool_token_balances_v1') => ({ source:'ethereum_rpc_pool_balances', sourceTimestamp:iso(at), tvlDefinition:definition,totalLiquidity:usd,
  poolDetails:[{ id,feeTier:500,tvlToken0:1000,tvlToken1:amount,token0:{id:'usdc',symbol:'USDC'},token1:{id:'weth',symbol:'WETH'} }] });
const liquidity = (at, value) => ({ timestamp:iso(at),raw_data:JSON.stringify({dexLiquidity:{dexes:{uniswap_v3:value}}}) });

test('RV uses exactly the requested completed hours, excludes current-hour closes, and refuses gaps', () => {
  const start=Math.floor(NOW/H)*H-721*H;
  const rows=Array.from({length:722},(_,i)=>({hour:iso(start+i*H),close:100+i}));
  assert.equal(completedCloses(rows,NOW,720).length,721);
  assert.equal(completedCloses(rows,NOW,720).at(-1),820);
  assert.deepEqual(completedCloses(rows.filter((_,i)=>i!==80),NOW,720),[]);
  assert.deepEqual(completedCloses(rows.slice(-20),NOW,168),[]);
});

test('full smile history includes both risk reversals and put VRP without inventing insufficient RV', () => {
  const rows=[smile(NOW-H),smile(NOW-H,expiry(60),.5),smile(NOW+H)];
  const history=buildSmileHistory(rows,[],NOW);
  assert.equal(history.length,1);
  assert.ok(Math.abs(history[0].call_rr25+3)<1e-9);
  assert.ok(Math.abs(history[0].put_rr25+3)<1e-9);
  assert.ok(Math.abs(history[0].term_slope-11)<1e-9);
  assert.equal(history[0].call_vrp,null);
  assert.equal(history[0].put_vrp,null);
});

test('smile hourly changes exclude expiry switches and missing hours, preserving valid changes', () => {
  const h=buildSmileHistory([smile(NOW-4*H),smile(NOW-3*H,expiry(7),.41),smile(NOW-2*H,expiry(8),.6),smile(NOW,expiry(8),.61)],[],NOW);
  const changes=smileChangeSeries(h).call_atm_change.map(r=>r.value);
  assert.equal(changes[0],null);
  assert.ok(Math.abs(changes[1]-1)<1e-9);
  assert.equal(changes[2],null);
  assert.equal(changes[3],null);
});

test('long smile horizons show available coverage and refuse rolled-expiry baselines', () => {
  const history=buildSmileHistory([smile(NOW-24*H,expiry(8)),smile(NOW,expiry(7),.6)],[],NOW);
  const summary=summarizeSmile(history,NOW);
  assert.equal(summary.features.call_atm.changes_vol_points['24h'],null);
  assert.equal(summary.features.call_atm.windows['90d'].expected_hours,2160);
  assert.equal(summary.features.call_atm.windows['90d'].observed_hours,1);
  assert.equal(summary.features.call_atm.windows['30d'].percentile,null);
  assert.equal(summarizeSmile(history,NOW+2*H).status,'unavailable');
});

test('price-only TVL changes retain zero reserve changes and cannot imply net withdrawal', () => {
  const summary=summarizeLiquidity([liquidity(NOW-H,dex(NOW-H,100)),liquidity(NOW,dex(NOW,90))],NOW);
  const current=summary.dexes[0];
  assert.equal(current.changes['1h'].usd_pct,-10);
  assert.equal(current.changes['1h'].pools[0].token1_delta,0);
  assert.equal(current.changes['30d'].usd_pct,null);
  assert.match(summary.interpretation,/not net deposit/);
});

test('legacy, changed-pool and replayed-source baselines are excluded from long liquidity trends', () => {
  for (const old of [dex(NOW-24*H,100,10,'pool','subgraph_token_balances'),dex(NOW-24*H,100,10,'different'),dex(NOW-48*H)]) {
    const summary=summarizeLiquidity([liquidity(NOW-24*H,old),liquidity(NOW,dex(NOW))],NOW);
    assert.equal(summary.dexes[0].changes['24h'].usd_pct,null);
  }
  assert.equal(summarizeLiquidity([liquidity(NOW,dex(NOW-H))],NOW).dexes[0].status,'unavailable');
});

test('valid 30d and 90d liquidity baselines report changes without claiming continuous history', () => {
  const rows=[liquidity(NOW-90*24*H,dex(NOW-90*24*H,50)),liquidity(NOW-30*24*H,dex(NOW-30*24*H,80)),liquidity(NOW,dex(NOW,100))];
  const current=summarizeLiquidity(rows,NOW).dexes[0];
  assert.equal(current.changes['30d'].usd_pct,25);
  assert.equal(current.changes['90d'].usd_pct,100);
  assert.ok(current.coverage['90d'].observed_hours<3);
});

test('hour alignment keeps calendar gaps and omits incomplete hours, preserving elapsed lag', () => {
  const at=Math.floor(NOW/H)*H;
  const aligned=alignHourly({a:[{hour:iso(at-4*H),value:1},{hour:iso(at-2*H),value:2},{hour:iso(at),value:99}]},at-4*H,NOW);
  assert.deepEqual(aligned.series.a,[1,null,2,null]);
  assert.equal(aligned.hours.length,4);
});

test('correlation reports actual pair counts and detects unstable split halves', () => {
  const a=Array.from({length:720},(_,i)=>Math.sin(i));
  const b=a.map((x,i)=>i<360?x:-x);
  const result=analyzeAligned({hours:Array.from({length:720},(_,i)=>iso(NOW-(720-i)*H)),series:{a,b}}, {},NOW);
  const pair=result.pairs[0];
  assert.equal(pair.n_7d,168);
  assert.equal(pair.n_30d,720);
  assert.equal(pair.stability_30d.status,'unstable_across_halves');
  assert.match(result.notice,/not out-of-sample/);
  assert.equal(result.coverage.a['90d'].expected_hours,2160);
});

test('hourly history SQL selects one coherent smile snapshot, respects as-of bounds and finds narrow baselines', () => {
  const db=new Database(':memory:');
  try {
    db.exec('CREATE TABLE iv_smile_snapshots(timestamp TEXT,expiry INTEGER,points TEXT); CREATE TABLE onchain_data(timestamp TEXT,raw_data TEXT);');
    const insert=db.prepare('INSERT INTO iv_smile_snapshots VALUES (?,?,?)');
    for (const row of [smile(NOW-20*60000),smile(NOW-10*60000),smile(NOW-10*60000,expiry(60)),smile(NOW+H)]) insert.run(row.timestamp,row.expiry,row.points);
    const bounds={since:iso(NOW-H),before:iso(NOW)};
    const rows=db.prepare(HISTORY_SQL.smile).all(bounds);
    assert.equal(rows.length,2);
    assert.equal(new Set(rows.map(r=>r.timestamp)).size,1);
    db.prepare('INSERT INTO onchain_data VALUES (?,?)').run(iso(NOW-H),liquidity(NOW-H,dex(NOW-H)).raw_data);
    assert.equal(db.prepare(HISTORY_SQL.near).get({since:iso(NOW-H-15*60000),before:iso(NOW-H+15*60000),target:iso(NOW-H)}).timestamp,iso(NOW-H));
  } finally { db.close(); }
});

test('advisory publication archives the exact shared historical evidence seen by the council', () => {
  const { loadProduction, SCRIPT_SOURCE } = require('./helpers/load-production');
  let entries;
  const evidence={schema:'market_history_v1',computed_at:iso(NOW),liquidity:{notice:'unknown history'},smile:{features:{}}};
  const { publishTradingAdvisoryDraft }=loadProduction(['publishTradingAdvisoryDraft'],{bindings:{
    db:{publishAdvisory:(_id,_rules,journal)=>{entries=journal;}},botData:{},persistCycleState(){},
    selectAssessmentText:(a,b)=>a||b,console:{log(){}} }});
  publishTradingAdvisoryDraft({advisoryId:'evidence-test',allRules:[],finalAgenda:{assessment:'fixture'},primaryAgenda:{},
    persistedAgenda:{entry_rules:[],exit_rules:[]},marketEvidence:evidence,spotPrice:2000},
    {inputSnapshot:{marketTimestamp:iso(NOW)},checkedSnapshot:{marketTimestamp:iso(NOW)},attempt:1});
  assert.deepEqual(JSON.parse(entries.find(e=>e.entry_type==='advisory_context').content).market_history,evidence);
  assert.match(SCRIPT_SOURCE,/market_history: buildStoredMarketEvidence\(\)/,'journal reads the same evidence builder');
  assert.match(SCRIPT_SOURCE,/formatMarketEvidence\(buildStoredMarketEvidence\(\)\)/,'wiki reads the same evidence builder');
});

test('a short history cannot masquerade as a 30d or 90d correlation window', () => {
  const a=Array.from({length:336},(_,i)=>Math.sin(i));
  const b=a.map(x=>2*x);
  const result=analyzeAligned({hours:Array.from({length:336},(_,i)=>iso(NOW-(336-i)*H)),series:{a,b}}, {},NOW);
  assert.ok(result.pairs[0].r_7d>0.99);
  assert.equal(result.pairs[0].r_30d,null);
  assert.equal(result.pairs[0].r_90d,null);
  assert.equal(result.pairs[0].lead_lag.lookback_days,7);
  assert.equal(result.pairs[0].n_90d,336);
});

test('correlations read completed hourly rollups without needing raw option tables', () => {
  const { readCorrelationHistory }=require('../bot/correlation');
  const { FUNDING_EXCHANGE,FUNDING_SYMBOL }=require('../bot/funding-rates');
  const db=new Database(':memory:');
  try {
    db.exec(`CREATE TABLE spot_prices_hourly(hour TEXT,avg_price REAL);
      CREATE TABLE options_hourly(hour TEXT,best_put_dv REAL,best_call_dv REAL,avg_spread REAL,avg_depth REAL,avg_iv REAL,total_oi REAL);
      CREATE TABLE onchain_hourly(hour TEXT,dex TEXT,avg_magnitude REAL,direction TEXT);
      CREATE TABLE funding_rates_hourly(hour TEXT,exchange TEXT,symbol TEXT,avg_rate REAL);`);
    const end=Math.floor(NOW/H)*H;
    for (const [hour,price] of [[end-3*H,100],[end-2*H,110],[end,999]]) db.prepare('INSERT INTO spot_prices_hourly VALUES (?,?)').run(iso(hour),price);
    db.prepare('INSERT INTO options_hourly VALUES (?,?,?,?,?,?,?)').run(iso(end-2*H),55,66,.01,3,.4,1000);
    db.prepare('INSERT INTO onchain_hourly VALUES (?,?,?,?)').run(iso(end-2*H),'uniswap_v3_onchain',.05,'outflow');
    db.prepare('INSERT INTO onchain_hourly VALUES (?,?,?,?)').run(iso(end-2*H),'uniswap_v3',.9,'inflow');
    db.prepare('INSERT INTO funding_rates_hourly VALUES (?,?,?,?)').run(iso(end-2*H),FUNDING_EXCHANGE,FUNDING_SYMBOL,-.0001);
    const raw=readCorrelationHistory(db,{since:iso(end-4*H),before:iso(end)});
    assert.equal(raw.spot_return.length,1,'current hour is excluded');
    assert.ok(Math.abs(raw.spot_return[0].value-10)<1e-9);
    assert.equal(raw.best_put_dv[0].value,55);
    assert.equal(raw.open_interest[0].value,1000);
    assert.equal(raw.liquidity_flow[0].value,-.05,'legacy v3 rollups are excluded');
    assert.equal(raw.funding_rate[0].value,-.01);
  } finally { db.close(); }
});
