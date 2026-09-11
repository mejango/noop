'use strict';

// Import-safe recovery. Venue evidence is read-only; this module cannot submit or cancel orders.
const { accountInitialReceipt } = require('./order-accounting');

function reconcileSubmission({ db, submissionId, accountId, accountAddress, order, trades }) {
  return db.db.transaction(() => {
    const submission = db.db.prepare('SELECT * FROM execution_submissions WHERE id=?').get(submissionId);
    if (!submission || !['unknown', 'acknowledged'].includes(submission.status)) {
      throw new Error('Submission missing or already accounted; nothing to replay');
    }
    const request = JSON.parse(submission.request_json);
    if (String(request.subaccount_id) !== String(accountId)
      || !request.account_address || request.account_address.toLowerCase() !== String(accountAddress).toLowerCase()) {
      throw new Error('Recovery account identity does not match the signed request');
    }
    if (!order?.order_id || String(order.subaccount_id) !== String(accountId)
      || String(order.nonce) !== String(request.nonce)
      || order.instrument_name !== request.instrument_name || order.direction !== request.direction
      || Number(order.amount) !== Number(request.amount)
      || Number(order.limit_price) !== Number(request.limit_price)) {
      throw new Error('Recovery order identity/nonce/terms conflict with the signed request');
    }
    // When an ACK was saved, a lookup must recover that same order, not a lookalike.
    if (submission.response_json) {
      const response = JSON.parse(submission.response_json);
      const acknowledged = response?.result?.order || response?.result || response?.order || response;
      if (acknowledged?.order_id && acknowledged.order_id !== order.order_id) {
        throw new Error('Recovery order ID conflicts with the saved acknowledgement');
      }
    }
    if (!Array.isArray(trades)) throw new Error('Complete trade evidence is required');
    const ids = new Set();
    let value = 0;
    for (const trade of trades) {
      if (!trade?.trade_id || ids.has(String(trade.trade_id)) || trade.order_id !== order.order_id
        || String(trade.subaccount_id) !== String(accountId)
        || trade.instrument_name !== request.instrument_name || trade.direction !== request.direction) {
        throw new Error('Recovery trade identity is missing, duplicated, or conflicting');
      }
      ids.add(String(trade.trade_id));
      value += Number(trade.trade_amount) * Number(trade.trade_price);
    }
    if (Number(order.filled_amount) > 0 && (!Number.isFinite(Number(order.average_price))
      || Math.abs(value - Number(order.filled_amount) * Number(order.average_price)) > 1e-6)) {
      throw new Error('Recovery trades conflict with cumulative order value');
    }
    const action = request.action;
    if (!['buy_put', 'sell_call', 'buyback_call', 'sell_put'].includes(action)
      || (action.startsWith('buy') ? 'buy' : 'sell') !== request.direction) {
      throw new Error('Original strategy action unavailable; refusing inferred accounting');
    }
    const state = db.loadBotState();
    if (!state || !Number.isFinite(Number(state.put_net_bought))) throw new Error('Existing cycle accounting is required');
    // Preserve every unrelated state field; recovery only changes the put spend.
    const recoveryDb = { ...db, saveBotState(botData) {
      db.db.prepare('UPDATE bot_state SET put_net_bought=? WHERE id=1').run(botData.putNetBought);
    } };
    return accountInitialReceipt({ db: recoveryDb, botData: { putNetBought: Number(state.put_net_bought) },
      action, instrumentName: request.instrument_name, amount: Number(request.amount),
      price: Number(request.limit_price), orderType: request.time_in_force,
      pendingActionId: submission.pending_action_id, instrument: {}, spotPrice: null,
      order: { result: { order, trades }, recovery: true }, record: order, trades, submissionId,
      exitIntent: request.exit_intent || null, approvedLimitPrice: request.approved_limit_price || request.limit_price });
  }).immediate();
}

async function collectRecoveryEvidence({ read, request }) {
  const account = { subaccount_id: request.subaccount_id };
  let order = null;
  const seenPages = new Set();
  for (let page = 1; page <= 100; page++) {
    const result = await read('get_order_history', { ...account, from_timestamp: 0, page, page_size: 100 });
    const rows = Array.isArray(result) ? result : result?.orders;
    if (!Array.isArray(rows)) throw new Error('Order history unavailable');
    const matches = rows.filter(row => String(row.nonce) === String(request.nonce));
    if (matches.length > 1) throw new Error('Ambiguous order nonce in venue history');
    if (matches.length) { order = matches[0]; break; }
    if (rows.length < 100) break;
    const key = rows.map(row => row.order_id).join(',');
    if (seenPages.has(key)) throw new Error('Order history pagination did not advance');
    seenPages.add(key);
  }
  if (!order) throw new Error('No order evidence for the nonce; absence cannot release the submission');
  // Refresh the cumulative fill watermark after finding its durable identity.
  let latest;
  try { latest = await read('get_order', { ...account, order_id: order.order_id }); }
  catch (error) {
    // A complete terminal history row remains affirmative evidence when the direct
    // endpoint has pruned the order. An open historical row is never sufficient.
    if (!['filled', 'cancelled', 'expired', 'rejected'].includes(order.order_status)) throw error;
  }
  if (latest !== undefined) {
    const current = latest?.order || latest;
    if (!current?.order_id || current.order_id !== order.order_id) throw new Error('Current order evidence unavailable or conflicting');
    order = current;
  }

  const trades = [];
  const tradePages = new Set();
  let complete = false;
  for (let page = 1; page <= 100; page++) {
    const result = await read('get_trade_history', { ...account, instrument_name: request.instrument_name,
      from_timestamp: 0, page, page_size: 100 });
    const rows = Array.isArray(result) ? result : result?.trades;
    if (!Array.isArray(rows)) throw new Error('Trade history unavailable');
    trades.push(...rows.filter(trade => trade.order_id === order.order_id));
    if (rows.length < 100) { complete = true; break; }
    const key = rows.map(row => row.trade_id).join(',');
    if (tradePages.has(key)) throw new Error('Trade history pagination did not advance');
    tradePages.add(key);
  }
  if (!complete) throw new Error('Trade history scan incomplete');
  // A fill racing these reads is safe: mismatched cumulative amounts fail recovery.
  return { order, trades };
}

async function main(argv = process.argv.slice(2)) {
  const arg = (name) => { const at = argv.indexOf(name); return at < 0 ? null : argv[at + 1]; };
  const submissionId = Number(arg('--submission'));
  if (!argv.includes('--bot-stopped') || !Number.isSafeInteger(submissionId) || submissionId <= 0) {
    throw new Error('Stop the bot, then run: node bot/reconcile-execution.js --submission ID --bot-stopped');
  }
  const db = require('./db');
  try {
    const submission = db.db.prepare('SELECT * FROM execution_submissions WHERE id=?').get(submissionId);
    if (!submission) throw new Error('Unknown submission');
    const request = JSON.parse(submission.request_json);
    if (!request.account_address || !request.subaccount_id) throw new Error('Stored account identity is incomplete');
    const fs = require('fs');
    const { Wallet } = require('ethers');
    const wallet = new Wallet((process.env.PRIVATE_KEY || fs.readFileSync('./.private_key.txt', 'utf8')).trim());
    const axios = require('axios');
    const read = async (method, params) => {
      if (!['get_order', 'get_order_history', 'get_trade_history'].includes(method)) throw new Error('Recovery supports read-only endpoints');
      const timestamp = Date.now();
      const response = await axios.post(`https://api.lyra.finance/private/${method}`, params, {
        headers: { 'X-LyraWallet': request.account_address, 'X-LyraTimestamp': String(timestamp),
          'X-LyraSignature': await wallet.signMessage(String(timestamp)) }, timeout: 15000 });
      if (response.data?.error || !response.data?.result) throw new Error(`Venue ${method} evidence unavailable`);
      return response.data.result;
    };
    const evidence = await collectRecoveryEvidence({ read, request });
    const result = reconcileSubmission({ db, submissionId, accountId: request.subaccount_id,
      accountAddress: request.account_address, ...evidence });
    console.log(JSON.stringify({ submissionId, ...result }, null, 2));
  } finally { db.close(); }
}

if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode = 1; });
module.exports = { reconcileSubmission, collectRecoveryEvidence, main };
