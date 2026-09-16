'use strict';

// A venue receipt and Noop's local accounting are separate commits. Keep the
// execution journal latched until this transaction and the caller complete.
function accountInitialReceipt({ db, botData, action, instrumentName, amount, price, orderType,
  pendingActionId, instrument, spotPrice, order, record, trades }) {
  if (!db?.db?.transaction) throw new Error('V3 execution requires a transactional strategy database');
  let filledAmt = 0;
  let totalValue = 0;
  for (const trade of trades) {
    const quantity = Number(trade.trade_amount);
    const fillPrice = Number(trade.trade_price);
    if (!(quantity > 0) || !(fillPrice > 0) || !Number.isFinite(quantity * fillPrice)) {
      throw new Error('Invalid V3 fill; accounting recovery required');
    }
    filledAmt += quantity;
    totalValue += quantity * fillPrice;
  }
  if (!Number.isFinite(Number(record?.filled_amount))
    || Math.abs(Number(record.filled_amount) - filledAmt) > 1e-9) {
    throw new Error('V3 receipt fill amount does not match trades');
  }
  const resting = record.order_status === 'open';
  if ((!resting && !['filled', 'cancelled', 'expired', 'rejected'].includes(record.order_status))
    || (record.order_status === 'rejected' && filledAmt > 0)
    || (resting && (orderType === 'ioc' || filledAmt >= Number(amount)))) {
    throw new Error('V3 receipt has an unexpected lifecycle state');
  }
  const avgPx = filledAmt > 0 ? totalValue / filledAmt : Number(price);
  const result = resting
    ? { resting: true, orderId: record.order_id, action, instrumentName, amount, price, orderType, filledAmt, avgPx, totalValue }
    : filledAmt > 0 ? { filledAmt, avgPx, totalValue, order, orderType }
      : { zeroFill: true, action, instrumentName, amount, price, orderType };
  const previousBudget = botData.putNetBought;
  try {
    db.db.transaction(() => {
      if (action === 'buy_put') botData.putNetBought += totalValue;
      db.saveBotState(botData);
      if (resting) {
        db.insertRestingOrder({ order_id: record.order_id, pending_action_id: pendingActionId,
          instrument_name: instrumentName, action, direction: record.direction,
          amount, limit_price: price, filled_amount: filledAmt, filled_value: totalValue });
      }
      if (filledAmt > 0 || !resting) {
        db.insertOrder({ action, success: filledAmt > 0, reason: `V3 ${record.order_status} [${orderType}]`,
          instrument_name: instrumentName, pending_action_id: pendingActionId,
          strike: instrument.option_details?.strike || null, expiry: instrument.option_details?.expiry || null,
          delta: null, price, intended_amount: amount, filled_amount: filledAmt,
          fill_price: filledAmt > 0 ? avgPx : null, total_value: totalValue, spot_price: spotPrice, raw_response: order });
      }
      if (pendingActionId != null) db.updatePendingAction(pendingActionId, {
        status: resting ? 'resting' : filledAmt > 0 ? 'executed' : 'failed',
        executed_at: new Date().toISOString(), execution_result: JSON.stringify(result),
      });
    })();
  } catch (error) {
    botData.putNetBought = previousBudget;
    throw error;
  }
  return result;
}

module.exports = { accountInitialReceipt };
