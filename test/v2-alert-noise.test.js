'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { loadProduction } = require('./helpers/load-production');

const { shouldAlertTradingError, getTelegramResendBody } = loadProduction(['shouldAlertTradingError', 'getTelegramResendBody']);

test('a one-tick unknown order status is not alerted; a repeated one and every other error are', () => {
  const unknown = 'Order 52ee31fe-33ec-4b45-ab6a-fd85c2397408 status unknown; reservations retained';
  assert.equal(shouldAlertTradingError(unknown, null), false);
  assert.equal(shouldAlertTradingError(unknown, unknown), true);
  // A different order going unknown on the next tick is a new one-tick event.
  assert.equal(shouldAlertTradingError(unknown, 'Order other status unknown; reservations retained'), false);
  assert.equal(shouldAlertTradingError('Untracked venue order abc; reconcile before trading', null), true);
});

test('a Telegram send is resent only when it cannot have been delivered', () => {
  const markdown = { text: '*filled*', parse_mode: 'Markdown' };
  assert.deepEqual(getTelegramResendBody({ response: { status: 400 } }, markdown), { text: '*filled*' });
  assert.equal(getTelegramResendBody({ code: 'ECONNRESET' }, markdown), markdown);
  assert.equal(getTelegramResendBody({ message: 'Error' }, markdown), markdown);
  assert.equal(getTelegramResendBody({ code: 'ECONNABORTED' }, markdown), null);
  assert.equal(getTelegramResendBody({ response: { status: 429 } }, markdown), null);
});
