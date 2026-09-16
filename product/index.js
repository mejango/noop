'use strict';

// Importing OOF never opens a database or constructs a venue client.
const { openLedger } = require('./ledger/store');
const { normalizeEvent } = require('./ledger/events');
const { createOperations, createGateway } = require('./ledger/operations');

module.exports = { openLedger, normalizeEvent, createOperations, createGateway };
