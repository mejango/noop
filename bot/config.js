const fs = require('fs');
const path = require('path');

// Data directory (Railway volume mount or local)
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', 'data');
const ARCHIVE_DIR = path.join(DATA_DIR, 'archive');
const DB_PATH = path.join(DATA_DIR, 'noop.db');

// Lyra API endpoints
const API_URL = {
  GET_TICKER: 'https://api.lyra.finance/public/get_ticker',
  GET_INSTRUMENTS: 'https://api.lyra.finance/public/get_instruments',
  PLACE_ORDER: 'https://api.lyra.finance/private/order',
};

// CoinGecko API for spot price
const COINGECKO_API = 'https://api.coingecko.com/api/v3';

// DEX APIs - API key from env or fallback
const THEGRAPH_API_KEY = process.env.THEGRAPH_API_KEY || '9bc783800c9a60b574487c0ee711609a';
const DEX_APIS = {
  UNISWAP_V3: `https://gateway.thegraph.com/api/${THEGRAPH_API_KEY}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV`,
  UNISWAP_V4: `https://gateway.thegraph.com/api/${THEGRAPH_API_KEY}/subgraphs/id/DiYPVdygkfjDWhbxGSqAQxwBKmfKnkWQojqeM2rkLb3G`
};

// Etherscan API
const ETHERSCAN_API = 'https://api.etherscan.io/v2/api';
const ETHERSCAN_API_KEY = process.env.ETHERSCAN_API_KEY || 'YT53NPT32Z7ZGYRHA7X7GGVNZWZJIY1VW4';

// Derive account constants
const DERIVE_ACCOUNT_ADDRESS = '0xD87890df93bf74173b51077e5c6cD12121d87903';
const ACTION_TYPEHASH = '0x4d7a9f27c403ff9c0f19bce61d76d82f9aa29f8d6d4b0c5474607d9770d1af17';
const TRADE_MODULE_ADDRESS = '0xB8D20c2B7a1Ad2EE33Bc50eF10876eD3035b5e7b';
const DOMAIN_SEPARATOR = '0xd96e5f90797da7ec8dc4e276260c7f3f87fedf68775fbe1ef116e996fc60441b';
const SUBACCOUNT_ID = 25923;

// Trading budgets
const PUT_BUYING_BASE_FUNDING_LIMIT = 0;
const CALL_SELLING_BASE_FUNDING_LIMIT = 0;

// Trading parameters - PUTS
const PUT_EXPIRATION_RANGE = [50, 90];
const PUT_DELTA_RANGE = [-0.12, -0.02];

// Trading parameters - CALLS (updated: tighter DTE, wider strike)
const CALL_EXPIRATION_RANGE = [5, 9];
const CALL_DELTA_RANGE = [0.06, 0.2];
const CALL_STRIKE_MULTIPLIER = 1.10; // was 1.14
const CALL_PREMIUM_ELEVATION_THRESHOLD = 1.20; // only sell when premiums > 120% of 7d avg

// Call buyback thresholds
const CALL_BUYBACK_PROFIT_THRESHOLD = 80;

// ETH contract addresses
const ETH_CONTRACTS = {
  WETH: '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
  ETH: '0x0000000000000000000000000000000000000000',
  USDC: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
  USDT: '0xdAC17F958D2ee523a2206206994597C13D831ec7',
  WBTC: '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
};

// Time constants
const TIME_CONSTANTS = {
  WEEK: 7 * 24 * 60 * 60 * 1000,
  THREE_DAYS: 3 * 24 * 60 * 60 * 1000,
  DAY: 24 * 60 * 60 * 1000,
  HOUR: 60 * 60 * 1000,
  MINUTE: 60 * 1000
};

// Dynamic check intervals
const DYNAMIC_INTERVALS = {
  'urgent': 45 * 1000,
  'normal': 5 * 60 * 1000
};

// Cycle period (10 days)
const PERIOD = 10 * 1000 * 60 * 60 * 24;

// Load private key from env or file
const loadPrivateKey = () => {
  if (process.env.PRIVATE_KEY) {
    return process.env.PRIVATE_KEY.trim();
  }
  try {
    const keyPath = path.join(__dirname, '..', '.private_key.txt');
    const privateKey = fs.readFileSync(keyPath, 'utf8').trim();
    if (!privateKey) throw new Error('Private key is empty.');
    return privateKey;
  } catch (error) {
    console.error('Error loading private key:', error.message);
    process.exit(1);
  }
};

module.exports = {
  DATA_DIR,
  ARCHIVE_DIR,
  DB_PATH,
  API_URL,
  COINGECKO_API,
  DEX_APIS,
  ETHERSCAN_API,
  ETHERSCAN_API_KEY,
  THEGRAPH_API_KEY,
  DERIVE_ACCOUNT_ADDRESS,
  ACTION_TYPEHASH,
  TRADE_MODULE_ADDRESS,
  DOMAIN_SEPARATOR,
  SUBACCOUNT_ID,
  PUT_BUYING_BASE_FUNDING_LIMIT,
  CALL_SELLING_BASE_FUNDING_LIMIT,
  PUT_EXPIRATION_RANGE,
  PUT_DELTA_RANGE,
  CALL_EXPIRATION_RANGE,
  CALL_DELTA_RANGE,
  CALL_STRIKE_MULTIPLIER,
  CALL_PREMIUM_ELEVATION_THRESHOLD,
  CALL_BUYBACK_PROFIT_THRESHOLD,
  ETH_CONTRACTS,
  TIME_CONSTANTS,
  DYNAMIC_INTERVALS,
  PERIOD,
  loadPrivateKey,
};
