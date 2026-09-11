import fs from 'fs';
import path from 'path';

export type StrategyBudgetConfig = {
  PUT_ANNUAL_RATE: number;
  PERIOD_DAYS: number;
  CALL_EXPOSURE_CAP_PCT: number;
  CALL_EXPOSURE_BUFFER_PCT: number;
  CALL_BREAKOUT_OVERRIDE_CAP_PCT: number;
};

export type StrategyFacts = {
  put_delta_range: [number, number];
  put_dte_range: [number, number];
  call_delta_range: [number, number];
  call_dte_range: [number, number];
  sell_call_fallback_min_bid: number;
  sell_call_fallback_min_score: number;
  put_roll_dte_threshold: number;
  call_buyback_profit_threshold_pct: number;
  put_monetization_profit_threshold_pct: number;
  put_monetization_max_tranche_fraction: number;
};

export const CONFIG_PATH = path.resolve(process.env.BOT_CONFIG_PATH || path.join(process.cwd(), '..', 'bot', 'config.json'));
export const BOT_CONFIG: StrategyBudgetConfig = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
export const STRATEGY_FACTS: StrategyFacts = JSON.parse(fs.readFileSync(path.join(path.dirname(CONFIG_PATH), 'strategy-facts.json'), 'utf8'));

export function describeStrategy(config = BOT_CONFIG, facts = STRATEGY_FACTS): string {
  return `Current configured strategy (these parameters govern the bot):
- Put buying: delta ${facts.put_delta_range.join(' to ')}, ${facts.put_dte_range.join('–')} DTE.
- Insurance spending allowance: ${(config.PUT_ANNUAL_RATE * 100).toFixed(2)}% annualized, allocated in ${config.PERIOD_DAYS}-day cycles. Unspent capacity and current hedge holdings are separate quantities.
- Put roll consideration: ${facts.put_roll_dte_threshold} DTE or less, subject to the live replacement-protection and execution checks. No calendar-only roll recommendation.
- Put monetization: profit threshold ${facts.put_monetization_profit_threshold_pct}%, maximum tranche ${(facts.put_monetization_max_tranche_fraction * 100).toFixed(0)}%, subject to remaining protection and live checks.
- Call selling: delta ${facts.call_delta_range.join(' to ')}, ${facts.call_dte_range.join('–')} DTE; fallback watcher bid at least $${facts.sell_call_fallback_min_bid}, normalized CALL EDGE at least ${facts.sell_call_fallback_min_score}.
- Short-call buyback threshold: ${facts.call_buyback_profit_threshold_pct}% premium capture, subject to execution checks.
- Calls are margin-sized: normal exposure cap ${(config.CALL_EXPOSURE_CAP_PCT * 100).toFixed(0)}%, buffer ${(config.CALL_EXPOSURE_BUFFER_PCT * 100).toFixed(0)} percentage points, explicit breakout override cap ${(config.CALL_BREAKOUT_OVERRIDE_CAP_PCT * 100).toFixed(0)}%.
Active rules can impose stricter entry conditions. These are configuration facts, not instructions to execute a trade. Spending the put budget does not mean protection is absent: inspect held puts and their coverage. Gross options cashflow is not realized profit; unknown fees, unavailable account data and incomplete capital-flow history must remain unknown.`;
}
