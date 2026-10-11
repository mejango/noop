import { buildCorrelationAnalysis as sharedCorrelationAnalysis } from '../../../bot/correlation';
import * as marketHistoryDb from './db';

// ─── Cache ──────────────────────────────────────────────────────────────────

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let _corrCache: { data: any; ts: number } | null = null;
const CORRELATION_TTL = 300_000; // 5 minutes

interface LeadLagResult {
  best_offset: number;
  best_r: number;
  contemporaneous_r: number;
  a_leads: boolean;
  aligned_pairs?: number;
}

// ─── Correlation Snapshot ───────────────────────────────────────────────────

export interface PairCorrelation {
  series_a: string;
  series_b: string;
  r_7d: number | null;
  r_30d: number | null;
  r_90d?: number | null;
  n_7d?: number;
  n_30d?: number;
  n_90d?: number;
  lead_lag: LeadLagResult | null;
}

export interface LeadingIndicator {
  leader: string;
  follower: string;
  offset_hours: number;
  lagged_r: number;
  contemporaneous_r: number;
}

export interface CorrelationSnapshot {
  pairs: PairCorrelation[];
  leading_indicators: LeadingIndicator[];
  series_descriptions: Record<string, string>;
  computed_at: string;
}

export function buildCorrelationAnalysis(): CorrelationSnapshot {
  if (_corrCache && Date.now() - _corrCache.ts < CORRELATION_TTL) return _corrCache.data;
  const result = _buildCorrelationUncached();
  _corrCache = { data: result, ts: Date.now() };
  return result;
}

function _buildCorrelationUncached(): CorrelationSnapshot {
  return sharedCorrelationAnalysis(marketHistoryDb);
}
