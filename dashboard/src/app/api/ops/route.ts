import { NextResponse } from 'next/server';
import { getActiveTradingRules, getRecentPendingActions, getRecentOrders, getOpsStats, getLatestAdvisoryAssessment } from '@/lib/db';

export const dynamic = 'force-dynamic';

export function GET() {
  try {
    const stats = getOpsStats();
    const rules = getActiveTradingRules();
    const actions = getRecentPendingActions(30);
    const orders = getRecentOrders(20);
    const assessment = getLatestAdvisoryAssessment();
    return NextResponse.json({ stats, rules, actions, orders, assessment });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
