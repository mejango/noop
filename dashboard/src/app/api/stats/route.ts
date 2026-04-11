import { NextResponse } from 'next/server';
import { getStats, getBotBudget, getLyraSpot } from '@/lib/db';
import { getSubaccount } from '@/lib/lyra';

export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const stats = getStats() as Record<string, unknown> || {};
    const budget = getBotBudget();
    const lyra = getLyraSpot();
    let margin_usage_pct: number | null = null;
    try {
      const subaccount = await getSubaccount();
      margin_usage_pct = subaccount.margin_usage_pct;
    } catch { /* leave null */ }
    return NextResponse.json({ ...stats, budget, lyra_spot: lyra?.lyra_spot ?? null, margin_usage_pct });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
