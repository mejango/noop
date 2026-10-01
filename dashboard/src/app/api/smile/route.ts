import { NextResponse } from 'next/server';
import { getSmileSnapshotAt, getSmileSnapshotNear } from '@/lib/db';
import { STRATEGY_FACTS } from '@/lib/strategy-config';
import { fromCompact } from '@/lib/vol-smile';
import { getChain } from '@/lib/smile-chain';

export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const chain = await getChain();
    const dayAgo = new Date(Date.now() - 24 * 3_600_000);
    // Prefer the full-chain snapshot; fall back to bot-candidate rows (its trade zones only).
    const full = getSmileSnapshotNear(dayAgo);
    const history = full.length
      ? full.flatMap(r => fromCompact(r, Date.parse(r.timestamp)).points.map(p => ({
        name: p.name, expiry: r.expiry, strike: p.strike, type: p.type, delta: p.delta, iv: p.iv, timestamp: r.timestamp,
      })))
      // Same <2Δ cut as the live chain, so both curves span the same wings.
      : getSmileSnapshotAt(dayAgo).filter(r => Math.abs(r.delta) >= 0.02).map(r => ({
        name: r.instrument_name, expiry: r.expiry, strike: r.strike,
        type: r.option_type?.toUpperCase().startsWith('P') ? 'P' : 'C',
        delta: r.delta, iv: r.implied_vol * 100, timestamp: r.timestamp,
      }));
    return NextResponse.json({
      asOf: new Date(chain.at).toISOString(),
      expiries: chain.expiries,
      history,
      historyScope: full.length ? 'full' : 'zones',
      zones: {
        put: { delta: STRATEGY_FACTS.put_delta_range, dte: STRATEGY_FACTS.put_dte_range },
        call: { delta: STRATEGY_FACTS.call_delta_range, dte: STRATEGY_FACTS.call_dte_range },
      },
    });
  } catch (e: unknown) {
    return NextResponse.json({ error: e instanceof Error ? e.message : 'Unknown error' }, { status: 502 });
  }
}
