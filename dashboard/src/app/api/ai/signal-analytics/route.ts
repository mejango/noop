import { NextResponse } from 'next/server';
import { buildSignalAnalytics } from '@/lib/signal-analytics';

export const dynamic = 'force-dynamic';

export function GET() {
  try {
    return NextResponse.json(buildSignalAnalytics());
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({
      error: message,
      meta: {
        computedAt: new Date().toISOString(),
        windowDays: 90,
        minSamples: 10,
        hours: 0,
        note: 'Dashboard-only statistical priors. These are not injected into advisor prompts until sample counts mature.',
      },
      priors: [],
    }, { status: 500 });
  }
}
