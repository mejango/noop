import { NextResponse } from 'next/server';
import { getWikiDiagnostics } from '@/lib/wiki';

export const dynamic = 'force-dynamic';

export function GET() {
  try {
    return NextResponse.json(getWikiDiagnostics());
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
