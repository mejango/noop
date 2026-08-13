import { NextResponse } from 'next/server';

import {
  ADMIN_SESSION_COOKIE,
  ADMIN_SESSION_TTL_SECONDS,
  createAdminSessionToken,
  hasValidAdminSession,
  isWriteTokenConfigured,
  validateAdminPassphrase,
} from '@/lib/write-access';

export const dynamic = 'force-dynamic';

const ATTEMPT_WINDOW_MS = 15 * 60 * 1000;
const MAX_FAILED_ATTEMPTS = 5;
const failedAttempts = new Map<string, number[]>();

function requestIp(request: Request): string {
  return request.headers.get('x-forwarded-for')?.split(',')[0]?.trim()
    || request.headers.get('x-real-ip')?.trim()
    || 'unknown';
}

function recentFailures(ip: string, now = Date.now()): number[] {
  const failures = (failedAttempts.get(ip) || []).filter((timestamp) => now - timestamp < ATTEMPT_WINDOW_MS);
  if (failures.length > 0) failedAttempts.set(ip, failures);
  else failedAttempts.delete(ip);
  return failures;
}

export function GET(request: Request) {
  return NextResponse.json({
    configured: isWriteTokenConfigured(),
    authenticated: hasValidAdminSession(request),
  }, { headers: { 'Cache-Control': 'no-store' } });
}

export async function POST(request: Request) {
  if (!isWriteTokenConfigured()) {
    return NextResponse.json({ error: 'NOOP_WRITE_TOKEN is not configured' }, { status: 503 });
  }

  const ip = requestIp(request);
  const now = Date.now();
  const failures = recentFailures(ip, now);
  if (failures.length >= MAX_FAILED_ATTEMPTS) {
    return NextResponse.json(
      { error: 'Too many failed attempts. Try again in 15 minutes.' },
      { status: 429, headers: { 'Cache-Control': 'no-store', 'Retry-After': '900' } },
    );
  }

  const body = await request.json().catch(() => ({})) as Record<string, unknown>;
  const passphrase = typeof body.passphrase === 'string' ? body.passphrase : '';
  if (!validateAdminPassphrase(passphrase)) {
    failedAttempts.set(ip, [...failures, now]);
    return NextResponse.json({ error: 'Admin passphrase is incorrect' }, { status: 401 });
  }

  failedAttempts.delete(ip);
  const response = NextResponse.json({ authenticated: true }, { headers: { 'Cache-Control': 'no-store' } });
  response.cookies.set(ADMIN_SESSION_COOKIE, createAdminSessionToken(now), {
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'strict',
    path: '/',
    maxAge: ADMIN_SESSION_TTL_SECONDS,
  });
  return response;
}

export function DELETE() {
  const response = NextResponse.json({ authenticated: false }, { headers: { 'Cache-Control': 'no-store' } });
  response.cookies.set(ADMIN_SESSION_COOKIE, '', {
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'strict',
    path: '/',
    maxAge: 0,
  });
  return response;
}
