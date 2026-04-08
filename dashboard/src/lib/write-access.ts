import crypto from 'crypto';

function timingSafeEqualString(a: string, b: string): boolean {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

export function validateWriteAccess(request: Request): { ok: true } | { ok: false; reason: string; status: number } {
  const configuredToken = process.env.NOOP_WRITE_TOKEN?.trim();
  if (!configuredToken) {
    return {
      ok: false,
      reason: 'NOOP_WRITE_TOKEN not configured; write endpoints are disabled',
      status: 503,
    };
  }

  const providedToken = request.headers.get('x-noop-write-token')?.trim();
  if (!providedToken || !timingSafeEqualString(providedToken, configuredToken)) {
    return {
      ok: false,
      reason: 'write access denied',
      status: 401,
    };
  }

  return { ok: true };
}
