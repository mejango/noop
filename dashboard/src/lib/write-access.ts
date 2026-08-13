import crypto from 'crypto';

export const ADMIN_SESSION_COOKIE = 'noop_admin_session';
export const ADMIN_SESSION_TTL_SECONDS = 12 * 60 * 60;

function timingSafeEqualString(a: string, b: string): boolean {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

function getConfiguredToken(): string | null {
  return process.env.NOOP_WRITE_TOKEN?.trim() || null;
}

function signSessionPayload(payload: string, configuredToken: string): string {
  return crypto
    .createHmac('sha256', configuredToken)
    .update(`noop-admin-session:${payload}`)
    .digest('base64url');
}

function readCookie(request: Request, name: string): string | null {
  const cookieHeader = request.headers.get('cookie') || '';
  for (const part of cookieHeader.split(';')) {
    const [rawName, ...rawValue] = part.trim().split('=');
    if (rawName === name) {
      try {
        return decodeURIComponent(rawValue.join('='));
      } catch {
        return null;
      }
    }
  }
  return null;
}

export function isWriteTokenConfigured(): boolean {
  return getConfiguredToken() != null;
}

export function validateAdminPassphrase(passphrase: string): boolean {
  const configuredToken = getConfiguredToken();
  return Boolean(configuredToken && passphrase && timingSafeEqualString(passphrase.trim(), configuredToken));
}

export function createAdminSessionToken(now = Date.now()): string {
  const configuredToken = getConfiguredToken();
  if (!configuredToken) throw new Error('NOOP_WRITE_TOKEN not configured');
  const payload = Math.floor(now / 1000 + ADMIN_SESSION_TTL_SECONDS).toString(36);
  return `${payload}.${signSessionPayload(payload, configuredToken)}`;
}

export function hasValidAdminSession(request: Request, now = Date.now()): boolean {
  const configuredToken = getConfiguredToken();
  const sessionToken = readCookie(request, ADMIN_SESSION_COOKIE);
  if (!configuredToken || !sessionToken) return false;
  const [payload, signature, ...extra] = sessionToken.split('.');
  if (!payload || !signature || extra.length > 0) return false;
  const expiresAt = Number.parseInt(payload, 36);
  if (!Number.isFinite(expiresAt) || expiresAt <= Math.floor(now / 1000)) return false;
  return timingSafeEqualString(signature, signSessionPayload(payload, configuredToken));
}

export function validateWriteAccess(request: Request): { ok: true } | { ok: false; reason: string; status: number } {
  const configuredToken = getConfiguredToken();
  if (!configuredToken) {
    return {
      ok: false,
      reason: 'NOOP_WRITE_TOKEN not configured; write endpoints are disabled',
      status: 503,
    };
  }

  if (hasValidAdminSession(request)) return { ok: true };

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
