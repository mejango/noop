import fs from 'node:fs';
import path from 'node:path';
import { timingSafeEqual } from 'node:crypto';
import { Readable } from 'node:stream';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

// Disabled unless an operator explicitly enables a short-lived download while
// trading is paused. Only the encrypted migration artifact can be served.
export async function GET(request: Request) {
  const token = process.env.NOOP_BACKUP_DOWNLOAD_TOKEN || '';
  const expires = Date.parse(process.env.NOOP_BACKUP_DOWNLOAD_EXPIRES || '');
  const authorization = request.headers.get('authorization') || '';
  const expected = `Bearer ${token}`;
  const now = Date.now();
  if (process.env.DERIVE_MAINTENANCE !== 'true' || !/^[a-f0-9]{64}$/.test(token)
    || !Number.isFinite(expires) || expires <= now || expires > now + 60 * 60_000
    || Buffer.byteLength(authorization) !== Buffer.byteLength(expected)
    || !timingSafeEqual(Buffer.from(authorization), Buffer.from(expected))) {
    return new Response(null, { status: 404, headers: { 'Cache-Control': 'no-store' } });
  }
  const filename = path.join(process.env.DATA_DIR || '/data', 'archive', 'derive-v3-export-20261006.enc');
  try {
    const stat = fs.statSync(filename);
    if (!stat.isFile()) throw new Error('Unavailable');
    const range = request.headers.get('range');
    const match = range?.match(/^bytes=(\d+)-(\d*)$/);
    const start = match ? Number(match[1]) : 0;
    const end = match && match[2] ? Math.min(Number(match[2]), stat.size - 1) : stat.size - 1;
    if (range && (!match || !Number.isSafeInteger(start) || !Number.isSafeInteger(end)
      || start < 0 || start >= stat.size || end < start)) {
      return new Response(null, { status: 416, headers: { 'Content-Range': `bytes */${stat.size}`, 'Cache-Control': 'no-store' } });
    }
    const stream = Readable.toWeb(fs.createReadStream(filename, { start, end })) as ReadableStream;
    return new Response(stream, { status: range ? 206 : 200, headers: {
      'Content-Type': 'application/octet-stream',
      'Content-Length': String(end - start + 1),
      'Accept-Ranges': 'bytes',
      ...(range ? { 'Content-Range': `bytes ${start}-${end}/${stat.size}` } : {}),
      'Content-Disposition': 'attachment; filename="derive-v3-export-20261006.enc"',
      'Cache-Control': 'private, no-store',
      'X-Content-Type-Options': 'nosniff',
    } });
  } catch {
    return new Response(null, { status: 404, headers: { 'Cache-Control': 'no-store' } });
  }
}
