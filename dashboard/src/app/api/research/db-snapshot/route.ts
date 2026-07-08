import { NextRequest, NextResponse } from 'next/server';
import Database from 'better-sqlite3';
import crypto from 'crypto';
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import { Readable } from 'stream';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

function authorized(req: NextRequest) {
  const expected = process.env.RESEARCH_EXPORT_TOKEN;
  if (!expected) return false;

  const raw = req.headers.get('authorization') || '';
  const token = raw.replace(/^Bearer\s+/i, '').trim();
  if (!token) return false;

  const expectedBuffer = Buffer.from(expected);
  const tokenBuffer = Buffer.from(token);
  return expectedBuffer.length === tokenBuffer.length
    && crypto.timingSafeEqual(expectedBuffer, tokenBuffer);
}

function dbPath() {
  const dataDir = process.env.DATA_DIR || path.join(process.cwd(), '..', 'data');
  return path.join(dataDir, 'noop.db');
}

export async function HEAD(req: NextRequest) {
  if (!authorized(req)) {
    return new NextResponse(null, { status: 401 });
  }

  const sourcePath = dbPath();
  const { size } = await fsp.stat(sourcePath);
  return new NextResponse(null, {
    headers: {
      'cache-control': 'no-store',
      'content-disposition': 'attachment; filename="noop-research.db"',
      'content-length': String(size),
      'content-type': 'application/vnd.sqlite3',
    },
  });
}

export async function GET(req: NextRequest) {
  if (!authorized(req)) {
    return NextResponse.json({ error: 'unauthorized' }, { status: 401 });
  }

  const sourcePath = dbPath();
  const snapshotPath = path.join('/tmp', `noop-research-${Date.now()}-${process.pid}.db`);

  let db: Database.Database | null = null;
  try {
    db = new Database(sourcePath, { readonly: true });
    await db.backup(snapshotPath);
  } catch (error) {
    await fsp.unlink(snapshotPath).catch(() => {});
    const message = error instanceof Error ? error.message : 'failed to create snapshot';
    return NextResponse.json({ error: message }, { status: 500 });
  } finally {
    db?.close();
  }

  const { size } = await fsp.stat(snapshotPath);
  const fileStream = fs.createReadStream(snapshotPath);
  fileStream.on('close', () => {
    void fsp.unlink(snapshotPath).catch(() => {});
  });
  fileStream.on('error', () => {
    void fsp.unlink(snapshotPath).catch(() => {});
  });

  return new NextResponse(Readable.toWeb(fileStream) as ReadableStream, {
    headers: {
      'cache-control': 'no-store',
      'content-disposition': 'attachment; filename="noop-research.db"',
      'content-length': String(size),
      'content-type': 'application/vnd.sqlite3',
    },
  });
}
