// e2e orchestrator: bring up ephemeral Postgres + MinIO, apply migrations,
// create the bucket, run every *.e2e.ts, and tear the infra down no matter
// what. Run with: npm run test:e2e  (from the e2e workspace).
import { spawn, spawnSync } from 'node:child_process';
import { readdirSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import pg from 'pg';
import {
  CreateBucketCommand,
  HeadBucketCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import { PG, S3, makeS3 } from './src/harness.js';

const here = dirname(fileURLToPath(import.meta.url));
const composeFile = join(here, 'docker-compose.yml');
const migrationsDir = join(here, '..', 'db', 'migrations');
const PROJECT = 'twstats-e2e';

function compose(args: string[]): void {
  const r = spawnSync(
    'docker',
    ['compose', '-p', PROJECT, '-f', composeFile, ...args],
    { stdio: 'inherit' }
  );
  if (r.status !== 0) {
    throw new Error(`docker compose ${args.join(' ')} exited ${r.status}`);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitForPostgres(timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const client = new pg.Client(PG);
    try {
      await client.connect();
      await client.query('SELECT 1');
      await client.end();
      return;
    } catch (e) {
      await client.end().catch(() => undefined);
      if (Date.now() > deadline) throw new Error(`postgres not ready: ${e}`);
      await sleep(500);
    }
  }
}

async function waitForMinio(timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      const res = await fetch(`${S3.endpoint}/minio/health/live`);
      if (res.ok) return;
    } catch {
      // not up yet
    }
    if (Date.now() > deadline) throw new Error('minio not ready');
    await sleep(500);
  }
}

// Apply the dbmate migrations by running each file's up-section. Avoids a
// dbmate binary dependency; the up-section is everything between the
// `-- migrate:up` and `-- migrate:down` markers.
async function migrate(): Promise<void> {
  const files = readdirSync(migrationsDir)
    .filter((f) => f.endsWith('.sql'))
    .sort();
  const client = new pg.Client(PG);
  await client.connect();
  try {
    for (const f of files) {
      const sql = readFileSync(join(migrationsDir, f), 'utf8');
      // up-section = everything before the down marker, minus the whole
      // `-- migrate:up[ options]` line (dbmate allows e.g. transaction:false).
      const up = sql
        .split('-- migrate:down')[0]
        .replace(/--\s*migrate:up[^\n]*\n/, '');
      await client.query(up);
    }
  } finally {
    await client.end();
  }
}

async function ensureBucket(): Promise<void> {
  const s3: S3Client = makeS3();
  try {
    await s3.send(new HeadBucketCommand({ Bucket: S3.bucket }));
  } catch {
    await s3.send(new CreateBucketCommand({ Bucket: S3.bucket }));
  }
  s3.destroy();
}

function runTests(): Promise<number> {
  const files = readdirSync(join(here, 'src'))
    .filter((f) => f.endsWith('.e2e.ts'))
    .map((f) => join(here, 'src', f));
  return new Promise((resolve) => {
    // --test-concurrency=1: the e2e files share one database and TRUNCATE it,
    // so their processes must run one at a time, not in parallel.
    const child = spawn(
      'node',
      ['--import', 'tsx', '--test', '--test-concurrency=1', ...files],
      { stdio: 'inherit', cwd: here }
    );
    child.on('exit', (code) => resolve(code ?? 1));
  });
}

async function main(): Promise<number> {
  compose(['up', '-d']);
  try {
    console.log('waiting for postgres + minio...');
    await waitForPostgres(60000);
    await waitForMinio(60000);
    console.log('applying migrations...');
    await migrate();
    await ensureBucket();
    console.log('running e2e tests...\n');
    return await runTests();
  } finally {
    console.log('\ntearing down infra...');
    compose(['down', '-v']);
  }
}

process.exitCode = await main();
