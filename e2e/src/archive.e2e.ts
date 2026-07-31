import { test, before, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import pino from 'pino';
import type pg from 'pg';
import { HeadObjectCommand } from '@aws-sdk/client-s3';
import type { S3Client } from '@aws-sdk/client-s3';
import Processing from '@twitch-stats/streams-process/dist/processing.js';
import { Archiver } from '@twitch-stats/streams-archive/dist/archiver.js';
import {
  makePool,
  makeS3,
  resetDb,
  sampleStream,
  stubProducer,
  S3,
} from './harness.js';

const log = pino({ level: 'silent' });
const T0 = new Date('2026-07-23T10:00:00.000Z');
const AFTER_T0 = '2026-07-23T10:05:00.000Z';

let pool: pg.Pool;
let s3: S3Client;

before(() => {
  pool = makePool();
  s3 = makeS3();
});
after(async () => {
  await pool.end();
  s3.destroy();
});
beforeEach(async () => {
  await resetDb(pool);
});

// Seed one twitch stream and end it, so the archiver has an ended stream to
// pick up. Returns the stream id.
async function seedEndedStream(viewers: number): Promise<string> {
  const { producer } = stubProducer();
  const p = new Processing(log, pool, producer, 'stream-id', 'stream-ended');
  const s = sampleStream({ viewer_count: viewers });
  await p.processStreams('twitch', T0, [s]);
  await p.processEnd('twitch', { updateStartTime: AFTER_T0, update: true });
  return s.id;
}

test('archiver uploads an ended stream to S3, indexes it, and clears the hot store', async () => {
  const id = await seedEndedStream(300);

  const archiver = new Archiver(log, pool, s3, S3.bucket, 'e2e/');
  const collected = await archiver.collect('twitch', [id]);
  assert.equal(collected, 1);
  assert.equal(archiver.bufferedCount, 1);

  const flushed = await archiver.flush();
  assert.equal(flushed, 1);

  // archive_stream index row written with an object key
  const idx = await pool.query(
    'SELECT object_key, byte_length FROM archive_stream WHERE platform = $1 AND stream_id = $2',
    ['twitch', id]
  );
  assert.equal(idx.rows.length, 1);
  const key: string = idx.rows[0].object_key;
  assert.match(key, /^e2e\/\d{4}\/\d{2}\/\d{2}\//);
  assert.ok(idx.rows[0].byte_length > 0);

  // the object really exists in S3/MinIO
  const head = await s3.send(
    new HeadObjectCommand({ Bucket: S3.bucket, Key: key })
  );
  assert.ok((head.ContentLength ?? 0) > 0);

  // summary math: exactly one probe of 300 viewers
  const summary = await pool.query(
    'SELECT probe_count, peak_viewers, avg_viewers FROM stream_summary WHERE platform = $1 AND stream_id = $2',
    ['twitch', id]
  );
  assert.equal(summary.rows[0].probe_count, 1);
  assert.equal(summary.rows[0].peak_viewers, 300);
  assert.equal(Number(summary.rows[0].avg_viewers), 300);

  // hot store cleared for the archived stream
  const stream = await pool.query(
    'SELECT 1 FROM stream WHERE platform = $1 AND stream_id = $2',
    ['twitch', id]
  );
  assert.equal(stream.rows.length, 0);
});

test('archiver skips a stream that has not ended', async () => {
  const { producer } = stubProducer();
  const p = new Processing(log, pool, producer, 'stream-id', 'stream-ended');
  const s = sampleStream();
  await p.processStreams('twitch', T0, [s]); // live, never ended

  const archiver = new Archiver(log, pool, s3, S3.bucket, 'e2e/');
  const collected = await archiver.collect('twitch', [s.id]);
  assert.equal(collected, 0);
  assert.equal(archiver.bufferedCount, 0);
  assert.equal(await archiver.flush(), 0);

  // still live in the hot store
  const stream = await pool.query(
    'SELECT 1 FROM stream WHERE platform = $1 AND stream_id = $2',
    ['twitch', s.id]
  );
  assert.equal(stream.rows.length, 1);
});
