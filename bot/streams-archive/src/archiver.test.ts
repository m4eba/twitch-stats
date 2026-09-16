import { describe, it } from 'node:test';
import assert from 'node:assert';
import type { Pool } from 'pg';
import type { Logger } from 'pino';
import type { S3Client } from '@aws-sdk/client-s3';
import { Archiver } from './archiver.js';

interface Call {
  text: string;
  values: any[];
}

const cutoff = new Date('2026-09-14T08:00:00Z');

// streams in the hot store, ordered by (started_at, stream_id)
const rows = [
  { stream_id: '1', started_at: new Date('2026-09-13T01:00:00Z') },
  { stream_id: '2', started_at: new Date('2026-09-13T02:00:00Z') },
  { stream_id: '3', started_at: new Date('2026-09-13T03:00:00Z') },
];

function fakePool(): { pool: Pool; calls: Call[] } {
  const calls: Call[] = [];
  const query = async (q: any, v?: any[]): Promise<{ rows: any[] }> => {
    const text: string = typeof q === 'string' ? q : q.text;
    const values: any[] = (typeof q === 'string' ? v : q.values) ?? [];
    calls.push({ text, values });
    if (text.includes('AS cutoff')) return { rows: [{ cutoff }] };
    if (text.includes('SELECT stream_id, started_at FROM stream')) {
      const [, afterStarted, afterId, limit] = values;
      const after = rows.filter(
        (r) =>
          afterStarted === '-infinity' ||
          r.started_at > afterStarted ||
          (r.started_at.getTime() === afterStarted.getTime() &&
            BigInt(r.stream_id) > BigInt(afterId))
      );
      return { rows: after.slice(0, limit) };
    }
    if (text.includes('FROM stream') && text.includes('COALESCE')) {
      const ids: string[] = values[0];
      return {
        rows: ids.map((id) => ({
          stream_id: id,
          user_id: '10',
          title: 't',
          tags: null,
          game_id: '0',
          started_at: new Date('2026-09-13T01:00:00Z'),
          ended_at: new Date('2026-09-13T05:00:00Z'),
        })),
      };
    }
    if (text.includes('FROM probe')) {
      return {
        rows: values[0].map((id: string) => ({
          stream_id: id,
          viewers: 7,
          time: new Date('2026-09-13T02:00:00Z'),
        })),
      };
    }
    return { rows: [] };
  };
  const pool = {
    query,
    connect: async () => ({ query, release: () => undefined }),
  };
  return { pool: pool as unknown as Pool, calls };
}

function fakeS3(): { s3: S3Client; puts: number[] } {
  const puts: number[] = [];
  const s3 = {
    send: async (cmd: any) => {
      puts.push(cmd.input.Body.length);
      return {};
    },
  };
  return { s3: s3 as unknown as S3Client, puts };
}

const log = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  debug: () => undefined,
} as unknown as Logger;

describe('Archiver.sweepCutoff', () => {
  it('measures maxAgeHours back from the newest data', async () => {
    const { pool, calls } = fakePool();
    const archiver = new Archiver(log, pool, fakeS3().s3, 'b', 'archive/');
    assert.deepStrictEqual(await archiver.sweepCutoff(52), cutoff);
    assert.match(calls[0].text, /least\(now\(\), max\(updated_at\)\)/);
    assert.deepStrictEqual(calls[0].values, [52]);
  });
});

describe('Archiver.sweep', () => {
  it('pages through old streams and archives them in one flush', async () => {
    const { pool, calls } = fakePool();
    const { s3, puts } = fakeS3();
    const archiver = new Archiver(log, pool, s3, 'b', 'archive/');
    const flushed: number[] = [];

    const count = await archiver.sweep(cutoff, {
      batchSize: 2,
      flushBytes: 64 * 1024 * 1024,
      onFlush: (streams) => flushed.push(streams),
    });

    assert.strictEqual(count, 3);
    assert.deepStrictEqual(flushed, [3]);
    assert.strictEqual(puts.length, 1);

    const pages = calls.filter((c) =>
      c.text.includes('SELECT stream_id, started_at FROM stream')
    );
    assert.strictEqual(pages.length, 3);
    assert.deepStrictEqual(pages[0].values, [cutoff, '-infinity', '0', 2]);
    // the cursor continues after the last row of the previous page
    assert.deepStrictEqual(pages[1].values, [
      cutoff,
      rows[1].started_at,
      '2',
      2,
    ]);

    // collect is bounded by the cutoff and does not depend on ended_at
    const collects = calls.filter((c) => c.text.includes('COALESCE'));
    assert.strictEqual(collects.length, 2);
    for (const c of collects) {
      assert.deepStrictEqual(c.values[1], cutoff);
      assert.doesNotMatch(c.text, /ended_at IS NOT NULL/);
      assert.match(c.text, /COALESCE\(ended_at, updated_at, started_at\)/);
    }

    // every archived stream leaves the hot store, live flag or not
    const del = calls.find((c) => c.text.startsWith('DELETE FROM stream'));
    assert.ok(del);
    assert.doesNotMatch(del.text, /ended_at/);
    assert.deepStrictEqual(del.values[0], ['1', '2', '3']);
    const delOnline = calls.find((c) =>
      c.text.startsWith('DELETE FROM user_online')
    );
    assert.deepStrictEqual(delOnline?.values[0], ['1', '2', '3']);
    assert.strictEqual(calls[calls.length - 1].text, 'COMMIT');
  });

  it('flushes whenever the buffer reaches flushBytes', async () => {
    const { pool } = fakePool();
    const { s3, puts } = fakeS3();
    const archiver = new Archiver(log, pool, s3, 'b', 'archive/');
    const flushed: number[] = [];

    await archiver.sweep(cutoff, {
      batchSize: 2,
      flushBytes: 1,
      onFlush: (streams) => flushed.push(streams),
    });

    assert.deepStrictEqual(flushed, [2, 1]);
    assert.strictEqual(puts.length, 2);
  });

  it('does nothing when asked to stop before the first batch', async () => {
    const { pool, calls } = fakePool();
    const { s3, puts } = fakeS3();
    const archiver = new Archiver(log, pool, s3, 'b', 'archive/');

    const count = await archiver.sweep(cutoff, {
      batchSize: 2,
      flushBytes: 64 * 1024 * 1024,
      shouldStop: () => true,
    });

    assert.strictEqual(count, 0);
    assert.strictEqual(calls.length, 0);
    assert.strictEqual(puts.length, 0);
  });

  it('flushes what it collected when stopped mid-sweep', async () => {
    const { pool, calls } = fakePool();
    const { s3, puts } = fakeS3();
    const archiver = new Archiver(log, pool, s3, 'b', 'archive/');
    let batches = 0;

    const count = await archiver.sweep(cutoff, {
      batchSize: 2,
      flushBytes: 64 * 1024 * 1024,
      shouldStop: () => batches++ > 0,
    });

    assert.strictEqual(count, 2);
    assert.strictEqual(puts.length, 1);
    const del = calls.find((c) => c.text.startsWith('DELETE FROM stream'));
    assert.deepStrictEqual(del?.values[0], ['1', '2']);
  });
});
