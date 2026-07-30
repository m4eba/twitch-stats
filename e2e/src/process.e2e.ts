import { test, before, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import pino from 'pino';
import type pg from 'pg';
import Processing from '@twitch-stats/streams-process/dist/processing.js';
import { makePool, resetDb, sampleStream, stubProducer } from './harness.js';

const log = pino({ level: 'silent' });
const T0 = new Date('2026-07-23T10:00:00.000Z');
const AFTER_T0 = '2026-07-23T10:05:00.000Z';

let pool: pg.Pool;

before(() => {
  pool = makePool();
});
after(async () => {
  await pool.end();
});
beforeEach(async () => {
  await resetDb(pool);
});

test('processStreams writes stream, user_online and probe rows with the platform', async () => {
  const { producer } = stubProducer();
  const p = new Processing(log, pool, producer, 'stream-id', 'stream-ended');
  const s = sampleStream({ viewer_count: 250 });

  await p.processStreams('twitch', T0, [s]);

  const stream = await pool.query(
    'SELECT platform, user_id, title, game_id, ended_at FROM stream WHERE stream_id = $1',
    [s.id]
  );
  assert.equal(stream.rows.length, 1);
  assert.equal(stream.rows[0].platform, 'twitch');
  assert.equal(stream.rows[0].title, s.title);
  assert.equal(stream.rows[0].ended_at, null);

  const online = await pool.query(
    'SELECT platform FROM user_online WHERE stream_id = $1',
    [s.id]
  );
  assert.equal(online.rows.length, 1);
  assert.equal(online.rows[0].platform, 'twitch');

  const probe = await pool.query(
    'SELECT viewers FROM probe WHERE platform = $2 AND stream_id = $1',
    [s.id, 'twitch']
  );
  assert.equal(probe.rows.length, 1);
  assert.equal(probe.rows[0].viewers, 250);
});

test('processEnd only ends streams of its own platform (load-bearing predicate)', async () => {
  const { producer, sent } = stubProducer();
  const p = new Processing(log, pool, producer, 'stream-id', 'stream-ended');

  const tw = sampleStream();
  const kick = sampleStream();
  await p.processStreams('twitch', T0, [tw]);
  await p.processStreams('kick', T0, [kick]);

  // sweep twitch only, with a cutoff after both were last seen
  await p.processEnd('twitch', { updateStartTime: AFTER_T0, update: true });

  const twRow = await pool.query(
    'SELECT ended_at FROM stream WHERE platform = $1 AND stream_id = $2',
    ['twitch', tw.id]
  );
  const kickRow = await pool.query(
    'SELECT ended_at FROM stream WHERE platform = $1 AND stream_id = $2',
    ['kick', kick.id]
  );
  assert.notEqual(twRow.rows[0].ended_at, null, 'twitch stream should be ended');
  assert.equal(
    kickRow.rows[0].ended_at,
    null,
    'kick stream must be untouched by a twitch sweep'
  );

  // twitch user_online cleared, kick still live
  const online = await pool.query(
    'SELECT platform FROM user_online ORDER BY platform'
  );
  assert.deepEqual(
    online.rows.map((r) => r.platform),
    ['kick']
  );

  // producer got platform-scoped id + ended messages for twitch
  const idMsg = sent.find((m) => m.topic === 'stream-id');
  const endedMsg = sent.find((m) => m.topic === 'stream-ended');
  assert.equal((idMsg?.value as { platform: string }).platform, 'twitch');
  assert.equal((endedMsg?.value as { platform: string }).platform, 'twitch');
  assert.deepEqual((idMsg?.value as { ids: string[] }).ids, [tw.user_id]);
});

test('processEnd on an empty sweep publishes nothing and ends no streams', async () => {
  const { producer, sent } = stubProducer();
  const p = new Processing(log, pool, producer, 'stream-id', 'stream-ended');

  const tw = sampleStream();
  await p.processStreams('twitch', T0, [tw]);

  // cutoff BEFORE the stream was seen -> nothing older than it
  await p.processEnd('twitch', {
    updateStartTime: '2026-07-23T09:00:00.000Z',
    update: true,
  });

  const row = await pool.query(
    'SELECT ended_at FROM stream WHERE stream_id = $1',
    [tw.id]
  );
  assert.equal(row.rows[0].ended_at, null);
  // update:true still fires the id message, but with an empty id list
  const idMsg = sent.find((m) => m.topic === 'stream-id');
  assert.deepEqual((idMsg?.value as { ids: string[] }).ids, []);
  assert.equal(
    sent.find((m) => m.topic === 'stream-ended'),
    undefined
  );
});
