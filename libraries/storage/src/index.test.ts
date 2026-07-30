import { test } from 'node:test';
import assert from 'node:assert/strict';
import { chunkKey, ChunkBuffer } from './index.js';

test('chunkKey formats the prefix path deterministically from a fixed Date', () => {
  const key = chunkKey('archive/', new Date('2026-07-23T14:05:09.123Z'));
  assert.match(
    key,
    /^archive\/2026\/07\/23\/140509-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.jsonl\.gz$/
  );
});

test('chunkKey zero-pads month, day and time components', () => {
  const key = chunkKey('p/', new Date('2026-01-02T03:04:05.000Z'));
  assert.match(key, /^p\/2026\/01\/02\/030405-/);
});

test('chunkKey gives each call a distinct uuid suffix', () => {
  const d = new Date('2026-07-23T00:00:00.000Z');
  assert.notEqual(chunkKey('p/', d), chunkKey('p/', d));
});

test('ChunkBuffer starts empty', () => {
  const b = new ChunkBuffer();
  assert.equal(b.count, 0);
  assert.equal(b.byteLength, 0);
  assert.equal(b.ageMs, 0);
});

test('ChunkBuffer tracks count, byteLength and offsets across adds', async () => {
  const b = new ChunkBuffer();
  const first = await b.add('{"a":1}');
  assert.equal(first.offset, 0);
  assert.equal(b.count, 1);
  assert.equal(b.byteLength, first.length);

  const second = await b.add('{"b":2}');
  // each doc is its own gzip member appended at the running size
  assert.equal(second.offset, first.length);
  assert.equal(b.count, 2);
  assert.equal(b.byteLength, first.length + second.length);
  assert.equal(b.concat().length, b.byteLength);
});

test('ChunkBuffer reset clears everything', async () => {
  const b = new ChunkBuffer();
  await b.add('{"a":1}');
  b.reset();
  assert.equal(b.count, 0);
  assert.equal(b.byteLength, 0);
  assert.equal(b.ageMs, 0);
  assert.equal(b.concat().length, 0);
});
