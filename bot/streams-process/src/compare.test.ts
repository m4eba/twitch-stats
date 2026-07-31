import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Stream } from '@twitch-stats/twitch';
import { assureGameId, changedStreams, DBStream, Split } from './compare.js';

test('assureGameId passes a numeric id through', () => {
  assert.equal(assureGameId('12345'), '12345');
});

test('assureGameId maps falsy/empty/non-numeric ids to 0', () => {
  assert.equal(assureGameId(''), '0');
  assert.equal(assureGameId(undefined as unknown as string), '0');
  assert.equal(assureGameId('abc'), '0');
  assert.equal(assureGameId('12a'), '0');
  assert.equal(assureGameId('-1'), '0'); // minus sign is non-numeric
});

function stream(over: Partial<Stream>): Stream {
  return {
    id: 's1',
    user_id: 'u1',
    user_name: 'name',
    game_id: '1',
    game_name: 'g',
    type: 'live',
    title: 'title',
    viewer_count: 0,
    started_at: '2026-07-23T00:00:00.000Z',
    language: 'en',
    thumbnail_url: 't',
    tags: [],
    ...over,
  };
}

function dbStream(over: Partial<DBStream>): DBStream {
  return {
    stream_id: 's1',
    user_id: 'u1',
    title: 'title',
    tags: [],
    game_id: '1',
    started_at: '2026-07-23T00:00:00.000Z',
    ended_at: '',
    updated_at: '',
    ...over,
  };
}

function split(incoming: Stream, stored: DBStream): Split {
  return {
    new: { ids: [], data: [] },
    old: { ids: [incoming.id], data: [incoming] },
    query: { [incoming.id]: stored },
  };
}

test('changedStreams reports no changes when everything matches', () => {
  const c = changedStreams(
    split(stream({ tags: ['x'] }), dbStream({ tags: ['x'] }))
  );
  assert.deepEqual(c, { title: [], game: [], tags: [] });
});

test('changedStreams detects a title change', () => {
  const inc = stream({ title: 'new' });
  const c = changedStreams(split(inc, dbStream({ title: 'old' })));
  assert.deepEqual(c.title, [inc]);
  assert.equal(c.game.length, 0);
  assert.equal(c.tags.length, 0);
});

test('changedStreams ignores the "" vs "0" uncategorized round-trip', () => {
  // assureGameId maps '' to '0' on write, so the stored row holds '0' while the
  // API keeps reporting ''. Comparing raw reported a change on every poll for
  // every uncategorized stream.
  const c = changedStreams(
    split(stream({ game_id: '' }), dbStream({ game_id: '0' }))
  );
  assert.deepEqual(c.game, []);
});

test('changedStreams detects a game change', () => {
  const inc = stream({ game_id: '2' });
  const c = changedStreams(split(inc, dbStream({ game_id: '1' })));
  assert.deepEqual(c.game, [inc]);
});

test('changedStreams detects a tag change (order-sensitive join)', () => {
  const inc = stream({ tags: ['a', 'b'] });
  const c = changedStreams(split(inc, dbStream({ tags: ['b', 'a'] })));
  assert.deepEqual(c.tags, [inc]);
});

test('changedStreams treats null/undefined stored tags as empty string', () => {
  // incoming has no tags, stored tags undefined -> '' === '' -> no change
  const same = changedStreams(
    split(stream({ tags: [] }), dbStream({ tags: undefined }))
  );
  assert.equal(same.tags.length, 0);

  // incoming has a tag, stored undefined -> 'x' !== '' -> change
  const inc = stream({ tags: ['x'] });
  const diff = changedStreams(split(inc, dbStream({ tags: undefined })));
  assert.deepEqual(diff.tags, [inc]);
});

test('changedStreams ignores new streams (only compares split.old)', () => {
  const c = changedStreams({
    new: { ids: ['s9'], data: [stream({ id: 's9', title: 'whatever' })] },
    old: { ids: [], data: [] },
    query: {},
  });
  assert.deepEqual(c, { title: [], game: [], tags: [] });
});
