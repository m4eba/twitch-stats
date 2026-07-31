import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Logger } from 'pino';
import type { KickLivestream } from '@twitch-stats/kick';
import { toStream } from './toStream.js';

// Minimal logger stub — toStream only ever calls logger.warn. Returns the
// stub plus a mutable counter of how many warnings it recorded.
function stubLogger(): { logger: Logger; warnings: () => number } {
  let warnings = 0;
  const logger = {
    warn: () => {
      warnings++;
    },
  } as unknown as Logger;
  return { logger, warnings: () => warnings };
}

function livestream(over: Partial<KickLivestream> = {}): KickLivestream {
  return {
    id: 'uuid-1',
    title: 'hello',
    thumbnail: 'https://t/img.jpg',
    started_at: '2026-07-23T10:00:00.000Z',
    viewer_count: 42,
    language_code: 'en',
    tags: ['a', 'b'],
    has_mature_content: false,
    broadcaster_user: { id: 777, username: 'streamer', profile_picture: 'p' },
    category: { id: 12, name: 'Just Chatting', thumbnail: 'c' },
    channel: { slug: 'streamer' },
    ...over,
  };
}

test('toStream maps a full livestream onto the normalized shape', () => {
  const { logger } = stubLogger();
  const s = toStream(livestream(), logger);
  assert.deepEqual(s, {
    id: 'uuid-1',
    user_id: '777', // numeric broadcaster id coerced to string
    user_name: 'streamer',
    game_id: '12',
    game_name: 'Just Chatting',
    type: 'live',
    title: 'hello',
    viewer_count: 42,
    started_at: '2026-07-23T10:00:00.000Z',
    language: 'en',
    thumbnail_url: 'https://t/img.jpg',
    tags: ['a', 'b'],
    user_login: 'streamer',
    profile_image_url: 'p',
    game_box_art_url: 'c',
  });
});

test('toStream falls back to 0/"" when the category is absent', () => {
  const { logger } = stubLogger();
  const s = toStream(
    livestream({ category: undefined as unknown as KickLivestream['category'] }),
    logger
  );
  assert.equal(s?.game_id, '0');
  assert.equal(s?.game_name, '');
});

test('toStream defaults missing tags to an empty array', () => {
  const { logger } = stubLogger();
  const s = toStream(
    livestream({ tags: undefined as unknown as string[] }),
    logger
  );
  assert.deepEqual(s?.tags, []);
});

test('toStream returns null and warns on an unparseable started_at', () => {
  const stub = stubLogger();
  const s = toStream(livestream({ started_at: 'not-a-date' }), stub.logger);
  assert.equal(s, null);
  assert.equal(stub.warnings(), 1);
});

test('toStream carries the dimension metadata kick gives us for free', () => {
  const { logger } = stubLogger();
  const s = toStream(livestream(), logger);
  // these are what let bot/missing hydrate streamers/game with no API call
  assert.equal(s?.user_login, 'streamer');
  assert.equal(s?.profile_image_url, 'p');
  assert.equal(s?.game_box_art_url, 'c');
  assert.equal(s?.user_name, 'streamer');
  assert.equal(s?.game_name, 'Just Chatting');
});

test('toStream falls back to the username when a channel has no slug', () => {
  const { logger } = stubLogger();
  const l = livestream();
  delete (l as { channel?: unknown }).channel;
  const s = toStream(l, logger);
  assert.equal(s?.user_login, 'streamer');
});

test('toStream leaves box art empty for an uncategorized stream', () => {
  const { logger } = stubLogger();
  const l = livestream();
  delete (l as { category?: unknown }).category;
  const s = toStream(l, logger);
  assert.equal(s?.game_id, '0');
  assert.equal(s?.game_box_art_url, '');
});
