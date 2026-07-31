import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  backoffMs,
  isAbortError,
  buildUrl,
  getLivestreams,
  LIVESTREAMS_MAX_LIMIT,
} from './index.js';

test('backoffMs doubles each try then saturates at the 30s cap', () => {
  assert.equal(backoffMs(1), 2000);
  assert.equal(backoffMs(2), 4000);
  assert.equal(backoffMs(3), 8000);
  assert.equal(backoffMs(4), 16000);
  assert.equal(backoffMs(5), 30000); // 32000 capped
  assert.equal(backoffMs(10), 30000); // stays capped
});

test('isAbortError only matches objects named AbortError', () => {
  assert.equal(isAbortError({ name: 'AbortError' }), true);
  const e = new Error('x');
  e.name = 'AbortError';
  assert.equal(isAbortError(e), true);
  assert.equal(isAbortError(new Error('other')), false);
  assert.equal(isAbortError(null), false);
  assert.equal(isAbortError(undefined), false);
  assert.equal(isAbortError('AbortError'), false);
});

test('buildUrl returns a bare url when there are no params', () => {
  assert.equal(
    buildUrl('public/v2/livestreams'),
    'https://api.kick.com/public/v2/livestreams'
  );
});

test('buildUrl strips leading slashes from the path', () => {
  assert.equal(buildUrl('//foo/bar'), 'https://api.kick.com/foo/bar');
});

test('buildUrl omits the ? for an empty params object', () => {
  assert.equal(buildUrl('foo', {}), 'https://api.kick.com/foo');
});

test('buildUrl repeats array params as multiple keys', () => {
  assert.equal(
    buildUrl('foo', { category_id: ['1', '2'], limit: '10' }),
    'https://api.kick.com/foo?category_id=1&category_id=2&limit=10'
  );
});

test('getLivestreams rejects an out-of-range limit before any network call', async () => {
  await assert.rejects(
    () => getLivestreams({ limit: 0 }),
    /limit must be between 1 and 1000/
  );
  await assert.rejects(
    () => getLivestreams({ limit: LIVESTREAMS_MAX_LIMIT + 1 }),
    /limit must be between 1 and 1000/
  );
});
