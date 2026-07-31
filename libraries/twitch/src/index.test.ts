import { test } from 'node:test';
import assert from 'node:assert/strict';
import { platformOf, DEFAULT_PLATFORM } from './index.js';

test('platformOf defaults a message with no platform to twitch', () => {
  // load-bearing for legacy replays: pre-platform messages must map to twitch.
  assert.equal(platformOf({}), 'twitch');
  assert.equal(DEFAULT_PLATFORM, 'twitch');
});

test('platformOf passes an explicit platform through', () => {
  assert.equal(platformOf({ platform: 'kick' }), 'kick');
  assert.equal(platformOf({ platform: 'twitch' }), 'twitch');
});
