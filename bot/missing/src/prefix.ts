import type { Platform } from '@twitch-stats/twitch';

/* eslint-disable @rushstack/typedef-var */
export const Prefix = {
  user: 'user_id_',
  game: 'game_id_',
  tag: 'tag_id_',
};

/**
 * Redis keys must be platform-scoped. Kick broadcaster ids and category ids are
 * plain integers drawn from an id space that overlaps twitch's, so an unscoped
 * `user_id_12345` would let a kick streamer mask a twitch one of the same id
 * (or the reverse) and the masked one would never be hydrated.
 *
 * Changing the scheme orphans the existing unscoped keys. That is self-healing:
 * the watermark key moves with the prefix too, so the next boot finds none,
 * falls back to 1970 and reloads the whole population from postgres in
 * initRedis - no API calls. The orphans can be dropped afterwards with
 * `redis-cli --scan --pattern 'user_id_*' | xargs redis-cli del`.
 */
export function scoped(platform: Platform, prefix: string): string {
  return `${platform}:${prefix}`;
}

export default Prefix;
