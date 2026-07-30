import type { KickLivestream } from '@twitch-stats/kick';
import type { Stream } from '@twitch-stats/twitch';
import type { Logger } from 'pino';

// Map a Kick livestream onto the pipeline's normalized stream shape. The field
// set lines up with Twitch's almost exactly; the only real differences are that
// the livestream id is a UUID rather than a numeric id (hence the text
// stream_id column) and that the category may be absent.
export function toStream(l: KickLivestream, logger: Logger): Stream | null {
  const started = Date.parse(l.started_at);
  if (Number.isNaN(started)) {
    logger.warn(
      { id: l.id, started_at: l.started_at },
      'unparseable started_at, skipping'
    );
    return null;
  }
  return {
    id: l.id,
    user_id: String(l.broadcaster_user.id),
    user_name: l.broadcaster_user.username,
    game_id: l.category ? String(l.category.id) : '0',
    game_name: l.category ? l.category.name : '',
    type: 'live',
    title: l.title,
    viewer_count: l.viewer_count,
    started_at: new Date(started).toISOString(),
    language: l.language_code,
    thumbnail_url: l.thumbnail,
    tags: l.tags ?? [],
  };
}
