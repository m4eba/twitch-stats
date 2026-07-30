import type { Stream } from '@twitch-stats/twitch';

export interface DBStream {
  stream_id: string;
  user_id: string;
  title: string;
  tags?: string[];
  game_id: string;
  started_at: string;
  ended_at: string;
  updated_at: string;
}

export interface Split {
  new: {
    ids: Array<string>;
    data: Array<Stream>;
  };
  old: {
    ids: Array<string>;
    data: Array<Stream>;
  };
  query: { [id: string]: DBStream };
}

export interface Changed {
  title: Array<Stream>;
  game: Array<Stream>;
  tags: Array<Stream>;
}

/**
 * Normalizes a game id to a canonical numeric string, mapping anything
 * non-numeric, empty or falsy to '0'. Kick and Twitch both hand us ids that
 * occasionally arrive empty or non-numeric; the game tables key on '0' for
 * "unknown".
 */
export function assureGameId(game_id: string): string {
  if (!game_id) return '0';
  if (game_id.length === 0) return '0';
  if (!/^\d+$/.test(game_id)) return '0';
  return game_id;
}

/**
 * Buckets the streams that already exist in the DB (split.old) by which
 * tracked field changed versus the stored row: title, game or tags. Tags are
 * compared as a comma-joined string so order matters and null/undefined tag
 * lists collapse to the empty string.
 */
export function changedStreams(split: Split): Changed {
  const result: Changed = {
    title: [],
    game: [],
    tags: [],
  };

  for (let i = 0; i < split.old.data.length; ++i) {
    const d = split.old.data[i];
    if (d.title !== split.query[d.id].title) {
      result.title.push(d);
    }
    // Compare the normalized value: the stored game_id went through
    // assureGameId, so an uncategorized stream holds '0' in the database while
    // the API keeps reporting ''. Comparing raw reports a game change on every
    // poll for every uncategorized stream.
    if (assureGameId(d.game_id) !== split.query[d.id].game_id) {
      result.game.push(d);
    }
    const dbtag = split.query[d.id].tags;
    if ((d.tags ? d.tags.join(',') : '') !== (dbtag ? dbtag.join(',') : '')) {
      result.tags.push(d);
    }
  }

  return result;
}
