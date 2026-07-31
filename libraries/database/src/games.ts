import { buildMultiInsert } from './utils.js';
import type { Platform } from '@twitch-stats/twitch';
import type { Pool, QueryResult } from 'pg';

/* eslint-disable @typescript-eslint/no-explicit-any */

/**
 * Platform-neutral game/category row. Twitch fills this from a helix Game;
 * kick fills it from the category embedded in the livestream listing.
 */
export interface GameRow {
  game_id: string;
  name: string;
  box_art_url: string;
}

export async function insertUpdateGames(
  pool: Pool,
  platform: Platform,
  data: Array<GameRow>,
  time: Date
): Promise<QueryResult<any> | undefined> {
  if (data.length === 0) return Promise.resolve(undefined);

  const insert = buildMultiInsert<GameRow>(
    'INSERT INTO game (platform,game_id,name,box_art_url,updated_at) VALUES ',
    '$1,$2,$3,$4,$5',
    data,
    (d: GameRow) => [platform, d.game_id, d.name, d.box_art_url, time]
  );
  insert.text +=
    ' ON CONFLICT (platform,game_id) DO UPDATE SET name=EXCLUDED.name, box_art_url=EXCLUDED.box_art_url, updated_at = EXCLUDED.updated_at';
  return pool.query(insert);
}
