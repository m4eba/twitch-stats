import { buildMultiInsert } from './utils.js';
import type { Game } from '@twitch-stats/twitch';
import type { Pool, QueryResult } from 'pg';

/* eslint-disable @typescript-eslint/no-explicit-any */

export async function insertUpdateGames(
  pool: Pool,
  data: Array<Game>,
  time: Date
): Promise<QueryResult<any> | undefined> {
  if (data.length === 0) return Promise.resolve(undefined);

  const insert = buildMultiInsert<Game>(
    'INSERT INTO game (game_id,name,box_art_url,updated_at) VALUES ',
    '$1,$2,$3,$4',
    data,
    (d: Game) => [d.id, d.name, d.box_art_url, time]
  );
  insert.text +=
    ' ON CONFLICT (game_id) DO UPDATE SET name=EXCLUDED.name, box_art_url=EXCLUDED.box_art_url, updated_at = EXCLUDED.updated_at';
  return pool.query(insert);
}
