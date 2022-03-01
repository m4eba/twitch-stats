import { buildMultiInsert } from './utils.js';
import type { Tag } from '@twitch-stats/twitch';
import type { Pool, QueryResult } from 'pg';

/* eslint-disable @typescript-eslint/no-explicit-any */

export async function insertUpdateTags(
  pool: Pool,
  data: Array<Tag>,
  time: Date
): Promise<QueryResult<any> | undefined> {
  if (data.length === 0) return Promise.resolve(undefined);

  const insert = buildMultiInsert<Tag>(
    'INSERT INTO tags (tag_id,is_auto,localization_names,localization_descriptions,updated_at) VALUES ',
    '$1,$2,$3,$4,$5',
    data,
    (d) => [
      d.tag_id,
      d.is_auto,
      JSON.stringify(d.localization_names, null, ' '),
      JSON.stringify(d.localization_descriptions, null, ' '),
      time,
    ]
  );
  insert.text +=
    ' ON CONFLICT (tag_id) DO UPDATE SET is_auto=EXCLUDED.is_auto, localization_names=EXCLUDED.localization_names, localization_descriptions=EXCLUDED.localization_descriptions, updated_at = EXCLUDED.updated_at';
  return pool.query(insert);
}
