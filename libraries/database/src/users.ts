import { buildMultiInsert } from './utils.js';
import type { Platform } from '@twitch-stats/twitch';
import type { Pool, QueryResult } from 'pg';

/* eslint-disable @typescript-eslint/no-explicit-any */

/**
 * Platform-neutral streamer row. Twitch fills this from a helix User; kick
 * fills it straight from the livestream listing, which already carries the
 * slug, display name and avatar. Fields with no counterpart on a platform are
 * left at their zero value rather than being invented.
 */
export interface StreamerRow {
  user_id: string;
  login: string;
  display_name: string;
  type: string;
  broadcaster_type: string;
  view_count: number;
  profile_image: string;
}

export async function insertUpdateStreamers(
  pool: Pool,
  platform: Platform,
  data: Array<StreamerRow>,
  time: Date
): Promise<QueryResult<any> | undefined> {
  if (data.length === 0) return Promise.resolve(undefined);

  const insert = buildMultiInsert<StreamerRow>(
    'INSERT INTO streamers (platform,user_id,login,display_name,type,broadcaster_type,view_count,profile_image,updated_at) VALUES',
    '$1,$2,$3,$4,$5,$6,$7,$8,$9',
    data,
    (d) => [
      platform,
      d.user_id,
      d.login,
      d.display_name,
      d.type,
      d.broadcaster_type,
      d.view_count,
      d.profile_image,
      time,
    ]
  );
  insert.text +=
    ' ON CONFLICT(platform,user_id) DO UPDATE SET login=EXCLUDED.login, display_name=EXCLUDED.display_name, profile_image=EXCLUDED.profile_image, type=EXCLUDED.type, broadcaster_type=EXCLUDED.broadcaster_type, view_count=EXCLUDED.view_count, updated_at=EXCLUDED.updated_at';

  return pool.query(insert);
}

export async function insertViewsProbes(
  pool: Pool,
  platform: Platform,
  data: Array<StreamerRow>,
  time: Date
): Promise<QueryResult<any> | undefined> {
  if (data.length === 0) return Promise.resolve(undefined);
  // insert into probe
  const insert = buildMultiInsert<StreamerRow>(
    'INSERT INTO streamers_views_probe (platform,user_id,view_count,time) VALUES ',
    '$1,$2,$3,$4',
    data,
    (d) => [platform, d.user_id, d.view_count, time]
  );
  insert.text += ' ON CONFLICT (platform,user_id,time) DO NOTHING';
  return pool.query(insert);
}
