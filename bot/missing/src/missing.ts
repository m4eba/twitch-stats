import type { Logger } from 'pino';
import type { Pool, QueryArrayResult } from 'pg';
import type { createClient } from 'redis';
import { helix } from '@twitch-stats/twitch';
import {
  insertUpdateStreamers,
  insertViewsProbes,
  insertUpdateGames,
} from '@twitch-stats/database';
import type { GameRow, StreamerRow } from '@twitch-stats/database';
import type {
  Game,
  Platform,
  User,
  Stream,
  PaginatedResult,
} from '@twitch-stats/twitch';
import Prefix, { scoped } from './prefix.js';

// Platforms this service maintains dimension rows for. Twitch needs a helix
// round-trip because GET /streams carries neither avatars nor box art; kick's
// listing already includes both, so its rows come straight off the message.
const PLATFORMS: Platform[] = ['twitch', 'kick'];

export default class Missing {
  private log: Logger;
  private pool: Pool;
  private redis: ReturnType<typeof createClient>;

  public constructor(
    log: Logger,
    pool: Pool,
    redis: ReturnType<typeof createClient>
  ) {
    this.log = log;
    this.pool = pool;
    this.redis = redis;
  }

  private addPrefix(prefix: string, ids: string[]): string[] {
    const result = new Array<string>(ids.length);
    for (let i = 0; i < ids.length; ++i) {
      result[i] = prefix + ids[i];
    }
    return result;
  }

  private valuesFromQueryResult(
    prefix: string,
    result: QueryArrayResult
  ): string[] {
    const ids = new Array<string>(result.rows.length * 2);
    for (let i = 0; i < result.rows.length; ++i) {
      ids[i * 2] = prefix + result.rows[i][0];
      ids[i * 2 + 1] = result.rows[i][0];
    }
    return ids;
  }

  private valuesFromArray(prefix: string, ids: string[]): string[] {
    const result = new Array<string>(ids.length);
    for (let i = 0; i < ids.length; ++i) {
      result[i * 2] = prefix + ids[i];
      result[i * 2 + 1] = ids[i];
    }
    return result;
  }

  public async insertIds(values: string[]): Promise<void> {
    let idx = 0;
    while (idx < values.length) {
      const command = values.slice(
        idx,
        idx + Math.min(values.length - idx, 1000)
      );
      idx = idx + command.length;
      await this.redis.mSet(command);
    }
  }

  public async checkIds(
    platform: Platform,
    prefix: string,
    ids: string[]
  ): Promise<string[]> {
    if (ids.length === 0) return [];
    const a = this.addPrefix(scoped(platform, prefix), ids);
    this.log.trace({ platform, prefix, count: ids.length }, 'checkIds');
    const existing_ids = await this.redis.mGet(a);
    let new_ids = new Array<string>(ids.length);
    let idx = 0;
    for (let i = 0; i < ids.length; ++i) {
      if (existing_ids[i] === null) {
        new_ids[idx] = ids[i];
        idx++;
      }
    }
    new_ids = new_ids.slice(0, idx);
    return new_ids;
  }

  private async getTimeFromRedis(
    platform: Platform,
    prefix: string
  ): Promise<string> {
    let user_update = await this.redis.get(scoped(platform, prefix) + 'time');
    try {
      if (user_update) {
        user_update = new Date(Date.parse(user_update)).toISOString();
      }
    } catch (e) {
      user_update = null;
    }
    if (!user_update) user_update = '1970-01-01T00:00:00.000Z';
    return user_update;
  }

  public async initRedis(): Promise<void> {
    for (const platform of PLATFORMS) {
      await this.warmStart(
        platform,
        Prefix.user,
        'select user_id, updated_at from streamers where platform = $1 and updated_at > $2 order by updated_at desc'
      );
      await this.warmStart(
        platform,
        Prefix.game,
        'select game_id, updated_at from game where platform = $1 and updated_at > $2 order by updated_at desc'
      );
    }
    this.log.info({}, 'initialized');
  }

  // Reload the ids postgres already knows about so a cold or lost redis does
  // not send the whole population back through the API. Note streamers.
  // created_at is nullable and never written by anything, so the watermark has
  // to be updated_at, which insertUpdateStreamers does maintain.
  private async warmStart(
    platform: Platform,
    prefix: string,
    sql: string
  ): Promise<void> {
    const since = await this.getTimeFromRedis(platform, prefix);
    this.log.info({ platform, prefix, since }, 'warm start from postgres');
    const rows = await this.pool.query({
      text: sql,
      values: [platform, since],
      rowMode: 'array',
    });
    await this.insertIds(
      this.valuesFromQueryResult(scoped(platform, prefix), rows)
    );
    if (rows.rows.length > 0) {
      // node-pg returns timestamptz as a Date, which the redis client rejects
      // with `TypeError: Invalid argument type`
      await this.redis.set(
        scoped(platform, prefix) + 'time',
        new Date(rows.rows[0][1]).toISOString()
      );
    }
    this.log.info(
      { platform, prefix, loaded: rows.rows.length },
      'warm start done'
    );
  }

  public async update(
    platform: Platform,
    streams: Stream[] | undefined
  ): Promise<void> {
    // Not every message on twitch-stats-streams carries a streams array -- the
    // batch-end marker sends only endConfig. streams-process tolerates that by
    // swallowing the error in its catch; this consumer's catch calls
    // process.exit(1), and when the TypeError escaped the try entirely kafkajs
    // stopped the consumer while the process stayed alive. The pod then sat
    // 1/1 Running with 0 restarts for 4 days, consuming nothing and building
    // 1.4M messages of lag (2026-07-26).
    if (!streams || streams.length === 0) return;
    const user_ids: string[] = [];
    const game_ids: string[] = [];
    const game_hash = new Set<string>();
    // keep the first sighting of each id so the kick path can hydrate straight
    // from the listing without a second lookup
    const byUser = new Map<string, Stream>();
    const byGame = new Map<string, Stream>();

    for (let i = 0; i < streams.length; ++i) {
      const s = streams[i];
      // Twitch injects a synthetic record into the live stream feed;
      // streams-process filters it too. Sending it to Helix is a 400, which
      // wedges this consumer retrying a deterministic failure forever.
      if (s.user_id === 'testDocumentId2') continue;
      if (!byUser.has(s.user_id)) {
        user_ids.push(s.user_id);
        byUser.set(s.user_id, s);
      }

      // uncategorized streams report game_id '' - Helix rejects an empty id,
      // and streams-process stores those as game 0 rather than a real category
      const gid = s.game_id;
      if (!gid || !/^\d+$/.test(gid)) continue;
      if (!game_hash.has(gid)) {
        game_ids.push(gid);
        game_hash.add(gid);
        byGame.set(gid, s);
      }
    }
    if (user_ids.length === 0 && game_ids.length === 0) return;

    const checked_user_ids = await this.checkIds(
      platform,
      Prefix.user,
      user_ids
    );
    const checked_game_ids = await this.checkIds(
      platform,
      Prefix.game,
      game_ids
    );
    this.log.trace(
      {
        platform,
        number_user: checked_user_ids.length,
        number_game: checked_game_ids.length,
      },
      'update length'
    );

    if (platform === 'twitch') {
      await Promise.all([
        this.updateUser(checked_user_ids),
        this.updateGame(checked_game_ids),
      ]);
      return;
    }

    await this.hydrateFromListing(
      platform,
      checked_user_ids.map((id) => byUser.get(id) as Stream),
      checked_game_ids.map((id) => byGame.get(id) as Stream)
    );
  }

  /**
   * Hydrate dimension rows from the stream listing itself. Kick's
   * /public/v2/livestreams response already carries the channel slug, the
   * broadcaster's display name and avatar, and the category name and art, so
   * unlike twitch there is nothing left to fetch - the rows are built from the
   * message that triggered them and cost no API quota at all.
   */
  private async hydrateFromListing(
    platform: Platform,
    users: Stream[],
    games: Stream[]
  ): Promise<void> {
    const time = new Date();

    if (users.length > 0) {
      const rows: StreamerRow[] = users.map((s) => ({
        user_id: s.user_id,
        login: s.user_login ?? s.user_name,
        display_name: s.user_name,
        // kick exposes neither of these; leave them empty rather than invent a
        // value that would read as real data
        type: '',
        broadcaster_type: '',
        // and it has no profile view count, so no views probe is written
        view_count: 0,
        profile_image: s.profile_image_url ?? '',
      }));
      this.log.debug(
        { platform, count: rows.length },
        'insert update streamers'
      );
      await insertUpdateStreamers(this.pool, platform, rows, time);
      await this.insertIds(
        this.valuesFromArray(
          scoped(platform, Prefix.user),
          rows.map((r) => r.user_id)
        )
      );
      await this.redis.set(
        scoped(platform, Prefix.user) + 'time',
        time.toISOString()
      );
    }

    if (games.length > 0) {
      const rows: GameRow[] = games.map((s) => ({
        game_id: s.game_id,
        name: s.game_name,
        box_art_url: s.game_box_art_url ?? '',
      }));
      this.log.debug({ platform, count: rows.length }, 'insert update games');
      await insertUpdateGames(this.pool, platform, rows, time);
      await this.insertIds(
        this.valuesFromArray(
          scoped(platform, Prefix.game),
          rows.map((r) => r.game_id)
        )
      );
      await this.redis.set(
        scoped(platform, Prefix.game) + 'time',
        time.toISOString()
      );
    }
  }

  public async updateUser(ids: string[]): Promise<void> {
    if (ids.length === 0) return;
    const time = new Date();
    const new_ids = [...ids];
    const found: string[] = [];

    while (new_ids.length > 0) {
      const params = new_ids.splice(0, 100);
      const urlParams = new URLSearchParams();
      for (let i = 0; i < params.length; ++i) {
        urlParams.append('id', params[i]);
      }
      const users = await helix<PaginatedResult<User>>(
        `users?${urlParams.toString()}`,
        null
      );
      if (!Array.isArray(users.data)) {
        throw new Error('helix returned no data array for users');
      }
      this.log.debug({ count: users.data.length }, 'insert update streamers');
      const rows: StreamerRow[] = users.data.map((u: User) => ({
        user_id: u.id,
        login: u.login,
        display_name: u.display_name,
        type: u.type,
        broadcaster_type: u.broadcaster_type,
        view_count: u.view_count,
        profile_image: u.profile_image_url,
      }));
      await insertUpdateStreamers(this.pool, 'twitch', rows, time);
      await insertViewsProbes(this.pool, 'twitch', rows, time);
      for (const u of users.data) found.push(u.id);
    }

    // Only cache ids that were actually stored. Marking a requested-but-absent
    // id as known (deleted or banned users are simply omitted by Helix) would
    // suppress every future retry, and these keys have no TTL.
    if (found.length > 0) {
      await this.insertIds(
        this.valuesFromArray(scoped('twitch', Prefix.user), found)
      );
    }
    await this.redis.set(
      scoped('twitch', Prefix.user) + 'time',
      time.toISOString()
    );
  }

  public async updateGame(ids: string[]): Promise<void> {
    if (ids.length === 0) return;
    const time = new Date();
    const new_ids = [...ids];
    const found: string[] = [];

    while (new_ids.length > 0) {
      const params = new_ids.splice(0, 100);
      const urlParams = new URLSearchParams();
      for (let i = 0; i < params.length; ++i) {
        urlParams.append('id', params[i]);
      }
      const games = await helix<PaginatedResult<Game>>(
        `games?${urlParams.toString()}`,
        null
      );
      if (!Array.isArray(games.data)) {
        throw new Error('helix returned no data array for games');
      }
      this.log.debug({ count: games.data.length }, 'insert update games');
      const rows: GameRow[] = games.data.map((g: Game) => ({
        game_id: g.id,
        name: g.name,
        box_art_url: g.box_art_url,
      }));
      await insertUpdateGames(this.pool, 'twitch', rows, time);
      for (const g of games.data) found.push(g.id);
    }

    if (found.length > 0) {
      await this.insertIds(
        this.valuesFromArray(scoped('twitch', Prefix.game), found)
      );
    }
    await this.redis.set(
      scoped('twitch', Prefix.game) + 'time',
      time.toISOString()
    );
  }
}
