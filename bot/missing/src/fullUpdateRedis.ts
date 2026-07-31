import {
  FileConfig,
  FileConfigOpt,
  PostgresConfig,
  PostgresConfigOpt,
  LogConfig,
  LogConfigOpt,
} from '@twitch-stats/config';
import pg from 'pg';
import { postgresConfig } from '@twitch-stats/database';
import QueryStream from 'pg-query-stream';
import { createClient } from 'redis';
import pino, { Logger } from 'pino';
import { ArgumentConfig, parse } from 'ts-command-line-args';
import Prefix, { scoped } from './prefix.js';
import type { Platform } from '@twitch-stats/twitch';

const PLATFORMS: Platform[] = ['twitch', 'kick'];

interface UpdateConfig {
  redisUrl: string;
  batchSize: number;
}

const UpdateConfigOpt: ArgumentConfig<UpdateConfig> = {
  redisUrl: { type: String },
  batchSize: { type: Number, defaultValue: 1000 },
};

interface Config extends UpdateConfig, FileConfig, PostgresConfig, LogConfig {}

const config: Config = parse<Config>(
  {
    ...UpdateConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
    ...PostgresConfigOpt,
  },
  {
    loadFromFileArg: 'config',
  }
);

const logger: Logger = pino({ level: config.logLevel }).child({
  module: 'fullUpdateRedis',
});

const client: pg.Client = new pg.Client(await postgresConfig(config));
await client.connect();

const redis: ReturnType<typeof createClient> = createClient({
  url: config.redisUrl,
});
await redis.connect();

redis.on('error', (err: Error) => {
  logger.error({ error: err }, 'redis error');
});

let count: number = 0;
const updateRedis = async (ids: string[]): Promise<void> => {
  // MSET with no arguments is a protocol error; an exact multiple of
  // batchSize (or an empty table) leaves nothing for the final call
  if (ids.length === 0) return;
  logger.debug({ update: ids.length }, 'redis update');
  count += ids.length;
  await redis.mSet(ids);
};

// `for await` applies backpressure and runs the writes sequentially. The
// previous 'data' handler was async but never paused the stream, so mSet calls
// stayed in flight past the 'end' event and could still be running when the
// job disconnected - silently dropping keys. A stream error also had no
// listener and no reject path, so it hung the job forever instead of failing.
const drainIds = async (
  sql: string,
  column: string,
  prefix: string,
  platform: string
): Promise<void> => {
  const stream = client.query(new QueryStream(sql, [platform]));
  let ids: string[] = [];
  for await (const chunk of stream) {
    const id = String(chunk[column]);
    ids.push(prefix + id);
    ids.push(id);
    if (ids.length >= config.batchSize * 2) {
      const batch = ids;
      ids = [];
      await updateRedis(batch);
    }
  }
  await updateRedis(ids);
};

// Rebuild every platform's keys. The cache is keyed per platform because kick
// and twitch ids overlap, so each has to be drained under its own prefix.
for (const platform of PLATFORMS) {
  count = 0;
  await drainIds(
    'SELECT user_id from streamers WHERE platform = $1',
    'user_id',
    scoped(platform, Prefix.user),
    platform
  );
  logger.info({ platform, count }, 'streamer count');

  count = 0;
  await drainIds(
    'SELECT game_id from game WHERE platform = $1',
    'game_id',
    scoped(platform, Prefix.game),
    platform
  );
  logger.info({ platform, count }, 'game count');
}
count = 0;
await redis.disconnect();
await client.end();
