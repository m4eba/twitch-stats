import {
  FileConfig,
  FileConfigOpt,
  PostgresConfig,
  PostgresConfigOpt,
  LogConfig,
  LogConfigOpt,
} from '@twitch-stats/config';
import pg from 'pg';
import QueryStream from 'pg-query-stream';
import { createClient } from 'redis';
import pino, { Logger } from 'pino';
import { ArgumentConfig, parse } from 'ts-command-line-args';
import Prefix from './prefix.js';

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

const client: pg.Client = new pg.Client({
  host: config.pgHost,
  port: config.pgPort,
  database: config.pgDatabase,
  user: config.pgUser,
  password: config.pgPassword,
});
await client.connect();

const redis: ReturnType<typeof createClient> = createClient({
  url: config.redisUrl,
});
await redis.connect();

let count: number = 0;
const updateRedis = async (ids: string[]): Promise<void> => {
  logger.debug({ update: ids }, 'redis update');
  count += ids.length;
  await redis.mSet(ids);
};

const updateGames = async (): Promise<void> => {
  const promise = new Promise<void>((resolve) => {
    const query = new QueryStream('SELECT game_id from game');
    const stream = client.query(query);
    let ids: string[] = [];

    stream.on('end', async () => {
      await updateRedis(ids);
      resolve();
    });

    stream.on('data', async (chunk) => {
      ids.push(Prefix.game + chunk.game_id);
      ids.push(chunk.game_id);
      if (ids.length === config.batchSize * 2) {
        const batch = [...ids];
        ids = [];
        await updateRedis(batch);
      }
    });
  });

  return promise;
};

const updateStreamer = async (): Promise<void> => {
  const promise = new Promise<void>((resolve) => {
    const query = new QueryStream('SELECT user_id from streamers');
    const stream = client.query(query);
    let ids: string[] = [];

    stream.on('end', async () => {
      await updateRedis(ids);
      resolve();
    });

    stream.on('data', async (chunk) => {
      ids.push(Prefix.user + chunk.user_id);
      ids.push(chunk.user_id);
      if (ids.length === config.batchSize * 2) {
        const batch = [...ids];
        ids = [];
        await updateRedis(batch);
      }
    });
  });

  return promise;
};

const updateTags = async (): Promise<void> => {
  const promise = new Promise<void>((resolve) => {
    const query = new QueryStream('SELECT tag_id from tags');
    const stream = client.query(query);
    let ids: string[] = [];

    stream.on('end', async () => {
      await updateRedis(ids);
      resolve();
    });

    stream.on('data', async (chunk) => {
      ids.push(Prefix.tag + chunk.tag_id);
      ids.push(chunk.tag_id);
      if (ids.length === config.batchSize * 2) {
        const batch = [...ids];
        ids = [];
        await updateRedis(batch);
      }
    });
  });

  return promise;
};

await updateStreamer();
logger.info({ count }, 'streamer count');
count = 0;
await updateGames();
logger.info({ count }, 'game count');
count = 0;
await updateTags();
logger.info({ count }, 'tag count');
count = 0;
await redis.disconnect();
await client.end();
