import {
  FileConfig,
  FileConfigOpt,
  PostgresConfig,
  PostgresConfigOpt,
  TwitchConfig,
  TwitchConfigOpt,
  LogConfig,
  LogConfigOpt,
} from '@twitch-stats/config';
import { init } from '@twitch-stats/twitch';
import pg from 'pg';
import QueryStream from 'pg-query-stream';
import { createClient } from 'redis';
import pino, { Logger } from 'pino';
import { ArgumentConfig, parse } from 'ts-command-line-args';
import Prefix from './prefix.js';
import Missing from './missing.js';

interface UpdateConfig {
  redisUrl: string;
}

const UpdateConfigOpt: ArgumentConfig<UpdateConfig> = {
  redisUrl: { type: String },
};

interface Config
  extends UpdateConfig,
    FileConfig,
    PostgresConfig,
    TwitchConfig,
    LogConfig {}

type UpdateFn = (ids: string[]) => Promise<void>;

class Update {
  private logger: Logger;
  private ids: string[] = new Array<string>(100);
  private ids_hash: Set<string> = new Set<string>();
  private ids_length: number = 0;
  private ids_offset: number = 0;
  private missing: Missing;
  private prefix: string;
  private updateFn: UpdateFn;
  public count: number = 0;

  public constructor(
    logger: Logger,
    prefix: string,
    missing: Missing,
    updateFn: UpdateFn
  ) {
    this.logger = logger;
    this.prefix = prefix;
    this.missing = missing;
    this.updateFn = updateFn;
  }

  public async update(): Promise<void> {
    this.count += this.ids_length;
    if (this.ids_length === 100) {
      await this.updateFn(this.ids);
    } else {
      await this.updateFn(this.ids.slice(0, this.ids_length));
    }
    this.ids_offset = 0;
    this.ids_length = 0;
    this.ids = new Array<string>(100);
    this.ids_hash.clear();
  }

  public async add(id: string): Promise<void> {
    if (this.ids_hash.has(id)) return;

    this.logger.debug({ id }, 'add id');
    this.ids_hash.add(id);
    this.ids[this.ids_length] = id;
    this.ids_length++;
    await this.checkIds(false);
  }

  public async checkIds(force: boolean): Promise<void> {
    if (this.ids_length === 100 || force) {
      this.logger.debug(
        { offset: this.ids_offset, length: this.ids_length, ids: this.ids },
        'check ids'
      );
      this.logger.debug(
        { length: this.ids_length - this.ids_offset },
        'check ids length'
      );
      const checked = await this.missing.checkIds(
        this.prefix,
        this.ids.slice(this.ids_offset, this.ids_length)
      );
      this.logger.debug({ ids: checked }, 'id check result');
      this.ids.splice(
        this.ids_offset,
        this.ids.length - this.ids_offset,
        ...checked
      );
      this.ids_offset = this.ids.length;
      this.ids_length = this.ids.length;
      this.logger.debug(
        { offset: this.ids_offset, length: this.ids.length, ids: this.ids },
        'ids'
      );
    }

    if (this.ids_length === 100 || force) {
      await this.update();
    }
  }
}

const config: Config = parse<Config>(
  {
    ...UpdateConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
    ...TwitchConfigOpt,
    ...PostgresConfigOpt,
  },
  {
    loadFromFileArg: 'config',
  }
);

const logger: Logger = pino({ level: config.logLevel }).child({
  module: 'fullUpdateMissing',
});

const pool: pg.Pool = new pg.Pool({
  host: config.pgHost,
  port: config.pgPort,
  database: config.pgDatabase,
  user: config.pgUser,
  password: config.pgPassword,
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

await init(config);

const missing: Missing = new Missing(logger, pool, redis);
const updateUser: Update = new Update(
  logger.child({ module: 'user' }),
  Prefix.user,
  missing,
  missing.updateUser.bind(missing)
);
const updateGame: Update = new Update(
  logger.child({ module: 'game' }),
  Prefix.game,
  missing,
  missing.updateGame.bind(missing)
);
const updateTag: Update = new Update(
  logger.child({ module: 'tag' }),
  Prefix.tag,
  missing,
  missing.updateTag.bind(missing)
);

let rowCount: number = 0;
const query = async (): Promise<void> => {
  const promise = new Promise<void>((resolve) => {
    const query = new QueryStream('SELECT user_id,game_id,tags from stream');
    const stream = client.query(query);

    stream.on('end', async () => {
      await updateUser.checkIds(true);
      await updateGame.update();
      await updateTag.update();
      resolve();
    });

    stream.on('data', async (chunk) => {
      rowCount++;
      stream.pause();
      const tagPromises: Array<Promise<void>> = [];
      if (chunk.tags) {
        chunk.tags
          .split(',')
          .forEach((t: string) => tagPromises.push(updateTag.add(t)));
      }
      await Promise.all([
        updateUser.add(chunk.user_id),
        updateGame.add(chunk.game_id),
        tagPromises,
      ]);
      stream.resume();
    });
  });

  return promise;
};

await query();
logger.info({ count: rowCount }, 'result row count');
logger.info({ count: updateUser.count }, 'streamer count');
logger.info({ count: updateGame.count }, 'game count');
logger.info({ count: updateTag.count }, 'tag count');

await redis.disconnect();
await client.end();
await pool.end();
