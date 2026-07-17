// One-shot job: archive all streams that ended more than graceSeconds ago,
// draining historical data out of the hot store. Safe to re-run; archived
// streams are deleted from the stream table as they are flushed.
import {
  PostgresConfig,
  PostgresConfigOpt,
  S3Config,
  S3ConfigOpt,
  FileConfig,
  FileConfigOpt,
  LogConfig,
  LogConfigOpt,
} from '@twitch-stats/config';
import { initPostgres } from '@twitch-stats/database';
import { initS3 } from '@twitch-stats/storage';
import type { Pool } from 'pg';
import pino, { Logger } from 'pino';
import { ArgumentConfig, parse } from 'ts-command-line-args';
import { Archiver } from './archiver.js';

interface BackfillConfig {
  graceSeconds: number;
  batchSize: number;
  flushBytes: number;
  keyPrefix: string;
}

const BackfillConfigOpt: ArgumentConfig<BackfillConfig> = {
  graceSeconds: { type: Number, defaultValue: 60 * 60 },
  batchSize: { type: Number, defaultValue: 2000 },
  flushBytes: { type: Number, defaultValue: 64 * 1024 * 1024 },
  keyPrefix: { type: String, defaultValue: 'archive/' },
};

interface Config
  extends BackfillConfig,
    PostgresConfig,
    S3Config,
    FileConfig,
    LogConfig {}

const config: Config = parse<Config>(
  {
    ...BackfillConfigOpt,
    ...PostgresConfigOpt,
    ...S3ConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
  },
  {
    loadFromFileArg: 'config',
  }
);

const logger: Logger = pino({ level: config.logLevel }).child({
  module: 'streams-archive-backfill',
});

const pool: Pool = await initPostgres(config);
const s3 = initS3(config);
const archiver: Archiver = new Archiver(
  logger,
  pool,
  s3,
  config.s3Bucket,
  config.keyPrefix
);

const cutoff = new Date(Date.now() - config.graceSeconds * 1000);
let total = 0;

for (;;) {
  const result = await pool.query(
    'SELECT stream_id FROM stream WHERE ended_at IS NOT NULL AND ended_at < $1 ORDER BY ended_at LIMIT $2',
    [cutoff, config.batchSize]
  );
  if (result.rows.length === 0) break;

  total += await archiver.collect(result.rows.map((r) => r.stream_id));
  if (archiver.bufferedBytes >= config.flushBytes) {
    await archiver.flush();
    logger.info({ total }, 'backfill progress');
  }
}

await archiver.flush();
logger.info({ total }, 'backfill done');
await pool.end();
