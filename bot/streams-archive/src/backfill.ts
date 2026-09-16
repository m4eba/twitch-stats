// One-shot job: run a single archive sweep (see Archiver.sweepCutoff) and
// exit. The streams-archive deployment does the same periodically; this is
// for draining the hot store manually. Safe to re-run.
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
  maxAgeHours: number;
  batchSize: number;
  flushBytes: number;
  keyPrefix: string;
}

const BackfillConfigOpt: ArgumentConfig<BackfillConfig> = {
  maxAgeHours: { type: Number, defaultValue: 52 },
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

const cutoff = await archiver.sweepCutoff(config.maxAgeHours);
logger.info({ cutoff }, 'backfill start');
let total = 0;
const count = await archiver.sweep(cutoff, {
  batchSize: config.batchSize,
  flushBytes: config.flushBytes,
  onFlush: (streams) => {
    total += streams;
    logger.info({ total }, 'backfill progress');
  },
});
logger.info({ total: count }, 'backfill done');
await pool.end();
