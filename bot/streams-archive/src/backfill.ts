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
import type { Platform } from '@twitch-stats/twitch';
import type { Pool, QueryResult } from 'pg';
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

// Keyset pagination on (ended_at, stream_id). Rows are only removed from
// `stream` by archiver.flush(), which fires at flushBytes - so a query without
// a cursor re-selects the same batch on every iteration and appends duplicate
// documents until the buffer happens to fill.
interface BackfillRow {
  platform: Platform;
  stream_id: string;
  ended_at: Date;
}

let afterEndedAt: Date | null = null;
let afterPlatform: string | null = null;
let afterStreamId: string | null = null;

for (;;) {
  const result: QueryResult<BackfillRow> =
    afterEndedAt === null
      ? await pool.query<BackfillRow>(
          'SELECT platform, stream_id, ended_at FROM stream WHERE ended_at IS NOT NULL AND ended_at < $1 ORDER BY ended_at, platform, stream_id LIMIT $2',
          [cutoff, config.batchSize]
        )
      : await pool.query<BackfillRow>(
          `SELECT platform, stream_id, ended_at FROM stream
            WHERE ended_at IS NOT NULL AND ended_at < $1
              AND (ended_at, platform, stream_id) > ($2::timestamptz, $3::text, $4::text)
            ORDER BY ended_at, platform, stream_id LIMIT $5`,
          [cutoff, afterEndedAt, afterPlatform, afterStreamId, config.batchSize]
        );
  if (result.rows.length === 0) break;

  const lastRow = result.rows[result.rows.length - 1];
  afterEndedAt = lastRow.ended_at;
  afterPlatform = lastRow.platform;
  afterStreamId = lastRow.stream_id;

  // a batch can straddle platforms, and collect() is scoped to one
  const byPlatform = new Map<Platform, string[]>();
  for (const r of result.rows) {
    const list = byPlatform.get(r.platform);
    if (list) list.push(r.stream_id);
    else byPlatform.set(r.platform, [r.stream_id]);
  }
  for (const [platform, ids] of byPlatform) {
    total += await archiver.collect(platform, ids);
  }
  if (archiver.bufferedBytes >= config.flushBytes) {
    await archiver.flush();
    logger.info({ total }, 'backfill progress');
  }
}

await archiver.flush();
logger.info({ total }, 'backfill done');
await pool.end();
