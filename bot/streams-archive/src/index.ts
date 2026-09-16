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
import { metrics, startMetricsServer } from '@twitch-stats/utils';
import type { Pool } from 'pg';
import pino, { Logger } from 'pino';
import { ArgumentConfig, parse } from 'ts-command-line-args';
import { Archiver } from './archiver.js';

interface ArchiveConfig {
  maxAgeHours: number;
  sweepIntervalSeconds: number;
  batchSize: number;
  flushBytes: number;
  keyPrefix: string;
  metricsPort: number;
}

const ArchiveConfigOpt: ArgumentConfig<ArchiveConfig> = {
  // Twitch's 48h broadcast limit plus margin for end detection, which puts
  // ended_at up to one crawl interval + 5 minutes past the last probe
  maxAgeHours: { type: Number, defaultValue: 52 },
  sweepIntervalSeconds: { type: Number, defaultValue: 15 * 60 },
  batchSize: { type: Number, defaultValue: 2000 },
  flushBytes: { type: Number, defaultValue: 64 * 1024 * 1024 },
  keyPrefix: { type: String, defaultValue: 'archive/' },
  metricsPort: { type: Number, defaultValue: 9090 },
};

interface Config
  extends ArchiveConfig,
    PostgresConfig,
    S3Config,
    FileConfig,
    LogConfig {}

const config: Config = parse<Config>(
  {
    ...ArchiveConfigOpt,
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
  module: 'streams-archive',
});

logger.info({ maxAgeHours: config.maxAgeHours }, 'starting');
const pool: Pool = await initPostgres(config);
const s3 = initS3(config);
const archiver: Archiver = new Archiver(
  logger,
  pool,
  s3,
  config.s3Bucket,
  config.keyPrefix
);

startMetricsServer(config.metricsPort);
const chunkBytes = new metrics.Counter({
  name: 'twstats_chunk_uploaded_bytes_total',
  help: 'gzipped bytes uploaded to object storage',
  labelNames: ['type'],
});
const chunkUploads = new metrics.Counter({
  name: 'twstats_chunk_uploads_total',
  help: 'chunk objects uploaded to object storage',
  labelNames: ['type'],
});
const streamsArchived = new metrics.Counter({
  name: 'twstats_streams_archived_total',
  help: 'streams archived to object storage',
});
new metrics.Gauge({
  name: 'twstats_archive_buffer_bytes',
  help: 'compressed bytes waiting in the archive buffer',
  collect() {
    this.set(archiver.bufferedBytes);
  },
});
new metrics.Gauge({
  name: 'twstats_archive_buffer_age_seconds',
  help: 'age of the oldest document in the archive buffer',
  collect() {
    this.set(archiver.bufferAgeMs / 1000);
  },
});
const lastSweep = new metrics.Gauge({
  name: 'twstats_archive_last_sweep_timestamp_seconds',
  help: 'unix time of the last completed archive sweep',
});
// Anything left in `stream` holds back partition retention in maintenance,
// which can only log about it; alert on this instead. Expected to stay near
// maxAgeHours while sweeps succeed.
new metrics.Gauge({
  name: 'twstats_stream_oldest_started_seconds',
  help: 'age of the oldest started_at in the stream table',
  async collect() {
    try {
      const result = await pool.query<{ age: number | null }>(
        'SELECT extract(epoch FROM now() - min(started_at))::float8 AS age FROM stream'
      );
      this.set(result.rows[0].age ?? 0);
    } catch (e) {
      logger.error({ error: e }, 'oldest stream query failed');
    }
  },
});

let stopping = false;
let wake: (() => void) | null = null;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    wake = () => {
      clearTimeout(timer);
      resolve();
    };
  });
}

async function runSweep(): Promise<void> {
  const cutoff = await archiver.sweepCutoff(config.maxAgeHours);
  const count = await archiver.sweep(cutoff, {
    batchSize: config.batchSize,
    flushBytes: config.flushBytes,
    shouldStop: () => stopping,
    onFlush: (streams, bytes) => {
      chunkBytes.labels('archive').inc(bytes);
      chunkUploads.labels('archive').inc();
      streamsArchived.inc(streams);
      logger.info({ streams }, 'flushed');
    },
  });
  lastSweep.setToCurrentTime();
  logger.info({ cutoff, streams: count }, 'sweep done');
}

async function loop(): Promise<void> {
  while (!stopping) {
    try {
      await runSweep();
    } catch (e) {
      // nothing is lost: rows leave `stream` only in the flush transaction
      logger.error({ error: e }, 'sweep failed');
      process.exit(1);
    }
    if (!stopping) await sleep(config.sweepIntervalSeconds * 1000);
  }
}

const running = loop();

async function shutdown(): Promise<void> {
  stopping = true;
  wake?.();
  // the sweep stops after its current batch and flushes what it collected
  await running;
  await pool.end();
  process.exit(0);
}

process.on('SIGTERM', () => {
  shutdown().catch(() => process.exit(1));
});
process.on('SIGINT', () => {
  shutdown().catch(() => process.exit(1));
});
