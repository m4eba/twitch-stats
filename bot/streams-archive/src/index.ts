import {
  KafkaConfig,
  KafkaConfigOpt,
  PostgresConfig,
  PostgresConfigOpt,
  S3Config,
  S3ConfigOpt,
  FileConfig,
  FileConfigOpt,
  LogConfig,
  LogConfigOpt,
  defaultValues,
} from '@twitch-stats/config';
import type { StreamEndedMessage } from '@twitch-stats/twitch';
import { initPostgres } from '@twitch-stats/database';
import { initS3 } from '@twitch-stats/storage';
import type { Pool } from 'pg';
import pino, { Logger } from 'pino';
import { Kafka, Consumer } from 'kafkajs';
import { ArgumentConfig, parse } from 'ts-command-line-args';
import { Archiver } from './archiver.js';

interface ArchiveConfig {
  streamEndedTopic: string;
  graceSeconds: number;
  flushIntervalSeconds: number;
  flushBytes: number;
  keyPrefix: string;
}

const ArchiveConfigOpt: ArgumentConfig<ArchiveConfig> = {
  streamEndedTopic: {
    type: String,
    defaultValue: defaultValues.streamEndedTopic,
  },
  graceSeconds: { type: Number, defaultValue: 60 * 60 },
  flushIntervalSeconds: { type: Number, defaultValue: 15 * 60 },
  flushBytes: { type: Number, defaultValue: 64 * 1024 * 1024 },
  keyPrefix: { type: String, defaultValue: 'archive/' },
};

interface Config
  extends ArchiveConfig,
    KafkaConfig,
    PostgresConfig,
    S3Config,
    FileConfig,
    LogConfig {}

const config: Config = parse<Config>(
  {
    ...KafkaConfigOpt,
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

logger.info({ topic: config.streamEndedTopic }, 'starting');
const pool: Pool = await initPostgres(config);
const s3 = initS3(config);
const archiver: Archiver = new Archiver(
  logger,
  pool,
  s3,
  config.s3Bucket,
  config.keyPrefix
);

const kafka: Kafka = new Kafka({
  clientId: config.kafkaClientId,
  brokers: config.kafkaBroker,
});

const consumer: Consumer = kafka.consumer({ groupId: 'streams-archive' });
await consumer.connect();
logger.info('kafka connected');
await consumer.subscribe({
  topic: config.streamEndedTopic,
  fromBeginning: true,
});
logger.info('subscribed');

// offsets of buffered-but-not-flushed messages, committed after a
// successful flush (kafka is the write-ahead log)
const pendingOffsets: Map<number, string> = new Map();

// eachMessage and the flush timer must not interleave
let lock: Promise<void> = Promise.resolve();
function withLock(fn: () => Promise<void>): Promise<void> {
  const run = lock.then(fn);
  lock = run.catch(() => undefined);
  return run;
}

async function flushAndCommit(): Promise<void> {
  const count = await archiver.flush();
  if (pendingOffsets.size > 0) {
    await consumer.commitOffsets(
      [...pendingOffsets.entries()].map(([partition, offset]) => ({
        topic: config.streamEndedTopic,
        partition,
        offset: (BigInt(offset) + 1n).toString(),
      }))
    );
    pendingOffsets.clear();
  }
  if (count > 0) {
    logger.info({ streams: count }, 'flushed');
  }
}

function maybeFlush(): Promise<void> {
  if (archiver.bufferedCount === 0) return Promise.resolve();
  if (
    archiver.bufferedBytes >= config.flushBytes ||
    archiver.bufferAgeMs >= config.flushIntervalSeconds * 1000
  ) {
    return flushAndCommit();
  }
  return Promise.resolve();
}

const flushTimer = setInterval(() => {
  withLock(maybeFlush).catch((e) => {
    logger.error({ error: e }, 'flush failed');
    process.exit(1);
  });
}, 30 * 1000);

await consumer.run({
  autoCommit: false,
  eachMessage: async ({ topic, partition, message }) => {
    try {
      if (!message.value) return;

      // wait out the grace period so a stream that briefly drops off and
      // comes back is not archived mid-stream
      const readyAt =
        parseInt(message.timestamp) + config.graceSeconds * 1000;
      const wait = readyAt - Date.now();
      if (wait > 0) {
        consumer.pause([{ topic, partitions: [partition] }]);
        consumer.seek({ topic, partition, offset: message.offset });
        setTimeout(() => {
          consumer.resume([{ topic, partitions: [partition] }]);
        }, Math.min(wait, 5 * 60 * 1000));
        return;
      }

      const msg = JSON.parse(message.value.toString()) as StreamEndedMessage;
      await withLock(async () => {
        const count = await archiver.collect(
          msg.streams.map((s) => s.stream_id)
        );
        logger.debug(
          { received: msg.streams.length, collected: count },
          'message processed'
        );
        pendingOffsets.set(partition, message.offset);
        await maybeFlush();
      });
    } catch (e) {
      logger.error({ error: e }, 'error in eachMessage');
      process.exit(1);
    }
  },
});

async function shutdown(): Promise<void> {
  clearInterval(flushTimer);
  try {
    await withLock(flushAndCommit);
  } catch (e) {
    logger.error({ error: e }, 'flush on shutdown failed');
  }
  await consumer.disconnect();
  await pool.end();
  process.exit(0);
}

process.on('SIGTERM', () => {
  shutdown().catch(() => process.exit(1));
});
process.on('SIGINT', () => {
  shutdown().catch(() => process.exit(1));
});
