import {
  KafkaConfig,
  KafkaConfigOpt,
  S3Config,
  S3ConfigOpt,
  defaultValues,
  FileConfig,
  FileConfigOpt,
  LogConfig,
  LogConfigOpt,
} from '@twitch-stats/config';
import { ChunkBuffer, initS3, chunkKey, putChunk } from '@twitch-stats/storage';
import { metrics, startMetricsServer } from '@twitch-stats/utils';
import pino, { Logger } from 'pino';
import { Kafka, Consumer } from 'kafkajs';
import { ArgumentConfig, parse } from 'ts-command-line-args';

interface WriterConfig {
  topic: string;
  keyPrefix: string;
  flushIntervalSeconds: number;
  flushBytes: number;
  metricsPort: number;
}

const WriterConfigOpt: ArgumentConfig<WriterConfig> = {
  topic: { type: String, defaultValue: defaultValues.streamsTopic },
  keyPrefix: { type: String, defaultValue: 'raw/' },
  flushIntervalSeconds: { type: Number, defaultValue: 5 * 60 },
  flushBytes: { type: Number, defaultValue: 32 * 1024 * 1024 },
  metricsPort: { type: Number, defaultValue: 9090 },
};

interface Config
  extends WriterConfig,
    KafkaConfig,
    S3Config,
    FileConfig,
    LogConfig {}

const config: Config = parse<Config>(
  {
    ...KafkaConfigOpt,
    ...WriterConfigOpt,
    ...S3ConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
  },
  {
    loadFromFileArg: 'config',
  }
);

const logger: Logger = pino({ level: config.logLevel }).child({
  module: 'log-writer',
});

const s3 = initS3(config);
const buffer: ChunkBuffer = new ChunkBuffer();

startMetricsServer(config.metricsPort);
const chunkBytes = new metrics.Counter({
  name: 'twstats_chunk_uploaded_bytes_total',
  help: 'gzipped bytes uploaded to object storage',
  labelNames: ['type'],
});
const chunkMessages = new metrics.Counter({
  name: 'twstats_chunk_messages_total',
  help: 'messages contained in uploaded chunks',
  labelNames: ['type'],
});
const chunkUploads = new metrics.Counter({
  name: 'twstats_chunk_uploads_total',
  help: 'chunk objects uploaded to object storage',
  labelNames: ['type'],
});

const kafka: Kafka = new Kafka({
  clientId: config.kafkaClientId,
  brokers: config.kafkaBroker,
});
logger.info({ topic: config.topic }, 'subscribe');
const consumer: Consumer = kafka.consumer({ groupId: 'stream-log' });
await consumer.connect();
await consumer.subscribe({ topic: config.topic, fromBeginning: true });

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

// Kafka timestamp of the first message in the current buffer. Chunks are keyed
// and day-bounded by message time rather than wall clock: during replay or
// backlog catch-up the two diverge by days, and raw/YYYY/MM/DD/ is meant to say
// when the data was produced, not when we happened to write it out.
let bufferFirstMs: number | null = null;

function utcDay(ms: number): string {
  return new Date(ms).toISOString().substring(0, 10);
}

async function flushAndCommit(): Promise<void> {
  if (buffer.count === 0) return;
  const key = chunkKey(config.keyPrefix, new Date(bufferFirstMs ?? Date.now()));
  const bytes = buffer.byteLength;
  const count = buffer.count;
  await putChunk(s3, config.s3Bucket, key, buffer.concat());
  buffer.reset();
  bufferFirstMs = null;
  if (pendingOffsets.size > 0) {
    await consumer.commitOffsets(
      [...pendingOffsets.entries()].map(([partition, offset]) => ({
        topic: config.topic,
        partition,
        offset: (BigInt(offset) + 1n).toString(),
      }))
    );
    pendingOffsets.clear();
  }
  chunkBytes.labels('raw').inc(bytes);
  chunkMessages.labels('raw').inc(count);
  chunkUploads.labels('raw').inc();
  logger.info({ key, messages: count, bytes }, 'chunk uploaded');
}

function maybeFlush(): Promise<void> {
  if (buffer.count === 0) return Promise.resolve();
  if (
    buffer.byteLength >= config.flushBytes ||
    buffer.ageMs >= config.flushIntervalSeconds * 1000
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
}, 15 * 1000);

await consumer.run({
  autoCommit: false,
  eachMessage: async ({ partition, message }) => {
    try {
      const raw = message.value?.toString();
      // A zero-length value is truthy as a Buffer but splices in as nothing,
      // producing `{"time":"...","data":}` - unparseable, and it poisons the
      // whole replay. Still record the offset, otherwise a partition of only
      // skipped messages never commits and is re-read on every restart.
      if (!raw) {
        await withLock(async () => {
          pendingOffsets.set(partition, message.offset);
        });
        return;
      }
      const parsedTs = Number(message.timestamp);
      const ts = Number.isFinite(parsedTs) ? parsedTs : Date.now();
      await withLock(async () => {
        // Never let one chunk span two UTC days, so the day prefix is exact by
        // construction and the key can name the day rather than approximate it.
        if (bufferFirstMs !== null && utcDay(ts) !== utcDay(bufferFirstMs)) {
          await flushAndCommit();
        }
        await buffer.add(`{"time":"${message.timestamp}","data":${raw}}`);
        if (bufferFirstMs === null) bufferFirstMs = ts;
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
  process.exit(0);
}

process.on('SIGTERM', () => {
  shutdown().catch(() => process.exit(1));
});
process.on('SIGINT', () => {
  shutdown().catch(() => process.exit(1));
});
