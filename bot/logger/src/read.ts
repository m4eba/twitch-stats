import type { StreamsMessage } from '@twitch-stats/twitch';
import {
  KafkaConfig,
  KafkaConfigOpt,
  defaultValues,
  FileConfig,
  FileConfigOpt,
} from '@twitch-stats/config';
import pino, { Logger } from 'pino';
import fs from 'node:fs';
import zlib from 'node:zlib';
import type { Readable } from 'node:stream';
import { Kafka, Producer } from 'kafkajs';
import { ArgumentConfig, parse } from 'ts-command-line-args';
import * as readline from 'node:readline';

interface LogData {
  time: string;
  data: StreamsMessage;
}

interface ReaderConfig {
  topic: string;
  filename: string;
}

const ReaderConfigOpt: ArgumentConfig<ReaderConfig> = {
  topic: { type: String, defaultValue: defaultValues.streamsTopic },
  filename: { type: String },
};

interface Config extends ReaderConfig, KafkaConfig, FileConfig {}

const logger: Logger = pino({ level: 'debug' }).child({ module: 'log-reader' });

const config: Config = parse<Config>(
  {
    ...KafkaConfigOpt,
    ...ReaderConfigOpt,
    ...FileConfigOpt,
  },
  {
    loadFromFileArg: 'config',
  }
);

const kafka: Kafka = new Kafka({
  clientId: config.kafkaClientId,
  brokers: config.kafkaBroker,
});
logger.info({ topic: config.topic }, 'topic');
const producer: Producer = kafka.producer();
await producer.connect();

let instream: Readable = fs.createReadStream(config.filename);
if (config.filename.endsWith('.gz')) {
  instream = instream.pipe(zlib.createGunzip());
}

const rl: readline.Interface = readline.createInterface({
  input: instream,
  crlfDelay: Infinity,
});

let replayed = 0;
let skipped = 0;

for await (const line of rl) {
  if (line.trim().length === 0) continue;
  // One malformed line must not abort a replay that has already produced
  // thousands of messages with no record of how far it got - re-running would
  // duplicate everything up to the failure. Skip and keep going.
  let logData: LogData;
  try {
    logData = JSON.parse(line) as LogData;
    if (!logData || typeof logData.data !== 'object' || logData.data === null) {
      throw new Error('line has no data object');
    }
  } catch (e) {
    ++skipped;
    logger.warn({ error: e, line: line.slice(0, 200) }, 'skipping bad line');
    continue;
  }
  if (logData.data.endConfig) {
    logData.data.endConfig.update = false;
  }
  await producer.send({
    topic: config.topic,
    messages: [
      {
        key: 'stream',
        value: JSON.stringify(logData.data),
        timestamp: logData.time,
      },
    ],
  });
  ++replayed;
}

logger.info({ replayed, skipped }, 'replay done');

await producer.disconnect();
