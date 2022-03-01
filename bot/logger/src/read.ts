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

const instream: fs.ReadStream = fs.createReadStream(config.filename, {
  encoding: 'utf-8',
});

const rl: readline.Interface = readline.createInterface({
  input: instream,
  crlfDelay: Infinity,
});

for await (const line of rl) {
  const logData: LogData = JSON.parse(line) as LogData;
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
}

await producer.disconnect();
