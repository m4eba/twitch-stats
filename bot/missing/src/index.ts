import {
  KafkaConfig,
  KafkaConfigOpt,
  FileConfig,
  FileConfigOpt,
  PostgresConfig,
  PostgresConfigOpt,
  TwitchConfig,
  TwitchConfigOpt,
  LogConfig,
  LogConfigOpt,
  defaultValues,
} from '@twitch-stats/config';
import { init, StreamsMessage } from '@twitch-stats/twitch';
import type { Pool } from 'pg';
import { initPostgres } from '@twitch-stats/database';
import { createClient } from 'redis';
import pino, { Logger } from 'pino';
import { Kafka, Consumer } from 'kafkajs';
import { ArgumentConfig, parse } from 'ts-command-line-args';
import Missing from './missing.js';

interface MissingConfig {
  topic: string;
  redisUrl: string;
}

const MissingConfigOpt: ArgumentConfig<MissingConfig> = {
  topic: { type: String, defaultValue: defaultValues.streamsTopic },
  redisUrl: { type: String },
};

interface Config
  extends MissingConfig,
    KafkaConfig,
    FileConfig,
    PostgresConfig,
    TwitchConfig,
    LogConfig {}

const config: Config = parse<Config>(
  {
    ...KafkaConfigOpt,
    ...MissingConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
    ...PostgresConfigOpt,
    ...TwitchConfigOpt,
  },
  {
    loadFromFileArg: 'config',
  }
);

const logger: Logger = pino({ level: config.logLevel }).child({
  module: 'missing',
});

const pool: Pool = await initPostgres(config);

const kafka: Kafka = new Kafka({
  clientId: config.kafkaClientId,
  brokers: config.kafkaBroker,
});
const client: ReturnType<typeof createClient> = createClient({
  url: config.redisUrl,
});
await client.connect();

logger.info({ topic: config.topic }, 'subscribe');
const consumer: Consumer = kafka.consumer({ groupId: 'stream-missing' });
await consumer.connect();
await consumer.subscribe({ topic: config.topic, fromBeginning: true });

await init(config);

const missing: Missing = new Missing(logger, pool, client);
await missing.initRedis();

await consumer.run({
  eachMessage: async ({ message }) => {
    if (message.value) {
      logger.debug(
        {
          message: JSON.parse(message.value.toString()),
        },
        'msg received'
      );
      const msg: StreamsMessage = JSON.parse(
        message.value.toString()
      ) as StreamsMessage;
      await missing.update(msg.streams);
    }
  },
});
