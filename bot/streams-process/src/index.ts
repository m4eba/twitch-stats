import {
  KafkaConfig,
  TwitchConfig,
  PostgresConfig,
  KafkaConfigOpt,
  defaultValues,
  TwitchConfigOpt,
  PostgresConfigOpt,
  FileConfig,
  FileConfigOpt,
  LogConfig,
  LogConfigOpt,
} from '@twitch-stats/config';
import { init, StreamsMessage } from '@twitch-stats/twitch';
import { initPostgres } from '@twitch-stats/database';
import type { Pool } from 'pg';
import pino, { Logger } from 'pino';
import { Kafka, Consumer, Producer } from 'kafkajs';
import { ArgumentConfig, parse } from 'ts-command-line-args';
import Processing from './processing.js';

interface TopicConfig {
  topic: string;
  streamIdTopic: string;
}

const TopicConfigOpt: ArgumentConfig<TopicConfig> = {
  topic: { type: String, defaultValue: defaultValues.streamsTopic },
  streamIdTopic: { type: String, defaultValue: defaultValues.streamsIdTopic },
};

interface Config
  extends KafkaConfig,
    TwitchConfig,
    PostgresConfig,
    FileConfig,
    LogConfig,
    TopicConfig {}

const config: Config = parse<Config>(
  {
    ...KafkaConfigOpt,
    ...TwitchConfigOpt,
    ...TopicConfigOpt,
    ...PostgresConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
  },
  {
    loadFromFileArg: 'config',
  }
);

const logger: Logger = pino({ level: config.logLevel }).child({
  module: 'streams-process',
});
await init(config);

const pool: Pool = await initPostgres(config);

const kafka: Kafka = new Kafka({
  clientId: config.kafkaClientId,
  brokers: config.kafkaBroker,
});

const consumer: Consumer = kafka.consumer({ groupId: 'stream-process' });
await consumer.connect();
await consumer.subscribe({ topic: config.topic, fromBeginning: true });
const producer: Producer = kafka.producer();
await producer.connect();
const processing: Processing = new Processing(
  logger,
  pool,
  producer,
  config.streamIdTopic
);

await consumer.run({
  eachMessage: async ({ message }) => {
    try {
      logger.trace({ message }, 'message received');
      if (!message.value) {
        logger.error({ message }, 'no message value');
        return;
      }
      if (!message.timestamp) {
        logger.error({ message }, 'message has no timestamp');
        return;
      }
      const d = new Date(parseInt(message.timestamp));
      const msg = JSON.parse(message.value.toString()) as StreamsMessage;

      await processing.processStreams(d, msg.streams);
      if (msg.endConfig) {
        await processing.processEnd(msg.endConfig);
      }

      logger.flush();
    } catch (e) {
      logger.error({ error: e }, 'error in eachMessage');
      console.log(e);
      logger.flush();
      process.exit();
    }
  },
});
