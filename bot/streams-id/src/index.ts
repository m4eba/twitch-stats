import {
  PaginatedResult,
  init,
  helix,
  Stream,
  StreamsMessage,
  StreamsByIdMessage,
} from '@twitch-stats/twitch';
import {
  KafkaConfig,
  TwitchConfig,
  KafkaConfigOpt,
  TwitchConfigOpt,
  defaultValues,
  FileConfig,
  FileConfigOpt,
  LogConfig,
  LogConfigOpt,
} from '@twitch-stats/config';
import pino, { Logger } from 'pino';
import { Kafka, Producer, Consumer, TopicMessages } from 'kafkajs';
import { parse, ArgumentConfig } from 'ts-command-line-args';

interface FetcherConfig {
  fromTopic: string;
  toTopic: string;
}

const FetcherConfigOpt: ArgumentConfig<FetcherConfig> = {
  fromTopic: { type: String, defaultValue: defaultValues.streamsIdTopic },
  toTopic: {
    type: String,
    defaultValue: defaultValues.streamsTopic,
  },
};

interface Config
  extends FetcherConfig,
    KafkaConfig,
    TwitchConfig,
    FileConfig,
    LogConfig {}

const config: Config = parse<Config>(
  {
    ...FetcherConfigOpt,
    ...KafkaConfigOpt,
    ...TwitchConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
  },
  {
    loadFromFileArg: 'config',
  }
);

async function query(
  logger: Logger,
  topic: string,
  ids: string[],
  producer: Producer
): Promise<void> {
  const time = new Date();

  while (ids.length > 0) {
    const messages: TopicMessages[] = [];
    const params = ids.splice(0, 100);
    logger.debug({ ids: params }, 'user ids');
    const urlParams = new URLSearchParams();
    // Helix pages with `first`, not `limit`. An unknown parameter is ignored,
    // which silently falls back to the default page size of 20 and drops the
    // remaining 80 users of every batch.
    urlParams.append('first', '100');
    for (let i = 0; i < params.length; ++i) {
      urlParams.append('user_id', params[i]);
    }

    const streams = await helix<PaginatedResult<Stream>>(
      `streams?${urlParams.toString()}`,
      null
    );
    if (!Array.isArray(streams.data)) {
      throw new Error('helix returned no data array for streams');
    }
    logger.debug({ count: streams.data.length }, 'result count');
    if (streams.data.length > 0) {
      const value: StreamsMessage = {
        streams: streams.data,
      };
      const topicMessage: TopicMessages = {
        topic,
        messages: [
          {
            key: 'stream',
            value: JSON.stringify(value),
            timestamp: time.getTime().toString(),
          },
        ],
      };
      messages.push(topicMessage);
    }
    await producer.sendBatch({ topicMessages: messages });
  }
}

await init(config);
const logger: Logger = pino({ level: config.logLevel }).child({
  module: 'streams-id',
});

const kafka: Kafka = new Kafka({
  clientId: config.kafkaClientId,
  brokers: config.kafkaBroker,
});
const producer: Producer = kafka.producer();
await producer.connect();

const consumer: Consumer = kafka.consumer({ groupId: 'streams-id' });
await consumer.connect();
await consumer.subscribe({ topic: config.fromTopic, fromBeginning: true });

await consumer.run({
  eachMessage: async ({ message }) => {
    try {
      if (!message.key || !message.value) {
        logger.error({ message }, 'no message key or value');
        return;
      }
      if (message.key.toString() === 'stream') {
        const data = JSON.parse(message.value.toString()) as StreamsByIdMessage;
        if (!Array.isArray(data.ids)) {
          logger.error({ message }, 'message has no ids array');
          return;
        }
        await query(logger, config.toTopic, data.ids, producer);
      }
    } catch (e) {
      logger.error({ error: e }, 'error in eachMessage');
      process.exit(1);
    }
  },
});

async function shutdown(): Promise<void> {
  await consumer.disconnect();
  await producer.disconnect();
  process.exit(0);
}

process.on('SIGTERM', () => {
  shutdown().catch(() => process.exit(1));
});
process.on('SIGINT', () => {
  shutdown().catch(() => process.exit(1));
});
