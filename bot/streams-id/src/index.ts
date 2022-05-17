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
  const messages: TopicMessages[] = [];

  while (ids.length > 0) {
    const params = ids.splice(0, 100);
    logger.debug({ ids: params }, 'user ids');
    const urlParams = new URLSearchParams();
    urlParams.append('limit', '100');
    for (let i = 0; i < params.length; ++i) {
      urlParams.append('user_id', params[i]);
    }

    const streams = await helix<PaginatedResult<Stream>>(
      `streams?${urlParams.toString()}`,
      null
    );
    if (streams.data) {
      logger.debug({ count: streams.data.length }, 'result count');
    }
    if (streams.data && streams.data.length > 0) {
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
  }

  await producer.sendBatch({ topicMessages: messages });
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
    if (!message.key || !message.value) {
      logger.error({ message }, 'no message key or value');
      return;
    }
    if (message.key.toString() === 'stream') {
      const data = JSON.parse(message.value.toString()) as StreamsByIdMessage;
      await query(logger, config.toTopic, data.ids, producer);
    }
  },
});
