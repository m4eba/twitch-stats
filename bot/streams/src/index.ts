import {
  PaginatedResult,
  init,
  helix,
  Stream,
  StreamsMessage,
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
import { Kafka, Producer, TopicMessages } from 'kafkajs';
import { parse, ArgumentConfig } from 'ts-command-line-args';

interface FetcherConfig {
  topic: string;
  minViewers: number;
}

const FetcherConfigOpt: ArgumentConfig<FetcherConfig> = {
  topic: {
    type: String,
    defaultValue: defaultValues.streamsTopic,
  },
  minViewers: { type: Number, defaultValue: 5 },
};

interface Config
  extends FetcherConfig,
    KafkaConfig,
    TwitchConfig,
    FileConfig,
    LogConfig {}

const logger: Logger = pino({ level: 'debug' }).child({
  module: 'stream-fetcher',
});

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

await init(config);

const kafka: Kafka = new Kafka({
  clientId: config.kafkaClientId,
  brokers: config.kafkaBroker,
});
const producer: Producer = kafka.producer();
await producer.connect();

let result: PaginatedResult<Stream>;
let cursor: string | undefined = undefined;
const params: { first: number; after?: string } = { first: 100 };
let count: number = 0;
let page: number = 0;

logger.info({ topics: config.topic }, 'sending to topics');

const startTime: string = new Date().toISOString();
do {
  if (cursor !== undefined) {
    params.after = cursor;
  }
  const log: Logger = logger.child({
    cursor,
    page,
  });
  page++;
  log.debug({ page, cursor }, 'get stream');
  result = await helix<PaginatedResult<Stream>>('streams', params);
  count += result.data.length;
  log.info({ result_count: result.data.length }, 'result_count');
  cursor = result.pagination.cursor;
  const time: Date = new Date();

  if (result.data.length > 0) {
    log.info(
      { viewer_count: result.data[result.data.length - 1].viewer_count },
      'viewer_count'
    );

    const messages: TopicMessages[] = [];
    const value: StreamsMessage = {
      streams: result.data,
    };
    const topicMessage: TopicMessages = {
      topic: config.topic,
      messages: [
        {
          key: 'stream',
          value: JSON.stringify(value),
          timestamp: time.getTime().toString(),
        },
      ],
    };
    messages.push(topicMessage);

    log.debug({ size: messages.length }, 'sending batch');
    await producer.sendBatch({ topicMessages: messages });
  }
  if (
    result.data &&
    result.data.length > 0 &&
    result.data[result.data.length - 1].viewer_count &&
    result.data[result.data.length - 1].viewer_count < config.minViewers
  ) {
    log.debug({}, 'last cursor');
    const messages: TopicMessages[] = [];
    const value: StreamsMessage = {
      streams: [],
      endConfig: {
        updateStartTime: startTime,
        update: true,
      },
    };

    messages.push({
      topic: config.topic,
      messages: [
        {
          key: 'stream',
          value: JSON.stringify(value),
          timestamp: time.getTime().toString(),
        },
      ],
    });

    await producer.sendBatch({ topicMessages: messages });
    break;
  }
} while (cursor);
logger.debug({ count }, 'count');

await producer.disconnect();
