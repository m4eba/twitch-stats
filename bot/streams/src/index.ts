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

const logger: Logger = pino({ level: config.logLevel }).child({
  module: 'stream-fetcher',
});

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
  if (!Array.isArray(result.data)) {
    throw new Error('helix returned no data array for streams');
  }
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
  // Helix returns streams viewer-count descending, so once the last entry of a
  // page is below the cutoff every remaining page is too. Compare explicitly
  // against undefined: viewer_count 0 is a valid value and a very common one,
  // and a truthiness test would skip the cutoff for exactly those pages.
  const last =
    result.data.length > 0 ? result.data[result.data.length - 1] : undefined;
  if (last !== undefined && last.viewer_count < config.minViewers) {
    log.debug(
      { viewer_count: last.viewer_count },
      'below minViewers, stopping'
    );
    break;
  }
} while (cursor);
logger.debug({ count }, 'count');

// The sentinel drives end detection in streams-process, and must be emitted on
// every successful exit path - both the minViewers cutoff above and normal
// cursor exhaustion. Without it no stream is ever marked ended for this cycle.
//
// It is deliberately NOT sent when the loop throws: the sentinel marks every
// stream not seen since startTime as ended, which is only true if the sweep
// actually completed. Emitting it after a partial sweep would end every stream
// the aborted run never reached.
await producer.sendBatch({
  topicMessages: [
    {
      topic: config.topic,
      messages: [
        {
          key: 'stream',
          value: JSON.stringify({
            streams: [],
            endConfig: {
              updateStartTime: startTime,
              update: true,
            },
          } as StreamsMessage),
          timestamp: new Date().getTime().toString(),
        },
      ],
    },
  ],
});
logger.debug({ startTime }, 'sentinel sent');

await producer.disconnect();
