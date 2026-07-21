import {
  KafkaConfig,
  KafkaConfigOpt,
  KickConfig,
  KickConfigOpt,
  defaultValues,
  FileConfig,
  FileConfigOpt,
  LogConfig,
  LogConfigOpt,
} from '@twitch-stats/config';
import {
  init,
  getLivestreams,
  LIVESTREAMS_MAX_LIMIT,
} from '@twitch-stats/kick';
import type { KickLivestream } from '@twitch-stats/kick';
import type { Stream, StreamsMessage } from '@twitch-stats/twitch';
import pino, { Logger } from 'pino';
import { Kafka, Producer, TopicMessages } from 'kafkajs';
import { ArgumentConfig, parse } from 'ts-command-line-args';

interface FetcherConfig {
  topic: string;
  minViewers: number;
  pageLimit: number;
}

const FetcherConfigOpt: ArgumentConfig<FetcherConfig> = {
  topic: { type: String, defaultValue: defaultValues.streamsTopic },
  // Kick reports viewer_count 0 both for an empty stream and for a broadcaster
  // who opted out of sharing it, so a cutoff above 0 silently discards real
  // streams. Kick is also far smaller than Twitch, so keeping everything is
  // affordable. Twitch's equivalent default is 5.
  minViewers: { type: Number, defaultValue: 0 },
  pageLimit: { type: Number, defaultValue: LIVESTREAMS_MAX_LIMIT },
};

interface Config
  extends FetcherConfig,
    KafkaConfig,
    KickConfig,
    FileConfig,
    LogConfig {}

const config: Config = parse<Config>(
  {
    ...FetcherConfigOpt,
    ...KafkaConfigOpt,
    ...KickConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
  },
  {
    loadFromFileArg: 'config',
  }
);

const logger: Logger = pino({ level: config.logLevel }).child({
  module: 'kick-stream-fetcher',
});

// Map a Kick livestream onto the pipeline's normalized stream shape. The field
// set lines up with Twitch's almost exactly; the only real differences are that
// the livestream id is a UUID rather than a numeric id (hence the text
// stream_id column) and that the category may be absent.
function toStream(l: KickLivestream): Stream | null {
  const started = Date.parse(l.started_at);
  if (Number.isNaN(started)) {
    logger.warn(
      { id: l.id, started_at: l.started_at },
      'unparseable started_at, skipping'
    );
    return null;
  }
  return {
    id: l.id,
    user_id: String(l.broadcaster_user.id),
    user_name: l.broadcaster_user.username,
    game_id: l.category ? String(l.category.id) : '0',
    game_name: l.category ? l.category.name : '',
    type: 'live',
    title: l.title,
    viewer_count: l.viewer_count,
    started_at: new Date(started).toISOString(),
    language: l.language_code,
    thumbnail_url: l.thumbnail,
    tags: l.tags ?? [],
  };
}

await init(config);

const kafka: Kafka = new Kafka({
  clientId: config.kafkaClientId,
  brokers: config.kafkaBroker,
});
const producer: Producer = kafka.producer();
await producer.connect();

logger.info({ topic: config.topic }, 'sending to topic');

const startTime: string = new Date().toISOString();
let cursor: string | undefined = undefined;
let page = 0;
let count = 0;
let skipped = 0;

// Unlike Helix, /public/v2/livestreams is sorted oldest-to-newest by
// started_at, not by viewer count. There is therefore no point at which the
// remaining pages are guaranteed to be below the cutoff, so the whole platform
// is paged and minViewers is applied per stream. The 1000/page limit keeps that
// affordable - Helix caps at 100.
for (;;) {
  const log: Logger = logger.child({ page, cursor });
  page++;
  const result = await getLivestreams({ limit: config.pageLimit, cursor });
  if (!Array.isArray(result.data)) {
    throw new Error('kick returned no data array for livestreams');
  }
  log.info({ result_count: result.data.length }, 'result_count');

  const streams: Stream[] = [];
  for (const l of result.data) {
    const s = toStream(l);
    if (s === null) {
      skipped++;
      continue;
    }
    if (s.viewer_count < config.minViewers) continue;
    streams.push(s);
  }
  count += streams.length;

  if (streams.length > 0) {
    const time: Date = new Date();
    const value: StreamsMessage = { platform: 'kick', streams };
    const messages: TopicMessages[] = [
      {
        topic: config.topic,
        messages: [
          {
            key: 'stream',
            value: JSON.stringify(value),
            timestamp: time.getTime().toString(),
          },
        ],
      },
    ];
    log.debug({ size: streams.length }, 'sending batch');
    await producer.sendBatch({ topicMessages: messages });
  }

  cursor = result.pagination?.next_cursor || undefined;
  if (cursor === undefined) break;
}

logger.debug({ count, skipped, pages: page }, 'count');

// End detection sentinel. Emitted after the loop so it covers every exit path,
// but deliberately not when the loop throws: it marks every kick stream not
// seen since startTime as ended, which is only true if the sweep completed.
// The platform field scopes that sweep - without it streams-process would end
// every live Twitch stream too.
await producer.sendBatch({
  topicMessages: [
    {
      topic: config.topic,
      messages: [
        {
          key: 'stream',
          value: JSON.stringify({
            platform: 'kick',
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
