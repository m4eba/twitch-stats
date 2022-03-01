import { init, helix, } from '@twitch-stats/twitch';
import { KafkaConfigOpt, TwitchConfigOpt, defaultValues, FileConfigOpt, LogConfigOpt, } from '@twitch-stats/config';
import pino from 'pino';
import { Kafka } from 'kafkajs';
import { parse } from 'ts-command-line-args';
const FetcherConfigOpt = {
    topic: {
        type: String,
        multiple: true,
        defaultValue: [defaultValues.streamsTopic],
    },
    minViewers: { type: Number, defaultValue: 5 },
};
const logger = pino({ level: 'debug' }).child({
    module: 'stream-fetcher',
});
const config = parse({
    ...FetcherConfigOpt,
    ...KafkaConfigOpt,
    ...TwitchConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
}, {
    loadFromFileArg: 'config',
});
await init(config);
const kafka = new Kafka({
    clientId: config.kafkaClientId,
    brokers: config.kafkaBroker,
});
const producer = kafka.producer();
await producer.connect();
let result;
let cursor = undefined;
const params = { first: 100 };
let count = 0;
let page = 0;
logger.info({ topics: config.topic }, 'sending to topics');
const startTime = new Date().toISOString();
do {
    if (cursor !== undefined) {
        params.after = cursor;
    }
    const log = logger.child({
        cursor,
        page,
    });
    page++;
    log.debug({ page, cursor }, 'get stream');
    result = await helix('streams', params);
    count += result.data.length;
    log.info({ result_count: result.data.length }, 'result_count');
    cursor = result.pagination.cursor;
    const time = new Date();
    if (result.data.length > 0) {
        log.info({ viewer_count: result.data[result.data.length - 1].viewer_count }, 'viewer_count');
        const messages = [];
        for (let i = 0; i < config.topic.length; ++i) {
            const value = {
                streams: result.data,
            };
            const topicMessage = {
                topic: config.topic[i],
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
        log.debug({ size: messages.length }, 'sending batch');
        await producer.sendBatch({ topicMessages: messages });
    }
    if (result.data &&
        result.data.length > 0 &&
        result.data[result.data.length - 1].viewer_count &&
        result.data[result.data.length - 1].viewer_count < config.minViewers) {
        log.debug({}, 'last cursor');
        const messages = [];
        const value = {
            streams: [],
            endConfig: {
                updateStartTime: startTime,
                update: true,
            },
        };
        for (let i = 0; i < config.topic.length; ++i) {
            messages.push({
                topic: config.topic[i],
                messages: [
                    {
                        key: 'stream',
                        value: JSON.stringify(value),
                        timestamp: time.getTime().toString(),
                    },
                ],
            });
        }
        await producer.sendBatch({ topicMessages: messages });
        break;
    }
} while (cursor);
logger.debug({ count }, 'count');
await producer.disconnect();
