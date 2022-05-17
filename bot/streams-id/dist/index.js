import { init, helix, } from '@twitch-stats/twitch';
import { KafkaConfigOpt, TwitchConfigOpt, defaultValues, FileConfigOpt, LogConfigOpt, } from '@twitch-stats/config';
import pino from 'pino';
import { Kafka } from 'kafkajs';
import { parse } from 'ts-command-line-args';
const FetcherConfigOpt = {
    fromTopic: { type: String, defaultValue: defaultValues.streamsIdTopic },
    toTopic: {
        type: String,
        defaultValue: defaultValues.streamsTopic,
    },
};
const config = parse({
    ...FetcherConfigOpt,
    ...KafkaConfigOpt,
    ...TwitchConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
}, {
    loadFromFileArg: 'config',
});
async function query(logger, topic, ids, producer) {
    const time = new Date();
    const messages = [];
    while (ids.length > 0) {
        const params = ids.splice(0, 100);
        logger.debug({ ids: params }, 'user ids');
        const urlParams = new URLSearchParams();
        urlParams.append('limit', '100');
        for (let i = 0; i < params.length; ++i) {
            urlParams.append('user_id', params[i]);
        }
        const streams = await helix(`streams?${urlParams.toString()}`, null);
        if (streams.data) {
            logger.debug({ count: streams.data.length }, 'result count');
        }
        if (streams.data && streams.data.length > 0) {
            const value = {
                streams: streams.data,
            };
            const topicMessage = {
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
const logger = pino({ level: config.logLevel }).child({
    module: 'streams-id',
});
const kafka = new Kafka({
    clientId: config.kafkaClientId,
    brokers: config.kafkaBroker,
});
const producer = kafka.producer();
await producer.connect();
const consumer = kafka.consumer({ groupId: 'streams-id' });
await consumer.connect();
await consumer.subscribe({ topic: config.fromTopic, fromBeginning: true });
await consumer.run({
    eachMessage: async ({ message }) => {
        if (!message.key || !message.value) {
            logger.error({ message }, 'no message key or value');
            return;
        }
        if (message.key.toString() === 'stream') {
            const data = JSON.parse(message.value.toString());
            await query(logger, config.toTopic, data.ids, producer);
        }
    },
});
