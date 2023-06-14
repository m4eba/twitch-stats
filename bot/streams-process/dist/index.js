import { KafkaConfigOpt, defaultValues, TwitchConfigOpt, PostgresConfigOpt, FileConfigOpt, LogConfigOpt, } from '@twitch-stats/config';
import { init } from '@twitch-stats/twitch';
import { initPostgres } from '@twitch-stats/database';
import pino from 'pino';
import { Kafka } from 'kafkajs';
import { parse } from 'ts-command-line-args';
import Processing from './processing.js';
const TopicConfigOpt = {
    topic: { type: String, defaultValue: defaultValues.streamsTopic },
    streamIdTopic: { type: String, defaultValue: defaultValues.streamsIdTopic },
};
const config = parse({
    ...KafkaConfigOpt,
    ...TwitchConfigOpt,
    ...TopicConfigOpt,
    ...PostgresConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
}, {
    loadFromFileArg: 'config',
});
const logger = pino({ level: config.logLevel }).child({
    module: 'streams-process',
});
await init(config);
const pool = await initPostgres(config);
const kafka = new Kafka({
    clientId: config.kafkaClientId,
    brokers: config.kafkaBroker,
});
const consumer = kafka.consumer({ groupId: 'stream-process' });
await consumer.connect();
await consumer.subscribe({ topic: config.topic, fromBeginning: true });
const producer = kafka.producer();
await producer.connect();
const processing = new Processing(logger, pool, producer, config.streamIdTopic);
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
            const msg = JSON.parse(message.value.toString());
            await processing.processStreams(d, msg.streams);
            if (msg.endConfig) {
                await processing.processEnd(msg.endConfig);
            }
            logger.flush();
        }
        catch (e) {
            logger.error({ error: e }, 'error in eachMessage');
            console.log(e);
            logger.flush();
            process.exit();
        }
    },
});
