import { KafkaConfigOpt, defaultValues, TwitchConfigOpt, PostgresConfigOpt, FileConfigOpt, } from '@twitch-stat-bot/common';
import { init } from '@twitch-stat-bot/twitch';
import { Pool } from 'pg';
import pino from 'pino';
import { Kafka } from 'kafkajs';
import { processEnd, processStream } from './processing.js';
import { parse } from 'ts-command-line-args';
const TopicConfigOpt = {
    topic: { type: String, defaultValue: defaultValues.topic },
};
const logger = pino().child({ module: 'stream-process' });
(async () => {
    const config = parse({
        ...KafkaConfigOpt,
        ...TwitchConfigOpt,
        ...TopicConfigOpt,
        ...PostgresConfigOpt,
        ...FileConfigOpt,
    }, {
        loadFromFileArg: 'config',
    });
    await init(config);
    const pool = new Pool({
        host: config.pgHost,
        port: config.pgPort,
        database: config.pgDatabase,
        user: config.pgUser,
        password: config.pgPassword,
    });
    const kafka = new Kafka({
        clientId: config.kafkaClientId,
        brokers: config.kafkaBroker,
    });
    const consumer = kafka.consumer({ groupId: 'stream-process' });
    await consumer.connect();
    await consumer.subscribe({ topic: config.topic, fromBeginning: true });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            logger.info({
                topic,
                partition,
                message,
            }, 'msg received');
            const d = new Date(message.timestamp);
            if (message.value == null) {
                await processEnd(pool);
            }
            else {
                const stream = JSON.parse(message.value.toString());
                await processStream(pool, d, stream);
            }
        },
    });
})();
