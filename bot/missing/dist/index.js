import { KafkaConfigOpt, FileConfigOpt, PostgresConfigOpt, TwitchConfigOpt, LogConfigOpt, } from '@twitch-stats/config';
import { init } from '@twitch-stats/twitch';
import pg from 'pg';
const { Pool } = pg;
import { createClient } from 'redis';
import pino from 'pino';
import { Kafka } from 'kafkajs';
import { parse } from 'ts-command-line-args';
import Missing from './missing.js';
const MissingConfigOpt = {
    topic: { type: String },
    redisUrl: { type: String },
};
const config = parse({
    ...KafkaConfigOpt,
    ...MissingConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
    ...PostgresConfigOpt,
    ...TwitchConfigOpt,
}, {
    loadFromFileArg: 'config',
});
const logger = pino({ level: config.logLevel }).child({
    module: 'missing',
});
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
const client = createClient({
    url: config.redisUrl,
});
await client.connect();
logger.info({ topic: config.topic }, 'subscribe');
const consumer = kafka.consumer({ groupId: 'stream-missing' });
await consumer.connect();
await consumer.subscribe({ topic: config.topic, fromBeginning: true });
await init(config);
const missing = new Missing(logger, pool, client);
await missing.initRedis();
await consumer.run({
    eachMessage: async ({ message }) => {
        if (message.value) {
            logger.debug({
                message: JSON.parse(message.value.toString()),
            }, 'msg received');
            const msg = JSON.parse(message.value.toString());
            await missing.update(msg.streams);
        }
    },
});
