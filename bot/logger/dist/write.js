import { KafkaConfigOpt, defaultValues, FileConfigOpt, } from '@twitch-stats/config';
import pino from 'pino';
import { Kafka } from 'kafkajs';
import { parse } from 'ts-command-line-args';
import { FileWriter } from './FileWriter.js';
const WriterConfigOpt = {
    topic: { type: String, defaultValue: defaultValues.streamsTopic },
    path: { type: String },
};
const logger = pino({ level: 'debug' }).child({ module: 'log-writer' });
const config = parse({
    ...KafkaConfigOpt,
    ...WriterConfigOpt,
    ...FileConfigOpt,
}, {
    loadFromFileArg: 'config',
});
const kafka = new Kafka({
    clientId: config.kafkaClientId,
    brokers: config.kafkaBroker,
});
logger.info({ topic: config.topic }, 'subscribe');
const consumer = kafka.consumer({ groupId: 'stream-log' });
await consumer.connect();
await consumer.subscribe({ topic: config.topic, fromBeginning: true });
const out = new FileWriter(config.path);
await consumer.run({
    eachMessage: async ({ message }) => {
        if (message.value) {
            out.write(`{"time":"${message.timestamp}","data":${message.value.toString()}}`);
        }
    },
});
