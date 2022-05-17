import { KafkaConfigOpt, defaultValues, FileConfigOpt, } from '@twitch-stats/config';
import pino from 'pino';
import { Kafka } from 'kafkajs';
import { parse } from 'ts-command-line-args';
import rfs from 'rotating-file-stream';
const WriterConfigOpt = {
    topic: { type: String, defaultValue: defaultValues.streamsTopic },
    filename: { type: String },
    rotateInterval: {
        type: String,
        defaultValue: '1d',
        description: 'interval the log file rotates (see https://github.com/iccicci/rotating-file-stream)',
    },
    rotateMaxfiles: {
        type: Number,
        defaultValue: 10,
        description: 'maximal number of log files',
    },
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
const out = rfs.createStream(config.filename, {
    interval: '1d',
    maxFiles: 10,
});
await consumer.run({
    eachMessage: async ({ message }) => {
        if (message.value) {
            logger.info({
                message: JSON.parse(message.value.toString()),
            }, 'msg received');
            out.write(JSON.stringify({
                time: message.timestamp,
                data: JSON.parse(message.value.toString()),
            }));
            out.write('\n');
        }
    },
});
