import { KafkaConfigOpt, defaultValues, FileConfigOpt, } from '@twitch-stats/config';
import pino from 'pino';
import fs from 'node:fs';
import { Kafka } from 'kafkajs';
import { parse } from 'ts-command-line-args';
import * as readline from 'node:readline';
const ReaderConfigOpt = {
    topic: { type: String, defaultValue: defaultValues.streamsTopic },
    filename: { type: String },
};
const logger = pino({ level: 'debug' }).child({ module: 'log-reader' });
const config = parse({
    ...KafkaConfigOpt,
    ...ReaderConfigOpt,
    ...FileConfigOpt,
}, {
    loadFromFileArg: 'config',
});
const kafka = new Kafka({
    clientId: config.kafkaClientId,
    brokers: config.kafkaBroker,
});
logger.info({ topic: config.topic }, 'topic');
const producer = kafka.producer();
await producer.connect();
const instream = fs.createReadStream(config.filename, {
    encoding: 'utf-8',
});
const rl = readline.createInterface({
    input: instream,
    crlfDelay: Infinity,
});
for await (const line of rl) {
    const logData = JSON.parse(line);
    if (logData.data.endConfig) {
        logData.data.endConfig.update = false;
    }
    await producer.send({
        topic: config.topic,
        messages: [
            {
                key: 'stream',
                value: JSON.stringify(logData.data),
                timestamp: logData.time,
            },
        ],
    });
}
await producer.disconnect();
