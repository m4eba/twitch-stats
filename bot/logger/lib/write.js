"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@twitch-stat-bot/common");
const pino_1 = __importDefault(require("pino"));
const fs_1 = __importDefault(require("fs"));
const kafkajs_1 = require("kafkajs");
const ts_command_line_args_1 = require("ts-command-line-args");
const WriterConfigOpt = {
    fromTopic: { type: String, defaultValue: common_1.defaultValues.topic },
    toTopic: { type: String, optional: true },
    filename: { type: String },
};
const logger = (0, pino_1.default)().child({ module: 'log-writer' });
(async () => {
    const config = (0, ts_command_line_args_1.parse)(Object.assign(Object.assign(Object.assign({}, common_1.KafkaConfigOpt), WriterConfigOpt), common_1.FileConfigOpt), {
        loadFromFileArg: 'config',
    });
    const kafka = new kafkajs_1.Kafka({
        clientId: config.kafkaClientId,
        brokers: config.kafkaBroker,
    });
    const consumer = kafka.consumer({ groupId: 'stream-process' });
    await consumer.connect();
    await consumer.subscribe({ topic: config.fromTopic, fromBeginning: true });
    const producer = kafka.producer();
    const out = fs_1.default.createWriteStream(config.filename, {
        encoding: 'utf-8',
        flags: 'a',
    });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            logger.info({
                topic,
                partition,
                message,
            }, 'msg received');
            const d = new Date(message.timestamp);
            if (message.value != null) {
                out.write(JSON.stringify(message));
                out.write('\n');
            }
            else {
                out.write('null\n');
            }
            if (config.toTopic) {
                await producer.send({
                    topic: config.toTopic,
                    messages: [message],
                });
            }
        },
    });
})();
//# sourceMappingURL=write.js.map