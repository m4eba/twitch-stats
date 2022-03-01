"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const twitch_1 = require("@twitch-stat-bot/twitch");
const common_1 = require("@twitch-stat-bot/common");
const pino_1 = __importDefault(require("pino"));
const kafkajs_1 = require("kafkajs");
const ts_command_line_args_1 = require("ts-command-line-args");
const FetcherConfigOpt = {
    topic: { type: String, multiple: true, defaultValue: [common_1.defaultValues.topic] },
    minViewers: { type: Number, defaultValue: 5 },
};
const logger = (0, pino_1.default)().child({ module: 'stream-fetcher' });
(async () => {
    const config = (0, ts_command_line_args_1.parse)(Object.assign(Object.assign(Object.assign(Object.assign({}, FetcherConfigOpt), common_1.KafkaConfigOpt), common_1.TwitchConfigOpt), common_1.FileConfigOpt), {
        loadFromFileArg: 'config',
    });
    await (0, twitch_1.init)(config);
    const kafka = new kafkajs_1.Kafka({
        clientId: config.kafkaClientId,
        brokers: config.kafkaBroker,
    });
    const producer = kafka.producer();
    let result;
    let cursor = undefined;
    let params = { first: 100 };
    let count = 0;
    let page = 0;
    do {
        if (cursor != null) {
            params.after = cursor;
        }
        const log = logger.child({
            cursor,
            page,
        });
        page++;
        result = await (0, twitch_1.helix)('streams', params);
        count += result.data.length;
        log.info(`result cound: ${result.data.length}`);
        if (result.data.length > 0) {
            log.info(`viewer count ${result.data[result.data.length - 1].viewer_count}`);
            const time = new Date().toISOString();
            let messages = [];
            for (let i = 0; i < config.topic.length; ++i) {
                let topicMessage = {
                    topic: config.topic[i],
                    messages: [],
                };
                for (let i = 0; i < result.data.length; ++i) {
                    const stream = result.data[i];
                    log.debug(stream, 'kafka.producer.stream');
                    topicMessage.messages.push({
                        key: 'stream',
                        value: JSON.stringify(stream),
                        timestamp: time,
                    });
                }
            }
            await producer.sendBatch({ topicMessages: messages });
        }
        if (result.data &&
            result.data.length > 0 &&
            result.data[result.data.length - 1].viewer_count &&
            result.data[result.data.length - 1].viewer_count < config.minViewers) {
            log.debug('last cursor');
            let messages = [];
            for (let i = 0; i < config.topic.length; ++i) {
                messages.push({
                    topic: config.topic[i],
                    messages: [
                        {
                            key: 'stream',
                            value: null,
                        },
                    ],
                });
            }
            await producer.sendBatch({ topicMessages: messages });
            break;
        }
    } while (cursor);
})();
//# sourceMappingURL=index.js.map