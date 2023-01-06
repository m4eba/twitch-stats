import {
  KafkaConfig,
  KafkaConfigOpt,
  defaultValues,
  FileConfig,
  FileConfigOpt,
} from '@twitch-stats/config';
import pino, { Logger } from 'pino';
import { Kafka, Consumer } from 'kafkajs';
import { ArgumentConfig, parse } from 'ts-command-line-args';
import { FileWriter } from './FileWriter';

interface WriterConfig {
  topic: string;
  path: string;
}

const WriterConfigOpt: ArgumentConfig<WriterConfig> = {
  topic: { type: String, defaultValue: defaultValues.streamsTopic },
  path: { type: String },
};

interface Config extends WriterConfig, KafkaConfig, FileConfig {}

const logger: Logger = pino({ level: 'debug' }).child({ module: 'log-writer' });

const config: Config = parse<Config>(
  {
    ...KafkaConfigOpt,
    ...WriterConfigOpt,
    ...FileConfigOpt,
  },
  {
    loadFromFileArg: 'config',
  }
);

const kafka: Kafka = new Kafka({
  clientId: config.kafkaClientId,
  brokers: config.kafkaBroker,
});
logger.info({ topic: config.topic }, 'subscribe');
const consumer: Consumer = kafka.consumer({ groupId: 'stream-log' });
await consumer.connect();
await consumer.subscribe({ topic: config.topic, fromBeginning: true });

const out: FileWriter = new FileWriter(config.path);

await consumer.run({
  eachMessage: async ({ message }) => {
    if (message.value) {
      out.write(
        JSON.stringify({
          time: message.timestamp,
          data: message.value.toString(),
        })
      );
    }
  },
});
