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
import rfs from 'rotating-file-stream';

interface WriterConfig {
  topic: string;
  filename: string;
  rotateInterval: string;
  rotateMaxfiles: number;
}

const WriterConfigOpt: ArgumentConfig<WriterConfig> = {
  topic: { type: String, defaultValue: defaultValues.streamsTopic },
  filename: { type: String },
  rotateInterval: {
    type: String,
    defaultValue: '1d',
    description:
      'interval the log file rotates (see https://github.com/iccicci/rotating-file-stream)',
  },
  rotateMaxfiles: {
    type: Number,
    defaultValue: 10,
    description: 'maximal number of log files',
  },
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

const out: rfs.RotatingFileStream = rfs.createStream(config.filename, {
  interval: config.rotateInterval,
  maxFiles: config.rotateMaxfiles,
});

await consumer.run({
  eachMessage: async ({ message }) => {
    if (message.value) {
      logger.info(
        {
          message: JSON.parse(message.value.toString()),
        },
        'msg received'
      );
      out.write(
        JSON.stringify({
          time: message.timestamp,
          data: JSON.parse(message.value.toString()),
        })
      );
      out.write('\n');
    }
  },
});
