import {
  PaginatedResult,
  init,
  helix,
  Stream,
  StreamsMessage,
} from '@twitch-stats/twitch';
import {
  KafkaConfig,
  TwitchConfig,
  KafkaConfigOpt,
  TwitchConfigOpt,
  defaultValues,
  FileConfig,
  FileConfigOpt,
  LogConfig,
  LogConfigOpt,
  PostgresConfigOpt,
  PostgresConfig,
} from '@twitch-stats/config';
import pino, { Logger } from 'pino';
import { Kafka, Producer, TopicMessages } from 'kafkajs';
import { parse, ArgumentConfig } from 'ts-command-line-args';
import { initPostgres } from '@twitch-stats/database';
import pg from 'pg';
import QueryStream from 'pg-query-stream';
import { DateTime } from 'luxon';
import fs from 'fs';
import path from 'path';

interface ServiceConfig {
  age: number;
  delete: boolean;
  outputTopic: string;
  path: string;
}

const ServiceConfigOpt: ArgumentConfig<ServiceConfig> = {
  age: { type: Number, defaultValue: 60 * 60 * 24 * 7 },
  delete: { type: Boolean, defaultValue: false },
  outputTopic: { type: String, defaultValue: defaultValues.exportTopic },
  path: { type: String },
};

interface Config
  extends ServiceConfig,
    KafkaConfig,
    TwitchConfig,
    FileConfig,
    PostgresConfig,
    LogConfig {}

const logger: Logger = pino({ level: 'debug' }).child({
  module: 'stream-export',
});

const config: Config = parse<Config>(
  {
    ...ServiceConfigOpt,
    ...KafkaConfigOpt,
    ...TwitchConfigOpt,
    ...FileConfigOpt,
    ...PostgresConfigOpt,
    ...LogConfigOpt,
  },
  {
    loadFromFileArg: 'config',
  }
);

await init(config);

const client: pg.Client = new pg.Client({
  host: config.pgHost,
  port: config.pgPort,
  database: config.pgDatabase,
  user: config.pgUser,
  password: config.pgPassword,
});
await client.connect();

const client2: pg.Client = new pg.Client({
  host: config.pgHost,
  port: config.pgPort,
  database: config.pgDatabase,
  user: config.pgUser,
  password: config.pgPassword,
});
await client2.connect();

const kafka: Kafka = new Kafka({
  clientId: config.kafkaClientId,
  brokers: config.kafkaBroker,
});
const producer: Producer = kafka.producer();
await producer.connect();

const pool = await initPostgres(config);

interface stream_db {
  stream_id: string;
  user_id: string;
  title: string;
  game_id: string;
  started_at: Date;
  ended_at: Date;
  updated_at: Date;
}

async function processStreamRow(stream: stream_db) {
  const d = DateTime.fromJSDate(stream.started_at);

  const out = path.join(
    config.path,
    stream.user_id,
    d.year.toString(),
    d.month.toString().padStart(2, '0')
  );

  await fs.promises.mkdir(out, { recursive: true });
  console.log('stream', stream);

  const games = await client2.query(
    'select * from stream_game where stream_id = $1',
    [stream.stream_id]
  );
  console.log(games.rows);
  const tags = await client2.query(
    'select * from stream_tags where stream_id = $1',
    [stream.stream_id]
  );
  console.log(tags.rows);
  const probe = await client2.query(
    'select * from probe where stream_id = $1',
    [stream.stream_id]
  );
  console.log(probe.rows);
  const title = await client2.query(
    'select * from stream_title where stream_id = $1',
    [stream.stream_id]
  );
  const data = {
    stream,
    games: games.rows,
    tags: tags.rows,
    probe: probe.rows,
    title: title.rows,
  };
  const name =
    stream.started_at.toISOString() + '-' + stream.stream_id + '.json';
  await fs.promises.writeFile(
    path.join(out, name),
    JSON.stringify(data, null, ' ')
  );

  if (config.delete) {
    await client2.query('delete from stream where stream_id = $1', [
      stream.stream_id,
    ]);
    await client2.query('delete from stream_game where stream_id = $1', [
      stream.stream_id,
    ]);
    await client2.query('delete from stream_tags where stream_id = $1', [
      stream.stream_id,
    ]);
    await client2.query('delete from stream_title where stream_id = $1', [
      stream.stream_id,
    ]);
    await client2.query('delete from probe where stream_id = $1', [
      stream.stream_id,
    ]);
  }
  console.log(title.rows);
}

const query = async (): Promise<void> => {
  const promise = new Promise<void>((resolve) => {
    const time = DateTime.now().minus({ seconds: config.age });

    const query = new QueryStream(
      'SELECT * FROM stream WHERE ended_at is not null AND ended_at < $1',
      [time]
    );
    const stream = client.query(query);

    stream.on('end', async () => {
      resolve();
    });

    stream.on('data', async (chunk) => {
      console.log('chunk received');
      stream.pause();
      await processStreamRow(chunk);
      stream.resume();
    });
  });

  return promise;
};

await query();
await pool.end();
console.log('query done');

await producer.disconnect();
