import pg from 'pg';
import { S3Client } from '@aws-sdk/client-s3';
import type { Producer } from 'kafkajs';
import type { Stream } from '@twitch-stats/twitch';

// Fixed infra coordinates — must match e2e/docker-compose.yml.
export const PG = {
  host: 'localhost',
  port: 5447,
  user: 'postgres',
  password: 'password',
  database: 'tw_stats',
};

export const S3 = {
  endpoint: 'http://localhost:9008',
  region: 'us-east-1',
  bucket: 'twstats-e2e',
  accessKeyId: 'minioadmin',
  secretAccessKey: 'minioadmin',
};

export function makePool(): pg.Pool {
  return new pg.Pool(PG);
}

export function makeS3(): S3Client {
  return new S3Client({
    endpoint: S3.endpoint,
    region: S3.region,
    forcePathStyle: true,
    credentials: {
      accessKeyId: S3.accessKeyId,
      secretAccessKey: S3.secretAccessKey,
    },
  });
}

// Records everything the code under test would have published to Kafka, so
// tests can assert on the pipeline contract without a broker.
export interface StubProducer {
  producer: Producer;
  sent: Array<{ topic: string; value: unknown }>;
}

export function stubProducer(): StubProducer {
  const sent: Array<{ topic: string; value: unknown }> = [];
  const producer = {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    send: async (record: any) => {
      for (const m of record.messages) {
        sent.push({ topic: record.topic, value: JSON.parse(m.value) });
      }
      return [];
    },
  } as unknown as Producer;
  return { producer, sent };
}

const HOT_TABLES = [
  'probe',
  'stream_title',
  'stream_game',
  'stream_tags',
  'user_online',
  'stream',
  'archive_stream',
  'stream_summary',
];

export async function resetDb(pool: pg.Pool): Promise<void> {
  await pool.query(`TRUNCATE ${HOT_TABLES.join(', ')} CASCADE`);
}

let seq = 0;
export function sampleStream(over: Partial<Stream> = {}): Stream {
  seq += 1;
  return {
    id: String(1000 + seq),
    user_id: String(2000 + seq),
    user_name: `streamer${seq}`,
    game_id: '509658',
    game_name: 'Just Chatting',
    type: 'live',
    title: `title ${seq}`,
    viewer_count: 100,
    started_at: '2026-07-23T10:00:00.000Z',
    language: 'en',
    thumbnail_url: 'https://t/img.jpg',
    tags: ['English'],
    ...over,
  };
}
