// Produce fake crawl runs into the streams topic:
//   run1 (t-20m): alice, bob, carol live
//   run2 (t-10m): alice changes title+game, viewers move
//   run3 (t-0):   carol gone -> ended
//   run4 (t+10s): bob gone   -> ended
// Leaves alice live; bob and carol should end up in the archive.
import { Kafka } from 'kafkajs';

const broker = process.env.KAFKA_BROKER ?? 'localhost:19000';
const topic = 'twitch-stats-streams';

const kafka = new Kafka({ clientId: 'seed', brokers: [broker] });
const producer = kafka.producer();
await producer.connect();

const now = Date.now();
const min = 60 * 1000;

function stream(id, user_id, user_name, title, game_id, viewer_count, tags) {
  return {
    id,
    user_id,
    user_name,
    game_id,
    game_name: 'game-' + game_id,
    type: 'live',
    title,
    viewer_count,
    started_at: new Date(now - 25 * min).toISOString(),
    language: 'en',
    thumbnail_url: '',
    tags,
  };
}

async function run(time, streams, endConfig) {
  if (streams.length > 0) {
    await producer.send({
      topic,
      messages: [
        {
          key: 'stream',
          value: JSON.stringify({ streams }),
          timestamp: String(time),
        },
      ],
    });
  }
  await producer.send({
    topic,
    messages: [
      {
        key: 'stream',
        value: JSON.stringify({ streams: [], endConfig }),
        timestamp: String(time),
      },
    ],
  });
}

const alice = () => stream('1001', '101', 'alice', 'building stuff', '509658', 10, ['en', 'dev']);
const bob = () => stream('1002', '102', 'bob', 'speedrun', '33214', 50, ['en']);
const carol = () => stream('1003', '103', 'carol', 'chatting', '509658', 100, ['en', 'chat']);

const t1 = now - 20 * min;
await run(t1, [alice(), bob(), carol()], {
  updateStartTime: new Date(t1 - 1000).toISOString(),
  update: true,
});

const t2 = now - 10 * min;
const alice2 = alice();
alice2.title = 'now: gamedev';
alice2.game_id = '33214';
alice2.viewer_count = 25;
const bob2 = bob();
bob2.viewer_count = 60;
const carol2 = carol();
carol2.viewer_count = 80;
await run(t2, [alice2, bob2, carol2], {
  updateStartTime: new Date(t2 - 1000).toISOString(),
  update: true,
});

const t3 = now;
const alice3 = alice2;
alice3.viewer_count = 30;
const bob3 = bob2;
bob3.viewer_count = 55;
await run(t3, [alice3, bob3], {
  updateStartTime: new Date(t3 - 1000).toISOString(),
  update: true,
});

const t4 = now + 10 * 1000;
await run(t4, [alice3], {
  updateStartTime: new Date(t4 - 1000).toISOString(),
  update: true,
});

console.log('seeded 4 crawl runs');
await producer.disconnect();
