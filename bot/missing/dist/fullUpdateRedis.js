import { FileConfigOpt, PostgresConfigOpt, LogConfigOpt, } from '@twitch-stats/config';
import pg from 'pg';
import QueryStream from 'pg-query-stream';
import { createClient } from 'redis';
import pino from 'pino';
import { parse } from 'ts-command-line-args';
import Prefix from './prefix.js';
const UpdateConfigOpt = {
    redisUrl: { type: String },
    batchSize: { type: Number, defaultValue: 1000 },
};
const config = parse({
    ...UpdateConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
    ...PostgresConfigOpt,
}, {
    loadFromFileArg: 'config',
});
const logger = pino({ level: config.logLevel }).child({
    module: 'fullUpdateRedis',
});
const client = new pg.Client({
    host: config.pgHost,
    port: config.pgPort,
    database: config.pgDatabase,
    user: config.pgUser,
    password: config.pgPassword,
});
await client.connect();
const redis = createClient({
    url: config.redisUrl,
});
await redis.connect();
let count = 0;
const updateRedis = async (ids) => {
    logger.debug({ update: ids }, 'redis update');
    count += ids.length;
    await redis.mSet(ids);
};
const updateGames = async () => {
    const promise = new Promise((resolve) => {
        const query = new QueryStream('SELECT game_id from game');
        const stream = client.query(query);
        let ids = [];
        stream.on('end', async () => {
            await updateRedis(ids);
            resolve();
        });
        stream.on('data', async (chunk) => {
            ids.push(Prefix.game + chunk.game_id);
            ids.push(chunk.game_id);
            if (ids.length === config.batchSize * 2) {
                const batch = [...ids];
                ids = [];
                await updateRedis(batch);
            }
        });
    });
    return promise;
};
const updateStreamer = async () => {
    const promise = new Promise((resolve) => {
        const query = new QueryStream('SELECT user_id from streamers');
        const stream = client.query(query);
        let ids = [];
        stream.on('end', async () => {
            await updateRedis(ids);
            resolve();
        });
        stream.on('data', async (chunk) => {
            ids.push(Prefix.user + chunk.user_id);
            ids.push(chunk.user_id);
            if (ids.length === config.batchSize * 2) {
                const batch = [...ids];
                ids = [];
                await updateRedis(batch);
            }
        });
    });
    return promise;
};
const updateTags = async () => {
    const promise = new Promise((resolve) => {
        const query = new QueryStream('SELECT tag_id from tags');
        const stream = client.query(query);
        let ids = [];
        stream.on('end', async () => {
            await updateRedis(ids);
            resolve();
        });
        stream.on('data', async (chunk) => {
            ids.push(Prefix.tag + chunk.tag_id);
            ids.push(chunk.tag_id);
            if (ids.length === config.batchSize * 2) {
                const batch = [...ids];
                ids = [];
                await updateRedis(batch);
            }
        });
    });
    return promise;
};
await updateStreamer();
logger.info({ count }, 'streamer count');
count = 0;
await updateGames();
logger.info({ count }, 'game count');
count = 0;
await updateTags();
logger.info({ count }, 'tag count');
count = 0;
await redis.disconnect();
await client.end();
