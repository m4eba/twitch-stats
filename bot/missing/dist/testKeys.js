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
(async () => {
    const config = parse({
        ...UpdateConfigOpt,
        ...FileConfigOpt,
        ...LogConfigOpt,
        ...PostgresConfigOpt,
    }, {
        loadFromFileArg: 'config',
    });
    const logger = pino({ level: config.logLevel }).child({
        module: 'testKeysRedis',
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
    const batch = new Array(config.batchSize);
    let length = 0;
    let count = 0;
    let result;
    const testRedis = async () => {
        if (length == 0)
            return;
        if (length < config.batchSize) {
            // batch size not reach
            const smaller = batch.slice(0, length);
            logger.debug({ update: smaller }, 'redis test');
            result = await redis.mGet(smaller);
        }
        else {
            logger.debug({ update: batch }, 'redis test');
            result = await redis.mGet(batch);
        }
        logger.debug({ ids: result }, 'redis check id result');
        count += length;
        length = 0;
    };
    const testGames = async () => {
        const promise = new Promise((resolve) => {
            const query = new QueryStream('SELECT game_id from game');
            const stream = client.query(query);
            stream.on('end', async () => {
                await testRedis();
                resolve();
            });
            stream.on('data', async (chunk) => {
                batch[length * 2] = Prefix.game + chunk.game_id;
                batch[length * 2 + 1] = chunk.game_id;
                length++;
                if (length == config.batchSize) {
                    await testRedis();
                }
            });
        });
        return promise;
    };
    const testStreamer = async () => {
        const promise = new Promise((resolve) => {
            const query = new QueryStream('SELECT user_id from streamers');
            const stream = client.query(query);
            stream.on('end', async () => {
                await testRedis();
                resolve();
            });
            stream.on('data', async (chunk) => {
                batch[length] = Prefix.user + chunk.user_id;
                length++;
                if (length == config.batchSize) {
                    await testRedis();
                }
            });
        });
        return promise;
    };
    const testTags = async () => {
        const promise = new Promise((resolve) => {
            const query = new QueryStream('SELECT tag_id from tags');
            const stream = client.query(query);
            stream.on('end', async () => {
                await testRedis();
                resolve();
            });
            stream.on('data', async (chunk) => {
                batch[length * 2] = Prefix.tag + chunk.tag_id;
                batch[length * 2 + 1] = chunk.tag_id;
                length++;
                if (length == config.batchSize) {
                    await testRedis();
                }
            });
        });
        return promise;
    };
    await testStreamer();
    logger.info({ count }, 'streamer count');
    /*count = 0;
    await testGames();
    logger.info({ count }, 'game count');
    count = 0;
    await testTags();
    logger.info({ count }, 'tag count');
    count = 0;*/
    await redis.disconnect();
    await client.end();
})();
