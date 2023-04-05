import { FileConfigOpt, PostgresConfigOpt, TwitchConfigOpt, LogConfigOpt, } from '@twitch-stats/config';
import { init } from '@twitch-stats/twitch';
import pg from 'pg';
import QueryStream from 'pg-query-stream';
import { createClient } from 'redis';
import pino from 'pino';
import { parse } from 'ts-command-line-args';
import Prefix from './prefix.js';
import Missing from './missing.js';
const UpdateConfigOpt = {
    redisUrl: { type: String },
};
class Update {
    constructor(logger, prefix, missing, updateFn) {
        this.ids = new Array(100);
        this.ids_hash = new Set();
        this.ids_length = 0;
        this.ids_offset = 0;
        this.count = 0;
        this.logger = logger;
        this.prefix = prefix;
        this.missing = missing;
        this.updateFn = updateFn;
    }
    async update() {
        this.count += this.ids_length;
        if (this.ids_length === 100) {
            await this.updateFn(this.ids);
        }
        else {
            await this.updateFn(this.ids.slice(0, this.ids_length));
        }
        this.ids_offset = 0;
        this.ids_length = 0;
        this.ids = new Array(100);
        this.ids_hash.clear();
    }
    async add(id) {
        if (this.ids_hash.has(id))
            return;
        this.logger.debug({ id }, 'add id');
        this.ids_hash.add(id);
        this.ids[this.ids_length] = id;
        this.ids_length++;
        await this.checkIds(false);
    }
    async checkIds(force) {
        if (this.ids_length === 100 || force) {
            this.logger.debug({ offset: this.ids_offset, length: this.ids_length, ids: this.ids }, 'check ids');
            this.logger.debug({ length: this.ids_length - this.ids_offset }, 'check ids length');
            const checked = await this.missing.checkIds(this.prefix, this.ids.slice(this.ids_offset, this.ids_length));
            this.logger.debug({ ids: checked }, 'id check result');
            this.ids.splice(this.ids_offset, this.ids.length - this.ids_offset, ...checked);
            this.ids_offset = this.ids.length;
            this.ids_length = this.ids.length;
            this.logger.debug({ offset: this.ids_offset, length: this.ids.length, ids: this.ids }, 'ids');
        }
        if (this.ids_length === 100 || force) {
            await this.update();
        }
    }
}
const config = parse({
    ...UpdateConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
    ...TwitchConfigOpt,
    ...PostgresConfigOpt,
}, {
    loadFromFileArg: 'config',
});
const logger = pino({ level: config.logLevel }).child({
    module: 'fullUpdateMissing',
});
const pool = new pg.Pool({
    host: config.pgHost,
    port: config.pgPort,
    database: config.pgDatabase,
    user: config.pgUser,
    password: config.pgPassword,
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
await init(config);
const missing = new Missing(logger, pool, redis);
const updateUser = new Update(logger.child({ module: 'user' }), Prefix.user, missing, missing.updateUser.bind(missing));
const updateGame = new Update(logger.child({ module: 'game' }), Prefix.game, missing, missing.updateGame.bind(missing));
let rowCount = 0;
const query = async () => {
    const promise = new Promise((resolve) => {
        const query = new QueryStream('SELECT user_id,game_id from stream');
        const stream = client.query(query);
        stream.on('end', async () => {
            await updateUser.checkIds(true);
            await updateGame.update();
            resolve();
        });
        stream.on('data', async (chunk) => {
            rowCount++;
            stream.pause();
            await Promise.all([
                updateUser.add(chunk.user_id),
                updateGame.add(chunk.game_id),
            ]);
            stream.resume();
        });
    });
    return promise;
};
await query();
logger.info({ count: rowCount }, 'result row count');
logger.info({ count: updateUser.count }, 'streamer count');
logger.info({ count: updateGame.count }, 'game count');
await redis.disconnect();
await client.end();
await pool.end();
