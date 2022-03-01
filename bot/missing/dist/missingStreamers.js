import { FileConfigOpt, PostgresConfigOpt, TwitchConfigOpt, LogConfigOpt, } from '@twitch-stats/config';
import { init } from '@twitch-stats/twitch';
import pg from 'pg';
const { Pool } = pg;
import { createClient } from 'redis';
import pino from 'pino';
import { parse } from 'ts-command-line-args';
import Missing from './missing.js';
const MissingConfigOpt = {
    redisUrl: { type: String },
};
(async () => {
    const config = parse({
        ...MissingConfigOpt,
        ...FileConfigOpt,
        ...LogConfigOpt,
        ...PostgresConfigOpt,
        ...TwitchConfigOpt,
    }, {
        loadFromFileArg: 'config',
    });
    const logger = pino({ level: config.logLevel }).child({ module: 'missing' });
    const pool = new Pool({
        host: config.pgHost,
        port: config.pgPort,
        database: config.pgDatabase,
        user: config.pgUser,
        password: config.pgPassword,
    });
    const client = createClient({
        url: config.redisUrl,
    });
    await client.connect();
    await init(config);
    const missing = new Missing(logger, pool, client);
    const result = await pool.query({
        text: 'select s.user_id from stream s where s.user_id not in ( SELECT st.user_id from streamers st) group by s.user_id',
        rowMode: 'array',
    });
    const ids = new Array(result.rows.length);
    for (let i = 0; i < ids.length; ++i) {
        ids[i] = result.rows[i][0];
    }
    missing.updateUser(ids);
    await pool.end();
})();
