import fs from 'fs';
import { initLogger } from '@twitch-stats/utils';
import pg from 'pg';
const logger = initLogger('database-init');
// see https://github.com/brianc/node-postgres/issues/811#issuecomment-488374261
pg.types.setTypeParser(1700, function (val) {
    return parseFloat(val);
});
export async function initPostgres(config) {
    const c = {
        host: config.pgHost,
        user: config.pgUser,
        password: config.pgPassword,
        port: config.pgPort,
        database: config.pgDatabase,
    };
    if (config.pgUseSsl) {
        c.ssl = true;
    }
    if (config.pgCa) {
        c.ssl = {
            ca: await fs.promises.readFile(config.pgCa, { encoding: 'utf8' }),
        };
    }
    if (config.pgCa && config.pgCert && config.pgKey) {
        c.ssl = {
            ca: await fs.promises.readFile(config.pgCa, { encoding: 'utf8' }),
            key: await fs.promises.readFile(config.pgKey, { encoding: 'utf8' }),
            cert: await fs.promises.readFile(config.pgCert, { encoding: 'utf8' }),
        };
    }
    const p = new pg.Pool(c);
    p.on('error', (err) => {
        logger.error(err);
    });
    return p;
}
