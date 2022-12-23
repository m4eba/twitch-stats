import fs from 'fs';
import type { PostgresConfig } from '@twitch-stats/config';
import type { Pool } from 'pg';
import type { Logger } from 'pino';
import { initLogger } from '@twitch-stats/utils';
import pg from 'pg';

const logger: Logger = initLogger('database-init');

// see https://github.com/brianc/node-postgres/issues/811#issuecomment-488374261
pg.types.setTypeParser(1700, function (val: string) {
  return parseFloat(val);
});

export async function initPostgres(config: PostgresConfig): Promise<Pool> {
  const c: pg.PoolConfig = {
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
  p.on('error', (err: Error) => {
    logger.error(err);
  });

  return p;
}
