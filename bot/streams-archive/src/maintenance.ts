// Partition maintenance for the day-partitioned history tables: creates
// partitions daysAhead into the future and drops partitions older than
// retentionDays. A partition is never dropped while an unarchived stream
// started before its upper bound (its probes would be lost before archiving);
// see planRetention.
import {
  PostgresConfig,
  PostgresConfigOpt,
  FileConfig,
  FileConfigOpt,
  LogConfig,
  LogConfigOpt,
} from '@twitch-stats/config';
import { initPostgres } from '@twitch-stats/database';
import type { Pool } from 'pg';
import pino, { Logger } from 'pino';
import { ArgumentConfig, parse } from 'ts-command-line-args';
import { Partition, planRetention, retentionCutoff } from './retention.js';

interface MaintenanceConfig {
  daysAhead: number;
  retentionDays: number;
}

const MaintenanceConfigOpt: ArgumentConfig<MaintenanceConfig> = {
  daysAhead: { type: Number, defaultValue: 7 },
  retentionDays: { type: Number, defaultValue: 14 },
};

interface Config
  extends MaintenanceConfig,
    PostgresConfig,
    FileConfig,
    LogConfig {}

const config: Config = parse<Config>(
  {
    ...MaintenanceConfigOpt,
    ...PostgresConfigOpt,
    ...FileConfigOpt,
    ...LogConfigOpt,
  },
  {
    loadFromFileArg: 'config',
  }
);

const logger: Logger = pino({ level: config.logLevel }).child({
  module: 'streams-archive-maintenance',
});

const TABLES = ['probe', 'stream_title', 'stream_game', 'stream_tags'];

const pool: Pool = await initPostgres(config);

function utcDay(offsetDays: number): string {
  const d = new Date();
  d.setUTCHours(0, 0, 0, 0);
  d.setUTCDate(d.getUTCDate() + offsetDays);
  return d.toISOString().substring(0, 10);
}

async function partitionsOf(table: string): Promise<Partition[]> {
  const result = await pool.query(
    `SELECT c.relname AS name, pg_get_expr(c.relpartbound, c.oid) AS bound
     FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
     WHERE i.inhparent = $1::regclass`,
    [table]
  );
  return result.rows.map((r) => {
    const match = /TO \('([^']+)'\)/.exec(r.bound);
    return { name: r.name, upper: match ? new Date(match[1]) : null };
  });
}

// create future partitions, starting after the existing coverage (the
// legacy partition may still cover today)
for (const table of TABLES) {
  const existing = await partitionsOf(table);
  let maxUpper: Date | null = null;
  for (const p of existing) {
    if (p.upper !== null && (maxUpper === null || p.upper > maxUpper)) {
      maxUpper = p.upper;
    }
  }
  let created = 0;
  for (let i = 0; i <= config.daysAhead; ++i) {
    const from = utcDay(i);
    const to = utcDay(i + 1);
    if (maxUpper !== null && new Date(from) < maxUpper) continue;
    const name = `${table}_p${from.replace(/-/g, '')}`;
    // Bounds must carry an explicit UTC offset. A bare date literal is cast
    // using the server's TimeZone, so on a DB behind UTC each partition is
    // shifted and the coverage check above then skips a day, leaving a gap that
    // makes every probe insert in that window fail and is never repaired.
    await pool.query(
      `CREATE TABLE IF NOT EXISTS ${name} PARTITION OF ${table} FOR VALUES FROM ('${from} 00:00:00+00') TO ('${to} 00:00:00+00')`
    );
    ++created;
  }
  logger.info({ table, created }, 'partitions ensured');
}

interface OldestStream {
  stream_id: string;
  started_at: Date;
  updated_at: Date | null;
  ended_at: Date | null;
}

const oldest = await pool.query<OldestStream>(
  'SELECT stream_id, started_at, updated_at, ended_at FROM stream ORDER BY started_at LIMIT 1'
);
const oldestStream: OldestStream | undefined = oldest.rows[0];
const retention = retentionCutoff(new Date(), config.retentionDays);

let kept = 0;
for (const table of TABLES) {
  const plan = planRetention(
    await partitionsOf(table),
    retention,
    oldestStream?.started_at ?? null
  );
  for (const part of plan.drop) {
    await pool.query(`DROP TABLE ${part.name}`);
    logger.info({ table, partition: part.name }, 'partition dropped');
  }
  kept += plan.kept.length;
}

// One warning per run rather than per partition, naming the row responsible.
// While the archiver sweeps, the oldest stream is never older than its
// maxAgeHours, so this means archiving is stuck.
if (kept > 0 && oldestStream !== undefined) {
  logger.warn(
    { ...oldestStream, partitionsKept: kept, retentionCutoff: retention },
    'retention blocked by an unarchived stream, partitions kept'
  );
}

await pool.end();
