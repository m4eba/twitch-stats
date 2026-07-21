// Partition maintenance for the day-partitioned history tables: creates
// partitions daysAhead into the future and drops partitions older than
// retentionDays. A partition is never dropped while a still-live stream
// started before its upper bound (its probes would be lost before archiving);
// *_legacy partitions are never touched (dropped manually after backfill).
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

interface Partition {
  name: string;
  upper: Date | null;
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

// The oldest stream still in the hot store limits what can be dropped. Every
// row left in `stream` is by definition unarchived - the archiver deletes on
// success - so ended-but-not-yet-archived streams must count too. Restricting
// this to ended_at IS NULL let a lagging or crashed archiver have its probe
// history dropped out from under it, archiving those streams with probe_count 0.
const live = await pool.query(
  'SELECT min(started_at) AS min_started FROM stream'
);
const minLiveStarted: Date | null = live.rows[0].min_started;

const retentionCutoff = new Date();
retentionCutoff.setUTCHours(0, 0, 0, 0);
retentionCutoff.setUTCDate(retentionCutoff.getUTCDate() - config.retentionDays);
const cutoff =
  minLiveStarted !== null && minLiveStarted < retentionCutoff
    ? minLiveStarted
    : retentionCutoff;

for (const table of TABLES) {
  for (const part of await partitionsOf(table)) {
    if (part.name.endsWith('_legacy')) continue;
    if (part.upper === null) continue;
    if (part.upper <= cutoff) {
      await pool.query(`DROP TABLE ${part.name}`);
      logger.info({ table, partition: part.name }, 'partition dropped');
    } else if (part.upper <= retentionCutoff) {
      logger.warn(
        { table, partition: part.name, minLiveStarted },
        'partition kept, long-running live stream overlaps it'
      );
    }
  }
}

await pool.end();
