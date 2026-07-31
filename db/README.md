# Database migrations (dbmate)

Postgres is the schema source of truth (the prisma schema is introspected via
`prisma db pull`, not migrated). Migrations are applied with
[dbmate](https://github.com/amacneil/dbmate):

```bash
export DATABASE_URL="postgres://user:pass@host:5432/tw_stats?sslmode=disable"
dbmate --migrations-dir db/migrations up
```

## 20260717000000 — base schema

The introspected production schema with `IF NOT EXISTS`: no-op on the old
database, full bootstrap on a fresh one.

## 20260717000001 — partition history tables

Converts `probe`, `stream_title`, `stream_game`, `stream_tags` to
day-partitioned tables. The existing table becomes the `<table>_legacy`
partition covering everything before the cutover date (the day after the
migration runs, UTC). Runs with `transaction:false`; the constraint validation
scans each table once but does not block writers. Daily partitions for the
first week are created immediately; afterwards the `streams-archive`
maintenance job creates and drops them.

## 20260717000002 — archive tables

`archive_stream` (index into the object-storage archive) and `stream_summary`
(per-stream aggregates for site queries).

## 20260721000004 — platform column

Makes every entity platform-scoped so Kick data can live alongside Twitch.
`platform text NOT NULL` is added to each table and folded into the primary
key, and `stream_id` widens from `bigint` to `text` because Kick livestream ids
are UUIDs. `user_id`, `game_id` and `game_ids` stay `bigint` — Kick's are plain
integers, and the platform column already separates the namespaces.

Adding the column is a catalog operation (constant default, PG11+), but the
`stream_id` type change rewrites every table including all partitions of the
history tables — in production that is ~3.5GB / 30M rows on `probe` alone,
under an ACCESS EXCLUSIVE lock, in a single transaction. Measured on a replica:
2M probe rows (192MB) took 7.2s and produced 271MB of WAL, so budget roughly
7–8GB of WAL and several minutes at production scale, and check there is
headroom on the WAL volume before starting.

The `platform` default is dropped at the end of the migration: every writer
sets it explicitly, so a missed code path fails loudly rather than silently
filing Kick rows under `twitch`. That is also why **the old images cannot run
against the new schema** — their inserts omit `platform` and hit a not-null
violation, which for these consumers means `process.exit(1)` and a crashloop.
Every writer must be on the new image before it is scaled back up.

The writers are `streams-process`, `streams-archive` and `missing` — `missing`
maintains `streamers`/`game`, which this migration also alters, so it has to
come down too. `streams` and `kick-streams` only produce to kafka and are
unaffected, but `kick-streams` must not run until `streams-id` and `missing`
are on the new image: an old `streams-id` would send Kick user ids to Helix and
write the resulting Twitch users back under Kick's ids.

Kafka is the write-ahead log for this pipeline, so the consumers can be stopped
for the duration and will replay the backlog afterwards:

```bash
# 1. stop every writer to the tables being altered
kubectl scale -n twstats --replicas=0 \
  deploy/twstats-streams-process deploy/twstats-streams-archive deploy/twstats-missing
# messages queue in kafka while they are down

# 2. optional but worthwhile: the *_legacy partitions are ~1.1GB of the rewrite
#    and hold no rows once the backfill is done (see below)
psql -c 'DROP TABLE probe_legacy, stream_title_legacy, stream_game_legacy, stream_tags_legacy;'

# 3. migrate. lock_timeout keeps a stray session from making this block
#    indefinitely while holding ACCESS EXCLUSIVE
PGOPTIONS='-c lock_timeout=30s' dbmate --migrations-dir db/migrations up

# 4. deploy the new image tag for every service, then scale back up
kubectl scale -n twstats --replicas=1 \
  deploy/twstats-streams-process deploy/twstats-streams-archive deploy/twstats-missing
# the consumers replay the backlog

# 5. only once the above are healthy, enable the kick poller
```

There is no rollback. The down section raises rather than silently succeeding,
because an empty one would let `dbmate rollback` report success and drop the
`schema_migrations` row, after which the next `up` fails with `column
"platform" already exists` and the database is wedged. Recovery is from backup.

The redis cache keys also change: they gain a platform prefix, because Kick
broadcaster and category ids overlap Twitch's id space. The orphaned keys are
harmless and self-healing — `missing` finds no watermark under the new prefix,
falls back to 1970 and reloads the population from postgres rather than from
the API. Clear the old ones afterwards with
`redis-cli --scan --pattern 'user_id_*' | xargs redis-cli del` (likewise
`game_id_*`).

## After the backfill

Once `streams-archive backfill` has drained all historical ended streams and
the few streams that were still live at backfill time have ended and been
archived (verify: `SELECT count(*) FROM stream WHERE started_at < '<cutover>'`
returns 0), drop the legacy partitions manually:

```sql
DROP TABLE probe_legacy, stream_title_legacy, stream_game_legacy, stream_tags_legacy;
```

The maintenance job never touches `*_legacy` partitions.
