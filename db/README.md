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
under an ACCESS EXCLUSIVE lock. Kafka is the write-ahead log for this pipeline,
so the safe procedure is:

```bash
kubectl scale -n twstats deploy/twstats-streams-process --replicas=0
kubectl scale -n twstats deploy/twstats-streams-archive --replicas=0
# messages queue in kafka while the consumers are down

# optional but worthwhile: the *_legacy partitions are ~1.1GB of the rewrite
# and hold no rows once the backfill is done (see below)
psql -c 'DROP TABLE probe_legacy, stream_title_legacy, stream_game_legacy, stream_tags_legacy;'

dbmate --migrations-dir db/migrations up

kubectl scale -n twstats deploy/twstats-streams-process --replicas=1
kubectl scale -n twstats deploy/twstats-streams-archive --replicas=1
# the consumers replay the backlog
```

The `platform` default is dropped at the end of the migration: every writer
sets it explicitly, so a missed code path should fail loudly rather than
silently file Kick rows under `twitch`.

## After the backfill

Once `streams-archive backfill` has drained all historical ended streams and
the few streams that were still live at backfill time have ended and been
archived (verify: `SELECT count(*) FROM stream WHERE started_at < '<cutover>'`
returns 0), drop the legacy partitions manually:

```sql
DROP TABLE probe_legacy, stream_title_legacy, stream_game_legacy, stream_tags_legacy;
```

The maintenance job never touches `*_legacy` partitions.
