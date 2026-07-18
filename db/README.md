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

## After the backfill

Once `streams-archive backfill` has drained all historical ended streams and
the few streams that were still live at backfill time have ended and been
archived (verify: `SELECT count(*) FROM stream WHERE started_at < '<cutover>'`
returns 0), drop the legacy partitions manually:

```sql
DROP TABLE probe_legacy, stream_title_legacy, stream_game_legacy, stream_tags_legacy;
```

The maintenance job never touches `*_legacy` partitions.
