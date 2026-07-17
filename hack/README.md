# Local end-to-end test

Runs the archive pipeline against docker-compose (Postgres 5447, Kafka 19000,
MinIO 9008/9009 — registered in ~/dev/sessions/PORT_REGISTRY.md).

```bash
cd hack
docker compose up -d

# schema: production base tables, then the dbmate migrations
psql postgres://postgres:password@localhost:5447/tw_stats -f base_schema.sql
DATABASE_URL='postgres://postgres:password@localhost:5447/tw_stats?sslmode=disable' \
  dbmate --migrations-dir ../db/migrations --no-dump-schema up

# bucket
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  aws --endpoint-url http://localhost:9008 --region auto s3 mb s3://twstats-archive

# seed fake crawl runs (4 polls; carol + bob end, alice stays live)
node seed.mjs

# process them into postgres (needs real twitch creds for the startup
# token fetch only; no API calls happen). Ctrl-C when idle.
node ../bot/streams-process/dist/index.js \
  --twitchClientId "$TW_CLIENT_ID" --twitchClientSecret "$TW_CLIENT_SECRET" \
  --kafkaBroker localhost:19000 \
  --pgHost localhost --pgPort 5447 --pgDatabase tw_stats \
  --pgUser postgres --pgPassword password

# archive the ended streams. Ctrl-C after the 'flushed' log line.
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
node ../bot/streams-archive/dist/index.js \
  --kafkaBroker localhost:19000 \
  --pgHost localhost --pgPort 5447 --pgDatabase tw_stats \
  --pgUser postgres --pgPassword password \
  --s3Endpoint http://localhost:9008 --s3Bucket twstats-archive \
  --graceSeconds 0 --flushIntervalSeconds 5

# check results: range GETs against minio, summaries, hot-store deletes
node verify.mjs

docker compose down -v
```

`maintenance.js` and `backfill.js` can be pointed at the same database with
the flags above to test partition creation/retention and the backfill path.
