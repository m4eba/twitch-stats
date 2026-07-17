-- migrate:up transaction:false

-- Convert history tables to day-partitioned tables. The existing table is
-- renamed to <table>_legacy and attached as the partition for everything
-- before cutover (tomorrow, UTC); daily partitions take over from there.
-- The NOT VALID + VALIDATE dance runs the full-table scan without blocking
-- writers, and lets ATTACH PARTITION skip its own scan.

-- probe ----------------------------------------------------------------------

DO $$
DECLARE cutover date := (now() at time zone 'utc')::date + 1;
BEGIN
  EXECUTE format('ALTER TABLE probe ADD CONSTRAINT probe_legacy_range CHECK ("time" < %L) NOT VALID', cutover);
END $$;

ALTER TABLE probe VALIDATE CONSTRAINT probe_legacy_range;

DO $$
DECLARE
  cutover date := (now() at time zone 'utc')::date + 1;
  pkname text;
BEGIN
  SELECT conname INTO STRICT pkname FROM pg_constraint
    WHERE conrelid = 'probe'::regclass AND contype = 'p';
  ALTER TABLE probe RENAME TO probe_legacy;
  EXECUTE format('ALTER TABLE probe_legacy RENAME CONSTRAINT %I TO probe_legacy_pkey', pkname);
  CREATE TABLE probe (
    stream_id bigint NOT NULL,
    user_id   bigint NOT NULL,
    viewers   integer NOT NULL,
    "time"    timestamptz(6) NOT NULL,
    PRIMARY KEY (stream_id, user_id, "time")
  ) PARTITION BY RANGE ("time");
  EXECUTE format('ALTER TABLE probe ATTACH PARTITION probe_legacy FOR VALUES FROM (MINVALUE) TO (%L)', cutover);
END $$;

-- stream_title ---------------------------------------------------------------

DO $$
DECLARE cutover date := (now() at time zone 'utc')::date + 1;
BEGIN
  EXECUTE format('ALTER TABLE stream_title ADD CONSTRAINT stream_title_legacy_range CHECK ("time" < %L) NOT VALID', cutover);
END $$;

ALTER TABLE stream_title VALIDATE CONSTRAINT stream_title_legacy_range;

DO $$
DECLARE
  cutover date := (now() at time zone 'utc')::date + 1;
  pkname text;
BEGIN
  SELECT conname INTO STRICT pkname FROM pg_constraint
    WHERE conrelid = 'stream_title'::regclass AND contype = 'p';
  ALTER TABLE stream_title RENAME TO stream_title_legacy;
  EXECUTE format('ALTER TABLE stream_title_legacy RENAME CONSTRAINT %I TO stream_title_legacy_pkey', pkname);
  CREATE TABLE stream_title (
    stream_id bigint NOT NULL,
    title     text NOT NULL,
    "time"    timestamptz(6) NOT NULL,
    PRIMARY KEY (stream_id, title, "time")
  ) PARTITION BY RANGE ("time");
  EXECUTE format('ALTER TABLE stream_title ATTACH PARTITION stream_title_legacy FOR VALUES FROM (MINVALUE) TO (%L)', cutover);
END $$;

-- stream_game ----------------------------------------------------------------

DO $$
DECLARE cutover date := (now() at time zone 'utc')::date + 1;
BEGIN
  EXECUTE format('ALTER TABLE stream_game ADD CONSTRAINT stream_game_legacy_range CHECK ("time" < %L) NOT VALID', cutover);
END $$;

ALTER TABLE stream_game VALIDATE CONSTRAINT stream_game_legacy_range;

DO $$
DECLARE
  cutover date := (now() at time zone 'utc')::date + 1;
  pkname text;
BEGIN
  SELECT conname INTO STRICT pkname FROM pg_constraint
    WHERE conrelid = 'stream_game'::regclass AND contype = 'p';
  ALTER TABLE stream_game RENAME TO stream_game_legacy;
  EXECUTE format('ALTER TABLE stream_game_legacy RENAME CONSTRAINT %I TO stream_game_legacy_pkey', pkname);
  CREATE TABLE stream_game (
    stream_id bigint NOT NULL,
    game_id   bigint NOT NULL,
    "time"    timestamptz(6) NOT NULL,
    PRIMARY KEY (stream_id, game_id, "time")
  ) PARTITION BY RANGE ("time");
  EXECUTE format('ALTER TABLE stream_game ATTACH PARTITION stream_game_legacy FOR VALUES FROM (MINVALUE) TO (%L)', cutover);
END $$;

-- stream_tags ----------------------------------------------------------------

DO $$
DECLARE cutover date := (now() at time zone 'utc')::date + 1;
BEGIN
  EXECUTE format('ALTER TABLE stream_tags ADD CONSTRAINT stream_tags_legacy_range CHECK ("time" < %L) NOT VALID', cutover);
END $$;

ALTER TABLE stream_tags VALIDATE CONSTRAINT stream_tags_legacy_range;

DO $$
DECLARE
  cutover date := (now() at time zone 'utc')::date + 1;
  pkname text;
BEGIN
  SELECT conname INTO STRICT pkname FROM pg_constraint
    WHERE conrelid = 'stream_tags'::regclass AND contype = 'p';
  ALTER TABLE stream_tags RENAME TO stream_tags_legacy;
  EXECUTE format('ALTER TABLE stream_tags_legacy RENAME CONSTRAINT %I TO stream_tags_legacy_pkey', pkname);
  CREATE TABLE stream_tags (
    stream_id bigint NOT NULL,
    tag       text[] NOT NULL,
    "time"    timestamptz(6) NOT NULL,
    PRIMARY KEY (stream_id, tag, "time")
  ) PARTITION BY RANGE ("time");
  EXECUTE format('ALTER TABLE stream_tags ATTACH PARTITION stream_tags_legacy FOR VALUES FROM (MINVALUE) TO (%L)', cutover);
END $$;

-- daily partitions for the first week; afterwards created by the
-- streams-archive maintenance job -------------------------------------------

DO $$
DECLARE
  cutover date := (now() at time zone 'utc')::date + 1;
  tbl text;
  day date;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['probe', 'stream_title', 'stream_game', 'stream_tags'] LOOP
    FOR i IN 0..6 LOOP
      day := cutover + i;
      EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        tbl || '_p' || to_char(day, 'YYYYMMDD'), tbl, day, day + 1
      );
    END LOOP;
  END LOOP;
END $$;

-- migrate:down transaction:false

-- WARNING: drops all rows written after the cutover (the daily partitions).
-- Only the legacy partition survives, renamed back to the original table name.

DO $$
DECLARE tbl text;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['probe', 'stream_title', 'stream_game', 'stream_tags'] LOOP
    EXECUTE format('ALTER TABLE %I DETACH PARTITION %I', tbl, tbl || '_legacy');
    EXECUTE format('DROP TABLE %I CASCADE', tbl);
    EXECUTE format('ALTER TABLE %I RENAME TO %I', tbl || '_legacy', tbl);
    EXECUTE format('ALTER TABLE %I RENAME CONSTRAINT %I TO %I', tbl, tbl || '_legacy_pkey', tbl || '_pkey');
    EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', tbl, tbl || '_legacy_range');
  END LOOP;
END $$;
