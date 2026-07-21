-- migrate:up

-- Make every entity platform-scoped so Kick data can live alongside Twitch.
--
-- Two changes, applied together:
--
--   1. `platform text NOT NULL DEFAULT 'twitch'` on every table, added to the
--      primary key. Kick broadcaster ids and category ids are plain integers
--      that overlap Twitch's id space, so without a discriminator Kick user
--      12345 and Twitch user 12345 are the same row - silent corruption rather
--      than an error. Adding the column with a constant default is a catalog
--      operation in PG11+, so it does not rewrite the table.
--
--   2. `stream_id bigint -> text`. Kick livestream ids are UUIDs
--      (endpoints.LivestreamV2.id, format: uuid) and do not fit in a bigint.
--      This one DOES rewrite, including every partition of the history tables.
--      user_id, game_id and game_ids stay bigint - Kick's are integers, and
--      the platform column already separates the namespaces.
--
-- The default is dropped at the end: every writer sets platform explicitly, so
-- a missed code path should fail loudly rather than silently file Kick rows
-- under 'twitch'. Existing rows keep their value (PG stores it as a missing
-- value in pg_attribute, unaffected by DROP DEFAULT).
--
-- OPERATIONAL NOTE: the stream_id rewrite takes an ACCESS EXCLUSIVE lock on
-- probe (~3.5GB / 30M rows in production) for the duration. Kafka is the
-- write-ahead log for this pipeline, so the procedure is:
--
--   1. scale streams-process and streams-archive to 0 (messages queue in kafka)
--   2. optionally DROP the *_legacy partitions first if the backfill is done
--      (see db/README.md) - they are ~1.1GB of the rewrite and hold no rows
--   3. dbmate up
--   4. scale back up; the consumers replay the backlog
--
-- Each PK is dropped before the type change and recreated after, so the index
-- is built once rather than rebuilt by the rewrite and then replaced.

-- game ------------------------------------------------------------------------
ALTER TABLE game ADD COLUMN platform text NOT NULL DEFAULT 'twitch';
ALTER TABLE game DROP CONSTRAINT game_pkey;
ALTER TABLE game ADD PRIMARY KEY (platform, game_id);

-- streamers -------------------------------------------------------------------
ALTER TABLE streamers ADD COLUMN platform text NOT NULL DEFAULT 'twitch';
ALTER TABLE streamers DROP CONSTRAINT streamers_pkey;
ALTER TABLE streamers ADD PRIMARY KEY (platform, user_id);
DROP INDEX IF EXISTS streamers_login_idx;
CREATE INDEX streamers_login_idx ON streamers (platform, login);

ALTER TABLE streamers_follower_probe ADD COLUMN platform text NOT NULL DEFAULT 'twitch';
ALTER TABLE streamers_follower_probe DROP CONSTRAINT streamers_follower_probe_pkey;
ALTER TABLE streamers_follower_probe ADD PRIMARY KEY (platform, user_id, "time");

ALTER TABLE streamers_views_probe ADD COLUMN platform text NOT NULL DEFAULT 'twitch';
ALTER TABLE streamers_views_probe DROP CONSTRAINT streamers_views_probe_pkey;
ALTER TABLE streamers_views_probe ADD PRIMARY KEY (platform, user_id, "time");

-- stream ----------------------------------------------------------------------
ALTER TABLE stream ADD COLUMN platform text NOT NULL DEFAULT 'twitch';
ALTER TABLE stream DROP CONSTRAINT stream_pkey;
ALTER TABLE stream ALTER COLUMN stream_id TYPE text;
ALTER TABLE stream ADD PRIMARY KEY (platform, stream_id);
DROP INDEX IF EXISTS stream_user_id_idx;
CREATE INDEX stream_user_id_idx ON stream (platform, user_id);

-- user_online -----------------------------------------------------------------
ALTER TABLE user_online ADD COLUMN platform text NOT NULL DEFAULT 'twitch';
ALTER TABLE user_online DROP CONSTRAINT user_online_pkey;
ALTER TABLE user_online ALTER COLUMN stream_id TYPE text;
ALTER TABLE user_online ADD PRIMARY KEY (platform, stream_id, user_id);

-- partitioned history tables --------------------------------------------------
-- ALTER on the partitioned parent cascades to every partition, including the
-- *_legacy ones. This is the expensive part.

ALTER TABLE probe ADD COLUMN platform text NOT NULL DEFAULT 'twitch';
ALTER TABLE probe DROP CONSTRAINT probe_pkey;
ALTER TABLE probe ALTER COLUMN stream_id TYPE text;
ALTER TABLE probe ADD PRIMARY KEY (platform, stream_id, user_id, "time");

ALTER TABLE stream_game ADD COLUMN platform text NOT NULL DEFAULT 'twitch';
ALTER TABLE stream_game DROP CONSTRAINT stream_game_pkey;
ALTER TABLE stream_game ALTER COLUMN stream_id TYPE text;
ALTER TABLE stream_game ADD PRIMARY KEY (platform, stream_id, game_id, "time");

ALTER TABLE stream_tags ADD COLUMN platform text NOT NULL DEFAULT 'twitch';
ALTER TABLE stream_tags DROP CONSTRAINT stream_tags_pkey;
ALTER TABLE stream_tags ALTER COLUMN stream_id TYPE text;
ALTER TABLE stream_tags ADD PRIMARY KEY (platform, stream_id, tag, "time");

ALTER TABLE stream_title ADD COLUMN platform text NOT NULL DEFAULT 'twitch';
ALTER TABLE stream_title DROP CONSTRAINT stream_title_pkey;
ALTER TABLE stream_title ALTER COLUMN stream_id TYPE text;
ALTER TABLE stream_title ADD PRIMARY KEY (platform, stream_id, title, "time");

-- archive ---------------------------------------------------------------------
ALTER TABLE archive_stream ADD COLUMN platform text NOT NULL DEFAULT 'twitch';
ALTER TABLE archive_stream DROP CONSTRAINT archive_stream_pkey;
ALTER TABLE archive_stream ALTER COLUMN stream_id TYPE text;
ALTER TABLE archive_stream ADD PRIMARY KEY (platform, stream_id);
DROP INDEX IF EXISTS archive_stream_user_started_idx;
CREATE INDEX archive_stream_user_started_idx ON archive_stream (platform, user_id, started_at);

ALTER TABLE stream_summary ADD COLUMN platform text NOT NULL DEFAULT 'twitch';
ALTER TABLE stream_summary DROP CONSTRAINT stream_summary_pkey;
ALTER TABLE stream_summary ALTER COLUMN stream_id TYPE text;
ALTER TABLE stream_summary ADD PRIMARY KEY (platform, stream_id);
DROP INDEX IF EXISTS stream_summary_user_started_idx;
CREATE INDEX stream_summary_user_started_idx ON stream_summary (platform, user_id, started_at);

-- Force every writer to be explicit from here on.
ALTER TABLE game                     ALTER COLUMN platform DROP DEFAULT;
ALTER TABLE streamers                ALTER COLUMN platform DROP DEFAULT;
ALTER TABLE streamers_follower_probe ALTER COLUMN platform DROP DEFAULT;
ALTER TABLE streamers_views_probe    ALTER COLUMN platform DROP DEFAULT;
ALTER TABLE stream                   ALTER COLUMN platform DROP DEFAULT;
ALTER TABLE user_online              ALTER COLUMN platform DROP DEFAULT;
ALTER TABLE probe                    ALTER COLUMN platform DROP DEFAULT;
ALTER TABLE stream_game              ALTER COLUMN platform DROP DEFAULT;
ALTER TABLE stream_tags              ALTER COLUMN platform DROP DEFAULT;
ALTER TABLE stream_title             ALTER COLUMN platform DROP DEFAULT;
ALTER TABLE archive_stream           ALTER COLUMN platform DROP DEFAULT;
ALTER TABLE stream_summary           ALTER COLUMN platform DROP DEFAULT;

-- migrate:down

-- intentionally empty: reverting would have to pick one platform's rows to
-- keep and narrow stream_id back to bigint, which is lossy by construction
