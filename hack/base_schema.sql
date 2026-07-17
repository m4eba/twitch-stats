-- Pre-migration schema for local testing: the tables the pipeline writes,
-- as they exist in production BEFORE the partitioning migration runs.
-- (Production's schema is the source of truth; this is only for a fresh
-- local database so the dbmate migrations have something to convert.)

CREATE TABLE probe (
  stream_id bigint NOT NULL,
  user_id   bigint NOT NULL,
  viewers   integer NOT NULL,
  "time"    timestamptz(6) NOT NULL,
  PRIMARY KEY (stream_id, user_id, "time")
);

CREATE TABLE stream (
  stream_id  bigint PRIMARY KEY,
  user_id    bigint NOT NULL,
  title      text NOT NULL,
  tags       text[],
  game_id    bigint NOT NULL,
  started_at timestamptz(6) NOT NULL,
  ended_at   timestamptz(6),
  updated_at timestamptz(6)
);
CREATE INDEX ON stream (ended_at);
CREATE INDEX ON stream (started_at);
CREATE INDEX ON stream (updated_at);
CREATE INDEX ON stream (user_id);

CREATE TABLE stream_game (
  stream_id bigint NOT NULL,
  game_id   bigint NOT NULL,
  "time"    timestamptz(6) NOT NULL,
  PRIMARY KEY (stream_id, game_id, "time")
);

CREATE TABLE stream_tags (
  stream_id bigint NOT NULL,
  tag       text[] NOT NULL,
  "time"    timestamptz(6) NOT NULL,
  PRIMARY KEY (stream_id, tag, "time")
);

CREATE TABLE stream_title (
  stream_id bigint NOT NULL,
  title     text NOT NULL,
  "time"    timestamptz(6) NOT NULL,
  PRIMARY KEY (stream_id, title, "time")
);

CREATE TABLE user_online (
  user_id     bigint NOT NULL,
  stream_id   bigint NOT NULL,
  last_update timestamptz(6) NOT NULL,
  PRIMARY KEY (stream_id, user_id)
);
CREATE INDEX ON user_online (last_update);
