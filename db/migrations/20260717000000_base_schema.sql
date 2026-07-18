-- migrate:up

-- Base schema as introspected from the original production database
-- (libraries/prisma/prisma/stats_schema.prisma). IF NOT EXISTS so this
-- no-ops on a database that predates dbmate; on a fresh database it
-- bootstraps everything the partitioning migration converts.

CREATE TABLE IF NOT EXISTS game (
  game_id     bigint PRIMARY KEY,
  name        text NOT NULL,
  box_art_url text NOT NULL,
  updated_at  timestamptz(6) NOT NULL
);

CREATE TABLE IF NOT EXISTS probe (
  stream_id bigint NOT NULL,
  user_id   bigint NOT NULL,
  viewers   integer NOT NULL,
  "time"    timestamptz(6) NOT NULL,
  PRIMARY KEY (stream_id, user_id, "time")
);

CREATE TABLE IF NOT EXISTS stream (
  stream_id  bigint PRIMARY KEY,
  user_id    bigint NOT NULL,
  title      text NOT NULL,
  tags       text[],
  game_id    bigint NOT NULL,
  started_at timestamptz(6) NOT NULL,
  ended_at   timestamptz(6),
  updated_at timestamptz(6)
);
CREATE INDEX IF NOT EXISTS stream_ended_at_idx ON stream (ended_at);
CREATE INDEX IF NOT EXISTS stream_started_at_idx ON stream (started_at);
CREATE INDEX IF NOT EXISTS stream_updated_at_idx ON stream (updated_at);
CREATE INDEX IF NOT EXISTS stream_user_id_idx ON stream (user_id);

CREATE TABLE IF NOT EXISTS stream_game (
  stream_id bigint NOT NULL,
  game_id   bigint NOT NULL,
  "time"    timestamptz(6) NOT NULL,
  PRIMARY KEY (stream_id, game_id, "time")
);

CREATE TABLE IF NOT EXISTS stream_tags (
  stream_id bigint NOT NULL,
  tag       text[] NOT NULL,
  "time"    timestamptz(6) NOT NULL,
  PRIMARY KEY (stream_id, tag, "time")
);

CREATE TABLE IF NOT EXISTS stream_title (
  stream_id bigint NOT NULL,
  title     text NOT NULL,
  "time"    timestamptz(6) NOT NULL,
  PRIMARY KEY (stream_id, title, "time")
);

CREATE TABLE IF NOT EXISTS streamers (
  user_id          bigint PRIMARY KEY,
  login            text NOT NULL,
  display_name     text NOT NULL,
  type             text NOT NULL,
  profile_image    text NOT NULL,
  view_count       bigint NOT NULL DEFAULT 0,
  follower         bigint NOT NULL DEFAULT 0,
  broadcaster_type text NOT NULL,
  created_at       timestamptz(6),
  vods_updated_at  timestamptz(6),
  vods_forever     boolean NOT NULL DEFAULT false,
  updated_at       timestamptz(6)
);
CREATE INDEX IF NOT EXISTS streamers_login_idx ON streamers (login);
CREATE INDEX IF NOT EXISTS streamers_updated_at_idx ON streamers (updated_at);
CREATE INDEX IF NOT EXISTS streamers_vods_updated_at_idx ON streamers (vods_updated_at);

CREATE TABLE IF NOT EXISTS streamers_follower_probe (
  user_id  bigint NOT NULL,
  follower bigint NOT NULL,
  "time"   timestamptz(6) NOT NULL,
  PRIMARY KEY (user_id, "time")
);

CREATE TABLE IF NOT EXISTS streamers_views_probe (
  user_id    bigint NOT NULL,
  view_count bigint NOT NULL,
  "time"     timestamptz(6) NOT NULL,
  PRIMARY KEY (user_id, "time")
);

CREATE TABLE IF NOT EXISTS tags (
  tag_id                    uuid PRIMARY KEY,
  is_auto                   boolean NOT NULL,
  localization_names        text NOT NULL,
  localization_descriptions text NOT NULL,
  updated_at                timestamptz(6) NOT NULL
);

CREATE TABLE IF NOT EXISTS user_online (
  user_id     bigint NOT NULL,
  stream_id   bigint NOT NULL,
  last_update timestamptz(6) NOT NULL,
  PRIMARY KEY (stream_id, user_id)
);
CREATE INDEX IF NOT EXISTS user_online_last_update_idx ON user_online (last_update);

-- migrate:down

-- intentionally empty: never drop the base tables on rollback
