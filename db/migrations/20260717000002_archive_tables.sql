-- migrate:up

-- Archive index + per-stream summary written by the streams-archive bot.

-- Points into the object-storage archive: the stream's JSON document is at
-- bytes [byte_offset, byte_offset + byte_length) of object_key (one gzip
-- member per document, readable with a single range GET).
CREATE TABLE archive_stream (
  stream_id   bigint PRIMARY KEY,
  user_id     bigint NOT NULL,
  started_at  timestamptz(6) NOT NULL,
  ended_at    timestamptz(6) NOT NULL,
  object_key  text NOT NULL,
  byte_offset bigint NOT NULL,
  byte_length integer NOT NULL
);
CREATE INDEX archive_stream_user_idx ON archive_stream (user_id, started_at);

CREATE TABLE stream_summary (
  stream_id        bigint PRIMARY KEY,
  user_id          bigint NOT NULL,
  started_at       timestamptz(6) NOT NULL,
  ended_at         timestamptz(6) NOT NULL,
  duration_seconds integer NOT NULL,
  avg_viewers      real NOT NULL,
  peak_viewers     integer NOT NULL,
  probe_count      integer NOT NULL,
  game_ids         bigint[] NOT NULL,
  title_count      integer NOT NULL
);
CREATE INDEX stream_summary_user_idx ON stream_summary (user_id, started_at);
CREATE INDEX stream_summary_started_idx ON stream_summary (started_at);

-- migrate:down

DROP TABLE stream_summary;
DROP TABLE archive_stream;
