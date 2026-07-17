import type { Pool, PoolClient } from 'pg';
import type { Logger } from 'pino';
import type { S3Client } from '@aws-sdk/client-s3';
import { ChunkBuffer, chunkKey, putChunk } from '@twitch-stats/storage';
import { buildMultiInsert } from '@twitch-stats/database';

interface StreamRow {
  stream_id: string;
  user_id: string;
  title: string;
  tags: string[] | null;
  game_id: string;
  started_at: Date;
  ended_at: Date;
  updated_at: Date | null;
}

interface IndexRow {
  stream_id: string;
  user_id: string;
  started_at: Date;
  ended_at: Date;
  offset: number;
  length: number;
}

interface SummaryRow {
  stream_id: string;
  user_id: string;
  started_at: Date;
  ended_at: Date;
  duration_seconds: number;
  avg_viewers: number;
  peak_viewers: number;
  probe_count: number;
  game_ids: string[];
  title_count: number;
}

const SELECT_BATCH = 500;
const INSERT_BATCH = 5000;

export class Archiver {
  private log: Logger;
  private pool: Pool;
  private s3: S3Client;
  private bucket: string;
  private keyPrefix: string;

  private buffer: ChunkBuffer = new ChunkBuffer();
  private index: IndexRow[] = [];
  private summaries: SummaryRow[] = [];

  public constructor(
    log: Logger,
    pool: Pool,
    s3: S3Client,
    bucket: string,
    keyPrefix: string
  ) {
    this.log = log;
    this.pool = pool;
    this.s3 = s3;
    this.bucket = bucket;
    this.keyPrefix = keyPrefix;
  }

  public get bufferedBytes(): number {
    return this.buffer.byteLength;
  }

  public get bufferedCount(): number {
    return this.buffer.count;
  }

  public get bufferAgeMs(): number {
    return this.buffer.ageMs;
  }

  // fetch history for the given streams and add one document per stream
  // to the buffer; streams that resumed (ended_at cleared) or are already
  // gone are skipped
  public async collect(streamIds: string[]): Promise<number> {
    let collected = 0;
    for (let i = 0; i < streamIds.length; i += SELECT_BATCH) {
      const chunk = streamIds.slice(i, i + SELECT_BATCH);
      collected += await this.collectChunk(chunk);
    }
    return collected;
  }

  private async collectChunk(streamIds: string[]): Promise<number> {
    const streams = await this.pool.query<StreamRow>(
      'SELECT * FROM stream WHERE stream_id = ANY($1::bigint[]) AND ended_at IS NOT NULL',
      [streamIds]
    );
    if (streams.rows.length === 0) return 0;
    const ids = streams.rows.map((s) => s.stream_id);

    const [probes, titles, games, tags] = await Promise.all([
      this.pool.query(
        'SELECT stream_id, viewers, time FROM probe WHERE stream_id = ANY($1::bigint[]) ORDER BY time',
        [ids]
      ),
      this.pool.query(
        'SELECT stream_id, title, time FROM stream_title WHERE stream_id = ANY($1::bigint[]) ORDER BY time',
        [ids]
      ),
      this.pool.query(
        'SELECT stream_id, game_id, time FROM stream_game WHERE stream_id = ANY($1::bigint[]) ORDER BY time',
        [ids]
      ),
      this.pool.query(
        'SELECT stream_id, tag, time FROM stream_tags WHERE stream_id = ANY($1::bigint[]) ORDER BY time',
        [ids]
      ),
    ]);

    const group = <T extends { stream_id: string }>(
      rows: T[]
    ): Map<string, T[]> => {
      const map = new Map<string, T[]>();
      for (const row of rows) {
        const list = map.get(row.stream_id);
        if (list) {
          list.push(row);
        } else {
          map.set(row.stream_id, [row]);
        }
      }
      return map;
    };

    const probeMap = group(probes.rows);
    const titleMap = group(titles.rows);
    const gameMap = group(games.rows);
    const tagMap = group(tags.rows);

    for (const stream of streams.rows) {
      const sProbes = probeMap.get(stream.stream_id) ?? [];
      const sTitles = titleMap.get(stream.stream_id) ?? [];
      const sGames = gameMap.get(stream.stream_id) ?? [];
      const sTags = tagMap.get(stream.stream_id) ?? [];

      const doc = {
        stream_id: stream.stream_id,
        user_id: stream.user_id,
        title: stream.title,
        tags: stream.tags,
        game_id: stream.game_id,
        started_at: stream.started_at.toISOString(),
        ended_at: stream.ended_at.toISOString(),
        viewers: sProbes.map((p) => [p.time.toISOString(), p.viewers]),
        titles: sTitles.map((t) => ({
          time: t.time.toISOString(),
          title: t.title,
        })),
        games: sGames.map((g) => ({
          time: g.time.toISOString(),
          game_id: g.game_id,
        })),
        tags_history: sTags.map((t) => ({
          time: t.time.toISOString(),
          tags: t.tag,
        })),
      };

      const entry = await this.buffer.add(JSON.stringify(doc));
      this.index.push({
        stream_id: stream.stream_id,
        user_id: stream.user_id,
        started_at: stream.started_at,
        ended_at: stream.ended_at,
        offset: entry.offset,
        length: entry.length,
      });

      let peak = 0;
      let sum = 0;
      for (const p of sProbes) {
        if (p.viewers > peak) peak = p.viewers;
        sum += p.viewers;
      }
      const gameIds = [...new Set(sGames.map((g) => String(g.game_id)))];
      if (gameIds.length === 0) gameIds.push(String(stream.game_id));
      this.summaries.push({
        stream_id: stream.stream_id,
        user_id: stream.user_id,
        started_at: stream.started_at,
        ended_at: stream.ended_at,
        duration_seconds: Math.max(
          0,
          Math.round(
            (stream.ended_at.getTime() - stream.started_at.getTime()) / 1000
          )
        ),
        avg_viewers: sProbes.length > 0 ? sum / sProbes.length : 0,
        peak_viewers: peak,
        probe_count: sProbes.length,
        game_ids: gameIds,
        title_count: sTitles.length,
      });
    }
    return streams.rows.length;
  }

  // upload the buffered documents as one chunk object, then index them and
  // remove the archived streams from the hot store in a single transaction
  public async flush(): Promise<number> {
    if (this.buffer.count === 0) return 0;
    const count = this.buffer.count;
    const key = chunkKey(this.keyPrefix, new Date());

    await putChunk(this.s3, this.bucket, key, this.buffer.concat());
    this.log.info(
      { key, streams: count, bytes: this.buffer.byteLength },
      'chunk uploaded'
    );

    const archivedIds = this.index.map((r) => r.stream_id);
    const client: PoolClient = await this.pool.connect();
    try {
      await client.query('BEGIN');
      for (let i = 0; i < this.index.length; i += INSERT_BATCH) {
        const insert = buildMultiInsert<IndexRow>(
          'INSERT INTO archive_stream (stream_id,user_id,started_at,ended_at,object_key,byte_offset,byte_length) VALUES ',
          '$1,$2,$3,$4,$5,$6,$7',
          this.index.slice(i, i + INSERT_BATCH),
          (r: IndexRow) => [
            r.stream_id,
            r.user_id,
            r.started_at,
            r.ended_at,
            key,
            r.offset,
            r.length,
          ]
        );
        insert.text += ' ON CONFLICT (stream_id) DO NOTHING';
        await client.query(insert);
      }
      for (let i = 0; i < this.summaries.length; i += INSERT_BATCH) {
        const insert = buildMultiInsert<SummaryRow>(
          'INSERT INTO stream_summary (stream_id,user_id,started_at,ended_at,duration_seconds,avg_viewers,peak_viewers,probe_count,game_ids,title_count) VALUES ',
          '$1,$2,$3,$4,$5,$6,$7,$8,$9,$10',
          this.summaries.slice(i, i + INSERT_BATCH),
          (r: SummaryRow) => [
            r.stream_id,
            r.user_id,
            r.started_at,
            r.ended_at,
            r.duration_seconds,
            r.avg_viewers,
            r.peak_viewers,
            r.probe_count,
            r.game_ids,
            r.title_count,
          ]
        );
        insert.text += ' ON CONFLICT (stream_id) DO NOTHING';
        await client.query(insert);
      }
      await client.query(
        'DELETE FROM stream WHERE stream_id = ANY($1::bigint[])',
        [archivedIds]
      );
      await client.query(
        'DELETE FROM user_online WHERE stream_id = ANY($1::bigint[])',
        [archivedIds]
      );
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }

    this.buffer.reset();
    this.index = [];
    this.summaries = [];
    return count;
  }
}
