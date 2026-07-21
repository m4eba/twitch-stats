import type {
  Platform,
  Stream,
  StreamsByIdMessage,
  StreamEndedMessage,
  EndedStream,
  ProcessingEndConfig,
} from '@twitch-stats/twitch';
import type { Pool } from 'pg';
import moment from 'moment';
import type { Logger } from 'pino';
import type { Producer } from 'kafkajs';
import { buildMultiInsert, buildInList, Query } from '@twitch-stats/database';

// max streams per kafka message when announcing ended streams
const END_CHUNK = 1000;

interface UserOnlineRow {
  user_id: string;
  stream_id: string;
  last_update: string;
}

interface StreamIdTag {
  stream_id: string;
  tag: string[];
}

interface DBStream {
  stream_id: string;
  user_id: string;
  title: string;
  tags?: string[];
  game_id: string;
  started_at: string;
  ended_at: string;
  updated_at: string;
}

interface Split {
  new: {
    ids: Array<string>;
    data: Array<Stream>;
  };
  old: {
    ids: Array<string>;
    data: Array<Stream>;
  };
  query: { [id: string]: DBStream };
}

interface Changed {
  title: Array<Stream>;
  game: Array<Stream>;
  tags: Array<Stream>;
}

export default class Processing {
  private log: Logger;
  private pool: Pool;
  private producer: Producer;
  private streamIdTopic: string;
  private streamEndedTopic: string;

  public constructor(
    log: Logger,
    pool: Pool,
    producer: Producer,
    streamIdTopic: string,
    streamEndedTopic: string
  ) {
    this.log = log;
    this.pool = pool;
    this.producer = producer;
    this.streamIdTopic = streamIdTopic;
    this.streamEndedTopic = streamEndedTopic;
  }

  /* eslint-disable @typescript-eslint/no-explicit-any */
  private async query(query: Query): Promise<any> {
    try {
      // must be awaited inside the try, otherwise a rejected query escapes as a
      // returned promise and the catch below never runs
      return await this.pool.query(query);
    } catch (e) {
      this.log.error({ query, error: e }, 'query error');
      // The rethrow is load-bearing: do not turn this into a log-and-continue.
      // queryMulti splits a batch into several statements, and processEnd
      // deletes user_online only after the update and the kafka sends. If a
      // failure were swallowed here, the remaining chunks would still run and
      // the DELETE would still fire, permanently losing the streams that never
      // got an ended_at. Propagating leaves the kafka offset uncommitted so the
      // batch is redelivered and replays cleanly.
      throw e;
    }
  }

  // Postgres encodes the bind-parameter count as an int16, so one statement can
  // carry at most 65535 parameters. buildMultiInsert will happily build past
  // that: the count silently wraps, the wire protocol desyncs and the server
  // rejects the bind with 08P01
  // ("bind message has N parameter formats but 0 parameters").
  //
  // This is unrecoverable on its own — the consumer dies before committing its
  // offset, Kafka redelivers the same batch, and it loops forever. processEnd
  // hit it with 39824 stale streams x 2 params = 79648 (79648 - 65536 = 14112,
  // the number in the error) and crashlooped 1321 times over 11 days.
  //
  // Every buildMultiInsert caller goes through here so the row count can never
  // decide whether the query is valid. splitNewAndOld is the one multi-row
  // statement not covered: it builds an IN-list via buildInList at one
  // parameter per row, and is bounded by the producer's page size, so it cannot
  // reach the limit. If that ever becomes unbounded it needs the same
  // treatment.
  private static readonly MAX_BIND_PARAMS = 60000;

  private async queryMulti<T>(
    stmt: string,
    template: string,
    data: T[],
    mapping: (d: T) => Array<any>,
    suffix = ''
  ): Promise<void> {
    if (data.length === 0) return;
    // same way buildMultiInsert derives the column count from the template
    const perRow = template.split(/\$\d+/).length - 1;
    const chunkSize = Math.max(
      1,
      Math.floor(Processing.MAX_BIND_PARAMS / Math.max(1, perRow))
    );
    for (let i = 0; i < data.length; i += chunkSize) {
      const query = buildMultiInsert<T>(
        stmt,
        template,
        data.slice(i, i + chunkSize),
        mapping
      );
      query.text += suffix;
      await this.query(query);
    }
  }

  private assureGameId(game_id: string): string {
    if (!game_id) return '0';
    if (game_id.length === 0) return '0';
    if (!/^\d+$/.test(game_id)) return '0';
    return game_id;
  }

  public async processStreams(
    platform: Platform,
    time: Date,
    data2: Stream[]
  ): Promise<void> {
    if (data2.length === 0) return Promise.resolve();

    /* filter out this test data
     
    {"id":"2","user_id":"testDocumentId2","user_login":"testDocumentName2","user_name":"","game_id":"2","game_name":"","type":"live","title":"","viewer_count":1000,"started_at":"1970-01-01T00:00:02Z","language":"testBroadcasterLanguage2","thumbnail_url":"https://static-cdn.jtvnw.net/previews-ttv/live_user_testDocumentName2-{width}x{height}.jpg","tag_ids":[],"tags":null,"is_mature":false}
    */
    const data = data2.filter((d) => d.user_id !== 'testDocumentId2');
    // the filter above can empty a page that was non-empty on entry, and
    // splitNewAndOld builds an `IN ()` list that is invalid SQL when empty
    if (data.length === 0) return;

    this.log.debug({ platform, time, data }, 'process streams');
    await this.insertProbes(platform, data, time);

    // insert into live
    await this.insertLiveStreams(platform, data, time);

    // we need to select all streams before updating them
    // to know what is new and old for the game_id and title tags
    const split: Split = await this.splitNewAndOld(platform, data);
    // insert new streams, update old ones
    await this.insertUpdateStreams(platform, data, time);

    // insert new game and title tags
    await this.insertStreamsGames(platform, split.new.data, time);
    await this.insertStreamsTitles(platform, split.new.data, time);
    await this.insertStreamsTags(platform, split.new.data, time);

    // look through the select we made before the insert
    // and find streams with changed title or game
    const change = this.changedStreams(split);
    await this.insertStreamsGames(platform, change.game, time);
    await this.insertStreamsTitles(platform, change.title, time);
    await this.insertStreamsTags(platform, change.tags, time);
  }

  public async processEnd(
    platform: Platform,
    endConfig: ProcessingEndConfig
  ): Promise<void> {
    this.log.debug({ platform, endConfig }, 'endStream');
    // Get all live streams of THIS platform not updated since the start of the
    // batch. The platform predicate is load-bearing: a sweep only covers its
    // own platform, so without it the kick poller's sentinel would mark every
    // live twitch stream as ended, and vice versa.
    const result = await this.pool.query(
      'SELECT * FROM user_online WHERE platform = $1 AND last_update < $2',
      [platform, endConfig.updateStartTime]
    );
    this.log.debug(
      { count: result.rows.length },
      'streams not update since batch start count'
    );

    const rows: UserOnlineRow[] = result.rows;
    const ended: EndedStream[] = rows.map((r) => ({
      stream_id: r.stream_id,
      user_id: r.user_id,
      // add 5 minutes to last update
      ended_at: moment(r.last_update).add(5, 'minutes').format(),
    }));

    if (ended.length === 0) return;

    await this.queryMulti<EndedStream>(
      'UPDATE stream AS s SET ended_at = v.ended_at, updated_at = v.ended_at FROM (VALUES ',
      '$1::text,$2::text,$3::timestamptz',
      ended,
      (d: EndedStream) => [platform, d.stream_id, d.ended_at],
      ') AS v(platform, stream_id, ended_at) WHERE s.platform = v.platform AND s.stream_id = v.stream_id'
    );

    // Produce before deleting user_online. Those rows are the only record that
    // these streams still need announcing: if a send fails after the delete,
    // the sentinel replays, re-runs the SELECT above against an empty table and
    // the notification is lost permanently - along with the stream's history,
    // once maintenance drops the partitions. Producing first means a failure
    // replays as duplicates instead, which both consumers already tolerate.
    if (endConfig.update) {
      // chunked so large batches stay within broker message limits
      for (let i = 0; i < ended.length; i += END_CHUNK) {
        const value: StreamsByIdMessage = {
          platform,
          ids: ended.slice(i, i + END_CHUNK).map((e) => e.user_id),
        };
        await this.producer.send({
          topic: this.streamIdTopic,
          messages: [
            {
              key: 'stream',
              value: JSON.stringify(value),
            },
          ],
        });
      }

      // notify the archiver
      for (let i = 0; i < ended.length; i += END_CHUNK) {
        const msg: StreamEndedMessage = {
          platform,
          streams: ended.slice(i, i + END_CHUNK),
        };
        await this.producer.send({
          topic: this.streamEndedTopic,
          messages: [
            {
              key: 'stream',
              value: JSON.stringify(msg),
            },
          ],
        });
      }
    }

    await this.query({
      text: 'DELETE FROM user_online WHERE last_update < $1',
      values: [endConfig.updateStartTime],
    });
  }

  private async insertLiveStreams(
    platform: Platform,
    data: Stream[],
    time: Date
  ): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    // insert live
    return this.queryMulti<Stream>(
      'INSERT INTO user_online (platform,user_id,stream_id,last_update) VALUES ',
      '$1,$2,$3,$4',
      data,
      (d: Stream) => [platform, d.user_id, d.id, time],
      ' ON CONFLICT (platform,user_id,stream_id) DO UPDATE SET last_update=EXCLUDED.last_update'
    );
  }

  private async insertProbes(
    platform: Platform,
    data: Stream[],
    time: Date
  ): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    // insert into probe
    return this.queryMulti<Stream>(
      'INSERT INTO probe (platform,stream_id,user_id,viewers,time) VALUES ',
      '$1,$2,$3,$4,$5',
      data,
      (d: Stream) => [platform, d.id, d.user_id, d.viewer_count, time],
      ' ON CONFLICT (platform,stream_id,user_id,time) DO NOTHING'
    );
  }

  private async insertUpdateStreams(
    platform: Platform,
    data: Stream[],
    time: Date
  ): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    // insert into stream
    return this.queryMulti<Stream>(
      'INSERT INTO stream (platform,stream_id,user_id,title,tags,game_id,started_at,updated_at) VALUES ',
      '$1,$2,$3,$4,$5,$6,$7,$8',
      data,
      (d: Stream) => [
        platform,
        d.id,
        d.user_id,
        d.title,
        d.tags,
        this.assureGameId(d.game_id),
        d.started_at,
        time,
      ],
      ' ON CONFLICT (platform,stream_id) DO UPDATE SET title = EXCLUDED.title, tags = EXCLUDED.tags, game_id = EXCLUDED.game_id, ended_at = null, updated_at = EXCLUDED.updated_at'
    );
  }

  private async insertStreamsGames(
    platform: Platform,
    data: Stream[],
    time: Date
  ): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    return this.queryMulti<Stream>(
      'INSERT INTO stream_game (platform,stream_id,game_id,time) VALUES ',
      '$1,$2,$3,$4',
      data,
      (d) => [platform, d.id, this.assureGameId(d.game_id), time],
      ' ON CONFLICT (platform,stream_id,game_id,time) DO NOTHING'
    );
  }

  private async insertStreamsTitles(
    platform: Platform,
    data: Stream[],
    time: Date
  ): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    return this.queryMulti<Stream>(
      'INSERT INTO stream_title (platform,stream_id,title,time) VALUES ',
      '$1,$2,$3,$4',
      data,
      (d: Stream) => [platform, d.id, d.title, time],
      ' ON CONFLICT (platform,stream_id,title,time) DO NOTHING'
    );
  }

  private async insertStreamsTags(
    platform: Platform,
    data: Stream[],
    time: Date
  ): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    const tags: StreamIdTag[] = [];
    data.forEach((d: Stream) => {
      // Helix reports "no tags" as either null or [], and changedStreams
      // treats both as a change. Skipping null here recorded the removal in
      // `stream` but never terminated the tag history, leaving the previous
      // tags looking active forever. Normalize both to an empty array.
      tags.push({
        stream_id: d.id,
        tag: d.tags ?? [],
      });
    });
    if (tags.length === 0) return Promise.resolve();
    return this.queryMulti<StreamIdTag>(
      'INSERT INTO stream_tags (platform,stream_id,tag,time) VALUES ',
      '$1,$2,$3,$4',
      tags,
      (d: StreamIdTag) => [platform, d.stream_id, d.tag, time],
      ' ON CONFLICT (platform,stream_id,tag,time) DO NOTHING'
    );
  }

  private async splitNewAndOld(
    platform: Platform,
    data: Array<Stream>
  ): Promise<Split> {
    // map of stream ids
    const ids = data.map((d) => d.id);
    // $1 is the platform, so the id placeholders start at $2
    const params = buildInList(ids, 2);
    const query = await this.pool.query<DBStream>({
      text:
        'SELECT * FROM stream WHERE platform = $1 AND stream_id IN (' +
        params.join(',') +
        ')',
      values: [platform, ...ids],
    });
    const oldHash: { [id: string]: DBStream } = {};
    for (let i = 0; i < query.rows.length; ++i) {
      oldHash[query.rows[i].stream_id] = query.rows[i];
    }
    const result: Split = {
      old: {
        ids: [],
        data: [],
      },
      new: {
        ids: [],
        data: [],
      },
      query: oldHash,
    };
    for (let i = 0; i < data.length; ++i) {
      const d = data[i];
      if (oldHash[d.id]) {
        result.old.data.push(d);
        result.old.ids.push(d.id);
      } else {
        result.new.data.push(d);
        result.new.ids.push(d.id);
      }
    }

    return result;
  }

  private changedStreams(split: Split): Changed {
    const result: Changed = {
      title: [],
      game: [],
      tags: [],
    };

    for (let i = 0; i < split.old.data.length; ++i) {
      const d = split.old.data[i];
      if (d.title !== split.query[d.id].title) {
        result.title.push(d);
      }
      // compare the normalized value: the stored game_id went through
      // assureGameId, so an uncategorized stream holds '0' in the database
      // while Helix keeps reporting ''. Comparing raw would report a game
      // change on every single poll for every uncategorized stream.
      if (this.assureGameId(d.game_id) !== split.query[d.id].game_id) {
        result.game.push(d);
      }
      const dbtag = split.query[d.id].tags;
      if ((d.tags ? d.tags.join(',') : '') !== (dbtag ? dbtag.join(',') : '')) {
        result.tags.push(d);
      }
    }

    return result;
  }
}
