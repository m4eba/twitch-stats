import type {
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
      throw e;
    }
  }

  private assureGameId(game_id: string): string {
    if (!game_id) return '0';
    if (game_id.length === 0) return '0';
    if (!/^\d+$/.test(game_id)) return '0';
    return game_id;
  }

  public async processStreams(time: Date, data2: Stream[]): Promise<void> {
    if (data2.length === 0) return Promise.resolve();

    /* filter out this test data
     
    {"id":"2","user_id":"testDocumentId2","user_login":"testDocumentName2","user_name":"","game_id":"2","game_name":"","type":"live","title":"","viewer_count":1000,"started_at":"1970-01-01T00:00:02Z","language":"testBroadcasterLanguage2","thumbnail_url":"https://static-cdn.jtvnw.net/previews-ttv/live_user_testDocumentName2-{width}x{height}.jpg","tag_ids":[],"tags":null,"is_mature":false}
    */
    const data = data2.filter((d) => d.user_id !== 'testDocumentId2');
    // the filter above can empty a page that was non-empty on entry, and
    // splitNewAndOld builds an `IN ()` list that is invalid SQL when empty
    if (data.length === 0) return;

    this.log.debug({ time, data }, 'process streams');
    await this.insertProbes(data, time);

    // insert into live
    await this.insertLiveStreams(data, time);

    // we need to select all streams before updating them
    // to know what is new and old for the game_id and title tags
    const split: Split = await this.splitNewAndOld(data);
    // insert new streams, update old ones
    await this.insertUpdateStreams(data, time);

    // insert new game and title tags
    await this.insertStreamsGames(split.new.data, time);
    await this.insertStreamsTitles(split.new.data, time);
    await this.insertStreamsTags(split.new.data, time);

    // look through the select we made before the insert
    // and find streams with changed title or game
    const change = this.changedStreams(split);
    await this.insertStreamsGames(change.game, time);
    await this.insertStreamsTitles(change.title, time);
    await this.insertStreamsTags(change.tags, time);
  }

  public async processEnd(endConfig: ProcessingEndConfig): Promise<void> {
    this.log.debug({ endConfig }, 'endStream');
    // get all live streams that are not updated since the start of the batch
    const result = await this.pool.query(
      'SELECT * FROM user_online WHERE last_update < $1',
      [endConfig.updateStartTime]
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

    const update = buildMultiInsert<EndedStream>(
      'UPDATE stream AS s SET ended_at = v.ended_at, updated_at = v.ended_at FROM (VALUES ',
      '$1::bigint,$2::timestamptz',
      ended,
      (d: EndedStream) => [d.stream_id, d.ended_at]
    );
    update.text +=
      ') AS v(stream_id, ended_at) WHERE s.stream_id = v.stream_id';
    await this.query(update);

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

  private async insertLiveStreams(data: Stream[], time: Date): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    // insert live
    const insert = buildMultiInsert<Stream>(
      'INSERT INTO user_online (user_id,stream_id,last_update) VALUES ',
      '$1,$2,$3',
      data,
      (d: Stream) => [d.user_id, d.id, time]
    );
    insert.text +=
      ' ON CONFLICT (user_id,stream_id) DO UPDATE SET last_update=EXCLUDED.last_update';

    return this.query(insert);
  }

  private async insertProbes(data: Stream[], time: Date): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    // insert into probe
    const insert = buildMultiInsert<Stream>(
      'INSERT INTO probe (stream_id,user_id,viewers,time) VALUES ',
      '$1,$2,$3,$4',
      data,
      (d: Stream) => [d.id, d.user_id, d.viewer_count, time]
    );
    insert.text += ' ON CONFLICT (stream_id, user_id,time) DO NOTHING';

    return this.query(insert);
  }

  private async insertUpdateStreams(data: Stream[], time: Date): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    // insert into stream
    const insert = buildMultiInsert<Stream>(
      'INSERT INTO stream (stream_id,user_id,title,tags,game_id,started_at,updated_at) VALUES ',
      '$1,$2,$3,$4,$5,$6,$7',
      data,
      (d: Stream) => [
        d.id,
        d.user_id,
        d.title,
        d.tags,
        this.assureGameId(d.game_id),
        d.started_at,
        time,
      ]
    );
    insert.text +=
      ' ON CONFLICT (stream_id) DO UPDATE SET title = EXCLUDED.title, tags = EXCLUDED.tags, game_id = EXCLUDED.game_id, ended_at = null, updated_at = EXCLUDED.updated_at';

    return this.query(insert);
  }

  private async insertStreamsGames(data: Stream[], time: Date): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    const insert = buildMultiInsert<Stream>(
      'INSERT INTO stream_game (stream_id,game_id,time) VALUES ',
      '$1,$2,$3',
      data,
      (d) => [d.id, this.assureGameId(d.game_id), time]
    );
    insert.text += ' ON CONFLICT (stream_id, game_id,time) DO NOTHING';

    return this.query(insert);
  }

  private async insertStreamsTitles(data: Stream[], time: Date): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    const insert = buildMultiInsert<Stream>(
      'INSERT INTO stream_title (stream_id,title,time) VALUES ',
      '$1,$2,$3',
      data,
      (d: Stream) => [d.id, d.title, time]
    );
    insert.text += ' ON CONFLICT (stream_id, title,time) DO NOTHING';

    return this.query(insert);
  }

  private async insertStreamsTags(data: Stream[], time: Date): Promise<void> {
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
    const insert = buildMultiInsert<StreamIdTag>(
      'INSERT INTO stream_tags (stream_id,tag,time) VALUES ',
      '$1,$2,$3',
      tags,
      (d: StreamIdTag) => [d.stream_id, d.tag, time]
    );
    insert.text += ' ON CONFLICT (stream_id, tag, time) DO NOTHING';

    return this.query(insert);
  }

  private async splitNewAndOld(data: Array<Stream>): Promise<Split> {
    // map of stream ids
    const ids = data.map((d) => d.id);
    const params = buildInList(ids);
    const query = await this.pool.query<DBStream>({
      text:
        'SELECT * FROM stream WHERE stream_id IN (' + params.join(',') + ')',
      values: ids,
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
