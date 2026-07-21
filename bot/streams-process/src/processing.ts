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
      return this.pool.query(query);
    } catch (e) {
      this.log.error({ query }, 'query error');
    }
    return;
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

    if (ended.length > 0) {
      const update = buildMultiInsert<EndedStream>(
        'UPDATE stream AS s SET ended_at = v.ended_at, updated_at = v.ended_at FROM (VALUES ',
        '$1::text,$2::text,$3::timestamptz',
        ended,
        (d: EndedStream) => [platform, d.stream_id, d.ended_at]
      );
      update.text +=
        ') AS v(platform, stream_id, ended_at) WHERE s.platform = v.platform AND s.stream_id = v.stream_id';
      await this.query(update);

      await this.query({
        text: 'DELETE FROM user_online WHERE platform = $1 AND last_update < $2',
        values: [platform, endConfig.updateStartTime],
      });
    }

    if (endConfig.update) {
      const value: StreamsByIdMessage = {
        platform,
        ids: ended.map((e) => e.user_id),
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

      // notify the archiver, chunked so large batches stay within message limits
      for (let i = 0; i < ended.length; i += 1000) {
        const msg: StreamEndedMessage = {
          platform,
          streams: ended.slice(i, i + 1000),
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
  }

  private async insertLiveStreams(
    platform: Platform,
    data: Stream[],
    time: Date
  ): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    // insert live
    const insert = buildMultiInsert<Stream>(
      'INSERT INTO user_online (platform,user_id,stream_id,last_update) VALUES ',
      '$1,$2,$3,$4',
      data,
      (d: Stream) => [platform, d.user_id, d.id, time]
    );
    insert.text +=
      ' ON CONFLICT (platform,user_id,stream_id) DO UPDATE SET last_update=EXCLUDED.last_update';

    return this.query(insert);
  }

  private async insertProbes(
    platform: Platform,
    data: Stream[],
    time: Date
  ): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    // insert into probe
    const insert = buildMultiInsert<Stream>(
      'INSERT INTO probe (platform,stream_id,user_id,viewers,time) VALUES ',
      '$1,$2,$3,$4,$5',
      data,
      (d: Stream) => [platform, d.id, d.user_id, d.viewer_count, time]
    );
    insert.text += ' ON CONFLICT (platform,stream_id,user_id,time) DO NOTHING';

    return this.query(insert);
  }

  private async insertUpdateStreams(
    platform: Platform,
    data: Stream[],
    time: Date
  ): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    // insert into stream
    const insert = buildMultiInsert<Stream>(
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
      ]
    );
    insert.text +=
      ' ON CONFLICT (platform,stream_id) DO UPDATE SET title = EXCLUDED.title, tags = EXCLUDED.tags, game_id = EXCLUDED.game_id, ended_at = null, updated_at = EXCLUDED.updated_at';

    return this.query(insert);
  }

  private async insertStreamsGames(
    platform: Platform,
    data: Stream[],
    time: Date
  ): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    const insert = buildMultiInsert<Stream>(
      'INSERT INTO stream_game (platform,stream_id,game_id,time) VALUES ',
      '$1,$2,$3,$4',
      data,
      (d) => [platform, d.id, this.assureGameId(d.game_id), time]
    );
    insert.text += ' ON CONFLICT (platform,stream_id,game_id,time) DO NOTHING';

    return this.query(insert);
  }

  private async insertStreamsTitles(
    platform: Platform,
    data: Stream[],
    time: Date
  ): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    const insert = buildMultiInsert<Stream>(
      'INSERT INTO stream_title (platform,stream_id,title,time) VALUES ',
      '$1,$2,$3,$4',
      data,
      (d: Stream) => [platform, d.id, d.title, time]
    );
    insert.text += ' ON CONFLICT (platform,stream_id,title,time) DO NOTHING';

    return this.query(insert);
  }

  private async insertStreamsTags(
    platform: Platform,
    data: Stream[],
    time: Date
  ): Promise<void> {
    if (data.length === 0) return Promise.resolve();
    const tags: StreamIdTag[] = [];
    data.forEach((d: Stream) => {
      if (!d.tags) return;
      tags.push({
        stream_id: d.id,
        tag: d.tags,
      });
    });
    if (tags.length === 0) return Promise.resolve();
    const insert = buildMultiInsert<StreamIdTag>(
      'INSERT INTO stream_tags (platform,stream_id,tag,time) VALUES ',
      '$1,$2,$3,$4',
      tags,
      (d: StreamIdTag) => [platform, d.stream_id, d.tag, time]
    );
    insert.text += ' ON CONFLICT (platform,stream_id,tag,time) DO NOTHING';

    return this.query(insert);
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
      if (d.game_id !== split.query[d.id].game_id) {
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
