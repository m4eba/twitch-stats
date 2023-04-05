import moment from 'moment';
import { buildMultiInsert, buildInList } from '@twitch-stats/database';
export default class Processing {
    constructor(log, pool, producer, streamIdTopic) {
        this.log = log;
        this.pool = pool;
        this.producer = producer;
        this.streamIdTopic = streamIdTopic;
    }
    /* eslint-disable @typescript-eslint/no-explicit-any */
    async query(query) {
        try {
            return this.pool.query(query);
        }
        catch (e) {
            this.log.error({ query }, 'query error');
        }
        return;
    }
    assureGameId(game_id) {
        if (!game_id)
            return '0';
        if (game_id.length === 0)
            return '0';
        if (!/^\d+$/.test(game_id))
            return '0';
        return game_id;
    }
    async processStreams(time, data) {
        if (data.length === 0)
            return Promise.resolve();
        this.log.debug({ time, data }, 'process streams');
        await this.insertProbes(data, time);
        // insert into live
        await this.insertLiveStreams(data, time);
        // we need to select all streams before updating them
        // to know what is new and old for the game_id and title tags
        const split = await this.splitNewAndOld(data);
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
    async processEnd(endConfig) {
        this.log.debug({ endConfig }, 'endStream');
        // get all live streams that are not updated since the start of the batch
        const result = await this.pool.query('SELECT * FROM user_online WHERE last_update < $1', [endConfig.updateStartTime]);
        this.log.debug({ count: result.rows.length }, 'streams not update since batch start count');
        const value = {
            ids: [],
        };
        for (let i = 0; i < result.rows.length; ++i) {
            const stream = result.rows[i];
            value.ids.push(stream.user_id);
            // add 5 minutes to last update
            const plus5 = moment(stream.last_update).add(5, 'minutes').format();
            this.log.debug({ id: stream.stream_id, time: plus5 }, 'end stream');
            await this.pool.query('UPDATE stream SET ended_at = $1, updated_at = $2 WHERE stream_id = $3', [plus5, plus5, stream.stream_id]);
            await this.pool.query('DELETE FROM user_online WHERE user_id = $1 and stream_id = $2', [stream.user_id, stream.stream_id]);
        }
        if (endConfig.update) {
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
        /*
        let ids: string[] = [];
        let idMap = new Map<string, UserOnlineRow>();
        for (let i = 0; i < result.rows.length; ++i) {
          ids.push(result.rows[i].user_id);
          idMap.set(result.rows[i].user_id, result.rows[i]);
        }
    
        // new streams to process
        let processStreams: Stream[] = [];
        // streams to end
        let endStreams: UserOnlineRow[] = [];
    
        const now = new Date();
        // check if stream still going
        while (update && ids.length > 0) {
          let params = ids.splice(0, 100);
          const urlParams = new URLSearchParams();
          urlParams.append('limit', '100');
          for (let i = 0; i < params.length; ++i) {
            urlParams.append('user_id', params[i]);
          }
    
          const streams = await helix<PaginatedResult<Stream>>(
            `streams?${urlParams.toString()}`,
            null
          );
          if (streams.data) {
            for (let i = 0; i < streams.data.length; ++i) {
              const newStream = streams.data[i];
              const oldStream = idMap.get(newStream.user_id);
              if (oldStream == null) continue; // that should not happen
              // test if user started a new stream
              if (newStream.id !== oldStream.stream_id) {
                this.log.info({ id: newStream.id }, 'new stream started');
                // prcess the new stream
                processStreams.push(newStream);
                // end the old stream
                endStreams.push(oldStream);
              }
              idMap.delete(oldStream.user_id);
            }
          }
        }
    
        // all ids that are still in the map need to be removed
        idMap.forEach((oldStream: UserOnlineRow) => {
          endStreams.push(oldStream);
        });
    
        this.log.info({ count: processStreams.length }, 'new streams count');
        this.log.info({ count: endStreams.length }, 'end streams count');
    
        // processing new streams
        for (let i = 0; i < processStreams.length; ++i) {
          await this.processStream(now, processStreams[i]);
        }
    
        // end old streams
        for (let i = 0; i < endStreams.length; ++i) {
          // add 5 minutes to last update
          const s = endStreams[i];
          const plus5 = moment(s.last_update).add(5, 'minutes').format();
          this.log.debug({ id: s.stream_id, time: plus5 }, 'end stream');
          await this.pool.query(
            'UPDATE stream SET ended_at = $1, updated_at = $2 WHERE stream_id = $3',
            [plus5, plus5, s.stream_id]
          );
          await this.pool.query(
            'DELETE FROM user_online WHERE user_id = $1 and stream_id = $2',
            [s.user_id, s.stream_id]
          );
        }
        */
    }
    async insertLiveStreams(data, time) {
        if (data.length === 0)
            return Promise.resolve();
        // insert live
        const insert = buildMultiInsert('INSERT INTO user_online (user_id,stream_id,last_update) VALUES ', '$1,$2,$3', data, (d) => [d.user_id, d.id, time]);
        insert.text +=
            ' ON CONFLICT (user_id,stream_id) DO UPDATE SET last_update=EXCLUDED.last_update';
        return this.query(insert);
    }
    async insertProbes(data, time) {
        if (data.length === 0)
            return Promise.resolve();
        // insert into probe
        const insert = buildMultiInsert('INSERT INTO probe (stream_id,user_id,viewers,time) VALUES ', '$1,$2,$3,$4', data, (d) => [d.id, d.user_id, d.viewer_count, time]);
        insert.text += ' ON CONFLICT (stream_id, user_id,time) DO NOTHING';
        return this.query(insert);
    }
    async insertUpdateStreams(data, time) {
        if (data.length === 0)
            return Promise.resolve();
        // insert into stream
        const insert = buildMultiInsert('INSERT INTO stream (stream_id,user_id,title,tags,game_id,started_at,updated_at) VALUES ', '$1,$2,$3,$4,$5,$6,$7', data, (d) => [
            d.id,
            d.user_id,
            d.title,
            d.tags,
            this.assureGameId(d.game_id),
            d.started_at,
            time,
        ]);
        insert.text +=
            ' ON CONFLICT (stream_id) DO UPDATE SET title = EXCLUDED.title, tags = EXCLUDED.tags, game_id = EXCLUDED.game_id, ended_at = null, updated_at = EXCLUDED.updated_at';
        return this.query(insert);
    }
    async insertStreamsGames(data, time) {
        if (data.length === 0)
            return Promise.resolve();
        const insert = buildMultiInsert('INSERT INTO stream_game (stream_id,game_id,time) VALUES ', '$1,$2,$3', data, (d) => [d.id, this.assureGameId(d.game_id), time]);
        insert.text += ' ON CONFLICT (stream_id, game_id,time) DO NOTHING';
        return this.query(insert);
    }
    async insertStreamsTitles(data, time) {
        if (data.length === 0)
            return Promise.resolve();
        const insert = buildMultiInsert('INSERT INTO stream_title (stream_id,title,time) VALUES ', '$1,$2,$3', data, (d) => [d.id, d.title, time]);
        insert.text += ' ON CONFLICT (stream_id, title,time) DO NOTHING';
        return this.query(insert);
    }
    async insertStreamsTags(data, time) {
        if (data.length === 0)
            return Promise.resolve();
        const tags = [];
        data.forEach((d) => {
            if (!d.tags)
                return;
            tags.push({
                stream_id: d.id,
                tag: d.tags,
            });
        });
        if (tags.length === 0)
            return Promise.resolve();
        const insert = buildMultiInsert('INSERT INTO stream_tags (stream_id,tag,time) VALUES ', '$1,$2,$3', tags, (d) => [d.stream_id, d.tag, time]);
        insert.text += ' ON CONFLICT (stream_id, tag, time) DO NOTHING';
        return this.query(insert);
    }
    async splitNewAndOld(data) {
        // map of stream ids
        const ids = data.map((d) => d.id);
        const params = buildInList(ids);
        const query = await this.pool.query({
            text: 'SELECT * FROM stream WHERE stream_id IN (' + params.join(',') + ')',
            values: ids,
        });
        const oldHash = {};
        for (let i = 0; i < query.rows.length; ++i) {
            oldHash[query.rows[i].stream_id] = query.rows[i];
        }
        const result = {
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
            }
            else {
                result.new.data.push(d);
                result.new.ids.push(d.id);
            }
        }
        return result;
    }
    changedStreams(split) {
        const result = {
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
