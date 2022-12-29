import { helix } from '@twitch-stats/twitch';
import { insertUpdateStreamers, insertViewsProbes, insertUpdateGames, insertUpdateTags, } from '@twitch-stats/database';
import Prefix from './prefix.js';
export default class Missing {
    constructor(log, pool, redis) {
        this.log = log;
        this.pool = pool;
        this.redis = redis;
    }
    addPrefix(prefix, ids) {
        const result = new Array(ids.length);
        for (let i = 0; i < ids.length; ++i) {
            result[i] = prefix + ids[i];
        }
        return result;
    }
    valuesFromQueryResult(prefix, result) {
        const ids = new Array(result.rows.length * 2);
        for (let i = 0; i < result.rows.length; ++i) {
            ids[i * 2] = prefix + result.rows[i][0];
            ids[i * 2 + 1] = result.rows[i][0];
        }
        return ids;
    }
    valuesFromArray(prefix, ids) {
        const result = new Array(ids.length);
        for (let i = 0; i < ids.length; ++i) {
            result[i * 2] = prefix + ids[i];
            result[i * 2 + 1] = ids[i];
        }
        return result;
    }
    async insertIds(values) {
        let idx = 0;
        while (idx < values.length) {
            const command = values.slice(idx, idx + Math.min(values.length - idx, 1000));
            idx = idx + command.length;
            await this.redis.mSet(command);
        }
    }
    async checkIds(prefix, ids) {
        const a = this.addPrefix(prefix, ids);
        const existing_ids = await this.redis.mGet(a);
        let new_ids = new Array(ids.length);
        let idx = 0;
        for (let i = 0; i < ids.length; ++i) {
            if (existing_ids[i] === null) {
                new_ids[idx] = ids[i];
                idx++;
            }
        }
        new_ids = new_ids.slice(0, idx);
        return new_ids;
    }
    async initRedis() {
        let user_update = await this.redis.get(Prefix.user + 'time');
        if (!user_update)
            user_update = '1900-01-01';
        const users = await this.pool.query({
            text: 'select user_id, created_at from streamers where created_at > $1 order by created_at desc',
            values: [user_update],
            rowMode: 'array',
        });
        await this.insertIds(this.valuesFromQueryResult(Prefix.user, users));
        if (users.rows.length > 0) {
            await this.redis.set(Prefix.user + 'time', users.rows[0][1]);
        }
        // don't have created column, use updated
        let game_update = await this.redis.get(Prefix.game + 'time');
        if (!game_update)
            game_update = '1900-01-01';
        const games = await this.pool.query({
            text: 'select game_id, updated_at from game where updated_at > $1 order by updated_at desc',
            values: [game_update],
            rowMode: 'array',
        });
        await this.insertIds(this.valuesFromQueryResult(Prefix.game, games));
        if (games.rows.length > 0) {
            await this.redis.set(Prefix.game + 'time', games.rows[0][1]);
        }
        // don't have created column, use updated
        let tag_update = await this.redis.get(Prefix.tag + 'time');
        if (!tag_update)
            tag_update = '1900-01-01';
        const tags = await this.pool.query({
            text: 'select tag_id, updated_at from tags where updated_at > $1 order by updated_at desc',
            values: [tag_update],
            rowMode: 'array',
        });
        await this.insertIds(this.valuesFromQueryResult(Prefix.tag, tags));
        if (tags.rows.length > 0) {
            await this.redis.set(Prefix.tag + 'time', tags.rows[0][1]);
        }
        this.log.info({}, 'initialized');
    }
    async update(streams) {
        if (streams.length === 0)
            return;
        const user_ids = new Array(streams.length);
        const game_ids = [];
        const tag_ids = [];
        const game_hash = new Set();
        const tag_hash = new Set();
        for (let i = 0; i < streams.length; ++i) {
            user_ids[i] = streams[i].user_id;
            const gid = streams[i].game_id;
            if (!game_hash.has(gid)) {
                game_ids.push(gid);
                game_hash.add(gid);
            }
            if (streams[i].tag_ids) {
                streams[i].tag_ids.forEach((tag) => {
                    if (!tag_hash.has(tag)) {
                        tag_ids.push(tag);
                        tag_hash.add(tag);
                    }
                });
            }
        }
        const checked_user_ids = await this.checkIds(Prefix.user, user_ids);
        const checked_game_ids = await this.checkIds(Prefix.game, game_ids);
        const checked_tag_ids = await this.checkIds(Prefix.tag, tag_ids);
        await Promise.all([
            this.updateUser(checked_user_ids),
            this.updateGame(checked_game_ids),
            this.updateTag(checked_tag_ids),
        ]);
    }
    async updateUser(ids) {
        const time = new Date();
        const new_ids = [...ids];
        const values = this.valuesFromArray(Prefix.user, new_ids);
        while (new_ids.length > 0) {
            const params = new_ids.splice(0, 100);
            const urlParams = new URLSearchParams();
            urlParams.append('limit', '100');
            for (let i = 0; i < params.length; ++i) {
                urlParams.append('id', params[i]);
            }
            const users = await helix(`users?${urlParams.toString()}`, null);
            await insertUpdateStreamers(this.pool, users.data, time);
            await insertViewsProbes(this.pool, users.data, time);
        }
        await this.insertIds(values);
        await this.redis.set(Prefix.user + 'time', time.toISOString());
    }
    async updateGame(ids) {
        const time = new Date();
        const new_ids = [...ids];
        const values = this.valuesFromArray(Prefix.game, new_ids);
        while (new_ids.length > 0) {
            const params = new_ids.splice(0, 100);
            const urlParams = new URLSearchParams();
            for (let i = 0; i < params.length; ++i) {
                urlParams.append('id', params[i]);
            }
            const games = await helix(`games?${urlParams.toString()}`, null);
            await insertUpdateGames(this.pool, games.data, time);
        }
        await this.insertIds(values);
        await this.redis.set(Prefix.game + 'time', time.toISOString());
    }
    async updateTag(ids) {
        const time = new Date();
        const new_ids = [...ids];
        const values = this.valuesFromArray(Prefix.tag, new_ids);
        while (new_ids.length > 0) {
            const params = new_ids.splice(0, 100);
            const urlParams = new URLSearchParams();
            for (let i = 0; i < params.length; ++i) {
                urlParams.append('tag_id', params[i]);
            }
            const tags = await helix(`tags/streams?${urlParams.toString()}`, null);
            await insertUpdateTags(this.pool, tags.data, time);
        }
        await this.insertIds(values);
        await this.redis.set(Prefix.tag + 'time', time.toISOString());
    }
}
