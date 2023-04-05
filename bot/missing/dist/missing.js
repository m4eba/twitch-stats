import { helix } from '@twitch-stats/twitch';
import { insertUpdateStreamers, insertViewsProbes, insertUpdateGames, } from '@twitch-stats/database';
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
        if (ids.length === 0)
            return [];
        const a = this.addPrefix(prefix, ids);
        this.log.info({ prefix, ids, arguments: a }, 'checkIdx');
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
    async getTimeFromRedis(prefix) {
        let user_update = await this.redis.get(prefix + 'time');
        try {
            if (user_update) {
                user_update = new Date(Date.parse(user_update)).toISOString();
            }
        }
        catch (e) {
            user_update = null;
        }
        if (!user_update)
            user_update = '1970-01-01T00:00:00.000Z';
        return user_update;
    }
    async initRedis() {
        const user_update = await this.getTimeFromRedis(Prefix.user);
        this.log.info({ update: user_update }, 'initRedis update user from');
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
        const game_update = await this.getTimeFromRedis(Prefix.game);
        this.log.info({ update: game_update }, 'initRedis update games from');
        const games = await this.pool.query({
            text: 'select game_id, updated_at from game where updated_at > $1 order by updated_at desc',
            values: [game_update],
            rowMode: 'array',
        });
        await this.insertIds(this.valuesFromQueryResult(Prefix.game, games));
        if (games.rows.length > 0) {
            await this.redis.set(Prefix.game + 'time', games.rows[0][1]);
        }
        this.log.info({}, 'initialized');
    }
    async update(streams) {
        if (streams.length === 0)
            return;
        const user_ids = new Array(streams.length);
        const game_ids = [];
        const game_hash = new Set();
        for (let i = 0; i < streams.length; ++i) {
            user_ids[i] = streams[i].user_id;
            const gid = streams[i].game_id;
            if (!game_hash.has(gid)) {
                game_ids.push(gid);
                game_hash.add(gid);
            }
        }
        const checked_user_ids = await this.checkIds(Prefix.user, user_ids);
        const checked_game_ids = await this.checkIds(Prefix.game, game_ids);
        this.log.trace({
            number_user: checked_user_ids.length,
            number_game: checked_game_ids.length,
        }, 'update length');
        await Promise.all([
            this.updateUser(checked_user_ids),
            this.updateGame(checked_game_ids),
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
            console.log('insert update streamers', time);
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
            console.log('insert update games', time);
            await insertUpdateGames(this.pool, games.data, time);
        }
        await this.insertIds(values);
        await this.redis.set(Prefix.game + 'time', time.toISOString());
    }
}
