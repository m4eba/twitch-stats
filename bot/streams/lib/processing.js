"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.processEnd = exports.processStream = void 0;
const twitch_1 = require("@twitch-stat-bot/twitch");
const url_1 = require("url");
const moment_1 = __importDefault(require("moment"));
function assureGameId(game_id) {
    if (!game_id)
        return '0';
    if (game_id.length == 0)
        return '0';
    if (!/^\d+$/.test(game_id))
        return '0';
    return game_id;
}
async function processStream(pool, time, stream) {
    await insertProbe(pool, stream, time);
    const old = await pool.query('SELECT * FROM stream WHERE stream_id = $1', [
        stream.id,
    ]);
    // insert into live
    await insertLiveStream(pool, stream, time);
    // insert new streams, update old ones
    await insertUpdateStream(pool, stream, time);
    let queries = [];
    if (old.rows.length == 0) {
        queries.push(insertStreamGames(pool, stream, time));
        queries.push(insertStreamTags(pool, stream, time));
        queries.push(insertStreamTitles(pool, stream, time));
    }
    else {
        const s = old.rows[0];
        if (s.title !== stream.title) {
            queries.push(insertStreamTitles(pool, stream, time));
        }
        if (s.game_id !== stream.game_id) {
            queries.push(insertStreamGames(pool, stream, time));
        }
        if (s.tag_id !== (stream.tag_ids ? stream.tag_ids.join(',') : '')) {
            queries.push(insertStreamTags(pool, stream, time));
        }
    }
    await Promise.all(queries);
}
exports.processStream = processStream;
async function processEnd(pool, time) {
    // get all live streams that are not updated in the last 15 minutes
    const result = await pool.query("SELECT * FROM user_online WHERE last_update < NOW() - INTERVAL '15 minutes'");
    let ids = [];
    let idMap = new Map();
    for (let i = 0; i < result.rows.length; ++i) {
        ids.push(result.rows[i].user_id);
        idMap.set(result.rows[i].user_id, result.rows[i]);
    }
    // new streams to process
    let processStreams = [];
    // streams to end
    let endStreams = [];
    const now = new Date();
    // check if stream still going
    while (ids.length > 0) {
        let params = ids.splice(0, 100);
        const urlParams = new url_1.URLSearchParams();
        urlParams.append('limit', '100');
        for (let i = 0; i < params.length; ++i) {
            urlParams.append('user_id', params[i]);
        }
        const streams = await (0, twitch_1.helix)(`streams?${urlParams.toString()}`, null);
        for (let i = 0; i < streams.data.length; ++i) {
            const newStream = streams.data[i];
            const oldStream = idMap.get(newStream.user_id);
            if (oldStream == null)
                continue; // that should not happen
            // test if user started a new stream
            if (newStream.id !== oldStream.stream_id) {
                // prcess the new stream
                processStreams.push(newStream);
                // end the old stream
                endStreams.push(oldStream);
            }
            idMap.delete(oldStream.user_id);
        }
    }
    // all ids that are still in the map need to be removed
    idMap.forEach((oldStream) => {
        endStreams.push(oldStream);
    });
    // processing new streams
    for (let i = 0; i < processStreams.length; ++i) {
        await processStream(pool, now, processStreams[i]);
    }
    // end old streams
    for (let i = 0; i < endStreams.length; ++i) {
        // add 5 minutes to last update
        const s = endStreams[i];
        const plus5 = (0, moment_1.default)(s.last_update).add(5, 'minutes').utc().format();
        await pool.query('UPDATE stream SET ended_at = $1, updated_at = $2 WHERE stream_id = $3', [plus5, plus5, s.stream_id]);
    }
}
exports.processEnd = processEnd;
async function insertLiveStream(pool, data, time) {
    // insert live
    await pool.query(`INSERT INTO user_online (user_id,stream_id,last_update,stream_id) VALUES ($1,$2,$3) 
    ON CONFLICT (user_id,stream_id) DO last_update=EXCLUDED.last_update`, [data.user_id, data.id, time]);
}
async function insertProbe(pool, data, time) {
    // insert into probe
    await pool.query(`INSERT INTO probe (stream_id,user_id,viewers,time) VALUES 
    ($1,$2,$3,$4)
    ON CONFLICT (stream_id, user_id,time) DO NOTHING`, [data.id, data.user_id, data.viewer_count, time]);
}
async function insertUpdateStream(pool, d, time) {
    // insert into stream
    await pool.query(`INSERT INTO stream (stream_id,user_id,title,tags,game_id,started_at,updated_at) VALUES 
    ($1,$2,$3,$4,$5,$6,$7)
    ON CONFLICT (stream_id) DO UPDATE SET title = EXCLUDED.title, tags = EXCLUDED.tags, game_id = EXCLUDED.game_id, ended_at = null, updated_at = EXCLUDED.updated_at`, [
        d.id,
        d.user_id,
        d.title,
        d.tag_ids ? d.tag_ids.join(',') : '',
        assureGameId(d.game_id),
        d.started_at,
        time,
    ]);
}
async function insertStreamGames(pool, d, time) {
    await pool.query(`INSERT INTO stream_game (stream_id,game_id,time) VALUES 
    ($1,$2,$3)
    ON CONFLICT (stream_id, game_id,time) DO NOTHING
    `, [d.id, assureGameId(d.game_id), time]);
}
async function insertStreamTitles(pool, d, time) {
    await pool.query(`INSERT INTO stream_title (stream_id,title,time) VALUES ',
    ($1,$2,$3)
    ON CONFLICT (stream_id, title,time) DO NOTHING
    `, [d.id, d.title, time]);
}
async function insertStreamTags(pool, d, time) {
    let queries = [];
    for (let i = 0; i < d.tag_ids.length; ++i) {
        queries.push(pool.query(`INSERT INTO stream_tags (stream_id,tag_id,time) VALUES 
    ($1,$2,$3)
    ON CONFLICT (stream_id, tag_id,time) DO NOTHING
    `, [d.id, d.tag_ids[i], time]));
    }
    await Promise.all(queries);
}
//# sourceMappingURL=processing.js.map