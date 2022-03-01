import { buildMultiInsert } from './utils.js';
/* eslint-disable @typescript-eslint/no-explicit-any */
export async function insertUpdateStreamers(pool, data, time) {
    if (data.length === 0)
        return Promise.resolve(undefined);
    const insert = buildMultiInsert('INSERT INTO streamers (user_id,login,display_name,type,broadcaster_type,view_count,profile_image,updated_at) VALUES', '$1,$2,$3,$4,$5,$6,$7,$8', data, (d) => [
        d.id,
        d.login,
        d.display_name,
        d.type,
        d.broadcaster_type,
        d.view_count,
        d.profile_image_url,
        time,
    ]);
    insert.text +=
        ' ON CONFLICT(user_id) DO UPDATE SET profile_image=EXCLUDED.profile_image, type=EXCLUDED.type, broadcaster_type=EXCLUDED.broadcaster_type, view_count=EXCLUDED.view_count, updated_at=EXCLUDED.updated_at';
    return pool.query(insert);
}
export async function insertViewsProbes(pool, data, time) {
    if (data.length === 0)
        return Promise.resolve(undefined);
    // insert into probe
    const insert = buildMultiInsert('INSERT INTO streamers_views_probe (user_id,view_count,time) VALUES ', '$1,$2,$3', data, (d) => [d.id, d.view_count, time]);
    insert.text += ' ON CONFLICT (user_id,time) DO NOTHING';
    return pool.query(insert);
}
