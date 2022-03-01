import type { Tag } from '@twitch-stats/twitch';
import type { Pool, QueryResult } from 'pg';
export declare function insertUpdateTags(pool: Pool, data: Array<Tag>, time: Date): Promise<QueryResult<any> | undefined>;
