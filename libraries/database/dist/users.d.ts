import type { User } from '@twitch-stats/twitch';
import type { Pool, QueryResult } from 'pg';
export declare function insertUpdateStreamers(pool: Pool, data: Array<User>, time: Date): Promise<QueryResult<any> | undefined>;
export declare function insertViewsProbes(pool: Pool, data: Array<User>, time: Date): Promise<QueryResult<any> | undefined>;
