import type { Game } from '@twitch-stats/twitch';
import type { Pool, QueryResult } from 'pg';
export declare function insertUpdateGames(pool: Pool, data: Array<Game>, time: Date): Promise<QueryResult<any> | undefined>;
//# sourceMappingURL=games.d.ts.map