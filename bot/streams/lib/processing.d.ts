import { Stream } from '@twitch-stat-bot/twitch';
import { Pool } from 'pg';
export declare function processStream(pool: Pool, time: Date, stream: Stream): Promise<void>;
export declare function processEnd(pool: Pool, time: Date): Promise<void>;
//# sourceMappingURL=processing.d.ts.map