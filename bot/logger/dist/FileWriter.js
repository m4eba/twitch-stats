import path from 'path';
import fs from 'fs';
import pino from 'pino';
const logger = pino({ level: 'debug' }).child({
    module: 'log-FileWriter',
});
export class FileWriter {
    constructor(folder) {
        this.folder = '';
        this.lastDay = '';
        this.filename = '';
        this.folder = folder;
        this.out = this.openStream(new Date().toISOString());
    }
    write(data) {
        // starting line looks like {"time":"1672938668808"
        const test = data.substring(2, 6);
        if (test !== 'time') {
            logger.error({ data }, 'lime not starting with time');
            throw new Error('line not starting with time');
        }
        const result = /\d+/.exec(data.substring(9, 26));
        if (!result) {
            logger.error({ data }, 'timestamp not found');
            throw new Error('timestamp not found');
        }
        const ts = result[0];
        const timestamp = new Date(parseInt(ts)).toISOString();
        if (this.out === undefined) {
            this.openStream(timestamp);
        }
        const tsDay = timestamp.substring(8, 10);
        if (tsDay !== this.lastDay) {
            this.openStream(timestamp);
        }
        this.out.write(data);
        this.out.write('\n');
    }
    openStream(timestamp) {
        const year = timestamp.substring(0, 4);
        const month = timestamp.substring(5, 7);
        const day = timestamp.substring(8, 10);
        this.lastDay = day;
        this.filename = `${year}-${month}-${day}.txt`;
        this.out = fs.createWriteStream(path.join(this.folder, this.filename), {
            flags: 'a',
        });
        return this.out;
    }
}
