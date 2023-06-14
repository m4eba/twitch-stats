import { FileConfigOpt } from '@twitch-stats/config';
import { parse } from 'ts-command-line-args';
import aws from 'aws-sdk';
import fs from 'node:fs';
import path from 'node:path';
import { DateTime } from 'luxon';
import { initLogger } from '@twitch-stats/utils';
const WriterConfigOpt = {
    path: { type: String },
    deleteAfter: { type: Number, defaultValue: 5 },
    awsEndpoint: { type: String },
    awsRegion: { type: String },
    awsBucket: { type: String },
};
const logger = initLogger('log-upload');
const config = parse({
    ...WriterConfigOpt,
    ...FileConfigOpt,
}, {
    loadFromFileArg: 'config',
});
const s3 = new aws.S3({
    endpoint: config.awsEndpoint,
    region: config.awsRegion,
});
const uploadFile = async (filename) => {
    const input = fs.createReadStream(path.join(config.path, filename));
    console.log('startupload');
    await s3
        .putObject({
        Bucket: config.awsBucket,
        Key: filename,
        Body: input,
    })
        .promise();
    console.log('endupload???');
};
const now = DateTime.now();
const files = await fs.promises.readdir(config.path);
const reg = /^\d\d\d\d\-\d\d\-\d\d\.txt$/;
console.log('path', config.path);
console.log('files', files);
for (let i = 0; i < files.length; ++i) {
    const name = files[i];
    logger.trace({
        name,
        match: name.match(reg),
    }, 'match');
    if (name.match(reg) === null)
        continue;
    const nDate = DateTime.fromISO(name.substring(0, 10));
    const days = now.diff(nDate, 'days').toObject().days;
    logger.trace({
        name,
        ageInDays: days,
        dateFromFilename: nDate.toISODate(),
    }, 'file found');
    if (days && days > config.deleteAfter) {
        try {
            const f = path.join(config.path, files[i]);
            await uploadFile(files[i]);
            // rename to testing before deleting
            await fs.promises.rm(f);
        }
        catch (e) {
            console.log('unable to upload file', files[i]);
            console.log(e);
        }
    }
}
