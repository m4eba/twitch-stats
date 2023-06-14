import { FileConfig, FileConfigOpt } from '@twitch-stats/config';
import type { Logger } from 'pino';
import { ArgumentConfig, parse } from 'ts-command-line-args';
import aws from 'aws-sdk';
import fs from 'node:fs';
import path from 'node:path';
import { DateTime } from 'luxon';
import { initLogger } from '@twitch-stats/utils';

interface UploadConfig {
  path: string;
  deleteAfter: number;
  awsEndpoint: string;
  awsRegion: string;
  awsBucket: string;
}

const WriterConfigOpt: ArgumentConfig<UploadConfig> = {
  path: { type: String },
  deleteAfter: { type: Number, defaultValue: 5 },
  awsEndpoint: { type: String },
  awsRegion: { type: String },
  awsBucket: { type: String },
};

interface Config extends UploadConfig, FileConfig {}

const logger: Logger = initLogger('log-upload');

const config: Config = parse<Config>(
  {
    ...WriterConfigOpt,
    ...FileConfigOpt,
  },
  {
    loadFromFileArg: 'config',
  }
);

const s3: aws.S3 = new aws.S3({
  endpoint: config.awsEndpoint,
  region: config.awsRegion,
});

const uploadFile = async (filename: string): Promise<void> => {
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

const now: DateTime = DateTime.now();

const files: string[] = await fs.promises.readdir(config.path);
const reg: RegExp = /^\d\d\d\d\-\d\d\-\d\d\.txt$/;

console.log('path', config.path);
console.log('files', files);

for (let i: number = 0; i < files.length; ++i) {
  const name: string = files[i];
  logger.trace(
    {
      name,
      match: name.match(reg),
    },
    'match'
  );
  if (name.match(reg) === null) continue;
  const nDate: DateTime = DateTime.fromISO(name.substring(0, 10));
  const days: number | undefined = now.diff(nDate, 'days').toObject().days;
  logger.trace(
    {
      name,
      ageInDays: days,
      dateFromFilename: nDate.toISODate(),
    },
    'file found'
  );
  if (days && days > config.deleteAfter) {
    try {
      const f: string = path.join(config.path, files[i]);
      await uploadFile(files[i]);
      // rename to testing before deleting
      await fs.promises.rm(f);
    } catch (e) {
      console.log('unable to upload file', files[i]);
      console.log(e);
    }
  }
}
