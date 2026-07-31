import type { ArgumentConfig } from 'ts-command-line-args';

export interface KafkaConfig {
  kafkaClientId: string;
  kafkaBroker: string[];
}

export const KafkaConfigOpt: ArgumentConfig<KafkaConfig> = {
  kafkaClientId: {
    type: String,
    defaultValue: 'twitch-chat-bot',
  },
  kafkaBroker: {
    type: String,
    multiple: true,
  },
};

export interface TwitchConfig {
  twitchClientId: string;
  twitchClientSecret: string;
}

export const TwitchConfigOpt: ArgumentConfig<TwitchConfig> = {
  twitchClientId: { type: String },
  twitchClientSecret: { type: String },
};

export interface KickConfig {
  kickClientId: string;
  kickClientSecret: string;
}

export const KickConfigOpt: ArgumentConfig<KickConfig> = {
  kickClientId: { type: String },
  kickClientSecret: { type: String },
};

export interface PostgresConfig {
  pgHost: string;
  pgPort: number;
  pgDatabase: string;
  pgUser: string;
  pgPassword: string;
  pgUseSsl: boolean;
  pgCa?: string;
  pgKey?: string;
  pgCert?: string;
}

export const PostgresConfigOpt: ArgumentConfig<PostgresConfig> = {
  pgHost: { type: String, defaultValue: 'localhost' },
  pgPort: { type: Number, defaultValue: 5432 },
  pgDatabase: { type: String },
  pgUser: { type: String, defaultValue: 'postgres' },
  pgPassword: { type: String },
  pgUseSsl: { type: Boolean, defaultValue: false },
  pgCa: { type: String, optional: true },
  pgKey: { type: String, optional: true },
  pgCert: { type: String, optional: true },
};

export interface FileConfig {
  config?: string;
}

export const FileConfigOpt: ArgumentConfig<FileConfig> = {
  config: { type: String, optional: true },
};

export interface LogConfig {
  logLevel: string;
}

export const LogConfigOpt: ArgumentConfig<LogConfig> = {
  logLevel: { type: String, defaultValue: 'info' },
};

export interface S3Config {
  s3Endpoint: string;
  s3Region: string;
  s3Bucket: string;
  s3ForcePathStyle: boolean;
}

// credentials come from the environment (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)
export const S3ConfigOpt: ArgumentConfig<S3Config> = {
  s3Endpoint: { type: String },
  s3Region: { type: String, defaultValue: 'auto' },
  s3Bucket: { type: String },
  s3ForcePathStyle: { type: Boolean, defaultValue: true },
};

export const defaultValues = {
  streamsTopic: 'twitch-stats-streams',
  streamsIdTopic: 'twitch-stats-streams-id',
  streamEndedTopic: 'twitch-stats-stream-ended',
  exportTopic: 'twitch-stats-exported-stream',
};
