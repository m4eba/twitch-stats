export const KafkaConfigOpt = {
    kafkaClientId: {
        type: String,
        defaultValue: 'twitch-chat-bot',
    },
    kafkaBroker: {
        type: String,
        multiple: true,
    },
};
export const TwitchConfigOpt = {
    twitchClientId: { type: String },
    twitchClientSecret: { type: String },
};
export const PostgresConfigOpt = {
    pgHost: { type: String, defaultValue: 'localhost' },
    pgPort: { type: Number, defaultValue: 5432 },
    pgDatabase: { type: String },
    pgUser: { type: String, defaultValue: 'postgres' },
    pgPassword: { type: String },
    pgCa: { type: String, optional: true },
    pgKey: { type: String, optional: true },
    pgCert: { type: String, optional: true },
};
export const FileConfigOpt = {
    config: { type: String, optional: true },
};
export const LogConfigOpt = {
    logLevel: { type: String, defaultValue: 'info' },
};
/* eslint-disable @rushstack/typedef-var */
export const defaultValues = {
    streamsTopic: 'twitch-stats-streams',
    streamsIdTopic: 'twitch-stats-streams-id',
};
