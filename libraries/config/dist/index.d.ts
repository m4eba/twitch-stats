import type { ArgumentConfig } from 'ts-command-line-args';
export interface KafkaConfig {
    kafkaClientId: string;
    kafkaBroker: string[];
}
export declare const KafkaConfigOpt: ArgumentConfig<KafkaConfig>;
export interface TwitchConfig {
    twitchClientId: string;
    twitchClientSecret: string;
}
export declare const TwitchConfigOpt: ArgumentConfig<TwitchConfig>;
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
export declare const PostgresConfigOpt: ArgumentConfig<PostgresConfig>;
export interface FileConfig {
    config?: string;
}
export declare const FileConfigOpt: ArgumentConfig<FileConfig>;
export interface LogConfig {
    logLevel: string;
}
export declare const LogConfigOpt: ArgumentConfig<LogConfig>;
export declare const defaultValues: {
    streamsTopic: string;
    streamsIdTopic: string;
};
//# sourceMappingURL=index.d.ts.map