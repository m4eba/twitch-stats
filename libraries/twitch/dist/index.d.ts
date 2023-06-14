import type { TwitchConfig } from '@twitch-stats/config';
export interface PaginatedResult<T> {
    data: Array<T>;
    pagination: {
        cursor?: string;
    };
}
export interface Stream {
    id: string;
    user_id: string;
    user_name: string;
    game_id: string;
    game_name: string;
    type: string;
    title: string;
    viewer_count: number;
    started_at: string;
    language: string;
    thumbnail_url: string;
    tags: Array<string>;
}
export interface Game {
    id: string;
    name: string;
    box_art_url: string;
}
export interface User {
    id: string;
    login: string;
    display_name: string;
    type: string;
    broadcaster_type: string;
    description: string;
    profile_image_url: string;
    offline_image_url: string;
    view_count: number;
}
export interface KrakenUser {
    _id: number;
    created_at: string;
    followers: number;
    views: number;
    broadcaster_type: string;
}
export interface Tag {
    tag_id: string;
    is_auto: boolean;
    localization_names: {
        [locale: string]: string;
    };
    localization_descriptions: {
        [locale: string]: string;
    };
}
export interface Video {
    id: string;
    user_id: string;
    user_name: string;
    title: string;
    description: string;
    created_at: string;
    published_at: string;
    url: string;
    thumbnail_url: string;
    viewable: string;
    view_count: number;
    language: string;
    type: string;
    duration: string;
}
export interface ProcessingEndConfig {
    updateStartTime: string;
    update: boolean;
}
export interface StreamsMessage {
    streams: Stream[];
    endConfig?: ProcessingEndConfig;
}
export interface StreamsByIdMessage {
    ids: string[];
}
export declare function init(config: TwitchConfig): Promise<void>;
export declare function helix<T>(endpoint: string, params: any, api?: string): Promise<T>;
//# sourceMappingURL=index.d.ts.map