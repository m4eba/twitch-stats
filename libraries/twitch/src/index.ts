import type { TwitchConfig } from '@twitch-stats/config';
import fetch from 'node-fetch';
import { URLSearchParams } from 'url';
import AbortController from 'abort-controller';
import { ClientCredentialsAuthProvider, AccessToken } from '@twurple/auth';
import pino from 'pino';

const logger = pino().child({ module: 'twitch' });

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
  localization_names: { [locale: string]: string };
  localization_descriptions: { [locale: string]: string };
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

let access_token: AccessToken | null = null;
let auth: ClientCredentialsAuthProvider | null = null;

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function init(config: TwitchConfig) {
  auth = new ClientCredentialsAuthProvider(
    config.twitchClientId,
    config.twitchClientSecret
  );
  access_token = await auth.getAccessToken();
}

export async function helix<T>(
  endpoint: string,
  params: any,
  api?: string
): Promise<T> {
  if (access_token == null) {
    throw new Error('not authenticated');
  }
  if (auth == null) {
    throw new Error('auth not initialized');
  }

  if (!api) api = 'helix';
  const log = logger.child({
    endpoint: `${api}/${endpoint}`,
    params: params,
  });

  let tries = 0;
  while (tries < 10) {
    log.debug(`tries: ${tries}`);
    tries++;
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      controller.abort();
      log.debug('timeout');
    }, 5000);

    try {
      let url = `https://api.twitch.tv/${api}/${endpoint}`;
      if (params) {
        const urlParams = new URLSearchParams(params);
        url = url + '?' + urlParams.toString();
      }

      const req = await fetch(url, {
        method: 'GET',
        headers: {
          'Client-ID': auth.clientId,
          Authorization: 'Bearer ' + access_token.accessToken,
          accept: 'application/vnd.twitchtv.v5+json',
        },
      });

      const data = await req.json();
      return data as T;
    } catch (e) {
      // @ts-ignore
      if (e && e instanceof fetch.AbortError) {
      } else {
        log.error(e);
      }
      sleep(2000);
    } finally {
      clearTimeout(timeout);
    }
  }
  process.exit(1);
}
