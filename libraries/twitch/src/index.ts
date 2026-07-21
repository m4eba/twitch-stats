import type { TwitchConfig } from '@twitch-stats/config';
import fetch from 'node-fetch';
import { URLSearchParams } from 'url';
import AbortController from 'abort-controller';
import { ClientCredentialsAuthProvider } from '@twurple/auth';
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

export interface EndedStream {
  stream_id: string;
  user_id: string;
  ended_at: string;
}

export interface StreamEndedMessage {
  streams: EndedStream[];
}

const MAX_TRIES = 10;
const REQUEST_TIMEOUT_MS = 5000;
const RETRY_BASE_DELAY_MS = 2000;
const RETRY_MAX_DELAY_MS = 30000;

let auth: ClientCredentialsAuthProvider | null = null;

/**
 * A non-2xx response from the Twitch API. `status` decides whether the request
 * is worth retrying.
 */
export class HelixError extends Error {
  readonly status: number;
  readonly body: string;

  constructor(status: number, statusText: string, body: string) {
    super(`twitch api request failed: ${status} ${statusText}`);
    this.name = 'HelixError';
    this.status = status;
    this.body = body;
  }
}

// 429 and 5xx are transient; any other 4xx is a bad request that will fail
// identically no matter how often it is repeated.
function isRetryableStatus(status: number): boolean {
  return status === 429 || status >= 500;
}

function isAbortError(e: unknown): boolean {
  return e instanceof Error && e.name === 'AbortError';
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function init(config: TwitchConfig) {
  auth = new ClientCredentialsAuthProvider(
    config.twitchClientId,
    config.twitchClientSecret
  );
  // Fail fast on bad credentials instead of on the first request.
  await auth.getAccessToken();
}

export async function helix<T>(
  endpoint: string,
  params: any,
  api?: string
): Promise<T> {
  if (auth == null) {
    throw new Error('auth not initialized');
  }

  if (!api) api = 'helix';
  const log = logger.child({
    endpoint: `${api}/${endpoint}`,
    params: params,
  });

  let url = `https://api.twitch.tv/${api}/${endpoint}`;
  if (params) {
    const urlParams = new URLSearchParams(params);
    url = url + '?' + urlParams.toString();
  }

  let lastError: Error = new Error('no request was attempted');
  let refreshed = false;

  for (let tries = 0; tries < MAX_TRIES; ++tries) {
    if (tries > 0) {
      const delay = Math.min(
        RETRY_BASE_DELAY_MS * 2 ** (tries - 1),
        RETRY_MAX_DELAY_MS
      );
      log.debug({ tries, delay }, 'retrying');
      await sleep(delay);
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => {
      controller.abort();
      log.debug('timeout');
    }, REQUEST_TIMEOUT_MS);

    try {
      // getAccessToken() returns the cached app token and transparently
      // refreshes it once it has expired.
      const token = await auth.getAccessToken();
      if (token == null) {
        throw new Error('no access token available');
      }

      const req = await fetch(url, {
        method: 'GET',
        headers: {
          'Client-ID': auth.clientId,
          Authorization: 'Bearer ' + token.accessToken,
          accept: 'application/vnd.twitchtv.v5+json',
        },
        signal: controller.signal,
      });

      if (req.ok) {
        return (await req.json()) as T;
      }

      lastError = new HelixError(
        req.status,
        req.statusText,
        await req.text().catch(() => '')
      );

      // A 401 means the token was invalidated rather than merely expired,
      // which getAccessToken() has no way of noticing on its own. That is
      // worth one forced refresh, but not ten.
      if (req.status === 401 && !refreshed) {
        refreshed = true;
        log.warn('access token rejected, forcing a refresh');
        await auth.refresh();
        continue;
      }

      throw lastError;
    } catch (e) {
      // Anything Twitch will keep rejecting is not worth another 9 attempts.
      if (e instanceof HelixError && !isRetryableStatus(e.status)) {
        throw e;
      }
      lastError = e instanceof Error ? e : new Error(String(e));
      if (isAbortError(e)) {
        log.warn({ timeout: REQUEST_TIMEOUT_MS }, 'request timed out');
      } else {
        log.error(e);
      }
    } finally {
      clearTimeout(timeout);
    }
  }

  log.error({ tries: MAX_TRIES }, 'giving up');
  throw lastError;
}
