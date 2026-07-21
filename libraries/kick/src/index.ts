import fetch from 'node-fetch';
import { URLSearchParams } from 'url';
import AbortController from 'abort-controller';
import pino from 'pino';

const logger = pino().child({ module: 'kick' });

const TOKEN_URL = 'https://id.kick.com/oauth/token';
const API_BASE = 'https://api.kick.com';

/** Seconds subtracted from `expires_in` so a token is refreshed before it dies. */
const TOKEN_SAFETY_MARGIN_SEC = 60;

const REQUEST_TIMEOUT_MS = 10000;
const MAX_TRIES = 10;
const BACKOFF_BASE_MS = 2000;
const BACKOFF_CAP_MS = 30000;

export interface KickConfig {
  kickClientId: string;
  kickClientSecret: string;
}

/** Raw OAuth 2.1 client-credentials token response from id.kick.com. */
export interface KickTokenResponse {
  access_token: string;
  token_type: string;
  expires_in: number;
}

/** Error carrying the HTTP status and raw response body of a failed Kick call. */
export class KickError extends Error {
  readonly status: number;
  readonly body: string;

  constructor(message: string, status: number, body: string) {
    super(message);
    this.name = 'KickError';
    this.status = status;
    this.body = body;
  }
}

// --- spec: endpoints.Pagination -------------------------------------------
export interface KickPagination {
  next_cursor: string;
}

// --- spec: endpoints.PaginatedResponse-* ----------------------------------
export interface KickPaginatedResult<T> {
  data: T[];
  message: string;
  pagination: KickPagination;
}

// --- spec: endpoints.LivestreamUser ---------------------------------------
export interface KickLivestreamUser {
  id: number;
  username: string;
  profile_picture: string;
}

// --- spec: endpoints.LivestreamCategory -----------------------------------
export interface KickLivestreamCategory {
  id: number;
  name: string;
  thumbnail: string;
}

// --- spec: endpoints.LivestreamChannel ------------------------------------
export interface KickLivestreamChannel {
  slug: string;
}

// --- spec: endpoints.LivestreamV2 -----------------------------------------
export interface KickLivestream {
  /** UUID */
  id: string;
  title: string;
  thumbnail: string;
  started_at: string;
  /** 0 when the streamer has opted not to share their viewer count. */
  viewer_count: number;
  language_code: string;
  tags: string[];
  has_mature_content: boolean;
  broadcaster_user: KickLivestreamUser;
  category: KickLivestreamCategory;
  channel: KickLivestreamChannel;
}

let config: KickConfig | null = null;
let accessToken: string | null = null;
/** Epoch ms at which the cached token is considered stale. */
let tokenExpiresAt = 0;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isAbortError(e: unknown): boolean {
  return (
    typeof e === 'object' &&
    e !== null &&
    (e as { name?: string }).name === 'AbortError'
  );
}

function backoffMs(tries: number): number {
  return Math.min(BACKOFF_BASE_MS * Math.pow(2, tries - 1), BACKOFF_CAP_MS);
}

async function fetchToken(): Promise<void> {
  if (config == null) {
    throw new Error('kick not initialized, call init() first');
  }

  const body = new URLSearchParams({
    grant_type: 'client_credentials',
    client_id: config.kickClientId,
    client_secret: config.kickClientSecret,
  });

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  try {
    const res = await fetch(TOKEN_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/x-www-form-urlencoded' },
      body: body.toString(),
      signal: controller.signal,
    });

    if (!res.ok) {
      const text = await res.text();
      throw new KickError(
        `kick token request failed with ${res.status}`,
        res.status,
        text
      );
    }

    const data = (await res.json()) as KickTokenResponse;
    accessToken = data.access_token;
    tokenExpiresAt =
      Date.now() + (data.expires_in - TOKEN_SAFETY_MARGIN_SEC) * 1000;
    logger.debug({ expires_in: data.expires_in }, 'kick token refreshed');
  } finally {
    clearTimeout(timeout);
  }
}

/** Returns a valid token, refreshing when expired or when `force` is set. */
async function getToken(force = false): Promise<string> {
  if (force || accessToken == null || Date.now() >= tokenExpiresAt) {
    await fetchToken();
  }
  if (accessToken == null) {
    throw new Error('kick not authenticated');
  }
  return accessToken;
}

/**
 * Stores credentials and eagerly fetches a token so bad credentials fail at
 * boot rather than on the first API call.
 */
export async function init(cfg: KickConfig): Promise<void> {
  config = cfg;
  accessToken = null;
  tokenExpiresAt = 0;
  await fetchToken();
}

function buildUrl(
  path: string,
  params?: Record<string, string | string[]> | null
): string {
  const url = `${API_BASE}/${path.replace(/^\/+/, '')}`;
  if (!params) return url;

  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (Array.isArray(value)) {
      // repeated params: category_id=1&category_id=2
      for (const v of value) search.append(key, v);
    } else {
      search.append(key, value);
    }
  }

  const qs = search.toString();
  return qs.length > 0 ? `${url}?${qs}` : url;
}

/**
 * Performs an authenticated GET against the Kick public API.
 *
 * Retries on 429, 5xx, timeouts and network errors with exponential backoff.
 * Fails fast on other 4xx. A 401 triggers exactly one forced token refresh.
 */
export async function kickApi<T>(
  path: string,
  params?: Record<string, string | string[]> | null
): Promise<T> {
  if (config == null) {
    throw new Error('kick not initialized, call init() first');
  }

  const log = logger.child({ path, params });
  const url = buildUrl(path, params);

  let refreshedOn401 = false;
  let lastError: unknown = null;

  for (let tries = 1; tries <= MAX_TRIES; tries++) {
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      controller.abort();
      log.debug('request timeout');
    }, REQUEST_TIMEOUT_MS);

    try {
      const token = await getToken();
      const res = await fetch(url, {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
          accept: 'application/json',
        },
        signal: controller.signal,
      });

      if (res.ok) {
        return (await res.json()) as T;
      }

      const body = await res.text();
      const err = new KickError(
        `kick api ${path} failed with ${res.status}`,
        res.status,
        body
      );

      // Token may have been revoked early: force one refresh and retry.
      if (res.status === 401 && !refreshedOn401) {
        refreshedOn401 = true;
        log.warn('got 401, forcing token refresh');
        await getToken(true);
        lastError = err;
        continue;
      }

      // Retry only on rate limit and server errors.
      if (res.status !== 429 && res.status < 500) {
        throw err;
      }

      lastError = err;
      log.warn({ status: res.status, tries }, 'retryable kick api error');
    } catch (e) {
      // A thrown KickError is a fail-fast decision made above; propagate it.
      if (e instanceof KickError && e.status !== 429 && e.status < 500) {
        throw e;
      }
      if (isAbortError(e)) {
        log.debug({ tries }, 'request aborted');
      } else {
        log.error({ err: e, tries }, 'kick api request error');
      }
      lastError = e;
    } finally {
      clearTimeout(timeout);
    }

    if (tries < MAX_TRIES) {
      await sleep(backoffMs(tries));
    }
  }

  throw new Error(
    `kick api ${path} failed after ${MAX_TRIES} tries: ${String(lastError)}`
  );
}

/** Max value accepted by the `limit` query param of /public/v2/livestreams. */
export const LIVESTREAMS_MAX_LIMIT = 1000;
const LIVESTREAMS_DEFAULT_LIMIT = 100;

/**
 * GET /public/v2/livestreams — active livestreams, sorted oldest to newest,
 * cursor-paginated.
 */
export async function getLivestreams(opts: {
  limit?: number;
  cursor?: string;
}): Promise<KickPaginatedResult<KickLivestream>> {
  const limit = opts.limit ?? LIVESTREAMS_DEFAULT_LIMIT;
  if (limit < 1 || limit > LIVESTREAMS_MAX_LIMIT) {
    throw new Error(
      `limit must be between 1 and ${LIVESTREAMS_MAX_LIMIT}, got ${limit}`
    );
  }

  const params: Record<string, string | string[]> = {
    limit: limit.toString(),
  };
  if (opts.cursor) params.cursor = opts.cursor;

  return kickApi<KickPaginatedResult<KickLivestream>>(
    'public/v2/livestreams',
    params
  );
}
