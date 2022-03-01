import fetch from 'node-fetch';
import { URLSearchParams } from 'url';
import AbortController from 'abort-controller';
import { ClientCredentialsAuthProvider } from '@twurple/auth';
import pino from 'pino';
const logger = pino().child({ module: 'twitch' });
let access_token = null;
let auth = null;
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
export async function init(config) {
    auth = new ClientCredentialsAuthProvider(config.twitchClientId, config.twitchClientSecret);
    access_token = await auth.getAccessToken();
}
export async function helix(endpoint, params, api) {
    if (access_token == null) {
        throw new Error('not authenticated');
    }
    if (auth == null) {
        throw new Error('auth not initialized');
    }
    if (!api)
        api = 'helix';
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
            return data;
        }
        catch (e) {
            // @ts-ignore
            if (e && e instanceof fetch.AbortError) {
            }
            else {
                log.error(e);
            }
            sleep(2000);
        }
        finally {
            clearTimeout(timeout);
        }
    }
    process.exit(1);
}
