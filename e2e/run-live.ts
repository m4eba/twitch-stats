// Live pipeline check for both ingestion paths.
//
// Unlike run-e2e.ts, which drives the Processing/Archiver classes directly with
// a stubbed kafka producer, this runs the actual bot binaries against a real
// broker and the real platform APIs:
//
//   bot/kick-streams  --\
//                        >--(twitch-stats-streams)--> bot/streams-process --> postgres
//   bot/streams       --/
//
// It is the only thing that exercises the OAuth handshakes, real pagination,
// the mapping of real payloads, and the platform column end to end - including
// the part that matters most, that a sweep of one platform leaves the other
// alone. Nothing here runs in CI: it needs credentials and depends on both
// APIs being up.
//
// Each platform is skipped if its credentials are blank, so either can be
// checked on its own.
//
//   cp e2e/.env.example e2e/.env   # then fill in whichever pair you have
//   npm run test:live -w @twitch-stats/e2e
import { spawn, spawnSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import pg from 'pg';
import { PG } from './src/harness.js';

const here = dirname(fileURLToPath(import.meta.url));
const repo = join(here, '..');
const composeFile = join(here, 'docker-compose.yml');
const migrationsDir = join(repo, 'db', 'migrations');
const PROJECT = 'twstats-live';
const BROKER = 'localhost:19000';
const TOPIC = 'twitch-stats-streams';
// keeps the twitch sweep to the top few pages instead of ~100k streams
const TWITCH_MIN_VIEWERS = 2000;

function compose(args: string[]): void {
  const r = spawnSync(
    'docker',
    ['compose', '-p', PROJECT, '-f', composeFile, '--profile', 'live', ...args],
    { stdio: 'inherit' }
  );
  if (r.status !== 0) {
    throw new Error(`docker compose ${args.join(' ')} exited ${r.status}`);
  }
}

const sleep = (ms: number): Promise<void> =>
  new Promise((r) => setTimeout(r, ms));

async function waitForPostgres(timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const client = new pg.Client(PG);
    try {
      await client.connect();
      await client.query('SELECT 1');
      await client.end();
      return;
    } catch (e) {
      await client.end().catch(() => undefined);
      if (Date.now() > deadline) throw new Error(`postgres not ready: ${e}`);
      await sleep(500);
    }
  }
}

// The compose healthcheck already gates on the broker API, so this only has to
// wait for the container to report healthy.
function waitForKafka(timeoutMs: number): void {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const r = spawnSync(
      'docker',
      [
        'compose',
        '-p',
        PROJECT,
        '-f',
        composeFile,
        '--profile',
        'live',
        'ps',
        '--format',
        '{{.Service}} {{.Health}}',
      ],
      { encoding: 'utf8' }
    );
    if ((r.stdout ?? '').includes('kafka healthy')) return;
    if (Date.now() > deadline) throw new Error('kafka not ready');
    spawnSync('sleep', ['1']);
  }
}

async function migrate(): Promise<void> {
  const files = readdirSync(migrationsDir)
    .filter((f) => f.endsWith('.sql'))
    .sort();
  const client = new pg.Client(PG);
  await client.connect();
  try {
    for (const f of files) {
      const sql = readFileSync(join(migrationsDir, f), 'utf8');
      const up = sql
        .split('-- migrate:down')[0]
        .replace(/--\s*migrate:up[^\n]*\n/, '');
      await client.query(up);
    }
  } finally {
    await client.end();
  }
}

interface RunResult {
  code: number;
  out: string;
}

function run(
  bin: string,
  args: string[],
  opts: { killAfterMs?: number } = {}
): Promise<RunResult> {
  return new Promise((resolve) => {
    const child = spawn('node', [bin, ...args], {
      cwd: repo,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let out = '';
    child.stdout.on('data', (d) => {
      out += d;
      process.stdout.write(d);
    });
    child.stderr.on('data', (d) => {
      out += d;
      process.stderr.write(d);
    });
    let timer: NodeJS.Timeout | undefined;
    if (opts.killAfterMs !== undefined) {
      timer = setTimeout(() => child.kill('SIGTERM'), opts.killAfterMs);
    }
    child.on('exit', (code) => {
      if (timer) clearTimeout(timer);
      resolve({ code: code ?? 1, out });
    });
  });
}

const PG_ARGS = [
  '--pgHost',
  'localhost',
  '--pgPort',
  '5447',
  '--pgDatabase',
  'tw_stats',
  '--pgUser',
  'postgres',
  '--pgPassword',
  'password',
];

let failures = 0;
function check(label: string, ok: boolean, extra = ''): void {
  if (!ok) failures++;
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}${extra ? '  ' + extra : ''}`);
}

interface SweepResult {
  ran: boolean;
  streams: number;
}

async function sweep(
  label: string,
  bin: string,
  args: string[]
): Promise<SweepResult> {
  console.log(`\n--- ${label} ---`);
  const r = await run(bin, args);
  check(`${label} exited cleanly`, r.code === 0, `exit=${r.code}`);
  const pages = /"pages":(\d+)/.exec(r.out);
  const count = /"count":(\d+)/.exec(r.out);
  const n = count ? Number(count[1]) : 0;
  check(
    `${label} paginated`,
    pages !== null && Number(pages[1]) > 0,
    `pages=${pages ? pages[1] : 0}`
  );
  check(`${label} returned streams`, n > 0, `count=${n}`);
  check(`${label} emitted its sentinel`, r.out.includes('sentinel sent'));
  return { ran: r.code === 0, streams: n };
}

async function main(): Promise<number> {
  const kickId = process.env['KICK_CLIENT_ID'];
  const kickSecret = process.env['KICK_CLIENT_SECRET'];
  const twId = process.env['TWITCH_CLIENT_ID'];
  const twSecret = process.env['TWITCH_CLIENT_SECRET'];
  const doKick = Boolean(kickId && kickSecret);
  const doTwitch = Boolean(twId && twSecret);

  if (!doKick && !doTwitch) {
    console.error(
      'No credentials found. Copy e2e/.env.example to e2e/.env and fill in\n' +
        'KICK_CLIENT_ID/KICK_CLIENT_SECRET and/or\n' +
        'TWITCH_CLIENT_ID/TWITCH_CLIENT_SECRET.'
    );
    return 1;
  }
  if (!existsSync(join(repo, 'bot/kick-streams/dist/index.js'))) {
    console.error('build first: npm run build');
    return 1;
  }
  if (!doKick) console.log('KICK_* unset - skipping the kick sweep');
  if (!doTwitch) console.log('TWITCH_* unset - skipping the twitch sweep');

  compose(['up', '-d']);
  try {
    console.log('waiting for postgres + kafka...');
    await waitForPostgres(60000);
    waitForKafka(120000);
    console.log('applying migrations...');
    await migrate();

    if (doKick) {
      await sweep(
        'kick-streams (live Kick API)',
        'bot/kick-streams/dist/index.js',
        [
          '--kickClientId',
          kickId as string,
          '--kickClientSecret',
          kickSecret as string,
          '--kafkaBroker',
          BROKER,
          '--topic',
          TOPIC,
          '--logLevel',
          'info',
        ]
      );
    }

    if (doTwitch) {
      // A high minViewers keeps this to the top few pages. Helix returns
      // streams viewer-descending, so the sweep stops as soon as a page ends
      // below the cutoff - the whole platform would be ~100k streams.
      await sweep('streams (live Helix API)', 'bot/streams/dist/index.js', [
        '--twitchClientId',
        twId as string,
        '--twitchClientSecret',
        twSecret as string,
        '--kafkaBroker',
        BROKER,
        '--topic',
        TOPIC,
        '--minViewers',
        String(TWITCH_MIN_VIEWERS),
        '--logLevel',
        'info',
      ]);
    }

    // One consumer drains whatever both sweeps produced.
    console.log('\n--- bot/streams-process (draining the topic) ---');
    await run(
      'bot/streams-process/dist/index.js',
      [
        // deliberately no --twitchClientId/--twitchClientSecret: this service
        // never calls helix, and requiring them used to kill it at boot
        '--kafkaBroker',
        BROKER,
        '--topic',
        TOPIC,
        ...PG_ARGS,
        '--logLevel',
        'warn',
      ],
      { killAfterMs: 30000 }
    );

    // bot/missing maintains the dimension tables. Twitch needs a helix
    // round-trip for avatars and box art; kick's rows come straight off the
    // listing, so this also proves the no-API-call path.
    console.log('\n--- bot/missing (hydrating streamers + game) ---');
    await run(
      'bot/missing/dist/index.js',
      [
        '--twitchClientId',
        twId ?? 'unused',
        '--twitchClientSecret',
        twSecret ?? 'unused',
        '--kafkaBroker',
        BROKER,
        '--topic',
        TOPIC,
        '--redisUrl',
        'redis://localhost:16379',
        ...PG_ARGS,
        '--logLevel',
        'warn',
      ],
      { killAfterMs: 40000 }
    );

    console.log('\n--- verifying postgres ---');
    const client = new pg.Client(PG);
    await client.connect();
    try {
      const countFor = async (
        table: string,
        platform: string
      ): Promise<number> =>
        (
          await client.query(
            `SELECT count(*)::int n FROM ${table} WHERE platform = $1`,
            [platform]
          )
        ).rows[0].n;

      const kickStreams = await countFor('stream', 'kick');
      const twStreams = await countFor('stream', 'twitch');

      if (doKick) {
        check('kick streams written', kickStreams > 0, `n=${kickStreams}`);
        check('kick probes written', (await countFor('probe', 'kick')) > 0);
        check(
          'kick user_online written',
          (await countFor('user_online', 'kick')) > 0
        );
        // Kick livestream ids are UUIDs; this is what forced stream_id to text.
        const uuid = await client.query(
          `SELECT count(*)::int n FROM stream WHERE platform = 'kick'
             AND stream_id ~ '^[0-9a-f-]{36}$'`
        );
        check(
          'kick stream_ids are UUIDs',
          uuid.rows[0].n === kickStreams,
          `${uuid.rows[0].n}/${kickStreams}`
        );
      } else {
        check('no kick rows when kick was skipped', kickStreams === 0);
      }

      if (doTwitch) {
        check('twitch streams written', twStreams > 0, `n=${twStreams}`);
        check('twitch probes written', (await countFor('probe', 'twitch')) > 0);
        check(
          'twitch user_online written',
          (await countFor('user_online', 'twitch')) > 0
        );
        // Twitch stream ids stay numeric even though the column is now text.
        const numeric = await client.query(
          `SELECT count(*)::int n FROM stream WHERE platform = 'twitch'
             AND stream_id ~ '^[0-9]+$'`
        );
        check(
          'twitch stream_ids are numeric',
          numeric.rows[0].n === twStreams,
          `${numeric.rows[0].n}/${twStreams}`
        );
      } else {
        check('no twitch rows when twitch was skipped', twStreams === 0);
      }

      if (doKick && doTwitch) {
        // The point of the platform column: both live in the same tables, and
        // each sweep's sentinel ends only its own platform's streams. If the
        // predicate in processEnd were dropped, whichever swept last would have
        // marked the other's streams ended.
        check(
          'both platforms coexist',
          kickStreams > 0 && twStreams > 0,
          `kick=${kickStreams} twitch=${twStreams}`
        );
        const endedKick = await client.query(
          "SELECT count(*)::int n FROM stream WHERE platform = 'kick' AND ended_at IS NOT NULL"
        );
        const endedTw = await client.query(
          "SELECT count(*)::int n FROM stream WHERE platform = 'twitch' AND ended_at IS NOT NULL"
        );
        check(
          'no cross-platform end sweep',
          endedKick.rows[0].n === 0 && endedTw.rows[0].n === 0,
          `endedKick=${endedKick.rows[0].n} endedTwitch=${endedTw.rows[0].n}`
        );
        // user_id overlap between platforms is expected and must not collide:
        // both are plain integers drawn from independent id spaces.
        const shared = await client.query(
          `SELECT count(*)::int n FROM (
             SELECT user_id FROM stream WHERE platform = 'kick'
             INTERSECT
             SELECT user_id FROM stream WHERE platform = 'twitch') x`
        );
        console.log(
          `  (user_ids present on both platforms: ${shared.rows[0].n})`
        );
      }

      if (doKick) {
        const s = await client.query(
          `SELECT count(*)::int AS n,
                  count(*) FILTER (WHERE profile_image <> '')::int AS img,
                  count(*) FILTER (WHERE login <> '')::int AS login
             FROM streamers WHERE platform = 'kick'`
        );
        check('kick streamers hydrated', s.rows[0].n > 0, `n=${s.rows[0].n}`);
        check(
          'kick streamers have avatars',
          s.rows[0].img === s.rows[0].n,
          `${s.rows[0].img}/${s.rows[0].n}`
        );
        check(
          'kick streamers have slugs',
          s.rows[0].login === s.rows[0].n,
          `${s.rows[0].login}/${s.rows[0].n}`
        );
        const g = await client.query(
          `SELECT count(*)::int AS n,
                  count(*) FILTER (WHERE box_art_url <> '')::int AS art
             FROM game WHERE platform = 'kick'`
        );
        check('kick categories hydrated', g.rows[0].n > 0, `n=${g.rows[0].n}`);
        // Kick does ship the occasional category with an empty thumbnail, so
        // this only has to catch the art being dropped on the floor entirely,
        // not demand every row have one.
        check(
          'kick categories carry box art',
          g.rows[0].art > g.rows[0].n * 0.9,
          `${g.rows[0].art}/${g.rows[0].n}`
        );
      }
      if (doTwitch) {
        const s = await client.query(
          "SELECT count(*)::int n FROM streamers WHERE platform = 'twitch'"
        );
        check('twitch streamers hydrated', s.rows[0].n > 0, `n=${s.rows[0].n}`);
        const g = await client.query(
          "SELECT count(*)::int n FROM game WHERE platform = 'twitch'"
        );
        check(
          'twitch categories hydrated',
          g.rows[0].n > 0,
          `n=${g.rows[0].n}`
        );
      }
      if (doKick && doTwitch) {
        // the whole reason the redis keys had to be platform-scoped
        const dupe = await client.query(
          `SELECT count(*)::int n FROM (
             SELECT user_id FROM streamers WHERE platform = 'kick'
             INTERSECT
             SELECT user_id FROM streamers WHERE platform = 'twitch') x`
        );
        console.log(
          `  (streamer user_ids present on both platforms: ${dupe.rows[0].n})`
        );
      }

      const sample = await client.query(
        `SELECT platform, stream_id, user_id, title FROM stream
           ORDER BY platform, started_at DESC LIMIT 4`
      );
      console.log('\nsample streams:');
      for (const r of sample.rows) console.log(' ', JSON.stringify(r));
      const who = await client.query(
        `SELECT platform, user_id, login, display_name, left(profile_image, 48) AS profile_image
           FROM streamers ORDER BY platform, user_id LIMIT 4`
      );
      console.log('sample streamers:');
      for (const r of who.rows) console.log(' ', JSON.stringify(r));
      const cats = await client.query(
        `SELECT platform, game_id, name FROM game ORDER BY platform, game_id LIMIT 4`
      );
      console.log('sample categories:');
      for (const r of cats.rows) console.log(' ', JSON.stringify(r));
    } finally {
      await client.end();
    }

    console.log(failures === 0 ? '\nALL PASS' : `\n${failures} FAILED`);
    return failures === 0 ? 0 : 1;
  } finally {
    console.log('\ntearing down infra...');
    compose(['down', '-v']);
  }
}

process.exitCode = await main();
