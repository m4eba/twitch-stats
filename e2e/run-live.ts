// Live pipeline check for the kick ingestion path.
//
// Unlike run-e2e.ts, which drives the Processing/Archiver classes directly with
// a stubbed kafka producer, this runs the actual bot binaries against a real
// broker and the real Kick API:
//
//   bot/kick-streams  --(twitch-stats-streams)-->  bot/streams-process  --> postgres
//
// It is the only thing that exercises OAuth against id.kick.com, the cursor
// pagination of /public/v2/livestreams, toStream's mapping of a real payload,
// and the platform column end to end. Nothing here runs in CI - it needs
// credentials and depends on Kick being up.
//
//   cp e2e/.env.example e2e/.env   # then fill in the two values
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

async function main(): Promise<number> {
  const id = process.env['KICK_CLIENT_ID'];
  const secret = process.env['KICK_CLIENT_SECRET'];
  if (!id || !secret) {
    console.error(
      'KICK_CLIENT_ID / KICK_CLIENT_SECRET are unset.\n' +
        'Copy e2e/.env.example to e2e/.env and fill in both values ' +
        '(https://kick.com/settings/developer).'
    );
    return 1;
  }
  if (!existsSync(join(repo, 'bot/kick-streams/dist/index.js'))) {
    console.error('build first: npm run build');
    return 1;
  }

  compose(['up', '-d']);
  try {
    console.log('waiting for postgres + kafka...');
    await waitForPostgres(60000);
    waitForKafka(120000);
    console.log('applying migrations...\n');
    await migrate();

    // 1. the real poller against the real Kick API
    console.log('--- bot/kick-streams (live Kick API) ---');
    const poll = await run('bot/kick-streams/dist/index.js', [
      '--kickClientId',
      id,
      '--kickClientSecret',
      secret,
      '--kafkaBroker',
      BROKER,
      '--topic',
      TOPIC,
      '--logLevel',
      'info',
    ]);
    check('kick-streams exited cleanly', poll.code === 0, `exit=${poll.code}`);
    // read the poller's own summary rather than counting log lines: the key and
    // the message both contain 'result_count', so grepping double-counts
    const summary = /"pages":(\d+).*?"msg":"sweep complete"/.exec(poll.out);
    const pages = summary ? Number(summary[1]) : 0;
    const swept = /"count":(\d+),"skipped"/.exec(poll.out);
    check('paginated at least one page', pages > 0, `pages=${pages}`);
    check(
      'streams returned by the api',
      swept !== null && Number(swept[1]) > 0,
      `count=${swept ? swept[1] : 0}`
    );
    check('sentinel emitted', poll.out.includes('sentinel sent'));

    // 2. the real consumer, killed once it has had time to drain
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
      { killAfterMs: 25000 }
    );

    // 3. assert on what actually landed
    console.log('\n--- verifying postgres ---');
    const client = new pg.Client(PG);
    await client.connect();
    try {
      const streams = await client.query(
        "SELECT count(*)::int n FROM stream WHERE platform = 'kick'"
      );
      check(
        'kick streams written',
        streams.rows[0].n > 0,
        `n=${streams.rows[0].n}`
      );

      const twitch = await client.query(
        "SELECT count(*)::int n FROM stream WHERE platform = 'twitch'"
      );
      check(
        'no rows misfiled as twitch',
        twitch.rows[0].n === 0,
        `n=${twitch.rows[0].n}`
      );

      // Kick livestream ids are UUIDs; this is what forced stream_id to text.
      const uuid = await client.query(
        `SELECT count(*)::int n FROM stream WHERE platform = 'kick'
           AND stream_id ~ '^[0-9a-f-]{36}$'`
      );
      check(
        'stream_id values are UUIDs',
        uuid.rows[0].n === streams.rows[0].n,
        `${uuid.rows[0].n}/${streams.rows[0].n}`
      );

      const probes = await client.query(
        "SELECT count(*)::int n FROM probe WHERE platform = 'kick'"
      );
      check('probes written', probes.rows[0].n > 0, `n=${probes.rows[0].n}`);

      const online = await client.query(
        "SELECT count(*)::int n FROM user_online WHERE platform = 'kick'"
      );
      check(
        'user_online written',
        online.rows[0].n > 0,
        `n=${online.rows[0].n}`
      );

      const sample = await client.query(
        `SELECT stream_id, user_id, title, game_id, started_at
           FROM stream WHERE platform = 'kick' ORDER BY started_at DESC LIMIT 3`
      );
      console.log('\nsample rows:');
      for (const r of sample.rows) console.log(' ', JSON.stringify(r));
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
