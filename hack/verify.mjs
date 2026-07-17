// Verify the archive after seed + streams-process + streams-archive ran:
// - archive_stream/stream_summary rows exist
// - each doc is readable from MinIO with a single range GET
// - archived streams are gone from the hot store
import pg from 'pg';
import zlib from 'node:zlib';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';

const pool = new pg.Pool({
  host: 'localhost',
  port: 5447,
  database: 'tw_stats',
  user: 'postgres',
  password: 'password',
});

const s3 = new S3Client({
  endpoint: 'http://localhost:9008',
  region: 'auto',
  forcePathStyle: true,
});

const bucket = 'twstats-archive';
let failed = false;

const idx = await pool.query(
  'SELECT * FROM archive_stream ORDER BY stream_id'
);
console.log(`archive_stream rows: ${idx.rows.length}`);
for (const row of idx.rows) {
  const res = await s3.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: row.object_key,
      Range: `bytes=${row.byte_offset}-${BigInt(row.byte_offset) + BigInt(row.byte_length) - 1n}`,
    })
  );
  const body = Buffer.concat(await res.Body.toArray());
  const doc = JSON.parse(zlib.gunzipSync(body).toString('utf8'));
  if (doc.stream_id !== row.stream_id) {
    console.error(`MISMATCH: index ${row.stream_id} -> doc ${doc.stream_id}`);
    failed = true;
  }
  console.log(
    `  ${row.stream_id} @ ${row.object_key}[${row.byte_offset}..+${row.byte_length}]`,
    JSON.stringify({
      title: doc.title,
      viewers: doc.viewers,
      titles: doc.titles,
      games: doc.games,
      tags_history: doc.tags_history,
    }, null, 2)
  );
}

const sum = await pool.query('SELECT * FROM stream_summary ORDER BY stream_id');
console.log('stream_summary:');
for (const r of sum.rows) {
  console.log(
    `  ${r.stream_id} user=${r.user_id} dur=${r.duration_seconds}s avg=${r.avg_viewers} peak=${r.peak_viewers} probes=${r.probe_count} games=[${r.game_ids}] titles=${r.title_count}`
  );
}

const hot = await pool.query('SELECT stream_id, user_id, ended_at FROM stream ORDER BY stream_id');
console.log('remaining in hot store:');
for (const r of hot.rows) {
  console.log(`  ${r.stream_id} user=${r.user_id} ended_at=${r.ended_at}`);
}

const archivedStillHot = await pool.query(
  'SELECT s.stream_id FROM stream s JOIN archive_stream a USING (stream_id)'
);
if (archivedStillHot.rows.length > 0) {
  console.error('FAIL: archived streams still in hot store');
  failed = true;
}

await pool.end();
if (failed) process.exit(1);
console.log('verify OK');
