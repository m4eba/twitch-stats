import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import type { S3Config } from '@twitch-stats/config';
import { randomUUID } from 'node:crypto';
import zlib from 'node:zlib';
import { promisify } from 'node:util';

const gzipAsync = promisify(zlib.gzip);

export interface ChunkEntry {
  offset: number;
  length: number;
}

// In-memory buffer of individually gzipped documents. Each document is its
// own gzip member at a known offset, so the concatenated chunk object is a
// valid .jsonl.gz as a whole while a single document remains readable with
// one range GET of [offset, offset+length).
export class ChunkBuffer {
  private chunks: Buffer[] = [];
  private size = 0;
  private firstAddedAt: number | null = null;

  public get byteLength(): number {
    return this.size;
  }

  public get count(): number {
    return this.chunks.length;
  }

  public get ageMs(): number {
    return this.firstAddedAt === null ? 0 : Date.now() - this.firstAddedAt;
  }

  public async add(doc: string): Promise<ChunkEntry> {
    const data = await gzipAsync(Buffer.from(doc + '\n', 'utf8'));
    const entry: ChunkEntry = { offset: this.size, length: data.length };
    this.chunks.push(data);
    this.size += data.length;
    if (this.firstAddedAt === null) {
      this.firstAddedAt = Date.now();
    }
    return entry;
  }

  public concat(): Buffer {
    return Buffer.concat(this.chunks);
  }

  public reset(): void {
    this.chunks = [];
    this.size = 0;
    this.firstAddedAt = null;
  }
}

// credentials are picked up from the environment
// (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
export function initS3(config: S3Config): S3Client {
  return new S3Client({
    endpoint: config.s3Endpoint,
    region: config.s3Region,
    forcePathStyle: config.s3ForcePathStyle,
  });
}

export function chunkKey(prefix: string, time: Date): string {
  const iso = time.toISOString();
  const date = iso.substring(0, 10).replace(/-/g, '/');
  const hms = iso.substring(11, 19).replace(/:/g, '');
  return `${prefix}${date}/${hms}-${randomUUID()}.jsonl.gz`;
}

export async function putChunk(
  s3: S3Client,
  bucket: string,
  key: string,
  body: Buffer
): Promise<void> {
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: 'application/gzip',
    })
  );
}
