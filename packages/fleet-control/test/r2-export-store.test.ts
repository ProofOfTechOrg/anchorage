// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import type {
  R2Bucket,
  R2Conditional,
  R2GetOptions,
  R2MultipartOptions,
  R2MultipartUpload,
  R2Object,
  R2ObjectBody,
  R2Objects,
  R2PutOptions,
  ReadableStream as WorkerReadableStream,
  WritableStream as WorkerWritableStream,
} from '@cloudflare/workers-types';
import { describe, expect, it } from 'vitest';
import type { DurableDatabaseExportStore } from '../src/database-export-store.js';
import { R2DatabaseExportStore } from '../src/r2-export-store.js';
import {
  NodeDigestStream,
  NodeFixedLengthStream,
  nodeWorkerStreams,
} from './fixtures/worker-streams.js';

type PutValue =
  | WorkerReadableStream<Uint8Array>
  | ArrayBuffer
  | ArrayBufferView
  | string
  | null
  | Blob;

type PutMode =
  | 'normal'
  | 'reject-before'
  | 'reject-mid'
  | 'size-mismatch'
  | 'null-before-read';
type GetMode =
  | 'normal'
  | 'null'
  | 'get-reject'
  | 'size-mismatch'
  | 'tamper'
  | 'short'
  | 'read-error';

function exactly(message: string): RegExp {
  return new RegExp(`^${message.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`);
}

function bytes(...values: number[]): Uint8Array {
  return Uint8Array.from(values);
}

function streamFrom(
  value: Uint8Array,
  options: {
    readonly errorAfter?: number;
    readonly stayOpen?: boolean;
    readonly rejectCancel?: boolean;
  } = {},
): {
  readonly body: ReadableStream<Uint8Array>;
  readonly cancellations: unknown[];
  readonly error: Error;
} {
  const cancellations: unknown[] = [];
  const error = new Error('scripted source failure');
  const cancelError = new Error('scripted cancel failure');
  let offset = 0;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (options.errorAfter !== undefined && offset >= options.errorAfter) {
        controller.error(error);
        return;
      }
      if (offset < value.byteLength) {
        const end = Math.min(offset + 2, value.byteLength);
        controller.enqueue(value.slice(offset, end));
        offset = end;
        return;
      }
      if (!options.stayOpen) controller.close();
    },
    cancel(reason) {
      cancellations.push(reason);
      if (options.rejectCancel) throw cancelError;
    },
  });
  return { body, cancellations, error };
}

function workerStreamFrom(
  value: Uint8Array,
  error?: Error,
): WorkerReadableStream<Uint8Array> {
  const fixed = new nodeWorkerStreams.FixedLengthStream(value.byteLength);
  const source = new ReadableStream<Uint8Array>({
    start(controller) {
      if (value.byteLength > 0) controller.enqueue(value);
      if (error === undefined) controller.close();
      else controller.error(error);
    },
  });
  void source.pipeTo(fixed.writable).catch(() => undefined);
  return fixed.readable;
}

function metadata(key: string, size: number) {
  return {
    key,
    version: '1',
    size,
    etag: 'etag',
    httpEtag: '"etag"',
    checksums: { toJSON: () => ({}) },
    uploaded: new Date(0),
    storageClass: 'Standard',
    writeHttpMetadata() {},
  } satisfies R2Object;
}

function objectBody(
  key: string,
  reportedSize: number,
  bodyBytes: Uint8Array,
  readError?: Error,
): R2ObjectBody {
  return {
    ...metadata(key, reportedSize),
    body: workerStreamFrom(bodyBytes, readError),
    bodyUsed: false,
    async arrayBuffer() {
      throw new Error('unused fake arrayBuffer');
    },
    async bytes() {
      throw new Error('unused fake bytes');
    },
    async text() {
      throw new Error('unused fake text');
    },
    async json<T>(): Promise<T> {
      throw new Error('unused fake json');
    },
    async blob() {
      throw new Error('unused fake blob');
    },
  };
}

function isReadable(
  value: PutValue,
): value is WorkerReadableStream<Uint8Array> {
  return (
    typeof value === 'object' &&
    value !== null &&
    'getReader' in value &&
    typeof value.getReader === 'function'
  );
}

class FakeR2Bucket implements R2Bucket {
  readonly objects = new Map<string, Uint8Array>();
  readonly deleteCalls: string[] = [];
  putCalls = 0;
  putMode: PutMode = 'normal';
  putResolveAfterBytes: number | undefined;
  getMode: GetMode = 'normal';
  deleteError: Error | undefined;
  readonly putError = new Error('scripted put failure');
  readonly readError = new Error('scripted readback failure');

  async head(_key: string): Promise<R2Object | null> {
    throw new Error('unused fake head');
  }

  get(
    key: string,
    options: R2GetOptions & { onlyIf: R2Conditional | Headers },
  ): Promise<R2ObjectBody | R2Object | null>;
  get(key: string, options?: R2GetOptions): Promise<R2ObjectBody | null>;
  async get(
    key: string,
    _options?: R2GetOptions,
  ): Promise<R2ObjectBody | null> {
    if (this.getMode === 'get-reject') throw this.readError;
    if (this.getMode === 'null') return null;
    const stored = this.objects.get(key);
    if (stored === undefined) return null;
    const reportedSize =
      this.getMode === 'size-mismatch'
        ? stored.byteLength + 1
        : stored.byteLength;
    const bodyBytes =
      this.getMode === 'tamper'
        ? Uint8Array.from(stored, (byte) => byte ^ 0xff)
        : this.getMode === 'short'
          ? stored.slice(0, -1)
          : stored;
    return objectBody(
      key,
      reportedSize,
      bodyBytes,
      this.getMode === 'read-error' ? this.readError : undefined,
    );
  }

  put(
    key: string,
    value: PutValue,
    options: R2PutOptions & { onlyIf: R2Conditional | Headers },
  ): Promise<R2Object | null>;
  put(key: string, value: PutValue, options?: R2PutOptions): Promise<R2Object>;
  async put(
    key: string,
    value: PutValue,
    options?: R2PutOptions,
  ): Promise<R2Object | null> {
    this.putCalls += 1;
    if (!isReadable(value)) throw new Error('fake expects a readable stream');
    if (this.putMode === 'null-before-read') return null;
    if (this.putMode === 'reject-before') throw this.putError;
    const reader = value.getReader();
    const chunks: Uint8Array[] = [];
    let size = 0;
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      chunks.push(chunk.value.slice());
      size += chunk.value.byteLength;
      if (
        this.putResolveAfterBytes !== undefined &&
        size >= this.putResolveAfterBytes
      ) {
        const result = combineChunks(chunks, size);
        reader.releaseLock();
        this.objects.set(key, result);
        return metadata(key, size);
      }
      if (this.putMode === 'reject-mid') {
        await reader.cancel(this.putError);
        throw this.putError;
      }
    }
    const result = combineChunks(chunks, size);
    const onlyIf = options?.onlyIf;
    const conditional =
      onlyIf !== undefined &&
      !(onlyIf instanceof Headers) &&
      'etagDoesNotMatch' in onlyIf
        ? onlyIf.etagDoesNotMatch
        : undefined;
    if (conditional === '*' && this.objects.has(key)) return null;
    this.objects.set(key, result);
    return metadata(key, this.putMode === 'size-mismatch' ? size + 1 : size);
  }

  async delete(keys: string | string[]): Promise<void> {
    if (this.deleteError !== undefined) throw this.deleteError;
    const values = typeof keys === 'string' ? [keys] : keys;
    for (const key of values) {
      this.deleteCalls.push(key);
      this.objects.delete(key);
    }
  }

  async list(): Promise<R2Objects> {
    throw new Error('unused fake list');
  }

  async createMultipartUpload(
    _key: string,
    _options?: R2MultipartOptions,
  ): Promise<R2MultipartUpload> {
    throw new Error('unused fake multipart upload');
  }

  resumeMultipartUpload(_key: string, _uploadId: string): R2MultipartUpload {
    throw new Error('unused fake multipart upload');
  }
}

function combineChunks(
  chunks: readonly Uint8Array[],
  size: number,
): Uint8Array {
  const result = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

function createStore(
  bucket: R2Bucket,
  options: {
    readonly bucketName?: string;
    readonly keyPrefix?: string;
    readonly randomUUID?: () => string;
    readonly streams?: typeof nodeWorkerStreams;
  } = {},
): R2DatabaseExportStore {
  return new R2DatabaseExportStore({
    bucket,
    bucketName: options.bucketName ?? 'exports',
    keyPrefix: options.keyPrefix,
    randomUUID: options.randomUUID ?? (() => 'uuid-1'),
    streams: options.streams ?? nodeWorkerStreams,
  });
}

async function rejection(
  operation: Promise<unknown>,
  message: string,
): Promise<Error> {
  const state = await Promise.allSettled([operation]);
  expect(state[0].status).toBe('rejected');
  if (state[0].status === 'fulfilled') throw new Error('expected rejection');
  expect(state[0].reason).toBeInstanceOf(Error);
  if (!(state[0].reason instanceof Error)) throw new Error('expected Error');
  expect(state[0].reason.message).toMatch(exactly(message));
  return state[0].reason;
}

describe('Node Worker stream fixtures', () => {
  it('keeps the digest pending until close and counts bytes', async () => {
    const digest = new NodeDigestStream('SHA-256');
    let settled = false;
    void digest.digest.then(() => {
      settled = true;
    });
    expect(Number(digest.bytesWritten)).toBe(0);
    const writer = digest.getWriter();
    await writer.write(bytes(1, 2, 3));
    expect(Number(digest.bytesWritten)).toBe(3);
    expect(settled).toBe(false);
    await writer.close();
    await expect(digest.digest).resolves.toBeInstanceOf(ArrayBuffer);
    expect(Number(digest.bytesWritten)).toBe(3);
  });

  it('rejects the digest after abort', async () => {
    const digest = new NodeDigestStream('SHA-256');
    const failure = new Error('abort');
    await digest.abort(failure);
    await expect(digest.digest).rejects.toBe(failure);
  });

  it('errors both fixed-length halves for short and long sources', async () => {
    for (const [expected, value] of [
      [4, bytes(1, 2)],
      [1, bytes(1, 2)],
    ] satisfies readonly (readonly [number, Uint8Array])[]) {
      const fixed = new NodeFixedLengthStream(expected);
      const source = streamFrom(value).body;
      const reader = fixed.readable.getReader();
      const reads = (async () => {
        for (;;) {
          const chunk = await reader.read();
          if (chunk.done) return;
        }
      })();
      const states = await Promise.allSettled([
        source.pipeTo(fixed.writable),
        reads,
      ]);
      expect(states.map((state) => state.status)).toEqual([
        'rejected',
        'rejected',
      ]);
    }
  });
});

describe('R2DatabaseExportStore', () => {
  it('streams bytes, returns the location, and hashes the readback', async () => {
    const bucket = new FakeR2Bucket();
    const source = bytes(1, 2, 3, 4, 5);
    const result = await createStore(bucket).write({
      databaseId: 'db-1',
      fileName: 'backup.sqlite3',
      body: streamFrom(source).body,
      contentLength: source.byteLength,
    });
    expect(result).toEqual({
      location: 'r2://exports/db-1/uuid-1-backup.sqlite3',
      size: source.byteLength,
      sha256: createHash('sha256').update(source).digest('hex'),
    });
    expect(bucket.objects.get('db-1/uuid-1-backup.sqlite3')).toEqual(source);
    expect(result.sha256).toMatch(/^[0-9a-f]{64}$/);
  });

  it('hashes same-length committed tampering from the readback', async () => {
    const bucket = new FakeR2Bucket();
    bucket.getMode = 'tamper';
    const source = bytes(1, 2, 3, 4);
    const result = await createStore(bucket).write({
      databaseId: 'db',
      fileName: 'x.db',
      body: streamFrom(source).body,
      contentLength: source.byteLength,
    });
    const tampered = Uint8Array.from(source, (byte) => byte ^ 0xff);
    expect(result.sha256).toBe(
      createHash('sha256').update(tampered).digest('hex'),
    );
    expect(result.sha256).not.toBe(
      createHash('sha256').update(source).digest('hex'),
    );
  });

  it('refuses invalid lengths, calls cancel, and starts no put', async () => {
    for (const [contentLength, message] of [
      [undefined, 'R2 export requires a known contentLength'],
      [0, 'R2 export refuses an empty body'],
      [-1, 'R2 export contentLength must be a positive safe integer'],
      [1.5, 'R2 export contentLength must be a positive safe integer'],
      [
        Number.MAX_SAFE_INTEGER + 1,
        'R2 export contentLength must be a positive safe integer',
      ],
    ] satisfies readonly (readonly [number | undefined, string])[]) {
      const bucket = new FakeR2Bucket();
      const source = streamFrom(bytes(1), { stayOpen: true });
      await rejection(
        createStore(bucket).write({
          databaseId: 'db',
          fileName: 'x.db',
          body: source.body,
          contentLength,
        }),
        message,
      );
      expect(source.cancellations).toHaveLength(1);
      expect(bucket.objects).toHaveLength(0);
      expect(bucket.putCalls).toBe(0);
    }
  });

  it('returns an absent-length refusal while a tee sibling remains unread', async () => {
    const source = streamFrom(bytes(1), { stayOpen: true }).body;
    const [body] = source.tee();
    const bucket = new FakeR2Bucket();
    await expect(
      // The race pins the message on the fast path; a suite timeout cannot.
      Promise.race([
        createStore(bucket).write({
          databaseId: 'db',
          fileName: 'x.db',
          body,
        }),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('refusal timeout')), 100),
        ),
      ]),
    ).rejects.toThrow(exactly('R2 export requires a known contentLength'));
    expect(bucket.putCalls).toBe(0);
  });

  it('cancels on UUID and fixed-stream construction failures', async () => {
    const uuidError = new Error('uuid failed');
    const cases: readonly (readonly [
      (bucket: FakeR2Bucket) => R2DatabaseExportStore,
      string,
    ])[] = [
      [
        (bucket) =>
          createStore(bucket, {
            randomUUID: () => {
              throw uuidError;
            },
          }),
        'uuid failed',
      ],
      [
        (bucket) => createStore(bucket, { randomUUID: () => '../bad' }),
        'R2 export key component must be one portable path segment',
      ],
      [
        (bucket) =>
          createStore(bucket, {
            streams: {
              ...nodeWorkerStreams,
              FixedLengthStream: class {
                declare readonly readable: WorkerReadableStream<Uint8Array>;
                declare readonly writable: WorkerWritableStream<Uint8Array>;

                constructor(_expectedLength: number) {
                  throw new Error('fixed failed');
                }
              },
            },
          }),
        'fixed failed',
      ],
    ];
    for (const [makeStore, message] of cases) {
      const bucket = new FakeR2Bucket();
      const source = streamFrom(bytes(1), { stayOpen: true });
      await rejection(
        makeStore(bucket).write({
          databaseId: 'db',
          fileName: 'x.db',
          body: source.body,
          contentLength: 1,
        }),
        message,
      );
      expect(source.cancellations).toHaveLength(1);
      expect(bucket.putCalls).toBe(0);
    }
  });

  it('preserves a refusal when source cancellation rejects', async () => {
    const bucket = new FakeR2Bucket();
    const source = streamFrom(bytes(1), { rejectCancel: true });
    await rejection(
      createStore(bucket).write({
        databaseId: 'db',
        fileName: 'x.db',
        body: source.body,
        contentLength: 0,
      }),
      'R2 export refuses an empty body',
    );
    expect(bucket.putCalls).toBe(0);
    expect(source.cancellations).toHaveLength(1);
  });

  it('refuses a locked body before starting a put', {
    timeout: 2_000,
  }, async () => {
    const bucket = new FakeR2Bucket();
    const { body } = streamFrom(bytes(1, 2));
    body.getReader();
    await rejection(
      createStore(bucket).write({
        databaseId: 'db',
        fileName: 'x.db',
        body,
        contentLength: 2,
      }),
      'R2 export body is locked',
    );
    expect(bucket.putCalls).toBe(0);
    expect(bucket.deleteCalls).toHaveLength(0);
    expect(bucket.objects.size).toBe(0);
  });

  it('reports malformed upload lengths without deleting an object', async () => {
    for (const [expected, sourceBytes] of [
      [4, bytes(1, 2)],
      [1, bytes(1, 2)],
    ] satisfies readonly (readonly [number, Uint8Array])[]) {
      const bucket = new FakeR2Bucket();
      const error = await rejection(
        createStore(bucket).write({
          databaseId: 'db',
          fileName: 'x.db',
          body: streamFrom(sourceBytes).body,
          contentLength: expected,
        }),
        'R2 export upload failed',
      );
      expect(error.cause).toBeInstanceOf(Error);
      if (!(error.cause instanceof Error)) throw new Error('expected cause');
      expect(error.cause.message).toMatch(
        exactly(
          expected > sourceBytes.byteLength
            ? 'FixedLengthStream did not see all expected bytes before close().'
            : 'Attempt to write too many bytes through a FixedLengthStream.',
        ),
      );
      expect(bucket.objects).toHaveLength(0);
      expect(bucket.deleteCalls).toHaveLength(0);
    }
  });

  it('aborts the source when put rejects prior to reading', async () => {
    const bucket = new FakeR2Bucket();
    bucket.putMode = 'reject-before';
    const source = streamFrom(bytes(1, 2, 3), { stayOpen: true });
    const error = await rejection(
      createStore(bucket).write({
        databaseId: 'db',
        fileName: 'x.db',
        body: source.body,
        contentLength: 3,
      }),
      'R2 export upload failed',
    );
    expect(error.cause).toBe(bucket.putError);
    expect(source.cancellations).toHaveLength(1);
    expect(bucket.deleteCalls).toHaveLength(0);
  });

  it('reports a mid-stream put rejection and does not delete', async () => {
    const bucket = new FakeR2Bucket();
    bucket.putMode = 'reject-mid';
    const error = await rejection(
      createStore(bucket).write({
        databaseId: 'db',
        fileName: 'x.db',
        body: streamFrom(bytes(1, 2, 3, 4)).body,
        contentLength: 4,
      }),
      'R2 export upload failed',
    );
    expect(error.cause).toBe(bucket.putError);
    expect(bucket.deleteCalls).toHaveLength(0);
  });

  it('preserves an existing object after rejected and colliding puts', async () => {
    const key = 'db/fixed-x.db';
    const original = bytes(9, 9);
    for (const mode of [
      'reject-before',
      'normal',
    ] satisfies readonly PutMode[]) {
      const bucket = new FakeR2Bucket();
      bucket.objects.set(key, original.slice());
      bucket.putMode = mode;
      const message =
        mode === 'normal'
          ? 'R2 export key already exists'
          : 'R2 export upload failed';
      await rejection(
        createStore(bucket, { randomUUID: () => 'fixed' }).write({
          databaseId: 'db',
          fileName: 'x.db',
          body: streamFrom(bytes(1, 2)).body,
          contentLength: 2,
        }),
        message,
      );
      expect(bucket.objects.get(key)).toEqual(original);
      expect(bucket.deleteCalls).toHaveLength(0);
    }
  });

  it('aborts a source when a conditional put returns null before reading', {
    timeout: 2_000,
  }, async () => {
    const bucket = new FakeR2Bucket();
    bucket.putMode = 'null-before-read';
    bucket.objects.set('db/uuid-1-x.db', bytes(9));
    const source = streamFrom(bytes(1, 2), { stayOpen: true });
    const error = await rejection(
      createStore(bucket).write({
        databaseId: 'db',
        fileName: 'x.db',
        body: source.body,
        contentLength: 2,
      }),
      'R2 export key already exists',
    );
    expect(source.cancellations).toHaveLength(1);
    expect(source.cancellations[0]).toBe(error);
    expect(bucket.objects.get('db/uuid-1-x.db')).toEqual(bytes(9));
    expect(bucket.deleteCalls).toHaveLength(0);
    expect(bucket.putCalls).toBe(1);
  });

  it('deletes an owned object when the pipe fails after put resolution', async () => {
    const bucket = new FakeR2Bucket();
    bucket.putResolveAfterBytes = 2;
    const source = streamFrom(bytes(1, 2), { errorAfter: 2 });
    const error = await rejection(
      createStore(bucket).write({
        databaseId: 'db',
        fileName: 'x.db',
        body: source.body,
        contentLength: 2,
      }),
      'R2 export body did not stream completely',
    );
    expect(error.cause).toBe(source.error);
    expect(bucket.objects).toHaveLength(0);
    expect(bucket.deleteCalls).toEqual(['db/uuid-1-x.db']);
  });

  it('deletes an owned object for commit-size mismatch', async () => {
    const bucket = new FakeR2Bucket();
    bucket.putMode = 'size-mismatch';
    await rejection(
      createStore(bucket).write({
        databaseId: 'db',
        fileName: 'x.db',
        body: streamFrom(bytes(1, 2)).body,
        contentLength: 2,
      }),
      'R2 export size differs from contentLength',
    );
    expect(bucket.objects).toHaveLength(0);
  });

  it('cleans up readback absence, metadata mismatch, short body, and pipe failure', async () => {
    for (const [mode, message] of [
      ['null', 'R2 export readback found no object'],
      ['size-mismatch', 'R2 export readback size differs from contentLength'],
      ['short', 'R2 export readback length differs from contentLength'],
      ['read-error', 'R2 export readback failed'],
      ['get-reject', 'R2 export readback failed'],
    ] satisfies readonly (readonly [GetMode, string])[]) {
      const bucket = new FakeR2Bucket();
      bucket.getMode = mode;
      const error = await rejection(
        createStore(bucket).write({
          databaseId: 'db',
          fileName: 'x.db',
          body: streamFrom(bytes(1, 2)).body,
          contentLength: 2,
        }),
        message,
      );
      if (mode === 'read-error' || mode === 'get-reject') {
        expect(error.cause).toBe(bucket.readError);
      }
      expect(bucket.deleteCalls).toEqual(['db/uuid-1-x.db']);
    }
  });

  it('reports a rejected digest promise as a readback failure', async () => {
    const bucket = new FakeR2Bucket();
    const sentinel = new Error('digest failed');
    class RejectingDigestStream extends NodeDigestStream {
      override readonly digest = Promise.reject(sentinel);
    }
    const error = await rejection(
      createStore(bucket, {
        streams: {
          ...nodeWorkerStreams,
          DigestStream: RejectingDigestStream,
        },
      }).write({
        databaseId: 'db',
        fileName: 'x.db',
        body: streamFrom(bytes(1, 2)).body,
        contentLength: 2,
      }),
      'R2 export readback failed',
    );
    expect(error.cause).toBe(sentinel);
    expect(bucket.deleteCalls).toEqual(['db/uuid-1-x.db']);
  });

  it('reports a digest constructor failure as a readback failure', async () => {
    const bucket = new FakeR2Bucket();
    const sentinel = new Error('digest constructor failed');
    class ThrowingDigestStream extends WritableStream<
      ArrayBuffer | ArrayBufferView
    > {
      declare readonly digest: Promise<ArrayBuffer>;
      declare readonly bytesWritten: number | bigint;

      constructor(_algorithm: 'SHA-256') {
        super();
        throw sentinel;
      }
    }
    const error = await rejection(
      createStore(bucket, {
        streams: {
          ...nodeWorkerStreams,
          DigestStream: ThrowingDigestStream,
        },
      }).write({
        databaseId: 'db',
        fileName: 'x.db',
        body: streamFrom(bytes(1, 2)).body,
        contentLength: 2,
      }),
      'R2 export readback failed',
    );
    expect(error.cause).toBe(sentinel);
    expect(bucket.deleteCalls).toEqual(['db/uuid-1-x.db']);
  });

  it('aggregates an owned failure with a cleanup rejection', async () => {
    const bucket = new FakeR2Bucket();
    bucket.getMode = 'null';
    bucket.deleteError = new Error('delete failed');
    const error = await rejection(
      createStore(bucket).write({
        databaseId: 'db',
        fileName: 'x.db',
        body: streamFrom(bytes(1)).body,
        contentLength: 1,
      }),
      'database export and R2 cleanup failed',
    );
    expect(error).toBeInstanceOf(AggregateError);
    if (!(error instanceof AggregateError))
      throw new Error('expected aggregate');
    expect(error.errors[0]).toBeInstanceOf(Error);
    const exportError = error.errors[0];
    if (!(exportError instanceof Error)) throw new Error('expected Error');
    expect(exportError.message).toMatch(
      exactly('R2 export readback found no object'),
    );
    expect(error.errors[1]).toBe(bucket.deleteError);
  });

  it('funnels a synchronous bucket throw into the fixed messages', async () => {
    const sentinel = new Error('sync bucket failure');
    class ThrowingGetBucket extends FakeR2Bucket {
      override get(): never {
        throw sentinel;
      }
    }
    const getBucket = new ThrowingGetBucket();
    const readbackError = await rejection(
      createStore(getBucket).write({
        databaseId: 'db',
        fileName: 'x.db',
        body: streamFrom(bytes(1, 2)).body,
        contentLength: 2,
      }),
      'R2 export readback failed',
    );
    expect(readbackError.cause).toBe(sentinel);
    expect(getBucket.deleteCalls).toEqual(['db/uuid-1-x.db']);
    expect(getBucket.objects.size).toBe(0);

    class ThrowingDeleteBucket extends FakeR2Bucket {
      override delete(): never {
        throw sentinel;
      }
    }
    const deleteBucket = new ThrowingDeleteBucket();
    deleteBucket.getMode = 'null';
    const aggregate = await rejection(
      createStore(deleteBucket).write({
        databaseId: 'db',
        fileName: 'x.db',
        body: streamFrom(bytes(1, 2)).body,
        contentLength: 2,
      }),
      'database export and R2 cleanup failed',
    );
    expect(aggregate).toBeInstanceOf(AggregateError);
    if (!(aggregate instanceof AggregateError))
      throw new Error('expected aggregate');
    expect(aggregate.errors[0]).toBeInstanceOf(Error);
    const exportError = aggregate.errors[0];
    if (!(exportError instanceof Error)) throw new Error('expected Error');
    expect(exportError.message).toMatch(
      exactly('R2 export readback found no object'),
    );
    expect(aggregate.errors[1]).toBe(sentinel);
  });

  it('uses a fresh key when retrying after a rejected put', async () => {
    for (const [mode, body] of [
      ['reject-before', bytes(1)],
      ['reject-mid', bytes(1, 2, 3, 4)],
    ] satisfies readonly (readonly [PutMode, Uint8Array])[]) {
      const bucket = new FakeR2Bucket();
      let attempt = 0;
      const store = createStore(bucket, {
        randomUUID: () => `uuid-${++attempt}`,
      });
      bucket.putMode = mode;
      await rejection(
        store.write({
          databaseId: 'db',
          fileName: 'x.db',
          body: streamFrom(body).body,
          contentLength: body.byteLength,
        }),
        'R2 export upload failed',
      );
      expect(bucket.deleteCalls).toHaveLength(0);
      expect(bucket.objects.size).toBe(0);
      bucket.putMode = 'normal';
      const result = await store.write({
        databaseId: 'db',
        fileName: 'x.db',
        body: streamFrom(body).body,
        contentLength: body.byteLength,
      });
      expect(result.location).toBe('r2://exports/db/uuid-2-x.db');
      expect(bucket.objects.get('db/uuid-2-x.db')).toEqual(body);
    }
  });

  it('validates constructor inputs and key components with fixed messages', async () => {
    const bucket = new FakeR2Bucket();
    const bucketMessage =
      'R2DatabaseExportStore requires the Workers R2Bucket put/get/delete interface';
    const streamsMessage =
      'R2DatabaseExportStore requires the Workers DigestStream and FixedLengthStream constructors';
    const validOptions = {
      bucket,
      bucketName: 'exports',
      streams: nodeWorkerStreams,
      randomUUID: () => 'uuid',
    };
    for (const [override, message] of [
      [{ bucket: Object.create(null) }, bucketMessage],
      [{ bucket: { get() {}, delete() {} } }, bucketMessage],
      [{ bucket: { put() {}, delete() {} } }, bucketMessage],
      [{ bucket: { put() {}, get() {} } }, bucketMessage],
      [{ bucket: undefined }, bucketMessage],
      [{ streams: Object.create(null) }, streamsMessage],
      [
        { streams: { FixedLengthStream: nodeWorkerStreams.FixedLengthStream } },
        streamsMessage,
      ],
      [
        { streams: { DigestStream: nodeWorkerStreams.DigestStream } },
        streamsMessage,
      ],
      [{ streams: undefined }, streamsMessage],
      [
        { randomUUID: undefined },
        'R2DatabaseExportStore requires a randomUUID function',
      ],
    ] satisfies readonly (readonly [Record<string, unknown>, string])[]) {
      expect(() =>
        Reflect.construct(R2DatabaseExportStore, [
          { ...validOptions, ...override },
        ]),
      ).toThrow(exactly(message));
    }
    expect(() => createStore(bucket, { bucketName: '../bad' })).toThrow(
      exactly('R2 export bucketName must be one portable path segment'),
    );
    expect(() => createStore(bucket, { keyPrefix: 'exports' })).toThrow(
      exactly(
        'R2 export keyPrefix must be portable path segments each followed by /',
      ),
    );
    expect(() => createStore(bucket, { keyPrefix: 'bad//path/' })).toThrow(
      exactly(
        'R2 export keyPrefix must be portable path segments each followed by /',
      ),
    );

    for (const [field, value, message] of [
      [
        'databaseId',
        '../db',
        'R2 export databaseId must be one portable path segment',
      ],
      ['fileName', '../x', 'export fileName must be one portable path segment'],
    ] satisfies readonly (readonly [string, string, string])[]) {
      const source = streamFrom(bytes(1), { stayOpen: true });
      await rejection(
        createStore(bucket).write({
          databaseId: field === 'databaseId' ? value : 'db',
          fileName: field === 'fileName' ? value : 'x.db',
          body: source.body,
          contentLength: 1,
        }),
        message,
      );
      expect(source.cancellations).toHaveLength(1);
      expect(bucket.putCalls).toBe(0);
    }
  });

  it('accepts a multi-segment key prefix', async () => {
    const bucket = new FakeR2Bucket();
    const result = await createStore(bucket, { keyPrefix: 'a/b/' }).write({
      databaseId: 'db',
      fileName: 'x.db',
      body: streamFrom(bytes(1)).body,
      contentLength: 1,
    });
    expect(result.location).toBe('r2://exports/a/b/db/uuid-1-x.db');
  });

  it('satisfies the database export store contract', () => {
    const store: DurableDatabaseExportStore = createStore(new FakeR2Bucket());
    expect(store).toBeInstanceOf(R2DatabaseExportStore);
  });
});
