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
import type {
  DatabaseExportIntegrity,
  DatabaseExportReceiptIdentity,
} from '../src/types.js';
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
  | 'reject-after-commit'
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

function metadata(
  key: string,
  size: number,
  customMetadata?: Record<string, string>,
) {
  return {
    key,
    version: '1',
    size,
    etag: 'etag',
    httpEtag: '"etag"',
    checksums: { toJSON: () => ({}) },
    uploaded: new Date(0),
    storageClass: 'Standard',
    ...(customMetadata === undefined ? {} : { customMetadata }),
    writeHttpMetadata() {},
  } satisfies R2Object;
}

function objectBody(
  key: string,
  reportedSize: number,
  bodyBytes: Uint8Array,
  customMetadata?: Record<string, string>,
  readError?: Error,
): R2ObjectBody {
  return {
    ...metadata(key, reportedSize, customMetadata),
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
  readonly customMetadata = new Map<string, Record<string, string>>();
  readonly deleteCalls: string[] = [];
  getCalls = 0;
  putCalls = 0;
  onPut: (() => void) | undefined;
  transformPutResult: ((object: R2Object) => R2Object) | undefined;
  transformGetResult: ((object: R2ObjectBody) => R2ObjectBody) | undefined;
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
    this.getCalls += 1;
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
    const result = objectBody(
      key,
      reportedSize,
      bodyBytes,
      this.customMetadata.get(key),
      this.getMode === 'read-error' ? this.readError : undefined,
    );
    return this.transformGetResult?.(result) ?? result;
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
    this.onPut?.();
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
        const customMetadata = options?.customMetadata;
        if (customMetadata !== undefined) {
          this.customMetadata.set(key, { ...customMetadata });
        }
        const object = metadata(key, size, this.customMetadata.get(key));
        return this.transformPutResult?.(object) ?? object;
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
    const customMetadata = options?.customMetadata;
    if (customMetadata !== undefined) {
      this.customMetadata.set(key, { ...customMetadata });
    }
    const object = metadata(
      key,
      this.putMode === 'size-mismatch' ? size + 1 : size,
      this.customMetadata.get(key),
    );
    if (this.putMode === 'reject-after-commit') throw this.putError;
    return this.transformPutResult?.(object) ?? object;
  }

  async delete(keys: string | string[]): Promise<void> {
    if (this.deleteError !== undefined) throw this.deleteError;
    const values = typeof keys === 'string' ? [keys] : keys;
    for (const key of values) {
      this.deleteCalls.push(key);
      this.objects.delete(key);
      this.customMetadata.delete(key);
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

function throwingProperty<T extends object, K extends keyof T>(
  value: T,
  property: K,
  error: Error,
): T {
  return Object.defineProperty({ ...value }, property, {
    configurable: true,
    enumerable: true,
    get(): never {
      throw error;
    },
  }) as T;
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

const RECEIPT_DATABASE_ID = '11111111-1111-1111-1111-111111111111';
const RECEIPT_OPERATION_ID = '22222222-2222-4222-8222-222222222222';

function integrityOf(value: Uint8Array): DatabaseExportIntegrity {
  return {
    size: value.byteLength,
    sha256: createHash('sha256').update(value).digest('hex'),
  };
}

async function integrityOfBody(
  body: ReadableStream<Uint8Array>,
): Promise<DatabaseExportIntegrity> {
  const reader = body.getReader();
  const hash = createHash('sha256');
  let size = 0;
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      hash.update(chunk.value);
      size += chunk.value.byteLength;
    }
  } finally {
    reader.releaseLock();
  }
  return { size, sha256: hash.digest('hex') };
}

function receiptIdentity(
  store: R2DatabaseExportStore,
  override: Partial<DatabaseExportReceiptIdentity> = {},
): DatabaseExportReceiptIdentity {
  return {
    version: 1,
    authority: store.receiptAuthority,
    databaseId: RECEIPT_DATABASE_ID,
    operationId: RECEIPT_OPERATION_ID,
    ...override,
  };
}

function receiptMetadataFor(
  identity: DatabaseExportReceiptIdentity,
): Record<string, string> {
  return {
    anchorageReceiptVersion: '1',
    anchorageReceiptAuthority: identity.authority,
    anchorageDatabaseId: identity.databaseId,
    anchorageOperationId: identity.operationId,
  };
}

function receiptKey(prefix = ''): string {
  return `${prefix}receipts/v1/${RECEIPT_DATABASE_ID}/${RECEIPT_OPERATION_ID}.sql`;
}

function seedReceipt(
  bucket: FakeR2Bucket,
  store: R2DatabaseExportStore,
  value: Uint8Array,
  metadataOverride?: Record<string, string>,
): void {
  const key = receiptKey();
  bucket.objects.set(key, value.slice());
  bucket.customMetadata.set(
    key,
    metadataOverride ?? receiptMetadataFor(receiptIdentity(store)),
  );
}

function writeReceipt(
  store: R2DatabaseExportStore,
  value: Uint8Array,
  options: {
    readonly identity?: DatabaseExportReceiptIdentity;
    readonly expectedIntegrity?: Promise<DatabaseExportIntegrity>;
    readonly contentLength?: number;
    readonly body?: ReadableStream<Uint8Array>;
  } = {},
) {
  return store.writeReceipt({
    identity: options.identity ?? receiptIdentity(store),
    body: options.body ?? streamFrom(value).body,
    contentLength: options.contentLength ?? value.byteLength,
    expectedIntegrity:
      options.expectedIntegrity ?? Promise.resolve(integrityOf(value)),
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

async function within<T>(operation: Promise<T>, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(message)), 250);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
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

  it('cancels before upload when a fixed stream half getter throws', async () => {
    for (const property of [
      'readable',
      'writable',
    ] satisfies readonly (keyof NodeFixedLengthStream)[]) {
      const sentinel = new Error(`${property} getter failed`);
      class ThrowingFixedLengthStream {
        readonly #fixed: NodeFixedLengthStream;

        constructor(expectedLength: number) {
          this.#fixed = new NodeFixedLengthStream(expectedLength);
        }

        get readable(): WorkerReadableStream<Uint8Array> {
          if (property === 'readable') throw sentinel;
          return this.#fixed.readable;
        }

        get writable(): WorkerWritableStream<Uint8Array> {
          if (property === 'writable') throw sentinel;
          return this.#fixed.writable;
        }
      }
      const bucket = new FakeR2Bucket();
      const source = streamFrom(bytes(1), { stayOpen: true });
      const error = await rejection(
        createStore(bucket, {
          streams: {
            ...nodeWorkerStreams,
            FixedLengthStream: ThrowingFixedLengthStream,
          },
        }).write({
          databaseId: 'db',
          fileName: 'x.db',
          body: source.body,
          contentLength: 1,
        }),
        sentinel.message,
      );
      expect(error).toBe(sentinel);
      expect(source.cancellations).toEqual([sentinel]);
      expect(bucket.putCalls).toBe(0);
      expect(bucket.deleteCalls).toHaveLength(0);
      expect(bucket.objects).toHaveLength(0);
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

  it('preserves a refusal when an injected cancel throws synchronously', async () => {
    const bucket = new FakeR2Bucket();
    const cancellations: unknown[] = [];
    const body = {
      locked: false,
      cancel(reason: unknown): never {
        cancellations.push(reason);
        throw new Error('synchronous cancel failure');
      },
    } as unknown as ReadableStream<Uint8Array>;
    const refusal = await rejection(
      createStore(bucket).write({
        databaseId: 'db',
        fileName: 'x.db',
        body,
        contentLength: 0,
      }),
      'R2 export refuses an empty body',
    );
    expect(cancellations).toHaveLength(1);
    expect(cancellations[0]).toBe(refusal);
    expect(bucket.putCalls).toBe(0);
  });

  it('refuses a locked body before starting a put', {
    // Deliberately tight: a locked body must be refused, not waited on.
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

  it('starts the put before piping the source', async () => {
    const events: string[] = [];
    const bucket = new FakeR2Bucket();
    bucket.onPut = () => events.push('put');
    const source = streamFrom(bytes(1, 2, 3, 4));
    const body = {
      locked: false,
      pipeTo(destination: unknown): Promise<void> {
        events.push('pipe');
        return source.body.pipeTo(destination as WritableStream<Uint8Array>);
      },
    } as unknown as ReadableStream<Uint8Array>;

    await within(
      createStore(bucket).write({
        databaseId: 'db',
        fileName: 'x.db',
        body,
        contentLength: 4,
      }),
      'ordered upload did not settle',
    );

    expect(events).toEqual(['put', 'pipe']);
    expect(bucket.objects.get('db/uuid-1-x.db')).toEqual(bytes(1, 2, 3, 4));
  });

  it('does not pipe or await cancellation when put throws synchronously', async () => {
    const sentinel = new Error('synchronous put failure');
    const events: string[] = [];
    const cancellations: unknown[] = [];
    const bucket = {
      put(): never {
        events.push('put');
        throw sentinel;
      },
      get(): never {
        throw new Error('get must not run');
      },
      delete(): never {
        throw new Error('delete must not run');
      },
    } as unknown as R2Bucket;
    const body = {
      locked: false,
      cancel(reason: unknown): Promise<never> {
        cancellations.push(reason);
        return new Promise(() => undefined);
      },
      pipeTo(): Promise<void> {
        events.push('pipe');
        return Promise.resolve();
      },
    } as unknown as ReadableStream<Uint8Array>;

    const error = await rejection(
      within(
        createStore(bucket).write({
          databaseId: 'db',
          fileName: 'x.db',
          body,
          contentLength: 1,
        }),
        'synchronous put handling did not settle',
      ),
      'R2 export upload failed',
    );

    expect(error.cause).toBe(sentinel);
    expect(events).toEqual(['put']);
    expect(cancellations).toEqual([sentinel]);
  });

  it('funnels a synchronous source pipe throw and settles the upload', async () => {
    const sentinel = new Error('synchronous pipe failure');
    const bucket = new FakeR2Bucket();
    const body = {
      locked: false,
      pipeTo(): never {
        throw sentinel;
      },
    } as unknown as ReadableStream<Uint8Array>;

    const error = await rejection(
      within(
        createStore(bucket).write({
          databaseId: 'db',
          fileName: 'x.db',
          body,
          contentLength: 1,
        }),
        'synchronous pipe handling did not settle',
      ),
      'R2 export upload failed',
    );

    expect(error.cause).toBe(sentinel);
    expect(bucket.putCalls).toBe(1);
    expect(bucket.deleteCalls).toHaveLength(0);
    expect(bucket.objects).toHaveLength(0);
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
    expect(source.cancellations[0]).toBe(bucket.putError);
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
    // Deliberately tight: a never-closing source is aborted, not waited on.
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

  it('cleans owned objects when upload and stored metadata getters throw', async () => {
    for (const target of [
      'uploaded-size',
      'stored-size',
      'stored-body',
    ] as const) {
      const sentinel = new Error(`${target} failed`);
      const bucket = new FakeR2Bucket();
      if (target === 'uploaded-size') {
        bucket.transformPutResult = (object) =>
          throwingProperty(object, 'size', sentinel);
      } else {
        bucket.transformGetResult = (object) =>
          throwingProperty(
            object,
            target === 'stored-size' ? 'size' : 'body',
            sentinel,
          );
      }
      const error = await rejection(
        createStore(bucket).write({
          databaseId: 'db',
          fileName: 'x.db',
          body: streamFrom(bytes(1, 2)).body,
          contentLength: 2,
        }),
        target === 'uploaded-size'
          ? 'R2 export upload failed'
          : 'R2 export readback failed',
      );
      expect(error.cause).toBe(sentinel);
      expect(bucket.deleteCalls).toEqual(['db/uuid-1-x.db']);
      expect(bucket.objects).toHaveLength(0);
    }
  });

  it('cleans owned objects when digest reads and conversions throw', async () => {
    for (const target of [
      'digest',
      'bytes-written',
      'bytes-number',
      'hex',
    ] as const) {
      const sentinel = new Error(`${target} failed`);
      class ThrowingDigestRead extends WritableStream<
        ArrayBuffer | ArrayBufferView
      > {
        constructor(_algorithm: 'SHA-256') {
          super();
        }

        get digest(): Promise<ArrayBuffer> {
          if (target === 'digest') throw sentinel;
          if (target === 'hex') {
            const malformed = new Proxy(Object.create(null), {
              get(_target, property): unknown {
                if (property === 'then') return undefined;
                throw sentinel;
              },
            });
            return Promise.resolve(malformed as ArrayBuffer);
          }
          return Promise.resolve(new Uint8Array(32).buffer);
        }

        get bytesWritten(): number | bigint {
          if (target === 'bytes-written') throw sentinel;
          if (target === 'bytes-number') {
            return {
              [Symbol.toPrimitive](): never {
                throw sentinel;
              },
            } as unknown as number;
          }
          return 2;
        }
      }
      const bucket = new FakeR2Bucket();
      const error = await rejection(
        createStore(bucket, {
          streams: {
            ...nodeWorkerStreams,
            DigestStream: ThrowingDigestRead,
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
      expect(bucket.objects).toHaveLength(0);
    }
  });

  it('does not await a pending digest after a readback pipe failure', async () => {
    const readError = new Error('readback pipe failed');
    const digestAborts: unknown[] = [];
    class PendingDigestStream extends WritableStream<
      ArrayBuffer | ArrayBufferView
    > {
      readonly digest = new Promise<ArrayBuffer>(() => undefined);
      readonly bytesWritten = 0;

      constructor(_algorithm: 'SHA-256') {
        super();
      }

      override abort(reason?: unknown): Promise<void> {
        digestAborts.push(reason);
        return super.abort(reason);
      }
    }
    const bucket = new FakeR2Bucket();
    bucket.transformGetResult = (object) => {
      Object.defineProperty(object, 'body', {
        configurable: true,
        enumerable: true,
        value: {
          pipeTo(): never {
            throw readError;
          },
        } as unknown as WorkerReadableStream<Uint8Array>,
      });
      return object;
    };
    const error = await rejection(
      within(
        createStore(bucket, {
          streams: {
            ...nodeWorkerStreams,
            DigestStream: PendingDigestStream,
          },
        }).write({
          databaseId: 'db',
          fileName: 'x.db',
          body: streamFrom(bytes(1, 2)).body,
          contentLength: 2,
        }),
        'readback failure awaited a pending digest',
      ),
      'R2 export readback failed',
    );
    expect(error.cause).toBe(readError);
    expect(digestAborts).toEqual([readError]);
    expect(bucket.deleteCalls).toEqual(['db/uuid-1-x.db']);
    expect(bucket.objects).toHaveLength(0);
  });

  it('prefers a readback pipe failure over an observed digest rejection', async () => {
    const readError = new Error('readback pipe failed');
    const digestError = new Error('digest failed first');
    class RejectedDigestStream extends WritableStream<
      ArrayBuffer | ArrayBufferView
    > {
      readonly digest = Promise.reject(digestError);
      readonly bytesWritten = 0;

      constructor(_algorithm: 'SHA-256') {
        super();
      }
    }
    const bucket = new FakeR2Bucket();
    bucket.transformGetResult = (object) => {
      Object.defineProperty(object, 'body', {
        configurable: true,
        enumerable: true,
        value: {
          pipeTo(): Promise<never> {
            return Promise.reject(readError);
          },
        } as unknown as WorkerReadableStream<Uint8Array>,
      });
      return object;
    };
    const error = await rejection(
      createStore(bucket, {
        streams: {
          ...nodeWorkerStreams,
          DigestStream: RejectedDigestStream,
        },
      }).write({
        databaseId: 'db',
        fileName: 'x.db',
        body: streamFrom(bytes(1, 2)).body,
        contentLength: 2,
      }),
      'R2 export readback failed',
    );
    expect(error.cause).toBe(readError);
    expect(bucket.deleteCalls).toEqual(['db/uuid-1-x.db']);
    expect(bucket.objects).toHaveLength(0);
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

  it('uses one canonical receipt key and exact metadata without minting a UUID', async () => {
    const bucket = new FakeR2Bucket();
    const source = bytes(1, 2, 3, 4);
    let uuidCalls = 0;
    const store = createStore(bucket, {
      keyPrefix: 'tenant/exports/',
      randomUUID: () => {
        uuidCalls += 1;
        throw new Error('receipt mode must not mint a UUID');
      },
    });
    expect(store.receiptAuthority).toBe(
      'r2://exports/tenant/exports/receipts/v1',
    );
    const identity = receiptIdentity(store);
    const result = await writeReceipt(store, source, { identity });
    const key = receiptKey('tenant/exports/');
    expect(result).toEqual({
      location: `r2://exports/${key}`,
      ...integrityOf(source),
    });
    expect(bucket.objects.get(key)).toEqual(source);
    expect(bucket.customMetadata.get(key)).toEqual(
      receiptMetadataFor(identity),
    );
    expect(Object.keys(bucket.customMetadata.get(key) ?? {})).toHaveLength(4);
    expect(bucket.putCalls).toBe(1);
    expect(bucket.deleteCalls).toHaveLength(0);
    expect(uuidCalls).toBe(0);

    const controlledBucket = new FakeR2Bucket();
    const controlledStore = createStore(controlledBucket);
    let signalPutStarted!: () => void;
    let signalPipeStarted!: () => void;
    let resolveExpected!: (value: DatabaseExportIntegrity) => void;
    const putStarted = new Promise<void>((resolve) => {
      signalPutStarted = resolve;
    });
    const pipeStarted = new Promise<void>((resolve) => {
      signalPipeStarted = resolve;
    });
    const expected = new Promise<DatabaseExportIntegrity>((resolve) => {
      resolveExpected = resolve;
    });
    controlledBucket.onPut = signalPutStarted;
    const streamed = streamFrom(source).body;
    const controlledBody = {
      get locked() {
        return streamed.locked;
      },
      pipeTo(
        destination: WritableStream<Uint8Array>,
        options?: StreamPipeOptions,
      ) {
        signalPipeStarted();
        return streamed.pipeTo(destination, options);
      },
      cancel(reason?: unknown) {
        return streamed.cancel(reason);
      },
    } as ReadableStream<Uint8Array>;
    const operation = writeReceipt(controlledStore, source, {
      body: controlledBody,
      expectedIntegrity: expected,
      identity: receiptIdentity(controlledStore, {
        operationId: '55555555-5555-4555-8555-555555555555',
      }),
    });
    await within(
      Promise.all([putStarted, pipeStarted]),
      'receipt upload awaited expected integrity before streaming',
    );
    resolveExpected(integrityOf(source));
    await expect(operation).resolves.toMatchObject(integrityOf(source));
  });

  it('converges exact sequential and conditional receipt collisions', async () => {
    const source = bytes(5, 6, 7, 8);
    const sequentialBucket = new FakeR2Bucket();
    const sequentialStore = createStore(sequentialBucket);
    const first = await writeReceipt(sequentialStore, source);
    const replayBody = streamFrom(source, { stayOpen: true });
    const replay = await writeReceipt(sequentialStore, source, {
      body: replayBody.body,
    });
    expect(replay).toEqual(first);
    expect(replayBody.cancellations).toHaveLength(1);
    expect(sequentialBucket.putCalls).toBe(1);
    expect(sequentialBucket.objects).toHaveLength(1);

    const concurrentBucket = new FakeR2Bucket();
    const concurrentStore = createStore(concurrentBucket);
    const results = await Promise.all([
      writeReceipt(concurrentStore, source),
      writeReceipt(concurrentStore, source),
    ]);
    expect(results[0]).toEqual(results[1]);
    expect(results[0]).toEqual({
      location: `r2://exports/${receiptKey()}`,
      ...integrityOf(source),
    });
    expect(concurrentBucket.objects).toHaveLength(1);
    expect(concurrentBucket.putCalls).toBe(2);
    expect(concurrentBucket.deleteCalls).toHaveLength(0);
  });

  it('recovers an exact commit whose put response is lost', async () => {
    const source = bytes(9, 10, 11, 12);
    const committedBucket = new FakeR2Bucket();
    committedBucket.putMode = 'reject-after-commit';
    const committedStore = createStore(committedBucket);
    await expect(writeReceipt(committedStore, source)).resolves.toEqual({
      location: `r2://exports/${receiptKey()}`,
      ...integrityOf(source),
    });
    expect(committedBucket.objects.get(receiptKey())).toEqual(source);
    expect(committedBucket.deleteCalls).toHaveLength(0);

    for (const mode of [
      'reject-before',
      'reject-mid',
    ] satisfies readonly PutMode[]) {
      const absentBucket = new FakeR2Bucket();
      absentBucket.putMode = mode;
      const error = await rejection(
        within(
          writeReceipt(createStore(absentBucket), source),
          `${mode} recovery did not settle`,
        ),
        'R2 export upload failed',
      );
      expect(error.cause).toBe(absentBucket.putError);
      expect(absentBucket.objects).toHaveLength(0);
      expect(absentBucket.deleteCalls).toHaveLength(0);
    }

    const synchronousBucket = new FakeR2Bucket();
    const synchronousError = new Error('synchronous put failure');
    Object.defineProperty(synchronousBucket, 'put', {
      value(): never {
        throw synchronousError;
      },
    });
    const synchronousResult = await rejection(
      within(
        writeReceipt(createStore(synchronousBucket), source),
        'synchronous put recovery did not settle',
      ),
      'R2 export upload failed',
    );
    expect(synchronousResult.cause).toBe(synchronousError);
    expect(synchronousBucket.objects).toHaveLength(0);
    expect(synchronousBucket.deleteCalls).toHaveLength(0);
  });

  it('preserves unowned or ambiguous mismatched receipt winners', async () => {
    const source = bytes(1, 3, 5, 7);
    const winner = bytes(2, 4, 6, 8);
    for (const mode of [
      'null-before-read',
      'reject-before',
    ] satisfies readonly PutMode[]) {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      bucket.putMode = mode;
      bucket.onPut = () => seedReceipt(bucket, store, winner);
      await rejection(
        within(writeReceipt(store, source), `${mode} mismatch did not settle`),
        'database export receipt collision differs from the committed export',
      );
      expect(bucket.objects.get(receiptKey())).toEqual(winner);
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    for (const metadataMutation of [
      (metadata: Record<string, string>) => {
        Reflect.deleteProperty(metadata, 'anchorageOperationId');
      },
      (metadata: Record<string, string>) => {
        metadata.extra = 'no';
      },
      (metadata: Record<string, string>) => {
        metadata.anchorageReceiptAuthority = 'r2://other/receipts/v1';
      },
    ]) {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      const customMetadata = receiptMetadataFor(receiptIdentity(store));
      metadataMutation(customMetadata);
      seedReceipt(bucket, store, winner, customMetadata);
      await rejection(
        writeReceipt(store, source),
        'database export receipt collision differs from the committed export',
      );
      expect(bucket.objects.get(receiptKey())).toEqual(winner);
      expect(bucket.deleteCalls).toHaveLength(0);
    }
  });

  it('fails closed on malformed receipt, integrity, key, and readback boundaries', async () => {
    const source = bytes(13, 14, 15, 16);
    const malformedIdentities: readonly (readonly [unknown, string])[] = [
      [
        {
          ...receiptIdentity(createStore(new FakeR2Bucket())),
          databaseId: 'AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA',
        },
        'database export receipt identity is malformed',
      ],
      [
        { ...receiptIdentity(createStore(new FakeR2Bucket())), version: 2 },
        'database export receipt identity is malformed',
      ],
      [
        { ...receiptIdentity(createStore(new FakeR2Bucket())), extra: true },
        'database export receipt identity is malformed',
      ],
      [
        Object.assign(
          { ...receiptIdentity(createStore(new FakeR2Bucket())) },
          { [Symbol('extra')]: true },
        ),
        'database export receipt identity is malformed',
      ],
      [
        Object.defineProperty(
          { ...receiptIdentity(createStore(new FakeR2Bucket())) },
          'databaseId',
          {
            enumerable: true,
            get(): never {
              throw new Error('identity accessor must not run');
            },
          },
        ),
        'database export receipt identity is malformed',
      ],
      [
        (() => {
          const revoked = Proxy.revocable(
            { ...receiptIdentity(createStore(new FakeR2Bucket())) },
            {},
          );
          revoked.revoke();
          return revoked.proxy;
        })(),
        'database export receipt identity is malformed',
      ],
      [
        new Proxy({ ...receiptIdentity(createStore(new FakeR2Bucket())) }, {}),
        'database export receipt identity is malformed',
      ],
      [
        {
          ...receiptIdentity(createStore(new FakeR2Bucket())),
          operationId: 'AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA',
        },
        'database export receipt identity is malformed',
      ],
      [
        (() => {
          const identity = {
            ...receiptIdentity(createStore(new FakeR2Bucket())),
          };
          Reflect.deleteProperty(identity, 'operationId');
          return identity;
        })(),
        'database export receipt identity is malformed',
      ],
      [
        {
          ...receiptIdentity(createStore(new FakeR2Bucket())),
          authority: 'r2://different/receipts/v1',
        },
        'database export receipt authority differs from configured authority',
      ],
    ];
    for (const [identity, message] of malformedIdentities) {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      const streamed = streamFrom(source, { stayOpen: true });
      await rejection(
        store.writeReceipt({
          identity: identity as DatabaseExportReceiptIdentity,
          body: streamed.body,
          contentLength: source.byteLength,
          expectedIntegrity: Promise.resolve(integrityOf(source)),
        }),
        message,
      );
      expect(streamed.cancellations).toHaveLength(1);
      expect(bucket.getCalls).toBe(0);
      expect(bucket.putCalls).toBe(0);
    }

    for (const contentLength of [
      undefined,
      0,
      -1,
      1.5,
      Number.MAX_SAFE_INTEGER + 1,
    ]) {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      const streamed = streamFrom(source, { stayOpen: true });
      await rejection(
        store.writeReceipt({
          identity: receiptIdentity(store),
          body: streamed.body,
          contentLength,
          expectedIntegrity: Promise.resolve(integrityOf(source)),
        }),
        'database export receipt contentLength must be a positive safe integer',
      );
      expect(streamed.cancellations).toHaveLength(1);
      expect(bucket.getCalls).toBe(0);
      expect(bucket.putCalls).toBe(0);
    }

    {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      const locked = streamFrom(source).body;
      locked.getReader();
      await rejection(
        writeReceipt(store, source, { body: locked }),
        'database export receipt body is locked or malformed',
      );
      expect(bucket.getCalls).toBe(0);
      expect(bucket.putCalls).toBe(0);
    }

    for (const cancellation of ['throw', 'reject'] as const) {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      const reasons: unknown[] = [];
      const scripted = streamFrom(source, {
        stayOpen: true,
        rejectCancel: cancellation === 'reject',
      });
      const body =
        cancellation === 'reject'
          ? scripted.body
          : ({
              locked: false,
              cancel(reason: unknown): never {
                reasons.push(reason);
                throw new Error('synchronous cancellation failure');
              },
            } as unknown as ReadableStream<Uint8Array>);
      const error = await rejection(
        store.writeReceipt({
          identity: { ...receiptIdentity(store), version: 2 } as never,
          body,
          contentLength: source.byteLength,
          expectedIntegrity: Promise.resolve(integrityOf(source)),
        }),
        'database export receipt identity is malformed',
      );
      if (cancellation === 'throw') expect(reasons).toEqual([error]);
      else expect(scripted.cancellations).toEqual([error]);
      expect(bucket.getCalls).toBe(0);
      expect(bucket.putCalls).toBe(0);
    }

    {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      const streamed = streamFrom(source, { stayOpen: true });
      const thenable = Object.defineProperty({}, 'then', {
        value() {},
      }) as unknown as Promise<DatabaseExportIntegrity>;
      await rejection(
        writeReceipt(store, source, {
          body: streamed.body,
          expectedIntegrity: thenable,
        }),
        'database export receipt integrity is malformed',
      );
      expect(streamed.cancellations).toHaveLength(1);
      expect(bucket.getCalls).toBe(0);
      expect(bucket.putCalls).toBe(0);
    }

    {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      const streamed = streamFrom(source, { stayOpen: true });
      const input = Object.defineProperties(Object.create(null), {
        body: { enumerable: true, value: streamed.body },
        expectedIntegrity: {
          enumerable: true,
          get(): never {
            throw new Error('expected-integrity accessor must not escape');
          },
        },
      });
      await rejection(
        store.writeReceipt(input as never),
        'database export receipt integrity is malformed',
      );
      expect(streamed.cancellations).toHaveLength(1);
      expect(bucket.getCalls).toBe(0);
      expect(bucket.putCalls).toBe(0);
    }

    {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      const error = await rejection(
        writeReceipt(store, source, {
          expectedIntegrity: Promise.resolve({
            size: 0,
            sha256: '0'.repeat(64),
          }),
        }),
        'database export receipt integrity is malformed',
      );
      expect(error).not.toHaveProperty('cause');
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      const error = await rejection(
        writeReceipt(store, source, {
          expectedIntegrity: Promise.resolve(
            new Proxy(integrityOf(source), {}),
          ),
        }),
        'database export receipt integrity is malformed',
      );
      expect(error).not.toHaveProperty('cause');
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      const expected = Promise.resolve(integrityOf(source));
      Object.defineProperty(expected, 'then', {
        value(): never {
          throw new Error('caller-owned then must not run');
        },
      });
      await expect(
        writeReceipt(store, source, { expectedIntegrity: expected }),
      ).resolves.toEqual({
        location: `r2://exports/${receiptKey()}`,
        ...integrityOf(source),
      });
    }

    for (const delta of [0, 1] as const) {
      const baseLength = receiptKey().length;
      const segmentLength = 1_024 - baseLength - 1 + delta;
      const prefix = `${'a'.repeat(segmentLength)}/`;
      expect(new TextEncoder().encode(receiptKey(prefix))).toHaveLength(
        1_024 + delta,
      );
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket, { keyPrefix: prefix });
      const operation = writeReceipt(store, source);
      if (delta === 0) {
        await expect(operation).resolves.toMatchObject(integrityOf(source));
      } else {
        await rejection(
          operation,
          'database export receipt key exceeds 1024 UTF-8 bytes',
        );
        expect(bucket.getCalls).toBe(0);
        expect(bucket.putCalls).toBe(0);
      }
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    for (const putMode of [
      'normal',
      'reject-before',
      'reject-after-commit',
      'null-before-read',
    ] satisfies readonly PutMode[]) {
      const bucket = new FakeR2Bucket();
      bucket.putMode = putMode;
      const store = createStore(bucket);
      const error = await rejection(
        within(
          writeReceipt(store, source, {
            expectedIntegrity: Promise.reject(
              new Error(`source failed during ${putMode}`),
            ),
          }),
          `${putMode} plus rejected integrity did not settle`,
        ),
        'database export receipt integrity is malformed',
      );
      expect(error).not.toHaveProperty('cause');
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    for (const errorAfter of [0, 2] as const) {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      const streamed = streamFrom(source, { errorAfter });
      await rejection(
        within(
          writeReceipt(store, source, {
            body: streamed.body,
            expectedIntegrity: Promise.resolve(integrityOf(source)),
          }),
          `source error after ${errorAfter} bytes did not settle`,
        ),
        'R2 export upload failed',
      );
      expect(bucket.objects).toHaveLength(0);
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    {
      const bucket = new FakeR2Bucket();
      bucket.putResolveAfterBytes = source.byteLength;
      const store = createStore(bucket);
      const streamed = streamFrom(source, {
        errorAfter: source.byteLength,
      });
      await rejection(
        within(
          writeReceipt(store, source, {
            body: streamed.body,
            expectedIntegrity: Promise.resolve(integrityOf(source)),
          }),
          'post-commit source error did not settle',
        ),
        'R2 export body did not stream completely',
      );
      expect(bucket.objects.get(receiptKey())).toEqual(source);
      expect(bucket.deleteCalls).toHaveLength(0);
      bucket.putResolveAfterBytes = undefined;
      await expect(writeReceipt(store, source)).resolves.toEqual({
        location: `r2://exports/${receiptKey()}`,
        ...integrityOf(source),
      });
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      const streamed = streamFrom(source, { errorAfter: 2 });
      const [storeBody, hashBody] = streamed.body.tee();
      await rejection(
        within(
          writeReceipt(store, source, {
            body: storeBody,
            expectedIntegrity: integrityOfBody(hashBody),
          }),
          'two-branch source error did not settle',
        ),
        'database export receipt integrity is malformed',
      );
      expect(bucket.objects).toHaveLength(0);
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    for (const [getMode, message] of [
      ['size-mismatch', 'database export receipt readback failed'],
      ['short', 'database export receipt readback failed'],
      ['read-error', 'database export receipt readback failed'],
      [
        'tamper',
        'database export receipt collision differs from the committed export',
      ],
    ] satisfies readonly (readonly [GetMode, string])[]) {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      seedReceipt(bucket, store, source);
      bucket.getMode = getMode;
      await rejection(writeReceipt(store, source), message);
      expect(bucket.objects.get(receiptKey())).toEqual(source);
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      bucket.onPut = () => {
        bucket.getMode = 'get-reject';
      };
      await rejection(
        writeReceipt(store, source),
        'database export receipt readback failed',
      );
      expect(bucket.objects.get(receiptKey())).toEqual(source);
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    {
      const bucket = new FakeR2Bucket();
      bucket.getMode = 'get-reject';
      const store = createStore(bucket);
      const streamed = streamFrom(source, { stayOpen: true });
      await rejection(
        within(
          writeReceipt(store, source, {
            body: streamed.body,
            expectedIntegrity: new Promise(() => undefined),
          }),
          'preflight get rejection awaited integrity',
        ),
        'database export receipt readback failed',
      );
      expect(streamed.cancellations).toHaveLength(1);
      expect(bucket.putCalls).toBe(0);
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    {
      const bucket = new FakeR2Bucket();
      bucket.getMode = 'get-reject';
      const error = await rejection(
        writeReceipt(createStore(bucket), source, {
          expectedIntegrity: Promise.reject(
            new Error('rejected alongside preflight get'),
          ),
        }),
        'database export receipt readback failed',
      );
      expect(error).not.toHaveProperty('cause');
      expect(bucket.putCalls).toBe(0);
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      seedReceipt(bucket, store, source, { wrong: 'metadata' });
      await rejection(
        within(
          writeReceipt(store, source, {
            expectedIntegrity: new Promise(() => undefined),
          }),
          'metadata mismatch awaited integrity',
        ),
        'database export receipt collision differs from the committed export',
      );
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      seedReceipt(bucket, store, source, { wrong: 'metadata' });
      const error = await rejection(
        writeReceipt(store, source, {
          expectedIntegrity: Promise.reject(
            new Error('rejected alongside metadata mismatch'),
          ),
        }),
        'database export receipt collision differs from the committed export',
      );
      expect(error).not.toHaveProperty('cause');
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      seedReceipt(bucket, store, source);
      bucket.getMode = 'read-error';
      const error = await rejection(
        writeReceipt(store, source, {
          expectedIntegrity: Promise.reject(new Error('source failed')),
        }),
        'database export receipt integrity is malformed',
      );
      expect(error).not.toHaveProperty('cause');
      expect(bucket.deleteCalls).toHaveLength(0);
    }

    {
      const bucket = new FakeR2Bucket();
      const store = createStore(bucket);
      const declared = integrityOf(source);
      const error = await rejection(
        writeReceipt(store, source, {
          expectedIntegrity: Promise.resolve({
            ...declared,
            sha256: '0'.repeat(64),
          }),
        }),
        'database export receipt source integrity differs from the streamed export',
      );
      expect(error).not.toHaveProperty('cause');
      expect(bucket.objects.get(receiptKey())).toEqual(source);
      expect(bucket.deleteCalls).toHaveLength(0);
    }
  });

  it('satisfies the database export store contract', () => {
    const store: DurableDatabaseExportStore = createStore(new FakeR2Bucket());
    expect(store).toBeInstanceOf(R2DatabaseExportStore);
  });
});
