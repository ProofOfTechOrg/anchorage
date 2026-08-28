// SPDX-License-Identifier: Apache-2.0

import type {
  R2Bucket,
  ReadableStream as WorkerReadableStream,
  WritableStream as WorkerWritableStream,
} from '@cloudflare/workers-types';
import type { DurableDatabaseExportStore } from './database-export-store.js';
import { assertFileName, isPortablePathSegment } from './export-file-name.js';

export type DigestStreamConstructor = new (
  algorithm: 'SHA-256',
) => WorkerWritableStream<ArrayBuffer | ArrayBufferView> & {
  readonly digest: Promise<ArrayBuffer>;
  readonly bytesWritten: number | bigint;
};

export type FixedLengthStreamConstructor = new (
  expectedLength: number,
) => {
  readonly readable: WorkerReadableStream<Uint8Array>;
  readonly writable: WorkerWritableStream<Uint8Array>;
};

export interface R2DatabaseExportStoreStreamPrimitives {
  readonly DigestStream: DigestStreamConstructor;
  readonly FixedLengthStream: FixedLengthStreamConstructor;
}

export interface R2DatabaseExportStoreOptions {
  readonly bucket: R2Bucket;
  /** A portable segment used to identify the bucket in returned locations. */
  readonly bucketName: string;
  /** Empty, or portable segments ending in `/`, such as `exports/`. */
  readonly keyPrefix?: string;
  /**
   * Worker stream constructors; a Worker passes
   * `{ DigestStream: crypto.DigestStream, FixedLengthStream }`.
   */
  readonly streams: R2DatabaseExportStoreStreamPrimitives;
  /** UUID source; a Worker passes `() => crypto.randomUUID()`. */
  readonly randomUUID: () => string;
}

interface PreparedUpload {
  readonly key: string;
  readonly contentLength: number;
  readonly fixed: {
    readonly readable: WorkerReadableStream<Uint8Array>;
    readonly writable: WorkerWritableStream<Uint8Array>;
  };
}

/**
 * Streams a database export into R2 and hashes an R2 readback.
 *
 * A signed direct download supplies `contentLength` when its response exposes a
 * usable `Content-Length`. Absence fails closed prior to an R2 upload and keeps
 * the database from being deleted. The Wrangler path supplies the scratch
 * file's size. R2 receives a single-part put backed by `FixedLengthStream`;
 * multipart exports are unsupported.
 *
 * A write attempt mints a UUID key. Cleanup after a committed-object failure
 * removes that key. A cleanup failure, rejected put, or conditional collision
 * can leave an orphan below the database prefix without blocking a retry.
 * `write()` is not idempotent.
 *
 * A failure after the put settles aborts the body pipe. When the body is a
 * `tee()` branch, that abort stays pending until the tee source is
 * exhausted or the other branch is cancelled.
 */
export class R2DatabaseExportStore implements DurableDatabaseExportStore {
  readonly #bucket: R2Bucket;
  readonly #bucketName: string;
  readonly #keyPrefix: string;
  readonly #DigestStream: DigestStreamConstructor;
  readonly #FixedLengthStream: FixedLengthStreamConstructor;
  readonly #randomUUID: () => string;

  constructor(options: R2DatabaseExportStoreOptions) {
    if (
      typeof options.bucket?.put !== 'function' ||
      typeof options.bucket.get !== 'function' ||
      typeof options.bucket.delete !== 'function'
    ) {
      throw new Error(
        'R2DatabaseExportStore requires the Workers R2Bucket put/get/delete interface',
      );
    }
    if (
      typeof options.streams?.DigestStream !== 'function' ||
      typeof options.streams.FixedLengthStream !== 'function'
    ) {
      throw new Error(
        'R2DatabaseExportStore requires the Workers DigestStream and FixedLengthStream constructors',
      );
    }
    if (typeof options.randomUUID !== 'function') {
      throw new Error('R2DatabaseExportStore requires a randomUUID function');
    }
    if (!isPortablePathSegment(options.bucketName)) {
      throw new Error('R2 export bucketName must be one portable path segment');
    }
    if (!isKeyPrefix(options.keyPrefix)) {
      throw new Error(
        'R2 export keyPrefix must be portable path segments each followed by /',
      );
    }
    this.#bucket = options.bucket;
    this.#bucketName = options.bucketName;
    this.#keyPrefix = options.keyPrefix ?? '';
    this.#DigestStream = options.streams.DigestStream;
    this.#FixedLengthStream = options.streams.FixedLengthStream;
    this.#randomUUID = options.randomUUID;
  }

  async write(input: {
    readonly databaseId: string;
    readonly fileName: string;
    readonly body: ReadableStream<Uint8Array>;
    readonly contentLength?: number;
  }): Promise<{
    readonly location: string;
    readonly size: number;
    readonly sha256: string;
  }> {
    const prepared = await Promise.resolve()
      .then(() => this.#prepare(input))
      .catch((error: unknown) => {
        // A tee branch's cancel settles when its sibling drains, so the
        // refusal does not await it.
        void input.body.cancel(error).catch(() => undefined);
        throw error;
      });
    const controller = new AbortController();
    const collision = new Error('R2 export key already exists');
    // A conforming R2Bucket.put returns a promise; a synchronous throw from
    // an injected bucket escapes the fixed-message set.
    const put = this.#bucket.put(prepared.key, prepared.fixed.readable, {
      onlyIf: { etagDoesNotMatch: '*' },
    });
    const pipe = input.body.pipeTo(prepared.fixed.writable, {
      signal: controller.signal,
    });
    // `pipeTo` can reject before locking its destination, and the body can be
    // locked between the check and this call; erroring the fixed writable
    // settles the put.
    void pipe.catch((reason: unknown) => {
      void prepared.fixed.writable.abort(reason).catch(() => undefined);
    });
    void put.then(
      (object) => {
        if (object === null) controller.abort(collision);
      },
      (reason: unknown) => controller.abort(reason),
    );
    const [putState, pipeState] = await Promise.allSettled([put, pipe]);

    if (putState.status === 'rejected') {
      throw new Error('R2 export upload failed', { cause: putState.reason });
    }
    if (putState.value === null) throw collision;
    if (pipeState.status === 'rejected') {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export body did not stream completely', {
          cause: pipeState.reason,
        }),
      );
    }
    if (putState.value.size !== prepared.contentLength) {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export size differs from contentLength'),
      );
    }

    const [storedState] = await Promise.allSettled([
      this.#bucket.get(prepared.key),
    ]);
    if (storedState.status === 'rejected') {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export readback failed', { cause: storedState.reason }),
      );
    }
    const stored = storedState.value;
    if (stored === null) {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export readback found no object'),
      );
    }
    if (stored.size !== prepared.contentLength) {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export readback size differs from contentLength'),
      );
    }

    const [digestConstructorState] = await Promise.allSettled([
      Promise.resolve().then(() => new this.#DigestStream('SHA-256')),
    ]);
    if (digestConstructorState.status === 'rejected') {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export readback failed', {
          cause: digestConstructorState.reason,
        }),
      );
    }
    const digest = digestConstructorState.value;
    const readback: WorkerReadableStream<Uint8Array> = stored.body;
    const [readState, digestState] = await Promise.allSettled([
      readback.pipeTo(digest),
      digest.digest,
    ]);
    if (readState.status === 'rejected') {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export readback failed', { cause: readState.reason }),
      );
    }
    if (digestState.status === 'rejected') {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export readback failed', { cause: digestState.reason }),
      );
    }
    // Commit size, read metadata, and streamed byte count identify distinct
    // disagreement points without buffering the export.
    if (Number(digest.bytesWritten) !== prepared.contentLength) {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export readback length differs from contentLength'),
      );
    }

    return {
      location: `r2://${this.#bucketName}/${prepared.key}`,
      size: prepared.contentLength,
      sha256: toHex(digestState.value),
    };
  }

  #prepare(input: {
    readonly databaseId: string;
    readonly fileName: string;
    readonly body: ReadableStream<Uint8Array>;
    readonly contentLength?: number;
  }): PreparedUpload {
    if (input.body.locked) throw new Error('R2 export body is locked');
    assertFileName(input.fileName);
    if (!isPortablePathSegment(input.databaseId)) {
      throw new Error('R2 export databaseId must be one portable path segment');
    }
    if (input.contentLength === undefined) {
      throw new Error('R2 export requires a known contentLength');
    }
    if (input.contentLength === 0) {
      throw new Error('R2 export refuses an empty body');
    }
    if (!Number.isSafeInteger(input.contentLength) || input.contentLength < 1) {
      throw new Error(
        'R2 export contentLength must be a positive safe integer',
      );
    }
    const uuid = this.#randomUUID();
    if (!isPortablePathSegment(uuid)) {
      throw new Error(
        'R2 export key component must be one portable path segment',
      );
    }
    const key = `${this.#keyPrefix}${input.databaseId}/${uuid}-${input.fileName}`;
    return {
      key,
      contentLength: input.contentLength,
      fixed: new this.#FixedLengthStream(input.contentLength),
    };
  }

  async #failOwned(key: string, error: unknown): Promise<never> {
    const [cleanupState] = await Promise.allSettled([this.#bucket.delete(key)]);
    if (cleanupState.status === 'rejected') {
      throw new AggregateError(
        [error, cleanupState.reason],
        'database export and R2 cleanup failed',
      );
    }
    throw error;
  }
}

function isKeyPrefix(value: string | undefined): boolean {
  if (value === undefined || value === '') return true;
  if (!value.endsWith('/')) return false;
  return value
    .slice(0, -1)
    .split('/')
    .every((segment) => isPortablePathSegment(segment));
}

function toHex(value: ArrayBuffer): string {
  return Array.from(new Uint8Array(value), (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');
}
