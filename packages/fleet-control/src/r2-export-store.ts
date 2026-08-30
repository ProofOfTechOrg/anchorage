// SPDX-License-Identifier: Apache-2.0

import type {
  R2Bucket,
  R2Object,
  R2ObjectBody,
  ReadableStream as WorkerReadableStream,
  WritableStream as WorkerWritableStream,
} from '@cloudflare/workers-types';
import {
  cancelBodyWithoutAwait,
  captureDatabaseExportIntegrityPromise,
  type DurableDatabaseExportStore,
  databaseExportReceiptAuthorityFromUnknown,
  databaseExportReceiptError,
  databaseExportReceiptIdentityFromUnknown,
} from './database-export-store.js';
import { assertFileName, isPortablePathSegment } from './export-file-name.js';
import type {
  DatabaseExportIntegrity,
  DatabaseExportReceiptIdentity,
} from './types.js';

const TEXT_ENCODER_ENCODE = TextEncoder.prototype.encode;
const textEncoder = new TextEncoder();
const RECEIPT_METADATA_KEYS = [
  'anchorageReceiptVersion',
  'anchorageReceiptAuthority',
  'anchorageDatabaseId',
  'anchorageOperationId',
] as const;

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
  /**
   * Must return a conforming Workers paired stream. If an injected writable
   * fails to error its paired readable when abort is requested, the associated
   * R2 put may not settle.
   */
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

interface ReceiptMetadata extends Record<string, string> {
  readonly anchorageReceiptVersion: '1';
  readonly anchorageReceiptAuthority: string;
  readonly anchorageDatabaseId: string;
  readonly anchorageOperationId: string;
}

interface PreparedReceiptUpload {
  readonly identity: DatabaseExportReceiptIdentity;
  readonly key: string;
  readonly location: string;
  readonly contentLength: number;
  readonly metadata: ReceiptMetadata;
}

type ReceiptLookup =
  | { readonly status: 'absent' }
  | { readonly status: 'present'; readonly object: R2ObjectBody };

/**
 * Streams a database export into R2 and hashes an R2 readback.
 *
 * A signed direct download supplies `contentLength` when its response exposes a
 * usable `Content-Length`. Absence fails closed prior to an R2 upload and keeps
 * the database from being deleted. The Wrangler path supplies the scratch
 * file's size. R2 receives a single-part put backed by `FixedLengthStream`;
 * multipart exports are unsupported.
 *
 * A legacy `write()` attempt mints a UUID key. Cleanup after a committed-object failure
 * removes that key. A cleanup failure, rejected put, or conditional collision
 * can leave an orphan below the database prefix without blocking a retry.
 * `write()` is not idempotent.
 *
 * `writeReceipt()` instead derives one immutable operation key, publishes with
 * a conditional put, and converges only after exact metadata and byte
 * integrity readback. It never deletes that stable key on any receipt path.
 *
 * A failure after the put settles aborts the body pipe. When the body is a
 * `tee()` branch, that abort can stay pending: it settles when the tee
 * source is exhausted or errors, or the other branch is cancelled.
 */
export class R2DatabaseExportStore implements DurableDatabaseExportStore {
  readonly #bucket: R2Bucket;
  readonly #bucketName: string;
  readonly #keyPrefix: string;
  readonly #DigestStream: DigestStreamConstructor;
  readonly #FixedLengthStream: FixedLengthStreamConstructor;
  readonly #randomUUID: () => string;
  declare readonly receiptAuthority: string;
  declare readonly writeReceipt: NonNullable<
    DurableDatabaseExportStore['writeReceipt']
  >;

  constructor(options: R2DatabaseExportStoreOptions) {
    const bucket = options.bucket;
    const streams = options.streams;
    const randomUUID = options.randomUUID;
    const bucketName = options.bucketName;
    const keyPrefix = options.keyPrefix;
    if (
      typeof bucket?.put !== 'function' ||
      typeof bucket.get !== 'function' ||
      typeof bucket.delete !== 'function'
    ) {
      throw new Error(
        'R2DatabaseExportStore requires the Workers R2Bucket put/get/delete interface',
      );
    }
    if (
      typeof streams?.DigestStream !== 'function' ||
      typeof streams.FixedLengthStream !== 'function'
    ) {
      throw new Error(
        'R2DatabaseExportStore requires the Workers DigestStream and FixedLengthStream constructors',
      );
    }
    if (typeof randomUUID !== 'function') {
      throw new Error('R2DatabaseExportStore requires a randomUUID function');
    }
    if (!isPortablePathSegment(bucketName)) {
      throw new Error('R2 export bucketName must be one portable path segment');
    }
    if (!isKeyPrefix(keyPrefix)) {
      throw new Error(
        'R2 export keyPrefix must be portable path segments each followed by /',
      );
    }
    this.#bucket = bucket;
    this.#bucketName = bucketName;
    this.#keyPrefix = keyPrefix ?? '';
    this.#DigestStream = streams.DigestStream;
    this.#FixedLengthStream = streams.FixedLengthStream;
    this.#randomUUID = randomUUID;
    const receiptAuthority = databaseExportReceiptAuthorityFromUnknown(
      `r2://${this.#bucketName}/${this.#keyPrefix}receipts/v1`,
    );
    Object.defineProperties(this, {
      receiptAuthority: {
        configurable: false,
        enumerable: true,
        value: receiptAuthority,
        writable: false,
      },
      writeReceipt: {
        configurable: false,
        enumerable: true,
        value: (input: unknown) => this.#writeReceipt(input, receiptAuthority),
        writable: false,
      },
    });
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
        // A tee branch's cancel settles when the tee source is exhausted
        // or errors, or the other branch is cancelled, so the refusal does
        // not await it.
        try {
          void input.body.cancel(error).catch(() => undefined);
        } catch {}
        throw error;
      });
    const controller = new AbortController();
    const collision = new Error('R2 export key already exists');
    // The wrapper prevents promise assimilation: the provider call starts
    // before the pipe without waiting for the provider promise to settle.
    const putStartState = await settled(() => ({
      operation: this.#bucket.put(prepared.key, prepared.fixed.readable, {
        onlyIf: { etagDoesNotMatch: '*' },
      }),
    }));
    if (putStartState.status === 'rejected') {
      try {
        void input.body.cancel(putStartState.reason).catch(() => undefined);
      } catch {}
      throw new Error('R2 export upload failed', {
        cause: putStartState.reason,
      });
    }
    const put = putStartState.value.operation;
    const pipe = funnel(() =>
      input.body.pipeTo(prepared.fixed.writable, {
        signal: controller.signal,
      }),
    );
    // `pipeTo` can reject before locking its destination, and the body can be
    // locked between the check and this call; erroring the fixed writable
    // settles the put.
    void pipe.catch((reason: unknown) => {
      void settled(() => prepared.fixed.writable.abort(reason));
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
    const uploaded = putState.value;
    if (uploaded === null) throw collision;
    if (pipeState.status === 'rejected') {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export body did not stream completely', {
          cause: pipeState.reason,
        }),
      );
    }
    const uploadedSize = await this.#ownedValue(
      prepared.key,
      'R2 export upload failed',
      () => uploaded.size,
    );
    if (uploadedSize !== prepared.contentLength) {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export size differs from contentLength'),
      );
    }

    const storedState = await settled(() => this.#bucket.get(prepared.key));
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
    const storedSize = await this.#ownedValue(
      prepared.key,
      'R2 export readback failed',
      () => stored.size,
    );
    if (storedSize !== prepared.contentLength) {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export readback size differs from contentLength'),
      );
    }

    const digestConstructorState = await settled(
      () => new this.#DigestStream('SHA-256'),
    );
    if (digestConstructorState.status === 'rejected') {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export readback failed', {
          cause: digestConstructorState.reason,
        }),
      );
    }
    const digest = digestConstructorState.value;
    const readback: WorkerReadableStream<Uint8Array> = await this.#ownedValue(
      prepared.key,
      'R2 export readback failed',
      () => stored.body,
    );
    // Capture without assimilating: the pipe must start before this promise is
    // awaited. Observe rejection now, but await settlement only after the pipe.
    const capturedDigest = await this.#ownedValue(
      prepared.key,
      'R2 export readback failed',
      () => {
        const promise = digest.digest;
        void promise.catch(() => undefined);
        return { promise };
      },
    );
    const digestSettlement = settled(() => capturedDigest.promise);
    const readState = await settled(() => readback.pipeTo(digest));
    if (readState.status === 'rejected') {
      void settled(() => digest.abort(readState.reason));
      return this.#failOwned(
        prepared.key,
        new Error('R2 export readback failed', { cause: readState.reason }),
      );
    }
    const digestState = await digestSettlement;
    if (digestState.status === 'rejected') {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export readback failed', { cause: digestState.reason }),
      );
    }
    // Commit size, read metadata, and streamed byte count identify distinct
    // disagreement points without buffering the export.
    const bytesWritten = await this.#ownedValue(
      prepared.key,
      'R2 export readback failed',
      () => Number(digest.bytesWritten),
    );
    if (bytesWritten !== prepared.contentLength) {
      return this.#failOwned(
        prepared.key,
        new Error('R2 export readback length differs from contentLength'),
      );
    }
    const sha256 = await this.#ownedValue(
      prepared.key,
      'R2 export readback failed',
      () => toHex(digestState.value),
    );

    return {
      location: `r2://${this.#bucketName}/${prepared.key}`,
      size: prepared.contentLength,
      sha256,
    };
  }

  async #writeReceipt(inputValue: unknown, authority: string) {
    const body = readReceiptInputField(
      inputValue,
      'body',
      'integrity-malformed',
    );
    let cancellationAttempted = false;
    const cancel = (reason: unknown) => {
      if (cancellationAttempted) return;
      cancellationAttempted = true;
      cancelBodyWithoutAwait(body, reason);
    };

    let expected: Promise<DatabaseExportIntegrity>;
    try {
      const expectedValue = readReceiptInputField(
        inputValue,
        'expectedIntegrity',
        'integrity-malformed',
      );
      expected = captureDatabaseExportIntegrityPromise(expectedValue);
    } catch (error) {
      cancel(error);
      throw error;
    }

    try {
      const prepared = this.#prepareReceipt(inputValue, authority, body);
      const preflight = await this.#lookupReceipt(
        prepared.key,
        prepared.identity,
      );
      if (preflight.status === 'present') {
        cancel(databaseExportReceiptError('collision'));
        return await this.#selectExistingReceipt(
          preflight.object,
          prepared,
          expected,
        );
      }

      let fixed: {
        readonly readable: WorkerReadableStream<Uint8Array>;
        readonly writable: WorkerWritableStream<Uint8Array>;
      };
      try {
        const stream = new this.#FixedLengthStream(prepared.contentLength);
        fixed = { readable: stream.readable, writable: stream.writable };
      } catch (error) {
        cancel(error);
        throw error;
      }

      const controller = new AbortController();
      const putStart = await settled(() => ({
        operation: this.#bucket.put(prepared.key, fixed.readable, {
          onlyIf: { etagDoesNotMatch: '*' },
          customMetadata: prepared.metadata,
        }),
      }));
      if (putStart.status === 'rejected') {
        cancel(putStart.reason);
        abortWritableWithoutAwait(fixed.writable, putStart.reason);
        const expectedState = await settled(() => expected);
        return this.#recoverRejectedReceiptPut(
          prepared,
          expectedState,
          putStart.reason,
        );
      }

      const put = funnel(() => putStart.value.operation);
      void put.then(
        (object) => {
          if (object === null) {
            controller.abort(databaseExportReceiptError('collision'));
          }
        },
        (reason: unknown) => controller.abort(reason),
      );
      const pipe = funnel(() =>
        (body as ReadableStream<Uint8Array>).pipeTo(
          fixed.writable as WritableStream<Uint8Array>,
          { signal: controller.signal },
        ),
      );
      void pipe.catch((reason: unknown) => {
        abortWritableWithoutAwait(fixed.writable, reason);
      });
      const [putState, pipeState, expectedState] = await Promise.allSettled([
        put,
        pipe,
        expected,
      ]);
      if (expectedState.status === 'rejected') throw expectedState.reason;
      if (expectedState.value.size !== prepared.contentLength) {
        throw databaseExportReceiptError('source-mismatch');
      }
      if (putState.status === 'rejected') {
        return await this.#recoverRejectedReceiptPut(
          prepared,
          expectedState,
          putState.reason,
        );
      }
      if (putState.value === null) {
        return await this.#recoverConditionalCollision(
          prepared,
          expectedState.value,
        );
      }
      if (pipeState.status === 'rejected') {
        throw new Error('R2 export body did not stream completely', {
          cause: pipeState.reason,
        });
      }
      if (!hasExactReceiptMetadata(putState.value, prepared.metadata)) {
        throw databaseExportReceiptError('readback');
      }
      if (readR2ObjectSize(putState.value) !== prepared.contentLength) {
        throw databaseExportReceiptError('readback');
      }

      const committed = await this.#lookupReceipt(
        prepared.key,
        prepared.identity,
      );
      if (committed.status === 'absent') {
        throw databaseExportReceiptError('readback');
      }
      const receipt = await this.#hashReceiptObject(
        committed.object,
        prepared.location,
      );
      if (!sameIntegrity(receipt, expectedState.value)) {
        throw databaseExportReceiptError('source-mismatch');
      }
      return receipt;
    } catch (error) {
      cancel(error);
      throw error;
    }
  }

  #prepareReceipt(
    inputValue: unknown,
    authority: string,
    body: unknown,
  ): PreparedReceiptUpload {
    const identity = databaseExportReceiptIdentityFromUnknown(
      readReceiptInputField(inputValue, 'identity', 'identity-malformed'),
      authority,
    );
    const contentLength = readReceiptInputField(
      inputValue,
      'contentLength',
      'integrity-malformed',
    );
    if (
      typeof contentLength !== 'number' ||
      !Number.isSafeInteger(contentLength) ||
      contentLength < 1
    ) {
      throw new Error(
        'database export receipt contentLength must be a positive safe integer',
      );
    }
    let locked: unknown;
    try {
      if (
        (typeof body !== 'object' || body === null) &&
        typeof body !== 'function'
      ) {
        throw new Error('database export receipt body is malformed');
      }
      locked = Reflect.get(body, 'locked');
    } catch {
      throw new Error('database export receipt body is malformed');
    }
    if (locked !== false) {
      throw new Error('database export receipt body is locked or malformed');
    }
    const key = `${this.#keyPrefix}receipts/v1/${identity.databaseId}/${identity.operationId}.sql`;
    if (utf8Length(key) > 1_024) {
      throw databaseExportReceiptError('key-too-long');
    }
    return {
      identity,
      key,
      location: `r2://${this.#bucketName}/${key}`,
      contentLength,
      metadata: receiptMetadata(identity),
    };
  }

  async #lookupReceipt(
    key: string,
    identity: DatabaseExportReceiptIdentity,
  ): Promise<ReceiptLookup> {
    const state = await settled(() => this.#bucket.get(key));
    if (state.status === 'rejected') {
      throw databaseExportReceiptError('readback');
    }
    if (state.value === null) return { status: 'absent' };
    if (!hasExactReceiptMetadata(state.value, receiptMetadata(identity))) {
      throw databaseExportReceiptError('collision');
    }
    return { status: 'present', object: state.value };
  }

  async #selectExistingReceipt(
    object: R2ObjectBody,
    prepared: PreparedReceiptUpload,
    expected: Promise<DatabaseExportIntegrity>,
  ) {
    const [readbackState, expectedState] = await Promise.allSettled([
      this.#hashReceiptObject(object, prepared.location),
      expected,
    ]);
    if (expectedState.status === 'rejected') throw expectedState.reason;
    if (readbackState.status === 'rejected') throw readbackState.reason;
    if (expectedState.value.size !== prepared.contentLength) {
      throw databaseExportReceiptError('source-mismatch');
    }
    if (!sameIntegrity(readbackState.value, expectedState.value)) {
      throw databaseExportReceiptError('collision');
    }
    return readbackState.value;
  }

  async #recoverRejectedReceiptPut(
    prepared: PreparedReceiptUpload,
    expectedState: PromiseSettledResult<DatabaseExportIntegrity>,
    reason: unknown,
  ) {
    if (expectedState.status === 'rejected') throw expectedState.reason;
    if (expectedState.value.size !== prepared.contentLength) {
      throw databaseExportReceiptError('source-mismatch');
    }
    const winner = await this.#lookupReceipt(prepared.key, prepared.identity);
    if (winner.status === 'absent') {
      throw new Error('R2 export upload failed', { cause: reason });
    }
    const receipt = await this.#hashReceiptObject(
      winner.object,
      prepared.location,
    );
    if (!sameIntegrity(receipt, expectedState.value)) {
      throw databaseExportReceiptError('collision');
    }
    return receipt;
  }

  async #recoverConditionalCollision(
    prepared: PreparedReceiptUpload,
    expected: DatabaseExportIntegrity,
  ) {
    const winner = await this.#lookupReceipt(prepared.key, prepared.identity);
    if (winner.status === 'absent') {
      throw databaseExportReceiptError('collision');
    }
    const receipt = await this.#hashReceiptObject(
      winner.object,
      prepared.location,
    );
    if (!sameIntegrity(receipt, expected)) {
      throw databaseExportReceiptError('collision');
    }
    return receipt;
  }

  async #hashReceiptObject(object: R2ObjectBody, location: string) {
    let reportedSize: number;
    let digest: InstanceType<DigestStreamConstructor>;
    let readback: WorkerReadableStream<Uint8Array>;
    let digestPromise: Promise<ArrayBuffer>;
    try {
      reportedSize = readR2ObjectSize(object);
      digest = new this.#DigestStream('SHA-256');
      readback = object.body;
      digestPromise = digest.digest;
    } catch {
      throw databaseExportReceiptError('readback');
    }
    const digestStatePromise = settled(() => digestPromise);
    const readState = await settled(() => readback.pipeTo(digest));
    if (readState.status === 'rejected') {
      abortWritableWithoutAwait(digest, readState.reason);
      throw databaseExportReceiptError('readback');
    }
    const digestState = await digestStatePromise;
    if (digestState.status === 'rejected') {
      throw databaseExportReceiptError('readback');
    }
    try {
      const bytesWritten = Number(digest.bytesWritten);
      if (
        !Number.isSafeInteger(bytesWritten) ||
        bytesWritten !== reportedSize
      ) {
        throw databaseExportReceiptError('readback');
      }
      return {
        location,
        size: reportedSize,
        sha256: toHex(digestState.value),
      };
    } catch {
      throw databaseExportReceiptError('readback');
    }
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
    const fixed = new this.#FixedLengthStream(input.contentLength);
    return {
      key,
      contentLength: input.contentLength,
      fixed: {
        readable: fixed.readable,
        writable: fixed.writable,
      },
    };
  }

  async #ownedValue<T>(
    key: string,
    message: 'R2 export upload failed' | 'R2 export readback failed',
    operation: () => T | PromiseLike<T>,
  ): Promise<Awaited<T>> {
    const state = await settled(operation);
    if (state.status === 'rejected') {
      return this.#failOwned(key, new Error(message, { cause: state.reason }));
    }
    return state.value;
  }

  async #failOwned(key: string, error: unknown): Promise<never> {
    const cleanupState = await settled(() => this.#bucket.delete(key));
    if (cleanupState.status === 'rejected') {
      throw new AggregateError(
        [error, cleanupState.reason],
        'database export and R2 cleanup failed',
      );
    }
    throw error;
  }
}

function readReceiptInputField(
  input: unknown,
  field: 'body' | 'contentLength' | 'expectedIntegrity' | 'identity',
  errorKind: 'identity-malformed' | 'integrity-malformed',
): unknown {
  try {
    if (
      (typeof input !== 'object' || input === null) &&
      typeof input !== 'function'
    ) {
      throw databaseExportReceiptError(errorKind);
    }
    return Reflect.get(input, field);
  } catch {
    throw databaseExportReceiptError(errorKind);
  }
}

function receiptMetadata(
  identity: DatabaseExportReceiptIdentity,
): ReceiptMetadata {
  return Object.freeze({
    anchorageReceiptVersion: '1',
    anchorageReceiptAuthority: identity.authority,
    anchorageDatabaseId: identity.databaseId,
    anchorageOperationId: identity.operationId,
  });
}

function hasExactReceiptMetadata(
  object: R2Object,
  expected: ReceiptMetadata,
): boolean {
  try {
    const value = Reflect.get(object, 'customMetadata');
    if (typeof value !== 'object' || value === null || Array.isArray(value)) {
      return false;
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) return false;
    const descriptors = Object.getOwnPropertyDescriptors(value);
    const keys = Reflect.ownKeys(descriptors);
    if (
      keys.length !== RECEIPT_METADATA_KEYS.length ||
      keys.some(
        (key) =>
          typeof key !== 'string' ||
          !RECEIPT_METADATA_KEYS.includes(
            key as (typeof RECEIPT_METADATA_KEYS)[number],
          ),
      )
    ) {
      return false;
    }
    return RECEIPT_METADATA_KEYS.every((key) => {
      const descriptor = descriptors[key];
      return (
        descriptor !== undefined &&
        'value' in descriptor &&
        descriptor.enumerable === true &&
        typeof descriptor.value === 'string' &&
        descriptor.value === expected[key]
      );
    });
  } catch {
    return false;
  }
}

function readR2ObjectSize(object: R2Object): number {
  let size: unknown;
  try {
    size = Reflect.get(object, 'size');
  } catch {
    throw databaseExportReceiptError('readback');
  }
  if (typeof size !== 'number' || !Number.isSafeInteger(size) || size < 1) {
    throw databaseExportReceiptError('readback');
  }
  return size;
}

function sameIntegrity(
  left: DatabaseExportIntegrity,
  right: DatabaseExportIntegrity,
): boolean {
  return left.size === right.size && left.sha256 === right.sha256;
}

function abortWritableWithoutAwait(
  writable: { abort(reason?: unknown): Promise<void> },
  reason: unknown,
): void {
  void settled(() => writable.abort(reason));
}

function utf8Length(value: string): number {
  return Reflect.apply(TEXT_ENCODER_ENCODE, textEncoder, [value]).byteLength;
}

async function settled<T>(
  operation: () => T | PromiseLike<T>,
): Promise<PromiseSettledResult<Awaited<T>>> {
  // Resolving through a promise makes a synchronous throw a rejection.
  const [state] = await Promise.allSettled([funnel(operation)]);
  return state;
}

async function funnel<T>(operation: () => T | PromiseLike<T>) {
  return operation();
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
