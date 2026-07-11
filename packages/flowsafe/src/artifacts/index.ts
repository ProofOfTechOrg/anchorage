// SPDX-License-Identifier: Apache-2.0
// Artifacts — R2-backed workflow artifact storage (Phase 4 "Workflow
// artifacts stored in R2"). Steps write files a run produces (reports,
// exports, generated media) keyed `${workflowId}/${runId}/${name}`, so
// artifacts inherit the run's addressing and can be reclaimed alongside it.
//
// One store implementation over a structural bucket seam: R2ArtifactStore
// speaks the ArtifactBucket subset of Cloudflare's R2Bucket, and
// InMemoryArtifactBucket implements the same subset for tests and local dev
// — the store logic is identical either way. Ids reuse the do-runner's
// PATH_SAFE_ID_PATTERN so anything addressable as a run is addressable as an
// artifact scope, and names are validated segment-wise (no '.'/'..', no
// empty segments) so a name can never escape its run's keyspace.

import type { R2Bucket } from '@cloudflare/workers-types';

import { PATH_SAFE_ID_PATTERN } from '../do-runner/path-safe-id.js';

/** Bodies R2's put() accepts; the in-memory bucket normalizes them all. */
export type ArtifactBody =
  | string
  | ArrayBuffer
  | ArrayBufferView
  | Blob
  | ReadableStream;

/** Structural subset of an R2Object (list results, put's return). */
export interface ArtifactBucketObject {
  readonly key: string;
  readonly size: number;
  readonly uploaded: Date;
  readonly httpMetadata?: { contentType?: string };
  readonly customMetadata?: Record<string, string>;
}

/** Structural subset of an R2ObjectBody (get's return). */
export interface ArtifactBucketObjectBody extends ArtifactBucketObject {
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
}

export interface ArtifactBucketListResult {
  objects: ArtifactBucketObject[];
  truncated: boolean;
  cursor?: string;
}

/** Structural subset of a Cloudflare R2Bucket binding. */
export interface ArtifactBucket {
  put(
    key: string,
    value: ArtifactBody,
    options?: {
      httpMetadata?: { contentType?: string };
      customMetadata?: Record<string, string>;
    },
  ): Promise<ArtifactBucketObject | null>;
  get(key: string): Promise<ArtifactBucketObjectBody | null>;
  delete(key: string): Promise<void>;
  list(options?: {
    prefix?: string;
    cursor?: string;
  }): Promise<ArtifactBucketListResult>;
}

// Compile-time proof that a real Cloudflare R2Bucket satisfies ArtifactBucket,
// so a consumer passes `env.MY_BUCKET` straight into R2ArtifactStore with no
// adapter. Type-only (erased at build; the non-exported alias never reaches the
// emitted .d.ts, so consumers pull no workers-types dependency) — drift in
// either type fails the gate here, the way deploy/worker.ts proves the
// audit-export Queue/MessageBatch subsets by feeding them real bindings.
type AssertTrue<T extends true> = T;
type _R2SatisfiesArtifactBucket = AssertTrue<
  R2Bucket extends ArtifactBucket ? true : false
>;

export interface ArtifactRef {
  workflowId: string;
  runId: string;
  /** Path-safe segments joined by '/', e.g. 'reports/summary.md'. */
  name: string;
}

export interface ArtifactRecord extends ArtifactRef {
  /** Full object key: `[keyPrefix/]workflowId/runId/name`. */
  key: string;
  size?: number;
  contentType?: string;
  /** ISO 8601. */
  uploadedAt?: string;
  metadata?: Record<string, string>;
}

export interface PutArtifactOptions {
  contentType?: string;
  metadata?: Record<string, string>;
}

export interface ArtifactContent {
  record: ArtifactRecord;
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
}

export interface ListArtifactsScope {
  workflowId: string;
  runId: string;
  /** Optional name prefix filter, e.g. 'reports/'. */
  namePrefix?: string;
}

export class InvalidArtifactRefError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidArtifactRefError';
  }
}

// R2's hard key limit; enforced here so an oversized name fails as a clear
// client error instead of an opaque bucket rejection.
const MAX_KEY_BYTES = 1024;

function assertPathSafeId(value: string, field: string): void {
  if (!PATH_SAFE_ID_PATTERN.test(value)) {
    throw new InvalidArtifactRefError(
      `${field} must be URL-path-safe (letters, digits, '.', '_', '~', '-'; 1-200 chars)`,
    );
  }
}

function assertArtifactName(name: string): void {
  const segments = name.split('/');
  if (
    name.length === 0 ||
    segments.some((segment) => !PATH_SAFE_ID_PATTERN.test(segment))
  ) {
    throw new InvalidArtifactRefError(
      "artifact name must be '/'-joined path-safe segments (letters, digits, '.', '_', '~', '-'; no empty, '.', or '..' segments)",
    );
  }
}

export interface R2ArtifactStoreOptions {
  /** Keyspace prefix inside the bucket, e.g. 'artifacts'. Default: none. */
  keyPrefix?: string;
}

/**
 * Workflow artifact store over an R2 bucket (or any ArtifactBucket, e.g.
 * InMemoryArtifactBucket in tests/dev). Typical step usage:
 *
 *   const artifacts = new R2ArtifactStore(env.ARTIFACTS);
 *   await artifacts.put(
 *     { workflowId, runId, name: 'reports/summary.md' },
 *     markdown,
 *     { contentType: 'text/markdown' },
 *   );
 */
export class R2ArtifactStore {
  readonly #bucket: ArtifactBucket;
  readonly #keyPrefix: string;

  constructor(bucket: ArtifactBucket, options: R2ArtifactStoreOptions = {}) {
    if (options.keyPrefix !== undefined) {
      assertArtifactName(options.keyPrefix);
    }
    this.#bucket = bucket;
    this.#keyPrefix = options.keyPrefix ? `${options.keyPrefix}/` : '';
  }

  async put(
    ref: ArtifactRef,
    body: ArtifactBody,
    options: PutArtifactOptions = {},
  ): Promise<ArtifactRecord> {
    const key = this.#key(ref);
    const object = await this.#bucket.put(key, body, {
      httpMetadata: options.contentType
        ? { contentType: options.contentType }
        : undefined,
      customMetadata: options.metadata,
    });
    // R2 returns the written object; a bucket that returns null (allowed by
    // the subset) still yields a correct record from the inputs.
    return object
      ? this.#record(object, ref)
      : {
          ...ref,
          key,
          contentType: options.contentType,
          metadata: options.metadata,
        };
  }

  async get(ref: ArtifactRef): Promise<ArtifactContent | undefined> {
    const object = await this.#bucket.get(this.#key(ref));
    if (!object) return undefined;
    return {
      record: this.#record(object, ref),
      arrayBuffer: () => object.arrayBuffer(),
      text: () => object.text(),
    };
  }

  /** All artifacts of a run (cursor pagination drained), sorted by name. */
  async list(scope: ListArtifactsScope): Promise<ArtifactRecord[]> {
    assertPathSafeId(scope.workflowId, 'workflowId');
    assertPathSafeId(scope.runId, 'runId');
    const runPrefix = `${this.#keyPrefix}${scope.workflowId}/${scope.runId}/`;
    const prefix = `${runPrefix}${scope.namePrefix ?? ''}`;
    const records: ArtifactRecord[] = [];
    let cursor: string | undefined;
    do {
      const page = await this.#bucket.list({ prefix, cursor });
      for (const object of page.objects) {
        records.push(
          this.#record(object, {
            workflowId: scope.workflowId,
            runId: scope.runId,
            name: object.key.slice(runPrefix.length),
          }),
        );
      }
      cursor = page.truncated ? page.cursor : undefined;
    } while (cursor !== undefined);
    return records.sort((a, b) => a.name.localeCompare(b.name));
  }

  async delete(ref: ArtifactRef): Promise<void> {
    await this.#bucket.delete(this.#key(ref));
  }

  /**
   * Delete every artifact of a run; returns the count. Idempotent (a
   * deleted run lists nothing). The purges call it: pass this store as
   * `artifactStore` to BOTH purgeExpiredWorkflowRuns (per aged-out run,
   * with its snapshot row) and purgeTenant (per surviving run at
   * offboarding) — the snapshot rows are the only enumerable record of a
   * run's artifact keys, so an unpaired retention purge strands them.
   */
  async deleteRun(workflowId: string, runId: string): Promise<number> {
    const records = await this.list({ workflowId, runId });
    for (const record of records) {
      await this.#bucket.delete(record.key);
    }
    return records.length;
  }

  #key(ref: ArtifactRef): string {
    assertPathSafeId(ref.workflowId, 'workflowId');
    assertPathSafeId(ref.runId, 'runId');
    assertArtifactName(ref.name);
    const key = `${this.#keyPrefix}${ref.workflowId}/${ref.runId}/${ref.name}`;
    if (new TextEncoder().encode(key).length > MAX_KEY_BYTES) {
      throw new InvalidArtifactRefError(
        `artifact key exceeds R2's ${MAX_KEY_BYTES}-byte limit`,
      );
    }
    return key;
  }

  #record(object: ArtifactBucketObject, ref: ArtifactRef): ArtifactRecord {
    return {
      ...ref,
      key: object.key,
      size: object.size,
      contentType: object.httpMetadata?.contentType,
      uploadedAt: object.uploaded.toISOString(),
      metadata: object.customMetadata,
    };
  }
}

async function collectBytes(body: ArtifactBody): Promise<Uint8Array> {
  if (typeof body === 'string') return new TextEncoder().encode(body);
  if (body instanceof ArrayBuffer) return new Uint8Array(body.slice(0));
  if (ArrayBuffer.isView(body)) {
    return new Uint8Array(
      body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength),
    );
  }
  if (body instanceof Blob) return new Uint8Array(await body.arrayBuffer());
  // ReadableStream — drain it, like R2 would.
  const chunks: Uint8Array[] = [];
  let total = 0;
  const reader = (body as ReadableStream<Uint8Array>).getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    total += value.byteLength;
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

interface StoredObject {
  bytes: Uint8Array;
  uploaded: Date;
  contentType?: string;
  customMetadata?: Record<string, string>;
}

/**
 * ArtifactBucket for tests and local dev — same subset R2 implements, one
 * page per list() (truncated: false), bodies normalized to bytes on put.
 */
export class InMemoryArtifactBucket implements ArtifactBucket {
  readonly #objects = new Map<string, StoredObject>();

  async put(
    key: string,
    value: ArtifactBody,
    options?: {
      httpMetadata?: { contentType?: string };
      customMetadata?: Record<string, string>;
    },
  ): Promise<ArtifactBucketObject | null> {
    const stored: StoredObject = {
      bytes: await collectBytes(value),
      uploaded: new Date(),
      contentType: options?.httpMetadata?.contentType,
      customMetadata: options?.customMetadata,
    };
    this.#objects.set(key, stored);
    return this.#object(key, stored);
  }

  async get(key: string): Promise<ArtifactBucketObjectBody | null> {
    const stored = this.#objects.get(key);
    if (!stored) return null;
    return {
      ...this.#object(key, stored),
      arrayBuffer: async () =>
        stored.bytes.buffer.slice(
          stored.bytes.byteOffset,
          stored.bytes.byteOffset + stored.bytes.byteLength,
        ) as ArrayBuffer,
      text: async () => new TextDecoder().decode(stored.bytes),
    };
  }

  async delete(key: string): Promise<void> {
    this.#objects.delete(key);
  }

  async list(options?: {
    prefix?: string;
    cursor?: string;
  }): Promise<ArtifactBucketListResult> {
    const prefix = options?.prefix ?? '';
    const objects = [...this.#objects.entries()]
      .filter(([key]) => key.startsWith(prefix))
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, stored]) => this.#object(key, stored));
    return { objects, truncated: false };
  }

  #object(key: string, stored: StoredObject): ArtifactBucketObject {
    return {
      key,
      size: stored.bytes.byteLength,
      uploaded: stored.uploaded,
      httpMetadata:
        stored.contentType !== undefined
          ? { contentType: stored.contentType }
          : undefined,
      customMetadata: stored.customMetadata,
    };
  }
}
