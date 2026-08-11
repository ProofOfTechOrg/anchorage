// SPDX-License-Identifier: Apache-2.0
// Deployment-wide signal-provider subscriptions. Physical deployment isolation
// makes every row belong to the one organization bound to this D1 database.

import type { SignalSubscription } from '@mastra/core/signals';

import { isPathSafeId } from '../do-runner/index.js';
import {
  d1Changes,
  dateOrUndefined,
  isoOrNull,
  parseJsonOrUndefined,
  type SignalDatabase,
} from '../signals/d1-shared.js';
import { PROVIDER_ID_PATTERN } from './provider.js';

export const SIGNAL_SUBSCRIPTIONS_TABLE = 'flowsafe_signal_subscriptions';

/** Maximum UTF-8 size of an opaque provider resource key. */
export const MAX_EXTERNAL_RESOURCE_ID_BYTES = 1024;
const textEncoder = new TextEncoder();

export type StoredSubscription = SignalSubscription;

export interface SubscribeInput {
  providerId: string;
  /** Opaque provider key, bounded to 1024 UTF-8 bytes and free of ASCII controls. */
  externalResourceId: string;
  threadId: string;
  resourceId: string;
  metadata?: Record<string, unknown>;
}

export interface SubscriptionStore {
  subscribe(input: SubscribeInput): Promise<StoredSubscription>;
  unsubscribe(
    providerId: string,
    externalResourceId: string,
    threadId: string,
  ): Promise<boolean>;
  listForThread(threadId: string): Promise<StoredSubscription[]>;
  listForProvider(providerId: string): Promise<StoredSubscription[]>;
  listByResource(
    providerId: string,
    externalResourceId: string,
  ): Promise<StoredSubscription[]>;
}

export interface SubscriptionStoreFactory {
  store(): SubscriptionStore;
}

interface SubscriptionRow {
  id: string;
  provider_id: string;
  thread_id: string;
  resource_id: string;
  external_resource_id: string;
  subscribed_at: string;
  metadata: string | null;
}

const SCHEMA_STATEMENTS: readonly string[] = [
  `CREATE TABLE IF NOT EXISTS ${SIGNAL_SUBSCRIPTIONS_TABLE} (
    id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    external_resource_id TEXT NOT NULL,
    subscribed_at TEXT NOT NULL,
    metadata TEXT
  )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS ${SIGNAL_SUBSCRIPTIONS_TABLE}_key
    ON ${SIGNAL_SUBSCRIPTIONS_TABLE} (provider_id, external_resource_id, thread_id)`,
  `CREATE INDEX IF NOT EXISTS ${SIGNAL_SUBSCRIPTIONS_TABLE}_resource
    ON ${SIGNAL_SUBSCRIPTIONS_TABLE} (provider_id, external_resource_id)`,
  `CREATE INDEX IF NOT EXISTS ${SIGNAL_SUBSCRIPTIONS_TABLE}_provider
    ON ${SIGNAL_SUBSCRIPTIONS_TABLE} (provider_id)`,
];

async function createSubscriptionSchema(db: SignalDatabase): Promise<void> {
  const existing = await db
    .prepare(`PRAGMA table_info(${SIGNAL_SUBSCRIPTIONS_TABLE})`)
    .all<{ name: string }>();
  if (existing.results.some((column) => column.name === 'tenant_id')) {
    throw new Error(
      `${SIGNAL_SUBSCRIPTIONS_TABLE} uses the retired pooled-tenant schema — recreate the database before serving this single-deployment release`,
    );
  }
  for (const statement of SCHEMA_STATEMENTS) {
    await db.prepare(statement).run();
  }
}

function rowToSubscription(row: SubscriptionRow): StoredSubscription {
  const metadata = parseJsonOrUndefined<unknown>(row.metadata);
  return {
    id: row.id,
    providerId: row.provider_id,
    threadId: row.thread_id,
    resourceId: row.resource_id,
    externalResourceId: row.external_resource_id,
    subscribedAt: dateOrUndefined(row.subscribed_at) ?? new Date(0),
    metadata:
      metadata !== null &&
      typeof metadata === 'object' &&
      !Array.isArray(metadata)
        ? (metadata as Record<string, unknown>)
        : {},
  };
}

function assertProviderId(
  value: unknown,
  operation: string,
): asserts value is string {
  if (typeof value !== 'string' || !PROVIDER_ID_PATTERN.test(value)) {
    throw new Error(`${operation}: providerId is invalid`);
  }
}

function assertExternalResourceId(
  value: unknown,
  operation: string,
): asserts value is string {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`${operation}: externalResourceId is required`);
  }
  for (const character of value) {
    const code = character.charCodeAt(0);
    if (code <= 0x1f || code === 0x7f) {
      throw new Error(
        `${operation}: externalResourceId must not contain ASCII control characters`,
      );
    }
  }
  if (textEncoder.encode(value).byteLength > MAX_EXTERNAL_RESOURCE_ID_BYTES) {
    throw new Error(
      `${operation}: externalResourceId exceeds ${MAX_EXTERNAL_RESOURCE_ID_BYTES} UTF-8 bytes`,
    );
  }
}

/** Whether a value is a non-empty, bounded provider resource key. */
export function isValidExternalResourceId(value: unknown): value is string {
  try {
    assertExternalResourceId(value, 'external resource');
    return true;
  } catch {
    return false;
  }
}

function assertThreadId(
  value: unknown,
  operation: string,
): asserts value is string {
  if (!isPathSafeId(value)) {
    throw new Error(`${operation}: threadId is not path-safe`);
  }
}

interface CanonicalSubscribeInput {
  readonly providerId: string;
  readonly externalResourceId: string;
  readonly threadId: string;
  readonly resourceId: string;
  readonly metadata?: Record<string, unknown>;
  readonly metadataJson: string | null;
}

function canonicalMetadata(value: unknown): {
  metadata: Record<string, unknown>;
  metadataJson: string;
} {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('subscribe: metadata must be an object');
  }
  let metadataJson: string | undefined;
  try {
    metadataJson = JSON.stringify(value);
  } catch {
    throw new Error('subscribe: metadata must be JSON-serializable');
  }
  if (metadataJson === undefined) {
    throw new Error('subscribe: metadata must be JSON-serializable');
  }
  const metadata = JSON.parse(metadataJson) as unknown;
  if (
    metadata === null ||
    typeof metadata !== 'object' ||
    Array.isArray(metadata)
  ) {
    throw new Error('subscribe: metadata must serialize to an object');
  }
  return {
    metadata: metadata as Record<string, unknown>,
    metadataJson,
  };
}

function canonicalSubscriptionInput(
  input: SubscribeInput,
): CanonicalSubscribeInput {
  if (input === null || typeof input !== 'object' || Array.isArray(input)) {
    throw new Error('subscribe: input must be an object');
  }
  let descriptors: PropertyDescriptorMap;
  try {
    descriptors = Object.getOwnPropertyDescriptors(input);
  } catch {
    throw new Error('subscribe: input properties could not be read');
  }
  const field = (name: keyof SubscribeInput): unknown => {
    const descriptor = descriptors[name];
    if (descriptor === undefined) return undefined;
    if (!('value' in descriptor)) {
      throw new Error(`subscribe: ${name} must be an own data property`);
    }
    return descriptor.value;
  };
  const providerId = field('providerId');
  const externalResourceId = field('externalResourceId');
  const threadId = field('threadId');
  const resourceId = field('resourceId');
  const metadataValue = field('metadata');

  assertProviderId(providerId, 'subscribe');
  assertExternalResourceId(externalResourceId, 'subscribe');
  assertThreadId(threadId, 'subscribe');
  if (!isPathSafeId(resourceId)) {
    throw new Error('subscribe: resourceId is not path-safe');
  }
  const canonicalMetadataValue =
    metadataValue === undefined ? undefined : canonicalMetadata(metadataValue);
  return Object.freeze({
    providerId,
    externalResourceId,
    threadId,
    resourceId,
    ...(canonicalMetadataValue === undefined
      ? {}
      : { metadata: canonicalMetadataValue.metadata }),
    metadataJson: canonicalMetadataValue?.metadataJson ?? null,
  });
}

function assertSubscriptionId(id: string): void {
  if (!isPathSafeId(id)) {
    throw new Error('subscribe: generated subscription id is not path-safe');
  }
}

class D1SubscriptionStore implements SubscriptionStore {
  readonly #db: SignalDatabase;
  readonly #ready: () => Promise<void>;
  readonly #uuid: () => string;

  constructor(
    db: SignalDatabase,
    options: { ready: () => Promise<void>; uuid: () => string },
  ) {
    this.#db = db;
    this.#ready = options.ready;
    this.#uuid = options.uuid;
  }

  async subscribe(input: SubscribeInput): Promise<StoredSubscription> {
    const canonical = canonicalSubscriptionInput(input);
    await this.#ready();
    const id = this.#uuid();
    assertSubscriptionId(id);
    const row = await this.#db
      .prepare(
        `INSERT INTO ${SIGNAL_SUBSCRIPTIONS_TABLE}
           (id, provider_id, thread_id, resource_id, external_resource_id, subscribed_at, metadata)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (provider_id, external_resource_id, thread_id)
         DO UPDATE SET resource_id = excluded.resource_id,
                       metadata = COALESCE(excluded.metadata, metadata)
         RETURNING *`,
      )
      .bind(
        id,
        canonical.providerId,
        canonical.threadId,
        canonical.resourceId,
        canonical.externalResourceId,
        isoOrNull(new Date()),
        canonical.metadataJson,
      )
      .first<SubscriptionRow>();
    if (row === null) throw new Error('subscribe: upsert returned no row');
    return rowToSubscription(row);
  }

  async unsubscribe(
    providerId: string,
    externalResourceId: string,
    threadId: string,
  ): Promise<boolean> {
    assertProviderId(providerId, 'unsubscribe');
    assertExternalResourceId(externalResourceId, 'unsubscribe');
    assertThreadId(threadId, 'unsubscribe');
    await this.#ready();
    const result = await this.#db
      .prepare(
        `DELETE FROM ${SIGNAL_SUBSCRIPTIONS_TABLE}
         WHERE provider_id = ? AND external_resource_id = ? AND thread_id = ?`,
      )
      .bind(providerId, externalResourceId, threadId)
      .run();
    return d1Changes(result) > 0;
  }

  async listForThread(threadId: string): Promise<StoredSubscription[]> {
    assertThreadId(threadId, 'listForThread');
    await this.#ready();
    const { results } = await this.#db
      .prepare(
        `SELECT * FROM ${SIGNAL_SUBSCRIPTIONS_TABLE}
         WHERE thread_id = ? ORDER BY subscribed_at`,
      )
      .bind(threadId)
      .all<SubscriptionRow>();
    return results.map(rowToSubscription);
  }

  async listForProvider(providerId: string): Promise<StoredSubscription[]> {
    assertProviderId(providerId, 'listForProvider');
    await this.#ready();
    const { results } = await this.#db
      .prepare(
        `SELECT * FROM ${SIGNAL_SUBSCRIPTIONS_TABLE}
         WHERE provider_id = ? ORDER BY subscribed_at`,
      )
      .bind(providerId)
      .all<SubscriptionRow>();
    return results.map(rowToSubscription);
  }

  async listByResource(
    providerId: string,
    externalResourceId: string,
  ): Promise<StoredSubscription[]> {
    assertProviderId(providerId, 'listByResource');
    assertExternalResourceId(externalResourceId, 'listByResource');
    await this.#ready();
    const { results } = await this.#db
      .prepare(
        `SELECT * FROM ${SIGNAL_SUBSCRIPTIONS_TABLE}
         WHERE provider_id = ? AND external_resource_id = ? ORDER BY subscribed_at`,
      )
      .bind(providerId, externalResourceId)
      .all<SubscriptionRow>();
    return results.map(rowToSubscription);
  }
}

export class D1SubscriptionStoreFactory implements SubscriptionStoreFactory {
  readonly #store: SubscriptionStore;
  #schemaReady?: Promise<void>;

  constructor(db: SignalDatabase, options: { uuid?: () => string } = {}) {
    const ready = (): Promise<void> => {
      this.#schemaReady ??= createSubscriptionSchema(db).catch(
        (error: unknown) => {
          this.#schemaReady = undefined;
          throw error;
        },
      );
      return this.#schemaReady;
    };
    this.#store = new D1SubscriptionStore(db, {
      ready,
      uuid: options.uuid ?? (() => crypto.randomUUID()),
    });
  }

  store(): SubscriptionStore {
    return this.#store;
  }
}

export class InMemorySubscriptionStoreFactory
  implements SubscriptionStoreFactory
{
  readonly #rows = new Map<string, StoredSubscription>();
  readonly #store: SubscriptionStore;

  constructor(options: { uuid?: () => string } = {}) {
    const rows = this.#rows;
    const uuid = options.uuid ?? (() => crypto.randomUUID());
    const keyOf = (sub: {
      providerId: string;
      externalResourceId: string;
      threadId: string;
    }): string =>
      `${sub.providerId}\0${sub.externalResourceId}\0${sub.threadId}`;
    this.#store = {
      async subscribe(input) {
        const canonical = canonicalSubscriptionInput(input);
        const key = keyOf(canonical);
        const existing = rows.get(key);
        const id = existing?.id ?? uuid();
        assertSubscriptionId(id);
        const subscription: StoredSubscription = {
          id,
          providerId: canonical.providerId,
          threadId: canonical.threadId,
          resourceId: canonical.resourceId,
          externalResourceId: canonical.externalResourceId,
          subscribedAt: existing?.subscribedAt ?? new Date(),
          metadata: structuredClone(
            canonical.metadata ?? existing?.metadata ?? {},
          ),
        };
        rows.set(key, subscription);
        return structuredClone(subscription);
      },
      async unsubscribe(providerId, externalResourceId, threadId) {
        assertProviderId(providerId, 'unsubscribe');
        assertExternalResourceId(externalResourceId, 'unsubscribe');
        assertThreadId(threadId, 'unsubscribe');
        return rows.delete(keyOf({ providerId, externalResourceId, threadId }));
      },
      async listForThread(threadId) {
        assertThreadId(threadId, 'listForThread');
        return [...rows.values()]
          .filter((subscription) => subscription.threadId === threadId)
          .map((subscription) => structuredClone(subscription));
      },
      async listForProvider(providerId) {
        assertProviderId(providerId, 'listForProvider');
        return [...rows.values()]
          .filter((subscription) => subscription.providerId === providerId)
          .map((subscription) => structuredClone(subscription));
      },
      async listByResource(providerId, externalResourceId) {
        assertProviderId(providerId, 'listByResource');
        assertExternalResourceId(externalResourceId, 'listByResource');
        return [...rows.values()]
          .filter(
            (subscription) =>
              subscription.providerId === providerId &&
              subscription.externalResourceId === externalResourceId,
          )
          .map((subscription) => structuredClone(subscription));
      },
    };
  }

  store(): SubscriptionStore {
    return this.#store;
  }
}
