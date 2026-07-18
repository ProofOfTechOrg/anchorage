// SPDX-License-Identifier: Apache-2.0
// Track E (M-007), CI-M-007-002 — the flowsafe-owned D1 subscription store.
//
// This is OUR table (`flowsafe_signal_subscriptions`), NOT a `mastra_*` domain:
// core's SignalProvider keeps subscriptions in an in-memory Map, which is lost on
// DO eviction and tenant-blind, so flowsafe persists them itself. It mirrors the
// approval store's INV-2 posture (tenant-store.ts / tenant-brand.ts /
// d1-store.ts): obtain a store ONLY through the factory, which hands request
// scope a store BOUND to one tenant at construction (every SELECT/DELETE carries
// `tenant_id = <ctor field>`) and hands the webhook route the cron/webhook-only
// `system()` view — a DISTINCT type that cannot flow into request-scoped code.
//
// The webhook's `system().listByResource` is the ONE legitimate cross-tenant read
// (a webhook arrives with no identity — the ROW is the tenant authority, exactly
// like sweepSLA over SystemApprovalStore). Offboarding is do-runner's `purgeTenant`
// (a `WHERE tenant_id = ?` delete, the flowsafe_approvals precedent); retention is
// `none` — a subscription is standing config a tenant means to keep, reaped only
// at offboarding (like `mastra_schedules`' `none`-with-because), so this table is
// deliberately absent from the run/thread TTL purges.

import type { SignalSubscription } from '@mastra/core/signals';

import { TENANT_ID_PATTERN } from '../do-runner/index.js';
import {
  d1Changes,
  dateOrUndefined,
  isoOrNull,
  jsonOrNull,
  parseJsonOrUndefined,
  type SignalDatabase,
} from '../signals/d1-shared.js';

/** The table this store owns. Kept in sync with do-runner's purgeTenant DELETE. */
export const SIGNAL_SUBSCRIPTIONS_TABLE = 'flowsafe_signal_subscriptions';

/**
 * A subscription row mapped to core's `SignalSubscription` shape PLUS its owning
 * `tenantId` — the webhook path needs the tenant per row (its rows span tenants);
 * the poll path ignores it (a host DO reads only its own tenant's rows). A
 * superset of `SignalSubscription`, so it flows into `buildNotification` and
 * `pollForDeliveries` unchanged.
 */
export interface StoredSubscription extends SignalSubscription {
  readonly tenantId: string;
}

/** What a subscribe writes — the memory ids are server-minted (never a client body). */
export interface SubscribeInput {
  providerId: string;
  externalResourceId: string;
  /** Tenant-salted threadId (`${tenantId}_${uuid}`) the caller owns (path-checked upstream). */
  threadId: string;
  /** Tenant-salted resourceId (`${tenantId}_${key}`) — the thread's owner. */
  resourceId: string;
  metadata?: Record<string, unknown>;
}

/**
 * The tenant-bound subscription store INV-2 keeps request scope on. Obtain via
 * `SubscriptionStoreFactory.forTenant`; the brand makes an unbound/system store a
 * compile error where one of these is required.
 */
export interface TenantBoundSubscriptionStore {
  readonly tenantId: string;
  readonly [SUBSCRIPTION_TENANT_BOUND]: true;
  /** Upsert a subscription (idempotent on provider × externalResourceId × thread). */
  subscribe(input: SubscribeInput): Promise<StoredSubscription>;
  /** Remove one subscription; true if a row was deleted. */
  unsubscribe(
    providerId: string,
    externalResourceId: string,
    threadId: string,
  ): Promise<boolean>;
  /** This tenant's subscriptions for one thread (the subscribe route's read). */
  listForThread(threadId: string): Promise<StoredSubscription[]>;
  /** This tenant's subscriptions for one provider (the host-DO rehydration query). */
  listForProvider(providerId: string): Promise<StoredSubscription[]>;
}

/**
 * The cron/webhook-only cross-tenant view. Deliberately NOT a
 * TenantBoundSubscriptionStore (`[SUBSCRIPTION_TENANT_BOUND]?: never`) so it can
 * never reach request-scoped code — the SystemApprovalStore posture.
 */
export interface SystemSubscriptionStore {
  readonly [SUBSCRIPTION_TENANT_BOUND]?: never;
  /**
   * Every subscription to `(providerId, externalResourceId)` ACROSS tenants —
   * each carries its own `tenantId`. The webhook's tenant-resolution authority:
   * a webhook payload NEVER names a tenant; the matched row does.
   */
  listByResource(
    providerId: string,
    externalResourceId: string,
  ): Promise<StoredSubscription[]>;
}

/** The brand (INV-2). A unique symbol, satisfied only by a store this module builds. */
export const SUBSCRIPTION_TENANT_BOUND: unique symbol = Symbol(
  'flowsafe.signalSubscriptionTenantBound',
);

export interface SubscriptionStoreFactory {
  /** Bind a store to one tenant. Throws unless tenantId satisfies INV-3. */
  forTenant(tenantId: string): TenantBoundSubscriptionStore;
  /** The cron/webhook-only cross-tenant view. Never request-scoped. */
  system(): SystemSubscriptionStore;
}

function assertTenantId(tenantId: string): void {
  if (typeof tenantId !== 'string' || !TENANT_ID_PATTERN.test(tenantId)) {
    throw new Error(
      `forTenant: tenantId '${tenantId}' violates INV-3 (^[a-z0-9]{3,32}$)`,
    );
  }
}

// --- D1 -------------------------------------------------------------------

interface SubscriptionRow {
  id: string;
  tenant_id: string;
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
    tenant_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    external_resource_id TEXT NOT NULL,
    subscribed_at TEXT NOT NULL,
    metadata TEXT
  )`,
  // One subscription per (tenant, provider, external resource, thread): a
  // re-subscribe upserts rather than duplicating (the ON CONFLICT target below).
  `CREATE UNIQUE INDEX IF NOT EXISTS ${SIGNAL_SUBSCRIPTIONS_TABLE}_key
    ON ${SIGNAL_SUBSCRIPTIONS_TABLE} (tenant_id, provider_id, external_resource_id, thread_id)`,
  // The webhook's cross-tenant lookup rides this: given a provider + external
  // resource, find every subscribed (tenant, thread).
  `CREATE INDEX IF NOT EXISTS ${SIGNAL_SUBSCRIPTIONS_TABLE}_resource
    ON ${SIGNAL_SUBSCRIPTIONS_TABLE} (provider_id, external_resource_id)`,
  // The host-DO rehydration rides this: one tenant's subscriptions for one provider.
  `CREATE INDEX IF NOT EXISTS ${SIGNAL_SUBSCRIPTIONS_TABLE}_rehydrate
    ON ${SIGNAL_SUBSCRIPTIONS_TABLE} (tenant_id, provider_id)`,
];

async function createSubscriptionSchema(db: SignalDatabase): Promise<void> {
  for (const statement of SCHEMA_STATEMENTS) {
    await db.prepare(statement).run();
  }
}

function rowToSubscription(row: SubscriptionRow): StoredSubscription {
  return {
    id: row.id,
    providerId: row.provider_id,
    threadId: row.thread_id,
    resourceId: row.resource_id,
    externalResourceId: row.external_resource_id,
    subscribedAt: dateOrUndefined(row.subscribed_at) ?? new Date(0),
    metadata: parseJsonOrUndefined<Record<string, unknown>>(row.metadata) ?? {},
    tenantId: row.tenant_id,
  };
}

class D1TenantBoundSubscriptionStore implements TenantBoundSubscriptionStore {
  readonly [SUBSCRIPTION_TENANT_BOUND] = true as const;
  readonly tenantId: string;
  readonly #db: SignalDatabase;
  readonly #ready: () => Promise<void>;
  readonly #uuid: () => string;

  constructor(
    db: SignalDatabase,
    options: {
      tenantId: string;
      ready: () => Promise<void>;
      uuid: () => string;
    },
  ) {
    this.#db = db;
    this.tenantId = options.tenantId;
    this.#ready = options.ready;
    this.#uuid = options.uuid;
  }

  async subscribe(input: SubscribeInput): Promise<StoredSubscription> {
    await this.#ready();
    // A server-minted salted id — a subscription id is tenant-owned like a runId,
    // so a purge by the tenant range would ALSO reap it (belt to the tenant_id
    // delete). Kept on re-subscribe (ON CONFLICT DO UPDATE never touches `id`).
    const id = `${this.tenantId}_${this.#uuid()}`;
    const subscribedAt = new Date();
    const row = await this.#db
      .prepare(
        `INSERT INTO ${SIGNAL_SUBSCRIPTIONS_TABLE}
           (id, tenant_id, provider_id, thread_id, resource_id, external_resource_id, subscribed_at, metadata)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (tenant_id, provider_id, external_resource_id, thread_id)
         DO UPDATE SET resource_id = excluded.resource_id,
                       metadata = COALESCE(excluded.metadata, metadata)
         RETURNING *`,
      )
      .bind(
        id,
        this.tenantId,
        input.providerId,
        input.threadId,
        input.resourceId,
        input.externalResourceId,
        isoOrNull(subscribedAt),
        // OMITTED metadata binds NULL, so the COALESCE above PRESERVES the
        // existing value on a re-subscribe (an idempotent retry never wipes it);
        // an explicit `{}` binds '{}' and REPLACES (clears) it.
        jsonOrNull(input.metadata),
      )
      .first<SubscriptionRow>();
    if (row === null) {
      throw new Error('subscribe: upsert returned no row');
    }
    return rowToSubscription(row);
  }

  async unsubscribe(
    providerId: string,
    externalResourceId: string,
    threadId: string,
  ): Promise<boolean> {
    await this.#ready();
    const result = await this.#db
      .prepare(
        `DELETE FROM ${SIGNAL_SUBSCRIPTIONS_TABLE}
         WHERE tenant_id = ? AND provider_id = ? AND external_resource_id = ? AND thread_id = ?`,
      )
      .bind(this.tenantId, providerId, externalResourceId, threadId)
      .run();
    return d1Changes(result) > 0;
  }

  async listForThread(threadId: string): Promise<StoredSubscription[]> {
    await this.#ready();
    const { results } = await this.#db
      .prepare(
        `SELECT * FROM ${SIGNAL_SUBSCRIPTIONS_TABLE}
         WHERE tenant_id = ? AND thread_id = ? ORDER BY subscribed_at`,
      )
      .bind(this.tenantId, threadId)
      .all<SubscriptionRow>();
    return results.map(rowToSubscription);
  }

  async listForProvider(providerId: string): Promise<StoredSubscription[]> {
    await this.#ready();
    const { results } = await this.#db
      .prepare(
        `SELECT * FROM ${SIGNAL_SUBSCRIPTIONS_TABLE}
         WHERE tenant_id = ? AND provider_id = ? ORDER BY subscribed_at`,
      )
      .bind(this.tenantId, providerId)
      .all<SubscriptionRow>();
    return results.map(rowToSubscription);
  }
}

export class D1SubscriptionStoreFactory implements SubscriptionStoreFactory {
  readonly #db: SignalDatabase;
  readonly #uuid: () => string;
  #schemaReady?: Promise<void>;

  constructor(db: SignalDatabase, options: { uuid?: () => string } = {}) {
    this.#db = db;
    this.#uuid = options.uuid ?? (() => crypto.randomUUID());
  }

  // One memoized DDL pass per factory; a failed attempt clears the memo so the
  // next call retries rather than pinning everything to a dead promise.
  #ready = (): Promise<void> => {
    this.#schemaReady ??= createSubscriptionSchema(this.#db).catch(
      (error: unknown) => {
        this.#schemaReady = undefined;
        throw error;
      },
    );
    return this.#schemaReady;
  };

  forTenant(tenantId: string): TenantBoundSubscriptionStore {
    assertTenantId(tenantId);
    return new D1TenantBoundSubscriptionStore(this.#db, {
      tenantId,
      ready: this.#ready,
      uuid: this.#uuid,
    });
  }

  system(): SystemSubscriptionStore {
    const db = this.#db;
    const ready = this.#ready;
    return {
      async listByResource(
        providerId: string,
        externalResourceId: string,
      ): Promise<StoredSubscription[]> {
        await ready();
        const { results } = await db
          .prepare(
            `SELECT * FROM ${SIGNAL_SUBSCRIPTIONS_TABLE}
             WHERE provider_id = ? AND external_resource_id = ?`,
          )
          .bind(providerId, externalResourceId)
          .all<SubscriptionRow>();
        return results.map(rowToSubscription);
      },
    };
  }
}

// --- In-memory (dev/test parity) -----------------------------------------

/**
 * In-memory factory over ONE shared Map — the mirror of `D1SubscriptionStoreFactory`
 * for hosts/tests with no D1. Not decoration: two `forTenant` views share the
 * backend, so a cross-tenant test is non-vacuous (the InMemoryApprovalStoreFactory
 * rationale). `purgeTenant` mirrors the D1 offboarding delete for in-memory hosts.
 */
export class InMemorySubscriptionStoreFactory
  implements SubscriptionStoreFactory
{
  readonly #rows = new Map<string, StoredSubscription>();
  readonly #uuid: () => string;

  constructor(options: { uuid?: () => string } = {}) {
    this.#uuid = options.uuid ?? (() => crypto.randomUUID());
  }

  #keyOf(sub: {
    tenantId: string;
    providerId: string;
    externalResourceId: string;
    threadId: string;
  }): string {
    return `${sub.tenantId} ${sub.providerId} ${sub.externalResourceId} ${sub.threadId}`;
  }

  forTenant(tenantId: string): TenantBoundSubscriptionStore {
    assertTenantId(tenantId);
    const rows = this.#rows;
    const keyOf = this.#keyOf.bind(this);
    const uuid = this.#uuid;
    return {
      [SUBSCRIPTION_TENANT_BOUND]: true,
      tenantId,
      async subscribe(input: SubscribeInput): Promise<StoredSubscription> {
        const key = keyOf({ tenantId, ...input });
        const existing = rows.get(key);
        const sub: StoredSubscription = {
          id: existing?.id ?? `${tenantId}_${uuid()}`,
          providerId: input.providerId,
          threadId: input.threadId,
          resourceId: input.resourceId,
          externalResourceId: input.externalResourceId,
          subscribedAt: existing?.subscribedAt ?? new Date(),
          // Omitted metadata PRESERVES the existing value (mirrors the D1
          // COALESCE); an explicit `{}` replaces it.
          metadata: input.metadata ?? existing?.metadata ?? {},
          tenantId,
        };
        rows.set(key, sub);
        return structuredClone(sub);
      },
      async unsubscribe(providerId, externalResourceId, threadId) {
        return rows.delete(
          keyOf({ tenantId, providerId, externalResourceId, threadId }),
        );
      },
      async listForThread(threadId) {
        return [...rows.values()]
          .filter((s) => s.tenantId === tenantId && s.threadId === threadId)
          .map((s) => structuredClone(s));
      },
      async listForProvider(providerId) {
        return [...rows.values()]
          .filter((s) => s.tenantId === tenantId && s.providerId === providerId)
          .map((s) => structuredClone(s));
      },
    };
  }

  system(): SystemSubscriptionStore {
    const rows = this.#rows;
    return {
      async listByResource(providerId, externalResourceId) {
        return [...rows.values()]
          .filter(
            (s) =>
              s.providerId === providerId &&
              s.externalResourceId === externalResourceId,
          )
          .map((s) => structuredClone(s));
      },
    };
  }

  /** Delete every subscription stamped with this tenant; returns the count. */
  purgeTenant(tenantId: string): number {
    assertTenantId(tenantId);
    let purged = 0;
    for (const [key, sub] of this.#rows) {
      if (sub.tenantId === tenantId) {
        this.#rows.delete(key);
        purged += 1;
      }
    }
    return purged;
  }
}
