// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import type { SignalDatabase } from '../signals/d1-shared.js';
import {
  D1SubscriptionStoreFactory,
  InMemorySubscriptionStoreFactory,
  MAX_EXTERNAL_RESOURCE_ID_BYTES,
  SIGNAL_SUBSCRIPTIONS_TABLE,
  type SubscribeInput,
  type SubscriptionStoreFactory,
} from './subscription-d1.js';

// A deterministic id generator (u1, u2, …) so minted subscription ids are stable.
function counter(): () => string {
  let n = 0;
  return () => {
    n += 1;
    return `u${n}`;
  };
}

function d1Factory(): D1SubscriptionStoreFactory {
  return new D1SubscriptionStoreFactory(
    sqliteUnitDatabase(openSqlite()) as SignalDatabase,
    { uuid: counter() },
  );
}

function inMemoryFactory(): InMemorySubscriptionStoreFactory {
  return new InMemorySubscriptionStoreFactory({ uuid: counter() });
}

// The shared contract suite — run against BOTH backends so InMemory can never
// hide what D1 does (the approval-store convention).
function contractSuite(
  label: string,
  make: () => SubscriptionStoreFactory,
): void {
  describe(`SubscriptionStore contract — ${label}`, () => {
    it('mints a path-safe id and round-trips core SignalSubscription shape', async () => {
      // #given
      const factory = make();
      const store = factory.store();
      // #when
      const sub = await store.subscribe({
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        threadId: 'acme_t1',
        resourceId: 'acme_user1',
        metadata: { note: 'x' },
      });
      // #then — the id is host-minted; the shape is core's SignalSubscription.
      expect(sub.id).toBe('u1');
      expect(sub).toMatchObject({
        providerId: 'github',
        threadId: 'acme_t1',
        resourceId: 'acme_user1',
        externalResourceId: 'github:acme/repo',
        metadata: { note: 'x' },
      });
      expect(sub.subscribedAt).toBeInstanceOf(Date);
    });

    it('is idempotent on (provider, externalResource, thread) — a re-subscribe keeps the id', async () => {
      // #given
      const store = make().store();
      const first = await store.subscribe({
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        threadId: 'acme_t1',
        resourceId: 'acme_user1',
      });
      // #when — same key, updated resource
      const second = await store.subscribe({
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        threadId: 'acme_t1',
        resourceId: 'acme_user2',
      });
      // #then — one row, same id, updated resource
      expect(second.id).toBe(first.id);
      expect(second.resourceId).toBe('acme_user2');
      const list = await store.listForThread('acme_t1');
      expect(list).toHaveLength(1);
    });

    it('preserves metadata on a re-subscribe that OMITS it; an explicit {} clears it', async () => {
      // #given — a subscription with metadata
      const store = make().store();
      await store.subscribe({
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        threadId: 'acme_t1',
        resourceId: 'acme_u1',
        metadata: { label: 'keep' },
      });
      // #when — a re-subscribe (idempotent retry) that OMITS metadata
      const preserved = await store.subscribe({
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        threadId: 'acme_t1',
        resourceId: 'acme_u2',
      });
      // #then — the existing metadata is PRESERVED (not wiped to {})
      expect(preserved.metadata).toEqual({ label: 'keep' });
      // #when — a re-subscribe with an EXPLICIT empty object
      const cleared = await store.subscribe({
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        threadId: 'acme_t1',
        resourceId: 'acme_u3',
        metadata: {},
      });
      // #then — an explicit {} REPLACES (clears) it
      expect(cleared.metadata).toEqual({});
    });

    it('snapshots metadata on write and read', async () => {
      const store = make().store();
      const metadata = { nested: { label: 'original' } };
      const created = await store.subscribe({
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        threadId: 'thread-1',
        resourceId: 'resource-1',
        metadata,
      });

      metadata.nested.label = 'mutated-input';
      (created.metadata as { nested: { label: string } }).nested.label =
        'mutated-output';

      await expect(store.listForThread('thread-1')).resolves.toMatchObject([
        { metadata: { nested: { label: 'original' } } },
      ]);
    });

    it('snapshots every input field before asynchronous persistence', async () => {
      const store = make().store();
      const metadata = { nested: { label: 'original' } };
      const input: SubscribeInput = {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        threadId: 'thread-1',
        resourceId: 'resource-1',
        metadata,
      };

      const pending = store.subscribe(input);
      input.providerId = 'changed-provider';
      input.externalResourceId = 'changed-resource';
      input.threadId = 'changed-thread';
      input.resourceId = 'changed-owner';
      metadata.nested.label = 'changed-metadata';

      await expect(pending).resolves.toMatchObject({
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        threadId: 'thread-1',
        resourceId: 'resource-1',
        metadata: { nested: { label: 'original' } },
      });
    });

    it('refuses accessor-backed input fields without invoking them', async () => {
      const store = make().store();
      let reads = 0;
      const input = {
        externalResourceId: 'github:acme/repo',
        threadId: 'thread-1',
        resourceId: 'resource-1',
      } as Record<string, unknown>;
      Object.defineProperty(input, 'providerId', {
        enumerable: true,
        get: () => {
          reads += 1;
          return 'github';
        },
      });

      await expect(
        store.subscribe(input as unknown as SubscribeInput),
      ).rejects.toThrow('providerId must be an own data property');
      expect(reads).toBe(0);
    });

    it('uses JSON metadata semantics and rejects non-serializable metadata', async () => {
      const store = make().store();
      const timestamp = new Date('2026-08-10T00:00:00.000Z');
      const subscription = await store.subscribe({
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        threadId: 'thread-1',
        resourceId: 'resource-1',
        metadata: {
          timestamp,
          omitted: undefined,
          nonFinite: Number.NaN,
        },
      });
      expect(subscription.metadata).toEqual({
        timestamp: timestamp.toISOString(),
        nonFinite: null,
      });

      await expect(
        store.subscribe({
          providerId: 'github',
          externalResourceId: 'github:acme/repo',
          threadId: 'thread-2',
          resourceId: 'resource-2',
          metadata: { unsupported: 1n },
        }),
      ).rejects.toThrow('metadata must be JSON-serializable');
      await expect(
        store.subscribe({
          providerId: 'github',
          externalResourceId: 'github:acme/repo',
          threadId: 'thread-3',
          resourceId: 'resource-3',
          metadata: { toJSON: () => 'not-an-object' },
        }),
      ).rejects.toThrow('metadata must serialize to an object');
    });

    it('bounds external resource ids by UTF-8 bytes and rejects controls', async () => {
      const store = make().store();
      const valid = {
        providerId: 'github',
        threadId: 'thread-1',
        resourceId: 'resource-1',
      };

      await expect(
        store.subscribe({
          ...valid,
          externalResourceId: 'x'.repeat(MAX_EXTERNAL_RESOURCE_ID_BYTES),
        }),
      ).resolves.toMatchObject({
        externalResourceId: 'x'.repeat(MAX_EXTERNAL_RESOURCE_ID_BYTES),
      });
      await expect(
        store.subscribe({
          ...valid,
          externalResourceId: 'x'.repeat(MAX_EXTERNAL_RESOURCE_ID_BYTES + 1),
        }),
      ).rejects.toThrow(
        `externalResourceId exceeds ${MAX_EXTERNAL_RESOURCE_ID_BYTES} UTF-8 bytes`,
      );
      await expect(
        store.listByResource('github', 'github:acme\nrepo'),
      ).rejects.toThrow('must not contain ASCII control characters');
    });

    it('rejects malformed untyped inputs consistently', async () => {
      const store = make().store();
      const valid = {
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        threadId: 'thread-1',
        resourceId: 'resource-1',
      };

      await expect(
        store.subscribe({ ...valid, providerId: 123 as unknown as string }),
      ).rejects.toThrow('providerId is invalid');
      await expect(
        store.subscribe({
          ...valid,
          externalResourceId: 123 as unknown as string,
        }),
      ).rejects.toThrow('externalResourceId is required');
      await expect(
        store.subscribe({ ...valid, threadId: 123 as unknown as string }),
      ).rejects.toThrow('threadId is not path-safe');
      await expect(
        store.subscribe({ ...valid, resourceId: 123 as unknown as string }),
      ).rejects.toThrow('resourceId is not path-safe');
      await expect(
        store.subscribe({
          ...valid,
          metadata: [] as unknown as Record<string, unknown>,
        }),
      ).rejects.toThrow('metadata must be an object');
      await expect(
        store.unsubscribe('github', 'resource', 123 as unknown as string),
      ).rejects.toThrow('threadId is not path-safe');
      await expect(
        store.listForProvider(123 as unknown as string),
      ).rejects.toThrow('providerId is invalid');
      await expect(
        store.listByResource('github', 123 as unknown as string),
      ).rejects.toThrow('externalResourceId is required');
    });

    it('store aliases share subscriptions from one deployment database', async () => {
      const factory = make();
      await factory.store().subscribe({
        providerId: 'github',
        externalResourceId: 'github:shared/repo',
        threadId: 'thread-1',
        resourceId: 'resource-1',
      });
      await factory.store().subscribe({
        providerId: 'github',
        externalResourceId: 'github:shared/repo',
        threadId: 'thread-2',
        resourceId: 'resource-2',
      });
      const rows = await factory.store().listForProvider('github');
      expect(rows.map((subscription) => subscription.threadId).sort()).toEqual([
        'thread-1',
        'thread-2',
      ]);
    });

    it('listByResource returns every matching deployment subscription', async () => {
      const factory = make();
      await factory.store().subscribe({
        providerId: 'github',
        externalResourceId: 'github:shared/repo',
        threadId: 'thread-1',
        resourceId: 'resource-1',
      });
      await factory.store().subscribe({
        providerId: 'github',
        externalResourceId: 'github:shared/repo',
        threadId: 'thread-2',
        resourceId: 'resource-2',
      });
      // #when
      const rows = await factory
        .store()
        .listByResource('github', 'github:shared/repo');
      // #then — both persisted rows are routing authority
      expect(rows).toHaveLength(2);
      // and a non-matching resource yields nothing
      expect(
        await factory.store().listByResource('github', 'github:other/repo'),
      ).toEqual([]);
    });

    it('unsubscribe removes exactly the one row and reports it', async () => {
      // #given
      const store = make().store();
      await store.subscribe({
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        threadId: 'acme_t1',
        resourceId: 'acme_u1',
      });
      // #when
      const removed = await store.unsubscribe(
        'github',
        'github:acme/repo',
        'acme_t1',
      );
      const missing = await store.unsubscribe(
        'github',
        'github:acme/repo',
        'acme_t1',
      );
      // #then
      expect(removed).toBe(true);
      expect(missing).toBe(false);
      expect(await store.listForThread('acme_t1')).toEqual([]);
    });
  });
}

contractSuite('D1', () => d1Factory());
contractSuite('in-memory', () => inMemoryFactory());

describe('subscription backend parity', () => {
  it('normalizes the same metadata into the same stored value', async () => {
    const metadata = {
      timestamp: new Date('2026-08-10T00:00:00.000Z'),
      omitted: undefined,
      nonFinite: Number.POSITIVE_INFINITY,
      nested: [{ value: 'kept' }],
    };
    const create = (factory: SubscriptionStoreFactory) =>
      factory.store().subscribe({
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        threadId: 'thread-1',
        resourceId: 'resource-1',
        metadata,
      });

    const [d1, inMemory] = await Promise.all([
      create(d1Factory()),
      create(inMemoryFactory()),
    ]);
    expect(inMemory.metadata).toEqual(d1.metadata);
  });
});

describe('deployment-wide subscription schema', () => {
  it('pins the table name', () => {
    expect(SIGNAL_SUBSCRIPTIONS_TABLE).toBe('flowsafe_signal_subscriptions');
  });

  it('refuses the retired pooled schema', async () => {
    const sqlite = openSqlite();
    sqlite
      .prepare(
        `CREATE TABLE ${SIGNAL_SUBSCRIPTIONS_TABLE} (
          id TEXT PRIMARY KEY,
          tenant_id TEXT NOT NULL
        )`,
      )
      .run();
    const store = new D1SubscriptionStoreFactory(
      sqliteUnitDatabase(sqlite) as SignalDatabase,
    ).store();

    await expect(store.listForProvider('github')).rejects.toThrow(
      /retired pooled-tenant schema.*recreate the database/,
    );
  });

  it('uses stable index names for a fresh deployment schema', async () => {
    const sqlite = openSqlite();
    const store = new D1SubscriptionStoreFactory(
      sqliteUnitDatabase(sqlite) as SignalDatabase,
    ).store();
    await store.listForProvider('github');

    const indexes = sqlite
      .prepare(`PRAGMA index_list(${SIGNAL_SUBSCRIPTIONS_TABLE})`)
      .all() as Array<{ name: string }>;
    expect(indexes.map(({ name }) => name).sort()).toEqual([
      `${SIGNAL_SUBSCRIPTIONS_TABLE}_key`,
      `${SIGNAL_SUBSCRIPTIONS_TABLE}_provider`,
      `${SIGNAL_SUBSCRIPTIONS_TABLE}_resource`,
      `sqlite_autoindex_${SIGNAL_SUBSCRIPTIONS_TABLE}_1`,
    ]);
  });
});
