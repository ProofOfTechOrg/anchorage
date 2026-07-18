// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import { d1DatabaseLike, openSqlite } from '../../test-support/sqlite.js';
import type { SignalDatabase } from '../signals/d1-shared.js';
import {
  D1SubscriptionStoreFactory,
  InMemorySubscriptionStoreFactory,
  SIGNAL_SUBSCRIPTIONS_TABLE,
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
    d1DatabaseLike(openSqlite()) as SignalDatabase,
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
    it('mints a tenant-salted id and round-trips core SignalSubscription shape', async () => {
      // #given
      const factory = make();
      const store = factory.forTenant('acme');
      // #when
      const sub = await store.subscribe({
        providerId: 'github',
        externalResourceId: 'github:acme/repo',
        threadId: 'acme_t1',
        resourceId: 'acme_user1',
        metadata: { note: 'x' },
      });
      // #then — the id is tenant-salted; the shape is core's SignalSubscription.
      expect(sub.id.startsWith('acme_')).toBe(true);
      expect(sub).toMatchObject({
        providerId: 'github',
        threadId: 'acme_t1',
        resourceId: 'acme_user1',
        externalResourceId: 'github:acme/repo',
        tenantId: 'acme',
        metadata: { note: 'x' },
      });
      expect(sub.subscribedAt).toBeInstanceOf(Date);
    });

    it('is idempotent on (provider, externalResource, thread) — a re-subscribe keeps the id', async () => {
      // #given
      const store = make().forTenant('acme');
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
      const store = make().forTenant('acme');
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

    it('forTenant isolates: listForProvider/listForThread never cross tenants', async () => {
      // #given — two tenants, SAME business shape
      const factory = make();
      await factory.forTenant('acme').subscribe({
        providerId: 'github',
        externalResourceId: 'github:shared/repo',
        threadId: 'acme_t1',
        resourceId: 'acme_u1',
      });
      await factory.forTenant('globex').subscribe({
        providerId: 'github',
        externalResourceId: 'github:shared/repo',
        threadId: 'globex_t1',
        resourceId: 'globex_u1',
      });
      // #when / #then — each tenant sees only its own
      const acme = await factory.forTenant('acme').listForProvider('github');
      expect(acme.map((s) => s.threadId)).toEqual(['acme_t1']);
      const globex = await factory
        .forTenant('globex')
        .listForProvider('github');
      expect(globex.map((s) => s.threadId)).toEqual(['globex_t1']);
    });

    it('system().listByResource returns rows ACROSS tenants (the webhook authority)', async () => {
      // #given — two tenants subscribed to the SAME external resource
      const factory = make();
      await factory.forTenant('acme').subscribe({
        providerId: 'github',
        externalResourceId: 'github:shared/repo',
        threadId: 'acme_t1',
        resourceId: 'acme_u1',
      });
      await factory.forTenant('globex').subscribe({
        providerId: 'github',
        externalResourceId: 'github:shared/repo',
        threadId: 'globex_t1',
        resourceId: 'globex_u1',
      });
      // #when
      const rows = await factory
        .system()
        .listByResource('github', 'github:shared/repo');
      // #then — both, each carrying its OWN tenantId (the row is the authority)
      expect(rows).toHaveLength(2);
      expect(new Set(rows.map((r) => r.tenantId))).toEqual(
        new Set(['acme', 'globex']),
      );
      // and a non-matching resource yields nothing
      expect(
        await factory.system().listByResource('github', 'github:other/repo'),
      ).toEqual([]);
    });

    it('unsubscribe removes exactly the one row and reports it', async () => {
      // #given
      const store = make().forTenant('acme');
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

    it('forTenant refuses a non-INV-3 tenantId', () => {
      // #then
      expect(() => make().forTenant('Bad_Tenant')).toThrow(/INV-3/);
    });
  });
}

contractSuite('D1', () => d1Factory());
contractSuite('in-memory', () => inMemoryFactory());

describe('table-name coupling', () => {
  it('pins the literal do-runner purgeTenant hardcodes (a rename here would silently miss offboarding)', () => {
    // do-runner/d1-storage.ts purgeTenant hardcodes 'flowsafe_signal_subscriptions'
    // (a real import would cycle do-runner -> signal-providers). This tripwire
    // couples the store's const to that literal: renaming the const without
    // updating the DELETE would fail HERE, not silently strand rows at offboarding.
    expect(SIGNAL_SUBSCRIPTIONS_TABLE).toBe('flowsafe_signal_subscriptions');
  });
});

describe('InMemorySubscriptionStoreFactory.purgeTenant', () => {
  it('reaps one tenant and leaves the other intact (two-tenant survival)', async () => {
    // #given
    const factory = inMemoryFactory();
    await factory.forTenant('acme').subscribe({
      providerId: 'github',
      externalResourceId: 'github:acme/repo',
      threadId: 'acme_t1',
      resourceId: 'acme_u1',
    });
    await factory.forTenant('globex').subscribe({
      providerId: 'github',
      externalResourceId: 'github:globex/repo',
      threadId: 'globex_t1',
      resourceId: 'globex_u1',
    });
    // #when
    const purged = factory.purgeTenant('acme');
    // #then
    expect(purged).toBe(1);
    expect(await factory.forTenant('acme').listForProvider('github')).toEqual(
      [],
    );
    expect(
      await factory.forTenant('globex').listForProvider('github'),
    ).toHaveLength(1);
  });
});
