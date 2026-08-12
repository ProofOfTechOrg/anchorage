// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { CloudflareProvisioningClient } from '../src/cloudflare-client.js';
import { D1CloudflareApiRateCoordinator } from '../src/cloudflare-rate-coordinator.js';
import type { FleetStateDatabase } from '../src/state-store.js';

const WINDOW_MS = 5 * 60_000;

class RateDatabase implements FleetStateDatabase {
  readonly reservations = new Map<
    string,
    Array<{ reservationId: string; reservedAt: number }>
  >();
  now = 1_000;
  failSchemaOnce = false;
  schemaAttempts = 0;

  async query(): Promise<readonly Readonly<Record<string, unknown>>[]> {
    return [];
  }

  async execute(sql: string): Promise<void> {
    if (sql.startsWith('CREATE TABLE')) {
      this.schemaAttempts += 1;
      if (this.failSchemaOnce) {
        this.failSchemaOnce = false;
        throw new Error('transient schema failure');
      }
    }
  }

  async batch(
    statements: readonly Readonly<{
      sql: string;
      bindings?: readonly unknown[];
    }>[],
  ): Promise<readonly (readonly Readonly<Record<string, unknown>>[])[]> {
    const scope = String(statements[0]?.bindings?.[0]);
    const windowMs = Number(statements[0]?.bindings?.[1]);
    const current = (this.reservations.get(scope) ?? []).filter(
      ({ reservedAt }) => reservedAt > this.now - windowMs,
    );
    const reservationId = String(statements[1]?.bindings?.[1]);
    const intervalCap = Number(statements[1]?.bindings?.[3]);
    const inserted = current.length < intervalCap;
    if (inserted) current.push({ reservationId, reservedAt: this.now });
    this.reservations.set(scope, current);
    const earliest = current.at(0)?.reservedAt;
    return [
      [],
      inserted
        ? [{ reservation_id: reservationId, reserved_at: this.now }]
        : [],
      [
        {
          retry_after_ms:
            earliest === undefined
              ? 1
              : Math.max(1, earliest + windowMs - this.now),
        },
      ],
    ];
  }

  seed(scope: string, count: number): void {
    this.reservations.set(
      scope,
      Array.from({ length: count }, (_, index) => ({
        reservationId: `seed-${index}`,
        reservedAt: this.now,
      })),
    );
  }
}

function envelope(result: unknown): Response {
  return Response.json({
    success: true,
    errors: [],
    messages: [],
    result,
    result_info: {
      page: 1,
      per_page: 20,
      count: Array.isArray(result) ? result.length : 1,
      total_count: Array.isArray(result) ? result.length : 1,
      total_pages: 1,
    },
  });
}

describe('D1CloudflareApiRateCoordinator', () => {
  it('fails closed before a provider request when coordination fails', async () => {
    const request = vi.fn(async () => envelope([]));
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'raw-token',
      dispatchNamespace: 'fleet',
      rateCoordinator: {
        async acquire() {
          throw new Error('coordinator unavailable');
        },
      },
      fetch: request,
    });

    await expect(client.findDatabase('one')).rejects.toBeDefined();
    expect(request).not.toHaveBeenCalled();
  });

  it('retries schema initialization after a transient failure on the same instance', async () => {
    const db = new RateDatabase();
    db.failSchemaOnce = true;
    const coordinator = new D1CloudflareApiRateCoordinator(db, {
      quotaScope: 'provider-principal-1',
    });

    await expect(coordinator.acquire()).rejects.toThrow(
      /transient schema failure/u,
    );
    await expect(coordinator.acquire()).resolves.toBeUndefined();
    expect(db.schemaAttempts).toBe(2);
  });

  it('coordinates separate client instances and replicas through one durable scope', async () => {
    vi.useFakeTimers();
    try {
      const db = new RateDatabase();
      const scope = 'provider-principal-1';
      db.seed(scope, 1_099);
      const requests: string[] = [];
      const request = vi.fn(async (input: string | URL | Request) => {
        const url = new URL(
          typeof input === 'string'
            ? input
            : input instanceof URL
              ? input.href
              : input.url,
        );
        requests.push(url.searchParams.get('name') ?? '');
        return envelope([]);
      });
      const client = (apiToken: string) =>
        new CloudflareProvisioningClient({
          accountId: 'account',
          apiToken,
          dispatchNamespace: 'fleet',
          rateCoordinator: new D1CloudflareApiRateCoordinator(db, {
            quotaScope: scope,
          }),
          fetch: request,
        });

      const first = client('raw-token-one').findDatabase('one');
      await vi.waitFor(() => expect(request).toHaveBeenCalledTimes(1));
      await expect(first).resolves.toBeUndefined();
      expect(db.reservations.get(scope)).toHaveLength(1_100);
      const second = client('raw-token-two').findDatabase('two');
      await vi.advanceTimersByTimeAsync(0);
      expect(request).toHaveBeenCalledTimes(1);

      db.now += WINDOW_MS;
      await vi.advanceTimersByTimeAsync(WINDOW_MS);
      await expect(second).resolves.toBeUndefined();
      expect(request).toHaveBeenCalledTimes(2);
      expect(requests.sort()).toEqual(['one', 'two']);
      expect(JSON.stringify([...db.reservations.entries()])).not.toContain(
        'raw-token',
      );
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps independent nonsecret quota scopes separate', async () => {
    const db = new RateDatabase();
    await Promise.all([
      new D1CloudflareApiRateCoordinator(db, {
        quotaScope: 'provider-principal-1',
      }).acquire(),
      new D1CloudflareApiRateCoordinator(db, {
        quotaScope: 'provider-principal-2',
      }).acquire(),
    ]);
    expect(db.reservations.get('provider-principal-1')).toHaveLength(1);
    expect(db.reservations.get('provider-principal-2')).toHaveLength(1);
  });

  it('validates the explicit quota scope', () => {
    const db = new RateDatabase();
    for (const quotaScope of ['', ' leading', 'trailing ', 'line\nbreak']) {
      expect(
        () => new D1CloudflareApiRateCoordinator(db, { quotaScope }),
      ).toThrow(/quotaScope/u);
    }
  });
});
