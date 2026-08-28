// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createTestHarness,
  type TestHarness,
  type WorkerHandle,
} from 'wrangler';

const ROOT = new URL('..', import.meta.url).pathname;
const PROBE = new URL(
  './fixtures/fleet-state-harness-probe.ts',
  import.meta.url,
).pathname;

interface ProbeError {
  readonly name: string;
  readonly message: string;
  readonly errors?: readonly ProbeError[];
}

function harnessOptions() {
  return {
    root: ROOT,
    workers: [
      {
        config: {
          name: 'fleet-state-harness-probe',
          main: PROBE,
          compatibility_date: '2026-08-06',
          compatibility_flags: ['nodejs_compat'],
          d1_databases: [
            {
              binding: 'DB',
              database_name: 'fleet-state-harness',
              database_id: '00000000-0000-0000-0000-000000000000',
            },
          ],
        },
      },
    ],
  } satisfies Parameters<typeof createTestHarness>[0];
}

describe.sequential('D1FleetStateStore Wrangler harness', {
  timeout: 30_000,
}, () => {
  let server: TestHarness;
  let worker: WorkerHandle;

  beforeAll(async () => {
    server = createTestHarness(harnessOptions());
    await server.listen();
    worker = server.getWorker();
  }, 30_000);

  afterAll(async () => {
    await server.close();
  }, 30_000);

  async function probe<T>(action: string): Promise<T> {
    const response = await worker.fetch('/fleet-state', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ action }),
    });
    const body = (await response.json()) as T | { error: ProbeError };
    if (!response.ok) {
      throw new Error(`probe failed: ${JSON.stringify(body)}`);
    }
    return body as T;
  }

  it('admits exactly one concurrent lease owner', async () => {
    await expect(
      probe<{
        entered: number;
        rejected: number;
        acquired: number;
      }>('concurrent-acquisition'),
    ).resolves.toEqual({ entered: 1, rejected: 15, acquired: 1 });
  });

  it('renews from D1 time and keeps a heartbeat lease alive past its original expiry', async () => {
    const result = await probe<{
      explicit: { before: number; after: number };
      heartbeatObserved: boolean;
      contenderRejected: boolean;
    }>('renewal');

    expect(result.explicit.before).toBeGreaterThan(14 * 60_000);
    expect(result.explicit.after).toBeGreaterThan(14 * 60_000);
    expect(result.heartbeatObserved).toBe(true);
    expect(result.contenderRejected).toBe(true);
  }, 30_000);

  it('allows DB-expired takeover and fences every stale state mutation', async () => {
    const result = await probe<{
      stalePut: ProbeError;
      staleDelete: ProbeError;
      staleMutation: ProbeError;
      externalMutations: number;
      staleOutcome: { ok: boolean; error: ProbeError };
      staleClaimNames: string[];
      final: { scriptName: string };
    }>('takeover-and-fence');

    expect(result.stalePut.message).toMatch(/lease is no longer owned/);
    expect(result.staleDelete.message).toMatch(/lease is no longer owned/);
    expect(result.staleMutation.message).toMatch(/lease is no longer owned/);
    expect(result.externalMutations).toBe(0);
    expect(result.staleOutcome).toMatchObject({ ok: false });
    expect(result.staleClaimNames).toEqual([]);
    expect(result.final.scriptName).toBe('script-winner');
  });

  it('preserves D1 uniqueness for scripts, database IDs and names, and route hostnames', async () => {
    const result = await probe<{
      rejected: string[];
      records: unknown[];
    }>('uniqueness');

    expect(result.rejected).toHaveLength(4);
    expect(result.rejected).toEqual([
      expect.stringMatching(
        /Worker names overlap another durable reservation/i,
      ),
      expect.stringMatching(/UNIQUE constraint failed.*database_id/i),
      expect.stringMatching(/UNIQUE constraint failed.*database_name/i),
      expect.stringMatching(/UNIQUE constraint failed.*route_hostname/i),
    ]);
    expect(result.records).toHaveLength(4);
  });

  it('reports the operation error before the real D1 release error', async () => {
    const result = await probe<ProbeError>('combined-errors');

    expect(result).toMatchObject({
      name: 'AggregateError',
      errors: [
        { message: 'forced operation failure' },
        { message: expect.stringMatching(/forced lease release failure/) },
      ],
    });
  });

  it('atomically reserves the platform resource set and admits one concurrent writer', async () => {
    const result = await probe<{
      entered: number;
      acquired: number;
      collisions: ProbeError[];
      claimCount: number;
    }>('platform-claims-and-concurrency');

    expect(result.entered).toBe(1);
    expect(result.acquired).toBe(1);
    expect(result.claimCount).toBe(7);
    expect(result.collisions).toHaveLength(3);
    for (const collision of result.collisions) {
      expect(collision.message).toMatch(/overlaps another durable reservation/);
    }
  });

  it('allows platform lease takeover after DB expiry and fences the stale owner', async () => {
    const result = await probe<{
      staleMutation: ProbeError;
      externalMutations: number;
      staleOutcome: { ok: boolean; error: ProbeError };
    }>('platform-takeover-and-fence');

    expect(result.staleMutation.message).toMatch(/lease is no longer owned/);
    expect(result.externalMutations).toBe(0);
    expect(result.staleOutcome.ok).toBe(false);
  });

  it('keeps the platform lease alive with a DB-time heartbeat', async () => {
    await expect(
      probe<{ heartbeatObserved: boolean; contenderRejected: boolean }>(
        'platform-renewal',
      ),
    ).resolves.toEqual({ heartbeatObserved: true, contenderRejected: true });
  }, 30_000);

  it('mutually excludes ordinary Worker claims in both durable claim directions', async () => {
    const result = await probe<{
      deploymentCollision: ProbeError;
      bridgeCollision: ProbeError;
      platformCollision: ProbeError;
      scriptNames: string[];
    }>('cross-plane-claim-exclusion');

    expect(result.deploymentCollision.message).toMatch(
      /Worker names overlap another durable reservation/,
    );
    expect(result.platformCollision.message).toMatch(
      /overlaps another durable reservation/,
    );
    expect(result.bridgeCollision.message).toMatch(
      /Worker names overlap another durable reservation/,
    );
    expect(result.scriptNames).toEqual(['plain-deployment-first']);
  });

  it('reconciles a committed lost batch response with high-cardinality claims', async () => {
    const result = await probe<{
      record: { tenantTag: string; phase: string };
      claimCount: number;
      switchedRole: string;
    }>('atomic-claim-batch');

    expect(result.record).toMatchObject({
      tenantTag: 'atomic',
      phase: 'ready',
    });
    expect(result.claimCount).toBe(33);
    expect(result.switchedRole).toBe('deployment-state');
  });

  it('rolls back every claim mutation when the lease expires inside the batch', async () => {
    const result = await probe<{
      failure: ProbeError;
      rows: number;
      claims: number;
    }>('final-lease-assertion-rollback');

    expect(result.failure.message).toMatch(/constraint|lease/i);
    expect(result.rows).toBe(0);
    expect(result.claims).toBe(0);
  });

  it('moves legacy backend-switch JSON into its dedicated column without losing the intent', async () => {
    await expect(
      probe<{
        subphase: string;
        migrationIntent: null;
        backendSwitchKind: string;
      }>('backend-switch-column-upgrade'),
    ).resolves.toEqual({
      subphase: 'finalized',
      migrationIntent: null,
      backendSwitchKind: 'backend-switch',
    });
  });

  it('preserves operation, heartbeat, and release errors for both lease types', async () => {
    const result = await probe<{
      deployment: ProbeError;
      platform: ProbeError;
    }>('lifecycle-errors');

    for (const [kind, failure] of Object.entries(result)) {
      expect(failure).toMatchObject({
        name: 'AggregateError',
        errors: [
          { message: `forced ${kind} operation failure` },
          {
            message: expect.stringContaining(
              `forced ${kind} heartbeat failure`,
            ),
          },
          {
            message: expect.stringContaining(`forced ${kind} release failure`),
          },
        ],
      });
    }
  });

  it('coordinates separate direct-binding rate coordinators atomically in real D1', async () => {
    await expect(
      probe<{ blocked: boolean; count: number }>(
        'cloudflare-rate-coordination',
      ),
    ).resolves.toEqual({ blocked: true, count: 1_100 });
  });

  // Resetting the server recreates storage and rebinds `worker`, so this
  // case stays last.
  it('initializes the schema under concurrent first writes on fresh D1 storage', async () => {
    await server.reset();
    worker = server.getWorker();

    await expect(
      probe<{
        written: string[];
        columns: string[];
        rows: number;
        tables: string[];
      }>('cold-concurrent-schema-initialization'),
    ).resolves.toEqual({
      written: Array.from({ length: 16 }, (_, index) => `cold${index}`).sort(),
      columns: ['backend_switch_intent', 'settled_settlement_key'],
      rows: 16,
      tables: [
        'anchorage_fleet_deployments',
        'anchorage_fleet_leases',
        'anchorage_platform_plane_claims',
        'anchorage_platform_plane_leases',
      ],
    });
  });
});
