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

interface CleanupTerminalProbe {
  stale: ProbeError | undefined;
  rowPhaseAfterStale: string | null;
  receiptsAfterStale: number;
  claimsBefore: number;
  claimsAfter: number;
  rowAfterTerminal: string | null;
  persistedHasCompletedAt: boolean;
  replayEqual: boolean;
  keyOrderReplayEqual: boolean;
  conflict: ProbeError | undefined;
  foreign: ProbeError | undefined;
  reprovisionPhase: string | null;
  survivingOperationId: string | null;
}

interface CleanupPruneProbe {
  invalid: (ProbeError | undefined)[];
  untouched: number;
  nothing: { deleted: number };
  firstTwo: { deleted: number };
  remainingAfterFirstTwo: string[];
  lowerBound: { deleted: number };
  rest: { deleted: number };
  finalCount: number;
}

interface CleanupClaimsProbe {
  identities: { resource_set_key: string; platform_plane_identity: string }[];
  claimsAfter: number;
  rowAfter: string | null;
}

interface BoundedDecommissionProbe {
  readonly result: {
    readonly status: 'pending' | 'blocked' | 'complete';
    readonly token: {
      readonly version: 1;
      readonly tenantTag: string;
      readonly environment: string;
      readonly operationId: string;
      readonly revision: number;
    };
  };
  readonly trace: string[];
  readonly phase: string;
  readonly lifecyclePhase: string;
  readonly intentState: string;
  readonly revision: number;
  readonly generation: number;
  readonly resourceStates: string[];
  readonly bucketName: string;
  readonly lostWriteCount: number;
  readonly precommitWriteFailureCount: number;
  readonly provider: {
    readonly databaseId: string;
    readonly databasePresent: boolean;
    readonly observedDatabaseId: string;
    readonly observedDatabaseName: string;
    readonly owner: string;
    readonly receiptAuthority: string | null;
    readonly receiptOperationId: string | null;
    readonly receiptLocation: string | null;
    readonly receiptSize: number | null;
    readonly receiptSha256: string | null;
    readonly receiptCommitCount: number;
    readonly exportCallCount: number;
    readonly deleteCount: number;
    readonly ownershipAssertionCount: number;
  };
  readonly claims: Array<{
    readonly resourceType: string;
    readonly resourceName: string;
    readonly resourceRole: string;
  }>;
}

const INVENTORY_OPERATION_ID = '123e4567-e89b-42d3-a456-426614174200';

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
  // Real workerd + D1 through Wrangler: the two-pass R2 detach/deletion title
  // timed out at a 30 s cap inside the full package suite and has since needed
  // as much as 45.2 s in a six-file run; 150 s keeps a 3x margin over that.
  // The hooks below repeat this value because hooks take vitest's hookTimeout,
  // not this option; every title inherits it.
  timeout: 150_000,
}, () => {
  let server: TestHarness;
  let worker: WorkerHandle;

  beforeAll(async () => {
    server = createTestHarness(harnessOptions());
    await server.listen();
    worker = server.getWorker();
  }, 150_000);

  afterAll(async () => {
    await server.close();
  }, 150_000);

  async function probe<T>(action: string, input?: unknown): Promise<T> {
    const response = await worker.fetch('/fleet-state', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        action,
        ...(input === undefined ? {} : { input }),
      }),
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
  });

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
  });

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

  it('upgrades a legacy table and round-trips the decommission shell', async () => {
    await expect(
      probe<{
        phase: string;
        revision: number;
        columns: string[];
      }>('decommission-intent-column-upgrade'),
    ).resolves.toEqual({
      phase: 'decommission-advancing',
      revision: 1,
      columns: [
        'backend_switch_intent',
        'cleanup_intent',
        'invocation_authority',
        'settled_settlement_key',
        'decommission_intent',
      ],
    });
  });

  it('converges only exact lost decommission writes in real D1', async () => {
    const result = await probe<{
      exactRevision: number;
      changedFailure: ProbeError;
      finalRevision: number;
      claimCount: number;
    }>('decommission-intent-lost-response');

    expect(result).toEqual({
      exactRevision: 1,
      changedFailure: {
        name: 'Error',
        message: expect.stringContaining('mixed atomic ownership commit'),
      },
      finalRevision: 3,
      claimCount: 1,
    });
  });

  it('persists one bounded R2 step per Worker request through two-pass detach and deletion', async () => {
    const step = (
      operation: Readonly<
        { kind: 'start' } | { kind: 'continue'; token: unknown }
      >,
    ) =>
      probe<BoundedDecommissionProbe>('bounded-decommission-step', {
        tenantTag: 'advance',
        operation,
      });
    const results: BoundedDecommissionProbe[] = [];
    results.push(await step({ kind: 'start' }));
    for (let index = 0; index < 20; index += 1) {
      if (results.at(-1)?.result.status === 'complete') break;
      results.push(
        await step({
          kind: 'continue',
          token: results.at(-1)?.result.token,
        }),
      );
    }

    expect(
      results.map((result) => ({
        revision: result.revision,
        generation: result.generation,
        lifecyclePhase: result.lifecyclePhase,
        intentState: result.intentState,
        resourceState: result.resourceStates[0],
        trace: result.trace,
      })),
    ).toEqual([
      {
        revision: 0,
        generation: 0,
        lifecyclePhase: 'application-resources-deleting',
        intentState: 'transitioning',
        resourceState: 'created',
        trace: [],
      },
      {
        revision: 1,
        generation: 1,
        lifecyclePhase: 'application-resources-deleting',
        intentState: 'discover',
        resourceState: 'detach-authorized',
        trace: ['r2-find'],
      },
      {
        revision: 2,
        generation: 1,
        lifecyclePhase: 'application-resources-deleting',
        intentState: 'verify',
        resourceState: 'detach-authorized',
        trace: ['r2-find', 'scan:discover'],
      },
      {
        revision: 3,
        generation: 1,
        lifecyclePhase: 'application-resources-deleting',
        intentState: 'transitioning',
        resourceState: 'detached',
        trace: ['r2-find', 'scan:verify'],
      },
      {
        revision: 4,
        generation: 1,
        lifecyclePhase: 'application-resources-deleting',
        intentState: 'transitioning',
        resourceState: 'empty-authorized',
        trace: ['r2-find'],
      },
      {
        revision: 5,
        generation: 1,
        lifecyclePhase: 'application-resources-deleting',
        intentState: 'transitioning',
        resourceState: 'empty',
        trace: ['r2-find', 'r2-empty'],
      },
      {
        revision: 6,
        generation: 1,
        lifecyclePhase: 'application-resources-deleting',
        intentState: 'transitioning',
        resourceState: 'delete-authorized',
        trace: [],
      },
      {
        revision: 7,
        generation: 1,
        lifecyclePhase: 'application-resources-deleting',
        intentState: 'transitioning',
        resourceState: 'deleted',
        trace: ['r2-find', 'r2-delete', 'r2-find'],
      },
      {
        revision: 8,
        generation: 1,
        lifecyclePhase: 'application-resources-deleted',
        intentState: 'transitioning',
        resourceState: 'deleted',
        trace: ['r2-find'],
      },
      {
        revision: 9,
        generation: 2,
        lifecyclePhase: 'application-resources-deleted',
        intentState: 'discover',
        resourceState: 'deleted',
        trace: ['d1-get', 'd1-owner'],
      },
      {
        revision: 10,
        generation: 2,
        lifecyclePhase: 'application-resources-deleted',
        intentState: 'verify',
        resourceState: 'deleted',
        trace: ['d1-get', 'd1-owner', 'scan:discover'],
      },
      {
        revision: 11,
        generation: 2,
        lifecyclePhase: 'database-exported',
        intentState: 'transitioning',
        resourceState: 'deleted',
        trace: [
          'd1-get',
          'd1-owner',
          'scan:verify',
          'd1-get',
          'd1-owner',
          'd1-residuals',
          'd1-export',
        ],
      },
      {
        revision: 12,
        generation: 3,
        lifecyclePhase: 'database-exported',
        intentState: 'discover',
        resourceState: 'deleted',
        trace: ['d1-get', 'd1-owner'],
      },
      {
        revision: 13,
        generation: 3,
        lifecyclePhase: 'database-exported',
        intentState: 'verify',
        resourceState: 'deleted',
        trace: ['d1-get', 'd1-owner', 'scan:discover'],
      },
      {
        revision: 14,
        generation: 3,
        lifecyclePhase: 'database-deleting',
        intentState: 'transitioning',
        resourceState: 'deleted',
        trace: [
          'd1-get',
          'd1-owner',
          'scan:verify',
          'd1-get',
          'd1-owner',
          'd1-residuals',
          'd1-delete',
          'd1-get',
        ],
      },
      {
        revision: 15,
        generation: 3,
        lifecyclePhase: 'decommissioned',
        intentState: 'complete',
        resourceState: 'deleted',
        trace: ['d1-get'],
      },
    ]);

    const expectedClaims = [
      {
        resourceType: 'r2-bucket',
        resourceName: results[0]?.bucketName,
        resourceRole: 'deployment-r2',
      },
      {
        resourceType: 'worker-script',
        resourceName: 'advance-worker',
        resourceRole: 'deployment-worker',
      },
    ];
    for (const result of results.slice(0, -1)) {
      expect(result.result.status).toBe('pending');
      expect(result.phase).toBe('decommission-advancing');
      expect(result.claims).toEqual(expectedClaims);
      expect(result.lostWriteCount).toBe(0);
    }
    const terminal = results.at(-1);
    expect(terminal).toMatchObject({
      result: { status: 'complete' },
      phase: 'decommissioned',
      provider: {
        databaseId: '00000000-0000-4000-8000-000000000201',
        databasePresent: false,
        receiptAuthority: 'd1-test://fleet-exports/receipts/v1',
        receiptOperationId: '00000000-0000-4000-8000-000000000101',
        receiptSize: 37,
        receiptSha256: 'c'.repeat(64),
        receiptCommitCount: 1,
        exportCallCount: 1,
        deleteCount: 1,
      },
      claims: expectedClaims,
      lostWriteCount: 0,
      precommitWriteFailureCount: 0,
    });

    const reset = () =>
      probe<{ reset: true }>('bounded-decommission-reset', {
        tenantTag: 'advance',
      });
    const reachD1Verify = async (
      boundary: 'application-resources-deleted' | 'database-exported',
    ): Promise<BoundedDecommissionProbe> => {
      await reset();
      let current = await probe<BoundedDecommissionProbe>(
        'bounded-decommission-step',
        {
          tenantTag: 'advance',
          operation: { kind: 'start' },
          seedAtD1: true,
        },
      );
      for (let index = 0; index < 8; index += 1) {
        if (
          current.lifecyclePhase === boundary &&
          current.intentState === 'verify'
        ) {
          return current;
        }
        current = await probe<BoundedDecommissionProbe>(
          'bounded-decommission-step',
          {
            tenantTag: 'advance',
            operation: { kind: 'continue', token: current.result.token },
          },
        );
      }
      throw new Error(`bounded harness did not reach ${boundary} verify`);
    };
    const postScanMutations = [
      { mutation: 'absent', error: 'is absent' },
      { mutation: 'id', error: 'resolved with unexpected identity' },
      { mutation: 'name', error: 'resolved with unexpected identity' },
      { mutation: 'owner', error: "owned by 'foreign'" },
    ] as const;
    for (const boundary of [
      'application-resources-deleted',
      'database-exported',
    ] as const) {
      for (const row of postScanMutations) {
        const verify = await reachD1Verify(boundary);
        await expect(
          probe<BoundedDecommissionProbe>('bounded-decommission-step', {
            tenantTag: 'advance',
            operation: { kind: 'continue', token: verify.result.token },
            afterScan: row.mutation,
          }),
        ).rejects.toThrow(row.error);
      }
    }
    await reset();
  });

  it('converges a lost coordinator write and makes the replayed token stale in real D1', async () => {
    const step = (
      operation: Readonly<
        { kind: 'start' } | { kind: 'continue'; token: unknown }
      >,
      faults: Readonly<{
        failWriteBeforeCommit?: boolean;
        loseWrite?: boolean;
        loseReceiptResponse?: boolean;
        loseDeleteResponse?: boolean;
        nextExportOutcome?:
          | Readonly<{
              status: 'fulfilled';
              value?: 'default' | 'present' | 'absent';
            }>
          | Readonly<{
              status: 'rejected';
              reason: 'error' | 'null' | 'undefined';
            }>;
        nextDeleteOutcome?:
          | Readonly<{
              status: 'fulfilled';
              value?: 'default' | 'present' | 'absent';
            }>
          | Readonly<{
              status: 'rejected';
              reason: 'error' | 'null' | 'undefined';
            }>;
        nextReadbackOutcome?:
          | Readonly<{
              status: 'fulfilled';
              value?: 'default' | 'present' | 'absent';
            }>
          | Readonly<{
              status: 'rejected';
              reason: 'error' | 'null' | 'undefined';
            }>;
        nextOwnershipFailureOrdinal?: number;
        seedAtD1?: boolean;
      }> = {},
    ) =>
      probe<BoundedDecommissionProbe>('bounded-decommission-step', {
        tenantTag: 'advancelost',
        operation,
        ...faults,
      });
    const started = await step({ kind: 'start' });
    const discover = await step({
      kind: 'continue',
      token: started.result.token,
    });
    const verify = await step(
      { kind: 'continue', token: discover.result.token },
      { loseWrite: true },
    );
    const replay = await step({
      kind: 'continue',
      token: discover.result.token,
    });

    expect(verify).toMatchObject({
      result: {
        status: 'pending',
        token: {
          version: 1,
          tenantTag: 'advancelost',
          environment: 'production',
          operationId: '00000000-0000-4000-8000-000000000102',
          revision: 2,
        },
      },
      trace: ['r2-find', 'scan:discover'],
      phase: 'decommission-advancing',
      lifecyclePhase: 'application-resources-deleting',
      intentState: 'verify',
      revision: 2,
      generation: 1,
      resourceStates: ['detach-authorized'],
      lostWriteCount: 1,
    });
    expect(replay).toMatchObject({
      result: { status: 'pending', token: verify.result.token },
      trace: [],
      phase: 'decommission-advancing',
      lifecyclePhase: 'application-resources-deleting',
      intentState: 'verify',
      revision: 2,
      generation: 1,
      resourceStates: ['detach-authorized'],
      lostWriteCount: 0,
    });
    expect(replay.claims).toEqual([
      {
        resourceType: 'r2-bucket',
        resourceName: verify.bucketName,
        resourceRole: 'deployment-r2',
      },
      {
        resourceType: 'worker-script',
        resourceName: 'advancelost-worker',
        resourceRole: 'deployment-worker',
      },
    ]);

    let current = await step({
      kind: 'continue',
      token: verify.result.token,
    });
    for (let index = 0; index < 10; index += 1) {
      if (
        current.lifecyclePhase === 'application-resources-deleted' &&
        current.intentState === 'verify'
      ) {
        break;
      }
      current = await step({
        kind: 'continue',
        token: current.result.token,
      });
    }
    expect(current).toMatchObject({
      result: { status: 'pending' },
      lifecyclePhase: 'application-resources-deleted',
      intentState: 'verify',
      revision: 10,
      generation: 2,
      provider: {
        receiptCommitCount: 0,
        exportCallCount: 0,
        deleteCount: 0,
      },
    });
    const preExportVerifyToken = current.result.token;

    await expect(
      step(
        { kind: 'continue', token: preExportVerifyToken },
        { loseReceiptResponse: true },
      ),
    ).rejects.toThrow('bounded receipt response lost');
    await expect(
      step(
        { kind: 'continue', token: preExportVerifyToken },
        { failWriteBeforeCommit: true },
      ),
    ).rejects.toThrow('mixed atomic ownership commit');
    const exported = await step(
      { kind: 'continue', token: preExportVerifyToken },
      { loseWrite: true },
    );
    expect(exported).toMatchObject({
      lifecyclePhase: 'database-exported',
      intentState: 'transitioning',
      revision: 11,
      lostWriteCount: 1,
      provider: {
        receiptAuthority: 'd1-test://fleet-exports/receipts/v1',
        receiptOperationId: '00000000-0000-4000-8000-000000000102',
        receiptCommitCount: 1,
        exportCallCount: 3,
        deleteCount: 0,
      },
    });
    const exportReplay = await step({
      kind: 'continue',
      token: preExportVerifyToken,
    });
    expect(exportReplay).toMatchObject({
      result: { token: exported.result.token },
      trace: [],
      revision: 11,
      provider: { receiptCommitCount: 1, exportCallCount: 3, deleteCount: 0 },
    });

    const preDeleteDiscover = await step({
      kind: 'continue',
      token: exported.result.token,
    });
    const preDeleteVerify = await step({
      kind: 'continue',
      token: preDeleteDiscover.result.token,
    });
    const barrier = await step(
      { kind: 'continue', token: preDeleteVerify.result.token },
      { loseDeleteResponse: true },
    );
    expect(barrier).toMatchObject({
      lifecyclePhase: 'database-deleting',
      intentState: 'transitioning',
      revision: 14,
      trace: [
        'd1-get',
        'd1-owner',
        'scan:verify',
        'd1-get',
        'd1-owner',
        'd1-residuals',
        'd1-delete',
        'd1-get',
      ],
      provider: {
        databasePresent: false,
        receiptCommitCount: 1,
        exportCallCount: 3,
        deleteCount: 1,
      },
    });
    const terminal = await step(
      { kind: 'continue', token: barrier.result.token },
      { loseWrite: true },
    );
    expect(terminal).toMatchObject({
      result: { status: 'complete' },
      trace: ['d1-get'],
      phase: 'decommissioned',
      lifecyclePhase: 'decommissioned',
      intentState: 'complete',
      revision: 15,
      lostWriteCount: 1,
      provider: {
        databasePresent: false,
        receiptCommitCount: 1,
        exportCallCount: 3,
        deleteCount: 1,
      },
    });
    const barrierReplay = await step({
      kind: 'continue',
      token: barrier.result.token,
    });
    expect(barrierReplay).toMatchObject({
      result: { status: 'complete', token: terminal.result.token },
      trace: [],
      revision: 15,
      provider: { exportCallCount: 3, deleteCount: 1 },
    });

    const reset = () =>
      probe<{ reset: true }>('bounded-decommission-reset', {
        tenantTag: 'advancelost',
      });
    const reachExportVerify = async () => {
      const startedAtD1 = await step({ kind: 'start' }, { seedAtD1: true });
      const selected = await step({
        kind: 'continue',
        token: startedAtD1.result.token,
      });
      return step({
        kind: 'continue',
        token: selected.result.token,
      });
    };
    const reachDeleteVerify = async () => {
      const exportVerify = await reachExportVerify();
      const exported = await step({
        kind: 'continue',
        token: exportVerify.result.token,
      });
      const deleteDiscover = await step({
        kind: 'continue',
        token: exported.result.token,
      });
      return step({
        kind: 'continue',
        token: deleteDiscover.result.token,
      });
    };
    await reset();
    const exportVerify = await reachExportVerify();
    const expectedClaims = [
      {
        resourceType: 'r2-bucket',
        resourceName: exportVerify.bucketName,
        resourceRole: 'deployment-r2',
      },
      {
        resourceType: 'worker-script',
        resourceName: 'advancelost-worker',
        resourceRole: 'deployment-worker',
      },
    ];
    await expect(
      step(
        { kind: 'continue', token: exportVerify.result.token },
        {
          nextExportOutcome: { status: 'rejected', reason: 'error' },
        },
      ),
    ).rejects.toThrow('bounded provider injected rejection');
    const afterExportFailure = await step({ kind: 'start' });
    expect(afterExportFailure).toMatchObject({
      lifecyclePhase: 'application-resources-deleted',
      intentState: 'verify',
      revision: 2,
      provider: {
        receiptCommitCount: 0,
        exportCallCount: 1,
        deleteCount: 0,
      },
      claims: expectedClaims,
    });
    const exportRetry = await step({
      kind: 'continue',
      token: afterExportFailure.result.token,
    });
    expect(exportRetry).toMatchObject({
      lifecyclePhase: 'database-exported',
      intentState: 'transitioning',
      revision: 3,
      provider: { receiptCommitCount: 1, exportCallCount: 2, deleteCount: 0 },
      claims: expectedClaims,
    });

    const deleteDiscover = await step({
      kind: 'continue',
      token: exportRetry.result.token,
    });
    const deleteVerify = await step({
      kind: 'continue',
      token: deleteDiscover.result.token,
    });
    await expect(
      step(
        { kind: 'continue', token: deleteVerify.result.token },
        {
          nextDeleteOutcome: { status: 'rejected', reason: 'error' },
          nextReadbackOutcome: { status: 'rejected', reason: 'undefined' },
        },
      ),
    ).rejects.toThrow('"name":"undefined"');
    const afterReadbackFailure = await step({ kind: 'start' });
    expect(afterReadbackFailure).toMatchObject({
      lifecyclePhase: 'database-deleting',
      intentState: 'transitioning',
      revision: 6,
      provider: {
        databasePresent: true,
        receiptCommitCount: 1,
        exportCallCount: 2,
        deleteCount: 0,
      },
      claims: expectedClaims,
    });
    let converged = await step({
      kind: 'continue',
      token: afterReadbackFailure.result.token,
    });
    for (
      let index = 0;
      index < 6 && converged.result.status !== 'complete';
      index += 1
    ) {
      converged = await step({
        kind: 'continue',
        token: converged.result.token,
      });
    }
    expect(converged).toMatchObject({
      result: { status: 'complete' },
      phase: 'decommissioned',
      revision: 10,
      provider: { databasePresent: false, deleteCount: 1 },
      claims: expectedClaims,
    });

    await reset();
    const ownershipVerify = await reachDeleteVerify();
    const ownershipClaims = [
      {
        resourceType: 'r2-bucket',
        resourceName: ownershipVerify.bucketName,
        resourceRole: 'deployment-r2',
      },
      {
        resourceType: 'worker-script',
        resourceName: 'advancelost-worker',
        resourceRole: 'deployment-worker',
      },
    ];
    await expect(
      step(
        { kind: 'continue', token: ownershipVerify.result.token },
        { nextOwnershipFailureOrdinal: 1 },
      ),
    ).rejects.toThrow('bounded provider lease ownership transferred');
    const afterOwnershipFailure = await step({ kind: 'start' });
    expect(afterOwnershipFailure).toMatchObject({
      lifecyclePhase: 'database-deleting',
      intentState: 'transitioning',
      revision: 6,
      provider: {
        databasePresent: true,
        ownershipAssertionCount: 1,
        deleteCount: 0,
      },
      claims: ownershipClaims,
    });
    let ownershipRetry = await step({
      kind: 'continue',
      token: afterOwnershipFailure.result.token,
    });
    for (
      let index = 0;
      index < 6 && ownershipRetry.result.status !== 'complete';
      index += 1
    ) {
      ownershipRetry = await step({
        kind: 'continue',
        token: ownershipRetry.result.token,
      });
    }
    expect(ownershipRetry).toMatchObject({
      result: { status: 'complete' },
      phase: 'decommissioned',
      revision: 10,
      provider: {
        databasePresent: false,
        ownershipAssertionCount: 4,
        deleteCount: 1,
      },
      claims: ownershipClaims,
    });
    await reset();
  });

  it('replays a bounded backend-switch operation after D1 write loss', async () => {
    type Stage = 'start' | 'cursor' | 'receipt' | 'barrier' | 'terminal';
    interface Result {
      readonly lostWriteCount: number;
      readonly phase: string;
      readonly switchSubphase: string;
      readonly shellState: string;
      readonly shellRevision: number;
      readonly lifecyclePhase: string;
      readonly scanStage?: string;
      readonly databaseExportLocation?: string;
      readonly columnsPresent: boolean;
    }
    await probe<{ reset: true }>('bounded-backend-switch-write-step', {
      stage: 'reset',
    });
    const step = (stage: Stage, loseWrite = true) =>
      probe<Result>('bounded-backend-switch-write-step', {
        stage,
        loseWrite,
      });

    await expect(step('start')).resolves.toMatchObject({
      lostWriteCount: 1,
      phase: 'decommission-advancing',
      switchSubphase: 'decommission-export-authorized',
      shellState: 'transitioning',
      shellRevision: 0,
      lifecyclePhase: 'application-resources-deleted',
      columnsPresent: true,
    });
    await expect(step('cursor')).resolves.toMatchObject({
      lostWriteCount: 1,
      switchSubphase: 'decommission-export-authorized',
      shellState: 'discover',
      shellRevision: 1,
      scanStage: 'ordinary-script-inventory',
      columnsPresent: true,
    });
    await expect(step('receipt')).resolves.toMatchObject({
      lostWriteCount: 1,
      switchSubphase: 'decommission-exported',
      shellState: 'transitioning',
      shellRevision: 2,
      lifecyclePhase: 'database-exported',
      databaseExportLocation:
        'memory://fleet-exports/backend-switch/switchlost.sql',
      columnsPresent: true,
    });
    await expect(step('barrier')).resolves.toMatchObject({
      lostWriteCount: 1,
      switchSubphase: 'decommission-database-authorized',
      shellRevision: 3,
      lifecyclePhase: 'database-deleting',
      columnsPresent: true,
    });
    await expect(step('terminal')).resolves.toMatchObject({
      lostWriteCount: 1,
      phase: 'decommissioned',
      switchSubphase: 'decommissioned',
      shellState: 'complete',
      shellRevision: 4,
      lifecyclePhase: 'decommissioned',
      databaseExportLocation:
        'memory://fleet-exports/backend-switch/switchlost.sql',
      columnsPresent: true,
    });

    await probe<{ reset: true }>('bounded-backend-switch-write-step', {
      stage: 'reset',
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

  // Resetting the server recreates storage and rebinds `worker`,
  // so this case stays last.

  it('completes a cleanup terminal atomically with a receipt, claims release, and row delete', async () => {
    const result = await probe<CleanupTerminalProbe>(
      'cleanup-terminal-receipt',
    );
    expect(result.claimsBefore).toBe(1);
    expect(result.claimsAfter).toBe(0);
    expect(result.rowAfterTerminal).toBeNull();
    expect(result.persistedHasCompletedAt).toBe(true);
  });

  it('refuses a stale cleanup terminal revision without mutating anything', async () => {
    const result = await probe<CleanupTerminalProbe>(
      'cleanup-terminal-receipt',
    );
    expect(result.stale).toEqual({
      name: 'Error',
      message: expect.stringContaining('no matching active cleanup operation'),
    });
    expect(result.rowPhaseAfterStale).toBe('cleanup-advancing');
    expect(result.receiptsAfterStale).toBe(0);
  });

  it('converges replayed cleanup terminals across evidence key order', async () => {
    const result = await probe<CleanupTerminalProbe>(
      'cleanup-terminal-receipt',
    );
    expect(result.replayEqual).toBe(true);
    expect(result.keyOrderReplayEqual).toBe(true);
  });

  it('refuses conflicting and foreign cleanup terminal receipts', async () => {
    const result = await probe<CleanupTerminalProbe>(
      'cleanup-terminal-receipt',
    );
    expect(result.conflict).toEqual({
      name: 'Error',
      message: expect.stringContaining('cleanup receipt conflict'),
    });
    expect(result.foreign).toEqual({
      name: 'Error',
      message: expect.stringContaining('cannot write'),
    });
  });

  it('keeps historical cleanup receipts across an immediate same-key reprovision', async () => {
    const result = await probe<CleanupTerminalProbe>(
      'cleanup-terminal-receipt',
    );
    expect(result.reprovisionPhase).toBe('ready');
    expect(result.survivingOperationId).toBe(
      '00000000-0000-4000-8000-0000000000aa',
    );
  });

  it('fails closed on invalid cleanup receipt prune limits', async () => {
    const result = await probe<CleanupPruneProbe>('cleanup-receipt-prune');
    for (const refusal of result.invalid) {
      expect(refusal).toEqual({
        name: 'Error',
        message: expect.stringContaining('limit'),
      });
    }
    expect(result.untouched).toBe(3);
  });

  it('prunes cleanup receipts in stable database-time order', async () => {
    const result = await probe<CleanupPruneProbe>('cleanup-receipt-prune');
    expect(result.nothing).toEqual({ deleted: 0 });
    expect(result.firstTwo).toEqual({ deleted: 2 });
    expect(result.remainingAfterFirstTwo).toEqual([
      '00000000-0000-4000-8000-0000000000a3',
    ]);
    expect(result.lowerBound).toEqual({ deleted: 1 });
    expect(result.rest).toEqual({ deleted: 0 });
    expect(result.finalCount).toBe(0);
  });

  it('releases claims and the fleet row through the force deletion path', async () => {
    const result = await probe<CleanupClaimsProbe>('cleanup-claims-release');
    expect(result.claimsAfter).toBe(0);
    expect(result.rowAfter).toBeNull();
  });

  it('writes every deployment claim under the deployment identity', async () => {
    const result = await probe<CleanupClaimsProbe>('cleanup-claims-release');
    expect(result.identities.length).toBeGreaterThan(0);
    for (const claim of result.identities) {
      expect(claim.resource_set_key).toBe('deployment:claimsrel:production');
      expect(claim.platform_plane_identity).toBe(
        'deployment:claimsrel:production',
      );
    }
  });

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
      columns: [
        'backend_switch_intent',
        'settled_settlement_key',
        'decommission_intent',
        'cleanup_intent',
        'invocation_authority',
      ],
      rows: 16,
      tables: [
        'anchorage_fleet_deployments',
        'anchorage_fleet_leases',
        'anchorage_platform_plane_claims',
        'anchorage_platform_plane_leases',
      ],
    });
  });

  it('applies the inventory start batch atomically under concurrent stores', async () => {
    const result = await probe<{
      started: number;
      rejected: number;
      head: {
        activeOperationId: string | null;
        latestFinalizedGeneration: number | null;
        nextGeneration: number;
      };
      runs: {
        operationId: string;
        generation: number;
        digestMatches: boolean;
        finalized: boolean;
      }[];
      generation: number;
    }>('inventory-start-atomicity');

    expect(result.started).toBe(1);
    expect(result.rejected).toBe(15);
    expect(result.head).toEqual({
      activeOperationId: INVENTORY_OPERATION_ID,
      latestFinalizedGeneration: null,
      nextGeneration: 2,
    });
    expect(result.runs).toEqual([
      {
        operationId: INVENTORY_OPERATION_ID,
        generation: 1,
        digestMatches: true,
        finalized: false,
      },
    ]);
    expect(result.generation).toBe(1);
  });

  it('admits one commit writer and converges the rest under concurrent batches', async () => {
    const result = await probe<{
      committed: number;
      converged: number;
      conflicts: number;
      corrupt: number;
      winnerIsWriter: boolean;
      lostResponseReplay: string;
      revision: number;
      staleReplay: string;
      rowCounts: { kind: string; count: number }[];
    }>('inventory-commit-concurrency');

    expect(result).toMatchObject({
      committed: 1,
      converged: 0,
      conflicts: 15,
      corrupt: 0,
      winnerIsWriter: true,
      lostResponseReplay: 'converged',
      revision: 2,
      staleReplay: 'conflict',
    });
    // The guarded staging inserts fence every loser out, so only the winner's
    // own meta ordinal and the later trailing chunk's row exist.
    expect(result.rowCounts).toEqual([
      { kind: 'deployment', count: 1 },
      { kind: 'finding', count: 1 },
      { kind: 'meta', count: 2 },
      { kind: 'registration', count: 1 },
    ]);
  });

  it('converges a lost finalize response through the run and head readback', async () => {
    const result = await probe<{
      first: { generation: number; factCount: number; finalizedAtMs: number };
      replayed: { generation: number };
      identical: boolean;
      head: {
        activeOperationId: string | null;
        latestFinalizedGeneration: number | null;
      };
    }>('inventory-finalize-convergence');

    expect(result.identical).toBe(true);
    expect(result.first.generation).toBe(1);
    expect(result.first.factCount).toBe(1);
    expect(result.first.finalizedAtMs).toBeGreaterThan(0);
    expect(result.head).toEqual({
      activeOperationId: null,
      latestFinalizedGeneration: 1,
    });
  });

  it('reads back a finalized generation with its manifest and ordinals', async () => {
    const result = await probe<{
      ref: {
        generation: number;
        rowManifest: Record<string, number>;
        factCount: number;
      };
      latestMatches: boolean;
      rowOrdinals: string[];
      factOrdinals: string[];
    }>('inventory-generation-readback');

    expect(result.ref.generation).toBe(1);
    expect(result.ref.rowManifest).toMatchObject({
      registration: 1,
      deployment: 1,
      finding: 1,
      meta: 0,
    });
    expect(result.ref.factCount).toBe(1);
    expect(result.latestMatches).toBe(true);
    expect(result.rowOrdinals).toEqual([
      'deployment:0',
      'finding:0',
      'registration:0',
    ]);
    expect(result.factOrdinals).toEqual(['0:secret-name:0']);
  });

  it('refuses a corrupt generation and leaves a mismatched finalize staging', async () => {
    const result = await probe<{
      readError: ProbeError;
      finalizeError: ProbeError;
      stateAfterFinalize: string | null;
      latestGeneration: number | null;
    }>('inventory-corrupt-unreadable');

    expect(result.readError.message).toBe(
      'fleet inventory generation 1 is corrupt',
    );
    expect(result.finalizeError.message).toMatch(
      /^fleet inventory run '[0-9a-f-]+' does not match its finalize manifest$/,
    );
    expect(result.stateAfterFinalize).toBe('staging');
    expect(result.latestGeneration).toBe(1);
  });

  it('prunes generations in stable order while protecting latest and pinned', async () => {
    const result = await probe<{
      deleted: number[];
      pinnedSurvives: number;
      surviving: number[];
      survivingRowGenerations: number[];
    }>('inventory-prune-order');

    expect(result.deleted).toEqual([1, 1, 0, 1]);
    expect(result.pinnedSurvives).toBe(2);
    expect(result.surviving).toEqual([4]);
    expect(result.survivingRowGenerations).toEqual([4]);
  });

  it('upserts the inventory lease when expired and keeps it alive by renewal', async () => {
    const result = await probe<{
      takeover: string | ProbeError;
      heartbeatObserved: boolean;
      contenderRejected: boolean;
      leasesAfterRelease: number;
    }>('inventory-lease-lifecycle');

    expect(result.takeover).toBe('acquired');
    expect(result.heartbeatObserved).toBe(true);
    expect(result.contenderRejected).toBe(true);
    expect(result.leasesAfterRelease).toBe(0);
  });

  it('initializes the six inventory tables under concurrent first reads on fresh D1 storage', async () => {
    await server.reset();
    worker = server.getWorker();

    const result = await probe<{
      latest: number;
      columns: Record<string, string[]>;
      tables: string[];
      generation: number | null;
    }>('inventory-cold-concurrent-schema');

    expect(result.latest).toBe(16);
    expect(result.tables).toEqual([
      'anchorage_fleet_inventory_deployment_facts',
      'anchorage_fleet_inventory_heads',
      'anchorage_fleet_inventory_leases',
      'anchorage_fleet_inventory_pins',
      'anchorage_fleet_inventory_rows',
      'anchorage_fleet_inventory_runs',
    ]);
    expect(result.columns).toEqual({
      anchorage_fleet_inventory_heads: [
        'account_id:TEXT',
        'active_operation_id:TEXT',
        'latest_finalized_generation:INTEGER',
        'next_generation:INTEGER',
      ],
      anchorage_fleet_inventory_runs: [
        'operation_id:TEXT',
        'account_id:TEXT',
        'generation:INTEGER',
        'options_digest:TEXT',
        'run_record:TEXT',
        'created_at_ms:INTEGER',
        'finalized_at_ms:INTEGER',
      ],
      anchorage_fleet_inventory_rows: [
        'account_id:TEXT',
        'generation:INTEGER',
        'kind:TEXT',
        'ordinal:INTEGER',
        'payload:TEXT',
      ],
      anchorage_fleet_inventory_deployment_facts: [
        'account_id:TEXT',
        'generation:INTEGER',
        'deployment_ordinal:INTEGER',
        'fact_kind:TEXT',
        'fact_ordinal:INTEGER',
        'payload:TEXT',
      ],
      anchorage_fleet_inventory_leases: [
        'account_id:TEXT',
        'owner_token:TEXT',
        'expires_at:INTEGER',
      ],
      anchorage_fleet_inventory_pins: [
        'account_id:TEXT',
        'generation:INTEGER',
        'pinned_by:TEXT',
        'pinned_at_ms:INTEGER',
      ],
    });
    expect(result.generation).toBe(1);
  });

  it('operation-start atomicity', async () => {
    await expect(
      probe<{
        started: number;
        rejected: number;
        activeOperationId: string | null;
        operations: number;
      }>('operation-start-atomicity'),
    ).resolves.toEqual({
      started: 1,
      rejected: 15,
      activeOperationId: '123e4567-e89b-42d3-a456-426614174300',
      operations: 1,
    });
  });

  it('commit concurrency (losers land zero rows; winner replay converges without a second revision advance)', async () => {
    const result = await probe<{
      winners: number;
      losers: number;
      rowOrdinals: number[];
      replayRevision: number;
      noSecondAdvance: boolean;
    }>('operation-commit-concurrency');
    expect(result.winners).toBe(1);
    expect(result.losers).toBe(15);
    expect(result.rowOrdinals).toHaveLength(1);
    expect(result.replayRevision).toBe(1);
    expect(result.noSecondAdvance).toBe(true);
  });

  it('finalize convergence', async () => {
    await expect(
      probe<{
        identical: boolean;
        revision: number;
        terminalAtMs: number;
        activeOperationId: string | null;
      }>('operation-finalize-convergence'),
    ).resolves.toEqual({
      identical: true,
      revision: 1,
      terminalAtMs: expect.any(Number),
      activeOperationId: null,
    });
  });

  it('rows-page readback', async () => {
    await expect(
      probe<{
        first: number[];
        firstDone: boolean;
        second: number[];
        secondDone: boolean;
      }>('operation-rows-readback'),
    ).resolves.toEqual({
      first: [0, 1],
      firstDone: false,
      second: [2],
      secondDone: true,
    });
  });

  it('corrupt payload unreadable', async () => {
    await expect(
      probe<ProbeError>('operation-corrupt-unreadable'),
    ).resolves.toEqual({
      name: 'FleetOperationStateError',
      message: 'fleet operation state is malformed',
    });
  });

  it('prune order + protected set', async () => {
    const result = await probe<{
      pruned: { deleted: number; releasedPins: number };
      remaining: string[];
    }>('operation-prune-order');
    expect(result.pruned).toEqual({ deleted: 1, releasedPins: 0 });
    expect(result.remaining).toEqual([
      '123e4567-e89b-42d3-a456-426614174311',
      '123e4567-e89b-42d3-a456-426614174312',
      '123e4567-e89b-42d3-a456-426614174313',
    ]);
  });

  it('per-kind lease independence + lifecycle at controlled times', async () => {
    await expect(
      probe<{
        takeover: string;
        heartbeatObserved: boolean;
        contenderRejected: boolean;
        leasesAfterRelease: number;
      }>('operation-lease-lifecycle'),
    ).resolves.toEqual({
      takeover: 'independent',
      heartbeatObserved: true,
      contenderRejected: true,
      leasesAfterRelease: 0,
    });
  });

  it('four-table cold+concurrent schema init', async () => {
    await server.reset();
    worker = server.getWorker();
    const result = await probe<{
      absent: number;
      columns: Record<string, string[]>;
      tables: string[];
    }>('operation-cold-concurrent-schema');
    expect(result.absent).toBe(16);
    expect(result.tables).toEqual([
      'anchorage_fleet_operation_heads',
      'anchorage_fleet_operation_leases',
      'anchorage_fleet_operation_rows',
      'anchorage_fleet_operations',
    ]);
    expect(result.columns).toEqual({
      anchorage_fleet_operation_leases: [
        'account_id:TEXT',
        'operation_kind:TEXT',
        'owner_token:TEXT',
        'expires_at:INTEGER',
      ],
      anchorage_fleet_operation_heads: [
        'account_id:TEXT',
        'operation_kind:TEXT',
        'active_operation_id:TEXT',
      ],
      anchorage_fleet_operations: [
        'account_id:TEXT',
        'operation_id:TEXT',
        'operation_kind:TEXT',
        'intake_digest:TEXT',
        'op_record:TEXT',
        'created_at_ms:INTEGER',
        'terminal_at_ms:INTEGER',
      ],
      anchorage_fleet_operation_rows: [
        'account_id:TEXT',
        'operation_id:TEXT',
        'row_kind:TEXT',
        'ordinal:INTEGER',
        'payload:TEXT',
      ],
    });
  });

  it('two-account same-UUID isolation', async () => {
    await expect(
      probe<Array<{ account_id: string; operation_id: string }>>(
        'operation-two-account-isolation',
      ),
    ).resolves.toEqual([
      {
        account_id: 'operation-account-one',
        operation_id: '123e4567-e89b-42d3-a456-426614174330',
      },
      {
        account_id: 'operation-account-two',
        operation_id: '123e4567-e89b-42d3-a456-426614174330',
      },
    ]);
  });
});
