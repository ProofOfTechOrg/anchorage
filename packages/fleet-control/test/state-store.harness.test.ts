// SPDX-License-Identifier: Apache-2.0

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createTestHarness,
  type TestHarness,
  type WorkerHandle,
} from 'wrangler';
import type { FleetInventoryAdvanceResult } from '../src/fleet-inventory-advance.js';
import {
  type FleetInventoryRunRecord,
  type FleetInventoryStagedFact,
  type FleetInventoryStagedRow,
  fleetInventoryOptionsDigest,
} from '../src/fleet-inventory-state.js';

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

interface InventorySnapshot {
  heads: Array<{
    account_id: string;
    active_operation_id: string | null;
    latest_finalized_generation: number | null;
    next_generation: number;
  }>;
  runs: Array<{
    account_id: string;
    operation_id: string;
    generation: number;
    options_digest: string;
    run_record: string;
    created_at_ms: number;
    finalized_at_ms: number | null;
  }>;
  rows: Array<{
    account_id: string;
    generation: number;
    kind: string;
    ordinal: number;
    payload: string;
  }>;
  facts: Array<{
    account_id: string;
    generation: number;
    deployment_ordinal: number;
    fact_kind: string;
    fact_ordinal: number;
    payload: string;
  }>;
}

interface InventoryCommitRefusalProbe {
  prior: FleetInventoryRunRecord;
  intended: FleetInventoryRunRecord;
  attempted: {
    runRecord: FleetInventoryRunRecord;
    rows: FleetInventoryStagedRow[];
    facts: FleetInventoryStagedFact[];
  };
  before: InventorySnapshot;
  afterRefusal: InventorySnapshot;
  refused: ProbeError | null;
  accepted: FleetInventoryRunRecord | null;
  acceptanceError: ProbeError | null;
  afterAcceptance: InventorySnapshot;
  replay: FleetInventoryRunRecord | null;
  afterReplay: InventorySnapshot;
}

interface OperationSnapshot {
  revision: number | null;
  record: string | null;
  rows: Array<{ row_kind: string; ordinal: number; payload: string }>;
}

interface OperationCommitProbe {
  before: OperationSnapshot;
  afterRefusal: OperationSnapshot;
  refused: ProbeError | null;
  accepted: { progress: { revision: number } };
  afterAcceptance: OperationSnapshot;
  replay: { progress: { revision: number } };
  afterReplay: OperationSnapshot;
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

function expectInventorySeed(
  snapshot: InventorySnapshot,
  record: FleetInventoryRunRecord,
) {
  expect(record).toMatchObject({
    state: 'staging',
    options: { scriptNamePrefix: 'anchorage' },
    progress: { generation: 1, revision: 1, factCount: 1 },
  });
  expect(snapshot.runs).toEqual([
    {
      account_id: 'account-inventory',
      operation_id: record.operationId,
      generation: 1,
      options_digest: fleetInventoryOptionsDigest(record.options),
      run_record: JSON.stringify(record),
      created_at_ms: expect.any(Number),
      finalized_at_ms: null,
    },
  ]);
  expect(snapshot.rows).toEqual(
    ['deployment', 'finding', 'registration'].map((kind) => ({
      account_id: 'account-inventory',
      generation: 1,
      kind,
      ordinal: 0,
      payload: JSON.stringify(
        kind === 'finding'
          ? { detail: 'stale route prior' }
          : { scriptName: 'prior' },
      ),
    })),
  );
  expect(snapshot.facts).toEqual([
    {
      account_id: 'account-inventory',
      generation: 1,
      deployment_ordinal: 0,
      fact_kind: 'secret-name',
      fact_ordinal: 0,
      payload: JSON.stringify({ name: 'ANCHORAGE_NAME_0' }),
    },
  ]);
}

function expectInventoryRefusalAndReplay(result: InventoryCommitRefusalProbe) {
  expectInventorySeed(result.before, result.prior);
  expect(result.afterRefusal).toEqual(result.before);
  expect(result.intended.progress.revision).toBe(2);
  expect(result.afterAcceptance.runs).toEqual([
    { ...result.before.runs[0], run_record: JSON.stringify(result.intended) },
  ]);
  expect(result.afterAcceptance.rows).toEqual([
    result.before.rows[0],
    result.before.rows[1],
    {
      account_id: 'account-inventory',
      generation: 1,
      kind: 'meta',
      ordinal: 0,
      payload: JSON.stringify({ marker: 'earlier-sibling' }),
    },
    result.before.rows[2],
  ]);
  expect(result.afterAcceptance.facts).toEqual([
    ...result.before.facts,
    {
      account_id: 'account-inventory',
      generation: 1,
      deployment_ordinal: 0,
      fact_kind: 'secret-name',
      fact_ordinal: 1,
      payload: JSON.stringify({ name: 'ANCHORAGE_NAME_1' }),
    },
  ]);
  expect(result.afterReplay).toEqual(result.afterAcceptance);
  expect(result.acceptanceError).toBeNull();
  expect(result.accepted).toEqual(result.intended);
  expect(result.replay).toEqual(result.intended);
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

  it('inventory maximum mixed chunk retains exact rows and facts through replay and finalization', async () => {
    const result = await probe<{
      acceptedRevision: number;
      replayEqual: boolean;
      rowsEqual: boolean;
      factsEqual: boolean;
      rowCount: number;
      factCount: number;
      maxStatements: number;
      maxBindings: number;
      maxSqlBytes: number;
      maxBindingBytes: number;
    }>('inventory-maximum-chunk');
    expect(result).toMatchObject({
      acceptedRevision: 1,
      replayEqual: true,
      rowsEqual: true,
      factsEqual: true,
      rowCount: 1000,
      factCount: 1000,
      maxStatements: 2001,
    });
    expect(result.maxBindings).toBeLessThanOrEqual(100);
    expect(result.maxSqlBytes).toBeLessThanOrEqual(100_000);
    expect(result.maxBindingBytes).toBeLessThanOrEqual(2_000_000);
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

  it.each([
    false,
    true,
  ])('rolls back cross-account inventory ID collisions (concurrent=%s)', async (concurrent) => {
    const result = await probe<{
      operationId: string;
      nextOperationId: string;
      winnerAccount: string;
      loserAccount: string;
      attempts: Array<
        | { status: 'fulfilled'; run: FleetInventoryRunRecord }
        | { status: 'rejected'; error: ProbeError }
      >;
      before: InventorySnapshot;
      afterCollision: InventorySnapshot;
      refused: ProbeError | null;
      replay: FleetInventoryRunRecord;
      busy: ProbeError | null;
      afterRefusals: InventorySnapshot;
      next: FleetInventoryRunRecord | null;
      nextError: ProbeError | null;
      afterNext: InventorySnapshot;
    }>('inventory-cross-account-start', { concurrent });
    expect(result.before.heads).toEqual(
      ['account-inventory', 'account-inventory-other'].map((account_id) => ({
        account_id,
        active_operation_id: null,
        latest_finalized_generation: 1,
        next_generation: 2,
      })),
    );
    expect(result.afterCollision.heads).toEqual(
      result.before.heads.map((head) =>
        head.account_id === result.winnerAccount
          ? {
              ...head,
              active_operation_id: result.operationId,
              next_generation: 3,
            }
          : head,
      ),
    );
    expect(
      result.afterCollision.runs.filter(
        (run) => run.operation_id !== result.operationId,
      ),
    ).toEqual(result.before.runs);
    expect(
      result.afterCollision.runs.filter(
        (run) => run.operation_id === result.operationId,
      ),
    ).toEqual([
      {
        account_id: result.winnerAccount,
        operation_id: result.operationId,
        generation: 2,
        options_digest: result.replay.optionsDigest,
        run_record: JSON.stringify(result.replay),
        created_at_ms: expect.any(Number),
        finalized_at_ms: null,
      },
    ]);
    expect(result.afterCollision.rows).toEqual(result.before.rows);
    expect(result.afterCollision.facts).toEqual(result.before.facts);
    expect(result.afterRefusals).toEqual(result.afterCollision);
    expect(result.next).toMatchObject({
      operationId: result.nextOperationId,
      state: 'staging',
      progress: { generation: 2, revision: 0 },
    });
    expect(result.afterNext.heads).toEqual(
      result.afterCollision.heads.map((head) =>
        head.account_id === result.loserAccount
          ? {
              ...head,
              active_operation_id: result.nextOperationId,
              next_generation: 3,
            }
          : head,
      ),
    );
    expect(
      result.afterNext.runs.filter(
        (run) => run.operation_id !== result.nextOperationId,
      ),
    ).toEqual(result.afterCollision.runs);
    expect(
      result.afterNext.runs.filter(
        (run) => run.operation_id === result.nextOperationId,
      ),
    ).toEqual([
      {
        account_id: result.loserAccount,
        operation_id: result.nextOperationId,
        generation: 2,
        options_digest: result.replay.optionsDigest,
        run_record: JSON.stringify(result.next),
        created_at_ms: expect.any(Number),
        finalized_at_ms: null,
      },
    ]);
    expect(result.afterNext.rows).toEqual(result.before.rows);
    expect(result.afterNext.facts).toEqual(result.before.facts);
    expect(
      result.attempts.filter((entry) => entry.status === 'fulfilled'),
    ).toEqual([{ status: 'fulfilled', run: result.replay }]);
    const uniqueError = {
      message: expect.stringContaining(
        'UNIQUE constraint failed: anchorage_fleet_inventory_runs.operation_id',
      ),
    };
    expect(
      result.attempts.filter((entry) => entry.status === 'rejected'),
    ).toEqual([
      { status: 'rejected', error: expect.objectContaining(uniqueError) },
    ]);
    expect(result.refused).toMatchObject(uniqueError);
    expect(result.busy).toEqual({
      name: 'Error',
      message: `fleet inventory for account '${result.winnerAccount}' has an active operation other than '${result.nextOperationId}'`,
    });
    expect(result.nextError).toBeNull();
  });

  it.each([
    'rows',
    'both',
    'empty',
  ] as const)('inventory pin availability handles interrupted %s reclamation and cleanup recovery', async (mode) => {
    const result = await probe<{
      before: InventorySnapshot;
      interrupted: ProbeError | null;
      partial: InventorySnapshot;
      pin: ProbeError | null;
      afterPin: InventorySnapshot;
      pins: Array<{
        account_id: string;
        generation: number;
        pinned_by: string;
      }>;
      read: {
        generation: { rows: unknown[]; facts: unknown[] } | null;
        error: ProbeError | null;
      };
      retried: { deleted: number };
      afterRetry: InventorySnapshot;
    }>('inventory-partial-prune', { mode });
    expect(result.before.runs).toHaveLength(2);
    expect(
      result.before.rows.filter((row) => row.generation === 1),
    ).toHaveLength(mode === 'empty' ? 0 : 3);
    expect(
      result.before.facts.filter((row) => row.generation === 1),
    ).toHaveLength(mode === 'empty' ? 0 : 1);
    expect(result.partial).toEqual({
      ...result.before,
      rows: result.before.rows.filter((row) => row.generation !== 1),
      facts:
        mode === 'rows'
          ? result.before.facts
          : result.before.facts.filter((row) => row.generation !== 1),
    });
    expect(result.afterPin).toEqual(result.partial);
    expect(result.interrupted).toEqual({
      name: 'Error',
      message:
        "fleet inventory for account 'account-inventory' lease is no longer owned by this operation",
    });
    if (mode === 'empty') {
      expect(result.pin).toBeNull();
      expect(result.pins).toEqual([
        { account_id: 'account-inventory', generation: 1, pinned_by: 'reader' },
      ]);
      expect(result.read.generation?.rows).toEqual([]);
      expect(result.read.generation?.facts).toEqual([]);
      expect(result.read.error).toBeNull();
    } else {
      expect(result.pins).toEqual([]);
      expect(result.pin).toEqual({
        name: 'Error',
        message: 'fleet inventory generation 1 is corrupt',
      });
      expect(result.read.generation).toBeNull();
    }
    expect(result.retried).toEqual({ deleted: 1 });
    expect(result.afterRetry).toEqual({
      ...result.before,
      runs: result.before.runs.filter((row) => row.generation !== 1),
      rows: result.before.rows.filter((row) => row.generation !== 1),
      facts: result.before.facts.filter((row) => row.generation !== 1),
    });
  });

  it('inventory finalization requires a dense manifest and accepts gap repair', async () => {
    const result = await probe<{
      operationId: string;
      before: InventorySnapshot;
      afterRefusal: InventorySnapshot;
      refused: ProbeError | null;
      final: { rows: Array<{ ordinal: number }>; facts: unknown[] };
    }>('inventory-dense-finalization');
    expect(result.afterRefusal).toEqual(result.before);
    expect(result.refused).toEqual({
      name: 'Error',
      message: `fleet inventory run '${result.operationId}' does not match its finalize manifest`,
    });
    expect(result.final.rows.map((row) => row.ordinal)).toEqual([0, 1, 2]);
    expect(result.final.facts).toEqual([]);
  });

  it.each([
    'pin',
    'prune',
  ] as const)('inventory pin admission preserves the %s winner without orphan protection', async (winner) => {
    const result = await probe<{
      before: InventorySnapshot;
      after: InventorySnapshot;
      pins: Array<{
        account_id: string;
        generation: number;
        pinned_by: string;
      }>;
      pinOutcome: { ok: boolean; error: ProbeError | null };
      pruneOutcome: { deleted: number | null; error: ProbeError | null };
      read: {
        generation: {
          ref: { generation: number };
          rows: Array<{ kind: string; ordinal: number; payload: unknown }>;
          facts: unknown[];
        } | null;
        error: ProbeError | null;
      };
    }>('inventory-pin-prune-race', { winner });
    expect(result.before.runs).toHaveLength(2);
    expect(result.before.rows).toHaveLength(6);
    expect(result.before.facts).toHaveLength(2);
    if (winner === 'prune') {
      expect(result.after).toEqual({
        ...result.before,
        runs: result.before.runs.filter((row) => row.generation !== 1),
        rows: result.before.rows.filter((row) => row.generation !== 1),
        facts: result.before.facts.filter((row) => row.generation !== 1),
      });
      expect(result.pins).toEqual([]);
      expect(result.pinOutcome).toEqual({
        ok: false,
        error: {
          name: 'Error',
          message: 'fleet inventory generation 1 is not finalized',
        },
      });
      expect(result.pruneOutcome).toEqual({ deleted: 1, error: null });
      expect(result.read.generation).toBeNull();
      expect(result.read.error).not.toBeNull();
    } else {
      expect(result.after).toEqual(result.before);
      expect(result.pins).toEqual([
        {
          account_id: 'account-inventory',
          generation: 1,
          pinned_by: 'race-reader',
        },
      ]);
      expect(result.pinOutcome).toEqual({ ok: true, error: null });
      expect(result.pruneOutcome).toEqual({ deleted: 0, error: null });
      expect(result.read.error).toBeNull();
      expect(result.read.generation?.ref.generation).toBe(1);
      expect(result.read.generation?.rows).toEqual(
        result.before.rows
          .filter((row) => row.generation === 1)
          .map((row) => ({
            kind: row.kind,
            ordinal: row.ordinal,
            payload: JSON.parse(row.payload),
          })),
      );
      expect(result.read.generation?.facts).toHaveLength(1);
    }
  });

  it('inventory pruning rechecks a late active owner before deleting rows, facts or the run', async () => {
    const result = await probe<{
      operationId: string;
      before: InventorySnapshot;
      afterPromotion: InventorySnapshot;
      pruned: { deleted: number };
      afterPrune: InventorySnapshot;
    }>('inventory-prune-active-race');
    expect(result.before.rows).toHaveLength(3);
    expect(result.before.facts).toHaveLength(1);
    expect(result.before.runs).toHaveLength(1);
    expect(JSON.parse(result.before.runs[0]?.run_record ?? 'null').state).toBe(
      'failed',
    );
    expect(result.before.heads).toEqual([
      {
        account_id: 'account-inventory',
        active_operation_id: null,
        latest_finalized_generation: null,
        next_generation: 2,
      },
    ]);
    expect(result.afterPromotion).toEqual({
      ...result.before,
      heads: [
        { ...result.before.heads[0], active_operation_id: result.operationId },
      ],
    });
    expect(result.afterPrune).toEqual(result.afterPromotion);
    expect(result.pruned).toEqual({ deleted: 0 });
  });

  it('repairs an interrupted inventory failure under a fresh lease without clearing a newer head', async () => {
    const result = await probe<{
      staged: FleetInventoryRunRecord;
      nextOperationId: string;
      before: InventorySnapshot;
      interrupted: ProbeError | null;
      afterInterrupted: InventorySnapshot;
      pruningBeforeRepair: { deleted: number };
      afterPruningBeforeRepair: InventorySnapshot;
      wrongRevision: ProbeError | null;
      afterWrongRevision: InventorySnapshot;
      recoveryError: ProbeError | null;
      afterRecovery: InventorySnapshot;
      leaseOwners: Array<{ owner_token: string; expires_at: number }>;
      now: number;
      next: FleetInventoryRunRecord | null;
      nextError: ProbeError | null;
      beforeNewHeadReplay: InventorySnapshot;
      newHeadReplayError: ProbeError | null;
      afterNewHeadReplay: InventorySnapshot;
      prunedInactive: { deleted: number };
      afterInactivePrune: InventorySnapshot;
    }>('inventory-failure-recovery');
    expect(result.staged).toMatchObject({
      state: 'staging',
      progress: { generation: 2, revision: 1 },
    });
    expect(result.before.heads).toEqual([
      {
        account_id: 'account-inventory',
        active_operation_id: result.staged.operationId,
        latest_finalized_generation: 1,
        next_generation: 3,
      },
    ]);
    expect(result.afterInterrupted).toEqual({
      ...result.before,
      runs: result.before.runs.map((run) =>
        run.operation_id === result.staged.operationId
          ? {
              ...run,
              run_record: JSON.stringify({ ...result.staged, state: 'failed' }),
            }
          : run,
      ),
    });
    expect(result.afterPruningBeforeRepair).toEqual(result.afterInterrupted);
    expect(result.pruningBeforeRepair).toEqual({ deleted: 0 });
    expect(result.afterWrongRevision).toEqual(result.afterInterrupted);
    expect(result.afterRecovery).toEqual({
      ...result.afterInterrupted,
      heads: result.before.heads.map((head) => ({
        ...head,
        active_operation_id: null,
      })),
    });
    expect(result.next).toMatchObject({
      operationId: result.nextOperationId,
      state: 'staging',
      progress: { generation: 3, revision: 0 },
    });
    expect(result.beforeNewHeadReplay.heads).toEqual([
      {
        account_id: 'account-inventory',
        active_operation_id: result.nextOperationId,
        latest_finalized_generation: 1,
        next_generation: 4,
      },
    ]);
    expect(
      result.beforeNewHeadReplay.runs.filter(
        (run) => run.operation_id !== result.nextOperationId,
      ),
    ).toEqual(result.afterRecovery.runs);
    expect(
      result.beforeNewHeadReplay.runs.filter(
        (run) => run.operation_id === result.nextOperationId,
      ),
    ).toEqual([
      {
        account_id: 'account-inventory',
        operation_id: result.nextOperationId,
        generation: 3,
        options_digest: result.staged.optionsDigest,
        run_record: JSON.stringify(result.next),
        created_at_ms: expect.any(Number),
        finalized_at_ms: null,
      },
    ]);
    expect(result.beforeNewHeadReplay.rows).toEqual(result.before.rows);
    expect(result.beforeNewHeadReplay.facts).toEqual(result.before.facts);
    expect(result.afterNewHeadReplay).toEqual(result.beforeNewHeadReplay);
    expect(result.afterInactivePrune).toEqual({
      ...result.afterNewHeadReplay,
      runs: result.afterNewHeadReplay.runs.filter(
        (row) => row.generation !== 2,
      ),
      rows: result.afterNewHeadReplay.rows.filter(
        (row) => row.generation !== 2,
      ),
      facts: result.afterNewHeadReplay.facts.filter(
        (row) => row.generation !== 2,
      ),
    });
    expect(result.prunedInactive).toEqual({ deleted: 1 });
    expect(result.leaseOwners).toHaveLength(2);
    const [expired, fresh] = result.leaseOwners;
    expect(expired?.owner_token).toEqual(expect.any(String));
    expect(fresh?.owner_token).toEqual(expect.any(String));
    expect(fresh?.owner_token).not.toBe(expired?.owner_token);
    expect(result.now).toBe(1_060_001);
    expect(expired?.expires_at).toBeLessThan(result.now);
    expect(fresh?.expires_at).toBeGreaterThan(result.now);
    expect(result.interrupted).toEqual({
      name: 'Error',
      message:
        "fleet inventory for account 'account-inventory' lease is no longer owned by this operation",
    });
    expect(result.wrongRevision).toEqual({
      name: 'Error',
      message: `fleet inventory run '${result.staged.operationId}' is no longer at the expected revision`,
    });
    expect(result.recoveryError).toBeNull();
    expect(result.nextError).toBeNull();
    expect(result.newHeadReplayError).toBeNull();
  });

  it.each([
    'normal',
    'stale',
    'fallback',
  ] as const)('repairs %s finalized inventory continuation without provider replay or weakening historical pins', async (mode) => {
    const result = await probe<{
      staged: FleetInventoryRunRecord;
      before: InventorySnapshot;
      interrupted: ProbeError | null;
      afterInterrupted: InventorySnapshot;
      pruningBeforeRepair: { deleted: number };
      afterPruningBeforeRepair: InventorySnapshot;
      recovery: {
        result: FleetInventoryAdvanceResult | null;
        error: ProbeError | null;
      };
      afterRecovery: InventorySnapshot;
      recoveryTrace: string[];
      recoveryRepairs: unknown[];
      providerCalls: number;
      historical: null | {
        newer: FleetInventoryRunRecord;
        before: InventorySnapshot;
        unpinned: ProbeError | null;
        afterUnpinned: InventorySnapshot;
        pinned: FleetInventoryAdvanceResult;
        afterPinned: InventorySnapshot;
        released: ProbeError | null;
        afterReleased: InventorySnapshot;
      };
    }>('inventory-finalized-continuation-recovery', { mode });
    expect(result.staged).toMatchObject({
      state: 'staging',
      progress: { generation: 1, revision: 1, stage: { step: 'finalize' } },
    });
    expect(result.before.heads).toEqual([
      {
        account_id: 'account-inventory',
        active_operation_id: result.staged.operationId,
        latest_finalized_generation: null,
        next_generation: 2,
      },
    ]);
    expect(result.afterInterrupted).toEqual({
      ...result.before,
      runs: result.before.runs.map((run) => ({
        ...run,
        run_record: JSON.stringify({ ...result.staged, state: 'finalized' }),
        finalized_at_ms: 1_000_000,
      })),
    });
    expect(result.afterPruningBeforeRepair).toEqual(result.afterInterrupted);
    expect(result.pruningBeforeRepair).toEqual({ deleted: 0 });
    expect(result.afterRecovery).toEqual({
      ...result.afterInterrupted,
      heads: result.before.heads.map((head) => ({
        ...head,
        active_operation_id: null,
        latest_finalized_generation: 1,
      })),
    });
    expect(result.recovery.result).toEqual({
      status: 'complete',
      token: {
        version: 1,
        operationId: result.staged.operationId,
        revision: 1,
      },
      generation: {
        generation: 1,
        operationId: result.staged.operationId,
        finalizedAtMs: 1_000_000,
        rowManifest: result.staged.progress.stagedCounts,
        factCount: result.staged.progress.factCount,
      },
    });
    expect(result.recoveryTrace).toEqual([
      'lease-read',
      ...(mode === 'fallback' ? ['fallback-read'] : []),
      'finalize',
      'generation-read',
    ]);
    expect(result.recoveryRepairs).toEqual([
      {
        operationId: result.staged.operationId,
        expectedRevision: 1,
        manifest: result.staged.progress.stagedCounts,
        factCount: result.staged.progress.factCount,
      },
    ]);
    expect(result.historical).not.toBeNull();
    if (!result.historical)
      throw new Error('historical continuation probe missing');
    expect(result.historical.newer).toMatchObject({
      state: 'staging',
      progress: { generation: 3 },
    });
    expect(result.historical.before.heads).toEqual([
      {
        account_id: 'account-inventory',
        active_operation_id: result.historical.newer.operationId,
        latest_finalized_generation: 2,
        next_generation: 4,
      },
    ]);
    expect(result.historical.afterUnpinned).toEqual(result.historical.before);
    expect(result.historical.afterPinned).toEqual(result.historical.before);
    expect(result.historical.afterReleased).toEqual(result.historical.before);
    expect(result.historical.pinned).toEqual(result.recovery.result);
    expect(result.providerCalls).toBe(0);
    expect(result.interrupted).toEqual({
      name: 'Error',
      message:
        "fleet inventory for account 'account-inventory' lease is no longer owned by this operation",
    });
    expect(result.recovery.error).toBeNull();
    const requiresPin = {
      name: 'Error',
      message:
        'fleet inventory generation 1 requires a pin before it can be read',
    };
    expect(result.historical.unpinned).toEqual(requiresPin);
    expect(result.historical.released).toEqual(requiresPin);
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
    expect(result.rowCounts).toEqual([
      { kind: 'deployment', count: 1 },
      { kind: 'finding', count: 1 },
      { kind: 'meta', count: 2 },
      { kind: 'registration', count: 1 },
    ]);
  });

  describe.each([
    'ordinary',
    'hidden',
  ] as const)('inventory chunk with %s results', (delivery) => {
    it.each([
      'account',
      'generation',
      'options',
    ] as const)('rejects a different %s before durable mutation', async (fault) => {
      const result = await probe<InventoryCommitRefusalProbe>(
        'inventory-commit-refusal',
        { fault, hideResults: delivery === 'hidden' },
      );
      expectInventoryRefusalAndReplay(result);
      if (fault === 'generation') {
        expect(result.attempted.runRecord.progress.generation).toBe(99);
      } else if (fault === 'options') {
        expect(result.attempted.runRecord.options).toEqual({
          ...result.prior.options,
          scriptNamePrefix: 'alternate',
        });
        expect(result.attempted.runRecord.optionsDigest).toBe(
          fleetInventoryOptionsDigest(result.attempted.runRecord.options),
        );
        expect(result.attempted.runRecord.optionsDigest).not.toBe(
          result.before.runs[0]?.options_digest,
        );
      }
      expect(result.refused).toEqual({
        name: 'Error',
        message:
          fault === 'account'
            ? `no fleet inventory run for operation '${result.prior.operationId}'`
            : `fleet inventory run '${result.prior.operationId}' is no longer at the expected revision`,
      });
    });

    it.each([
      'row',
      'fact',
    ] as const)('rolls back earlier siblings on an immutable %s conflict', async (kind) => {
      const result = await probe<InventoryCommitRefusalProbe>(
        'inventory-commit-refusal',
        { fault: `${kind}-conflict`, hideResults: delivery === 'hidden' },
      );
      expectInventoryRefusalAndReplay(result);
      expect(result.attempted.rows[0]).toEqual({
        kind: 'meta',
        ordinal: 0,
        payload: { marker: 'earlier-sibling' },
      });
      if (kind === 'row') {
        expect(result.attempted.rows[1]).toEqual({
          kind: 'registration',
          ordinal: 0,
          payload: { scriptName: 'different' },
        });
      } else {
        expect(result.attempted.facts).toEqual([
          {
            deploymentOrdinal: 0,
            factKind: 'secret-name',
            factOrdinal: 1,
            payload: { name: 'ANCHORAGE_NAME_1' },
          },
          {
            deploymentOrdinal: 0,
            factKind: 'secret-name',
            factOrdinal: 0,
            payload: { name: 'DIFFERENT_NAME' },
          },
        ]);
      }
      expect(result.refused).toMatchObject({
        name: 'Error',
        message: expect.stringContaining(
          `UNIQUE constraint failed: anchorage_fleet_inventory_${kind === 'row' ? 'rows' : 'deployment_facts'}.account_id`,
        ),
      });
    });

    it.each([
      { fault: 'duplicate-row', duplicatePayload: 'same' },
      { fault: 'duplicate-row', duplicatePayload: 'different' },
      { fault: 'duplicate-fact', duplicatePayload: 'same' },
      { fault: 'duplicate-fact', duplicatePayload: 'different' },
    ] as const)('rejects $fault keys with $duplicatePayload payloads without effects', async ({
      fault,
      duplicatePayload,
    }) => {
      const result = await probe<InventoryCommitRefusalProbe>(
        'inventory-commit-refusal',
        { fault, duplicatePayload, hideResults: delivery === 'hidden' },
      );
      expectInventoryRefusalAndReplay(result);
      const duplicates =
        fault === 'duplicate-row'
          ? result.attempted.rows
          : result.attempted.facts;
      expect(duplicates).toHaveLength(2);
      expect(duplicates[1]).toEqual({
        ...duplicates[0],
        payload:
          duplicatePayload === 'same'
            ? duplicates[0]?.payload
            : fault === 'duplicate-row'
              ? { marker: 'different' }
              : { name: 'DIFFERENT_NAME' },
      });
      expect(result.refused).toEqual({
        name: 'FleetInventoryStateError',
        message: 'fleet inventory state is malformed',
      });
    });

    it.each([
      'failed',
      'stage',
      'provider-requests',
      'updated-at',
    ] as const)('refuses same-revision %s convergence after an exact replay', async (change) => {
      const result = await probe<{
        intended: FleetInventoryRunRecord;
        attempted: FleetInventoryRunRecord;
        accepted: FleetInventoryRunRecord;
        afterAcceptance: InventorySnapshot;
        replay: FleetInventoryRunRecord;
        afterReplay: InventorySnapshot;
        beforeRefusal: InventorySnapshot;
        afterRefusal: InventorySnapshot;
        refused: ProbeError | null;
      }>('inventory-commit-replay', {
        change,
        hideResults: delivery === 'hidden',
      });
      expectInventorySeed(result.afterAcceptance, result.intended);
      expect(result.afterReplay).toEqual(result.afterAcceptance);
      expect(result.beforeRefusal).toEqual({
        ...result.afterReplay,
        heads:
          change === 'failed'
            ? [
                {
                  ...result.afterReplay.heads[0],
                  active_operation_id: null,
                },
              ]
            : result.afterReplay.heads,
        runs: [
          {
            ...result.afterReplay.runs[0],
            run_record: JSON.stringify({
              ...result.intended,
              state: change === 'failed' ? 'failed' : 'staging',
            }),
          },
        ],
      });
      expect(result.afterRefusal).toEqual(result.beforeRefusal);
      expect(result.accepted).toEqual(result.intended);
      expect(result.replay).toEqual(result.intended);
      expect(result.attempted.progress.revision).toBe(1);
      if (change === 'failed') {
        expect(result.attempted).toEqual(result.intended);
      } else {
        expect(result.attempted).not.toEqual(result.intended);
      }
      expect(result.refused).toEqual({
        name: 'Error',
        message: `fleet inventory run '${result.intended.operationId}' is no longer at the expected revision`,
      });
    });
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

  it('commitProgress watermark conjuncts (an unsatisfiable claim lands no row and does not advance the revision; a dense prefix commits)', async () => {
    const result = await probe<
      OperationCommitProbe & {
        beforeFindingRefusal: OperationSnapshot;
        afterFindingRefusal: OperationSnapshot;
        findingRefused: ProbeError | null;
      }
    >('operation-commit-watermark');
    expect(result.beforeFindingRefusal.revision).toBe(0);
    expect(result.beforeFindingRefusal.rows).toEqual([]);
    expect(result.afterFindingRefusal).toEqual(result.beforeFindingRefusal);
    expect(result.before.revision).toBe(0);
    expect(result.before.rows).toEqual([
      { row_kind: 'fact', ordinal: 1, payload: expect.any(String) },
      { row_kind: 'finding', ordinal: 0, payload: expect.any(String) },
    ]);
    expect(result.afterRefusal).toEqual(result.before);
    const conflict = {
      name: 'Error',
      message:
        "fleet operation '123e4567-e89b-42d3-a456-426614174305' is no longer at the expected revision",
    };
    expect(result.findingRefused).toEqual(conflict);
    expect(result.refused).toEqual(conflict);
    expect(result.accepted.progress.revision).toBe(1);
    expect(result.afterAcceptance.revision).toBe(1);
    expect(result.afterAcceptance.rows).toEqual([
      { row_kind: 'fact', ordinal: 0, payload: expect.any(String) },
      result.before.rows[0],
      { row_kind: 'fact', ordinal: 2, payload: expect.any(String) },
      result.before.rows[1],
      { row_kind: 'finding', ordinal: 1, payload: expect.any(String) },
      { row_kind: 'finding', ordinal: 2, payload: expect.any(String) },
    ]);
    expect(result.replay).toEqual(result.accepted);
    expect(result.afterReplay).toEqual(result.afterAcceptance);
  });

  it('commitProgress row-UPDATE dense prefix (updates ordinal 1 while preserving ordinal 0 under an item watermark of 2)', async () => {
    const result = await probe<{
      before: OperationSnapshot;
      afterAcceptance: OperationSnapshot;
      acceptedRevision: number;
      items: Array<{
        rowKind: string;
        ordinal: number;
        payload: Record<string, unknown>;
      }>;
    }>('operation-commit-row-update');
    expect(result.before.revision).toBe(0);
    expect(result.before.rows).toEqual([
      { row_kind: 'item', ordinal: 0, payload: expect.any(String) },
      { row_kind: 'item', ordinal: 1, payload: expect.any(String) },
    ]);
    expect(result.acceptedRevision).toBe(1);
    expect(result.afterAcceptance.revision).toBe(1);
    expect(result.afterAcceptance.rows).toEqual([
      result.before.rows[0],
      { row_kind: 'item', ordinal: 1, payload: expect.any(String) },
    ]);
    expect(result.items).toEqual([
      {
        rowKind: 'item',
        ordinal: 0,
        payload: {
          ordinal: 0,
          tenantTag: 'tenant',
          environment: 'production',
          entryRecordDigest: 'c'.repeat(64),
          status: 'pending',
        },
      },
      {
        rowKind: 'item',
        ordinal: 1,
        payload: {
          ordinal: 1,
          tenantTag: 'tenant',
          environment: 'production',
          entryRecordDigest: 'c'.repeat(64),
          targetSpecDigest: 'd'.repeat(64),
          plan: [{ step: 'promote' }],
          planCursor: 0,
          status: 'active',
        },
      },
    ]);
  });

  it.each([
    { delivery: 'ordinary', viaStage: false },
    { delivery: 'hidden', viaStage: false },
    { delivery: 'ordinary', viaStage: true },
    { delivery: 'hidden', viaStage: true },
  ] as const)('commitProgress immutable conflict rolls back an earlier insert and accepts exact staged bytes with $delivery results (stageRows $viaStage)', async ({
    delivery,
    viaStage,
  }) => {
    const result = await probe<OperationCommitProbe>(
      'operation-commit-immutable-conflict',
      { hideResults: delivery === 'hidden', viaStage },
    );
    expect(result.before.revision).toBe(0);
    expect(result.before.rows).toEqual([
      {
        row_kind: 'finding',
        ordinal: 1,
        payload: JSON.stringify({
          tenantTag: 'tenant',
          environment: 'production',
          kind: 'audit-error',
          detail: 'safe finding 1',
        }),
      },
    ]);
    expect(result.afterRefusal).toEqual(result.before);
    expect(result.refused).toMatchObject({
      message: expect.stringContaining('UNIQUE constraint failed'),
    });
    expect(result.accepted.progress.revision).toBe(1);
    expect(result.afterAcceptance.revision).toBe(1);
    expect(result.afterAcceptance.rows).toEqual([
      {
        row_kind: 'finding',
        ordinal: 0,
        payload: JSON.stringify({
          tenantTag: 'tenant',
          environment: 'production',
          kind: 'audit-error',
          detail: 'safe finding 0',
        }),
      },
      result.before.rows[0],
    ]);
    expect(result.accepted).toEqual(
      JSON.parse(result.afterAcceptance.record ?? 'null'),
    );
    expect(result.replay).toEqual(result.accepted);
    expect(result.afterReplay).toEqual(result.afterAcceptance);
  });

  it.each([
    'ordinary',
    'hidden',
  ] as const)('commitProgress missing update target preserves the batch and permits repaired update/insert replay with %s results', async (delivery) => {
    const result = await probe<OperationCommitProbe>(
      'operation-commit-missing-update',
      { hideResults: delivery === 'hidden' },
    );
    expect(result.before.revision).toBe(0);
    expect(result.before.rows).toEqual([
      {
        row_kind: 'item',
        ordinal: 0,
        payload: JSON.stringify({
          ordinal: 0,
          tenantTag: 'tenant',
          environment: 'production',
          entryRecordDigest: 'c'.repeat(64),
          status: 'pending',
        }),
      },
    ]);
    expect(result.afterRefusal).toEqual(result.before);
    expect(result.refused).toEqual({
      name: 'Error',
      message:
        "fleet operation '123e4567-e89b-42d3-a456-426614174308' is no longer at the expected revision",
    });
    expect(result.accepted.progress.revision).toBe(1);
    expect(result.afterAcceptance.revision).toBe(1);
    expect(result.afterAcceptance.rows).toEqual([
      ...[0, 1].map((ordinal) => ({
        row_kind: 'item',
        ordinal,
        payload: JSON.stringify({
          ordinal,
          tenantTag: 'tenant',
          environment: 'production',
          entryRecordDigest: 'c'.repeat(64),
          targetSpecDigest: 'd'.repeat(64),
          plan: [{ step: 'promote' }],
          planCursor: 0,
          status: 'active',
        }),
      })),
      {
        row_kind: 'item',
        ordinal: 2,
        payload: JSON.stringify({
          ordinal: 2,
          tenantTag: 'tenant',
          environment: 'production',
          entryRecordDigest: 'c'.repeat(64),
          status: 'pending',
        }),
      },
    ]);
    expect(result.accepted).toEqual(
      JSON.parse(result.afterAcceptance.record ?? 'null'),
    );
    expect(result.replay).toEqual(result.accepted);
    expect(result.afterReplay).toEqual(result.afterAcceptance);
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
