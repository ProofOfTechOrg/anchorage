// SPDX-License-Identifier: Apache-2.0

import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import {
  EXECUTION_FENCE_ROW_ID,
  EXECUTION_FENCE_TABLE,
} from '@proofoftech/flowsafe/deployment-identity-protocol';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  createDirectInvocationClient,
  type DirectInvocationClient,
  DirectInvocationError,
} from '../scripts/direct-credentialed-invocation.mjs';
import { observeDirectWorkerVersion } from '../scripts/direct-credentialed-observations.mjs';
import {
  type DirectRunJournal,
  isAbandonedDirectScenario,
  openDirectRunState,
} from '../scripts/direct-credentialed-run-state.mjs';
import {
  type DirectScenarioState,
  runDirectCredentialedScenario,
} from '../scripts/direct-credentialed-scenario.mjs';
import { DIRECT_SCENARIO_MIN_INVOCATIONS } from '../scripts/direct-credentialed-scenario-budget.mjs';
import {
  hash,
  jsonHash,
  recordFacts,
} from '../scripts/direct-credentialed-scenario-checks.mjs';
import { directDeploymentSpec } from '../scripts/direct-credentialed-spec.js';
import type { DirectReferenceAction } from '../scripts/direct-reference-contract.mjs';
import { DIRECT_REFERENCE_LEASE } from '../scripts/direct-reference-transport.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import { directObservationFixture } from './fixtures/direct-observations.js';
import {
  type MutableScenario,
  PROCESS,
  present,
  RESUMED,
  scenarioJournal,
  scenarioWith,
} from './fixtures/direct-run-state-builder.js';
import {
  childResumeDirectScenario as childResume,
  directScenarioCleanup as cleanup,
  closeDirectScenarioFixtures,
  type DirectScenarioNodeResponse,
  createDirectScenarioFixture as fixture,
  resumeDirectScenarioJournal as resume,
} from './fixtures/direct-scenario-harness.js';

afterEach(closeDirectScenarioFixtures);

type FenceMutationAfter = {
  state: string;
  mutationEpoch: number;
  requireMutationEpoch: boolean;
  transitionRevision: number;
};

/**
 * One armed interception of the reference Worker's own fence answer. The first
 * `tenant-fence` reply whose new state matches `operation` is replaced by the
 * CAS conflict `reason` describes; every later reply passes through, so the
 * case pins what the run does with a single conflict rather than a stream.
 */
function fenceConflict(
  operation: 'drain' | 'reopen',
  reason: (after: FenceMutationAfter) => Record<string, unknown>,
  onConflict?: () => void,
) {
  let armed = true;
  const nodeResponse: DirectScenarioNodeResponse = async (
    _request,
    response,
  ) => {
    if (
      !armed ||
      !response.headers.get('content-type')?.includes('application/json')
    )
      return response;
    const value = (await response.clone().json()) as {
      action?: string;
      result?: { after?: FenceMutationAfter };
    };
    const after = value.result?.after;
    if (
      value.action !== 'tenant-fence' ||
      after?.state !== (operation === 'drain' ? 'draining' : 'open')
    )
      return response;
    armed = false;
    onConflict?.();
    return Response.json(
      {
        ...value,
        result: {
          ok: false,
          reason: {
            code: 'FENCE_CAS_CONFLICT',
            ...reason(after),
            conflict: 'expectation-mismatch',
          },
        },
      },
      { status: response.status, headers: response.headers },
    );
  };
  return { nodeResponse, fired: () => !armed };
}

describe.sequential('scenario proof failures in native reference state', {
  timeout: 660_000,
}, () => {
  it('reads uploaded runtime limits through the native fixture and rejects settings drift', async () => {
    const f = await fixture();
    const input = f.input();
    await input.invocation.invoke({
      kind: 'provision',
      role: 'a',
      release: 'initial',
    });
    const control = (await input.invocation.invoke({ kind: 'control-read' }))
      .result as {
      records: {
        role: string;
        artifactVersion: string;
        databaseId: string;
        desiredSpecDigest: string;
      }[];
    };
    const record = control.records.find(
      (candidateRecord) => candidateRecord.role === 'a',
    );
    if (!record) throw new Error('native fixture record is missing');
    const expected = {
      ...input,
      role: 'a' as const,
      applicationRelease: '1' as const,
      versionId: record.artifactVersion,
      databaseId: record.databaseId,
      specDigest: record.desiredSpecDigest,
    };
    expect(await observeDirectWorkerVersion(expected)).toMatchObject({
      cpuLimitMs: 50,
      subrequestLimit: 50,
    });
    const upload = f.native.projection.requests.find(
      (request) =>
        request.method === 'PUT' && request.url.includes('/workers/scripts/'),
    );
    const metadata = (
      upload?.body as
        | { metadata: { limits: { subrequests: number } } }
        | undefined
    )?.metadata;
    if (!metadata) throw new Error('native fixture upload metadata is missing');
    metadata.limits.subrequests++;
    await expect(observeDirectWorkerVersion(expected)).rejects.toMatchObject({
      code: 'observation-mismatch',
    });
    expect(f.native.bridgeErrors).toEqual([]);
  });

  it('rejects a changed original witness before any resumed mutation', async () => {
    let corrupt = false;
    const f = await fixture({
      nodeResponse: async (_request, response) => {
        if (
          !corrupt ||
          !response.headers.get('content-type')?.includes('application/json')
        )
          return response;
        const value = (await response.clone().json()) as {
          action?: string;
          result?: { interruption?: string };
        };
        if (value.action !== 'control-read' || !value.result?.interruption)
          return response;
        const witness = JSON.parse(value.result.interruption);
        witness.claimJson = JSON.stringify({
          ...JSON.parse(witness.claimJson),
          operationId: 'foreign-operation',
        });
        value.result.interruption = JSON.stringify(witness);
        return Response.json(value, {
          status: response.status,
          headers: response.headers,
        });
      },
    });
    expect(await runDirectCredentialedScenario(f.input())).toEqual({
      status: 'restart-required',
    });
    const mutations = [...f.native.world.mutationLog];
    corrupt = true;
    const child = await childResume(f);
    expect(child.result).toMatchObject({
      status: 'failed',
      reason: 'observation-mismatch',
      phase: 'migration-restart',
    });
    expect(f.native.world.mutationLog).toEqual(mutations);
  });

  it('hashes export bytes and dispatches no later tenant D1 deletion on mismatch', async () => {
    let corruptions = 0;
    const f = await fixture({
      nodeResponse: async (request, response) => {
        if (
          !request.url.includes('/r2/buckets/') ||
          !request.url.includes('/objects/') ||
          !response.ok
        )
          return response;
        corruptions++;
        const bytes = new Uint8Array(await response.arrayBuffer());
        bytes[0] = (bytes[0] ?? 0) ^ 1;
        return new Response(bytes, { status: 200, headers: response.headers });
      },
    });
    expect(await runDirectCredentialedScenario(f.input())).toEqual({
      status: 'restart-required',
    });
    const databaseId =
      f.local.journal.snapshot().scenario?.proofs.initial.a?.databaseId;
    const child = await childResume(f);
    expect(child.result).toMatchObject({
      status: 'failed',
      reason: 'observation-mismatch',
      phase: 'decommission-a',
    });
    expect(corruptions).toBe(1);
    expect(
      f.native.projection.requests.filter(
        (request) =>
          request.method === 'DELETE' &&
          request.url.endsWith(`/d1/database/${databaseId}`),
      ),
    ).toEqual([]);
    const state = JSON.parse(
      await readFile(join(f.local.journal.directory, 'journal.json'), 'utf8'),
    );
    expect(state.scenario.proofs.exports.a).toBeNull();
    expect(state.scenario.proofs.decommission.a).toBeNull();
    const resumed = await resume(f);
    const count = resumed.snapshot().invocationCount;
    expect(await runDirectCredentialedScenario(f.input(resumed))).toMatchObject(
      { status: 'failed', reason: 'observation-mismatch' },
    );
    expect(resumed.snapshot().invocationCount).toBe(count);
  });

  it('stops on proof fsync failure and safely resumes the settled export boundary', async () => {
    const f = await fixture();
    expect(await runDirectCredentialedScenario(f.input())).toEqual({
      status: 'restart-required',
    });
    const databaseId =
      f.local.journal.snapshot().scenario?.proofs.initial.a?.databaseId;
    const child = await childResume(f, 'export-fsync');
    expect(child.stdout).toContain('SCENARIO_FAULT export-fsync');
    expect(child.result).toMatchObject({
      status: 'failed',
      reason: 'journal-failed',
      phase: 'decommission-a',
    });
    expect(
      f.native.projection.requests.filter(
        (request) =>
          request.method === 'DELETE' &&
          request.url.endsWith(`/d1/database/${databaseId}`),
      ),
    ).toEqual([]);
    const resumed = await resume(f);
    expect(resumed.snapshot().scenario?.proofs.exports.a).toBeNull();
    const result = await runDirectCredentialedScenario(f.input(resumed));
    expect({ result, state: resumed.snapshot().scenario }).toMatchObject({
      result: { status: 'complete' },
    });
    expect(resumed.snapshot().scenario?.proofs.exports.a?.verified).toBe(true);
    expect(resumed.snapshot().invocationCount).toBeLessThanOrEqual(
      f.local.prepared.config.referenceWorker.maxInvocations,
    );
  });
});

describe('scenario journal refusal boundaries', () => {
  it('refuses a configured budget below the declared scenario floor before any invocation', async () => {
    const local = await directObservationFixture(1000, 'confirmed', {
      maxInvocations: DIRECT_SCENARIO_MIN_INVOCATIONS - 1,
    });
    cleanup.push(() => local.close());
    let calls = 0;
    const invocation = {
      async invoke() {
        calls++;
        throw new Error('unexpected invocation');
      },
    };
    const input = {
      prepared: local.prepared,
      journal: local.journal,
      invocation,
      apiToken: 'inert',
    };
    expect(await runDirectCredentialedScenario(input)).toMatchObject({
      status: 'failed',
      reason: 'budget-exhausted',
      detail: 'below-scenario-floor',
      phase: 'provision-a',
      invocationCount: 1,
    });
    expect(calls).toBe(0);
    expect(local.journal.snapshot().scenario?.failure).toMatchObject({
      code: 'budget-exhausted',
    });
    expect(await runDirectCredentialedScenario(input)).toMatchObject({
      status: 'failed',
      reason: 'budget-exhausted',
      detail: 'below-scenario-floor',
    });
    expect(calls).toBe(0);
  });

  it('retains the exhausted original invocation budget and closed scenario fields', async () => {
    const local = await directObservationFixture(1000, 'confirmed', {
      maxInvocations: DIRECT_SCENARIO_MIN_INVOCATIONS,
    });
    cleanup.push(() => local.close());
    while (
      local.journal.snapshot().invocationCount < DIRECT_SCENARIO_MIN_INVOCATIONS
    )
      await local.settle({ kind: 'control-read' });
    let calls = 0;
    const invocation = {
      async invoke() {
        calls++;
        throw new Error('unexpected invocation');
      },
    };
    const outcome = await runDirectCredentialedScenario({
      prepared: local.prepared,
      journal: local.journal,
      invocation,
      apiToken: 'inert',
    });
    expect(outcome).toMatchObject({
      status: 'failed',
      reason: 'invocation-budget-exhausted',
      invocationCount: DIRECT_SCENARIO_MIN_INVOCATIONS,
    });
    expect(calls).toBe(0);
    const state = local.journal.snapshot().scenario;
    if (!state) throw new Error('scenario state is missing');
    for (const bad of [
      { ...state, headers: { authorization: 'private-sentinel' } },
      { ...state, phase: 'complete' },
      {
        ...state,
        lastCall: {
          ordinal: DIRECT_SCENARIO_MIN_INVOCATIONS,
          action: { kind: 'control-read', token: 'opaque-sentinel' },
          outcome: 'returned',
          attempts: { provider: 0, maintenance: 0, application: 0 },
          migration: null,
        },
      },
    ])
      await expect(
        local.journal.recordScenario(bad as typeof state),
      ).rejects.toMatchObject({ code: 'invalid-state' });
    expect(
      await readFile(join(local.journal.directory, 'journal.json'), 'utf8'),
    ).not.toContain('sentinel');
    await local.journal.close();
    const resumed = await openDirectRunState({
      configPath: local.configPath,
      prepared: local.prepared,
      accountId: 'account',
      mode: 'resume',
    });
    cleanup.push(() => resumed.close());
    expect(resumed.snapshot().invocationCount).toBe(
      DIRECT_SCENARIO_MIN_INVOCATIONS,
    );
    expect(resumed.snapshot().bootstrap).toEqual(
      local.journal.snapshot().bootstrap,
    );
    expect(
      await runDirectCredentialedScenario({
        prepared: local.prepared,
        journal: resumed,
        invocation,
        apiToken: 'inert',
      }),
    ).toMatchObject({
      status: 'failed',
      reason: 'invocation-budget-exhausted',
    });
    expect(calls).toBe(0);
  });

  it('refuses a provisioned deployment carrying more versions than the journal holds', async () => {
    const local = await directObservationFixture(1000, 'confirmed', {
      maxInvocations: DIRECT_SCENARIO_MIN_INVOCATIONS,
    });
    cleanup.push(() => local.close());
    const [target] = local.expected;
    if (!target) throw new Error('observation fixture target is missing');
    for (const versionId of ['zero-weight-1', 'zero-weight-2'])
      local.deployment.versions.push({ version_id: versionId, percentage: 0 });
    local.hook(async (request, fallback) => {
      const response = fallback();
      const path = new URL(request.url).pathname;
      if (
        !path.endsWith(`/versions/${target.versionId}`) &&
        !path.endsWith('/settings')
      )
        return response;
      const released = (bindings: unknown) =>
        (bindings as { name: string; text?: string }[]).map((entry) =>
          entry.name === 'APPLICATION_RELEASE' ||
          entry.name === 'FLEET_SCHEMA_VERSION'
            ? { ...entry, text: '1' }
            : entry,
        );
      const body = (await response.json()) as {
        result: {
          bindings?: unknown;
          resources?: { bindings?: unknown };
        };
      };
      if (body.result.resources?.bindings)
        body.result.resources.bindings = released(
          body.result.resources.bindings,
        );
      if (body.result.bindings)
        body.result.bindings = released(body.result.bindings);
      return Response.json(body);
    });
    let provisioned = false;
    const control = () => ({
      binding: {
        version: 1,
        accountId: 'account',
        fleetDatabaseId: 'fleet-id',
        quotaDatabaseId: 'quota-id',
        exportBucketName: local.prepared.names.exportBucket,
        referenceModuleSetSha256: local.prepared.referenceModuleSetSha256,
        accountWorkersDevSubdomain: 'attested-account',
      },
      operations: [],
      records: [
        ...local.expected.map((entry) => ({
          role: entry.role,
          present: provisioned && entry.role === target.role,
          ...(provisioned && entry.role === target.role
            ? {
                phase: 'ready',
                desiredSpecDigest: entry.specDigest,
                pendingSpecDigest: entry.specDigest,
                artifactVersion: entry.versionId,
                pendingArtifactVersion: entry.versionId,
                databaseId: entry.databaseId,
              }
            : {}),
        })),
        { role: 'recovery' as const, present: false },
      ],
      interruption: null,
      forceBefore: null,
      forceAfter: null,
    });
    const invocation: DirectInvocationClient = {
      async invoke(action) {
        const reservation = await local.journal.reserveInvocation(
          JSON.stringify({
            contractVersion: 2,
            configSha256: local.prepared.configSha256,
            action,
          }),
        );
        await local.journal.settleInvocation(reservation);
        const attempts = { provider: 0, maintenance: 0, application: 0 };
        if (action.kind === 'control-read')
          return { result: control(), attempts };
        if (action.kind === 'provision' && action.role === target.role) {
          provisioned = true;
          return { result: { status: 'ready' }, attempts };
        }
        throw new Error(`unexpected fixture action ${action.kind}`);
      },
    };
    expect(
      await runDirectCredentialedScenario({ ...local.input, invocation }),
    ).toMatchObject({
      status: 'failed',
      reason: 'observation-mismatch',
      phase: 'provision-a',
    });
    const state = local.journal.snapshot().scenario;
    expect(state?.failure).toMatchObject({ code: 'observation-mismatch' });
    expect(state?.proofs.initial.a).toBeNull();
  });

  it('retains an unknown pending invocation and refuses readbacks and concurrent mutation', async () => {
    const local = await directObservationFixture();
    cleanup.push(() => local.close());
    let calls = 0;
    const invocation = createDirectInvocationClient({
      prepared: local.prepared,
      journal: local.journal,
      accountWorkersDevSubdomain: 'attested-account',
      invokeSecret: 'inert',
      fetch: async () => {
        calls++;
        throw new Error('unknown synthetic delivery');
      },
    });
    const input = {
      prepared: local.prepared,
      journal: local.journal,
      invocation,
      apiToken: 'inert',
    };
    expect(await runDirectCredentialedScenario(input)).toMatchObject({
      status: 'failed',
      reason: 'outcome-unknown',
    });
    const snapshot = local.journal.snapshot();
    expect(snapshot.lastInvocation?.state).toBe('pending');
    expect(snapshot.invocationCount).toBe(2);
    const firstRunCalls = calls;
    expect(firstRunCalls).toBeGreaterThanOrEqual(1);
    expect(await runDirectCredentialedScenario(input)).toMatchObject({
      status: 'failed',
      reason: 'outcome-unknown',
    });
    expect(local.journal.snapshot()).toEqual(snapshot);
    expect(calls).toBe(firstRunCalls);
  });
});

describe.sequential('fixed Node scenario through native reference dispatch', {
  timeout: 660_000,
}, () => {
  it('LV2 singleton continuation native feasibility', async () => {
    const f = await fixture({
      maxProviderRequests: 400,
      nativeArtifacts: true,
      nativeTenant: true,
    });
    const attempts: Array<{
      action: string;
      provider: number;
      maintenance: number;
      application: number;
      elapsedMs: number;
    }> = [];
    let leaseOutcome = 'not-reached';
    const invoke = async (action: DirectReferenceAction) => {
      const startedAt = performance.now();
      const result = await f.input().invocation.invoke(action);
      attempts.push({
        action:
          action.kind === 'tenant-continuation' ||
          action.kind === 'tenant-fence'
            ? `${action.kind}:${action.operation}`
            : action.kind,
        ...result.attempts,
        elapsedMs: Math.round(performance.now() - startedAt),
      });
      return result.result;
    };
    try {
      await invoke({
        kind: 'provision',
        role: 'a',
        release: 'initial',
        cycle: 'reprovision',
      });
      const tenant = f.tenant;
      const generationA = tenant?.generation;
      if (!tenant || !generationA)
        throw new Error('native tenant generation A is missing');
      const marker = await (await tenant.database())
        .prepare('SELECT marker FROM direct_conformance_fixture WHERE id=1')
        .first<{ marker: string }>();
      expect(marker).toEqual({ marker: 'initial' });

      const control = (await invoke({ kind: 'control-read' })) as {
        records: Array<{
          role: string;
          artifactVersion?: string;
          databaseId?: string;
          desiredSpecDigest?: string;
        }>;
      };
      const recordA = control.records.find(({ role }) => role === 'a');
      if (
        !recordA?.artifactVersion ||
        !recordA.databaseId ||
        !recordA.desiredSpecDigest
      )
        throw new Error('native tenant record A is missing');
      const observationA = await observeDirectWorkerVersion({
        ...f.input(),
        role: 'a',
        applicationRelease: '1',
        versionId: recordA.artifactVersion,
        databaseId: recordA.databaseId,
        specDigest: recordA.desiredSpecDigest,
      });
      expect(observationA.currentDeployment.versions).toEqual([
        { versionId: recordA.artifactVersion, percentage: 100 },
      ]);

      const challenge = 'a'.repeat(64);
      const liveness = await tenant.runnerLiveness(
        f.native.secrets.a.deploymentIdentity,
      );
      expect(liveness.status).toBe(200);
      expect(await liveness.json()).toEqual({ live: false });
      const started = (await invoke({
        kind: 'tenant-continuation',
        operation: 'start',
        challenge,
      })) as {
        status: number;
        summary: {
          runId?: string;
          status?: string;
          approval?: { id?: string };
        };
      };
      expect(started).toMatchObject({
        status: 200,
        summary: { runId: expect.any(String), status: 'suspended' },
      });
      const runId = started.summary.runId;
      const approvalId = started.summary.approval?.id;
      if (!runId || !approvalId)
        throw new Error('native continuation identity is missing');

      const beforeLock = (await invoke({
        kind: 'tenant-fence',
        role: 'a',
        operation: 'read',
      })) as FenceMutationAfter;
      const locked = (await invoke({
        kind: 'tenant-fence',
        role: 'a',
        operation: 'lock',
        expectedMutationEpoch: beforeLock.mutationEpoch,
        expectedRevision: beforeLock.transitionRevision,
      })) as { ok: boolean; after?: FenceMutationAfter };
      expect(locked).toMatchObject({
        ok: true,
        after: {
          state: 'migration-locked',
          mutationEpoch: beforeLock.mutationEpoch + 1,
        },
      });

      const migrated = (await invoke({
        kind: 'migration-reprovision-a',
      })) as {
        databaseId: string;
        scriptName: string;
        routeHostname: string;
        initialVersionId: string;
        finalVersionId: string;
        initialSpecDigest: string;
        targetSpecDigest: string;
        settlementKey: string;
      };
      expect(migrated).toMatchObject({
        databaseId: generationA.databaseId,
        initialVersionId: generationA.versionId,
        finalVersionId: expect.any(String),
        settlementKey: expect.stringMatching(/^[a-f0-9]{64}$/u),
      });
      expect(migrated.finalVersionId).not.toBe(migrated.initialVersionId);
      const migrationAttempt = attempts.find(
        ({ action: actionName }) => actionName === 'migration-reprovision-a',
      );
      if (!migrationAttempt)
        throw new Error('native migration timing is missing');
      leaseOutcome =
        migrationAttempt.elapsedMs < DIRECT_REFERENCE_LEASE.leaseTtlMs
          ? 'completed-within-ttl'
          : 'completed-after-ttl';
      const generationB = tenant.generation;
      if (!generationB)
        throw new Error('native tenant generation B is missing');
      expect(generationB).toMatchObject({
        databaseId: generationA.databaseId,
        maintenanceNamespaceId: generationA.maintenanceNamespaceId,
        runnerNamespaceId: generationA.runnerNamespaceId,
        versionId: migrated.finalVersionId,
        release: '2',
      });

      const nextSpec = directDeploymentSpec(
        f.local.prepared.manifest,
        'a',
        'next',
        f.native.secrets.a,
        f.native.binding,
      );
      const observationB = await observeDirectWorkerVersion({
        ...f.input(),
        role: 'a',
        applicationRelease: '2',
        versionId: migrated.finalVersionId,
        databaseId: migrated.databaseId,
        specDigest: deploymentSpecDigest(nextSpec),
      });
      expect(observationB.currentDeployment.versions).toEqual([
        { versionId: migrated.finalVersionId, percentage: 100 },
      ]);
      expect(observationB).toMatchObject({
        databaseId: observationA.databaseId,
        scriptName: observationA.scriptName,
        namespaces: observationA.namespaces,
        schemaVersion: 2,
        applicationRelease: '2',
      });
      expect(f.local.prepared.manifest.tenantModule.sha256).toMatch(
        /^[a-f0-9]{64}$/u,
      );

      const beforeRefusal = await invoke({
        kind: 'tenant-continuation',
        operation: 'status',
        runId,
      });
      expect(
        await invoke({
          kind: 'tenant-continuation',
          operation: 'resume-locked',
          runId,
        }),
      ).toMatchObject({
        runId,
        status: 503,
        reason: { code: 'EXECUTION_FENCED', state: 'migration-locked' },
      });
      expect(
        await invoke({
          kind: 'tenant-continuation',
          operation: 'status',
          runId,
        }),
      ).toEqual(beforeRefusal);

      const beforeUnlock = locked.after;
      if (!beforeUnlock) throw new Error('native lock reading is missing');
      expect(
        await invoke({
          kind: 'tenant-fence',
          role: 'a',
          operation: 'unlock',
          expectedMutationEpoch: beforeUnlock.mutationEpoch,
          expectedRevision: beforeUnlock.transitionRevision,
        }),
      ).toMatchObject({
        ok: true,
        after: {
          state: 'open',
          mutationEpoch: beforeUnlock.mutationEpoch,
          transitionRevision: beforeUnlock.transitionRevision + 1,
        },
      });
      const finished = (await invoke({
        kind: 'tenant-continuation',
        operation: 'resume',
        runId,
        approvalId,
      })) as { status: number; summary: Record<string, unknown> };
      expect(finished.status).toBe(200);
      expect(finished.summary).toMatchObject({ runId, status: 'success' });
      expect(JSON.stringify(finished.summary)).toContain(challenge);
      expect(JSON.stringify(finished.summary)).toContain('"release":"2"');
      const terminalInventory = (await invoke({
        kind: 'tenant-fence',
        role: 'a',
        operation: 'inventory',
      })) as {
        categories: Array<{ category: string; class: string; empty: boolean }>;
      };
      console.log(
        'LV2_TERMINAL_INVENTORY',
        JSON.stringify(terminalInventory.categories),
      );
      expect(
        terminalInventory.categories.every(
          ({ class: categoryClass, empty }) =>
            categoryClass !== 'work' || empty,
        ),
      ).toBe(true);

      const script = f.native.world.scripts.get(migrated.scriptName);
      const active = script?.versions.find(
        ({ versionId }) => versionId === migrated.finalVersionId,
      );
      if (!active) throw new Error('native B provider version is missing');
      f.native.world.deleteScript(migrated.scriptName);
      const freshDatabase = f.native.world.createDatabase(
        f.local.prepared.names.roles.a.databaseName,
      );
      const freshBindings = active.bindings.map((entry) => {
        if (!entry || typeof entry !== 'object') return entry;
        const binding = { ...entry } as Record<string, unknown>;
        if (binding.name === 'DB')
          binding.database_id = freshDatabase.databaseId;
        if (
          binding.type === 'durable_object_namespace' &&
          (binding.name === 'RUNNER' || binding.name === 'MAINTENANCE')
        )
          delete binding.namespace_id;
        if (binding.name === 'APPLICATION_RELEASE') binding.text = '1';
        if (binding.name === 'FLEET_SPEC_DIGEST')
          binding.text = migrated.initialSpecDigest;
        return binding;
      });
      const [freshVersion] = f.native.world.applyUpload({
        scriptName: migrated.scriptName,
        mode: 'initial',
        tag: undefined,
        bindings: freshBindings,
        mainModule: active.mainModule,
        modules: active.modules,
      });
      if (!freshVersion) throw new Error('fresh provider version is missing');
      const nativeBinding = (name: string) => {
        const selected = freshVersion.bindings.find(
          (entry) =>
            entry &&
            typeof entry === 'object' &&
            Reflect.get(entry, 'name') === name,
        );
        const value =
          selected && typeof selected === 'object'
            ? Reflect.get(selected, 'namespace_id')
            : undefined;
        if (typeof value !== 'string')
          throw new Error(`fresh namespace '${name}' is missing`);
        return value;
      };
      const freshBucketBinding = freshVersion.bindings.find(
        (entry) =>
          entry &&
          typeof entry === 'object' &&
          Reflect.get(entry, 'name') === 'PROBE_BUCKET',
      );
      const freshBucketName =
        freshBucketBinding && typeof freshBucketBinding === 'object'
          ? Reflect.get(freshBucketBinding, 'bucket_name')
          : undefined;
      if (typeof freshBucketName !== 'string')
        throw new Error('fresh native bucket is missing');
      const freshGeneration = await tenant.activate({
        spec: directDeploymentSpec(
          f.local.prepared.manifest,
          'a',
          'initial',
          f.native.secrets.a,
          f.native.binding,
        ),
        versionId: freshVersion.versionId,
        databaseId: freshDatabase.databaseId,
        maintenanceNamespaceId: nativeBinding('MAINTENANCE'),
        runnerNamespaceId: nativeBinding('RUNNER'),
        deploymentIdentitySecret: f.native.secrets.a.deploymentIdentity,
        maintenanceAdminSecret: f.native.secrets.a.maintenanceAdmin,
        applicationToken: f.native.secrets.a.application?.APP_PROBE_TOKEN ?? '',
        release: '1',
        bucketName: freshBucketName,
      });
      expect(freshGeneration.databaseId).not.toBe(generationA.databaseId);
      expect(freshGeneration.maintenanceNamespaceId).not.toBe(
        generationA.maintenanceNamespaceId,
      );
      expect(freshGeneration.runnerNamespaceId).not.toBe(
        generationA.runnerNamespaceId,
      );
      expect(freshGeneration.workerName).not.toBe(generationA.workerName);

      console.log(
        'LV2_NATIVE_FEASIBILITY',
        JSON.stringify({
          attempts,
          migration: attempts.find(
            ({ action }) => action === 'migration-reprovision-a',
          ),
          leaseOutcome,
          locallyEnforced: {
            combinedAttempts: 400,
            invocationDeadlineMs: 600_000,
            leaseTtlMs: 900_000,
          },
          configuredNotLocallyEnforced: {
            referenceSubrequestLimit: 10_000,
            tenantSubrequestLimit: 50,
            cpuLimitMs: 50,
          },
        }),
      );
    } finally {
      console.log(
        'LV2_NATIVE_FEASIBILITY_PARTIAL',
        JSON.stringify({
          attempts,
          leaseOutcome,
        }),
      );
      await closeDirectScenarioFixtures();
    }
  }, 660_000);
});

describe.sequential('fence composition through native reference dispatch', {
  timeout: 660_000,
}, () => {
  it.each([
    ['reopen', 'draining', 1],
    ['reopen', 'open', 2],
    ['drain', 'open', 1],
  ] as const)('refuses a %s conflict at %s epoch %i without a later mutation', async (operation, state, mutationEpoch) => {
    let mutations: string[] | undefined;
    const conflict = fenceConflict(
      operation,
      (after) => ({
        state,
        mutationEpoch,
        requireMutationEpoch: true,
        transitionRevision: after.transitionRevision,
      }),
      () => {
        mutations = [...f.native.world.mutationLog];
      },
    );
    const f = await fixture({ nodeResponse: conflict.nodeResponse });
    const first = await runDirectCredentialedScenario(f.input());
    if (operation === 'reopen') {
      expect(first).toEqual({ status: 'restart-required' });
      expect((await childResume(f)).result).toMatchObject({
        status: 'failed',
        reason: 'observation-mismatch',
        phase: 'fence-reopen',
      });
    } else {
      expect(first).toMatchObject({
        status: 'failed',
        reason: 'observation-mismatch',
        phase: 'fence-drain',
      });
    }
    expect(conflict.fired()).toBe(true);
    const disk = JSON.parse(
      await readFile(join(f.local.journal.directory, 'journal.json'), 'utf8'),
    );
    expect(disk.scenario.failure.code).toBe('observation-mismatch');
    expect(f.native.world.mutationLog).toEqual(mutations);
  });

  it.each([
    'drain',
    'reopen',
  ] as const)('accepts a flattened %s conflict with an intervening revision', async (operation) => {
    const conflict = fenceConflict(operation, (after) => ({
      ...after,
      transitionRevision: after.transitionRevision + 1,
    }));
    const f = await fixture({ nodeResponse: conflict.nodeResponse });
    expect(await runDirectCredentialedScenario(f.input())).toEqual({
      status: 'restart-required',
    });
    const child = await childResume(f);
    expect(child.result).toMatchObject({ status: 'complete' });
    expect(conflict.fired()).toBe(true);
    if (child.result?.status !== 'complete')
      throw new Error('scenario did not complete');
    const entry = child.result.facts.fence[operation].a;
    expect(entry?.after).toEqual({
      state: operation === 'drain' ? 'draining' : 'open',
      mutationEpoch: 1,
      requireMutationEpoch: true,
      transitionRevision: (entry?.before.transitionRevision ?? -1) + 2,
    });
    if (operation === 'drain')
      expect(child.result.facts.fence.reopen.a?.before.transitionRevision).toBe(
        (entry?.before.transitionRevision ?? -1) + 1,
      );
  });

  it('replays a settled reopen body and preserves the saved draining sweep', async () => {
    const f = await fixture();
    expect(await runDirectCredentialedScenario(f.input())).toEqual({
      status: 'restart-required',
    });
    await f.native.reload();
    const hostname = f.local.prepared.names.roles.a.routeHostname;
    const bodies: Buffer[] = [];
    const originalFetch = f.native.projection.fetch;
    const capture = vi
      .spyOn(f.native.projection, 'fetch')
      .mockImplementation(async (input, init) => {
        const url = new URL(input instanceof Request ? input.url : input);
        if (
          url.hostname === hostname &&
          url.pathname === '/admin/execution-fence' &&
          init?.method === 'POST' &&
          typeof init.body === 'string'
        ) {
          const parsed = JSON.parse(init.body);
          if (parsed.next === 'open')
            bodies.push(Buffer.from(init.body, 'utf8'));
        }
        return originalFetch(input, init);
      });
    cleanup.push(async () => {
      capture.mockRestore();
    });
    const child = await childResume(f, 'fence-reopen-after-settle');
    expect(child.stdout).toContain('SCENARIO_FAULT fence-reopen-after-settle');
    expect(child.result).toBeNull();
    expect(bodies).toHaveLength(1);
    const saved = JSON.parse(
      await readFile(join(f.local.journal.directory, 'journal.json'), 'utf8'),
    ) as { scenario: DirectScenarioState };
    expect(saved.scenario).toMatchObject({
      phase: 'fence-reopen',
      failure: null,
      mutation: {
        outcome: 'returned',
        action: { kind: 'tenant-fence', operation: 'reopen', role: 'a' },
      },
      proofs: {
        fence: {
          reopen: { a: { after: null, ordinal: null } },
          sweeps: {
            a: {
              second: { fence: { state: 'draining' } },
              intervalMs: expect.any(Number),
            },
          },
        },
      },
    });
    const inventoryCount = () =>
      f.native.projection.requests.filter((request) => {
        const url = new URL(request.url);
        return url.hostname === hostname && url.pathname === '/admin/inventory';
      }).length;
    const beforeInventory = inventoryCount();
    const record = await f.native.fleetStore.get(
      f.local.prepared.names.roles.a.tenantTag,
      f.local.prepared.manifest.environment,
    );
    const database = f.native.world.databases.find(
      (entry) => entry.databaseId === record?.databaseId,
    )?.d1;
    if (!database) throw new Error('missing fixture database for role a');
    const resumed = await resume(f);
    const result = await runDirectCredentialedScenario(f.input(resumed));
    expect(result.status).toBe('complete');
    const proof = resumed.snapshot().scenario?.proofs.fence;
    expect(JSON.stringify(proof?.reopen.a?.before)).toBe(
      JSON.stringify(saved.scenario.proofs.fence.reopen.a?.before),
    );
    expect(JSON.stringify(proof?.sweeps.a?.second)).toBe(
      JSON.stringify(saved.scenario.proofs.fence.sweeps.a?.second),
    );
    expect(proof?.sweeps.a?.intervalMs).toBe(
      saved.scenario.proofs.fence.sweeps.a?.intervalMs,
    );
    expect(bodies).toHaveLength(3);
    expect(bodies[1]).toEqual(bodies[0]);
    expect(JSON.parse(bodies[2]?.toString('utf8') ?? '{}')).toMatchObject({
      expected: 'migration-locked',
      next: 'open',
    });
    expect(inventoryCount()).toBe(beforeInventory + 20);
    expect(proof?.reopen.a?.after).toMatchObject({
      state: 'open',
      mutationEpoch: 1,
      requireMutationEpoch: true,
    });
    const revision = (proof?.reopen.a?.before.transitionRevision ?? -1) + 1;
    expect(proof?.reopen.a?.after?.transitionRevision).toBe(revision);
    expect(
      database.queryDatabase(
        `SELECT mutation_epoch, transition_revision FROM ${EXECUTION_FENCE_TABLE} WHERE id = ?`,
        [EXECUTION_FENCE_ROW_ID],
      ),
    ).toEqual([{ mutation_epoch: 1, transition_revision: revision }]);
  });

  it('resumes a split drain without repeating completed role work', async () => {
    const f = await fixture();
    const child = await childResume(f, 'fence-drain-role-split');
    expect(child.stdout).toContain('SCENARIO_FAULT fence-drain-role-split');
    expect(child.result).toBeNull();
    const saved = JSON.parse(
      await readFile(join(f.local.journal.directory, 'journal.json'), 'utf8'),
    ) as { scenario: DirectScenarioState };
    expect(saved.scenario).toMatchObject({
      phase: 'fence-drain',
      failure: null,
      mutation: {
        outcome: 'returned',
        action: { kind: 'tenant-fence', operation: 'drain', role: 'a' },
      },
      lastCall: {
        outcome: 'returned',
        action: { kind: 'tenant-fence', operation: 'read', role: 'b' },
      },
      proofs: {
        fence: {
          drain: {
            a: { after: { state: 'draining', mutationEpoch: 1 } },
            b: { after: null, ordinal: null },
          },
          sweeps: { a: { first: { fence: { state: 'draining' } } } },
        },
      },
    });
    const requests = () =>
      f.native.projection.requests.filter((request) => {
        const url = new URL(request.url);
        return (
          url.hostname === f.local.prepared.names.roles.a.routeHostname &&
          (url.pathname === '/admin/execution-fence' ||
            url.pathname === '/admin/inventory')
        );
      }).length;
    const beforeRequests = requests();
    const resumed = await resume(f);
    expect(await runDirectCredentialedScenario(f.input(resumed))).toEqual({
      status: 'restart-required',
    });
    const drained = resumed.snapshot().scenario?.proofs.fence;
    expect(requests()).toBe(beforeRequests);
    expect(JSON.stringify(drained?.drain.a)).toBe(
      JSON.stringify(saved.scenario.proofs.fence.drain.a),
    );
    expect(JSON.stringify(drained?.sweeps.a?.first)).toBe(
      JSON.stringify(saved.scenario.proofs.fence.sweeps.a?.first),
    );
    expect(JSON.stringify(drained?.drain.b?.before)).toBe(
      JSON.stringify(saved.scenario.proofs.fence.drain.b?.before),
    );
    expect(drained?.drain.b).toMatchObject({
      after: { state: 'draining', mutationEpoch: 1 },
      ordinal: expect.any(Number),
    });
    await resumed.close();
    await f.native.reload();
    const completed = await childResume(f);
    expect(completed.result).toMatchObject({ status: 'complete' });
    if (completed.result?.status !== 'complete')
      throw new Error('scenario did not complete');
    expect(completed.result.facts.fence.drain.a).toEqual(
      saved.scenario.proofs.fence.drain.a,
    );
  });
});

describe('scenario resume re-entry against a settled journal', () => {
  const OPERATION_INPUT = '{"records":[]}';
  const OPERATION_ID = 'migration-operation';
  const ENTRY_DIGEST = 'e'.repeat(64);
  const TARGET_DIGEST = 'f'.repeat(64);
  const SPEC_DIGEST = 'd'.repeat(64);

  type Journal = Awaited<ReturnType<typeof scenarioJournal>>;

  const remoteOperation = () => ({
    slot: 'migration-next' as const,
    operationId: OPERATION_ID,
    inputJson: OPERATION_INPUT,
    tokenRevision: 1,
  });

  const remoteRecord = (role: 'a' | 'b' | 'recovery') => ({
    role,
    present: true,
    phase: 'ready',
    desiredSpecDigest: SPEC_DIGEST,
    pendingSpecDigest: SPEC_DIGEST,
    artifactVersion: `version-${role}`,
    pendingArtifactVersion: `version-${role}`,
    databaseId: `database-${role}`,
  });

  const migrationItems = (f: Journal['f']) =>
    (['a', 'b'] as const).map((role, index) => ({
      ordinal: index,
      tenantTag: f.prepared.names.roles[role].tenantTag,
      environment: f.prepared.config.environment,
      status: index === 0 ? 'active' : 'pending',
      planCursor: 0,
      entryRecordDigest: ENTRY_DIGEST,
      targetSpecDigest: TARGET_DIGEST,
      plan: [{ step: 'arm-maintenance' }, { step: 'promote' }],
    }));

  const witness = (f: Journal['f']) => {
    const claimJson = JSON.stringify({
      operationId: OPERATION_ID,
      revision: 1,
    });
    const returnedTokenJson = JSON.stringify({
      operationId: OPERATION_ID,
      revision: 2,
    });
    return {
      claimJson,
      returnedTokenJson,
      interruption: JSON.stringify({
        version: 1,
        boundary: 'after-migration-admission',
        slot: 'migration-next',
        operationId: OPERATION_ID,
        claimJson,
        returnedTokenJson,
        item: {
          ordinal: 0,
          beforeStatus: 'pending',
          afterStatus: 'active',
          planCursor: 0,
          tenantTag: f.prepared.names.roles.a.tenantTag,
          environment: f.prepared.config.environment,
          entryRecordDigest: ENTRY_DIGEST,
          targetSpecDigest: TARGET_DIGEST,
        },
      }),
    };
  };

  function reference(journal: Journal, options: { interrupted: boolean }) {
    const { f, journal: handle } = journal;
    const proof = witness(f);
    const actions: DirectReferenceAction[] = [];
    let interruption = options.interrupted ? proof.interruption : null;
    const control = () => ({
      binding: {
        version: 1,
        accountId: 'account',
        fleetDatabaseId: 'fleet-uuid',
        quotaDatabaseId: 'quota-uuid',
        exportBucketName: f.prepared.names.exportBucket,
        referenceModuleSetSha256: f.prepared.referenceModuleSetSha256,
        accountWorkersDevSubdomain: 'attested-account',
      },
      operations: [remoteOperation()],
      records: (['a', 'b', 'recovery'] as const).map(remoteRecord),
      interruption,
      forceBefore: null,
      forceAfter: null,
    });
    const invocation: DirectInvocationClient = {
      async invoke(action) {
        actions.push(action);
        const reservation = await handle.reserveInvocation(
          JSON.stringify({
            contractVersion: 2,
            configSha256: f.prepared.configSha256,
            action,
          }),
        );
        await handle.settleInvocation(reservation);
        const attempts = { provider: 0, maintenance: 0, application: 0 };
        if (handle.snapshot().scenario?.phase === 'migration')
          throw new DirectInvocationError('reference-refused', attempts);
        if (action.kind === 'control-read')
          return { result: control(), attempts };
        if (action.kind === 'migration-page')
          return { result: { done: true, items: migrationItems(f) }, attempts };
        if (action.kind === 'migration-start')
          return { result: { status: 'pending' }, attempts };
        if (action.kind === 'migration-continue') {
          if (!action.token) {
            interruption = proof.interruption;
            throw new DirectInvocationError('injected-response-loss', attempts);
          }
          return {
            result: {
              status: 'pending',
              token: JSON.parse(proof.returnedTokenJson),
              itemOrdinal: 0,
              planCursor: 0,
            },
            attempts,
          };
        }
        throw new Error(`unexpected fixture action ${action.kind}`);
      },
    };
    return { actions, invocation, proof, control };
  }

  function seed(
    phase: MutableScenario['phase'],
    mutate: (state: MutableScenario) => void,
  ): MutableScenario {
    return scenarioWith((state) => {
      state.phase = phase;
      state.failure = null;
      state.lastCall = null;
      state.mutation = null;
      state.operations = [
        {
          slot: 'migration-next',
          operationId: OPERATION_ID,
          inputSha256: hash(OPERATION_INPUT),
          tokenRevision: 1,
        },
      ];
      state.records = (['a', 'b', 'recovery'] as const).map(remoteRecord);
      state.proofs.restart = null;
      state.proofs.force = null;
      state.proofs.residual = null;
      mutate(state);
    });
  }

  const settledCall = (
    kind: string,
    outcome: string,
    ordinal: number,
    action: Record<string, unknown> = {},
    attempts = { provider: 0, maintenance: 0, application: 0 },
  ) => ({
    ordinal,
    action: { kind, ...action },
    outcome,
    attempts,
    migration: null,
  });

  const restartProof = (
    f: Journal['f'],
    overrides: Record<string, unknown>,
  ) => {
    const proof = witness(f);
    return {
      process: { ...PROCESS },
      resumedProcess: null,
      lossOrdinal: 2,
      operationId: OPERATION_ID,
      witnessSha256: hash(proof.interruption),
      claimSha256: hash(proof.claimJson),
      successorSha256: hash(proof.returnedTokenJson),
      itemsSha256: jsonHash(migrationItems(f)),
      replayOrdinal: null,
      ...overrides,
    };
  };

  const run = (journal: Journal, invocation: DirectInvocationClient) =>
    runDirectCredentialedScenario({
      prepared: journal.f.prepared,
      journal: journal.journal,
      invocation,
      apiToken: 'inert',
    });

  const stored = (journal: Journal): DirectScenarioState => {
    const state = journal.journal.snapshot().scenario;
    if (!state) throw new Error('scenario state is missing');
    return state;
  };

  type ControlRead = { records: { role: string }[] };

  const withControlRead = <Result>(
    invocation: DirectInvocationClient,
    rewrite: (result: Result) => object,
  ): DirectInvocationClient => ({
    async invoke(action) {
      const outcome = await invocation.invoke(action);
      if (action.kind !== 'control-read') return outcome;
      return { ...outcome, result: rewrite(outcome.result as Result) };
    },
  });

  const withoutRoleA = (result: ControlRead) => ({
    ...result,
    records: result.records.map((record) =>
      record.role === 'a' ? { role: 'a', present: false } : record,
    ),
  });

  const seedTerminalForceResume = async (
    target: Journal,
    attempts: { provider: number; maintenance: number; application: number },
    options: {
      persisted?: boolean;
      identity?: 'matching' | 'absent' | 'foreign';
    } = {},
  ) => {
    const identity = options.identity ?? 'matching';
    const state = seed('force-terminal-a', (draftState) => {
      draftState.mutation = settledCall(
        'force-terminal',
        'returned',
        3,
        { role: 'a' },
        attempts,
      ) as MutableScenario['mutation'];
      present(draftState.mutation).before =
        identity === 'absent'
          ? null
          : {
              databaseId:
                identity === 'foreign'
                  ? 'foreign-database'
                  : present(draftState.proofs.decommission.a).databaseId,
              scriptName: present(draftState.proofs.decommission.a).scriptName,
            };
      draftState.proofs.terminalForce = { a: null };
      present(draftState.proofs.steps[0]).step = 'arm-maintenance';
      present(draftState.proofs.steps[1]).step = 'arm-maintenance';
      draftState.proofs.restart = restartProof(target.f, {
        resumedProcess: { ...RESUMED },
        replayOrdinal: 3,
      }) as MutableScenario['proofs']['restart'];
      draftState.records = draftState.records.map((entry) =>
        entry.role === 'a' ? recordFacts({ role: 'a', present: false }) : entry,
      );
    });
    const proof = {
      databaseId: present(state.proofs.decommission.a).databaseId,
      scriptName: present(state.proofs.decommission.a).scriptName,
      ordinal: 3,
      attempts,
    };
    if (options.persisted) state.proofs.terminalForce.a = proof;
    const frozen = JSON.stringify(state.proofs.terminalForce);
    await expect(target.journal.recordScenario(state)).resolves.toBeUndefined();
    expect(stored(target).phase).toBe('force-terminal-a');
    const { invocation, actions } = reference(target, { interrupted: true });
    return { state, proof, frozen, actions, invocation };
  };

  const noForceTerminal = (actions: DirectReferenceAction[]) =>
    expect(
      actions.filter((action) => action.kind === 'force-terminal'),
    ).toEqual([]);

  it.each([
    ['interrupted at mutation settlement', false],
    ['interrupted at proof persistence', true],
  ] as const)('terminal force resume: %s', async (_title, persisted) => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    const { proof, frozen, actions, invocation } =
      await seedTerminalForceResume(
        target,
        { provider: 0, maintenance: 0, application: 0 },
        { persisted },
      );
    await run(target, withControlRead(invocation, withoutRoleA));
    noForceTerminal(actions);
    expect(stored(target).phase).toBe('reprovision-a');
    expect(stored(target).proofs.terminalForce.a).toEqual(proof);
    if (persisted)
      expect(JSON.stringify(stored(target).proofs.terminalForce)).toBe(frozen);
  });

  it('terminal force resume: persisted nonzero-attempt witness', async () => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    const { actions, invocation } = await seedTerminalForceResume(target, {
      provider: 1,
      maintenance: 0,
      application: 0,
    });
    const result = await run(target, withControlRead(invocation, withoutRoleA));
    noForceTerminal(actions);
    expect(result).toMatchObject({
      status: 'failed',
      reason: 'observation-mismatch',
      phase: 'force-terminal-a',
    });
    expect(stored(target).proofs.terminalForce.a).toBeNull();
  });

  it.each([
    ['persisted absent-row no-op witness', 'absent'],
    ['persisted foreign before identity', 'foreign'],
  ] as const)('terminal force resume: %s', async (_title, identity) => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    const { actions, invocation } = await seedTerminalForceResume(
      target,
      { provider: 0, maintenance: 0, application: 0 },
      { identity },
    );
    const result = await run(target, withControlRead(invocation, withoutRoleA));
    noForceTerminal(actions);
    expect(result).toMatchObject({
      status: 'failed',
      reason: 'observation-mismatch',
      phase: 'force-terminal-a',
    });
    expect(stored(target).proofs.terminalForce.a).toBeNull();
  });

  it('terminal force resume: reconciles the mutation against the record it still holds', async () => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    const attempts = { provider: 0, maintenance: 0, application: 0 };
    const state = seed('force-terminal-a', (draftState) => {
      draftState.mutation = settledCall(
        'force-terminal',
        'returned',
        3,
        { role: 'a' },
        attempts,
      ) as MutableScenario['mutation'];
      present(draftState.mutation).before = {
        databaseId: present(draftState.proofs.decommission.a).databaseId,
        scriptName: present(draftState.proofs.decommission.a).scriptName,
      };
      draftState.proofs.terminalForce = { a: null };
      present(draftState.proofs.steps[0]).step = 'arm-maintenance';
      present(draftState.proofs.steps[1]).step = 'arm-maintenance';
      draftState.proofs.restart = restartProof(target.f, {
        resumedProcess: { ...RESUMED },
        replayOrdinal: 3,
      }) as MutableScenario['proofs']['restart'];
      // The mutation settled after the last reconciliation and role a is still
      // recorded present, so the resumed run reads the deletion through the
      // reconciliation the force permits rather than from a record that
      // already agreed with the provider.
      draftState.reconciledOrdinal = 2;
    });
    await target.journal.recordScenario(state);
    expect(stored(target).records[0]).toMatchObject({
      role: 'a',
      present: true,
    });
    const { invocation, actions } = reference(target, { interrupted: true });
    await run(target, withControlRead(invocation, withoutRoleA));
    noForceTerminal(actions);
    expect(stored(target).phase).toBe('reprovision-a');
    expect(stored(target).proofs.terminalForce.a).toEqual({
      databaseId: present(state.proofs.decommission.a).databaseId,
      scriptName: present(state.proofs.decommission.a).scriptName,
      ordinal: 3,
      attempts,
    });
    expect(stored(target).records[0]).toMatchObject({
      role: 'a',
      present: false,
    });
    expect(stored(target).reconciledOrdinal).toBeGreaterThan(3);
  });

  // Columns: the witness the reference returns, the invocation failure it
  // raises instead, and the reason the run reports. A refused reference is the
  // one row whose own code reaches the caller.
  it.each([
    ['matching identity', 'matching', null, 'observation-mismatch'],
    ['absent row', null, null, 'observation-mismatch'],
    ['missing identity', undefined, null, 'observation-mismatch'],
    [
      'extra identity key',
      { databaseId: 'db', scriptName: 'script', extra: true },
      null,
      'observation-mismatch',
    ],
    [
      'non-string databaseId',
      { databaseId: 1, scriptName: 'script' },
      null,
      'observation-mismatch',
    ],
    [
      'non-string scriptName',
      { databaseId: 'db', scriptName: null },
      null,
      'observation-mismatch',
    ],
    // Two string members, both named, one outside the journal's identifier
    // charset: the refusal lands before settlement, not at the persist that
    // would follow it.
    [
      'out-of-charset databaseId',
      { databaseId: 'db one', scriptName: 'script' },
      null,
      'observation-mismatch',
    ],
    ['array identity', [], null, 'observation-mismatch'],
    [
      'lost response',
      undefined,
      'injected-response-loss',
      'observation-mismatch',
    ],
    ['refused response', undefined, 'reference-refused', 'reference-refused'],
  ] as const)('terminal force settlement: %s', async (_title, witnessKind, failure, reason) => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    const state = seed('force-terminal-a', (draftState) => {
      draftState.proofs.terminalForce = { a: null };
      present(draftState.proofs.steps[0]).step = 'arm-maintenance';
      present(draftState.proofs.steps[1]).step = 'arm-maintenance';
      draftState.proofs.restart = restartProof(target.f, {
        resumedProcess: { ...RESUMED },
        replayOrdinal: 3,
      }) as MutableScenario['proofs']['restart'];
    });
    const before =
      witnessKind === 'matching'
        ? {
            databaseId: present(state.proofs.decommission.a).databaseId,
            scriptName: present(state.proofs.decommission.a).scriptName,
          }
        : witnessKind;
    await target.journal.recordScenario(state);
    const { invocation, actions } = reference(target, { interrupted: true });
    const force: DirectInvocationClient = {
      async invoke(action) {
        if (action.kind !== 'force-terminal') return invocation.invoke(action);
        actions.push(action);
        const reservation = await target.journal.reserveInvocation(
          target.f.request(action),
        );
        await target.journal.settleInvocation(reservation);
        const attempts = { provider: 0, maintenance: 0, application: 0 };
        if (failure) throw new DirectInvocationError(failure, attempts);
        return {
          result: { returned: true, before, after: { present: false } },
          attempts,
        };
      },
    };
    // Left unassigned so a row that never settles is distinguishable from one
    // that settled on a null call.
    let settled: DirectScenarioState['mutation'] | undefined;
    const journal: DirectRunJournal = {
      ...target.journal,
      async recordScenario(value) {
        await target.journal.recordScenario(value);
        const call = stored(target).mutation;
        if (
          call?.action.kind === 'force-terminal' &&
          call.outcome !== 'prepared'
        ) {
          settled = call;
        }
      },
    };
    const result = await runDirectCredentialedScenario({
      prepared: target.f.prepared,
      journal,
      invocation: force,
      apiToken: 'inert',
    });
    expect(result).toMatchObject({
      status: 'failed',
      phase: 'force-terminal-a',
      reason,
    });
    expect(
      actions.filter((action) => action.kind === 'force-terminal'),
    ).toHaveLength(1);
    expect(stored(target).proofs.terminalForce.a).toBeNull();
    if (witnessKind === 'matching')
      expect(settled).toMatchObject({
        outcome: 'returned',
        before,
        attempts: { provider: 0, maintenance: 0, application: 0 },
      });
    else if (failure)
      expect(settled).toMatchObject({
        outcome: failure,
        before: null,
        attempts: { provider: 0, maintenance: 0, application: 0 },
      });
    else if (witnessKind === null)
      expect(settled).toMatchObject({
        outcome: 'returned',
        before: null,
        attempts: { provider: 0, maintenance: 0, application: 0 },
      });
    else {
      expect(settled).toBeUndefined();
      expect(stored(target).mutation?.outcome).toBe('prepared');
      expect(stored(target).mutation).not.toHaveProperty('before');
      expect(stored(target).failure?.code).toBe('observation-mismatch');
    }
  });

  it('refuses a control read that repeats an operation slot', async () => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    await target.journal.recordScenario(
      seed('migration-start', () => {}) as DirectScenarioState,
    );
    const { invocation } = reference(target, { interrupted: false });
    const duplicating = withControlRead(
      invocation,
      (result: { operations: readonly unknown[] }) => ({
        ...result,
        operations: [...result.operations, ...result.operations],
      }),
    );
    expect(await run(target, duplicating)).toMatchObject({
      status: 'failed',
      reason: 'observation-mismatch',
      phase: 'migration-start',
    });
    expect(stored(target).failure).toMatchObject({
      code: 'observation-mismatch',
    });
  });

  it.each([
    'platform-page',
    'transport-failure',
    'non-contract-answer',
    'delivery-window-expired',
  ] as const)('persists invocation outcome-unknown detail %s in scenario failure', async (detail) => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    const invocation: DirectInvocationClient = {
      async invoke() {
        throw new DirectInvocationError(
          'outcome-unknown',
          undefined,
          undefined,
          detail,
        );
      },
    };
    expect(await run(target, invocation)).toMatchObject({
      status: 'failed',
      reason: 'outcome-unknown',
      detail,
    });
    expect(stored(target).failure).toMatchObject({
      code: 'outcome-unknown',
      detail,
    });
  });

  it('re-raises the persisted refusal detail on resume', async () => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    await target.journal.recordScenario(
      seed('migration-start', (state) => {
        state.failure = {
          code: 'budget-exhausted',
          ordinal: 3,
          detail: 'below-scenario-floor',
        };
      }) as DirectScenarioState,
    );
    const { actions, invocation } = reference(target, { interrupted: false });
    expect(await run(target, invocation)).toMatchObject({
      status: 'failed',
      reason: 'budget-exhausted',
      detail: 'below-scenario-floor',
      phase: 'migration-start',
    });
    expect(actions).toEqual([]);
    expect(stored(target).failure).toEqual({
      code: 'budget-exhausted',
      ordinal: 3,
      detail: 'below-scenario-floor',
    });
  });

  it('records the prepared continuation-start run id as abandoned and never replays it', async () => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    const prepared = {
      ordinal: target.journal.snapshot().invocationCount + 1,
      action: {
        kind: 'tenant-continuation' as const,
        operation: 'start' as const,
        challenge: 'a'.repeat(64),
      },
      outcome: 'prepared' as const,
      attempts: null,
      migration: null,
    };
    await target.journal.recordScenario(
      seed('continuation-start', (state) => {
        const [armA, armB] = state.proofs.steps;
        if (!armA || !armB) throw new Error('migration steps are missing');
        armA.step = 'arm-maintenance';
        armB.step = 'arm-maintenance';
        state.proofs.restart = restartProof(target.f, {
          resumedProcess: { ...RESUMED },
          replayOrdinal: 3,
        }) as MutableScenario['proofs']['restart'];
        state.lastCall = prepared;
        state.mutation = prepared;
        state.proofs.continuation.started = null;
      }) as DirectScenarioState,
    );
    const { actions, invocation } = reference(target, { interrupted: false });
    expect(await run(target, invocation)).toMatchObject({
      status: 'failed',
      reason: 'proof-unavailable',
      detail: 'lost-run-id-abandoned',
      phase: 'continuation-start',
    });
    expect(actions).toEqual([]);
    expect(stored(target).failure).toMatchObject({
      code: 'proof-unavailable',
      detail: 'lost-run-id-abandoned',
    });
    expect(isAbandonedDirectScenario(target.journal.snapshot().scenario)).toBe(
      true,
    );
  });

  it('reports the persisted failure code after a resume spends an invocation', async () => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    await target.journal.recordScenario(
      seed('migration-start', (state) => {
        state.failure = { code: 'reference-refused', ordinal: 3 };
      }) as DirectScenarioState,
    );
    const reservation = await target.journal.reserveInvocation(
      JSON.stringify({
        contractVersion: 2,
        configSha256: target.f.prepared.configSha256,
        action: { kind: 'control-read' },
      }),
    );
    await target.journal.settleInvocation(reservation);
    const { actions, invocation } = reference(target, { interrupted: false });
    expect(await run(target, invocation)).toMatchObject({
      status: 'failed',
      reason: 'reference-refused',
      phase: 'migration-start',
    });
    expect(actions).toEqual([]);
    expect(stored(target).failure).toEqual({
      code: 'reference-refused',
      ordinal: 3,
    });
  });

  it('issues no second migration start and consumes the injection only at the interrupt', async () => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    await target.journal.recordScenario(
      seed('migration-start', (draftState) => {
        const call = settledCall('migration-start', 'returned', 3);
        draftState.lastCall = call as MutableScenario['lastCall'];
        draftState.mutation = call as MutableScenario['mutation'];
      }) as DirectScenarioState,
    );
    const { actions, invocation } = reference(target, { interrupted: false });
    expect(await run(target, invocation)).toEqual({
      status: 'restart-required',
    });
    expect(
      actions.filter((action) => action.kind === 'migration-start'),
    ).toEqual([]);
    expect(
      actions.filter((action) => action.kind === 'migration-continue'),
    ).toHaveLength(1);
    const state = stored(target);
    expect(state.phase).toBe('migration-restart');
    expect(state.proofs.restart?.process.pid).toBe(process.pid);
    expect(state.proofs.restart?.replayOrdinal).toBeNull();
  });

  it('keeps the frozen restart proof when the interrupt re-enters after its own persist', async () => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    const frozen = restartProof(target.f, {});
    await target.journal.recordScenario(
      seed('migration-interrupt', (draftState) => {
        const call = settledCall(
          'migration-continue',
          'injected-response-loss',
          2,
        );
        draftState.lastCall = call as MutableScenario['lastCall'];
        draftState.mutation = call as MutableScenario['mutation'];
        draftState.proofs.restart =
          frozen as MutableScenario['proofs']['restart'];
      }) as DirectScenarioState,
    );
    const { actions, invocation } = reference(target, { interrupted: true });
    expect(await run(target, invocation)).toEqual({
      status: 'restart-required',
    });
    expect(actions.filter((action) => action.kind !== 'control-read')).toEqual(
      [],
    );
    const state = stored(target);
    expect(state.phase).toBe('migration-restart');
    expect(JSON.stringify(state.proofs.restart)).toBe(JSON.stringify(frozen));
  });

  it('keeps the frozen replay ordinal when the restart re-enters after its own persist', async () => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    const frozen = restartProof(target.f, {
      resumedProcess: { ...RESUMED },
      replayOrdinal: 3,
    });
    await target.journal.recordScenario(
      seed('migration-restart', (state) => {
        const call = settledCall('migration-continue', 'returned', 3);
        state.lastCall = call as MutableScenario['lastCall'];
        state.mutation = call as MutableScenario['mutation'];
        state.proofs.restart = frozen as MutableScenario['proofs']['restart'];
      }) as DirectScenarioState,
    );
    const { actions, invocation } = reference(target, { interrupted: true });
    expect(await run(target, invocation)).toMatchObject({
      status: 'failed',
      reason: 'reference-refused',
      phase: 'migration',
    });
    expect(
      actions.filter((action) => action.kind === 'migration-continue'),
    ).toEqual([]);
    expect(JSON.stringify(stored(target).proofs.restart)).toBe(
      JSON.stringify(frozen),
    );
  });
});
