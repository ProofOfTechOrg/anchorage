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
  openDirectRunState,
} from '../scripts/direct-credentialed-run-state.mjs';
import {
  type DirectScenarioState,
  runDirectCredentialedScenario,
} from '../scripts/direct-credentialed-scenario.mjs';
import {
  DIRECT_SCENARIO_INVOCATION_BUDGET,
  DIRECT_SCENARIO_MIN_INVOCATIONS,
} from '../scripts/direct-credentialed-scenario-budget.mjs';
import {
  hash,
  jsonHash,
  recordFacts,
} from '../scripts/direct-credentialed-scenario-checks.mjs';
import { DIRECT_TENANT_OBJECT_BODY } from '../scripts/direct-credentialed-tenant-object.mjs';
import type { DirectReferenceAction } from '../scripts/direct-reference-contract.mjs';
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
            contractVersion: 1,
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
  it('completes normal migration, historical recovery and force after a real child restart', async () => {
    const f = await fixture();
    const first = await runDirectCredentialedScenario(f.input());
    expect({
      first,
      bridgeErrors: f.native.bridgeErrors,
      state: f.local.journal.snapshot().scenario,
    }).toMatchObject({
      first: { status: 'restart-required' },
      bridgeErrors: [],
    });
    const initial = f.local.journal.snapshot();
    const count = initial.invocationCount;
    expect(await runDirectCredentialedScenario(f.input())).toEqual({
      status: 'restart-required',
    });
    expect(f.local.journal.snapshot().invocationCount).toBe(count);
    const witness = await f.native.journal().readInterruption();
    expect(witness).not.toBeNull();
    const stale = JSON.parse(JSON.parse(witness as string).claimJson);
    const wrong = await f.native.call({
      kind: 'migration-continue',
      token: { ...stale, operationId: 'foreign-operation' },
    });
    expect(wrong.response.status).toBe(409);
    expect(wrong.response.headers.get('X-Direct-Provider-Attempts')).toBe('0');
    await f.native.reload();
    const child = await childResume(f);
    expect({
      result: child.result,
      bridgeErrors: f.native.bridgeErrors,
      journal: await readFile(
        join(f.local.journal.directory, 'journal.json'),
        'utf8',
      ),
    }).toMatchObject({ result: { status: 'complete' }, bridgeErrors: [] });
    if (child.result?.status !== 'complete')
      throw new Error('scenario did not complete');
    const proofs = child.result.facts;
    expect(proofs.restart?.process.pid).toBe(process.pid);
    expect(proofs.restart?.resumedProcess?.pid).not.toBe(process.pid);
    for (const role of ['a', 'b'] as const) {
      const fence = proofs.fence;
      expect(fence.drain[role]).toMatchObject({
        before: {
          state: 'open',
          mutationEpoch: 0,
          requireMutationEpoch: false,
        },
        after: {
          state: 'draining',
          mutationEpoch: 1,
          requireMutationEpoch: true,
        },
        ordinal: expect.any(Number),
      });
      expect(fence.reopen[role]).toMatchObject({
        before: {
          state: 'draining',
          mutationEpoch: 1,
          requireMutationEpoch: true,
        },
        after: { state: 'open', mutationEpoch: 1, requireMutationEpoch: true },
        ordinal: expect.any(Number),
      });
      expect(fence.sweeps[role]).toMatchObject({
        first: { fence: { state: 'draining' }, ordinal: expect.any(Number) },
        second: { fence: { state: 'draining' }, ordinal: expect.any(Number) },
        intervalMs: expect.any(Number),
      });
      for (const sweep of [
        fence.sweeps[role]?.first,
        fence.sweeps[role]?.second,
      ]) {
        expect(sweep).not.toBeNull();
        expect(
          sweep?.categories.every(
            (entry) => entry.class !== 'work' || entry.empty,
          ),
        ).toBe(true);
      }
      expect(fence.probes[role]).toEqual({
        current: 'accepted',
        missing: 'missing',
        stale: 'stale',
        future: 'future',
        mutationEpoch: 1,
        ordinal: expect.any(Number),
      });
      expect(proofs.initial[role]?.trafficPercentage).toBe(100);
      expect(proofs.candidate[role]?.trafficPercentage).toBe(0);
      expect(proofs.final[role]?.trafficPercentage).toBe(100);
      expect(proofs.exports[role]?.verified).toBe(true);
      expect(proofs.decommission[role]?.phase).toBe('decommissioned');
      const history = proofs.exportVerifications.filter(
        (entry) => entry.role === role,
      );
      expect(history.length).toBeGreaterThan(0);
      expect(history.filter((entry) => entry.verified)).toEqual(history);
      expect(history.at(-1)).toEqual(proofs.exports[role]);
    }
    expect(proofs.terminalForce.a).toEqual({
      databaseId: proofs.decommission.a?.databaseId,
      scriptName: proofs.decommission.a?.scriptName,
      ordinal: expect.any(Number),
      attempts: { provider: 0, maintenance: 0, application: 0 },
    });
    expect(proofs.inventories.after?.routeHostnames).toEqual(
      proofs.inventories.before?.routeHostnames,
    );
    expect(proofs.inventories.after?.routeHostnames).toEqual(
      (['a', 'b'] as const)
        .map((role) => f.local.prepared.names.roles[role].routeHostname)
        .sort(),
    );
    expect(proofs.effects).toHaveLength(2);
    expect(proofs.inventories.before?.calls).toBeGreaterThan(1);
    expect(proofs.inventories.after?.generation).toBeGreaterThan(
      proofs.inventories.before?.generation ?? 0,
    );
    expect(proofs.audits.before?.findings).toEqual([]);
    expect(proofs.audits.after?.findings).toEqual([]);
    expect(proofs.force?.worker.scriptPresent).toBe(true);
    expect(proofs.residual?.worker.scriptPresent).toBe(false);
    expect(proofs.force?.priorCleanup.operationId).toBe(
      proofs.cleanup?.operationId,
    );
    expect(proofs.residual?.priorCleanup).toEqual(proofs.force?.priorCleanup);
    expect(f.native.world.databases).toEqual([]);
    expect(f.native.buckets.size).toBe(0);
    expect((await f.native.exportBytes.list()).objects.length).toBeGreaterThan(
      0,
    );
    const serialized = await readFile(
      join(f.local.journal.directory, 'journal.json'),
      'utf8',
    );
    for (const sentinel of [
      'APP_PROBE_TOKEN',
      'inert-provider-token',
      'inert-invoke',
      'claimJson',
      'tokenJson',
      'SELECT ',
      'CREATE TABLE ',
      'INSERT INTO ',
      DIRECT_TENANT_OBJECT_BODY,
    ])
      expect(serialized).not.toContain(sentinel);
    const budget: Record<string, { ceiling: number }> =
      DIRECT_SCENARIO_INVOCATION_BUDGET;
    const phaseCalls: Record<string, number> =
      JSON.parse(serialized).scenario.phaseCalls;
    for (const phase of ['fence-drain', 'fence-reopen', 'fence-proofs'])
      expect(phaseCalls[phase]).toBe(9);
    expect(
      Object.entries(phaseCalls).filter(
        ([phase, calls]) => calls > (budget[phase]?.ceiling ?? 0),
      ),
    ).toEqual([]);
    expect(serialized).not.toContain(JSON.parse(witness as string).claimJson);
    const resumed = await resume(f);
    expect((await runDirectCredentialedScenario(f.input(resumed))).status).toBe(
      'complete',
    );
  });
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
    expect(bodies).toHaveLength(2);
    expect(bodies[1]).toEqual(bodies[0]);
    expect(inventoryCount()).toBe(beforeInventory);
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
            contractVersion: 1,
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
    expect(stored(target).phase).toBe('force-recovery');
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
    expect(stored(target).phase).toBe('force-recovery');
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

  it('reports the persisted failure code after a resume spends an invocation', async () => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    await target.journal.recordScenario(
      seed('migration-start', (state) => {
        state.failure = { code: 'reference-refused', ordinal: 3 };
      }) as DirectScenarioState,
    );
    const reservation = await target.journal.reserveInvocation(
      JSON.stringify({
        contractVersion: 1,
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
