// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';
import { readFile, writeFile } from 'node:fs/promises';
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
  type DirectScenarioOutcome,
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
} from '../scripts/direct-credentialed-scenario-checks.mjs';
import { DIRECT_TENANT_OBJECT_BODY } from '../scripts/direct-credentialed-tenant-object.mjs';
import type { DirectReferenceAction } from '../scripts/direct-reference-contract.mjs';
import { directObservationFixture } from './fixtures/direct-observations.js';
import { createDirectReferenceHarness } from './fixtures/direct-reference-harness.js';
import {
  cleanupDirectRunState,
  type MutableScenario,
  PROCESS,
  RESUMED,
  scenarioJournal,
  scenarioWith,
} from './fixtures/direct-run-state-builder.js';

const cleanup: (() => Promise<void>)[] = [];
afterEach(async () => {
  const results = await Promise.allSettled(
    cleanup
      .splice(0)
      .reverse()
      .map((close) => close()),
  );
  await cleanupDirectRunState();
  const failed = results.filter((result) => result.status === 'rejected');
  expect(failed).toEqual([]);
});

async function resume(f: Awaited<ReturnType<typeof fixture>>) {
  const resumed = await openDirectRunState({
    configPath: f.local.configPath,
    prepared: f.local.prepared,
    accountId: 'account',
    mode: 'resume',
  });
  cleanup.push(() => resumed.close());
  return resumed;
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
    const record = control.records.find((record) => record.role === 'a');
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

async function fixture(
  options: {
    nodeResponse?: NonNullable<
      Parameters<typeof createDirectReferenceHarness>[0]
    >['nodeResponse'];
  } = {},
) {
  const local = await directObservationFixture(30_000, 'confirmed', {
    invocationTimeoutMs: 600_000,
    maxProviderRequests: 1000,
  });
  cleanup.push(() => local.close());
  const native = await createDirectReferenceHarness({
    manifest: local.prepared.manifest,
    binding: {
      version: 1,
      accountId: 'account',
      fleetDatabaseId: 'fleet-id',
      quotaDatabaseId: 'quota-id',
      exportBucketName: local.prepared.names.exportBucket,
      referenceModuleSetSha256: local.prepared.referenceModuleSetSha256,
      accountWorkersDevSubdomain: 'attested-account',
    },
    maintenanceNow: Date.now,
    applicationProbes: true,
    nodeProviderRest: true,
    ...(options.nodeResponse ? { nodeResponse: options.nodeResponse } : {}),
  });
  cleanup.push(() => native.close());
  const input = (journal: DirectRunJournal = local.journal) => ({
    prepared: local.prepared,
    journal,
    apiToken: 'inert-provider-token',
    fetch: native.fetch,
    invocation: createDirectInvocationClient({
      prepared: local.prepared,
      journal,
      accountWorkersDevSubdomain: native.binding.accountWorkersDevSubdomain,
      invokeSecret: 'inert-invoke',
      fetch: native.fetch,
    }),
  });
  return { local, native, input };
}

async function childResume(
  f: Awaited<ReturnType<typeof fixture>>,
  fault?:
    | 'export-fsync'
    | 'fence-reopen-after-settle'
    | 'fence-drain-role-split',
) {
  await f.local.journal.close();
  const script = join(f.local.directory, 'resume.mjs');
  const moduleUrl = (name: string) =>
    new URL(`../scripts/${name}.mjs`, import.meta.url).href;
  await writeFile(
    script,
    `
import {preflightDirectConformance} from ${JSON.stringify(moduleUrl('direct-credentialed-conformance-preflight'))};
import {openDirectRunState} from ${JSON.stringify(moduleUrl('direct-credentialed-run-state'))};
import {createDirectInvocationClient} from ${JSON.stringify(moduleUrl('direct-credentialed-invocation'))};
import {runDirectCredentialedScenario} from ${JSON.stringify(moduleUrl('direct-credentialed-scenario'))};
import fs from 'node:fs/promises';
import {syncBuiltinESMExports} from 'node:module';
${fault === 'export-fsync' ? `const realOpen=fs.open;let injected=false;fs.open=async(...args)=>{const handle=await realOpen(...args);if(String(args[0]).includes('/.journal-')){const realWrite=handle.writeFile.bind(handle),realSync=handle.sync.bind(handle);let proof=false;handle.writeFile=async(value,...rest)=>{const state=JSON.parse(String(value));proof=Boolean(state.scenario?.proofs.exports.a);return realWrite(value,...rest);};handle.sync=async()=>{if(proof&&!injected){injected=true;console.log('SCENARIO_FAULT export-fsync');throw new Error('fixture proof fsync failure');}return realSync();};}return handle;};syncBuiltinESMExports();` : ''}
const prepared=await preflightDirectConformance({configPath:${JSON.stringify(f.local.configPath)}});
const journal=await openDirectRunState({configPath:${JSON.stringify(f.local.configPath)},prepared,accountId:'account',mode:'resume'});
${
  fault === 'fence-reopen-after-settle'
    ? `const faulted = Object.freeze({
  ...journal,
  recordScenario: async (...args) => {
    const result = await journal.recordScenario(...args);
    const [scenario] = args;
    if (scenario.phase === 'fence-reopen' &&
        scenario.mutation?.outcome === 'returned' &&
        scenario.mutation?.action?.kind === 'tenant-fence' &&
        scenario.mutation?.action?.operation === 'reopen' &&
        scenario.mutation?.action?.role === 'a' &&
        scenario.proofs.fence.reopen.a?.after === null) {
      console.log('SCENARIO_FAULT fence-reopen-after-settle');
      process.exit(0);
    }
    return result;
  },
});`
    : fault === 'fence-drain-role-split'
      ? `const faulted = Object.freeze({
  ...journal,
  recordScenario: async (...args) => {
    const result = await journal.recordScenario(...args);
    const [scenario] = args;
    if (scenario.phase === 'fence-drain' &&
        scenario.proofs.fence.drain.a?.after != null &&
        scenario.proofs.fence.sweeps.a !== null &&
        scenario.proofs.fence.drain.b?.after === null &&
        scenario.proofs.fence.drain.b?.ordinal === null) {
      console.log('SCENARIO_FAULT fence-drain-role-split');
      process.exit(0);
    }
    return result;
  },
});`
      : 'const faulted = journal;'
}
const originalFetch=globalThis.fetch;
const fetch=async(input,init)=>{const request=new Request(input,init);const url=new URL(request.url);if(url.origin!=='https://api.cloudflare.com'&&url.origin!==${JSON.stringify(`https://${f.local.prepared.names.referenceWorker}.attested-account.workers.dev`)})throw new Error('unexpected child origin');const headers=new Headers(request.headers);headers.set('X-Direct-Fixture-Url',request.url);return originalFetch(${JSON.stringify(f.native.bridgeUrl)},{method:request.method,headers,body:request.body,signal:request.signal,redirect:'manual',duplex:'half'});};
globalThis.fetch=async()=>{throw new Error('unexpected child network');};
try {const invocation=createDirectInvocationClient({prepared,journal: faulted,accountWorkersDevSubdomain:'attested-account',invokeSecret:'inert-invoke',fetch});const result=await runDirectCredentialedScenario({prepared,journal: faulted,invocation,apiToken:'inert-provider-token',fetch});console.log('SCENARIO_RESULT '+JSON.stringify(result));}finally{await faulted.close();}
`,
  );
  const child = spawn(process.execPath, [script], {
    env: { PATH: process.env.PATH },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (chunk) => {
    stdout += String(chunk);
  });
  child.stderr.on('data', (chunk) => {
    stderr += String(chunk);
  });
  const status = await new Promise<number | null>((resolve, reject) => {
    const timer = setTimeout(() => {
      child.kill('SIGKILL');
    }, 540_000);
    child.once('error', (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once('close', (code) => {
      clearTimeout(timer);
      resolve(code);
    });
  });
  expect({ status, stderr }).toEqual({ status: 0, stderr: '' });
  const line = stdout
    .split('\n')
    .find((line) => line.startsWith('SCENARIO_RESULT '));
  if (!line) {
    if (
      fault !== 'fence-reopen-after-settle' &&
      fault !== 'fence-drain-role-split'
    )
      throw new Error(`child returned no result: ${stdout}`);
    return { result: null, stdout, stderr };
  }
  return {
    result: JSON.parse(
      line.slice('SCENARIO_RESULT '.length),
    ) as DirectScenarioOutcome,
    stdout,
    stderr,
  };
}

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
      process.stdout.write(
        `A1_SWEEP_INTERVAL ${role} ${fence.sweeps[role]?.intervalMs}\n`,
      );
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
    for (const phase of ['fence-drain', 'fence-reopen', 'fence-proofs']) {
      expect(phaseCalls[phase]).toBe(9);
      process.stdout.write(`A1_PHASE_CALLS ${phase} ${phaseCalls[phase]}\n`);
    }
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
    let armed = true;
    let mutations: string[] | undefined;
    const f = await fixture({
      nodeResponse: async (_request, response) => {
        if (
          !armed ||
          !response.headers.get('content-type')?.includes('application/json')
        )
          return response;
        const value = (await response.clone().json()) as {
          action?: string;
          result?: {
            ok?: boolean;
            after?: { state: string; transitionRevision: number };
          };
        };
        if (
          value.action !== 'tenant-fence' ||
          value.result?.after?.state !==
            (operation === 'drain' ? 'draining' : 'open')
        )
          return response;
        armed = false;
        mutations = [...f.native.world.mutationLog];
        return Response.json(
          {
            ...value,
            result: {
              ok: false,
              reason: {
                code: 'FENCE_CAS_CONFLICT',
                state,
                mutationEpoch,
                requireMutationEpoch: true,
                transitionRevision: value.result.after.transitionRevision,
                conflict: 'expectation-mismatch',
              },
            },
          },
          { status: response.status, headers: response.headers },
        );
      },
    });
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
    expect(armed).toBe(false);
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
    let armed = true;
    const f = await fixture({
      nodeResponse: async (_request, response) => {
        if (
          !armed ||
          !response.headers.get('content-type')?.includes('application/json')
        )
          return response;
        const value = (await response.clone().json()) as {
          action?: string;
          result?: {
            after?: {
              state: string;
              mutationEpoch: number;
              requireMutationEpoch: boolean;
              transitionRevision: number;
            };
          };
        };
        if (
          value.action !== 'tenant-fence' ||
          value.result?.after?.state !==
            (operation === 'drain' ? 'draining' : 'open')
        )
          return response;
        armed = false;
        return Response.json(
          {
            ...value,
            result: {
              ok: false,
              reason: {
                code: 'FENCE_CAS_CONFLICT',
                ...value.result.after,
                transitionRevision: value.result.after.transitionRevision + 1,
                conflict: 'expectation-mismatch',
              },
            },
          },
          { status: response.status, headers: response.headers },
        );
      },
    });
    expect(await runDirectCredentialedScenario(f.input())).toEqual({
      status: 'restart-required',
    });
    const child = await childResume(f);
    expect(child.result).toMatchObject({ status: 'complete' });
    expect(armed).toBe(false);
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

  const settledCall = (kind: string, outcome: string, ordinal: number) => ({
    ordinal,
    action: { kind },
    outcome,
    attempts: { provider: 0, maintenance: 0, application: 0 },
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

  it('refuses a control read that repeats an operation slot', async () => {
    const target = await scenarioJournal(DIRECT_SCENARIO_MIN_INVOCATIONS);
    await target.journal.recordScenario(
      seed('migration-start', () => {}) as DirectScenarioState,
    );
    const { invocation } = reference(target, { interrupted: false });
    const duplicating: DirectInvocationClient = {
      async invoke(action) {
        const outcome = await invocation.invoke(action);
        if (action.kind !== 'control-read') return outcome;
        const result = outcome.result as { operations: readonly unknown[] };
        return {
          ...outcome,
          result: {
            ...result,
            operations: [...result.operations, ...result.operations],
          },
        };
      },
    };
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
      seed('migration-start', (state) => {
        const call = settledCall('migration-start', 'returned', 3);
        state.lastCall = call as MutableScenario['lastCall'];
        state.mutation = call as MutableScenario['mutation'];
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
      seed('migration-interrupt', (state) => {
        const call = settledCall(
          'migration-continue',
          'injected-response-loss',
          2,
        );
        state.lastCall = call as MutableScenario['lastCall'];
        state.mutation = call as MutableScenario['mutation'];
        state.proofs.restart = frozen as MutableScenario['proofs']['restart'];
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
