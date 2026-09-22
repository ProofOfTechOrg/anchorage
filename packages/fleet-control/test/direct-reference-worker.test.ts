// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, expect, it, vi } from 'vitest';
import { createDirectInvocationClient } from '../scripts/direct-credentialed-invocation.mjs';
import {
  type DirectRunJournal,
  openDirectRunState,
} from '../scripts/direct-credentialed-run-state.mjs';
import {
  createDirectReferenceContext,
  type DirectReferenceContext,
  type DirectReferenceEnvironment,
} from '../scripts/direct-reference-context.js';
import { dispatchDirectContinuation } from '../scripts/direct-reference-continuation.js';
import {
  DIRECT_REFERENCE_PATH,
  directReferenceRequestSha256,
} from '../scripts/direct-reference-contract.mjs';
import { DirectReferenceExecutionError } from '../scripts/direct-reference-http.js';
import { DirectReferenceJournalError } from '../scripts/direct-reference-journal.js';
import { DirectReferenceTransport } from '../scripts/direct-reference-transport.js';
import { createDirectReferenceWorker } from '../scripts/direct-reference-worker.js';
import { closeFixtures } from './fixtures/cleanup.js';
import { directFixtureManifest } from './fixtures/direct-credentialed-config.js';
import { directObservationFixture } from './fixtures/direct-observations.js';
import { providerRefusalCases } from './fixtures/direct-provider-errors.js';

vi.mock('../scripts/direct-reference-context.js', () => ({
  createDirectReferenceContext: vi.fn(),
}));

afterEach(() => vi.clearAllMocks());

it.each([
  409, 500,
])('refuses a locked-resume HTTP %i instead of accepting it as fence evidence', async (status) => {
  const manifest = directFixtureManifest({ maxProviderRequests: 400 });
  const transport = new DirectReferenceTransport({
    runtime: manifest.referenceRuntime,
    startedAt: performance.now(),
    signal: AbortSignal.timeout(5_000),
    fetch: async () =>
      Response.json(
        {
          error: 'wrong refusal',
          reason: { code: 'EXECUTION_FENCED', state: 'migration-locked' },
        },
        { status },
      ),
  });
  const record = { tenantTag: manifest.names.roles.a.tenantTag };
  const context = {
    control: { getDeployment: async () => record },
    roleFor: () => 'a',
    specFor: () => ({ routeHostname: manifest.names.roles.a.routeHostname }),
    secrets: () => ({ application: { APP_PROBE_TOKEN: 'probe-token' } }),
    transport,
  } as unknown as DirectReferenceContext;
  await expect(
    dispatchDirectContinuation(
      context,
      manifest,
      {
        kind: 'tenant-continuation',
        operation: 'resume-locked',
        runId: 'run-id',
      },
      AbortSignal.timeout(5_000),
    ),
  ).rejects.toBeInstanceOf(DirectReferenceExecutionError);
  const digest = (value: string) =>
    createHash('sha256').update(value).digest('hex');
  console.log(
    'LV2_NEGATIVE locked-refusal',
    JSON.stringify({
      status,
      before: digest('503:EXECUTION_FENCED:migration-locked'),
      after: digest(`${status}:EXECUTION_FENCED:migration-locked`),
    }),
  );
});

async function closeWorkerFixtures(
  resumed: Pick<DirectRunJournal, 'close'> | undefined,
  closeLocal: () => Promise<void>,
): Promise<void> {
  await closeFixtures(
    [() => resumed?.close() ?? Promise.resolve(), closeLocal],
    [],
    'composed fixture cleanup failed',
  );
}

it.each([
  ...providerRefusalCases,
  {
    name: 'journal corruption',
    produce: async () => new DirectReferenceJournalError(),
  },
])('joins producer $name to the real client and disk journal', async ({
  name,
  produce,
}) => {
  const error = await produce();
  const local = await directObservationFixture(1000, 'absent', {
    invocationTimeoutMs: 1000,
  });
  let resumed: DirectRunJournal | undefined;
  let refusal = true;
  const failures: unknown[] = [];
  vi.mocked(createDirectReferenceContext).mockImplementation(
    async (manifest, _environment, invocation) => {
      const transport = new DirectReferenceTransport({
        runtime: manifest.referenceRuntime,
        ...invocation,
      });
      return {
        transport,
        binding: { version: 1 },
        control: { getDeployment: async () => undefined },
        journal: {
          receiveInvocation: async () => {
            if (name === 'journal corruption') throw error;
          },
          settleReceivedInvocation: async () => {},
          reconcileInvocation: async () => 'cancelled',
          readOperation: async () => {
            failures.push(transport.snapshot().failure);
            if (refusal) throw error;
            return undefined;
          },
          readInterruption: async () => undefined,
          readForceBefore: async () => undefined,
          readForceAfter: async () => undefined,
        },
      } as unknown as DirectReferenceContext;
    },
  );
  const worker = createDirectReferenceWorker(local.prepared.manifest);
  const responses: Response[] = [];
  const options = {
    prepared: local.prepared,
    accountWorkersDevSubdomain: 'direct-fixture',
    invokeSecret: 'test-invoke',
    fetch: (async (input, init) => {
      const response = await worker.fetch(new Request(input, init), {
        FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: 'test-invoke',
      } as DirectReferenceEnvironment);
      responses.push(response.clone());
      return response;
    }) as typeof fetch,
  };
  try {
    const rejected = await createDirectInvocationClient({
      ...options,
      journal: local.journal,
    })
      .invoke({ kind: 'control-read' })
      .catch((value: unknown) => value);
    const corrupt = name === 'journal corruption';
    expect(rejected).toMatchObject(
      corrupt
        ? { code: 'outcome-unknown' }
        : {
            code: 'reference-refused',
            referenceCode: 'operation-refused',
            attempts: { provider: 0, maintenance: 0, application: 0 },
          },
    );
    expect(responses.length).toBeGreaterThan(0);
    for (const response of responses) {
      expect(response.status).toBe(corrupt ? 500 : 409);
      expect(await response.json()).toEqual({
        contractVersion: 2,
        ok: false,
        error: { code: corrupt ? 'journal-state' : 'operation-refused' },
      });
      expect(response.headers.get('cache-control')).toBe('no-store');
      for (const category of ['Provider', 'Maintenance', 'Application'])
        expect(response.headers.get(`X-Direct-${category}-Attempts`)).toBe('0');
    }
    expect(failures.every((failure) => failure === null)).toBe(true);
    expect(
      JSON.parse(
        await readFile(join(local.journal.directory, 'journal.json'), 'utf8'),
      ).lastInvocation.state,
    ).toBe(corrupt ? 'pending' : 'settled');
    await local.journal.close();
    const reopen = {
      configPath: local.configPath,
      prepared: local.prepared,
      accountId: 'account',
      mode: 'resume' as const,
    };
    if (corrupt) {
      await expect(openDirectRunState(reopen)).rejects.toMatchObject({
        code: 'outcome-unknown',
      });
    } else {
      resumed = await openDirectRunState(reopen);
      refusal = false;
      await expect(
        createDirectInvocationClient({ ...options, journal: resumed }).invoke({
          kind: 'control-read',
        }),
      ).resolves.toMatchObject({
        attempts: { provider: 0, maintenance: 0, application: 0 },
      });
      expect(resumed.snapshot().lastInvocation?.state).toBe('settled');
    }
  } finally {
    await closeWorkerFixtures(resumed, () => local.close());
  }
});

it('continues composed cleanup after the first closer rejects', async () => {
  const calls: string[] = [];
  const sentinel = new Error('resumed-close-sentinel');
  const failure = closeWorkerFixtures(
    {
      async close() {
        calls.push('resumed');
        throw sentinel;
      },
    },
    async () => {
      calls.push('local');
    },
  ).catch((error: unknown) => error);
  await expect(failure).resolves.toBeInstanceOf(AggregateError);
  expect(calls).toEqual(['resumed', 'local']);
});

it('classifies a raw Promise.all dispatch rejection after attempts exhaust', async () => {
  const manifest = directFixtureManifest({ maxProviderRequests: 9 });
  const nativeFetch = vi.fn<typeof fetch>(
    async () => new Response(null, { status: 204 }),
  );
  const deployments = vi.fn();
  const sentinel = new Error('raw-journal-sentinel');
  let transport: DirectReferenceTransport | undefined;
  let overflow: unknown;
  let rawRejection: unknown;
  vi.mocked(createDirectReferenceContext).mockImplementation(
    async (_manifest, _environment, invocation) => {
      const current = new DirectReferenceTransport({
        runtime: manifest.referenceRuntime,
        startedAt: invocation.startedAt,
        signal: invocation.signal,
        fetch: nativeFetch,
      });
      transport = current;
      const peers: Array<Promise<undefined>> = [];
      let selected = false;
      return {
        transport: current,
        control: { getDeployment: deployments },
        journal: {
          receiveInvocation: async () => {},
          settleReceivedInvocation: async () => {},
          reconcileInvocation: async () => 'cancelled',
          readOperation() {
            const peer = Promise.resolve(undefined);
            peers.push(peer);
            if (selected) return peer;
            selected = true;
            return (async () => {
              await Promise.resolve();
              await Promise.all(peers);
              for (
                let ordinal = 0;
                ordinal < manifest.referenceRuntime.maxProviderRequests;
                ordinal++
              ) {
                await current.providerFetch(
                  'https://worker-budget.fixture.test/fill',
                );
              }
              try {
                await current.providerFetch(
                  'https://worker-budget.fixture.test/overflow',
                );
              } catch (error) {
                overflow = error;
              }
              rawRejection = sentinel;
              throw sentinel;
            })();
          },
        },
      } as unknown as DirectReferenceContext;
    },
  );
  const core = {
    contractVersion: 2,
    configSha256: manifest.configSha256,
    action: { kind: 'control-read' as const },
  };
  const response = await createDirectReferenceWorker(manifest).fetch(
    new Request(`https://reference.test${DIRECT_REFERENCE_PATH}`, {
      method: 'POST',
      headers: { authorization: 'Bearer test-invoke' },
      body: JSON.stringify({
        ...core,
        reservation: {
          ordinal: 1,
          requestSha256: directReferenceRequestSha256(core),
        },
      }),
    }),
    {
      FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: 'test-invoke',
    } as DirectReferenceEnvironment,
  );
  expect(rawRejection).toBe(sentinel);
  expect(overflow).toBeInstanceOf(DirectReferenceExecutionError);
  expect(overflow).toMatchObject({ code: 'budget-exhausted' });
  expect(transport?.snapshot()).toMatchObject({
    failure: 'attempts',
    providerAttempts: 9,
  });
  expect(nativeFetch).toHaveBeenCalledTimes(9);
  expect(response.status).toBe(503);
  expect(await response.json()).toMatchObject({
    ok: false,
    error: { code: 'budget-exhausted' },
  });
  expect(response.headers.get('X-Direct-Provider-Attempts')).toBe('9');
  expect(response.headers.get('X-Direct-Maintenance-Attempts')).toBe('0');
  expect(response.headers.get('X-Direct-Application-Attempts')).toBe('0');
  expect(response.headers.get('Cache-Control')).toBe('no-store');
  expect(response.headers.get('Content-Type')).toContain('application/json');
  expect(deployments).not.toHaveBeenCalled();
});
