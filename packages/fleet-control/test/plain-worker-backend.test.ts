// SPDX-License-Identifier: Apache-2.0

import {
  APIConnectionError,
  APIConnectionTimeoutError,
  APIError,
  APIUserAbortError,
} from 'cloudflare';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ActiveRouteAttestationError } from '../src/active-route.js';
import { CloudflareApiPlainWorkerProvisioningApi } from '../src/cloudflare-api-plain-worker-provisioning-api.js';
import { CloudflareProvisioningClient } from '../src/cloudflare-client.js';
import {
  isNotFound,
  isTransientProviderError,
  sanitizeProviderError,
} from '../src/cloudflare-provider-errors.js';
import { WorkerDeploymentError } from '../src/deployment-error.js';
import { PlainWorkerBackend } from '../src/plain-worker-backend.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type {
  ApplicationR2Binding,
  DatabaseExportReceiptIdentity,
  DatabaseReference,
  DecommissionAttachmentScanInput,
  DecommissionAttachmentScanResult,
  DeploymentSecrets,
  DeploymentSpec,
  ExternalMutationFence,
  FleetRecord,
  PlainWorkerProvisioningApi,
  PlainWorkerUploadIntent,
  PlainWorkerVersionDetail,
} from '../src/types.js';
import { WranglerLoopBackend } from '../src/wrangler-loop-backend.js';
import {
  pageItems,
  recordingFetch,
  restProjection,
  testRateCoordinator,
  zoneAuthorityResponse,
} from './fixtures/cloudflare-fetch-fixture.js';
import {
  mutationFence,
  rejectedValue,
  routeApi,
} from './fixtures/plain-worker-port-probe.js';
import {
  type FenceAssertionMode,
  PlainWorkerProvisioningApiFake,
} from './fixtures/plain-worker-provisioning-api-fake.js';
import { D1State, providerWorld } from './fixtures/provider-world.js';

const RECEIPT_AUTHORITY = 'memory://fleet-exports/receipts/v1';

it('seeds optional FS8 metadata through string-bound provider SQL', async () => {
  const api = new PlainWorkerProvisioningApiFake('per-request');
  const d1 = new D1State();
  const query = api.queryDatabase.bind(api);
  api.queryDatabase = async (databaseId, sql, bindings = []) => {
    await query(databaseId, sql, bindings);
    expect(databaseId).toBe(database.id);
    const parameters = bindings.map((value) => {
      if (typeof value !== 'string')
        throw new Error('fence parameters must be strings');
      return value;
    });
    return d1.queryDatabase(sql, parameters);
  };
  await backend(api).seedDeploymentIdentity(database, 'acme', api.fence(), {
    initialExecutionFenceState: 'migration-locked',
  });
  expect(d1.queryDatabase('SELECT * FROM flowsafe_execution_fence')).toEqual([
    {
      id: 'deployment',
      state: 'migration-locked',
      proof_key: null,
      proof_run_id: null,
      updated_at: expect.any(Number),
      last_transition_request: null,
      transition_revision: 0,
      mutation_epoch: 0,
      require_mutation_epoch: 0,
      proof_table_prefix: null,
      proof_workflow_id: null,
      proof_start_token: null,
    },
  ]);
  expect(api.queries.length).toBeGreaterThan(0);
  expect(api.events.filter((event) => event === 'port-assert')).toHaveLength(
    api.queries.length,
  );
});
const RECEIPT_IDENTITY: DatabaseExportReceiptIdentity = {
  version: 1,
  authority: RECEIPT_AUTHORITY,
  databaseId: '00000000-0000-0000-0000-000000000001',
  operationId: '00000000-0000-4000-8000-000000000002',
};

// The legacy Wrangler suite remains the compatibility proof. The core-policy
// cases here prove that the core runs without a CLI adapter and seed the
// direct-API conformance fixture; they do not duplicate adapter behavior.

const spec: DeploymentSpec = {
  tenantTag: 'acme',
  environment: 'production',
  scriptName: 'acme-production',
  databaseName: 'acme-production',
  compatibilityDate: '2026-08-10',
  compatibilityFlags: ['nodejs_compat'],
  mainModule: 'worker.js',
  modules: [{ name: 'worker.js', content: 'export default { fetch() {} }' }],
  authoredBy: 'platform',
  schemaVersion: 3,
  migrations: [],
  durableObjectMigrations: [],
  durableObjectBindings: [],
  maintenanceBaseUrl: 'https://control.example.test',
  routeHostname: 'app.example.test',
};

const database: DatabaseReference = {
  id: 'database-id',
  name: spec.databaseName,
  created: true,
};

const r2Resource: ApplicationR2Binding = {
  name: 'ARTIFACTS',
  bucketName: 'acme-production-artifacts',
  jurisdiction: 'default',
};

const secrets: DeploymentSecrets = {
  deploymentIdentity: 'deployment-identity-secret-value-0001',
  maintenanceAdmin: 'maintenance-admin-secret-value-00001',
};

function backend(
  api: PlainWorkerProvisioningApiFake,
  options: {
    readonly fetch?: typeof fetch;
    readonly clock?: () => number;
    readonly wait?: (ms: number) => Promise<void>;
    readonly maintenanceRouteReadyTimeoutMs?: number;
    readonly maintenanceRouteReadyIntervalMs?: number;
  } = {},
): PlainWorkerBackend {
  return new PlainWorkerBackend({
    api,
    identityCaller: 'PlainWorkerBackend.test',
    ...options,
  });
}

function ownedVersion(id: string, deployment = spec): PlainWorkerVersionDetail {
  const digest = deploymentSpecDigest(deployment);
  return {
    versionId: id,
    tag: digest,
    bindings: [
      { type: 'd1', name: 'DB', databaseId: database.id },
      {
        type: 'plain-text',
        name: 'DEPLOYMENT_TENANT',
        value: deployment.tenantTag,
      },
      {
        type: 'plain-text',
        name: 'FLEET_ENVIRONMENT',
        value: deployment.environment,
      },
      {
        type: 'plain-text',
        name: 'FLEET_SCHEMA_VERSION',
        value: String(deployment.schemaVersion),
      },
      {
        type: 'plain-text',
        name: 'FLEET_SPEC_DIGEST',
        value: digest,
      },
      {
        type: 'plain-text',
        name: 'FLEET_INGRESS_CONTRACT',
        value: 'guarded-object-v1',
      },
    ],
  };
}

function installOnUpload(
  api: PlainWorkerProvisioningApiFake,
  deployment = spec,
): void {
  api.onUploadCandidate = (intent) => {
    api.versions.set(intent.scriptName, [
      ...(api.versions.get(intent.scriptName) ?? []),
      ownedVersion('candidate', deployment),
    ]);
    if (intent.mode === 'initial') {
      api.deployments.set(intent.scriptName, {
        versions: [{ versionId: 'candidate', percentage: 100 }],
      });
    }
  };
}

function deployedCandidate(api: PlainWorkerProvisioningApiFake): void {
  api.versions.set(spec.scriptName, [ownedVersion('candidate')]);
  api.deployments.set(spec.scriptName, {
    versions: [{ versionId: 'candidate', percentage: 100 }],
  });
}

function maintenanceResponse(digest = deploymentSpecDigest(spec)): Response {
  return Response.json({
    nextSweepAt: 2_000,
    nextPurgeAt: 3_000,
    alarmAt: 2_000,
    lastSweepAt: 1_000,
    deploymentSpecDigest: digest,
  });
}

describe('reconciled transient provisioning failures', () => {
  const wait = vi.fn(
    (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)),
  );
  beforeEach(() => {
    vi.useFakeTimers();
    wait.mockClear();
  });
  afterEach(() => vi.useRealTimers());

  function providerFailure(status = 520): APIError {
    return APIError.generate(
      status,
      undefined,
      'provider unavailable',
      new Headers(),
    );
  }

  function uploadFixture() {
    const api = new PlainWorkerProvisioningApiFake();
    installOnUpload(api);
    const upload = vi.spyOn(api, 'uploadCandidate');
    const fence = api.fence();
    const subject = backend(api, { wait });
    return {
      api,
      upload,
      fence,
      invoke: () =>
        subject.deployWorker(spec, database, secrets, undefined, fence),
    };
  }

  it('retries the Worker upload after a transient provider failure when reconciliation finds no tagged version: two uploads return candidate', async () => {
    const { api, upload, invoke } = uploadFixture();
    upload.mockResolvedValueOnce({
      status: 'failed',
      error: sanitizeProviderError(providerFailure(), []),
      cleanup: { status: 'succeeded' },
    });
    const result = invoke();
    await vi.advanceTimersByTimeAsync(1_999);
    expect(upload).toHaveBeenCalledTimes(1);
    expect(api.versions.get(spec.scriptName)).toBeUndefined();
    await vi.advanceTimersByTimeAsync(1);
    await expect(result).resolves.toEqual({
      artifactVersion: 'candidate',
      created: true,
    });
    expect(upload).toHaveBeenCalledTimes(2);
    expect(wait).toHaveBeenNthCalledWith(1, 2_000);
    expect(
      api.versions.get(spec.scriptName)?.map(({ versionId }) => versionId),
    ).toEqual(['candidate']);
  });

  it('accepts a tagged version created by an upload that answered a transient failure without a second upload: one upload returns candidate', async () => {
    const { api, upload, invoke } = uploadFixture();
    api.uploadOutcome = {
      status: 'failed',
      error: sanitizeProviderError(providerFailure(), []),
    };
    api.footprints.set(spec.scriptName, {
      scriptPresent: true,
      workersDevEnabled: true,
      previewUrlsEnabled: false,
      customDomains: [],
      zoneRoutes: [],
    });
    await expect(invoke()).resolves.toEqual({
      artifactVersion: 'candidate',
      created: true,
    });
    expect(upload).toHaveBeenCalledTimes(1);
    expect(
      api.versions.get(spec.scriptName)?.map(({ versionId }) => versionId),
    ).toEqual(['candidate']);
    expect(vi.getTimerCount()).toBe(0);
    expect(wait.mock.calls.map(([ms]) => ms)).toEqual([]);
  });

  it('fails the upload after three transient failures with the last provider error: three uploads leave no version', async () => {
    const { api, upload, invoke } = uploadFixture();
    const errors = [520, 502, 503].map((status) =>
      sanitizeProviderError(providerFailure(status), []),
    );
    for (const error of errors) {
      upload.mockResolvedValueOnce({
        status: 'failed',
        error,
        cleanup: { status: 'succeeded' },
      });
    }
    const result = rejectedValue(invoke());
    await vi.advanceTimersByTimeAsync(2_000);
    expect(upload).toHaveBeenCalledTimes(2);
    expect(wait).toHaveBeenNthCalledWith(1, 2_000);
    await vi.advanceTimersByTimeAsync(3_999);
    expect(upload).toHaveBeenCalledTimes(2);
    expect(wait).toHaveBeenNthCalledWith(1, 2_000);
    await vi.advanceTimersByTimeAsync(1);
    expect(await result).toMatchObject({
      cause: errors[2],
      resourceState: 'absent',
    });
    expect(((await result) as Error).cause).toBe(errors[2]);
    expect(upload).toHaveBeenCalledTimes(3);
    expect(api.versions.get(spec.scriptName)).toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
    expect(wait.mock.calls.map(([ms]) => ms)).toEqual([2_000, 4_000]);
  });

  it('does not retry a non-transient upload refusal: one upload leaves no version', async () => {
    const { api, upload, invoke } = uploadFixture();
    const error = sanitizeProviderError(providerFailure(403), []);
    upload.mockResolvedValueOnce({
      status: 'failed',
      error,
      cleanup: { status: 'succeeded' },
    });
    expect(await rejectedValue(invoke())).toMatchObject({ cause: error });
    expect(upload).toHaveBeenCalledTimes(1);
    expect(api.versions.get(spec.scriptName)).toBeUndefined();
    expect(vi.getTimerCount()).toBe(0);
    expect(wait.mock.calls.map(([ms]) => ms)).toEqual([]);
  });

  it('retries within the mutation fence duration: two uploads return candidate after a duration check before each upload', async () => {
    const { api, upload, fence, invoke } = uploadFixture();
    const events: string[] = [];
    Object.defineProperty(fence, 'mutationLeaseTtlMs', {
      get() {
        events.push('duration');
        return 15 * 60_000;
      },
    });
    const original =
      PlainWorkerProvisioningApiFake.prototype.uploadCandidate.bind(api);
    upload.mockImplementation(async (...args) => {
      expect(events.at(-1)).toBe('duration');
      events.push('upload');
      return upload.mock.calls.length === 1
        ? {
            status: 'failed',
            error: sanitizeProviderError(providerFailure(), []),
            cleanup: { status: 'succeeded' },
          }
        : original(...args);
    });
    const result = invoke();
    await vi.advanceTimersByTimeAsync(2_000);
    await expect(result).resolves.toEqual({
      artifactVersion: 'candidate',
      created: true,
    });
    expect(upload).toHaveBeenCalledTimes(2);
    expect(wait).toHaveBeenNthCalledWith(1, 2_000);
    expect(events.filter((event) => event === 'upload')).toHaveLength(2);
  });

  it('stops before a second upload when the mutation duration exceeds the fence: one upload leaves no version', async () => {
    const { api, upload, fence, invoke } = uploadFixture();
    let ttl = 15 * 60_000;
    Object.defineProperty(fence, 'mutationLeaseTtlMs', { get: () => ttl });
    upload.mockResolvedValueOnce({
      status: 'failed',
      error: sanitizeProviderError(providerFailure(), []),
      cleanup: { status: 'succeeded' },
    });
    const result = rejectedValue(invoke());
    await vi.advanceTimersByTimeAsync(0);
    ttl = api.maxMutationDurationMs;
    await vi.advanceTimersByTimeAsync(2_000);
    expect(String(await result)).toContain(
      'provider mutation maximum duration must be below',
    );
    expect(upload).toHaveBeenCalledTimes(1);
    expect(api.versions.get(spec.scriptName)).toBeUndefined();
  });

  it('does not retry an ambiguous upload: one upload rejects multiple tagged versions', async () => {
    const { api, upload, invoke } = uploadFixture();
    api.onUploadCandidate = () => {
      api.versions.set(spec.scriptName, [
        ownedVersion('first'),
        ownedVersion('second'),
      ]);
    };
    api.uploadOutcome = {
      status: 'failed',
      error: sanitizeProviderError(providerFailure(), []),
    };
    await rejectedValue(invoke());
    expect(upload).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
    expect(wait.mock.calls.map(([ms]) => ms)).toEqual([]);
  });

  it('does not retry when upload reconciliation fails: one upload leaves the version unknown', async () => {
    const { api, upload, invoke } = uploadFixture();
    upload.mockResolvedValueOnce({
      status: 'failed',
      error: sanitizeProviderError(providerFailure(), []),
      cleanup: { status: 'succeeded' },
    });
    vi.spyOn(api, 'listVersions')
      .mockResolvedValueOnce(undefined)
      .mockRejectedValue(new Error('inventory unavailable'));
    await rejectedValue(invoke());
    expect(upload).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
    expect(wait.mock.calls.map(([ms]) => ms)).toEqual([]);
  });

  it('preserves scratch cleanup failure across retries: two uploads install candidate but report the cleanup failure', async () => {
    const { api, upload, invoke } = uploadFixture();
    const cleanupError = new Error('scratch cleanup failed');
    upload.mockResolvedValueOnce({
      status: 'failed',
      error: sanitizeProviderError(providerFailure(), []),
      cleanup: { status: 'failed', error: cleanupError },
    });
    const result = rejectedValue(invoke());
    await vi.advanceTimersByTimeAsync(2_000);
    expect(await result).toMatchObject({
      cause: cleanupError,
      resourceState: 'present',
    });
    expect(upload).toHaveBeenCalledTimes(2);
    expect(wait).toHaveBeenNthCalledWith(1, 2_000);
    expect(
      api.versions.get(spec.scriptName)?.map(({ versionId }) => versionId),
    ).toEqual(['candidate']);
  });

  for (const operation of [
    'D1 creation',
    'R2 creation',
    'candidate-at-zero deployment',
    'promotion deployment',
  ] as const) {
    function fixture() {
      const api = new PlainWorkerProvisioningApiFake();
      const fence = api.fence();
      const subject = backend(api, { wait });
      if (operation === 'D1 creation') {
        const mutation = vi.spyOn(api, 'createDatabase');
        return {
          api,
          mutation,
          fail(error: unknown) {
            mutation.mockResolvedValueOnce({ status: 'failed', error });
          },
          invoke: () => subject.ensureDatabase(spec, fence),
          verify() {
            const databases = [...api.databases.values()];
            expect(databases).toEqual([
              {
                id: expect.stringMatching(/.+/u),
                name: spec.databaseName,
                created: false,
              },
            ]);
            // The obligation is that a provider id is not the name a consumer
            // passes, not the fake's own formula for deriving one.
            expect(databases[0]?.id).not.toBe(spec.databaseName);
          },
        };
      }
      if (operation === 'R2 creation') {
        const mutation = vi.spyOn(api, 'createR2Bucket');
        return {
          api,
          mutation,
          fail(error: unknown) {
            mutation.mockRejectedValueOnce(error);
          },
          invoke: () => subject.ensureApplicationR2Bucket(r2Resource, fence),
          verify() {
            expect([...api.buckets.values()]).toEqual([
              expect.objectContaining(r2Resource),
            ]);
          },
        };
      }
      api.versions.set(spec.scriptName, [
        ownedVersion('current'),
        ownedVersion('candidate'),
      ]);
      api.deployments.set(spec.scriptName, {
        versions:
          operation === 'promotion deployment'
            ? [
                { versionId: 'current', percentage: 100 },
                { versionId: 'candidate', percentage: 0 },
              ]
            : [{ versionId: 'current', percentage: 100 }],
      });
      const mutation = vi.spyOn(api, 'createDeployment');
      return {
        api,
        mutation,
        fail(error: unknown) {
          mutation.mockResolvedValueOnce({ status: 'failed', error });
        },
        invoke: () =>
          operation === 'promotion deployment'
            ? subject.promoteWorker(
                spec,
                {
                  allowedCurrentScriptNames: [spec.scriptName],
                  allowUnrouted: true,
                },
                undefined,
                fence,
                'candidate',
              )
            : subject.deployWorker(
                spec,
                database,
                secrets,
                undefined,
                fence,
                'candidate',
              ),
        verify() {
          expect(api.deployments.get(spec.scriptName)?.versions).toEqual(
            operation === 'promotion deployment'
              ? [{ versionId: 'candidate', percentage: 100 }]
              : [
                  { versionId: 'current', percentage: 100 },
                  { versionId: 'candidate', percentage: 0 },
                ],
          );
        },
      };
    }

    it(`retries ${operation} after a transient provider failure when reconciliation finds no effect: two calls create the intended resource or candidate deployment`, async () => {
      const { mutation, fail, invoke, verify } = fixture();
      fail(providerFailure());
      const result = invoke();
      await vi.advanceTimersByTimeAsync(1_999);
      expect(mutation).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(1);
      await result;
      expect(mutation).toHaveBeenCalledTimes(2);
      expect(wait.mock.calls.map(([ms]) => ms)).toEqual([2_000]);
      verify();
    });

    it(`fails ${operation} after three transient failures with the last provider error`, async () => {
      const { mutation, fail, invoke } = fixture();
      const errors = [520, 502, 503].map(providerFailure);
      for (const error of errors) fail(error);
      const result = rejectedValue(invoke());
      await vi.advanceTimersByTimeAsync(6_000);
      const error = await result;
      expect(error instanceof WorkerDeploymentError ? error.cause : error).toBe(
        errors[2],
      );
      expect(mutation).toHaveBeenCalledTimes(3);
      expect(vi.getTimerCount()).toBe(0);
      expect(wait.mock.calls.map(([ms]) => ms)).toEqual([2_000, 4_000]);
    });

    it(`does not retry a non-transient ${operation} refusal: one call`, async () => {
      const { mutation, fail, invoke } = fixture();
      const providerError = providerFailure(403);
      fail(providerError);
      const error = await rejectedValue(invoke());
      expect(error instanceof WorkerDeploymentError ? error.cause : error).toBe(
        providerError,
      );
      expect(mutation).toHaveBeenCalledTimes(1);
      expect(vi.getTimerCount()).toBe(0);
      expect(wait.mock.calls.map(([ms]) => ms)).toEqual([]);
    });
  }

  it.each([
    ['520', () => providerFailure(520), true],
    ['500', () => providerFailure(500), true],
    ['408', () => providerFailure(408), true],
    ['429', () => providerFailure(429), true],
    ['403', () => providerFailure(403), false],
    ['409', () => providerFailure(409), false],
    [
      'connection',
      () => new APIConnectionError({ cause: new Error('connection reset') }),
      true,
    ],
    ['timeout', () => new APIConnectionTimeoutError(), true],
    ['abort', () => new APIUserAbortError(), false],
  ])('classifies raw and sanitized %s failures as transient=%s', (_label, create, transient) => {
    const error = create();
    expect(isTransientProviderError(error)).toBe(transient);
    expect(isTransientProviderError(sanitizeProviderError(error, []))).toBe(
      transient,
    );
  });

  it.each([
    ['429', () => providerFailure(429)],
    ['timeout', () => new APIConnectionTimeoutError()],
  ])('classifies raw and sanitized %s failures as absence=false', (_label, create) => {
    const error = create();
    expect(isNotFound(error)).toBe(false);
    expect(isNotFound(sanitizeProviderError(error, []))).toBe(false);
  });

  it('does not classify arbitrary failures without status as transient', () => {
    for (const error of [
      undefined,
      null,
      false,
      new Error('validation'),
      { status: 520 },
      { name: 'CloudflareProviderError' },
    ]) {
      expect(isTransientProviderError(error)).toBe(false);
    }
  });
});

describe('maintenance route readiness', () => {
  function platform404(): Response {
    return new Response('error code: 1042', {
      status: 404,
      headers: {
        'content-type': 'text/plain; charset=UTF-8',
        server: 'cloudflare',
        'cache-control':
          'private, max-age=0, no-store, no-cache, must-revalidate, post-check=0, pre-check=0',
      },
    });
  }

  for (const operation of [
    'inspect',
    'ensureMaintenance',
    'inspect without override',
  ] as const) {
    describe(operation, () => {
      const override = operation !== 'inspect without override';
      const expectedDigest = deploymentSpecDigest(spec);
      const staleDigest = 'f'.repeat(64);
      const mismatch = override
        ? `maintenance response did not attest fleet specification digest '${expectedDigest}'`
        : "maintenance response does not match inspected Worker version 'candidate'";

      function setup(request: typeof fetch) {
        const api = new PlainWorkerProvisioningApiFake();
        deployedCandidate(api);
        if (!override) {
          api.versions.set(spec.scriptName, [
            { ...ownedVersion('candidate'), tag: 'unmatched-tag' },
          ]);
        }
        const fence = api.fence();
        const subject = backend(api, {
          fetch: request,
          maintenanceRouteReadyTimeoutMs: 5,
          maintenanceRouteReadyIntervalMs: 2,
        });
        return {
          api,
          fence,
          invoke: () =>
            operation === 'ensureMaintenance'
              ? subject.ensureMaintenance(
                  spec,
                  secrets.maintenanceAdmin,
                  fence,
                  'candidate',
                )
              : subject.inspect(
                  spec,
                  secrets.maintenanceAdmin,
                  override ? 'candidate' : undefined,
                ),
        };
      }

      it(
        override
          ? `retries ${operation} while the version override answer attests the previous version, until the candidate serves`
          : 'retries inspect without override while the answer attests the previous version, until the inspected version serves',
        async () => {
          vi.useFakeTimers();
          try {
            const requests: Request[] = [];
            const events: string[] = [];
            const digests = [staleDigest, staleDigest, expectedDigest];
            const request = vi.fn(
              async (
                input: Parameters<typeof fetch>[0],
                init?: RequestInit,
              ) => {
                requests.push(new Request(input, init));
                events.push('fetch');
                return maintenanceResponse(digests[requests.length - 1]);
              },
            );
            const { invoke, fence } = setup(request);
            vi.spyOn(fence, 'assertOwned').mockImplementation(async () => {
              events.push('fence');
            });
            const pending = invoke();
            await vi.advanceTimersByTimeAsync(0);
            expect(request).toHaveBeenCalledTimes(1);
            await vi.advanceTimersByTimeAsync(1);
            expect(request).toHaveBeenCalledTimes(1);
            await vi.advanceTimersByTimeAsync(1);
            expect(request).toHaveBeenCalledTimes(2);
            await vi.advanceTimersByTimeAsync(1);
            expect(request).toHaveBeenCalledTimes(2);
            await vi.advanceTimersByTimeAsync(1);
            expect(await pending).toMatchObject(
              operation === 'ensureMaintenance'
                ? { deploymentSpecDigest: expectedDigest }
                : { maintenance: { deploymentSpecDigest: expectedDigest } },
            );
            expect(request).toHaveBeenCalledTimes(3);
            for (const sent of requests) {
              expect(sent.url).toBe(
                new URL(
                  operation === 'ensureMaintenance'
                    ? '/admin/ensure-maintenance'
                    : '/admin/maintenance-status',
                  spec.maintenanceBaseUrl,
                ).href,
              );
              expect(sent.method).toBe(
                operation === 'ensureMaintenance' ? 'POST' : 'GET',
              );
              expect([...sent.headers]).toEqual([
                ['authorization', `Bearer ${secrets.maintenanceAdmin}`],
                ...(override
                  ? [
                      [
                        'cloudflare-workers-version-overrides',
                        `${spec.scriptName}="candidate"`,
                      ],
                    ]
                  : []),
              ]);
            }
            expect(events).toEqual(
              operation === 'ensureMaintenance'
                ? ['fence', 'fetch', 'fence', 'fetch', 'fence', 'fetch']
                : ['fetch', 'fetch', 'fetch'],
            );
          } finally {
            vi.useRealTimers();
          }
        },
      );

      it(
        override
          ? 'names the deployment wait when the override answer never attests the candidate'
          : 'names the deployment wait when the answer never attests the inspected version without override',
        async () => {
          vi.useFakeTimers();
          try {
            const request = vi.fn(async () => maintenanceResponse(staleDigest));
            const { invoke } = setup(request);
            const pending = expect(invoke()).rejects.toEqual(
              new Error(`${mismatch} within 5 ms after the deployment change`),
            );
            await vi.advanceTimersByTimeAsync(5);
            await pending;
            expect(request).toHaveBeenCalledTimes(3);
          } finally {
            vi.useRealTimers();
          }
        },
      );

      it(
        override
          ? 'passes an override answer that attests the candidate through without retrying'
          : 'passes an answer that attests the inspected version through without override or retrying',
        async () => {
          const request = vi.fn(async () => maintenanceResponse());
          const { invoke } = setup(request);
          await expect(invoke()).resolves.toBeDefined();
          expect(request).toHaveBeenCalledTimes(1);
        },
      );

      it('shares one deadline between platform 404s and stale deployment healths', async () => {
        vi.useFakeTimers();
        try {
          const request = vi.fn(async () =>
            request.mock.calls.length === 1
              ? platform404()
              : maintenanceResponse(staleDigest),
          );
          const { invoke } = setup(request);
          const pending = expect(invoke()).rejects.toEqual(
            new Error(`${mismatch} within 5 ms after the deployment change`),
          );
          await vi.advanceTimersByTimeAsync(5);
          await pending;
          expect(request).toHaveBeenCalledTimes(3);
        } finally {
          vi.useRealTimers();
        }
      });

      it('preserves the deployment wait error when the next fetch times out at the deadline', async () => {
        vi.useFakeTimers();
        try {
          const request = vi.fn(async (): Promise<Response> => {
            if (request.mock.calls.length === 1) {
              return maintenanceResponse(staleDigest);
            }
            return new Promise((_resolve, reject) => {
              setTimeout(() => reject(new Error('request timed out')), 3);
            });
          });
          const { invoke } = setup(request);
          const pending = expect(invoke()).rejects.toEqual(
            new Error(`${mismatch} within 5 ms after the deployment change`),
          );
          await vi.advanceTimersByTimeAsync(5);
          await pending;
          expect(request).toHaveBeenCalledTimes(2);
        } finally {
          vi.useRealTimers();
        }
      });

      it.each([
        [
          'invalid digest',
          { deploymentSpecDigest: 'invalid' },
          "maintenance response field 'deploymentSpecDigest' is invalid",
        ],
        [
          'invalid health',
          { deploymentSpecDigest: staleDigest, alarmAt: -1 },
          "maintenance response field 'alarmAt' is invalid",
        ],
      ])('rejects %s without retrying', async (_label, health, message) => {
        const request = vi.fn(async () => Response.json(health));
        const { invoke } = setup(request);
        await expect(invoke()).rejects.toThrow(message as string);
        expect(request).toHaveBeenCalledTimes(1);
      });

      it('preserves the missing-digest policy without retrying', async () => {
        const request = vi.fn(async () => Response.json({ alarmAt: 2_000 }));
        const { invoke } = setup(request);
        if (override) {
          await expect(invoke()).rejects.toEqual(new Error(mismatch));
        } else {
          await expect(invoke()).resolves.toMatchObject({
            maintenance: { armed: true },
          });
        }
        expect(request).toHaveBeenCalledTimes(1);
      });
    });
  }

  for (const operation of ['inspect', 'ensureMaintenance'] as const) {
    function invoke(
      subject: PlainWorkerBackend,
      api: PlainWorkerProvisioningApiFake,
    ) {
      return operation === 'inspect'
        ? subject.inspect(spec, secrets.maintenanceAdmin, 'candidate')
        : subject.ensureMaintenance(
            spec,
            secrets.maintenanceAdmin,
            api.fence(),
            'candidate',
          );
    }

    it.each([
      404, 500,
    ])(`retries platform %i pages with the same ${operation} request until health is served`, async (status) => {
      const api = new PlainWorkerProvisioningApiFake();
      deployedCandidate(api);
      const requests: Request[] = [];
      const request = vi.fn(
        async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
          requests.push(new Request(input, init));
          if (requests.length === 1)
            return new Response('platform unavailable', {
              status,
              headers: { 'content-type': 'text/plain; charset=UTF-8' },
            });
          if (requests.length === 2) {
            return new Response('<html>route unavailable</html>', {
              status,
              headers: { 'content-type': 'text/html; charset=UTF-8' },
            });
          }
          return maintenanceResponse();
        },
      );
      const result = await invoke(
        backend(api, {
          fetch: request,
          maintenanceRouteReadyTimeoutMs: 1_000,
          maintenanceRouteReadyIntervalMs: 1,
        }),
        api,
      );
      expect(result).toMatchObject(
        operation === 'inspect'
          ? { maintenance: { armed: true } }
          : { armed: true },
      );
      expect(request).toHaveBeenCalledTimes(3);
      for (const sent of requests) {
        expect(sent.url).toBe(
          new URL(
            operation === 'inspect'
              ? '/admin/maintenance-status'
              : '/admin/ensure-maintenance',
            spec.maintenanceBaseUrl,
          ).href,
        );
        expect(sent.method).toBe(operation === 'inspect' ? 'GET' : 'POST');
        expect([...sent.headers]).toEqual([
          ['authorization', `Bearer ${secrets.maintenanceAdmin}`],
          [
            'cloudflare-workers-version-overrides',
            `${spec.scriptName}="candidate"`,
          ],
        ]);
      }
      expect(new Set(requests.map(({ signal }) => signal)).size).toBe(3);
      expect(
        api.events.filter((event) => event === 'assertOwned'),
      ).toHaveLength(operation === 'inspect' ? 0 : 3);
    });

    it.each([
      [
        'JSON Worker 404',
        () => Response.json({ error: 'not_found' }, { status: 404 }),
      ],
      ...[404, 500].flatMap((status) =>
        ['text/plain', 'text/html'].flatMap((mediaType) =>
          ['cache-control', 'www-authenticate'].map(
            (marker) =>
              [
                `${status} ${mediaType} with ${marker}`,
                () =>
                  new Response('unavailable', {
                    status,
                    headers: {
                      'content-type': mediaType,
                      [marker]:
                        marker === 'cache-control' ? 'no-store' : 'Bearer',
                    },
                  }),
              ] as const,
          ),
        ),
      ),
      ['empty ingress 404', () => new Response(null, { status: 404 })],
      [
        'JSON Worker 500',
        () => Response.json({ error: 'operation-refused' }, { status: 500 }),
      ],
      [
        'Worker 401',
        () =>
          Response.json(
            { error: 'unauthorized' },
            { status: 401, headers: { 'www-authenticate': 'Bearer' } },
          ),
      ],
      [
        'authenticated plain-text 404',
        () => {
          const response = platform404();
          response.headers.set('www-authenticate', 'Bearer');
          return response;
        },
      ],
      [
        'authenticated HTML 404',
        () =>
          new Response('', {
            status: 404,
            headers: {
              'content-type': 'text/html',
              'www-authenticate': 'Bearer',
            },
          }),
      ],
      [
        'Worker 302',
        () =>
          new Response(null, {
            status: 302,
            headers: { location: 'https://redirected.invalid/elsewhere' },
          }),
      ],
    ] as const)(`passes a %s through ${operation} without retrying`, async (_label, response) => {
      const api = new PlainWorkerProvisioningApiFake();
      deployedCandidate(api);
      const request = vi.fn(async () => response());
      await expect(
        invoke(backend(api, { fetch: request }), api),
      ).rejects.toThrow(
        `maintenance request failed with HTTP ${response().status}`,
      );
      expect(request).toHaveBeenCalledTimes(1);
    });

    it(`cancels the body of a refused ${operation} maintenance response`, async () => {
      const api = new PlainWorkerProvisioningApiFake();
      deployedCandidate(api);
      const cancelled = vi.fn();
      const request = vi.fn(
        async () =>
          new Response(new ReadableStream({ cancel: cancelled }), {
            status: 302,
            headers: { location: 'https://redirected.invalid/elsewhere' },
          }),
      );

      await expect(
        invoke(backend(api, { fetch: request }), api),
      ).rejects.toThrow('maintenance request failed with HTTP 302');

      expect(request).toHaveBeenCalledTimes(1);
      expect(cancelled).toHaveBeenCalledTimes(1);
    });

    it(`names the route wait when an ${operation} retry times out at the deadline`, async () => {
      vi.useFakeTimers();
      try {
        const api = new PlainWorkerProvisioningApiFake();
        deployedCandidate(api);
        const request = vi.fn(async (): Promise<Response> => {
          if (request.mock.calls.length === 1) return platform404();
          return new Promise((_resolve, reject) => {
            setTimeout(() => reject(new Error('request timed out')), 3);
          });
        });
        const pending = expect(
          invoke(
            backend(api, {
              fetch: request,
              maintenanceRouteReadyTimeoutMs: 5,
              maintenanceRouteReadyIntervalMs: 2,
            }),
            api,
          ),
        ).rejects.toThrow(
          'maintenance request failed with HTTP 404. workers.dev route did not serve within 5 ms; a Worker fetching another Worker on the same workers.dev subdomain needs the global_fetch_strictly_public compatibility flag',
        );
        await vi.advanceTimersByTimeAsync(5);
        await pending;
        expect(request).toHaveBeenCalledTimes(2);
      } finally {
        vi.useRealTimers();
      }
    });

    it(`bounds the workers.dev route wait for ${operation}`, async () => {
      vi.useFakeTimers();
      try {
        const api = new PlainWorkerProvisioningApiFake();
        deployedCandidate(api);
        const request = vi.fn(async () => platform404());
        const pending = expect(
          invoke(
            backend(api, {
              fetch: request,
              maintenanceRouteReadyTimeoutMs: 5,
              maintenanceRouteReadyIntervalMs: 2,
            }),
            api,
          ),
        ).rejects.toThrow(
          'maintenance request failed with HTTP 404. workers.dev route did not serve within 5 ms; a Worker fetching another Worker on the same workers.dev subdomain needs the global_fetch_strictly_public compatibility flag',
        );
        await vi.advanceTimersByTimeAsync(5);
        await pending;
        expect(request).toHaveBeenCalledTimes(3);
      } finally {
        vi.useRealTimers();
      }
    });
  }

  it('defaults to a 60-second route wait with 2-second retries', async () => {
    vi.useFakeTimers();
    try {
      const api = new PlainWorkerProvisioningApiFake();
      deployedCandidate(api);
      const request = vi.fn(async () => platform404());
      const pending = expect(
        backend(api, { fetch: request }).ensureMaintenance(
          spec,
          secrets.maintenanceAdmin,
          api.fence(),
          'candidate',
        ),
      ).rejects.toThrow(
        'maintenance request failed with HTTP 404. workers.dev route did not serve within 60000 ms; a Worker fetching another Worker on the same workers.dev subdomain needs the global_fetch_strictly_public compatibility flag',
      );
      await vi.advanceTimersByTimeAsync(59_999);
      expect(request).toHaveBeenCalledTimes(30);
      await vi.advanceTimersByTimeAsync(1);
      await pending;
      expect(request).toHaveBeenCalledTimes(30);
    } finally {
      vi.useRealTimers();
    }
  });

  it.each([
    'platform 404',
    'stale digest',
  ])('refuses a maintenance retry after mutation ownership is lost (%s)', async (answer) => {
    const api = new PlainWorkerProvisioningApiFake();
    deployedCandidate(api);
    const owned = api.fence();
    vi.spyOn(owned, 'assertOwned')
      .mockResolvedValueOnce()
      .mockRejectedValue(new Error('lease lost'));
    const request = vi.fn(async () =>
      answer === 'platform 404'
        ? platform404()
        : maintenanceResponse('f'.repeat(64)),
    );
    await expect(
      backend(api, {
        fetch: request,
        maintenanceRouteReadyIntervalMs: 1,
      }).ensureMaintenance(spec, secrets.maintenanceAdmin, owned, 'candidate'),
    ).rejects.toThrow('lease lost');
    expect(request).toHaveBeenCalledTimes(1);
  });

  it.each([
    'ensureMaintenance',
    'inspect',
  ] as const)('sends the %s maintenance request with manual redirect handling', async (operation) => {
    const api = new PlainWorkerProvisioningApiFake();
    deployedCandidate(api);
    const request = vi.fn(
      async (_input: Parameters<typeof fetch>[0], _init?: RequestInit) =>
        maintenanceResponse(),
    );
    const subject = backend(api, { fetch: request });

    await (operation === 'ensureMaintenance'
      ? subject.ensureMaintenance(
          spec,
          secrets.maintenanceAdmin,
          api.fence(),
          'candidate',
        )
      : subject.inspect(spec, secrets.maintenanceAdmin, 'candidate'));

    expect(request).toHaveBeenCalledTimes(1);
    expect(request.mock.calls[0]?.[1]?.redirect).toBe('manual');
  });
});

describe('inspection across release bindings', () => {
  const target: DeploymentSpec = {
    ...spec,
    egressProxyService: 'next-egress',
    queueProducer: { binding: 'EVENTS', queueName: 'next-events' },
    application: {
      vars: [{ name: 'RELEASE', value: 'next' }],
      secrets: [],
      r2Buckets: [],
    },
  };
  const releaseBindings: PlainWorkerVersionDetail['bindings'] = [
    { type: 'service', name: 'EGRESS_PROXY', service: 'next-egress' },
    { type: 'queue-producer', name: 'EVENTS', queueName: 'next-events' },
    { type: 'plain-text', name: 'RELEASE', value: 'next' },
  ];

  it.each([
    'EGRESS_PROXY',
    'EVENTS',
    'RELEASE',
  ])('rejects target-digest %s drift through candidate and active discovery', async (missing) => {
    for (const selection of ['tag', 'explicit', 'fallback'] as const) {
      const api = new PlainWorkerProvisioningApiFake();
      const version = ownedVersion('target', target);
      api.versions.set(target.scriptName, [
        {
          ...version,
          tag: selection === 'fallback' ? 'unmatched-tag' : version.tag,
          bindings: [
            ...version.bindings,
            ...releaseBindings.filter(({ name }) => name !== missing),
          ],
        },
      ]);
      api.deployments.set(target.scriptName, {
        versions: [{ versionId: 'target', percentage: 100 }],
      });
      const request = vi.fn(async () =>
        maintenanceResponse(deploymentSpecDigest(target)),
      );
      await expect(
        backend(api, { fetch: request }).inspect(
          target,
          secrets.maintenanceAdmin,
          selection === 'explicit' ? 'target' : undefined,
        ),
      ).rejects.toThrow('different resource mapping');
      expect(request).not.toHaveBeenCalled();
    }
  });
});

function fleetRecord(): FleetRecord {
  return {
    tenantTag: spec.tenantTag,
    backend: 'plain-worker',
    environment: spec.environment,
    scriptName: spec.scriptName,
    databaseId: database.id,
    databaseName: database.name,
    schemaVersion: spec.schemaVersion,
    artifactVersion: 'candidate',
    desiredSpecDigest: deploymentSpecDigest(spec),
    durableObjectBindings: [],
    routeHostname: spec.routeHostname,
    phase: 'decommissioning',
    updatedAt: '2026-08-26T00:00:00.000Z',
  };
}

const activeRelease = {
  physicalScriptName: spec.scriptName,
  specDigest: deploymentSpecDigest(spec),
  artifactVersion: 'candidate',
  releaseSchemaVersion: spec.schemaVersion,
} as const;

describe('WranglerLoopBackend construction', () => {
  it('keeps wrapper validation order before adapter construction', () => {
    const exportStore = {
      async write() {
        throw new Error('not called');
      },
    };
    const construct = (overrides: Record<string, unknown>) =>
      new WranglerLoopBackend({
        runner: undefined as never,
        routeApi: routeApi(),
        exportDirectory: '/tmp/export',
        exportStore,
        ...overrides,
      });
    expect(() => construct({ exportDirectory: '' })).toThrow(
      'exportDirectory is required',
    );
    expect(() => construct({ exportStore: undefined })).toThrow(
      'exportStore is required',
    );
    expect(() => construct({ routeApi: undefined })).toThrow(
      'routeApi is required',
    );
    expect(() => construct({ maintenanceRequestTimeoutMs: 0 })).toThrow(
      'maintenance request timeout must be positive',
    );
    expect(() => construct({})).toThrow(TypeError);
  });
});

describe('PlainWorkerBackend core policy', () => {
  it('exposes and forwards receipt export only for a capable plain API', async () => {
    const absentApi = new PlainWorkerProvisioningApiFake();
    const absent = backend(absentApi);
    expect('databaseExportReceiptAuthority' in absent).toBe(false);
    expect('exportDatabaseReceipt' in absent).toBe(false);

    const capableApi = new PlainWorkerProvisioningApiFake();
    const legacy = vi
      .spyOn(capableApi, 'exportDatabase')
      .mockRejectedValue(new Error('legacy export must not run'));
    let authorityReads = 0;
    let methodReads = 0;
    let receiver: unknown;
    let received: DatabaseExportReceiptIdentity | undefined;
    Object.defineProperties(capableApi, {
      databaseExportReceiptAuthority: {
        configurable: true,
        get() {
          authorityReads += 1;
          return RECEIPT_AUTHORITY;
        },
      },
      exportDatabaseReceipt: {
        configurable: true,
        get() {
          methodReads += 1;
          return async function (
            this: unknown,
            identity: DatabaseExportReceiptIdentity,
            receiptFence: ExternalMutationFence,
          ) {
            receiver = this;
            received = identity;
            await receiptFence.assertOwned();
            return {
              location: 'memory://receipt',
              size: 4,
              sha256: 'a'.repeat(64),
            };
          };
        },
      },
    });
    const capable = backend(capableApi);
    expect(capable.databaseExportReceiptAuthority).toBe(RECEIPT_AUTHORITY);
    expect([authorityReads, methodReads]).toEqual([1, 1]);
    const exportReceipt = capable.exportDatabaseReceipt;
    if (!exportReceipt) throw new Error('expected receipt export capability');
    const assertOwned = vi.fn(async () => {});
    const receiptFence: ExternalMutationFence = {
      mutationLeaseTtlMs: 15 * 60_000,
      assertOwned,
    };
    await expect(
      exportReceipt(RECEIPT_IDENTITY, receiptFence),
    ).resolves.toEqual({
      databaseId: RECEIPT_IDENTITY.databaseId,
      location: 'memory://receipt',
      size: 4,
      sha256: 'a'.repeat(64),
    });
    expect(receiver).toBe(capableApi);
    expect(received).toEqual(RECEIPT_IDENTITY);
    expect(assertOwned).toHaveBeenCalledTimes(1);
    expect(legacy).not.toHaveBeenCalled();

    received = undefined;
    assertOwned.mockClear();
    const authorityFailure = await exportReceipt(
      { ...RECEIPT_IDENTITY, authority: 'memory://other/receipts/v1' },
      receiptFence,
    ).catch((error: unknown) => error);
    expect(authorityFailure).toBeInstanceOf(Error);
    expect((authorityFailure as Error).message).toBe(
      'database export receipt authority differs from configured authority',
    );
    expect((authorityFailure as Error).cause).toBeUndefined();
    expect(received).toBeUndefined();
    expect(assertOwned).not.toHaveBeenCalled();

    const incomplete = new PlainWorkerProvisioningApiFake();
    Object.defineProperty(incomplete, 'databaseExportReceiptAuthority', {
      configurable: true,
      value: RECEIPT_AUTHORITY,
    });
    const failure = (() => {
      try {
        return backend(incomplete);
      } catch (error) {
        return error;
      }
    })();
    expect(failure).toBeInstanceOf(Error);
    expect((failure as Error).message).toBe(
      'database export receipt capability is malformed',
    );
    expect((failure as Error).cause).toBeUndefined();

    for (const property of [
      'databaseExportReceiptAuthority',
      'exportDatabaseReceipt',
    ] as const) {
      const throwingApi = new PlainWorkerProvisioningApiFake();
      if (property === 'exportDatabaseReceipt') {
        Object.defineProperty(throwingApi, 'databaseExportReceiptAuthority', {
          configurable: true,
          value: RECEIPT_AUTHORITY,
        });
      }
      Object.defineProperty(throwingApi, property, {
        configurable: true,
        get() {
          throw new Error(`${property} getter must not escape`);
        },
      });
      const getterFailure = (() => {
        try {
          return backend(throwingApi);
        } catch (error) {
          return error;
        }
      })();
      expect(getterFailure).toBeInstanceOf(Error);
      expect((getterFailure as Error).message).toBe(
        'database export receipt capability is malformed',
      );
      expect((getterFailure as Error).cause).toBeUndefined();
    }
  });

  it('exposes a truly optional bound decommission attachment scan capability', async () => {
    const absentApi = new PlainWorkerProvisioningApiFake();
    const absent = backend(absentApi);
    expect(absent.advanceDecommissionAttachmentScan).toBeUndefined();
    expect('advanceDecommissionAttachmentScan' in absent).toBe(false);
    expect(Object.hasOwn(absent, 'advanceDecommissionAttachmentScan')).toBe(
      false,
    );

    const input: DecommissionAttachmentScanInput = {
      progress: {
        version: 1,
        target: { kind: 'd1', databaseId: database.id },
        evidenceSha256: 'a'.repeat(64),
        evidenceCount: 1,
        stage: 'ordinary-script-inventory',
        scriptIndex: 0,
      },
      maxProviderRequests: 12,
    };
    const result: DecommissionAttachmentScanResult = { status: 'drift' };
    let receiver: PlainWorkerProvisioningApiFake | undefined;
    let received: DecommissionAttachmentScanInput | undefined;
    const capableApi = Object.assign(new PlainWorkerProvisioningApiFake(), {
      async advanceDecommissionAttachmentScan(
        this: PlainWorkerProvisioningApiFake,
        candidate: DecommissionAttachmentScanInput,
      ): Promise<DecommissionAttachmentScanResult> {
        receiver = this;
        received = candidate;
        return result;
      },
    });
    const capable = backend(capableApi);
    expect(typeof capable.advanceDecommissionAttachmentScan).toBe('function');
    expect('advanceDecommissionAttachmentScan' in capable).toBe(true);
    expect(Object.hasOwn(capable, 'advanceDecommissionAttachmentScan')).toBe(
      true,
    );
    await expect(
      capable.advanceDecommissionAttachmentScan?.(input),
    ).resolves.toBe(result);
    expect(receiver).toBe(capableApi);
    expect(received).toBe(input);
  });

  it('splits database attachment listing from every residual proof without changing legacy order', async () => {
    type Attachment = Awaited<
      ReturnType<PlainWorkerProvisioningApi['listWorkerDatabaseAttachments']>
    >[number];
    type Assertion =
      | 'assertDatabaseDeletionResidualsRemoved'
      | 'assertDatabaseDetached';
    const tailReads = [
      'read:deployment',
      'read:versions',
      'read:domains',
      'read:footprint',
      'read:namespaces',
    ] as const;
    const tracked = (attachments: readonly Attachment[] = []) => {
      const fake = new PlainWorkerProvisioningApiFake();
      const api: PlainWorkerProvisioningApi = fake;
      const deploymentStatus = api.deploymentStatus.bind(api);
      const listVersions = api.listVersions.bind(api);
      const listCustomDomains = api.listCustomDomains.bind(api);
      const inspectOrdinaryWorkerFootprint =
        api.inspectOrdinaryWorkerFootprint.bind(api);
      const listDurableObjectNamespaces =
        api.listDurableObjectNamespaces.bind(api);
      const viewVersion = api.viewVersion.bind(api);
      vi.spyOn(api, 'listWorkerDatabaseAttachments').mockImplementation(
        async () => {
          fake.events.push('read:attachments');
          return attachments;
        },
      );
      vi.spyOn(api, 'deploymentStatus').mockImplementation(
        async (scriptName) => {
          fake.events.push('read:deployment');
          return deploymentStatus(scriptName);
        },
      );
      vi.spyOn(api, 'listVersions').mockImplementation(async (scriptName) => {
        fake.events.push('read:versions');
        return listVersions(scriptName);
      });
      vi.spyOn(api, 'listCustomDomains').mockImplementation(async () => {
        fake.events.push('read:domains');
        return listCustomDomains();
      });
      vi.spyOn(api, 'inspectOrdinaryWorkerFootprint').mockImplementation(
        async (scriptName) => {
          fake.events.push('read:footprint');
          return inspectOrdinaryWorkerFootprint(scriptName);
        },
      );
      vi.spyOn(api, 'listDurableObjectNamespaces').mockImplementation(
        async (scriptName) => {
          fake.events.push('read:namespaces');
          return listDurableObjectNamespaces(scriptName);
        },
      );
      vi.spyOn(api, 'viewVersion').mockImplementation(
        async (scriptName, versionId) => {
          fake.events.push('read:version');
          return viewVersion(scriptName, versionId);
        },
      );
      return { api: fake, fence: fake.fence(), subject: backend(fake) };
    };
    const assertDatabase = (
      assertion: Assertion,
      subject: PlainWorkerBackend,
      fence: ReturnType<PlainWorkerProvisioningApiFake['fence']>,
      record = fleetRecord(),
    ) => subject[assertion](spec, record, database, fence);
    const rejectionFrom = async (operation: Promise<unknown>) => {
      try {
        await operation;
      } catch (error) {
        return error;
      }
      throw new Error('expected database residual assertion to reject');
    };

    for (const assertion of [
      'assertDatabaseDeletionResidualsRemoved',
      'assertDatabaseDetached',
    ] as const) {
      const mismatch = tracked();
      const mismatchRefusal = await rejectionFrom(
        assertDatabase(assertion, mismatch.subject, mismatch.fence, {
          ...fleetRecord(),
          databaseName: 'foreign-database',
        }),
      );
      expect(mismatchRefusal).toBeInstanceOf(Error);
      expect((mismatchRefusal as Error).message).toBe(
        `refusing to attest database detachment for mismatched fleet record '${spec.tenantTag}:${spec.environment}'`,
      );
      expect(mismatch.api.events).toEqual(['assertOwned']);

      const success = tracked();
      await expect(
        assertDatabase(assertion, success.subject, success.fence),
      ).resolves.toBeUndefined();
      expect(success.api.events).toEqual([
        'assertOwned',
        ...(assertion === 'assertDatabaseDetached' ? ['read:attachments'] : []),
        ...tailReads,
        'assertOwned',
      ]);
    }

    const attached = tracked([
      { plane: 'ordinary', scriptName: 'foreign-worker' },
    ]);
    const attachmentRefusal = await rejectionFrom(
      assertDatabase(
        'assertDatabaseDetached',
        attached.subject,
        attached.fence,
      ),
    );
    expect(attachmentRefusal).toBeInstanceOf(Error);
    expect((attachmentRefusal as Error).message).toBe(
      `database '${database.id}' remains attached to ordinary Worker 'foreign-worker'`,
    );
    expect(attached.api.events).toEqual(['assertOwned', 'read:attachments']);

    const residuals = [
      {
        label: 'deployment',
        configure(api: PlainWorkerProvisioningApiFake) {
          api.deployments.set(spec.scriptName, {
            versions: [{ versionId: 'candidate', percentage: 100 }],
          });
        },
        message: `database '${database.id}' has a foreign or mismatched Worker footprint`,
      },
      {
        label: 'version',
        configure(api: PlainWorkerProvisioningApiFake) {
          api.versions.set(spec.scriptName, [ownedVersion('candidate')]);
        },
        message: `database '${database.id}' remains attached to owned Worker '${spec.scriptName}'`,
      },
      {
        label: 'route',
        configure(api: PlainWorkerProvisioningApiFake) {
          api.domains.push({
            id: 'residual-domain',
            hostname: spec.routeHostname,
            service: 'foreign-worker',
          });
        },
        message: `database '${database.id}' has a residual route or Durable Object namespace footprint`,
      },
      {
        label: 'footprint',
        configure(api: PlainWorkerProvisioningApiFake) {
          api.footprints.set(spec.scriptName, {
            scriptPresent: true,
            customDomains: [],
            zoneRoutes: [],
          });
        },
        message: `database '${database.id}' has an ordinary Worker footprint that the provider cannot attest`,
      },
      {
        label: 'Durable Object namespace',
        configure(api: PlainWorkerProvisioningApiFake) {
          api.namespaces.set(spec.scriptName, ['residual-namespace']);
        },
        message: `database '${database.id}' has a residual route or Durable Object namespace footprint`,
      },
    ] as const;
    for (const residual of residuals) {
      for (const assertion of [
        'assertDatabaseDeletionResidualsRemoved',
        'assertDatabaseDetached',
      ] as const) {
        const scenario = tracked();
        residual.configure(scenario.api);
        const refusal = await rejectionFrom(
          assertDatabase(assertion, scenario.subject, scenario.fence),
        );
        expect(refusal, `${assertion}: ${residual.label}`).toBeInstanceOf(
          Error,
        );
        expect(
          (refusal as Error).message,
          `${assertion}: ${residual.label}`,
        ).toBe(residual.message);
        expect(
          scenario.api.events.filter((event) => event === 'assertOwned'),
          `${assertion}: ${residual.label}`,
        ).toHaveLength(1);
        expect(
          scenario.api.events.filter((event) => event === 'read:attachments'),
        ).toHaveLength(assertion === 'assertDatabaseDetached' ? 1 : 0);
      }
    }
  });

  it.each([
    ['empty', ''],
    ['space', 'contains space'],
    ['newline', 'contains\nnewline'],
    ['non-printable', '\u007f'],
    ['too long', 'x'.repeat(129)],
    ['non-string', 42 as unknown as string],
  ])('rejects a %s identity caller', (_label, identityCaller) => {
    expect(
      () =>
        new PlainWorkerBackend({
          api: new PlainWorkerProvisioningApiFake(),
          identityCaller,
        }),
    ).toThrow(
      'plain Worker backend identityCaller must be a 1-128 character single-line token',
    );
  });

  it('accepts the identity caller boundary lengths', () => {
    expect(
      new PlainWorkerBackend({
        api: new PlainWorkerProvisioningApiFake(),
        identityCaller: 'x',
      }),
    ).toBeInstanceOf(PlainWorkerBackend);
    expect(
      new PlainWorkerBackend({
        api: new PlainWorkerProvisioningApiFake(),
        identityCaller: 'x'.repeat(128),
      }),
    ).toBeInstanceOf(PlainWorkerBackend);
  });

  it('reconciles a failed database creation by provider name', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    const providerError = new Error('create response lost');
    api.databases.set(database.id, { ...database, created: false });
    api.createDatabaseOutcome = { status: 'failed', error: providerError };

    await expect(
      backend(api).ensureDatabase(spec, mutationFence()),
    ).resolves.toEqual({ ...database, created: true });
    expect(api.queries).toHaveLength(1);
  });

  it('passes the deployment database name to inventory listing', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    api.databases.set(database.id, { ...database, created: false });

    await expect(backend(api).findDatabase(spec)).resolves.toEqual({
      ...database,
      created: false,
    });
    expect(api.listDatabaseFilters).toEqual([{ name: spec.databaseName }]);
  });

  it('refuses duplicate exact database names', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    api.databases.set('database-1', {
      id: 'database-1',
      name: spec.databaseName,
      created: false,
    });
    api.databases.set('database-2', {
      id: 'database-2',
      name: spec.databaseName,
      created: false,
    });

    await expect(backend(api).findDatabase(spec)).rejects.toThrow(
      `multiple D1 databases are named '${spec.databaseName}'`,
    );
  });

  it('refuses a matching database row whose uuid is empty', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    api.databases.set('', {
      id: '',
      name: spec.databaseName,
      created: false,
    });

    await expect(backend(api).findDatabase(spec)).rejects.toThrow(
      'D1 list result has no uuid',
    );
  });

  it.each([
    undefined,
    '',
  ])('refuses a D1 row without a usable name: %s', async (name) => {
    const api = new PlainWorkerProvisioningApiFake();
    vi.spyOn(api, 'listDatabases').mockResolvedValue([
      { databaseId: 'database', name },
    ]);
    await expect(backend(api).findDatabase(spec)).rejects.toThrow(/D1.*name/);
  });

  it('selects an exact database name from search-like inventory', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    api.databases.set('database-1', {
      id: 'database-1',
      name: spec.databaseName,
      created: false,
    });
    api.databases.set('database-2', {
      id: 'database-2',
      name: `${spec.databaseName}-canary`,
      created: false,
    });

    await expect(backend(api).findDatabase(spec)).resolves.toEqual({
      id: 'database-1',
      name: spec.databaseName,
      created: false,
    });
  });

  it('rediscovers a tagged upload after a succeeded outcome without reading its footprint', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    installOnUpload(api);
    const inspectFootprint = vi.spyOn(api, 'inspectOrdinaryWorkerFootprint');

    await expect(
      backend(api).deployWorker(
        spec,
        database,
        secrets,
        undefined,
        mutationFence(),
      ),
    ).resolves.toEqual({ artifactVersion: 'candidate', created: true });
    expect(inspectFootprint).not.toHaveBeenCalled();
  });

  it('accepts a failed upload rediscovered by tag when public access matches', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    api.uploadOutcome = { status: 'failed', error: new Error('lost') };
    api.footprints.set(spec.scriptName, {
      scriptPresent: true,
      workersDevEnabled: true,
      previewUrlsEnabled: false,
      customDomains: [],
      zoneRoutes: [],
    });
    installOnUpload(api);

    await expect(
      backend(api).deployWorker(
        spec,
        database,
        secrets,
        undefined,
        mutationFence(),
      ),
    ).resolves.toEqual({ artifactVersion: 'candidate', created: true });
  });

  it('refuses a failed upload rediscovered by tag when public access differs', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    api.versions.set(spec.scriptName, [ownedVersion('current')]);
    api.deployments.set(spec.scriptName, {
      versions: [{ versionId: 'current', percentage: 100 }],
    });
    api.uploadOutcome = { status: 'failed', error: new Error('lost') };
    api.footprints.set(spec.scriptName, {
      scriptPresent: true,
      workersDevEnabled: false,
      previewUrlsEnabled: false,
      customDomains: [],
      zoneRoutes: [],
    });
    installOnUpload(api);

    await expect(
      backend(api).deployWorker(
        spec,
        database,
        secrets,
        undefined,
        mutationFence(),
      ),
    ).rejects.toThrow(
      `reconciled Worker upload for '${spec.scriptName}' did not converge public access`,
    );
    expect(api.events).not.toContain('mutation:createDeployment');
  });

  it('uses rediscovery failure for a falsy dispatched upload rejection', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    api.uploadOutcome = { status: 'failed', error: undefined };
    const error = await rejectedValue(
      backend(api).deployWorker(
        spec,
        database,
        secrets,
        undefined,
        mutationFence(),
      ),
    );
    expect(error).toBeInstanceOf(WorkerDeploymentError);
    expect((error as WorkerDeploymentError).cause).toMatchObject({
      message: expect.stringContaining(
        'did not create exactly one new tagged Worker version',
      ),
    });
  });

  it('propagates pre-dispatch lease rejection without rollback', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    const denied = new Error('lease lost');
    await expect(
      backend(api).deployWorker(
        spec,
        database,
        secrets,
        undefined,
        mutationFence(vi.fn(async () => Promise.reject(denied))),
      ),
    ).rejects.toBe(denied);
    expect(api.events).toEqual(['port-assert']);
  });

  it.each(
    (['initial', 'staged'] as const).flatMap((mode) =>
      [
        {
          label: 'neither limit',
          specLimits: {},
          intentLimits: { cpuMs: undefined },
        },
        {
          label: 'CPU only',
          specLimits: { cpuLimitMs: 30_000 },
          intentLimits: { cpuMs: 30_000 },
        },
        {
          label: 'subrequests only',
          specLimits: { subrequestLimit: 500 },
          intentLimits: { cpuMs: undefined, subrequests: 500 },
        },
        {
          label: 'both limits',
          specLimits: { cpuLimitMs: 30_000, subrequestLimit: 500 },
          intentLimits: { cpuMs: 30_000, subrequests: 500 },
        },
      ].map((limits) => ({ mode, ...limits })),
    ),
  )('forwards $label to the shared $mode upload intent', async ({
    mode,
    specLimits,
    intentLimits,
  }) => {
    const deployment = { ...spec, ...specLimits } satisfies DeploymentSpec;
    const api = new PlainWorkerProvisioningApiFake();
    if (mode === 'staged') {
      api.versions.set(deployment.scriptName, [
        ownedVersion('current', deployment),
      ]);
      api.deployments.set(deployment.scriptName, {
        versions: [{ versionId: 'current', percentage: 100 }],
      });
    }
    installOnUpload(api, deployment);
    const upload = vi.spyOn(api, 'uploadCandidate');

    await expect(
      backend(api).deployWorker(
        deployment,
        database,
        secrets,
        undefined,
        mutationFence(),
      ),
    ).resolves.toEqual({
      artifactVersion: 'candidate',
      created: mode === 'initial',
    });
    expect(upload).toHaveBeenCalledOnce();
    expect(upload.mock.calls[0]?.[0].mode).toBe(mode);
    expect(upload.mock.calls[0]?.[0].limits).toStrictEqual(intentLimits);
  });

  it('separates initial and staged upload intents and refuses staged migrations', async () => {
    const initialApi = new PlainWorkerProvisioningApiFake();
    let initialIntent: PlainWorkerUploadIntent | undefined;
    initialApi.onUploadCandidate = (intent) => {
      initialIntent = intent;
      initialApi.versions.set(spec.scriptName, [ownedVersion('candidate')]);
      initialApi.deployments.set(spec.scriptName, {
        versions: [{ versionId: 'candidate', percentage: 100 }],
      });
    };
    await backend(initialApi).deployWorker(
      spec,
      database,
      secrets,
      undefined,
      mutationFence(),
    );
    expect(initialIntent).toMatchObject({
      mode: 'initial',
      durableObjectMigrations: [],
    });

    const stagedApi = new PlainWorkerProvisioningApiFake();
    stagedApi.versions.set(spec.scriptName, [ownedVersion('current')]);
    stagedApi.deployments.set(spec.scriptName, {
      versions: [{ versionId: 'current', percentage: 100 }],
    });
    let stagedIntent: PlainWorkerUploadIntent | undefined;
    stagedApi.onUploadCandidate = (intent) => {
      stagedIntent = intent;
      stagedApi.versions.set(spec.scriptName, [
        ownedVersion('current'),
        ownedVersion('candidate'),
      ]);
    };
    await backend(stagedApi).deployWorker(
      spec,
      database,
      secrets,
      undefined,
      mutationFence(),
    );
    expect(stagedIntent).toEqual(
      expect.not.objectContaining({
        durableObjectMigrations: expect.anything(),
      }),
    );
    expect(stagedIntent).toMatchObject({ mode: 'staged' });

    const migrating = {
      ...spec,
      durableObjectMigrations: [{ tag: 'v1', newSqliteClasses: ['State'] }],
      durableObjectBindings: [{ name: 'STATE', className: 'State' }],
    } satisfies DeploymentSpec;
    const migratingApi = new PlainWorkerProvisioningApiFake();
    migratingApi.versions.set(migrating.scriptName, [
      ownedVersion('current', migrating),
    ]);
    migratingApi.deployments.set(migrating.scriptName, {
      versions: [{ versionId: 'current', percentage: 100 }],
    });
    await expect(
      backend(migratingApi).deployWorker(
        migrating,
        database,
        secrets,
        undefined,
        mutationFence(),
      ),
    ).rejects.toThrow('pending Durable Object lifecycle migration');
  });

  it('guards promotion and confirms the attached custom domain', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    deployedCandidate(api);
    await backend(api).promoteWorker(
      spec,
      { allowedCurrentScriptNames: [spec.scriptName], allowUnrouted: true },
      undefined,
      mutationFence(),
      'candidate',
    );
    expect(api.domains).toEqual([
      {
        id: expect.stringMatching(/.+/u),
        hostname: spec.routeHostname,
        service: spec.scriptName,
      },
    ]);
    // The obligation is that a provider domain id is not the hostname
    // ownership checks read, not the fake's own formula for deriving one.
    expect(api.domains[0]?.id).not.toBe(spec.routeHostname);
  });

  it('checks maintenance digest through injected fetch', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    deployedCandidate(api);
    const request = vi.fn(async () => maintenanceResponse());
    await expect(
      backend(api, { fetch: request }).ensureMaintenance(
        spec,
        secrets.maintenanceAdmin,
        mutationFence(),
        'candidate',
      ),
    ).resolves.toMatchObject({
      armed: true,
      deploymentSpecDigest: deploymentSpecDigest(spec),
    });
  });

  it('attests only a SHA-256 active route and stamps the injected clock', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    const clock = () => Date.parse('2026-08-26T04:00:00.000Z');
    await expect(
      backend(api, { clock }).attestActiveRoute(spec),
    ).rejects.toBeInstanceOf(ActiveRouteAttestationError);
    api.activeRoute = {
      artifactVersion: 'candidate',
      specDigest: 'not-a-digest',
    };
    await expect(
      backend(api, { clock }).attestActiveRoute(spec),
    ).rejects.toBeInstanceOf(ActiveRouteAttestationError);
    api.activeRoute = {
      artifactVersion: 'candidate',
      specDigest: deploymentSpecDigest(spec),
    };
    await expect(
      backend(api, { clock }).attestActiveRoute(spec),
    ).resolves.toEqual({
      specDigest: deploymentSpecDigest(spec),
      artifactVersion: 'candidate',
      physicalScriptName: spec.scriptName,
      source: 'workers-deployments',
      observedAt: '2026-08-26T04:00:00.000Z',
    });
  });

  it.each([
    ['new Worker', false, false, true],
    ['version-only Worker', false, true, false],
    ['deployed Worker', true, true, false],
  ])('classifies post-success cleanup for a %s', async (_label, deployed, versionPresent, createdByAttempt) => {
    const api = new PlainWorkerProvisioningApiFake();
    if (versionPresent) {
      api.versions.set(spec.scriptName, [ownedVersion('current')]);
    }
    if (deployed) {
      api.deployments.set(spec.scriptName, {
        versions: [{ versionId: 'current', percentage: 100 }],
      });
    }
    installOnUpload(api);
    const cleanupError = new Error('scratch cleanup failed');
    api.uploadCleanup = { status: 'failed', error: cleanupError };

    const error = await rejectedValue(
      backend(api).deployWorker(
        spec,
        database,
        secrets,
        undefined,
        mutationFence(),
      ),
    );
    expect(error).toBeInstanceOf(WorkerDeploymentError);
    expect(error).toMatchObject({
      message: `installed Worker '${spec.scriptName}' but failed to clean up the adapter credential scratch: scratch cleanup failed`,
      createdByAttempt,
      resourceState: 'present',
    });
    expect((error as WorkerDeploymentError).cause).toBe(cleanupError);
  });

  it('uses neutral mutation-duration diagnostics', async () => {
    const zeroApi = new PlainWorkerProvisioningApiFake('per-request', 0);
    await expect(
      backend(zeroApi).ensureDatabase(spec, mutationFence()),
    ).rejects.toThrow('provider mutation maximum duration must be positive');
    const longApi = new PlainWorkerProvisioningApiFake(
      'per-request',
      15 * 60_000,
    );
    await expect(
      backend(longApi).ensureDatabase(spec, mutationFence()),
    ).rejects.toThrow(
      'provider mutation maximum duration must be below the external mutation fence lease TTL',
    );
  });

  it('refuses an R2 create before provider dispatch when the lease is already lost', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    const readback = vi.spyOn(api, 'getR2Bucket');
    const denied = new Error('lease lost');

    await expect(
      backend(api).ensureApplicationR2Bucket(
        r2Resource,
        mutationFence(vi.fn(async () => Promise.reject(denied))),
      ),
    ).rejects.toBe(denied);
    expect(readback).not.toHaveBeenCalled();
    expect(api.events).not.toContain('mutation:createR2Bucket');
  });

  it('refuses R2 readback when the lease is lost during provider creation', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    const readback = vi.spyOn(api, 'getR2Bucket');
    const create = vi
      .spyOn(api, 'createR2Bucket')
      .mockRejectedValue(new Error('create response lost'));
    const denied = new Error('lease lost');
    const assertOwned = vi
      .fn<() => Promise<void>>()
      .mockResolvedValueOnce()
      .mockRejectedValue(denied);

    await expect(
      backend(api).ensureApplicationR2Bucket(
        r2Resource,
        mutationFence(assertOwned),
      ),
    ).rejects.toBe(denied);
    expect(readback).not.toHaveBeenCalled();
    expect(create).toHaveBeenCalledOnce();
  });

  it('reconciles a duplicate R2 create while the lease remains healthy', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    const creationDate = '2026-08-26T00:00:00.000Z';
    api.createR2Bucket = vi.fn(async (resource) => {
      api.buckets.set(resource.bucketName, { ...resource, creationDate });
      throw Object.assign(new Error('duplicate'), { status: 409 });
    });

    await expect(
      backend(api).ensureApplicationR2Bucket(r2Resource, mutationFence()),
    ).resolves.toEqual({ ...r2Resource, creationDate });
  });
});

const fenceModes = ['entry', 'per-request'] satisfies FenceAssertionMode[];

function portMutation(name: string): readonly string[] {
  return ['port-assert', `mutation:${name}`];
}

function carriedMutation(
  mode: FenceAssertionMode,
  name: string,
): readonly string[] {
  return [...(mode === 'entry' ? ['port-assert'] : []), ...portMutation(name)];
}

const carrierScenarios = fenceModes.flatMap((mode) => [
  { mode, scenario: 'deleteDatabase' as const },
  { mode, scenario: 'force delete-database' as const },
  { mode, scenario: 'applyMigrations' as const },
]);

describe('PlainWorkerBackend mutation-fence carrier ordering', () => {
  it.each([
    {},
    { workersDevEnabled: false },
    { previewUrlsEnabled: false },
  ])('refuses unknown present-Worker public access in normal and force proofs %#', async (flags) => {
    const api = new PlainWorkerProvisioningApiFake();
    vi.spyOn(api, 'inspectOrdinaryWorkerFootprint').mockResolvedValue({
      scriptPresent: true,
      customDomains: [],
      zoneRoutes: [],
      ...flags,
    });
    vi.spyOn(api, 'disableOrdinaryWorkerPublicAccess').mockResolvedValue(
      undefined,
    );
    await expect(backend(api).assertTrafficRemoved(spec)).rejects.toThrow(
      'public-access footprint is incomplete',
    );
    await expect(
      backend(api).forceDecommissionStep(
        fleetRecord(),
        'remove-traffic',
        api.fence(),
      ),
    ).rejects.toThrow('public-access footprint is incomplete');
  });

  it('accepts an absent Worker without subdomain flags in ingress proofs', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    vi.spyOn(api, 'inspectOrdinaryWorkerFootprint').mockResolvedValue({
      scriptPresent: false,
      customDomains: [],
      zoneRoutes: [],
    });
    await expect(
      backend(api).assertTrafficRemoved(spec),
    ).resolves.toBeUndefined();
    await expect(
      backend(api).forceDecommissionStep(
        fleetRecord(),
        'remove-traffic',
        api.fence(),
      ),
    ).resolves.toBeUndefined();
  });

  it.each(
    carrierScenarios,
  )('$scenario records exact ordering in $mode mode', async ({
    mode,
    scenario,
  }) => {
    const api = new PlainWorkerProvisioningApiFake(mode);
    const ownedFence = api.fence();

    if (scenario === 'deleteDatabase') {
      api.databases.set(database.id, database);
      await backend(api).deleteDatabase(database, ownedFence);
      expect(api.events).toEqual(carriedMutation(mode, 'deleteDatabaseFenced'));
      return;
    }

    if (scenario === 'force delete-database') {
      api.databases.set(database.id, database);
      await backend(api).forceDecommissionStep(
        fleetRecord(),
        'delete-database',
        ownedFence,
      );
      expect(api.events).toEqual(carriedMutation(mode, 'deleteDatabaseFenced'));
      return;
    }

    if (scenario === 'applyMigrations') {
      api.failures.set('batchDatabase', new Error('batch failed'));
      const error = await rejectedValue(
        backend(api).applyMigrations(
          database,
          [{ version: 1, sql: 'CREATE TABLE example (id TEXT)' }],
          ownedFence,
        ),
      );
      expect(error).toMatchObject({
        message: 'failed to apply D1 migration 1',
      });
      expect(api.events).toEqual([
        ...carriedMutation(mode, 'queryDatabase'),
        ...carriedMutation(mode, 'queryDatabase'),
        ...carriedMutation(mode, 'batchDatabase'),
        ...carriedMutation(mode, 'queryDatabase'),
      ]);
      return;
    }

    throw new Error(`unhandled scenario '${scenario satisfies never}'`);
  });
});

// None of these paths enters withMutationFence, so entry mode must not add a
// `port-assert` event.
const directFenceScenarios = fenceModes.flatMap((mode) => [
  { mode, scenario: 'promotion attach' as const },
  { mode, scenario: 'normal traffic removal' as const },
  { mode, scenario: 'force traffic removal' as const },
  { mode, scenario: 'secret deletion' as const },
  { mode, scenario: 'maintenance request' as const },
]);

describe('PlainWorkerBackend direct mutation assertion ownership', () => {
  it.each(
    directFenceScenarios,
  )('$scenario pins backend and port assertions in $mode mode', async ({
    mode,
    scenario,
  }) => {
    const api = new PlainWorkerProvisioningApiFake(mode);
    const ownedFence = api.fence();

    if (scenario === 'promotion attach') {
      deployedCandidate(api);
      await backend(api).promoteWorker(
        spec,
        { allowedCurrentScriptNames: [spec.scriptName], allowUnrouted: true },
        undefined,
        ownedFence,
        'candidate',
      );
      expect(api.events).toEqual([
        'assertOwned',
        ...portMutation('attachCustomDomain'),
      ]);
      return;
    }

    if (scenario === 'normal traffic removal') {
      deployedCandidate(api);
      api.domains.push({
        id: 'domain-id',
        hostname: spec.routeHostname,
        service: spec.scriptName,
      });
      await backend(api).removeTraffic(
        spec,
        undefined,
        activeRelease,
        database,
        ownedFence,
      );
      expect(api.events).toEqual([
        'assertOwned',
        ...portMutation('detachCustomDomain'),
        ...portMutation('disableOrdinaryWorkerPublicAccess'),
      ]);
      return;
    }

    if (scenario === 'force traffic removal') {
      api.domains.push({
        id: 'domain-id',
        hostname: spec.routeHostname,
        service: spec.scriptName,
      });
      api.footprints.set(spec.scriptName, {
        scriptPresent: true,
        workersDevEnabled: true,
        previewUrlsEnabled: true,
        customDomains: [],
        zoneRoutes: [],
      });
      await backend(api).forceDecommissionStep(
        fleetRecord(),
        'remove-traffic',
        ownedFence,
      );
      expect(api.events).toEqual([
        ...portMutation('detachCustomDomain'),
        ...portMutation('disableOrdinaryWorkerPublicAccess'),
      ]);
      return;
    }

    if (scenario === 'secret deletion') {
      api.secretNames.set(spec.scriptName, ['A']);
      await backend(api).forceDecommissionStep(
        fleetRecord(),
        'revoke-credentials',
        ownedFence,
      );
      expect(api.events).toEqual(portMutation('deleteControlSecrets'));
      return;
    }

    if (scenario === 'maintenance request') {
      deployedCandidate(api);
      const request = vi.fn(async () => {
        // Recorded into the fake's stream so the final assertion pins that the
        // backend asserted the fence BEFORE dispatching, not merely that it
        // asserted.
        api.events.push('maintenance-dispatch');
        return maintenanceResponse();
      });
      await backend(api, { fetch: request }).ensureMaintenance(
        spec,
        secrets.maintenanceAdmin,
        ownedFence,
        'candidate',
      );
      expect(request).toHaveBeenCalledTimes(1);
      expect(api.events).toEqual(['assertOwned', 'maintenance-dispatch']);
      return;
    }

    throw new Error(`unhandled scenario '${scenario satisfies never}'`);
  });
});

describe('PlainWorkerBackend core-policy refusals', () => {
  it('refuses to upload over a script a force decommission left behind', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    api.versions.set(spec.scriptName, [ownedVersion('surviving')]);
    api.deployments.set(spec.scriptName, {
      versions: [{ versionId: 'surviving', percentage: 100 }],
    });

    // A fresh provision of the same slug mints a new D1; the surviving
    // version still binds the retired one.
    await expect(
      backend(api).deployWorker(
        spec,
        { ...database, id: 'replacement-database-id' },
        secrets,
        undefined,
        mutationFence(),
      ),
    ).rejects.toThrow(
      `refusing to upload over existing Worker '${spec.scriptName}' with drifted tenant, environment, or D1 ownership`,
    );
    expect(api.events).not.toContain('mutation:uploadCandidate');
  });

  it('refuses promotion from a disallowed route before creating a deployment', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    api.versions.set(spec.scriptName, [
      ownedVersion('current'),
      ownedVersion('candidate'),
    ]);
    api.deployments.set(spec.scriptName, {
      versions: [
        { versionId: 'current', percentage: 100 },
        { versionId: 'candidate', percentage: 0 },
      ],
    });
    api.domains.push({
      id: 'foreign-domain',
      hostname: spec.routeHostname,
      service: 'foreign-worker',
    });

    await expect(
      backend(api).promoteWorker(
        spec,
        { allowedCurrentScriptNames: [spec.scriptName], allowUnrouted: false },
        undefined,
        api.fence(),
        'candidate',
      ),
    ).rejects.toThrow(
      `custom domain '${spec.routeHostname}' is owned by unexpected Worker 'foreign-worker'`,
    );
    expect(api.events).not.toContain('mutation:createDeployment');
  });

  it('refuses a mismatched maintenance digest without promoting', async () => {
    vi.useFakeTimers();
    try {
      const api = new PlainWorkerProvisioningApiFake();
      deployedCandidate(api);
      const request = vi.fn(
        async (input: Parameters<typeof fetch>[0], init?: RequestInit) => {
          expect(String(input)).toContain('/admin/ensure-maintenance');
          expect(init?.headers).toMatchObject({
            'Cloudflare-Workers-Version-Overrides': `${spec.scriptName}="candidate"`,
          });
          return maintenanceResponse('f'.repeat(64));
        },
      );

      const pending = expect(
        backend(api, { fetch: request }).ensureMaintenance(
          spec,
          secrets.maintenanceAdmin,
          api.fence(),
          'candidate',
        ),
      ).rejects.toThrow(
        'maintenance response did not attest fleet specification',
      );
      await vi.advanceTimersByTimeAsync(60_000);
      await pending;
      expect(request).toHaveBeenCalledTimes(30);
      expect(api.events).not.toContain('mutation:createDeployment');
    } finally {
      vi.useRealTimers();
    }
  });

  it('refuses a second secret deletion after live ownership changes', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    deployedCandidate(api);
    api.secretNames.set(spec.scriptName, ['A', 'B']);
    api.onDeleteControlSecrets = () => {
      const version = ownedVersion('candidate');
      api.versions.set(spec.scriptName, [
        {
          ...version,
          bindings: version.bindings.map((binding) =>
            binding.type === 'plain-text' &&
            binding.name === 'DEPLOYMENT_TENANT'
              ? { ...binding, value: 'foreign' }
              : binding,
          ),
        },
      ]);
    };

    await expect(
      backend(api).revokeCredentials(
        spec,
        undefined,
        activeRelease,
        database,
        api.fence(),
      ),
    ).rejects.toThrow('drifted live teardown ownership');
    expect(api.events).toEqual(portMutation('deleteControlSecrets'));
    expect(api.secretNames.get(spec.scriptName)).toEqual(['B']);
  });

  it('accepts a version-list 404 on the page after the last version during the post-delete residual check', async () => {
    const world = providerWorld();
    const digest = deploymentSpecDigest(spec);
    const script = world.seedScript(spec.scriptName, {
      versions: [
        {
          versionId: 'candidate',
          tag: digest,
          bindings: [
            { type: 'd1', name: 'DB', database_id: database.id },
            ...Object.entries({
              DEPLOYMENT_TENANT: spec.tenantTag,
              FLEET_ENVIRONMENT: spec.environment,
              FLEET_SCHEMA_VERSION: String(spec.schemaVersion),
              FLEET_SPEC_DIGEST: digest,
              FLEET_INGRESS_CONTRACT: 'guarded-object-v1',
            }).map(([name, text]) => ({ type: 'plain_text', name, text })),
          ],
          mainModule: spec.mainModule,
          modules: spec.modules,
        },
      ],
      deployment: [{ versionId: 'candidate', percentage: 100 }],
      subdomain: { enabled: false, previewsEnabled: false },
    });
    const persistedVersions = script.versions.map(({ versionId, tag }) => ({
      id: versionId,
      annotations: { 'workers/tag': tag },
    }));
    const projected = restProjection(world);
    const postDeletePages: number[] = [];
    const fixture = recordingFetch((request) => {
      const url = new URL(request.url);
      const authority = zoneAuthorityResponse(url, []);
      if (authority) return authority;
      if (
        !script.present &&
        url.pathname.endsWith(`/workers/scripts/${spec.scriptName}/versions`)
      ) {
        const page = Number(url.searchParams.get('page') ?? '1');
        postDeletePages.push(page);
        return page === 1
          ? pageItems(persistedVersions, {
              page: 1,
              total_count: persistedVersions.length,
            })
          : Response.json(
              {
                success: false,
                errors: [
                  {
                    code: 10007,
                    message: 'This Worker does not exist on your account.',
                  },
                ],
                messages: [],
                result: null,
              },
              { status: 404 },
            );
      }
      return projected(request);
    });
    const client = new CloudflareProvisioningClient({
      accountId: 'account',
      apiToken: 'token',
      plane: 'plain-worker',
      rateCoordinator: testRateCoordinator(),
      fetch: fixture.fetch,
      requestTimeoutMs: 1_000,
    });
    const subject = new PlainWorkerBackend({
      api: new CloudflareApiPlainWorkerProvisioningApi({ client }),
      identityCaller: 'PlainWorkerBackend.test',
    });

    await expect(
      subject.deleteWorker(
        spec,
        undefined,
        database,
        activeRelease,
        mutationFence(),
      ),
    ).resolves.toBeUndefined();
    expect(postDeletePages).toEqual([1, 2]);
    expect(world.mutationLog).toEqual([`delete-script:${spec.scriptName}`]);
  });

  it('refuses deletion when a namespace remains after script deletion', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    api.scripts.add(spec.scriptName);
    deployedCandidate(api);
    api.footprints.set(spec.scriptName, {
      scriptPresent: true,
      workersDevEnabled: false,
      previewUrlsEnabled: false,
      customDomains: [],
      zoneRoutes: [],
    });
    api.onDeleteWorkerScript = () => {
      api.namespaces.set(spec.scriptName, ['residual-namespace']);
    };

    await expect(
      backend(api).deleteWorker(
        spec,
        undefined,
        database,
        activeRelease,
        api.fence(),
      ),
    ).rejects.toThrow(
      `Worker '${spec.scriptName}' or its custom domain remains after delete`,
    );
    expect(api.events).toEqual(portMutation('deleteWorkerScript'));
  });
});
