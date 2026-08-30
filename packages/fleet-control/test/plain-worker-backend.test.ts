// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { ActiveRouteAttestationError } from '../src/active-route.js';
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
  mutationFence,
  rejectedValue,
  routeApi,
} from './fixtures/plain-worker-port-probe.js';
import {
  type FenceAssertionMode,
  PlainWorkerProvisioningApiFake,
} from './fixtures/plain-worker-provisioning-api-fake.js';

const RECEIPT_AUTHORITY = 'memory://fleet-exports/receipts/v1';
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

function installOnUpload(api: PlainWorkerProvisioningApiFake): void {
  api.onUploadCandidate = (intent) => {
    api.versions.set(intent.scriptName, [
      ...(api.versions.get(intent.scriptName) ?? []),
      ownedVersion('candidate'),
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
        id: spec.routeHostname,
        hostname: spec.routeHostname,
        service: spec.scriptName,
      },
    ]);
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
    const api = new PlainWorkerProvisioningApiFake();
    deployedCandidate(api);
    const request = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        expect(String(input)).toContain('/admin/ensure-maintenance');
        expect(init?.headers).toMatchObject({
          'Cloudflare-Workers-Version-Overrides': `${spec.scriptName}="candidate"`,
        });
        return maintenanceResponse('f'.repeat(64));
      },
    );

    await expect(
      backend(api, { fetch: request }).ensureMaintenance(
        spec,
        secrets.maintenanceAdmin,
        api.fence(),
        'candidate',
      ),
    ).rejects.toThrow(
      'maintenance response did not attest fleet specification',
    );
    expect(api.events).toEqual(['assertOwned']);
    expect(api.events).not.toContain('mutation:createDeployment');
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

  it('refuses deletion when a namespace remains after script deletion', async () => {
    const api = new PlainWorkerProvisioningApiFake();
    api.scripts.add(spec.scriptName);
    deployedCandidate(api);
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
