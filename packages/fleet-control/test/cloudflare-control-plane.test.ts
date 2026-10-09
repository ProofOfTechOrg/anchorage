// SPDX-License-Identifier: Apache-2.0

import type { D1Database, R2Bucket } from '@cloudflare/workers-types';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { advanceCleanupDeployment } from '../src/cleanup-advance.js';
import { CloudflareApiPlainWorkerBackend } from '../src/cloudflare-api-plain-worker-backend.js';
import { cloudflareFleetInventoryContext } from '../src/cloudflare-client.js';
import {
  type CloudflareAdvanceFleetAuditOptions,
  type CloudflareAdvanceFleetMigrationOptions,
  type CloudflareControlPlaneOptions,
  type CloudflareDeploymentSpec,
  type CloudflareWorkersForPlatformsControlPlaneOptions,
  type CloudflareWorkersForPlatformsDeploymentSpec,
  createCloudflareControlPlane,
  createCloudflareWorkersForPlatformsControlPlane,
  D1CloudflareApiRateCoordinator,
  D1FleetStateDatabase,
  ProvisioningError,
  R2DatabaseExportStore,
} from '../src/cloudflare-control-plane.js';
import { D1FleetInventoryRunStore } from '../src/d1-fleet-inventory-run-store.js';
import { D1FleetOperationStore } from '../src/d1-fleet-operation-store.js';
import { advanceDecommissionDeployment } from '../src/decommission-advance.js';
import { rollbackExternalRelease } from '../src/fleet.js';
import {
  abandonFleetAuditOperation,
  advanceFleetAudit,
  readFleetAuditFindingsPage,
} from '../src/fleet-audit-advance.js';
import {
  advanceFleetInventory,
  readFleetInventoryGeneration,
} from '../src/fleet-inventory-advance.js';
import {
  canonicalFleetInventoryRunOptions,
  emptyFleetInventoryRowCounts,
  type FleetInventoryProviderContext,
  type FleetInventoryStageInput,
} from '../src/fleet-inventory-state.js';
import {
  abandonFleetMigrationOperation,
  advanceFleetMigration,
  readFleetMigrationItemsPage,
} from '../src/fleet-migration-advance.js';
import { provisionDeployment } from '../src/provision.js';
import { D1FleetStateStore } from '../src/state-store.js';
import type { FleetRecord, FleetStateLease } from '../src/types.js';
import { WorkersForPlatformsBackend } from '../src/workers-for-platforms-backend.js';
import {
  initialSpec,
  routeAttestation,
  sharedSecrets,
} from './fixtures/plain-worker-harnesses.js';
import { nodeWorkerStreams } from './fixtures/worker-streams.js';

const constructed = vi.hoisted(() => ({
  database: vi.fn(),
  fleetStore: vi.fn(),
  inventoryStore: vi.fn(),
  operationStore: vi.fn(),
  quota: vi.fn(),
  client: vi.fn(),
  backend: vi.fn(),
  wfpBackend: vi.fn(),
}));

vi.mock('../src/d1-fleet-state-database.js', async (importOriginal) => {
  const actual =
    await importOriginal<typeof import('../src/d1-fleet-state-database.js')>();
  return {
    ...actual,
    D1FleetStateDatabase: class extends actual.D1FleetStateDatabase {
      constructor(
        ...args: ConstructorParameters<typeof actual.D1FleetStateDatabase>
      ) {
        super(...args);
        constructed.database(this, ...args);
      }
    },
  };
});
vi.mock('../src/state-store.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/state-store.js')>();
  return {
    ...actual,
    D1FleetStateStore: class extends actual.D1FleetStateStore {
      constructor(
        ...args: ConstructorParameters<typeof actual.D1FleetStateStore>
      ) {
        super(...args);
        constructed.fleetStore(this, ...args);
      }
    },
  };
});
vi.mock('../src/d1-fleet-inventory-run-store.js', async (importOriginal) => {
  const actual =
    await importOriginal<
      typeof import('../src/d1-fleet-inventory-run-store.js')
    >();
  return {
    ...actual,
    D1FleetInventoryRunStore: class extends actual.D1FleetInventoryRunStore {
      constructor(
        ...args: ConstructorParameters<typeof actual.D1FleetInventoryRunStore>
      ) {
        super(...args);
        constructed.inventoryStore(this, ...args);
      }
    },
  };
});
vi.mock('../src/d1-fleet-operation-store.js', async (importOriginal) => {
  const actual =
    await importOriginal<typeof import('../src/d1-fleet-operation-store.js')>();
  return {
    ...actual,
    D1FleetOperationStore: class extends actual.D1FleetOperationStore {
      constructor(
        ...args: ConstructorParameters<typeof actual.D1FleetOperationStore>
      ) {
        super(...args);
        constructed.operationStore(this, ...args);
      }
    },
  };
});
vi.mock('../src/cloudflare-rate-coordinator.js', async (importOriginal) => {
  const actual =
    await importOriginal<
      typeof import('../src/cloudflare-rate-coordinator.js')
    >();
  return {
    ...actual,
    D1CloudflareApiRateCoordinator: class extends actual.D1CloudflareApiRateCoordinator {
      constructor(
        ...args: ConstructorParameters<
          typeof actual.D1CloudflareApiRateCoordinator
        >
      ) {
        super(...args);
        constructed.quota(this, ...args);
      }
    },
  };
});
vi.mock('../src/cloudflare-client.js', async (importOriginal) => {
  const actual =
    await importOriginal<typeof import('../src/cloudflare-client.js')>();
  return {
    ...actual,
    CloudflareProvisioningClient: class extends actual.CloudflareProvisioningClient {
      constructor(
        ...args: ConstructorParameters<
          typeof actual.CloudflareProvisioningClient
        >
      ) {
        super(...args);
        constructed.client(this, ...args);
      }
    },
    cloudflareFleetInventoryContext: vi.fn(),
  };
});
vi.mock(
  '../src/cloudflare-api-plain-worker-backend.js',
  async (importOriginal) => {
    const actual =
      await importOriginal<
        typeof import('../src/cloudflare-api-plain-worker-backend.js')
      >();
    return {
      ...actual,
      CloudflareApiPlainWorkerBackend: class extends actual.CloudflareApiPlainWorkerBackend {
        constructor(
          ...args: ConstructorParameters<
            typeof actual.CloudflareApiPlainWorkerBackend
          >
        ) {
          super(...args);
          constructed.backend(this, ...args);
        }
      },
    };
  },
);
vi.mock('../src/provision.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../src/provision.js')>()),
  provisionDeployment: vi.fn(),
}));
vi.mock('../src/workers-for-platforms-backend.js', async (importOriginal) => {
  const actual =
    await importOriginal<
      typeof import('../src/workers-for-platforms-backend.js')
    >();
  return {
    ...actual,
    WorkersForPlatformsBackend: class extends actual.WorkersForPlatformsBackend {
      constructor(
        ...args: ConstructorParameters<typeof actual.WorkersForPlatformsBackend>
      ) {
        super(...args);
        constructed.wfpBackend(this, ...args);
      }
    },
  };
});
vi.mock('../src/fleet.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../src/fleet.js')>()),
  rollbackExternalRelease: vi.fn(),
}));
vi.mock('../src/cleanup-advance.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../src/cleanup-advance.js')>()),
  advanceCleanupDeployment: vi.fn(),
}));
vi.mock('../src/decommission-advance.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../src/decommission-advance.js')>()),
  advanceDecommissionDeployment: vi.fn(),
}));
vi.mock('../src/fleet-inventory-advance.js', async (importOriginal) => ({
  ...(await importOriginal<
    typeof import('../src/fleet-inventory-advance.js')
  >()),
  advanceFleetInventory: vi.fn(),
  readFleetInventoryGeneration: vi.fn(),
}));
vi.mock('../src/fleet-audit-advance.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../src/fleet-audit-advance.js')>()),
  advanceFleetAudit: vi.fn(),
  readFleetAuditFindingsPage: vi.fn(),
  abandonFleetAuditOperation: vi.fn(),
}));
vi.mock('../src/fleet-migration-advance.js', async (importOriginal) => ({
  ...(await importOriginal<
    typeof import('../src/fleet-migration-advance.js')
  >()),
  advanceFleetMigration: vi.fn(),
  readFleetMigrationItemsPage: vi.fn(),
  abandonFleetMigrationOperation: vi.fn(),
}));

const OPERATION_ID = '123e4567-e89b-42d3-a456-426614174000';
const SPEC: CloudflareDeploymentSpec = {
  ...initialSpec(),
  authoredBy: 'platform',
};
const RECORD: FleetRecord = {
  tenantTag: SPEC.tenantTag,
  environment: SPEC.environment,
  scriptName: SPEC.scriptName,
  databaseName: SPEC.databaseName,
  databaseId: 'database-1',
  backend: 'plain-worker',
  schemaVersion: SPEC.schemaVersion,
  artifactVersion: 'artifact-1',
  desiredSpecDigest: 'a'.repeat(64),
  durableObjectBindings: [],
  routeHostname: SPEC.routeHostname,
  phase: 'ready',
  updatedAt: '2026-09-09T00:00:00.000Z',
};
const TOKEN = {
  version: 1 as const,
  tenantTag: SPEC.tenantTag,
  environment: SPEC.environment,
  operationId: OPERATION_ID,
  revision: 1,
};
const WFP_SPEC: CloudflareWorkersForPlatformsDeploymentSpec = {
  ...SPEC,
  authoredBy: 'external',
  durableObjectBindings: [{ name: 'RUNNER', className: 'Runner' }],
};
const WFP_RESOURCES = {
  maintenanceCapabilityPublicKey: 'test-verifier',
  sharedOutboundWorkerName: 'fleet-outbound',
  auditQueueName: 'fleet-audit',
  stateWorker: {
    scriptName: 'fleet-state',
    artifactVersion: 'state-v1',
    artifactDigest: 'b'.repeat(64),
    plane: 'dispatch' as const,
    dispatchNamespace: 'fleet-dispatch',
    durableObjectBindings: [],
    namespaceIds: [],
  },
};
const WFP_RECORD: FleetRecord = {
  ...RECORD,
  backend: 'workers-for-platforms',
  platformResources: WFP_RESOURCES,
};

function required<T>(value: T | undefined): T {
  expect(value).toBeDefined();
  if (value === undefined) throw new Error('missing recorded call');
  return value;
}

function binding(): D1Database {
  return { prepare: vi.fn(), batch: vi.fn() } as unknown as D1Database;
}

function hostOptions(): CloudflareControlPlaneOptions {
  return {
    accountId: 'account-1',
    apiToken: 'provider-token',
    fleetDatabase: binding(),
    quotaDatabase: binding(),
    quotaScope: 'account-token-quota',
    databaseExports: {
      bucket: {
        put: vi.fn(),
        get: vi.fn(),
        delete: vi.fn(),
      } as unknown as R2Bucket,
      bucketName: 'fleet-exports',
      streams: nodeWorkerStreams,
      randomUUID: () => OPERATION_ID,
    },
    randomUUID: () => OPERATION_ID,
  };
}

function wfpHostOptions(): CloudflareWorkersForPlatformsControlPlaneOptions {
  return {
    ...hostOptions(),
    dispatchNamespace: 'fleet-dispatch',
    hostRoutingKvId: 'fleet-hosts',
    auditQueueName: 'fleet-audit',
    sharedOutboundWorkerName: 'fleet-outbound',
    stateEgressRootSecret: 'test-state-egress-root-secret-00000001',
    platformProfileFor() {
      throw new Error('platform profile was not configured by this test');
    },
  };
}

function stageInput(
  overrides: Partial<FleetInventoryStageInput['options']> = {},
): FleetInventoryStageInput {
  return {
    stage: { step: 'ordinary-scripts' },
    options: canonicalFleetInventoryRunOptions({
      databaseNamePrefix: 'fleet-',
      scriptNamePrefix: 'fleet-',
      includeDispatchNamespace: false,
      ...overrides,
    }),
    progress: {
      providerRequests: 0,
      generation: 1,
      revision: 0,
      stage: { step: 'ordinary-scripts' },
      stagedCounts: emptyFleetInventoryRowCounts(),
      factCount: 0,
    },
    maxProviderRequests: 9,
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  vi.mocked(cloudflareFleetInventoryContext).mockImplementation(() => ({
    advanceStage: vi.fn(async () => ({
      rows: [],
      facts: [],
      nextStage: { step: 'finalize' as const },
      providerRequests: 1,
      diagnostics: [],
    })),
  }));
});

describe('Cloudflare control-plane composition with real constructors and mocked coordinator forwarding', () => {
  it('selects ordinary client construction and concrete D1 stores with inventory pin release wiring', () => {
    const options = {
      ...hostOptions(),
      leaseTtlMs: 10_000,
      leaseRenewalIntervalMs: 1_000,
      concurrency: 2,
      requestTimeoutMs: 4_000,
      fetch: vi.fn<typeof fetch>(),
      maintenanceFetch: vi.fn<typeof fetch>(),
      maintenanceRequestTimeoutMs: 3_000,
      clock: () => 99,
      plane: 'workers-for-platforms',
      dispatchNamespace: 'forged-dispatch',
      rateCoordinator: {},
      exportStore: {},
      client: {},
      backend: {},
      store: {},
      inventoryStore: {},
      operationStore: {},
    };
    createCloudflareControlPlane(options);
    const [database, fleetBinding] = required(
      constructed.database.mock.calls[0],
    );
    const [fleetStore, fleetAdapter, fleetOptions] = required(
      constructed.fleetStore.mock.calls[0],
    );
    const [inventoryStore, inventoryAdapter, inventoryOptions] = required(
      constructed.inventoryStore.mock.calls[0],
    );
    const [operationStore, operationAdapter, operationOptions] = required(
      constructed.operationStore.mock.calls[0],
    );
    const [quota, quotaBinding, quotaOptions] = required(
      constructed.quota.mock.calls[0],
    );
    const [client, clientOptions] = required(constructed.client.mock.calls[0]);
    const [backend, backendOptions] = required(
      constructed.backend.mock.calls[0],
    );
    expect(database).toBeInstanceOf(D1FleetStateDatabase);
    expect(fleetBinding).toBe(options.fleetDatabase);
    expect([fleetAdapter, inventoryAdapter, operationAdapter]).toEqual([
      database,
      database,
      database,
    ]);
    expect(fleetStore).toBeInstanceOf(D1FleetStateStore);
    expect(inventoryStore).toBeInstanceOf(D1FleetInventoryRunStore);
    expect(operationStore).toBeInstanceOf(D1FleetOperationStore);
    expect(fleetOptions).toEqual({
      accountId: options.accountId,
      leaseTtlMs: 10_000,
      leaseRenewalIntervalMs: 1_000,
    });
    expect(inventoryOptions).toEqual(fleetOptions);
    expect(operationOptions).toEqual({ ...fleetOptions, inventoryStore });
    expect(quota).toBeInstanceOf(D1CloudflareApiRateCoordinator);
    expect(quotaBinding).toBe(options.quotaDatabase);
    expect(quotaOptions).toEqual({ quotaScope: options.quotaScope });
    expect(clientOptions).toEqual({
      accountId: options.accountId,
      apiToken: options.apiToken,
      plane: 'plain-worker',
      rateCoordinator: quota,
      exportStore: expect.any(R2DatabaseExportStore),
      concurrency: 2,
      requestTimeoutMs: 4_000,
      fetch: options.fetch,
    });
    expect(backend).toBeInstanceOf(CloudflareApiPlainWorkerBackend);
    expect(backendOptions).toEqual({
      client,
      fetch: options.maintenanceFetch,
      maintenanceRequestTimeoutMs: 3_000,
      clock: expect.any(Function),
    });
    expect(backendOptions.clock()).toBe(99);
    expect(options.fleetDatabase.prepare).not.toHaveBeenCalled();
    expect(options.quotaDatabase.prepare).not.toHaveBeenCalled();
    expect(options.fetch).not.toHaveBeenCalled();
    expect(options.maintenanceFetch).not.toHaveBeenCalled();
  });

  it('returns a frozen plain object without raw client, backend, or store capabilities', () => {
    const control = createCloudflareControlPlane(hostOptions());
    expect(Object.getPrototypeOf(control)).toBe(Object.prototype);
    expect(Object.isFrozen(control)).toBe(true);
    expect(Reflect.ownKeys(control).sort()).toEqual(
      [
        'abandonFleetAuditOperation',
        'abandonFleetMigrationOperation',
        'advanceCleanupDeployment',
        'advanceDecommissionDeployment',
        'advanceFleetAudit',
        'advanceFleetInventory',
        'advanceFleetMigration',
        'getDeployment',
        'latestFinalizedInventoryGeneration',
        'provisionDeployment',
        'pruneCleanupReceipts',
        'pruneFleetOperations',
        'pruneInventoryGenerations',
        'readCleanupReceipt',
        'readFleetAuditFindingsPage',
        'readFleetInventoryGeneration',
        'readFleetMigrationItemsPage',
      ].sort(),
    );
    expect(
      Object.values(control).every((value) => typeof value === 'function'),
    ).toBe(true);
    expect(() => Object.assign(control, { backend: {} })).toThrow(TypeError);
  });

  it('forces bounded failure cleanup and preserves the coordinator error and cleanup outcome', async () => {
    const control = createCloudflareControlPlane(hostOptions());
    const cleanup = { status: 'pending' as const, token: TOKEN };
    const failure = new ProvisioningError(
      'failed',
      new Error('provider failure'),
      [],
      cleanup,
    );
    vi.mocked(provisionDeployment).mockRejectedValueOnce(failure);
    const input = {
      spec: SPEC,
      secrets: sharedSecrets,
      initialExecutionFenceState: 'open' as const,
      routeAttestation,
      clock: () => 100,
      failureCleanup: 'drain',
      backend: {},
      store: {},
      finalizedStateProvider: {},
    };
    await expect(control.provisionDeployment(input)).rejects.toBe(failure);
    expect(failure.cleanup).toBe(cleanup);
    const forwarded = required(vi.mocked(provisionDeployment).mock.calls[0])[0];
    expect(forwarded).toEqual({
      backend: required(constructed.backend.mock.calls[0])[0],
      store: required(constructed.fleetStore.mock.calls[0])[0],
      spec: SPEC,
      secrets: sharedSecrets,
      initialExecutionFenceState: 'open',
      routeAttestation,
      failureCleanup: 'bounded',
      clock: expect.any(Function),
    });
    expect(forwarded.clock?.()).toBe(100);
    expect(advanceCleanupDeployment).not.toHaveBeenCalled();
  });

  it.each([
    'advanceCleanupDeployment',
    'advanceDecommissionDeployment',
  ] as const)('forwards %s once with captured UUID authority and call-local inputs', async (method) => {
    const host = {
      ...hostOptions(),
      randomUUID() {
        expect(this).toBe(host);
        return OPERATION_ID;
      },
      clock() {
        expect(this).toBe(host);
        return 90;
      },
    };
    const control = createCloudflareControlPlane(host);
    const coordinator =
      method === 'advanceCleanupDeployment'
        ? vi.mocked(advanceCleanupDeployment)
        : vi.mocked(advanceDecommissionDeployment);
    const result = { status: 'pending' as const, token: TOKEN };
    coordinator.mockResolvedValueOnce(result);
    const action = { kind: 'continue' as const, token: TOKEN };
    const input = {
      spec: SPEC,
      action,
      maxProviderRequests: 9,
      signal: new AbortController().signal,
      clock() {
        expect(this).toBe(input);
        return 101;
      },
      randomUUID: () => 'forged-uuid',
      backend: {},
      store: {},
    };
    host.randomUUID = () => 'mutated-uuid';
    host.clock = () => 999;
    Reflect.set(host, 'fleetDatabase', binding());
    const invoke = control[method];
    await expect(invoke(input)).resolves.toBe(result);
    const forwarded = required(coordinator.mock.calls[0])[0];
    expect(forwarded).toEqual({
      backend: required(constructed.backend.mock.calls[0])[0],
      store: required(constructed.fleetStore.mock.calls[0])[0],
      spec: SPEC,
      action,
      maxProviderRequests: 9,
      signal: input.signal,
      clock: expect.any(Function),
      randomUUID: expect.any(Function),
    });
    expect(forwarded.randomUUID()).toBe(OPERATION_ID);
    expect(forwarded.clock?.()).toBe(101);
    expect(required(constructed.backend.mock.calls[0])[1].clock()).toBe(90);
    expect(coordinator).toHaveBeenCalledTimes(1);
  });

  it('uses the runtime UUID source when the host does not provide one', async () => {
    const options = hostOptions();
    const control = createCloudflareControlPlane({
      ...options,
      randomUUID: undefined,
    });
    await control.advanceCleanupDeployment({
      spec: SPEC,
      action: { kind: 'start' },
      maxProviderRequests: 9,
    });
    const randomUUID = required(
      vi.mocked(advanceCleanupDeployment).mock.calls[0],
    )[0].randomUUID;
    expect(randomUUID()).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u,
    );
  });

  it.each([
    { ...SPEC, authoredBy: 'external' },
    {
      ...SPEC,
      durableObjectBindings: [
        {
          name: 'RUNNER',
          className: 'Runner',
          scriptName: 'state',
          dispatchNamespace: 'platform',
        },
      ],
    },
  ])('rejects nonordinary runtime specifications before lifecycle forwarding', async (invalid) => {
    const control = createCloudflareControlPlane(hostOptions());
    const spec = invalid as CloudflareDeploymentSpec;
    await expect(
      control.provisionDeployment({
        spec,
        secrets: sharedSecrets,
        initialExecutionFenceState: 'open',
      }),
    ).rejects.toThrow(TypeError);
    await expect(
      control.advanceCleanupDeployment({
        spec,
        action: { kind: 'start' },
        maxProviderRequests: 9,
      }),
    ).rejects.toThrow(TypeError);
    await expect(
      control.advanceDecommissionDeployment({
        spec,
        action: { kind: 'start' },
        maxProviderRequests: 9,
      }),
    ).rejects.toThrow(TypeError);
    expect(provisionDeployment).not.toHaveBeenCalled();
    expect(advanceCleanupDeployment).not.toHaveBeenCalled();
    expect(advanceDecommissionDeployment).not.toHaveBeenCalled();
  });

  it('creates an inventory context per invocation and strips dispatch and host-routing start options', async () => {
    const control = createCloudflareControlPlane(hostOptions());
    const result = {
      status: 'pending' as const,
      token: { version: 1 as const, operationId: OPERATION_ID, revision: 1 },
    };
    vi.mocked(advanceFleetInventory).mockResolvedValue(result);
    const input = {
      action: {
        kind: 'start' as const,
        operationId: OPERATION_ID,
        options: {
          databaseNamePrefix: 'fleet-',
          scriptNamePrefix: 'fleet-',
          includeR2Buckets: true,
          includeDispatchNamespace: true,
          hostRoutingKvId: 'root-hosts',
        },
      },
      maxProviderRequests: 9,
      maxStagedRowsPerChunk: 13,
      signal: new AbortController().signal,
      context: {},
      store: {},
    };
    await expect(control.advanceFleetInventory(input)).resolves.toBe(result);
    await control.advanceFleetInventory({
      ...input,
      action: { kind: 'continue', token: result.token },
    });
    const first = required(vi.mocked(advanceFleetInventory).mock.calls[0])[0];
    const second = required(vi.mocked(advanceFleetInventory).mock.calls[1])[0];
    expect(first).toEqual({
      context: { advanceStage: expect.any(Function) },
      store: required(constructed.inventoryStore.mock.calls[0])[0],
      expectedScope: { includeDispatchNamespace: false },
      action: {
        kind: 'start',
        operationId: OPERATION_ID,
        options: {
          databaseNamePrefix: 'fleet-',
          scriptNamePrefix: 'fleet-',
          includeR2Buckets: true,
          includeDispatchNamespace: false,
        },
      },
      maxProviderRequests: 9,
      maxStagedRowsPerChunk: 13,
      signal: input.signal,
    });
    expect(first.context).not.toBe(second.context);
    expect(second.action).toEqual({ kind: 'continue', token: result.token });
    expect(cloudflareFleetInventoryContext).toHaveBeenCalledTimes(2);
    expect(
      required(vi.mocked(cloudflareFleetInventoryContext).mock.results[0])
        .value,
    ).not.toBe(
      required(vi.mocked(cloudflareFleetInventoryContext).mock.results[1])
        .value,
    );
    expect(cloudflareFleetInventoryContext).toHaveBeenNthCalledWith(
      1,
      required(constructed.client.mock.calls[0])[0],
    );
    expect(cloudflareFleetInventoryContext).toHaveBeenNthCalledWith(
      2,
      required(constructed.client.mock.calls[0])[0],
    );
  });

  it('preserves the private provider context receiver for ordinary stage advancement', async () => {
    const context: FleetInventoryProviderContext = {
      async advanceStage(input) {
        expect(this).toBe(context);
        expect(input.options.includeDispatchNamespace).toBe(false);
        return {
          rows: [],
          facts: [],
          nextStage: { step: 'finalize' as const },
          providerRequests: 1,
          diagnostics: [],
        };
      },
    };
    vi.mocked(cloudflareFleetInventoryContext).mockReturnValue(context);
    const control = createCloudflareControlPlane(hostOptions());
    await control.advanceFleetInventory({
      action: { kind: 'continue', token: TOKEN },
      maxProviderRequests: 9,
    });
    const forwarded = required(
      vi.mocked(advanceFleetInventory).mock.calls[0],
    )[0];
    await expect(
      forwarded.context.advanceStage(stageInput()),
    ).resolves.toMatchObject({ providerRequests: 1 });
  });

  it('forwards audit callbacks with captured receivers and rejects nonordinary callback specs and records', async () => {
    const control = createCloudflareControlPlane(hostOptions());
    let resolvedSpec = SPEC;
    const input: CloudflareAdvanceFleetAuditOptions = {
      action: {
        kind: 'start',
        operationId: OPERATION_ID,
        records: [RECORD],
        staleAfterMs: 100,
      },
      specFor(record) {
        expect(this).toBe(input);
        expect(record).toBe(RECORD);
        return resolvedSpec;
      },
      maintenanceSecretFor(record) {
        expect(this).toBe(input);
        expect(record).toBe(RECORD);
        return sharedSecrets.maintenanceAdmin;
      },
      auditClock() {
        expect(this).toBe(input);
        return 200;
      },
      authorityClock() {
        expect(this).toBe(input);
        return 201;
      },
      maxItemsPerCall: 11,
      signal: new AbortController().signal,
    };
    Reflect.set(input, 'backendFor', () => ({}));
    await control.advanceFleetAudit(input);
    const forwarded = required(vi.mocked(advanceFleetAudit).mock.calls[0])[0];
    expect(forwarded.backendFor(RECORD)).toBe(
      required(constructed.backend.mock.calls[0])[0],
    );
    expect(() =>
      forwarded.backendFor({ ...RECORD, backend: 'workers-for-platforms' }),
    ).toThrow('requires plain-worker records');
    expect(forwarded.specFor(RECORD)).toBe(SPEC);
    expect(forwarded.maintenanceSecretFor(RECORD)).toBe(
      sharedSecrets.maintenanceAdmin,
    );
    expect(forwarded.auditClock?.()).toBe(200);
    expect(forwarded.authorityClock?.()).toBe(201);
    expect(forwarded).toMatchObject({
      operationStore: required(constructed.operationStore.mock.calls[0])[0],
      inventoryStore: required(constructed.inventoryStore.mock.calls[0])[0],
      fleetStore: required(constructed.fleetStore.mock.calls[0])[0],
      action: input.action,
      maxItemsPerCall: 11,
      signal: input.signal,
    });
    Reflect.set(input, 'specFor', () => {
      throw new Error('mutated callback');
    });
    expect(forwarded.specFor(RECORD)).toBe(SPEC);
    resolvedSpec = {
      ...SPEC,
      authoredBy: 'external',
    } as unknown as CloudflareDeploymentSpec;
    expect(() => forwarded.specFor(RECORD)).toThrow(
      'requires platform-authored specifications',
    );
  });

  it('forwards migration callbacks with captured receivers and no backend-switch provider', async () => {
    const control = createCloudflareControlPlane(hostOptions());
    let resolvedSpec = SPEC;
    const settlement = { settle: vi.fn(async () => {}) };
    const controller = new AbortController();
    const completions: unknown[] = [];
    const completionResult = {
      operationId: OPERATION_ID,
      itemCount: 1,
      completedItemCount: 1,
      finalizedAtMs: 900,
    };
    const input: CloudflareAdvanceFleetMigrationOptions = {
      action: {
        kind: 'start',
        operationId: OPERATION_ID,
        records: [RECORD],
        canaryTenantTags: [],
      },
      specFor(record) {
        expect(this).toBe(input);
        expect(record).toBe(RECORD);
        return resolvedSpec;
      },
      secretsFor(record) {
        expect(this).toBe(input);
        expect(record).toBe(RECORD);
        return sharedSecrets;
      },
      settlementFor(record) {
        expect(this).toBe(input);
        expect(record).toBe(RECORD);
        return settlement;
      },
      clock() {
        expect(this).toBe(input);
        return 300;
      },
      routeAttestation,
      signal: controller.signal,
      onComplete(result) {
        expect(this).toBe(input);
        completions.push(result);
      },
    };
    Reflect.set(input, 'finalizedStateProviderFor', () => ({}));
    Reflect.set(input, 'backendFor', () => ({}));
    await control.advanceFleetMigration(input);
    const forwarded = required(
      vi.mocked(advanceFleetMigration).mock.calls[0],
    )[0];
    expect(forwarded.backendFor(RECORD)).toBe(
      required(constructed.backend.mock.calls[0])[0],
    );
    expect(() =>
      forwarded.backendFor({ ...RECORD, backend: 'workers-for-platforms' }),
    ).toThrow('requires plain-worker records');
    expect(forwarded.specFor(RECORD)).toBe(SPEC);
    expect(forwarded.secretsFor(RECORD)).toBe(sharedSecrets);
    expect(forwarded.settlementFor?.(RECORD)).toBe(settlement);
    expect(forwarded.clock?.()).toBe(300);
    expect(forwarded.routeAttestation).toBe(routeAttestation);
    expect(forwarded.signal).toBe(controller.signal);
    await forwarded.onComplete?.(completionResult);
    expect(completions).toEqual([completionResult]);
    expect(completions[0]).toBe(completionResult);
    expect(forwarded).not.toHaveProperty('finalizedStateProviderFor');
    expect(forwarded).toMatchObject({
      operationStore: required(constructed.operationStore.mock.calls[0])[0],
      fleetStore: required(constructed.fleetStore.mock.calls[0])[0],
      action: input.action,
    });
    Reflect.set(input, 'specFor', () => {
      throw new Error('mutated callback');
    });
    Reflect.set(input, 'onComplete', () => {
      throw new Error('mutated callback');
    });
    expect(forwarded.specFor(RECORD)).toBe(SPEC);
    await forwarded.onComplete?.(completionResult);
    expect(completions).toEqual([completionResult, completionResult]);
    resolvedSpec = {
      ...SPEC,
      durableObjectBindings: [
        {
          name: 'RUNNER',
          className: 'Runner',
          scriptName: 'state',
          dispatchNamespace: 'platform',
        },
      ],
    } as unknown as CloudflareDeploymentSpec;
    expect(() => forwarded.specFor(RECORD)).toThrow(
      'cannot use dispatch namespace bindings',
    );
  });

  it('forwards page reads and abandonment with the private stores', async () => {
    const control = createCloudflareControlPlane(hostOptions());
    const page = {
      operationId: OPERATION_ID,
      afterOrdinal: 4,
      limit: 10,
      store: {},
    };
    const selectedPage = {
      operationId: OPERATION_ID,
      afterOrdinal: 4,
      limit: 10,
    };
    const auditResult = { findings: [], done: true as const };
    const migrationResult = { items: [], done: true };
    vi.mocked(readFleetAuditFindingsPage).mockResolvedValueOnce(auditResult);
    vi.mocked(readFleetMigrationItemsPage).mockResolvedValueOnce(
      migrationResult,
    );
    await expect(control.readFleetAuditFindingsPage(page)).resolves.toBe(
      auditResult,
    );
    await expect(control.readFleetMigrationItemsPage(page)).resolves.toBe(
      migrationResult,
    );
    await control.readFleetInventoryGeneration(7);
    await control.abandonFleetAuditOperation(OPERATION_ID);
    await control.abandonFleetMigrationOperation(OPERATION_ID);
    const operationStore = required(
      constructed.operationStore.mock.calls[0],
    )[0];
    const inventoryStore = required(
      constructed.inventoryStore.mock.calls[0],
    )[0];
    expect(readFleetAuditFindingsPage).toHaveBeenCalledExactlyOnceWith(
      operationStore,
      selectedPage,
    );
    expect(readFleetMigrationItemsPage).toHaveBeenCalledExactlyOnceWith(
      operationStore,
      selectedPage,
    );
    expect(readFleetInventoryGeneration).toHaveBeenCalledExactlyOnceWith(
      inventoryStore,
      7,
    );
    expect(abandonFleetAuditOperation).toHaveBeenCalledExactlyOnceWith({
      operationStore,
      inventoryStore,
      operationId: OPERATION_ID,
    });
    expect(abandonFleetMigrationOperation).toHaveBeenCalledExactlyOnceWith({
      operationStore,
      operationId: OPERATION_ID,
    });
  });

  it('preserves store receivers for deployment reads and bounded retention', async () => {
    const control = createCloudflareControlPlane(hostOptions());
    const fleetStore = required(
      constructed.fleetStore.mock.calls[0],
    )[0] as D1FleetStateStore;
    const inventoryStore = required(
      constructed.inventoryStore.mock.calls[0],
    )[0] as D1FleetInventoryRunStore;
    const operationStore = required(
      constructed.operationStore.mock.calls[0],
    )[0] as D1FleetOperationStore;
    const get = vi.spyOn(fleetStore, 'get').mockImplementation(async function (
      this: D1FleetStateStore,
    ) {
      expect(this).toBe(fleetStore);
      return RECORD;
    });
    const receipt = vi
      .spyOn(fleetStore, 'readCleanupReceipt')
      .mockImplementation(async function (this: D1FleetStateStore) {
        expect(this).toBe(fleetStore);
        return undefined;
      });
    const cleanupPrune = vi
      .spyOn(fleetStore, 'pruneCleanupReceipts')
      .mockImplementation(async function (this: D1FleetStateStore) {
        expect(this).toBe(fleetStore);
        return { deleted: 2 };
      });
    const generation = vi
      .spyOn(inventoryStore, 'latestFinalizedGeneration')
      .mockImplementation(async function (this: D1FleetInventoryRunStore) {
        expect(this).toBe(inventoryStore);
        return undefined;
      });
    const inventoryPrune = vi
      .spyOn(inventoryStore, 'pruneInventoryGenerations')
      .mockImplementation(async function (this: D1FleetInventoryRunStore) {
        expect(this).toBe(inventoryStore);
        return { deleted: 3 };
      });
    const operationPrune = vi
      .spyOn(operationStore, 'pruneFleetOperations')
      .mockImplementation(async function (this: D1FleetOperationStore) {
        expect(this).toBe(operationStore);
        return { deleted: 4, releasedPins: 1 };
      });
    const getDeployment = control.getDeployment;
    await expect(getDeployment('acme', 'production')).resolves.toBe(RECORD);
    await expect(
      control.readCleanupReceipt(OPERATION_ID),
    ).resolves.toBeUndefined();
    await expect(
      control.latestFinalizedInventoryGeneration(),
    ).resolves.toBeUndefined();
    await expect(
      control.pruneCleanupReceipts({ completedBeforeMs: 99, limit: 5 }),
    ).resolves.toEqual({ deleted: 2 });
    await expect(
      control.pruneInventoryGenerations({ limit: 6 }),
    ).resolves.toEqual({ deleted: 3 });
    await expect(
      control.pruneFleetOperations({ kind: 'audit', limit: 7 }),
    ).resolves.toEqual({ deleted: 4, releasedPins: 1 });
    expect(get).toHaveBeenCalledExactlyOnceWith('acme', 'production');
    expect(receipt).toHaveBeenCalledExactlyOnceWith(OPERATION_ID);
    expect(generation).toHaveBeenCalledExactlyOnceWith();
    expect(cleanupPrune).toHaveBeenCalledExactlyOnceWith({
      completedBeforeMs: 99,
      limit: 5,
    });
    expect(inventoryPrune).toHaveBeenCalledExactlyOnceWith({ limit: 6 });
    expect(operationPrune).toHaveBeenCalledExactlyOnceWith({
      kind: 'audit',
      limit: 7,
    });
  });
});

describe('Workers for Platforms control-plane composition', () => {
  it('captures the trusted platform configuration and constructs native stores and a dispatch backend', () => {
    const profileFailure = new Error('profile resolution');
    const options = {
      ...wfpHostOptions(),
      fetch: vi.fn<typeof fetch>(),
      maintenanceFetch: vi.fn<typeof fetch>(),
      maintenanceRequestTimeoutMs: 3_000,
      clock() {
        expect(this).toBe(options);
        return 91;
      },
      platformProfileFor(spec: CloudflareWorkersForPlatformsDeploymentSpec) {
        expect(this).toBe(options);
        expect(spec).toBe(WFP_SPEC);
        throw profileFailure;
      },
      plane: 'plain-worker',
      rateCoordinator: {},
      exportStore: {},
      client: {},
      backend: {},
      store: {},
      inventoryStore: {},
      operationStore: {},
    };
    const control = createCloudflareWorkersForPlatformsControlPlane(options);
    const [database, databaseBinding] = required(
      constructed.database.mock.calls[0],
    );
    const [fleetStore, fleetAdapter] = required(
      constructed.fleetStore.mock.calls[0],
    );
    const [inventoryStore, inventoryAdapter] = required(
      constructed.inventoryStore.mock.calls[0],
    );
    const [operationStore, operationAdapter, operationOptions] = required(
      constructed.operationStore.mock.calls[0],
    );
    const [quota, quotaBinding, quotaOptions] = required(
      constructed.quota.mock.calls[0],
    );
    const [client, clientOptions] = required(constructed.client.mock.calls[0]);
    const [backend, backendOptions] = required(
      constructed.wfpBackend.mock.calls[0],
    );
    expect(database).toBeInstanceOf(D1FleetStateDatabase);
    expect(databaseBinding).toBe(options.fleetDatabase);
    expect([fleetAdapter, inventoryAdapter, operationAdapter]).toEqual([
      database,
      database,
      database,
    ]);
    expect(fleetStore).toBeInstanceOf(D1FleetStateStore);
    expect(inventoryStore).toBeInstanceOf(D1FleetInventoryRunStore);
    expect(operationStore).toBeInstanceOf(D1FleetOperationStore);
    expect(operationOptions.inventoryStore).toBe(inventoryStore);
    expect(quota).toBeInstanceOf(D1CloudflareApiRateCoordinator);
    expect(quotaBinding).toBe(options.quotaDatabase);
    expect(quotaOptions).toEqual({ quotaScope: 'account-token-quota' });
    expect(clientOptions).toEqual({
      accountId: 'account-1',
      apiToken: 'provider-token',
      dispatchNamespace: 'fleet-dispatch',
      rateCoordinator: quota,
      exportStore: expect.any(R2DatabaseExportStore),
      concurrency: undefined,
      requestTimeoutMs: undefined,
      fetch: options.fetch,
    });
    expect(backend).toBeInstanceOf(WorkersForPlatformsBackend);
    expect(backendOptions).toEqual({
      client,
      fetch: options.maintenanceFetch,
      maintenanceRequestTimeoutMs: 3_000,
      clock: expect.any(Function),
      hostRoutingKvId: 'fleet-hosts',
      auditQueueName: 'fleet-audit',
      platformProfileFor: expect.any(Function),
      namespacedState: {
        dispatchNamespace: 'fleet-dispatch',
        sharedOutboundWorkerName: 'fleet-outbound',
        stateEgressRootSecret: 'test-state-egress-root-secret-00000001',
      },
    });
    options.clock = () => 999;
    options.platformProfileFor = () => {
      throw new Error('mutated profile resolver');
    };
    expect(backendOptions.clock()).toBe(91);
    expect(() => backendOptions.platformProfileFor(WFP_SPEC)).toThrow(
      profileFailure,
    );
    expect(Object.isFrozen(control)).toBe(true);
    expect(control).not.toHaveProperty('advanceCleanupDeployment');
    expect(control).not.toHaveProperty('backend');
    expect(control).not.toHaveProperty('store');
    expect(control).not.toHaveProperty('client');
    expect(constructed.backend).not.toHaveBeenCalled();
    expect(options.fleetDatabase.prepare).not.toHaveBeenCalled();
    expect(options.quotaDatabase.prepare).not.toHaveBeenCalled();
    expect(options.fetch).not.toHaveBeenCalled();
    expect(options.maintenanceFetch).not.toHaveBeenCalled();
  });

  it.each([
    { ...WFP_SPEC, authoredBy: 'platform' },
    {
      ...WFP_SPEC,
      durableObjectBindings: [
        { name: 'RUNNER', className: 'Runner', scriptName: 'foreign-state' },
      ],
    },
    {
      ...WFP_SPEC,
      durableObjectBindings: [
        {
          name: 'RUNNER',
          className: 'Runner',
          dispatchNamespace: 'foreign-dispatch',
        },
      ],
    },
  ])('refuses runtime specs outside external, locally bound deployments before lifecycle forwarding', async (invalid) => {
    const control = createCloudflareWorkersForPlatformsControlPlane(
      wfpHostOptions(),
    );
    const spec = invalid as CloudflareWorkersForPlatformsDeploymentSpec;
    await expect(
      control.provisionDeployment({
        spec,
        secrets: sharedSecrets,
        initialExecutionFenceState: 'open',
      }),
    ).rejects.toThrow(TypeError);
    await expect(
      control.advanceDecommissionDeployment({
        spec,
        action: { kind: 'start' },
        maxProviderRequests: 9,
      }),
    ).rejects.toThrow(TypeError);
    await expect(
      control.rollbackExternalRelease({
        currentSpec: spec,
        rollbackSpec: WFP_SPEC,
        secrets: sharedSecrets,
      }),
    ).rejects.toThrow(TypeError);
    await expect(
      control.rollbackExternalRelease({
        currentSpec: WFP_SPEC,
        rollbackSpec: spec,
        secrets: sharedSecrets,
      }),
    ).rejects.toThrow(TypeError);
    expect(provisionDeployment).not.toHaveBeenCalled();
    expect(advanceDecommissionDeployment).not.toHaveBeenCalled();
    expect(rollbackExternalRelease).not.toHaveBeenCalled();
  });

  it.each([
    { ...WFP_RECORD, backend: 'plain-worker' },
    { ...WFP_RECORD, wfpMode: 'platform-catalog' },
    {
      ...WFP_RECORD,
      platformResources: {
        ...WFP_RESOURCES,
        sharedOutboundWorkerName: 'foreign-outbound',
      },
    },
    {
      ...WFP_RECORD,
      platformResources: { ...WFP_RESOURCES, auditQueueName: 'foreign-audit' },
    },
    {
      ...WFP_RECORD,
      platformResources: {
        ...WFP_RESOURCES,
        stateWorker: { ...WFP_RESOURCES.stateWorker, plane: 'ordinary' },
      },
    },
    {
      ...WFP_RECORD,
      platformResources: {
        ...WFP_RESOURCES,
        stateWorker: {
          ...WFP_RESOURCES.stateWorker,
          dispatchNamespace: 'foreign-dispatch',
        },
      },
    },
    {
      ...WFP_RECORD,
      backendSwitchIntent: {
        kind: 'backend-switch',
        tenantTag: WFP_RECORD.tenantTag,
        environment: WFP_RECORD.environment,
        prior: {
          scriptName: SPEC.scriptName,
          artifactVersion: 'artifact-1',
          specDigest: 'a'.repeat(64),
          databaseId: 'database-1',
          databaseName: SPEC.databaseName,
          durableObjectBindings: [],
          namespaceIds: [],
          secretNames: [],
          applicationResources: [],
          customDomain: { id: 'domain-1', hostname: SPEC.routeHostname },
        },
        targetSpecDigest: 'b'.repeat(64),
        targetApplication: { vars: [], secrets: [], r2Buckets: [] },
        target: {
          maintenanceCapabilityPublicKey: 'test-verifier',
          stateArtifactDigest: 'c'.repeat(64),
          stateDurableObjectHistoryDigest: 'd'.repeat(64),
          d1SchemaVersion: 1,
          d1SchemaHistoryDigest: 'e'.repeat(64),
          outboundPolicy: {
            policyId: 'policy-1',
            policyHosts: [],
            policyDigest: 'f'.repeat(64),
          },
        },
        rollbackUntil: '2026-09-10T00:00:00.000Z',
        subphase: 'finalized',
      },
    },
  ] satisfies FleetRecord[])('refuses a foreign or adopted record while holding the deployment lease and on reads', async (record) => {
    const control = createCloudflareWorkersForPlatformsControlPlane(
      wfpHostOptions(),
    );
    const durableStore = required(
      constructed.fleetStore.mock.calls[0],
    )[0] as D1FleetStateStore;
    const lease: FleetStateLease = {
      tenantTag: WFP_SPEC.tenantTag,
      environment: WFP_SPEC.environment,
      mutationLeaseTtlMs: 10_000,
      assertOwned: vi.fn(async () => {}),
      renew: vi.fn(async () => {}),
      put: vi.fn(async () => {}),
      delete: vi.fn(async () => {}),
    };
    let held = false;
    const acquired = vi
      .spyOn(durableStore, 'withDeploymentLease')
      .mockImplementation(async (tenantTag, environment, operation) => {
        expect([tenantTag, environment]).toEqual([
          WFP_SPEC.tenantTag,
          WFP_SPEC.environment,
        ]);
        held = true;
        try {
          return await operation(lease);
        } finally {
          held = false;
        }
      });
    const read = vi.spyOn(durableStore, 'get').mockImplementation(async () => {
      expect(held).toBe(true);
      return record;
    });
    const mutation = vi.fn(async () => {
      throw new Error('foreign record reached mutation');
    });
    vi.mocked(provisionDeployment).mockImplementation(async (operation) =>
      operation.store.withDeploymentLease(
        operation.spec.tenantTag,
        operation.spec.environment,
        mutation,
      ),
    );
    await expect(
      control.provisionDeployment({
        spec: WFP_SPEC,
        secrets: sharedSecrets,
        initialExecutionFenceState: 'open',
      }),
    ).rejects.toThrow(TypeError);
    expect(acquired).toHaveBeenCalledOnce();
    expect(read).toHaveBeenCalledExactlyOnceWith(
      WFP_SPEC.tenantTag,
      WFP_SPEC.environment,
    );
    expect(mutation).not.toHaveBeenCalled();
    read.mockResolvedValue(record);
    await expect(
      control.getDeployment(WFP_SPEC.tenantTag, WFP_SPEC.environment),
    ).rejects.toThrow(TypeError);
  });

  it('passes the acquired lease through for new and matching deployments with bounded failure cleanup', async () => {
    const control = createCloudflareWorkersForPlatformsControlPlane(
      wfpHostOptions(),
    );
    const durableStore = required(
      constructed.fleetStore.mock.calls[0],
    )[0] as D1FleetStateStore;
    const lease: FleetStateLease = {
      tenantTag: WFP_SPEC.tenantTag,
      environment: WFP_SPEC.environment,
      mutationLeaseTtlMs: 10_000,
      assertOwned: vi.fn(async () => {}),
      renew: vi.fn(async () => {}),
      put: vi.fn(async () => {}),
      delete: vi.fn(async () => {}),
    };
    vi.spyOn(durableStore, 'withDeploymentLease').mockImplementation(
      async (_tenantTag, _environment, operation) => operation(lease),
    );
    vi.spyOn(durableStore, 'get')
      .mockResolvedValueOnce(undefined)
      .mockResolvedValueOnce(WFP_RECORD);
    const mutation = vi.fn(async (owned: FleetStateLease) => {
      expect(owned).toBe(lease);
      await owned.put(WFP_RECORD);
      return {
        record: WFP_RECORD,
        maintenance: {
          armed: true,
          nextAlarmAt: null,
          lastSweepAt: null,
          lastPurgeAt: null,
        },
      };
    });
    vi.mocked(provisionDeployment).mockImplementation(async (operation) =>
      operation.store.withDeploymentLease(
        operation.spec.tenantTag,
        operation.spec.environment,
        mutation,
      ),
    );
    const input = {
      spec: WFP_SPEC,
      secrets: sharedSecrets,
      initialExecutionFenceState: 'open' as const,
      routeAttestation,
      clock() {
        expect(this).toBe(input);
        return 105;
      },
      failureCleanup: 'drain',
      backend: {},
      store: {},
    };
    await control.provisionDeployment(input);
    await control.provisionDeployment(input);
    const forwarded = required(vi.mocked(provisionDeployment).mock.calls[0])[0];
    expect(forwarded).toEqual({
      backend: required(constructed.wfpBackend.mock.calls[0])[0],
      store: expect.objectContaining({
        withDeploymentLease: expect.any(Function),
      }),
      spec: WFP_SPEC,
      secrets: sharedSecrets,
      initialExecutionFenceState: 'open',
      routeAttestation,
      failureCleanup: 'bounded',
      clock: expect.any(Function),
    });
    expect(forwarded.store).not.toBe(input.store);
    expect(forwarded.clock?.()).toBe(105);
    expect(mutation).toHaveBeenCalledTimes(2);
    expect(lease.put).toHaveBeenCalledTimes(2);
  });

  it('pins start and continuation inventory to the configured namespace and HOSTS store', async () => {
    const options = wfpHostOptions();
    const control = createCloudflareWorkersForPlatformsControlPlane(options);
    Reflect.set(options, 'dispatchNamespace', 'mutated-dispatch');
    Reflect.set(options, 'hostRoutingKvId', 'mutated-hosts');
    const action = {
      kind: 'start' as const,
      operationId: OPERATION_ID,
      options: {
        databaseNamePrefix: 'fleet-',
        scriptNamePrefix: 'fleet-',
        includeR2Buckets: true,
        includeDispatchNamespace: false,
        dispatchNamespace: 'forged-dispatch',
        hostRoutingKvId: 'forged-hosts',
      },
    };
    await control.advanceFleetInventory({ action, maxProviderRequests: 9 });
    await control.advanceFleetInventory({
      action: { kind: 'continue', token: TOKEN },
      maxProviderRequests: 9,
    });
    const first = required(vi.mocked(advanceFleetInventory).mock.calls[0])[0];
    const continued = required(
      vi.mocked(advanceFleetInventory).mock.calls[1],
    )[0];
    const scope = {
      includeDispatchNamespace: true,
      dispatchNamespace: 'fleet-dispatch',
      hostRoutingKvId: 'fleet-hosts',
    };
    expect(first.action).toEqual({
      kind: 'start',
      operationId: OPERATION_ID,
      options: {
        databaseNamePrefix: 'fleet-',
        scriptNamePrefix: 'fleet-',
        includeR2Buckets: true,
        ...scope,
      },
    });
    expect(first.expectedScope).toEqual(scope);
    expect(continued.expectedScope).toEqual(scope);
    expect(continued.action).toEqual({ kind: 'continue', token: TOKEN });
  });

  it('binds audit and migration callbacks while refusing specs and records outside the WFP contract', async () => {
    const control = createCloudflareWorkersForPlatformsControlPlane(
      wfpHostOptions(),
    );
    let spec = WFP_SPEC;
    const audit: CloudflareAdvanceFleetAuditOptions<CloudflareWorkersForPlatformsDeploymentSpec> =
      {
        action: {
          kind: 'start',
          operationId: OPERATION_ID,
          records: [WFP_RECORD],
          staleAfterMs: 100,
        },
        specFor(record) {
          expect(this).toBe(audit);
          expect(record).toBe(WFP_RECORD);
          return spec;
        },
        maintenanceSecretFor(record) {
          expect(this).toBe(audit);
          expect(record).toBe(WFP_RECORD);
          return sharedSecrets.maintenanceAdmin;
        },
        auditClock() {
          expect(this).toBe(audit);
          return 210;
        },
        authorityClock() {
          expect(this).toBe(audit);
          return 211;
        },
      };
    const settlement = { settle: vi.fn(async () => {}) };
    const completions: unknown[] = [];
    const migration: CloudflareAdvanceFleetMigrationOptions<CloudflareWorkersForPlatformsDeploymentSpec> =
      {
        action: {
          kind: 'start',
          operationId: OPERATION_ID,
          records: [WFP_RECORD],
          canaryTenantTags: [],
        },
        specFor(record) {
          expect(this).toBe(migration);
          expect(record).toBe(WFP_RECORD);
          return spec;
        },
        secretsFor(record) {
          expect(this).toBe(migration);
          expect(record).toBe(WFP_RECORD);
          return sharedSecrets;
        },
        settlementFor(record) {
          expect(this).toBe(migration);
          expect(record).toBe(WFP_RECORD);
          return settlement;
        },
        clock() {
          expect(this).toBe(migration);
          return 310;
        },
        onComplete(result) {
          expect(this).toBe(migration);
          completions.push(result);
        },
      };
    await control.advanceFleetAudit(audit);
    await control.advanceFleetMigration(migration);
    const auditInput = required(vi.mocked(advanceFleetAudit).mock.calls[0])[0];
    const migrationInput = required(
      vi.mocked(advanceFleetMigration).mock.calls[0],
    )[0];
    const backend = required(constructed.wfpBackend.mock.calls[0])[0];
    for (const forwarded of [auditInput, migrationInput]) {
      expect(forwarded.backendFor(WFP_RECORD)).toBe(backend);
      expect(() => forwarded.backendFor(RECORD)).toThrow(TypeError);
      expect(forwarded.specFor(WFP_RECORD)).toBe(WFP_SPEC);
    }
    expect(auditInput.maintenanceSecretFor(WFP_RECORD)).toBe(
      sharedSecrets.maintenanceAdmin,
    );
    expect(auditInput.auditClock?.()).toBe(210);
    expect(auditInput.authorityClock?.()).toBe(211);
    expect(migrationInput.secretsFor(WFP_RECORD)).toBe(sharedSecrets);
    expect(migrationInput.settlementFor?.(WFP_RECORD)).toBe(settlement);
    expect(migrationInput.clock?.()).toBe(310);
    const completion = {
      operationId: OPERATION_ID,
      itemCount: 1,
      completedItemCount: 1,
      finalizedAtMs: 900,
    };
    await migrationInput.onComplete?.(completion);
    expect(completions).toEqual([completion]);
    spec = SPEC as unknown as CloudflareWorkersForPlatformsDeploymentSpec;
    expect(() => auditInput.specFor(WFP_RECORD)).toThrow(TypeError);
    expect(() => migrationInput.specFor(WFP_RECORD)).toThrow(TypeError);
  });

  it('forwards exact rollback and decommission inputs through the same guarded store and captured UUID source', async () => {
    const host = {
      ...wfpHostOptions(),
      randomUUID() {
        expect(this).toBe(host);
        return OPERATION_ID;
      },
    };
    const control = createCloudflareWorkersForPlatformsControlPlane(host);
    const rollbackSpec = { ...WFP_SPEC, schemaVersion: 2 };
    const settlement = { settle: vi.fn(async () => {}) };
    const rollback = {
      currentSpec: WFP_SPEC,
      rollbackSpec,
      secrets: sharedSecrets,
      settlement,
      routeAttestation,
      clock() {
        expect(this).toBe(rollback);
        return 410;
      },
      store: {},
      backend: {},
    };
    const decommission = {
      spec: WFP_SPEC,
      action: { kind: 'continue' as const, token: TOKEN },
      maxProviderRequests: 9,
      signal: new AbortController().signal,
      clock() {
        expect(this).toBe(decommission);
        return 411;
      },
      randomUUID: () => 'forged-uuid',
      store: {},
      backend: {},
    };
    host.randomUUID = () => 'mutated-uuid';
    const invokeRollback = control.rollbackExternalRelease;
    const invokeDecommission = control.advanceDecommissionDeployment;
    await invokeRollback(rollback);
    await invokeDecommission(decommission);
    const rolledBack = required(
      vi.mocked(rollbackExternalRelease).mock.calls[0],
    )[0];
    const teardown = required(
      vi.mocked(advanceDecommissionDeployment).mock.calls[0],
    )[0];
    const backend = required(constructed.wfpBackend.mock.calls[0])[0];
    expect(rolledBack).toEqual({
      backend,
      store: expect.objectContaining({
        withDeploymentLease: expect.any(Function),
      }),
      currentSpec: WFP_SPEC,
      rollbackSpec,
      secrets: sharedSecrets,
      settlement,
      routeAttestation,
      clock: expect.any(Function),
    });
    expect(teardown).toEqual({
      backend,
      store: rolledBack.store,
      spec: WFP_SPEC,
      action: decommission.action,
      maxProviderRequests: 9,
      signal: decommission.signal,
      clock: expect.any(Function),
      randomUUID: expect.any(Function),
    });
    expect(rolledBack.store).not.toBe(rollback.store);
    expect(rolledBack.currentSpec).toBe(WFP_SPEC);
    expect(rolledBack.rollbackSpec).toBe(rollbackSpec);
    expect(rolledBack.clock?.()).toBe(410);
    expect(teardown.clock?.()).toBe(411);
    expect(teardown.randomUUID()).toBe(OPERATION_ID);
    expect(rollbackExternalRelease).toHaveBeenCalledOnce();
    expect(advanceDecommissionDeployment).toHaveBeenCalledOnce();
  });
  it('keeps external deployments without optional audit enrollment readable', async () => {
    const control = createCloudflareWorkersForPlatformsControlPlane(
      wfpHostOptions(),
    );
    const store = required(constructed.fleetStore.mock.calls[0])[0];
    const { auditQueueName: _queue, ...platformResources } = required(
      WFP_RECORD.platformResources,
    );
    const record = { ...WFP_RECORD, platformResources };
    vi.spyOn(store, 'get').mockResolvedValue(record);
    await expect(
      control.getDeployment(record.tenantTag, record.environment),
    ).resolves.toBe(record);
  });
});
