// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { reserveApplicationR2Resources } from '../src/application-bindings.js';
import {
  type AdvanceCleanupDeploymentOptions,
  advanceCleanupDeployment,
  advanceCleanupUnderLease,
  CleanupAdvanceCapabilityError,
  CleanupAdvanceRestartError,
  type CleanupAdvanceResult,
  startProvisioningRollbackCleanup,
} from '../src/cleanup-advance.js';
import {
  CleanupAdvanceTokenDeploymentError,
  CleanupAdvanceTokenFutureError,
  CleanupAdvanceTokenOperationError,
  normalizeCleanupAdvanceIntent,
} from '../src/cleanup-intent.js';
import { initialWorkerAttachmentScan } from '../src/cloudflare-worker-attachment-scan-state.js';
import { advanceDecommissionDeployment } from '../src/decommission-advance.js';
import { DecommissionAdvanceTokenOperationError } from '../src/decommission-intent.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type {
  ApplicationR2BucketSnapshot,
  ApplicationR2Resource,
  CleanupAdvanceIntent,
  CleanupAdvanceState,
  CleanupAdvanceToken,
  CleanupAuthority,
  CleanupTerminalReceipt,
  DatabaseReference,
  DecommissionAdvanceIntent,
  DecommissionAttachmentScanInput,
  DecommissionAttachmentScanResult,
  DeploymentSpec,
  ExternalPlatformResources,
  FleetRecord,
  FleetStateLease,
  FleetStateStore,
  InitialExecutionFenceState,
  InvocationAuthorityCarrier,
  ProvisioningBackend,
  ProvisioningBackendKind,
  ProvisioningPhase,
  SeedDeploymentIdentityOptions,
} from '../src/types.js';

const DATABASE_ID = '00000000-0000-0000-0000-000000000101';
const OPERATION_ID = '11111111-1111-4111-8111-111111111111';
const OTHER_OPERATION_ID = '33333333-3333-4333-8333-333333333333';
const NOW = '2026-08-29T12:00:00.000Z';
const AUTHORIZED_AT = '2026-08-29T13:00:00.000Z';
const EVIDENCE_A = 'a'.repeat(64);
const EVIDENCE_B = 'b'.repeat(64);
const CREATION_DATE = '2026-08-01T00:00:00.000Z';
const MAINTENANCE_PUBLIC_KEY =
  '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}';

function unexpectedUUID(): string {
  throw new Error('unexpected new operation id');
}

function spec(overrides: Partial<DeploymentSpec> = {}): DeploymentSpec {
  return {
    tenantTag: 'acme',
    environment: 'production',
    scriptName: 'acme-production',
    databaseName: 'acme-production',
    compatibilityDate: '2026-08-10',
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    authoredBy: 'platform',
    schemaVersion: 3,
    migrations: [
      { version: 1, sql: 'CREATE TABLE example (id TEXT PRIMARY KEY)' },
      { version: 2, sql: 'ALTER TABLE example ADD COLUMN value TEXT' },
      { version: 3, sql: 'ALTER TABLE example ADD COLUMN note TEXT' },
    ],
    durableObjectMigrations: [{ tag: 'v1', newSqliteClasses: ['Maintenance'] }],
    durableObjectBindings: [{ name: 'MAINTENANCE', className: 'Maintenance' }],
    egressProxyService: 'fleet-egress-proxy',
    maintenanceBaseUrl: 'https://control-acme.example.test',
    routeHostname: 'acme.example.test',
    ...overrides,
  };
}

function r2Spec(): DeploymentSpec {
  return spec({
    application: { vars: [], secrets: [], r2Buckets: [{ name: 'DATA' }] },
  });
}

const PLATFORM_RESOURCES: ExternalPlatformResources = {
  maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
  stateWorker: {
    scriptName: 'acme-production-state',
    artifactVersion: 'state-v1',
    artifactDigest: 'c'.repeat(64),
    durableObjectBindings: [],
    namespaceIds: [],
  },
};

function record(overrides: Partial<FleetRecord> = {}): FleetRecord {
  return {
    tenantTag: 'acme',
    environment: 'production',
    backend: 'plain-worker',
    scriptName: 'acme-production',
    databaseId: DATABASE_ID,
    databaseName: 'acme-production',
    schemaVersion: 3,
    artifactVersion: 'artifact-v1',
    desiredSpecDigest: deploymentSpecDigest(spec()),
    durableObjectBindings: [],
    applicationResources: [],
    routeHostname: 'acme.example.test',
    invocationAuthority: { version: 1, authorizedAt: null },
    phase: 'worker-deployed',
    updatedAt: NOW,
    ...overrides,
  };
}

function intentFor(
  source: FleetRecord,
  state: CleanupAdvanceState,
  overrides: Partial<CleanupAdvanceIntent> = {},
): CleanupAdvanceIntent {
  return {
    version: 1,
    operationId: OPERATION_ID,
    revision: 0,
    generation: 0,
    updatedAt: NOW,
    authority: { kind: 'manual-cleanup' },
    identity: {
      record: {
        tenantTag: source.tenantTag,
        environment: source.environment,
        backend: source.backend,
        scriptName: source.scriptName,
        databaseId: source.databaseId,
        databaseName: source.databaseName,
        routeHostname: source.routeHostname,
      },
      admittedPhase: 'worker-deployed',
      externalArtifact: false,
    },
    state,
    ...overrides,
  };
}

function activeRecord(
  state: CleanupAdvanceState,
  input: Readonly<{
    admittedPhase?: ProvisioningPhase;
    authority?: CleanupAuthority;
    revision?: number;
    generation?: number;
    record?: Partial<FleetRecord>;
  }> = {},
): FleetRecord {
  const base = record({ ...input.record, phase: 'cleanup-advancing' });
  return {
    ...base,
    cleanupIntent: intentFor(base, state, {
      ...(input.authority ? { authority: input.authority } : {}),
      revision: input.revision ?? 0,
      generation: input.generation ?? 0,
      identity: {
        ...intentFor(base, state).identity,
        admittedPhase: input.admittedPhase ?? 'worker-deployed',
      },
    }),
  };
}

function discoverState(): CleanupAdvanceState {
  return {
    step: 'attachment-scan',
    scan: {
      purpose: {
        kind: 'cleanup-database-pre-delete',
        databaseId: DATABASE_ID,
        operationId: OPERATION_ID,
      },
      pass: 'discover',
      progress: initialWorkerAttachmentScan({
        kind: 'd1',
        databaseId: DATABASE_ID,
      }),
    },
  };
}

function verifyState(): CleanupAdvanceState {
  return {
    step: 'attachment-scan',
    scan: {
      purpose: {
        kind: 'cleanup-database-pre-delete',
        databaseId: DATABASE_ID,
        operationId: OPERATION_ID,
      },
      pass: 'verify',
      progress: initialWorkerAttachmentScan({
        kind: 'd1',
        databaseId: DATABASE_ID,
      }),
      discoverEvidence: { evidenceSha256: EVIDENCE_A, evidenceCount: 2 },
    },
  };
}

function token(
  revision = 0,
  operationId = OPERATION_ID,
  overrides: Partial<CleanupAdvanceToken> = {},
): CleanupAdvanceToken {
  return {
    version: 1,
    tenantTag: 'acme',
    environment: 'production',
    operationId,
    revision,
    ...overrides,
  };
}

function scanComplete(
  evidenceSha256 = EVIDENCE_A,
  evidenceCount = 2,
): DecommissionAttachmentScanResult {
  return {
    status: 'complete',
    evidenceSha256,
    evidenceCount,
    providerFetchAttemptsReserved: 3,
  };
}

function scanAttached(): DecommissionAttachmentScanResult {
  return {
    status: 'attached',
    attachment: {
      plane: 'dispatch',
      scriptName: 'tenant-holder',
      dispatchNamespace: 'tenants',
    },
    providerFetchAttemptsReserved: 3,
  };
}

class CleanupMemoryStore implements FleetStateStore {
  record: FleetRecord | undefined;
  readonly receipts = new Map<string, CleanupTerminalReceipt>();
  puts = 0;
  completeCleanupCalls = 0;
  completedAtMs = 1_000;
  leased = false;
  supportsCompleteCleanup = true;
  failTerminalResponseOnce = false;
  readCleanupReceipt?: (
    operationId: string,
  ) => Promise<CleanupTerminalReceipt | undefined>;
  pruneCleanupReceipts?: (
    input: Readonly<{ completedBeforeMs: number; limit: number }>,
  ) => Promise<Readonly<{ deleted: number }>>;

  constructor() {
    this.readCleanupReceipt = async (operationId) =>
      this.receipts.get(operationId);
    this.pruneCleanupReceipts = async ({ completedBeforeMs, limit }) => {
      const doomed = [...this.receipts.values()]
        .filter((receipt) => (receipt.completedAtMs ?? 0) < completedBeforeMs)
        .slice(0, limit);
      for (const receipt of doomed) this.receipts.delete(receipt.operationId);
      return { deleted: doomed.length };
    };
  }

  async withDeploymentLease<T>(
    tenantTag: string,
    environment: string,
    operation: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T> {
    if (this.leased) throw new Error('deployment is already being modified');
    this.leased = true;
    try {
      return await operation(this.lease(tenantTag, environment));
    } finally {
      this.leased = false;
    }
  }

  lease(tenantTag: string, environment: string): FleetStateLease {
    return {
      tenantTag,
      environment,
      mutationLeaseTtlMs: 15 * 60_000,
      assertOwned: async () => {},
      renew: async () => {},
      put: (next) => this.put(next),
      delete: async () => {
        this.record = undefined;
      },
      ...(this.supportsCompleteCleanup
        ? {
            completeCleanup: (input: {
              receipt: CleanupTerminalReceipt;
              expectedRevision: number;
            }) => this.completeCleanup(input),
          }
        : {}),
    };
  }

  async put(next: FleetRecord): Promise<void> {
    let normalized = next;
    if (next.cleanupIntent) {
      const { cleanupIntent, ...source } = next;
      normalized = {
        ...source,
        cleanupIntent: normalizeCleanupAdvanceIntent(cleanupIntent, source),
      };
    }
    this.puts += 1;
    this.record = normalized;
  }

  async completeCleanup(input: {
    receipt: CleanupTerminalReceipt;
    expectedRevision: number;
  }): Promise<CleanupTerminalReceipt> {
    this.completeCleanupCalls += 1;
    const current = this.record;
    if (
      current?.phase !== 'cleanup-advancing' ||
      current.cleanupIntent?.operationId !== input.receipt.operationId ||
      current.cleanupIntent.revision !== input.expectedRevision
    ) {
      const existing = this.receipts.get(input.receipt.operationId);
      if (existing) return existing;
      throw new Error(
        `cleanup receipt conflict for operation '${input.receipt.operationId}'`,
      );
    }
    const persisted = { ...input.receipt, completedAtMs: this.completedAtMs };
    this.completedAtMs += 1;
    this.receipts.set(persisted.operationId, persisted);
    this.record = undefined;
    if (this.failTerminalResponseOnce) {
      this.failTerminalResponseOnce = false;
      throw new Error('terminal response lost');
    }
    return persisted;
  }

  async get(): Promise<FleetRecord | undefined> {
    return this.record;
  }

  async list(): Promise<readonly FleetRecord[]> {
    return this.record ? [this.record] : [];
  }
}

class CleanupBackend implements ProvisioningBackend {
  kind: ProvisioningBackendKind = 'plain-worker';
  immutableExternalArtifacts?: true;
  providerCalls = 0;
  readonly events: string[] = [];
  databaseExists = true;
  databaseId = DATABASE_ID;
  databaseName = 'acme-production';
  databaseOwner: string | undefined = 'acme';
  detachFailure: unknown;
  readonly seededFenceStates: InitialExecutionFenceState[] = [];
  readonly scanInputs: DecommissionAttachmentScanInput[] = [];
  scanResults: DecommissionAttachmentScanResult[] = [];
  readonly liveBuckets = new Map<string, ApplicationR2BucketSnapshot>();

  advanceDecommissionAttachmentScan?: (
    input: DecommissionAttachmentScanInput,
  ) => Promise<DecommissionAttachmentScanResult>;
  assertDatabaseDeletionResidualsRemoved?: () => Promise<void>;
  revokePlatformResourceCredentials?: () => Promise<void>;
  deletePlatformResources?: () => Promise<void>;
  findApplicationR2Bucket?: (
    resource: ApplicationR2Resource,
  ) => Promise<ApplicationR2BucketSnapshot | undefined>;
  assertApplicationR2Empty?: () => Promise<void>;
  assertApplicationR2Detached?: () => Promise<void>;
  deleteApplicationR2Bucket?: (
    resource: ApplicationR2Resource,
  ) => Promise<void>;

  constructor() {
    this.advanceDecommissionAttachmentScan = async (input) => {
      this.call('attachmentScan');
      this.scanInputs.push(input);
      const result = this.scanResults.shift();
      if (!result) throw new Error('unexpected attachment scan');
      return result;
    };
    this.assertDatabaseDeletionResidualsRemoved = async () => {
      this.call('residuals');
    };
    this.revokePlatformResourceCredentials = async () => {
      this.call('revokePlatformResourceCredentials');
    };
    this.deletePlatformResources = async () => {
      this.call('deletePlatformResources');
    };
    this.findApplicationR2Bucket = async (resource) => {
      this.call('findApplicationR2Bucket');
      return this.liveBuckets.get(resource.bucketName);
    };
    this.assertApplicationR2Empty = async () => {
      this.call('assertApplicationR2Empty');
    };
    this.assertApplicationR2Detached = async () => {
      this.call('assertApplicationR2Detached');
      if (this.detachFailure !== undefined) throw this.detachFailure;
    };
    this.deleteApplicationR2Bucket = async (resource) => {
      this.call('deleteApplicationR2Bucket');
      this.liveBuckets.delete(resource.bucketName);
    };
  }

  call(name: string): void {
    this.providerCalls += 1;
    this.events.push(name);
  }

  drainEvents(): readonly string[] {
    return this.events.splice(0);
  }

  async findDatabase(): Promise<DatabaseReference | undefined> {
    this.call('findDatabase');
    return this.databaseExists
      ? { id: this.databaseId, name: this.databaseName, created: false }
      : undefined;
  }

  async getDatabase(
    databaseId: string,
  ): Promise<DatabaseReference | undefined> {
    this.call('getDatabase');
    return this.databaseExists && databaseId === this.databaseId
      ? { id: this.databaseId, name: this.databaseName, created: false }
      : undefined;
  }

  async readDeploymentIdentity(): Promise<string | undefined> {
    this.call('readDeploymentIdentity');
    return this.databaseOwner;
  }

  async seedDeploymentIdentity(
    _database: DatabaseReference,
    tenantTag: string,
    _fence: unknown,
    options: SeedDeploymentIdentityOptions,
  ): Promise<void> {
    this.call('seedDeploymentIdentity');
    this.seededFenceStates.push(options.initialExecutionFenceState);
    this.databaseOwner = tenantTag;
  }

  async deleteDatabase(): Promise<void> {
    this.call('deleteDatabase');
    this.databaseExists = false;
  }

  async removeTraffic(): Promise<void> {
    this.call('removeTraffic');
  }

  async assertTrafficRemoved(): Promise<void> {
    this.call('assertTrafficRemoved');
  }

  async revokeCredentials(): Promise<void> {
    this.call('revokeCredentials');
  }

  async deleteWorker(): Promise<void> {
    this.call('deleteWorker');
  }

  async ensureDatabase(): Promise<never> {
    throw new Error('unexpected ensureDatabase call');
  }

  async applyMigrations(): Promise<never> {
    throw new Error('unexpected applyMigrations call');
  }

  async deployWorker(): Promise<never> {
    throw new Error('unexpected deployWorker call');
  }

  async promoteWorker(): Promise<never> {
    throw new Error('unexpected promoteWorker call');
  }

  async ensureMaintenance(): Promise<never> {
    throw new Error('unexpected ensureMaintenance call');
  }

  async inspect(): Promise<never> {
    throw new Error('unexpected inspect call');
  }

  async attestActiveRoute(): Promise<never> {
    throw new Error('unexpected attestActiveRoute call');
  }

  async assertDatabaseDetached(): Promise<never> {
    throw new Error('unexpected assertDatabaseDetached call');
  }

  async exportDatabase(): Promise<never> {
    throw new Error('unexpected exportDatabase call');
  }
}

function harness(): {
  store: CleanupMemoryStore;
  backend: CleanupBackend;
} {
  return { store: new CleanupMemoryStore(), backend: new CleanupBackend() };
}

function options(
  store: CleanupMemoryStore,
  backend: CleanupBackend,
  action: AdvanceCleanupDeploymentOptions['action'],
  overrides: Partial<AdvanceCleanupDeploymentOptions> = {},
): AdvanceCleanupDeploymentOptions {
  return {
    backend,
    store,
    spec: spec(),
    action,
    maxProviderRequests: 9,
    randomUUID: () => OPERATION_ID,
    ...overrides,
  };
}

function start(
  store: CleanupMemoryStore,
  backend: CleanupBackend,
  overrides: Partial<AdvanceCleanupDeploymentOptions> = {},
): Promise<CleanupAdvanceResult> {
  return advanceCleanupDeployment(
    options(store, backend, { kind: 'start' }, overrides),
  );
}

function continueWith(
  store: CleanupMemoryStore,
  backend: CleanupBackend,
  continuation: unknown,
  overrides: Partial<AdvanceCleanupDeploymentOptions> = {},
): Promise<CleanupAdvanceResult> {
  return advanceCleanupDeployment(
    options(
      store,
      backend,
      { kind: 'continue', token: continuation },
      overrides,
    ),
  );
}

function activeIntent(store: CleanupMemoryStore): CleanupAdvanceIntent {
  const intent = store.record?.cleanupIntent;
  if (!intent) throw new Error('expected an active cleanup intent');
  return intent;
}

function r2Resource(
  state: ApplicationR2Resource['state'],
  overrides: Partial<ApplicationR2Resource> = {},
): ApplicationR2Resource {
  const [reserved] = reserveApplicationR2Resources(r2Spec());
  if (!reserved) throw new Error('expected a reserved application resource');
  return { ...reserved, state, creationDate: CREATION_DATE, ...overrides };
}

function r2ActiveRecord(
  state: CleanupAdvanceState,
  resource: ApplicationR2Resource,
): FleetRecord {
  return activeRecord(state, {
    record: {
      applicationResources: [resource],
      desiredSpecDigest: deploymentSpecDigest(r2Spec()),
    },
  });
}

function liveBucket(
  resource: ApplicationR2Resource,
): ApplicationR2BucketSnapshot {
  return {
    name: resource.name,
    bucketName: resource.bucketName,
    jurisdiction: resource.jurisdiction,
    creationDate: resource.creationDate ?? CREATION_DATE,
  };
}

describe('bounded cleanup admission', () => {
  it('starts a manual cleanup into teardown-traffic and persists the admission identity', async () => {
    const { store, backend } = harness();
    store.record = record();
    let uuidCalls = 0;
    const result = await start(store, backend, {
      randomUUID: () => {
        uuidCalls += 1;
        return OPERATION_ID;
      },
    });
    expect(result).toEqual({ status: 'pending', token: token(0) });
    expect(uuidCalls).toBe(1);
    expect(store.puts).toBe(1);
    expect(backend.providerCalls).toBe(0);
    expect(store.record?.phase).toBe('cleanup-advancing');
    const intent = activeIntent(store);
    expect(intent.authority).toEqual({ kind: 'manual-cleanup' });
    expect(intent.identity.admittedPhase).toBe('worker-deployed');
    expect(intent.identity.externalArtifact).toBe(false);
    expect(intent.state).toEqual({ step: 'teardown-traffic' });
  });

  it('starts a reservation cleanup at database-deletion and clears an absent reservation with a receipt', async () => {
    const { store, backend } = harness();
    store.record = record({ phase: 'database-create-authorized' });
    backend.databaseExists = false;
    const started = await start(store, backend);
    expect(started.status).toBe('pending');
    expect(activeIntent(store).state).toEqual({ step: 'database-deletion' });
    const result = await continueWith(store, backend, token(0));
    if (result.status !== 'complete') throw new Error('expected complete');
    expect(result.receipt.disposition).toBe('reservation-cleared');
    expect(result.receipt.admittedPhase).toBe('database-create-authorized');
    expect(result.receipt.evidence).toEqual({
      eligibility: 'reservation-only',
      ingressRemoved: false,
      workerAbsent: false,
      platformResourcesAbsent: false,
      applicationR2Settled: false,
      databaseAbsentReadback: true,
    });
    expect(typeof result.receipt.completedAtMs).toBe('number');
    expect(store.record).toBeUndefined();
    expect(store.receipts.get(OPERATION_ID)).toEqual(result.receipt);
  });

  it('preserves the unauthorized database reservation refusal at admission and in the group', async () => {
    const { store, backend } = harness();
    store.record = record({ phase: 'database-reserved' });
    const message = `refusing to clear an unauthorized database reservation while '${DATABASE_ID}:acme-production' exists`;
    await expect(start(store, backend)).rejects.toThrow(message);
    expect(store.record?.phase).toBe('database-reserved');
    expect(store.record?.cleanupIntent).toBeUndefined();
    const seeded = harness();
    seeded.store.record = activeRecord(
      { step: 'database-deletion' },
      { admittedPhase: 'database-reserved' },
    );
    await expect(
      continueWith(seeded.store, seeded.backend, token(0)),
    ).rejects.toThrow(message);
    expect(seeded.store.record?.cleanupIntent).toBeDefined();
  });

  it('refuses ineligible admissions with fixed messages and zero provider mutations', async () => {
    const cases: readonly Readonly<{
      overrides: Partial<FleetRecord>;
      backendKind?: ProvisioningBackendKind;
      message: string;
    }>[] = [
      {
        overrides: {
          invocationAuthority: { version: 1, authorizedAt: AUTHORIZED_AT },
        },
        message:
          'deployment candidate invocation was durably authorized; use export-backed decommissioning',
      },
      {
        overrides: { invocationAuthority: undefined },
        message:
          'legacy deployment phase cannot rule out candidate invocation; use export-backed decommissioning',
      },
      {
        overrides: { phase: 'maintenance-armed' },
        message:
          'invocation authority carrier is inconsistent with the deployment phase; use export-backed decommissioning',
      },
      {
        overrides: {
          invocationAuthority: {
            version: 2,
          } as unknown as InvocationAuthorityCarrier,
        },
        message:
          'invocation authority carrier is malformed; use export-backed decommissioning',
      },
      {
        overrides: { backend: 'workers-for-platforms' },
        backendKind: 'workers-for-platforms',
        message:
          'deployment carries an untrusted data binding; use export-backed decommissioning',
      },
      {
        overrides: {
          pendingRelease: {
            physicalScriptName: 'acme-production-r2',
            specDigest: 'd'.repeat(64),
            artifactVersion: 'artifact-v2',
            releaseSchemaVersion: 3,
          },
        },
        message:
          'deployment carries external staging evidence; use export-backed decommissioning',
      },
      {
        overrides: { phase: 'publishing' },
        message:
          "deployment in phase 'publishing' requires export-backed decommissioning",
      },
    ];
    for (const testCase of cases) {
      const { store, backend } = harness();
      const seeded = record();
      const overridden: FleetRecord = { ...seeded, ...testCase.overrides };
      if (testCase.overrides.invocationAuthority === undefined) {
        const { invocationAuthority: _carrier, ...legacy } = overridden;
        store.record =
          'invocationAuthority' in testCase.overrides ? legacy : overridden;
      } else {
        store.record = overridden;
      }
      if (testCase.backendKind) backend.kind = testCase.backendKind;
      await expect(start(store, backend)).rejects.toThrow(testCase.message);
      expect(store.puts).toBe(0);
      expect(backend.providerCalls).toBe(0);
      expect(store.record).toEqual(store.record);
    }
    const empty = harness();
    await expect(start(empty.store, empty.backend)).rejects.toThrow(
      'deployment is not registered',
    );
  });

  it('persists the admission externalArtifact and consumes the persisted value over the live backend flag', async () => {
    const refused = harness();
    refused.store.record = record();
    refused.backend.immutableExternalArtifacts = true;
    await expect(start(refused.store, refused.backend)).rejects.toThrow(
      'deployment carries an untrusted data binding; use export-backed decommissioning',
    );
    const { store, backend } = harness();
    store.record = record({ phase: 'database-created' });
    await start(store, backend);
    expect(activeIntent(store).identity.externalArtifact).toBe(false);
    const persisted = harness();
    persisted.store.record = activeRecord(
      { step: 'database-deletion' },
      { admittedPhase: 'database-created' },
    );
    persisted.backend.databaseOwner = undefined;
    persisted.backend.immutableExternalArtifacts = true;
    const result = await continueWith(
      persisted.store,
      persisted.backend,
      token(0),
    );
    expect(result.status).toBe('complete');
  });

  it('validates the provider request budget before any work', async () => {
    for (const budget of [8, 1_001]) {
      const { store, backend } = harness();
      store.record = record();
      await expect(
        start(store, backend, { maxProviderRequests: budget }),
      ).rejects.toThrow(
        'maxProviderRequests must be an integer from 9 to 1000',
      );
      expect(store.puts).toBe(0);
      expect(backend.providerCalls).toBe(0);
    }
  });

  it('refuses to admit teardown cleanup without the bounded start capabilities', async () => {
    const scanless = harness();
    scanless.store.record = record();
    scanless.backend.advanceDecommissionAttachmentScan = undefined;
    await expect(start(scanless.store, scanless.backend)).rejects.toThrow(
      new CleanupAdvanceCapabilityError('attachment-scan').message,
    );
    const residualless = harness();
    residualless.store.record = record();
    residualless.backend.assertDatabaseDeletionResidualsRemoved = undefined;
    await expect(
      start(residualless.store, residualless.backend),
    ).rejects.toThrow(
      new CleanupAdvanceCapabilityError('database-residuals').message,
    );
    expect(scanless.store.puts + residualless.store.puts).toBe(0);
    expect(
      scanless.backend.providerCalls + residualless.backend.providerCalls,
    ).toBe(0);
  });
});

describe('bounded cleanup start resume', () => {
  it('resumes an existing manual cleanup from start without a new operation or classifier re-run', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(
      { step: 'teardown-worker' },
      {
        revision: 4,
        record: {
          invocationAuthority: { version: 1, authorizedAt: AUTHORIZED_AT },
        },
      },
    );
    const result = await start(store, backend, { randomUUID: unexpectedUUID });
    expect(result).toEqual({ status: 'pending', token: token(4) });
    expect(store.puts).toBe(0);
    expect(backend.providerCalls).toBe(0);
  });

  it('returns the blocked result from start when the operation is blocked', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(
      {
        step: 'blocked',
        purpose: {
          kind: 'cleanup-database-pre-delete',
          databaseId: DATABASE_ID,
          operationId: OPERATION_ID,
        },
        attachment: { plane: 'ordinary', scriptName: 'tenant-holder' },
      },
      { revision: 2 },
    );
    const result = await start(store, backend, { randomUUID: unexpectedUUID });
    expect(result).toEqual({
      status: 'blocked',
      token: token(2),
      purpose: {
        kind: 'cleanup-database-pre-delete',
        databaseId: DATABASE_ID,
        operationId: OPERATION_ID,
      },
      attachment: { plane: 'ordinary', scriptName: 'tenant-holder' },
    });
  });

  it('validates the resuming caller before returning the authoritative result', async () => {
    const { store, backend } = harness();
    store.record = activeRecord({ step: 'teardown-worker' });
    backend.kind = 'workers-for-platforms';
    await expect(
      start(store, backend, { randomUUID: unexpectedUUID }),
    ).rejects.toThrow('cleanup backend does not own this deployment');
    backend.kind = 'plain-worker';
    await expect(
      start(store, backend, {
        spec: spec({ databaseName: 'acme-other' }),
        randomUUID: unexpectedUUID,
      }),
    ).rejects.toThrow(
      "deployment 'acme:production' already exists with a different immutable resource mapping",
    );
  });

  it('resumes a provisioning-rollback intent from start only for the requested specification', async () => {
    const authority: CleanupAuthority = {
      kind: 'provisioning-rollback',
      reservationOwned: true,
      databaseOwned: true,
      workerCreatedByAttempt: true,
      workerResourceState: 'unknown',
      requestedSpecDigest: deploymentSpecDigest(spec()),
    };
    const { store, backend } = harness();
    store.record = activeRecord(
      { step: 'teardown-traffic' },
      { authority, revision: 1 },
    );
    const result = await start(store, backend, { randomUUID: unexpectedUUID });
    expect(result).toEqual({ status: 'pending', token: token(1) });
    const mismatched = harness();
    mismatched.store.record = activeRecord(
      { step: 'teardown-traffic' },
      { authority: { ...authority, requestedSpecDigest: 'f'.repeat(64) } },
    );
    await expect(
      start(mismatched.store, mismatched.backend, {
        randomUUID: unexpectedUUID,
      }),
    ).rejects.toThrow('cleanup retry uses a different requested specification');
  });
});

describe('bounded cleanup one-group-per-call', () => {
  it('performs at most one action group per call through the full teardown path', async () => {
    const { store, backend } = harness();
    store.record = record({
      phase: 'platform-resources-deployed',
      platformResources: PLATFORM_RESOURCES,
    });
    backend.scanResults = [scanComplete(), scanComplete()];
    const started = await start(store, backend);
    expect(started).toEqual({ status: 'pending', token: token(0) });
    expect(backend.drainEvents()).toEqual([]);

    const traffic = await continueWith(store, backend, token(0));
    expect(traffic).toEqual({ status: 'pending', token: token(1) });
    expect(backend.drainEvents()).toEqual([
      'removeTraffic',
      'assertTrafficRemoved',
    ]);
    expect(activeIntent(store).state).toEqual({ step: 'teardown-worker' });

    const worker = await continueWith(store, backend, token(1));
    expect(worker).toEqual({ status: 'pending', token: token(2) });
    expect(backend.drainEvents()).toEqual([
      'revokeCredentials',
      'deleteWorker',
    ]);
    expect(activeIntent(store).state).toEqual({ step: 'teardown-platform' });

    const platform = await continueWith(store, backend, token(2));
    expect(platform).toEqual({ status: 'pending', token: token(3) });
    expect(backend.drainEvents()).toEqual([
      'revokePlatformResourceCredentials',
      'deletePlatformResources',
    ]);
    expect(activeIntent(store).state).toEqual({
      step: 'r2-deletion',
      startResourceIndex: 0,
    });

    const r2 = await continueWith(store, backend, token(3));
    expect(r2).toEqual({ status: 'pending', token: token(4) });
    expect(backend.drainEvents()).toEqual([]);
    expect(activeIntent(store).state).toEqual(
      intentFor(store.record as FleetRecord, discoverState()).state,
    );

    const discover = await continueWith(store, backend, token(4));
    expect(discover).toEqual({ status: 'pending', token: token(5) });
    expect(backend.drainEvents()).toEqual(['attachmentScan']);
    const verifyIntent = activeIntent(store);
    if (verifyIntent.state.step !== 'attachment-scan') {
      throw new Error('expected an attachment scan state');
    }
    expect(verifyIntent.state.scan.pass).toBe('verify');
    expect(verifyIntent.state.scan.discoverEvidence).toEqual({
      evidenceSha256: EVIDENCE_A,
      evidenceCount: 2,
    });

    const verify = await continueWith(store, backend, token(5));
    expect(verify).toEqual({ status: 'pending', token: token(6) });
    expect(backend.drainEvents()).toEqual(['attachmentScan']);
    expect(activeIntent(store).state).toEqual({ step: 'database-deletion' });

    const result = await continueWith(store, backend, token(6));
    if (result.status !== 'complete') throw new Error('expected complete');
    expect(backend.drainEvents()).toEqual([
      'getDatabase',
      'readDeploymentIdentity',
      'residuals',
      'deleteDatabase',
      'getDatabase',
    ]);
    expect(result.token).toEqual(token(6));
    expect(result.receipt.disposition).toBe('prepublication-owned-no-export');
    expect(result.receipt.authority).toBe('manual-cleanup');
    expect(result.receipt.admittedPhase).toBe('platform-resources-deployed');
    expect(result.receipt.evidence).toEqual({
      eligibility: 'carrier-null',
      ingressRemoved: true,
      workerAbsent: true,
      platformResourcesAbsent: true,
      applicationR2Settled: true,
      databaseAbsentReadback: true,
    });
    expect(store.record).toBeUndefined();
    expect(store.completeCleanupCalls).toBe(1);
  });

  it('replays a lost teardown transition as a durable no-op', async () => {
    const groups: readonly Readonly<{
      state: CleanupAdvanceState;
      events: readonly string[];
      next: CleanupAdvanceState;
    }>[] = [
      {
        state: { step: 'teardown-traffic' },
        events: ['removeTraffic', 'assertTrafficRemoved'],
        next: { step: 'teardown-worker' },
      },
      {
        state: { step: 'teardown-worker' },
        events: ['revokeCredentials', 'deleteWorker'],
        next: { step: 'teardown-platform' },
      },
      {
        state: { step: 'teardown-platform' },
        events: [
          'revokePlatformResourceCredentials',
          'deletePlatformResources',
        ],
        next: { step: 'r2-deletion', startResourceIndex: 0 },
      },
    ];
    for (const group of groups) {
      const { store, backend } = harness();
      store.record = activeRecord(group.state, {
        admittedPhase: 'platform-resources-deployed',
        record: { platformResources: PLATFORM_RESOURCES },
      });
      const snapshot = store.record;
      const first = await continueWith(store, backend, token(0));
      expect(first).toEqual({ status: 'pending', token: token(1) });
      expect(backend.drainEvents()).toEqual(group.events);
      expect(activeIntent(store).state).toEqual(group.next);
      store.record = snapshot;
      const replayed = await continueWith(store, backend, token(0));
      expect(replayed).toEqual({ status: 'pending', token: token(1) });
      expect(backend.drainEvents()).toEqual(group.events);
      expect(activeIntent(store).state).toEqual(group.next);
    }
  });
});

describe('bounded cleanup application R2 deletion', () => {
  it('persists advanced application resources and moves the start index only on deleted', async () => {
    const { store, backend } = harness();
    const resource = r2Resource('empty');
    backend.liveBuckets.set(resource.bucketName, liveBucket(resource));
    store.record = r2ActiveRecord(
      { step: 'r2-deletion', startResourceIndex: 0 },
      resource,
    );
    const authorized = await continueWith(store, backend, token(0), {
      spec: r2Spec(),
    });
    expect(authorized).toEqual({ status: 'pending', token: token(1) });
    expect(backend.drainEvents()).toEqual([]);
    expect(store.record?.applicationResources?.[0]?.state).toBe(
      'delete-authorized',
    );
    expect(activeIntent(store).state).toEqual({
      step: 'r2-deletion',
      startResourceIndex: 0,
    });
    const deleted = await continueWith(store, backend, token(1), {
      spec: r2Spec(),
    });
    expect(deleted).toEqual({ status: 'pending', token: token(2) });
    expect(backend.drainEvents()).toEqual([
      'findApplicationR2Bucket',
      'deleteApplicationR2Bucket',
      'findApplicationR2Bucket',
    ]);
    expect(store.record?.applicationResources?.[0]?.state).toBe('deleted');
    expect(activeIntent(store).state).toEqual({
      step: 'r2-deletion',
      startResourceIndex: 1,
    });
    const complete = await continueWith(store, backend, token(2), {
      spec: r2Spec(),
    });
    expect(complete).toEqual({ status: 'pending', token: token(3) });
    const scanIntent = activeIntent(store);
    expect(scanIntent.state.step).toBe('attachment-scan');
    expect(scanIntent.generation).toBe(1);
  });

  it('runs the detachment lifecycle through the application-r2-detach capability', async () => {
    const { store, backend } = harness();
    const resource = r2Resource('created');
    backend.liveBuckets.set(resource.bucketName, liveBucket(resource));
    store.record = r2ActiveRecord(
      { step: 'r2-deletion', startResourceIndex: 0 },
      resource,
    );
    const detachRequired = await continueWith(store, backend, token(0), {
      spec: r2Spec(),
    });
    expect(detachRequired).toEqual({ status: 'pending', token: token(1) });
    expect(backend.drainEvents()).toEqual([
      'findApplicationR2Bucket',
      'assertApplicationR2Detached',
    ]);
    expect(store.record?.applicationResources?.[0]?.state).toBe(
      'detach-authorized',
    );
    expect(activeIntent(store).state).toEqual({
      step: 'r2-deletion',
      startResourceIndex: 0,
      verifiedDetachmentResourceIndex: 0,
    });
    const detached = await continueWith(store, backend, token(1), {
      spec: r2Spec(),
    });
    expect(detached).toEqual({ status: 'pending', token: token(2) });
    expect(backend.drainEvents()).toEqual([]);
    expect(store.record?.applicationResources?.[0]?.state).toBe('detached');
    const state = activeIntent(store).state;
    expect(state).toEqual({ step: 'r2-deletion', startResourceIndex: 0 });
    expect(Object.hasOwn(state, 'verifiedDetachmentResourceIndex')).toBe(false);
  });

  it('persists nothing when the detachment assertion fails and replay re-derives the requirement', async () => {
    const { store, backend } = harness();
    const resource = r2Resource('created');
    backend.liveBuckets.set(resource.bucketName, liveBucket(resource));
    store.record = r2ActiveRecord(
      { step: 'r2-deletion', startResourceIndex: 0 },
      resource,
    );
    backend.detachFailure = new Error('bucket is still attached');
    await expect(
      continueWith(store, backend, token(0), { spec: r2Spec() }),
    ).rejects.toThrow('bucket is still attached');
    expect(store.puts).toBe(0);
    expect(store.record?.applicationResources?.[0]?.state).toBe('created');
    expect(activeIntent(store).revision).toBe(0);
    backend.detachFailure = undefined;
    backend.drainEvents();
    const retried = await continueWith(store, backend, token(0), {
      spec: r2Spec(),
    });
    expect(retried).toEqual({ status: 'pending', token: token(1) });
    expect(backend.drainEvents()).toEqual([
      'findApplicationR2Bucket',
      'assertApplicationR2Detached',
    ]);
    expect(activeIntent(store).state).toEqual({
      step: 'r2-deletion',
      startResourceIndex: 0,
      verifiedDetachmentResourceIndex: 0,
    });
  });
});

describe('bounded cleanup attachment scan', () => {
  it('targets the deployment database for cleanup attachment scans', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(discoverState());
    backend.scanResults = [
      {
        status: 'pending',
        progress: initialWorkerAttachmentScan({
          kind: 'd1',
          databaseId: DATABASE_ID,
        }),
        providerFetchAttemptsReserved: 3,
      },
    ];
    const pending = await continueWith(store, backend, token(0));
    expect(pending).toEqual({ status: 'pending', token: token(1) });
    const input = backend.scanInputs[0];
    expect(input?.progress.target).toEqual({
      kind: 'd1',
      databaseId: DATABASE_ID,
    });
    backend.scanResults = [
      {
        status: 'pending',
        progress: initialWorkerAttachmentScan({
          kind: 'r2',
          bucketName: 'stray-bucket',
        }),
        providerFetchAttemptsReserved: 3,
      },
    ];
    await expect(continueWith(store, backend, token(1))).rejects.toThrow(
      'bounded cleanup attachment result is malformed',
    );
  });

  it('passes the call-local abort signal to the scan without persisting it', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(discoverState());
    backend.scanResults = [scanComplete()];
    const controller = new AbortController();
    await continueWith(store, backend, token(0), { signal: controller.signal });
    expect(backend.scanInputs[0]?.signal).toBe(controller.signal);
    expect(JSON.stringify(store.record)).not.toContain('signal');
  });

  it('consumes matching discover and verify evidence into the database-deletion transition', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(verifyState());
    backend.scanResults = [scanComplete()];
    const consumed = await continueWith(store, backend, token(0));
    expect(consumed).toEqual({ status: 'pending', token: token(1) });
    expect(backend.drainEvents()).toEqual(['attachmentScan']);
    expect(activeIntent(store).state).toEqual({ step: 'database-deletion' });
    const result = await continueWith(store, backend, token(1));
    if (result.status !== 'complete') throw new Error('expected complete');
    expect(result.receipt.evidence.scan).toBeUndefined();
    expect(result.receipt.disposition).toBe('prepublication-owned-no-export');
  });

  it('restarts a new discover generation when verify evidence mismatches', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(verifyState());
    backend.scanResults = [scanComplete(EVIDENCE_B, 3)];
    const restarted = await continueWith(store, backend, token(0));
    expect(restarted).toEqual({ status: 'pending', token: token(1) });
    const intent = activeIntent(store);
    expect(intent.generation).toBe(1);
    if (intent.state.step !== 'attachment-scan') {
      throw new Error('expected an attachment scan state');
    }
    expect(intent.state.scan.pass).toBe('discover');
    expect(intent.state.scan.progress).toEqual(
      initialWorkerAttachmentScan({ kind: 'd1', databaseId: DATABASE_ID }),
    );
    expect(backend.events).not.toContain('getDatabase');
    expect(backend.events).not.toContain('deleteDatabase');
  });

  it('persists a safe blocked attachment and restarts only through restart-blocked', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(discoverState());
    backend.scanResults = [scanAttached()];
    const blocked = await continueWith(store, backend, token(0));
    expect(blocked).toEqual({
      status: 'blocked',
      token: token(1),
      purpose: {
        kind: 'cleanup-database-pre-delete',
        databaseId: DATABASE_ID,
        operationId: OPERATION_ID,
      },
      attachment: {
        plane: 'dispatch',
        scriptName: 'tenant-holder',
        dispatchNamespace: 'tenants',
      },
    });
    backend.drainEvents();
    const stillBlocked = await continueWith(store, backend, token(1));
    expect(stillBlocked.status).toBe('blocked');
    expect(backend.providerCalls).toBe(1);
    expect(backend.drainEvents()).toEqual([]);
    const restarted = await advanceCleanupDeployment(
      options(store, backend, { kind: 'restart-blocked', token: token(1) }),
    );
    expect(restarted).toEqual({ status: 'pending', token: token(2) });
    const intent = activeIntent(store);
    expect(intent.generation).toBe(1);
    if (intent.state.step !== 'attachment-scan') {
      throw new Error('expected an attachment scan state');
    }
    expect(intent.state.scan.pass).toBe('discover');
    await expect(
      advanceCleanupDeployment(
        options(store, backend, { kind: 'restart-blocked', token: token(2) }),
      ),
    ).rejects.toThrow(CleanupAdvanceRestartError);
  });

  it('returns blocked again when a restarted scan still finds an attachment', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(
      {
        step: 'blocked',
        purpose: {
          kind: 'cleanup-database-pre-delete',
          databaseId: DATABASE_ID,
          operationId: OPERATION_ID,
        },
        attachment: { plane: 'ordinary', scriptName: 'tenant-holder' },
      },
      { revision: 3, generation: 1 },
    );
    const restarted = await advanceCleanupDeployment(
      options(store, backend, { kind: 'restart-blocked', token: token(3) }),
    );
    expect(restarted).toEqual({ status: 'pending', token: token(4) });
    expect(activeIntent(store).generation).toBe(2);
    backend.scanResults = [scanAttached()];
    const blockedAgain = await continueWith(store, backend, token(4));
    expect(blockedAgain.status).toBe('blocked');
    const intent = activeIntent(store);
    expect(intent.state.step).toBe('blocked');
  });
});

describe('bounded cleanup tokens', () => {
  it('returns the authoritative result for a stale token without provider work', async () => {
    const { store, backend } = harness();
    store.record = activeRecord({ step: 'teardown-worker' }, { revision: 2 });
    const result = await continueWith(store, backend, token(1));
    expect(result).toEqual({ status: 'pending', token: token(2) });
    expect(store.puts).toBe(0);
    expect(backend.providerCalls).toBe(0);
  });

  it('rejects a future token', async () => {
    const { store, backend } = harness();
    store.record = activeRecord({ step: 'teardown-worker' }, { revision: 2 });
    await expect(continueWith(store, backend, token(5))).rejects.toThrow(
      CleanupAdvanceTokenFutureError,
    );
  });

  it('rejects a token for another deployment before taking the lease', async () => {
    const { store, backend } = harness();
    store.record = activeRecord({ step: 'teardown-worker' });
    await expect(
      continueWith(
        store,
        backend,
        token(0, OPERATION_ID, { tenantTag: 'zeta' }),
      ),
    ).rejects.toThrow(CleanupAdvanceTokenDeploymentError);
    expect(store.puts).toBe(0);
    expect(backend.providerCalls).toBe(0);
  });

  it('adjudicates delayed tokens against the immutable receipt', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(
      { step: 'database-deletion' },
      { admittedPhase: 'database-create-authorized' },
    );
    backend.databaseExists = false;
    const terminal = await continueWith(store, backend, token(0));
    if (terminal.status !== 'complete') throw new Error('expected complete');
    const delayed = await continueWith(store, backend, token(0));
    expect(delayed).toEqual({
      status: 'complete',
      token: token(0),
      receipt: terminal.receipt,
    });
  });

  it('returns the old receipt across a same-key reprovision and never touches the new row', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(
      { step: 'database-deletion' },
      { admittedPhase: 'database-create-authorized' },
    );
    backend.databaseExists = false;
    const terminal = await continueWith(store, backend, token(0));
    if (terminal.status !== 'complete') throw new Error('expected complete');
    const reprovisioned = record({ phase: 'worker-deployed' });
    store.record = reprovisioned;
    const putsBefore = store.puts;
    const delayed = await continueWith(store, backend, token(0));
    expect(delayed).toEqual({
      status: 'complete',
      token: token(0),
      receipt: terminal.receipt,
    });
    expect(store.record).toEqual(reprovisioned);
    expect(store.puts).toBe(putsBefore);
    store.record = activeRecord({ step: 'teardown-traffic' }, { record: {} });
    store.record = {
      ...store.record,
      cleanupIntent: {
        ...activeIntent(store),
        operationId: OTHER_OPERATION_ID,
        state: { step: 'teardown-traffic' },
      },
    };
    const acrossNewOperation = await continueWith(store, backend, token(0));
    expect(acrossNewOperation.status).toBe('complete');
    expect(activeIntent(store).operationId).toBe(OTHER_OPERATION_ID);
    expect(activeIntent(store).revision).toBe(0);
  });

  it('rejects a delayed token after its receipt is pruned', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(
      { step: 'database-deletion' },
      { admittedPhase: 'database-create-authorized' },
    );
    backend.databaseExists = false;
    const terminal = await continueWith(store, backend, token(0));
    expect(terminal.status).toBe('complete');
    const pruned = await store.pruneCleanupReceipts?.({
      completedBeforeMs: store.completedAtMs + 1,
      limit: 1_000,
    });
    expect(pruned).toEqual({ deleted: 1 });
    await expect(continueWith(store, backend, token(0))).rejects.toThrow(
      CleanupAdvanceTokenOperationError,
    );
  });

  it('rejects cross-purpose tokens at the engine boundary', async () => {
    const { store, backend } = harness();
    store.record = record({
      phase: 'decommission-advancing',
      decommissionIntent: {
        operationId: OPERATION_ID,
      } as unknown as DecommissionAdvanceIntent,
    });
    await expect(continueWith(store, backend, token(0))).rejects.toThrow(
      CleanupAdvanceTokenOperationError,
    );
    const cleanup = harness();
    cleanup.store.record = activeRecord({ step: 'teardown-traffic' });
    await expect(
      advanceDecommissionDeployment({
        backend: cleanup.backend,
        store: cleanup.store,
        spec: spec(),
        action: { kind: 'continue', token: token(0) },
        maxProviderRequests: 9,
        randomUUID: unexpectedUUID,
      }),
    ).rejects.toThrow(DecommissionAdvanceTokenOperationError);
  });

  it('converges a lost terminal write through the receipt lookup', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(
      { step: 'database-deletion' },
      { admittedPhase: 'database-create-authorized' },
    );
    backend.databaseExists = false;
    store.failTerminalResponseOnce = true;
    await expect(continueWith(store, backend, token(0))).rejects.toThrow(
      'terminal response lost',
    );
    expect(store.record).toBeUndefined();
    expect(store.receipts.has(OPERATION_ID)).toBe(true);
    const converged = await continueWith(store, backend, token(0));
    if (converged.status !== 'complete') throw new Error('expected complete');
    expect(converged.receipt).toEqual(store.receipts.get(OPERATION_ID));
  });
});

describe('bounded cleanup capabilities', () => {
  it('fails closed without provider calls when the terminal receipt capability is missing', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(
      { step: 'database-deletion' },
      { admittedPhase: 'database-create-authorized' },
    );
    store.supportsCompleteCleanup = false;
    await expect(continueWith(store, backend, token(0))).rejects.toThrow(
      new CleanupAdvanceCapabilityError('terminal-receipt').message,
    );
    expect(backend.providerCalls).toBe(0);
    expect(store.puts).toBe(0);
  });

  it('fails closed when the store cannot read cleanup receipts', async () => {
    const { store, backend } = harness();
    store.readCleanupReceipt = undefined;
    await expect(continueWith(store, backend, token(0))).rejects.toThrow(
      new CleanupAdvanceCapabilityError('receipt-read').message,
    );
    expect(backend.providerCalls).toBe(0);
  });

  it('fails closed without provider calls when the attachment scan capability is missing', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(discoverState());
    backend.advanceDecommissionAttachmentScan = undefined;
    await expect(continueWith(store, backend, token(0))).rejects.toThrow(
      new CleanupAdvanceCapabilityError('attachment-scan').message,
    );
    expect(backend.providerCalls).toBe(0);
    expect(store.puts).toBe(0);
  });

  it('fails closed without provider calls when an application R2 capability is missing', async () => {
    const cases: readonly Readonly<{
      state: ApplicationR2Resource['state'];
      omit:
        | 'findApplicationR2Bucket'
        | 'assertApplicationR2Empty'
        | 'assertApplicationR2Detached'
        | 'deleteApplicationR2Bucket';
      capability:
        | 'application-r2-inspection'
        | 'application-r2-empty'
        | 'application-r2-detach'
        | 'application-r2-delete';
    }>[] = [
      {
        state: 'created',
        omit: 'findApplicationR2Bucket',
        capability: 'application-r2-inspection',
      },
      {
        state: 'empty-authorized',
        omit: 'assertApplicationR2Empty',
        capability: 'application-r2-empty',
      },
      {
        state: 'created',
        omit: 'assertApplicationR2Detached',
        capability: 'application-r2-detach',
      },
      {
        state: 'delete-authorized',
        omit: 'deleteApplicationR2Bucket',
        capability: 'application-r2-delete',
      },
    ];
    for (const testCase of cases) {
      const { store, backend } = harness();
      const resource = r2Resource(testCase.state);
      backend.liveBuckets.set(resource.bucketName, liveBucket(resource));
      store.record = r2ActiveRecord(
        { step: 'r2-deletion', startResourceIndex: 0 },
        resource,
      );
      backend[testCase.omit] = undefined;
      await expect(
        continueWith(store, backend, token(0), { spec: r2Spec() }),
      ).rejects.toThrow(
        new CleanupAdvanceCapabilityError(testCase.capability).message,
      );
      expect(backend.providerCalls).toBe(0);
      expect(store.puts).toBe(0);
    }
  });
});

describe('bounded cleanup database deletion', () => {
  it('reconciles a database-created cleanup without the owner requirement', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(
      { step: 'database-deletion' },
      { admittedPhase: 'database-created' },
    );
    backend.databaseOwner = undefined;
    const result = await continueWith(store, backend, token(0));
    expect(result.status).toBe('complete');
    expect(backend.events).toEqual([
      'getDatabase',
      'residuals',
      'deleteDatabase',
      'getDatabase',
    ]);
    const owned = harness();
    owned.store.record = activeRecord(
      { step: 'database-deletion' },
      { admittedPhase: 'identity-seeded' },
    );
    owned.backend.databaseOwner = undefined;
    await expect(
      continueWith(owned.store, owned.backend, token(0)),
    ).rejects.toThrow(
      `refusing database operation for '${DATABASE_ID}' owned by 'no deployment'`,
    );
  });

  it('clears an authorized reservation with the pinned freshness proof', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(
      { step: 'database-deletion' },
      { admittedPhase: 'database-create-authorized' },
    );
    backend.databaseOwner = undefined;
    const result = await continueWith(store, backend, token(0));
    if (result.status !== 'complete') throw new Error('expected complete');
    expect(backend.seededFenceStates).toEqual(['migration-locked']);
    expect(result.receipt.disposition).toBe('prepublication-owned-no-export');
    expect(result.receipt.evidence.eligibility).toBe('reservation-only');
    expect(backend.databaseExists).toBe(false);
    const ownedElsewhere = harness();
    ownedElsewhere.store.record = activeRecord(
      { step: 'database-deletion' },
      { admittedPhase: 'database-create-authorized' },
    );
    ownedElsewhere.backend.databaseOwner = 'zeta';
    await expect(
      continueWith(ownedElsewhere.store, ownedElsewhere.backend, token(0)),
    ).rejects.toThrow(
      `refusing reserved database cleanup for '${DATABASE_ID}' owned by 'zeta'`,
    );
  });

  it('re-checks eligibility with the persisted phase and the live carrier before deletion', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(
      { step: 'database-deletion' },
      { admittedPhase: 'worker-deployed' },
    );
    const result = await continueWith(store, backend, token(0));
    expect(result.status).toBe('complete');
    const authorized = harness();
    authorized.store.record = activeRecord(
      { step: 'database-deletion' },
      {
        admittedPhase: 'worker-deployed',
        record: {
          invocationAuthority: { version: 1, authorizedAt: AUTHORIZED_AT },
        },
      },
    );
    await expect(
      continueWith(authorized.store, authorized.backend, token(0)),
    ).rejects.toThrow(
      'deployment candidate invocation was durably authorized; use export-backed decommissioning',
    );
    expect(authorized.backend.events).not.toContain('deleteDatabase');
    expect(authorized.store.completeCleanupCalls).toBe(0);
    expect(authorized.store.record?.cleanupIntent?.revision).toBe(0);
  });

  it('refuses rollback database deletion the attempt does not own', async () => {
    const { store, backend } = harness();
    store.record = activeRecord(
      { step: 'database-deletion' },
      {
        admittedPhase: 'identity-seeded',
        authority: {
          kind: 'provisioning-rollback',
          reservationOwned: true,
          databaseOwned: false,
          workerCreatedByAttempt: false,
          workerResourceState: 'absent',
          requestedSpecDigest: deploymentSpecDigest(spec()),
        },
      },
    );
    await expect(continueWith(store, backend, token(0))).rejects.toThrow(
      'provisioning rollback cannot delete a database the attempt does not own',
    );
    expect(backend.events).not.toContain('deleteDatabase');
    expect(store.record?.cleanupIntent).toBeDefined();
  });
});

describe('bounded cleanup rollback internals', () => {
  it('persists rollback authority under a held lease and drains through the under-lease entry', async () => {
    const { store, backend } = harness();
    const seeded = record({ phase: 'database-created' });
    store.record = seeded;
    const authority = {
      kind: 'provisioning-rollback',
      reservationOwned: true,
      databaseOwned: true,
      workerCreatedByAttempt: true,
      workerResourceState: 'unknown',
      requestedSpecDigest: deploymentSpecDigest(spec()),
    } as const;
    const result = await store.withDeploymentLease(
      'acme',
      'production',
      async (lease) => {
        const admitted = await startProvisioningRollbackCleanup(
          lease,
          seeded,
          authority,
          { backend, spec: spec(), randomUUID: () => OPERATION_ID },
        );
        expect(admitted.cleanupIntent?.authority).toEqual(authority);
        expect(admitted.cleanupIntent?.state).toEqual({
          step: 'teardown-traffic',
        });
        expect(admitted.cleanupIntent?.identity.admittedPhase).toBe(
          'database-created',
        );
        const drainOptions = options(store, backend, {
          kind: 'continue',
          token: token(0),
        });
        return advanceCleanupUnderLease(
          drainOptions,
          { kind: 'continue', token: token(0) },
          token(0),
          lease,
        );
      },
    );
    expect(result).toEqual({ status: 'pending', token: token(1) });
    expect(backend.events).toEqual(['removeTraffic', 'assertTrafficRemoved']);
    expect(activeIntent(store).state).toEqual({ step: 'teardown-worker' });
  });
});
