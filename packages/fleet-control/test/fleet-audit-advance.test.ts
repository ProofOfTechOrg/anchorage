// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { auditFleetDrift, type DriftFinding } from '../src/fleet.js';
import {
  type AdvanceFleetAuditOptions,
  abandonFleetAuditOperation,
  advanceFleetAudit,
  type FleetAuditAdvanceAction,
  FleetAuditAdvanceCapabilityError,
  type FleetAuditAdvanceResult,
  readFleetAuditFindingsPage,
} from '../src/fleet-audit-advance.js';
import {
  type FleetAuditProgress,
  type FleetAuditStage,
  fleetAuditFactRowFromUnknown,
  fleetAuditProgressFromUnknown,
} from '../src/fleet-audit-state.js';
import {
  canonicalFleetOperationBytes,
  FLEET_OPERATION_INTAKE_BYTE_BOUND,
  FLEET_OPERATION_ITEM_BOUND,
  FLEET_OPERATION_NODE_BOUND,
  FLEET_OPERATION_RECORD_ROW_BYTE_BOUND,
  FLEET_OPERATION_ROW_PAYLOAD_BYTE_BOUND,
  FLEET_OPERATION_ROW_READ_BOUND,
  FLEET_OPERATION_STRING_BYTE_BOUND,
  type FleetOperationKind,
  type FleetOperationLease,
  type FleetOperationRunRecord,
  type FleetOperationStagedRow,
  type FleetOperationStore,
  FleetOperationTokenFutureError,
  FleetOperationTokenKindError,
  FleetOperationTokenOperationError,
  fleetOperationIntakeDigest,
  fleetOperationItemsIntake,
  readAllFleetOperationRows,
} from '../src/fleet-operation-state.js';
import { providerBindingIdentitiesForInspection } from '../src/provider-binding-inventory.js';
import type {
  DeploymentSpec,
  FleetInventoryDeployment,
  FleetInventoryFinding,
  FleetRecord,
  FleetResourceInventory,
  FleetStateLease,
  FleetStateStore,
  LiveDeployment,
  MaintenanceHealth,
  ProvisioningBackend,
  ProvisioningBackendKind,
} from '../src/types.js';
import {
  AUDIT_NOW,
  FakeOperationStore,
  RegisteredGenerationInventoryRunStore,
  uuidFor,
} from './fixtures/fleet-operation-fakes.js';

// ---------------------------------------------------------------------------
// Fixed identities, clocks, and small builders. This world is INLINE and
// INDEPENDENT of `test/fixtures/fleet-audit-world.ts`: it never imports that
// fixture. The durable operation and inventory store fakes come from
// `test/fixtures/fleet-operation-fakes.ts`, which carries no world of its own.
// ---------------------------------------------------------------------------

/**
 * A view of `target` with exactly one method hidden — including inherited
 * (prototype) methods, unlike an object spread, which drops every method a
 * class declares on its prototype rather than as an instance field.
 */
function withoutMethod<T extends object>(target: T, method: keyof T): T {
  return new Proxy(target, {
    get(obj, prop, receiver) {
      if (prop === method) return undefined;
      const value = Reflect.get(obj, prop, receiver);
      return typeof value === 'function' ? value.bind(obj) : value;
    },
    has(obj, prop) {
      if (prop === method) return false;
      return Reflect.has(obj, prop);
    },
  });
}

type RowsPageInput = Parameters<
  FleetOperationStore['readOperationRowsPage']
>[0];
type RowsPage = Awaited<
  ReturnType<FleetOperationStore['readOperationRowsPage']>
>;

function pageTransformingStore(
  store: FleetOperationStore,
  transform: (
    readPage: () => Promise<RowsPage>,
    input: RowsPageInput,
  ) => RowsPage | Promise<RowsPage>,
): FleetOperationStore {
  return new Proxy(store, {
    get(target, property, receiver) {
      if (property === 'readOperationRowsPage') {
        return async (input: RowsPageInput) =>
          transform(() => target.readOperationRowsPage(input), input);
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

type StartOperationInput = Parameters<FleetOperationLease['startOperation']>[0];
type StageRowsInput = Parameters<FleetOperationLease['stageRows']>[0];
type CommitProgressInput = Parameters<FleetOperationLease['commitProgress']>[0];

/**
 * A `FakeOperationStore` view whose `created` outcome persists — and echoes —
 * `persistedGeneration` instead of the generation the coordinator submitted,
 * and whose first call to `fault.member` throws `fault.error` without writing.
 * The fake hands its callback a plain lease object, so overriding two members
 * is a spread rather than a second proxy.
 */
function divergentCreateStore(
  store: FakeOperationStore,
  persistedGeneration: number,
  fault: Readonly<{ member: 'stageRows' | 'commitProgress'; error: Error }>,
): FleetOperationStore {
  let faultsRemaining = 1;
  const raiseOnce = (): void => {
    if (faultsRemaining > 0) {
      faultsRemaining -= 1;
      throw fault.error;
    }
  };
  return new Proxy(store, {
    get(target, property, receiver) {
      if (property === 'withAccountOperationLease') {
        return <T>(
          kind: FleetOperationKind,
          operation: (lease: FleetOperationLease) => Promise<T>,
        ): Promise<T> =>
          target.withAccountOperationLease(kind, (lease) =>
            operation({
              ...lease,
              startOperation: (input: StartOperationInput) => {
                const progress: FleetAuditProgress = {
                  ...fleetAuditProgressFromUnknown(input.runRecord.progress),
                  generation: persistedGeneration,
                };
                return lease.startOperation({
                  ...input,
                  runRecord: { ...input.runRecord, progress },
                });
              },
              stageRows: (input: StageRowsInput) => {
                if (fault.member === 'stageRows') raiseOnce();
                return lease.stageRows(input);
              },
              commitProgress: (input: CommitProgressInput) => {
                if (fault.member === 'commitProgress') raiseOnce();
                return lease.commitProgress(input);
              },
            }),
          );
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

const ENVIRONMENT = 'production';
const SPEC_DIGEST = 'a'.repeat(64);
const STALE_AFTER_MS = 3_600_000;
const FRESH_UPDATED_AT = new Date(AUDIT_NOW - 30 * 60_000).toISOString();

const HEALTHY_MAINTENANCE: MaintenanceHealth = {
  armed: true,
  nextAlarmAt: AUDIT_NOW + 60_000,
  lastSweepAt: AUDIT_NOW - 60_000,
  lastPurgeAt: AUDIT_NOW - 60_000,
};

const UNARMED_MAINTENANCE: MaintenanceHealth = {
  armed: false,
  nextAlarmAt: null,
  lastSweepAt: null,
  lastPurgeAt: null,
};

function baseRecord(
  tenantTag: string,
  overrides: Partial<FleetRecord> = {},
): FleetRecord {
  return {
    tenantTag,
    backend: 'plain-worker',
    environment: ENVIRONMENT,
    scriptName: `${tenantTag}-worker`,
    databaseId: `db-${tenantTag}`,
    databaseName: `database-${tenantTag}`,
    schemaVersion: 1,
    artifactVersion: 'v1',
    desiredSpecDigest: SPEC_DIGEST,
    durableObjectBindings: [
      { name: 'RUNNER', className: 'Runner', namespaceId: `ns-${tenantTag}` },
    ],
    routeHostname: `${tenantTag}.example.test`,
    phase: 'ready',
    updatedAt: FRESH_UPDATED_AT,
    ...overrides,
  };
}

function plainDataMetrics(value: unknown): {
  nodeCount: number;
  maxStringBytes: number;
} {
  let nodeCount = 0;
  let maxStringBytes = 0;
  const encoder = new TextEncoder();
  const pending = [value];
  while (pending.length > 0) {
    const current = pending.pop();
    nodeCount += 1;
    if (typeof current === 'string') {
      maxStringBytes = Math.max(
        maxStringBytes,
        encoder.encode(current).byteLength,
      );
    } else if (Array.isArray(current)) pending.push(...current);
    else if (current && typeof current === 'object') {
      pending.push(...Object.values(current));
    }
  }
  return { nodeCount, maxStringBytes };
}

function specForRecord(
  record: FleetRecord,
  overrides: Partial<DeploymentSpec> = {},
): DeploymentSpec {
  return {
    tenantTag: record.tenantTag,
    environment: record.environment,
    scriptName: record.scriptName,
    databaseName: record.databaseName,
    compatibilityDate: '2026-05-01',
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    authoredBy: 'external',
    schemaVersion: record.schemaVersion,
    migrations: [],
    durableObjectMigrations: [],
    durableObjectBindings: [],
    maintenanceBaseUrl: `https://control-${record.scriptName}.example.test`,
    routeHostname: record.routeHostname,
    ...overrides,
  };
}

function cleanLiveDeployment(
  record: FleetRecord,
  overrides: Partial<LiveDeployment> = {},
): LiveDeployment {
  const base = {
    tenantTag: record.tenantTag,
    environment: record.environment,
    scriptName: record.scriptName,
    databaseId: record.databaseId,
    durableObjectBindings: record.durableObjectBindings,
    plainTextBindings: {},
    secretNames: [] as readonly string[],
    artifactVersion: record.artifactVersion,
    desiredSpecDigest: record.desiredSpecDigest,
    schemaVersion: record.schemaVersion,
    maintenance: HEALTHY_MAINTENANCE,
    ...overrides,
  };
  return {
    ...base,
    providerBindingIdentities: providerBindingIdentitiesForInspection({
      ...base,
      databaseIds: [base.databaseId],
    }),
  };
}

function cleanInventoryDeployment(
  record: FleetRecord,
  overrides: Partial<FleetInventoryDeployment> = {},
): FleetInventoryDeployment {
  return {
    backend: record.backend,
    scriptName: record.scriptName,
    tenantTag: record.tenantTag,
    environment: record.environment,
    databaseIds: [record.databaseId],
    durableObjectBindings: record.durableObjectBindings,
    secretNames: [],
    plainTextBindings: {},
    routeHostnames: [record.routeHostname],
    artifactVersion: record.artifactVersion,
    desiredSpecDigest: record.desiredSpecDigest,
    schemaVersion: record.schemaVersion,
    ...overrides,
  };
}

function cleanRoute(
  record: FleetRecord,
  overrides: Partial<FleetResourceInventory['routes'][number]> = {},
): FleetResourceInventory['routes'][number] {
  return {
    backend: record.backend,
    hostname: record.routeHostname,
    scriptName: record.scriptName,
    tenantTag: record.tenantTag,
    environment: record.environment,
    ...overrides,
  };
}

/** A test-mutable variant of `FleetResourceInventory` (readonly at the public boundary). */
interface MutableInventory {
  unavailableR2Jurisdictions: FleetResourceInventory['unavailableR2Jurisdictions'];
  findings: FleetInventoryFinding[];
  scriptRegistrations: FleetResourceInventory['scriptRegistrations'][number][];
  deployments: FleetInventoryDeployment[];
  databaseIds: string[];
  namespaceIds: string[];
  r2Buckets: NonNullable<FleetResourceInventory['r2Buckets']>[number][];
  routes: FleetResourceInventory['routes'][number][];
  hostRoutingKvId?: string;
}

function emptyInventory(): MutableInventory {
  return {
    findings: [],
    unavailableR2Jurisdictions: Object.freeze([]),
    scriptRegistrations: [],
    deployments: [],
    databaseIds: [],
    namespaceIds: [],
    r2Buckets: [],
    routes: [],
  };
}

/** A clean inventory matching `records` exactly (zero drift). */
function inventoryFor(records: readonly FleetRecord[]): MutableInventory {
  return {
    findings: [],
    unavailableR2Jurisdictions: Object.freeze([]),
    scriptRegistrations: [],
    deployments: records.map((record) => cleanInventoryDeployment(record)),
    databaseIds: records.map((record) => record.databaseId),
    namespaceIds: records.flatMap((record) =>
      record.durableObjectBindings.map((binding) => binding.namespaceId),
    ),
    r2Buckets: [],
    routes: records.map((record) => cleanRoute(record)),
  };
}

class SimpleBackend implements ProvisioningBackend {
  readonly kind: ProvisioningBackendKind = 'plain-worker';

  constructor(
    private readonly liveByTenant: Map<string, LiveDeployment | undefined>,
    private readonly opsLog: string[] = [],
    private readonly throwOnInspect = new Set<string>(),
    private readonly throwOnEnsureMaintenance = new Set<string>(),
  ) {}

  async findDatabase(): Promise<never> {
    throw new Error('unused');
  }
  async getDatabase(): Promise<never> {
    throw new Error('unused');
  }
  async ensureDatabase(): Promise<never> {
    throw new Error('unused');
  }
  async seedDeploymentIdentity(): Promise<void> {
    throw new Error('unused');
  }
  async readDeploymentIdentity(): Promise<never> {
    throw new Error('unused');
  }
  async applyMigrations(): Promise<void> {
    throw new Error('unused');
  }
  async deployWorker(): Promise<never> {
    throw new Error('unused');
  }
  async promoteWorker(): Promise<void> {
    throw new Error('unused');
  }

  async ensureMaintenance(
    spec: DeploymentSpec,
    _maintenanceAdminSecret: string,
    _lease: FleetStateLease,
  ): Promise<MaintenanceHealth> {
    this.opsLog.push('ensureMaintenance');
    if (this.throwOnEnsureMaintenance.has(spec.tenantTag)) {
      throw new Error('maintenance re-arm blew up');
    }
    return HEALTHY_MAINTENANCE;
  }

  async inspect(spec: DeploymentSpec): Promise<LiveDeployment | undefined> {
    this.opsLog.push(`inspect:${spec.tenantTag}`);
    if (this.throwOnInspect.has(spec.tenantTag)) {
      throw new Error('inspection blew up');
    }
    return this.liveByTenant.get(spec.tenantTag);
  }

  async attestActiveRoute(): Promise<never> {
    throw new Error('unused');
  }
  async removeTraffic(): Promise<void> {
    throw new Error('unused');
  }
  async assertTrafficRemoved(): Promise<void> {
    throw new Error('unused');
  }
  async revokeCredentials(): Promise<void> {
    throw new Error('unused');
  }
  async deleteWorker(): Promise<void> {
    throw new Error('unused');
  }
  async assertDatabaseDetached(): Promise<void> {
    throw new Error('unused');
  }
  async exportDatabase(): Promise<never> {
    throw new Error('unused');
  }
  async deleteDatabase(): Promise<void> {
    throw new Error('unused');
  }
}

class FakeFleetStateStore implements FleetStateStore {
  readonly records = new Map<string, FleetRecord>();
  readonly ops: string[] = [];

  constructor(records: readonly FleetRecord[] = []) {
    for (const record of records) {
      this.records.set(`${record.tenantTag}:${record.environment}`, record);
    }
  }

  async withDeploymentLease<T>(
    tenantTag: string,
    environment: string,
    operation: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T> {
    this.ops.push('withDeploymentLease');
    const key = `${tenantTag}:${environment}`;
    const lease: FleetStateLease = {
      tenantTag,
      environment,
      mutationLeaseTtlMs: 900_000,
      assertOwned: async () => {
        this.ops.push('assertOwned');
      },
      renew: async () => {},
      put: async (record) => {
        this.ops.push('put');
        this.records.set(key, record);
      },
      delete: async () => {
        this.records.delete(key);
      },
    };
    return operation(lease);
  }

  async get(
    tenantTag: string,
    environment: string,
  ): Promise<FleetRecord | undefined> {
    this.ops.push('get');
    return this.records.get(`${tenantTag}:${environment}`);
  }

  async list(): Promise<readonly FleetRecord[]> {
    return [...this.records.values()];
  }
}

// ---------------------------------------------------------------------------
// Shared drive helpers.
// ---------------------------------------------------------------------------

interface Harness {
  readonly records: readonly FleetRecord[];
  readonly inventory: FleetResourceInventory;
  readonly operationStore: FakeOperationStore;
  readonly inventoryStore: RegisteredGenerationInventoryRunStore;
  readonly fleetStore: FakeFleetStateStore;
  readonly backend: SimpleBackend;
  readonly opsLog: string[];
  readonly liveByTenant: Map<string, LiveDeployment | undefined>;
  readonly specByTenant: Map<string, DeploymentSpec>;
  readonly secretByTenant: Map<string, string>;
  baseOptions(action: FleetAuditAdvanceAction): AdvanceFleetAuditOptions;
}

function generationReadCounts(
  store: RegisteredGenerationInventoryRunStore,
): Readonly<{
  latest: number;
  finalized: number;
  runByOperation: number;
}> {
  return {
    latest: store.latestFinalizedGenerationCalls,
    finalized: store.readFinalizedGenerationCalls,
    runByOperation: store.readRunByOperationCalls,
  };
}

function expectZeroHarnessWork(
  harness: Harness,
  coordination: Readonly<{
    leaseCount: number;
    readOperationByIdCalls: number;
    generationReads: ReturnType<typeof generationReadCounts>;
  }> = {
    leaseCount: 0,
    readOperationByIdCalls: 0,
    generationReads: { latest: 0, finalized: 0, runByOperation: 0 },
  },
): void {
  expect(harness.operationStore.leaseCount).toBe(coordination.leaseCount);
  expect(harness.operationStore.readOperationByIdCalls).toBe(
    coordination.readOperationByIdCalls,
  );
  expect(harness.operationStore.operations.size).toBe(0);
  expect(harness.operationStore.rows.size).toBe(0);
  expect(harness.operationStore.heads.size).toBe(0);
  expect(harness.operationStore.intakeDigests.size).toBe(0);
  expect(harness.operationStore.stagedRowCodecCalls).toBe(0);
  expect(harness.opsLog).toEqual([]);
  expect(harness.fleetStore.ops).toEqual([]);
  expect(generationReadCounts(harness.inventoryStore)).toEqual(
    coordination.generationReads,
  );
  expect(harness.inventoryStore.pins).toEqual([]);
  expect(harness.inventoryStore.releasedPins).toEqual([]);
}

function buildHarness(
  records: readonly FleetRecord[],
  inventory: FleetResourceInventory,
  overrides: Partial<{
    throwOnInspect: Set<string>;
    throwOnEnsureMaintenance: Set<string>;
    throwBackendFor: Set<string>;
    throwSpecFor: Set<string>;
    throwSecretFor: Set<string>;
    maxItemsPerCall: number;
    auditClock: () => number;
    authorityClock: () => number;
    signal: AbortSignal;
  }> = {},
): Harness {
  const operationStore = new FakeOperationStore();
  const inventoryStore = new RegisteredGenerationInventoryRunStore();
  inventoryStore.registerFinalizedGeneration(1, inventory);
  const fleetStore = new FakeFleetStateStore(records);
  const opsLog: string[] = [];
  const liveByTenant = new Map<string, LiveDeployment | undefined>();
  for (const record of records) {
    liveByTenant.set(record.tenantTag, cleanLiveDeployment(record));
  }
  const specByTenant = new Map<string, DeploymentSpec>(
    records.map((record) => [record.tenantTag, specForRecord(record)]),
  );
  const secretByTenant = new Map<string, string>(
    records.map((record) => [
      record.tenantTag,
      `maintenance-secret-${record.tenantTag}`,
    ]),
  );
  const backend = new SimpleBackend(
    liveByTenant,
    opsLog,
    overrides.throwOnInspect,
    overrides.throwOnEnsureMaintenance,
  );
  return {
    records,
    inventory,
    operationStore,
    inventoryStore,
    fleetStore,
    backend,
    opsLog,
    liveByTenant,
    specByTenant,
    secretByTenant,
    baseOptions(action: FleetAuditAdvanceAction): AdvanceFleetAuditOptions {
      return {
        operationStore,
        inventoryStore,
        fleetStore,
        action,
        ...(overrides.maxItemsPerCall === undefined
          ? {}
          : { maxItemsPerCall: overrides.maxItemsPerCall }),
        ...(overrides.auditClock === undefined
          ? {}
          : { auditClock: overrides.auditClock }),
        ...(overrides.authorityClock === undefined
          ? {}
          : { authorityClock: overrides.authorityClock }),
        ...(overrides.signal === undefined ? {} : { signal: overrides.signal }),
        backendFor: (record) => {
          opsLog.push('resolver:backendFor');
          if (overrides.throwBackendFor?.has(record.tenantTag)) {
            throw new Error('backend resolver blew up');
          }
          return backend;
        },
        specFor: (record) => {
          opsLog.push('resolver:specFor');
          if (overrides.throwSpecFor?.has(record.tenantTag)) {
            throw new Error('spec resolver blew up');
          }
          const spec = specByTenant.get(record.tenantTag);
          if (!spec)
            throw new Error(`no spec fixture for '${record.tenantTag}'`);
          return spec;
        },
        maintenanceSecretFor: (record) => {
          opsLog.push('resolver:maintenanceSecretFor');
          if (overrides.throwSecretFor?.has(record.tenantTag)) {
            throw new Error('secret resolver blew up');
          }
          return secretByTenant.get(record.tenantTag) as string;
        },
      };
    },
  };
}

/**
 * Drives `advanceFleetAudit` with `continue` until the status is not
 * 'pending'. Always advances once before it inspects a status, so it never
 * returns the result its caller already holds.
 *
 * `cap` is slack rather than derived. This loop runs to a terminal result, so
 * the whole-operation aggregate the fleet control guide states (see its
 * per-call cost paragraph, docs/fleet-control.md) bounds it in full: that
 * aggregate turns on `maxItemsPerCall` and the per-stage source sizes rather
 * than on the stage count alone, so a call site that lowers `maxItemsPerCall`
 * against a large source has to pass its own cap.
 */
async function driveToTerminal(
  harness: Harness,
  firstToken: unknown,
  cap = 200,
): Promise<FleetAuditAdvanceResult> {
  let token = firstToken;
  for (let i = 0; i < cap; i++) {
    const result = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token }),
    );
    if (result.status !== 'pending') return result;
    token = result.token;
  }
  throw new Error(
    `driveToTerminal exceeded its ${cap}-iteration cap before a terminal result`,
  );
}

type PendingFleetAuditAdvance = Extract<
  FleetAuditAdvanceResult,
  { status: 'pending' }
>;

/**
 * Drives `advanceFleetAudit` with `continue` until `stage.step` is `step`.
 * Always advances once before it compares the step, so a token already parked
 * on `step` still costs one call.
 *
 * `cap` is slack rather than derived. This loop stops at `step`, so a prefix
 * of the whole-operation aggregate the fleet control guide states (see its
 * per-call cost paragraph, docs/fleet-control.md) bounds it: that aggregate
 * turns on `maxItemsPerCall` and the per-stage source sizes rather than on
 * the stage count alone, so a call site that lowers `maxItemsPerCall` against
 * a large source has to pass its own cap.
 */
async function driveToStage(
  harness: Harness,
  firstToken: unknown,
  step: FleetAuditStage['step'],
  cap = 200,
): Promise<PendingFleetAuditAdvance> {
  let token = firstToken;
  for (let i = 0; i < cap; i++) {
    const result = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token }),
    );
    if (result.status !== 'pending') {
      throw new Error(
        `driveToStage reached ${result.status} before the ${step} stage`,
      );
    }
    if (result.stage.step === step) return result;
    token = result.token;
  }
  throw new Error(
    `driveToStage exceeded its ${cap}-iteration cap before the ${step} stage`,
  );
}

async function startAndDrive(
  harness: Harness,
  operationId: string,
  records: readonly FleetRecord[],
  staleAfterMs = STALE_AFTER_MS,
): Promise<FleetAuditAdvanceResult> {
  const started = await advanceFleetAudit(
    harness.baseOptions({ kind: 'start', operationId, records, staleAfterMs }),
  );
  if (started.status !== 'pending') return started;
  return driveToTerminal(harness, started.token);
}

function expectPendingToken(
  result: FleetAuditAdvanceResult,
): PendingFleetAuditAdvance['token'] {
  expect(result.status).toBe('pending');
  if (result.status !== 'pending') {
    throw new Error(`expected a pending result, got '${result.status}'`);
  }
  return result.token;
}

// The bounded run can re-arm maintenance in its Fleet store.
function drainWith(harness: Harness): Promise<readonly DriftFinding[]> {
  return auditFleetDrift({
    store: new FakeFleetStateStore(harness.records),
    records: harness.records,
    inventory: harness.inventory,
    backendFor: () => harness.backend,
    specFor: (record) =>
      harness.specByTenant.get(record.tenantTag) as DeploymentSpec,
    maintenanceSecretFor: (record) =>
      harness.secretByTenant.get(record.tenantTag) as string,
    staleAfterMs: STALE_AFTER_MS,
    now: AUDIT_NOW,
  });
}

interface PreflightRefusalCase {
  readonly name: string;
  /** The fleet the harness starts from; the inventory is derived from it. */
  readonly fleet: readonly FleetRecord[];
  readonly operationId: string;
  readonly intake: () => readonly FleetRecord[];
  readonly generation?: number;
  readonly auditClock?: () => number;
  readonly message: string;
  /** Omitted where the refusal precedes the lease entirely. */
  readonly coordination?: Parameters<typeof expectZeroHarnessWork>[1];
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('advanceFleetAudit', () => {
  it('start creates the operation THEN pins; freezes auditTimeMs/staleAfterMs', async () => {
    const alice = baseRecord('alice');
    let auditClockCalls = 0;
    const harness = buildHarness([alice], inventoryFor([alice]), {
      auditClock: () => {
        auditClockCalls += 1;
        return AUDIT_NOW;
      },
    });
    const events: string[] = [];
    // Instrument: record when the row exists (created) vs when the pin lands.
    const originalPinGeneration = harness.inventoryStore.pinGeneration.bind(
      harness.inventoryStore,
    );
    harness.inventoryStore.pinGeneration = async (input) => {
      events.push(
        harness.operationStore.operations.has(uuidFor(1))
          ? 'operation-row-present-at-pin'
          : 'operation-row-absent-at-pin',
      );
      return originalPinGeneration(input);
    };
    const operationId = uuidFor(1);
    const result = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(result.status).toBe('pending');
    expect(auditClockCalls).toBe(1);
    expect(events).toEqual(['operation-row-present-at-pin']);
    const persisted =
      await harness.operationStore.readOperationById(operationId);
    expect(persisted).toBeDefined();
    const progress = fleetAuditProgressFromUnknown(persisted?.progress);
    expect(progress.auditTimeMs).toBe(AUDIT_NOW);
    expect(progress.staleAfterMs).toBe(STALE_AFTER_MS);
    if (result.status !== 'pending') throw new Error('unreachable');
    const terminal = await driveToTerminal(harness, result.token);
    expect(terminal.status).toBe('complete');
    expect(auditClockCalls).toBe(1);
  });

  it('a record aging past staleAfterMs mid-operation still audits fresh', async () => {
    const alice = baseRecord('alice', {
      updatedAt: new Date(AUDIT_NOW - 30 * 60_000).toISOString(),
      phase: 'worker-deployed',
    });
    let clock = AUDIT_NOW;
    const harness = buildHarness([alice], inventoryFor([alice]), {
      auditClock: () => clock,
      authorityClock: () => clock,
    });
    const operationId = uuidFor(2);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    // Real time moves far past staleAfterMs before the per-record stage runs.
    clock = AUDIT_NOW + 10 * STALE_AFTER_MS;
    const result = await driveToTerminal(harness, expectPendingToken(started));
    expect(result.status).toBe('complete');
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 100,
    });
    expect(
      page.findings.some(
        (finding) => finding.kind === 'incomplete-provisioning',
      ),
    ).toBe(false);
  });

  it('start replay converges, re-pins, and stages no duplicate rows — and NEVER calls latestFinalizedGeneration', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const operationId = uuidFor(3);
    const action: FleetAuditAdvanceAction = {
      kind: 'start',
      operationId,
      records: [alice],
      staleAfterMs: STALE_AFTER_MS,
    };
    const first = await advanceFleetAudit(harness.baseOptions(action));
    expect(first.status).toBe('pending');
    expect(harness.inventoryStore.latestFinalizedGenerationCalls).toBe(1);
    harness.inventoryStore.latestFinalizedGenerationError = new Error(
      'latestFinalizedGeneration must not be called once the operation exists',
    );
    const rowCountBefore = harness.operationStore.rows.get(
      `${operationId}:record`,
    )?.length;
    const second = await advanceFleetAudit(harness.baseOptions(action));
    expect(second.status).toBe('pending');
    if (first.status !== 'pending' || second.status !== 'pending') {
      throw new Error('unreachable');
    }
    expect(second.token).toEqual(first.token);
    expect(second.stage).toEqual(first.stage);
    expect(harness.inventoryStore.latestFinalizedGenerationCalls).toBe(1);
    expect(
      harness.operationStore.rows.get(`${operationId}:record`)?.length,
    ).toBe(rowCountBefore);
    expect(
      harness.inventoryStore.pins.filter(
        (pin) => pin.pinnedBy === `fleet-audit:${operationId}`,
      ).length,
    ).toBe(2);

    const continuedOnce = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token: second.token }),
    );
    expect(continuedOnce.status).toBe('pending');
    if (continuedOnce.status !== 'pending') throw new Error('unreachable');
    const continuedTwice = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token: continuedOnce.token }),
    );
    expect(continuedTwice.status).toBe('pending');
    if (continuedTwice.status !== 'pending') throw new Error('unreachable');
    expect(continuedTwice.token).not.toEqual(first.token);
    const rowCountsBeforeAdvancedReplay = new Map(
      [...harness.operationStore.rows].map(([key, rows]) => [key, rows.length]),
    );
    const latestCallsBeforeAdvancedReplay =
      harness.inventoryStore.latestFinalizedGenerationCalls;
    const replayPastRevisionOne = await advanceFleetAudit(
      harness.baseOptions(action),
    );
    expect(replayPastRevisionOne.status).toBe('pending');
    if (replayPastRevisionOne.status !== 'pending')
      throw new Error('unreachable');
    expect(replayPastRevisionOne.token).toEqual(continuedTwice.token);
    expect(replayPastRevisionOne.stage).toEqual(continuedTwice.stage);
    expect(
      new Map(
        [...harness.operationStore.rows].map(([key, rows]) => [
          key,
          rows.length,
        ]),
      ),
    ).toEqual(rowCountsBeforeAdvancedReplay);
    expect(harness.inventoryStore.latestFinalizedGenerationCalls).toBe(
      latestCallsBeforeAdvancedReplay,
    );

    // Replay against a TERMINAL operation.
    const finalResult = await driveToTerminal(
      harness,
      replayPastRevisionOne.token,
    );
    expect(finalResult.status).toBe('complete');
    const pinsBeforeTerminalReplay = harness.inventoryStore.pins.length;
    const rowCountsBeforeTerminalReplay = new Map(
      [...harness.operationStore.rows].map(([key, rows]) => [key, rows.length]),
    );
    const replayAfterTerminal = await advanceFleetAudit(
      harness.baseOptions(action),
    );
    expect(replayAfterTerminal).toStrictEqual(finalResult);
    expect(harness.inventoryStore.pins).toHaveLength(pinsBeforeTerminalReplay);
    expect(
      new Map(
        [...harness.operationStore.rows].map(([key, rows]) => [
          key,
          rows.length,
        ]),
      ),
    ).toEqual(rowCountsBeforeTerminalReplay);
    expect(harness.inventoryStore.latestFinalizedGenerationCalls).toBe(1);

    const crossKindOperationId = uuidFor(52);
    harness.operationStore.operations.set(crossKindOperationId, {
      version: 1,
      operationId: crossKindOperationId,
      kind: 'migration',
      state: 'running',
      progress: {
        kind: 'migration',
        revision: 0,
      } as unknown as FleetOperationRunRecord['progress'],
      updatedAt: new Date(AUDIT_NOW).toISOString(),
    });
    harness.operationStore.heads.set('migration', crossKindOperationId);
    const latestCallsBeforeCrossKind =
      harness.inventoryStore.latestFinalizedGenerationCalls;
    await expect(
      advanceFleetAudit(
        harness.baseOptions({
          kind: 'start',
          operationId: crossKindOperationId,
          records: [alice],
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toThrow(
      `fleet operation '${crossKindOperationId}' belongs to the other operation kind`,
    );
    expect(harness.inventoryStore.latestFinalizedGenerationCalls).toBe(
      latestCallsBeforeCrossKind,
    );
    expect(
      harness.inventoryStore.pins.some(
        (pin) => pin.pinnedBy === `fleet-audit:${crossKindOperationId}`,
      ),
    ).toBe(false);
    expect(
      harness.operationStore.rows.get(`${crossKindOperationId}:record`) ?? [],
    ).toEqual([]);
  });

  it('an implicit-generation replay re-pins the PERSISTED generation even after generation N+1 finalizes; two different EXPLICIT-generation starts under one operationId conflict', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const operationId = uuidFor(4);
    const implicit: FleetAuditAdvanceAction = {
      kind: 'start',
      operationId,
      records: [alice],
      staleAfterMs: STALE_AFTER_MS,
    };
    const first = await advanceFleetAudit(harness.baseOptions(implicit));
    expect(first.status).toBe('pending');
    const persisted1 =
      await harness.operationStore.readOperationById(operationId);
    const generation1 = fleetAuditProgressFromUnknown(
      persisted1?.progress,
    ).generation;
    expect(generation1).toBe(1);

    // A newer generation finalizes.
    harness.inventoryStore.registerFinalizedGeneration(
      2,
      inventoryFor([alice]),
    );
    expect(harness.inventoryStore.latestGeneration).toBe(2);

    const pinsBeforeReplay = harness.inventoryStore.pins.length;
    const replay = await advanceFleetAudit(harness.baseOptions(implicit));
    expect(replay.status).toBe('pending');
    const persisted2 =
      await harness.operationStore.readOperationById(operationId);
    expect(fleetAuditProgressFromUnknown(persisted2?.progress).generation).toBe(
      1,
    );
    expect(harness.inventoryStore.pins.slice(pinsBeforeReplay)).toEqual([
      { generation: 1, pinnedBy: `fleet-audit:${operationId}` },
    ]);

    // Two DIFFERENT explicit-generation starts under one operationId conflict.
    // A fresh harness keeps this independent of the still-running operation above.
    const otherHarness = buildHarness([alice], inventoryFor([alice]));
    const otherId = uuidFor(5);
    const explicit1: FleetAuditAdvanceAction = {
      kind: 'start',
      operationId: otherId,
      records: [alice],
      staleAfterMs: STALE_AFTER_MS,
      generation: 1,
    };
    const explicit2: FleetAuditAdvanceAction = {
      ...explicit1,
      generation: 2,
    };
    await advanceFleetAudit(otherHarness.baseOptions(explicit1));
    await expect(
      advanceFleetAudit(otherHarness.baseOptions(explicit2)),
    ).rejects.toThrow(
      `fleet operation '${otherId}' already exists with a different intake`,
    );
  });

  it('start intake-digest mismatch conflict', async () => {
    const alice = baseRecord('alice');
    const bob = baseRecord('bob');
    const harness = buildHarness([alice, bob], inventoryFor([alice, bob]));
    const operationId = uuidFor(6);
    await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    await expect(
      advanceFleetAudit(
        harness.baseOptions({
          kind: 'start',
          operationId,
          records: [alice, bob],
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toThrow(
      `fleet operation '${operationId}' already exists with a different intake`,
    );
  });

  it('start contention under a foreign active audit', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId: uuidFor(7),
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    await expect(
      advanceFleetAudit(
        harness.baseOptions({
          kind: 'start',
          operationId: uuidFor(8),
          records: [alice],
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toThrow(
      'another fleet audit operation is active for this account',
    );
  });

  it('stale token → authoritative pending/complete/failed with zero resolver/generation/provider work', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const operationId = uuidFor(9);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(started.status).toBe('pending');
    if (started.status !== 'pending') throw new Error('unreachable');
    const oldToken = started.token;
    // Advance once more so `oldToken` is now stale.
    const advanced = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token: oldToken }),
    );
    expect(advanced.status).toBe('pending');
    if (advanced.status !== 'pending') throw new Error('unreachable');

    const opsBefore = harness.opsLog.length;
    const readsBeforePending = generationReadCounts(harness.inventoryStore);
    const staleResult = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token: oldToken }),
    );
    expect(staleResult.status).toBe('pending');
    if (staleResult.status === 'pending') {
      expect(staleResult.token).toEqual(advanced.token);
    }
    expect(harness.opsLog.length).toBe(opsBefore);
    expect(generationReadCounts(harness.inventoryStore)).toEqual(
      readsBeforePending,
    );

    const completed = await driveToTerminal(harness, advanced.token);
    expect(completed.status).toBe('complete');
    const opsBeforeStaleComplete = harness.opsLog.length;
    const readsBeforeComplete = generationReadCounts(harness.inventoryStore);
    const staleAgainstComplete = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token: oldToken }),
    );
    expect(staleAgainstComplete).toStrictEqual(completed);
    expect(harness.opsLog.length).toBe(opsBeforeStaleComplete);
    expect(generationReadCounts(harness.inventoryStore)).toEqual(
      readsBeforeComplete,
    );

    // Drive to a failed operation and re-poll with a stale token against it.
    const failingHarness = buildHarness([alice], inventoryFor([alice]));
    const failOperationId = uuidFor(10);
    const failStart = await advanceFleetAudit(
      failingHarness.baseOptions({
        kind: 'start',
        operationId: failOperationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(failStart.status).toBe('pending');
    if (failStart.status !== 'pending') throw new Error('unreachable');
    const staleFailToken = failStart.token;
    failingHarness.inventoryStore.unreadableGenerations.add(1);
    const failed = await driveToTerminal(failingHarness, failStart.token);
    expect(failed.status).toBe('failed');
    const opsBeforeStaleFailed = failingHarness.opsLog.length;
    const readsBeforeFailed = generationReadCounts(
      failingHarness.inventoryStore,
    );
    const staleAgainstFailed = await advanceFleetAudit(
      failingHarness.baseOptions({ kind: 'continue', token: staleFailToken }),
    );
    expect(staleAgainstFailed.status).toBe('failed');
    expect(failingHarness.opsLog.length).toBe(opsBeforeStaleFailed);
    expect(generationReadCounts(failingHarness.inventoryStore)).toEqual(
      readsBeforeFailed,
    );
  });

  it('future token error', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const operationId = uuidFor(11);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(started.status).toBe('pending');
    if (started.status !== 'pending') throw new Error('unreachable');
    const futureToken = {
      ...started.token,
      revision: started.token.revision + 50,
    };
    await expect(
      advanceFleetAudit(
        harness.baseOptions({ kind: 'continue', token: futureToken }),
      ),
    ).rejects.toBeInstanceOf(FleetOperationTokenFutureError);
  });

  it("a migration operation's token → kind error before any resolver work", async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const migrationOperationId = uuidFor(12);
    harness.operationStore.operations.set(migrationOperationId, {
      version: 1,
      operationId: migrationOperationId,
      kind: 'migration',
      state: 'running',
      progress: {
        kind: 'migration',
        revision: 0,
      } as unknown as FleetOperationRunRecord['progress'],
      updatedAt: new Date(AUDIT_NOW).toISOString(),
    });
    harness.operationStore.heads.set('migration', migrationOperationId);
    const opsBefore = harness.opsLog.length;
    await expect(
      advanceFleetAudit(
        harness.baseOptions({
          kind: 'continue',
          token: { version: 1, operationId: migrationOperationId, revision: 0 },
        }),
      ),
    ).rejects.toBeInstanceOf(FleetOperationTokenKindError);
    expect(harness.opsLog.length).toBe(opsBefore);
  });

  it('absent-operation adjudication', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    await expect(
      advanceFleetAudit(
        harness.baseOptions({
          kind: 'continue',
          token: { version: 1, operationId: uuidFor(13), revision: 0 },
        }),
      ),
    ).rejects.toBeInstanceOf(FleetOperationTokenOperationError);
  });

  it("'operation-store' capability error with zero work", async () => {
    const alice = baseRecord('alice');
    for (const member of [
      'withAccountOperationLease',
      'readOperationById',
      'readOperationRowsPage',
    ] as const) {
      const harness = buildHarness([alice], inventoryFor([alice]));
      const options = harness.baseOptions({
        kind: 'start',
        operationId: uuidFor(14),
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      });
      const broken = {
        ...options,
        operationStore: withoutMethod(harness.operationStore, member),
      };
      await expect(advanceFleetAudit(broken)).rejects.toBeInstanceOf(
        FleetAuditAdvanceCapabilityError,
      );
      await expect(advanceFleetAudit(broken)).rejects.toThrow(
        'fleet audit advance requires an operation store',
      );
      expectZeroHarnessWork(harness);
    }
  });

  it("'generation-read' capability error with zero work", async () => {
    const alice = baseRecord('alice');
    for (const member of [
      'readFinalizedGeneration',
      'readRunByOperation',
    ] as const) {
      const harness = buildHarness([alice], inventoryFor([alice]));
      const options = harness.baseOptions({
        kind: 'start',
        operationId: uuidFor(15),
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      });
      const broken = {
        ...options,
        inventoryStore: withoutMethod(harness.inventoryStore, member),
      };
      await expect(advanceFleetAudit(broken)).rejects.toThrow(
        'fleet audit advance requires an inventory store that can read finalized generations',
      );
      expectZeroHarnessWork(harness);
    }
  });

  it("'generation-pin' capability error with zero work", async () => {
    const alice = baseRecord('alice');
    for (const member of ['pinGeneration', 'releasePin'] as const) {
      const harness = buildHarness([alice], inventoryFor([alice]));
      const options = harness.baseOptions({
        kind: 'start',
        operationId: uuidFor(16),
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      });
      const broken = {
        ...options,
        inventoryStore: withoutMethod(harness.inventoryStore, member),
      };
      await expect(advanceFleetAudit(broken)).rejects.toThrow(
        'fleet audit advance requires an inventory store that can pin finalized generations',
      );
      expectZeroHarnessWork(harness);
    }
  });

  it('the legacy staleAfterMs refusal message', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const attempt = advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId: uuidFor(17),
        records: [alice],
        staleAfterMs: 0,
      }),
    );
    await expect(attempt).rejects.toBeInstanceOf(Error);
    await expect(attempt).rejects.toHaveProperty(
      'message',
      'staleAfterMs must be a positive safe integer',
    );
  });

  it('item-bound refusal at 10,001', async () => {
    const many = Array.from({ length: 10_001 }, (_, i) =>
      baseRecord(`tenant${i}`),
    );
    const harness = buildHarness([], emptyInventory());
    const attempt = advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId: uuidFor(18),
        records: many,
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    await expect(attempt).rejects.toBeInstanceOf(Error);
    await expect(attempt).rejects.toHaveProperty(
      'message',
      `fleet audit start accepts at most ${FLEET_OPERATION_ITEM_BOUND} records`,
    );
  });

  it('intake byte-bound refusal at start', async () => {
    const padding = Object.fromEntries(
      Array.from({ length: 21 }, (_, i) => [`padding${i}`, 'x'.repeat(4_000)]),
    );
    const many = Array.from({ length: 200 }, (_, i) =>
      Object.assign(baseRecord(`tenant${i}`), { padding }),
    );
    // This fixture is ASCII, so string lengths equal UTF-8 byte lengths.
    const serializedLengths = many.map(
      (record) => JSON.stringify(record).length,
    );
    expect(
      serializedLengths.reduce((sum, length) => sum + length, 0),
    ).toBeGreaterThan(FLEET_OPERATION_INTAKE_BYTE_BOUND);
    expect(Math.max(...serializedLengths)).toBeLessThan(
      FLEET_OPERATION_RECORD_ROW_BYTE_BOUND,
    );
    const harness = buildHarness([], emptyInventory());
    const attempt = advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId: uuidFor(19),
        records: many,
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    await expect(attempt).rejects.toBeInstanceOf(Error);
    await expect(attempt).rejects.toHaveProperty(
      'message',
      'fleet audit start canonical intake exceeds the intake byte bound',
    );
  });

  it("'operationId' validation refusal at start", async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const attempt = advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId: 'not-a-uuid',
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    await expect(attempt).rejects.toBeInstanceOf(Error);
    await expect(attempt).rejects.toHaveProperty(
      'message',
      'operationId must be a lowercase UUIDv4',
    );
  });

  it('the per-record chunk performs exactly one inspect + at most one re-arm (instrumented)', async () => {
    const alice = baseRecord('alice');
    const bob = baseRecord('bob');
    const harness = buildHarness([alice, bob], inventoryFor([alice, bob]), {
      auditClock: () => AUDIT_NOW,
    });
    harness.liveByTenant.set(
      'alice',
      cleanLiveDeployment(alice, { maintenance: UNARMED_MAINTENANCE }),
    );
    const operationId = uuidFor(20);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice, bob],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(started.status).toBe('pending');
    if (started.status !== 'pending') throw new Error('unreachable');
    const atPerRecord = await driveToStage(
      harness,
      started.token,
      'per-record',
    );
    let token = atPerRecord.token;
    for (const [tenant, expectedRearmCalls] of [
      ['alice', 1],
      ['bob', 0],
    ] as const) {
      const opsBefore = harness.opsLog.length;
      const result = await advanceFleetAudit(
        harness.baseOptions({ kind: 'continue', token }),
      );
      if (result.status !== 'pending') throw new Error('unexpected terminal');
      token = result.token;
      const callOps = harness.opsLog.slice(opsBefore);
      expect(callOps.filter((op) => op === 'resolver:backendFor')).toHaveLength(
        1,
      );
      expect(callOps.filter((op) => op === 'resolver:specFor')).toHaveLength(1);
      expect(
        callOps.filter((op) => op === 'resolver:maintenanceSecretFor'),
      ).toHaveLength(1);
      expect(callOps.filter((op) => op.startsWith('inspect:'))).toEqual([
        `inspect:${tenant}`,
      ]);
      expect(callOps.filter((op) => op === 'ensureMaintenance')).toHaveLength(
        expectedRearmCalls,
      );
    }
  });

  it('two-clock proof: staleness uses frozen auditTimeMs; a first-time authorizedAt uses the call-time authorityClock', async () => {
    const stale = baseRecord('stalemaint', {
      updatedAt: FRESH_UPDATED_AT,
      phase: 'worker-deployed',
    });
    const authority = baseRecord('authorityclock');
    const inventory = inventoryFor([stale, authority]);
    const laterClock = AUDIT_NOW + 5 * STALE_AFTER_MS;
    let clock = AUDIT_NOW;
    let authorityClockCalls = 0;
    const harness = buildHarness([stale, authority], inventory, {
      auditClock: () => AUDIT_NOW,
      authorityClock: () => {
        authorityClockCalls += 1;
        return clock;
      },
    });
    harness.liveByTenant.set(
      'authorityclock',
      cleanLiveDeployment(authority, { maintenance: UNARMED_MAINTENANCE }),
    );
    const operationId = uuidFor(21);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [stale, authority],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(started.status).toBe('pending');
    if (started.status !== 'pending') throw new Error('unreachable');
    expect(authorityClockCalls).toBe(0);
    clock = laterClock;
    const atPerRecord = await driveToStage(
      harness,
      started.token,
      'per-record',
    );
    // `authorityClockCalls` only ever increments, so a zero once the global
    // stages are behind us covers every call `driveToStage` just made, not
    // only the last of them.
    expect(authorityClockCalls).toBe(0);
    const staleRecord = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token: atPerRecord.token }),
    );
    expect(authorityClockCalls).toBe(0);
    if (staleRecord.status !== 'pending')
      throw new Error('unexpected terminal');
    const authorityRecord = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token: staleRecord.token }),
    );
    expect(authorityClockCalls).toBe(1);
    if (authorityRecord.status !== 'pending')
      throw new Error('unexpected terminal');
    await driveToTerminal(harness, authorityRecord.token);
    expect(authorityClockCalls).toBe(1);
    const persisted =
      await harness.operationStore.readOperationById(operationId);
    expect(fleetAuditProgressFromUnknown(persisted?.progress).auditTimeMs).toBe(
      AUDIT_NOW,
    );
    const putRecord = harness.fleetStore.records.get(
      'authorityclock:production',
    );
    expect(putRecord?.invocationAuthority?.authorizedAt).toBe(
      new Date(laterClock).toISOString(),
    );
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 100,
    });
    expect(
      page.findings.some(
        (finding) => finding.kind === 'incomplete-provisioning',
      ),
    ).toBe(false);

    // Default Date.now proven by omission.
    const defaultHarness = buildHarness([authority], inventoryFor([authority]));
    defaultHarness.liveByTenant.set(
      'authorityclock',
      cleanLiveDeployment(authority, { maintenance: UNARMED_MAINTENANCE }),
    );
    const before = Date.now();
    await startAndDrive(defaultHarness, uuidFor(22), [authority]);
    const after = Date.now();
    const authorizedAt = Date.parse(
      defaultHarness.fleetStore.records.get('authorityclock:production')
        ?.invocationAuthority?.authorizedAt ?? '',
    );
    expect(authorizedAt).toBeGreaterThanOrEqual(before);
    expect(authorizedAt).toBeLessThanOrEqual(after);
  });

  it('a database owner inspected in call N yields duplicate-database in call N+k', async () => {
    const shared = 'db-shared-owner';
    const first = baseRecord('dbowner1', { databaseId: shared });
    const second = baseRecord('dbowner2', { databaseId: shared });
    const inventory = inventoryFor([first, second]);
    const harness = buildHarness([first, second], inventory);
    harness.liveByTenant.set(
      'dbowner1',
      cleanLiveDeployment(first, { databaseId: shared }),
    );
    harness.liveByTenant.set(
      'dbowner2',
      cleanLiveDeployment(second, { databaseId: shared }),
    );
    const operationId = uuidFor(23);
    await startAndDrive(harness, operationId, [first, second]);
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 100,
    });
    expect(
      page.findings.some((finding) => finding.kind === 'duplicate-database'),
    ).toBe(true);
  });

  it('namespace owner facts + duplicate suppression across calls', async () => {
    const shared = 'ns-shared-live';
    const first = baseRecord('nsowner1', {
      durableObjectBindings: [
        { name: 'R', className: 'Runner', namespaceId: 'ns-nsowner1' },
      ],
    });
    const second = baseRecord('nsowner2', {
      durableObjectBindings: [
        { name: 'R', className: 'Runner', namespaceId: 'ns-nsowner2' },
      ],
    });
    const inventory = inventoryFor([first, second]);
    inventory.namespaceIds.push(shared);
    const harness = buildHarness([first, second], inventory);
    harness.liveByTenant.set(
      'nsowner1',
      cleanLiveDeployment(first, {
        durableObjectBindings: [
          { name: 'R', className: 'Runner', namespaceId: shared },
        ],
      }),
    );
    harness.liveByTenant.set(
      'nsowner2',
      cleanLiveDeployment(second, {
        durableObjectBindings: [
          { name: 'R', className: 'Runner', namespaceId: shared },
        ],
      }),
    );
    const operationId = uuidFor(24);
    await startAndDrive(harness, operationId, [first, second]);
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 100,
    });
    const duplicates = page.findings.filter(
      (finding) => finding.kind === 'duplicate-namespace',
    );
    expect(duplicates.length).toBe(1);
  });

  it('the records-derived expected-duplicate seed suppresses a later live duplicate across calls', async () => {
    const shared = 'ns-expected-and-live-shared';
    const first = baseRecord('seedowner1', {
      durableObjectBindings: [
        { name: 'R', className: 'Runner', namespaceId: shared },
      ],
    });
    const second = baseRecord('seedowner2', {
      durableObjectBindings: [
        { name: 'R', className: 'Runner', namespaceId: shared },
      ],
    });
    const inventory = inventoryFor([first, second]);
    const harness = buildHarness([first, second], inventory);
    harness.liveByTenant.set(
      'seedowner1',
      cleanLiveDeployment(first, {
        durableObjectBindings: [
          { name: 'R', className: 'Runner', namespaceId: shared },
        ],
      }),
    );
    harness.liveByTenant.set(
      'seedowner2',
      cleanLiveDeployment(second, {
        durableObjectBindings: [
          { name: 'R', className: 'Runner', namespaceId: shared },
        ],
      }),
    );
    const operationId = uuidFor(25);
    await startAndDrive(harness, operationId, [first, second]);
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 100,
    });
    // The expected-side (namespace-expectations) already contributes exactly
    // one duplicate-namespace finding for `shared`; the live per-record side
    // must not contribute a second one for the very same id.
    const duplicates = page.findings.filter(
      (finding) =>
        finding.kind === 'duplicate-namespace' &&
        finding.detail.includes(shared),
    );
    expect(duplicates.length).toBe(1);
  });

  it('the first-owner prefix rule: a shared namespace/bucket claimant does not self-collide across chunks', async () => {
    const shared = 'ns-prefix-shared';
    const sharedBucket = 'bucket-prefix-shared';
    const sharedBucketResource = {
      name: 'DATA',
      bucketName: sharedBucket,
      jurisdiction: 'default' as const,
      state: 'created' as const,
      reservationNonce: 'a'.repeat(32),
      creationDate: '2026-06-01T00:00:00.000Z',
    };
    const first = baseRecord('prefixa', {
      durableObjectBindings: [
        { name: 'R', className: 'Runner', namespaceId: shared },
      ],
      applicationResources: [sharedBucketResource],
    });
    const second = baseRecord('prefixb', {
      durableObjectBindings: [
        { name: 'R', className: 'Runner', namespaceId: shared },
      ],
      applicationResources: [sharedBucketResource],
    });
    const inventory = inventoryFor([first, second]);
    inventory.r2Buckets.push({
      bucketName: sharedBucket,
      jurisdiction: 'default',
      creationDate: sharedBucketResource.creationDate,
    });
    const harness = buildHarness([first, second], inventory, {
      maxItemsPerCall: 1,
      auditClock: () => AUDIT_NOW,
      authorityClock: () => AUDIT_NOW,
    });
    const drainFindings = await drainWith(harness);
    const operationId = uuidFor(26);
    await startAndDrive(harness, operationId, [first, second]);
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 100,
    });
    const duplicates = page.findings.filter(
      (finding) => finding.kind === 'duplicate-namespace',
    );
    expect(duplicates.length).toBe(1);
    expect(duplicates[0]?.tenantTag).toBe('prefixb');
    const bucketDuplicates = page.findings.filter(
      (finding) =>
        finding.kind === 'r2-bucket-drift' &&
        finding.detail ===
          `R2 bucket '${sharedBucket}' is claimed by more than one deployment`,
    );
    const drainBucketDuplicates = drainFindings.filter(
      (finding) =>
        finding.kind === 'r2-bucket-drift' &&
        finding.detail ===
          `R2 bucket '${sharedBucket}' is claimed by more than one deployment`,
    );
    expect(bucketDuplicates).toEqual(drainBucketDuplicates);
    expect(bucketDuplicates).toHaveLength(1);
    expect(bucketDuplicates[0]?.tenantTag).toBe('prefixb');
  });

  it('a pruned/unpinned generation → durable generation-unavailable failure with the pin released', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const operationId = uuidFor(27);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    harness.inventoryStore.unreadableGenerations.add(1);
    const result = await driveToTerminal(harness, expectPendingToken(started));
    expect(result.status).toBe('failed');
    if (result.status === 'failed') {
      expect(result.failure.reason).toBe('generation-unavailable');
    }
    expect(
      harness.inventoryStore.releasedPins.some(
        (pin) =>
          pin.pinnedBy === `fleet-audit:${operationId}` && pin.generation === 1,
      ),
    ).toBe(true);
  });

  it('per-record emission-bound overflow → durable emission-bound-exceeded failure, pin released, head freed', async () => {
    const SETUP_COUNT = 100;
    const setupRecords = Array.from({ length: SETUP_COUNT }, (_, i) =>
      baseRecord(`overflowsetup${i}`, {
        durableObjectBindings: [
          { name: 'R', className: 'Runner', namespaceId: `ns-overflow-${i}` },
        ],
      }),
    );
    // The collision record's own EXPECTED namespace is unique, so the
    // global namespace-expectations stage stays clean; the collision is
    // engineered to appear only in its LIVE inspection result, which is what
    // the per-record stage's single guarded batch must absorb.
    const collisionRecord = baseRecord('overflowcollision', {
      durableObjectBindings: [
        {
          name: 'OWN',
          className: 'Runner',
          namespaceId: 'ns-overflowcollision-own',
        },
      ],
    });
    const allRecords = [...setupRecords, collisionRecord];
    const inventory = inventoryFor(allRecords);
    const harness = buildHarness(allRecords, inventory);
    for (const record of setupRecords) {
      harness.liveByTenant.set(record.tenantTag, cleanLiveDeployment(record));
    }
    harness.liveByTenant.set(
      'overflowcollision',
      cleanLiveDeployment(collisionRecord, {
        durableObjectBindings: setupRecords.map((_, i) => ({
          name: `R${i}`,
          className: 'Runner',
          namespaceId: `ns-overflow-${i}`,
        })),
      }),
    );
    const operationId = uuidFor(28);
    const result = await startAndDrive(harness, operationId, allRecords);
    expect(result.status).toBe('failed');
    if (result.status === 'failed') {
      expect(result.failure.reason).toBe('emission-bound-exceeded');
      expect(result.failure.itemOrdinal).toBe(SETUP_COUNT);
    }
    expect(
      harness.inventoryStore.releasedPins.some(
        (pin) => pin.pinnedBy === `fleet-audit:${operationId}`,
      ),
    ).toBe(true);
    expect(harness.operationStore.heads.has('audit')).toBe(false);
  });

  it('abandonFleetAuditOperation: running → operator-abandoned + pin released; terminal → releases any surviving pin with no state change', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const operationId = uuidFor(29);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(started.status).toBe('pending');
    await abandonFleetAuditOperation({
      operationStore: harness.operationStore,
      inventoryStore: harness.inventoryStore,
      operationId,
    });
    const abandoned =
      await harness.operationStore.readOperationById(operationId);
    expect(abandoned?.state).toBe('failed');
    expect(
      fleetAuditProgressFromUnknown(abandoned?.progress).failure?.reason,
    ).toBe('operator-abandoned');
    expect(
      harness.inventoryStore.releasedPins.some(
        (pin) => pin.pinnedBy === `fleet-audit:${operationId}`,
      ),
    ).toBe(true);

    // Terminal → releases any surviving pin, no state change.
    const beforeTerminalAbandonment =
      await harness.operationStore.readOperationById(operationId);
    const releasedBefore = harness.inventoryStore.releasedPins.length;
    await abandonFleetAuditOperation({
      operationStore: harness.operationStore,
      inventoryStore: harness.inventoryStore,
      operationId,
    });
    const stillAbandoned =
      await harness.operationStore.readOperationById(operationId);
    expect(stillAbandoned).toStrictEqual(beforeTerminalAbandonment);
    expect(harness.inventoryStore.releasedPins.length).toBe(releasedBefore + 1);
  });

  it('a start whose store persisted another generation pins the PERSISTED one, so abandonment after a failing revision-1 commit releases the pin that was taken', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    // Generation 2 becomes "latest", so the start resolves 2 while the store
    // below persists 1. Both are finalized, so both are pinnable.
    harness.inventoryStore.registerFinalizedGeneration(
      2,
      inventoryFor([alice]),
    );
    const operationId = uuidFor(61);
    const owner = `fleet-audit:${operationId}`;
    const commitFault = new Error('fleet operation store commit failed');
    const operationStore = divergentCreateStore(harness.operationStore, 1, {
      member: 'commitProgress',
      error: commitFault,
    });
    const action: FleetAuditAdvanceAction = {
      kind: 'start',
      operationId,
      records: [alice],
      staleAfterMs: STALE_AFTER_MS,
    };

    const started = await advanceFleetAudit({
      ...harness.baseOptions(action),
      operationStore,
    });
    // The revision-1 commit threw, so the start answers from durable state.
    expect(started.status).toBe('pending');
    const persistedProgress = fleetAuditProgressFromUnknown(
      (await harness.operationStore.readOperationById(operationId))?.progress,
    );
    expect(persistedProgress.revision).toBe(0);
    expect(persistedProgress.generation).toBe(1);
    expect(harness.inventoryStore.pins).toEqual([
      { generation: 1, pinnedBy: owner },
    ]);

    await abandonFleetAuditOperation({
      operationStore,
      inventoryStore: harness.inventoryStore,
      operationId,
    });
    expect(harness.inventoryStore.releasedPins).toEqual([
      { generation: 1, pinnedBy: owner },
    ]);
    expect(harness.inventoryStore.releasedPins).toEqual(
      harness.inventoryStore.pins,
    );
  });

  it('a start whose store persisted another generation pins the PERSISTED one, so abandonment after a throwing stageRows releases the pin that was taken', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    harness.inventoryStore.registerFinalizedGeneration(
      2,
      inventoryFor([alice]),
    );
    const operationId = uuidFor(62);
    const owner = `fleet-audit:${operationId}`;
    const stageFault = new Error('fleet operation store staging failed');
    const operationStore = divergentCreateStore(harness.operationStore, 1, {
      member: 'stageRows',
      error: stageFault,
    });
    const action: FleetAuditAdvanceAction = {
      kind: 'start',
      operationId,
      records: [alice],
      staleAfterMs: STALE_AFTER_MS,
    };

    // Staging sits outside the start's catch, so the throw leaves the lease
    // callback and the pin is already taken.
    await expect(
      advanceFleetAudit({ ...harness.baseOptions(action), operationStore }),
    ).rejects.toBe(stageFault);
    const persistedProgress = fleetAuditProgressFromUnknown(
      (await harness.operationStore.readOperationById(operationId))?.progress,
    );
    expect(persistedProgress.revision).toBe(0);
    expect(persistedProgress.generation).toBe(1);
    expect(harness.inventoryStore.pins).toEqual([
      { generation: 1, pinnedBy: owner },
    ]);

    await abandonFleetAuditOperation({
      operationStore,
      inventoryStore: harness.inventoryStore,
      operationId,
    });
    expect(harness.inventoryStore.releasedPins).toEqual(
      harness.inventoryStore.pins,
    );
  });

  it('continue on a failed operation returns the failed member with zero provider work', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const operationId = uuidFor(30);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    harness.inventoryStore.unreadableGenerations.add(1);
    const failed = await driveToTerminal(harness, expectPendingToken(started));
    expect(failed.status).toBe('failed');
    if (failed.status !== 'failed') throw new Error('unreachable');
    const opsBefore = harness.opsLog.length;
    const again = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token: failed.token }),
    );
    expect(again.status).toBe('failed');
    expect(harness.opsLog.length).toBe(opsBefore);
  });

  it('findings page: running refusal; failed operation readable; no inventory-store interaction; a reverse-ordinal page still yields the drain order, and the next-cursor idiom pages the whole set', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const operationId = uuidFor(31);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    await expect(
      readFleetAuditFindingsPage(harness.operationStore, {
        operationId,
        limit: 10,
      }),
    ).rejects.toThrow(`fleet audit operation '${operationId}' is not terminal`);

    harness.inventoryStore.unreadableGenerations.add(1);
    const failed = await driveToTerminal(harness, expectPendingToken(started));
    expect(failed.status).toBe('failed');
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 10,
    });
    expect(page.done).toBe(true);

    const control = baseRecord('control28');
    const missingA = baseRecord('missing28a');
    const missingB = baseRecord('missing28b');
    const orderedRecords = [control, missingA, missingB];
    const orderedInventory = inventoryFor([control]);
    orderedInventory.deployments.push({
      backend: 'plain-worker',
      scriptName: 'ghost-orphan-script28',
      tenantTag: 'ghost-tenant28',
      environment: ENVIRONMENT,
      databaseIds: ['db-ghost28'],
      durableObjectBindings: [],
      secretNames: [],
      plainTextBindings: {},
      routeHostnames: [],
      artifactVersion: 'v1',
      schemaVersion: 1,
    });
    const orderedHarness = buildHarness(orderedRecords, orderedInventory, {
      auditClock: () => AUDIT_NOW + STALE_AFTER_MS + 60_001,
    });
    const orderedOperationId = uuidFor(3128);
    const complete = await startAndDrive(
      orderedHarness,
      orderedOperationId,
      orderedRecords,
    );
    expect(complete.status).toBe('complete');
    const ascending = await readFleetAuditFindingsPage(
      orderedHarness.operationStore,
      { operationId: orderedOperationId, limit: 1_000 },
    );
    expect(ascending.done).toBe(true);
    expect(ascending.findings.length).toBe(6);
    const reversedStore = pageTransformingStore(
      orderedHarness.operationStore,
      async (readPage) => {
        const rowsPage = await readPage();
        return { ...rowsPage, rows: [...rowsPage.rows].reverse() };
      },
    );
    await expect(
      readFleetAuditFindingsPage(reversedStore, {
        operationId: orderedOperationId,
        limit: 1_000,
      }),
    ).resolves.toEqual(ascending);
    const paged: (typeof ascending.findings)[number][] = [];
    let afterOrdinal: number | undefined;
    // A store can report done on a following empty page.
    const pageCap = ascending.findings.length + 1;
    let reachedDone = false;
    for (let pageIndex = 0; pageIndex < pageCap; pageIndex += 1) {
      const next = await readFleetAuditFindingsPage(reversedStore, {
        operationId: orderedOperationId,
        limit: 2,
        ...(afterOrdinal === undefined ? {} : { afterOrdinal }),
      });
      paged.push(...next.findings);
      if (next.done) {
        reachedDone = true;
        break;
      }
      afterOrdinal = next.nextAfterOrdinal;
    }
    if (!reachedDone) {
      throw new Error(
        `findings paging exceeded its ${pageCap}-page cap before the reader reported done`,
      );
    }
    expect(paged).toEqual(ascending.findings);
  });

  it('findings page: the reader verifies page conformance instead of trusting the store, refuses a final page accounting for fewer rows than findingCount, accepts the two other port-permitted shapes — a non-final page shorter than the limit, and an arbitrary permutation — and returns the next cursor off the page rows', async () => {
    const control = baseRecord('control29');
    const missingA = baseRecord('missing29a');
    const missingB = baseRecord('missing29b');
    const records = [control, missingA, missingB];
    const harness = buildHarness(records, inventoryFor([control]), {
      auditClock: () => AUDIT_NOW + STALE_AFTER_MS + 60_001,
    });
    const operationId = uuidFor(90);
    const complete = await startAndDrive(harness, operationId, records);
    expect(complete.status).toBe('complete');

    const conforming = await readFleetAuditFindingsPage(
      harness.operationStore,
      { operationId, limit: 1_000 },
    );
    expect(conforming.done).toBe(true);
    expect(conforming.findings.length).toBe(5);
    expect(conforming.nextAfterOrdinal).toBe(conforming.findings.length - 1);

    const shapedPage = (
      transform: (
        rows: readonly FleetOperationStagedRow[],
      ) => readonly FleetOperationStagedRow[],
      done?: boolean,
    ) =>
      pageTransformingStore(harness.operationStore, async (readPage) => {
        const rowsPage = await readPage();
        return {
          ...rowsPage,
          rows: transform(rowsPage.rows),
          done: done ?? rowsPage.done,
        };
      });
    const malformedMessage = 'fleet operation state is malformed';

    await expect(
      readFleetAuditFindingsPage(
        shapedPage(() => [], false),
        {
          operationId,
          limit: 1_000,
        },
      ),
    ).rejects.toThrow(malformedMessage);

    await expect(
      readFleetAuditFindingsPage(
        shapedPage((rows) => rows.filter((row) => row.ordinal !== 1)),
        { operationId, limit: 1_000 },
      ),
    ).rejects.toThrow(malformedMessage);

    await expect(
      readFleetAuditFindingsPage(
        shapedPage((rows) => [...rows.slice(0, -1), ...rows.slice(0, 1)]),
        { operationId, limit: 1_000 },
      ),
    ).rejects.toThrow(malformedMessage);

    await expect(
      readFleetAuditFindingsPage(
        shapedPage((rows) => rows.filter((row) => row.ordinal !== 0)),
        { operationId, limit: 1_000 },
      ),
    ).rejects.toThrow(malformedMessage);

    await expect(
      readFleetAuditFindingsPage(
        shapedPage((rows) =>
          rows.map((row) => ({ ...row, ordinal: row.ordinal - 1 })),
        ),
        { operationId, afterOrdinal: 0, limit: 1_000 },
      ),
    ).rejects.toThrow(malformedMessage);

    const shortNonFinal = await readFleetAuditFindingsPage(
      shapedPage((rows) => rows.slice(0, 1), false),
      { operationId, limit: 1_000 },
    );
    expect(shortNonFinal.done).toBe(false);
    expect(shortNonFinal.findings).toEqual(conforming.findings.slice(0, 1));
    expect(shortNonFinal.nextAfterOrdinal).toBe(0);

    await expect(
      readFleetAuditFindingsPage(
        shapedPage((rows) => rows.slice(0, 1), true),
        {
          operationId,
          limit: 1_000,
        },
      ),
    ).rejects.toThrow(malformedMessage);

    await expect(
      readFleetAuditFindingsPage(
        shapedPage(() => [], true),
        {
          operationId,
          limit: 1_000,
        },
      ),
    ).rejects.toThrow(malformedMessage);

    await expect(
      readFleetAuditFindingsPage(
        shapedPage((rows) => [...rows.slice(1), ...rows.slice(0, 1)]),
        { operationId, limit: 1_000 },
      ),
    ).resolves.toEqual(conforming);

    const emptyHarness = buildHarness([], emptyInventory());
    const emptyOperationId = uuidFor(91);
    const emptyRun = await startAndDrive(emptyHarness, emptyOperationId, []);
    expect(emptyRun.status).toBe('complete');
    const emptyPage = await readFleetAuditFindingsPage(
      emptyHarness.operationStore,
      { operationId: emptyOperationId, limit: 10 },
    );
    expect(emptyPage.findings).toEqual([]);
    expect(emptyPage.done).toBe(true);
    expect(emptyPage).not.toHaveProperty('nextAfterOrdinal');
  });

  it('second-world drain-vs-bounded equivalence over one frozen clock', async () => {
    const control = baseRecord('control2');
    const missing = baseRecord('missing2');
    const routeDup = baseRecord('routedup2');
    const records = [control, missing, routeDup];
    const inventory = inventoryFor([control, routeDup]);
    // `missing` is absent from `inventory.deployments` entirely ->
    // missing-deployment (global stage); its per-record call exits at its
    // own `!inventoryDeployment` guard before ever reaching `inspect`.
    // A live deployment entry that owns no record at all -> orphan-deployment.
    inventory.deployments.push({
      backend: 'plain-worker',
      scriptName: 'ghost-orphan-script2',
      tenantTag: 'ghost-tenant2',
      environment: ENVIRONMENT,
      databaseIds: ['db-ghost2'],
      durableObjectBindings: [],
      secretNames: [],
      plainTextBindings: {},
      routeHostnames: [],
      artifactVersion: 'v1',
      schemaVersion: 1,
    });
    // A second route sharing `routeDup`'s hostname under a different script.
    inventory.routes.push(
      cleanRoute(routeDup, { scriptName: 'route-dup-ghost2' }),
    );

    // Both paths read one frozen clock; the equivalence holds only over that.
    const harness = buildHarness(records, inventory, {
      auditClock: () => AUDIT_NOW,
      authorityClock: () => AUDIT_NOW,
    });

    const drainFindings = await drainWith(harness);

    const operationId = uuidFor(32);
    const result = await startAndDrive(harness, operationId, records);
    expect(result.status).toBe('complete');
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 1_000,
    });
    // Provider-supplied findings seed identically; the rest is order-preserving.
    expect(page.findings).toEqual(drainFindings);
  });

  it('the six sanitized template families carry no String(error) or duty.lastError bytes', async () => {
    const backendFailure = baseRecord('backendfailure');
    const specFailure = baseRecord('specfailure');
    const secretFailure = baseRecord('secretfailure');
    const inspectionFailure = baseRecord('inspectionfailure');
    const rearmFailure = baseRecord('rearmfailure');
    const dutyFailure = baseRecord('dutyfailure');
    const records = [
      backendFailure,
      specFailure,
      secretFailure,
      inspectionFailure,
      rearmFailure,
      dutyFailure,
    ];
    const inventory = inventoryFor(records);
    const harness = buildHarness(records, inventory, {
      throwBackendFor: new Set(['backendfailure']),
      throwSpecFor: new Set(['specfailure']),
      throwSecretFor: new Set(['secretfailure']),
      throwOnInspect: new Set(['inspectionfailure']),
      throwOnEnsureMaintenance: new Set(['rearmfailure']),
      auditClock: () => AUDIT_NOW,
      authorityClock: () => AUDIT_NOW,
    });
    harness.liveByTenant.set(
      'rearmfailure',
      cleanLiveDeployment(rearmFailure, { maintenance: UNARMED_MAINTENANCE }),
    );
    const dutyLastAttemptAt = AUDIT_NOW - 1_000;
    const dutyLastError = 'duty lastError diagnostic bytes';
    harness.liveByTenant.set(
      'dutyfailure',
      cleanLiveDeployment(dutyFailure, {
        maintenance: {
          ...HEALTHY_MAINTENANCE,
          lastSweepAttemptAt: dutyLastAttemptAt,
          lastSweepError: dutyLastError,
        },
      }),
    );
    const operationId = uuidFor(33);
    const terminal = await startAndDrive(harness, operationId, records);
    expect(terminal.status).toBe('complete');
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 100,
    });
    const expectedFamilies = [
      ['backendfailure', 'audit-error', 'backend resolver failed'],
      ['specfailure', 'audit-error', 'spec resolver failed'],
      ['secretfailure', 'audit-error', 'maintenance secret resolver failed'],
      ['inspectionfailure', 'audit-error', 'inspection failed'],
      ['rearmfailure', 'audit-error', 'maintenance re-arm failed'],
      [
        'dutyfailure',
        'maintenance-stale',
        `sweep last attempt failed at ${dutyLastAttemptAt}`,
      ],
    ] as const;
    for (const [tenantTag, kind, detail] of expectedFamilies) {
      expect(
        page.findings.find(
          (finding) => finding.tenantTag === tenantTag && finding.kind === kind,
        )?.detail,
      ).toBe(detail);
    }
    const durableRows = JSON.stringify([
      ...harness.operationStore.rows.values(),
    ]);
    for (const diagnostic of [
      'backend resolver blew up',
      'spec resolver blew up',
      'secret resolver blew up',
      'inspection blew up',
      'maintenance re-arm blew up',
      dutyLastError,
    ]) {
      expect(durableRows).not.toContain(diagnostic);
    }
  });

  it('maxItemsPerCall chunk atomicity, pinning the namespace interleave BOTH within one record and across records sharing a namespace', async () => {
    const shared = 'ns-atomic-shared';
    const withinRecord = baseRecord('atomicwithin', {
      durableObjectBindings: [
        { name: 'A', className: 'Runner', namespaceId: shared },
        {
          name: 'B',
          className: 'Runner',
          namespaceId: 'ns-atomicwithin-missing',
        },
      ],
    });
    const acrossRecord = baseRecord('atomicacross', {
      durableObjectBindings: [
        { name: 'A', className: 'Runner', namespaceId: shared },
        {
          name: 'B',
          className: 'Runner',
          namespaceId: 'ns-atomicacross-missing',
        },
      ],
    });
    const records = [withinRecord, acrossRecord];
    const inventory = inventoryFor(records);
    inventory.namespaceIds = [shared];
    const harness = buildHarness(records, inventory, {
      maxItemsPerCall: 1,
      auditClock: () => AUDIT_NOW,
      authorityClock: () => AUDIT_NOW,
    });
    const drainFindings = await drainWith(harness);
    const operationId = uuidFor(34);
    await startAndDrive(harness, operationId, records);
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 100,
    });
    expect(page.findings).toEqual(drainFindings);
    const missing = page.findings.filter(
      (finding) => finding.kind === 'missing-namespace',
    );
    expect(missing.length).toBe(2);
  });

  it('an empty fleet finalizes zero findings', async () => {
    const harness = buildHarness([], emptyInventory());
    const operationId = uuidFor(35);
    const result = await startAndDrive(harness, operationId, []);
    expect(result.status).toBe('complete');
    if (result.status === 'complete') {
      expect(result.result.findingCount).toBe(0);
    }
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 10,
    });
    expect(page.findings).toEqual([]);
  });

  it('empty iteration sources advance', async () => {
    const harness = buildHarness([], emptyInventory());
    const operationId = uuidFor(36);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    let token = expectPendingToken(started);
    const stages: string[] = [];
    for (let i = 0; i < 20; i++) {
      const result = await advanceFleetAudit(
        harness.baseOptions({ kind: 'continue', token }),
      );
      if (result.status !== 'pending') {
        expect(result.status).toBe('complete');
        break;
      }
      stages.push(result.stage.step);
      token = result.token;
    }
    expect(stages).toEqual([
      'registration-orphans',
      'deployment-orphans',
      'deployment-gaps',
      'orphan-databases',
      'orphan-routes',
      'namespace-orphans',
      'namespace-expectations',
      'r2-expected',
      'r2-orphans',
      'r2-missing-identity',
      'per-record',
      'finalize',
    ]);
  });

  it('lost commitProgress converges with the revision discriminator', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const operationId = uuidFor(37);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    const startedToken = expectPendingToken(started);

    // The start path catches commit failures and reads back durable progress.
    const lostResponse = new Error(
      'fleet operation store lost the commitProgress response',
    );
    harness.operationStore.loseNextSuccessfulCommitProgressResponse =
      lostResponse;
    await expect(
      advanceFleetAudit(
        harness.baseOptions({ kind: 'continue', token: startedToken }),
      ),
    ).rejects.toBe(lostResponse);

    const persisted =
      await harness.operationStore.readOperationById(operationId);
    expect(persisted?.state).toBe('running');
    const persistedProgress = fleetAuditProgressFromUnknown(
      persisted?.progress,
    );
    expect(persistedProgress.revision).toBe(startedToken.revision + 1);
    expect(
      harness.inventoryStore.releasedPins.some(
        (pin) => pin.pinnedBy === `fleet-audit:${operationId}`,
      ),
    ).toBe(false);

    const rowsBeforeRetry = structuredClone([...harness.operationStore.rows]);
    const retried = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token: startedToken }),
    );
    expect(retried).toEqual({
      status: 'pending',
      token: {
        ...startedToken,
        revision: persistedProgress.revision,
      },
      stage: persistedProgress.stage,
    });
    expect([...harness.operationStore.rows]).toEqual(rowsBeforeRetry);
  });

  it('the abort signal is call-local and never persisted', async () => {
    const alice = baseRecord('alice');
    const controller = new AbortController();
    const harness = buildHarness([alice], inventoryFor([alice]), {
      signal: controller.signal,
    });
    const operationId = uuidFor(38);
    const result = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    const persisted =
      await harness.operationStore.readOperationById(operationId);
    expect(JSON.stringify(persisted)).not.toContain('signal');
    expect(result.status).toBe('pending');

    const abortReason = new Error('fleet audit aborted by the caller');
    const abortedController = new AbortController();
    abortedController.abort(abortReason);
    const abortedHarness = buildHarness([alice], inventoryFor([alice]), {
      signal: abortedController.signal,
    });
    await expect(
      advanceFleetAudit(
        abortedHarness.baseOptions({
          kind: 'start',
          operationId: uuidFor(39),
          records: [alice],
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toBe(abortReason);
    expectZeroHarnessWork(abortedHarness);
  });

  it("the abort signal is re-checked INSIDE a per-record call: an abort landing after the fact-row read refuses with the caller's own reason, does no provider work, and leaves the operation running at its revision", async () => {
    const alice = baseRecord('midrecordabort');
    const controller = new AbortController();
    const harness = buildHarness([alice], inventoryFor([alice]), {
      signal: controller.signal,
    });
    const operationId = uuidFor(92);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    const atPerRecord = await driveToStage(
      harness,
      expectPendingToken(started),
      'per-record',
    );

    // The fact read places the abort after the entry check.
    const abortReason = new Error('fleet audit aborted mid per-record call');
    const readOperationRowsPage = harness.operationStore.readOperationRowsPage;
    harness.operationStore.readOperationRowsPage = async (
      input: RowsPageInput,
    ) => {
      const page = await readOperationRowsPage.call(
        harness.operationStore,
        input,
      );
      if (input.rowKind === 'fact') controller.abort(abortReason);
      return page;
    };

    const opsBefore = [...harness.opsLog];
    const fleetOpsBefore = [...harness.fleetStore.ops];
    const factReadsBefore =
      harness.operationStore.rowPageReadCounts.get('fact') ?? 0;
    const rowsBefore = structuredClone([...harness.operationStore.rows]);
    const persistedBefore = fleetAuditProgressFromUnknown(
      (await harness.operationStore.readOperationById(operationId))?.progress,
    );
    try {
      await expect(
        advanceFleetAudit(
          harness.baseOptions({ kind: 'continue', token: atPerRecord.token }),
        ),
      ).rejects.toBe(abortReason);
    } finally {
      harness.operationStore.readOperationRowsPage = readOperationRowsPage;
    }

    expect(
      harness.operationStore.rowPageReadCounts.get('fact') ?? 0,
    ).toBeGreaterThan(factReadsBefore);
    expect(harness.opsLog).toEqual(opsBefore);
    expect(harness.fleetStore.ops).toEqual(fleetOpsBefore);
    expect([...harness.operationStore.rows]).toEqual(rowsBefore);
    const persistedAfter =
      await harness.operationStore.readOperationById(operationId);
    expect(persistedAfter?.state).toBe('running');
    expect(fleetAuditProgressFromUnknown(persistedAfter?.progress)).toEqual(
      persistedBefore,
    );
    expect(harness.inventoryStore.releasedPins).toEqual([]);
  });

  it('byte scan: no secret value, Authorization bytes, or bearer token outside record rows', async () => {
    const alice = baseRecord('bytescan');
    const records = [alice];
    const inventory = inventoryFor(records);
    const harness = buildHarness(records, inventory);
    harness.secretByTenant.set(
      'bytescan',
      'Bearer super-secret-credential-value',
    );
    const leakMarker = 'leaked-provider-header';
    const sweepError = `Authorization: Bearer ${leakMarker}`;
    harness.liveByTenant.set(
      'bytescan',
      cleanLiveDeployment(alice, {
        maintenance: {
          ...HEALTHY_MAINTENANCE,
          lastSweepAttemptAt: AUDIT_NOW - 1_000,
          lastSweepError: sweepError,
        },
      }),
    );
    const operationId = uuidFor(40);
    await startAndDrive(harness, operationId, records);
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 100,
    });
    const maintenanceFinding = page.findings.find(
      (finding) =>
        finding.tenantTag === 'bytescan' &&
        finding.kind === 'maintenance-stale',
    );
    expect(maintenanceFinding).toBeDefined();
    expect(maintenanceFinding?.detail).toContain(
      `sweep last attempt failed at ${AUDIT_NOW - 1_000}`,
    );
    const expectNoSensitiveBytes = (value: unknown): void => {
      const text = JSON.stringify(value).toLowerCase();
      expect(text).not.toContain('bearer');
      expect(text).not.toContain('authorization');
      expect(text).not.toContain('super-secret-credential-value');
      expect(text).not.toContain(leakMarker);
    };
    for (const [key, rows] of harness.operationStore.rows) {
      const [, rowKind] = key.split(':');
      if (rowKind === 'record') continue;
      for (const row of rows) {
        expectNoSensitiveBytes(row.payload);
      }
    }
    const run = await harness.operationStore.readOperationById(operationId);
    expectNoSensitiveBytes(run);
    expectNoSensitiveBytes(run?.progress);
    expectNoSensitiveBytes([...harness.operationStore.operations]);
    expectNoSensitiveBytes([...harness.operationStore.intakeDigests]);
    expectNoSensitiveBytes([...harness.operationStore.heads]);
  });

  it('a hostile provider-sourced detail persists the withheld fallback; the drain is unaffected', async () => {
    const hostileScriptName = 'bearer-tainted-script';
    const ghost = baseRecord('ghosthostile');
    const records = [ghost];
    const inventory = inventoryFor(records);
    inventory.deployments.push({
      backend: 'plain-worker',
      scriptName: hostileScriptName,
      tenantTag: 'ghost-hostile-owner',
      environment: ENVIRONMENT,
      databaseIds: [],
      durableObjectBindings: [],
      secretNames: [],
      plainTextBindings: {},
      routeHostnames: [],
      artifactVersion: 'v1',
      schemaVersion: 1,
    });
    const harness = buildHarness(records, inventory, {
      auditClock: () => AUDIT_NOW,
      authorityClock: () => AUDIT_NOW,
    });
    const operationId = uuidFor(41);
    await startAndDrive(harness, operationId, records);
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 100,
    });
    const orphan = page.findings.find(
      (finding) =>
        finding.kind === 'orphan-deployment' &&
        finding.tenantTag === 'ghost-hostile-owner',
    );
    expect(orphan?.detail).toBe(
      "finding detail withheld: unsafe bytes (kind 'orphan-deployment')",
    );

    const drainFindings = await drainWith(harness);
    const drainOrphan = drainFindings.find(
      (finding) =>
        finding.kind === 'orphan-deployment' &&
        finding.tenantTag === 'ghost-hostile-owner',
    );
    expect(drainOrphan?.detail).toContain(hostileScriptName);
  });

  it('concurrent-mutation drift (class (c), both halves): a migration Fleet mutation trips the reread refusal while a Fleet-silent ready-path step does not; and a provider-truth mutation between audit start and the bounded per-record call yields the drifted inspection finding, asserted against what a start-time drain produced', async () => {
    const drifted = baseRecord('driftmaint');
    const silent = baseRecord('silentmaint');
    const providerDrifted = baseRecord('providerdrift');
    const records = [drifted, silent, providerDrifted];
    const inventory = inventoryFor(records);
    const harness = buildHarness(records, inventory, {
      auditClock: () => AUDIT_NOW,
      authorityClock: () => AUDIT_NOW,
    });
    harness.liveByTenant.set(
      'driftmaint',
      cleanLiveDeployment(drifted, { maintenance: UNARMED_MAINTENANCE }),
    );
    harness.liveByTenant.set(
      'silentmaint',
      cleanLiveDeployment(silent, { maintenance: UNARMED_MAINTENANCE }),
    );
    const startTimeDrainFindings = await drainWith(harness);
    const operationId = uuidFor(42);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records,
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(started.status).toBe('pending');
    if (started.status !== 'pending') throw new Error('unreachable');
    // A migration mutates the drifted record's Fleet row between the frozen
    // snapshot and its per-record call, tripping the reread refusal.
    harness.fleetStore.records.set('driftmaint:production', {
      ...drifted,
      updatedAt: new Date(AUDIT_NOW + 1).toISOString(),
    });
    harness.liveByTenant.set(
      'providerdrift',
      cleanLiveDeployment(providerDrifted, {
        databaseId: 'db-providerdrift-mutated',
      }),
    );
    const completed = await driveToTerminal(harness, started.token);
    expect(completed.status).toBe('complete');
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 200,
    });
    const rearmFailures = page.findings.filter(
      (finding) =>
        finding.kind === 'audit-error' &&
        finding.detail === 'maintenance re-arm failed',
    );
    expect(
      rearmFailures.some((finding) => finding.tenantTag === 'driftmaint'),
    ).toBe(true);
    expect(
      rearmFailures.some((finding) => finding.tenantTag === 'silentmaint'),
    ).toBe(false);
    const staleFindings = page.findings.filter(
      (finding) => finding.kind === 'maintenance-stale',
    );
    expect(
      staleFindings.some((finding) => finding.tenantTag === 'silentmaint'),
    ).toBe(true);
    expect(
      page.findings.some(
        (finding) =>
          finding.tenantTag === 'providerdrift' &&
          finding.kind === 'database-mismatch',
      ),
    ).toBe(true);
    expect(
      startTimeDrainFindings.some(
        (finding) =>
          finding.tenantTag === 'providerdrift' &&
          finding.kind === 'database-mismatch',
      ),
    ).toBe(false);
  });

  it('maxItemsPerCall range refusal (0 and 2,001 refused with the fixed message; 1 and 2,000 accepted; default 500 observed)', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    await expect(
      advanceFleetAudit({
        ...harness.baseOptions({
          kind: 'start',
          operationId: uuidFor(43),
          records: [alice],
          staleAfterMs: STALE_AFTER_MS,
        }),
        maxItemsPerCall: 0,
      }),
    ).rejects.toThrow('maxItemsPerCall must be an integer from 1 to 2000');
    await expect(
      advanceFleetAudit({
        ...harness.baseOptions({
          kind: 'start',
          operationId: uuidFor(44),
          records: [alice],
          staleAfterMs: STALE_AFTER_MS,
        }),
        maxItemsPerCall: 2_001,
      }),
    ).rejects.toThrow('maxItemsPerCall must be an integer from 1 to 2000');
    const acceptedMin = await advanceFleetAudit({
      ...harness.baseOptions({
        kind: 'start',
        operationId: uuidFor(45),
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
      maxItemsPerCall: 1,
    });
    expect(acceptedMin.status).toBe('pending');
    const maxHarness = buildHarness([alice], inventoryFor([alice]));
    const acceptedMax = await advanceFleetAudit({
      ...maxHarness.baseOptions({
        kind: 'start',
        operationId: uuidFor(46),
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
      maxItemsPerCall: 2_000,
    });
    expect(acceptedMax.status).toBe('pending');

    // Default 500 observed: 501 script registrations need two calls at the
    // registration-orphans stage under the default, but would need only one
    // under an explicit 2,000.
    const manyRegistrations = Array.from({ length: 501 }, (_, i) => ({
      scriptName: `ghost-script-${i}`,
      tenantTag: `ghost-tenant-${i}`,
      environment: ENVIRONMENT,
      databaseId: `db-ghost-${i}`,
      routeHostname: `ghost-${i}.example.test`,
    }));
    const bigInventory = {
      ...emptyInventory(),
      scriptRegistrations: manyRegistrations,
    };
    const defaultHarness = buildHarness([], bigInventory);
    const started = await advanceFleetAudit(
      defaultHarness.baseOptions({
        kind: 'start',
        operationId: uuidFor(47),
        records: [],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(started.status).toBe('pending');
    if (started.status !== 'pending') throw new Error('unreachable');
    // First continue: the empty `provider-findings` stage advances immediately.
    const afterProviderFindings = await advanceFleetAudit(
      defaultHarness.baseOptions({ kind: 'continue', token: started.token }),
    );
    expect(afterProviderFindings.status).toBe('pending');
    if (afterProviderFindings.status !== 'pending')
      throw new Error('unreachable');
    expect(afterProviderFindings.stage).toEqual({
      step: 'registration-orphans',
      rowOrdinal: 0,
    });
    // Second continue: one `registration-orphans` chunk under the default.
    const afterOneChunk = await advanceFleetAudit(
      defaultHarness.baseOptions({
        kind: 'continue',
        token: afterProviderFindings.token,
      }),
    );
    expect(afterOneChunk.status).toBe('pending');
    if (afterOneChunk.status === 'pending') {
      expect(afterOneChunk.stage).toEqual({
        step: 'registration-orphans',
        rowOrdinal: 500,
      });
    }
  });

  it('audit kind-lease loss at the dispatch boundary aborts with zero resolver, generation, and provider work', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const operationId = uuidFor(48);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(started.status).toBe('pending');
    if (started.status !== 'pending') throw new Error('unreachable');
    harness.operationStore.loseLeaseKind = 'audit';
    const opsBefore = harness.opsLog.length;
    const readsBefore = generationReadCounts(harness.inventoryStore);
    await expect(
      advanceFleetAudit(
        harness.baseOptions({ kind: 'continue', token: started.token }),
      ),
    ).rejects.toThrow(/no longer owned by this operation/);
    expect(harness.opsLog.length).toBe(opsBefore);
    expect(generationReadCounts(harness.inventoryStore)).toEqual(readsBefore);
  });

  it('the pin is STILL HELD after finalizeOperation; only GC, terminal failure, or abandonment releases it', async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const operationId = uuidFor(49);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(started.status).toBe('pending');
    if (started.status !== 'pending') throw new Error('unreachable');
    const result = await driveToTerminal(harness, started.token);
    expect(result.status).toBe('complete');
    expect(
      harness.inventoryStore.pins.some(
        (pin) =>
          pin.pinnedBy === `fleet-audit:${operationId}` && pin.generation === 1,
      ),
    ).toBe(true);
    expect(
      harness.inventoryStore.releasedPins.some(
        (pin) => pin.pinnedBy === `fleet-audit:${operationId}`,
      ),
    ).toBe(false);
    const releasesBeforeStaleContinue =
      harness.inventoryStore.releasedPins.length;
    const staleComplete = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token: started.token }),
    );
    expect(staleComplete).toStrictEqual(result);
    expect(harness.inventoryStore.releasedPins).toHaveLength(
      releasesBeforeStaleContinue,
    );
    // Prune releases the audit pin FIRST: a crash in that window leaves an
    // unpinned terminal operation for the next call to delete.
  });

  it("a multi-duty maintenance-stale finding (≥2 failing duties + the not-armed marker, raw bytes mid-string, including an empty-string lastError producing legacy's trailing ': ') persists the templates-only joined detail with no lastError bytes anywhere; the drain emits legacyDetails[i] byte-identically", async () => {
    const record = baseRecord('multiduty', { updatedAt: FRESH_UPDATED_AT });
    const records = [record];
    const inventory = inventoryFor(records);
    const harness = buildHarness(records, inventory, {
      auditClock: () => AUDIT_NOW,
      authorityClock: () => AUDIT_NOW,
    });
    const liveMaintenance: MaintenanceHealth = {
      armed: false,
      nextAlarmAt: null,
      lastSweepAt: null,
      lastSweepAttemptAt: AUDIT_NOW - 1_000,
      lastSweepError: 'Bearer super-secret-token',
      lastPurgeAt: null,
      lastPurgeAttemptAt: AUDIT_NOW - 2_000,
      lastPurgeError: '',
    };
    harness.liveByTenant.set(
      'multiduty',
      cleanLiveDeployment(record, { maintenance: liveMaintenance }),
    );
    const operationId = uuidFor(50);
    await startAndDrive(harness, operationId, records);
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 100,
    });
    const finding = page.findings.find((f) => f.kind === 'maintenance-stale');
    expect(finding?.detail).toBe(
      `maintenance scheduler is not armed; sweep last attempt failed at ${AUDIT_NOW - 1_000}; purge last attempt failed at ${AUDIT_NOW - 2_000}`,
    );

    const drainFindings = await drainWith(harness);
    const drainFinding = drainFindings.find(
      (f) => f.kind === 'maintenance-stale',
    );
    expect(drainFinding?.detail).toBe(
      `maintenance scheduler is not armed; sweep last attempt failed at ${AUDIT_NOW - 1_000}: Bearer super-secret-token; purge last attempt failed at ${AUDIT_NOW - 2_000}: `,
    );
  });

  it("adoption race: a start whose probe saw ABSENT but whose startOperation returned adopted-running pins the RETURNED record's progress.generation, never the locally resolved one", async () => {
    const alice = baseRecord('alice');
    const losingAuditTimeMs = AUDIT_NOW + 12_345;
    const harness = buildHarness([alice], inventoryFor([alice]), {
      auditClock: () => losingAuditTimeMs,
    });
    harness.inventoryStore.registerFinalizedGeneration(
      2,
      inventoryFor([alice]),
    );
    harness.inventoryStore.registerFinalizedGeneration(
      3,
      inventoryFor([alice]),
    );
    const operationId = uuidFor(51);

    // Simulate a concurrent winner that already started under generation 2.
    const winnerRunRecord: FleetOperationRunRecord = {
      version: 1,
      operationId,
      kind: 'audit',
      state: 'running',
      progress: {
        kind: 'audit',
        revision: 0,
        stage: { step: 'provider-findings', rowOrdinal: 0 },
        generation: 2,
        auditTimeMs: AUDIT_NOW,
        staleAfterMs: STALE_AFTER_MS,
        recordCount: 1,
        findingCount: 0,
        factCount: 0,
      } as unknown as FleetOperationRunRecord['progress'],
      updatedAt: new Date(AUDIT_NOW).toISOString(),
    };
    const matchingIntake = fleetOperationItemsIntake({
      envelope: { staleAfterMs: STALE_AFTER_MS, generation: null },
      items: [alice],
      itemByteBound: FLEET_OPERATION_RECORD_ROW_BYTE_BOUND,
    });
    expect('digest' in matchingIntake).toBe(true);
    if (!('digest' in matchingIntake)) throw new Error('unreachable');
    const matchingDigest = matchingIntake.digest;
    harness.operationStore.operations.set(operationId, winnerRunRecord);
    harness.operationStore.intakeDigests.set(operationId, matchingDigest);
    harness.operationStore.heads.set('audit', operationId);
    harness.operationStore.probeMiss.add(operationId);

    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(started.status).toBe('pending');
    expect(harness.inventoryStore.latestGeneration).toBe(3);
    expect(harness.inventoryStore.pins).toEqual([
      { generation: 2, pinnedBy: `fleet-audit:${operationId}` },
    ]);
    const persisted = fleetAuditProgressFromUnknown(
      (await harness.operationStore.readOperationById(operationId))?.progress,
    );
    expect(persisted.generation).toBe(2);
    expect(persisted.auditTimeMs).toBe(AUDIT_NOW);
    expect(persisted.auditTimeMs).not.toBe(losingAuditTimeMs);
  });

  it('readAllFleetOperationRows fails closed on a page with zero rows before done, on a repeating page, on a duplicate ordinal within one page, on an overlapping row, and on a gapped page sequence; it caps the row read by kind and reads descending pages correctly', async () => {
    let mutationCalls = 0;
    const store: FleetOperationStore = {
      withAccountOperationLease: async () => {
        mutationCalls += 1;
        throw new Error('unused');
      },
      readOperationById: async () => undefined,
      readOperationRowsPage: async () => ({ rows: [], done: false }),
      pruneFleetOperations: async () => {
        mutationCalls += 1;
        return { deleted: 0, releasedPins: 0 };
      },
    };
    await expect(
      readAllFleetOperationRows(store, uuidFor(53), 'record'),
    ).rejects.toThrow('fleet operation state is malformed');

    let repeatingPageCalls = 0;
    const repeatingRow: FleetOperationStagedRow = {
      rowKind: 'record',
      ordinal: 0,
      payload: {},
    };
    const repeatingStore: FleetOperationStore = {
      ...store,
      readOperationRowsPage: async () => {
        repeatingPageCalls += 1;
        return { rows: [repeatingRow], done: false };
      },
    };
    await expect(
      readAllFleetOperationRows(repeatingStore, uuidFor(53), 'record'),
    ).rejects.toThrow('fleet operation state is malformed');
    expect(repeatingPageCalls).toBe(2);

    let duplicatePageCalls = 0;
    const duplicateBaseStore = new FakeOperationStore();
    const duplicateOrdinalStore = pageTransformingStore(
      duplicateBaseStore,
      (_readPage, input) => {
        duplicatePageCalls += 1;
        return {
          rows: [
            { rowKind: input.rowKind, ordinal: 0, payload: {} },
            { rowKind: input.rowKind, ordinal: 0, payload: {} },
          ],
          done: false,
        };
      },
    );
    await expect(
      readAllFleetOperationRows(duplicateOrdinalStore, uuidFor(533), 'record'),
    ).rejects.toThrow('fleet operation state is malformed');
    expect(duplicatePageCalls).toBe(1);
    expect(duplicateBaseStore.rowPageReadCounts.size).toBe(0);

    expect(FLEET_OPERATION_ROW_READ_BOUND).toBe(990_000);
    let advancingPageCalls = 0;
    let advancingRows = 0;
    const advancingStore: FleetOperationStore = {
      ...store,
      readOperationRowsPage: async (input) => {
        advancingPageCalls += 1;
        const firstOrdinal = (input.afterOrdinal ?? -1) + 1;
        const pageLength = Math.min(
          input.limit,
          FLEET_OPERATION_ITEM_BOUND + 1 - advancingRows,
        );
        advancingRows += pageLength;
        return {
          rows: Array.from({ length: pageLength }, (_, index) => ({
            rowKind: input.rowKind,
            ordinal: firstOrdinal + index,
            payload: {},
          })),
          done: advancingRows === FLEET_OPERATION_ITEM_BOUND + 1,
        };
      },
    };
    await expect(
      readAllFleetOperationRows(advancingStore, uuidFor(53), 'record'),
    ).rejects.toThrow('fleet operation state is malformed');
    expect(advancingRows).toBe(FLEET_OPERATION_ITEM_BOUND + 1);
    expect(advancingPageCalls).toBe(FLEET_OPERATION_ITEM_BOUND / 1_000 + 1);

    const orderedRows: FleetOperationStagedRow[] = Array.from(
      { length: 1_001 },
      (_, ordinal) => ({ rowKind: 'record', ordinal, payload: { ordinal } }),
    );
    const orderedStore = new FakeOperationStore();
    const orderedOperationId = uuidFor(530);
    orderedStore.rows.set(`${orderedOperationId}:record`, orderedRows);
    const expectedRows = await readAllFleetOperationRows(
      orderedStore,
      orderedOperationId,
      'record',
    );
    expect(expectedRows.map((row) => row.ordinal)).toEqual(
      Array.from({ length: 1_001 }, (_, ordinal) => ordinal),
    );
    expect(new Set(expectedRows.map((row) => row.ordinal)).size).toBe(
      expectedRows.length,
    );
    const descendingStore = pageTransformingStore(
      orderedStore,
      async (readPage) => {
        const page = await readPage();
        return { ...page, rows: [...page.rows].reverse() };
      },
    );
    await expect(
      readAllFleetOperationRows(descendingStore, orderedOperationId, 'record'),
    ).resolves.toEqual(expectedRows);

    let overlappingPageCalls = 0;
    const overlappingBaseStore = new FakeOperationStore();
    const overlappingStore = pageTransformingStore(
      overlappingBaseStore,
      (_readPage, input) => {
        overlappingPageCalls += 1;
        return input.afterOrdinal === undefined
          ? {
              rows: [{ rowKind: input.rowKind, ordinal: 1, payload: {} }],
              done: false,
            }
          : {
              rows: [
                { rowKind: input.rowKind, ordinal: 2, payload: {} },
                { rowKind: input.rowKind, ordinal: 1, payload: {} },
              ],
              done: true,
            };
      },
    );
    await expect(
      readAllFleetOperationRows(overlappingStore, uuidFor(531), 'record'),
    ).rejects.toThrow('fleet operation state is malformed');
    expect(overlappingPageCalls).toBe(2);
    expect(overlappingBaseStore.rowPageReadCounts.size).toBe(0);
    expect(overlappingBaseStore.operations.size).toBe(0);
    expect(overlappingBaseStore.rows.size).toBe(0);
    expect(overlappingBaseStore.heads.size).toBe(0);

    const gappedFirstPages = [
      [5, 1],
      [5, 0],
    ] as const;
    for (const [index, firstPageOrdinals] of gappedFirstPages.entries()) {
      let gappedPageCalls = 0;
      const gappedStore: FleetOperationStore = {
        ...store,
        readOperationRowsPage: async (input) => {
          gappedPageCalls += 1;
          return input.afterOrdinal === undefined
            ? {
                rows: firstPageOrdinals.map((ordinal) => ({
                  rowKind: input.rowKind,
                  ordinal,
                  payload: {},
                })),
                done: false,
              }
            : {
                rows: [{ rowKind: input.rowKind, ordinal: 6, payload: {} }],
                done: true,
              };
        },
      };
      await expect(
        readAllFleetOperationRows(gappedStore, uuidFor(534 + index), 'record'),
      ).rejects.toThrow('fleet operation state is malformed');
      expect(gappedPageCalls).toBe(2);
    }
    expect(mutationCalls).toBe(0);
  });

  it('a start with no resolvable generation refuses with the fixed message and persists nothing', async () => {
    const alice = baseRecord('nogeneration');
    const harness = buildHarness([alice], inventoryFor([alice]));
    harness.inventoryStore.latestGeneration = undefined;
    harness.inventoryStore.refs.clear();
    harness.inventoryStore.generations.clear();
    harness.inventoryStore.runs.clear();
    await expect(
      advanceFleetAudit(
        harness.baseOptions({
          kind: 'start',
          operationId: uuidFor(54),
          records: [alice],
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toThrow('no finalized fleet inventory generation is available');
    expect(harness.operationStore.operations.size).toBe(0);
    expect(harness.operationStore.rows.size).toBe(0);
    expect(harness.operationStore.heads.size).toBe(0);
    expect(harness.operationStore.intakeDigests.size).toBe(0);
    expect(harness.inventoryStore.pins).toEqual([]);
    expect(harness.inventoryStore.releasedPins).toEqual([]);
    expect(generationReadCounts(harness.inventoryStore)).toEqual({
      latest: 1,
      finalized: 0,
      runByOperation: 0,
    });
  });

  it('a start-time pinGeneration refusal durably fails the operation as generation-unavailable with the head released', async () => {
    const alice = baseRecord('pinrefusal');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const operationId = uuidFor(55);
    harness.inventoryStore.pinFailsForGeneration = 1;
    const failed = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(failed.status).toBe('failed');
    if (failed.status !== 'failed') throw new Error('unreachable');
    expect(failed.failure).toEqual({ reason: 'generation-unavailable' });
    expect(
      (await harness.operationStore.readOperationById(operationId))?.state,
    ).toBe('failed');
    expect(harness.inventoryStore.pins).toEqual([]);
    expect(harness.inventoryStore.releasedPins).toContainEqual({
      generation: 1,
      pinnedBy: `fleet-audit:${operationId}`,
    });
    expect(harness.operationStore.heads.has('audit')).toBe(false);

    harness.inventoryStore.pinFailsForGeneration = undefined;
    const next = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId: uuidFor(56),
        records: [alice],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(next.status).toBe('pending');
  });

  it('findings-page reads and abandonment refuse an unknown id (FleetOperationTokenOperationError) and a foreign-kind id (the fixed cross-kind refusal) before any write, in both the running and terminal abandonment branches', async () => {
    const alice = baseRecord('foreignids');
    const harness = buildHarness([alice], inventoryFor([alice]));
    const unknownId = uuidFor(57);
    const runningForeignId = uuidFor(58);
    const terminalForeignId = uuidFor(59);
    const runningForeign: FleetOperationRunRecord = {
      version: 1,
      operationId: runningForeignId,
      kind: 'migration',
      state: 'running',
      progress: { kind: 'migration', revision: 3 },
      updatedAt: new Date(AUDIT_NOW).toISOString(),
    };
    const terminalForeign: FleetOperationRunRecord = {
      version: 1,
      operationId: terminalForeignId,
      kind: 'migration',
      state: 'finalized',
      progress: { kind: 'migration', revision: 7 },
      updatedAt: new Date(AUDIT_NOW).toISOString(),
      terminalAtMs: AUDIT_NOW,
    };
    harness.operationStore.operations.set(runningForeignId, runningForeign);
    harness.operationStore.operations.set(terminalForeignId, terminalForeign);
    harness.operationStore.heads.set('migration', runningForeignId);
    const durableStateBefore = {
      operations: structuredClone([...harness.operationStore.operations]),
      rows: structuredClone([...harness.operationStore.rows]),
      heads: structuredClone([...harness.operationStore.heads]),
      intakeDigests: structuredClone([...harness.operationStore.intakeDigests]),
      pins: structuredClone(harness.inventoryStore.pins),
      releasedPins: structuredClone(harness.inventoryStore.releasedPins),
    };

    await expect(
      readFleetAuditFindingsPage(harness.operationStore, {
        operationId: unknownId,
        limit: 10,
      }),
    ).rejects.toBeInstanceOf(FleetOperationTokenOperationError);
    await expect(
      readFleetAuditFindingsPage(harness.operationStore, {
        operationId: unknownId,
        limit: 10,
      }),
    ).rejects.toThrow(`no fleet operation '${unknownId}'`);
    await expect(
      abandonFleetAuditOperation({
        operationStore: harness.operationStore,
        inventoryStore: harness.inventoryStore,
        operationId: unknownId,
      }),
    ).rejects.toBeInstanceOf(FleetOperationTokenOperationError);
    await expect(
      abandonFleetAuditOperation({
        operationStore: harness.operationStore,
        inventoryStore: harness.inventoryStore,
        operationId: unknownId,
      }),
    ).rejects.toThrow(`no fleet operation '${unknownId}'`);

    for (const operationId of [runningForeignId, terminalForeignId]) {
      const message = `fleet operation '${operationId}' belongs to the other operation kind`;
      await expect(
        readFleetAuditFindingsPage(harness.operationStore, {
          operationId,
          limit: 10,
        }),
      ).rejects.toThrow(message);
      await expect(
        abandonFleetAuditOperation({
          operationStore: harness.operationStore,
          inventoryStore: harness.inventoryStore,
          operationId,
        }),
      ).rejects.toThrow(message);
    }

    expect({
      operations: [...harness.operationStore.operations],
      rows: [...harness.operationStore.rows],
      heads: [...harness.operationStore.heads],
      intakeDigests: [...harness.operationStore.intakeDigests],
      pins: harness.inventoryStore.pins,
      releasedPins: harness.inventoryStore.releasedPins,
    }).toStrictEqual(durableStateBefore);
    expect(
      await harness.operationStore.readOperationById(runningForeignId),
    ).toStrictEqual(runningForeign);
    expect(
      await harness.operationStore.readOperationById(terminalForeignId),
    ).toStrictEqual(terminalForeign);
  });

  it('a finding row the read codec would reject fails the operation closed at write time instead of poisoning the findings page', async () => {
    const inventory = emptyInventory();
    inventory.findings.push({
      tenantTag: 'provider-observation',
      environment: ENVIRONMENT,
      kind: 'out-of-vocabulary' as FleetInventoryFinding['kind'],
      detail: 'malformed route fixture',
    });
    const harness = buildHarness([], inventory);
    const operationId = uuidFor(60);
    const started = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId,
        records: [],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(started.status).toBe('pending');
    if (started.status !== 'pending') throw new Error('unreachable');
    expect(started.stage).toEqual({ step: 'provider-findings', rowOrdinal: 0 });
    await expect(
      advanceFleetAudit(
        harness.baseOptions({ kind: 'continue', token: started.token }),
      ),
    ).rejects.toThrow('fleet operation state is malformed');
    expect(
      harness.operationStore.rows.get(`${operationId}:finding`)?.length ?? 0,
    ).toBe(0);
    expect(
      (await harness.operationStore.readOperationById(operationId))?.state,
    ).toBe('running');
    await expect(
      readFleetAuditFindingsPage(harness.operationStore, {
        operationId,
        limit: 10,
      }),
    ).rejects.toThrow(`fleet audit operation '${operationId}' is not terminal`);
    await abandonFleetAuditOperation({
      operationStore: harness.operationStore,
      inventoryStore: harness.inventoryStore,
      operationId,
    });
    const abandoned =
      await harness.operationStore.readOperationById(operationId);
    expect(abandoned?.state).toBe('failed');
    expect(
      fleetAuditProgressFromUnknown(abandoned?.progress).failure?.reason,
    ).toBe('operator-abandoned');
    expect(harness.operationStore.heads.has('audit')).toBe(false);
    expect(harness.inventoryStore.releasedPins).toContainEqual({
      generation: 1,
      pinnedBy: `fleet-audit:${operationId}`,
    });

    const observed = baseRecord('observed');
    const observedInventory = inventoryFor([observed]);
    const observedFindings: FleetInventoryFinding[] = [
      {
        tenantTag: '',
        environment: ENVIRONMENT,
        kind: 'malformed-route',
        detail: 'empty provider tenant tag',
      },
      {
        tenantTag: 'provider-tenant',
        environment: 'production\u0000observed',
        kind: 'stale-route',
        detail: 'control-byte provider environment',
      },
      {
        tenantTag: 'provider-tenant',
        environment: ENVIRONMENT,
        kind: 'stale-route',
        detail: 'x\u0000y',
      },
    ];
    observedInventory.findings.push(...observedFindings);
    const liveNamespaceId = 'live\u0000namespace';
    const observedHarness = buildHarness([observed], observedInventory, {
      auditClock: () => AUDIT_NOW,
      authorityClock: () => AUDIT_NOW,
    });
    observedHarness.liveByTenant.set(
      observed.tenantTag,
      cleanLiveDeployment(observed, {
        durableObjectBindings: [
          { name: 'RUNNER', className: 'Runner', namespaceId: liveNamespaceId },
        ],
      }),
    );
    const observedOperationId = uuidFor(64);
    const observedTerminal = await startAndDrive(
      observedHarness,
      observedOperationId,
      [observed],
    );
    expect(observedTerminal.status).toBe('complete');
    const observedPage = await readFleetAuditFindingsPage(
      observedHarness.operationStore,
      { operationId: observedOperationId, limit: 100 },
    );
    const boundedProviderFindings = observedFindings.map((finding) =>
      finding.detail === 'x\u0000y'
        ? {
            ...finding,
            detail:
              "finding detail withheld: unsafe bytes (kind 'stale-route')",
          }
        : finding,
    );
    expect(
      observedPage.findings.slice(0, observedFindings.length),
    ).toStrictEqual(boundedProviderFindings);
    const stagedFacts =
      observedHarness.operationStore.rows.get(`${observedOperationId}:fact`) ??
      [];
    expect(stagedFacts).toContainEqual(
      expect.objectContaining({
        payload: {
          factKind: 'namespace-owner',
          key: liveNamespaceId,
          tenantTag: observed.tenantTag,
          environment: observed.environment,
        },
      }),
    );
    const readFacts = (
      await readAllFleetOperationRows(
        observedHarness.operationStore,
        observedOperationId,
        'fact',
      )
    ).map((row) => fleetAuditFactRowFromUnknown(row.payload));
    expect(readFacts).toContainEqual({
      factKind: 'namespace-owner',
      key: liveNamespaceId,
      tenantTag: observed.tenantTag,
      environment: observed.environment,
    });
    const drainFindings = await drainWith(observedHarness);
    expect(drainFindings.slice(0, observedFindings.length)).toStrictEqual(
      observedFindings,
    );
    expect(observedPage.findings).toStrictEqual(
      drainFindings.map((finding) =>
        finding.detail === 'x\u0000y'
          ? {
              ...finding,
              detail:
                "finding detail withheld: unsafe bytes (kind 'stale-route')",
            }
          : finding,
      ),
    );
  });

  const GRAMMAR_REFUSAL =
    'fleet audit record tenantTag and environment must satisfy the deployment identifier grammar';
  const STRUCTURE_REFUSAL =
    'fleet audit record exceeds the intake structure bounds';
  const ROW_BYTE_REFUSAL =
    'fleet audit record exceeds the staged row byte bound';
  const CLOCK_REFUSAL =
    'fleet audit auditClock sample must be a non-negative safe integer representable by Date';

  it('a start whose record carries a malformed deployment identifier refuses with the fixed message and persists nothing', async () => {
    const cases = [
      baseRecord('emptyenvironment', { environment: '' }),
      baseRecord('control\u0000tenant'),
    ];
    for (const [index, record] of cases.entries()) {
      const harness = buildHarness([record], inventoryFor([record]));
      const attempt = advanceFleetAudit(
        harness.baseOptions({
          kind: 'start',
          operationId: uuidFor(61 + index),
          records: [record],
          staleAfterMs: STALE_AFTER_MS,
        }),
      );
      await expect(attempt).rejects.toBeInstanceOf(Error);
      await expect(attempt).rejects.toHaveProperty('message', GRAMMAR_REFUSAL);
      expectZeroHarnessWork(harness);
    }
  });

  const AFTER_LEASE_AND_PROBE = {
    leaseCount: 1,
    readOperationByIdCalls: 1,
    generationReads: { latest: 1, finalized: 0, runByOperation: 0 },
  } as const;

  const preflightRefusalCases: readonly PreflightRefusalCase[] = [
    {
      name: 'a non-positive explicit generation',
      fleet: [baseRecord('preflight')],
      operationId: uuidFor(70),
      intake: () => [baseRecord('preflight')],
      generation: 0,
      message: 'generation must be a positive safe integer',
    },
    {
      name: 'a non-integer explicit generation',
      fleet: [baseRecord('preflight')],
      operationId: uuidFor(71),
      intake: () => [baseRecord('preflight')],
      generation: 1.5,
      message: 'generation must be a positive safe integer',
    },
    {
      name: 'a non-string tenant tag',
      fleet: [],
      operationId: uuidFor(72),
      intake: () => [
        {
          ...baseRecord('nonstringtenant'),
          tenantTag: null as unknown as string,
        },
      ],
      message: GRAMMAR_REFUSAL,
    },
    {
      name: 'a record with a throwing tenantTag',
      fleet: [],
      operationId: uuidFor(720),
      intake: () => {
        const record = baseRecord('throwingtenant');
        Object.defineProperty(record, 'tenantTag', {
          get() {
            throw new Error('boom');
          },
          enumerable: true,
        });
        return [record];
      },
      message: STRUCTURE_REFUSAL,
    },
    {
      name: 'a null record element',
      fleet: [],
      operationId: uuidFor(79),
      intake: () => [null as unknown as FleetRecord],
      message: STRUCTURE_REFUSAL,
    },
    {
      name: 'a record over the staged row bound',
      fleet: [],
      operationId: uuidFor(73),
      intake: () => {
        const record = Object.assign(
          baseRecord('oversized'),
          Object.fromEntries(
            Array.from({ length: 25 }, (_, index) => [
              `padding${index}`,
              'x'.repeat(FLEET_OPERATION_STRING_BYTE_BOUND),
            ]),
          ),
        );
        const metrics = plainDataMetrics(record);
        expect(
          new TextEncoder().encode(canonicalFleetOperationBytes(record))
            .byteLength,
        ).toBeGreaterThan(FLEET_OPERATION_RECORD_ROW_BYTE_BOUND);
        expect(metrics.nodeCount).toBeLessThan(FLEET_OPERATION_NODE_BOUND);
        expect(metrics.maxStringBytes).toBeLessThanOrEqual(
          FLEET_OPERATION_STRING_BYTE_BOUND,
        );
        return [record];
      },
      message: ROW_BYTE_REFUSAL,
    },
    {
      name: 'a non-integer audit clock sample',
      fleet: [baseRecord('preflight')],
      operationId: uuidFor(74),
      intake: () => [baseRecord('preflight')],
      auditClock: () => 1.5,
      message: CLOCK_REFUSAL,
      coordination: AFTER_LEASE_AND_PROBE,
    },
    {
      name: 'an out-of-Date-range clock sample',
      fleet: [baseRecord('preflight')],
      operationId: uuidFor(75),
      intake: () => [baseRecord('preflight')],
      auditClock: () => 9e15,
      message: CLOCK_REFUSAL,
      coordination: AFTER_LEASE_AND_PROBE,
    },
    {
      name: 'a record over the intake node bound',
      fleet: [],
      operationId: uuidFor(76),
      intake: () => [
        Object.assign(baseRecord('toomanynodes'), {
          padding: Array.from({ length: 9_000 }, () => null),
        }),
      ],
      message: STRUCTURE_REFUSAL,
    },
    {
      name: 'a record over the intake string bound',
      fleet: [],
      operationId: uuidFor(77),
      intake: () => [
        Object.assign(baseRecord('overlongstring'), {
          padding: 'x'.repeat(5_000),
        }),
      ],
      message: STRUCTURE_REFUSAL,
    },
    {
      name: 'a record over both bounds (row wins)',
      fleet: [],
      operationId: uuidFor(78),
      intake: () => {
        const padding = Object.fromEntries(
          Array.from({ length: 8_000 }, (_, index) => [
            `padding${index}`,
            'x'.repeat(2_087),
          ]),
        );
        const record = Object.assign(baseRecord('overlappingbounds'), padding);
        expect(plainDataMetrics(record).nodeCount).toBeLessThan(
          FLEET_OPERATION_NODE_BOUND,
        );
        // The fixture is ASCII, so string lengths equal UTF-8 byte lengths.
        let serializedByteCount = JSON.stringify(
          baseRecord('overlappingbounds'),
        ).length;
        for (const [key, value] of Object.entries(padding)) {
          serializedByteCount += 1 + 2 + key.length + 1 + 2 + value.length;
        }
        expect(serializedByteCount).toBeGreaterThan(
          FLEET_OPERATION_INTAKE_BYTE_BOUND,
        );
        return [record];
      },
      message: ROW_BYTE_REFUSAL,
    },
  ];

  const PREFLIGHT_TITLE =
    'a start refuses $name before any operation row, staged row, or pin';

  it.each(preflightRefusalCases)(PREFLIGHT_TITLE, async (testCase) => {
    const harness = buildHarness(
      testCase.fleet,
      inventoryFor(testCase.fleet),
      testCase.auditClock === undefined
        ? {}
        : { auditClock: testCase.auditClock },
    );
    const attempt = advanceFleetAudit(
      harness.baseOptions({
        kind: 'start',
        operationId: testCase.operationId,
        records: testCase.intake(),
        staleAfterMs: STALE_AFTER_MS,
        ...(testCase.generation === undefined
          ? {}
          : { generation: testCase.generation }),
      }),
    );
    await expect(attempt).rejects.toBeInstanceOf(Error);
    await expect(attempt).rejects.toHaveProperty('message', testCase.message);
    expectZeroHarnessWork(harness, testCase.coordination);
  });

  it('an emitted finding or fact row whose serialized payload or any string exceeds the staged-row envelope fails the operation durably as emission-bound-exceeded with the pin released, before the store sees the row', async () => {
    const escapedNamespaceId = '\u0000'.repeat(3_000);
    const overlongDatabaseId = 'd'.repeat(5_000);
    const escapedDriftedDatabaseId = 'db-escapedfact-drifted';
    const cases = [
      {
        tenantTag: 'escapedfact',
        live: (record: FleetRecord) =>
          cleanLiveDeployment(record, {
            databaseId: escapedDriftedDatabaseId,
            durableObjectBindings: [
              {
                name: 'RUNNER',
                className: 'Runner',
                namespaceId: escapedNamespaceId,
              },
            ],
          }),
        payload: {
          factKind: 'namespace-owner',
          key: escapedNamespaceId,
          tenantTag: 'escapedfact',
          environment: ENVIRONMENT,
        },
      },
      {
        tenantTag: 'overlongfact',
        live: (record: FleetRecord) =>
          cleanLiveDeployment(record, { databaseId: overlongDatabaseId }),
        payload: {
          factKind: 'database-owner',
          key: overlongDatabaseId,
          tenantTag: 'overlongfact',
          environment: ENVIRONMENT,
        },
      },
    ] as const;

    expect(new TextEncoder().encode(escapedNamespaceId).byteLength).toBe(3_000);
    expect(
      new TextEncoder().encode(JSON.stringify(cases[0].payload)).byteLength,
    ).toBeGreaterThan(FLEET_OPERATION_ROW_PAYLOAD_BYTE_BOUND);
    expect(new TextEncoder().encode(overlongDatabaseId).byteLength).toBe(5_000);
    expect(
      new TextEncoder().encode(overlongDatabaseId).byteLength,
    ).toBeGreaterThan(FLEET_OPERATION_STRING_BYTE_BOUND);

    for (const [index, testCase] of cases.entries()) {
      const record = baseRecord(testCase.tenantTag);
      const harness = buildHarness([record], inventoryFor([record]), {
        auditClock: () => AUDIT_NOW,
        authorityClock: () => AUDIT_NOW,
      });
      harness.liveByTenant.set(record.tenantTag, testCase.live(record));
      if (index === 0) {
        const drainFindings = await drainWith(harness);
        expect(drainFindings).toEqual([
          {
            tenantTag: testCase.tenantTag,
            environment: ENVIRONMENT,
            kind: 'database-mismatch',
            detail: `expected ${record.databaseId}, found ${escapedDriftedDatabaseId}`,
          },
        ]);
      }
      const operationId = uuidFor(80 + index);
      const started = await advanceFleetAudit(
        harness.baseOptions({
          kind: 'start',
          operationId,
          records: [record],
          staleAfterMs: STALE_AFTER_MS,
        }),
      );
      expect(started.status).toBe('pending');
      if (started.status !== 'pending') throw new Error('unreachable');
      const result = await driveToStage(harness, started.token, 'per-record');
      expect(result.stage).toEqual({ step: 'per-record', recordOrdinal: 0 });

      const codecCallsBefore = harness.operationStore.stagedRowCodecCalls;
      const failed = await advanceFleetAudit(
        harness.baseOptions({ kind: 'continue', token: result.token }),
      );
      expect(failed.status).toBe('failed');
      if (failed.status !== 'failed') throw new Error('unreachable');
      expect(failed.failure).toEqual({
        reason: 'emission-bound-exceeded',
        itemOrdinal: 0,
      });
      expect(harness.operationStore.stagedRowCodecCalls).toBe(codecCallsBefore);
      const persisted =
        await harness.operationStore.readOperationById(operationId);
      expect(persisted?.state).toBe('failed');
      expect(
        fleetAuditProgressFromUnknown(persisted?.progress).failure,
      ).toEqual({
        reason: 'emission-bound-exceeded',
        itemOrdinal: 0,
      });
      expect(harness.operationStore.heads.has('audit')).toBe(false);
      expect(harness.inventoryStore.releasedPins).toContainEqual({
        generation: 1,
        pinnedBy: `fleet-audit:${operationId}`,
      });
    }

    const globalInventory = emptyInventory();
    globalInventory.findings.push({
      tenantTag: '\u0000'.repeat(3_000),
      environment: ENVIRONMENT,
      kind: 'malformed-route',
      detail: 'provider pass-through finding',
    });
    const globalHarness = buildHarness([], globalInventory);
    const globalOperationId = uuidFor(82);
    const globalStarted = await advanceFleetAudit(
      globalHarness.baseOptions({
        kind: 'start',
        operationId: globalOperationId,
        records: [],
        staleAfterMs: STALE_AFTER_MS,
      }),
    );
    expect(globalStarted.status).toBe('pending');
    if (globalStarted.status !== 'pending') throw new Error('unreachable');
    expect(globalStarted.stage).toEqual({
      step: 'provider-findings',
      rowOrdinal: 0,
    });
    const globalCodecCallsBefore =
      globalHarness.operationStore.stagedRowCodecCalls;
    const globalFailed = await advanceFleetAudit(
      globalHarness.baseOptions({
        kind: 'continue',
        token: globalStarted.token,
      }),
    );
    expect(globalFailed.status).toBe('failed');
    if (globalFailed.status !== 'failed') throw new Error('unreachable');
    expect(globalFailed.failure).toStrictEqual({
      reason: 'emission-bound-exceeded',
    });
    expect(globalHarness.operationStore.stagedRowCodecCalls).toBe(
      globalCodecCallsBefore,
    );
    expect(
      (await globalHarness.operationStore.readOperationById(globalOperationId))
        ?.state,
    ).toBe('failed');
    expect(globalHarness.operationStore.heads.has('audit')).toBe(false);
    expect(globalHarness.inventoryStore.releasedPins).toContainEqual({
      generation: 1,
      pinnedBy: `fleet-audit:${globalOperationId}`,
    });
  });

  it("a start whose grammar-valid records' aggregate node count exceeds 8,192 creates the operation with recordCount equal to the INTAKE SNAPSHOT — mid-start caller mutation of the records array, of record[0], and of the action's own operationId, staleAfterMs and generation notwithstanding — and its first per-record call reads the accumulated record rows across two pages", async () => {
    const records = Array.from({ length: 1_001 }, (_, index) =>
      baseRecord(`aggregate${index}`),
    );
    expect(plainDataMetrics(records).nodeCount).toBeGreaterThan(
      FLEET_OPERATION_NODE_BOUND,
    );
    expect(() =>
      fleetOperationIntakeDigest({
        records,
        staleAfterMs: STALE_AFTER_MS,
        generation: null,
      }),
    ).toThrow('fleet operation state is malformed');

    const operationId = uuidFor(83);
    const probeMutationOperationId = uuidFor(829);
    const clockMutationOperationId = uuidFor(830);
    const pinMutationOperationId = uuidFor(831);
    const action = {
      kind: 'start' as const,
      operationId,
      records,
      staleAfterMs: STALE_AFTER_MS,
      generation: undefined as number | undefined,
    };
    const intakeCount = records.length;
    const intakeTenantTag = records[0]?.tenantTag;
    let mutationOrdinal = 0;
    const mutateCaller = (
      nextOperationId: string,
      nextStaleAfterMs: number,
    ) => {
      mutationOrdinal += 1;
      records.push(baseRecord(`appendedmidstart${mutationOrdinal}`));
      Object.assign(records[0] as FleetRecord, {
        tenantTag: `mutatedtenant${mutationOrdinal}`,
      });
      action.staleAfterMs = nextStaleAfterMs;
      action.operationId = nextOperationId;
      // Generation 2 is never registered here, so an implementation that
      // re-read `action.generation` instead of the value hoisted at the top
      // of the start would pin generation 2 rather than the latest finalized
      // 1 — and the pin assertion below would see it.
      action.generation = 2;
    };
    const harness = buildHarness(records, inventoryFor(records), {
      auditClock: () => {
        mutateCaller(clockMutationOperationId, STALE_AFTER_MS + 1);
        return AUDIT_NOW;
      },
    });
    const pinGeneration = harness.inventoryStore.pinGeneration.bind(
      harness.inventoryStore,
    );
    harness.inventoryStore.pinGeneration = async (input) => {
      mutateCaller(pinMutationOperationId, STALE_AFTER_MS + 2);
      await pinGeneration(input);
    };
    harness.operationStore.onNextReadOperationById = () => {
      mutateCaller(probeMutationOperationId, STALE_AFTER_MS + 3);
    };
    const started = await advanceFleetAudit(harness.baseOptions(action));
    expect(started.status).toBe('pending');
    if (started.status !== 'pending') throw new Error('unreachable');
    const persisted =
      await harness.operationStore.readOperationById(operationId);
    expect(persisted).toBeDefined();
    if (!persisted) throw new Error('unreachable');
    expect(persisted.state).toBe('running');
    const progress = fleetAuditProgressFromUnknown(persisted.progress);
    expect(progress.recordCount).toBe(intakeCount);
    expect(progress.staleAfterMs).toBe(STALE_AFTER_MS);
    expect(
      await harness.operationStore.readOperationById(probeMutationOperationId),
    ).toBeUndefined();
    expect(
      await harness.operationStore.readOperationById(clockMutationOperationId),
    ).toBeUndefined();
    expect(
      await harness.operationStore.readOperationById(pinMutationOperationId),
    ).toBeUndefined();
    // The mid-call `action.generation = 2` did not reach the resolution: the
    // operation pins, and persists, the latest finalized generation 1.
    expect(action.generation).toBe(2);
    expect(progress.generation).toBe(1);
    expect(harness.inventoryStore.pins).toEqual([
      { generation: 1, pinnedBy: `fleet-audit:${operationId}` },
    ]);
    const stagedRecords = await readAllFleetOperationRows(
      harness.operationStore,
      operationId,
      'record',
    );
    expect(stagedRecords).toHaveLength(intakeCount);
    expect(stagedRecords[0]?.payload.tenantTag).toBe(intakeTenantTag);

    // AFTER-START MUTATION. Only the array length is restored: the rest of
    // this title drives `continue`, which never reads `action` again, and
    // `records[0].tenantTag` is overwritten on the very next line.
    records.length = intakeCount;
    Object.assign(records[0] as FleetRecord, {
      tenantTag: 'mutatedafterstart',
    });
    records.push(baseRecord('appendedafterstart'));
    expect(
      fleetAuditProgressFromUnknown(
        (await harness.operationStore.readOperationById(operationId))?.progress,
      ).recordCount,
    ).toBe(intakeCount);
    expect(
      await readAllFleetOperationRows(
        harness.operationStore,
        operationId,
        'record',
      ),
    ).toEqual(stagedRecords);
    records.pop();
    Object.assign(records[0] as FleetRecord, { tenantTag: intakeTenantTag });

    // Skip the preceding global stages so this fixture pays for only the last
    // global-stage call and the first per-record call that it measures.
    //
    // A DELIBERATE raw cast, unlike every read above: the point is to write a
    // fast-forwarded stage straight into the store. Routing it through
    // `fleetAuditProgressFromUnknown` would only re-validate what this line
    // just built, and the codec is what the coordinator must apply on the way
    // back OUT.
    harness.operationStore.operations.set(operationId, {
      ...persisted,
      progress: {
        ...progress,
        stage: { step: 'r2-missing-identity', expectedOrdinal: 0 },
      } as FleetAuditProgress,
    });
    const afterGlobalStage = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token: started.token }),
    );
    expect(afterGlobalStage.status).toBe('pending');
    if (afterGlobalStage.status !== 'pending') throw new Error('unreachable');
    expect(afterGlobalStage.stage).toEqual({
      step: 'per-record',
      recordOrdinal: 0,
    });

    const recordPageReadsBefore =
      harness.operationStore.rowPageReadCounts.get('record') ?? 0;
    const afterFirstRecord = await advanceFleetAudit(
      harness.baseOptions({
        kind: 'continue',
        token: afterGlobalStage.token,
      }),
    );
    expect(afterFirstRecord.status).toBe('pending');
    if (afterFirstRecord.status !== 'pending') throw new Error('unreachable');
    expect(afterFirstRecord.stage).toEqual({
      step: 'per-record',
      recordOrdinal: 1,
    });
    expect(
      (harness.operationStore.rowPageReadCounts.get('record') ?? 0) -
        recordPageReadsBefore,
    ).toBe(2);
    expect(
      (await harness.operationStore.readOperationById(operationId))?.state,
    ).toBe('running');
  });

  it('a persisted global-stage cursor beyond its source length or a per-record cursor beyond the record count refuses as malformed with no provider work and no durable mutation instead of truncating the audit', async () => {
    const globalInventory = emptyInventory();
    globalInventory.findings.push({
      tenantTag: 'cursor-global',
      environment: ENVIRONMENT,
      kind: 'malformed-route',
      detail: 'cursor fixture',
    });
    const globalCase = {
      records: [] as readonly FleetRecord[],
      inventory: globalInventory,
      stage: { step: 'provider-findings', rowOrdinal: 2 } as const,
    };
    const equalGlobalCase = {
      records: [] as readonly FleetRecord[],
      inventory: globalInventory,
      stage: { step: 'provider-findings', rowOrdinal: 1 } as const,
    };
    const record = baseRecord('cursorrecord');
    const perRecordCase = {
      records: [record] as readonly FleetRecord[],
      inventory: inventoryFor([record]),
      stage: { step: 'per-record', recordOrdinal: 2 } as const,
    };

    for (const [index, testCase] of [
      globalCase,
      equalGlobalCase,
      perRecordCase,
    ].entries()) {
      const harness = buildHarness(testCase.records, testCase.inventory);
      const operationId = uuidFor(84 + index);
      const started = await advanceFleetAudit(
        harness.baseOptions({
          kind: 'start',
          operationId,
          records: testCase.records,
          staleAfterMs: STALE_AFTER_MS,
        }),
      );
      expect(started.status).toBe('pending');
      if (started.status !== 'pending') throw new Error('unreachable');
      const persisted =
        await harness.operationStore.readOperationById(operationId);
      expect(persisted).toBeDefined();
      if (!persisted) throw new Error('unreachable');
      const progress = fleetAuditProgressFromUnknown(persisted.progress);
      // A DELIBERATE raw cast: this line CORRUPTS the persisted cursor on
      // purpose, so it must bypass the codec the read above went through.
      // Every read in this file goes the other way.
      const corrupted: FleetOperationRunRecord = {
        ...persisted,
        progress: { ...progress, stage: testCase.stage } as FleetAuditProgress,
      };
      harness.operationStore.operations.set(operationId, corrupted);
      const rowsBefore = structuredClone([...harness.operationStore.rows]);

      await expect(
        advanceFleetAudit(
          harness.baseOptions({ kind: 'continue', token: started.token }),
        ),
      ).rejects.toThrow('fleet operation state is malformed');
      expect(harness.opsLog).toEqual([]);
      expect(harness.fleetStore.ops).toEqual([]);
      expect([...harness.operationStore.rows]).toEqual(rowsBefore);
      const afterRefusal =
        await harness.operationStore.readOperationById(operationId);
      expect(afterRefusal).toEqual(corrupted);
      expect(harness.operationStore.heads.get('audit')).toBe(operationId);
      // Read back off the STORE, not off the local fixture: the claim is that
      // the refusal left the durable operation running, which a property of
      // `corrupted` could never falsify.
      expect(afterRefusal?.state).toBe('running');
    }
  });

  it('the per-record stage visits every staged record exactly once, in staged ordinal order, including a record that emits no finding', async () => {
    const bob = baseRecord('bob');
    const carol = baseRecord('carol');
    const alice = baseRecord('alice');
    const records = [bob, carol, alice];
    const auditTime = AUDIT_NOW + STALE_AFTER_MS + 60_001;
    const harness = buildHarness(records, inventoryFor(records), {
      auditClock: () => auditTime,
    });
    harness.liveByTenant.set(
      carol.tenantTag,
      cleanLiveDeployment(carol, {
        maintenance: {
          ...HEALTHY_MAINTENANCE,
          lastSweepAt: auditTime,
          lastPurgeAt: auditTime,
        },
      }),
    );
    const operationId = uuidFor(94);
    const visited: string[] = [];
    // `driveToTerminal` and `startAndDrive` call `baseOptions` themselves on
    // every iteration, so the instrumentation has to sit on the harness rather
    // than on one options object.
    const instrumented: Harness = {
      ...harness,
      baseOptions: (action) => {
        const options = harness.baseOptions(action);
        return {
          ...options,
          specFor: (record) => {
            visited.push(record.tenantTag);
            return options.specFor(record);
          },
        };
      },
    };

    const result = await startAndDrive(instrumented, operationId, records);
    expect(result.status).toBe('complete');
    const stagedTags = (
      harness.operationStore.rows.get(`${operationId}:record`) ?? []
    )
      .slice()
      .sort((left, right) => left.ordinal - right.ordinal)
      .map((row) => row.payload.tenantTag);
    expect(stagedTags).toEqual(['bob', 'carol', 'alice']);
    expect(visited).toEqual(stagedTags);
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 10,
    });
    expect(page.done).toBe(true);
    expect(
      page.findings.map((finding) => [finding.tenantTag, finding.kind]),
    ).toEqual([
      ['bob', 'maintenance-stale'],
      ['alice', 'maintenance-stale'],
    ]);
  });
});
