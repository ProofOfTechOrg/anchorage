// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { auditFleetDrift } from '../src/fleet.js';
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
import type {
  FleetInventoryGeneration,
  FleetInventoryGenerationRef,
  FleetInventoryLease,
  FleetInventoryRowKind,
  FleetInventoryRunOptions,
  FleetInventoryRunRecord,
  FleetInventoryRunStore,
  FleetInventoryStagedFact,
  FleetInventoryStagedRow,
} from '../src/fleet-inventory-state.js';
import { emptyFleetInventoryRowCounts } from '../src/fleet-inventory-state.js';
import {
  canonicalFleetOperationBytes,
  FLEET_OPERATION_INTAKE_BYTE_BOUND,
  FLEET_OPERATION_ITEM_BOUND,
  FLEET_OPERATION_RECORD_ROW_BYTE_BOUND,
  FLEET_OPERATION_ROW_PAYLOAD_BYTE_BOUND,
  FLEET_OPERATION_ROW_READ_BOUND,
  FLEET_OPERATION_STRING_BYTE_BOUND,
  type FleetOperationKind,
  type FleetOperationLease,
  type FleetOperationRowKind,
  type FleetOperationRunRecord,
  type FleetOperationStagedRow,
  type FleetOperationStore,
  FleetOperationTokenFutureError,
  FleetOperationTokenKindError,
  FleetOperationTokenOperationError,
  fleetOperationIntakeDigest,
  fleetOperationItemsIntake,
  fleetOperationStagedRowFromUnknown,
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

// ---------------------------------------------------------------------------
// Fixed identities, clocks, and small builders. This world is INLINE and
// INDEPENDENT of `test/fixtures/fleet-audit-world.ts` (§10 SECOND-WORLD NOTE):
// it never imports that fixture.
// ---------------------------------------------------------------------------

function uuidFor(seed: number): string {
  return `${seed.toString(16).padStart(8, '0')}-0000-4000-8000-000000000000`;
}

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

const ENVIRONMENT = 'production';
const SPEC_DIGEST = 'a'.repeat(64);
const AUDIT_NOW = Date.parse('2026-06-01T00:00:00.000Z');
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

function countPlainDataNodes(value: unknown): number {
  let count = 0;
  const pending = [value];
  while (pending.length > 0) {
    const current = pending.pop();
    count += 1;
    if (Array.isArray(current)) pending.push(...current);
    else if (current && typeof current === 'object') {
      pending.push(...Object.values(current));
    }
  }
  return count;
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
// Fake FleetInventoryRunStore: registers a finalized generation directly from
// a FleetResourceInventory, going through the real materialization codec.
// ---------------------------------------------------------------------------

function stageInventoryFixture(inventory: FleetResourceInventory): {
  rows: FleetInventoryStagedRow[];
  facts: FleetInventoryStagedFact[];
  options: FleetInventoryRunOptions;
} {
  const rows: FleetInventoryStagedRow[] = [];
  const facts: FleetInventoryStagedFact[] = [];
  let ordinal = 0;
  for (const finding of inventory.findings) {
    rows.push({
      kind: 'finding',
      ordinal: ordinal++,
      payload: { record: 'finding', ...finding },
    });
  }
  ordinal = 0;
  for (const registration of inventory.scriptRegistrations) {
    rows.push({
      kind: 'registration',
      ordinal: ordinal++,
      payload: { record: 'registration', ...registration },
    });
  }
  ordinal = 0;
  for (const deployment of inventory.deployments) {
    const deploymentOrdinal = ordinal++;
    const {
      databaseIds,
      durableObjectBindings,
      serviceBindings,
      queueProducerBindings,
      kvNamespaceBindings,
      r2BucketBindings,
      secretNames,
      plainTextBindings,
      routeHostnames,
      zoneRoutes,
      ...identity
    } = deployment as FleetInventoryDeployment & {
      kvNamespaceBindings?: readonly Readonly<{
        name: string;
        namespaceId: string;
      }>[];
      zoneRoutes?: readonly Readonly<{ zoneId: string; routeId: string }>[];
    };
    rows.push({
      kind: 'deployment',
      ordinal: deploymentOrdinal,
      payload: { record: 'deployment', ...identity },
    });
    let factOrdinal = 0;
    for (const databaseId of databaseIds ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'database-id',
        factOrdinal: factOrdinal++,
        payload: { databaseId },
      });
    }
    for (const binding of durableObjectBindings ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'durable-object-binding',
        factOrdinal: factOrdinal++,
        payload: { ...binding },
      });
    }
    for (const binding of serviceBindings ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'service-binding',
        factOrdinal: factOrdinal++,
        payload: { ...binding },
      });
    }
    for (const binding of queueProducerBindings ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'queue-producer-binding',
        factOrdinal: factOrdinal++,
        payload: { ...binding },
      });
    }
    for (const binding of kvNamespaceBindings ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'kv-binding',
        factOrdinal: factOrdinal++,
        payload: { ...binding },
      });
    }
    for (const binding of r2BucketBindings ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'r2-binding',
        factOrdinal: factOrdinal++,
        payload: { ...binding },
      });
    }
    for (const secretName of secretNames ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'secret-name',
        factOrdinal: factOrdinal++,
        payload: { secretName },
      });
    }
    for (const [name, text] of Object.entries(plainTextBindings ?? {})) {
      facts.push({
        deploymentOrdinal,
        factKind: 'plain-text-binding',
        factOrdinal: factOrdinal++,
        payload: { name, text },
      });
    }
    for (const hostname of routeHostnames ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'route-hostname',
        factOrdinal: factOrdinal++,
        payload: { hostname },
      });
    }
    for (const zoneRoute of zoneRoutes ?? []) {
      facts.push({
        deploymentOrdinal,
        factKind: 'zone-route',
        factOrdinal: factOrdinal++,
        payload: { ...zoneRoute },
      });
    }
  }
  ordinal = 0;
  for (const databaseId of inventory.databaseIds) {
    rows.push({
      kind: 'database-id',
      ordinal: ordinal++,
      payload: { record: 'database-id', databaseId },
    });
  }
  ordinal = 0;
  for (const namespaceId of inventory.namespaceIds) {
    rows.push({
      kind: 'namespace-id',
      ordinal: ordinal++,
      payload: { record: 'namespace-id', namespaceId },
    });
  }
  ordinal = 0;
  for (const bucket of inventory.r2Buckets ?? []) {
    rows.push({
      kind: 'r2-bucket',
      ordinal: ordinal++,
      payload: { record: 'r2-bucket', ...bucket },
    });
  }
  ordinal = 0;
  for (const route of inventory.routes) {
    rows.push({
      kind: 'route',
      ordinal: ordinal++,
      payload: { record: 'route', ...route },
    });
  }
  const options: FleetInventoryRunOptions = {
    databaseNamePrefix: 'fleet-',
    scriptNamePrefix: 'fleet-',
    includeDispatchNamespace: false,
    includeR2Buckets: true,
    ...(inventory.hostRoutingKvId === undefined
      ? {}
      : { hostRoutingKvId: inventory.hostRoutingKvId }),
  };
  return { rows, facts, options };
}

class FakeInventoryRunStore implements FleetInventoryRunStore {
  readonly refs = new Map<number, FleetInventoryGenerationRef>();
  readonly generations = new Map<
    number,
    { rows: FleetInventoryStagedRow[]; facts: FleetInventoryStagedFact[] }
  >();
  readonly runs = new Map<string, FleetInventoryRunRecord>();
  readonly pins: { generation: number; pinnedBy: string }[] = [];
  readonly releasedPins: { generation: number; pinnedBy: string }[] = [];
  latestGeneration: number | undefined;
  latestFinalizedGenerationCalls = 0;
  readFinalizedGenerationCalls = 0;
  readRunByOperationCalls = 0;
  unreadableGenerations = new Set<number>();
  pinFailsForGeneration: number | undefined;

  registerFinalizedGeneration(
    generation: number,
    inventory: FleetResourceInventory,
  ): void {
    const { rows, facts, options } = stageInventoryFixture(inventory);
    const rowManifest: Record<FleetInventoryRowKind, number> = {
      ...emptyFleetInventoryRowCounts(),
    };
    for (const row of rows) {
      rowManifest[row.kind] = (rowManifest[row.kind] ?? 0) + 1;
    }
    const operationId = uuidFor(900_000 + generation);
    const ref: FleetInventoryGenerationRef = {
      generation,
      operationId,
      finalizedAtMs: AUDIT_NOW,
      rowManifest,
      factCount: facts.length,
    };
    this.refs.set(generation, ref);
    this.generations.set(generation, { rows, facts });
    this.runs.set(operationId, {
      version: 1,
      operationId,
      optionsDigest: `digest-${generation}`,
      options,
      state: 'finalized',
      progress: {
        stage: { step: 'finalize' },
        generation,
        revision: 1,
        stagedCounts: rowManifest,
        factCount: facts.length,
        providerRequests: 0,
      },
      updatedAt: new Date(AUDIT_NOW).toISOString(),
    });
    this.latestGeneration = generation;
  }

  async withAccountInventoryLease<T>(
    operation: (lease: FleetInventoryLease) => Promise<T>,
  ): Promise<T> {
    // Unused by the audit coordinator (pinGeneration/releasePin are
    // store-level, not lease-level); a throwing stub is sufficient.
    return operation({
      assertOwned: () => Promise.reject(new Error('unused')),
    } as unknown as FleetInventoryLease);
  }

  async readFinalizedGeneration(
    generation: number,
  ): Promise<FleetInventoryGeneration> {
    this.readFinalizedGenerationCalls += 1;
    if (this.unreadableGenerations.has(generation)) {
      throw new Error(
        `fleet inventory generation ${generation} is not finalized`,
      );
    }
    const ref = this.refs.get(generation);
    const stored = this.generations.get(generation);
    if (!ref || !stored) {
      throw new Error(
        `fleet inventory generation ${generation} is not finalized`,
      );
    }
    return { ref, rows: stored.rows, facts: stored.facts };
  }

  async latestFinalizedGeneration(): Promise<
    FleetInventoryGenerationRef | undefined
  > {
    this.latestFinalizedGenerationCalls += 1;
    return this.latestGeneration === undefined
      ? undefined
      : this.refs.get(this.latestGeneration);
  }

  async readRunByOperation(
    operationId: string,
  ): Promise<FleetInventoryRunRecord | undefined> {
    this.readRunByOperationCalls += 1;
    return this.runs.get(operationId);
  }

  async pinGeneration(
    input: Readonly<{ generation: number; pinnedBy: string }>,
  ): Promise<void> {
    if (this.pinFailsForGeneration === input.generation) {
      throw new Error(
        `fleet inventory generation ${input.generation} cannot be pinned`,
      );
    }
    this.pins.push({ ...input });
  }

  async releasePin(
    input: Readonly<{ generation: number; pinnedBy: string }>,
  ): Promise<void> {
    this.releasedPins.push({ ...input });
  }

  async pruneInventoryGenerations(): Promise<Readonly<{ deleted: number }>> {
    return { deleted: 0 };
  }
}

// ---------------------------------------------------------------------------
// Fake FleetOperationStore/FleetOperationLease: an in-memory, deliberately
// faithful reimplementation of the R4-A guarded-batch contract (head/lease
// exclusivity, DO-NOTHING staging, revision-guarded commit with a
// byte-identical convergence read, watermark verification, probe-first
// start classification). `lease.readOperation` is deliberately HEAD-SCOPED
// (stricter than the shipped D1 adapter) so the coordinator's probe-first
// fallback to the head-independent `readOperationById` is genuinely
// exercised, not merely accepted by coincidence.
// ---------------------------------------------------------------------------

class FakeOperationStore implements FleetOperationStore {
  readonly heads = new Map<FleetOperationKind, string>();
  readonly operations = new Map<string, FleetOperationRunRecord>();
  readonly intakeDigests = new Map<string, string>();
  readonly rows = new Map<string, FleetOperationStagedRow[]>();
  readonly locked = new Set<FleetOperationKind>();
  readonly probeMiss = new Set<string>();
  loseLeaseKind: FleetOperationKind | undefined;
  leaseCount = 0;
  readOperationByIdCalls = 0;
  readonly rowPageReadCounts = new Map<FleetOperationRowKind, number>();
  stagedRowCodecCalls = 0;

  #rowsKey(operationId: string, rowKind: FleetOperationRowKind): string {
    return `${operationId}:${rowKind}`;
  }

  async withAccountOperationLease<T>(
    kind: FleetOperationKind,
    operation: (lease: FleetOperationLease) => Promise<T>,
  ): Promise<T> {
    if (this.locked.has(kind)) {
      throw new Error(
        `fleet ${kind} operations for account 'test' are already being modified`,
      );
    }
    this.locked.add(kind);
    this.leaseCount += 1;
    const lost = this.loseLeaseKind === kind;
    const lease: FleetOperationLease = {
      assertOwned: async () => {
        if (lost) {
          throw new Error(
            `fleet ${kind} operation lease for account 'test' is no longer owned by this operation`,
          );
        }
      },
      startOperation: async (input) => this.#startOperation(input),
      readOperation: async (operationId) => {
        const op = this.operations.get(operationId);
        if (!op) return undefined;
        return this.heads.get(op.kind) === operationId ? op : undefined;
      },
      stageRows: async (input) => this.#stageRows(input),
      commitProgress: async (input) => this.#commitProgress(input),
      finalizeOperation: async (input) => this.#finalizeOperation(input),
      failOperation: async (input) => this.#failOperation(input),
    };
    try {
      return await operation(lease);
    } finally {
      this.locked.delete(kind);
    }
  }

  async readOperationById(
    operationId: string,
  ): Promise<FleetOperationRunRecord | undefined> {
    this.readOperationByIdCalls += 1;
    if (this.probeMiss.has(operationId)) {
      this.probeMiss.delete(operationId);
      return undefined;
    }
    return this.operations.get(operationId);
  }

  async readOperationRowsPage(
    input: Readonly<{
      operationId: string;
      rowKind: FleetOperationRowKind;
      afterOrdinal?: number;
      limit: number;
    }>,
  ): Promise<
    Readonly<{ rows: readonly FleetOperationStagedRow[]; done: boolean }>
  > {
    this.rowPageReadCounts.set(
      input.rowKind,
      (this.rowPageReadCounts.get(input.rowKind) ?? 0) + 1,
    );
    const key = this.#rowsKey(input.operationId, input.rowKind);
    const all = [...(this.rows.get(key) ?? [])].sort(
      (left, right) => left.ordinal - right.ordinal,
    );
    const after = input.afterOrdinal ?? -1;
    const filtered = all.filter((row) => row.ordinal > after);
    const page = filtered.slice(0, input.limit);
    return { rows: page, done: filtered.length <= input.limit };
  }

  async pruneFleetOperations(): Promise<
    Readonly<{ deleted: number; releasedPins: number }>
  > {
    return { deleted: 0, releasedPins: 0 };
  }

  #startOperation(
    input: Parameters<FleetOperationLease['startOperation']>[0],
  ): ReturnType<FleetOperationLease['startOperation']> {
    const { operationId, kind, runRecord, intakeDigest } = input;
    const existing = this.operations.get(operationId);
    if (existing) {
      if (existing.kind !== kind) {
        throw new Error(
          `fleet operation '${operationId}' belongs to the other operation kind`,
        );
      }
      if (this.intakeDigests.get(operationId) !== intakeDigest) {
        throw new Error(
          `fleet operation '${operationId}' already exists with a different intake`,
        );
      }
      return Promise.resolve({
        outcome:
          existing.state === 'running'
            ? ('adopted-running' as const)
            : ('adopted-terminal' as const),
        record: existing,
      });
    }
    if (this.heads.has(kind)) {
      throw new Error(
        `another fleet ${kind} operation is active for this account`,
      );
    }
    this.operations.set(operationId, runRecord);
    this.intakeDigests.set(operationId, intakeDigest);
    this.heads.set(kind, operationId);
    return Promise.resolve({ outcome: 'created' as const, record: runRecord });
  }

  #stageRows(input: Parameters<FleetOperationLease['stageRows']>[0]): void {
    const { operationId } = input;
    const rows = this.#validatedRows(input.rows);
    for (const row of rows) {
      const key = this.#rowsKey(operationId, row.rowKind);
      const list = this.rows.get(key) ?? [];
      if (!list.some((existing) => existing.ordinal === row.ordinal)) {
        list.push(row);
        this.rows.set(key, list);
      }
    }
  }

  #commitProgress(
    input: Parameters<FleetOperationLease['commitProgress']>[0],
  ): ReturnType<FleetOperationLease['commitProgress']> {
    const {
      operationId,
      expectedRevision,
      runRecord,
      rows: inputRows = [],
      updateRows: inputUpdateRows = [],
      expectedRowWatermarks = {},
    } = input;
    const rows = this.#validatedRows(inputRows);
    const updateRows = this.#validatedRows(inputUpdateRows);
    if (rows.length + updateRows.length + 1 > 100) {
      throw new Error(
        'commitProgress exceeds the operation batch budget of 100 statements',
      );
    }
    const current = this.operations.get(operationId);
    const matches =
      current !== undefined &&
      current.state === 'running' &&
      current.progress.revision === expectedRevision;
    if (matches) {
      for (const row of rows) {
        const key = this.#rowsKey(operationId, row.rowKind);
        const list = this.rows.get(key) ?? [];
        if (!list.some((existing) => existing.ordinal === row.ordinal)) {
          list.push(row);
          this.rows.set(key, list);
        }
      }
      for (const row of updateRows) {
        const key = this.#rowsKey(operationId, row.rowKind);
        const list = this.rows.get(key) ?? [];
        const index = list.findIndex(
          (existing) => existing.ordinal === row.ordinal,
        );
        if (index >= 0) list[index] = row;
      }
      this.operations.set(operationId, runRecord);
      return Promise.resolve(runRecord);
    }
    let complete = true;
    for (const row of [...rows, ...updateRows]) {
      const key = this.#rowsKey(operationId, row.rowKind);
      const list = this.rows.get(key) ?? [];
      const stored = list.find((existing) => existing.ordinal === row.ordinal);
      if (!stored) complete = false;
      else if (JSON.stringify(stored.payload) !== JSON.stringify(row.payload)) {
        throw new Error(
          `fleet operation '${operationId}' staged rows diverge from the persisted operation`,
        );
      }
    }
    for (const [rowKind, watermark] of Object.entries(expectedRowWatermarks)) {
      const list =
        this.rows.get(
          this.#rowsKey(operationId, rowKind as FleetOperationRowKind),
        ) ?? [];
      const count = list.filter(
        (row) => row.ordinal < (watermark as number),
      ).length;
      if (count !== watermark) {
        throw new Error(
          `fleet operation '${operationId}' is no longer at the expected revision`,
        );
      }
    }
    const persisted = this.operations.get(operationId);
    if (
      complete &&
      persisted &&
      persisted.progress.revision === runRecord.progress.revision &&
      JSON.stringify(persisted) === JSON.stringify(runRecord)
    ) {
      return Promise.resolve(persisted);
    }
    throw new Error(
      `fleet operation '${operationId}' is no longer at the expected revision`,
    );
  }

  #validatedRows(
    rows: readonly FleetOperationStagedRow[],
  ): FleetOperationStagedRow[] {
    return rows.map((row) => {
      this.stagedRowCodecCalls += 1;
      return fleetOperationStagedRowFromUnknown(row);
    });
  }

  #finalizeOperation(
    input: Parameters<FleetOperationLease['finalizeOperation']>[0],
  ): ReturnType<FleetOperationLease['finalizeOperation']> {
    const {
      operationId,
      expectedRevision,
      runRecord,
      expectedRowCounts,
      requireAllItemsComplete,
    } = input;
    const current = this.operations.get(operationId);
    if (
      current?.state !== 'running' ||
      current.progress.revision !== expectedRevision
    ) {
      throw new Error(
        `fleet operation '${operationId}' is no longer at the expected revision`,
      );
    }
    for (const [rowKind, count] of Object.entries(expectedRowCounts)) {
      const list =
        this.rows.get(
          this.#rowsKey(operationId, rowKind as FleetOperationRowKind),
        ) ?? [];
      if (list.length !== count) {
        throw new Error(
          `fleet operation '${operationId}' does not match its finalize counts`,
        );
      }
    }
    if (requireAllItemsComplete) {
      const items = this.rows.get(this.#rowsKey(operationId, 'item')) ?? [];
      const complete = items.filter(
        (row) =>
          (row.payload as Readonly<{ status?: string }>).status === 'complete',
      ).length;
      const itemCount = (runRecord.progress as Readonly<{ itemCount?: number }>)
        .itemCount;
      if (complete !== itemCount) {
        throw new Error(
          `fleet operation '${operationId}' does not match its finalize counts`,
        );
      }
    }
    const finalized: FleetOperationRunRecord = {
      ...runRecord,
      terminalAtMs: Date.now(),
    };
    this.operations.set(operationId, finalized);
    if (this.heads.get(current.kind) === operationId) {
      this.heads.delete(current.kind);
    }
    return Promise.resolve(finalized);
  }

  #failOperation(
    input: Parameters<FleetOperationLease['failOperation']>[0],
  ): Promise<void> {
    const { operationId, expectedRevision, runRecord, updateRows = [] } = input;
    const current = this.operations.get(operationId);
    if (
      current?.state !== 'running' ||
      current.progress.revision !== expectedRevision
    ) {
      throw new Error(
        `fleet operation '${operationId}' is no longer at the expected revision`,
      );
    }
    for (const row of updateRows) {
      const key = this.#rowsKey(operationId, row.rowKind);
      const list = this.rows.get(key) ?? [];
      const index = list.findIndex(
        (existing) => existing.ordinal === row.ordinal,
      );
      if (index >= 0) list[index] = row;
    }
    const failed: FleetOperationRunRecord = {
      ...runRecord,
      terminalAtMs: Date.now(),
    };
    this.operations.set(operationId, failed);
    if (this.heads.get(current.kind) === operationId) {
      this.heads.delete(current.kind);
    }
    return Promise.resolve();
  }
}

// ---------------------------------------------------------------------------
// Shared drive helpers.
// ---------------------------------------------------------------------------

interface Harness {
  readonly operationStore: FakeOperationStore;
  readonly inventoryStore: FakeInventoryRunStore;
  readonly fleetStore: FakeFleetStateStore;
  readonly backend: SimpleBackend;
  readonly opsLog: string[];
  readonly liveByTenant: Map<string, LiveDeployment | undefined>;
  readonly specByTenant: Map<string, DeploymentSpec>;
  readonly secretByTenant: Map<string, string>;
  baseOptions(action: FleetAuditAdvanceAction): AdvanceFleetAuditOptions;
}

function generationReadCounts(store: FakeInventoryRunStore): Readonly<{
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
  const inventoryStore = new FakeInventoryRunStore();
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

/** Drives `advanceFleetAudit` with `continue` until the status is not 'pending'. */
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
  throw new Error('driveToTerminal exceeded its iteration cap');
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
    const originalStart = harness.operationStore.withAccountOperationLease.bind(
      harness.operationStore,
    );
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
    void originalStart;
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
    const progress = persisted?.progress as FleetAuditProgress;
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
    expect(started.status).toBe('pending');
    // Real time moves far past staleAfterMs before the per-record stage runs.
    clock = AUDIT_NOW + 10 * STALE_AFTER_MS;
    const result = await driveToTerminal(
      harness,
      started.status === 'pending' ? started.token : undefined,
    );
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
    const rowCountBefore = harness.operationStore.rows.get(
      `${operationId}:record`,
    )?.length;
    const second = await advanceFleetAudit(harness.baseOptions(action));
    expect(second.status).toBe('pending');
    if (first.status === 'pending' && second.status === 'pending') {
      expect(second.token).toEqual(first.token);
      expect(second.stage).toEqual(first.stage);
    }
    expect(harness.inventoryStore.latestFinalizedGenerationCalls).toBe(1);
    expect(
      harness.operationStore.rows.get(`${operationId}:record`)?.length,
    ).toBe(rowCountBefore);
    expect(
      harness.inventoryStore.pins.filter(
        (pin) => pin.pinnedBy === `fleet-audit:${operationId}`,
      ).length,
    ).toBe(2);

    if (second.status !== 'pending') throw new Error('unreachable');
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
    const generation1 = (persisted1?.progress as FleetAuditProgress).generation;
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
    expect((persisted2?.progress as FleetAuditProgress).generation).toBe(1);
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
    await expect(
      advanceFleetAudit(
        harness.baseOptions({
          kind: 'start',
          operationId: uuidFor(17),
          records: [alice],
          staleAfterMs: 0,
        }),
      ),
    ).rejects.toThrow('staleAfterMs must be a positive safe integer');
  });

  it('item-bound refusal at 10,001', async () => {
    const many = Array.from({ length: 10_001 }, (_, i) =>
      baseRecord(`tenant${i}`),
    );
    const harness = buildHarness([], emptyInventory());
    await expect(
      advanceFleetAudit(
        harness.baseOptions({
          kind: 'start',
          operationId: uuidFor(18),
          records: many,
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toThrow(/at most 10000 records/);
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
    await expect(
      advanceFleetAudit(
        harness.baseOptions({
          kind: 'start',
          operationId: uuidFor(19),
          records: many,
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toThrow(
      'fleet audit start canonical intake exceeds the intake byte bound',
    );
  });

  it("'operationId' validation refusal at start", async () => {
    const alice = baseRecord('alice');
    const harness = buildHarness([alice], inventoryFor([alice]));
    await expect(
      advanceFleetAudit(
        harness.baseOptions({
          kind: 'start',
          operationId: 'not-a-uuid',
          records: [alice],
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toThrow('fleet operation state is malformed');
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
    let token = started.token;
    let stage: FleetAuditStage = started.stage;
    while (stage.step !== 'per-record') {
      const result = await advanceFleetAudit(
        harness.baseOptions({ kind: 'continue', token }),
      );
      if (result.status !== 'pending') throw new Error('unexpected terminal');
      token = result.token;
      stage = result.stage;
    }
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
    let token = started.token;
    let stage = started.stage;
    while (stage.step !== 'per-record') {
      const advanced = await advanceFleetAudit(
        harness.baseOptions({ kind: 'continue', token }),
      );
      expect(authorityClockCalls).toBe(0);
      if (advanced.status !== 'pending') throw new Error('unexpected terminal');
      token = advanced.token;
      stage = advanced.stage;
    }
    const staleRecord = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token }),
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
    const drainFindings = await auditFleetDrift({
      store: new FakeFleetStateStore([first, second]),
      records: [first, second],
      inventory,
      backendFor: () => harness.backend,
      specFor: (record) =>
        harness.specByTenant.get(record.tenantTag) as DeploymentSpec,
      maintenanceSecretFor: (record) =>
        harness.secretByTenant.get(record.tenantTag) as string,
      staleAfterMs: STALE_AFTER_MS,
      now: AUDIT_NOW,
    });
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
    expect(started.status).toBe('pending');
    harness.inventoryStore.unreadableGenerations.add(1);
    const result = await driveToTerminal(
      harness,
      started.status === 'pending' ? started.token : undefined,
    );
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

  it('abandonFleetAuditOperation: running → operator-abandoned + pin released; terminal → releases any surviving pin', async () => {
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
    expect((abandoned?.progress as FleetAuditProgress).failure?.reason).toBe(
      'operator-abandoned',
    );
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
    expect(started.status).toBe('pending');
    harness.inventoryStore.unreadableGenerations.add(1);
    const failed = await driveToTerminal(
      harness,
      started.status === 'pending' ? started.token : undefined,
    );
    expect(failed.status).toBe('failed');
    if (failed.status !== 'failed') throw new Error('unreachable');
    const opsBefore = harness.opsLog.length;
    const again = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token: failed.token }),
    );
    expect(again.status).toBe('failed');
    expect(harness.opsLog.length).toBe(opsBefore);
  });

  it('findings page: running refusal; failed operation readable; no inventory-store interaction', async () => {
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
    expect(started.status).toBe('pending');
    await expect(
      readFleetAuditFindingsPage(harness.operationStore, {
        operationId,
        limit: 10,
      }),
    ).rejects.toThrow(`fleet audit operation '${operationId}' is not terminal`);

    harness.inventoryStore.unreadableGenerations.add(1);
    const failed = await driveToTerminal(
      harness,
      started.status === 'pending' ? started.token : undefined,
    );
    expect(failed.status).toBe('failed');
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 10,
    });
    expect(page.done).toBe(true);

    // FINDINGS PAGE ORDER (round 9): the port lets a page arrive in any
    // order; the reader still returns the drain's (ordinal) order, and the
    // next-cursor idiom pages the whole set through such a store.
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
    const orderedHarness = buildHarness(orderedRecords, orderedInventory);
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
    expect(ascending.findings.length).toBeGreaterThanOrEqual(3);
    const reversedStore = new Proxy(orderedHarness.operationStore, {
      get(target, property, receiver) {
        if (property === 'readOperationRowsPage') {
          return async (
            input: Parameters<FleetOperationStore['readOperationRowsPage']>[0],
          ) => {
            const rowsPage = await target.readOperationRowsPage(input);
            return { ...rowsPage, rows: [...rowsPage.rows].reverse() };
          };
        }
        const value = Reflect.get(target, property, receiver);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    await expect(
      readFleetAuditFindingsPage(reversedStore, {
        operationId: orderedOperationId,
        limit: 1_000,
      }),
    ).resolves.toEqual(ascending);
    const paged: (typeof ascending.findings)[number][] = [];
    let afterOrdinal: number | undefined;
    for (;;) {
      const next = await readFleetAuditFindingsPage(reversedStore, {
        operationId: orderedOperationId,
        limit: 2,
        ...(afterOrdinal === undefined ? {} : { afterOrdinal }),
      });
      paged.push(...next.findings);
      if (next.done) break;
      afterOrdinal = (afterOrdinal ?? -1) + next.findings.length;
    }
    expect(paged).toEqual(ascending.findings);
  });

  it('second-world drain-vs-bounded equivalence modulo the §5.5 difference set', async () => {
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

    // §5.5 EQUIVALENCE SCOPE: both paths run over the SAME frozen clock.
    const harness = buildHarness(records, inventory, {
      auditClock: () => AUDIT_NOW,
      authorityClock: () => AUDIT_NOW,
    });

    const drainFindings = await auditFleetDrift({
      store: new FakeFleetStateStore(records) as unknown as FleetStateStore,
      records,
      inventory,
      backendFor: () => harness.backend,
      specFor: (record) =>
        harness.specByTenant.get(record.tenantTag) as DeploymentSpec,
      maintenanceSecretFor: (record) =>
        harness.secretByTenant.get(record.tenantTag) as string,
      staleAfterMs: STALE_AFTER_MS,
      now: AUDIT_NOW,
    });

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
    const drainFindings = await auditFleetDrift({
      store: new FakeFleetStateStore(records) as unknown as FleetStateStore,
      records,
      inventory,
      backendFor: () => harness.backend,
      specFor: (record) =>
        harness.specByTenant.get(record.tenantTag) as DeploymentSpec,
      maintenanceSecretFor: (record) =>
        harness.secretByTenant.get(record.tenantTag) as string,
      staleAfterMs: STALE_AFTER_MS,
      now: AUDIT_NOW,
    });
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
    expect(started.status).toBe('pending');
    let token = started.status === 'pending' ? started.token : undefined;
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
    const action: FleetAuditAdvanceAction = {
      kind: 'start',
      operationId,
      records: [alice],
      staleAfterMs: STALE_AFTER_MS,
    };
    const first = await advanceFleetAudit(harness.baseOptions(action));
    expect(first.status).toBe('pending');
    // Replay the exact same start call: its revision-1 commit has already
    // landed, so the replay must converge via the byte-identical read
    // rather than throwing a conflict.
    const second = await advanceFleetAudit(harness.baseOptions(action));
    expect(second.status).toBe('pending');
    if (first.status === 'pending' && second.status === 'pending') {
      expect(second.token).toEqual(first.token);
    }
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

    const abortedController = new AbortController();
    abortedController.abort();
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
    ).rejects.toThrow();
    expectZeroHarnessWork(abortedHarness);
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
    const operationId = uuidFor(40);
    await startAndDrive(harness, operationId, records);
    const expectNoSensitiveBytes = (value: unknown): void => {
      const text = JSON.stringify(value).toLowerCase();
      expect(text).not.toContain('bearer');
      expect(text).not.toContain('authorization');
      expect(text).not.toContain('super-secret-credential-value');
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
    const harness = buildHarness(records, inventory);
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

    const drainFindings = await auditFleetDrift({
      store: new FakeFleetStateStore(records) as unknown as FleetStateStore,
      records,
      inventory,
      backendFor: () => harness.backend,
      specFor: (record) =>
        harness.specByTenant.get(record.tenantTag) as DeploymentSpec,
      maintenanceSecretFor: (record) =>
        harness.secretByTenant.get(record.tenantTag) as string,
      staleAfterMs: STALE_AFTER_MS,
      now: AUDIT_NOW,
    });
    const drainOrphan = drainFindings.find(
      (finding) =>
        finding.kind === 'orphan-deployment' &&
        finding.tenantTag === 'ghost-hostile-owner',
    );
    expect(drainOrphan?.detail).toContain(hostileScriptName);
  });

  it('concurrent-mutation drift (class (c), both halves)', async () => {
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
    const startTimeDrainFindings = await auditFleetDrift({
      store: new FakeFleetStateStore(records) as unknown as FleetStateStore,
      records,
      inventory,
      backendFor: () => harness.backend,
      specFor: (record) =>
        harness.specByTenant.get(record.tenantTag) as DeploymentSpec,
      maintenanceSecretFor: (record) =>
        harness.secretByTenant.get(record.tenantTag) as string,
      staleAfterMs: STALE_AFTER_MS,
      now: AUDIT_NOW,
    });
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

  it('maxItemsPerCall range refusal (0 and 2,001 refused; 1 and 2,000 accepted; default 500 observed)', async () => {
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
    // R4-A: “prune releases the audit pin FIRST; the crash window leaves an unpinned terminal operation the next call deletes”.
  });

  it('a multi-duty maintenance-stale finding persists the templates-only joined detail with no lastError bytes anywhere; the drain emits legacyDetails[i] byte-identically', async () => {
    const record = baseRecord('multiduty', { updatedAt: FRESH_UPDATED_AT });
    const records = [record];
    const inventory = inventoryFor(records);
    const harness = buildHarness(records, inventory);
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

    const drainStore = new FakeFleetStateStore(
      records,
    ) as unknown as FleetStateStore;
    const drainFindings = await auditFleetDrift({
      store: drainStore,
      records,
      inventory,
      backendFor: () => harness.backend,
      specFor: (r) => harness.specByTenant.get(r.tenantTag) as DeploymentSpec,
      maintenanceSecretFor: (r) =>
        harness.secretByTenant.get(r.tenantTag) as string,
      staleAfterMs: STALE_AFTER_MS,
      now: AUDIT_NOW,
    });
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

  it('readAllFleetOperationRows fails closed on a page with zero rows before done', async () => {
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
    const descendingStore = new Proxy(orderedStore, {
      get(target, property, receiver) {
        if (property === 'readOperationRowsPage') {
          return async (
            input: Parameters<FleetOperationStore['readOperationRowsPage']>[0],
          ) => {
            const page = await target.readOperationRowsPage(input);
            return { ...page, rows: [...page.rows].reverse() };
          };
        }
        const value = Reflect.get(target, property, receiver);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    await expect(
      readAllFleetOperationRows(descendingStore, orderedOperationId, 'record'),
    ).resolves.toEqual(expectedRows);

    let overlappingPageCalls = 0;
    const overlappingBaseStore = new FakeOperationStore();
    const overlappingStore = new Proxy(overlappingBaseStore, {
      get(target, property, receiver) {
        if (property === 'readOperationRowsPage') {
          return async (
            input: Parameters<FleetOperationStore['readOperationRowsPage']>[0],
          ) => {
            overlappingPageCalls += 1;
            if (input.afterOrdinal === undefined) {
              return {
                rows: [{ rowKind: input.rowKind, ordinal: 1, payload: {} }],
                done: false,
              };
            }
            return {
              rows: [
                { rowKind: input.rowKind, ordinal: 2, payload: {} },
                { rowKind: input.rowKind, ordinal: 1, payload: {} },
              ],
              done: true,
            };
          };
        }
        const value = Reflect.get(target, property, receiver);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    await expect(
      readAllFleetOperationRows(overlappingStore, uuidFor(531), 'record'),
    ).rejects.toThrow('fleet operation state is malformed');
    expect(overlappingPageCalls).toBe(2);
    expect(overlappingBaseStore.operations.size).toBe(0);
    expect(overlappingBaseStore.rows.size).toBe(0);
    expect(overlappingBaseStore.heads.size).toBe(0);

    let gappedPageCalls = 0;
    const gappedStore: FleetOperationStore = {
      ...store,
      readOperationRowsPage: async (input) => {
        gappedPageCalls += 1;
        return input.afterOrdinal === undefined
          ? {
              rows: [
                { rowKind: input.rowKind, ordinal: 5, payload: {} },
                { rowKind: input.rowKind, ordinal: 1, payload: {} },
              ],
              done: false,
            }
          : {
              rows: [{ rowKind: input.rowKind, ordinal: 6, payload: {} }],
              done: true,
            };
      },
    };
    await expect(
      readAllFleetOperationRows(gappedStore, uuidFor(532), 'record'),
    ).rejects.toThrow('fleet operation state is malformed');
    expect(gappedPageCalls).toBe(2);
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

  it('findings-page reads and abandonment refuse an unknown id and a foreign-kind id before any write', async () => {
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
    expect((abandoned?.progress as FleetAuditProgress).failure?.reason).toBe(
      'operator-abandoned',
    );
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
    const drainFindings = await auditFleetDrift({
      store: observedHarness.fleetStore,
      records: [observed],
      inventory: observedInventory,
      backendFor: () => observedHarness.backend,
      specFor: (record) =>
        observedHarness.specByTenant.get(record.tenantTag) as DeploymentSpec,
      maintenanceSecretFor: (record) =>
        observedHarness.secretByTenant.get(record.tenantTag) as string,
      staleAfterMs: STALE_AFTER_MS,
      now: AUDIT_NOW,
    });
    expect(drainFindings.slice(0, observedFindings.length)).toStrictEqual(
      observedFindings,
    );
    // Detail withholding is the only expected difference in this world.
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

  it('a start whose record carries a malformed deployment identifier refuses with the fixed message and persists nothing', async () => {
    const cases = [
      baseRecord('emptyenvironment', { environment: '' }),
      baseRecord('control\u0000tenant'),
    ];
    for (const [index, record] of cases.entries()) {
      const harness = buildHarness([record], inventoryFor([record]));
      const probeCallsBefore = harness.operationStore.readOperationByIdCalls;
      await expect(
        advanceFleetAudit(
          harness.baseOptions({
            kind: 'start',
            operationId: uuidFor(61 + index),
            records: [record],
            staleAfterMs: STALE_AFTER_MS,
          }),
        ),
      ).rejects.toThrow(
        'fleet audit record tenantTag and environment must satisfy the deployment identifier grammar',
      );
      expect(harness.operationStore.readOperationByIdCalls).toBe(
        probeCallsBefore,
      );
      expectZeroHarnessWork(harness);
    }
  });

  it('a start refuses a non-positive or non-integer explicit generation, a non-string tenant tag, a record over the staged row byte bound, and a non-integer or out-of-Date-range audit clock sample before any operation row, staged row, or pin', async () => {
    const alice = baseRecord('preflight');
    for (const [index, generation] of [0, 1.5].entries()) {
      const harness = buildHarness([alice], inventoryFor([alice]));
      const operationId = uuidFor(70 + index);
      await expect(
        advanceFleetAudit(
          harness.baseOptions({
            kind: 'start',
            operationId,
            records: [alice],
            staleAfterMs: STALE_AFTER_MS,
            generation,
          }),
        ),
      ).rejects.toThrow('generation must be a positive safe integer');
      expectZeroHarnessWork(harness);
      expect(harness.operationStore.operations.has(operationId)).toBe(false);
      expect(harness.operationStore.rows.size).toBe(0);
      expect(harness.inventoryStore.pins).toEqual([]);
    }

    const nonStringTenant = {
      ...baseRecord('nonstringtenant'),
      tenantTag: null as unknown as string,
    };
    const nonStringHarness = buildHarness([], emptyInventory());
    const nonStringOperationId = uuidFor(72);
    await expect(
      advanceFleetAudit(
        nonStringHarness.baseOptions({
          kind: 'start',
          operationId: nonStringOperationId,
          records: [nonStringTenant],
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toThrow(
      'fleet audit record tenantTag and environment must satisfy the deployment identifier grammar',
    );
    expectZeroHarnessWork(nonStringHarness);
    expect(
      nonStringHarness.operationStore.operations.has(nonStringOperationId),
    ).toBe(false);
    expect(nonStringHarness.operationStore.rows.size).toBe(0);
    expect(nonStringHarness.inventoryStore.pins).toEqual([]);

    const throwingTenant = baseRecord('throwingtenant');
    Object.defineProperty(throwingTenant, 'tenantTag', {
      get() {
        throw new Error('boom');
      },
      enumerable: true,
    });
    const throwingTenantHarness = buildHarness([], emptyInventory());
    await expect(
      advanceFleetAudit(
        throwingTenantHarness.baseOptions({
          kind: 'start',
          operationId: uuidFor(720),
          records: [throwingTenant],
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toThrow('fleet audit record exceeds the intake structure bounds');
    expectZeroHarnessWork(throwingTenantHarness);

    const nullRecordHarness = buildHarness([], emptyInventory());
    const nullRecordOperationId = uuidFor(79);
    await expect(
      advanceFleetAudit(
        nullRecordHarness.baseOptions({
          kind: 'start',
          operationId: nullRecordOperationId,
          records: [null as unknown as FleetRecord],
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toThrow('fleet audit record exceeds the intake structure bounds');
    expectZeroHarnessWork(nullRecordHarness);

    const padding = Object.fromEntries(
      Array.from({ length: 25 }, (_, index) => [
        `padding${index}`,
        'x'.repeat(FLEET_OPERATION_STRING_BYTE_BOUND),
      ]),
    );
    const oversizedRecord = Object.assign(baseRecord('oversized'), padding);
    const pending: unknown[] = [oversizedRecord];
    const strings: string[] = [];
    let nodeCount = 0;
    while (pending.length > 0) {
      const current = pending.pop();
      nodeCount += 1;
      if (typeof current === 'string') strings.push(current);
      else if (Array.isArray(current)) pending.push(...current);
      else if (current && typeof current === 'object') {
        pending.push(...Object.values(current));
      }
    }
    const oversizedCanonical = canonicalFleetOperationBytes(oversizedRecord);
    expect(
      new TextEncoder().encode(oversizedCanonical).byteLength,
    ).toBeGreaterThan(FLEET_OPERATION_RECORD_ROW_BYTE_BOUND);
    expect(nodeCount).toBeLessThan(8_192);
    expect(
      Math.max(
        ...strings.map((value) => new TextEncoder().encode(value).byteLength),
      ),
    ).toBeLessThanOrEqual(FLEET_OPERATION_STRING_BYTE_BOUND);
    const oversizedHarness = buildHarness([], emptyInventory());
    const oversizedOperationId = uuidFor(73);
    await expect(
      advanceFleetAudit(
        oversizedHarness.baseOptions({
          kind: 'start',
          operationId: oversizedOperationId,
          records: [oversizedRecord],
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toThrow('fleet audit record exceeds the staged row byte bound');
    expectZeroHarnessWork(oversizedHarness);
    expect(
      oversizedHarness.operationStore.operations.has(oversizedOperationId),
    ).toBe(false);
    expect(oversizedHarness.operationStore.rows.size).toBe(0);
    expect(oversizedHarness.inventoryStore.pins).toEqual([]);

    const clockHarness = buildHarness([alice], inventoryFor([alice]), {
      auditClock: () => 1.5,
    });
    const clockOperationId = uuidFor(74);
    await expect(
      advanceFleetAudit(
        clockHarness.baseOptions({
          kind: 'start',
          operationId: clockOperationId,
          records: [alice],
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toThrow(
      'fleet audit auditClock sample must be a non-negative safe integer representable by Date',
    );
    expectZeroHarnessWork(clockHarness, {
      leaseCount: 1,
      readOperationByIdCalls: 1,
      generationReads: { latest: 1, finalized: 0, runByOperation: 0 },
    });
    expect(clockHarness.operationStore.operations.has(clockOperationId)).toBe(
      false,
    );
    expect(clockHarness.operationStore.rows.size).toBe(0);
    expect(clockHarness.inventoryStore.pins).toEqual([]);

    const outOfRangeClockHarness = buildHarness(
      [alice],
      inventoryFor([alice]),
      { auditClock: () => 9e15 },
    );
    const outOfRangeClockOperationId = uuidFor(75);
    await expect(
      advanceFleetAudit(
        outOfRangeClockHarness.baseOptions({
          kind: 'start',
          operationId: outOfRangeClockOperationId,
          records: [alice],
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toThrow(
      'fleet audit auditClock sample must be a non-negative safe integer representable by Date',
    );
    expectZeroHarnessWork(outOfRangeClockHarness, {
      leaseCount: 1,
      readOperationByIdCalls: 1,
      generationReads: { latest: 1, finalized: 0, runByOperation: 0 },
    });

    const structureBoundCases: readonly FleetRecord[] = [
      Object.assign(baseRecord('toomanynodes'), {
        padding: Array.from({ length: 9_000 }, () => null),
      }),
      Object.assign(baseRecord('overlongstring'), {
        padding: 'x'.repeat(5_000),
      }),
    ];
    for (const [index, record] of structureBoundCases.entries()) {
      const harness = buildHarness([], emptyInventory());
      const operationId = uuidFor(76 + index);
      await expect(
        advanceFleetAudit(
          harness.baseOptions({
            kind: 'start',
            operationId,
            records: [record],
            staleAfterMs: STALE_AFTER_MS,
          }),
        ),
      ).rejects.toThrow(
        'fleet audit record exceeds the intake structure bounds',
      );
      expectZeroHarnessWork(harness);
      expect(harness.operationStore.operations.has(operationId)).toBe(false);
    }

    const overlappingPadding = Object.fromEntries(
      Array.from({ length: 8_000 }, (_, index) => [
        `padding${index}`,
        'x'.repeat(2_087),
      ]),
    );
    const overlappingBaseRecord = baseRecord('overlappingbounds');
    const overlappingRecord = Object.assign(
      overlappingBaseRecord,
      overlappingPadding,
    );
    expect(countPlainDataNodes(overlappingRecord)).toBeLessThan(8_192);
    // This fixture is ASCII, so string lengths equal UTF-8 byte lengths.
    let overlappingSerializedByteCount = JSON.stringify(
      baseRecord('overlappingbounds'),
    ).length;
    for (let index = 0; index < 8_000; index += 1) {
      overlappingSerializedByteCount +=
        1 + 2 + `padding${index}`.length + 1 + 2 + 2_087;
    }
    expect(overlappingSerializedByteCount).toBeGreaterThan(
      FLEET_OPERATION_INTAKE_BYTE_BOUND,
    );
    const overlappingHarness = buildHarness([], emptyInventory());
    const overlappingOperationId = uuidFor(78);
    await expect(
      advanceFleetAudit(
        overlappingHarness.baseOptions({
          kind: 'start',
          operationId: overlappingOperationId,
          records: [overlappingRecord],
          staleAfterMs: STALE_AFTER_MS,
        }),
      ),
    ).rejects.toThrow('fleet audit record exceeds the staged row byte bound');
    expectZeroHarnessWork(overlappingHarness);
    expect(
      overlappingHarness.operationStore.operations.has(overlappingOperationId),
    ).toBe(false);
  });

  it('an emitted finding or fact row whose serialized payload or any string exceeds the staged-row envelope fails the operation durably as emission-bound-exceeded with the pin released, before the store sees the row', async () => {
    const escapedNamespaceId = '\u0000'.repeat(3_000);
    const overlongDatabaseId = 'd'.repeat(5_000);
    const cases = [
      {
        tenantTag: 'escapedfact',
        live: (record: FleetRecord) =>
          cleanLiveDeployment(record, {
            databaseId: 'db-escapedfact-drifted',
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
        const drainFindings = await auditFleetDrift({
          store: new FakeFleetStateStore([record]),
          records: [record],
          inventory: inventoryFor([record]),
          backendFor: () => harness.backend,
          specFor: (entry) =>
            harness.specByTenant.get(entry.tenantTag) as DeploymentSpec,
          maintenanceSecretFor: (entry) =>
            harness.secretByTenant.get(entry.tenantTag) as string,
          staleAfterMs: STALE_AFTER_MS,
          now: AUDIT_NOW,
        });
        expect(drainFindings.length).toBeGreaterThan(0);
      }
      const operationId = uuidFor(80 + index);
      let result = await advanceFleetAudit(
        harness.baseOptions({
          kind: 'start',
          operationId,
          records: [record],
          staleAfterMs: STALE_AFTER_MS,
        }),
      );
      for (let call = 0; call < 50; call += 1) {
        if (result.status !== 'pending' || result.stage.step === 'per-record') {
          break;
        }
        result = await advanceFleetAudit(
          harness.baseOptions({ kind: 'continue', token: result.token }),
        );
      }
      expect(result.status).toBe('pending');
      if (result.status !== 'pending') throw new Error('unreachable');
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
      expect((persisted?.progress as FleetAuditProgress).failure).toEqual({
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

  it("a start whose grammar-valid records' aggregate node count exceeds 8,192 creates the operation with recordCount equal to the input length, and its first per-record call reads the accumulated record rows across two pages", async () => {
    const records = Array.from({ length: 1_001 }, (_, index) =>
      baseRecord(`aggregate${index}`),
    );
    expect(countPlainDataNodes(records)).toBeGreaterThan(8_192);
    expect(() =>
      fleetOperationIntakeDigest({
        records,
        staleAfterMs: STALE_AFTER_MS,
        generation: null,
      }),
    ).toThrow('fleet operation state is malformed');

    const operationId = uuidFor(83);
    const clockMutationOperationId = uuidFor(830);
    const pinMutationOperationId = uuidFor(831);
    const action = {
      kind: 'start' as const,
      operationId,
      records,
      staleAfterMs: STALE_AFTER_MS,
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
    const started = await advanceFleetAudit(harness.baseOptions(action));
    expect(started.status).toBe('pending');
    if (started.status !== 'pending') throw new Error('unreachable');
    const persisted =
      await harness.operationStore.readOperationById(operationId);
    expect(persisted).toBeDefined();
    if (!persisted) throw new Error('unreachable');
    expect(persisted.state).toBe('running');
    const progress = persisted.progress as FleetAuditProgress;
    expect(progress.recordCount).toBe(intakeCount);
    expect(progress.staleAfterMs).toBe(STALE_AFTER_MS);
    expect(
      await harness.operationStore.readOperationById(clockMutationOperationId),
    ).toBeUndefined();
    expect(
      await harness.operationStore.readOperationById(pinMutationOperationId),
    ).toBeUndefined();
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

    records.length = intakeCount;
    Object.assign(records[0] as FleetRecord, { tenantTag: intakeTenantTag });
    action.operationId = operationId;
    action.staleAfterMs = STALE_AFTER_MS;
    Object.assign(records[0] as FleetRecord, {
      tenantTag: 'mutatedafterstart',
    });
    records.push(baseRecord('appendedafterstart'));
    expect(
      (
        (await harness.operationStore.readOperationById(operationId))
          ?.progress as FleetAuditProgress
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
      expect(
        await harness.operationStore.readOperationById(operationId),
      ).toEqual(corrupted);
      expect(harness.operationStore.heads.get('audit')).toBe(operationId);
      expect(corrupted.state).toBe('running');
    }
  });
});
