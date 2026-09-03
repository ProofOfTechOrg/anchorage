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
  FLEET_OPERATION_NODE_BOUND,
  FLEET_OPERATION_RECORD_ROW_BYTE_BOUND,
  FLEET_OPERATION_ROW_PAYLOAD_BYTE_BOUND,
  FLEET_OPERATION_ROW_READ_BOUND,
  FLEET_OPERATION_STAGE_BATCH_STATEMENTS,
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

type RowsPageInput = Parameters<
  FleetOperationStore['readOperationRowsPage']
>[0];
type RowsPage = Awaited<
  ReturnType<FleetOperationStore['readOperationRowsPage']>
>;

/**
 * A view of `store` whose `readOperationRowsPage` answers `transform(page,
 * input)` over the page `store` itself produced. Every other member behaves
 * as `store`'s does, bound to it — the same Proxy idiom `withoutMethod` uses
 * to shape a capability.
 *
 * `transform` may return a page unrelated to the one it was handed, which is
 * how a case models a store that fabricates rows instead of reordering the
 * ones it holds.
 */
function pageTransformingStore(
  store: FleetOperationStore,
  transform: (page: RowsPage, input: RowsPageInput) => RowsPage,
): FleetOperationStore {
  return new Proxy(store, {
    get(target, property, receiver) {
      if (property === 'readOperationRowsPage') {
        return async (input: RowsPageInput) =>
          transform(await target.readOperationRowsPage(input), input);
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === 'function' ? value.bind(target) : value;
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
  /**
   * When set, `latestFinalizedGeneration` throws it instead of answering, so
   * an unwanted call fails its title outright rather than being counted after
   * the fact (§11's "instrumented to fail the test if invoked").
   *
   * Arming is the caller's job because this fake has no notion of "the replay
   * path" and cannot detect one; a title arms it once its own legitimate call
   * has returned. Arming it for every title would break the suite instead:
   * `buildHarness` hands each title its own store, and EVERY implicit-generation
   * start calls this method.
   */
  latestFinalizedGenerationError: Error | undefined;

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
    if (this.latestFinalizedGenerationError !== undefined) {
      throw this.latestFinalizedGenerationError;
    }
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
//
// TWO DELIBERATE SOFTNESSES, stated rather than reproduced, because closing
// either would change what this suite's worlds exercise rather than what the
// coordinator does:
//
//  - ROW VALIDATION. `#validatedRows` runs `fleetOperationStagedRowFromUnknown`
//    only. `d1-fleet-operation-store.ts`'s `stagedRowForKindFromUnknown`
//    additionally rejects an `item` row under the audit kind and re-parses a
//    `finding`/`fact` payload through `driftFindingRowFromUnknown` /
//    `fleetAuditFactRowFromUnknown`. A payload this fake accepts can therefore
//    be one the shipped store would refuse; the write-side gate that matters
//    is pinned against the real codecs by the titles that read rows back.
//  - STAGING REVISION. `#stageRows` ignores `expectedRevision` entirely, where
//    D1 binds it into `OPERATION_GUARD_SQL` on every insert. The adopted-running
//    start path stages under `expectedRevision: 0` against a possibly-advanced
//    operation; on that guard miss the real store silently inserts nothing.
//    The operation can only have advanced past revision 0 after this same
//    staging ran, so under the pinned intake digest the rows are already
//    present at the same ordinals and this fake's own ordinal check drops
//    them too — an unmodelled guard, not a live divergence.
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
  /**
   * Runs on the NEXT `readOperationById` and disarms itself. One-shot on
   * purpose: `readFleetAuditFindingsPage` and the start path's catch-all call
   * the same method, so a hook armed for one coordinator call must not leak
   * into either.
   */
  onNextReadOperationById: (() => void) | undefined;
  /**
   * When set, the NEXT `commitProgress` applies its write durably and then
   * throws this instead of returning it — the lost-RESPONSE failure a
   * transport cannot distinguish from a lost request. One-shot, so the retry
   * that follows meets an ordinary store.
   */
  loseCommitProgressResponse: Error | undefined;

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
      commitProgress: async (input) => {
        const committed = await this.#commitProgress(input);
        const lost = this.loseCommitProgressResponse;
        if (lost === undefined) return committed;
        this.loseCommitProgressResponse = undefined;
        throw lost;
      },
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
    const hook = this.onNextReadOperationById;
    if (hook !== undefined) {
      this.onNextReadOperationById = undefined;
      hook();
    }
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
    if (
      rows.length + updateRows.length + 1 >
      FLEET_OPERATION_STAGE_BATCH_STATEMENTS
    ) {
      throw new Error(
        `commitProgress exceeds the operation batch budget of ${FLEET_OPERATION_STAGE_BATCH_STATEMENTS} statements`,
      );
    }
    const current = this.operations.get(operationId);
    const matches =
      current !== undefined &&
      current.state === 'running' &&
      current.progress.revision === expectedRevision;
    if (matches) {
      // D1 binds every claimed watermark into the same guarded batch, so a
      // claim the post-insert row set cannot satisfy refuses the whole commit
      // before anything persists. Evaluated BEFORE the mutations below, and on
      // the matching branch as well as the convergence one: otherwise the
      // coordinator's watermark claims run against no enforcing implementation
      // on the path its titles actually take. Both obligations the port states
      // are checked here before this branch mutates anything: the watermark
      // count over the persisted ordinals plus this batch's own, and the
      // contiguous-run precondition on the batch's inserts below the
      // watermark, which `commitWatermarkBindings` refuses before any SQL.
      for (const [rowKind, watermark] of Object.entries(
        expectedRowWatermarks,
      )) {
        const below = rows.filter(
          (row) =>
            row.rowKind === rowKind && row.ordinal < (watermark as number),
        );
        const prefix = (watermark as number) - below.length;
        if (below.some((row) => row.ordinal < prefix)) {
          throw new Error(
            `commitProgress ${rowKind} rows below the watermark must be the contiguous run ending at it`,
          );
        }
        const key = this.#rowsKey(
          operationId,
          rowKind as FleetOperationRowKind,
        );
        const ordinals = new Set(
          (this.rows.get(key) ?? []).map((existing) => existing.ordinal),
        );
        for (const row of rows) {
          if (row.rowKind === rowKind) ordinals.add(row.ordinal);
        }
        const count = [...ordinals].filter(
          (ordinal) => ordinal < (watermark as number),
        ).length;
        if (count !== watermark) {
          throw new Error(
            `fleet operation '${operationId}' is no longer at the expected revision`,
          );
        }
      }
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

/** The token of a result the caller has already asserted is `pending`. */
function expectPendingToken(
  result: FleetAuditAdvanceResult,
): PendingFleetAuditAdvance['token'] {
  if (result.status !== 'pending') {
    throw new Error(`expected a pending result, got '${result.status}'`);
  }
  return result.token;
}

/**
 * Runs the whole-fleet drain over `harness`'s resolvers against a FRESH state
 * store built from `records` — never the harness's own store, which a bounded
 * run may already have re-armed.
 *
 * `staleAfterMs` and `now` are pinned here because every call site passed the
 * same two values. That freezes the DRAIN's clock only: a title comparing the
 * two paths under the §5.5 equivalence scope also has to freeze the bounded
 * path's, which it does through `buildHarness`.
 */
function drainWith(
  harness: Harness,
  world: Readonly<{
    records: readonly FleetRecord[];
    inventory: FleetResourceInventory;
  }>,
): Promise<readonly DriftFinding[]> {
  return auditFleetDrift({
    store: new FakeFleetStateStore(world.records),
    records: world.records,
    inventory: world.inventory,
    backendFor: () => harness.backend,
    specFor: (record) =>
      harness.specByTenant.get(record.tenantTag) as DeploymentSpec,
    maintenanceSecretFor: (record) =>
      harness.secretByTenant.get(record.tenantTag) as string,
    staleAfterMs: STALE_AFTER_MS,
    now: AUDIT_NOW,
  });
}

/**
 * One INTAKE PREFLIGHT refusal case. Every case drives a single `start` and
 * asserts the same two things — the fixed refusal message, and that the
 * harness did no work beyond `coordination` — so only the fixture, the
 * injected clock, the message and the coordination vary.
 *
 * `records` BUILDS the fixture and pins whatever properties make the case's
 * refusal the only one it can trip; it runs inside the test, so it may
 * assert, and a fixture costing megabytes is never built during collection.
 */
interface PreflightRefusalCase {
  readonly name: string;
  /** The fleet the harness starts from; the inventory is derived from it. */
  readonly fleet: readonly FleetRecord[];
  readonly operationId: string;
  readonly records: () => readonly FleetRecord[];
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
    expect(started.status).toBe('pending');
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
    // §11 says `latestFinalizedGeneration` is "instrumented to fail the
    // test if invoked". The store cannot recognise a replay by itself, so the
    // trap is armed HERE — once this title's single legitimate call has
    // returned — and stays armed for the rest of it. Every later call below
    // (two continues, `driveToTerminal`, the terminal replay, the cross-kind
    // refusal) resolves its generation from the PERSISTED record, so any hit
    // on this seam is the re-read the title exists to forbid. The call-count
    // assertions stay as belt-and-braces.
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
    ).rejects.toThrow(
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
    ).rejects.toThrow('operationId must be a lowercase UUIDv4');
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
    const drainFindings = await drainWith(harness, {
      records: [first, second],
      inventory,
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
    expect(started.status).toBe('pending');
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
    // EXACT, not a floor: this world produces one `orphan-deployment` for
    // the injected ghost, one `missing-deployment` and one
    // `missing-namespace` for each of the two records absent from the
    // inventory, and one `maintenance-stale` for `control28` (this title
    // injects no clock, so `HEALTHY_MAINTENANCE`'s sweep timestamps are long
    // past by wall-clock time). A floor would keep the reversed-store
    // comparison below non-vacuous while letting the world drift underneath
    // it; the count is what that comparison actually rests on.
    expect(ascending.findings.length).toBe(6);
    const reversedStore = pageTransformingStore(
      orderedHarness.operationStore,
      (rowsPage) => ({ ...rowsPage, rows: [...rowsPage.rows].reverse() }),
    );
    await expect(
      readFleetAuditFindingsPage(reversedStore, {
        operationId: orderedOperationId,
        limit: 1_000,
      }),
    ).resolves.toEqual(ascending);
    const paged: (typeof ascending.findings)[number][] = [];
    let afterOrdinal: number | undefined;
    // Feeding `nextAfterOrdinal` straight back is the documented idiom, so the
    // cursor the reader publishes — not a formula this loop recomputes — is
    // what has to advance. A cursor that stops advancing would spin this loop
    // forever, so the page count is capped. Every non-final page carries at
    // least one finding, so an honest read needs at most one page per finding,
    // plus one more page for a store that reports `done` only on a following
    // empty page: that extra slot is why the cap is `length + 1` rather than
    // `length`, and a tightening to `length` would break a legal store.
    const pageCap = ascending.findings.length + 1;
    let reachedDone = false;
    for (let page = 0; page < pageCap; page += 1) {
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

  it('findings page: the reader verifies page conformance instead of trusting the store, accepts the two other port-permitted shapes — a non-final page shorter than the limit, and an arbitrary permutation — and returns the next cursor off the page rows', async () => {
    const control = baseRecord('control29');
    const missingA = baseRecord('missing29a');
    const missingB = baseRecord('missing29b');
    const records = [control, missingA, missingB];
    const harness = buildHarness(records, inventoryFor([control]));
    const operationId = uuidFor(90);
    const complete = await startAndDrive(harness, operationId, records);
    expect(complete.status).toBe('complete');

    const conforming = await readFleetAuditFindingsPage(
      harness.operationStore,
      { operationId, limit: 1_000 },
    );
    expect(conforming.done).toBe(true);
    // EXACT, not a floor: the same shape as the preceding title's world
    // minus its injected ghost deployment — one `missing-deployment` and one
    // `missing-namespace` per absent record, plus `control29`'s
    // `maintenance-stale`. Every case below slices or reorders this page, so
    // its length is the world they all rest on.
    expect(conforming.findings.length).toBe(5);
    // The cursor is read off the page's own rows, so a caller never recomputes
    // it from the prose formula. A full first page ends at length - 1. This
    // pins one full page's value only; the paging loop at the end of the
    // preceding test is what discriminates a published cursor from a
    // recomputed formula.
    expect(conforming.nextAfterOrdinal).toBe(conforming.findings.length - 1);

    // Shapes a page the store would not have produced. The refusal cases
    // below return pages the port FORBIDS, and every one must reach
    // `malformed()` rather than a truncated, duplicated, or non-terminating
    // read; the two acceptance cases after them return pages the port PERMITS
    // and nothing else pinned.
    const shapedPage = (
      transform: (
        rows: readonly FleetOperationStagedRow[],
      ) => readonly FleetOperationStagedRow[],
      done?: boolean,
    ) =>
      pageTransformingStore(harness.operationStore, (rowsPage) => ({
        ...rowsPage,
        rows: transform(rowsPage.rows),
        done: done ?? rowsPage.done,
      }));
    const malformedMessage = 'fleet operation state is malformed';

    // EMPTY-PAGE GUARD: an empty page while the store still claims more rows is
    // the condition `readAllFleetOperationRows` already refuses. Without this
    // guard the published next-cursor loop spins forever against such a store.
    await expect(
      readFleetAuditFindingsPage(
        shapedPage(() => [], false),
        {
          operationId,
          limit: 1_000,
        },
      ),
    ).rejects.toThrow(malformedMessage);

    // CONTIGUOUS-RUN GUARD, gap: ordinal 1 withheld.
    await expect(
      readFleetAuditFindingsPage(
        shapedPage((rows) => rows.filter((row) => row.ordinal !== 1)),
        { operationId, limit: 1_000 },
      ),
    ).rejects.toThrow(malformedMessage);

    // CONTIGUOUS-RUN GUARD, duplicate: a repeated ordinal keeps the page length
    // right, so only the run assertion catches it.
    await expect(
      readFleetAuditFindingsPage(
        shapedPage((rows) => [...rows.slice(0, -1), ...rows.slice(0, 1)]),
        { operationId, limit: 1_000 },
      ),
    ).rejects.toThrow(malformedMessage);

    // CONTIGUOUS-RUN GUARD, not the smallest qualifying ordinals: the store
    // skipped ordinal 0 instead of returning it first.
    await expect(
      readFleetAuditFindingsPage(
        shapedPage((rows) => rows.filter((row) => row.ordinal !== 0)),
        { operationId, limit: 1_000 },
      ),
    ).rejects.toThrow(malformedMessage);

    // CONTIGUOUS-RUN GUARD, row at or below the exclusive cursor.
    await expect(
      readFleetAuditFindingsPage(
        shapedPage((rows) =>
          rows.map((row) => ({ ...row, ordinal: row.ordinal - 1 })),
        ),
        { operationId, afterOrdinal: 0, limit: 1_000 },
      ),
    ).rejects.toThrow(malformedMessage);

    // PORT-PERMITTED SHAPE, a non-final page SHORTER than `limit`. The reader
    // refuses only an EMPTY unfinished page, never a short one, so this page
    // is legal and must come back with its cursor. The `done: false` ARM is
    // already driven by the preceding title's `limit: 2` paging loop — but
    // only ever by FULL pages; nothing until here accepts a short one, which
    // is the shape the advance-by-length idiom most depends on.
    const shortNonFinal = await readFleetAuditFindingsPage(
      shapedPage((rows) => rows.slice(0, 1), false),
      { operationId, limit: 1_000 },
    );
    expect(shortNonFinal.done).toBe(false);
    expect(shortNonFinal.findings).toEqual(conforming.findings.slice(0, 1));
    expect(shortNonFinal.nextAfterOrdinal).toBe(0);

    // PORT-PERMITTED SHAPE, an ARBITRARY permutation. The reader sorts before
    // it checks contiguity, so every order the port permits is accepted — not
    // just the reversal the preceding title uses. A left rotation of the five
    // findings this world holds is neither ascending nor descending.
    await expect(
      readFleetAuditFindingsPage(
        shapedPage((rows) => [...rows.slice(1), ...rows.slice(0, 1)]),
        { operationId, limit: 1_000 },
      ),
    ).resolves.toEqual(conforming);

    // A conforming empty page is legal only because it is terminal, and it
    // carries no cursor: that absence is why the field must stay optional.
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

    const drainFindings = await drainWith(harness, { records, inventory });

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
    const drainFindings = await drainWith(harness, { records, inventory });
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
    expect(started.status).toBe('pending');
    const startedToken = expectPendingToken(started);

    // The loss has to land on a CONTINUE. On the START path the revision-1
    // commit sits inside a catch-all that reads the operation back, so a
    // durably-applied-then-thrown response there produces an ordinary
    // `pending` and proves nothing about the discriminator; start-replay
    // convergence (same token, no duplicate rows) is the start-replay
    // title's own subject and stays pinned there. The continue-path commits
    // carry no such catch, so the throw propagates while the write stands —
    // exactly a lost response.
    const lostResponse = new Error(
      'fleet operation store lost the commitProgress response',
    );
    harness.operationStore.loseCommitProgressResponse = lostResponse;
    const rowsBeforeLoss = structuredClone([...harness.operationStore.rows]);
    await expect(
      advanceFleetAudit(
        harness.baseOptions({ kind: 'continue', token: startedToken }),
      ),
    ).rejects.toBe(lostResponse);

    // The write LANDED: the operation is still running, its pin still held,
    // and its persisted revision is one past the token the caller holds.
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

    // The retry replays the SAME token. `classifyFleetOperationToken` reads it
    // as `stale` against the advanced persisted revision — the revision
    // discriminator — so the caller converges on the authoritative result
    // instead of re-running the chunk.
    const retried = await advanceFleetAudit(
      harness.baseOptions({ kind: 'continue', token: startedToken }),
    );
    expect(retried.status).toBe('pending');
    const retriedToken = expectPendingToken(retried);
    expect(retriedToken).toEqual({
      ...startedToken,
      revision: persistedProgress.revision,
    });
    expect(retried).toEqual({
      status: 'pending',
      token: retriedToken,
      stage: persistedProgress.stage,
    });

    // …and it staged nothing: convergence, not a second application.
    expect([...harness.operationStore.rows]).toEqual(rowsBeforeLoss);
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
    // A near-unfalsifiable assertion, kept deliberately. The first title in
    // this file reads the persisted progress through
    // `fleetAuditProgressFromUnknown`, whose exact-key assertion already
    // rejects any extra field, so a `signal` landing in `progress` is caught
    // there. This is a byte SCAN over the whole run record rather than a shape
    // check on one field, which is the half no shape assertion makes: it also
    // catches the option arriving under some other key, or nested inside a
    // value. It is the load-bearing half of the call-local claim.
    expect(JSON.stringify(persisted)).not.toContain('signal');
    expect(result.status).toBe('pending');

    // An explicit abort reason, so the refusal is pinned by IDENTITY rather
    // than by "something threw": the coordinator must propagate the caller's
    // own reason out of `signal.throwIfAborted()` untouched.
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
    expect(started.status).toBe('pending');
    const atPerRecord = await driveToStage(
      harness,
      expectPendingToken(started),
      'per-record',
    );

    // The coordinator re-checks the signal INSIDE the per-record chunk, after
    // it has read the accumulated fact rows and before it calls the record
    // step. Aborting from the fact read is what lands the abort in that
    // window: the entry check has already passed, so a refusal here can only
    // come from the mid-record checkpoint.
    const abortReason = new Error('fleet audit aborted mid per-record call');
    const readOperationRowsPage =
      harness.operationStore.readOperationRowsPage.bind(harness.operationStore);
    harness.operationStore.readOperationRowsPage = async (
      input: RowsPageInput,
    ) => {
      const page = await readOperationRowsPage(input);
      if (input.rowKind === 'fact') controller.abort(abortReason);
      return page;
    };

    const opsBefore = harness.opsLog.length;
    const factReadsBefore =
      harness.operationStore.rowPageReadCounts.get('fact') ?? 0;
    const rowsBefore = structuredClone([...harness.operationStore.rows]);
    const persistedBefore = fleetAuditProgressFromUnknown(
      (await harness.operationStore.readOperationById(operationId))?.progress,
    );
    await expect(
      advanceFleetAudit(
        harness.baseOptions({ kind: 'continue', token: atPerRecord.token }),
      ),
    ).rejects.toBe(abortReason);
    harness.operationStore.readOperationRowsPage = readOperationRowsPage;

    // The call got PAST the entry check — it read fact rows — and stopped
    // before any resolver or provider work.
    expect(
      harness.operationStore.rowPageReadCounts.get('fact') ?? 0,
    ).toBeGreaterThan(factReadsBefore);
    expect(harness.opsLog.length).toBe(opsBefore);
    expect(harness.fleetStore.ops).toEqual([]);
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
    // The `Authorization` half of the scan is vacuous unless some
    // provider-sourced text actually carries those bytes into the call. This
    // duty error does: the maintenance-stale template names the failed attempt
    // and its timestamp and NEVER the provider's own error string, so the scan
    // below has something real to refute.
    const sweepError = 'Authorization: Bearer leaked-provider-header';
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
      expect(text).not.toContain('leaked-provider-header');
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

    const drainFindings = await drainWith(harness, { records, inventory });
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
    const startTimeDrainFindings = await drainWith(harness, {
      records,
      inventory,
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
    // R4-A: “prune releases the audit pin FIRST; the crash window leaves an unpinned terminal operation the next call deletes”.
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

    const drainFindings = await drainWith(harness, { records, inventory });
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

    // DUPLICATE ORDINAL WITHIN ONE PAGE. On a FIRST page `afterOrdinal` is
    // `undefined`, so the `row.ordinal <= afterOrdinal` arm cannot fire and
    // only the page-scoped `Set` can decide. The overlapping case further
    // down never reaches that arm — its repeated row is rejected by the
    // cursor comparison first — and the gapped case carries no duplicate at
    // all, so this fixture is the `Set` arm's only falsifying case: the page
    // is deliberately NOT `done`, so without the arm the reader would fetch
    // a second page, and `duplicatePageCalls` pins that it does not.
    let duplicatePageCalls = 0;
    const duplicateOrdinalStore = pageTransformingStore(
      new FakeOperationStore(),
      (_page, input) => {
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

    // ROW-READ CAP BY KIND. The cap case below runs on the `record` kind, so
    // the `record` arm of `readAllFleetOperationRows`'s bound ternary is what
    // it exercises; the 990,000 non-`record` arm is pinned here as a constant
    // only. Driving a fixture through it would mean materializing ~990,001
    // rows to exercise a two-value ternary over the same guard, which is not
    // worth the suite time.
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
    const descendingStore = pageTransformingStore(orderedStore, (page) => ({
      ...page,
      rows: [...page.rows].reverse(),
    }));
    await expect(
      readAllFleetOperationRows(descendingStore, orderedOperationId, 'record'),
    ).resolves.toEqual(expectedRows);

    let overlappingPageCalls = 0;
    const overlappingBaseStore = new FakeOperationStore();
    const overlappingStore = pageTransformingStore(
      overlappingBaseStore,
      (_page, input) => {
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
    expect(overlappingBaseStore.operations.size).toBe(0);
    expect(overlappingBaseStore.rows.size).toBe(0);
    expect(overlappingBaseStore.heads.size).toBe(0);

    // PAGE CONTIGUITY. `[5, 1]`,`[6]` refuses on the MISSING ZERO — the
    // sorted run starts at 1, so the final index check fails on the very
    // first row and the 2-4 skip is never reached. `[5, 0]`,`[6]` is the same
    // sequence with that first-row objection removed, so only the interior
    // gap can decide it. Both are kept: they refuse for different reasons.
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
    const drainFindings = await drainWith(observedHarness, {
      records: [observed],
      inventory: observedInventory,
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
      expectZeroHarnessWork(harness);
    }
  });

  // INTAKE PREFLIGHT, re-cut here as a table. Eleven fixtures used to share
  // one ~250-line body — nine `rejects.toThrow` blocks, two of them looping
  // over two fixtures each — that repeated the same build /
  // `rejects.toThrow` / `expectZeroHarnessWork` work; each fixture now
  // reports under its own title, so a failure names the one that broke.
  const preflightRecord = baseRecord('preflight');
  const GRAMMAR_REFUSAL =
    'fleet audit record tenantTag and environment must satisfy the deployment identifier grammar';
  const STRUCTURE_REFUSAL =
    'fleet audit record exceeds the intake structure bounds';
  const ROW_BYTE_REFUSAL =
    'fleet audit record exceeds the staged row byte bound';
  const CLOCK_REFUSAL =
    'fleet audit auditClock sample must be a non-negative safe integer representable by Date';
  // The clock refusal is the one preflight case that follows the lease row:
  // it is sampled inside the lease, after the probe and the generation read.
  const AFTER_LEASE_AND_PROBE = {
    leaseCount: 1,
    readOperationByIdCalls: 1,
    generationReads: { latest: 1, finalized: 0, runByOperation: 0 },
  } as const;

  const preflightRefusalCases: readonly PreflightRefusalCase[] = [
    {
      name: 'a non-positive explicit generation',
      fleet: [preflightRecord],
      operationId: uuidFor(70),
      records: () => [preflightRecord],
      generation: 0,
      message: 'generation must be a positive safe integer',
    },
    {
      name: 'a non-integer explicit generation',
      fleet: [preflightRecord],
      operationId: uuidFor(71),
      records: () => [preflightRecord],
      generation: 1.5,
      message: 'generation must be a positive safe integer',
    },
    {
      name: 'a non-string tenant tag',
      fleet: [],
      operationId: uuidFor(72),
      records: () => [
        {
          ...baseRecord('nonstringtenant'),
          tenantTag: null as unknown as string,
        },
      ],
      message: GRAMMAR_REFUSAL,
    },
    {
      name: 'a throwing tenantTag accessor',
      fleet: [],
      operationId: uuidFor(720),
      records: () => {
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
      records: () => [null as unknown as FleetRecord],
      message: STRUCTURE_REFUSAL,
    },
    {
      name: 'a record over the staged row bound',
      fleet: [],
      operationId: uuidFor(73),
      records: () => {
        const record = Object.assign(
          baseRecord('oversized'),
          Object.fromEntries(
            Array.from({ length: 25 }, (_, index) => [
              `padding${index}`,
              'x'.repeat(FLEET_OPERATION_STRING_BYTE_BOUND),
            ]),
          ),
        );
        // The walk survives for the max-string assertion alone; the node
        // count comes from the file's own helper.
        const strings: string[] = [];
        const pending: unknown[] = [record];
        while (pending.length > 0) {
          const current = pending.pop();
          if (typeof current === 'string') strings.push(current);
          else if (Array.isArray(current)) pending.push(...current);
          else if (current && typeof current === 'object') {
            pending.push(...Object.values(current));
          }
        }
        expect(
          new TextEncoder().encode(canonicalFleetOperationBytes(record))
            .byteLength,
        ).toBeGreaterThan(FLEET_OPERATION_RECORD_ROW_BYTE_BOUND);
        expect(countPlainDataNodes(record)).toBeLessThan(
          FLEET_OPERATION_NODE_BOUND,
        );
        expect(
          Math.max(
            ...strings.map(
              (value) => new TextEncoder().encode(value).byteLength,
            ),
          ),
        ).toBeLessThanOrEqual(FLEET_OPERATION_STRING_BYTE_BOUND);
        return [record];
      },
      message: ROW_BYTE_REFUSAL,
    },
    {
      name: 'a non-integer audit clock sample',
      fleet: [preflightRecord],
      operationId: uuidFor(74),
      records: () => [preflightRecord],
      auditClock: () => 1.5,
      message: CLOCK_REFUSAL,
      coordination: AFTER_LEASE_AND_PROBE,
    },
    {
      name: 'an out-of-range audit clock sample',
      fleet: [preflightRecord],
      operationId: uuidFor(75),
      records: () => [preflightRecord],
      auditClock: () => 9e15,
      message: CLOCK_REFUSAL,
      coordination: AFTER_LEASE_AND_PROBE,
    },
    {
      name: 'a record over the intake node bound',
      fleet: [],
      operationId: uuidFor(76),
      records: () => [
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
      records: () => [
        Object.assign(baseRecord('overlongstring'), {
          padding: 'x'.repeat(5_000),
        }),
      ],
      message: STRUCTURE_REFUSAL,
    },
    {
      name: 'a record over row and intake bounds',
      fleet: [],
      operationId: uuidFor(78),
      records: () => {
        const padding = Object.fromEntries(
          Array.from({ length: 8_000 }, (_, index) => [
            `padding${index}`,
            'x'.repeat(2_087),
          ]),
        );
        const record = Object.assign(baseRecord('overlappingbounds'), padding);
        expect(countPlainDataNodes(record)).toBeLessThan(
          FLEET_OPERATION_NODE_BOUND,
        );
        // Both terms are derived from the fixture rather than restated. The
        // fixture is ASCII, so string lengths equal UTF-8 byte lengths, and
        // each padding entry adds to the record's JSON exactly: one ','
        // separator, the key's two '"' quotes, the key itself, one ':'
        // separator, the value's two '"' quotes, and the value itself.
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

  // Named so the frozen title survives as a greppable literal: `it.each`
  // resolves `$name` per case, so none of the eleven generated titles appears
  // anywhere in this file.
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
    await expect(
      advanceFleetAudit(
        harness.baseOptions({
          kind: 'start',
          operationId: testCase.operationId,
          records: testCase.records(),
          staleAfterMs: STALE_AFTER_MS,
          ...(testCase.generation === undefined
            ? {}
            : { generation: testCase.generation }),
        }),
      ),
    ).rejects.toThrow(testCase.message);
    // `expectZeroHarnessWork` already asserts the empty operation, row,
    // head, digest and pin maps, so no case repeats them.
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
        const drainFindings = await drainWith(harness, {
          records: [record],
          inventory: inventoryFor([record]),
        });
        // §5.5 class (d): the drain over the IDENTICAL world completes and
        // returns its FULL finding array, where the bounded path refuses the
        // first fact row. A length floor would pass on any finding at all, so
        // the whole array is pinned.
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
    expect(countPlainDataNodes(records)).toBeGreaterThan(
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
    // The clock and pin hooks both fire AFTER the generation is resolved, so
    // neither can falsify the hoisted `action.generation`. The probe —
    // `readOperationById`, the single seam between the hoist and the use — is
    // the only hook early enough, and it is one-shot so the direct reads
    // further down cannot re-trigger it.
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
    const harness = buildHarness(records, inventoryFor(records));
    // `carol` is the finding-free record: the same clean fixture as the
    // other two, differing only in maintenance duties fresh against the
    // harness's UNFROZEN audit clock, which is what leaves the other two
    // stale. Staged in the MIDDLE, so a stage that visited only the
    // emitting records would break the order read back below.
    const liveNow = Date.now();
    harness.liveByTenant.set(
      carol.tenantTag,
      cleanLiveDeployment(carol, {
        maintenance: {
          ...HEALTHY_MAINTENANCE,
          nextAlarmAt: liveNow + 60_000,
          lastSweepAt: liveNow,
          lastPurgeAt: liveNow,
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
    for (const tag of stagedTags) {
      expect(visited.filter((seen) => seen === tag)).toHaveLength(1);
    }
    const page = await readFleetAuditFindingsPage(harness.operationStore, {
      operationId,
      limit: 10,
    });
    expect(page.done).toBe(true);
    // The added conjunct, asserted first so it is the one that names the
    // record when it breaks: the middle record the stage visited emitted
    // nothing.
    expect(
      page.findings.filter((finding) => finding.tenantTag === carol.tenantTag),
    ).toEqual([]);
    // `bob` and `alice` still emit one `maintenance-stale` finding each
    // under this harness's unfrozen maintenance clock, so the page carries
    // exactly one finding for each of them, in the order the stage visited
    // them.
    expect(
      page.findings.map((finding) => [finding.tenantTag, finding.kind]),
    ).toEqual([
      ['bob', 'maintenance-stale'],
      ['alice', 'maintenance-stale'],
    ]);
  });
});
