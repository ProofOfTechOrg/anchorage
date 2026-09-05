// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { applicationBindingTopology } from '../src/application-bindings.js';
import {
  admitFleetMigrationItem,
  assertFleetMigrationPlanCompatibility,
  executeNextMigrationStep,
  migrateFleet,
} from '../src/fleet.js';
import {
  type AdvanceFleetMigrationOptions,
  abandonFleetMigrationOperation,
  advanceFleetMigration,
  type FleetMigrationAdvanceAction,
  FleetMigrationAdvanceCapabilityError,
  type FleetMigrationAdvanceResult,
  readFleetMigrationItemsPage,
} from '../src/fleet-migration-advance.js';
import {
  type FleetMigrationItem,
  type FleetMigrationStep,
  fleetMigrationItemFromUnknown,
} from '../src/fleet-migration-state.js';
import {
  FLEET_OPERATION_INTAKE_BYTE_BOUND,
  FLEET_OPERATION_ITEM_BOUND,
  FLEET_OPERATION_RECORD_ROW_BYTE_BOUND,
  type FleetOperationKind,
  type FleetOperationLease,
  type FleetOperationRowKind,
  type FleetOperationRunRecord,
  type FleetOperationStagedRow,
  FleetOperationStateError,
  type FleetOperationStore,
  FleetOperationTokenFutureError,
  FleetOperationTokenKindError,
  FleetOperationTokenOperationError,
  fleetOperationIntakeDigest,
  fleetOperationStagedRowFromUnknown,
} from '../src/fleet-operation-state.js';
import {
  canonicalDeploymentEgressPolicy,
  durableObjectMigrationHistoryDigest,
  externalEgressProxyScriptName,
  externalPlatformResourceGroupId,
  externalStateScriptName,
} from '../src/platform-resources.js';
import { providerBindingIdentitiesForInspection } from '../src/provider-binding-inventory.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type {
  ApplicationBindingTopology,
  DeploymentSecrets,
  DeploymentSpec,
  ExternalPlatformResources,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  FleetRecord,
  FleetStateLease,
  FleetStateStore,
  LiveDeployment,
  MaintenanceHealth,
  ProvisioningBackend,
} from '../src/types.js';
import { externalReleaseScriptName } from '../src/workers-for-platforms-backend.js';

const NOW = Date.parse('2026-09-05T12:00:00.000Z');
const KEY =
  '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}';
const EMPTY_APPLICATION: ApplicationBindingTopology = {
  vars: [],
  secrets: [],
  r2Buckets: [],
};
const HEALTHY: MaintenanceHealth = {
  armed: true,
  nextAlarmAt: NOW + 60_000,
  lastSweepAt: NOW,
  lastPurgeAt: NOW,
};
const SECRETS: DeploymentSecrets = {
  deploymentIdentity: 'migration-identity-private-value-00000001',
  maintenanceAdmin: 'migration-maintenance-private-value-00001',
};
const unused = async (): Promise<never> => {
  throw new Error('unexpected provider operation');
};
const uuid = (seed = 1) =>
  `${seed.toString(16).padStart(8, '0')}-0000-4000-8000-000000000000`;
const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
const conflict = (id: string) =>
  new Error(`fleet operation '${id}' is no longer at the expected revision`);
const divergence = (id: string) =>
  new Error(
    `fleet operation '${id}' staged rows diverge from the persisted operation`,
  );

class MemoryOperationStore implements FleetOperationStore {
  readonly operations = new Map<string, FleetOperationRunRecord>();
  readonly rows = new Map<string, FleetOperationStagedRow[]>();
  readonly digests = new Map<string, string>();
  readonly heads = new Map<FleetOperationKind, string>();
  readonly locked = new Set<FleetOperationKind>();
  readonly calls: string[] = [];
  readonly commits: Parameters<FleetOperationLease['commitProgress']>[0][] = [];
  loseLease = false;
  loseCommit: 'before' | 'after' | undefined;
  stageLimit: number | undefined;
  beforeLease: (() => void) | undefined;
  beforeCommit:
    | ((input: Parameters<FleetOperationLease['commitProgress']>[0]) => void)
    | undefined;
  beforePage: (() => void) | undefined;
  reversePages = false;

  async withAccountOperationLease<T>(
    kind: FleetOperationKind,
    run: (lease: FleetOperationLease) => Promise<T>,
  ): Promise<T> {
    this.beforeLease?.();
    if (this.locked.has(kind)) throw new Error(`contended ${kind} lease`);
    this.locked.add(kind);
    const assertOwned = async () => {
      if (this.loseLease) throw new Error('operation lease lost');
    };
    try {
      return await run({
        assertOwned,
        readOperation: async (id) => this.operations.get(id),
        startOperation: async (input) => {
          await assertOwned();
          this.calls.push('start');
          const prior = this.operations.get(input.operationId);
          if (prior) {
            if (prior.kind !== kind)
              throw new Error(
                `fleet operation '${input.operationId}' belongs to the other operation kind`,
              );
            if (this.digests.get(input.operationId) !== input.intakeDigest)
              throw new Error(
                `fleet operation '${input.operationId}' already exists with a different intake`,
              );
            return {
              outcome:
                prior.state === 'running'
                  ? 'adopted-running'
                  : 'adopted-terminal',
              record: copy(prior),
            };
          }
          if (this.heads.has(kind))
            throw new Error(
              `another fleet ${kind} operation is active for this account`,
            );
          this.operations.set(input.operationId, copy(input.runRecord));
          this.digests.set(input.operationId, input.intakeDigest);
          this.heads.set(kind, input.operationId);
          return { outcome: 'created', record: copy(input.runRecord) };
        },
        stageRows: async (input) => {
          await assertOwned();
          this.calls.push('stage');
          const record = this.operations.get(input.operationId);
          if (
            record?.state !== 'running' ||
            record.progress.revision !== input.expectedRevision
          )
            return;
          const previous = new Map<FleetOperationRowKind, number>();
          for (const [index, row] of input.rows.entries()) {
            if (index === this.stageLimit) {
              this.stageLimit = undefined;
              throw new Error('staging interrupted');
            }
            if (row.ordinal <= (previous.get(row.rowKind) ?? -1))
              throw new Error('staging is not ordered');
            previous.set(row.rowKind, row.ordinal);
            const validated = this.validateRow(row);
            const rows = this.rows.get(input.operationId) ?? [];
            if (
              !rows.some(
                (prior) =>
                  prior.rowKind === row.rowKind &&
                  prior.ordinal === row.ordinal,
              )
            )
              rows.push(validated);
            this.rows.set(input.operationId, rows);
          }
        },
        commitProgress: async (input) => {
          this.calls.push('commit');
          this.commits.push(copy(input));
          this.beforeCommit?.(input);
          if (this.loseCommit === 'before') {
            this.loseCommit = undefined;
            throw new Error('progress response lost');
          }
          const rows = (input.rows ?? []).map((row) => this.validateRow(row));
          const updates = (input.updateRows ?? []).map((row) =>
            this.validateRow(row),
          );
          const mutationKeys = [...rows, ...updates].map(
            (row) => `${row.rowKind}:${row.ordinal}`,
          );
          if (
            updates.some((row) => row.rowKind !== 'item') ||
            new Set(mutationKeys).size !== mutationKeys.length
          ) {
            throw new FleetOperationStateError();
          }
          if (rows.length + updates.length + 1 > 100)
            throw new Error(
              'commitProgress exceeds the operation batch budget of 100 statements',
            );
          for (const [kind, watermark] of Object.entries(
            input.expectedRowWatermarks ?? {},
          )) {
            const below = rows.filter(
              (row) => row.rowKind === kind && row.ordinal < watermark,
            );
            const ordinals = below
              .map((row) => row.ordinal)
              .sort((a, b) => a - b);
            if (
              ordinals.some(
                (ordinal, index) =>
                  ordinal !== watermark - below.length + index,
              )
            )
              throw new Error(
                `commitProgress ${kind} rows below the watermark must be the contiguous run ending at it`,
              );
          }
          const prior = this.operations.get(input.operationId);
          const persistedRows = this.rows.get(input.operationId) ?? [];
          if (
            !this.loseLease &&
            prior?.state === 'running' &&
            prior.progress.revision === input.expectedRevision
          ) {
            for (const [kind, watermark] of Object.entries(
              input.expectedRowWatermarks ?? {},
            )) {
              const ordinals = new Set(
                [...persistedRows, ...rows]
                  .filter(
                    (row) => row.rowKind === kind && row.ordinal < watermark,
                  )
                  .map((row) => row.ordinal),
              );
              if (ordinals.size !== watermark)
                throw conflict(input.operationId);
            }
            for (const row of rows) {
              if (
                !persistedRows.some(
                  (prior) =>
                    prior.rowKind === row.rowKind &&
                    prior.ordinal === row.ordinal,
                )
              )
                persistedRows.push(row);
            }
            for (const row of updates) {
              const index = persistedRows.findIndex(
                (prior) =>
                  prior.rowKind === row.rowKind &&
                  prior.ordinal === row.ordinal,
              );
              if (index >= 0) persistedRows[index] = row;
            }
            this.rows.set(input.operationId, persistedRows);
            this.operations.set(input.operationId, copy(input.runRecord));
          } else {
            if (!prior)
              throw new Error(`no fleet operation '${input.operationId}'`);
            for (const [kind, watermark] of Object.entries(
              input.expectedRowWatermarks ?? {},
            )) {
              if (
                persistedRows.filter(
                  (row) => row.rowKind === kind && row.ordinal < watermark,
                ).length !== watermark
              )
                throw conflict(input.operationId);
            }
            if (JSON.stringify(prior) !== JSON.stringify(input.runRecord))
              throw conflict(input.operationId);
            let complete = true;
            for (const row of [...rows, ...updates]) {
              const persisted = persistedRows.find(
                (prior) =>
                  prior.rowKind === row.rowKind &&
                  prior.ordinal === row.ordinal,
              );
              if (!persisted) complete = false;
              else if (
                JSON.stringify(persisted.payload) !==
                JSON.stringify(row.payload)
              )
                throw divergence(input.operationId);
            }
            if (!complete) throw conflict(input.operationId);
          }
          if (this.loseCommit === 'after') {
            this.loseCommit = undefined;
            throw new Error('progress response lost');
          }
          return copy(
            this.operations.get(input.operationId) as FleetOperationRunRecord,
          );
        },
        finalizeOperation: async (input) => {
          await assertOwned();
          this.calls.push('finalize');
          const prior = this.operations.get(input.operationId);
          if (!prior)
            throw new Error(`no fleet operation '${input.operationId}'`);
          if (
            prior.state === 'finalized' &&
            prior.progress.revision === input.runRecord.progress.revision
          )
            return prior;
          if (
            prior.state !== 'running' ||
            prior.progress.revision !== input.expectedRevision
          )
            throw conflict(input.operationId);
          const rows = this.rows.get(input.operationId) ?? [];
          for (const [kind, count] of Object.entries(input.expectedRowCounts)) {
            if (rows.filter((row) => row.rowKind === kind).length !== count)
              throw new Error('finalize counts differ');
          }
          if (
            input.requireAllItemsComplete &&
            rows.some(
              (row) =>
                row.rowKind === 'item' && row.payload.status !== 'complete',
            )
          )
            throw new Error('items are incomplete');
          const finalized = { ...input.runRecord, terminalAtMs: NOW };
          this.operations.set(input.operationId, copy(finalized));
          this.heads.delete(kind);
          return copy(finalized);
        },
        failOperation: async (input) => {
          await assertOwned();
          this.calls.push('fail');
          if ((input.updateRows?.length ?? 0) > 1)
            throw new Error('failOperation accepts at most one updateRow');
          const prior = this.operations.get(input.operationId);
          if (!prior)
            throw new Error(`no fleet operation '${input.operationId}'`);
          const rows = this.rows.get(input.operationId) ?? [];
          if (
            prior.state === 'failed' &&
            prior.progress.revision === input.runRecord.progress.revision
          ) {
            for (const row of input.updateRows ?? []) {
              const stored = rows.find(
                (candidate) =>
                  candidate.rowKind === row.rowKind &&
                  candidate.ordinal === row.ordinal,
              );
              if (
                !stored ||
                JSON.stringify(stored.payload) !== JSON.stringify(row.payload)
              )
                throw divergence(input.operationId);
            }
            return;
          }
          if (
            prior.state !== 'running' ||
            prior.progress.revision !== input.expectedRevision
          )
            throw conflict(input.operationId);
          const updates = (input.updateRows ?? []).map((row) =>
            this.validateRow(row),
          );
          for (const row of updates) {
            if (
              !rows.some(
                (prior) =>
                  prior.rowKind === row.rowKind &&
                  prior.ordinal === row.ordinal,
              )
            )
              throw conflict(input.operationId);
          }
          for (const row of updates) {
            const index = rows.findIndex(
              (prior) =>
                prior.rowKind === row.rowKind && prior.ordinal === row.ordinal,
            );
            rows[index] = row;
          }
          this.operations.set(
            input.operationId,
            copy({ ...input.runRecord, terminalAtMs: NOW }),
          );
          this.heads.delete(kind);
        },
      });
    } finally {
      this.locked.delete(kind);
    }
  }

  validateRow(row: FleetOperationStagedRow): FleetOperationStagedRow {
    const parsed = fleetOperationStagedRowFromUnknown(row);
    if (parsed.rowKind === 'item')
      return {
        ...parsed,
        payload: { ...fleetMigrationItemFromUnknown(parsed.payload) },
      };
    return parsed;
  }

  async readOperationById(id: string) {
    return this.operations.get(id);
  }

  async readOperationRowsPage(
    input: Parameters<FleetOperationStore['readOperationRowsPage']>[0],
  ) {
    this.beforePage?.();
    if (
      !Number.isSafeInteger(input.limit) ||
      input.limit < 1 ||
      input.limit > 1000
    )
      throw new Error('limit must be an integer from 1 to 1000');
    const qualifying = (this.rows.get(input.operationId) ?? [])
      .filter(
        (row) =>
          row.rowKind === input.rowKind &&
          row.ordinal > (input.afterOrdinal ?? -1),
      )
      .sort((a, b) => a.ordinal - b.ordinal);
    const rows = qualifying.slice(0, input.limit).map(copy);
    return {
      rows: this.reversePages ? rows.reverse() : rows,
      done: qualifying.length <= input.limit,
    };
  }

  async pruneFleetOperations() {
    return { deleted: 0, releasedPins: 0 };
  }

  item(id = uuid(), ordinal = 0): FleetMigrationItem {
    const row = this.rows
      .get(id)
      ?.find((row) => row.rowKind === 'item' && row.ordinal === ordinal);
    if (!row) throw new Error('missing item fixture');
    return fleetMigrationItemFromUnknown(row.payload);
  }

  setItem(item: FleetMigrationItem, id = uuid()): void {
    const rows = this.rows.get(id);
    if (!rows) throw new Error('missing rows fixture');
    const index = rows.findIndex(
      (row) => row.rowKind === 'item' && row.ordinal === item.ordinal,
    );
    rows[index] = {
      rowKind: 'item',
      ordinal: item.ordinal,
      payload: { ...item },
    };
  }
}

class MemoryFleetStore implements FleetStateStore {
  readonly records = new Map<string, FleetRecord>();
  readonly puts: FleetRecord[] = [];
  readonly ops: string[] = [];
  held = false;
  beforeGet: (() => void) | undefined;
  beforePut: ((record: FleetRecord) => void) | undefined;
  transformRead: (record: FleetRecord) => FleetRecord = copy;

  constructor(records: readonly FleetRecord[]) {
    for (const record of records) this.set(record);
  }

  set(record: FleetRecord): void {
    this.records.set(`${record.tenantTag}:${record.environment}`, copy(record));
  }

  async get(tenant: string, environment: string) {
    this.ops.push('get');
    this.beforeGet?.();
    const record = this.records.get(`${tenant}:${environment}`);
    return record ? this.transformRead(record) : undefined;
  }

  async withDeploymentLease<T>(
    tenantTag: string,
    environment: string,
    run: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T> {
    if (this.held) throw new Error('deployment lease contended');
    this.held = true;
    this.ops.push('lease');
    try {
      return await run({
        tenantTag,
        environment,
        mutationLeaseTtlMs: 900_000,
        assertOwned: async () => {
          if (!this.held) throw new Error('deployment lease lost');
        },
        renew: unused,
        put: async (record) => {
          if (
            !this.held ||
            record.tenantTag !== tenantTag ||
            record.environment !== environment
          )
            throw new Error('unfenced put');
          this.beforePut?.(record);
          this.puts.push(copy(record));
          this.ops.push(
            `put:${record.phase}:${record.migrationIntent?.subphase ?? record.schemaVersion}`,
          );
          this.set(record);
        },
        delete: unused,
        completeCleanup: unused,
        deleteReleasingClaims: unused,
      });
    } finally {
      this.held = false;
    }
  }

  async list() {
    return [...this.records.values()].map(copy);
  }
  readCleanupReceipt = unused;
  pruneCleanupReceipts = unused;
}

function deploymentSpec(
  tenantTag = 'cedar',
  schemaVersion = 3,
  external = false,
): DeploymentSpec {
  return {
    tenantTag,
    environment: 'production',
    scriptName: `worker-${tenantTag}`,
    databaseName: `database-${tenantTag}`,
    compatibilityDate: '2026-05-01',
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default { fetch() {} }' }],
    authoredBy: external ? 'external' : 'platform',
    schemaVersion,
    migrations: Array.from({ length: schemaVersion }, (_, index) => ({
      version: index + 1,
      sql: `SELECT ${index + 1}`,
      rollbackCompatible: true,
    })),
    durableObjectMigrations: [],
    durableObjectBindings: [],
    maintenanceBaseUrl: `https://control-${tenantTag}.example.test`,
    routeHostname: `${tenantTag}.example.test`,
  };
}

function baseRecord(spec: DeploymentSpec): FleetRecord {
  return {
    tenantTag: spec.tenantTag,
    environment: spec.environment,
    backend:
      spec.authoredBy === 'external' ? 'workers-for-platforms' : 'plain-worker',
    scriptName: spec.scriptName,
    databaseName: spec.databaseName,
    databaseId: `db-${spec.tenantTag}`,
    schemaVersion: spec.schemaVersion,
    artifactVersion: `v${spec.schemaVersion}`,
    desiredSpecDigest: deploymentSpecDigest(spec),
    durableObjectBindings: [],
    routeHostname: spec.routeHostname,
    phase: 'ready',
    applicationBindings: EMPTY_APPLICATION,
    updatedAt: '2026-08-01T00:00:00.000Z',
  };
}

function createWorld(
  input: {
    path?: 'ready' | 'platform-only' | 'full';
    external?: boolean;
    tenant?: string;
    schemaVersion?: number;
    lagging?: boolean;
  } = {},
) {
  const path = input.path ?? 'full';
  const external = input.external ?? path === 'platform-only';
  const priorSpec = deploymentSpec(input.tenant ?? 'cedar', 1, external);
  let spec =
    path === 'full'
      ? deploymentSpec(priorSpec.tenantTag, input.schemaVersion ?? 3, external)
      : priorSpec;
  let stateDigest = 'a'.repeat(64);
  const ops: string[] = [];
  const releases = new Map<string, LiveDeployment>();
  const routed = new Map<string, string>();
  const applications = new Map<string, ApplicationBindingTopology>();
  const ledger = new Set([1]);
  const targetFor = (
    targetSpec: DeploymentSpec,
  ): ExternalPlatformTargetDescription => ({
    maintenanceCapabilityPublicKey: KEY,
    stateArtifactDigest: stateDigest,
    stateDurableObjectHistoryDigest: durableObjectMigrationHistoryDigest([]),
    egressArtifactDigest: 'b'.repeat(64),
    d1SchemaVersion: targetSpec.schemaVersion,
    d1SchemaHistoryDigest: deploymentSpecDigest(targetSpec),
    outboundPolicy: canonicalDeploymentEgressPolicy({
      policyId: externalPlatformResourceGroupId(targetSpec),
      tenantTag: targetSpec.tenantTag,
      environment: targetSpec.environment,
      allowedHosts: ['api.example.test'],
    }),
  });
  const resourcesFor = (
    targetSpec: DeploymentSpec,
  ): ExternalPlatformResources => ({
    maintenanceCapabilityPublicKey: KEY,
    stateWorker: {
      scriptName: externalStateScriptName(targetSpec),
      artifactVersion: `state-${stateDigest[0]}`,
      artifactDigest: stateDigest,
      durableObjectBindings: [],
      namespaceIds: [],
    },
    egressProxy: {
      scriptName: externalEgressProxyScriptName(targetSpec),
      artifactVersion: 'egress-v1',
      artifactDigest: 'b'.repeat(64),
      ...targetFor(targetSpec).outboundPolicy,
    },
  });
  const physicalName = (target: DeploymentSpec) =>
    external ? externalReleaseScriptName(target) : target.scriptName;
  const liveFor = (
    target: DeploymentSpec,
    artifactVersion: string,
    application = EMPTY_APPLICATION,
  ): LiveDeployment => {
    const live = {
      tenantTag: target.tenantTag,
      environment: target.environment,
      scriptName: physicalName(target),
      databaseId: `db-${target.tenantTag}`,
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      plainTextBindings: Object.fromEntries(
        application.vars.map(({ name, value }) => [name, value]),
      ),
      secretNames: [
        'DEPLOYMENT_IDENTITY_SECRET',
        ...(external ? [] : ['MAINTENANCE_ADMIN_SECRET']),
        ...application.secrets.map(({ name }) => name),
      ].sort(),
      r2BucketBindings: application.r2Buckets,
      artifactVersion,
      desiredSpecDigest: deploymentSpecDigest(target),
      schemaVersion: target.schemaVersion,
      maintenance: HEALTHY,
    };
    return {
      ...live,
      providerBindingIdentities: providerBindingIdentitiesForInspection({
        ...live,
        databaseIds: [live.databaseId],
      }),
    };
  };
  const priorRelease: ExternalReleaseSnapshot = {
    physicalScriptName: physicalName(priorSpec),
    specDigest: deploymentSpecDigest(priorSpec),
    artifactVersion: 'v1',
    releaseSchemaVersion: 1,
    application: EMPTY_APPLICATION,
  };
  const priorTarget = targetFor(priorSpec);
  const initial: FleetRecord = {
    ...baseRecord(priorSpec),
    ...(external
      ? {
          activeRelease: priorRelease,
          platformTarget: priorTarget,
          outboundPolicy: priorTarget.outboundPolicy,
          platformResources: resourcesFor(priorSpec),
        }
      : {}),
    ...(input.lagging
      ? {
          schemaVersion: 2,
          platformTarget: {
            ...priorTarget,
            d1SchemaVersion: 2,
            d1SchemaHistoryDigest: 'f'.repeat(64),
          },
        }
      : {}),
  };
  releases.set(physicalName(priorSpec), liveFor(priorSpec, 'v1'));
  routed.set(spec.tenantTag, physicalName(priorSpec));
  if (path === 'platform-only') stateDigest = 'd'.repeat(64);
  const fleetStore = new MemoryFleetStore([initial]);
  const operationStore = new MemoryOperationStore();
  let maintenance = HEALTHY;
  let failure: { operation: string; error: unknown } | undefined;
  const call = (name: string) => {
    if (!fleetStore.held)
      throw new Error(`provider ${name} called outside deployment lease`);
    ops.push(name);
    if (failure?.operation === name) throw failure.error;
  };
  const backend: ProvisioningBackend = {
    kind: external ? 'workers-for-platforms' : 'plain-worker',
    findDatabase: unused,
    ensureDatabase: unused,
    removeTraffic: unused,
    assertTrafficRemoved: unused,
    revokeCredentials: unused,
    deleteWorker: unused,
    assertDatabaseDetached: unused,
    exportDatabase: unused,
    deleteDatabase: unused,
    getDatabase: async (id) => {
      call('getDatabase');
      return { id, name: `database-${id.slice(3)}`, created: false };
    },
    readDeploymentIdentity: async (database) => {
      call('readDeploymentIdentity');
      return database.id.slice(3);
    },
    seedDeploymentIdentity: async (_database, tenant) => {
      call(`seed:${tenant}`);
    },
    applyMigrations: async (_database, migrations) => {
      call(
        migrations === spec.migrations
          ? 'apply:verify'
          : `apply:${migrations.at(-1)?.version}`,
      );
      if (migrations === spec.migrations) {
        if (migrations.some(({ version }) => !ledger.has(version)))
          throw new Error('migration ledger is incomplete');
      } else {
        for (const { version } of migrations) ledger.add(version);
      }
    },
    deployWorker: async (
      target,
      _database,
      _secrets,
      _resources,
      _lease,
      _expected,
      application,
    ) => {
      call('deploy');
      const artifactVersion = `v${target.schemaVersion}`;
      releases.set(
        physicalName(target),
        liveFor(target, artifactVersion, application),
      );
      applications.set(physicalName(target), application ?? EMPTY_APPLICATION);
      return {
        artifactVersion,
        created: true,
        physicalScriptName: physicalName(target),
      };
    },
    inspect: async (target) => {
      call('inspect');
      return releases.get(physicalName(target));
    },
    ensureMaintenance: async (target) => {
      call('maintenance');
      const live = releases.get(physicalName(target));
      if (live) releases.set(physicalName(target), { ...live, maintenance });
      return maintenance;
    },
    promoteWorker: async (target, guard) => {
      call('promote');
      const serving = routed.get(target.tenantTag);
      if (serving && !guard.allowedCurrentScriptNames.includes(serving))
        throw new Error('route changed');
      routed.set(target.tenantTag, physicalName(target));
    },
    attestActiveRoute: async (target) => {
      call('attest');
      const script = routed.get(target.tenantTag);
      const live = script ? releases.get(script) : undefined;
      if (!script || !live) throw new Error('route is absent');
      return {
        specDigest: live.desiredSpecDigest,
        artifactVersion: live.artifactVersion,
        physicalScriptName: script,
        source: external ? 'dispatch-route' : 'workers-deployments',
        observedAt: new Date(NOW).toISOString(),
      };
    },
    ...(external
      ? {
          immutableExternalArtifacts: true as const,
          releaseScriptName: (target: DeploymentSpec) => {
            call('releaseScriptName');
            return physicalName(target);
          },
          describeExternalPlatformTarget: (target: DeploymentSpec) => {
            call('describeTarget');
            return targetFor(target);
          },
          ensurePlatformResources: async (target: DeploymentSpec) => {
            call('platform');
            return {
              resources: resourcesFor(target),
              created: { stateWorker: false, egressProxy: false },
            };
          },
          deleteRetainedRelease: async (
            _target: DeploymentSpec,
            release: ExternalReleaseSnapshot,
          ) => {
            call(`retire:${release.physicalScriptName}`);
            releases.delete(release.physicalScriptName);
          },
        }
      : {}),
  };
  const options = (
    action: FleetMigrationAdvanceAction,
  ): AdvanceFleetMigrationOptions => ({
    operationStore,
    fleetStore,
    backendFor(record) {
      ops.push(`backend:${record.tenantTag}`);
      return backend;
    },
    specFor(record) {
      ops.push(`spec:${record.tenantTag}`);
      return spec;
    },
    secretsFor(record) {
      ops.push(`secrets:${record.tenantTag}`);
      return SECRETS;
    },
    settlementFor() {
      ops.push('settlementFor');
      return {
        settle: async () => {
          call('settle');
        },
      };
    },
    clock: () => NOW,
    action,
  });
  return {
    initial,
    priorSpec,
    priorRelease,
    priorTarget,
    backend,
    fleetStore,
    operationStore,
    ops,
    releases,
    routed,
    applications,
    ledger,
    options,
    targetFor,
    resourcesFor,
    liveFor,
    get spec() {
      return spec;
    },
    set spec(next: DeploymentSpec) {
      spec = next;
    },
    setMaintenance(next: MaintenanceHealth) {
      maintenance = next;
    },
    fail(operation: string, error: unknown) {
      failure = { operation, error };
    },
    clearFailure() {
      failure = undefined;
    },
    start(
      id = uuid(),
      records: readonly FleetRecord[] = [initial],
      canaryTenantTags: readonly string[] = [],
    ) {
      return advanceFleetMigration(
        options({ kind: 'start', operationId: id, records, canaryTenantTags }),
      );
    },
    current() {
      return fleetStore.records.get(
        `${initial.tenantTag}:${initial.environment}`,
      ) as FleetRecord;
    },
  };
}

type World = ReturnType<typeof createWorld>;

async function continueWorld(
  world: World,
  result: FleetMigrationAdvanceResult,
): Promise<FleetMigrationAdvanceResult> {
  return advanceFleetMigration(
    world.options({ kind: 'continue', token: result.token }),
  );
}

async function advanceTo(
  world: World,
  step: FleetMigrationStep,
  result?: FleetMigrationAdvanceResult,
): Promise<FleetMigrationAdvanceResult> {
  let next = result ?? (await world.start());
  for (let count = 0; count < 80; count += 1) {
    const item = world.operationStore.item();
    if (item.plan?.[item.planCursor ?? -1]?.step === step) return next;
    if (next.status !== 'pending') throw new Error(`terminated before ${step}`);
    next = await continueWorld(world, next);
  }
  throw new Error(`did not reach ${step}`);
}

async function drainWorld(
  world: World,
  result?: FleetMigrationAdvanceResult,
): Promise<FleetMigrationAdvanceResult> {
  let next = result ?? (await world.start());
  for (let count = 0; count < 100; count += 1) {
    if (next.status !== 'pending') return next;
    next = await continueWorld(world, next);
  }
  throw new Error('migration did not finish');
}

async function withAdmitted<T>(
  world: World,
  run: (
    admitted: Awaited<ReturnType<typeof admitFleetMigrationItem>>,
  ) => Promise<T>,
): Promise<T> {
  return world.fleetStore.withDeploymentLease(
    world.initial.tenantTag,
    world.initial.environment,
    async (lease) => {
      const options = world.options({ kind: 'continue', token: {} });
      return run(
        await admitFleetMigrationItem(
          {
            store: world.fleetStore,
            lease,
            backendFor: options.backendFor,
            specFor: options.specFor,
            secretsFor: options.secretsFor,
            settlementFor: options.settlementFor,
            clock: () => NOW,
            ordinal: 1,
            attestationOptions: { clock: () => NOW },
          },
          world.initial.tenantTag,
          world.initial.environment,
        ),
      );
    },
  );
}

describe('bounded fleet migration', () => {
  it('start freezes the exact legacy order (dup-canary last-wins, stable equal rank, localeCompare)', async () => {
    const world = createWorld();
    const records = [
      baseRecord(deploymentSpec('gamma')),
      { ...baseRecord(deploymentSpec('cedar')), environment: 'staging' },
      baseRecord(deploymentSpec('birch')),
      baseRecord(deploymentSpec('cedar')),
      baseRecord(deploymentSpec('alpha')),
    ];
    const expected = [
      records[2],
      records[1],
      records[3],
      records[4],
      records[0],
    ].map((record) => copy(record));
    const canaries = ['cedar', 'birch', 'cedar'];
    const action = {
      kind: 'start' as const,
      operationId: uuid(),
      records,
      canaryTenantTags: canaries,
    };
    let idReads = 0;
    Object.defineProperty(action, 'operationId', {
      get: () => {
        expect(++idReads).toBe(1);
        return uuid();
      },
    });
    world.operationStore.beforeLease = () => {
      records.reverse();
      records[0] = baseRecord(deploymentSpec('replaced'));
      canaries.reverse();
    };
    await advanceFleetMigration(world.options(action));
    const items = (
      await readFleetMigrationItemsPage(world.operationStore, {
        operationId: uuid(),
        limit: 100,
      })
    ).items;
    expect(
      items.map(({ tenantTag, environment }) => `${tenantTag}:${environment}`),
    ).toEqual(
      expected.map((record) => `${record?.tenantTag}:${record?.environment}`),
    );
    expect(items.map(({ canaryRank }) => canaryRank)).toEqual([
      1,
      2,
      2,
      undefined,
      undefined,
    ]);
    expect(items.map(({ entryRecordDigest }) => entryRecordDigest)).toEqual(
      expected.map(fleetOperationIntakeDigest),
    );
    expect(world.ops).toEqual([]);
    expect(world.fleetStore.ops).toEqual([]);
  });

  it('start replay converges on the classified outcome; item staging is callback-free', async () => {
    const world = createWorld({ path: 'ready' });
    const first = await world.start();
    expect(first).toMatchObject({ status: 'pending', token: { revision: 1 } });
    expect(world.operationStore.calls).toEqual(['start', 'stage', 'commit']);
    expect(world.ops).toEqual([]);
    expect(await world.start()).toEqual(first);
    expect(world.operationStore.calls).toEqual([
      'start',
      'stage',
      'commit',
      'start',
    ]);
    const admitted = await continueWorld(world, first);
    const calls = [...world.operationStore.calls];
    world.ops.length = 0;
    expect(await world.start()).toEqual(admitted);
    expect(world.operationStore.calls).toEqual([...calls, 'start']);
    expect(world.ops).toEqual([]);
    const completed = await drainWorld(world, admitted);
    const beforeReplay = [...world.operationStore.calls];
    world.ops.length = 0;
    expect(await world.start()).toEqual(completed);
    expect(world.operationStore.calls).toEqual([...beforeReplay, 'start']);
    expect(world.ops).toEqual([]);

    for (const staged of [0, 1]) {
      const interrupted = createWorld();
      const records = [
        interrupted.initial,
        baseRecord(deploymentSpec('other')),
      ];
      interrupted.operationStore.stageLimit = staged;
      await expect(interrupted.start(uuid(), records)).rejects.toThrow(
        'staging interrupted',
      );
      expect(interrupted.operationStore.rows.get(uuid())?.length ?? 0).toBe(
        staged,
      );
      const pending = await advanceFleetMigration(
        interrupted.options({
          kind: 'continue',
          token: { version: 1, operationId: uuid(), revision: 0 },
        }),
      );
      expect(pending).toMatchObject({
        status: 'pending',
        token: { revision: 0 },
      });
      expect(interrupted.ops).toEqual([]);
      await interrupted.start(uuid(), records);
      expect(interrupted.operationStore.rows.get(uuid())).toHaveLength(2);
      expect(interrupted.operationStore.item().status).toBe('pending');
      expect(interrupted.ops).toEqual([]);
    }
    const responseLost = createWorld();
    responseLost.operationStore.loseCommit = 'after';
    await expect(responseLost.start()).rejects.toThrow(
      'progress response lost',
    );
    const replayed = await responseLost.start();
    expect(replayed).toMatchObject({
      status: 'pending',
      token: { revision: 1 },
    });
    expect(
      responseLost.operationStore.calls.filter((call) => call === 'commit'),
    ).toHaveLength(1);
    expect(responseLost.ops).toEqual([]);

    const composed = responseLost.operationStore.commits[0];
    if (!composed) throw new Error('missing committed start');
    await responseLost.operationStore.withAccountOperationLease(
      'migration',
      async (lease) => {
        await expect(lease.commitProgress(composed)).resolves.toEqual(
          composed.runRecord,
        );
        await expect(lease.commitProgress(copy(composed))).resolves.toEqual(
          composed.runRecord,
        );
        await expect(
          lease.commitProgress({
            ...composed,
            runRecord: {
              ...composed.runRecord,
              updatedAt: '2000-01-01T00:00:00.000Z',
            },
          }),
        ).rejects.toThrow('expected revision');
      },
    );
    const empty = createWorld();
    const emptyStart = await empty.start(uuid(), []);
    expect(await continueWorld(empty, emptyStart)).toMatchObject({
      status: 'complete',
      result: { itemCount: 0, completedItemCount: 0 },
    });
    expect(empty.ops).toEqual([]);
    const invalidReplay = createWorld();
    await invalidReplay.start(uuid(), [
      invalidReplay.initial,
      invalidReplay.initial,
    ]);
    const intended = invalidReplay.operationStore.operations.get(uuid());
    const persistedRows = copy(invalidReplay.operationStore.rows.get(uuid()));
    const row = persistedRows?.[1];
    if (!intended || !row) throw new Error('missing replay fixture');
    for (const mutations of [
      { rows: [row, row] },
      { rows: [row], updateRows: [row] },
      { updateRows: [row, row] },
      { updateRows: [{ ...row, rowKind: 'record' as const }] },
    ]) {
      await invalidReplay.operationStore.withAccountOperationLease(
        'migration',
        async (lease) => {
          await expect(
            lease.commitProgress({
              operationId: uuid(),
              expectedRevision: 0,
              runRecord: intended,
              expectedRowWatermarks: { item: 2 },
              ...mutations,
            }),
          ).rejects.toBeInstanceOf(FleetOperationStateError);
        },
      );
      expect(invalidReplay.operationStore.operations.get(uuid())).toEqual(
        intended,
      );
      expect(invalidReplay.operationStore.rows.get(uuid())).toEqual(
        persistedRows,
      );
    }
  });

  it('intake-digest mismatch conflict', async () => {
    const world = createWorld();
    await world.start();
    await expect(
      world.start(uuid(), [{ ...world.initial, artifactVersion: 'changed' }]),
    ).rejects.toThrow('already exists with a different intake');
    await expect(
      world.start(uuid(), [world.initial], ['unmatched']),
    ).rejects.toThrow('already exists with a different intake');
    expect(world.ops).toEqual([]);
    expect(world.operationStore.rows.get(uuid())).toHaveLength(1);
  });

  it('contention under a foreign active migration', async () => {
    const world = createWorld();
    await world.start();
    await expect(world.start(uuid(2))).rejects.toThrow(
      'another fleet migration operation is active for this account',
    );
    expect(world.operationStore.operations.size).toBe(1);
    expect(world.ops).toEqual([]);
  });

  it("an audit operation's token yields a kind error", async () => {
    const world = createWorld();
    const started = await world.start();
    const prior = world.operationStore.operations.get(uuid());
    if (!prior) throw new Error('missing run fixture');
    world.operationStore.operations.set(uuid(), {
      ...prior,
      kind: 'audit',
      progress: { ...prior.progress, kind: 'audit' },
    });
    await expect(continueWorld(world, started)).rejects.toThrow(
      FleetOperationTokenKindError,
    );
    expect(world.ops).toEqual([]);
  });

  it('stale token returns authoritative state, including failed, with zero provider work', async () => {
    const world = createWorld();
    const started = await world.start();
    const admitted = await continueWorld(world, started);
    world.ops.length = 0;
    expect(await continueWorld(world, started)).toEqual(admitted);
    expect(world.ops).toEqual([]);
    await abandonFleetMigrationOperation({
      operationStore: world.operationStore,
      operationId: uuid(),
    });
    world.ops.length = 0;
    expect(await continueWorld(world, started)).toMatchObject({
      status: 'failed',
      failure: { reason: 'operator-abandoned' },
    });
    expect(world.ops).toEqual([]);
  });

  it('future token error', async () => {
    const world = createWorld();
    const started = await world.start();
    await expect(
      advanceFleetMigration(
        world.options({
          kind: 'continue',
          token: { ...started.token, revision: started.token.revision + 1 },
        }),
      ),
    ).rejects.toThrow(FleetOperationTokenFutureError);
    expect(world.ops).toEqual([]);
  });

  it('absent-operation adjudication', async () => {
    const world = createWorld();
    await expect(
      advanceFleetMigration(
        world.options({
          kind: 'continue',
          token: { version: 1, operationId: uuid(), revision: 0 },
        }),
      ),
    ).rejects.toThrow(FleetOperationTokenOperationError);
    expect(world.ops).toEqual([]);
    expect(world.operationStore.operations.size).toBe(0);
  });

  it('operation-store capability error', async () => {
    for (const member of [
      'withAccountOperationLease',
      'readOperationById',
      'readOperationRowsPage',
    ] as const) {
      const world = createWorld();
      const options = world.options({
        kind: 'start',
        operationId: uuid(),
        records: [world.initial],
        canaryTenantTags: [],
      });
      Object.defineProperty(world.operationStore, member, { value: undefined });
      await expect(advanceFleetMigration(options)).rejects.toMatchObject({
        name: 'FleetMigrationAdvanceCapabilityError',
        capability: 'operation-store',
        message: 'fleet migration advance requires an operation store',
      });
      expect(world.operationStore.calls).toEqual([]);
      expect(world.ops).toEqual([]);
    }
    expect(new FleetMigrationAdvanceCapabilityError()).toBeInstanceOf(Error);
  });

  it('operationId validation refusal at start', async () => {
    for (const id of [
      '',
      uuid(0xab).toUpperCase(),
      'not-a-uuid',
      uuid().replace('-4000-', '-1000-'),
    ]) {
      const world = createWorld();
      await expect(world.start(id)).rejects.toThrow(
        'operationId must be a lowercase UUIDv4',
      );
      expect(world.operationStore.calls).toEqual([]);
      expect(world.ops).toEqual([]);
    }
  });

  it('item-bound and intake byte-bound refusals at start', async () => {
    const world = createWorld();
    await expect(
      world.start(
        uuid(),
        Array.from(
          { length: FLEET_OPERATION_ITEM_BOUND + 1 },
          () => world.initial,
        ),
      ),
    ).rejects.toThrow('at most 10000 records');
    const oversized = {
      ...world.initial,
      padding: Array.from({ length: 40 }, () => 'x'.repeat(3000)),
    };
    expect(JSON.stringify(oversized).length).toBeGreaterThan(
      FLEET_OPERATION_RECORD_ROW_BYTE_BOUND,
    );
    await expect(world.start(uuid(), [oversized])).rejects.toThrow(
      'record 1 exceeds the staged row byte bound',
    );
    const medium = {
      ...world.initial,
      padding: Array.from({ length: 25 }, () => 'x'.repeat(3000)),
    };
    const count =
      Math.ceil(
        FLEET_OPERATION_INTAKE_BYTE_BOUND / JSON.stringify(medium).length,
      ) + 1;
    await expect(
      world.start(
        uuid(),
        Array.from({ length: count }, () => medium),
      ),
    ).rejects.toThrow('canonical intake exceeds the intake byte bound');
    await expect(
      world.start(uuid(), [{ ...world.initial, tenantTag: 'INVALID' }]),
    ).rejects.toThrow('deployment identifier grammar');
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    const nested: unknown[] = [];
    let cursor = nested;
    for (let index = 0; index < 65; index += 1) {
      const next: unknown[] = [];
      cursor.push(next);
      cursor = next;
    }
    for (const canaries of [
      null,
      3,
      'cedar',
      [3],
      cyclic,
      nested,
      ['x'.repeat(4097)],
    ]) {
      const options = world.options({
        kind: 'start',
        operationId: uuid(),
        records: [world.initial],
        canaryTenantTags: canaries as readonly string[],
      });
      await expect(advanceFleetMigration(options)).rejects.toThrow(
        /canaryTenantTags/,
      );
    }
    expect(world.operationStore.calls).toEqual([]);
    expect(world.ops).toEqual([]);
  });

  it('the ADMIT call is mutation-free and commits the stored target digest, frozen plan and cursor zero', async () => {
    const world = createWorld();
    world.fleetStore.set({
      ...world.initial,
      schemaVersion: 2,
      artifactVersion: 'stored-v2',
    });
    const started = await world.start();
    const admitted = await continueWorld(world, started);
    expect(admitted).toMatchObject({
      status: 'pending',
      itemOrdinal: 0,
      planCursor: 0,
    });
    expect(world.fleetStore.puts).toEqual([]);
    expect(world.ops).toEqual([
      'backend:cedar',
      'spec:cedar',
      'secrets:cedar',
      'getDatabase',
      'readDeploymentIdentity',
    ]);
    expect(world.operationStore.item()).toMatchObject({
      entryRecordDigest: fleetOperationIntakeDigest(world.initial),
      targetSpecDigest: deploymentSpecDigest(world.spec),
      planCursor: 0,
      status: 'active',
    });
    expect(
      world.operationStore
        .item()
        .plan?.filter(({ step }) => step === 'apply-migrations'),
    ).toEqual([{ step: 'apply-migrations', targetSchemaVersion: 3 }]);
    expect(world.fleetStore.ops.filter((op) => op === 'get')).toHaveLength(1);
  });

  it('retire-pre joins the plan only with both admission conjuncts and its re-run no-ops', async () => {
    const world = createWorld({ external: true });
    world.fleetStore.set({
      ...world.initial,
      retiringRelease: {
        ...world.priorRelease,
        physicalScriptName: 'expired-release',
      },
    });
    const admitted = await continueWorld(world, await world.start());
    expect(world.operationStore.item().plan?.[0]).toEqual({
      step: 'retire-pre',
    });
    world.operationStore.loseCommit = 'before';
    await expect(continueWorld(world, admitted)).rejects.toThrow(
      'progress response lost',
    );
    expect(world.current().retiringRelease).toBeUndefined();
    expect(world.ops).toContain('retire:expired-release');
    world.ops.length = 0;
    const puts = world.fleetStore.puts.length;
    await continueWorld(world, admitted);
    expect(world.ops.some((op) => op.startsWith('retire:'))).toBe(false);
    expect(world.fleetStore.puts).toHaveLength(puts);
    const migrating = createWorld();
    migrating.fleetStore.set({
      ...migrating.initial,
      phase: 'migrating',
      pendingSpecDigest: deploymentSpecDigest(migrating.spec),
      retiringRelease: world.priorRelease,
    });
    await continueWorld(migrating, await migrating.start());
    expect(migrating.operationStore.item().plan?.[0]).toEqual({
      step: 'assert-migrating',
    });
    const noRetirement = createWorld();
    await continueWorld(noRetirement, await noRetirement.start());
    expect(
      noRetirement.operationStore
        .item()
        .plan?.some(({ step }) => step === 'retire-pre'),
    ).toBe(false);
  });

  it('each version-applying occurrence applies exactly its frozen targetSchemaVersion, one per call', async () => {
    const world = createWorld({ schemaVersion: 4 });
    let token = await advanceTo(world, 'apply-migrations');
    expect(
      world.operationStore
        .item()
        .plan?.filter(({ step }) => step === 'apply-migrations'),
    ).toEqual(
      [2, 3, 4].map((targetSchemaVersion) => ({
        step: 'apply-migrations',
        targetSchemaVersion,
      })),
    );
    for (const version of [2, 3, 4]) {
      world.ops.length = 0;
      token = await continueWorld(world, token);
      expect(world.ops.filter((op) => op.startsWith('apply:'))).toEqual([
        `apply:${version}`,
      ]);
      expect(world.current().schemaVersion).toBe(version);
      expect([...world.ledger]).toEqual(
        Array.from({ length: version }, (_, index) => index + 1),
      );
    }
  });

  it('the zero-pending occurrence has no targetSchemaVersion and performs ledger verification', async () => {
    const world = createWorld();
    world.fleetStore.set({ ...world.initial, schemaVersion: 3 });
    world.ledger.add(2);
    world.ledger.add(3);
    const token = await advanceTo(world, 'apply-migrations');
    expect(
      world.operationStore
        .item()
        .plan?.filter(({ step }) => step === 'apply-migrations'),
    ).toEqual([{ step: 'apply-migrations' }]);
    world.ops.length = 0;
    const puts = world.fleetStore.puts.length;
    await continueWorld(world, token);
    expect(world.ops.filter((op) => op.startsWith('apply:'))).toEqual([
      'apply:verify',
    ]);
    expect(world.fleetStore.puts).toHaveLength(puts);
  });

  it('per-step mutations match the table across all three plans, including exact admitted intent bytes', async () => {
    const effects = (ops: readonly string[]) =>
      ops.filter(
        (op) =>
          !/^(backend:|spec:|secrets:)/u.test(op) &&
          ![
            'releaseScriptName',
            'describeTarget',
            'getDatabase',
            'readDeploymentIdentity',
          ].includes(op),
      );
    for (const config of [
      { path: 'ready', external: true },
      { path: 'platform-only', external: true },
      { path: 'full', external: true },
      { path: 'full', external: false },
    ] as const) {
      const world = createWorld(config);
      let token = await continueWorld(world, await world.start());
      const plan = world.operationStore.item().plan;
      if (!plan) throw new Error('missing frozen plan');
      const expected: Partial<Record<FleetMigrationStep, readonly string[]>> = {
        'ready-target-backfill': [],
        'ready-platform-resources': ['platform'],
        'ready-maintenance': ['inspect'],
        'ready-promote': ['promote'],
        'ready-attest-settle': ['settlementFor', 'attest', 'settle'],
        'ready-retire-post': [],
        'admit-migrating': [],
        'assert-migrating': [],
        'platform-only-schema': [],
        'platform-only-resources': ['platform'],
        'platform-only-maintenance': ['inspect', 'maintenance'],
        'platform-only-promote': ['inspect', 'promote'],
        'platform-only-ready': ['inspect', 'settlementFor', 'attest', 'settle'],
        'seed-identity': ['seed:cedar'],
        'migration-schema-applied': [],
        'platform-resources': config.external ? ['platform'] : [],
        'pending-topology': [],
        'deploy-candidate': config.external
          ? ['deploy', 'inspect']
          : ['inspect', 'deploy', 'inspect'],
        'arm-maintenance': ['maintenance'],
        promote: ['inspect', 'promote'],
        'settle-ready': ['inspect', 'settlementFor', 'attest', 'settle'],
        'retire-post': [],
      };
      const expectedPuts: Partial<Record<FleetMigrationStep, number>> = {
        'ready-target-backfill': 0,
        'ready-platform-resources': 0,
        'ready-maintenance': 0,
        'ready-promote': 1,
        'ready-attest-settle': 1,
        'ready-retire-post': 0,
        'admit-migrating': 1,
        'assert-migrating': 0,
        'platform-only-schema': 1,
        'platform-only-resources': 2,
        'platform-only-maintenance': 1,
        'platform-only-promote': 1,
        'platform-only-ready': 1,
        'seed-identity': 0,
        'apply-migrations': 1,
        'migration-schema-applied': config.external ? 1 : 0,
        'platform-resources': config.external ? 2 : 0,
        'pending-topology': config.external ? 1 : 0,
        'deploy-candidate': 2,
        'arm-maintenance': config.external ? 1 : 0,
        promote: config.external ? 1 : 0,
        'settle-ready': 1,
        'retire-post': 0,
      };
      for (const [cursor, entry] of plan.entries()) {
        world.ops.length = 0;
        world.fleetStore.ops.length = 0;
        const puts = world.fleetStore.puts.length;
        const commits = world.operationStore.calls.filter(
          (op) => op === 'commit',
        ).length;
        token = await continueWorld(world, token);
        expect(
          effects(world.ops),
          `${config.path}/${config.external}/${entry.step}`,
        ).toEqual(
          entry.step === 'apply-migrations'
            ? [`apply:${entry.targetSchemaVersion}`]
            : expected[entry.step],
        );
        expect(
          world.fleetStore.puts.length - puts,
          `${config.path}/${config.external}/${entry.step} puts`,
        ).toBe(expectedPuts[entry.step]);
        expect(world.fleetStore.ops.filter((op) => op === 'get')).toHaveLength(
          1,
        );
        expect(
          world.operationStore.calls.filter((op) => op === 'commit'),
        ).toHaveLength(commits + 1);
        expect(world.operationStore.item().planCursor).toBe(cursor + 1);
        if (entry.step === 'admit-migrating' && config.external) {
          const intent = world.current().migrationIntent;
          const common = {
            targetSpecDigest: deploymentSpecDigest(world.spec),
            priorRelease: world.priorRelease,
            priorTarget: world.priorTarget,
            priorOutboundPolicy: world.priorTarget.outboundPolicy,
            subphase: 'planned',
          };
          expect(intent).toStrictEqual(
            config.path === 'platform-only'
              ? {
                  platformOnly: true,
                  ...common,
                  targetRelease: world.priorRelease,
                  target: world.targetFor(world.spec),
                }
              : {
                  ...common,
                  targetRelease: {
                    physicalScriptName: externalReleaseScriptName(world.spec),
                    specDigest: deploymentSpecDigest(world.spec),
                    artifactVersion: 'pending',
                    releaseSchemaVersion: world.spec.schemaVersion,
                    application: EMPTY_APPLICATION,
                  },
                  target: world.targetFor(world.spec),
                },
          );
          expect(Object.hasOwn(intent ?? {}, 'platformOnly')).toBe(
            config.path === 'platform-only',
          );
        }
      }
      expect(world.operationStore.item().status).toBe('complete');
      expect(world.current().phase).toBe('ready');
      expect((await continueWorld(world, token)).status).toBe('complete');
    }
    for (const config of [
      { path: 'full', external: false },
      { path: 'full', external: true },
      { path: 'platform-only', external: true },
    ] as const) {
      const world = createWorld(config);
      const token = await advanceTo(world, 'admit-migrating');
      const cursor = world.operationStore.item().planCursor;
      world.operationStore.loseCommit = 'before';
      await expect(continueWorld(world, token)).rejects.toThrow(
        'progress response lost',
      );
      const migrated = copy(world.current());
      const puts = world.fleetStore.puts.length;
      expect(migrated.phase).toBe('migrating');
      expect(world.operationStore.item().planCursor).toBe(cursor);
      await continueWorld(world, token);
      expect(world.current()).toEqual(migrated);
      expect(world.fleetStore.puts).toHaveLength(puts);
      expect(world.operationStore.item().planCursor).toBe((cursor ?? 0) + 1);
      expect(world.operationStore.calls).not.toContain('fail');
    }
    for (const bounded of [false, true]) {
      const world = createWorld();
      const applicationSecret = 'migration-application-secret';
      world.spec = {
        ...world.spec,
        application: {
          vars: [{ name: 'FEATURE', value: 'enabled' }],
          secrets: [
            {
              name: 'UPSTREAM_KEY',
              valueSha256: createHash('sha256')
                .update(applicationSecret)
                .digest('hex'),
            },
          ],
          r2Buckets: [],
        },
      };
      const optionsFor = world.options;
      world.options = (action) => ({
        ...optionsFor(action),
        secretsFor: () => ({
          ...SECRETS,
          application: { UPSTREAM_KEY: applicationSecret },
        }),
      });
      if (bounded) {
        expect((await drainWorld(world)).status).toBe('complete');
      } else {
        const options = world.options({ kind: 'continue', token: {} });
        await migrateFleet({
          store: world.fleetStore,
          records: [world.initial],
          canaryTenantTags: [],
          backendFor: options.backendFor,
          specFor: options.specFor,
          secretsFor: options.secretsFor,
          settlementFor: options.settlementFor,
          clock: options.clock,
        });
      }
      expect(world.current().phase).toBe('ready');
      expect(world.current().applicationBindings?.vars).toEqual([
        { name: 'FEATURE', value: 'enabled' },
      ]);
      expect(
        world.releases.get(world.spec.scriptName)?.plainTextBindings,
      ).toEqual({
        FEATURE: 'enabled',
      });
      expect(world.ops).toContain('promote');
      expect(world.ops).toContain('settle');
      expect(world.releases.get(world.spec.scriptName)?.secretNames).toContain(
        'UPSTREAM_KEY',
      );
    }
  });

  it('target-drift is classified before non-ready guards and cannot be forged by callback throws', async () => {
    for (const backfillShape of [false, true]) {
      const world = createWorld({ external: backfillShape });
      const admitted = await continueWorld(world, await world.start());
      if (backfillShape) {
        const record = { ...world.current() };
        delete record.platformTarget;
        world.fleetStore.set(record);
        expect(record.platformResources).toBeDefined();
      }
      world.spec = {
        ...world.spec,
        modules: [
          { name: 'worker.js', content: 'export default { changed: true }' },
        ],
      };
      if (backfillShape) {
        world.fleetStore.set({
          ...world.current(),
          desiredSpecDigest: deploymentSpecDigest(world.spec),
        });
      }
      const puts = world.fleetStore.puts.length;
      await expect(continueWorld(world, admitted)).rejects.toThrow(
        'fleet migration target specification changed after admission',
      );
      expect(
        world.operationStore.operations.get(uuid())?.progress.failure,
      ).toEqual({ reason: 'target-drift', itemOrdinal: 0 });
      expect(world.operationStore.item().status).toBe('failed');
      expect(world.fleetStore.puts).toHaveLength(puts);
      expect(
        world.operationStore.calls.filter((op) => op === 'fail'),
      ).toHaveLength(1);
    }
    for (const config of [
      { path: 'full', external: false },
      { path: 'full', external: true },
      { path: 'platform-only', external: true },
    ] as const) {
      const world = createWorld(config);
      const token = await advanceTo(world, 'assert-migrating');
      world.spec = {
        ...world.spec,
        modules: [
          { name: 'worker.js', content: 'export default { changed: true }' },
        ],
      };
      const puts = world.fleetStore.puts.length;
      await expect(continueWorld(world, token)).rejects.toThrow(
        "deployment 'cedar:production' retry uses a different desired specification",
      );
      expect(
        world.operationStore.operations.get(uuid())?.progress.failure,
      ).toEqual({
        reason: 'item-failed',
        itemOrdinal: 0,
      });
      expect(world.fleetStore.puts).toHaveLength(puts);
    }
    for (const thrown of [
      new Error('fleet migration target specification changed after admission'),
      { reason: 'target-drift' },
    ]) {
      const world = createWorld();
      const admitted = await continueWorld(world, await world.start());
      const options = world.options({
        kind: 'continue',
        token: admitted.token,
      });
      await expect(
        advanceFleetMigration({
          ...options,
          specFor: () => {
            throw thrown;
          },
        }),
      ).rejects.toBe(thrown);
      expect(
        world.operationStore.operations.get(uuid())?.progress.failure,
      ).toEqual({ reason: 'item-failed', itemOrdinal: 0 });
    }
  });

  it('a migrating-phase admission resumes both persisted intent kinds and preserves the legacy divergence refusal', async () => {
    for (const path of ['full', 'platform-only'] as const) {
      const world = createWorld({ path, external: true });
      await advanceTo(world, 'assert-migrating');
      const store = new MemoryOperationStore();
      const options = (action: FleetMigrationAdvanceAction) => ({
        ...world.options(action),
        operationStore: store,
      });
      const started = await advanceFleetMigration(
        options({
          kind: 'start',
          operationId: uuid(2),
          records: [world.current()],
          canaryTenantTags: [],
        }),
      );
      world.fleetStore.puts.length = 0;
      await advanceFleetMigration(
        options({ kind: 'continue', token: started.token }),
      );
      const item = store.item(uuid(2));
      expect(item.plan?.[0]).toEqual({ step: 'assert-migrating' });
      expect(item.plan?.some(({ step }) => step === 'admit-migrating')).toBe(
        false,
      );
      expect(
        item.plan?.some(({ step }) => step.startsWith('platform-only-')),
      ).toBe(path === 'platform-only');
      expect(world.fleetStore.puts).toEqual([]);
      const current = world.current();
      if (!current.migrationIntent) throw new Error('missing migration intent');
      world.fleetStore.set({
        ...current,
        migrationIntent: {
          ...current.migrationIntent,
          ...(path === 'platform-only'
            ? {
                target: {
                  ...current.migrationIntent.target,
                  stateArtifactDigest: 'f'.repeat(64),
                },
              }
            : {}),
        },
        ...(path === 'full'
          ? {
              pendingRelease: {
                ...(current.pendingRelease as ExternalReleaseSnapshot),
                physicalScriptName: 'foreign-pending',
              },
            }
          : {}),
      });
      const failedStore = new MemoryOperationStore();
      const failedOptions = (action: FleetMigrationAdvanceAction) => ({
        ...world.options(action),
        operationStore: failedStore,
      });
      const pending = await advanceFleetMigration(
        failedOptions({
          kind: 'start',
          operationId: uuid(3),
          records: [world.current()],
          canaryTenantTags: [],
        }),
      );
      await expect(
        advanceFleetMigration(
          failedOptions({ kind: 'continue', token: pending.token }),
        ),
      ).rejects.toThrow(
        'migration retry uses a different desired specification',
      );
      expect(failedStore.item(uuid(3))).toMatchObject({ status: 'failed' });
      expect(failedStore.item(uuid(3)).plan).toBeUndefined();
      expect(world.fleetStore.puts).toEqual([]);
    }
  });

  it('retire-post follows the ready commit and refuses a restored migrating carrier', async () => {
    for (const restored of [false, true]) {
      const world = createWorld({ external: true });
      world.fleetStore.set({
        ...world.initial,
        rollbackRelease: {
          ...world.priorRelease,
          physicalScriptName: 'expired-rollback',
        },
      });
      const beforeCommit = await advanceTo(world, 'settle-ready');
      const migrating = copy(world.current());
      const committed = await continueWorld(world, beforeCommit);
      expect(world.current().phase).toBe('ready');
      expect(
        world.operationStore.item().plan?.[
          world.operationStore.item().planCursor ?? -1
        ]?.step,
      ).toBe('retire-post');
      if (restored) world.fleetStore.set(migrating);
      world.ops.length = 0;
      const puts = world.fleetStore.puts.length;
      if (restored) {
        await expect(continueWorld(world, committed)).rejects.toThrow(
          'fleet migration item no longer matches its frozen plan',
        );
        expect(world.ops.some((op) => op.startsWith('retire:'))).toBe(false);
        expect(world.fleetStore.puts).toHaveLength(puts);
        expect(world.operationStore.item().status).toBe('failed');
      } else {
        await continueWorld(world, committed);
        expect(world.ops).toContain('retire:expired-rollback');
        expect(world.current().retiringRelease).toBeUndefined();
        expect(world.operationStore.item().status).toBe('complete');
      }
    }
  });

  it('terminal re-runs short-circuit exactly the converged projection and preserve its documented accepted classes', async () => {
    async function terminal(
      world: World,
      step: 'settle-ready' | 'platform-only-ready',
    ) {
      const token = await advanceTo(world, step);
      const item = world.operationStore.item();
      world.operationStore.loseCommit = 'before';
      await expect(continueWorld(world, token)).rejects.toThrow(
        'progress response lost',
      );
      expect(world.current().phase).toBe('ready');
      expect(world.operationStore.item()).toEqual(item);
      return { token, item };
    }
    async function directProjection(
      world: World,
      item: FleetMigrationItem,
      accepts: boolean,
    ) {
      await withAdmitted(world, async ({ admitted, reread }) => {
        if (!item.plan || item.planCursor === undefined)
          throw new Error('missing terminal cursor');
        world.ops.length = 0;
        const platformOnly =
          item.plan[item.planCursor]?.step === 'platform-only-ready';
        if (!accepts) {
          await expect(
            assertFleetMigrationPlanCompatibility(
              admitted,
              { plan: item.plan, planCursor: item.planCursor },
              reread,
            ),
          ).rejects.toThrow(
            'fleet migration item no longer matches its frozen plan',
          );
          expect(world.ops).toEqual([]);
          if (platformOnly) return;
        }
        if (!accepts)
          world.backend.inspect = async () => {
            world.ops.push('projection-fallback');
            throw new Error('terminal projection did not accept');
          };
        const result = executeNextMigrationStep(
          admitted,
          item.plan,
          item.planCursor,
          { entry: reread, current: reread },
        );
        if (accepts) {
          const terminalResult = await result;
          expect(terminalResult.record).toBe(reread);
          expect(terminalResult.done).toBe(platformOnly);
          expect(terminalResult.resultOnDone).toBe(
            platformOnly ? reread : undefined,
          );
          expect(world.ops).toEqual([]);
        } else
          await expect(result).rejects.toThrow(
            'terminal projection did not accept',
          );
      });
    }
    for (const path of ['full', 'platform-only'] as const) {
      const world = createWorld({
        path,
        external: true,
        lagging: path === 'platform-only',
      });
      const { token, item } = await terminal(
        world,
        path === 'full' ? 'settle-ready' : 'platform-only-ready',
      );
      if (path === 'platform-only') {
        expect(
          world.current().activeRelease?.releaseSchemaVersion,
        ).toBeLessThan(world.current().schemaVersion);
        expect(world.current().platformTarget?.d1SchemaVersion).toBe(2);
        expect(world.targetFor(world.spec).d1SchemaVersion).toBe(1);
      }
      await directProjection(world, item, true);
      world.ops.length = 0;
      const puts = world.fleetStore.puts.length;
      await continueWorld(world, token);
      expect(
        world.ops.filter(
          (op) =>
            !/^(backend:|spec:|secrets:)/u.test(op) &&
            ![
              'releaseScriptName',
              'describeTarget',
              'getDatabase',
              'readDeploymentIdentity',
            ].includes(op),
        ),
      ).toEqual([]);
      expect(world.fleetStore.puts).toHaveLength(puts);
      expect(world.operationStore.item().planCursor).toBe(
        (item.planCursor ?? 0) + 1,
      );
    }
    const fullDowngrade = createWorld({ external: true });
    const fullTerminal = await terminal(fullDowngrade, 'settle-ready');
    fullDowngrade.fleetStore.set({
      ...fullDowngrade.current(),
      schemaVersion: 4,
    });
    await expect(
      continueWorld(fullDowngrade, fullTerminal.token),
    ).rejects.toThrow('schema downgrade refused for cedar:production');

    const premature = createWorld({ path: 'platform-only', lagging: true });
    const beforeAdmission = await continueWorld(
      premature,
      await premature.start(),
    );
    premature.fleetStore.set({
      ...premature.current(),
      platformTarget: {
        ...premature.targetFor(premature.spec),
        d1SchemaVersion: 2,
        d1SchemaHistoryDigest: 'f'.repeat(64),
      },
    });
    const prematurePuts = premature.fleetStore.puts.length;
    await expect(continueWorld(premature, beforeAdmission)).rejects.toThrow(
      'fleet migration item no longer matches its frozen plan',
    );
    expect(premature.fleetStore.puts).toHaveLength(prematurePuts);

    const corruptions: readonly [
      string,
      (record: FleetRecord) => FleetRecord,
    ][] = [
      [
        'policy',
        (record) => ({
          ...record,
          outboundPolicy: {
            ...(record.outboundPolicy as NonNullable<
              FleetRecord['outboundPolicy']
            >),
            policyHosts: ['wrong.example.test'],
          },
        }),
      ],
      ['schema', (record) => ({ ...record, schemaVersion: 2 })],
      [
        'pending digest',
        (record) => ({ ...record, pendingSpecDigest: 'f'.repeat(64) }),
      ],
      [
        'pending artifact',
        (record) => ({ ...record, pendingArtifactVersion: 'unsettled' }),
      ],
      [
        'pending release',
        (record) => ({ ...record, pendingRelease: record.activeRelease }),
      ],
      [
        'prior release',
        (record) => ({
          ...record,
          migrationPriorRelease: record.activeRelease,
        }),
      ],
      [
        'cleared target',
        (record) => {
          const copy = { ...record };
          delete copy.platformTarget;
          return copy;
        },
      ],
      [
        'active name',
        (record) => ({
          ...record,
          activeRelease: {
            ...(record.activeRelease as ExternalReleaseSnapshot),
            physicalScriptName: 'foreign-active',
          },
        }),
      ],
      [
        'active schema',
        (record) => ({
          ...record,
          activeRelease: {
            ...(record.activeRelease as ExternalReleaseSnapshot),
            releaseSchemaVersion: 1,
          },
        }),
      ],
      [
        'bindings',
        (record) => ({
          ...record,
          applicationBindings: {
            ...EMPTY_APPLICATION,
            vars: [{ name: 'WRONG', value: '1' }],
          },
        }),
      ],
    ];
    for (const [_label, corrupt] of corruptions) {
      const world = createWorld({ external: true });
      const { item } = await terminal(world, 'settle-ready');
      world.fleetStore.set(corrupt(world.current()));
      await directProjection(world, item, false);
    }
    const wrongPoPolicy = createWorld({ path: 'platform-only', lagging: true });
    const poTerminal = await terminal(wrongPoPolicy, 'platform-only-ready');
    const corruptPolicy = corruptions[0]?.[1];
    if (!corruptPolicy) throw new Error('missing policy corruption');
    wrongPoPolicy.fleetStore.set(corruptPolicy(wrongPoPolicy.current()));
    await directProjection(wrongPoPolicy, poTerminal.item, false);

    for (const foreignRetirement of [false, true]) {
      const world = createWorld({ external: true });
      const { token, item } = await terminal(world, 'settle-ready');
      const current = world.current();
      if (!current.platformResources)
        throw new Error('missing platform resources');
      world.fleetStore.set(
        foreignRetirement
          ? {
              ...current,
              retiringRelease: {
                ...world.priorRelease,
                physicalScriptName: 'foreign-retiring',
              },
            }
          : {
              ...current,
              platformResources: {
                ...current.platformResources,
                stateWorker: {
                  ...current.platformResources.stateWorker,
                  artifactVersion: 'corrupted-spread-field',
                },
              },
            },
      );
      await directProjection(world, item, true);
      const after = await continueWorld(world, token);
      if (foreignRetirement) {
        await continueWorld(world, after);
        expect(world.ops).toContain('retire:foreign-retiring');
      }
    }

    const resourceWorld = createWorld();
    resourceWorld.spec = {
      ...resourceWorld.spec,
      application: { vars: [], secrets: [], r2Buckets: [{ name: 'ASSETS' }] },
    };
    const resource = {
      name: 'ASSETS',
      bucketName: 'owned-bucket',
      jurisdiction: 'default' as const,
      state: 'created' as const,
      reservationNonce: 'test-reservation',
      creationDate: '2026-08-01T00:00:00.000Z',
    };
    resourceWorld.fleetStore.set({
      ...resourceWorld.initial,
      applicationResources: [resource],
      applicationBindings: applicationBindingTopology(resourceWorld.spec, [
        resource,
      ]),
    });
    const { item: resourceItem } = await terminal(
      resourceWorld,
      'settle-ready',
    );
    const resourceRecord = resourceWorld.current();
    resourceWorld.fleetStore.set({
      ...resourceRecord,
      applicationResources: [{ ...resource, bucketName: 'foreign-bucket' }],
      applicationBindings: {
        ...EMPTY_APPLICATION,
        r2Buckets: [
          {
            name: resource.name,
            bucketName: 'foreign-bucket',
            jurisdiction: resource.jurisdiction,
          },
        ],
      },
    });
    await directProjection(resourceWorld, resourceItem, true);

    const historyWorld = createWorld();
    const history = [
      { tag: 'v1', newClasses: ['Runner'] },
      { tag: 'v2', newClasses: ['Second'] },
      { tag: 'v3', newClasses: ['Third'] },
    ];
    historyWorld.spec = {
      ...historyWorld.spec,
      previousDurableObjectTag: 'v1',
      durableObjectMigrations: history,
    };
    historyWorld.fleetStore.set({
      ...historyWorld.initial,
      durableObjectTag: 'v1',
      durableObjectMigrationHistory: history.slice(0, 1),
      durableObjectMigrationHistoryDigest: durableObjectMigrationHistoryDigest(
        history.slice(0, 1),
      ),
    });
    const { item: historyItem } = await terminal(historyWorld, 'settle-ready');
    historyWorld.fleetStore.set({
      ...historyWorld.current(),
      durableObjectTag: 'v1',
      durableObjectMigrationHistory: history.slice(0, 1),
      durableObjectMigrationHistoryDigest: durableObjectMigrationHistoryDigest(
        history.slice(0, 1),
      ),
    });
    await directProjection(historyWorld, historyItem, true);

    const reordered = createWorld({ external: true });
    const reorderedTerminal = await terminal(reordered, 'settle-ready');
    reordered.fleetStore.transformRead = (record) => {
      const fresh = copy(record);
      return {
        ...fresh,
        platformTarget: Object.fromEntries(
          Object.entries(fresh.platformTarget ?? {}).reverse(),
        ) as unknown as ExternalPlatformTargetDescription,
      };
    };
    await withAdmitted(reordered, async ({ plan, reread }) => {
      expect(reread).not.toBe(reordered.current());
      expect(plan.some(({ step }) => step.startsWith('platform-only-'))).toBe(
        true,
      );
    });
    await expect(
      continueWorld(reordered, reorderedTerminal.token),
    ).rejects.toThrow('fleet migration item no longer matches its frozen plan');
  });
});
