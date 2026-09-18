// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { applicationBindingTopology } from '../src/application-bindings.js';
import type { FinalizedOrdinaryStateProvider } from '../src/backend-switch.js';
import {
  admitFleetMigrationItem,
  assertFleetMigrationPlanCompatibility,
  assertMigratingCarrierState,
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
  canonicalFleetOperationBytes,
  FLEET_OPERATION_INTAKE_BYTE_BOUND,
  FLEET_OPERATION_ITEM_BOUND,
  FLEET_OPERATION_RECORD_ROW_BYTE_BOUND,
  FLEET_OPERATION_SINGLE_UPDATE_ROW_MESSAGE,
  FLEET_OPERATION_STAGE_BATCH_STATEMENTS,
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
  fleetOperationOtherKindMessage,
  fleetOperationPageLimit,
  fleetOperationRunRecordFromUnknown,
  fleetOperationStagedRowFromUnknown,
  fleetOperationWatermarkRunMessage,
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
  BridgeMutationPlan,
  BridgeSnapshot,
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

function payloadBytes(row: FleetOperationStagedRow): string {
  return row.rowKind === 'record'
    ? canonicalFleetOperationBytes(row.payload)
    : JSON.stringify(row.payload);
}

class MemoryOperationStore implements FleetOperationStore {
  readonly operations = new Map<string, FleetOperationRunRecord>();
  readonly rows = new Map<string, FleetOperationStagedRow[]>();
  readonly digests = new Map<string, string>();
  readonly heads = new Map<FleetOperationKind, string>();
  readonly locked = new Set<FleetOperationKind>();
  readonly calls: string[] = [];
  readonly commits: Parameters<FleetOperationLease['commitProgress']>[0][] = [];
  readonly failures: Parameters<FleetOperationLease['failOperation']>[0][] = [];
  loseLease = false;
  loseCommit: 'before' | 'after' | undefined;
  stageLimit: number | undefined;
  beforeLease: (() => void) | undefined;
  beforeCommit:
    | ((input: Parameters<FleetOperationLease['commitProgress']>[0]) => void)
    | undefined;
  beforePage: (() => void) | undefined;
  beforeFail: (() => void) | undefined;
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
          let batchBefore: FleetOperationStagedRow[] = [];
          for (const [index, row] of input.rows.entries()) {
            if (index % FLEET_OPERATION_STAGE_BATCH_STATEMENTS === 0) {
              batchBefore = [...(this.rows.get(input.operationId) ?? [])];
            }
            if (index === this.stageLimit) {
              this.stageLimit = undefined;
              throw new Error('staging interrupted');
            }
            if (row.ordinal <= (previous.get(row.rowKind) ?? -1))
              throw new Error('staging is not ordered');
            previous.set(row.rowKind, row.ordinal);
            const validated = this.validateRow(row);
            const rows = this.rows.get(input.operationId) ?? [];
            const existing = rows.find(
              (prior) =>
                prior.rowKind === row.rowKind && prior.ordinal === row.ordinal,
            );
            if (
              existing &&
              payloadBytes(existing) !== payloadBytes(validated)
            ) {
              this.rows.set(input.operationId, batchBefore);
              throw new Error('immutable operation row payload differs');
            }
            if (!existing) rows.push(validated);
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
                fleetOperationWatermarkRunMessage(
                  kind as FleetOperationRowKind,
                ),
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
            for (const row of updates) {
              if (
                !persistedRows.some(
                  (prior) =>
                    prior.rowKind === row.rowKind &&
                    prior.ordinal === row.ordinal,
                )
              )
                throw conflict(input.operationId);
            }
            for (const row of rows) {
              const persisted = persistedRows.find(
                (prior) =>
                  prior.rowKind === row.rowKind &&
                  prior.ordinal === row.ordinal,
              );
              if (persisted && payloadBytes(persisted) !== payloadBytes(row))
                throw new Error('immutable operation row payload differs');
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
              else if (payloadBytes(persisted) !== payloadBytes(row))
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
          if (prior.kind !== kind) {
            throw new Error(fleetOperationOtherKindMessage(input.operationId));
          }
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
          this.beforeFail?.();
          await assertOwned();
          this.calls.push('fail');
          this.failures.push(copy(input));
          if ((input.updateRows?.length ?? 0) > 1)
            throw new Error(FLEET_OPERATION_SINGLE_UPDATE_ROW_MESSAGE);
          const prior = this.operations.get(input.operationId);
          if (!prior)
            throw new Error(`no fleet operation '${input.operationId}'`);
          if (prior.kind !== kind) {
            throw new Error(fleetOperationOtherKindMessage(input.operationId));
          }
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
    const limit = fleetOperationPageLimit(input.limit);
    const qualifying = (this.rows.get(input.operationId) ?? [])
      .filter(
        (row) =>
          row.rowKind === input.rowKind &&
          row.ordinal > (input.afterOrdinal ?? -1),
      )
      .sort((a, b) => a.ordinal - b.ordinal);
    const rows = qualifying.slice(0, limit).map(copy);
    return {
      rows: this.reversePages ? rows.reverse() : rows,
      done: qualifying.length <= limit,
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
    namespace?: boolean;
  } = {},
) {
  const path = input.path ?? 'full';
  const external = input.external ?? path === 'platform-only';
  const priorSpec = {
    ...deploymentSpec(input.tenant ?? 'cedar', 1, external),
    ...(input.namespace
      ? { durableObjectBindings: [{ name: 'STATE', className: 'Runner' }] }
      : {}),
  };
  let spec =
    path === 'full'
      ? {
          ...deploymentSpec(
            priorSpec.tenantTag,
            input.schemaVersion ?? 3,
            external,
          ),
          durableObjectBindings: priorSpec.durableObjectBindings,
        }
      : priorSpec;
  let stateDigest = 'a'.repeat(64);
  let namespaceId = 'namespace-original';
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
      durableObjectBindings: targetSpec.durableObjectBindings.map(
        (binding) => ({ ...binding, namespaceId }),
      ),
      namespaceIds: input.namespace ? [namespaceId] : [],
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
      durableObjectBindings: target.durableObjectBindings.map((binding) => ({
        ...binding,
        namespaceId,
        scriptName: externalStateScriptName(target),
      })),
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
    setNamespace(next: string) {
      namespaceId = next;
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

function finalizedWorld(path: 'ready' | 'platform-only' | 'full' = 'full') {
  const world = createWorld({ path, external: true });
  const history = [{ tag: 'state-v2', newClasses: ['StateV2'] }];
  const target = {
    ...world.targetFor(world.spec),
    stateDurableObjectTag: 'state-v2',
    stateDurableObjectHistoryDigest:
      durableObjectMigrationHistoryDigest(history),
  };
  const bridge: BridgeSnapshot = {
    scriptName: world.initial.scriptName,
    artifactVersion: 'state-a',
    artifactDigest: world.priorTarget.stateArtifactDigest,
    databaseId: world.initial.databaseId,
    durableObjectBindings: [],
    namespaceIds: [],
    secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
    stateOnly: true,
    publicRouteAttached: false,
  };
  const resources = world.initial.platformResources;
  if (!resources) throw new Error('missing finalized resources');
  const initial = {
    ...world.initial,
    durableObjectMigrationHistory: [],
    durableObjectMigrationHistoryDigest: durableObjectMigrationHistoryDigest(
      [],
    ),
    ...(path === 'ready' ? { platformTarget: target } : {}),
    platformResources: {
      ...resources,
      stateWorker: {
        ...resources.stateWorker,
        scriptName: bridge.scriptName,
        plane: 'ordinary' as const,
      },
    },
    backendSwitchIntent: {
      kind: 'backend-switch' as const,
      tenantTag: world.initial.tenantTag,
      environment: world.initial.environment,
      prior: {
        scriptName: world.initial.scriptName,
        artifactVersion: 'plain-v1',
        specDigest: world.initial.desiredSpecDigest,
        databaseId: world.initial.databaseId,
        databaseName: world.initial.databaseName,
        durableObjectBindings: [],
        namespaceIds: [],
        secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
        applicationResources: [],
        customDomain: {
          id: 'domain-cedar',
          hostname: world.initial.routeHostname as string,
        },
      },
      targetSpecDigest: world.initial.desiredSpecDigest,
      targetApplication: EMPTY_APPLICATION,
      target: world.priorTarget,
      rollbackUntil: '2026-08-20T00:00:00.000Z',
      subphase: 'finalized' as const,
      bridge,
    },
  };
  world.fleetStore.set(initial);
  const plan: BridgeMutationPlan = {
    artifactDigest: target.stateArtifactDigest,
    durableObjectMigrations: history,
    targetDurableObjectTag: target.stateDurableObjectTag,
    secretNames: bridge.secretNames,
    mutationDigest: 'e'.repeat(64),
  };
  const provider: FinalizedOrdinaryStateProvider = {
    describeFinalizedBridgeTarget() {
      world.ops.push('finalizedTarget');
      return target;
    },
    describeFinalizedState() {
      world.ops.push('finalizedPlan');
      return plan;
    },
    async assertFinalizedState() {
      world.ops.push('finalizedAssert');
    },
    async ensureFinalizedState() {
      world.ops.push('finalizedEnsure');
      return {
        ...bridge,
        artifactVersion: 'state-v2',
        artifactDigest: target.stateArtifactDigest,
      };
    },
    async commitFinalizedOwnership({
      currentRecord,
      bridge: nextBridge,
      target: nextTarget,
    }) {
      world.ops.push('finalizedCommit');
      const prior = currentRecord.platformResources;
      if (!prior) throw new Error('missing finalized resources');
      return {
        ...currentRecord,
        platformResources: {
          ...prior,
          stateWorker: {
            ...prior.stateWorker,
            artifactVersion: nextBridge.artifactVersion,
            artifactDigest: nextBridge.artifactDigest,
            durableObjectTag: nextTarget.stateDurableObjectTag,
            durableObjectBindings: nextBridge.durableObjectBindings,
            namespaceIds: nextBridge.namespaceIds,
          },
        },
      };
    },
  };
  const options = world.options;
  world.options = (action) => ({
    ...options(action),
    finalizedStateProviderFor() {
      world.ops.push('finalizedFor');
      return provider;
    },
  });
  return { world, provider, target };
}

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
            finalizedStateProviderFor: options.finalizedStateProviderFor,
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

function migrateWorld(world: World) {
  const options = world.options({ kind: 'continue', token: {} });
  return migrateFleet({
    ...options,
    store: world.fleetStore,
    records: [world.current()],
    canaryTenantTags: [],
  });
}

function expectItemFailure(world: World, ordinal = 0) {
  expect(world.operationStore.item(uuid(), ordinal).status).toBe('failed');
  expect(world.operationStore.operations.get(uuid())).toMatchObject({
    state: 'failed',
    progress: { failure: { reason: 'item-failed', itemOrdinal: ordinal } },
  });
  expect(
    world.operationStore.calls.filter((call) => call === 'fail'),
  ).toHaveLength(1);
}

function armOptions(
  world: World,
  extra: Partial<AdvanceFleetMigrationOptions>,
): void {
  const base = world.options;
  world.options = (action) => ({ ...base(action), ...extra });
}

/**
 * A start driven through `world.options`, so an `armOptions` override reaches
 * it. `world.start()` calls the module-local `options` const instead, which no
 * override can see.
 */
function armedStart(
  world: World,
  id = uuid(),
  records: readonly FleetRecord[] = [world.initial],
): Promise<FleetMigrationAdvanceResult> {
  return advanceFleetMigration(
    world.options({
      kind: 'start',
      operationId: id,
      records,
      canaryTenantTags: [],
    }),
  );
}

function providerMutations(world: World) {
  return world.ops.filter((op) =>
    /^(apply:|seed:|deploy$|maintenance$|promote$|platform$|settle$|retire:)/u.test(
      op,
    ),
  );
}

async function loseStepResponse(
  world: World,
  result: FleetMigrationAdvanceResult,
) {
  const item = world.operationStore.item();
  world.operationStore.loseCommit = 'before';
  await expect(continueWorld(world, result)).rejects.toThrow(
    'progress response lost',
  );
  expect(world.operationStore.item()).toEqual(item);
  return item;
}

describe('migration operation fake guarded progress contract', () => {
  it.each([
    'missing-operation',
    'watermark',
    'other-record',
    'different-row',
    'missing-row',
    'converged',
  ] as const)('orders the %s convergence identity', async (variant) => {
    const world = createWorld();
    await world.start();
    const initial = copy(world.operationStore.operations.get(uuid()));
    const row = copy(world.operationStore.rows.get(uuid())?.[0]);
    if (!initial || !row) throw new Error('missing convergence fixture');
    const intended = {
      ...initial,
      progress: {
        ...initial.progress,
        revision: initial.progress.revision + 1,
      },
    };
    const different = {
      ...row,
      payload: { ...row.payload, tenantTag: 'other' },
    };
    const store = new MemoryOperationStore();
    if (variant !== 'missing-operation') {
      store.operations.set(
        uuid(),
        variant === 'other-record'
          ? { ...intended, state: 'failed' }
          : intended,
      );
    }
    store.rows.set(
      uuid(),
      variant === 'missing-row'
        ? []
        : [variant === 'converged' ? row : different],
    );
    const operationsBefore = structuredClone([...store.operations]);
    const rowsBefore = structuredClone([...store.rows]);
    await store.withAccountOperationLease('migration', async (lease) => {
      const commit = lease.commitProgress({
        operationId: uuid(),
        expectedRevision: initial.progress.revision,
        runRecord: intended,
        updateRows:
          variant === 'different-row'
            ? [
                { ...row, ordinal: 1, payload: { ...row.payload, ordinal: 1 } },
                row,
              ]
            : [row],
        expectedRowWatermarks: {
          item: variant === 'watermark' ? 2 : variant === 'missing-row' ? 0 : 1,
        },
      });
      if (variant === 'converged') {
        await expect(commit).resolves.toEqual(intended);
      } else {
        const message =
          variant === 'missing-operation'
            ? `no fleet operation '${uuid()}'`
            : variant === 'different-row'
              ? divergence(uuid()).message
              : conflict(uuid()).message;
        await expect(commit).rejects.toBeInstanceOf(Error);
        await expect(commit).rejects.toHaveProperty('message', message);
      }
    });
    expect([...store.operations]).toEqual(operationsBefore);
    expect([...store.rows]).toEqual(rowsBefore);
  });

  it.each([
    { method: 'finalizeOperation', state: 'running' },
    { method: 'finalizeOperation', state: 'finalized' },
    { method: 'finalizeOperation', state: 'failed' },
    { method: 'failOperation', state: 'running' },
    { method: 'failOperation', state: 'finalized' },
    { method: 'failOperation', state: 'failed' },
  ] as const)('$method refuses a foreign-kind $state record using the captured lease', async ({
    method,
    state,
  }) => {
    const world = createWorld();
    await world.start();
    const store = world.operationStore;
    const initial = copy(store.operations.get(uuid()));
    if (!initial) throw new Error('missing terminal fixture');
    const operationId = uuid();
    const source =
      state === 'running'
        ? initial
        : {
            ...initial,
            progress: {
              ...initial.progress,
              revision: initial.progress.revision + 1,
            },
          };
    store.heads.set('audit', uuid(991));
    store.operations.set(
      operationId,
      fleetOperationRunRecordFromUnknown({
        ...source,
        state,
        progress: {
          ...source.progress,
          ...(state === 'failed'
            ? { failure: { reason: 'operator-abandoned' } }
            : {}),
          ...(state === 'finalized' ? { completedItemCount: 1 } : {}),
        },
      }),
    );
    const operationsBefore = structuredClone([...store.operations]);
    const rowsBefore = structuredClone([...store.rows]);
    const headsBefore = [...store.heads];
    await store.withAccountOperationLease('audit', async (lease) => {
      const runRecord = fleetOperationRunRecordFromUnknown({
        ...source,
        kind: 'audit',
        state: method === 'finalizeOperation' ? 'finalized' : 'failed',
        progress: {
          kind: 'audit',
          revision: initial.progress.revision + 1,
          stage: { step: 'finalize' },
          generation: 1,
          auditTimeMs: 0,
          staleAfterMs: 60_000,
          recordCount: 0,
          findingCount: 0,
          factCount: 0,
          ...(method === 'failOperation'
            ? { failure: { reason: 'operator-abandoned' } }
            : {}),
        },
      });
      const input = {
        operationId,
        expectedRevision: initial.progress.revision,
        runRecord,
      };
      const result =
        method === 'finalizeOperation'
          ? lease.finalizeOperation({ ...input, expectedRowCounts: {} })
          : lease.failOperation(input);
      await expect(result).rejects.toBeInstanceOf(Error);
      await expect(result).rejects.toHaveProperty(
        'message',
        fleetOperationOtherKindMessage(operationId),
      );
      expect([...store.operations]).toEqual(operationsBefore);
      expect([...store.rows]).toEqual(rowsBefore);
      expect([...store.heads]).toEqual(headsBefore);
    });
  });

  it('refuses missing updates and different immutable bytes before sibling writes, and accepts exact retries', async () => {
    const world = createWorld();
    await world.start(uuid(), [world.initial, world.initial]);
    const initial = copy(world.operationStore.operations.get(uuid()));
    const staged = copy(world.operationStore.rows.get(uuid()));
    const row = staged?.[0];
    const secondRow = staged?.[1];
    if (!initial || !row || !secondRow)
      throw new Error('missing commit fixture');
    const intended = {
      ...initial,
      progress: {
        ...initial.progress,
        revision: initial.progress.revision + 1,
      },
    };
    const different = {
      ...row,
      payload: { ...row.payload, tenantTag: 'other' },
    };
    const updated = {
      ...secondRow,
      payload: { ...secondRow.payload, tenantTag: 'updated' },
    };
    const sibling = {
      ...row,
      ordinal: 2,
      payload: { ...row.payload, ordinal: 2 },
    };
    const missing = {
      ...row,
      ordinal: 3,
      payload: { ...row.payload, ordinal: 3 },
    };
    for (const variant of ['missing-update', 'different-insert', 'exact']) {
      const store = new MemoryOperationStore();
      store.operations.set(uuid(), copy(initial));
      store.heads.set('migration', uuid());
      store.rows.set(uuid(), copy([row, secondRow]));
      const input = {
        operationId: uuid(),
        expectedRevision: initial.progress.revision,
        runRecord: intended,
        rows: [sibling, variant === 'different-insert' ? different : row],
        updateRows:
          variant === 'missing-update' ? [updated, missing] : [updated],
        expectedRowWatermarks: { item: 1 },
      };
      await store.withAccountOperationLease('migration', async (lease) => {
        const result = await lease
          .commitProgress(input)
          .catch((error: unknown) => error);
        if (variant === 'exact') {
          expect(store.operations.get(uuid())).toEqual(intended);
          expect(store.rows.get(uuid())).toEqual([row, updated, sibling]);
          expect(result).toEqual(intended);
          expect(await lease.commitProgress(copy(input))).toEqual(intended);
          expect(store.operations.get(uuid())).toEqual(intended);
          expect(store.rows.get(uuid())).toEqual([row, updated, sibling]);
        } else {
          expect(store.operations.get(uuid())).toEqual(initial);
          expect(store.rows.get(uuid())).toEqual([row, secondRow]);
          expect(store.heads.get('migration')).toBe(uuid());
          expect(result).toBeInstanceOf(Error);
          if (variant === 'missing-update') {
            expect(result).toHaveProperty('message', conflict(uuid()).message);
          }
        }
      });
    }
    for (const state of ['running', 'failed'] as const) {
      const store = new MemoryOperationStore();
      const persisted = { ...intended, state };
      store.operations.set(uuid(), copy(persisted));
      store.rows.set(uuid(), copy([row, secondRow]));
      await store.withAccountOperationLease('migration', async (lease) => {
        const error = await lease
          .commitProgress({
            operationId: uuid(),
            expectedRevision: initial.progress.revision,
            runRecord: intended,
            rows: [sibling, different],
            updateRows: [updated, missing],
            expectedRowWatermarks: { item: 1 },
          })
          .catch((error: unknown) => error);
        expect(store.operations.get(uuid())).toEqual(persisted);
        expect(store.rows.get(uuid())).toEqual([row, secondRow]);
        expect(error).toHaveProperty(
          'message',
          state === 'failed'
            ? conflict(uuid()).message
            : divergence(uuid()).message,
        );
      });
    }
  });

  it('compares record payloads canonically for fresh commits and convergence', async () => {
    const record = baseRecord(deploymentSpec('canonical'));
    const stored: FleetOperationStagedRow = {
      rowKind: 'record',
      ordinal: 0,
      payload: { ...record },
    };
    const reordered = {
      ...stored,
      payload: Object.fromEntries(Object.entries(record).reverse()),
    };
    expect(JSON.stringify(stored.payload)).not.toBe(
      JSON.stringify(reordered.payload),
    );
    const initial: FleetOperationRunRecord = {
      version: 1,
      operationId: uuid(),
      kind: 'audit',
      state: 'running',
      progress: { kind: 'audit', revision: 0 },
      updatedAt: new Date(NOW).toISOString(),
    };
    const intended = {
      ...initial,
      progress: { ...initial.progress, revision: 1 },
    };
    const store = new MemoryOperationStore();
    store.operations.set(uuid(), copy(initial));
    store.rows.set(uuid(), copy([stored]));
    await store.withAccountOperationLease('audit', async (lease) => {
      const input = {
        operationId: uuid(),
        expectedRevision: 0,
        runRecord: intended,
        rows: [reordered],
        expectedRowWatermarks: { record: 1 },
      };
      const error = await lease
        .commitProgress({
          ...input,
          rows: [
            { ...stored, ordinal: 1 },
            { ...stored, payload: { ...stored.payload, tenantTag: 'other' } },
          ],
        })
        .catch((error: unknown) => error);
      expect(store.operations.get(uuid())).toEqual(initial);
      expect(store.rows.get(uuid())).toEqual([stored]);
      expect(error).toBeInstanceOf(Error);
      for (let retry = 0; retry < 2; retry += 1) {
        expect(await lease.commitProgress(copy(input))).toEqual(intended);
        expect(store.operations.get(uuid())).toEqual(intended);
        expect(JSON.stringify(store.rows.get(uuid()))).toBe(
          JSON.stringify([stored]),
        );
      }
    });
  });

  it('refuses multiple failure updates before changing rows or releasing the head', async () => {
    const world = createWorld();
    await world.start(uuid(), [world.initial, world.initial]);
    const store = world.operationStore;
    const initial = copy(store.operations.get(uuid()));
    const rows = copy(store.rows.get(uuid()));
    if (!initial || !rows) throw new Error('missing failure fixture');
    await store.withAccountOperationLease('migration', async (lease) => {
      const error = await lease
        .failOperation({
          operationId: uuid(),
          expectedRevision: initial.progress.revision,
          runRecord: {
            ...initial,
            state: 'failed',
            progress: {
              ...initial.progress,
              revision: initial.progress.revision + 1,
            },
          },
          updateRows: rows.map((row) => ({
            ...row,
            payload: { ...row.payload, status: 'failed' },
          })),
        })
        .catch((error: unknown) => error);
      expect(store.operations.get(uuid())).toEqual(initial);
      expect(store.rows.get(uuid())).toEqual(rows);
      expect(store.heads.get('migration')).toBe(uuid());
      expect(error).toHaveProperty(
        'message',
        FLEET_OPERATION_SINGLE_UPDATE_ROW_MESSAGE,
      );
    });
  });
});

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

  it('a migrating-phase admission refuses a migration candidate whose live artifact version left the persisted release', async () => {
    const world = createWorld({ external: true });
    await advanceTo(world, 'arm-maintenance');
    const migrating = world.current();
    expect(migrating.phase).toBe('migrating');
    expect(migrating.migrationIntent?.subphase).toBe('candidate-deployed');
    const release = migrating.pendingRelease as ExternalReleaseSnapshot;
    expect(release.artifactVersion).toBe(`v${world.spec.schemaVersion}`);
    const resumed = new MemoryOperationStore();
    armOptions(world, { operationStore: resumed });
    let next = await armedStart(world, uuid(2), [migrating]);
    for (let count = 0; count < 80; count += 1) {
      const staged = resumed.item(uuid(2));
      if (staged.plan?.[staged.planCursor ?? -1]?.step === 'deploy-candidate') {
        break;
      }
      if (next.status !== 'pending') {
        throw new Error('terminated before deploy-candidate');
      }
      next = await continueWorld(world, next);
    }
    const item = resumed.item(uuid(2));
    expect(item.plan?.[item.planCursor ?? -1]?.step).toBe('deploy-candidate');
    // The release name now serves a different artifact than the record pins.
    world.releases.set(
      release.physicalScriptName,
      world.liveFor(world.spec, 'v9'),
    );
    world.ops.length = 0;
    await expect(continueWorld(world, next)).rejects.toThrow(
      `migration candidate immutable release '${release.physicalScriptName}' does not match persisted artifact version '${release.artifactVersion}'`,
    );
    expect(resumed.item(uuid(2)).status).toBe('failed');
    expect(world.current().pendingRelease?.artifactVersion).toBe(
      release.artifactVersion,
    );
    expect(providerMutations(world)).toEqual([]);
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

  it('crash window admit-migrating: no re-put from migrating', async () => {
    for (const input of [
      {},
      { external: true },
      { path: 'platform-only' as const },
    ]) {
      const world = createWorld(input);
      const result = await advanceTo(world, 'admit-migrating');
      const item = await loseStepResponse(world, result);
      const admitted = copy(world.current());
      expect(admitted.phase).toBe('migrating');
      world.ops.length = 0;
      const puts = world.fleetStore.puts.length;
      await continueWorld(world, result);
      expect(world.current()).toEqual(admitted);
      expect(world.fleetStore.puts).toHaveLength(puts);
      expect(providerMutations(world)).toEqual([]);
      expect(world.operationStore.item().planCursor).toBe(
        (item.planCursor ?? 0) + 1,
      );
    }
  });

  it('crash window apply-migrations: occurrence identity and ledger verification cover both sides of the schema put', async () => {
    const intermediate = createWorld();
    const result = await advanceTo(intermediate, 'apply-migrations');
    await loseStepResponse(intermediate, result);
    expect(intermediate.current().schemaVersion).toBe(2);
    intermediate.ops.length = 0;
    const puts = intermediate.fleetStore.puts.length;
    const next = await continueWorld(intermediate, result);
    expect(intermediate.ops.filter((op) => op.startsWith('apply:'))).toEqual(
      [],
    );
    expect(intermediate.fleetStore.puts).toHaveLength(puts);
    await continueWorld(intermediate, next);
    expect(intermediate.ops.filter((op) => op.startsWith('apply:'))).toEqual([
      'apply:3',
    ]);

    for (const missingLedger of [false, true]) {
      const final = createWorld({ schemaVersion: 2 });
      const token = await advanceTo(final, 'apply-migrations');
      if (missingLedger)
        final.fleetStore.set({ ...final.current(), schemaVersion: 2 });
      else await loseStepResponse(final, token);
      final.ops.length = 0;
      const writes = final.fleetStore.puts.length;
      if (missingLedger) {
        await expect(continueWorld(final, token)).rejects.toThrow(
          'migration ledger is incomplete',
        );
        expectItemFailure(final);
      } else {
        await continueWorld(final, token);
      }
      expect(final.ops.filter((op) => op.startsWith('apply:'))).toEqual([
        'apply:verify',
      ]);
      expect(final.fleetStore.puts).toHaveLength(writes);
    }

    const beforePut = createWorld();
    const token = await advanceTo(beforePut, 'apply-migrations');
    const applied: number[] = [];
    const apply = beforePut.backend.applyMigrations;
    beforePut.backend.applyMigrations = async (...args) => {
      for (const { version } of args[1])
        if (!beforePut.ledger.has(version)) applied.push(version);
      return apply(...args);
    };
    const interruption = new Error('process terminated before schema put');
    beforePut.fleetStore.beforePut = (record) => {
      if (record.schemaVersion === 2) throw interruption;
    };
    beforePut.operationStore.beforeFail = () => {
      throw interruption;
    };
    await expect(continueWorld(beforePut, token)).rejects.toBe(interruption);
    expect(beforePut.current().schemaVersion).toBe(1);
    expect(beforePut.ledger.has(2)).toBe(true);
    expect(beforePut.operationStore.item().status).toBe('active');
    beforePut.fleetStore.beforePut = undefined;
    beforePut.operationStore.beforeFail = undefined;
    beforePut.ops.length = 0;
    await continueWorld(beforePut, token);
    expect(beforePut.ops.filter((op) => op.startsWith('apply:'))).toEqual([
      'apply:2',
    ]);
    expect(applied).toEqual([2]);
    expect(beforePut.current().schemaVersion).toBe(2);
  });

  it('crash window deploy-candidate: external re-deploys the same artifact; non-external adopts via inspect', async () => {
    for (const external of [false, true]) {
      const world = createWorld({ external });
      const token = await advanceTo(world, 'deploy-candidate');
      const deploys: Parameters<ProvisioningBackend['deployWorker']>[] = [];
      const deploy = world.backend.deployWorker;
      world.backend.deployWorker = async (...args) => {
        deploys.push(args);
        return deploy(...args);
      };
      const interruption = new Error('candidate commit interrupted');
      world.fleetStore.beforePut = (record) => {
        if (
          record.pendingArtifactVersion ||
          record.migrationIntent?.subphase === 'candidate-deployed'
        )
          throw interruption;
      };
      world.operationStore.beforeFail = () => {
        throw interruption;
      };
      await expect(continueWorld(world, token)).rejects.toBe(interruption);
      expect(deploys).toHaveLength(1);
      world.fleetStore.beforePut = undefined;
      world.operationStore.beforeFail = undefined;
      world.ops.length = 0;
      await continueWorld(world, token);
      expect(deploys).toHaveLength(external ? 2 : 1);
      if (external) {
        expect(deploys[1]?.[0]).toBe(deploys[0]?.[0]);
        expect(deploys[1]?.[1]).toEqual(deploys[0]?.[1]);
        expect(deploys[1]?.[2]).toBe(deploys[0]?.[2]);
        expect(deploys[1]?.[5]).toBe(deploys[0]?.[5]);
        expect(world.ops).toContain('deploy');
      } else {
        expect(world.ops).toContain('inspect');
        expect(world.ops).not.toContain('deploy');
      }
      expect(
        external
          ? world.current().pendingRelease?.artifactVersion
          : world.current().pendingArtifactVersion,
      ).toBe(`v${world.spec.schemaVersion}`);
    }
  });

  it('crash window pending-topology: timestamp-only re-put', async () => {
    const world = createWorld({ external: true });
    const token = await advanceTo(world, 'pending-topology');
    await loseStepResponse(world, token);
    const prior = copy(world.current());
    const puts = world.fleetStore.puts.length;
    world.ops.length = 0;
    await advanceFleetMigration({
      ...world.options({ kind: 'continue', token: token.token }),
      clock: () => NOW + 1000,
    });
    expect(world.fleetStore.puts).toHaveLength(puts + 1);
    expect(world.current()).toEqual({
      ...prior,
      updatedAt: new Date(NOW + 1000).toISOString(),
    });
    expect(providerMutations(world)).toEqual([]);
  });

  it('crash window retire-post: cleared retiringRelease no-ops', async () => {
    const world = createWorld({ external: true });
    world.fleetStore.set({
      ...world.initial,
      rollbackRelease: {
        ...world.priorRelease,
        physicalScriptName: 'obsolete-release',
      },
    });
    const token = await advanceTo(world, 'retire-post');
    await loseStepResponse(world, token);
    expect(world.ops).toContain('retire:obsolete-release');
    expect(world.current().retiringRelease).toBeUndefined();
    const prior = copy(world.current());
    const puts = world.fleetStore.puts.length;
    world.ops.length = 0;
    await continueWorld(world, token);
    expect(world.current()).toEqual(prior);
    expect(world.fleetStore.puts).toHaveLength(puts);
    expect(providerMutations(world)).toEqual([]);
    expect(world.operationStore.item().status).toBe('complete');
  });

  it('first error: ONE failOperation batch and the original error rethrown; later ordinals never run', async () => {
    const world = createWorld();
    const later = baseRecord(deploymentSpec('later'));
    world.fleetStore.set(later);
    const result = await advanceTo(
      world,
      'seed-identity',
      await world.start(uuid(), [world.initial, later]),
    );
    const original = new Error('Authorization: Bearer private-provider-error');
    world.fail('seed:cedar', original);
    await expect(continueWorld(world, result)).rejects.toBe(original);
    expectItemFailure(world);
    expect(world.operationStore.failures).toHaveLength(1);
    expect(world.operationStore.failures[0]?.updateRows).toEqual([
      {
        rowKind: 'item',
        ordinal: 0,
        payload: { ...world.operationStore.item() },
      },
    ]);
    expect(
      world.operationStore.failures[0]?.runRecord.progress.failure,
    ).toEqual({ reason: 'item-failed', itemOrdinal: 0 });
    expect(world.operationStore.item(uuid(), 1)).toMatchObject({
      status: 'pending',
    });
    expect(world.ops.some((op) => op.endsWith(':later'))).toBe(false);
    expect(world.operationStore.heads.has('migration')).toBe(false);
    expect(world.operationStore.operations.get(uuid())?.terminalAtMs).toBe(NOW);
    expect(world.operationStore.operations.get(uuid())?.progress.revision).toBe(
      result.token.revision + 1,
    );
    const bytes = JSON.stringify([
      ...world.operationStore.operations.values(),
      ...world.operationStore.rows.values(),
    ]);
    expect(bytes).not.toContain(original.message);
    expect(bytes).not.toContain('Bearer');
  });

  it('continue on a failed operation returns the failed member with zero provider work', async () => {
    const world = createWorld();
    const token = await world.start();
    const original = new Error('backend resolver failed');
    await expect(
      advanceFleetMigration({
        ...world.options({ kind: 'continue', token: token.token }),
        backendFor() {
          throw original;
        },
      }),
    ).rejects.toBe(original);
    const run = world.operationStore.operations.get(uuid());
    if (!run) throw new Error('missing failed run');
    world.ops.length = 0;
    world.fleetStore.ops.length = 0;
    const calls = [...world.operationStore.calls];
    const failed = await advanceFleetMigration(
      world.options({
        kind: 'continue',
        token: { ...token.token, revision: run.progress.revision },
      }),
    );
    expect(failed).toMatchObject({
      status: 'failed',
      failure: { reason: 'item-failed', itemOrdinal: 0 },
    });
    expect(world.ops).toEqual([]);
    expect(world.fleetStore.ops).toEqual([]);
    expect(world.operationStore.calls).toEqual(calls);
  });

  it('a disappeared record becomes the item failure', async () => {
    for (const admitted of [false, true]) {
      const world = createWorld();
      let token = await world.start();
      if (admitted) token = await continueWorld(world, token);
      world.fleetStore.records.clear();
      world.ops.length = 0;
      await expect(continueWorld(world, token)).rejects.toThrow(
        'fleet migration record disappeared',
      );
      expectItemFailure(world);
      expect(world.ops).toEqual([]);
      expect(world.fleetStore.puts).toEqual([]);
    }
  });

  it('abandonFleetMigrationOperation: running becomes operator-abandoned with the active item failed; terminal is a no-op', async () => {
    for (const admitted of [false, true]) {
      const world = createWorld();
      let token = await world.start();
      if (admitted) token = await continueWorld(world, token);
      const item = world.operationStore.item();
      await abandonFleetMigrationOperation({
        operationStore: world.operationStore,
        operationId: uuid(),
      });
      expect(world.operationStore.item()).toEqual({
        ...item,
        status: 'failed',
      });
      expect(world.operationStore.operations.get(uuid())).toMatchObject({
        state: 'failed',
        terminalAtMs: NOW,
        progress: {
          revision: token.token.revision + 1,
          failure: { reason: 'operator-abandoned', itemOrdinal: 0 },
        },
      });
      expect(world.operationStore.heads.has('migration')).toBe(false);
      const state = copy([...world.operationStore.operations.values()]);
      const calls = [...world.operationStore.calls];
      await abandonFleetMigrationOperation({
        operationStore: world.operationStore,
        operationId: uuid(),
      });
      expect([...world.operationStore.operations.values()]).toEqual(state);
      expect(world.operationStore.calls).toEqual(calls);
    }
    const complete = createWorld({ path: 'ready' });
    await drainWorld(complete);
    const prior = copy([...complete.operationStore.operations.values()]);
    const calls = [...complete.operationStore.calls];
    await abandonFleetMigrationOperation({
      operationStore: complete.operationStore,
      operationId: uuid(),
    });
    expect([...complete.operationStore.operations.values()]).toEqual(prior);
    expect(complete.operationStore.calls).toEqual(calls);
  });

  it('items page is readable while running', async () => {
    const world = createWorld();
    const records = [
      world.initial,
      baseRecord(deploymentSpec('elm')),
      baseRecord(deploymentSpec('fir')),
    ];
    const token = await world.start(uuid(), records);
    await continueWorld(world, token);
    world.ops.length = 0;
    const first = await readFleetMigrationItemsPage(world.operationStore, {
      operationId: uuid(),
      limit: 2,
    });
    expect(first.items.map((item) => item.ordinal)).toEqual([0, 1]);
    expect(first.items.map((item) => item.status)).toEqual([
      'active',
      'pending',
    ]);
    expect(first.done).toBe(false);
    const second = await readFleetMigrationItemsPage(world.operationStore, {
      operationId: uuid(),
      afterOrdinal: 1,
      limit: 2,
    });
    expect(second.items.map((item) => item.ordinal)).toEqual([2]);
    expect(second.done).toBe(true);
    expect(world.ops).toEqual([]);
    expect(world.operationStore.operations.get(uuid())?.state).toBe('running');
    world.operationStore.reversePages = true;
    expect(
      (
        await readFleetMigrationItemsPage(world.operationStore, {
          operationId: uuid(),
          limit: 3,
        })
      ).items.map((item) => item.ordinal),
    ).toEqual([0, 1, 2]);
  });

  it('second-world drain-versus-bounded end states and sanitized rows retain callback and clock contracts', async () => {
    for (const input of [
      { path: 'ready' as const },
      {},
      { external: true },
      { path: 'platform-only' as const, lagging: true },
    ]) {
      const bounded = createWorld(input);
      const drain = createWorld(input);
      await drainWorld(bounded);
      const [legacy] = await migrateWorld(drain);
      expect(bounded.current()).toEqual(legacy);
      expect([...bounded.releases]).toEqual([...drain.releases]);
      expect([...bounded.routed]).toEqual([...drain.routed]);
      expect([...bounded.ledger]).toEqual([...drain.ledger]);
      const items = bounded.operationStore.rows.get(uuid()) ?? [];
      const bytes = JSON.stringify(items);
      for (const forbidden of [
        'databaseId',
        'scriptName',
        'platformResources',
        'applicationBindings',
        'maintenanceAdmin',
        'deploymentIdentity',
        SECRETS.maintenanceAdmin,
        SECRETS.deploymentIdentity,
        JSON.stringify(bounded.initial),
      ])
        expect(bytes).not.toContain(forbidden);
      const failed = createWorld(input);
      const token = await failed.start();
      await expect(
        advanceFleetMigration({
          ...failed.options({ kind: 'continue', token: token.token }),
          secretsFor() {
            throw new Error(
              `Authorization: Bearer ${SECRETS.maintenanceAdmin}`,
            );
          },
        }),
      ).rejects.toThrow('Authorization: Bearer');
      expect(
        failed.operationStore.operations.get(uuid())?.progress.failure,
      ).toEqual({ reason: 'item-failed', itemOrdinal: 0 });
      const failureBytes = JSON.stringify([
        ...failed.operationStore.operations.values(),
        ...failed.operationStore.rows.values(),
      ]);
      expect(failureBytes).not.toContain('Authorization');
      expect(failureBytes).not.toContain('Bearer');
      expect(failureBytes).not.toContain(SECRETS.maintenanceAdmin);
    }

    const resolverNames = [
      'backendFor',
      'specFor',
      'secretsFor',
      'finalizedStateProviderFor',
      'settlementFor',
    ] as const;
    for (const mode of ['bounded', 'drain'] as const) {
      for (const selected of resolverNames) {
        const world =
          selected === 'finalizedStateProviderFor'
            ? finalizedWorld('ready').world
            : createWorld({ path: 'ready' });
        const token =
          mode === 'bounded'
            ? selected === 'settlementFor'
              ? await advanceTo(world, 'ready-attest-settle')
              : await world.start()
            : undefined;
        const options = {
          ...world.options({ kind: 'continue', token: token?.token ?? {} }),
          store: world.fleetStore,
          records: [world.current()],
          canaryTenantTags: [],
        };
        const calls: string[] = [];
        const original = new Error(`selected ${selected}`);
        const values = {
          backendFor: world.backend,
          specFor: world.spec,
          secretsFor: SECRETS,
        };
        world.fleetStore.ops.length = 0;
        for (const name of resolverNames)
          Object.defineProperty(options, name, {
            get() {
              expect(this).toBe(options);
              expect(world.fleetStore.ops).toContain('get');
              calls.push(`get:${name}`);
              return function (this: unknown, record: FleetRecord) {
                expect(this).toBe(options);
                expect(record.tenantTag).toBe(world.initial.tenantTag);
                calls.push(`call:${name}`);
                if (name === selected) throw original;
                return name in values
                  ? values[name as keyof typeof values]
                  : undefined;
              };
            },
          });
        await expect(
          mode === 'bounded'
            ? advanceFleetMigration(options)
            : migrateFleet(options),
        ).rejects.toBe(original);
        const expected = resolverNames
          .slice(0, 3)
          .slice(
            0,
            selected === 'backendFor' ? 1 : selected === 'specFor' ? 2 : 3,
          );
        const order =
          selected === 'finalizedStateProviderFor' ||
          selected === 'settlementFor'
            ? [...expected, selected]
            : expected;
        expect(calls).toEqual(
          order.flatMap((name) => [`get:${name}`, `call:${name}`]),
        );
      }
      const vanished = createWorld();
      const token = mode === 'bounded' ? await vanished.start() : undefined;
      const options = {
        ...vanished.options({ kind: 'continue', token: token?.token ?? {} }),
        store: vanished.fleetStore,
        records: [vanished.initial],
        canaryTenantTags: [],
      };
      vanished.fleetStore.records.clear();
      for (const name of resolverNames)
        Object.defineProperty(options, name, {
          get() {
            throw new Error(`eager ${name}`);
          },
        });
      let clockReads = 0;
      Object.defineProperty(options, 'clock', {
        get() {
          clockReads += 1;
          return () => NOW;
        },
      });
      await expect(
        mode === 'bounded'
          ? advanceFleetMigration(options)
          : migrateFleet(options),
      ).rejects.toThrow('fleet migration record disappeared');
      expect(clockReads).toBe(1);

      const retirement = createWorld({ path: 'ready', external: true });
      retirement.fleetStore.set({
        ...retirement.current(),
        retiringRelease: {
          ...retirement.priorRelease,
          physicalScriptName: 'expired-release',
        },
      });
      const retiringToken =
        mode === 'bounded'
          ? await advanceTo(retirement, 'retire-pre')
          : undefined;
      const retiringOptions = {
        ...retirement.options({
          kind: 'continue',
          token: retiringToken?.token ?? {},
        }),
        store: retirement.fleetStore,
        records: [retirement.current()],
        canaryTenantTags: [],
      };
      let deleted = false;
      let reads = 0;
      let selectedCalls = 0;
      retirement.backend.deleteRetainedRelease = async () => {
        await Promise.resolve();
        deleted = true;
      };
      Object.defineProperty(retiringOptions, 'clock', {
        get() {
          reads += 1;
          if (reads === 1) return () => NOW;
          if (!deleted)
            return function (this: unknown) {
              expect(this).toBeUndefined();
              expect(deleted).toBe(true);
              selectedCalls += 1;
              return NOW;
            };
          return () => NOW + 1000;
        },
      });
      await (mode === 'bounded'
        ? advanceFleetMigration(retiringOptions)
        : migrateFleet(retiringOptions));
      expect(selectedCalls).toBe(1);
      expect(
        retirement.fleetStore.puts.find((record) => !record.retiringRelease)
          ?.updatedAt,
      ).toBe(new Date(NOW).toISOString());

      const schema = createWorld({ schemaVersion: 2 });
      const schemaToken =
        mode === 'bounded'
          ? await advanceTo(schema, 'apply-migrations')
          : undefined;
      const schemaOptions = {
        ...schema.options({
          kind: 'continue',
          token: schemaToken?.token ?? {},
        }),
        store: schema.fleetStore,
        records: [schema.current()],
        canaryTenantTags: [],
      };
      let providerDone = false;
      const apply = schema.backend.applyMigrations;
      schema.backend.applyMigrations = async (...args) => {
        await apply(...args);
        await Promise.resolve();
        providerDone = true;
      };
      Object.defineProperty(schemaOptions, 'clock', {
        get() {
          const instant = providerDone ? NOW + 1000 : NOW;
          return function (this: unknown) {
            expect(this).toBeUndefined();
            return instant;
          };
        },
      });
      await (mode === 'bounded'
        ? advanceFleetMigration(schemaOptions)
        : migrateFleet(schemaOptions));
      expect(
        schema.fleetStore.puts.find((record) => record.schemaVersion === 2)
          ?.updatedAt,
      ).toBe(new Date(NOW + 1000).toISOString());

      const finalized = finalizedWorld('ready');
      const ordinary = finalized.world;
      const ordinaryToken =
        mode === 'bounded'
          ? await advanceTo(ordinary, 'ready-platform-resources')
          : undefined;
      const ordinaryOptions = {
        ...ordinary.options({
          kind: 'continue',
          token: ordinaryToken?.token ?? {},
        }),
        store: ordinary.fleetStore,
        records: [ordinary.current()],
        canaryTenantTags: [],
      };
      let described = false;
      let ordinaryReads = 0;
      const describe = finalized.provider.describeFinalizedState;
      finalized.provider.describeFinalizedState = (input) => {
        described = true;
        return describe(input);
      };
      const selected = function (this: {
        provider: FinalizedOrdinaryStateProvider;
        record: FleetRecord;
        clock: () => number;
      }) {
        expect(this.provider).toBe(finalized.provider);
        expect(this.clock).toBe(selected);
        expect(this.record.tenantTag).toBe('cedar');
        expect(described).toBe(true);
        throw new Error('ordinary clock receiver verified');
      };
      Object.defineProperty(ordinaryOptions, 'clock', {
        get() {
          ordinaryReads += 1;
          expect(described).toBe(false);
          return ordinaryReads === 1 ? () => NOW : selected;
        },
      });
      await expect(
        mode === 'bounded'
          ? advanceFleetMigration(ordinaryOptions)
          : migrateFleet(ordinaryOptions),
      ).rejects.toThrow('ordinary clock receiver verified');
      expect(ordinaryReads).toBe(2);
    }
    for (const [step, expectedReads] of [
      ['ready-target-backfill', 1],
      ['ready-platform-resources', 1],
      ['ready-maintenance', 1],
      ['ready-promote', 2],
      ['ready-retire-post', 2],
    ] as const) {
      const world = createWorld({ path: 'ready' });
      world.fleetStore.set({
        ...world.current(),
        invocationAuthority: {
          version: 1,
          authorizedAt: new Date(NOW).toISOString(),
        },
      });
      const token = await advanceTo(world, step);
      const options = world.options({ kind: 'continue', token: token.token });
      let reads = 0;
      let calls = 0;
      Object.defineProperty(options, 'clock', {
        get() {
          reads += 1;
          return () => {
            calls += 1;
            return NOW;
          };
        },
      });
      await advanceFleetMigration(options);
      expect(reads).toBe(expectedReads);
      expect(calls).toBe(0);
    }
  });

  it('ready-target-backfill trusted-resources refusal fails the item from inside the step', async () => {
    const world = createWorld({ path: 'ready', external: true });
    const current = { ...world.current() };
    delete current.platformTarget;
    delete current.platformResources;
    world.fleetStore.set(current);
    const token = await advanceTo(world, 'ready-target-backfill');
    expect(world.operationStore.item().status).toBe('active');
    await expect(continueWorld(world, token)).rejects.toThrow(
      'ready external deployment has no trusted platform resources',
    );
    expectItemFailure(world);
    expect(world.fleetStore.puts).toEqual([]);
    expect(providerMutations(world)).toEqual([]);
  });

  it('ready-platform-resources preserves its exact platform-target refusal and the earlier coordinator fence', async () => {
    const world = createWorld({ path: 'ready', external: true });
    const token = await advanceTo(world, 'ready-platform-resources');
    const divergent = {
      ...world.current(),
      platformTarget: {
        ...world.priorTarget,
        stateArtifactDigest: 'f'.repeat(64),
      },
    };
    await withAdmitted(world, async ({ admitted }) => {
      await expect(
        executeNextMigrationStep(
          admitted,
          [{ step: 'ready-platform-resources' }],
          0,
          { entry: divergent, current: divergent },
        ),
      ).rejects.toThrow(
        'ready deployment does not match the persisted platform target',
      );
    });
    world.fleetStore.set(divergent);
    world.ops.length = 0;
    const puts = world.fleetStore.puts.length;
    await expect(continueWorld(world, token)).rejects.toThrow(
      'fleet migration item no longer matches its frozen plan',
    );
    expectItemFailure(world);
    expect(world.fleetStore.puts).toHaveLength(puts);
    expect(providerMutations(world)).toEqual([]);
  });

  it('platform-only-maintenance unarmed refusal fails the item from inside the step', async () => {
    const world = createWorld({ path: 'platform-only' });
    const token = await advanceTo(world, 'platform-only-maintenance');
    world.setMaintenance({ ...HEALTHY, armed: false });
    world.ops.length = 0;
    await expect(continueWorld(world, token)).rejects.toThrow(
      'platform-only migration maintenance is unarmed before route publication',
    );
    expectItemFailure(world);
    expect(world.ops).toContain('maintenance');
    expect(world.ops).not.toContain('promote');
  });

  it('last apply-migrations missing-path refusal uses the entry schema and fails the item', async () => {
    const world = createWorld();
    const token = await advanceTo(world, 'apply-migrations');
    const item = world.operationStore.item();
    world.operationStore.setItem({
      ...item,
      plan: item.plan?.filter(
        (entry) =>
          entry.step !== 'apply-migrations' || entry.targetSchemaVersion !== 3,
      ),
    });
    await expect(continueWorld(world, token)).rejects.toThrow(
      'missing D1 migration path from 1 to 3',
    );
    expect(world.current().schemaVersion).toBe(2);
    expectItemFailure(world);
    expect(world.ops.filter((op) => op.startsWith('apply:'))).toEqual([
      'apply:2',
    ]);
    const invalid = createWorld();
    invalid.spec = {
      ...invalid.spec,
      migrations: invalid.spec.migrations.slice(0, 2),
    };
    const start = await invalid.start();
    await expect(continueWorld(invalid, start)).rejects.toThrow(
      'D1 migration history must contain every version through schemaVersion',
    );
    expectItemFailure(invalid);
    expect(invalid.fleetStore.puts).toEqual([]);
    expect(providerMutations(invalid)).toEqual([]);
  });

  it('READY-plan reachability: trusted resources without platformTarget reach the backfill step', async () => {
    const world = createWorld({ path: 'ready', external: true });
    const current = { ...world.current() };
    delete current.platformTarget;
    world.fleetStore.set(current);
    const token = await advanceTo(world, 'ready-target-backfill');
    expect(world.operationStore.item().plan?.map(({ step }) => step)).toEqual([
      'ready-target-backfill',
      'ready-platform-resources',
      'ready-maintenance',
      'ready-promote',
      'ready-attest-settle',
      'ready-retire-post',
    ]);
    expect(world.fleetStore.puts).toEqual([]);
    await continueWorld(world, token);
    expect(world.current().platformTarget).toEqual(world.priorTarget);
    expect(world.fleetStore.puts).toHaveLength(1);
    expect(providerMutations(world)).toEqual([]);
  });

  it('assert-migrating lost-intent refusal fails the item from inside the step', async () => {
    const world = createWorld({ external: true });
    const token = await advanceTo(world, 'assert-migrating');
    const current = { ...world.current() };
    delete current.migrationPriorRelease;
    world.fleetStore.set(current);
    const puts = world.fleetStore.puts.length;
    world.ops.length = 0;
    await expect(continueWorld(world, token)).rejects.toThrow(
      'immutable external migration lost its durable release intent',
    );
    expectItemFailure(world);
    expect(world.fleetStore.puts).toHaveLength(puts);
    expect(providerMutations(world)).toEqual([]);
  });

  it('plan-compatibility fence covers admission gaps, intent drift and premature foreign convergence', async () => {
    const equal = createWorld({ path: 'ready' });
    const equalToken = await advanceTo(equal, 'ready-target-backfill');
    equal.fleetStore.set({
      ...equal.current(),
      updatedAt: new Date(NOW).toISOString(),
    });
    await continueWorld(equal, equalToken);
    expect(equal.operationStore.item().status).toBe('active');
    for (const opposite of [false, true]) {
      const world = createWorld({ path: 'ready', external: true });
      const token = await advanceTo(world, 'ready-target-backfill');
      const donor = createWorld({
        path: opposite ? 'platform-only' : 'full',
        external: true,
      });
      donor.spec = world.spec;
      if (!opposite)
        donor.fleetStore.set({
          ...donor.current(),
          desiredSpecDigest: 'f'.repeat(64),
        });
      await advanceTo(donor, 'assert-migrating');
      const carrier = donor.current();
      if (!carrier.migrationIntent) throw new Error('missing donor intent');
      world.fleetStore.set({
        ...carrier,
        migrationIntent: {
          ...carrier.migrationIntent,
          target: world.priorTarget,
        },
      });
      world.ops.length = 0;
      const puts = world.fleetStore.puts.length;
      await expect(continueWorld(world, token)).rejects.toThrow(
        'fleet migration item no longer matches its frozen plan',
      );
      expectItemFailure(world);
      expect(world.fleetStore.puts).toHaveLength(puts);
      expect(providerMutations(world)).toEqual([]);
    }
    const divergent = createWorld({ external: true });
    const divergentToken = await advanceTo(divergent, 'seed-identity');
    const carrier = divergent.current();
    if (!carrier.migrationIntent) throw new Error('missing intent');
    divergent.fleetStore.set({
      ...carrier,
      pendingRelease: {
        ...(carrier.pendingRelease as ExternalReleaseSnapshot),
        physicalScriptName: 'foreign-candidate',
      },
    });
    await expect(continueWorld(divergent, divergentToken)).rejects.toThrow(
      'migration retry uses a different desired specification',
    );
    expectItemFailure(divergent);
    for (const path of ['full', 'platform-only'] as const) {
      const admitted = createWorld({ path, external: true });
      const admittedToken = await advanceTo(admitted, 'admit-migrating');
      expect(admitted.current().phase).toBe('ready');
      await continueWorld(admitted, admittedToken);
      expect(admitted.current().phase).toBe('migrating');
      const frozen = admitted.operationStore.item().plan;
      const terminal = frozen?.findIndex(
        ({ step }) =>
          step === (path === 'full' ? 'settle-ready' : 'platform-only-ready'),
      );
      if (terminal === undefined || terminal < 1)
        throw new Error('missing terminal entry');
      const donor = createWorld({ path, external: true });
      await drainWorld(donor);
      for (let cursor = 0; cursor < terminal; cursor += 1) {
        const world = createWorld({ path, external: true });
        let token = await advanceTo(world, 'admit-migrating');
        for (let completed = 0; completed < cursor; completed += 1)
          token = await continueWorld(world, token);
        expect(world.operationStore.item().planCursor).toBe(cursor);
        world.fleetStore.set(donor.current());
        const puts = world.fleetStore.puts.length;
        world.ops.length = 0;
        await expect(continueWorld(world, token)).rejects.toThrow(
          'fleet migration item no longer matches its frozen plan',
        );
        expectItemFailure(world);
        expect(world.fleetStore.puts).toHaveLength(puts);
        expect(providerMutations(world)).toEqual([]);
      }
    }
  });

  it('carrier-fact drift past assert-migrating is caught by the next fence assertion re-run', async () => {
    for (const field of [
      'migrationPriorRelease',
      'target',
      'finalizedState',
    ] as const) {
      const finalized = finalizedWorld();
      const world = finalized.world;
      const token = await advanceTo(world, 'seed-identity');
      const current = { ...world.current() };
      if (!current.migrationIntent) throw new Error('missing intent');
      const expected =
        field === 'migrationPriorRelease'
          ? 'immutable external migration lost its durable release intent'
          : field === 'target'
            ? 'migration retry does not match the persisted platform target'
            : 'finalized provider state drifted';
      if (field === 'migrationPriorRelease')
        delete current.migrationPriorRelease;
      if (field === 'target')
        current.migrationIntent = {
          ...current.migrationIntent,
          target: {
            ...current.migrationIntent.target,
            stateArtifactDigest: 'f'.repeat(64),
          },
        };
      if (field === 'finalizedState')
        finalized.provider.assertFinalizedState = async () => {
          throw new Error(expected);
        };
      world.fleetStore.set(current);
      const puts = world.fleetStore.puts.length;
      world.ops.length = 0;
      await expect(continueWorld(world, token)).rejects.toThrow(expected);
      expectItemFailure(world);
      expect(world.fleetStore.puts).toHaveLength(puts);
      expect(providerMutations(world)).toEqual([]);
    }
  });

  it('all ten bounded entry bindings observe the leased retry snapshot rather than the admission snapshot', async () => {
    for (const step of [
      'platform-only-maintenance',
      'platform-only-promote',
      'platform-only-ready',
      'deploy-candidate',
      'promote',
      'settle-ready',
    ] as const) {
      const path = step.startsWith('platform-only-') ? 'platform-only' : 'full';
      const world = createWorld({ path, external: true, namespace: true });
      let token = await advanceTo(
        world,
        path === 'platform-only'
          ? 'platform-only-resources'
          : 'platform-resources',
      );
      world.setNamespace('namespace-converged');
      token = await continueWorld(world, token);
      expect(
        world.current().platformResources?.stateWorker.namespaceIds,
      ).toEqual(['namespace-converged']);
      if (path === 'platform-only')
        world.releases.set(
          world.priorRelease.physicalScriptName,
          world.liveFor(world.spec, world.priorRelease.artifactVersion),
        );
      token = await advanceTo(world, step, token);
      const before = copy(world.current());
      const item = world.operationStore.item();
      if (!item.plan || item.planCursor === undefined)
        throw new Error('missing step cursor');
      const plan = item.plan;
      const planCursor = item.planCursor;
      await withAdmitted(world, async ({ admitted, reread }) => {
        const staleEntry = {
          ...reread,
          platformResources: world.initial.platformResources,
        };
        await expect(
          executeNextMigrationStep(admitted, plan, planCursor, {
            entry: staleEntry,
            current: reread,
          }),
        ).rejects.toThrow(
          "deployment 'cedar:production' live state does not exactly match the desired specification",
        );
      });
      world.fleetStore.set(before);
      const retry = createWorld({ path, external: true, namespace: true });
      retry.setNamespace('namespace-converged');
      retry.fleetStore.set(before);
      retry.releases.clear();
      for (const [name, live] of world.releases)
        retry.releases.set(name, copy(live));
      retry.routed.clear();
      for (const [tenant, name] of world.routed) retry.routed.set(tenant, name);
      for (const version of world.ledger) retry.ledger.add(version);
      await continueWorld(world, token);
      expect(world.operationStore.item().status).not.toBe('failed');
      expect(world.operationStore.item().planCursor).toBe(planCursor + 1);
      await expect(migrateWorld(retry)).resolves.toHaveLength(1);
    }

    const ownership = createWorld();
    const ownershipToken = await advanceTo(ownership, 'seed-identity');
    const seeded: unknown[][] = [];
    ownership.backend.seedDeploymentIdentity = async (...args) => {
      seeded.push(args);
    };
    await withAdmitted(ownership, async ({ admitted, reread }) => {
      await executeNextMigrationStep(admitted, [{ step: 'seed-identity' }], 0, {
        entry: { ...reread, tenantTag: 'entry-owner' },
        current: { ...reread, tenantTag: 'current-owner' },
      });
      expect(seeded[0]).toEqual([
        admitted.database,
        'entry-owner',
        admitted.lease,
        { initialExecutionFenceState: 'open' },
      ]);
    });
    seeded.length = 0;
    await continueWorld(ownership, ownershipToken);
    expect(seeded[0]?.[1]).toBe('cedar');
    expect(seeded[0]?.[3]).toEqual({ initialExecutionFenceState: 'open' });

    const missingPath = createWorld();
    await withAdmitted(missingPath, async ({ admitted, reread }) => {
      await expect(
        executeNextMigrationStep(
          admitted,
          [{ step: 'apply-migrations', targetSchemaVersion: 2 }],
          0,
          { entry: { ...reread, schemaVersion: 17 }, current: reread },
        ),
      ).rejects.toThrow('missing D1 migration path from 17 to 3');
      expect(missingPath.current().schemaVersion).toBe(2);
    });

    const absent = createWorld({ external: true });
    const absentToken = await advanceTo(absent, 'settle-ready');
    absent.backend.inspect = async () => undefined;
    await withAdmitted(absent, async ({ admitted, reread }) => {
      await expect(
        executeNextMigrationStep(
          admitted,
          [{ step: 'settle-ready' }, { step: 'retire-post' }],
          0,
          {
            entry: {
              ...reread,
              tenantTag: 'entry-tenant',
              environment: 'entry-environment',
            },
            current: reread,
          },
        ),
      ).rejects.toThrow(
        'deployment did not converge after migration for entry-tenant:entry-environment',
      );
    });
    await expect(continueWorld(absent, absentToken)).rejects.toThrow(
      'deployment did not converge after migration for cedar:production',
    );
    expectItemFailure(absent);

    const retirement = createWorld({ external: true });
    const retiringToken = await advanceTo(retirement, 'settle-ready');
    const entryRelease = {
      ...retirement.priorRelease,
      physicalScriptName: 'entry-rollback',
    };
    const currentRelease = {
      ...retirement.priorRelease,
      physicalScriptName: 'current-rollback',
    };
    const before = copy(retirement.current());
    await withAdmitted(retirement, async ({ admitted, reread }) => {
      const result = await executeNextMigrationStep(
        admitted,
        [{ step: 'settle-ready' }, { step: 'retire-post' }],
        0,
        {
          entry: { ...reread, rollbackRelease: entryRelease },
          current: { ...reread, rollbackRelease: currentRelease },
        },
      );
      expect(result.record.retiringRelease).toEqual(entryRelease);
    });
    retirement.fleetStore.set({ ...before, rollbackRelease: currentRelease });
    await continueWorld(retirement, retiringToken);
    expect(retirement.current().retiringRelease).toEqual(currentRelease);
    expect(retirement.initial.rollbackRelease).toBeUndefined();
  });

  it('migration kind-lease loss at dispatch aborts with zero provider work', async () => {
    const world = createWorld();
    const token = await advanceTo(world, 'seed-identity');
    const item = world.operationStore.item();
    const run = copy(world.operationStore.operations.get(uuid()));
    world.operationStore.loseLease = true;
    world.ops.length = 0;
    world.fleetStore.ops.length = 0;
    await expect(continueWorld(world, token)).rejects.toThrow(
      'operation lease lost',
    );
    expect(world.ops).toEqual([]);
    expect(world.fleetStore.ops).toEqual([]);
    expect(world.operationStore.item()).toEqual(item);
    expect(world.operationStore.operations.get(uuid())).toEqual(run);
  });

  it('fence and step observe the same shared migrating-carrier refusal', async () => {
    for (const divergence of ['prior', 'target', 'provider'] as const) {
      const finalized = finalizedWorld();
      const world = finalized.world;
      await advanceTo(world, 'seed-identity');
      await withAdmitted(world, async ({ admitted, reread }) => {
        const current = { ...reread };
        if (!current.migrationIntent) throw new Error('missing intent');
        const expected =
          divergence === 'prior'
            ? 'immutable external migration lost its durable release intent'
            : divergence === 'target'
              ? 'migration retry does not match the persisted platform target'
              : 'async finalized assertion refused';
        if (divergence === 'prior') delete current.migrationPriorRelease;
        if (divergence === 'target')
          current.migrationIntent = {
            ...current.migrationIntent,
            target: {
              ...current.migrationIntent.target,
              stateArtifactDigest: 'f'.repeat(64),
            },
          };
        let completed = 0;
        if (divergence === 'provider')
          finalized.provider.assertFinalizedState = async (input) => {
            await Promise.resolve();
            expect(input.currentRecord).toBe(current);
            expect(input.fence).toBe(admitted.lease);
            completed += 1;
            throw new Error(expected);
          };
        const item = world.operationStore.item();
        if (!item.plan || item.planCursor === undefined)
          throw new Error('missing plan');
        const assertionCursor = item.plan.findIndex(
          ({ step }) => step === 'assert-migrating',
        );
        for (const invoke of [
          () => assertMigratingCarrierState(admitted, current),
          () =>
            executeNextMigrationStep(
              admitted,
              item.plan as NonNullable<FleetMigrationItem['plan']>,
              assertionCursor,
              { entry: current, current },
            ),
          () =>
            assertFleetMigrationPlanCompatibility(
              admitted,
              {
                plan: item.plan as NonNullable<FleetMigrationItem['plan']>,
                planCursor: item.planCursor as number,
              },
              current,
            ),
        ])
          await expect(invoke()).rejects.toThrow(expected);
        expect(completed).toBe(divergence === 'provider' ? 3 : 0);
      });
    }
  });

  it('monotonic-floor regressions fail the item without mutation', async () => {
    for (const scenario of [
      'backward',
      'unreachable',
      'schema',
      'zero-pending-schema',
      'candidate',
      'ready-target',
    ] as const) {
      const world = createWorld({
        path:
          scenario === 'unreachable'
            ? 'platform-only'
            : scenario === 'ready-target'
              ? 'ready'
              : 'full',
        external: ['backward', 'unreachable', 'ready-target'].includes(
          scenario,
        ),
      });
      if (scenario === 'zero-pending-schema') {
        world.fleetStore.set({ ...world.current(), schemaVersion: 3 });
        world.ledger.add(2);
        world.ledger.add(3);
      }
      const step =
        scenario === 'backward'
          ? 'pending-topology'
          : scenario === 'unreachable'
            ? 'platform-only-maintenance'
            : scenario === 'candidate'
              ? 'arm-maintenance'
              : scenario === 'ready-target'
                ? 'ready-platform-resources'
                : 'migration-schema-applied';
      const token = await advanceTo(world, step);
      const current = { ...world.current() };
      if (scenario === 'backward' || scenario === 'unreachable') {
        if (!current.migrationIntent) throw new Error('missing intent');
        current.migrationIntent = {
          ...current.migrationIntent,
          subphase:
            scenario === 'backward' ? 'schema-applied' : 'candidate-armed',
        };
      } else if (scenario === 'schema' || scenario === 'zero-pending-schema')
        current.schemaVersion = 2;
      else if (scenario === 'candidate') delete current.pendingArtifactVersion;
      else delete current.platformTarget;
      world.fleetStore.set(current);
      world.ops.length = 0;
      const puts = world.fleetStore.puts.length;
      await expect(continueWorld(world, token)).rejects.toThrow(
        'fleet migration item no longer matches its frozen plan',
      );
      expectItemFailure(world);
      expect(world.fleetStore.puts).toHaveLength(puts);
      expect(providerMutations(world)).toEqual([]);
    }
  });

  it('READY leave and re-enter continues at its cursor, converging or refusing remaining work', async () => {
    for (const refuse of [false, true]) {
      const world = createWorld({ path: 'ready', external: true });
      const token = await advanceTo(world, 'ready-platform-resources');
      const item = world.operationStore.item();
      world.fleetStore.set({ ...world.current(), phase: 'migrating' });
      const resources = world.current().platformResources;
      if (!resources) throw new Error('missing resources');
      world.fleetStore.set({
        ...world.current(),
        phase: 'ready',
        platformResources: {
          ...resources,
          stateWorker: {
            ...resources.stateWorker,
            artifactVersion: 'foreign-provider-version',
          },
        },
        updatedAt: new Date(NOW).toISOString(),
      });
      const release = world.releases.get(world.priorRelease.physicalScriptName);
      if (!release) throw new Error('missing release');
      world.releases.set(world.priorRelease.physicalScriptName, {
        ...release,
        maintenance: { ...HEALTHY, armed: false },
      });
      if (refuse) world.setMaintenance({ ...HEALTHY, armed: false });
      const next = await continueWorld(world, token);
      expect(world.operationStore.item().planCursor).toBe(
        (item.planCursor ?? 0) + 1,
      );
      expect(world.current().platformResources).toEqual(
        world.resourcesFor(world.spec),
      );
      if (refuse) {
        await expect(continueWorld(world, next)).rejects.toThrow(
          'maintenance did not re-arm',
        );
        expectItemFailure(world);
      } else {
        expect(await drainWorld(world, next)).toMatchObject({
          status: 'complete',
        });
        expect(world.ops).toContain('maintenance');
        expect(world.ops).toContain('promote');
        expect(world.ops).toContain('settle');
        expect(world.operationStore.item().planCursor).toBe(item.plan?.length);
      }
    }
  });

  it('platform-authored DO-tag-changing FULL migration resumes through terminal commit and refuses foreign tags and fresh-admission base drift', async () => {
    const history = [
      { tag: 'v1', newClasses: ['First'] },
      { tag: 'v2', newClasses: ['Second'] },
      { tag: 'v3', newClasses: ['Third'] },
    ];
    function tagged() {
      const world = createWorld();
      world.spec = {
        ...world.spec,
        previousDurableObjectTag: 'v1',
        durableObjectMigrations: history,
      };
      world.fleetStore.set({
        ...world.current(),
        durableObjectTag: 'v1',
        durableObjectMigrationHistory: history.slice(0, 1),
        durableObjectMigrationHistoryDigest:
          durableObjectMigrationHistoryDigest(history.slice(0, 1)),
      });
      return world;
    }
    const world = tagged();
    const token = await advanceTo(world, 'settle-ready');
    const item = await loseStepResponse(world, token);
    expect(world.current()).toMatchObject({
      phase: 'ready',
      durableObjectTag: 'v3',
      durableObjectMigrationHistory: history,
      durableObjectMigrationHistoryDigest:
        durableObjectMigrationHistoryDigest(history),
    });
    world.ops.length = 0;
    const puts = world.fleetStore.puts.length;
    const retired = await continueWorld(world, token);
    expect(providerMutations(world)).toEqual([]);
    expect(world.fleetStore.puts).toHaveLength(puts);
    expect(
      world.operationStore.item().plan?.[
        world.operationStore.item().planCursor ?? -1
      ]?.step,
    ).toBe('retire-post');
    expect(await drainWorld(world, retired)).toMatchObject({
      status: 'complete',
    });
    expect(world.operationStore.item().planCursor).toBe(item.plan?.length);
    for (const mode of ['bounded', 'drain'] as const) {
      const error =
        "Durable Object migration base mismatch for cedar:production: expected 'v3'";
      if (mode === 'drain')
        await expect(migrateWorld(world)).rejects.toThrow(error);
      else {
        const start = await world.start(uuid(2), [world.current()]);
        await expect(continueWorld(world, start)).rejects.toThrow(error);
        expect(world.operationStore.item(uuid(2)).status).toBe('failed');
      }
    }
    for (const shape of [
      'consistent-intermediate',
      'bare-target',
      'bare-foreign',
      'no-history-target',
    ] as const) {
      const foreign = tagged();
      const pending = await advanceTo(foreign, 'seed-identity');
      const current = {
        ...foreign.current(),
        durableObjectTag:
          shape === 'consistent-intermediate'
            ? 'v2'
            : shape === 'bare-foreign'
              ? 'foreign'
              : 'v3',
      };
      if (shape === 'consistent-intermediate') {
        current.durableObjectMigrationHistory = history.slice(0, 2);
        current.durableObjectMigrationHistoryDigest =
          durableObjectMigrationHistoryDigest(history.slice(0, 2));
      } else if (shape === 'no-history-target') {
        delete current.durableObjectMigrationHistory;
        delete current.durableObjectMigrationHistoryDigest;
      }
      foreign.fleetStore.set(current);
      foreign.ops.length = 0;
      const writes = foreign.fleetStore.puts.length;
      const expected =
        shape === 'consistent-intermediate'
          ? "Durable Object migration base mismatch for cedar:production: expected 'v2'"
          : shape === 'no-history-target'
            ? 'platform-authored Durable Object state has no persisted migration history'
            : 'platform-authored Durable Object migration history is internally inconsistent';
      await expect(continueWorld(foreign, pending)).rejects.toThrow(expected);
      expectItemFailure(foreign);
      expect(foreign.fleetStore.puts).toHaveLength(writes);
      expect(providerMutations(foreign)).toEqual([]);
    }
  });

  it('external ordinary-plane DO-tag movement resumes FULL, PO and READY plans while preserving strict fresh-admission refusal', async () => {
    for (const path of ['full', 'platform-only', 'ready'] as const) {
      const { world, target } = finalizedWorld(path);
      const step =
        path === 'full'
          ? 'platform-resources'
          : path === 'platform-only'
            ? 'platform-only-resources'
            : 'ready-platform-resources';
      const pending = await advanceTo(world, step);
      world.ops.length = 0;
      const moved = await continueWorld(world, pending);
      expect(world.current()).toMatchObject({
        durableObjectTag: 'state-v2',
        platformResources: {
          stateWorker: { plane: 'ordinary', durableObjectTag: 'state-v2' },
        },
      });
      expect(world.current().durableObjectMigrationHistoryDigest).toBe(
        target.stateDurableObjectHistoryDigest,
      );
      expect(world.ops.indexOf('finalizedFor')).toBeGreaterThan(
        world.ops.indexOf('secrets:cedar'),
      );
      expect(world.ops.indexOf('finalizedTarget')).toBeGreaterThan(
        world.ops.indexOf('finalizedFor'),
      );
      expect(world.ops.indexOf('finalizedEnsure')).toBeGreaterThan(
        world.ops.lastIndexOf('finalizedPlan'),
      );
      expect(world.ops.indexOf('finalizedCommit')).toBeGreaterThan(
        world.ops.indexOf('finalizedEnsure'),
      );
      expect(world.ops).not.toContain('platform');
      if (path === 'full') {
        const stillMigrating = copy(world.current());
        const freshStore = new MemoryOperationStore();
        const start = await advanceFleetMigration({
          ...world.options({
            kind: 'start',
            operationId: uuid(2),
            records: [stillMigrating],
            canaryTenantTags: [],
          }),
          operationStore: freshStore,
        });
        await expect(
          advanceFleetMigration({
            ...world.options({ kind: 'continue', token: start.token }),
            operationStore: freshStore,
          }),
        ).rejects.toThrow(
          "Durable Object migration base mismatch for cedar:production: expected 'state-v2'",
        );
        await expect(migrateWorld(world)).rejects.toThrow(
          "Durable Object migration base mismatch for cedar:production: expected 'state-v2'",
        );
        expect(freshStore.item(uuid(2)).status).toBe('failed');
      }
      const completed = await drainWorld(world, moved);
      expect(completed).toMatchObject({ status: 'complete' });
      const item = world.operationStore.item();
      expect(item.planCursor).toBe(item.plan?.length);
      expect(item.plan?.at(-1)?.step).toBe(
        path === 'full'
          ? 'retire-post'
          : path === 'platform-only'
            ? 'platform-only-ready'
            : 'ready-retire-post',
      );
      expect(world.current().durableObjectTag).toBe('state-v2');
      const fresh = await world.start(uuid(3), [world.current()]);
      await expect(continueWorld(world, fresh)).rejects.toThrow(
        "Durable Object migration base mismatch for cedar:production: expected 'state-v2'",
      );
      await expect(migrateWorld(world)).rejects.toThrow(
        "Durable Object migration base mismatch for cedar:production: expected 'state-v2'",
      );
      expect(world.operationStore.item(uuid(3)).status).toBe('failed');
    }
    for (const afterReconcile of [false, true]) {
      for (const consistent of [false, true]) {
        const { world } = finalizedWorld();
        let token = await advanceTo(world, 'platform-resources');
        if (afterReconcile) token = await continueWorld(world, token);
        const current = world.current();
        if (!current.platformResources) throw new Error('missing resources');
        world.fleetStore.set({
          ...current,
          durableObjectTag: 'foreign-tag',
          ...(consistent
            ? {
                platformResources: {
                  ...current.platformResources,
                  stateWorker: {
                    ...current.platformResources.stateWorker,
                    durableObjectTag: 'foreign-tag',
                  },
                },
              }
            : {}),
        });
        world.ops.length = 0;
        const puts = world.fleetStore.puts.length;
        if (!consistent) {
          await expect(continueWorld(world, token)).rejects.toThrow(
            "Durable Object migration base mismatch for cedar:production: expected 'foreign-tag'",
          );
          expectItemFailure(world);
          expect(world.fleetStore.puts).toHaveLength(puts);
          expect(providerMutations(world)).toEqual([]);
          expect(world.ops).not.toContain('finalizedEnsure');
        } else {
          const next = await continueWorld(world, token);
          expect(world.current().durableObjectTag).toBe(
            afterReconcile ? 'foreign-tag' : 'state-v2',
          );
          expect(await drainWorld(world, next)).toMatchObject({
            status: 'complete',
          });
          expect(world.current().durableObjectTag).toBe(
            afterReconcile ? 'foreign-tag' : 'state-v2',
          );
          const fresh = await world.start(uuid(2), [world.current()]);
          await expect(continueWorld(world, fresh)).rejects.toThrow(
            `Durable Object migration base mismatch for cedar:production: expected '${afterReconcile ? 'foreign-tag' : 'state-v2'}'`,
          );
        }
      }
    }
  });
});

describe('bounded fleet migration abort signal and completion callback', () => {
  it('an already-aborted start rejects with the sentinel and writes nothing', async () => {
    const world = createWorld();
    const sentinel = new Error('host cancelled the migration');
    await expect(
      advanceFleetMigration({
        ...world.options({
          kind: 'start',
          operationId: uuid(),
          records: [world.initial],
          canaryTenantTags: [],
        }),
        signal: AbortSignal.abort(sentinel),
      }),
    ).rejects.toBe(sentinel);
    expect(providerMutations(world)).toEqual([]);
    expect(world.ops).toEqual([]);
    expect(world.operationStore.calls).toEqual([]);
    expect(world.operationStore.operations.size).toBe(0);
    expect(world.operationStore.rows.size).toBe(0);
    expect(world.fleetStore.puts).toEqual([]);
  });

  it('an abort raised before the item step leaves the operation resumable', async () => {
    const world = createWorld();
    const started = await world.start();
    const item = world.operationStore.item();
    const run = copy(world.operationStore.operations.get(uuid()));
    const sentinel = new Error('host cancelled between calls');
    const controller = new AbortController();
    world.operationStore.beforeLease = () => controller.abort(sentinel);
    world.ops.length = 0;
    world.operationStore.calls.length = 0;
    await expect(
      advanceFleetMigration({
        ...world.options({ kind: 'continue', token: started.token }),
        signal: controller.signal,
      }),
    ).rejects.toBe(sentinel);
    expect(providerMutations(world)).toEqual([]);
    expect(world.operationStore.item()).toEqual(item);
    expect(world.operationStore.operations.get(uuid())).toEqual(run);
    expect(world.operationStore.calls).toEqual([]);
    expect(world.operationStore.failures).toEqual([]);
    world.operationStore.beforeLease = undefined;
    const resumed = await advanceFleetMigration({
      ...world.options({ kind: 'continue', token: started.token }),
      signal: new AbortController().signal,
    });
    expect(await drainWorld(world, resumed)).toMatchObject({
      status: 'complete',
    });
  });

  it('completion invokes onComplete once with the returned result, after the durable finalize and with the operation lease released', async () => {
    const world = createWorld();
    const seen: unknown[] = [];
    const states: (string | undefined)[] = [];
    const held: boolean[] = [];
    armOptions(world, {
      async onComplete(result) {
        seen.push(result);
        states.push(world.operationStore.operations.get(uuid())?.state);
        held.push(world.operationStore.locked.has('migration'));
      },
    });
    const final = await drainWorld(world, await armedStart(world));
    if (final.status !== 'complete') throw new Error('expected a complete run');
    expect(seen).toEqual([final.result]);
    expect(seen[0]).toBe(final.result);
    expect(states).toEqual(['finalized']);
    expect(held).toEqual([false]);
  });

  it('a continue on the finalized operation delivers onComplete again', async () => {
    const world = createWorld();
    const seen: unknown[] = [];
    armOptions(world, {
      onComplete(result) {
        seen.push(result);
      },
    });
    const final = await drainWorld(world, await armedStart(world));
    if (final.status !== 'complete') throw new Error('expected a complete run');
    const replayed = await continueWorld(world, final);
    if (replayed.status !== 'complete')
      throw new Error('expected a complete replay');
    expect(seen).toHaveLength(2);
    expect(seen[1]).toBe(replayed.result);
    expect(replayed.result).toEqual(final.result);
    expect(
      world.operationStore.calls.filter((call) => call === 'finalize'),
    ).toHaveLength(1);
  });

  it('a replayed start carrying the same intake delivers onComplete again, with the operation lease released', async () => {
    const world = createWorld();
    const seen: unknown[] = [];
    const held: boolean[] = [];
    armOptions(world, {
      onComplete(result) {
        seen.push(result);
        held.push(world.operationStore.locked.has('migration'));
      },
    });
    const final = await drainWorld(world, await armedStart(world));
    if (final.status !== 'complete') throw new Error('expected a complete run');
    const replayed = await armedStart(world);
    if (replayed.status !== 'complete')
      throw new Error('expected a complete replay');
    expect(seen).toHaveLength(2);
    expect(seen[1]).toBe(replayed.result);
    expect(replayed.result).toEqual(final.result);
    expect(held).toEqual([false, false]);
    expect(
      world.operationStore.calls.filter((call) => call === 'finalize'),
    ).toHaveLength(1);
  });

  it.each([
    'asynchronously',
    'synchronously',
  ] as const)('an onComplete that fails %s rejects the call and leaves the operation finalized', async (mode) => {
    const world = createWorld();
    const sentinel = new Error('host completion callback failed');
    const base = world.options;
    const onComplete =
      mode === 'synchronously'
        ? () => {
            throw sentinel;
          }
        : () => Promise.reject(sentinel);
    armOptions(world, { onComplete });
    const started = await armedStart(world);
    await expect(drainWorld(world, started)).rejects.toBe(sentinel);
    expect(world.operationStore.operations.get(uuid())).toMatchObject({
      state: 'finalized',
    });
    world.options = base;
    expect(await continueWorld(world, started)).toMatchObject({
      status: 'complete',
    });
  });

  it('omitting both options drains exactly as inert ones do', async () => {
    const plain = createWorld();
    const drained = await drainWorld(plain);
    const armed = createWorld();
    const controller = new AbortController();
    const seen: unknown[] = [];
    armOptions(armed, {
      signal: controller.signal,
      onComplete(result) {
        seen.push(result);
      },
    });
    expect(await drainWorld(armed, await armedStart(armed))).toEqual(drained);
    expect(armed.ops).toEqual(plain.ops);
    expect(armed.operationStore.calls).toEqual(plain.operationStore.calls);
    expect(armed.operationStore.item()).toEqual(plain.operationStore.item());
    expect(seen).toHaveLength(1);
  });
});
