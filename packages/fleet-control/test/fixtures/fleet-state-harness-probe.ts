// SPDX-License-Identifier: Apache-2.0
/// <reference types="@cloudflare/workers-types" />

import {
  applicationBindingTopology,
  reserveApplicationR2Resources,
} from '../../src/application-bindings.js';
import { D1CloudflareApiRateCoordinator } from '../../src/cloudflare-rate-coordinator.js';
import { initialWorkerAttachmentScan } from '../../src/cloudflare-worker-attachment-scan-state.js';
import { D1FleetInventoryRunStore } from '../../src/d1-fleet-inventory-run-store.js';
import { D1FleetStateDatabase } from '../../src/d1-fleet-state-database.js';
import { advanceDecommissionDeployment } from '../../src/decommission-advance.js';
import {
  canonicalFleetInventoryRunOptions,
  emptyFleetInventoryRowCounts,
  type FleetInventoryRowKind,
  type FleetInventoryRunRecord,
  type FleetInventoryStagedFact,
  type FleetInventoryStagedRow,
  fleetInventoryOptionsDigest,
} from '../../src/fleet-inventory-state.js';
import {
  canonicalDeploymentEgressPolicy,
  externalEgressProxyScriptName,
  externalStateScriptName,
} from '../../src/platform-resources.js';
import { deploymentSpecDigest } from '../../src/spec-digest.js';
import {
  ADDED_NULLABLE_TEXT_COLUMNS,
  D1FleetStateStore,
  type FleetStateDatabase,
} from '../../src/state-store.js';
import type {
  ApplicationR2BucketSnapshot,
  ApplicationR2Resource,
  CleanupAdvanceIntent,
  CleanupTerminalReceipt,
  DatabaseExport,
  DatabaseExportReceiptIdentity,
  DatabaseReference,
  DecommissionAdvanceIntent,
  DecommissionAttachmentScanInput,
  DecommissionAttachmentScanResult,
  DeploymentSpec,
  FleetRecord,
  FleetStateLease,
  FleetStateStore,
  PlatformPlaneLease,
  PlatformPlaneResourceSet,
  ProvisioningBackend,
} from '../../src/types.js';
import {
  backendSwitchDecommissionRecordFixture,
  decommissionAdvancingRecordFixture,
} from './decommission-intent-fixture.js';

interface Env {
  DB: D1Database;
}

const STATE_TABLE = 'anchorage_fleet_deployments';
const LEASE_TABLE = 'anchorage_fleet_leases';
const PLATFORM_CLAIM_TABLE = 'anchorage_platform_plane_claims';
const PLATFORM_LEASE_TABLE = 'anchorage_platform_plane_leases';
const BOUNDED_PROVIDER_TABLE = 'anchorage_test_bounded_decommission_provider';
const CLEANUP_RECEIPT_TABLE = 'anchorage_fleet_cleanup_receipts';
const DB_NOW_MS = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";

function controlledLeaseClock(
  db: D1Database,
  leaseTable: string,
): Readonly<{
  database: FleetStateDatabase;
  advance(ms: number): void;
  allowHeartbeat(): void;
  now(): number;
  heartbeat: Promise<void>;
}> {
  const delegate = new D1FleetStateDatabase(db);
  let now = 1_000_000;
  let allowHeartbeat: (() => void) | undefined;
  const heartbeatAllowed = new Promise<void>((resolve) => {
    allowHeartbeat = resolve;
  });
  let heartbeatObserved: (() => void) | undefined;
  const heartbeat = new Promise<void>((resolve) => {
    heartbeatObserved = resolve;
  });
  const atControlledTime = (sql: string) =>
    sql.replaceAll(DB_NOW_MS, String(now));
  return {
    database: {
      async query(sql, bindings = []) {
        const isHeartbeat = sql.startsWith(`UPDATE ${leaseTable}\n`);
        if (isHeartbeat) await heartbeatAllowed;
        const rows = await delegate.query(atControlledTime(sql), bindings);
        if (isHeartbeat) heartbeatObserved?.();
        return rows;
      },
      execute: (sql, bindings = []) =>
        delegate.execute(atControlledTime(sql), bindings),
      batch: (statements) =>
        delegate.batch(
          statements.map((statement) => ({
            ...statement,
            sql: atControlledTime(statement.sql),
          })),
        ),
    },
    advance(ms) {
      now += ms;
    },
    allowHeartbeat() {
      allowHeartbeat?.();
    },
    now: () => now,
    heartbeat,
  };
}

function record(
  tenantTag: string,
  environment: string,
  suffix = `${tenantTag}-${environment}`,
): FleetRecord {
  return {
    tenantTag,
    environment,
    backend: 'plain-worker',
    scriptName: `script-${suffix}`,
    databaseId: `database-${suffix}`,
    databaseName: `database-${suffix}`,
    schemaVersion: 1,
    artifactVersion: `artifact-${suffix}`,
    desiredSpecDigest: 'a'.repeat(64),
    durableObjectBindings: [],
    routeHostname: `${suffix}.example.test`,
    phase: 'ready',
    updatedAt: '2026-08-10T00:00:00.000Z',
  };
}

function decommissionRecord(tenantTag: string, revision: number): FleetRecord {
  const base = {
    ...record(tenantTag, 'production'),
    applicationResources: [],
    applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
  };
  return decommissionAdvancingRecordFixture(base, 'ready', {
    operationId: '123e4567-e89b-42d3-a456-426614174000',
    revision,
    generation: 0,
    updatedAt: `2026-08-11T00:00:${String(revision).padStart(2, '0')}.000Z`,
  });
}

const DECOMMISSION_SCAN_EVIDENCE = 'b'.repeat(64);
const DECOMMISSION_SCAN_EVIDENCE_COUNT = 2;
const DECOMMISSION_CREATED_AT = '2026-08-30T00:00:00.000Z';
const DECOMMISSION_RECEIPT_AUTHORITY = 'd1-test://fleet-exports/receipts/v1';
const DECOMMISSION_EXPORT_SHA256 = 'c'.repeat(64);
const DECOMMISSION_EXPORT_SIZE = 37;

function boundedDatabaseId(tenantTag: 'advance' | 'advancelost'): string {
  return tenantTag === 'advance'
    ? '00000000-0000-4000-8000-000000000201'
    : '00000000-0000-4000-8000-000000000202';
}

function boundedDecommissionSpec(tenantTag: string): DeploymentSpec {
  return {
    tenantTag,
    environment: 'production',
    scriptName: `${tenantTag}-worker`,
    databaseName: `${tenantTag}-database`,
    compatibilityDate: '2026-08-30',
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    authoredBy: 'platform',
    schemaVersion: 1,
    migrations: [{ version: 1, sql: 'CREATE TABLE example (id TEXT)' }],
    durableObjectMigrations: [],
    durableObjectBindings: [],
    maintenanceBaseUrl: `https://${tenantTag}-control.example.test`,
    routeHostname: `${tenantTag}.example.test`,
    application: {
      vars: [],
      secrets: [],
      r2Buckets: [{ name: 'FILES' }],
    },
  };
}

function boundedDecommissionRecord(
  spec: DeploymentSpec,
  startAtD1 = false,
): Readonly<{ record: FleetRecord; resource: ApplicationR2Resource }> {
  const reserved = reserveApplicationR2Resources(spec)[0];
  if (!reserved) throw new Error('bounded decommission R2 reservation missing');
  const resource: ApplicationR2Resource = {
    ...reserved,
    state: startAtD1 ? 'deleted' : 'created',
    creationDate: DECOMMISSION_CREATED_AT,
  };
  return {
    resource,
    record: {
      tenantTag: spec.tenantTag,
      environment: spec.environment,
      backend: 'plain-worker',
      scriptName: spec.scriptName,
      databaseId: boundedDatabaseId(
        spec.tenantTag as 'advance' | 'advancelost',
      ),
      databaseName: spec.databaseName,
      schemaVersion: spec.schemaVersion,
      artifactVersion: `${spec.tenantTag}-artifact-v1`,
      desiredSpecDigest: deploymentSpecDigest(spec),
      applicationResources: [resource],
      applicationBindings: applicationBindingTopology(spec, [resource]),
      durableObjectBindings: [],
      routeHostname: spec.routeHostname,
      phase: startAtD1
        ? 'application-resources-deleted'
        : 'application-resources-deleting',
      updatedAt: '2026-08-29T00:00:00.000Z',
    },
  };
}

type BoundedProviderOutcome =
  | Readonly<{ status: 'fulfilled'; value?: 'default' | 'present' | 'absent' }>
  | Readonly<{
      status: 'rejected';
      reason: 'error' | 'null' | 'undefined';
    }>;

interface BoundedProviderRow {
  readonly tenant_tag: string;
  readonly database_id: string;
  readonly database_name: string;
  readonly observed_database_id: string;
  readonly observed_database_name: string;
  readonly owner: string;
  readonly database_present: number;
  readonly receipt_authority: string | null;
  readonly receipt_operation_id: string | null;
  readonly receipt_location: string | null;
  readonly receipt_size: number | null;
  readonly receipt_sha256: string | null;
  readonly receipt_commit_count: number;
  readonly export_call_count: number;
  readonly delete_count: number;
  readonly next_export_outcome: string | null;
  readonly next_delete_outcome: string | null;
  readonly next_readback_outcome: string | null;
  readonly ownership_assertion_count: number;
  readonly next_ownership_failure_ordinal: number | null;
}

async function ensureBoundedProviderState(
  db: D1Database,
  tenantTag: 'advance' | 'advancelost',
  databaseName: string,
): Promise<void> {
  await db
    .prepare(
      `CREATE TABLE IF NOT EXISTS ${BOUNDED_PROVIDER_TABLE} (
        tenant_tag TEXT PRIMARY KEY,
        database_id TEXT NOT NULL,
        database_name TEXT NOT NULL,
        observed_database_id TEXT NOT NULL,
        observed_database_name TEXT NOT NULL,
        owner TEXT NOT NULL,
        database_present INTEGER NOT NULL,
        receipt_authority TEXT,
        receipt_operation_id TEXT,
        receipt_location TEXT,
        receipt_size INTEGER,
        receipt_sha256 TEXT,
        receipt_commit_count INTEGER NOT NULL DEFAULT 0,
        export_call_count INTEGER NOT NULL DEFAULT 0,
        delete_count INTEGER NOT NULL DEFAULT 0,
        next_export_outcome TEXT,
        next_delete_outcome TEXT,
        next_readback_outcome TEXT,
        ownership_assertion_count INTEGER NOT NULL DEFAULT 0,
        next_ownership_failure_ordinal INTEGER
      )`,
    )
    .run();
  const databaseId = boundedDatabaseId(tenantTag);
  await db
    .prepare(
      `INSERT OR IGNORE INTO ${BOUNDED_PROVIDER_TABLE} (
        tenant_tag,
        database_id,
        database_name,
        observed_database_id,
        observed_database_name,
        owner,
        database_present
      ) VALUES (?, ?, ?, ?, ?, ?, 1)`,
    )
    .bind(
      tenantTag,
      databaseId,
      databaseName,
      databaseId,
      databaseName,
      tenantTag,
    )
    .run();
}

async function readBoundedProviderState(
  db: D1Database,
  tenantTag: 'advance' | 'advancelost',
): Promise<BoundedProviderRow> {
  const row = await db
    .prepare(`SELECT * FROM ${BOUNDED_PROVIDER_TABLE} WHERE tenant_tag = ?`)
    .bind(tenantTag)
    .first<BoundedProviderRow>();
  if (!row) throw new Error('bounded provider state is absent');
  return row;
}

function decodeBoundedProviderOutcome(
  value: string | null,
): BoundedProviderOutcome | undefined {
  return value === null
    ? undefined
    : (JSON.parse(value) as BoundedProviderOutcome);
}

function rejectBoundedProviderOutcome(
  outcome: Extract<BoundedProviderOutcome, { status: 'rejected' }>,
): never {
  if (outcome.reason === 'null') throw null;
  if (outcome.reason === 'undefined') throw undefined;
  throw new Error('bounded provider injected rejection');
}

class BoundedDecommissionBackend implements ProvisioningBackend {
  readonly kind = 'plain-worker' as const;
  readonly databaseExportReceiptAuthority = DECOMMISSION_RECEIPT_AUTHORITY;
  readonly events: string[] = [];
  #bucketExists: boolean;
  #deleteAttempted = false;

  constructor(
    readonly db: D1Database,
    readonly tenantTag: 'advance' | 'advancelost',
    readonly resource: ApplicationR2Resource,
    bucketExists: boolean,
    readonly scanPass: string | undefined,
    readonly options: Readonly<{
      afterScan?: 'absent' | 'id' | 'name' | 'owner';
      loseReceiptResponse?: boolean;
      loseDeleteResponse?: boolean;
    }>,
  ) {
    this.#bucketExists = bucketExists;
  }

  async advanceDecommissionAttachmentScan(
    input: DecommissionAttachmentScanInput,
  ): Promise<DecommissionAttachmentScanResult> {
    this.events.push(`scan:${this.scanPass ?? 'unexpected'}`);
    if (input.progress.target.kind === 'd1') {
      switch (this.options.afterScan) {
        case 'absent':
          await this.db
            .prepare(
              `UPDATE ${BOUNDED_PROVIDER_TABLE}
               SET database_present = 0
               WHERE tenant_tag = ?`,
            )
            .bind(this.tenantTag)
            .run();
          break;
        case 'id':
          await this.db
            .prepare(
              `UPDATE ${BOUNDED_PROVIDER_TABLE}
               SET observed_database_id = database_id || '-drift'
               WHERE tenant_tag = ?`,
            )
            .bind(this.tenantTag)
            .run();
          break;
        case 'name':
          await this.db
            .prepare(
              `UPDATE ${BOUNDED_PROVIDER_TABLE}
               SET observed_database_name = database_name || '-drift'
               WHERE tenant_tag = ?`,
            )
            .bind(this.tenantTag)
            .run();
          break;
        case 'owner':
          await this.db
            .prepare(
              `UPDATE ${BOUNDED_PROVIDER_TABLE}
               SET owner = 'foreign'
               WHERE tenant_tag = ?`,
            )
            .bind(this.tenantTag)
            .run();
          break;
      }
    }
    return {
      status: 'complete',
      evidenceSha256: DECOMMISSION_SCAN_EVIDENCE,
      evidenceCount: DECOMMISSION_SCAN_EVIDENCE_COUNT,
      providerFetchAttemptsReserved: 6,
    };
  }

  async findApplicationR2Bucket(): Promise<
    ApplicationR2BucketSnapshot | undefined
  > {
    this.events.push('r2-find');
    return this.#bucketExists
      ? {
          name: this.resource.name,
          bucketName: this.resource.bucketName,
          jurisdiction: this.resource.jurisdiction,
          creationDate: this.resource.creationDate as string,
        }
      : undefined;
  }

  async assertApplicationR2Empty(): Promise<void> {
    this.events.push('r2-empty');
  }

  async deleteApplicationR2Bucket(): Promise<void> {
    this.events.push('r2-delete');
    this.#bucketExists = false;
  }

  async assertApplicationR2Detached(): Promise<never> {
    throw new Error('bounded coordinator called legacy R2 attachment listing');
  }

  async assertDatabaseDeletionResidualsRemoved(): Promise<void> {
    this.events.push('d1-residuals');
  }

  async findDatabase(): Promise<never> {
    throw new Error('bounded coordinator unexpectedly found D1');
  }

  async getDatabase(
    databaseId: string,
  ): Promise<DatabaseReference | undefined> {
    this.events.push('d1-get');
    const row = await readBoundedProviderState(this.db, this.tenantTag);
    if (databaseId !== row.database_id) {
      throw new Error('bounded coordinator read an unexpected D1 ID');
    }
    if (this.#deleteAttempted) {
      const outcome = decodeBoundedProviderOutcome(row.next_readback_outcome);
      if (outcome) {
        await this.db
          .prepare(
            `UPDATE ${BOUNDED_PROVIDER_TABLE}
             SET next_readback_outcome = NULL
             WHERE tenant_tag = ?`,
          )
          .bind(this.tenantTag)
          .run();
        if (outcome.status === 'rejected') {
          rejectBoundedProviderOutcome(outcome);
        }
        if (outcome.value === 'absent') return undefined;
        if (outcome.value === 'present') {
          return {
            id: row.observed_database_id,
            name: row.observed_database_name,
            created: false,
          };
        }
      }
    }
    return row.database_present === 0
      ? undefined
      : {
          id: row.observed_database_id,
          name: row.observed_database_name,
          created: false,
        };
  }

  async ensureDatabase(): Promise<never> {
    throw new Error('bounded coordinator unexpectedly created D1');
  }

  async seedDeploymentIdentity(): Promise<never> {
    throw new Error('bounded coordinator unexpectedly seeded D1');
  }

  async readDeploymentIdentity(): Promise<string | undefined> {
    this.events.push('d1-owner');
    return (await readBoundedProviderState(this.db, this.tenantTag)).owner;
  }

  async applyMigrations(): Promise<never> {
    throw new Error('bounded coordinator unexpectedly migrated D1');
  }

  async deployWorker(): Promise<never> {
    throw new Error('bounded coordinator unexpectedly deployed a Worker');
  }

  async promoteWorker(): Promise<never> {
    throw new Error('bounded coordinator unexpectedly promoted a Worker');
  }

  async ensureMaintenance(): Promise<never> {
    throw new Error('bounded coordinator unexpectedly armed maintenance');
  }

  async inspect(): Promise<never> {
    throw new Error('bounded coordinator unexpectedly inspected a Worker');
  }

  async attestActiveRoute(): Promise<never> {
    throw new Error('bounded coordinator unexpectedly attested a route');
  }

  async removeTraffic(): Promise<never> {
    throw new Error('bounded coordinator unexpectedly removed traffic');
  }

  async assertTrafficRemoved(): Promise<never> {
    throw new Error('bounded coordinator unexpectedly attested traffic');
  }

  async revokeCredentials(): Promise<never> {
    throw new Error('bounded coordinator unexpectedly revoked credentials');
  }

  async deleteWorker(): Promise<never> {
    throw new Error('bounded coordinator unexpectedly deleted a Worker');
  }

  async assertDatabaseDetached(): Promise<never> {
    throw new Error('bounded coordinator called legacy D1 attachment listing');
  }

  async exportDatabase(): Promise<never> {
    throw new Error('bounded coordinator unexpectedly exported D1');
  }

  async exportDatabaseReceipt(
    identity: DatabaseExportReceiptIdentity,
  ): Promise<DatabaseExport> {
    this.events.push('d1-export');
    const row = await readBoundedProviderState(this.db, this.tenantTag);
    await this.db
      .prepare(
        `UPDATE ${BOUNDED_PROVIDER_TABLE}
         SET export_call_count = export_call_count + 1
         WHERE tenant_tag = ?`,
      )
      .bind(this.tenantTag)
      .run();
    const outcome = decodeBoundedProviderOutcome(row.next_export_outcome);
    if (outcome) {
      await this.db
        .prepare(
          `UPDATE ${BOUNDED_PROVIDER_TABLE}
           SET next_export_outcome = NULL
           WHERE tenant_tag = ?`,
        )
        .bind(this.tenantTag)
        .run();
      if (outcome.status === 'rejected') {
        rejectBoundedProviderOutcome(outcome);
      }
    }
    if (
      identity.authority !== DECOMMISSION_RECEIPT_AUTHORITY ||
      identity.databaseId !== row.database_id
    ) {
      throw new Error(
        'bounded provider received a mismatched receipt identity',
      );
    }
    const location =
      row.receipt_location ??
      `${DECOMMISSION_RECEIPT_AUTHORITY}/${identity.databaseId}/${identity.operationId}.sql`;
    if (row.receipt_operation_id === null) {
      await this.db
        .prepare(
          `UPDATE ${BOUNDED_PROVIDER_TABLE}
           SET receipt_authority = ?,
               receipt_operation_id = ?,
               receipt_location = ?,
               receipt_size = ?,
               receipt_sha256 = ?,
               receipt_commit_count = receipt_commit_count + 1
           WHERE tenant_tag = ?`,
        )
        .bind(
          identity.authority,
          identity.operationId,
          location,
          DECOMMISSION_EXPORT_SIZE,
          DECOMMISSION_EXPORT_SHA256,
          this.tenantTag,
        )
        .run();
    } else if (
      row.receipt_authority !== identity.authority ||
      row.receipt_operation_id !== identity.operationId ||
      row.receipt_size !== DECOMMISSION_EXPORT_SIZE ||
      row.receipt_sha256 !== DECOMMISSION_EXPORT_SHA256
    ) {
      throw new Error('bounded provider preserved a mismatched receipt winner');
    }
    if (this.options.loseReceiptResponse) {
      throw new Error('bounded receipt response lost');
    }
    return {
      databaseId: identity.databaseId,
      location,
      size: DECOMMISSION_EXPORT_SIZE,
      sha256: DECOMMISSION_EXPORT_SHA256,
    };
  }

  async deleteDatabase(): Promise<void> {
    this.events.push('d1-delete');
    this.#deleteAttempted = true;
    const row = await readBoundedProviderState(this.db, this.tenantTag);
    const outcome = decodeBoundedProviderOutcome(row.next_delete_outcome);
    if (outcome) {
      await this.db
        .prepare(
          `UPDATE ${BOUNDED_PROVIDER_TABLE}
           SET next_delete_outcome = NULL
           WHERE tenant_tag = ?`,
        )
        .bind(this.tenantTag)
        .run();
      if (outcome.status === 'rejected') {
        rejectBoundedProviderOutcome(outcome);
      }
    }
    await this.db
      .prepare(
        `UPDATE ${BOUNDED_PROVIDER_TABLE}
         SET database_present = ?, delete_count = delete_count + 1
         WHERE tenant_tag = ?`,
      )
      .bind(outcome?.value === 'present' ? 1 : 0, this.tenantTag)
      .run();
    if (this.options.loseDeleteResponse) {
      throw new Error('bounded delete response lost');
    }
  }
}

interface BoundedDecommissionStepInput {
  readonly tenantTag: 'advance' | 'advancelost';
  readonly operation: Readonly<
    { kind: 'start' } | { kind: 'continue'; token: unknown }
  >;
  readonly afterScan?: 'absent' | 'id' | 'name' | 'owner';
  readonly failWriteBeforeCommit?: boolean;
  readonly loseWrite?: boolean;
  readonly loseReceiptResponse?: boolean;
  readonly loseDeleteResponse?: boolean;
  readonly nextExportOutcome?: BoundedProviderOutcome;
  readonly nextDeleteOutcome?: BoundedProviderOutcome;
  readonly nextReadbackOutcome?: BoundedProviderOutcome;
  readonly nextOwnershipFailureOrdinal?: number;
  readonly seedAtD1?: boolean;
}

async function configureBoundedProviderState(
  db: D1Database,
  input: BoundedDecommissionStepInput,
): Promise<void> {
  const outcomes = [
    ['next_export_outcome', input.nextExportOutcome],
    ['next_delete_outcome', input.nextDeleteOutcome],
    ['next_readback_outcome', input.nextReadbackOutcome],
  ] as const;
  for (const [column, outcome] of outcomes) {
    if (outcome === undefined) continue;
    await db
      .prepare(
        `UPDATE ${BOUNDED_PROVIDER_TABLE}
         SET ${column} = ?
         WHERE tenant_tag = ?`,
      )
      .bind(JSON.stringify(outcome), input.tenantTag)
      .run();
  }
  if (input.nextOwnershipFailureOrdinal !== undefined) {
    await db
      .prepare(
        `UPDATE ${BOUNDED_PROVIDER_TABLE}
         SET ownership_assertion_count = 0,
             next_ownership_failure_ordinal = ?
         WHERE tenant_tag = ?`,
      )
      .bind(input.nextOwnershipFailureOrdinal, input.tenantTag)
      .run();
  }
}

function providerAssertionStore(
  db: D1Database,
  tenantTag: 'advance' | 'advancelost',
  delegate: D1FleetStateStore,
): FleetStateStore {
  return {
    get: (requestedTenantTag, environment) =>
      delegate.get(requestedTenantTag, environment),
    list: () => delegate.list(),
    withDeploymentLease: (requestedTenantTag, environment, operation) =>
      delegate.withDeploymentLease(requestedTenantTag, environment, (lease) =>
        operation({
          tenantTag: lease.tenantTag,
          environment: lease.environment,
          mutationLeaseTtlMs: lease.mutationLeaseTtlMs,
          async assertOwned() {
            const state = await readBoundedProviderState(db, tenantTag);
            const ordinal = state.ownership_assertion_count + 1;
            await db
              .prepare(
                `UPDATE ${BOUNDED_PROVIDER_TABLE}
                   SET ownership_assertion_count = ?,
                       next_ownership_failure_ordinal =
                         CASE WHEN next_ownership_failure_ordinal = ?
                           THEN NULL
                           ELSE next_ownership_failure_ordinal
                         END
                   WHERE tenant_tag = ?`,
              )
              .bind(ordinal, ordinal, tenantTag)
              .run();
            if (state.next_ownership_failure_ordinal === ordinal) {
              throw new Error('bounded provider lease ownership transferred');
            }
            await lease.assertOwned();
          },
          renew: () => lease.renew(),
          put: (record) => lease.put(record),
          delete: () => lease.delete(),
        }),
      ),
  };
}

async function boundedDecommissionStep(
  db: D1Database,
  input: BoundedDecommissionStepInput,
): Promise<unknown> {
  const spec = boundedDecommissionSpec(input.tenantTag);
  await ensureBoundedProviderState(db, input.tenantTag, spec.databaseName);
  await configureBoundedProviderState(db, input);
  const seedStore = new D1FleetStateStore(new D1FleetStateDatabase(db), {
    accountId: 'account-primary',
  });
  let current = await seedStore.get(spec.tenantTag, spec.environment);
  if (!current) {
    const seeded = boundedDecommissionRecord(spec, input.seedAtD1).record;
    await seedStore.withDeploymentLease(
      seeded.tenantTag,
      seeded.environment,
      (lease) => lease.put(seeded),
    );
    current = seeded;
  }

  const delegate = new D1FleetStateDatabase(db);
  let lostWriteCount = 0;
  let precommitWriteFailureCount = 0;
  const database: FleetStateDatabase =
    input.loseWrite || input.failWriteBeforeCommit
      ? {
          query: (sql, bindings) => delegate.query(sql, bindings),
          execute: (sql, bindings) => delegate.execute(sql, bindings),
          async batch(statements) {
            if (
              input.failWriteBeforeCommit &&
              precommitWriteFailureCount === 0
            ) {
              precommitWriteFailureCount += 1;
              throw new Error('bounded coordinator write failed before commit');
            }
            const result = await delegate.batch(statements);
            if (input.loseWrite && lostWriteCount === 0) {
              lostWriteCount += 1;
              throw new Error('bounded coordinator write response lost');
            }
            return result;
          },
        }
      : delegate;
  const store = new D1FleetStateStore(database, {
    accountId: 'account-primary',
  });
  const resource = current.applicationResources?.[0];
  if (!resource) throw new Error('bounded decommission resource missing');
  const backend = new BoundedDecommissionBackend(
    db,
    input.tenantTag,
    resource,
    resource.state !== 'deleted',
    current.decommissionIntent?.state,
    {
      afterScan: input.afterScan,
      loseReceiptResponse: input.loseReceiptResponse,
      loseDeleteResponse: input.loseDeleteResponse,
    },
  );
  const result = await advanceDecommissionDeployment({
    backend,
    store: providerAssertionStore(db, input.tenantTag, store),
    spec,
    action: input.operation,
    maxProviderRequests: 12,
    clock: () => Date.parse('2026-08-30T00:00:00.000Z'),
    randomUUID: () =>
      input.tenantTag === 'advance'
        ? '00000000-0000-4000-8000-000000000101'
        : '00000000-0000-4000-8000-000000000102',
  });
  const stored = await store.get(spec.tenantTag, spec.environment);
  const provider = await readBoundedProviderState(db, input.tenantTag);
  const claims = await db
    .prepare(
      `SELECT resource_type, resource_name, resource_role
       FROM ${PLATFORM_CLAIM_TABLE}
       WHERE resource_set_key = ?
       ORDER BY resource_type, resource_name, resource_role`,
    )
    .bind(`deployment:${spec.tenantTag}:${spec.environment}`)
    .all<{
      resource_type: string;
      resource_name: string;
      resource_role: string;
    }>();
  return {
    result,
    trace: backend.events,
    phase: stored?.phase,
    lifecyclePhase: stored?.decommissionIntent?.lifecyclePhase,
    intentState: stored?.decommissionIntent?.state,
    revision: stored?.decommissionIntent?.revision,
    generation: stored?.decommissionIntent?.generation,
    resourceStates:
      stored?.applicationResources?.map(({ state }) => state) ?? [],
    bucketName: resource.bucketName,
    lostWriteCount,
    precommitWriteFailureCount,
    provider: {
      databaseId: provider.database_id,
      databasePresent: provider.database_present === 1,
      observedDatabaseId: provider.observed_database_id,
      observedDatabaseName: provider.observed_database_name,
      owner: provider.owner,
      receiptAuthority: provider.receipt_authority,
      receiptOperationId: provider.receipt_operation_id,
      receiptLocation: provider.receipt_location,
      receiptSize: provider.receipt_size,
      receiptSha256: provider.receipt_sha256,
      receiptCommitCount: provider.receipt_commit_count,
      exportCallCount: provider.export_call_count,
      deleteCount: provider.delete_count,
      ownershipAssertionCount: provider.ownership_assertion_count,
    },
    claims: claims.results.map((claim) => ({
      resourceType: claim.resource_type,
      resourceName: claim.resource_name,
      resourceRole: claim.resource_role,
    })),
  };
}

async function resetBoundedDecommission(
  db: D1Database,
  tenantTag: 'advance' | 'advancelost',
): Promise<unknown> {
  await db
    .prepare(`DELETE FROM ${PLATFORM_CLAIM_TABLE} WHERE resource_set_key = ?`)
    .bind(`deployment:${tenantTag}:production`)
    .run();
  await db
    .prepare(
      `DELETE FROM ${STATE_TABLE}
       WHERE tenant_tag = ? AND environment = 'production'`,
    )
    .bind(tenantTag)
    .run();
  await db
    .prepare(
      `DELETE FROM ${LEASE_TABLE}
       WHERE tenant_tag = ? AND environment = 'production'`,
    )
    .bind(tenantTag)
    .run();
  await db
    .prepare(`DELETE FROM ${BOUNDED_PROVIDER_TABLE} WHERE tenant_tag = ?`)
    .bind(tenantTag)
    .run();
  return { reset: true };
}

function errorShape(error: unknown): unknown {
  if (error instanceof AggregateError) {
    return {
      name: error.name,
      message: error.message,
      errors: error.errors.map(errorShape),
    };
  }
  if (error instanceof Error) {
    return { name: error.name, message: error.message };
  }
  return { name: typeof error, message: String(error) };
}

async function clean(db: D1Database): Promise<void> {
  await db.prepare(`DELETE FROM ${STATE_TABLE}`).run();
  await db.prepare(`DELETE FROM ${LEASE_TABLE}`).run();
  await db.prepare(`DELETE FROM ${PLATFORM_CLAIM_TABLE}`).run();
  await db.prepare(`DELETE FROM ${PLATFORM_LEASE_TABLE}`).run();
}

async function readyStore(db: D1Database): Promise<D1FleetStateStore> {
  const store = new D1FleetStateStore(new D1FleetStateDatabase(db), {
    accountId: 'account-primary',
  });
  await store.get('warm', 'test');
  await clean(db);
  return store;
}

async function concurrentAcquisition(db: D1Database): Promise<unknown> {
  await readyStore(db);
  let entered = 0;
  let rejected = 0;
  let releaseWinner: (() => void) | undefined;
  let settleLosers: (() => void) | undefined;
  const winnerHeld = new Promise<void>((resolve) => {
    releaseWinner = resolve;
  });
  const losersSettled = new Promise<void>((resolve) => {
    settleLosers = resolve;
  });
  const attempts = Array.from({ length: 16 }, () => {
    const store = new D1FleetStateStore(new D1FleetStateDatabase(db), {
      accountId: 'account-primary',
    });
    return store
      .withDeploymentLease('race', 'production', async (lease) => {
        entered += 1;
        await winnerHeld;
        await lease.put(record('race', 'production'));
        return 'acquired';
      })
      .catch((error: unknown) => {
        rejected += 1;
        if (rejected === 15) settleLosers?.();
        return errorShape(error);
      });
  });
  await losersSettled;
  releaseWinner?.();
  const results = await Promise.all(attempts);
  return {
    entered,
    rejected,
    acquired: results.filter((result) => result === 'acquired').length,
  };
}

async function renewal(db: D1Database): Promise<unknown> {
  const store = await readyStore(db);
  const explicit = await store.withDeploymentLease(
    'renew',
    'production',
    async (lease) => {
      const before = await db
        .prepare(
          `SELECT expires_at - ${DB_NOW_MS} AS remaining
           FROM ${LEASE_TABLE}
           WHERE tenant_tag = 'renew' AND environment = 'production'`,
        )
        .first<{ remaining: number }>();
      await db
        .prepare(
          `UPDATE ${LEASE_TABLE}
           SET expires_at = ${DB_NOW_MS} + 60000
           WHERE tenant_tag = 'renew' AND environment = 'production'`,
        )
        .run();
      await lease.renew();
      const after = await db
        .prepare(
          `SELECT expires_at - ${DB_NOW_MS} AS remaining
           FROM ${LEASE_TABLE}
           WHERE tenant_tag = 'renew' AND environment = 'production'`,
        )
        .first<{ remaining: number }>();
      return { before: before?.remaining, after: after?.remaining };
    },
  );

  const clock = controlledLeaseClock(db, LEASE_TABLE);
  const heartbeatStore = new D1FleetStateStore(clock.database, {
    accountId: 'account-primary',
    leaseTtlMs: 2_500,
    leaseRenewalIntervalMs: 1,
  });
  let contenderRejected = false;
  await heartbeatStore.withDeploymentLease('heart', 'production', async () => {
    const originalExpiry = await clock.database.query(
      `SELECT expires_at FROM ${LEASE_TABLE}
       WHERE tenant_tag = 'heart' AND environment = 'production'`,
    );
    const originalExpiresAt = Number(originalExpiry[0]?.expires_at);
    if (!Number.isFinite(originalExpiresAt)) {
      throw new Error('deployment lease did not expose its original expiry');
    }
    clock.advance(2_000);
    clock.allowHeartbeat();
    await clock.heartbeat;
    clock.advance(600);
    try {
      await new D1FleetStateStore(clock.database, {
        accountId: 'account-primary',
        leaseTtlMs: 2_500,
        leaseRenewalIntervalMs: 1,
      }).withDeploymentLease('heart', 'production', async () => {});
    } catch {
      contenderRejected = true;
    }
    if (clock.now() <= originalExpiresAt) {
      throw new Error('controlled D1 time did not pass the original expiry');
    }
  });
  return { explicit, heartbeatObserved: true, contenderRejected };
}

async function takeoverAndFence(db: D1Database): Promise<unknown> {
  const staleStore = await readyStore(db);
  const winnerStore = new D1FleetStateStore(new D1FleetStateDatabase(db), {
    accountId: 'account-primary',
  });
  let staleLease: FleetStateLease | undefined;
  let staleReady: (() => void) | undefined;
  let finishStale: (() => void) | undefined;
  const ready = new Promise<void>((resolve) => {
    staleReady = resolve;
  });
  const finish = new Promise<void>((resolve) => {
    finishStale = resolve;
  });
  const staleRun = staleStore
    .withDeploymentLease('stale', 'production', async (lease) => {
      staleLease = lease;
      staleReady?.();
      await finish;
    })
    .then(
      () => ({ ok: true }),
      (error: unknown) => ({ ok: false, error: errorShape(error) }),
    );
  await ready;
  await db
    .prepare(
      `UPDATE ${LEASE_TABLE}
       SET expires_at = ${DB_NOW_MS} - 1
       WHERE tenant_tag = 'stale' AND environment = 'production'`,
    )
    .run();

  let stalePut: unknown;
  let staleDelete: unknown;
  let staleMutation: unknown;
  let externalMutations = 0;
  const staleRecord = record('stale', 'production', 'stale');
  await winnerStore.withDeploymentLease(
    'stale',
    'production',
    async (winner) => {
      await winner.put(record('stale', 'production', 'winner'));
      const retained = staleLease;
      if (!retained) throw new Error('stale lease was not captured');
      try {
        await retained.put(staleRecord);
      } catch (error) {
        stalePut = errorShape(error);
      }
      try {
        await retained.delete();
      } catch (error) {
        staleDelete = errorShape(error);
      }
      try {
        await retained.assertOwned();
        externalMutations += 1;
      } catch (error) {
        staleMutation = errorShape(error);
      }
    },
  );
  finishStale?.();
  const staleOutcome = await staleRun;
  const staleOnlyNames = [
    staleRecord.scriptName,
    externalStateScriptName(staleRecord),
    externalEgressProxyScriptName(staleRecord),
  ];
  const staleClaims = await db
    .prepare(
      `SELECT resource_name FROM ${PLATFORM_CLAIM_TABLE}
       WHERE account_id = 'account-primary'
         AND resource_type = 'worker-script'
         AND resource_name IN (?, ?, ?)
       ORDER BY resource_name`,
    )
    .bind(...staleOnlyNames)
    .all<{ resource_name: string }>();
  return {
    stalePut,
    staleDelete,
    staleMutation,
    externalMutations,
    staleOutcome,
    staleClaimNames: staleClaims.results.map((row) => row.resource_name),
    final: await winnerStore.get('stale', 'production'),
  };
}

async function uniqueness(db: D1Database): Promise<unknown> {
  const store = await readyStore(db);
  const rejected: string[] = [];
  const uniqueFields = [
    'scriptName',
    'databaseId',
    'databaseName',
    'routeHostname',
  ] as const;
  for (const [index, field] of uniqueFields.entries()) {
    const first = record(`u${index}a`, 'production', `unique-${index}-a`);
    const second = {
      ...record(`u${index}b`, 'production', `unique-${index}-b`),
      [field]: first[field],
    };
    await store.withDeploymentLease(
      first.tenantTag,
      first.environment,
      (lease) => lease.put(first),
    );
    try {
      await store.withDeploymentLease(
        second.tenantTag,
        second.environment,
        (lease) => lease.put(second),
      );
    } catch (error) {
      rejected.push(String(error));
    }
  }
  return { rejected, records: await store.list() };
}

async function combinedErrors(db: D1Database): Promise<unknown> {
  const store = await readyStore(db);
  const trigger = 'anchorage_fleet_release_failure';
  await db.prepare(`DROP TRIGGER IF EXISTS ${trigger}`).run();
  let failure: unknown;
  try {
    await store.withDeploymentLease('error', 'production', async () => {
      await db
        .prepare(
          `CREATE TRIGGER ${trigger}
           BEFORE DELETE ON ${LEASE_TABLE}
           WHEN OLD.tenant_tag = 'error' AND OLD.environment = 'production'
           BEGIN
             SELECT RAISE(ABORT, 'forced lease release failure');
           END`,
        )
        .run();
      throw new Error('forced operation failure');
    });
  } catch (error) {
    failure = errorShape(error);
  } finally {
    await db.prepare(`DROP TRIGGER IF EXISTS ${trigger}`).run();
    await db
      .prepare(
        `DELETE FROM ${LEASE_TABLE}
         WHERE tenant_tag = 'error' AND environment = 'production'`,
      )
      .run();
  }
  return failure;
}

const platformSet: PlatformPlaneResourceSet = {
  accountId: 'account-primary',
  dispatchNamespace: 'fleet-tenants',
  dispatchScriptName: 'fleet-dispatch',
  outboundScriptName: 'fleet-outbound',
  auditScriptName: 'fleet-audit',
  hostRoutingKvId: 'kv-hosts',
  auditQueueName: 'fleet-audit-events',
  maintenanceCapabilityPublicKey:
    '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}',
  auditDeadLetterQueue: 'fleet-audit-dlq',
};

async function forcedLifecycleErrors(
  db: D1Database,
  kind: 'deployment' | 'platform',
): Promise<unknown> {
  await readyStore(db);
  const store = new D1FleetStateStore(new D1FleetStateDatabase(db), {
    accountId: 'account-primary',
    leaseTtlMs: 1_000,
    leaseRenewalIntervalMs: 100,
  });
  const table = kind === 'deployment' ? LEASE_TABLE : PLATFORM_LEASE_TABLE;
  const updateTrigger = `anchorage_${kind}_heartbeat_failure`;
  const deleteTrigger = `anchorage_${kind}_release_failure`;
  let failure: unknown;
  try {
    const operation = async () => {
      await db
        .prepare(
          `CREATE TRIGGER ${updateTrigger}
           BEFORE UPDATE ON ${table}
           BEGIN
             SELECT RAISE(ABORT, 'forced ${kind} heartbeat failure');
           END`,
        )
        .run();
      await db
        .prepare(
          `CREATE TRIGGER ${deleteTrigger}
           BEFORE DELETE ON ${table}
           BEGIN
             SELECT RAISE(ABORT, 'forced ${kind} release failure');
           END`,
        )
        .run();
      await new Promise((resolve) => setTimeout(resolve, 300));
      throw new Error(`forced ${kind} operation failure`);
    };
    if (kind === 'deployment') {
      await store.withDeploymentLease('lifecycle', 'production', operation);
    } else {
      await store.withPlatformPlaneLease(
        platformSet,
        'anchorage:primary',
        operation,
      );
    }
  } catch (error) {
    failure = errorShape(error);
  } finally {
    await db.prepare(`DROP TRIGGER IF EXISTS ${updateTrigger}`).run();
    await db.prepare(`DROP TRIGGER IF EXISTS ${deleteTrigger}`).run();
    await db.prepare(`DELETE FROM ${table}`).run();
  }
  return failure;
}

async function lifecycleErrors(db: D1Database): Promise<unknown> {
  return {
    deployment: await forcedLifecycleErrors(db, 'deployment'),
    platform: await forcedLifecycleErrors(db, 'platform'),
  };
}

async function platformClaimsAndConcurrency(db: D1Database): Promise<unknown> {
  const store = await readyStore(db);
  let entered = 0;
  let rejected = 0;
  let releaseWinner: (() => void) | undefined;
  let settleLosers: (() => void) | undefined;
  const held = new Promise<void>((resolve) => {
    releaseWinner = resolve;
  });
  const losersSettled = new Promise<void>((resolve) => {
    settleLosers = resolve;
  });
  const attempts = Array.from({ length: 8 }, () =>
    store
      .withPlatformPlaneLease(platformSet, 'anchorage:primary', async () => {
        entered += 1;
        await held;
        return true;
      })
      .catch(() => {
        rejected += 1;
        if (rejected === 7) settleLosers?.();
        return false;
      }),
  );
  await losersSettled;
  releaseWinner?.();
  const results = await Promise.all(attempts);
  await store.withPlatformPlaneLease(
    platformSet,
    'anchorage:primary',
    async () => {},
  );
  const collisions: unknown[] = [];
  const foreignBase: PlatformPlaneResourceSet = {
    accountId: platformSet.accountId,
    dispatchNamespace: 'other-tenants',
    dispatchScriptName: 'other-dispatch',
    outboundScriptName: 'other-outbound',
    auditScriptName: 'other-audit',
    hostRoutingKvId: 'other-kv',
    auditQueueName: 'other-audit-events',
    maintenanceCapabilityPublicKey: platformSet.maintenanceCapabilityPublicKey,
    auditDeadLetterQueue: 'other-audit-dlq',
  };
  for (const resourceSet of [
    { ...foreignBase, hostRoutingKvId: platformSet.hostRoutingKvId },
    { ...foreignBase, auditQueueName: platformSet.auditQueueName },
    {
      ...foreignBase,
      auditDeadLetterQueue: platformSet.auditDeadLetterQueue,
    },
  ]) {
    try {
      await store.withPlatformPlaneLease(
        resourceSet,
        'anchorage:foreign',
        async () => {},
      );
    } catch (error) {
      collisions.push(errorShape(error));
    }
  }
  const claimCount = await db
    .prepare(`SELECT COUNT(*) AS count FROM ${PLATFORM_CLAIM_TABLE}`)
    .first<{ count: number }>();
  return {
    entered,
    acquired: results.filter(Boolean).length,
    collisions,
    claimCount: claimCount?.count,
  };
}

async function platformTakeoverAndFence(db: D1Database): Promise<unknown> {
  const staleStore = await readyStore(db);
  const winnerStore = new D1FleetStateStore(new D1FleetStateDatabase(db), {
    accountId: 'account-primary',
  });
  let staleLease: PlatformPlaneLease | undefined;
  let staleReady: (() => void) | undefined;
  let finishStale: (() => void) | undefined;
  const ready = new Promise<void>((resolve) => {
    staleReady = resolve;
  });
  const finish = new Promise<void>((resolve) => {
    finishStale = resolve;
  });
  const staleRun = staleStore
    .withPlatformPlaneLease(platformSet, 'anchorage:primary', async (lease) => {
      staleLease = lease;
      staleReady?.();
      await finish;
    })
    .then(
      () => ({ ok: true }),
      (error: unknown) => ({ ok: false, error: errorShape(error) }),
    );
  await ready;
  await db
    .prepare(`UPDATE ${PLATFORM_LEASE_TABLE} SET expires_at = ${DB_NOW_MS} - 1`)
    .run();
  let staleMutation: unknown;
  let externalMutations = 0;
  await winnerStore.withPlatformPlaneLease(
    platformSet,
    'anchorage:primary',
    async () => {
      const retained = staleLease;
      if (!retained) throw new Error('stale platform lease was not captured');
      try {
        await retained.assertOwned();
        externalMutations += 1;
      } catch (error) {
        staleMutation = errorShape(error);
      }
    },
  );
  finishStale?.();
  return {
    staleMutation,
    externalMutations,
    staleOutcome: await staleRun,
  };
}

async function platformRenewal(db: D1Database): Promise<unknown> {
  await readyStore(db);
  const clock = controlledLeaseClock(db, PLATFORM_LEASE_TABLE);
  const heartbeatStore = new D1FleetStateStore(clock.database, {
    accountId: 'account-primary',
    leaseTtlMs: 2_500,
    leaseRenewalIntervalMs: 1,
  });
  let contenderRejected = false;
  await heartbeatStore.withPlatformPlaneLease(
    platformSet,
    'anchorage:primary',
    async () => {
      const originalExpiry = await clock.database.query(
        `SELECT expires_at FROM ${PLATFORM_LEASE_TABLE}`,
      );
      const originalExpiresAt = Number(originalExpiry[0]?.expires_at);
      if (!Number.isFinite(originalExpiresAt)) {
        throw new Error('platform lease did not expose its original expiry');
      }
      clock.advance(2_000);
      clock.allowHeartbeat();
      await clock.heartbeat;
      clock.advance(600);
      try {
        await new D1FleetStateStore(clock.database, {
          accountId: 'account-primary',
          leaseTtlMs: 2_500,
          leaseRenewalIntervalMs: 1,
        }).withPlatformPlaneLease(
          platformSet,
          'anchorage:primary',
          async () => {},
        );
      } catch {
        contenderRejected = true;
      }
      if (clock.now() <= originalExpiresAt) {
        throw new Error('controlled D1 time did not pass the original expiry');
      }
    },
  );
  return { heartbeatObserved: true, contenderRejected };
}

async function crossPlaneClaimExclusion(db: D1Database): Promise<unknown> {
  const store = await readyStore(db);
  const platformFirst = {
    ...platformSet,
    dispatchScriptName: 'plain-platform-first',
  };
  await store.withPlatformPlaneLease(
    platformFirst,
    'platform-first',
    async () => {},
  );
  let deploymentCollision: unknown;
  try {
    const deployment = {
      ...record('claima', 'production'),
      backend: 'plain-worker' as const,
      scriptName: platformFirst.dispatchScriptName,
    };
    await store.withDeploymentLease(
      deployment.tenantTag,
      deployment.environment,
      (lease) => lease.put(deployment),
    );
  } catch (error) {
    deploymentCollision = errorShape(error);
  }

  const plain = {
    ...record('claimb', 'production'),
    backend: 'plain-worker' as const,
    scriptName: 'plain-deployment-first',
  };
  await store.withDeploymentLease(plain.tenantTag, plain.environment, (lease) =>
    lease.put(plain),
  );
  const externalPolicy = canonicalDeploymentEgressPolicy({
    policyId: 'policy-claimc',
    tenantTag: 'claimc',
    environment: 'production',
    allowedHosts: [],
  });
  const external = {
    ...record('claimc', 'production'),
    backend: 'workers-for-platforms' as const,
    scriptName: plain.scriptName,
    outboundPolicy: externalPolicy,
    platformResources: {
      maintenanceCapabilityPublicKey:
        platformSet.maintenanceCapabilityPublicKey,
      outboundPolicy: externalPolicy,
      stateWorker: {
        scriptName: plain.scriptName,
        artifactVersion: 'bridge-version',
        artifactDigest: 'a'.repeat(64),
        plane: 'ordinary' as const,
        durableObjectBindings: [],
        namespaceIds: [],
      },
      egressProxy: {
        scriptName: 'claimc-production-egress',
        artifactVersion: 'bridge-egress-version',
        artifactDigest: 'c'.repeat(64),
        ...externalPolicy,
      },
    },
    platformTarget: {
      maintenanceCapabilityPublicKey:
        platformSet.maintenanceCapabilityPublicKey,
      stateArtifactDigest: 'a'.repeat(64),
      stateDurableObjectHistoryDigest: 'b'.repeat(64),
      egressArtifactDigest: 'c'.repeat(64),
      d1SchemaVersion: 1,
      d1SchemaHistoryDigest: 'd'.repeat(64),
      outboundPolicy: externalPolicy,
    },
  };
  let bridgeCollision: unknown;
  try {
    await store.withDeploymentLease(
      external.tenantTag,
      external.environment,
      (lease) => lease.put(external),
    );
  } catch (error) {
    bridgeCollision = errorShape(error);
  }
  let platformCollision: unknown;
  try {
    await store.withPlatformPlaneLease(
      {
        accountId: platformSet.accountId,
        dispatchNamespace: 'fleet-tenants-inverse',
        dispatchScriptName: plain.scriptName,
        outboundScriptName: 'fleet-outbound-inverse',
        auditScriptName: 'fleet-audit-inverse',
        hostRoutingKvId: 'kv-hosts-inverse',
        auditQueueName: 'fleet-audit-events-inverse',
        maintenanceCapabilityPublicKey:
          platformSet.maintenanceCapabilityPublicKey,
      },
      'platform-inverse',
      async () => {},
    );
  } catch (error) {
    platformCollision = errorShape(error);
  }
  const deploymentRows = await db
    .prepare(`SELECT script_name FROM ${STATE_TABLE} ORDER BY script_name`)
    .all<{ script_name: string }>();
  return {
    deploymentCollision,
    bridgeCollision,
    platformCollision,
    scriptNames: deploymentRows.results.map((row) => row.script_name),
  };
}

async function atomicClaimBatch(db: D1Database): Promise<unknown> {
  await readyStore(db);
  const delegate = new D1FleetStateDatabase(db);
  let loseResponse = true;
  const lostResponseDatabase: FleetStateDatabase = {
    query: (sql, bindings) => delegate.query(sql, bindings),
    execute: (sql, bindings) => delegate.execute(sql, bindings),
    async batch(statements) {
      const result = await delegate.batch(statements);
      if (loseResponse) {
        loseResponse = false;
        throw new Error('committed D1 batch response lost');
      }
      return result;
    },
  };
  const store = new D1FleetStateStore(lostResponseDatabase, {
    accountId: 'account-primary',
  });
  const applicationResources = Array.from({ length: 32 }, (_, index) => ({
    name: `BUCKET_${String(index).padStart(2, '0')}`,
    bucketName: `bucket-${String(index).padStart(2, '0')}`,
    jurisdiction: 'default' as const,
    state: 'reserved' as const,
    reservationNonce: String(index).padStart(32, '0'),
  }));
  const intended: FleetRecord = {
    ...record('atomic', 'production'),
    phase: 'database-reserved',
    applicationResources,
    applicationBindings: {
      vars: [],
      secrets: [],
      r2Buckets: applicationResources.map(
        ({ name, bucketName, jurisdiction }) => ({
          name,
          bucketName,
          jurisdiction,
        }),
      ),
    },
  };
  await store.withDeploymentLease('atomic', 'production', (lease) =>
    lease.put(intended),
  );
  const switchedPolicy = canonicalDeploymentEgressPolicy({
    policyId: 'policy-atomic',
    tenantTag: 'atomic',
    environment: 'production',
    allowedHosts: [],
  });
  const switched: FleetRecord = {
    ...intended,
    backend: 'workers-for-platforms',
    phase: 'ready',
    outboundPolicy: switchedPolicy,
    platformResources: {
      maintenanceCapabilityPublicKey:
        platformSet.maintenanceCapabilityPublicKey,
      outboundPolicy: switchedPolicy,
      stateWorker: {
        scriptName: intended.scriptName,
        artifactVersion: 'bridge-version',
        artifactDigest: 'a'.repeat(64),
        plane: 'ordinary',
        durableObjectBindings: [],
        namespaceIds: [],
      },
      egressProxy: {
        scriptName: 'atomic-production-egress',
        artifactVersion: 'bridge-egress-version',
        artifactDigest: 'c'.repeat(64),
        ...switchedPolicy,
      },
    },
    platformTarget: {
      maintenanceCapabilityPublicKey:
        platformSet.maintenanceCapabilityPublicKey,
      stateArtifactDigest: 'a'.repeat(64),
      stateDurableObjectHistoryDigest: 'b'.repeat(64),
      egressArtifactDigest: 'c'.repeat(64),
      d1SchemaVersion: 1,
      d1SchemaHistoryDigest: 'd'.repeat(64),
      outboundPolicy: switchedPolicy,
    },
  };
  await store.withDeploymentLease('atomic', 'production', (lease) =>
    lease.put(switched),
  );
  const switchedClaim = await db
    .prepare(
      `SELECT resource_role FROM ${PLATFORM_CLAIM_TABLE}
       WHERE resource_set_key = ? AND resource_type = 'worker-script'`,
    )
    .bind('deployment:atomic:production')
    .first<{ resource_role: string }>();
  const rolledBack = { ...intended, phase: 'ready' as const };
  await store.withDeploymentLease('atomic', 'production', (lease) =>
    lease.put(rolledBack),
  );
  const claims = await db
    .prepare(
      `SELECT resource_type, resource_name FROM ${PLATFORM_CLAIM_TABLE}
       WHERE resource_set_key = ? ORDER BY resource_type, resource_name`,
    )
    .bind('deployment:atomic:production')
    .all();
  return {
    record: await store.get('atomic', 'production'),
    claimCount: claims.results.length,
    switchedRole: switchedClaim?.resource_role,
  };
}

async function finalLeaseAssertionRollback(db: D1Database): Promise<unknown> {
  const store = await readyStore(db);
  await db
    .prepare(
      `CREATE TRIGGER expire_fleet_lease_after_claim
       AFTER INSERT ON ${PLATFORM_CLAIM_TABLE}
       BEGIN
         UPDATE ${LEASE_TABLE} SET expires_at = 0
         WHERE tenant_tag = 'lease-boundary' AND environment = 'production';
       END`,
    )
    .run();
  let failure: unknown;
  try {
    await store.withDeploymentLease('lease-boundary', 'production', (lease) =>
      lease.put(record('lease-boundary', 'production')),
    );
  } catch (error) {
    failure = errorShape(error);
  }
  const rows = await db
    .prepare(
      `SELECT COUNT(*) AS count FROM ${STATE_TABLE}
       WHERE tenant_tag = 'lease-boundary' AND environment = 'production'`,
    )
    .all<{ count: number }>();
  const claims = await db
    .prepare(
      `SELECT COUNT(*) AS count FROM ${PLATFORM_CLAIM_TABLE}
       WHERE resource_set_key = 'deployment:lease-boundary:production'`,
    )
    .all<{ count: number }>();
  await db.prepare('DROP TRIGGER expire_fleet_lease_after_claim').run();
  return {
    failure,
    rows: rows.results[0]?.count,
    claims: claims.results[0]?.count,
  };
}

const SWITCH_LOST_TENANT = 'switchlost';
const SWITCH_LOST_RECEIPT_AUTHORITY =
  'memory://fleet-exports/backend-switch/receipts/v1';
const SWITCH_LOST_EXPORT: DatabaseExport = {
  databaseId: 'database-switchlost-production',
  location: 'memory://fleet-exports/backend-switch/switchlost.sql',
  sha256: 'f'.repeat(64),
  size: 37,
};

type BackendSwitchLostWriteStage =
  | 'reset'
  | 'start'
  | 'cursor'
  | 'receipt'
  | 'barrier'
  | 'terminal';

function backendSwitchLostWriteSource(): FleetRecord {
  const base = record(SWITCH_LOST_TENANT, 'production');
  const outboundPolicy = canonicalDeploymentEgressPolicy({
    policyId: 'policy-switchlost',
    tenantTag: base.tenantTag,
    environment: base.environment,
    allowedHosts: [],
  });
  const target = {
    maintenanceCapabilityPublicKey: platformSet.maintenanceCapabilityPublicKey,
    stateArtifactDigest: 'a'.repeat(64),
    stateDurableObjectHistoryDigest: 'b'.repeat(64),
    egressArtifactDigest: 'c'.repeat(64),
    d1SchemaVersion: base.schemaVersion,
    d1SchemaHistoryDigest: 'd'.repeat(64),
    outboundPolicy,
  } as const;
  const current: FleetRecord = {
    ...base,
    backend: 'workers-for-platforms',
    outboundPolicy,
    platformTarget: target,
    platformResources: {
      maintenanceCapabilityPublicKey:
        platformSet.maintenanceCapabilityPublicKey,
      outboundPolicy,
      stateWorker: {
        scriptName: base.scriptName,
        artifactVersion: 'bridge-switchlost-v1',
        artifactDigest: target.stateArtifactDigest,
        plane: 'ordinary',
        durableObjectBindings: [],
        namespaceIds: [],
      },
      egressProxy: {
        scriptName: 'switchlost-production-egress',
        artifactVersion: 'egress-switchlost-v1',
        artifactDigest: target.egressArtifactDigest,
        ...outboundPolicy,
      },
    },
    applicationResources: [],
    applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
  };
  const prior = {
    scriptName: current.scriptName,
    artifactVersion: current.artifactVersion,
    specDigest: current.desiredSpecDigest,
    databaseId: current.databaseId,
    databaseName: current.databaseName,
    durableObjectBindings: [],
    namespaceIds: [],
    secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
    application: { vars: [], secrets: [], r2Buckets: [] },
    applicationResources: [],
    customDomain: {
      id: 'domain-switchlost',
      hostname: current.routeHostname,
    },
  } as const;
  return backendSwitchDecommissionRecordFixture(
    current,
    {
      kind: 'backend-switch',
      tenantTag: current.tenantTag,
      environment: current.environment,
      prior,
      targetSpecDigest: current.desiredSpecDigest,
      targetApplication: { vars: [], secrets: [], r2Buckets: [] },
      target,
      rollbackUntil: '2026-09-30T00:00:00.000Z',
      subphase: 'decommission-export-authorized',
      applicationR2Progress: [],
      decommissionSnapshot: {
        prior,
        restoredArtifactVersion: null,
        entryPendingArtifactVersion: null,
        entryPendingNamespaceIds: null,
        providerTargetSpecDigest: current.desiredSpecDigest,
        routeHostname: current.routeHostname,
        routeTargets: [],
        desiredSpecDigest: current.desiredSpecDigest,
        target,
        releases: [],
        applicationResources: [],
      },
    },
    { subphase: 'decommission-export-authorized' },
  );
}

function backendSwitchShellCommon(
  shell: DecommissionAdvanceIntent,
  revision: number,
  updatedAt: string,
): Omit<
  Extract<DecommissionAdvanceIntent, { readonly state: 'transitioning' }>,
  'lifecyclePhase' | 'state'
> {
  return {
    version: 1,
    operationId: shell.operationId,
    revision,
    generation: shell.generation,
    updatedAt,
    identity: shell.identity,
    ...(shell.databaseExportReceiptAuthority
      ? {
          databaseExportReceiptAuthority: shell.databaseExportReceiptAuthority,
        }
      : {}),
  };
}

function backendSwitchLostWriteNext(
  current: FleetRecord,
  stage: Exclude<BackendSwitchLostWriteStage, 'reset' | 'start'>,
): FleetRecord {
  const intent = current.backendSwitchIntent;
  const shell = current.decommissionIntent;
  if (!intent || !shell || shell.state === 'complete') {
    throw new Error('bounded backend-switch harness authority is absent');
  }
  const revision = shell.revision + 1;
  const updatedAt = `2026-08-30T00:00:${String(revision).padStart(2, '0')}.000Z`;
  const common = backendSwitchShellCommon(shell, revision, updatedAt);
  if (stage === 'cursor') {
    const decommissionIntent: DecommissionAdvanceIntent = {
      ...common,
      databaseExportReceiptAuthority: SWITCH_LOST_RECEIPT_AUTHORITY,
      lifecyclePhase: 'application-resources-deleted',
      state: 'discover',
      purpose: {
        kind: 'database-pre-export',
        databaseId: current.databaseId,
      },
      progress: initialWorkerAttachmentScan({
        kind: 'd1',
        databaseId: current.databaseId,
      }),
    };
    return {
      ...current,
      decommissionIntent,
      updatedAt,
    };
  }
  if (stage === 'receipt') {
    const decommissionIntent: DecommissionAdvanceIntent = {
      ...common,
      databaseExportReceiptAuthority: SWITCH_LOST_RECEIPT_AUTHORITY,
      lifecyclePhase: 'database-exported',
      state: 'transitioning',
    };
    return {
      ...current,
      backendSwitchIntent: {
        ...intent,
        subphase: 'decommission-exported',
        databaseExport: SWITCH_LOST_EXPORT,
      },
      decommissionIntent,
      databaseExportLocation: SWITCH_LOST_EXPORT.location,
      databaseExportSha256: SWITCH_LOST_EXPORT.sha256,
      databaseExportSize: SWITCH_LOST_EXPORT.size,
      updatedAt,
    };
  }
  if (stage === 'barrier') {
    const decommissionIntent: DecommissionAdvanceIntent = {
      ...common,
      databaseExportReceiptAuthority: SWITCH_LOST_RECEIPT_AUTHORITY,
      lifecyclePhase: 'database-deleting',
      state: 'transitioning',
    };
    return {
      ...current,
      backendSwitchIntent: {
        ...intent,
        subphase: 'decommission-database-authorized',
      },
      decommissionIntent,
      updatedAt,
    };
  }
  return backendSwitchDecommissionRecordFixture(
    current,
    {
      ...intent,
      subphase: 'decommissioned',
      databaseExport: SWITCH_LOST_EXPORT,
      applicationR2Progress: [],
    },
    {
      operationId: shell.operationId,
      revision,
      generation: shell.generation,
      ...(intent.decommissionEntrySubphase
        ? { entrySubphase: intent.decommissionEntrySubphase }
        : {}),
      subphase: 'decommissioned',
      updatedAt,
    },
  );
}

async function backendSwitchLostWriteStep(
  db: D1Database,
  input: Readonly<{
    stage: BackendSwitchLostWriteStage;
    loseWrite?: boolean;
  }>,
): Promise<unknown> {
  if (input.stage === 'reset') {
    await readyStore(db);
    await db
      .prepare(
        `DELETE FROM ${STATE_TABLE} WHERE tenant_tag = ? AND environment = 'production'`,
      )
      .bind(SWITCH_LOST_TENANT)
      .run();
    await db
      .prepare(
        `DELETE FROM ${LEASE_TABLE} WHERE tenant_tag = ? AND environment = 'production'`,
      )
      .bind(SWITCH_LOST_TENANT)
      .run();
    await db
      .prepare(`DELETE FROM ${PLATFORM_CLAIM_TABLE} WHERE resource_set_key = ?`)
      .bind(`deployment:${SWITCH_LOST_TENANT}:production`)
      .run();
    return { reset: true };
  }
  const delegate = new D1FleetStateDatabase(db);
  let lostWriteCount = 0;
  const database: FleetStateDatabase = {
    query: (sql, bindings = []) => delegate.query(sql, bindings),
    execute: (sql, bindings = []) => delegate.execute(sql, bindings),
    async batch(statements) {
      const results = await delegate.batch(statements);
      if (
        input.loseWrite &&
        lostWriteCount === 0 &&
        statements.some(({ sql }) => sql.includes(`INSERT INTO ${STATE_TABLE}`))
      ) {
        lostWriteCount += 1;
        throw new Error('bounded backend-switch D1 write response lost');
      }
      return results;
    },
  };
  const store = new D1FleetStateStore(database, {
    accountId: 'account-primary',
  });
  const current = await store.get(SWITCH_LOST_TENANT, 'production');
  const next =
    input.stage === 'start'
      ? backendSwitchLostWriteSource()
      : current
        ? backendSwitchLostWriteNext(current, input.stage)
        : (() => {
            throw new Error('bounded backend-switch harness record is absent');
          })();
  await store.withDeploymentLease(SWITCH_LOST_TENANT, 'production', (lease) =>
    lease.put(next),
  );
  const persisted = await new D1FleetStateStore(new D1FleetStateDatabase(db), {
    accountId: 'account-primary',
  }).get(SWITCH_LOST_TENANT, 'production');
  const raw = await db
    .prepare(
      `SELECT backend_switch_intent, decommission_intent
       FROM ${STATE_TABLE}
       WHERE tenant_tag = ? AND environment = 'production'`,
    )
    .bind(SWITCH_LOST_TENANT)
    .first<{
      backend_switch_intent: string | null;
      decommission_intent: string | null;
    }>();
  return {
    lostWriteCount,
    phase: persisted?.phase,
    switchSubphase: persisted?.backendSwitchIntent?.subphase,
    shellState: persisted?.decommissionIntent?.state,
    shellRevision: persisted?.decommissionIntent?.revision,
    lifecyclePhase: persisted?.decommissionIntent?.lifecyclePhase,
    scanStage:
      persisted?.decommissionIntent?.state === 'discover' ||
      persisted?.decommissionIntent?.state === 'verify'
        ? persisted.decommissionIntent.progress.stage
        : undefined,
    databaseExportLocation: persisted?.databaseExportLocation,
    columnsPresent:
      typeof raw?.backend_switch_intent === 'string' &&
      typeof raw.decommission_intent === 'string',
  };
}

async function backendSwitchColumnUpgrade(db: D1Database): Promise<unknown> {
  const store = await readyStore(db);
  const base = record('legacyswitch', 'production');
  const policy = canonicalDeploymentEgressPolicy({
    policyId: 'policy-legacyswitch',
    tenantTag: base.tenantTag,
    environment: base.environment,
    allowedHosts: [],
  });
  const legacy: FleetRecord = {
    ...base,
    backendSwitchIntent: {
      kind: 'backend-switch',
      tenantTag: base.tenantTag,
      environment: base.environment,
      prior: {
        scriptName: base.scriptName,
        artifactVersion: base.artifactVersion,
        specDigest: base.desiredSpecDigest,
        databaseId: base.databaseId,
        databaseName: base.databaseName,
        durableObjectBindings: [],
        namespaceIds: [],
        secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
        applicationResources: [],
        customDomain: {
          id: 'legacy-domain',
          hostname: base.routeHostname,
        },
      },
      targetSpecDigest: base.desiredSpecDigest,
      targetApplication: { vars: [], secrets: [], r2Buckets: [] },
      target: {
        maintenanceCapabilityPublicKey:
          platformSet.maintenanceCapabilityPublicKey,
        stateArtifactDigest: 'b'.repeat(64),
        stateDurableObjectHistoryDigest: 'c'.repeat(64),
        stateEgressCredentialDigest: 'd'.repeat(64),
        sharedOutboundWorkerName: 'fleet-outbound',
        d1SchemaVersion: base.schemaVersion,
        d1SchemaHistoryDigest: 'e'.repeat(64),
        outboundPolicy: policy,
      },
      rollbackUntil: '2026-08-20T00:00:00.000Z',
      subphase: 'finalized',
    },
  };
  await store.withDeploymentLease(base.tenantTag, base.environment, (lease) =>
    lease.put(legacy),
  );
  await db
    .prepare(
      `UPDATE ${STATE_TABLE}
       SET migration_intent = backend_switch_intent,
           backend_switch_intent = NULL
       WHERE tenant_tag = ? AND environment = ?`,
    )
    .bind(base.tenantTag, base.environment)
    .run();

  const upgraded = await new D1FleetStateStore(new D1FleetStateDatabase(db), {
    accountId: 'account-primary',
  }).get(base.tenantTag, base.environment);
  const raw = await db
    .prepare(
      `SELECT migration_intent, backend_switch_intent
       FROM ${STATE_TABLE}
       WHERE tenant_tag = ? AND environment = ?`,
    )
    .bind(base.tenantTag, base.environment)
    .first<{
      migration_intent: string | null;
      backend_switch_intent: string | null;
    }>();
  return {
    subphase: upgraded?.backendSwitchIntent?.subphase,
    migrationIntent: raw?.migration_intent,
    backendSwitchKind: raw?.backend_switch_intent
      ? (JSON.parse(raw.backend_switch_intent) as { kind?: unknown }).kind
      : undefined,
  };
}

async function decommissionIntentColumnUpgrade(
  db: D1Database,
): Promise<unknown> {
  await readyStore(db);
  await db
    .prepare(`ALTER TABLE ${STATE_TABLE} DROP COLUMN decommission_intent`)
    .run();
  const store = new D1FleetStateStore(new D1FleetStateDatabase(db), {
    accountId: 'account-primary',
  });
  const intended = decommissionRecord('intentupgrade', 1);
  await store.withDeploymentLease(
    intended.tenantTag,
    intended.environment,
    (lease) => lease.put(intended),
  );
  const columns = await db
    .prepare(`PRAGMA table_info(${STATE_TABLE})`)
    .all<{ name: string }>();
  const upgraded = await store.get(intended.tenantTag, intended.environment);
  return {
    phase: upgraded?.phase,
    revision: upgraded?.decommissionIntent?.revision,
    columns: columns.results
      .map(({ name }) => name)
      .filter((name) => ADDED_NULLABLE_TEXT_COLUMNS.includes(name as never)),
  };
}

async function decommissionIntentLostResponse(
  db: D1Database,
): Promise<unknown> {
  await readyStore(db);
  const delegate = new D1FleetStateDatabase(db);
  let lose: 'exact' | 'changed' | undefined = 'exact';
  const database: FleetStateDatabase = {
    query: (sql, bindings) => delegate.query(sql, bindings),
    execute: (sql, bindings) => delegate.execute(sql, bindings),
    async batch(statements) {
      const results = await delegate.batch(statements);
      const failure = lose;
      if (!failure) return results;
      lose = undefined;
      if (failure === 'changed') {
        await db
          .prepare(
            `UPDATE ${STATE_TABLE}
             SET decommission_intent = json_set(
               decommission_intent, '$.revision', 3
             )
             WHERE tenant_tag = 'intentlost'
               AND environment = 'production'`,
          )
          .run();
      }
      throw new Error(`committed ${failure} D1 batch response lost`);
    },
  };
  const store = new D1FleetStateStore(database, {
    accountId: 'account-primary',
  });
  const first = decommissionRecord('intentlost', 1);
  await store.withDeploymentLease(first.tenantTag, first.environment, (lease) =>
    lease.put(first),
  );
  const exact = await store.get(first.tenantTag, first.environment);

  lose = 'changed';
  let changedFailure: unknown;
  try {
    await store.withDeploymentLease(
      first.tenantTag,
      first.environment,
      (lease) => lease.put(decommissionRecord('intentlost', 2)),
    );
  } catch (error) {
    changedFailure = errorShape(error);
  }
  const final = await store.get(first.tenantTag, first.environment);
  const claim = await db
    .prepare(
      `SELECT COUNT(*) AS count FROM ${PLATFORM_CLAIM_TABLE}
       WHERE resource_set_key = 'deployment:intentlost:production'`,
    )
    .first<{ count: number }>();
  return {
    exactRevision: exact?.decommissionIntent?.revision,
    changedFailure,
    finalRevision: final?.decommissionIntent?.revision,
    claimCount: Number(claim?.count),
  };
}

function cleanupIntentFor(base: FleetRecord): CleanupAdvanceIntent {
  return {
    version: 1,
    operationId: '00000000-0000-4000-8000-0000000000aa',
    revision: 0,
    generation: 0,
    updatedAt: '2026-08-11T00:00:00.000Z',
    authority: { kind: 'manual-cleanup' },
    identity: {
      record: {
        tenantTag: base.tenantTag,
        environment: base.environment,
        backend: base.backend,
        scriptName: base.scriptName,
        databaseId: base.databaseId,
        databaseName: base.databaseName,
        routeHostname: base.routeHostname,
      },
      admittedPhase: 'worker-deployed',
      externalArtifact: false,
    },
    state: { step: 'database-deletion' },
  };
}

function cleanupAdvancingFixture(
  tenantTag: string,
  operationId?: string,
): FleetRecord {
  const base = {
    ...record(tenantTag, 'production'),
    phase: 'cleanup-advancing' as const,
    applicationResources: [],
    applicationBindings: { vars: [], secrets: [], r2Buckets: [] },
  };
  const intent = cleanupIntentFor(base);
  return {
    ...base,
    cleanupIntent: operationId ? { ...intent, operationId } : intent,
  };
}

function cleanupReceiptFor(active: FleetRecord): CleanupTerminalReceipt {
  const intent = active.cleanupIntent;
  if (!intent) throw new Error('fixture record has no cleanup intent');
  return {
    version: 1,
    operationId: intent.operationId,
    tenantTag: active.tenantTag,
    environment: active.environment,
    backend: active.backend,
    scriptName: active.scriptName,
    databaseId: active.databaseId,
    databaseName: active.databaseName,
    authority: 'manual-cleanup',
    admittedPhase: intent.identity.admittedPhase,
    disposition: 'prepublication-owned-no-export',
    evidence: {
      eligibility: 'carrier-null',
      ingressRemoved: true,
      workerAbsent: true,
      platformResourcesAbsent: true,
      applicationR2Settled: true,
      databaseAbsentReadback: true,
      scan: {
        discover: { evidenceSha256: 'b'.repeat(64), evidenceCount: 2 },
        verify: { evidenceSha256: 'b'.repeat(64), evidenceCount: 2 },
      },
    },
  };
}

async function cleanupClaimCount(
  db: D1Database,
  tenantTag: string,
): Promise<number> {
  const row = await db
    .prepare(
      `SELECT COUNT(*) AS count FROM ${PLATFORM_CLAIM_TABLE}
       WHERE resource_set_key = ?`,
    )
    .bind(`deployment:${tenantTag}:production`)
    .first<{ count: number }>();
  return Number(row?.count);
}

async function cleanupReceiptCount(db: D1Database): Promise<number> {
  const row = await db
    .prepare(`SELECT COUNT(*) AS count FROM ${CLEANUP_RECEIPT_TABLE}`)
    .first<{ count: number }>();
  return Number(row?.count);
}

async function cleanupTerminalReceipt(db: D1Database): Promise<unknown> {
  const store = await readyStore(db);
  await db.prepare(`DELETE FROM ${CLEANUP_RECEIPT_TABLE}`).run();
  const active = cleanupAdvancingFixture('cleanupterm');
  const receipt = cleanupReceiptFor(active);
  return store.withDeploymentLease(
    'cleanupterm',
    'production',
    async (lease) => {
      if (!lease.completeCleanup) {
        throw new Error('completeCleanup capability missing');
      }
      await lease.put(active);
      const claimsBefore = await cleanupClaimCount(db, 'cleanupterm');
      const stale = await lease
        .completeCleanup({ receipt, expectedRevision: 4 })
        .then(
          () => undefined,
          (error: unknown) => errorShape(error),
        );
      const rowAfterStale = await store.get('cleanupterm', 'production');
      const receiptsAfterStale = await cleanupReceiptCount(db);
      const persisted = await lease.completeCleanup({
        receipt,
        expectedRevision: 0,
      });
      const claimsAfter = await cleanupClaimCount(db, 'cleanupterm');
      const rowAfter = await store.get('cleanupterm', 'production');
      const replay = await lease.completeCleanup({
        receipt,
        expectedRevision: 0,
      });
      const reordered: CleanupTerminalReceipt = {
        ...receipt,
        evidence: {
          scan: {
            verify: { evidenceCount: 2, evidenceSha256: 'b'.repeat(64) },
            discover: { evidenceCount: 2, evidenceSha256: 'b'.repeat(64) },
          },
          databaseAbsentReadback: true,
          applicationR2Settled: true,
          platformResourcesAbsent: true,
          workerAbsent: true,
          ingressRemoved: true,
          eligibility: 'carrier-null',
        },
      };
      const keyOrderReplay = await lease.completeCleanup({
        receipt: reordered,
        expectedRevision: 0,
      });
      const conflict = await lease
        .completeCleanup({
          receipt: { ...receipt, disposition: 'reservation-cleared' },
          expectedRevision: 0,
        })
        .then(
          () => undefined,
          (error: unknown) => errorShape(error),
        );
      const foreign = await lease
        .completeCleanup({
          receipt: { ...receipt, tenantTag: 'other' },
          expectedRevision: 0,
        })
        .then(
          () => undefined,
          (error: unknown) => errorShape(error),
        );
      await lease.put(record('cleanupterm', 'production'));
      const reprovisioned = await store.get('cleanupterm', 'production');
      const survivingReceipt = await store.readCleanupReceipt?.(
        receipt.operationId,
      );
      return {
        stale,
        rowPhaseAfterStale: rowAfterStale?.phase ?? null,
        receiptsAfterStale,
        claimsBefore,
        claimsAfter,
        rowAfterTerminal: rowAfter?.phase ?? null,
        persistedHasCompletedAt: typeof persisted.completedAtMs === 'number',
        replayEqual: JSON.stringify(replay) === JSON.stringify(persisted),
        keyOrderReplayEqual:
          JSON.stringify(keyOrderReplay) === JSON.stringify(persisted),
        conflict,
        foreign,
        reprovisionPhase: reprovisioned?.phase ?? null,
        survivingOperationId: survivingReceipt?.operationId ?? null,
      };
    },
  );
}

async function cleanupReceiptPrune(db: D1Database): Promise<unknown> {
  const store = await readyStore(db);
  await db.prepare(`DELETE FROM ${CLEANUP_RECEIPT_TABLE}`).run();
  const operations = [
    '00000000-0000-4000-8000-0000000000a1',
    '00000000-0000-4000-8000-0000000000a2',
    '00000000-0000-4000-8000-0000000000a3',
  ];
  for (const [index, operationId] of operations.entries()) {
    const tenantTag = `prune${index}`;
    const active = cleanupAdvancingFixture(tenantTag, operationId);
    const receipt = cleanupReceiptFor(active);
    await store.withDeploymentLease(tenantTag, 'production', async (lease) => {
      if (!lease.completeCleanup) {
        throw new Error('completeCleanup capability missing');
      }
      await lease.put(active);
      await lease.completeCleanup({ receipt, expectedRevision: 0 });
    });
  }
  if (!store.pruneCleanupReceipts) {
    throw new Error('pruneCleanupReceipts capability missing');
  }
  const cutoff = Date.now() + 3_600_000;
  const invalid: unknown[] = [];
  for (const limit of [0, 1001, 1.5]) {
    invalid.push(
      await store
        .pruneCleanupReceipts({ completedBeforeMs: cutoff, limit })
        .then(
          () => undefined,
          (error: unknown) => errorShape(error),
        ),
    );
  }
  const untouched = await cleanupReceiptCount(db);
  const nothing = await store.pruneCleanupReceipts({
    completedBeforeMs: 0,
    limit: 1000,
  });
  const firstTwo = await store.pruneCleanupReceipts({
    completedBeforeMs: cutoff,
    limit: 2,
  });
  const remaining = await db
    .prepare(
      `SELECT operation_id FROM ${CLEANUP_RECEIPT_TABLE} ORDER BY operation_id`,
    )
    .all<{ operation_id: string }>();
  const lowerBound = await store.pruneCleanupReceipts({
    completedBeforeMs: cutoff,
    limit: 1,
  });
  const rest = await store.pruneCleanupReceipts({
    completedBeforeMs: cutoff,
    limit: 1000,
  });
  return {
    invalid,
    untouched,
    nothing,
    firstTwo,
    remainingAfterFirstTwo: remaining.results.map((row) => row.operation_id),
    lowerBound,
    rest,
    finalCount: await cleanupReceiptCount(db),
  };
}

async function cleanupClaimsRelease(db: D1Database): Promise<unknown> {
  const store = await readyStore(db);
  const active = record('claimsrel', 'production');
  return store.withDeploymentLease('claimsrel', 'production', async (lease) => {
    await lease.put(active);
    const claims = await db
      .prepare(
        `SELECT resource_set_key, platform_plane_identity
           FROM ${PLATFORM_CLAIM_TABLE}`,
      )
      .all<{ resource_set_key: string; platform_plane_identity: string }>();
    if (!lease.deleteReleasingClaims) {
      throw new Error('deleteReleasingClaims capability missing');
    }
    await lease.deleteReleasingClaims();
    return {
      identities: claims.results,
      claimsAfter: await cleanupClaimCount(db, 'claimsrel'),
      rowAfter: (await store.get('claimsrel', 'production'))?.phase ?? null,
    };
  });
}

async function coldConcurrentSchemaInitialization(
  db: D1Database,
): Promise<unknown> {
  const stores = Array.from(
    { length: 16 },
    () =>
      new D1FleetStateStore(new D1FleetStateDatabase(db), {
        accountId: 'account-primary',
      }),
  );
  const written = await Promise.all(
    stores.map((store, index) => {
      const tenantTag = `cold${index}`;
      return store.withDeploymentLease(
        tenantTag,
        'production',
        async (lease) => {
          await lease.put(record(tenantTag, 'production'));
          return tenantTag;
        },
      );
    }),
  );
  const columnRows = await db
    .prepare(`PRAGMA table_info(${STATE_TABLE})`)
    .all<{ name: string }>();
  const tableRows = await db
    .prepare(
      `SELECT name FROM sqlite_master
       WHERE type = 'table' AND name IN (?, ?, ?, ?)
       ORDER BY name`,
    )
    .bind(STATE_TABLE, LEASE_TABLE, PLATFORM_CLAIM_TABLE, PLATFORM_LEASE_TABLE)
    .all<{ name: string }>();
  const row = await db
    .prepare(`SELECT COUNT(*) AS count FROM ${STATE_TABLE}`)
    .first<{ count: number }>();
  return {
    written: [...written].sort(),
    columns: ADDED_NULLABLE_TEXT_COLUMNS.filter((name) =>
      columnRows.results.some((column) => column.name === name),
    ),
    rows: Number(row?.count),
    tables: tableRows.results.map(({ name }) => name),
  };
}

const INVENTORY_TABLES = [
  'anchorage_fleet_inventory_deployment_facts',
  'anchorage_fleet_inventory_heads',
  'anchorage_fleet_inventory_leases',
  'anchorage_fleet_inventory_pins',
  'anchorage_fleet_inventory_rows',
  'anchorage_fleet_inventory_runs',
];
const INVENTORY_LEASE_TABLE = 'anchorage_fleet_inventory_leases';
const INVENTORY_ACCOUNT = 'account-inventory';
const INVENTORY_OPTIONS = canonicalFleetInventoryRunOptions({
  hostRoutingKvId: 'kv-host-routing',
  databaseNamePrefix: 'anchorage-db',
  scriptNamePrefix: 'anchorage',
});
const INVENTORY_DIGEST = fleetInventoryOptionsDigest(INVENTORY_OPTIONS);

function inventoryOperationId(index: number): string {
  return `123e4567-e89b-42d3-a456-42661417${String(4200 + index)}`;
}

function inventoryStore(
  database: FleetStateDatabase,
): D1FleetInventoryRunStore {
  return new D1FleetInventoryRunStore(database, {
    accountId: INVENTORY_ACCOUNT,
  });
}

async function readyInventoryStore(
  db: D1Database,
): Promise<D1FleetInventoryRunStore> {
  const store = inventoryStore(new D1FleetStateDatabase(db));
  await store.latestFinalizedGeneration();
  for (const table of INVENTORY_TABLES) {
    await db.prepare(`DELETE FROM ${table}`).run();
  }
  return store;
}

/** Drops the next batch's result rows, reproducing a lost D1 response. */
function inventoryLostResponse(
  delegate: FleetStateDatabase,
): FleetStateDatabase & Readonly<{ loseNextBatch(): void }> {
  let lose = false;
  return {
    query: (sql, bindings) => delegate.query(sql, bindings),
    execute: (sql, bindings) => delegate.execute(sql, bindings),
    async batch(statements) {
      const results = await delegate.batch(statements);
      if (!lose) return results;
      lose = false;
      return results.map(() => []);
    },
    loseNextBatch() {
      lose = true;
    },
  };
}

function inventoryRows(label: string): readonly FleetInventoryStagedRow[] {
  return [
    { kind: 'registration', ordinal: 0, payload: { scriptName: label } },
    { kind: 'deployment', ordinal: 0, payload: { scriptName: label } },
    {
      kind: 'finding',
      ordinal: 0,
      payload: { detail: `stale route ${label}` },
    },
  ];
}

function inventoryFacts(): readonly FleetInventoryStagedFact[] {
  return [
    {
      deploymentOrdinal: 0,
      factKind: 'secret-name',
      factOrdinal: 0,
      payload: { name: 'ANCHORAGE_NAME_0' },
    },
  ];
}

function inventoryCounts(
  rows: readonly FleetInventoryStagedRow[],
): Record<FleetInventoryRowKind, number> {
  const counts = emptyFleetInventoryRowCounts() as Record<
    FleetInventoryRowKind,
    number
  >;
  for (const row of rows) counts[row.kind] += 1;
  return counts;
}

function inventoryCommitted(
  record: FleetInventoryRunRecord,
  rows: readonly FleetInventoryStagedRow[],
  facts: readonly FleetInventoryStagedFact[],
): FleetInventoryRunRecord {
  return {
    ...record,
    progress: {
      ...record.progress,
      stage: { step: 'finalize' },
      revision: record.progress.revision + 1,
      stagedCounts: inventoryCounts(rows),
      factCount: facts.length,
      providerRequests: record.progress.providerRequests + 1,
    },
    updatedAt: '2026-08-29T00:00:00.000Z',
  };
}

async function seedInventoryGeneration(
  store: D1FleetInventoryRunStore,
  index: number,
  rows: readonly FleetInventoryStagedRow[] = inventoryRows(`gen-${index}`),
  facts: readonly FleetInventoryStagedFact[] = inventoryFacts(),
): Promise<number> {
  const operationId = inventoryOperationId(index);
  return store.withAccountInventoryLease(async (lease) => {
    const started = await lease.startRun({
      operationId,
      options: INVENTORY_OPTIONS,
      optionsDigest: INVENTORY_DIGEST,
    });
    const record = await lease.commitChunk({
      operationId,
      expectedRevision: started.progress.revision,
      runRecord: inventoryCommitted(started, rows, facts),
      rows,
      facts,
    });
    const ref = await lease.finalizeRun({
      operationId,
      expectedRevision: record.progress.revision,
      manifest: record.progress.stagedCounts,
      factCount: record.progress.factCount,
    });
    return ref.generation;
  });
}

async function inventoryStartAtomicity(db: D1Database): Promise<unknown> {
  await readyInventoryStore(db);
  const operationId = inventoryOperationId(0);
  const stores = Array.from({ length: 16 }, () =>
    inventoryStore(new D1FleetStateDatabase(db)),
  );
  const attempts = await Promise.allSettled(
    stores.map((store) =>
      store.withAccountInventoryLease((lease) =>
        lease.startRun({
          operationId,
          options: INVENTORY_OPTIONS,
          optionsDigest: INVENTORY_DIGEST,
        }),
      ),
    ),
  );
  const head = await db
    .prepare(
      `SELECT active_operation_id, latest_finalized_generation, next_generation
         FROM anchorage_fleet_inventory_heads WHERE account_id = ?`,
    )
    .bind(INVENTORY_ACCOUNT)
    .first<{
      active_operation_id: string | null;
      latest_finalized_generation: number | null;
      next_generation: number;
    }>();
  const run = await db
    .prepare(
      `SELECT operation_id, generation, options_digest, finalized_at_ms
         FROM anchorage_fleet_inventory_runs WHERE account_id = ?`,
    )
    .bind(INVENTORY_ACCOUNT)
    .all<{
      operation_id: string;
      generation: number;
      options_digest: string;
      finalized_at_ms: number | null;
    }>();
  return {
    started: attempts.filter((attempt) => attempt.status === 'fulfilled')
      .length,
    rejected: attempts.filter((attempt) => attempt.status === 'rejected')
      .length,
    head: {
      activeOperationId: head?.active_operation_id ?? null,
      latestFinalizedGeneration: head?.latest_finalized_generation ?? null,
      nextGeneration: Number(head?.next_generation),
    },
    runs: run.results.map((row) => ({
      operationId: row.operation_id,
      generation: Number(row.generation),
      digestMatches: row.options_digest === INVENTORY_DIGEST,
      finalized: row.finalized_at_ms !== null,
    })),
    generation:
      run.results.length === 1 ? Number(run.results[0]?.generation) : 0,
  };
}

async function inventoryCommitConcurrency(db: D1Database): Promise<unknown> {
  const store = await readyInventoryStore(db);
  const operationId = inventoryOperationId(1);
  // The account lease serializes lease HOLDERS, so the guarded commit batch can
  // only be raced by concurrent calls under one lease. Concurrency is expressed
  // exactly like coldConcurrentSchemaInitialization: Promise.allSettled over N
  // writers inside the one request.
  return store.withAccountInventoryLease(async (lease) => {
    const started = await lease.startRun({
      operationId,
      options: INVENTORY_OPTIONS,
      optionsDigest: INVENTORY_DIGEST,
    });
    const shared = inventoryRows('race');
    const writers = Array.from({ length: 16 }, (_unused, index) => {
      const rows = [
        ...shared,
        { kind: 'meta' as const, ordinal: index, payload: { writer: index } },
      ];
      const base = inventoryCommitted(started, rows, []);
      return {
        operationId,
        expectedRevision: started.progress.revision,
        // Each writer's intended record differs, so the winner is the writer
        // whose own record the guarded UPDATE persisted.
        runRecord: {
          ...base,
          progress: { ...base.progress, providerRequests: index + 1 },
        },
        rows,
        facts: [] as readonly FleetInventoryStagedFact[],
      };
    });
    const settled = await Promise.allSettled(
      writers.map((input) => lease.commitChunk(input)),
    );
    const outcomes = settled.map((result, index) => {
      if (result.status === 'rejected') {
        return {
          index,
          outcome: (result.reason as Error).message.includes('diverge')
            ? 'corrupt'
            : 'conflict',
        };
      }
      const intended = JSON.stringify(writers[index]?.runRecord);
      return {
        index,
        outcome:
          JSON.stringify(result.value) === intended ? 'committed' : 'converged',
      };
    });
    const winner = outcomes.find((entry) => entry.outcome === 'committed');
    const winning = writers[winner?.index ?? -1];
    if (!winning) throw new Error('inventory commit race had no winner');
    // The winner's own replay is the lost-response case: its rows are already
    // present byte-identically at the intended revision, so it converges on the
    // persisted record without advancing the revision a second time.
    const beforeReplay = (await store.readRunByOperation(operationId))?.progress
      .revision;
    const replayResult = await lease.commitChunk(winning).then(
      (record) => JSON.stringify(record) === JSON.stringify(winning.runRecord),
      () => false,
    );
    const afterReplay = (await store.readRunByOperation(operationId))?.progress
      .revision;
    const lostResponseReplay =
      replayResult && beforeReplay === afterReplay ? 'converged' : 'conflict';
    const persisted = await store.readRunByOperation(operationId);
    if (!persisted) throw new Error('inventory commit race lost its run');
    const trailing = {
      kind: 'meta' as const,
      ordinal: 100,
      payload: { writer: 'trailing' },
    };
    const advanced = await lease.commitChunk({
      operationId,
      expectedRevision: persisted.progress.revision,
      runRecord: inventoryCommitted(persisted, [...shared, trailing], []),
      rows: [trailing],
      facts: [],
    });
    const replayed = writers[0];
    if (!replayed) throw new Error('inventory commit race had no writer');
    const staleReplay = await lease.commitChunk(replayed).then(
      () => 'committed',
      (error: unknown) =>
        (error as Error).message.includes('no longer at the expected revision')
          ? 'conflict'
          : 'other',
    );
    const counts = await db
      .prepare(
        `SELECT kind, COUNT(*) AS count FROM anchorage_fleet_inventory_rows
          WHERE account_id = ? AND generation = ? GROUP BY kind ORDER BY kind`,
      )
      .bind(INVENTORY_ACCOUNT, started.progress.generation)
      .all<{ kind: string; count: number }>();
    return {
      committed: outcomes.filter((entry) => entry.outcome === 'committed')
        .length,
      converged: outcomes.filter((entry) => entry.outcome === 'converged')
        .length,
      conflicts: outcomes.filter((entry) => entry.outcome === 'conflict')
        .length,
      corrupt: outcomes.filter((entry) => entry.outcome === 'corrupt').length,
      winnerIsWriter: typeof winner?.index === 'number',
      lostResponseReplay,
      revision: advanced.progress.revision,
      staleReplay,
      rowCounts: counts.results.map((row) => ({
        kind: row.kind,
        count: Number(row.count),
      })),
    };
  });
}

async function inventoryFinalizeConvergence(db: D1Database): Promise<unknown> {
  await readyInventoryStore(db);
  const database = inventoryLostResponse(new D1FleetStateDatabase(db));
  const store = inventoryStore(database);
  const operationId = inventoryOperationId(2);
  const rows = inventoryRows('finalize');
  const facts = inventoryFacts();
  const refs = await store.withAccountInventoryLease(async (lease) => {
    const started = await lease.startRun({
      operationId,
      options: INVENTORY_OPTIONS,
      optionsDigest: INVENTORY_DIGEST,
    });
    const record = await lease.commitChunk({
      operationId,
      expectedRevision: started.progress.revision,
      runRecord: inventoryCommitted(started, rows, facts),
      rows,
      facts,
    });
    const input = {
      operationId,
      expectedRevision: record.progress.revision,
      manifest: record.progress.stagedCounts,
      factCount: record.progress.factCount,
    };
    database.loseNextBatch();
    const first = await lease.finalizeRun(input);
    const replayed = await lease.finalizeRun(input);
    return { first, replayed };
  });
  const head = await db
    .prepare(
      `SELECT active_operation_id, latest_finalized_generation
         FROM anchorage_fleet_inventory_heads WHERE account_id = ?`,
    )
    .bind(INVENTORY_ACCOUNT)
    .first<{
      active_operation_id: string | null;
      latest_finalized_generation: number | null;
    }>();
  return {
    first: refs.first,
    replayed: refs.replayed,
    identical: JSON.stringify(refs.first) === JSON.stringify(refs.replayed),
    head: {
      activeOperationId: head?.active_operation_id ?? null,
      latestFinalizedGeneration: head?.latest_finalized_generation ?? null,
    },
  };
}

async function inventoryGenerationReadback(db: D1Database): Promise<unknown> {
  const store = await readyInventoryStore(db);
  const generation = await seedInventoryGeneration(store, 3);
  const read = await store.readFinalizedGeneration(generation);
  const latest = await store.latestFinalizedGeneration();
  return {
    ref: read.ref,
    latestMatches: JSON.stringify(latest) === JSON.stringify(read.ref),
    rowOrdinals: read.rows.map((row) => `${row.kind}:${row.ordinal}`),
    factOrdinals: read.facts.map(
      (fact) =>
        `${fact.deploymentOrdinal}:${fact.factKind}:${fact.factOrdinal}`,
    ),
  };
}

async function inventoryCorruptUnreadable(db: D1Database): Promise<unknown> {
  const store = await readyInventoryStore(db);
  const generation = await seedInventoryGeneration(store, 4);
  await db
    .prepare(
      `DELETE FROM anchorage_fleet_inventory_rows
        WHERE account_id = ? AND generation = ? AND kind = 'finding'`,
    )
    .bind(INVENTORY_ACCOUNT, generation)
    .run();
  const readError = await store.readFinalizedGeneration(generation).then(
    () => null,
    (error: unknown) => errorShape(error),
  );
  const operationId = inventoryOperationId(5);
  const rows = inventoryRows('mismatch');
  const facts = inventoryFacts();
  const finalizeError = await store
    .withAccountInventoryLease(async (lease) => {
      const started = await lease.startRun({
        operationId,
        options: INVENTORY_OPTIONS,
        optionsDigest: INVENTORY_DIGEST,
      });
      // The persisted record claims more findings than the rows it staged, so
      // finalize's in-SQL count guard is what refuses.
      const overstated = inventoryCommitted(started, rows, facts);
      const record = await lease.commitChunk({
        operationId,
        expectedRevision: started.progress.revision,
        runRecord: {
          ...overstated,
          progress: {
            ...overstated.progress,
            stagedCounts: { ...overstated.progress.stagedCounts, finding: 9 },
          },
        },
        rows,
        facts,
      });
      return lease.finalizeRun({
        operationId,
        expectedRevision: record.progress.revision,
        manifest: record.progress.stagedCounts,
        factCount: record.progress.factCount,
      });
    })
    .then(
      () => null,
      (error: unknown) => errorShape(error),
    );
  const run = await store.readRunByOperation(operationId);
  return {
    readError,
    finalizeError,
    stateAfterFinalize: run?.state ?? null,
    latestGeneration:
      (await store.latestFinalizedGeneration())?.generation ?? null,
  };
}

async function inventoryPruneOrder(db: D1Database): Promise<unknown> {
  const store = await readyInventoryStore(db);
  for (const index of [10, 11, 12, 13]) {
    await seedInventoryGeneration(store, index);
  }
  await store.pinGeneration({ generation: 2, pinnedBy: 'audit' });
  const deleted = [
    await store.pruneInventoryGenerations({ limit: 1 }),
    await store.pruneInventoryGenerations({ limit: 1 }),
    await store.pruneInventoryGenerations({ limit: 10 }),
  ];
  const pinnedSurvives = (await store.readFinalizedGeneration(2)).ref
    .generation;
  await store.releasePin({ generation: 2, pinnedBy: 'audit' });
  deleted.push(await store.pruneInventoryGenerations({ limit: 10 }));
  const surviving = await db
    .prepare(
      `SELECT generation FROM anchorage_fleet_inventory_runs
        WHERE account_id = ? ORDER BY generation`,
    )
    .bind(INVENTORY_ACCOUNT)
    .all<{ generation: number }>();
  const rows = await db
    .prepare(
      `SELECT DISTINCT generation FROM anchorage_fleet_inventory_rows
        WHERE account_id = ? ORDER BY generation`,
    )
    .bind(INVENTORY_ACCOUNT)
    .all<{ generation: number }>();
  return {
    deleted: deleted.map((entry) => entry.deleted),
    pinnedSurvives,
    surviving: surviving.results.map((row) => Number(row.generation)),
    survivingRowGenerations: rows.results.map((row) => Number(row.generation)),
  };
}

async function inventoryLeaseLifecycle(db: D1Database): Promise<unknown> {
  await readyInventoryStore(db);
  await db
    .prepare(
      `INSERT INTO ${INVENTORY_LEASE_TABLE} (account_id, owner_token, expires_at)
       VALUES (?, 'abandoned-token', 1)`,
    )
    .bind(INVENTORY_ACCOUNT)
    .run();
  const takeover = await inventoryStore(new D1FleetStateDatabase(db))
    .withAccountInventoryLease(async (lease) => {
      await lease.assertOwned();
      return 'acquired';
    })
    .catch((error: unknown) => errorShape(error));
  const clock = controlledLeaseClock(db, INVENTORY_LEASE_TABLE);
  const store = new D1FleetInventoryRunStore(clock.database, {
    accountId: INVENTORY_ACCOUNT,
    leaseTtlMs: 2_500,
    leaseRenewalIntervalMs: 1,
  });
  let contenderRejected = false;
  await store.withAccountInventoryLease(async () => {
    const original = await clock.database.query(
      `SELECT expires_at FROM ${INVENTORY_LEASE_TABLE} WHERE account_id = ?`,
      [INVENTORY_ACCOUNT],
    );
    const originalExpiresAt = Number(original[0]?.expires_at);
    if (!Number.isFinite(originalExpiresAt)) {
      throw new Error('inventory lease did not expose its original expiry');
    }
    clock.advance(2_000);
    clock.allowHeartbeat();
    await clock.heartbeat;
    clock.advance(600);
    try {
      await new D1FleetInventoryRunStore(clock.database, {
        accountId: INVENTORY_ACCOUNT,
        leaseTtlMs: 2_500,
        leaseRenewalIntervalMs: 1,
      }).withAccountInventoryLease(async () => {});
    } catch {
      contenderRejected = true;
    }
    if (clock.now() <= originalExpiresAt) {
      throw new Error('controlled D1 time did not pass the original expiry');
    }
  });
  const remaining = await db
    .prepare(`SELECT COUNT(*) AS count FROM ${INVENTORY_LEASE_TABLE}`)
    .first<{ count: number }>();
  return {
    takeover,
    heartbeatObserved: true,
    contenderRejected,
    leasesAfterRelease: Number(remaining?.count),
  };
}

async function inventoryColdConcurrentSchema(db: D1Database): Promise<unknown> {
  const stores = Array.from({ length: 16 }, () =>
    inventoryStore(new D1FleetStateDatabase(db)),
  );
  const latest = await Promise.all(
    stores.map((store) => store.latestFinalizedGeneration()),
  );
  const columns: Record<string, string[]> = {};
  for (const table of INVENTORY_TABLES) {
    const rows = await db
      .prepare(`PRAGMA table_info(${table})`)
      .all<{ name: string; type: string }>();
    columns[table] = rows.results.map((row) => `${row.name}:${row.type}`);
  }
  const tables = await db
    .prepare(
      `SELECT name FROM sqlite_master
        WHERE type = 'table' AND name LIKE 'anchorage_fleet_inventory_%'
        ORDER BY name`,
    )
    .all<{ name: string }>();
  const started = await stores[0]?.withAccountInventoryLease((lease) =>
    lease.startRun({
      operationId: inventoryOperationId(20),
      options: INVENTORY_OPTIONS,
      optionsDigest: INVENTORY_DIGEST,
    }),
  );
  return {
    latest: latest.filter((entry) => entry === undefined).length,
    columns,
    tables: tables.results.map((row) => row.name),
    generation: started?.progress.generation ?? null,
  };
}

async function cloudflareRateCoordination(db: D1Database): Promise<unknown> {
  await db
    .prepare('DROP TABLE IF EXISTS anchorage_cloudflare_api_rate_reservations')
    .run();
  const quotaScope = 'real-d1-shared-provider-principal';
  const first = new D1CloudflareApiRateCoordinator(db, { quotaScope });
  const second = new D1CloudflareApiRateCoordinator(db, { quotaScope });
  await first.acquire();
  await db
    .prepare(
      'DELETE FROM anchorage_cloudflare_api_rate_reservations WHERE quota_scope = ?',
    )
    .bind(quotaScope)
    .run();
  await db
    .prepare(`WITH RECURSIVE sequence(value) AS (
      SELECT 1 UNION ALL SELECT value + 1 FROM sequence WHERE value < 1099
    )
    INSERT INTO anchorage_cloudflare_api_rate_reservations (
      quota_scope, reservation_id, reserved_at
    )
    SELECT ?, 'seed-' || value, CAST(unixepoch('subsec') * 1000 AS INTEGER)
    FROM sequence`)
    .bind(quotaScope)
    .run();
  await first.acquire();
  const controller = new AbortController();
  const blocked = second.acquire(controller.signal).then(
    () => false,
    (error) => error instanceof Error && error.name === 'AbortError',
  );
  await new Promise((resolve) => setTimeout(resolve, 0));
  controller.abort();
  const row = await db
    .prepare(`SELECT COUNT(*) AS count
      FROM anchorage_cloudflare_api_rate_reservations
      WHERE quota_scope = ?`)
    .bind(quotaScope)
    .first<{ count: number }>();
  return { blocked: await blocked, count: Number(row?.count) };
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (request.method !== 'POST' || url.pathname !== '/fleet-state') {
      return new Response('not found', { status: 404 });
    }
    const body = (await request.json()) as {
      action?: unknown;
      input?: unknown;
    };
    try {
      switch (body.action) {
        case 'concurrent-acquisition':
          return Response.json(await concurrentAcquisition(env.DB));
        case 'renewal':
          return Response.json(await renewal(env.DB));
        case 'takeover-and-fence':
          return Response.json(await takeoverAndFence(env.DB));
        case 'uniqueness':
          return Response.json(await uniqueness(env.DB));
        case 'combined-errors':
          return Response.json(await combinedErrors(env.DB));
        case 'platform-claims-and-concurrency':
          return Response.json(await platformClaimsAndConcurrency(env.DB));
        case 'platform-takeover-and-fence':
          return Response.json(await platformTakeoverAndFence(env.DB));
        case 'platform-renewal':
          return Response.json(await platformRenewal(env.DB));
        case 'cross-plane-claim-exclusion':
          return Response.json(await crossPlaneClaimExclusion(env.DB));
        case 'atomic-claim-batch':
          return Response.json(await atomicClaimBatch(env.DB));
        case 'final-lease-assertion-rollback':
          return Response.json(await finalLeaseAssertionRollback(env.DB));
        case 'backend-switch-column-upgrade':
          return Response.json(await backendSwitchColumnUpgrade(env.DB));
        case 'bounded-backend-switch-write-step':
          return Response.json(
            await backendSwitchLostWriteStep(
              env.DB,
              body.input as {
                stage: BackendSwitchLostWriteStage;
                loseWrite?: boolean;
              },
            ),
          );
        case 'decommission-intent-column-upgrade':
          return Response.json(await decommissionIntentColumnUpgrade(env.DB));
        case 'decommission-intent-lost-response':
          return Response.json(await decommissionIntentLostResponse(env.DB));
        case 'bounded-decommission-step':
          return Response.json(
            await boundedDecommissionStep(
              env.DB,
              body.input as BoundedDecommissionStepInput,
            ),
          );
        case 'bounded-decommission-reset':
          return Response.json(
            await resetBoundedDecommission(
              env.DB,
              (body.input as { tenantTag: 'advance' | 'advancelost' })
                .tenantTag,
            ),
          );
        case 'lifecycle-errors':
          return Response.json(await lifecycleErrors(env.DB));
        case 'cloudflare-rate-coordination':
          return Response.json(await cloudflareRateCoordination(env.DB));
        case 'cleanup-terminal-receipt':
          return Response.json(await cleanupTerminalReceipt(env.DB));
        case 'cleanup-receipt-prune':
          return Response.json(await cleanupReceiptPrune(env.DB));
        case 'cleanup-claims-release':
          return Response.json(await cleanupClaimsRelease(env.DB));
        case 'cold-concurrent-schema-initialization':
          return Response.json(
            await coldConcurrentSchemaInitialization(env.DB),
          );
        case 'inventory-start-atomicity':
          return Response.json(await inventoryStartAtomicity(env.DB));
        case 'inventory-commit-concurrency':
          return Response.json(await inventoryCommitConcurrency(env.DB));
        case 'inventory-finalize-convergence':
          return Response.json(await inventoryFinalizeConvergence(env.DB));
        case 'inventory-generation-readback':
          return Response.json(await inventoryGenerationReadback(env.DB));
        case 'inventory-corrupt-unreadable':
          return Response.json(await inventoryCorruptUnreadable(env.DB));
        case 'inventory-prune-order':
          return Response.json(await inventoryPruneOrder(env.DB));
        case 'inventory-lease-lifecycle':
          return Response.json(await inventoryLeaseLifecycle(env.DB));
        case 'inventory-cold-concurrent-schema':
          return Response.json(await inventoryColdConcurrentSchema(env.DB));
        default:
          return Response.json({ error: 'unknown action' }, { status: 400 });
      }
    } catch (error) {
      return Response.json({ error: errorShape(error) }, { status: 500 });
    }
  },
} satisfies ExportedHandler<Env>;
