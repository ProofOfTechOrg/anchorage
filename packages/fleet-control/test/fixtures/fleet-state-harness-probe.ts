// SPDX-License-Identifier: Apache-2.0
/// <reference types="@cloudflare/workers-types" />

import {
  applicationBindingTopology,
  reserveApplicationR2Resources,
} from '../../src/application-bindings.js';
import { D1CloudflareApiRateCoordinator } from '../../src/cloudflare-rate-coordinator.js';
import { D1FleetStateDatabase } from '../../src/d1-fleet-state-database.js';
import { advanceDecommissionDeployment } from '../../src/decommission-advance.js';
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
  DatabaseExport,
  DatabaseExportReceiptIdentity,
  DatabaseReference,
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
import { decommissionAdvancingRecordFixture } from './decommission-intent-fixture.js';

interface Env {
  DB: D1Database;
}

const STATE_TABLE = 'anchorage_fleet_deployments';
const LEASE_TABLE = 'anchorage_fleet_leases';
const PLATFORM_CLAIM_TABLE = 'anchorage_platform_plane_claims';
const PLATFORM_LEASE_TABLE = 'anchorage_platform_plane_leases';
const BOUNDED_PROVIDER_TABLE = 'anchorage_test_bounded_decommission_provider';
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
        case 'cold-concurrent-schema-initialization':
          return Response.json(
            await coldConcurrentSchemaInitialization(env.DB),
          );
        default:
          return Response.json({ error: 'unknown action' }, { status: 400 });
      }
    } catch (error) {
      return Response.json({ error: errorShape(error) }, { status: 500 });
    }
  },
} satisfies ExportedHandler<Env>;
