// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  canonicalDeploymentEgressPolicy,
  durableObjectMigrationHistoryDigest,
  externalEgressProxyScriptName,
  externalStateScriptName,
} from '../src/platform-resources.js';
import {
  D1FleetStateStore,
  type FleetStateDatabase,
} from '../src/state-store.js';
import type { FleetRecord, PlatformPlaneResourceSet } from '../src/types.js';

const MAINTENANCE_PUBLIC_KEY =
  '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}';

/**
 * The columns the store adds to a table that already shipped. A fake that
 * models only some of them reports a half-upgraded schema, which is the one
 * shape the store is entitled to refuse.
 */
const UPGRADED_COLUMNS = [
  'backend_switch_intent',
  'settled_settlement_key',
] as const;

function nullableTextColumn(name: string): Readonly<Record<string, unknown>> {
  return { name, type: 'TEXT', notnull: 0, pk: 0 };
}

function addedColumnName(sql: string): string | undefined {
  return /ADD COLUMN (?<name>[a-z_]+) TEXT/u.exec(sql)?.groups?.name;
}

class MemoryD1 implements FleetStateDatabase {
  row: Readonly<Record<string, unknown>> | undefined;
  readonly claims = new Map<string, Readonly<Record<string, unknown>>>();

  async query(
    sql: string,
    bindings: readonly unknown[] = [],
  ): Promise<readonly Readonly<Record<string, unknown>>[]> {
    if (sql.startsWith('PRAGMA table_info(anchorage_fleet_deployments)')) {
      return UPGRADED_COLUMNS.map((name) => nullableTextColumn(name));
    }
    if (sql.startsWith('SELECT *')) return this.row ? [this.row] : [];
    if (sql.startsWith('INSERT INTO anchorage_fleet_leases')) {
      return [{ owner_token: bindings[2], expires_at: 1 }];
    }
    if (sql.startsWith('UPDATE anchorage_fleet_leases')) {
      return [{ owner_token: bindings[3], expires_at: 1 }];
    }
    if (sql.startsWith('DELETE FROM anchorage_fleet_leases')) {
      return [{ owner_token: bindings[2] }];
    }
    if (sql.startsWith('SELECT owner_token FROM anchorage_fleet_leases')) {
      return [{ owner_token: bindings[2] }];
    }
    if (sql.startsWith('INSERT INTO anchorage_platform_plane_leases')) {
      return [{ owner_token: bindings[1], expires_at: 1 }];
    }
    if (sql.startsWith('UPDATE anchorage_platform_plane_leases')) {
      return [{ owner_token: bindings[2], expires_at: 1 }];
    }
    if (sql.startsWith('DELETE FROM anchorage_platform_plane_leases')) {
      return [{ owner_token: bindings[1] }];
    }
    if (
      sql.startsWith('INSERT INTO anchorage_platform_plane_claims') ||
      sql.startsWith('WITH owned_lease')
    ) {
      const claimBindings = sql.startsWith('WITH owned_lease')
        ? bindings.slice(3)
        : bindings;
      const pending: Array<{
        key: string;
        claim: Readonly<Record<string, unknown>>;
      }> = [];
      for (let index = 0; index < claimBindings.length; index += 6) {
        const [
          accountId,
          resourceType,
          resourceName,
          resourceRole,
          setKey,
          identity,
        ] = claimBindings.slice(index, index + 6) as string[];
        const key = `${accountId}:${resourceType}:${resourceName}`;
        if (this.claims.has(key)) throw new Error('claim conflict');
        const claim = {
          account_id: accountId,
          resource_type: resourceType,
          resource_name: resourceName,
          resource_role: resourceRole,
          resource_set_key: setKey,
          platform_plane_identity: identity,
        };
        pending.push({ key, claim });
      }
      for (const { key, claim } of pending) this.claims.set(key, claim);
      return pending.map(({ claim }) => claim);
    }
    if (sql.startsWith('SELECT resource_type, resource_name, resource_role')) {
      const accountId = String(bindings[0]);
      if (sql.includes('resource_set_key = ?')) {
        const setKey = String(bindings[1]);
        return [...this.claims.values()].filter(
          (claim) =>
            claim.account_id === accountId && claim.resource_set_key === setKey,
        );
      }
      const requested = new Set(bindings.slice(1).map(String));
      return [...this.claims.values()].filter(
        (claim) =>
          claim.account_id === accountId &&
          requested.has(String(claim.resource_name)),
      );
    }
    if (sql.startsWith('WITH desired AS') && sql.includes('SELECT claims.')) {
      const desired = JSON.parse(String(bindings[0])) as Array<{
        resourceType: string;
        resourceName: string;
      }>;
      const names = new Set(
        desired.map(
          ({ resourceType, resourceName }) => `${resourceType}:${resourceName}`,
        ),
      );
      return [...this.claims.values()].filter(
        (claim) =>
          claim.account_id === String(bindings[1]) &&
          names.has(
            `${String(claim.resource_type)}:${String(claim.resource_name)}`,
          ),
      );
    }
    if (sql.startsWith('INSERT INTO anchorage_fleet_deployments')) {
      const names = [
        'tenant_tag',
        'environment',
        'backend',
        'script_name',
        'database_id',
        'database_name',
        'schema_version',
        'artifact_version',
        'desired_spec_digest',
        'pending_spec_digest',
        'pending_artifact_version',
        'active_release',
        'pending_release',
        'migration_prior_release',
        'rollback_release',
        'retiring_release',
        'outbound_policy',
        'platform_resources',
        'platform_target',
        'migration_intent',
        'backend_switch_intent',
        'durable_object_tag',
        'durable_object_migration_history',
        'durable_object_migration_history_digest',
        'durable_object_bindings',
        'application_resources',
        'application_bindings',
        'route_hostname',
        'phase',
        'database_export_location',
        'database_export_sha256',
        'database_export_size',
        'settled_settlement_key',
        'updated_at',
      ];
      this.row = Object.fromEntries(
        names.map((name, index) => [name, bindings[index] ?? null]),
      );
      return [{ tenant_tag: bindings[0], environment: bindings[1] }];
    }
    return [];
  }

  async execute(sql: string, bindings: readonly unknown[] = []): Promise<void> {
    if (sql.startsWith('DELETE FROM anchorage_platform_plane_claims')) {
      const [accountId, setKey, identity, resourceType, resourceName, role] =
        bindings.map(String);
      for (const [key, claim] of this.claims) {
        if (
          claim.account_id === accountId &&
          claim.resource_set_key === setKey &&
          claim.platform_plane_identity === identity &&
          (claim.resource_type !== resourceType ||
            claim.resource_name !== resourceName ||
            claim.resource_role !== role)
        ) {
          this.claims.delete(key);
        }
      }
    }
  }

  async batch(
    statements: readonly Readonly<{
      sql: string;
      bindings?: readonly unknown[];
    }>[],
  ): Promise<readonly (readonly Readonly<Record<string, unknown>>[])[]> {
    const priorRow = this.row;
    const priorClaims = new Map(this.claims);
    try {
      const results: Array<readonly Readonly<Record<string, unknown>>[]> = [];
      for (const { sql, bindings = [] } of statements) {
        if (sql.includes('INSERT INTO anchorage_platform_plane_claims')) {
          const desired = JSON.parse(String(bindings[0])) as Array<{
            resourceType: string;
            resourceName: string;
            resourceRole: string;
          }>;
          const [accountId, setKey, identity] = bindings
            .slice(1, 4)
            .map(String);
          const inserted: Readonly<Record<string, unknown>>[] = [];
          for (const claim of desired) {
            const key = `${accountId}:${claim.resourceType}:${claim.resourceName}`;
            if (this.claims.has(key)) throw new Error('claim conflict');
            const row = {
              account_id: accountId,
              resource_type: claim.resourceType,
              resource_name: claim.resourceName,
              resource_role: claim.resourceRole,
              resource_set_key: setKey,
              platform_plane_identity: identity,
            };
            this.claims.set(key, row);
            inserted.push(row);
          }
          results.push(inserted);
        } else if (
          sql.includes('DELETE FROM anchorage_platform_plane_claims')
        ) {
          const desired = JSON.parse(String(bindings[0])) as Array<{
            resourceType: string;
            resourceName: string;
            resourceRole: string;
          }>;
          const [accountId, setKey, identity] = bindings
            .slice(1, 4)
            .map(String);
          const expected = new Set(
            desired.map(
              ({ resourceType, resourceName, resourceRole }) =>
                `${resourceType}:${resourceName}:${resourceRole}`,
            ),
          );
          const names = new Set(
            desired.map(
              ({ resourceType, resourceName }) =>
                `${resourceType}:${resourceName}`,
            ),
          );
          const removed: Readonly<Record<string, unknown>>[] = [];
          for (const [key, claim] of this.claims) {
            if (
              claim.account_id === accountId &&
              claim.resource_set_key === setKey &&
              claim.platform_plane_identity === identity &&
              (sql.includes('AND NOT EXISTS')
                ? !expected.has(
                    `${String(claim.resource_type)}:${String(claim.resource_name)}:${String(claim.resource_role)}`,
                  )
                : names.has(
                    `${String(claim.resource_type)}:${String(claim.resource_name)}`,
                  ))
            ) {
              this.claims.delete(key);
              removed.push(claim);
            }
          }
          results.push(removed);
        } else if (sql.includes('INSERT INTO anchorage_fleet_deployments')) {
          results.push(await this.query(sql, bindings.slice(3)));
        } else {
          results.push(await this.query(sql, bindings));
        }
      }
      return results;
    } catch (error) {
      this.row = priorRow;
      this.claims.clear();
      for (const [key, value] of priorClaims) this.claims.set(key, value);
      throw error;
    }
  }
}

class SchemaD1 implements FleetStateDatabase {
  readonly columns = new Map<string, Readonly<Record<string, unknown>>>(
    UPGRADED_COLUMNS.map((name) => [name, nullableTextColumn(name)]),
  );
  createAttempts = 0;
  alterAttempts = 0;
  failCreateOnce = false;
  failAlterOnce = false;

  async query(
    sql: string,
  ): Promise<readonly Readonly<Record<string, unknown>>[]> {
    if (sql.startsWith('PRAGMA table_info')) return [...this.columns.values()];
    return [];
  }

  async execute(sql: string): Promise<void> {
    if (
      sql.startsWith('CREATE TABLE IF NOT EXISTS anchorage_fleet_deployments')
    ) {
      this.createAttempts += 1;
      if (this.failCreateOnce) {
        this.failCreateOnce = false;
        throw new Error('transient schema failure');
      }
    }
    const added = addedColumnName(sql);
    if (added) {
      this.alterAttempts += 1;
      if (this.failAlterOnce) {
        this.failAlterOnce = false;
        throw new Error('genuine migration failure');
      }
      this.columns.set(added, nullableTextColumn(added));
    }
  }

  async batch(): Promise<
    readonly (readonly Readonly<Record<string, unknown>>[])[]
  > {
    return [];
  }
}

class ConcurrentSchemaD1 extends SchemaD1 {
  /** Every column an ALTER found already present — a real racer's signature. */
  readonly duplicates: string[] = [];
  readonly #bothInspected: Promise<void>;
  #releaseInspections!: () => void;
  #coldInspections = 0;

  constructor() {
    super();
    this.columns.clear();
    this.#bothInspected = new Promise((resolve) => {
      this.#releaseInspections = resolve;
    });
  }

  override async query(
    sql: string,
  ): Promise<readonly Readonly<Record<string, unknown>>[]> {
    if (sql.startsWith('PRAGMA table_info') && this.#coldInspections < 2) {
      this.#coldInspections += 1;
      if (this.#coldInspections === 2) this.#releaseInspections();
      await this.#bothInspected;
      return [];
    }
    return super.query(sql);
  }

  override async execute(sql: string): Promise<void> {
    const added = addedColumnName(sql);
    if (added && this.columns.has(added)) {
      this.alterAttempts += 1;
      this.duplicates.push(added);
      throw new Error('D1 migration failed', {
        cause: new Error(`duplicate column name: ${added}: SQLITE_ERROR`),
      });
    }
    await super.execute(sql);
  }
}

function reservedRecord(
  backend: FleetRecord['backend'],
  scriptName = 'tenant-worker',
): FleetRecord {
  return {
    tenantTag: 'acme',
    backend,
    environment: 'production',
    scriptName,
    databaseId: 'reserved-database',
    databaseName: 'acme-production',
    schemaVersion: 1,
    artifactVersion: 'pending',
    desiredSpecDigest: 'a'.repeat(64),
    durableObjectBindings: [],
    routeHostname: 'acme.example.test',
    phase: 'database-reserved',
    updatedAt: '2026-08-11T00:00:00.000Z',
  };
}

function platformSet(workerName: string): PlatformPlaneResourceSet {
  return {
    accountId: 'account',
    dispatchNamespace: 'dispatch-namespace',
    dispatchScriptName: workerName,
    outboundScriptName: 'shared-outbound',
    auditScriptName: 'shared-audit',
    hostRoutingKvId: 'host-routing-kv',
    auditQueueName: 'audit-queue',
    maintenanceCapabilityPublicKey:
      '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}',
  };
}

describe('D1FleetStateStore release state', () => {
  it('retries a transient schema bootstrap failure on the same instance', async () => {
    const db = new SchemaD1();
    db.failCreateOnce = true;
    const store = new D1FleetStateStore(db, { accountId: 'account' });

    await expect(store.get('acme', 'production')).rejects.toThrow(
      /transient schema failure/u,
    );
    await expect(store.get('acme', 'production')).resolves.toBeUndefined();
    expect(db.createAttempts).toBe(2);
  });

  it('accepts only a verified concurrent duplicate-column race', async () => {
    const db = new ConcurrentSchemaD1();
    const first = new D1FleetStateStore(db, { accountId: 'account' });
    const second = new D1FleetStateStore(db, { accountId: 'account' });

    await expect(
      Promise.all([
        first.get('acme', 'production'),
        second.get('other', 'production'),
      ]),
    ).resolves.toEqual([undefined, undefined]);
    // #then the losing racer's duplicate-column error was swallowed, and every
    // upgraded column ended well-formed rather than half-applied
    expect(db.duplicates.length).toBeGreaterThan(0);
    for (const name of UPGRADED_COLUMNS) {
      expect(db.columns.get(name)).toMatchObject({
        name,
        type: 'TEXT',
        notnull: 0,
        pk: 0,
      });
    }
  });

  it('propagates a genuine migration failure and retries it later', async () => {
    const db = new SchemaD1();
    db.columns.clear();
    db.failAlterOnce = true;
    const store = new D1FleetStateStore(db, { accountId: 'account' });

    await expect(store.get('acme', 'production')).rejects.toThrow(
      /genuine migration failure/u,
    );
    await expect(store.get('acme', 'production')).resolves.toBeUndefined();
    // The failed column is retried, and every other upgraded column still runs.
    expect(db.alterAttempts).toBe(UPGRADED_COLUMNS.length + 1);
    expect([...db.columns.keys()].sort()).toEqual([...UPGRADED_COLUMNS].sort());
  });

  it('verifies current column shape and does not repeat migration work', async () => {
    const current = new SchemaD1();
    const currentStore = new D1FleetStateStore(current, {
      accountId: 'account',
    });
    await currentStore.get('acme', 'production');
    await currentStore.get('other', 'production');
    expect(current.createAttempts).toBe(1);
    expect(current.alterAttempts).toBe(0);

    const incompatible = new SchemaD1();
    incompatible.columns.set('settled_settlement_key', {
      name: 'settled_settlement_key',
      type: 'INTEGER',
      notnull: 1,
      pk: 0,
    });
    await expect(
      new D1FleetStateStore(incompatible, { accountId: 'account' }).get(
        'acme',
        'production',
      ),
    ).rejects.toThrow(/absent or incompatible/u);
  });

  it('atomically transitions the deployment claim role with backend ownership', async () => {
    const db = new MemoryD1();
    const store = new D1FleetStateStore(db, { accountId: 'account' });
    const plain = reservedRecord('plain-worker');
    await store.withDeploymentLease('acme', 'production', (lease) =>
      lease.put(plain),
    );
    const external = { ...plain, backend: 'workers-for-platforms' as const };

    await store.withDeploymentLease('acme', 'production', (lease) =>
      lease.put(external),
    );

    expect([...db.claims.values()]).toEqual([
      expect.objectContaining({
        resource_type: 'dispatch-script',
        resource_name: externalStateScriptName(external),
        resource_role: 'deployment-state',
      }),
    ]);
  });

  it('atomically flips an adopted ordinary bridge claim across switch rollback', async () => {
    const db = new MemoryD1();
    const store = new D1FleetStateStore(db, { accountId: 'account' });
    const plain = reservedRecord('plain-worker');
    await store.withDeploymentLease('acme', 'production', (lease) =>
      lease.put(plain),
    );
    const switched: FleetRecord = {
      ...plain,
      backend: 'workers-for-platforms',
      phase: 'ready',
      platformResources: {
        maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
        stateWorker: {
          scriptName: plain.scriptName,
          artifactVersion: 'bridge-v1',
          artifactDigest: 'a'.repeat(64),
          plane: 'ordinary',
          durableObjectBindings: [],
          namespaceIds: [],
        },
      },
    };
    await store.withDeploymentLease('acme', 'production', (lease) =>
      lease.put(switched),
    );
    expect([...db.claims.values()]).toEqual([
      expect.objectContaining({
        resource_type: 'worker-script',
        resource_name: plain.scriptName,
        resource_role: 'deployment-state',
      }),
    ]);

    await store.withDeploymentLease('acme', 'production', (lease) =>
      lease.put({ ...plain, phase: 'ready' }),
    );
    expect([...db.claims.values()]).toEqual([
      expect.objectContaining({
        resource_type: 'worker-script',
        resource_name: plain.scriptName,
        resource_role: 'deployment-worker',
      }),
    ]);
  });

  it('rejects a plain deployment name already reserved by the platform plane', async () => {
    const db = new MemoryD1();
    const store = new D1FleetStateStore(db, { accountId: 'account' });
    await store.withPlatformPlaneLease(
      platformSet('tenant-worker'),
      'platform-v1',
      async () => {},
    );

    await expect(
      store.withDeploymentLease('acme', 'production', (lease) =>
        lease.put(reservedRecord('plain-worker')),
      ),
    ).rejects.toThrow(/overlap another durable reservation/);
    expect(db.row).toBeUndefined();
  });

  it('rejects a platform-plane name already reserved by a plain deployment', async () => {
    const db = new MemoryD1();
    const store = new D1FleetStateStore(db, { accountId: 'account' });
    await store.withDeploymentLease('acme', 'production', (lease) =>
      lease.put(reservedRecord('plain-worker')),
    );

    await expect(
      store.withPlatformPlaneLease(
        platformSet('tenant-worker'),
        'platform-v1',
        async () => {},
      ),
    ).rejects.toThrow(/overlaps another durable reservation/);
  });

  it.each([
    'state',
    'egress',
  ] as const)('does not consume an ordinary Worker claim for external trusted %s resources', async (role) => {
    const db = new MemoryD1();
    const store = new D1FleetStateStore(db, { accountId: 'account' });
    const record = reservedRecord('workers-for-platforms');
    const trustedName =
      role === 'state'
        ? externalStateScriptName(record)
        : externalEgressProxyScriptName(record);
    await store.withDeploymentLease('acme', 'production', (lease) =>
      lease.put(record),
    );

    await expect(
      store.withPlatformPlaneLease(
        platformSet(trustedName),
        'platform-v1',
        async () => {},
      ),
    ).resolves.toBeUndefined();
  });

  it('round-trips active, pending, and retained immutable release metadata', async () => {
    const db = new MemoryD1();
    const store = new D1FleetStateStore(db, { accountId: 'account' });
    const policy = canonicalDeploymentEgressPolicy({
      policyId: 'policy-acme',
      tenantTag: 'acme',
      environment: 'production',
      allowedHosts: ['api.example.com'],
    });
    const application = {
      vars: [{ name: 'API_ORIGIN', value: 'https://api.example.test' }],
      secrets: [{ name: 'API_TOKEN', valueSha256: '7'.repeat(64) }],
      r2Buckets: [],
    };
    const topology = {
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      secretNames: ['API_TOKEN', 'DEPLOYMENT_IDENTITY_SECRET'],
      application,
    };
    const durableObjectMigrationHistory = [
      { tag: 'v1', newClasses: ['Runner'] },
      { tag: 'v2', newSqliteClasses: ['Maintenance'] },
    ];
    const record: FleetRecord = {
      tenantTag: 'acme',
      backend: 'workers-for-platforms',
      environment: 'production',
      scriptName: 'acme-production',
      databaseId: 'db-acme',
      databaseName: 'acme-production',
      schemaVersion: 2,
      artifactVersion: 'etag-current',
      desiredSpecDigest: 'a'.repeat(64),
      pendingSpecDigest: 'b'.repeat(64),
      activeRelease: {
        physicalScriptName: 'acme-production-aaaaaaaaaaaaaaaaaaaa',
        specDigest: 'a'.repeat(64),
        artifactVersion: 'etag-current',
        releaseSchemaVersion: 1,
        application,
        topology,
      },
      pendingRelease: {
        physicalScriptName: 'acme-production-bbbbbbbbbbbbbbbbbbbb',
        specDigest: 'b'.repeat(64),
        artifactVersion: 'etag-pending',
        releaseSchemaVersion: 2,
        application,
        topology,
      },
      migrationPriorRelease: {
        physicalScriptName: 'acme-production-aaaaaaaaaaaaaaaaaaaa',
        specDigest: 'a'.repeat(64),
        artifactVersion: 'etag-current',
        releaseSchemaVersion: 1,
        application,
        topology,
      },
      rollbackRelease: {
        physicalScriptName: 'acme-production-cccccccccccccccccccc',
        specDigest: 'c'.repeat(64),
        artifactVersion: 'etag-retained',
        releaseSchemaVersion: 1,
        application,
        topology,
      },
      retiringRelease: {
        physicalScriptName: 'acme-production-dddddddddddddddddddd',
        specDigest: 'd'.repeat(64),
        artifactVersion: 'etag-retiring',
        releaseSchemaVersion: 1,
        application,
        topology,
      },
      outboundPolicy: policy,
      platformResources: {
        maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
        stateWorker: {
          scriptName: 'acme-production-state-a1b2c3d4',
          artifactVersion: 'state-version',
          artifactDigest: 'e'.repeat(64),
          plane: 'dispatch',
          dispatchNamespace: 'fleet-tenants',
          durableObjectTag: 'state-v1',
          durableObjectBindings: [],
          namespaceIds: [],
        },
        egressProxy: {
          scriptName: 'acme-production-egress-a1b2c3d4',
          artifactVersion: 'egress-version',
          artifactDigest: 'f'.repeat(64),
          ...policy,
        },
      },
      platformTarget: {
        maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
        stateArtifactDigest: 'e'.repeat(64),
        stateDurableObjectHistoryDigest: '1'.repeat(64),
        stateDurableObjectTag: 'state-v1',
        egressArtifactDigest: 'f'.repeat(64),
        d1SchemaVersion: 2,
        d1SchemaHistoryDigest: '2'.repeat(64),
        outboundPolicy: policy,
      },
      migrationIntent: {
        targetSpecDigest: 'b'.repeat(64),
        priorRelease: {
          physicalScriptName: 'acme-production-aaaaaaaaaaaaaaaaaaaa',
          specDigest: 'a'.repeat(64),
          artifactVersion: 'etag-current',
          releaseSchemaVersion: 1,
          application,
          topology,
        },
        priorTarget: {
          maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
          stateArtifactDigest: 'e'.repeat(64),
          stateDurableObjectHistoryDigest: '1'.repeat(64),
          stateDurableObjectTag: 'state-v1',
          egressArtifactDigest: 'f'.repeat(64),
          d1SchemaVersion: 2,
          d1SchemaHistoryDigest: '2'.repeat(64),
          outboundPolicy: policy,
        },
        priorOutboundPolicy: policy,
        targetRelease: {
          physicalScriptName: 'acme-production-bbbbbbbbbbbbbbbbbbbb',
          specDigest: 'b'.repeat(64),
          artifactVersion: 'etag-pending',
          releaseSchemaVersion: 2,
          application,
          topology,
        },
        target: {
          maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
          stateArtifactDigest: '3'.repeat(64),
          stateDurableObjectHistoryDigest: '4'.repeat(64),
          stateDurableObjectTag: 'state-v2',
          egressArtifactDigest: '5'.repeat(64),
          d1SchemaVersion: 2,
          d1SchemaHistoryDigest: '6'.repeat(64),
          outboundPolicy: policy,
        },
        subphase: 'planned',
      },
      durableObjectTag: 'v2',
      durableObjectMigrationHistory,
      durableObjectMigrationHistoryDigest: durableObjectMigrationHistoryDigest(
        durableObjectMigrationHistory,
      ),
      durableObjectBindings: [],
      applicationResources: [],
      applicationBindings: application,
      routeHostname: 'acme.example.test',
      phase: 'migrating',
      updatedAt: '2026-08-10T00:00:00.000Z',
    };

    await store.withDeploymentLease('acme', 'production', (lease) =>
      lease.put(record),
    );
    await expect(store.get('acme', 'production')).resolves.toEqual(record);

    const serializedIntent = JSON.parse(
      String(db.row?.migration_intent),
    ) as Record<string, unknown>;
    db.row = {
      ...db.row,
      migration_intent: JSON.stringify({
        ...serializedIntent,
        targetSpecDigest: '9'.repeat(64),
      }),
    };
    await expect(store.get('acme', 'production')).rejects.toThrow(
      /inconsistent migration intent/,
    );

    const {
      pendingRelease: _pendingRelease,
      migrationPriorRelease: _migrationPriorRelease,
      pendingSpecDigest: _pendingSpecDigest,
      ...stable
    } = record;
    if (!stable.activeRelease) throw new Error('missing active release');
    if (!stable.platformTarget || !stable.outboundPolicy) {
      throw new Error('missing stable external platform target');
    }
    const platformOnly: FleetRecord = {
      ...stable,
      backendSwitchIntent: {
        kind: 'backend-switch',
        tenantTag: stable.tenantTag,
        environment: stable.environment,
        prior: {
          scriptName: stable.scriptName,
          artifactVersion: 'plain-v1',
          specDigest: stable.activeRelease.specDigest,
          databaseId: stable.databaseId,
          databaseName: stable.databaseName,
          durableObjectBindings: [],
          namespaceIds: [],
          secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
          applicationResources: [],
          customDomain: {
            id: 'domain-acme',
            hostname: stable.routeHostname,
          },
        },
        targetSpecDigest: stable.activeRelease.specDigest,
        targetApplication: application,
        target: stable.platformTarget,
        rollbackUntil: '2026-08-20T00:00:00.000Z',
        subphase: 'finalized',
      },
      migrationIntent: {
        platformOnly: true,
        targetSpecDigest: stable.activeRelease.specDigest,
        priorRelease: stable.activeRelease,
        priorTarget: stable.platformTarget,
        priorOutboundPolicy: stable.outboundPolicy,
        targetRelease: stable.activeRelease,
        target: {
          maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
          stateArtifactDigest: '7'.repeat(64),
          stateDurableObjectHistoryDigest: '8'.repeat(64),
          stateDurableObjectTag: 'state-v2',
          egressArtifactDigest: '9'.repeat(64),
          d1SchemaVersion: stable.schemaVersion,
          d1SchemaHistoryDigest: 'a'.repeat(64),
          outboundPolicy: policy,
        },
        subphase: 'planned',
      },
    };
    await store.withDeploymentLease('acme', 'production', (lease) =>
      lease.put(platformOnly),
    );
    await expect(store.get('acme', 'production')).resolves.toEqual(
      platformOnly,
    );
    expect(db.row?.migration_intent).toBeTypeOf('string');
    expect(db.row?.backend_switch_intent).toBeTypeOf('string');
  });

  it('round-trips the settled settlement key and refuses a malformed one', async () => {
    // #given a deployment that has completed a settlement
    const db = new MemoryD1();
    const store = new D1FleetStateStore(db, { accountId: 'account' });
    const settledSettlementKey = '7'.repeat(64);
    const record: FleetRecord = {
      tenantTag: 'acme',
      environment: 'production',
      backend: 'plain-worker',
      scriptName: 'acme-production',
      databaseId: 'db-acme',
      databaseName: 'acme-production',
      schemaVersion: 1,
      artifactVersion: 'artifact-v1',
      desiredSpecDigest: 'a'.repeat(64),
      durableObjectBindings: [],
      routeHostname: 'acme.example.test',
      phase: 'ready',
      settledSettlementKey,
      updatedAt: '2026-08-10T00:00:00.000Z',
    };

    // #when it is written and read back
    await store.withDeploymentLease('acme', 'production', (lease) =>
      lease.put(record),
    );

    // #then the key survives the row, in its own column rather than folded
    // into a blob a later reader would have to guess at
    await expect(store.get('acme', 'production')).resolves.toMatchObject(
      record,
    );
    expect(db.row?.settled_settlement_key).toBe(settledSettlementKey);

    // #and a row carrying something that is not a key is refused rather than
    // read as "settled", which would silently suppress a real settlement
    db.row = { ...db.row, settled_settlement_key: 'not-a-key' };
    await expect(store.get('acme', 'production')).rejects.toThrow(
      /invalid settled_settlement_key/,
    );

    // #and an absent key reads as "never settled", so a database written
    // before this column existed settles once more rather than never
    db.row = { ...db.row, settled_settlement_key: null };
    const unsettled = await store.get('acme', 'production');
    expect(unsettled).toBeDefined();
    expect(unsettled).not.toHaveProperty('settledSettlementKey');
  });

  it('refuses to use a lease capability for a different deployment key', async () => {
    const store = new D1FleetStateStore(new MemoryD1(), {
      accountId: 'account',
    });
    const record: FleetRecord = {
      tenantTag: 'other',
      environment: 'production',
      backend: 'plain-worker',
      scriptName: 'other-production',
      databaseId: 'db-other',
      databaseName: 'other-production',
      schemaVersion: 1,
      artifactVersion: 'artifact-other',
      desiredSpecDigest: 'd'.repeat(64),
      durableObjectBindings: [],
      routeHostname: 'other.example.test',
      phase: 'ready',
      updatedAt: '2026-08-10T00:00:00.000Z',
    };

    await expect(
      store.withDeploymentLease('acme', 'production', (lease) =>
        lease.put(record),
      ),
    ).rejects.toThrow(/cannot write 'other:production'/);
  });

  it('rejects malformed persisted platform resource ownership', async () => {
    const db = new MemoryD1();
    const store = new D1FleetStateStore(db, { accountId: 'account' });
    const base: FleetRecord = {
      tenantTag: 'acme',
      environment: 'production',
      backend: 'workers-for-platforms',
      scriptName: 'acme-production',
      databaseId: 'db-acme',
      databaseName: 'acme-production',
      schemaVersion: 1,
      artifactVersion: 'release-version',
      desiredSpecDigest: 'a'.repeat(64),
      outboundPolicy: canonicalDeploymentEgressPolicy({
        policyId: 'policy-acme',
        tenantTag: 'acme',
        environment: 'production',
        allowedHosts: [],
      }),
      durableObjectBindings: [],
      routeHostname: 'acme.example.test',
      phase: 'platform-resources-deployed',
      platformResources: {
        maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
        stateWorker: {
          scriptName: 'acme-production-state',
          artifactVersion: 'state-version',
          artifactDigest: 'a'.repeat(64),
          plane: 'dispatch',
          dispatchNamespace: 'fleet-tenants',
          durableObjectBindings: [],
          namespaceIds: [],
        },
      },
      updatedAt: '2026-08-11T00:00:00.000Z',
    };
    await store.withDeploymentLease('acme', 'production', (lease) =>
      lease.put(base),
    );
    db.row = {
      ...db.row,
      platform_resources: JSON.stringify({
        stateWorker: {
          scriptName: '../other',
          artifactVersion: 'state-version',
          artifactDigest: 'e'.repeat(64),
        },
        egressProxy: {
          scriptName: 'acme-egress',
          artifactVersion: 'egress-version',
          artifactDigest: 'f'.repeat(64),
          policyId: 'policy-acme',
          policyHosts: ['api.example.com'],
          policyDigest: '1'.repeat(64),
        },
      }),
    };

    await expect(store.get('acme', 'production')).rejects.toThrow(
      /invalid platform_resources/,
    );
  });
});
