// SPDX-License-Identifier: Apache-2.0
/// <reference types="@cloudflare/workers-types" />

import { D1CloudflareApiRateCoordinator } from '../../src/cloudflare-rate-coordinator.js';
import { D1FleetStateDatabase } from '../../src/d1-fleet-state-database.js';
import {
  canonicalDeploymentEgressPolicy,
  externalEgressProxyScriptName,
  externalStateScriptName,
} from '../../src/platform-resources.js';
import {
  ADDED_NULLABLE_TEXT_COLUMNS,
  D1FleetStateStore,
  type FleetStateDatabase,
} from '../../src/state-store.js';
import type {
  FleetRecord,
  FleetStateLease,
  PlatformPlaneLease,
  PlatformPlaneResourceSet,
} from '../../src/types.js';

interface Env {
  DB: D1Database;
}

const STATE_TABLE = 'anchorage_fleet_deployments';
const LEASE_TABLE = 'anchorage_fleet_leases';
const PLATFORM_CLAIM_TABLE = 'anchorage_platform_plane_claims';
const PLATFORM_LEASE_TABLE = 'anchorage_platform_plane_leases';
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
  return {
    ...base,
    phase: 'decommission-advancing',
    decommissionIntent: {
      version: 1,
      operationId: '123e4567-e89b-42d3-a456-426614174000',
      revision,
      generation: 0,
      updatedAt: `2026-08-11T00:00:${String(revision).padStart(2, '0')}.000Z`,
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
        mode: {
          kind: 'normal',
          requestedSpecDigest: base.desiredSpecDigest,
          entryLifecyclePhase: 'ready',
        },
      },
      lifecyclePhase: 'ready',
      state: 'transitioning',
    },
  };
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
    const body = (await request.json()) as { action?: unknown };
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
