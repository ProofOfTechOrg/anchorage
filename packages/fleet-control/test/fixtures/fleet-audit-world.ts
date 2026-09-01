// SPDX-License-Identifier: Apache-2.0

/**
 * Hand-authored deterministic world for the `auditFleetDrift` golden baseline.
 * `scripts/record-audit-baseline.mjs` and `test/fleet-audit-golden.test.ts` both
 * import this file; the recorder NEVER writes it, so the recorded literals can
 * never rewrite their own input.
 *
 * The world freezes the SHIPPED behavior of `auditFleetDrift` in
 * `src/fleet.ts` (lines 560-1403) before its internals are decomposed into
 * bounded stages (R4-B.2), so the decomposition can be proven
 * behavior-equivalent. It drives every finding kind `auditFleetDrift` itself
 * pushes except `duplicate-database` (pinned exact-order by
 * fleet.test.ts:803); provider-supplied inventory-finding kinds are
 * represented by the single seeded `stale-route`. It also records both
 * the exact findings array AND the exact sequence of calls the function makes
 * onto its `store`, `backendFor`, `specFor`, and `maintenanceSecretFor`
 * collaborators (the "op log") — every
 * `withDeploymentLease`/`get`/`put`/`inspect`/`ensureMaintenance` call, every
 * `resolver:<kind>` invocation, and every `lease.assertOwned()` call (tagged
 * `assertOwned:<tenantTag>:<environment>`, the only op token that carries a
 * key — every other token is bare because relative order alone identifies it
 * against this frozen, single-pass world).
 *
 * Most records are independent "stories", each engineered to trip one or a
 * few specific `findings.push(...)` sites in `auditFleetDrift`; a handful of
 * sites are deliberately left uncovered by this world and pinned instead by
 * the existing exact-order titles in `test/fleet.test.ts`:
 *   - fleet.ts:1047, :1059, :1103, :1273 (the `String(error)` resolver- and
 *     inspection-failure catch sites) — this world's `backendFor`/
 *     `specFor`/`maintenanceSecretFor`/`inspect` collaborators never throw,
 *     so only `test/fleet.test.ts:2384` ("contains resolver, inspection,
 *     and maintenance re-arm failures per deployment") exercises them.
 *   - fleet.ts:1291, :1300, :1313, :1328 (the `database-mismatch`,
 *     `duplicate-database`, `duplicate-namespace`, and `version-drift`
 *     checks that compare against `backend.inspect()`'s OWN reported
 *     identity, not the per-record inventory entry) — pinned instead by
 *     `test/fleet.test.ts:803` ("finds duplicate ownership, version drift,
 *     and re-arms stale maintenance"); `duplicate-database` (fleet.ts:1300)
 *     has no story in this world at all.
 *   - fleet.ts:1393 (`maintenance re-arm failed`) IS covered here (by
 *     `rearmFail`, below), so `test/fleet.test.ts:2384`'s pin of the same
 *     site is redundant with this world, not a gap it fills.
 *   - fleet.ts:581-608 and the fleet.ts:866 `continue` (the active-bounded-
 *     cleanup suppression branch) — no record in this world has an active
 *     cleanup, so the branch is deliberately unexercised here; pinned
 *     instead by `test/fleet.test.ts:2454` ("suppresses drift findings in
 *     both directions for a deployment under active bounded cleanup") and
 *     `test/fleet.test.ts:2536` ("reports no incomplete provisioning for a
 *     stale blocked cleanup record").
 */

import { auditFleetDrift, type DriftFinding } from '../../src/fleet.js';
import { providerBindingIdentitiesForInspection } from '../../src/provider-binding-inventory.js';
import type {
  ActiveRouteAttestation,
  CleanupTerminalReceipt,
  DatabaseExport,
  DatabaseReference,
  DeploymentSpec,
  ExternalMutationFence,
  FleetInventoryDeployment,
  FleetRecord,
  FleetResourceInventory,
  FleetStateLease,
  FleetStateStore,
  LiveDeployment,
  MaintenanceHealth,
  ProvisioningBackend,
  ProvisioningBackendKind,
  SeedDeploymentIdentityOptions,
} from '../../src/types.js';

const ENVIRONMENT = 'production';
const SPEC_DIGEST = 'a'.repeat(64);

/** Frozen clock: every staleness comparison and the re-arm authority clock. */
const AUDIT_NOW = Date.parse('2026-06-01T00:00:00.000Z');
const AUDIT_STALE_AFTER_MS = 3_600_000;

const FRESH_UPDATED_AT = new Date(AUDIT_NOW - 30 * 60_000).toISOString();
const STALE_UPDATED_AT = new Date(
  AUDIT_NOW - 10 * AUDIT_STALE_AFTER_MS,
).toISOString();

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

/** A live inspection result that matches `record` exactly (no drift). */
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

/** A live inventory deployment entry that matches `record` exactly. */
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

/** A live inventory deployment entry: empty bindings and routes unless the caller supplies them. */
function bareDeployment(
  fields: Pick<
    FleetInventoryDeployment,
    'scriptName' | 'tenantTag' | 'artifactVersion'
  > &
    Partial<FleetInventoryDeployment>,
): FleetInventoryDeployment {
  return {
    backend: 'plain-worker',
    environment: ENVIRONMENT,
    databaseIds: [],
    durableObjectBindings: [],
    secretNames: [],
    plainTextBindings: {},
    routeHostnames: [],
    schemaVersion: 1,
    ...fields,
  };
}

// ---------------------------------------------------------------------------
// Records. Every record but `control` cites the fleet.ts lines its story targets.
// ---------------------------------------------------------------------------

/** A fully healthy deployment: proves the audit does not false-positive. */
const control = baseRecord('control');

/**
 * Two live `inventory.deployments` entries answer this record's one expected
 * script key -> fleet.ts:680 `duplicate-deployment` ("appears N times").
 */
const liveDup = baseRecord('livedup');

/**
 * Two records share `scriptName` ("shared-record-script"), so both fall
 * under one `recordsByScript` key with two members -> fleet.ts:871
 * `duplicate-deployment` ("is registered N times"), for BOTH records. `b` is
 * phase `worker-deployed` (not `ready`) so it exits at the fleet.ts:965 gate
 * before any of the ready-only per-record checks can also fire.
 */
const recordDupA = baseRecord('recdupa', {
  scriptName: 'shared-record-script',
});
const recordDupB = baseRecord('recdupb', {
  scriptName: 'shared-record-script',
  phase: 'worker-deployed',
});

/**
 * Two records both declare `durableObjectBindings` under the SAME namespace
 * id -> fleet.ts:766 `duplicate-namespace` (expected side), for the second
 * one processed.
 */
const SHARED_EXPECTED_NAMESPACE = 'ns-shared-expected';
const namespaceDupA = baseRecord('dupexpnsa', {
  durableObjectBindings: [
    {
      name: 'RUNNER',
      className: 'Runner',
      namespaceId: SHARED_EXPECTED_NAMESPACE,
    },
  ],
});
const namespaceDupB = baseRecord('dupexpnsb', {
  durableObjectBindings: [
    {
      name: 'RUNNER',
      className: 'Runner',
      namespaceId: SHARED_EXPECTED_NAMESPACE,
    },
  ],
});

/**
 * Expects a namespace id absent from `inventory.namespaceIds` -> fleet.ts:776
 * `missing-namespace`.
 */
const MISSING_EXPECTED_NAMESPACE = 'ns-missing-expected';
const missingNamespace = baseRecord('missingns', {
  durableObjectBindings: [
    {
      name: 'RUNNER',
      className: 'Runner',
      namespaceId: MISSING_EXPECTED_NAMESPACE,
    },
  ],
});

/**
 * Two records both claim the same R2 bucket -> fleet.ts:811 `r2-bucket-drift`
 * ("claimed by more than one deployment"), for the second one processed.
 */
const SHARED_BUCKET_CREATION_DATE = '2026-01-01T00:00:00.000Z';
const bucketDupA = baseRecord('r2dupa', {
  applicationResources: [
    {
      name: 'EXPORTS',
      bucketName: 'shared-bucket',
      jurisdiction: 'default',
      state: 'created',
      reservationNonce: 'nonce-r2dupa',
      creationDate: SHARED_BUCKET_CREATION_DATE,
    },
  ],
});
const bucketDupB = baseRecord('r2dupb', {
  applicationResources: [
    {
      name: 'EXPORTS',
      bucketName: 'shared-bucket',
      jurisdiction: 'default',
      state: 'created',
      reservationNonce: 'nonce-r2dupb',
      creationDate: SHARED_BUCKET_CREATION_DATE,
    },
  ],
});

/** Claims a bucket absent from `inventory.r2Buckets` -> fleet.ts:844 `missing-r2-bucket`. */
const bucketMissing = baseRecord('r2missing', {
  applicationResources: [
    {
      name: 'EXPORTS',
      bucketName: 'bucket-missing',
      jurisdiction: 'default',
      state: 'created',
      reservationNonce: 'nonce-r2missing',
      creationDate: '2026-01-01T00:00:00.000Z',
    },
  ],
});

/**
 * Claims a bucket present in `inventory.r2Buckets` under a different
 * jurisdiction and creation date -> fleet.ts:854 `r2-bucket-drift` ("changed
 * its persisted creation identity").
 */
const bucketDrift = baseRecord('r2drift', {
  applicationResources: [
    {
      name: 'EXPORTS',
      bucketName: 'bucket-drift',
      jurisdiction: 'default',
      state: 'created',
      reservationNonce: 'nonce-r2drift',
      creationDate: '2026-02-01T00:00:00.000Z',
    },
  ],
});

/** No live deployment at all answers this record's expected key -> fleet.ts:672 `missing-deployment`. */
const missingDeploy = baseRecord('missingdeploy');

/** The live inventory deployment's `databaseIds` mismatch -> fleet.ts:974 `database-mismatch`. */
const dbMismatch = baseRecord('dbmismatch');

/** The live inventory deployment's Durable Object bindings mismatch -> fleet.ts:1012 `binding-drift`. */
const bindingMismatch = baseRecord('bindingmismatch');

/**
 * No live deployment route and no matching `inventory.routes` entry ->
 * fleet.ts:996 `route-drift` ("does not contain exactly route") AND
 * fleet.ts:1036 `route-drift` ("is missing or mismatched").
 */
const routeBroken = baseRecord('routebroken');

/** Two `inventory.routes` entries share this record's hostname -> fleet.ts:1021 `duplicate-route`. */
const routeDup = baseRecord('routedup');

/**
 * Declares `platformResources.stateWorker` AND `platformResources.egressProxy`.
 * Each declared worker's one matching live deployment entry carries the
 * wrong ownership metadata (no `resourceRole`, wrong `resourceGroupId`) ->
 * fleet.ts:1146 `version-drift` ("has drifted ownership or artifact
 * metadata"), fired ONCE PER WORKER (two `version-drift` findings). The
 * state worker's live entry ALSO carries the wrong database/binding count
 * -> fleet.ts:1224 `binding-drift` ("trusted state Worker ... has drifted
 * database, Durable Object, or egress bindings"). The egress worker's live
 * entry has no `plainTextBindings` at all, and since
 * `options.inventory.hostRoutingKvId` is never set in this world,
 * fleet.ts:1245's `!options.inventory.hostRoutingKvId` disjunct alone
 * guarantees fleet.ts:1256 `binding-drift` ("trusted egress Worker ... has
 * drifted policy or attribution bindings") ADDITIONALLY. The record's OWN
 * worker inspects clean; only the declared `platformResources.stateWorker`
 * and `platformResources.egressProxy` (separate live deployment entries in
 * `inventory.deployments`) carry the drift.
 *
 * This record ALSO carries the world's only multi-namespace-id story
 * (§8.6): a second `durableObjectBindings` entry reuses
 * `SHARED_EXPECTED_NAMESPACE` (already claimed by `namespaceDupA`/`namespaceDupB`
 * above), so this record's OWN pass through the fleet.ts:762 inner loop
 * lands the namespace's THIRD claimant, landing the world's second
 * `duplicate-namespace` finding; a populated
 * `platformResources.stateWorker.namespaceIds` adds a namespace id present
 * nowhere in the inventory, landing a `missing-namespace` finding in the
 * same inner loop. The two land on different fleet.ts:762 arms within one
 * record's iteration, proving the loop actually iterates more than once.
 */
const PLATFORM_STATE_SCRIPT_NAME = 'platform-drift-state-worker';
const PLATFORM_EGRESS_SCRIPT_NAME = 'platform-drift-egress-worker';
const PLATFORM_STATE_MISSING_NAMESPACE = 'ns-platformdrift-missing';
const platformDrift = baseRecord('platformdrift', {
  durableObjectBindings: [
    { name: 'RUNNER', className: 'Runner', namespaceId: 'ns-platformdrift' },
    {
      name: 'STATE_MIRROR',
      className: 'Runner',
      namespaceId: SHARED_EXPECTED_NAMESPACE,
    },
  ],
  platformResources: {
    maintenanceCapabilityPublicKey: 'maintenance-capability-public-key-fixture',
    stateWorker: {
      scriptName: PLATFORM_STATE_SCRIPT_NAME,
      artifactVersion: 'state-v1',
      artifactDigest: 'b'.repeat(64),
      durableObjectBindings: [],
      namespaceIds: [PLATFORM_STATE_MISSING_NAMESPACE],
    },
    egressProxy: {
      scriptName: PLATFORM_EGRESS_SCRIPT_NAME,
      artifactVersion: 'egress-v1',
      artifactDigest: 'c'.repeat(64),
      policyId: 'policy-platformdrift',
      policyHosts: ['api.example.test'],
      policyDigest: 'd'.repeat(64),
    },
  },
});

/**
 * `spec.authoredBy` is `platform` with an `egressProxyService` declared, so
 * fleet.ts:1071-1073 expects one `EGRESS_PROXY` service binding on the live
 * deployment; the clean inventory entry never sets `serviceBindings`
 * (defaults to none) -> fleet.ts:1091 `binding-drift` ("drifted trusted
 * channel bindings").
 */
const CHANNEL_EGRESS_SERVICE_NAME = 'channel-egress-worker';
const channelDrift = baseRecord('channeldrift');

/** `backend.inspect()` returns `undefined` -> fleet.ts:1282 `missing-deployment` ("script is absent"). */
const inspectAbsent = baseRecord('inspectabsent');

/**
 * Unarmed live maintenance drives the full re-arm sequence to a successful
 * commit -> fleet.ts:1353 `maintenance-stale`, and the op log's
 * `withDeploymentLease`/`get`/`put`/`assertOwned:<key>`/`ensureMaintenance`
 * sequence (fleet.ts:1360-1391). No prior `invocationAuthority`, so
 * `commitInvocationAuthority` performs the `put`.
 */
const maintStale = baseRecord('maintstale');

/**
 * Unarmed live maintenance triggers the same re-arm sequence, but
 * `backend.ensureMaintenance()` throws -> fleet.ts:1353 `maintenance-stale`
 * THEN fleet.ts:1393 `audit-error` ("maintenance re-arm failed:").
 */
const rearmFail = baseRecord('rearmfail');

/**
 * Phase is not `ready` and `updatedAt` is stale -> fleet.ts:887
 * `incomplete-provisioning`. Not a `CLEANLY_INVENTORIED_RECORDS` member, so
 * its own script and expected namespace are absent from inventory too ->
 * fleet.ts:672 `missing-deployment` and fleet.ts:776 `missing-namespace`
 * also fire, as accepted collateral of that omission (this record's story
 * is incomplete-provisioning, not inventory cleanliness).
 */
const staleNotReady = baseRecord('stalenotready', {
  phase: 'worker-deployed',
  updatedAt: STALE_UPDATED_AT,
});

/**
 * A workers-for-platforms record whose `pendingRelease` AND `activeRelease`
 * live entries BOTH mismatch their persisted identity — the pending live
 * entry sets no `desiredSpecDigest` at all; the active live entry mismatches
 * `artifactVersion` — so fleet.ts:909 `version-drift` fires TWICE, once per
 * release. Its `pendingRelease` carries no `topology` -> fleet.ts:928
 * `audit-error` ("has no durable binding topology"), fired ONCE (pending
 * only; `activeRelease` has a topology, so it falls to the fleet.ts:934
 * comparison instead). The active release's live entry ALSO mismatches
 * `databaseIds` -> fleet.ts:920 `database-mismatch`, and mismatches Durable
 * Object topology -> fleet.ts:957 `binding-drift` (both fired ONCE, active
 * only — the pending release's own `databaseIds` matches, and its missing
 * topology short-circuits it out of the fleet.ts:934 comparison). Phase
 * `worker-deployed` keeps this record out of the ready-only per-record
 * checks (fleet.ts:965 gate).
 */
const WFP_ACTIVE_PHYSICAL_NAME = 'wfp-release-active';
const WFP_PENDING_PHYSICAL_NAME = 'wfp-release-pending';
const wfpRelease = baseRecord('wfprelease', {
  backend: 'workers-for-platforms',
  scriptName: 'wfp-release-worker',
  phase: 'worker-deployed',
  durableObjectBindings: [],
  activeRelease: {
    physicalScriptName: WFP_ACTIVE_PHYSICAL_NAME,
    specDigest: SPEC_DIGEST,
    artifactVersion: 'release-v1',
    releaseSchemaVersion: 1,
    topology: {
      durableObjectBindings: [
        {
          name: 'RUNNER',
          className: 'Runner',
          namespaceId: 'ns-wfp-active-expected',
        },
      ],
      serviceBindings: [],
      queueProducerBindings: [],
      secretNames: [],
    },
  },
  pendingRelease: {
    physicalScriptName: WFP_PENDING_PHYSICAL_NAME,
    specDigest: SPEC_DIGEST,
    artifactVersion: 'release-v2-pending',
    releaseSchemaVersion: 1,
    // No `topology`: fleet.ts:927-933's `!release.topology` arm.
  },
});

const AUDIT_WORLD_RECORDS: readonly FleetRecord[] = [
  control,
  liveDup,
  recordDupA,
  recordDupB,
  namespaceDupA,
  namespaceDupB,
  missingNamespace,
  bucketDupA,
  bucketDupB,
  bucketMissing,
  bucketDrift,
  missingDeploy,
  dbMismatch,
  bindingMismatch,
  routeBroken,
  routeDup,
  platformDrift,
  channelDrift,
  inspectAbsent,
  maintStale,
  rearmFail,
  staleNotReady,
  wfpRelease,
];

// ---------------------------------------------------------------------------
// Inventory: derived cleanly from the records above, then mutated per story.
// ---------------------------------------------------------------------------

/**
 * Records whose own worker/database/route inventory entries stay clean
 * (matching exactly), so their ONLY drift comes from the deliberate mutation
 * their story adds elsewhere (an extra live entry, a missing entry, an R2
 * fixture, or a `platformResources` mismatch). This list ALSO governs the
 * namespace axis below: a member's own `durableObjectBindings` namespace ids
 * are folded into `fleetAuditWorldInventory()`'s `namespaceIds` (via the
 * flatMap in `fleetAuditWorldInventory`, minus `missingNamespace` — see its
 * exclusion there), so only a member's expected namespace stays "clean";
 * two further ids are whitelisted explicitly below.
 *
 * The seven non-members (`recordDupB`, `missingDeploy`, `dbMismatch`,
 * `bindingMismatch`, `routeBroken`, `staleNotReady`, `wfpRelease`) each
 * leak collateral on the axis their story doesn't isolate. Six of them
 * (all but `wfpRelease`, whose one expected namespace id is explicitly
 * whitelisted below) ALSO leak a `missing-namespace` finding for their own
 * expected namespace id. That collateral is accepted, not isolated: unlike
 * the database/route axis, where `dbMismatch`/`bindingMismatch`/
 * `routeBroken` each get an explicit `databaseIds.push`/`routes.push` below
 * to neutralize the OTHER axes for their specific story, no equivalent
 * per-record `namespaceIds.push` exists to neutralize this one.
 */
const CLEANLY_INVENTORIED_RECORDS: readonly FleetRecord[] = [
  control,
  liveDup,
  recordDupA,
  namespaceDupA,
  namespaceDupB,
  missingNamespace,
  bucketDupA,
  bucketDupB,
  bucketMissing,
  bucketDrift,
  routeDup,
  platformDrift,
  channelDrift,
  inspectAbsent,
  maintStale,
  rearmFail,
];

function fleetAuditWorldInventory(): FleetResourceInventory {
  // `scriptRegistrations` models a workers-for-platforms dispatch
  // registration: fleet.ts:618-634's orphan check always keys it as
  // `workers-for-platforms:<scriptName>`, regardless of a registration's own
  // fields, so a plain-worker record must never get one — only `wfpRelease`
  // (backend `workers-for-platforms`) and the deliberate fleet.ts:629 ghost
  // below need entries here.
  const scriptRegistrations: FleetResourceInventory['scriptRegistrations'][number][] =
    [];
  const deployments = CLEANLY_INVENTORIED_RECORDS.map((record) =>
    cleanInventoryDeployment(record),
  );
  const databaseIds = CLEANLY_INVENTORIED_RECORDS.map(
    (record) => record.databaseId,
  );
  const namespaceIds = [
    ...new Set([
      // `missingNamespace` is excluded here (but stays a CLEANLY_INVENTORIED_RECORDS
      // member for deployments/databaseIds/routes): its whole story is that
      // `MISSING_EXPECTED_NAMESPACE` is absent from fleet inventory
      // (fleet.ts:776), so folding its own expected namespace id into this
      // derivation — the opposite of its design — would silently launder it
      // into "present" and defeat the story.
      ...CLEANLY_INVENTORIED_RECORDS.filter(
        (record) => record !== missingNamespace,
      ).flatMap((record) =>
        record.durableObjectBindings.map((binding) => binding.namespaceId),
      ),
      SHARED_EXPECTED_NAMESPACE,
      'ns-wfp-active-expected',
    ]),
  ];
  const routes = CLEANLY_INVENTORIED_RECORDS.map((record) =>
    cleanRoute(record),
  );

  // fleet.ts:629 — a registered script with no live fleet owner at all.
  scriptRegistrations.push({
    scriptName: 'ghost-registered-script',
    tenantTag: 'ghost-registration',
    environment: ENVIRONMENT,
    databaseId: 'db-ghost-registration',
    routeHostname: 'ghost-registration.example.test',
  });

  // fleet.ts:647 — an unregistered live script with no owning record.
  deployments.push(
    bareDeployment({
      scriptName: 'ghost-live-script',
      tenantTag: 'ghost-live',
      databaseIds: ['db-ghost-live'],
      routeHostnames: ['ghost-live.example.test'],
      artifactVersion: 'v1',
    }),
  );

  // fleet.ts:680 — a second live entry answering `liveDup`'s expected key.
  // `routeHostnames: []` keeps this second entry from ALSO satisfying
  // fleet.ts:981-989's per-record route-ownership count, which only wants
  // exactly one owning live entry; two identical route claims would trip
  // fleet.ts:996 too.
  deployments.push(
    cleanInventoryDeployment(liveDup, {
      tenantTag: 'livedup-ghost-owner',
      routeHostnames: [],
    }),
  );

  // fleet.ts:871 — the second `recordDupB` claimant of `shared-record-script`
  // is deliberately NOT added here: `recordDupA`'s clean entry is the only live
  // deployment under that script name, which is what makes `recordDupB`'s own
  // `recordsByScript` lookup see two RECORDS behind one live entry.

  // fleet.ts:697 — an unregistered database id.
  databaseIds.push('db-orphan-ghost');

  // fleet.ts:731 — a route with no owning record.
  routes.push({
    backend: 'plain-worker',
    hostname: 'orphan-route.example.test',
    scriptName: 'nonexistent-script',
    tenantTag: 'orphan-route-owner',
    environment: ENVIRONMENT,
  });

  // fleet.ts:753 — an unregistered namespace id.
  namespaceIds.push('ns-orphan-ghost');

  // fleet.ts:1021 — a second route sharing `routeDup`'s hostname.
  routes.push(cleanRoute(routeDup, { scriptName: 'route-dup-ghost-script' }));

  const r2Buckets: NonNullable<FleetResourceInventory['r2Buckets']> = [
    // fleet.ts:811's second claimant (`bucketDupB`) is deliberately NOT added
    // as its own live bucket: the single `shared-bucket` entry below is
    // what both records compete over.
    {
      bucketName: 'shared-bucket',
      jurisdiction: 'default',
      creationDate: SHARED_BUCKET_CREATION_DATE,
    },
    // fleet.ts:854 — present, but under a different jurisdiction/creation
    // date than `bucketDrift`'s persisted claim.
    {
      bucketName: 'bucket-drift',
      jurisdiction: 'eu',
      creationDate: '2026-03-01T00:00:00.000Z',
    },
    // fleet.ts:833 — an unclaimed live bucket.
    {
      bucketName: 'bucket-orphan-ghost',
      jurisdiction: 'default',
      creationDate: '2026-01-01T00:00:00.000Z',
    },
  ];

  // fleet.ts:974 — the live deployment's databaseIds mismatch the record.
  // `dbMismatch.databaseId` itself is added to the top-level `databaseIds`
  // list so fleet.ts:970-972's THIRD conjunct
  // (`!options.inventory.databaseIds.includes(record.databaseId)`) does not
  // ALSO fire on its own, and a clean route entry keeps fleet.ts:996/:1036
  // from firing alongside the deliberate database mismatch.
  databaseIds.push(dbMismatch.databaseId);
  deployments.push(
    cleanInventoryDeployment(dbMismatch, {
      databaseIds: ['db-dbmismatch-wrong'],
    }),
  );
  routes.push(cleanRoute(dbMismatch));

  // fleet.ts:1012 — the live deployment's Durable Object bindings mismatch.
  // Same THIRD-conjunct and route-cleanliness reasoning as `dbMismatch`.
  databaseIds.push(bindingMismatch.databaseId);
  deployments.push(
    cleanInventoryDeployment(bindingMismatch, {
      durableObjectBindings: [
        {
          name: 'RUNNER',
          className: 'Runner',
          namespaceId: 'ns-bindingmismatch-wrong',
        },
      ],
    }),
  );
  // Unlike the two pushes above, this one ADDS orphan-namespace collateral
  // (`findings[10]`) rather than suppressing another axis: `ns-bindingmismatch-wrong`
  // is nobody's expected namespace, so it lands its own finding instead of
  // neutralizing one for `bindingMismatch`.
  namespaceIds.push('ns-bindingmismatch-wrong');
  routes.push(cleanRoute(bindingMismatch));

  // fleet.ts:996/:1036 — `routeBroken` gets a live deployment (so its
  // registry/provider-inventory presence checks pass cleanly) but NO
  // route in either `routeHostnames` or `inventory.routes`. Its own
  // `databaseId` is still added to the top-level list so fleet.ts:970-972's
  // database check does not ALSO fire alongside the deliberate route drift.
  databaseIds.push(routeBroken.databaseId);
  deployments.push(
    cleanInventoryDeployment(routeBroken, { routeHostnames: [] }),
  );

  // fleet.ts:1146/:1224 — the one live deployment behind `platformDrift`'s
  // declared `stateWorker`, with wrong ownership metadata (no
  // `resourceRole`, wrong `resourceGroupId`/`artifactVersion`) and an empty
  // `databaseIds` (fleet.ts:1168 expects exactly one matching entry).
  deployments.push(
    bareDeployment({
      scriptName: PLATFORM_STATE_SCRIPT_NAME,
      tenantTag: platformDrift.tenantTag,
      resourceGroupId: 'wrong-resource-group',
      artifactVersion: 'state-v0-wrong',
    }),
  );
  // No `scriptRegistrations` entry for the state-worker key: it is
  // `plain-worker`-backed, so fleet.ts:659-666's `registered` check is
  // unconditionally true for it regardless (`expected.backend !==
  // 'workers-for-platforms'`), and — as fleet.ts:629's ghost registration
  // above demonstrates — ANY `scriptRegistrations` entry is read as a
  // workers-for-platforms dispatch registration by fleet.ts:618-634's own
  // orphan check, so adding one here would wrongly orphan this script.

  // fleet.ts:1256 — the one live deployment behind `platformDrift`'s
  // declared `egressProxy`. `options.inventory.hostRoutingKvId` is never
  // set in this world, so fleet.ts:1245's `!options.inventory.hostRoutingKvId`
  // disjunct alone guarantees the drift regardless of every other field.
  deployments.push(
    bareDeployment({
      scriptName: PLATFORM_EGRESS_SCRIPT_NAME,
      tenantTag: platformDrift.tenantTag,
      resourceGroupId: 'wrong-resource-group',
      artifactVersion: 'egress-v0-wrong',
    }),
  );

  // fleet.ts:1091 — `channelDrift` is already in `CLEANLY_INVENTORIED_RECORDS`,
  // which gives it its ONE clean live deployment entry (`serviceBindings`
  // deliberately left unset there — it is what fleet.ts:1085-1090 compares
  // against the channel service its `platform`-authored spec expects); no
  // second entry is added here.

  // fleet.ts:909/:920/:928/:957 — the WfP release-snapshot story. Both
  // physical release script names get exactly one live deployment and one
  // registration (so the pre-per-record loop sees them as present and
  // registered, never orphaned or duplicated); the active release's live
  // entry deliberately mismatches artifact version, database id, and
  // Durable Object topology against `wfpRelease.activeRelease`.
  scriptRegistrations.push(
    {
      scriptName: WFP_ACTIVE_PHYSICAL_NAME,
      tenantTag: wfpRelease.tenantTag,
      environment: ENVIRONMENT,
      databaseId: wfpRelease.databaseId,
      routeHostname: wfpRelease.routeHostname,
    },
    {
      scriptName: WFP_PENDING_PHYSICAL_NAME,
      tenantTag: wfpRelease.tenantTag,
      environment: ENVIRONMENT,
      databaseId: wfpRelease.databaseId,
      routeHostname: wfpRelease.routeHostname,
    },
  );
  deployments.push(
    bareDeployment({
      backend: 'workers-for-platforms',
      scriptName: WFP_ACTIVE_PHYSICAL_NAME,
      tenantTag: wfpRelease.tenantTag,
      databaseIds: ['db-wfp-active-wrong'],
      durableObjectBindings: [
        {
          name: 'RUNNER',
          className: 'Runner',
          namespaceId: 'ns-wfp-active-live-wrong',
        },
      ],
      artifactVersion: 'release-v1-wrong-live',
    }),
    bareDeployment({
      backend: 'workers-for-platforms',
      scriptName: WFP_PENDING_PHYSICAL_NAME,
      tenantTag: wfpRelease.tenantTag,
      databaseIds: [wfpRelease.databaseId],
      artifactVersion: 'release-v2-pending',
    }),
  );

  return {
    // fleet.ts:574 — the seed: every provider-supplied finding is copied
    // verbatim into the result, ahead of anything the audit itself adds.
    findings: [
      {
        tenantTag: 'seed-provider',
        environment: ENVIRONMENT,
        kind: 'stale-route',
        detail: 'seeded provider finding carried through the golden baseline',
      },
    ],
    scriptRegistrations,
    deployments,
    databaseIds,
    namespaceIds,
    r2Buckets,
    routes,
  };
}

// ---------------------------------------------------------------------------
// Recording collaborators: store, backend, and resolver wrappers.
// ---------------------------------------------------------------------------

/**
 * The frozen vocabulary of collaborator interactions this world's op log can
 * record. `list`/`renew`/`delete` are defensive: the recording `store` and
 * `lease` still push them if called, but the pre-decomposition
 * `auditFleetDrift` never calls `store.list()`, `lease.renew()`, or
 * `lease.delete()`, so they never appear in the recorded baseline.
 */
export type AuditOpLogEntry =
  | 'withDeploymentLease'
  | 'get'
  | 'put'
  | 'inspect'
  | 'ensureMaintenance'
  | 'list'
  | 'renew'
  | 'delete'
  | `resolver:${string}`
  | `assertOwned:${string}`;

class RecordingFleetStore implements FleetStateStore {
  private readonly records = new Map<string, FleetRecord>();

  constructor(
    records: readonly FleetRecord[],
    private readonly ops: AuditOpLogEntry[],
    private readonly fenceViolations: string[],
  ) {
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
    return operation({
      tenantTag,
      environment,
      mutationLeaseTtlMs: 900_000,
      assertOwned: async () => {
        this.ops.push(`assertOwned:${key}`);
      },
      renew: async () => {
        this.ops.push('renew');
      },
      put: async (record) => {
        // Pins §6.1's authority clock wiring: `commitInvocationAuthority`
        // must stamp `updatedAt` from the audited authority clock
        // (`options.now`), not a bare `Date.now()`. The audit re-wired to
        // the latter must fail loudly, not pass silently through to a
        // baseline that would then encode the wrong clock as "correct". The
        // throw below surfaces as an `audit-error` finding inside
        // `auditFleetDrift` (fleet.ts:1392-1399 swallows it — fail-soft by
        // design), same as the credential-delivery pins in
        // `RecordingBackend.ensureMaintenance`/`inspect` below, which record
        // into `fenceViolations` too; `runFleetAuditBaseline`'s post-run
        // throw — a fresh Error aggregating the recorded fence-violation
        // messages — is what makes the recorder and golden test fail loudly
        // instead.
        if (record.updatedAt !== new Date(AUDIT_NOW).toISOString()) {
          const message = `re-arm put payload updatedAt '${record.updatedAt}' does not match the authority clock`;
          this.fenceViolations.push(message);
          throw new Error(message);
        }
        this.ops.push('put');
        this.records.set(key, record);
      },
      delete: async () => {
        this.ops.push('delete');
        this.records.delete(key);
      },
      completeCleanup: async (): Promise<CleanupTerminalReceipt> => {
        throw new Error('unused');
      },
      deleteReleasingClaims: async (): Promise<void> => {
        throw new Error('unused');
      },
    });
  }

  async get(
    tenantTag: string,
    environment: string,
  ): Promise<FleetRecord | undefined> {
    this.ops.push('get');
    return this.records.get(`${tenantTag}:${environment}`);
  }

  async list(): Promise<readonly FleetRecord[]> {
    this.ops.push('list');
    return [...this.records.values()];
  }

  async readCleanupReceipt(): Promise<CleanupTerminalReceipt | undefined> {
    throw new Error('unused');
  }

  async pruneCleanupReceipts(): Promise<Readonly<{ deleted: number }>> {
    throw new Error('unused');
  }
}

class RecordingBackend implements ProvisioningBackend {
  readonly kind: ProvisioningBackendKind = 'plain-worker';

  constructor(
    private readonly ops: AuditOpLogEntry[],
    private readonly liveByTenant: ReadonlyMap<
      string,
      LiveDeployment | undefined
    >,
    private readonly ensureMaintenanceThrowTenants: ReadonlySet<string>,
    private readonly fenceViolations: string[],
  ) {}

  async findDatabase(): Promise<DatabaseReference | undefined> {
    throw new Error('unused');
  }

  async getDatabase(): Promise<DatabaseReference | undefined> {
    throw new Error('unused');
  }

  async ensureDatabase(): Promise<DatabaseReference> {
    throw new Error('unused');
  }

  async seedDeploymentIdentity(
    _database: DatabaseReference,
    _tenantTag: string,
    _fence: ExternalMutationFence,
    _options: SeedDeploymentIdentityOptions,
  ): Promise<void> {
    throw new Error('unused');
  }

  async readDeploymentIdentity(): Promise<string | undefined> {
    throw new Error('unused');
  }

  async applyMigrations(): Promise<void> {
    throw new Error('unused');
  }

  async deployWorker(): Promise<{
    artifactVersion: string;
    created: boolean;
  }> {
    throw new Error('unused');
  }

  async promoteWorker(): Promise<void> {
    throw new Error('unused');
  }

  // Argument-delivery pins: record A's credentials must never reach record
  // B's calls.
  async ensureMaintenance(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
    lease: FleetStateLease,
  ): Promise<MaintenanceHealth> {
    this.ops.push('ensureMaintenance');
    if (maintenanceAdminSecret !== `maintenance-secret-${spec.tenantTag}`) {
      const message = `ensureMaintenance received the wrong maintenance secret for '${spec.tenantTag}'`;
      this.fenceViolations.push(message);
      throw new Error(message);
    }
    if (lease.tenantTag !== spec.tenantTag) {
      const message = `ensureMaintenance received a lease for '${lease.tenantTag}' while auditing '${spec.tenantTag}'`;
      this.fenceViolations.push(message);
      throw new Error(message);
    }
    if (this.ensureMaintenanceThrowTenants.has(spec.tenantTag)) {
      throw new Error('maintenance re-arm failed');
    }
    return HEALTHY_MAINTENANCE;
  }

  async inspect(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
  ): Promise<LiveDeployment | undefined> {
    this.ops.push('inspect');
    if (maintenanceAdminSecret !== `maintenance-secret-${spec.tenantTag}`) {
      const message = `inspect received the wrong maintenance secret for '${spec.tenantTag}'`;
      this.fenceViolations.push(message);
      throw new Error(message);
    }
    return this.liveByTenant.get(spec.tenantTag);
  }

  async attestActiveRoute(): Promise<ActiveRouteAttestation> {
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

  async exportDatabase(): Promise<DatabaseExport> {
    throw new Error('unused');
  }

  async deleteDatabase(): Promise<void> {
    throw new Error('unused');
  }
}

/**
 * Every record reached through `backend.inspect()` gets a clean, matching
 * live deployment EXCEPT the records whose story is specifically about
 * inspect-time drift (`inspectAbsent` returns `undefined`; `maintStale` and
 * `rearmFail` report unarmed maintenance). `missingDeploy`, `recordDupB`,
 * `staleNotReady`, and `wfpRelease` are absent from this map entirely:
 * `missingDeploy` never reaches `inspect` at all, exiting at fleet.ts:966's
 * `!inventoryDeployment` check, while the other three exit earlier still, at
 * fleet.ts:965's `phase !== 'ready'` gate.
 */
function inspectResultsByTenant(): Map<string, LiveDeployment | undefined> {
  const live = new Map<string, LiveDeployment | undefined>();
  for (const record of [
    control,
    liveDup,
    recordDupA,
    namespaceDupA,
    namespaceDupB,
    missingNamespace,
    bucketDupA,
    bucketDupB,
    bucketMissing,
    bucketDrift,
    dbMismatch,
    bindingMismatch,
    routeBroken,
    routeDup,
    platformDrift,
    channelDrift,
  ]) {
    live.set(record.tenantTag, cleanLiveDeployment(record));
  }
  live.set(inspectAbsent.tenantTag, undefined);
  live.set(
    maintStale.tenantTag,
    cleanLiveDeployment(maintStale, { maintenance: UNARMED_MAINTENANCE }),
  );
  live.set(
    rearmFail.tenantTag,
    cleanLiveDeployment(rearmFail, { maintenance: UNARMED_MAINTENANCE }),
  );
  return live;
}

/** Runs the current audit against the frozen world through recording collaborators. */
export async function runFleetAuditBaseline(): Promise<{
  readonly findings: readonly DriftFinding[];
  readonly ops: readonly AuditOpLogEntry[];
}> {
  const ops: AuditOpLogEntry[] = [];
  // One array feeds the store's clock fence, the backend's credential pins, and the post-run throw.
  const fenceViolations: string[] = [];
  const store = new RecordingFleetStore(
    AUDIT_WORLD_RECORDS,
    ops,
    fenceViolations,
  );
  const backend = new RecordingBackend(
    ops,
    inspectResultsByTenant(),
    new Set([rearmFail.tenantTag]),
    fenceViolations,
  );
  const specByTenant = new Map<string, DeploymentSpec>(
    AUDIT_WORLD_RECORDS.map((record) => [
      record.tenantTag,
      specForRecord(record),
    ]),
  );
  // fleet.ts:1091's channel-binding-drift check only evaluates a non-empty
  // expectation for a `platform`-authored spec that declares
  // `egressProxyService`; every other record stays `authoredBy: 'external'`.
  specByTenant.set(
    channelDrift.tenantTag,
    specForRecord(channelDrift, {
      authoredBy: 'platform',
      egressProxyService: CHANNEL_EGRESS_SERVICE_NAME,
    }),
  );

  const findings = await auditFleetDrift({
    store,
    records: AUDIT_WORLD_RECORDS,
    inventory: fleetAuditWorldInventory(),
    backendFor: (_record) => {
      ops.push('resolver:backendFor');
      return backend;
    },
    specFor: (record) => {
      ops.push('resolver:specFor');
      const spec = specByTenant.get(record.tenantTag);
      if (!spec) {
        throw new Error(`no spec fixture for '${record.tenantTag}'`);
      }
      return spec;
    },
    maintenanceSecretFor: (record) => {
      ops.push('resolver:maintenanceSecretFor');
      return `maintenance-secret-${record.tenantTag}`;
    },
    staleAfterMs: AUDIT_STALE_AFTER_MS,
    now: AUDIT_NOW,
  });

  if (fenceViolations.length > 0) {
    throw new Error(`fence violated: ${fenceViolations.join('; ')}`);
  }

  return { findings, ops };
}
