// SPDX-License-Identifier: Apache-2.0

/**
 * Hand-authored deterministic worlds for the `migrateFleet` golden baselines.
 * `scripts/record-migration-baseline.mjs` and
 * `test/fleet-migration-golden.test.ts` both import this file; the recorder
 * NEVER writes it, so the recorded literals can never rewrite their own input.
 *
 * The worlds freeze the SHIPPED behavior of `migrateFleet` in `src/fleet.ts`
 * before its internals are decomposed into a bounded frozen-plan executor
 * (R4-C.2), so the decomposition can be proven behavior-equivalent. Each world
 * records the value `migrateFleet` produced AND the exact sequence of calls it
 * made onto its `store`, `backendFor`, `specFor`, `secretsFor`, and
 * `settlementFor` collaborators (the "op log").
 *
 * TWO worlds, because the drain has two observable contracts:
 *   - the SUCCESS world (`runFleetMigrationSuccessBaseline`) drives four
 *     records — one immutable-external full migration, one non-external
 *     platform-authored full migration over several D1 versions, one
 *     platform-only change, and one ready steady-state reconcile — and freezes
 *     the returned `readonly FleetRecord[]` beside the op log.
 *   - the STOP world (`runFleetMigrationStopBaseline`) drives three records in
 *     the frozen scheduler order and freezes FIRST-ERROR STOP PARITY: the
 *     first record completes, the second is refused in the admit preamble, and
 *     the third contributes no op at all. `migrateFleet` rejects, so the world
 *     freezes the refusal message beside the op log.
 *
 * OP-LOG VOCABULARY (`MigrationOpLogEntry`, below) is derived from the calls
 * the migration BODY actually makes, not from the collaborator port's member
 * list. Four token compositions carry a key:
 *   - `resolver:<kind>:<tenantTag>:<environment>` — the resolver invocation,
 *     keyed by the record it resolved for.
 *   - `put:<phase-or-subphase>` — the value of the field THAT put advances:
 *     the record `phase` when it moves (including the admission put, which
 *     moves `phase` AND the external `migrationIntent.subphase`), otherwise
 *     the advanced `migrationIntent.subphase`, otherwise the record's current
 *     `phase` for a put that advances neither.
 *   - `applyMigrations:<versions-or-verify>` — `verify` for the zero-pending
 *     ledger-verification call, which passes `spec.migrations` itself, and the
 *     sliced array's length for each per-version call. The two are told apart
 *     by REFERENCE identity against the spec object `specFor` returned, never
 *     by comparing contents: the last per-version slice is content-equal to
 *     `spec.migrations`.
 *   - `settle:<settlementKey>` — the settlement the host was handed, keyed by
 *     the key the promotion settled under.
 *
 * SEAMS. Every collaborator member these two worlds never reach THROWS, so a
 * bounded decomposition that starts calling one fails loudly instead of
 * silently no-opping. The four FEATURE-DETECTED optional backend members —
 * `releaseScriptName`, `ensurePlatformResources`, `deleteRetainedRelease`, and
 * `describeExternalPlatformTarget` — are REAL on the immutable-external
 * backend, because a present-but-throwing member is observably different from
 * an absent one at a feature-detection site; the non-external backend declares
 * none of them, which is what makes its records take the non-external path.
 *
 * CLOCK FENCE. `migrateFleet` stamps every write it performs from
 * `options.clock`, so the recording lease refuses any put whose `updatedAt` is
 * not the frozen instant. A mis-wired clock therefore fails the recorder and
 * the golden test loudly rather than writing a plausible baseline: the success
 * runner lets the violation propagate, and the stop runner inspects the
 * violations BEFORE it reports the caught refusal, so a clock fault can never
 * render as a plausible stop.
 *
 * WHAT IS DELIBERATELY ABSENT. Neither world holds a finalized-ordinary-plane
 * external record, so the finalized-state provider is never resolved and the
 * state-reconcile route is never entered: `describeFinalizedState`,
 * `describeFinalizedBridgeTarget`, `assertFinalizedState`,
 * `ensureFinalizedState`, `commitFinalizedOwnership`,
 * `resolver:finalizedStateProviderFor:<key>`, and the reconcile's
 * `put:upload-authorized`/`put:uploaded` are vocabulary-only here. Their drain
 * behavior stays pinned by `test/fleet.test.ts:3703`.
 */

import { migrateFleet } from '../../src/fleet.js';
import {
  canonicalDeploymentEgressPolicy,
  externalEgressProxyScriptName,
  externalPlatformResourceGroupId,
  externalStateScriptName,
} from '../../src/platform-resources.js';
import { providerBindingIdentitiesForInspection } from '../../src/provider-binding-inventory.js';
import { fleetSettlementKey } from '../../src/settlement.js';
import { deploymentSpecDigest } from '../../src/spec-digest.js';
import type {
  ActiveRouteAttestation,
  ApplicationBindingTopology,
  D1Migration,
  DatabaseExport,
  DatabaseReference,
  DeploymentEgressPolicy,
  DeploymentSecrets,
  DeploymentSpec,
  ExternalMutationFence,
  ExternalPlatformResources,
  ExternalPlatformTargetDescription,
  ExternalReleaseSnapshot,
  FleetRecord,
  FleetSettlementContext,
  FleetSettlementHost,
  FleetStateLease,
  FleetStateStore,
  LiveDeployment,
  MaintenanceHealth,
  PromotionGuard,
  ProvisioningBackend,
  ProvisioningBackendKind,
} from '../../src/types.js';
import { externalReleaseScriptName } from '../../src/workers-for-platforms-backend.js';

const ENVIRONMENT = 'production';

/**
 * Frozen clock: the only time source `migrateFleet` reads (`options.clock`,
 * src/fleet.ts:2018), so every `updatedAt` it writes is this instant.
 */
const MIGRATION_NOW = Date.parse('2026-06-01T00:00:00.000Z');
const FROZEN_UPDATED_AT = new Date(MIGRATION_NOW).toISOString();
const MIGRATION_CLOCK = () => MIGRATION_NOW;

/** Every seeded record's pre-migration stamp, distinct from the frozen one. */
const ORIGIN_UPDATED_AT = '2026-05-01T00:00:00.000Z';

const MAINTENANCE_PUBLIC_KEY =
  '{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","kid":"fleet-maintenance-v1","x":"Lhp1XFeTJJx8FLOCKpn4nkO-tWuZZxXX8ziw0LEvUZo"}';

/** 64-hex platform-target digests: `describeExternalPlatformTarget` validates their shape. */
const STATE_ARTIFACT_DIGEST = 'a'.repeat(64);
const MOVED_STATE_ARTIFACT_DIGEST = 'd'.repeat(64);
const EGRESS_ARTIFACT_DIGEST = 'b'.repeat(64);
const STATE_DURABLE_OBJECT_HISTORY_DIGEST = 'c'.repeat(64);
const PLATFORM_ONLY_PRIOR_D1_HISTORY_DIGEST = 'f'.repeat(64);

const HEALTHY_MAINTENANCE: MaintenanceHealth = {
  armed: true,
  nextAlarmAt: MIGRATION_NOW + 60_000,
  lastSweepAt: MIGRATION_NOW - 60_000,
  lastPurgeAt: MIGRATION_NOW - 60_000,
};

const UNARMED_MAINTENANCE: MaintenanceHealth = {
  armed: false,
  nextAlarmAt: null,
  lastSweepAt: null,
  lastPurgeAt: null,
};

const EMPTY_APPLICATION: ApplicationBindingTopology = {
  vars: [],
  secrets: [],
  r2Buckets: [],
};

const D1_MIGRATIONS: readonly D1Migration[] = [
  { version: 1, sql: 'CREATE TABLE example (id TEXT PRIMARY KEY)' },
  {
    version: 2,
    sql: 'ALTER TABLE example ADD COLUMN value TEXT',
    rollbackCompatible: true,
  },
  {
    version: 3,
    sql: 'ALTER TABLE example ADD COLUMN expanded TEXT',
    rollbackCompatible: true,
  },
];

/**
 * The op log's frozen vocabulary. Bare tokens name a call whose relative
 * position identifies it against these single-pass worlds; the four keyed
 * families carry the one field that distinguishes otherwise identical calls.
 */
export type MigrationOpLogEntry =
  | 'withDeploymentLease'
  | 'get'
  | 'assertOwned'
  | 'releaseScriptName'
  | 'getDatabase'
  | 'readDeploymentIdentity'
  | 'seedDeploymentIdentity'
  | 'ensurePlatformResources'
  | 'describeExternalPlatformTarget'
  | 'describeFinalizedState'
  | 'describeFinalizedBridgeTarget'
  | 'assertFinalizedState'
  | 'ensureFinalizedState'
  | 'commitFinalizedOwnership'
  | 'deployWorker'
  | 'promoteWorker'
  | 'ensureMaintenance'
  | 'inspect'
  | 'attestActiveRoute'
  | 'deleteRetainedRelease'
  | `put:${string}`
  | `resolver:${string}`
  | `applyMigrations:${string}`
  | `settle:${string}`;

function deploymentKey(record: {
  readonly tenantTag: string;
  readonly environment: string;
}): string {
  return `${record.tenantTag}:${record.environment}`;
}

function secretsForTenant(tenantTag: string): DeploymentSecrets {
  return {
    deploymentIdentity: `deployment-identity-secret-${tenantTag}-00000001`,
    maintenanceAdmin: `maintenance-admin-secret-${tenantTag}-00000001`,
  };
}

/**
 * The token a put carries: the value of the field THIS put advances. `phase`
 * wins where a put moves more than one (the admission put moves `phase` to
 * `migrating` AND the external intent to `planned`); a put advancing neither
 * a phase nor a subphase carries the record's current `phase`.
 */
function putToken(
  previous: FleetRecord | undefined,
  next: FleetRecord,
): string {
  if (!previous) return next.phase;
  if (next.phase !== previous.phase) return next.phase;
  const migrationSubphase = next.migrationIntent?.subphase;
  if (
    migrationSubphase !== undefined &&
    migrationSubphase !== previous.migrationIntent?.subphase
  ) {
    return migrationSubphase;
  }
  const reconcileSubphase =
    next.backendSwitchIntent?.stateReconcileIntent?.subphase;
  if (
    reconcileSubphase !== undefined &&
    reconcileSubphase !==
      previous.backendSwitchIntent?.stateReconcileIntent?.subphase
  ) {
    return reconcileSubphase;
  }
  return next.phase;
}

class RecordingFleetStore implements FleetStateStore {
  readonly #records = new Map<string, FleetRecord>();
  readonly #ops: MigrationOpLogEntry[];
  readonly #fenceViolations: string[];

  constructor(
    records: readonly FleetRecord[],
    ops: MigrationOpLogEntry[],
    fenceViolations: string[],
  ) {
    this.#ops = ops;
    this.#fenceViolations = fenceViolations;
    for (const record of records) {
      this.#records.set(deploymentKey(record), record);
    }
  }

  async withDeploymentLease<T>(
    tenantTag: string,
    environment: string,
    operation: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T> {
    this.#ops.push('withDeploymentLease');
    const key = `${tenantTag}:${environment}`;
    return operation({
      tenantTag,
      environment,
      mutationLeaseTtlMs: 900_000,
      assertOwned: async () => {
        this.#ops.push('assertOwned');
      },
      renew: async () => {
        throw new Error('unused');
      },
      put: async (record) => {
        // Clock fence: `migrateFleet` stamps every write it performs from
        // `options.clock`, so a put carrying any other instant means the
        // injected clock stopped reaching a write site. Recording the message
        // before throwing lets the stop runner see the fault even though the
        // migration's own rejection is what the world would otherwise freeze.
        if (record.updatedAt !== FROZEN_UPDATED_AT) {
          const message = `put payload updatedAt '${record.updatedAt}' for '${key}' does not match the frozen migration clock`;
          this.#fenceViolations.push(message);
          throw new Error(message);
        }
        if (deploymentKey(record) !== key) {
          const message = `put payload for '${deploymentKey(record)}' arrived under the lease for '${key}'`;
          this.#fenceViolations.push(message);
          throw new Error(message);
        }
        this.#ops.push(`put:${putToken(this.#records.get(key), record)}`);
        this.#records.set(key, record);
      },
      delete: async () => {
        throw new Error('unused');
      },
      completeCleanup: async () => {
        throw new Error('unused');
      },
      deleteReleasingClaims: async () => {
        throw new Error('unused');
      },
    });
  }

  async get(
    tenantTag: string,
    environment: string,
  ): Promise<FleetRecord | undefined> {
    this.#ops.push('get');
    return this.#records.get(`${tenantTag}:${environment}`);
  }

  async list(): Promise<readonly FleetRecord[]> {
    throw new Error('unused');
  }

  async readCleanupReceipt(): Promise<never> {
    throw new Error('unused');
  }

  async pruneCleanupReceipts(): Promise<never> {
    throw new Error('unused');
  }
}

/**
 * The non-external backend: no `immutableExternalArtifacts`, and none of the
 * four feature-detected external members, so its records take the plain path.
 */
class RecordingPlainBackend implements ProvisioningBackend {
  readonly kind: ProvisioningBackendKind = 'plain-worker';
  protected readonly ops: MigrationOpLogEntry[];
  protected readonly specsByDatabaseId: ReadonlyMap<string, DeploymentSpec>;
  protected readonly fenceViolations: string[];
  protected readonly live = new Map<string, LiveDeployment>();
  protected readonly routed = new Map<string, ActiveRouteAttestation>();

  constructor(
    ops: MigrationOpLogEntry[],
    specsByDatabaseId: ReadonlyMap<string, DeploymentSpec>,
    fenceViolations: string[],
  ) {
    this.ops = ops;
    this.specsByDatabaseId = specsByDatabaseId;
    this.fenceViolations = fenceViolations;
  }

  /** Refuses a credential that belongs to another record. */
  protected assertOwnCredential(spec: DeploymentSpec, secret: string): void {
    const expected = secretsForTenant(spec.tenantTag).maintenanceAdmin;
    if (secret !== expected) {
      const message = `maintenance credential for '${deploymentKey(spec)}' reached a call for another deployment`;
      this.fenceViolations.push(message);
      throw new Error(message);
    }
  }

  seedLive(tenantTag: string, live: LiveDeployment): void {
    this.live.set(tenantTag, live);
  }

  async findDatabase(): Promise<never> {
    throw new Error('unused');
  }

  async getDatabase(databaseId: string): Promise<DatabaseReference> {
    this.ops.push('getDatabase');
    return {
      id: databaseId,
      name: `database-${databaseId.replace(/^db-/u, '')}`,
      created: false,
    };
  }

  async ensureDatabase(): Promise<never> {
    throw new Error('unused');
  }

  async seedDeploymentIdentity(): Promise<void> {
    this.ops.push('seedDeploymentIdentity');
  }

  async readDeploymentIdentity(
    database: DatabaseReference,
  ): Promise<string | undefined> {
    this.ops.push('readDeploymentIdentity');
    return database.id.replace(/^db-/u, '');
  }

  async applyMigrations(
    database: DatabaseReference,
    migrations: readonly D1Migration[],
  ): Promise<void> {
    const spec = this.specsByDatabaseId.get(database.id);
    // Reference identity, never contents: the LAST per-version slice is
    // content-equal to the zero-pending call's `spec.migrations`.
    this.ops.push(
      migrations === spec?.migrations
        ? 'applyMigrations:verify'
        : `applyMigrations:${migrations.length}`,
    );
  }

  async deployWorker(
    spec: DeploymentSpec,
    database: DatabaseReference,
    _secrets: DeploymentSecrets,
    _platformResources: ExternalPlatformResources | undefined,
    _fence: ExternalMutationFence,
    _expectedArtifactVersion: string | undefined,
    application?: ApplicationBindingTopology,
  ): Promise<Readonly<{ artifactVersion: string; created: boolean }>> {
    this.ops.push('deployWorker');
    const artifactVersion = `v${spec.schemaVersion}`;
    this.live.set(
      spec.tenantTag,
      liveDeployment({
        tenantTag: spec.tenantTag,
        environment: spec.environment,
        scriptName: spec.scriptName,
        databaseId: database.id,
        durableObjectBindings: [],
        serviceBindings: [],
        queueProducerBindings: [],
        plainTextBindings: {},
        secretNames: [
          'DEPLOYMENT_IDENTITY_SECRET',
          ...(spec.authoredBy === 'platform'
            ? ['MAINTENANCE_ADMIN_SECRET']
            : []),
          ...(application?.secrets ?? []).map(({ name }) => name),
        ].sort(),
        artifactVersion,
        desiredSpecDigest: deploymentSpecDigest(spec),
        schemaVersion: spec.schemaVersion,
        maintenance: HEALTHY_MAINTENANCE,
      }),
    );
    return { artifactVersion, created: false };
  }

  async promoteWorker(
    spec: DeploymentSpec,
    _guard: PromotionGuard,
    _outboundPolicy: DeploymentEgressPolicy | undefined,
    _fence: ExternalMutationFence,
    expectedArtifactVersion: string | undefined,
  ): Promise<void> {
    this.ops.push('promoteWorker');
    this.routed.set(spec.tenantTag, {
      specDigest: deploymentSpecDigest(spec),
      artifactVersion:
        expectedArtifactVersion ??
        this.live.get(spec.tenantTag)?.artifactVersion ??
        `v${spec.schemaVersion}`,
      physicalScriptName: spec.scriptName,
      source: 'workers-deployments',
      observedAt: FROZEN_UPDATED_AT,
    });
  }

  async ensureMaintenance(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
  ): Promise<MaintenanceHealth> {
    this.ops.push('ensureMaintenance');
    this.assertOwnCredential(spec, maintenanceAdminSecret);
    const live = this.live.get(spec.tenantTag);
    if (live) {
      this.live.set(spec.tenantTag, {
        ...live,
        maintenance: HEALTHY_MAINTENANCE,
      });
    }
    return HEALTHY_MAINTENANCE;
  }

  async inspect(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
  ): Promise<LiveDeployment | undefined> {
    this.ops.push('inspect');
    this.assertOwnCredential(spec, maintenanceAdminSecret);
    return this.live.get(spec.tenantTag);
  }

  async attestActiveRoute(
    spec: DeploymentSpec,
  ): Promise<ActiveRouteAttestation> {
    this.ops.push('attestActiveRoute');
    const attestation = this.routed.get(spec.tenantTag);
    if (!attestation) throw new Error(`no route serves '${spec.tenantTag}'`);
    return attestation;
  }

  async removeTraffic(): Promise<never> {
    throw new Error('unused');
  }

  async assertTrafficRemoved(): Promise<never> {
    throw new Error('unused');
  }

  async revokeCredentials(): Promise<never> {
    throw new Error('unused');
  }

  async deleteWorker(): Promise<never> {
    throw new Error('unused');
  }

  async assertDatabaseDetached(): Promise<never> {
    throw new Error('unused');
  }

  async exportDatabase(): Promise<DatabaseExport> {
    throw new Error('unused');
  }

  async deleteDatabase(): Promise<never> {
    throw new Error('unused');
  }
}

/**
 * The immutable-external backend. All four feature-detected members are REAL,
 * because a present-but-throwing member is observably different from an absent
 * one everywhere `migrateFleet` feature-detects.
 */
class RecordingImmutableBackend extends RecordingPlainBackend {
  override readonly kind: ProvisioningBackendKind = 'workers-for-platforms';
  readonly immutableExternalArtifacts = true as const;
  readonly retiredScriptNames: string[] = [];
  readonly releases = new Map<string, LiveDeployment>();
  /** The release each deployment's own host route names, written by promotion. */
  readonly routedScriptNames = new Map<string, string>();
  stateArtifactDigest = STATE_ARTIFACT_DIGEST;
  policyHosts: readonly string[] = ['api.example.test'];

  releaseScriptName(spec: DeploymentSpec): string {
    this.ops.push('releaseScriptName');
    return externalReleaseScriptName(spec);
  }

  describeExternalPlatformTarget(
    spec: DeploymentSpec,
  ): ExternalPlatformTargetDescription {
    this.ops.push('describeExternalPlatformTarget');
    return this.platformTargetFor(spec);
  }

  /** The pure derivation behind `describeExternalPlatformTarget`, unrecorded. */
  platformTargetFor(spec: DeploymentSpec): ExternalPlatformTargetDescription {
    return {
      maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
      stateArtifactDigest: this.stateArtifactDigest,
      stateDurableObjectHistoryDigest: STATE_DURABLE_OBJECT_HISTORY_DIGEST,
      egressArtifactDigest: EGRESS_ARTIFACT_DIGEST,
      d1SchemaVersion: spec.schemaVersion,
      d1SchemaHistoryDigest: deploymentSpecDigest(spec),
      outboundPolicy: canonicalDeploymentEgressPolicy({
        policyId: externalPlatformResourceGroupId(spec),
        tenantTag: spec.tenantTag,
        environment: spec.environment,
        allowedHosts: this.policyHosts,
      }),
    };
  }

  platformResourcesFor(spec: DeploymentSpec): ExternalPlatformResources {
    const target = this.platformTargetFor(spec);
    return {
      maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
      stateWorker: {
        scriptName: externalStateScriptName(spec),
        artifactVersion: 'state-v1',
        artifactDigest: target.stateArtifactDigest,
        durableObjectBindings: [],
        namespaceIds: [],
      },
      egressProxy: {
        scriptName: externalEgressProxyScriptName(spec),
        artifactVersion: 'egress-v1',
        artifactDigest: EGRESS_ARTIFACT_DIGEST,
        ...target.outboundPolicy,
      },
    };
  }

  async ensurePlatformResources(spec: DeploymentSpec): Promise<
    Readonly<{
      resources: ExternalPlatformResources;
      created: Readonly<{ stateWorker: boolean; egressProxy: boolean }>;
    }>
  > {
    this.ops.push('ensurePlatformResources');
    return {
      resources: this.platformResourcesFor(spec),
      created: { stateWorker: false, egressProxy: false },
    };
  }

  override async deployWorker(
    spec: DeploymentSpec,
    database: DatabaseReference,
    _secrets: DeploymentSecrets,
    _platformResources: ExternalPlatformResources | undefined,
    _fence: ExternalMutationFence,
    _expectedArtifactVersion: string | undefined,
    application?: ApplicationBindingTopology,
  ): Promise<
    Readonly<{
      artifactVersion: string;
      created: boolean;
      physicalScriptName: string;
    }>
  > {
    this.ops.push('deployWorker');
    const physicalScriptName = externalReleaseScriptName(spec);
    const existing = this.releases.get(physicalScriptName);
    if (!existing) {
      this.releases.set(
        physicalScriptName,
        liveDeployment({
          tenantTag: spec.tenantTag,
          environment: spec.environment,
          scriptName: physicalScriptName,
          databaseId: database.id,
          durableObjectBindings: [],
          serviceBindings: [],
          queueProducerBindings: [],
          plainTextBindings: {},
          secretNames: [
            'DEPLOYMENT_IDENTITY_SECRET',
            ...(application?.secrets ?? []).map(({ name }) => name),
          ].sort(),
          artifactVersion: `etag:${physicalScriptName}`,
          desiredSpecDigest: deploymentSpecDigest(spec),
          schemaVersion: spec.schemaVersion,
          maintenance: HEALTHY_MAINTENANCE,
        }),
      );
    }
    return {
      artifactVersion: `etag:${physicalScriptName}`,
      created: !existing,
      physicalScriptName,
    };
  }

  override async promoteWorker(
    spec: DeploymentSpec,
    guard: PromotionGuard,
  ): Promise<void> {
    this.ops.push('promoteWorker');
    const physical = externalReleaseScriptName(spec);
    const routed = this.routedScriptNames.get(spec.tenantTag);
    if (
      (routed === undefined && !guard.allowUnrouted) ||
      (routed !== undefined &&
        !guard.allowedCurrentScriptNames.includes(routed))
    ) {
      throw new Error('route changed after lifecycle intent was persisted');
    }
    this.routedScriptNames.set(spec.tenantTag, physical);
  }

  override async ensureMaintenance(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
  ): Promise<MaintenanceHealth> {
    this.ops.push('ensureMaintenance');
    this.assertOwnCredential(spec, maintenanceAdminSecret);
    const physical = externalReleaseScriptName(spec);
    const release = this.releases.get(physical);
    if (release) {
      this.releases.set(physical, {
        ...release,
        maintenance: HEALTHY_MAINTENANCE,
      });
    }
    return HEALTHY_MAINTENANCE;
  }

  override async inspect(
    spec: DeploymentSpec,
    maintenanceAdminSecret: string,
  ): Promise<LiveDeployment | undefined> {
    this.ops.push('inspect');
    this.assertOwnCredential(spec, maintenanceAdminSecret);
    return this.releases.get(externalReleaseScriptName(spec));
  }

  override async attestActiveRoute(
    spec: DeploymentSpec,
  ): Promise<ActiveRouteAttestation> {
    this.ops.push('attestActiveRoute');
    const routed = this.routedScriptNames.get(spec.tenantTag);
    const release = routed ? this.releases.get(routed) : undefined;
    if (!routed || !release) {
      throw new Error(`no release serves '${spec.routeHostname}'`);
    }
    return {
      specDigest: release.desiredSpecDigest,
      artifactVersion: release.artifactVersion,
      physicalScriptName: routed,
      source: 'dispatch-route',
      observedAt: FROZEN_UPDATED_AT,
    };
  }

  async deleteRetainedRelease(
    _spec: DeploymentSpec,
    release: ExternalReleaseSnapshot,
  ): Promise<void> {
    this.ops.push('deleteRetainedRelease');
    this.retiredScriptNames.push(release.physicalScriptName);
    this.releases.delete(release.physicalScriptName);
  }
}

class RecordingSettlementHost implements FleetSettlementHost {
  readonly #ops: MigrationOpLogEntry[];

  constructor(ops: MigrationOpLogEntry[]) {
    this.#ops = ops;
  }

  async settle(context: FleetSettlementContext): Promise<void> {
    this.#ops.push(`settle:${context.settlementKey}`);
  }
}

function liveDeployment(
  live: Omit<LiveDeployment, 'providerBindingIdentities'>,
): LiveDeployment {
  return {
    ...live,
    providerBindingIdentities: providerBindingIdentitiesForInspection({
      ...live,
      databaseIds: [live.databaseId],
    }),
  };
}

function baseSpec(
  tenantTag: string,
  overrides: Partial<DeploymentSpec> = {},
): DeploymentSpec {
  const schemaVersion = overrides.schemaVersion ?? 1;
  return {
    tenantTag,
    environment: ENVIRONMENT,
    scriptName: `worker-${tenantTag}`,
    databaseName: `database-${tenantTag}`,
    compatibilityDate: '2026-05-01',
    mainModule: 'worker.js',
    modules: [{ name: 'worker.js', content: 'export default {}' }],
    authoredBy: 'external',
    schemaVersion,
    migrations: D1_MIGRATIONS.slice(0, schemaVersion),
    durableObjectMigrations: [],
    durableObjectBindings: [],
    maintenanceBaseUrl: `https://control-worker-${tenantTag}.example.test`,
    routeHostname: `worker-${tenantTag}.example.test`,
    ...overrides,
  };
}

function baseRecord(
  tenantTag: string,
  backend: ProvisioningBackendKind,
  overrides: Partial<FleetRecord> = {},
): FleetRecord {
  return {
    tenantTag,
    backend,
    environment: ENVIRONMENT,
    scriptName: `worker-${tenantTag}`,
    databaseId: `db-${tenantTag}`,
    databaseName: `database-${tenantTag}`,
    schemaVersion: 1,
    artifactVersion: 'v1',
    desiredSpecDigest: 'e'.repeat(64),
    durableObjectBindings: [],
    routeHostname: `worker-${tenantTag}.example.test`,
    phase: 'ready',
    updatedAt: ORIGIN_UPDATED_AT,
    ...overrides,
  };
}

function externalRelease(
  spec: DeploymentSpec,
  overrides: Partial<ExternalReleaseSnapshot> = {},
): ExternalReleaseSnapshot {
  const physicalScriptName = externalReleaseScriptName(spec);
  return {
    physicalScriptName,
    specDigest: deploymentSpecDigest(spec),
    artifactVersion: `etag:${physicalScriptName}`,
    releaseSchemaVersion: spec.schemaVersion,
    application: EMPTY_APPLICATION,
    ...overrides,
  };
}

/** The live release an immutable backend already serves for `spec`. */
function seededRelease(
  spec: DeploymentSpec,
  record: FleetRecord,
  maintenance: MaintenanceHealth = HEALTHY_MAINTENANCE,
): LiveDeployment {
  const physicalScriptName = externalReleaseScriptName(spec);
  return liveDeployment({
    tenantTag: spec.tenantTag,
    environment: spec.environment,
    scriptName: physicalScriptName,
    databaseId: record.databaseId,
    durableObjectBindings: [],
    serviceBindings: [],
    queueProducerBindings: [],
    plainTextBindings: {},
    secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
    artifactVersion: `etag:${physicalScriptName}`,
    desiredSpecDigest: deploymentSpecDigest(spec),
    schemaVersion: spec.schemaVersion,
    maintenance,
  });
}

interface WorldRun {
  readonly records: readonly FleetRecord[];
  readonly specs: ReadonlyMap<string, DeploymentSpec>;
  readonly backends: ReadonlyMap<string, ProvisioningBackend>;
  readonly ops: MigrationOpLogEntry[];
  readonly fenceViolations: string[];
}

/** Drives `migrateFleet` over one assembled world through recording resolvers. */
async function runWorld(world: WorldRun): Promise<readonly FleetRecord[]> {
  const store = new RecordingFleetStore(
    world.records,
    world.ops,
    world.fenceViolations,
  );
  const settlementHost = new RecordingSettlementHost(world.ops);
  const resolve = <Value>(
    kind: string,
    record: FleetRecord,
    source: ReadonlyMap<string, Value>,
  ): Value => {
    const key = deploymentKey(record);
    world.ops.push(`resolver:${kind}:${key}`);
    const value = source.get(key);
    if (value === undefined) {
      throw new Error(`no ${kind} fixture for '${key}'`);
    }
    return value;
  };
  return migrateFleet({
    store,
    records: world.records,
    canaryTenantTags: [],
    backendFor: (record) => resolve('backendFor', record, world.backends),
    specFor: (record) => resolve('specFor', record, world.specs),
    secretsFor: (record) => {
      world.ops.push(`resolver:secretsFor:${deploymentKey(record)}`);
      return secretsForTenant(record.tenantTag);
    },
    settlementFor: (record) => {
      world.ops.push(`resolver:settlementFor:${deploymentKey(record)}`);
      return settlementHost;
    },
    clock: MIGRATION_CLOCK,
    // The body spreads `routeAttestation` AFTER its own `clock`
    // (`fleet.ts:2035-2038`), so this object must never carry a `clock` key: one
    // here would silently override the frozen clock for the attestation, and the
    // put fence could not notice, because that clock is read only for the
    // convergence budget and never stamps an `updatedAt`. The no-op sleep keeps
    // a frozen clock from turning that budget's break condition
    // (`active-route.ts:289-292`) into real waiting. Every world converges on
    // the first attestation attempt, so no delay is ever scheduled.
    routeAttestation: { sleep: async () => {} },
  });
}

// ---------------------------------------------------------------------------
// SUCCESS WORLD
// ---------------------------------------------------------------------------

/**
 * Assembles the success world. Four records, visited in the scheduler's frozen
 * `localeCompare` order over `<tenantTag>:<environment>` because no canary tag
 * is declared: `extfull`, `plainmulti`, `platformonly`, `readysteady`.
 */
function successWorld(): WorldRun {
  const ops: MigrationOpLogEntry[] = [];
  const fenceViolations: string[] = [];
  const specs = new Map<string, DeploymentSpec>();
  const backends = new Map<string, ProvisioningBackend>();
  const specsByDatabaseId = new Map<string, DeploymentSpec>();
  // Two immutable-external backends, because a deployment's trusted platform
  // profile is a property of the backend that describes it: `steady` still
  // describes the profile its records already carry, while `moved` describes a
  // new state artifact and a narrower egress policy — which is exactly what
  // makes `platformonly` a platform-only change and nothing else.
  const steady = new RecordingImmutableBackend(
    ops,
    specsByDatabaseId,
    fenceViolations,
  );
  const moved = new RecordingImmutableBackend(
    ops,
    specsByDatabaseId,
    fenceViolations,
  );
  moved.stateArtifactDigest = MOVED_STATE_ARTIFACT_DIGEST;
  moved.policyHosts = ['narrow.example.test'];
  const plain = new RecordingPlainBackend(
    ops,
    specsByDatabaseId,
    fenceViolations,
  );

  // -- extfull: an immutable-external FULL migration whose D1 ledger is already
  // at the target schema, so its single `applyMigrations` call is the
  // zero-pending ledger VERIFICATION. Its entry `rollbackRelease` becomes the
  // committed record's `retiringRelease`, which is what drives `retire-post`.
  const extfullOrigin = baseSpec('extfull');
  const extfullSpec = baseSpec('extfull', {
    modules: [{ name: 'worker.js', content: 'export default { release: 2 }' }],
  });
  const extfullActive = externalRelease(extfullOrigin);
  const extfullRollback = externalRelease(
    baseSpec('extfull', {
      modules: [
        { name: 'worker.js', content: 'export default { release: 0 }' },
      ],
    }),
  );
  const extfullTarget = steady.platformTargetFor(extfullOrigin);
  const extfull = baseRecord('extfull', 'workers-for-platforms', {
    artifactVersion: extfullActive.artifactVersion,
    desiredSpecDigest: extfullActive.specDigest,
    activeRelease: extfullActive,
    rollbackRelease: extfullRollback,
    platformTarget: extfullTarget,
    outboundPolicy: extfullTarget.outboundPolicy,
    platformResources: steady.platformResourcesFor(extfullOrigin),
    applicationBindings: EMPTY_APPLICATION,
    applicationResources: [],
  });

  // -- plainmulti: the NON-EXTERNAL record. Its backend declares no
  // `immutableExternalArtifacts`, so `immutableExternal` is false and the
  // migration takes the plain path; its platform-authored spec declares three
  // D1 versions against a record at version 1, so the per-version loop runs
  // twice and emits `applyMigrations:2` then `applyMigrations:3`.
  const plainmultiOrigin = baseSpec('plainmulti', {
    authoredBy: 'platform',
    egressProxyService: undefined,
  });
  const plainmultiSpec = baseSpec('plainmulti', {
    authoredBy: 'platform',
    schemaVersion: 3,
  });
  const plainmulti = baseRecord('plainmulti', 'plain-worker', {
    desiredSpecDigest: deploymentSpecDigest(plainmultiOrigin),
  });
  plain.seedLive(
    'plainmulti',
    liveDeployment({
      tenantTag: 'plainmulti',
      environment: ENVIRONMENT,
      scriptName: plainmulti.scriptName,
      databaseId: plainmulti.databaseId,
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      plainTextBindings: {},
      secretNames: ['DEPLOYMENT_IDENTITY_SECRET', 'MAINTENANCE_ADMIN_SECRET'],
      artifactVersion: 'v1',
      desiredSpecDigest: deploymentSpecDigest(plainmultiOrigin),
      schemaVersion: 1,
      maintenance: HEALTHY_MAINTENANCE,
    }),
  );

  // -- platformonly: the specification is unchanged, but the backend's trusted
  // platform profile has moved (a new state artifact digest and a narrower
  // egress policy), so `platformOnlyChange` selects the platform-only path.
  // Its active release lags the record's schema version, which is what makes
  // `effectiveAppliedPlatformTarget` pin the prior D1 columns.
  const platformonlySpec = baseSpec('platformonly');
  const platformonlyActive = externalRelease(platformonlySpec);
  const platformonlyPriorTarget: ExternalPlatformTargetDescription = {
    ...steady.platformTargetFor(platformonlySpec),
    d1SchemaVersion: 2,
    d1SchemaHistoryDigest: PLATFORM_ONLY_PRIOR_D1_HISTORY_DIGEST,
  };
  const platformonly = baseRecord('platformonly', 'workers-for-platforms', {
    schemaVersion: 2,
    artifactVersion: platformonlyActive.artifactVersion,
    desiredSpecDigest: platformonlyActive.specDigest,
    activeRelease: platformonlyActive,
    platformTarget: platformonlyPriorTarget,
    outboundPolicy: platformonlyPriorTarget.outboundPolicy,
    platformResources: {
      maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
      stateWorker: {
        scriptName: externalStateScriptName(platformonlySpec),
        artifactVersion: 'state-v1',
        artifactDigest: platformonlyPriorTarget.stateArtifactDigest,
        durableObjectBindings: [],
        namespaceIds: [],
      },
      egressProxy: {
        scriptName: externalEgressProxyScriptName(platformonlySpec),
        artifactVersion: 'egress-v1',
        artifactDigest: EGRESS_ARTIFACT_DIGEST,
        ...platformonlyPriorTarget.outboundPolicy,
      },
    },
    applicationBindings: EMPTY_APPLICATION,
    applicationResources: [],
  });

  // -- readysteady: an unchanged deployment reconciled again. It carries a
  // `retiringRelease`, so the pre-dispatch retirement runs; its live
  // maintenance is unarmed, so the ready path's re-arm runs; and its
  // `settledSettlementKey` already names the release it serves, so
  // `skipWhenAlreadySettled` SKIPS — which is what pins that flag, since a
  // steady-state reconcile that settled every pass would bill a fleet for
  // standing still.
  const readysteadySpec = baseSpec('readysteady');
  const readysteadyActive = externalRelease(readysteadySpec);
  const readysteadyRollback = externalRelease(
    baseSpec('readysteady', {
      modules: [
        { name: 'worker.js', content: 'export default { release: 0 }' },
      ],
    }),
  );
  const readysteadyRetiring = externalRelease(
    baseSpec('readysteady', {
      modules: [
        { name: 'worker.js', content: 'export default { release: -1 }' },
      ],
    }),
  );
  const readysteadyTarget = steady.platformTargetFor(readysteadySpec);
  const readysteady = baseRecord('readysteady', 'workers-for-platforms', {
    artifactVersion: readysteadyActive.artifactVersion,
    desiredSpecDigest: readysteadyActive.specDigest,
    activeRelease: readysteadyActive,
    rollbackRelease: readysteadyRollback,
    retiringRelease: readysteadyRetiring,
    platformTarget: readysteadyTarget,
    outboundPolicy: readysteadyTarget.outboundPolicy,
    platformResources: steady.platformResourcesFor(readysteadySpec),
    applicationBindings: EMPTY_APPLICATION,
    applicationResources: [],
    settledSettlementKey: fleetSettlementKey({
      tenantTag: 'readysteady',
      environment: ENVIRONMENT,
      specDigest: readysteadyActive.specDigest,
      artifactVersion: readysteadyActive.artifactVersion,
    }),
  });

  const records = [extfull, plainmulti, platformonly, readysteady];
  for (const [record, spec, backend] of [
    [extfull, extfullSpec, steady],
    [plainmulti, plainmultiSpec, plain],
    [platformonly, platformonlySpec, moved],
    [readysteady, readysteadySpec, steady],
  ] as const) {
    specs.set(deploymentKey(record), spec);
    backends.set(deploymentKey(record), backend);
    specsByDatabaseId.set(record.databaseId, spec);
  }

  steady.releases.set(
    extfullActive.physicalScriptName,
    seededRelease(extfullOrigin, extfull),
  );
  steady.releases.set(
    readysteadyActive.physicalScriptName,
    seededRelease(readysteadySpec, readysteady, UNARMED_MAINTENANCE),
  );
  steady.routedScriptNames.set('extfull', extfullActive.physicalScriptName);
  steady.routedScriptNames.set(
    'readysteady',
    readysteadyActive.physicalScriptName,
  );
  moved.releases.set(
    platformonlyActive.physicalScriptName,
    seededRelease(platformonlySpec, platformonly),
  );
  moved.routedScriptNames.set(
    'platformonly',
    platformonlyActive.physicalScriptName,
  );

  return { records, specs, backends, ops, fenceViolations };
}

/**
 * Runs the success world and returns the records `migrateFleet` produced
 * beside the op log it made getting there.
 */
export async function runFleetMigrationSuccessBaseline(): Promise<{
  readonly result: readonly FleetRecord[];
  readonly ops: readonly MigrationOpLogEntry[];
}> {
  const world = successWorld();
  const result = await runWorld(world);
  if (world.fenceViolations.length > 0) {
    throw new Error(`fence violated: ${world.fenceViolations.join('; ')}`);
  }
  return { result, ops: world.ops };
}

// ---------------------------------------------------------------------------
// STOP WORLD
// ---------------------------------------------------------------------------

/** The refusal the stop world's second record is guaranteed to produce. */
const STOP_REFUSAL =
  "deployment 'bravo:production' has active backend switch 'candidate-deployed'";

/**
 * Assembles the stop world. Three records whose keys sort so that the record
 * that COMPLETES is visited first, the record that is REFUSED second, and the
 * record that stays UNTOUCHED third.
 */
function stopWorld(): WorldRun {
  const ops: MigrationOpLogEntry[] = [];
  const fenceViolations: string[] = [];
  const specs = new Map<string, DeploymentSpec>();
  const backends = new Map<string, ProvisioningBackend>();
  const specsByDatabaseId = new Map<string, DeploymentSpec>();
  const plain = new RecordingPlainBackend(
    ops,
    specsByDatabaseId,
    fenceViolations,
  );

  // -- alpha completes: a plain-path full migration over one pending D1
  // version, so the frozen log proves the drain really did the first record's
  // whole body before it reached the refusal.
  const alphaOrigin = baseSpec('alpha', { authoredBy: 'platform' });
  const alphaSpec = baseSpec('alpha', {
    authoredBy: 'platform',
    schemaVersion: 2,
  });
  const alpha = baseRecord('alpha', 'plain-worker', {
    desiredSpecDigest: deploymentSpecDigest(alphaOrigin),
  });
  plain.seedLive(
    'alpha',
    liveDeployment({
      tenantTag: 'alpha',
      environment: ENVIRONMENT,
      scriptName: alpha.scriptName,
      databaseId: alpha.databaseId,
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      plainTextBindings: {},
      secretNames: ['DEPLOYMENT_IDENTITY_SECRET', 'MAINTENANCE_ADMIN_SECRET'],
      artifactVersion: 'v1',
      desiredSpecDigest: deploymentSpecDigest(alphaOrigin),
      schemaVersion: 1,
      maintenance: HEALTHY_MAINTENANCE,
    }),
  );

  // -- bravo is refused. Its backend switch is mid-flight, so
  // `assertBackendSwitchInactive` throws in the admit preamble — AFTER the
  // lease and the leased reread, which is why the frozen log carries exactly
  // that two-token prefix for this record and nothing more. The subphase is
  // deliberately one of the literals the external-migration namespace also
  // uses: the body emits no `backendSwitchIntent.subphase` token at all, so
  // the shared literal cannot collide in the op log.
  const bravoSpec = baseSpec('bravo');
  const bravo = baseRecord('bravo', 'plain-worker', {
    desiredSpecDigest: deploymentSpecDigest(bravoSpec),
    backendSwitchIntent: {
      kind: 'backend-switch',
      tenantTag: 'bravo',
      environment: ENVIRONMENT,
      prior: {
        scriptName: 'worker-bravo',
        artifactVersion: 'plain-v1',
        specDigest: deploymentSpecDigest(bravoSpec),
        databaseId: 'db-bravo',
        databaseName: 'database-bravo',
        durableObjectBindings: [],
        namespaceIds: [],
        secretNames: ['DEPLOYMENT_IDENTITY_SECRET'],
        applicationResources: [],
        customDomain: {
          id: 'domain-bravo',
          hostname: 'worker-bravo.example.test',
        },
      },
      targetSpecDigest: deploymentSpecDigest(bravoSpec),
      targetApplication: EMPTY_APPLICATION,
      target: {
        maintenanceCapabilityPublicKey: MAINTENANCE_PUBLIC_KEY,
        stateArtifactDigest: STATE_ARTIFACT_DIGEST,
        stateDurableObjectHistoryDigest: STATE_DURABLE_OBJECT_HISTORY_DIGEST,
        egressArtifactDigest: EGRESS_ARTIFACT_DIGEST,
        d1SchemaVersion: bravoSpec.schemaVersion,
        d1SchemaHistoryDigest: deploymentSpecDigest(bravoSpec),
        outboundPolicy: canonicalDeploymentEgressPolicy({
          policyId: externalPlatformResourceGroupId(bravoSpec),
          tenantTag: 'bravo',
          environment: ENVIRONMENT,
          allowedHosts: ['api.example.test'],
        }),
      },
      rollbackUntil: '2026-06-08T00:00:00.000Z',
      subphase: 'candidate-deployed',
    },
  });

  // -- charlie is never visited: the refusal above ends the drain, so this
  // record contributes NO op at all. That absence is the observable stop.
  const charlieSpec = baseSpec('charlie', { authoredBy: 'platform' });
  const charlie = baseRecord('charlie', 'plain-worker', {
    desiredSpecDigest: deploymentSpecDigest(charlieSpec),
  });

  const records = [alpha, bravo, charlie];
  for (const [record, spec] of [
    [alpha, alphaSpec],
    [bravo, bravoSpec],
    [charlie, charlieSpec],
  ] as const) {
    specs.set(deploymentKey(record), spec);
    backends.set(deploymentKey(record), plain);
    specsByDatabaseId.set(record.databaseId, spec);
  }

  return { records, specs, backends, ops, fenceViolations };
}

/**
 * Runs the stop world and returns the refusal `migrateFleet` rejected with
 * beside the op log it made getting there.
 *
 * The clock fence is checked BEFORE the caught error is reported, so a
 * mis-wired clock surfaces as a fence failure rather than masquerading as a
 * plausible stop; and the caught value must be exactly the chosen refusal, so
 * a collaborator fault or a different validation refusal can never be frozen
 * in its place.
 */
export async function runFleetMigrationStopBaseline(): Promise<{
  readonly error: string;
  readonly ops: readonly MigrationOpLogEntry[];
}> {
  const world = stopWorld();
  let caught: unknown;
  let settled = false;
  try {
    await runWorld(world);
    settled = true;
  } catch (error) {
    caught = error;
  }
  if (world.fenceViolations.length > 0) {
    throw new Error(`fence violated: ${world.fenceViolations.join('; ')}`);
  }
  if (settled) {
    throw new Error('the stop world resolved instead of refusing');
  }
  if (!(caught instanceof Error) || caught.message !== STOP_REFUSAL) {
    throw new Error(
      `the stop world refused with '${String(caught)}' instead of '${STOP_REFUSAL}'`,
    );
  }
  return { error: caught.message, ops: world.ops };
}
