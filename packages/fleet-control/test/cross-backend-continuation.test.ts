// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import { migrateFleet } from '../src/fleet.js';
import {
  cleanupDeploymentArtifacts,
  decommissionDeployment,
  forceDecommissionDeployment,
  ProvisioningError,
  provisionDeployment,
} from '../src/provision.js';
import {
  type DeploymentSpec,
  effectiveLifecyclePhase,
  type FleetRecord,
  type FleetStateLease,
  type FleetStateStore,
} from '../src/types.js';
import {
  assertHarnessFailuresConsumed,
  buildPlainWorkerSpec,
  captureFailure,
  directHarness,
  errorChain,
  HarnessFleetStore,
  ignoreFailure,
  initialSpec,
  migrationSpec,
  type PlainWorkerHarness,
  routeAttestation,
  sharedSecrets,
  wranglerHarness,
} from './fixtures/plain-worker-harnesses.js';
import type { ProviderWorld } from './fixtures/provider-world.js';
import {
  type PlainWorkerFsControl,
  registerScratchCleanup,
} from './fixtures/wrangler-fs-mock.js';

const fsControl = vi.hoisted<PlainWorkerFsControl>(() => ({
  failFleetCleanup: false,
  residualDirectory: undefined,
  cleanupError: new Error('continuation scratch cleanup failed'),
}));

vi.mock('node:fs/promises', async () => {
  const { createFsPromisesMock } = await import(
    './fixtures/wrangler-fs-mock.js'
  );
  return createFsPromisesMock(fsControl);
});

const exportDirectories = registerScratchCleanup(fsControl, {
  cleanupError: fsControl.cleanupError,
});

function wrangler(world?: ProviderWorld): PlainWorkerHarness {
  const harness = wranglerHarness(world, { snapshot: true });
  exportDirectories.add(harness.exportDirectory);
  return harness;
}

function provision(harness: PlainWorkerHarness, spec: DeploymentSpec) {
  return provisionWithStore(harness, harness.store, spec);
}

function provisionWithStore(
  harness: Pick<PlainWorkerHarness, 'backend'>,
  store: FleetStateStore,
  spec: DeploymentSpec,
) {
  return provisionDeployment({
    backend: harness.backend,
    store,
    spec,
    secrets: sharedSecrets,
    initialExecutionFenceState: 'open',
    clock: () => 1_000,
    routeAttestation,
  });
}

function mapping(record: FleetRecord) {
  return {
    backend: record.backend,
    scriptName: record.scriptName,
    databaseName: record.databaseName,
    databaseId: record.databaseId,
    routeHostname: record.routeHostname,
  };
}

function worldFacts(world: ProviderWorld) {
  return {
    scripts: [...world.scripts.entries()].map(([name, script]) => ({
      name,
      present: script.present,
      versions: structuredClone(script.versions),
      deployment: structuredClone(script.deployment),
      subdomain: { ...script.subdomain },
      secretNames: [...script.secretNames].sort(),
    })),
    databases: world.databases.map(({ databaseId, name }) => ({
      databaseId,
      name,
    })),
    customDomains: structuredClone(world.customDomains),
    zones: structuredClone(world.zones),
    routes: structuredClone(world.routes),
    durableObjectNamespaces: structuredClone(world.durableObjectNamespaces),
    dispatchNamespaces: structuredClone(world.dispatchNamespaces),
    exports: [...world.exports].map(([databaseId, bytes]) => [
      databaseId,
      [...bytes],
    ]),
    mutationLog: [...world.mutationLog],
  };
}

class ContinuationFleetStore implements FleetStateStore {
  readonly #records = new Map<string, FleetRecord>();
  readonly #leases = new Set<string>();

  constructor(records: readonly FleetRecord[]) {
    for (const record of records) {
      this.#records.set(this.#key(record.tenantTag, record.environment), {
        ...structuredClone(record),
      });
    }
  }

  async withDeploymentLease<T>(
    tenantTag: string,
    environment: string,
    operation: (lease: FleetStateLease) => Promise<T>,
  ): Promise<T> {
    const key = this.#key(tenantTag, environment);
    if (this.#leases.has(key)) throw new Error('deployment is already leased');
    this.#leases.add(key);
    try {
      return await operation({
        tenantTag,
        environment,
        mutationLeaseTtlMs: 15 * 60_000,
        assertOwned: async () => {},
        renew: async () => {},
        put: async (record) => {
          this.#records.set(key, structuredClone(record));
        },
        delete: async () => {
          this.#records.delete(key);
        },
      });
    } finally {
      this.#leases.delete(key);
    }
  }

  async get(
    tenantTag: string,
    environment: string,
  ): Promise<FleetRecord | undefined> {
    const record = this.#records.get(this.#key(tenantTag, environment));
    return record ? structuredClone(record) : undefined;
  }

  async list(): Promise<readonly FleetRecord[]> {
    return [...this.#records.values()].map((record) => structuredClone(record));
  }

  #key(tenantTag: string, environment: string): string {
    return `${tenantTag}:${environment}`;
  }
}

class RepeatedPhaseFailureStore extends HarnessFleetStore {
  #remainingFailures: number;

  constructor(
    world: ProviderWorld,
    readonly failedPhase: FleetRecord['phase'],
    failureCount: number,
  ) {
    super(world);
    this.#remainingFailures = failureCount;
  }

  override async put(record: FleetRecord): Promise<void> {
    if (
      (record.phase === this.failedPhase ||
        effectiveLifecyclePhase(record) === this.failedPhase) &&
      this.#remainingFailures > 0
    ) {
      this.#remainingFailures -= 1;
      throw new Error(`failed state write at ${this.failedPhase}`);
    }
    await super.put(record);
  }
}

function migrate(
  harness: Pick<PlainWorkerHarness, 'backend'>,
  store: FleetStateStore,
  record: FleetRecord,
  spec: DeploymentSpec,
) {
  return migrateFleet({
    store,
    records: [record],
    canaryTenantTags: [],
    backendFor: () => harness.backend,
    specFor: () => spec,
    secretsFor: () => sharedSecrets,
    clock: () => 2_000,
    routeAttestation,
  });
}

async function authorizedWranglerCreate(dispatched: boolean) {
  const harness = wrangler();
  const spec = buildPlainWorkerSpec();
  const store = dispatched
    ? new RepeatedPhaseFailureStore(harness.world, 'database-created', 2)
    : harness.store;
  harness.world.failNext('createDatabase', { dispatched });
  const failure = await captureFailure(
    provisionWithStore(harness, store, spec),
  );
  if (dispatched) {
    expect(failure).toMatchObject({
      message: 'failed state write at database-created',
    });
  } else {
    expect(failure).toBeInstanceOf(ProvisioningError);
  }
  expect(store.record?.phase).toBe('database-create-authorized');
  return { harness, spec, store };
}

describe('ordinary Worker cross-backend continuation', () => {
  afterEach(assertHarnessFailuresConsumed);
  it('converges a Wrangler-created ready deployment through the direct backend without provider mutations', async () => {
    const spec = buildPlainWorkerSpec();
    const source = wrangler();
    const ready = await provision(source, spec);
    const before = worldFacts(source.world);
    const direct = directHarness(source.world);
    direct.store.record = structuredClone(ready.record);

    const converged = await provision(direct, spec);

    expect(converged.record).toEqual(ready.record);
    expect(worldFacts(source.world)).toEqual(before);
  });

  it('resumes every Wrangler provisioning snapshot with the direct backend', async () => {
    const spec = buildPlainWorkerSpec();
    const source = wrangler();
    const ready = await provision(source, spec);
    const snapshots = source.store.snapshots.filter(
      ({ record }) => record.phase !== 'ready',
    );
    expect(
      snapshots.map(({ record }) => [record.phase, record.schemaVersion]),
    ).toEqual([
      ['database-reserved', 0],
      ['database-create-authorized', 0],
      ['database-created', 0],
      ['identity-seeded', 0],
      ['identity-seeded', 1],
      ['identity-seeded', 2],
      ['migrated', 2],
      ['application-resources-create-authorized', 2],
      ['application-resources-deployed', 2],
      ['worker-deployed', 2],
      ['maintenance-armed', 2],
      ['publishing', 2],
    ]);

    for (const snapshot of snapshots) {
      const world = snapshot.world.clone();
      const direct = directHarness(world);
      direct.store.record = structuredClone(snapshot.record);
      const persistedVersionExists = snapshot.world.scripts
        .get(snapshot.record.scriptName)
        ?.versions.some(
          ({ versionId }) => versionId === snapshot.record.artifactVersion,
        );

      const resumed = await provision(direct, spec);

      expect(resumed.record.phase).toBe('ready');
      expect(resumed.record).toMatchObject({
        backend: snapshot.record.backend,
        scriptName: snapshot.record.scriptName,
        databaseName: snapshot.record.databaseName,
        routeHostname: snapshot.record.routeHostname,
      });
      if (
        snapshot.record.phase !== 'database-reserved' &&
        snapshot.record.phase !== 'database-create-authorized'
      ) {
        expect(resumed.record.databaseId).toBe(snapshot.record.databaseId);
      }
      expect(resumed.record.applicationResources).toEqual([]);
      expect(
        world.databases
          .find(({ databaseId }) => databaseId === resumed.record.databaseId)
          ?.d1.queryDatabase(
            'SELECT version FROM anchorage_fleet_migrations ORDER BY version',
          ),
      ).toEqual([{ version: 1 }, { version: 2 }]);
      expect(
        world.scripts
          .get(spec.scriptName)
          ?.versions.some(
            ({ versionId }) => versionId === resumed.record.artifactVersion,
          ),
      ).toBe(true);
      if (persistedVersionExists) {
        expect(resumed.record.artifactVersion).toBe(
          snapshot.record.artifactVersion,
        );
      }
    }
    expect(ready.record.phase).toBe('ready');
  });

  it('resumes migration before and after the staged artifact is persisted', async () => {
    const currentSpec = initialSpec();
    const targetSpec = migrationSpec();
    const source = wrangler();
    const ready = await provision(source, currentSpec);
    source.store.snapshots.length = 0;

    await migrate(source, source.store, ready.record, targetSpec);

    const withoutArtifact = source.store.snapshots.find(
      ({ record }) =>
        record.phase === 'migrating' &&
        record.pendingSpecDigest !== undefined &&
        record.pendingArtifactVersion === undefined,
    );
    const withArtifact = source.store.snapshots.find(
      ({ record }) =>
        record.phase === 'migrating' &&
        record.pendingSpecDigest !== undefined &&
        record.pendingArtifactVersion !== undefined,
    );
    if (!withoutArtifact || !withArtifact) {
      throw new Error('Wrangler migration did not persist both resume states');
    }

    for (const snapshot of [withoutArtifact, withArtifact]) {
      const world = snapshot.world.clone();
      const direct = directHarness(world);
      const otherRecord: FleetRecord = {
        ...structuredClone(snapshot.record),
        tenantTag: 'other',
        environment: 'staging',
        scriptName: 'other-staging',
        databaseName: 'other-staging',
        databaseId: 'database-other',
        routeHostname: 'other.example.test',
      };
      const store = new ContinuationFleetStore([snapshot.record, otherRecord]);
      const [resumed] = await migrate(
        direct,
        store,
        snapshot.record,
        targetSpec,
      );

      expect(resumed).toMatchObject({
        phase: 'ready',
        schemaVersion: targetSpec.schemaVersion,
      });
      expect(mapping(resumed ?? snapshot.record)).toEqual(
        mapping(snapshot.record),
      );
      expect(resumed?.pendingSpecDigest).toBeUndefined();
      expect(resumed?.pendingArtifactVersion).toBeUndefined();
      await expect(store.get('other', 'staging')).resolves.toEqual(otherRecord);
      expect(
        world.scripts
          .get(targetSpec.scriptName)
          ?.versions.some(
            ({ versionId }) => versionId === resumed?.artifactVersion,
          ),
      ).toBe(true);
    }
  });

  it('aborts authorized and owned Wrangler-shaped partial deployments through the direct backend', async () => {
    const authorized = await authorizedWranglerCreate(true);
    const directAuthorized = directHarness(authorized.harness.world);
    directAuthorized.store.record = structuredClone(authorized.store.record);

    await cleanupDeploymentArtifacts({
      backend: directAuthorized.backend,
      store: directAuthorized.store,
      spec: authorized.spec,
    });

    expect(directAuthorized.store.record).toBeUndefined();
    expect(directAuthorized.world.databases).toEqual([]);

    const source = wrangler();
    const spec = buildPlainWorkerSpec();
    await provision(source, spec);
    for (const phase of ['worker-deployed', 'maintenance-armed'] as const) {
      const snapshot = source.store.snapshots.find(
        ({ record }) => record.phase === phase,
      );
      if (!snapshot) throw new Error(`missing ${phase} snapshot`);
      const direct = directHarness(snapshot.world.clone());
      direct.store.record = structuredClone(snapshot.record);

      await cleanupDeploymentArtifacts({
        backend: direct.backend,
        store: direct.store,
        spec,
      });

      expect(direct.store.record).toBeUndefined();
      expect(direct.world.databases).toEqual([]);
      expect(direct.world.scripts.get(spec.scriptName)?.present).toBe(false);
    }
  });

  it('refuses foreign and mismatched resources during direct abort', async () => {
    const foreign = await authorizedWranglerCreate(false);
    const foreignDirect = directHarness(foreign.harness.world);
    foreignDirect.store.record = structuredClone(foreign.store.record);
    const foreignDatabase = foreignDirect.world.seedDatabase(
      foreign.spec.databaseName,
    );
    await foreignDirect.backend.seedDeploymentIdentity(
      {
        id: foreignDatabase.databaseId,
        name: foreignDatabase.name,
        created: false,
      },
      'foreign',
      {
        mutationLeaseTtlMs: 15 * 60_000,
        assertOwned: async () => {},
      },
      { initialExecutionFenceState: 'open' },
    );

    const foreignFailure = await captureFailure(
      cleanupDeploymentArtifacts({
        backend: foreignDirect.backend,
        store: foreignDirect.store,
        spec: foreign.spec,
      }),
    );
    expect(errorChain(foreignFailure)).toContain("owned by 'foreign'");
    expect(foreignDirect.store.record?.phase).toBe(
      'database-create-authorized',
    );

    const source = wrangler();
    const spec = buildPlainWorkerSpec();
    await provision(source, spec);
    const worker = source.store.snapshots.find(
      ({ record }) => record.phase === 'worker-deployed',
    );
    if (!worker) throw new Error('missing worker-deployed snapshot');
    const mismatched = directHarness(worker.world.clone());
    mismatched.store.record = structuredClone(worker.record);
    const index = mismatched.world.databases.findIndex(
      ({ databaseId }) => databaseId === worker.record.databaseId,
    );
    mismatched.world.databases.splice(index, 1);
    mismatched.world.seedDatabase('mismatched-name', {
      databaseId: worker.record.databaseId,
    });

    const mismatchFailure = await captureFailure(
      cleanupDeploymentArtifacts({
        backend: mismatched.backend,
        store: mismatched.store,
        spec,
      }),
    );
    expect(errorChain(mismatchFailure)).toContain(
      'resolved with unexpected identity',
    );
    expect(mismatched.store.record?.phase).toBe('worker-deployed');
  });

  it('compensates a Wrangler ambiguous create after direct recovery creates the Worker', async () => {
    const authorized = await authorizedWranglerCreate(true);
    const direct = directHarness(authorized.harness.world);
    direct.store.record = structuredClone(authorized.store.record);
    const deployWorker = direct.backend.deployWorker.bind(direct.backend);
    let deployedCreated: boolean | undefined;
    vi.spyOn(direct.backend, 'deployWorker').mockImplementation(
      async (...arguments_) => {
        const deployed = await deployWorker(...arguments_);
        deployedCreated = deployed.created;
        return deployed;
      },
    );
    direct.world.failNext('ensureMaintenance', { dispatched: false });

    const failure = await captureFailure(provision(direct, authorized.spec));

    expect(failure).toBeInstanceOf(ProvisioningError);
    expect(deployedCreated).toBe(true);
    expect(direct.store.record).toBeUndefined();
    expect(direct.world.databases).toEqual([]);
    expect(direct.world.scripts.get(authorized.spec.scriptName)?.present).toBe(
      false,
    );
    expect(direct.world.customDomains).toEqual([]);
    expect(direct.world.mutationLog).toEqual(
      expect.arrayContaining([
        `delete-script:${authorized.spec.scriptName}`,
        expect.stringMatching(/^delete-database:/u),
      ]),
    );
  });

  it('does not roll back a direct resume that started at database-created', async () => {
    const source = wrangler();
    const spec = buildPlainWorkerSpec();
    await provision(source, spec);
    const created = source.store.snapshots.find(
      ({ record }) => record.phase === 'database-created',
    );
    if (!created) throw new Error('missing database-created snapshot');
    const direct = directHarness(created.world.clone());
    direct.store.record = structuredClone(created.record);
    direct.world.failNext('ensureMaintenance', { dispatched: false });

    await expect(provision(direct, spec)).rejects.toBeInstanceOf(
      ProvisioningError,
    );

    expect(direct.store.record?.phase).toBe('worker-deployed');
    expect(direct.world.databases).toHaveLength(1);
    expect(direct.world.scripts.get(spec.scriptName)?.present).toBe(true);
  });

  it('decommissions a Wrangler-created ready deployment through the direct backend', async () => {
    const source = wrangler();
    const spec = buildPlainWorkerSpec();
    const ready = await provision(source, spec);
    const direct = directHarness(source.world);
    direct.store.record = structuredClone(ready.record);

    const result = await decommissionDeployment({
      backend: direct.backend,
      store: direct.store,
      spec,
    });

    expect(result.record.phase).toBe('decommissioned');
    expect(direct.world.databases).toEqual([]);
    expect(direct.world.scripts.get(spec.scriptName)?.present).toBe(false);
  });

  it('retries every direct teardown state write from its retained predecessor', async () => {
    const source = wrangler();
    const spec = buildPlainWorkerSpec();
    const ready = await provision(source, spec);
    const rows: readonly Readonly<{
      phase: FleetRecord['phase'];
      predecessor: FleetRecord['phase'];
    }>[] = [
      { phase: 'decommissioning', predecessor: 'ready' },
      { phase: 'traffic-removed', predecessor: 'decommissioning' },
      { phase: 'credentials-revoked', predecessor: 'traffic-removed' },
      { phase: 'worker-deleted', predecessor: 'credentials-revoked' },
      {
        phase: 'platform-credentials-revoked',
        predecessor: 'worker-deleted',
      },
      {
        phase: 'platform-resources-deleted',
        predecessor: 'platform-credentials-revoked',
      },
      {
        phase: 'application-resources-deleting',
        predecessor: 'platform-resources-deleted',
      },
      {
        phase: 'application-resources-deleted',
        predecessor: 'application-resources-deleting',
      },
      {
        phase: 'database-exported',
        predecessor: 'application-resources-deleted',
      },
      { phase: 'database-deleting', predecessor: 'database-exported' },
      { phase: 'decommissioned', predecessor: 'database-deleting' },
    ];

    for (const row of rows) {
      const direct = directHarness(source.world.clone());
      direct.store.record = structuredClone(ready.record);
      direct.store.failPutPhase = row.phase;

      await expect(
        decommissionDeployment({
          backend: direct.backend,
          store: direct.store,
          spec,
        }),
      ).rejects.toThrow(`failed state write at ${row.phase}`);
      const retained = direct.store.record;
      if (!retained) throw new Error('failed write removed the Fleet row');
      expect(effectiveLifecyclePhase(retained)).toBe(row.predecessor);

      const retried = await decommissionDeployment({
        backend: direct.backend,
        store: direct.store,
        spec,
      });
      expect(retried.record.phase).toBe('decommissioned');
      expect(direct.world.databases).toEqual([]);
    }
  });

  it('force-decommissions a wedged Wrangler-created deployment through the direct backend', async () => {
    const source = wrangler();
    const spec = buildPlainWorkerSpec();
    const ready = await provision(source, spec);
    const direct = directHarness(source.world);
    direct.store.record = {
      ...structuredClone(ready.record),
      phase: 'migrating',
      pendingSpecDigest: 'f'.repeat(64),
    };

    await forceDecommissionDeployment({
      backend: direct.backend,
      store: direct.store,
      tenantTag: spec.tenantTag,
      environment: spec.environment,
    });

    expect(direct.store.record).toBeUndefined();
    expect(direct.world.databases).toEqual([]);
    expect(direct.world.scripts.get(spec.scriptName)?.present).toBe(true);
    expect(direct.world.scripts.get(spec.scriptName)?.subdomain).toEqual({
      enabled: false,
      previewsEnabled: false,
    });
    expect(direct.world.scripts.get(spec.scriptName)?.secretNames.size).toBe(0);
  });

  it('converges direct continuation after every ambiguous provider boundary', async () => {
    const spec = buildPlainWorkerSpec();

    const createSource = await authorizedWranglerCreate(false);
    const create = directHarness(createSource.harness.world);
    create.store.record = structuredClone(createSource.store.record);
    create.world.failNext('createDatabase', { dispatched: true });
    // The core adopts an exact unowned database after a lost create response.
    const created = await provision(create, spec);
    expect(created.record.phase).toBe('ready');
    expect(create.world.databases).toHaveLength(1);
    expect(created.record.databaseId).toBe(
      create.world.databases[0]?.databaseId,
    );
    expect(
      create.world.mutationLog.filter((entry) =>
        entry.startsWith('create-database:'),
      ),
    ).toHaveLength(1);

    const source = wrangler();
    const ready = await provision(source, spec);
    const beforeWorker = source.store.snapshots.find(
      ({ record }) => record.phase === 'application-resources-deployed',
    );
    if (!beforeWorker) {
      throw new Error('missing application-resources-deployed snapshot');
    }
    const upload = directHarness(beforeWorker.world.clone());
    upload.store.record = structuredClone(beforeWorker.record);
    upload.world.failNext('uploadCandidate', { dispatched: true });
    expect((await provision(upload, spec)).record.phase).toBe('ready');

    for (const operation of ['deployCandidate', 'promoteWorker']) {
      const direct = directHarness(source.world.clone());
      direct.store.record = structuredClone(ready.record);
      const targetSpec = migrationSpec();
      direct.world.failNext(operation, { dispatched: true });
      const [resumed] = await migrate(
        direct,
        direct.store,
        ready.record,
        targetSpec,
      );
      expect(resumed?.phase).toBe('ready');
    }

    const deletion = directHarness(source.world.clone());
    deletion.store.record = structuredClone(ready.record);
    deletion.world.failNext('deleteWorkerScript', { dispatched: true });
    await ignoreFailure(
      decommissionDeployment({
        backend: deletion.backend,
        store: deletion.store,
        spec,
      }),
    );
    const removed = await decommissionDeployment({
      backend: deletion.backend,
      store: deletion.store,
      spec,
    });
    expect(removed.record.phase).toBe('decommissioned');

    const beforeAttach = source.store.snapshots.find(
      ({ record }) => record.phase === 'publishing',
    );
    if (!beforeAttach) throw new Error('missing publishing snapshot');
    const attached = directHarness(beforeAttach.world.clone());
    attached.store.record = structuredClone(beforeAttach.record);
    attached.world.failNext('attachCustomDomain', { dispatched: true });
    await ignoreFailure(provision(attached, spec));
    const attachedReady = await provision(attached, spec);
    expect(attachedReady.record.phase).toBe('ready');
    expect(attached.world.customDomains).toEqual([
      expect.objectContaining({
        hostname: spec.routeHostname,
        service: spec.scriptName,
      }),
    ]);

    const detached = directHarness(source.world.clone());
    detached.store.record = structuredClone(ready.record);
    detached.world.failNext('detachCustomDomain', { dispatched: true });
    await ignoreFailure(
      decommissionDeployment({
        backend: detached.backend,
        store: detached.store,
        spec,
      }),
    );
    const detachedReady = await decommissionDeployment({
      backend: detached.backend,
      store: detached.store,
      spec,
    });
    expect(detachedReady.record.phase).toBe('decommissioned');

    const databaseDeletion = directHarness(source.world.clone());
    databaseDeletion.store.record = structuredClone(ready.record);
    databaseDeletion.world.failNext('deleteDatabase', { dispatched: true });
    const databaseDeleted = await decommissionDeployment({
      backend: databaseDeletion.backend,
      store: databaseDeletion.store,
      spec,
    });
    expect(databaseDeleted.record.phase).toBe('decommissioned');
    expect(databaseDeletion.world.databases).toEqual([]);
    expect(
      databaseDeletion.world.mutationLog.filter(
        (entry) => entry === `delete-database:${ready.record.databaseId}`,
      ),
    ).toHaveLength(1);

    const secretDeletion = directHarness(source.world.clone());
    secretDeletion.store.record = structuredClone(ready.record);
    secretDeletion.world.failNext('deleteControlSecrets', {
      dispatched: true,
    });
    await ignoreFailure(
      decommissionDeployment({
        backend: secretDeletion.backend,
        store: secretDeletion.store,
        spec,
      }),
    );
    const secretsDeleted = await decommissionDeployment({
      backend: secretDeletion.backend,
      store: secretDeletion.store,
      spec,
    });
    expect(secretsDeleted.record.phase).toBe('decommissioned');
    expect(
      secretDeletion.world.scripts.get(spec.scriptName)?.secretNames.size,
    ).toBe(0);

    const publicAccess = directHarness(source.world.clone());
    publicAccess.store.record = {
      ...structuredClone(ready.record),
      phase: 'migrating',
      pendingSpecDigest: 'f'.repeat(64),
    };
    publicAccess.world.failNext('disablePublicAccess', { dispatched: true });
    await ignoreFailure(
      forceDecommissionDeployment({
        backend: publicAccess.backend,
        store: publicAccess.store,
        tenantTag: spec.tenantTag,
        environment: spec.environment,
      }),
    );
    await forceDecommissionDeployment({
      backend: publicAccess.backend,
      store: publicAccess.store,
      tenantTag: spec.tenantTag,
      environment: spec.environment,
    });
    expect(publicAccess.store.record).toBeUndefined();
    expect(publicAccess.world.scripts.get(spec.scriptName)?.subdomain).toEqual({
      enabled: false,
      previewsEnabled: false,
    });
  });
});
