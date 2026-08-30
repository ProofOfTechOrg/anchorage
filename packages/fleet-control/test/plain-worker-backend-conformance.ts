// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { ActiveRouteAttestationError } from '../src/active-route.js';
import { migrateFleet } from '../src/fleet.js';
import { plainWorkerIngressModule } from '../src/plain-worker-backend.js';
import {
  decommissionDeployment,
  forceDecommissionDeployment,
  provisionDeployment,
} from '../src/provision.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import {
  type DeploymentSecrets,
  type DeploymentSpec,
  effectiveLifecyclePhase,
  type FleetRecord,
} from '../src/types.js';
import {
  buildPlainWorkerSpec,
  captureFailure,
  causeChain,
  errorChain,
  ignoreFailure,
  initialSpec,
  migrationSpec,
  type PlainWorkerHarness,
  routeAttestation,
  seedWorkerFromSpec,
  sharedSecrets,
} from './fixtures/plain-worker-harnesses.js';
import type {
  ProviderDatabase,
  ProviderWorld,
} from './fixtures/provider-world.js';

const ownedFence = {
  mutationLeaseTtlMs: 15 * 60_000,
  assertOwned: async () => {},
};

function databaseReference(database: ProviderDatabase) {
  return {
    id: database.databaseId,
    name: database.name,
    created: false,
  };
}

async function provisionReady(
  harness: PlainWorkerHarness,
  spec: DeploymentSpec = buildPlainWorkerSpec(),
  secrets: DeploymentSecrets = sharedSecrets,
) {
  return provisionDeployment({
    backend: harness.backend,
    store: harness.store,
    spec,
    secrets,
    initialExecutionFenceState: 'open',
    clock: () => 1_000,
    routeAttestation,
  });
}

function migrate(
  harness: PlainWorkerHarness,
  record: FleetRecord,
  spec: DeploymentSpec,
  secrets: DeploymentSecrets = sharedSecrets,
) {
  return migrateFleet({
    store: harness.store,
    records: [record],
    canaryTenantTags: [],
    backendFor: () => harness.backend,
    specFor: () => spec,
    secretsFor: () => secrets,
    clock: () => 2_000,
    routeAttestation,
  });
}

function alterFleetDigest(
  world: ProviderWorld,
  scriptName: string,
  value: string | undefined,
): void {
  const binding = world.scripts
    .get(scriptName)
    ?.versions[0]?.bindings.find(
      (candidate) =>
        candidate !== null &&
        typeof candidate === 'object' &&
        Reflect.get(candidate, 'name') === 'FLEET_SPEC_DIGEST',
    );
  if (!binding || typeof binding !== 'object') {
    throw new Error('seeded Worker has no fleet digest binding');
  }
  if (value === undefined) Reflect.deleteProperty(binding, 'text');
  else Reflect.set(binding, 'text', value);
}

function clearWorker(world: ProviderWorld, scriptName: string): void {
  world.deleteScript(scriptName);
  // deleteScript does not sweep custom domains, so this seed helper clears them
  // before adding the one residual each row exercises.
  for (let index = world.customDomains.length - 1; index >= 0; index -= 1) {
    if (world.customDomains[index]?.service === scriptName) {
      world.customDomains.splice(index, 1);
    }
  }
}

export function describePlainWorkerConformance(
  label: string,
  makeHarness: (world?: ProviderWorld) => PlainWorkerHarness,
): void {
  describe(`ordinary Worker conformance: ${label}`, () => {
    it('1. provisions an initial deployment to ready with one guarded live version', async () => {
      const harness = makeHarness();
      const spec = buildPlainWorkerSpec();
      const result = await provisionReady(harness, spec);

      expect(result.record).toMatchObject({
        backend: 'plain-worker',
        tenantTag: spec.tenantTag,
        environment: spec.environment,
        scriptName: spec.scriptName,
        databaseName: spec.databaseName,
        schemaVersion: spec.schemaVersion,
        desiredSpecDigest: deploymentSpecDigest(spec),
        routeHostname: spec.routeHostname,
        phase: 'ready',
        applicationResources: [],
      });
      expect(result.maintenance).toMatchObject({
        armed: true,
        deploymentSpecDigest: deploymentSpecDigest(spec),
      });
      expect(harness.store.record).toEqual(result.record);
      expect(harness.store.phases).toEqual([
        'database-reserved',
        'database-create-authorized',
        'database-created',
        'identity-seeded',
        'identity-seeded',
        'identity-seeded',
        'migrated',
        'application-resources-create-authorized',
        'application-resources-deployed',
        'worker-deployed',
        'maintenance-armed',
        'publishing',
        'ready',
      ]);

      const script = harness.world.scripts.get(spec.scriptName);
      expect(script).toBeDefined();
      expect(script?.present).toBe(true);
      expect(script?.versions).toHaveLength(1);
      expect(script?.versions[0]).toMatchObject({
        versionId: result.record.artifactVersion,
        tag: deploymentSpecDigest(spec),
      });
      expect(script?.deployment).toEqual([
        { versionId: result.record.artifactVersion, percentage: 100 },
      ]);
      expect(script?.subdomain).toEqual({
        enabled: true,
        previewsEnabled: false,
      });
      expect([...(script?.secretNames ?? [])].sort()).toEqual([
        'DEPLOYMENT_IDENTITY_SECRET',
        'MAINTENANCE_ADMIN_SECRET',
      ]);
      const ingress = plainWorkerIngressModule(spec);
      expect(script?.versions[0]?.mainModule).toBe(ingress.name);
      expect(
        script?.versions[0]?.modules.find(
          (module) => module.name === ingress.name,
        )?.content,
      ).toBe(ingress.content);
      expect(script?.versions[0]?.bindings).toEqual(
        expect.arrayContaining([
          {
            type: 'plain_text',
            name: 'FLEET_INGRESS_CONTRACT',
            text: 'guarded-object-v1',
          },
          {
            type: 'd1',
            name: 'DB',
            database_id: result.record.databaseId,
          },
          expect.objectContaining({
            type: 'durable_object_namespace',
            name: 'MAINTENANCE',
            class_name: 'Maintenance',
            namespace_id: result.record.durableObjectBindings[0]?.namespaceId,
          }),
        ]),
      );
      expect(harness.world.customDomains).toEqual([
        {
          id: expect.any(String),
          hostname: spec.routeHostname,
          service: spec.scriptName,
        },
      ]);
      expect(harness.world.durableObjectNamespaces).toEqual([
        {
          id: result.record.durableObjectBindings[0]?.namespaceId,
          script: spec.scriptName,
          className: 'Maintenance',
        },
      ]);

      const database = harness.world.databases.find(
        ({ databaseId }) => databaseId === result.record.databaseId,
      );
      expect(database).toBeDefined();
      expect(
        database?.d1.queryDatabase(
          'SELECT id, tenant_tag FROM flowsafe_deployment',
        ),
      ).toEqual([{ id: 1, tenant_tag: spec.tenantTag }]);
      expect(
        database?.d1.queryDatabase(
          'SELECT id, state FROM flowsafe_execution_fence',
        ),
      ).toEqual([{ id: 'deployment', state: 'open' }]);
      expect(
        database?.d1.queryDatabase(
          'SELECT version FROM anchorage_fleet_migrations ORDER BY version',
        ),
      ).toEqual([{ version: 1 }, { version: 2 }]);
      expect(
        database?.d1
          .queryDatabase('PRAGMA table_info(example)')
          .map((row) => row.name),
      ).toEqual(['id', 'value']);
    });

    it('2. migrates an existing deployment through a staged candidate and promotion', async () => {
      const harness = makeHarness();
      const currentSpec = initialSpec();
      const targetSpec = migrationSpec();
      const initial = await provisionReady(harness, currentSpec);
      let stagedDeployment:
        | readonly { versionId: string; percentage: number }[]
        | undefined;
      let stagedDigest: string | undefined;
      harness.world.afterNext('ensureMaintenance', (world) => {
        const script = world.scripts.get(targetSpec.scriptName);
        stagedDeployment = script?.deployment?.map((version) => ({
          ...version,
        }));
        stagedDigest = script?.versions[0]?.tag;
      });

      const [migrated] = await migrate(harness, initial.record, targetSpec);

      expect(stagedDeployment).toEqual([
        { versionId: initial.record.artifactVersion, percentage: 100 },
        { versionId: expect.any(String), percentage: 0 },
      ]);
      expect(stagedDigest).toBe(deploymentSpecDigest(targetSpec));
      expect(migrated).toMatchObject({
        phase: 'ready',
        desiredSpecDigest: deploymentSpecDigest(targetSpec),
        schemaVersion: 2,
      });
      expect(migrated?.pendingSpecDigest).toBeUndefined();
      expect(migrated?.pendingArtifactVersion).toBeUndefined();
      const script = harness.world.scripts.get(targetSpec.scriptName);
      expect(script?.deployment).toEqual([
        { versionId: migrated?.artifactVersion, percentage: 100 },
      ]);
      expect(harness.world.customDomains).toEqual([
        expect.objectContaining({
          hostname: targetSpec.routeHostname,
          service: targetSpec.scriptName,
        }),
      ]);
    });

    it('3. leaves the candidate at zero traffic when maintenance attestation fails', async () => {
      const harness = makeHarness();
      const currentSpec = initialSpec();
      const targetSpec = migrationSpec();
      const initial = await provisionReady(harness, currentSpec);
      harness.world.mutationLog.length = 0;
      harness.world.afterNext('ensureMaintenance', (world) => {
        alterFleetDigest(world, targetSpec.scriptName, 'f'.repeat(64));
      });

      const failure = await captureFailure(
        migrate(harness, initial.record, targetSpec),
      );

      expect(errorChain(failure)).toContain(
        'maintenance response did not attest fleet specification',
      );
      const script = harness.world.scripts.get(targetSpec.scriptName);
      expect(script?.deployment).toEqual([
        { versionId: initial.record.artifactVersion, percentage: 100 },
        { versionId: expect.any(String), percentage: 0 },
      ]);
      expect(harness.world.mutationLog).toContain(
        `deploy-candidate:${targetSpec.scriptName}`,
      );
      expect(harness.world.mutationLog).not.toContain(
        `deploy:${targetSpec.scriptName}`,
      );
    });

    it('4. attests one active route and refuses absent or malformed routed digests', async () => {
      const spec = buildPlainWorkerSpec();
      const healthy = makeHarness();
      const [version] = seedWorkerFromSpec(healthy.world, { spec });
      await expect(
        healthy.backend.attestActiveRoute(spec),
      ).resolves.toMatchObject({
        specDigest: deploymentSpecDigest(spec),
        artifactVersion: version?.versionId,
        physicalScriptName: spec.scriptName,
        source: 'workers-deployments',
      });

      for (const digest of [undefined, 'malformed-digest']) {
        const harness = makeHarness();
        seedWorkerFromSpec(harness.world, { spec });
        alterFleetDigest(harness.world, spec.scriptName, digest);
        const failure = await captureFailure(
          harness.backend.attestActiveRoute(spec),
        );
        expect(failure).toBeInstanceOf(ActiveRouteAttestationError);
      }
    });

    it('5. refuses split traffic during active-route attestation', async () => {
      const harness = makeHarness();
      const spec = buildPlainWorkerSpec();
      const nextSpec = buildPlainWorkerSpec({
        modules: [
          { name: 'worker.js', content: 'export default { fetch() {} }' },
        ],
      });
      const [active] = seedWorkerFromSpec(harness.world, { spec });
      const [candidate] = seedWorkerFromSpec(harness.world, {
        spec: nextSpec,
        mode: 'staged',
      });
      const script = harness.world.scripts.get(spec.scriptName);
      if (!active || !candidate || !script) {
        throw new Error('failed to seed split-traffic Worker');
      }
      script.deployment = [
        { versionId: active.versionId, percentage: 50 },
        { versionId: candidate.versionId, percentage: 50 },
      ];

      await expect(
        harness.backend.attestActiveRoute(spec),
      ).rejects.toBeInstanceOf(ActiveRouteAttestationError);
    });

    it('6. refuses foreign Worker, database, and custom-domain ownership', async () => {
      const spec = buildPlainWorkerSpec({ previousDurableObjectTag: 'v1' });

      const foreignWorker = makeHarness();
      const workerDatabase = foreignWorker.world.seedDatabase(
        spec.databaseName,
      );
      seedWorkerFromSpec(foreignWorker.world, {
        spec: buildPlainWorkerSpec({ tenantTag: 'foreign' }),
        databaseId: workerDatabase.databaseId,
      });
      const workerFailure = await captureFailure(
        foreignWorker.backend.deployWorker(
          spec,
          databaseReference(workerDatabase),
          sharedSecrets,
          undefined,
          ownedFence,
          undefined,
        ),
      );
      expect(errorChain(workerFailure)).toMatch(
        /different deployment ownership|drifted tenant/u,
      );

      const foreignDatabase = makeHarness();
      const database = foreignDatabase.world.seedDatabase(spec.databaseName);
      const reference = databaseReference(database);
      await foreignDatabase.backend.seedDeploymentIdentity(
        reference,
        'foreign',
        ownedFence,
        { initialExecutionFenceState: 'open' },
      );
      const databaseFailure = await captureFailure(
        foreignDatabase.backend.seedDeploymentIdentity(
          reference,
          spec.tenantTag,
          ownedFence,
          { initialExecutionFenceState: 'open' },
        ),
      );
      expect(errorChain(databaseFailure)).toContain(
        "already belongs to deployment 'foreign'",
      );

      const foreignDomain = makeHarness();
      const [domainVersion] = seedWorkerFromSpec(foreignDomain.world, { spec });
      foreignDomain.world.customDomains.push({
        id: 'foreign-domain',
        hostname: spec.routeHostname,
        service: 'foreign-worker',
      });
      const domainFailure = await captureFailure(
        foreignDomain.backend.promoteWorker(
          spec,
          {
            allowedCurrentScriptNames: [spec.scriptName],
            allowUnrouted: false,
          },
          undefined,
          ownedFence,
          domainVersion?.versionId,
        ),
      );
      expect(errorChain(domainFailure)).toContain(
        `custom domain '${spec.routeHostname}' is owned by unexpected Worker 'foreign-worker'`,
      );
    });

    it('7. refuses duplicate tagged candidates in provider inventory', async () => {
      const spec = buildPlainWorkerSpec();
      const duplicateInventory = makeHarness();
      seedWorkerFromSpec(duplicateInventory.world, { spec });
      seedWorkerFromSpec(duplicateInventory.world, {
        spec,
        mode: 'staged',
      });
      await expect(
        duplicateInventory.backend.inspect(
          spec,
          sharedSecrets.maintenanceAdmin,
          undefined,
        ),
      ).rejects.toThrow(
        /multiple Worker versions use fleet specification tag/u,
      );

      const dispatchedDuplicate = makeHarness();
      const currentSpec = initialSpec();
      const targetSpec = migrationSpec();
      const ready = await provisionReady(dispatchedDuplicate, currentSpec);
      const readyRecord = structuredClone(ready.record);
      const database = dispatchedDuplicate.world.databases.find(
        ({ databaseId }) => databaseId === ready.record.databaseId,
      );
      if (!database) throw new Error('ready database disappeared');
      const injected = new Error('dispatched duplicate upload');
      dispatchedDuplicate.world.failNext('uploadCandidate', {
        dispatched: true,
        duplicate: true,
        error: injected,
      });
      dispatchedDuplicate.world.mutationLog.length = 0;

      const failure = await captureFailure(
        dispatchedDuplicate.backend.deployWorker(
          targetSpec,
          databaseReference(database),
          sharedSecrets,
          undefined,
          ownedFence,
          undefined,
        ),
      );

      expect(
        causeChain(failure).some(
          (cause) =>
            cause instanceof Error &&
            cause.constructor === injected.constructor &&
            cause.message === injected.message,
        ),
      ).toBe(true);
      expect(dispatchedDuplicate.store.record).toEqual(readyRecord);
      expect(
        dispatchedDuplicate.world.scripts
          .get(targetSpec.scriptName)
          ?.versions.filter(
            ({ tag }) => tag === deploymentSpecDigest(targetSpec),
          ),
      ).toHaveLength(2);
      expect(dispatchedDuplicate.world.mutationLog).not.toContain(
        `deploy:${targetSpec.scriptName}`,
      );
      expect(dispatchedDuplicate.world.mutationLog).not.toContain(
        `deploy-candidate:${targetSpec.scriptName}`,
      );
    });

    it('8. converges the exact secret set and refuses a residual secret during decommission', async () => {
      const applicationSecret = 'application-secret-value-000000000001';
      const application = {
        vars: [],
        secrets: [
          {
            name: 'APP_TOKEN',
            valueSha256: createHash('sha256')
              .update(applicationSecret)
              .digest('hex'),
          },
        ],
        r2Buckets: [],
      };
      const secrets = {
        ...sharedSecrets,
        application: { APP_TOKEN: applicationSecret },
      };
      const currentSpec = initialSpec();
      const initialWithSecret = { ...currentSpec, application };
      const targetSpec = migrationSpec({ application });
      const harness = makeHarness();
      const initial = await provisionReady(harness, initialWithSecret, secrets);
      const expectedSecrets = [
        'APP_TOKEN',
        'DEPLOYMENT_IDENTITY_SECRET',
        'MAINTENANCE_ADMIN_SECRET',
      ];
      expect(
        [
          ...(harness.world.scripts.get(currentSpec.scriptName)?.secretNames ??
            []),
        ].sort(),
      ).toEqual(expectedSecrets);

      const [migrated] = await migrate(
        harness,
        initial.record,
        targetSpec,
        secrets,
      );
      expect(
        [
          ...(harness.world.scripts.get(targetSpec.scriptName)?.secretNames ??
            []),
        ].sort(),
      ).toEqual(expectedSecrets);
      harness.world.afterNext('deleteControlSecrets', (world) => {
        world.scripts.get(targetSpec.scriptName)?.secretNames.add('APP_TOKEN');
      });

      const failure = await captureFailure(
        decommissionDeployment({
          backend: harness.backend,
          store: harness.store,
          spec: targetSpec,
          clock: () => 3_000,
        }),
      );
      expect(errorChain(failure)).toContain('failed exact secret revocation');
      expect(
        harness.world.scripts.get(targetSpec.scriptName)?.secretNames,
      ).toContain('APP_TOKEN');
      expect(migrated?.phase).toBe('ready');
    });

    it('9. refuses foreign and concurrently claimed custom domains', async () => {
      const spec = buildPlainWorkerSpec();

      const foreign = makeHarness();
      const [foreignVersion] = seedWorkerFromSpec(foreign.world, { spec });
      foreign.world.customDomains.push({
        id: 'foreign-domain',
        hostname: spec.routeHostname,
        service: 'foreign-worker',
      });
      const foreignFailure = await captureFailure(
        foreign.backend.promoteWorker(
          spec,
          {
            allowedCurrentScriptNames: [spec.scriptName],
            allowUnrouted: false,
          },
          undefined,
          ownedFence,
          foreignVersion?.versionId,
        ),
      );
      expect(errorChain(foreignFailure)).toContain(
        "owned by unexpected Worker 'foreign-worker'",
      );

      const racing = makeHarness();
      const [racingVersion] = seedWorkerFromSpec(racing.world, { spec });
      racing.world.afterNext('listCustomDomains', (world) => {
        world.customDomains.push({
          id: 'racing-domain',
          hostname: spec.routeHostname,
          service: 'racing-worker',
        });
      });
      racing.world.mutationLog.length = 0;
      const racingFailure = await captureFailure(
        racing.backend.promoteWorker(
          spec,
          { allowedCurrentScriptNames: [spec.scriptName], allowUnrouted: true },
          undefined,
          ownedFence,
          racingVersion?.versionId,
        ),
      );
      expect(errorChain(racingFailure)).toContain(
        "owned by unexpected Worker 'racing-worker'",
      );
      expect(racing.world.mutationLog).not.toContain(
        `attach-domain:${spec.routeHostname}`,
      );
    });

    it('10. preserves public-access and guarded-ingress facts through ambiguous initial upload recovery', async () => {
      const harness = makeHarness();
      const spec = buildPlainWorkerSpec();
      harness.world.failNext('uploadCandidate', { dispatched: true });
      const accepted = await provisionReady(harness, spec);
      const retried = await provisionReady(harness, spec);
      const script = harness.world.scripts.get(spec.scriptName);

      expect(accepted.record.phase).toBe('ready');
      expect(retried.record.phase).toBe('ready');
      expect(script?.subdomain).toEqual({
        enabled: true,
        previewsEnabled: false,
      });
      expect(script?.deployment).toEqual([
        { versionId: accepted.record.artifactVersion, percentage: 100 },
      ]);
      expect(script?.versions).toHaveLength(1);
      const ingress = plainWorkerIngressModule(spec);
      expect(script?.versions[0]?.mainModule).toBe(ingress.name);
      expect(
        script?.versions[0]?.modules.find(
          (module) => module.name === ingress.name,
        )?.content,
      ).toBe(ingress.content);
      expect(script?.versions[0]?.bindings).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            type: 'plain_text',
            name: 'FLEET_INGRESS_CONTRACT',
            text: 'guarded-object-v1',
          }),
        ]),
      );
    });

    it('11. reconciles committed mutations and refuses requests that never commit', async () => {
      const spec = buildPlainWorkerSpec();

      const databaseCreate = makeHarness();
      databaseCreate.world.failNext('createDatabase', { dispatched: true });
      // The core adopts an exact unowned database after a lost create response.
      const created = await provisionReady(databaseCreate, spec);
      expect(created.record.phase).toBe('ready');
      expect(databaseCreate.world.databases).toHaveLength(1);
      expect(created.record.databaseId).toBe(
        databaseCreate.world.databases[0]?.databaseId,
      );
      expect(
        databaseCreate.world.mutationLog.filter((entry) =>
          entry.startsWith('create-database:'),
        ),
      ).toHaveLength(1);

      const upload = makeHarness();
      upload.world.failNext('uploadCandidate', { dispatched: true });
      const uploaded = await provisionReady(upload, spec);
      expect(uploaded.record.phase).toBe('ready');
      expect(upload.world.scripts.get(spec.scriptName)?.versions).toHaveLength(
        1,
      );

      const rejectedUpload = makeHarness();
      rejectedUpload.world.failNext('uploadCandidate', {
        dispatched: false,
        error: new Error('injected upload sentinel 0001'),
      });
      const uploadRejection = await captureFailure(
        provisionReady(rejectedUpload, spec),
      );
      const rejectedScript = rejectedUpload.world.scripts.get(spec.scriptName);
      expect(errorChain(uploadRejection)).toContain(
        'injected upload sentinel 0001',
      );
      expect(rejectedScript).toBeUndefined();
      expect(
        rejectedUpload.world.mutationLog.some((entry) =>
          entry.startsWith(`upload:${spec.scriptName}`),
        ),
      ).toBe(false);

      const stagedUpload = makeHarness();
      const stagedReady = await provisionReady(stagedUpload, initialSpec());
      const stagedTarget = migrationSpec();
      const database = stagedUpload.world.databases.find(
        ({ databaseId }) => databaseId === stagedReady.record.databaseId,
      );
      if (!database) throw new Error('ready database disappeared');
      stagedUpload.world.failNext('uploadCandidate', {
        dispatched: false,
        error: new Error('injected staged upload sentinel 0001'),
      });
      stagedUpload.world.mutationLog.length = 0;

      const stagedRejection = await captureFailure(
        stagedUpload.backend.deployWorker(
          stagedTarget,
          databaseReference(database),
          sharedSecrets,
          undefined,
          ownedFence,
          undefined,
        ),
      );

      expect(errorChain(stagedRejection)).toContain(
        'injected staged upload sentinel 0001',
      );
      expect(errorChain(stagedRejection)).toContain(
        `failed to update existing Worker '${stagedTarget.scriptName}'`,
      );
      expect(
        stagedUpload.world.scripts.get(stagedTarget.scriptName)?.versions,
      ).toHaveLength(1);
      expect(
        stagedUpload.world.mutationLog.some((entry) =>
          entry.startsWith('upload:'),
        ),
      ).toBe(false);

      for (const operation of ['deployCandidate', 'promoteWorker']) {
        const deployment = makeHarness();
        const firstSpec = initialSpec();
        const targetSpec = migrationSpec();
        const initial = await provisionReady(deployment, firstSpec);
        deployment.world.failNext(operation, { dispatched: true });
        const [recovered] = await migrate(
          deployment,
          initial.record,
          targetSpec,
        );
        expect(recovered?.phase).toBe('ready');
        expect(
          deployment.world.scripts.get(targetSpec.scriptName)?.deployment,
        ).toEqual([{ versionId: recovered?.artifactVersion, percentage: 100 }]);

        const rejected = makeHarness();
        const rejectedInitial = await provisionReady(rejected, firstSpec);
        const initialRecord = structuredClone(rejectedInitial.record);
        rejected.world.mutationLog.length = 0;
        rejected.world.failNext(operation, {
          dispatched: false,
          error: new Error(`injected ${operation} sentinel 0001`),
        });
        const rejection = await captureFailure(
          migrate(rejected, rejectedInitial.record, targetSpec),
        );
        const script = rejected.world.scripts.get(targetSpec.scriptName);
        const targetDigest = deploymentSpecDigest(targetSpec);

        expect(errorChain(rejection)).toContain(
          `injected ${operation} sentinel 0001`,
        );
        expect(script?.versions).toHaveLength(2);
        expect(script?.versions[0]?.tag).toBe(targetDigest);
        expect(rejected.world.mutationLog).toContain(
          `upload:${targetSpec.scriptName}`,
        );
        expect(rejected.world.mutationLog).not.toContain(
          `deploy:${targetSpec.scriptName}`,
        );
        expect(rejected.store.record).toMatchObject({
          phase: 'migrating',
          pendingSpecDigest: targetDigest,
          desiredSpecDigest: deploymentSpecDigest(firstSpec),
          artifactVersion: initialRecord.artifactVersion,
          schemaVersion: 2,
        });

        if (operation === 'deployCandidate') {
          expect(errorChain(rejection)).toContain(
            "failed to update existing Worker 'acme-production'",
          );
          expect(script?.deployment).toEqual([
            {
              versionId: initialRecord.artifactVersion,
              percentage: 100,
            },
          ]);
          expect(rejected.world.mutationLog).not.toContain(
            `deploy-candidate:${targetSpec.scriptName}`,
          );
          expect(rejected.store.record?.pendingArtifactVersion).toBeUndefined();
        } else {
          const candidateVersion = script?.versions[0]?.versionId;
          expect(errorChain(rejection)).not.toContain(
            'failed to update existing Worker',
          );
          expect(script?.deployment).toEqual([
            {
              versionId: initialRecord.artifactVersion,
              percentage: 100,
            },
            { versionId: candidateVersion, percentage: 0 },
          ]);
          expect(
            rejected.world.mutationLog.filter(
              (entry) => entry === `deploy-candidate:${targetSpec.scriptName}`,
            ),
          ).toHaveLength(1);
          expect(
            rejected.world.customDomains.map(({ hostname, service }) => ({
              hostname,
              service,
            })),
          ).toEqual([
            {
              hostname: 'acme.example.test',
              service: 'acme-production',
            },
          ]);
          expect(rejected.store.record?.pendingArtifactVersion).toBe(
            candidateVersion,
          );
        }
      }

      const deletion = makeHarness();
      await provisionReady(deletion, spec);
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
      expect(deletion.world.databases).toHaveLength(0);
      expect(deletion.world.scripts.get(spec.scriptName)?.present).toBe(false);
    });

    it('12. refuses lease takeover after promotion reads and before the first write', async () => {
      const harness = makeHarness();
      const spec = buildPlainWorkerSpec();
      const [version] = seedWorkerFromSpec(harness.world, { spec });
      let domainReadCompleted = false;
      harness.world.afterNext('listCustomDomains', () => {
        domainReadCompleted = true;
      });
      harness.world.mutationLog.length = 0;
      const leaseLost = new Error('lease ownership transferred');
      const fence = {
        mutationLeaseTtlMs: 15 * 60_000,
        assertOwned: async () => {
          if (domainReadCompleted) throw leaseLost;
        },
      };

      const failure = await captureFailure(
        harness.backend.promoteWorker(
          spec,
          { allowedCurrentScriptNames: [spec.scriptName], allowUnrouted: true },
          undefined,
          fence,
          version?.versionId,
        ),
      );

      expect(failure).toBe(leaseLost);
      expect(domainReadCompleted).toBe(true);
      expect(harness.world.customDomains).toEqual([]);
      expect(harness.world.mutationLog).toEqual([]);
    });

    it('13. refuses unknown and non-object provider bindings', async () => {
      const spec = buildPlainWorkerSpec();
      for (const malformed of [
        {
          binding: { type: 'analytics_engine', name: 'AE' },
          sentence: 'has an unsupported or malformed provider binding',
        },
        { binding: null, sentence: /binding \d+ is not an object/u },
      ]) {
        const harness = makeHarness();
        const [version] = seedWorkerFromSpec(harness.world, { spec });
        const script = harness.world.scripts.get(spec.scriptName);
        if (!script || !version) throw new Error('failed to seed Worker');
        harness.world.seedScript(spec.scriptName, {
          present: true,
          versions: script.versions.map((candidate) => ({
            ...candidate,
            bindings: [...candidate.bindings, malformed.binding],
          })),
          deployment: script.deployment,
          subdomain: script.subdomain,
          secretNames: new Set(script.secretNames),
        });

        const failure = await captureFailure(
          harness.backend.inspect(
            spec,
            sharedSecrets.maintenanceAdmin,
            version.versionId,
          ),
        );
        expect(errorChain(failure)).toMatch(malformed.sentence);
      }
    });

    it('14. resumes every teardown phase and preserves export ordering and integrity', async () => {
      const spec = buildPlainWorkerSpec();
      const baseline = makeHarness();
      const ready = await provisionReady(baseline, spec);
      const teardownPhases: FleetRecord['phase'][] = [
        'decommissioning',
        'traffic-removed',
        'credentials-revoked',
        'worker-deleted',
        'platform-credentials-revoked',
        'platform-resources-deleted',
        'application-resources-deleting',
        'application-resources-deleted',
        'database-exported',
        'database-deleting',
        'decommissioned',
      ];

      for (const [index, phase] of teardownPhases.entries()) {
        const harness = makeHarness(baseline.world.clone());
        harness.store.record = structuredClone(ready.record);
        harness.world.mutationLog.length = 0;
        harness.store.failPutPhase = phase;
        const sourceBytes = harness.world.exports.get(ready.record.databaseId);
        if (!sourceBytes)
          throw new Error('seeded database has no export bytes');
        const expectedBytes = new Uint8Array(sourceBytes);

        await expect(
          decommissionDeployment({
            backend: harness.backend,
            store: harness.store,
            spec,
          }),
        ).rejects.toThrow(`failed state write at ${phase}`);
        const retained = harness.store.record;
        if (!retained) throw new Error('failed write removed the Fleet row');
        const predecessor = teardownPhases[index - 1] ?? 'ready';
        expect(effectiveLifecyclePhase(retained)).toBe(predecessor);
        expect(retained.phase).toBe('decommission-advancing');
        const result = await decommissionDeployment({
          backend: harness.backend,
          store: harness.store,
          spec,
        });

        const exportIndex = harness.world.mutationLog.indexOf(
          `export:${ready.record.databaseId}`,
        );
        const deleteIndex = harness.world.mutationLog.indexOf(
          `delete-database:${ready.record.databaseId}`,
        );
        expect(exportIndex).toBeGreaterThanOrEqual(0);
        expect(deleteIndex).toBeGreaterThan(exportIndex);
        expect(result.databaseExport).toMatchObject({
          databaseId: ready.record.databaseId,
          size: expectedBytes.byteLength,
          sha256: createHash('sha256').update(expectedBytes).digest('hex'),
        });
        expect(
          harness.exportStore.exports.get(ready.record.databaseId)?.bytes,
        ).toEqual(expectedBytes);
        expect(result.record).toMatchObject({
          phase: 'decommissioned',
          decommissionIntent: {
            lifecyclePhase: 'decommissioned',
            state: 'complete',
          },
        });
      }

      const exportFailure = makeHarness(baseline.world.clone());
      exportFailure.store.record = structuredClone(ready.record);
      exportFailure.world.mutationLog.length = 0;
      exportFailure.world.failNext('exportDatabase', {
        dispatched: false,
      });
      const exportRejection = await captureFailure(
        decommissionDeployment({
          backend: exportFailure.backend,
          store: exportFailure.store,
          spec,
        }),
      );
      expect(exportRejection).toBeInstanceOf(Error);
      // This is a regression guard because the row injects no state-write failure.
      expect(errorChain(exportRejection)).not.toContain('failed state write');
      expect(
        exportFailure.exportStore.exports.has(ready.record.databaseId),
      ).toBe(false);
      expect(exportFailure.world.mutationLog).not.toContain(
        `delete-database:${ready.record.databaseId}`,
      );

      const integrityFailure = makeHarness(baseline.world.clone());
      integrityFailure.store.record = structuredClone(ready.record);
      integrityFailure.world.mutationLog.length = 0;
      integrityFailure.world.exports.set(
        ready.record.databaseId,
        new Uint8Array(),
      );
      const integrityRejection = await captureFailure(
        decommissionDeployment({
          backend: integrityFailure.backend,
          store: integrityFailure.store,
          spec,
        }),
      );
      expect(errorChain(integrityRejection)).not.toContain(
        'failed state write',
      );
      expect(integrityFailure.world.mutationLog).toContain(
        `export:${ready.record.databaseId}`,
      );
      expect(integrityFailure.world.mutationLog).not.toContain(
        `delete-database:${ready.record.databaseId}`,
      );
      expect(integrityFailure.store.record?.phase).not.toBe('decommissioned');
      // Eleven teardown phases each run a failed and a resumed decommission.
    }, 15_000);

    it('15. force-decommissions a deployment wedged after traffic removal', async () => {
      const harness = makeHarness();
      const spec = buildPlainWorkerSpec();
      const ready = await provisionReady(harness, spec);
      harness.store.record = {
        ...ready.record,
        phase: 'migrating',
        pendingSpecDigest: 'f'.repeat(64),
      };
      harness.store.failPutPhase = 'traffic-removed';

      await expect(
        forceDecommissionDeployment({
          backend: harness.backend,
          store: harness.store,
          tenantTag: spec.tenantTag,
          environment: spec.environment,
        }),
      ).rejects.toThrow('failed state write at traffic-removed');
      expect(harness.store.record?.phase).toBe('decommissioning');

      await expect(
        forceDecommissionDeployment({
          backend: harness.backend,
          store: harness.store,
          tenantTag: spec.tenantTag,
          environment: spec.environment,
        }),
      ).resolves.toBeUndefined();
      expect(harness.store.record).toBeUndefined();
      expect(harness.world.databases).toEqual([]);
      expect(harness.world.customDomains).toEqual([]);
      expect(harness.world.scripts.get(spec.scriptName)?.subdomain).toEqual({
        enabled: false,
        previewsEnabled: false,
      });
      expect(harness.world.scripts.get(spec.scriptName)?.secretNames.size).toBe(
        0,
      );
    });

    it('16. refuses every positive residual before database deletion', async () => {
      const spec = buildPlainWorkerSpec();
      const baseline = makeHarness();
      const ready = await provisionReady(baseline, spec);

      for (const residual of [
        'domain',
        'zone-route',
        'namespace',
        'attachment',
      ]) {
        const harness = makeHarness(baseline.world.clone());
        harness.store.record = structuredClone(ready.record);
        clearWorker(harness.world, spec.scriptName);
        if (residual === 'domain') {
          harness.world.customDomains.push({
            id: 'residual-domain',
            hostname: spec.routeHostname,
            service: 'foreign-worker',
          });
        } else if (residual === 'zone-route') {
          harness.world.zones.push({ id: 'zone-1' });
          harness.world.routes.push({
            zoneId: 'zone-1',
            id: 'residual-route',
            pattern: `${spec.routeHostname}/*`,
            script: spec.scriptName,
          });
        } else if (residual === 'namespace') {
          harness.world.durableObjectNamespaces.push({
            id: 'residual-namespace',
            script: spec.scriptName,
            className: 'Maintenance',
          });
        } else {
          harness.world.dispatchNamespaces.push({
            name: 'foreign-dispatch',
            scripts: [
              {
                name: 'foreign-worker',
                bindings: [
                  {
                    type: 'd1',
                    name: 'DB',
                    database_id: ready.record.databaseId,
                  },
                ],
              },
            ],
          });
        }
        const database = harness.world.databases.find(
          ({ databaseId }) => databaseId === ready.record.databaseId,
        );
        if (!database) throw new Error('ready database disappeared');
        const failure = await captureFailure(
          harness.backend.assertDatabaseDetached(
            spec,
            ready.record,
            databaseReference(database),
            ownedFence,
          ),
        );
        expect(failure).toBeInstanceOf(Error);
        expect(errorChain(failure)).toMatch(
          residual === 'attachment'
            ? /remains attached to dispatch Worker 'foreign-worker'/u
            : /residual route or Durable Object namespace footprint/u,
        );
      }

      const residualSecret = makeHarness(baseline.world.clone());
      residualSecret.store.record = structuredClone(ready.record);
      residualSecret.world.afterNext('deleteControlSecrets', (world) => {
        world.scripts
          .get(spec.scriptName)
          ?.secretNames.add('DEPLOYMENT_IDENTITY_SECRET');
      });
      const failure = await captureFailure(
        decommissionDeployment({
          backend: residualSecret.backend,
          store: residualSecret.store,
          spec,
        }),
      );
      expect(errorChain(failure)).toContain('failed exact secret revocation');
      expect(residualSecret.world.databases).toHaveLength(1);
    });
  });
}
