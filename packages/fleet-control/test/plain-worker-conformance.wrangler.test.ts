// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it, vi } from 'vitest';
import { CloudflareApiPlainWorkerProvisioningApi } from '../src/cloudflare-api-plain-worker-provisioning-api.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import { WranglerPlainWorkerProvisioningApi } from '../src/wrangler-plain-worker-provisioning-api.js';
import {
  recordingFetch,
  restProjection,
} from './fixtures/cloudflare-fetch-fixture.js';
import {
  assertHarnessFailuresConsumed,
  buildPlainWorkerSpec,
  HarnessExportStore,
  initialSpec,
  migrationSpec,
  plainOnlyClient,
  seedWorkerFromSpec,
  sharedSecrets,
  uploadIntentForSpec,
  wranglerHarness,
} from './fixtures/plain-worker-harnesses.js';
import { mutationFence } from './fixtures/plain-worker-port-probe.js';
import type { ProviderWorld } from './fixtures/provider-world.js';
import { providerWorld } from './fixtures/provider-world.js';
import {
  type PlainWorkerFsControl,
  registerScratchCleanup,
} from './fixtures/wrangler-fs-mock.js';
import { cliProjection } from './fixtures/wrangler-world-projection.js';
import { describePlainWorkerConformance } from './plain-worker-backend-conformance.js';

const fsControl = vi.hoisted<PlainWorkerFsControl>(() => ({
  failFleetCleanup: false,
  residualDirectory: undefined,
  cleanupError: new Error('conformance upload cleanup failed'),
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

afterEach(assertHarnessFailuresConsumed);

describePlainWorkerConformance('Wrangler loop', (world?: ProviderWorld) => {
  const harness = wranglerHarness(world);
  exportDirectories.add(harness.exportDirectory);
  return harness;
});

describe('provider projection equivalence', () => {
  it('writes identical raw bindings for one shared upload intent', async () => {
    const cliWorld = providerWorld();
    const restWorld = providerWorld();
    const exportStore = new HarnessExportStore();
    const exportDirectory = 'fleet-conformance-export-unused';
    const cliFetch = recordingFetch(restProjection(cliWorld));
    const restFetch = recordingFetch(restProjection(restWorld));
    const cliApi = new WranglerPlainWorkerProvisioningApi({
      runner: cliProjection(cliWorld),
      routeApi: plainOnlyClient(cliFetch, exportStore),
      exportDirectory,
      exportStore,
    });
    const restApi = new CloudflareApiPlainWorkerProvisioningApi({
      client: plainOnlyClient(restFetch, exportStore),
    });
    const intent = uploadIntentForSpec(
      buildPlainWorkerSpec(),
      '00000000-0000-4000-8000-000000000001',
      'initial',
    );
    const fence = mutationFence();

    await expect(cliApi.uploadCandidate(intent, fence)).resolves.toMatchObject({
      status: 'succeeded',
    });
    await expect(restApi.uploadCandidate(intent, fence)).resolves.toMatchObject(
      { status: 'succeeded' },
    );

    expect(
      cliWorld.scripts.get(intent.scriptName)?.versions[0]?.bindings,
    ).toEqual(restWorld.scripts.get(intent.scriptName)?.versions[0]?.bindings);
  });

  it('does not apply public-access config during a staged upload', async () => {
    const world = providerWorld();
    const currentSpec = initialSpec();
    const targetSpec = migrationSpec();
    seedWorkerFromSpec(world, { spec: currentSpec });
    const script = world.scripts.get(currentSpec.scriptName);
    const database = world.databases.find(
      ({ name }) => name === currentSpec.databaseName,
    );
    if (!script || !database)
      throw new Error('ready Worker seed is incomplete');
    script.subdomain = { enabled: false, previewsEnabled: false };
    const harness = wranglerHarness(world);
    exportDirectories.add(harness.exportDirectory);

    await harness.backend.deployWorker(
      targetSpec,
      { id: database.databaseId, name: database.name, created: false },
      sharedSecrets,
      undefined,
      mutationFence(),
      undefined,
    );

    expect(script.subdomain).toEqual({
      enabled: false,
      previewsEnabled: false,
    });
  });

  it('serves maintenance only from the exact control origin', async () => {
    const world = providerWorld();
    const spec = buildPlainWorkerSpec();
    seedWorkerFromSpec(world, { spec });
    world.customDomains.push({
      id: 'domain-1',
      hostname: spec.routeHostname,
      service: spec.scriptName,
    });
    const projected = restProjection(world);

    const exact = await projected({
      method: 'GET',
      url: `${world.maintenanceOrigin}/admin/maintenance-status`,
      body: undefined,
      headers: new Headers(),
      redirect: undefined,
    });
    const decoy = await projected({
      method: 'GET',
      url: `${world.routeOrigin}/admin/maintenance-status`,
      body: undefined,
      headers: new Headers(),
      redirect: undefined,
    });

    expect(exact.status).toBe(200);
    await expect(exact.json()).resolves.toMatchObject({
      deploymentSpecDigest: deploymentSpecDigest(spec),
    });
    expect(decoy.status).toBe(403);
  });
});
