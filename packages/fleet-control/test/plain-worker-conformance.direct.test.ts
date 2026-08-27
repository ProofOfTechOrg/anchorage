// SPDX-License-Identifier: Apache-2.0

import { afterEach, describe, expect, it } from 'vitest';
import { provisionDeployment } from '../src/provision.js';
import {
  assertHarnessFailuresConsumed,
  buildPlainWorkerSpec,
  captureFailure,
  causeChain,
  directHarness,
  errorChain,
  hostileCauseProxy,
  initialSpec,
  malformedErrorsBody,
  migrationSpec,
  routeAttestation,
  sharedSecrets,
  throwingConstructorError,
} from './fixtures/plain-worker-harnesses.js';
import type { ProviderFailure } from './fixtures/provider-world.js';
import { describePlainWorkerConformance } from './plain-worker-backend-conformance.js';

afterEach(assertHarnessFailuresConsumed);

// The direct harness creates no Wrangler scratch directory or fs mock state.
describePlainWorkerConformance('direct Cloudflare API', directHarness);

it('settles an initial upload failure at the REST script request when selected', async () => {
  const harness = directHarness();
  const spec = buildPlainWorkerSpec();
  harness.world.failNext('uploadCandidate', {
    dispatched: true,
    at: 'script',
  });

  const rejection = await captureFailure(
    provisionDeployment({
      backend: harness.backend,
      store: harness.store,
      spec,
      secrets: sharedSecrets,
      initialExecutionFenceState: 'open',
      clock: () => 1_000,
      routeAttestation,
    }),
  );

  expect(errorChain(rejection)).toContain(
    `reconciled Worker upload for '${spec.scriptName}' did not converge public access`,
  );
});

async function stagedUploadFailure(failure: ProviderFailure) {
  const harness = directHarness();
  const currentSpec = initialSpec();
  const targetSpec = migrationSpec();
  const ready = await provisionDeployment({
    backend: harness.backend,
    store: harness.store,
    spec: currentSpec,
    secrets: sharedSecrets,
    initialExecutionFenceState: 'open',
    clock: () => 1_000,
    routeAttestation,
  });
  const database = harness.world.databases.find(
    ({ databaseId }) => databaseId === ready.record.databaseId,
  );
  if (!database) throw new Error('ready database disappeared');
  harness.world.failNext('uploadCandidate', failure);
  const rejection = await captureFailure(
    harness.backend.deployWorker(
      targetSpec,
      { id: database.databaseId, name: database.name, created: false },
      sharedSecrets,
      undefined,
      {
        mutationLeaseTtlMs: 15 * 60_000,
        assertOwned: async () => {},
      },
      undefined,
    ),
  );
  return { rejection, harness };
}

describe('sanitizer boundary through the shared fixtures', () => {
  // This wrapper-local suite is outside the shared body, so rejection-shape
  // assertions do not constrain the cross-backend conformance contract.
  it('redacts secret-bearing errors and rebuilds their cause chain', async () => {
    const injected = throwingConstructorError(sharedSecrets.deploymentIdentity);
    const { rejection } = await stagedUploadFailure({
      dispatched: true,
      duplicate: true,
      error: injected,
    });
    const serialized = `${JSON.stringify(rejection)} ${errorChain(rejection)}`;

    expect(serialized).not.toContain(sharedSecrets.deploymentIdentity);
    expect(serialized).not.toContain(sharedSecrets.maintenanceAdmin);
    expect(causeChain(rejection).some((cause) => cause === injected)).toBe(
      false,
    );
    for (const cause of causeChain(rejection)) {
      if (!(cause instanceof Error)) continue;
      expect(cause.name).toMatch(/^(?:[A-Za-z][A-Za-z0-9]*Error|unknown)$/u);
    }
  });

  it('drops a hostile proxy carried as an error cause', async () => {
    const proxy = hostileCauseProxy();
    const { rejection } = await stagedUploadFailure({
      dispatched: true,
      duplicate: true,
      error: new Error('hostile cause carrier', { cause: proxy }),
    });

    expect(rejection).toBeInstanceOf(Error);
    expect(causeChain(rejection).some((cause) => cause === proxy)).toBe(false);
  });

  it('normalizes a malformed provider errors body', async () => {
    const { rejection } = await stagedUploadFailure({
      dispatched: true,
      duplicate: true,
      response: malformedErrorsBody(),
    });
    const providerError = causeChain(rejection).find(
      (cause) =>
        cause instanceof Error && Array.isArray(Reflect.get(cause, 'errors')),
    );

    expect(providerError).toBeDefined();
    expect(Reflect.get(providerError as object, 'errors')).toEqual([]);
    expect(errorChain(rejection)).not.toContain('not an array');
  });

  it('surfaces the SDK timeout as one constant cause level', async () => {
    const { rejection } = await stagedUploadFailure({
      dispatched: true,
      duplicate: true,
      error: new Error('transport timed out'),
    });
    const chain = errorChain(rejection);

    // The SDK classifies timeout text in client.mjs:386-388 and constructs
    // APIConnectionTimeoutError without a cause in core/error.mjs:81-85.
    expect(chain.match(/Request timed out\./gu)).toHaveLength(1);
    expect(chain).not.toContain('transport timed out');
  });
});
