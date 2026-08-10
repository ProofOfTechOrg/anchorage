// SPDX-License-Identifier: Apache-2.0
/// <reference types="@cloudflare/vitest-pool-workers/types" />
import { reset } from 'cloudflare:test';
import { env } from 'cloudflare:workers';
import { beforeEach, describe, expect, it } from 'vitest';

import {
  D1ResourceOwnershipStore,
  type ResourceOwner,
} from './approval-api/resource-ownership.js';
import {
  assertDeploymentIdentity,
  readDeploymentIdentity,
  seedDeploymentIdentity,
} from './do-runner/deployment-identity.js';

interface TestBindings {
  DB: D1Database;
}

const db = (): D1Database => (env as unknown as TestBindings).DB;

describe('FlowSafe Cloudflare fidelity', () => {
  beforeEach(reset);

  it('provisions one deployment identity concurrently and refuses re-homing', async () => {
    expect(await readDeploymentIdentity(db())).toBeUndefined();

    await Promise.all([
      seedDeploymentIdentity(db(), 'acme'),
      seedDeploymentIdentity(db(), 'acme'),
    ]);

    await expect(assertDeploymentIdentity(db(), 'acme')).resolves.toBe(
      undefined,
    );
    await expect(seedDeploymentIdentity(db(), 'globex')).rejects.toThrow(
      /already belongs to deployment 'acme'/,
    );
    expect(await readDeploymentIdentity(db())).toBe('acme');
  });

  it('isolates D1 per test and resolves concurrent resource claims to one owner', async () => {
    expect(await readDeploymentIdentity(db())).toBeUndefined();
    await seedDeploymentIdentity(db(), 'acme');

    const store = new D1ResourceOwnershipStore(db());
    const contenders: readonly ResourceOwner[] = [
      { kind: 'human', id: 'alice' },
      { kind: 'human', id: 'bob' },
    ];
    const outcomes = await Promise.all(
      contenders.map(async (owner) => ({
        claimed: await store.claim('run', 'run-cas', owner),
        owner,
      })),
    );

    const winners = outcomes.filter(({ claimed }) => claimed);
    expect(winners).toHaveLength(1);
    expect(await store.owner('run', 'run-cas')).toEqual(winners[0]?.owner);
  });
});
