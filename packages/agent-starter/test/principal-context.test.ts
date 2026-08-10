// SPDX-License-Identifier: Apache-2.0

import { approvalStoreFactoryFor } from '@proofoftech/flowsafe/host-kit';
import { describe, expect, it } from 'vitest';

import {
  contextForPrincipal,
  contextForResourceOwner,
} from '../src/principal-context.js';

function resourceDatabase(): Env['DB'] {
  const owners = new Map<string, { owner_kind: string; owner_id: string }>();
  return {
    prepare: (query: string) => {
      let values: unknown[] = [];
      const statement = {
        bind: (...bound: unknown[]) => {
          values = bound;
          return statement;
        },
        run: async () => {
          if (
            query.includes('INSERT OR IGNORE INTO flowsafe_resource_owners')
          ) {
            const [kind, resourceId, ownerKind, ownerId] = values as string[];
            const key = `${kind}:${resourceId}`;
            if (!owners.has(key)) {
              owners.set(key, {
                owner_kind: ownerKind as string,
                owner_id: ownerId as string,
              });
            }
          }
          return { success: true };
        },
        first: async () => {
          const [kind, resourceId] = values as string[];
          return owners.get(`${kind}:${resourceId}`) ?? null;
        },
      };
      return statement;
    },
  } as unknown as Env['DB'];
}

describe('starter resource-owner inheritance', () => {
  it('assigns a signal-woken run to the registered thread owner', async () => {
    const db = resourceDatabase();
    const env = { DB: db, DEPLOYMENT_TENANT: 'acme' } as Env;
    const owner = contextForPrincipal(env, {
      kind: 'human',
      id: 'operator-1',
      role: 'operator',
    });
    await owner.claimResource('thread', 'thread-1');

    const automation = await contextForResourceOwner(
      env,
      'thread',
      'thread-1',
      'signal-wake',
    );
    await automation.claimResource('run', 'run-1');

    await expect(
      approvalStoreFactoryFor(db).resources().owner('run', 'run-1'),
    ).resolves.toEqual({ kind: 'human', id: 'operator-1' });
  });

  it('refuses inheritance from an unregistered source resource', async () => {
    const env = {
      DB: resourceDatabase(),
      DEPLOYMENT_TENANT: 'acme',
    } as Env;

    await expect(
      contextForResourceOwner(env, 'thread', 'missing', 'signal-wake'),
    ).rejects.toThrow("thread 'missing' has no registered owner");
  });
});
