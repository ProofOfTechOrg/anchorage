// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import { d1DatabaseLike, openSqlite } from '../../test-support/sqlite.js';
import {
  canonicalResourceOwner,
  createResourceOwnershipSchema,
  D1ResourceOwnershipStore,
  InMemoryResourceOwnershipStore,
  principalMayAccess,
  principalOwner,
  type RecoverableResourceOwnershipStore,
  type ResourceOwnershipDatabase,
  requireCommonResourceOwner,
} from './resource-ownership.js';

const OPAL = { kind: 'human', id: 'opal' } as const;
const EVE = { kind: 'human', id: 'eve' } as const;

function stores(): Array<[string, RecoverableResourceOwnershipStore]> {
  const db = d1DatabaseLike(openSqlite()) as ResourceOwnershipDatabase;
  return [
    ['memory', new InMemoryResourceOwnershipStore()],
    ['d1', new D1ResourceOwnershipStore(db)],
  ];
}

describe('canonicalResourceOwner', () => {
  it.each([
    'human',
    'service',
    'agent',
    'system',
  ] as const)("accepts the '%s' owner kind", (kind) => {
    expect(canonicalResourceOwner({ kind, id: `${kind}-owner` })).toEqual({
      kind,
      id: `${kind}-owner`,
    });
  });

  it('accepts an owner id exactly at the principal-id bound', () => {
    const id = 'o'.repeat(200);
    expect(canonicalResourceOwner({ kind: 'human', id })).toEqual({
      kind: 'human',
      id,
    });
  });

  it.each([
    ['an invalid kind', { kind: 'operator', id: 'owner' }],
    ['an id over the bound', { kind: 'human', id: 'o'.repeat(201) }],
    ['an all-whitespace id', { kind: 'human', id: '   ' }],
    ['a control character', { kind: 'human', id: 'owner\u0000forged' }],
  ])('rejects %s', (_label, owner) => {
    expect(() => canonicalResourceOwner(owner)).toThrow(
      'resource owner must be a valid execution principal',
    );
  });
});

describe.each(stores())('%s resource ownership store', (_name, store) => {
  it('claims atomically and never re-homes an opaque id', async () => {
    expect(await store.claim('run', 'run-1', OPAL)).toBe(true);
    expect(await store.claim('run', 'run-1', OPAL)).toBe(true);
    expect(await store.claim('run', 'run-1', EVE)).toBe(false);
    expect(await store.owner('run', 'run-1')).toEqual(OPAL);
  });

  it('keeps resource kinds distinct and returns undefined for missing ids', async () => {
    await store.claim('thread', 'opaque-1', OPAL);
    await store.claim('resource', 'opaque-1', EVE);
    expect(await store.owner('thread', 'opaque-1')).toEqual(OPAL);
    expect(await store.owner('resource', 'opaque-1')).toEqual(EVE);
    expect(await store.owner('schedule', 'missing')).toBeUndefined();
  });

  it('rejects invalid kinds and non-string ids consistently', async () => {
    await expect(
      store.claim('invalid' as never, 'run-1', OPAL),
    ).rejects.toThrow('resource kind is invalid');
    await expect(store.owner('run', 123 as unknown as string)).rejects.toThrow(
      'resource id must be path-safe',
    );
    await expect(
      store.release('invalid' as never, 'run-1', OPAL),
    ).rejects.toThrow('resource kind is invalid');
    await expect(
      store.reserveAll(
        [{ kind: 'invalid' as never, resourceId: 'run-1' }],
        OPAL,
        'token-1',
      ),
    ).rejects.toThrow('resource kind is invalid');
    await expect(
      store.settleReservation('token-1', [
        { kind: 'invalid' as never, resourceId: 'run-1' },
      ]),
    ).rejects.toThrow('resource kind is invalid');
  });

  it('rejects accessor-backed and inherited authority fields', async () => {
    const inheritedOwner = Object.create(OPAL) as typeof OPAL;
    const accessorClaim = Object.defineProperties(
      {},
      {
        kind: { get: () => 'run', enumerable: true },
        resourceId: { value: 'run-accessor', enumerable: true },
      },
    ) as { kind: 'run'; resourceId: string };

    await expect(
      store.claim('run', 'run-inherited-owner', inheritedOwner),
    ).rejects.toThrow('resource owner must be a valid execution principal');
    await expect(
      store.reserveAll([accessorClaim], OPAL, 'token-accessor'),
    ).rejects.toThrow('resource kind is invalid');
  });
});

describe.each(stores())('%s resource reservation recovery', (_name, store) => {
  it('commits all claims created by an attempt', async () => {
    const claims = [
      { kind: 'thread', resourceId: 'reserve-thread-commit' },
      { kind: 'resource', resourceId: 'reserve-resource-commit' },
      { kind: 'run', resourceId: 'reserve-run-commit' },
    ] as const;

    expect(await store.reserveAll(claims, OPAL, 'token-commit')).toBe(true);
    expect(await store.reserveAll(claims, OPAL, 'token-commit')).toBe(true);
    for (const claim of claims) {
      expect(await store.owner(claim.kind, claim.resourceId)).toBeUndefined();
      expect(await store.claim(claim.kind, claim.resourceId, OPAL)).toBe(false);
      expect(await store.release(claim.kind, claim.resourceId, OPAL)).toBe(
        false,
      );
    }
    await store.settleReservation('token-commit', []);
    await store.settleReservation('token-commit', []);

    for (const claim of claims) {
      expect(await store.owner(claim.kind, claim.resourceId)).toEqual(OPAL);
    }
  });

  it('rolls back only claims created by the attempt', async () => {
    const existing = {
      kind: 'thread',
      resourceId: 'reserve-thread-existing',
    } as const;
    const created = { kind: 'run', resourceId: 'reserve-run-created' } as const;
    await store.claim(existing.kind, existing.resourceId, OPAL);

    expect(
      await store.reserveAll(
        [existing, created],
        OPAL,
        'token-selective-rollback',
      ),
    ).toBe(true);
    await store.settleReservation('token-selective-rollback', [
      existing,
      created,
    ]);

    expect(await store.owner(existing.kind, existing.resourceId)).toEqual(OPAL);
    expect(await store.owner(created.kind, created.resourceId)).toBeUndefined();
  });

  it('inserts nothing when one claim belongs to another owner', async () => {
    const conflict = {
      kind: 'resource',
      resourceId: 'reserve-resource-conflict',
    } as const;
    const newClaim = {
      kind: 'run',
      resourceId: 'reserve-run-conflict-batch',
    } as const;
    await store.claim(conflict.kind, conflict.resourceId, EVE);

    expect(
      await store.reserveAll(
        [newClaim, conflict],
        OPAL,
        'token-owner-conflict',
      ),
    ).toBe(false);

    expect(
      await store.owner(newClaim.kind, newClaim.resourceId),
    ).toBeUndefined();
    expect(await store.owner(conflict.kind, conflict.resourceId)).toEqual(EVE);
  });

  it('does not adopt another in-flight reservation for the same owner', async () => {
    const claim = {
      kind: 'run',
      resourceId: 'reserve-run-token-conflict',
    } as const;
    expect(await store.reserveAll([claim], OPAL, 'token-first')).toBe(true);

    expect(await store.reserveAll([claim], OPAL, 'token-second')).toBe(false);
    await store.settleReservation('token-second', [claim]);
    expect(await store.owner(claim.kind, claim.resourceId)).toBeUndefined();

    await store.settleReservation('token-first', [claim]);
    expect(await store.owner(claim.kind, claim.resourceId)).toBeUndefined();
  });

  it('selectively releases abandoned claims and commits retained claims', async () => {
    const thread = {
      kind: 'thread',
      resourceId: 'reserve-thread-selective',
    } as const;
    const resource = {
      kind: 'resource',
      resourceId: 'reserve-resource-selective',
    } as const;
    const run = { kind: 'run', resourceId: 'reserve-run-selective' } as const;
    expect(
      await store.reserveAll([thread, resource, run], OPAL, 'token-selective'),
    ).toBe(true);

    await store.settleReservation('token-selective', [run]);

    expect(await store.owner(thread.kind, thread.resourceId)).toEqual(OPAL);
    expect(await store.owner(resource.kind, resource.resourceId)).toEqual(OPAL);
    expect(await store.owner(run.kind, run.resourceId)).toBeUndefined();
  });

  it('validates settlement tokens and duplicate releases', async () => {
    await expect(
      store.reserveAll([], OPAL, 123 as unknown as string),
    ).rejects.toThrow('resource id must be path-safe');
    await expect(
      store.settleReservation(123 as unknown as string, []),
    ).rejects.toThrow('resource id must be path-safe');
    await expect(
      store.settleReservation('token-valid', [
        { kind: 'run', resourceId: 'run-duplicate' },
        { kind: 'run', resourceId: 'run-duplicate' },
      ]),
    ).rejects.toThrow("duplicate resource claim 'run:run-duplicate'");
  });
});

describe('D1ResourceOwnershipStore schema gate', () => {
  it('memoizes direct-constructor schema setup across operations', async () => {
    const backing = d1DatabaseLike(openSqlite()) as ResourceOwnershipDatabase;
    let schemaWrites = 0;
    const db: ResourceOwnershipDatabase = {
      prepare(query) {
        if (query.startsWith('CREATE TABLE')) schemaWrites += 1;
        return backing.prepare(query);
      },
    };
    const store = new D1ResourceOwnershipStore(db);

    await store.claim('run', 'run-1', OPAL);
    await store.owner('run', 'run-1');
    await store.owner('run', 'run-1');

    expect(schemaWrites).toBe(1);
  });

  it('snapshots owner and claims before awaiting schema readiness', async () => {
    const db = d1DatabaseLike(openSqlite()) as ResourceOwnershipDatabase;
    await createResourceOwnershipSchema(db);
    let releaseReady: () => void = () => undefined;
    const ready = new Promise<void>((resolve) => {
      releaseReady = resolve;
    });
    const store = new D1ResourceOwnershipStore(db, {
      ready: () => ready,
    });
    const owner = { kind: 'human' as const, id: 'opal' };
    const claims = [{ kind: 'run' as const, resourceId: 'run-original' }];

    const reservation = store.reserveAll(claims, owner, 'token-original');
    owner.id = 'eve';
    claims[0] = { kind: 'run', resourceId: 'run-mutated' };
    releaseReady();

    await expect(reservation).resolves.toBe(true);
    await store.settleReservation('token-original', []);
    await expect(store.owner('run', 'run-original')).resolves.toEqual(OPAL);
    await expect(store.owner('run', 'run-mutated')).resolves.toBeUndefined();
  });
});

describe('resource access policy', () => {
  it('allows owners, read reviewers, admins, and trusted automation only', () => {
    expect(
      principalMayAccess(
        { kind: 'human', id: 'opal', role: 'operator' },
        OPAL,
        'write',
      ),
    ).toBe(true);
    expect(
      principalMayAccess(
        { kind: 'human', id: 'ray', role: 'reviewer' },
        OPAL,
        'read',
      ),
    ).toBe(true);
    expect(
      principalMayAccess(
        { kind: 'human', id: 'ray', role: 'reviewer' },
        OPAL,
        'write',
      ),
    ).toBe(false);
    expect(
      principalMayAccess(
        { kind: 'human', id: 'vic', role: 'viewer' },
        OPAL,
        'read',
      ),
    ).toBe(true);
    expect(
      principalMayAccess(
        { kind: 'human', id: 'eve', role: 'operator' },
        OPAL,
        'read',
      ),
    ).toBe(false);
    expect(
      principalMayAccess(
        { kind: 'human', id: 'ada', role: 'admin' },
        OPAL,
        'write',
      ),
    ).toBe(true);
    expect(
      principalMayAccess(
        { kind: 'system', id: 'scheduler', purpose: 'schedule.fire' },
        OPAL,
        'write',
      ),
    ).toBe(false);
  });

  it('canonicalizes principal and owner values at the exported policy boundary', () => {
    const principal = {
      kind: 'human' as const,
      id: 'opal',
      role: 'operator' as const,
    };
    const owner = { kind: 'human' as const, id: 'opal' };
    const snapshot = principalOwner(principal);

    principal.id = 'eve';
    owner.id = 'eve';

    expect(snapshot).toEqual(OPAL);
    expect(Object.isFrozen(snapshot)).toBe(true);
    expect(principalMayAccess(principal, owner, 'write')).toBe(true);
    expect(() =>
      principalMayAccess(principal, owner, 'delete' as never),
    ).toThrow('resource access mode is invalid');
  });

  it('snapshots common-owner claims before the first store await', async () => {
    let releaseFirst: () => void = () => undefined;
    const firstRead = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const seen: string[] = [];
    const resources = {
      async owner(
        _kind: 'run' | 'thread' | 'resource' | 'schedule',
        resourceId: string,
      ) {
        seen.push(resourceId);
        if (seen.length === 1) await firstRead;
        return OPAL;
      },
    };
    const claims = [
      { kind: 'thread' as const, resourceId: 'thread-original' },
      { kind: 'resource' as const, resourceId: 'resource-original' },
    ];

    const pending = requireCommonResourceOwner(resources, claims);
    claims[1] = { kind: 'resource', resourceId: 'resource-mutated' };
    releaseFirst();

    await expect(pending).resolves.toEqual(OPAL);
    expect(seen).toEqual(['thread-original', 'resource-original']);
  });
});
