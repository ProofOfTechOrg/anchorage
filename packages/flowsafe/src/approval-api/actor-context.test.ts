// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';
import { DEPLOYMENT_IDENTITY_HEADER } from '../do-runner/deployment-identity.js';
import { EXECUTION_PRINCIPAL_HEADER } from '../do-runner/execution-principal-header.js';
import {
  type ActorContext,
  ActorResolutionError,
  type CreateActorResolverOptions,
  createActorResolver,
} from './actor-context.js';
import type { ApprovalActor } from './contract.js';
import { InMemoryApprovalStoreFactory } from './store-factory.js';

async function resolveContext(
  allowSelfDecision?: CreateActorResolverOptions['allowSelfDecision'],
): Promise<ActorContext> {
  const resolve = createActorResolver({
    authenticate: () => ({ id: 'actor-1', role: 'admin' }),
    storeFactory: new InMemoryApprovalStoreFactory(),
    deploymentTag: 'acme',
    buildService: () => {
      throw new Error('service() untouched by these tests');
    },
    newRunId: () => 'uuid-1',
    allowSelfDecision,
  });
  const context = await resolve(new Request('https://host.example/'));
  if (!context) throw new Error('expected an authenticated actor');
  return context;
}

describe('ActorContext', () => {
  it('mints path-safe opaque ids and preserves the verified deployment tag', async () => {
    const context = await resolveContext();

    expect(context.newRunId()).toBe('uuid-1');
    expect(context.newThreadId()).toBe('uuid-1');
    expect(context.resourceIdFromKey('user-1')).toBe('user-1');
    expect(context.deploymentTag).toBe('acme');
    expect(() => context.resourceIdFromKey('a/b')).toThrow(
      /PATH_SAFE_ID_PATTERN/,
    );
  });

  it('rejects a non-path-safe host run-id generator', async () => {
    const resolve = createActorResolver({
      authenticate: () => ({ id: 'actor-1', role: 'admin' }),
      storeFactory: new InMemoryApprovalStoreFactory(),
      buildService: () => {
        throw new Error('service() untouched by this test');
      },
      newRunId: () => 'a/b',
    });
    const context = await resolve(new Request('https://host.example/'));
    if (!context) throw new Error('expected an authenticated actor');

    expect(() => context.newRunId()).toThrow(/PATH_SAFE_ID_PATTERN/);
  });

  it('reports only configured separation-of-duties exemptions', async () => {
    const defaultContext = await resolveContext();
    const exemptContext = await resolveContext({ roles: ['admin'] });

    expect(defaultContext.canSelfDecide('admin')).toBe(false);
    expect(exemptContext.canSelfDecide('admin')).toBe(true);
    expect(exemptContext.canSelfDecide('reviewer')).toBe(false);
    expect(exemptContext.canSelfDecide('builder')).toBe(false);
  });
});

describe('createActorResolver boundary validation', () => {
  it('snapshots the authenticated actor before lazy service construction', async () => {
    const source: {
      id: string;
      role: ApprovalActor['role'];
    } = { id: 'actor-1', role: 'operator' };
    const buildService = vi.fn(
      (_store, actor) =>
        ({ actorSeen: actor }) as unknown as ReturnType<
          CreateActorResolverOptions['buildService']
        >,
    );
    const resolve = createActorResolver({
      authenticate: () => source,
      storeFactory: new InMemoryApprovalStoreFactory(),
      buildService,
    });

    const context = await resolve(new Request('https://host.example/'));
    if (!context) throw new Error('expected an authenticated actor');
    source.id = 'mutated';
    source.role = 'admin';
    context.service();

    expect(context.actor).toEqual({ id: 'actor-1', role: 'operator' });
    expect(context.principal).toEqual({
      kind: 'human',
      id: 'actor-1',
      role: 'operator',
    });
    expect(buildService).toHaveBeenCalledWith(expect.anything(), {
      id: 'actor-1',
      role: 'operator',
    });
    expect(Object.isFrozen(context.actor)).toBe(true);
  });

  it('refuses accessor-backed authenticated claims', async () => {
    const resolve = createActorResolver({
      authenticate: () =>
        Object.defineProperties(
          {},
          {
            id: { get: () => 'actor-1', enumerable: true },
            role: { get: () => 'admin', enumerable: true },
          },
        ) as never,
      storeFactory: new InMemoryApprovalStoreFactory(),
      buildService: () => {
        throw new Error('service must not be built');
      },
    });

    await expect(
      resolve(new Request('https://host.example/')),
    ).rejects.toBeInstanceOf(ActorResolutionError);
  });

  it.each([
    ['empty actor id', { id: '', role: 'admin' }],
    ['whitespace actor id', { id: '   ', role: 'admin' }],
    ['overlong actor id', { id: 'a'.repeat(201), role: 'admin' }],
    [
      'header-invalid actor id',
      { id: 'actor-1\r\nx-forged: yes', role: 'admin' },
    ],
    ['unknown actor role', { id: 'actor-1', role: 'root' }],
  ])('rejects an %s from a custom authenticator', async (_label, actor) => {
    const resolve = createActorResolver({
      authenticate: () => actor as never,
      storeFactory: new InMemoryApprovalStoreFactory(),
      buildService: () => {
        throw new Error('service must not be built');
      },
    });

    await expect(
      resolve(new Request('https://host.example/')),
    ).rejects.toBeInstanceOf(ActorResolutionError);
  });

  it.each([
    DEPLOYMENT_IDENTITY_HEADER,
    EXECUTION_PRINCIPAL_HEADER,
    'x-flowsafe-actor',
    'x-flowsafe-role',
    'x-flowsafe-tenant',
  ])('refuses an inbound server identity header %s', async (header) => {
    const authenticate = vi.fn(() => ({
      id: 'actor-1',
      role: 'admin' as const,
    }));
    const resolve = createActorResolver({
      authenticate,
      storeFactory: new InMemoryApprovalStoreFactory(),
      buildService: () => {
        throw new Error('service must not be built');
      },
    });
    const request = new Request('https://host.example/', {
      headers: { [header.toUpperCase()]: 'forged' },
    });

    await expect(resolve(request)).rejects.toBeInstanceOf(ActorResolutionError);
    expect(authenticate).not.toHaveBeenCalled();
  });
});
