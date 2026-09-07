// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';
import { DEPLOYMENT_IDENTITY_HEADER } from '../do-runner/deployment-identity.js';
import {
  InvalidMutationEpochError,
  MUTATION_EPOCH_HEADER,
} from '../do-runner/execution-admission.js';
import { EXECUTION_PRINCIPAL_HEADER } from '../do-runner/execution-principal-header.js';
import {
  type ActorContext,
  ActorResolutionError,
  type CreateActorResolverOptions,
  captureActorContext,
  createActorResolver,
  createPrincipalActorContext,
  withRegisteredResourceOwner,
} from './actor-context.js';
import type { ApprovalActor } from './contract.js';
import { type ExecutionPrincipal, humanPrincipal } from './principal.js';
import type { ResourceOwner } from './resource-ownership.js';
import { ApprovalService } from './service.js';
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

const contextMethods = [
  'service',
  'newRunId',
  'newThreadId',
  'resourceIdFromKey',
  'claimResource',
  'releaseResource',
  'resourceOwnerFor',
  'canAccessResource',
  'canSelfDecide',
] as const;

class ReceiverContext implements ActorContext {
  actor: ApprovalActor = { id: 'original-actor', role: 'operator' };
  principal: ExecutionPrincipal = {
    kind: 'human',
    id: 'principal-1',
    role: 'admin',
  };
  mutationEpoch = 2;
  deploymentTag = 'original-tag';
  resourceOwner: ResourceOwner = { kind: 'service', id: 'original-owner' };
  readonly calls: string[] = [];
  #service = new ApprovalService({
    store: new InMemoryApprovalStoreFactory().store(),
    executionFence: 'none',
  });
  service() {
    this.calls.push('service');
    return this.#service;
  }
  newRunId() {
    this.calls.push('newRunId');
    return this.#service ? 'original-run' : '';
  }
  newThreadId() {
    this.calls.push('newThreadId');
    return this.#service ? 'original-thread' : '';
  }
  resourceIdFromKey(key: string) {
    this.calls.push('resourceIdFromKey');
    return this.#service ? key : '';
  }
  async claimResource() {
    this.calls.push('claimResource');
    void this.#service;
  }
  async releaseResource() {
    this.calls.push('releaseResource');
    void this.#service;
  }
  async resourceOwnerFor() {
    this.calls.push('resourceOwnerFor');
    return this.#service
      ? { kind: 'human' as const, id: 'registered' }
      : undefined;
  }
  async canAccessResource() {
    this.calls.push('canAccessResource');
    return !!this.#service;
  }
  canSelfDecide() {
    this.calls.push('canSelfDecide');
    return !!this.#service;
  }
}

async function captureWithoutFault(source: ActorContext) {
  const result = (async () => captureActorContext(source))();
  await expect(result).resolves.toBeDefined();
  return result;
}

async function observeMethods(context: ActorContext) {
  const results: unknown[] = [];
  for (const method of contextMethods) {
    try {
      switch (method) {
        case 'service':
          results.push(context.service() instanceof ApprovalService);
          break;
        case 'newRunId':
          results.push(context.newRunId());
          break;
        case 'newThreadId':
          results.push(context.newThreadId());
          break;
        case 'resourceIdFromKey':
          results.push(context.resourceIdFromKey('key'));
          break;
        case 'claimResource':
          results.push(await context.claimResource('run', 'r'));
          break;
        case 'releaseResource':
          results.push(await context.releaseResource('run', 'r'));
          break;
        case 'resourceOwnerFor':
          results.push(await context.resourceOwnerFor('run', 'r'));
          break;
        case 'canAccessResource':
          results.push(await context.canAccessResource('run', 'r', 'write'));
          break;
        case 'canSelfDecide':
          results.push(context.canSelfDecide('admin'));
          break;
      }
    } catch (error) {
      results.push(error);
    }
  }
  return results;
}

const methodResults = [
  true,
  'original-run',
  'original-thread',
  'key',
  undefined,
  undefined,
  { kind: 'human', id: 'registered' },
  true,
  true,
];

function deferredContext() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

describe('C context capture', () => {
  it.each([
    undefined,
    0,
    Number.MAX_SAFE_INTEGER,
  ])('C constructors capture one epoch observation (%s)', async (epoch) => {
    const storeFactory = new InMemoryApprovalStoreFactory();
    const buildService = vi.fn(
      () =>
        new ApprovalService({
          store: storeFactory.store(),
          executionFence: 'none',
        }),
    );
    const authenticate = vi.fn(() => ({
      id: 'actor-1',
      role: 'admin' as const,
    }));
    const readEpoch = vi.fn(() => epoch);
    const options = {
      get mutationEpoch() {
        return readEpoch();
      },
      authenticate,
      storeFactory,
      buildService,
    };
    const resolve = createActorResolver(options);
    expect(readEpoch).toHaveBeenCalledTimes(1);
    const context = await resolve(new Request('https://host/'));
    expect(context?.mutationEpoch).toBe(epoch);
    expect(readEpoch).toHaveBeenCalledTimes(1);
    const directReads = vi.fn(() => epoch);
    const direct = createPrincipalActorContext({
      storeFactory,
      buildService,
      get mutationEpoch() {
        return directReads();
      },
      principal: humanPrincipal({ id: 'actor-1', role: 'admin' }),
    });
    expect(direct.mutationEpoch).toBe(epoch);
    expect(directReads).toHaveBeenCalledTimes(1);
    expect(readEpoch).toHaveBeenCalledTimes(1);
    expect(buildService).not.toHaveBeenCalled();
    expect(direct.service()).toBe(direct.service());
    expect(buildService).toHaveBeenCalledOnce();
    expect(directReads).toHaveBeenCalledTimes(1);
  });

  it.each([
    null,
    '2',
    true,
    1.5,
    -1,
    Number.NaN,
    Infinity,
    Number.MAX_SAFE_INTEGER + 1,
  ])('C malformed constructor epoch refuses before effects (%s)', (epoch) => {
    const storeFactory = new InMemoryApprovalStoreFactory();
    const resources = vi.spyOn(storeFactory, 'resources');
    const store = vi.spyOn(storeFactory, 'store');
    const authenticate = vi.fn();
    const buildService = vi.fn();
    const readEpoch = vi.fn(() => epoch);
    const options = {
      get mutationEpoch() {
        return readEpoch();
      },
      storeFactory,
      authenticate,
      buildService,
    };
    expect(() => createActorResolver(options)).toThrow(
      InvalidMutationEpochError,
    );
    const directReads = vi.fn(() => epoch);
    expect(() =>
      createPrincipalActorContext({
        storeFactory,
        buildService,
        get mutationEpoch() {
          return directReads();
        },
        principal: humanPrincipal({ id: 'a', role: 'admin' }),
      }),
    ).toThrow(InvalidMutationEpochError);
    expect(readEpoch).toHaveBeenCalledTimes(1);
    expect(directReads).toHaveBeenCalledTimes(1);
    expect(resources).not.toHaveBeenCalled();
    expect(store).not.toHaveBeenCalled();
    expect(authenticate).not.toHaveBeenCalled();
    expect(buildService).not.toHaveBeenCalled();
  });

  it.each(
    [undefined, 0, Number.MAX_SAFE_INTEGER].flatMap((epoch) =>
      (['alternate', 'second-throw'] as const).map((mode) => ({ epoch, mode })),
    ),
  )('C direct principal context captures its epoch exactly once ($epoch, $mode)', ({
    epoch,
    mode,
  }) => {
    const factory = new InMemoryApprovalStoreFactory();
    const build = vi.fn(
      () =>
        new ApprovalService({ store: factory.store(), executionFence: 'none' }),
    );
    const read = vi
      .fn<() => unknown>()
      .mockReturnValueOnce(epoch)
      .mockImplementation(() => {
        if (mode === 'second-throw')
          throw new Error('second direct epoch read');
        return 'replacement';
      });
    const options = {
      principal: humanPrincipal({ id: 'direct', role: 'operator' }),
      storeFactory: factory,
      buildService: build,
      get mutationEpoch() {
        return read();
      },
    };
    const context = createPrincipalActorContext(options);
    expect(read).toHaveBeenCalledTimes(1);
    expect(context.mutationEpoch).toBe(epoch);
    expect(build).not.toHaveBeenCalled();
    Object.defineProperty(options, 'mutationEpoch', {
      get() {
        throw new Error('late direct epoch read');
      },
    });
    expect(context.service()).toBe(context.service());
    expect(build).toHaveBeenCalledOnce();
    expect(context.mutationEpoch).toBe(epoch);
    expect(read).toHaveBeenCalledTimes(1);
  });

  it('C direct principal context preserves a first epoch getter fault without effects', () => {
    const factory = new InMemoryApprovalStoreFactory();
    const resources = vi.spyOn(factory, 'resources');
    const store = vi.spyOn(factory, 'store');
    const buildService = vi.fn();
    const fault = new Error('first direct epoch read');
    let error: unknown;
    try {
      createPrincipalActorContext({
        principal: humanPrincipal({ id: 'direct', role: 'operator' }),
        storeFactory: factory,
        buildService,
        get mutationEpoch() {
          throw fault;
        },
      });
    } catch (cause) {
      error = cause;
    }
    expect(error).toBe(fault);
    expect(resources).not.toHaveBeenCalled();
    expect(store).not.toHaveBeenCalled();
    expect(buildService).not.toHaveBeenCalled();
  });

  it('C resolver keeps its construction epoch through authentication', async () => {
    const hold = deferredContext();
    const entered = deferredContext();
    const options = {
      mutationEpoch: 2,
      storeFactory: new InMemoryApprovalStoreFactory(),
      buildService: vi.fn(),
      authenticate: async () => {
        entered.release();
        await hold.promise;
        return { id: 'a', role: 'admin' as const };
      },
    };
    const resolve = createActorResolver(options);
    const pending = resolve(new Request('https://host/'));
    await entered.promise;
    options.mutationEpoch = 3;
    try {
      hold.release();
      expect((await pending)?.mutationEpoch).toBe(2);
    } finally {
      hold.release();
      await pending;
    }
  });

  it.each([
    '2',
    'forged',
  ])('C rejects the protected epoch header before authentication (%s)', async (value) => {
    const authenticate = vi.fn(() => ({ id: 'a', role: 'admin' as const }));
    const resolve = createActorResolver({
      authenticate,
      storeFactory: new InMemoryApprovalStoreFactory(),
      buildService: vi.fn(),
    });
    await expect(
      resolve(
        new Request('https://host/', {
          headers: { [MUTATION_EPOCH_HEADER.toUpperCase()]: value },
        }),
      ),
    ).rejects.toBeInstanceOf(ActorResolutionError);
    expect(authenticate).not.toHaveBeenCalled();
    expect(
      await resolve(
        new Request('https://host/', { headers: { 'x-ordinary': 'yes' } }),
      ),
    ).toBeDefined();
  });

  it.each([
    'prototype',
    'non-enumerable',
    'own-enumerable',
  ] as const)('C captures all declared ActorContext methods without enumeration', async (shape) => {
    const source = new ReceiverContext();
    const reads = new Map<string, number>();
    if (shape !== 'prototype') {
      for (const key of [
        ...contextMethods,
        'actor',
        'principal',
        'mutationEpoch',
        'deploymentTag',
        'resourceOwner',
      ] as const) {
        const value = source[key];
        Object.defineProperty(source, key, {
          configurable: true,
          enumerable: shape === 'own-enumerable',
          get() {
            const count = (reads.get(key) ?? 0) + 1;
            reads.set(key, count);
            if (count > 1) throw new Error(`second lookup ${key}`);
            return value;
          },
        });
      }
    }
    const captured = await captureWithoutFault(source);
    expect(source.calls).toEqual([]);
    expect(await observeMethods(captured)).toEqual(methodResults);
    expect(source.calls).toEqual(contextMethods);
    for (const count of reads.values()) expect(count).toBe(1);
    expect(Object.isFrozen(source)).toBe(false);
    expect(Object.isFrozen(captured)).toBe(true);
  });

  it.each([
    'prototype',
    'own-enumerable',
  ] as const)('C preserves the original receiver of captured ActorContext methods', async (shape) => {
    const source = new ReceiverContext();
    if (shape === 'own-enumerable') {
      for (const method of contextMethods)
        Object.defineProperty(source, method, {
          value: source[method],
          enumerable: true,
          configurable: true,
        });
    }
    const captured = await captureWithoutFault(source);
    expect(await observeMethods(captured)).toEqual(methodResults);
    expect(source.calls).toEqual(contextMethods);
    expect(Object.isFrozen(source.service)).toBe(false);
    expect(Object.isFrozen(source.actor)).toBe(false);
    expect(Object.isFrozen(source.resourceOwner)).toBe(false);
  });

  it('C never refreshes captured context methods after owner lookup', async () => {
    const source = new ReceiverContext();
    const hold = deferredContext();
    const entered = deferredContext();
    const registered: ResourceOwner = { kind: 'human', id: 'registered' };
    const resources = new InMemoryApprovalStoreFactory().resources();
    const owner = vi.spyOn(resources, 'owner').mockImplementation(async () => {
      entered.release();
      await hold.promise;
      return registered;
    });
    const claim = vi.spyOn(resources, 'claim').mockResolvedValue(true);
    const release = vi.spyOn(resources, 'release').mockResolvedValue(true);
    const pending = withRegisteredResourceOwner(source, resources, [
      { kind: 'run', resourceId: 'r' },
    ]);
    await entered.promise;
    const replacement = vi.fn(() => {
      throw new Error('replacement');
    });
    for (const method of contextMethods)
      Object.defineProperty(source, method, { value: replacement });
    source.actor = { id: 'changed', role: 'admin' };
    source.principal = humanPrincipal(source.actor);
    source.mutationEpoch = 9;
    source.deploymentTag = 'changed';
    source.resourceOwner = { kind: 'human', id: 'changed' };
    try {
      hold.release();
      const captured = await pending;
      expect(captured.actor).toEqual({
        id: 'original-actor',
        role: 'operator',
      });
      expect(captured.principal.id).toBe('principal-1');
      expect(captured.mutationEpoch).toBe(2);
      expect(captured.deploymentTag).toBe('original-tag');
      expect(captured.resourceOwner).toEqual(registered);
      expect(captured.resourceOwner).not.toBe(registered);
      expect(await observeMethods(captured)).toEqual(methodResults);
      expect(source.calls).toEqual([
        'service',
        'newRunId',
        'newThreadId',
        'resourceIdFromKey',
        'canSelfDecide',
      ]);
      expect(claim).toHaveBeenCalledWith('run', 'r', registered);
      expect(release).toHaveBeenCalledWith('run', 'r', registered);
      expect(owner).toHaveBeenCalledTimes(3);
      expect(replacement).not.toHaveBeenCalled();
    } finally {
      hold.release();
      await pending;
    }
  });

  it('C preserves the original actor without imposing principal equality', () => {
    const source = new ReceiverContext();
    const id = vi.fn(() => 'custom-actor');
    const role = vi.fn(() => 'builder');
    source.actor = Object.defineProperties(
      {},
      { id: { get: id }, role: { get: role } },
    ) as ApprovalActor;
    const captured = captureActorContext(source);
    expect(captured.actor).toEqual({ id: 'custom-actor', role: 'builder' });
    expect(captured.principal).toEqual({
      kind: 'human',
      id: 'principal-1',
      role: 'admin',
    });
    expect(id).toHaveBeenCalledTimes(1);
    expect(role).toHaveBeenCalledTimes(1);
  });

  it.each([
    null,
    undefined,
    'actor',
    { id: '', role: 'admin' },
    { id: 'secret', role: 'root' },
  ])('C refuses malformed first actor data with the fixed error (%s)', (actor) => {
    const source = new ReceiverContext();
    Object.defineProperty(source, 'actor', { value: actor });
    const principal = vi.fn();
    Object.defineProperty(source, 'principal', { get: principal });
    expect(() => captureActorContext(source)).toThrow(
      new ActorResolutionError('actor context actor is malformed'),
    );
    expect(principal).not.toHaveBeenCalled();
    expect(source.calls).toEqual([]);
  });

  it('C preserves first getter faults and principal own-data rules', () => {
    const sentinel = new Error('first fault');
    const source = new ReceiverContext();
    Object.defineProperty(source.actor, 'id', {
      get() {
        throw sentinel;
      },
    });
    expect(() => captureActorContext(source)).toThrow(sentinel);
    const other = new ReceiverContext();
    const id = vi.fn(() => 'principal-1');
    Object.defineProperty(other.principal, 'id', { get: id });
    expect(() => captureActorContext(other)).toThrow();
    expect(id).not.toHaveBeenCalled();
  });
});

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
