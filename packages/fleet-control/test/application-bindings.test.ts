// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import {
  advanceApplicationR2Deletion,
  applicationBindingTopology,
  applicationR2Bindings,
  applicationSecretValues,
  assertApplicationR2EmptyBeforeDecommission,
  canonicalApplicationBindings,
  convergeApplicationR2Creation,
  convergeApplicationR2Deletion,
  DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
  liveApplicationTopologyMatches,
  reserveApplicationR2Resources,
} from '../src/application-bindings.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type {
  ApplicationR2Resource,
  DeploymentSecrets,
  DeploymentSpec,
} from '../src/types.js';
import {
  validateDeploymentSecrets,
  validateDeploymentSpec,
} from '../src/validation.js';

const sha256 = (value: string): string =>
  createHash('sha256').update(value).digest('hex');

function spec(application: DeploymentSpec['application']): DeploymentSpec {
  return {
    tenantTag: 'tenanta',
    environment: 'prod',
    scriptName: 'fleet-tenanta-prod',
    databaseName: 'fleet-tenanta-prod',
    compatibilityDate: '2026-08-01',
    mainModule: 'worker.js',
    modules: [
      {
        name: 'worker.js',
        content: 'export default { fetch() { return new Response("ok") } }',
        contentType: 'application/javascript+module',
      },
    ],
    authoredBy: 'platform',
    schemaVersion: 0,
    migrations: [],
    durableObjectMigrations: [],
    durableObjectBindings: [],
    maintenanceBaseUrl: 'https://fleet.example.test',
    routeHostname: 'tenanta.example.test',
    application,
  };
}

const secrets = (
  application: Record<string, string> = {},
): DeploymentSecrets => ({
  deploymentIdentity: 'd'.repeat(32),
  maintenanceAdmin: 'm'.repeat(32),
  application,
});

describe('application bindings', () => {
  it('canonicalizes by code point without mutating the caller and stabilizes the spec digest', () => {
    const first = spec({
      vars: [
        { name: 'Z_VALUE', value: 'z' },
        { name: 'A_VALUE', value: 'a' },
      ],
      secrets: [],
      r2Buckets: [{ name: 'FILES' }],
    });
    const second = spec({
      vars: [...(first.application?.vars ?? [])].reverse(),
      secrets: [],
      r2Buckets: [{ name: 'FILES', jurisdiction: 'default' }],
    });
    expect(
      canonicalApplicationBindings(first).vars.map(({ name }) => name),
    ).toEqual(['A_VALUE', 'Z_VALUE']);
    expect(first.application?.vars[0]?.name).toBe('Z_VALUE');
    expect(deploymentSpecDigest(first)).toBe(deploymentSpecDigest(second));
  });

  it('rejects application KV and reserved names before provider work', () => {
    expect(() =>
      canonicalApplicationBindings({
        application: { kvNamespaces: [] } as never,
      }),
    ).toThrow(/KV is unsupported/u);
    expect(() =>
      validateDeploymentSpec(
        spec({
          vars: [{ name: 'FLEET_TOKEN', value: 'x' }],
          secrets: [],
          r2Buckets: [],
        }),
      ),
    ).toThrow(/reserved/u);
  });

  it('requires exact secret names and digests without storing plaintext in topology', () => {
    const value = 'rotatable-secret-value';
    const deployment = spec({
      vars: [],
      secrets: [{ name: 'API_TOKEN', valueSha256: sha256(value) }],
      r2Buckets: [],
    });
    const trusted = secrets({ API_TOKEN: value });
    validateDeploymentSecrets(deployment, trusted);
    expect(applicationSecretValues(deployment, trusted)).toEqual({
      API_TOKEN: value,
    });
    expect(
      JSON.stringify(applicationBindingTopology(deployment, [])),
    ).not.toContain(value);
    expect(() =>
      applicationSecretValues(deployment, secrets({ API_TOKEN: 'wrong' })),
    ).toThrow(/SHA-256 descriptor/u);
    expect(() =>
      applicationSecretValues(
        deployment,
        secrets({ API_TOKEN: value, EXTRA: 'x' }),
      ),
    ).toThrow(/exactly match/u);
  });

  it('makes secret rotation and each nonce-backed R2 reservation change identity', () => {
    const first = spec({
      vars: [],
      secrets: [{ name: 'API_TOKEN', valueSha256: sha256('one') }],
      r2Buckets: [{ name: 'FILES', jurisdiction: 'eu' }],
    });
    const rotated = spec({
      vars: first.application?.vars ?? [],
      secrets: [{ name: 'API_TOKEN', valueSha256: sha256('two') }],
      r2Buckets: first.application?.r2Buckets ?? [],
    });
    expect(deploymentSpecDigest(first)).not.toBe(deploymentSpecDigest(rotated));
    const reservationA = reserveApplicationR2Resources(first);
    const reservationB = reserveApplicationR2Resources(first);
    expect(reservationA[0]?.bucketName).not.toBe(reservationB[0]?.bucketName);
    expect(reservationA[0]?.reservationNonce).not.toBe(
      reservationB[0]?.reservationNonce,
    );
    expect(() => applicationR2Bindings(first)).toThrow(/not been reserved/u);
    expect(applicationR2Bindings(first, reservationA)).toEqual([
      {
        name: 'FILES',
        bucketName: reservationA[0]?.bucketName,
        jurisdiction: 'eu',
      },
    ]);
  });

  it('compares the complete application variable and R2 topology', () => {
    const expected = {
      vars: [{ name: 'API_ORIGIN', value: 'https://api.example.test' }],
      secrets: [],
      r2Buckets: [
        {
          name: 'FILES',
          bucketName: 'files-0123',
          jurisdiction: 'default' as const,
        },
      ],
    };
    const live = {
      plainTextBindings: {
        DEPLOYMENT_TENANT: 'tenanta',
        API_ORIGIN: 'https://api.example.test',
      },
      r2BucketBindings: expected.r2Buckets,
    };
    expect(
      liveApplicationTopologyMatches(
        expected,
        live,
        DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
      ),
    ).toBe(true);
    expect(
      liveApplicationTopologyMatches(
        expected,
        {
          ...live,
          plainTextBindings: { ...live.plainTextBindings, EXTRA: 'spoofed' },
        },
        DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
      ),
    ).toBe(false);
    expect(
      liveApplicationTopologyMatches(
        expected,
        {
          ...live,
          r2BucketBindings: [
            ...expected.r2Buckets,
            {
              name: 'EXTRA',
              bucketName: 'foreign-bucket',
              jurisdiction: 'default',
            },
          ],
        },
        DEPLOYMENT_PLATFORM_VARIABLE_NAMES,
      ),
    ).toBe(false);
  });

  it('validates, advances, and recovers the complete application R2 lifecycle matrix', async () => {
    const deployment = spec({
      vars: [],
      secrets: [],
      r2Buckets: [{ name: 'FILES' }],
    });
    const resource = reserveApplicationR2Resources(deployment)[0];
    if (!resource) throw new Error('R2 reservation was not created');
    let live:
      | {
          name: string;
          bucketName: string;
          jurisdiction: 'default';
          creationDate: string;
        }
      | undefined;
    const persisted: ApplicationR2Resource[][] = [];
    const fence = {
      mutationLeaseTtlMs: 1_000,
      assertOwned: async () => {},
    };
    const backend = {
      findApplicationR2Bucket: async () => live,
      ensureApplicationR2Bucket: async () => {
        live = {
          name: resource.name,
          bucketName: resource.bucketName,
          jurisdiction: 'default' as const,
          creationDate: '2026-08-11T00:00:00.000Z',
        };
        throw new Error('create response lost');
      },
      assertApplicationR2Detached: async () => {},
      assertApplicationR2Empty: async () => {},
      deleteApplicationR2Bucket: async () => {
        live = undefined;
        throw new Error('delete response lost');
      },
    };
    const created = await convergeApplicationR2Creation({
      spec: deployment,
      resources: [resource],
      backend,
      fence,
      persist: async (resources) => {
        persisted.push([...resources]);
      },
    });
    expect(persisted.map(([entry]) => entry?.state)).toEqual([
      'create-authorized',
      'created',
    ]);
    const deleted = await convergeApplicationR2Deletion({
      spec: deployment,
      resources: created,
      backend,
      fence,
      persist: async (resources) => {
        persisted.push([...resources]);
      },
    });
    expect(deleted[0]?.state).toBe('deleted');
    expect(persisted.map(([entry]) => entry?.state)).toEqual(
      expect.arrayContaining([
        'detach-authorized',
        'detached',
        'empty-authorized',
        'empty',
        'delete-authorized',
        'deleted',
      ]),
    );

    const twoBucketDeployment = spec({
      vars: [],
      secrets: [],
      r2Buckets: [{ name: 'A' }, { name: 'B' }],
    });
    const reservations = reserveApplicationR2Resources(twoBucketDeployment);
    const reservationA = reservations[0];
    const reservationB = reservations[1];
    if (!reservationA || !reservationB) {
      throw new Error('two R2 reservations were not created');
    }
    const creationDate = '2026-08-11T00:00:00.000Z';
    const createdB: ApplicationR2Resource = {
      ...reservationB,
      state: 'created',
      creationDate,
    };
    for (const [label, first, expectedFirstFinds] of [
      ['reserved', reservationA, 1],
      ['deleted', { ...reservationA, state: 'deleted', creationDate }, 1],
      [
        'newly deleted',
        { ...reservationA, state: 'delete-authorized', creationDate },
        2,
      ],
    ] as const) {
      const calls: string[] = [];
      const liveBuckets = new Map(
        [first, createdB]
          .filter(
            (candidate) =>
              candidate.state !== 'reserved' && candidate.state !== 'deleted',
          )
          .map((candidate) => [
            candidate.bucketName,
            {
              name: candidate.name,
              bucketName: candidate.bucketName,
              jurisdiction: candidate.jurisdiction,
              creationDate,
            },
          ]),
      );
      const traceFence = {
        mutationLeaseTtlMs: 1_000,
        assertOwned: async () => {
          calls.push('fence');
        },
      };
      const traceBackend = {
        async findApplicationR2Bucket(candidate: ApplicationR2Resource) {
          expect(this).toBe(traceBackend);
          calls.push(`find:${candidate.name}`);
          return liveBuckets.get(candidate.bucketName);
        },
        async assertApplicationR2Detached(candidate: ApplicationR2Resource) {
          expect(this).toBe(traceBackend);
          calls.push(`detach:${candidate.name}`);
        },
        async assertApplicationR2Empty(candidate: ApplicationR2Resource) {
          expect(this).toBe(traceBackend);
          calls.push(`empty:${candidate.name}`);
        },
        async deleteApplicationR2Bucket(candidate: ApplicationR2Resource) {
          expect(this).toBe(traceBackend);
          calls.push(`delete:${candidate.name}`);
          liveBuckets.delete(candidate.bucketName);
        },
      };
      const result = await convergeApplicationR2Deletion({
        spec: twoBucketDeployment,
        resources: [first, createdB],
        backend: traceBackend,
        fence: traceFence,
        persist: async () => {},
      });
      expect(
        result.map(({ state }) => state),
        label,
      ).toEqual([
        first.state === 'reserved' ? 'reserved' : 'deleted',
        'deleted',
      ]);
      expect(
        calls.filter((call) => call === 'find:A'),
        `${label} prefix`,
      ).toHaveLength(expectedFirstFinds);
      expect(
        calls.filter((call) => call === 'find:B'),
        label,
      ).toHaveLength(5);
      expect(
        calls.filter((call) => call === 'detach:B'),
        label,
      ).toHaveLength(1);
      expect(
        calls.filter((call) => call === 'empty:B'),
        label,
      ).toHaveLength(1);
      expect(
        calls.filter((call) => call === 'delete:B'),
        label,
      ).toHaveLength(1);
      expect(
        calls.filter((call) => call === 'fence'),
        label,
      ).toHaveLength(first.state === 'delete-authorized' ? 4 : 3);
    }

    const stableReads: string[] = [];
    const stableCalls: string[] = [];
    const stableLive = new Map(
      [reservationA, reservationB].map((candidate) => [
        candidate.bucketName,
        {
          name: candidate.name,
          bucketName: candidate.bucketName,
          jurisdiction: candidate.jurisdiction,
          creationDate,
        },
      ]),
    );
    const stableBackend = {} as {
      findApplicationR2Bucket?: (resource: ApplicationR2Resource) => Promise<
        | {
            name: string;
            bucketName: string;
            jurisdiction: ApplicationR2Resource['jurisdiction'];
            creationDate: string;
          }
        | undefined
      >;
      assertApplicationR2Detached?: (
        resource: ApplicationR2Resource,
      ) => Promise<void>;
      assertApplicationR2Empty?: (
        resource: ApplicationR2Resource,
      ) => Promise<void>;
      deleteApplicationR2Bucket?: (
        resource: ApplicationR2Resource,
      ) => Promise<void>;
    };
    Object.defineProperties(stableBackend, {
      findApplicationR2Bucket: {
        configurable: true,
        get() {
          stableReads.push('find');
          return async function (
            this: typeof stableBackend,
            candidate: ApplicationR2Resource,
          ) {
            expect(this).toBe(stableBackend);
            stableCalls.push(`find:${candidate.name}`);
            return stableLive.get(candidate.bucketName);
          };
        },
      },
      assertApplicationR2Detached: {
        configurable: true,
        get() {
          stableReads.push('detach');
          return async function (
            this: typeof stableBackend,
            candidate: ApplicationR2Resource,
            mutationFence: typeof fence,
          ) {
            expect(this).toBe(stableBackend);
            expect(mutationFence).toBe(fence);
            stableCalls.push(`detach:${candidate.name}`);
          };
        },
      },
      assertApplicationR2Empty: {
        configurable: true,
        get() {
          stableReads.push('empty');
          return async function (
            this: typeof stableBackend,
            candidate: ApplicationR2Resource,
            mutationFence: typeof fence,
          ) {
            expect(this).toBe(stableBackend);
            expect(mutationFence).toBe(fence);
            stableCalls.push(`empty:${candidate.name}`);
          };
        },
      },
      deleteApplicationR2Bucket: {
        configurable: true,
        get() {
          stableReads.push('delete');
          return async function (
            this: typeof stableBackend,
            candidate: ApplicationR2Resource,
            mutationFence: typeof fence,
          ) {
            expect(this).toBe(stableBackend);
            expect(mutationFence).toBe(fence);
            stableCalls.push(`delete:${candidate.name}`);
            stableLive.delete(candidate.bucketName);
          };
        },
      },
    });
    const stableResult = await convergeApplicationR2Deletion({
      spec: twoBucketDeployment,
      resources: [
        { ...reservationA, state: 'created', creationDate },
        { ...reservationB, state: 'created', creationDate },
      ],
      backend: stableBackend,
      fence,
      persist: async () => {},
    });
    expect(stableReads).toEqual(['find', 'detach', 'empty', 'delete']);
    expect(stableResult.map(({ state }) => state)).toEqual([
      'deleted',
      'deleted',
    ]);
    for (const name of ['A', 'B']) {
      expect(
        stableCalls.filter((call) => call === `find:${name}`),
      ).toHaveLength(5);
      expect(stableCalls.filter((call) => call === `detach:${name}`)).toEqual([
        `detach:${name}`,
      ]);
      expect(stableCalls.filter((call) => call === `empty:${name}`)).toEqual([
        `empty:${name}`,
      ]);
      expect(stableCalls.filter((call) => call === `delete:${name}`)).toEqual([
        `delete:${name}`,
      ]);
    }

    const lifecycleMembers = [
      'findApplicationR2Bucket',
      'assertApplicationR2Detached',
      'assertApplicationR2Empty',
      'deleteApplicationR2Bucket',
    ] as const;
    for (const noncallable of lifecycleMembers) {
      const reads: string[] = [];
      const calls: string[] = [];
      const invalidBackend: Record<string, unknown> = {};
      for (const property of lifecycleMembers) {
        Object.defineProperty(invalidBackend, property, {
          configurable: true,
          get() {
            reads.push(property);
            return property === noncallable
              ? {}
              : async () => {
                  calls.push(property);
                };
          },
        });
      }
      let writes = 0;
      await expect(
        convergeApplicationR2Deletion({
          spec: twoBucketDeployment,
          resources: [{ ...reservationA, state: 'created', creationDate }],
          backend: invalidBackend as never,
          fence,
          persist: async () => {
            writes += 1;
          },
        }),
        noncallable,
      ).rejects.toThrow(
        'backend cannot safely delete application R2 resources',
      );
      expect(reads, noncallable).toEqual(lifecycleMembers);
      expect(calls, noncallable).toEqual([]);
      expect(writes, noncallable).toBe(0);
    }

    const detachAuthorized: ApplicationR2Resource = {
      ...reservationB,
      state: 'detach-authorized',
      creationDate,
    };
    const proofCalls: string[] = [];
    const proofFence = {
      mutationLeaseTtlMs: 1_000,
      assertOwned: async () => {
        proofCalls.push('fence');
      },
    };
    const proofBackend = {} as {
      findApplicationR2Bucket?: (
        resource: ApplicationR2Resource,
      ) => Promise<undefined>;
      assertApplicationR2Empty?: () => Promise<void>;
      deleteApplicationR2Bucket?: () => Promise<void>;
    };
    for (const property of [
      'findApplicationR2Bucket',
      'assertApplicationR2Empty',
      'deleteApplicationR2Bucket',
    ] as const) {
      Object.defineProperty(proofBackend, property, {
        configurable: true,
        get() {
          proofCalls.push(`get:${property}`);
          throw new Error(`${property} must not be read`);
        },
      });
    }
    const proof = await advanceApplicationR2Deletion({
      spec: twoBucketDeployment,
      resources: [detachAuthorized],
      backend: proofBackend,
      fence: proofFence,
      verifiedDetachmentResourceIndex: 0,
    });
    expect(proof).toMatchObject({
      status: 'resource-advanced',
      resourceIndex: 0,
      resources: [{ state: 'detached' }],
    });
    expect(proofCalls).toEqual([]);

    const empty = await advanceApplicationR2Deletion({
      spec: twoBucketDeployment,
      resources: [{ ...detachAuthorized, state: 'empty' }],
      backend: proofBackend,
      fence: proofFence,
    });
    expect(empty).toMatchObject({
      status: 'resource-advanced',
      resourceIndex: 0,
      resources: [{ state: 'delete-authorized' }],
    });
    expect(proofCalls).toEqual([]);

    const lazyReads: string[] = [];
    const lazyCalls: string[] = [];
    const lazyBackend = {} as {
      findApplicationR2Bucket?: (resource: ApplicationR2Resource) => Promise<
        | {
            name: string;
            bucketName: string;
            jurisdiction: ApplicationR2Resource['jurisdiction'];
            creationDate: string;
          }
        | undefined
      >;
      assertApplicationR2Empty?: (
        resource: ApplicationR2Resource,
      ) => Promise<void>;
      deleteApplicationR2Bucket?: () => Promise<void>;
    };
    let lazyExists = true;
    Object.defineProperties(lazyBackend, {
      findApplicationR2Bucket: {
        configurable: true,
        get() {
          lazyReads.push('find');
          return async function (
            this: typeof lazyBackend,
            candidate: ApplicationR2Resource,
          ) {
            expect(this).toBe(lazyBackend);
            lazyCalls.push('find');
            return lazyExists
              ? {
                  name: candidate.name,
                  bucketName: candidate.bucketName,
                  jurisdiction: candidate.jurisdiction,
                  creationDate,
                }
              : undefined;
          };
        },
      },
      assertApplicationR2Empty: {
        configurable: true,
        get() {
          lazyReads.push('empty');
          return async function (this: typeof lazyBackend) {
            expect(this).toBe(lazyBackend);
            lazyCalls.push('empty');
          };
        },
      },
      deleteApplicationR2Bucket: {
        configurable: true,
        get() {
          lazyReads.push('delete');
          return async function (this: typeof lazyBackend) {
            expect(this).toBe(lazyBackend);
            lazyCalls.push('delete');
            lazyExists = false;
          };
        },
      },
    });
    const emptied = await advanceApplicationR2Deletion({
      spec: twoBucketDeployment,
      resources: [{ ...detachAuthorized, state: 'empty-authorized' }],
      backend: lazyBackend,
      fence: proofFence,
    });
    expect(emptied).toMatchObject({
      status: 'resource-advanced',
      resources: [{ state: 'empty' }],
    });
    expect(lazyReads).toEqual(['find', 'empty']);
    expect(lazyCalls).toEqual(['find', 'empty']);
    expect(proofCalls).toEqual(['fence']);

    lazyReads.length = 0;
    lazyCalls.length = 0;
    proofCalls.length = 0;
    const removed = await advanceApplicationR2Deletion({
      spec: twoBucketDeployment,
      resources: [{ ...detachAuthorized, state: 'delete-authorized' }],
      backend: lazyBackend,
      fence: proofFence,
    });
    expect(removed).toMatchObject({
      status: 'resource-advanced',
      resources: [{ state: 'deleted' }],
    });
    expect(lazyReads).toEqual(['find', 'delete']);
    expect(lazyCalls).toEqual(['find', 'delete', 'find']);
    expect(proofCalls).toEqual(['fence']);
    await expect(
      advanceApplicationR2Deletion({
        spec: twoBucketDeployment,
        resources: [detachAuthorized],
        backend: {},
        fence: proofFence,
        verifiedDetachmentResourceIndex: 1,
      }),
    ).rejects.toThrow('application R2 detachment proof is invalid');
    await expect(
      advanceApplicationR2Deletion({
        spec: twoBucketDeployment,
        resources: [detachAuthorized],
        backend: {},
        fence: proofFence,
        startResourceIndex: 2,
      }),
    ).rejects.toThrow('application R2 deletion start index is invalid');
  });

  it('preflights R2 identity and emptiness without authorizing deletion', async () => {
    const deployment = spec({
      vars: [],
      secrets: [],
      r2Buckets: [{ name: 'FILES' }],
    });
    const reserved = reserveApplicationR2Resources(deployment)[0];
    if (!reserved) throw new Error('R2 reservation was not created');
    const resource: ApplicationR2Resource = {
      ...reserved,
      state: 'created',
      creationDate: '2026-08-11T00:00:00.000Z',
    };
    const calls: string[] = [];
    const fence = {
      mutationLeaseTtlMs: 1_000,
      assertOwned: async () => {
        calls.push('fence');
      },
    };
    const backend = {
      async findApplicationR2Bucket() {
        expect(this).toBe(backend);
        return {
          name: resource.name,
          bucketName: resource.bucketName,
          jurisdiction: resource.jurisdiction,
          creationDate: resource.creationDate as string,
        };
      },
      async assertApplicationR2Empty() {
        expect(this).toBe(backend);
        calls.push('empty');
        throw new Error(`R2 bucket '${resource.bucketName}' is not empty`);
      },
      deleteApplicationR2Bucket: async () => {
        calls.push('delete');
      },
    };

    await expect(
      assertApplicationR2EmptyBeforeDecommission({
        resources: [resource],
        backend,
        fence,
      }),
    ).rejects.toThrow(/not empty/u);
    expect(calls).toEqual(['fence', 'empty']);

    const deleted = { ...resource, state: 'deleted' as const };
    const inspectionCalls: string[] = [];
    const inspectionOnlyBackend = {
      async findApplicationR2Bucket(candidate: ApplicationR2Resource) {
        expect(this).toBe(inspectionOnlyBackend);
        inspectionCalls.push(candidate.state);
        return undefined;
      },
    };
    await expect(
      assertApplicationR2EmptyBeforeDecommission({
        resources: [deleted],
        backend: inspectionOnlyBackend,
        fence,
      }),
    ).resolves.toBeUndefined();
    await expect(
      assertApplicationR2EmptyBeforeDecommission({
        resources: [reserved],
        backend: inspectionOnlyBackend,
        fence,
      }),
    ).resolves.toBeUndefined();
    expect(inspectionCalls).toEqual(['deleted', 'reserved']);

    await expect(
      assertApplicationR2EmptyBeforeDecommission({
        resources: [reserved],
        backend: {
          findApplicationR2Bucket: async () => ({
            name: reserved.name,
            bucketName: reserved.bucketName,
            jurisdiction: reserved.jurisdiction,
            creationDate: '2026-08-11T00:00:00.000Z',
          }),
        },
        fence,
      }),
    ).rejects.toThrow(/refusing to decommission unauthorized R2 bucket/u);
  });

  it('never adopts a bucket from a reservation that lacks create authorization', async () => {
    const deployment = spec({
      vars: [],
      secrets: [],
      r2Buckets: [{ name: 'FILES' }],
    });
    const resource = reserveApplicationR2Resources(deployment)[0];
    if (!resource) throw new Error('R2 reservation was not created');
    await expect(
      convergeApplicationR2Creation({
        spec: deployment,
        resources: [resource],
        backend: {
          findApplicationR2Bucket: async () => ({
            name: resource.name,
            bucketName: resource.bucketName,
            jurisdiction: resource.jurisdiction,
            creationDate: '2026-08-11T00:00:00.000Z',
          }),
        },
        fence: { mutationLeaseTtlMs: 1_000, assertOwned: async () => {} },
        persist: async () => {},
      }),
    ).rejects.toThrow(/refusing to claim pre-existing R2 bucket/u);
  });
});
