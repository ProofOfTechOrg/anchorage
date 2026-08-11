// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import {
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

  it('persists R2 authorization and reconciles lost create and delete responses', async () => {
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
      findApplicationR2Bucket: async () => ({
        name: resource.name,
        bucketName: resource.bucketName,
        jurisdiction: resource.jurisdiction,
        creationDate: resource.creationDate as string,
      }),
      assertApplicationR2Empty: async () => {
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
