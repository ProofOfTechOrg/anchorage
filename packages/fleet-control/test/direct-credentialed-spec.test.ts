// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { describe, expect, it, vi } from 'vitest';
import { DIRECT_MAX_UPLOAD_BYTES } from '../scripts/direct-credentialed-conformance-preflight.mjs';
import {
  type DirectFixtureRelease,
  type DirectFixtureRole,
  type DirectProviderContext,
  directDeploymentSpec,
  generateDirectDeploymentSecrets,
} from '../scripts/direct-credentialed-spec.js';
import { plainWorkerIngressModule } from '../src/plain-worker-backend.js';
import { provisionDeployment } from '../src/provision.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type {
  FleetStateLease,
  FleetStateStore,
  ProvisioningBackend,
} from '../src/types.js';
import {
  validateDeploymentSecrets,
  validateDeploymentSpec,
} from '../src/validation.js';
import {
  DIRECT_FIXTURE_PROVIDER,
  directFixtureManifest,
} from './fixtures/direct-credentialed-config.js';

describe('direct fixture specifications', () => {
  it.each([
    undefined,
    {},
  ])('preserves the raw example digest with overrides %j', (overrides) => {
    const raw = readFileSync(
      new URL(
        '../scripts/direct-credentialed-conformance.example.json',
        import.meta.url,
      ),
    );
    expect(directFixtureManifest(overrides).configSha256).toBe(
      createHash('sha256').update(raw).digest('hex'),
    );
  });

  it.each([
    { maxProviderRequests: 100 },
    { invocationTimeoutMs: 1000 },
    { maxProviderRequests: 100, invocationTimeoutMs: 1000 },
  ])('validates runtime overrides and digests their configuration bytes %j', (overrides) => {
    const raw = readFileSync(
      new URL(
        '../scripts/direct-credentialed-conformance.example.json',
        import.meta.url,
      ),
    );
    const expected = JSON.parse(raw.toString('utf8'));
    Object.assign(expected.referenceWorker, overrides);
    const manifest = directFixtureManifest(overrides);
    expect(manifest.referenceRuntime).toMatchObject(overrides);
    expect(manifest.configSha256).toBe(
      createHash('sha256').update(JSON.stringify(expected)).digest('hex'),
    );
    expect(manifest.configSha256).not.toBe(
      directFixtureManifest().configSha256,
    );
  });

  it.each([
    8, 1001,
  ])('rejects an invalid fixture provider budget %s', (maxProviderRequests) => {
    expect(() => directFixtureManifest({ maxProviderRequests })).toThrow(
      /referenceWorker\.maxProviderRequests/u,
    );
  });

  it.each([
    undefined,
    '',
    'UPPER',
    'bad.workers.dev',
    'bad/name',
    'bad\n',
    'a'.repeat(64),
  ])('rejects invalid provider subdomain %j', (subdomain) => {
    expect(() =>
      directDeploymentSpec(
        directFixtureManifest(),
        'a',
        'initial',
        generateDirectDeploymentSecrets(),
        { accountWorkersDevSubdomain: subdomain } as DirectProviderContext,
      ),
    ).toThrow(/account Workers.dev subdomain/);
  });

  it('rejects a provider origin that overlaps the configured application origin', () => {
    const manifest = directFixtureManifest();
    const names = manifest.names.roles.a;
    const changed = {
      ...manifest,
      names: {
        ...manifest.names,
        roles: {
          ...manifest.names.roles,
          a: {
            ...names,
            routeHostname: `${names.scriptName}.${DIRECT_FIXTURE_PROVIDER.accountWorkersDevSubdomain}.workers.dev`,
          },
        },
      },
    };
    expect(() =>
      directDeploymentSpec(
        changed,
        'a',
        'initial',
        generateDirectDeploymentSecrets(),
        DIRECT_FIXTURE_PROVIDER,
      ),
    ).toThrow(/hosts must differ/);
  });

  it.each([
    ['a', 'initial'],
    ['a', 'next'],
    ['b', 'initial'],
    ['b', 'next'],
    ['recovery', 'initial'],
    ['recovery', 'next'],
    ['recovery', 'failed-recovery'],
  ] as const)('passes coordinator admission for %s/%s before any provider dispatch', async (role, release) => {
    const secrets = generateDirectDeploymentSecrets();
    const spec = directDeploymentSpec(
      directFixtureManifest(),
      role,
      release,
      secrets,
      DIRECT_FIXTURE_PROVIDER,
    );
    const stop = new Error('reached initial store read');
    const get = vi.fn(async () => {
      throw stop;
    });
    const store: Pick<FleetStateStore, 'withDeploymentLease' | 'get'> = {
      withDeploymentLease: async (_tenant, _environment, run) =>
        run({} as FleetStateLease),
      get,
    };
    await expect(
      provisionDeployment({
        spec,
        secrets,
        initialExecutionFenceState: 'open',
        backend: { kind: 'plain-worker' } as ProvisioningBackend,
        store: store as FleetStateStore,
      }),
    ).rejects.toBe(stop);
    expect(get).toHaveBeenCalledOnce();
  });

  it('checks the complete tenant upload boundary including generated ingress and Wasm', () => {
    const manifest = directFixtureManifest();
    const secrets = generateDirectDeploymentSecrets();
    const binary = Buffer.from([0, 97, 115, 109, 1, 0, 0, 0]);
    const small = directDeploymentSpec(
      manifest,
      'a',
      'initial',
      secrets,
      DIRECT_FIXTURE_PROVIDER,
    );
    const finalOrigin = `https://${small.scriptName}.${DIRECT_FIXTURE_PROVIDER.accountWorkersDevSubdomain}.workers.dev`;
    const ingress = plainWorkerIngressModule({
      ...small,
      maintenanceBaseUrl: finalOrigin,
    });
    const mainBytes =
      DIRECT_MAX_UPLOAD_BYTES -
      Buffer.byteLength(ingress.content) -
      binary.length;
    const base = manifest.tenantModule.source;
    const source = `${base}/*${'a'.repeat(mainBytes - Buffer.byteLength(base) - 4)}*/`;
    const atBoundary = {
      ...manifest,
      tenantModule: {
        ...manifest.tenantModule,
        source,
        byteLength: Buffer.byteLength(source),
        sha256: createHash('sha256').update(source).digest('hex'),
      },
      tenantWasm: [
        {
          name: 'fixture.wasm',
          contentType: 'application/wasm' as const,
          base64: binary.toString('base64'),
          byteLength: binary.length,
          sha256: createHash('sha256').update(binary).digest('hex'),
        },
      ],
    };
    expect(() =>
      directDeploymentSpec(
        atBoundary,
        'a',
        'initial',
        secrets,
        DIRECT_FIXTURE_PROVIDER,
      ),
    ).not.toThrow();
    const oversized = `${source} `;
    expect(() =>
      directDeploymentSpec(
        {
          ...atBoundary,
          tenantModule: {
            ...atBoundary.tenantModule,
            source: oversized,
            byteLength: Buffer.byteLength(oversized),
            sha256: createHash('sha256').update(oversized).digest('hex'),
          },
        },
        'a',
        'initial',
        secrets,
        DIRECT_FIXTURE_PROVIDER,
      ),
    ).toThrow(/upload size/);
  });

  it('passes retained Wasm bytes and media type into each specification', () => {
    const bytes = Buffer.from([0, 97, 115, 109, 1, 0, 0, 0]);
    const manifest = {
      ...directFixtureManifest(),
      tenantWasm: [
        {
          name: 'fixture.wasm',
          base64: bytes.toString('base64'),
          contentType: 'application/wasm' as const,
          byteLength: bytes.length,
          sha256: createHash('sha256').update(bytes).digest('hex'),
        },
      ],
    };
    const secrets = generateDirectDeploymentSecrets();
    for (const release of ['initial', 'next'] as const) {
      const spec = directDeploymentSpec(
        manifest,
        'a',
        release,
        secrets,
        DIRECT_FIXTURE_PROVIDER,
      );
      expect(spec.modules[1]).toEqual({
        name: 'fixture.wasm',
        content: new Uint8Array(bytes),
        contentType: 'application/wasm',
      });
    }
    const changed = {
      ...manifest,
      tenantWasm: manifest.tenantWasm.map((module) => ({
        ...module,
        sha256: '0'.repeat(64),
      })),
    };
    expect(() =>
      directDeploymentSpec(
        changed,
        'a',
        'initial',
        secrets,
        DIRECT_FIXTURE_PROVIDER,
      ),
    ).toThrow(/Wasm artifact/);
  });

  it.each([
    'a',
    'b',
    'recovery',
  ] as const)('builds validated initial/next specs for %s with stable bytes and secrets', (role) => {
    const manifest = directFixtureManifest();
    const secrets = generateDirectDeploymentSecrets();
    const initial = directDeploymentSpec(
      manifest,
      role,
      'initial',
      secrets,
      DIRECT_FIXTURE_PROVIDER,
    );
    const next = directDeploymentSpec(
      manifest,
      role,
      'next',
      secrets,
      DIRECT_FIXTURE_PROVIDER,
    );
    for (const spec of [initial, next]) {
      expect(() => validateDeploymentSpec(spec)).not.toThrow();
      expect(() => validateDeploymentSecrets(spec, secrets)).not.toThrow();
      expect(spec.modules).toEqual([
        {
          name: manifest.tenantModule.name,
          content: manifest.tenantModule.source,
          contentType: 'application/javascript+module',
        },
      ]);
      expect(spec).toMatchObject({
        ...manifest.names.roles[role],
        environment: manifest.environment,
        authoredBy: 'platform',
        cpuLimitMs: 50,
        subrequestLimit: 50,
      });
      expect(spec.application?.secrets).toEqual([
        {
          name: 'APP_PROBE_TOKEN',
          valueSha256: createHash('sha256')
            .update(secrets.application?.APP_PROBE_TOKEN ?? '')
            .digest('hex'),
        },
      ]);
      expect(spec.application?.r2Buckets).toEqual([{ name: 'PROBE_BUCKET' }]);
      expect(spec.durableObjectBindings).toEqual([
        { name: 'MAINTENANCE', className: 'Maintenance' },
        { name: 'RUNNER', className: 'Runner' },
      ]);
      expect(JSON.stringify(spec)).not.toContain(secrets.maintenanceAdmin);
      expect(JSON.stringify(spec)).not.toContain(
        secrets.application?.APP_PROBE_TOKEN,
      );
    }
    expect(next.modules).toEqual(initial.modules);
    expect(next.application?.secrets).toEqual(initial.application?.secrets);
    expect(next.migrations[0]).toEqual(initial.migrations[0]);
    expect(next.migrations[1]?.rollbackCompatible).toBe(true);
    expect(next.previousDurableObjectTag).toBe('v1');
    expect(next.durableObjectMigrations).toEqual(
      initial.durableObjectMigrations,
    );
    expect(initial.application?.vars).toEqual([
      { name: 'APPLICATION_RELEASE', value: '1' },
      { name: 'APPROVAL_ALLOW_SELF_DECISION', value: 'true' },
    ]);
    expect(next.application?.vars).toEqual([
      { name: 'APPLICATION_RELEASE', value: '2' },
      { name: 'APPROVAL_ALLOW_SELF_DECISION', value: 'true' },
    ]);
    expect(deploymentSpecDigest(next)).not.toBe(deploymentSpecDigest(initial));
    expect(
      deploymentSpecDigest(
        directDeploymentSpec(
          manifest,
          role,
          'next',
          secrets,
          DIRECT_FIXTURE_PROVIDER,
        ),
      ),
    ).toBe(deploymentSpecDigest(next));
    expect(initial.compatibilityFlags).not.toBe(
      manifest.deploymentRuntime.compatibilityFlags,
    );
  });

  it('generates separate credentials without mutable secret containers', () => {
    const first = generateDirectDeploymentSecrets();
    const second = generateDirectDeploymentSecrets();
    const values = [
      first.deploymentIdentity,
      first.maintenanceAdmin,
      first.application?.APP_PROBE_TOKEN,
      second.deploymentIdentity,
      second.maintenanceAdmin,
      second.application?.APP_PROBE_TOKEN,
    ];
    for (const value of values) expect(value).toMatch(/^[A-Za-z0-9_-]{43}$/);
    expect(new Set(values).size).toBe(values.length);
    expect(Object.isFrozen(first)).toBe(true);
    expect(Object.isFrozen(first.application)).toBe(true);
  });

  it.each([
    ['a', 'failed-recovery'],
    ['unknown', 'initial'],
    ['__proto__', 'initial'],
    ['a', 'unknown'],
  ] as const)('refuses invalid selection %s/%s', (role, release) => {
    expect(() =>
      directDeploymentSpec(
        directFixtureManifest(),
        role as DirectFixtureRole,
        release as DirectFixtureRelease,
        generateDirectDeploymentSecrets(),
        DIRECT_FIXTURE_PROVIDER,
      ),
    ).toThrow(/selection/);
  });

  it('binds failed recovery to a distinct retained specification', () => {
    const manifest = directFixtureManifest();
    const secrets = generateDirectDeploymentSecrets();
    const failed = directDeploymentSpec(
      manifest,
      'recovery',
      'failed-recovery',
      secrets,
      DIRECT_FIXTURE_PROVIDER,
    );
    const valid = directDeploymentSpec(
      manifest,
      'recovery',
      'initial',
      secrets,
      DIRECT_FIXTURE_PROVIDER,
    );
    expect(failed.migrations[0]).toEqual(valid.migrations[0]);
    expect(failed.schemaVersion).toBe(2);
    expect(failed.migrations[1]?.sql).toContain(
      'direct_conformance_missing_table',
    );
    expect(deploymentSpecDigest(failed)).not.toBe(deploymentSpecDigest(valid));
  });

  it.each([
    'source',
    'sha256',
    'byteLength',
  ] as const)('rejects changed artifact %s before building a spec', (field) => {
    const manifest = structuredClone(directFixtureManifest());
    Object.assign(manifest.tenantModule, {
      [field]: field === 'byteLength' ? 1 : 'changed',
    });
    expect(() =>
      directDeploymentSpec(
        manifest,
        'a',
        'initial',
        generateDirectDeploymentSecrets(),
        DIRECT_FIXTURE_PROVIDER,
      ),
    ).toThrow(/artifact/);
  });

  it('refuses missing or extra application secrets without echoing their values', () => {
    const secrets = generateDirectDeploymentSecrets();
    const manifest = directFixtureManifest();
    expect(() =>
      directDeploymentSpec(
        manifest,
        'a',
        'initial',
        {
          ...secrets,
          application: {},
        },
        DIRECT_FIXTURE_PROVIDER,
      ),
    ).toThrow(/application secret/);
    expect(() =>
      directDeploymentSpec(
        manifest,
        'a',
        'initial',
        {
          ...secrets,
          application: { ...secrets.application, extra: 'secret-sentinel' },
        },
        DIRECT_FIXTURE_PROVIDER,
      ),
    ).toThrow(/exactly match/);
  });
});
