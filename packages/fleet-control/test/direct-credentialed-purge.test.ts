// SPDX-License-Identifier: Apache-2.0

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  DIRECT_PURGE_OUTPUT_PREFIX,
  parseDirectPurgeArgs,
  runDirectCredentialedPurge,
} from '../scripts/direct-credentialed-purge-runtime.mjs';
import {
  directModuleUrl,
  spawnDirectChild,
} from './fixtures/direct-cli-child.js';
import { providerJson } from './fixtures/direct-observations.js';
import {
  ACCOUNT,
  API_TOKEN,
  absent,
  providerWorld,
  ROOT,
  ROUTES,
  unexpectedRequests,
} from './fixtures/direct-provider-world.js';
import { cleanupDirectRunState } from './fixtures/direct-run-state-builder.js';

const env = {
  CLOUDFLARE_ACCOUNT_ID: ACCOUNT,
  CLOUDFLARE_API_TOKEN: API_TOKEN,
};
const roles = ['a', 'b', 'recovery'] as const;

beforeEach(() => {
  vi.stubGlobal(
    'fetch',
    vi.fn(() => {
      unexpectedRequests.push('global fetch');
      throw new Error('Unexpected global network');
    }),
  );
});

afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  await cleanupDirectRunState();
  expect(unexpectedRequests.splice(0)).toEqual([]);
});

async function purge(
  world: Awaited<ReturnType<typeof providerWorld>>,
  argv: readonly string[] = ['--delete', world.prefix],
) {
  return runDirectCredentialedPurge({
    argv,
    configPath: world.f.configPath,
    env,
    fetch: world.fetch,
    delay: async () => {},
  });
}

describe('direct credentialed purge', () => {
  it('lists without issuing a mutation', async () => {
    const world = await providerWorld({ tenants: true });
    const before = {
      scriptPresent: world.state.scriptPresent,
      scripts: structuredClone(world.state.scripts),
      databases: structuredClone([...world.state.databases]),
      buckets: structuredClone(world.state.extraBuckets),
      objects: [...world.state.objects],
    };
    const result = await purge(world, []);

    expect(result).toMatchObject({
      exitCode: 0,
      summary: { mode: 'list', residual: 'present' },
    });
    expect(
      world.requests.some((request) => request.startsWith('DELETE ')),
    ).toBe(false);
    expect(world.requests.some((request) => request.startsWith('POST '))).toBe(
      false,
    );
    expect({
      scriptPresent: world.state.scriptPresent,
      scripts: world.state.scripts,
      databases: [...world.state.databases],
      buckets: world.state.extraBuckets,
      objects: [...world.state.objects],
    }).toEqual(before);
  });

  it('deletes prefix-owned resources in the required order', async () => {
    const world = await providerWorld({ tenants: true });
    const result = await purge(world);

    expect(result).toMatchObject({
      exitCode: 0,
      summary: { mode: 'delete', residual: 'none' },
    });
    expect(
      world.requests.filter((request) => request.startsWith('DELETE ')),
    ).toEqual([
      ...roles.map((role) => `DELETE ${ROOT}/workers/domains/${role}-domain`),
      ...roles.map((role) => `DELETE ${ROUTES}/${role}-route`),
      `DELETE ${world.script}`,
      ...roles.map(
        (role) =>
          `DELETE ${ROOT}/workers/scripts/${world.names.roles[role].scriptName}`,
      ),
      `DELETE ${ROOT}/d1/database/fleet-uuid`,
      `DELETE ${ROOT}/d1/database/quota-uuid`,
      ...roles.map((role) => `DELETE ${ROOT}/d1/database/tenant-${role}-uuid`),
      ...[world.keyA, world.keyB, world.keyC].map(
        (key) => `DELETE ${world.bucketPath}/objects/${key}`,
      ),
      `DELETE ${world.bucketPath}`,
      ...roles.map(
        (role) =>
          `DELETE ${ROOT}/r2/buckets/${world.names.roles[role].scriptName}-application`,
      ),
    ]);
  });

  it('refuses an account mismatch before any deletion', async () => {
    const world = await providerWorld({ tenants: true });
    world.setHook((request, url) =>
      request.method === 'GET' && url.pathname === ROOT
        ? providerJson({ id: 'different-account' })
        : undefined,
    );
    const result = await purge(world);

    expect(result).toMatchObject({
      exitCode: 2,
      summary: { code: 'account-mismatch' },
    });
    expect(
      world.requests.some((request) => request.startsWith('DELETE ')),
    ).toBe(false);
  });

  it('refuses a mismatched confirmation before the provider is called', async () => {
    const world = await providerWorld({ tenants: true });
    const result = await purge(world, ['--delete', `${world.prefix}-wrong`]);

    expect(result).toMatchObject({
      exitCode: 2,
      summary: { code: 'prefix-mismatch' },
    });
    expect(world.requests).toEqual([]);
  });

  it('fails closed on a malformed classifying row', async () => {
    const world = await providerWorld({ tenants: true });
    world.state.domains.push({ id: 'malformed', service: 1 });
    const result = await purge(world);

    expect(result).toMatchObject({
      exitCode: 3,
      summary: {
        residual: 'unverified',
        failure: { surface: 'domains', step: 'classify' },
      },
    });
    expect(
      world.requests.some((request) => request.startsWith('DELETE ')),
    ).toBe(false);
  });

  it('does not delete a resource whose classifying name only contains the prefix', async () => {
    const world = await providerWorld({ tenants: true });
    world.state.domains.push({
      id: 'foreign-domain',
      service: `other-${world.prefix}`,
    });
    const result = await purge(world);

    expect(result.exitCode).toBe(0);
    expect(world.requests).not.toContain(
      `DELETE ${ROOT}/workers/domains/foreign-domain`,
    );
  });

  it('empties and re-lists a bucket before deleting it', async () => {
    const world = await providerWorld({ tenants: true });
    const bucket = `${world.names.roles.a.scriptName}-application`;
    world.state.bucketObjects.get(bucket)?.add('nested/object.txt');
    const result = await purge(world);
    const list = `GET ${ROOT}/r2/buckets/${bucket}/objects`;
    const removeObject = `DELETE ${ROOT}/r2/buckets/${bucket}/objects/nested/object.txt`;
    const removeBucket = `DELETE ${ROOT}/r2/buckets/${bucket}`;

    expect(result.exitCode).toBe(0);
    expect(world.requests.indexOf(list)).toBeLessThan(
      world.requests.indexOf(removeObject),
    );
    expect(world.requests.lastIndexOf(list)).toBeGreaterThan(
      world.requests.indexOf(removeObject),
    );
    expect(world.requests.lastIndexOf(list)).toBeLessThan(
      world.requests.indexOf(removeBucket),
    );
  });

  it('reports queues and dispatch namespaces after purging deletable resources', async () => {
    const world = await providerWorld({ tenants: true });
    world.state.queues.push({ queue_name: `${world.prefix}-queue` });
    world.state.dispatch.push({
      namespace_id: 'dispatch-id',
      namespace_name: `${world.prefix}-dispatch`,
    });
    const result = await purge(world);

    expect(result).toMatchObject({
      exitCode: 1,
      summary: {
        residual: 'present',
        after: {
          queues: { count: 1 },
          dispatch: { count: 1 },
        },
      },
    });
    expect(world.state.scriptPresent).toBe(false);
    expect(world.state.databases.size).toBe(0);
  });

  it('reports a fail-closed dispatch read as unverified', async () => {
    const world = await providerWorld({ tenants: true });
    world.setHook((request, url) =>
      request.method === 'GET' &&
      url.pathname === `${ROOT}/workers/dispatch/namespaces`
        ? absent(403)
        : undefined,
    );
    const result = await purge(world);

    expect(result).toMatchObject({
      exitCode: 1,
      summary: { residual: 'unverified' },
    });
  });

  it('reports a residual that survives deletion', async () => {
    const world = await providerWorld({ tenants: true });
    world.setHook((request, url) =>
      request.method === 'DELETE' && url.pathname === world.script
        ? providerJson(null)
        : undefined,
    );
    const result = await purge(world);

    expect(result).toMatchObject({
      exitCode: 1,
      summary: {
        residual: 'present',
        after: { scripts: { count: 1 } },
      },
    });
  });

  it('stops after a provider failure and names the surface and step', async () => {
    const world = await providerWorld({ tenants: true });
    world.setHook((request, url) =>
      request.method === 'DELETE' && url.pathname === `${ROUTES}/a-route`
        ? absent(500)
        : undefined,
    );
    const result = await purge(world);

    expect(result).toMatchObject({
      exitCode: 3,
      summary: { failure: { surface: 'routes', step: 'delete' } },
    });
    expect(
      world.requests.some((request) =>
        request.startsWith(`DELETE ${ROOT}/workers/scripts/`),
      ),
    ).toBe(false);
  });

  it('keeps the token out of runtime and entry output', async () => {
    const world = await providerWorld({ tenants: true });
    const runtime = await purge(world, []);
    expect(runtime.stdoutLine).toContain(DIRECT_PURGE_OUTPUT_PREFIX);
    expect(runtime.stdoutLine).not.toContain(API_TOKEN);

    const entry = new URL(directModuleUrl('direct-credentialed-purge'))
      .pathname;
    const help = await spawnDirectChild([entry, '--help'], {
      timeoutMs: 10_000,
    });
    const usage = await spawnDirectChild([entry, '--unknown'], {
      timeoutMs: 10_000,
    });
    const invalid = await spawnDirectChild([entry, '--delete', 'wrong'], {
      timeoutMs: 10_000,
      env: {
        FLEET_DIRECT_CONFORMANCE_CONFIG: world.f.configPath,
        CLOUDFLARE_ACCOUNT_ID: ACCOUNT,
        CLOUDFLARE_API_TOKEN: API_TOKEN,
      },
    });

    expect(help).toMatchObject({ status: 0, stderr: '' });
    expect(help.stdout).toContain('"mode":"help"');
    expect(usage).toMatchObject({ status: 2, stderr: '' });
    expect(usage.stdout).toContain('"code":"usage"');
    expect(invalid).toMatchObject({ status: 2, stderr: '' });
    expect(invalid.stdout).toContain('"code":"prefix-mismatch"');
    expect(help.stdout + usage.stdout + invalid.stdout).not.toContain(
      API_TOKEN,
    );
  });
});

it('parses the destructive confirmation as an exact second argument', () => {
  expect(parseDirectPurgeArgs([])).toEqual({
    mode: 'list',
    confirmation: null,
  });
  expect(parseDirectPurgeArgs(['--delete', 'prefix'])).toEqual({
    mode: 'delete',
    confirmation: 'prefix',
  });
  expect(parseDirectPurgeArgs(['--delete'])).toBeNull();
});
