// SPDX-License-Identifier: Apache-2.0

import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  DIRECT_PURGE_EXIT_CODES,
  DIRECT_PURGE_MAX_REQUESTS,
  DIRECT_PURGE_OUTPUT_PREFIX,
  type DirectPurgeResult,
  parseDirectPurgeArgs,
  resolveDirectPurgeExitCode,
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

function expectSummaryTarget(
  summary: Readonly<Record<string, unknown>>,
  prefix: string,
) {
  expect(summary).toMatchObject({
    accountId: ACCOUNT,
    prefix,
    maxRequestCount: DIRECT_PURGE_MAX_REQUESTS,
  });
  expect(summary.requestCount).toEqual(expect.any(Number));
  expect(summary.requestCount).toBeGreaterThan(0);
  expect(summary.requestCount).toBeLessThanOrEqual(DIRECT_PURGE_MAX_REQUESTS);
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
    expectSummaryTarget(result.summary, world.prefix);
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
    expectSummaryTarget(result.summary, world.prefix);
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
        before: null,
        after: null,
        uncorroborated: null,
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
          queues: { count: 1, names: [`${world.prefix}-queue`] },
          dispatch: { count: 1, names: [`${world.prefix}-dispatch`] },
        },
      },
    });
    expectSummaryTarget(result.summary, world.prefix);
    expect(world.state.scriptPresent).toBe(false);
    expect(world.state.databases.size).toBe(0);
  });

  it('reports a fail-closed dispatch read as unverified', async () => {
    const world = await providerWorld({ corroborate: true, tenants: true });
    world.setHook((request, url) =>
      request.method === 'GET' &&
      url.pathname === `${ROOT}/workers/dispatch/namespaces`
        ? absent(403)
        : undefined,
    );
    const result = await purge(world);

    expect(result).toMatchObject({
      exitCode: 1,
      summary: {
        residual: 'unverified',
        after: { dispatch: { count: 0, exhaustive: false } },
        uncorroborated: ['dispatch'],
      },
    });
  });

  it("names the rescan's uncorroborated surfaces after a delete", async () => {
    const world = await providerWorld({ corroborate: true, tenants: true });
    let dispatchReads = 0;
    world.setHook((request, url) => {
      if (
        request.method !== 'GET' ||
        url.pathname !== `${ROOT}/workers/dispatch/namespaces`
      )
        return undefined;
      dispatchReads += 1;
      return dispatchReads === 2 ? absent() : undefined;
    });
    const result = await purge(world);

    expect(dispatchReads).toBe(2);
    expect(result).toMatchObject({
      exitCode: 0,
      summary: {
        mode: 'delete',
        residual: 'none',
        before: { dispatch: { exhaustive: true } },
        after: { dispatch: { count: 0, names: [], exhaustive: false } },
        uncorroborated: ['dispatch'],
      },
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
      summary: {
        failure: { surface: 'routes', step: 'delete' },
        uncorroborated: ['scripts', 'domains', 'routes', 'queues', 'dispatch'],
      },
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
    expect(help.stdout).toContain('4 internal error');
    expect(usage).toMatchObject({ status: 2, stderr: '' });
    expect(usage.stdout).toContain('"code":"usage"');
    expect(invalid).toMatchObject({ status: 2, stderr: '' });
    expect(invalid.stdout).toContain('"code":"prefix-mismatch"');
    expect(help.stdout + usage.stdout + invalid.stdout).not.toContain(
      API_TOKEN,
    );
  });

  it.each([
    [
      'rejection',
      'setTimeout(() => Promise.reject(new Error(process.env.CLOUDFLARE_API_TOKEN)), 0);',
    ],
    [
      'exception',
      'setTimeout(() => { throw new Error(process.env.CLOUDFLARE_API_TOKEN); }, 0);',
    ],
  ] as const)('traps an injected %s behind the fixed internal-error line', async (name, injection) => {
    const world = await providerWorld({ tenants: true });
    const preload = join(world.f.directory, `purge-${name}.mjs`);
    await writeFile(
      preload,
      `const write = process.stdout.write.bind(process.stdout);
let injected = false;
process.stdout.write = (...args) => {
  const result = write(...args);
  if (!injected) {
    injected = true;
    ${injection}
  }
  return result;
};
`,
    );
    const entry = new URL(directModuleUrl('direct-credentialed-purge'))
      .pathname;
    const result = await spawnDirectChild([entry, '--delete', 'wrong'], {
      timeoutMs: 10_000,
      env: {
        NODE_OPTIONS: `--import=${preload}`,
        FLEET_DIRECT_CONFORMANCE_CONFIG: world.f.configPath,
        CLOUDFLARE_ACCOUNT_ID: ACCOUNT,
        CLOUDFLARE_API_TOKEN: API_TOKEN,
      },
    });

    expect(result).toMatchObject({ status: 4, stderr: '' });
    expect(result.stdout).toContain(
      `${DIRECT_PURGE_OUTPUT_PREFIX}{"code":"internal-error"}\n`,
    );
    expect(result.stdout).not.toContain(API_TOKEN);
  });

  it('keeps exit 4 when the runtime settles after a trapped rejection', async () => {
    const world = await providerWorld({ tenants: true });
    const entry = new URL(directModuleUrl('direct-credentialed-purge'))
      .pathname;
    const preload = join(world.f.directory, 'purge-pending-rejection.mjs');
    await writeFile(
      preload,
      `let injected = false;
globalThis.fetch = async () => {
  if (!injected) {
    injected = true;
    Promise.reject(new Error(process.env.CLOUDFLARE_API_TOKEN));
    await new Promise((resolve) => setImmediate(resolve));
  }
  return Response.json(
    { success: false, errors: [{ code: 10000, message: 'synthetic refusal' }] },
    { status: 403 },
  );
};
`,
    );
    const result = await spawnDirectChild([entry, '--list'], {
      timeoutMs: 10_000,
      env: {
        NODE_OPTIONS: `--import=${preload}`,
        FLEET_DIRECT_CONFORMANCE_CONFIG: world.f.configPath,
        CLOUDFLARE_ACCOUNT_ID: ACCOUNT,
        CLOUDFLARE_API_TOKEN: API_TOKEN,
      },
    });
    const internal = `${DIRECT_PURGE_OUTPUT_PREFIX}{"code":"internal-error"}\n`;

    expect(result).toMatchObject({ status: 4, stderr: '' });
    expect(result.stdout.startsWith(internal)).toBe(true);
    expect(result.stdout.slice(internal.length)).toContain(
      '"residual":"unverified"',
    );
    expect(result.stdout).not.toContain(API_TOKEN);
  });

  it('keeps an internal error over every later exit code', () => {
    const { internalError, ...results } = DIRECT_PURGE_EXIT_CODES;
    const codes: readonly DirectPurgeResult['exitCode'][] =
      Object.values(results);
    // @ts-expect-error The runtime result never carries the entry's internal-error code.
    const entryOnly: DirectPurgeResult['exitCode'] = internalError;
    void entryOnly;
    expect(codes).toEqual([0, 1, 2, 3]);
    for (const code of codes) {
      expect(resolveDirectPurgeExitCode(null, code)).toBe(code);
      expect(resolveDirectPurgeExitCode(code, internalError)).toBe(
        internalError,
      );
      expect(resolveDirectPurgeExitCode(internalError, code)).toBe(
        internalError,
      );
    }
    expect(
      resolveDirectPurgeExitCode(results.success, results.providerFailed),
    ).toBe(results.providerFailed);
  });

  it.each([
    [
      false,
      ['scripts', 'domains', 'routes', 'queues', 'dispatch'],
      {
        databases: true,
        durableObjectNamespaces: true,
        scripts: false,
        buckets: true,
        domains: false,
        routes: false,
        queues: false,
        dispatch: false,
      },
    ],
    [
      true,
      [],
      {
        databases: true,
        durableObjectNamespaces: true,
        scripts: true,
        buckets: true,
        domains: true,
        routes: true,
        queues: true,
        dispatch: true,
      },
    ],
  ] as const)('reports provider corroboration on every summary surface (corroborate=%s)', async (corroborate, uncorroborated, exhaustive) => {
    const world = await providerWorld({ corroborate, tenants: true });
    const result = await purge(world, []);
    const before = result.summary.before as Record<
      string,
      { exhaustive: boolean }
    >;

    expect(result.summary.uncorroborated).toEqual(uncorroborated);
    expect(
      Object.fromEntries(
        Object.entries(before).map(([surface, summary]) => [
          surface,
          summary.exhaustive,
        ]),
      ),
    ).toEqual(exhaustive);
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
