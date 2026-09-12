// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { bootstrapDirectConformance } from '../scripts/direct-credentialed-bootstrap.mjs';
import { preflightDirectConformance } from '../scripts/direct-credentialed-conformance-preflight.mjs';
import {
  type DirectRunJournal,
  openDirectRunState,
} from '../scripts/direct-credentialed-run-state.mjs';
import { DIRECT_REFERENCE_PATH } from '../scripts/direct-reference-contract.mjs';

const probes = vi.hoisted(() => ({ sdk: vi.fn(), generate: vi.fn() }));
vi.mock('cloudflare', async (importOriginal) => {
  const actual = await importOriginal<typeof import('cloudflare')>();
  return {
    ...actual,
    default: class extends actual.default {
      constructor(options: ConstructorParameters<typeof actual.default>[0]) {
        probes.sdk(options);
        expect(process.env.CLOUDFLARE_CUSTOM_HEADERS).toBeUndefined();
        expect(process.env.CLOUDFLARE_LOG).toBeUndefined();
        expect(process.env.CLOUDFLARE_BASE_URL).toBeUndefined();
        super(options);
      }
    },
  };
});
vi.mock('../scripts/direct-credentialed-spec.ts', async (importOriginal) => {
  const actual =
    await importOriginal<
      typeof import('../scripts/direct-credentialed-spec.js')
    >();
  return {
    ...actual,
    generateDirectDeploymentSecrets: () => {
      probes.generate();
      return actual.generateDirectDeploymentSecrets();
    },
  };
});

const API_TOKEN = 'legacy/provider-token+sentinel==';
const INVOKE_SECRET = 'reference-invoke-secret-sentinel';
const ACCOUNT = 'account';
const FLEET_ID = '00000000-1111-4222-8333-444444444444';
const QUOTA_ID = '00000000-1111-4222-8333-555555555555';
const DEPLOYMENT_ID = '00000000-1111-4222-8333-666666666666';
const VERSION_ID = '00000000-1111-4222-8333-777777777777';
const directories: string[] = [];
const journals = new Set<DirectRunJournal>();
const unexpectedRequests: string[] = [];
const hash = (value: string | Uint8Array) =>
  createHash('sha256').update(value).digest('hex');
const json = (result: unknown, result_info?: unknown) =>
  Response.json({
    success: true,
    errors: [],
    result,
    ...(result_info === undefined ? {} : { result_info }),
  });
const absent = () =>
  Response.json(
    { success: false, errors: [{ code: 10000, message: 'synthetic absence' }] },
    { status: 404 },
  );
type Binding = {
  name: string;
  type: string;
  text?: string;
  database_id?: string;
  bucket_name?: string;
};
type Hook = (
  request: Request,
  url: URL,
) => Promise<Response | undefined> | Response | undefined;

async function fixture(
  options: {
    limit?: number;
    timeout?: number;
    tag?: string | null;
    mainModule?: string;
  } = {},
) {
  const directory = await mkdtemp(join(tmpdir(), 'direct-bootstrap-'));
  directories.push(directory);
  const config = JSON.parse(
    await readFile(
      new URL(
        '../scripts/direct-credentialed-conformance.example.json',
        import.meta.url,
      ),
      'utf8',
    ),
  );
  const wasm = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);
  const reference =
    "import manifest from './direct-run-manifest.js'; import wasm from './fixture.wasm'; void wasm; export default {fetch(){return Response.json(manifest.contractVersion)}};";
  const tenant =
    'export class Maintenance {} export class Runner {} export default {};';
  config.referenceWorker.artifact = {
    bundle: './reference.mjs',
    mainModule: options.mainModule ?? 'worker.js',
    sha256: hash(reference),
    auxiliaryWasm: [
      { file: './fixture.wasm', name: 'fixture.wasm', sha256: hash(wasm) },
    ],
  };
  config.referenceWorker.maxInvocations = options.limit ?? 20;
  config.referenceWorker.requestTimeoutMs = options.timeout ?? 1000;
  config.referenceWorker.invocationTimeoutMs = 2000;
  config.deployment.artifact = {
    bundle: './tenant.mjs',
    mainModule: 'worker.js',
    sha256: hash(tenant),
  };
  const configPath = join(directory, 'config.json');
  await Promise.all([
    writeFile(configPath, JSON.stringify(config)),
    writeFile(join(directory, 'reference.mjs'), reference),
    writeFile(join(directory, 'tenant.mjs'), tenant),
    writeFile(join(directory, 'fixture.wasm'), wasm),
  ]);
  const prepared = await preflightDirectConformance({ configPath });
  let journal = await openDirectRunState({
    configPath,
    prepared,
    accountId: ACCOUNT,
    mode: 'run',
  });
  journals.add(journal);
  const names = prepared.names;
  const root = `/client/v4/accounts/${ACCOUNT}`;
  const script = `${root}/workers/scripts/${names.referenceWorker}`;
  const databases = new Map<string, { uuid: string; name: string }>();
  let bucket: { name: string; creation_date: string } | undefined;
  let metadata:
    | {
        bindings: Binding[];
        compatibility_date: string;
        compatibility_flags: string[];
        limits: { cpu_ms: number; subrequests: number };
      }
    | undefined;
  let uploaded = false;
  let ingress = false;
  let hook: Hook | undefined;
  const requests: Request[] = [];
  const parts = new Map<string, Uint8Array>();
  const tag = options.tag === undefined ? 'immutable-script-tag' : options.tag;
  const runtime = () => ({
    compatibility_date: metadata?.compatibility_date,
    compatibility_flags: metadata?.compatibility_flags,
    limits: metadata?.limits,
  });
  const bindings = () =>
    metadata?.bindings.map((binding) =>
      binding.type === 'secret_text'
        ? { name: binding.name, type: binding.type }
        : binding,
    );
  const deployment = () => ({
    id: DEPLOYMENT_ID,
    strategy: 'percentage',
    versions: [{ version_id: VERSION_ID, percentage: 100 }],
  });
  const policy = (id: string) => ({
    id,
    status: 'active',
    policies: [
      {
        id: 'policy',
        effect: 'allow',
        permission_groups: [
          'Zone Read',
          'Workers Routes Read',
          'Workers Routes Edit',
        ].map((name, index) => ({ id: `permission-${index}`, name })),
        resources: {
          [`com.cloudflare.api.account.${ACCOUNT}`]: {
            'com.cloudflare.api.account.zone.*': '*',
          },
        },
      },
    ],
  });
  const runBinding = () => ({
    version: 1,
    accountId: ACCOUNT,
    fleetDatabaseId: FLEET_ID,
    quotaDatabaseId: QUOTA_ID,
    exportBucketName: names.exportBucket,
    referenceModuleSetSha256: prepared.referenceModuleSetSha256,
    accountWorkersDevSubdomain: 'attested-account',
  });
  const controlResponse = (
    result: unknown = { binding: runBinding() },
    status = 200,
  ) =>
    Response.json(
      status === 200
        ? {
            contractVersion: 1,
            configSha256: prepared.configSha256,
            action: 'control-read',
            ok: true,
            result,
          }
        : {
            contractVersion: 1,
            ok: false,
            error: { code: 'budget-exhausted' },
          },
      {
        status,
        headers: {
          'Cache-Control': 'no-store',
          'X-Direct-Provider-Attempts': '0',
          'X-Direct-Maintenance-Attempts': '0',
          'X-Direct-Application-Attempts': '0',
        },
      },
    );
  const fetchRequest = vi.fn<typeof fetch>(async (input, init) => {
    const request = new Request(input, init);
    const url = new URL(request.url);
    requests.push(request.clone());
    expect(request.redirect).toBe('manual');
    expect(request.signal.aborted).toBe(false);
    if (
      url.origin ===
      `https://${names.referenceWorker}.attested-account.workers.dev`
    ) {
      expect(request.headers.get('authorization')).toBe(
        `Bearer ${INVOKE_SECRET}`,
      );
      expect(url.pathname).toBe(DIRECT_REFERENCE_PATH);
      expect(journal.snapshot().lastInvocation?.state).toBe('pending');
      return (await hook?.(request, url)) ?? controlResponse();
    }
    if (url.origin !== 'https://api.cloudflare.com') {
      unexpectedRequests.push('unexpected origin');
      throw new Error('Unexpected synthetic origin');
    }
    expect(request.headers.get('authorization')).toBe(`Bearer ${API_TOKEN}`);
    expect(request.headers.has('x-auth-key')).toBe(false);
    expect(request.headers.has('x-auth-email')).toBe(false);
    expect(request.headers.has('x-auth-user-service-key')).toBe(false);
    if (request.method !== 'GET') {
      const disk = JSON.parse(
        await readFile(join(journal.directory, 'journal.json'), 'utf8'),
      );
      const pending =
        url.pathname === `${root}/d1/database`
          ? ((await request.clone().json()) as { name: string }).name ===
            names.fleetDatabase
            ? 'create-fleet-d1'
            : 'create-quota-d1'
          : url.pathname === `${root}/r2/buckets`
            ? 'create-export-r2'
            : url.pathname === script
              ? 'upload-reference'
              : 'enable-reference-ingress';
      expect(disk.bootstrap.pending).toBe(pending);
      if (pending !== 'create-fleet-d1')
        expect(disk.bootstrap.fleet).toEqual({
          uuid: FLEET_ID,
          name: names.fleetDatabase,
        });
      if (
        pending === 'upload-reference' ||
        pending === 'enable-reference-ingress'
      )
        expect(disk.bootstrap.exports).toMatchObject({
          name: names.exportBucket,
        });
    }
    const intercepted = await hook?.(request, url);
    if (intercepted) return intercepted;
    if (request.method === 'GET') {
      if (url.pathname === root) return json({ id: ACCOUNT });
      if (url.pathname === `${root}/workers/subdomain`)
        return json({ subdomain: 'attested-account' });
      if (url.pathname === `${root}/tokens/verify`)
        return json({ id: 'account-token', status: 'active' });
      if (url.pathname === `${root}/tokens/account-token`)
        return json(policy('account-token'));
      if (url.pathname === '/client/v4/user/tokens/verify')
        return json({ id: 'user-token', status: 'active' });
      if (url.pathname === '/client/v4/user/tokens/user-token')
        return json(policy('user-token'));
      if (url.pathname === '/client/v4/zones') {
        expect(url.searchParams.get('account.id')).toBe(ACCOUNT);
        expect(url.searchParams.get('per_page')).toBe('50');
        return json(
          url.searchParams.has('page')
            ? []
            : [{ id: 'zone', name: 'example.test', account: { id: ACCOUNT } }],
        );
      }
      if (url.pathname === '/client/v4/zones/zone')
        return json({
          id: 'zone',
          name: 'example.test',
          account: { id: ACCOUNT },
        });
      if (url.pathname === `${root}/workers/dispatch/namespaces`)
        return json([]);
      if (url.pathname === `${root}/d1/database`) {
        expect(url.searchParams.get('per_page')).toBe('100');
        expect([names.fleetDatabase, names.quotaDatabase]).toContain(
          url.searchParams.get('name'),
        );
        return json(
          url.searchParams.has('page')
            ? []
            : [...databases.values()].filter(
                (row) => row.name === url.searchParams.get('name'),
              ),
        );
      }
      if (url.pathname.startsWith(`${root}/d1/database/`))
        return databases.has(url.pathname.split('/').at(-1) ?? '')
          ? json(databases.get(url.pathname.split('/').at(-1) ?? ''))
          : absent();
      if (url.pathname === `${root}/r2/buckets/${names.exportBucket}`) {
        expect(request.headers.get('cf-r2-jurisdiction')).toBe('default');
        return bucket ? json(bucket) : absent();
      }
      if (url.pathname === script)
        return uploaded
          ? new Response('synthetic worker bytes', {
              headers: { 'Content-Type': 'application/javascript' },
            })
          : absent();
      if (url.pathname === `${root}/workers/scripts`)
        return json([
          { id: names.referenceWorker, ...(tag === null ? {} : { tag }) },
        ]);
      if (url.pathname === `${script}/deployments`)
        return json({ deployments: [deployment()] });
      if (url.pathname === `${script}/deployments/${DEPLOYMENT_ID}`)
        return json(deployment());
      if (url.pathname === `${script}/versions/${VERSION_ID}`)
        return json({
          id: VERSION_ID,
          resources: {
            script_runtime: {
              ...runtime(),
              limits: { cpu_ms: metadata?.limits.cpu_ms },
            },
            bindings: bindings(),
          },
        });
      if (url.pathname === `${script}/settings`)
        return json({ ...runtime(), bindings: bindings() });
      if (url.pathname === `${script}/subdomain`)
        return json({ enabled: ingress, previews_enabled: false });
    }
    if (request.method === 'POST' && url.pathname === `${root}/d1/database`) {
      const body = (await request.json()) as { name: string };
      expect(Object.keys(body)).toEqual(['name']);
      const receipt = {
        uuid: body.name === names.fleetDatabase ? FLEET_ID : QUOTA_ID,
        name: body.name,
      };
      databases.set(receipt.uuid, receipt);
      return json(receipt);
    }
    if (request.method === 'POST' && url.pathname === `${root}/r2/buckets`) {
      expect(request.headers.get('cf-r2-jurisdiction')).toBe('default');
      expect(await request.json()).toEqual({ name: names.exportBucket });
      bucket = {
        name: names.exportBucket,
        creation_date: '2026-09-10T00:00:00.000Z',
      };
      return json(bucket);
    }
    if (request.method === 'PUT' && url.pathname === script) {
      expect(request.headers.get('content-type')).toMatch(
        /^multipart\/form-data; boundary=/u,
      );
      const form = await request.formData();
      expect([...form.keys()].sort()).toEqual(
        [
          'metadata',
          ...prepared.referenceModules.map((module) => module.name),
        ].sort(),
      );
      expect(typeof form.get('metadata')).toBe('string');
      metadata = JSON.parse(form.get('metadata') as string);
      for (const module of prepared.referenceModules) {
        const part = form.get(module.name) as File;
        expect(part.name).toBe(module.name);
        expect(part.type).toBe(module.contentType);
        parts.set(module.name, new Uint8Array(await part.arrayBuffer()));
      }
      uploaded = true;
      return json({
        ...(tag === null
          ? {}
          : { id: names.referenceWorker, tag, etag: 'provider-etag' }),
      });
    }
    if (request.method === 'POST' && url.pathname === `${script}/subdomain`) {
      expect(await request.json()).toEqual({
        enabled: true,
        previews_enabled: false,
      });
      ingress = true;
      return json({ enabled: true, previews_enabled: false });
    }
    unexpectedRequests.push(`${request.method} ${url.pathname}`);
    throw new Error(
      `Unexpected synthetic request: ${request.method} ${url.pathname}`,
    );
  });
  return {
    directory,
    configPath,
    prepared,
    names,
    root,
    script,
    requests,
    parts,
    databases,
    fetchRequest,
    policy,
    runBinding,
    controlResponse,
    deployment,
    runtime,
    bindings,
    get journal() {
      return journal;
    },
    get metadata() {
      return metadata;
    },
    setHook(value: Hook | undefined) {
      hook = value;
    },
    run(
      override: Partial<Parameters<typeof bootstrapDirectConformance>[0]> = {},
    ) {
      return bootstrapDirectConformance({
        prepared,
        journal,
        apiToken: API_TOKEN,
        invokeSecret: INVOKE_SECRET,
        fetch: fetchRequest,
        ...override,
      });
    },
    async reopen() {
      await journal.close();
      journals.delete(journal);
      journal = await openDirectRunState({
        configPath,
        prepared,
        accountId: ACCOUNT,
        mode: 'resume',
      });
      journals.add(journal);
    },
  };
}

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
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  probes.sdk.mockClear();
  probes.generate.mockClear();
  try {
    await Promise.all([...journals].map((journal) => journal.close()));
  } finally {
    journals.clear();
    await Promise.all(
      directories
        .splice(0)
        .map((directory) => rm(directory, { recursive: true, force: true })),
    );
    expect(unexpectedRequests.splice(0)).toEqual([]);
  }
});

const describeLinux =
  process.platform === 'linux' ? describe.sequential : describe.skip;
describeLinux('SDK direct bootstrap', () => {
  it.each([
    'application/JSON',
    'APPLICATION/JSON',
    'application/problem+JSON',
  ])('validates mixed-case SDK JSON inventories: %s', async (contentType) => {
    for (const role of ['fleet', 'quota'] as const) {
      const f = await fixture();
      const name =
        role === 'fleet' ? f.names.fleetDatabase : f.names.quotaDatabase;
      f.setHook((_request, url) => {
        if (
          url.pathname === `${f.root}/d1/database` &&
          url.searchParams.get('name') === name
        ) {
          const response = json(
            url.searchParams.has('page')
              ? []
              : [{ uuid: 'foreign-existing-database', name }],
          );
          response.headers.set('content-type', contentType);
          return response;
        }
      });
      await expect(f.run()).rejects.toMatchObject({ code: 'name-collision' });
      expect(f.databases.size).toBe(0);
      expect(f.journal.snapshot().bootstrap?.pending).toBeNull();
    }
    for (const surface of ['d1', 'dispatch']) {
      const f = await fixture();
      f.setHook((_request, url) => {
        const target =
          surface === 'd1'
            ? `${f.root}/d1/database`
            : `${f.root}/workers/dispatch/namespaces`;
        if (url.pathname === target)
          return Response.json(
            { success: false, errors: [{ code: 1 }], result: {} },
            { headers: { 'content-type': contentType } },
          );
      });
      await expect(f.run()).rejects.toMatchObject({
        code: 'provider-unavailable',
      });
      expect(f.databases.size).toBe(0);
    }
    const f = await fixture();
    await f.run({
      fetch: async (input, init) => {
        const response = await f.fetchRequest(input, init);
        const url = new URL(
          typeof input === 'string' || input instanceof URL ? input : input.url,
        );
        if (
          url.origin === 'https://api.cloudflare.com' &&
          response.headers.get('content-type')?.includes('application/json')
        )
          response.headers.set(
            'content-type',
            `${contentType}; profile="CaseSensitive"`,
          );
        return response;
      },
    });
    expect(f.journal.snapshot().bootstrap?.controlReadOrdinal).toBe(1);
  });

  it('preserves the preflight module-name length boundary in named multipart', async () => {
    const mainModule = `${'m'.repeat(252)}.js`;
    const f = await fixture({ mainModule });
    await f.run();
    expect(f.parts.has(mainModule)).toBe(true);
  });

  it.each([
    403, 404, 429, 500, 503,
  ])('tries the other token endpoint family once after HTTP %s unavailability', async (status) => {
    const f = await fixture();
    f.setHook((_request, url) =>
      url.pathname === `${f.root}/tokens/verify`
        ? Response.json({}, { status })
        : undefined,
    );
    await f.run();
    expect(
      f.requests.filter((request) =>
        request.url.endsWith('/user/tokens/verify'),
      ),
    ).toHaveLength(1);
    expect(
      f.requests.filter((request) =>
        request.url.endsWith('/tokens/account-token'),
      ),
    ).toHaveLength(0);
  });

  it.each([
    'worker',
    'r2',
    'd1',
    'namespaces',
  ])('never treats a forbidden %s read as absence', async (kind) => {
    const f = await fixture();
    f.setHook((_request, url) =>
      (
        kind === 'worker'
          ? url.pathname === f.script
          : kind === 'r2'
            ? url.pathname.endsWith(`/r2/buckets/${f.names.exportBucket}`)
            : kind === 'd1'
              ? url.pathname.endsWith('/d1/database')
              : url.pathname.endsWith('/dispatch/namespaces')
      )
        ? Response.json({}, { status: 403 })
        : undefined,
    );
    await expect(f.run()).rejects.toMatchObject({
      code: 'provider-unavailable',
    });
    expect(f.requests.every((request) => request.method === 'GET')).toBe(true);
  });

  it.each([
    'foreign-account',
    'wrong-boundary',
    'duplicate-name',
    'duplicate-id',
    'malformed-name',
    'later-failure',
  ])('refuses %s zone proofs', async (kind) => {
    const f = await fixture();
    f.setHook((_request, url) => {
      if (url.pathname !== '/client/v4/zones') return;
      if (url.searchParams.has('page'))
        return kind === 'later-failure'
          ? Response.json({}, { status: 403 })
          : json([]);
      const row = {
        id: 'zone',
        name:
          kind === 'wrong-boundary'
            ? 'ample.test'
            : kind === 'malformed-name'
              ? 'bad..test'
              : 'example.test',
        account: { id: kind === 'foreign-account' ? 'foreign' : ACCOUNT },
      };
      return json([
        row,
        ...(kind === 'duplicate-name'
          ? [{ ...row, id: 'another' }]
          : kind === 'duplicate-id'
            ? [{ ...row, name: 'other.test' }]
            : []),
      ]);
    });
    await expect(f.run()).rejects.toHaveProperty('code');
    expect(f.requests.every((request) => request.method === 'GET')).toBe(true);
  });

  it.each([
    'missing-name',
    'duplicate-id',
    'truncated',
    'cursor',
  ])('refuses %s SinglePage namespace proofs', async (kind) => {
    const f = await fixture();
    f.setHook((_request, url) => {
      if (!url.pathname.endsWith('/dispatch/namespaces')) return;
      if (kind === 'missing-name') return json([{ namespace_id: 'id' }]);
      if (kind === 'duplicate-id')
        return json([
          { namespace_id: 'id', namespace_name: 'one' },
          { namespace_id: 'id', namespace_name: 'two' },
        ]);
      return json(
        [{ namespace_id: 'id', namespace_name: 'one' }],
        kind === 'cursor' ? { cursor: 'more' } : { total_count: 2 },
      );
    });
    await expect(f.run()).rejects.toHaveProperty('code');
    expect(f.journal.snapshot().bootstrap).toBeNull();
  });

  it('selects the longest owned zone suffix after complete traversal', async () => {
    const f = await fixture();
    f.setHook((_request, url) =>
      url.pathname === '/client/v4/zones'
        ? json(
            url.searchParams.has('page')
              ? []
              : [
                  { id: 'parent', name: 'test', account: { id: ACCOUNT } },
                  {
                    id: 'zone',
                    name: 'example.test',
                    account: { id: ACCOUNT },
                  },
                ],
          )
        : undefined,
    );
    await f.run();
    expect(f.journal.snapshot().bootstrap?.context.zoneName).toBe(
      'example.test',
    );
  });

  it('rejects a pending provider mutation and a changed prepared runtime before SDK construction', async () => {
    const f = await fixture();
    const changed = structuredClone(f.prepared);
    (changed.config.referenceWorker as { cpuLimitMs: number }).cpuLimitMs += 1;
    await expect(f.run({ prepared: changed })).rejects.toMatchObject({
      code: 'invalid-input',
    });
    await f.journal.bindBootstrapContext({
      names: f.names,
      zoneId: 'zone',
      zoneName: 'example.test',
      accountWorkersDevSubdomain: 'attested-account',
      dispatch: { kind: 'empty', count: 0 },
    });
    await f.journal.beginBootstrapMutation('create-fleet-d1');
    await expect(f.run()).rejects.toMatchObject({ code: 'outcome-unknown' });
    expect(probes.sdk).not.toHaveBeenCalled();
    expect(f.fetchRequest).not.toHaveBeenCalled();
  });

  it('refuses redirects and cancels the unconsumed response', async () => {
    const f = await fixture();
    const cancelled = vi.fn();
    f.setHook(
      () =>
        new Response(new ReadableStream({ cancel: cancelled }), {
          status: 302,
          headers: { Location: 'https://unexpected.invalid' },
        }),
    );
    await expect(f.run()).rejects.toMatchObject({
      code: 'provider-unavailable',
    });
    expect(f.fetchRequest).toHaveBeenCalledTimes(1);
    expect(cancelled).toHaveBeenCalledTimes(1);
  });

  it('cancels script collision bodies without parsing or retaining their content', async () => {
    const f = await fixture();
    const cancelled = vi.fn();
    f.setHook((_request, url) =>
      url.pathname === f.script
        ? new Response(new ReadableStream({ cancel: cancelled }), {
            headers: { 'Content-Type': 'application/javascript' },
          })
        : undefined,
    );
    await expect(f.run()).rejects.toMatchObject({ code: 'name-collision' });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(cancelled).toHaveBeenCalledTimes(1);
    expect(f.journal.snapshot().bootstrap?.pending).toBeNull();
  });

  it.each([
    'headers',
    'body',
  ])('bounds actual response %s time and cancels late or unread bodies', async (kind) => {
    const f = await fixture({ timeout: 30 });
    const cancelled = vi.fn();
    let resolveHeaders: ((response: Response) => void) | undefined;
    const response = new Response(new ReadableStream({ cancel: cancelled }), {
      headers: { 'Content-Type': 'application/json' },
    });
    f.setHook(() =>
      kind === 'body'
        ? response
        : new Promise<Response>((resolve) => {
            resolveHeaders = resolve;
          }),
    );
    await expect(f.run()).rejects.toMatchObject({
      code: 'provider-unavailable',
    });
    resolveHeaders?.(response);
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(cancelled).toHaveBeenCalledTimes(1);
    expect(f.fetchRequest).toHaveBeenCalledTimes(1);
    expect(f.journal.snapshot().bootstrap).toBeNull();
  });

  it('bounds JSON bytes before native parsing completes', async () => {
    const f = await fixture();
    const cancelled = vi.fn();
    f.setHook(
      () =>
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new Uint8Array(8 * 1024 * 1024 + 1));
            },
            cancel: cancelled,
          }),
          { headers: { 'Content-Type': 'application/json' } },
        ),
    );
    await expect(f.run()).rejects.toMatchObject({
      code: 'provider-unavailable',
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(cancelled).toHaveBeenCalledTimes(1);
    expect(f.fetchRequest).toHaveBeenCalledTimes(1);
  });

  it('bounds the bootstrap attempt duration without resetting durable state', async () => {
    const f = await fixture();
    let now = 100;
    vi.spyOn(performance, 'now').mockImplementation(() => now);
    f.setHook(() => {
      now += 300_001;
      return json({ id: ACCOUNT });
    });
    await expect(f.run()).rejects.toMatchObject({ code: 'budget-exhausted' });
    expect(f.fetchRequest).toHaveBeenCalledTimes(1);
    expect(f.journal.snapshot().invocationCount).toBe(0);
  });

  it('bounds SDK pagination attempts and refuses an unfinished inventory', async () => {
    const f = await fixture();
    f.setHook((_request, url) => {
      if (url.pathname !== '/client/v4/zones') return;
      const page = url.searchParams.get('page') ?? '1';
      return json([
        {
          id: `zone-${page}`,
          name: `zone-${page}.test`,
          account: { id: ACCOUNT },
        },
      ]);
    });
    await expect(f.run()).rejects.toMatchObject({ code: 'budget-exhausted' });
    expect(f.fetchRequest).toHaveBeenCalledTimes(512);
    expect(f.journal.snapshot().bootstrap).toBeNull();
  }, 15_000);

  it('retains create intent after a lost response and never retries dispatch', async () => {
    const f = await fixture({ timeout: 30 });
    f.setHook((request, url) =>
      request.method === 'POST' && url.pathname.endsWith('/d1/database')
        ? new Promise<Response>(() => {})
        : undefined,
    );
    await expect(f.run()).rejects.toMatchObject({ code: 'outcome-unknown' });
    expect(
      f.requests.filter((request) => request.method === 'POST'),
    ).toHaveLength(1);
    expect(f.journal.snapshot().bootstrap).toMatchObject({
      pending: 'create-fleet-d1',
      fleet: null,
    });
    const calls = probes.sdk.mock.calls.length;
    await expect(f.run()).rejects.toMatchObject({ code: 'outcome-unknown' });
    expect(probes.sdk).toHaveBeenCalledTimes(calls);
    expect(probes.generate).not.toHaveBeenCalled();
  });

  it('uploads original multipart bytes and fixed bindings, durably settles control-read, and resumes retained secrets', async () => {
    const f = await fixture();
    vi.stubEnv('CLOUDFLARE_CUSTOM_HEADERS', 'Authorization: injected');
    vi.stubEnv('CLOUDFLARE_LOG', 'debug');
    vi.stubEnv('CLOUDFLARE_BASE_URL', 'https://unexpected.invalid');
    await writeFile(
      join(f.directory, 'reference.mjs'),
      'changed on disk after preflight',
    );
    const client = await f.run();
    expect(typeof client.invoke).toBe('function');
    expect(probes.sdk.mock.calls[0]?.[0]).toMatchObject({
      baseURL: 'https://api.cloudflare.com/client/v4',
      apiToken: API_TOKEN,
      apiKey: null,
      apiEmail: null,
      userServiceKey: null,
      logLevel: 'off',
      timeout: 1000,
      maxRetries: 0,
    });
    for (const module of f.prepared.referenceModules)
      expect(f.parts.get(module.name)).toEqual(
        new Uint8Array(
          'source' in module
            ? Buffer.from(module.source)
            : Buffer.from(module.base64, 'base64'),
        ),
      );
    const bindings = f.metadata?.bindings ?? [];
    expect(bindings.find((value) => value.name === 'FLEET_DB')).toEqual({
      name: 'FLEET_DB',
      type: 'd1',
      database_id: FLEET_ID,
    });
    expect(bindings.find((value) => value.name === 'EXPORTS')).toEqual({
      name: 'EXPORTS',
      type: 'r2_bucket',
      bucket_name: f.names.exportBucket,
    });
    const roleMap = JSON.parse(
      bindings.find((value) => value.name === 'DIRECT_DEPLOYMENT_SECRETS')
        ?.text ?? 'null',
    );
    expect(Object.keys(roleMap)).toEqual(['a', 'b', 'recovery']);
    expect(probes.generate).toHaveBeenCalledTimes(3);
    const disk = await readFile(
      join(f.journal.directory, 'journal.json'),
      'utf8',
    );
    for (const secret of [
      API_TOKEN,
      INVOKE_SECRET,
      ...Object.values(roleMap).flatMap((value) => {
        const role = value as {
          deploymentIdentity: string;
          maintenanceAdmin: string;
          application: { APP_PROBE_TOKEN: string };
        };
        return [
          role.deploymentIdentity,
          role.maintenanceAdmin,
          role.application.APP_PROBE_TOKEN,
        ];
      }),
    ])
      expect(disk).not.toContain(secret);
    expect(f.journal.snapshot()).toMatchObject({
      version: 2,
      invocationCount: 1,
      lastInvocation: { state: 'settled' },
      bootstrap: {
        pending: null,
        controlReadOrdinal: 1,
        active: { deploymentId: DEPLOYMENT_ID, versionId: VERSION_ID },
      },
    });
    const mutations = f.requests.filter(
      (request) =>
        request.method !== 'GET' &&
        new URL(request.url).origin === 'https://api.cloudflare.com',
    ).length;
    await f.reopen();
    f.setHook((_request, url) =>
      url.pathname.endsWith('/dispatch/namespaces')
        ? json([{ namespace_id: 'namespace-id', namespace_name: 'fresh-name' }])
        : undefined,
    );
    await f.run();
    expect(
      f.requests.filter(
        (request) =>
          request.method !== 'GET' &&
          new URL(request.url).origin === 'https://api.cloudflare.com',
      ),
    ).toHaveLength(mutations);
    expect(probes.generate).toHaveBeenCalledTimes(3);
    expect(f.journal.snapshot()).toMatchObject({
      invocationCount: 2,
      bootstrap: {
        controlReadOrdinal: 2,
        context: { dispatch: { kind: 'empty', count: 0 } },
      },
    });
  });

  it('accepts a positive upload without optional identity metadata', async () => {
    const f = await fixture({ tag: null });
    await f.run();
    expect(f.journal.snapshot().bootstrap?.upload).toMatchObject({ tag: null });
    expect(
      f.requests.some(
        (request) =>
          new URL(request.url).pathname === `${f.root}/workers/scripts`,
      ),
    ).toBe(false);
    await f.reopen();
    await f.run();
    expect(probes.generate).toHaveBeenCalledTimes(3);
  });

  it('rejects invalid prepared bytes, journal binding, pending state and auth before SDK construction', async () => {
    const f = await fixture();
    const prepared = structuredClone(f.prepared);
    (prepared.referenceModules[0] as { source: string }).source += 'changed';
    const foreign = {
      ...f.journal,
      snapshot: () => ({
        ...f.journal.snapshot(),
        binding: {
          ...f.journal.snapshot().binding,
          configSha256: 'f'.repeat(64),
        },
      }),
    };
    for (const override of [
      { prepared },
      { journal: foreign },
      { apiToken: 'bad\r\nheader' },
      { invokeSecret: 'bad\u0100header' },
    ])
      await expect(f.run(override)).rejects.toMatchObject({
        code: 'invalid-input',
      });
    const reservation = await f.journal.reserveInvocation(
      JSON.stringify({
        contractVersion: 1,
        configSha256: f.prepared.configSha256,
        action: { kind: 'control-read' },
      }),
    );
    await expect(f.run()).rejects.toMatchObject({ code: 'outcome-unknown' });
    await f.journal.settleInvocation(reservation);
    await expect(f.run()).rejects.toMatchObject({ code: 'invalid-input' });
    expect(probes.sdk).not.toHaveBeenCalled();
    expect(f.fetchRequest).not.toHaveBeenCalled();
  });

  it('falls back to the user endpoint family once, correlates that ID, and does not freeze token identity', async () => {
    const f = await fixture();
    f.setHook((_request, url) =>
      url.pathname === `${f.root}/tokens/account-token` ? absent() : undefined,
    );
    await f.run();
    expect(
      f.requests.filter((request) =>
        request.url.endsWith('/user/tokens/verify'),
      ),
    ).toHaveLength(1);
    expect(
      f.requests.some((request) =>
        request.url.endsWith('/user/tokens/account-token'),
      ),
    ).toBe(false);
    await f.reopen();
    f.setHook(undefined);
    await f.run();
    expect(f.journal.snapshot().invocationCount).toBe(2);
  });

  it.each([
    'inactive',
    'id-mismatch',
    'missing-policies',
    'malformed-group',
    'deny',
    'partial-grant',
    'malformed-resources',
  ])('refuses %s policy without endpoint fallback or creation', async (failure) => {
    const f = await fixture();
    f.setHook((_request, url) => {
      if (url.pathname !== `${f.root}/tokens/account-token`) return;
      const token = f.policy('account-token') as Record<string, unknown>;
      const policies = token.policies as {
        effect: string;
        permission_groups: unknown[];
        resources: Record<string, unknown>;
      }[];
      const policy = policies[0];
      if (!policy) throw new Error('Missing fixture policy');
      if (failure === 'inactive') token.status = 'disabled';
      if (failure === 'id-mismatch') token.id = 'foreign';
      if (failure === 'missing-policies') delete token.policies;
      if (failure === 'malformed-group') policy.permission_groups = [null];
      if (failure === 'deny') policy.effect = 'deny';
      if (failure === 'partial-grant') policy.permission_groups.pop();
      if (failure === 'malformed-resources')
        policy.resources = { arbitrary: [] };
      return json(token);
    });
    await expect(f.run()).rejects.toHaveProperty('code');
    expect(
      f.requests.some((request) => request.url.includes('/user/tokens/')),
    ).toBe(false);
    expect(f.requests.every((request) => request.method === 'GET')).toBe(true);
  });

  it.each([
    'worker',
    'r2',
    'd1',
  ])('refuses exact %s name collisions without adoption', async (kind) => {
    const f = await fixture();
    f.setHook((_request, url) => {
      if (kind === 'worker' && url.pathname === f.script)
        return new Response('foreign');
      if (
        kind === 'r2' &&
        url.pathname.endsWith(`/r2/buckets/${f.names.exportBucket}`)
      )
        return json({ name: f.names.exportBucket });
      if (kind === 'd1' && url.pathname.endsWith('/d1/database'))
        return json(
          url.searchParams.has('page')
            ? []
            : [{ uuid: 'foreign', name: f.names.fleetDatabase }],
        );
    });
    await expect(f.run()).rejects.toMatchObject({ code: 'name-collision' });
    expect(f.requests.every((request) => request.method === 'GET')).toBe(true);
    expect(f.journal.snapshot().bootstrap?.fleet).toBeNull();
  });

  it.each([
    'zones',
    'namespaces',
    'd1',
  ])('rejects malformed and incomplete %s listing envelopes', async (kind) => {
    const invalid = [
      null,
      { success: true },
      { success: false, result: [] },
      { success: true, result: {} },
      { success: true, result: [], errors: {} },
      { success: true, result: [], result_info: { cursor: 'more' } },
      { success: true, result: [], result_info: { total_pages: 2 } },
      { success: true, result: [], result_info: { total_count: 1 } },
    ];
    for (const envelope of invalid) {
      const f = await fixture();
      f.setHook((_request, url) =>
        (
          kind === 'zones'
            ? url.pathname === '/client/v4/zones'
            : kind === 'namespaces'
              ? url.pathname.endsWith('/dispatch/namespaces')
              : url.pathname.endsWith('/d1/database')
        )
          ? Response.json(envelope)
          : undefined,
      );
      await expect(f.run()).rejects.toHaveProperty('code');
      expect(f.requests.every((request) => request.method === 'GET')).toBe(
        true,
      );
    }
  });

  it.each([
    'later-forbidden',
    'later-malformed',
    'middle-gap',
    'omitted-later-metadata',
    'missing-name',
    'duplicate',
  ])('refuses %s D1 selection, including nonmatching rows', async (kind) => {
    const f = await fixture();
    f.setHook((_request, url) => {
      if (!url.pathname.endsWith('/d1/database')) return;
      const page = Number(url.searchParams.get('page') ?? '1');
      if (kind === 'missing-name') return json([{ uuid: 'foreign' }]);
      if (kind === 'duplicate')
        return json([
          { uuid: 'same', name: 'foreign' },
          { uuid: 'same', name: 'foreign' },
        ]);
      if (page === 1)
        return json([{ uuid: 'foreign', name: 'foreign' }], {
          page: 1,
          per_page: 1,
          total_count: ['middle-gap', 'omitted-later-metadata'].includes(kind)
            ? 3
            : 1,
          total_pages: ['middle-gap', 'omitted-later-metadata'].includes(kind)
            ? 3
            : 1,
        });
      if (kind === 'later-forbidden') return Response.json({}, { status: 403 });
      if (kind === 'later-malformed') return json([{}]);
      if (kind === 'omitted-later-metadata') return json([]);
      return json([], { page: 2, per_page: 1, total_count: 3, total_pages: 3 });
    });
    await expect(f.run()).rejects.toHaveProperty('code');
    expect(f.requests.every((request) => request.method === 'GET')).toBe(true);
  });

  it('accepts complete nonmatching D1 searches with terminal numbered empty pages and initial dispatch 404', async () => {
    const f = await fixture();
    f.setHook((_request, url) => {
      if (url.pathname.endsWith('/dispatch/namespaces')) return absent();
      if (!url.pathname.endsWith('/d1/database') || _request.method !== 'GET')
        return;
      const page = Number(url.searchParams.get('page') ?? '1');
      return json(
        page === 1 ? [{ uuid: 'foreign', name: 'nonmatching' }] : [],
        { page, per_page: 100, total_count: 1, total_pages: 1 },
      );
    });
    await f.run();
    expect(f.journal.snapshot().bootstrap?.context.dispatch).toEqual({
      kind: 'first-page-404',
      count: 0,
    });
  });

  it.each([
    'missing-uuid',
    'wrong-name',
    'r2-date',
    'r2-jurisdiction',
    'upload-id',
    'upload-envelope',
  ])('retains the pending %s mutation and never retries or regenerates after ambiguity', async (kind) => {
    const f = await fixture();
    f.setHook((request, url) => {
      if (request.method === 'POST' && url.pathname.endsWith('/d1/database')) {
        if (kind === 'missing-uuid')
          return json({ name: f.names.fleetDatabase });
        if (kind === 'wrong-name')
          return json({ uuid: FLEET_ID, name: 'foreign' });
      }
      if (request.method === 'POST' && url.pathname.endsWith('/r2/buckets')) {
        if (kind === 'r2-date') return json({ name: f.names.exportBucket });
        if (kind === 'r2-jurisdiction')
          return json({
            name: f.names.exportBucket,
            creation_date: '2026-09-10T00:00:00Z',
            jurisdiction: 'eu',
          });
      }
      if (request.method === 'PUT') {
        if (kind === 'upload-id') return json({ id: 'foreign-script' });
        if (kind === 'upload-envelope') return Response.json({ result: {} });
      }
    });
    await expect(f.run()).rejects.toMatchObject({ code: 'outcome-unknown' });
    const count = f.fetchRequest.mock.calls.length;
    const generated = probes.generate.mock.calls.length;
    await expect(f.run()).rejects.toMatchObject({ code: 'outcome-unknown' });
    expect(f.fetchRequest).toHaveBeenCalledTimes(count);
    expect(probes.generate).toHaveBeenCalledTimes(generated);
    await expect(f.reopen()).rejects.toMatchObject({ code: 'outcome-unknown' });
  });

  it('resumes partial confirmed infrastructure before generating secrets', async () => {
    const f = await fixture();
    let quotaLists = 0;
    f.setHook((request, url) =>
      request.method === 'GET' &&
      url.searchParams.get('name') === f.names.quotaDatabase &&
      ++quotaLists === 2
        ? Response.json({}, { status: 403 })
        : undefined,
    );
    await expect(f.run()).rejects.toMatchObject({
      code: 'provider-unavailable',
    });
    expect(f.journal.snapshot().bootstrap).toMatchObject({
      fleet: { uuid: FLEET_ID },
      quota: null,
      pending: null,
    });
    expect(probes.generate).not.toHaveBeenCalled();
    await f.reopen();
    f.setHook(undefined);
    await f.run();
    expect(
      f.requests.filter(
        (request) =>
          request.method === 'POST' &&
          new URL(request.url).pathname.endsWith('/d1/database'),
      ),
    ).toHaveLength(2);
    expect(probes.generate).toHaveBeenCalledTimes(3);
  });

  it('resumes a confirmed upload after observation failure using retained secrets', async () => {
    const f = await fixture();
    f.setHook((_request, url) =>
      url.pathname.endsWith('/deployments')
        ? Response.json({}, { status: 503 })
        : undefined,
    );
    await expect(f.run()).rejects.toMatchObject({
      code: 'provider-unavailable',
    });
    expect(f.journal.snapshot().bootstrap).toMatchObject({
      upload: { scriptName: f.names.referenceWorker },
      active: null,
      pending: null,
    });
    await f.reopen();
    f.setHook(undefined);
    await f.run();
    expect(probes.generate).toHaveBeenCalledTimes(3);
    expect(
      f.requests.filter((request) => request.method === 'PUT'),
    ).toHaveLength(1);
  });

  it.each([
    'tag',
    'traffic',
    'deployment',
    'version',
    'cpu',
    'subrequests',
    'bindings',
    'ingress',
    'control-binding',
  ])('refuses %s mismatch after confirmed upload', async (kind) => {
    const f = await fixture();
    f.setHook((_request, url) => {
      if (kind === 'tag' && url.pathname === `${f.root}/workers/scripts`)
        return json([{ id: f.names.referenceWorker, tag: 'replacement' }]);
      if (kind === 'traffic' && url.pathname.endsWith('/deployments'))
        return json({
          deployments: [
            {
              ...f.deployment(),
              versions: [{ version_id: VERSION_ID, percentage: 50 }],
            },
          ],
        });
      if (
        kind === 'deployment' &&
        url.pathname.endsWith(`/deployments/${DEPLOYMENT_ID}`)
      )
        return json({ ...f.deployment(), id: 'replacement' });
      if (
        url.pathname.endsWith(`/versions/${VERSION_ID}`) &&
        ['version', 'cpu', 'bindings'].includes(kind)
      )
        return json({
          id: kind === 'version' ? 'replacement' : VERSION_ID,
          resources: {
            script_runtime: {
              ...f.runtime(),
              ...(kind === 'cpu' ? { limits: {} } : {}),
            },
            bindings: kind === 'bindings' ? [] : f.bindings(),
          },
        });
      if (kind === 'subrequests' && url.pathname.endsWith('/settings'))
        return json({
          ...f.runtime(),
          limits: { cpu_ms: f.prepared.config.referenceWorker.cpuLimitMs },
          bindings: f.bindings(),
        });
      if (
        kind === 'ingress' &&
        url.pathname.endsWith('/subdomain') &&
        _request.method === 'GET' &&
        url.pathname.includes('/scripts/')
      )
        return json({ enabled: true, previews_enabled: true });
      if (kind === 'control-binding' && url.hostname.endsWith('.workers.dev'))
        return f.controlResponse({
          binding: { ...f.runBinding(), accountId: 'foreign' },
        });
    });
    await expect(f.run()).rejects.toMatchObject({
      code: 'observation-mismatch',
    });
    expect(f.journal.snapshot().bootstrap?.upload).not.toBeNull();
    expect(f.journal.snapshot().bootstrap?.controlReadOrdinal).toBeNull();
    if (kind === 'control-binding')
      expect(f.journal.snapshot().lastInvocation?.state).toBe('settled');
  });

  it.each([
    'd1',
    'r2',
    'zone',
    'subdomain',
    'active',
  ])('refuses a changed confirmed %s identity on resume', async (kind) => {
    const f = await fixture();
    await f.run();
    await f.reopen();
    f.setHook((_request, url) => {
      if (kind === 'd1' && url.pathname.endsWith(`/d1/database/${FLEET_ID}`))
        return json({ uuid: 'foreign', name: f.names.fleetDatabase });
      if (
        kind === 'r2' &&
        url.pathname.endsWith(`/r2/buckets/${f.names.exportBucket}`)
      )
        return json({
          name: f.names.exportBucket,
          creation_date: '2026-09-11T00:00:00Z',
        });
      if (kind === 'zone' && url.pathname === '/client/v4/zones/zone')
        return json({
          id: 'foreign',
          name: 'example.test',
          account: { id: ACCOUNT },
        });
      if (
        kind === 'subdomain' &&
        url.pathname === `${f.root}/workers/subdomain`
      )
        return json({ subdomain: 'replacement' });
      if (kind === 'active' && url.pathname.endsWith('/deployments'))
        return json({
          deployments: [{ ...f.deployment(), id: 'replacement' }],
        });
    });
    const before = f.requests.length;
    await expect(f.run()).rejects.toHaveProperty('code');
    expect(
      f.requests.slice(before).every((request) => request.method === 'GET'),
    ).toBe(true);
    expect(probes.generate).toHaveBeenCalledTimes(3);
  });

  it('spends the committed control-read budget and preserves known refusal settlement', async () => {
    const f = await fixture({ limit: 1 });
    f.setHook((_request, url) =>
      url.hostname.endsWith('.workers.dev')
        ? f.controlResponse(undefined, 503)
        : undefined,
    );
    await expect(f.run()).rejects.toMatchObject({ code: 'reference-refused' });
    expect(f.journal.snapshot().lastInvocation?.state).toBe('settled');
    await f.reopen();
    f.setHook(undefined);
    await expect(f.run()).rejects.toMatchObject({
      code: 'invocation-budget-exhausted',
    });
    expect(
      f.requests.filter((request) =>
        new URL(request.url).hostname.endsWith('.workers.dev'),
      ),
    ).toHaveLength(1);
  });
});
