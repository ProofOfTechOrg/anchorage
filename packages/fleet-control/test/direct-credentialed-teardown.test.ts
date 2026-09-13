// SPDX-License-Identifier: Apache-2.0

import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { DirectProviderError } from '../scripts/direct-credentialed-provider.mjs';
import type { DirectRunJournal } from '../scripts/direct-credentialed-run-state.mjs';
import type { DirectTeardownOutcome } from '../scripts/direct-credentialed-teardown.mjs';
import { teardownDirectReference } from '../scripts/direct-credentialed-teardown.mjs';
import {
  bootstrapContext,
  cleanupDirectRunState,
  completeScenario,
  completeScenarioJournal,
  exportKey,
  fixture,
  opened,
  present,
  scenarioJournal,
} from './fixtures/direct-run-state-builder.js';

const API_TOKEN = 'teardown/provider-token+sentinel==';
const ACCOUNT = 'account';
const ROOT = `/client/v4/accounts/${ACCOUNT}`;
const ROUTES = '/client/v4/zones/zone/workers/routes';
const SECRET_NAMES = [
  'CLOUDFLARE_API_TOKEN',
  'DIRECT_DEPLOYMENT_SECRETS',
  'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
];
const unexpectedRequests: string[] = [];

type Hook = (
  request: Request,
  url: URL,
) => Promise<Response | undefined> | Response | undefined;
type Row = Record<string, unknown>;

const json = (result: unknown, result_info?: unknown) =>
  Response.json({
    success: true,
    errors: [],
    result,
    ...(result_info === undefined ? {} : { result_info }),
  });
const absent = (status = 404) =>
  Response.json(
    { success: false, errors: [{ code: 10000, message: 'synthetic absence' }] },
    { status },
  );
const forbid = () => absent(403);
const paged = (url: URL, rows: Row[]) =>
  json(url.searchParams.has('page') ? [] : rows);
const never = () =>
  vi.fn<typeof fetch>(() => {
    unexpectedRequests.push('provider call on a refused precondition');
    throw new Error('teardown must not call the provider');
  });

function retained(outcome: DirectTeardownOutcome) {
  if (outcome.status !== 'retained')
    throw new Error(`expected a retained outcome, saw ${outcome.status}`);
  return outcome;
}

async function diskState(journal: DirectRunJournal) {
  return JSON.parse(
    await readFile(join(journal.directory, 'journal.json'), 'utf8'),
  ) as { teardown?: Record<string, unknown> };
}

async function world(
  options: {
    limit?: number;
    disposableAccount?: boolean;
    complete?: boolean;
  } = {},
) {
  const limit = options.limit ?? 8;
  const disposable = options.disposableAccount ?? true;
  const { f, journal } =
    options.complete === false
      ? await scenarioJournal(limit, disposable)
      : await completeScenarioJournal(limit, disposable);
  const names = f.prepared.names;
  const prefix = f.prepared.config.resourcePrefix;
  const script = `${ROOT}/workers/scripts/${names.referenceWorker}`;
  const bucketPath = `${ROOT}/r2/buckets/${names.exportBucket}`;
  const keys = [exportKey(prefix, 'a'), exportKey(prefix, 'b')];
  const state = {
    ingress: true,
    scriptPresent: true,
    bucketPresent: true,
    secretNames: [...SECRET_NAMES],
    versions: [{ id: 'version' }] as Row[],
    databases: new Map<string, Row>([
      ['fleet-uuid', { uuid: 'fleet-uuid', name: names.fleetDatabase }],
      ['quota-uuid', { uuid: 'quota-uuid', name: names.quotaDatabase }],
    ]),
    objects: new Set(keys),
    extraDatabases: [] as Row[],
    extraBuckets: [] as Row[],
    namespaces: [] as Row[],
    scripts: [] as Row[],
    domains: [] as Row[],
    routes: [] as Row[],
    dispatch: [] as Row[],
  };
  let hook: Hook | undefined;
  const requests: string[] = [];
  const residualDatabases = (): Row[] => [
    ...state.databases.values(),
    ...state.extraDatabases,
  ];
  const residualBuckets = (): Row[] => [
    ...(state.bucketPresent ? [{ name: names.exportBucket }] : []),
    ...state.extraBuckets,
  ];
  const residualScripts = (): Row[] => [
    ...(state.scriptPresent ? [{ id: names.referenceWorker }] : []),
    ...state.scripts,
  ];
  const fetchRequest = vi.fn<typeof fetch>(async (input, init) => {
    const request = new Request(input, init);
    const url = new URL(request.url);
    const path = decodeURIComponent(url.pathname);
    requests.push(`${request.method} ${path}`);
    if (url.origin !== 'https://api.cloudflare.com') {
      unexpectedRequests.push('unexpected origin');
      throw new Error('Unexpected synthetic origin');
    }
    expect(request.headers.get('authorization')).toBe(`Bearer ${API_TOKEN}`);
    const intercepted = await hook?.(request, url);
    if (intercepted) return intercepted;
    if (request.method === 'GET') {
      if (path === `${script}/subdomain`)
        return json({ enabled: state.ingress, previews_enabled: false });
      if (path === `${script}/secrets`)
        return json(state.secretNames.map((name) => ({ name })));
      if (path === `${script}/deployments`)
        return json({
          deployments: [
            {
              id: 'deployment',
              strategy: 'percentage',
              versions: [{ version_id: 'version', percentage: 100 }],
            },
          ],
        });
      if (path === `${script}/versions`)
        return state.scriptPresent ? json({ items: state.versions }) : absent();
      if (path === script)
        return state.scriptPresent
          ? new Response('synthetic worker bytes', {
              headers: { 'Content-Type': 'application/javascript' },
            })
          : absent();
      if (path === `${ROOT}/workers/scripts`) return json(residualScripts());
      if (path.startsWith(`${ROOT}/d1/database/`)) {
        const row = state.databases.get(
          path.slice(`${ROOT}/d1/database/`.length),
        );
        return row ? json(row) : absent();
      }
      if (path === `${ROOT}/d1/database`) {
        const name = url.searchParams.get('name');
        return paged(
          url,
          residualDatabases().filter(
            (row) =>
              name === null ||
              (typeof row.name === 'string' && row.name.includes(name)),
          ),
        );
      }
      if (path === `${ROOT}/workers/durable_objects/namespaces`)
        return paged(url, state.namespaces);
      if (path === `${ROOT}/r2/buckets`) {
        const after = url.searchParams.get('start_after');
        return json({
          buckets: residualBuckets().filter(
            (row) =>
              after === null ||
              (typeof row.name === 'string' && row.name > after),
          ),
        });
      }
      if (path === `${bucketPath}/objects`) {
        const scoped = url.searchParams.get('prefix');
        return json(
          [...state.objects]
            .filter((key) => scoped === null || key.startsWith(scoped))
            .map((key) => ({ key })),
        );
      }
      if (path.startsWith(`${bucketPath}/objects/`))
        return state.objects.has(path.slice(`${bucketPath}/objects/`.length))
          ? new Response('synthetic export bytes')
          : absent();
      if (path === bucketPath)
        return state.bucketPresent
          ? json({
              name: names.exportBucket,
              creation_date: '2026-09-10T00:00:00.000Z',
            })
          : absent();
      if (path === `${ROOT}/workers/domains`) return json(state.domains);
      if (path === ROUTES) return json(state.routes);
      if (path === `${ROOT}/workers/dispatch/namespaces`)
        return json(state.dispatch);
    }
    if (request.method === 'POST' && path === `${script}/subdomain`) {
      expect(await request.json()).toEqual({
        enabled: false,
        previews_enabled: false,
      });
      state.ingress = false;
      return json({ enabled: false, previews_enabled: false });
    }
    if (request.method === 'DELETE') {
      if (path === script) {
        state.scriptPresent = false;
        return json(null);
      }
      if (path.startsWith(`${ROOT}/d1/database/`)) {
        state.databases.delete(path.slice(`${ROOT}/d1/database/`.length));
        return json(null);
      }
      if (path.startsWith(`${bucketPath}/objects/`)) {
        state.objects.delete(path.slice(`${bucketPath}/objects/`.length));
        return json({});
      }
      if (path === bucketPath) {
        state.bucketPresent = false;
        return json({});
      }
    }
    unexpectedRequests.push(`${request.method} ${path}`);
    throw new Error(`Unexpected synthetic request: ${request.method} ${path}`);
  });
  return {
    f,
    names,
    prefix,
    script,
    bucketPath,
    keyA: present(keys[0]),
    keyB: present(keys[1]),
    state,
    requests,
    journal,
    setHook(value: Hook | undefined) {
      hook = value;
    },
    run() {
      return teardownDirectReference({
        prepared: f.prepared,
        journal,
        apiToken: API_TOKEN,
        fetch: fetchRequest,
        delay: async () => {},
      });
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
  await cleanupDirectRunState();
  expect(unexpectedRequests.splice(0)).toEqual([]);
});

const describeLinux =
  process.platform === 'linux' ? describe.sequential : describe.skip;

describeLinux('direct reference teardown', () => {
  it('deletes every receipt in order and proves a zero residual', async () => {
    const w = await world();
    const outcome = await w.run();
    expect(outcome.status).toBe('cleaned');
    expect(w.requests).toEqual([
      `POST ${w.script}/subdomain`,
      `GET ${w.script}/subdomain`,
      `GET ${w.script}/secrets`,
      `DELETE ${w.script}`,
      `GET ${w.script}`,
      `DELETE ${ROOT}/d1/database/fleet-uuid`,
      `GET ${ROOT}/d1/database/fleet-uuid`,
      `DELETE ${ROOT}/d1/database/quota-uuid`,
      `GET ${ROOT}/d1/database/quota-uuid`,
      `GET ${w.bucketPath}/objects`,
      `GET ${w.bucketPath}/objects`,
      `DELETE ${w.bucketPath}/objects/${w.keyA}`,
      `GET ${w.bucketPath}/objects/${w.keyA}`,
      `DELETE ${w.bucketPath}/objects/${w.keyB}`,
      `GET ${w.bucketPath}/objects/${w.keyB}`,
      `GET ${w.bucketPath}/objects`,
      `DELETE ${w.bucketPath}`,
      `GET ${w.bucketPath}`,
      `GET ${ROOT}/d1/database`,
      `GET ${ROOT}/d1/database`,
      `GET ${ROOT}/workers/durable_objects/namespaces`,
      `GET ${ROOT}/workers/scripts`,
      `GET ${ROOT}/r2/buckets`,
      `GET ${ROOT}/workers/domains`,
      `GET ${ROUTES}`,
      `GET ${ROOT}/workers/dispatch/namespaces`,
      `GET ${w.script}/versions`,
    ]);
    expect(outcome.facts.receipts).toMatchObject({
      ingress: { ordinal: 2, settledByReread: false },
      worker: {
        scriptName: w.names.referenceWorker,
        secretNames: SECRET_NAMES,
        ordinal: 5,
        settledByReread: false,
      },
      fleet: { uuid: 'fleet-uuid', ordinal: 7, settledByReread: false },
      quota: { uuid: 'quota-uuid', ordinal: 9, settledByReread: false },
      exports: {
        name: w.names.exportBucket,
        ordinal: 18,
        settledByReread: false,
      },
    });
    expect(outcome.facts.receipts.exportObjects).toEqual([
      { key: w.keyA, ordinal: 13, settledByReread: false },
      { key: w.keyB, ordinal: 15, settledByReread: false },
    ]);
    expect(outcome.facts.retainedIdentities).toEqual({
      fleetUuid: null,
      quotaUuid: null,
      exportBucket: null,
      scriptName: null,
      activeVersionId: null,
    });
    const residual = present(outcome.facts.residual);
    expect(residual).toMatchObject({
      version: 1,
      bucketJurisdictions: ['default'],
      dispatch: { kind: 'empty', count: 0, status: null, prefixCount: 0 },
      versionsGone: true,
      settleAttempts: 1,
    });
    for (const surface of Object.values(residual.surfaces))
      expect(surface).toMatchObject({
        prefixCount: 0,
        prefixNames: [],
        globalCount: 0,
      });
    expect(outcome.facts.providerRequests).toBe(w.requests.length);
    expect((await diskState(w.journal)).teardown).toMatchObject({
      phase: 'complete',
      pending: null,
      failure: null,
    });
  });

  it('returns recorded facts without a provider call once complete', async () => {
    const w = await world();
    const first = await w.run();
    const calls = w.requests.length;
    expect(await w.run()).toEqual(first);
    expect(w.requests.length).toBe(calls);
  });

  it('refuses a pending invocation before any provider call', async () => {
    const w = await world();
    await w.journal.reserveInvocation(w.f.request());
    const outcome = retained(await w.run());
    expect(outcome).toMatchObject({
      reason: 'outcome-unknown',
      phase: 'refused',
    });
    expect(w.requests).toEqual([]);
    expect(outcome.facts.retainedIdentities.fleetUuid).toBe('fleet-uuid');
  });

  it('refuses a pending bootstrap mutation and an incomplete bootstrap before any provider call', async () => {
    const f = await fixture();
    const journal = await opened({ ...f.input, mode: 'run' });
    await journal.bindBootstrapContext(bootstrapContext(f));
    const call = () =>
      teardownDirectReference({
        prepared: f.prepared,
        journal,
        apiToken: API_TOKEN,
        fetch: never(),
      });
    expect(retained(await call()).reason).toBe('invalid-state');
    await journal.beginBootstrapMutation('create-fleet-d1');
    const pending = retained(await call());
    expect(pending.reason).toBe('outcome-unknown');
    expect(pending.facts.retainedIdentities).toEqual({
      fleetUuid: null,
      quotaUuid: null,
      exportBucket: null,
      scriptName: null,
      activeVersionId: null,
    });
  });

  it('records a refused state with residuals, deletes nothing and re-observes in place', async () => {
    const w = await world({ complete: false });
    const outcome = retained(await w.run());
    expect(outcome).toMatchObject({
      reason: 'scenario-incomplete',
      phase: 'refused',
    });
    expect(w.requests.some((entry) => entry.startsWith('DELETE'))).toBe(false);
    expect(outcome.facts.residual).toMatchObject({
      versionsGone: null,
      settleAttempts: 1,
    });
    expect(present(outcome.facts.residual).surfaces.databases).toMatchObject({
      prefixCount: 2,
      prefixNames: [w.names.fleetDatabase, w.names.quotaDatabase],
    });
    expect(outcome.facts.retainedIdentities).toEqual({
      fleetUuid: 'fleet-uuid',
      quotaUuid: 'quota-uuid',
      exportBucket: w.names.exportBucket,
      scriptName: w.names.referenceWorker,
      activeVersionId: 'version',
    });
    const first = w.requests.length;
    expect(retained(await w.run()).reason).toBe('scenario-incomplete');
    expect(w.requests.length).toBeGreaterThan(first);
    expect(w.requests.some((entry) => entry.startsWith('DELETE'))).toBe(false);
    expect((await diskState(w.journal)).teardown).toMatchObject({
      phase: 'refused',
      failure: 'scenario-incomplete',
      receipts: { exportObjects: [] },
    });
  });

  it('refuses a confirmed export set that is not exactly two keys', async () => {
    const w = await world({ complete: false });
    const state = completeScenario();
    const a = present(state.proofs.exports.a);
    present(state.proofs.exports.b).receipt = structuredClone(a.receipt);
    state.proofs.exportVerifications = state.proofs.exportVerifications.map(
      () => structuredClone(a),
    );
    await w.journal.recordScenario(state);
    const outcome = retained(await w.run());
    expect(outcome.reason).toBe('invalid-state');
    expect(w.requests).toEqual([]);
  });

  it('refuses an unexpected object before any object delete', async () => {
    for (const kind of ['under-prefix', 'outside-prefix', 'keyless'] as const) {
      const w = await world();
      if (kind === 'under-prefix')
        w.state.objects.add(`${w.prefix}/receipts/v1/other/object.sql`);
      if (kind === 'outside-prefix')
        w.state.objects.add('unrelated/object.sql');
      if (kind === 'keyless')
        w.setHook((request, url) =>
          request.method === 'GET' && url.pathname.endsWith('/objects')
            ? json([{ size: 1 }])
            : undefined,
        );
      expect(retained(await w.run()).reason).toBe('unexpected-object');
      expect(
        w.requests.filter(
          (entry) => entry.startsWith('DELETE') && entry.includes('/objects/'),
        ),
      ).toEqual([]);
    }
  });

  it('settles an unusable delete answer by exact-identity reread in the same run', async () => {
    for (const kind of [
      'empty',
      'array',
      'unsuccessful',
      'no-content',
      'throw',
    ] as const) {
      const w = await world();
      w.setHook((request, url) => {
        if (request.method !== 'DELETE' || url.pathname !== w.script)
          return undefined;
        w.state.scriptPresent = false;
        if (kind === 'throw') throw new Error('synthetic transport loss');
        if (kind === 'array') return json([]);
        if (kind === 'unsuccessful')
          return Response.json({ success: false, errors: [], result: null });
        if (kind === 'no-content') return new Response(null, { status: 204 });
        return new Response(null, {
          status: 200,
          headers: {
            'content-type': 'application/json',
            'content-length': '0',
          },
        });
      });
      const outcome = await w.run();
      expect(outcome.status).toBe('cleaned');
      expect(outcome.facts.receipts.worker).toMatchObject({
        settledByReread: true,
      });
    }
  });

  it('resumes a pending second export object with the first already receipted', async () => {
    const w = await world();
    w.setHook((request, url) =>
      request.method === 'DELETE' &&
      decodeURIComponent(url.pathname).endsWith(w.keyB)
        ? json([])
        : undefined,
    );
    expect(retained(await w.run()).reason).toBe('outcome-unknown');
    expect((await diskState(w.journal)).teardown).toMatchObject({
      phase: 'export-objects',
      pending: { kind: 'delete-export-object', key: w.keyB },
      receipts: { exportObjects: [{ key: w.keyA }] },
    });
    w.setHook(undefined);
    const outcome = await w.run();
    expect(outcome.status).toBe('cleaned');
    expect(
      outcome.facts.receipts.exportObjects.map((entry) => entry.key),
    ).toEqual([w.keyA, w.keyB]);
  });

  it('refuses an unreadable probe instead of claiming absence', async () => {
    const w = await world();
    w.setHook((request, url) =>
      request.method === 'GET' && url.pathname === `${w.script}/subdomain`
        ? absent(500)
        : undefined,
    );
    expect(retained(await w.run()).reason).toBe('provider-unavailable');
  });

  it('retains a pending mutation whose reread still shows the resource', async () => {
    const w = await world();
    w.setHook((request, url) =>
      request.method === 'DELETE' && url.pathname === w.script
        ? json([])
        : undefined,
    );
    const outcome = retained(await w.run());
    expect(outcome).toMatchObject({
      reason: 'outcome-unknown',
      phase: 'worker',
    });
    expect((await diskState(w.journal)).teardown).toMatchObject({
      phase: 'worker',
      pending: { kind: 'delete-reference-worker' },
      receipts: { worker: null },
    });
  });

  it('resumes an absent pending mutation without a second delete', async () => {
    const w = await world();
    w.setHook((request, url) =>
      request.method === 'DELETE' && url.pathname.includes('/d1/database/')
        ? json([])
        : undefined,
    );
    expect(retained(await w.run()).reason).toBe('outcome-unknown');
    w.setHook(undefined);
    w.state.databases.delete('fleet-uuid');
    const mark = w.requests.length;
    const outcome = await w.run();
    expect(outcome.status).toBe('cleaned');
    expect(outcome.facts.receipts.fleet).toMatchObject({
      uuid: 'fleet-uuid',
      settledByReread: true,
    });
    expect(
      w.requests
        .slice(mark)
        .filter((entry) => entry === `DELETE ${ROOT}/d1/database/fleet-uuid`),
    ).toEqual([]);
  });

  it('re-issues a present pending mutation once the identity matches', async () => {
    const w = await world();
    w.setHook((request, url) =>
      request.method === 'DELETE' && url.pathname === w.bucketPath
        ? json([])
        : undefined,
    );
    expect(retained(await w.run()).reason).toBe('outcome-unknown');
    w.setHook(undefined);
    const mark = w.requests.length;
    const outcome = await w.run();
    expect(outcome.status).toBe('cleaned');
    expect(outcome.facts.receipts.exports).toMatchObject({
      name: w.names.exportBucket,
      settledByReread: false,
    });
    expect(
      w.requests.slice(mark).filter((entry) => entry.startsWith('DELETE')),
    ).toEqual([`DELETE ${w.bucketPath}`]);
  });

  it('refuses a changed identity on resume instead of deleting', async () => {
    for (const kind of ['bucket', 'script'] as const) {
      const w = await world();
      const target = kind === 'bucket' ? w.bucketPath : w.script;
      w.setHook((request, url) =>
        request.method === 'DELETE' && url.pathname === target
          ? json([])
          : undefined,
      );
      expect(retained(await w.run()).reason).toBe('outcome-unknown');
      const mark = w.requests.length;
      w.setHook((request, url) => {
        if (request.method !== 'GET') return undefined;
        if (kind === 'bucket' && url.pathname === w.bucketPath)
          return json({
            name: w.names.exportBucket,
            creation_date: '2020-01-01T00:00:00.000Z',
          });
        if (kind === 'script' && url.pathname === `${w.script}/deployments`)
          return json({
            deployments: [
              {
                id: 'deployment',
                strategy: 'percentage',
                versions: [{ version_id: 'other-version', percentage: 100 }],
              },
            ],
          });
        return undefined;
      });
      expect(retained(await w.run()).reason).toBe('identity-mismatch');
      expect(
        w.requests.slice(mark).filter((entry) => entry.startsWith('DELETE')),
      ).toEqual([]);
    }
  });

  it('refuses a mismatched secret set instead of deleting the worker', async () => {
    const w = await world();
    w.state.secretNames.push('UNEXPECTED_SECRET');
    const outcome = retained(await w.run());
    expect(outcome.reason).toBe('identity-mismatch');
    expect(w.requests).not.toContain(`DELETE ${w.script}`);
  });

  it('refuses a forbidden residual surface and records a fail-closed dispatch', async () => {
    for (const surface of [
      `${ROOT}/d1/database`,
      `${ROOT}/workers/durable_objects/namespaces`,
      `${ROOT}/workers/scripts`,
      `${ROOT}/r2/buckets`,
      `${ROOT}/workers/domains`,
      ROUTES,
    ]) {
      const w = await world();
      w.setHook((request, url) =>
        request.method === 'GET' && url.pathname === surface
          ? forbid()
          : undefined,
      );
      expect(retained(await w.run()).reason).toBe('forbidden');
    }
    const w = await world();
    w.setHook((request, url) =>
      request.method === 'GET' &&
      url.pathname === `${ROOT}/workers/dispatch/namespaces`
        ? forbid()
        : undefined,
    );
    const outcome = retained(await w.run());
    expect(outcome.reason).toBe('residual-present');
    expect(present(outcome.facts.residual).dispatch).toMatchObject({
      kind: 'fail-closed',
      count: 0,
      status: 403,
      prefixCount: 0,
    });
  });

  it('refuses a short numbered page and an unusable bucket page', async () => {
    for (const page of [
      json([{ uuid: 'one', name: 'one' }], { total_count: 2 }),
      json([], { total_pages: 2 }),
    ]) {
      const short = await world();
      short.setHook((request, url) =>
        request.method === 'GET' &&
        url.pathname === `${ROOT}/d1/database` &&
        !url.searchParams.has('page')
          ? page.clone()
          : undefined,
      );
      expect(retained(await short.run()).reason).toBe('provider-unavailable');
    }

    const unsorted = await world();
    unsorted.setHook((request, url) =>
      request.method === 'GET' && url.pathname === `${ROOT}/r2/buckets`
        ? json({
            buckets: url.searchParams.has('start_after')
              ? [{ name: 'aaa' }]
              : [{ name: 'zzz' }],
          })
        : undefined,
    );
    expect(retained(await unsorted.run()).reason).toBe('provider-unavailable');

    const nameless = await world();
    nameless.setHook((request, url) =>
      request.method === 'GET' && url.pathname === `${ROOT}/r2/buckets`
        ? json({ buckets: [{ location: 'weur' }] })
        : undefined,
    );
    expect(retained(await nameless.run()).reason).toBe('provider-unavailable');
  });

  it('terminates the bucket loop only on an empty page', async () => {
    const w = await world();
    w.state.extraBuckets.push({ name: 'zzz-other-bucket' });
    const outcome = retained(await w.run());
    expect(outcome.reason).toBe('residual-present');
    expect(
      w.requests.filter((entry) => entry === `GET ${ROOT}/r2/buckets`).length,
    ).toBe(10);
    expect(present(outcome.facts.residual).surfaces.buckets).toMatchObject({
      prefixCount: 0,
      prefixNames: [],
      globalCount: 1,
      exhaustive: true,
    });
  });

  it('reports a prefix residual with names after the settle ceiling', async () => {
    const w = await world();
    w.state.extraDatabases.push({
      uuid: 'residual-uuid',
      name: `${w.prefix}-left-behind`,
    });
    const outcome = retained(await w.run());
    expect(outcome).toMatchObject({
      reason: 'residual-present',
      phase: 'complete',
    });
    expect(present(outcome.facts.residual).settleAttempts).toBe(5);
    expect(present(outcome.facts.residual).surfaces.databases).toMatchObject({
      prefixCount: 1,
      prefixNames: [`${w.prefix}-left-behind`],
      exhaustive: true,
    });
    expect((await diskState(w.journal)).teardown).toMatchObject({
      phase: 'complete',
      failure: 'residual-present',
    });
  });

  it('settles a namespace that disappears part way through the loop', async () => {
    const w = await world();
    w.state.namespaces.push({ id: 'slow', script: w.names.referenceWorker });
    let attempts = 0;
    w.setHook((request, url) => {
      if (
        request.method !== 'GET' ||
        url.pathname !== `${ROOT}/workers/durable_objects/namespaces` ||
        url.searchParams.has('page')
      )
        return undefined;
      attempts += 1;
      if (attempts >= 3) w.state.namespaces.splice(0);
      return undefined;
    });
    const outcome = await w.run();
    expect(outcome.status).toBe('cleaned');
    expect(present(outcome.facts.residual).settleAttempts).toBe(3);
  });

  it('re-runs only the residual settle after a recorded residual failure', async () => {
    const w = await world();
    w.state.extraDatabases.push({
      uuid: 'residual-uuid',
      name: `${w.prefix}-left-behind`,
    });
    expect(retained(await w.run()).reason).toBe('residual-present');
    const mark = w.requests.length;
    w.state.extraDatabases.splice(0);
    const outcome = await w.run();
    expect(outcome.status).toBe('cleaned');
    expect(present(outcome.facts.residual).settleAttempts).toBe(1);
    expect(
      w.requests.slice(mark).some((entry) => entry.startsWith('DELETE')),
    ).toBe(false);
  });

  it('nulls every global count and issues no unfiltered list off a shared account', async () => {
    const w = await world({ disposableAccount: false });
    const outcome = await w.run();
    expect(outcome.status).toBe('cleaned');
    for (const surface of Object.values(
      present(outcome.facts.residual).surfaces,
    ))
      expect(surface.globalCount).toBeNull();
    expect(
      w.requests.filter((entry) => entry === `GET ${ROOT}/d1/database`).length,
    ).toBe(1);
  });

  it('retains the pending mutation when the budget is exhausted mid-sequence', async () => {
    const w = await world();
    w.setHook((request, url) => {
      if (request.method === 'DELETE' && url.pathname === w.script)
        throw new DirectProviderError('budget-exhausted');
      return undefined;
    });
    const outcome = retained(await w.run());
    expect(outcome.reason).toBe('budget-exhausted');
    expect((await diskState(w.journal)).teardown).toMatchObject({
      phase: 'worker',
      pending: { kind: 'delete-reference-worker' },
    });
  });

  it('keeps the API token and request headers out of the journal', async () => {
    const w = await world();
    await w.run();
    const bytes = await readFile(
      join(w.journal.directory, 'journal.json'),
      'utf8',
    );
    expect(bytes).not.toContain(API_TOKEN);
    expect(bytes).not.toContain('Bearer');
    expect(bytes).not.toContain('authorization');
    expect(bytes).toContain('CLOUDFLARE_API_TOKEN');
  });
});
