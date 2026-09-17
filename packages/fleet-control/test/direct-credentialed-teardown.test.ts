// SPDX-License-Identifier: Apache-2.0

import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { DirectProviderError } from '../scripts/direct-credentialed-provider.mjs';
import {
  DIRECT_TEARDOWN_MAXIMA,
  REFERENCE_SECRET_NAMES,
} from '../scripts/direct-credentialed-reference-vocabulary.mjs';
import type { DirectRunJournal } from '../scripts/direct-credentialed-run-state.mjs';
import type { DirectTeardownOutcome } from '../scripts/direct-credentialed-teardown.mjs';
import { teardownDirectReference } from '../scripts/direct-credentialed-teardown.mjs';
import { CLOUDFLARE_INVENTORY_BOUND } from '../src/cloudflare-client-config.js';
import { providerJson as json } from './fixtures/direct-observations.js';
import {
  bootstrapContext,
  cleanupDirectRunState,
  closed,
  completeScenario,
  completeScenarioJournal,
  exportKey,
  fixture,
  opened,
  present,
  scenarioJournal,
  teardownWith,
} from './fixtures/direct-run-state-builder.js';

const API_TOKEN = 'teardown/provider-token+sentinel==';
const ACCOUNT = 'account';
const ROOT = `/client/v4/accounts/${ACCOUNT}`;
const ROUTES = '/client/v4/zones/zone/workers/routes';
// The listing answers in the provider's order, which teardown sorts before it
// compares; the names themselves are the upload's own list.
const SECRET_NAMES = [...REFERENCE_SECRET_NAMES].sort();
const unexpectedRequests: string[] = [];

type Hook = (
  request: Request,
  url: URL,
) => Promise<Response | undefined> | Response | undefined;
type Row = Record<string, unknown>;
// The world collections whose rows the residual scan classifies by one field.
type ListedSurface = 'scripts' | 'routes' | 'domains' | 'queues' | 'namespaces';

const absent = (status = 404) =>
  Response.json(
    { success: false, errors: [{ code: 10000, message: 'synthetic absence' }] },
    { status },
  );
const forbid = () => absent(403);
const deployments = (versionId: string) =>
  json({
    deployments: [
      {
        id: 'deployment',
        strategy: 'percentage',
        versions: [{ version_id: versionId, percentage: 100 }],
      },
    ],
  });
// The bucket read the identity check refuses: the run's own name carrying a
// creation date that is not the one the bootstrap recorded.
const changedBucket = (w: { names: { exportBucket: string } }) =>
  json({
    name: w.names.exportBucket,
    creation_date: '2020-01-01T00:00:00.000Z',
  });
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
    corroborate?: boolean;
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
    queues: [] as Row[],
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
  // The default world sends no `result_info` on any listing: the live shape for
  // scripts and routes, and the uncorroborated case for the rest.
  // `corroborate` opts into the attested shape.
  const listing = (rows: Row[]) =>
    options.corroborate === true
      ? json(rows, { total_count: rows.length })
      : json(rows);
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
      if (path === `${script}/deployments`) return deployments('version');
      if (path === `${script}/versions`)
        return state.scriptPresent ? json({ items: state.versions }) : absent();
      if (path === script)
        return state.scriptPresent
          ? new Response('synthetic worker bytes', {
              headers: { 'Content-Type': 'application/javascript' },
            })
          : absent();
      if (path === `${ROOT}/workers/scripts`) return listing(residualScripts());
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
      if (path === `${ROOT}/workers/domains`) return listing(state.domains);
      if (path === ROUTES) return listing(state.routes);
      if (path === `${ROOT}/queues`) return listing(state.queues);
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
      // disable-reference-ingress: probe, identity, disable, reread
      `GET ${w.script}/subdomain`,
      `GET ${w.script}/deployments`,
      `POST ${w.script}/subdomain`,
      `GET ${w.script}/subdomain`,

      // delete-reference-worker: probe, identity (version and secret set),
      // delete, reread
      `GET ${w.script}`,
      `GET ${w.script}/deployments`,
      `GET ${w.script}/secrets`,
      `DELETE ${w.script}`,
      `GET ${w.script}`,

      // delete-fleet-d1: probe, identity, delete, reread
      `GET ${ROOT}/d1/database/fleet-uuid`,
      `GET ${ROOT}/d1/database/fleet-uuid`,
      `DELETE ${ROOT}/d1/database/fleet-uuid`,
      `GET ${ROOT}/d1/database/fleet-uuid`,

      // delete-quota-d1: probe, identity, delete, reread
      `GET ${ROOT}/d1/database/quota-uuid`,
      `GET ${ROOT}/d1/database/quota-uuid`,
      `DELETE ${ROOT}/d1/database/quota-uuid`,
      `GET ${ROOT}/d1/database/quota-uuid`,

      // export objects: one bucket attestation, then the prefix-scoped and
      // whole-bucket listings that admit the keys
      `GET ${w.bucketPath}`,
      `GET ${w.bucketPath}/objects`,
      `GET ${w.bucketPath}/objects`,

      // delete-export-object a: probe, delete, reread
      `GET ${w.bucketPath}/objects/${w.keyA}`,
      `DELETE ${w.bucketPath}/objects/${w.keyA}`,
      `GET ${w.bucketPath}/objects/${w.keyA}`,

      // delete-export-object b: probe, delete, reread
      `GET ${w.bucketPath}/objects/${w.keyB}`,
      `DELETE ${w.bucketPath}/objects/${w.keyB}`,
      `GET ${w.bucketPath}/objects/${w.keyB}`,

      // the prefix settles empty before the bucket goes
      `GET ${w.bucketPath}/objects`,

      // delete-export-r2: probe, delete, reread — its identity is the
      // attestation above
      `GET ${w.bucketPath}`,
      `DELETE ${w.bucketPath}`,
      `GET ${w.bucketPath}`,

      // the residual scan, one surface at a time
      `GET ${ROOT}/d1/database`,
      `GET ${ROOT}/d1/database`,
      `GET ${ROOT}/workers/durable_objects/namespaces`,
      `GET ${ROOT}/workers/scripts`,
      `GET ${ROOT}/r2/buckets`,
      `GET ${ROOT}/workers/domains`,
      `GET ${ROUTES}`,
      `GET ${ROOT}/queues`,
      `GET ${ROOT}/workers/dispatch/namespaces`,
      `GET ${w.script}/versions`,
    ]);
    // Each ordinal below is the position of that step's own reread in the list
    // above: the receipt is written once the probe has seen the resource gone.
    expect(outcome.facts.receipts).toMatchObject({
      ingress: { ordinal: 4, settledByReread: false },
      worker: {
        scriptName: w.names.referenceWorker,
        secretNames: SECRET_NAMES,
        ordinal: 9,
        settledByReread: false,
      },
      fleet: { uuid: 'fleet-uuid', ordinal: 13, settledByReread: false },
      quota: { uuid: 'quota-uuid', ordinal: 17, settledByReread: false },
      exports: {
        name: w.names.exportBucket,
        ordinal: 30,
        settledByReread: false,
      },
    });
    expect(outcome.facts.receipts.exportObjects).toEqual([
      { key: w.keyA, ordinal: 23, settledByReread: false },
      { key: w.keyB, ordinal: 26, settledByReread: false },
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
    expect(outcome.facts.retainedIdentities).toEqual({
      fleetUuid: 'fleet-uuid',
      quotaUuid: 'quota-uuid',
      exportBucket: w.names.exportBucket,
      scriptName: w.names.referenceWorker,
      activeVersionId: 'version',
    });
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
    const incomplete = retained(await call());
    expect(incomplete).toMatchObject({
      reason: 'invalid-state',
      phase: 'refused',
    });
    await journal.beginBootstrapMutation('create-fleet-d1');
    const pending = retained(await call());
    expect(pending).toMatchObject({
      reason: 'outcome-unknown',
      phase: 'refused',
    });
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

  it('re-enters deletion from a recorded refusal the run has since cleared', async () => {
    const w = await world();
    await w.journal.recordTeardown(
      teardownWith((state) => {
        state.phase = 'refused';
        state.failure = 'scenario-incomplete';
      }),
    );
    const outcome = await w.run();
    expect(outcome.status).toBe('cleaned');
    expect((await diskState(w.journal)).teardown).toMatchObject({
      phase: 'complete',
      failure: null,
    });
  });

  it.each([
    1, 3,
  ])('refuses a confirmed export set of %d keys', async (count) => {
    const w = await world({ complete: false });
    const state = completeScenario();
    const a = present(state.proofs.exports.a);
    if (count === 1) {
      present(state.proofs.exports.b).receipt = structuredClone(a.receipt);
      state.proofs.exportVerifications = state.proofs.exportVerifications.map(
        () => structuredClone(a),
      );
    } else {
      // A third distinct receipt: the count is exactly two, not "at most the
      // journal's `exportObjects` maximum".
      const third = structuredClone(a);
      third.receipt.operationId = `${a.receipt.operationId}-third`;
      state.proofs.exportVerifications = [third];
    }
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

    const resumed = await world();
    resumed.setHook((request, url) =>
      request.method === 'DELETE' && url.pathname === resumed.script
        ? json([])
        : undefined,
    );
    expect(retained(await resumed.run()).reason).toBe('outcome-unknown');
    resumed.setHook((request, url) =>
      request.method === 'GET' && url.pathname === resumed.script
        ? absent(500)
        : undefined,
    );
    expect(retained(await resumed.run()).reason).toBe('provider-unavailable');
    // The probe refuses ahead of every write, so the record the earlier run
    // left pending is still the record a later run re-enters on.
    expect((await diskState(resumed.journal)).teardown).toMatchObject({
      phase: 'worker',
      pending: { kind: 'delete-reference-worker' },
      receipts: { worker: null },
    });
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
    // No second delete of any shape for that database: the resumed run reads
    // it and settles the receipt by that read.
    expect(
      w.requests
        .slice(mark)
        .filter((entry) => entry.includes(`${ROOT}/d1/database/fleet-uuid`)),
    ).toEqual([`GET ${ROOT}/d1/database/fleet-uuid`]);
  });

  it('lists the receipts prefix on a resume whose object receipts are complete', async () => {
    const w = await world();
    w.setHook((request, url) =>
      request.method === 'DELETE' && url.pathname === w.bucketPath
        ? json([])
        : undefined,
    );
    const first = retained(await w.run());
    expect(first.reason).toBe('outcome-unknown');
    expect(first.facts.receipts.exportObjects).toHaveLength(2);
    w.setHook(undefined);
    w.state.objects.add(`${w.prefix}/receipts/v1/other/object.sql`);
    const mark = w.requests.length;
    // Every object receipt is recorded, so the deletes are skipped; ownership
    // is attested, the prefix is still read before the bucket goes, and an
    // object that was not there when the receipts were written refuses here.
    expect(retained(await w.run()).reason).toBe('unexpected-object');
    expect(w.requests.slice(mark)).toEqual([
      `GET ${w.bucketPath}`,
      `GET ${w.bucketPath}/objects`,
    ]);
    expect(w.state.bucketPresent).toBe(true);
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
          return changedBucket(w);
        if (kind === 'script' && url.pathname === `${w.script}/deployments`)
          return deployments('other-version');
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

  it('keeps ingress present while preview URLs stay enabled', async () => {
    const w = await world();
    let reads = 0;
    w.setHook((request, url) => {
      if (request.method !== 'GET' || url.pathname !== `${w.script}/subdomain`)
        return undefined;
      reads += 1;
      return reads === 1
        ? json({ enabled: false, previews_enabled: true })
        : undefined;
    });
    const outcome = await w.run();
    expect(outcome.status).toBe('cleaned');
    // On `enabled` alone that first read settles the step: the disable is never
    // issued and the receipt records a reread that never happened.
    expect(w.requests).toContain(`POST ${w.script}/subdomain`);
    expect(outcome.facts.receipts.ingress).toMatchObject({
      settledByReread: false,
    });
  });

  it('attests the export bucket once for the object deletes and the bucket delete', async () => {
    const w = await world();
    const outcome = await w.run();
    expect(outcome.status).toBe('cleaned');
    // The attestation, then the bucket delete's own probe and reread. A second
    // attestation would read the bucket a fourth time.
    expect(
      w.requests.filter((entry) => entry === `GET ${w.bucketPath}`),
    ).toHaveLength(3);
    expect(w.requests.indexOf(`GET ${w.bucketPath}`)).toBeLessThan(
      w.requests.findIndex((entry) =>
        entry.startsWith(`DELETE ${w.bucketPath}`),
      ),
    );
  });

  it('refuses a resume owing only the bucket delete before it reads the prefix', async () => {
    const w = await world();
    w.setHook((request, url) =>
      request.method === 'DELETE' && url.pathname === w.bucketPath
        ? json([])
        : undefined,
    );
    expect(retained(await w.run()).reason).toBe('outcome-unknown');
    w.setHook((request, url) =>
      request.method === 'GET' && url.pathname === w.bucketPath
        ? changedBucket(w)
        : undefined,
    );
    w.state.objects.add(`${w.prefix}/receipts/v1/other/object.sql`);
    const mark = w.requests.length;
    // Ownership is proven first, so the changed identity refuses ahead of the
    // listing that would otherwise report the object as unexpected.
    expect(retained(await w.run()).reason).toBe('identity-mismatch');
    expect(w.requests.slice(mark)).toEqual([`GET ${w.bucketPath}`]);
    expect(w.state.bucketPresent).toBe(true);
  });

  it('refuses a first-attempt fleet database delete whose name changed', async () => {
    const w = await world();
    w.state.databases.set('fleet-uuid', {
      uuid: 'fleet-uuid',
      name: `${w.names.fleetDatabase}-replacement`,
    });
    expect(retained(await w.run()).reason).toBe('identity-mismatch');
    expect(w.requests).not.toContain(`DELETE ${ROOT}/d1/database/fleet-uuid`);
    expect(w.state.databases.has('fleet-uuid')).toBe(true);
  });

  it('refuses a first-attempt worker delete whose active version changed', async () => {
    const w = await world();
    let reads = 0;
    w.setHook((request, url) => {
      if (
        request.method !== 'GET' ||
        url.pathname !== `${w.script}/deployments`
      )
        return undefined;
      reads += 1;
      return reads === 1 ? undefined : deployments('other-version');
    });
    expect(retained(await w.run()).reason).toBe('identity-mismatch');
    expect(w.requests).toContain(`POST ${w.script}/subdomain`);
    expect(w.requests).not.toContain(`DELETE ${w.script}`);
    expect(w.state.scriptPresent).toBe(true);
  });

  it('refuses a secret set carrying an extra unrecordable name', async () => {
    for (const extra of [
      'n'.repeat(DIRECT_TEARDOWN_MAXIMA.nameBytes + 1),
      `CONTROL${String.fromCharCode(1)}NAME`,
    ]) {
      const w = await world();
      w.state.secretNames.push(extra);
      expect(retained(await w.run()).reason).toBe('identity-mismatch');
      expect(w.requests).not.toContain(`DELETE ${w.script}`);
      expect(w.state.scriptPresent).toBe(true);
    }
  });

  it('refuses a first-attempt ingress disable whose active version changed', async () => {
    const w = await world();
    w.setHook((request, url) =>
      request.method === 'GET' && url.pathname === `${w.script}/deployments`
        ? deployments('other-version')
        : undefined,
    );
    expect(retained(await w.run()).reason).toBe('identity-mismatch');
    expect(w.requests).not.toContain(`POST ${w.script}/subdomain`);
    expect(w.state.ingress).toBe(true);
  });

  it('refuses a first-attempt export object delete whose bucket changed', async () => {
    const w = await world();
    w.setHook((request, url) =>
      request.method === 'GET' && url.pathname === w.bucketPath
        ? changedBucket(w)
        : undefined,
    );
    expect(retained(await w.run()).reason).toBe('identity-mismatch');
    expect(
      w.requests.filter(
        (entry) => entry.startsWith('DELETE') && entry.includes('/objects/'),
      ),
    ).toEqual([]);
    expect([...w.state.objects]).toHaveLength(2);
  });

  it('settles a first-attempt step whose probe already finds the resource absent', async () => {
    const w = await world();
    w.state.databases.delete('fleet-uuid');
    const outcome = await w.run();
    expect(outcome.status).toBe('cleaned');
    expect(outcome.facts.receipts.fleet).toMatchObject({
      uuid: 'fleet-uuid',
      settledByReread: true,
    });
    expect(
      w.requests.filter(
        (entry) => entry === `GET ${ROOT}/d1/database/fleet-uuid`,
      ),
    ).toHaveLength(1);
    expect(w.requests).not.toContain(`DELETE ${ROOT}/d1/database/fleet-uuid`);
    expect((await diskState(w.journal)).teardown).toMatchObject({
      phase: 'complete',
      pending: null,
      failure: null,
    });
  });

  it('refuses a forbidden residual surface and records a fail-closed dispatch', async () => {
    for (const surface of [
      `${ROOT}/d1/database`,
      `${ROOT}/workers/durable_objects/namespaces`,
      `${ROOT}/workers/scripts`,
      `${ROOT}/r2/buckets`,
      `${ROOT}/workers/domains`,
      ROUTES,
      `${ROOT}/queues`,
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

    // A page that repeats the name it was asked to start after: the ordering
    // rule is `<=`, so a repeat refuses exactly as an out-of-order name does.
    const repeated = await world();
    repeated.setHook((request, url) =>
      request.method === 'GET' && url.pathname === `${ROOT}/r2/buckets`
        ? json({ buckets: [{ name: 'zzz' }] })
        : undefined,
    );
    expect(retained(await repeated.run()).reason).toBe('provider-unavailable');
  });

  it('bounds the bucket inventory with the session limit it is given', async () => {
    const w = await world();
    w.setHook((request, url) =>
      request.method === 'GET' && url.pathname === `${ROOT}/r2/buckets`
        ? json({
            buckets: Array.from(
              { length: CLOUDFLARE_INVENTORY_BOUND + 1 },
              (_row, index) => ({
                name: `bucket-${`${index}`.padStart(6, '0')}`,
              }),
            ),
          })
        : undefined,
    );
    // Without the session's bound the loop would read the whole page, see an
    // empty second page and settle; the refusal is the bound arriving.
    expect(retained(await w.run()).reason).toBe('provider-unavailable');
  });

  it('refuses a listed row whose classifying field is not a string', async () => {
    for (const [surface, row] of [
      ['scripts', {}],
      ['routes', { script: 7 }],
      ['domains', {}],
      ['queues', {}],
      ['namespaces', { id: 'orphan' }],
    ] as [ListedSurface, Row][]) {
      // A shared account records no global count, so without this refusal the
      // unclassifiable row would leave a zero prefix count and settle.
      const w = await world({ disposableAccount: false });
      w.state[surface].push(row);
      expect(retained(await w.run()).reason).toBe('provider-unavailable');
    }
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

  it('lists domains and routes inside the recorded zone', async () => {
    const w = await world();
    const scoped: string[] = [];
    w.setHook((_request, url) => {
      if (url.pathname === `${ROOT}/workers/domains`)
        scoped.push(`domains zone_id=${url.searchParams.get('zone_id')}`);
      if (url.pathname === ROUTES) scoped.push(`routes ${url.pathname}`);
      return undefined;
    });
    const outcome = await w.run();
    expect(outcome.status).toBe('cleaned');
    // Both counts these surfaces record are the zone's, the way
    // `bucketJurisdictions` records the jurisdiction the bucket count covers.
    expect(scoped).toEqual(['domains zone_id=zone', `routes ${ROUTES}`]);
    expect(present(outcome.facts.residual).bucketJurisdictions).toEqual([
      'default',
    ]);
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
    // Mid-sequence: the phases before the exhausted call keep their receipts,
    // and the call it stopped on stays pending rather than being rolled back.
    expect((await diskState(w.journal)).teardown).toMatchObject({
      phase: 'worker',
      pending: { kind: 'delete-reference-worker' },
      receipts: { ingress: { settledByReread: false }, worker: null },
    });
  });

  it('records a prefixed queue as a residual', async () => {
    const left = await world();
    left.state.queues.push({ queue_name: `${left.prefix}-left-behind` });
    const retainedOutcome = retained(await left.run());
    expect(retainedOutcome.reason).toBe('residual-present');
    expect(present(retainedOutcome.facts.residual).surfaces.queues).toEqual({
      prefixCount: 1,
      prefixNames: [`${left.prefix}-left-behind`],
      globalCount: 1,
      exhaustive: false,
    });
  });

  it('records the provider attestation on scripts, domains, routes and queues', async () => {
    const surfaces = ['scripts', 'domains', 'routes', 'queues'] as const;
    const plain = await world();
    const uncorroborated = await plain.run();
    expect(uncorroborated.status).toBe('cleaned');
    for (const name of surfaces)
      expect(present(uncorroborated.facts.residual).surfaces[name]).toEqual({
        prefixCount: 0,
        prefixNames: [],
        globalCount: 0,
        exhaustive: false,
      });
    const attested = await world({ corroborate: true });
    const corroborated = await attested.run();
    expect(corroborated.status).toBe('cleaned');
    for (const name of surfaces)
      expect(present(corroborated.facts.residual).surfaces[name]).toEqual({
        prefixCount: 0,
        prefixNames: [],
        globalCount: 0,
        exhaustive: true,
      });
  });

  it('settles a shared account on the prefix-scoped claim alone', async () => {
    const w = await world({ disposableAccount: false });
    w.state.scripts.push({ id: 'unrelated-worker' });
    const outcome = await w.run();
    expect(outcome.status).toBe('cleaned');
    expect(present(outcome.facts.residual).surfaces.scripts).toEqual({
      prefixCount: 0,
      prefixNames: [],
      globalCount: null,
      exhaustive: false,
    });
  });

  it('reads a queues 404 as an account without a queue collection', async () => {
    const w = await world();
    w.setHook((request, url) =>
      request.method === 'GET' && url.pathname === `${ROOT}/queues`
        ? absent()
        : undefined,
    );
    const outcome = await w.run();
    expect(outcome.status).toBe('cleaned');
    expect(present(outcome.facts.residual).surfaces.queues).toEqual({
      prefixCount: 0,
      prefixNames: [],
      globalCount: 0,
      exhaustive: false,
    });
  });

  it('refuses a journal recorded before the queues surface', async () => {
    const w = await world();
    expect((await w.run()).status).toBe('cleaned');
    await closed(w.journal);
    const path = join(w.journal.directory, 'journal.json');
    // Control: the same journal resumes while the surface is there, so the
    // refusal below is the missing surface and not another part of the record.
    await closed(await opened({ ...w.f.input, mode: 'resume' }));
    const snapshot = JSON.parse(await readFile(path, 'utf8')) as {
      teardown: { residual: { surfaces: Record<string, unknown> } };
    };
    delete snapshot.teardown.residual.surfaces.queues;
    await writeFile(path, `${JSON.stringify(snapshot)}\n`);
    await expect(
      opened({ ...w.f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'invalid-state' });
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
