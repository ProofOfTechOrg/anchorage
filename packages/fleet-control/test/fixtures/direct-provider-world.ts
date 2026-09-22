// SPDX-License-Identifier: Apache-2.0

import { expect, vi } from 'vitest';
import { REFERENCE_SECRET_NAMES } from '../../scripts/direct-credentialed-reference-vocabulary.mjs';
import { teardownDirectReference } from '../../scripts/direct-credentialed-teardown.mjs';
import { providerJson as json } from './direct-observations.js';
import {
  completeScenarioJournal,
  exportKey,
  present,
  scenarioJournal,
} from './direct-run-state-builder.js';

export const API_TOKEN = 'teardown/provider-token+sentinel==';
export const ACCOUNT = 'account';
export const ROOT = `/client/v4/accounts/${ACCOUNT}`;
export const ROUTES = '/client/v4/zones/zone/workers/routes';
export const SECRET_NAMES = [...REFERENCE_SECRET_NAMES].sort();
export const unexpectedRequests: string[] = [];

export type Hook = (
  request: Request,
  url: URL,
) => Promise<Response | undefined> | Response | undefined;
export type Row = Record<string, unknown>;
export type ListedSurface =
  | 'scripts'
  | 'routes'
  | 'domains'
  | 'queues'
  | 'namespaces';

export const absent = (status = 404) =>
  Response.json(
    { success: false, errors: [{ code: 10000, message: 'synthetic absence' }] },
    { status },
  );
export const forbid = () => absent(403);
export const deployments = (versionId: string) =>
  json({
    deployments: [
      {
        id: 'deployment',
        strategy: 'percentage',
        versions: [{ version_id: versionId, percentage: 100 }],
      },
    ],
  });
const paged = (url: URL, rows: Row[]) =>
  json(url.searchParams.has('page') ? [] : rows);

export async function providerWorld(
  options: {
    limit?: number;
    disposableAccount?: boolean;
    complete?: boolean;
    corroborate?: boolean;
    tenants?: boolean;
  } = {},
) {
  const limit = options.limit ?? 16;
  const disposable = options.disposableAccount ?? true;
  const { f, journal } =
    options.complete === false
      ? await scenarioJournal(limit, disposable)
      : await completeScenarioJournal(limit, disposable);
  const names = f.prepared.names;
  const prefix = f.prepared.config.resourcePrefix;
  const script = `${ROOT}/workers/scripts/${names.referenceWorker}`;
  const bucketPath = `${ROOT}/r2/buckets/${names.exportBucket}`;
  const keys = [
    exportKey(prefix, 'a'),
    exportKey(prefix, 'b'),
    exportKey(prefix, 'reprovision'),
  ];
  const objects = new Set(keys);
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
    objects,
    bucketObjects: new Map<string, Set<string>>([
      [names.exportBucket, objects],
    ]),
    extraDatabases: [] as Row[],
    extraBuckets: [] as Row[],
    namespaces: [] as Row[],
    scripts: [] as Row[],
    domains: [] as Row[],
    routes: [] as Row[],
    queues: [] as Row[],
    dispatch: [] as Row[],
  };
  if (options.tenants) {
    for (const [role, tenant] of Object.entries(names.roles)) {
      state.databases.set(`tenant-${role}-uuid`, {
        uuid: `tenant-${role}-uuid`,
        name: tenant.databaseName,
      });
      state.scripts.push({ id: tenant.scriptName });
      state.namespaces.push(
        { id: `${role}-maintenance`, script: tenant.scriptName },
        { id: `${role}-runner`, script: tenant.scriptName },
      );
      state.domains.push({
        id: `${role}-domain`,
        hostname: tenant.routeHostname,
        service: tenant.scriptName,
      });
      state.routes.push({
        id: `${role}-route`,
        pattern: `${tenant.routeHostname}/*`,
        script: tenant.scriptName,
      });
      const bucketName = `${tenant.scriptName}-application`;
      state.extraBuckets.push({ name: bucketName });
      state.bucketObjects.set(bucketName, new Set());
    }
  }
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
      if (path === `${ROOT}`) return json({ id: ACCOUNT });
      if (path === '/client/v4/zones')
        return paged(url, [
          {
            id: 'zone',
            name: f.prepared.config.ownedHostname,
            type: 'full',
            account: { id: ACCOUNT },
          },
        ]);
      if (path === '/client/v4/zones/zone')
        return json({
          id: 'zone',
          name: f.prepared.config.ownedHostname,
          type: 'full',
          account: { id: ACCOUNT },
        });
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
      const objectList = path.match(
        new RegExp(`^${ROOT}/r2/buckets/([^/]+)/objects$`, 'u'),
      );
      if (objectList) {
        const bucketObjects =
          state.bucketObjects.get(present(objectList[1])) ?? new Set<string>();
        const scoped = url.searchParams.get('prefix');
        return json(
          [...bucketObjects]
            .filter((key) => scoped === null || key.startsWith(scoped))
            .map((key) => ({ key })),
        );
      }
      const objectRead = path.match(
        new RegExp(`^${ROOT}/r2/buckets/([^/]+)/objects/(.+)$`, 'u'),
      );
      if (objectRead)
        return state.bucketObjects
          .get(present(objectRead[1]))
          ?.has(present(objectRead[2]))
          ? new Response('synthetic export bytes')
          : absent();
      const bucketRead = path.match(
        new RegExp(`^${ROOT}/r2/buckets/([^/]+)$`, 'u'),
      );
      if (bucketRead) {
        const bucketName = present(bucketRead[1]);
        const presentBucket = residualBuckets().some(
          (row) => row.name === bucketName,
        );
        return presentBucket
          ? json({
              name: bucketName,
              creation_date: '2026-09-10T00:00:00.000Z',
            })
          : absent();
      }
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
      if (path.startsWith(`${ROOT}/workers/domains/`)) {
        const id = path.slice(`${ROOT}/workers/domains/`.length);
        state.domains = state.domains.filter((row) => row.id !== id);
        return json(null);
      }
      if (path.startsWith(`${ROUTES}/`)) {
        const id = path.slice(`${ROUTES}/`.length);
        state.routes = state.routes.filter((row) => row.id !== id);
        return json(null);
      }
      if (path.startsWith(`${ROOT}/workers/scripts/`)) {
        const scriptName = path.slice(`${ROOT}/workers/scripts/`.length);
        if (scriptName === names.referenceWorker) state.scriptPresent = false;
        else {
          state.scripts = state.scripts.filter((row) => row.id !== scriptName);
          state.namespaces = state.namespaces.filter(
            (row) => row.script !== scriptName,
          );
        }
        return json(null);
      }
      if (path.startsWith(`${ROOT}/d1/database/`)) {
        state.databases.delete(path.slice(`${ROOT}/d1/database/`.length));
        return json(null);
      }
      const objectDelete = path.match(
        new RegExp(`^${ROOT}/r2/buckets/([^/]+)/objects/(.+)$`, 'u'),
      );
      if (objectDelete) {
        state.bucketObjects
          .get(present(objectDelete[1]))
          ?.delete(present(objectDelete[2]));
        return json({});
      }
      const bucketDelete = path.match(
        new RegExp(`^${ROOT}/r2/buckets/([^/]+)$`, 'u'),
      );
      if (bucketDelete) {
        const bucketName = present(bucketDelete[1]);
        if (bucketName === names.exportBucket) state.bucketPresent = false;
        else
          state.extraBuckets = state.extraBuckets.filter(
            (row) => row.name !== bucketName,
          );
        state.bucketObjects.delete(bucketName);
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
    keyC: present(keys[2]),
    state,
    requests,
    fetch: fetchRequest,
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
