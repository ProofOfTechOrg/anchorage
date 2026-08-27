// SPDX-License-Identifier: Apache-2.0

import { expect } from 'vitest';
import type { CloudflareProvisioningClient } from '../../src/cloudflare-client.js';
import {
  type CloudflareApiRateCoordinator,
  ProcessLocalCloudflareApiRateCoordinator,
} from '../../src/cloudflare-rate-coordinator.js';
import {
  maintenanceResponder,
  type ProviderWorld,
  type WorkerRoute,
} from './provider-world.js';

type PageInfo = Readonly<{
  page?: number;
  per_page?: number;
  count?: number;
  total_count?: number;
  total_pages?: number;
  cursor?: string;
  cursors?: Readonly<{ after?: string }>;
}>;

function page(result: unknown, info: PageInfo): Response {
  let items: readonly unknown[] | undefined;
  if (Array.isArray(result)) {
    items = result;
  } else if (result && typeof result === 'object') {
    const candidate = Reflect.get(result, 'items');
    if (Array.isArray(candidate)) items = candidate;
  }
  const count = items?.length ?? 1;
  return Response.json({
    success: true,
    errors: [],
    messages: [],
    result,
    result_info: {
      page: 1,
      per_page: count,
      count,
      total_count: count,
      total_pages: 1,
      ...info,
    },
  });
}

export function pageArray(
  items: readonly unknown[],
  info: PageInfo = {},
): Response {
  return page(items, info);
}

export function pageItems(
  items: readonly unknown[],
  info: PageInfo = {},
): Response {
  return page({ items }, info);
}

export function single(result: unknown): Response {
  return Response.json({
    success: true,
    errors: [],
    messages: [],
    result,
  });
}

export function envelope(result: unknown): Response {
  return page(result, { per_page: 20 });
}

export function zoneAuthorityResponse(
  url: URL,
  zoneIds: readonly string[],
  routes?: readonly WorkerRoute[],
): Response | undefined {
  if (url.pathname.endsWith('/user/tokens/verify')) {
    return envelope({ id: 'token-id', status: 'active' });
  }
  if (url.pathname.endsWith('/accounts/account/tokens/token-id')) {
    return envelope({
      id: 'token-id',
      status: 'active',
      policies: [
        {
          id: 'zone-authority',
          effect: 'allow',
          permission_groups: [
            { id: 'zone-read', name: 'Zone Read' },
            { id: 'routes-read', name: 'Workers Routes Read' },
            { id: 'routes-write', name: 'Workers Routes Write' },
          ],
          resources: {
            'com.cloudflare.api.account.account': {
              'com.cloudflare.api.account.zone.*': '*',
            },
          },
        },
      ],
    });
  }
  if (url.pathname.endsWith('/zones')) {
    expect(url.searchParams.get('account.id')).toBe('account');
    if (url.searchParams.has('page')) return envelope([]);
    return envelope(zoneIds.map((id) => ({ id, account: { id: 'account' } })));
  }
  const parts = url.pathname.split('/').filter(Boolean);
  const zoneIndex = parts.indexOf('zones');
  const zoneId = zoneIndex >= 0 ? parts[zoneIndex + 1] : undefined;
  if (routes && zoneId && url.pathname.endsWith('/workers/routes')) {
    return envelope(
      routes
        .filter((route) => route.zoneId === zoneId)
        .map(({ id, pattern, script }) => ({ id, pattern, script })),
    );
  }
  return undefined;
}

export function fenced<T>(
  client: CloudflareProvisioningClient,
  operation: () => Promise<T>,
  events?: string[],
): Promise<T> {
  return client.withMutationFence(
    {
      mutationLeaseTtlMs: 15 * 60_000,
      assertOwned: async () => {
        events?.push('assertOwned');
      },
    },
    operation,
  );
}

export function testRateCoordinator(
  intervalCap?: number,
  events?: string[],
): CloudflareApiRateCoordinator {
  const coordinator = new ProcessLocalCloudflareApiRateCoordinator(intervalCap);
  if (!events) return coordinator;
  return {
    acquire: async (signal) => {
      events.push('quota:acquire');
      await coordinator.acquire(signal);
    },
  };
}

export function deferred<T>(): {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

export interface CloudflareFetchRecord {
  readonly method: string;
  readonly url: string;
  readonly body: unknown;
}

export interface CloudflareFixtureRequest extends CloudflareFetchRecord {
  readonly headers: Headers;
  readonly redirect: RequestInit['redirect'];
}

export type CloudflareFixtureHandler = (
  request: CloudflareFixtureRequest,
) => Response | Promise<Response>;

// cloudflare/internal/uploads.mjs:51-74 probes whether this fetch
// implementation encodes a real FormData body (instead of stringifying it)
// before deciding whether multipart uploads are usable.
class UnsupportedFormDataResponse {
  constructor(readonly body: unknown) {}

  async text(): Promise<string> {
    return String(this.body);
  }
}

async function decodeBody(body: BodyInit | null | undefined): Promise<unknown> {
  if (body instanceof FormData) {
    const files: Array<{ name: string; type: string; text: string }> = [];
    const fields: Record<string, unknown> = {};
    for (const [name, value] of body.entries()) {
      if (value instanceof File) {
        files.push({
          name: value.name,
          type: value.type,
          text: await value.text(),
        });
      } else if (name === 'metadata') {
        fields[name] = JSON.parse(value);
      } else {
        fields[name] = value;
      }
    }
    return { ...fields, files };
  }
  if (typeof body === 'string') {
    try {
      return JSON.parse(body);
    } catch {
      return body;
    }
  }
  return body ?? undefined;
}

export function recordingFetch(
  handler: CloudflareFixtureHandler,
  events: string[] = [],
  options: { readonly formDataProbe?: 'unsupported' } = {},
): {
  readonly fetch: typeof fetch;
  readonly requests: CloudflareFetchRecord[];
  readonly probes: string[];
  readonly events: string[];
} {
  const requests: CloudflareFetchRecord[] = [];
  const probes: string[] = [];
  const fixtureFetch: typeof fetch = async (input, init) => {
    const url = new URL(
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.href
          : input.url,
    );
    if (url.protocol === 'data:') {
      probes.push(url.href);
      if (options.formDataProbe === 'unsupported') {
        return new UnsupportedFormDataResponse(
          new FormData(),
        ) as unknown as Response;
      }
      return new Response(new FormData());
    }
    const method = (
      init?.method ?? (input instanceof Request ? input.method : 'GET')
    ).toUpperCase();
    const body = await decodeBody(init?.body);
    const record = { method, url: url.href, body };
    requests.push(record);
    events.push(`request:${url.pathname}`);
    return handler({
      ...record,
      headers: new Headers(init?.headers),
      redirect: init?.redirect,
    });
  };
  return { fetch: fixtureFetch, requests, probes, events };
}

export function restProjection(world: ProviderWorld): CloudflareFixtureHandler {
  return async (request) => {
    const { method, url, body } = request;
    const target = new URL(url);
    const maintenance = await maintenanceResponder(world, request);
    if (maintenance) return maintenance;
    if (target.hostname === 'd1-export.example.test') {
      const databaseId = target.pathname
        .split('/')
        .at(-1)
        ?.replace(/\.sql$/u, '');
      const bytes = databaseId ? world.exports.get(databaseId) : undefined;
      return bytes
        ? new Response(bytes, {
            headers: { 'content-length': String(bytes.byteLength) },
          })
        : new Response('missing export', { status: 404 });
    }
    if (target.hostname !== 'api.cloudflare.com') {
      return new Response('provider projection refuses this origin', {
        status: 403,
      });
    }
    const authority = zoneAuthorityResponse(
      target,
      world.zones.map(({ id }) => id),
      world.routes,
    );
    if (authority) return authority;
    const parts = target.pathname.split('/').filter(Boolean);
    const routeIndex = parts.indexOf('routes');
    const routeId = routeIndex >= 0 ? parts[routeIndex + 1] : undefined;
    if (routeId && method === 'DELETE') {
      const index = world.routes.findIndex(({ id }) => id === routeId);
      if (index < 0) return Response.json({ errors: [] }, { status: 404 });
      // Ordinary-Worker public-access writes use subdomain.create; the only
      // routes.delete caller is disableControlWorkerPublicAccess.
      world.routes.splice(index, 1);
      world.mutationLog.push(`delete-route:${routeId}`);
      return single({});
    }
    const scriptsIndex = parts.indexOf('scripts');
    const scriptName = scriptsIndex >= 0 ? parts[scriptsIndex + 1] : undefined;
    const bodyField = (name: string): unknown =>
      body && typeof body === 'object' ? Reflect.get(body, name) : undefined;
    if (target.pathname.endsWith('/d1/database') && method === 'GET') {
      if (target.searchParams.has('page')) return pageArray([]);
      const requestedName = target.searchParams.get('name');
      return pageArray(
        world.databases
          .filter(
            ({ name }) => requestedName === null || name === requestedName,
          )
          .map(({ databaseId, name }) => ({
            uuid: databaseId,
            name,
          })),
      );
    }
    if (target.pathname.endsWith('/d1/database') && method === 'POST') {
      const name = bodyField('name');
      if (
        typeof name === 'string' &&
        world.databases.some((database) => database.name === name)
      ) {
        return failedResponse();
      }
      const failure = world.consumeFailure('createDatabase');
      if (failure && !failure.dispatched) return failureResponse(failure);
      const database = world.createDatabase(
        typeof name === 'string' ? name : '',
      );
      await world.applyAfter('createDatabase');
      if (failure) return failureResponse(failure);
      const { databaseId } = database;
      return single({ uuid: databaseId, name });
    }
    const databaseIndex = parts.indexOf('database');
    const databaseId =
      databaseIndex >= 0 ? parts[databaseIndex + 1] : undefined;
    if (
      databaseId &&
      target.pathname.endsWith(`/d1/database/${databaseId}`) &&
      method === 'GET'
    ) {
      const database = world.databases.find(
        (candidate) => candidate.databaseId === databaseId,
      );
      return database
        ? single({ uuid: database.databaseId, name: database.name })
        : Response.json({ errors: [] }, { status: 404 });
    }
    if (
      databaseId &&
      target.pathname.endsWith(`/d1/database/${databaseId}`) &&
      method === 'DELETE'
    ) {
      const index = world.databases.findIndex(
        (candidate) => candidate.databaseId === databaseId,
      );
      if (index < 0) return Response.json({ errors: [] }, { status: 404 });
      const failure = world.consumeFailure('deleteDatabase');
      if (failure && !failure.dispatched) return failureResponse(failure);
      world.databases.splice(index, 1);
      world.mutationLog.push(`delete-database:${databaseId}`);
      await world.applyAfter('deleteDatabase');
      if (failure) return failureResponse(failure);
      return single({});
    }
    if (databaseId && target.pathname.endsWith('/query') && method === 'POST') {
      const database = world.databases.find(
        (candidate) => candidate.databaseId === databaseId,
      );
      if (!database) return Response.json({ errors: [] }, { status: 404 });
      const batch = bodyField('batch');
      if (Array.isArray(batch)) {
        database.d1.batchDatabase(
          batch.map((statement) => ({
            sql: readStringFact(statement, 'sql') ?? '',
            bindings: readStringArray(statement, 'params'),
          })),
        );
        await world.applyAfter('batchDatabase');
        return pageArray(batch.map(() => ({ success: true, results: [] })));
      }
      const sql = bodyField('sql');
      const rows = database.d1.queryDatabase(
        typeof sql === 'string' ? sql : '',
        readStringArray(body, 'params'),
      );
      await world.applyAfter('queryDatabase');
      return pageArray([{ success: true, results: rows }]);
    }
    if (
      databaseId &&
      target.pathname.endsWith('/export') &&
      method === 'POST'
    ) {
      const failure = world.consumeFailure('exportDatabase');
      if (failure && !failure.dispatched) return failureResponse(failure);
      world.mutationLog.push(`export:${databaseId}`);
      await world.applyAfter('exportDatabase');
      if (failure) return failureResponse(failure);
      return single({
        status: 'complete',
        result: {
          signed_url: `https://d1-export.example.test/${encodeURIComponent(databaseId)}.sql?signature=world`,
        },
      });
    }
    if (target.pathname.endsWith('/workers/scripts') && method === 'GET') {
      if (target.searchParams.has('page')) return pageArray([]);
      return pageArray(
        [...world.scripts.entries()].flatMap(([id, script]) =>
          script.present ? [{ id }] : [],
        ),
      );
    }
    if (target.pathname.endsWith('/workers/domains') && method === 'GET') {
      const domains = world.customDomains.map((domain) => ({ ...domain }));
      await world.applyAfter('listCustomDomains');
      return pageArray(domains);
    }
    if (target.pathname.endsWith('/workers/domains') && method === 'PUT') {
      const failure = world.consumeFailure('attachCustomDomain');
      if (failure && !failure.dispatched) return failureResponse(failure);
      const hostname = bodyField('hostname');
      const service = bodyField('service');
      if (typeof hostname !== 'string' || typeof service !== 'string') {
        return failedResponse();
      }
      const current = world.customDomains.find(
        (domain) => domain.hostname === hostname,
      );
      if (current) current.service = service;
      else {
        world.customDomains.push({
          id: world.allocateDomainId(),
          hostname,
          service,
        });
      }
      world.mutationLog.push(`attach-domain:${hostname}`);
      await world.applyAfter('attachCustomDomain');
      if (failure) return failureResponse(failure);
      return single({
        id: world.customDomains.find((domain) => domain.hostname === hostname)
          ?.id,
      });
    }
    const domainsIndex = parts.indexOf('domains');
    const domainId = domainsIndex >= 0 ? parts[domainsIndex + 1] : undefined;
    if (domainId && method === 'DELETE') {
      const index = world.customDomains.findIndex(({ id }) => id === domainId);
      if (index < 0) return Response.json({ errors: [] }, { status: 404 });
      const failure = world.consumeFailure('detachCustomDomain');
      if (failure && !failure.dispatched) return failureResponse(failure);
      world.customDomains.splice(index, 1);
      world.mutationLog.push(`detach-domain:${domainId}`);
      await world.applyAfter('detachCustomDomain');
      if (failure) return failureResponse(failure);
      return single({});
    }
    if (
      target.pathname.endsWith('/workers/durable_objects/namespaces') &&
      method === 'GET'
    ) {
      if (target.searchParams.has('page')) return pageArray([]);
      return pageArray(
        world.durableObjectNamespaces.map((namespace) => ({
          id: namespace.id,
          script: namespace.script,
          class: namespace.className,
        })),
      );
    }
    if (
      target.pathname.endsWith('/workers/dispatch/namespaces') &&
      method === 'GET'
    ) {
      return pageArray(
        world.dispatchNamespaces.map((namespace) => ({
          namespace_name: namespace.name,
          trusted_workers: false,
          script_count: namespace.scripts.length,
        })),
      );
    }
    const dispatchIndex = parts.indexOf('namespaces');
    const dispatchNamespace =
      dispatchIndex >= 0 ? parts[dispatchIndex + 1] : undefined;
    const dispatchScriptIndex = parts.indexOf('scripts', dispatchIndex + 1);
    const dispatchScriptName =
      dispatchScriptIndex >= 0 ? parts[dispatchScriptIndex + 1] : undefined;
    if (
      dispatchNamespace &&
      target.pathname.endsWith(`/${dispatchNamespace}/scripts`) &&
      method === 'GET'
    ) {
      const namespace = world.dispatchNamespaces.find(
        ({ name }) => name === dispatchNamespace,
      );
      return pageArray(
        namespace?.scripts.map(({ name }) => ({ id: name, tags: [] })) ?? [],
      );
    }
    if (
      dispatchNamespace &&
      dispatchScriptName &&
      target.pathname.endsWith('/settings') &&
      method === 'GET'
    ) {
      const script = world.dispatchNamespaces
        .find(({ name }) => name === dispatchNamespace)
        ?.scripts.find(({ name }) => name === dispatchScriptName);
      return script
        ? single({ bindings: script.bindings })
        : Response.json({ errors: [] }, { status: 404 });
    }
    if (!scriptName) {
      throw new Error(`unexpected request ${method} ${target.pathname}`);
    }
    const script = world.scripts.get(scriptName);
    if (
      target.pathname.endsWith(`/workers/scripts/${scriptName}`) &&
      method === 'PUT'
    ) {
      const metadata = bodyField('metadata');
      const pending = world.peekFailure('uploadCandidate');
      if (pending && !pending.dispatched) {
        world.consumeFailure('uploadCandidate');
        if (pending.at === 'public-access') {
          throw new Error(
            "ProviderFailure.at:'public-access' requires dispatched:true",
          );
        }
        if (pending.error) throw pending.error;
        return pending.response ?? failureResponse(pending);
      }
      const settlesAtScript = pending?.at === 'script';
      if (pending?.error && !settlesAtScript) {
        world.consumeFailure('uploadCandidate');
        // subdomain.create is outside the sanitized send() boundary and keeps
        // the SDK's retries, so thrown fixture errors must settle at the PUT.
        throw new Error(
          "ProviderFailure.error requires at:'script' — the subdomain endpoint is outside sanitizeProviderError() and is retried by the SDK",
        );
      }
      const failure = settlesAtScript
        ? world.consumeFailure('uploadCandidate')
        : undefined;
      const versions = world.applyUpload(
        {
          scriptName,
          mode: 'initial',
          tag: readVersionTag(metadata),
          bindings: readBindings(metadata),
          mainModule: readStringFact(metadata, 'main_module') ?? '',
          modules: readModules(body),
        },
        { duplicate: failure?.duplicate },
      );
      // The real adapter writes public access with a separate subdomain request
      // after the script exists; the script PUT records only upload state.
      if (pending && !settlesAtScript) {
        world.deferFailure('uploadCandidate');
      } else {
        await world.applyAfter('uploadCandidate');
      }
      if (failure?.error) throw failure.error;
      if (failure) return failure.response ?? failureResponse(failure);
      return single({ id: scriptName, etag: versions[0]?.versionId });
    }
    if (!script?.present) return Response.json({ errors: [] }, { status: 404 });
    if (
      target.pathname.endsWith(`/workers/scripts/${scriptName}`) &&
      method === 'GET'
    ) {
      return single({ id: scriptName });
    }
    if (
      target.pathname.endsWith(`/workers/scripts/${scriptName}`) &&
      method === 'DELETE'
    ) {
      const failure = world.consumeFailure('deleteWorkerScript');
      if (failure && !failure.dispatched) return failureResponse(failure);
      world.deleteScript(scriptName);
      await world.applyAfter('deleteWorkerScript');
      if (failure) return failureResponse(failure);
      return single({});
    }
    if (target.pathname.endsWith('/secrets') && method === 'GET') {
      if (target.searchParams.has('page')) return pageArray([]);
      return pageArray(
        [...script.secretNames].sort().map((name) => ({ name })),
      );
    }
    if (target.pathname.endsWith('/secrets-bulk') && method === 'PATCH') {
      // Ordinary-Worker secret writes only delete individual secrets; bulkUpdate
      // belongs to Workers for Platforms and platform-control paths.
      const secrets = bodyField('secrets');
      if (secrets && typeof secrets === 'object') {
        for (const name of Reflect.ownKeys(secrets)) {
          if (typeof name !== 'string') continue;
          if (Reflect.get(secrets, name) === null)
            script.secretNames.delete(name);
          else script.secretNames.add(name);
        }
      }
      world.mutationLog.push(`update-secrets:${scriptName}`);
      return single({});
    }
    const secretsIndex = parts.indexOf('secrets');
    const secretName = secretsIndex >= 0 ? parts[secretsIndex + 1] : undefined;
    if (secretName && method === 'DELETE') {
      const failure = world.consumeFailure('deleteControlSecrets');
      if (failure && !failure.dispatched) return failureResponse(failure);
      script.secretNames.delete(secretName);
      world.mutationLog.push(`delete-secret:${scriptName}:${secretName}`);
      await world.applyAfter('deleteControlSecrets');
      if (failure) return failureResponse(failure);
      return single({});
    }
    if (secretName && method === 'PUT') {
      // The ordinary-Worker plane only deletes secrets by name; no source path
      // calls the SDK's single-secret update endpoint.
      script.secretNames.add(secretName);
      world.mutationLog.push(`update-secret:${scriptName}:${secretName}`);
      return single({});
    }
    if (target.pathname.endsWith('/deployments') && method === 'GET') {
      return single({
        deployments: script.deployment
          ? [
              {
                id: 'deployment',
                created_on: '2026-08-26T00:00:00.000Z',
                source: 'api',
                strategy: 'percentage',
                versions: script.deployment.map(
                  ({ versionId, percentage }) => ({
                    version_id: versionId,
                    percentage,
                  }),
                ),
              },
            ]
          : [],
      });
    }
    if (target.pathname.endsWith('/versions') && method === 'GET') {
      if (target.searchParams.has('page')) return pageItems([]);
      return pageItems(
        script.versions.map(({ versionId, tag }) => ({
          id: versionId,
          annotations: tag === undefined ? undefined : { 'workers/tag': tag },
        })),
      );
    }
    if (target.pathname.endsWith('/versions') && method === 'POST') {
      const metadata = bodyField('metadata');
      const failure = world.consumeFailure('uploadCandidate');
      if (failure && !failure.dispatched) {
        if (failure.error) throw failure.error;
        return failure.response ?? failureResponse(failure);
      }
      const versions = world.applyUpload(
        {
          scriptName,
          mode: 'staged',
          tag: readVersionTag(metadata),
          bindings: readBindings(metadata),
          mainModule: readStringFact(metadata, 'main_module') ?? '',
          modules: readModules(body),
        },
        { duplicate: failure?.duplicate },
      );
      await world.applyAfter('uploadCandidate');
      if (failure?.error) throw failure.error;
      if (failure) return failure.response ?? failureResponse(failure);
      const version = versions[0];
      return single({
        id: version?.versionId,
        resources: { bindings: version?.bindings ?? [] },
      });
    }
    const versionId = parts.at(-1);
    if (
      versionId &&
      target.pathname.endsWith(`/versions/${versionId}`) &&
      method === 'GET'
    ) {
      const version = script.versions.find(
        (item) => item.versionId === versionId,
      );
      if (!version) return Response.json({ errors: [] }, { status: 404 });
      return single({
        id: version.versionId,
        annotations:
          version.tag === undefined
            ? undefined
            : { 'workers/tag': version.tag },
        resources: { bindings: version.bindings },
      });
    }
    if (target.pathname.endsWith('/subdomain') && method === 'GET') {
      return single({
        enabled: script.subdomain.enabled,
        previews_enabled: script.subdomain.previewsEnabled,
      });
    }
    if (target.pathname.endsWith('/subdomain') && method === 'POST') {
      const uploadFailure = world.consumeDeferredFailure('uploadCandidate');
      const failure =
        uploadFailure ?? world.consumeFailure('disablePublicAccess');
      if (failure && !failure.dispatched) return failureResponse(failure);
      const payload = body && typeof body === 'object' ? body : {};
      script.subdomain.enabled = Reflect.get(payload, 'enabled') === true;
      script.subdomain.previewsEnabled =
        Reflect.get(payload, 'previews_enabled') === true;
      world.mutationLog.push(`configure-public-access:${scriptName}`);
      if (uploadFailure) await world.applyAfter('uploadCandidate');
      else await world.applyAfter('disablePublicAccess');
      if (failure) return failure.response ?? failureResponse(failure);
      return single({ enabled: script.subdomain.enabled });
    }
    if (target.pathname.endsWith('/deployments') && method === 'POST') {
      const versions = bodyField('versions');
      const deployment = Array.isArray(versions)
        ? versions.map((version) => ({
            versionId: readStringFact(version, 'version_id') ?? '',
            percentage: Number(Reflect.get(version, 'percentage')),
          }))
        : [];
      const operation =
        deployment.length === 1 && deployment[0]?.percentage === 100
          ? 'promoteWorker'
          : 'deployCandidate';
      const failure = world.consumeFailure(operation);
      if (failure && !failure.dispatched) return failureResponse(failure);
      world.applyDeployment(scriptName, deployment);
      await world.applyAfter(operation);
      if (failure) return failureResponse(failure);
      return single({ id: 'deployment' });
    }
    throw new Error(`unexpected request ${method} ${target.pathname}`);
  };
}

function failureResponse(failure: { readonly error?: Error }): Response {
  return failedResponse(failure.error?.message);
}

function failedResponse(message = 'injected provider failure'): Response {
  return Response.json(
    {
      success: false,
      errors: [{ code: 1, message }],
    },
    { status: 400 },
  );
}

function readVersionTag(metadata: unknown): string | undefined {
  if (!metadata || typeof metadata !== 'object') return undefined;
  const annotations = Reflect.get(metadata, 'annotations');
  if (!annotations || typeof annotations !== 'object') return undefined;
  const tag = Reflect.get(annotations, 'workers/tag');
  return typeof tag === 'string' ? tag : undefined;
}

function readStringFact(value: unknown, name: string): string | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const candidate = Reflect.get(value, name);
  return typeof candidate === 'string' ? candidate : undefined;
}

function readBindings(metadata: unknown): readonly unknown[] {
  if (!metadata || typeof metadata !== 'object') return [];
  const bindings = Reflect.get(metadata, 'bindings');
  return Array.isArray(bindings) ? bindings : [];
}

function readStringArray(value: unknown, name: string): readonly string[] {
  if (!value || typeof value !== 'object') return [];
  const values = Reflect.get(value, name);
  return Array.isArray(values)
    ? values.flatMap((item) => (typeof item === 'string' ? [item] : []))
    : [];
}

function readModules(body: unknown): readonly {
  name: string;
  content: string;
  contentType: string;
}[] {
  if (!body || typeof body !== 'object') return [];
  const files = Reflect.get(body, 'files');
  if (!Array.isArray(files)) return [];
  return files.flatMap((file) => {
    const name = readStringFact(file, 'name');
    const content = readStringFact(file, 'text');
    const contentType = readStringFact(file, 'type');
    return name && content !== undefined
      ? [
          {
            name,
            content,
            contentType: contentType ?? 'application/javascript+module',
          },
        ]
      : [];
  });
}
