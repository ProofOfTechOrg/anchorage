// SPDX-License-Identifier: Apache-2.0

import { expect } from 'vitest';
import type { CloudflareProvisioningClient } from '../../src/cloudflare-client.js';
import {
  type CloudflareApiRateCoordinator,
  ProcessLocalCloudflareApiRateCoordinator,
} from '../../src/cloudflare-rate-coordinator.js';

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

export function errorChain(error: unknown): string {
  const messages: string[] = [];
  let current = error;
  while (current instanceof Error) {
    messages.push(current.message);
    current = current.cause;
  }
  return messages.join(' | ');
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

export interface ProviderWorld {
  readonly scripts: Map<
    string,
    {
      versions: Array<{
        versionId: string;
        tag: string | undefined;
        bindings: readonly unknown[];
      }>;
      deployment?: Array<{ versionId: string; percentage: number }>;
      subdomain: { enabled: boolean; previewsEnabled: boolean };
    }
  >;
  readonly databases: Array<{ databaseId: string; name: string }>;
}

export function providerWorld(): ProviderWorld {
  return { scripts: new Map(), databases: [] };
}

export function restProjection(world: ProviderWorld): CloudflareFixtureHandler {
  return async ({ method, url, body }) => {
    const target = new URL(url);
    const parts = target.pathname.split('/').filter(Boolean);
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
      const databaseId = `database-${world.databases.length + 1}`;
      world.databases.push({
        databaseId,
        name: typeof name === 'string' ? name : '',
      });
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
      world.databases.splice(index, 1);
      return single({});
    }
    if (databaseId && target.pathname.endsWith('/query') && method === 'POST') {
      return pageArray([{ success: true, results: [] }]);
    }
    if (!scriptName) {
      throw new Error(`unexpected request ${method} ${target.pathname}`);
    }
    let script = world.scripts.get(scriptName);
    if (
      target.pathname.endsWith(`/workers/scripts/${scriptName}`) &&
      method === 'PUT'
    ) {
      const metadata = bodyField('metadata');
      const versionId = `version-${(script?.versions.length ?? 0) + 1}`;
      script ??= {
        versions: [],
        subdomain: { enabled: false, previewsEnabled: false },
      };
      script.versions.unshift({
        versionId,
        tag: readVersionTag(metadata),
        bindings: readBindings(metadata),
      });
      world.scripts.set(scriptName, script);
      return single({ id: scriptName });
    }
    if (!script) return Response.json({ errors: [] }, { status: 404 });
    if (
      target.pathname.endsWith(`/workers/scripts/${scriptName}`) &&
      method === 'DELETE'
    ) {
      world.scripts.delete(scriptName);
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
      const bindings = readBindings(metadata);
      const versionId = `version-${script.versions.length + 1}`;
      script.versions.unshift({
        versionId,
        tag: readVersionTag(metadata),
        bindings,
      });
      return single({
        id: versionId,
        resources: { bindings },
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
      const payload = body && typeof body === 'object' ? body : {};
      script.subdomain.enabled = Reflect.get(payload, 'enabled') === true;
      script.subdomain.previewsEnabled =
        Reflect.get(payload, 'previews_enabled') === true;
      return single({ enabled: script.subdomain.enabled });
    }
    if (target.pathname.endsWith('/deployments') && method === 'POST') {
      const versions = bodyField('versions');
      script.deployment = Array.isArray(versions)
        ? versions.map((version) => ({
            versionId: readStringFact(version, 'version_id') ?? '',
            percentage: Number(Reflect.get(version, 'percentage')),
          }))
        : [];
      return single({ id: 'deployment' });
    }
    throw new Error(`unexpected request ${method} ${target.pathname}`);
  };
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
