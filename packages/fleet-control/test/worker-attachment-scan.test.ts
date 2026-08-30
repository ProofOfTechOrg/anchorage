// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { BaseNamespaces } from 'cloudflare/resources/workers-for-platforms/dispatch/namespaces/namespaces';
import { describe, expect, it, vi } from 'vitest';
import {
  advanceCloudflareWorkerAttachmentScan,
  CloudflareProvisioningClient,
  mapDecommissionAttachmentScanChunk,
} from '../src/cloudflare-client.js';
import {
  CLOUDFLARE_SDK_MAX_ATTEMPTS,
  CLOUDFLARE_SDK_MAX_RETRIES,
} from '../src/cloudflare-client-config.js';
import {
  CloudflareAttachmentScanDriftError,
  CloudflareAttachmentScanProgressError,
  initialWorkerAttachmentScan,
  parseWorkerAttachmentScanProgress,
  type WorkerAttachment,
  type WorkerAttachmentScanChunk,
  type WorkerAttachmentScanInput,
  type WorkerAttachmentScanProgress,
  type WorkerAttachmentScanTarget,
} from '../src/cloudflare-worker-attachment-scan.js';
import {
  assertWorkerAttachmentProviderRequestBudget,
  initialWorkerAttachmentScan as initialWorkerAttachmentScanFromState,
  parseWorkerAttachmentScanProgress as parseWorkerAttachmentScanProgressFromState,
  type WorkerAttachment as StateWorkerAttachment,
  type WorkerAttachmentScanChunk as StateWorkerAttachmentScanChunk,
  type WorkerAttachmentScanInput as StateWorkerAttachmentScanInput,
  type WorkerAttachmentScanProgress as StateWorkerAttachmentScanProgress,
  type WorkerAttachmentScanTarget as StateWorkerAttachmentScanTarget,
} from '../src/cloudflare-worker-attachment-scan-state.js';
import type { DecommissionAttachmentProgress } from '../src/index.js';
import * as fleetRoot from '../src/index.js';
import {
  type CloudflareFixtureHandler,
  deferred,
  pageArray,
  recordingFetch,
  single,
  testRateCoordinator,
} from './fixtures/cloudflare-fetch-fixture.js';

interface VersionFixture {
  readonly id: string;
  readonly percentage?: number;
  readonly bindings?: readonly Readonly<Record<string, unknown>>[];
}

interface OrdinaryFixture {
  readonly id: string;
  readonly versions?: readonly VersionFixture[];
}

interface DispatchPageFixture {
  readonly cursor?: string;
  readonly scripts: readonly string[];
  readonly nextCursor?: unknown;
}

interface NamespaceFixture {
  readonly name: string;
  readonly pages: readonly DispatchPageFixture[];
  readonly tags?: Readonly<Record<string, readonly string[]>>;
  readonly bindings?: Readonly<
    Record<string, readonly Readonly<Record<string, unknown>>[]>
  >;
}

interface AttachmentWorld {
  readonly ordinary: readonly OrdinaryFixture[];
  readonly namespaces: readonly NamespaceFixture[];
}

function apiFailure(status: number): Response {
  return Response.json(
    {
      success: false,
      errors: [{ code: 10_000 + status, message: 'provider failure' }],
      messages: [],
      result: null,
    },
    { status, headers: { 'retry-after-ms': '1' } },
  );
}

function worldHandler(
  world: AttachmentWorld,
  events: string[] = [],
): CloudflareFixtureHandler {
  return ({ url }) => {
    const target = new URL(url);
    const path = decodeURIComponent(target.pathname);
    events.push(`${path}?${target.searchParams.toString()}`);
    if (path.endsWith('/workers/scripts')) {
      return pageArray(world.ordinary.map(({ id }) => ({ id })));
    }
    const ordinary = world.ordinary.find(({ id }) =>
      path.includes(`/workers/scripts/${id}/`),
    );
    if (ordinary && path.endsWith('/deployments')) {
      return single({
        deployments:
          ordinary.versions === undefined
            ? []
            : [
                {
                  versions: ordinary.versions.map((version) => ({
                    version_id: version.id,
                    percentage: version.percentage,
                  })),
                },
              ],
      });
    }
    if (ordinary && path.includes('/versions/')) {
      const versionId = path.split('/versions/')[1] ?? '';
      const version = ordinary.versions?.find(({ id }) => id === versionId);
      return single({ resources: { bindings: version?.bindings ?? [] } });
    }
    if (path.endsWith('/workers/dispatch/namespaces')) {
      return pageArray(
        world.namespaces.map((namespace, index) => ({
          namespace_name: namespace.name,
          namespace_id: `namespace-${index}`,
          script_count: namespace.pages.reduce(
            (count, page) => count + page.scripts.length,
            0,
          ),
        })),
      );
    }
    const namespace = world.namespaces.find(({ name }) =>
      path.includes(`/namespaces/${name}/scripts`),
    );
    if (namespace && path.endsWith('/scripts')) {
      const cursor = target.searchParams.get('cursor') ?? undefined;
      const page = namespace.pages.find(
        (candidate) => candidate.cursor === cursor,
      );
      if (!page) throw new Error(`unexpected cursor ${String(cursor)}`);
      return pageArray(
        page.scripts.map((id) => ({
          id,
          tags: namespace.tags?.[id] ?? [`tag:${id}`],
        })),
        { cursor: page.nextCursor as string | undefined },
      );
    }
    if (namespace && path.endsWith('/settings')) {
      const scriptId = path.split('/scripts/')[1]?.split('/')[0] ?? '';
      return single({ bindings: namespace.bindings?.[scriptId] ?? [] });
    }
    throw new Error(`unexpected request ${path}`);
  };
}

function client(
  fetch: typeof globalThis.fetch,
  options: { readonly plainOnly?: boolean } = {},
): CloudflareProvisioningClient {
  const base = {
    accountId: 'account',
    apiToken: 'token',
    rateCoordinator: testRateCoordinator(),
    fetch,
    requestTimeoutMs: 1_000,
  };
  return options.plainOnly
    ? new CloudflareProvisioningClient({ ...base, plane: 'plain-worker' })
    : new CloudflareProvisioningClient({
        ...base,
        dispatchNamespace: 'fleet',
      });
}

async function drain(
  subject: CloudflareProvisioningClient,
  target: WorkerAttachmentScanTarget,
  options: {
    readonly budget?: number;
    readonly stopOnFirstAttachment?: boolean;
    readonly signal?: AbortSignal;
  } = {},
): Promise<{
  readonly chunks: readonly WorkerAttachmentScanChunk[];
  readonly attachments: readonly WorkerAttachment[];
  readonly terminal: WorkerAttachmentScanChunk;
}> {
  let progress = initialWorkerAttachmentScan(target);
  const chunks: WorkerAttachmentScanChunk[] = [];
  const attachments: WorkerAttachment[] = [];
  for (let attempt = 0; attempt < 1_000; attempt += 1) {
    const chunk = await advanceCloudflareWorkerAttachmentScan(subject, {
      target,
      progress,
      maxProviderRequests: options.budget ?? 9,
      stopOnFirstAttachment: options.stopOnFirstAttachment,
      signal: options.signal,
    });
    chunks.push(chunk);
    if (chunk.status === 'attached') {
      attachments.push(chunk.attachment);
      return { chunks, attachments, terminal: chunk };
    }
    attachments.push(...chunk.attachments);
    if (chunk.status === 'complete') {
      return { chunks, attachments, terminal: chunk };
    }
    progress = parseWorkerAttachmentScanProgress(
      JSON.parse(JSON.stringify(chunk.progress)),
      target,
    );
  }
  throw new Error('attachment scan did not terminate');
}

function hash(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

function evidence(
  target: WorkerAttachmentScanTarget,
  leaves: readonly (readonly unknown[])[],
): string {
  let digest = hash(
    JSON.stringify([
      'attachment-scan-v1',
      target.kind,
      target.kind === 'd1' ? target.databaseId : target.bucketName,
    ]),
  );
  for (const leaf of leaves) digest = hash(JSON.stringify([digest, leaf]));
  return digest;
}

function multisetEvidence(leaves: readonly (readonly unknown[])[]): string {
  const modulus = 1n << 256n;
  let sum = 0n;
  for (const leaf of leaves) {
    sum = (sum + BigInt(`0x${hash(JSON.stringify(leaf))}`)) % modulus;
  }
  return sum.toString(16).padStart(64, '0');
}

const D1_TARGET = { kind: 'd1', databaseId: 'target-db' } as const;
const R2_TARGET = { kind: 'r2', bucketName: 'target-bucket' } as const;

describe('Cloudflare Worker attachment scan', () => {
  it('resumes a D1 scan across every ordinary version and dispatch page', async () => {
    const events: string[] = [];
    const world: AttachmentWorld = {
      ordinary: [
        {
          id: 'ordinary-a',
          versions: [
            { id: 'a1', percentage: 50 },
            {
              id: 'a2',
              percentage: 50,
              bindings: [{ type: 'd1', database_id: 'target-db' }],
            },
            { id: 'a0', percentage: 0 },
          ],
        },
        { id: 'ordinary-b', versions: [{ id: 'b1', percentage: 100 }] },
      ],
      namespaces: [
        {
          name: 'one',
          pages: [
            { scripts: ['dispatch-a'], nextCursor: 'next' },
            { cursor: 'next', scripts: ['dispatch-b'] },
          ],
          bindings: {
            'dispatch-b': [{ type: 'd1', database_id: 'target-db' }],
          },
        },
        { name: 'two', pages: [{ scripts: ['dispatch-c'] }] },
      ],
    };
    const fixture = recordingFetch(worldHandler(world, events));
    const result = await drain(client(fixture.fetch), D1_TARGET);

    expect(result.attachments).toEqual([
      { scriptName: 'ordinary-a', plane: 'ordinary' },
      {
        scriptName: 'dispatch-b',
        plane: 'dispatch',
        dispatchNamespace: 'one',
      },
    ]);
    expect(result.chunks.length).toBeGreaterThan(4);
    expect(
      result.chunks.every((chunk) => chunk.providerFetchAttemptsReserved <= 9),
    ).toBe(true);
    expect(events.filter((event) => event.includes('/versions/a0'))).toEqual(
      [],
    );
    expect(events.filter((event) => event.endsWith('/settings?'))).toHaveLength(
      3,
    );
    expect(
      events
        .filter((event) => event.endsWith('/settings?'))
        .map((event) =>
          event
            .match(/namespaces\/([^/]+)\/scripts\/([^/]+)\/settings/)
            ?.slice(1),
        ),
    ).toEqual([
      ['one', 'dispatch-a'],
      ['one', 'dispatch-b'],
      ['two', 'dispatch-c'],
    ]);
  });

  it('preserves target-specific attachment and malformed-inventory behavior', async () => {
    const world: AttachmentWorld = {
      ordinary: [
        { id: 'empty', versions: [] },
        {
          id: 'percentage-free',
          versions: [
            {
              id: 'v1',
              bindings: [{ type: 'r2_bucket', bucket_name: 'target-bucket' }],
            },
          ],
        },
      ],
      namespaces: [
        {
          name: 'fleet',
          pages: [
            {
              scripts: ['dispatch-r2', 'dispatch-other', 'dispatch-r2'],
            },
          ],
          bindings: {
            'dispatch-r2': [
              { type: 'r2_bucket', bucket_name: 'target-bucket' },
            ],
          },
        },
      ],
    };
    const fixture = recordingFetch(worldHandler(world));
    const result = await drain(client(fixture.fetch), R2_TARGET);
    expect(result.attachments).toEqual([
      { scriptName: 'percentage-free', plane: 'ordinary' },
      {
        scriptName: 'dispatch-r2',
        plane: 'dispatch',
        dispatchNamespace: 'fleet',
      },
      {
        scriptName: 'dispatch-r2',
        plane: 'dispatch',
        dispatchNamespace: 'fleet',
      },
    ]);
    await expect(
      client(fixture.fetch).listWorkerR2Attachments('target-bucket'),
    ).resolves.toEqual([
      {
        scriptName: 'dispatch-r2',
        plane: 'dispatch',
        dispatchNamespace: 'fleet',
      },
      {
        scriptName: 'dispatch-r2',
        plane: 'dispatch',
        dispatchNamespace: 'fleet',
      },
      { scriptName: 'percentage-free', plane: 'ordinary' },
    ]);

    const duplicateD1Fixture = recordingFetch(
      worldHandler({
        ordinary: [],
        namespaces: [
          {
            name: 'fleet',
            pages: [{ scripts: ['dispatch-d1', 'dispatch-d1'] }],
            bindings: {
              'dispatch-d1': [{ type: 'd1', database_id: 'target-db' }],
            },
          },
        ],
      }),
    );
    await expect(
      drain(client(duplicateD1Fixture.fetch), D1_TARGET),
    ).resolves.toMatchObject({
      attachments: [
        {
          scriptName: 'dispatch-d1',
          plane: 'dispatch',
          dispatchNamespace: 'fleet',
        },
        {
          scriptName: 'dispatch-d1',
          plane: 'dispatch',
          dispatchNamespace: 'fleet',
        },
      ],
    });
    await expect(
      client(duplicateD1Fixture.fetch).listWorkerDatabaseAttachments(
        'target-db',
      ),
    ).resolves.toEqual([
      {
        scriptName: 'dispatch-d1',
        plane: 'dispatch',
        dispatchNamespace: 'fleet',
      },
    ]);

    const malformedFixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) {
        return pageArray([{ id: 'malformed' }]);
      }
      if (target.pathname.endsWith('/deployments')) return single({});
      throw new Error(`unexpected request ${target.pathname}`);
    });
    await expect(
      drain(client(malformedFixture.fetch), R2_TARGET),
    ).rejects.toThrow(
      "Cloudflare deployment inventory for ordinary Worker 'malformed' was malformed",
    );

    const oversizedScripts = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) {
        return pageArray(
          Array.from({ length: 10_001 }, (_, index) => ({
            id: `script-${index}`,
          })),
        );
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    await expect(
      drain(client(oversizedScripts.fetch), D1_TARGET),
    ).rejects.toThrow(
      'ordinary Worker script inventory exceeded the supported inventory bound of 10000 items',
    );

    const oversizedNamespaces = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
      if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
        return pageArray(
          Array.from({ length: 10_001 }, (_, index) => ({
            namespace_name: `namespace-${index}`,
          })),
        );
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    await expect(
      drain(client(oversizedNamespaces.fetch), D1_TARGET),
    ).rejects.toThrow(
      'dispatch namespace inventory exceeded the supported inventory bound of 10000 items',
    );

    const oversizedVersions = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) {
        return pageArray([{ id: 'many-versions' }]);
      }
      if (target.pathname.endsWith('/deployments')) {
        return single({
          deployments: [
            {
              versions: Array.from({ length: 10_001 }, (_, index) => ({
                version_id: `version-${index}`,
                percentage: index === 0 ? 100 : 0,
              })),
            },
          ],
        });
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    await expect(
      drain(client(oversizedVersions.fetch), D1_TARGET),
    ).rejects.toThrow(
      "ordinary Worker 'many-versions' deployment version inventory exceeded the supported inventory bound of 10000 items",
    );

    for (const malformedBindings of ['version', 'settings'] as const) {
      const fixture = recordingFetch(({ url }) => {
        const target = new URL(url);
        if (target.pathname.endsWith('/workers/scripts')) {
          return pageArray(
            malformedBindings === 'version' ? [{ id: 'ordinary' }] : [],
          );
        }
        if (target.pathname.endsWith('/deployments')) {
          return single({
            deployments: [
              { versions: [{ version_id: 'version', percentage: 100 }] },
            ],
          });
        }
        if (target.pathname.includes('/versions/')) return single({});
        if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
          return pageArray([{ namespace_name: 'fleet' }]);
        }
        if (target.pathname.endsWith('/namespaces/fleet/scripts')) {
          return pageArray([{ id: 'dispatch', tags: [] }]);
        }
        if (target.pathname.endsWith('/settings')) return single({});
        throw new Error(`unexpected request ${target.pathname}`);
      });
      await expect(drain(client(fixture.fetch), D1_TARGET)).rejects.toThrow(
        malformedBindings === 'version'
          ? 'Cloudflare ordinary Worker version binding inventory was malformed'
          : 'Cloudflare dispatch Worker binding inventory was malformed',
      );
    }

    const malformedNamespace = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
      if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
        return pageArray([
          { namespace_name: 'fleet', namespace_id: 42 as unknown as string },
        ]);
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    await expect(
      drain(client(malformedNamespace.fetch), D1_TARGET),
    ).rejects.toThrow(
      "Cloudflare dispatch namespace 'fleet' had malformed identity metadata",
    );
  });

  it('reserves complete retry sets for SDK and raw page operations', async () => {
    expect(CLOUDFLARE_SDK_MAX_ATTEMPTS).toBe(CLOUDFLARE_SDK_MAX_RETRIES + 1);
    let rawAttempts = 0;
    const fixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
      if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
        return pageArray([{ namespace_name: 'fleet' }]);
      }
      if (target.pathname.endsWith('/namespaces/fleet/scripts')) {
        rawAttempts += 1;
        return rawAttempts < 3 ? apiFailure(503) : pageArray([]);
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    const subject = client(fixture.fetch);
    for (const invalidBudget of [8, 1_001, 9.5]) {
      expect(() =>
        assertWorkerAttachmentProviderRequestBudget(invalidBudget),
      ).toThrow('maxProviderRequests must be an integer from 9 to 1000');
      const requestCount = fixture.requests.length;
      let budgetError: unknown;
      try {
        await advanceCloudflareWorkerAttachmentScan(subject, {
          target: D1_TARGET,
          progress: initialWorkerAttachmentScan(D1_TARGET),
          maxProviderRequests: invalidBudget,
        });
      } catch (error) {
        budgetError = error;
      }
      expect(budgetError).toBeInstanceOf(Error);
      expect((budgetError as Error).message).toBe(
        'maxProviderRequests must be an integer from 9 to 1000',
      );
      expect(fixture.requests).toHaveLength(requestCount);
    }
    expect(() => assertWorkerAttachmentProviderRequestBudget(9)).not.toThrow();
    expect(() =>
      assertWorkerAttachmentProviderRequestBudget(1_000),
    ).not.toThrow();
    const result = await drain(subject, D1_TARGET, { budget: 9 });
    expect(result.terminal.status).toBe('complete');
    expect(rawAttempts).toBe(3);
    expect(
      result.chunks.map((chunk) => chunk.providerFetchAttemptsReserved),
    ).toContain(9);

    let sdkAttempts = 0;
    const sdkFixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) {
        sdkAttempts += 1;
        return sdkAttempts < 3 ? apiFailure(503) : pageArray([]);
      }
      if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
        return pageArray([]);
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    const sdkResult = await drain(client(sdkFixture.fetch), D1_TARGET, {
      budget: 9,
    });
    expect(sdkResult.terminal).toMatchObject({
      status: 'complete',
      providerFetchAttemptsReserved: 6,
    });
    expect(sdkAttempts).toBe(CLOUDFLARE_SDK_MAX_ATTEMPTS);

    let ceilingAttempts = 0;
    const ceilingFixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) {
        ceilingAttempts += 1;
        return ceilingAttempts <= CLOUDFLARE_SDK_MAX_ATTEMPTS
          ? apiFailure(503)
          : pageArray([]);
      }
      if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
        return pageArray([]);
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    let ceilingError: unknown;
    try {
      await drain(client(ceilingFixture.fetch), D1_TARGET, { budget: 9 });
    } catch (error) {
      ceilingError = error;
    }
    expect(ceilingError).toBeInstanceOf(Error);
    expect(ceilingAttempts).toBe(CLOUDFLARE_SDK_MAX_ATTEMPTS);
  });

  it('resumes an ordinary version index without repeating a committed version read', async () => {
    const events: string[] = [];
    const world: AttachmentWorld = {
      ordinary: [
        {
          id: 'ordinary',
          versions: [
            { id: 'v1', percentage: 50 },
            { id: 'v2', percentage: 50 },
          ],
        },
      ],
      namespaces: [],
    };
    const fixture = recordingFetch(worldHandler(world, events));
    await drain(client(fixture.fetch), D1_TARGET, { budget: 9 });
    expect(
      events.filter((event) => event.includes('/versions/v1')),
    ).toHaveLength(1);
    expect(
      events.filter((event) => event.includes('/versions/v2')),
    ).toHaveLength(1);
    expect(
      events.filter((event) => event.endsWith('/deployments?')).length,
    ).toBe(3);
  });

  it('re-fetches a partial dispatch page and skips committed settings offsets', async () => {
    const events: string[] = [];
    const world: AttachmentWorld = {
      ordinary: [],
      namespaces: [
        {
          name: 'fleet',
          pages: [{ scripts: ['a', 'B', 'c'] }],
        },
      ],
    };
    const fixture = recordingFetch(worldHandler(world, events));
    await drain(client(fixture.fetch), D1_TARGET, { budget: 9 });
    expect(
      events.filter((event) => event.includes('/namespaces/fleet/scripts?')),
    ).toHaveLength(5);
    for (const script of ['B', 'a', 'c']) {
      expect(
        events.filter((event) => event.includes(`/scripts/${script}/settings`)),
      ).toHaveLength(1);
    }
    expect(
      events
        .filter((event) => event.endsWith('/settings?'))
        .map((event) => event.split('/scripts/')[1]?.split('/settings')[0]),
    ).toEqual(['B', 'a', 'c']);
  });

  it('produces page-independent evidence for every leaf kind and target seed', async () => {
    const ordinary = [
      { id: 'a', versions: [{ id: 'version-a', percentage: 100 }] },
      { id: 'B', versions: [{ id: 'version-B', percentage: 100 }] },
    ] as const;
    const bindings = {
      a: [
        { type: 'd1', database_id: 'target-db' },
        { type: 'r2_bucket', bucket_name: 'target-bucket' },
      ],
    };
    const splitWorld: AttachmentWorld = {
      ordinary,
      namespaces: [
        { name: 'a', pages: [{ scripts: [] }] },
        {
          name: 'B',
          pages: [
            { scripts: ['a'], nextCursor: 'opaque-A' },
            { cursor: 'opaque-A', scripts: ['B'] },
          ],
          tags: { B: ['z', 'A'], a: ['tag:a', 'b', 'Z'] },
          bindings,
        },
      ],
    };
    const singlePageWorld: AttachmentWorld = {
      ordinary,
      namespaces: [
        { name: 'a', pages: [{ scripts: [] }] },
        {
          name: 'B',
          pages: [{ scripts: ['B', 'a'] }],
          tags: { B: ['z', 'A'], a: ['tag:a', 'b', 'Z'] },
          bindings,
        },
      ],
    };
    const d1Split = await drain(
      client(recordingFetch(worldHandler(splitWorld)).fetch),
      D1_TARGET,
      { budget: 12 },
    );
    const d1Single = await drain(
      client(recordingFetch(worldHandler(singlePageWorld)).fetch),
      D1_TARGET,
      { budget: 12 },
    );
    const r2Single = await drain(
      client(recordingFetch(worldHandler(singlePageWorld)).fetch),
      R2_TARGET,
      { budget: 12 },
    );
    for (const result of [d1Split, d1Single, r2Single]) {
      expect(result.terminal.status).toBe('complete');
    }
    if (
      d1Split.terminal.status !== 'complete' ||
      d1Single.terminal.status !== 'complete' ||
      r2Single.terminal.status !== 'complete'
    ) {
      throw new Error('attachment scan did not complete');
    }
    const dispatchLeaves = [
      ['dispatch-settings', 'B', 'B', ['A', 'z'], false],
      ['dispatch-settings', 'B', 'a', ['Z', 'b', 'tag:a'], true],
    ] as const;
    const leaves = [
      ['ordinary-inventory', ['B', 'a']],
      ['ordinary-deployment', 'B', [['version-B', 100]]],
      ['ordinary-version', 'B', 'version-B', false],
      ['ordinary-deployment', 'a', [['version-a', 100]]],
      ['ordinary-version', 'a', 'version-a', false],
      [
        'dispatch-namespaces',
        [
          ['B', 'namespace-1', 2],
          ['a', 'namespace-0', 0],
        ],
      ],
      [
        'dispatch-namespace',
        'B',
        dispatchLeaves.length,
        multisetEvidence(dispatchLeaves),
      ],
      ['dispatch-namespace', 'a', 0, '0'.repeat(64)],
    ] as const;
    const expectedCount = 6 + dispatchLeaves.length + 2;
    expect(d1Split.terminal).toMatchObject({
      evidenceCount: expectedCount,
      evidenceSha256: evidence(D1_TARGET, leaves),
    });
    expect(d1Single.terminal).toMatchObject({
      evidenceCount: d1Split.terminal.evidenceCount,
      evidenceSha256: d1Split.terminal.evidenceSha256,
    });
    expect(r2Single.terminal).toMatchObject({
      evidenceCount: expectedCount,
      evidenceSha256: evidence(R2_TARGET, leaves),
    });
  });

  it('round-trips every exact progress arm and rejects cross-stage keys', () => {
    const common = {
      version: 1 as const,
      target: D1_TARGET,
      evidenceSha256: 'a'.repeat(64),
      evidenceCount: 10,
    };
    const arms: WorkerAttachmentScanProgress[] = [
      initialWorkerAttachmentScan(D1_TARGET),
      {
        ...common,
        stage: 'ordinary-deployment',
        ordinaryInventorySha256: 'b'.repeat(64),
        scriptIndex: 1,
        scriptName: 'script',
      },
      {
        ...common,
        stage: 'ordinary-version',
        ordinaryInventorySha256: 'b'.repeat(64),
        scriptIndex: 1,
        scriptName: 'script',
        deploymentSha256: 'c'.repeat(64),
        versionIndex: 2,
      },
      {
        ...common,
        stage: 'dispatch-namespace-inventory',
        ordinaryInventorySha256: 'b'.repeat(64),
        namespaceIndex: 0,
      },
      {
        ...common,
        stage: 'dispatch-script-page',
        ordinaryInventorySha256: 'b'.repeat(64),
        namespaceInventorySha256: 'd'.repeat(64),
        namespaceIndex: 1,
        namespaceName: 'fleet',
        pageStartCursor: 'cursor-2',
        pageNumber: 2,
        seenCursorSha256: [hash('cursor-1'), hash('cursor-2')],
        totalDispatchItems: 20,
        dispatchEvidenceSum256: 'e'.repeat(64),
        dispatchEvidenceCount: 20,
      },
      {
        ...common,
        stage: 'dispatch-script-settings',
        ordinaryInventorySha256: 'b'.repeat(64),
        namespaceInventorySha256: 'd'.repeat(64),
        namespaceIndex: 1,
        namespaceName: 'fleet',
        pageStartCursor: 'cursor-2',
        nextCursor: 'next',
        pageSha256: 'f'.repeat(64),
        pageItemCount: 3,
        itemOffset: 2,
        pageNumber: 2,
        seenCursorSha256: [hash('cursor-1'), hash('cursor-2'), hash('next')],
        totalDispatchItems: 20,
        dispatchEvidenceSum256: 'e'.repeat(64),
        dispatchEvidenceCount: 19,
      },
    ];
    for (const arm of arms) {
      expect(
        parseWorkerAttachmentScanProgress(
          JSON.parse(JSON.stringify(arm)),
          D1_TARGET,
        ),
      ).toEqual(arm);
      expect(() =>
        parseWorkerAttachmentScanProgress(
          { ...arm, unexpected: true },
          D1_TARGET,
        ),
      ).toThrow(CloudflareAttachmentScanProgressError);
    }
    for (const insufficientEvidence of [
      {
        version: 1,
        target: D1_TARGET,
        stage: 'ordinary-deployment',
        evidenceSha256: 'a'.repeat(64),
        evidenceCount: 1,
        ordinaryInventorySha256: 'b'.repeat(64),
        scriptIndex: 1,
        scriptName: 'script',
      },
      {
        version: 1,
        target: D1_TARGET,
        stage: 'ordinary-version',
        evidenceSha256: 'a'.repeat(64),
        evidenceCount: 4,
        ordinaryInventorySha256: 'b'.repeat(64),
        scriptIndex: 1,
        scriptName: 'script',
        deploymentSha256: 'c'.repeat(64),
        versionIndex: 2,
      },
      {
        version: 1,
        target: D1_TARGET,
        stage: 'dispatch-script-page',
        evidenceSha256: 'a'.repeat(64),
        evidenceCount: 2,
        ordinaryInventorySha256: 'b'.repeat(64),
        namespaceInventorySha256: 'c'.repeat(64),
        namespaceIndex: 1,
        namespaceName: 'fleet',
        pageNumber: 0,
        seenCursorSha256: [],
        totalDispatchItems: 0,
        dispatchEvidenceSum256: '0'.repeat(64),
        dispatchEvidenceCount: 0,
      },
    ]) {
      expect(() =>
        parseWorkerAttachmentScanProgress(insufficientEvidence, D1_TARGET),
      ).toThrow(CloudflareAttachmentScanProgressError);
    }
  });

  it('rejects future, wrong-target, accessor, prototype, hash, and index progress', () => {
    const valid = initialWorkerAttachmentScan(D1_TARGET);
    const accessor = vi.fn(() => 0);
    const deepTrap = vi.fn(() => Object.prototype);
    let deeplyNested: unknown = new Proxy({}, { getPrototypeOf: deepTrap });
    for (let depth = 0; depth < 65; depth += 1) {
      deeplyNested = [deeplyNested];
    }
    let wideArrayOwnKeyReads = 0;
    let wideArrayItemReads = 0;
    const wideArray = new Proxy(Array(8_192).fill(0), {
      ownKeys: (target) => {
        wideArrayOwnKeyReads += 1;
        return Reflect.ownKeys(target);
      },
      getOwnPropertyDescriptor: (target, property) => {
        if (property !== 'length') wideArrayItemReads += 1;
        return Reflect.getOwnPropertyDescriptor(target, property);
      },
    });
    let wideRecordValueReads = 0;
    const wideRecord = new Proxy(
      Object.fromEntries(
        Array.from({ length: 8_192 }, (_, index) => [`key-${index}`, 0]),
      ),
      {
        getOwnPropertyDescriptor: (target, property) => {
          wideRecordValueReads += 1;
          return Reflect.getOwnPropertyDescriptor(target, property);
        },
      },
    );
    let oversizedKeyValueReads = 0;
    const oversizedKey = new Proxy(
      { ['\u0000'.repeat(12_000)]: 0 },
      {
        getOwnPropertyDescriptor: (target, property) => {
          oversizedKeyValueReads += 1;
          return Reflect.getOwnPropertyDescriptor(target, property);
        },
      },
    );
    let largestNumberIndexRead = -1;
    const wideNumbers = new Proxy(Array(3_000).fill(Number.MAX_VALUE), {
      getOwnPropertyDescriptor: (target, property) => {
        if (typeof property === 'string' && /^\d+$/.test(property)) {
          largestNumberIndexRead = Number(property);
        }
        return Reflect.getOwnPropertyDescriptor(target, property);
      },
    });
    let cycleOwnKeyReads = 0;
    let cycleDescriptorReads = 0;
    const cycleTarget: { self?: unknown } = {};
    const cyclic = new Proxy(cycleTarget, {
      ownKeys: (target) => {
        cycleOwnKeyReads += 1;
        return Reflect.ownKeys(target);
      },
      getOwnPropertyDescriptor: (target, property) => {
        cycleDescriptorReads += 1;
        return Reflect.getOwnPropertyDescriptor(target, property);
      },
    });
    cycleTarget.self = cyclic;
    const encode = vi.spyOn(TextEncoder.prototype, 'encode');
    try {
      expect(() =>
        parseWorkerAttachmentScanProgress('x'.repeat(65_537), D1_TARGET),
      ).toThrow(CloudflareAttachmentScanProgressError);
      expect(encode).not.toHaveBeenCalled();
      expect(() =>
        parseWorkerAttachmentScanProgress(
          { ['x'.repeat(65_537)]: 0 },
          D1_TARGET,
        ),
      ).toThrow(CloudflareAttachmentScanProgressError);
      expect(encode).not.toHaveBeenCalled();
    } finally {
      encode.mockRestore();
    }
    for (const malformed of [
      { ...valid, version: 2 },
      { ...valid, target: R2_TARGET },
      { ...valid, evidenceSha256: 'not-a-hash' },
      { ...valid, evidenceCount: -1 },
      { ...valid, scriptIndex: 1 },
      { ...valid, scriptIndex: 10_001 },
      {
        version: 1,
        target: D1_TARGET,
        stage: 'dispatch-namespace-inventory',
        evidenceSha256: 'a'.repeat(64),
        evidenceCount: 1,
        ordinaryInventorySha256: 'b'.repeat(64),
        namespaceIndex: 1,
      },
      Object.assign(Object.create({ inherited: true }), valid),
      Object.defineProperty({ ...valid }, 'scriptIndex', {
        enumerable: true,
        get: accessor,
      }),
      new Proxy(valid, {
        getPrototypeOf: () => {
          throw new Error('prototype trap');
        },
      }),
      Object.assign({ ...valid }, { [Symbol('unexpected')]: true }),
      deeplyNested,
      wideArray,
      wideRecord,
      oversizedKey,
      wideNumbers,
      cyclic,
    ]) {
      expect(() =>
        parseWorkerAttachmentScanProgress(malformed, D1_TARGET),
      ).toThrow(CloudflareAttachmentScanProgressError);
    }
    expect(accessor).not.toHaveBeenCalled();
    expect(deepTrap).not.toHaveBeenCalled();
    expect(wideArrayOwnKeyReads).toBe(0);
    expect(wideArrayItemReads).toBe(0);
    expect(wideRecordValueReads).toBe(0);
    expect(oversizedKeyValueReads).toBe(0);
    expect(largestNumberIndexRead).toBeLessThan(2_999);
    expect(cycleOwnKeyReads).toBe(1);
    expect(cycleDescriptorReads).toBe(1);

    let progressError: unknown;
    try {
      parseWorkerAttachmentScanProgress({ ...valid, version: 2 }, D1_TARGET);
    } catch (error) {
      progressError = error;
    }
    expect(progressError).toBeInstanceOf(CloudflareAttachmentScanProgressError);
    expect(progressError).toMatchObject({
      name: 'CloudflareAttachmentScanProgressError',
      message: 'Cloudflare attachment scan progress is malformed',
    });
  });

  it('enforces cursor, page, item, evidence, and reachable-state bounds', () => {
    const page = {
      version: 1 as const,
      target: D1_TARGET,
      stage: 'dispatch-script-page' as const,
      evidenceSha256: 'a'.repeat(64),
      evidenceCount: 2,
      ordinaryInventorySha256: 'b'.repeat(64),
      namespaceInventorySha256: 'c'.repeat(64),
      namespaceIndex: 0,
      namespaceName: 'fleet',
      pageNumber: 0,
      seenCursorSha256: [] as string[],
      totalDispatchItems: 0,
      dispatchEvidenceSum256: '0'.repeat(64),
      dispatchEvidenceCount: 0,
    };
    const oversizedCursor = 'é'.repeat(2_049);
    const page100Cursors = Array.from(
      { length: 100 },
      (_, index) => `cursor-${index}`,
    );
    for (const malformed of [
      {
        ...page,
        pageStartCursor: oversizedCursor,
        pageNumber: 1,
        seenCursorSha256: [hash(oversizedCursor)],
      },
      {
        ...page,
        pageStartCursor: page100Cursors.at(-1),
        pageNumber: 100,
        seenCursorSha256: page100Cursors.map(hash),
      },
      {
        ...page,
        totalDispatchItems: 10_001,
        dispatchEvidenceSum256: 'd'.repeat(64),
        dispatchEvidenceCount: 10_001,
      },
      {
        ...page,
        totalDispatchItems: 1,
        dispatchEvidenceSum256: 'd'.repeat(64),
        dispatchEvidenceCount: 1,
      },
      { ...page, evidenceCount: 1_000_001 },
      {
        ...page,
        pageStartCursor: 'cursor',
        pageNumber: 1,
        seenCursorSha256: Object.defineProperty([hash('cursor')], '0', {
          enumerable: true,
          get: () => hash('cursor'),
        }),
      },
      {
        ...page,
        pageStartCursor: '',
        pageNumber: 1,
        seenCursorSha256: [hash('')],
      },
      {
        ...page,
        stage: 'dispatch-script-settings',
        nextCursor: '',
        pageSha256: 'd'.repeat(64),
        pageItemCount: 1,
        itemOffset: 0,
        seenCursorSha256: [hash('')],
        totalDispatchItems: 1,
      },
      {
        ...page,
        stage: 'dispatch-script-settings',
        pageSha256: 'd'.repeat(64),
        pageItemCount: 0,
        itemOffset: 0,
      },
      {
        ...page,
        stage: 'dispatch-script-settings',
        pageSha256: hash(JSON.stringify([null, [['only', []]], null])),
        pageItemCount: 1,
        itemOffset: 0,
        totalDispatchItems: 2,
        dispatchEvidenceSum256: 'd'.repeat(64),
        dispatchEvidenceCount: 1,
      },
    ]) {
      expect(() =>
        parseWorkerAttachmentScanProgress(malformed, D1_TARGET),
      ).toThrow(CloudflareAttachmentScanProgressError);
    }
    expect(() =>
      parseWorkerAttachmentScanProgress(
        {
          ...page,
          stage: 'dispatch-script-settings',
          pageSha256: 'd'.repeat(64),
          pageItemCount: 3,
          itemOffset: 4,
          totalDispatchItems: 3,
          dispatchEvidenceSum256: 'e'.repeat(64),
          dispatchEvidenceCount: 4,
        },
        D1_TARGET,
      ),
    ).toThrow(CloudflareAttachmentScanProgressError);
  });

  it('rejects repeated and unbounded dispatch cursors and overfilled pages', async () => {
    for (const mode of ['same', 'cycle', 'endless', 'overfilled'] as const) {
      let page = 0;
      const fixture = recordingFetch(({ url }) => {
        const target = new URL(url);
        if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
        if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
          return pageArray([{ namespace_name: 'fleet' }]);
        }
        if (target.pathname.endsWith('/namespaces/fleet/scripts')) {
          if (mode === 'overfilled') {
            return pageArray(
              Array.from({ length: 101 }, (_, index) => ({
                id: `script-${index}`,
                tags: [],
              })),
            );
          }
          const cursor = target.searchParams.get('cursor');
          page += 1;
          const next =
            mode === 'same'
              ? 'a'
              : mode === 'cycle'
                ? cursor === null
                  ? 'a'
                  : cursor === 'a'
                    ? 'b'
                    : 'a'
                : `cursor-${page}`;
          return pageArray([], { cursor: next });
        }
        throw new Error(`unexpected request ${target.pathname}`);
      });
      await expect(drain(client(fixture.fetch), D1_TARGET)).rejects.toThrow(
        mode === 'overfilled'
          ? 'Cloudflare dispatch script listing returned more than 100 items in one page'
          : mode === 'endless'
            ? 'Cloudflare dispatch script listing exceeded 100 pages'
            : 'Cloudflare dispatch script listing repeated a cursor',
      );
    }
  });

  it('detects ordinary inventory and deployment drift before later version reads', async () => {
    for (const driftTarget of ['inventory', 'deployment'] as const) {
      let scriptsCalls = 0;
      let deploymentCalls = 0;
      let versionCalls = 0;
      const fixture = recordingFetch(({ url }) => {
        const target = new URL(url);
        if (target.pathname.endsWith('/workers/scripts')) {
          scriptsCalls += 1;
          return pageArray([
            {
              id:
                driftTarget === 'inventory' && scriptsCalls > 1
                  ? 'changed'
                  : 'ordinary',
            },
          ]);
        }
        if (target.pathname.endsWith('/deployments')) {
          deploymentCalls += 1;
          return single({
            deployments: [
              {
                versions: [
                  { version_id: 'v1', percentage: 50 },
                  {
                    version_id:
                      driftTarget === 'deployment' && deploymentCalls > 1
                        ? 'changed-v2'
                        : 'v2',
                    percentage: 50,
                  },
                ],
              },
            ],
          });
        }
        if (target.pathname.includes('/versions/')) {
          versionCalls += 1;
          return single({ resources: { bindings: [] } });
        }
        throw new Error(`unexpected request ${target.pathname}`);
      });
      const subject = client(fixture.fetch);
      const first = await advanceCloudflareWorkerAttachmentScan(subject, {
        target: D1_TARGET,
        progress: initialWorkerAttachmentScan(D1_TARGET),
        maxProviderRequests: 9,
      });
      expect(first.status).toBe('pending');
      if (first.status !== 'pending') throw new Error('expected pending');
      await expect(
        advanceCloudflareWorkerAttachmentScan(subject, {
          target: D1_TARGET,
          progress: first.progress,
          maxProviderRequests: 9,
        }),
      ).rejects.toBeInstanceOf(CloudflareAttachmentScanDriftError);
      expect(versionCalls).toBe(1);
    }

    let deploymentCalls = 0;
    let ordinaryRequests = 0;
    const fixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) {
        ordinaryRequests += 1;
        return pageArray([{ id: 'ordinary' }]);
      }
      if (target.pathname.endsWith('/deployments')) deploymentCalls += 1;
      throw new Error(`unexpected request ${target.pathname}`);
    });
    let driftError: unknown;
    try {
      await advanceCloudflareWorkerAttachmentScan(client(fixture.fetch), {
        target: D1_TARGET,
        progress: {
          version: 1,
          target: D1_TARGET,
          stage: 'ordinary-deployment',
          evidenceSha256: 'a'.repeat(64),
          evidenceCount: 3,
          ordinaryInventorySha256: hash(JSON.stringify(['ordinary'])),
          scriptIndex: 2,
          scriptName: 'ordinary',
        },
        maxProviderRequests: 9,
      });
    } catch (error) {
      driftError = error;
    }
    expect(driftError).toBeInstanceOf(CloudflareAttachmentScanDriftError);
    expect(driftError).toMatchObject({
      name: 'CloudflareAttachmentScanDriftError',
      message:
        'Cloudflare attachment inventory changed during a resumable scan',
    });
    expect(deploymentCalls).toBe(0);
    expect(ordinaryRequests).toBe(1);

    await expect(
      advanceCloudflareWorkerAttachmentScan(client(fixture.fetch), {
        target: D1_TARGET,
        progress: {
          version: 1,
          target: D1_TARGET,
          stage: 'ordinary-script-inventory',
          evidenceSha256: evidence(D1_TARGET, [
            ['ordinary-inventory', ['ordinary']],
          ]),
          evidenceCount: 1,
          ordinaryInventorySha256: hash(JSON.stringify(['ordinary'])),
          scriptIndex: 1,
        },
        maxProviderRequests: 9,
      }),
    ).rejects.toBeInstanceOf(CloudflareAttachmentScanProgressError);
    expect(ordinaryRequests).toBe(1);
  });

  it('detects namespace and partial-page drift before any later settings read', async () => {
    for (const driftTarget of ['namespace', 'page'] as const) {
      let namespaceCalls = 0;
      let pageCalls = 0;
      let settingsCalls = 0;
      const fixture = recordingFetch(({ url }) => {
        const target = new URL(url);
        if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
        if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
          namespaceCalls += 1;
          return pageArray([
            {
              namespace_name:
                driftTarget === 'namespace' && namespaceCalls > 1
                  ? 'changed'
                  : 'fleet',
            },
          ]);
        }
        if (target.pathname.endsWith('/namespaces/fleet/scripts')) {
          pageCalls += 1;
          return pageArray([
            { id: 'a', tags: [] },
            {
              id: driftTarget === 'page' && pageCalls > 1 ? 'changed-b' : 'b',
              tags: [],
            },
          ]);
        }
        if (target.pathname.endsWith('/settings')) {
          settingsCalls += 1;
          return single({ bindings: [] });
        }
        throw new Error(`unexpected request ${target.pathname}`);
      });
      const subject = client(fixture.fetch);
      const first = await advanceCloudflareWorkerAttachmentScan(subject, {
        target: D1_TARGET,
        progress: initialWorkerAttachmentScan(D1_TARGET),
        maxProviderRequests: 9,
      });
      expect(first.status).toBe('pending');
      if (first.status !== 'pending') throw new Error('expected pending');
      await expect(
        advanceCloudflareWorkerAttachmentScan(subject, {
          target: D1_TARGET,
          progress: first.progress,
          maxProviderRequests: 9,
        }),
      ).rejects.toBeInstanceOf(CloudflareAttachmentScanDriftError);
      expect(settingsCalls).toBe(0);
    }

    let mutateCommittedItem = false;
    let committedSettingsCalls = 0;
    const committedFixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
      if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
        return pageArray([{ namespace_name: 'fleet' }]);
      }
      if (target.pathname.endsWith('/namespaces/fleet/scripts')) {
        return pageArray([
          { id: mutateCommittedItem ? 'changed-a' : 'a', tags: [] },
          { id: 'b', tags: [] },
        ]);
      }
      if (target.pathname.endsWith('/settings')) {
        committedSettingsCalls += 1;
        return single({ bindings: [] });
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    const subject = client(committedFixture.fetch);
    const beforeSettings = await advanceCloudflareWorkerAttachmentScan(
      subject,
      {
        target: D1_TARGET,
        progress: initialWorkerAttachmentScan(D1_TARGET),
        maxProviderRequests: 9,
      },
    );
    expect(beforeSettings.status).toBe('pending');
    if (beforeSettings.status !== 'pending')
      throw new Error('expected pending');
    const requestsBeforeMalformedProgress = committedFixture.requests.length;
    await expect(
      advanceCloudflareWorkerAttachmentScan(subject, {
        target: D1_TARGET,
        progress: { ...beforeSettings.progress, evidenceCount: 1 },
        maxProviderRequests: 9,
      }),
    ).rejects.toBeInstanceOf(CloudflareAttachmentScanProgressError);
    expect(committedFixture.requests).toHaveLength(
      requestsBeforeMalformedProgress,
    );
    const afterFirstSetting = await advanceCloudflareWorkerAttachmentScan(
      subject,
      {
        target: D1_TARGET,
        progress: beforeSettings.progress,
        maxProviderRequests: 9,
      },
    );
    expect(afterFirstSetting).toMatchObject({
      status: 'pending',
      progress: { stage: 'dispatch-script-settings', itemOffset: 1 },
    });
    if (afterFirstSetting.status !== 'pending') {
      throw new Error('expected pending');
    }
    mutateCommittedItem = true;
    await expect(
      advanceCloudflareWorkerAttachmentScan(subject, {
        target: D1_TARGET,
        progress: afterFirstSetting.progress,
        maxProviderRequests: 9,
      }),
    ).rejects.toBeInstanceOf(CloudflareAttachmentScanDriftError);
    expect(committedSettingsCalls).toBe(1);

    let aliasedSettingsCalls = 0;
    const aliasFixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
      if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
        return pageArray([{ namespace_name: 'fleet' }]);
      }
      if (target.pathname.endsWith('/namespaces/fleet/scripts')) {
        const cursor = target.searchParams.get('cursor');
        return cursor === null
          ? pageArray([], { cursor: 'start' })
          : pageArray([
              { id: 'a', tags: [] },
              { id: 'b', tags: [] },
            ]);
      }
      if (target.pathname.endsWith('/settings')) {
        aliasedSettingsCalls += 1;
        return single({ bindings: [] });
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    const aliasSubject = client(aliasFixture.fetch);
    const beforeSecondPage = await advanceCloudflareWorkerAttachmentScan(
      aliasSubject,
      {
        target: D1_TARGET,
        progress: initialWorkerAttachmentScan(D1_TARGET),
        maxProviderRequests: 9,
      },
    );
    expect(beforeSecondPage).toMatchObject({
      status: 'pending',
      progress: { stage: 'dispatch-script-page', pageStartCursor: 'start' },
    });
    if (beforeSecondPage.status !== 'pending') {
      throw new Error('expected pending');
    }
    const beforeAliasedSettings = await advanceCloudflareWorkerAttachmentScan(
      aliasSubject,
      {
        target: D1_TARGET,
        progress: beforeSecondPage.progress,
        maxProviderRequests: 9,
      },
    );
    expect(beforeAliasedSettings).toMatchObject({
      status: 'pending',
      progress: { stage: 'dispatch-script-settings', itemOffset: 0 },
    });
    if (
      beforeAliasedSettings.status !== 'pending' ||
      beforeAliasedSettings.progress.stage !== 'dispatch-script-settings'
    ) {
      throw new Error('expected pending');
    }
    await expect(
      advanceCloudflareWorkerAttachmentScan(aliasSubject, {
        target: D1_TARGET,
        progress: {
          ...beforeAliasedSettings.progress,
          pageStartCursor: 'alias',
          seenCursorSha256: [hash('alias')],
        },
        maxProviderRequests: 9,
      }),
    ).rejects.toBeInstanceOf(CloudflareAttachmentScanDriftError);
    expect(aliasedSettingsCalls).toBe(0);

    const forgedFixture = recordingFetch(({ url }) => {
      throw new Error(`unexpected request ${new URL(url).pathname}`);
    });
    await expect(
      advanceCloudflareWorkerAttachmentScan(client(forgedFixture.fetch), {
        target: D1_TARGET,
        progress: {
          version: 1,
          target: D1_TARGET,
          stage: 'dispatch-script-settings',
          evidenceSha256: 'a'.repeat(64),
          evidenceCount: 2,
          ordinaryInventorySha256: hash(JSON.stringify([])),
          namespaceInventorySha256: hash(
            JSON.stringify([['fleet', null, null]]),
          ),
          namespaceIndex: 0,
          namespaceName: 'fleet',
          pageSha256: hash(JSON.stringify([null, [['only', []]], null])),
          pageItemCount: 1,
          itemOffset: 1,
          pageNumber: 0,
          seenCursorSha256: [],
          totalDispatchItems: 2,
          dispatchEvidenceSum256: 'd'.repeat(64),
          dispatchEvidenceCount: 2,
        },
        maxProviderRequests: 9,
      }),
    ).rejects.toBeInstanceOf(CloudflareAttachmentScanProgressError);
    expect(forgedFixture.requests).toHaveLength(0);

    let namespaceRequests = 0;
    const namespaceFixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
        namespaceRequests += 1;
        return pageArray([{ namespace_name: 'fleet' }]);
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    const ordinaryInventorySha256 = hash(JSON.stringify([]));
    const namespaceInventorySha256 = hash(
      JSON.stringify([['fleet', null, null]]),
    );
    await expect(
      advanceCloudflareWorkerAttachmentScan(client(namespaceFixture.fetch), {
        target: D1_TARGET,
        progress: {
          version: 1,
          target: D1_TARGET,
          stage: 'dispatch-script-page',
          evidenceSha256: 'a'.repeat(64),
          evidenceCount: 4,
          ordinaryInventorySha256,
          namespaceInventorySha256,
          namespaceIndex: 2,
          namespaceName: 'fleet',
          pageNumber: 0,
          seenCursorSha256: [],
          totalDispatchItems: 0,
          dispatchEvidenceSum256: '0'.repeat(64),
          dispatchEvidenceCount: 0,
        },
        maxProviderRequests: 9,
      }),
    ).rejects.toBeInstanceOf(CloudflareAttachmentScanDriftError);
    expect(namespaceRequests).toBe(1);

    await expect(
      advanceCloudflareWorkerAttachmentScan(client(namespaceFixture.fetch), {
        target: D1_TARGET,
        progress: {
          version: 1,
          target: D1_TARGET,
          stage: 'dispatch-namespace-inventory',
          evidenceSha256: evidence(D1_TARGET, [
            ['ordinary-inventory', []],
            ['dispatch-namespaces', [['fleet', null, null]]],
          ]),
          evidenceCount: 2,
          ordinaryInventorySha256,
          namespaceInventorySha256,
          namespaceIndex: 1,
        },
        maxProviderRequests: 9,
      }),
    ).rejects.toBeInstanceOf(CloudflareAttachmentScanProgressError);
    expect(namespaceRequests).toBe(1);
  });

  it('preserves plain-only initial namespace 404 and refuses configured or later 404', async () => {
    const plainFixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
      if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
        return apiFailure(404);
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    await expect(
      drain(client(plainFixture.fetch, { plainOnly: true }), D1_TARGET),
    ).resolves.toMatchObject({ terminal: { status: 'complete' } });
    await expect(
      drain(client(plainFixture.fetch), D1_TARGET),
    ).rejects.toMatchObject({ status: 404 });

    const namespaceList = vi
      .spyOn(BaseNamespaces.prototype, 'list')
      .mockReturnValueOnce(
        (async function* () {
          yield { namespace_name: 'first' };
          throw Object.assign(new Error('later missing'), { status: 404 });
        })() as never,
      );
    try {
      const laterFixture = recordingFetch(({ url }) => {
        const target = new URL(url);
        if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
        if (target.pathname.endsWith('/namespaces/first/scripts')) {
          return pageArray([]);
        }
        throw new Error(`unexpected request ${target.pathname}`);
      });
      await expect(
        drain(client(laterFixture.fetch, { plainOnly: true }), D1_TARGET),
      ).rejects.toMatchObject({ status: 404 });
    } finally {
      namespaceList.mockRestore();
    }
  });

  it('returns attached only for a match and makes no provider call after an early match', async () => {
    for (const match of ['early', 'final', 'none'] as const) {
      const events: string[] = [];
      const versions: VersionFixture[] = [
        {
          id: 'v1',
          percentage: 50,
          bindings:
            match === 'early' ? [{ type: 'd1', database_id: 'target-db' }] : [],
        },
        {
          id: 'v2',
          percentage: 50,
          bindings:
            match === 'final' ? [{ type: 'd1', database_id: 'target-db' }] : [],
        },
      ];
      const fixture = recordingFetch(
        worldHandler(
          { ordinary: [{ id: 'ordinary', versions }], namespaces: [] },
          events,
        ),
      );
      const result = await drain(client(fixture.fetch), D1_TARGET, {
        budget: 12,
        stopOnFirstAttachment: true,
      });
      expect(result.terminal.status).toBe(
        match === 'none' ? 'complete' : 'attached',
      );
      expect(events.some((event) => event.includes('/versions/v2'))).toBe(
        match !== 'early',
      );
      if (match !== 'none') {
        expect(
          events.some((event) => event.includes('/dispatch/namespaces')),
        ).toBe(false);
      }
    }
  });

  it('forwards and honors an in-flight SDK abort signal without follow-up requests', async () => {
    const response = deferred<Response>();
    const started = deferred<AbortSignal>();
    let requests = 0;
    const signalCaptureFetch: typeof fetch = async (_input, init) => {
      requests += 1;
      const signal = init?.signal;
      if (!signal) throw new Error('SDK request had no signal');
      started.resolve(signal);
      return Promise.race([
        response.promise,
        new Promise<Response>((_resolve, reject) => {
          signal.addEventListener('abort', () => reject(signal.reason), {
            once: true,
          });
        }),
      ]);
    };
    const controller = new AbortController();
    const operation = advanceCloudflareWorkerAttachmentScan(
      client(signalCaptureFetch),
      {
        target: D1_TARGET,
        progress: initialWorkerAttachmentScan(D1_TARGET),
        maxProviderRequests: 9,
        signal: controller.signal,
      },
    );
    const received = await started.promise;
    expect(received).not.toBe(controller.signal);
    expect(received.aborted).toBe(false);
    controller.abort(new Error('stop SDK'));
    expect(received.aborted).toBe(true);
    await expect(operation).rejects.toThrow();
    expect(requests).toBe(1);
  });

  it('forwards and honors an in-flight raw-page abort signal without retry', async () => {
    let rawRequests = 0;
    let receivedSignal: AbortSignal | undefined;
    const fixture = recordingFetch(({ url }) => {
      const target = new URL(url);
      if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
      if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
        return pageArray([{ namespace_name: 'fleet' }]);
      }
      throw new Error(`unexpected request ${target.pathname}`);
    });
    const signalFetch: typeof fetch = (input, init) => {
      const url = new URL(
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
      );
      if (url.pathname.endsWith('/namespaces/fleet/scripts')) {
        rawRequests += 1;
        receivedSignal = init?.signal ?? undefined;
        const signal = init?.signal;
        if (!signal)
          return Promise.reject(new Error('raw request had no signal'));
        return new Promise<Response>((_resolve, reject) => {
          signal.addEventListener('abort', () => reject(signal.reason), {
            once: true,
          });
        });
      }
      return fixture.fetch(input, init);
    };
    const controller = new AbortController();
    const operation = advanceCloudflareWorkerAttachmentScan(
      client(signalFetch),
      {
        target: D1_TARGET,
        progress: initialWorkerAttachmentScan(D1_TARGET),
        maxProviderRequests: 9,
        signal: controller.signal,
      },
    );
    await vi.waitFor(() => expect(receivedSignal).toBeDefined());
    expect(receivedSignal).not.toBe(controller.signal);
    expect(receivedSignal?.aborted).toBe(false);
    controller.abort(new Error('stop raw'));
    expect(receivedSignal?.aborted).toBe(true);
    await expect(operation).rejects.toThrow();
    expect(rawRequests).toBe(1);
  });

  it('injects authorization internally without serializing it into progress or errors', async () => {
    for (const terminalCursor of [null, ''] as const) {
      let rawRequests = 0;
      const terminalFixture = recordingFetch((request) => {
        const target = new URL(request.url);
        if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
        if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
          return pageArray([{ namespace_name: 'fleet' }]);
        }
        rawRequests += 1;
        return Response.json({
          success: true,
          errors: [],
          messages: [],
          result: [],
          result_info: {
            cursor: terminalCursor,
            cursors: { after: 'must-not-override-explicit-cursor' },
          },
        });
      });
      await expect(
        drain(client(terminalFixture.fetch), D1_TARGET),
      ).resolves.toMatchObject({ terminal: { status: 'complete' } });
      expect(rawRequests).toBe(1);
    }

    for (const resultInfo of [42, { cursors: 42 }]) {
      const malformedMetadata = recordingFetch((request) => {
        const target = new URL(request.url);
        if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
        if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
          return pageArray([{ namespace_name: 'fleet' }]);
        }
        return Response.json({
          success: true,
          errors: [],
          messages: [],
          result: [],
          result_info: resultInfo,
        });
      });
      await expect(
        drain(client(malformedMetadata.fetch), D1_TARGET),
      ).rejects.toThrow(
        'Cloudflare dispatch script listing returned a malformed cursor',
      );
    }

    const headers: string[] = [];
    const fixture = recordingFetch((request) => {
      const target = new URL(request.url);
      if (target.pathname.endsWith('/workers/scripts')) return pageArray([]);
      if (target.pathname.endsWith('/workers/dispatch/namespaces')) {
        return pageArray([{ namespace_name: 'fleet' }]);
      }
      headers.push(request.headers.get('authorization') ?? '');
      return pageArray([], { cursor: 42 as unknown as string });
    });
    let refusal: unknown;
    try {
      await drain(client(fixture.fetch), D1_TARGET);
    } catch (error) {
      refusal = error;
    }
    expect(refusal).toBeInstanceOf(Error);
    expect((refusal as Error).message).toBe(
      'Cloudflare dispatch script listing returned a malformed cursor',
    );
    expect(String(refusal)).not.toContain('token');
    expect(String(refusal)).not.toContain('Bearer');
    expect(headers).toEqual(['Bearer token']);
    expect(
      JSON.stringify(initialWorkerAttachmentScan(D1_TARGET)),
    ).not.toContain('token');
  });

  it('maps the real bounded decommission scan and refuses hostile internal results', async () => {
    const emptyFixture = recordingFetch(
      worldHandler({ ordinary: [], namespaces: [] }),
    );
    const emptyClient = client(emptyFixture.fetch);
    const initial = initialWorkerAttachmentScan(D1_TARGET);
    const complete = await emptyClient.advanceDecommissionAttachmentScan({
      progress: initial,
      maxProviderRequests: 12,
    });
    expect(complete).toMatchObject({
      status: 'complete',
      providerFetchAttemptsReserved: 6,
    });
    expect(Object.keys(complete).sort()).toEqual([
      'evidenceCount',
      'evidenceSha256',
      'providerFetchAttemptsReserved',
      'status',
    ]);

    const ordinaryFixture = recordingFetch(
      worldHandler({
        ordinary: [
          {
            id: 'ordinary',
            versions: [
              {
                id: 'v1',
                percentage: 100,
                bindings: [{ type: 'd1', database_id: 'target-db' }],
              },
            ],
          },
        ],
        namespaces: [],
      }),
    );
    await expect(
      client(ordinaryFixture.fetch).advanceDecommissionAttachmentScan({
        progress: initial,
        maxProviderRequests: 12,
      }),
    ).resolves.toEqual({
      status: 'attached',
      attachment: { plane: 'ordinary', scriptName: 'ordinary' },
      providerFetchAttemptsReserved: 9,
    });
    expect(
      ordinaryFixture.requests.some(({ url }) =>
        url.includes('/workers/dispatch/namespaces'),
      ),
    ).toBe(false);

    const dispatchFixture = recordingFetch(
      worldHandler({
        ordinary: [],
        namespaces: [
          {
            name: 'fleet',
            pages: [{ scripts: ['dispatch'] }],
            bindings: {
              dispatch: [{ type: 'r2_bucket', bucket_name: 'target-bucket' }],
            },
          },
        ],
      }),
    );
    const dispatchClient = client(dispatchFixture.fetch);
    const dispatchPending =
      await dispatchClient.advanceDecommissionAttachmentScan({
        progress: initialWorkerAttachmentScan(R2_TARGET),
        maxProviderRequests: 12,
      });
    expect(dispatchPending).toEqual({
      status: 'pending',
      progress: {
        version: 1,
        target: R2_TARGET,
        stage: 'dispatch-script-settings',
        evidenceSha256:
          '1c3482eb38516849afb66c2b303a216a39a3946e602a27234d52922a5d7b0293',
        evidenceCount: 2,
        ordinaryInventorySha256:
          '4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945',
        namespaceInventorySha256:
          'def04ac77fe26c1a976ff1a49bba63c2aaba7d547843733a1db279560b949fc9',
        namespaceIndex: 0,
        namespaceName: 'fleet',
        pageSha256:
          '2ac417d32ad8b417715caee181af9a3f4dca29e57a9cf5e8f897abd89d53b894',
        pageItemCount: 1,
        itemOffset: 0,
        pageNumber: 0,
        seenCursorSha256: [],
        totalDispatchItems: 1,
        dispatchEvidenceSum256: '0'.repeat(64),
        dispatchEvidenceCount: 0,
      },
      providerFetchAttemptsReserved: 12,
    });
    if (dispatchPending.status !== 'pending') {
      throw new Error('expected pending dispatch scan');
    }
    await expect(
      dispatchClient.advanceDecommissionAttachmentScan({
        progress: dispatchPending.progress,
        maxProviderRequests: 9,
      }),
    ).resolves.toEqual({
      status: 'attached',
      attachment: {
        plane: 'dispatch',
        scriptName: 'dispatch',
        dispatchNamespace: 'fleet',
      },
      providerFetchAttemptsReserved: 9,
    });

    let activeWorld: AttachmentWorld = {
      ordinary: [
        { id: 'first', versions: [{ id: 'v1', percentage: 100 }] },
        { id: 'second', versions: [{ id: 'v2', percentage: 100 }] },
      ],
      namespaces: [],
    };
    const changingFixture = recordingFetch((request) =>
      worldHandler(activeWorld)(request),
    );
    const changingClient = client(changingFixture.fetch);
    const pending = await changingClient.advanceDecommissionAttachmentScan({
      progress: initial,
      maxProviderRequests: 9,
    });
    expect(pending.status).toBe('pending');
    if (pending.status !== 'pending') throw new Error('expected pending scan');
    activeWorld = {
      ordinary: [
        { id: 'first', versions: [{ id: 'v1', percentage: 100 }] },
        { id: 'changed', versions: [{ id: 'v2', percentage: 100 }] },
      ],
      namespaces: [],
    };
    await expect(
      changingClient.advanceDecommissionAttachmentScan({
        progress: pending.progress,
        maxProviderRequests: 12,
      }),
    ).resolves.toEqual({ status: 'drift' });

    const abort = new AbortController();
    const abortReason = new Error('stop bounded scan');
    abort.abort(abortReason);
    await expect(
      emptyClient.advanceDecommissionAttachmentScan({
        progress: initial,
        maxProviderRequests: 12,
        signal: abort.signal,
      }),
    ).rejects.toBe(abortReason);

    const sentinel = new Error('provider read failed');
    sentinel.name = 'CloudflareAttachmentScanDriftError';
    const sentinelSignal = new AbortController().signal;
    Object.defineProperty(sentinelSignal, 'throwIfAborted', {
      value() {
        throw sentinel;
      },
    });
    await expect(
      emptyClient.advanceDecommissionAttachmentScan({
        progress: initial,
        maxProviderRequests: 12,
        signal: sentinelSignal,
      }),
    ).rejects.toBe(sentinel);

    const basePending = {
      status: 'pending' as const,
      progress: initial,
      attachments: [] as const,
      providerFetchAttemptsReserved: 3,
    };
    expect(mapDecommissionAttachmentScanChunk(basePending)).toEqual({
      status: 'pending',
      progress: initial,
      providerFetchAttemptsReserved: 3,
    });
    expect(
      Object.keys(mapDecommissionAttachmentScanChunk(basePending)),
    ).toEqual(['status', 'progress', 'providerFetchAttemptsReserved']);

    expect(
      mapDecommissionAttachmentScanChunk({
        status: 'attached',
        attachment: {
          plane: 'ordinary',
          scriptName: 'ordinary',
          dispatchNamespace: 'must-be-stripped',
          token: 'must-be-stripped',
        } as never,
        providerFetchAttemptsReserved: 3,
      }),
    ).toEqual({
      status: 'attached',
      attachment: { plane: 'ordinary', scriptName: 'ordinary' },
      providerFetchAttemptsReserved: 3,
    });
    expect(
      mapDecommissionAttachmentScanChunk({
        status: 'attached',
        attachment: {
          plane: 'dispatch',
          scriptName: 'dispatch',
          dispatchNamespace: 'fleet',
          token: 'must-be-stripped',
        } as never,
        providerFetchAttemptsReserved: 3,
      }),
    ).toEqual({
      status: 'attached',
      attachment: {
        plane: 'dispatch',
        scriptName: 'dispatch',
        dispatchNamespace: 'fleet',
      },
      providerFetchAttemptsReserved: 3,
    });

    const expectExactMapperFailure = (
      operation: () => unknown,
      message: string,
    ) => {
      let refusal: unknown;
      try {
        operation();
      } catch (error) {
        refusal = error;
      }
      expect(refusal).toBeInstanceOf(Error);
      expect((refusal as Error).message).toBe(message);
    };
    for (const chunk of [
      {
        ...basePending,
        attachments: [{ plane: 'ordinary' as const, scriptName: 'unexpected' }],
      },
      {
        status: 'complete' as const,
        evidenceSha256: 'a'.repeat(64),
        evidenceCount: 2,
        attachments: [{ plane: 'ordinary' as const, scriptName: 'unexpected' }],
        providerFetchAttemptsReserved: 3,
      },
    ]) {
      expectExactMapperFailure(
        () => mapDecommissionAttachmentScanChunk(chunk),
        'bounded attachment scan returned unexpected accumulated attachments',
      );
    }
    for (const attachment of [
      { plane: 'dispatch', scriptName: 'broken' },
      { plane: 'dispatch', scriptName: 'broken', dispatchNamespace: '' },
      {
        plane: 'unknown',
        scriptName: 'broken',
        dispatchNamespace: 'fleet',
      },
    ]) {
      expectExactMapperFailure(
        () =>
          mapDecommissionAttachmentScanChunk({
            status: 'attached',
            attachment: attachment as never,
            providerFetchAttemptsReserved: 3,
          }),
        'bounded attachment scan returned malformed dispatch attachment',
      );
    }
    expectExactMapperFailure(
      () => mapDecommissionAttachmentScanChunk({ status: 'unknown' } as never),
      'bounded attachment scan returned unknown result',
    );
  });

  it('keeps the scan friend off the root while retaining its internal callable seam', () => {
    type Equal<Left, Right> =
      (<Value>() => Value extends Left ? 1 : 2) extends <
        Value,
      >() => Value extends Right ? 1 : 2
        ? true
        : false;
    const movedTypesAreIdentical: readonly [
      Equal<WorkerAttachment, StateWorkerAttachment>,
      Equal<WorkerAttachmentScanChunk, StateWorkerAttachmentScanChunk>,
      Equal<WorkerAttachmentScanInput, StateWorkerAttachmentScanInput>,
      Equal<WorkerAttachmentScanProgress, StateWorkerAttachmentScanProgress>,
      Equal<WorkerAttachmentScanTarget, StateWorkerAttachmentScanTarget>,
    ] = [true, true, true, true, true];
    const decommissionProgressMatchesScanner: WorkerAttachmentScanProgress extends DecommissionAttachmentProgress
      ? DecommissionAttachmentProgress extends WorkerAttachmentScanProgress
        ? true
        : false
      : false = true;

    expect('advanceCloudflareWorkerAttachmentScan' in fleetRoot).toBe(false);
    expect('mapDecommissionAttachmentScanChunk' in fleetRoot).toBe(false);
    expect(typeof advanceCloudflareWorkerAttachmentScan).toBe('function');
    expect(initialWorkerAttachmentScan).toBe(
      initialWorkerAttachmentScanFromState,
    );
    expect(parseWorkerAttachmentScanProgress).toBe(
      parseWorkerAttachmentScanProgressFromState,
    );
    expect(movedTypesAreIdentical).toEqual([true, true, true, true, true]);
    expect(decommissionProgressMatchesScanner).toBe(true);
    expect(CLOUDFLARE_SDK_MAX_RETRIES).toBe(2);
    expect(CLOUDFLARE_SDK_MAX_ATTEMPTS).toBe(3);
  });
});
