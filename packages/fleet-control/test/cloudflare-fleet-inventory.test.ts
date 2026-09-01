// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  advanceCloudflareFleetInventoryStage,
  CloudflareFleetInventoryBudgetError,
  CloudflareFleetInventoryCursorDriftError,
  CloudflareFleetInventoryCursorError,
  type CloudflareFleetInventoryDeps,
  type FleetInventoryBucketPage,
  type FleetInventoryDispatchNamespace,
  type FleetInventoryDispatchWorker,
  type FleetInventoryOrdinaryScriptDetail,
} from '../src/cloudflare-fleet-inventory.js';
import type { CloudflareSdk } from '../src/cloudflare-ordinary-worker-operations.js';
import {
  emptyFleetInventoryRowCounts,
  FLEET_INVENTORY_STAGE_ORDER,
  FleetInventoryFindingValueError,
  type FleetInventoryRunOptions,
  type FleetInventoryRunProgress,
  type FleetInventoryStage,
  type FleetInventoryStagedFact,
  type FleetInventoryStagedRow,
  initialFleetInventoryStage,
} from '../src/fleet-inventory-state.js';
import { canonicalDeploymentEgressPolicy } from '../src/platform-resources.js';

interface KeyItem {
  readonly name?: string;
}

interface DispatchItem {
  readonly id: string;
  readonly tags: readonly string[];
}

interface World {
  readonly kvPages?: readonly (readonly KeyItem[])[];
  readonly kvValues?: Readonly<Record<string, string>>;
  readonly dispatchNamespace?: string;
  readonly dispatchPages?: readonly (readonly DispatchItem[])[];
  readonly dispatchStatuses?: readonly number[];
  readonly namespaceInventory?: FleetInventoryDispatchNamespace;
  readonly dispatchWorkers?: Readonly<
    Record<string, FleetInventoryDispatchWorker | 'missing' | 'error'>
  >;
  readonly domainPages?: readonly (readonly Readonly<{
    hostname: string;
    service: string;
  }>[])[];
  readonly zoneIds?: readonly string[];
  readonly zoneRoutePages?: Readonly<
    Record<
      string,
      readonly (readonly Readonly<{
        id?: string;
        pattern?: string;
        script?: string;
      }>[])[]
    >
  >;
  readonly scriptPages?: readonly (readonly Readonly<{ id?: string }>[])[];
  readonly scriptDetails?: Readonly<
    Record<string, FleetInventoryOrdinaryScriptDetail | 'error'>
  >;
  readonly databasePages?: readonly (readonly Readonly<{
    uuid?: string;
    name?: string;
  }>[])[];
  readonly namespacePages?: readonly (readonly Readonly<{
    id?: string;
    script?: string;
  }>[])[];
  readonly buckets?: Readonly<
    Record<string, readonly FleetInventoryBucketPage['buckets'][number][]>
  >;
}

const CAPABILITY_ERROR = new Error('dispatch namespace capability');
const INSPECT_FAILURE = 'inspect exploded with token sk-live-secret';
const DETAIL_FAILURE = 'detail exploded with token sk-live-secret';

function pageAt<T>(
  pages: readonly (readonly T[])[] | undefined,
  cursor: string | undefined,
): Readonly<{ items: readonly T[]; cursor?: string }> {
  const all = pages ?? [];
  const index = cursor === undefined ? 0 : Number(cursor.slice(1));
  const items = all[index] ?? [];
  const next = index + 1 < all.length ? `c${index + 1}` : undefined;
  return { items, ...(next === undefined ? {} : { cursor: next }) };
}

interface Harness {
  readonly deps: CloudflareFleetInventoryDeps;
  readonly calls: string[];
  readonly dispatchRequests: string[];
}

function harness(world: World): Harness {
  const calls: string[] = [];
  const dispatchRequests: string[] = [];
  let dispatchAttempt = 0;
  const deps: CloudflareFleetInventoryDeps = {
    attachmentScan: {
      accountId: 'account-1',
      client: undefined as unknown as CloudflareSdk,
      ...(world.dispatchNamespace === undefined
        ? {}
        : { dispatchNamespace: world.dispatchNamespace }),
      requestDispatchScriptPage: async ({ namespace, cursor, perPage }) => {
        dispatchRequests.push(`${namespace}|${cursor ?? ''}|${perPage}`);
        const status = world.dispatchStatuses?.[dispatchAttempt] ?? 200;
        dispatchAttempt += 1;
        if (status !== 200) {
          return new Response('{}', { status });
        }
        const page = pageAt(world.dispatchPages, cursor);
        return new Response(
          JSON.stringify({
            result: page.items,
            ...(page.cursor === undefined
              ? {}
              : { result_info: { cursor: page.cursor } }),
          }),
          { status: 200 },
        );
      },
    },
    dispatchNamespace: () => {
      if (world.dispatchNamespace === undefined) throw CAPABILITY_ERROR;
      return world.dispatchNamespace;
    },
    isDispatchCapabilityError: (error) => error === CAPABILITY_ERROR,
    listHostRoutingKeys: async ({ cursor }) => {
      calls.push(`kv-keys:${cursor ?? ''}`);
      const page = pageAt(world.kvPages, cursor);
      return {
        keys: page.items,
        ...(page.cursor === undefined ? {} : { cursor: page.cursor }),
      };
    },
    readHostRoutingValue: async ({ keyName }) => {
      calls.push(`kv-value:${keyName}`);
      return world.kvValues?.[keyName];
    },
    inspectDispatchWorker: async ({ scriptName }) => {
      calls.push(`inspect:${scriptName}`);
      const entry = world.dispatchWorkers?.[scriptName];
      if (entry === 'error') throw new Error(INSPECT_FAILURE);
      if (entry === undefined || entry === 'missing') return undefined;
      return entry;
    },
    getDispatchNamespace: async ({ namespace }) => {
      calls.push(`namespace:${namespace}`);
      return world.namespaceInventory ?? {};
    },
    listCustomDomains: async ({ cursor }) => {
      calls.push(`domains:${cursor ?? ''}`);
      const page = pageAt(world.domainPages, cursor);
      return {
        domains: page.items,
        ...(page.cursor === undefined ? {} : { cursor: page.cursor }),
      };
    },
    listWorkerRouteZoneIds: async () => {
      calls.push('zones');
      return world.zoneIds ?? [];
    },
    listZoneRoutes: async ({ zoneId, cursor }) => {
      calls.push(`zone-routes:${zoneId}:${cursor ?? ''}`);
      const page = pageAt(world.zoneRoutePages?.[zoneId], cursor);
      return {
        routes: page.items,
        ...(page.cursor === undefined ? {} : { cursor: page.cursor }),
      };
    },
    listOrdinaryScripts: async ({ cursor }) => {
      calls.push(`scripts:${cursor ?? ''}`);
      const page = pageAt(world.scriptPages, cursor);
      return {
        scripts: page.items,
        ...(page.cursor === undefined ? {} : { cursor: page.cursor }),
      };
    },
    readOrdinaryScriptDetail: async ({ scriptName }) => {
      calls.push(`detail:${scriptName}`);
      const detail = world.scriptDetails?.[scriptName];
      if (detail === undefined || detail === 'error') {
        throw new Error(DETAIL_FAILURE);
      }
      return detail;
    },
    listDatabases: async ({ cursor }) => {
      calls.push(`databases:${cursor ?? ''}`);
      const page = pageAt(world.databasePages, cursor);
      return {
        databases: page.items,
        ...(page.cursor === undefined ? {} : { cursor: page.cursor }),
      };
    },
    listDurableObjectNamespaces: async ({ cursor }) => {
      calls.push(`namespaces:${cursor ?? ''}`);
      const page = pageAt(world.namespacePages, cursor);
      return {
        namespaces: page.items,
        ...(page.cursor === undefined ? {} : { cursor: page.cursor }),
      };
    },
    listR2Buckets: async ({ jurisdiction, startAfter }) => {
      calls.push(`r2:${jurisdiction}:${startAfter ?? ''}`);
      const all = world.buckets?.[jurisdiction] ?? [];
      const start =
        startAfter === undefined
          ? 0
          : all.findIndex((bucket) => bucket.name === startAfter) + 1;
      return { buckets: all.slice(start, start + 1_000) };
    },
  };
  return { deps, calls, dispatchRequests };
}

function stageKey(stage: FleetInventoryStage): string {
  return JSON.stringify(
    Object.entries(stage).sort(([left], [right]) => (left < right ? -1 : 1)),
  );
}

function initialProgress(
  options: FleetInventoryRunOptions,
): FleetInventoryRunProgress {
  return {
    stage: initialFleetInventoryStage(options),
    generation: 1,
    revision: 0,
    stagedCounts: emptyFleetInventoryRowCounts(),
    factCount: 0,
    providerRequests: 0,
  };
}

interface DriveResult {
  readonly steps: readonly string[];
  readonly stages: readonly FleetInventoryStage[];
  readonly rows: readonly FleetInventoryStagedRow[];
  readonly facts: readonly FleetInventoryStagedFact[];
  readonly diagnostics: readonly string[];
  readonly providerRequests: number;
}

/**
 * Drives the engine exactly as the coordinator will: one chunk per call, and
 * `lastPageDigest` is retained only while the stage position is unchanged.
 */
async function drive(
  deps: CloudflareFleetInventoryDeps,
  options: FleetInventoryRunOptions,
  maxProviderRequests = 1_000,
): Promise<DriveResult> {
  let progress = initialProgress(options);
  const steps: string[] = [];
  const stages: FleetInventoryStage[] = [];
  const rows: FleetInventoryStagedRow[] = [];
  const facts: FleetInventoryStagedFact[] = [];
  const diagnostics: string[] = [];
  let providerRequests = 0;
  for (let index = 0; index < 200; index += 1) {
    const stage = progress.stage;
    steps.push(stage.step);
    stages.push(stage);
    const result = await advanceCloudflareFleetInventoryStage(deps, {
      stage,
      options,
      progress,
      maxProviderRequests,
    });
    rows.push(...result.rows);
    facts.push(...result.facts);
    diagnostics.push(...result.diagnostics);
    providerRequests += result.providerRequests;
    const stagedCounts = { ...progress.stagedCounts };
    for (const row of result.rows) stagedCounts[row.kind] += 1;
    const resumed = stageKey(result.nextStage) === stageKey(stage);
    progress = {
      stage: result.nextStage,
      generation: 1,
      revision: progress.revision + 1,
      stagedCounts,
      factCount: progress.factCount + result.facts.length,
      ...(resumed && result.pageDigest !== undefined
        ? { lastPageDigest: result.pageDigest }
        : {}),
      providerRequests,
    };
    if (stage.step === 'finalize') break;
  }
  return { steps, stages, rows, facts, diagnostics, providerRequests };
}

function findings(
  rows: readonly FleetInventoryStagedRow[],
): readonly Readonly<Record<string, unknown>>[] {
  return rows.filter((row) => row.kind === 'finding').map((row) => row.payload);
}

function details(rows: readonly FleetInventoryStagedRow[]): readonly string[] {
  return findings(rows).map((payload) => String(payload.detail));
}

const TENANT = 'tenant1';
const ENVIRONMENT = 'prod';
const POLICY = canonicalDeploymentEgressPolicy({
  policyId: 'policy-1',
  tenantTag: TENANT,
  environment: ENVIRONMENT,
  allowedHosts: ['api.example.com'],
});

function hostRouteValue(scriptName: string): string {
  return JSON.stringify({
    scriptName,
    tenantTag: TENANT,
    environment: ENVIRONMENT,
    policyId: 'policy-1',
    policyDigest: POLICY.policyDigest,
    policyHosts: POLICY.policyHosts,
  });
}

function registrationValue(
  scriptName: string,
  overrides: Readonly<Record<string, unknown>> = {},
): string {
  return JSON.stringify({
    scriptName,
    tenantTag: TENANT,
    environment: ENVIRONMENT,
    databaseId: 'db-1',
    routeHostname: 'app.example.com',
    ...overrides,
  });
}

function dispatchWorker(
  overrides: Partial<FleetInventoryDispatchWorker> = {},
): FleetInventoryDispatchWorker {
  return {
    artifactVersion: 'version-1',
    tenantTag: TENANT,
    environment: ENVIRONMENT,
    schemaVersion: 3,
    desiredSpecDigest: 'digest-1',
    databaseIds: ['db-1'],
    durableObjectBindings: [
      { name: 'DO', className: 'Runner', namespaceId: 'ns-a' },
    ],
    serviceBindings: [{ name: 'SVC', service: 'anchorage-plain' }],
    queueProducerBindings: [{ name: 'Q', queueName: 'queue-1' }],
    r2BucketBindings: [
      { name: 'R2', bucketName: 'anchorage-bucket', jurisdiction: 'default' },
    ],
    secretNames: ['SECRET_ONE'],
    plainTextBindings: { DEPLOYMENT_TENANT: TENANT },
    ...overrides,
  };
}

function ordinaryDetail(
  overrides: Partial<FleetInventoryOrdinaryScriptDetail> = {},
): FleetInventoryOrdinaryScriptDetail {
  return {
    artifactVersion: 'version-9',
    bindings: [
      { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: TENANT },
      { type: 'plain_text', name: 'FLEET_ENVIRONMENT', text: ENVIRONMENT },
      { type: 'plain_text', name: 'FLEET_SCHEMA_VERSION', text: '3' },
      { type: 'd1', name: 'DB', database_id: 'db-1' },
    ],
    subdomainEnabled: false,
    previewsEnabled: false,
    secretNames: ['PLAIN_SECRET'],
    ...overrides,
  };
}

const RICH_OPTIONS: FleetInventoryRunOptions = {
  hostRoutingKvId: 'kv-1',
  databaseNamePrefix: 'anchorage-db-',
  scriptNamePrefix: 'anchorage-',
  includeDispatchNamespace: true,
  includeR2Buckets: true,
};

const RICH_WORLD: World = {
  dispatchNamespace: 'anchorage-ns',
  kvPages: [
    [
      { name: '__anchorage_script__:anchorage-alpha' },
      { name: '__anchorage_script__:anchorage-beta' },
      { name: '__anchorage_script__:anchorage-bad' },
      { name: '__anchorage_script__:anchorage-delta' },
      { name: '__anchorage_script__:anchorage-epsilon' },
      { name: 'app.example.com' },
      { name: 'other.example.com' },
      { name: 'stale.example.com' },
    ],
  ],
  kvValues: {
    '__anchorage_script__:anchorage-alpha':
      registrationValue('anchorage-alpha'),
    '__anchorage_script__:anchorage-beta': registrationValue('anchorage-gamma'),
    '__anchorage_script__:anchorage-bad': 'not json',
    '__anchorage_script__:anchorage-delta':
      registrationValue('anchorage-delta'),
    '__anchorage_script__:anchorage-epsilon':
      registrationValue('anchorage-epsilon'),
    'app.example.com': hostRouteValue('anchorage-alpha'),
    'other.example.com': hostRouteValue('anchorage-alpha'),
    'stale.example.com': JSON.stringify({ scriptName: 'anchorage-alpha' }),
  },
  dispatchPages: [
    [
      {
        id: 'anchorage-alpha',
        tags: ['fleet:anchorage', `tenant:${TENANT}`, 'environment:prod'],
      },
      { id: 'anchorage-delta', tags: ['fleet:anchorage'] },
      {
        id: 'anchorage-epsilon',
        tags: ['fleet:anchorage', `tenant:${TENANT}`, 'environment:prod'],
      },
      { id: 'anchorage-orphan', tags: [] },
    ],
  ],
  namespaceInventory: {
    namespace_name: 'other-ns',
    namespace_id: 'ns-1',
    trusted_workers: false,
    script_count: 5,
  },
  dispatchWorkers: {
    'anchorage-alpha': dispatchWorker(),
    'anchorage-gamma': 'missing',
    'anchorage-delta': 'error',
    'anchorage-epsilon': dispatchWorker({ tenantTag: 'tenant2' }),
  },
  domainPages: [
    [
      { hostname: 'cd.example.com', service: 'anchorage-plain' },
      { hostname: 'orphan.example.com', service: 'anchorage-ghost' },
    ],
  ],
  zoneIds: ['zone-1'],
  zoneRoutePages: {
    'zone-1': [
      [
        {
          id: 'route-1',
          pattern: 'zone.example.com/*',
          script: 'anchorage-plain',
        },
      ],
    ],
  },
  scriptPages: [
    [{ id: 'anchorage-plain' }, { id: 'anchorage-ghost' }, { id: 'other' }],
  ],
  scriptDetails: {
    'anchorage-plain': ordinaryDetail({
      bindings: [
        { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: TENANT },
        { type: 'plain_text', name: 'FLEET_ENVIRONMENT', text: ENVIRONMENT },
        { type: 'plain_text', name: 'FLEET_SCHEMA_VERSION', text: '3' },
        {
          type: 'plain_text',
          name: 'FLEET_RESOURCE_ROLE',
          text: 'platform-state',
        },
        { type: 'plain_text', name: 'FLEET_RESOURCE_GROUP', text: 'group-1' },
        { type: 'plain_text', name: 'FLEET_SPEC_DIGEST', text: 'spec-1' },
        { type: 'd1', name: 'DB', database_id: 'db-1' },
        { type: 'kv_namespace', name: 'KV', namespace_id: 'kv-9' },
      ],
      previewsEnabled: true,
    }),
    'anchorage-ghost': 'error',
  },
  databasePages: [
    [
      { uuid: 'db-1', name: 'anchorage-db-alpha' },
      { uuid: 'db-2', name: 'unrelated' },
    ],
  ],
  namespacePages: [
    [
      { id: 'ns-a', script: 'anchorage-alpha' },
      { id: 'ns-b', script: 'unrelated' },
    ],
  ],
  buckets: {
    default: [
      { name: 'anchorage-bucket', creation_date: '2024-01-01T00:00:00.000Z' },
    ],
  },
};

const EMPTY_OPTIONS: FleetInventoryRunOptions = {
  databaseNamePrefix: 'anchorage-db-',
  scriptNamePrefix: 'anchorage-',
  includeDispatchNamespace: false,
  includeR2Buckets: false,
};

describe('advanceCloudflareFleetInventoryStage', () => {
  it('walks the fifteen provider stages in encounter order, one chunk per call', async () => {
    const { deps } = harness(RICH_WORLD);
    const run = await drive(deps, RICH_OPTIONS);
    expect(run.steps).toEqual([...FLEET_INVENTORY_STAGE_ORDER]);
    expect(
      run.rows
        .filter((row) => row.kind === 'route')
        .map((row) => row.payload.surface),
    ).toEqual([
      'host-registry',
      'host-registry',
      'custom-domain',
      'custom-domain',
      'zone-route',
    ]);
    expect(
      run.rows
        .filter((row) => row.kind === 'database-id')
        .map((row) => row.payload.databaseId),
    ).toEqual(['db-1']);
    expect(
      run.rows
        .filter((row) => row.kind === 'namespace-id')
        .map((row) => row.payload.namespaceId),
    ).toEqual(['ns-a']);
    expect(
      run.rows
        .filter((row) => row.kind === 'r2-bucket')
        .map((row) => row.payload.bucketName),
    ).toEqual(['anchorage-bucket']);
  });

  it('advances past a zero-row page for every host routing and dispatch stage', async () => {
    const steps = [
      'host-kv-keys',
      'host-kv-values',
      'dispatch-pages',
      'registration-checks',
      'registration-postprocess',
    ] as const;
    const world: World = {
      dispatchNamespace: 'anchorage-ns',
      kvPages: [[]],
      dispatchPages: [[]],
      namespaceInventory: {
        namespace_name: 'anchorage-ns',
        trusted_workers: false,
        script_count: 0,
      },
    };
    for (const step of steps) {
      const { deps } = harness(world);
      const stage = (
        step === 'host-kv-values'
          ? { step, keyOrdinal: 0 }
          : step === 'dispatch-pages'
            ? { step, pageOrdinal: 0 }
            : step === 'registration-checks'
              ? { step, registrationOrdinal: 0 }
              : { step }
      ) as FleetInventoryStage;
      const result = await advanceCloudflareFleetInventoryStage(deps, {
        stage,
        options: RICH_OPTIONS,
        progress: { ...initialProgress(RICH_OPTIONS), stage },
        maxProviderRequests: 1_000,
      });
      // The attestation record is the one row a zero-row registry still
      // stages, because the namespace itself was read.
      expect(result.rows.map((row) => row.kind)).toEqual(
        step === 'registration-postprocess' ? ['meta'] : [],
      );
      expect(result.facts).toEqual([]);
      expect(result.nextStage.step).not.toBe(step);
    }
  });

  it('advances past a zero-row page for every routing stage', async () => {
    const steps = [
      'custom-domains',
      'zone-authority',
      'zone-routes',
      'route-claims',
    ] as const;
    const world: World = { domainPages: [[]], zoneIds: [], scriptPages: [[]] };
    for (const step of steps) {
      const { deps } = harness(world);
      const stage = (
        step === 'zone-routes' ? { step, zoneOrdinal: 0 } : { step }
      ) as FleetInventoryStage;
      const result = await advanceCloudflareFleetInventoryStage(deps, {
        stage,
        options: EMPTY_OPTIONS,
        progress: { ...initialProgress(EMPTY_OPTIONS), stage },
        maxProviderRequests: 1_000,
      });
      expect(result.rows).toEqual([]);
      expect(result.facts).toEqual([]);
      expect(result.nextStage.step).not.toBe(step);
    }
  });

  it('advances past a zero-row page for every ordinary Worker stage', async () => {
    const steps = ['ordinary-scripts', 'ordinary-script-detail'] as const;
    const world: World = { scriptPages: [[]], domainPages: [[]], zoneIds: [] };
    for (const step of steps) {
      const { deps } = harness(world);
      const stage = (
        step === 'ordinary-script-detail'
          ? { step, scriptOrdinal: 0 }
          : { step }
      ) as FleetInventoryStage;
      const result = await advanceCloudflareFleetInventoryStage(deps, {
        stage,
        options: EMPTY_OPTIONS,
        progress: { ...initialProgress(EMPTY_OPTIONS), stage },
        maxProviderRequests: 1_000,
      });
      expect(result.rows).toEqual([]);
      expect(result.nextStage.step).not.toBe(step);
    }
  });

  it('advances past a zero-row page for every account resource stage', async () => {
    const steps = [
      'd1-databases',
      'do-namespaces',
      'r2-buckets',
      'finalize',
    ] as const;
    const world: World = {
      databasePages: [[]],
      namespacePages: [[]],
      buckets: {},
    };
    for (const step of steps) {
      const { deps } = harness(world);
      const stage = (
        step === 'r2-buckets' ? { step, jurisdictionOrdinal: 0 } : { step }
      ) as FleetInventoryStage;
      const result = await advanceCloudflareFleetInventoryStage(deps, {
        stage,
        options: { ...EMPTY_OPTIONS, includeR2Buckets: true },
        progress: { ...initialProgress(EMPTY_OPTIONS), stage },
        maxProviderRequests: 1_000,
      });
      expect(result.rows).toEqual([]);
      expect(result.nextStage.step).toBe(
        step === 'finalize'
          ? 'finalize'
          : FLEET_INVENTORY_STAGE_ORDER[
              FLEET_INVENTORY_STAGE_ORDER.indexOf(step) + 1
            ],
      );
    }
  });

  it('finalizes an empty generation for an account with no host routing KV', async () => {
    const { deps, calls } = harness({
      domainPages: [[]],
      zoneIds: [],
      scriptPages: [[]],
      databasePages: [[]],
      namespacePages: [[]],
    });
    const run = await drive(deps, EMPTY_OPTIONS);
    expect(run.steps).toEqual([
      'custom-domains',
      'zone-authority',
      'ordinary-scripts',
      'route-claims',
      'd1-databases',
      'do-namespaces',
      'finalize',
    ]);
    expect(run.rows).toEqual([]);
    expect(run.facts).toEqual([]);
    expect(calls).not.toContain('kv-keys:');
    expect(calls).not.toContain('r2:default:');
  });

  it('advances through a host routing KV namespace that is empty', async () => {
    const { deps } = harness({
      dispatchNamespace: 'anchorage-ns',
      kvPages: [[]],
      dispatchPages: [[]],
      namespaceInventory: {
        namespace_name: 'anchorage-ns',
        trusted_workers: false,
        script_count: 0,
      },
      domainPages: [[]],
      zoneIds: [],
      scriptPages: [[]],
      databasePages: [[]],
      namespacePages: [[]],
    });
    const run = await drive(deps, { ...RICH_OPTIONS, includeR2Buckets: false });
    expect(run.steps).toEqual([
      'host-kv-keys',
      'dispatch-pages',
      'registration-postprocess',
      'custom-domains',
      'zone-authority',
      // The attestation meta row enables the zone-route stage, which then
      // finds no zone to walk.
      'zone-routes',
      'ordinary-scripts',
      'route-claims',
      'd1-databases',
      'do-namespaces',
      'finalize',
    ]);
    expect(details(run.rows)).toEqual([]);
  });

  it('advances the offset when a resumed chunk re-reads the same page', async () => {
    const world: World = {
      kvPages: [
        [{ name: 'a.example.com' }, { name: 'b.example.com' }],
        [{ name: 'c.example.com' }],
      ],
    };
    const { deps } = harness(world);
    const stage: FleetInventoryStage = { step: 'host-kv-keys' };
    const progress = { ...initialProgress(RICH_OPTIONS), stage };
    const first = await advanceCloudflareFleetInventoryStage(deps, {
      stage,
      options: RICH_OPTIONS,
      progress,
      maxProviderRequests: 9,
    });
    expect(first.pageDigest).toMatch(/^[0-9a-f]{64}$/u);
    const replay = await advanceCloudflareFleetInventoryStage(deps, {
      stage,
      options: RICH_OPTIONS,
      progress: { ...progress, lastPageDigest: first.pageDigest },
      maxProviderRequests: 9,
    });
    expect(replay.pageDigest).toBe(first.pageDigest);
    expect(replay.nextStage).toEqual(first.nextStage);
    expect(replay.rows).toEqual(first.rows);
  });

  it('refuses a resumed chunk whose page digest drifted', async () => {
    const stage: FleetInventoryStage = { step: 'host-kv-keys' };
    const progress = { ...initialProgress(RICH_OPTIONS), stage };
    const first = await advanceCloudflareFleetInventoryStage(
      harness({ kvPages: [[{ name: 'a.example.com' }]] }).deps,
      { stage, options: RICH_OPTIONS, progress, maxProviderRequests: 9 },
    );
    const drifted = harness({
      kvPages: [[{ name: 'a.example.com' }, { name: 'b.example.com' }]],
    });
    await expect(
      advanceCloudflareFleetInventoryStage(drifted.deps, {
        stage,
        options: RICH_OPTIONS,
        progress: { ...progress, lastPageDigest: first.pageDigest },
        maxProviderRequests: 9,
      }),
    ).rejects.toThrow(CloudflareFleetInventoryCursorDriftError);
    await expect(
      advanceCloudflareFleetInventoryStage(drifted.deps, {
        stage,
        options: RICH_OPTIONS,
        progress: { ...progress, lastPageDigest: first.pageDigest },
        maxProviderRequests: 9,
      }),
    ).rejects.toThrow(
      "fleet inventory stage 'host-kv-keys' page changed between bounded chunks",
    );
  });

  it('refuses a provider listing that repeats its cursor', async () => {
    const deps = harness({ kvPages: [[{ name: 'a.example.com' }]] }).deps;
    const repeating: CloudflareFleetInventoryDeps = {
      ...deps,
      listHostRoutingKeys: async () => ({
        keys: [{ name: 'a.example.com' }],
        cursor: 'stuck',
      }),
    };
    const stage: FleetInventoryStage = {
      step: 'host-kv-keys',
      cursor: 'stuck',
    };
    await expect(
      advanceCloudflareFleetInventoryStage(repeating, {
        stage,
        options: RICH_OPTIONS,
        progress: { ...initialProgress(RICH_OPTIONS), stage },
        maxProviderRequests: 9,
      }),
    ).rejects.toThrow(CloudflareFleetInventoryCursorError);
  });

  it('stops at the provider request budget and persists the exact offset', async () => {
    const keys = Array.from({ length: 40 }, (_, index) => ({
      name: `host-${index}.example.com`,
    }));
    const { deps, calls } = harness({
      kvPages: [keys],
      kvValues: Object.fromEntries(
        keys.map((key) => [String(key.name), 'not json']),
      ),
    });
    const stage: FleetInventoryStage = {
      step: 'host-kv-values',
      keyOrdinal: 0,
    };
    const result = await advanceCloudflareFleetInventoryStage(deps, {
      stage,
      options: RICH_OPTIONS,
      progress: { ...initialProgress(RICH_OPTIONS), stage },
      maxProviderRequests: 9,
    });
    // One page listing plus eight value reads exhausts the budget of nine.
    expect(result.providerRequests).toBe(9);
    expect(result.nextStage).toEqual({ step: 'host-kv-values', keyOrdinal: 8 });
    expect(result.rows).toHaveLength(8);
    expect(calls.filter((call) => call.startsWith('kv-value:'))).toHaveLength(
      8,
    );
    await expect(
      advanceCloudflareFleetInventoryStage(deps, {
        stage,
        options: RICH_OPTIONS,
        progress: { ...initialProgress(RICH_OPTIONS), stage },
        maxProviderRequests: 9,
      }),
    ).resolves.toMatchObject({ providerRequests: 9 });
    // A chunk that cannot reach its own offset fails closed instead of
    // returning zero progress forever.
    const paged = harness({
      kvPages: keys.map((key) => [key]),
      kvValues: Object.fromEntries(
        keys.map((key) => [String(key.name), 'not json']),
      ),
    });
    await expect(
      advanceCloudflareFleetInventoryStage(paged.deps, {
        stage,
        options: RICH_OPTIONS,
        progress: { ...initialProgress(RICH_OPTIONS), stage },
        maxProviderRequests: 9,
      }),
    ).rejects.toThrow(CloudflareFleetInventoryBudgetError);
  });

  it('accepts the ten-thousandth host routing key and refuses the next one', async () => {
    const keys = (count: number) =>
      Array.from({ length: count }, (_, index) => ({ name: `key-${index}` }));
    const stage: FleetInventoryStage = { step: 'host-kv-keys' };
    const accepted = await advanceCloudflareFleetInventoryStage(
      harness({ kvPages: [keys(10_000)] }).deps,
      {
        stage,
        options: RICH_OPTIONS,
        progress: { ...initialProgress(RICH_OPTIONS), stage },
        maxProviderRequests: 9,
      },
    );
    expect(accepted.rows).toHaveLength(10_000);
    await expect(
      advanceCloudflareFleetInventoryStage(
        harness({ kvPages: [keys(10_001)] }).deps,
        {
          stage,
          options: RICH_OPTIONS,
          progress: { ...initialProgress(RICH_OPTIONS), stage },
          maxProviderRequests: 9,
        },
      ),
    ).rejects.toThrow(
      'host-routing KV key inventory exceeded the supported inventory bound of 10000 items',
    );
  });

  it('accepts the twenty-five-thousandth D1 database and refuses the next one', async () => {
    const databases = (count: number) =>
      Array.from({ length: count }, (_, index) => ({
        uuid: `db-${index}`,
        name: `anchorage-db-${index}`,
      }));
    const stage: FleetInventoryStage = { step: 'd1-databases' };
    const accepted = await advanceCloudflareFleetInventoryStage(
      harness({ databasePages: [databases(25_000)] }).deps,
      {
        stage,
        options: EMPTY_OPTIONS,
        progress: { ...initialProgress(EMPTY_OPTIONS), stage },
        maxProviderRequests: 9,
      },
    );
    expect(accepted.rows).toHaveLength(25_000);
    await expect(
      advanceCloudflareFleetInventoryStage(
        harness({ databasePages: [databases(25_001)] }).deps,
        {
          stage,
          options: EMPTY_OPTIONS,
          progress: { ...initialProgress(EMPTY_OPTIONS), stage },
          maxProviderRequests: 9,
        },
      ),
    ).rejects.toThrow(
      'D1 database inventory exceeded the supported inventory bound of 25000 items',
    );
  });

  it('resumes R2 pagination inside one jurisdiction', async () => {
    const buckets = Array.from({ length: 9_001 }, (_, index) => ({
      name: `anchorage-${String(index).padStart(4, '0')}`,
      creation_date: '2024-01-01T00:00:00.000Z',
    }));
    const { deps, calls } = harness({ buckets: { default: buckets } });
    const stage: FleetInventoryStage = {
      step: 'r2-buckets',
      jurisdictionOrdinal: 0,
    };
    const options = { ...EMPTY_OPTIONS, includeR2Buckets: true };
    const first = await advanceCloudflareFleetInventoryStage(deps, {
      stage,
      options,
      progress: { ...initialProgress(options), stage },
      maxProviderRequests: 9,
    });
    expect(first.rows).toHaveLength(9_000);
    const resumed = first.nextStage as Readonly<{
      step: 'r2-buckets';
      jurisdictionOrdinal: 0 | 1 | 2;
      startAfter?: string;
    }>;
    expect(resumed).toEqual({
      step: 'r2-buckets',
      jurisdictionOrdinal: 0,
      startAfter: 'anchorage-8999',
    });
    const stagedCounts = emptyFleetInventoryRowCounts();
    const second = await advanceCloudflareFleetInventoryStage(deps, {
      stage: resumed,
      options,
      progress: {
        ...initialProgress(options),
        stage: resumed,
        stagedCounts: { ...stagedCounts, 'r2-bucket': 9_000 },
      },
      maxProviderRequests: 9,
    });
    expect(second.rows).toHaveLength(1);
    expect(second.rows[0]?.ordinal).toBe(9_000);
    expect(second.nextStage).toEqual({ step: 'finalize' });
    expect(calls).toContain('r2:default:anchorage-8999');
    expect(calls).toContain('r2:eu:');
    expect(calls).toContain('r2:fedramp:');
  });

  it('lists dispatch pages through listDispatchScriptPage, including its retry', async () => {
    const { deps, dispatchRequests } = harness({
      dispatchNamespace: 'anchorage-ns',
      dispatchStatuses: [429, 200],
      dispatchPages: [[{ id: 'anchorage-alpha', tags: ['fleet:anchorage'] }]],
    });
    const stage: FleetInventoryStage = {
      step: 'dispatch-pages',
      pageOrdinal: 0,
    };
    const result = await advanceCloudflareFleetInventoryStage(deps, {
      stage,
      options: RICH_OPTIONS,
      progress: { ...initialProgress(RICH_OPTIONS), stage },
      maxProviderRequests: 9,
    });
    expect(dispatchRequests).toEqual([
      'anchorage-ns||1000',
      'anchorage-ns||1000',
    ]);
    expect(result.providerRequests).toBe(1);
    expect(result.rows).toEqual([
      {
        kind: 'dispatch-script',
        ordinal: 0,
        payload: {
          record: 'dispatch-script',
          scriptId: 'anchorage-alpha',
          tags: ['fleet:anchorage'],
        },
      },
    ]);
  });

  it('stages fixed templates for both provider-error findings without provider text', async () => {
    const { deps } = harness(RICH_WORLD);
    const run = await drive(deps, RICH_OPTIONS);
    expect(details(run.rows)).toContain(
      "registered script 'anchorage-delta' could not be inspected",
    );
    expect(details(run.rows)).toContain(
      "plain Worker 'anchorage-ghost' could not be inventoried",
    );
    for (const detail of details(run.rows)) {
      expect(detail).not.toContain(INSPECT_FAILURE);
      expect(detail).not.toContain(DETAIL_FAILURE);
      expect(detail).not.toContain('Error');
    }
    expect(run.diagnostics.join('\n')).toContain(INSPECT_FAILURE);
    expect(run.diagnostics.join('\n')).toContain(DETAIL_FAILURE);
  });

  it("stages every other finding detail with today's exact bytes", async () => {
    const { deps } = harness(RICH_WORLD);
    const run = await drive(deps, RICH_OPTIONS);
    expect(
      findings(run.rows).map((payload) => [payload.kind, payload.detail]),
    ).toEqual([
      [
        'stale-script-registration',
        "script inventory key '__anchorage_script__:anchorage-beta' claims 'anchorage-gamma'",
      ],
      [
        'malformed-script-registration',
        "fleet inventory key '__anchorage_script__:anchorage-bad' is not valid JSON",
      ],
      [
        'malformed-route',
        "host route 'stale.example.com' has incomplete ownership metadata",
      ],
      [
        'stale-script-registration',
        "registered script 'anchorage-gamma' is absent from the dispatch namespace listing",
      ],
      [
        'stale-script-registration',
        "registered script 'anchorage-gamma' is missing",
      ],
      [
        'stale-script-registration',
        "registered script 'anchorage-delta' does not match its live fleet tags",
      ],
      [
        'stale-script-registration',
        "registered script 'anchorage-delta' could not be inspected",
      ],
      [
        'stale-script-registration',
        "registered script 'anchorage-epsilon' does not match its live tenant, environment, or database ownership",
      ],
      [
        'unknown-dispatch-scripts',
        "dispatch script 'anchorage-orphan' has no valid owner-checked registry entry",
      ],
      [
        'stale-route',
        "host route 'other.example.com' does not match its script registration owner",
      ],
      [
        'trusted-dispatch-namespace',
        "dispatch namespace 'anchorage-ns' does not attest trusted_workers=false",
      ],
      [
        'unknown-dispatch-scripts',
        "dispatch namespace 'anchorage-ns' reports 1 script(s) missing from the paginated listing",
      ],
      [
        'incomplete-deployment',
        "trusted Worker 'anchorage-plain' is publicly reachable on workers.dev, a preview URL, or a zone route",
      ],
      [
        'incomplete-deployment',
        "plain Worker 'anchorage-ghost' could not be inventoried",
      ],
      [
        'stale-route',
        "custom domain 'orphan.example.com' points to a missing or incomplete plain Worker 'anchorage-ghost'",
      ],
      [
        'stale-route',
        "zone route 'zone.example.com/*' exposes plain Worker 'anchorage-plain'",
      ],
    ]);
  });

  it('keeps provider diagnostics out of staged rows and facts', async () => {
    const { deps } = harness(RICH_WORLD);
    const run = await drive(deps, RICH_OPTIONS);
    const durable = JSON.stringify([run.rows, run.facts]);
    expect(run.diagnostics.length).toBeGreaterThan(0);
    for (const diagnostic of run.diagnostics) {
      expect(durable).not.toContain(diagnostic);
    }
    expect(durable).not.toContain('sk-live-secret');
  });

  it('records cross-stage drift as an incomplete-deployment finding', async () => {
    let listed = true;
    const base = harness({
      scriptPages: [[{ id: 'anchorage-vanished' }]],
      domainPages: [[]],
      zoneIds: [],
    });
    const deps: CloudflareFleetInventoryDeps = {
      ...base.deps,
      readOrdinaryScriptDetail: async () => {
        listed = false;
        throw new Error('404 script not found');
      },
    };
    const stage: FleetInventoryStage = {
      step: 'ordinary-script-detail',
      scriptOrdinal: 0,
    };
    const result = await advanceCloudflareFleetInventoryStage(deps, {
      stage,
      options: EMPTY_OPTIONS,
      progress: { ...initialProgress(EMPTY_OPTIONS), stage },
      maxProviderRequests: 9,
    });
    expect(listed).toBe(false);
    expect(details(result.rows)).toEqual([
      "plain Worker 'anchorage-vanished' could not be inventoried",
    ]);
    expect(result.nextStage.step).toBe('route-claims');
  });

  it('validates every interpolated finding value and falls back to the key ordinal', async () => {
    const hostile = 'bad\u0007name';
    const { deps } = harness({
      kvPages: [[{ name: 'a.example.com' }, { name: hostile }]],
      kvValues: { 'a.example.com': 'not json', [hostile]: 'not json' },
    });
    const stage: FleetInventoryStage = {
      step: 'host-kv-values',
      keyOrdinal: 0,
    };
    const result = await advanceCloudflareFleetInventoryStage(deps, {
      stage,
      options: RICH_OPTIONS,
      progress: { ...initialProgress(RICH_OPTIONS), stage },
      maxProviderRequests: 9,
    });
    expect(details(result.rows)).toEqual([
      "fleet inventory key 'a.example.com' is not valid JSON",
      'script inventory key at ordinal 1 has an unsafe name',
    ]);
    const credentialed = harness({
      kvPages: [[{ name: 'authorization-key.example.com' }]],
      kvValues: { 'authorization-key.example.com': 'not json' },
    });
    await expect(
      advanceCloudflareFleetInventoryStage(credentialed.deps, {
        stage,
        options: RICH_OPTIONS,
        progress: { ...initialProgress(RICH_OPTIONS), stage },
        maxProviderRequests: 9,
      }),
    ).rejects.toThrow(FleetInventoryFindingValueError);
  });

  it('stages a deployment with zero facts', async () => {
    const { deps } = harness({
      scriptPages: [[{ id: 'anchorage-bare' }]],
      domainPages: [[]],
      zoneIds: [],
      scriptDetails: {
        'anchorage-bare': {
          artifactVersion: 'version-1',
          bindings: [
            { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: TENANT },
            {
              type: 'plain_text',
              name: 'FLEET_ENVIRONMENT',
              text: ENVIRONMENT,
            },
            { type: 'plain_text', name: 'FLEET_SCHEMA_VERSION', text: '1' },
          ],
          subdomainEnabled: false,
          previewsEnabled: false,
          secretNames: [],
        },
      },
    });
    const stage: FleetInventoryStage = {
      step: 'ordinary-script-detail',
      scriptOrdinal: 0,
    };
    const result = await advanceCloudflareFleetInventoryStage(deps, {
      stage,
      options: EMPTY_OPTIONS,
      progress: { ...initialProgress(EMPTY_OPTIONS), stage },
      maxProviderRequests: 9,
    });
    const deployments = result.rows.filter((row) => row.kind === 'deployment');
    expect(deployments).toHaveLength(1);
    expect(
      result.facts.filter((fact) => fact.factKind !== 'plain-text-binding'),
    ).toEqual([]);
    expect(result.nextStage.step).toBe('route-claims');
  });

  it('keeps fact ordinals byte-stable across a replayed chunk', async () => {
    const { deps } = harness(RICH_WORLD);
    const stage: FleetInventoryStage = {
      step: 'registration-checks',
      registrationOrdinal: 0,
    };
    const progress = { ...initialProgress(RICH_OPTIONS), stage };
    const first = await advanceCloudflareFleetInventoryStage(deps, {
      stage,
      options: RICH_OPTIONS,
      progress,
      maxProviderRequests: 1_000,
    });
    const replay = await advanceCloudflareFleetInventoryStage(deps, {
      stage,
      options: RICH_OPTIONS,
      progress: { ...progress, lastPageDigest: first.pageDigest },
      maxProviderRequests: 1_000,
    });
    expect(replay.facts).toEqual(first.facts);
    expect(replay.rows).toEqual(first.rows);
    expect(first.facts.length).toBeGreaterThan(0);
    expect(
      first.facts.map(
        (fact) =>
          `${fact.deploymentOrdinal}:${fact.factKind}:${fact.factOrdinal}`,
      ),
    ).toEqual(
      replay.facts.map(
        (fact) =>
          `${fact.deploymentOrdinal}:${fact.factKind}:${fact.factOrdinal}`,
      ),
    );
  });

  it('refuses a provider request budget outside nine to one thousand', async () => {
    const { deps } = harness({ kvPages: [[]] });
    const stage: FleetInventoryStage = { step: 'host-kv-keys' };
    const call = (maxProviderRequests: number) =>
      advanceCloudflareFleetInventoryStage(deps, {
        stage,
        options: RICH_OPTIONS,
        progress: { ...initialProgress(RICH_OPTIONS), stage },
        maxProviderRequests,
      });
    await expect(call(8)).rejects.toThrow(
      'maxProviderRequests must be an integer from 9 to 1000',
    );
    await expect(call(1_001)).rejects.toThrow(
      'maxProviderRequests must be an integer from 9 to 1000',
    );
    await expect(call(9)).resolves.toMatchObject({ providerRequests: 1 });
    await expect(call(1_000)).resolves.toMatchObject({ providerRequests: 1 });
  });
});
