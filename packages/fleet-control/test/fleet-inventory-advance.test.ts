// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  CloudflareProvisioningClient,
  cloudflareFleetInventoryContext,
} from '../src/cloudflare-client.js';
import {
  type AdvanceFleetInventoryOptions,
  advanceFleetInventory,
  FleetInventoryAdvanceCapabilityError,
  type FleetInventoryAdvanceResult,
  readFleetInventoryGeneration,
} from '../src/fleet-inventory-advance.js';
import {
  type CollectFleetInventoryOptions,
  canonicalFleetInventoryRunOptions,
  emptyFleetInventoryRowCounts,
  type FleetInventoryGeneration,
  type FleetInventoryGenerationRef,
  type FleetInventoryLease,
  type FleetInventoryProviderContext,
  type FleetInventoryRowKind,
  type FleetInventoryRunOptions,
  type FleetInventoryRunRecord,
  type FleetInventoryRunStore,
  FleetInventoryRunTokenError,
  FleetInventoryRunTokenFutureError,
  FleetInventoryRunTokenOperationError,
  type FleetInventoryStage,
  type FleetInventoryStagedFact,
  type FleetInventoryStagedRow,
  type FleetInventoryStageInput,
  fleetInventoryOptionsDigest,
  initialFleetInventoryStage,
} from '../src/fleet-inventory-state.js';
import { canonicalDeploymentEgressPolicy } from '../src/platform-resources.js';
import type { FleetResourceInventory } from '../src/types.js';
import {
  type CloudflareFixtureHandler,
  pageArray,
  recordingFetch,
  restProjection,
  single,
  testRateCoordinator,
} from './fixtures/cloudflare-fetch-fixture.js';
import { DRAIN_BASELINE_INVENTORY } from './fixtures/fleet-inventory-drain-baseline.js';
import {
  DRAIN_ACCOUNT_ID,
  DRAIN_DISPATCH_NAMESPACE,
  DRAIN_INVENTORY_OPTIONS,
  fleetInventoryDrainHandler,
  fleetInventoryDrainWorld,
} from './fixtures/fleet-inventory-drain-world.js';
import {
  type ProviderWorld,
  providerWorld,
} from './fixtures/provider-world.js';

const OPERATION_ID = '123e4567-e89b-42d3-a456-426614174000';
const FOREIGN_OPERATION_ID = '123e4567-e89b-42d3-a456-4266141740ff';
const MAX_PROVIDER_REQUESTS = 1_000;

const STUB_OPTIONS: CollectFleetInventoryOptions = {
  hostRoutingKvId: 'hosts',
  databaseNamePrefix: 'fleet-',
  scriptNamePrefix: 'fleet-',
};

const INSPECT_SUFFIX = ' could not be inspected';
const INVENTORY_SUFFIX = ' could not be inventoried';

/**
 * The two provider-error details the durable engine sanitizes. The drain
 * composes the transient text back from its call-local diagnostics, so the
 * expected materialization is derived from the drain rather than hard-coded.
 */
function sanitizedInventory(
  inventory: FleetResourceInventory,
): FleetResourceInventory {
  return {
    ...inventory,
    findings: inventory.findings.map((finding) => {
      for (const suffix of [INSPECT_SUFFIX, INVENTORY_SUFFIX]) {
        const at = finding.detail.indexOf(`${suffix}: `);
        if (at >= 0) {
          return {
            ...finding,
            detail: finding.detail.slice(0, at + suffix.length),
          };
        }
      }
      return finding;
    }),
  };
}

class FakeInventoryRunStore implements FleetInventoryRunStore {
  readonly runs = new Map<string, FleetInventoryRunRecord>();
  readonly rows = new Map<number, FleetInventoryStagedRow[]>();
  readonly facts = new Map<number, FleetInventoryStagedFact[]>();
  readonly refs = new Map<number, FleetInventoryGenerationRef>();
  readonly hiddenFromLease = new Set<string>();
  readonly pins: number[] = [];
  activeOperationId: string | undefined;
  latestGeneration: number | undefined;
  nextGeneration = 1;
  leaseLost = false;
  leases = 0;

  async withAccountInventoryLease<T>(
    operation: (lease: FleetInventoryLease) => Promise<T>,
  ): Promise<T> {
    this.leases += 1;
    return operation(this.#lease());
  }

  async readFinalizedGeneration(
    generation: number,
  ): Promise<FleetInventoryGeneration> {
    const ref = this.refs.get(generation);
    if (!ref) {
      throw new Error(
        `fleet inventory generation ${generation} is not finalized`,
      );
    }
    const rows = this.rows.get(generation) ?? [];
    const facts = this.facts.get(generation) ?? [];
    for (const kind of Object.keys(
      ref.rowManifest,
    ) as FleetInventoryRowKind[]) {
      const stored = rows.filter((row) => row.kind === kind).length;
      if (stored !== ref.rowManifest[kind]) {
        throw new Error(
          `fleet inventory generation ${generation} does not match its manifest`,
        );
      }
    }
    return { ref, rows, facts };
  }

  async latestFinalizedGeneration(): Promise<
    FleetInventoryGenerationRef | undefined
  > {
    return this.latestGeneration === undefined
      ? undefined
      : this.refs.get(this.latestGeneration);
  }

  async readRunByOperation(
    operationId: string,
  ): Promise<FleetInventoryRunRecord | undefined> {
    return this.runs.get(operationId);
  }

  async pinGeneration(input: Readonly<{ generation: number }>): Promise<void> {
    this.pins.push(input.generation);
  }

  async releasePin(input: Readonly<{ generation: number }>): Promise<void> {
    this.pins.splice(this.pins.indexOf(input.generation), 1);
  }

  async pruneInventoryGenerations(): Promise<Readonly<{ deleted: number }>> {
    return { deleted: 0 };
  }

  #lease(): FleetInventoryLease {
    const store = this;
    return {
      async assertOwned() {
        if (store.leaseLost) {
          throw new Error('fleet inventory account lease was lost');
        }
      },
      async startRun(input) {
        const existing = store.runs.get(input.operationId);
        if (existing) {
          if (existing.optionsDigest !== input.optionsDigest) {
            throw new Error(
              `fleet inventory run '${input.operationId}' has a different options digest`,
            );
          }
          return existing;
        }
        if (
          store.activeOperationId !== undefined &&
          store.activeOperationId !== input.operationId
        ) {
          throw new Error(
            'another fleet inventory operation owns this account head',
          );
        }
        const generation = store.nextGeneration;
        store.nextGeneration += 1;
        store.activeOperationId = input.operationId;
        const run: FleetInventoryRunRecord = {
          version: 1,
          operationId: input.operationId,
          optionsDigest: input.optionsDigest,
          options: input.options,
          state: 'staging',
          progress: {
            stage: initialFleetInventoryStage(input.options),
            generation,
            revision: 0,
            stagedCounts: emptyFleetInventoryRowCounts(),
            factCount: 0,
            providerRequests: 0,
          },
          updatedAt: new Date(0).toISOString(),
        };
        store.runs.set(input.operationId, run);
        return run;
      },
      async readRun(operationId) {
        return store.hiddenFromLease.has(operationId)
          ? undefined
          : store.runs.get(operationId);
      },
      async commitChunk(input) {
        const run = store.runs.get(input.operationId);
        if (!run || run.progress.revision !== input.expectedRevision) {
          throw new Error('fleet inventory chunk lost its revision guard');
        }
        const generation = run.progress.generation;
        store.rows.set(generation, [
          ...(store.rows.get(generation) ?? []),
          ...input.rows,
        ]);
        store.facts.set(generation, [
          ...(store.facts.get(generation) ?? []),
          ...input.facts,
        ]);
        store.runs.set(input.operationId, input.runRecord);
        return input.runRecord;
      },
      async finalizeRun(input) {
        const run = store.runs.get(input.operationId);
        if (!run || run.progress.revision !== input.expectedRevision) {
          throw new Error('fleet inventory finalize lost its revision guard');
        }
        const ref: FleetInventoryGenerationRef = {
          generation: run.progress.generation,
          operationId: run.operationId,
          finalizedAtMs: 1_700_000_000_000,
          rowManifest: input.manifest,
          factCount: input.factCount,
        };
        store.runs.set(input.operationId, { ...run, state: 'finalized' });
        store.refs.set(ref.generation, ref);
        store.latestGeneration = ref.generation;
        store.activeOperationId = undefined;
        return ref;
      },
      async failRun(input) {
        const run = store.runs.get(input.operationId);
        if (run) store.runs.set(input.operationId, { ...run, state: 'failed' });
        store.activeOperationId = undefined;
      },
      pinGeneration: (input) => store.pinGeneration(input),
      releasePin: (input) => store.releasePin(input),
      pruneInventoryGenerations: () => store.pruneInventoryGenerations(),
    };
  }
}

/** A context that stages nothing and finalizes on its first chunk. */
function stubContext(): FleetInventoryProviderContext & {
  readonly inputs: FleetInventoryStageInput[];
} {
  const inputs: FleetInventoryStageInput[] = [];
  return {
    inputs,
    async advanceStage(input) {
      inputs.push(input);
      return {
        rows: [],
        facts: [],
        nextStage: { step: 'finalize' },
        providerRequests: 0,
        diagnostics: [],
      };
    },
  };
}

function storeWithout(
  store: FleetInventoryRunStore,
  member: keyof FleetInventoryRunStore,
): FleetInventoryRunStore {
  const members: (keyof FleetInventoryRunStore)[] = [
    'withAccountInventoryLease',
    'readFinalizedGeneration',
    'latestFinalizedGeneration',
    'readRunByOperation',
    'pinGeneration',
    'releasePin',
    'pruneInventoryGenerations',
  ];
  const partial: Record<string, unknown> = {};
  for (const name of members) {
    if (name === member) continue;
    partial[name] = (...input: unknown[]) =>
      (store[name] as (...args: unknown[]) => unknown)(...input);
  }
  return partial as unknown as FleetInventoryRunStore;
}

async function runToCompletion(
  options: Omit<AdvanceFleetInventoryOptions, 'action'> &
    Readonly<{
      operationId?: string;
      runOptions?: CollectFleetInventoryOptions;
    }>,
): Promise<FleetInventoryAdvanceResult> {
  const operationId = options.operationId ?? OPERATION_ID;
  let result = await advanceFleetInventory({
    ...options,
    action: {
      kind: 'start',
      operationId,
      options: options.runOptions ?? STUB_OPTIONS,
    },
  });
  while (result.status === 'pending') {
    result = await advanceFleetInventory({
      ...options,
      action: { kind: 'continue', token: result.token },
    });
  }
  return result;
}

async function drainWithClient(
  handler: CloudflareFixtureHandler,
  accountId: string,
  dispatchNamespace: string,
  options: CollectFleetInventoryOptions,
): Promise<FleetResourceInventory> {
  const client = new CloudflareProvisioningClient({
    accountId,
    apiToken: 'token',
    rateCoordinator: testRateCoordinator(),
    dispatchNamespace,
    fetch: recordingFetch(handler).fetch,
  });
  return client.collectFleetInventory(options);
}

async function boundedWithClient(
  handler: CloudflareFixtureHandler,
  accountId: string,
  dispatchNamespace: string,
  options: CollectFleetInventoryOptions,
  maxProviderRequests = MAX_PROVIDER_REQUESTS,
): Promise<
  Readonly<{
    inventory: FleetResourceInventory;
    store: FakeInventoryRunStore;
    chunks: number;
    executed: readonly FleetInventoryStage[];
  }>
> {
  const client = new CloudflareProvisioningClient({
    accountId,
    apiToken: 'token',
    rateCoordinator: testRateCoordinator(),
    dispatchNamespace,
    fetch: recordingFetch(handler).fetch,
  });
  const store = new FakeInventoryRunStore();
  const context = cloudflareFleetInventoryContext(client);
  const executed: FleetInventoryStage[] = [
    initialFleetInventoryStage(canonicalFleetInventoryRunOptions(options)),
  ];
  let chunks = 0;
  let result = await advanceFleetInventory({
    context,
    store,
    action: { kind: 'start', operationId: OPERATION_ID, options },
    maxProviderRequests,
    maxStagedRowsPerChunk: 2_000,
  });
  while (result.status === 'pending') {
    chunks += 1;
    const persisted = store.runs.get(OPERATION_ID);
    if (persisted) executed.push(persisted.progress.stage);
    result = await advanceFleetInventory({
      context,
      store,
      action: { kind: 'continue', token: result.token },
      maxProviderRequests,
      maxStagedRowsPerChunk: 2_000,
    });
  }
  return {
    inventory: await readFleetInventoryGeneration(
      store,
      result.generation.generation,
    ),
    store,
    chunks,
    executed,
  };
}

// ---------------------------------------------------------------------------
// A SECOND, independent provider world. It is deliberately NOT the golden
// world and records no baseline: it drives an equivalence derived at test time
// so the drain rewrite must generalize instead of memorizing one recording.
// ---------------------------------------------------------------------------

const SECOND_ACCOUNT_ID = 'account';
const SECOND_DISPATCH_NAMESPACE = 'edge';
const SECOND_HOST_ROUTING_KV_ID = 'edge-hosts';
const SECOND_SPEC_DIGEST = 'c'.repeat(64);

const SECOND_INVENTORY_OPTIONS: CollectFleetInventoryOptions = {
  hostRoutingKvId: SECOND_HOST_ROUTING_KV_ID,
  databaseNamePrefix: 'edge-',
  scriptNamePrefix: 'edge-',
  includeDispatchNamespace: true,
  // R2 is deliberately excluded here, unlike the golden world.
  includeR2Buckets: false,
};

const SECOND_POLICY = canonicalDeploymentEgressPolicy({
  policyId: 'policy-edge',
  tenantTag: 'omega',
  environment: 'staging',
  allowedHosts: ['api.edge.test'],
});

const SECOND_KV_ENTRIES: readonly (readonly [string, string])[] = [
  [
    '__anchorage_script__:edge-one',
    JSON.stringify({
      scriptName: 'edge-one',
      tenantTag: 'omega',
      environment: 'staging',
      databaseId: 'db-edge-one',
      routeHostname: 'one.edge.test',
    }),
  ],
  [
    '__anchorage_script__:edge-broken',
    JSON.stringify({
      scriptName: 'edge-broken',
      tenantTag: 'omega',
      environment: 'staging',
      databaseId: 'db-edge-broken',
      routeHostname: 'broken.edge.test',
    }),
  ],
  [
    'one.edge.test',
    JSON.stringify({
      scriptName: 'edge-one',
      tenantTag: 'omega',
      environment: 'staging',
      ...SECOND_POLICY,
    }),
  ],
  ['unparsable.edge.test', 'not json'],
  ['second-unparsable.edge.test', 'not json'],
  // A host route whose ownership metadata is incomplete. These two extra keys
  // exist so the host-routing re-read costs enough that `registration-checks`
  // cannot finish both inspections inside the minimum provider budget.
  [
    'missing-owner.edge.test',
    JSON.stringify({ scriptName: 'edge-one', tenantTag: 'omega' }),
  ],
];

const SECOND_DISPATCH_LISTING: readonly Readonly<{
  id: string;
  tags: readonly string[];
}>[] = [
  {
    id: 'edge-one',
    tags: ['fleet:anchorage', 'tenant:omega', 'environment:staging'],
  },
  {
    id: 'edge-broken',
    tags: ['fleet:anchorage', 'tenant:omega', 'environment:staging'],
  },
];

const SECOND_DISPATCH_SETTINGS: Readonly<
  Record<
    string,
    Readonly<{ bindings: readonly unknown[]; tags: readonly string[] }>
  >
> = {
  'edge-one': {
    bindings: [
      { type: 'd1', name: 'DB', database_id: 'db-edge-one' },
      { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: 'omega' },
      { type: 'secret_text', name: 'EDGE_ADMIN' },
    ],
    tags: [
      'fleet:anchorage',
      'tenant:omega',
      'environment:staging',
      'schema:3',
      `spec:${SECOND_SPEC_DIGEST}`,
    ],
  },
  // No `spec:` tag, so the dispatch inspection refuses and the drain composes
  // the transient provider text the engine keeps out of durable state.
  'edge-broken': {
    bindings: [],
    tags: [
      'fleet:anchorage',
      'tenant:omega',
      'environment:staging',
      'schema:3',
    ],
  },
};

function secondWorld(): ProviderWorld {
  const world = providerWorld();
  world.zones.push({ id: 'zone-edge' });
  world.routes.push({
    zoneId: 'zone-edge',
    id: 'route-edge',
    pattern: 'edge.example.test/*',
    script: 'edge-state',
  });
  world.customDomains.push({
    id: 'domain-edge',
    hostname: 'app.edge.test',
    service: 'edge-state',
  });
  world.durableObjectNamespaces.push({
    id: 'ns-edge',
    script: 'edge-state',
    className: 'EdgeState',
  });
  world.seedDatabase('edge-main', { databaseId: 'db-edge-main' });
  world.seedDatabase('other-main', { databaseId: 'db-other-main' });
  // A trusted Worker reachable only through a preview URL: the arm the golden
  // world never exercised.
  world.seedScript('edge-state', {
    versions: [
      {
        versionId: 'version-edge-state',
        tag: undefined,
        mainModule: 'worker.js',
        modules: [],
        bindings: [
          { type: 'd1', name: 'STATE_DB', database_id: 'db-edge-main' },
          {
            type: 'durable_object_namespace',
            name: 'STATE',
            class_name: 'EdgeState',
            namespace_id: 'ns-edge',
          },
          { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: 'omega' },
          { type: 'plain_text', name: 'FLEET_ENVIRONMENT', text: 'staging' },
          { type: 'plain_text', name: 'FLEET_SCHEMA_VERSION', text: '3' },
          {
            type: 'plain_text',
            name: 'FLEET_RESOURCE_ROLE',
            text: 'platform-state',
          },
          {
            type: 'plain_text',
            name: 'FLEET_RESOURCE_GROUP',
            text: 'policy-edge',
          },
          {
            type: 'plain_text',
            name: 'FLEET_SPEC_DIGEST',
            text: SECOND_SPEC_DIGEST,
          },
          { type: 'secret_text', name: 'EDGE_CREDENTIAL' },
        ],
      },
    ],
    deployment: [{ versionId: 'version-edge-state', percentage: 100 }],
    subdomain: { enabled: false, previewsEnabled: true },
  });
  // No FLEET_SCHEMA_VERSION, so the per-script inventory refuses.
  world.seedScript('edge-unreadable', {
    versions: [
      {
        versionId: 'version-edge-unreadable',
        tag: undefined,
        mainModule: 'worker.js',
        modules: [],
        bindings: [
          { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: 'omega' },
        ],
      },
    ],
    deployment: [{ versionId: 'version-edge-unreadable', percentage: 100 }],
    subdomain: { enabled: false, previewsEnabled: false },
  });
  world.seedScript('other-app', {
    versions: [
      {
        versionId: 'version-other-app',
        tag: undefined,
        mainModule: 'worker.js',
        modules: [],
        bindings: [],
      },
    ],
    deployment: [{ versionId: 'version-other-app', percentage: 100 }],
    subdomain: { enabled: true, previewsEnabled: true },
  });
  return world;
}

function secondWorldHandler(world: ProviderWorld): CloudflareFixtureHandler {
  const rest = restProjection(world);
  const accountPrefix = `/client/v4/accounts/${SECOND_ACCOUNT_ID}`;
  const kvPath = `/storage/kv/namespaces/${SECOND_HOST_ROUTING_KV_ID}`;
  const namespacePath = `/workers/dispatch/namespaces/${SECOND_DISPATCH_NAMESPACE}`;
  const scriptsPath = `${namespacePath}/scripts`;
  const notFound = () =>
    Response.json(
      {
        success: false,
        errors: [{ code: 10090, message: 'missing' }],
        messages: [],
        result: null,
      },
      { status: 404 },
    );
  return async (request) => {
    const target = new URL(request.url);
    const path = decodeURIComponent(target.pathname);
    if (!path.startsWith(`${accountPrefix}/`)) return rest(request);
    const route = path.slice(accountPrefix.length);
    if (route === `${kvPath}/keys`) {
      return pageArray(SECOND_KV_ENTRIES.map(([name]) => ({ name })));
    }
    if (route.startsWith(`${kvPath}/values/`)) {
      const key = route.slice(`${kvPath}/values/`.length);
      const entry = SECOND_KV_ENTRIES.find(([name]) => name === key);
      return entry === undefined ? notFound() : new Response(entry[1]);
    }
    if (route === namespacePath) {
      // The namespace attests a DIFFERENT name, which is the trust arm the
      // golden world never reached.
      return single({
        namespace_name: 'edge-renamed',
        namespace_id: 'edge-namespace-id',
        script_count: SECOND_DISPATCH_LISTING.length,
        trusted_workers: false,
      });
    }
    if (route === scriptsPath) {
      return pageArray(
        SECOND_DISPATCH_LISTING.map(({ id, tags }) => ({ id, tags })),
      );
    }
    if (route.startsWith(`${scriptsPath}/`)) {
      const remainder = route.slice(`${scriptsPath}/`.length);
      const settingsRead = remainder.endsWith('/settings');
      const scriptName = settingsRead
        ? remainder.slice(0, -'/settings'.length)
        : remainder;
      const settings = SECOND_DISPATCH_SETTINGS[scriptName];
      if (!settings) return notFound();
      return settingsRead
        ? single(settings)
        : single({ script: { etag: `etag-${scriptName}` } });
    }
    return rest(request);
  };
}

describe('bounded fleet inventory advance', () => {
  it('materializes a second independent world exactly as its drain does', async () => {
    const drained = await drainWithClient(
      secondWorldHandler(secondWorld()),
      SECOND_ACCOUNT_ID,
      SECOND_DISPATCH_NAMESPACE,
      SECOND_INVENTORY_OPTIONS,
    );
    // The MINIMUM legal provider budget forces `registration-checks` to stop
    // mid-stage and resume, so the equivalence covers single-stage resumption
    // rather than only the 15-stage walk.
    const bounded = await boundedWithClient(
      secondWorldHandler(secondWorld()),
      SECOND_ACCOUNT_ID,
      SECOND_DISPATCH_NAMESPACE,
      SECOND_INVENTORY_OPTIONS,
      9,
    );

    // The world must reach the two branches the golden baseline never did, and
    // both sanitized detail sites, or the equivalence proves too little.
    expect(drained.findings.map((finding) => finding.detail)).toEqual(
      expect.arrayContaining([
        expect.stringContaining('does not attest trusted_workers=false'),
        expect.stringContaining(
          'is publicly reachable on workers.dev, a preview URL, or a zone route',
        ),
        expect.stringContaining(INSPECT_SUFFIX),
        expect.stringContaining(INVENTORY_SUFFIX),
      ]),
    );
    expect(bounded.inventory).toEqual(sanitizedInventory(drained));
    expect(bounded.chunks).toBeGreaterThan(1);
    expect(
      bounded.executed.filter((stage) => stage.step === 'registration-checks'),
    ).toEqual([
      { step: 'registration-checks', registrationOrdinal: 0 },
      { step: 'registration-checks', registrationOrdinal: 1 },
    ]);
  });

  it('refuses a provider-request budget outside 9..1000 and accepts both bounds', async () => {
    for (const maxProviderRequests of [8, 1_001]) {
      await expect(
        runToCompletion({
          context: stubContext(),
          store: new FakeInventoryRunStore(),
          maxProviderRequests,
        }),
      ).rejects.toThrow(
        'maxProviderRequests must be an integer from 9 to 1000',
      );
    }
    for (const maxProviderRequests of [9, 1_000]) {
      const result = await runToCompletion({
        context: stubContext(),
        store: new FakeInventoryRunStore(),
        maxProviderRequests,
      });
      expect(result.status).toBe('complete');
    }
  });

  it('refuses a staged-row budget outside 1..2000 and accepts both bounds', async () => {
    for (const maxStagedRowsPerChunk of [0, 2_001]) {
      await expect(
        runToCompletion({
          context: stubContext(),
          store: new FakeInventoryRunStore(),
          maxProviderRequests: MAX_PROVIDER_REQUESTS,
          maxStagedRowsPerChunk,
        }),
      ).rejects.toThrow(
        'maxStagedRowsPerChunk must be an integer from 1 to 2000',
      );
    }
    for (const maxStagedRowsPerChunk of [1, 2_000]) {
      const result = await runToCompletion({
        context: stubContext(),
        store: new FakeInventoryRunStore(),
        maxProviderRequests: MAX_PROVIDER_REQUESTS,
        maxStagedRowsPerChunk,
      });
      expect(result.status).toBe('complete');
    }
  });

  it('canonicalizes and digests the options it persists at start', async () => {
    const store = new FakeInventoryRunStore();
    await advanceFleetInventory({
      context: stubContext(),
      store,
      action: {
        kind: 'start',
        operationId: OPERATION_ID,
        options: STUB_OPTIONS,
      },
      maxProviderRequests: MAX_PROVIDER_REQUESTS,
    });

    const run = store.runs.get(OPERATION_ID);
    const canonical: FleetInventoryRunOptions =
      canonicalFleetInventoryRunOptions(STUB_OPTIONS);
    expect(canonical.includeDispatchNamespace).toBe(true);
    expect(canonical.includeR2Buckets).toBe(false);
    expect(run?.options).toEqual(canonical);
    expect(run?.optionsDigest).toBe(fleetInventoryOptionsDigest(canonical));
  });

  it('replays a start for the same operation without a new generation', async () => {
    const store = new FakeInventoryRunStore();
    const context = stubContext();
    const first = await advanceFleetInventory({
      context,
      store,
      action: {
        kind: 'start',
        operationId: OPERATION_ID,
        options: STUB_OPTIONS,
      },
      maxProviderRequests: MAX_PROVIDER_REQUESTS,
    });
    const second = await advanceFleetInventory({
      context,
      store,
      action: {
        kind: 'start',
        operationId: OPERATION_ID,
        options: STUB_OPTIONS,
      },
      maxProviderRequests: MAX_PROVIDER_REQUESTS,
    });

    expect(first.token.operationId).toBe(OPERATION_ID);
    expect(second.status).toBe('complete');
    expect(store.runs.size).toBe(1);
    expect(store.nextGeneration).toBe(2);
  });

  it('contends on a foreign active operation without any provider call', async () => {
    const store = new FakeInventoryRunStore();
    store.activeOperationId = FOREIGN_OPERATION_ID;
    const context = stubContext();

    await expect(
      advanceFleetInventory({
        context,
        store,
        action: {
          kind: 'start',
          operationId: OPERATION_ID,
          options: STUB_OPTIONS,
        },
        maxProviderRequests: MAX_PROVIDER_REQUESTS,
      }),
    ).rejects.toThrow(
      'another fleet inventory operation owns this account head',
    );
    expect(context.inputs).toHaveLength(0);
  });

  it('returns the authoritative result for a stale token with zero provider calls', async () => {
    const store = new FakeInventoryRunStore();
    const context = stubContext();
    const started = await advanceFleetInventory({
      context,
      store,
      action: {
        kind: 'start',
        operationId: OPERATION_ID,
        options: STUB_OPTIONS,
      },
      maxProviderRequests: MAX_PROVIDER_REQUESTS,
    });
    const calls = context.inputs.length;

    const stale = await advanceFleetInventory({
      context,
      store,
      action: {
        kind: 'continue',
        token: { version: 1, operationId: OPERATION_ID, revision: 0 },
      },
      maxProviderRequests: MAX_PROVIDER_REQUESTS,
    });

    expect(started.token.revision).toBe(1);
    expect(stale).toEqual(started);
    expect(context.inputs).toHaveLength(calls);
  });

  it('refuses a token ahead of the persisted run', async () => {
    const store = new FakeInventoryRunStore();
    const context = stubContext();
    await advanceFleetInventory({
      context,
      store,
      action: {
        kind: 'start',
        operationId: OPERATION_ID,
        options: STUB_OPTIONS,
      },
      maxProviderRequests: MAX_PROVIDER_REQUESTS,
    });

    await expect(
      advanceFleetInventory({
        context,
        store,
        action: {
          kind: 'continue',
          token: { version: 1, operationId: OPERATION_ID, revision: 9 },
        },
        maxProviderRequests: MAX_PROVIDER_REQUESTS,
      }),
    ).rejects.toBeInstanceOf(FleetInventoryRunTokenFutureError);
  });

  it('completes an unknown lease operation whose persisted run is finalized', async () => {
    const store = new FakeInventoryRunStore();
    const completed = await runToCompletion({
      context: stubContext(),
      store,
      maxProviderRequests: MAX_PROVIDER_REQUESTS,
    });
    store.hiddenFromLease.add(OPERATION_ID);

    const replay = await advanceFleetInventory({
      context: stubContext(),
      store,
      action: { kind: 'continue', token: completed.token },
      maxProviderRequests: MAX_PROVIDER_REQUESTS,
    });

    expect(replay.status).toBe('complete');
    expect(replay).toEqual(completed);
  });

  it('refuses a token for an operation the store has never seen', async () => {
    await expect(
      advanceFleetInventory({
        context: stubContext(),
        store: new FakeInventoryRunStore(),
        action: {
          kind: 'continue',
          token: { version: 1, operationId: OPERATION_ID, revision: 0 },
        },
        maxProviderRequests: MAX_PROVIDER_REQUESTS,
      }),
    ).rejects.toBeInstanceOf(FleetInventoryRunTokenOperationError);
  });

  it('refuses a malformed token before it reaches the account lease', async () => {
    const store = new FakeInventoryRunStore();

    await expect(
      advanceFleetInventory({
        context: stubContext(),
        store,
        action: { kind: 'continue', token: { version: 2, revision: -1 } },
        maxProviderRequests: MAX_PROVIDER_REQUESTS,
      }),
    ).rejects.toBeInstanceOf(FleetInventoryRunTokenError);
    expect(store.leases).toBe(0);
  });

  it('aborts a lost lease at the dispatch boundary before any provider call', async () => {
    const store = new FakeInventoryRunStore();
    store.leaseLost = true;
    const context = stubContext();

    await expect(
      advanceFleetInventory({
        context,
        store,
        action: {
          kind: 'start',
          operationId: OPERATION_ID,
          options: STUB_OPTIONS,
        },
        maxProviderRequests: MAX_PROVIDER_REQUESTS,
      }),
    ).rejects.toThrow('fleet inventory account lease was lost');
    expect(context.inputs).toHaveLength(0);
  });

  it('refuses a store that cannot stage bounded inventory runs', async () => {
    const context = stubContext();

    await expect(
      advanceFleetInventory({
        context,
        store: storeWithout(
          new FakeInventoryRunStore(),
          'withAccountInventoryLease',
        ),
        action: {
          kind: 'start',
          operationId: OPERATION_ID,
          options: STUB_OPTIONS,
        },
        maxProviderRequests: MAX_PROVIDER_REQUESTS,
      }),
    ).rejects.toThrow(
      'fleet inventory requires a run store with bounded staging support',
    );
    expect(context.inputs).toHaveLength(0);
  });

  it('refuses a store that cannot read finalized generations', async () => {
    const context = stubContext();

    await expect(
      advanceFleetInventory({
        context,
        store: storeWithout(
          new FakeInventoryRunStore(),
          'readFinalizedGeneration',
        ),
        action: {
          kind: 'start',
          operationId: OPERATION_ID,
          options: STUB_OPTIONS,
        },
        maxProviderRequests: MAX_PROVIDER_REQUESTS,
      }),
    ).rejects.toThrow(
      'fleet inventory requires a run store that can read finalized generations',
    );
    expect(context.inputs).toHaveLength(0);
  });

  it('refuses a store that cannot read a run by operation', async () => {
    const context = stubContext();

    await expect(
      advanceFleetInventory({
        context,
        store: storeWithout(new FakeInventoryRunStore(), 'readRunByOperation'),
        action: {
          kind: 'start',
          operationId: OPERATION_ID,
          options: STUB_OPTIONS,
        },
        maxProviderRequests: MAX_PROVIDER_REQUESTS,
      }),
    ).rejects.toThrow(
      'fleet inventory requires a run store that can read finalized generations',
    );
    expect(context.inputs).toHaveLength(0);
  });

  it('refuses a store that cannot pin finalized generations', async () => {
    const context = stubContext();
    const failure = await advanceFleetInventory({
      context,
      store: storeWithout(new FakeInventoryRunStore(), 'pinGeneration'),
      action: {
        kind: 'start',
        operationId: OPERATION_ID,
        options: STUB_OPTIONS,
      },
      maxProviderRequests: MAX_PROVIDER_REQUESTS,
    }).catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(FleetInventoryAdvanceCapabilityError);
    expect((failure as FleetInventoryAdvanceCapabilityError).capability).toBe(
      'generation-pin',
    );
    expect(context.inputs).toHaveLength(0);
  });

  it('materializes a generation from a store that can only read it', async () => {
    const bounded = await boundedWithClient(
      fleetInventoryDrainHandler(fleetInventoryDrainWorld()),
      DRAIN_ACCOUNT_ID,
      DRAIN_DISPATCH_NAMESPACE,
      DRAIN_INVENTORY_OPTIONS,
    );
    const readOnly = storeWithout(
      storeWithout(bounded.store, 'withAccountInventoryLease'),
      'pinGeneration',
    );

    await expect(readFleetInventoryGeneration(readOnly, 1)).resolves.toEqual(
      bounded.inventory,
    );
  });

  it('completes with a generation reference rather than an inventory', async () => {
    const store = new FakeInventoryRunStore();
    const result = await runToCompletion({
      context: stubContext(),
      store,
      maxProviderRequests: MAX_PROVIDER_REQUESTS,
    });

    expect(result.status).toBe('complete');
    if (result.status !== 'complete') return;
    expect(Object.keys(result.generation).sort()).toEqual([
      'factCount',
      'finalizedAtMs',
      'generation',
      'operationId',
      'rowManifest',
    ]);
    expect(result.generation).not.toHaveProperty('findings');
    expect(result.generation).not.toHaveProperty('deployments');
  });

  it('materializes the golden world into the baseline inventory minus the two provider details', async () => {
    const bounded = await boundedWithClient(
      fleetInventoryDrainHandler(fleetInventoryDrainWorld()),
      DRAIN_ACCOUNT_ID,
      DRAIN_DISPATCH_NAMESPACE,
      DRAIN_INVENTORY_OPTIONS,
    );

    expect(bounded.inventory).toEqual(
      sanitizedInventory(DRAIN_BASELINE_INVENTORY as FleetResourceInventory),
    );
  });

  it('refuses to materialize a staging, failed, or corrupt generation', async () => {
    const staging = new FakeInventoryRunStore();
    await advanceFleetInventory({
      context: stubContext(),
      store: staging,
      action: {
        kind: 'start',
        operationId: OPERATION_ID,
        options: STUB_OPTIONS,
      },
      maxProviderRequests: MAX_PROVIDER_REQUESTS,
    });
    await expect(readFleetInventoryGeneration(staging, 1)).rejects.toThrow(
      'fleet inventory generation 1 is not finalized',
    );

    const failed = new FakeInventoryRunStore();
    await failed.withAccountInventoryLease(async (lease) => {
      await lease.startRun({
        operationId: OPERATION_ID,
        options: canonicalFleetInventoryRunOptions(STUB_OPTIONS),
        optionsDigest: fleetInventoryOptionsDigest(
          canonicalFleetInventoryRunOptions(STUB_OPTIONS),
        ),
      });
      await lease.failRun({
        operationId: OPERATION_ID,
        expectedRevision: 0,
        reason: 'operator-abandoned',
      });
    });
    await expect(readFleetInventoryGeneration(failed, 1)).rejects.toThrow(
      'fleet inventory generation 1 is not finalized',
    );

    const corrupt = await boundedWithClient(
      fleetInventoryDrainHandler(fleetInventoryDrainWorld()),
      DRAIN_ACCOUNT_ID,
      DRAIN_DISPATCH_NAMESPACE,
      DRAIN_INVENTORY_OPTIONS,
    );
    corrupt.store.rows.get(1)?.pop();
    await expect(
      readFleetInventoryGeneration(corrupt.store, 1),
    ).rejects.toThrow(
      'fleet inventory generation 1 does not match its manifest',
    );
  });

  it('keeps the abort signal call-local and never persists it', async () => {
    const store = new FakeInventoryRunStore();
    const context = stubContext();
    const controller = new AbortController();
    await advanceFleetInventory({
      context,
      store,
      action: {
        kind: 'start',
        operationId: OPERATION_ID,
        options: STUB_OPTIONS,
      },
      maxProviderRequests: MAX_PROVIDER_REQUESTS,
      signal: controller.signal,
    });

    expect(context.inputs[0]?.signal).toBe(controller.signal);
    expect(JSON.stringify(store.runs.get(OPERATION_ID))).not.toContain(
      'signal',
    );

    const aborted = new AbortController();
    aborted.abort(new Error('inventory aborted'));
    const abortingContext: FleetInventoryProviderContext = {
      advanceStage: async (input) => {
        input.signal?.throwIfAborted();
        throw new Error('unreachable');
      },
    };
    await expect(
      advanceFleetInventory({
        context: abortingContext,
        store: new FakeInventoryRunStore(),
        action: {
          kind: 'start',
          operationId: OPERATION_ID,
          options: STUB_OPTIONS,
        },
        maxProviderRequests: MAX_PROVIDER_REQUESTS,
        signal: aborted.signal,
      }),
    ).rejects.toThrow('inventory aborted');
  });

  it('never stages a secret value, credential header byte, or provider cursor', async () => {
    const bounded = await boundedWithClient(
      fleetInventoryDrainHandler(fleetInventoryDrainWorld()),
      DRAIN_ACCOUNT_ID,
      DRAIN_DISPATCH_NAMESPACE,
      DRAIN_INVENTORY_OPTIONS,
    );
    const staged = JSON.stringify([
      ...(bounded.store.rows.get(1) ?? []),
      ...(bounded.store.facts.get(1) ?? []),
    ]);
    const runRecord = JSON.stringify(bounded.store.runs.get(OPERATION_ID));

    for (const forbidden of [
      'Authorization',
      'authorization',
      'Bearer ',
      'token',
    ]) {
      expect(staged).not.toContain(forbidden);
      expect(runRecord).not.toContain(forbidden);
    }
    for (const forbidden of ['cursor', 'startAfter']) {
      expect(staged).not.toContain(forbidden);
    }
    // Secret NAMES are durable by design; a secret VALUE never is.
    expect(staged).toContain('MAINTENANCE_ADMIN');
  });
});
