// SPDX-License-Identifier: Apache-2.0

/**
 * Hand-authored deterministic provider world for the `collectFleetInventory`
 * golden baseline. `scripts/record-drain-baseline.mjs` and
 * `test/cloudflare-client.test.ts` both import this file; the recorder NEVER
 * writes it, so the recorded literals can never rewrite their own input.
 *
 * The world deliberately drives every interesting drain path: Workers for
 * Platforms registrations and independent plain Workers, malformed and stale
 * host-routing KV entries, a registration whose dispatch inspection fails, a
 * plain Worker whose per-script inventory fails, zone routes, custom domains,
 * R2 buckets in two of the three scanned jurisdictions, D1 databases, and
 * Durable Object namespaces.
 *
 * It also meets the spec's branch-coverage floor: all seven finding kinds
 * appear, including the dispatch-namespace trust attestation, the live dispatch
 * ownership mismatch, both arms of `incomplete-deployment` (public reachability
 * and a zone route on a trusted Worker), and the host-route state-egress parse
 * failure.
 *
 * PRESERVED BEHAVIOR NOTE: the recorded request ORDER is part of the frozen
 * baseline, and parts of it come from `Promise.all` sites in the drain
 * (`cloudflare-client.ts` per-script inventory and dispatch inspection). That
 * order is stable for this world but is not guaranteed by construction, so a
 * later rewrite must preserve it deliberately rather than assume it. If a
 * rewrite legitimately changes concurrency, the ordering change is a
 * compatibility decision to escalate — not a baseline to quietly re-record.
 */

import { CloudflareProvisioningClient } from '../../src/cloudflare-client.js';
import { canonicalDeploymentEgressPolicy } from '../../src/platform-resources.js';
import type { FleetResourceInventory } from '../../src/types.js';
import {
  type CloudflareFetchRecord,
  type CloudflareFixtureHandler,
  pageArray,
  recordingFetch,
  restProjection,
  single,
  testRateCoordinator,
} from './cloudflare-fetch-fixture.js';
import { type ProviderWorld, providerWorld } from './provider-world.js';

export const DRAIN_ACCOUNT_ID = 'account';
export const DRAIN_DISPATCH_NAMESPACE = 'fleet';
export const DRAIN_HOST_ROUTING_KV_ID = 'hosts';

/** The exact options the recorded baseline was drained with. */
export const DRAIN_INVENTORY_OPTIONS = {
  hostRoutingKvId: DRAIN_HOST_ROUTING_KV_ID,
  databaseNamePrefix: 'fleet-',
  scriptNamePrefix: 'fleet-',
  includeDispatchNamespace: true,
  includeR2Buckets: true,
} as const;

const ALPHA_POLICY = canonicalDeploymentEgressPolicy({
  policyId: 'policy-alpha',
  tenantTag: 'acme',
  environment: 'production',
  allowedHosts: ['api.example.com'],
});

const STALE_POLICY = canonicalDeploymentEgressPolicy({
  policyId: 'policy-stale',
  tenantTag: 'acme',
  environment: 'production',
  allowedHosts: [],
});

const SPEC_DIGEST = 'a'.repeat(64);
const CREDENTIAL_DIGEST = 'b'.repeat(64);

const R2_JURISDICTION_HEADER = 'cf-r2-jurisdiction';

/**
 * The only headers copied into a recorded request. The R2 jurisdiction travels
 * as a header, so the three R2 page GETs would otherwise be byte-identical and
 * their order unpinnable. `authorization` and every other credential-bearing
 * header must never be added here.
 */
const RECORDED_HEADER_ALLOWLIST: readonly string[] = [R2_JURISDICTION_HEADER];

/**
 * Listing order of the host-routing KV namespace, followed by each stored
 * value. Keys stay lowercase because the client lowercases every value read.
 */
const HOST_ROUTING_KV_ENTRIES: readonly (readonly [string, string])[] = [
  [
    '__anchorage_script__:fleet-alpha',
    JSON.stringify({
      scriptName: 'fleet-alpha',
      tenantTag: 'acme',
      environment: 'production',
      databaseId: 'db-alpha',
      routeHostname: 'alpha.example.test',
    }),
  ],
  [
    '__anchorage_script__:fleet-broken',
    JSON.stringify({
      scriptName: 'fleet-broken',
      tenantTag: 'beta',
      environment: 'staging',
      databaseId: 'db-broken',
      routeHostname: 'broken.example.test',
    }),
  ],
  [
    '__anchorage_script__:fleet-wrong-key',
    JSON.stringify({
      scriptName: 'fleet-other-owner',
      tenantTag: 'gamma',
      environment: 'production',
      databaseId: 'db-other-owner',
      routeHostname: 'other-owner.example.test',
    }),
  ],
  // Owner-checked registration whose live dispatch Worker binds another
  // database, so the drain records the ownership mismatch at
  // cloudflare-client.ts:1666-1679.
  [
    '__anchorage_script__:fleet-drifted',
    JSON.stringify({
      scriptName: 'fleet-drifted',
      tenantTag: 'acme',
      environment: 'production',
      databaseId: 'db-drifted',
      routeHostname: 'drifted.example.test',
    }),
  ],
  ['__anchorage_script__:fleet-malformed', '{'],
  [
    '__anchorage_script__:fleet-incomplete',
    JSON.stringify({
      scriptName: 'fleet-incomplete',
      tenantTag: 'delta',
      environment: 'production',
      routeHostname: 'incomplete.example.test',
    }),
  ],
  [
    'alpha.example.test',
    JSON.stringify({
      scriptName: 'fleet-alpha',
      tenantTag: 'acme',
      environment: 'production',
      ...ALPHA_POLICY,
      stateEgress: {
        resourceGroupId: ALPHA_POLICY.policyId,
        stateScriptName: 'fleet-state',
        credentialDigest: CREDENTIAL_DIGEST,
      },
    }),
  ],
  [
    'stale.example.test',
    JSON.stringify({
      scriptName: 'fleet-alpha',
      tenantTag: 'acme',
      environment: 'production',
      ...STALE_POLICY,
    }),
  ],
  [
    'bad-policy.example.test',
    JSON.stringify({
      scriptName: 'fleet-alpha',
      tenantTag: 'acme',
      environment: 'production',
      policyId: ALPHA_POLICY.policyId,
      policyHosts: ALPHA_POLICY.policyHosts,
      policyDigest: '0'.repeat(64),
    }),
  ],
  // Consistent policy metadata with an unusable state-egress block, so
  // parseHostRoutingTarget refuses it at cloudflare-client.ts:1560-1570.
  [
    'bad-state-egress.example.test',
    JSON.stringify({
      scriptName: 'fleet-alpha',
      tenantTag: 'acme',
      environment: 'production',
      ...ALPHA_POLICY,
      stateEgress: {
        resourceGroupId: ALPHA_POLICY.policyId,
        stateScriptName: 'fleet-state',
        credentialDigest: 'not-a-sha256-digest',
      },
    }),
  ],
  ['malformed.example.test', '{'],
];

const DISPATCH_SCRIPT_LISTING: readonly Readonly<{
  id: string;
  tags: readonly string[];
}>[] = [
  {
    id: 'fleet-alpha',
    tags: ['fleet:anchorage', 'tenant:acme', 'environment:production'],
  },
  {
    id: 'fleet-broken',
    tags: ['fleet:anchorage', 'tenant:beta', 'environment:staging'],
  },
  // The listing tags agree with the registration, so only the live-settings
  // ownership check can flag this script.
  {
    id: 'fleet-drifted',
    tags: ['fleet:anchorage', 'tenant:acme', 'environment:production'],
  },
  {
    id: 'fleet-orphan',
    tags: ['fleet:anchorage', 'tenant:orphan', 'environment:production'],
  },
];

const DISPATCH_SCRIPT_SETTINGS: Readonly<
  Record<
    string,
    Readonly<{ bindings: readonly unknown[]; tags: readonly string[] }>
  >
> = {
  'fleet-alpha': {
    bindings: [
      { type: 'd1', name: 'DB', database_id: 'db-alpha' },
      {
        type: 'durable_object_namespace',
        name: 'MAINTENANCE',
        class_name: 'Maintenance',
        namespace_id: 'ns-alpha',
      },
      { type: 'service', name: 'EGRESS', service: 'fleet-egress' },
      { type: 'queue', name: 'AUDIT', queue_name: 'fleet-audit' },
      { type: 'r2_bucket', name: 'EXPORTS', bucket_name: 'fleet-exports' },
      { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: 'acme' },
      { type: 'secret_text', name: 'MAINTENANCE_ADMIN' },
    ],
    tags: [
      'fleet:anchorage',
      'tenant:acme',
      'environment:production',
      'schema:2',
      `spec:${SPEC_DIGEST}`,
      'do:v3',
    ],
  },
  // No `spec:` tag, so inspectDispatchWorker refuses this registration and the
  // drain records the String(error) detail at cloudflare-client.ts:1627-1630.
  'fleet-broken': {
    bindings: [{ type: 'd1', name: 'DB', database_id: 'db-broken' }],
    tags: ['fleet:anchorage', 'tenant:beta', 'environment:staging', 'schema:2'],
  },
  // Complete metadata that binds a database the registration does not claim.
  'fleet-drifted': {
    bindings: [{ type: 'd1', name: 'DB', database_id: 'db-drifted-live' }],
    tags: [
      'fleet:anchorage',
      'tenant:acme',
      'environment:production',
      'schema:2',
      `spec:${SPEC_DIGEST}`,
    ],
  },
};

const R2_BUCKETS: Readonly<
  Record<string, readonly Readonly<{ name: string; creation_date: string }>[]>
> = {
  default: [
    { name: 'fleet-alpha-exports', creation_date: '2026-08-01T00:00:00.000Z' },
    { name: 'other-exports', creation_date: '2026-08-02T00:00:00.000Z' },
  ],
  eu: [{ name: 'fleet-eu-state', creation_date: '2026-08-03T00:00:00.000Z' }],
  fedramp: [],
};

function providerNotFound(): Response {
  return Response.json(
    {
      success: false,
      errors: [{ code: 10090, message: 'missing' }],
      messages: [],
      result: null,
    },
    { status: 404 },
  );
}

/**
 * The rich world every drain baseline run starts from. A fresh instance per
 * run keeps the recorder and the equivalence test byte-identical.
 */
export function fleetInventoryDrainWorld(): ProviderWorld {
  const world = providerWorld();
  world.zones.push({ id: 'zone-a' }, { id: 'zone-b' });
  world.routes.push(
    {
      zoneId: 'zone-a',
      id: 'route-state',
      pattern: 'state.example.test/*',
      script: 'fleet-state',
    },
    {
      zoneId: 'zone-b',
      id: 'route-plain',
      pattern: 'plain.example.test/*',
      script: 'fleet-plain',
    },
    {
      zoneId: 'zone-b',
      id: 'route-other',
      pattern: 'other.example.test/*',
      script: 'other-worker',
    },
  );
  world.customDomains.push(
    {
      id: 'domain-plain',
      hostname: 'plain.example.test',
      service: 'fleet-plain',
    },
    {
      id: 'domain-ghost',
      hostname: 'ghost.example.test',
      service: 'fleet-ghost',
    },
    {
      id: 'domain-other',
      hostname: 'other.example.test',
      service: 'other-worker',
    },
  );
  world.durableObjectNamespaces.push(
    { id: 'ns-alpha', script: 'fleet-alpha', className: 'Maintenance' },
    { id: 'ns-state', script: 'fleet-state', className: 'State' },
    { id: 'ns-other', script: 'other-worker', className: 'Other' },
  );
  world.seedDatabase('fleet-alpha', { databaseId: 'db-alpha' });
  world.seedDatabase('fleet-plain', { databaseId: 'db-plain' });
  world.seedDatabase('other-db', { databaseId: 'db-other' });
  world.seedScript('fleet-state', {
    versions: [
      {
        versionId: 'version-fleet-state',
        tag: undefined,
        mainModule: 'worker.js',
        modules: [],
        bindings: [
          { type: 'd1', name: 'STATE_DB', database_id: 'db-plain' },
          {
            type: 'durable_object_namespace',
            name: 'STATE',
            class_name: 'State',
            namespace_id: 'ns-state',
          },
          { type: 'service', name: 'EGRESS', service: 'fleet-egress' },
          { type: 'queue', name: 'AUDIT', queue_name: 'fleet-audit' },
          { type: 'kv_namespace', name: 'HOSTS', namespace_id: 'hosts' },
          {
            type: 'r2_bucket',
            name: 'EXPORTS',
            bucket_name: 'fleet-alpha-exports',
          },
          { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: 'acme' },
          { type: 'plain_text', name: 'FLEET_ENVIRONMENT', text: 'production' },
          { type: 'plain_text', name: 'FLEET_SCHEMA_VERSION', text: '2' },
          {
            type: 'plain_text',
            name: 'FLEET_RESOURCE_ROLE',
            text: 'platform-state',
          },
          {
            type: 'plain_text',
            name: 'FLEET_RESOURCE_GROUP',
            text: 'policy-alpha',
          },
          { type: 'plain_text', name: 'FLEET_SPEC_DIGEST', text: SPEC_DIGEST },
          // No `text` at all: no fixture in this world carries a secret value,
          // even one the provider projection would strip before recording.
          { type: 'secret_text', name: 'STATE_CREDENTIAL' },
        ],
      },
    ],
    deployment: [{ versionId: 'version-fleet-state', percentage: 100 }],
    subdomain: { enabled: false, previewsEnabled: false },
  });
  world.seedScript('fleet-plain', {
    versions: [
      {
        versionId: 'version-fleet-plain',
        tag: undefined,
        mainModule: 'worker.js',
        modules: [],
        bindings: [
          { type: 'd1', name: 'DB', database_id: 'db-plain' },
          { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: 'plain' },
          { type: 'plain_text', name: 'FLEET_ENVIRONMENT', text: 'staging' },
          { type: 'plain_text', name: 'FLEET_SCHEMA_VERSION', text: '4' },
        ],
      },
    ],
    deployment: [{ versionId: 'version-fleet-plain', percentage: 100 }],
    subdomain: { enabled: false, previewsEnabled: false },
  });
  // A trusted Worker reachable on workers.dev with no zone route, so the drain
  // records the public-access arm of incomplete-deployment at
  // cloudflare-client.ts:1931-1934.
  world.seedScript('fleet-egress', {
    versions: [
      {
        versionId: 'version-fleet-egress',
        tag: undefined,
        mainModule: 'worker.js',
        modules: [],
        bindings: [
          { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: 'acme' },
          { type: 'plain_text', name: 'FLEET_ENVIRONMENT', text: 'production' },
          { type: 'plain_text', name: 'FLEET_SCHEMA_VERSION', text: '2' },
          {
            type: 'plain_text',
            name: 'FLEET_RESOURCE_ROLE',
            text: 'deployment-egress',
          },
          {
            type: 'plain_text',
            name: 'FLEET_RESOURCE_GROUP',
            text: 'policy-alpha',
          },
        ],
      },
    ],
    deployment: [{ versionId: 'version-fleet-egress', percentage: 100 }],
    subdomain: { enabled: true, previewsEnabled: false },
  });
  // No FLEET_SCHEMA_VERSION, so the per-script inventory throws and the drain
  // records the String(error) detail at cloudflare-client.ts:1974-1978.
  world.seedScript('fleet-unreadable', {
    versions: [
      {
        versionId: 'version-fleet-unreadable',
        tag: undefined,
        mainModule: 'worker.js',
        modules: [],
        bindings: [
          { type: 'plain_text', name: 'DEPLOYMENT_TENANT', text: 'epsilon' },
          { type: 'plain_text', name: 'FLEET_ENVIRONMENT', text: 'production' },
        ],
      },
    ],
    deployment: [{ versionId: 'version-fleet-unreadable', percentage: 100 }],
    subdomain: { enabled: false, previewsEnabled: false },
  });
  world.seedScript('other-worker', {
    versions: [
      {
        versionId: 'version-other-worker',
        tag: undefined,
        mainModule: 'worker.js',
        modules: [],
        bindings: [],
      },
    ],
    deployment: [{ versionId: 'version-other-worker', percentage: 100 }],
    subdomain: { enabled: true, previewsEnabled: true },
  });
  return world;
}

/**
 * Serves the endpoints `restProjection` does not model — the host-routing KV
 * namespace, the dispatch namespace and its scripts (whose fleet tags the
 * shared projection does not carry), and R2 bucket listing — and delegates
 * everything else to the shared projection.
 */
export function fleetInventoryDrainHandler(
  world: ProviderWorld,
): CloudflareFixtureHandler {
  const rest = restProjection(world);
  const accountPrefix = `/client/v4/accounts/${DRAIN_ACCOUNT_ID}`;
  const kvPath = `/storage/kv/namespaces/${DRAIN_HOST_ROUTING_KV_ID}`;
  const namespacePath = `/workers/dispatch/namespaces/${DRAIN_DISPATCH_NAMESPACE}`;
  const scriptsPath = `${namespacePath}/scripts`;
  return async (request) => {
    const target = new URL(request.url);
    const path = decodeURIComponent(target.pathname);
    if (!path.startsWith(`${accountPrefix}/`)) return rest(request);
    const route = path.slice(accountPrefix.length);
    if (route === `${kvPath}/keys`) {
      return pageArray(HOST_ROUTING_KV_ENTRIES.map(([name]) => ({ name })));
    }
    if (route.startsWith(`${kvPath}/values/`)) {
      const key = route.slice(`${kvPath}/values/`.length);
      const entry = HOST_ROUTING_KV_ENTRIES.find(([name]) => name === key);
      return entry === undefined ? providerNotFound() : new Response(entry[1]);
    }
    if (route === namespacePath) {
      return single({
        namespace_name: DRAIN_DISPATCH_NAMESPACE,
        namespace_id: 'dispatch-namespace-id',
        // trusted_workers=true fails the namespace attestation, and a
        // script_count above the paginated listing reports missing scripts, so
        // both branches of the attestation block stay observable.
        script_count: DISPATCH_SCRIPT_LISTING.length + 1,
        trusted_workers: true,
      });
    }
    if (route === scriptsPath) {
      return pageArray(
        DISPATCH_SCRIPT_LISTING.map(({ id, tags }) => ({ id, tags })),
      );
    }
    if (route.startsWith(`${scriptsPath}/`)) {
      const remainder = route.slice(`${scriptsPath}/`.length);
      const settingsRead = remainder.endsWith('/settings');
      const scriptName = settingsRead
        ? remainder.slice(0, -'/settings'.length)
        : remainder;
      const settings = DISPATCH_SCRIPT_SETTINGS[scriptName];
      if (!settings) return providerNotFound();
      return settingsRead
        ? single(settings)
        : single({ script: { etag: `etag-${scriptName}` } });
    }
    if (route === '/r2/buckets' && request.method === 'GET') {
      const jurisdiction =
        request.headers.get(R2_JURISDICTION_HEADER) ?? 'default';
      return single({ buckets: R2_BUCKETS[jurisdiction] ?? [] });
    }
    return rest(request);
  };
}

/**
 * A recorded provider request. `headers` carries ONLY
 * `RECORDED_HEADER_ALLOWLIST`; no credential-bearing header is ever recorded,
 * because the baseline is committed to the repository.
 */
export interface DrainRequestRecord extends CloudflareFetchRecord {
  readonly headers?: Readonly<Record<string, string>>;
}

function allowlistedHeaders(
  input: Parameters<typeof fetch>[0],
  init: Parameters<typeof fetch>[1],
): Readonly<Record<string, string>> {
  const headers = new Headers(
    init?.headers ?? (input instanceof Request ? input.headers : undefined),
  );
  return Object.fromEntries(
    RECORDED_HEADER_ALLOWLIST.flatMap((name) => {
      const value = headers.get(name);
      return value === null ? [] : [[name, value] as const];
    }),
  );
}

/** Runs the current drain against a fresh world through the recording fetch. */
export async function runFleetInventoryDrain(): Promise<{
  readonly requests: readonly DrainRequestRecord[];
  readonly inventory: FleetResourceInventory;
}> {
  const world = fleetInventoryDrainWorld();
  const fixture = recordingFetch(fleetInventoryDrainHandler(world));
  const recordedHeaders: Readonly<Record<string, string>>[] = [];
  const client = new CloudflareProvisioningClient({
    accountId: DRAIN_ACCOUNT_ID,
    apiToken: 'token',
    rateCoordinator: testRateCoordinator(),
    dispatchNamespace: DRAIN_DISPATCH_NAMESPACE,
    fetch: (input, init) => {
      // The recording fixture keeps `data:` FormData probes out of `requests`,
      // so skipping them here keeps both sequences index-aligned.
      const url = new URL(
        typeof input === 'string'
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
      );
      if (url.protocol !== 'data:') {
        recordedHeaders.push(allowlistedHeaders(input, init));
      }
      return fixture.fetch(input, init);
    },
  });
  const inventory = await client.collectFleetInventory(DRAIN_INVENTORY_OPTIONS);
  const requests = fixture.requests.map((request, index) => {
    const headers = recordedHeaders[index] ?? {};
    return {
      ...request,
      ...(Object.keys(headers).length > 0 ? { headers } : {}),
    };
  });
  return { requests, inventory };
}
