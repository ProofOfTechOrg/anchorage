// SPDX-License-Identifier: Apache-2.0

/**
 * GENERATED FILE — DO NOT EDIT BY HAND.
 *
 * Written by `scripts/record-drain-baseline.mjs` from the hand-authored world
 * in `fleet-inventory-drain-world.ts`. It freezes the observable behavior of
 * `CloudflareProvisioningClient.collectFleetInventory()` before its internals
 * are rewritten, so the rewrite can be proven byte-equivalent. Verify with
 * `node scripts/record-drain-baseline.mjs --check`; any required change to
 * these literals is a compatibility break, not a fixture update.
 */

import type { FleetResourceInventory } from '../../src/types.js';
import type { DrainRequestRecord } from './fleet-inventory-drain-world.js';

/** Every provider request the drain issued, in order. */
export const DRAIN_BASELINE_REQUESTS = [
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/storage/kv/namespaces/hosts/keys',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/storage/kv/namespaces/hosts/values/__anchorage_script__:fleet-alpha',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/storage/kv/namespaces/hosts/values/__anchorage_script__:fleet-broken',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/storage/kv/namespaces/hosts/values/__anchorage_script__:fleet-wrong-key',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/storage/kv/namespaces/hosts/values/__anchorage_script__:fleet-drifted',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/storage/kv/namespaces/hosts/values/__anchorage_script__:fleet-malformed',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/storage/kv/namespaces/hosts/values/__anchorage_script__:fleet-incomplete',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/storage/kv/namespaces/hosts/values/alpha.example.test',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/storage/kv/namespaces/hosts/values/stale.example.test',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/storage/kv/namespaces/hosts/values/bad-policy.example.test',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/storage/kv/namespaces/hosts/values/bad-state-egress.example.test',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/storage/kv/namespaces/hosts/values/malformed.example.test',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/dispatch/namespaces/fleet/scripts?per_page=1000',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/dispatch/namespaces/fleet/scripts/fleet-alpha',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/dispatch/namespaces/fleet/scripts/fleet-alpha/settings',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/dispatch/namespaces/fleet/scripts/fleet-broken',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/dispatch/namespaces/fleet/scripts/fleet-broken/settings',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/dispatch/namespaces/fleet/scripts/fleet-other-owner',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/dispatch/namespaces/fleet/scripts/fleet-other-owner/settings',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/dispatch/namespaces/fleet/scripts/fleet-drifted',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/dispatch/namespaces/fleet/scripts/fleet-drifted/settings',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/dispatch/namespaces/fleet',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/domains',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/user/tokens/verify',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/tokens/token-id',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/zones?account.id=account&per_page=50&type=full&type=partial&type=secondary&type=internal',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/zones?account.id=account&per_page=50&type=full&type=partial&type=secondary&type=internal&page=2',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/zones/zone-a/workers/routes',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/zones/zone-b/workers/routes',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-state/deployments',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-state/secrets',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-state/versions/version-fleet-state',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-state/subdomain',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-plain/deployments',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-plain/secrets',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-plain/versions/version-fleet-plain',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-plain/subdomain',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-egress/deployments',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-egress/secrets',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-egress/versions/version-fleet-egress',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-egress/subdomain',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-unreadable/deployments',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-unreadable/secrets',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-unreadable/versions/version-fleet-unreadable',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/scripts/fleet-unreadable/subdomain',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/d1/database',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/d1/database?page=2',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/durable_objects/namespaces',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/workers/durable_objects/namespaces?page=2',
    body: undefined,
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/r2/buckets?name_contains=fleet-&order=name&direction=asc&per_page=1000',
    body: undefined,
    headers: { 'cf-r2-jurisdiction': 'default' },
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/r2/buckets?name_contains=fleet-&order=name&direction=asc&per_page=1000',
    body: undefined,
    headers: { 'cf-r2-jurisdiction': 'eu' },
  },
  {
    method: 'GET',
    url: 'https://api.cloudflare.com/client/v4/accounts/account/r2/buckets?name_contains=fleet-&order=name&direction=asc&per_page=1000',
    body: undefined,
    headers: { 'cf-r2-jurisdiction': 'fedramp' },
  },
] as const satisfies readonly DrainRequestRecord[];

/** The exact inventory the drain returned. */
export const DRAIN_BASELINE_INVENTORY = {
  findings: [
    {
      tenantTag: 'gamma',
      environment: 'production',
      kind: 'stale-script-registration',
      detail:
        "script inventory key '__anchorage_script__:fleet-wrong-key' claims 'fleet-other-owner'",
    },
    {
      tenantTag: 'unknown',
      environment: 'unknown',
      kind: 'malformed-script-registration',
      detail:
        "fleet inventory key '__anchorage_script__:fleet-malformed' is not valid JSON",
    },
    {
      tenantTag: 'delta',
      environment: 'production',
      kind: 'malformed-script-registration',
      detail:
        "script inventory key '__anchorage_script__:fleet-incomplete' has incomplete ownership metadata",
    },
    {
      tenantTag: 'acme',
      environment: 'production',
      kind: 'malformed-route',
      detail:
        "host route 'bad-policy.example.test' has inconsistent policy metadata",
    },
    {
      tenantTag: 'acme',
      environment: 'production',
      kind: 'malformed-route',
      detail:
        "host route 'bad-state-egress.example.test' has invalid state-egress metadata",
    },
    {
      tenantTag: 'unknown',
      environment: 'unknown',
      kind: 'malformed-route',
      detail: "fleet inventory key 'malformed.example.test' is not valid JSON",
    },
    {
      tenantTag: 'beta',
      environment: 'staging',
      kind: 'stale-script-registration',
      detail:
        "registered script 'fleet-broken' could not be inspected: Error: script 'fleet-broken' has incomplete fleet metadata",
    },
    {
      tenantTag: 'gamma',
      environment: 'production',
      kind: 'stale-script-registration',
      detail:
        "registered script 'fleet-other-owner' is absent from the dispatch namespace listing",
    },
    {
      tenantTag: 'gamma',
      environment: 'production',
      kind: 'stale-script-registration',
      detail: "registered script 'fleet-other-owner' is missing",
    },
    {
      tenantTag: 'acme',
      environment: 'production',
      kind: 'stale-script-registration',
      detail:
        "registered script 'fleet-drifted' does not match its live tenant, environment, or database ownership",
    },
    {
      tenantTag: 'orphan',
      environment: 'production',
      kind: 'unknown-dispatch-scripts',
      detail:
        "dispatch script 'fleet-orphan' has no valid owner-checked registry entry",
    },
    {
      tenantTag: 'acme',
      environment: 'production',
      kind: 'stale-route',
      detail:
        "host route 'stale.example.test' does not match its script registration owner",
    },
    {
      tenantTag: 'unknown',
      environment: 'unknown',
      kind: 'trusted-dispatch-namespace',
      detail:
        "dispatch namespace 'fleet' does not attest trusted_workers=false",
    },
    {
      tenantTag: 'unknown',
      environment: 'unknown',
      kind: 'unknown-dispatch-scripts',
      detail:
        "dispatch namespace 'fleet' reports 1 script(s) missing from the paginated listing",
    },
    {
      tenantTag: 'acme',
      environment: 'production',
      kind: 'incomplete-deployment',
      detail:
        "trusted Worker 'fleet-state' is publicly reachable on workers.dev, a preview URL, or a zone route",
    },
    {
      tenantTag: 'acme',
      environment: 'production',
      kind: 'incomplete-deployment',
      detail:
        "trusted Worker 'fleet-egress' is publicly reachable on workers.dev, a preview URL, or a zone route",
    },
    {
      tenantTag: 'unknown',
      environment: 'unknown',
      kind: 'incomplete-deployment',
      detail:
        "plain Worker 'fleet-unreadable' could not be inventoried: Error: active Worker identity settings are missing",
    },
    {
      tenantTag: 'unknown',
      environment: 'unknown',
      kind: 'stale-route',
      detail:
        "custom domain 'ghost.example.test' points to a missing or incomplete plain Worker 'fleet-ghost'",
    },
    {
      tenantTag: 'acme',
      environment: 'production',
      kind: 'stale-route',
      detail:
        "zone route 'state.example.test/*' exposes plain Worker 'fleet-state'",
    },
    {
      tenantTag: 'plain',
      environment: 'staging',
      kind: 'stale-route',
      detail:
        "zone route 'plain.example.test/*' exposes plain Worker 'fleet-plain'",
    },
  ],
  hostRoutingKvId: 'hosts',
  dispatchScriptCount: 5,
  dispatchNamespace: {
    name: 'fleet',
    namespaceId: 'dispatch-namespace-id',
    trustedWorkers: true,
    scriptCount: 5,
  },
  scriptRegistrations: [
    {
      scriptName: 'fleet-alpha',
      tenantTag: 'acme',
      environment: 'production',
      databaseId: 'db-alpha',
      routeHostname: 'alpha.example.test',
    },
    {
      scriptName: 'fleet-broken',
      tenantTag: 'beta',
      environment: 'staging',
      databaseId: 'db-broken',
      routeHostname: 'broken.example.test',
    },
    {
      scriptName: 'fleet-other-owner',
      tenantTag: 'gamma',
      environment: 'production',
      databaseId: 'db-other-owner',
      routeHostname: 'other-owner.example.test',
    },
    {
      scriptName: 'fleet-drifted',
      tenantTag: 'acme',
      environment: 'production',
      databaseId: 'db-drifted',
      routeHostname: 'drifted.example.test',
    },
  ],
  deployments: [
    {
      backend: 'workers-for-platforms',
      scriptName: 'fleet-alpha',
      tenantTag: 'acme',
      environment: 'production',
      databaseIds: ['db-alpha'],
      durableObjectBindings: [
        {
          name: 'MAINTENANCE',
          className: 'Maintenance',
          namespaceId: 'ns-alpha',
        },
      ],
      serviceBindings: [{ name: 'EGRESS', service: 'fleet-egress' }],
      queueProducerBindings: [{ name: 'AUDIT', queueName: 'fleet-audit' }],
      r2BucketBindings: [
        {
          name: 'EXPORTS',
          bucketName: 'fleet-exports',
          jurisdiction: 'default',
        },
      ],
      plainTextBindings: { DEPLOYMENT_TENANT: 'acme' },
      secretNames: ['MAINTENANCE_ADMIN'],
      routeHostnames: ['alpha.example.test', 'stale.example.test'],
      artifactVersion: 'etag-fleet-alpha',
      desiredSpecDigest:
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      schemaVersion: 2,
    },
    {
      backend: 'workers-for-platforms',
      scriptName: 'fleet-drifted',
      tenantTag: 'acme',
      environment: 'production',
      databaseIds: ['db-drifted-live'],
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      r2BucketBindings: [],
      plainTextBindings: {},
      secretNames: [],
      routeHostnames: [],
      artifactVersion: 'etag-fleet-drifted',
      desiredSpecDigest:
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      schemaVersion: 2,
    },
    {
      backend: 'plain-worker',
      resourceRole: 'platform-state',
      resourceGroupId: 'policy-alpha',
      scriptName: 'fleet-state',
      tenantTag: 'acme',
      environment: 'production',
      databaseIds: ['db-plain'],
      durableObjectBindings: [
        { name: 'STATE', className: 'State', namespaceId: 'ns-state' },
      ],
      serviceBindings: [{ name: 'EGRESS', service: 'fleet-egress' }],
      queueProducerBindings: [{ name: 'AUDIT', queueName: 'fleet-audit' }],
      kvNamespaceBindings: [{ name: 'HOSTS', namespaceId: 'hosts' }],
      r2BucketBindings: [
        {
          name: 'EXPORTS',
          bucketName: 'fleet-alpha-exports',
          jurisdiction: 'default',
        },
      ],
      secretNames: ['STATE_CREDENTIAL'],
      plainTextBindings: {
        DEPLOYMENT_TENANT: 'acme',
        FLEET_ENVIRONMENT: 'production',
        FLEET_SCHEMA_VERSION: '2',
        FLEET_RESOURCE_ROLE: 'platform-state',
        FLEET_RESOURCE_GROUP: 'policy-alpha',
        FLEET_SPEC_DIGEST:
          'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      },
      routeHostnames: [],
      zoneRoutes: [
        {
          zoneId: 'zone-a',
          routeId: 'route-state',
          pattern: 'state.example.test/*',
        },
      ],
      artifactVersion: 'version-fleet-state',
      desiredSpecDigest:
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      schemaVersion: 2,
    },
    {
      backend: 'plain-worker',
      scriptName: 'fleet-plain',
      tenantTag: 'plain',
      environment: 'staging',
      databaseIds: ['db-plain'],
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      kvNamespaceBindings: [],
      r2BucketBindings: [],
      secretNames: [],
      plainTextBindings: {
        DEPLOYMENT_TENANT: 'plain',
        FLEET_ENVIRONMENT: 'staging',
        FLEET_SCHEMA_VERSION: '4',
      },
      routeHostnames: ['plain.example.test'],
      zoneRoutes: [
        {
          zoneId: 'zone-b',
          routeId: 'route-plain',
          pattern: 'plain.example.test/*',
        },
      ],
      artifactVersion: 'version-fleet-plain',
      schemaVersion: 4,
    },
    {
      backend: 'plain-worker',
      resourceRole: 'deployment-egress',
      resourceGroupId: 'policy-alpha',
      scriptName: 'fleet-egress',
      tenantTag: 'acme',
      environment: 'production',
      databaseIds: [],
      durableObjectBindings: [],
      serviceBindings: [],
      queueProducerBindings: [],
      kvNamespaceBindings: [],
      r2BucketBindings: [],
      secretNames: [],
      plainTextBindings: {
        DEPLOYMENT_TENANT: 'acme',
        FLEET_ENVIRONMENT: 'production',
        FLEET_SCHEMA_VERSION: '2',
        FLEET_RESOURCE_ROLE: 'deployment-egress',
        FLEET_RESOURCE_GROUP: 'policy-alpha',
      },
      routeHostnames: [],
      zoneRoutes: [],
      artifactVersion: 'version-fleet-egress',
      schemaVersion: 2,
    },
  ],
  databaseIds: ['db-alpha', 'db-plain'],
  namespaceIds: ['ns-alpha', 'ns-state'],
  r2Buckets: [
    {
      bucketName: 'fleet-alpha-exports',
      jurisdiction: 'default',
      creationDate: '2026-08-01T00:00:00.000Z',
    },
    {
      bucketName: 'fleet-eu-state',
      jurisdiction: 'eu',
      creationDate: '2026-08-03T00:00:00.000Z',
    },
  ],
  routes: [
    {
      backend: 'workers-for-platforms',
      surface: 'host-registry',
      hostname: 'alpha.example.test',
      scriptName: 'fleet-alpha',
      tenantTag: 'acme',
      environment: 'production',
      policyId: 'policy-alpha',
      policyHosts: ['api.example.com'],
      policyDigest:
        '045d08ae55129b208b03729375751e319888778ae2f9ab9af401234ca11b31b0',
      stateEgress: {
        resourceGroupId: 'policy-alpha',
        stateScriptName: 'fleet-state',
        credentialDigest:
          'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
      },
    },
    {
      backend: 'workers-for-platforms',
      surface: 'host-registry',
      hostname: 'stale.example.test',
      scriptName: 'fleet-alpha',
      tenantTag: 'acme',
      environment: 'production',
      policyId: 'policy-stale',
      policyHosts: [],
      policyDigest:
        'f863229329a0910143f8a475f09d04c5dbe5a0d5cbdda54c343d83616c7e8491',
    },
    {
      backend: 'plain-worker',
      surface: 'custom-domain',
      hostname: 'plain.example.test',
      scriptName: 'fleet-plain',
      tenantTag: 'plain',
      environment: 'staging',
    },
    {
      backend: 'plain-worker',
      surface: 'custom-domain',
      hostname: 'ghost.example.test',
      scriptName: 'fleet-ghost',
      tenantTag: 'unknown',
      environment: 'unknown',
    },
    {
      backend: 'plain-worker',
      surface: 'zone-route',
      zoneId: 'zone-a',
      routeId: 'route-state',
      hostname: 'state.example.test/*',
      scriptName: 'fleet-state',
      tenantTag: 'acme',
      environment: 'production',
    },
    {
      backend: 'plain-worker',
      surface: 'zone-route',
      zoneId: 'zone-b',
      routeId: 'route-plain',
      hostname: 'plain.example.test/*',
      scriptName: 'fleet-plain',
      tenantTag: 'plain',
      environment: 'staging',
    },
  ],
} as const satisfies FleetResourceInventory;
