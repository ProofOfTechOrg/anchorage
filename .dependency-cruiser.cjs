const FLOWSAFE_PUBLIC_ENTRY =
  '^packages/flowsafe/src/(?:index|host-kit/index|agent-runner/index|signals/client)\\.ts$';
// `principal-identity` is the import-free half of `principal` (the kind list
// and the two identity predicates). It is admitted here for the same reason the
// others are, and is strictly leafier than any of them: it imports nothing at
// all, so it cannot widen what do-runner reaches through approval-api.
const ALLOWED_APPROVAL_API_LEAVES =
  '^packages/flowsafe/src/approval-api/(?:principal-identity|principal|contract|types)\\.ts$';
// NOT extended with `principal-identity`: this is the exception list for the
// one tolerated import cycle, and a module with no imports can never be in one.
const KNOWN_APPROVAL_API_CYCLE =
  '^packages/flowsafe/src/approval-api/(?:principal|contract|types)\\.ts$';

/** @type {import('dependency-cruiser').IConfiguration} */
module.exports = {
  forbidden: [
    {
      name: 'flowsafe-public-entry-no-agent-host',
      severity: 'error',
      comment:
        'Public, host-kit, runner, and signals-client entrypoints must stay independent of the optional agent host.',
      from: {
        path: [
          FLOWSAFE_PUBLIC_ENTRY,
          '^scripts/architecture-fixtures/public-entry-imports-agent-host\\.ts$',
        ],
      },
      to: {
        path: '^packages/flowsafe/src/agent-host/',
        reachable: true,
      },
    },
    {
      name: 'flowsafe-public-entry-no-breakwater',
      severity: 'error',
      comment:
        'The same entrypoints must not transitively acquire the optional Breakwater peer.',
      from: {
        path: [
          FLOWSAFE_PUBLIC_ENTRY,
          '^scripts/architecture-fixtures/public-entry-imports-breakwater\\.ts$',
        ],
      },
      to: {
        path: '^(?:@proofoftech/breakwater(?:/|$)|packages/breakwater/)',
        reachable: true,
      },
    },
    {
      name: 'do-runner-approval-api-leaves-only',
      severity: 'error',
      comment:
        'do-runner may reach approval-api only through principal.ts, its import-free principal-identity.ts half, contract.ts, and their type-only types.ts leaf.',
      from: {
        path: [
          '^packages/flowsafe/src/do-runner/index\\.ts$',
          '^scripts/architecture-fixtures/do-runner-imports-approval-router\\.ts$',
        ],
      },
      to: {
        path: '^packages/flowsafe/src/approval-api/',
        pathNot: ALLOWED_APPROVAL_API_LEAVES,
        reachable: true,
      },
    },
    {
      name: 'host-kit-no-durable-agent',
      severity: 'error',
      comment:
        'The host-kit barrel uses the pure approval-shapes leaf and must not pull Mastra durable Agent Node built-ins.',
      from: {
        path: [
          '^packages/flowsafe/src/host-kit/index\\.ts$',
          '^scripts/architecture-fixtures/host-kit-imports-durable-agent\\.ts$',
        ],
      },
      to: {
        path: '^(?:@mastra/core/agent/durable|node_modules/.*/@mastra/core/.*/agent/durable)',
        reachable: true,
      },
    },
    {
      name: 'host-kit-no-breakwater',
      severity: 'error',
      comment:
        'Breakwater belongs to the separate host-kit/module authoring subpath, not the route-hosting barrel.',
      from: {
        path: [
          '^packages/flowsafe/src/host-kit/index\\.ts$',
          '^scripts/architecture-fixtures/host-kit-imports-breakwater\\.ts$',
        ],
      },
      to: {
        path: '^(?:@proofoftech/breakwater(?:/|$)|packages/breakwater/)',
        reachable: true,
      },
    },
    {
      name: 'flowsafe-architecture-resolves',
      severity: 'error',
      comment:
        'An unresolved edge can truncate a reachable graph and make an isolation rule pass vacuously.',
      from: {
        path: [
          '^packages/flowsafe/src/',
          '^scripts/architecture-fixtures/unresolved-import\\.ts$',
        ],
      },
      to: { couldNotResolve: true },
    },
    {
      name: 'agent-starter-no-private-bare-entrypoints',
      severity: 'error',
      comment:
        'Starter code imports documented package exports, never src/dist entrypoints or repository-root source paths.',
      from: {
        path: [
          '^packages/agent-starter/(?:src|test|scripts)/',
          '^scripts/architecture-fixtures/starter-imports-private-entrypoint\\.ts$',
        ],
      },
      to: {
        path: '^(?:@proofoftech/(?:flowsafe|breakwater)/(?:src|dist)/|packages/(?:flowsafe|breakwater)/src/)',
      },
    },
    {
      name: 'agent-starter-no-relative-package-reaches',
      severity: 'error',
      comment:
        'Starter code must not bypass package exports with a relative edge into a sibling package.',
      from: {
        path: [
          '^packages/agent-starter/(?:src|test|scripts)/',
          '^scripts/architecture-fixtures/starter-reaches-flowsafe-source\\.ts$',
        ],
      },
      to: {
        path: '^packages/(?:flowsafe|breakwater)/',
        dependencyTypes: ['local'],
      },
    },
    {
      name: 'fleet-control-is-control-plane-only',
      severity: 'error',
      comment:
        'Fleet control holds account credentials, routing ownership, and tenant lifecycle. Publishing it removed the registry barrier, so no other package may reach it, by bare name or by any subpath. Stated as everything-except rather than an allowlist of today packages, and architecture:check:rules cruises packages as ONE root, so a new package or source directory is covered the day it lands. Showcase and the flowsafe deploy template alias imports through bundler config this file does not resolve, so coverage is per-module direct-import rather than transitive.',
      from: {
        path: [
          '^packages/',
          '^scripts/architecture-fixtures/data-plane-imports-fleet-control\\.ts$',
        ],
        pathNot: '^packages/fleet-control/',
      },
      to: {
        path: '^(?:@proofoftech/fleet-control(?:/|$)|packages/fleet-control/)',
        reachable: true,
      },
    },
    {
      name: 'no-new-architecture-cycles',
      severity: 'error',
      comment:
        'The principal-contract-types type cycle is the sole existing exception; any cycle involving another module fails.',
      from: {
        path: '^(?:packages/flowsafe/src|packages/fleet-control/src|packages/agent-starter/(?:src|test|scripts)|scripts/architecture-fixtures)/',
      },
      to: {
        circular: true,
        via: { pathNot: KNOWN_APPROVAL_API_CYCLE },
      },
    },
    {
      name: 'fleet-control-client-layers-are-one-way',
      severity: 'error',
      comment:
        'These fleet-control modules must not reach the Cloudflare client; a back-import would restore the coupling the extraction removed. The general cycle rule also covers Fleet Control, and tsPreCompilationDeps keeps type-only imports in the graph.',
      from: {
        path: [
          '^packages/fleet-control/src/',
          '^scripts/architecture-fixtures/fleet-control-leaf-imports-client\\.ts$',
        ],
        pathNot:
          '^packages/fleet-control/src/(?:cloudflare-api-plain-worker-backend|cloudflare-api-plain-worker-provisioning-api|cloudflare-client|index)\\.ts$',
      },
      to: {
        path: '^packages/fleet-control/src/cloudflare-client\\.ts$',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-decommission-state-does-not-reach-provider',
      severity: 'error',
      comment:
        'Persisted decommission state is a provider-free authority boundary. Keeping provider clients, operations, and error classification out of its reachable graph prevents the Fleet D1 codec from acquiring credential or transport dependencies.',
      from: {
        path: [
          '^packages/fleet-control/src/(?:strict-plain-data|cloudflare-worker-attachment-scan-state|decommission-intent|decommission-advance|state-store)\\.ts$',
          '^scripts/architecture-fixtures/decommission-state-imports-provider\\.ts$',
        ],
      },
      to: {
        path: '(?:^packages/fleet-control/src/(?:cloudflare-worker-attachment-scan|cloudflare-client|cloudflare-ordinary-worker-operations|cloudflare-provider-errors)\\.ts$|^cloudflare(?:/|$)|(?:^|/)node_modules/(?:\\.pnpm/)?cloudflare(?:@|/))',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-cleanup-state-does-not-reach-provider',
      severity: 'error',
      comment:
        'Persisted cleanup state is a provider-free authority boundary. Keeping provider clients, operations, and error classification out of its reachable graph prevents the cleanup codec and eligibility classifier from acquiring credential or transport dependencies.',
      from: {
        path: [
          '^packages/fleet-control/src/cleanup-intent\\.ts$',
          '^scripts/architecture-fixtures/cleanup-state-imports-provider\\.ts$',
        ],
      },
      to: {
        path: '(?:^packages/fleet-control/src/(?:cloudflare-worker-attachment-scan|cloudflare-client|cloudflare-ordinary-worker-operations|cloudflare-provider-errors)\\.ts$|^cloudflare(?:/|$)|(?:^|/)node_modules/(?:\\.pnpm/)?cloudflare(?:@|/))',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-decommission-advance-is-transport-neutral',
      severity: 'error',
      comment:
        'The bounded decommission coordinator depends only on provider-neutral ports and state, including the provider-neutral database receipt port. Keeping provider clients, Wrangler, concrete export stores, root barrels, and unbounded lifecycle coordinators out of its reachable graph preserves the Worker-safe transport boundary.',
      from: {
        path: [
          '^packages/fleet-control/src/decommission-advance\\.ts$',
          '^scripts/architecture-fixtures/decommission-advance-imports-provider\\.ts$',
        ],
      },
      to: {
        path: '(?:^packages/fleet-control/src/(?:backend-switch|cloudflare-worker-attachment-scan|cloudflare-client|cloudflare-ordinary-worker-operations|cloudflare-provider-errors|workers-for-platforms-backend-switch-provider|wrangler-plain-worker-provisioning-api|wrangler-loop-backend|wrangler-runner|export-file-name|export-store|r2-export-store|provision|fleet|index)\\.ts$|^packages/fleet-control/src/workers/|^cloudflare(?:/|$)|(?:^|/)node_modules/(?:\\.pnpm/)?cloudflare(?:@|/))',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-cleanup-advance-is-transport-neutral',
      severity: 'error',
      comment:
        'The bounded cleanup coordinator depends only on provider-neutral ports and state. Keeping provider clients, Wrangler, concrete export stores, root barrels, and unbounded lifecycle coordinators out of its reachable graph preserves the transport-neutral boundary.',
      from: {
        path: [
          '^packages/fleet-control/src/cleanup-advance\\.ts$',
          '^scripts/architecture-fixtures/cleanup-advance-imports-provider\\.ts$',
        ],
      },
      to: {
        path: '(?:^packages/fleet-control/src/(?:backend-switch|cloudflare-worker-attachment-scan|cloudflare-client|cloudflare-ordinary-worker-operations|cloudflare-provider-errors|workers-for-platforms-backend-switch-provider|wrangler-plain-worker-provisioning-api|wrangler-loop-backend|wrangler-runner|export-file-name|export-store|r2-export-store|provision|fleet|index)\\.ts$|^packages/fleet-control/src/workers/|^cloudflare(?:/|$)|(?:^|/)node_modules/(?:\\.pnpm/)?cloudflare(?:@|/))',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-decommission-database-is-provider-neutral',
      severity: 'error',
      comment:
        'The shared bounded-D1 choreography is a provider-neutral runtime leaf. It may import only the database receipt port and strict plain-data guard at runtime; provider shapes remain type-only callback contracts.',
      from: {
        path: [
          '^packages/fleet-control/src/decommission-database\\.ts$',
          '^scripts/architecture-fixtures/decommission-database-imports-provider\\.ts$',
        ],
      },
      to: {
        path: '.*',
        pathNot:
          '^packages/fleet-control/src/(?:database-export-store|strict-plain-data)\\.ts$',
        dependencyTypesNot: ['type-only', 'type-import'],
      },
    },
    {
      name: 'fleet-control-backend-switch-does-not-reach-its-provider',
      severity: 'error',
      comment:
        'The root switch coordinator depends on provider-neutral ports. It must not reach the concrete Workers for Platforms switch provider, which implements those ports over Cloudflare transports.',
      from: {
        path: [
          '^packages/fleet-control/src/backend-switch\\.ts$',
          '^scripts/architecture-fixtures/decommission-database-imports-provider\\.ts$',
        ],
      },
      to: {
        path: '^packages/fleet-control/src/workers-for-platforms-backend-switch-provider\\.ts$',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-strict-plain-data-is-import-free',
      severity: 'error',
      comment:
        'The descriptor-safe plain-data guard is shared by persisted codecs and must remain an import-free leaf so validation cannot execute package code before it rejects hostile input.',
      from: {
        path: [
          '^packages/fleet-control/src/strict-plain-data\\.ts$',
          '^scripts/architecture-fixtures/decommission-state-imports-provider\\.ts$',
        ],
      },
      to: {
        path: '.*',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-ports-do-not-reach-d1-adapter',
      severity: 'error',
      comment:
        'The D1 adapter implements ports that state-store.ts and migration-ledger.ts declare and imports state-store.ts, which reaches migration-ledger.ts through backend-switch.ts, so a port module reaching the adapter would close a cycle.',
      from: {
        path: [
          '^packages/fleet-control/src/(?:state-store|migration-ledger)\\.ts$',
          '^scripts/architecture-fixtures/fleet-control-port-imports-d1-adapter\\.ts$',
        ],
      },
      to: {
        path: '^packages/fleet-control/src/d1-fleet-state-database\\.ts$',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-worker-reachable-modules-avoid-node-builtins',
      severity: 'error',
      comment:
        'These modules are Worker entry points or are reached from one in the import graph, where a Node builtin needs nodejs_compat. The two D1 harnesses set nodejs_compat, so a builtin import in the D1 adapter fails this rule rather than a harness; the R2 export harness runs without the flag.',
      from: {
        path: [
          '^packages/fleet-control/src/(?:d1-fleet-state-database|database-export-store|export-file-name|r2-export-store)\\.ts$',
          '^packages/fleet-control/src/workers/',
          '^scripts/architecture-fixtures/fleet-control-worker-reachable-imports-node-builtin\\.ts$',
        ],
      },
      to: { dependencyTypes: ['core'] },
    },
    {
      name: 'fleet-control-client-does-not-reach-its-consumers',
      severity: 'error',
      comment:
        'index.ts, cloudflare-api-plain-worker-backend.ts, and cloudflare-api-plain-worker-provisioning-api.ts import the Cloudflare client, so the client reaching one of them would close a cycle. The one-way rule and the general Fleet Control cycle rule both reject that reverse reach.',
      from: {
        path: [
          '^packages/fleet-control/src/cloudflare-client\\.ts$',
          '^scripts/architecture-fixtures/fleet-control-client-imports-consumer\\.ts$',
        ],
      },
      to: {
        path: '^packages/fleet-control/src/(?:cloudflare-api-plain-worker-backend|cloudflare-api-plain-worker-provisioning-api|index)\\.ts$',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-export-port-does-not-reach-adapters',
      severity: 'error',
      comment:
        'export-store.ts and r2-export-store.ts import DurableDatabaseExportStore from database-export-store.ts to implement it, so the port reaching either store would close a cycle. Those two imports are type-only, and tsPreCompilationDeps keeps a type-only edge in the graph.',
      from: {
        path: [
          '^packages/fleet-control/src/database-export-store\\.ts$',
          '^scripts/architecture-fixtures/fleet-control-export-port-imports-adapter\\.ts$',
        ],
      },
      to: {
        path: '^packages/fleet-control/src/(?:export-store|r2-export-store)\\.ts$',
        reachable: true,
      },
    },
  ],
  required: [
    {
      name: 'host-kit-reaches-approval-bridge',
      severity: 'error',
      comment:
        'Positive reachability prevents the host-kit isolation graph from silently shrinking before the bridge.',
      module: {
        path: [
          '^packages/flowsafe/src/host-kit/index\\.ts$',
          '^scripts/architecture-fixtures/host-kit-misses-approval-bridge\\.ts$',
        ],
      },
      to: {
        path: '^packages/flowsafe/src/host-kit/approval-bridge\\.ts$',
        reachable: true,
      },
    },
    {
      name: 'host-kit-reaches-approval-shapes',
      severity: 'error',
      comment:
        'The barrel must continue reaching the pure approval-shapes leaf instead of the durable agent barrel.',
      module: {
        path: [
          '^packages/flowsafe/src/host-kit/index\\.ts$',
          '^scripts/architecture-fixtures/host-kit-misses-approval-shapes\\.ts$',
        ],
      },
      to: {
        path: '^packages/flowsafe/src/agent-runner/approval-shapes\\.ts$',
        reachable: true,
      },
    },
  ],
  options: {
    tsConfig: { fileName: 'tsconfig.base.json' },
    tsPreCompilationDeps: true,
    detectProcessBuiltinModuleCalls: true,
    enhancedResolveOptions: {
      exportsFields: ['exports'],
      conditionNames: ['types', 'import', 'node', 'default'],
      mainFields: ['types', 'module', 'main'],
    },
    doNotFollow: {
      path: '^(?:node_modules/|packages/(?:breakwater|flowsafe)/dist/|@proofoftech/)',
    },
    skipAnalysisNotInRules: true,
    progress: { type: 'none' },
  },
};
