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
        path: '^(?:packages/flowsafe/src|packages/agent-starter/(?:src|test|scripts)|scripts/architecture-fixtures)/',
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
        'These fleet-control modules must not reach the Cloudflare client; a back-import would restore the coupling the extraction removed. packages/fleet-control is outside no-new-architecture-cycles. tsPreCompilationDeps keeps type-only imports in the graph, so an `import type` back-edge is covered.',
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
      name: 'fleet-control-ports-do-not-reach-d1-adapter',
      severity: 'error',
      comment:
        'The D1 adapter implements ports that state-store.ts and migration-ledger.ts declare and imports state-store.ts, so a port module reaching the adapter would close a cycle.',
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
        'These modules are the Workers this package publishes plus the R2 export store, the D1 adapter, and the two leaf modules the R2 store imports. The two D1 harnesses set nodejs_compat, so a builtin import in the D1 adapter fails this rule rather than a harness; the R2 export harness runs without the flag.',
      from: {
        path: [
          '^packages/fleet-control/src/(?:d1-fleet-state-database|database-export-store|export-file-name|r2-export-store)\\.ts$',
          '^packages/fleet-control/src/workers/',
          '^scripts/architecture-fixtures/fleet-control-worker-reachable-imports-node-builtin\\.ts$',
        ],
      },
      to: { dependencyTypes: ['core'] },
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
