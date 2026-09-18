const { builtinModules } = require('node:module');

const CLOUDFLARE_CONTROL_PLANE_ENTRY =
  '^packages/fleet-control/src/cloudflare-control-plane\\.ts$';
const CLOUDFLARE_FORBIDDEN_CORE = `^(?:node:(?!(?:crypto|async_hooks)$).+|${[
  ...new Set(builtinModules.map((name) => name.replace(/^node:/, ''))),
]
  .filter((name) => name !== 'crypto' && name !== 'async_hooks')
  .map((name) => name.replace(/[.*+?^$(){}|[\]\\]/g, '\\$&'))
  .join('|')})$`;

const FLOWSAFE_PUBLIC_ENTRY =
  '^packages/flowsafe/src/(?:index|host-kit/index|agent-runner/index|signals/client)\\.ts$';
const ALLOWED_APPROVAL_API_LEAVES =
  '^packages/flowsafe/src/approval-api/(?:principal-identity|principal|contract|types)\\.ts$';
const KNOWN_APPROVAL_API_CYCLE =
  '^packages/flowsafe/src/approval-api/(?:principal|contract|types)\\.ts$';

/** @type {import('dependency-cruiser').IConfiguration} */
module.exports = {
  forbidden: [
    {
      name: 'fleet-control-worker-entry-avoids-node-host-adapters',
      severity: 'error',
      from: {
        path: [
          CLOUDFLARE_CONTROL_PLANE_ENTRY,
          '^scripts/architecture-fixtures/control-plane-imports-node-host\\.ts$',
        ],
      },
      to: {
        path: '^packages/fleet-control/src/(?:export-store|wrangler-loop-backend|wrangler-plain-worker-provisioning-api|wrangler-runner)\\.ts$',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-worker-entry-limits-core-imports',
      severity: 'error',
      from: {
        path: [
          CLOUDFLARE_CONTROL_PLANE_ENTRY,
          '^scripts/architecture-fixtures/control-plane-imports-forbidden-core\\.ts$',
        ],
      },
      to: {
        path: CLOUDFLARE_FORBIDDEN_CORE,
        reachable: true,
      },
    },
    {
      name: 'flowsafe-public-entry-no-agent-host',
      severity: 'error',
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
      comment: 'Mastra durable Agent dependencies require Node built-ins.',
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
      comment: 'Breakwater belongs to the separate module-authoring subpath.',
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
        'Fleet control holds account credentials, routing ownership, and tenant lifecycle authority. Showcase and the Flowsafe deploy template use bundler aliases that this resolver cannot follow.',
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
      from: {
        path: ['^packages/', '^scripts/architecture-fixtures/'],
      },
      to: {
        circular: true,
        via: { pathNot: KNOWN_APPROVAL_API_CYCLE },
      },
    },
    {
      name: 'fleet-control-client-layers-are-one-way',
      severity: 'error',
      from: {
        path: [
          '^packages/fleet-control/src/',
          '^scripts/architecture-fixtures/fleet-control-leaf-imports-client\\.ts$',
        ],
        pathNot:
          '^packages/fleet-control/src/(?:cloudflare-api-plain-worker-backend|cloudflare-api-plain-worker-provisioning-api|cloudflare-client|cloudflare-control-plane|index)\\.ts$',
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
        'Persisted state codecs must not acquire credentials or transport dependencies.',
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
      name: 'fleet-control-inventory-state-does-not-reach-provider',
      severity: 'error',
      from: {
        path: [
          '^packages/fleet-control/src/(?:fleet-inventory-state|d1-fleet-inventory-run-store)\\.ts$',
          '^scripts/architecture-fixtures/inventory-state-imports-provider\\.ts$',
        ],
      },
      to: {
        path: '(?:^packages/fleet-control/src/(?:cloudflare-worker-attachment-scan|cloudflare-client|cloudflare-ordinary-worker-operations|cloudflare-provider-errors)\\.ts$|^cloudflare(?:/|$)|(?:^|/)node_modules/(?:\\.pnpm/)?cloudflare(?:@|/))',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-operation-state-does-not-reach-provider',
      severity: 'error',
      from: {
        path: [
          '^packages/fleet-control/src/(?:fleet-operation-state|fleet-audit-state|fleet-migration-state|d1-fleet-operation-store)\\.ts$',
          '^scripts/architecture-fixtures/operation-state-imports-provider\\.ts$',
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
      from: {
        path: [
          '^packages/fleet-control/src/decommission-advance\\.ts$',
          '^scripts/architecture-fixtures/decommission-advance-imports-provider\\.ts$',
        ],
      },
      to: {
        path: '(?:^packages/fleet-control/src/(?:backend-switch|cloudflare-worker-attachment-scan|cloudflare-client|cloudflare-ordinary-worker-operations|cloudflare-provider-errors|workers-for-platforms-backend-switch-provider|wrangler-plain-worker-provisioning-api|wrangler-loop-backend|wrangler-runner|export-file-name|export-store|r2-export-store|d1-fleet-state-database|provision|fleet|index)\\.ts$|^packages/fleet-control/src/workers/|^cloudflare(?:/|$)|(?:^|/)node_modules/(?:\\.pnpm/)?cloudflare(?:@|/))',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-inventory-advance-is-transport-neutral',
      severity: 'error',
      from: {
        path: [
          '^packages/fleet-control/src/fleet-inventory-advance\\.ts$',
          '^scripts/architecture-fixtures/inventory-advance-imports-provider\\.ts$',
        ],
      },
      to: {
        path: '(?:^packages/fleet-control/src/(?:backend-switch|cloudflare-fleet-inventory|cloudflare-worker-attachment-scan|cloudflare-client|cloudflare-ordinary-worker-operations|cloudflare-provider-errors|workers-for-platforms-backend-switch-provider|wrangler-plain-worker-provisioning-api|wrangler-loop-backend|wrangler-runner|export-file-name|export-store|r2-export-store|d1-fleet-state-database|provision|fleet|index)\\.ts$|^packages/fleet-control/src/workers/|^cloudflare(?:/|$)|(?:^|/)node_modules/(?:\\.pnpm/)?cloudflare(?:@|/))',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-operation-advance-avoids-concrete-transports',
      severity: 'error',
      comment:
        'The reachable graph includes SDK types used by provider-neutral ports. Runtime SDK imports need a separate direct-edge rule because reachable restrictions cannot exempt erased edges.',
      from: {
        path: [
          '^packages/fleet-control/src/(?:fleet-audit-advance|fleet-migration-advance)\\.ts$',
          '^scripts/architecture-fixtures/operation-advance-imports-provider\\.ts$',
        ],
      },
      to: {
        path: '(?:^packages/fleet-control/src/(?:cloudflare-fleet-inventory|cloudflare-worker-attachment-scan|cloudflare-client|cloudflare-ordinary-worker-operations|cloudflare-provider-errors|cloudflare-rate-coordinator|workers-for-platforms-backend-switch-provider|workers-for-platforms-backend|plain-worker-backend|cloudflare-api-plain-worker-backend|wrangler-plain-worker-provisioning-api|cloudflare-api-plain-worker-provisioning-api|wrangler-loop-backend|wrangler-runner|export-file-name|export-store|r2-export-store|d1-fleet-state-database|index)\\.ts$|^packages/fleet-control/src/workers/)',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-runtime-sdk-stays-in-provider-modules',
      severity: 'error',
      comment:
        'A direct-edge restriction can exempt erased SDK types without allowing runtime SDK values through the type-inclusive reachable graph.',
      from: {
        path: [
          '^packages/fleet-control/src/',
          '^scripts/architecture-fixtures/runtime-sdk-import\\.ts$',
        ],
        pathNot:
          '^packages/fleet-control/src/(?:cloudflare-client|cloudflare-ordinary-worker-operations|cloudflare-provider-errors)\\.ts$',
      },
      to: {
        path: '(?:^cloudflare(?:/|$)|(?:^|/)node_modules/(?:\\.pnpm/)?cloudflare(?:@|/))',
        dependencyTypesNot: ['type-only', 'type-import'],
      },
    },
    {
      name: 'fleet-control-cleanup-advance-is-transport-neutral',
      severity: 'error',
      from: {
        path: [
          '^packages/fleet-control/src/cleanup-advance\\.ts$',
          '^scripts/architecture-fixtures/cleanup-advance-imports-provider\\.ts$',
        ],
      },
      to: {
        path: '(?:^packages/fleet-control/src/(?:backend-switch|cloudflare-worker-attachment-scan|cloudflare-client|cloudflare-ordinary-worker-operations|cloudflare-provider-errors|workers-for-platforms-backend-switch-provider|wrangler-plain-worker-provisioning-api|wrangler-loop-backend|wrangler-runner|export-file-name|export-store|r2-export-store|d1-fleet-state-database|provision|fleet|index)\\.ts$|^packages/fleet-control/src/workers/|^cloudflare(?:/|$)|(?:^|/)node_modules/(?:\\.pnpm/)?cloudflare(?:@|/))',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-decommission-database-is-provider-neutral',
      severity: 'error',
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
        'The concrete provider implements the coordinator ports; reverse reach couples coordination to its transport.',
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
        'Validation must not execute package code before rejecting hostile input.',
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
        'Binding adapters depend on the ports; store implementations accept injected databases.',
      from: {
        path: [
          '^packages/fleet-control/src/(?:state-store|migration-ledger|d1-fleet-inventory-run-store|d1-fleet-operation-store)\\.ts$',
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
        'Node built-ins require nodejs_compat. A harness with that flag can mask an incompatible import for consumers without it.',
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
      from: {
        path: [
          '^packages/fleet-control/src/cloudflare-client\\.ts$',
          '^scripts/architecture-fixtures/fleet-control-client-imports-consumer\\.ts$',
        ],
      },
      to: {
        path: '^packages/fleet-control/src/(?:cloudflare-api-plain-worker-backend|cloudflare-api-plain-worker-provisioning-api|cloudflare-control-plane|index)\\.ts$',
        reachable: true,
      },
    },
    {
      name: 'fleet-control-export-port-does-not-reach-adapters',
      severity: 'error',
      comment:
        'Adapters implement the port; reverse reach introduces a dependency cycle, including through type imports.',
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
