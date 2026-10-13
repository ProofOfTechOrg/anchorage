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
