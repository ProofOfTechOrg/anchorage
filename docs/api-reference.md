# API reference map

The generated TypeDoc site is published at [proofoftechorg.github.io/anchorage](https://proofoftechorg.github.io/anchorage/). This page explains which import path to use before you open the symbol reference.

Use public package exports only. Paths under `src/` and generated `dist/` files are implementation details and may change without a compatibility guarantee.

## breakwater exports

| Import path | Use it for |
| --- | --- |
| `@proofoftech/breakwater` | Convenience barrel for guarded agents, policy engine, RBAC, audit, connector SDK, and agent CLI adapters |
| `@proofoftech/breakwater/agent` | `createGuardedAgent`, its narrow invocation handle, validated application processor contracts, and guarded-handle validation |
| `@proofoftech/breakwater/policy-engine` | `PolicyEngine`, content inspection, output channels, hold-back, and tool evaluators |
| `@proofoftech/breakwater/rbac` | Actor roles, request-context lookup, `RBACMiddleware`, and the permission identifier/projection contract |
| `@proofoftech/breakwater/audit` | `AuditLogger`, metrics adapter, sink fan-out, and audit types |
| `@proofoftech/breakwater/connector-sdk` | `createConnector`, manifests, context keys, guarded fetch, D1/in-memory idempotency and rate-limit stores |
| `@proofoftech/breakwater/agent-cli` | Generic, Claude Code, and Codex CLI connectors plus injected execution types |

The root barrel re-exports every supported breakwater module. Prefer the
`agent-cli` subpath when a Node process boundary is part of the integration so
that requirement is visible at the import site. The adapters also work with an
injected `AgentCliExec` in another server runtime.

## flowsafe root export

`@proofoftech/flowsafe` is the compatibility barrel for the original workflow and approval surface. It mirrors:

- `@proofoftech/flowsafe/approval-api`
- `@proofoftech/flowsafe/do-runner`
- `@proofoftech/flowsafe/artifacts`
- `@proofoftech/flowsafe/audit-export`

New host-side and React features remain subpath-only so importing the root does not pull durable-agent or UI dependencies into every consumer.

## flowsafe subpath exports

| Import path | Use it for |
| --- | --- |
| `@proofoftech/flowsafe/agent-host` | Server-only guarded-agent catalogs, authenticated start/status/NDJSON routes, thread hosting, and approval-only resume |
| `@proofoftech/flowsafe/agent-runner` | Runtime-driven Mastra durable agents, approval suspend parsing, and restart resume |
| `@proofoftech/flowsafe/approval-api` | Records, actor resolver, deployment store, service, router, grant provider, retention, SLA, notifications, and stream events |
| `@proofoftech/flowsafe/background-tasks` | D1 task domains, deployment task host, routes, and terminal-task purge |
| `@proofoftech/flowsafe/approval-ui` | React dashboard, client, headless hook, component slots, and live transport |
| `@proofoftech/flowsafe/artifacts` | R2 artifact store and in-memory bucket |
| `@proofoftech/flowsafe/audit-export` | Queue producer sink and NDJSON SIEM consumer |
| `@proofoftech/flowsafe/do-runner` | Runtime, Durable Object classes, D1 storage, deployment sentinel and caller attestation, identity helpers, pub/sub, retention, and run summaries |
| `@proofoftech/flowsafe/goals` | Objective HTTP router and goal request-context contract |
| `@proofoftech/flowsafe/host-kit` | Authenticator and verifier seams, run/thread/hub/provider topologies, routes, approval bridges, tickets, and composed Worker |
| `@proofoftech/flowsafe/host-kit/module` | Workflow-module interface for import-safe host registration |
| `@proofoftech/flowsafe/schedules` | D1 schedule domain, deployment router, reserved-context guard, and CAS tick |
| `@proofoftech/flowsafe/signal-providers` | Provider adapters, host Durable Object, topology, subscriptions, verified webhooks, and GitHub provider |
| `@proofoftech/flowsafe/signals` | D1 signal domains, thread routes, ingress router, notification dispatch, and client |
| `@proofoftech/flowsafe/signals/client` | DOM-free `SignalClient` without host-side signal code |

## Browser and server boundaries

Safe browser imports:

- `@proofoftech/flowsafe/approval-ui`
- `@proofoftech/flowsafe/signals/client`
- `ApprovalApiClient` and structural client types

Server or Worker imports:

- breakwater processors and connector SDK
- flowsafe approval API, agent host, runner, host kit, agents, schedules, signals, providers, tasks, artifacts, and audit export

Node-only execution:

- the default `@proofoftech/breakwater/agent-cli` runner

The agent CLI module can be constructed in another runtime when you inject `AgentCliExec`, but the built-in process runner requires Node's `child_process`.

## Optional peers

| Peer | Required by |
| --- | --- |
| `@mastra/core` | Both packages |
| `@proofoftech/breakwater` | flowsafe `agent-host`, host-kit module types, and grant-protected connector integrations |
| `react` and `react-dom` | flowsafe approval UI only |
| `wrangler` `>=4 <5` | flowsafe `flowsafe-provision` CLI only |

Flowsafe does not impose a CSS or design-system dependency. Its default approval views render semantic HTML.

## Compatibility policy

Both packages are pre-1.0. Public exported types, documented behavior, and package subpaths are the compatibility surface. Changesets describe each release. A breaking change can occur in a minor version before 1.0 and will be called out in the changelog.

The repository runs a non-blocking canary against the newest Mastra 1.x. The declared peer range, not a passing canary, defines supported compatibility.

## Source guides

- [breakwater README](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/README.md)
- [flowsafe README](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/flowsafe/README.md)
- [Connector authoring](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/CONNECTORS.md)
- [Getting started](getting-started.md)
- [Approval system](approval-system.md)
- [Durable agents](durable-agents.md)
