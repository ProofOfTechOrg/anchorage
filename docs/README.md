# Anchorage documentation

Anchorage is a pair of open-source packages for Mastra applications on Cloudflare:

- **breakwater** enforces policy at agent and connector boundaries.
- **flowsafe** runs workflows and agents durably, turns suspensions into human approvals, and keeps long-running state tenant-safe.

Start with the task you need to complete. The generated API reference is useful after you understand the lifecycle and trust boundaries.

## Start here

| Goal | Read |
| --- | --- |
| Decide whether Anchorage fits your system | [Product boundaries](breakwater-purpose-and-boundaries.md) |
| Install the packages and reach a first gated run | [Getting started](getting-started.md) |
| Choose the correct package or export | [API reference map](api-reference.md) |
| See the system running | [Live demo](https://anchorage.proofoftech.org/) |

## Build guardrails

| Topic | What it covers |
| --- | --- |
| [Breakwater architecture](breakwater-architecture.md) | Processor, tool-boundary, audit, and tenancy responsibilities |
| [Policy engine](policy-engine-design.md) | Input/output channels, streaming behavior, content inspection, and tool policies |
| [Connector interface](connector-interface.md) | Permission manifests, grant enforcement, egress, idempotency, dry-run, rate limits, and stores |
| [Connector authoring guide](../packages/breakwater/CONNECTORS.md) | A complete connector implementation and test expectations |
| [Agent CLI connectors](agent-cli-connectors.md) | Claude Code and Codex as approval-gated workspace editors |

## Build durable approvals and agents

| Topic | What it covers |
| --- | --- |
| [Approval system](approval-system.md) | Queue lifecycle, REST API, separation of duties, grant derivation, live UI, and failure recovery |
| [Flowsafe architecture](flowsafe-architecture.md) | Components, tenant boundaries, storage, and host composition |
| [Durable Object runner](do-runner-design.md) | Run identity, D1 snapshots, restart behavior, concurrency, retention, and offboarding |
| [Durable agents](durable-agents.md) | Threads, memory, signals, goals, schedules, background tasks, providers, and restart resume |
| [Agent-memory tenancy](agent-memory-tenancy.md) | Tenant-minted thread/resource ids, host rejection, recall isolation, and TTL |

## Deploy and operate

| Topic | What it covers |
| --- | --- |
| [Deployment reference](deployment-reference.md) | Baseline and advanced host choices, bindings, secrets, routes, and scheduled duties |
| [Operations runbook](operations-runbook.md) | Local validation, production checks, retention, offboarding, and incident response |
| [Security threat model](security-threat-model.md) | Assets, trust boundaries, controls, residual risks, and deployment obligations |
| [Observability and quality](observability-and-quality.md) | Audit, metrics, notifications, SIEM export, and repository gates |

## Reference and examples

- [Generated API reference](https://proofoftechorg.github.io/anchorage/)
- [API reference map](api-reference.md)
- [breakwater package README](../packages/breakwater/README.md)
- [flowsafe package README](../packages/flowsafe/README.md)
- [Baseline Worker template](../packages/flowsafe/deploy/README.md)
- [Advanced agent starter](../packages/agent-starter/README.md)
- [Runnable workflow examples](examples/README.md)
- [Showcase application](../packages/showcase/README.md)

## Support and project policy

- [Support](../SUPPORT.md)
- [Security reporting](../SECURITY.md)
- [Contributing](../CONTRIBUTING.md)
- [Maintainer guide](maintainer-guide.md)
- [Changelog: breakwater](../packages/breakwater/CHANGELOG.md)
- [Changelog: flowsafe](../packages/flowsafe/CHANGELOG.md)

Documents under [`proposals/`](proposals/) describe uncommitted ideas. They are not supported product behavior. Shipped behavior is documented in the guides above and in exported types.
