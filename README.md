# Anchorage

Enterprise safety layer for AI agent workflows — open-source. Built on
[Mastra](https://mastra.ai), not competing with it.

**Status:** breakwater's
processor chain (`PolicyEngine`, `RBACMiddleware`, `AuditLogger`) and connector
SDK (`createConnector()` with egress, write-approval, idempotency, dry-run, and
rate-limit gates) are built and tested; flowsafe's Durable Object runner + D1
adapter run a real Mastra workflow on Workers + DO, suspend at an approval step,
survive a dev-server restart, and resume from the D1 snapshot; and flowsafe's
approval queue closes the loop — a suspension queues a D1-backed approval, a
reviewer decision (role-checked, CAS-guarded, audited, SLA-tracked) resumes the
run through its DO, and the breakwater connector grant is derived from approved
records at resume time, so forged resumes fail closed. A React dashboard ships
as `@proofoftech/flowsafe/approval-ui`. Anchorage also includes a copy-ready
production deploy template (`packages/flowsafe/deploy/`, with cron-owned SLA
sweep + retention purge), Cloudflare Queues → SIEM audit export, R2 artifact
storage, and Claude Code / Codex CLIs as approval-gated connectors. See
[`docs/`](docs/) for the architecture and design specification.

## What It Is

Mastra covers the runtime — workflows, agents, memory, RAG, observability.
Anchorage adds the enterprise controls teams need on top: RBAC, audit logs,
approval management, policy enforcement, and Cloudflare-native durable
execution — as Mastra middleware.

## Packages

| Package | Purpose | Package name |
|---------|---------|--------------|
| **breakwater** | Safety middleware — policy engine, RBAC + audit, connector SDK | `@proofoftech/breakwater` |
| **flowsafe** | Approval UX + durable execution — approval API/dashboard, Cloudflare DO workflow runner | `@proofoftech/flowsafe` |

Source-only today — clone and build (see [Quick Start](#quick-start)); not yet published to npm.

### breakwater — safety layer

Plugs into Mastra as processors (`@mastra/core/processors`) and tool/workflow
wrappers:

- **Policy engine** — network egress policies, write permission gates, data
  retention, cross-workflow isolation
- **RBAC + audit** — 5 roles (admin, builder, operator, reviewer, viewer),
  per-workflow scoping, audit log for every action
- **Connector SDK** — wraps Mastra `createTool()` with permission manifests,
  side-effect classification, idempotency keys, dry-run, and rate limits
- **Agent CLI adapters** — Claude Code and Codex as approval-gated connectors

### flowsafe — approval & execution

Approval management UI + Cloudflare-native durable workflow runner:

- **Approval API** — queue, claim, decide, delegate, SLA tracking, escalation
- **Approval dashboard** — styling-agnostic React UI for the approval queue
- **DO runner** — init()-based import-swap for Mastra workflows on Cloudflare
  Durable Objects (the same mechanism `@mastra/inngest` and the experimental
  `@mastra/temporal` use)
- **Deploy template + ops** — copy-ready production Worker with cron-owned SLA
  sweep and retention purge; Queues → SIEM audit export; R2 artifact storage

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      anchorage                          │
│                                                         │
│  ┌──────────────────────┐  ┌────────────────────────┐   │
│  │     breakwater        │  │       flowsafe          │   │
│  │                        │  │                         │   │
│  │  • Policy engine      │  │  • Approval API         │   │
│  │  • RBAC + audit log   │  │  • Approval dashboard   │   │
│  │  • Connector SDK      │  │  • Cloudflare DO runner │   │
│  └──────────┬───────────┘  └───────────┬─────────────┘   │
│             │                          │                  │
└─────────────┼──────────────────────────┼──────────────────┘
              │                          │
┌─────────────┴──────────────────────────┴──────────────────┐
│                        Mastra                              │
│  Workflows │ Agents │ Memory │ RAG │ Tools │ Evals │ MCP  │
└─────────────────────────┬─────────────────────────────────┘
                          │
                          ▼
               ┌─────────────────────┐
               │   Durable Objects   │
               │    (Cloudflare)     │
               └─────────────────────┘
```

Mastra is runtime-agnostic (Inngest, the experimental Temporal integration, and
others); Anchorage's flowsafe ships the Cloudflare Durable Objects runner.

## Repository Structure

```
anchorage/
  packages/
    breakwater/         # @proofoftech/breakwater
      src/
        policy-engine/  # Policy gates: egress, output-channel, retention, cross-workflow isolation
        rbac/           # Roles, scopes, audit log
        connector-sdk/  # Permission manifests: egress, write-approval, idempotency, dry-run, rate limit
        agent-cli/      # Claude Code / Codex CLIs as approval-gated connectors
    flowsafe/           # @proofoftech/flowsafe
      src/
        approval-api/   # Queue, claim, decide, delegate, SLA
        approval-ui/    # Styling-agnostic React approval dashboard
        do-runner/      # Cloudflare Durable Objects workflow runner
        audit-export/   # Cloudflare Queues -> SIEM audit export
        artifacts/      # R2-backed workflow artifact storage
      deploy/           # Copy-ready production Worker (cron SLA sweep + retention purge)
  docs/                 # Product & engineering design specification
```

## Quick Start

```bash
pnpm install
pnpm -r build && pnpm -r test          # 388 tests across both packages
pnpm --filter @proofoftech/flowsafe spike:verify   # workerd: suspend -> kill -> restart -> resume proof
```

To deploy flowsafe, copy `packages/flowsafe/deploy/` and follow its
[README](packages/flowsafe/deploy/README.md).

## License

Apache-2.0. See [`LICENSE`](LICENSE).

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md).
