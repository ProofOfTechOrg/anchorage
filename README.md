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
records at resume time, so forged resumes fail closed. flowsafe is
multi-tenant by construction — run ids carry their tenant, approval stores are
tenant-bound at construction, and cross-tenant reads are a compile error
outside the cron path. A React dashboard ships as
`@proofoftech/flowsafe/approval-ui`. Anchorage also includes a runnable
five-workflow showcase (`packages/showcase/`), a copy-ready
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
| **showcase** | Runnable demo — five workflows, one React frontend, one Cloudflare deploy | `showcase` (private) |

Published on npm: `npm i @proofoftech/breakwater @proofoftech/flowsafe`. `showcase`
is private — clone and build to run the demo (see [Quick Start](#quick-start)).
Targets Node >= 22, ESM only (TypeScript `moduleResolution` `node16`/`nodenext`/`bundler`), `@mastra/core` ^1.50.0.

### breakwater — safety layer

Plugs into Mastra as processors (`@mastra/core/processors`) and tool/workflow
wrappers:

- **Policy engine** — network egress policies, write permission gates, data
  retention, cross-workflow and cross-tenant isolation
- **RBAC + audit** — 5 roles (admin, builder, operator, reviewer, viewer),
  per-workflow scoping, audit log for every action
- **Connector SDK** — wraps Mastra `createTool()` with permission manifests,
  side-effect classification, idempotency keys, dry-run, and rate limits
  (idempotency and rate-limit budgets segment per tenant when the host mints
  an isolation scope; breakwater itself stays tenant-agnostic)
- **Agent CLI adapters** — Claude Code and Codex as approval-gated connectors

### flowsafe — approval & execution

Approval management UI + Cloudflare-native durable workflow runner:

- **Approval API** — queue, claim, decide, delegate, SLA tracking, escalation
- **Approval dashboard** — styling-agnostic React UI for the approval queue
- **DO runner** — init()-based import-swap for Mastra workflows on Cloudflare
  Durable Objects (the same mechanism `@mastra/inngest` and the experimental
  `@mastra/temporal` use)
- **Multi-tenancy** — server-minted tenant-carrying run ids, tenant-bound
  approval stores enforced by the type system, per-tenant connector budgets,
  and one-call tenant offboarding
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
        approval-api/   # Queue, claim, decide, delegate, SLA, tenant-bound stores
        approval-ui/    # Styling-agnostic React approval dashboard
        do-runner/      # Cloudflare Durable Objects workflow runner
        host-kit/       # Identity seam, tenant resolver + registry, shared run routes
        audit-export/   # Cloudflare Queues -> SIEM audit export
        artifacts/      # R2-backed workflow artifact storage
      deploy/           # Copy-ready production Worker (cron SLA sweep + retention purge)
    showcase/           # Runnable demo (private): five workflows, one frontend, one deploy
      src/              # Vite React SPA: launcher, run cards, approval dashboard
      worker/           # The Cloudflare host: runtime, the five workflows, demo auth
  docs/                 # Product & engineering design specification
```

## Quick Start

```bash
pnpm install
pnpm build && pnpm test                # 837 tests across the workspace
pnpm --filter @proofoftech/flowsafe spike:verify   # workerd: suspend -> kill -> restart -> resume proof
```

To deploy flowsafe, copy `packages/flowsafe/deploy/` and follow its
[README](packages/flowsafe/deploy/README.md).

## Multi-tenancy

flowsafe runs many tenants on one Worker + D1 without a per-tenant database.
Three invariants carry it, each with a single chokepoint: run ids are minted
server-side as `` `${tenantId}_${uuid}` `` from the authenticated tenant
(so snapshots, Durable Object instances, grants, and R2 keys are disjoint for
free); approval stores are bound to one tenant at construction, with an
unbound or cross-tenant store rejected at compile time; and the tenant-id
charset excludes the delimiter, which is what makes the ownership check and
the offboarding range-delete exact. Details, including what each invariant
does *not* cover:
[`packages/flowsafe/README.md#multi-tenancy`](packages/flowsafe/README.md#multi-tenancy)
and [`docs/security-threat-model.md`](docs/security-threat-model.md).

## License

Apache-2.0. See [`LICENSE`](LICENSE).

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md).
