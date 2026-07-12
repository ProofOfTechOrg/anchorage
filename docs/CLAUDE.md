# docs/

Architecture, design, security, and operations docs for Anchorage, plus TS workflow examples.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `breakwater-architecture.md` | Safety middleware architecture (processor chain + tool/workflow wrappers) | Implementing or modifying breakwater |
| `flowsafe-architecture.md` | Approval UX + DO runner architecture, the approval REST surface, and the three multi-tenancy invariants | Implementing or modifying flowsafe |
| `do-runner-design.md` | Durable Object import-swap pattern, lifecycle, run identity + tenant scoping (INV-1), the ctx.storage resume ledger, retention vs tenant purge, and known Workers constraints | Implementing the DO runner, understanding Cloudflare-native execution |
| `policy-engine-design.md` | Four policy domains (egress, write, retention, isolation — cross-workflow AND cross-tenant) beyond Mastra processors | Extending policy domains, implementing custom policies |
| `connector-interface.md` | Connector permission / idempotency / dry-run manifest wrapping `createTool()` (`createConnector()`), and the opaque isolation scope that segments idempotency + rate-limit keys per tenant | Implementing or extending the connector SDK |
| `model-gateway-policy.md` | Processor-based pre/post gates around `Agent.generate()` | Implementing guardrails around model calls |
| `security-threat-model.md` | Assets, seven trust boundaries (incl. tenant↔tenant: INV-1/2/3 and the residuals they do not cover), threats (incl. content-inspection, notification-exposure, and agent-memory rows), RBAC model, audit log schema | Security review, threat modeling, RBAC design |
| `agent-memory-tenancy.md` | Agent-memory tenancy: why Mastra threads/messages/resources are a cross-tenant leak unsalted, the INV-1 extension (salted threadId/resourceId, memory-id chokepoints, purgeTenant coverage — SHIPPED 2026-07-12), and the obligations the first agents-with-memory feature must implement (host boundary, recall-path proof, thread TTL) | Building or reviewing anything that touches Mastra agent memory |
| `observability-and-quality.md` | Approval metrics, the audit→metrics adapter (`metricsAuditSink`), the notification seam, audit export, quality gates | Implementing observability, notification, and audit features |
| `operations-runbook.md` | Build, deploy, tenant provisioning + offboarding, the two-cron split, incident response, and tuning | Deploying, operating, responding to incidents |

## Subdirectories

| Directory | What | When to read |
| --------- | ---- | ------------ |
| `examples/` | TypeScript `createWorkflow()` design sketches (serial, parallel, conditional, approval, RBAC scoping) — illustrative, not runnable | Learning Mastra workflow patterns and breakwater/flowsafe integration points |
| `api/` | GENERATED typedoc output (`pnpm docs:api`), gitignored — never edit or commit | Browsing the generated API reference locally |
