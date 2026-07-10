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
| `security-threat-model.md` | Assets, seven trust boundaries (incl. tenant↔tenant: INV-1/2/3 and the residuals they do not cover), threats, RBAC model, audit log schema | Security review, threat modeling, RBAC design |
| `observability-and-quality.md` | Approval metrics, audit export, quality gates | Implementing observability and audit features |
| `operations-runbook.md` | Build, deploy, tenant provisioning + offboarding, the two-cron split, incident response, and tuning | Deploying, operating, responding to incidents |

## Subdirectories

| Directory | What | When to read |
| --------- | ---- | ------------ |
| `examples/` | TypeScript `createWorkflow()` design sketches (serial, parallel, conditional, approval, RBAC scoping) — illustrative, not runnable | Learning Mastra workflow patterns and breakwater/flowsafe integration points |
