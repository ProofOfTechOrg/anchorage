# Observability And Quality

Mastra provides OTel tracing, Studio visual debugger, 20+ evaluation scorers, and Langfuse/Datadog/Sentry exporters. Anchorage adds approval-specific observability for the flowsafe layer.

## Mastra Surfaces (Used As-Is)

- Run list and current status (Mastra Studio)
- Per-run timeline (Mastra Studio)
- Trace inspection and replay (Mastra Studio)
- Eval scorer results (Mastra Studio)

## Anchorage Addition: Approval Metrics

| Metric | Source | Description |
|---|---|---|
| `approval.pending.count` | flowsafe API | Number of pending approvals |
| `approval.sla_breach.count` | flowsafe API | Approvals past their SLA |
| `approval.avg_resolution_time` | flowsafe API | Mean time to approve or reject |
| `approval.escalation.count` | flowsafe API | Escalations triggered |

Metrics are read through the caller's tenant-bound store, so
`GET /api/approvals/metrics` reports that tenant's queue, never the fleet's.

## Audit Export

Audit log is exported as JSON Lines via Cloudflare Queues for SIEM ingestion.
Each line is a single audit event in the schema defined in
`security-threat-model.md`. Approval-service events carry `tenantId` in
`detail`; the SLA sweep — the one cross-tenant writer — would otherwise emit
unattributable escalations. Export deliberately co-batches all tenants into one
NDJSON POST: payload-level tagging is sufficient for a shared SIEM, and
per-tenant fan-out is a non-goal.

## Quality Gates

- Connector idempotency verification (post-execution check for duplicate keys)
- Policy compliance checks (did execution violate any pre-gate policy?)
- Approval SLA attainment (percentage of approvals resolved within SLA)
