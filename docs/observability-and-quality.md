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

## Audit Export

Audit log is exported as JSON Lines via Cloudflare Queues for SIEM ingestion. Each line is a single audit event in the schema defined in `security-threat-model.md`.

## Quality Gates

- Connector idempotency verification (post-execution check for duplicate keys)
- Policy compliance checks (did execution violate any pre-gate policy?)
- Approval SLA attainment (percentage of approvals resolved within SLA)
