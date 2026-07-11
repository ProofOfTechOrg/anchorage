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

## Anchorage Addition: Metrics Over Audit Events

breakwater's `metricsAuditSink(recorder)` adapts the audit stream onto any
`{increment, observe}`-shaped metrics client (StatsD, Prometheus push, OTel):
every event increments `breakwater.audit.decision` tagged
`{action, decision}` — so denials, tripwires, rate-limit rejections, and SLA
escalations are queryable without enumerating actions — and any event
carrying a finite, non-negative `detail.durationSeconds` observes
`breakwater.audit.duration_seconds` tagged `{action}` (flowsafe's `decide()`
emits queue dwell time on this convention). `combineAuditSinks(...)` fans one
`AuditLogger` out to several sinks — e.g. metrics alongside the Queues → SIEM
export — isolating per-sink failures. Counters and histograms only; Mastra's
OTel tracing is not duplicated.

## Anchorage Addition: Approval Notifications

`ApprovalNotificationSink` is the reviewer-facing transport seam: fired once
per record actually entering the queue (`created`) and once per SLA
escalation (`escalated`), contained fire-and-forget — a throwing or rejecting
transport audits as `approval.notify`/'error' and never fails the approval
action. flowsafe ships NO transport; hosts wire email/chat/pager adapters
through `HostApprovalServiceOptions.notify`, `SweepSLAOptions.notify`, or
`createFlowsafeWorker`'s `notify` hook. The event carries the full
`ApprovalRecord` (reviewer context); transports addressing lower-trust
channels must project/redact — see the threat model.

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
