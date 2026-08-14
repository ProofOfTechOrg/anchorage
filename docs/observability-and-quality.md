# Observability and quality

Mastra supplies traces, execution events, evaluation, and its own observability integrations. Anchorage adds decision-oriented evidence around policy, connectors, approvals, deployment boundaries, unattended starts, maintenance, and durable recovery.

Use both. A trace explains what executed; an Anchorage audit event explains what a specific enforcement point allowed, denied, or failed.

## Audit logger

`AuditLogger` keeps a bounded in-memory ring and calls an optional sink:

```typescript
import {
  AuditLogger,
  combineAuditSinks,
  metricsAuditSink,
} from '@proofoftech/breakwater/audit';
import { queueAuditSink } from '@proofoftech/flowsafe/audit-export';

const audit = new AuditLogger({
  maxBuffered: 1_000,
  sink: combineAuditSinks(
    metricsAuditSink(metrics),
    queueAuditSink(queue),
  ),
  onSinkError: (error, event) => {
    console.error('audit sink failed', {
      action: event.action,
      decision: event.decision,
      error,
    });
  },
});
```

Sink failure does not change the gated application decision. The event remains in the ring until eviction. This improves application availability but means export health needs a separate alert.

`combineAuditSinks()` invokes every sink and aggregates synchronous and asynchronous failures after all sinks settle.

## Audit-derived metrics

`metricsAuditSink()` uses a structural recorder:

```typescript
interface MetricsRecorder {
  increment(name: string, tags?: Record<string, string>): void;
  observe(
    name: string,
    value: number,
    tags?: Record<string, string>,
  ): void;
}
```

It records:

| Metric | Tags | Meaning |
| --- | --- | --- |
| `breakwater.audit.decision` | `action`, `decision` | One audit verdict |
| `breakwater.audit.duration_seconds` | `action` | Non-negative finite `detail.durationSeconds`, when supplied |

This adapter does not create tracing spans or define a backend-specific metrics package.

## Approval metrics

`GET /api/approvals/metrics` returns the deployment approval aggregate:

```typescript
interface ApprovalMetrics {
  openCount: number;
  slaBreachedCount: number;
  escalationCount: number;
  decidedCount: number;
  approvedCount: number;
  rejectedCount: number;
  avgResolutionSeconds: number | null;
}
```

D1 computes this through SQL aggregation rather than loading every record into the Worker. The in-memory store uses equivalent reduction.

Useful derived service indicators:

- approval backlog by workflow, age, and priority;
- breached/open ratio;
- approval and rejection rate;
- decision latency percentiles from events or traces;
- resume success after an approved decision;
- re-suspension frequency;
- reviewer conflict and separation-of-duties denial rate.

The built-in aggregate reports a mean, not percentiles.

## Approval notifications

`ApprovalNotificationSink` receives `created` and `escalated` events. A failure emits an `approval.notify` error event and does not reverse the record mutation.

The event contains a full approval record. Project or redact it before sending to an email, chat, or pager channel with lower trust than the approval dashboard.

Track notification-delivery success separately from approval creation. “Record exists” and “reviewer was notified” are different service levels.

## Live-stream observability

`ApprovalStreamSink` emits mutation frames to the deployment hub. The run Durable Object emits complete `RunSummary` frames.

Clients retain polling:

- approval polling continuously reconciles queue/metrics drift;
- run polling pauses while the socket is healthy and resumes after closure;
- heartbeat/liveness detects a socket that appears open but no longer delivers.

Monitor ticket-mint failures, WebSocket upgrade failures, unexpected closures, reconnect rate, and poll-reconciliation corrections.

Live delivery is not the source of truth. D1 and run status are.

## Signals, schedules, tasks, and providers

Opt-in domains have their own structured audit types:

- `signal.ingest`: verified deployment tag, actor, thread, channel, outcome, reason, and content bytes;
- `schedule.route`: authenticated schedule CRUD outcome;
- `schedule.fire`: claim, skip, start, or failure details;
- objective mutations;
- subscription route and webhook verification outcomes;
- provider polling and delivery;
- background-task lifecycle and cleanup;
- notification dispatch.

Post-auth denials that look like probes are audited. Anonymous rejection is deliberately bounded or omitted on high-volume webhook/signal boundaries so an attacker cannot create unbounded evidence storage.

## Maintenance logs

The composed Worker emits structured configuration and maintenance results for:

- SLA sweep;
- deployment-sentinel or internal caller-credential refusal;
- workflow and approval retention;
- thread, notification, thread-state, trigger, and task retention;
- deployment-owned retention duties;
- maintenance bootstrap, alarm status, and duty failures;
- provider alarm reconciliation;
- Queue export.

Each duty has its own failure boundary. Alert on both explicit error events and the absence of an expected success heartbeat.

## Queue and SIEM export

`queueAuditSink()` sends events to Cloudflare Queues. `createAuditQueueConsumer()`:

1. optionally transforms each event;
2. serializes a batch as NDJSON;
3. posts to the collector;
4. acknowledges only on a 2xx response;
5. retries the batch otherwise.

Configure Queue retry and a dead-letter queue. Monitor:

- producer send failures;
- queue age and depth;
- consumer invocation and HTTP status;
- retry count;
- dead-letter arrival;
- collector parse failures;
- deployment/event schema drift.

The control-plane consumer can co-batch events from several physical deployments. Preserve the verified deployment tag and enforce organization-aware access in the SIEM.

## Safe error surfaces

Arbitrary thrown values are untrusted. A connector, evaluator, process, parser, store, or provider can throw secrets.

Breakwater's safe-error registry lets a built-in error expose a static audit reason and bounded metadata. Unregistered throws use a static class-level reason.

Agent CLI errors additionally guarantee:

- prompt-free command display;
- no stdout/stderr body in error or audit;
- sanitized system error code;
- booleans for captured output;
- numeric exit/timeout data only.

Do not log caught errors again through a generic serializer without reapplying the same policy.

## Recommended dashboards

### Guardrails

- decisions by `action` and `decision`;
- deny and error rate by connector/policy;
- classifier timeout and failure;
- detected PII/secret category, without matched text;
- egress declaration and runtime-fetch denial;
- idempotency reservation, replay, pending, takeover, and store degradation;
- rate-limit denial by deployment-wide connector budget.

### Approvals

- open, breached, escalated, approved, and rejected counts;
- resolution time;
- self-decision and cross-gate denial;
- decision CAS conflict;
- resume failure and redrive;
- queue notification failure.

### Durable execution

- start, resume, status, terminate, and deadline-sweep latency and errors;
- runs by current status and age;
- suspended runs by workflow and gate age;
- restart recovery;
- live socket connections and reconciliation;
- purge rows/artifacts by domain and failures.

### Long-running agents

- active and idle wakes;
- signal rate/size rejection;
- notification backlog and dispatch;
- goal mutations and max-run rejection;
- schedule due/claimed/skipped/started;
- unattended-run-cap denial;
- background task queue/worker/failure/cleanup;
- provider alarm, poll, webhook verification, and delivery;
- subscription mutation with failed post-commit reconciliation.

## Repository quality gates

The merge gate covers:

- Biome lint and formatting;
- strict TypeScript across source, tests, Worker, React UI, and React 18 peer-floor probe;
- Vitest workspace suites;
- production builds;
- packed consumer tests for public npm exports;
- generated API documentation;
- documentation links, anchors, exports, npm-safe links, and orphan checks;
- showcase public metadata and token-free bundle assertions;
- react-doctor;
- deterministic workerd restart/security spike;
- supply-chain minimum release age;
- SPDX source guards;
- a non-blocking newest-Mastra canary.

The exact commands are in [Maintainer guide](maintainer-guide.md).

## Release evidence

Before announcing a release, retain:

- CI run and commit SHA;
- package tarball contents and packed-consumer result;
- changeset and changelog;
- TypeDoc Pages deployment;
- workerd spike output;
- demo build metadata assertion;
- optional live-model provider proof when relevant;
- migration and rollback notes for any Durable Object or D1 change.

Do not publish a test-count claim in product copy. The count changes frequently and does not explain which security invariants are covered.
