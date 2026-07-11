# audit/

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `index.ts` | `AuditLogger` shared sink (ring buffer, sink isolation via `onSinkError`) + `AuditEvent`/`AuditSink`/`AuditLoggerOptions`; every breakwater gate and flowsafe's approval service write here. Plus the observability seam: `MetricsRecorder` + `metricsAuditSink` (`breakwater.audit.decision` counter tagged `{action, decision}`; `breakwater.audit.duration_seconds` observed when `detail.durationSeconds` is finite) and `combineAuditSinks` (fan-out; sync throws aggregate after every sink ran, async rejections collected via allSettled — no unhandled rejections) | Changing audit event shape, buffering, sink behavior, or the metrics adapter |
| `audit.test.ts` | Buffer cap, sync/async sink-failure isolation, metrics-adapter tag/duration/never-throws, combineAuditSinks aggregation tests | Adding audit tests, debugging sink error handling |
