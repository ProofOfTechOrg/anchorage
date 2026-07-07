# audit/

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `index.ts` | `AuditLogger` shared sink (ring buffer, sink isolation via `onSinkError`) + `AuditEvent`/`AuditSink`/`AuditLoggerOptions`; every breakwater gate and flowsafe's approval service write here | Changing audit event shape, buffering, or sink behavior |
| `audit.test.ts` | Buffer cap, sync/async sink-failure isolation tests | Adding audit tests, debugging sink error handling |
