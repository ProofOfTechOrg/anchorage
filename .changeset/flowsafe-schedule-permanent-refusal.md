---
"@proofoftech/flowsafe": minor
---

A threaded schedule fire that the thread refuses permanently is recorded as `failed` with the refusal's message, and the schedule keeps firing. The trigger and audit carry the reason `dispatch-refused`. This covers an invalid dispatch or host input, an agent that does not allow the scheduled automation, and no matching thread binding or claimed dispatch. A 403 from a host automation policy fails that fire, not the schedule. Conflicts, server errors and other failures keep being retried. Fires already waiting in `deferred` close the same way on their next retry.

A permission-requiring agent whose `resolvePrincipalPermissions` throws, rejects or returns malformed output refuses the entry with 503 `permission resolution unavailable` instead of 403 `forbidden`. Automated callers, including the schedule tick, retry it. Human callers of agent starts receive this status too. A missing resolver or unsatisfied permissions still answer 403.

A fire refused because its thread binding does not exist yet is lost and is visible in trigger history.

Signal routes answer a request the thread refuses as malformed, including host input refusals, with 400 `{ "error": "bad request" }` instead of 502 `{ "error": "internal error" }`. Public signal callers receive this response too.

In `D1SchedulesStorage`, permanently refused fires no longer hold schedule deletion open while they wait for retries. A custom store decides its own deletion handling.

Migration: The required `failDeferredTrigger` member on `ScheduleTickStore` is a breaking interface change. If you supply a custom store, implement this signature:

```typescript
failDeferredTrigger(
  id: string,
  scheduleId: string,
  error: string,
  metadata: Record<string, unknown>,
): Promise<boolean>;
```

Merge `metadata` into the stored metadata, as `touchDeferredTrigger` does. Record the failure only while the trigger is still `deferred`, and resolve `true` when a row changes or `false` otherwise. Leave a trigger whose target already settled a receipt, or that another tick resolved, unchanged.

A custom `signalAgent` seam must throw an error with the thread route's status in a numeric `status` property, as `ScheduleTickSignalAgent` documents. An adapter that reaches the route through another hop must not report that hop's own status as a refusal. A seam that throws without a status keeps every refusal retrying.
