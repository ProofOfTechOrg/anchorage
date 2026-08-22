---
'@proofoftech/flowsafe': minor
---

Signal delivery through a Flowsafe durable agent no longer starts an unowned
run below the host seam; an unbranded agent on an active thread keeps core's own
behavior as a degraded configuration.

This changes the public signal contract:

- `/signal/queue` persists in both active and idle states. Success now returns
  `decision.action: 'persist'` without a `runId`; active-thread auto-drain is
  removed, so the message surfaces on the next host-started turn.
- `/signal/state` now applies the queue route's owner gates and can return
  `principal-mismatch` or `persistence-forbidden`.
- `/signal/notification` creates the notification record for every accepted
  provider delivery. Owners receive core's `{ record, decision, ... }` result
  under the top-level `record` field, with the signal-routing decision exposed
  separately as `delivery` when core returns one. Non-owners receive a flat
  `NotificationRecord` under `record` plus
  `delivery: { action: 'deferred', reason: 'dispatcher' }`; they never send a
  signal directly. Low-priority owner notifications use summarize-later and
  have no immediate `delivery`.
- Unbranded agents return `degraded: 'not-runtime-driven'` from successful,
  non-skipped state and owner-notification responses, regardless of thread state.
  Skipped state and an early `memory-unavailable` state response carry no marker.
- `/signal/message`, `/signal`, `/signal/schedule`, and
  `/signal/notifications/dispatch` now persist on a stale-active-id fall-through
  instead of waking. A forbidden fallback returns `persistence-forbidden`; a
  memory-less fallback returns `memory-unavailable`. The notification dispatch
  lane counts either discard as failed and performs no persisted write. A
  non-owner `/signal` request for `ifActive: 'persist'` degrades to `discard`
  for active delivery: when the thread was active, the response is
  `persistence-forbidden` without a `signalId` because the gate refused and
  nothing was delivered; when the thread was idle, the caller's own `ifIdle`
  outcome is returned unchanged with `signalId`. Owners still forward
  `persist`. Non-owner active deliveries carry non-rendered metadata so a
  completion drain cannot preserve a leftover through the terminal path.
- Persist outcomes return a `memory-unavailable` discard decision when the
  resolved agent has no memory, after the content gate. A default or
  `ifIdle: 'persist'` message or signal is delivered into an active run without
  memory; an active persist that no memory could write answers
  `memory-unavailable`. A persist-behavior `/signal/schedule` fire instead
  settles a canonical `discard` receipt with `outcome: 'discarded'` and no
  reason, where it previously settled `persisted`.
  Owner `/signal/notification` is the other exception: its model-visible memory
  write is best-effort because the inbox record is already durable. The shipped
  starter host does not configure agent memory, so its other persist outcomes
  return `memory-unavailable` until the host adds memory configuration.
- Non-owner `/signal/notification` ingestion now requires notification storage
  and returns `409` without it. Those rows bypass the agent's delivery policy and
  readiness hook; the host must run `createNotificationDispatchTick()` to
  deliver them. The starter runs it every 60 seconds, giving up to one tick of
  latency. A host without the tick records but never delivers them; the spike
  has no tick and its provider probes assert only the inbox row.
- The durable-agent runner terminally fails every run that was not registered
  through `streamUntilPersisted()`. Direct `stream()` resolves to a failed
  output; direct `generate()` rejects. `stream()`, `generate()`, `prepare()`, and
  `streamUntilPersisted()` synchronously refuse a live id, and `prepare(X)`
  keeps `X` live until cleanup. `streamUntilPersisted()` also refuses
  `untilIdle`. If the runner's two terminal-publication attempts and core's own
  fire-and-forget attempt all fail, the output never closes and the thread stays
  active until eviction or a new host start.
- The public `signals/router.ts` state and notification channels carry these new
  response shapes.

Migrate run starts to the host routes or `streamUntilPersisted()`. Treat queue
success as `{ action: 'persist' }` without a `runId`, and read queued messages on
the next host-started turn.
