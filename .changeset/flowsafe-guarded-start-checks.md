---
"@proofoftech/flowsafe": minor
---

The thread host refuses durable starts containing caller system messages or call-level provider options rejected by Breakwater's `assertAcceptedCallProviderOptions()` with `400`, before authorization or any write. A threadless fire whose stored options are refused records `failed`. The optional `scheduleProviderOptionsPolicy` runs on every threaded schedule fire before signal creation; a denial settles a discard, while an error leaves the lease for a later tick. The optional Breakwater peer minimum rises to `0.16.0`, which supplies these checks.

Security: before this release, a caller `role: 'system'` message passed to the thread host's durable start, and a stored schedule's call-level provider options reached the model outside the guarded agent's input policies.

Migration: raise `@proofoftech/breakwater` to `0.16.0`. Move per-agent provider options from schedules and topology starts to the agent's model entry. Wire `scheduleProviderOptionsPolicy` with both `providerOptionsCarryContent()` and `assertAcceptedCallProviderOptions()` so threaded fires are refused when either check rejects their stored options.
