---
"@proofoftech/flowsafe": patch
---

When you set `canPersistSchedule`, its answer governs persistence for every threaded schedule fire in place of `canPersist`'s answer. This includes a wake that falls back to persistence during a deployment drain, under a run cap, or without a start seam. Delivered signals whose schedule persistence is denied carry the persistence-forbidden marker, preventing the runner from persisting them after the run ends. For those fires, `canPersist` is not called, so a throwing `canPersist` no longer fails them and hosts see fewer `canPersist` calls.

An authorized schedule wake with memory available persists, instead of being discarded, when a drain keeps it from starting a run. A fire that `canPersistSchedule` refuses is never persisted through a wake fallback, even when `canPersist` allows the system principal.

A throwing `canPersistSchedule` fails every fire on which it is consulted, including wake and deliver targets, and the schedule tick retries the fire.
