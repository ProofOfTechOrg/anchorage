---
"@proofoftech/flowsafe": patch
---

`FencedWorkflowsStorageD1`'s `replaceSnapshot` capability member refuses a replacement whose `snapshot` is not a JSON object or whose `updatedAt` is not an ISO-8601 time in `Date.prototype.toISOString()` form, and writes nothing. Run retention compares the stored `updatedAt` with its cutoff as text, so any other form, including a valid time without milliseconds, can keep a run from being purged on time, and text that is not a time also makes every later settlement read of that run fail as unreadable.
