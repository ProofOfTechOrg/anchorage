---
'@proofoftech/flowsafe': patch
---

`flowsafe-provision` sets a 64 MiB `maxBuffer` on the `wrangler d1 execute --json` child process whose output it parses. Node's default is 1 MiB counted across the captured stdout and stderr together, so a response past that was truncated and the run surfaced as `failed to execute Wrangler 4` with an `ENOBUFS` cause instead of the parsed rows.
