---
"@proofoftech/flowsafe": patch
---

A run resumed through `resumeViaRuntime()`, as an approval decision resumes one, registers with Mastra's thread runtime in a form whose `status` Mastra can read. Before this release, reading it threw `Cannot read private member #status`: Mastra's completion check failed after every threaded resume and never released the finished run's registration, and on a thread host built with `cache: false`, or with a `CachingPubSub` as its pub/sub, later signals, messages and schedule fires to that thread answered `502`.
