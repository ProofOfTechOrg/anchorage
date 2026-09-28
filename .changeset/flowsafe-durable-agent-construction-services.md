---
"@proofoftech/flowsafe": minor
---

`createFlowsafeDurableAgent()` fixes the wrapper's agent-level pub/sub at construction to `pubsub ?? runtime.pubsub`, or its own stream bus when both are absent. HTTP-started threaded runs register on the thread Durable Object's pub/sub from their first start. Co-located thread Durable Objects keep separate active-run state, signals sent into those runs are drained by them, and each run publishes one stream to the thread's pub/sub topic from its first start. A `threadRuntime` object passed to the wrapper receives `registerRun` only for runs resumed through `resumeViaRuntime()`; core registers started runs. The constructor throws a `TypeError` when the wrapped agent has a different pub/sub of its own.

`FlowsafeDurableAgent` refuses `__setMemory()`, `__setPubSub()`, `abortRunStream()`, `abortThreadStream()` and `discoverThreadPeers()`.

Security: a controller's service installation reached the wrapped agent before the Mastra refusal. Direct aborts bypassed the terminate route's ownership and disputed-settlement checks and could reach another thread's run in the isolate. Peer discovery returned every advertised thread identity on the pub/sub.

Migration: the signal routes set pub/sub only on agents that are not runtime-driven and answer `503` when a runtime-driven agent's pub/sub is missing or differs from the thread's. Construct the wrapper with the thread Durable Object's pub/sub, as the thread host does, and use a fresh wrapped agent for each thread Durable Object. Configure memory and pub/sub at construction. Cancel runs through the terminate route; the stream result's `abort()` still works.
