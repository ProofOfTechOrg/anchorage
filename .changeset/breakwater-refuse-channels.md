---
"@proofoftech/breakwater": minor
---

`createGuardedAgent()` now refuses the `channels` construction option, and `GuardedAgentConfig` omits it.

Security: Mastra dispatches a channel's inbound messages and tool approvals to the agent the channel is configured on, outside the host that drives the guarded handle. A guarded agent already refused each such dispatch at its call-option check, but a Mastra that registered it still initialized the channels and started their listeners.

Migration: remove `channels` from the options passed to `createGuardedAgent()`. TypeScript code that passes it no longer compiles.
