---
"@proofoftech/breakwater": minor
---

Export `assertNoGuardedSystemMessages()` and `assertAcceptedCallProviderOptions()` for hosts that start a guarded agent's durable loop. The call-level check accepts only a closed list of generation settings and refuses everything else before Mastra prepares the call. Export `providerOptionsCarryContent()` for hosts that carry provider options in a message without running input policies, such as a schedule signal.
