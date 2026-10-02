---
"@proofoftech/flowsafe": patch
---

A run resumed through `resumeViaRuntime()` that suspends at another approval keeps blocking its thread until that approval is decided, even while an output processor has not processed or drops the approval. Under the default cache, the resumed observer starts from the current stream position without replaying earlier legs' events. Earlier resumed legs' thread registrations complete when the run ends or a resume fails after rehydration; the wrapper then publishes a terminal error.

Availability: Before this change, the thread was released while the run awaited approval. Signals to the thread were refused as blocked, or a host without a durable blocking check could start a second run.

No migration is needed.
