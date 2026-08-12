---
'@proofoftech/flowsafe': minor
---

Create artifact purgers from the current Worker environment, apply the configured D1 table prefix to built-in maintenance, enforce the shared 39-character prefix limit at every storage and low-level purge boundary, and pin the D1 adapter compatible with the minimum supported Mastra core.

Migrate a configured artifact purger from `artifactStore: store` to `artifactStore: () => store`. For R2, use `artifactStore: (env) => new R2ArtifactStore(env.ARTIFACTS)`. If runtime storage uses `tablePrefix`, keep it at 39 characters or fewer and set the identical `storageTablePrefix` on `createFlowsafeWorker()`.
