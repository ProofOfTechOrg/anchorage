---
'@proofoftech/flowsafe': minor
---

Create artifact purgers from the current Worker environment, apply the configured D1 table prefix to built-in maintenance, and pin the D1 adapter compatible with the minimum supported Mastra core.

Migrate a configured artifact purger from `artifactStore: store` to `artifactStore: () => store`. For R2, use `artifactStore: (env) => new R2ArtifactStore(env.ARTIFACTS)`. If runtime storage uses `tablePrefix`, set the identical `storageTablePrefix` on `createFlowsafeWorker()`.
