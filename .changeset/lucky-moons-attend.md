---
"@proofoftech/breakwater": minor
"@proofoftech/flowsafe": minor
"showcase": patch
"anchorage-agent-starter": patch
---

Require `@mastra/core` 1.67.0 exactly (previously 1.53.0). The peer is exact, so every consumer must move to 1.67.0 as well; this is breaking for consumers pinned to 1.53.0. 1.67.0 bundles for Cloudflare Workers and Vite again — mastra-ai/mastra#20638, the dynamic-import regression that held the pin at 1.53.0, closed upstream.

`@mastra/cloudflare-d1` moves from 1.1.1 to 1.3.2, whose own peer requires a core newer than 1.53.0. FlowSafe's `@proofoftech/breakwater` peer floor rises to `>=0.15.0 <1.0.0` in step, that being the first Breakwater release built against the same core.

The `@mastra/core` patch FlowSafe shipped under `patches/` is retired: 1.67.0 carries both fixes upstream (mastra-ai/mastra#23693, mastra-ai/mastra#23694). The patch file is gone from the published package, and so are the two refusals that required it — FlowSafe no longer refuses to construct a delivering notification dispatch tick, nor notification ingestion and dispatch requests, on an install whose core lacks the patch. Consequence for anyone running a core outside the declared peer without having applied the patch: a notification `source` named after an `Object.prototype` member is miscounted in the summary core renders, and its source delivery policy resolves the inherited member instead of the configured priority or default action. That configuration was unsupported before and remains so, but it now fails silently rather than loudly. Application roots that copied the patch into their own `patches/` should drop it along with the `patchedDependencies` entry or `postinstall` script that applied it.

`createD1Storage({ domains })` composes the two storage domains 1.67.0 adds, `workflowDefinitions` and `knowledge`, through the same override seam as the other domains. `@mastra/cloudflare-d1` backs neither, so each resolves `undefined` unless a host supplies one through that seam.

The workspace lockfile behind this release was resolved once with pnpm's seven-day minimum-release-age gate overridden for that resolution only. Five newly resolved versions were younger than the gate at resolution on 2026-09-19: `@mastra/core` 1.67.0 and `@mastra/schema-compat` 1.3.10 (published 2026-09-15), which the workspace's standing `@mastra/*` exclude admits with the gate on, and `posthog-node` 5.52.4 (2026-09-15), `@posthog/types` 1.412.2 (2026-09-17) and `@posthog/core` 1.55.0 (2026-09-18), which the override alone admitted. None of the five declares a lifecycle script or ships a native component; all five carry npm provenance attestations (SLSA v1, published from GitHub Actions); and `posthog-node`, with `@posthog/core` and `@posthog/types` beneath it, is a hard dependency of `@mastra/core` that ships inside a bundled Worker.

The private `showcase` and `anchorage-agent-starter` packages move to the same core.
