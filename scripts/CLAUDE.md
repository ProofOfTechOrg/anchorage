# `scripts/`

Markdown syntax checks use unified and remark; repository policy remains local.

## Contents

- `build-api-docs.mjs` — builds the TypeDoc API reference.
- `docs-check.mjs` — validates the repository documentation.
- `docs-check.test.mjs` — `node:test` fixtures for the documentation checker.
- `github-yaml-check.mjs` — validates YAML under `.github`.
- `github-yaml-check.test.mjs` — `node:test` suite for `github-yaml-check.mjs`.
- `shell-command-analysis.mjs` — parses Bash workflow steps for the pnpm
  installation-order check.
- `publish-ordered.mjs` — publishes the `PUBLISH_PREREQUISITES` packages before the Changesets remainder and gates release on prerequisite peer floors.
- `publish-ordered.test.mjs` — `node:test` suite for `publish-ordered.mjs`.
- `publish-invocation-check.mjs` — dry-runs the real publish command per package and validates peer-floor grammar.
- `conformance-config-check.test.mjs` — `node:test` checks that agent-starter's
  configuration satisfies fleet control's own validators.
- `architecture-positive-controls.test.mjs` — `node:test` positive controls; run with `pnpm architecture:controls`.
- `entry-point.mjs` — `isInvokedAsEntryPoint`, the entry-point predicate the root scripts guard their side effects with. The disposition it carries is documented on the export.
- `entry-point.test.mjs` — requires `pnpm build` before `node --test scripts/entry-point.test.mjs` because the suite's `mint` cases run the agent-starter token script against flowsafe's built `approval-api` entry.
- `child-process-fixture.mjs` — the `node:test` child-process harness.
- `vitest-project-selectors.mjs` — parses the `--project` selectors of a root script's Vitest command.
- [`baseline-recorder.mjs`](baseline-recorder.mjs): recorder configuration and supported literal values are documented on `runBaselineRecorder`.
- [`baseline-recorder.test.mjs`](baseline-recorder.test.mjs): run with `node --test scripts/baseline-recorder.test.mjs`.
- [`record-drain-baseline.mjs`](record-drain-baseline.mjs): inventory recorder. Golden assertions live in [`cloudflare-client.test.ts`](../packages/fleet-control/test/cloudflare-client.test.ts).
- [`record-audit-baseline.mjs`](record-audit-baseline.mjs): audit recorder. Golden assertions live in [`fleet-audit-golden.test.ts`](../packages/fleet-control/test/fleet-audit-golden.test.ts).
- [`record-migration-baseline.mjs`](record-migration-baseline.mjs): migration recorder. Golden assertions live in [`fleet-migration-golden.test.ts`](../packages/fleet-control/test/fleet-migration-golden.test.ts).
- [`workerd-server-lifecycle.mjs`](workerd-server-lifecycle.mjs)
- `workerd-server-lifecycle.test.mjs` — its vitest suite, run through the root
  `vitest.workerd-lifecycle.config.ts` project.
- `flowsafe-harness.test.ts` — the wrangler `createTestHarness` suite for the flowsafe worker, run through the root `vitest.flowsafe-harness.config.ts` project.
- `r2-type-compatibility.ts` — type-only assertion that the root toolchain's `R2Bucket` satisfies flowsafe's `ArtifactBucket` seam; a member of the `tsconfig.harness.json` program (`pnpm typecheck:harness`).

## Record or compare a baseline

Run a recorder manually with an explicit mode. Use `--check` to compare derived values without writing, or `--write` to replace the configured baseline and format it with Biome.

`--check` compares the exports a recorder's `exports` declarations name. An export the committed module holds without a matching declaration is compared against nothing. `--check` does not establish refusal-guard coverage; retain the ordinary guard tests and architecture checks alongside the golden assertions.

Run these checks before accepting a generated-file change:

```bash
node scripts/record-drain-baseline.mjs --check
node scripts/record-audit-baseline.mjs --check
node scripts/record-migration-baseline.mjs --check
```

To record an intended baseline change, select its command:

```bash
node scripts/record-drain-baseline.mjs --write
node scripts/record-audit-baseline.mjs --write
node scripts/record-migration-baseline.mjs --write
```
