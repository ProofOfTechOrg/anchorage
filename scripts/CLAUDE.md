# `scripts/`

Repository documentation, architecture, and publication checks. Markdown syntax checks use unified and remark; repository policy remains local.

## Contents

- `build-api-docs.mjs` — runs the Worker, React UI, and merged TypeDoc passes.
- `docs-check.mjs` — validates local docs, public exports, TypeDoc coverage,
  and external links.
- `docs-check.test.mjs` — `node:test` fixtures for the documentation checker.
- `github-yaml-check.mjs` — validates every YAML file under `.github`.
- `github-yaml-check.test.mjs` — `node:test` fixtures for the GitHub YAML checker.
- `publish-ordered.mjs` — publishes Breakwater before the Changesets remainder and gates release on prerequisite peer floors.
- `publish-ordered.test.mjs` — `node:test` fixtures for publish ordering, the
  publish argv and cwd shape, and the tag line `changesets/action` greps for.
- `publish-invocation-check.mjs` — dry-runs the real publish command per package and validates peer-floor grammar in CI and the release pre-flight.
- `conformance-config-check.test.mjs` — `node:test` checks that agent-starter's
  Workers for Platforms operator configuration and harness wrangler configs
  satisfy fleet control's own validators. Lives here because
  `.dependency-cruiser.cjs` forbids anything under `packages/` from importing
  fleet control.
- `record-drain-baseline.mjs` — records fleet control's `collectFleetInventory`
  golden baseline from the hand-authored provider world in
  `packages/fleet-control/test/fixtures/fleet-inventory-drain-world.ts` and
  writes only `…/fixtures/fleet-inventory-drain-baseline.ts`, formatting it with
  the repository's Biome. `--check` re-derives both values from the unchanged
  world, compares them structurally against the committed module's exports,
  prints every structural difference, and exits non-zero without writing. This
  script is deliberately NOT part of CI: the in-suite equivalence title in
  `packages/fleet-control/test/cloudflare-client.test.ts` is the automatic
  behavioral gate, and `--check` is the re-recording aid an author runs by hand.
- `record-audit-baseline.mjs` — records fleet control's `auditFleetDrift`
  golden baseline (findings AND the store/backend/resolver op log) from the
  hand-authored world in
  `packages/fleet-control/test/fixtures/fleet-audit-world.ts` and writes only
  `…/fixtures/fleet-audit-baseline.ts`, formatting it with the repository's
  Biome. `--check` re-derives both values from the unchanged world, compares
  them structurally against the committed module's exports, prints every
  structural difference, and exits non-zero without writing. This script is
  deliberately NOT part of CI: the in-suite equivalence title in
  `packages/fleet-control/test/fleet-audit-golden.test.ts` is the automatic
  behavioral gate, and `--check` is the re-recording aid an author runs by hand.
- `workerd-server-lifecycle.mjs` — the one `wrangler dev` start/stop protocol
  shared by the FlowSafe workerd harnesses and the conformance harness.
- `workerd-server-lifecycle.test.mjs` — its vitest suite, run through the root
  `vitest.workerd-lifecycle.config.ts` project.
