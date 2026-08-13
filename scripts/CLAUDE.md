# `scripts/`

Repository documentation, architecture, and publication checks. Markdown syntax checks use unified and remark; repository policy remains local.

## Contents

- `build-api-docs.mjs` — runs the Worker, React UI, and merged TypeDoc passes.
- `docs-check.mjs` — validates local docs, public exports, TypeDoc coverage,
  and external links.
- `docs-check.test.mjs` — `node:test` fixtures for the documentation checker.
- `publish-ordered.mjs` — publishes Breakwater before the Changesets remainder.
- `publish-ordered.test.mjs` — `node:test` fixtures for publish ordering, the
  publish argv and cwd shape, and the tag line `changesets/action` greps for.
- `publish-invocation-check.mjs` — dry-runs the real publish command per package.
- `conformance-config-check.test.mjs` — `node:test` checks that agent-starter's
  Workers for Platforms operator configuration and harness wrangler configs
  satisfy fleet control's own validators. Lives here because
  `.dependency-cruiser.cjs` forbids anything under `packages/` from importing
  fleet control.
- `workerd-server-lifecycle.mjs` — the one `wrangler dev` start/stop protocol
  shared by the FlowSafe workerd harnesses and the conformance harness.
- `workerd-server-lifecycle.test.mjs` — its vitest suite, run through the root
  `vitest.workerd-lifecycle.config.ts` project.
