# `scripts/`

Markdown syntax checks use unified and remark; repository policy remains local.

## Entry points

- `docs-check.mjs`: `pnpm docs:check` and `pnpm docs:check:external`
- `github-yaml-check.mjs`: `pnpm github:check`
- `build-api-docs.mjs`: `pnpm docs:api`
- `publish-ordered.mjs`: `pnpm release:publish`; `publish-invocation-check.mjs`: `pnpm test:release-invocation`
- `architecture-positive-controls.test.mjs`: `pnpm architecture:controls`; `conformance-config-check.test.mjs`: `pnpm test:conformance-config`
- `record-drain-baseline.mjs`, `record-audit-baseline.mjs`, `record-migration-baseline.mjs`: baseline recorders (below); their golden assertions live in `packages/fleet-control/test/` (`cloudflare-client.test.ts`, `fleet-audit-golden.test.ts`, `fleet-migration-golden.test.ts`)

A `*.test.mjs` beside a tool is its `node:test` suite. `entry-point.test.mjs` needs `pnpm build` first: its `mint` cases run the agent-starter token script against flowsafe's built `approval-api` entry. `workerd-server-lifecycle.test.mjs` and `flowsafe-harness.test.ts` run as root Vitest projects, not under `node --test`.

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
