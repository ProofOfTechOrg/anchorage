# `scripts/`

Markdown syntax checks use unified and remark; repository policy remains local.

## Entry points

- `docs-check.mjs`: `pnpm docs:check` and `pnpm docs:check:external`
- `github-yaml-check.mjs`: `pnpm github:check`
- `build-api-docs.mjs`: `pnpm docs:api`
- `publish-ordered.mjs`: `pnpm release:publish`; `publish-invocation-check.mjs`: `pnpm test:release-invocation`
- `architecture-positive-controls.test.mjs`: `pnpm architecture:controls`

A `*.test.mjs` beside a tool is its `node:test` suite. `entry-point.test.mjs` needs `pnpm build` first: its `mint` cases run the agent-starter token script against flowsafe's built `approval-api` entry. `workerd-server-lifecycle.test.mjs` and `flowsafe-harness.test.ts` run as root Vitest projects, not under `node --test`.
