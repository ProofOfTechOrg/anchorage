# flowsafe/

`@proofoftech/flowsafe` — approval UX + Cloudflare-native durable execution for Mastra workflows.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `README.md` | Package purpose, subpackage roles, status, DO-runner usage example | Understanding flowsafe's design and wiring a workflow into a Durable Object |
| `CHANGELOG.md` | Release notes (0.1.0 released 2026-07-11; breakwater is an optional peer consumed only by `./host-kit/module`) | Cutting a release |
| `LICENSE` | Apache-2.0 copy shipped in the npm tarball (packing ignores the repo-root LICENSE) | Never edit separately — keep identical to the root LICENSE |
| `package.json` | Manifest, subpath exports (`./approval-api`, `./approval-ui`, `./artifacts`, `./audit-export`, `./do-runner`, `./host-kit`), scripts (`spike`/`spike:verify`, `deploy:cf`/`deploy:dev`; build/typecheck run the extra approval-ui passes incl. the UI TEST pass `src/approval-ui/tsconfig.test.json`, plus `spike/` + `deploy/` tsc passes). Sole runtime dep is `@mastra/cloudflare-d1`. Optional react peers (React 18+); breakwater devDep (tests only); happy-dom devDep (the one renderer-backed hook regression test only). The Astryx-styled demo app lives in the separate `showcase` package — published consumers pull zero Astryx | Adding a subpath export, changing scripts, bumping deps |
| `tsconfig.json` | Build TS config (emits `dist/`; excludes the approval-ui JSX set, which compiles in its own pass) | Changing build output or compiler options |
| `tsconfig.test.json` | Test-only TS config (`paths`/`rootDir` resolve `@proofoftech/breakwater` AND the `@proofoftech/flowsafe/*` subpaths from source — the latter so `deploy/worker.ts`, pulled in by its e2e test, typechecks without `dist/`) | Debugging test typecheck failures, changing cross-package test resolution |
| `vitest.config.ts` | Vitest runner config with source aliases for cross-package tests: `@proofoftech/breakwater` plus the four `@proofoftech/flowsafe/*` subpaths the deploy template imports (its exports map points at `dist/`, which tests must not depend on). Loaded by the root `vitest.config.ts` as a workspace project — root `pnpm test` runs it | Changing test globs, runner options, or the source aliases |

## Subdirectories

| Directory | What | When to read |
| --------- | ---- | ------------ |
| `src/` | All source: do-runner, approval-api, approval-ui, host-kit, and the barrel `index.ts` | Implementing or modifying the DO runner or approval surface |
| `spike/` | wrangler-dev spike Worker proving the full Phase 3 loop on workerd: suspend → auto-queued approval → restart persistence → decide → grant-minted resume (formerly `demo/`) | Running the end-to-end spike, seeing a complete Worker wiring one DO per run |
| `examples/` | Runnable, self-verifying examples (vitest specs — the only TS runner present): the gtm-outbound sketch executing end-to-end on the real seams in one Node process (`example:gtm`; also runs under `pnpm -r test`) | Seeing the full suspend→approve→grant→gated-write loop without Cloudflare |
| `deploy/` | Reference production deployment (copy-me template): bearer-token auth seam, cron-owned SLA sweep + retention purge (`scheduled()`), multi-gate approval bridging, `.dev.vars.example` for local dev. `worker.e2e.test.ts` drives the real handler's `fetch()`/`scheduled()`/`queue()` in-process (D1-shaped `node:sqlite`, stub DO namespace over real `FlowsafeRunner`s) | Deploying flowsafe for real, changing the production wiring conventions |
| `scripts/` | `spike-verify.mjs` — zero-dep Node orchestrator automating the workerd spike as a pass/fail command: scoped kill protocol (descendant-PID capture → group SIGKILL → port-refused proof → `fuser` last resort), restart on persisted state, fail-closed forged-resume probe, self-decision-denial probe (SoD across the bridge) | Changing the automated spike proof or its kill protocol |
| `test-support/` | Shared test fixtures (outside `src/`, never built into `dist/`): `sqlite.ts` — the `openSqlite()` node:sqlite probe + the full `d1DatabaseLike` D1 adapter, previously byte-copied across six suites | Changing the node:sqlite test harness or the D1 adapter shape |
| `dist/` | Generated build output (`tsc` → `dist/`; also the `vite build app` client bundle), gitignored | Never edit — rebuild with the Build command below |

## Build

```bash
pnpm --filter @proofoftech/flowsafe build
```

## Test

```bash
pnpm --filter @proofoftech/flowsafe test
```

## Spike (end-to-end suspend/resume on workerd)

Interactive server on port 8787:

```bash
pnpm --filter @proofoftech/flowsafe spike
```

## Spike verify (automated pass/fail proof)

Runs the whole protocol — start → suspend → scoped process-tree kill →
port-refused proof → restart on persisted state → decide → resumed/published
asserts → forged-resume fail-closed probe → self-decision-denied probe (the
run's requester cannot decide their own approval; SoD survives the bridge) —
and exits 0/1. Port 8799 (`SPIKE_VERIFY_PORT` overrides);
`SPIKE_VERIFY_FAULT=skip-decide` exercises the harness's own failure path:

```bash
pnpm --filter @proofoftech/flowsafe spike:verify
```

## Demo app

The Astryx-styled dashboard + the five-workflow showcase live in the separate
`showcase` package (`packages/showcase/` — `pnpm dev` at the repo root).

## Deploy (reference production template)

Checklist in `deploy/README.md`. Local iteration (`cp deploy/.dev.vars.example
deploy/.dev.vars` first for auth tokens):

```bash
pnpm --filter @proofoftech/flowsafe deploy:dev   # local workerd
pnpm --filter @proofoftech/flowsafe deploy:cf    # wrangler deploy
```
