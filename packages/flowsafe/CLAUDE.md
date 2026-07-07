# flowsafe/

`@proofoftech/flowsafe` — approval UX + Cloudflare-native durable execution for Mastra workflows.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `README.md` | Package purpose, subpackage roles, status, DO-runner usage example | Understanding flowsafe's design and wiring a workflow into a Durable Object |
| `package.json` | Manifest, subpath exports (`./approval-api`, `./approval-ui`, `./do-runner`), scripts (incl. `spike`/`spike:verify`, `app:dev`/`app:build`, `deploy:cf`/`deploy:dev`; build/typecheck run the extra approval-ui + app + demo + deploy tsc passes and `vite build app`; lint covers `src/ app/ scripts/ deploy/`). Sole runtime dep is `@mastra/cloudflare-d1`; Astryx (`@astryxdesign/*`, `@stylexjs/stylex`, pinned) is a **devDependency** used only by the app adapter, so published consumers pull zero Astryx. Optional react peers (React 18+); breakwater devDep (tests only) | Adding a subpath export, changing scripts, bumping deps |
| `tsconfig.json` | Build TS config (emits `dist/`; excludes the approval-ui JSX set, which compiles in its own pass) | Changing build output or compiler options |
| `tsconfig.test.json` | Test-only TS config (`paths`/`rootDir` resolve `@proofoftech/breakwater` from source) | Debugging test typecheck failures, changing cross-package test resolution |
| `vitest.config.ts` | Vitest runner config with the breakwater source alias for cross-package tests | Changing test globs, runner options, or the breakwater alias |

## Subdirectories

| Directory | What | When to read |
| --------- | ---- | ------------ |
| `src/` | All source: do-runner, approval-api, approval-ui, and the barrel `index.ts` | Implementing or modifying the DO runner or approval surface |
| `demo/` | wrangler-dev spike Worker proving the full Phase 3 loop on workerd: suspend → auto-queued approval → restart persistence → decide → grant-minted resume | Running the end-to-end spike, seeing a complete Worker wiring one DO per run |
| `deploy/` | Reference production deployment (copy-me template): bearer-token auth seam, cron-owned SLA sweep + retention purge (`scheduled()`), multi-gate approval bridging, `.dev.vars.example` for local dev | Deploying flowsafe for real, changing the production wiring conventions |
| `scripts/` | `spike-verify.mjs` — zero-dep Node orchestrator automating the workerd spike as a pass/fail command: scoped kill protocol (descendant-PID capture → group SIGKILL → port-refused proof → `fuser` last resort), restart on persisted state, fail-closed forged-resume probe, self-decision-denial probe (SoD across the bridge) | Changing the automated spike proof or its kill protocol |
| `app/` | Full Vite React app: the Astryx-styled approval dashboard, with a dev-only Vite plugin mounting a live seeded approval-api at `/api/approvals` | Running/screenshotting the dashboard, changing the app shell or dev backend |
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

## App (Astryx dashboard, live seeded API in dev)

```bash
pnpm --filter @proofoftech/flowsafe app:dev
```

## Deploy (reference production template)

Checklist in `deploy/README.md`. Local iteration (`cp deploy/.dev.vars.example
deploy/.dev.vars` first for auth tokens):

```bash
pnpm --filter @proofoftech/flowsafe deploy:dev   # local workerd
pnpm --filter @proofoftech/flowsafe deploy:cf    # wrangler deploy
```
