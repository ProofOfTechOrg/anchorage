# showcase/

The `showcase` workspace package — the Anchorage demo product: all five example
workflows runnable behind one React frontend, shipped as a single Cloudflare
deploy. The Vite SPA lives at the package root (`index.html`, `src/`,
`public/`); the Cloudflare host lives in `worker/` (its own `CLAUDE.md` covers
the workflows, DO topology, demo auth, and reset). One package because it is
ONE deployable unit: the Worker serves the API and the built SPA on the same
origin.

The SPA is styled with the Astryx **y2k** theme and is ONE narrative page (no
view switch, 2026-07-15): the guardrails control room on top (`src/
control-room/`), the approval dashboard directly below it (`#approvals-panel`;
the wire card's approve action is a same-page scroll), then runs + the
workflow launcher beside the client-derived activity feed. The dashboard is
composed from the flowsafe library's headless hook + views (the library `App`
shell is not used here). Two Astryx couplings, by design:

- **App-owned panels import Astryx directly** (control room, launcher, run
  cards, feed, legend, tour, switchers) — this app is the Astryx adapter's
  home.
- **Library views** (Queue/Detail/Metrics) still render through the injected
  slot adapter (`src/astryx-components.tsx`), so published `approval-ui`
  consumers keep pulling zero Astryx.

The theme is ACTIVATED, not just imported: every Astryx theme.css is
`@scope`'d to `[data-astryx-theme=<name>]`, so `main.tsx` mounts
`<Theme theme={y2kTheme}>` (which stamps the attribute and registers the
embedded icon set) and `index.html` pre-stamps it for first paint. Fonts
(Poppins, JetBrains Mono) are vendored via pinned `@fontsource` packages —
single origin, no CDN.

**Import convention (Biome-enforced, tests exempt):** relative imports are
BANNED in `src/` and `worker/`. Use:

- `@/*` → `src/*` (tsconfig paths + the vite/vitest alias)
- `#worker/*` → `worker/*` (package.json `imports` field — platform-native, so
  tsc, Vite, vitest, wrangler's esbuild, and Node all resolve it with no
  per-tool alias; this is what lets the dev plugin's config-time module graph
  work, where Vite's `resolve.alias` does not apply)
- `@flowsafe/*` → `../flowsafe/src/*` (deep DOM-free flowsafe source imports —
  `approval-api/types`, individual `approval-ui` modules — deliberately NOT
  the package barrels: the approval-api barrel pulls workers-typed stores
  into the SPA's DOM program)
- Real package specifiers for everything else (`@proofoftech/flowsafe/<sub>`,
  `@proofoftech/breakwater`) — the worker's flowsafe imports resolve to
  SOURCE in tsc/vitest/wrangler via paths/aliases, and to dist via the
  exports map everywhere else (so `pnpm dev`'s plugin graph needs a prior
  `pnpm -w build`).

**Narration honesty contract:** the activity feed/toasts are derived in the
browser from the observed run and queue updates. Those updates now arrive over a
live WebSocket stream when streaming is enabled (a per-run runner-DO socket + a
per-tenant hub socket, opt-in behind a `HUB` binding + `STREAM_TICKET_SECRET`),
and over the interval polls (runs 3s, queue/metrics 5s) otherwise — and, for the
queue, the poll keeps running as the reconciler even while the socket is healthy
(DL-021). Events restating an observed update — a poll response OR a live stream
frame — render ● solid; events describing what the deployed architecture does
between observations render ○ "by design" and only ever anchor to an observed
event. Event KEYS are deterministic, and every surface dedups by key — that (not
effect guards) is what makes StrictMode double-invokes, racing poll/stream
sources, and decider-vs-observer overlap collapse to one line.

In `serve` mode a dev-only plugin mounts the **in-process showcase host** —
`/api/approvals` + `/runs` + `/workflows` — so `pnpm dev` is a real working
backend: launch a workflow, approve it, watch it resume. A production `build`
is a pure client bundle served by the single-deploy Worker on the same origin.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `package.json` | The package manifest: scripts (`predev` builds the two libraries — the dev plugin resolves them via dist; `dev`, `build`, `dev:worker`, `deploy:cf` — NOT `deploy`, which pnpm's builtin of the same name shadows — `test`, `typecheck`, `lint`, `react-doctor`, `react-doctor:diff`), the `#worker/*` imports field (maps to `./worker/*.ts` — LITERAL substitution, so it resolves `.ts` files ONLY; a non-.ts asset under worker/ would need its own mapping), runtime deps (Astryx, react 19, workspace flowsafe + breakwater, @mastra/core, zod) and tooling devDeps (vite, vitest, wrangler, jsdom + testing-library) | Adding a dep or script, changing the `#worker` mapping |
| `wrangler.jsonc` | Deploy config — custom domain `anchorage.proofoftech.org` + `workers_dev: false` (single public origin: the Google OAuth callback is registered for exactly that origin), `RUNNER` DO + `HUB` DO (the `ShowcaseHub` streaming hub, added under an append-only `v2` migration) + `DB` D1, two crons, `main: worker/worker.ts`, `tsconfig: tsconfig.worker.json` (esbuild bundles flowsafe + breakwater from SOURCE — no dist build order for wrangler), the `assets` block (serves `./dist` at `/`, `run_worker_first` keeps the API — including `/api/stream/*` — on the same origin). Carries NO credentials: `APPROVAL_ACTOR_TOKENS` is a secret (a deploy without it 401s everywhere), and `STREAM_TICKET_SECRET` gates streaming (absent ⇒ the composer leaves it unmounted, poll-only) | Changing bindings, the public origin, the streaming hub/migration, or the assets/single-deploy config |
| `.dev.vars.example` | Local-dev secrets (`cp .dev.vars.example .dev.vars` — REQUIRED for `dev:worker`): the demo `APPROVAL_ACTOR_TOKENS` map, plus the optional SIEM header. Lives beside wrangler.jsonc, where wrangler reads it | Running `dev:worker`, or wiring real secrets |
| `doctor.config.jsonc` | react-doctor config: scopes the scan to the React app (worker/ + node glue ignored — deslop can't resolve `#worker/*`), plus per-file rule ignores with the WHY inline (composition root, polling-architecture session module, dev-only switcher) | Changing what the react-doctor gate ignores |
| `index.html` | Vite entry; mounts `src/main.tsx` into `#root`; pre-stamps `data-astryx-theme="y2k"` (first-paint theming). Carries a TEMPORARY `noindex` robots meta (pre-announce; remove when the live demo should be indexed — tracked in root `CLAUDE.md`) | Changing the HTML shell, title, or the noindex posture |
| `vite.config.ts` | `@vitejs/plugin-react` + (serve-only) `runApiDevPlugin`; the `@/` + `@flowsafe/` resolve aliases; port 4321 | Changing build/dev config or aliases |
| `vitest.config.ts` | Test config: `@/` + `@flowsafe/` + cross-package source aliases (`@proofoftech/*` → source, mirroring tsconfig.worker.json), `src/test/setup.ts`, node env by default (component tests opt into jsdom per-file via `// @vitest-environment jsdom`) | Changing test globs or aliases |
| `tsconfig.json` | Browser pass (DOM + `react-jsx` + `vite/client`; `@/*` + `@flowsafe/*` paths); type-checks `src` only | Debugging app typecheck |
| `tsconfig.worker.json` | Worker pass (workers-types; `@proofoftech/*` → source paths). ALSO the tsconfig wrangler's esbuild bundles with — its paths are why deploy needs no dist | Debugging worker typecheck or wrangler bundling |
| `tsconfig.node.json` | Node pass: type-checks `vite.config.ts` + `run-api-dev-plugin.ts`. Uses `@cloudflare/workers-types` (NOT DOM — they collide) because the plugin pulls the workers-native showcase runtime | Debugging the dev backend's types |
| `run-api-dev-plugin.ts` | Dev-server middleware: the showcase host in-process — one `buildShowcaseRuntime` over an `InMemoryApprovalStoreFactory` (grant provider recovers each leg's tenant from the runId prefix), per-request tenant-bound services via the SAME `createTenantResolver` seam and the SAME routers the deployed worker mounts (approvals + runs + `/demo/reset` over in-memory seams), plus a Node↔web `Request` adapter. Only the resume topology differs (in-process, not a DO stub). Package-root node file: its own imports stay relative (`./worker/*.js`) because Vite's config bundler resolves them before any alias exists | Changing the dev backend or its routing |
| `scripts/assert-clean-app-bundle.mjs` | Post-build tripwire: the production bundle in `dist/` must contain NO demo token (main.tsx keeps the dev switcher behind a DEV-only dynamic import; this proves the dead branch got eliminated) | Changing the bundle-cleanliness guarantee |
| `scripts/resolve-react-doctor-diff-base.sh` | Resolves the `--base` ref for `react-doctor:diff` (open PR's base via `gh`, else react-doctor's `parent` fork-point heuristic); consumed by the pre-push hook | Changing the pre-push diff base |
| `src/main.tsx` | The `Root` shell: mounts the plain-HTML `AppErrorBoundary` OUTSIDE `<Theme y2k>` + `ToastViewport`; holds the acting TOKEN + launched runs + the activity feed, derives both API clients, computes `canReset` + `clearRuns`, renders the switcher variants and `ShowcaseApp` — identity always comes from the server's actor echo | Changing app bootstrap or client wiring |
| `src/showcase-app.tsx` | `ShowcaseApp` — the signed-in composition, one scrolling page: header (legend + tour + "Reset sandbox" + identity chip), the control room (`#control-room`), the approvals card (`#approvals-panel`: TabList over the library hook + views, SoD notice), then runs + walkthrough banner + launcher beside the activity feed; narration + toast hooks, the reset `AlertDialog`, the run-poll `retryNonce`. Jumps (`reviewApproval`/`viewRun`) are plain same-page scrolls | Changing the page composition |
| `src/card-ink.ts` | `cardInkMap` — toned `--color-text-*` overrides for tinted SelectableCard grids; without it dark mode paints near-white text on the pastel tints (the y2k tints keep one light hex in both modes) | Changing card tinting or adding a tinted card grid |
| `src/error-boundary.tsx` | `AppErrorBoundary` — class boundary whose fallback is deliberately PLAIN HTML (inline styles): if the theme/Astryx is what threw, re-rendering Astryx inside the boundary would throw again | Changing crash handling |
| `src/narration.ts` | The narration core (DOM-free, tested): `NarrationEvent` + deterministic key discipline, snapshot derivers, one-shot builders, `interpretRunResult` | Changing what gets narrated |
| `src/glossary.ts` | All explanatory copy: `TAGLINE`, `ZONES`, `GLOSSARY`, `ROLE_NOTES`, `WORKFLOW_GUIDES` (step ids MUST match Mastra step ids), `claimableSteps`, `DRY_RUN_TRIO_FOOTER`. Bound by the truthfulness rules | Changing any user-facing copy |
| `src/use-activity-feed.ts` | The feed store: newest-first, cap 200, key-dedup INSIDE the setState updater | Changing feed retention |
| `src/use-snapshot-narration.ts` | Diffs each polled snapshot vs a ref advanced before deriving; first snapshot silent; `ready` gate | Changing snapshot diffing |
| `src/use-narration-toasts.tsx` | Feed events → Astryx toasts: seen-key cursor, tone mapping, uniqueID dedup, jump actions | Changing toast behavior |
| `src/narrating-approval-client.ts` | `NarratingApprovalClient extends ApprovalApiClient` — narrates decide/claim/delegate | Changing mutation narration |
| `src/activity-feed.tsx` | The feed panel: ●/○ rows with zone badges, scrolling row list with head-change snap-back | Changing the feed UI |
| `src/zone-badge.tsx` | `ZoneBadge` — one fixed Token color per architecture zone + hover blurb | Changing zone presentation |
| `src/architecture-legend.tsx` | "Where things run" Dialog + "What's real here?" Collapsible | Changing the architecture explainers |
| `src/intro-tour.tsx` | The 60-second tour Dialog — localStorage-dismissed, reopenable | Changing onboarding |
| `src/workflow-launcher.tsx` | `WorkflowLauncher` — SelectableCard picker (toned ink via `card-ink.ts`), first-load spinner, capability Tokens, role gates from the SERVER'S catalog actor echo, JSON input Collapsible | Changing the launcher |
| `src/run-cards.tsx` | `RunCards` — per-run Card: step chips, suspension story, interpreted outcome badges over the raw result JSON, per-workflow reality notes, retry button | Changing the run cards |
| `src/use-run-polling.ts` | `useRunPolling` — the self-scheduling 3s run poll (pure `pollableRuns`/`mergeRunResults`/`allRunsSettled`) with transient-failure abandonment + `retryNonce` re-arm; also subscribes to the per-run WebSocket stream when a `runStream` option is present, updating status from the wholesale `RunSummary` frames and PAUSING a run's poll while its socket is healthy, resuming on close (DL-021 wholesale channel) | Changing run polling or the run stream |
| `src/token-gate.tsx` | `TokenGate` (signed-out landing) + `OperatorIdentityChip` | Changing sign-in UI |
| `src/demo-session.tsx` | Public-demo session: OAuth callback fragment read, sandbox chip + role SegmentedControl, silent JWT refresh, expiry warning, `useDemoSignIn` tri-state provider probe | Changing the public-demo UX |
| `src/dev-actor-switcher.tsx` | DEV-ONLY: the sole app module importing the public demo-actors tokens (`#worker/demo-actors`); reachable only through main.tsx's `import.meta.env.DEV` dynamic import, so the production bundle stays token-free (`scripts/assert-clean-app-bundle.mjs` pins it) | Changing the dev identity switcher |
| `src/proofoftech-logo.tsx` | `ProofOfTechMark` — the Proof of Tech interlocked-squares mark inlined as JSX | Changing the brand footer |
| `src/marker-row.tsx` | `MarkerRow` — the one marker+text row primitive behind the explainer surfaces | Changing explainer-row styling |
| `src/run-client.ts` | `RunClient` mirroring `ApprovalApiClient` — catalog/start/status plus `reset()`. Local structural types keep the browser bundle decoupled from Workers-typed server modules | Changing the run API client |
| `src/astryx-components.tsx` | The slot adapter for LIBRARY views — maps `ApprovalUIComponents` onto `@astryxdesign/core` | Changing the slot mapping |
| `src/astryx-stream-components.tsx` | The Astryx `Toast` + `PresenceIndicator` slot adapters (Part B streaming), merged into the provider alongside `astryx-components` | Changing the streaming slot adapters |
| `src/web-socket-transport.ts` | Thin showcase re-export of approval-ui's browser-WebSocket `StreamTransport` (Part B) | Changing the SPA WS transport |
| `src/hub-stream-client.ts` | Builds the hub + per-run `ticket()` thunks over the authenticated `ApprovalApiClient` (Part B); the run thunk passes the workflowId so the SERVER returns the fully-qualified run WS url (the route shape is authored once, server-side) | Changing SPA stream addressing |
| `src/index.css` | Font `@import`s then Astryx CSS (reset → base → y2k theme), bundled by Vite; plus the app-level layout guards: overscroll kill, `overflow-x: clip`, Grid minmax `min()` clamps, sticky activity column, selected-card ring, and the narrow-viewport clamps (header tools + approvals fields) that keep intrinsic-width Astryx controls inside phone viewports | Changing theme or global styles |
| `src/test/setup.ts` | Vitest setup (jest-dom matchers; safe under node env too) | Changing test setup |
| `src/marker-row.test.tsx` | Component smoke test proving the jsdom + testing-library pipeline | Changing the component-test plumbing |
| `src/narration.test.ts` / `src/glossary.test.ts` | Derivation coverage + copy completeness | Changing narration or copy |
| `public/favicon.svg` | The Proof of Tech mark, verbatim from proofoftech.org (solid tile — works on light and dark tab bars) | Changing the favicon |

## Subdirectories

| Directory | What | When to read |
| --------- | ---- | ------------ |
| `src/control-room/` | The guardrails control room, the page's flagship top section: `control-room.tsx` (scenario SelectableCards + the scenario/wire panels; wire run state hoisted so card switches can't double-start a real run), `engine.ts` (DOM-free harness running REAL breakwater policy/RBAC/tool evaluators over scripted token streams), `scenarios.ts` (the seven client-side scenarios + copy), `engine.test.ts` | Changing scenarios, the control-room UI, or the in-browser enforcement harness |
| `worker/` | The Cloudflare host: `worker.ts` (DO topology, crons, auth), `runtime.ts`, `workflows/` (the 5 modules), demo-auth/demo-reset/demo-actors + their tests. Has its own `CLAUDE.md` | Changing the host, a workflow, or the demo lifecycle |
| `dist/` | Generated Vite client bundle (served by the Worker as assets), gitignored | Never edit — rebuild with `pnpm --filter showcase build` |

## Run

```bash
pnpm dev                             # root shortcut → vite dev server + in-process host (:4321)
pnpm --filter showcase build         # production client bundle (dist/) + clean-bundle assert
pnpm --filter showcase dev:worker    # wrangler dev (:8787) — real Worker + DO + D1; needs .dev.vars + a built dist/
pnpm showcase:deploy                 # build + wrangler deploy (anchorage.proofoftech.org)
pnpm --filter showcase test          # or root `pnpm test` for the whole workspace
pnpm react-doctor                    # full 100/100 gate (also in CI); react-doctor:diff = changed files (pre-push)
```

Demo actors (bearer tokens, declared once in `worker/demo-actors.ts`):
`demo-admin`, `demo-builder`, `demo-operator`, `demo-reviewer`, `demo-viewer`.
Switch identity in the UI to see RBAC + SoD. `pnpm dev` needs no `.dev.vars`
(the in-process host reads the const directly) but DOES need a prior
`pnpm -w build` (the plugin's flowsafe/breakwater imports resolve to dist at
config-load time); `dev:worker` needs `.dev.vars` and bundles both libraries
from SOURCE via tsconfig.worker.json.

Dev-host quirks vs the deployed demo: `pnpm dev` bootstraps as **admin**
(`DEMO_ACTORS[0]`), has no OAuth/budgets/429 path, and uses a 1h approval SLA
(the deployed showcase worker uses the 4h the tips describe).
