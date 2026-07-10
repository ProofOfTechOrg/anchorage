# app/

Full Vite React app for the Anchorage **showcase**, styled with the Astryx
**y2k** theme: a workflow launcher, live run cards, a client-derived activity
feed + toasts narrating what the platform does, and the approval dashboard —
composed from the library's headless hook + views (the library `App` shell is
not used here). Two Astryx couplings, by design:

- **App-owned panels import Astryx directly** (launcher, run cards, feed,
  legend, tour, switchers) — the app is the Astryx adapter's home.
- **Library views** (Queue/Detail/Metrics) still render through the injected
  slot adapter (`src/astryx-components.tsx`), so published `approval-ui`
  consumers keep pulling zero Astryx.

The theme is ACTIVATED, not just imported: every Astryx theme.css is
`@scope`'d to `[data-astryx-theme=<name>]`, so `main.tsx` mounts
`<Theme theme={y2kTheme}>` (which stamps the attribute and registers the
embedded icon set) and `index.html` pre-stamps it for first paint. Fonts
(Poppins, JetBrains Mono) are vendored via pinned `@fontsource` packages —
single origin, no CDN.

**Narration honesty contract:** the activity feed/toasts are derived in the
browser from the polled snapshots (runs 3s, queue/metrics 5s) — there is no
event API. Events restating an observed response render ● solid; events
describing what the deployed architecture does between observations render
○ "by design" and only ever anchor to an observed event. Event KEYS are
deterministic, and every surface dedups by key — that (not effect guards) is
what makes StrictMode double-invokes, racing poll streams, and
decider-vs-observer overlap collapse to one line.

In `serve` mode a dev-only plugin mounts the **in-process showcase host** —
`/api/approvals` + `/runs` + `/workflows` — so `app:dev` is a real working
backend: launch a workflow, approve it, watch it resume. A production `build`
is a pure client bundle served by the single-deploy Worker on the same origin.

Astryx is a `devDependency` — only this app uses it; published `approval-ui`
consumers pull zero Astryx.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `index.html` | Vite entry; mounts `src/main.tsx` into `#root`; pre-stamps `data-astryx-theme="y2k"` (first-paint theming). Carries a TEMPORARY `noindex` robots meta (pre-announce; remove when the live demo should be indexed — tracked in root `CLAUDE.md`) | Changing the HTML shell, title, or the noindex posture |
| `vite.config.ts` | `@vitejs/plugin-react` + (serve-only) `runApiDevPlugin`; port 4321 | Changing build/dev config |
| `run-api-dev-plugin.ts` | Dev-server middleware: the showcase host in-process — one `buildShowcaseRuntime` over an `InMemoryApprovalStoreFactory` (grant provider recovers each leg's tenant from the runId prefix), per-request tenant-bound services via the SAME `createTenantResolver` seam and the SAME routers the deployed worker mounts (approvals + runs + `/demo/reset` over in-memory seams: Mastra workflows-domain per-run delete + `storeFactory.purgeTenant`), plus a Node↔web `Request` adapter. Only the resume topology differs (in-process, not a DO stub) | Changing the dev backend or its routing |
| `src/main.tsx` | The `Root` shell: mounts the plain-HTML `AppErrorBoundary` OUTSIDE `<Theme y2k>` + `ToastViewport`; holds the acting TOKEN + launched runs + the activity feed, derives both API clients (`NarratingApprovalClient` pinned to the feed's stable `record`), computes `canReset` (demo session or dev only) + `clearRuns`, renders the switcher variants and `ShowcaseApp` — identity always comes from the server's actor echo | Changing app bootstrap or client wiring |
| `src/showcase-app.tsx` | `ShowcaseApp` — the signed-in composition: header (legend + tour + "Reset sandbox" + identity chip from the server echo), walkthrough banner, 2-col grid (launcher + run cards \| sticky feed + reality legend), approvals TabList over the library hook + views (queue spins until the FIRST poll settles), SoD notice, snapshot-narration + toast hooks (narration gated on the dashboard's FIRST settled refresh — reload noise), the reset `AlertDialog` (destructive confirm; on success: clear runs + feed → narrate counts → `dashboard.refresh()` → launcher remount via `key={resetEpoch}`; AlertDialog does NOT auto-close), and the run-poll `retryNonce` | Changing the page composition |
| `src/error-boundary.tsx` | `AppErrorBoundary` — class boundary whose fallback is deliberately PLAIN HTML (inline styles): if the theme/Astryx is what threw, re-rendering Astryx inside the boundary would throw again | Changing crash handling |
| `src/narration.ts` | The narration core (DOM-free, tested): `NarrationEvent` + deterministic key discipline, snapshot derivers (`deriveRunEvents` keyed on (step, resume ordinal) — catches gate1→gate2 flips where status stays 'suspended'; `deriveApprovalEvents`), one-shot builders (`startEvent`/`decideEvents` pre-record the keys polls re-derive; `resetEvent`/`resetErrorEvent` — timestamp-keyed, counts verbatim from the response, budget-not-refilled copy), `interpretRunResult` (simulated/real-write/declined/preview/replayed) | Changing what gets narrated |
| `src/glossary.ts` | All explanatory copy: `TAGLINE`, `ZONES`, `GLOSSARY` concept tips, `ROLE_NOTES`, `WORKFLOW_GUIDES` (step ids MUST match Mastra step ids — the run card highlights the suspended step by id; `.branch()` targets marked `conditionalSteps`), `claimableSteps` (the steps a start narration may truthfully claim — excludes branch-conditional ones), `DRY_RUN_TRIO_FOOTER`. Bound by the truthfulness rules (never claim delivery; grants are derived, never sent; verified numbers only) | Changing any user-facing copy |
| `src/use-activity-feed.ts` | The feed store: newest-first, cap 200, key-dedup INSIDE the setState updater (StrictMode/poll-race safe) | Changing feed retention |
| `src/use-snapshot-narration.ts` | Diffs each polled snapshot vs a ref advanced before deriving; first snapshot silent; `ready` gate keeps the pre-fetch empty state from becoming the diff baseline | Changing snapshot diffing |
| `src/use-narration-toasts.tsx` | Feed events → Astryx toasts: seen-key cursor (first pass swallows history), tone→info\|error, sticky/long durations, uniqueID dedup, Review/View-run jump actions | Changing toast behavior |
| `src/narrating-approval-client.ts` | `NarratingApprovalClient extends ApprovalApiClient` — narrates decide (captures `DecideResult.resume`, pre-records resumed/grant keys; 403 → SoD story), claim, delegate | Changing mutation narration |
| `src/activity-feed.tsx` | The feed panel: ●/○ rows with zone badges, timestamps, jump buttons; honesty-contract header. The row list scrolls inside its own `min(60vh, 720px)` container and snaps back to the top when the head event changes (feed is newest-first; instant under `prefers-reduced-motion`) | Changing the feed UI |
| `src/zone-badge.tsx` | `ZoneBadge` — one fixed Token color per architecture zone + hover blurb; feed and legend must agree | Changing zone presentation |
| `src/architecture-legend.tsx` | "Where things run" Dialog (five zone cards + four-gate and grant-derivation stories) + "What's real here?" Collapsible (real / simulated / in-between) | Changing the architecture explainers |
| `src/intro-tour.tsx` | The 60-second tour Dialog — localStorage-dismissed (`anchorage-tour-dismissed`), reopenable via the header "Tour" button | Changing onboarding |
| `src/workflow-launcher.tsx` | `WorkflowLauncher` — SelectableCard picker (one y2k categorical color per workflow, each card carrying its `CARD_INK` toned text vars — the pastels stay light in dark mode while `--color-text-primary` flips near-white, so unstyled titles would wash out), first-load spinner (`isLoading`), capability Tokens with hover tips, role gates from the SERVER'S catalog actor echo, JSON input in a Collapsible (invalid JSON stays an inline error, never a toast), start/start-error narration | Changing the launcher |
| `src/run-cards.tsx` | `RunCards` — per-run Card: step chips in definition order (gate + suspended steps marked), suspension story (reason, connectors-to-grant, (suspendedAt, resume #) fingerprint, Review-approval deep link preferring the run's OPEN record), interpreted outcome badges (SIMULATED / REAL WRITE / DECLINED / DRY-RUN PREVIEW / REPLAYED) over the raw result JSON as proof, per-workflow reality notes, and a "Retry live updates" button when polling abandoned the run | Changing the run cards |
| `src/use-run-polling.ts` | `useRunPolling` — the self-scheduling 3s run poll (`RunEntry`/`RunResult`; surfaces a failed status READ as `unavailable`, stops on a hard `RunApiError` or after `MAX_TRANSIENT_FAILURES`, marks abandonment as `stopped` for narration; a `retryNonce` bump re-arms the effect, forgiving abandonments by construction) | Changing run polling |
| `src/token-gate.tsx` | `TokenGate` (signed-out landing: wordmark + "demo" chip on the public demo, tagline, `DEMO_POINTS` what/why bullets (truthfulness-rule-bound), sandbox promise, OAuth demo entry + operator token paste — inline when it is the only entry, behind an "operator API token" modal when the OAuth demo leads; the entry section HOLDS while the tri-state /auth/config probe is in flight so the OAuth landing never paints the paste layout and swaps it; Proof of Tech attribution footer with GitHub + proofoftech.org links) + `OperatorIdentityChip` | Changing sign-in UI |
| `src/demo-session.tsx` | Public-demo session: reads the OAuth callback's `#demo-tokens=` FRAGMENT once (scrubbing the hash), the compact sandbox chip + role SegmentedControl, silent JWT refresh (narrated), T-15min expiry warning, `useDemoSignIn` tri-state provider probe (`loading`/`oauth`/`none` — loading must stay distinguishable or the landing flashes the wrong layout) | Changing the public-demo UX |
| `src/dev-actor-switcher.tsx` | DEV-ONLY: the sole app module importing the public demo-actors tokens; reachable only through main.tsx's `import.meta.env.DEV` dynamic import, so the production bundle stays token-free (`scripts/assert-clean-app-bundle.mjs` pins it) | Changing the dev identity switcher |
| `src/proofoftech-logo.tsx` | `ProofOfTechMark` — the Proof of Tech interlocked-squares mark inlined as JSX (geometry from proofoftech.org's `public/logo-mark.svg`; the notch is a real even-odd cut-out — the source SVG paints it with that site's canvas color, which would render as an off-color patch on the y2k card — and the ink is `currentColor`; aria-hidden) | Changing the brand footer |
| `src/marker-row.tsx` | `MarkerRow` — the one marker+text row primitive behind the landing's demo points, the tour steps, and the reality legend (`markerHidden` aria-hides decorative bullet glyphs; ordinals stay audible) | Changing explainer-row styling |
| `src/run-client.ts` | `RunClient` mirroring `ApprovalApiClient` (injectable baseUrl/fetch/headers) — catalog/start/status plus `reset()` (`POST /demo/reset` → `DemoResetResponse` with the exact purge counts). Local structural `WorkflowMeta`/`RunSummary` types (incl. the suspension fingerprint fields `suspendPayload`/`suspendedAt`/`resumeCount`) keep the browser bundle decoupled from Workers-typed server modules | Changing the run API client |
| `src/astryx-components.tsx` | The slot adapter for LIBRARY views — maps `ApprovalUIComponents` (incl. `InfoTip` → Tooltip with a dotted help underline; `EmptyState` forwards `description`) onto `@astryxdesign/core`. Swap it to restyle the dashboard views | Changing the slot mapping |
| `src/index.css` | Font `@import`s (`@fontsource` Poppins 400–700 + JetBrains Mono 400) then Astryx CSS (reset → base → y2k theme), bundled by Vite; plus the `.anchorage-activity-column` sticky rule (media queries can't live in Astryx inline-style props) | Changing theme or global styles |
| `src/narration.test.ts` / `src/glossary.test.ts` | Derivation coverage (flips, fingerprints, gate1→gate2, declined/simulated, short-circuit, key idempotence, decide/claim/error branches) + copy completeness | Changing narration or copy |
| `tsconfig.json` | Browser pass (DOM + `react-jsx` + `vite/client`); type-checks `src` only (tests included) | Debugging app typecheck |
| `tsconfig.node.json` | Node pass: type-checks `vite.config.ts` + `run-api-dev-plugin.ts`. Uses `@cloudflare/workers-types` (NOT DOM — they collide) because the plugin pulls the workers-native showcase runtime | Debugging the dev backend's types |

## Run

```bash
pnpm --filter @proofoftech/flowsafe app:dev      # dev server + in-process showcase host
pnpm --filter @proofoftech/flowsafe app:build    # production client bundle (dist/, gitignored)
```

Demo actors (bearer tokens, declared once in `showcase/demo-actors.ts`):
`demo-admin`, `demo-builder`, `demo-operator`, `demo-reviewer`, `demo-viewer`.
Switch identity in the UI to see RBAC + SoD. `app:dev` needs no `.dev.vars`
(the in-process host reads the const directly); `showcase:dev` does.
`pnpm --filter @proofoftech/flowsafe build` also runs `app:build`, so the app is
covered by the repo verification gate.

Dev-host quirks vs the deployed demo: `app:dev` bootstraps as **admin**
(`DEMO_ACTORS[0]`), has no OAuth/budgets/429 path, and uses a 1h approval SLA
(the deployed showcase worker uses the 4h the tips describe).
