# app/

Full Vite React app for the Anchorage **showcase** — a launcher + run-status
panel + actor switcher layered beside the approval dashboard. The `approval-ui`
library is styling-agnostic; this app injects an **Astryx adapter**
(`src/astryx-components.tsx`) into `ApprovalUIProvider`, and Vite bundles Astryx's
CSS (`src/index.css`). The new panels render through the same injected slot
components, so they inherit the Astryx look with zero adapter changes.

In `serve` mode a dev-only plugin mounts the **in-process showcase host** —
`/api/approvals` (the dashboard) + `/runs` + `/workflows` (the launcher/status
panel) — so `app:dev` is a real working backend: launch a workflow, approve it
in the dashboard, watch it run to success. A production `build` is a pure client
bundle served by the single-deploy Worker on the same origin.

Astryx is a `devDependency` — only this app uses it; published `approval-ui`
consumers pull zero Astryx.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `index.html` | Vite entry; mounts `src/main.tsx` into `#root` | Changing the HTML shell or title |
| `vite.config.ts` | `@vitejs/plugin-react` + (serve-only) `runApiDevPlugin`; port 4321 | Changing build/dev config |
| `run-api-dev-plugin.ts` | Dev-server middleware: the showcase host in-process — one `buildShowcaseRuntime` over an `InMemoryApprovalStoreFactory` (grant provider recovers each leg's tenant from the runId prefix), per-request tenant-bound services via the SAME `createTenantResolver` seam and the SAME two routers the deployed worker mounts, plus a Node↔web `Request` adapter. Only the resume topology differs (in-process, not a DO stub) | Changing the dev backend or its routing |
| `src/main.tsx` | The `Root` shell: holds the acting TOKEN + launched runs, derives both API clients from it, renders (dev) the lazy `DevActorSwitcher`, (public demo) `DemoActorSwitcher`, or (operators) `TokenGate` — identity itself always comes from the server's `/workflows` actor echo | Changing app bootstrap, the client wiring, or panel layout |
| `src/dev-actor-switcher.tsx` | DEV-ONLY: the sole app module importing the public demo-actors tokens; reachable only through main.tsx's `import.meta.env.DEV` dynamic import, so the production bundle stays token-free (`scripts/assert-clean-app-bundle.mjs` pins it) | Changing the dev identity switcher |
| `src/demo-session.tsx` | Public-demo session: reads the OAuth callback's `#demo-tokens=` FRAGMENT once (scrubbing the hash), renders the four-role sandbox switcher, silently refreshes JWTs while the tenant lives, `useDemoSignIn` probes `/auth/config` | Changing the public-demo UX |
| `src/showcase-panels.tsx` | `TokenGate` (operator sign-in + optional GitHub demo entry), `LauncherPanel` (start any of the 5; role gates render from the SERVER'S catalog actor echo — `actorForToken`/`DEFAULT_ACTOR` are gone, an unknown token renders as nothing, never admin), `RunStatusPanel` (self-scheduling poll: surfaces a failed status READ as `unavailable`, stops on a hard `RunApiError` or after `MAX_TRANSIENT_FAILURES`). Holds NO token table and must not import demo-actors. All render through `useApprovalUIComponents()` slots | Changing the launcher/status/sign-in UI |
| `src/run-client.ts` | `RunClient` mirroring `ApprovalApiClient` (injectable baseUrl/fetch/headers, `#request`/`#post`). Local structural `WorkflowMeta`/`RunSummary` types keep the browser bundle decoupled from the Workers-typed server modules | Changing the run API client |
| `src/astryx-components.tsx` | The Astryx adapter — maps the library's `ApprovalUIComponents` slots onto `@astryxdesign/core`. The only Astryx-coupled module; swap it to restyle | Changing the Astryx mapping, or adapting another design system |
| `src/index.css` | Astryx CSS `@import`s (reset → base → theme), bundled by Vite | Changing theme or global styles |
| `tsconfig.json` | Browser pass (DOM + `react-jsx` + `vite/client`); type-checks `src` only | Debugging app typecheck |
| `tsconfig.node.json` | Node pass: type-checks `vite.config.ts` + `run-api-dev-plugin.ts`. Uses `@cloudflare/workers-types` (NOT DOM — they collide) because the plugin now pulls the workers-native showcase runtime through it | Debugging the dev backend's types |

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
