# app/

Full Vite React app for the approval dashboard — the runnable, deployable
consumer of the `approval-ui` library. The library is styling-agnostic; this
app injects an **Astryx adapter** (`src/astryx-components.tsx`) into
`ApprovalUIProvider`, and Vite bundles Astryx's CSS (`src/index.css`) + the
React components. In `serve` mode a dev-only plugin mounts a live, **seeded**
approval-api at `/api/approvals`, so the real `ApprovalApiClient` drives real
CAS transitions (claim / decide / delegate) and live metrics against
`InMemoryApprovalStore` — a working backend, not a mock.

CSS handling differs by layer: `tsc` (the library build) can't bundle CSS, so a
library consumer wires styling themselves (their own adapter, or the unstyled
HTML default); Vite (this app) bundles the Astryx adapter's CSS, so running the
app needs no extra wiring. Astryx is a `devDependency` — only this app uses it.

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `index.html` | Vite entry; mounts `src/main.tsx` into `#root` | Changing the HTML shell or title |
| `vite.config.ts` | `@vitejs/plugin-react` + (serve-only) `approvalApiDevPlugin`; requests port 4321 (auto-increments if taken) | Changing build/dev config |
| `approval-api-dev-plugin.ts` | Dev-server middleware: seeded `InMemoryApprovalStore` + `ApprovalService` + `createApprovalRouter`, with a Node↔web `Request` adapter. Runs in Node (Vite esbuild), outside the browser tsconfig's `src` root | Changing the dev backend or seed data |
| `src/main.tsx` | Builds `ApprovalApiClient`, then renders `<App>` wrapped in `ApprovalUIProvider` with the Astryx adapter | Changing app bootstrap or API target |
| `src/astryx-components.tsx` | The Astryx adapter — maps the library's `ApprovalUIComponents` slots onto `@astryxdesign/core`. The only Astryx-coupled module; swap it to restyle | Changing the Astryx mapping, or writing an adapter for another design system |
| `src/index.css` | Astryx CSS `@import`s (order: reset → base → theme), bundled by Vite | Changing theme or global styles |
| `tsconfig.json` | Browser pass (DOM + `react-jsx` + `vite/client`); typechecks `src` only | Debugging app typecheck |
| `tsconfig.node.json` | Node pass: type-checks `vite.config.ts` + `approval-api-dev-plugin.ts` (so their approval-api usage can't rot silently). Part of the `typecheck` script | Debugging the dev backend's types |

## Run

```bash
pnpm --filter @proofoftech/flowsafe app:dev      # dev server + live seeded API
pnpm --filter @proofoftech/flowsafe app:build    # production client bundle (dist/, gitignored)
```

A production `build` is a pure client bundle (the dev API plugin is not
included); point it at a deployed approval router via `VITE_APPROVAL_API_URL`.
`pnpm --filter @proofoftech/flowsafe build` also runs `app:build`, so the app
is covered by the repo verification gate.
