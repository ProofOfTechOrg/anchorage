# Showcase navigation

[`README.md`](README.md) documents local, workerd, and public-demo operation.

- `src/control-room/`: seven deterministic breakwater scenarios
- `src/showcase-app.tsx`: signed-in one-page composition
- `src/token-gate.tsx`: signed-out public entry
- `src/glossary.ts`: product claims, workflow guides, and architecture copy
- `worker/`: Cloudflare host and six workflow modules
- `public/`: favicon, crawler metadata, sitemap, and social card
- `scripts/`: production-bundle and public-metadata assertions
- `wrangler.jsonc`: single Worker deployment for API and SPA

Showcase application imports use the configured aliases (`@/`, `#worker/`, and `@flowsafe/`). Keep external side-effect claims aligned with the binding-gated behavior.

```bash
pnpm --filter showcase test
pnpm --filter showcase typecheck
pnpm --filter showcase build
pnpm --filter showcase react-doctor
pnpm --filter showcase dev
```
