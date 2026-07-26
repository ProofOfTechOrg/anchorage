# Approval UI navigation

- `client.ts`: DOM-free REST client
- `use-approval-dashboard.ts`: headless queue and mutation state
- `components.tsx`: injectable slots and plain-HTML defaults
- `App.tsx`, `QueueView.tsx`, `DetailView.tsx`, `MetricsView.tsx`, `FilterBar.tsx`: views
- `use-web-socket-transport.ts`, `stream.ts`: live updates and reconciliation
- `mount.tsx`: imperative React mount

```bash
pnpm --filter @proofoftech/flowsafe typecheck:react18
```
